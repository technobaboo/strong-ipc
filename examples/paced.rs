//! paced round-trip latency for strong-ipc — the sparse, frame-rate regime
//!
//! run with `cargo run --release --example paced`. it re-execs itself as the server
//! child, so there's nothing to start by hand.
//!
//! `bench.rs` measures the *saturated* regime: both processes spinning flat out, caches
//! hot, the peer always already runnable. an xr app doesn't look like that. it sends a
//! handful of messages per frame and then goes quiet for the rest of the frame, so every
//! send has to wake a parked process off an idle core. that wakeup is real cost, and the
//! saturated benchmark cannot see it.
//!
//! this example reproduces the frame-loop shape instead:
//!   - `--hz N`        wake N times a second (144 = practical hmd maximum)
//!   - `--burst N`     send N messages per wakeup
//!   - `--caps N`      capabilities attached to each message
//!   - `--sequential`  await each reply before sending the next, instead of pipelining
//!   - `--busy-wait`   spin to the next deadline instead of sleeping
//!
//! `--busy-wait` is the control. same pacing, same message count, but the client never
//! parks — so the gap between a normal run and a `--busy-wait` run is the price of going
//! idle between frames. that difference is the whole reason this file exists.
//!
//! reported per message and per burst, since what a frame loop actually cares about is
//! whether the *whole* burst fits in the frame, not what one message cost.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::{Child, Command},
    sync::Mutex,
    time::{Duration, Instant},
};
use strong_ipc::{BoundNode, FdVec, Handler, Message, Node, Ref};
use tokio::sync::mpsc::error::TrySendError;

/// recv_loop's read buffer in lib.rs; payloads above this get truncated by the kernel
const MAX_PAYLOAD: usize = 8192;
/// the sequence number lives in the first 8 bytes of every payload
const SEQ_BYTES: usize = 8;
/// give up on a burst that never comes back, rather than hanging the run
const REPLY_TIMEOUT: Duration = Duration::from_secs(1);

// ---------------------------------------------------------------- /proc helpers

fn clk_tck() -> f64 {
    (unsafe { libc::sysconf(libc::_SC_CLK_TCK) }) as f64
}

/// utime+stime for `pid`, in seconds
fn cpu_seconds(pid: u32) -> Option<f64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // comm can contain spaces and parens, so split on the *last* ')'
    let rest = &stat[stat.rfind(')')? + 1..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // after the ')' index 0 is `state` (field 3), so utime (14) is 11 and stime (15) is 12
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some((utime + stime) as f64 / clk_tck())
}

fn rss_kb(pid: u32) -> u64 {
    let Ok(status) = std::fs::read_to_string(format!("/proc/{pid}/status")) else {
        return 0;
    };
    status
        .lines()
        .find(|l| l.starts_with("VmRSS:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

fn fd_count(pid: u32) -> usize {
    std::fs::read_dir(format!("/proc/{pid}/fd"))
        .map(|d| d.count())
        .unwrap_or(0)
}

// ---------------------------------------------------------------- server side

/// echoes whatever it gets back, replying on the capability it was handed
///
/// identical in shape to the one in `bench.rs`: a message carrying a capability replaces
/// the cached reply channel, a message without one reuses it. that's what lets `--caps 0`
/// work at all, and what makes the caps=0 vs caps=1 comparison meaningful.
struct EchoHandler {
    cached: Mutex<Option<Ref>>,
}

impl Handler for EchoHandler {
    async fn handle(&self, data: &mut [u8], fds: FdVec, _creds: Option<tokio_seqpacket::UCred>) {
        if data == b"QUIT" {
            std::process::exit(0);
        }

        let mut fds = fds.into_iter();
        let reply = match fds.next() {
            Some(fd) => match Ref::from_owned_fd(fd) {
                Ok(r) => {
                    *self.cached.lock().unwrap() = Some(r.clone());
                    r
                }
                Err(e) => {
                    eprintln!("server: could not build a ref from the received fd: {e}");
                    return;
                }
            },
            None => match self.cached.lock().unwrap().clone() {
                Some(r) => r,
                None => return,
            },
        };
        // extra capabilities drop here, which closes the descriptors the kernel duped in
        drop(fds);

        let mut message = Message::from_data(data.to_vec());
        loop {
            match reply.send_message(message) {
                Ok(()) => break,
                Err(TrySendError::Full(m)) => {
                    message = m;
                    tokio::task::yield_now().await;
                }
                Err(TrySendError::Closed(_)) => break,
            }
        }
    }
}

async fn server_main(path: PathBuf) {
    let _node = BoundNode::bind(&path, EchoHandler {
        cached: Mutex::new(None),
    })
    .expect("server: bind failed");
    // a second listener that skips strong-ipc entirely, so --raw can measure what the
    // same kernel primitive costs at the same pace with nothing layered on top
    tokio::spawn(raw_echo(raw_path(&path)));
    // the parent drives everything and kills us when it's done
    std::future::pending::<()>().await;
}

fn raw_path(path: &Path) -> PathBuf {
    let mut p = path.as_os_str().to_owned();
    p.push(".raw");
    PathBuf::from(p)
}

/// bare seqpacket echo — no Node, no Ref, no handler trait, no fd passing
async fn raw_echo(path: PathBuf) {
    let mut listener =
        tokio_seqpacket::UnixSeqpacketListener::bind(&path).expect("server: raw bind failed");
    loop {
        let Ok(sock) = listener.accept().await else {
            return;
        };
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            loop {
                let Ok(info) = sock.recv(&mut buf).await else {
                    return;
                };
                let n = info.bytes_read();
                if n == 0 || sock.send(&buf[..n]).await.is_err() {
                    return;
                }
            }
        });
    }
}

// ---------------------------------------------------------------- client side

/// timestamps each reply the moment it lands, and forwards its sequence number
///
/// stamping here rather than in the frame loop keeps scheduling delay on the receive side
/// out of the measurement — we want the round trip, not how long the loop took to notice.
struct ReplyHandler {
    tx: tokio::sync::mpsc::UnboundedSender<(u64, Instant)>,
}

impl Handler for ReplyHandler {
    async fn handle(&self, data: &mut [u8], _fds: FdVec, _creds: Option<tokio_seqpacket::UCred>) {
        let now = Instant::now();
        if data.len() >= SEQ_BYTES {
            let seq = u64::from_le_bytes(data[..SEQ_BYTES].try_into().unwrap());
            let _ = self.tx.send((seq, now));
        }
    }
}

// ---------------------------------------------------------------- stats

/// waits for one reply, from whichever transport this run is measuring
///
/// the ipc path is already timestamped by `ReplyHandler` at arrival; the raw path has no
/// handler to stamp in, so it stamps here, immediately after the recv returns
async fn recv_reply(
    raw: Option<&tokio_seqpacket::UnixSeqpacket>,
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<(u64, Instant)>,
    buf: &mut [u8],
) -> Option<(u64, Instant)> {
    match raw {
        Some(sock) => {
            let info = tokio::time::timeout(REPLY_TIMEOUT, sock.recv(buf))
                .await
                .ok()?
                .ok()?;
            let now = Instant::now();
            if info.bytes_read() < SEQ_BYTES {
                return None;
            }
            Some((
                u64::from_le_bytes(buf[..SEQ_BYTES].try_into().unwrap()),
                now,
            ))
        }
        None => tokio::time::timeout(REPLY_TIMEOUT, rx.recv()).await.ok()?,
    }
}

struct Stats {
    p50: f64,
    p90: f64,
    p99: f64,
    p999: f64,
    max: f64,
    mean: f64,
}

/// nanosecond samples in, microsecond percentiles out
fn summarize(mut samples: Vec<u64>) -> Stats {
    samples.sort_unstable();
    let at = |q: f64| -> f64 {
        let i = ((samples.len() as f64 - 1.0) * q).round() as usize;
        samples[i] as f64 / 1000.0
    };
    Stats {
        p50: at(0.50),
        p90: at(0.90),
        p99: at(0.99),
        p999: at(0.999),
        max: *samples.last().unwrap() as f64 / 1000.0,
        mean: samples.iter().sum::<u64>() as f64 / samples.len() as f64 / 1000.0,
    }
}

fn print_stat_row(label: &str, s: &Stats) {
    println!(
        "  {label:<14} {:>9.2} {:>9.2} {:>9.2} {:>9.2} {:>9.2} {:>10.2}",
        s.p50, s.p90, s.p99, s.p999, s.max, s.mean
    );
}

fn print_stat_header(what: &str) {
    println!(
        "  {what:<14} {:>9} {:>9} {:>9} {:>9} {:>9} {:>10}",
        "p50", "p90", "p99", "p99.9", "max", "mean"
    );
}

// ---------------------------------------------------------------- args

fn arg<T: std::str::FromStr>(args: &[String], name: &str, default: T) -> T {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn flag(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

// ---------------------------------------------------------------- entry point

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if flag(&args, "--help") || flag(&args, "-h") {
        println!("usage: paced [options]");
        println!("  --hz N          wakeups per second        (default 144)");
        println!("  --burst N       messages per wakeup       (default 5)");
        println!("  --payload N     payload bytes, 8..=8192   (default 64)");
        println!("  --caps N        capabilities per message  (default 1)");
        println!("  --secs N        how long to measure       (default 10)");
        println!("  --sequential    await each reply before sending the next");
        println!("  --busy-wait     spin to the next deadline instead of sleeping");
        println!("  --raw           bare seqpacket floor, no strong-ipc (ignores --caps)");
        return;
    }

    if let Some(i) = args.iter().position(|a| a == "--server") {
        let path = PathBuf::from(&args[i + 1]);
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(server_main(path));
        return;
    }

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(parent_main(args));
}

struct Config {
    hz: f64,
    burst: usize,
    payload: usize,
    caps: usize,
    secs: f64,
    sequential: bool,
    busy_wait: bool,
    raw: bool,
}

async fn parent_main(args: Vec<String>) {
    let cfg = Config {
        hz: arg(&args, "--hz", 144.0f64),
        burst: arg(&args, "--burst", 5usize),
        payload: arg(&args, "--payload", 64usize).clamp(SEQ_BYTES, MAX_PAYLOAD),
        caps: arg(&args, "--caps", 1usize),
        secs: arg(&args, "--secs", 10.0f64),
        sequential: flag(&args, "--sequential"),
        busy_wait: flag(&args, "--busy-wait"),
        raw: flag(&args, "--raw"),
    };
    let period = Duration::from_secs_f64(1.0 / cfg.hz);
    let ticks = (cfg.secs * cfg.hz) as u64;

    let path = std::env::temp_dir().join(format!("strong-ipc-paced-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let exe = std::env::current_exe().expect("current_exe");
    let mut child = Command::new(&exe)
        .arg("--server")
        .arg(&path)
        .spawn()
        .expect("failed to spawn the server child");
    let child_pid = child.id();
    let self_pid = std::process::id();

    println!("strong-ipc paced latency — frame-loop regime");
    println!("  client pid          {self_pid}");
    println!("  server pid          {child_pid}");
    println!("  build               {}", if cfg!(debug_assertions) { "debug" } else { "release" });
    println!(
        "  pacing              {:.0} Hz × {} msg = {:.0} msg/s",
        cfg.hz,
        cfg.burst,
        cfg.hz * cfg.burst as f64
    );
    println!(
        "  frame budget        {:.2} µs",
        period.as_secs_f64() * 1e6
    );
    println!("  payload             {} B", cfg.payload);
    println!("  caps per message    {}", cfg.caps);
    println!(
        "  burst mode          {}",
        if cfg.sequential { "sequential (await each reply)" } else { "pipelined (send all, then await)" }
    );
    println!(
        "  between frames      {}",
        if cfg.busy_wait { "BUSY-WAIT (control run — never parks)" } else { "sleep (parks the process, like a real app)" }
    );

    println!(
        "  transport           {}",
        if cfg.raw { "RAW seqpacket (floor — no strong-ipc, caps ignored)" } else { "strong-ipc" }
    );

    let server = connect_with_retry(&path, &mut child).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let reply_node = Node::new(ReplyHandler { tx }).expect("failed to build the reply node");

    // the floor: same two processes, same kernel primitive, none of the library
    let raw_sock = if cfg.raw {
        Some(
            tokio_seqpacket::UnixSeqpacket::connect(raw_path(&path))
                .await
                .expect("failed to connect to the raw baseline listener"),
        )
    } else {
        None
    };
    let raw = raw_sock.as_ref();
    let mut raw_buf = vec![0u8; 65536];

    let mut payload = vec![0x41u8; cfg.payload];
    let mut seq = 0u64;
    let mut sent_at: HashMap<u64, Instant> = HashMap::new();

    // build a message carrying `caps` copies of our reply capability
    let make = |payload: &[u8], caps: usize| {
        let mut m = Message::from_data(payload.to_vec());
        for _ in 0..caps {
            m.add_ref(reply_node.get_ref());
        }
        m
    };

    // send one message, spinning while the ref's 8-slot queue is full
    let push = |m: Message| async {
        let mut m = m;
        loop {
            match server.send_message(m) {
                Ok(()) => return true,
                Err(TrySendError::Full(back)) => {
                    m = back;
                    tokio::task::yield_now().await;
                }
                Err(TrySendError::Closed(_)) => return false,
            }
        }
    };

    // ---- warmup: fault in pages, and hand the server a reply ref so caps=0 can work
    for _ in 0..200 {
        payload[..SEQ_BYTES].copy_from_slice(&seq.to_le_bytes());
        match raw {
            Some(sock) => {
                let _ = sock.send(&payload).await;
            }
            None => {
                push(make(&payload, cfg.caps.max(1))).await;
            }
        }
        let _ = recv_reply(raw, &mut rx, &mut raw_buf).await;
        seq += 1;
    }
    sent_at.clear();
    while rx.try_recv().is_ok() {}

    // ---- the frame loop
    let mut msg_ns: Vec<u64> = Vec::with_capacity(ticks as usize * cfg.burst);
    let mut burst_ns: Vec<u64> = Vec::with_capacity(ticks as usize);
    let mut wake_ns: Vec<u64> = Vec::with_capacity(ticks as usize);
    let mut late_frames = 0u64;
    let mut lost = 0u64;

    let cpu0_client = cpu_seconds(self_pid).unwrap_or(0.0);
    let cpu0_server = cpu_seconds(child_pid).unwrap_or(0.0);
    let start = Instant::now();
    let wall = Instant::now();

    for tick in 0..ticks {
        // absolute deadlines, so a slow frame doesn't push every later frame back
        let deadline = start + period.mul_f64(tick as f64);
        if cfg.busy_wait {
            while Instant::now() < deadline {
                std::hint::spin_loop();
            }
        } else if let Some(d) = deadline.checked_duration_since(Instant::now()) {
            tokio::time::sleep(d).await;
        }
        // how long after the deadline we actually got the cpu back
        let woke = Instant::now();
        wake_ns.push(woke.saturating_duration_since(deadline).as_nanos() as u64);

        let frame_start = Instant::now();
        let mut done = 0usize;

        if cfg.sequential {
            for _ in 0..cfg.burst {
                payload[..SEQ_BYTES].copy_from_slice(&seq.to_le_bytes());
                let t0 = Instant::now();
                let ok = match raw {
                    Some(sock) => sock.send(&payload).await.is_ok(),
                    None => push(make(&payload, cfg.caps)).await,
                };
                if !ok {
                    break;
                }
                seq += 1;
                match recv_reply(raw, &mut rx, &mut raw_buf).await {
                    Some((_, at)) => {
                        msg_ns.push(at.duration_since(t0).as_nanos() as u64);
                        done += 1;
                    }
                    None => lost += 1,
                }
            }
        } else {
            for _ in 0..cfg.burst {
                payload[..SEQ_BYTES].copy_from_slice(&seq.to_le_bytes());
                sent_at.insert(seq, Instant::now());
                let ok = match raw {
                    Some(sock) => sock.send(&payload).await.is_ok(),
                    None => push(make(&payload, cfg.caps)).await,
                };
                if !ok {
                    break;
                }
                seq += 1;
            }
            for _ in 0..cfg.burst {
                match recv_reply(raw, &mut rx, &mut raw_buf).await {
                    Some((got, at)) => {
                        if let Some(t0) = sent_at.remove(&got) {
                            msg_ns.push(at.duration_since(t0).as_nanos() as u64);
                            done += 1;
                        }
                    }
                    None => lost += 1,
                }
            }
        }

        let frame = frame_start.elapsed();
        burst_ns.push(frame.as_nanos() as u64);
        if frame > period {
            late_frames += 1;
        }
        if done < cfg.burst {
            lost += (cfg.burst - done) as u64;
        }
    }

    let elapsed = wall.elapsed().as_secs_f64();
    let client_cpu = cpu_seconds(self_pid).unwrap_or(0.0) - cpu0_client;
    let server_cpu = cpu_seconds(child_pid).unwrap_or(0.0) - cpu0_server;

    if msg_ns.is_empty() {
        println!("\nno replies came back — nothing to report");
    } else {
        let msgs = summarize(msg_ns);
        let bursts = summarize(burst_ns);
        let wakes = summarize(wake_ns);
        let budget_us = period.as_secs_f64() * 1e6;

        println!();
        println!("── latency (µs) {}", "─".repeat(63));
        print_stat_header("");
        print_stat_row("round trip", &msgs);
        print_stat_row("burst total", &bursts);
        print_stat_row("wake delay", &wakes);
        println!();
        println!("  'wake delay' is deadline → client running again. under --busy-wait it is");
        println!("  scheduler jitter only; otherwise it is the cost of having been parked.");

        println!();
        println!("── frame budget {}", "─".repeat(63));
        println!(
            "  budget              {budget_us:.2} µs ({:.0} Hz)",
            cfg.hz
        );
        println!(
            "  burst p50           {:>8.2} µs   {:>5.1}% of frame",
            bursts.p50,
            bursts.p50 / budget_us * 100.0
        );
        println!(
            "  burst p99           {:>8.2} µs   {:>5.1}% of frame",
            bursts.p99,
            bursts.p99 / budget_us * 100.0
        );
        println!(
            "  burst max           {:>8.2} µs   {:>5.1}% of frame",
            bursts.max,
            bursts.max / budget_us * 100.0
        );
        println!(
            "  frames over budget  {late_frames} / {ticks} ({:.2}%)",
            late_frames as f64 / ticks as f64 * 100.0
        );
        if lost > 0 {
            println!("  replies never seen  {lost}");
        }

        println!();
        println!("── cost {}", "─".repeat(71));
        let total_msgs = (ticks * cfg.burst as u64) as f64;
        println!(
            "  messages            {:.0} over {elapsed:.2} s ({:.0} msg/s)",
            total_msgs,
            total_msgs / elapsed
        );
        println!(
            "  client cpu          {:>8.4} s   {:>5.2}% of a core   {:>7.2} µs/msg",
            client_cpu,
            client_cpu / elapsed * 100.0,
            client_cpu * 1e6 / total_msgs
        );
        println!(
            "  server cpu          {:>8.4} s   {:>5.2}% of a core   {:>7.2} µs/msg",
            server_cpu,
            server_cpu / elapsed * 100.0,
            server_cpu * 1e6 / total_msgs
        );
        println!(
            "  combined            {:>8.4} s   {:>5.2}% of a core   {:>7.2} µs/msg",
            client_cpu + server_cpu,
            (client_cpu + server_cpu) / elapsed * 100.0,
            (client_cpu + server_cpu) * 1e6 / total_msgs
        );
        println!(
            "  client   rss {:>7} KiB   fds {:>4}",
            rss_kb(self_pid),
            fd_count(self_pid)
        );
        println!(
            "  server   rss {:>7} KiB   fds {:>4}",
            rss_kb(child_pid),
            fd_count(child_pid)
        );
    }

    let _ = push(Message::from_data(b"QUIT".to_vec())).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_file(&path);
    println!();
}

async fn connect_with_retry(path: &Path, child: &mut Child) -> Ref {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match Ref::connect(path).await {
            Ok(r) => return r,
            Err(e) => {
                if let Ok(Some(status)) = child.try_wait() {
                    panic!("server child exited before it could be reached: {status}");
                }
                if Instant::now() >= deadline {
                    panic!("could not connect to {} within 5 s: {e}", path.display());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}
