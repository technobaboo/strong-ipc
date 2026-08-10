//! per-stage latency decomposition — every section of the round trip, measured
//!
//! run with `cargo run --release --example timeline`. it re-execs itself as the server
//! child, so there's nothing to start by hand.
//!
//! `paced.rs` and `bench.rs` both measure the round trip end to end, so the only way to
//! attribute cost to a stage is to diff two whole configurations against each other. that
//! gives you a number per stage but never a *distribution* per stage — and the interesting
//! question (is this stage constant-cost, or is it where the variance comes from?) needs
//! the distribution.
//!
//! so this one stamps the clock at four points and carries the stamps in the payload:
//!
//! ```text
//!   t0 ──► t0b ─────────► t1 ─────► t2 ─────────────► t3
//!   │      │              │         │                 │
//!   │      │              │         │                 client ReplyHandler entry
//!   │      │              │         server, just before send_message
//!   │      │              server, Handler::handle entry
//!   │      client, just after send_message returned
//!   client, just before send_message
//! ```
//!
//! which cuts the round trip into four sections that sum to the whole:
//!
//!   A  t0 →t0b  the client's own send: cmsg build + `sendmsg`
//!   B  t0b→t1   transit, server wakeup, `recvmsg`, cmsg parse, dispatch to the handler
//!   C  t1 →t2   the handler itself: `Ref::from_owned_fd`, the reply `Message`
//!   D  t2 →t3   the server's send, transit back, client wakeup, dispatch to our handler
//!
//! `CLOCK_MONOTONIC` is system-wide on linux, so t1/t2 taken in the child are directly
//! comparable with t0/t3 taken in the parent — no clock-offset correction needed.
//!
//! caveats worth knowing before reading the output:
//!   - the received capability's own `close()` happens when the handler's `Ref` drops, at
//!     the *end* of `handle`, which is after t2 — so it lands in section D, not C
//!   - t3 is handler entry, so D includes the client's reactor wakeup and task poll
//!
//! options:
//!   --hz N        wakeups per second       (default 144)
//!   --burst N     messages per wakeup      (default 5)
//!   --payload N   payload bytes, 32..=8192 (default 64)
//!   --caps N      capabilities per message (default 1)
//!   --secs N      how long to measure      (default 10)
//!   --busy-wait   spin to the next deadline instead of sleeping
//!   --threads N   worker threads on both sides; 1 = current_thread (default 1)
//!   --dump PATH   write `seq,a_ns,b_ns,c_ns,d_ns,total_ns` per message

use std::{
    path::{Path, PathBuf},
    process::{Child, Command},
    sync::Mutex,
    time::{Duration, Instant},
};
use strong_ipc::{BoundNode, FdVec, Handler, Message, Node, Ref};
use tokio::sync::mpsc::error::TrySendError;

/// recv_loop's read buffer in lib.rs; payloads above this get truncated by the kernel
const MAX_PAYLOAD: usize = 8192;
/// seq, then the two server stamps, all little-endian u64
const HDR: usize = 24;
/// give up on a reply that never comes back, rather than hanging the run
const REPLY_TIMEOUT: Duration = Duration::from_secs(1);

/// raw `CLOCK_MONOTONIC` nanoseconds
///
/// deliberately not `Instant`, which can't be put on the wire — and the whole point here
/// is comparing a stamp taken in one process against a stamp taken in the other
fn now_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: ts is a live, correctly-typed timespec for the duration of the call
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

fn get(data: &[u8], slot: usize) -> u64 {
    u64::from_le_bytes(data[slot * 8..slot * 8 + 8].try_into().unwrap())
}
fn put(data: &mut [u8], slot: usize, v: u64) {
    data[slot * 8..slot * 8 + 8].copy_from_slice(&v.to_le_bytes());
}

// ---------------------------------------------------------------- server side

/// echoes the payload back, stamping handler entry and pre-send into it
///
/// same shape as the handler in `paced.rs` — a message carrying a capability replaces the
/// cached reply channel, one without reuses it — so the numbers stay comparable
struct EchoHandler {
    cached: Mutex<Option<Ref>>,
}

impl Handler for EchoHandler {
    async fn handle(&self, data: &mut [u8], fds: FdVec, _creds: Option<tokio_seqpacket::UCred>) {
        // first thing, before any of our own work lands in the measurement
        let t1 = now_ns();

        if data.starts_with(b"QUIT") {
            std::process::exit(0);
        }
        if data.len() < HDR {
            return;
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

        let mut out = data.to_vec();
        put(&mut out, 1, t1);
        // last thing before handing it back, so section C is handler work and nothing else
        put(&mut out, 2, now_ns());

        let mut message = Message::from_data(out);
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
    // the parent drives everything and kills us when it's done
    std::future::pending::<()>().await;
}

// ---------------------------------------------------------------- client side

/// stamps t3 the instant a reply lands, and forwards the server's stamps with it
struct ReplyHandler {
    tx: tokio::sync::mpsc::UnboundedSender<(u64, u64, u64, u64)>,
}

impl Handler for ReplyHandler {
    async fn handle(&self, data: &mut [u8], _fds: FdVec, _creds: Option<tokio_seqpacket::UCred>) {
        let t3 = now_ns();
        if data.len() >= HDR {
            let _ = self
                .tx
                .send((get(data, 0), get(data, 1), get(data, 2), t3));
        }
    }
}

// ---------------------------------------------------------------- stats

struct Stats {
    p50: f64,
    p90: f64,
    p99: f64,
    max: f64,
    mean: f64,
}

/// nanosecond samples in, microsecond percentiles out
fn summarize(samples: &[u64]) -> Stats {
    let mut s = samples.to_vec();
    s.sort_unstable();
    let at = |q: f64| -> f64 {
        let i = ((s.len() as f64 - 1.0) * q).round() as usize;
        s[i] as f64 / 1000.0
    };
    Stats {
        p50: at(0.50),
        p90: at(0.90),
        p99: at(0.99),
        max: *s.last().unwrap() as f64 / 1000.0,
        mean: s.iter().sum::<u64>() as f64 / s.len() as f64 / 1000.0,
    }
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

/// both sides get the same runtime shape, so a comparison isolates the runtime and not
/// some asymmetry between client and server
fn runtime(threads: usize) -> tokio::runtime::Runtime {
    if threads <= 1 {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    } else {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .enable_all()
            .build()
            .unwrap()
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let threads: usize = arg(&args, "--threads", 1usize);

    if let Some(i) = args.iter().position(|a| a == "--server") {
        let path = PathBuf::from(&args[i + 1]);
        runtime(threads).block_on(server_main(path));
        return;
    }

    runtime(threads).block_on(parent_main(args));
}

async fn parent_main(args: Vec<String>) {
    let hz: f64 = arg(&args, "--hz", 144.0);
    let burst: usize = arg(&args, "--burst", 5);
    let payload: usize = arg(&args, "--payload", 64usize).clamp(HDR, MAX_PAYLOAD);
    let caps: usize = arg(&args, "--caps", 1);
    let secs: f64 = arg(&args, "--secs", 10.0);
    let busy_wait = flag(&args, "--busy-wait");

    let period = Duration::from_secs_f64(1.0 / hz);
    let ticks = (secs * hz) as u64;

    let path = std::env::temp_dir().join(format!("strong-ipc-tl-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let exe = std::env::current_exe().expect("current_exe");
    let threads: usize = arg(&args, "--threads", 1usize);
    let mut child = Command::new(&exe)
        .arg("--server")
        .arg(&path)
        .arg("--threads")
        .arg(threads.to_string())
        .spawn()
        .expect("failed to spawn the server child");

    println!("strong-ipc per-stage timeline");
    println!(
        "  pacing              {hz:.0} Hz x {burst} msg = {:.0} msg/s",
        hz * burst as f64
    );
    println!("  payload             {payload} B");
    println!("  caps per message    {caps}");
    println!(
        "  between frames      {}",
        if busy_wait { "BUSY-WAIT (never parks)" } else { "sleep (parks, like a real app)" }
    );
    println!(
        "  runtime             {}",
        if threads <= 1 { "current_thread".to_string() } else { format!("multi_thread, {threads} workers") }
    );

    let server = connect_with_retry(&path, &mut child).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let reply_node = Node::new(ReplyHandler { tx }).expect("failed to build the reply node");

    let mut buf = vec![0x41u8; payload];
    let mut seq = 0u64;

    let make = |payload: &[u8], caps: usize| {
        let mut m = Message::from_data(payload.to_vec());
        for _ in 0..caps {
            m.add_ref(reply_node.get_ref());
        }
        m
    };

    // ---- warmup: fault in pages, and hand the server a reply ref so caps=0 can work
    for _ in 0..500 {
        put(&mut buf, 0, seq);
        let mut m = make(&buf, caps.max(1));
        while let Err(TrySendError::Full(back)) = server.send_message(m) {
            m = back;
            tokio::task::yield_now().await;
        }
        let _ = tokio::time::timeout(REPLY_TIMEOUT, rx.recv()).await;
        seq += 1;
    }
    while rx.try_recv().is_ok() {}

    // ---- the frame loop, strictly sequential so one message is in flight at a time
    let (mut a, mut b, mut c, mut d, mut total) = (vec![], vec![], vec![], vec![], vec![]);
    let mut seqs: Vec<u64> = Vec::new();
    let mut lost = 0u64;
    let start = Instant::now();

    for tick in 0..ticks {
        // absolute deadlines, so a slow frame doesn't push every later frame back
        let deadline = start + period.mul_f64(tick as f64);
        if busy_wait {
            while Instant::now() < deadline {
                std::hint::spin_loop();
            }
        } else if let Some(w) = deadline.checked_duration_since(Instant::now()) {
            tokio::time::sleep(w).await;
        }

        for _ in 0..burst {
            put(&mut buf, 0, seq);

            let t0 = now_ns();
            let mut m = make(&buf, caps);
            let mut ok = true;
            loop {
                match server.send_message(m) {
                    Ok(()) => break,
                    Err(TrySendError::Full(back)) => {
                        m = back;
                        tokio::task::yield_now().await;
                    }
                    Err(TrySendError::Closed(_)) => {
                        ok = false;
                        break;
                    }
                }
            }
            if !ok {
                break;
            }
            let t0b = now_ns();

            match tokio::time::timeout(REPLY_TIMEOUT, rx.recv()).await {
                Ok(Some((got, t1, t2, t3))) if got == seq => {
                    // a stamp out of order means the two processes disagree about the
                    // clock, which would make every section below meaningless
                    if t0 <= t0b && t0b <= t1 && t1 <= t2 && t2 <= t3 {
                        a.push(t0b - t0);
                        b.push(t1 - t0b);
                        c.push(t2 - t1);
                        d.push(t3 - t2);
                        total.push(t3 - t0);
                        seqs.push(got);
                    } else {
                        lost += 1;
                    }
                }
                _ => lost += 1,
            }
            seq += 1;
        }
    }

    if total.is_empty() {
        println!("\nno replies came back — nothing to report");
    } else {
        if let Some(i) = args.iter().position(|a| a == "--dump") {
            if let Some(p) = args.get(i + 1) {
                let mut out = String::from("seq,a_ns,b_ns,c_ns,d_ns,total_ns\n");
                for i in 0..total.len() {
                    out.push_str(&format!(
                        "{},{},{},{},{},{}\n",
                        seqs[i], a[i], b[i], c[i], d[i], total[i]
                    ));
                }
                match std::fs::write(p, out) {
                    Ok(()) => println!("  samples             written to {p}"),
                    Err(e) => eprintln!("  could not write {p}: {e}"),
                }
            }
        }

        println!();
        println!("── per-stage latency (µs, n={}) {}", total.len(), "─".repeat(30));
        println!(
            "  {:<44} {:>8} {:>8} {:>8} {:>8} {:>9}",
            "", "p50", "p90", "p99", "max", "mean"
        );
        let t = summarize(&total);
        for (label, samples) in [
            ("A  client send: cmsg build + sendmsg", &a),
            ("B  transit, server wake, recv, parse, dispatch", &b),
            ("C  server handler: Ref::from_owned_fd + build", &c),
            ("D  server send, transit back, client wake+disp", &d),
        ] {
            let s = summarize(samples);
            println!(
                "  {label:<44} {:>8.2} {:>8.2} {:>8.2} {:>8.2} {:>9.2}   {:>5.1}% of p50",
                s.p50,
                s.p90,
                s.p99,
                s.max,
                s.mean,
                s.p50 / t.p50 * 100.0
            );
        }
        println!(
            "  {:<44} {:>8.2} {:>8.2} {:>8.2} {:>8.2} {:>9.2}",
            "   round trip (A+B+C+D)", t.p50, t.p90, t.p99, t.max, t.mean
        );
        if lost > 0 {
            println!("  discarded (timeout or non-monotonic stamps): {lost}");
        }
    }

    let mut m = Message::from_data(b"QUIT".to_vec());
    for _ in 0..caps {
        m.add_ref(reply_node.get_ref());
    }
    let _ = server.send_message(m);
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
