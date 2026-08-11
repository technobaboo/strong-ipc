//! two-process benchmark for strong-ipc
//!
//! run with `cargo run --release --example bench`. it re-execs itself as the server
//! child, so there's nothing to start by hand.
//!
//! measures, for a real cross-process round trip:
//!   - latency (p50/p90/p99/max) with and without a capability on each message
//!   - throughput (messages/s and bytes/s) at several payload sizes
//!   - cpu seconds burned on both sides, and rss/peak-rss on both sides
//!   - file descriptor pressure, sampled continuously on both processes
//!
//! the fd watchdog is the loud one: if either process crosses `FD_WARN_FRACTION` of its
//! RLIMIT_NOFILE the whole run stops immediately with a banner and a nonzero exit. pass
//! `--fd-limit N` to squeeze both processes into a small descriptor budget on purpose.

use std::{
    path::{Path, PathBuf},
    process::{Child, Command},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use strong_ipc::{BoundNode, FdVec, Handler, MAX_MESSAGE_SIZE, Message, Node, Ref, TrySendError};

/// stop the run once either side is using this much of its descriptor budget
const FD_WARN_FRACTION: f64 = 0.80;
/// how often the watchdog samples /proc
const FD_SAMPLE_INTERVAL: Duration = Duration::from_millis(5);

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

fn status_kb(pid: u32, key: &str) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status
        .lines()
        .find(|l| l.starts_with(key))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

fn rss_kb(pid: u32) -> u64 {
    status_kb(pid, "VmRSS:").unwrap_or(0)
}
fn peak_rss_kb(pid: u32) -> u64 {
    status_kb(pid, "VmHWM:").unwrap_or(0)
}

fn fd_count(pid: u32) -> usize {
    std::fs::read_dir(format!("/proc/{pid}/fd"))
        .map(|d| d.count())
        .unwrap_or(0)
}

fn nofile_limit() -> u64 {
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) };
    lim.rlim_cur
}

fn set_nofile_limit(soft: u64) {
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) };
    lim.rlim_cur = soft.min(lim.rlim_max);
    unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &lim) };
}

// ---------------------------------------------------------------- fd watchdog

struct FdWatch {
    self_pid: u32,
    child_pid: u32,
    limit: u64,
    threshold: usize,
    /// high-water marks, so a phase can report the worst it saw
    peak_self: AtomicU64,
    peak_child: AtomicU64,
    tripped: AtomicBool,
    /// when recording, every sample also logs (seconds, client rss, server rss, server fds)
    /// so a long phase can show whether usage plateaus or climbs
    timeline: Mutex<Option<(Instant, Vec<(f64, u64, u64, u64)>)>>,
}

impl FdWatch {
    fn sample(&self) {
        let (mine, theirs) = (fd_count(self.self_pid), fd_count(self.child_pid));
        self.peak_self.fetch_max(mine as u64, Ordering::Relaxed);
        self.peak_child.fetch_max(theirs as u64, Ordering::Relaxed);
        if let Ok(mut guard) = self.timeline.try_lock() {
            if let Some((start, log)) = guard.as_mut() {
                let t = start.elapsed().as_secs_f64();
                if log.last().is_none_or(|(last, ..)| t - last >= 1.0) {
                    log.push((
                        t,
                        rss_kb(self.self_pid),
                        rss_kb(self.child_pid),
                        theirs as u64,
                    ));
                }
            }
        }
        if mine >= self.threshold || theirs >= self.threshold {
            if !self.tripped.swap(true, Ordering::SeqCst) {
                self.abort(mine, theirs, "descriptor budget exceeded");
            }
        }
    }

    /// prints the banner, kills the child and leaves. deliberately does not unwind —
    /// running out of fds mid-benchmark makes every number after it a lie
    fn abort(&self, mine: usize, theirs: usize, why: &str) -> ! {
        eprintln!();
        eprintln!("{}", "!".repeat(78));
        eprintln!("!! OUT OF FILE DESCRIPTORS — BENCHMARK STOPPED");
        eprintln!("!! reason: {why}");
        eprintln!("!!");
        eprintln!(
            "!!   client (pid {:>7}): {:>7} fds open",
            self.self_pid, mine
        );
        eprintln!(
            "!!   server (pid {:>7}): {:>7} fds open",
            self.child_pid, theirs
        );
        eprintln!(
            "!!   RLIMIT_NOFILE (soft): {:>7}   stop threshold: {} ({:.0}%)",
            self.limit,
            self.threshold,
            FD_WARN_FRACTION * 100.0
        );
        eprintln!("!!");
        eprintln!("!! results printed before this point are still valid; everything the");
        eprintln!("!! run would have measured after it is discarded.");
        eprintln!("{}", "!".repeat(78));
        let _ = Command::new("kill")
            .arg("-9")
            .arg(self.child_pid.to_string())
            .status();
        std::process::exit(3);
    }

    fn reset_peaks(&self) {
        self.peak_self.store(0, Ordering::Relaxed);
        self.peak_child.store(0, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------- server side

/// echoes whatever it gets back to the client
///
/// if the message carries a capability we reply on that and cache it; if it doesn't we
/// reply on the last one we were handed. that split is the whole point: it lets the
/// benchmark separate the cost of moving bytes from the cost of moving a capability
struct EchoHandler {
    cached: Mutex<Option<Ref>>,
}

impl Handler for EchoHandler {
    async fn handle(&self, data: &mut [u8], fds: FdVec, _creds: Option<strong_ipc::UCred>) {
        if data == b"QUIT" {
            std::process::exit(0);
        }

        let mut fds = fds.into_iter();
        let reply = match fds.next() {
            Some(fd) => {
                let r = Ref::from_owned_fd(fd);
                *self.cached.lock().unwrap() = Some(r.clone());
                r
            }
            None => match self.cached.lock().unwrap().clone() {
                Some(r) => r,
                None => return,
            },
        };
        // any extra capabilities on the message are dropped here, which is what closes
        // the descriptors the kernel duped into us
        drop(fds);

        let _ = reply.send(Message::from_data(data.to_vec())).await;
    }
}

async fn server_main(path: PathBuf) {
    let _node = BoundNode::bind(&path, EchoHandler {
        cached: Mutex::new(None),
    })
    .expect("server: bind failed");
    // a second listener that skips strong-ipc entirely, so the client can measure what
    // the same kernel primitive costs with nothing layered on top
    tokio::spawn(raw_echo(raw_path(&path)));
    // the parent drives everything and kills us when it's done
    std::future::pending::<()>().await;
}

fn raw_path(path: &Path) -> PathBuf {
    let mut p = path.as_os_str().to_owned();
    p.push(".raw");
    PathBuf::from(p)
}

/// bare seqpacket echo — no Node, no Ref, no mpsc hop, no handler trait
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

struct ReplyHandler {
    /// wakes the latency loop; unbounded so the notify itself never adds backpressure
    tx: tokio::sync::mpsc::UnboundedSender<usize>,
    received: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
}

impl Handler for ReplyHandler {
    async fn handle(&self, data: &mut [u8], _fds: FdVec, _creds: Option<strong_ipc::UCred>) {
        self.received.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
        let _ = self.tx.send(data.len());
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

fn summarize(mut samples: Vec<u64>) -> Stats {
    samples.sort_unstable();
    let at = |q: f64| -> f64 {
        let i = ((samples.len() as f64 - 1.0) * q).round() as usize;
        samples[i] as f64 / 1000.0
    };
    let mean = samples.iter().sum::<u64>() as f64 / samples.len() as f64 / 1000.0;
    Stats {
        p50: at(0.50),
        p90: at(0.90),
        p99: at(0.99),
        p999: at(0.999),
        max: *samples.last().unwrap() as f64 / 1000.0,
        mean,
    }
}

/// a phase's cost on both processes, so cpu can be reported per message
struct Usage {
    client_cpu: f64,
    server_cpu: f64,
    wall: f64,
}

struct Meter {
    self_pid: u32,
    child_pid: u32,
    t0: Instant,
    c0: f64,
    s0: f64,
}

impl Meter {
    fn start(self_pid: u32, child_pid: u32) -> Self {
        Self {
            self_pid,
            child_pid,
            t0: Instant::now(),
            c0: cpu_seconds(self_pid).unwrap_or(0.0),
            s0: cpu_seconds(child_pid).unwrap_or(0.0),
        }
    }
    fn stop(self) -> Usage {
        Usage {
            client_cpu: cpu_seconds(self.self_pid).unwrap_or(0.0) - self.c0,
            server_cpu: cpu_seconds(self.child_pid).unwrap_or(0.0) - self.s0,
            wall: self.t0.elapsed().as_secs_f64(),
        }
    }
}

struct Client {
    server: Ref,
    /// kept alive so the reply node's socket stays up; the ref beside it is the
    /// capability the server actually answers on
    _reply_node: Node<ReplyHandler>,
    reply_node_ref: Ref,
    rx: tokio::sync::mpsc::UnboundedReceiver<usize>,
    received: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
    watch: Arc<FdWatch>,
}

impl Client {
    /// pushes one message, waiting if the ref's outbound queue is full
    ///
    /// tries the non-blocking path first purely so backpressure can be *counted*: a
    /// `Full` here means this send found the socket and the 8-slot queue both full. It
    /// then parks on `send` rather than spinning, so a squeezed sender costs a wakeup
    /// instead of a core
    async fn send(&self, message: Message, backpressured: &mut u64) -> bool {
        match self.server.try_send(message) {
            Ok(()) => true,
            Err(TrySendError::Closed(_)) => false,
            // the payload-ceiling phase deliberately probes past MAX_MESSAGE_SIZE
            Err(TrySendError::TooLarge(_)) => false,
            Err(TrySendError::Full(m)) => {
                *backpressured += 1;
                self.watch.sample();
                self.server.send(m).await.is_ok()
            }
        }
    }

    /// waits out anything still in flight from the previous phase
    ///
    /// a pipelined phase leaves replies arriving after it stops counting; without this
    /// the next phase reads someone else's mail
    async fn quiesce(&mut self) {
        loop {
            let before = self.received.load(Ordering::Relaxed);
            let quiet =
                tokio::time::timeout(Duration::from_millis(200), self.rx.recv())
                    .await
                    .is_err();
            while self.rx.try_recv().is_ok() {}
            if quiet && self.received.load(Ordering::Relaxed) == before {
                return;
            }
        }
    }

    fn message(&self, payload: &[u8], with_cap: usize) -> Message {
        let mut m = Message::from_data(payload.to_vec());
        for _ in 0..with_cap {
            m.add_ref(&self.reply_node_ref);
        }
        m
    }

    /// sequential ping-pong: one message in flight, so each sample is a full round trip
    async fn latency(&mut self, payload: usize, caps: usize, iters: usize) -> (Stats, Usage) {
        let data = vec![0x41u8; payload];
        // warm up so page faults and lazy allocations don't land in the samples
        for _ in 0..1000usize.min(iters) {
            let mut r = 0;
            self.send(self.message(&data, caps.max(1)), &mut r).await;
            self.rx.recv().await;
        }
        self.watch.reset_peaks();

        let mut samples = Vec::with_capacity(iters);
        let meter = Meter::start(self.watch.self_pid, self.watch.child_pid);
        let mut last_sample = Instant::now();
        for _ in 0..iters {
            let t0 = Instant::now();
            if !self.send(self.message(&data, caps), &mut 0).await {
                break;
            }
            self.rx.recv().await;
            samples.push(t0.elapsed().as_nanos() as u64);
            if last_sample.elapsed() > FD_SAMPLE_INTERVAL {
                self.watch.sample();
                last_sample = Instant::now();
            }
        }
        (summarize(samples), meter.stop())
    }

    /// pipelined: keep pushing without waiting, measure how many complete
    async fn throughput(
        &mut self,
        payload: usize,
        caps: usize,
        duration: Duration,
    ) -> (u64, u64, u64, Usage) {
        let data = vec![0x41u8; payload];
        // prime the cached reply ref, then drain anything left over from earlier phases
        let mut r = 0;
        self.send(self.message(&data, caps.max(1)), &mut r).await;
        self.rx.recv().await;
        while self.rx.try_recv().is_ok() {}
        self.received.store(0, Ordering::Relaxed);
        self.bytes.store(0, Ordering::Relaxed);
        self.watch.reset_peaks();

        let mut sent = 0u64;
        let mut retries = 0u64;
        let meter = Meter::start(self.watch.self_pid, self.watch.child_pid);
        let deadline = Instant::now() + duration;
        let mut last_sample = Instant::now();
        loop {
            if !self.send(self.message(&data, caps), &mut retries).await {
                break;
            }
            sent += 1;
            // draining as we go keeps the reply node's queue from growing without bound
            while self.rx.try_recv().is_ok() {}
            if sent % 64 == 0 {
                if Instant::now() >= deadline {
                    break;
                }
                if last_sample.elapsed() > FD_SAMPLE_INTERVAL {
                    self.watch.sample();
                    last_sample = Instant::now();
                }
            }
        }
        // let the tail land before we stop the clock
        let drain_until = Instant::now() + Duration::from_secs(2);
        while self.received.load(Ordering::Relaxed) < sent && Instant::now() < drain_until {
            let _ = tokio::time::timeout(Duration::from_millis(20), self.rx.recv()).await;
            self.watch.sample();
        }
        let usage = meter.stop();
        (
            sent,
            self.received.load(Ordering::Relaxed),
            retries,
            usage,
        )
    }
}

// ---------------------------------------------------------------- reporting

fn header(title: &str) {
    println!();
    println!("── {title} {}", "─".repeat(72usize.saturating_sub(title.len())));
}

fn fmt_bytes_per_sec(b: f64) -> String {
    const UNITS: [&str; 4] = ["B/s", "KiB/s", "MiB/s", "GiB/s"];
    let mut v = b;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    format!("{v:.1} {}", UNITS[i])
}

// ---------------------------------------------------------------- entry point

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if let Some(i) = args.iter().position(|a| a == "--fd-limit") {
        if let Some(n) = args.get(i + 1).and_then(|s| s.parse::<u64>().ok()) {
            set_nofile_limit(n);
        }
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

async fn parent_main(args: Vec<String>) {
    let fd_limit_arg = args
        .iter()
        .position(|a| a == "--fd-limit")
        .and_then(|i| args.get(i + 1).cloned());

    let path = std::env::temp_dir().join(format!("strong-ipc-bench-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let exe = std::env::current_exe().expect("current_exe");
    let mut cmd = Command::new(&exe);
    cmd.arg("--server").arg(&path);
    if let Some(n) = &fd_limit_arg {
        cmd.arg("--fd-limit").arg(n);
    }
    let mut child = cmd.spawn().expect("failed to spawn the server child");
    let child_pid = child.id();
    let self_pid = std::process::id();

    let limit = nofile_limit();
    let watch = Arc::new(FdWatch {
        self_pid,
        child_pid,
        limit,
        threshold: (limit as f64 * FD_WARN_FRACTION) as usize,
        peak_self: AtomicU64::new(0),
        peak_child: AtomicU64::new(0),
        tripped: AtomicBool::new(false),
        timeline: Mutex::new(None),
    });

    println!("strong-ipc two-process benchmark");
    println!("  client pid          {self_pid}");
    println!("  server pid          {child_pid}");
    println!("  socket              {}", path.display());
    println!(
        "  RLIMIT_NOFILE       {limit} (soft)   stop threshold {} ({:.0}%)",
        watch.threshold,
        FD_WARN_FRACTION * 100.0
    );
    println!("  build               {}", if cfg!(debug_assertions) { "debug" } else { "release" });

    // idle footprint, before either side has done any work
    tokio::time::sleep(Duration::from_millis(200)).await;
    let idle_client_rss = rss_kb(self_pid);
    let idle_server_rss = rss_kb(child_pid);
    let idle_client_fds = fd_count(self_pid);
    let idle_server_fds = fd_count(child_pid);

    let server = connect_with_retry(&path, &mut child).await;

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let received = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    let (reply_node, reply_node_ref) = Node::new(ReplyHandler {
        tx,
        received: received.clone(),
        bytes: bytes.clone(),
    })
    .expect("failed to build the reply node");

    let mut client = Client {
        server,
        _reply_node: reply_node,
        reply_node_ref,
        rx,
        received,
        bytes,
        watch: watch.clone(),
    };

    // the floor: same kernel primitive, same two processes, none of the library
    let raw = tokio_seqpacket::UnixSeqpacket::connect(raw_path(&path))
        .await
        .expect("failed to connect to the raw baseline listener");

    header("idle footprint");
    println!("  client   rss {idle_client_rss:>7} KiB   fds {idle_client_fds:>4}");
    println!("  server   rss {idle_server_rss:>7} KiB   fds {idle_server_fds:>4}");

    // ---- soak: does anything grow without bound?
    if let Some(secs) = args
        .iter()
        .position(|a| a == "--soak")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse::<u64>().ok())
    {
        header(&format!("soak — {secs} s of capability passing"));
        println!(
            "  {:>6}  {:>12} {:>12} {:>10}",
            "t (s)", "client rss", "server rss", "server fds"
        );
        *watch.timeline.lock().unwrap() = Some((Instant::now(), Vec::new()));
        let (sent, recv, _, u) = client.throughput(64, 1, Duration::from_secs(secs)).await;
        let log = watch.timeline.lock().unwrap().take().unwrap().1;
        for (t, c, s, f) in log.iter().step_by(10) {
            println!("  {t:>6.0}  {c:>8} KiB {s:>8} KiB {f:>10}");
        }
        println!(
            "  {sent} sent / {recv} returned at {:.0} msg/s",
            recv as f64 / u.wall
        );
        client.quiesce().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        println!(
            "  at rest: client {} KiB / {} fds, server {} KiB / {} fds",
            rss_kb(self_pid),
            fd_count(self_pid),
            rss_kb(child_pid),
            fd_count(child_pid)
        );
        let mut r = 0;
        client.send(Message::from_data(b"QUIT".to_vec()), &mut r).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = child.kill();
        let _ = child.wait();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(raw_path(&path));
        return;
    }

    // ---- baseline: raw seqpacket, no library
    header("baseline — raw SOCK_SEQPACKET round trip, no strong-ipc (µs)");
    println!(
        "  {:>8}  {:>8} {:>8} {:>8} {:>8} {:>8} {:>9}",
        "payload", "p50", "p90", "p99", "p99.9", "max", "mean"
    );
    let mut raw_p50 = std::collections::BTreeMap::new();
    {
        let mut rbuf = vec![0u8; 65536];
        for payload in [8usize, 1024, 8192] {
            let data = vec![0x41u8; payload];
            for _ in 0..1000 {
                raw.send(&data).await.unwrap();
                raw.recv(&mut rbuf).await.unwrap();
            }
            let mut samples = Vec::with_capacity(20_000);
            for _ in 0..20_000 {
                let t0 = Instant::now();
                raw.send(&data).await.unwrap();
                raw.recv(&mut rbuf).await.unwrap();
                samples.push(t0.elapsed().as_nanos() as u64);
            }
            let s = summarize(samples);
            raw_p50.insert(payload, s.p50);
            println!(
                "  {:>8}  {:>8.2} {:>8.2} {:>8.2} {:>8.2} {:>8.2} {:>9.2}",
                payload, s.p50, s.p90, s.p99, s.p999, s.max, s.mean
            );
        }
    }

    // ---- latency, no capability on the message
    header("round-trip latency — data only (µs, one message in flight)");
    println!(
        "  {:>8}  {:>8} {:>8} {:>8} {:>8} {:>8} {:>9}  {:>9}",
        "payload", "p50", "p90", "p99", "p99.9", "max", "mean", "cpu/msg"
    );
    for payload in [8usize, 64, 512, 1024, 4096, 8192] {
        let (s, u) = client.latency(payload, 0, 20_000).await;
        let cpu_us = (u.client_cpu + u.server_cpu) * 1e6 / 20_000.0;
        let vs_raw = match raw_p50.get(&payload) {
            Some(r) => format!("  {:+.1}µs vs raw", s.p50 - r),
            None => String::new(),
        };
        println!(
            "  {:>8}  {:>8.2} {:>8.2} {:>8.2} {:>8.2} {:>8.2} {:>9.2}  {:>7.2}µs{vs_raw}",
            payload, s.p50, s.p90, s.p99, s.p999, s.max, s.mean, cpu_us
        );
    }

    // ---- latency, one capability per message
    header("round-trip latency — one capability per message (µs)");
    println!(
        "  {:>8}  {:>8} {:>8} {:>8} {:>8} {:>8} {:>9}  {:>9}",
        "payload", "p50", "p90", "p99", "p99.9", "max", "mean", "cpu/msg"
    );
    for payload in [8usize, 1024, 8192] {
        let (s, u) = client.latency(payload, 1, 20_000).await;
        let cpu_us = (u.client_cpu + u.server_cpu) * 1e6 / 20_000.0;
        println!(
            "  {:>8}  {:>8.2} {:>8.2} {:>8.2} {:>8.2} {:>8.2} {:>9.2}  {:>7.2}µs",
            payload, s.p50, s.p90, s.p99, s.p999, s.max, s.mean, cpu_us
        );
    }

    // ---- latency, many capabilities per message
    header("round-trip latency — capability batches, 8 B payload (µs)");
    println!(
        "  {:>8}  {:>8} {:>8} {:>8} {:>8}  {:>9}  {:>9}",
        "caps/msg", "p50", "p90", "p99", "max", "mean", "cpu/msg"
    );
    for caps in [1usize, 8, 32, 128, 253] {
        let (s, u) = client.latency(8, caps, 5_000).await;
        let cpu_us = (u.client_cpu + u.server_cpu) * 1e6 / 5_000.0;
        println!(
            "  {:>8}  {:>8.2} {:>8.2} {:>8.2} {:>8.2}  {:>9.2}  {:>7.2}µs",
            caps, s.p50, s.p90, s.p99, s.max, s.mean, cpu_us
        );
        println!(
            "            peak fds during phase — client {:>6}  server {:>6}",
            watch.peak_self.load(Ordering::Relaxed),
            watch.peak_child.load(Ordering::Relaxed)
        );
    }

    // ---- throughput
    header("throughput — pipelined, 3 s per row");
    println!(
        "  {:>8} {:>5}  {:>11} {:>12}  {:>8} {:>8}  {:>9} {:>9}",
        "payload", "caps", "msg/s", "goodput", "cli cpu", "srv cpu", "backpres", "pk fds srv"
    );
    for (payload, caps) in [
        (8usize, 0usize),
        (1024, 0),
        (8192, 0),
        (8, 1),
        (1024, 1),
        (8192, 1),
    ] {
        client.quiesce().await;
        let (sent, recv, retries, u) = client.throughput(payload, caps, Duration::from_secs(3)).await;
        let rate = recv as f64 / u.wall;
        println!(
            "  {:>8} {:>5}  {:>11.0} {:>12}  {:>7.0}% {:>7.0}%  {:>8.1}% {:>10}",
            payload,
            caps,
            rate,
            fmt_bytes_per_sec(rate * payload as f64),
            u.client_cpu / u.wall * 100.0,
            u.server_cpu / u.wall * 100.0,
            retries as f64 / sent.max(1) as f64 * 100.0,
            watch.peak_child.load(Ordering::Relaxed)
        );
        if sent > recv {
            println!(
                "            note: {sent} sent, {recv} came back ({} still unaccounted for)",
                sent - recv
            );
        }
    }
    println!("  'backpres' is the share of sends that found the socket and the ref's 8-slot");
    println!("  queue both full, and had to wait for room.");

    // ---- descriptor churn under sustained capability passing
    header("descriptor churn — 30 s of capability passing at full rate");
    {
        client.quiesce().await;
        *watch.timeline.lock().unwrap() = Some((Instant::now(), Vec::new()));
        let (sent, recv, _retries, u) = client.throughput(64, 1, Duration::from_secs(30)).await;
        let log = watch.timeline.lock().unwrap().take().unwrap().1;

        println!("  messages          {sent} sent, {recv} returned");
        println!("  rate              {:.0} msg/s", recv as f64 / u.wall);
        println!();
        println!(
            "  {:>6}  {:>12} {:>12} {:>10}",
            "t (s)", "client rss", "server rss", "server fds"
        );
        for (t, c, s, f) in log.iter().step_by(3) {
            println!("  {t:>6.0}  {c:>8} KiB {s:>8} KiB {f:>10}");
        }
        println!();
        println!(
            "  peak fds          client {}   server {}",
            watch.peak_self.load(Ordering::Relaxed),
            watch.peak_child.load(Ordering::Relaxed)
        );
        // give the server a moment to finish dropping whatever was in flight
        client.quiesce().await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        println!(
            "  fds at rest       client {}   server {}",
            fd_count(self_pid),
            fd_count(child_pid)
        );
        println!(
            "  rss at rest       client {} KiB   server {} KiB",
            rss_kb(self_pid),
            rss_kb(child_pid)
        );
        println!();
        println!("  flat 'at rest' numbers mean every passed capability was reclaimed; a rising");
        println!("  server fd column means descriptors outrun the drops and the process dies.");
    }

    // ---- oversize payload behaviour
    header("payload ceiling");
    println!("  MAX_MESSAGE_SIZE is {} B. anything larger is refused at the call site, so", MAX_MESSAGE_SIZE);
    println!("  the sender finds out immediately and still holds its message — nothing is");
    println!("  truncated at the far end and nothing is lost in between.");
    for payload in [4096usize, 8192, 8193, 16384, 65536] {
        client.quiesce().await;
        let data = vec![0x41u8; payload];
        match client.server.try_send(client.message(&data, 1)) {
            Err(TrySendError::TooLarge(returned)) => {
                // the message comes back intact, so a caller can shrink it or move the
                // bulk behind a descriptor and try again
                println!(
                    "  {payload:>6} B  → refused at send, message handed back ({} B still intact)",
                    returned.data().len()
                );
                continue;
            }
            Err(e) => {
                println!("  {payload:>6} B  → send failed: {e}");
                continue;
            }
            Ok(()) => {}
        }
        match tokio::time::timeout(Duration::from_millis(500), client.rx.recv()).await {
            Ok(Some(n)) if n == payload => println!("  {payload:>6} B  → echoed intact"),
            Ok(Some(n)) => println!("  {payload:>6} B  → came back as {n} B — TRUNCATED"),
            Ok(None) => println!("  {payload:>6} B  → reply channel closed"),
            Err(_) => println!("  {payload:>6} B  → no reply within 500 ms (dropped)"),
        }
    }

    // ---- final resource summary
    header("resource summary");
    println!(
        "  client   rss {:>7} KiB   peak rss {:>7} KiB   cpu {:>6.2} s   fds {:>5}",
        rss_kb(self_pid),
        peak_rss_kb(self_pid),
        cpu_seconds(self_pid).unwrap_or(0.0),
        fd_count(self_pid)
    );
    println!(
        "  server   rss {:>7} KiB   peak rss {:>7} KiB   cpu {:>6.2} s   fds {:>5}",
        rss_kb(child_pid),
        peak_rss_kb(child_pid),
        cpu_seconds(child_pid).unwrap_or(0.0),
        fd_count(child_pid)
    );
    println!(
        "  growth   client rss +{} KiB   server rss +{} KiB   (vs idle)",
        rss_kb(self_pid) as i64 - idle_client_rss as i64,
        rss_kb(child_pid) as i64 - idle_server_rss as i64
    );

    // shut the server down politely, then make sure
    let mut r = 0;
    client.send(Message::from_data(b"QUIT".to_vec()), &mut r).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(raw_path(&path));
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
