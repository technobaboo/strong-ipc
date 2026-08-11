//! decomposes the cost of a round trip layer by layer, to separate what the kernel
//! charges for moving a descriptor from what strong-ipc charges on top
//!
//! run with `cargo run --release --example fdcost`.
//!
//! four rungs, each adding one layer:
//!   1. blocking sendmsg/recvmsg between two forked processes — no tokio, no library.
//!      this is the kernel floor, and the 0-fd/1-fd gap here is the true SCM_RIGHTS cost
//!   2. the same over tokio-seqpacket — adds the reactor and a task wakeup per direction
//!   3. strong-ipc with the received fd dropped on arrival — adds the Ref send queue and
//!      the handler dispatch, but never promotes the descriptor to a capability
//!   4. strong-ipc with the received fd promoted via `Ref::from_owned_fd` — the extra
//!      over rung 3 is exactly what building a capability costs
//!
//! rungs 3 and 4 carry an identical message over an identical socket; the *only*
//! difference is whether the receiver calls `Ref::from_owned_fd`.

use std::{
    ffi::c_void,
    mem::MaybeUninit,
    os::fd::{AsFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use strong_ipc::{BoundNode, FdVec, Handler, Message, Node, Ref};
use tokio::sync::mpsc::error::TrySendError;

const ITERS: usize = 50_000;
const WARMUP: usize = 5_000;
const PAYLOADS: [usize; 3] = [8, 1024, 8192];

/// message opcodes for the strong-ipc rungs, in byte 0
const OP_ESTABLISH: u8 = 0;
/// drop any received fd without promoting it
const OP_DROP_FD: u8 = 1;
/// promote the received fd with `Ref::from_owned_fd` and reply on it
const OP_MAKE_REF: u8 = 2;

// ---------------------------------------------------------------- stats

struct Stats {
    p50: f64,
    p90: f64,
    p99: f64,
    mean: f64,
}

fn summarize(mut s: Vec<u64>) -> Stats {
    s.sort_unstable();
    let at = |q: f64| s[((s.len() as f64 - 1.0) * q).round() as usize] as f64 / 1000.0;
    Stats {
        p50: at(0.50),
        p90: at(0.90),
        p99: at(0.99),
        mean: s.iter().sum::<u64>() as f64 / s.len() as f64 / 1000.0,
    }
}

fn header(t: &str) {
    println!();
    println!("── {t} {}", "─".repeat(70usize.saturating_sub(t.len())));
}

// ---------------------------------------------------------------- rung 1: raw syscalls

/// one blocking sendmsg + recvmsg pair, optionally carrying a descriptor
///
/// deliberately hand-rolled rather than going through any wrapper: this is the number
/// everything else gets measured against, so nothing may sit between it and the kernel
unsafe fn xchg(
    sock: RawFd,
    data: &[u8],
    recv_buf: &mut [u8],
    send_fd: Option<RawFd>,
    recv_fds: &mut usize,
) -> isize {
    unsafe {
        let mut cmsg_out = [0u8; 64];
        let mut iov = libc::iovec {
            iov_base: data.as_ptr() as *mut c_void,
            iov_len: data.len(),
        };
        let mut msg: libc::msghdr = std::mem::zeroed();
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        if let Some(fd) = send_fd {
            msg.msg_control = cmsg_out.as_mut_ptr() as *mut c_void;
            msg.msg_controllen = libc::CMSG_SPACE(4) as _;
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN(4) as _;
            std::ptr::copy_nonoverlapping(&fd, libc::CMSG_DATA(cmsg) as *mut RawFd, 1);
        }
        if libc::sendmsg(sock, &msg, 0) < 0 {
            return -1;
        }
        recv_once(sock, recv_buf, recv_fds)
    }
}

/// blocking recvmsg that closes any descriptors it is handed
///
/// closing is part of the cost and cannot be skipped — a receiver that keeps them runs
/// out within a second at these rates
unsafe fn recv_once(sock: RawFd, buf: &mut [u8], recv_fds: &mut usize) -> isize {
    unsafe {
        let mut cmsg_in = [0u8; 256];
        let mut iov = libc::iovec {
            iov_base: buf.as_mut_ptr() as *mut c_void,
            iov_len: buf.len(),
        };
        let mut msg: libc::msghdr = std::mem::zeroed();
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cmsg_in.as_mut_ptr() as *mut c_void;
        msg.msg_controllen = cmsg_in.len() as _;

        let n = libc::recvmsg(sock, &mut msg, 0);
        if n <= 0 {
            return n;
        }
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let count =
                    ((*cmsg).cmsg_len as usize - libc::CMSG_LEN(0) as usize) / size_of::<RawFd>();
                let data = libc::CMSG_DATA(cmsg) as *const RawFd;
                for i in 0..count {
                    libc::close(data.add(i).read_unaligned());
                    *recv_fds += 1;
                }
            }
            cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
        }
        n
    }
}

/// forks an echo child and times `ITERS` blocking round trips against it
fn rung_raw_syscall(payload: usize, with_fd: bool) -> (Stats, usize) {
    unsafe {
        let mut sv = [0 as RawFd; 2];
        assert_eq!(
            libc::socketpair(libc::AF_UNIX, libc::SOCK_SEQPACKET, 0, sv.as_mut_ptr()),
            0,
            "socketpair failed"
        );
        // a spare socket to hand over, so the transferred fd is the same kind of object
        // strong-ipc moves (a unix socket, not a chardev)
        let mut spare = [0 as RawFd; 2];
        assert_eq!(
            libc::socketpair(libc::AF_UNIX, libc::SOCK_SEQPACKET, 0, spare.as_mut_ptr()),
            0
        );

        let pid = libc::fork();
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            // child: echo back whatever arrives, closing any descriptors it receives
            libc::close(sv[0]);
            let mut buf = vec![0u8; 65536];
            let mut got = 0usize;
            loop {
                let n = recv_once(sv[1], &mut buf, &mut got);
                if n <= 0 {
                    break;
                }
                let mut iov = libc::iovec {
                    iov_base: buf.as_ptr() as *mut c_void,
                    iov_len: n as usize,
                };
                let mut msg: libc::msghdr = std::mem::zeroed();
                msg.msg_iov = &mut iov;
                msg.msg_iovlen = 1;
                if libc::sendmsg(sv[1], &msg, 0) < 0 {
                    break;
                }
            }
            eprintln!("      [child received and closed {got} descriptors]");
            libc::_exit(0);
        }

        libc::close(sv[1]);
        let data = vec![0x41u8; payload];
        let mut buf = vec![0u8; 65536];
        let mut sent_fds = 0usize;
        let fd = with_fd.then_some(spare[0]);

        for _ in 0..WARMUP {
            xchg(sv[0], &data, &mut buf, fd, &mut 0);
        }
        let mut samples = Vec::with_capacity(ITERS);
        for _ in 0..ITERS {
            let t0 = Instant::now();
            let n = xchg(sv[0], &data, &mut buf, fd, &mut 0);
            samples.push(t0.elapsed().as_nanos() as u64);
            assert!(n > 0, "raw exchange failed");
            if with_fd {
                sent_fds += 1;
            }
        }

        libc::close(sv[0]);
        let mut status = 0;
        libc::waitpid(pid, &mut status, 0);
        libc::close(spare[0]);
        libc::close(spare[1]);
        (summarize(samples), sent_fds)
    }
}

// ---------------------------------------------------------------- server child

struct EchoHandler {
    cached: Mutex<Option<Ref>>,
    /// counts descriptors that actually arrived, so the fd rows can be proven non-empty
    fds_seen: AtomicU64,
    refs_built: AtomicU64,
}

impl Handler for EchoHandler {
    async fn handle(&self, data: &mut [u8], fds: FdVec, _creds: Option<tokio_seqpacket::UCred>) {
        if data.starts_with(b"QUIT") {
            eprintln!(
                "      [server saw {} descriptors, promoted {} of them to Refs]",
                self.fds_seen.load(Ordering::Relaxed),
                self.refs_built.load(Ordering::Relaxed)
            );
            std::process::exit(0);
        }
        self.fds_seen.fetch_add(fds.len() as u64, Ordering::Relaxed);
        let op = data.first().copied().unwrap_or(OP_DROP_FD);
        let mut fds = fds.into_iter();

        let reply = match op {
            OP_ESTABLISH | OP_MAKE_REF => {
                let Some(fd) = fds.next() else {
                    return;
                };
                let r = Ref::from_owned_fd(fd);
                self.refs_built.fetch_add(1, Ordering::Relaxed);
                if op == OP_ESTABLISH {
                    *self.cached.lock().unwrap() = Some(r.clone());
                }
                r
            }
            // the descriptor arrived and is dropped right here without ever becoming a
            // capability — the whole point of this rung
            _ => {
                drop(fds);
                let Some(r) = self.cached.lock().unwrap().clone() else {
                    return;
                };
                r
            }
        };

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

fn raw_path(p: &Path) -> PathBuf {
    let mut s = p.as_os_str().to_owned();
    s.push(".raw");
    PathBuf::from(s)
}

async fn server_main(path: PathBuf) {
    let _node = BoundNode::bind(
        &path,
        EchoHandler {
            cached: Mutex::new(None),
            fds_seen: AtomicU64::new(0),
            refs_built: AtomicU64::new(0),
        },
    )
    .expect("bind");

    // plain tokio-seqpacket echo for rung 2, closing received fds like the raw child does
    let rp = raw_path(&path);
    tokio::spawn(async move {
        let mut listener = tokio_seqpacket::UnixSeqpacketListener::bind(&rp).expect("raw bind");
        loop {
            let Ok(sock) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buf = vec![0u8; 65536];
                let mut anc = vec![0u8; strong_ipc::EXPECTED_ANCILLARY_BUFFER_SIZE];
                loop {
                    let Ok((info, msgs)) = sock.recv_with_ancillary(&mut buf, &mut anc).await
                    else {
                        return;
                    };
                    let n = info.bytes_read();
                    if n == 0 {
                        return;
                    }
                    // dropping the iterator closes anything that came with the message
                    drop(msgs);
                    if sock.send(&buf[..n]).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    std::future::pending::<()>().await;
}

// ---------------------------------------------------------------- client rungs

struct ReplyHandler(tokio::sync::mpsc::UnboundedSender<usize>);
impl Handler for ReplyHandler {
    async fn handle(&self, data: &mut [u8], _fds: FdVec, _creds: Option<tokio_seqpacket::UCred>) {
        let _ = self.0.send(data.len());
    }
}

/// rung 2: tokio-seqpacket with no strong-ipc in the path
async fn rung_tokio_raw(
    sock: &tokio_seqpacket::UnixSeqpacket,
    spare: &OwnedFd,
    payload: usize,
    with_fd: bool,
) -> Stats {
    use tokio_seqpacket::ancillary::AncillaryMessageWriter;
    let data = vec![0x41u8; payload];
    let mut buf = vec![0u8; 65536];
    let mut anc = vec![0u8; strong_ipc::EXPECTED_ANCILLARY_BUFFER_SIZE];

    let once =
        async |sock: &tokio_seqpacket::UnixSeqpacket, buf: &mut Vec<u8>, anc: &mut Vec<u8>| {
            if with_fd {
                let mut w = AncillaryMessageWriter::new(anc);
                let borrowed = [spare.as_fd()];
                w.add_fds(&borrowed).unwrap();
                sock.send_vectored_with_ancillary(&[std::io::IoSlice::new(&data)], &mut w)
                    .await
                    .unwrap();
            } else {
                sock.send(&data).await.unwrap();
            }
            sock.recv(buf).await.unwrap();
        };

    for _ in 0..WARMUP {
        once(sock, &mut buf, &mut anc).await;
    }
    let mut samples = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let t0 = Instant::now();
        once(sock, &mut buf, &mut anc).await;
        samples.push(t0.elapsed().as_nanos() as u64);
    }
    summarize(samples)
}

/// rungs 3 and 4: identical wire traffic, differing only in the opcode that tells the
/// server whether to promote the descriptor
async fn rung_strong_ipc(
    server: &Ref,
    reply_node: &Node<ReplyHandler>,
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<usize>,
    payload: usize,
    op: u8,
    attach_fd: bool,
) -> Stats {
    let mut data = vec![0x41u8; payload.max(1)];
    data[0] = op;

    let send = async |server: &Ref| {
        let mut m = Message::from_data(data.clone());
        if attach_fd {
            m.add_ref(reply_node.get_ref());
        }
        let mut m = Some(m);
        loop {
            match server.send_message(m.take().unwrap()) {
                Ok(()) => return,
                Err(TrySendError::Full(x)) => {
                    m = Some(x);
                    tokio::task::yield_now().await;
                }
                Err(TrySendError::Closed(_)) => panic!("server closed"),
            }
        }
    };

    for _ in 0..WARMUP {
        send(server).await;
        rx.recv().await;
    }
    let mut samples = Vec::with_capacity(ITERS);
    for _ in 0..ITERS {
        let t0 = Instant::now();
        send(server).await;
        rx.recv().await;
        samples.push(t0.elapsed().as_nanos() as u64);
    }
    summarize(samples)
}

// ---------------------------------------------------------------- main

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if let Some(i) = args.iter().position(|a| a == "--server") {
        let path = PathBuf::from(&args[i + 1]);
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(server_main(path));
        return;
    }

    println!("strong-ipc round-trip cost decomposition");
    println!("  {ITERS} timed iterations per row after {WARMUP} warmup, release build, all µs");

    // rung 1 runs before any runtime exists, so fork() only ever copies one thread
    header("rung 1 — raw blocking sendmsg/recvmsg, no tokio, no library");
    println!(
        "  {:>8}  {:>8} {:>8} {:>8} {:>8}   {:>8} {:>8} {:>8} {:>8}   {:>9}",
        "", "0fd p50", "p90", "p99", "mean", "1fd p50", "p90", "p99", "mean", "Δ p50"
    );
    let mut kernel_fd_cost = Vec::new();
    for payload in PAYLOADS {
        let (a, _) = rung_raw_syscall(payload, false);
        let (b, nfds) = rung_raw_syscall(payload, true);
        assert_eq!(nfds, ITERS, "fd row did not actually transfer descriptors");
        kernel_fd_cost.push(b.p50 - a.p50);
        println!(
            "  {:>8}  {:>8.2} {:>8.2} {:>8.2} {:>8.2}   {:>8.2} {:>8.2} {:>8.2} {:>8.2}   {:>+8.2}",
            payload,
            a.p50,
            a.p90,
            a.p99,
            a.mean,
            b.p50,
            b.p90,
            b.p99,
            b.mean,
            b.p50 - a.p50
        );
    }

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async_main(args, kernel_fd_cost));
}

async fn async_main(_args: Vec<String>, kernel_fd_cost: Vec<f64>) {
    let path = std::env::temp_dir().join(format!("strong-ipc-fdcost-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(raw_path(&path));

    let exe = std::env::current_exe().unwrap();
    let mut child = Command::new(&exe)
        .arg("--server")
        .arg(&path)
        .spawn()
        .expect("spawn server");

    // a spare socket to hand over on the fd rows
    let spare: OwnedFd = {
        let mut sv = [0 as RawFd; 2];
        unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_SEQPACKET, 0, sv.as_mut_ptr()) };
        unsafe { libc::close(sv[1]) };
        unsafe { OwnedFd::from_raw_fd(sv[0]) }
    };

    let deadline = Instant::now() + Duration::from_secs(5);
    let server = loop {
        match Ref::connect(&path).await {
            Ok(r) => break r,
            Err(e) if Instant::now() >= deadline => panic!("connect: {e}"),
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    };
    let raw_sock = loop {
        match tokio_seqpacket::UnixSeqpacket::connect(raw_path(&path)).await {
            Ok(s) => break s,
            Err(e) if Instant::now() >= deadline => panic!("raw connect: {e}"),
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    };

    header("rung 2 — tokio-seqpacket, still no strong-ipc");
    println!(
        "  {:>8}  {:>8} {:>8} {:>8} {:>8}   {:>8} {:>8} {:>8} {:>8}   {:>9}",
        "", "0fd p50", "p90", "p99", "mean", "1fd p50", "p90", "p99", "mean", "Δ p50"
    );
    for payload in PAYLOADS {
        let a = rung_tokio_raw(&raw_sock, &spare, payload, false).await;
        let b = rung_tokio_raw(&raw_sock, &spare, payload, true).await;
        println!(
            "  {:>8}  {:>8.2} {:>8.2} {:>8.2} {:>8.2}   {:>8.2} {:>8.2} {:>8.2} {:>8.2}   {:>+8.2}",
            payload,
            a.p50,
            a.p90,
            a.p99,
            a.mean,
            b.p50,
            b.p90,
            b.p99,
            b.mean,
            b.p50 - a.p50
        );
    }

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let reply_node = Node::new(ReplyHandler(tx)).unwrap();

    // hand the server a reply capability once, so the later rows have somewhere to
    // answer without needing to build a Ref
    {
        let mut m = Message::from_data(vec![OP_ESTABLISH; 8]);
        m.add_ref(reply_node.get_ref());
        server.send_message(m).unwrap();
        rx.recv().await;
    }

    header("rung 3/4 — strong-ipc: descriptor dropped vs promoted to a Ref");
    println!(
        "  {:>8}  {:>9} {:>9}   {:>9} {:>9}   {:>9} {:>9}   {:>10}",
        "payload", "no fd p50", "mean", "fd drop", "mean", "fd→Ref", "mean", "Ref costs"
    );
    for payload in PAYLOADS {
        let none = rung_strong_ipc(&server, &reply_node, &mut rx, payload, OP_DROP_FD, false).await;
        let drop_ = rung_strong_ipc(&server, &reply_node, &mut rx, payload, OP_DROP_FD, true).await;
        let mkref =
            rung_strong_ipc(&server, &reply_node, &mut rx, payload, OP_MAKE_REF, true).await;
        println!(
            "  {:>8}  {:>9.2} {:>9.2}   {:>9.2} {:>9.2}   {:>9.2} {:>9.2}   {:>+9.2}",
            payload,
            none.p50,
            none.mean,
            drop_.p50,
            drop_.mean,
            mkref.p50,
            mkref.mean,
            mkref.p50 - drop_.p50
        );
    }

    header("where the time goes (8 B payload, p50)");
    {
        let k0 = rung_raw_syscall(8, false).0.p50;
        let t0 = rung_tokio_raw(&raw_sock, &spare, 8, false).await.p50;
        let s_none = rung_strong_ipc(&server, &reply_node, &mut rx, 8, OP_DROP_FD, false)
            .await
            .p50;
        let s_drop = rung_strong_ipc(&server, &reply_node, &mut rx, 8, OP_DROP_FD, true)
            .await
            .p50;
        let s_ref = rung_strong_ipc(&server, &reply_node, &mut rx, 8, OP_MAKE_REF, true)
            .await
            .p50;
        println!("  kernel round trip, no fd                {k0:>7.2} µs");
        println!(
            "  + tokio reactor                         {:>+7.2} µs   → {t0:.2}",
            t0 - k0
        );
        println!(
            "  + strong-ipc Ref queue and handler      {:>+7.2} µs   → {s_none:.2}",
            s_none - t0
        );
        println!(
            "  + one SCM_RIGHTS descriptor             {:>+7.2} µs   → {s_drop:.2}",
            s_drop - s_none
        );
        println!(
            "  + Ref::from_owned_fd on the receiver    {:>+7.2} µs   → {s_ref:.2}",
            s_ref - s_drop
        );
        println!();
        println!(
            "  kernel's own charge for the descriptor  {:>7.2} µs (rung 1, 8 B)",
            kernel_fd_cost[0]
        );
    }

    let _ = server.send_message(Message::from_data(b"QUIT".to_vec()));
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(raw_path(&path));
    let _ = spare.into_raw_fd();
    let _: Option<MaybeUninit<u8>> = None;
    println!();
}
