//! how many capabilities can actually ride on one message?
//!
//! `MAX_FDS` is 253, the kernel's `SCM_MAX_FD`, and it needs to mean what it says.
//!
//! It did not always. The ancillary buffer was a fixed `EXPECTED_ANCILLARY_BUFFER_SIZE`
//! — 80 bytes, enough for **16** descriptors — while the inline send path was gated on
//! `MAX_FDS`. Anything above 16 failed with `ENOSPC`, which is not `WouldBlock`, so it
//! surfaced as `TrySendError::Closed` and the caller concluded the peer was dead. The
//! receive side had the same undersized buffer, so a successful send would still have
//! come back `MSG_CTRUNC` with descriptors silently dropped.
//!
//! Now the send buffer is sized from the actual descriptor count and the receive buffer
//! is sized for `MAX_FDS` up front, so both ends of that boundary are covered here.

use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::time::Duration;
use strong_ipc::{FdVec, Handler, Message, Node};

struct CountFds {
    tx: tokio::sync::mpsc::UnboundedSender<usize>,
}

impl Handler for CountFds {
    async fn handle(&self, _data: &mut [u8], fds: FdVec, _creds: Option<strong_ipc::UCred>) {
        // dropping `fds` here is what closes the descriptors the kernel duped into us,
        // so a leak would show up as this test running out of descriptors
        let _ = self.tx.send(fds.len());
    }
}

/// `n` distinct descriptors, all naming the same open file
fn descriptors(n: usize) -> Vec<OwnedFd> {
    let file = std::fs::File::open("/dev/null").expect("open /dev/null");
    (0..n)
        .map(|_| OwnedFd::from(file.try_clone().expect("dup /dev/null")))
        .collect()
}

async fn expect_count(counts: &[usize]) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let node = Node::new(CountFds { tx }).unwrap();

    for &n in counts {
        let mut message = Message::from_data(format!("carrying {n}").into_bytes());
        for fd in descriptors(n) {
            message.add_fd(fd);
        }
        node.get_ref()
            .send_message(message)
            .unwrap_or_else(|e| panic!("send of {n} descriptors failed: {e:?}"));

        let got = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for the {n}-descriptor message"))
            .expect("channel closed");
        assert_eq!(got, n, "sent {n} descriptors, handler saw {got}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn descriptors_within_the_inline_buffer() {
    expect_count(&[0, 1, 2, 8, 15, 16]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn descriptors_above_the_inline_buffer() {
    expect_count(&[17, 32, 64, 128, 253]).await;
}

/// descriptors must not accumulate when the same `Ref` is used over and over
///
/// every received capability is duped into the receiving process by the kernel, so a
/// handler that fails to drop them exhausts the descriptor table. this is the cheap
/// always-on version of the `descriptor churn` phase in `examples/bench.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn received_descriptors_are_reclaimed() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let node = Arc::new(Node::new(CountFds { tx }).unwrap());

    let before = open_fd_count();
    for _ in 0..2_000 {
        let mut message = Message::from_data(b"churn".to_vec());
        for fd in descriptors(4) {
            message.add_fd(fd);
        }
        let mut message = Some(message);
        loop {
            match node.get_ref().send_message(message.take().unwrap()) {
                Ok(()) => break,
                Err(tokio::sync::mpsc::error::TrySendError::Full(m)) => {
                    message = Some(m);
                    tokio::task::yield_now().await;
                }
                Err(e) => panic!("send failed: {e:?}"),
            }
        }
        let _ = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
    }
    // let the last few drop
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after = open_fd_count();

    assert!(
        after < before + 64,
        "descriptors leaked: {before} open before, {after} after 2000 messages x 4 caps"
    );
}

fn open_fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd").map(|d| d.count()).unwrap_or(0)
}
