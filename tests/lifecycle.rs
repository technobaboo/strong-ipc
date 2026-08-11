//! the two invariants in `Ref`'s construction that nothing else covers
//!
//! both are load-bearing and both are easy to break by accident while moving code:
//!   - the outbound drain task is deliberately **not** an `AbortOnDrop`, so dropping the
//!     last `Ref` still drains whatever it already accepted
//!   - `RefInner` captures a `runtime::Handle` at construction, because the slow path is
//!     built from `send_message`, which is sync and may not be on a runtime thread

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use strong_ipc::{BoundNode, FdVec, Handler, Message, Node, Ref};
use tokio::sync::mpsc::error::TrySendError;

const PAYLOAD: usize = 4096;

fn socket_path(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("strong-ipc-life-{}-{name}.sock", std::process::id()));
    let _ = std::fs::remove_file(&p);
    p
}

struct Gated {
    gate: tokio::sync::watch::Receiver<bool>,
    seen: Arc<Mutex<Vec<u32>>>,
}

impl Handler for Gated {
    async fn handle(&self, data: &mut [u8], _fds: FdVec, _creds: Option<tokio_seqpacket::UCred>) {
        let mut gate = self.gate.clone();
        while !*gate.borrow_and_update() {
            if gate.changed().await.is_err() {
                return;
            }
        }
        self.seen
            .lock()
            .unwrap()
            .push(u32::from_le_bytes(data[..4].try_into().unwrap()));
    }
}

struct Null;
impl Handler for Null {
    async fn handle(&self, _d: &mut [u8], _f: FdVec, _c: Option<tokio_seqpacket::UCred>) {}
}

fn numbered(seq: u32) -> Message {
    let mut data = vec![0x5Au8; PAYLOAD];
    data[..4].copy_from_slice(&seq.to_le_bytes());
    Message::from_data(data)
}

/// a `Ref` whose node is gone is as dead as a closed channel
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ref_outliving_its_node_reports_closed() {
    let node = Node::new(Null).unwrap();
    let orphan = node.get_ref().clone();
    drop(node);

    // the node's recv task is aborted and its socket dropped, so the peer is genuinely
    // gone; give the runtime a moment to actually run the abort
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(
        matches!(
            orphan.send_message(Message::from_data(b"anyone there?".to_vec())),
            Err(TrySendError::Closed(_))
        ),
        "sending through a ref whose node is gone should report Closed"
    );
}

/// dropping the last `Ref` must not discard what it already accepted
///
/// the drain task is deliberately not aborted on drop: mpsc closes the channel but still
/// yields everything queued before `recv()` returns `None`. aborting instead would race
/// with in-flight sends and lose them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_the_last_ref_still_drains_the_queue() {
    let path = socket_path("drain");
    let (gate_tx, gate_rx) = tokio::sync::watch::channel(false);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let _server = BoundNode::bind(&path, Gated {
        gate: gate_rx,
        seen: seen.clone(),
    })
    .unwrap();

    // a Ref we own outright, rather than one borrowed from a Node that would keep it alive
    let client = Ref::connect(&path).await.unwrap();

    // fill the socket, then the queue, so there is a real backlog to lose
    let mut accepted = 0u32;
    for seq in 0..100_000u32 {
        match client.send_message(numbered(seq)) {
            Ok(()) => accepted += 1,
            Err(TrySendError::Full(_)) => break,
            Err(TrySendError::Closed(_)) => panic!("closed while filling"),
        }
    }
    assert!(accepted > 0, "nothing was accepted");

    // the last Ref goes away while the backlog is still queued
    drop(client);

    gate_tx.send(true).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while (seen.lock().unwrap().len() as u32) < accepted
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let seen = seen.lock().unwrap();
    assert_eq!(
        seen.len() as u32,
        accepted,
        "dropping the last Ref discarded {} of {accepted} already-accepted messages",
        accepted - seen.len() as u32
    );
    assert_eq!(*seen, (0..accepted).collect::<Vec<_>>(), "queue drained out of order");
}

/// the slow path can be built from a thread that is not a runtime thread
///
/// `send_message` is sync, so nothing stops a caller invoking it off-runtime. building the
/// outbox needs to spawn a task, which is why `RefInner` captures a `Handle` at
/// construction rather than calling `Handle::current()` at use.
///
/// that capture only covers half of it, which is why this is `#[ignore]`d: the handle is
/// used for `spawn`, but registering the duplicated socket with the reactor needs to
/// happen *inside* runtime context, and it does not. Today this panics with "there is no
/// reactor running" the moment an off-runtime sender fills the socket. The fix is to hold
/// a `runtime.enter()` guard across the registration — needed either way once the outbox
/// moves to `AsyncFd`, which has the same requirement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "panics until outbox construction enters the runtime before registering (step 4)"]
async fn the_outbox_can_be_built_from_a_non_runtime_thread() {
    let path = socket_path("offthread");
    let (gate_tx, gate_rx) = tokio::sync::watch::channel(false);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let _server = BoundNode::bind(&path, Gated {
        gate: gate_rx,
        seen: seen.clone(),
    })
    .unwrap();

    // constructed on the runtime, so it captures a handle...
    let client = Ref::connect(&path).await.unwrap();

    // ...then used from a plain OS thread with no runtime context at all. the socket fills
    // partway through, so it is this thread that ends up building the outbox.
    let worker = std::thread::spawn(move || {
        assert!(
            tokio::runtime::Handle::try_current().is_err(),
            "this thread was supposed to have no runtime context"
        );
        let mut accepted = 0u32;
        for seq in 0..100_000u32 {
            match client.send_message(numbered(seq)) {
                Ok(()) => accepted += 1,
                Err(TrySendError::Full(_)) => break,
                Err(TrySendError::Closed(_)) => panic!("closed while filling off-runtime"),
            }
        }
        accepted
    });

    let accepted = tokio::task::spawn_blocking(move || worker.join().unwrap())
        .await
        .unwrap();
    assert!(accepted > 0, "nothing was accepted from the off-runtime thread");

    gate_tx.send(true).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while (seen.lock().unwrap().len() as u32) < accepted
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let seen = seen.lock().unwrap();
    assert_eq!(
        seen.len() as u32,
        accepted,
        "messages sent from an off-runtime thread were lost"
    );
    assert_eq!(*seen, (0..accepted).collect::<Vec<_>>(), "off-runtime sends reordered");
}
