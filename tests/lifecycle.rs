//! who outlives whom, and how each side finds out
//!
//! the construction invariants nothing else covers — both load-bearing, both easy to
//! break by accident while moving code:
//!   - the outbound drain task is deliberately **not** an `AbortOnDrop`, so dropping the
//!     last `Ref` still drains whatever it already accepted
//!   - `RefInner` captures a `runtime::Handle` at construction, because the outbox is
//!     built from `try_send`, which is sync and may not be on a runtime thread
//!
//! and death detection in both directions: a `Ref` learning its node is gone, and a
//! `Node` learning the last `Ref` to it is. The second only works because `Node::new`
//! hands the capability back rather than keeping one.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use strong_ipc::{BoundNode, FdVec, Handler, Message, Node, Ref, TrySendError};

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
    async fn handle(&self, data: &mut [u8], _fds: FdVec, _creds: Option<strong_ipc::UCred>) {
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
    async fn handle(&self, _d: &mut [u8], _f: FdVec, _c: Option<strong_ipc::UCred>) {}
}

fn numbered(seq: u32) -> Message {
    let mut data = vec![0x5Au8; PAYLOAD];
    data[..4].copy_from_slice(&seq.to_le_bytes());
    Message::from_data(data)
}

/// a `Ref` whose node is gone is as dead as a closed channel
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ref_outliving_its_node_reports_closed() {
    let (node, node_ref) = Node::new(Null).unwrap();
    let orphan = node_ref.clone();
    drop(node);

    // the node's recv task is aborted and its socket dropped, so the peer is genuinely
    // gone; give the runtime a moment to actually run the abort
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(
        matches!(
            orphan.try_send(Message::from_data(b"anyone there?".to_vec())),
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
        match client.try_send(numbered(seq)) {
            Ok(()) => accepted += 1,
            Err(TrySendError::Full(_)) => break,
            Err(TrySendError::Closed(_)) => panic!("closed while filling"),
            Err(TrySendError::TooLarge(_)) => unreachable!("fixed {PAYLOAD} B payload"),
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

/// the outbox can be built from a thread that is not a runtime thread
///
/// `try_send` is sync, so nothing stops a caller invoking it off-runtime. Building the
/// outbox needs to spawn a task, which is why `RefInner` captures a `Handle` at
/// construction rather than calling `Handle::current()` at use.
///
/// that capture is necessary but not sufficient, which is what this test caught: the
/// handle covers `spawn`, but registering the duplicated socket with the reactor needs
/// runtime *context*, and holding a handle is not the same as being entered into it.
/// Without the `enter()` guard in `Outbox::build` this panics with "there is no reactor
/// running" the moment an off-runtime sender fills its socket.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
            match client.try_send(numbered(seq)) {
                Ok(()) => accepted += 1,
                Err(TrySendError::Full(_)) => break,
                Err(TrySendError::Closed(_)) => panic!("closed while filling off-runtime"),
                Err(TrySendError::TooLarge(_)) => unreachable!("fixed {PAYLOAD} B payload"),
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

/// death detection on a `Ref`: the same condition `try_send` reports as `Closed`, asked
/// without a message to lose
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ref_notices_its_node_going_away() {
    let (node, node_ref) = Node::new(Null).unwrap();
    let orphan = node_ref.clone();

    assert!(!orphan.is_dead(), "a live node's ref reported dead");
    // and the notification does not fire early
    assert!(
        tokio::time::timeout(Duration::from_millis(100), orphan.death_notification())
            .await
            .is_err(),
        "death fired while the node was still alive"
    );

    let notified = tokio::spawn({
        let orphan = orphan.clone();
        async move { orphan.death_notification().await }
    });
    drop(node);

    tokio::time::timeout(Duration::from_secs(5), notified)
        .await
        .expect("death_notification never resolved after the node was dropped")
        .unwrap();
    assert!(orphan.is_dead(), "is_dead disagrees with death_notification");

    // the whole point: this is knowable before a send fails
    assert!(matches!(
        orphan.try_send(Message::from_data(b"?".to_vec())),
        Err(TrySendError::Closed(_))
    ));
}

/// a ref taken from a `BoundNode` sees the server go, not just a peer socketpair
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connected_ref_notices_the_server_going_away() {
    let path = socket_path("death");
    let server = BoundNode::bind(&path, Null).unwrap();
    let client = Ref::connect(&path).await.unwrap();
    assert!(!client.is_dead());

    drop(server);
    tokio::time::timeout(Duration::from_secs(5), client.death_notification())
        .await
        .expect("client ref never noticed the bound node going away");
    assert!(client.is_dead());
}

/// the reason [`Node::new`] hands the `Ref` back instead of keeping one
///
/// a node holding a capability to itself would pin its own socket open, so "nobody can
/// reach me any more" would be unobservable from the inside. Split, dropping the last
/// `Ref` really does hang the node's socket up, and its receive loop sees it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_notices_its_last_ref_going_away() {
    let (node, node_ref) = Node::new(Null).unwrap();
    let second = node_ref.clone();

    assert!(!node.is_dead(), "a fresh node reported dead");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), node.death_notification())
            .await
            .is_err(),
        "a live node's death fired"
    );

    // one of two refs going is not the last word
    drop(node_ref);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), node.death_notification())
            .await
            .is_err(),
        "the node gave up while a Ref to it was still alive"
    );
    assert!(!node.is_dead());

    // the last one is
    drop(second);
    tokio::time::timeout(Duration::from_secs(5), node.death_notification())
        .await
        .expect("the node never noticed its last Ref going away");
    assert!(node.is_dead(), "is_dead disagrees with death_notification");
}

/// the node also gives up when its loop ends for a reason that is not a hangup
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_reports_a_receive_loop_that_stopped_on_its_own() {
    let (node, node_ref) = Node::new(Null).unwrap();

    // an empty message is a zero-length read, indistinguishable from a hangup, so the
    // loop ends while the Ref that sent it is still very much alive
    node_ref.send(Message::from_data(Vec::new())).await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), node.death_notification())
        .await
        .expect("the node never reported its receive loop stopping");
    assert!(node.is_dead());
    // and it propagates: the loop ending drops the node's socket, so the still-live Ref
    // on the other end hangs up too. the tombstone is declared after that drop, so by the
    // time the node reports dead the Ref already agrees — no race between the two
    assert!(
        node_ref.is_dead(),
        "a node that stopped receiving left its Refs thinking they could still reach it"
    );
}
