//! strict ordering across the inline-fast-path / queue boundary
//!
//! `Ref::try_send` lets a message skip the outbound queue and go straight to the
//! kernel. that is only sound if a message can never overtake one already waiting in the
//! queue, so this deliberately drives the socket into backpressure: the sender outruns the
//! receiver, the buffer fills, sends start falling back to the queue, then the buffer
//! drains and they start going inline again. every crossing between the two paths is a
//! chance to reorder.
//!
//! this is the fast, always-run version of `examples/ordering.rs`, which does the same
//! thing at 500 000 messages as a soak.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;
use strong_ipc::{FdVec, Handler, Message, Node, TrySendError};

const MESSAGES: u32 = 50_000;
/// big enough that the socket buffer fills quickly and the queue path gets used
const PAYLOAD: usize = 4096;

struct SeqChecker {
    next_expected: AtomicU64,
    out_of_order: AtomicU64,
    received: AtomicU64,
    fds_seen: AtomicU64,
    payload_corrupt: AtomicU64,
}

impl Handler for SeqChecker {
    async fn handle(&self, data: &mut [u8], fds: FdVec, _creds: Option<strong_ipc::UCred>) {
        self.received.fetch_add(1, Ordering::Relaxed);
        self.fds_seen.fetch_add(fds.len() as u64, Ordering::Relaxed);

        let seq = u32::from_le_bytes(data[..4].try_into().unwrap()) as u64;
        if seq != self.next_expected.swap(seq + 1, Ordering::Relaxed) {
            self.out_of_order.fetch_add(1, Ordering::Relaxed);
        }
        // the tail of the payload is derived from the sequence number, so a message whose
        // body or ancillary data got crossed with another's shows up here
        let tag = (seq as u8).wrapping_mul(31);
        if data[4..].iter().any(|b| *b != tag) {
            self.payload_corrupt.fetch_add(1, Ordering::Relaxed);
        }
    }
}

struct NullHandler;
impl Handler for NullHandler {
    async fn handle(&self, _d: &mut [u8], _f: FdVec, _c: Option<strong_ipc::UCred>) {}
}

/// multi-threaded on purpose
///
/// on a current_thread runtime the drain task empties the whole channel before it yields
/// back to the sender, so the queue is almost never non-empty at the moment a send is
/// attempted and the race simply cannot appear. it takes real parallelism between the
/// sender and the drain task to expose it
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_ordering_across_the_fast_path_boundary() {
    let checker = Arc::new(SeqChecker {
        next_expected: AtomicU64::new(0),
        out_of_order: AtomicU64::new(0),
        received: AtomicU64::new(0),
        fds_seen: AtomicU64::new(0),
        payload_corrupt: AtomicU64::new(0),
    });
    let (_node, node_ref) = Node::new_raw(checker.clone()).unwrap();
    let target = node_ref.clone();
    // a second node just to have a capability worth attaching to every message
    let (_cap_node, cap_node_ref) = Node::new(NullHandler).unwrap();

    let mut queue_full = 0u64;
    for seq in 0..MESSAGES {
        let tag = (seq as u8).wrapping_mul(31);
        let mut data = vec![tag; PAYLOAD];
        data[..4].copy_from_slice(&seq.to_le_bytes());

        let mut message = Message::from_data(data);
        message.add_ref(&cap_node_ref);

        loop {
            match target.try_send(message) {
                Ok(()) => break,
                Err(TrySendError::Full(m)) => {
                    message = m;
                    queue_full += 1;
                    tokio::task::yield_now().await;
                }
                Err(TrySendError::Closed(_)) => panic!("target ref closed at seq {seq}"),
                Err(TrySendError::TooLarge(_)) => {
                    unreachable!("payload is a fixed {PAYLOAD} B, under the limit")
                }
            }
        }
        if seq % 1024 == 0 {
            tokio::task::yield_now().await;
        }
    }

    // let the tail arrive
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while checker.received.load(Ordering::Relaxed) < MESSAGES as u64
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let received = checker.received.load(Ordering::Relaxed);

    // without this the whole test can pass vacuously: if the queue never filled, every
    // send took the inline path, no crossing ever happened, and ordering was never at risk
    assert!(
        queue_full > 0,
        "the outbound queue never filled, so the fallback path was never exercised and \
         this test proves nothing — raise MESSAGES or PAYLOAD"
    );
    assert_eq!(received, MESSAGES as u64, "not every message arrived");
    assert_eq!(
        checker.out_of_order.load(Ordering::Relaxed),
        0,
        "messages arrived out of order"
    );
    assert_eq!(
        checker.payload_corrupt.load(Ordering::Relaxed),
        0,
        "payload corrupted"
    );
    assert_eq!(
        checker.fds_seen.load(Ordering::Relaxed),
        received,
        "expected exactly one capability per message"
    );
}
