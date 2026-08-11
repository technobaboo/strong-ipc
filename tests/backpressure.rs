//! what a sender that outruns its receiver actually sees
//!
//! the contract is that a `Ref` never blocks its caller: once the socket buffer and the
//! outbound queue are both full, `send_message` hands the message back as
//! `TrySendError::Full` rather than parking. A full queue must never be mistaken for a
//! dead peer, and nothing already accepted may be lost or reordered by the squeeze.

use std::sync::{Arc, Mutex};
use std::time::Duration;
use strong_ipc::{FdVec, Handler, Message, Node};
use tokio::sync::mpsc::error::TrySendError;

const PAYLOAD: usize = 4096;

/// refuses to return from `handle` until the gate opens, which stops the node draining
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
        let seq = u32::from_le_bytes(data[..4].try_into().unwrap());
        self.seen.lock().unwrap().push(seq);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_queue_is_reported_as_full_never_as_closed() {
    let (gate_tx, gate_rx) = tokio::sync::watch::channel(false);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let node = Node::new(Gated {
        gate: gate_rx,
        seen: seen.clone(),
    })
    .unwrap();
    let target = node.get_ref().clone();

    // no yields in this loop on purpose — yielding would let the drain task empty the
    // queue and we would never reach the state under test
    let mut accepted = 0u32;
    let mut rejected = None;
    for seq in 0..100_000u32 {
        let mut data = vec![0xABu8; PAYLOAD];
        data[..4].copy_from_slice(&seq.to_le_bytes());
        match target.send_message(Message::from_data(data)) {
            Ok(()) => accepted += 1,
            Err(TrySendError::Full(returned)) => {
                rejected = Some(returned);
                break;
            }
            Err(TrySendError::Closed(_)) => {
                panic!("a merely-full queue was reported as a dead peer at seq {seq}")
            }
        }
    }

    // the message handed back by `Full` must still be usable — that is the whole point of
    // returning it rather than dropping it
    let mut rejected =
        rejected.expect("the queue never filled, so backpressure was never exercised");
    assert!(accepted > 0, "nothing was accepted at all");

    // open the gate and let everything land, retrying the one that bounced
    gate_tx.send(true).unwrap();
    loop {
        match target.send_message(rejected) {
            Ok(()) => break,
            Err(TrySendError::Full(m)) => {
                rejected = m;
                tokio::task::yield_now().await;
            }
            Err(TrySendError::Closed(_)) => panic!("peer closed while retrying"),
        }
    }
    let total = accepted + 1;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while (seen.lock().unwrap().len() as u32) < total && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let seen = seen.lock().unwrap();
    assert_eq!(
        seen.len() as u32,
        total,
        "{total} messages were accepted but only {} arrived",
        seen.len()
    );
    // including the retried one, which must land in its original position
    let expected: Vec<u32> = (0..total).collect();
    assert_eq!(*seen, expected, "backpressure reordered accepted messages");
}
