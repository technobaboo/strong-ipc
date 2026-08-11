//! correctness check for the inline fast path in `Ref::send_message`
//!
//! the fast path lets a message skip the queue and go straight to the kernel. that is
//! only sound if a message can never overtake one already waiting in the queue, so this
//! deliberately drives the socket into backpressure — the sender outruns the receiver,
//! the buffer fills, sends start falling back to the queue, and then the buffer drains
//! and they start going inline again. every crossing between the two paths is a chance
//! to reorder.
//!
//! run with `cargo run --release --example ordering`. exits nonzero on any violation.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;
use strong_ipc::{FdVec, Handler, Message, Node};
use tokio::sync::mpsc::error::TrySendError;

const MESSAGES: u32 = 500_000;
/// big enough that the socket buffer fills quickly and the queue path gets used
const PAYLOAD: usize = 4096;

struct SeqChecker {
    next_expected: AtomicU64,
    out_of_order: AtomicU64,
    first_violation: std::sync::Mutex<Option<(u64, u64)>>,
    received: AtomicU64,
    fds_seen: AtomicU64,
    payload_corrupt: AtomicU64,
}

impl Handler for SeqChecker {
    async fn handle(&self, data: &mut [u8], fds: FdVec, _creds: Option<strong_ipc::UCred>) {
        self.received.fetch_add(1, Ordering::Relaxed);
        self.fds_seen.fetch_add(fds.len() as u64, Ordering::Relaxed);

        let seq = u32::from_le_bytes(data[..4].try_into().unwrap()) as u64;
        let expected = self.next_expected.swap(seq + 1, Ordering::Relaxed);
        if seq != expected {
            self.out_of_order.fetch_add(1, Ordering::Relaxed);
            let mut first = self.first_violation.lock().unwrap();
            if first.is_none() {
                *first = Some((expected, seq));
            }
        }
        // the tail of the payload is derived from the sequence number, so a message that
        // got its ancillary data or its body crossed with another's shows up here
        let tag = (seq as u8).wrapping_mul(31);
        if data[4..].iter().any(|b| *b != tag) {
            self.payload_corrupt.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// multi-threaded on purpose
///
/// on a current_thread runtime the drain task empties the whole channel before it yields
/// back to the sender, so the queue is almost never non-empty at the moment a send is
/// attempted and the race simply cannot appear. it takes real parallelism between the
/// sender and the drain task to expose it
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let checker = Arc::new(SeqChecker {
        next_expected: AtomicU64::new(0),
        out_of_order: AtomicU64::new(0),
        first_violation: std::sync::Mutex::new(None),
        received: AtomicU64::new(0),
        fds_seen: AtomicU64::new(0),
        payload_corrupt: AtomicU64::new(0),
    });
    let node = Node::new_raw(checker.clone()).unwrap();
    let target = node.get_ref().clone();

    // a second node just to have a capability worth attaching to every message
    let cap_node = Node::new(NullHandler).unwrap();

    println!("sending {MESSAGES} sequenced messages of {PAYLOAD} B, one capability each");
    println!("(sender deliberately outruns the receiver so both send paths get used)");

    let mut queue_full = 0u64;
    for seq in 0..MESSAGES {
        let tag = (seq as u8).wrapping_mul(31);
        let mut data = vec![tag; PAYLOAD];
        data[..4].copy_from_slice(&seq.to_le_bytes());

        let mut message = Message::from_data(data);
        message.add_ref(cap_node.get_ref());

        loop {
            match target.send_message(message) {
                Ok(()) => break,
                Err(TrySendError::Full(m)) => {
                    // the queue backed up too — yield so the drain task and the receiver
                    // both get to run, then try again
                    message = m;
                    queue_full += 1;
                    tokio::task::yield_now().await;
                }
                Err(TrySendError::Closed(_)) => panic!("target ref closed at seq {seq}"),
            }
        }
        if seq % 1024 == 0 {
            tokio::task::yield_now().await;
        }
    }

    // let the tail arrive
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while checker.received.load(Ordering::Relaxed) < MESSAGES as u64
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let received = checker.received.load(Ordering::Relaxed);
    let out_of_order = checker.out_of_order.load(Ordering::Relaxed);
    let corrupt = checker.payload_corrupt.load(Ordering::Relaxed);
    let fds = checker.fds_seen.load(Ordering::Relaxed);

    println!();
    println!("  messages sent        {MESSAGES}");
    println!("  messages received    {received}");
    println!("  capabilities received {fds}");
    println!("  queue-full retries   {queue_full}");
    println!("  out of order         {out_of_order}");
    println!("  corrupt payloads     {corrupt}");
    if let Some((expected, got)) = *checker.first_violation.lock().unwrap() {
        println!("  first violation      expected {expected}, got {got}");
    }

    println!();
    if queue_full == 0 {
        println!("  WARNING: the queue never filled, so the fallback path was barely exercised.");
        println!("  this run does not prove much — raise MESSAGES or PAYLOAD.");
    }

    let ok = received == MESSAGES as u64 && out_of_order == 0 && corrupt == 0 && fds == received;
    if ok {
        println!("  PASS — strict ordering held, every capability arrived, no corruption");
    } else {
        println!("  FAIL");
        std::process::exit(1);
    }
}

struct NullHandler;
impl Handler for NullHandler {
    async fn handle(&self, _d: &mut [u8], _f: FdVec, _c: Option<strong_ipc::UCred>) {}
}
