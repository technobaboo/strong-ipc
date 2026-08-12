//! a capability handed over twice is one capability
//!
//! `SCM_RIGHTS` installs a fresh descriptor number on every crossing, so nothing about
//! the descriptor a node receives says which capability it is. The socket underneath it
//! does: `fstat` gives every descriptor naming one socket the same `(dev, ino)`, and
//! that is what `Ref::from_owned_fd` looks up.
//!
//! what this buys is not the allocation. It is that the expensive per-`Ref` machinery —
//! the outbox, the reactor registration behind `death_notification` — belongs to the
//! peer rather than to the arrival, so a node handed the same capability repeatedly
//! builds it once.

use std::time::Duration;
use strong_ipc::{FdVec, Handler, Message, Node, Ref};
use tokio::sync::mpsc;

/// turns every received capability into a `Ref` and forwards it for inspection
struct Collect {
	tx: mpsc::UnboundedSender<Vec<Ref>>,
}

impl Handler for Collect {
	async fn handle(&self, _data: &mut [u8], fds: FdVec, _creds: Option<strong_ipc::UCred>) {
		let _ = self
			.tx
			.send(fds.into_iter().map(Ref::from_owned_fd).collect());
	}
}

fn collector() -> (Node<Collect>, Ref, mpsc::UnboundedReceiver<Vec<Ref>>) {
	let (tx, rx) = mpsc::unbounded_channel();
	let (node, node_ref) = Node::new(Collect { tx }).expect("build node");
	(node, node_ref, rx)
}

async fn next(rx: &mut mpsc::UnboundedReceiver<Vec<Ref>>) -> Vec<Ref> {
	tokio::time::timeout(Duration::from_secs(5), rx.recv())
		.await
		.expect("timed out waiting for a message")
		.expect("handler channel closed")
}

/// the same capability, sent twice, arrives as one `Ref` — and as the one we sent
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_same_capability_is_recognised_across_arrivals() {
	let (_collector, collector_ref, mut rx) = collector();
	let (_subject, subject_ref, _subject_rx) = collector();

	let mut first = Message::from_data(b"here it is".to_vec());
	first.add_ref(&subject_ref);
	collector_ref.send(first).await.expect("peer closed");
	let first = next(&mut rx).await;

	let mut again = Message::from_data(b"here it is again".to_vec());
	again.add_ref(&subject_ref);
	collector_ref.send(again).await.expect("peer closed");
	let again = next(&mut rx).await;

	assert!(
		first[0].is_same(&again[0]),
		"the same capability arriving twice produced two unrelated Refs"
	);
	assert!(
		first[0].is_same(&subject_ref),
		"a capability handed out and received back is not the one we handed out"
	);
}

/// several copies riding on *one* message collapse the same way
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicates_within_one_message_collapse() {
	let (_collector, collector_ref, mut rx) = collector();
	let (_subject, subject_ref, _subject_rx) = collector();

	let mut message = Message::from_data(b"three times".to_vec());
	for _ in 0..3 {
		message.add_ref(&subject_ref);
	}
	collector_ref.send(message).await.expect("peer closed");

	let received = next(&mut rx).await;
	assert_eq!(received.len(), 3, "all three descriptors should arrive");
	assert!(
		received[0].is_same(&received[1]) && received[1].is_same(&received[2]),
		"three copies of one capability produced more than one Ref"
	);
}

/// capabilities to *different* nodes must never be confused for each other
///
/// the hazard the registry has to survive is inode reuse: the kernel reissues an inode
/// number as soon as the socket holding it closes, so a registry that outlived its
/// entries would hand out a stale `Ref` for an unrelated node. Here that would show up
/// as one of these rounds recognising the previous round's capability.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distinct_nodes_stay_distinct_across_inode_reuse() {
	let (_collector, collector_ref, mut rx) = collector();

	let mut previous: Option<Ref> = None;
	for round in 0..64 {
		// dropped at the end of each iteration, freeing its socket's inode number for
		// the next round to be issued
		let (_subject, subject_ref, _subject_rx) = collector();

		let mut message = Message::from_data(format!("round {round}").into_bytes());
		message.add_ref(&subject_ref);
		collector_ref.send(message).await.expect("peer closed");
		let received = next(&mut rx).await;

		assert!(
			received[0].is_same(&subject_ref),
			"round {round}: received capability is not the one sent"
		);
		if let Some(previous) = &previous {
			assert!(
				!received[0].is_same(previous),
				"round {round}: a new node's capability was mistaken for a dead one's"
			);
		}
		previous = Some(received.into_iter().next().expect("one capability"));
	}
}

/// the point of `Hash` + `Eq`: per-peer state that a returning peer actually finds
///
/// this is the shape a node keeping anything per-caller wants — a sequence number, a
/// quota, a session. Each of the three messages below arrives on its own descriptor, so
/// without recognition this map would end up with three entries of one message each
/// instead of one entry of three.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refs_key_a_hashmap_of_per_peer_state() {
	use std::collections::HashMap;

	let (_collector, collector_ref, mut rx) = collector();
	let (_a, a_ref, _a_rx) = collector();
	let (_b, b_ref, _b_rx) = collector();

	let mut seen: HashMap<Ref, u32> = HashMap::new();
	for sender in [&a_ref, &b_ref, &a_ref, &a_ref, &b_ref] {
		let mut message = Message::from_data(b"tally me".to_vec());
		message.add_ref(sender);
		collector_ref.send(message).await.expect("peer closed");
		for received in next(&mut rx).await {
			*seen.entry(received).or_default() += 1;
		}
	}

	assert_eq!(seen.len(), 2, "two peers should occupy two entries");
	assert_eq!(seen.get(&a_ref), Some(&3), "peer a sent three times");
	assert_eq!(seen.get(&b_ref), Some(&2), "peer b sent twice");

	// and the keys are usable as capabilities, not just as identities
	for peer in seen.keys() {
		peer.send(Message::from_data(b"tallied".to_vec()))
			.await
			.expect("peer closed");
	}
}

/// a recognised `Ref` is a real capability, not a bookkeeping entry
///
/// deduplication that quietly weakened what a `Ref` *is* would be worse than none: the
/// node's whole lifetime rule is that it dies when the last capability to it drops. So
/// the shared `Ref` has to hold the subject open on its own, and dropping it has to be
/// what lets go.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recognised_ref_keeps_its_node_alive() {
	let (_collector, collector_ref, mut rx) = collector();
	let (subject, subject_ref, _subject_rx) = collector();

	let mut message = Message::from_data(b"hold this".to_vec());
	message.add_ref(&subject_ref);
	collector_ref.send(message).await.expect("peer closed");
	let received = next(&mut rx).await;
	let received = received.into_iter().next().expect("one capability");

	// the capability we sent is gone; the one the handler was given is not, and it is the
	// same one, so the subject is still reachable through it
	drop(subject_ref);
	tokio::time::sleep(Duration::from_millis(100)).await;
	assert!(
		!subject.is_dead(),
		"the received capability stopped counting"
	);
	assert!(!received.is_dead(), "the subject hung up while still held");

	drop(received);
	tokio::time::timeout(Duration::from_secs(5), subject.death_notification())
		.await
		.expect("dropping the last capability should hang the subject's socket up");
}
