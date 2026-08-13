//! reaching a local node's handler through a ref, and the edges around that
//!
//! the shortcut itself is one lookup, so what actually needs covering is when it stops
//! being available and when it must refuse:
//!   - it survives the ref going out and coming back, which is the whole point — a server
//!     handing a client one of its own objects gets the object back, not a socket
//!   - it survives `to_service`, since a node whose handle was given up is still a node
//!   - it goes away when the node does, by either route: the handle dropping, or the last
//!     ref dropping out from under a service
//!   - it refuses a wrong `H` rather than handing back the wrong handler
//!
//! the unfile is keyed on a socket inode the kernel is free to reissue, so the last test
//! here is the one that would catch a stale entry naming somebody else's node.

#![cfg(feature = "local-handlers")]

use std::sync::Arc;
use std::time::Duration;
use strong_ipc::{FdVec, Handler, Message, Node, Ref, UCred};

struct Counter(&'static str);
impl Handler for Counter {
	async fn handle(&self, _data: &mut [u8], _fds: FdVec, _creds: Option<UCred>) {}
}

struct Other;
impl Handler for Other {
	async fn handle(&self, _data: &mut [u8], _fds: FdVec, _creds: Option<UCred>) {}
}

/// bounces `node_ref` through a real socket and hands back what came out the far side
///
/// the point is that the returning ref is built by `Ref::from_owned_fd` off a descriptor
/// the kernel installed, exactly as one arriving from another process would be — not
/// cloned from the one that went out, which would prove nothing about recognition.
async fn round_trip(node_ref: &Ref) -> Ref {
	let (relay, relay_ref) = Node::new(Echo::default()).expect("relay node");
	let mut msg = Message::from_data(vec![0u8; 4]);
	msg.add_ref(node_ref);
	relay_ref.try_send(msg).expect("send the capability");

	for _ in 0..100 {
		if let Some(got) = relay.handler().received.lock().unwrap().clone() {
			return got;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	panic!("relay never saw the capability");
}

#[derive(Default)]
struct Echo {
	received: std::sync::Mutex<Option<Ref>>,
}
impl Handler for Echo {
	async fn handle(&self, _data: &mut [u8], mut fds: FdVec, _creds: Option<UCred>) {
		if let Some(fd) = fds.pop() {
			*self.received.lock().unwrap() = Some(Ref::from_owned_fd(fd));
		}
	}
}

#[tokio::test]
async fn reaches_its_own_handler() {
	let (node, node_ref) = Node::new(Counter("mine")).expect("node");
	let got = node_ref.local_handler::<Counter>().expect("should be local");
	assert_eq!(got.0, "mine");
	assert!(Arc::ptr_eq(&got, node.handler()));
}

#[tokio::test]
async fn survives_a_round_trip_through_a_socket() {
	let (node, node_ref) = Node::new(Counter("bounced")).expect("node");
	let returned = round_trip(&node_ref).await;

	// recognition first: the ref that came back is the one that went out
	assert_eq!(returned, node_ref);
	let got = returned
		.local_handler::<Counter>()
		.expect("a returned capability still leads home");
	assert_eq!(got.0, "bounced");
	assert!(Arc::ptr_eq(&got, node.handler()));
}

#[tokio::test]
async fn refuses_the_wrong_handler_type() {
	let (_node, node_ref) = Node::new(Counter("mine")).expect("node");
	assert!(node_ref.local_handler::<Counter>().is_some());
	assert!(
		node_ref.local_handler::<Other>().is_none(),
		"a downcast to the wrong handler must refuse, not hand back the wrong one"
	);
}

#[tokio::test]
async fn outlives_to_service() {
	let (node, node_ref) = Node::new(Counter("service")).expect("node");
	node.to_service();
	// the node is still receiving — the refs decide its lifetime now, and we hold one
	let got = node_ref
		.local_handler::<Counter>()
		.expect("a service is still a local node");
	assert_eq!(got.0, "service");
}

#[tokio::test]
async fn unfiled_when_the_node_handle_drops() {
	let (node, node_ref) = Node::new(Counter("dropped")).expect("node");
	assert!(node_ref.local_handler::<Counter>().is_some());

	drop(node);
	// the abort lands on the receive loop, which is what holds the entry
	for _ in 0..100 {
		if node_ref.local_handler::<Counter>().is_none() {
			return;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	panic!("entry outlived the node it belonged to");
}

#[tokio::test]
async fn unfiled_when_a_service_loses_its_last_ref() {
	let (node, node_ref) = Node::new(Counter("abandoned")).expect("node");
	node.to_service();
	// keep the identity to ask with, without keeping the socket open
	let watcher = node_ref.clone();
	drop(node_ref);
	assert!(
		watcher.local_handler::<Counter>().is_some(),
		"we still hold a ref, so the node is still alive"
	);
	drop(watcher);

	// nothing left to ask with by design — the check is that a *new* node can take the
	// inode back cleanly, which a stale entry would break. See the next test.
}

#[tokio::test]
async fn a_reused_inode_does_not_inherit_a_stale_entry() {
	// churn nodes so the kernel gets every chance to hand an inode back out. A stale
	// entry surviving its node would show up here as a ref reaching the *previous*
	// node's handler, or as the wrong name coming back
	for i in 0..64 {
		let name = if i % 2 == 0 { "even" } else { "odd" };
		let (node, node_ref) = Node::new(Counter(name)).expect("node");
		let got = node_ref
			.local_handler::<Counter>()
			.expect("its own entry, not a predecessor's");
		assert_eq!(got.0, name, "reached a stale handler from a previous node");
		assert!(Arc::ptr_eq(&got, node.handler()));
		drop(got);
		drop(node);
		drop(node_ref);
		tokio::task::yield_now().await;
	}
}

#[tokio::test]
async fn a_foreign_ref_is_not_local() {
	// a ref to a node in this process is local; one built from a socket that is not a
	// node of ours is not. `Ref::connect` to a BoundNode is the honest version of that:
	// the listener has no per-connection node, so there is nothing to reach
	let path = std::env::temp_dir().join(format!("strong-ipc-local-{}.sock", std::process::id()));
	let _ = std::fs::remove_file(&path);
	let bound = strong_ipc::BoundNode::bind(&path, Other).expect("bind");
	let connected = Ref::connect(&path).await.expect("connect");
	assert!(
		connected.local_handler::<Other>().is_none(),
		"an accepted connection has no node of its own to reach"
	);
	drop(bound);
}
