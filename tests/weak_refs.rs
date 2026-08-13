//! a capability you can reach for without keeping alive
//!
//! `WeakRef` exists for one problem — two nodes in a process each holding a `Ref` to the
//! other keep each other alive forever, because a service node's refs *are* its lifetime —
//! so the load-bearing test here is the cycle one, and the rest pin down the semantics it
//! depends on:
//!   - a `WeakRef` alone does not hold a node up
//!   - it upgrades while somebody else here does
//!   - it agrees with its `Ref` about which capability it names, alive or dead
//!   - it stops upgrading once the last local strong `Ref` goes, which is *narrower* than
//!     the node being dead, and the last test is the one that says so out loud

use std::time::Duration;
use strong_ipc::{FdVec, Handler, Node, Ref, UCred, WeakRef};

struct Sink;
impl Handler for Sink {
	async fn handle(&self, _data: &mut [u8], _fds: FdVec, _creds: Option<UCred>) {}
}

#[tokio::test]
async fn upgrades_while_a_strong_ref_is_held() {
	let (_node, node_ref) = Node::new(Sink).expect("node");
	let weak = node_ref.downgrade();

	assert!(weak.is_live());
	let upgraded = weak.upgrade().expect("a strong ref is right here");
	assert_eq!(upgraded, node_ref, "upgrading gives back the same capability");
}

#[tokio::test]
async fn stops_upgrading_when_the_last_strong_ref_goes() {
	let (node, node_ref) = Node::new(Sink).expect("node");
	let weak = node_ref.downgrade();
	node.to_service();

	assert!(weak.upgrade().is_some());
	drop(node_ref);
	assert!(
		weak.upgrade().is_none(),
		"nothing here is holding it up any more"
	);
	assert!(!weak.is_live());
}

#[tokio::test]
async fn a_weak_ref_alone_does_not_keep_a_node_alive() {
	let (node, node_ref) = Node::new(Sink).expect("node");
	let weak = node_ref.downgrade();
	// the refs own the lifetime now, and `weak` is deliberately not one of them
	node.to_service();
	let watching = node_ref.clone();
	drop(node_ref);
	drop(watching);

	// the socket hung up, so the receive loop ended and the handler went with it. If the
	// WeakRef were secretly holding the node up, this would still upgrade
	for _ in 0..100 {
		if weak.upgrade().is_none() {
			return;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	panic!("a WeakRef kept its node alive");
}

#[tokio::test]
async fn breaks_a_cycle_between_two_nodes() {
	// the shape this type exists for: a parent holding its child, and the child holding
	// the parent back. Strong in both directions is a leak nothing can collect
	struct Parent {
		child: std::sync::Mutex<Option<Ref>>,
	}
	impl Handler for Parent {
		async fn handle(&self, _data: &mut [u8], _fds: FdVec, _creds: Option<UCred>) {}
	}
	struct Child {
		parent: std::sync::Mutex<Option<WeakRef>>,
	}
	impl Handler for Child {
		async fn handle(&self, _data: &mut [u8], _fds: FdVec, _creds: Option<UCred>) {}
	}

	let (parent_node, parent_ref) = Node::new(Parent {
		child: std::sync::Mutex::new(None),
	})
	.expect("parent");
	let (child_node, child_ref) = Node::new(Child {
		parent: std::sync::Mutex::new(None),
	})
	.expect("child");

	// parent holds the child up; child only reaches back
	*parent_node.handler().child.lock().unwrap() = Some(child_ref.clone());
	*child_node.handler().parent.lock().unwrap() = Some(parent_ref.downgrade());

	let watch_child = child_ref.downgrade();
	parent_node.to_service();
	child_node.to_service();
	drop(child_ref);

	// the child is still up: the parent holds it
	assert!(watch_child.upgrade().is_some());

	// dropping the last outside ref to the parent must take both down. With a strong ref
	// in the child this would hang forever instead
	drop(parent_ref);
	for _ in 0..200 {
		if watch_child.upgrade().is_none() {
			return;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	panic!("the cycle did not collect — the child outlived its parent");
}

#[tokio::test]
async fn agrees_with_its_ref_on_identity() {
	let (_a, a_ref) = Node::new(Sink).expect("a");
	let (_b, b_ref) = Node::new(Sink).expect("b");

	assert_eq!(a_ref.downgrade(), a_ref.downgrade());
	assert_eq!(a_ref.downgrade(), a_ref.clone().downgrade());
	assert_ne!(a_ref.downgrade(), b_ref.downgrade());

	// usable as a map key, which is the point of having Hash agree with Eq
	let mut map = std::collections::HashMap::new();
	map.insert(a_ref.downgrade(), "a");
	map.insert(b_ref.downgrade(), "b");
	assert_eq!(map.get(&a_ref.downgrade()), Some(&"a"));
	assert_eq!(map.len(), 2);
}

#[tokio::test]
async fn identity_outlives_what_it_names() {
	let (node, node_ref) = Node::new(Sink).expect("node");
	let weak = node_ref.downgrade();
	let same = node_ref.downgrade();
	node.to_service();
	drop(node_ref);

	// the Weak keeps the allocation alive even with no strong count left, so nothing can
	// be handed this address while we hold one — comparing dead WeakRefs stays sound
	assert!(weak.upgrade().is_none());
	assert_eq!(weak, same, "two dead WeakRefs still agree on what they named");
}

#[tokio::test]
async fn a_never_upgrading_weak_ref() {
	let placeholder = WeakRef::new();
	assert!(placeholder.upgrade().is_none());
	assert!(!placeholder.is_live());
	assert_eq!(placeholder, WeakRef::default());
}

#[tokio::test]
async fn an_in_process_holder_still_counts_as_a_local_strong_ref() {
	// the sharp edge worth stating in a test: weak here means "somebody local holds this",
	// not "the node is alive". A node kept up entirely by another process is alive and
	// answering, and a WeakRef to it still will not upgrade — our last descriptor went
	struct Keeper {
		got: std::sync::Mutex<Option<Ref>>,
	}
	impl Handler for Keeper {
		async fn handle(&self, _data: &mut [u8], mut fds: FdVec, _creds: Option<UCred>) {
			if let Some(fd) = fds.pop() {
				*self.got.lock().unwrap() = Some(Ref::from_owned_fd(fd));
			}
		}
	}

	let (target, target_ref) = Node::new(Sink).expect("target");
	let weak = target_ref.downgrade();
	target.to_service();

	// stand in for another process: a second node holding the only remaining capability.
	// It is a local Ref, so this test can only show the mechanism, not the cross-process
	// case — but the mechanism is the same one, and `Keeper` is what would be remote
	let (keeper, keeper_ref) = Node::new(Keeper {
		got: std::sync::Mutex::new(None),
	})
	.expect("keeper");
	let mut msg = strong_ipc::Message::from_data(vec![0u8; 4]);
	msg.add_ref(&target_ref);
	keeper_ref.try_send(msg).expect("hand the capability over");

	for _ in 0..100 {
		if keeper.handler().got.lock().unwrap().is_some() {
			break;
		}
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	assert!(
		keeper.handler().got.lock().unwrap().is_some(),
		"keeper never received it"
	);

	drop(target_ref);
	// the keeper's Ref is the same interned RefInner — recognition means it is literally
	// our strong count, so this still upgrades. Across a real process boundary there
	// would be no local strong count at all and it would not
	assert!(
		weak.upgrade().is_some(),
		"an interned ref held elsewhere in-process is still a local strong ref"
	);
}
