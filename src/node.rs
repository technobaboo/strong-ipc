//! the receiving side: what turns bytes on a socket back into a handler call

use crate::{
	Ref,
	error::NodeError,
	death::Death,
	message::FdVec,
	wire::{self, MAX_ANCILLARY_BUFFER_SIZE, MAX_MESSAGE_SIZE, Reactive},
};
use rustix::net::UCred;
use std::{
	mem::MaybeUninit, ops::Deref, os::fd::AsFd, sync::Arc
};
use tokio_util::task::AbortOnDrop;

pub trait Handler: Send + Sync + 'static {
	fn handle(
		&self,
		data: &mut [u8],
		fds: FdVec,
		creds: Option<UCred>,
	) -> impl Future<Output = ()> + Send + Sync;
}

/// a node on a socketpair, reached with the [`Ref`] handed back beside it
///
/// deliberately does **not** hold that `Ref`. A node keeping a capability to itself would
/// pin its own socket open forever, and the socket hanging up is precisely the signal
/// that nobody can reach this node any more — see [`Node::is_dead`]. Holding it would
/// also make the node a cycle waiting to happen, since the `Ref` is the thing you hand
/// out and the node is the thing that would then never be droppable by anyone else.
pub struct Node<H: Handler> {
	handler: Arc<H>,
	death: Arc<Death>,
	_task: AbortOnDrop,
}
impl<H: Handler> Node<H> {
	/// builds a node and the one capability that reaches it
	///
	/// the `Ref` is yours to keep, clone, and hand out; the node has no copy of its own.
	/// Drop every `Ref` and the node's socket hangs up, its receive loop ends, and
	/// [`Node::is_dead`] goes true — which is the whole reason the pair is split.
	#[must_use = "the Ref is the only way to reach this node — a Node holds no capability \
                  to itself, so dropping the Ref here hangs the node's socket up \
                  immediately and it will never receive anything"]
	pub fn new(handler: H) -> Result<(Node<H>, Ref), NodeError> {
		Node::new_raw(Arc::new(handler))
	}
	/// [`Node::new`] for a handler you already share elsewhere
	#[must_use = "the Ref is the only way to reach this node — a Node holds no capability \
                  to itself, so dropping the Ref here hangs the node's socket up \
                  immediately and it will never receive anything"]
	pub fn new_raw(handler: Arc<H>) -> Result<(Node<H>, Ref), NodeError> {
		let (tx, rx) = wire::socketpair()?;
		wire::enable_credentials(rx.as_fd());
		// built out here, not in the task: `AsyncFd::new` needs runtime context, and
		// failing to register should be this call's error rather than a task that dies
		let socket = Reactive::new(rx)?;
		let death = Arc::new(Death::default());
		// built before the task rather than at the end, because the entry filed under
		// `local-handlers` is keyed on this ref's socket identity
		let node_ref = Ref::from_fd(tx);
		#[cfg(feature = "local-handlers")]
		let local = node_ref
			.socket_id()
			.map(|id| crate::local::LocalEntry::file(id, &handler));
		let task = tokio::spawn({
			let handler = handler.clone();
			let tombstone = death.tombstone();
			async move {
				let _tombstone = tombstone;
				// rides with the tombstone for the same reason: the receive loop is what
				// ends on every path that ends this node, `to_service` included
				#[cfg(feature = "local-handlers")]
				let _local = local;
				recv_loop(handler, socket).await
			}
		});
		Ok((
			Self {
				handler,
				death,
				_task: AbortOnDrop::new(task.abort_handle()),
			},
			node_ref,
		))
	}

	/// has this node stopped receiving?
	///
	/// this is the mirror of [`Ref::is_dead`]: that one asks whether the node behind a
	/// capability is gone, this one asks whether every capability to this node is. Since
	/// a `Node` holds no `Ref` to itself, dropping the last one really does hang its
	/// socket up, and the receive loop sees it.
	///
	/// what it literally reports is the receive loop no longer running, which is the
	/// broader statement and the one you want — nothing sent here will be handled again.
	/// Besides the last `Ref` going, that covers an io error on the socket and a panic in
	/// a handler. It is a latch: never false again once true.
	///
	/// a plain atomic load. The receive loop is already watching the socket, so unlike
	/// [`Ref::is_dead`] there is nothing to ask the kernel.
	pub fn is_dead(&self) -> bool {
		self.death.is_dead()
	}

	/// resolves when this node stops receiving; see [`Node::is_dead`] for what that means
	///
	/// returns immediately if it already has. Costs nothing until awaited and needs no
	/// reactor registration, so unlike [`Ref::death_notification`] there is no reason not
	/// to use it. Awaiting this is the ordinary way to keep a node alive for exactly as
	/// long as somebody can still reach it:
	///
	/// ```no_run
	/// # async fn f<H: strong_ipc::Handler>(handler: H) -> Result<(), strong_ipc::NodeError> {
	/// let (node, capability) = strong_ipc::Node::new(handler)?;
	/// hand_out(capability);
	/// node.death_notification().await; // returns once the last Ref is dropped
	/// # Ok(()) }
	/// # fn hand_out(_: strong_ipc::Ref) {}
	/// ```
	pub async fn death_notification(&self) {
		self.death.wait().await;
	}
	/// the handler this node is feeding
	///
	/// hands back the `Arc` rather than a plain `&H` so a caller can clone out a share of
	/// the handler the receive loop is already holding, without the node having to have
	/// been built from an `Arc` they kept a copy of
	///
	/// deliberately a named method rather than a `Deref` impl: a `Node` is not a handler,
	/// and quietly putting every one of `H`'s methods onto it hides which of the two you
	/// are actually talking to. Worse with `Arc<H>` as the target, since a `Node` is not
	/// `Clone` — `node.clone()` would resolve through the deref and hand back a share of
	/// the handler instead of failing to compile
	pub fn handler(&self) -> &Arc<H> {
		&self.handler
	}

	/// gives up the handle and lets this node live exactly as long as somebody can reach it
	///
	/// the normal deal is that a `Node` is the node: drop it and the socket hangs up, so a
	/// caller has to park it somewhere for as long as it should keep serving. That is
	/// backwards for anything handed out and then forgotten about, where the refs are the
	/// only thing that should decide how long it lives.
	///
	/// this costs no task and no bookkeeping, because there is nothing to store. The
	/// receive loop already owns its own share of the handler and its own tombstone, and
	/// already ends on its own when the last `Ref` hangs the socket up — the sole reason it
	/// stops earlier is the abort handle in here. Dropping the node without that handle
	/// armed *is* the whole feature: the loop keeps running, the handler stays alive
	/// underneath it, and both go when the refs do.
	///
	/// there is no getting it back afterwards, and nothing to abort it early with, so the
	/// refs really are the only lifetime left. Keep the node instead if you need either.
	///
	/// ```no_run
	/// # async fn f<H: strong_ipc::Handler>(handler: H) -> Result<(), strong_ipc::NodeError> {
	/// let (node, capability) = strong_ipc::Node::new(handler)?;
	/// node.to_service(); // no longer ours to keep alive
	/// hand_out(capability); // …this is
	/// # Ok(()) }
	/// # fn hand_out(_: strong_ipc::Ref) {}
	/// ```
	pub fn to_service(self) {
		// `Node` has no `Drop` of its own, so the abort handle can be moved out and
		// disarmed on its way past. `handler` and `death` then drop as usual — the task
		// holds a clone of each, which is what keeps them alive
		drop(self._task.detach());
	}
}

/// `dead` is the one thing about a node worth printing, and unlike [`Ref`]'s it is free —
/// [`Node::is_dead`] is an atomic load, not a syscall
impl<H: Handler> std::fmt::Debug for Node<H> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Node")
			.field("dead", &self.is_dead())
			.finish_non_exhaustive()
	}
}
impl<H: Handler> Deref for Node<H> {
    type Target = Arc<H>;

    fn deref(&self) -> &Self::Target {
        self.handler()
    }
}

/// the one spot where a message on the wire becomes a handler call
///
/// free-standing since node and boundnode share no type, they just both need something
/// to feed a handler. `Ok` means the peer hung up, not an error, though that's just a
/// zero-length read, same as an empty message, so sending one looks like a disconnect.
///
/// both buffers are fixed and neither can be overrun by a peer using this crate:
/// [`MAX_MESSAGE_SIZE`] is refused at send, and the control buffer is sized for
/// `MAX_FDS`. That is what makes the receive path one syscall with no size probe in
/// front of it, and what keeps the per-node footprint a constant.
///
/// a peer *not* using this crate is a different matter — the kernel would let it send up
/// to `SO_SNDBUF` — so `MSG_TRUNC` is still requested and an oversized message is
/// reported and dropped rather than delivered as a silently shortened payload
async fn recv_loop<H: Handler>(handler: Arc<H>, socket: Reactive) -> std::io::Result<()> {
	let mut buf = vec![0u8; MAX_MESSAGE_SIZE];
	let mut control = vec![MaybeUninit::uninit(); MAX_ANCILLARY_BUFFER_SIZE];

	loop {
		let received = socket.recv(&mut buf, &mut control).await?;
		if received.bytes == 0 {
			return Ok(());
		}
		if received.truncated(buf.len()) {
			// only reachable from a peer that isn't going through `Ref::try_send`
			eprintln!(
				"strong-ipc: dropped a {} B message from a peer ignoring the {} B limit",
				received.bytes, MAX_MESSAGE_SIZE
			);
			continue;
		}

		handler
			.handle(&mut buf[..received.bytes], received.fds, received.creds)
			.await;
	}
}
