//! reaching the handler behind a capability that leads back to one of our own nodes
//!
//! a [`Ref`] is deliberately opaque: it names a socket, not a type, and that is exactly
//! what lets it cross a process boundary. But a capability handed to a peer and handed
//! back is *recognised* — [`Ref::from_owned_fd`] interns on the socket's inode, so the ref
//! that comes back is the one that went out. When both ends are in one process, that
//! recognition is enough to go one step further and reach the handler itself, skipping the
//! socket entirely.
//!
//! that is what this module is for, and why it is behind a feature. It is a deliberate
//! hole in the abstraction, for a process that is both ends of a capability: a server
//! handing out refs to its own objects, needing the object back when a client hands one
//! in, gets it for a hash lookup instead of a round trip through its own wire format.
//!
//! nothing here works across processes and nothing here should. [`Ref::local_handler`] on
//! a ref that leads somewhere else is `None`, which is the same answer it gives for a ref
//! whose node is gone — a caller that cannot tell those apart is asking the right
//! question anyway, since neither one is a handler it can have.
//!
//! [`Ref`]: crate::Ref
//! [`Ref::from_owned_fd`]: crate::Ref::from_owned_fd
//! [`Ref::local_handler`]: crate::Ref::local_handler

use crate::{Handler, capability::SocketId};
use dashmap::DashMap;
use std::{
	any::Any,
	sync::{Arc, LazyLock, Weak},
};

/// every live local node's handler, by the socket its refs send to
///
/// keyed the same way the `Ref` registry is, because it answers the same question about
/// the same socket: that one maps the identity to the machinery for *sending* there, this
/// one maps it to the handler that would *receive* it. Two maps rather than one field on
/// `RefInner` because a `Ref` and the node it leads to have unrelated lifetimes — the ref
/// can be dropped locally while a peer still holds a dup, and the node outlives that.
///
/// entries are [`Weak`], so being in here keeps no handler alive, and the map is bounded
/// by the number of live local nodes.
static LOCALS: LazyLock<DashMap<SocketId, Weak<dyn Any + Send + Sync>>> = LazyLock::new(DashMap::new);

/// files a node's handler for as long as that node can still receive
///
/// held by the receive loop rather than by the [`Node`], because the node is not the thing
/// whose lifetime this has to match: [`Node::to_service`] gives the handle up and leaves
/// the loop running, and the entry has to outlive that. The loop ends on every path that
/// ends the node — the last ref going, an io error, the abort handle in a dropped `Node` —
/// so dropping this along with it is exactly the right span.
///
/// [`Node`]: crate::Node
/// [`Node::to_service`]: crate::Node::to_service
pub(crate) struct LocalEntry {
	id: SocketId,
	/// the address of the `Weak` we filed, so a late unfile cannot take somebody else's
	/// entry out
	///
	/// the race is real and the same one the `Ref` registry guards against: our entry is
	/// removed once the receive loop ends, and by then the socket is closed, so the kernel
	/// is free to hand that inode number to the next socketpair — which may file its own
	/// entry under this key before our drop runs.
	///
	/// a `usize` rather than a pointer because this crosses into a spawned task and a raw
	/// pointer is not `Send`. It is only ever compared, never dereferenced.
	filed: usize,
}

impl LocalEntry {
	pub(crate) fn file<H: Handler>(id: SocketId, handler: &Arc<H>) -> Self {
		// in two steps: ascribing the unsized type straight onto the call makes it the
		// *argument* that has to coerce, and `&Arc<H>` to `&Arc<dyn Any>` is not a coercion
		let weak: Weak<H> = Arc::downgrade(handler);
		let weak: Weak<dyn Any + Send + Sync> = weak;
		let filed = Weak::as_ptr(&weak) as *const () as usize;
		// straight `insert`: a live entry under this key would mean the kernel reissued an
		// inode whose socket is still open, which it does not do
		LOCALS.insert(id, weak);
		Self { id, filed }
	}
}

impl Drop for LocalEntry {
	fn drop(&mut self) {
		LOCALS.remove_if(&self.id, |_, existing| {
			Weak::as_ptr(existing) as *const () as usize == self.filed
		});
	}
}

/// the handler filed under `id`, if there is one and it is an `H`
pub(crate) fn local_handler<H: Handler>(id: SocketId) -> Option<Arc<H>> {
	// cloned out from under the shard lock rather than upgraded while holding it: a failed
	// downcast drops its `Arc`, which can be the last one and can run `H`'s destructor,
	// and running somebody else's code while holding a shard is how a map like this
	// deadlocks
	let handler = LOCALS.get(&id)?.clone();
	handler.upgrade()?.downcast::<H>().ok()
}
