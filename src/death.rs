//! "the other end is gone" — one flag, one wakeup
//!
//! this is the *observed* kind of death, used by [`Node`]: something is already watching
//! the socket (the receive loop), so when it stops there is a moment to record, and the
//! answer is a plain atomic afterwards.
//!
//! [`Ref`] deliberately does **not** use this. Nothing watches a `Ref`'s socket — that is
//! the whole point of the unregistered fast path — so there is no loop to notice a
//! hangup and set a flag. It asks the kernel instead, in [`crate::wire::is_hung_up`] and
//! [`crate::wire::Reactive::hangup`].
//!
//! [`Node`]: crate::Node
//! [`Ref`]: crate::Ref

use std::sync::{
	Arc,
	atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;

/// a one-way latch: not dead, then dead, never back again
#[derive(Default)]
pub(crate) struct Death {
	dead: AtomicBool,
	notify: Notify,
}

impl Death {
	pub(crate) fn is_dead(&self) -> bool {
		self.dead.load(Ordering::Acquire)
	}

	/// parks until [`Death::declare`], or returns straight away if it already happened
	///
	/// the ordering here is load-bearing. `Notified` does not register with the `Notify`
	/// until it is first polled, so checking the flag and *then* awaiting would lose a
	/// `declare` landing in between and park forever. `enable()` registers up front, so
	/// the check happens with the waiter already in place.
	pub(crate) async fn wait(&self) {
		let mut notified = std::pin::pin!(self.notify.notified());
		notified.as_mut().enable();
		if self.is_dead() {
			return;
		}
		notified.await;
	}

	fn declare(&self) {
		self.dead.store(true, Ordering::Release);
		self.notify.notify_waiters();
	}

	/// a guard that declares this death when it drops
	///
	/// held *inside* the task rather than called at the end of it, so the death is
	/// recorded however the task ends: a clean hangup, an io error, a panic, or the
	/// abort that comes with dropping the node
	pub(crate) fn tombstone(self: &Arc<Self>) -> Tombstone {
		Tombstone(self.clone())
	}
}

pub(crate) struct Tombstone(Arc<Death>);

impl Drop for Tombstone {
	fn drop(&mut self) {
		self.0.declare();
	}
}
