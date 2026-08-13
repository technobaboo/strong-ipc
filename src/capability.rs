//! `Ref` — a sending capability, and the two-tier send that keeps it cheap

use crate::{
	error::{SendError, TrySendError},
	message::Message,
	outbox::Outbox,
	wire,
};
use dashmap::{DashMap, mapref::entry::Entry};
use std::{
	os::fd::{AsFd, BorrowedFd, OwnedFd},
	path::Path,
	sync::{Arc, LazyLock, OnceLock, Weak},
};

/// a socket's identity as the kernel sees it: the device its inode lives on, and the inode
///
/// every descriptor naming one socket agrees on this — the original, a `dup` of it, and
/// the copy the kernel installs when it crosses a `SCM_RIGHTS`. Descriptor *numbers* agree
/// on nothing, so this is the only thing a received capability can be recognised by.
///
/// it is only unique while the socket is open. The kernel hands an inode number straight
/// back out once it is free, so an entry that outlived its socket would not merely be
/// stale, it would eventually name somebody else's — which is the whole reason
/// [`RefInner::drop`] unfiles itself before its descriptor closes.
pub(crate) type SocketId = (u64, u64);

/// every live `Ref`, by the socket it sends to
///
/// this is what makes a capability arriving twice *be* the same capability rather than
/// two that happen to reach the same place. The point is not the allocation saved: it is
/// that the per-`Ref` machinery — the outbox, the death watch — is built once for a peer
/// and shared, instead of once per arrival. A node that is handed the same capability a
/// thousand times ends up with one outbox, not a thousand.
///
/// entries are [`Weak`], so being in here keeps nothing alive; a `Ref` is reachable
/// exactly as long as somebody outside holds one.
///
/// global on purpose. The key is a kernel-wide identity, not a process-local one, so any
/// narrower scope would just be a registry that fails to recognise a capability handed
/// between two of its own nodes. Its size is bounded by the number of live `Ref`s.
static REFS: LazyLock<DashMap<SocketId, Weak<RefInner>>> = LazyLock::new(DashMap::new);

fn socket_id(fd: BorrowedFd<'_>) -> std::io::Result<SocketId> {
	let stat = rustix::fs::fstat(fd)?;
	Ok((stat.st_dev, stat.st_ino))
}

struct RefInner {
	/// the socket, deliberately *not* registered with the reactor — every inline send is
	/// a bare `sendmsg` on this, which needs no readiness tracking at all
	fd: OwnedFd,
	/// this `Ref`'s key in [`REFS`], or `None` if it never made it in
	///
	/// only `None` when `fstat` failed, which for an open descriptor it does not — but a
	/// capability that cannot be filed is still a perfectly good capability, so that case
	/// gives up on deduplication rather than on the `Ref`
	id: Option<SocketId>,
	/// captured at construction, since the outbox is built from `try_send`, which is
	/// sync and may not be running on a runtime thread
	runtime: Option<tokio::runtime::Handle>,
	outbox: OnceLock<Outbox>,
	/// built only if someone actually awaits [`Ref::death_notification`]
	///
	/// same bargain as the outbox: a reactor registration is exactly what a `Ref` exists
	/// to avoid paying per capability received, so it is deferred until asked for. Once
	/// built it is shared by every waiter on this `Ref` and every later call, so watching
	/// costs one `epoll_ctl` per `Ref` that ever asks, not one per wait
	death: OnceLock<wire::Reactive>,
}

impl RefInner {
	fn new(fd: OwnedFd, id: Option<SocketId>) -> Self {
		Self {
			fd,
			id,
			runtime: tokio::runtime::Handle::try_current().ok(),
			outbox: OnceLock::new(),
			death: OnceLock::new(),
		}
	}

	fn outbox(&self) -> std::io::Result<&Outbox> {
		if let Some(outbox) = self.outbox.get() {
			return Ok(outbox);
		}
		let built = Outbox::build(&self.fd, self.runtime.as_ref())?;
		// a concurrent caller may have got there first; theirs is as good as ours, so
		// ours drops here — closing its channel, which retires the task it just spawned
		let _ = self.outbox.set(built);
		Ok(self
			.outbox
			.get()
			.expect("just set, or set by whoever raced us"))
	}

	/// the socket registered with the reactor, so a hangup can be awaited
	///
	/// a *dup*, not the `Ref`'s own fd, for the same reason the outbox uses one: the
	/// `Ref`'s fd stays out of the reactor so the inline send path never touches it.
	/// Both name the same open file description, so the dup sees the same hangup.
	fn death_watch(&self) -> std::io::Result<&wire::Reactive> {
		if let Some(watch) = self.death.get() {
			return Ok(watch);
		}
		let built = wire::Reactive::new(self.fd.try_clone()?)?;
		// whoever raced us has an equally good watch on the same file description, so
		// ours drops here, deregistering the dup it just added
		let _ = self.death.set(built);
		Ok(self
			.death
			.get()
			.expect("just set, or set by whoever raced us"))
	}
}

impl Drop for RefInner {
	fn drop(&mut self) {
		let Some(id) = self.id else { return };
		// `self.fd` is still open here — a struct's fields drop after its `Drop` body —
		// so the kernel cannot yet have reissued this inode number to a different socket.
		// That is the ordering the registry depends on: while this runs, `id` still means
		// what it meant when it was filed
		//
		// what is *not* guaranteed is that the entry under `id` is still ours. Our strong
		// count reached zero before this ran, so a `Ref::from_fd` racing us on another
		// descriptor for this same socket will have found our `Weak` un-upgradable and
		// filed a live entry over the top of it. `strong_count` is what tells the two
		// apart: a dead `Weak` is one nobody can upgrade, and only that one is ours to
		// remove
		REFS.remove_if(&id, |_, existing| existing.strong_count() == 0);
	}
}

/// a capability to send to one node
///
/// cloning is free and shares the same socket; handing one to a peer over `SCM_RIGHTS`
/// hands them the same authority.
#[derive(Clone)]
pub struct Ref {
	inner: Arc<RefInner>,
}

impl Ref {
	/// connects to the [`crate::BoundNode`] at `path`, giving you a ref that sends to it
	///
	/// nothing comes back this way, if you want a reply, put a ref of your own on the
	/// message with [`Message::add_ref`]
	pub async fn connect<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
		// `connect` on an AF_UNIX socket completes without blocking unless the listener's
		// backlog is full, in which case it reports `WouldBlock` and the caller retries —
		// the connection is not queued, so there is nothing to wait on
		Ok(Self::from_fd(wire::connect(path.as_ref())?))
	}
	/// wraps an fd received over `SCM_RIGHTS`
	///
	/// if this socket is one some other live `Ref` already sends to — the usual case when
	/// a peer hands you the same capability twice — you get that `Ref` back, sharing its
	/// outbox and its death watch, and `fd` is closed here as the redundant descriptor it
	/// is. See [`Ref::is_same`] for exactly what "the same socket" covers.
	///
	/// costs one `fstat` and one sharded hash lookup, and nothing else that can fail.
	/// this is the hot path — a node handling capabilities builds one of these per
	/// message — so it deliberately stays short of anything that touches the reactor
	pub fn from_owned_fd(fd: OwnedFd) -> Self {
		Self::from_fd(fd)
	}
	pub(crate) fn from_fd(fd: OwnedFd) -> Self {
		let Ok(id) = socket_id(fd.as_fd()) else {
			// nothing to key on, so this one lives outside the registry: it will never be
			// recognised and will never try to unfile itself
			return Self {
				inner: Arc::new(RefInner::new(fd, None)),
			};
		};
		// the whole hit-or-file decision happens under one shard lock, so a second thread
		// interning the same socket either sees our entry or is seen by it, never neither
		match REFS.entry(id) {
			Entry::Occupied(mut occupied) => {
				if let Some(inner) = occupied.get().upgrade() {
					// `fd` drops here: a second descriptor for a socket we already have a
					// capability to is just a descriptor, and holding it buys nothing
					return Self { inner };
				}
				// filed, but its holder is on its way out and cannot be revived. Ours
				// replaces it; theirs will decline to remove a live entry on the way past
				let inner = Arc::new(RefInner::new(fd, Some(id)));
				occupied.insert(Arc::downgrade(&inner));
				Self { inner }
			}
			Entry::Vacant(vacant) => {
				let inner = Arc::new(RefInner::new(fd, Some(id)));
				vacant.insert(Arc::downgrade(&inner));
				Self { inner }
			}
		}
	}

	/// do these two capabilities send to the same socket?
	///
	/// true exactly when both came from descriptors naming one socket, however far apart
	/// those arrived — a `Ref` you were handed back is the one you handed out. This is a
	/// pointer comparison; the recognition itself happened when each was built.
	///
	/// it is an answer about the *socket*, not about the node behind it. Two separate
	/// [`Ref::connect`]s to one [`crate::BoundNode`] are two sockets and compare false,
	/// correctly — they are distinct capabilities that happen to share a destination, and
	/// dropping one does not affect the other. Nothing at the descriptor level can unify
	/// them, so if that is the question you have, put an identity in the payload.
	///
	/// the named form of `==`; [`Ref`]'s [`PartialEq`] is exactly this.
	pub fn is_same(&self, other: &Ref) -> bool {
		Arc::ptr_eq(&self.inner, &other.inner)
	}

	/// this capability's socket identity, or `None` if `fstat` failed when it was built
	#[cfg(feature = "local-handlers")]
	pub(crate) fn socket_id(&self) -> Option<SocketId> {
		self.inner.id
	}

	/// the handler behind this capability, if it leads to a node in *this* process
	///
	/// the shortcut a process that is both ends of a capability is entitled to: a ref it
	/// handed out and got back is recognised on arrival, so the handler is a hash lookup
	/// away and nothing has to go over the wire to reach it. See [`crate::local`] for why
	/// this is behind a feature.
	///
	/// `None` covers every way this can fail to be a handler you can have, without
	/// distinguishing them: the ref leads to another process, its node is gone, or it is
	/// live and simply isn't an `H`. Nothing here is a capability check — a ref you were
	/// handed is authority to *send*, and if it happens to lead home this hands you the
	/// receiving side of it, so treat the `Arc<H>` as you would the node's own.
	///
	/// costs one sharded hash lookup, a `Weak` upgrade, and a downcast.
	#[cfg(feature = "local-handlers")]
	pub fn local_handler<H: crate::Handler>(&self) -> Option<Arc<H>> {
		crate::local::local_handler(self.inner.id?)
	}

	/// what `Hash` and `Eq` agree to treat as this capability's identity
	///
	/// the address of the shared `RefInner`, which is only meaningful while the `Ref` is
	/// alive — but nothing can hash or compare one that isn't, since doing either needs a
	/// borrow. Two live `Ref`s share this address if and only if they are the same
	/// capability, which is what keeps `Hash` and `Eq` consistent.
	fn identity(&self) -> *const RefInner {
		Arc::as_ptr(&self.inner)
	}
}

/// two capabilities are equal when they send to the same socket — see [`Ref::is_same`]
///
/// this is worth having only because a received capability is recognised on arrival: it
/// is what lets a node keep a `HashMap<Ref, _>` of per-peer state and have the second
/// message from a peer find the first one's entry. Without the registry behind
/// [`Ref::from_owned_fd`] every arrival would be a fresh key and the map would grow
/// without ever hitting.
///
/// clippy's `mutable_key_type` will object to that map: a `Ref` caches its outbox and its
/// death watch in `OnceLock`s, so it is interior-mutable by inspection. Neither is
/// reachable from `Hash` or `Eq` — both compare the address of the shared `RefInner` and
/// nothing else — so a `Ref` cannot change its own hash. Silence it with
/// `ignore-interior-mutability = ["strong_ipc::Ref"]` in your `clippy.toml`.
impl PartialEq for Ref {
	fn eq(&self, other: &Self) -> bool {
		self.is_same(other)
	}
}

/// equality here is `Arc` identity, which is reflexive, symmetric and transitive
///
/// including for the `Ref` that missed the registry because its `fstat` failed: that one
/// is equal to its own clones and to nothing else, which is a smaller equivalence class
/// than it deserves but a well-formed one
impl Eq for Ref {}

impl std::hash::Hash for Ref {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		std::ptr::hash(self.identity(), state);
	}
}

/// prints the identity `Hash` and `Eq` agree on, so two refs logging the same handle are
/// the same capability and two logging different ones are not
///
/// that address is both the most and the least this can honestly say: the socket's inode
/// names an open file description, which tells a reader nothing they could act on, and
/// the payloads that went through it are gone. Deliberately says nothing about liveness —
/// answering that is a syscall, and `Debug` should not make one
impl std::fmt::Debug for Ref {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "Ref({:p})", self.identity())
	}
}

/// a capability you can reach for but are not keeping alive
///
/// exists for cycles. A [`Ref`] is the node's lifetime — a service node dies when its last
/// one goes — so two nodes in one process each holding a `Ref` to the other keep each
/// other alive forever, and nothing in the socket layer can notice. One of the two holds a
/// `WeakRef` instead and the cycle collects.
///
/// ## what "weak" means here, exactly
///
/// this holds a `Weak` to the *local* `Ref`, not to the node. So [`WeakRef::upgrade`]
/// succeeds exactly while somebody **in this process** still holds a strong `Ref` to that
/// socket — which is a narrower thing than the node being alive, and the difference is
/// worth knowing before you reach for one:
///
/// - a node kept alive only by a **peer in another process** cannot be upgraded to, even
///   though it is perfectly alive and would happily answer. Our own last `Ref` closed our
///   only descriptor for that socket, and a `WeakRef` deliberately does not hold one — if
///   it did, it would be a `Ref`.
/// - an upgrade succeeding is not a promise the node is alive, for the same reason holding
///   a `Ref` never was: ask [`Ref::is_dead`] if you need that.
///
/// so read this as "reachable as long as something else here is holding it up", which is
/// the question the cycle case is actually asking. If what you want is "reachable while
/// the node lives", you want a `Ref` and a death notification.
///
/// upgrading costs an atomic increment; it never touches the kernel.
#[derive(Clone)]
pub struct WeakRef {
	inner: Weak<RefInner>,
}

impl Ref {
	/// a [`WeakRef`] to this capability's socket — see there for what weak means here
	pub fn downgrade(&self) -> WeakRef {
		WeakRef {
			inner: Arc::downgrade(&self.inner),
		}
	}
}

impl WeakRef {
	/// a `WeakRef` that never upgrades
	///
	/// for a field that has to exist before the capability it will point at does. Costs no
	/// allocation, and is what a `WeakRef` whose peers have all gone decays to in practice.
	pub fn new() -> Self {
		Self { inner: Weak::new() }
	}

	/// the capability, if anything here is still holding it up
	pub fn upgrade(&self) -> Option<Ref> {
		self.inner.upgrade().map(|inner| Ref { inner })
	}

	/// could [`WeakRef::upgrade`] succeed right now?
	///
	/// a convenience for logging and assertions. Anything that then *acts* on the answer
	/// should call `upgrade` and keep what it gets, since between the two calls the last
	/// strong `Ref` may go.
	pub fn is_live(&self) -> bool {
		self.inner.strong_count() > 0
	}

	/// the address `Hash` and `Eq` agree to treat as identity — see [`Ref::is_same`]
	///
	/// sound to compare even once this cannot upgrade, and for a different reason than
	/// [`Ref`]'s: a `Weak` keeps the allocation itself alive even after the last strong
	/// count goes, so nothing can be given this address while we hold one. A `WeakRef`
	/// from [`WeakRef::new`] has a dangling address it shares with every other such
	/// `WeakRef`, which is the one case where equality here says less than it looks.
	fn identity(&self) -> *const RefInner {
		self.inner.as_ptr()
	}
}

impl Default for WeakRef {
	fn default() -> Self {
		Self::new()
	}
}

/// the same identity [`Ref`] uses, so a `WeakRef` and the `Ref` it came from agree on
/// which capability they are talking about
impl PartialEq for WeakRef {
	fn eq(&self, other: &Self) -> bool {
		std::ptr::addr_eq(self.identity(), other.identity())
	}
}
impl Eq for WeakRef {}

impl std::hash::Hash for WeakRef {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		std::ptr::hash(self.identity(), state);
	}
}

/// prints the identity, like [`Ref`]'s, plus whether it can still be upgraded — which
/// unlike liveness is free to ask, being an atomic load rather than a syscall
impl std::fmt::Debug for WeakRef {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "WeakRef({:p}, live: {})", self.identity(), self.is_live())
	}
}

impl Ref {
	/// is the node behind this capability gone?
	///
	/// a `Ref` is only worth as much as the node on the other end, and nothing else in
	/// this crate will tell you it has gone: sends fail with [`TrySendError::Closed`],
	/// but only once you have a message to lose. This asks without one.
	///
	/// costs a single `ppoll` — no reactor registration, no allocation, no task — so it
	/// is fine to call on a `Ref` you were just handed and may never send to. The answer
	/// is one-way: a dead `Ref` never becomes live again, so a `false` can go stale but a
	/// `true` cannot.
	///
	/// `false` is not a promise the next send will land — the peer may be closing as you
	/// ask, and there is nothing that could close that window. Treat this as "worth
	/// trying" rather than "guaranteed to arrive", and keep handling `Closed` on send.
	pub fn is_dead(&self) -> bool {
		wire::is_hung_up(self.inner.fd.as_fd())
	}

	/// resolves when the node behind this capability is gone
	///
	/// returns immediately if it already is. Meant to be raced against your own work:
	///
	/// ```no_run
	/// # async fn f(peer: strong_ipc::Ref, mut work: impl Future<Output = ()> + Unpin) {
	/// tokio::select! {
	///     () = peer.death_notification() => return, // nobody left to answer to
	///     () = &mut work => {}
	/// }
	/// # }
	/// ```
	///
	/// unlike [`Ref::is_dead`] this registers the socket with the reactor, which is the
	/// cost a `Ref` normally refuses to pay — so it happens on the first call and not
	/// before. The registration is shared by every waiter and outlives the wait, so a
	/// `Ref` that is watched repeatedly pays once and a `Ref` that is never watched pays
	/// nothing.
	///
	/// if the registration cannot be made at all — descriptor limit, no runtime — this
	/// never resolves, rather than claiming a death that has not happened. Guard against
	/// that with a timeout if a stuck waiter would wedge you.
	pub async fn death_notification(&self) {
		if self.is_dead() {
			return;
		}
		match self.inner.death_watch() {
			Ok(watch) => watch.hangup().await,
			Err(_) => std::future::pending().await,
		}
	}

	/// the underlying socket, for putting this capability on a message
	pub(crate) fn borrowed_fd(&self) -> BorrowedFd<'_> {
		self.inner.fd.as_fd()
	}

	/// hands `message` to the peer, waiting for room if there is none
	///
	/// takes the same inline fast path as [`Ref::try_send`], and only parks if the
	/// socket *and* the outbound queue are both full. Because it waits out backpressure,
	/// the only thing it can report is a peer that is actually gone.
	///
	/// this is what you want unless you have something better to do than wait. Ordering
	/// is preserved for any one sender: a caller awaiting here cannot have another send
	/// of its own in flight to overtake it.
	pub async fn send(&self, message: Message) -> Result<(), SendError> {
		let message = match self.try_send(message) {
			Ok(()) => return Ok(()),
			Err(TrySendError::Closed(m)) => return Err(SendError::Closed(m)),
			Err(TrySendError::TooLarge(m)) => return Err(SendError::TooLarge(m)),
			Err(TrySendError::Full(m)) => m,
		};
		// `try_send` only reports `Full` once the outbox exists, so this cannot be the
		// call that has to build it — but handle the failure rather than assuming
		let Ok(outbox) = self.inner.outbox() else {
			return Err(SendError::Closed(message));
		};
		outbox.send(message).await
	}

	/// hands `message` to the peer without ever blocking the caller
	///
	/// tries the syscall inline first, so in the common case there is no channel, no
	/// task wakeup and no scheduler hop. only a socket that is genuinely backed up falls
	/// through to the queue, and only a full queue gives `Full` back.
	///
	/// prefer [`Ref::send`] unless you genuinely cannot wait — a caller that spins on
	/// `Full` is busy-waiting, where `send` parks until there is room.
	///
	/// the error carries the `Message` back so a caller that hit backpressure can retry
	/// with it rather than losing it. that makes the `Err` variant 176 bytes, past
	/// clippy's 128-byte comfort line — but boxing it would put a heap allocation on
	/// exactly the path that is already under pressure, which is the wrong trade
	#[allow(clippy::result_large_err)]
	pub fn try_send(&self, message: Message) -> Result<(), TrySendError> {
		// refused here rather than at the syscall, so an oversized message never reaches
		// the wire and is never mistaken for a dead peer. the receiving side sizes its
		// buffer to exactly this limit, which is what makes truncation unreachable
		if message.data().len() > wire::MAX_MESSAGE_SIZE {
			return Err(TrySendError::TooLarge(message));
		}
		// a backlog can only exist if something already built the outbox, so an untouched
		// ref answers this with one relaxed load and no indirection
		let backed_up = self.inner.outbox.get().is_some_and(Outbox::is_backed_up);
		// a message with more fds than one `SCM_RIGHTS` can carry would fail the inline
		// build; leave it to the task so oversized sends fail exactly as they used to
		let inline_ok = message.fds().len() <= wire::MAX_FDS && !backed_up;
		if inline_ok {
			match wire::send_now(self.inner.fd.as_fd(), &message) {
				Ok(()) => return Ok(()),
				Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
					// socket buffer is full, so fall through and let the task drain it
				}
				// the peer is gone or the socket is broken. a `Ref` that can't reach its
				// node is as dead as a closed channel, so report it the same way
				Err(_) => return Err(TrySendError::Closed(message)),
			}
		}
		// first time we've needed the queue on this ref, so pay for it now
		let Ok(outbox) = self.inner.outbox() else {
			return Err(TrySendError::Closed(message));
		};
		outbox.push(message)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// the registry must not outlive what it describes
	///
	/// inode numbers are reissued the moment a socket closes, so an entry that survived
	/// its `Ref` would not just be dead weight — the next socket to be handed that number
	/// would be recognised as this one. `Weak` alone does not cover it: a `Weak` that
	/// cannot be upgraded still occupies the key. Only the removal in `RefInner::drop`,
	/// which runs while the descriptor is still open, closes that window.
	#[test]
	fn dropping_a_ref_unfiles_it() {
		let (fd, _peer) = crate::wire::socketpair().expect("socketpair");
		let id = socket_id(fd.as_fd()).expect("fstat");

		let first = Ref::from_fd(fd);
		assert!(REFS.contains_key(&id), "a live Ref should be filed");

		// a second descriptor for the same socket is the case the registry exists for
		let second = Ref::from_fd(first.inner.fd.try_clone().expect("dup"));
		assert!(
			first.is_same(&second),
			"a dup of the socket was not recognised"
		);
		assert_eq!(REFS.len(), 1, "one socket should occupy one entry");

		drop(second);
		assert!(
			REFS.contains_key(&id),
			"a Ref with another clone outstanding must stay filed"
		);

		drop(first);
		assert!(
			!REFS.contains_key(&id),
			"the last Ref to a socket left its entry behind, so the next socket to be \
			 issued this inode number would be mistaken for it"
		);
	}

	/// a `Ref` the registry could not key stays usable, it just never dedups
	#[test]
	fn an_unfiled_ref_removes_nothing_on_the_way_out() {
		let (fd, _peer) = crate::wire::socketpair().expect("socketpair");
		let id = socket_id(fd.as_fd()).expect("fstat");
		let filed = Ref::from_fd(fd);

		// what `from_fd` builds when `fstat` fails: same socket, no entry of its own
		let unfiled = Ref {
			inner: Arc::new(RefInner::new(
				filed.inner.fd.try_clone().expect("dup"),
				None,
			)),
		};
		assert!(!filed.is_same(&unfiled), "an unfiled Ref is nobody's twin");

		drop(unfiled);
		assert!(
			REFS.contains_key(&id),
			"dropping an unfiled Ref took someone else's entry with it"
		);
	}
}
