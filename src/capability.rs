//! `Ref` — a sending capability, and the two-tier send that keeps it cheap

use crate::{
    error::{SendError, TrySendError},
    message::Message,
    outbox::Outbox,
    wire,
};
use std::{
    os::fd::{AsFd, BorrowedFd, OwnedFd},
    path::Path,
    sync::{Arc, OnceLock},
};

struct RefInner {
    /// the socket, deliberately *not* registered with the reactor — every inline send is
    /// a bare `sendmsg` on this, which needs no readiness tracking at all
    fd: OwnedFd,
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
    /// costs one allocation and nothing else — no reactor registration, no task, nothing
    /// that can fail. this is the hot path: a node handling capabilities builds one of
    /// these per message
    pub fn from_owned_fd(fd: OwnedFd) -> Self {
        Self::from_fd(fd)
    }
    pub(crate) fn from_fd(fd: OwnedFd) -> Self {
        Self {
            inner: Arc::new(RefInner {
                fd,
                runtime: tokio::runtime::Handle::try_current().ok(),
                outbox: OnceLock::new(),
                death: OnceLock::new(),
            }),
        }
    }

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
