//! `Ref` — a sending capability, and the two-tier send that keeps it cheap

use crate::{message::Message, outbox::Outbox, wire};
use std::{
    os::fd::{AsFd, BorrowedFd, OwnedFd},
    path::Path,
    sync::{Arc, OnceLock},
};
use tokio::sync::mpsc::error::TrySendError;

struct RefInner {
    /// the socket, deliberately *not* registered with the reactor — every inline send is
    /// a bare `sendmsg` on this, which needs no readiness tracking at all
    fd: OwnedFd,
    /// captured at construction, since the outbox is built from `send_message`, which is
    /// sync and may not be running on a runtime thread
    runtime: Option<tokio::runtime::Handle>,
    outbox: OnceLock<Outbox>,
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
            }),
        }
    }

    /// the underlying socket, for putting this capability on a message
    pub(crate) fn borrowed_fd(&self) -> BorrowedFd<'_> {
        self.inner.fd.as_fd()
    }

    /// hands `message` to the peer without ever blocking the caller
    ///
    /// tries the syscall inline first, so in the common case there is no channel, no
    /// task wakeup and no scheduler hop. only a socket that is genuinely backed up falls
    /// through to the queue, and only a full queue gives `Full` back
    ///
    /// the error carries the `Message` back so a caller that hit backpressure can retry
    /// with it rather than losing it. that makes the `Err` variant 176 bytes, past
    /// clippy's 128-byte comfort line — but boxing it would put a heap allocation on
    /// exactly the path that is already under pressure, which is the wrong trade
    #[allow(clippy::result_large_err)]
    pub fn send_message(&self, message: Message) -> Result<(), TrySendError<Message>> {
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
