//! the backlog a [`Ref`] grows only once its socket has actually backed up
//!
//! building this is the expensive half of a `Ref`: a reactor registration (one
//! `epoll_ctl`, and another when it drops), a channel, and a spawned task. none of it is
//! touched while `sendmsg` keeps succeeding inline, and a `Ref` that is only ever sent to
//! at a sane rate never builds it at all — which matters because a `Ref` is constructed
//! for *every capability received*, so this used to be per-message cost.
//!
//! [`Ref`]: crate::Ref

use crate::{
    error::{SendError, TrySendError},
    message::Message,
    wire::Reactive,
};
use std::{
    os::fd::OwnedFd,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::mpsc;

/// how many messages may wait here before a sender is told to back off
const DEPTH: usize = 8;

pub(crate) struct Outbox {
    sender: mpsc::Sender<Message>,
    /// messages handed to the drain task that aren't on the wire yet
    ///
    /// gates the inline fast path in [`crate::Ref::try_send`]: while anything is
    /// waiting, a message that skipped the queue would overtake it, so everyone queues
    /// until the backlog is gone. the task decrements only *after* its `sendmsg` returns,
    /// so the message it is holding mid-send still counts
    pending: Arc<AtomicUsize>,
}

impl Outbox {
    pub(crate) fn build(
        fd: &OwnedFd,
        runtime: Option<&tokio::runtime::Handle>,
    ) -> std::io::Result<Self> {
        let runtime = runtime
            .cloned()
            .unwrap_or_else(tokio::runtime::Handle::current);

        // the drain task needs the socket *in* the reactor, so give it a dup and leave
        // the `Ref`'s own fd unregistered. both name the same open file description, so a
        // send on either is a send on the same socket, and ordering between them holds
        //
        // the `enter()` guard is load-bearing: registering with the reactor requires
        // runtime *context*, and holding a `Handle` is not the same thing. without it a
        // `try_send` from a non-runtime thread panics with "there is no reactor
        // running" the moment its socket backs up
        let socket = {
            let _guard = runtime.enter();
            Reactive::new(fd.try_clone()?)?
        };

        let (sender, receiver) = mpsc::channel(DEPTH);
        let pending = Arc::new(AtomicUsize::new(0));
        // deliberately not an AbortOnDrop: once the last `Ref` goes, mpsc closes the
        // channel but still drains what's queued before `recv()` gives `None`. aborting
        // would race with in-flight sends and lose them
        runtime.spawn(Self::drain(receiver, socket, pending.clone()));
        Ok(Self { sender, pending })
    }

    /// is anything still waiting to reach the wire?
    ///
    /// while this is true the inline fast path must stay shut, or a new message would
    /// overtake one already queued
    pub(crate) fn is_backed_up(&self) -> bool {
        self.pending.load(Ordering::Acquire) != 0
    }

    /// queue `message` behind whatever is already waiting, or give it straight back
    #[allow(clippy::result_large_err)]
    pub(crate) fn push(&self, message: Message) -> Result<(), TrySendError> {
        // claimed before the push so a concurrent fast path sees us coming and queues
        // behind us instead of jumping the line
        self.pending.fetch_add(1, Ordering::AcqRel);
        match self.sender.try_send(message) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.pending.fetch_sub(1, Ordering::AcqRel);
                Err(match e {
                    mpsc::error::TrySendError::Full(m) => TrySendError::Full(m),
                    mpsc::error::TrySendError::Closed(m) => TrySendError::Closed(m),
                })
            }
        }
    }

    /// queue `message`, waiting for room if the queue is full
    ///
    /// takes the slot *before* claiming it in `pending`, so the window between the claim
    /// and the push stays as tight as [`Outbox::push`]'s. Incrementing first and then
    /// awaiting would hold the inline fast path shut for the entire wait
    pub(crate) async fn send(&self, message: Message) -> Result<(), SendError> {
        let Ok(permit) = self.sender.reserve().await else {
            return Err(SendError::Closed(message));
        };
        self.pending.fetch_add(1, Ordering::AcqRel);
        permit.send(message);
        Ok(())
    }

    async fn drain(
        mut message_rx: mpsc::Receiver<Message>,
        socket: Reactive,
        pending: Arc<AtomicUsize>,
    ) -> std::io::Result<()> {
        loop {
            let Some(message) = message_rx.recv().await else {
                return Ok(());
            };
            // the same `wire::send_now` the inline fast path uses, with readiness in
            // front of it — one implementation, so the two paths cannot drift apart
            let result = socket.send(&message).await;
            // released only now, so the fast path stays shut until this really is on the
            // wire. on error too — the message is gone either way, and leaving the count
            // raised would wedge the fast path forever
            pending.fetch_sub(1, Ordering::AcqRel);
            result?;
        }
    }
}
