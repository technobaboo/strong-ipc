//! the backlog a [`Ref`] grows only once its socket has actually backed up
//!
//! building this is the expensive half of a `Ref`: a reactor registration (one
//! `epoll_ctl`, and another when it drops), a channel, and a spawned task. none of it is
//! touched while `sendmsg` keeps succeeding inline, and a `Ref` that is only ever sent to
//! at a sane rate never builds it at all — which matters because a `Ref` is constructed
//! for *every capability received*, so this used to be per-message cost.
//!
//! [`Ref`]: crate::Ref

use crate::{message::Message, wire::EXPECTED_ANCILLARY_BUFFER_SIZE};
use std::{
    os::fd::OwnedFd,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio_seqpacket::{UnixSeqpacket, ancillary::AncillaryMessageWriter, borrow_fd::BorrowFd};

/// how many messages may wait here before a sender is told to back off
const DEPTH: usize = 8;

pub(crate) struct Outbox {
    sender: mpsc::Sender<Message>,
    /// messages handed to the drain task that aren't on the wire yet
    ///
    /// gates the inline fast path in [`crate::Ref::send_message`]: while anything is
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
        // the drain task needs the socket *in* the reactor, so give it a dup and leave
        // the `Ref`'s own fd unregistered. both name the same open file description, so a
        // send on either is a send on the same socket, and ordering between them holds
        let seqpacket = UnixSeqpacket::try_from(fd.try_clone()?)?;
        let (sender, receiver) = mpsc::channel(DEPTH);
        let pending = Arc::new(AtomicUsize::new(0));
        let runtime = runtime
            .cloned()
            .unwrap_or_else(tokio::runtime::Handle::current);
        // deliberately not an AbortOnDrop: once the last `Ref` goes, mpsc closes the
        // channel but still drains what's queued before `recv()` gives `None`. aborting
        // would race with in-flight sends and lose them
        runtime.spawn(Self::drain(receiver, seqpacket, pending.clone()));
        Ok(Self { sender, pending })
    }

    /// is anything still waiting to reach the wire?
    ///
    /// while this is true the inline fast path must stay shut, or a new message would
    /// overtake one already queued
    pub(crate) fn is_backed_up(&self) -> bool {
        self.pending.load(Ordering::Acquire) != 0
    }

    /// queue `message` behind whatever is already waiting
    #[allow(clippy::result_large_err)]
    pub(crate) fn push(&self, message: Message) -> Result<(), TrySendError<Message>> {
        // claimed before the push so a concurrent fast path sees us coming and queues
        // behind us instead of jumping the line
        self.pending.fetch_add(1, Ordering::AcqRel);
        match self.sender.try_send(message) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.pending.fetch_sub(1, Ordering::AcqRel);
                Err(e)
            }
        }
    }

    async fn drain(
        mut message_rx: mpsc::Receiver<Message>,
        seqpacket: UnixSeqpacket,
        pending: Arc<AtomicUsize>,
    ) -> std::io::Result<()> {
        let mut ancillary_buffer = vec![0_u8; EXPECTED_ANCILLARY_BUFFER_SIZE];
        loop {
            let Some(message) = message_rx.recv().await else {
                return Ok(());
            };

            let mut writer = AncillaryMessageWriter::new(&mut ancillary_buffer);
            writer.add_fds(message.fds().iter().map(|f| f.borrow_fd()))?;
            let result = seqpacket
                .send_with_ancillary(message.data(), &mut writer)
                .await;
            // released only now, so the fast path stays shut until this really is on the
            // wire. on error too — the message is gone either way, and leaving the count
            // raised would wedge the fast path forever
            pending.fetch_sub(1, Ordering::AcqRel);
            result?;
        }
    }
}
