#![allow(clippy::result_large_err)]

use smallvec::SmallVec;
use std::{
    ops::Deref,
    os::fd::{AsFd, BorrowedFd, OwnedFd, RawFd},
    sync::Arc,
};
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio_seqpacket::{
    UCred, UnixSeqpacket,
    ancillary::{AncillaryMessageWriter, OwnedAncillaryMessage},
    borrow_fd::BorrowFd,
};
use tokio_util::task::AbortOnDrop;

pub const MAX_FDS: usize = 253;
pub const MAX_ANCILLARY_BUFFER_SIZE: usize = {
    // one block for the fds...
    (unsafe { libc::CMSG_SPACE((MAX_FDS * size_of::<RawFd>()) as u32) }
    // ...plus one block for SCM_CREDENTIALS, since Node::task pulls both
    // out of the same buffer
    + unsafe { libc::CMSG_SPACE(size_of::<libc::ucred>() as u32) }) as usize
};

pub trait Handler: Send + Sync + 'static {
    fn handle(
        &self,
        data: &mut [u8],
        fds: FdVec,
        creds: Option<UCred>,
    ) -> impl Future<Output = ()> + Send + Sync;
}

pub type FdVec = SmallVec<[OwnedFd; MAX_FDS]>;

/// A fd to be sent as ancillary data on a [`Message`].
///
/// `sendmsg`/`SCM_RIGHTS` only ever borrows the fd it sends — the kernel duplicates
/// it into the receiver's fd table, the sender's fd is untouched — so this only needs
/// to keep *something* alive that can yield a [`BorrowedFd`] at send time. No `dup` is
/// ever required locally, whichever variant this is.
pub enum SendFd {
    /// A fd owned outright, e.g. one forwarded from `Handler::handle`.
    Owned(OwnedFd),
    /// A clone of a [`Ref`]'s underlying socket, shared rather than duplicated.
    Ref(Arc<UnixSeqpacket>),
}
impl AsFd for SendFd {
    fn as_fd(&self) -> BorrowedFd<'_> {
        match self {
            SendFd::Owned(fd) => fd.as_fd(),
            SendFd::Ref(seqpacket) => seqpacket.as_fd(),
        }
    }
}

pub struct Message {
    data: Vec<u8>,
    fds: SmallVec<[SendFd; MAX_FDS]>,
    // peer_creds: bool,
}
impl Message {
    pub fn from_data(data: Vec<u8>) -> Self {
        Self {
            data,
            fds: SmallVec::new(),
        }
    }
    /// Embeds `r`'s socket so its fd is sent over the wire.
    ///
    /// `r` stays fully usable afterward, and can be embedded in any number of messages.
    pub fn add_ref(&mut self, r: &Ref) {
        self.fds.push(SendFd::Ref(r.seqpacket.clone()));
    }
    /// Embeds a raw received fd, e.g. to forward one from `Handler::handle`.
    pub fn add_fd(&mut self, fd: OwnedFd) {
        self.fds.push(SendFd::Owned(fd));
    }
}

pub struct Node<H: Handler> {
    handler: Arc<H>,
    ref_: Ref,
    _task: AbortOnDrop,
}
impl<H: Handler> Deref for Node<H> {
    type Target = H;

    fn deref(&self) -> &Self::Target {
        &self.handler
    }
}
impl<H: Handler> Node<H> {
    pub fn new(handler: H) -> std::io::Result<Node<H>> {
        Node::new_raw(Arc::new(handler))
    }
    pub fn new_raw(handler: Arc<H>) -> std::io::Result<Node<H>> {
        let (tx, rx) = UnixSeqpacket::pair()?;
        let ref_ = Ref::from_seqpacket(tx)?;

        Node::from_seqpacket(handler, rx, ref_)
    }
    fn from_seqpacket(
        handler: Arc<H>,
        seqpacket: UnixSeqpacket,
        ref_: Ref,
    ) -> std::io::Result<Self> {
        let task = tokio::spawn(Self::task(handler.clone(), seqpacket));
        Ok(Self {
            handler,
            ref_,
            _task: AbortOnDrop::new(task.abort_handle()),
        })
    }
    /// A `Ref` pointing back at this node, kept alive as long as the node is.
    pub fn get_ref(&self) -> &Ref {
        &self.ref_
    }
    async fn task(handler: Arc<H>, seqpacket: UnixSeqpacket) -> std::io::Result<()> {
        let mut buf = vec![0u8; 8192];
        let mut ancillary_buf = vec![0u8; MAX_ANCILLARY_BUFFER_SIZE];
        loop {
            let (message_info, ancillary_messages) = seqpacket
                .recv_with_ancillary(&mut buf, &mut ancillary_buf)
                .await?;
            if message_info.bytes_read() == 0 {
                return Ok(());
            }
            let mut fd_buf = SmallVec::new();
            let mut peer_cred: Option<UCred> = None;
            for ancillary_data in ancillary_messages.into_messages() {
                match ancillary_data {
                    OwnedAncillaryMessage::FileDescriptors(fds) => fd_buf.extend(fds),
                    OwnedAncillaryMessage::Credentials(mut creds) => {
                        peer_cred = creds.next();
                    }
                    OwnedAncillaryMessage::Other(_) => (),
                }
            }

            handler.handle(buf.as_mut_slice(), fd_buf, peer_cred).await;
        }
    }
}

#[derive(Clone)]
pub struct Ref {
    sender: mpsc::Sender<Message>,
    seqpacket: Arc<UnixSeqpacket>,
}
impl Ref {
    pub fn from_owned_fd(fd: OwnedFd) -> std::io::Result<Self> {
        Self::from_seqpacket(UnixSeqpacket::try_from(fd)?)
    }
    pub fn from_seqpacket(seqpacket: UnixSeqpacket) -> std::io::Result<Self> {
        let (sender, receiver) = mpsc::channel(8);
        let seqpacket = Arc::new(seqpacket);
        // Deliberately not tied to an AbortOnDrop: once the last `Ref` (and its
        // `sender` clone) is dropped, `mpsc` closes the channel but still drains
        // whatever's already queued before `recv()` returns `None`. Aborting here
        // instead would race with in-flight `send_message` calls and drop them.
        tokio::spawn(Self::task(receiver, seqpacket.clone()));
        Ok(Self { sender, seqpacket })
    }
    async fn task(
        mut message_rx: mpsc::Receiver<Message>,
        seqpacket: Arc<UnixSeqpacket>,
    ) -> std::io::Result<()> {
        let mut ancillary_buffer = vec![0_u8; MAX_ANCILLARY_BUFFER_SIZE];
        loop {
            let Some(message) = message_rx.recv().await else {
                return Ok(());
            };

            let mut ancillary_message_writer = AncillaryMessageWriter::new(&mut ancillary_buffer);
            // if message.peer_creds {
            // ancillary_message_writer.add_ucreds([UCred {}])
            // }
            ancillary_message_writer.add_fds(message.fds.iter().map(|f| f.borrow_fd()))?;
            seqpacket
                .send_with_ancillary(&message.data, &mut ancillary_message_writer)
                .await?;
        }
    }
    pub fn send_message(&self, message: Message) -> Result<(), TrySendError<Message>> {
        self.sender.try_send(message)
    }
}
