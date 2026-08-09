#![allow(clippy::result_large_err)]

use smallvec::SmallVec;
use std::{
    ffi::c_void,
    ops::Deref,
    os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::{
    sync::mpsc::{self, error::TrySendError},
    task::JoinSet,
};
use tokio_seqpacket::{
    UCred, UnixSeqpacket, UnixSeqpacketListener,
    ancillary::{AncillaryMessageWriter, OwnedAncillaryMessage},
    borrow_fd::BorrowFd,
};
use tokio_util::task::AbortOnDrop;

pub const MAX_FDS: usize = 253;
pub const MAX_ANCILLARY_BUFFER_SIZE: usize = {
    // one block for the fds...
    (unsafe { libc::CMSG_SPACE((MAX_FDS * size_of::<RawFd>()) as u32) }
    // ...plus one for SCM_CREDENTIALS, since recv_loop pulls both out of the
    // same buffer
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

/// a fd to send as ancillary data on a [`Message`]
///
/// `SCM_RIGHTS` only borrows the fd, the kernel dups it into the receiver's table and
/// leaves yours alone, so this just has to keep something alive that can hand out a
/// [`BorrowedFd`] at send time. never needs a local `dup` either way
pub enum SendFd {
    /// a fd owned outright, e.g. forwarded out of `Handler::handle`
    Owned(OwnedFd),
    /// a clone of a [`Ref`]'s socket, shared rather than duped
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
    /// embeds `r`'s socket so its fd goes over the wire
    ///
    /// `r` stays usable after, and can go on as many messages as you want
    pub fn add_ref(&mut self, r: &Ref) {
        self.fds.push(SendFd::Ref(r.seqpacket.clone()));
    }
    /// embeds a raw received fd, e.g. to forward one out of `Handler::handle`
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
        let task = tokio::spawn(recv_loop(handler.clone(), seqpacket));
        Ok(Self {
            handler,
            ref_,
            _task: AbortOnDrop::new(task.abort_handle()),
        })
    }
    /// a `Ref` pointing back at this node, alive as long as the node is
    pub fn get_ref(&self) -> &Ref {
        &self.ref_
    }
}

/// the one spot where a message on the wire becomes a handler call
///
/// free-standing since node and boundnode share no type, they just both need something
/// to feed a handler. `Ok` means the peer hung up, not an error, though that's just a
/// zero-length read, same as an empty message, so sending one looks like a disconnect
async fn recv_loop<H: Handler>(handler: Arc<H>, seqpacket: UnixSeqpacket) -> std::io::Result<()> {
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

        handler
            .handle(&mut buf[..message_info.bytes_read()], fd_buf, peer_cred)
            .await;
    }
}

/// a node listening on a path instead of a socketpair
///
/// caps have a bootstrap problem: if the only way to get a ref is someone handing you an
/// fd, two unrelated processes can never meet. a path is the one name that isn't itself a
/// capability, so it's the door you knock on before you have any
///
/// accepted conns only receive, no ref back out, peers pass one in-band with
/// [`Message::add_ref`] if they want a reply. they all share the one handler and die
/// with the boundnode
pub struct BoundNode<H: Handler> {
    handler: Arc<H>,
    /// only set if we made the socket file, so we know whether it's ours to unlink
    path: Option<PathBuf>,
    _accept_task: AbortOnDrop,
}
impl<H: Handler> Deref for BoundNode<H> {
    type Target = H;

    fn deref(&self) -> &Self::Target {
        &self.handler
    }
}
impl<H: Handler> BoundNode<H> {
    /// binds a socket at `path`, unlinked again on drop
    ///
    /// won't clobber a path that already exists, a stale socket left by a crash fails
    /// with `AddrInUse` instead, since replacing it could yank the path out from under
    /// something still alive
    pub fn bind<P: AsRef<Path>>(path: P, handler: H) -> std::io::Result<Self> {
        Self::bind_raw(path, Arc::new(handler))
    }
    pub fn bind_raw<P: AsRef<Path>>(path: P, handler: Arc<H>) -> std::io::Result<Self> {
        let path = path.as_ref();
        let listener = UnixSeqpacketListener::bind(path)?;

        let mut node = Self::from_listener(listener, handler)?;
        node.path = Some(path.to_owned());
        Ok(node)
    }
    /// takes over an already-bound listener, e.g. one handed to us by a service manager
    ///
    /// leaves the socket file alone on drop since it isn't ours to remove
    pub fn from_listener(
        listener: UnixSeqpacketListener,
        handler: Arc<H>,
    ) -> std::io::Result<Self> {
        let task = tokio::spawn(Self::task(listener, handler.clone()));
        Ok(Self {
            handler,
            path: None,
            _accept_task: AbortOnDrop::new(task.abort_handle()),
        })
    }
    async fn task(mut listener: UnixSeqpacketListener, handler: Arc<H>) -> std::io::Result<()> {
        // lives in the task and not the boundnode so it needs no locking, killing this
        // task drops the set, and every connection with it
        let mut connections = JoinSet::new();
        loop {
            let seqpacket = listener.accept().await?;
            // clear out whoever hung up since, so this doesn't grow forever
            while connections.try_join_next().is_some() {}
            connections.spawn(recv_loop(handler.clone(), seqpacket));
        }
    }
}
impl<H: Handler> Drop for BoundNode<H> {
    fn drop(&mut self) {
        if let Some(path) = &self.path {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[derive(Clone)]
pub struct Ref {
    sender: mpsc::Sender<Message>,
    seqpacket: Arc<UnixSeqpacket>,
    /// messages handed to the drain task that aren't on the wire yet
    ///
    /// gates the inline fast path in [`Ref::send_message`]: while anything is waiting,
    /// a message that skipped the queue would overtake it, so everyone queues until the
    /// backlog is gone. the task decrements only *after* its `sendmsg` returns, so the
    /// message it is holding mid-send still counts
    pending: Arc<AtomicUsize>,
}

/// one non-blocking `sendmsg`, straight to the kernel
///
/// no reactor, no task, no await — `MSG_DONTWAIT` means this either completes or fails,
/// it never parks the caller. `message` is only borrowed, so a `WouldBlock` leaves it
/// intact for the caller to queue instead
fn try_send_now(seqpacket: &UnixSeqpacket, message: &Message) -> std::io::Result<()> {
    // `AncillaryMessageWriter` realigns whatever buffer it is handed, and it hands back
    // only a length, not the realigned slice — so align it up front and check that no
    // bytes were skipped, otherwise the pointer below wouldn't match the length
    #[repr(align(8))]
    struct CmsgBuf([u8; MAX_ANCILLARY_BUFFER_SIZE]);
    debug_assert_eq!(
        align_of::<CmsgBuf>() % align_of::<libc::cmsghdr>(),
        0,
        "cmsg buffer is under-aligned for this target"
    );
    let mut cmsg = CmsgBuf([0; MAX_ANCILLARY_BUFFER_SIZE]);

    let control_len = if message.fds.is_empty() {
        0
    } else {
        let mut writer = AncillaryMessageWriter::new(&mut cmsg.0);
        debug_assert_eq!(
            writer.capacity(),
            MAX_ANCILLARY_BUFFER_SIZE,
            "buffer was realigned, so its start no longer matches the write pointer"
        );
        writer.add_fds(message.fds.iter().map(|f| f.borrow_fd()))?;
        writer.len()
    };

    let mut iov = libc::iovec {
        iov_base: message.data.as_ptr() as *mut c_void,
        iov_len: message.data.len(),
    };
    // SAFETY: msghdr is a plain C struct with no invalid bit patterns, and every pointer
    // written into it below outlives the sendmsg call
    let mut header: libc::msghdr = unsafe { std::mem::zeroed() };
    header.msg_iov = &mut iov;
    header.msg_iovlen = 1;
    if control_len > 0 {
        header.msg_control = cmsg.0.as_mut_ptr() as *mut c_void;
        header.msg_controllen = control_len as _;
    }

    // MSG_NOSIGNAL so a dead peer surfaces as EPIPE instead of killing the process
    let sent = unsafe {
        libc::sendmsg(
            seqpacket.as_fd().as_raw_fd(),
            &header,
            libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
        )
    };
    if sent < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}
impl Ref {
    /// connects to the [`BoundNode`] at `path`, giving you a ref that sends to it
    ///
    /// nothing comes back this way, if you want a reply, put a ref of your own on the
    /// message with [`Message::add_ref`]
    pub async fn connect<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        Self::from_seqpacket(UnixSeqpacket::connect(path).await?)
    }
    pub fn from_owned_fd(fd: OwnedFd) -> std::io::Result<Self> {
        Self::from_seqpacket(UnixSeqpacket::try_from(fd)?)
    }
    pub fn from_seqpacket(seqpacket: UnixSeqpacket) -> std::io::Result<Self> {
        let (sender, receiver) = mpsc::channel(8);
        let seqpacket = Arc::new(seqpacket);
        let pending = Arc::new(AtomicUsize::new(0));
        // deliberately not an AbortOnDrop: once the last `Ref` goes, mpsc closes the
        // channel but still drains what's queued before `recv()` gives `None`. aborting
        // would race with in-flight `send_message` calls and lose them
        tokio::spawn(Self::task(receiver, seqpacket.clone(), pending.clone()));
        Ok(Self {
            sender,
            seqpacket,
            pending,
        })
    }
    async fn task(
        mut message_rx: mpsc::Receiver<Message>,
        seqpacket: Arc<UnixSeqpacket>,
        pending: Arc<AtomicUsize>,
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
            let result = seqpacket
                .send_with_ancillary(&message.data, &mut ancillary_message_writer)
                .await;
            // released only now, so the fast path stays shut until this really is on the
            // wire. on error too — the message is gone either way, and leaving the count
            // raised would wedge the fast path forever
            pending.fetch_sub(1, Ordering::AcqRel);
            result?;
        }
    }
    /// hands `message` to the peer without ever blocking the caller
    ///
    /// tries the syscall inline first, so in the common case there is no channel, no
    /// task wakeup and no scheduler hop. only a socket that is genuinely backed up falls
    /// through to the queue, and only a full queue gives `Full` back
    pub fn send_message(&self, message: Message) -> Result<(), TrySendError<Message>> {
        // a message with more fds than one `SCM_RIGHTS` can carry would fail the inline
        // build; leave it to the task so oversized sends fail exactly as they used to
        let inline_ok =
            message.fds.len() <= MAX_FDS && self.pending.load(Ordering::Acquire) == 0;
        if inline_ok {
            match try_send_now(&self.seqpacket, &message) {
                Ok(()) => return Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // socket buffer is full, so fall through and let the task drain it
                }
                // the peer is gone or the socket is broken. a `Ref` that can't reach its
                // node is as dead as a closed channel, so report it the same way
                Err(_) => return Err(TrySendError::Closed(message)),
            }
        }
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
}
