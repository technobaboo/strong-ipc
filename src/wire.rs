//! the only module that talks to the kernel
//!
//! everything above this file is plain Rust moving [`Message`]s around; this is where one
//! becomes bytes and descriptors on a socket, and the only place a syscall happens.
//!
//! there is exactly one implementation of "send a `Message`" — `send_now` — used both
//! by [`Ref`]'s inline fast path and by the outbox's drain task. That matters: when the
//! two had separate implementations they silently diverged, disagreeing about `MSG_EOR`
//! and about whether an fd-less message still carried an empty `SCM_RIGHTS` block.
//!
//! [`Message`]: crate::Message
//! [`Ref`]: crate::Ref

use crate::message::{FdVec, Message};
use rustix::event::{PollFd, PollFlags, Timespec};
use rustix::net::{
    self, AddressFamily, RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, ReturnFlags,
    SendAncillaryBuffer, SendAncillaryMessage, SendFlags, SocketAddrUnix, SocketFlags, SocketType,
    UCred,
};
use smallvec::SmallVec;
use std::{
    io::{IoSlice, IoSliceMut},
    mem::MaybeUninit,
    os::fd::{AsFd, BorrowedFd, OwnedFd},
    path::Path,
};
use tokio::io::{Interest, unix::AsyncFd};

/// the kernel's `SCM_MAX_FD` — the most descriptors one `SCM_RIGHTS` block can carry
pub const MAX_FDS: usize = 253;

/// how many descriptors a message is expected to carry in the common case
///
/// sizes the inline capacity of [`crate::FdVec`] and `Message`'s descriptor list, and the
/// stack ancillary buffer on the send path. Nothing is capped at this — it is purely the
/// point past which those spill to the heap
pub(crate) const EXPECTED_FDS: usize = 8;

/// the send-side stack buffer: enough for `EXPECTED_FDS` (8) descriptors plus credentials
pub const EXPECTED_ANCILLARY_BUFFER_SIZE: usize =
    rustix::cmsg_space!(ScmRights(EXPECTED_FDS), ScmCredentials(1));

/// the receive-side buffer, sized for the largest message the kernel will ever deliver
///
/// about a kilobyte per connection, which is cheap, and it makes `MSG_CTRUNC` structurally
/// impossible. That matters more here than the memory does: a truncated control message
/// means the kernel dropped descriptors, and in a capability system a silently dropped
/// descriptor is a capability that vanished with no error anywhere
pub(crate) const MAX_ANCILLARY_BUFFER_SIZE: usize =
    rustix::cmsg_space!(ScmRights(MAX_FDS), ScmCredentials(1));

/// the largest payload a [`Message`] may carry — **the one number to tune**
///
/// this governs two things at once, and that is the point:
///   - [`crate::Ref::try_send`] refuses anything larger, before it leaves the process
///   - every receive buffer is exactly this size, and never needs to grow
///
/// because no accepted send can exceed it, a receive buffer of this size can never be
/// truncated. That is what lets the receive path be a single `recvmsg` with no size probe
/// in front of it, and what keeps the cost per node fixed rather than per-peer.
///
/// the memory is per *connection*, so at thousands of nodes this is the number that
/// decides your footprint: 8 KiB × 1000 nodes is 8 MiB. Raising it is a linear cost.
///
/// it is deliberately small. A capability system's answer to a large payload is not a
/// larger payload — it is a `memfd` or dmabuf attached with [`crate::Message::add_fd`],
/// which costs one descriptor, is zero-copy, and does not touch this budget at all.
///
/// the kernel enforces its own ceiling on top of this (`SO_SNDBUF`, 256 KiB by default,
/// reported as `EMSGSIZE`), so raising this past that will not work.
pub const MAX_MESSAGE_SIZE: usize = 8192;

/// `RLIM_INFINITY` is `None` on the way in and [`usize::MAX`] on the way out — there is no
/// number to report for "no limit", and the largest `usize` is the closest true thing
fn as_count(value: Option<u64>) -> usize {
    value.map_or(usize::MAX, |v| v.try_into().unwrap_or(usize::MAX))
}

/// the inverse: [`usize::MAX`] asks for `RLIM_INFINITY`, which only a process that may
/// raise its hard limit will actually get
fn as_rlim(count: usize) -> Option<u64> {
    (count != usize::MAX).then(|| u64::try_from(count).unwrap_or(u64::MAX))
}

/// this process's current open-descriptor limit — the soft `RLIMIT_NOFILE`
///
/// the soft limit is the one that bites: it is what the kernel checks, and what returns
/// `EMFILE`. See [`maximize_fd_limit`] for why that matters more here than in most crates.
///
/// the `Result` is for symmetry with the other two, and against the day this needs a
/// syscall that can fail; today reading the limit cannot.
pub fn fd_limit() -> std::io::Result<usize> {
    Ok(as_count(
        rustix::process::getrlimit(rustix::process::Resource::Nofile).current,
    ))
}

/// sets this process's open-descriptor limit to exactly `limit`
///
/// only the soft limit moves. Asking for more than the hard limit fails (`EPERM`, or
/// `EINVAL` past the kernel's own `nr_open`) rather than being clamped to it — a silent
/// clamp would hand back a process that quietly holds fewer capabilities than it was told
/// to. [`maximize_fd_limit`] is the version that wants the ceiling without naming it.
///
/// lowering is allowed and is not undoable in general: a process that drops its soft limit
/// can raise it again, but only up to the hard limit it still has.
///
/// returns the new soft limit, which on success is `limit`.
pub fn set_fd_limit(limit: usize) -> std::io::Result<usize> {
    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};

    let current = as_rlim(limit);
    // the hard limit is preserved rather than passed through as `None`: on Linux `None`
    // means infinity, so echoing it back would be a request to *raise* the ceiling
    setrlimit(
        Resource::Nofile,
        Rlimit {
            current,
            maximum: getrlimit(Resource::Nofile).maximum,
        },
    )?;

    Ok(as_count(current))
}

/// raises this process's open-descriptor limit to its hard ceiling, returning the new soft
/// limit
///
/// a capability is a descriptor, so in this crate the descriptor limit *is* the ceiling on
/// how many things you can hold the authority to talk to. The default soft limit is often
/// 1024 while the hard limit is orders of magnitude higher, and nothing but the soft limit
/// stands between the two — raising it needs no privilege, only the asking.
///
/// this is the entire reason it is public: `EMFILE` on a `recvmsg` carrying `SCM_RIGHTS`
/// means the kernel dropped capabilities that were already in flight, which is a far worse
/// failure than being told up front that you cannot have them.
///
/// the hard limit is left alone — lowering it is irreversible, and raising it is what
/// actually needs privilege. When the soft limit is already at the ceiling this changes
/// nothing and just reports it. An unlimited (`RLIM_INFINITY`) limit is reported as
/// [`usize::MAX`], since there is no number to give.
pub fn maximize_fd_limit() -> std::io::Result<usize> {
    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};

    // None is RLIM_INFINITY on both fields: no ceiling to raise to, nothing to raise
    let limit = getrlimit(Resource::Nofile);

    if limit.current != limit.maximum {
        setrlimit(
            Resource::Nofile,
            Rlimit {
                current: limit.maximum,
                maximum: limit.maximum,
            },
        )?;
    }

    Ok(as_count(limit.maximum))
}

fn flags_for_send() -> SendFlags {
    // DONTWAIT so this never parks the caller; NOSIGNAL so a dead peer surfaces as EPIPE
    // instead of killing the process; EOR to mark the record boundary, which is what
    // tokio-seqpacket did and what SOCK_SEQPACKET semantics call for
    SendFlags::DONTWAIT | SendFlags::NOSIGNAL | SendFlags::EOR
}

/// a connected `SOCK_SEQPACKET` pair, both ends non-blocking and close-on-exec
pub(crate) fn socketpair() -> std::io::Result<(OwnedFd, OwnedFd)> {
    Ok(net::socketpair(
        AddressFamily::UNIX,
        SocketType::SEQPACKET,
        SocketFlags::NONBLOCK | SocketFlags::CLOEXEC,
        None,
    )?)
}

/// binds a listening socket at `path`
///
/// deliberately does not unlink an existing path first: a stale socket left by a crash
/// fails with `AddrInUse` rather than being clobbered, since replacing it could yank the
/// path out from under something still alive
pub(crate) fn bind(path: &Path) -> std::io::Result<OwnedFd> {
    let addr = SocketAddrUnix::new(path)?;
    let fd = net::socket_with(
        AddressFamily::UNIX,
        SocketType::SEQPACKET,
        SocketFlags::NONBLOCK | SocketFlags::CLOEXEC,
        None,
    )?;
    net::bind(&fd, &addr)?;
    net::listen(&fd, 128)?;
    Ok(fd)
}

/// connects to a bound socket at `path`
pub(crate) fn connect(path: &Path) -> std::io::Result<OwnedFd> {
    let addr = SocketAddrUnix::new(path)?;
    let fd = net::socket_with(
        AddressFamily::UNIX,
        SocketType::SEQPACKET,
        SocketFlags::NONBLOCK | SocketFlags::CLOEXEC,
        None,
    )?;
    net::connect(&fd, &addr)?;
    Ok(fd)
}

/// one non-blocking `sendmsg`, straight to the kernel
///
/// no reactor, no task, no await — `MSG_DONTWAIT` means this either completes or fails,
/// it never parks the caller. `message` is only borrowed, so a `WouldBlock` leaves it
/// intact for the caller to queue instead.
///
/// the ancillary buffer is sized from the *actual* descriptor count rather than a fixed
/// `EXPECTED_FDS` allowance, which is what lets [`MAX_FDS`] mean what it says
pub(crate) fn send_now(fd: BorrowedFd<'_>, message: &Message) -> std::io::Result<()> {
    let iov = [IoSlice::new(message.data())];

    // no descriptors means no control message at all. building an empty `SCM_RIGHTS`
    // block instead would put 16 bytes of nothing on the wire and hand the receiver a
    // spurious empty-descriptor-list to unpack
    if message.fds().is_empty() {
        net::sendmsg(fd, &iov, &mut SendAncillaryBuffer::default(), flags_for_send())?;
        return Ok(());
    }

    let borrowed: SmallVec<[BorrowedFd<'_>; EXPECTED_FDS]> =
        message.fds().iter().map(|f| f.as_fd()).collect();

    let mut stack = [MaybeUninit::uninit(); EXPECTED_ANCILLARY_BUFFER_SIZE];
    let mut spilled;
    let space: &mut [MaybeUninit<u8>] = if borrowed.len() <= EXPECTED_FDS {
        &mut stack
    } else {
        spilled = vec![MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(borrowed.len()))];
        &mut spilled
    };

    let mut control = SendAncillaryBuffer::new(space);
    if !control.push(SendAncillaryMessage::ScmRights(&borrowed)) {
        // the buffer is sized from `borrowed.len()` immediately above, so this cannot
        // happen unless that sizing is wrong
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("could not fit {} descriptors in the control buffer", borrowed.len()),
        ));
    }
    net::sendmsg(fd, &iov, &mut control, flags_for_send())?;
    Ok(())
}

/// what came off the wire
pub(crate) struct Received {
    /// the true length of the datagram, which may exceed the buffer it was read into
    ///
    /// `MSG_TRUNC` is always requested, so this is the real size even when the message
    /// did not fit — which is what lets the receive loop grow to match its peers instead
    /// of silently delivering a shortened payload
    pub bytes: usize,
    pub fds: FdVec,
    pub creds: Option<UCred>,
}

impl Received {
    /// did the payload not fit in the buffer it was read into?
    pub fn truncated(&self, buffer_len: usize) -> bool {
        self.bytes > buffer_len
    }
}

/// one non-blocking `recvmsg`
pub(crate) fn recv_now(
    fd: BorrowedFd<'_>,
    buf: &mut [u8],
    control_space: &mut [MaybeUninit<u8>],
) -> std::io::Result<Received> {
    let mut iov = [IoSliceMut::new(buf)];
    let mut ancillary = RecvAncillaryBuffer::new(control_space);
    // CMSG_CLOEXEC so a received descriptor can't leak through an exec that races with
    // us; TRUNC so `bytes` reports the datagram's real length rather than what we caught
    let msg = net::recvmsg(
        fd,
        &mut iov,
        &mut ancillary,
        RecvFlags::CMSG_CLOEXEC | RecvFlags::TRUNC,
    )?;

    let mut fds = FdVec::new();
    let mut creds = None;
    for ancillary_message in ancillary.drain() {
        match ancillary_message {
            RecvAncillaryMessage::ScmRights(received) => fds.extend(received),
            RecvAncillaryMessage::ScmCredentials(c) => creds = Some(c),
            _ => (),
        }
    }
    debug_assert!(
        !msg.flags.contains(ReturnFlags::CTRUNC),
        "control buffer too small — descriptors were dropped by the kernel"
    );
    Ok(Received {
        bytes: msg.bytes,
        fds,
        creds,
    })
}

/// accepts one pending connection, non-blocking
pub(crate) fn accept_now(fd: BorrowedFd<'_>) -> std::io::Result<OwnedFd> {
    Ok(net::accept_with(
        fd,
        SocketFlags::NONBLOCK | SocketFlags::CLOEXEC,
    )?)
}

/// asks the kernel to stamp every incoming message with the sender's credentials
///
/// `SO_PASSCRED` is set by the *receiver*, and the kernel then attaches `SCM_CREDENTIALS`
/// to everything arriving on the socket — the sender does nothing and cannot opt out or
/// forge them. That is what makes the credentials worth having: they are the kernel's
/// word, not the peer's.
///
/// best-effort: a socket that refuses the option simply reports `None` credentials rather
/// than failing to receive
pub(crate) fn enable_credentials(fd: BorrowedFd<'_>) {
    let _ = net::sockopt::set_socket_passcred(fd, true);
}

/// is the far end of this connected socket gone?
///
/// one `ppoll` with a zero timeout — no reactor, no registration, no allocation, which
/// is what lets a [`Ref`] answer this without giving up its unregistered fast path.
///
/// `POLLHUP` and `POLLERR` are reported whether or not you ask for them, so the requested
/// event set is empty on purpose: this asks the kernel for the socket's *state*, not for
/// readiness, and pending unread data never makes it say yes. When the peer of an
/// `AF_UNIX` socket closes, the kernel sets our end's shutdown mask outright, which is
/// what shows up here as `POLLHUP`; unread data still queued when it went turns into
/// `ECONNRESET`, hence `POLLERR` too.
///
/// this is one-way — a socket that reports dead never comes back — so the answer does not
/// go stale on you. False *negatives* are still possible for a moment: the peer may be
/// closing as you ask.
///
/// [`Ref`]: crate::Ref
pub(crate) fn is_hung_up(fd: BorrowedFd<'_>) -> bool {
    let mut poll_fds = [PollFd::new(&fd, PollFlags::empty())];
    match rustix::event::poll(&mut poll_fds, Some(&Timespec::default())) {
        // a socket we cannot poll is not evidence of a dead peer, so say nothing
        Err(_) => false,
        Ok(_) => poll_fds[0]
            .revents()
            .intersects(PollFlags::HUP | PollFlags::ERR | PollFlags::NVAL),
    }
}

/// a socket registered with the tokio reactor
///
/// this is the piece that used to come from `tokio-seqpacket`. It is deliberately thin:
/// readiness plus a retry loop around the non-blocking calls above, which is exactly what
/// `tokio-seqpacket` did internally.
///
/// **must be constructed inside runtime context** — `AsyncFd::new` registers with the
/// reactor, and holding a `runtime::Handle` is not the same as being entered into it
pub(crate) struct Reactive {
    io: AsyncFd<OwnedFd>,
}

impl Reactive {
    pub(crate) fn new(fd: OwnedFd) -> std::io::Result<Self> {
        Ok(Self {
            io: AsyncFd::new(fd)?,
        })
    }

    /// waits for room, then sends
    ///
    /// the same `send_now` the inline fast path uses, with readiness in front of it
    pub(crate) async fn send(&self, message: &Message) -> std::io::Result<()> {
        loop {
            let mut guard = self.io.writable().await?;
            // `try_io` clears the readiness flag itself when the closure reports
            // WouldBlock, so there is no manual clear_ready to forget
            match guard.try_io(|inner| send_now(inner.get_ref().as_fd(), message)) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }

    /// receives the next message
    ///
    /// one syscall, no size probe: `buf` is always [`MAX_MESSAGE_SIZE`] and no accepted
    /// send can exceed that, so a message from a peer using this crate always fits.
    /// `MSG_TRUNC` is still requested so that a peer *not* using this crate — which the
    /// kernel would let send up to `SO_SNDBUF` — is detected rather than silently
    /// delivering a shortened payload
    pub(crate) async fn recv(
        &self,
        buf: &mut [u8],
        control_space: &mut [MaybeUninit<u8>],
    ) -> std::io::Result<Received> {
        loop {
            let mut guard = self.io.readable().await?;
            match guard.try_io(|inner| recv_now(inner.get_ref().as_fd(), buf, control_space)) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }

    /// parks until the far end of this socket is gone
    ///
    /// `epoll` reports `EPOLLHUP`/`EPOLLERR` regardless of the interest mask, so a
    /// hangup wakes this even though nothing is being read. Anything else that makes the
    /// socket readable — a peer that talks back on what we only ever send on — is cleared
    /// and waited on again, which is why this is a loop rather than a single `.await`.
    ///
    /// a reactor that reports an error is the runtime going away underneath us. There is
    /// no way to keep watching after that, and hanging forever inside someone's `select!`
    /// is the worse failure, so it reports death.
    pub(crate) async fn hangup(&self) {
        loop {
            let Ok(mut guard) = self.io.ready(Interest::READABLE | Interest::ERROR).await else {
                return;
            };
            if guard.ready().is_read_closed() || guard.ready().is_error() {
                return;
            }
            guard.clear_ready();
        }
    }

    pub(crate) async fn accept(&self) -> std::io::Result<OwnedFd> {
        loop {
            let mut guard = self.io.readable().await?;
            match guard.try_io(|inner| accept_now(inner.get_ref().as_fd())) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }
}
