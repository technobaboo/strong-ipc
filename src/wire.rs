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
use tokio::io::unix::AsyncFd;

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

/// how big a receive buffer starts out; it grows if a peer sends something larger
pub(crate) const INITIAL_RECV_BUFFER: usize = 8192;

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

/// the socket's receive buffer, which bounds the largest datagram it can ever deliver
pub(crate) fn recv_buffer_limit(fd: BorrowedFd<'_>) -> usize {
    net::sockopt::socket_recv_buffer_size(fd).unwrap_or(INITIAL_RECV_BUFFER)
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

    pub(crate) fn get_ref(&self) -> BorrowedFd<'_> {
        self.io.get_ref().as_fd()
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
