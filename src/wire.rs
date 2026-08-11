//! the only module that talks to the kernel
//!
//! everything above this file is plain Rust moving `Message`s around; this is where one
//! becomes bytes and descriptors on a socket. keeping that in one place is what lets the
//! rest of the crate be `deny(unsafe_code)`.

use crate::message::Message;
use std::{
    ffi::c_void,
    os::fd::{AsRawFd, BorrowedFd, RawFd},
};
use tokio_seqpacket::{ancillary::AncillaryMessageWriter, borrow_fd::BorrowFd};

/// the kernel's `SCM_MAX_FD` — the most descriptors one `SCM_RIGHTS` block can carry
pub const MAX_FDS: usize = 253;

/// how many descriptors a message is expected to carry in the common case
///
/// sizes the inline capacity of [`crate::FdVec`] and `Message`'s descriptor list, and the
/// stack ancillary buffer. Nothing is capped at this — it is purely the point past which
/// these spill to the heap
pub(crate) const EXPECTED_FDS: usize = 8;

pub const EXPECTED_ANCILLARY_BUFFER_SIZE: usize = {
    // one block for the fds...
    (unsafe { libc::CMSG_SPACE((EXPECTED_FDS * size_of::<RawFd>()) as u32) }
    // ...plus one for SCM_CREDENTIALS, since recv_loop pulls both out of the
    // same buffer
    + unsafe { libc::CMSG_SPACE(size_of::<libc::ucred>() as u32) }) as usize
};

/// one non-blocking `sendmsg`, straight to the kernel
///
/// no reactor, no task, no await — `MSG_DONTWAIT` means this either completes or fails,
/// it never parks the caller. `message` is only borrowed, so a `WouldBlock` leaves it
/// intact for the caller to queue instead
pub(crate) fn try_send_now(fd: BorrowedFd<'_>, message: &Message) -> std::io::Result<()> {
    // `AncillaryMessageWriter` realigns whatever buffer it is handed, and it hands back
    // only a length, not the realigned slice — so align it up front and check that no
    // bytes were skipped, otherwise the pointer below wouldn't match the length
    #[repr(align(8))]
    struct CmsgBuf([u8; EXPECTED_ANCILLARY_BUFFER_SIZE]);
    debug_assert_eq!(
        align_of::<CmsgBuf>() % align_of::<libc::cmsghdr>(),
        0,
        "cmsg buffer is under-aligned for this target"
    );
    let mut cmsg = CmsgBuf([0; EXPECTED_ANCILLARY_BUFFER_SIZE]);

    let control_len = if message.fds().is_empty() {
        0
    } else {
        let mut writer = AncillaryMessageWriter::new(&mut cmsg.0);
        debug_assert_eq!(
            writer.capacity(),
            EXPECTED_ANCILLARY_BUFFER_SIZE,
            "buffer was realigned, so its start no longer matches the write pointer"
        );
        writer.add_fds(message.fds().iter().map(|f| f.borrow_fd()))?;
        writer.len()
    };

    let mut iov = libc::iovec {
        iov_base: message.data().as_ptr() as *mut c_void,
        iov_len: message.data().len(),
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
            fd.as_raw_fd(),
            &header,
            libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
        )
    };
    if sent < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}
