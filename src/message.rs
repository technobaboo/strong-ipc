//! what goes on the wire: a byte payload plus the capabilities riding along with it

use crate::{Ref, wire::EXPECTED_FDS};
use smallvec::SmallVec;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};

/// the descriptors that arrived on a message
///
/// inline up to `EXPECTED_FDS` (8), heap-allocated beyond it. sizing this by
/// [`crate::MAX_FDS`] instead would put a kilobyte of descriptors on the stack for every
/// message, almost always to hold one or none
pub type FdVec = SmallVec<[OwnedFd; EXPECTED_FDS]>;

/// a fd to send as ancillary data on a [`Message`]
///
/// `SCM_RIGHTS` only borrows the fd, the kernel dups it into the receiver's table and
/// leaves yours alone, so this just has to keep something alive that can hand out a
/// [`BorrowedFd`] at send time. never needs a local `dup` either way
pub(crate) enum SendFd {
    /// a fd owned outright, e.g. forwarded out of `Handler::handle`
    Owned(OwnedFd),
    /// a clone of a [`Ref`], shared rather than duped
    Ref(Ref),
}
impl AsFd for SendFd {
    fn as_fd(&self) -> BorrowedFd<'_> {
        match self {
            SendFd::Owned(fd) => fd.as_fd(),
            SendFd::Ref(r) => r.borrowed_fd(),
        }
    }
}

pub struct Message {
    data: Vec<u8>,
    fds: SmallVec<[SendFd; EXPECTED_FDS]>,
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
        self.fds.push(SendFd::Ref(r.clone()));
    }
    /// embeds a raw received fd, e.g. to forward one out of `Handler::handle`
    pub fn add_fd(&mut self, fd: OwnedFd) {
        self.fds.push(SendFd::Owned(fd));
    }

    pub(crate) fn data(&self) -> &[u8] {
        &self.data
    }
    pub(crate) fn fds(&self) -> &[SendFd] {
        &self.fds
    }
}
