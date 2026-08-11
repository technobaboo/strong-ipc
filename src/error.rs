//! what can go wrong on the way out
//!
//! both of these hand the [`Message`] back rather than dropping it, so a caller that hit
//! backpressure can retry with the same message instead of rebuilding it. The types are
//! owned by this crate rather than re-exported from tokio, so which failures a given
//! method can produce is visible in its signature: [`Ref::try_send`] can report either,
//! [`Ref::send`] waits out a full queue and so can only ever report [`Closed`].
//!
//! [`Ref::try_send`]: crate::Ref::try_send
//! [`Ref::send`]: crate::Ref::send

use crate::Message;
use std::fmt;

/// the peer is gone; this `Ref` will never deliver anything again
pub struct Closed(pub Message);

impl Closed {
    /// take back the message that was not delivered
    pub fn into_message(self) -> Message {
        self.0
    }
}

/// [`Ref::try_send`] could not take the message right now
///
/// [`Ref::try_send`]: crate::Ref::try_send
pub enum TrySendError {
    /// the socket buffer and the outbound queue are both full
    ///
    /// the peer is alive and simply behind. retry, or use [`Ref::send`] to wait for room.
    ///
    /// [`Ref::send`]: crate::Ref::send
    Full(Message),
    /// the peer is gone
    Closed(Message),
}

impl TrySendError {
    /// take back the message that was not delivered
    pub fn into_message(self) -> Message {
        match self {
            TrySendError::Full(m) | TrySendError::Closed(m) => m,
        }
    }
    /// was this merely backpressure, rather than a dead peer?
    pub fn is_full(&self) -> bool {
        matches!(self, TrySendError::Full(_))
    }
}

impl From<Closed> for TrySendError {
    fn from(closed: Closed) -> Self {
        TrySendError::Closed(closed.0)
    }
}

// `Message` holds descriptors and an arbitrary payload, neither of which is useful or
// safe to print, so these describe the failure and say nothing about its contents
impl fmt::Debug for Closed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Closed")
    }
}
impl fmt::Debug for TrySendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TrySendError::Full(_) => "Full",
            TrySendError::Closed(_) => "Closed",
        })
    }
}
impl fmt::Display for Closed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the peer is gone")
    }
}
impl fmt::Display for TrySendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TrySendError::Full(_) => "the outbound queue is full",
            TrySendError::Closed(_) => "the peer is gone",
        })
    }
}
impl std::error::Error for Closed {}
impl std::error::Error for TrySendError {}

impl From<Closed> for std::io::Error {
    fn from(_: Closed) -> Self {
        std::io::Error::from(std::io::ErrorKind::BrokenPipe)
    }
}
