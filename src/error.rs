//! what can go wrong on the way out
//!
//! all of these hand the [`Message`] back rather than dropping it, so a caller can retry
//! or salvage its contents instead of rebuilding it. The types are owned by this crate
//! rather than re-exported from tokio, so which failures a given method can produce is
//! visible in its signature: [`Ref::try_send`] can report any of them, [`Ref::send`]
//! waits out a full queue and so cannot report [`TrySendError::Full`].
//!
//! [`Ref::try_send`]: crate::Ref::try_send
//! [`Ref::send`]: crate::Ref::send

use crate::{Message, wire::MAX_MESSAGE_SIZE};
use std::fmt;

/// [`Ref::send`] could not deliver the message
///
/// [`Ref::send`]: crate::Ref::send
pub enum SendError {
	/// the payload exceeds [`MAX_MESSAGE_SIZE`]
	///
	/// refused before anything leaves the process, so nothing was partially sent. Large
	/// payloads belong behind a descriptor — attach a `memfd` with
	/// [`Message::add_fd`] instead of inlining the bytes.
	TooLarge(Message),
	/// the peer is gone
	Closed(Message),
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
	/// the payload exceeds [`MAX_MESSAGE_SIZE`]; see [`SendError::TooLarge`]
	TooLarge(Message),
	/// the peer is gone
	Closed(Message),
}

impl SendError {
	/// take back the message that was not delivered
	pub fn into_message(self) -> Message {
		match self {
			SendError::TooLarge(m) | SendError::Closed(m) => m,
		}
	}
}

impl TrySendError {
	/// take back the message that was not delivered
	pub fn into_message(self) -> Message {
		match self {
			TrySendError::Full(m) | TrySendError::TooLarge(m) | TrySendError::Closed(m) => m,
		}
	}
	/// was this merely backpressure, rather than a dead peer or a rejected message?
	pub fn is_full(&self) -> bool {
		matches!(self, TrySendError::Full(_))
	}
}

impl From<SendError> for TrySendError {
	fn from(e: SendError) -> Self {
		match e {
			SendError::TooLarge(m) => TrySendError::TooLarge(m),
			SendError::Closed(m) => TrySendError::Closed(m),
		}
	}
}

// `Message` holds descriptors and an arbitrary payload, neither of which is useful or
// safe to print, so these describe the failure and say nothing about its contents
impl fmt::Debug for SendError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(match self {
			SendError::TooLarge(_) => "TooLarge",
			SendError::Closed(_) => "Closed",
		})
	}
}
impl fmt::Debug for TrySendError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(match self {
			TrySendError::Full(_) => "Full",
			TrySendError::TooLarge(_) => "TooLarge",
			TrySendError::Closed(_) => "Closed",
		})
	}
}
impl fmt::Display for SendError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			SendError::TooLarge(m) => write!(
				f,
				"payload is {} B, over the {MAX_MESSAGE_SIZE} B limit",
				m.data().len()
			),
			SendError::Closed(_) => f.write_str("the peer is gone"),
		}
	}
}
impl fmt::Display for TrySendError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			TrySendError::Full(_) => f.write_str("the outbound queue is full"),
			TrySendError::TooLarge(m) => write!(
				f,
				"payload is {} B, over the {MAX_MESSAGE_SIZE} B limit",
				m.data().len()
			),
			TrySendError::Closed(_) => f.write_str("the peer is gone"),
		}
	}
}
impl std::error::Error for SendError {}
impl std::error::Error for TrySendError {}

impl From<SendError> for std::io::Error {
	fn from(e: SendError) -> Self {
		match e {
			SendError::TooLarge(_) => std::io::Error::from(std::io::ErrorKind::InvalidInput),
			SendError::Closed(_) => std::io::Error::from(std::io::ErrorKind::BrokenPipe),
		}
	}
}
