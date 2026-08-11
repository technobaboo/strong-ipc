//! capability-based IPC over `AF_UNIX` `SOCK_SEQPACKET`
//!
//! A [`Ref`] is a capability: holding one *is* the authority to send to the node behind
//! it. There is no namespace to look things up in and no permission to check — you can
//! only talk to what someone has handed you. Capabilities move between processes as
//! descriptors in `SCM_RIGHTS`, so passing one is the same operation as passing a file.
//!
//! ```text
//!   Message ──► Ref::send / try_send ──┬─► sendmsg inline        (the common case)
//!                                      └─► Outbox ──► drain task (only when backed up)
//!
//!   socket ──► recv_loop ──► Handler::handle(data, fds, creds)
//! ```
//!
//! [`Ref::send`] waits for room; [`Ref::try_send`] never blocks and reports
//! [`TrySendError::Full`] when the socket and the outbound queue are both backed up.
//! Both hand the [`Message`] back on failure, so a caller can retry with it rather than
//! rebuilding it. Prefer `send` — spinning on `Full` is a busy-wait.
//!
//! A payload over [`MAX_MESSAGE_SIZE`] is refused at the call site rather than truncated
//! at the far end. That one constant is also every receive buffer's size, so an accepted
//! send can never overrun a receiver: the failure is visible, early, and belongs to the
//! sender. Bulk data belongs behind a descriptor — attach a `memfd` with
//! [`Message::add_fd`] and pay one descriptor instead of a copy.
//!
//! `creds` is the kernel's word on who the peer is, not the peer's: `SO_PASSCRED` is set
//! by the receiving side, so credentials cannot be forged or omitted by the sender.
//!
//! [`Node`] is a node on a socketpair, which you reach with the [`Ref`] it hands you.
//! [`BoundNode`] listens on a filesystem path instead, because capabilities have a
//! bootstrap problem: if the only way to get a `Ref` is for someone to hand you one, two
//! unrelated processes can never meet. A path is the one name that isn't itself a
//! capability, so it's the door you knock on before you hold any.
//!
//! # Why sending and receiving don't look alike
//!
//! Sending has an inline fast path (`wire::send_now`) that goes straight to
//! `sendmsg` with no reactor involvement, and only falls back to a queue when the socket
//! is genuinely full. Receiving has no such thing, and that asymmetry is deliberate.
//!
//! The fast path does not exist to skip an `.await` — it exists to skip *reactor
//! registration entirely*. A `Ref` is constructed for every capability received, so
//! registering each one cost an `epoll_ctl` to add and another to drop, per message. A
//! receiving socket registers once and lives for the life of the node, so there is
//! nothing analogous to save, and a hand-rolled non-blocking read before the `.await`
//! would buy a saved future poll and nothing more.
//!
//! # Module map
//!
//! The crate is `forbid(unsafe_code)`: every syscall goes through `rustix`'s typed,
//! safe wrappers, so there is no raw `msghdr` construction anywhere.
//!
//! | module | what lives there |
//! |---|---|
//! | [`wire`] | the only code that talks to the kernel — every syscall in the crate |
//! | `message` | [`Message`], [`FdVec`] — payload plus attached capabilities |
//! | `capability` | [`Ref`] and its two-tier send |
//! | `outbox` | the backlog a `Ref` grows only once its socket backs up |
//! | `node` | [`Handler`], [`Node`], [`BoundNode`], and the receive loop |
//! | `death` | the latch behind [`Node::is_dead`]; `Ref`'s answer comes from the kernel |
//!
//! # Knowing when the other end is gone
//!
//! [`Ref::is_dead`] and [`Ref::death_notification`] answer for the node behind a
//! capability, so you can drop a `Ref` that leads nowhere without first losing a message
//! to [`TrySendError::Closed`]. `is_dead` is one `ppoll` and keeps the `Ref` out of the
//! reactor; awaiting the notification is what finally registers it, once per `Ref` that
//! ever asks.
//!
//! [`Node`] has the mirror pair, answering "can anyone still reach me?". That works
//! because [`Node::new`] hands the node's one capability *back to you* rather than
//! keeping it — a node holding a `Ref` to itself could never see its own socket hang up.
//! So dropping the last `Ref` ends the node's receive loop, and [`Node::is_dead`] says
//! so.

#![forbid(unsafe_code)]

mod capability;
mod death;
mod error;
mod message;
mod node;
mod outbox;
pub mod wire;

pub use capability::Ref;
pub use error::{SendError, TrySendError};
pub use message::{FdVec, Message};
pub use node::{BoundNode, Handler, Node};
pub use wire::{EXPECTED_ANCILLARY_BUFFER_SIZE, MAX_FDS, MAX_MESSAGE_SIZE};

/// the peer's credentials, as reported by `SCM_CREDENTIALS`
///
/// re-exported so implementing [`Handler`] doesn't require naming one of our
/// dependencies — the type appears in the trait, so it is part of this crate's API
/// whether or not we own it
pub use rustix::net::UCred;

#[cfg(test)]
mod tests {
    use super::*;

    /// `Message` is moved by value on every send, including back out of a failed
    /// `try_send`, so its inline descriptor storage has to stay small
    ///
    /// sizing the SmallVec by `MAX_FDS` instead of `EXPECTED_FDS` put roughly four
    /// kilobytes on the stack per message, which is what made clippy's `result_large_err`
    /// fire and had to be silenced crate-wide
    #[test]
    fn message_stays_small() {
        assert!(
            size_of::<Message>() <= 256,
            "Message is {} B — check the inline capacity of its descriptor list",
            size_of::<Message>()
        );
        assert!(
            size_of::<FdVec>() <= 128,
            "FdVec is {} B",
            size_of::<FdVec>()
        );
    }
}
