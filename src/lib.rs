//! capability-based IPC over `AF_UNIX` `SOCK_SEQPACKET`
//!
//! A [`Ref`] is a capability: holding one *is* the authority to send to the node behind
//! it. There is no namespace to look things up in and no permission to check — you can
//! only talk to what someone has handed you. Capabilities move between processes as
//! descriptors in `SCM_RIGHTS`, so passing one is the same operation as passing a file.
//!
//! ```text
//!   Message ──► Ref::send_message ──┬─► sendmsg inline          (the common case)
//!                                   └─► Outbox ──► drain task   (only when backed up)
//!
//!   socket ──► recv_loop ──► Handler::handle(data, fds, creds)
//! ```
//!
//! [`Node`] is a node on a socketpair, which you reach with the [`Ref`] it hands you.
//! [`BoundNode`] listens on a filesystem path instead, because capabilities have a
//! bootstrap problem: if the only way to get a `Ref` is for someone to hand you one, two
//! unrelated processes can never meet. A path is the one name that isn't itself a
//! capability, so it's the door you knock on before you hold any.
//!
//! # Why sending and receiving don't look alike
//!
//! Sending has an inline fast path (`wire::try_send_now`) that goes straight to
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
//! | module | what lives there |
//! |---|---|
//! | [`wire`] | the only code that talks to the kernel, and the only `unsafe` |
//! | `message` | [`Message`], [`FdVec`] — payload plus attached capabilities |
//! | `capability` | [`Ref`] and its two-tier send |
//! | `outbox` | the backlog a `Ref` grows only once its socket backs up |
//! | `node` | [`Handler`], [`Node`], [`BoundNode`], and the receive loop |

#![deny(unsafe_code)]

mod capability;
mod message;
mod node;
mod outbox;
// the one module allowed to touch raw syscalls; everything else is safe Rust
#[allow(unsafe_code)]
pub mod wire;

pub use capability::Ref;
pub use message::{FdVec, Message};
pub use node::{BoundNode, Handler, Node};
pub use wire::{EXPECTED_ANCILLARY_BUFFER_SIZE, MAX_FDS};

#[cfg(test)]
mod tests {
    use super::*;

    /// `Message` is moved by value on every send, including back out of a failed
    /// `send_message`, so its inline descriptor storage has to stay small
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
