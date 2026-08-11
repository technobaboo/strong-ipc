//! the receiving side: what turns bytes on a socket back into a handler call

use crate::{
    Ref,
    death::Death,
    message::FdVec,
    wire::{self, MAX_ANCILLARY_BUFFER_SIZE, MAX_MESSAGE_SIZE, Reactive},
};
use rustix::net::UCred;
use std::{
    mem::MaybeUninit,
    os::fd::AsFd,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::task::JoinSet;
use tokio_util::task::AbortOnDrop;

pub trait Handler: Send + Sync + 'static {
    fn handle(
        &self,
        data: &mut [u8],
        fds: FdVec,
        creds: Option<UCred>,
    ) -> impl Future<Output = ()> + Send + Sync;
}

/// a node on a socketpair, reached with the [`Ref`] handed back beside it
///
/// deliberately does **not** hold that `Ref`. A node keeping a capability to itself would
/// pin its own socket open forever, and the socket hanging up is precisely the signal
/// that nobody can reach this node any more — see [`Node::is_dead`]. Holding it would
/// also make the node a cycle waiting to happen, since the `Ref` is the thing you hand
/// out and the node is the thing that would then never be droppable by anyone else.
pub struct Node<H: Handler> {
    handler: Arc<H>,
    death: Arc<Death>,
    _task: AbortOnDrop,
}
impl<H: Handler> Node<H> {
    /// builds a node and the one capability that reaches it
    ///
    /// the `Ref` is yours to keep, clone, and hand out; the node has no copy of its own.
    /// Drop every `Ref` and the node's socket hangs up, its receive loop ends, and
    /// [`Node::is_dead`] goes true — which is the whole reason the pair is split.
    #[must_use = "the Ref is the only way to reach this node — a Node holds no capability \
                  to itself, so dropping the Ref here hangs the node's socket up \
                  immediately and it will never receive anything"]
    pub fn new(handler: H) -> std::io::Result<(Node<H>, Ref)> {
        Node::new_raw(Arc::new(handler))
    }
    /// [`Node::new`] for a handler you already share elsewhere
    #[must_use = "the Ref is the only way to reach this node — a Node holds no capability \
                  to itself, so dropping the Ref here hangs the node's socket up \
                  immediately and it will never receive anything"]
    pub fn new_raw(handler: Arc<H>) -> std::io::Result<(Node<H>, Ref)> {
        let (tx, rx) = wire::socketpair()?;
        wire::enable_credentials(rx.as_fd());
        // built out here, not in the task: `AsyncFd::new` needs runtime context, and
        // failing to register should be this call's error rather than a task that dies
        let socket = Reactive::new(rx)?;
        let death = Arc::new(Death::default());
        let task = tokio::spawn({
            let handler = handler.clone();
            let tombstone = death.tombstone();
            async move {
                let _tombstone = tombstone;
                recv_loop(handler, socket).await
            }
        });
        Ok((
            Self {
                handler,
                death,
                _task: AbortOnDrop::new(task.abort_handle()),
            },
            Ref::from_fd(tx),
        ))
    }

    /// has this node stopped receiving?
    ///
    /// this is the mirror of [`Ref::is_dead`]: that one asks whether the node behind a
    /// capability is gone, this one asks whether every capability to this node is. Since
    /// a `Node` holds no `Ref` to itself, dropping the last one really does hang its
    /// socket up, and the receive loop sees it.
    ///
    /// what it literally reports is the receive loop no longer running, which is the
    /// broader statement and the one you want — nothing sent here will be handled again.
    /// Besides the last `Ref` going, that covers an io error on the socket and a panic in
    /// a handler. It is a latch: never false again once true.
    ///
    /// a plain atomic load. The receive loop is already watching the socket, so unlike
    /// [`Ref::is_dead`] there is nothing to ask the kernel.
    pub fn is_dead(&self) -> bool {
        self.death.is_dead()
    }

    /// resolves when this node stops receiving; see [`Node::is_dead`] for what that means
    ///
    /// returns immediately if it already has. Costs nothing until awaited and needs no
    /// reactor registration, so unlike [`Ref::death_notification`] there is no reason not
    /// to use it. Awaiting this is the ordinary way to keep a node alive for exactly as
    /// long as somebody can still reach it:
    ///
    /// ```no_run
    /// # async fn f<H: strong_ipc::Handler>(handler: H) -> std::io::Result<()> {
    /// let (node, capability) = strong_ipc::Node::new(handler)?;
    /// hand_out(capability);
    /// node.death_notification().await; // returns once the last Ref is dropped
    /// # Ok(()) }
    /// # fn hand_out(_: strong_ipc::Ref) {}
    /// ```
    pub async fn death_notification(&self) {
        self.death.wait().await;
    }
    /// the handler this node is feeding
    ///
    /// deliberately a named method rather than a `Deref` impl: a `Node` is not a handler,
    /// and quietly putting every one of `H`'s methods onto it hides which of the two you
    /// are actually talking to
    pub fn handler(&self) -> &H {
        &self.handler
    }
}

/// the one spot where a message on the wire becomes a handler call
///
/// free-standing since node and boundnode share no type, they just both need something
/// to feed a handler. `Ok` means the peer hung up, not an error, though that's just a
/// zero-length read, same as an empty message, so sending one looks like a disconnect.
///
/// both buffers are fixed and neither can be overrun by a peer using this crate:
/// [`MAX_MESSAGE_SIZE`] is refused at send, and the control buffer is sized for
/// `MAX_FDS`. That is what makes the receive path one syscall with no size probe in
/// front of it, and what keeps the per-node footprint a constant.
///
/// a peer *not* using this crate is a different matter — the kernel would let it send up
/// to `SO_SNDBUF` — so `MSG_TRUNC` is still requested and an oversized message is
/// reported and dropped rather than delivered as a silently shortened payload
async fn recv_loop<H: Handler>(handler: Arc<H>, socket: Reactive) -> std::io::Result<()> {
    let mut buf = vec![0u8; MAX_MESSAGE_SIZE];
    let mut control = [MaybeUninit::uninit(); MAX_ANCILLARY_BUFFER_SIZE];

    loop {
        let received = socket.recv(&mut buf, &mut control).await?;
        if received.bytes == 0 {
            return Ok(());
        }
        if received.truncated(buf.len()) {
            // only reachable from a peer that isn't going through `Ref::try_send`
            eprintln!(
                "strong-ipc: dropped a {} B message from a peer ignoring the {} B limit",
                received.bytes, MAX_MESSAGE_SIZE
            );
            continue;
        }

        handler
            .handle(&mut buf[..received.bytes], received.fds, received.creds)
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
/// [`crate::Message::add_ref`] if they want a reply. they all share the one handler and
/// die with the boundnode
pub struct BoundNode<H: Handler> {
    handler: Arc<H>,
    /// we made this socket file, so it's ours to unlink again on drop
    path: PathBuf,
    _accept_task: AbortOnDrop,
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
        let listener = Reactive::new(wire::bind(path)?)?;
        let task = tokio::spawn(Self::task(listener, handler.clone()));
        Ok(Self {
            handler,
            path: path.to_owned(),
            _accept_task: AbortOnDrop::new(task.abort_handle()),
        })
    }
    /// the handler shared by every connection this node accepts
    ///
    /// see [`Node::handler`] for why this isn't a `Deref`
    pub fn handler(&self) -> &H {
        &self.handler
    }
    async fn task(listener: Reactive, handler: Arc<H>) -> std::io::Result<()> {
        // lives in the task and not the boundnode so it needs no locking, killing this
        // task drops the set, and every connection with it
        let mut connections = JoinSet::new();
        loop {
            let fd = listener.accept().await?;
            // set per connection: SO_PASSCRED is not inherited from the listener
            wire::enable_credentials(fd.as_fd());
            // clear out whoever hung up since, so this doesn't grow forever
            while connections.try_join_next().is_some() {}
            connections.spawn(recv_loop(handler.clone(), Reactive::new(fd)?));
        }
    }
}
impl<H: Handler> Drop for BoundNode<H> {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
