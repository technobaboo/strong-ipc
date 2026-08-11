//! the receiving side: what turns bytes on a socket back into a handler call

use crate::{
    Ref,
    message::FdVec,
    wire::{self, INITIAL_RECV_BUFFER, MAX_ANCILLARY_BUFFER_SIZE, Reactive},
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

pub struct Node<H: Handler> {
    handler: Arc<H>,
    ref_: Ref,
    _task: AbortOnDrop,
}
impl<H: Handler> Node<H> {
    pub fn new(handler: H) -> std::io::Result<Node<H>> {
        Node::new_raw(Arc::new(handler))
    }
    pub fn new_raw(handler: Arc<H>) -> std::io::Result<Node<H>> {
        let (tx, rx) = wire::socketpair()?;
        wire::enable_credentials(rx.as_fd());
        let task = tokio::spawn(recv_loop(handler.clone(), Reactive::new(rx)?));
        Ok(Self {
            handler,
            ref_: Ref::from_fd(tx),
            _task: AbortOnDrop::new(task.abort_handle()),
        })
    }
    /// a `Ref` pointing back at this node, alive as long as the node is
    pub fn get_ref(&self) -> &Ref {
        &self.ref_
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
/// the payload buffer starts small and grows to whatever peers actually send. `MSG_TRUNC`
/// is always requested, so an oversized message reports its true length instead of being
/// silently shortened — the message that discovered the new size is still lost, but every
/// later one of that size fits. the control buffer needs no such treatment: it is sized
/// for `MAX_FDS` up front, because a truncated control message means the kernel dropped
/// descriptors, and a silently dropped descriptor is a capability that vanished
async fn recv_loop<H: Handler>(handler: Arc<H>, socket: Reactive) -> std::io::Result<()> {
    let mut buf = vec![0u8; INITIAL_RECV_BUFFER];
    let mut control = [MaybeUninit::uninit(); MAX_ANCILLARY_BUFFER_SIZE];
    let ceiling = wire::recv_buffer_limit(socket.get_ref()).max(INITIAL_RECV_BUFFER);

    loop {
        let received = socket.recv(&mut buf, &mut control).await?;
        if received.bytes == 0 {
            return Ok(());
        }
        if received.truncated(buf.len()) {
            // grow to fit and carry on; this message is already gone
            let want = received.bytes.min(ceiling);
            eprintln!(
                "strong-ipc: dropped a {} B message that did not fit a {} B buffer; \
                 growing to {want} B",
                received.bytes,
                buf.len()
            );
            buf.resize(want, 0);
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
