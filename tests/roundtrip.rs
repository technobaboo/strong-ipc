//! payload integrity, reply-by-capability, and forwarding a real descriptor

use std::io::Read;
use std::os::fd::OwnedFd;
use std::time::Duration;
use strong_ipc::{FdVec, Handler, Message, Node, Ref, TrySendError};

/// hands every received message straight back to the test
struct Collector {
    tx: tokio::sync::mpsc::UnboundedSender<Delivered>,
}

impl Handler for Collector {
    async fn handle(&self, data: &mut [u8], fds: FdVec, _creds: Option<strong_ipc::UCred>) {
        let _ = self.tx.send((data.to_vec(), fds.into_iter().collect()));
    }
}

/// one delivered message: its payload and whatever descriptors rode along
type Delivered = (Vec<u8>, Vec<OwnedFd>);
type Inbox = tokio::sync::mpsc::UnboundedReceiver<Delivered>;

fn collector() -> (Node<Collector>, Inbox) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (Node::new(Collector { tx }).unwrap(), rx)
}

async fn next(rx: &mut Inbox) -> Delivered {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for a message")
        .expect("channel closed")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn payloads_arrive_intact() {
    let (node, mut rx) = collector();

    // 8192 is where `recv_loop`'s buffer starts, so these all fit without it having to
    // grow — the growth path itself is covered by the payload-ceiling phase in bench.rs
    for len in [1usize, 2, 64, 512, 4096, 8191, 8192] {
        let payload: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        node.get_ref()
            .try_send(Message::from_data(payload.clone()))
            .unwrap();
        let (got, fds) = next(&mut rx).await;
        assert_eq!(got.len(), len, "wrong length for a {len} B payload");
        assert_eq!(got, payload, "corrupted {len} B payload");
        assert!(fds.is_empty(), "unexpected descriptors on a {len} B payload");
    }
}

/// a zero-length message is indistinguishable from a hangup, and shuts the node down
///
/// `recv_loop` treats a zero-byte read as the peer going away, which is exactly what a
/// zero-length send produces. it then returns, dropping the socket it owns — so the
/// sender's next send fails outright rather than vanishing. pinning both halves here so
/// the quirk is a decision and not a surprise.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_length_message_looks_like_a_hangup() {
    let (node, mut rx) = collector();

    node.get_ref()
        .try_send(Message::from_data(Vec::new()))
        .unwrap();
    // nothing is delivered to the handler
    assert!(
        tokio::time::timeout(Duration::from_millis(300), rx.recv())
            .await
            .is_err(),
        "a zero-length message was delivered to the handler"
    );
    // and the receiving end is now gone, so the sender finds out rather than silently
    // dropping everything after it
    assert!(
        matches!(
            node.get_ref()
                .try_send(Message::from_data(b"after".to_vec())),
            Err(TrySendError::Closed(_))
        ),
        "the recv loop survived a zero-length message, or the sender was not told"
    );
}

/// echoes whatever it receives back on the capability it was handed
struct Echo;
impl Handler for Echo {
    async fn handle(&self, data: &mut [u8], fds: FdVec, _creds: Option<strong_ipc::UCred>) {
        let Some(fd) = fds.into_iter().next() else {
            return;
        };
        let reply = Ref::from_owned_fd(fd);
        let _ = reply.try_send(Message::from_data(data.to_vec()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reply_travels_back_over_an_attached_capability() {
    let echo = Node::new(Echo).unwrap();
    let (mine, mut rx) = collector();

    let mut message = Message::from_data(b"ping".to_vec());
    message.add_ref(mine.get_ref());
    echo.get_ref().try_send(message).unwrap();

    let (got, _) = next(&mut rx).await;
    assert_eq!(got, b"ping");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forwarded_descriptor_is_usable_on_the_far_side() {
    let path = std::env::temp_dir().join(format!("strong-ipc-fdtest-{}", std::process::id()));
    std::fs::write(&path, b"capability payload").unwrap();
    let file = std::fs::File::open(&path).unwrap();

    let (node, mut rx) = collector();
    let mut message = Message::from_data(b"here is a file".to_vec());
    message.add_fd(OwnedFd::from(file));
    node.get_ref().try_send(message).unwrap();

    let (got, fds) = next(&mut rx).await;
    assert_eq!(got, b"here is a file");
    assert_eq!(fds.len(), 1, "the descriptor did not arrive");

    // the fd the kernel installed in us must name the same open file
    let mut received = std::fs::File::from(fds.into_iter().next().unwrap());
    let mut contents = String::new();
    received.read_to_string(&mut contents).unwrap();
    assert_eq!(contents, "capability payload");

    let _ = std::fs::remove_file(&path);
}

/// the kernel stamps every message with the sender's real credentials
///
/// `SO_PASSCRED` is set by the receiver, so credentials arrive whether or not the sender
/// asks — and cannot be forged by it. Before this was wired up the `creds` argument was
/// always `None`, which made it a parameter every `Handler` had to write and nobody could
/// use.
struct Creds {
    tx: tokio::sync::mpsc::UnboundedSender<Option<strong_ipc::UCred>>,
}
impl Handler for Creds {
    async fn handle(&self, _d: &mut [u8], _f: FdVec, creds: Option<strong_ipc::UCred>) {
        let _ = self.tx.send(creds);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messages_arrive_stamped_with_peer_credentials() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let node = Node::new(Creds { tx }).unwrap();

    node.get_ref()
        .send(Message::from_data(b"who am i".to_vec()))
        .await
        .expect("peer closed");

    let creds = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out")
        .expect("channel closed")
        .expect("no credentials on the message — is SO_PASSCRED set on the receiver?");

    assert_eq!(
        creds.pid.as_raw_nonzero().get() as u32,
        std::process::id(),
        "credentials named a different process"
    );
    assert_eq!(creds.uid.as_raw(), rustix::process::getuid().as_raw());
    assert_eq!(creds.gid.as_raw(), rustix::process::getgid().as_raw());
}

/// an oversized payload is refused at the call site, not truncated at the far end
///
/// this is the contract that lets every receive buffer be a fixed `MAX_MESSAGE_SIZE`
/// with no size probe in front of it: nothing that is accepted can overrun a receiver.
/// The failure is early, visible, and belongs to the sender, which still holds its
/// message and can shrink it or move the bulk behind a descriptor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_messages_are_refused_at_the_sender() {
    let (node, mut rx) = collector();

    // the largest accepted payload still goes through
    node.get_ref()
        .send(Message::from_data(vec![1u8; strong_ipc::MAX_MESSAGE_SIZE]))
        .await
        .expect("a payload of exactly MAX_MESSAGE_SIZE should be accepted");
    let (got, _) = next(&mut rx).await;
    assert_eq!(got.len(), strong_ipc::MAX_MESSAGE_SIZE);

    // one byte more is refused, and comes back intact
    for over in [1usize, 8, 4096, 57344] {
        let len = strong_ipc::MAX_MESSAGE_SIZE + over;
        let err = node
            .get_ref()
            .try_send(Message::from_data(vec![2u8; len]))
            .expect_err("a payload over MAX_MESSAGE_SIZE should be refused");
        match err {
            TrySendError::TooLarge(returned) => {
                assert_eq!(
                    returned.data().len(),
                    len,
                    "the refused message was not handed back intact"
                );
            }
            other => panic!("expected TooLarge for a {len} B payload, got {other:?}"),
        }
    }

    // and `send` reports it too, rather than waiting forever for room that would never help
    assert!(
        matches!(
            node.get_ref()
                .send(Message::from_data(vec![3u8; strong_ipc::MAX_MESSAGE_SIZE + 1]))
                .await,
            Err(strong_ipc::SendError::TooLarge(_))
        ),
        "send should refuse an oversized payload rather than block on it"
    );

    // nothing oversized reached the handler
    assert!(
        tokio::time::timeout(Duration::from_millis(300), rx.recv())
            .await
            .is_err(),
        "an oversized message was delivered anyway"
    );
}
