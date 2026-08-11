//! `BoundNode` — the path-bound door you knock on before you hold any capability

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use strong_ipc::{BoundNode, FdVec, Handler, Message, Node, Ref};

/// a socket path unique to this process and test
fn socket_path(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("strong-ipc-test-{}-{name}.sock", std::process::id()));
    let _ = std::fs::remove_file(&p);
    p
}

/// replies on the capability it was handed, tagging the reply with a per-node id
struct Echo {
    tag: &'static str,
    served: AtomicU32,
}

impl Handler for Echo {
    async fn handle(&self, data: &mut [u8], fds: FdVec, _creds: Option<tokio_seqpacket::UCred>) {
        self.served.fetch_add(1, Ordering::Relaxed);
        let Some(fd) = fds.into_iter().next() else {
            return;
        };
        let reply = Ref::from_owned_fd(fd).unwrap();
        let mut out = self.tag.as_bytes().to_vec();
        out.extend_from_slice(data);
        let _ = reply.send_message(Message::from_data(out));
    }
}

struct Collector {
    tx: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
}
impl Handler for Collector {
    async fn handle(&self, data: &mut [u8], _fds: FdVec, _creds: Option<tokio_seqpacket::UCred>) {
        let _ = self.tx.send(data.to_vec());
    }
}

fn collector() -> (Node<Collector>, tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (Node::new(Collector { tx }).unwrap(), rx)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_and_echo_over_a_bound_socket() {
    let path = socket_path("echo");
    let _server = BoundNode::bind(&path, Echo { tag: "S:", served: AtomicU32::new(0) }).unwrap();

    let server_ref = Ref::connect(&path).await.unwrap();
    let (mine, mut rx) = collector();

    let mut message = Message::from_data(b"knock".to_vec());
    message.add_ref(mine.get_ref());
    server_ref.send_message(message).unwrap();

    let got = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out")
        .expect("channel closed");
    assert_eq!(got, b"S:knock");
}

/// a path that already exists is not ours to take over
///
/// replacing it could yank the path out from under something still alive, so a stale
/// socket left by a crash has to be cleaned up deliberately rather than clobbered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binding_an_occupied_path_fails_rather_than_clobbering() {
    let path = socket_path("occupied");
    let _first = BoundNode::bind(&path, Echo { tag: "A:", served: AtomicU32::new(0) }).unwrap();

    let second = BoundNode::bind(&path, Echo { tag: "B:", served: AtomicU32::new(0) });
    match second {
        Err(e) => assert_eq!(
            e.kind(),
            std::io::ErrorKind::AddrInUse,
            "expected AddrInUse, got {e:?}"
        ),
        Ok(_) => panic!("the second bind clobbered the first"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_the_node_unlinks_the_socket_file() {
    let path = socket_path("unlink");
    {
        let _server = BoundNode::bind(&path, Echo { tag: "U:", served: AtomicU32::new(0) }).unwrap();
        assert!(path.exists(), "bind did not create the socket file");
    }
    assert!(
        !path.exists(),
        "the socket file outlived the BoundNode that created it"
    );
}

/// every accepted connection shares the one handler
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_clients_share_one_handler() {
    let path = socket_path("shared");
    let _server = BoundNode::bind(&path, Echo { tag: "S:", served: AtomicU32::new(0) }).unwrap();

    let a = Ref::connect(&path).await.unwrap();
    let b = Ref::connect(&path).await.unwrap();
    let (mine, mut rx) = collector();

    for (r, body) in [(&a, &b"one"[..]), (&b, &b"two"[..])] {
        let mut m = Message::from_data(body.to_vec());
        m.add_ref(mine.get_ref());
        r.send_message(m).unwrap();
    }

    let mut got = Vec::new();
    for _ in 0..2 {
        got.push(
            tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("timed out")
                .expect("channel closed"),
        );
    }
    got.sort();
    assert_eq!(got, vec![b"S:one".to_vec(), b"S:two".to_vec()]);
}
