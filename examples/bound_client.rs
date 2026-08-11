//! talks to the `bound_server` example, start that one first

use std::path::PathBuf;
use std::time::Duration;
use strong_ipc::{FdVec, Handler, Message, Node, Ref};
use tokio_util::sync::CancellationToken;

pub struct ReplyHandler(CancellationToken);
impl Handler for ReplyHandler {
    async fn handle(&self, data: &mut [u8], _fds: FdVec, _creds: Option<strong_ipc::UCred>) {
        println!("reply: {}", String::from_utf8_lossy(data));
        self.0.cancel();
    }
}

/// both examples default to the same path, pass another as the first arg
fn socket_path() -> PathBuf {
    std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("strong-ipc-echo.sock"))
}

#[tokio::main(flavor = "current_thread")]
pub async fn main() {
    let path = socket_path();
    let server = match Ref::connect(&path).await {
        Ok(server) => server,
        Err(e) => {
            eprintln!("could not connect to {}: {e}", path.display());
            eprintln!("is `cargo run --example bound_server` running?");
            std::process::exit(1);
        }
    };

    // connecting only gets a ref pointing at the server, nothing comes back over it. so
    // we make a node of our own and hand the server the cap to reach it on the message
    let done = CancellationToken::new();
    let reply_node = Node::new(ReplyHandler(done.clone())).unwrap();

    let text = format!("hello from pid {}", std::process::id());
    println!("sending {text:?}");
    let mut message = Message::from_data(text.into_bytes());
    message.add_ref(reply_node.get_ref());
    server.send_message(message).unwrap();

    tokio::time::timeout(Duration::from_secs(10), done.cancelled())
        .await
        .expect("timed out waiting for the server to reply");
}
