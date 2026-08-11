//! echo server on a bound socket. run this, then `cargo run --example bound_client`
//!
//! the two sides are separate processes sharing nothing but the path — nobody inherits
//! an fd. that's the whole point of binding: the client's only way in is the path, and
//! every capability after that rides along as an fd

use std::path::PathBuf;
use strong_ipc::{BoundNode, FdVec, Handler, Message, Ref};

pub struct EchoHandler;
impl Handler for EchoHandler {
    async fn handle(&self, data: &mut [u8], fds: FdVec, _creds: Option<strong_ipc::UCred>) {
        let text = String::from_utf8_lossy(data).into_owned();
        // an accepted conn has no ref back out, so the client hands us one to reply on
        // in-band, as an fd on the message
        let Some(return_fd) = fds.into_iter().next() else {
            println!("received {text:?}, but no reply ref was attached");
            return;
        };
        println!("received {text:?}, echoing it back");
        Ref::from_owned_fd(return_fd)
            .try_send(Message::from_data(text.into_bytes()))
            .unwrap();
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
    let node = match BoundNode::bind(&path, EchoHandler) {
        Ok(node) => node,
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            eprintln!("{} is already in use.", path.display());
            eprintln!("Another server is running, or a previous one was killed before it could");
            eprintln!("clean up — in which case remove the stale socket file and try again.");
            std::process::exit(1);
        }
        Err(e) => panic!("failed to bind {}: {e}", path.display()),
    };

    println!("listening on {}", path.display());
    println!("run `cargo run --example bound_client` in another terminal; ctrl-c to stop");
    tokio::signal::ctrl_c().await.unwrap();

    // dropping kills the accept loop and every live conn, and unlinks the socket file.
    // just letting it fall out of scope does the same, it's explicit to print after
    drop(node);
    println!("\nstopped, {} removed", path.display());
}
