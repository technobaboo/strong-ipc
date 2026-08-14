//! the `echo` example but over a bound socket
//!
//! both halves are in the one process here, so it doesn't really show the point of
//! binding — see `bound_server`/`bound_client` for that

use std::time::Duration;
use strong_ipc::{FdVec, Handler, Message, Node, Ref, RefFsBinding};
use tokio_util::sync::CancellationToken;

pub struct EchoHandler;
impl Handler for EchoHandler {
	async fn handle(&self, data: &mut [u8], fds: FdVec, _creds: Option<strong_ipc::UCred>) {
		let return_fd = fds.into_iter().next().unwrap();
		let return_ref = Ref::from_owned_fd(return_fd);
		return_ref
			.try_send(Message::from_data(data.to_vec()))
			.unwrap();
	}
}
pub struct EchoReplyHandler(CancellationToken);
impl Handler for EchoReplyHandler {
	async fn handle(&self, data: &mut [u8], _fds: FdVec, _creds: Option<strong_ipc::UCred>) {
		println!("Echo response: {}", String::from_utf8_lossy(data));
		self.0.cancel();
	}
}

#[tokio::main(flavor = "current_thread")]
pub async fn main() {
	let path = std::env::temp_dir().join(format!("strong-ipc-echo-{}.sock", std::process::id()));
	let (_echo_node,node_ref) = Node::new(EchoHandler).unwrap();
	let _ref_binding = RefFsBinding::new(node_ref, &path).unwrap();

	let echo_ref = Ref::connect(&path).await.unwrap();

	let finished_token = CancellationToken::new();
	let (_reply_node, reply_node_ref) =
		Node::new(EchoReplyHandler(finished_token.clone())).unwrap();

	let mut message = Message::from_data("test".to_string().into_bytes());
	message.add_ref(&reply_node_ref);
	echo_ref.try_send(message).unwrap();

	tokio::time::timeout(Duration::from_secs(30), finished_token.cancelled())
		.await
		.unwrap();
}
