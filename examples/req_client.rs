//! REQ/REP — client side.
//!
//! Dials the server, sends one request, prints the reply, and exits.
//!
//! Run in two terminals:
//!   cargo run --example req_server
//!   cargo run --example req_client

use nng_core::{Message, socket::reqrep0};

const ADDR: &str = "tcp://127.0.0.1:10000";

#[tokio::main]
async fn main() {
    let mut req = reqrep0::Req0::dial(ADDR).await.expect("dial failed");

    let mut msg = Message::new();
    msg.push_back(b"Ping!");
    println!("Client sent: Ping!");

    let reply = req.request(msg).await.expect("request failed");
    println!("Client received: {}", String::from_utf8_lossy(reply.body()));
}
