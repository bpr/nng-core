//! PAIR0 example — bidirectional point-to-point messaging.
//!
//! Two tasks exchange messages in both directions simultaneously.
//!
//! Run with:  cargo run -p nng-core --example pair

use std::fmt::Write;

use nng_core::{Message, socket::pair0};

const ADDR: &str = "tcp://127.0.0.1:54324";

#[tokio::main]
async fn main() {
    // Peer B (dialer): sends greetings, receives replies.
    let peer_b = tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let mut pair = pair0::Pair0::dial(ADDR).await.expect("dial failed");

        for i in 1u32..=3 {
            let mut msg = Message::new();
            write!(msg, "ping-{i}").unwrap();
            println!("[B] sending: ping-{i}");
            pair.send(msg).await.expect("send failed");

            let reply = pair.recv().await.expect("recv failed");
            println!("[B] received: {}", String::from_utf8_lossy(reply.body()));
        }
    });

    // Peer A (listener): echoes each message back with a "pong" prefix.
    let mut pair = pair0::Pair0::listen(ADDR).await.expect("listen failed");
    println!("[A] listening on {ADDR}");

    for _ in 0..3 {
        let msg = pair.recv().await.expect("recv failed");
        let text = String::from_utf8_lossy(msg.body()).into_owned();
        println!("[A] received: {text}");

        let mut reply = Message::new();
        write!(reply, "pong({text})").unwrap();
        pair.send(reply).await.expect("send failed");
    }

    peer_b.await.unwrap();
    println!("[A] done");
}
