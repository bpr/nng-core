//! PAIR0 — bidirectional point-to-point messaging.
//!
//! Node A (listener) acts as an echo server. Node B (dialer) sends numbered
//! pings and prints the echoed replies.
//!
//! Run in two terminals:
//!   cargo run --example pair_node -- A
//!   cargo run --example pair_node -- B

use nng_core::{Message, socket::pair0};
use std::{env, time::Duration};

const ADDR: &str = "tcp://127.0.0.1:10004";

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let is_node_a = args.get(1).map(|s| s == "A").unwrap_or(false);

    let mut pair = if is_node_a {
        println!("[A] listening on {ADDR}");
        pair0::Pair0::listen(ADDR).await.expect("listen failed")
    } else {
        println!("[B] dialing {ADDR}");
        pair0::Pair0::dial(ADDR).await.expect("dial failed")
    };

    if is_node_a {
        // Echo server: receive a ping, send back pong(ping).
        loop {
            match pair.recv().await {
                Ok(msg) => {
                    let text = String::from_utf8_lossy(msg.body()).into_owned();
                    println!("[A] received: {text}");
                    let reply_text = format!("pong({text})");
                    let mut reply = Message::new();
                    reply.push_back(reply_text.as_bytes());
                    pair.send(reply).await.expect("send failed");
                }
                Err(e) => { println!("[A] done: {e}"); break; }
            }
        }
    } else {
        // Ping sender: send a numbered ping, wait for the echo, repeat.
        let mut counter = 1u32;
        loop {
            let payload = format!("ping-{counter}");
            let mut msg = Message::new();
            msg.push_back(payload.as_bytes());
            println!("[B] sending: {payload}");
            pair.send(msg).await.expect("send failed");

            let reply = pair.recv().await.expect("recv failed");
            println!("[B] received: {}", String::from_utf8_lossy(reply.body()));

            counter += 1;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
