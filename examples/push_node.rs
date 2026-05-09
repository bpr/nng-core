//! Pipeline (PUSH/PULL) — push side.
//!
//! Listens for one worker to connect, then pushes numbered tasks every 500 ms
//! until interrupted.
//!
//! Run in two terminals:
//!   cargo run --example push_node
//!   cargo run --example pull_worker

use nng_core::{Message, socket::pipeline0};
use std::time::Duration;

const ADDR: &str = "tcp://127.0.0.1:10001";

#[tokio::main]
async fn main() {
    let mut push = pipeline0::Push0::listen(ADDR).await.expect("listen failed");
    println!("Push node listening on {ADDR}");

    let mut counter = 1u32;
    loop {
        let payload = format!("Task #{counter}");
        let mut msg = Message::new();
        msg.push_back(payload.as_bytes());
        push.push(msg).await.expect("push failed");
        println!("Pushed: {payload}");
        counter += 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
