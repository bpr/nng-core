//! Pipeline (PUSH/PULL) example — work distribution.
//!
//! A pusher distributes 8 tasks to a puller, which processes each one.
//!
//! Run with:  cargo run -p nng-core --example pipeline

use std::fmt::Write;

use nng_core::{Message, socket::pipeline0};

const ADDR: &str = "tcp://127.0.0.1:54323";

#[tokio::main]
async fn main() {
    // Worker (Pull0): receive tasks and process them.
    let worker = tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let mut pull = pipeline0::Pull0::dial(ADDR).await.expect("dial failed");
        for _ in 0..8 {
            let msg = pull.pull().await.expect("pull failed");
            let task = String::from_utf8_lossy(msg.body());
            println!("[worker] processing: {task}");
        }
    });

    // Distributor (Push0): listen then push 8 tasks.
    let mut push = pipeline0::Push0::listen(ADDR).await.expect("listen failed");
    println!("[dist] listening on {ADDR}");

    for i in 1u32..=8 {
        let mut msg = Message::new();
        write!(msg, "task-{i}").unwrap();
        push.push(msg).await.expect("push failed");
        println!("[dist] pushed task-{i}");
    }

    worker.await.unwrap();
    println!("[dist] all tasks delivered");
}
