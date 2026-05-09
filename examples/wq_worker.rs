//! Work-queue — worker.
//!
//! Each worker listens on its own port (12000 + index) and processes
//! requests sequentially.  The broker (wq_broker) connects to all workers
//! and routes client requests to whichever worker is idle.
//!
//! Run in separate terminals:
//!   cargo run --example wq_worker -- 0
//!   cargo run --example wq_worker -- 1
//!   cargo run --example wq_worker -- 2
//!   cargo run --example wq_broker
//!   cargo run --example wq_client

use nng_core::{Message, socket::reqrep0::Rep0};
use std::{env, time::Duration};

const BASE_PORT: u16 = 12000;

#[tokio::main]
async fn main() {
    let index: u16 = env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let addr = format!("tcp://127.0.0.1:{}", BASE_PORT + index);

    let mut rep = Rep0::listen(&addr).await.expect("listen failed");
    println!("[worker-{index}] listening on {addr}");

    loop {
        let (req, responder) = match rep.receive().await {
            Ok(r) => r,
            Err(e) => { println!("[worker-{index}] done: {e}"); break; }
        };

        let text = String::from_utf8_lossy(req.body()).into_owned();
        println!("[worker-{index}] processing: {text}");

        // Simulate variable-length work.
        let work_ms = (text.len() as u64 % 5 + 1) * 100;
        tokio::time::sleep(Duration::from_millis(work_ms)).await;

        let reply_text = format!("worker-{index}:{text}:done");
        let mut reply = Message::new();
        reply.push_back(reply_text.as_bytes());
        responder.reply(reply).await.expect("reply failed");
        println!("[worker-{index}] replied to {text}");
    }
}
