//! Pipeline (PUSH/PULL) — pull side.
//!
//! Dials the push node and processes each task as it arrives.
//!
//! Run in two terminals:
//!   cargo run --example push_node
//!   cargo run --example pull_worker

use nng_core::socket::pipeline0;

const ADDR: &str = "tcp://127.0.0.1:10001";

#[tokio::main]
async fn main() {
    let mut pull = pipeline0::Pull0::dial(ADDR).await.expect("dial failed");
    println!("Worker connected to {ADDR}, waiting for tasks...");

    loop {
        match pull.pull().await {
            Ok(msg) => println!("Worker processed: {}", String::from_utf8_lossy(msg.body())),
            Err(e) => {
                println!("Done: {e}");
                break;
            }
        }
    }
}
