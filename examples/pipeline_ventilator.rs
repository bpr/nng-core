//! Pipeline — ventilator (task distributor).
//!
//! Waits for N_WORKERS workers to connect, then distributes N_TASKS tasks in
//! round-robin order.  Each task encodes a work duration so the sink can
//! verify actual parallelism: N_TASKS × WORK_MS sequential would take
//! ~1500 ms; three workers bring it to ~500 ms.
//!
//! Start order:
//!   cargo run --example pipeline_sink
//!   cargo run --example pipeline_ventilator
//!   cargo run --example pipeline_worker -- alpha   # (3 terminals)
//!   cargo run --example pipeline_worker -- beta
//!   cargo run --example pipeline_worker -- gamma

use nng_core::{Message, socket::pipeline0::Push0Fan};

const VENTILATOR_ADDR: &str = "tcp://127.0.0.1:11000";
const N_WORKERS: usize = 3;
const N_TASKS: u32 = 15;
const WORK_MS: u32 = 100;

#[tokio::main]
async fn main() {
    println!("[ventilator] waiting for {N_WORKERS} workers on {VENTILATOR_ADDR}");
    let mut fan = Push0Fan::listen_and_accept(VENTILATOR_ADDR, N_WORKERS)
        .await
        .expect("listen failed");
    println!("[ventilator] {N_WORKERS} workers connected — distributing {N_TASKS} tasks");

    for i in 1..=N_TASKS {
        let mut msg = Message::new();
        msg.push_back(format!("{i} {WORK_MS}").as_bytes());
        fan.push(msg).await.expect("push failed");
        println!("[ventilator] dispatched task {i}/{N_TASKS}");
    }
    println!("[ventilator] all tasks dispatched");
}
