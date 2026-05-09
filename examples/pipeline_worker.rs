//! Pipeline — worker (task executor).
//!
//! Dials the sink first (so the sink is ready when the ventilator starts
//! pushing), then dials the ventilator to receive tasks.  Parses each task
//! as "{id} {work_ms}", sleeps that many milliseconds, and sends the result
//! to the sink.
//!
//! Start order:
//!   cargo run --example pipeline_sink
//!   cargo run --example pipeline_ventilator
//!   cargo run --example pipeline_worker -- alpha
//!   cargo run --example pipeline_worker -- beta
//!   cargo run --example pipeline_worker -- gamma

use nng_core::Message;
use nng_core::socket::pipeline0::{Pull0, Push0};
use std::{env, time::Duration};

const VENTILATOR_ADDR: &str = "tcp://127.0.0.1:11000";
const SINK_ADDR: &str = "tcp://127.0.0.1:11001";

#[tokio::main]
async fn main() {
    let name = env::args().nth(1).unwrap_or_else(|| "worker".to_string());

    // Dial sink first — ensures the sink connection is established before the
    // ventilator begins pushing (otherwise the first few tasks could be lost).
    let mut sink = Push0::dial(SINK_ADDR).await.expect("sink dial failed");
    println!("[{name}] connected to sink at {SINK_ADDR}");

    let mut source = Pull0::dial(VENTILATOR_ADDR)
        .await
        .expect("ventilator dial failed");
    println!("[{name}] connected to ventilator at {VENTILATOR_ADDR}");

    loop {
        let msg = match source.pull().await {
            Ok(m) => m,
            Err(e) => {
                println!("[{name}] ventilator closed: {e}");
                break;
            }
        };

        let text = String::from_utf8_lossy(msg.body()).into_owned();
        let mut parts = text.splitn(2, ' ');
        let task_id: u32 = parts.next().unwrap_or("0").parse().unwrap_or(0);
        let work_ms: u64 = parts.next().unwrap_or("0").parse().unwrap_or(0);

        println!("[{name}] task {task_id}: working for {work_ms} ms");
        tokio::time::sleep(Duration::from_millis(work_ms)).await;

        let result = format!("{task_id} done by {name}");
        let mut out = Message::new();
        out.push_back(result.as_bytes());
        sink.push(out).await.expect("sink send failed");
        println!("[{name}] task {task_id}: reported done");
    }
}
