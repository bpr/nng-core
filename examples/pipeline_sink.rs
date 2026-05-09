//! Pipeline — sink (result collector).
//!
//! Listens for N_WORKERS workers and collects N_TASKS results.  Prints wall
//! time at the end: parallel execution of N_TASKS × WORK_MS ms of work
//! across N_WORKERS workers should be ~N_TASKS/N_WORKERS × WORK_MS ms total,
//! versus N_TASKS × WORK_MS ms if sequential.
//!
//! Start order:
//!   cargo run --example pipeline_sink
//!   cargo run --example pipeline_ventilator
//!   cargo run --example pipeline_worker -- alpha
//!   cargo run --example pipeline_worker -- beta
//!   cargo run --example pipeline_worker -- gamma

use nng_core::socket::pipeline0::Pull0Fan;
use std::time::Instant;

const SINK_ADDR: &str = "tcp://127.0.0.1:11001";
const N_WORKERS: usize = 3;
const N_TASKS: usize = 15;

#[tokio::main]
async fn main() {
    println!("[sink] waiting for {N_WORKERS} workers on {SINK_ADDR}");
    let mut fan = Pull0Fan::listen_and_accept(SINK_ADDR, N_WORKERS)
        .await
        .expect("listen failed");
    println!("[sink] {N_WORKERS} workers connected — expecting {N_TASKS} results");

    let start = Instant::now();
    for received in 1..=N_TASKS {
        let msg = fan.pull_any().await.expect("pull failed");
        let text = String::from_utf8_lossy(msg.body()).into_owned();
        println!("[sink] result {received}/{N_TASKS}: {text}");
    }

    let elapsed = start.elapsed();
    println!(
        "[sink] all {N_TASKS} tasks complete in {:.1} s (parallel speedup visible if < {} s)",
        elapsed.as_secs_f64(),
        N_TASKS / N_WORKERS,
    );
}
