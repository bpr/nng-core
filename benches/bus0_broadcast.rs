//! Bus0 broadcast throughput benchmarks.
//!
//! One hub node broadcasts to N spoke nodes over TCP loopback.
//! Throughput is reported as total bytes delivered (hub × N peers) per iteration
//! so that criterion's MB/s output shows how fan-out cost scales.
//!
//! Group: `bus0/broadcast/<N>`  where N ∈ {2, 4, 8}
//!
//! Run:
//!   cargo bench --bench bus0_broadcast
//!   cargo bench --bench bus0_broadcast -- --sample-size 10

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use nng_core::{Message, socket::bus0};
use std::{sync::Arc, thread, time::Duration};
use tokio::sync::Mutex;

const MSG_SIZE: usize = 256;
const BATCH: usize = 200;
const N_PEERS: &[usize] = &[2, 4, 8];

fn make_msg() -> Message {
    let mut m = Message::new();
    m.push_back(&vec![0u8; MSG_SIZE]);
    m
}

fn bench_bus0_broadcast(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("bus0");

    for &n_peers in N_PEERS {
        // Each peer count gets its own port so Criterion can run groups in any order.
        let port = 19300 + n_peers as u16;
        let addr = format!("tcp://127.0.0.1:{port}");

        // Spawn hub listener — blocks inside until n_peers have completed the SP
        // handshake, so it must run as a task while spokes dial concurrently.
        let addr_hub = addr.clone();
        let hub_task = rt.spawn(async move {
            bus0::Bus0::listen_and_accept(&addr_hub, n_peers)
                .await
                .unwrap()
        });
        thread::sleep(Duration::from_millis(50)); // let the port bind

        // Spawn N spokes: each dials the hub then drains indefinitely.
        // Keeping spokes alive prevents kernel send-buffer backpressure from
        // stalling the hub's broadcast mid-benchmark.
        let spoke_tasks: Vec<_> = (0..n_peers)
            .map(|_| {
                let addr_spoke = addr.clone();
                rt.spawn(async move {
                    let mut spoke = bus0::Bus0::dial(&addr_spoke).await.unwrap();
                    loop {
                        match spoke.recv_any().await {
                            Ok(_) => {}
                            Err(_) => break,
                        }
                    }
                })
            })
            .collect();

        let hub = Arc::new(Mutex::new(rt.block_on(hub_task).unwrap()));

        // Throughput = bytes delivered to all peers per iteration.
        // A broadcast of MSG_SIZE bytes reaches n_peers recipients.
        group.throughput(Throughput::Bytes((n_peers * BATCH * MSG_SIZE) as u64));
        group.bench_with_input(BenchmarkId::new("broadcast", n_peers), &n_peers, |b, _| {
            b.iter_custom(|iters| {
                let hub_clone = Arc::clone(&hub);
                rt.block_on(async move {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let mut h = hub_clone.lock().await;
                        for _ in 0..BATCH {
                            h.broadcast(make_msg()).await.unwrap();
                        }
                    }
                    start.elapsed()
                })
            });
        });

        for task in spoke_tasks {
            task.abort();
        }
    }

    group.finish();
}

criterion_group!(benches, bench_bus0_broadcast);
criterion_main!(benches);
