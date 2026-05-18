//! PUSH/PULL throughput benchmarks.
//!
//! Two groups:
//!   - `throughput/rust_loopback`   — Rust PUSH0 → Rust PULL0, TCP loopback
//!   - `throughput/push_to_nngcat`  — Rust PUSH0 → nngcat PULL0 (C libnng sink)
//!
//! Each criterion iteration sends BATCH messages; criterion's `Throughput` setting
//! lets it report both iterations/sec and MB/s.
//!
//! Run:
//!   cargo bench --bench throughput
//!   cargo bench --bench throughput -- --sample-size 10

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use nng_core::{Message, socket::pipeline0};
use std::{process::Command, sync::Arc, thread, time::Duration};
use tokio::sync::Mutex;

const PAYLOAD_SIZES: &[usize] = &[256, 4096, 65536];
const BATCH: usize = 100;

fn make_msg(size: usize) -> Message {
    let mut m = Message::new();
    m.push_back(&vec![0u8; size]);
    m
}

fn nngcat_available() -> bool {
    Command::new("nngcat")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn bench_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("throughput");

    // ── Rust-only TCP loopback ─────────────────────────────────────────────────
    // Pull0 drains messages in a background task; Push0 is the measured side.
    let pull_task = rt.spawn(async {
        let mut pull = pipeline0::Pull0::listen("tcp://127.0.0.1:19101")
            .await
            .unwrap();
        loop {
            match pull.pull().await {
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });
    thread::sleep(Duration::from_millis(50));

    let push = Arc::new(Mutex::new(
        rt.block_on(pipeline0::Push0::dial("tcp://127.0.0.1:19101"))
            .unwrap(),
    ));
    for &size in PAYLOAD_SIZES {
        group.throughput(Throughput::Bytes((BATCH * size) as u64));
        group.bench_with_input(BenchmarkId::new("rust_loopback", size), &size, |b, &sz| {
            b.iter_custom(|iters| {
                let push_clone = Arc::clone(&push);
                rt.block_on(async move {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let mut p = push_clone.lock().await;
                        for _ in 0..BATCH {
                            p.push(make_msg(sz)).await.unwrap();
                        }
                    }
                    start.elapsed()
                })
            });
        });
    }
    pull_task.abort();

    // ── Rust PUSH → nngcat PULL (C libnng sink) ────────────────────────────────
    // nngcat silently discards received messages; measures our send throughput
    // against a real C libnng endpoint.
    if nngcat_available() {
        let mut nngcat = Command::new("nngcat")
            .args(["--pull0", "--listen", "tcp://127.0.0.1:19102", "--silent"])
            .spawn()
            .expect("failed to spawn nngcat");
        thread::sleep(Duration::from_millis(150));

        let push = Arc::new(Mutex::new(
            rt.block_on(pipeline0::Push0::dial("tcp://127.0.0.1:19102"))
                .unwrap(),
        ));
        for &size in PAYLOAD_SIZES {
            group.throughput(Throughput::Bytes((BATCH * size) as u64));
            group.bench_with_input(BenchmarkId::new("push_to_nngcat", size), &size, |b, &sz| {
                b.iter_custom(|iters| {
                    let push_clone = Arc::clone(&push);
                    rt.block_on(async move {
                        let start = std::time::Instant::now();
                        for _ in 0..iters {
                            let mut p = push_clone.lock().await;
                            for _ in 0..BATCH {
                                p.push(make_msg(sz)).await.unwrap();
                            }
                        }
                        start.elapsed()
                    })
                });
            });
        }
        let _ = nngcat.kill();
        let _ = nngcat.wait();
    }

    group.finish();
}

criterion_group!(benches, bench_throughput);
criterion_main!(benches);
