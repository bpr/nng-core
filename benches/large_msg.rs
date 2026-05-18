//! Large-message throughput benchmarks.
//!
//! Exercises `FramedTransport` with payloads well above 64 KiB to verify:
//! - The 8-byte length field correctly encodes/decodes values > 0xFFFF.
//! - `recv` allocates a `Vec<u8>` of exactly `frame_len` bytes and reads all
//!   of them without silent truncation.
//! - No implicit size cap exists anywhere in the send or receive paths.
//!
//! Two groups, both over TCP loopback:
//!   `large_msg/push_pull/<N>MiB` — one-way PUSH/PULL throughput
//!   `large_msg/req_rep/<N>MiB`   — REQ/REP round-trip (payload sent twice)
//!
//! The reply-body length assertion in `req_rep` catches truncation bugs that
//! would be invisible to a pure timing benchmark.
//!
//! Run:
//!   cargo bench --bench large_msg
//!   cargo bench --bench large_msg -- --sample-size 10

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use nng_core::{
    Message,
    socket::{pipeline0, reqrep0},
};
use std::{sync::Arc, thread, time::Duration};
use tokio::sync::Mutex;

const MIB: usize = 1024 * 1024;
const PAYLOAD_SIZES: &[usize] = &[MIB, 4 * MIB, 16 * MIB];
const BATCH: usize = 5;

fn make_msg(size: usize) -> Message {
    let mut m = Message::new();
    // Non-zero fill — prevents the allocator from handing back zero pages,
    // which could mask an unread-but-allocated buffer.
    m.push_back(&vec![0xABu8; size]);
    m
}

fn mib_label(size: usize) -> String {
    format!("{}MiB", size / MIB)
}

// ── PUSH/PULL one-way throughput ──────────────────────────────────────────────

fn bench_push_pull(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("large_msg/push_pull");

    let pull_task = rt.spawn(async {
        let mut pull = pipeline0::Pull0::listen("tcp://127.0.0.1:19601")
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
        rt.block_on(pipeline0::Push0::dial("tcp://127.0.0.1:19601"))
            .unwrap(),
    ));

    for &size in PAYLOAD_SIZES {
        group.throughput(Throughput::Bytes((BATCH * size) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(mib_label(size)),
            &size,
            |b, &sz| {
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
            },
        );
    }

    pull_task.abort();
    group.finish();
}

// ── REQ/REP round-trip ────────────────────────────────────────────────────────

fn bench_req_rep(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("large_msg/req_rep");

    let rep_task = rt.spawn(async {
        let mut rep = reqrep0::Rep0::listen("tcp://127.0.0.1:19602")
            .await
            .unwrap();
        loop {
            match rep.receive().await {
                Ok((msg, responder)) => {
                    let _ = responder.reply(msg).await;
                }
                Err(_) => break,
            }
        }
    });
    thread::sleep(Duration::from_millis(50));

    let req = Arc::new(Mutex::new(
        rt.block_on(reqrep0::Req0::dial("tcp://127.0.0.1:19602"))
            .unwrap(),
    ));

    for &size in PAYLOAD_SIZES {
        // Payload travels twice: request + echo reply.
        group.throughput(Throughput::Bytes((2 * size) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(mib_label(size)),
            &size,
            |b, &sz| {
                b.iter_custom(|iters| {
                    let req_clone = Arc::clone(&req);
                    rt.block_on(async move {
                        let start = std::time::Instant::now();
                        for _ in 0..iters {
                            let mut r = req_clone.lock().await;
                            let reply = r.request(make_msg(sz)).await.unwrap();
                            // Truncation in recv would produce a shorter body.
                            assert_eq!(
                                reply.body().len(),
                                sz,
                                "reply body length mismatch — possible recv truncation"
                            );
                        }
                        start.elapsed()
                    })
                });
            },
        );
    }

    rep_task.abort();
    group.finish();
}

criterion_group!(benches, bench_push_pull, bench_req_rep);
criterion_main!(benches);
