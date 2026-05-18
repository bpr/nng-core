//! Round-trip latency benchmarks for REQ/REP.
//!
//! Groups:
//!   - `latency/tcp_rust`     — both sides nng-core, TCP loopback
//!   - `latency/vs_nngcat`    — nng-core REQ vs nngcat REP0 (C libnng peer), TCP
//!   - `ipc_latency/ipc_rust` — both sides nng-core, Unix domain socket
//!   - `ipc_latency/ipc_vs_nngcat` — nng-core REQ vs nngcat REP0, IPC
//!
//! Run:
//!   cargo bench --bench latency
//!   cargo bench --bench latency -- --sample-size 10   # quick sanity check

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use nng_core::{Message, socket::reqrep0};
use std::{process::Command, sync::Arc, thread, time::Duration};
use tokio::sync::Mutex;

const PAYLOAD_SIZES: &[usize] = &[8, 256, 4096, 65536];

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

fn bench_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("latency");

    // ── Rust-only TCP loopback ─────────────────────────────────────────────────
    // Rep0 echoes each message back; measures full nng-core stack over TCP lo.
    let rep_task = rt.spawn(async {
        let mut rep = reqrep0::Rep0::listen("tcp://127.0.0.1:19001")
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
        rt.block_on(reqrep0::Req0::dial("tcp://127.0.0.1:19001"))
            .unwrap(),
    ));
    for &size in PAYLOAD_SIZES {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("tcp_rust", size), &size, |b, &sz| {
            b.iter_custom(|iters| {
                let req_clone = Arc::clone(&req);
                rt.block_on(async move {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let mut r = req_clone.lock().await;
                        r.request(make_msg(sz)).await.unwrap();
                    }
                    start.elapsed()
                })
            });
        });
    }
    rep_task.abort();

    // ── Rust REQ → nngcat REP (C libnng peer) ─────────────────────────────────
    // nngcat replies with a fixed "ack" payload regardless of request size, so
    // this measures time-to-send-N-bytes + ack-receive against a C libnng endpoint.
    if nngcat_available() {
        let mut nngcat = Command::new("nngcat")
            .args([
                "--rep0",
                "--listen",
                "tcp://127.0.0.1:19002",
                "--data",
                "ack",
                "--count",
                "999999",
            ])
            .spawn()
            .expect("failed to spawn nngcat");
        thread::sleep(Duration::from_millis(150));

        let req = Arc::new(Mutex::new(
            rt.block_on(reqrep0::Req0::dial("tcp://127.0.0.1:19002"))
                .unwrap(),
        ));
        for &size in PAYLOAD_SIZES {
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_with_input(BenchmarkId::new("vs_nngcat", size), &size, |b, &sz| {
                b.iter_custom(|iters| {
                    let req_clone = Arc::clone(&req);
                    rt.block_on(async move {
                        let start = std::time::Instant::now();
                        for _ in 0..iters {
                            let mut r = req_clone.lock().await;
                            r.request(make_msg(sz)).await.unwrap();
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

// ── IPC latency ───────────────────────────────────────────────────────────────
// Unix domain socket REQ/REP — exercises the 9-byte NNG 1.5.x IPC framing
// (type byte + 8-byte length) instead of TCP's 8-byte length header.
// Socket files are removed before binding and again on teardown.

const IPC_RUST_ADDR: &str = "ipc:///tmp/nng_bench_ipc_rust.sock";
const IPC_RUST_PATH: &str = "/tmp/nng_bench_ipc_rust.sock";
const IPC_NNGCAT_ADDR: &str = "ipc:///tmp/nng_bench_ipc_nngcat.sock";
const IPC_NNGCAT_PATH: &str = "/tmp/nng_bench_ipc_nngcat.sock";

fn bench_ipc_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("ipc_latency");

    // ── Rust-only IPC loopback ─────────────────────────────────────────────────
    let _ = std::fs::remove_file(IPC_RUST_PATH);
    let rep_task = rt.spawn(async {
        let mut rep = reqrep0::Rep0::listen(IPC_RUST_ADDR).await.unwrap();
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
        rt.block_on(reqrep0::Req0::dial(IPC_RUST_ADDR)).unwrap(),
    ));
    for &size in PAYLOAD_SIZES {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("ipc_rust", size), &size, |b, &sz| {
            b.iter_custom(|iters| {
                let req_clone = Arc::clone(&req);
                rt.block_on(async move {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let mut r = req_clone.lock().await;
                        r.request(make_msg(sz)).await.unwrap();
                    }
                    start.elapsed()
                })
            });
        });
    }
    rep_task.abort();
    let _ = std::fs::remove_file(IPC_RUST_PATH);

    // ── Rust REQ → nngcat REP over IPC ────────────────────────────────────────
    if nngcat_available() {
        let _ = std::fs::remove_file(IPC_NNGCAT_PATH);
        let mut nngcat = Command::new("nngcat")
            .args([
                "--rep0",
                "--listen",
                IPC_NNGCAT_ADDR,
                "--data",
                "ack",
                "--count",
                "999999",
            ])
            .spawn()
            .expect("failed to spawn nngcat");
        thread::sleep(Duration::from_millis(150));

        let req = Arc::new(Mutex::new(
            rt.block_on(reqrep0::Req0::dial(IPC_NNGCAT_ADDR)).unwrap(),
        ));
        for &size in PAYLOAD_SIZES {
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_with_input(BenchmarkId::new("ipc_vs_nngcat", size), &size, |b, &sz| {
                b.iter_custom(|iters| {
                    let req_clone = Arc::clone(&req);
                    rt.block_on(async move {
                        let start = std::time::Instant::now();
                        for _ in 0..iters {
                            let mut r = req_clone.lock().await;
                            r.request(make_msg(sz)).await.unwrap();
                        }
                        start.elapsed()
                    })
                });
            });
        }
        let _ = nngcat.kill();
        let _ = nngcat.wait();
        let _ = std::fs::remove_file(IPC_NNGCAT_PATH);
    }

    group.finish();
}

criterion_group!(benches, bench_latency, bench_ipc_latency);
criterion_main!(benches);
