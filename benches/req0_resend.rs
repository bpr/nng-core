//! Req0 resend-path benchmarks and correctness stress test.
//!
//! Rep0 echoes every request payload back unchanged.  Req0 is configured with
//! progressively shorter resend deadlines so that retransmits fire during the
//! benchmark.  Each iteration asserts that the reply payload matches what was
//! sent — a stale-reply acceptance bug would return the *previous* iteration's
//! payload and trip the assertion.
//!
//! Groups (one Req0 connection, resend_time reconfigured between groups):
//!   - `req0_resend/no_resend`   — baseline, resend disabled
//!   - `req0_resend/resend_5ms`  — 5 ms deadline (well above TCP RTT, rarely fires)
//!   - `req0_resend/resend_200us`— 200 µs (above RTT, fires under jitter)
//!   - `req0_resend/resend_20us` — 20 µs (below RTT, fires on every call)
//!
//! Run:
//!   cargo bench --bench req0_resend
//!   cargo bench --bench req0_resend -- --sample-size 10

use criterion::{Criterion, criterion_group, criterion_main};
use nng_core::{Message, socket::reqrep0};
use std::{sync::Arc, thread, time::Duration};
use tokio::sync::Mutex;

fn make_msg(counter: u32) -> Message {
    let mut m = Message::new();
    m.push_back(&counter.to_be_bytes());
    m
}

fn bench_req0_resend(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("req0_resend");

    // Rep0 echoes every request back immediately.  With resend_time < RTT the
    // retransmit fires before the original reply arrives; after request() returns
    // the retransmit reply is still in-flight and must be discarded by the next
    // call's ID check.
    let rep_task = rt.spawn(async {
        let mut rep = reqrep0::Rep0::listen("tcp://127.0.0.1:19401")
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
        rt.block_on(reqrep0::Req0::dial("tcp://127.0.0.1:19401"))
            .unwrap(),
    ));

    // ── no_resend: baseline, resend disabled ──────────────────────────────────
    group.bench_function("no_resend", |b| {
        b.iter_custom(|iters| {
            let req_clone = Arc::clone(&req);
            rt.block_on(async move {
                let start = std::time::Instant::now();
                for i in 0..iters as u32 {
                    let mut r = req_clone.lock().await;
                    let reply = r.request(make_msg(i)).await.unwrap();
                    assert_eq!(reply.body(), &i.to_be_bytes(), "stale reply accepted");
                }
                start.elapsed()
            })
        });
    });

    // ── resend_5ms: deadline well above RTT, retransmit rarely fires ──────────
    rt.block_on(async {
        req.lock().await.set_resend_time(Duration::from_millis(5));
    });
    group.bench_function("resend_5ms", |b| {
        b.iter_custom(|iters| {
            let req_clone = Arc::clone(&req);
            rt.block_on(async move {
                let start = std::time::Instant::now();
                for i in 0..iters as u32 {
                    let mut r = req_clone.lock().await;
                    let reply = r.request(make_msg(i)).await.unwrap();
                    assert_eq!(reply.body(), &i.to_be_bytes(), "stale reply accepted");
                }
                start.elapsed()
            })
        });
    });

    // ── resend_200us: above RTT but close — fires under scheduler jitter ──────
    rt.block_on(async {
        req.lock().await.set_resend_time(Duration::from_micros(200));
    });
    group.bench_function("resend_200us", |b| {
        b.iter_custom(|iters| {
            let req_clone = Arc::clone(&req);
            rt.block_on(async move {
                let start = std::time::Instant::now();
                for i in 0..iters as u32 {
                    let mut r = req_clone.lock().await;
                    let reply = r.request(make_msg(i)).await.unwrap();
                    assert_eq!(reply.body(), &i.to_be_bytes(), "stale reply accepted");
                }
                start.elapsed()
            })
        });
    });

    // ── resend_20us: below TCP RTT (~37 µs) — retransmit fires on every call ──
    // After each request() returns there is always one stale reply still in
    // flight.  The very next call must read and discard it before receiving the
    // correct reply.  The assertion verifies no stale reply is accepted.
    rt.block_on(async {
        req.lock().await.set_resend_time(Duration::from_micros(20));
    });
    group.bench_function("resend_20us", |b| {
        b.iter_custom(|iters| {
            let req_clone = Arc::clone(&req);
            rt.block_on(async move {
                let start = std::time::Instant::now();
                for i in 0..iters as u32 {
                    let mut r = req_clone.lock().await;
                    let reply = r.request(make_msg(i)).await.unwrap();
                    assert_eq!(reply.body(), &i.to_be_bytes(), "stale reply accepted");
                }
                start.elapsed()
            })
        });
    });

    rep_task.abort();
    group.finish();
}

criterion_group!(benches, bench_req0_resend);
criterion_main!(benches);
