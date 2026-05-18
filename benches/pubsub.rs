//! PUB/SUB filter and throughput benchmarks.
//!
//! Two benchmark functions:
//!
//! `pubsub_filter` — pure `Sub0State::matches` micro-benchmarks (no I/O).
//!   Parameterised by subscription count (1, 10, 100) and match scenario:
//!   - `match_first`  — message matches the lexicographically smallest prefix
//!                      (BTreeSet range scan short-circuits on the first entry)
//!   - `match_last`   — message matches the largest prefix (scans all N entries)
//!   - `no_match`     — message matches nothing (scans all N entries, returns false)
//!   - `empty_prefix` — `b""` subscription (matches everything, always first hit)
//!
//! `pubsub_e2e` — end-to-end Pub0 → Sub0 throughput over TCP.
//!   Pub publishes BATCH messages cycling through all N topic prefixes; Sub must
//!   receive exactly BATCH messages.  A filtering bug that drops a matching
//!   message would cause `next()` to hang waiting for a message that never comes.
//!
//! Run:
//!   cargo bench --bench pubsub
//!   cargo bench --bench pubsub -- --sample-size 10

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use nng_core::{Message, protocols::pubsub::Sub0State, socket::pubsub0};
use std::{sync::Arc, thread, time::Duration};
use tokio::sync::Mutex;

const SUB_COUNTS: &[usize] = &[1, 10, 100];
const BATCH: usize = 500;
const MSG_SIZE: usize = 64;

fn topic(i: usize) -> String {
    format!("topic{i:04}:")
}

fn make_msg(body: &[u8]) -> Message {
    let mut m = Message::new();
    m.push_back(body);
    m
}

// ── Pure filter micro-benchmarks ─────────────────────────────────────────────

fn bench_pubsub_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("pubsub_filter");

    for &n in SUB_COUNTS {
        let mut state = Sub0State::new();
        for i in 0..n {
            state.subscribe(topic(i).as_bytes());
        }

        // match_first: body starts with the smallest prefix ("topic0000:")
        // BTreeSet range scan finds it on the first entry → O(1) in practice.
        let first_body = format!("{} payload", topic(0));
        let msg_first = make_msg(first_body.as_bytes());
        assert!(state.matches(&msg_first), "setup error: first should match");
        group.bench_with_input(BenchmarkId::new("match_first", n), &n, |b, _| {
            b.iter(|| state.matches(&msg_first));
        });

        // match_last: body starts with the largest prefix ("topic{n-1:04}:")
        // Range scan visits all N entries before finding the match.
        let last_body = format!("{} payload", topic(n - 1));
        let msg_last = make_msg(last_body.as_bytes());
        assert!(state.matches(&msg_last), "setup error: last should match");
        group.bench_with_input(BenchmarkId::new("match_last", n), &n, |b, _| {
            b.iter(|| state.matches(&msg_last));
        });

        // no_match: body sorts above all "topic*" entries (lowercase 'z' > 't')
        // so range(..=body) includes all N entries; starts_with fails for each.
        let no_match_body = b"zzz: unsubscribed topic";
        let msg_none = make_msg(no_match_body);
        assert!(
            !state.matches(&msg_none),
            "setup error: none should not match"
        );
        group.bench_with_input(BenchmarkId::new("no_match", n), &n, |b, _| {
            b.iter(|| state.matches(&msg_none));
        });
    }

    // empty_prefix: b"" is always the first entry in BTreeSet order and always
    // matches via `body.starts_with(b"")`.  Exercises the subscribe-all path.
    {
        let mut state = Sub0State::new();
        state.subscribe(b""); // subscribe-all
        for i in 0..100 {
            state.subscribe(topic(i).as_bytes()); // noise: 100 specific subs
        }
        let msg = make_msg(b"anything at all");
        assert!(
            state.matches(&msg),
            "setup error: empty prefix should match"
        );
        group.bench_function("empty_prefix_100_subs", |b| {
            b.iter(|| state.matches(&msg));
        });
    }

    group.finish();
}

// ── End-to-end PUB/SUB throughput ────────────────────────────────────────────

fn bench_pubsub_e2e(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("pubsub_e2e");

    for &n_subs in &[1usize, 10, 50] {
        let port = 19500 + n_subs as u16;
        let addr = format!("tcp://127.0.0.1:{port}");
        let topics: Vec<String> = (0..n_subs).map(topic).collect();

        // Pub binds first, then Sub dials as a spawned task.
        //
        // Sub0::dial cannot be driven by a plain block_on before
        // wait_for_subscribers: the dial's SP handshake waits for the server's
        // frame, which only arrives after accept.  Spawning Sub0 lets both sides
        // make progress concurrently while block_on drives the runtime.
        let mut pub0_inner = rt.block_on(pubsub0::Pub0::listen(&addr)).unwrap();

        let addr_sub = addr.clone();
        let topics_sub = topics.clone();
        let sub0_task = rt.spawn(async move {
            let mut s = pubsub0::Sub0::dial(&addr_sub).await.unwrap();
            for t in &topics_sub {
                s.subscribe_to(t.as_bytes());
            }
            s
        });
        thread::sleep(Duration::from_millis(50)); // let sub0 reach SP handshake

        rt.block_on(pub0_inner.wait_for_subscribers(1)).unwrap();
        let pub0 = Arc::new(Mutex::new(pub0_inner));
        let sub0 = Arc::new(Mutex::new(rt.block_on(sub0_task).unwrap()));

        // Warm up: publish and receive one message before measuring.
        rt.block_on(async {
            pub0.lock()
                .await
                .publish(make_msg(topics[0].as_bytes()))
                .await
                .unwrap();
            sub0.lock().await.next().await.unwrap();
        });

        group.throughput(Throughput::Bytes((BATCH * MSG_SIZE) as u64));
        group.bench_with_input(BenchmarkId::new("subs", n_subs), &n_subs, |b, _| {
            b.iter_custom(|iters| {
                let pub_clone = Arc::clone(&pub0);
                let sub_clone = Arc::clone(&sub0);
                let topics_clone = topics.clone();
                rt.block_on(async move {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        // Publish BATCH messages cycling through all N topics.
                        let mut p = pub_clone.lock().await;
                        for j in 0..BATCH {
                            let t = &topics_clone[j % n_subs];
                            let mut msg = Message::new();
                            msg.push_back(t.as_bytes());
                            // Pad to MSG_SIZE so throughput numbers are meaningful.
                            let pad = MSG_SIZE.saturating_sub(t.len());
                            if pad > 0 {
                                msg.push_back(&vec![0u8; pad]);
                            }
                            p.publish(msg).await.unwrap();
                        }
                        drop(p);

                        // Receive all BATCH messages; a filtering bug that drops a
                        // matching message would cause next() to hang here.
                        let mut s = sub_clone.lock().await;
                        for _ in 0..BATCH {
                            s.next().await.unwrap();
                        }
                    }
                    start.elapsed()
                })
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_pubsub_filter, bench_pubsub_e2e);
criterion_main!(benches);
