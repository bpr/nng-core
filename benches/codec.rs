//! Codec micro-benchmarks — pure computation, no I/O, no tokio.
//!
//! Measures the cost of SP handshake encoding/decoding and message frame
//! encoding/decoding at various payload sizes.
//!
//! These benchmarks are stable enough to be tracked as performance regression
//! guards in CI. They are also good candidates for iai-callgrind instruction
//! counting if `valgrind` is available (future follow-up).
//!
//! Run:
//!   cargo bench --bench codec

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use nng_core::{
    Message,
    codec::{ProtocolId, decode_frame, decode_handshake, encode_frame, encode_handshake},
};

fn make_msg(size: usize) -> Message {
    let mut m = Message::new();
    m.push_back(&vec![0u8; size]);
    m
}

fn bench_handshake(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec/handshake");

    group.bench_function("encode", |b| {
        b.iter(|| encode_handshake(black_box(ProtocolId::REQ0)));
    });

    let valid = encode_handshake(ProtocolId::REP0);
    group.bench_function("decode", |b| {
        b.iter(|| decode_handshake(black_box(&valid)));
    });

    group.finish();
}

fn bench_frame(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec/frame");

    for &size in &[64usize, 1024, 65536] {
        group.throughput(Throughput::Bytes(size as u64));

        let msg = make_msg(size);
        group.bench_with_input(BenchmarkId::new("encode", size), &size, |b, _| {
            b.iter(|| encode_frame(black_box(&msg)));
        });

        let framed = encode_frame(&msg);
        group.bench_with_input(BenchmarkId::new("decode", size), &size, |b, _| {
            b.iter(|| decode_frame(black_box(&framed)));
        });
    }

    group.finish();
}

criterion_group!(benches, bench_handshake, bench_frame);
criterion_main!(benches);
