//! Integration tests for `BufferPool` + `FramedTransport::recv_pooled`.
//!
//! Two concerns are exercised:
//!
//! 1. **Correctness**: `recv_pooled` returns the same bytes as `recv`; the
//!    pool's bounds (`max_bufs`, `max_buf_bytes`) are respected; cancellation
//!    safety carries over from the underlying `RecvBuf`.
//! 2. **Buffer reuse**: after `pool.recycle(msg)`, the next `recv_pooled`
//!    fills the previously-allocated buffer in place — verifiable by pointer
//!    equality on the received body.

use nng_core::{
    BufferPool, Message,
    codec::ProtocolId,
    transport::{
        FrameFormat, FramedTransport,
        loopback::{TokioDuplex, inproc_pair},
    },
};

fn body_of(bytes: &[u8]) -> Message {
    let mut m = Message::new();
    m.push_back(bytes);
    m
}

// ── 1. Basic round-trip ──────────────────────────────────────────────────────

#[tokio::test]
async fn recv_pooled_basic_roundtrip() {
    let (mut a, mut b) = inproc_pair(ProtocolId::REQ0, ProtocolId::REP0)
        .await
        .unwrap();
    let mut pool = BufferPool::new();

    a.send(&body_of(b"ping")).await.unwrap();
    let got = b.recv_pooled(&mut pool).await.unwrap();
    assert_eq!(got.body(), b"ping");

    b.send(&body_of(b"pong")).await.unwrap();
    let got = a.recv_pooled(&mut pool).await.unwrap();
    assert_eq!(got.body(), b"pong");
}

// ── 2. Buffer reuse: pointer equality across recycle ─────────────────────────

#[tokio::test]
async fn recv_pooled_recycles_buffer_capacity() {
    let (mut a, mut b) = inproc_pair(ProtocolId::REQ0, ProtocolId::REP0)
        .await
        .unwrap();
    let mut pool = BufferPool::new();

    let payload = vec![0xABu8; 1024];

    // First iteration — pool is empty, recv allocates fresh.
    a.send(&body_of(&payload)).await.unwrap();
    let msg1 = b.recv_pooled(&mut pool).await.unwrap();
    assert_eq!(msg1.body(), payload.as_slice());
    let ptr1 = msg1.body().as_ptr();
    pool.recycle(msg1);
    assert_eq!(pool.len(), 1, "buffer should be recycled into pool");

    // Subsequent iterations — pool serves the same buffer.
    for _ in 0..100 {
        a.send(&body_of(&payload)).await.unwrap();
        let msg = b.recv_pooled(&mut pool).await.unwrap();
        assert_eq!(msg.body(), payload.as_slice());
        assert_eq!(
            msg.body().as_ptr(),
            ptr1,
            "body pointer should match the originally-pooled allocation"
        );
        pool.recycle(msg);
    }
}

// ── 3. max_bufs caps the pool size ───────────────────────────────────────────

#[tokio::test]
async fn recv_pooled_respects_max_bufs() {
    let mut pool = BufferPool::with_capacity(2, 64 * 1024);
    for _ in 0..5 {
        pool.recycle(Message::from_parts(Vec::new(), vec![0u8; 64]));
    }
    assert_eq!(pool.len(), 2, "pool must not exceed max_bufs");
}

// ── 4. max_buf_bytes drops oversized buffers ────────────────────────────────

#[tokio::test]
async fn recv_pooled_respects_max_buf_bytes() {
    let mut pool = BufferPool::with_capacity(16, 256);

    // Within limit — stored.
    pool.recycle(Message::from_parts(Vec::new(), vec![0u8; 200]));
    assert_eq!(pool.len(), 1);

    // Exceeds limit — dropped.
    pool.recycle(Message::from_parts(Vec::new(), vec![0u8; 1024]));
    assert_eq!(pool.len(), 1, "oversized buffer must not be stored");
}

// ── 5. Existing `recv` API still works after the from_parts refactor ─────────

#[tokio::test]
async fn recv_unchanged_after_refactor() {
    let (mut a, mut b) = inproc_pair(ProtocolId::REQ0, ProtocolId::REP0)
        .await
        .unwrap();

    for i in 0u8..10 {
        a.send(&body_of(&[i, i + 1, i + 2])).await.unwrap();
        let got = b.recv().await.unwrap();
        assert_eq!(got.body(), &[i, i + 1, i + 2]);
    }
}

// ── 6. Cancellation safety carries through recv_pooled ───────────────────────

/// Same approach as `tests/bus0.rs::framed_recv_cancellation_safe_small_buffer`
/// but exercising `recv_pooled`. A 9-byte duplex is too small for an 8-byte
/// length header + 11-byte body in one go, so partial reads are unavoidable.
/// `recv_pooled` must store its progress in `RecvBuf` so that cancellation
/// followed by a fresh `recv_pooled` produces exactly one intact message.
#[tokio::test]
async fn recv_pooled_cancellation_safe() {
    let (a, b) = tokio::io::duplex(9);
    let (mut receiver, mut sender) = tokio::try_join!(
        FramedTransport::connect(TokioDuplex(a), ProtocolId::PULL0, FrameFormat::Tcp),
        FramedTransport::connect(TokioDuplex(b), ProtocolId::PUSH0, FrameFormat::Tcp),
    )
    .unwrap();

    let send_task = tokio::spawn(async move {
        sender.send(&body_of(b"hello world")).await.unwrap();
    });

    let mut pool = BufferPool::new();
    let received = loop {
        let poll = tokio::select! {
            biased;
            r = receiver.recv_pooled(&mut pool) => Some(r),
            _ = std::future::ready(()) => None,
        };
        match poll {
            Some(Ok(msg)) => break msg,
            Some(Err(e)) => panic!("recv_pooled error: {e}"),
            None => tokio::task::yield_now().await,
        }
    };

    send_task.await.unwrap();
    assert_eq!(received.body(), b"hello world");
}
