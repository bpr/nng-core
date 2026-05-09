//! Integration tests for Bus0: cancellation-safe recv and dynamic membership.
//!
//! Two bugs are covered:
//!
//! 1. `recv_any` used `select! { biased; recv() => .., ready(()) => None }`,
//!    which cancels the `recv` future mid-read.  Because `FramedTransport::recv`
//!    does multiple `read_exact` calls, dropping it after a partial read
//!    consumes bytes from the TCP stream without recording them, corrupting
//!    every subsequent frame boundary.
//!
//! 2. `Bus0::listen_and_accept(addr, n)` dropped its listener after accepting
//!    `n` peers.  Peers that dialled afterwards got "connection refused" and
//!    could never join.  `accept_pending()` + keeping the listener in the struct
//!    fixes this.

use nng_core::{Message, socket::bus0::Bus0};

// ── helpers ──────────────────────────────────────────────────────────────────

fn free_port() -> u16 {
    // Bind port 0 to get an OS-assigned free port, then release it.
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn addr(port: u16) -> String {
    format!("tcp://127.0.0.1:{port}")
}

// ── Bug 1: cancellation-unsafe recv ──────────────────────────────────────────

/// Reproduces the cancellation bug at the `FramedTransport` level using a
/// tiny in-memory duplex stream.
///
/// A 4-byte duplex buffer is smaller than the 8-byte TCP frame-length header,
/// so the very first `read_exact` inside `recv` is forced to return Pending
/// mid-header.  The old biased-select approach drops the future at that point,
/// losing the already-read bytes.  The next `recv` call treats bytes 4-7 of
/// the length header as bytes 0-3, producing a garbage frame length and
/// panicking on allocation.
///
/// With the fix, `recv` stores its partial-read progress in the transport
/// struct and resumes correctly on the next poll.
#[tokio::test]
async fn framed_recv_cancellation_safe_small_buffer() {
    use nng_core::codec::ProtocolId;
    use nng_core::transport::{FrameFormat, FramedTransport, loopback::TokioDuplex};

    // 9-byte buffer: just large enough for the 8-byte SP handshake (so the
    // handshake doesn't deadlock) but smaller than an 8-byte frame-length
    // header + 11-byte body = 19 bytes total, which forces a partial body read.
    let (a, b) = tokio::io::duplex(9);

    let (mut receiver, mut sender) = tokio::try_join!(
        FramedTransport::connect(TokioDuplex(a), ProtocolId::PULL0, FrameFormat::Tcp),
        FramedTransport::connect(TokioDuplex(b), ProtocolId::PUSH0, FrameFormat::Tcp),
    )
    .unwrap();

    // Send a known payload in a background task; the tiny buffer forces the
    // sender to drip data into the pipe a few bytes at a time.
    let send_task = tokio::spawn(async move {
        let mut msg = Message::new();
        msg.push_back(b"hello world");
        sender.send(&msg).await.unwrap();
    });

    // Simulate the old recv_any polling loop: poll recv() once and abandon it
    // if it is not immediately ready, then retry on the next yield.
    // With the old code this drops a partially-read future and corrupts the
    // stream.  With the fix the next poll resumes from where it left off.
    let received = loop {
        let poll = tokio::select! {
            biased;
            r = receiver.recv() => Some(r),
            _ = std::future::ready(()) => None,
        };
        match poll {
            Some(Ok(msg)) => break msg,
            Some(Err(e)) => panic!("recv error: {e}"),
            None => tokio::task::yield_now().await,
        }
    };

    send_task.await.unwrap();
    assert_eq!(received.body(), b"hello world");
}

/// End-to-end version of the cancellation bug through the `Bus0` API.
///
/// Three peers each send 20 messages concurrently to a hub.  The hub collects
/// all 60 messages via `recv_any`.  With the old implementation concurrent
/// sends race to deliver partial frame-length bytes; the biased-select loop
/// drops and re-creates `recv()` futures mid-read, producing garbled lengths
/// that cause a panic or a wrong-data assertion failure.  With the fix every
/// message is received intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bus0_recv_any_concurrent_senders() {
    const N_PEERS: usize = 3;
    const N_MSGS: usize = 20;
    const TOTAL: usize = N_PEERS * N_MSGS;

    let port = free_port();
    let hub_addr = addr(port);

    // Start hub listener in a task.
    let hub_addr2 = hub_addr.clone();
    let hub_task =
        tokio::spawn(async move { Bus0::listen_and_accept(&hub_addr2, N_PEERS).await.unwrap() });

    // Give the listener a moment to bind, then connect peers.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let mut peer_tasks = Vec::new();
    for peer_id in 0..N_PEERS {
        let hub_addr3 = hub_addr.clone();
        peer_tasks.push(tokio::spawn(async move {
            let mut peer = Bus0::dial(&hub_addr3).await.unwrap();
            for seq in 0..N_MSGS {
                let payload = format!("peer{peer_id}-msg{seq}");
                let mut msg = Message::new();
                msg.push_back(payload.as_bytes());
                peer.broadcast(msg).await.unwrap();
            }
        }));
    }

    let mut hub = hub_task.await.unwrap();

    let mut received = Vec::with_capacity(TOTAL);
    for _ in 0..TOTAL {
        let msg = hub.recv_any().await.expect("recv_any failed");
        let text = String::from_utf8(msg.body().to_vec()).expect("non-UTF8 body");
        assert!(
            text.starts_with("peer") && text.contains("-msg"),
            "garbled message body: {text:?}"
        );
        received.push(text);
    }

    for t in peer_tasks {
        t.await.unwrap();
    }
    assert_eq!(received.len(), TOTAL);
}

// ── Bug 2: bounded membership ─────────────────────────────────────────────────

/// After `listen_and_accept(addr, 1)` returns, the listener is no longer
/// kept: a second peer dialling the same address gets "connection refused".
///
/// With the fix, `Bus0` keeps its listener and `accept_pending()` drains any
/// queued connections between uses.
#[tokio::test]
async fn bus0_dynamic_membership() {
    let port = free_port();
    let hub_addr = addr(port);

    // Hub accepts only the first peer at construction time.
    let hub_addr2 = hub_addr.clone();
    let hub_task =
        tokio::spawn(async move { Bus0::listen_and_accept(&hub_addr2, 1).await.unwrap() });

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // Peer 1 joins immediately.
    let mut peer1 = Bus0::dial(&hub_addr).await.unwrap();

    let mut hub = hub_task.await.unwrap();

    // Peer 2 joins *after* listen_and_accept has returned.
    // With the fix the OS still has the listener socket open and accepts the
    // connection into the kernel backlog; accept_pending() then drains it.
    let hub_addr3 = hub_addr.clone();
    let peer2_task = tokio::spawn(async move { Bus0::dial(&hub_addr3).await.unwrap() });

    // Give peer 2 time to connect at the TCP level.
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    // Accept pending connections so peer 2 joins the bus.
    hub.accept_pending().await;

    let mut peer2 = peer2_task.await.unwrap();

    // Broadcast from hub — both peers must receive it.
    let mut bcast = Message::new();
    bcast.push_back(b"hello everyone");
    hub.broadcast(bcast).await.unwrap();

    let m1 = tokio::time::timeout(std::time::Duration::from_secs(2), peer1.recv_any())
        .await
        .expect("peer1 timeout")
        .expect("peer1 recv error");
    let m2 = tokio::time::timeout(std::time::Duration::from_secs(2), peer2.recv_any())
        .await
        .expect("peer2 timeout")
        .expect("peer2 recv error");

    assert_eq!(m1.body(), b"hello everyone");
    assert_eq!(m2.body(), b"hello everyone");
}
