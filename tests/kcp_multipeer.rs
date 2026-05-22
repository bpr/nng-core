//! Multi-peer integration tests for the KCP transport.
//!
//! These exercise the accept loop on sockets that hold multiple concurrent
//! peer connections — Bus0 (broadcast) and Pull0Fan (fan-in PUSH/PULL).
//!
//! The plumbing under test is the same `Arc<TokioMutex<KcpListener>>` +
//! drop-guard mechanism used by the single-peer REQ/REP tests, but exercised
//! through `accept_as_transport` called repeatedly and (for Bus0)
//! `accept_raw` inside a `biased select!` drain loop — the same pattern that
//! made `Pull0Fan` and `Bus0` cancellation-safe over TCP.

#![cfg(feature = "kcp")]

use nng_core::{
    Message,
    socket::{
        bus0::Bus0,
        pipeline0::{Pull0Fan, Push0},
    },
};
use std::time::Duration;
use tokio::net::UdpSocket;

/// Bind a UDP socket to port 0, read the assigned port, drop the socket,
/// and return a `kcp://` URL.  Same TOCTOU caveat as the single-peer tests
/// — fine for localhost.
async fn free_kcp_addr() -> String {
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = sock.local_addr().unwrap().port();
    drop(sock);
    format!("kcp://127.0.0.1:{port}")
}

/// Bus0 hub accepts N concurrent peers, then receives all N×M broadcasts.
///
/// Mirrors `tests/bus0.rs::bus0_recv_any_concurrent_senders`.  Verifies that
/// `accept_as_transport` works across multiple sequential accepts over a
/// single KCP listener.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bus0_kcp_concurrent_senders() {
    const N_PEERS: usize = 3;
    const N_MSGS: usize = 20;
    const TOTAL: usize = N_PEERS * N_MSGS;

    let hub_addr = free_kcp_addr().await;
    let hub_addr2 = hub_addr.clone();

    let hub_task =
        tokio::spawn(async move { Bus0::listen_and_accept(&hub_addr2, N_PEERS).await.unwrap() });

    // Let the listener bind before peers dial.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut peer_tasks = Vec::new();
    for peer_id in 0..N_PEERS {
        let addr = hub_addr.clone();
        peer_tasks.push(tokio::spawn(async move {
            let mut peer = Bus0::dial(&addr).await.unwrap();
            for seq in 0..N_MSGS {
                let payload = format!("peer{peer_id}-msg{seq}");
                let mut msg = Message::new();
                msg.push_back(payload.as_bytes());
                peer.broadcast(msg).await.unwrap();
            }
            // KCP has no FIN; sleep so the last sends and their ACKs drain
            // before the peer Bus0 (and its KCP actor) drops.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }));
    }

    let mut hub = hub_task.await.unwrap();

    let mut received = Vec::with_capacity(TOTAL);
    for _ in 0..TOTAL {
        let msg = hub.recv_any().await.expect("recv_any failed");
        let text = String::from_utf8(msg.body().to_vec()).expect("non-UTF8 body");
        assert!(
            text.starts_with("peer") && text.contains("-msg"),
            "garbled body: {text:?}"
        );
        received.push(text);
    }

    for t in peer_tasks {
        t.await.unwrap();
    }
    assert_eq!(received.len(), TOTAL);
}

/// Bus0 accepts a peer that dials after `listen_and_accept` returns.
///
/// Mirrors `tests/bus0.rs::bus0_dynamic_membership`.  Verifies that
/// `accept_pending()` — which drives `accept_raw` inside a biased select
/// — works correctly over KCP.  Cancellation safety of `KcpListener::accept`
/// matters here: if dropping the accept future loses state, the second peer
/// is never admitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bus0_kcp_dynamic_membership() {
    let hub_addr = free_kcp_addr().await;
    let hub_addr2 = hub_addr.clone();

    let hub_task =
        tokio::spawn(async move { Bus0::listen_and_accept(&hub_addr2, 1).await.unwrap() });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut peer1 = Bus0::dial(&hub_addr).await.unwrap();
    let mut hub = hub_task.await.unwrap();

    // Peer 2 dials AFTER listen_and_accept returned.
    let hub_addr3 = hub_addr.clone();
    let peer2_task = tokio::spawn(async move { Bus0::dial(&hub_addr3).await.unwrap() });

    // Give peer2's KCP handshake a moment to land in the listener's accept queue.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Drain the pending peer.
    hub.accept_pending().await;
    let mut peer2 = peer2_task.await.unwrap();

    // Hub now broadcasts to both peers.
    let mut hello = Message::new();
    hello.push_back(b"hi");
    hub.broadcast(hello).await.unwrap();

    let m1 = peer1.recv_any().await.unwrap();
    assert_eq!(m1.body(), b"hi");
    let m2 = peer2.recv_any().await.unwrap();
    assert_eq!(m2.body(), b"hi");

    // Drain ACKs before drops.
    tokio::time::sleep(Duration::from_millis(100)).await;
}

/// Pull0Fan receives from N concurrent Push0 senders over KCP.
///
/// Each PUSH peer connects on its own KCP session.  Pull0Fan spawns a
/// per-sender reader task that owns the transport — no cancellation hazard,
/// so this stresses only the multi-accept path of `listen_and_accept`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pull0fan_kcp_concurrent_pushers() {
    const N_SENDERS: usize = 3;
    const N_MSGS: usize = 20;
    const TOTAL: usize = N_SENDERS * N_MSGS;

    let pull_addr = free_kcp_addr().await;
    let pull_addr2 = pull_addr.clone();

    let pull_task = tokio::spawn(async move {
        Pull0Fan::listen_and_accept(&pull_addr2, N_SENDERS)
            .await
            .unwrap()
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut push_tasks = Vec::new();
    for sender_id in 0..N_SENDERS {
        let addr = pull_addr.clone();
        push_tasks.push(tokio::spawn(async move {
            let mut push = Push0::dial(&addr).await.unwrap();
            for seq in 0..N_MSGS {
                let payload = format!("s{sender_id}-m{seq}");
                let mut msg = Message::new();
                msg.push_back(payload.as_bytes());
                push.push(msg).await.unwrap();
            }
            // Drain ACKs.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }));
    }

    let mut pull = pull_task.await.unwrap();

    let mut received = Vec::with_capacity(TOTAL);
    for _ in 0..TOTAL {
        let msg = pull.pull_any().await.expect("pull_any");
        let text = String::from_utf8(msg.body().to_vec()).expect("non-UTF8 body");
        assert!(
            text.starts_with('s') && text.contains("-m"),
            "garbled body: {text:?}"
        );
        received.push(text);
    }

    for t in push_tasks {
        t.await.unwrap();
    }
    assert_eq!(received.len(), TOTAL);
}
