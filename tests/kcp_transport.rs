#![cfg(feature = "kcp")]

use kcp_tokio::KcpConfig;
use nng_core::{
    Message,
    socket::reqrep0::{Rep0, Req0},
};
use std::time::Duration;
use tokio::net::UdpSocket;

/// Bind a UDP socket to port 0, read the assigned port, drop the socket,
/// and return a `kcp://` URL.  Mirrors `free_quic_addr()` in
/// tests/quic_transport.rs; same TOCTOU caveat (fine for localhost tests).
async fn free_kcp_addr() -> String {
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = sock.local_addr().unwrap().port();
    drop(sock);
    format!("kcp://127.0.0.1:{port}")
}

#[tokio::test]
async fn kcp_reqrep_loopback() {
    let addr = free_kcp_addr().await;
    let server_addr = addr.clone();

    let server = tokio::spawn(async move {
        let mut rep = Rep0::listen(&server_addr).await.expect("rep listen");
        let (msg, responder) = rep.receive().await.expect("server receive");
        assert_eq!(msg.body(), b"hello kcp");
        let mut reply = Message::new();
        reply.push_back(b"hello kcp");
        responder.reply(reply).await.expect("server reply");
        // Brief pause so the reply packet (and its KCP ACK) drain before
        // we drop `rep` — KCP has no FIN/CONNECTION_CLOSE handshake, so
        // dropping the stream kills the actor task and any unacked sends.
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    // Give the server task time to bind before the client dials.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut req = Req0::dial(&addr).await.expect("req dial");
    let mut payload = Message::new();
    payload.push_back(b"hello kcp");
    let reply = req.request(payload).await.expect("client request");
    assert_eq!(reply.body(), b"hello kcp");

    drop(req);
    server.await.unwrap();
}

#[tokio::test]
async fn kcp_reqrep_loopback_many() {
    let addr = free_kcp_addr().await;
    let server_addr = addr.clone();

    const N: u32 = 100;
    let server = tokio::spawn(async move {
        let mut rep = Rep0::listen(&server_addr).await.expect("rep listen");
        for i in 0..N {
            let (msg, responder) = rep.receive().await.expect("server receive");
            assert_eq!(msg.body(), &i.to_be_bytes());
            let mut reply = Message::new();
            reply.push_back(&(i * 2).to_be_bytes());
            responder.reply(reply).await.expect("server reply");
        }
        // See loopback test for why we sleep instead of `let _ = receive()`.
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut req = Req0::dial(&addr).await.expect("req dial");
    for i in 0..N {
        let mut msg = Message::new();
        msg.push_back(&i.to_be_bytes());
        let reply = req.request(msg).await.expect("client request");
        assert_eq!(reply.body(), &(i * 2).to_be_bytes());
    }

    drop(req);
    server.await.unwrap();
}

/// Exercises `dial_kcp_with` / `listen_kcp_with` with a non-default
/// [`KcpConfig`].  Both peers must use the same config; this test passes the
/// same builder to both sides via `clone()`.
#[tokio::test]
async fn kcp_reqrep_loopback_with_config() {
    let addr = free_kcp_addr().await;
    let server_addr = addr.clone();

    // `fast_mode()` is the tuning shown in kcp-tokio's quick-start docs;
    // it lowers latency in exchange for slightly higher bandwidth use.
    let cfg = KcpConfig::new().fast_mode();
    let server_cfg = cfg.clone();

    let server = tokio::spawn(async move {
        let mut rep = Rep0::listen_kcp_with(&server_addr, server_cfg)
            .await
            .expect("rep listen_kcp_with");
        let (msg, responder) = rep.receive().await.expect("server receive");
        assert_eq!(msg.body(), b"hello kcp with config");
        let mut reply = Message::new();
        reply.push_back(b"hello kcp with config");
        responder.reply(reply).await.expect("server reply");
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut req = Req0::dial_kcp_with(&addr, cfg)
        .await
        .expect("req dial_kcp_with");
    let mut payload = Message::new();
    payload.push_back(b"hello kcp with config");
    let reply = req.request(payload).await.expect("client request");
    assert_eq!(reply.body(), b"hello kcp with config");

    drop(req);
    server.await.unwrap();
}
