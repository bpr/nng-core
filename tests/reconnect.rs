/// Reconnect / redial tests.
///
/// Each test simulates a server restart: a listener accepts one connection,
/// is dropped, then a new listener binds the same port.  The client socket —
/// created with `dial_reconnecting` — must transparently re-establish the
/// connection on the next operation.
use std::time::Duration;

use nng_core::Message;
use nng_core::socket::{
    ReconnectOptions,
    pair0::Pair0,
    pipeline0::{Pull0, Push0},
    reqrep0::{Rep0, Req0},
};
use tokio::net::TcpListener;

// ── helpers ───────────────────────────────────────────────────────────────────

/// Bind port 0 on loopback and return the `tcp://` URL for that port.
async fn free_tcp_addr() -> String {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    format!("tcp://127.0.0.1:{port}")
}

fn msg(body: &[u8]) -> Message {
    let mut m = Message::new();
    m.push_back(body);
    m
}

// ── 1. Basic req/rep reconnect ────────────────────────────────────────────────

/// Client sends one request, server answers, server is dropped and restarted,
/// client sends a second request and must receive a second answer.
#[tokio::test]
async fn req0_reconnects_after_server_restart() {
    let addr = free_tcp_addr().await;

    // First server lifetime
    let a1 = addr.clone();
    let server1 = tokio::spawn(async move {
        let mut rep = Rep0::listen(&a1).await.unwrap();
        let (mut msg, resp) = rep.receive().await.unwrap();
        msg.push_back(b"-reply1");
        resp.reply(msg).await.unwrap();
        // Drop rep — server closes
    });

    // Give the listener time to bind.
    tokio::time::sleep(Duration::from_millis(5)).await;

    let mut req = Req0::dial_reconnecting(&addr).await.unwrap();
    let reply = req.request(msg(b"hello")).await.unwrap();
    assert_eq!(reply.body(), b"hello-reply1");
    server1.await.unwrap();

    // Pause so the OS releases the port before the second listener binds.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Second server lifetime on the same address.
    let a2 = addr.clone();
    let server2 = tokio::spawn(async move {
        let mut rep = Rep0::listen(&a2).await.unwrap();
        let (mut msg, resp) = rep.receive().await.unwrap();
        msg.push_back(b"-reply2");
        resp.reply(msg).await.unwrap();
    });

    // Wait for the second server to be ready.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // The client reconnects transparently.
    let reply = req.request(msg(b"world")).await.unwrap();
    assert_eq!(reply.body(), b"world-reply2");
    server2.await.unwrap();
}

// ── 2. Push/pull reconnect ────────────────────────────────────────────────────

#[tokio::test]
async fn push0_reconnects_after_server_restart() {
    let addr = free_tcp_addr().await;

    let a1 = addr.clone();
    let server1 = tokio::spawn(async move {
        let mut pull = Pull0::listen(&a1).await.unwrap();
        let msg = pull.pull().await.unwrap();
        assert_eq!(msg.body(), b"msg1");
        // Drop — server closes
    });

    tokio::time::sleep(Duration::from_millis(5)).await;

    let mut push = Push0::dial_reconnecting(&addr).await.unwrap();
    push.push(msg(b"msg1")).await.unwrap();
    server1.await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let a2 = addr.clone();
    let server2 = tokio::spawn(async move {
        let mut pull = Pull0::listen(&a2).await.unwrap();
        let msg = pull.pull().await.unwrap();
        assert_eq!(msg.body(), b"msg2");
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    push.push(msg(b"msg2")).await.unwrap();
    server2.await.unwrap();
}

// ── 3. Pair0 reconnect ───────────────────────────────────────────────────────

#[tokio::test]
async fn pair0_reconnects_after_server_restart() {
    let addr = free_tcp_addr().await;

    let a1 = addr.clone();
    let server1 = tokio::spawn(async move {
        let mut pair = Pair0::listen(&a1).await.unwrap();
        let msg = pair.recv().await.unwrap();
        assert_eq!(msg.body(), b"ping1");
        pair.send(msg).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(5)).await;

    let mut client = Pair0::dial_reconnecting(&addr).await.unwrap();
    client.send(msg(b"ping1")).await.unwrap();
    let reply = client.recv().await.unwrap();
    assert_eq!(reply.body(), b"ping1");
    server1.await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let a2 = addr.clone();
    let server2 = tokio::spawn(async move {
        let mut pair = Pair0::listen(&a2).await.unwrap();
        let msg = pair.recv().await.unwrap();
        assert_eq!(msg.body(), b"ping2");
        pair.send(msg).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    client.send(msg(b"ping2")).await.unwrap();
    let reply = client.recv().await.unwrap();
    assert_eq!(reply.body(), b"ping2");
    server2.await.unwrap();
}

// ── 4. max_attempts exhausted → error ────────────────────────────────────────

/// With max_attempts=2 and no server restarting, the client must return an
/// error rather than looping forever.
#[tokio::test]
async fn reconnect_fails_after_max_attempts() {
    let addr = free_tcp_addr().await;

    // Start a server, let the client connect, then drop it.
    let a = addr.clone();
    let server = tokio::spawn(async move {
        let mut rep = Rep0::listen(&a).await.unwrap();
        let (msg, resp) = rep.receive().await.unwrap();
        resp.reply(msg).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(5)).await;

    let opts = ReconnectOptions {
        min_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(50),
        max_attempts: Some(2),
    };
    let mut req = Req0::dial_with_reconnect(&addr, opts).await.unwrap();

    // First request succeeds while server is alive.
    let reply = req.request(msg(b"ok")).await.unwrap();
    assert_eq!(reply.body(), b"ok");
    server.await.unwrap();

    // Server is gone. Reconnect attempts will all fail; expect an error.
    // (max_attempts=2, so we'll wait at most 10ms + 20ms = 30ms total)
    let result = req.request(msg(b"gone")).await;
    assert!(
        result.is_err(),
        "expected error after max_attempts exhausted"
    );
}

// ── 5. Plain dial still errors immediately ────────────────────────────────────

/// A socket created with plain `dial` (no reconnect) returns an error
/// immediately on connection loss rather than retrying.
#[tokio::test]
async fn plain_dial_does_not_reconnect() {
    let addr = free_tcp_addr().await;

    let a = addr.clone();
    let server = tokio::spawn(async move {
        let mut rep = Rep0::listen(&a).await.unwrap();
        let (msg, resp) = rep.receive().await.unwrap();
        resp.reply(msg).await.unwrap();
        // Drop — no second round
    });

    tokio::time::sleep(Duration::from_millis(5)).await;

    let mut req = Req0::dial(&addr).await.unwrap();
    let _ = req.request(msg(b"first")).await.unwrap();
    server.await.unwrap();

    // With no reconnect configured, the next operation must fail promptly.
    let start = tokio::time::Instant::now();
    let result = req.request(msg(b"second")).await;
    let elapsed = start.elapsed();

    assert!(result.is_err());
    // Should fail in well under 1 second (no backoff sleep at all).
    assert!(
        elapsed < Duration::from_millis(500),
        "plain dial took too long to fail: {elapsed:?}"
    );
}
