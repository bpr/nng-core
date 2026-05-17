use std::time::Duration;

use nng_core::{Message, socket::reqrep0};

/// Pick a free port for the test server.  We bind 0 and extract the port.
async fn free_addr() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("tcp://127.0.0.1:{port}")
}

#[tokio::test]
async fn req_rep_single_roundtrip() {
    let addr = free_addr().await;

    let server_addr = addr.clone();
    let server = tokio::spawn(async move {
        let mut rep = reqrep0::Rep0::listen(&server_addr).await.unwrap();
        let (msg, responder) = rep.receive().await.unwrap();
        assert_eq!(msg.body(), b"hello");
        let mut reply = Message::new();
        reply.push_back(b"world");
        responder.reply(reply).await.unwrap();
    });

    // Give server a moment to bind.
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let mut req = reqrep0::Req0::dial(&addr).await.unwrap();
    let mut request = Message::new();
    request.push_back(b"hello");
    let reply = req.request(request).await.unwrap();
    assert_eq!(reply.body(), b"world");

    server.await.unwrap();
}

#[tokio::test]
async fn req_rep_100_roundtrips() {
    let addr = free_addr().await;

    let server_addr = addr.clone();
    let server = tokio::spawn(async move {
        let mut rep = reqrep0::Rep0::listen(&server_addr).await.unwrap();
        for i in 0u32..100 {
            let (msg, responder) = rep.receive().await.unwrap();
            assert_eq!(msg.body(), &i.to_be_bytes());
            let mut reply = Message::new();
            reply.push_back(&(i * 2).to_be_bytes());
            responder.reply(reply).await.unwrap();
        }
    });

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let mut req = reqrep0::Req0::dial(&addr).await.unwrap();
    for i in 0u32..100 {
        let mut request = Message::new();
        request.push_back(&i.to_be_bytes());
        let reply = req.request(request).await.unwrap();
        assert_eq!(reply.body(), &(i * 2).to_be_bytes());
    }

    server.await.unwrap();
}

#[tokio::test]
async fn req_rep_large_message() {
    let addr = free_addr().await;

    let data: Vec<u8> = (0u8..=255).cycle().take(50_000).collect();
    let data_clone = data.clone();

    let server_addr = addr.clone();
    let server = tokio::spawn(async move {
        let mut rep = reqrep0::Rep0::listen(&server_addr).await.unwrap();
        let (msg, responder) = rep.receive().await.unwrap();
        // Echo back
        let mut reply = Message::new();
        reply.push_back(msg.body());
        responder.reply(reply).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let mut req = reqrep0::Req0::dial(&addr).await.unwrap();
    let mut request = Message::new();
    request.push_back(&data);
    let reply = req.request(request).await.unwrap();
    assert_eq!(reply.body(), data_clone.as_slice());

    server.await.unwrap();
}

// ── resend tests ──────────────────────────────────────────────────────────────

/// Server drops the first request (no reply) and only answers the resend.
/// Client must receive the reply from the resend.
#[tokio::test]
async fn req0_resend_when_first_request_dropped() {
    let addr = free_addr().await;

    let server_addr = addr.clone();
    let server = tokio::spawn(async move {
        let mut rep = reqrep0::Rep0::listen(&server_addr).await.unwrap();

        // Receive the first request but do not reply — drop the Responder.
        let _dropped = rep.receive().await.unwrap();

        // Reply to the resend.
        let (msg, responder) = rep.receive().await.unwrap();
        let mut reply = Message::new();
        reply.push_back(msg.body());
        responder.reply(reply).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut req = reqrep0::Req0::dial(&addr).await.unwrap();
    req.set_resend_time(Duration::from_millis(50));

    let mut msg = Message::new();
    msg.push_back(b"ping");
    let reply = req.request(msg).await.unwrap();
    assert_eq!(reply.body(), b"ping");

    server.await.unwrap();
}

/// Stale replies (from resends of a prior request) must be discarded when the
/// next `request()` call runs.
///
/// The gate delays the first reply until after the resend fires, guaranteeing
/// the server produces at least one extra reply (for the resend).  The server
/// loops until it answers `b"req2"`, handling any number of resends — so the
/// test is robust to scheduling jitter that causes multiple resends to fire.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn req0_stale_reply_discarded_on_next_request() {
    let addr = free_addr().await;
    let addr2 = addr.clone();

    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        let mut rep = reqrep0::Rep0::listen(&addr2).await.unwrap();

        // Receive the first request; hold the reply until the gate opens so
        // the resend fires first.
        let (msg, resp) = rep.receive().await.unwrap();
        gate_rx.await.unwrap();
        let mut reply = Message::new();
        reply.push_back(msg.body());
        resp.reply(reply).await.unwrap();

        // Reply to all subsequent messages (resends of request 1, then request 2)
        // until the body b"req2" is answered.
        loop {
            let Ok((msg, resp)) = rep.receive().await else {
                break;
            };
            let done = msg.body() == b"req2";
            let mut reply = Message::new();
            reply.push_back(msg.body());
            let _ = resp.reply(reply).await;
            if done {
                break;
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut req = reqrep0::Req0::dial(&addr).await.unwrap();
    req.set_resend_time(Duration::from_millis(50));

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(80)).await;
        let _ = gate_tx.send(());
    });

    // Request 1 (ID=1): resend fires at 50 ms; gate opens at 80 ms.
    let mut msg1 = Message::new();
    msg1.push_back(b"req1");
    let reply1 = req.request(msg1).await.unwrap();
    assert_eq!(reply1.body(), b"req1");

    // Request 2 (ID=2): stale replies from request 1's resends must be
    // silently discarded; the first reply with ID=2 is accepted.
    let mut msg2 = Message::new();
    msg2.push_back(b"req2");
    let reply2 = req.request(msg2).await.unwrap();
    assert_eq!(reply2.body(), b"req2");

    server.await.unwrap();
}

/// With no resend configured, request() waits indefinitely and returns the
/// first valid reply even when the server is slow.
#[tokio::test]
async fn req0_no_resend_waits_for_late_reply() {
    let addr = free_addr().await;

    let server_addr = addr.clone();
    let server = tokio::spawn(async move {
        let mut rep = reqrep0::Rep0::listen(&server_addr).await.unwrap();
        let (msg, responder) = rep.receive().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut reply = Message::new();
        reply.push_back(msg.body());
        responder.reply(reply).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut req = reqrep0::Req0::dial(&addr).await.unwrap();
    // resend_time deliberately NOT set.
    let mut msg = Message::new();
    msg.push_back(b"slow");
    let reply = req.request(msg).await.unwrap();
    assert_eq!(reply.body(), b"slow");

    server.await.unwrap();
}
