use std::fmt::Write;

use nng_pure::{Message, socket::reqrep0};

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
