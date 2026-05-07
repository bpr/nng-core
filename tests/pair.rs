use std::time::Duration;

use nng_core::{Message, socket::pair0};

async fn free_addr() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("tcp://127.0.0.1:{port}")
}

#[tokio::test]
async fn pair_bidirectional_echo() {
    let addr = free_addr().await;

    let server_addr = addr.clone();
    let server = tokio::spawn(async move {
        let mut pair = pair0::Pair0::listen(&server_addr).await.unwrap();
        // Echo 5 messages back.
        for _ in 0..5 {
            let msg = pair.recv().await.unwrap();
            pair.send(msg).await.unwrap();
        }
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut client = pair0::Pair0::dial(&addr).await.unwrap();
    for i in 0u32..5 {
        let mut msg = Message::new();
        msg.push_back(&i.to_be_bytes());
        client.send(msg).await.unwrap();
        let reply = client.recv().await.unwrap();
        assert_eq!(reply.body(), &i.to_be_bytes());
    }

    server.await.unwrap();
}

#[tokio::test]
async fn pair_both_directions_independently() {
    let addr = free_addr().await;

    let server_addr = addr.clone();
    let server = tokio::spawn(async move {
        let mut pair = pair0::Pair0::listen(&server_addr).await.unwrap();

        // Send 3 messages from server side.
        for i in 0u32..3 {
            let mut msg = Message::new();
            msg.push_back(b"from-server");
            msg.push_back(&i.to_be_bytes());
            pair.send(msg).await.unwrap();
        }

        // Receive 3 messages from client.
        for i in 0u32..3 {
            let msg = pair.recv().await.unwrap();
            let expected = {
                let mut m = Message::new();
                m.push_back(b"from-client");
                m.push_back(&i.to_be_bytes());
                m
            };
            assert_eq!(msg.body(), expected.body());
        }
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut client = pair0::Pair0::dial(&addr).await.unwrap();

    // Send 3 messages from client side.
    for i in 0u32..3 {
        let mut msg = Message::new();
        msg.push_back(b"from-client");
        msg.push_back(&i.to_be_bytes());
        client.send(msg).await.unwrap();
    }

    // Receive 3 messages from server.
    for i in 0u32..3 {
        let msg = client.recv().await.unwrap();
        let expected = {
            let mut m = Message::new();
            m.push_back(b"from-server");
            m.push_back(&i.to_be_bytes());
            m
        };
        assert_eq!(msg.body(), expected.body());
    }

    server.await.unwrap();
}
