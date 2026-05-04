use std::time::Duration;

use nng_pure::{Message, socket::bus0};

async fn free_addr() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("tcp://127.0.0.1:{port}")
}

#[tokio::test]
async fn bus_broadcast_to_two_peers() {
    let addr = free_addr().await;

    // Hub accepts 2 spoke connections.
    let hub_addr = addr.clone();
    let hub = tokio::spawn(async move {
        let mut hub = bus0::Bus0::listen_and_accept(&hub_addr, 2).await.unwrap();
        assert_eq!(hub.peer_count(), 2);

        let mut msg = Message::new();
        msg.push_back(b"broadcast");
        hub.broadcast(msg).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let addr1 = addr.clone();
    let spoke1 = tokio::spawn(async move {
        let mut node = bus0::Bus0::dial(&addr1).await.unwrap();
        let msg = node.recv_any().await.unwrap();
        assert_eq!(msg.body(), b"broadcast");
    });

    let spoke2 = tokio::spawn(async move {
        let mut node = bus0::Bus0::dial(&addr).await.unwrap();
        let msg = node.recv_any().await.unwrap();
        assert_eq!(msg.body(), b"broadcast");
    });

    hub.await.unwrap();
    spoke1.await.unwrap();
    spoke2.await.unwrap();
}

#[tokio::test]
async fn bus_two_nodes_bidirectional() {
    let addr = free_addr().await;

    let server_addr = addr.clone();
    let server = tokio::spawn(async move {
        let mut node_a = bus0::Bus0::listen(&server_addr).await.unwrap();

        // A receives from B.
        let msg = node_a.recv_from(0).await.unwrap();
        assert_eq!(msg.body(), b"from-B");

        // A sends back to B.
        let mut reply = Message::new();
        reply.push_back(b"from-A");
        node_a.broadcast(reply).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut node_b = bus0::Bus0::dial(&addr).await.unwrap();

    let mut msg = Message::new();
    msg.push_back(b"from-B");
    node_b.broadcast(msg).await.unwrap();

    let reply = node_b.recv_from(0).await.unwrap();
    assert_eq!(reply.body(), b"from-A");

    server.await.unwrap();
}
