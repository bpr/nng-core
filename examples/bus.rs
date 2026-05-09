//! BUS0 example — many-to-many broadcast.
//!
//! A hub node accepts two spoke nodes; it broadcasts a message to both and
//! then each spoke broadcasts a reply back.
//!
//! Run with:  cargo run --example bus

use std::fmt::Write;

use nng_core::{Message, socket::bus0};

const ADDR: &str = "tcp://127.0.0.1:54326";

#[tokio::main]
async fn main() {
    // Spoke 1: receives hub broadcast, then sends its own message.
    let spoke1 = tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let mut node = bus0::Bus0::dial(ADDR).await.expect("dial failed");

        let msg = node.recv_any().await.expect("recv failed");
        println!("[spoke1] received: {}", String::from_utf8_lossy(msg.body()));

        let mut reply = Message::new();
        write!(reply, "hello from spoke1").unwrap();
        node.broadcast(reply).await.expect("broadcast failed");
    });

    // Spoke 2: receives hub broadcast, then sends its own message.
    let spoke2 = tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let mut node = bus0::Bus0::dial(ADDR).await.expect("dial failed");

        let msg = node.recv_any().await.expect("recv failed");
        println!("[spoke2] received: {}", String::from_utf8_lossy(msg.body()));

        let mut reply = Message::new();
        write!(reply, "hello from spoke2").unwrap();
        node.broadcast(reply).await.expect("broadcast failed");
    });

    // Hub: accept 2 spokes, broadcast, then collect one message from each.
    let mut hub = bus0::Bus0::listen_and_accept(ADDR, 2)
        .await
        .expect("listen failed");
    println!("[hub] 2 spokes connected");

    let mut announcement = Message::new();
    write!(announcement, "hub says hello").unwrap();
    hub.broadcast(announcement).await.expect("broadcast failed");
    println!("[hub] broadcast sent");

    // Receive one message from each spoke (order may vary).
    for i in 0..2 {
        let msg = hub.recv_any().await.expect("recv failed");
        println!(
            "[hub] response {}: {}",
            i + 1,
            String::from_utf8_lossy(msg.body())
        );
    }

    spoke1.await.unwrap();
    spoke2.await.unwrap();
    println!("[hub] done");
}
