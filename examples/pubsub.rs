//! PUB/SUB example with topic filtering.
//!
//! A publisher broadcasts messages with different topic prefixes.
//! Two subscribers each filter to a single topic and print what they receive.
//!
//! Run with:  cargo run -p nng-core --example pubsub

use std::fmt::Write;

use nng_core::{Message, socket::pubsub0};

const ADDR: &str = "tcp://127.0.0.1:54322";

#[tokio::main]
async fn main() {
    // Sports subscriber: only wants "sports:" messages.
    let sports = tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let mut sub = pubsub0::Sub0::dial(ADDR).await.expect("dial failed");
        sub.subscribe_to(b"sports:");
        for _ in 0..2 {
            let msg = sub.next().await.expect("recv failed");
            println!("[sports] {}", String::from_utf8_lossy(msg.body()));
        }
    });

    // Weather subscriber: only wants "weather:" messages.
    let weather = tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let mut sub = pubsub0::Sub0::dial(ADDR).await.expect("dial failed");
        sub.subscribe_to(b"weather:");
        let msg = sub.next().await.expect("recv failed");
        println!("[weather] {}", String::from_utf8_lossy(msg.body()));
    });

    // Publisher: wait for both subscribers then broadcast.
    let mut pub0 = pubsub0::Pub0::listen(ADDR).await.expect("listen failed");
    println!("[pub] listening on {ADDR}");
    pub0.wait_for_subscribers(2).await.expect("wait failed");
    println!("[pub] 2 subscribers connected");

    let topics = [
        "sports: Team scores!",
        "weather: Sunny skies",
        "sports: Final whistle!",
    ];
    for text in topics {
        let mut msg = Message::new();
        write!(msg, "{text}").unwrap();
        println!("[pub] sending: {text}");
        pub0.publish(msg).await.expect("publish failed");
    }

    sports.await.unwrap();
    weather.await.unwrap();
    println!("[pub] done");
}
