//! Topic pub/sub — publisher.
//!
//! Publishes on three topic prefixes — SPORTS:, WEATHER:, FINANCE: — in a
//! round-robin cycle.  Each message is prefixed with its topic so subscribers
//! can filter by topic using Sub0's prefix-match subscription.
//!
//! Run in separate terminals:
//!   cargo run --example topic_pub
//!   cargo run --example topic_sub -- SPORTS
//!   cargo run --example topic_sub -- WEATHER
//!   cargo run --example topic_sub -- FINANCE

use nng_core::{Message, socket::pubsub0::Pub0};
use std::time::Duration;

const ADDR: &str = "tcp://127.0.0.1:11002";

#[tokio::main]
async fn main() {
    let mut pub0 = Pub0::listen(ADDR).await.expect("listen failed");
    println!("[pub] publishing on {ADDR}");

    let topics = ["SPORTS", "WEATHER", "FINANCE"];
    let mut seq = 0u32;
    loop {
        let topic = topics[seq as usize % topics.len()];
        seq += 1;
        let payload = format!("{topic}:update-{seq}");

        let mut msg = Message::new();
        msg.push_back(payload.as_bytes());
        println!("[pub] {payload}");
        pub0.publish(msg).await.expect("publish failed");

        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}
