//! PUB/SUB — publisher side.
//!
//! Waits for one subscriber to connect, then broadcasts a "NEWS:" update every
//! second until interrupted.
//!
//! Run in two terminals:
//!   cargo run --example pub_server
//!   cargo run --example sub_client

use nng_core::{Message, socket::pubsub0};
use std::time::Duration;

const ADDR: &str = "tcp://127.0.0.1:10002";

#[tokio::main]
async fn main() {
    let mut pub0 = pubsub0::Pub0::listen(ADDR).await.expect("listen failed");
    println!("Publisher listening on {ADDR}, waiting for subscriber...");
    pub0.wait_for_subscribers(1).await.expect("accept failed");
    println!("Subscriber connected. Broadcasting...");

    let mut counter = 1u32;
    loop {
        let payload = format!("NEWS: Update #{counter}");
        let mut msg = Message::new();
        msg.push_back(payload.as_bytes());
        println!("Published: {payload}");
        pub0.publish(msg).await.expect("publish failed");
        counter += 1;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
