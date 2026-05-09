//! Topic pub/sub — subscriber.
//!
//! Connects to the publisher and subscribes to the topic prefix given on the
//! command line (e.g. "SPORTS", "WEATHER", "FINANCE").  Only messages whose
//! body starts with that prefix are delivered.
//!
//! Run in separate terminals:
//!   cargo run --example topic_pub
//!   cargo run --example topic_sub -- SPORTS
//!   cargo run --example topic_sub -- WEATHER
//!   cargo run --example topic_sub -- FINANCE

use nng_core::socket::pubsub0::Sub0;
use std::env;

const ADDR: &str = "tcp://127.0.0.1:11002";

#[tokio::main]
async fn main() {
    let topic = env::args().nth(1).unwrap_or_else(|| "SPORTS".to_string());

    let mut sub = Sub0::dial(ADDR).await.expect("dial failed");
    sub.subscribe_to(topic.as_bytes());
    println!("[sub/{topic}] subscribed on {ADDR}");

    loop {
        match sub.next().await {
            Ok(msg) => {
                let text = String::from_utf8_lossy(msg.body()).into_owned();
                println!("[sub/{topic}] {text}");
            }
            Err(e) => {
                println!("[sub/{topic}] done: {e}");
                break;
            }
        }
    }
}
