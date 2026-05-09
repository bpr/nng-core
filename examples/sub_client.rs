//! PUB/SUB — subscriber side.
//!
//! Dials the publisher, subscribes to the "NEWS:" topic prefix, and prints
//! every matching message until interrupted.
//!
//! Run in two terminals:
//!   cargo run --example pub_server
//!   cargo run --example sub_client

use nng_core::socket::pubsub0;

const ADDR: &str = "tcp://127.0.0.1:10002";

#[tokio::main]
async fn main() {
    let mut sub = pubsub0::Sub0::dial(ADDR).await.expect("dial failed");
    sub.subscribe_to(b"NEWS:");
    println!("Subscribed to NEWS: on {ADDR}");

    loop {
        match sub.next().await {
            Ok(msg) => println!("Received: {}", String::from_utf8_lossy(msg.body())),
            Err(e) => { println!("Done: {e}"); break; }
        }
    }
}
