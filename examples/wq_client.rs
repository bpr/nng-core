//! Work-queue — client.
//!
//! Sends 8 requests to the broker sequentially and prints each reply.  The
//! broker distributes these across its worker pool so multiple requests can
//! run in parallel (visible in worker terminal output).
//!
//! Run in separate terminals:
//!   cargo run --example wq_worker -- 0
//!   cargo run --example wq_worker -- 1
//!   cargo run --example wq_worker -- 2
//!   cargo run --example wq_broker
//!   cargo run --example wq_client

use nng_core::{Message, socket::reqrep0::Req0};

const BROKER_ADDR: &str = "tcp://127.0.0.1:11005";
const N_REQUESTS: u32 = 8;

#[tokio::main]
async fn main() {
    let mut req = Req0::dial(BROKER_ADDR).await.expect("dial failed");
    println!("[client] connected to broker at {BROKER_ADDR}");

    for i in 1..=N_REQUESTS {
        let payload = format!("job-{i}");
        println!("[client] sending: {payload}");

        let mut msg = Message::new();
        msg.push_back(payload.as_bytes());

        let reply = req.request(msg).await.expect("request failed");
        let text = String::from_utf8_lossy(reply.body()).into_owned();
        println!("[client] reply: {text}");
    }
    println!("[client] all {N_REQUESTS} requests complete");
}
