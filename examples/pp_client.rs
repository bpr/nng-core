//! Paranoid Pirate — client.
//!
//! Sends requests with a per-attempt timeout.  If a request times out (because
//! the worker crashed), the client re-dials and retries up to MAX_RETRIES
//! times before giving up.  This pattern tolerates transient worker failures
//! without losing work.
//!
//! Run in separate terminals:
//!   cargo run --example pp_worker
//!   cargo run --example pp_client

use nng_core::{Message, socket::reqrep0::Req0};
use std::time::Duration;

const ADDR: &str = "tcp://127.0.0.1:11004";
const TIMEOUT: Duration = Duration::from_secs(2);
const MAX_RETRIES: u32 = 3;
const N_REQUESTS: u32 = 10;

async fn send_with_retry(payload: &str) -> Option<String> {
    for attempt in 1..=MAX_RETRIES {
        let mut req = match Req0::dial(ADDR).await {
            Ok(r) => r,
            Err(e) => {
                println!("[client] dial failed (attempt {attempt}): {e}");
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };

        let mut msg = Message::new();
        msg.push_back(payload.as_bytes());

        match tokio::time::timeout(TIMEOUT, req.request(msg)).await {
            Ok(Ok(reply)) => {
                return Some(String::from_utf8_lossy(reply.body()).into_owned());
            }
            Ok(Err(e)) => println!("[client] request error (attempt {attempt}): {e}"),
            Err(_) => println!("[client] timeout (attempt {attempt})"),
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

#[tokio::main]
async fn main() {
    for i in 1..=N_REQUESTS {
        let payload = format!("task-{i}");
        println!("[client] sending: {payload}");
        match send_with_retry(&payload).await {
            Some(reply) => println!("[client] reply: {reply}"),
            None => println!("[client] gave up on {payload} after {MAX_RETRIES} retries"),
        }
    }
    println!("[client] done");
}
