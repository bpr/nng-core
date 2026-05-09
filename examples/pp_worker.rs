//! Paranoid Pirate — worker.
//!
//! Demonstrates a resilient worker that crashes every 4th request and
//! recovers by re-listening.  The client (pp_client) retries on timeout,
//! so no request is permanently lost as long as the worker restarts.
//!
//! Run in separate terminals:
//!   cargo run --example pp_worker
//!   cargo run --example pp_client

use nng_core::{Message, socket::reqrep0::Rep0};
use std::time::Duration;

const ADDR: &str = "tcp://127.0.0.1:11004";

#[tokio::main]
async fn main() {
    let mut served = 0u32;
    loop {
        println!("[worker] listening on {ADDR}");
        let mut rep = match Rep0::listen(ADDR).await {
            Ok(r) => r,
            Err(e) => { eprintln!("[worker] bind failed: {e}"); break; }
        };

        loop {
            let (req, responder) = match rep.receive().await {
                Ok(r) => r,
                Err(e) => { println!("[worker] connection closed: {e}"); break; }
            };

            served += 1;
            let text = String::from_utf8_lossy(req.body()).into_owned();
            println!("[worker] request #{served}: {text}");

            // Simulate a crash every 4th request.
            if served % 4 == 0 {
                println!("[worker] simulating crash on request #{served}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                break; // Drop `rep`; the client will timeout and retry.
            }

            let reply_text = format!("ok:{text}");
            let mut reply = Message::new();
            reply.push_back(reply_text.as_bytes());
            responder.reply(reply).await.expect("reply failed");
        }
        // Brief pause before re-listening so the OS releases the port.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
