//! Service discovery — node (respondent side).
//!
//! Dials the registry and responds to ROLL_CALL surveys with its name and
//! address.  Takes two command-line arguments: name and address.
//!
//! Run in separate terminals:
//!   cargo run --example discovery_registry
//!   cargo run --example discovery_node -- alpha tcp://127.0.0.1:9001
//!   cargo run --example discovery_node -- beta  tcp://127.0.0.1:9002

use nng_core::{Message, socket::survey0::Respondent0};
use std::env;

const REGISTRY_ADDR: &str = "tcp://127.0.0.1:11003";

#[tokio::main]
async fn main() {
    let name = env::args().nth(1).unwrap_or_else(|| "node".to_string());
    let addr = env::args().nth(2).unwrap_or_else(|| "unknown".to_string());

    let mut resp = Respondent0::dial(REGISTRY_ADDR).await.expect("dial failed");
    println!("[{name}] connected to registry at {REGISTRY_ADDR}");

    loop {
        match resp.receive().await {
            Ok((survey, handle)) => {
                let query = String::from_utf8_lossy(survey.body()).into_owned();
                println!("[{name}] received survey: {query}");

                if query == "ROLL_CALL" {
                    let reply_text = format!("{name} at {addr}");
                    let mut reply = Message::new();
                    reply.push_back(reply_text.as_bytes());
                    handle.respond(reply).await.expect("respond failed");
                    println!("[{name}] replied: {reply_text}");
                }
            }
            Err(e) => {
                println!("[{name}] registry closed: {e}");
                break;
            }
        }
    }
}
