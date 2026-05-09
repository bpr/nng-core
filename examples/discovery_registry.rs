//! Service discovery — registry (surveyor side).
//!
//! Listens for nodes, then periodically broadcasts a ROLL_CALL survey and
//! prints all responses received within the timeout window.  New nodes can
//! join or leave between surveys.
//!
//! Run in separate terminals:
//!   cargo run --example discovery_registry
//!   cargo run --example discovery_node -- alpha tcp://127.0.0.1:9001
//!   cargo run --example discovery_node -- beta  tcp://127.0.0.1:9002

use nng_core::{Message, socket::survey0::Surveyor0};
use std::time::Duration;

const ADDR: &str = "tcp://127.0.0.1:11003";
const SURVEY_TIMEOUT: Duration = Duration::from_millis(500);
const SURVEY_INTERVAL: Duration = Duration::from_secs(3);

#[tokio::main]
async fn main() {
    let mut surveyor = Surveyor0::listen(ADDR).await.expect("listen failed");
    println!("[registry] listening on {ADDR}");

    // Wait for at least one node before starting surveys.
    surveyor.wait_for_respondents(1).await.expect("wait failed");
    println!("[registry] at least one node connected — starting surveys");

    loop {
        // Pick up any nodes that joined since the last survey.
        surveyor.accept_pending().await;

        let mut query = Message::new();
        query.push_back(b"ROLL_CALL");

        println!("[registry] --- survey ---");
        match surveyor.survey(query, SURVEY_TIMEOUT).await {
            Ok(replies) if replies.is_empty() => {
                println!("[registry] no responses");
            }
            Ok(replies) => {
                for reply in &replies {
                    let text = String::from_utf8_lossy(reply.body());
                    println!("[registry] node: {text}");
                }
                println!("[registry] {} node(s) responded", replies.len());
            }
            Err(e) => println!("[registry] survey error: {e}"),
        }

        tokio::time::sleep(SURVEY_INTERVAL).await;
    }
}
