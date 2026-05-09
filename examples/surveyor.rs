//! SURVEYOR/RESPONDENT — surveyor side.
//!
//! Waits for one respondent to connect, broadcasts a ROLL_CALL survey, then
//! collects and prints all replies that arrive within 1 second.
//!
//! Run in two terminals:
//!   cargo run --example surveyor
//!   cargo run --example respondent

use nng_core::{Message, socket::survey0};
use std::time::Duration;

const ADDR: &str = "tcp://127.0.0.1:10003";

#[tokio::main]
async fn main() {
    let mut surveyor = survey0::Surveyor0::listen(ADDR).await.expect("listen failed");
    println!("Surveyor listening on {ADDR}, waiting for respondents...");
    surveyor.wait_for_respondents(1).await.expect("accept failed");
    println!("Respondent connected. Sending survey...");

    let mut q = Message::new();
    q.push_back(b"ROLL_CALL");

    let responses = surveyor
        .survey(q, Duration::from_secs(1))
        .await
        .expect("survey failed");

    println!("Survey complete. {} response(s):", responses.len());
    for r in &responses {
        println!("  - {}", String::from_utf8_lossy(r.body()));
    }
}
