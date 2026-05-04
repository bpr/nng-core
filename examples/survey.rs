//! SURVEYOR/RESPONDENT example — collect answers from multiple nodes.
//!
//! Surveyor broadcasts a "status?" query; two respondents reply with their
//! identities.  The surveyor prints all responses.
//!
//! Run with:  cargo run -p nng-pure --example survey

use std::fmt::Write;
use std::time::Duration;

use nng_pure::{Message, socket::survey0};

const ADDR: &str = "tcp://127.0.0.1:54325";

#[tokio::main]
async fn main() {
    // Respondent 1.
    let r1 = tokio::spawn(async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut resp = survey0::Respondent0::dial(ADDR).await.expect("dial failed");
        let (_q, handle) = resp.receive().await.expect("receive failed");
        let mut reply = Message::new();
        write!(reply, "node-alpha").unwrap();
        handle.respond(reply).await.expect("respond failed");
    });

    // Respondent 2.
    let r2 = tokio::spawn(async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut resp = survey0::Respondent0::dial(ADDR).await.expect("dial failed");
        let (_q, handle) = resp.receive().await.expect("receive failed");
        let mut reply = Message::new();
        write!(reply, "node-beta").unwrap();
        handle.respond(reply).await.expect("respond failed");
    });

    // Surveyor: wait for both then broadcast "status?".
    let mut surveyor = survey0::Surveyor0::listen(ADDR).await.expect("listen failed");
    println!("[surveyor] listening on {ADDR}");
    surveyor.wait_for_respondents(2).await.expect("wait failed");
    println!("[surveyor] 2 respondents connected");

    let mut q = Message::new();
    write!(q, "status?").unwrap();
    let responses = surveyor
        .survey(q, Duration::from_secs(1))
        .await
        .expect("survey failed");

    println!("[surveyor] received {} response(s):", responses.len());
    for r in &responses {
        println!("  - {}", String::from_utf8_lossy(r.body()));
    }

    r1.await.unwrap();
    r2.await.unwrap();
}
