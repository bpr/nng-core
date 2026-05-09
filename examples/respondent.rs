//! SURVEYOR/RESPONDENT — respondent side.
//!
//! Dials the surveyor, receives surveys in a loop, and replies to ROLL_CALL
//! queries until the surveyor disconnects.
//!
//! Run in two terminals:
//!   cargo run --example surveyor
//!   cargo run --example respondent

use nng_core::{Message, socket::survey0};

const ADDR: &str = "tcp://127.0.0.1:10003";

#[tokio::main]
async fn main() {
    let mut resp = survey0::Respondent0::dial(ADDR).await.expect("dial failed");
    println!("Respondent connected to {ADDR}");

    loop {
        match resp.receive().await {
            Ok((survey, handle)) => {
                let query = String::from_utf8_lossy(survey.body()).into_owned();
                println!("Received survey: {query}");

                if query == "ROLL_CALL" {
                    let mut reply = Message::new();
                    reply.push_back(b"I am here!");
                    handle.respond(reply).await.expect("respond failed");
                }
            }
            Err(e) => {
                println!("Respondent done: {e}");
                break;
            }
        }
    }
}
