//! REQ/REP — server side.
//!
//! Listens for one client connection, then handles requests in a loop until
//! the client disconnects.
//!
//! Run in two terminals:
//!   cargo run --example req_server
//!   cargo run --example req_client

use nng_core::{Message, socket::reqrep0};

const ADDR: &str = "tcp://127.0.0.1:10000";

#[tokio::main]
async fn main() {
    let mut rep = reqrep0::Rep0::listen(ADDR).await.expect("listen failed");
    println!("Server listening on {ADDR}");

    loop {
        match rep.receive().await {
            Ok((request, responder)) => {
                let text = String::from_utf8_lossy(request.body()).into_owned();
                println!("Server received: {text}");

                let mut reply = Message::new();
                reply.push_back(b"Pong!");
                responder.reply(reply).await.expect("reply failed");
            }
            Err(e) => {
                println!("Server done: {e}");
                break;
            }
        }
    }
}
