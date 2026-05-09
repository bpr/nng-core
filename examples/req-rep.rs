//! REQ/REP example — mirrors anng/examples/async_reqrep.rs
//!
//! Two tokio tasks communicate over TCP using the REQ/REP pattern.
//! Run with:  cargo run --example req-rep

use std::fmt::Write;

use nng_core::{Message, socket::reqrep0};

const ADDR: &str = "tcp://127.0.0.1:54321";

#[tokio::main]
async fn main() {
    // Server task: handle one request and reply.
    let server = tokio::spawn(async {
        let mut rep = reqrep0::Rep0::listen(ADDR).await.expect("listen failed");
        println!("[server] listening on {ADDR}");

        let (request, responder) = rep.receive().await.expect("receive failed");
        let text = String::from_utf8_lossy(request.body());
        println!("[server] received: {text}");

        let mut reply = Message::new();
        write!(reply, "Hello, {text}!").unwrap();
        responder.reply(reply).await.expect("reply failed");
        println!("[server] replied");
    });

    // Give the server a moment to bind.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // Client: send one request and print the reply.
    let mut req = reqrep0::Req0::dial(ADDR).await.expect("dial failed");
    println!("[client] connected to {ADDR}");

    let mut msg = Message::new();
    write!(msg, "Ferris").unwrap();
    let reply = req.request(msg).await.expect("request failed");

    let text = String::from_utf8_lossy(reply.body());
    println!("[client] reply: {text}");

    server.await.unwrap();
}
