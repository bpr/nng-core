//! Minimal REQ/REP example over KCP (`--features kcp`).
//!
//! Run with two terminals:
//! ```text
//! cargo run --example kcp_reqrep --features kcp -- --server
//! cargo run --example kcp_reqrep --features kcp -- --client
//! ```

use nng_core::{
    Message,
    socket::reqrep0::{Rep0, Req0},
};
use std::env;

const ADDR: &str = "kcp://127.0.0.1:5555";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match env::args().nth(1).as_deref() {
        Some("--server") => run_server().await,
        Some("--client") => run_client().await,
        _ => {
            eprintln!("usage: kcp_reqrep --server | --client");
            Ok(())
        }
    }
}

async fn run_server() -> Result<(), Box<dyn std::error::Error>> {
    let mut rep = Rep0::listen(ADDR).await?;
    println!("rep listening on {ADDR}");
    loop {
        let (msg, responder) = rep.receive().await?;
        println!("rep got: {:?}", std::str::from_utf8(msg.body()).ok());
        let mut echo = Message::new();
        echo.push_back(msg.body());
        responder.reply(echo).await?;
    }
}

async fn run_client() -> Result<(), Box<dyn std::error::Error>> {
    let mut req = Req0::dial(ADDR).await?;
    let mut msg = Message::new();
    msg.push_back(b"hello kcp");
    let reply = req.request(msg).await?;
    println!("req got: {:?}", std::str::from_utf8(reply.body()).ok());
    Ok(())
}
