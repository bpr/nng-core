//! Work-queue — broker.
//!
//! Fronts a pool of workers: listens for client requests on CLIENT_ADDR and
//! round-robins each request to the next available worker.  Sequential within
//! each request-reply cycle, so this is a simple (non-concurrent) proxy.
//!
//! Run in separate terminals:
//!   cargo run --example wq_worker -- 0
//!   cargo run --example wq_worker -- 1
//!   cargo run --example wq_worker -- 2
//!   cargo run --example wq_broker
//!   cargo run --example wq_client

use nng_core::{
    Message,
    socket::reqrep0::{Rep0, Req0},
};

const CLIENT_ADDR: &str = "tcp://127.0.0.1:11005";
const WORKER_BASE_PORT: u16 = 12000;
const N_WORKERS: usize = 3;

#[tokio::main]
async fn main() {
    // Connect to all workers up front.
    let mut workers: Vec<Req0> = Vec::with_capacity(N_WORKERS);
    for i in 0..N_WORKERS {
        let addr = format!("tcp://127.0.0.1:{}", WORKER_BASE_PORT + i as u16);
        let w = Req0::dial(&addr).await.expect("worker dial failed");
        println!("[broker] connected to worker-{i} at {addr}");
        workers.push(w);
    }

    // Listen for client connections.
    let mut frontend = Rep0::listen(CLIENT_ADDR).await.expect("listen failed");
    println!("[broker] client endpoint on {CLIENT_ADDR}");

    let mut next_worker = 0usize;
    loop {
        let (client_req, responder) = match frontend.receive().await {
            Ok(r) => r,
            Err(e) => {
                println!("[broker] done: {e}");
                break;
            }
        };

        let text = String::from_utf8_lossy(client_req.body()).into_owned();
        println!("[broker] routing '{text}' to worker-{next_worker}");

        let mut fwd = Message::new();
        fwd.push_back(text.as_bytes());

        let reply = workers[next_worker]
            .request(fwd)
            .await
            .expect("worker request failed");

        next_worker = (next_worker + 1) % N_WORKERS;

        let reply_text = String::from_utf8_lossy(reply.body()).into_owned();
        println!("[broker] got reply: {reply_text}");

        let mut out = Message::new();
        out.push_back(reply_text.as_bytes());
        responder.reply(out).await.expect("reply to client failed");
    }
}
