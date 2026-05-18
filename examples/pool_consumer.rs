//! Pipeline (PUSH/PULL) pull side — with zero-allocation steady state.
//!
//! Same role as `pull_worker.rs` but drives `FramedTransport::recv_pooled` +
//! `BufferPool::recycle` directly so every iteration after the first reuses
//! the same `Vec<u8>` body buffer.
//!
//! Run in two terminals:
//!   cargo run --example push_node
//!   cargo run --example pool_consumer
//!
//! Watch the printed body pointer: once the first frame is recycled, every
//! subsequent receive lands at the same address — proof that the buffer was
//! reused in place rather than reallocated.

use nng_core::{
    BufferPool,
    codec::ProtocolId,
    transport::{FrameFormat, FramedTransport, tcp::TokioTcpStream},
};
use tokio::net::TcpStream;

const ADDR: &str = "127.0.0.1:10001";

#[tokio::main]
async fn main() {
    let stream = TcpStream::connect(ADDR)
        .await
        .expect("connect failed — start push_node first");
    let mut transport =
        FramedTransport::connect(TokioTcpStream(stream), ProtocolId::PULL0, FrameFormat::Tcp)
            .await
            .expect("SP handshake failed");
    println!("Consumer connected to {ADDR}, waiting for tasks...");

    let mut pool = BufferPool::new();
    let mut received = 0u64;

    loop {
        match transport.recv_pooled(&mut pool).await {
            Ok(msg) => {
                received += 1;
                println!(
                    "recv #{received}: {:<14} body @ {:p}",
                    String::from_utf8_lossy(msg.body()),
                    msg.body().as_ptr(),
                );
                pool.recycle(msg);
            }
            Err(e) => {
                println!("Done: {e}");
                break;
            }
        }
    }
}
