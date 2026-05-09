//! Interoperability tests between nng-core and the native C `nngcat` tool.
//!
//! Tests are skipped automatically when `nngcat` is not found in PATH.
//! Each test uses a unique socket path / port to avoid collisions when the
//! suite runs in parallel.

use nng_core::{
    Message,
    socket::{pipeline0, pubsub0, reqrep0},
};
use std::time::Duration;

fn nngcat_available() -> bool {
    std::process::Command::new("nngcat")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

macro_rules! require_nngcat {
    () => {
        if !nngcat_available() {
            eprintln!("nngcat not found in PATH — skipping interop test");
            return;
        }
    };
}

// ── REQ/REP ──────────────────────────────────────────────────────────────────

/// Rust REQ → nngcat REP over IPC.
#[tokio::test]
async fn req_rust_to_rep_nngcat_ipc() {
    require_nngcat!();
    let url = "ipc:///tmp/nng_core_req_rust_rep_c.ipc";

    // nngcat REP: bind, receive one request, reply with "pong", exit.
    let mut server = std::process::Command::new("nngcat")
        .args(["--rep0", "--listen", url, "--data", "pong"])
        .spawn()
        .expect("failed to spawn nngcat");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut req = reqrep0::Req0::dial(url).await.unwrap();
    let mut msg = Message::new();
    msg.push_back(b"ping");
    let reply = req.request(msg).await.unwrap();
    assert_eq!(reply.body(), b"pong");

    let _ = server.kill();
    let _ = server.wait();
}

/// Rust REQ → nngcat REP over TCP.
#[tokio::test]
async fn req_rust_to_rep_nngcat_tcp() {
    require_nngcat!();
    let url = "tcp://127.0.0.1:15580";

    let mut server = std::process::Command::new("nngcat")
        .args(["--rep0", "--listen", url, "--data", "pong"])
        .spawn()
        .expect("failed to spawn nngcat");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut req = reqrep0::Req0::dial(url).await.unwrap();
    let mut msg = Message::new();
    msg.push_back(b"ping");
    let reply = req.request(msg).await.unwrap();
    assert_eq!(reply.body(), b"pong");

    let _ = server.kill();
    let _ = server.wait();
}

/// nngcat REQ → Rust REP over IPC.
///
/// Rep0::listen = bind + accept.  We spawn the listen future as a background
/// task so that the bind completes before nngcat tries to dial.
#[tokio::test]
async fn req_nngcat_to_rep_rust_ipc() {
    require_nngcat!();
    let url = "ipc:///tmp/nng_core_req_c_rep_rust.ipc";

    let url_owned = url.to_string();
    let listen_task = tokio::spawn(async move { reqrep0::Rep0::listen(&url_owned).await });

    // Wait for bind to complete before nngcat dials.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = std::process::Command::new("nngcat")
        .args(["--req0", "--dial", url, "--data", "ping"])
        .spawn()
        .expect("failed to spawn nngcat");

    let mut rep = tokio::time::timeout(Duration::from_secs(2), listen_task)
        .await
        .expect("listen timed out")
        .unwrap()
        .unwrap();

    let (msg, responder) = tokio::time::timeout(Duration::from_secs(2), rep.receive())
        .await
        .expect("receive timed out")
        .unwrap();
    assert_eq!(msg.body(), b"ping");

    let mut reply = Message::new();
    reply.push_back(b"pong");
    responder.reply(reply).await.unwrap();

    let _ = client.wait();
}

/// nngcat REQ → Rust REP over TCP.
#[tokio::test]
async fn req_nngcat_to_rep_rust_tcp() {
    require_nngcat!();
    let url = "tcp://127.0.0.1:15581";

    let url_owned = url.to_string();
    let listen_task = tokio::spawn(async move { reqrep0::Rep0::listen(&url_owned).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = std::process::Command::new("nngcat")
        .args(["--req0", "--dial", url, "--data", "ping"])
        .spawn()
        .expect("failed to spawn nngcat");

    let mut rep = tokio::time::timeout(Duration::from_secs(2), listen_task)
        .await
        .expect("listen timed out")
        .unwrap()
        .unwrap();

    let (msg, responder) = tokio::time::timeout(Duration::from_secs(2), rep.receive())
        .await
        .expect("receive timed out")
        .unwrap();
    assert_eq!(msg.body(), b"ping");

    let mut reply = Message::new();
    reply.push_back(b"pong");
    responder.reply(reply).await.unwrap();

    let _ = client.wait();
}

// ── PUSH/PULL ─────────────────────────────────────────────────────────────────

/// Rust PUSH → nngcat PULL over IPC.
#[tokio::test]
async fn push_rust_to_pull_nngcat_ipc() {
    require_nngcat!();
    let url = "ipc:///tmp/nng_core_push_rust_pull_c.ipc";

    let mut pull_proc = std::process::Command::new("nngcat")
        .args(["--pull0", "--listen", url])
        .spawn()
        .expect("failed to spawn nngcat");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut push = pipeline0::Push0::dial(url).await.unwrap();
    let mut msg = Message::new();
    msg.push_back(b"hello from rust");
    push.push(msg).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = pull_proc.kill();
    let _ = pull_proc.wait();
}

/// nngcat PUSH → Rust PULL over IPC.
#[tokio::test]
async fn push_nngcat_to_pull_rust_ipc() {
    require_nngcat!();
    let url = "ipc:///tmp/nng_core_push_c_pull_rust.ipc";

    let url_owned = url.to_string();
    let listen_task = tokio::spawn(async move { pipeline0::Pull0::listen(&url_owned).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut push_proc = std::process::Command::new("nngcat")
        .args(["--push0", "--dial", url, "--data", "hello from nngcat"])
        .spawn()
        .expect("failed to spawn nngcat");

    let mut pull = tokio::time::timeout(Duration::from_secs(2), listen_task)
        .await
        .expect("listen timed out")
        .unwrap()
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), pull.pull())
        .await
        .expect("pull timed out")
        .unwrap();

    assert_eq!(msg.body(), b"hello from nngcat");

    let _ = push_proc.wait();
}

// ── PUB/SUB ──────────────────────────────────────────────────────────────────

/// Rust PUB → nngcat SUB over IPC.
#[tokio::test]
async fn pub_rust_to_sub_nngcat_ipc() {
    require_nngcat!();
    let url = "ipc:///tmp/nng_core_pub_rust_sub_c.ipc";

    let mut pub0 = pubsub0::Pub0::listen(url).await.unwrap();

    let mut sub_proc = std::process::Command::new("nngcat")
        .args(["--sub0", "--dial", url])
        .spawn()
        .expect("failed to spawn nngcat");

    // Wait for the subscriber to connect and complete the SP handshake.
    pub0.wait_for_subscribers(1).await.unwrap();

    let mut msg = Message::new();
    msg.push_back(b"hello from rust pub");
    pub0.publish(msg).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = sub_proc.kill();
    let _ = sub_proc.wait();
}

// ── Large payload (TCP fragmentation) ────────────────────────────────────────

/// Rust REQ → nngcat REP with a 512 KB payload.
///
/// Verifies that the framing layer handles TCP-fragmented messages correctly.
#[tokio::test]
async fn req_rust_large_payload_tcp() {
    require_nngcat!();
    let url = "tcp://127.0.0.1:15582";

    let mut server = std::process::Command::new("nngcat")
        .args(["--rep0", "--listen", url, "--data", "ack", "-Z", "1048576"])
        .spawn()
        .expect("failed to spawn nngcat");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut req = reqrep0::Req0::dial(url).await.unwrap();
    let mut msg = Message::new();
    msg.push_back(&vec![0xABu8; 512 * 1024]); // 512 KB
    let reply = req.request(msg).await.unwrap();
    assert_eq!(reply.body(), b"ack");

    let _ = server.kill();
    let _ = server.wait();
}
