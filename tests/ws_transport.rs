/// WebSocket transport tests (require `--features ws`).
///
/// Three layers of coverage:
///   1. `WsTransport` loopback — raw transport, no SP state machine
///   2. Socket API integration — major protocol pairs over ws://
///   3. `websocat` interop — skipped if websocat is not in PATH

// ── helpers ───────────────────────────────────────────────────────────────────

/// Bind to port 0 and return a `ws://` URL with the assigned port.
#[cfg(feature = "ws")]
async fn free_ws_addr() -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    format!("ws://127.0.0.1:{port}")
}

// ── 1. WsTransport loopback ───────────────────────────────────────────────────

#[cfg(feature = "ws")]
mod raw_transport {
    use nng_core::{Message, codec::ProtocolId, transport::ws::WsTransport};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn echo_single_message() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let addr = format!("ws://127.0.0.1:{port}");

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut t = WsTransport::accept(tcp, ProtocolId::REP0).await.unwrap();
            let msg = t.recv().await.unwrap();
            t.send(&msg).await.unwrap();
        });

        let mut client = WsTransport::connect(&addr, ProtocolId::REQ0).await.unwrap();
        let mut req = Message::new();
        req.push_back(b"hello websocket");
        client.send(&req).await.unwrap();
        let reply = client.recv().await.unwrap();
        assert_eq!(reply.body(), b"hello websocket");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn echo_empty_message() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let addr = format!("ws://127.0.0.1:{port}");

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut t = WsTransport::accept(tcp, ProtocolId::REP0).await.unwrap();
            let msg = t.recv().await.unwrap();
            t.send(&msg).await.unwrap();
        });

        let mut client = WsTransport::connect(&addr, ProtocolId::REQ0).await.unwrap();
        client.send(&Message::new()).await.unwrap();
        let reply = client.recv().await.unwrap();
        assert!(reply.body().is_empty());

        server.await.unwrap();
    }

    #[tokio::test]
    async fn echo_large_message() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let addr = format!("ws://127.0.0.1:{port}");

        let data: Vec<u8> = (0u8..=255).cycle().take(64_000).collect();
        let data_clone = data.clone();

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut t = WsTransport::accept(tcp, ProtocolId::REP0).await.unwrap();
            let msg = t.recv().await.unwrap();
            t.send(&msg).await.unwrap();
        });

        let mut client = WsTransport::connect(&addr, ProtocolId::REQ0).await.unwrap();
        let mut req = Message::new();
        req.push_back(&data);
        client.send(&req).await.unwrap();
        let reply = client.recv().await.unwrap();
        assert_eq!(reply.body(), data_clone.as_slice());

        server.await.unwrap();
    }

    #[tokio::test]
    async fn subprotocol_mismatch_rejected() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let addr = format!("ws://127.0.0.1:{port}");

        // Server expects REP0; client is SUB0 (whose peer is PUB0) — mismatch.
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let result = WsTransport::accept(tcp, ProtocolId::REP0).await;
            assert!(
                result.is_err(),
                "server should reject mismatched subprotocol"
            );
        });

        let client_result = WsTransport::connect(&addr, ProtocolId::SUB0).await;
        assert!(
            client_result.is_err(),
            "client should fail on subprotocol mismatch"
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn many_messages_sequential() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let addr = format!("ws://127.0.0.1:{port}");

        const N: u32 = 50;
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut t = WsTransport::accept(tcp, ProtocolId::REP0).await.unwrap();
            for _ in 0..N {
                let msg = t.recv().await.unwrap();
                t.send(&msg).await.unwrap();
            }
        });

        let mut client = WsTransport::connect(&addr, ProtocolId::REQ0).await.unwrap();
        for i in 0u32..N {
            let mut req = Message::new();
            req.push_back(&i.to_be_bytes());
            client.send(&req).await.unwrap();
            let reply = client.recv().await.unwrap();
            assert_eq!(reply.body(), &i.to_be_bytes());
        }
        server.await.unwrap();
    }
}

// ── 2. Socket API integration over ws:// ─────────────────────────────────────

#[cfg(feature = "ws")]
mod socket_api {
    use super::free_ws_addr;
    use nng_core::{
        Message,
        socket::{pair0, pipeline0, pubsub0, reqrep0},
    };
    use std::time::Duration;

    #[tokio::test]
    async fn req_rep_ws() {
        let addr = free_ws_addr().await;
        let server_addr = addr.clone();

        let server = tokio::spawn(async move {
            let mut rep = reqrep0::Rep0::listen(&server_addr).await.unwrap();
            let (msg, responder) = rep.receive().await.unwrap();
            assert_eq!(msg.body(), b"ping");
            let mut reply = Message::new();
            reply.push_back(b"pong");
            responder.reply(reply).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut req = reqrep0::Req0::dial(&addr).await.unwrap();
        let mut request = Message::new();
        request.push_back(b"ping");
        let reply = req.request(request).await.unwrap();
        assert_eq!(reply.body(), b"pong");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn req_rep_ws_multiple_roundtrips() {
        let addr = free_ws_addr().await;
        let server_addr = addr.clone();

        let server = tokio::spawn(async move {
            let mut rep = reqrep0::Rep0::listen(&server_addr).await.unwrap();
            for i in 0u32..20 {
                let (msg, responder) = rep.receive().await.unwrap();
                assert_eq!(msg.body(), &i.to_be_bytes());
                let mut reply = Message::new();
                reply.push_back(&(i * 2).to_be_bytes());
                responder.reply(reply).await.unwrap();
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut req = reqrep0::Req0::dial(&addr).await.unwrap();
        for i in 0u32..20 {
            let mut request = Message::new();
            request.push_back(&i.to_be_bytes());
            let reply = req.request(request).await.unwrap();
            assert_eq!(reply.body(), &(i * 2).to_be_bytes());
        }

        server.await.unwrap();
    }

    #[tokio::test]
    async fn push_pull_ws() {
        let addr = free_ws_addr().await;
        let server_addr = addr.clone();

        let server = tokio::spawn(async move {
            let mut pull = pipeline0::Pull0::listen(&server_addr).await.unwrap();
            for i in 0u32..10 {
                let msg = pull.pull().await.unwrap();
                assert_eq!(msg.body(), &i.to_be_bytes());
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut push = pipeline0::Push0::dial(&addr).await.unwrap();
        for i in 0u32..10 {
            let mut msg = Message::new();
            msg.push_back(&i.to_be_bytes());
            push.push(msg).await.unwrap();
        }

        server.await.unwrap();
    }

    #[tokio::test]
    async fn pair_ws_bidirectional() {
        let addr = free_ws_addr().await;
        let server_addr = addr.clone();

        let server = tokio::spawn(async move {
            let mut pair = pair0::Pair0::listen(&server_addr).await.unwrap();
            for i in 0u32..10 {
                let msg = pair.recv().await.unwrap();
                assert_eq!(msg.body(), &i.to_be_bytes());
                let mut reply = Message::new();
                reply.push_back(&(i + 100).to_be_bytes());
                pair.send(reply).await.unwrap();
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut pair = pair0::Pair0::dial(&addr).await.unwrap();
        for i in 0u32..10 {
            let mut msg = Message::new();
            msg.push_back(&i.to_be_bytes());
            pair.send(msg).await.unwrap();
            let reply = pair.recv().await.unwrap();
            assert_eq!(reply.body(), &(i + 100).to_be_bytes());
        }

        server.await.unwrap();
    }

    #[tokio::test]
    async fn pub_sub_ws() {
        let addr = free_ws_addr().await;
        let server_addr = addr.clone();

        // Spawn publisher first so it can bind; then dial the subscriber.
        let server = tokio::spawn(async move {
            let mut pub0 = pubsub0::Pub0::listen(&server_addr).await.unwrap();
            pub0.wait_for_subscribers(1).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            for i in 0u8..5 {
                let mut msg = Message::new();
                msg.push_back(&[b'A', i]);
                pub0.publish(msg).await.unwrap();
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut sub = pubsub0::Sub0::dial(&addr).await.unwrap();
        sub.subscribe_to(b"A");

        let mut received = Vec::new();
        for _ in 0..5 {
            match tokio::time::timeout(Duration::from_millis(500), sub.next()).await {
                Ok(Ok(msg)) => received.push(msg.body()[1]),
                _ => break,
            }
        }
        assert_eq!(received, vec![0, 1, 2, 3, 4]);

        server.await.unwrap();
    }
}

// ── 3. websocat interop ───────────────────────────────────────────────────────

#[cfg(feature = "ws")]
mod websocat_interop {
    use nng_core::{Message, codec::ProtocolId, transport::ws::WsTransport};
    use std::process::Stdio;
    use tokio::net::TcpListener;

    fn websocat_bin() -> Option<String> {
        // Allow override; fall back to PATH.
        if let Ok(p) = std::env::var("WEBSOCAT") {
            return Some(p);
        }
        // Check PATH without pulling in the `which` crate.
        std::env::var("PATH").ok().and_then(|path| {
            path.split(':').find_map(|dir| {
                let candidate = format!("{dir}/websocat");
                std::fs::metadata(&candidate).ok().map(|_| candidate)
            })
        })
    }

    /// nng-core REQ0 client sends to websocat acting as a REP server.
    ///
    /// websocat command:
    ///   websocat --protocol rep.sp.nanomsg.org -b -E ws-listen:127.0.0.1:PORT -
    ///
    /// websocat echoes stdin to the connected client, so we send one message
    /// and verify we receive it back.
    #[tokio::test]
    async fn nng_req_websocat_rep_echo() {
        let ws_bin = match websocat_bin() {
            Some(p) => p,
            None => {
                eprintln!("SKIP nng_req_websocat_rep_echo: websocat not found");
                return;
            }
        };

        // Pick a free port for websocat to listen on.
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        drop(l);
        let addr = format!("ws://127.0.0.1:{port}");

        // websocat listens as REP0 and echoes stdin to clients.
        // We write the payload bytes to its stdin and read stdout.
        let mut child = tokio::process::Command::new(&ws_bin)
            .args([
                "--protocol",
                "rep.sp.nanomsg.org",
                "-b",
                "-E", // exit after one connection
                &format!("ws-listen:127.0.0.1:{port}"),
                "-", // stdio
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn websocat");

        // Give websocat a moment to bind.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Connect as nng-core REQ0 and send one message.
        let mut transport = WsTransport::connect(&addr, ProtocolId::REQ0).await.unwrap();
        let mut req = Message::new();
        req.push_back(b"hello from nng-core");
        transport.send(&req).await.unwrap();

        // Write the same payload to websocat's stdin so it echoes it back.
        use tokio::io::AsyncWriteExt;
        if let Some(stdin) = child.stdin.as_mut() {
            let _ = stdin.write_all(b"hello from nng-core\n").await;
        }

        // Read the echoed reply from the transport.
        let reply = tokio::time::timeout(std::time::Duration::from_secs(2), transport.recv()).await;
        assert!(reply.is_ok(), "timed out waiting for websocat echo");
        let reply = reply.unwrap().unwrap();
        // websocat echoes the stdin line including the newline terminator.
        let body = reply.body();
        let body = body.strip_suffix(b"\n").unwrap_or(body);
        assert_eq!(body, b"hello from nng-core");

        let _ = child.kill().await;
    }
}
