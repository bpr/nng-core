/// UDP transport tests (require `--features udp`).
///
/// UDP has no SP handshake and no length prefix — each datagram is one SP
/// message.  The listener learns the peer's address from the first datagram it
/// receives, so the dialer always initiates.

#[cfg(feature = "udp")]
mod udp {
    use nng_core::{
        Message,
        socket::{pair0, pipeline0, reqrep0},
        transport::udp::UdpTransport,
    };
    use std::time::Duration;
    use tokio::net::UdpSocket;

    /// Bind to port 0 and return `(transport, udp://… URL)` without a TOCTOU gap.
    async fn bound_listener() -> (UdpTransport, String) {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = socket.local_addr().unwrap().port();
        let url = format!("udp://127.0.0.1:{port}");
        (UdpTransport::from_socket(socket), url)
    }

    /// Return a `udp://` URL for a socket-API test (listener is created internally).
    async fn free_udp_addr() -> String {
        let s = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = s.local_addr().unwrap().port();
        format!("udp://127.0.0.1:{port}")
    }

    // ── 1. Raw UdpTransport loopback ─────────────────────────────────────────

    #[tokio::test]
    async fn raw_echo_single_message() {
        let (mut server_t, server_url) = bound_listener().await;
        let dialer_addr = server_url.strip_prefix("udp://").unwrap().to_owned();

        let server = tokio::spawn(async move {
            let msg = server_t.recv().await.unwrap();
            server_t.send(&msg).await.unwrap();
        });

        let mut client = UdpTransport::connect(&dialer_addr).await.unwrap();
        let mut req = Message::new();
        req.push_back(b"hello UDP");
        client.send(&mut req).await.unwrap();
        let reply = client.recv().await.unwrap();
        assert_eq!(reply.body(), b"hello UDP");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn raw_echo_empty_message() {
        let (mut server_t, server_url) = bound_listener().await;
        let dialer_addr = server_url.strip_prefix("udp://").unwrap().to_owned();

        let server = tokio::spawn(async move {
            let msg = server_t.recv().await.unwrap();
            server_t.send(&msg).await.unwrap();
        });

        let mut client = UdpTransport::connect(&dialer_addr).await.unwrap();
        client.send(&mut Message::new()).await.unwrap();
        let reply = client.recv().await.unwrap();
        assert!(reply.body().is_empty());

        server.await.unwrap();
    }

    #[tokio::test]
    async fn raw_many_sequential_messages() {
        let (mut server_t, server_url) = bound_listener().await;
        let dialer_addr = server_url.strip_prefix("udp://").unwrap().to_owned();
        const N: u32 = 50;

        let server = tokio::spawn(async move {
            for _ in 0..N {
                let msg = server_t.recv().await.unwrap();
                server_t.send(&msg).await.unwrap();
            }
        });

        let mut client = UdpTransport::connect(&dialer_addr).await.unwrap();

        for i in 0u32..N {
            let mut req = Message::new();
            req.push_back(&i.to_be_bytes());
            client.send(&mut req).await.unwrap();
            let reply = client.recv().await.unwrap();
            assert_eq!(reply.body(), &i.to_be_bytes());
        }

        server.await.unwrap();
    }

    // ── 2. Socket API over udp:// ─────────────────────────────────────────────

    #[tokio::test]
    async fn push_pull_udp() {
        let addr = free_udp_addr().await;
        let server_addr = addr.clone();
        const N: u32 = 20;

        let server = tokio::spawn(async move {
            let mut pull = pipeline0::Pull0::listen(&server_addr).await.unwrap();
            for i in 0..N {
                let msg = pull.pull().await.unwrap();
                assert_eq!(msg.body(), &i.to_be_bytes());
            }
        });

        // Give the listener time to bind.
        tokio::time::sleep(Duration::from_millis(5)).await;

        let mut push = pipeline0::Push0::dial(&addr).await.unwrap();
        for i in 0..N {
            let mut msg = Message::new();
            msg.push_back(&i.to_be_bytes());
            push.push(msg).await.unwrap();
        }

        server.await.unwrap();
    }

    #[tokio::test]
    async fn pair_udp_bidirectional() {
        let addr = free_udp_addr().await;
        let server_addr = addr.clone();

        // The listener must recv before send (to learn the peer address).
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

        tokio::time::sleep(Duration::from_millis(5)).await;

        let mut client = pair0::Pair0::dial(&addr).await.unwrap();
        for i in 0u32..10 {
            let mut msg = Message::new();
            msg.push_back(&i.to_be_bytes());
            client.send(msg).await.unwrap();
            let reply = client.recv().await.unwrap();
            assert_eq!(reply.body(), &(i + 100).to_be_bytes());
        }

        server.await.unwrap();
    }

    #[tokio::test]
    async fn req_rep_udp() {
        let addr = free_udp_addr().await;
        let server_addr = addr.clone();

        let server = tokio::spawn(async move {
            let mut rep = reqrep0::Rep0::listen(&server_addr).await.unwrap();
            let (msg, responder) = rep.receive().await.unwrap();
            assert_eq!(msg.body(), b"ping");
            let mut reply = Message::new();
            reply.push_back(b"pong");
            responder.reply(reply).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(5)).await;

        let mut req = reqrep0::Req0::dial(&addr).await.unwrap();
        let mut msg = Message::new();
        msg.push_back(b"ping");
        let reply = req.request(msg).await.unwrap();
        assert_eq!(reply.body(), b"pong");

        server.await.unwrap();
    }
}
