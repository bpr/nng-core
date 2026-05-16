use nng_core::{
    Message,
    codec::ProtocolId,
    transport::{FrameFormat, loopback::inproc_pair},
};

// ── ipc2:// tests (unix only) ─────────────────────────────────────────────────

#[cfg(unix)]
mod ipc2 {
    use nng_core::{
        Message,
        socket::{pair0, reqrep0},
    };
    use std::time::Duration;

    #[tokio::test]
    async fn ipc2_loopback_pair() {
        let url = "ipc2:///tmp/nng_core_ipc2_pair.sock";

        let listen_task = tokio::spawn(async move { pair0::Pair0::listen(url).await });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut client = pair0::Pair0::dial(url).await.unwrap();
        let mut server = listen_task.await.unwrap().unwrap();

        let mut msg = Message::new();
        msg.push_back(b"hello ipc2");
        client.send(msg).await.unwrap();

        let received = server.recv().await.unwrap();
        assert_eq!(received.body(), b"hello ipc2");

        let mut reply = Message::new();
        reply.push_back(b"world");
        server.send(reply).await.unwrap();

        let got = client.recv().await.unwrap();
        assert_eq!(got.body(), b"world");
    }

    #[tokio::test]
    async fn ipc2_loopback_req_rep() {
        let url = "ipc2:///tmp/nng_core_ipc2_reqrep.sock";
        let url_owned = url.to_owned();

        let listen_task = tokio::spawn(async move { reqrep0::Rep0::listen(&url_owned).await });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut req = reqrep0::Req0::dial(url).await.unwrap();
        let mut rep = listen_task.await.unwrap().unwrap();

        // Run req and rep concurrently: rep echoes the request body back.
        let (response, _) = tokio::join!(
            async {
                let mut msg = Message::new();
                msg.push_back(b"ping");
                req.request(msg).await.unwrap()
            },
            async {
                let (msg, responder) = rep.receive().await.unwrap();
                assert_eq!(msg.body(), b"ping");
                responder.reply(msg).await.unwrap();
            },
        );
        assert_eq!(response.body(), b"ping");
    }

    // ipc:// (9-byte framing) and ipc2:// (8-byte framing) speak different
    // frame formats.  The SP handshake succeeds (same 8 bytes), but the first
    // message causes the listener to see a type byte of 0x00 instead of 0x01
    // and return a BadFrameType error immediately.
    #[tokio::test]
    async fn ipc2_mismatch_with_ipc_errors() {
        let path = "/tmp/nng_core_ipc_mismatch.sock";

        let listen_task =
            tokio::spawn(async move { pair0::Pair0::listen(&format!("ipc://{path}")).await });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut client = pair0::Pair0::dial(&format!("ipc2://{path}")).await.unwrap();
        let mut server = listen_task.await.unwrap().unwrap();

        let mut msg = Message::new();
        msg.push_back(b"hello");
        client.send(msg).await.unwrap();

        // The server reads with 9-byte framing: the first byte of the 8-byte
        // TCP-format length header is 0x00, not 0x01, so recv returns
        // BadFrameType immediately rather than timing out.
        let result = server.recv().await;
        assert!(
            result.is_err(),
            "expected BadFrameType error due to frame format mismatch"
        );
    }
}

#[tokio::test]
async fn loopback_req_rep_handshake_and_messages() {
    let (mut req, mut rep) = inproc_pair(ProtocolId::REQ0, ProtocolId::REP0)
        .await
        .unwrap();

    for i in 0u32..5 {
        // REQ sends
        let mut msg = Message::new();
        msg.push_back(&i.to_be_bytes());
        req.send(&msg).await.unwrap();

        // REP receives
        let received = rep.recv().await.unwrap();
        assert_eq!(received.body(), &i.to_be_bytes());

        // REP sends reply
        let mut reply = Message::new();
        reply.push_back(b"ok");
        rep.send(&reply).await.unwrap();

        // REQ receives reply
        let got = req.recv().await.unwrap();
        assert_eq!(got.body(), b"ok");
    }
}

#[tokio::test]
async fn loopback_message_with_header() {
    let (mut a, mut b) = inproc_pair(ProtocolId::PAIR0, ProtocolId::PAIR0)
        .await
        .unwrap();

    // Message with both header and body — on the wire they arrive as one body.
    let mut msg = Message::new();
    msg.header_push_back(&[0xDE, 0xAD]);
    msg.push_back(b"payload");
    a.send(&msg).await.unwrap();

    let received = b.recv().await.unwrap();
    // Wire puts header before body, all lands in received body.
    assert_eq!(
        received.body(),
        &[0xDE, 0xAD, b'p', b'a', b'y', b'l', b'o', b'a', b'd']
    );
}

#[tokio::test]
async fn loopback_empty_message() {
    let (mut a, mut b) = inproc_pair(ProtocolId::PUSH0, ProtocolId::PULL0)
        .await
        .unwrap();

    a.send(&Message::new()).await.unwrap();
    let received = b.recv().await.unwrap();
    assert!(received.body().is_empty());
}

#[tokio::test]
async fn loopback_protocol_mismatch_is_error() {
    // Connecting REQ to PUB should fail with IncompatibleProtocol.
    use nng_core::transport::{FramedTransport, loopback::TokioDuplex};

    let (a, b) = tokio::io::duplex(64 * 1024);
    let result = tokio::try_join!(
        FramedTransport::connect(TokioDuplex(a), ProtocolId::REQ0, FrameFormat::Tcp),
        FramedTransport::connect(TokioDuplex(b), ProtocolId::PUB0, FrameFormat::Tcp),
    );
    assert!(result.is_err());
}

#[tokio::test]
async fn loopback_large_message() {
    // Use a large duplex buffer so the 100KB message doesn't deadlock.
    use nng_core::transport::{FramedTransport, loopback::TokioDuplex};
    let (a_stream, b_stream) = tokio::io::duplex(256 * 1024);
    let (mut a, mut b) = tokio::try_join!(
        FramedTransport::connect(TokioDuplex(a_stream), ProtocolId::PAIR0, FrameFormat::Tcp),
        FramedTransport::connect(TokioDuplex(b_stream), ProtocolId::PAIR0, FrameFormat::Tcp),
    )
    .unwrap();

    let data: Vec<u8> = (0u8..=255u8).cycle().take(100_000).collect();
    let mut msg = Message::new();
    msg.push_back(&data);
    // Run sender and receiver concurrently to avoid deadlock.
    let (send_result, received) = tokio::join!(a.send(&msg), b.recv());
    send_result.unwrap();
    assert_eq!(received.unwrap().body(), data.as_slice());
}
