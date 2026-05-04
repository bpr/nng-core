use nng_pure::{
    Message,
    codec::ProtocolId,
    transport::loopback::inproc_pair,
};

#[tokio::test]
async fn loopback_req_rep_handshake_and_messages() {
    let (mut req, mut rep) =
        inproc_pair(ProtocolId::REQ0, ProtocolId::REP0).await.unwrap();

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
    let (mut a, mut b) =
        inproc_pair(ProtocolId::PAIR0, ProtocolId::PAIR0).await.unwrap();

    // Message with both header and body — on the wire they arrive as one body.
    let mut msg = Message::new();
    msg.header_push_back(&[0xDE, 0xAD]);
    msg.push_back(b"payload");
    a.send(&msg).await.unwrap();

    let received = b.recv().await.unwrap();
    // Wire puts header before body, all lands in received body.
    assert_eq!(received.body(), &[0xDE, 0xAD, b'p', b'a', b'y', b'l', b'o', b'a', b'd']);
}

#[tokio::test]
async fn loopback_empty_message() {
    let (mut a, mut b) =
        inproc_pair(ProtocolId::PUSH0, ProtocolId::PULL0).await.unwrap();

    a.send(&Message::new()).await.unwrap();
    let received = b.recv().await.unwrap();
    assert!(received.body().is_empty());
}

#[tokio::test]
async fn loopback_protocol_mismatch_is_error() {
    // Connecting REQ to PUB should fail with IncompatibleProtocol.
    use nng_pure::transport::{FramedTransport, loopback::TokioDuplex};

    let (a, b) = tokio::io::duplex(64 * 1024);
    let result = tokio::try_join!(
        FramedTransport::connect(TokioDuplex(a), ProtocolId::REQ0),
        FramedTransport::connect(TokioDuplex(b), ProtocolId::PUB0),
    );
    assert!(result.is_err());
}

#[tokio::test]
async fn loopback_large_message() {
    // Use a large duplex buffer so the 100KB message doesn't deadlock.
    use nng_pure::transport::{FramedTransport, loopback::TokioDuplex};
    let (a_stream, b_stream) = tokio::io::duplex(256 * 1024);
    let (mut a, mut b) = tokio::try_join!(
        FramedTransport::connect(TokioDuplex(a_stream), ProtocolId::PAIR0),
        FramedTransport::connect(TokioDuplex(b_stream), ProtocolId::PAIR0),
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
