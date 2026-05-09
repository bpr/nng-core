use nng_core::{
    Message,
    codec::{
        CodecError, ProtocolId, check_peer, decode_frame, decode_handshake, encode_frame,
        encode_handshake,
    },
};

// ── Handshake ──

#[test]
fn handshake_roundtrip_req0() {
    let buf = encode_handshake(ProtocolId::REQ0);
    let remote = decode_handshake(&buf).unwrap();
    assert_eq!(remote, ProtocolId::REQ0);
}

#[test]
fn handshake_all_protocols() {
    let protocols = [
        ProtocolId::PAIR0,
        ProtocolId::PAIR1,
        ProtocolId::PUB0,
        ProtocolId::SUB0,
        ProtocolId::REQ0,
        ProtocolId::REP0,
        ProtocolId::PUSH0,
        ProtocolId::PULL0,
        ProtocolId::SURVEYOR0,
        ProtocolId::RESPONDENT0,
        ProtocolId::BUS0,
    ];
    for proto in protocols {
        let buf = encode_handshake(proto);
        let decoded = decode_handshake(&buf).unwrap();
        assert_eq!(decoded, proto, "roundtrip failed for {:#x}", proto.0);
    }
}

#[test]
fn handshake_magic_bytes() {
    let buf = encode_handshake(ProtocolId::REP0);
    assert_eq!(&buf[..4], &[0x00, b'S', b'P', 0x00]);
    assert_eq!(&buf[6..], &[0x00, 0x00]);
    assert_eq!(u16::from_be_bytes([buf[4], buf[5]]), 0x31);
}

#[test]
fn handshake_invalid_magic() {
    let mut buf = encode_handshake(ProtocolId::REQ0);
    buf[1] = b'X';
    assert_eq!(decode_handshake(&buf), Err(CodecError::InvalidMagic));
}

#[test]
fn handshake_invalid_reserved() {
    let mut buf = encode_handshake(ProtocolId::REQ0);
    buf[6] = 0x01;
    assert_eq!(decode_handshake(&buf), Err(CodecError::ReservedNotZero));
}

#[test]
fn handshake_peer_check_compatible() {
    check_peer(ProtocolId::REQ0, ProtocolId::REP0).unwrap();
    check_peer(ProtocolId::REP0, ProtocolId::REQ0).unwrap();
    check_peer(ProtocolId::PUB0, ProtocolId::SUB0).unwrap();
    check_peer(ProtocolId::SUB0, ProtocolId::PUB0).unwrap();
    check_peer(ProtocolId::PUSH0, ProtocolId::PULL0).unwrap();
    check_peer(ProtocolId::PULL0, ProtocolId::PUSH0).unwrap();
    check_peer(ProtocolId::PAIR0, ProtocolId::PAIR0).unwrap();
    check_peer(ProtocolId::PAIR1, ProtocolId::PAIR1).unwrap();
    check_peer(ProtocolId::SURVEYOR0, ProtocolId::RESPONDENT0).unwrap();
    check_peer(ProtocolId::RESPONDENT0, ProtocolId::SURVEYOR0).unwrap();
    check_peer(ProtocolId::BUS0, ProtocolId::BUS0).unwrap();
}

#[test]
fn handshake_peer_check_incompatible() {
    let err = check_peer(ProtocolId::REQ0, ProtocolId::PUB0).unwrap_err();
    assert!(matches!(err, CodecError::IncompatibleProtocol { .. }));
}

// ── Frame encode / decode ──

#[test]
fn frame_empty_body_roundtrip() {
    let msg = Message::new();
    let encoded = encode_frame(&msg);
    assert_eq!(encoded.len(), 8); // just the length prefix
    assert_eq!(&encoded[..8], &[0, 0, 0, 0, 0, 0, 0, 0]);

    let (decoded, consumed) = decode_frame(&encoded).unwrap();
    assert_eq!(consumed, 8);
    assert!(decoded.body().is_empty());
}

#[test]
fn frame_body_roundtrip() {
    let mut msg = Message::new();
    msg.push_back(b"hello world");
    let encoded = encode_frame(&msg);

    assert_eq!(encoded.len(), 8 + 11);
    let (decoded, consumed) = decode_frame(&encoded).unwrap();
    assert_eq!(consumed, 19);
    assert_eq!(decoded.body(), b"hello world");
}

#[test]
fn frame_header_and_body_in_wire_payload() {
    // The wire payload includes header bytes prepended to body bytes.
    let mut msg = Message::new();
    msg.header_push_back(&[0xDE, 0xAD]);
    msg.push_back(b"data");
    let encoded = encode_frame(&msg);

    // length = 2 (header) + 4 (body) = 6
    assert_eq!(u64::from_be_bytes(encoded[..8].try_into().unwrap()), 6);
    assert_eq!(&encoded[8..10], &[0xDE, 0xAD]);
    assert_eq!(&encoded[10..], b"data");

    // decoded: all bytes go into body (protocol layer splits header later)
    let (decoded, consumed) = decode_frame(&encoded).unwrap();
    assert_eq!(consumed, 14);
    assert_eq!(decoded.body(), &[0xDE, 0xAD, b'd', b'a', b't', b'a']);
}

#[test]
fn frame_incomplete_length() {
    let encoded = [0x00u8, 0x00, 0x00]; // only 3 bytes, need 8
    assert_eq!(decode_frame(&encoded), Err(CodecError::Incomplete));
}

#[test]
fn frame_incomplete_payload() {
    let mut msg = Message::new();
    msg.push_back(b"hello world");
    let encoded = encode_frame(&msg);
    // Give only partial payload
    assert_eq!(decode_frame(&encoded[..10]), Err(CodecError::Incomplete));
}

#[test]
fn frame_multiple_frames_in_buffer() {
    let mut m1 = Message::new();
    m1.push_back(b"first");
    let mut m2 = Message::new();
    m2.push_back(b"second");

    let mut buf = encode_frame(&m1);
    buf.extend(encode_frame(&m2));

    let (d1, c1) = decode_frame(&buf).unwrap();
    assert_eq!(d1.body(), b"first");

    let (d2, c2) = decode_frame(&buf[c1..]).unwrap();
    assert_eq!(d2.body(), b"second");

    assert_eq!(c1 + c2, buf.len());
}
