use nng_core::{
    Message,
    codec::{CodecError, ProtocolId, decode_frame, decode_handshake, encode_frame, encode_handshake},
};
use proptest::prelude::*;

proptest! {
    /// encode_handshake followed by decode_handshake is the identity for any u16 protocol ID.
    #[test]
    fn handshake_roundtrip_any_id(id in any::<u16>()) {
        let buf = encode_handshake(ProtocolId(id));
        prop_assert_eq!(decode_handshake(&buf), Ok(ProtocolId(id)));
    }

    /// The length field, decoded body, and consumed byte count are all consistent
    /// for arbitrary header and body byte sequences.
    #[test]
    fn frame_roundtrip(
        header in prop::collection::vec(any::<u8>(), 0..256),
        body   in prop::collection::vec(any::<u8>(), 0..256),
    ) {
        let mut msg = Message::new();
        msg.header_push_back(&header);
        msg.push_back(&body);

        let encoded = encode_frame(&msg);

        let wire_len = u64::from_be_bytes(encoded[..8].try_into().unwrap()) as usize;
        prop_assert_eq!(wire_len, header.len() + body.len());

        let (decoded, consumed) = decode_frame(&encoded).unwrap();
        prop_assert_eq!(consumed, 8 + wire_len);

        // decode_frame places header || body into decoded.body(); header is empty.
        let mut expected = header.clone();
        expected.extend_from_slice(&body);
        prop_assert_eq!(decoded.body(), expected.as_slice());
        prop_assert!(decoded.header().is_empty());
    }

    /// Supplying fewer than 8 bytes always returns Incomplete regardless of payload.
    #[test]
    fn frame_incomplete_on_prefix_truncation(
        header in prop::collection::vec(any::<u8>(), 0..64),
        body   in prop::collection::vec(any::<u8>(), 0..64),
        trunc  in 0usize..8,
    ) {
        let mut msg = Message::new();
        msg.header_push_back(&header);
        msg.push_back(&body);
        let encoded = encode_frame(&msg);
        prop_assert_eq!(decode_frame(&encoded[..trunc]), Err(CodecError::Incomplete));
    }

    /// Supplying the 8-byte length prefix but one fewer payload byte than promised
    /// returns Incomplete.
    #[test]
    fn frame_incomplete_on_payload_truncation(
        header in prop::collection::vec(any::<u8>(), 0..64),
        body   in prop::collection::vec(any::<u8>(), 0..64),
    ) {
        prop_assume!(header.len() + body.len() > 0);

        let mut msg = Message::new();
        msg.header_push_back(&header);
        msg.push_back(&body);
        let encoded = encode_frame(&msg);

        prop_assert_eq!(
            decode_frame(&encoded[..encoded.len() - 1]),
            Err(CodecError::Incomplete),
        );
    }

    /// Multiple encoded frames concatenated in a buffer can be decoded sequentially
    /// using the consumed offsets returned by decode_frame.
    #[test]
    fn multi_frame_sequential_decode(
        payloads in prop::collection::vec(
            prop::collection::vec(any::<u8>(), 0..64),
            1..8,
        )
    ) {
        let mut buf = Vec::new();
        for payload in &payloads {
            let mut msg = Message::new();
            msg.push_back(payload);
            buf.extend(encode_frame(&msg));
        }

        let mut offset = 0;
        for payload in &payloads {
            let (decoded, consumed) = decode_frame(&buf[offset..]).unwrap();
            prop_assert_eq!(decoded.body(), payload.as_slice());
            offset += consumed;
        }
        prop_assert_eq!(offset, buf.len());
    }
}
