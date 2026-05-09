//! Property-based tests for the SP codec.
//!
//! Covers encode/decode round-trips and the overflow regression found by
//! fuzzing (a length field of 0xffffffffffffffff must return Incomplete,
//! not panic).

use nng_core::{
    Message,
    codec::{
        CodecError, ProtocolId, decode_frame, decode_handshake, encode_frame, encode_handshake,
    },
};
use proptest::prelude::*;

proptest! {
    /// encode_frame + decode_frame is identity: the body survives a round-trip.
    #[test]
    fn codec_frame_roundtrip(body in proptest::collection::vec(any::<u8>(), 0..256)) {
        let mut msg = Message::new();
        msg.push_back(&body);
        let wire = encode_frame(&msg);
        let (decoded, consumed) = decode_frame(&wire).unwrap();
        prop_assert_eq!(decoded.body(), body.as_slice());
        prop_assert_eq!(consumed, wire.len());
    }

    /// Any input shorter than 8 bytes must return Incomplete, not panic.
    #[test]
    fn codec_frame_short_input_is_incomplete(
        input in proptest::collection::vec(any::<u8>(), 0..8)
    ) {
        prop_assert_eq!(decode_frame(&input), Err(CodecError::Incomplete));
    }

    /// Fuzz regression: any 8-byte length header that claims more bytes than
    /// are present in the buffer must return Incomplete, not panic or overflow.
    /// Covers the crash found by fuzzing (input: 0xffffffffffffffff).
    #[test]
    fn codec_frame_oversized_length_is_incomplete(declared_len in 1u64..=u64::MAX) {
        // Provide only the 8-byte header, no payload bytes.
        // Any non-zero declared length therefore exceeds available data.
        let src = declared_len.to_be_bytes();
        prop_assert_eq!(decode_frame(&src), Err(CodecError::Incomplete));
    }

    /// encode_handshake + decode_handshake is identity for every protocol ID.
    #[test]
    fn codec_handshake_roundtrip(proto_val in any::<u16>()) {
        let proto = ProtocolId(proto_val);
        let wire = encode_handshake(proto);
        let decoded = decode_handshake(&wire).unwrap();
        prop_assert_eq!(decoded, proto);
    }

    /// decode_handshake must never panic on arbitrary 8-byte input — it should
    /// only return Ok or a CodecError variant.
    #[test]
    fn codec_handshake_no_panic(bytes in any::<[u8; 8]>()) {
        let _ = decode_handshake(&bytes);
    }
}
