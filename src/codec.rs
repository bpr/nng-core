//! SP (Scalability Protocol) wire codec: handshake and message framing.
//!
//! # Handshake
//!
//! Both sides send an 8-byte header immediately on connect, before any messages:
//!
//! ```text
//! byte:  0     1     2     3     4       5       6     7
//!       0x00  'S'   'P'  0x00  proto_hi proto_lo  0x00  0x00
//!                          └───── protocol ID, u16 big-endian ─────┘
//! ```
//!
//! `proto` is the *sender's own* protocol ID. Each side independently checks
//! that the remote's ID is the expected peer (e.g., REQ0 accepts only REP0).
//! This is a symmetric handshake: both sides validate, both sides may reject.
//!
//! # Message framing
//!
//! After the handshake, each message is framed as:
//!
//! ```text
//! [8-byte u64 BE length][payload bytes …]
//! ```
//!
//! where `length = header_len + body_len`. On receive, the entire payload lands
//! in the message **body**. The protocol state machine is then responsible for
//! stripping its own header fields off the front of the body. This design means
//! the transport layer needs no knowledge of protocol-specific header formats.

use crate::Message;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

// ── Protocol IDs ──────────────────────────────────────────────────────────────
//
// Values are computed by NNG's NNI_PROTO(major, minor) macro:
//   NNI_PROTO(m, n) = (m << 4) | n    (i.e., major * 16 + minor)
//
// Confirmed against nng/include/nng/nng.h in the NNG source.

/// SP protocol identifier, sent in bytes 4–5 of the handshake.
///
/// The numeric value encodes the protocol family (high nibble) and version
/// (low nibble) using NNG's `NNI_PROTO(major, minor)` convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolId(pub u16);

impl ProtocolId {
    pub const PAIR0: Self = Self(0x10); // NNI_PROTO(1,0)
    pub const PAIR1: Self = Self(0x11); // NNI_PROTO(1,1)
    pub const PUB0: Self = Self(0x20); // NNI_PROTO(2,0)
    pub const SUB0: Self = Self(0x21); // NNI_PROTO(2,1)
    pub const REQ0: Self = Self(0x30); // NNI_PROTO(3,0)
    pub const REP0: Self = Self(0x31); // NNI_PROTO(3,1)
    pub const PUSH0: Self = Self(0x50); // NNI_PROTO(5,0)
    pub const PULL0: Self = Self(0x51); // NNI_PROTO(5,1)
    pub const SURVEYOR0: Self = Self(0x62); // NNI_PROTO(6,2)
    pub const RESPONDENT0: Self = Self(0x63); // NNI_PROTO(6,3)
    pub const BUS0: Self = Self(0x70); // NNI_PROTO(7,0)

    /// Return the protocol ID that the remote peer must present.
    ///
    /// Asymmetric pairs (REQ↔REP, PUB↔SUB, PUSH↔PULL, SURVEYOR↔RESPONDENT)
    /// require opposite IDs. Symmetric protocols (PAIR0, PAIR1, BUS0) accept
    /// the same ID on both sides.
    pub fn expected_peer(self) -> Self {
        match self {
            Self::REQ0 => Self::REP0,
            Self::REP0 => Self::REQ0,
            Self::PUB0 => Self::SUB0,
            Self::SUB0 => Self::PUB0,
            Self::PUSH0 => Self::PULL0,
            Self::PULL0 => Self::PUSH0,
            Self::PAIR0 => Self::PAIR0,
            Self::PAIR1 => Self::PAIR1,
            Self::SURVEYOR0 => Self::RESPONDENT0,
            Self::RESPONDENT0 => Self::SURVEYOR0,
            Self::BUS0 => Self::BUS0,
            _ => self, // unknown protocol — skip peer check
        }
    }

    /// Short name used in the `Sec-WebSocket-Protocol` header.
    ///
    /// This is the lowercase ASCII name NNG assigns to each protocol in its
    /// `proto_self` / `proto_peer` descriptors (e.g. `"req"`, `"rep"`).
    pub fn ws_protocol_name(self) -> &'static str {
        match self {
            Self::REQ0 => "req",
            Self::REP0 => "rep",
            Self::PUB0 => "pub",
            Self::SUB0 => "sub",
            Self::PUSH0 => "push",
            Self::PULL0 => "pull",
            Self::PAIR0 | Self::PAIR1 => "pair",
            Self::SURVEYOR0 => "surveyor",
            Self::RESPONDENT0 => "respondent",
            Self::BUS0 => "bus",
            _ => "unknown",
        }
    }

    /// Full `Sec-WebSocket-Protocol` header value for this protocol.
    ///
    /// Both the dialer and the listener use the *listener's* protocol name:
    /// - Listener sets: `self.ws_subprotocol()`
    /// - Dialer   sets: `self.expected_peer().ws_subprotocol()`
    ///
    /// Example: a REQ dialer calls `ProtocolId::REQ0.expected_peer().ws_subprotocol()`
    /// which yields `"rep.sp.nanomsg.org"`, matching what the REP listener expects.
    #[cfg(feature = "ws")]
    pub fn ws_subprotocol(self) -> String {
        format!("{}.sp.nanomsg.org", self.ws_protocol_name())
    }
}

// ── Handshake ─────────────────────────────────────────────────────────────────

const MAGIC: [u8; 4] = [0x00, b'S', b'P', 0x00];

/// Encode the 8-byte SP handshake header for `local`.
///
/// The caller should write the returned bytes to the wire immediately on
/// connect, before sending any messages.
pub fn encode_handshake(local: ProtocolId) -> [u8; 8] {
    let [hi, lo] = local.0.to_be_bytes();
    [MAGIC[0], MAGIC[1], MAGIC[2], MAGIC[3], hi, lo, 0, 0]
}

/// Validate an 8-byte SP handshake buffer and return the remote's protocol ID.
///
/// Returns `Err` if the magic bytes are wrong or the reserved bytes are
/// non-zero. Use [`check_peer`] afterwards to verify protocol compatibility.
pub fn decode_handshake(buf: &[u8; 8]) -> Result<ProtocolId, CodecError> {
    if buf[0] != MAGIC[0] || buf[1] != MAGIC[1] || buf[2] != MAGIC[2] || buf[3] != MAGIC[3] {
        return Err(CodecError::InvalidMagic);
    }
    if buf[6] != 0 || buf[7] != 0 {
        return Err(CodecError::ReservedNotZero);
    }
    let remote = ProtocolId(u16::from_be_bytes([buf[4], buf[5]]));
    Ok(remote)
}

/// Verify that `remote` is the expected peer for `local`.
///
/// Called after [`decode_handshake`] to enforce protocol compatibility. For
/// example, a REQ0 socket must connect to a REP0 socket; any other remote
/// protocol ID yields [`CodecError::IncompatibleProtocol`].
pub fn check_peer(local: ProtocolId, remote: ProtocolId) -> Result<(), CodecError> {
    let expected = local.expected_peer();
    if remote != expected {
        Err(CodecError::IncompatibleProtocol { local, remote })
    } else {
        Ok(())
    }
}

// ── Frame encode ──────────────────────────────────────────────────────────────

/// Encode a message as a complete SP frame into a `Vec<u8>`.
///
/// Wire layout: `[8-byte u64 BE length][header bytes][body bytes]`.
/// The length field covers both header and body — the receiver allocates that
/// many bytes without knowing the internal split.
pub fn encode_frame(msg: &Message) -> Vec<u8> {
    let header = msg.header();
    let body = msg.body();
    let total = header.len() + body.len();

    let mut out = Vec::with_capacity(8 + total);
    out.extend_from_slice(&(total as u64).to_be_bytes());
    out.extend_from_slice(header);
    out.extend_from_slice(body);
    out
}

// ── Frame decode ──────────────────────────────────────────────────────────────

/// Attempt to decode a single SP frame from `src`.
///
/// Returns `Ok((msg, bytes_consumed))` on success. The entire wire payload
/// (header + body combined) is placed into `msg.body()` — the protocol state
/// machine will strip its own header fields from the front later.
///
/// Returns `Err(`[`CodecError::Incomplete`]`)` if `src` does not yet contain a
/// full frame (the caller should buffer and retry).
pub fn decode_frame(src: &[u8]) -> Result<(Message, usize), CodecError> {
    if src.len() < 8 {
        return Err(CodecError::Incomplete);
    }
    let len = u64::from_be_bytes(src[..8].try_into().unwrap()) as usize;
    // Use subtraction, not addition, to avoid overflow when len is near usize::MAX.
    if src.len() - 8 < len {
        return Err(CodecError::Incomplete);
    }
    let mut msg = Message::new();
    msg.push_back(&src[8..8 + len]);
    Ok((msg, 8 + len))
}

// ── Errors ────────────────────────────────────────────────────────────────────

/// Errors that can occur during SP handshake or frame decoding.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// The first four bytes were not `\0SP\0`.
    #[error("invalid SP magic bytes")]
    InvalidMagic,
    /// Handshake bytes 6–7 (reserved) were non-zero.
    #[error("non-zero reserved bytes in SP handshake")]
    ReservedNotZero,
    /// Remote's protocol ID does not match the expected peer for our protocol.
    #[error(
        "incompatible SP protocols: local={:#x} expects peer={:#x}, got {:#x}",
        local.0, local.expected_peer().0, remote.0
    )]
    IncompatibleProtocol {
        local: ProtocolId,
        remote: ProtocolId,
    },
    /// Not enough bytes to complete decoding; caller should buffer and retry.
    #[error("incomplete SP frame (more bytes needed)")]
    Incomplete,
}

// ── Kani proof harnesses ──────────────────────────────────────────────────────

#[cfg(kani)]
mod kani_proofs {
    //! Kani proof harnesses for the SP codec.
    //!
    //! Each `#[kani::proof]` function is a bounded model-checking harness.
    //! `kani::any::<T>()` produces a symbolic value that stands for *every*
    //! possible value of type `T` simultaneously; the solver explores all
    //! paths through the code for all such inputs and reports any path that
    //! reaches a panic, assertion failure, or memory-safety violation.
    //! `kani::assume(cond)` constrains the symbolic inputs (equivalent to a
    //! precondition).
    //!
    //! `#[kani::unwind(N)]` sets the loop-unroll bound.  N must be at least
    //! `max_iterations + 1`; each harness documents which constant drives N.

    use super::*;

    /// decode_handshake must never panic on any 8-byte input — only Ok or Err.
    #[kani::proof]
    fn decode_handshake_never_panics() {
        let bytes: [u8; 8] = kani::any();
        let _ = decode_handshake(&bytes);
    }

    /// encode_handshake always produces bytes that decode_handshake accepts,
    /// and the decoded ProtocolId equals the original.
    #[kani::proof]
    fn encode_decode_handshake_roundtrip() {
        let proto = ProtocolId(kani::any::<u16>());
        let wire = encode_handshake(proto);
        // encode_handshake always emits valid magic and zero reserved bytes,
        // so decode must always succeed.
        let decoded = decode_handshake(&wire).unwrap();
        assert_eq!(decoded, proto);
    }

    /// decode_frame must never panic on any input up to 16 bytes.
    /// Unwind bound 17 = MAX (16) + 1, covering the byte-copy loop inside
    /// push_back when the full 8-byte payload (16 − 8 header bytes) is copied.
    #[kani::proof]
    #[kani::unwind(17)]
    fn decode_frame_never_panics() {
        const MAX: usize = 16;
        let data: [u8; MAX] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= MAX);
        let _ = decode_frame(&data[..len]);
    }

    /// Fuzz regression: any non-zero declared length with no payload bytes must
    /// return Incomplete, proving the integer-overflow fix is correct for all
    /// possible u64 length values including usize::MAX.
    #[kani::proof]
    fn decode_frame_oversized_length_is_incomplete() {
        let len_val: u64 = kani::any();
        kani::assume(len_val > 0);
        let src = len_val.to_be_bytes(); // 8-byte header only, no payload
        let result = decode_frame(&src);
        assert!(matches!(result, Err(CodecError::Incomplete)));
    }
}
