//! SP (Scalability Protocol) wire codec: handshake and message framing.
//!
//! ## Handshake (8 bytes, sent by both sides immediately on connect)
//! ```text
//! [0x00] ['S'] ['P'] [0x00] [proto_hi] [proto_lo] [0x00] [0x00]
//! ```
//! `proto` is the sender's own protocol ID as a big-endian `u16`.
//!
//! ## Message frame (per-message, after handshake)
//! ```text
//! [8-byte u64 BE length = header_len + body_len] [header bytes] [body bytes]
//! ```
//! The receiver allocates `length` bytes and places them all into the body.
//! The protocol state machine is then responsible for extracting its header
//! portion from the front of the body.

use crate::Message;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

// ── Protocol IDs (confirmed against NNG C source, NNI_PROTO(major, minor) = major*16+minor) ──

/// SP protocol identifier (the value sent in bytes 4..5 of the handshake).
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

    /// Return the protocol ID expected from the remote peer.
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
            _ => self, // unknown — no peer check
        }
    }
}

// ── Handshake ──

const MAGIC: [u8; 4] = [0x00, b'S', b'P', 0x00];

/// Encode the 8-byte SP handshake for a given local protocol.
pub fn encode_handshake(local: ProtocolId) -> [u8; 8] {
    let [hi, lo] = local.0.to_be_bytes();
    [MAGIC[0], MAGIC[1], MAGIC[2], MAGIC[3], hi, lo, 0, 0]
}

/// Validate an 8-byte SP handshake buffer and return the remote's protocol ID.
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

/// Verify that the remote's protocol ID is the expected peer for `local`.
pub fn check_peer(local: ProtocolId, remote: ProtocolId) -> Result<(), CodecError> {
    let expected = local.expected_peer();
    if remote != expected {
        Err(CodecError::IncompatibleProtocol { local, remote })
    } else {
        Ok(())
    }
}

// ── Frame encode ──

/// Encode a message as an SP frame into a `Vec<u8>`.
/// Wire layout: [8-byte u64 BE length][header bytes][body bytes].
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

// ── Frame decode ──

/// Attempt to decode a single SP frame from `src`.
///
/// Returns `Ok((Message, bytes_consumed))` on success, where the entire
/// wire payload (header + body bytes) is placed into the message **body**
/// (the protocol state machine is responsible for splitting out its header).
///
/// Returns `Err(CodecError::Incomplete)` if more data is needed.
pub fn decode_frame(src: &[u8]) -> Result<(Message, usize), CodecError> {
    if src.len() < 8 {
        return Err(CodecError::Incomplete);
    }
    let len = u64::from_be_bytes(src[..8].try_into().unwrap()) as usize;
    if src.len() < 8 + len {
        return Err(CodecError::Incomplete);
    }
    let mut msg = Message::new();
    msg.push_back(&src[8..8 + len]);
    Ok((msg, 8 + len))
}

// ── Errors ──

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// The first four magic bytes were not `\0SP\0`.
    InvalidMagic,
    /// Reserved bytes (6 and 7 of handshake) were non-zero.
    ReservedNotZero,
    /// Remote's protocol ID does not match what we expect.
    IncompatibleProtocol {
        local: ProtocolId,
        remote: ProtocolId,
    },
    /// Not enough bytes to complete decoding.
    Incomplete,
}

impl core::fmt::Display for CodecError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidMagic => write!(f, "invalid SP magic bytes"),
            Self::ReservedNotZero => write!(f, "non-zero reserved bytes in SP handshake"),
            Self::IncompatibleProtocol { local, remote } => write!(
                f,
                "incompatible SP protocols: local={:#x} expects peer={:#x}, got {:#x}",
                local.0,
                local.expected_peer().0,
                remote.0
            ),
            Self::Incomplete => write!(f, "incomplete SP frame (more bytes needed)"),
        }
    }
}
