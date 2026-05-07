//! PAIR0 / PAIR1 protocol state machines.
//!
//! # PAIR0
//!
//! A simple point-to-point channel with no protocol headers. Either peer can
//! send at any time. There is no request/reply discipline — PAIR0 is the
//! lowest-level SP protocol.
//!
//! # PAIR1 and the TTL hop count
//!
//! PAIR1 adds a 4-byte **TTL** (time-to-live) field that prevents routing
//! loops when messages are forwarded through NNG "device" nodes. Each device
//! that relays a message decrements the TTL; if it reaches zero the message
//! is discarded rather than forwarded again.
//!
//! Wire format for PAIR1 messages: `[4-byte TTL field][body bytes]`
//!
//! The TTL field layout (u32 big-endian):
//! ```text
//! bit 31: "end of path" marker — always set (0x8000_0000)
//! bits 4–0: TTL value (1–15)
//! bits 30–5: reserved / zero
//! ```
//!
//! The NNG default TTL is 8, which allows up to 8 device hops before a message
//! is dropped. Valid values are 1–15.

use crate::message::MessageBuf;
use crate::codec::ProtocolId;

pub const PROTOCOL_ID_PAIR0: ProtocolId = ProtocolId::PAIR0;
pub const PROTOCOL_ID_PAIR1: ProtocolId = ProtocolId::PAIR1;

// ── PAIR0 ─────────────────────────────────────────────────────────────────────

/// State machine for PAIR0. Stateless — no protocol headers are added.
pub struct Pair0State;

impl Pair0State {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Pair0State {
    fn default() -> Self {
        Self::new()
    }
}

// ── PAIR1 ─────────────────────────────────────────────────────────────────────

/// Errors produced by the PAIR1 state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PairError {
    /// The TTL field reached zero — discard this message to prevent a
    /// routing loop. A well-behaved device will never forward a zero-TTL
    /// message; the endpoint state machine rejects it defensively.
    TtlExpired,
    /// Message body was shorter than 4 bytes — too short to contain a TTL field.
    MessageTooShort,
}

impl core::fmt::Display for PairError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::TtlExpired => write!(f, "PAIR1 TTL expired"),
            Self::MessageTooShort => write!(f, "message too short to contain PAIR1 TTL"),
        }
    }
}

/// State machine for PAIR1. Tracks the maximum allowed TTL.
///
/// `max_ttl` is the starting hop count for outgoing messages; it is not used
/// to validate incoming TTL values directly, but a future device implementation
/// could compare the decremented value against it. Valid range is 1–15.
pub struct Pair1State {
    max_ttl: u8,
}

impl Pair1State {
    /// Construct with a specific maximum TTL. Panics if `max_ttl` is outside 1–15.
    pub fn new(max_ttl: u8) -> Self {
        assert!((1..=15).contains(&max_ttl), "max_ttl must be 1..=15");
        Self { max_ttl }
    }

    /// Attach a fresh TTL header to an outgoing message.
    ///
    /// Writes a 4-byte TTL field into the message header with:
    /// - Bits 4–0 set to `max_ttl`.
    /// - Bit 31 set to `1` ("end of path" marker per the PAIR1 spec).
    ///
    /// [`FramedTransport`] will transmit this header before the body, so the
    /// remote's [`process_incoming`](Self::process_incoming) will find it at
    /// the front of the received body.
    ///
    /// [`FramedTransport`]: crate::transport::FramedTransport
    pub fn attach_ttl<M: MessageBuf>(&self, msg: &mut M) {
        let ttl_field: u32 = 0x8000_0000 | u32::from(self.max_ttl);
        msg.header_push_back(&ttl_field.to_be_bytes());
    }

    /// Strip and validate the TTL field from the front of an incoming message.
    ///
    /// Reads the first 4 bytes as a big-endian u32, extracts the TTL from bits
    /// 4–0, and returns `Err(`[`PairError::TtlExpired`]`)` if the value is
    /// zero. On success the 4-byte field is consumed and `msg.body()` holds
    /// only the application payload.
    ///
    /// A device node would decrement the returned TTL before re-attaching it
    /// for forwarding; an endpoint simply discards the TTL value.
    pub fn process_incoming<M: MessageBuf>(&self, msg: &mut M) -> Result<u8, PairError> {
        if msg.body().len() < 4 {
            return Err(PairError::MessageTooShort);
        }
        let raw = u32::from_be_bytes(msg.body()[..4].try_into().unwrap());
        msg.trim_front(4);
        let ttl = (raw & 0x0F) as u8;
        if ttl == 0 {
            Err(PairError::TtlExpired)
        } else {
            Ok(ttl)
        }
    }
}

impl Default for Pair1State {
    fn default() -> Self {
        Self::new(8)
    }
}
