//! PAIR0 / PAIR1 protocol state machines.
//!
//! PAIR0 is stateless (no protocol headers).
//!
//! PAIR1 adds a TTL (time-to-live / hop count) header to prevent routing
//! loops in device-forwarding topologies.  The TTL is a 4-byte field at the
//! front of the message header; the high bit marks the "end of path".
//!
//! Wire format for PAIR1:  `[4-byte TTL field][body]`
//! (header is prepended, just like REQ/REP uses its header for the req ID)

use crate::Message;
use crate::codec::ProtocolId;

pub const PROTOCOL_ID_PAIR0: ProtocolId = ProtocolId::PAIR0;
pub const PROTOCOL_ID_PAIR1: ProtocolId = ProtocolId::PAIR1;

// ── PAIR0 ──

/// State machine for PAIR0.  Stateless.
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

// ── PAIR1 ──

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PairError {
    /// TTL reached zero — discard this message to prevent routing loops.
    TtlExpired,
    /// Message header too short to contain TTL field.
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

/// State machine for PAIR1.  Tracks the maximum allowed TTL.
pub struct Pair1State {
    max_ttl: u8,
}

impl Pair1State {
    /// Valid TTL range is 1–15 (NNG default is 8).
    pub fn new(max_ttl: u8) -> Self {
        assert!((1..=15).contains(&max_ttl), "max_ttl must be 1..=15");
        Self { max_ttl }
    }

    /// Attach a fresh TTL header to an outgoing message.
    pub fn attach_ttl(&self, msg: &mut Message) {
        // High bit (0x80000000) marks "end of path" per NNG PAIR1 spec.
        let ttl_field: u32 = 0x8000_0000 | u32::from(self.max_ttl);
        msg.header_push_back(&ttl_field.to_be_bytes());
    }

    /// Process an incoming message: strip the 4-byte TTL field from the front
    /// of the body and validate it.  Returns the remaining TTL value (> 0) or
    /// `Err(TtlExpired)` if the message should be discarded.  On success `msg`
    /// holds only the application payload.
    pub fn process_incoming(&self, msg: &mut Message) -> Result<u8, PairError> {
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
