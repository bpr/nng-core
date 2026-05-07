//! REQ0 / REP0 protocol state machines.
//!
//! ## Wire encoding
//! Each request carries a 4-byte request ID (u32 big-endian) prepended to
//! the message body.  The ID is placed in the **header** when encoding for
//! the wire (so it precedes the user body), and pulled from the front of the
//! received body when decoding.
//!
//! REQ side:
//!   - send: clears header, appends `request_id` as u32 BE to header
//!   - recv: pops 4 bytes from front of body, validates ID matches sent ID
//!
//! REP side:
//!   - recv: pops 4 bytes from front of body, stores as routing header
//!   - send (reply): prepends saved routing header bytes to the reply header

use crate::Message;
use crate::codec::ProtocolId;

pub const PROTOCOL_ID_REQ: ProtocolId = ProtocolId::REQ0;
pub const PROTOCOL_ID_REP: ProtocolId = ProtocolId::REP0;

// ── Errors ──

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReqRepError {
    /// Received reply had a request ID that did not match the pending one.
    IdMismatch { got: u32, expected: u32 },
    /// Received message too short to contain a request ID.
    MessageTooShort,
}

impl core::fmt::Display for ReqRepError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::IdMismatch { got, expected } => {
                write!(f, "req/rep ID mismatch: got {got:#010x}, expected {expected:#010x}")
            }
            Self::MessageTooShort => write!(f, "message too short to contain request ID"),
        }
    }
}

// ── REQ0 state ──

/// State machine for the REQ0 (client) side of a request/reply pair.
pub struct Req0State {
    /// ID to use for the next outgoing request.  Incremented on each send.
    next_id: u32,
}

impl Req0State {
    pub fn new() -> Self {
        // Start at 1 — 0 is used as "no pending request" in NNG.
        Self { next_id: 1 }
    }

    /// Prepare an outgoing request: inject the request ID into the message
    /// header (so it becomes part of the wire payload preceding the body).
    /// Returns the ID that was used so the caller can match the reply.
    ///
    /// The SP REQ/REP backtrace header terminates at the first 4-byte word
    /// with the high bit set.  We set it unconditionally (direct connection,
    /// no device hops).
    pub fn prepare_outgoing(&mut self, msg: &mut Message) -> u32 {
        let id = self.next_id;
        // Increment; wrap but skip 0.
        self.next_id = self.next_id.wrapping_add(1).max(1);

        // High bit marks "end of backtrace" in the SP header.
        let wire_id = id | 0x8000_0000u32;
        msg.header_push_back(&wire_id.to_be_bytes());
        id
    }

    /// Process an incoming reply: extract and validate the request ID from
    /// the front of the body.  On success the ID is stripped and `msg` holds
    /// only the reply payload.
    pub fn process_incoming(&self, msg: &mut Message, sent_id: u32) -> Result<(), ReqRepError> {
        if msg.body().len() < 4 {
            return Err(ReqRepError::MessageTooShort);
        }
        let wire_id = u32::from_be_bytes(msg.body()[..4].try_into().unwrap());
        msg.trim_front(4);
        let id = wire_id & 0x7fff_ffffu32; // strip the end-of-backtrace flag
        if id != sent_id {
            return Err(ReqRepError::IdMismatch { got: id, expected: sent_id });
        }
        Ok(())
    }
}

impl Default for Req0State {
    fn default() -> Self {
        Self::new()
    }
}

// ── REP0 state ──

/// Opaque routing info saved by the REP side from an incoming request.
/// Must be re-attached when sending the reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingInfo(pub [u8; 4]);

/// State machine for the REP0 (server) side of a request/reply pair.
///
/// `Rep0State` is stateless between requests; routing info is carried in
/// the `RoutingInfo` value returned from `process_incoming`.
pub struct Rep0State;

impl Rep0State {
    pub fn new() -> Self {
        Self
    }

    /// Process an incoming request: extract the 4-byte request ID from the
    /// front of the body.  On success returns the routing info (to be passed
    /// back to `prepare_reply`) and `msg` holds only the application payload.
    pub fn process_incoming(&self, msg: &mut Message) -> Result<RoutingInfo, ReqRepError> {
        if msg.body().len() < 4 {
            return Err(ReqRepError::MessageTooShort);
        }
        let id_bytes: [u8; 4] = msg.body()[..4].try_into().unwrap();
        msg.trim_front(4);
        Ok(RoutingInfo(id_bytes))
    }

    /// Prepare a reply: prepend the saved routing info to the reply header
    /// so it will be transmitted ahead of the body on the wire.
    pub fn prepare_reply(&self, msg: &mut Message, routing: &RoutingInfo) {
        msg.header_push_back(&routing.0);
    }
}

impl Default for Rep0State {
    fn default() -> Self {
        Self::new()
    }
}
