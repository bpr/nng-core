//! REQ0 / REP0 protocol state machines.
//!
//! # Wire encoding
//!
//! Each request carries a **4-byte request ID** (u32 big-endian) that travels
//! ahead of the user payload on the wire:
//!
//! ```text
//! ┌─────────────────────┬────────────────────┐
//! │  request ID (4 B)   │   body (variable)  │
//! └─────────────────────┴────────────────────┘
//! ```
//!
//! On send the ID is placed in the message **header**, so [`FramedTransport`]
//! automatically writes it before the body. On receive the entire wire payload
//! lands in the message body; the state machine strips the leading 4 bytes.
//!
//! # The high-bit backtrace marker
//!
//! The SP specification supports multi-hop routing through NNG "device"
//! forwarders. A device sits between a REQ and REP and rewrites the routing
//! header as the message passes through, building a backtrace that the REP can
//! use to route the reply back the same way.
//!
//! The backtrace is a chain of 4-byte hop addresses prepended to the header.
//! To mark the *end* of the backtrace (i.e., the actual request ID), the
//! protocol sets the **high bit** (`0x8000_0000`) of the final word. The REP
//! scans the header until it finds a word with the high bit set; everything
//! from there to the end of the header is the routing backtrace.
//!
//! For a direct (no-device) connection there is only one hop, so the header
//! is exactly one word: `reqID | 0x8000_0000`. When the reply comes back, the
//! REQ strips the high bit before comparing with the locally stored ID.
//!
//! If the high bit is *not* set, NNG's REP side keeps scanning into the
//! payload bytes looking for the terminator — corrupting the message.
//!
//! [`FramedTransport`]: crate::transport::FramedTransport

use crate::codec::ProtocolId;
use crate::message::MessageBuf;

pub const PROTOCOL_ID_REQ: ProtocolId = ProtocolId::REQ0;
pub const PROTOCOL_ID_REP: ProtocolId = ProtocolId::REP0;

// ── Errors ────────────────────────────────────────────────────────────────────

/// Errors produced by the REQ0 / REP0 state machines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReqRepError {
    /// Received reply ID did not match the ID of the pending request.
    ///
    /// This can happen if a stale reply arrives after a new request has been
    /// issued, or if the peer is misbehaving.
    IdMismatch { got: u32, expected: u32 },
    /// Received message body was shorter than 4 bytes — no room for a request ID.
    MessageTooShort,
}

impl core::fmt::Display for ReqRepError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::IdMismatch { got, expected } => {
                write!(
                    f,
                    "req/rep ID mismatch: got {got:#010x}, expected {expected:#010x}"
                )
            }
            Self::MessageTooShort => write!(f, "message too short to contain request ID"),
        }
    }
}

// ── REQ0 state ────────────────────────────────────────────────────────────────

/// State machine for the REQ0 (client) side of a request/reply exchange.
///
/// Tracks a monotonically increasing request ID counter so that replies can
/// be correlated with their requests. The counter skips zero (NNG uses 0 as
/// "no pending request" internally) and wraps via `wrapping_add(1).max(1)`.
pub struct Req0State {
    next_id: u32,
}

impl Req0State {
    pub fn new() -> Self {
        Self { next_id: 1 }
    }

    /// Stamp an outgoing request with a fresh ID and return that ID.
    ///
    /// The ID is written to the message **header** as a big-endian u32 with
    /// the high bit set (`reqID | 0x8000_0000`). The high bit marks this word
    /// as the end of the SP backtrace chain — without it a multi-hop NNG device
    /// (or the REP side on a direct connection) would scan past the ID into the
    /// payload bytes.
    ///
    /// Returns the raw ID (without the high bit) so the caller can pass it to
    /// [`process_incoming`](Self::process_incoming) for reply validation.
    pub fn prepare_outgoing<M: MessageBuf>(&mut self, msg: &mut M) -> u32 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1);

        let wire_id = id | 0x8000_0000u32;
        msg.header_push_back(&wire_id.to_be_bytes());
        id
    }

    /// Validate an incoming reply and strip the request ID from the body.
    ///
    /// Reads the first 4 bytes of `msg.body()` as a big-endian u32, strips the
    /// high bit (backtrace marker), and compares with `sent_id`. On success
    /// the 4 bytes are consumed and `msg.body()` holds only the reply payload.
    pub fn process_incoming<M: MessageBuf>(
        &self,
        msg: &mut M,
        sent_id: u32,
    ) -> Result<(), ReqRepError> {
        if msg.body().len() < 4 {
            return Err(ReqRepError::MessageTooShort);
        }
        let wire_id = u32::from_be_bytes(msg.body()[..4].try_into().unwrap());
        msg.trim_front(4);
        let id = wire_id & 0x7fff_ffffu32; // strip the end-of-backtrace flag
        if id != sent_id {
            return Err(ReqRepError::IdMismatch {
                got: id,
                expected: sent_id,
            });
        }
        Ok(())
    }
}

impl Default for Req0State {
    fn default() -> Self {
        Self::new()
    }
}

// ── REP0 state ────────────────────────────────────────────────────────────────

/// Opaque routing information extracted from an incoming request.
///
/// Contains the 4-byte wire request ID (with the high backtrace-marker bit
/// still set). Must be passed back to [`Rep0State::prepare_reply`] so the
/// reply is routed to the correct caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingInfo(pub [u8; 4]);

/// State machine for the REP0 (server) side of a request/reply exchange.
///
/// `Rep0State` is stateless between requests: all per-request state lives in
/// the [`RoutingInfo`] value returned by [`process_incoming`](Self::process_incoming).
/// This makes it straightforward to handle multiple in-flight requests in
/// different tasks — each task holds its own `RoutingInfo`.
pub struct Rep0State;

impl Rep0State {
    pub fn new() -> Self {
        Self
    }

    /// Strip the request ID from the front of an incoming message.
    ///
    /// Returns [`RoutingInfo`] containing the raw 4-byte ID (including the
    /// high backtrace-marker bit). The caller must pass this back to
    /// [`prepare_reply`](Self::prepare_reply) when sending the response.
    ///
    /// On success `msg.body()` holds only the application payload.
    pub fn process_incoming<M: MessageBuf>(&self, msg: &mut M) -> Result<RoutingInfo, ReqRepError> {
        if msg.body().len() < 4 {
            return Err(ReqRepError::MessageTooShort);
        }
        let id_bytes: [u8; 4] = msg.body()[..4].try_into().unwrap();
        msg.trim_front(4);
        Ok(RoutingInfo(id_bytes))
    }

    /// Attach the saved routing ID to the reply header.
    ///
    /// Places the 4-byte `routing` value into the message header so that
    /// [`FramedTransport`] will transmit it before the body, and the REQ side
    /// can find and validate it.
    ///
    /// [`FramedTransport`]: crate::transport::FramedTransport
    pub fn prepare_reply<M: MessageBuf>(&self, msg: &mut M, routing: &RoutingInfo) {
        msg.header_push_back(&routing.0);
    }
}

impl Default for Rep0State {
    fn default() -> Self {
        Self::new()
    }
}
