//! BUS0 protocol state machine.
//!
//! BUS is stateless: each message is broadcast to all connected peers except
//! the one it was received from.  No protocol headers are added to messages.

use crate::codec::ProtocolId;

pub const PROTOCOL_ID_BUS: ProtocolId = ProtocolId::BUS0;

/// State machine for BUS0.  Stateless.
pub struct Bus0State;

impl Bus0State {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Bus0State {
    fn default() -> Self {
        Self::new()
    }
}
