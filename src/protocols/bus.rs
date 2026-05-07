//! BUS0 protocol state machine.
//!
//! The bus pattern is a fully connected mesh: every node can send to and
//! receive from every other node. Each message a node sends is broadcast to
//! all directly connected peers; a node does not receive its own messages.
//!
//! BUS0 is stateless and adds no protocol headers — the wire payload is
//! exactly the application body. The transport layer is responsible for
//! the actual fan-out delivery to connected peers.

use crate::codec::ProtocolId;

pub const PROTOCOL_ID_BUS: ProtocolId = ProtocolId::BUS0;

/// State machine for BUS0. Stateless.
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
