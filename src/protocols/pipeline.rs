//! PUSH0 / PULL0 protocol state machines.
//!
//! Both sides are stateless: no protocol headers are added to messages.
//! PUSH distributes messages in round-robin to connected PULL endpoints;
//! PULL receives from any connected PUSH.

use crate::codec::ProtocolId;

pub const PROTOCOL_ID_PUSH: ProtocolId = ProtocolId::PUSH0;
pub const PROTOCOL_ID_PULL: ProtocolId = ProtocolId::PULL0;

/// State machine for PUSH0.  Stateless.
pub struct Push0State;

impl Push0State {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Push0State {
    fn default() -> Self {
        Self::new()
    }
}

/// State machine for PULL0.  Stateless.
pub struct Pull0State;

impl Pull0State {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Pull0State {
    fn default() -> Self {
        Self::new()
    }
}
