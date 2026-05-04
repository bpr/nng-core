//! PUB0 / SUB0 protocol state machines.
//!
//! PUB is stateless: it sends messages to all connected subscribers.
//! SUB maintains a list of topic prefixes; a message matches if any
//! subscription is a byte-prefix of the message body.
//!
//! An empty subscription matches every message.
//!
//! No message headers are used at the wire level for pub/sub.

use crate::Message;
use crate::codec::ProtocolId;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

pub const PROTOCOL_ID_PUB: ProtocolId = ProtocolId::PUB0;
pub const PROTOCOL_ID_SUB: ProtocolId = ProtocolId::SUB0;

/// State machine for the PUB0 (publisher) side.  Stateless.
pub struct Pub0State;

impl Pub0State {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Pub0State {
    fn default() -> Self {
        Self::new()
    }
}

/// State machine for the SUB0 (subscriber) side.
///
/// Maintains a list of topic subscriptions.  A message is accepted if any
/// subscription is a byte-prefix of the message body.
pub struct Sub0State {
    subscriptions: Vec<Vec<u8>>,
}

impl Sub0State {
    pub fn new() -> Self {
        Self {
            subscriptions: Vec::new(),
        }
    }

    /// Add a subscription.  An empty prefix matches all messages.
    pub fn subscribe(&mut self, prefix: &[u8]) {
        if !self.subscriptions.iter().any(|s| s == prefix) {
            self.subscriptions.push(prefix.to_vec());
        }
    }

    /// Remove a subscription.  No-op if the prefix is not subscribed.
    pub fn unsubscribe(&mut self, prefix: &[u8]) {
        self.subscriptions.retain(|s| s.as_slice() != prefix);
    }

    /// Returns `true` if the message body matches at least one subscription.
    pub fn matches(&self, msg: &Message) -> bool {
        let body = msg.body();
        self.subscriptions
            .iter()
            .any(|prefix| body.starts_with(prefix))
    }

    /// Returns `true` if there are no active subscriptions.
    pub fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }
}

impl Default for Sub0State {
    fn default() -> Self {
        Self::new()
    }
}
