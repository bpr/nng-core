//! PUB0 / SUB0 protocol state machines.
//!
//! # Publish / Subscribe pattern
//!
//! The publisher sends messages to *all* connected subscribers. No headers are
//! added at the wire level — PUB/SUB is pure fan-out delivery.
//!
//! # Topic filtering
//!
//! Filtering happens on the **subscriber** side, not the publisher. The
//! subscriber registers one or more byte-string prefixes ("topics"); a message
//! is accepted if any registered prefix is a byte-prefix of the message body.
//!
//! ```text
//! subscription "news:" matches body "news: headline"  ✓
//! subscription "news:" does not match "weather: sunny" ✗
//! ```
//!
//! An **empty** subscription (`b""`) is a prefix of every byte string, so it
//! matches all messages — the subscriber receives everything the publisher sends.
//!
//! Duplicate subscriptions are de-duplicated on `subscribe`; a single call to
//! `unsubscribe` removes the entry regardless of how many times it was added.
//!
//! # No headers
//!
//! Neither PUB0 nor SUB0 prepend any header to messages. The message body is
//! the topic-keyed payload exactly as the application wrote it. This means
//! [`Sub0State::matches`] only needs to read `msg.body()` — it never calls
//! `trim_front` or `header_push_back`.

use crate::message::MessageBuf;
use crate::codec::ProtocolId;

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

pub const PROTOCOL_ID_PUB: ProtocolId = ProtocolId::PUB0;
pub const PROTOCOL_ID_SUB: ProtocolId = ProtocolId::SUB0;

/// State machine for the PUB0 (publisher) side. Stateless.
///
/// The publisher has no per-message state: it simply sends to all connected
/// subscribers and relies on the transport layer for fan-out delivery.
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
/// Maintains a de-duplicated list of topic prefixes. A message passes the
/// filter if *any* prefix is a byte-prefix of the message body.
///
/// Requires `alloc` (for the subscription list). The individual `matches`
/// check is `no_alloc`-compatible via the [`MessageBuf`] bound.
pub struct Sub0State {
    subscriptions: Vec<Vec<u8>>,
}

impl Sub0State {
    pub fn new() -> Self {
        Self {
            subscriptions: Vec::new(),
        }
    }

    /// Register a topic prefix. An empty prefix (`b""`) matches all messages.
    ///
    /// Duplicate prefixes are silently ignored so that a single call to
    /// [`unsubscribe`](Self::unsubscribe) is always sufficient to remove the topic.
    pub fn subscribe(&mut self, prefix: &[u8]) {
        if !self.subscriptions.iter().any(|s| s == prefix) {
            self.subscriptions.push(prefix.to_vec());
        }
    }

    /// Remove a previously registered topic prefix. No-op if not subscribed.
    pub fn unsubscribe(&mut self, prefix: &[u8]) {
        self.subscriptions.retain(|s| s.as_slice() != prefix);
    }

    /// Return `true` if `msg.body()` starts with at least one registered prefix.
    ///
    /// Generic over [`MessageBuf`] so this check works for both heap-backed
    /// [`Message`](crate::Message) and stack-allocated
    /// [`ZeroCopyMessage`](crate::ZeroCopyMessage) values.
    pub fn matches<M: MessageBuf>(&self, msg: &M) -> bool {
        let body = msg.body();
        self.subscriptions
            .iter()
            .any(|prefix| body.starts_with(prefix))
    }

    /// Return `true` if there are no active subscriptions.
    ///
    /// When empty, [`matches`](Self::matches) always returns `false`.
    pub fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }
}

impl Default for Sub0State {
    fn default() -> Self {
        Self::new()
    }
}
