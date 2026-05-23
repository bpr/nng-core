//! PUB0 / SUB0 socket API.
//!
//! `Pub0` listens for subscriber connections and fans out each published
//! message to all currently connected subscribers.  `Sub0` dials a
//! publisher, registers topic-prefix subscriptions, and reads messages
//! that match.

use crate::{Message, codec::ProtocolId, protocols::pubsub::Sub0State};

use super::{
    AnyListener, AnyTransport, NngError, ReconnectOptions, bind_listener, connect_transport,
    reconnect_transport,
};

/// Publish socket: listens for subscriber connections, fans out messages.
pub struct Pub0 {
    listener: AnyListener,
    subscribers: Vec<AnyTransport>,
}

impl Pub0 {
    /// Bind to `addr` and start accepting subscriber connections.
    pub async fn listen(addr: &str) -> Result<Self, NngError> {
        let listener = bind_listener(addr).await?;
        Ok(Self {
            listener,
            subscribers: Vec::new(),
        })
    }

    /// Block until at least `n` subscribers have completed the handshake.
    pub async fn wait_for_subscribers(&mut self, n: usize) -> Result<(), NngError> {
        while self.subscribers.len() < n {
            if let Ok(t) = self.listener.accept_as_transport(ProtocolId::PUB0).await {
                self.subscribers.push(t);
            }
        }
        Ok(())
    }

    async fn drain_incoming(&mut self) {
        loop {
            // Only the raw OS accept belongs in the biased select: it
            // returns Pending immediately when the queue is empty, which
            // is the signal to stop.  The handshake runs outside so it
            // cannot be dropped mid-flight.
            let raw = tokio::select! {
                biased;
                result = self.listener.accept_raw() => match result {
                    Ok(raw) => raw,
                    Err(_) => break,
                },
                _ = std::future::ready(()) => break,
            };
            if let Ok(t) = raw.into_transport(ProtocolId::PUB0).await {
                self.subscribers.push(t);
            }
        }
    }

    /// Publish `msg` to all connected subscribers.
    ///
    /// New connections that arrived since the last publish are accepted
    /// first.  Subscribers whose connections have broken are silently
    /// removed.
    pub async fn publish(&mut self, msg: Message) -> Result<(), NngError> {
        self.drain_incoming().await;
        let mut i = 0;
        while i < self.subscribers.len() {
            if self.subscribers[i].send(&msg).await.is_err() {
                self.subscribers.swap_remove(i);
            } else {
                i += 1;
            }
        }
        Ok(())
    }

    /// Number of currently connected subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.len()
    }

    /// Consume this socket and return a [`futures_sink::Sink`] that
    /// publishes each flushed message to all connected subscribers.
    /// Requires the `streams` feature.
    ///
    /// The returned sink is `Unpin`; call [`SinkExt`](futures_util::SinkExt)
    /// methods on it without `pin_mut!`.
    #[cfg(feature = "streams")]
    pub fn into_sink(
        self,
    ) -> std::pin::Pin<Box<dyn futures_sink::Sink<Message, Error = crate::NngError> + Send>> {
        Box::pin(futures_util::sink::unfold(
            self,
            |mut this: Self, msg: Message| async move {
                this.publish(msg).await?;
                Ok(this)
            },
        ))
    }
}

/// Subscribe socket: connects to a publisher, filters by topic prefix.
pub struct Sub0 {
    transport: AnyTransport,
    state: Sub0State,
    dial_addr: Option<String>,
    reconnect: Option<ReconnectOptions>,
}

impl Sub0 {
    /// Connect to the publisher at `addr`.
    pub async fn dial(addr: &str) -> Result<Self, NngError> {
        let transport = connect_transport(addr, ProtocolId::SUB0).await?;
        Ok(Self {
            transport,
            state: Sub0State::new(),
            dial_addr: None,
            reconnect: None,
        })
    }

    /// Dial with automatic reconnect using default backoff (100 ms → 30 s).
    pub async fn dial_reconnecting(addr: &str) -> Result<Self, NngError> {
        Self::dial_with_reconnect(addr, ReconnectOptions::default()).await
    }

    /// Dial with automatic reconnect using custom `ReconnectOptions`.
    pub async fn dial_with_reconnect(addr: &str, opts: ReconnectOptions) -> Result<Self, NngError> {
        let transport = connect_transport(addr, ProtocolId::SUB0).await?;
        Ok(Self {
            transport,
            state: Sub0State::new(),
            dial_addr: Some(addr.to_owned()),
            reconnect: Some(opts),
        })
    }

    /// Subscribe to messages whose body starts with `prefix`.
    ///
    /// An empty prefix (`b""`) matches every message.  Until at least one
    /// subscription is added, `next()` will never return.
    pub fn subscribe_to(&mut self, prefix: &[u8]) {
        self.state.subscribe(prefix);
    }

    /// Remove a previously added subscription.
    pub fn unsubscribe_from(&mut self, prefix: &[u8]) {
        self.state.unsubscribe(prefix);
    }

    /// Consume this socket and return a [`futures_core::Stream`] of received
    /// messages.  Requires the `streams` feature.
    ///
    /// The stream ends — yielding the error first — when the transport fails
    /// permanently (e.g. the publisher disconnects and no reconnect is
    /// configured).  The returned stream is `Unpin`.
    #[cfg(feature = "streams")]
    pub fn into_stream(
        self,
    ) -> std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<Message, crate::NngError>> + Send>>
    {
        Box::pin(futures_util::stream::unfold(
            Some(self),
            |state| async move {
                let mut this = state?;
                match this.next().await {
                    Ok(msg) => Some((Ok(msg), Some(this))),
                    Err(e) => Some((Err(e), None)),
                }
            },
        ))
    }

    /// Return the next message that matches any active subscription.
    ///
    /// Non-matching messages are silently discarded.
    pub async fn next(&mut self) -> Result<Message, NngError> {
        loop {
            match self.transport.recv().await {
                Ok(msg) => {
                    if self.state.matches(&msg) {
                        return Ok(msg);
                    }
                }
                Err(e) => {
                    if let (Some(addr), Some(opts)) = (&self.dial_addr, &self.reconnect) {
                        let addr = addr.clone();
                        let opts = opts.clone();
                        reconnect_transport(&mut self.transport, &addr, ProtocolId::SUB0, &opts)
                            .await?;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }
}
