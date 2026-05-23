//! BUS0 socket API — many-to-many broadcast.
//!
//! Every `Bus0` node can broadcast messages to all connected peers and
//! receive from any of them.  BUS is stateless at the protocol level.
//!
//! `recv_any` polls peers in round-robin with cooperative yielding.  It is
//! safe to cancel between iterations because `FramedTransport::recv` is
//! cancellation-safe: any bytes already read from the stream are preserved
//! in the transport's internal `RecvBuf` and resumed on the next call.
//! For WebSocket peers, `WsTransport::recv` is also cancellation-safe
//! because tungstenite reassembles frames internally before yielding.
//!
//! [`RecvBuf`]: crate::transport::FramedTransport
//! [`Pull0Fan`]: super::pipeline0::Pull0Fan

use crate::{Message, codec::ProtocolId};

use super::{AnyListener, AnyTransport, NngError, bind_listener, connect_transport};

/// A BUS0 node that can be connected to any number of peers.
///
/// When constructed via [`listen`](Self::listen) or
/// [`listen_and_accept`](Self::listen_and_accept), the OS listener socket
/// is kept open so that [`accept_pending`](Self::accept_pending) can admit
/// new peers at any time after construction.
pub struct Bus0 {
    /// Present when this node bound a listener; `None` for dialled nodes.
    listener: Option<AnyListener>,
    peers: Vec<AnyTransport>,
}

impl Bus0 {
    /// Bind and accept exactly one peer connection.
    pub async fn listen(addr: &str) -> Result<Self, NngError> {
        Self::listen_and_accept(addr, 1).await
    }

    /// Bind and accept `n` peer connections before returning.
    ///
    /// The listener socket is **kept open** so that
    /// [`accept_pending`](Self::accept_pending) can admit peers that
    /// connect after this call returns.
    pub async fn listen_and_accept(addr: &str, n: usize) -> Result<Self, NngError> {
        let listener = bind_listener(addr).await?;
        let mut peers = Vec::with_capacity(n);
        while peers.len() < n {
            if let Ok(t) = listener.accept_as_transport(ProtocolId::BUS0).await {
                peers.push(t);
            }
        }
        Ok(Self {
            listener: Some(listener),
            peers,
        })
    }

    /// Dial one peer and return a `Bus0` with that single connection.
    pub async fn dial(addr: &str) -> Result<Self, NngError> {
        let transport = connect_transport(addr, ProtocolId::BUS0).await?;
        Ok(Self {
            listener: None,
            peers: vec![transport],
        })
    }

    /// Accept any peers whose connections arrived since the last call.
    ///
    /// Returns immediately when the kernel accept queue is empty.  Only
    /// has an effect on nodes created with [`listen`](Self::listen) or
    /// [`listen_and_accept`](Self::listen_and_accept).
    pub async fn accept_pending(&mut self) {
        let Some(listener) = &self.listener else {
            return;
        };
        loop {
            let raw = tokio::select! {
                biased;
                result = listener.accept_raw() => match result {
                    Ok(raw) => raw,
                    Err(_) => break,
                },
                _ = std::future::ready(()) => break,
            };
            if let Ok(t) = raw.into_transport(ProtocolId::BUS0).await {
                self.peers.push(t);
            }
        }
    }

    /// Broadcast `msg` to all connected peers.  Best-effort: broken
    /// connections are silently removed.
    pub async fn broadcast(&mut self, msg: Message) -> Result<(), NngError> {
        let mut i = 0;
        while i < self.peers.len() {
            if self.peers[i].send(&msg).await.is_err() {
                self.peers.swap_remove(i);
            } else {
                i += 1;
            }
        }
        Ok(())
    }

    /// Receive the next message from any connected peer.
    ///
    /// Polls peers in round-robin order with cooperative yielding between
    /// passes.
    pub async fn recv_any(&mut self) -> Result<Message, NngError> {
        loop {
            if self.peers.is_empty() {
                return Err(NngError::NoPeers);
            }
            let mut i = 0;
            while i < self.peers.len() {
                let poll_result = tokio::select! {
                    biased;
                    result = self.peers[i].recv() => Some(result),
                    _ = std::future::ready(()) => None,
                };
                match poll_result {
                    Some(Ok(msg)) => return Ok(msg),
                    Some(Err(_)) => {
                        self.peers.swap_remove(i);
                    }
                    None => {
                        i += 1;
                    }
                }
            }
            tokio::task::yield_now().await;
        }
    }

    /// Receive from a specific peer by index.
    pub async fn recv_from(&mut self, peer_idx: usize) -> Result<Message, NngError> {
        self.peers
            .get_mut(peer_idx)
            .ok_or(NngError::NoPeers)?
            .recv()
            .await
    }

    /// Number of currently connected peers.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    // ── Stream-based accept API ───────────────────────────────────────────

    /// Bind to `addr` and split the result into an empty hub plus an
    /// [`AcceptStream`] yielding incoming peers.
    ///
    /// This is the preferred shape for dynamic membership: the caller
    /// decides when and whether to admit each incoming peer, rather than
    /// pre-committing to a fixed `n` as
    /// [`listen_and_accept`](Self::listen_and_accept) requires.
    ///
    /// ```ignore
    /// use nng_core::socket::bus0::Bus0;
    ///
    /// let (mut hub, mut accepts) = Bus0::bind("tcp://127.0.0.1:5555").await?;
    ///
    /// // Admit the first three peers, then keep serving the hub.
    /// for _ in 0..3 {
    ///     let peer = accepts.accept().await?;
    ///     hub.add_peer(peer);
    /// }
    /// // accepts can be dropped here, or kept for later admission, or
    /// // moved into another task that filters by peer addr / rate-limits.
    /// ```
    ///
    /// The returned [`Bus0`] has `listener: None`, so
    /// [`accept_pending`](Self::accept_pending) is a no-op on it — admit
    /// peers exclusively through the stream while it exists.  The classic
    /// [`listen`](Self::listen) / [`listen_and_accept`](Self::listen_and_accept)
    /// constructors are unchanged.
    pub async fn bind(addr: &str) -> Result<(Self, AcceptStream), NngError> {
        let listener = bind_listener(addr).await?;
        let hub = Self {
            listener: None,
            peers: Vec::new(),
        };
        Ok((hub, AcceptStream { listener }))
    }

    /// Add an accepted peer (from [`AcceptStream::accept`]) to this hub.
    ///
    /// Subsequent [`broadcast`](Self::broadcast) calls will fan out to this
    /// peer; [`recv_any`](Self::recv_any) will poll it.
    pub fn add_peer(&mut self, peer: AcceptedPeer) {
        self.peers.push(peer.0);
    }
}

/// A BUS0-side accepted peer, ready to be handed to [`Bus0::add_peer`].
///
/// Opaque newtype around an internal transport.  Yielded by
/// [`AcceptStream::accept`].
pub struct AcceptedPeer(pub(crate) AnyTransport);

/// Stream of incoming BUS0 peer connections produced by [`Bus0::bind`].
///
/// The stream stays alive as long as the listener socket: callers may
/// keep it past hub construction to admit later peers, or drop it at any
/// time to stop accepting.
pub struct AcceptStream {
    listener: AnyListener,
}

impl AcceptStream {
    /// Await the next incoming peer and complete its SP handshake.
    ///
    /// Cancellation-safe in the same way the underlying transport's
    /// accept is: dropping this future never loses a connection that was
    /// already accepted at the kernel level.
    pub async fn accept(&mut self) -> Result<AcceptedPeer, NngError> {
        self.listener
            .accept_as_transport(ProtocolId::BUS0)
            .await
            .map(AcceptedPeer)
    }
}
