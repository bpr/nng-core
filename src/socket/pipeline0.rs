//! PUSH0 / PULL0 socket API (pipeline pattern).
//!
//! `Push0` / `Pull0` handle a single connection each.  `Push0Fan` and
//! `Pull0Fan` handle N connections: `Push0Fan` round-robins outgoing
//! messages across N pullers; `Pull0Fan` receives from whichever of N
//! pushers has data available first.

use crate::{
    Message,
    codec::ProtocolId,
    protocols::pipeline::{Pull0State, Push0State},
    socket::{NngError, ReconnectOptions, Socket},
};

use super::{AnyListener, AnyTransport, bind_listener};

/// Push socket: sends messages to a single connected pull endpoint.
pub struct Push0(Socket<Push0State>);

impl Push0 {
    forward_socket_method!(dial, ProtocolId::PUSH0, tuple);
    forward_socket_method!(listen, ProtocolId::PUSH0, tuple);
    forward_socket_method!(dial_reconnecting, ProtocolId::PUSH0, tuple);
    forward_socket_method!(dial_with_reconnect, ProtocolId::PUSH0, tuple);
    forward_socket_method!(listen_tls, ProtocolId::PUSH0, tuple);
    forward_socket_method!(listen_tls_tcp, ProtocolId::PUSH0, tuple);
    forward_socket_method!(dial_tls_tcp, ProtocolId::PUSH0, tuple);
    forward_socket_method!(listen_quic, ProtocolId::PUSH0, tuple);
    forward_socket_method!(dial_quic, ProtocolId::PUSH0, tuple);
    forward_socket_method!(listen_kcp_with, ProtocolId::PUSH0, tuple);
    forward_socket_method!(dial_kcp_with, ProtocolId::PUSH0, tuple);

    pub async fn push(&mut self, msg: Message) -> Result<(), NngError> {
        self.0.send_raw(&msg).await
    }

    /// Consume this socket and return a [`futures_sink::Sink`] that sends
    /// each flushed message to the connected pull endpoint.
    /// Requires the `streams` feature.
    ///
    /// The returned sink is `Unpin`.
    #[cfg(feature = "streams")]
    pub fn into_sink(
        self,
    ) -> std::pin::Pin<Box<dyn futures_sink::Sink<Message, Error = crate::NngError> + Send>> {
        Box::pin(futures_util::sink::unfold(
            self,
            |mut this: Self, msg: Message| async move {
                this.push(msg).await?;
                Ok(this)
            },
        ))
    }
}

/// Pull socket: receives messages from a single connected push endpoint.
pub struct Pull0(Socket<Pull0State>);

impl Pull0 {
    forward_socket_method!(dial, ProtocolId::PULL0, tuple);
    forward_socket_method!(listen, ProtocolId::PULL0, tuple);
    forward_socket_method!(dial_reconnecting, ProtocolId::PULL0, tuple);
    forward_socket_method!(dial_with_reconnect, ProtocolId::PULL0, tuple);
    forward_socket_method!(listen_tls, ProtocolId::PULL0, tuple);
    forward_socket_method!(listen_tls_tcp, ProtocolId::PULL0, tuple);
    forward_socket_method!(dial_tls_tcp, ProtocolId::PULL0, tuple);
    forward_socket_method!(listen_quic, ProtocolId::PULL0, tuple);
    forward_socket_method!(dial_quic, ProtocolId::PULL0, tuple);
    forward_socket_method!(listen_kcp_with, ProtocolId::PULL0, tuple);
    forward_socket_method!(dial_kcp_with, ProtocolId::PULL0, tuple);

    pub async fn pull(&mut self) -> Result<Message, NngError> {
        self.0.recv_raw().await
    }

    /// Consume this socket and return a [`futures_core::Stream`] of received
    /// messages.  Requires the `streams` feature.
    ///
    /// The stream ends — yielding the error first — when the transport fails.
    /// The returned stream is `Unpin`; use [`StreamExt`](futures_util::StreamExt)
    /// combinators on it without `pin_mut!`.
    #[cfg(feature = "streams")]
    pub fn into_stream(
        self,
    ) -> std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<Message, crate::NngError>> + Send>>
    {
        Box::pin(futures_util::stream::unfold(
            Some(self),
            |state| async move {
                let mut this = state?;
                match this.pull().await {
                    Ok(msg) => Some((Ok(msg), Some(this))),
                    Err(e) => Some((Err(e), None)),
                }
            },
        ))
    }
}

/// Multi-worker push socket: distributes messages across N connected
/// pullers in round-robin order.
pub struct Push0Fan {
    workers: Vec<AnyTransport>,
    next: usize,
}

impl Push0Fan {
    /// Bind to `addr` and block until exactly `n` PULL connections have
    /// completed the handshake.
    pub async fn listen_and_accept(addr: &str, n: usize) -> Result<Self, NngError> {
        let listener = bind_listener(addr).await?;
        let mut workers = Vec::with_capacity(n);
        while workers.len() < n {
            if let Ok(t) = listener.accept_as_transport(ProtocolId::PUSH0).await {
                workers.push(t);
            }
        }
        Ok(Self { workers, next: 0 })
    }

    /// Send `msg` to the next worker in round-robin order.
    pub async fn push(&mut self, msg: Message) -> Result<(), NngError> {
        if self.workers.is_empty() {
            return Err(NngError::NoPeers);
        }
        let i = self.next;
        self.next = (self.next + 1) % self.workers.len();
        self.workers[i].send(&msg).await
    }

    /// Number of connected workers.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }
}

/// Multi-sender pull socket: accepts N pushers and receives from whichever
/// has data available first.
///
/// Each connected sender runs in its own tokio task so `recv` is never
/// cancelled mid-read.  Dropping `Pull0Fan` cancels all background tasks
/// promptly, even if their peers are idle.
pub struct Pull0Fan {
    rx: tokio::sync::mpsc::Receiver<Result<Message, NngError>>,
    /// Cloned by [`bind`](Self::bind)'s [`add_pusher`](Self::add_pusher)
    /// to wire each new sender into the shared receive channel.
    tx: tokio::sync::mpsc::Sender<Result<Message, NngError>>,
    /// Cloned by [`add_pusher`](Self::add_pusher) so new reader tasks
    /// observe the shutdown watch alongside the original ones.
    shutdown_rx: tokio::sync::watch::Receiver<()>,
    /// Running total of pushers ever added (via `listen_and_accept` or
    /// `add_pusher`).  Inspected by [`sender_count`](Self::sender_count).
    n: usize,
    /// Dropping this sender closes the watch channel, which unblocks every
    /// background task's `cancel.changed()` call and causes it to exit.
    _shutdown: tokio::sync::watch::Sender<()>,
}

impl Pull0Fan {
    /// Bind to `addr` and block until exactly `n` PUSH connections have
    /// completed the handshake.
    pub async fn listen_and_accept(addr: &str, n: usize) -> Result<Self, NngError> {
        let listener = bind_listener(addr).await?;
        let (tx, rx) = tokio::sync::mpsc::channel(n * 4);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
        let mut fan = Self {
            rx,
            tx,
            shutdown_rx,
            n: 0,
            _shutdown: shutdown_tx,
        };
        while fan.n < n {
            if let Ok(transport) = listener.accept_as_transport(ProtocolId::PULL0).await {
                fan.spawn_reader(transport);
            }
        }
        Ok(fan)
    }

    /// Bind to `addr` and split the result into an empty fan plus an
    /// [`AcceptStream`] yielding incoming pushers.  Mirrors
    /// [`Bus0::bind`](crate::socket::bus0::Bus0::bind) and
    /// [`Surveyor0::bind`](crate::socket::survey0::Surveyor0::bind): the
    /// caller decides when and whether to admit each pusher.
    ///
    /// ```ignore
    /// let (mut fan, mut accepts) = Pull0Fan::bind("tcp://127.0.0.1:5555").await?;
    /// for _ in 0..3 {
    ///     let p = accepts.accept().await?;
    ///     fan.add_pusher(p);
    /// }
    /// // Drop accepts to stop admitting; fan.pull_any() now reads from the three.
    /// ```
    pub async fn bind(addr: &str) -> Result<(Self, AcceptStream), NngError> {
        let listener = bind_listener(addr).await?;
        // Channel capacity here is arbitrary; pick something reasonable for
        // a hub that may grow over time.  Callers who need a different
        // backpressure profile can adjust the queue separately.
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
        let fan = Self {
            rx,
            tx,
            shutdown_rx,
            n: 0,
            _shutdown: shutdown_tx,
        };
        Ok((fan, AcceptStream { listener }))
    }

    /// Add a pusher accepted via [`AcceptStream::accept`].
    ///
    /// Spawns a per-sender reader task — same cancellation-safety guarantees
    /// as the senders added by [`listen_and_accept`](Self::listen_and_accept).
    pub fn add_pusher(&mut self, pusher: AcceptedPusher) {
        self.spawn_reader(pusher.0);
    }

    fn spawn_reader(&mut self, mut transport: AnyTransport) {
        let tx = self.tx.clone();
        let mut cancel = self.shutdown_rx.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = transport.recv() => {
                        match result {
                            Ok(msg) => {
                                if tx.send(Ok(msg)).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                let _ = tx.send(Err(e)).await;
                                break;
                            }
                        }
                    }
                    // Fires (with Err) when the Pull0Fan is dropped and the
                    // shutdown_tx Sender is closed.
                    _ = cancel.changed() => break,
                }
            }
        });
        self.n += 1;
    }

    /// Receive the next message from any connected sender.
    ///
    /// Skips sender-disconnect errors so callers keep receiving from the
    /// remaining live senders.  Returns `Err` only when all senders are gone.
    pub async fn pull_any(&mut self) -> Result<Message, NngError> {
        loop {
            match self.rx.recv().await {
                None => return Err(NngError::NoPeers),
                Some(Ok(msg)) => return Ok(msg),
                Some(Err(_)) => {}
            }
        }
    }

    /// Running total of pushers ever added (via
    /// [`listen_and_accept`](Self::listen_and_accept) or
    /// [`add_pusher`](Self::add_pusher)).  Does **not** decrement when a
    /// sender disconnects — see the disconnect handling in
    /// [`pull_any`](Self::pull_any).
    pub fn sender_count(&self) -> usize {
        self.n
    }
}

/// A pusher accepted via [`AcceptStream::accept`], ready to be handed to
/// [`Pull0Fan::add_pusher`].  Opaque newtype around an internal transport.
pub struct AcceptedPusher(pub(crate) AnyTransport);

/// Stream of incoming PUSH peer connections produced by [`Pull0Fan::bind`].
pub struct AcceptStream {
    listener: AnyListener,
}

impl AcceptStream {
    /// Await the next incoming pusher and complete its SP handshake.
    pub async fn accept(&mut self) -> Result<AcceptedPusher, NngError> {
        self.listener
            .accept_as_transport(ProtocolId::PULL0)
            .await
            .map(AcceptedPusher)
    }
}

/// [`Pull0Fan`] implements [`futures_core::Stream`] directly, mirroring
/// [`Pull0Fan::pull_any`]: individual sender-disconnect errors are skipped
/// transparently, and the stream ends (yields `None`) only when all sender
/// tasks have exited and the channel is drained.
///
/// Requires the `streams` feature.
#[cfg(feature = "streams")]
impl futures_core::Stream for Pull0Fan {
    type Item = Message;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            match self.rx.poll_recv(cx) {
                std::task::Poll::Ready(Some(Ok(msg))) => {
                    return std::task::Poll::Ready(Some(msg));
                }
                std::task::Poll::Ready(Some(Err(_))) => {
                    // A single sender disconnected — skip and poll again.
                    continue;
                }
                std::task::Poll::Ready(None) => {
                    // All sender tasks have exited; the stream is done.
                    return std::task::Poll::Ready(None);
                }
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
    }
}
