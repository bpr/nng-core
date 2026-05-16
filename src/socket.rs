//! High-level socket API for nng-core (requires `std` / tokio).
//!
//! Supported URL schemes:
//! - `tcp://host:port` — plain TCP
//! - `ipc:///path` — Unix domain socket, NNG 1.5.x framing (9-byte header)
//! - `ipc2:///path` — Unix domain socket, NNG 2.x framing (8-byte header, same as TCP)
//! - `ws://host:port[/path]` — WebSocket (requires `ws` feature)
//! - `wss://host:port[/path]` — Secure WebSocket / TLS (requires `wss` feature)
//!
//! Each socket type wraps an [`AnyTransport`], which dispatches to either
//! [`FramedTransport`] (TCP / IPC) or [`WsTransport`] (WebSocket / TLS)
//! depending on the URL scheme used at construction time.

use std::io;

use tokio::net::{TcpListener, TcpStream};

use embedded_io_async::{ErrorType, Read as EioRead, Write as EioWrite};

use crate::{
    Message,
    codec::ProtocolId,
    transport::{FrameFormat, FramedTransport, TransportError, tcp::TokioTcpStream},
};

#[cfg(unix)]
use crate::transport::ipc::TokioUnixStream;
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};

#[cfg(feature = "ws")]
use crate::transport::ws::{WsError, WsTransport};

#[cfg(feature = "wss")]
use crate::transport::ws::build_tls_acceptor;

// ── AnyTransport: FramedTransport or WsTransport ─────────────────────────────

pub(crate) enum AnyTransport {
    Framed(FramedTransport<AnyStream>),
    #[cfg(feature = "ws")]
    Ws(WsTransport),
}

impl AnyTransport {
    pub(crate) async fn send(&mut self, msg: &Message) -> io::Result<()> {
        match self {
            Self::Framed(t) => t.send(msg).await.map_err(te_to_io),
            #[cfg(feature = "ws")]
            Self::Ws(t) => t.send(msg).await.map_err(ws_to_io),
        }
    }

    pub(crate) async fn recv(&mut self) -> io::Result<Message> {
        match self {
            Self::Framed(t) => t.recv().await.map_err(te_to_io),
            #[cfg(feature = "ws")]
            Self::Ws(t) => t.recv().await.map_err(ws_to_io),
        }
    }
}

fn te_to_io(e: TransportError) -> io::Error {
    io::Error::other(e.to_string())
}

#[cfg(feature = "ws")]
fn ws_to_io(e: WsError) -> io::Error {
    io::Error::other(e.to_string())
}

// ── AnyStream: TCP or IPC behind a single trait impl ─────────────────────────

pub(crate) enum AnyStream {
    Tcp(TokioTcpStream),
    /// NNG 1.5.x IPC: 9-byte frame header (0x01 type + 8-byte length).
    #[cfg(unix)]
    Ipc(TokioUnixStream),
    /// NNG 2.x IPC: 8-byte frame header (same as TCP).
    #[cfg(unix)]
    Ipc2(TokioUnixStream),
}

impl ErrorType for AnyStream {
    type Error = std::io::Error;
}

impl EioRead for AnyStream {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        match self {
            Self::Tcp(s) => EioRead::read(s, buf).await,
            #[cfg(unix)]
            Self::Ipc(s) | Self::Ipc2(s) => EioRead::read(s, buf).await,
        }
    }
}

impl EioWrite for AnyStream {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        match self {
            Self::Tcp(s) => EioWrite::write(s, buf).await,
            #[cfg(unix)]
            Self::Ipc(s) | Self::Ipc2(s) => EioWrite::write(s, buf).await,
        }
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        match self {
            Self::Tcp(s) => EioWrite::flush(s).await,
            #[cfg(unix)]
            Self::Ipc(s) | Self::Ipc2(s) => EioWrite::flush(s).await,
        }
    }
}

impl AnyStream {
    fn frame_format(&self) -> FrameFormat {
        match self {
            Self::Tcp(_) => FrameFormat::Tcp,
            #[cfg(unix)]
            Self::Ipc(_) => FrameFormat::Ipc,
            #[cfg(unix)]
            Self::Ipc2(_) => FrameFormat::Tcp,
        }
    }
}

// ── AnyListener: TCP, IPC, or WebSocket ──────────────────────────────────────

pub(crate) enum AnyListener {
    Tcp(TcpListener),
    /// NNG 1.5.x IPC: 9-byte frame header.
    #[cfg(unix)]
    Ipc(UnixListener),
    /// NNG 2.x IPC: 8-byte frame header (same as TCP).
    #[cfg(unix)]
    Ipc2(UnixListener),
    #[cfg(feature = "ws")]
    Ws(TcpListener),
    #[cfg(feature = "wss")]
    Wss(TcpListener, tokio_rustls::TlsAcceptor),
}

impl AnyListener {
    /// Accept one connection and complete the SP handshake (TCP/IPC) or
    /// WebSocket upgrade (ws://), returning a ready-to-use `AnyTransport`.
    pub(crate) async fn accept_as_transport(&self, proto: ProtocolId) -> io::Result<AnyTransport> {
        match self {
            Self::Tcp(l) => {
                let (stream, _) = l.accept().await?;
                connect_framed(AnyStream::Tcp(TokioTcpStream(stream)), proto)
                    .await
                    .map(AnyTransport::Framed)
                    .map_err(te_to_io)
            }
            #[cfg(unix)]
            Self::Ipc(l) => {
                let (stream, _) = l.accept().await?;
                connect_framed(AnyStream::Ipc(TokioUnixStream(stream)), proto)
                    .await
                    .map(AnyTransport::Framed)
                    .map_err(te_to_io)
            }
            #[cfg(unix)]
            Self::Ipc2(l) => {
                let (stream, _) = l.accept().await?;
                connect_framed(AnyStream::Ipc2(TokioUnixStream(stream)), proto)
                    .await
                    .map(AnyTransport::Framed)
                    .map_err(te_to_io)
            }
            #[cfg(feature = "ws")]
            Self::Ws(l) => {
                let (tcp, _) = l.accept().await?;
                WsTransport::accept(tcp, proto)
                    .await
                    .map(AnyTransport::Ws)
                    .map_err(ws_to_io)
            }
            #[cfg(feature = "wss")]
            Self::Wss(l, acceptor) => {
                let (tcp, _) = l.accept().await?;
                WsTransport::accept_tls_with_acceptor(tcp, proto, acceptor)
                    .await
                    .map(AnyTransport::Ws)
                    .map_err(ws_to_io)
            }
        }
    }

    /// Accept one raw OS-level connection without performing any protocol
    /// handshake.  Used in `biased`-select drain loops where only the kernel
    /// accept (which returns `Pending` when the queue is empty) belongs inside
    /// the select; the handshake must run outside so it cannot be cancelled.
    pub(crate) async fn accept_raw(&self) -> io::Result<RawConn> {
        match self {
            Self::Tcp(l) => {
                let (stream, _) = l.accept().await?;
                Ok(RawConn::Stream(AnyStream::Tcp(TokioTcpStream(stream))))
            }
            #[cfg(unix)]
            Self::Ipc(l) => {
                let (stream, _) = l.accept().await?;
                Ok(RawConn::Stream(AnyStream::Ipc(TokioUnixStream(stream))))
            }
            #[cfg(unix)]
            Self::Ipc2(l) => {
                let (stream, _) = l.accept().await?;
                Ok(RawConn::Stream(AnyStream::Ipc2(TokioUnixStream(stream))))
            }
            #[cfg(feature = "ws")]
            Self::Ws(l) => {
                let (tcp, _) = l.accept().await?;
                Ok(RawConn::Ws(tcp))
            }
            #[cfg(feature = "wss")]
            Self::Wss(l, acceptor) => {
                let (tcp, _) = l.accept().await?;
                Ok(RawConn::Wss(tcp, acceptor.clone()))
            }
        }
    }
}

/// A raw accepted connection before any protocol handshake or WS upgrade.
pub(crate) enum RawConn {
    Stream(AnyStream),
    #[cfg(feature = "ws")]
    Ws(TcpStream),
    #[cfg(feature = "wss")]
    Wss(TcpStream, tokio_rustls::TlsAcceptor),
}

impl RawConn {
    /// Complete the protocol handshake or WebSocket upgrade and return a
    /// ready-to-use transport.
    pub(crate) async fn into_transport(self, proto: ProtocolId) -> io::Result<AnyTransport> {
        match self {
            Self::Stream(s) => connect_framed(s, proto)
                .await
                .map(AnyTransport::Framed)
                .map_err(te_to_io),
            #[cfg(feature = "ws")]
            Self::Ws(tcp) => WsTransport::accept(tcp, proto)
                .await
                .map(AnyTransport::Ws)
                .map_err(ws_to_io),
            #[cfg(feature = "wss")]
            Self::Wss(tcp, acceptor) => {
                WsTransport::accept_tls_with_acceptor(tcp, proto, &acceptor)
                    .await
                    .map(AnyTransport::Ws)
                    .map_err(ws_to_io)
            }
        }
    }
}

// ── URL dispatch helpers ──────────────────────────────────────────────────────

pub(crate) async fn bind_listener(addr: &str) -> io::Result<AnyListener> {
    if let Some(tcp_addr) = addr.strip_prefix("tcp://") {
        TcpListener::bind(tcp_addr).await.map(AnyListener::Tcp)
    } else if let Some(ipc_path) = addr.strip_prefix("ipc://") {
        bind_ipc_listener(ipc_path)
    } else if let Some(ipc_path) = addr.strip_prefix("ipc2://") {
        bind_ipc2_listener(ipc_path)
    } else if let Some(ws_addr) = addr.strip_prefix("ws://") {
        #[cfg(feature = "ws")]
        {
            // Bind to host:port only; ignore any path component in the URL.
            let host_port = ws_addr.split('/').next().unwrap_or(ws_addr);
            return TcpListener::bind(host_port).await.map(AnyListener::Ws);
        }
        #[cfg(not(feature = "ws"))]
        {
            let _ = ws_addr;
            return Err(io::Error::other("ws:// requires the `ws` Cargo feature"));
        }
    } else if addr.starts_with("wss://") {
        Err(io::Error::other(
            "wss:// requires listen_tls — use listen_tls(addr, cert_pem, key_pem)",
        ))
    } else {
        Err(io::Error::other(format!("unsupported URL scheme: {addr}")))
    }
}

/// Bind a TLS WebSocket listener.
///
/// Loads the certificate chain and private key from PEM files once; the
/// resulting `TlsAcceptor` is reused for every incoming connection.
#[cfg(feature = "wss")]
pub(crate) async fn bind_listener_tls(
    addr: &str,
    cert_pem: &std::path::Path,
    key_pem: &std::path::Path,
) -> io::Result<AnyListener> {
    let host_port = addr
        .strip_prefix("wss://")
        .ok_or_else(|| io::Error::other("bind_listener_tls requires a wss:// URL"))?
        .split('/')
        .next()
        .unwrap_or(addr);
    let acceptor =
        build_tls_acceptor(cert_pem, key_pem).map_err(|e| io::Error::other(e.to_string()))?;
    let listener = TcpListener::bind(host_port).await?;
    Ok(AnyListener::Wss(listener, acceptor))
}

pub(crate) async fn connect_stream(addr: &str) -> io::Result<AnyStream> {
    if let Some(tcp_addr) = addr.strip_prefix("tcp://") {
        TcpStream::connect(tcp_addr)
            .await
            .map(|s| AnyStream::Tcp(TokioTcpStream(s)))
    } else if let Some(ipc_path) = addr.strip_prefix("ipc://") {
        connect_ipc_stream(ipc_path).await
    } else if let Some(ipc_path) = addr.strip_prefix("ipc2://") {
        connect_ipc2_stream(ipc_path).await
    } else {
        Err(io::Error::other(format!("unsupported URL scheme: {addr}")))
    }
}

/// Dial a URL and return a ready `AnyTransport`.
///
/// Handles `ws://` via [`WsTransport::connect`]; all other schemes go through
/// [`connect_stream`] + [`connect_framed`].
pub(crate) async fn connect_transport(addr: &str, proto: ProtocolId) -> io::Result<AnyTransport> {
    #[cfg(feature = "ws")]
    if addr.starts_with("ws://") {
        return WsTransport::connect(addr, proto)
            .await
            .map(AnyTransport::Ws)
            .map_err(ws_to_io);
    }
    #[cfg(feature = "wss")]
    if addr.starts_with("wss://") {
        return WsTransport::connect(addr, proto)
            .await
            .map(AnyTransport::Ws)
            .map_err(ws_to_io);
    }
    let stream = connect_stream(addr).await?;
    connect_framed(stream, proto)
        .await
        .map(AnyTransport::Framed)
        .map_err(te_to_io)
}

pub(crate) async fn connect_framed(
    stream: AnyStream,
    proto: ProtocolId,
) -> Result<FramedTransport<AnyStream>, TransportError> {
    let format = stream.frame_format();
    FramedTransport::connect(stream, proto, format).await
}

#[cfg(unix)]
fn bind_ipc_listener(path: &str) -> io::Result<AnyListener> {
    let _ = std::fs::remove_file(path);
    UnixListener::bind(path).map(AnyListener::Ipc)
}

#[cfg(not(unix))]
fn bind_ipc_listener(_path: &str) -> io::Result<AnyListener> {
    Err(io::Error::other(
        "IPC (Unix domain sockets) is not supported on this platform",
    ))
}

#[cfg(unix)]
fn bind_ipc2_listener(path: &str) -> io::Result<AnyListener> {
    let _ = std::fs::remove_file(path);
    UnixListener::bind(path).map(AnyListener::Ipc2)
}

#[cfg(not(unix))]
fn bind_ipc2_listener(_path: &str) -> io::Result<AnyListener> {
    Err(io::Error::other(
        "IPC (Unix domain sockets) is not supported on this platform",
    ))
}

#[cfg(unix)]
async fn connect_ipc_stream(path: &str) -> io::Result<AnyStream> {
    UnixStream::connect(path)
        .await
        .map(|s| AnyStream::Ipc(TokioUnixStream(s)))
}

#[cfg(not(unix))]
async fn connect_ipc_stream(_path: &str) -> io::Result<AnyStream> {
    Err(io::Error::other(
        "IPC (Unix domain sockets) is not supported on this platform",
    ))
}

#[cfg(unix)]
async fn connect_ipc2_stream(path: &str) -> io::Result<AnyStream> {
    UnixStream::connect(path)
        .await
        .map(|s| AnyStream::Ipc2(TokioUnixStream(s)))
}

#[cfg(not(unix))]
async fn connect_ipc2_stream(_path: &str) -> io::Result<AnyStream> {
    Err(io::Error::other(
        "IPC (Unix domain sockets) is not supported on this platform",
    ))
}

// ── Socket<P> ────────────────────────────────────────────────────────────────

/// A connected socket wrapping a single transport (TCP, IPC, or WebSocket).
pub struct Socket<P> {
    transport: AnyTransport,
    _protocol: core::marker::PhantomData<P>,
}

impl<P> Socket<P> {
    fn new(transport: AnyTransport) -> Self {
        Self {
            transport,
            _protocol: core::marker::PhantomData,
        }
    }

    /// Bind and wait for the first incoming connection, then complete the
    /// handshake.  The listener is dropped after accepting one connection.
    pub async fn listen(addr: &str, proto: ProtocolId) -> io::Result<Self> {
        let listener = bind_listener(addr).await?;
        let transport = listener.accept_as_transport(proto).await?;
        Ok(Self::new(transport))
    }

    /// Bind a TLS WebSocket listener and wait for the first connection.
    ///
    /// Loads the server certificate and private key from PEM files once,
    /// then performs the TLS handshake and WebSocket upgrade on the first
    /// accepted connection.
    #[cfg(feature = "wss")]
    pub async fn listen_tls(
        addr: &str,
        proto: ProtocolId,
        cert_pem: &std::path::Path,
        key_pem: &std::path::Path,
    ) -> io::Result<Self> {
        let listener = bind_listener_tls(addr, cert_pem, key_pem).await?;
        let transport = listener.accept_as_transport(proto).await?;
        Ok(Self::new(transport))
    }

    /// Connect to `addr` and complete the handshake.
    pub async fn dial(addr: &str, proto: ProtocolId) -> io::Result<Self> {
        let transport = connect_transport(addr, proto).await?;
        Ok(Self::new(transport))
    }

    pub async fn send_raw(&mut self, msg: &Message) -> io::Result<()> {
        self.transport.send(msg).await
    }

    pub async fn recv_raw(&mut self) -> io::Result<Message> {
        self.transport.recv().await
    }
}

// ── Protocol modules ──────────────────────────────────────────────────────────

pub mod pubsub0 {
    //! PUB0 / SUB0 socket API.
    //!
    //! `Pub0` listens for subscriber connections and fans out each published
    //! message to all currently connected subscribers.  `Sub0` dials a
    //! publisher, registers topic-prefix subscriptions, and reads messages
    //! that match.

    use std::io;

    use crate::{Message, codec::ProtocolId, protocols::pubsub::Sub0State};

    use super::{AnyListener, AnyTransport, bind_listener, connect_transport};

    /// Publish socket: listens for subscriber connections, fans out messages.
    pub struct Pub0 {
        listener: AnyListener,
        subscribers: Vec<AnyTransport>,
    }

    impl Pub0 {
        /// Bind to `addr` and start accepting subscriber connections.
        pub async fn listen(addr: &str) -> io::Result<Self> {
            let listener = bind_listener(addr).await?;
            Ok(Self {
                listener,
                subscribers: Vec::new(),
            })
        }

        /// Block until at least `n` subscribers have completed the handshake.
        pub async fn wait_for_subscribers(&mut self, n: usize) -> io::Result<()> {
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
        pub async fn publish(&mut self, msg: Message) -> io::Result<()> {
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
    }

    /// Subscribe socket: connects to a publisher, filters by topic prefix.
    pub struct Sub0 {
        transport: AnyTransport,
        state: Sub0State,
    }

    impl Sub0 {
        /// Connect to the publisher at `addr`.
        pub async fn dial(addr: &str) -> io::Result<Self> {
            let transport = connect_transport(addr, ProtocolId::SUB0).await?;
            Ok(Self {
                transport,
                state: Sub0State::new(),
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

        /// Return the next message that matches any active subscription.
        ///
        /// Non-matching messages are silently discarded.
        pub async fn next(&mut self) -> io::Result<Message> {
            loop {
                let msg = self.transport.recv().await?;
                if self.state.matches(&msg) {
                    return Ok(msg);
                }
            }
        }
    }
}

pub mod survey0 {
    //! SURVEYOR0 / RESPONDENT0 socket API.
    //!
    //! The surveyor broadcasts a question to all connected respondents and
    //! collects answers within a deadline.  The respondent receives surveys
    //! and may reply via the returned `SurveyHandle`.

    use std::{io, time::Duration};

    use crate::{
        Message,
        codec::ProtocolId,
        protocols::survey::{Respondent0State, SurveyRoutingInfo, Surveyor0State},
    };

    use super::{AnyListener, AnyTransport, bind_listener, connect_transport};

    /// Surveyor socket: broadcasts surveys to multiple respondents.
    pub struct Surveyor0 {
        listener: AnyListener,
        respondents: Vec<AnyTransport>,
        state: Surveyor0State,
    }

    impl Surveyor0 {
        /// Bind and start accepting respondent connections.
        pub async fn listen(addr: &str) -> io::Result<Self> {
            let listener = bind_listener(addr).await?;
            Ok(Self {
                listener,
                respondents: Vec::new(),
                state: Surveyor0State::new(),
            })
        }

        /// Block until at least `n` respondents have connected.
        pub async fn wait_for_respondents(&mut self, n: usize) -> io::Result<()> {
            while self.respondents.len() < n {
                if let Ok(t) = self
                    .listener
                    .accept_as_transport(ProtocolId::SURVEYOR0)
                    .await
                {
                    self.respondents.push(t);
                }
            }
            Ok(())
        }

        /// Accept any respondents that connected since the last call.
        ///
        /// Returns immediately when the kernel's accept queue is empty.
        pub async fn accept_pending(&mut self) {
            loop {
                let raw = tokio::select! {
                    biased;
                    result = self.listener.accept_raw() => match result {
                        Ok(raw) => raw,
                        Err(_) => break,
                    },
                    _ = std::future::ready(()) => break,
                };
                if let Ok(t) = raw.into_transport(ProtocolId::SURVEYOR0).await {
                    self.respondents.push(t);
                }
            }
        }

        /// Broadcast `msg` as a survey; collect all responses arriving within
        /// `timeout`.  Returns the set of application-level response bodies.
        pub async fn survey(
            &mut self,
            msg: Message,
            timeout: Duration,
        ) -> io::Result<Vec<Message>> {
            let mut outgoing = msg;
            self.state.prepare_survey(&mut outgoing);

            let deadline = tokio::time::Instant::now() + timeout;

            let mut active: Vec<usize> = Vec::new();
            for (i, resp) in self.respondents.iter_mut().enumerate() {
                if resp.send(&outgoing).await.is_ok() {
                    active.push(i);
                }
            }

            let mut responses = Vec::new();
            for i in active {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match tokio::time::timeout(remaining, self.respondents[i].recv()).await {
                    Ok(Ok(mut raw)) => {
                        if self.state.process_response(&mut raw).is_ok() {
                            responses.push(raw);
                        }
                    }
                    _ => {}
                }
            }

            Ok(responses)
        }
    }

    /// One-shot handle that allows sending a single response to the active survey.
    pub struct SurveyHandle<'a> {
        transport: &'a mut AnyTransport,
        routing: SurveyRoutingInfo,
    }

    impl<'a> SurveyHandle<'a> {
        /// Send a response to the surveyor.
        pub async fn respond(self, msg: Message) -> io::Result<()> {
            let state = Respondent0State::new();
            let mut outgoing = msg;
            state.prepare_response(&mut outgoing, &self.routing);
            self.transport.send(&outgoing).await
        }
    }

    /// Respondent socket: dials a surveyor, receives surveys, sends responses.
    pub struct Respondent0 {
        transport: AnyTransport,
        state: Respondent0State,
    }

    impl Respondent0 {
        /// Connect to a surveyor at `addr`.
        pub async fn dial(addr: &str) -> io::Result<Self> {
            let transport = connect_transport(addr, ProtocolId::RESPONDENT0).await?;
            Ok(Self {
                transport,
                state: Respondent0State::new(),
            })
        }

        /// Receive the next survey.  Returns the application message and a
        /// `SurveyHandle` that must be used to respond (or dropped to skip).
        pub async fn receive(&mut self) -> io::Result<(Message, SurveyHandle<'_>)> {
            let mut msg = self.transport.recv().await?;
            let routing = self
                .state
                .process_incoming(&mut msg)
                .map_err(|e| io::Error::other(e.to_string()))?;
            Ok((
                msg,
                SurveyHandle {
                    transport: &mut self.transport,
                    routing,
                },
            ))
        }
    }
}

pub mod bus0 {
    //! BUS0 socket API — many-to-many broadcast.
    //!
    //! Every `Bus0` node can broadcast messages to all connected peers and
    //! receive from any of them.  BUS is stateless at the protocol level.
    //!
    //! `recv_any` polls peers in round-robin with cooperative yielding.  It is
    //! safe to cancel between iterations because [`FramedTransport::recv`] is
    //! cancellation-safe: any bytes already read from the stream are preserved
    //! in the transport's internal [`RecvBuf`] and resumed on the next call.
    //! For WebSocket peers, `WsTransport::recv` is also cancellation-safe
    //! because tungstenite reassembles frames internally before yielding.
    //!
    //! [`RecvBuf`]: crate::transport::FramedTransport
    //! [`Pull0Fan`]: super::pipeline0::Pull0Fan

    use std::io;

    use crate::{Message, codec::ProtocolId};

    use super::{AnyListener, AnyTransport, bind_listener, connect_transport};

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
        pub async fn listen(addr: &str) -> io::Result<Self> {
            Self::listen_and_accept(addr, 1).await
        }

        /// Bind and accept `n` peer connections before returning.
        ///
        /// The listener socket is **kept open** so that
        /// [`accept_pending`](Self::accept_pending) can admit peers that
        /// connect after this call returns.
        pub async fn listen_and_accept(addr: &str, n: usize) -> io::Result<Self> {
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
        pub async fn dial(addr: &str) -> io::Result<Self> {
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
        pub async fn broadcast(&mut self, msg: Message) -> io::Result<()> {
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
        pub async fn recv_any(&mut self) -> io::Result<Message> {
            loop {
                if self.peers.is_empty() {
                    return Err(io::Error::other("all peers disconnected"));
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
        pub async fn recv_from(&mut self, peer_idx: usize) -> io::Result<Message> {
            self.peers
                .get_mut(peer_idx)
                .ok_or_else(|| io::Error::other("peer index out of range"))?
                .recv()
                .await
        }

        /// Number of currently connected peers.
        pub fn peer_count(&self) -> usize {
            self.peers.len()
        }
    }
}

pub mod pipeline0 {
    //! PUSH0 / PULL0 socket API (pipeline pattern).
    //!
    //! `Push0` / `Pull0` handle a single connection each.  `Push0Fan` and
    //! `Pull0Fan` handle N connections: `Push0Fan` round-robins outgoing
    //! messages across N pullers; `Pull0Fan` receives from whichever of N
    //! pushers has data available first.

    use std::io;

    use crate::{
        Message,
        codec::ProtocolId,
        protocols::pipeline::{Pull0State, Push0State},
        socket::Socket,
    };

    use super::{AnyTransport, bind_listener};

    /// Push socket: sends messages to a single connected pull endpoint.
    pub struct Push0(Socket<Push0State>);

    impl Push0 {
        pub async fn listen(addr: &str) -> io::Result<Self> {
            Socket::listen(addr, ProtocolId::PUSH0).await.map(Self)
        }

        #[cfg(feature = "wss")]
        pub async fn listen_tls(
            addr: &str,
            cert_pem: &std::path::Path,
            key_pem: &std::path::Path,
        ) -> io::Result<Self> {
            Socket::listen_tls(addr, ProtocolId::PUSH0, cert_pem, key_pem)
                .await
                .map(Self)
        }

        pub async fn dial(addr: &str) -> io::Result<Self> {
            Socket::dial(addr, ProtocolId::PUSH0).await.map(Self)
        }

        pub async fn push(&mut self, msg: Message) -> io::Result<()> {
            self.0.send_raw(&msg).await
        }
    }

    /// Pull socket: receives messages from a single connected push endpoint.
    pub struct Pull0(Socket<Pull0State>);

    impl Pull0 {
        pub async fn listen(addr: &str) -> io::Result<Self> {
            Socket::listen(addr, ProtocolId::PULL0).await.map(Self)
        }

        #[cfg(feature = "wss")]
        pub async fn listen_tls(
            addr: &str,
            cert_pem: &std::path::Path,
            key_pem: &std::path::Path,
        ) -> io::Result<Self> {
            Socket::listen_tls(addr, ProtocolId::PULL0, cert_pem, key_pem)
                .await
                .map(Self)
        }

        pub async fn dial(addr: &str) -> io::Result<Self> {
            Socket::dial(addr, ProtocolId::PULL0).await.map(Self)
        }

        pub async fn pull(&mut self) -> io::Result<Message> {
            self.0.recv_raw().await
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
        pub async fn listen_and_accept(addr: &str, n: usize) -> io::Result<Self> {
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
        pub async fn push(&mut self, msg: Message) -> io::Result<()> {
            if self.workers.is_empty() {
                return Err(io::Error::other("no workers connected"));
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
    /// cancelled mid-read.
    pub struct Pull0Fan {
        rx: tokio::sync::mpsc::Receiver<io::Result<Message>>,
        n: usize,
    }

    impl Pull0Fan {
        /// Bind to `addr` and block until exactly `n` PUSH connections have
        /// completed the handshake.
        pub async fn listen_and_accept(addr: &str, n: usize) -> io::Result<Self> {
            let listener = bind_listener(addr).await?;
            let (tx, rx) = tokio::sync::mpsc::channel(n * 4);
            let mut count = 0;
            while count < n {
                if let Ok(mut transport) = listener.accept_as_transport(ProtocolId::PULL0).await {
                    let tx2 = tx.clone();
                    tokio::spawn(async move {
                        loop {
                            match transport.recv().await {
                                Ok(msg) => {
                                    if tx2.send(Ok(msg)).await.is_err() {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    let _ = tx2.send(Err(e)).await;
                                    break;
                                }
                            }
                        }
                    });
                    count += 1;
                }
            }
            Ok(Self { rx, n })
        }

        /// Receive the next message from any connected sender.
        ///
        /// Skips sender-disconnect errors so callers keep receiving from the
        /// remaining live senders.  Returns `Err` only when all senders are gone.
        pub async fn pull_any(&mut self) -> io::Result<Message> {
            loop {
                match self.rx.recv().await {
                    None => return Err(io::Error::other("all senders disconnected")),
                    Some(Ok(msg)) => return Ok(msg),
                    Some(Err(_)) => {}
                }
            }
        }

        /// Number of senders accepted at construction time.
        pub fn sender_count(&self) -> usize {
            self.n
        }
    }
}

pub mod pair0 {
    //! PAIR0 socket API (bidirectional point-to-point).
    //!
    //! Both ends can send and receive freely with no protocol headers.

    use std::io;

    use crate::{Message, codec::ProtocolId, protocols::pair::Pair0State, socket::Socket};

    /// Pair socket: bidirectional point-to-point messaging.
    pub struct Pair0(Socket<Pair0State>);

    impl Pair0 {
        pub async fn listen(addr: &str) -> io::Result<Self> {
            Socket::listen(addr, ProtocolId::PAIR0).await.map(Self)
        }

        #[cfg(feature = "wss")]
        pub async fn listen_tls(
            addr: &str,
            cert_pem: &std::path::Path,
            key_pem: &std::path::Path,
        ) -> io::Result<Self> {
            Socket::listen_tls(addr, ProtocolId::PAIR0, cert_pem, key_pem)
                .await
                .map(Self)
        }

        pub async fn dial(addr: &str) -> io::Result<Self> {
            Socket::dial(addr, ProtocolId::PAIR0).await.map(Self)
        }

        pub async fn send(&mut self, msg: Message) -> io::Result<()> {
            self.0.send_raw(&msg).await
        }

        pub async fn recv(&mut self) -> io::Result<Message> {
            self.0.recv_raw().await
        }
    }
}

pub mod reqrep0 {
    //! REQ0 / REP0 socket API.

    use std::io;

    use crate::{
        Message,
        codec::ProtocolId,
        protocols::reqrep::{Rep0State, Req0State, RoutingInfo},
        socket::Socket,
    };

    /// Request socket: connects to a reply server, sends requests, awaits replies.
    pub struct Req0(Socket<Req0State>);

    impl Req0 {
        pub async fn dial(addr: &str) -> io::Result<Self> {
            Socket::dial(addr, ProtocolId::REQ0).await.map(Self)
        }

        /// Send `msg` and receive the reply.  The request ID is managed
        /// transparently.
        pub async fn request(&mut self, msg: Message) -> io::Result<Message> {
            let mut state = Req0State::new();

            let mut outgoing = msg;
            let sent_id = state.prepare_outgoing(&mut outgoing);
            self.0.send_raw(&outgoing).await?;

            let mut reply = self.0.recv_raw().await?;
            state
                .process_incoming(&mut reply, sent_id)
                .map_err(|e| io::Error::other(e.to_string()))?;
            Ok(reply)
        }
    }

    /// Reply socket: listens for connections from requesters.
    pub struct Rep0 {
        inner: Socket<Rep0State>,
        state: Rep0State,
    }

    /// Consumed by `reply()` to enforce that each receive has exactly one reply.
    pub struct Responder<'a> {
        socket: &'a mut Socket<Rep0State>,
        routing: RoutingInfo,
    }

    impl<'a> Responder<'a> {
        pub async fn reply(self, msg: Message) -> io::Result<()> {
            let state = Rep0State::new();
            let mut outgoing = msg;
            state.prepare_reply(&mut outgoing, &self.routing);
            self.socket.send_raw(&outgoing).await
        }
    }

    impl Rep0 {
        pub async fn listen(addr: &str) -> io::Result<Self> {
            Socket::listen(addr, ProtocolId::REP0).await.map(|s| Self {
                inner: s,
                state: Rep0State::new(),
            })
        }

        #[cfg(feature = "wss")]
        pub async fn listen_tls(
            addr: &str,
            cert_pem: &std::path::Path,
            key_pem: &std::path::Path,
        ) -> io::Result<Self> {
            Socket::listen_tls(addr, ProtocolId::REP0, cert_pem, key_pem)
                .await
                .map(|s| Self {
                    inner: s,
                    state: Rep0State::new(),
                })
        }

        /// Receive the next request.  Returns the application message plus a
        /// `Responder` that must be used to send the reply.
        pub async fn receive(&mut self) -> io::Result<(Message, Responder<'_>)> {
            let mut msg = self.inner.recv_raw().await?;
            let routing = self
                .state
                .process_incoming(&mut msg)
                .map_err(|e| io::Error::other(e.to_string()))?;
            Ok((
                msg,
                Responder {
                    socket: &mut self.inner,
                    routing,
                },
            ))
        }
    }
}
