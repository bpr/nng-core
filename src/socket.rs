//! High-level socket API for nng-core (requires `std` / tokio).
//!
//! Supported URL schemes:
//! - `tcp://host:port` — plain TCP
//! - `ipc:///path` — Unix domain socket, NNG 1.5.x framing (9-byte header)
//! - `ipc2:///path` — Unix domain socket, NNG 2.x framing (8-byte header, same as TCP)
//! - `ws://host:port[/path]` — WebSocket (requires `ws` feature)
//! - `wss://host:port[/path]` — Secure WebSocket / TLS (requires `wss` feature)
//! - `tls+tcp://host:port` — TLS over TCP (requires `tls-tcp` feature)
//! - `udp://host:port` — UDP datagram (requires `udp` feature; no SP handshake)
//!
//! Each socket type wraps an `AnyTransport`, which dispatches to either
//! `FramedTransport` (TCP / IPC) or `WsTransport` (WebSocket / TLS)
//! depending on the URL scheme used at construction time.

use std::io;

use tokio::net::{TcpListener, TcpStream};

use embedded_io_async::{ErrorType, Read as EioRead, Write as EioWrite};

use crate::{
    Message,
    codec::ProtocolId,
    error::NngError,
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

#[cfg(feature = "tls-tcp")]
use crate::transport::tls_tcp::{
    TlsTcpStream, build_tls_acceptor as build_tls_tcp_acceptor, build_tls_connector,
};

#[cfg(feature = "udp")]
use crate::transport::udp::UdpTransport;

// ── AnyTransport: FramedTransport or WsTransport ─────────────────────────────

pub(crate) enum AnyTransport {
    Framed(FramedTransport<AnyStream>),
    #[cfg(feature = "ws")]
    Ws(WsTransport),
    #[cfg(feature = "udp")]
    Udp(UdpTransport),
}

impl AnyTransport {
    pub(crate) async fn send(&mut self, msg: &Message) -> Result<(), NngError> {
        match self {
            Self::Framed(t) => t.send(msg).await.map_err(NngError::from),
            #[cfg(feature = "ws")]
            Self::Ws(t) => t.send(msg).await.map_err(ws_err_to_nng),
            #[cfg(feature = "udp")]
            Self::Udp(t) => t.send(msg).await.map_err(NngError::from),
        }
    }

    pub(crate) async fn recv(&mut self) -> Result<Message, NngError> {
        match self {
            Self::Framed(t) => t.recv().await.map_err(NngError::from),
            #[cfg(feature = "ws")]
            Self::Ws(t) => t.recv().await.map_err(ws_err_to_nng),
            #[cfg(feature = "udp")]
            Self::Udp(t) => t.recv().await.map_err(NngError::from),
        }
    }
}

#[cfg(feature = "ws")]
fn ws_err_to_nng(e: WsError) -> NngError {
    use crate::transport::ws::WsError;
    match e {
        WsError::Closed => NngError::ConnectionClosed,
        WsError::SubprotocolMismatch { expected, got } => NngError::ProtocolViolation(format!(
            "WebSocket subprotocol mismatch: expected {expected:?}, got {got:?}"
        )),
        WsError::InvalidHeader(s) => {
            NngError::ProtocolViolation(format!("invalid WebSocket header: {s}"))
        }
        WsError::Tungstenite(e) => NngError::Io(io::Error::other(e.to_string())),
        #[cfg(feature = "wss")]
        WsError::Tls(s) => NngError::Io(io::Error::other(format!("TLS error: {s}"))),
    }
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
    /// TLS over TCP: 8-byte frame header, TLS socket layer.
    #[cfg(feature = "tls-tcp")]
    TlsTcp(TlsTcpStream),
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
            #[cfg(feature = "tls-tcp")]
            Self::TlsTcp(s) => EioRead::read(s, buf).await,
        }
    }
}

impl EioWrite for AnyStream {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        match self {
            Self::Tcp(s) => EioWrite::write(s, buf).await,
            #[cfg(unix)]
            Self::Ipc(s) | Self::Ipc2(s) => EioWrite::write(s, buf).await,
            #[cfg(feature = "tls-tcp")]
            Self::TlsTcp(s) => EioWrite::write(s, buf).await,
        }
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        match self {
            Self::Tcp(s) => EioWrite::flush(s).await,
            #[cfg(unix)]
            Self::Ipc(s) | Self::Ipc2(s) => EioWrite::flush(s).await,
            #[cfg(feature = "tls-tcp")]
            Self::TlsTcp(s) => EioWrite::flush(s).await,
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
            #[cfg(feature = "tls-tcp")]
            Self::TlsTcp(_) => FrameFormat::Tcp,
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
    /// TLS over TCP: 8-byte frame header, TLS socket layer.
    #[cfg(feature = "tls-tcp")]
    TlsTcp(TcpListener, tokio_rustls::TlsAcceptor),
}

impl AnyListener {
    /// Accept one connection and complete the SP handshake (TCP/IPC) or
    /// WebSocket upgrade (ws://), returning a ready-to-use `AnyTransport`.
    pub(crate) async fn accept_as_transport(
        &self,
        proto: ProtocolId,
    ) -> Result<AnyTransport, NngError> {
        match self {
            Self::Tcp(l) => {
                let (stream, _) = l.accept().await?;
                stream.set_nodelay(true).map_err(NngError::from)?;
                connect_framed(AnyStream::Tcp(TokioTcpStream(stream)), proto)
                    .await
                    .map(AnyTransport::Framed)
                    .map_err(NngError::from)
            }
            #[cfg(unix)]
            Self::Ipc(l) => {
                let (stream, _) = l.accept().await?;
                connect_framed(AnyStream::Ipc(TokioUnixStream(stream)), proto)
                    .await
                    .map(AnyTransport::Framed)
                    .map_err(NngError::from)
            }
            #[cfg(unix)]
            Self::Ipc2(l) => {
                let (stream, _) = l.accept().await?;
                connect_framed(AnyStream::Ipc2(TokioUnixStream(stream)), proto)
                    .await
                    .map(AnyTransport::Framed)
                    .map_err(NngError::from)
            }
            #[cfg(feature = "ws")]
            Self::Ws(l) => {
                let (tcp, _) = l.accept().await?;
                WsTransport::accept(tcp, proto)
                    .await
                    .map(AnyTransport::Ws)
                    .map_err(ws_err_to_nng)
            }
            #[cfg(feature = "wss")]
            Self::Wss(l, acceptor) => {
                let (tcp, _) = l.accept().await?;
                WsTransport::accept_tls_with_acceptor(tcp, proto, acceptor)
                    .await
                    .map(AnyTransport::Ws)
                    .map_err(ws_err_to_nng)
            }
            #[cfg(feature = "tls-tcp")]
            Self::TlsTcp(l, acceptor) => {
                let (tcp, _) = l.accept().await?;
                let tls = acceptor.accept(tcp).await.map_err(io::Error::other)?;
                connect_framed(AnyStream::TlsTcp(TlsTcpStream::Server(tls)), proto)
                    .await
                    .map(AnyTransport::Framed)
                    .map_err(NngError::from)
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
                stream.set_nodelay(true)?;
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
            #[cfg(feature = "tls-tcp")]
            Self::TlsTcp(l, acceptor) => {
                let (tcp, _) = l.accept().await?;
                Ok(RawConn::TlsTcp(tcp, acceptor.clone()))
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
    #[cfg(feature = "tls-tcp")]
    TlsTcp(TcpStream, tokio_rustls::TlsAcceptor),
}

impl RawConn {
    /// Complete the protocol handshake or WebSocket upgrade and return a
    /// ready-to-use transport.
    pub(crate) async fn into_transport(self, proto: ProtocolId) -> Result<AnyTransport, NngError> {
        match self {
            Self::Stream(s) => connect_framed(s, proto)
                .await
                .map(AnyTransport::Framed)
                .map_err(NngError::from),
            #[cfg(feature = "ws")]
            Self::Ws(tcp) => WsTransport::accept(tcp, proto)
                .await
                .map(AnyTransport::Ws)
                .map_err(ws_err_to_nng),
            #[cfg(feature = "wss")]
            Self::Wss(tcp, acceptor) => {
                WsTransport::accept_tls_with_acceptor(tcp, proto, &acceptor)
                    .await
                    .map(AnyTransport::Ws)
                    .map_err(ws_err_to_nng)
            }
            #[cfg(feature = "tls-tcp")]
            Self::TlsTcp(tcp, acceptor) => {
                let tls = acceptor.accept(tcp).await.map_err(io::Error::other)?;
                connect_framed(AnyStream::TlsTcp(TlsTcpStream::Server(tls)), proto)
                    .await
                    .map(AnyTransport::Framed)
                    .map_err(NngError::from)
            }
        }
    }
}

// ── URL dispatch helpers ──────────────────────────────────────────────────────

pub(crate) async fn bind_listener(addr: &str) -> Result<AnyListener, NngError> {
    if let Some(tcp_addr) = addr.strip_prefix("tcp://") {
        TcpListener::bind(tcp_addr)
            .await
            .map(AnyListener::Tcp)
            .map_err(NngError::from)
    } else if let Some(ipc_path) = addr.strip_prefix("ipc://") {
        bind_ipc_listener(ipc_path).map_err(NngError::from)
    } else if let Some(ipc_path) = addr.strip_prefix("ipc2://") {
        bind_ipc2_listener(ipc_path).map_err(NngError::from)
    } else if let Some(ws_addr) = addr.strip_prefix("ws://") {
        #[cfg(feature = "ws")]
        {
            // Bind to host:port only; ignore any path component in the URL.
            let host_port = ws_addr.split('/').next().unwrap_or(ws_addr);
            return TcpListener::bind(host_port)
                .await
                .map(AnyListener::Ws)
                .map_err(NngError::from);
        }
        #[cfg(not(feature = "ws"))]
        {
            let _ = ws_addr;
            return Err(NngError::FeatureNotEnabled("ws"));
        }
    } else if addr.starts_with("wss://") {
        Err(NngError::ProtocolViolation(
            "wss:// requires listen_tls — use listen_tls(addr, cert_pem, key_pem)".into(),
        ))
    } else if addr.starts_with("tls+tcp://") {
        Err(NngError::ProtocolViolation(
            "tls+tcp:// requires listen_tls_tcp — use listen_tls_tcp(addr, cert_pem, key_pem)"
                .into(),
        ))
    } else if addr.starts_with("udp://") {
        Err(NngError::UnsupportedScheme(
            "udp:// is not supported for multi-connection sockets".into(),
        ))
    } else {
        Err(NngError::UnsupportedScheme(addr.to_owned()))
    }
}

/// Bind a `tls+tcp://` listener.
///
/// Loads the certificate chain and private key from PEM files once; the
/// resulting `TlsAcceptor` is reused for every incoming connection.
#[cfg(feature = "tls-tcp")]
pub(crate) async fn bind_listener_tls_tcp(
    addr: &str,
    cert_pem: &std::path::Path,
    key_pem: &std::path::Path,
) -> io::Result<AnyListener> {
    let host_port = addr
        .strip_prefix("tls+tcp://")
        .ok_or_else(|| io::Error::other("bind_listener_tls_tcp requires a tls+tcp:// URL"))?
        .split('/')
        .next()
        .unwrap_or(addr);
    let acceptor = build_tls_tcp_acceptor(cert_pem, key_pem)?;
    let listener = TcpListener::bind(host_port).await?;
    Ok(AnyListener::TlsTcp(listener, acceptor))
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

pub(crate) async fn connect_stream(addr: &str) -> Result<AnyStream, NngError> {
    if let Some(tcp_addr) = addr.strip_prefix("tcp://") {
        TcpStream::connect(tcp_addr)
            .await
            .and_then(|s| {
                s.set_nodelay(true)?;
                Ok(s)
            })
            .map(|s| AnyStream::Tcp(TokioTcpStream(s)))
            .map_err(NngError::from)
    } else if let Some(ipc_path) = addr.strip_prefix("ipc://") {
        connect_ipc_stream(ipc_path).await.map_err(NngError::from)
    } else if let Some(ipc_path) = addr.strip_prefix("ipc2://") {
        connect_ipc2_stream(ipc_path).await.map_err(NngError::from)
    } else if let Some(tls_addr) = addr.strip_prefix("tls+tcp://") {
        #[cfg(feature = "tls-tcp")]
        {
            return connect_tls_tcp_stream(tls_addr)
                .await
                .map_err(NngError::from);
        }
        #[cfg(not(feature = "tls-tcp"))]
        {
            let _ = tls_addr;
            return Err(NngError::FeatureNotEnabled("tls-tcp"));
        }
    } else {
        Err(NngError::UnsupportedScheme(addr.to_owned()))
    }
}

/// Dial a URL and return a ready `AnyTransport`.
///
/// Handles `ws://` via [`WsTransport::connect`]; all other schemes go through
/// [`connect_stream`] + [`connect_framed`].
pub(crate) async fn connect_transport(
    addr: &str,
    proto: ProtocolId,
) -> Result<AnyTransport, NngError> {
    #[cfg(feature = "ws")]
    if addr.starts_with("ws://") {
        return WsTransport::connect(addr, proto)
            .await
            .map(AnyTransport::Ws)
            .map_err(ws_err_to_nng);
    }
    #[cfg(feature = "wss")]
    if addr.starts_with("wss://") {
        return WsTransport::connect(addr, proto)
            .await
            .map(AnyTransport::Ws)
            .map_err(ws_err_to_nng);
    }
    let stream = connect_stream(addr).await?;
    connect_framed(stream, proto)
        .await
        .map(AnyTransport::Framed)
        .map_err(NngError::from)
}

pub(crate) async fn connect_framed(
    stream: AnyStream,
    proto: ProtocolId,
) -> Result<FramedTransport<AnyStream>, TransportError> {
    let format = stream.frame_format();
    FramedTransport::connect(stream, proto, format).await
}

#[cfg(feature = "tls-tcp")]
async fn connect_tls_tcp_stream(host_port: &str) -> io::Result<AnyStream> {
    let connector = build_tls_connector()?;
    connect_tls_tcp_stream_with(host_port, connector).await
}

#[cfg(feature = "tls-tcp")]
async fn connect_tls_tcp_stream_with(
    host_port: &str,
    connector: tokio_rustls::TlsConnector,
) -> io::Result<AnyStream> {
    // Strip any path component (e.g. tls+tcp://host:port/ignored).
    let addr = host_port.split('/').next().unwrap_or(host_port);
    // Extract host for TLS SNI — everything before the last ':'.
    let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(addr);
    let server_name = rustls::pki_types::ServerName::try_from(host.to_owned())
        .map_err(|_| io::Error::other(format!("invalid TLS server name: {host}")))?;
    let tcp = TcpStream::connect(addr).await?;
    let tls = connector.connect(server_name, tcp).await?;
    Ok(AnyStream::TlsTcp(TlsTcpStream::Client(tls)))
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

// ── Reconnect ────────────────────────────────────────────────────────────────

/// Exponential-backoff configuration for automatic redial on connection loss.
///
/// Pass to [`Socket::dial_with_reconnect`] or the typed-socket equivalents
/// (e.g. [`reqrep0::Req0::dial_reconnecting`]) to enable transparent reconnect.
///
/// # Backoff schedule
///
/// After the first send/recv failure the transport is torn down and
/// reconnect attempts begin.  The first attempt fires after `min_backoff`;
/// each subsequent failure doubles the delay up to `max_backoff`.  When
/// `max_attempts` is `None` the loop runs until a connection is re-established.
#[derive(Clone, Debug)]
pub struct ReconnectOptions {
    /// Delay before the first reconnect attempt. Default: 100 ms.
    pub min_backoff: std::time::Duration,
    /// Upper bound on the inter-attempt delay. Default: 30 s.
    pub max_backoff: std::time::Duration,
    /// Maximum number of attempts before returning an error.
    /// `None` (the default) retries indefinitely.
    pub max_attempts: Option<u32>,
}

impl Default for ReconnectOptions {
    fn default() -> Self {
        Self {
            min_backoff: std::time::Duration::from_millis(100),
            max_backoff: std::time::Duration::from_secs(30),
            max_attempts: None,
        }
    }
}

/// Reconnect `transport` to `addr` with exponential backoff as described by
/// `opts`.  Replaces `*transport` in-place on success.
pub(crate) async fn reconnect_transport(
    transport: &mut AnyTransport,
    addr: &str,
    proto: ProtocolId,
    opts: &ReconnectOptions,
) -> Result<(), NngError> {
    let mut backoff = opts.min_backoff;
    let mut attempts = 0u32;
    loop {
        tokio::time::sleep(backoff).await;
        match connect_transport(addr, proto).await {
            Ok(t) => {
                *transport = t;
                return Ok(());
            }
            Err(_) => {
                attempts += 1;
                if let Some(max) = opts.max_attempts {
                    if attempts >= max {
                        return Err(NngError::ReconnectExhausted);
                    }
                }
                backoff = (backoff * 2).min(opts.max_backoff);
            }
        }
    }
}

// ── Socket<P> ────────────────────────────────────────────────────────────────

/// A connected socket wrapping a single transport (TCP, IPC, or WebSocket).
pub struct Socket<P> {
    transport: AnyTransport,
    proto: ProtocolId,
    dial_addr: Option<String>,
    reconnect: Option<ReconnectOptions>,
    _protocol: core::marker::PhantomData<P>,
}

impl<P> Socket<P> {
    fn new(transport: AnyTransport, proto: ProtocolId) -> Self {
        Self {
            transport,
            proto,
            dial_addr: None,
            reconnect: None,
            _protocol: core::marker::PhantomData,
        }
    }

    fn new_reconnecting(
        transport: AnyTransport,
        proto: ProtocolId,
        addr: String,
        opts: ReconnectOptions,
    ) -> Self {
        Self {
            transport,
            proto,
            dial_addr: Some(addr),
            reconnect: Some(opts),
            _protocol: core::marker::PhantomData,
        }
    }

    async fn do_reconnect(&mut self) -> Result<(), NngError> {
        let (addr, opts) = match (&self.dial_addr, &self.reconnect) {
            (Some(a), Some(o)) => (a.clone(), o.clone()),
            _ => return Err(NngError::Io(io::Error::other("reconnect not configured"))),
        };
        reconnect_transport(&mut self.transport, &addr, self.proto, &opts).await
    }

    /// Bind and wait for the first incoming connection, then complete the
    /// handshake.  The listener is dropped after accepting one connection.
    ///
    /// For `udp://` URLs the socket is bound immediately and returned without
    /// waiting; UDP has no connection concept and no SP handshake.
    pub async fn listen(addr: &str, proto: ProtocolId) -> Result<Self, NngError> {
        #[cfg(feature = "udp")]
        if let Some(udp_addr) = addr.strip_prefix("udp://") {
            let transport = UdpTransport::bind(udp_addr).await?;
            return Ok(Self::new(AnyTransport::Udp(transport), proto));
        }
        let listener = bind_listener(addr).await?;
        let transport = listener.accept_as_transport(proto).await?;
        Ok(Self::new(transport, proto))
    }

    /// Bind a `tls+tcp://` listener and wait for the first connection.
    ///
    /// Loads the server certificate and private key from PEM files once,
    /// then performs the TLS handshake on the first accepted connection and
    /// runs the SP framing handshake over the encrypted stream.
    #[cfg(feature = "tls-tcp")]
    pub async fn listen_tls_tcp(
        addr: &str,
        proto: ProtocolId,
        cert_pem: &std::path::Path,
        key_pem: &std::path::Path,
    ) -> Result<Self, NngError> {
        let listener = bind_listener_tls_tcp(addr, cert_pem, key_pem).await?;
        let transport = listener.accept_as_transport(proto).await?;
        Ok(Self::new(transport, proto))
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
    ) -> Result<Self, NngError> {
        let listener = bind_listener_tls(addr, cert_pem, key_pem).await?;
        let transport = listener.accept_as_transport(proto).await?;
        Ok(Self::new(transport, proto))
    }

    /// Dial a `tls+tcp://` server with a custom `ClientConfig`.
    ///
    /// Use this when the server presents a self-signed certificate (e.g. in
    /// tests).  For production servers with public CA certificates, use
    /// [`dial`](Self::dial) with a `tls+tcp://` URL, which uses the system's
    /// native root certificate store.
    #[cfg(feature = "tls-tcp")]
    pub async fn dial_tls_tcp(
        addr: &str,
        proto: ProtocolId,
        client_config: std::sync::Arc<rustls::ClientConfig>,
    ) -> Result<Self, NngError> {
        let host_port = addr.strip_prefix("tls+tcp://").ok_or_else(|| {
            NngError::Io(io::Error::other("dial_tls_tcp requires a tls+tcp:// URL"))
        })?;
        let connector = tokio_rustls::TlsConnector::from(client_config);
        let stream = connect_tls_tcp_stream_with(host_port, connector).await?;
        connect_framed(stream, proto)
            .await
            .map(|t| Self::new(AnyTransport::Framed(t), proto))
            .map_err(NngError::from)
    }

    /// Connect to `addr` and complete the handshake.
    ///
    /// For `udp://` URLs the socket is bound to an ephemeral port and
    /// UDP-connected to the peer; no SP handshake is performed.
    pub async fn dial(addr: &str, proto: ProtocolId) -> Result<Self, NngError> {
        #[cfg(feature = "udp")]
        if let Some(udp_addr) = addr.strip_prefix("udp://") {
            let transport = UdpTransport::connect(udp_addr).await?;
            return Ok(Self::new(AnyTransport::Udp(transport), proto));
        }
        let transport = connect_transport(addr, proto).await?;
        Ok(Self::new(transport, proto))
    }

    /// Connect to `addr` and configure automatic reconnect with default backoff.
    ///
    /// Equivalent to `dial_with_reconnect(addr, proto, ReconnectOptions::default())`.
    pub async fn dial_reconnecting(addr: &str, proto: ProtocolId) -> Result<Self, NngError> {
        Self::dial_with_reconnect(addr, proto, ReconnectOptions::default()).await
    }

    /// Connect to `addr` and configure automatic reconnect with `opts`.
    ///
    /// On any send or receive failure the transport is torn down and redialled
    /// with exponential backoff as described by `opts`.
    pub async fn dial_with_reconnect(
        addr: &str,
        proto: ProtocolId,
        opts: ReconnectOptions,
    ) -> Result<Self, NngError> {
        let transport = connect_transport(addr, proto).await?;
        Ok(Self::new_reconnecting(
            transport,
            proto,
            addr.to_owned(),
            opts,
        ))
    }

    pub async fn send_raw(&mut self, msg: &Message) -> Result<(), NngError> {
        match self.transport.send(msg).await {
            Ok(()) => Ok(()),
            Err(e) if self.reconnect.is_some() => match self.do_reconnect().await {
                Ok(()) => self.transport.send(msg).await,
                Err(_) => Err(e),
            },
            Err(e) => Err(e),
        }
    }

    pub async fn recv_raw(&mut self) -> Result<Message, NngError> {
        match self.transport.recv().await {
            Ok(msg) => Ok(msg),
            Err(e) if self.reconnect.is_some() => match self.do_reconnect().await {
                Ok(()) => self.transport.recv().await,
                Err(_) => Err(e),
            },
            Err(e) => Err(e),
        }
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
        ) -> std::pin::Pin<Box<dyn futures_sink::Sink<Message, Error = crate::NngError> + Send>>
        {
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
        pub async fn dial_with_reconnect(
            addr: &str,
            opts: ReconnectOptions,
        ) -> Result<Self, NngError> {
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
        ) -> std::pin::Pin<
            Box<dyn futures_core::Stream<Item = Result<Message, crate::NngError>> + Send>,
        > {
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
                            reconnect_transport(
                                &mut self.transport,
                                &addr,
                                ProtocolId::SUB0,
                                &opts,
                            )
                            .await?;
                        } else {
                            return Err(e);
                        }
                    }
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

    use std::time::Duration;

    use crate::{
        Message,
        codec::ProtocolId,
        protocols::survey::{Respondent0State, SurveyRoutingInfo, Surveyor0State},
    };

    use super::{
        AnyListener, AnyTransport, NngError, ReconnectOptions, bind_listener, connect_transport,
        reconnect_transport,
    };

    /// Surveyor socket: broadcasts surveys to multiple respondents.
    pub struct Surveyor0 {
        listener: AnyListener,
        respondents: Vec<AnyTransport>,
        state: Surveyor0State,
        /// Deadline given to each `survey` call.  Matches NNG's `NNG_OPT_SURVEYOR_SURVEYTIME`.
        survey_time: Duration,
    }

    impl Surveyor0 {
        /// Bind and start accepting respondent connections.
        pub async fn listen(addr: &str) -> Result<Self, NngError> {
            let listener = bind_listener(addr).await?;
            Ok(Self {
                listener,
                respondents: Vec::new(),
                state: Surveyor0State::new(),
                survey_time: Duration::from_secs(1),
            })
        }

        /// Set the default survey deadline used by `survey()`.
        ///
        /// Equivalent to NNG's `NNG_OPT_SURVEYOR_SURVEYTIME`.  Defaults to 1 second.
        pub fn set_survey_time(&mut self, d: Duration) {
            self.survey_time = d;
        }

        /// Block until at least `n` respondents have connected.
        pub async fn wait_for_respondents(&mut self, n: usize) -> Result<(), NngError> {
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
        /// `self.survey_time`.  Returns the application-level response messages.
        pub async fn survey(&mut self, msg: Message) -> Result<Vec<Message>, NngError> {
            self.survey_with_timeout(msg, self.survey_time).await
        }

        /// Broadcast `msg` as a survey; collect all responses arriving within
        /// `timeout`.  All respondents are polled **concurrently** — a slow
        /// respondent does not starve the others.
        pub async fn survey_with_timeout(
            &mut self,
            msg: Message,
            timeout: Duration,
        ) -> Result<Vec<Message>, NngError> {
            let mut outgoing = msg;
            self.state.prepare_survey(&mut outgoing);

            let deadline = tokio::time::Instant::now() + timeout;

            let mut pending: Vec<usize> = Vec::new();
            for (i, resp) in self.respondents.iter_mut().enumerate() {
                if resp.send(&outgoing).await.is_ok() {
                    pending.push(i);
                }
            }

            let mut responses = Vec::new();

            // Poll all pending respondents concurrently within the shared deadline.
            // FramedTransport::recv is cancellation-safe (state stored in RecvBuf),
            // so dropping a half-polled future and retrying is correct.
            'outer: loop {
                if pending.is_empty() {
                    break;
                }

                let mut still_pending = Vec::new();
                for &i in &pending {
                    let result = tokio::select! {
                        biased;
                        r = self.respondents[i].recv() => Some(r),
                        _ = std::future::ready(()) => None,
                    };
                    match result {
                        Some(Ok(mut raw)) => {
                            if self.state.process_response(&mut raw).is_ok() {
                                responses.push(raw);
                            }
                        }
                        Some(Err(_)) => {}
                        None => still_pending.push(i),
                    }
                }
                pending = still_pending;

                if pending.is_empty() {
                    break;
                }

                // Yield once so the runtime can service I/O, then check deadline.
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(deadline) => break 'outer,
                    _ = tokio::task::yield_now() => continue,
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
        pub async fn respond(self, msg: Message) -> Result<(), NngError> {
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
        dial_addr: Option<String>,
        reconnect: Option<ReconnectOptions>,
    }

    impl Respondent0 {
        /// Connect to a surveyor at `addr`.
        pub async fn dial(addr: &str) -> Result<Self, NngError> {
            let transport = connect_transport(addr, ProtocolId::RESPONDENT0).await?;
            Ok(Self {
                transport,
                state: Respondent0State::new(),
                dial_addr: None,
                reconnect: None,
            })
        }

        /// Dial with automatic reconnect using default backoff (100 ms → 30 s).
        pub async fn dial_reconnecting(addr: &str) -> Result<Self, NngError> {
            Self::dial_with_reconnect(addr, ReconnectOptions::default()).await
        }

        /// Dial with automatic reconnect using custom `ReconnectOptions`.
        pub async fn dial_with_reconnect(
            addr: &str,
            opts: ReconnectOptions,
        ) -> Result<Self, NngError> {
            let transport = connect_transport(addr, ProtocolId::RESPONDENT0).await?;
            Ok(Self {
                transport,
                state: Respondent0State::new(),
                dial_addr: Some(addr.to_owned()),
                reconnect: Some(opts),
            })
        }

        /// Receive the next survey.  Returns the application message and a
        /// `SurveyHandle` that must be used to respond (or dropped to skip).
        pub async fn receive(&mut self) -> Result<(Message, SurveyHandle<'_>), NngError> {
            let mut msg = loop {
                match self.transport.recv().await {
                    Ok(m) => break m,
                    Err(e) => {
                        if let (Some(addr), Some(opts)) = (&self.dial_addr, &self.reconnect) {
                            let addr = addr.clone();
                            let opts = opts.clone();
                            reconnect_transport(
                                &mut self.transport,
                                &addr,
                                ProtocolId::RESPONDENT0,
                                &opts,
                            )
                            .await?;
                        } else {
                            return Err(e);
                        }
                    }
                }
            };
            let routing = self
                .state
                .process_incoming(&mut msg)
                .map_err(|e| NngError::ProtocolViolation(e.to_string()))?;
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
    }
}

pub mod pipeline0 {
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

    use super::{AnyTransport, bind_listener};

    /// Push socket: sends messages to a single connected pull endpoint.
    pub struct Push0(Socket<Push0State>);

    impl Push0 {
        pub async fn listen(addr: &str) -> Result<Self, NngError> {
            Socket::listen(addr, ProtocolId::PUSH0).await.map(Self)
        }

        #[cfg(feature = "wss")]
        pub async fn listen_tls(
            addr: &str,
            cert_pem: &std::path::Path,
            key_pem: &std::path::Path,
        ) -> Result<Self, NngError> {
            Socket::listen_tls(addr, ProtocolId::PUSH0, cert_pem, key_pem)
                .await
                .map(Self)
        }

        #[cfg(feature = "tls-tcp")]
        pub async fn listen_tls_tcp(
            addr: &str,
            cert_pem: &std::path::Path,
            key_pem: &std::path::Path,
        ) -> Result<Self, NngError> {
            Socket::listen_tls_tcp(addr, ProtocolId::PUSH0, cert_pem, key_pem)
                .await
                .map(Self)
        }

        #[cfg(feature = "tls-tcp")]
        pub async fn dial_tls_tcp(
            addr: &str,
            client_config: std::sync::Arc<rustls::ClientConfig>,
        ) -> Result<Self, NngError> {
            Socket::dial_tls_tcp(addr, ProtocolId::PUSH0, client_config)
                .await
                .map(Self)
        }

        pub async fn dial(addr: &str) -> Result<Self, NngError> {
            Socket::dial(addr, ProtocolId::PUSH0).await.map(Self)
        }

        /// Dial with automatic reconnect using default backoff (100 ms → 30 s).
        pub async fn dial_reconnecting(addr: &str) -> Result<Self, NngError> {
            Socket::dial_reconnecting(addr, ProtocolId::PUSH0)
                .await
                .map(Self)
        }

        /// Dial with automatic reconnect using custom `ReconnectOptions`.
        pub async fn dial_with_reconnect(
            addr: &str,
            opts: ReconnectOptions,
        ) -> Result<Self, NngError> {
            Socket::dial_with_reconnect(addr, ProtocolId::PUSH0, opts)
                .await
                .map(Self)
        }

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
        ) -> std::pin::Pin<Box<dyn futures_sink::Sink<Message, Error = crate::NngError> + Send>>
        {
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
        pub async fn listen(addr: &str) -> Result<Self, NngError> {
            Socket::listen(addr, ProtocolId::PULL0).await.map(Self)
        }

        #[cfg(feature = "wss")]
        pub async fn listen_tls(
            addr: &str,
            cert_pem: &std::path::Path,
            key_pem: &std::path::Path,
        ) -> Result<Self, NngError> {
            Socket::listen_tls(addr, ProtocolId::PULL0, cert_pem, key_pem)
                .await
                .map(Self)
        }

        #[cfg(feature = "tls-tcp")]
        pub async fn listen_tls_tcp(
            addr: &str,
            cert_pem: &std::path::Path,
            key_pem: &std::path::Path,
        ) -> Result<Self, NngError> {
            Socket::listen_tls_tcp(addr, ProtocolId::PULL0, cert_pem, key_pem)
                .await
                .map(Self)
        }

        #[cfg(feature = "tls-tcp")]
        pub async fn dial_tls_tcp(
            addr: &str,
            client_config: std::sync::Arc<rustls::ClientConfig>,
        ) -> Result<Self, NngError> {
            Socket::dial_tls_tcp(addr, ProtocolId::PULL0, client_config)
                .await
                .map(Self)
        }

        pub async fn dial(addr: &str) -> Result<Self, NngError> {
            Socket::dial(addr, ProtocolId::PULL0).await.map(Self)
        }

        /// Dial with automatic reconnect using default backoff (100 ms → 30 s).
        pub async fn dial_reconnecting(addr: &str) -> Result<Self, NngError> {
            Socket::dial_reconnecting(addr, ProtocolId::PULL0)
                .await
                .map(Self)
        }

        /// Dial with automatic reconnect using custom `ReconnectOptions`.
        pub async fn dial_with_reconnect(
            addr: &str,
            opts: ReconnectOptions,
        ) -> Result<Self, NngError> {
            Socket::dial_with_reconnect(addr, ProtocolId::PULL0, opts)
                .await
                .map(Self)
        }

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
        ) -> std::pin::Pin<
            Box<dyn futures_core::Stream<Item = Result<Message, crate::NngError>> + Send>,
        > {
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
    /// cancelled mid-read.
    pub struct Pull0Fan {
        rx: tokio::sync::mpsc::Receiver<Result<Message, NngError>>,
        n: usize,
    }

    impl Pull0Fan {
        /// Bind to `addr` and block until exactly `n` PUSH connections have
        /// completed the handshake.
        pub async fn listen_and_accept(addr: &str, n: usize) -> Result<Self, NngError> {
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
        pub async fn pull_any(&mut self) -> Result<Message, NngError> {
            loop {
                match self.rx.recv().await {
                    None => return Err(NngError::NoPeers),
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
}

pub mod pair0 {
    //! PAIR0 socket API (bidirectional point-to-point).
    //!
    //! Both ends can send and receive freely with no protocol headers.

    use crate::{
        Message,
        codec::ProtocolId,
        protocols::pair::Pair0State,
        socket::{NngError, ReconnectOptions, Socket},
    };

    /// Pair socket: bidirectional point-to-point messaging.
    pub struct Pair0(Socket<Pair0State>);

    impl Pair0 {
        pub async fn listen(addr: &str) -> Result<Self, NngError> {
            Socket::listen(addr, ProtocolId::PAIR0).await.map(Self)
        }

        #[cfg(feature = "wss")]
        pub async fn listen_tls(
            addr: &str,
            cert_pem: &std::path::Path,
            key_pem: &std::path::Path,
        ) -> Result<Self, NngError> {
            Socket::listen_tls(addr, ProtocolId::PAIR0, cert_pem, key_pem)
                .await
                .map(Self)
        }

        #[cfg(feature = "tls-tcp")]
        pub async fn listen_tls_tcp(
            addr: &str,
            cert_pem: &std::path::Path,
            key_pem: &std::path::Path,
        ) -> Result<Self, NngError> {
            Socket::listen_tls_tcp(addr, ProtocolId::PAIR0, cert_pem, key_pem)
                .await
                .map(Self)
        }

        #[cfg(feature = "tls-tcp")]
        pub async fn dial_tls_tcp(
            addr: &str,
            client_config: std::sync::Arc<rustls::ClientConfig>,
        ) -> Result<Self, NngError> {
            Socket::dial_tls_tcp(addr, ProtocolId::PAIR0, client_config)
                .await
                .map(Self)
        }

        pub async fn dial(addr: &str) -> Result<Self, NngError> {
            Socket::dial(addr, ProtocolId::PAIR0).await.map(Self)
        }

        /// Dial with automatic reconnect using default backoff (100 ms → 30 s).
        pub async fn dial_reconnecting(addr: &str) -> Result<Self, NngError> {
            Socket::dial_reconnecting(addr, ProtocolId::PAIR0)
                .await
                .map(Self)
        }

        /// Dial with automatic reconnect using custom `ReconnectOptions`.
        pub async fn dial_with_reconnect(
            addr: &str,
            opts: ReconnectOptions,
        ) -> Result<Self, NngError> {
            Socket::dial_with_reconnect(addr, ProtocolId::PAIR0, opts)
                .await
                .map(Self)
        }

        pub async fn send(&mut self, msg: Message) -> Result<(), NngError> {
            self.0.send_raw(&msg).await
        }

        pub async fn recv(&mut self) -> Result<Message, NngError> {
            self.0.recv_raw().await
        }

        /// Consume this socket and return a [`futures_core::Stream`] of
        /// received messages.  Requires the `streams` feature.
        ///
        /// The stream ends — yielding the error first — when the peer
        /// disconnects.  The returned stream is `Unpin`.
        #[cfg(feature = "streams")]
        pub fn into_stream(
            self,
        ) -> std::pin::Pin<
            Box<dyn futures_core::Stream<Item = Result<Message, crate::NngError>> + Send>,
        > {
            Box::pin(futures_util::stream::unfold(
                Some(self),
                |state| async move {
                    let mut this = state?;
                    match this.recv().await {
                        Ok(msg) => Some((Ok(msg), Some(this))),
                        Err(e) => Some((Err(e), None)),
                    }
                },
            ))
        }

        /// Consume this socket and return a [`futures_sink::Sink`] that sends
        /// each flushed message to the connected peer.
        /// Requires the `streams` feature.
        ///
        /// The returned sink is `Unpin`.
        #[cfg(feature = "streams")]
        pub fn into_sink(
            self,
        ) -> std::pin::Pin<Box<dyn futures_sink::Sink<Message, Error = crate::NngError> + Send>>
        {
            Box::pin(futures_util::sink::unfold(
                self,
                |mut this: Self, msg: Message| async move {
                    this.send(msg).await?;
                    Ok(this)
                },
            ))
        }
    }
}

pub mod reqrep0 {
    //! REQ0 / REP0 socket API.

    use std::time::Duration;

    use crate::{
        Message,
        codec::ProtocolId,
        protocols::reqrep::{Rep0State, Req0State, ReqRepError, RoutingInfo},
        socket::{NngError, ReconnectOptions, Socket},
    };

    /// Request socket: connects to a reply server, sends requests, awaits replies.
    ///
    /// The request ID counter is per-socket and increments across calls, so a
    /// stale reply from a previous call is detected and silently discarded.
    pub struct Req0 {
        inner: Socket<Req0State>,
        state: Req0State,
        /// When `Some`, the request is resent after this interval if no reply
        /// arrives.  Equivalent to NNG's `NNG_OPT_REQ_RESENDTIME`.
        resend_time: Option<Duration>,
    }

    impl Req0 {
        fn from_socket(inner: Socket<Req0State>) -> Self {
            Self {
                inner,
                state: Req0State::new(),
                resend_time: None,
            }
        }

        pub async fn dial(addr: &str) -> Result<Self, NngError> {
            Socket::dial(addr, ProtocolId::REQ0)
                .await
                .map(Self::from_socket)
        }

        /// Dial with automatic reconnect using default backoff (100 ms → 30 s).
        pub async fn dial_reconnecting(addr: &str) -> Result<Self, NngError> {
            Socket::dial_reconnecting(addr, ProtocolId::REQ0)
                .await
                .map(Self::from_socket)
        }

        /// Dial with automatic reconnect using custom `ReconnectOptions`.
        pub async fn dial_with_reconnect(
            addr: &str,
            opts: ReconnectOptions,
        ) -> Result<Self, NngError> {
            Socket::dial_with_reconnect(addr, ProtocolId::REQ0, opts)
                .await
                .map(Self::from_socket)
        }

        #[cfg(feature = "tls-tcp")]
        pub async fn dial_tls_tcp(
            addr: &str,
            client_config: std::sync::Arc<rustls::ClientConfig>,
        ) -> Result<Self, NngError> {
            Socket::dial_tls_tcp(addr, ProtocolId::REQ0, client_config)
                .await
                .map(Self::from_socket)
        }

        /// Set the resend interval.
        ///
        /// When set, [`request`](Self::request) retransmits the request after
        /// `d` with no reply and repeats until a valid reply arrives or the
        /// transport returns an error.  Matches NNG's `NNG_OPT_REQ_RESENDTIME`.
        ///
        /// Resend is disabled by default; `request()` waits indefinitely.
        pub fn set_resend_time(&mut self, d: Duration) {
            self.resend_time = Some(d);
        }

        /// Send `msg` and receive the reply.
        ///
        /// The request ID is stamped once for this call and reused verbatim on
        /// every retransmit, so the server cannot distinguish an original from
        /// a resend.  Stale replies — from a previous call's resend that arrived
        /// after the call returned — are detected by ID mismatch and silently
        /// discarded; `request` then waits for the correct reply.
        ///
        /// Returns an error only when the transport fails; a timeout (when
        /// `set_resend_time` is configured) triggers a retransmit, not an error.
        pub async fn request(&mut self, msg: Message) -> Result<Message, NngError> {
            let mut outgoing = msg;
            let sent_id = self.state.prepare_outgoing(&mut outgoing);

            'send: loop {
                self.inner.send_raw(&outgoing).await?;

                loop {
                    let raw = match self.resend_time {
                        None => self.inner.recv_raw().await?,
                        Some(t) => match tokio::time::timeout(t, self.inner.recv_raw()).await {
                            Ok(r) => r?,
                            Err(_elapsed) => continue 'send,
                        },
                    };

                    let mut reply = raw;
                    match self.state.process_incoming(&mut reply, sent_id) {
                        Ok(()) => return Ok(reply),
                        Err(ReqRepError::IdMismatch { .. }) => {} // stale reply, keep waiting
                        Err(e) => return Err(NngError::ProtocolViolation(e.to_string())),
                    }
                }
            }
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
        pub async fn reply(self, msg: Message) -> Result<(), NngError> {
            let state = Rep0State::new();
            let mut outgoing = msg;
            state.prepare_reply(&mut outgoing, &self.routing);
            self.socket.send_raw(&outgoing).await
        }
    }

    impl Rep0 {
        pub async fn listen(addr: &str) -> Result<Self, NngError> {
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
        ) -> Result<Self, NngError> {
            Socket::listen_tls(addr, ProtocolId::REP0, cert_pem, key_pem)
                .await
                .map(|s| Self {
                    inner: s,
                    state: Rep0State::new(),
                })
        }

        #[cfg(feature = "tls-tcp")]
        pub async fn listen_tls_tcp(
            addr: &str,
            cert_pem: &std::path::Path,
            key_pem: &std::path::Path,
        ) -> Result<Self, NngError> {
            Socket::listen_tls_tcp(addr, ProtocolId::REP0, cert_pem, key_pem)
                .await
                .map(|s| Self {
                    inner: s,
                    state: Rep0State::new(),
                })
        }

        /// Receive the next request.  Returns the application message plus a
        /// `Responder` that must be used to send the reply.
        pub async fn receive(&mut self) -> Result<(Message, Responder<'_>), NngError> {
            let mut msg = self.inner.recv_raw().await?;
            let routing = self
                .state
                .process_incoming(&mut msg)
                .map_err(|e| NngError::ProtocolViolation(e.to_string()))?;
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

/// [`tower_service::Service`] adapter for [`reqrep0::Req0`].
///
/// Requires the `tower` Cargo feature.
#[cfg(feature = "tower")]
pub mod tower_svc {
    use std::{
        future::Future,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    };

    use tokio::sync::Mutex;

    use crate::{Message, error::NngError};

    use super::reqrep0::Req0;

    /// A cloneable [`tower_service::Service`] that sends requests through a
    /// shared [`Req0`] socket.
    ///
    /// All clones share the same underlying socket via `Arc<Mutex<Req0>>`,
    /// serializing requests (REQ0 allows only one in-flight request at a time).
    /// Any resend time configured on the socket is preserved.
    #[derive(Clone)]
    pub struct Req0Service {
        inner: Arc<Mutex<Req0>>,
    }

    impl Req0Service {
        pub fn new(req0: Req0) -> Self {
            Self {
                inner: Arc::new(Mutex::new(req0)),
            }
        }
    }

    impl tower_service::Service<Message> for Req0Service {
        type Response = Message;
        type Error = NngError;
        type Future = Pin<Box<dyn Future<Output = Result<Message, NngError>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: Message) -> Self::Future {
            let inner = Arc::clone(&self.inner);
            Box::pin(async move { inner.lock().await.request(req).await })
        }
    }
}
