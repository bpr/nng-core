//! High-level socket API for nng-core (requires `std` / tokio).
//!
//! Supported URL schemes:
//! - `tcp://host:port` — plain TCP
//! - `ipc:///path` — Unix domain socket, NNG 1.5.x framing (9-byte header)
//! - `ipc2:///path` — Unix domain socket, NNG 2.x framing (8-byte header, same as TCP)
//! - `ws://host:port[/path]` — WebSocket (requires `ws` feature)
//! - `wss://host:port[/path]` — Secure WebSocket / TLS (requires `wss` feature)
//! - `tls+tcp://host:port` — TLS over TCP (requires `tls-tcp` feature)
//! - `quic://host:port` — QUIC / TLS 1.3 (requires `quic` feature)
//! - `vsock://CID:port` — VSOCK VM transport, Linux only (requires `vsock` feature)
//! - `kcp://host:port` — KCP / reliable UDP, no encryption (requires `kcp` feature)
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

#[cfg(feature = "quic")]
use crate::transport::quic::{self as quic_mod, QuicStream};

#[cfg(feature = "vsock")]
use crate::transport::vsock::{self as vsock_mod, TokioVsockStream};
#[cfg(feature = "vsock")]
use tokio_vsock::VsockListener;

#[cfg(feature = "kcp")]
use crate::transport::kcp::{self as kcp_mod, TokioKcpStream};
#[cfg(feature = "kcp")]
use kcp_tokio::KcpListener;
#[cfg(feature = "kcp")]
use std::sync::Arc as StdArc;
#[cfg(feature = "kcp")]
use tokio::sync::Mutex as TokioMutex;
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
    /// QUIC bidirectional stream: 8-byte frame header, TLS 1.3 socket layer.
    #[cfg(feature = "quic")]
    Quic(QuicStream),
    /// VSOCK stream: 8-byte frame header, Linux AF_VSOCK socket layer.
    #[cfg(feature = "vsock")]
    Vsock(TokioVsockStream),
    /// KCP stream: 8-byte frame header, KCP socket layer.
    #[cfg(feature = "kcp")]
    Kcp(TokioKcpStream),
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
            #[cfg(feature = "quic")]
            Self::Quic(s) => EioRead::read(s, buf).await,
            #[cfg(feature = "vsock")]
            Self::Vsock(s) => EioRead::read(s, buf).await,
            #[cfg(feature = "kcp")]
            Self::Kcp(s) => EioRead::read(s, buf).await,
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
            #[cfg(feature = "quic")]
            Self::Quic(s) => EioWrite::write(s, buf).await,
            #[cfg(feature = "vsock")]
            Self::Vsock(s) => EioWrite::write(s, buf).await,
            #[cfg(feature = "kcp")]
            Self::Kcp(s) => EioWrite::write(s, buf).await,
        }
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        match self {
            Self::Tcp(s) => EioWrite::flush(s).await,
            #[cfg(unix)]
            Self::Ipc(s) | Self::Ipc2(s) => EioWrite::flush(s).await,
            #[cfg(feature = "tls-tcp")]
            Self::TlsTcp(s) => EioWrite::flush(s).await,
            #[cfg(feature = "quic")]
            Self::Quic(s) => EioWrite::flush(s).await,
            #[cfg(feature = "vsock")]
            Self::Vsock(s) => EioWrite::flush(s).await,
            #[cfg(feature = "kcp")]
            Self::Kcp(s) => EioWrite::flush(s).await,
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
            #[cfg(feature = "quic")]
            Self::Quic(_) => FrameFormat::Tcp,
            #[cfg(feature = "vsock")]
            Self::Vsock(_) => FrameFormat::Tcp,
            #[cfg(feature = "kcp")]
            Self::Kcp(_) => FrameFormat::Tcp,
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
    /// QUIC endpoint: TLS 1.3, one bidirectional stream per SP connection.
    #[cfg(feature = "quic")]
    Quic(quinn::Endpoint),
    /// VSOCK listener: Linux AF_VSOCK, 8-byte TCP frame header.
    #[cfg(feature = "vsock")]
    Vsock(VsockListener),
    /// KCP listener.  Wrapped in `TokioMutex` because
    /// `KcpListener::accept` requires `&mut self` (so we wrap in `TokioMutex`),
    /// and the listener owns the shared UDP socket plus a background routing
    /// task that every accepted [`kcp_tokio::KcpStream`] depends on — dropping
    /// it kills the streams.  We therefore hold an [`StdArc`] so accepted
    /// streams can clone a reference and keep the listener alive past the
    /// `Socket::listen` scope.  See the `src/transport/kcp.rs` module docs.
    #[cfg(feature = "kcp")]
    Kcp(StdArc<TokioMutex<KcpListener>>),
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
            #[cfg(feature = "quic")]
            Self::Quic(ep) => {
                let qs = quic_mod::server_stream(ep).await.map_err(NngError::from)?;
                connect_framed(AnyStream::Quic(qs), proto)
                    .await
                    .map(AnyTransport::Framed)
                    .map_err(NngError::from)
            }
            #[cfg(feature = "vsock")]
            Self::Vsock(l) => {
                let (stream, _) = l.accept().await?;
                connect_framed(AnyStream::Vsock(TokioVsockStream(stream)), proto)
                    .await
                    .map(AnyTransport::Framed)
                    .map_err(NngError::from)
            }
            #[cfg(feature = "kcp")]
            Self::Kcp(l) => {
                let keepalive = l.clone();
                let (kcp, _peer) = {
                    let mut guard = l.lock().await;
                    guard
                        .accept()
                        .await
                        .map_err(|e| io::Error::other(format!("kcp accept failed: {e}")))?
                };
                connect_framed(AnyStream::Kcp(TokioKcpStream(kcp, Some(keepalive))), proto)
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
            #[cfg(feature = "quic")]
            Self::Quic(_) => Err(io::Error::other(
                "QUIC listeners do not support non-blocking accept; \
                 use accept_as_transport instead",
            )),
            #[cfg(feature = "vsock")]
            Self::Vsock(l) => {
                let (stream, _) = l.accept().await?;
                Ok(RawConn::Stream(AnyStream::Vsock(TokioVsockStream(stream))))
            }
            #[cfg(feature = "kcp")]
            Self::Kcp(l) => {
                let keepalive = l.clone();
                let (kcp, _peer) = {
                    let mut guard = l.lock().await;
                    guard
                        .accept()
                        .await
                        .map_err(|e| io::Error::other(format!("kcp accept failed: {e}")))?
                };
                Ok(RawConn::Stream(AnyStream::Kcp(TokioKcpStream(
                    kcp,
                    Some(keepalive),
                ))))
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
    } else if addr.starts_with("quic://") {
        Err(NngError::ProtocolViolation(
            "quic:// requires listen_quic — use listen_quic(addr, cert_pem, key_pem)".into(),
        ))
    } else if let Some(vsock_addr) = addr.strip_prefix("vsock://") {
        #[cfg(feature = "vsock")]
        {
            return vsock_mod::bind_vsock_listener(vsock_addr)
                .map(AnyListener::Vsock)
                .map_err(NngError::from);
        }
        #[cfg(not(feature = "vsock"))]
        {
            let _ = vsock_addr;
            return Err(NngError::FeatureNotEnabled("vsock"));
        }
    } else if let Some(kcp_addr) = addr.strip_prefix("kcp://") {
        #[cfg(feature = "kcp")]
        {
            return kcp_mod::bind_kcp_listener(kcp_addr)
                .await
                .map(|l| AnyListener::Kcp(StdArc::new(TokioMutex::new(l))))
                .map_err(NngError::from);
        }
        #[cfg(not(feature = "kcp"))]
        {
            let _ = kcp_addr;
            return Err(NngError::FeatureNotEnabled("kcp"));
        }
    } else {
        Err(NngError::UnsupportedScheme(addr.to_owned()))
    }
}

/// Bind a `quic://` listener endpoint.
///
/// `addr` must start with `quic://` followed by a `host:port` that can be
/// parsed as a [`SocketAddr`].  `cert_pem` and `key_pem` are paths to
/// PEM-encoded certificate chain and private key files.
///
/// The QUIC endpoint is bound synchronously (no async, just a UDP socket
/// bind); the first connection is not awaited here.
///
/// [`SocketAddr`]: std::net::SocketAddr
#[cfg(feature = "quic")]
pub(crate) async fn bind_listener_quic(
    addr: &str,
    cert_pem: &std::path::Path,
    key_pem: &std::path::Path,
) -> io::Result<AnyListener> {
    let host_port = addr
        .strip_prefix("quic://")
        .ok_or_else(|| io::Error::other("bind_listener_quic requires a quic:// URL"))?
        .split('/')
        .next()
        .unwrap_or(addr);
    let socket_addr = tokio::net::lookup_host(host_port)
        .await?
        .next()
        .ok_or_else(|| {
            io::Error::other(format!("could not resolve QUIC bind address: {host_port}"))
        })?;
    let endpoint = quic_mod::build_server_endpoint(socket_addr, cert_pem, key_pem)?;
    Ok(AnyListener::Quic(endpoint))
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
    // 666 URL dispatch in connect_stream
    } else if let Some(vsock_addr) = addr.strip_prefix("vsock://") {
        #[cfg(feature = "vsock")]
        {
            return vsock_mod::connect_vsock_stream(vsock_addr)
                .await
                .map(|s| AnyStream::Vsock(TokioVsockStream(s)))
                .map_err(NngError::from);
        }
        #[cfg(not(feature = "vsock"))]
        {
            let _ = vsock_addr;
            return Err(NngError::FeatureNotEnabled("vsock"));
        }
    } else if let Some(kcp_addr) = addr.strip_prefix("kcp://") {
        #[cfg(feature = "kcp")]
        {
            return kcp_mod::connect_kcp_stream(kcp_addr)
                .await
                .map(|s| AnyStream::Kcp(TokioKcpStream(s, None)))
                .map_err(NngError::from);
        }
        #[cfg(not(feature = "kcp"))]
        {
            let _ = kcp_addr;
            return Err(NngError::FeatureNotEnabled("kcp"));
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
    #[cfg(feature = "quic")]
    if let Some(quic_addr) = addr.strip_prefix("quic://") {
        let stream = connect_quic_stream(quic_addr)
            .await
            .map_err(NngError::from)?;
        return connect_framed(stream, proto)
            .await
            .map(AnyTransport::Framed)
            .map_err(NngError::from);
    }
    let stream = connect_stream(addr).await?;
    connect_framed(stream, proto)
        .await
        .map(AnyTransport::Framed)
        .map_err(NngError::from)
}

/// Dial a `quic://` server using the platform's native root certificate store.
///
/// Suitable for servers with certificates signed by a public CA.  For
/// self-signed certificates (e.g. in tests), use [`Socket::dial_quic`] which
/// accepts a custom [`rustls::ClientConfig`].
#[cfg(feature = "quic")]
async fn connect_quic_stream(host_port: &str) -> io::Result<AnyStream> {
    let client_config = quic_mod::build_native_roots_client_config()?;
    connect_quic_stream_with(host_port, client_config).await
}

/// Dial a `quic://host:port` with a caller-supplied quinn client config.
///
/// Resolves the host, binds an ephemeral UDP socket, connects, and opens a
/// bidirectional QUIC stream.  Returns the stream wrapped as `AnyStream::Quic`.
#[cfg(feature = "quic")]
pub(crate) async fn connect_quic_stream_with(
    host_port: &str,
    client_config: quinn::ClientConfig,
) -> io::Result<AnyStream> {
    let addr = host_port.split('/').next().unwrap_or(host_port);
    let peer = tokio::net::lookup_host(addr)
        .await?
        .next()
        .ok_or_else(|| io::Error::other(format!("could not resolve QUIC address: {addr}")))?;
    let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(addr);
    let endpoint = quic_mod::build_client_endpoint(peer, client_config)?;
    let qs = quic_mod::client_stream(endpoint, peer, host).await?;
    Ok(AnyStream::Quic(qs))
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

    /// Bind a `quic://` listener endpoint and wait for the first connection.
    ///
    /// Loads the server certificate and private key from PEM files and binds a
    /// UDP socket at `addr` (a `quic://host:port` URL; both hostname and IP
    /// address forms are accepted).  Waits for the first QUIC connection, then
    /// runs the SP framing handshake.
    #[cfg(feature = "quic")]
    pub async fn listen_quic(
        addr: &str,
        proto: ProtocolId,
        cert_pem: &std::path::Path,
        key_pem: &std::path::Path,
    ) -> Result<Self, NngError> {
        let listener = bind_listener_quic(addr, cert_pem, key_pem)
            .await
            .map_err(NngError::from)?;
        let transport = listener.accept_as_transport(proto).await?;
        Ok(Self::new(transport, proto))
    }

    /// Dial a `quic://` server with a custom `rustls::ClientConfig`.
    ///
    /// Use this when the server presents a self-signed certificate (e.g. in
    /// tests).  For production servers with public CA certificates, use
    /// [`dial`](Self::dial) with a `quic://` URL, which uses the system's
    /// native root certificate store.
    ///
    /// The `"nng/1"` ALPN identifier is added to `client_config` automatically
    /// if not already present.
    #[cfg(feature = "quic")]
    pub async fn dial_quic(
        addr: &str,
        proto: ProtocolId,
        client_config: std::sync::Arc<rustls::ClientConfig>,
    ) -> Result<Self, NngError> {
        let host_port = addr
            .strip_prefix("quic://")
            .ok_or_else(|| NngError::Io(io::Error::other("dial_quic requires a quic:// URL")))?;
        let quinn_config =
            quic_mod::build_custom_client_config(client_config).map_err(NngError::from)?;
        let stream = connect_quic_stream_with(host_port, quinn_config)
            .await
            .map_err(NngError::from)?;
        connect_framed(stream, proto)
            .await
            .map(|t| Self::new(AnyTransport::Framed(t), proto))
            .map_err(NngError::from)
    }

    /// Bind a `kcp://` listener with a caller-supplied [`kcp_tokio::KcpConfig`]
    /// and wait for the first connection.
    ///
    /// Both peers MUST use the same config (MTU, snd_wnd, rcv_wnd,
    /// nodelay/interval, fast_resend, …) for correct ARQ behavior.  Use
    /// [`listen`](Self::listen) with a `kcp://` URL for the default config.
    #[cfg(feature = "kcp")]
    pub async fn listen_kcp_with(
        addr: &str,
        proto: ProtocolId,
        config: kcp_tokio::KcpConfig,
    ) -> Result<Self, NngError> {
        let kcp_addr = addr.strip_prefix("kcp://").ok_or_else(|| {
            NngError::Io(io::Error::other("listen_kcp_with requires a kcp:// URL"))
        })?;
        let listener = kcp_mod::bind_kcp_listener_with(kcp_addr, config)
            .await
            .map_err(NngError::from)?;
        let listener = AnyListener::Kcp(StdArc::new(TokioMutex::new(listener)));
        let transport = listener.accept_as_transport(proto).await?;
        Ok(Self::new(transport, proto))
    }

    /// Dial a `kcp://` server with a caller-supplied [`kcp_tokio::KcpConfig`].
    ///
    /// See [`listen_kcp_with`](Self::listen_kcp_with) for the config
    /// symmetry requirement.
    #[cfg(feature = "kcp")]
    pub async fn dial_kcp_with(
        addr: &str,
        proto: ProtocolId,
        config: kcp_tokio::KcpConfig,
    ) -> Result<Self, NngError> {
        let kcp_addr = addr
            .strip_prefix("kcp://")
            .ok_or_else(|| NngError::Io(io::Error::other("dial_kcp_with requires a kcp:// URL")))?;
        let stream = kcp_mod::connect_kcp_stream_with(kcp_addr, config)
            .await
            .map_err(NngError::from)?;
        let any_stream = AnyStream::Kcp(TokioKcpStream(stream, None));
        connect_framed(any_stream, proto)
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

// ── Per-protocol method-forwarding macro ─────────────────────────────────────
//
// Each socket wrapper type that delegates straight to `Socket` (Push0, Pull0,
// Pair0, Req0, Rep0) used to carry 5–11 hand-written stub methods that just
// call `Socket::<method>(addr, ProtocolId::<X>0, …)` and wrap the result.
// `forward_socket_method!` collapses those stubs to one-liners.  Protocols
// with bespoke internal state (Pub0, Sub0, Surveyor0, Respondent0, Bus0)
// don't use this — they have their own implementations.
//
// Usage inside an `impl XX { … }` block:
//
//     forward_socket_method!(dial,          ProtocolId::PUSH0, tuple);
//     forward_socket_method!(listen_quic,   ProtocolId::PUSH0, tuple);
//     forward_socket_method!(dial_kcp_with, ProtocolId::REQ0,  from_socket);
//
// The third argument selects how the `Socket<…>` returned by the underlying
// call is wrapped into `Self`:
//
//   - `tuple`       — `.map(Self)` for `pub struct X(Socket<XState>);`
//   - `from_socket` — `.map(Self::from_socket)` for state-holding wrappers
//   - `rep0`        — bespoke closure for `pub struct Rep0 { inner, state }`
//
// All transport-specific arms emit their own `#[cfg(feature = "…")]` gate,
// so callers don't repeat it.
macro_rules! forward_socket_method {
    // ── plain dial / listen ────────────────────────────────────────────
    (dial, $proto:expr, $ctor:tt) => {
        pub async fn dial(addr: &str) -> Result<Self, NngError> {
            Socket::dial(addr, $proto)
                .await
                .map(forward_socket_method!(@map $ctor))
        }
    };
    (listen, $proto:expr, $ctor:tt) => {
        pub async fn listen(addr: &str) -> Result<Self, NngError> {
            Socket::listen(addr, $proto)
                .await
                .map(forward_socket_method!(@map $ctor))
        }
    };
    (dial_reconnecting, $proto:expr, $ctor:tt) => {
        /// Dial with automatic reconnect using default backoff (100 ms → 30 s).
        pub async fn dial_reconnecting(addr: &str) -> Result<Self, NngError> {
            Socket::dial_reconnecting(addr, $proto)
                .await
                .map(forward_socket_method!(@map $ctor))
        }
    };
    (dial_with_reconnect, $proto:expr, $ctor:tt) => {
        /// Dial with automatic reconnect using custom `ReconnectOptions`.
        pub async fn dial_with_reconnect(
            addr: &str,
            opts: ReconnectOptions,
        ) -> Result<Self, NngError> {
            Socket::dial_with_reconnect(addr, $proto, opts)
                .await
                .map(forward_socket_method!(@map $ctor))
        }
    };

    // ── tls-tcp ────────────────────────────────────────────────────────
    (listen_tls_tcp, $proto:expr, $ctor:tt) => {
        /// Bind a `tls+tcp://` listener.  Requires the `tls-tcp` feature.
        #[cfg(feature = "tls-tcp")]
        pub async fn listen_tls_tcp(
            addr: &str,
            cert_pem: &std::path::Path,
            key_pem: &std::path::Path,
        ) -> Result<Self, NngError> {
            Socket::listen_tls_tcp(addr, $proto, cert_pem, key_pem)
                .await
                .map(forward_socket_method!(@map $ctor))
        }
    };
    (dial_tls_tcp, $proto:expr, $ctor:tt) => {
        /// Dial a `tls+tcp://` server with a custom TLS client config.
        /// Requires the `tls-tcp` feature.
        #[cfg(feature = "tls-tcp")]
        pub async fn dial_tls_tcp(
            addr: &str,
            client_config: std::sync::Arc<rustls::ClientConfig>,
        ) -> Result<Self, NngError> {
            Socket::dial_tls_tcp(addr, $proto, client_config)
                .await
                .map(forward_socket_method!(@map $ctor))
        }
    };

    // ── wss ────────────────────────────────────────────────────────────
    (listen_tls, $proto:expr, $ctor:tt) => {
        /// Bind a `wss://` listener.  Requires the `wss` feature.
        #[cfg(feature = "wss")]
        pub async fn listen_tls(
            addr: &str,
            cert_pem: &std::path::Path,
            key_pem: &std::path::Path,
        ) -> Result<Self, NngError> {
            Socket::listen_tls(addr, $proto, cert_pem, key_pem)
                .await
                .map(forward_socket_method!(@map $ctor))
        }
    };

    // ── quic ───────────────────────────────────────────────────────────
    (listen_quic, $proto:expr, $ctor:tt) => {
        /// Bind a `quic://` listener.  Requires the `quic` feature.
        #[cfg(feature = "quic")]
        pub async fn listen_quic(
            addr: &str,
            cert_pem: &std::path::Path,
            key_pem: &std::path::Path,
        ) -> Result<Self, NngError> {
            Socket::listen_quic(addr, $proto, cert_pem, key_pem)
                .await
                .map(forward_socket_method!(@map $ctor))
        }
    };
    (dial_quic, $proto:expr, $ctor:tt) => {
        /// Dial a `quic://` server with a custom TLS client config.
        /// Requires the `quic` feature.
        #[cfg(feature = "quic")]
        pub async fn dial_quic(
            addr: &str,
            client_config: std::sync::Arc<rustls::ClientConfig>,
        ) -> Result<Self, NngError> {
            Socket::dial_quic(addr, $proto, client_config)
                .await
                .map(forward_socket_method!(@map $ctor))
        }
    };

    // ── kcp ────────────────────────────────────────────────────────────
    (listen_kcp_with, $proto:expr, $ctor:tt) => {
        /// Bind a `kcp://` listener with a caller-supplied `KcpConfig`.
        /// Requires the `kcp` feature.  Both peers must use the same config.
        #[cfg(feature = "kcp")]
        pub async fn listen_kcp_with(
            addr: &str,
            config: kcp_tokio::KcpConfig,
        ) -> Result<Self, NngError> {
            Socket::listen_kcp_with(addr, $proto, config)
                .await
                .map(forward_socket_method!(@map $ctor))
        }
    };
    (dial_kcp_with, $proto:expr, $ctor:tt) => {
        /// Dial a `kcp://` server with a caller-supplied `KcpConfig`.
        /// Requires the `kcp` feature.  Both peers must use the same config.
        #[cfg(feature = "kcp")]
        pub async fn dial_kcp_with(
            addr: &str,
            config: kcp_tokio::KcpConfig,
        ) -> Result<Self, NngError> {
            Socket::dial_kcp_with(addr, $proto, config)
                .await
                .map(forward_socket_method!(@map $ctor))
        }
    };

    // ── constructor-flavor helpers (internal) ──────────────────────────
    (@map tuple) => { Self };
    (@map from_socket) => { Self::from_socket };
    (@map rep0) => { |s| Self { inner: s, state: Rep0State::new() } };
}

// ── Protocol modules ──────────────────────────────────────────────────────────

pub mod pubsub0;

pub mod survey0;

pub mod bus0;
pub mod pair0;
pub mod pipeline0;
pub mod reqrep0;
#[cfg(feature = "tower")]
pub mod tower_svc;
