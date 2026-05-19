//! QUIC transport for the SP protocol via `quinn`.
//!
//! QUIC provides reliable, ordered byte streams with TLS 1.3 built-in, so it
//! maps cleanly onto [`FramedTransport`]: each SP "connection" is one QUIC
//! connection with a single bidirectional stream.  The SP handshake and 8-byte
//! TCP frame format run over that stream unchanged.
//!
//! # ALPN
//!
//! Both sides negotiate the application-layer protocol `"nng/1"`.  Custom
//! client configs (e.g. for self-signed certificates in tests) must include
//! this identifier in their `alpn_protocols` list, or use
//! [`build_custom_client_config`] which adds it automatically.
//!
//! [`FramedTransport`]: crate::transport::FramedTransport

use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use embedded_io_async::{ErrorType, Read as EioRead, Write as EioWrite};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// ALPN identifier used for SP-over-QUIC.
pub const ALPN_NNG: &[u8] = b"nng/1";

/// A bidirectional QUIC stream that implements `embedded-io-async` `Read` +
/// `Write`, making it usable directly with [`FramedTransport`].
///
/// Both `_conn` and `_endpoint` are kept alive for the stream's entire
/// lifetime.  The endpoint owns the UDP socket that processes QUIC packets;
/// dropping it sends CONNECTION_CLOSE and terminates the session.  The
/// connection is a ref-counted handle; keeping it prevents the endpoint from
/// deciding the connection is idle.
///
/// [`FramedTransport`]: crate::transport::FramedTransport
pub(crate) struct QuicStream {
    /// Keeps the QUIC connection alive.
    _conn: quinn::Connection,
    /// Keeps the UDP I/O endpoint alive (client side; `None` on the server).
    _endpoint: Option<quinn::Endpoint>,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
}

impl ErrorType for QuicStream {
    type Error = io::Error;
}

impl EioRead for QuicStream {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        AsyncReadExt::read(&mut self.recv, buf).await
    }
}

impl EioWrite for QuicStream {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        AsyncWriteExt::write(&mut self.send, buf).await
    }

    async fn flush(&mut self) -> Result<(), io::Error> {
        AsyncWriteExt::flush(&mut self.send).await
    }
}

impl Drop for QuicStream {
    fn drop(&mut self) {
        // Signal graceful end-of-stream rather than letting quinn's SendStream
        // destructor send RESET_STREAM.  finish() enqueues a STREAM FIN so the
        // peer can read all data already written before the stream is torn down.
        let _ = self.send.finish();
    }
}

// ── Dialer helpers ────────────────────────────────────────────────────────────

/// Connect via a QUIC endpoint, open a bidirectional stream, and return a
/// [`QuicStream`] that owns both the connection and the endpoint.
///
/// The endpoint is consumed and stored inside the returned [`QuicStream`] so
/// that its UDP socket (which drives all QUIC I/O) stays alive for as long as
/// the stream is in use.
///
/// `sni` is the TLS server-name indication value (typically the hostname from
/// the URL, without the port).
pub(crate) async fn client_stream(
    endpoint: quinn::Endpoint,
    peer: SocketAddr,
    sni: &str,
) -> io::Result<QuicStream> {
    let conn = endpoint
        .connect(peer, sni)
        .map_err(|e| io::Error::other(e.to_string()))?
        .await
        .map_err(|e| io::Error::other(e.to_string()))?;
    let (send, recv) = conn
        .open_bi()
        .await
        .map_err(|e| io::Error::other(e.to_string()))?;
    Ok(QuicStream {
        _conn: conn,
        _endpoint: Some(endpoint),
        send,
        recv,
    })
}

/// Create a QUIC [`Endpoint`] for dialing (client side) with the given client
/// config.  Binds to an ephemeral port on either `0.0.0.0:0` or `[::]:0`
/// depending on whether `peer` is IPv4 or IPv6.
pub(crate) fn build_client_endpoint(
    peer: SocketAddr,
    client_config: quinn::ClientConfig,
) -> io::Result<quinn::Endpoint> {
    let bind_addr: SocketAddr = if peer.is_ipv6() {
        "[::]:0".parse().unwrap()
    } else {
        "0.0.0.0:0".parse().unwrap()
    };
    let mut ep = quinn::Endpoint::client(bind_addr)?;
    ep.set_default_client_config(client_config);
    Ok(ep)
}

// ── Listener helpers ──────────────────────────────────────────────────────────

/// Build a QUIC [`Endpoint`] for listening (server side) at `addr`.
///
/// `cert_pem` and `key_pem` are paths to PEM-encoded certificate chain and
/// private key files respectively.  The acceptor is wired up once and reused
/// for every incoming connection — store the returned endpoint in an
/// [`AnyListener::Quic`] variant.
///
/// [`AnyListener::Quic`]: crate::socket::AnyListener
pub(crate) fn build_server_endpoint(
    addr: SocketAddr,
    cert_pem: &Path,
    key_pem: &Path,
) -> io::Result<quinn::Endpoint> {
    let server_config = build_server_tls_config(cert_pem, key_pem)?;
    quinn::Endpoint::server(server_config, addr).map_err(io::Error::from)
}

/// Accept the next QUIC connection on `endpoint` and open a bidirectional
/// stream on it, ready for the SP handshake.
///
/// A clone of `endpoint` is stored inside the returned [`QuicStream`] to keep
/// its UDP socket alive for the duration of the stream.  Cloning an
/// `Endpoint` is cheap (it shares the same underlying I/O driver).
///
/// Returns `Err` if the endpoint has been closed (no more incoming
/// connections are possible).
pub(crate) async fn server_stream(endpoint: &quinn::Endpoint) -> io::Result<QuicStream> {
    let incoming = endpoint
        .accept()
        .await
        .ok_or_else(|| io::Error::other("QUIC endpoint closed"))?;
    let conn = incoming
        .await
        .map_err(|e| io::Error::other(e.to_string()))?;
    let (send, recv) = conn
        .accept_bi()
        .await
        .map_err(|e| io::Error::other(e.to_string()))?;
    Ok(QuicStream {
        _conn: conn,
        _endpoint: Some(endpoint.clone()),
        send,
        recv,
    })
}

// ── TLS configuration ─────────────────────────────────────────────────────────

fn build_server_tls_config(cert_pem: &Path, key_pem: &Path) -> io::Result<quinn::ServerConfig> {
    let certs = load_certs(cert_pem)?;
    let key = load_key(key_pem)?;
    let mut tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(io::Error::other)?;
    tls.alpn_protocols = vec![ALPN_NNG.to_vec()];
    let quic_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls)
        .map_err(|e| io::Error::other(e.to_string()))?;
    Ok(quinn::ServerConfig::with_crypto(Arc::new(quic_crypto)))
}

/// Build a `quinn::ClientConfig` that uses the platform's native root
/// certificate store.  Suitable for connecting to servers with public CA
/// certificates.  For self-signed certificates (e.g. in tests), construct a
/// `rustls::ClientConfig` manually and pass it to [`build_custom_client_config`].
pub(crate) fn build_native_roots_client_config() -> io::Result<quinn::ClientConfig> {
    let root_store = load_native_roots()?;
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    tls.alpn_protocols = vec![ALPN_NNG.to_vec()];
    let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
        .map_err(|e| io::Error::other(e.to_string()))?;
    Ok(quinn::ClientConfig::new(Arc::new(quic_crypto)))
}

/// Wrap a caller-supplied `rustls::ClientConfig` into a `quinn::ClientConfig`,
/// inserting the `"nng/1"` ALPN identifier if not already present.
///
/// Use this for custom TLS configurations, e.g. when the server uses a
/// self-signed certificate and the test needs to skip verification.
pub(crate) fn build_custom_client_config(
    client_config: Arc<rustls::ClientConfig>,
) -> io::Result<quinn::ClientConfig> {
    let mut tls = (*client_config).clone();
    if !tls.alpn_protocols.contains(&ALPN_NNG.to_vec()) {
        tls.alpn_protocols.push(ALPN_NNG.to_vec());
    }
    let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
        .map_err(|e| io::Error::other(e.to_string()))?;
    Ok(quinn::ClientConfig::new(Arc::new(quic_crypto)))
}

// ── PEM loading (mirrors tls_tcp.rs; duplicated to avoid cross-feature dep) ──

fn load_certs(path: &Path) -> io::Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let file = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(io::Error::other)
}

fn load_key(path: &Path) -> io::Result<rustls::pki_types::PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("no private key found in PEM file"))
}

fn load_native_roots() -> io::Result<rustls::RootCertStore> {
    let result = rustls_native_certs::load_native_certs();
    if result.certs.is_empty() {
        return Err(io::Error::other(
            "failed to load any native root certificates",
        ));
    }
    let mut store = rustls::RootCertStore::empty();
    for cert in result.certs {
        let _ = store.add(cert);
    }
    Ok(store)
}
