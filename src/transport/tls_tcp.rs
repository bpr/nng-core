//! TLS-over-TCP stream types for [`FramedTransport`].
//!
//! `TlsTcpStream` wraps either a client-side or server-side TLS stream over
//! TCP and implements `embedded-io-async` `Read` + `Write` so it can be used
//! directly with [`FramedTransport`].  The SP handshake and 8-byte TCP frame
//! format are unchanged; TLS is purely a socket-layer concern.
//!
//! [`FramedTransport`]: crate::transport::FramedTransport

use std::io;
use std::path::Path;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::{
    TlsAcceptor, TlsConnector, client::TlsStream as ClientTlsStream,
    server::TlsStream as ServerTlsStream,
};

use embedded_io_async::{ErrorType, Read as EioRead, Write as EioWrite};

/// A TLS stream over TCP, holding either a dialer-side or acceptor-side stream.
///
/// Both variants implement `embedded-io-async` `Read` + `Write` identically;
/// the enum exists only because `tokio-rustls` exposes two distinct types.
pub(crate) enum TlsTcpStream {
    Client(ClientTlsStream<TcpStream>),
    Server(ServerTlsStream<TcpStream>),
}

impl ErrorType for TlsTcpStream {
    type Error = io::Error;
}

impl EioRead for TlsTcpStream {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        match self {
            Self::Client(s) => s.read(buf).await,
            Self::Server(s) => s.read(buf).await,
        }
    }
}

impl EioWrite for TlsTcpStream {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        match self {
            Self::Client(s) => s.write(buf).await,
            Self::Server(s) => s.write(buf).await,
        }
    }

    async fn flush(&mut self) -> Result<(), io::Error> {
        match self {
            Self::Client(s) => s.flush().await,
            Self::Server(s) => s.flush().await,
        }
    }
}

// ── TLS setup helpers ─────────────────────────────────────────────────────────

/// Build a `TlsAcceptor` from PEM cert and key files.
///
/// Loads the certificate chain and private key once; the acceptor can be
/// cloned and reused for every accepted connection.
pub(crate) fn build_tls_acceptor(cert_pem: &Path, key_pem: &Path) -> io::Result<TlsAcceptor> {
    use rustls::ServerConfig;
    let certs = load_certs(cert_pem)?;
    let key = load_key(key_pem)?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(io::Error::other)?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Build a `TlsConnector` using the platform's native root certificate store.
///
/// Suitable for connecting to servers that present a certificate signed by a
/// public CA.  For self-signed certificates (e.g. in tests), construct a
/// `rustls::ClientConfig` manually and wrap it in `TlsConnector::from`.
pub(crate) fn build_tls_connector() -> io::Result<TlsConnector> {
    use rustls::ClientConfig;
    let roots = native_roots()?;
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(TlsConnector::from(Arc::new(config)))
}

fn native_roots() -> io::Result<rustls::RootCertStore> {
    let result = rustls_native_certs::load_native_certs();
    if result.certs.is_empty() {
        return Err(io::Error::other(
            "failed to load any native root certificates",
        ));
    }
    let mut store = rustls::RootCertStore::empty();
    for cert in result.certs {
        let _ = store.add(cert); // individual failures are non-fatal
    }
    Ok(store)
}

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
