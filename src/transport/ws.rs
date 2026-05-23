//! WebSocket SP transport.
//!
//! Implements the SP-over-WebSocket mapping from sp-websocket-mapping-01:
//! protocol identification via `Sec-WebSocket-Protocol`, one SP message per
//! WebSocket binary message, no SP handshake bytes on the wire.
//!
//! The dialer and acceptor sides produce different inner stream types
//! (`MaybeTlsStream<TcpStream>` vs `TcpStream`).  `WebSocketStream` is not
//! coercible between them, so [`WsTransport`] uses an internal `WsInner` enum
//! rather than a generic parameter, keeping a single concrete public type.
//!
//! The `wss` feature adds TLS support via `tokio-rustls`.  [`WsTransport::accept_tls`]
//! takes PEM file paths (matching C NNG's `NNG_OPT_TLS_CERT_KEY_FILE`);
//! [`WsTransport::connect`] handles `wss://` URLs automatically when the `wss`
//! feature is enabled.

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{
        Message as WsMessage,
        handshake::server::{ErrorResponse, Request as WsRequest, Response as WsResponse},
        http::{HeaderValue, StatusCode},
    },
};

use crate::{Message, codec::ProtocolId};

enum WsInner {
    Dial(WebSocketStream<MaybeTlsStream<TcpStream>>),
    Accept(WebSocketStream<TcpStream>),
    #[cfg(feature = "wss")]
    AcceptTls(WebSocketStream<tokio_rustls::server::TlsStream<TcpStream>>),
}

/// WebSocket transport for the SP protocol.
///
/// Wraps a `WebSocketStream` and maps SP messages to WebSocket binary frames.
/// There is no length prefix: WebSocket itself provides message framing, and
/// protocol identification is handled by the HTTP `Sec-WebSocket-Protocol`
/// header during the upgrade handshake.
///
/// With the `wss` feature enabled, [`connect`](Self::connect) handles `wss://`
/// URLs via rustls, and [`accept_tls`](Self::accept_tls) upgrades an accepted
/// TCP stream through TLS before the WebSocket handshake.
pub struct WsTransport {
    inner: WsInner,
}

impl WsTransport {
    /// Open a WebSocket (or secure WebSocket) connection to `url`.
    ///
    /// Sends the peer's `Sec-WebSocket-Protocol` subprotocol string in the
    /// HTTP upgrade request and validates that the server echoes it back.
    ///
    /// With the `wss` feature enabled, `wss://` URLs are handled automatically
    /// using the system's native root certificate store.  For custom TLS
    /// configuration (e.g. self-signed certs in tests), use
    /// [`connect_with_tls`](Self::connect_with_tls).
    pub async fn connect(url: &str, local_proto: ProtocolId) -> Result<Self, WsError> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;

        let peer_subproto = local_proto.expected_peer().ws_subprotocol();

        let mut request = url.into_client_request()?;
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_str(&peer_subproto)
                .map_err(|e| WsError::InvalidHeader(e.to_string()))?,
        );

        let (ws, response) = tokio_tungstenite::connect_async(request).await?;

        let server_proto = response
            .headers()
            .get("Sec-WebSocket-Protocol")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if server_proto != peer_subproto {
            return Err(WsError::SubprotocolMismatch {
                expected: peer_subproto,
                got: server_proto.to_owned(),
            });
        }

        Ok(Self {
            inner: WsInner::Dial(ws),
        })
    }

    /// Open a `wss://` connection with a custom rustls `ClientConfig`.
    ///
    /// Use this when the server presents a self-signed certificate (e.g. in
    /// tests).  For production, prefer [`connect`](Self::connect), which uses
    /// the system's native root certificate store.
    #[cfg(feature = "wss")]
    pub async fn connect_with_tls(
        url: &str,
        local_proto: ProtocolId,
        connector: tokio_tungstenite::Connector,
    ) -> Result<Self, WsError> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;

        let peer_subproto = local_proto.expected_peer().ws_subprotocol();

        let mut request = url.into_client_request()?;
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_str(&peer_subproto)
                .map_err(|e| WsError::InvalidHeader(e.to_string()))?,
        );

        let (ws, response) =
            tokio_tungstenite::connect_async_tls_with_config(request, None, false, Some(connector))
                .await?;

        let server_proto = response
            .headers()
            .get("Sec-WebSocket-Protocol")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if server_proto != peer_subproto {
            return Err(WsError::SubprotocolMismatch {
                expected: peer_subproto,
                got: server_proto.to_owned(),
            });
        }

        Ok(Self {
            inner: WsInner::Dial(ws),
        })
    }

    /// Upgrade an accepted TCP connection to a WebSocket SP transport.
    ///
    /// Validates that the client's `Sec-WebSocket-Protocol` matches the
    /// listener's own protocol subprotocol string.  Rejects with HTTP 400 on
    /// mismatch — HTTP 400 is the correct rejection at the upgrade stage
    /// (close code 1002 requires a fully established WebSocket connection).
    pub async fn accept(tcp: TcpStream, local_proto: ProtocolId) -> Result<Self, WsError> {
        let ws = ws_upgrade(tcp, local_proto).await?;
        Ok(Self {
            inner: WsInner::Accept(ws),
        })
    }

    /// Upgrade an accepted TCP connection through TLS, then to a WebSocket
    /// SP transport.
    ///
    /// Loads the certificate chain and private key from PEM files, performs
    /// the TLS handshake, then upgrades to WebSocket.  This mirrors C NNG's
    /// `NNG_OPT_TLS_CERT_KEY_FILE` option for listeners.
    ///
    /// For repeated accepts (e.g. in a listener loop), prefer
    /// [`accept_tls_with_acceptor`](Self::accept_tls_with_acceptor) to avoid
    /// reloading the PEM files on every connection.
    #[cfg(feature = "wss")]
    pub async fn accept_tls(
        tcp: TcpStream,
        local_proto: ProtocolId,
        cert_pem: &std::path::Path,
        key_pem: &std::path::Path,
    ) -> Result<Self, WsError> {
        let acceptor = build_tls_acceptor(cert_pem, key_pem)?;
        Self::accept_tls_with_acceptor(tcp, local_proto, &acceptor).await
    }

    /// Upgrade an accepted TCP connection using a pre-built `TlsAcceptor`.
    ///
    /// Use this in listener loops where the cert and key are loaded once and
    /// the acceptor is reused for every incoming connection.
    #[cfg(feature = "wss")]
    pub async fn accept_tls_with_acceptor(
        tcp: TcpStream,
        local_proto: ProtocolId,
        acceptor: &tokio_rustls::TlsAcceptor,
    ) -> Result<Self, WsError> {
        let tls_stream = acceptor
            .accept(tcp)
            .await
            .map_err(|e| WsError::Tls(e.to_string()))?;
        let ws = ws_upgrade_tls(tls_stream, local_proto).await?;
        Ok(Self {
            inner: WsInner::AcceptTls(ws),
        })
    }

    /// Send an SP message as a single WebSocket binary frame.
    ///
    /// The SP protocol header bytes (e.g. the REQ request ID) are prepended to
    /// the body, matching what `FramedTransport` does on the TCP path.  The
    /// receiver places all bytes into `msg.body()`; the protocol state machine
    /// strips the header from the front via `trim_front`.
    pub async fn send(&mut self, msg: &Message) -> Result<(), WsError> {
        let header = msg.header();
        let body = msg.body();
        let mut payload = Vec::with_capacity(header.len() + body.len());
        payload.extend_from_slice(header);
        payload.extend_from_slice(body);
        let frame = WsMessage::Binary(payload.into());
        match &mut self.inner {
            WsInner::Dial(ws) => ws.send(frame).await?,
            WsInner::Accept(ws) => ws.send(frame).await?,
            #[cfg(feature = "wss")]
            WsInner::AcceptTls(ws) => ws.send(frame).await?,
        }
        Ok(())
    }

    /// Receive the next SP message from the WebSocket stream.
    ///
    /// Skips control frames (Ping, Pong) and text frames.
    /// `tokio-tungstenite` handles Ping→Pong automatically; Ping frames still
    /// appear in the stream and are skipped here.
    ///
    /// The full WebSocket message payload lands in `msg.body()`.  The protocol
    /// state machine strips its header bytes from the front of the body,
    /// identical to how the TCP path works.
    ///
    /// # Cancellation safety
    ///
    /// This future is cancellation-safe.  `tungstenite` reassembles fragmented
    /// WebSocket frames internally before yielding a message, so dropping this
    /// future between `next()` calls does not lose partial data.
    pub async fn recv(&mut self) -> Result<Message, WsError> {
        loop {
            let item = match &mut self.inner {
                WsInner::Dial(ws) => ws.next().await,
                WsInner::Accept(ws) => ws.next().await,
                #[cfg(feature = "wss")]
                WsInner::AcceptTls(ws) => ws.next().await,
            };
            match item {
                Some(Ok(WsMessage::Binary(data))) => {
                    let mut msg = Message::new();
                    msg.push_back(data.as_ref());
                    return Ok(msg);
                }
                Some(Ok(WsMessage::Close(_))) | None => return Err(WsError::Closed),
                Some(Ok(_)) => continue, // Ping, Pong, Text — ignore
                Some(Err(e)) => return Err(WsError::Tungstenite(e)),
            }
        }
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Build and validate the subprotocol-checking upgrade callback, then run the
/// WebSocket upgrade over a plain TCP stream.
async fn ws_upgrade(
    tcp: TcpStream,
    local_proto: ProtocolId,
) -> Result<WebSocketStream<TcpStream>, WsError> {
    let expected = local_proto.ws_subprotocol();
    let expected_clone = expected.clone();
    let callback =
        move |req: &WsRequest, mut resp: WsResponse| -> Result<WsResponse, ErrorResponse> {
            let client_proto = req
                .headers()
                .get("Sec-WebSocket-Protocol")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if client_proto != expected_clone {
                let mut err = ErrorResponse::new(Some("SP subprotocol mismatch".to_owned()));
                *err.status_mut() = StatusCode::BAD_REQUEST;
                return Err(err);
            }
            resp.headers_mut().insert(
                "Sec-WebSocket-Protocol",
                HeaderValue::from_str(&expected_clone).unwrap(),
            );
            Ok(resp)
        };
    tokio_tungstenite::accept_hdr_async(tcp, callback)
        .await
        .map_err(WsError::Tungstenite)
}

/// Same as `ws_upgrade` but over a TLS stream.
#[cfg(feature = "wss")]
async fn ws_upgrade_tls(
    tls: tokio_rustls::server::TlsStream<TcpStream>,
    local_proto: ProtocolId,
) -> Result<WebSocketStream<tokio_rustls::server::TlsStream<TcpStream>>, WsError> {
    let expected = local_proto.ws_subprotocol();
    let expected_clone = expected.clone();
    let callback =
        move |req: &WsRequest, mut resp: WsResponse| -> Result<WsResponse, ErrorResponse> {
            let client_proto = req
                .headers()
                .get("Sec-WebSocket-Protocol")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if client_proto != expected_clone {
                let mut err = ErrorResponse::new(Some("SP subprotocol mismatch".to_owned()));
                *err.status_mut() = StatusCode::BAD_REQUEST;
                return Err(err);
            }
            resp.headers_mut().insert(
                "Sec-WebSocket-Protocol",
                HeaderValue::from_str(&expected_clone).unwrap(),
            );
            Ok(resp)
        };
    tokio_tungstenite::accept_hdr_async(tls, callback)
        .await
        .map_err(WsError::Tungstenite)
}

/// Build a `TlsAcceptor` from PEM cert and key files.
#[cfg(feature = "wss")]
pub(crate) fn build_tls_acceptor(
    cert_pem: &std::path::Path,
    key_pem: &std::path::Path,
) -> Result<tokio_rustls::TlsAcceptor, WsError> {
    use rustls::ServerConfig;
    use std::sync::Arc;
    let certs = load_certs(cert_pem)?;
    let key = load_key(key_pem)?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| WsError::Tls(e.to_string()))?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

#[cfg(feature = "wss")]
fn load_certs(
    path: &std::path::Path,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, WsError> {
    let file = std::fs::File::open(path).map_err(|e| WsError::Tls(e.to_string()))?;
    let mut reader = std::io::BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| WsError::Tls(e.to_string()))
}

#[cfg(feature = "wss")]
fn load_key(path: &std::path::Path) -> Result<rustls::pki_types::PrivateKeyDer<'static>, WsError> {
    let file = std::fs::File::open(path).map_err(|e| WsError::Tls(e.to_string()))?;
    let mut reader = std::io::BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| WsError::Tls(e.to_string()))?
        .ok_or_else(|| WsError::Tls("no private key found in PEM file".to_owned()))
}

// ── Error type ────────────────────────────────────────────────────────────────

/// Errors from the WebSocket transport layer.
#[derive(thiserror::Error, Debug)]
pub enum WsError {
    /// Underlying tungstenite / WebSocket protocol error.
    #[error("WebSocket error: {0}")]
    Tungstenite(#[from] tokio_tungstenite::tungstenite::Error),
    /// Server responded with a different `Sec-WebSocket-Protocol` than requested.
    #[error("WebSocket subprotocol mismatch: expected {expected:?}, got {got:?}")]
    SubprotocolMismatch { expected: String, got: String },
    /// Invalid HTTP header value (should not occur with well-formed protocol names).
    #[error("invalid WebSocket header value: {0}")]
    InvalidHeader(String),
    /// The WebSocket connection was closed cleanly.
    #[error("WebSocket connection closed")]
    Closed,
    /// TLS configuration or handshake error (requires the `wss` feature).
    #[cfg(feature = "wss")]
    #[error("TLS error: {0}")]
    Tls(String),
}
