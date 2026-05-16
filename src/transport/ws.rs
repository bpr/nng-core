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
}

/// WebSocket transport for the SP protocol.
///
/// Wraps a `WebSocketStream` and maps SP messages to WebSocket binary frames.
/// There is no length prefix: WebSocket itself provides message framing, and
/// protocol identification is handled by the HTTP `Sec-WebSocket-Protocol`
/// header during the upgrade handshake.
pub struct WsTransport {
    inner: WsInner,
}

impl WsTransport {
    /// Open a WebSocket connection to `url` as the given `local_proto`.
    ///
    /// Sends the peer's `Sec-WebSocket-Protocol` subprotocol string in the
    /// HTTP upgrade request and validates that the server echoes it back.
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

    /// Upgrade an accepted TCP connection to a WebSocket SP transport.
    ///
    /// Validates that the client's `Sec-WebSocket-Protocol` matches the
    /// listener's own protocol subprotocol string.  Rejects with HTTP 400 on
    /// mismatch — HTTP 400 is the correct rejection at the upgrade stage
    /// (close code 1002 requires a fully established WebSocket connection).
    pub async fn accept(tcp: TcpStream, local_proto: ProtocolId) -> Result<Self, WsError> {
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

        let ws = tokio_tungstenite::accept_hdr_async(tcp, callback).await?;
        Ok(Self {
            inner: WsInner::Accept(ws),
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

/// Errors from the WebSocket transport layer.
#[derive(Debug)]
pub enum WsError {
    /// Underlying tungstenite / WebSocket protocol error.
    Tungstenite(tokio_tungstenite::tungstenite::Error),
    /// Server responded with a different `Sec-WebSocket-Protocol` than requested.
    SubprotocolMismatch { expected: String, got: String },
    /// Invalid HTTP header value (should not occur with well-formed protocol names).
    InvalidHeader(String),
    /// The WebSocket connection was closed cleanly.
    Closed,
}

impl From<tokio_tungstenite::tungstenite::Error> for WsError {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::Tungstenite(e)
    }
}

impl core::fmt::Display for WsError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Tungstenite(e) => write!(f, "WebSocket error: {e}"),
            Self::SubprotocolMismatch { expected, got } => {
                write!(
                    f,
                    "WebSocket subprotocol mismatch: expected {expected:?}, got {got:?}"
                )
            }
            Self::InvalidHeader(s) => write!(f, "invalid WebSocket header value: {s}"),
            Self::Closed => write!(f, "WebSocket connection closed"),
        }
    }
}
