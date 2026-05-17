//! Error type returned by all nng-core socket operations.

use std::io;

use crate::{codec::CodecError, transport::TransportError};

/// All failure modes that nng-core socket operations can return.
///
/// Callers can match on specific variants to distinguish recoverable conditions
/// (e.g. [`NngError::ConnectionClosed`] → wait and retry) from permanent ones
/// (e.g. [`NngError::HandshakeFailed`] → wrong protocol on the other side).
///
/// # Backward compatibility
///
/// This type implements `From<NngError> for std::io::Error`, so code that
/// boxes errors or uses `anyhow` / `thiserror` continues to work.
///
/// # Example
///
/// ```rust,ignore
/// use nng_core::{NngError, socket::reqrep0::Req0};
///
/// match req.request(msg).await {
///     Ok(reply) => { /* handle reply */ }
///     Err(NngError::ConnectionClosed) => { /* peer restarted */ }
///     Err(NngError::ReconnectExhausted) => { /* gave up retrying */ }
///     Err(NngError::HandshakeFailed(e)) => { /* wrong peer protocol: {e} */ }
///     Err(NngError::Io(e)) => { /* OS-level error: {e} */ }
///     Err(e) => { /* other: {e} */ }
/// }
/// ```
#[derive(Debug)]
#[non_exhaustive]
pub enum NngError {
    /// An OS or network I/O error.
    Io(io::Error),
    /// The SP handshake was rejected — wrong magic bytes or incompatible peer protocol.
    HandshakeFailed(CodecError),
    /// The peer closed the connection cleanly.
    ConnectionClosed,
    /// A frame length exceeded [`crate::transport::MAX_FRAME_BYTES`]; the peer is misbehaving.
    FrameTooLarge(usize),
    /// An IPC frame contained an unexpected type byte (NNG 1.5.x framing only).
    BadFrameType(u8),
    /// No peers are connected; the operation cannot proceed.
    NoPeers,
    /// Reconnect failed after exhausting all configured attempts.
    ReconnectExhausted,
    /// The URL scheme is not recognized.
    UnsupportedScheme(String),
    /// The URL scheme requires a Cargo feature that was not enabled at compile time.
    FeatureNotEnabled(&'static str),
    /// A protocol-level violation such as a WebSocket subprotocol mismatch or
    /// an unexpected request-ID in a REQ/REP exchange.
    ProtocolViolation(String),
}

impl std::fmt::Display for NngError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::HandshakeFailed(e) => write!(f, "SP handshake failed: {e}"),
            Self::ConnectionClosed => write!(f, "connection closed by peer"),
            Self::FrameTooLarge(n) => {
                write!(f, "frame length {n} exceeds limit")
            }
            Self::BadFrameType(b) => write!(f, "unexpected IPC frame type: {b:#04x}"),
            Self::NoPeers => write!(f, "no peers connected"),
            Self::ReconnectExhausted => write!(f, "reconnect failed: all attempts exhausted"),
            Self::UnsupportedScheme(s) => write!(f, "unsupported URL scheme: {s}"),
            Self::FeatureNotEnabled(feat) => {
                write!(f, "URL scheme requires the `{feat}` Cargo feature")
            }
            Self::ProtocolViolation(msg) => write!(f, "protocol violation: {msg}"),
        }
    }
}

impl std::error::Error for NngError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for NngError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<TransportError> for NngError {
    fn from(e: TransportError) -> Self {
        match e {
            TransportError::Handshake(c) => Self::HandshakeFailed(c),
            TransportError::Io => Self::Io(io::Error::other("transport I/O error")),
            TransportError::Closed => Self::ConnectionClosed,
            TransportError::BadFrameType(b) => Self::BadFrameType(b),
            TransportError::FrameTooLarge(n) => Self::FrameTooLarge(n),
        }
    }
}

impl From<NngError> for io::Error {
    fn from(e: NngError) -> Self {
        match e {
            NngError::Io(e) => e,
            e => io::Error::other(e.to_string()),
        }
    }
}
