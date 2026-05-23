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
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum NngError {
    /// An OS or network I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// The SP handshake was rejected — wrong magic bytes or incompatible peer protocol.
    #[error("SP handshake failed: {0}")]
    HandshakeFailed(CodecError),
    /// The peer closed the connection cleanly.
    #[error("connection closed by peer")]
    ConnectionClosed,
    /// A frame length exceeded [`crate::transport::MAX_FRAME_BYTES`]; the peer is misbehaving.
    #[error("frame length {0} exceeds limit")]
    FrameTooLarge(usize),
    /// An IPC frame contained an unexpected type byte (NNG 1.5.x framing only).
    #[error("unexpected IPC frame type: {0:#04x}")]
    BadFrameType(u8),
    /// No peers are connected; the operation cannot proceed.
    #[error("no peers connected")]
    NoPeers,
    /// Reconnect failed after exhausting all configured attempts.
    #[error("reconnect failed: all attempts exhausted")]
    ReconnectExhausted,
    /// The URL scheme is not recognized.
    #[error("unsupported URL scheme: {0}")]
    UnsupportedScheme(String),
    /// The URL scheme requires a Cargo feature that was not enabled at compile time.
    #[error("URL scheme requires the `{0}` Cargo feature")]
    FeatureNotEnabled(&'static str),
    /// A protocol-level violation such as a WebSocket subprotocol mismatch or
    /// an unexpected request-ID in a REQ/REP exchange.
    #[error("protocol violation: {0}")]
    ProtocolViolation(String),
}

impl From<TransportError> for NngError {
    fn from(e: TransportError) -> Self {
        match e {
            TransportError::Handshake(c) => Self::HandshakeFailed(c),
            TransportError::Io(s) => Self::Io(io::Error::other(s)),
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
