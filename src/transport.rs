//! `FramedTransport<T>` — wraps any `embedded-io-async` byte stream with SP
//! handshake + message framing.
//!
//! After construction via [`FramedTransport::connect`] the handshake has
//! completed and `send` / `recv` exchange complete SP messages.

#[cfg(not(feature = "std"))]
extern crate alloc;
#[cfg(not(feature = "std"))]
use alloc::vec;

use embedded_io_async::{Read, Write};

use crate::{
    Message,
    codec::{
        CodecError, ProtocolId, check_peer, decode_handshake, encode_handshake,
    },
};

/// Error type for transport-layer operations.
#[derive(Debug)]
pub enum TransportError {
    /// The remote sent an invalid or incompatible SP handshake.
    Handshake(CodecError),
    /// An I/O error occurred on the underlying stream.
    Io,
    /// The connection was closed before the operation completed.
    Closed,
}

impl core::fmt::Display for TransportError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Handshake(e) => write!(f, "SP handshake error: {e}"),
            Self::Io => write!(f, "I/O error"),
            Self::Closed => write!(f, "connection closed"),
        }
    }
}

impl From<CodecError> for TransportError {
    fn from(e: CodecError) -> Self {
        Self::Handshake(e)
    }
}

/// A framed SP transport over any `embedded-io-async` `Read + Write` stream.
///
/// After construction the SP handshake has been completed; callers may then
/// call `send` and `recv` to exchange complete messages.
pub struct FramedTransport<T> {
    inner: T,
}

impl<T> FramedTransport<T>
where
    T: Read + Write,
{
    /// Perform the SP handshake and return a ready transport.
    ///
    /// Sends the local protocol's 8-byte header, then reads and validates the
    /// remote header.  Returns `Err` if the remote's protocol is incompatible.
    pub async fn connect(mut inner: T, local: ProtocolId) -> Result<Self, TransportError> {
        // Send our header.
        let tx = encode_handshake(local);
        write_all(&mut inner, &tx).await?;

        // Read remote header.
        let mut rx = [0u8; 8];
        read_exact(&mut inner, &mut rx).await?;

        let remote = decode_handshake(&rx)?;
        check_peer(local, remote)?;

        Ok(Self { inner })
    }

    /// Send a complete message.  The header bytes are sent before the body.
    pub async fn send(&mut self, msg: &Message) -> Result<(), TransportError> {
        let header = msg.header();
        let body = msg.body();
        let total = (header.len() + body.len()) as u64;

        write_all(&mut self.inner, &total.to_be_bytes()).await?;
        if !header.is_empty() {
            write_all(&mut self.inner, header).await?;
        }
        if !body.is_empty() {
            write_all(&mut self.inner, body).await?;
        }
        Ok(())
    }

    /// Receive a complete message.  All wire bytes (header + body) are placed
    /// in the message body; the protocol state machine splits them later.
    pub async fn recv(&mut self) -> Result<Message, TransportError> {
        let mut len_buf = [0u8; 8];
        read_exact(&mut self.inner, &mut len_buf).await?;
        let len = u64::from_be_bytes(len_buf) as usize;

        let mut payload = vec![0u8; len];
        if len > 0 {
            read_exact(&mut self.inner, &mut payload).await?;
        }

        let mut msg = Message::new();
        msg.push_back(&payload);
        Ok(msg)
    }

    /// Unwrap the inner stream.
    pub fn into_inner(self) -> T {
        self.inner
    }
}

// ── Helpers ──

async fn write_all<T: Write>(w: &mut T, buf: &[u8]) -> Result<(), TransportError> {
    let mut written = 0;
    while written < buf.len() {
        match w.write(&buf[written..]).await {
            Ok(0) => return Err(TransportError::Closed),
            Ok(n) => written += n,
            Err(_) => return Err(TransportError::Io),
        }
    }
    Ok(())
}

async fn read_exact<T: Read>(r: &mut T, buf: &mut [u8]) -> Result<(), TransportError> {
    let mut read = 0;
    while read < buf.len() {
        match r.read(&mut buf[read..]).await {
            Ok(0) => return Err(TransportError::Closed),
            Ok(n) => read += n,
            Err(_) => return Err(TransportError::Io),
        }
    }
    Ok(())
}

// ── Transport submodules ──

#[cfg(feature = "std")]
pub mod tcp;

// ── In-memory loopback (requires `std` / tokio) ──

#[cfg(feature = "std")]
pub mod loopback {
    //! In-memory loopback transport for testing.
    //!
    //! `inproc_pair(local, peer)` returns two `FramedTransport`s connected to
    //! each other via a tokio duplex stream.

    use super::{FramedTransport, TransportError};
    use crate::codec::ProtocolId;
    use embedded_io_async::{ErrorType, Read, Write};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

    /// Thin wrapper making `tokio::io::DuplexStream` implement
    /// `embedded-io-async`'s `Read` + `Write`.
    pub struct TokioDuplex(pub DuplexStream);

    impl ErrorType for TokioDuplex {
        type Error = std::io::Error;
    }

    impl Read for TokioDuplex {
        async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
            AsyncReadExt::read(&mut self.0, buf).await
        }
    }

    impl Write for TokioDuplex {
        async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
            AsyncWriteExt::write(&mut self.0, buf).await
        }

        async fn flush(&mut self) -> Result<(), Self::Error> {
            AsyncWriteExt::flush(&mut self.0).await
        }
    }

    /// Create a connected pair of `FramedTransport`s for use in tests.
    ///
    /// Both sides perform the SP handshake concurrently; the future resolves
    /// when both handshakes succeed.
    pub async fn inproc_pair(
        local: ProtocolId,
        peer: ProtocolId,
    ) -> Result<
        (FramedTransport<TokioDuplex>, FramedTransport<TokioDuplex>),
        TransportError,
    > {
        let (a, b) = tokio::io::duplex(64 * 1024);
        let (t1, t2) = tokio::try_join!(
            FramedTransport::connect(TokioDuplex(a), local),
            FramedTransport::connect(TokioDuplex(b), peer),
        )
        .map_err(|e| e)?;
        Ok((t1, t2))
    }
}
