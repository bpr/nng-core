//! [`FramedTransport<T>`] — wraps any `embedded-io-async` byte stream with SP
//! handshake + message framing.
//!
//! After construction via [`FramedTransport::connect`] the handshake has
//! completed and `send` / `recv` exchange complete SP messages.
//!
//! # Frame formats
//!
//! The SP handshake is identical for every transport variant. Only the
//! per-message frame differs:
//!
//! | Variant | Header | Used by |
//! |---|---|---|
//! | [`FrameFormat::Tcp`] | 8-byte BE u64 length | TCP, loopback, NNG ≥ 2.0 IPC |
//! | [`FrameFormat::Ipc`] | 1-byte type (`0x01`) + 8-byte BE u64 length | NNG 1.5.x IPC |
//!
//! The 9-byte IPC frame header is a NNG 1.5.x implementation detail. NNG 1.5.x
//! uses Unix domain sockets for IPC and prepends a type byte (`0x01`) before
//! the length so that future frame types could be distinguished. NNG 2.0 dropped
//! that type byte and aligns IPC framing with TCP. If you connect to a system
//! NNG installed from most Linux distributions (which ship 1.5.x), use
//! [`FrameFormat::Ipc`]; for NNG 2.x use [`FrameFormat::Tcp`].

#[cfg(not(feature = "std"))]
extern crate alloc;
#[cfg(not(feature = "std"))]
use alloc::vec;

use embedded_io_async::{Read, Write};

use crate::{
    Message,
    codec::{CodecError, ProtocolId, check_peer, decode_handshake, encode_handshake},
};

/// Wire framing variant — controls how the per-message length header is encoded.
///
/// The SP handshake (8-byte `\0SP\0` header) is identical for all variants.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FrameFormat {
    /// 8-byte BE u64 length header. Used by TCP connections and by NNG ≥ 2.0
    /// IPC. Also used by the in-memory loopback transport.
    Tcp,
    /// 1-byte type (`0x01`) followed by 8-byte BE u64 length. Used by NNG
    /// 1.5.x IPC (Unix domain sockets). The type byte is validated on receive;
    /// an unexpected value yields [`TransportError::BadFrameType`].
    Ipc,
}

/// Error type for transport-layer operations.
#[derive(Debug)]
pub enum TransportError {
    /// The remote sent an invalid or incompatible SP handshake.
    Handshake(CodecError),
    /// An I/O error occurred on the underlying stream.
    Io,
    /// The connection was closed before the operation completed.
    Closed,
    /// The remote sent an IPC frame whose type byte was not `0x01`.
    ///
    /// Only returned when using [`FrameFormat::Ipc`]. The inner value is the
    /// unexpected byte that was received.
    BadFrameType(u8),
}

impl core::fmt::Display for TransportError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Handshake(e) => write!(f, "SP handshake error: {e}"),
            Self::Io => write!(f, "I/O error"),
            Self::Closed => write!(f, "connection closed"),
            Self::BadFrameType(t) => write!(f, "unexpected IPC frame type: {t:#04x}"),
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
/// After [`connect`](Self::connect) returns, the SP handshake has completed and
/// both sides have verified protocol compatibility. All subsequent `send` /
/// `recv` calls exchange complete SP messages.
///
/// # Send path
///
/// `send` transmits the message header bytes immediately before the body bytes,
/// prefixed by the frame length. The receiver therefore sees header + body as
/// one contiguous payload and places the entire thing in the received message's
/// body — the protocol state machine splits them apart later.
///
/// # Receive path
///
/// `recv` reads the frame header (8 or 9 bytes depending on [`FrameFormat`]),
/// allocates a buffer of `length` bytes, fills it, and wraps it in a new
/// [`Message`] whose body is the entire payload. The caller's protocol state
/// machine then strips its own header fields off the front via `trim_front`.
pub struct FramedTransport<T> {
    inner: T,
    format: FrameFormat,
}

impl<T> FramedTransport<T>
where
    T: Read + Write,
{
    /// Perform the SP handshake and return a ready transport.
    ///
    /// Sends the local protocol's 8-byte header, then reads and validates the
    /// remote header. Returns `Err` if the remote's protocol is incompatible
    /// or if I/O fails during the handshake.
    pub async fn connect(
        mut inner: T,
        local: ProtocolId,
        format: FrameFormat,
    ) -> Result<Self, TransportError> {
        let tx = encode_handshake(local);
        write_all(&mut inner, &tx).await?;

        let mut rx = [0u8; 8];
        read_exact(&mut inner, &mut rx).await?;

        let remote = decode_handshake(&rx)?;
        check_peer(local, remote)?;

        Ok(Self { inner, format })
    }

    /// Send a complete message.
    ///
    /// The frame length covers both `header` and `body`. Header bytes are
    /// transmitted first, so the remote receives them as a prefix of the body.
    pub async fn send(&mut self, msg: &Message) -> Result<(), TransportError> {
        let header = msg.header();
        let body = msg.body();
        let total = (header.len() + body.len()) as u64;

        if self.format == FrameFormat::Ipc {
            write_all(&mut self.inner, &[0x01]).await?;
        }
        write_all(&mut self.inner, &total.to_be_bytes()).await?;
        if !header.is_empty() {
            write_all(&mut self.inner, header).await?;
        }
        if !body.is_empty() {
            write_all(&mut self.inner, body).await?;
        }
        Ok(())
    }

    /// Receive a complete message.
    ///
    /// All wire bytes (header + body concatenated by the sender) are placed
    /// into the returned message's body. The caller's protocol state machine
    /// is responsible for stripping protocol header fields from the front.
    pub async fn recv(&mut self) -> Result<Message, TransportError> {
        let len = match self.format {
            FrameFormat::Tcp => {
                let mut buf = [0u8; 8];
                read_exact(&mut self.inner, &mut buf).await?;
                u64::from_be_bytes(buf) as usize
            }
            FrameFormat::Ipc => {
                let mut buf = [0u8; 9];
                read_exact(&mut self.inner, &mut buf).await?;
                if buf[0] != 0x01 {
                    return Err(TransportError::BadFrameType(buf[0]));
                }
                let len_bytes: [u8; 8] = buf[1..9].try_into().unwrap();
                u64::from_be_bytes(len_bytes) as usize
            }
        };

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

// ── Internal helpers ──────────────────────────────────────────────────────────

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

// ── Transport submodules ──────────────────────────────────────────────────────

#[cfg(feature = "std")]
pub mod tcp;

#[cfg(all(feature = "std", unix))]
pub mod ipc;

// ── In-memory loopback (requires `std` / tokio) ───────────────────────────────

#[cfg(feature = "std")]
pub mod loopback {
    //! In-memory loopback transport for testing.
    //!
    //! [`inproc_pair`] returns two [`FramedTransport`]s connected back-to-back
    //! via a tokio duplex stream. Both sides race through the SP handshake
    //! concurrently; the future resolves when both succeed.

    use super::{FrameFormat, FramedTransport, TransportError};
    use crate::codec::ProtocolId;
    use embedded_io_async::{ErrorType, Read, Write};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

    /// Thin wrapper making [`tokio::io::DuplexStream`] implement
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

    /// Create a connected pair of [`FramedTransport`]s for use in tests.
    ///
    /// Both sides perform the SP handshake concurrently; the future resolves
    /// when both handshakes succeed.
    pub async fn inproc_pair(
        local: ProtocolId,
        peer: ProtocolId,
    ) -> Result<(FramedTransport<TokioDuplex>, FramedTransport<TokioDuplex>), TransportError> {
        let (a, b) = tokio::io::duplex(64 * 1024);
        let (t1, t2) = tokio::try_join!(
            FramedTransport::connect(TokioDuplex(a), local, FrameFormat::Tcp),
            FramedTransport::connect(TokioDuplex(b), peer, FrameFormat::Tcp),
        )
        .map_err(|e| e)?;
        Ok((t1, t2))
    }
}
