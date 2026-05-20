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
//!
//! # Buffer reuse
//!
//! Each call to [`FramedTransport::recv`] allocates a `Vec<u8>` sized to the
//! incoming frame. Callers that recv in a tight loop can amortize the
//! allocation with [`BufferPool`]: take a buffer from the pool via
//! [`FramedTransport::recv_pooled`], process the message, then return its body
//! buffer with [`BufferPool::recycle`]. After the first frame, the pool's
//! buffer is reused in place — `Vec::resize` is a no-op when the existing
//! capacity already covers the new frame length.

#[cfg(not(feature = "std"))]
extern crate alloc;
#[cfg(not(feature = "std"))]
use alloc::vec;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

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

/// Maximum frame payload accepted by [`FramedTransport::recv`].
///
/// Frames whose declared length exceeds this value are rejected with
/// [`TransportError::FrameTooLarge`] rather than attempting an allocation
/// that could exhaust memory or overflow address-space limits.
pub const MAX_FRAME_BYTES: usize = 1 << 30; // 1 GiB

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
    /// The remote declared a frame larger than [`MAX_FRAME_BYTES`].
    ///
    /// The inner value is the declared length. Either the remote is
    /// misbehaving or the message must be split at the application layer.
    FrameTooLarge(usize),
}

impl core::fmt::Display for TransportError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Handshake(e) => write!(f, "SP handshake error: {e}"),
            Self::Io => write!(f, "I/O error"),
            Self::Closed => write!(f, "connection closed"),
            Self::BadFrameType(t) => write!(f, "unexpected IPC frame type: {t:#04x}"),
            Self::FrameTooLarge(n) => {
                write!(f, "frame length {n} exceeds limit of {MAX_FRAME_BYTES}")
            }
        }
    }
}

impl From<CodecError> for TransportError {
    fn from(e: CodecError) -> Self {
        Self::Handshake(e)
    }
}

/// Per-field receive state for cancellation-safe `recv`.
///
/// Stored inside [`FramedTransport`] so that partial reads survive future
/// cancellation.  If a `recv()` future is dropped (e.g. by a `select!` branch
/// that fires before the recv completes), the bytes already pulled from the
/// underlying stream are preserved here.  The next `recv()` call resumes from
/// exactly where the cancelled one left off instead of starting a fresh read
/// that would treat mid-frame bytes as a new frame header.
struct RecvBuf {
    phase: RecvPhase,
    /// Scratch space for the length header (max 9 bytes for IPC format).
    len_buf: [u8; 9],
    len_filled: usize,
    /// Growing payload buffer; allocated once the frame length is known.
    body: Vec<u8>,
    body_filled: usize,
}

#[derive(PartialEq)]
enum RecvPhase {
    Idle,
    Len,
    Body,
}

impl Default for RecvBuf {
    fn default() -> Self {
        Self {
            phase: RecvPhase::Idle,
            len_buf: [0u8; 9],
            len_filled: 0,
            body: Vec::new(),
            body_filled: 0,
        }
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
///
/// `recv` is **cancellation-safe**: partial reads are buffered in the
/// `RecvBuf` stored inside the transport.  Dropping a `recv` future
/// mid-read and calling `recv` again on the same transport is safe and will
/// produce exactly one complete message, not a corrupted one.
pub struct FramedTransport<T> {
    inner: T,
    format: FrameFormat,
    rx: RecvBuf,
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

        Ok(Self {
            inner,
            format,
            rx: RecvBuf::default(),
        })
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
    ///
    /// This method is **cancellation-safe**: if the returned future is dropped
    /// before it resolves, any bytes already read from the stream are retained
    /// in the transport's internal `RecvBuf`.  The next call to `recv` picks
    /// up exactly where the cancelled call left off.
    pub async fn recv(&mut self) -> Result<Message, TransportError> {
        self.recv_inner(|len| vec![0u8; len]).await
    }

    /// Receive a complete message, drawing the body buffer from `pool` if one
    /// is available.
    ///
    /// Semantically identical to [`recv`](Self::recv). The only difference is
    /// the source of the body `Vec<u8>`: instead of always allocating, this
    /// method first tries `pool.take()`. After processing the returned message,
    /// call [`BufferPool::recycle`] to return the body buffer to the pool.
    ///
    /// In a steady-state loop where `recycle` puts the buffer back and the
    /// next frame fits within the recycled buffer's capacity,
    /// `Vec::resize` is a no-op and **no allocation occurs**.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use nng_core::BufferPool;
    ///
    /// let mut pool = BufferPool::new();
    /// loop {
    ///     let msg = transport.recv_pooled(&mut pool).await?;
    ///     handle(msg.body());
    ///     pool.recycle(msg);
    /// }
    /// ```
    ///
    /// See `examples/pool_consumer.rs` for a complete end-to-end program.
    pub async fn recv_pooled(&mut self, pool: &mut BufferPool) -> Result<Message, TransportError> {
        self.recv_inner(|len| {
            let mut buf = pool.take();
            buf.resize(len, 0);
            buf
        })
        .await
    }

    /// Shared body of `recv` / `recv_pooled`. The `alloc_body` closure is
    /// invoked exactly once per message, at the Len → Body phase transition.
    /// If the future is dropped mid-body and a subsequent call resumes in the
    /// Body phase, the closure is never invoked — `self.rx.body` already holds
    /// the partially-filled buffer.
    async fn recv_inner<F>(&mut self, alloc_body: F) -> Result<Message, TransportError>
    where
        F: FnOnce(usize) -> Vec<u8>,
    {
        let needed = match self.format {
            FrameFormat::Tcp => 8usize,
            FrameFormat::Ipc => 9usize,
        };

        // Phase 1 — read the frame-length header into self.rx.len_buf.
        if self.rx.phase == RecvPhase::Idle {
            self.rx.len_filled = 0;
            self.rx.phase = RecvPhase::Len;
        }

        if self.rx.phase == RecvPhase::Len {
            while self.rx.len_filled < needed {
                // Borrow inner and len_buf as disjoint fields.
                let filled = self.rx.len_filled;
                let n = self
                    .inner
                    .read(&mut self.rx.len_buf[filled..needed])
                    .await
                    .map_err(|_e| TransportError::Io)?;
                if n == 0 {
                    return Err(TransportError::Closed);
                }
                self.rx.len_filled += n;
            }

            let payload_len = match self.format {
                FrameFormat::Tcp => {
                    let bytes: [u8; 8] = self.rx.len_buf[..8].try_into().unwrap();
                    u64::from_be_bytes(bytes) as usize
                }
                FrameFormat::Ipc => {
                    if self.rx.len_buf[0] != 0x01 {
                        self.rx.phase = RecvPhase::Idle;
                        return Err(TransportError::BadFrameType(self.rx.len_buf[0]));
                    }
                    let bytes: [u8; 8] = self.rx.len_buf[1..9].try_into().unwrap();
                    u64::from_be_bytes(bytes) as usize
                }
            };

            if payload_len > MAX_FRAME_BYTES {
                self.rx.phase = RecvPhase::Idle;
                return Err(TransportError::FrameTooLarge(payload_len));
            }
            self.rx.body = alloc_body(payload_len);
            self.rx.body_filled = 0;
            self.rx.phase = RecvPhase::Body;
        }

        // Phase 2 — read the payload into self.rx.body.
        while self.rx.body_filled < self.rx.body.len() {
            let filled = self.rx.body_filled;
            let n = self
                .inner
                .read(&mut self.rx.body[filled..])
                .await
                .map_err(|_e| TransportError::Io)?;
            if n == 0 {
                return Err(TransportError::Closed);
            }
            self.rx.body_filled += n;
        }

        // Complete: hand the body out and reset for the next frame.
        self.rx.phase = RecvPhase::Idle;
        let payload = core::mem::replace(&mut self.rx.body, Vec::new());

        Ok(Message::from_parts(Vec::new(), payload))
    }

    /// Unwrap the inner stream.
    pub fn into_inner(self) -> T {
        self.inner
    }
}

// ── BufferPool ────────────────────────────────────────────────────────────────

/// Bounded LIFO pool of body buffers for [`FramedTransport::recv_pooled`].
///
/// `BufferPool` recycles the `Vec<u8>` body buffer from a received [`Message`]
/// so that the next recv can fill it in place instead of allocating fresh.
/// Pair `recv_pooled` with [`recycle`](Self::recycle) inside a hot loop:
///
/// ```rust,ignore
/// use nng_core::{BufferPool, transport::FramedTransport};
///
/// let mut pool = BufferPool::new();
/// loop {
///     let msg = transport.recv_pooled(&mut pool).await?;
///     // ... process msg ...
///     pool.recycle(msg);
/// }
/// ```
///
/// The pool is bounded by two limits, configurable via
/// [`with_capacity`](Self::with_capacity):
///
/// - `max_bufs` — number of buffers held simultaneously (defaults to 16).
/// - `max_buf_bytes` — buffers whose `capacity()` exceeds this are dropped on
///   `recycle` instead of being stored, so outlier large frames do not bloat
///   the pool (defaults to 64 KiB).
///
/// `BufferPool` is not internally synchronized; share a pool across tasks via
/// `Arc<Mutex<BufferPool>>` if needed, or use one pool per receiving task.
pub struct BufferPool {
    bufs: Vec<Vec<u8>>,
    max_bufs: usize,
    max_buf_bytes: usize,
}

impl BufferPool {
    /// Create a pool with defaults: at most 16 buffers, each ≤ 64 KiB.
    pub fn new() -> Self {
        Self::with_capacity(16, 64 * 1024)
    }

    /// Create a pool with explicit caps on buffer count and per-buffer size.
    pub fn with_capacity(max_bufs: usize, max_buf_bytes: usize) -> Self {
        Self {
            bufs: Vec::with_capacity(max_bufs),
            max_bufs,
            max_buf_bytes,
        }
    }

    /// Return a message's body buffer to the pool. The header is dropped.
    ///
    /// Buffers are dropped (not stored) when the pool is already at capacity
    /// or when the body's `capacity()` exceeds `max_buf_bytes`.
    pub fn recycle(&mut self, msg: Message) {
        let (_header, body) = msg.into_parts();
        if self.bufs.len() < self.max_bufs && body.capacity() <= self.max_buf_bytes {
            self.bufs.push(body);
        }
    }

    /// Number of buffers currently held in the pool.
    pub fn len(&self) -> usize {
        self.bufs.len()
    }

    /// `true` when the pool holds no buffers.
    pub fn is_empty(&self) -> bool {
        self.bufs.is_empty()
    }

    /// Pop a buffer from the pool, or return an empty `Vec` if the pool is
    /// empty. Used by [`FramedTransport::recv_pooled`].
    fn take(&mut self) -> Vec<u8> {
        self.bufs.pop().unwrap_or_default()
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new()
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

async fn write_all<T: Write>(w: &mut T, buf: &[u8]) -> Result<(), TransportError> {
    let mut written = 0;
    while written < buf.len() {
        match w.write(&buf[written..]).await {
            Ok(0) => return Err(TransportError::Closed),
            Ok(n) => written += n,
            Err(_e) => return Err(TransportError::Io),
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
            Err(_e) => return Err(TransportError::Io),
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
#[cfg(feature = "ws")]
pub mod ws;
#[cfg(feature = "ws")]
pub use ws::{WsError, WsTransport};

#[cfg(feature = "tls-tcp")]
pub mod tls_tcp;

#[cfg(feature = "quic")]
pub(crate) mod quic;

#[cfg(feature = "vsock")]
pub(crate) mod vsock;

#[cfg(feature = "dtls")]
pub mod dtls;
#[cfg(feature = "dtls")]
pub use dtls::DtlsTransport;

#[cfg(feature = "udp")]
pub mod udp;
#[cfg(feature = "udp")]
pub use udp::UdpTransport;
