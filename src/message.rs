//! NNG message types and the [`MessageBuf`] trait.
//!
//! # Two-part message structure
//!
//! Every NNG message has two logical regions:
//!
//! - **Header** — SP protocol metadata prepended by the protocol state machine:
//!   request IDs (REQ/REP), TTL hop counts (PAIR1), survey IDs (SURVEYOR), etc.
//!   On send the header is transmitted *before* the body. On receive all wire
//!   bytes land in the body; the state machine then strips its header off the
//!   front.
//! - **Body** — application payload, opaque to the transport layer.
//!
//! # Implementations
//!
//! | Type | Storage | `trim_front` | Requires |
//! |---|---|---|---|
//! | [`Message`] | `Vec<u8>` on the heap | O(n) — shifts bytes | `alloc` |
//! | [`ZeroCopyMessage<N>`] | `[u8; N]` on the stack | **O(1)** — advances a pointer | nothing |
//!
//! Both implement [`MessageBuf`], so all protocol state machines are generic
//! over either type.

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

// ── MessageBuf trait ──────────────────────────────────────────────────────────

/// Minimal interface over an NNG message buffer.
///
/// All SP protocol state machines are generic over `M: MessageBuf`, which lets
/// them operate on both heap-backed [`Message`] values and stack-allocated
/// [`ZeroCopyMessage`] values without any change to logic.
///
/// Only the five operations actually required by the state machines are
/// included. This keeps `no_alloc` implementations simple: there is no need
/// for `push_front`, `trim_back`, or any operation that implies reallocation.
///
/// # Implementing `MessageBuf`
///
/// The invariant callers rely on:
/// - After `trim_front(n)`, `body()` returns `n` fewer bytes from the front.
/// - After `header_push_back(data)`, `header()` returns `data` at its back.
/// - `body()` and `header()` return non-overlapping slices.
pub trait MessageBuf {
    /// The application payload.
    fn body(&self) -> &[u8];

    /// The protocol header (request IDs, routing info, TTL, …).
    fn header(&self) -> &[u8];

    /// Append `data` to the back of the body.
    fn push_back(&mut self, data: &[u8]);

    /// Append `data` to the back of the header.
    fn header_push_back(&mut self, data: &[u8]);

    /// Discard the first `n` bytes of the body.
    ///
    /// Used by protocol state machines to strip their header fields after
    /// reading them off the front of an incoming message body. In
    /// [`ZeroCopyMessage`] this is O(1); in [`Message`] it shifts bytes.
    fn trim_front(&mut self, n: usize);
}

// ── Message (heap-backed) ─────────────────────────────────────────────────────

/// A heap-allocated two-part NNG message: protocol `header` + application `body`.
///
/// Each part is stored in an independent `Vec<u8>`, so appending to either
/// region is amortized O(1). The downside is that [`trim_front`](Self::trim_front)
/// calls `Vec::drain` which is O(n) — it shifts the remaining bytes left.
/// Use [`ZeroCopyMessage`] when that matters.
///
/// Requires the `alloc` feature (enabled by default via `std`).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Message {
    header: Vec<u8>,
    body: Vec<u8>,
}

impl Message {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(header: usize, body: usize) -> Self {
        Self {
            header: Vec::with_capacity(header),
            body: Vec::with_capacity(body),
        }
    }

    // --- header ---

    pub fn header(&self) -> &[u8] {
        &self.header
    }

    /// Append bytes to the back of the header.
    pub fn header_push_back(&mut self, data: &[u8]) {
        self.header.extend_from_slice(data);
    }

    /// Prepend bytes to the front of the header.
    pub fn header_push_front(&mut self, data: &[u8]) {
        let old_len = self.header.len();
        self.header.resize(old_len + data.len(), 0);
        self.header.copy_within(0..old_len, data.len());
        self.header[..data.len()].copy_from_slice(data);
    }

    /// Remove `n` bytes from the front of the header.
    pub fn header_trim_front(&mut self, n: usize) {
        self.header.drain(..n);
    }

    /// Remove `n` bytes from the back of the header.
    pub fn header_trim_back(&mut self, n: usize) {
        let new_len = self.header.len().saturating_sub(n);
        self.header.truncate(new_len);
    }

    // --- body ---

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn body_mut(&mut self) -> &mut [u8] {
        &mut self.body
    }

    /// Append bytes to the back of the body (standard `write`-style).
    pub fn push_back(&mut self, data: &[u8]) {
        self.body.extend_from_slice(data);
    }

    /// Prepend bytes to the front of the body.
    pub fn push_front(&mut self, data: &[u8]) {
        let old_len = self.body.len();
        self.body.resize(old_len + data.len(), 0);
        self.body.copy_within(0..old_len, data.len());
        self.body[..data.len()].copy_from_slice(data);
    }

    /// Remove `n` bytes from the front of the body.
    pub fn trim_front(&mut self, n: usize) {
        self.body.drain(..n);
    }

    /// Remove `n` bytes from the back of the body.
    pub fn trim_back(&mut self, n: usize) {
        let new_len = self.body.len().saturating_sub(n);
        self.body.truncate(new_len);
    }

    // --- convenience ---

    /// The full body as a byte slice (alias for `body()`).
    pub fn as_slice(&self) -> &[u8] {
        &self.body
    }

    /// Total bytes in header + body.
    pub fn len(&self) -> usize {
        self.header.len() + self.body.len()
    }

    pub fn is_empty(&self) -> bool {
        self.header.is_empty() && self.body.is_empty()
    }

    /// Consume self, returning raw (header, body) vecs.
    pub fn into_parts(self) -> (Vec<u8>, Vec<u8>) {
        (self.header, self.body)
    }

    /// Reconstruct from (header, body) vecs.
    pub fn from_parts(header: Vec<u8>, body: Vec<u8>) -> Self {
        Self { header, body }
    }
}

impl MessageBuf for Message {
    fn body(&self) -> &[u8] {
        self.body()
    }
    fn header(&self) -> &[u8] {
        self.header()
    }
    fn push_back(&mut self, data: &[u8]) {
        self.push_back(data);
    }
    fn header_push_back(&mut self, data: &[u8]) {
        self.header_push_back(data);
    }
    fn trim_front(&mut self, n: usize) {
        self.trim_front(n);
    }
}

/// Appending to a `Message` writes to its body.
impl core::fmt::Write for Message {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        self.push_back(s.as_bytes());
        Ok(())
    }
}

/// Appending raw bytes to a `Message` writes to its body.
impl embedded_io_async::Write for Message {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        self.push_back(buf);
        Ok(buf.len())
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl embedded_io_async::ErrorType for Message {
    type Error = core::convert::Infallible;
}

// ── ZeroCopyMessage ───────────────────────────────────────────────────────────

/// A stack-allocated NNG message for `no_alloc` environments.
///
/// # Buffer layout
///
/// The `N`-byte backing array is split into a header region (the first `N/4`
/// bytes) and a body region (the remaining `3N/4` bytes):
///
/// ```text
///  index:  0       h_end   N/4        b_start       b_end        N
///          ├────────┼────────┼───────────┼─────────────┼───────────┤
///          │ header │ h-free │  trimmed  │    body     │  tailroom │
///          └────────┴────────┴───────────┴─────────────┴───────────┘
///                                         ↑ advances right on trim_front
/// ```
///
/// - `header()` returns `buf[0..h_end]`.
/// - `body()` returns `buf[b_start..b_end]`.
/// - `b_start` begins at `N/4` (right after the header region) and only ever
///   moves rightward. This is the key to **O(1) `trim_front`**: instead of
///   shifting bytes like `Vec::drain`, we just increment a pointer.
///
/// The "trimmed" zone between `N/4` and `b_start` is dead space — bytes that
/// have been consumed but not zeroed. This is the same trick used by Linux's
/// `sk_buff` network buffer for O(1) header stripping in the network stack.
///
/// # Choosing `N`
///
/// - Header capacity: `N / 4` bytes. SP protocol headers are at most 4 bytes
///   (one `u32`), so `N = 16` is enough for protocols; use more if you need
///   multi-layer routing headroom.
/// - Body capacity: `3N / 4` bytes, minus any bytes consumed by `trim_front`.
///
/// # Panics
///
/// Panics on capacity overflow or out-of-bounds `trim_front`. Size your buffer
/// at compile time — that is the entire point of this type.
pub struct ZeroCopyMessage<const N: usize> {
    buf: [u8; N],
    /// End of the used header region: `header() == buf[0..h_end]`.
    h_end: usize,
    /// Current start of the body, advances right on `trim_front`.
    b_start: usize,
    /// Current end of the body, advances right on `push_back`.
    b_end: usize,
}

impl<const N: usize> ZeroCopyMessage<N> {
    /// Bytes reserved at the front of the buffer for the header region.
    const HEADER_CAP: usize = N / 4;

    /// Construct a new, empty message.
    ///
    /// Both the header and body are empty; `b_start` is positioned right after
    /// the header region so the body can grow into `3N/4` bytes of tailroom.
    pub fn new() -> Self {
        Self {
            buf: [0; N],
            h_end: 0,
            b_start: Self::HEADER_CAP,
            b_end: Self::HEADER_CAP,
        }
    }

    /// Maximum number of header bytes this message can hold.
    pub const fn header_capacity() -> usize {
        Self::HEADER_CAP
    }

    /// Maximum number of body bytes this message can hold (initial, before any `trim_front`).
    pub const fn body_capacity() -> usize {
        N - Self::HEADER_CAP
    }

    /// Remaining body capacity: bytes available for further `push_back` calls.
    pub fn body_remaining(&self) -> usize {
        N - self.b_end
    }
}

// ── Kani proof harnesses ──────────────────────────────────────────────────────

#[cfg(kani)]
mod kani_proofs {
    //! Kani proof harnesses for the message buffer types.
    //!
    //! `kani::any::<T>()` is a symbolic value representing every possible `T`
    //! simultaneously; the solver checks all paths for all such inputs.
    //! `kani::assume(cond)` adds a precondition that constrains the inputs.
    //!
    //! `#[kani::unwind(N)]` sets the loop-unroll bound to `N`.  The rule is
    //! `N = max_loop_iterations + 1`; each harness documents which constant
    //! determines that maximum.
    //!
    //! `ZeroCopyMessage<32>` is used throughout: `HEADER_CAP = 8`,
    //! `BODY_CAP = 24`.  The small fixed size keeps verification fast while
    //! covering all index-arithmetic paths through the struct's pointer fields.

    use super::*;

    // ── ZeroCopyMessage (N=32: HEADER_CAP=8, BODY_CAP=24) ────────────────────

    /// push_back stores exactly the bytes that were passed.
    /// Unwind bound 25 = BODY_CAP (24) + 1, covering the copy loop.
    #[kani::proof]
    #[kani::unwind(25)]
    fn zcm_push_back_body_correct() {
        const BODY_CAP: usize = 24; // 3 * 32 / 4
        let len: usize = kani::any();
        kani::assume(len <= BODY_CAP);
        let data: [u8; BODY_CAP] = kani::any();

        let mut msg: ZeroCopyMessage<32> = ZeroCopyMessage::new();
        msg.push_back(&data[..len]);
        assert_eq!(msg.body(), &data[..len]);
    }

    /// trim_front(n) removes exactly the first n bytes from the body.
    /// Unwind bound 17 = N (16) + 1, covering the slice-comparison loop in
    /// the assert_eq.
    #[kani::proof]
    #[kani::unwind(17)]
    fn zcm_trim_front_body_correct() {
        const N: usize = 16;
        let data: [u8; N] = kani::any();
        let trim: usize = kani::any();
        kani::assume(trim <= N);

        let mut msg: ZeroCopyMessage<32> = ZeroCopyMessage::new();
        msg.push_back(&data);
        msg.trim_front(trim);
        assert_eq!(msg.body(), &data[trim..]);
    }

    /// Writing to the header region does not alias or corrupt the body, and
    /// writing to the body does not corrupt the header.
    /// Unwind bound 13 = hdr (4) + body (8) + 1, covering the copy and
    /// comparison loops across both regions.
    #[kani::proof]
    #[kani::unwind(13)]
    fn zcm_header_does_not_alias_body() {
        let hdr: [u8; 4] = kani::any(); // one u32 — typical SP header
        let body: [u8; 8] = kani::any();

        let mut msg: ZeroCopyMessage<32> = ZeroCopyMessage::new();
        msg.header_push_back(&hdr);
        msg.push_back(&body);
        assert_eq!(msg.header(), &hdr);
        assert_eq!(msg.body(), &body);
    }

    // ── Message (heap-backed, bounded inputs) ─────────────────────────────────

    /// push_back followed by trim_front(n) leaves exactly the suffix starting
    /// at byte n.  Unwind bound 17 = N (16) + 1, covering Vec's internal
    /// drain-and-shift loop plus the slice-comparison in assert_eq.
    #[kani::proof]
    #[kani::unwind(17)]
    fn message_push_back_trim_front_correct() {
        const N: usize = 16;
        let data: [u8; N] = kani::any();
        let trim: usize = kani::any();
        kani::assume(trim <= N);

        let mut msg = Message::new();
        msg.push_back(&data);
        msg.trim_front(trim);
        assert_eq!(msg.body(), &data[trim..]);
    }

    /// Header and body are independent: writing to one does not affect the
    /// other.  Unwind bound 9 = hdr (4) + body (4) + 1.
    #[kani::proof]
    #[kani::unwind(9)]
    fn message_header_body_independent() {
        let hdr: [u8; 4] = kani::any();
        let body: [u8; 4] = kani::any();

        let mut msg = Message::new();
        msg.header_push_back(&hdr);
        msg.push_back(&body);
        assert_eq!(msg.header(), &hdr);
        assert_eq!(msg.body(), &body);
    }
}

impl<const N: usize> Default for ZeroCopyMessage<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> MessageBuf for ZeroCopyMessage<N> {
    fn body(&self) -> &[u8] {
        &self.buf[self.b_start..self.b_end]
    }

    fn header(&self) -> &[u8] {
        &self.buf[..self.h_end]
    }

    fn push_back(&mut self, data: &[u8]) {
        let new_end = self.b_end + data.len();
        assert!(new_end <= N, "ZeroCopyMessage body overflow");
        self.buf[self.b_end..new_end].copy_from_slice(data);
        self.b_end = new_end;
    }

    fn header_push_back(&mut self, data: &[u8]) {
        let new_end = self.h_end + data.len();
        assert!(
            new_end <= Self::HEADER_CAP,
            "ZeroCopyMessage header overflow"
        );
        self.buf[self.h_end..new_end].copy_from_slice(data);
        self.h_end = new_end;
    }

    /// Advance the body start pointer by `n` bytes.
    ///
    /// This is O(1): no bytes are moved or zeroed. The vacated bytes become
    /// "trimmed" dead space that is never read again.
    fn trim_front(&mut self, n: usize) {
        assert!(
            self.b_start + n <= self.b_end,
            "ZeroCopyMessage trim_front out of bounds"
        );
        self.b_start += n;
    }
}
