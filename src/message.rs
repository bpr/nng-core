//! NNG message with separate header and body buffers.
//!
//! The header carries SP protocol metadata (request IDs, routing info, TTL, survey IDs).
//! The body carries application payload. Both are manipulated via prepend/append/trim operations
//! that avoid copying when possible.

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// A two-part NNG message: protocol `header` + application `body`.
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
