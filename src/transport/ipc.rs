//! IPC (Unix domain socket) transport adapter — Unix only.
//!
//! Wraps `tokio::net::UnixStream` to implement `embedded-io-async`'s
//! `Read + Write` traits, enabling use with `FramedTransport`.

use embedded_io_async::{ErrorType, Read, Write};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

/// Wraps a `tokio::net::UnixStream` as an `embedded-io-async` stream.
pub struct TokioUnixStream(pub UnixStream);

impl ErrorType for TokioUnixStream {
    type Error = std::io::Error;
}

impl Read for TokioUnixStream {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.0.read(buf).await
    }
}

impl Write for TokioUnixStream {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        self.0.write(buf).await
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        self.0.flush().await
    }
}
