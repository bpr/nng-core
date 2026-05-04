//! TCP transport adapter (requires `std` / tokio).
//!
//! Wraps `tokio::net::TcpStream` to implement `embedded-io-async`'s
//! `Read + Write` traits, enabling use with `FramedTransport`.

use embedded_io_async::{ErrorType, Read, Write};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

/// Wraps a `tokio::net::TcpStream` as an `embedded-io-async` stream.
pub struct TokioTcpStream(pub TcpStream);

impl ErrorType for TokioTcpStream {
    type Error = std::io::Error;
}

impl Read for TokioTcpStream {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.0.read(buf).await
    }
}

impl Write for TokioTcpStream {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        self.0.write(buf).await
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        self.0.flush().await
    }
}
