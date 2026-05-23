//! TCP transport adapter (requires `std` / tokio).
//!
//! Wraps `tokio::net::TcpStream` to implement `embedded-io-async`'s
//! `Read + Write` traits, enabling use with `FramedTransport`.

adapt_async_io!(pub TokioTcpStream wraps tokio::net::TcpStream);
