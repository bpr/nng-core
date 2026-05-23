//! IPC (Unix domain socket) transport adapter — Unix only.
//!
//! Wraps `tokio::net::UnixStream` to implement `embedded-io-async`'s
//! `Read + Write` traits, enabling use with `FramedTransport`.

adapt_async_io!(pub TokioUnixStream wraps tokio::net::UnixStream);
