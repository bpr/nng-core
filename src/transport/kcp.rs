//! KCP transport adapter.
//!
//! Wraps [`kcp_tokio::KcpStream`] to implement `embedded-io-async`'s
//! `Read + Write` traits, enabling use with [`FramedTransport`].
//!
//! KCP is a reliable, ordered ARQ protocol layered over UDP, similar in role
//! to QUIC but without TLS.  The SP 8-byte handshake and TCP-style 8-byte
//! length-prefix framing flow through unchanged — KCP gives us an ordered
//! reliable byte stream which is all `FramedTransport` requires.
//!
//! # URL scheme
//!
//! `kcp://host:port` — for example `kcp://127.0.0.1:5555`.  DNS names are
//! resolved at dial/bind time via [`tokio::net::lookup_host`].
//!
//! # Wire compatibility
//!
//! KCP is **not** part of the NNG/nanomsg ecosystem.  This transport is
//! intended for nng-core ↔ nng-core communication only.  There is no interop
//! with C NNG.
//!
//! # Cancellation safety
//!
//! [`KcpListener::accept`] is cancellation-safe — internally it delegates to
//! [`tokio::sync::mpsc::Receiver::recv`], which Tokio documents as safe to
//! cancel.  This means [`AnyListener::accept_raw`] over KCP can be used
//! inside `biased select!` drain loops (as `Bus0::accept_pending` does)
//! without losing pending connections.  The `tests/kcp_multipeer.rs`
//! `bus0_kcp_dynamic_membership` test exercises this path.
//!
//! [`AnyListener::accept_raw`]: crate::socket::AnyListener::accept_raw
//!
//! # Listener keepalive (server side)
//!
//! Unlike TCP, a server-side [`KcpStream`] returned by [`KcpListener::accept`]
//! depends on the listener: the listener owns the shared UDP socket and runs
//! a background task that routes inbound UDP packets into the right stream's
//! receive queue.  `KcpListener::Drop` aborts that task, after which accepted
//! streams stop receiving data.  Because `Socket::listen` drops the
//! [`crate::socket::AnyListener`] right after one accept, we must keep an
//! [`Arc`] to the listener alive *inside* the accepted [`TokioKcpStream`] —
//! that's what the `Option<Arc<TokioMutex<KcpListener>>>` field does.  Dialer
//! streams (built via [`KcpStream::connect`]) own their own UDP socket and
//! pass `None`.
//!
//! [`FramedTransport`]: crate::transport::FramedTransport

use std::io;
use std::net::SocketAddr;

use kcp_tokio::{KcpConfig, KcpListener, KcpStream};

/// Parse a `host:port` string (kcp:// scheme already stripped, plus any
/// trailing `/path` component dropped) into a [`SocketAddr`] via DNS lookup.
pub(crate) async fn parse_kcp_addr(s: &str) -> io::Result<SocketAddr> {
    let host_port = s.split('/').next().unwrap_or(s);
    tokio::net::lookup_host(host_port)
        .await?
        .next()
        .ok_or_else(|| io::Error::other(format!("could not resolve KCP address: {host_port}")))
}

/// Build the default [`KcpConfig`] for SP traffic.
///
/// The initial implementation just returns `KcpConfig::default()`.  Tuning
/// (MTU, snd_wnd, rcv_wnd, nodelay/interval, fast_resend) is deferred to a
/// future step.  Both sides MUST use the same config for correct ARQ
/// behavior, so once we expose tuning knobs they will need to be set
/// symmetrically.
pub(crate) fn default_kcp_config() -> KcpConfig {
    KcpConfig::default()
}

/// Bind a [`KcpListener`] at `addr` using the default [`KcpConfig`].
///
/// The listener binds a UDP socket immediately; no connections are awaited
/// here.  KCP accept happens in [`crate::socket::AnyListener::accept_as_transport`].
pub(crate) async fn bind_kcp_listener(addr: &str) -> io::Result<KcpListener> {
    bind_kcp_listener_with(addr, default_kcp_config()).await
}

/// Bind a [`KcpListener`] at `addr` using a caller-supplied [`KcpConfig`].
///
/// Both peers MUST use the same config (MTU, nodelay, snd_wnd, rcv_wnd,
/// fast_resend, interval, etc.) for correct ARQ behavior.
pub(crate) async fn bind_kcp_listener_with(
    addr: &str,
    config: KcpConfig,
) -> io::Result<KcpListener> {
    let socket_addr = parse_kcp_addr(addr).await?;
    KcpListener::bind(socket_addr, config)
        .await
        .map_err(|e| io::Error::other(format!("kcp bind failed: {e}")))
}

/// Connect a [`KcpStream`] to `addr` using the default [`KcpConfig`].
pub(crate) async fn connect_kcp_stream(addr: &str) -> io::Result<KcpStream> {
    connect_kcp_stream_with(addr, default_kcp_config()).await
}

/// Connect a [`KcpStream`] to `addr` using a caller-supplied [`KcpConfig`].
///
/// Both peers MUST use the same config — see [`bind_kcp_listener_with`].
pub(crate) async fn connect_kcp_stream_with(
    addr: &str,
    config: KcpConfig,
) -> io::Result<KcpStream> {
    let socket_addr = parse_kcp_addr(addr).await?;
    KcpStream::connect(socket_addr, config)
        .await
        .map_err(|e| io::Error::other(format!("kcp connect failed: {e}")))
}

adapt_async_io! {
    /// Wraps a [`kcp_tokio::KcpStream`] as an `embedded-io-async` stream.
    ///
    /// The second field is a listener keepalive — see the module-level docs.
    /// Dialer streams set it to `None`; accepted streams set it to a clone
    /// of the listener's [`std::sync::Arc`].  `flush_each_write` is needed
    /// because `kcp_tokio` buffers writes into the next outgoing KCP segment
    /// until `poll_flush`; without it the SP handshake hangs.  See the
    /// module-level docs for the full story.
    pub(crate) TokioKcpStream wraps KcpStream,
    flush_each_write,
    extra: (
        /// Held only for its `Drop`: keeps the listener (and its UDP-routing
        /// background task) alive for the lifetime of an accepted stream.
        /// `None` for dialer streams, which own their own UDP socket.
        #[allow(dead_code)]
        pub Option<std::sync::Arc<tokio::sync::Mutex<KcpListener>>>,
    )
}
