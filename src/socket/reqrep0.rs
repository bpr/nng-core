//! REQ0 / REP0 socket API.

use std::time::Duration;

use crate::{
    Message,
    codec::ProtocolId,
    protocols::reqrep::{Rep0State, Req0State, ReqRepError, RoutingInfo},
    socket::{NngError, ReconnectOptions, Socket},
};

/// Request socket: connects to a reply server, sends requests, awaits replies.
///
/// The request ID counter is per-socket and increments across calls, so a
/// stale reply from a previous call is detected and silently discarded.
pub struct Req0 {
    inner: Socket<Req0State>,
    state: Req0State,
    /// When `Some`, the request is resent after this interval if no reply
    /// arrives.  Equivalent to NNG's `NNG_OPT_REQ_RESENDTIME`.
    resend_time: Option<Duration>,
}

impl Req0 {
    fn from_socket(inner: Socket<Req0State>) -> Self {
        Self {
            inner,
            state: Req0State::new(),
            resend_time: None,
        }
    }

    /// Start configuring a new request socket via the builder API.
    ///
    /// Preferred over the bare `dial` constructors when you need to set
    /// any per-socket option (currently just `resend_time`, with more
    /// likely to come).  The builder collects options first and applies
    /// them atomically once a transport is dialed:
    ///
    /// ```ignore
    /// use std::time::Duration;
    /// use nng_core::socket::reqrep0::Req0;
    ///
    /// let req = Req0::builder()
    ///     .resend_time(Duration::from_secs(2))
    ///     .dial("tcp://127.0.0.1:5555")
    ///     .await?;
    /// ```
    pub fn builder() -> Req0Builder {
        Req0Builder::default()
    }

    forward_socket_method!(dial, ProtocolId::REQ0, from_socket);
    forward_socket_method!(dial_reconnecting, ProtocolId::REQ0, from_socket);
    forward_socket_method!(dial_with_reconnect, ProtocolId::REQ0, from_socket);
    forward_socket_method!(dial_tls_tcp, ProtocolId::REQ0, from_socket);
    forward_socket_method!(dial_quic, ProtocolId::REQ0, from_socket);
    forward_socket_method!(dial_kcp_with, ProtocolId::REQ0, from_socket);

    /// Set the resend interval at runtime.
    ///
    /// When set, [`request`](Self::request) retransmits the request after
    /// `d` with no reply and repeats until a valid reply arrives or the
    /// transport returns an error.  Matches NNG's `NNG_OPT_REQ_RESENDTIME`.
    ///
    /// Resend is disabled by default; `request()` waits indefinitely.
    ///
    /// Prefer [`Req0::builder().resend_time(d).dial(addr)`](Req0Builder::resend_time)
    /// for initial configuration; this setter is intended for runtime
    /// adjustment of an already-dialed socket.
    pub fn set_resend_time(&mut self, d: Duration) {
        self.resend_time = Some(d);
    }

    /// Send `msg` and receive the reply.
    ///
    /// The request ID is stamped once for this call and reused verbatim on
    /// every retransmit, so the server cannot distinguish an original from
    /// a resend.  Stale replies — from a previous call's resend that arrived
    /// after the call returned — are detected by ID mismatch and silently
    /// discarded; `request` then waits for the correct reply.
    ///
    /// Returns an error only when the transport fails; a timeout (when
    /// `set_resend_time` is configured) triggers a retransmit, not an error.
    pub async fn request(&mut self, msg: Message) -> Result<Message, NngError> {
        let mut outgoing = msg;
        let sent_id = self.state.prepare_outgoing(&mut outgoing);

        'send: loop {
            self.inner.send_raw(&outgoing).await?;

            loop {
                let raw = match self.resend_time {
                    None => self.inner.recv_raw().await?,
                    Some(t) => match tokio::time::timeout(t, self.inner.recv_raw()).await {
                        Ok(r) => r?,
                        Err(_elapsed) => continue 'send,
                    },
                };

                let mut reply = raw;
                match self.state.process_incoming(&mut reply, sent_id) {
                    Ok(()) => return Ok(reply),
                    Err(ReqRepError::IdMismatch { .. }) => {} // stale reply, keep waiting
                    Err(e) => return Err(NngError::ProtocolViolation(e.to_string())),
                }
            }
        }
    }
}

/// Builder for [`Req0`].
///
/// Collect per-socket options here, then call a terminal `dial*` method to
/// open the transport.  Every dial method on this type mirrors the
/// matching `Req0::dial*` static method; the only difference is that the
/// builder's options are applied to the resulting socket before it's
/// returned.
///
/// Construct via [`Req0::builder`].
#[derive(Debug, Default, Clone)]
pub struct Req0Builder {
    resend_time: Option<Duration>,
}

impl Req0Builder {
    /// Resend the outgoing request every `d` if no reply arrives.
    ///
    /// Matches NNG's `NNG_OPT_REQ_RESENDTIME`.  Disabled by default
    /// ([`Req0::request`] waits indefinitely).
    pub fn resend_time(mut self, d: Duration) -> Self {
        self.resend_time = Some(d);
        self
    }

    fn apply(&self, mut req: Req0) -> Req0 {
        req.resend_time = self.resend_time;
        req
    }

    /// Dial `addr` and return the configured socket.
    pub async fn dial(self, addr: &str) -> Result<Req0, NngError> {
        Req0::dial(addr).await.map(|r| self.apply(r))
    }

    /// Dial with automatic reconnect using default backoff (100 ms → 30 s).
    pub async fn dial_reconnecting(self, addr: &str) -> Result<Req0, NngError> {
        Req0::dial_reconnecting(addr).await.map(|r| self.apply(r))
    }

    /// Dial with automatic reconnect using custom `ReconnectOptions`.
    pub async fn dial_with_reconnect(
        self,
        addr: &str,
        opts: ReconnectOptions,
    ) -> Result<Req0, NngError> {
        Req0::dial_with_reconnect(addr, opts)
            .await
            .map(|r| self.apply(r))
    }

    /// Dial a `tls+tcp://` server with a custom TLS client config.
    /// Requires the `tls-tcp` feature.
    #[cfg(feature = "tls-tcp")]
    pub async fn dial_tls_tcp(
        self,
        addr: &str,
        client_config: std::sync::Arc<rustls::ClientConfig>,
    ) -> Result<Req0, NngError> {
        Req0::dial_tls_tcp(addr, client_config)
            .await
            .map(|r| self.apply(r))
    }

    /// Dial a `quic://` server with a custom TLS client config.
    /// Requires the `quic` feature.
    #[cfg(feature = "quic")]
    pub async fn dial_quic(
        self,
        addr: &str,
        client_config: std::sync::Arc<rustls::ClientConfig>,
    ) -> Result<Req0, NngError> {
        Req0::dial_quic(addr, client_config)
            .await
            .map(|r| self.apply(r))
    }

    /// Dial a `kcp://` server with a caller-supplied `KcpConfig`.
    /// Requires the `kcp` feature.  Both peers must use the same config.
    #[cfg(feature = "kcp")]
    pub async fn dial_kcp_with(
        self,
        addr: &str,
        config: kcp_tokio::KcpConfig,
    ) -> Result<Req0, NngError> {
        Req0::dial_kcp_with(addr, config)
            .await
            .map(|r| self.apply(r))
    }
}

/// Reply socket: listens for connections from requesters.
pub struct Rep0 {
    inner: Socket<Rep0State>,
    state: Rep0State,
}

/// Consumed by `reply()` to enforce that each receive has exactly one reply.
pub struct Responder<'a> {
    socket: &'a mut Socket<Rep0State>,
    routing: RoutingInfo,
}

impl<'a> Responder<'a> {
    pub async fn reply(self, msg: Message) -> Result<(), NngError> {
        let state = Rep0State::new();
        let mut outgoing = msg;
        state.prepare_reply(&mut outgoing, &self.routing);
        self.socket.send_raw(&outgoing).await
    }
}

impl Rep0 {
    forward_socket_method!(listen, ProtocolId::REP0, rep0);
    forward_socket_method!(listen_tls, ProtocolId::REP0, rep0);
    forward_socket_method!(listen_tls_tcp, ProtocolId::REP0, rep0);
    forward_socket_method!(listen_quic, ProtocolId::REP0, rep0);
    forward_socket_method!(listen_kcp_with, ProtocolId::REP0, rep0);

    /// Receive the next request.  Returns the application message plus a
    /// `Responder` that must be used to send the reply.
    pub async fn receive(&mut self) -> Result<(Message, Responder<'_>), NngError> {
        let mut msg = self.inner.recv_raw().await?;
        let routing = self
            .state
            .process_incoming(&mut msg)
            .map_err(|e| NngError::ProtocolViolation(e.to_string()))?;
        Ok((
            msg,
            Responder {
                socket: &mut self.inner,
                routing,
            },
        ))
    }
}
