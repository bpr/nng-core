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

    forward_socket_method!(dial, ProtocolId::REQ0, from_socket);
    forward_socket_method!(dial_reconnecting, ProtocolId::REQ0, from_socket);
    forward_socket_method!(dial_with_reconnect, ProtocolId::REQ0, from_socket);
    forward_socket_method!(dial_tls_tcp, ProtocolId::REQ0, from_socket);
    forward_socket_method!(dial_quic, ProtocolId::REQ0, from_socket);
    forward_socket_method!(dial_kcp_with, ProtocolId::REQ0, from_socket);

    /// Set the resend interval.
    ///
    /// When set, [`request`](Self::request) retransmits the request after
    /// `d` with no reply and repeats until a valid reply arrives or the
    /// transport returns an error.  Matches NNG's `NNG_OPT_REQ_RESENDTIME`.
    ///
    /// Resend is disabled by default; `request()` waits indefinitely.
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
