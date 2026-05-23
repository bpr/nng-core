//! PAIR0 socket API (bidirectional point-to-point).
//!
//! Both ends can send and receive freely with no protocol headers.

use crate::{
    Message,
    codec::ProtocolId,
    protocols::pair::Pair0State,
    socket::{NngError, ReconnectOptions, Socket},
};

/// Pair socket: bidirectional point-to-point messaging.
pub struct Pair0(Socket<Pair0State>);

impl Pair0 {
    forward_socket_method!(dial, ProtocolId::PAIR0, tuple);
    forward_socket_method!(listen, ProtocolId::PAIR0, tuple);
    forward_socket_method!(dial_reconnecting, ProtocolId::PAIR0, tuple);
    forward_socket_method!(dial_with_reconnect, ProtocolId::PAIR0, tuple);
    forward_socket_method!(listen_tls, ProtocolId::PAIR0, tuple);
    forward_socket_method!(listen_tls_tcp, ProtocolId::PAIR0, tuple);
    forward_socket_method!(dial_tls_tcp, ProtocolId::PAIR0, tuple);
    forward_socket_method!(listen_quic, ProtocolId::PAIR0, tuple);
    forward_socket_method!(dial_quic, ProtocolId::PAIR0, tuple);
    forward_socket_method!(listen_kcp_with, ProtocolId::PAIR0, tuple);
    forward_socket_method!(dial_kcp_with, ProtocolId::PAIR0, tuple);

    pub async fn send(&mut self, msg: Message) -> Result<(), NngError> {
        self.0.send_raw(&msg).await
    }

    pub async fn recv(&mut self) -> Result<Message, NngError> {
        self.0.recv_raw().await
    }

    /// Consume this socket and return a [`futures_core::Stream`] of
    /// received messages.  Requires the `streams` feature.
    ///
    /// The stream ends — yielding the error first — when the peer
    /// disconnects.  The returned stream is `Unpin`.
    #[cfg(feature = "streams")]
    pub fn into_stream(
        self,
    ) -> std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<Message, crate::NngError>> + Send>>
    {
        Box::pin(futures_util::stream::unfold(
            Some(self),
            |state| async move {
                let mut this = state?;
                match this.recv().await {
                    Ok(msg) => Some((Ok(msg), Some(this))),
                    Err(e) => Some((Err(e), None)),
                }
            },
        ))
    }

    /// Consume this socket and return a [`futures_sink::Sink`] that sends
    /// each flushed message to the connected peer.
    /// Requires the `streams` feature.
    ///
    /// The returned sink is `Unpin`.
    #[cfg(feature = "streams")]
    pub fn into_sink(
        self,
    ) -> std::pin::Pin<Box<dyn futures_sink::Sink<Message, Error = crate::NngError> + Send>> {
        Box::pin(futures_util::sink::unfold(
            self,
            |mut this: Self, msg: Message| async move {
                this.send(msg).await?;
                Ok(this)
            },
        ))
    }
}
