//! UDP transport for the SP protocol.
//!
//! UDP is connectionless, so this transport diverges from [`FramedTransport`]
//! in several ways:
//!
//! - **No SP handshake.**  UDP sockets are not connected in the TCP sense; the
//!   `\0SP\0` 8-byte handshake is omitted entirely.
//! - **No length prefix.**  UDP preserves message boundaries; the receiver gets
//!   exactly the bytes the sender wrote.
//! - **Payload convention.**  Following the same rule as [`WsTransport`]: the
//!   sender concatenates `msg.header()` then `msg.body()` into one datagram;
//!   the receiver places all datagram bytes into `msg.body()`.  The protocol
//!   state machine strips its header via `trim_front`, identical to TCP.
//! - **Peer discovery.**  The listener side starts with no known peer address.
//!   The first [`recv`](UdpTransport::recv) call records the sender's address
//!   so that subsequent [`send`](UdpTransport::send) calls know where to reply.
//!   The dialer side connects the socket at construction, so `send` is always
//!   ready.
//! - **Reliability.**  None.  Datagrams may be lost, reordered, or duplicated.
//!   Higher-level retry logic (e.g. REQ resend) is the application's concern.
//! - **Message size.**  Limited to one datagram (≤ 65,507 bytes for IPv4 UDP,
//!   less in practice — standard Ethernet MTU gives ~1,400 bytes payload).
//!   No fragmentation.
//!
//! [`FramedTransport`]: crate::transport::FramedTransport
//! [`WsTransport`]: crate::transport::ws::WsTransport

use std::io;
use std::net::SocketAddr;

use tokio::net::UdpSocket;

use crate::Message;

/// Maximum datagram payload this transport will allocate for receive buffers.
///
/// Matches the maximum IPv4 UDP payload (65,535 bytes minus the 8-byte UDP
/// header).  In practice, payloads larger than the path MTU (~1,400 bytes for
/// standard Ethernet) will be fragmented by IP and are best avoided.
pub const MAX_UDP_PAYLOAD: usize = 65_507;

/// SP transport over UDP.
///
/// Constructed via [`bind`](Self::bind) (listener side) or
/// [`connect`](Self::connect) (dialer side).
///
/// **Peer discovery:** The listener has no peer address until the first
/// [`recv`](Self::recv) call, which records the sender's address.  Calling
/// [`send`](Self::send) before any `recv` on a listener returns an error.
/// The dialer always has a peer address from construction.
pub struct UdpTransport {
    socket: UdpSocket,
    /// Known peer address.  `None` on a freshly bound listener; set to `Some`
    /// after the first received datagram or at construction for dialers.
    peer: Option<SocketAddr>,
    /// `true` when `socket.connect()` was called (dialer side).  Determines
    /// whether to use `send()` vs `send_to()`.
    connected: bool,
}

impl UdpTransport {
    /// Bind to `addr` without connecting to any peer.
    ///
    /// The returned transport is ready to receive datagrams from any sender.
    /// The peer address is learned from the first [`recv`](Self::recv) call.
    /// `addr` must be in `host:port` form (e.g. `"0.0.0.0:5555"`).
    pub async fn bind(addr: &str) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr).await?;
        Ok(Self::from_socket(socket))
    }

    /// Wrap an already-bound [`UdpSocket`] as a listener-side transport.
    ///
    /// Use this when you need to determine the bound address before handing
    /// control to the transport (e.g., to avoid a bind-probe TOCTOU race in
    /// tests).  The peer address is still learned from the first `recv` call.
    pub fn from_socket(socket: UdpSocket) -> Self {
        Self {
            socket,
            peer: None,
            connected: false,
        }
    }

    /// Bind to an ephemeral local port and connect to `remote`.
    ///
    /// `remote` must be in `host:port` form (e.g. `"127.0.0.1:5555"`).
    /// Hostname resolution is attempted if `remote` is not a bare IP.
    ///
    /// The UDP-level `connect` filters incoming datagrams to only those from
    /// `remote` and sets the default send destination, so neither `send_to`
    /// nor an explicit address is needed on this side.
    pub async fn connect(remote: &str) -> io::Result<Self> {
        let peer = tokio::net::lookup_host(remote)
            .await?
            .next()
            .ok_or_else(|| io::Error::other(format!("could not resolve UDP remote: {remote}")))?;
        let bind_addr: SocketAddr = if peer.is_ipv6() {
            "[::]:0".parse().unwrap()
        } else {
            "0.0.0.0:0".parse().unwrap()
        };
        let socket = UdpSocket::bind(bind_addr).await?;
        socket.connect(peer).await?;
        Ok(Self {
            socket,
            peer: Some(peer),
            connected: true,
        })
    }

    /// Send `msg` as a single UDP datagram.
    ///
    /// The wire bytes are `msg.header()` followed by `msg.body()`, matching
    /// the convention used by [`WsTransport`](crate::transport::ws::WsTransport).
    ///
    /// Returns an error if this is a listener that has not yet received any
    /// datagram (peer address unknown).
    pub async fn send(&mut self, msg: &Message) -> io::Result<()> {
        let header = msg.header();
        let body = msg.body();
        let mut payload = Vec::with_capacity(header.len() + body.len());
        payload.extend_from_slice(header);
        payload.extend_from_slice(body);

        if self.connected {
            self.socket.send(&payload).await?;
        } else if let Some(peer) = self.peer {
            self.socket.send_to(&payload, peer).await?;
        } else {
            return Err(io::Error::other(
                "UDP send: peer address unknown — call recv() first on a listener",
            ));
        }
        Ok(())
    }

    /// Receive the next UDP datagram as an SP message.
    ///
    /// All datagram bytes land in `msg.body()`.  The sender's address is
    /// recorded in `self.peer` so that a subsequent [`send`](Self::send) can
    /// reply to the correct address.
    ///
    /// # Cancellation safety
    ///
    /// This future is cancellation-safe: `UdpSocket::recv_from` is atomic at
    /// the datagram level — the entire datagram is either read or not.
    pub async fn recv(&mut self) -> io::Result<Message> {
        let mut buf = vec![0u8; MAX_UDP_PAYLOAD];
        let (len, from) = self.socket.recv_from(&mut buf).await?;
        self.peer = Some(from);
        let mut msg = Message::new();
        msg.push_back(&buf[..len]);
        Ok(msg)
    }

    /// The peer address currently known to this transport.
    ///
    /// Returns `None` on a fresh listener that has not yet received a datagram.
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer
    }

    /// The local address this socket is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}
