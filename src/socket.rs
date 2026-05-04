//! High-level socket API for nng-pure (requires `std` / tokio).
//!
//! Each socket type is generic over a protocol marker and wraps a
//! `FramedTransport`.  Listeners can accept multiple connections; for
//! simplicity, `Socket::listen` accepts exactly **one** connection before
//! returning (sufficient for examples and most tests).
//!
//! For multi-client servers, call `Socket::accept_one` in a loop and spawn
//! a task for each connection.

use std::io;

use tokio::net::{TcpListener, TcpStream};

use crate::{
    Message,
    codec::ProtocolId,
    transport::{FramedTransport, TransportError, tcp::TokioTcpStream},
};

fn transport_error_to_io(e: TransportError) -> io::Error {
    io::Error::other(e.to_string())
}

/// A connected socket wrapping a single `FramedTransport`.
pub struct Socket<P> {
    transport: FramedTransport<TokioTcpStream>,
    _protocol: core::marker::PhantomData<P>,
}

impl<P> Socket<P> {
    fn new(transport: FramedTransport<TokioTcpStream>) -> Self {
        Self { transport, _protocol: core::marker::PhantomData }
    }

    /// Bind a TCP listener and wait for the first incoming connection,
    /// perform the SP handshake, then return the connected `Socket`.
    ///
    /// The listener is dropped after accepting one connection.
    pub async fn listen(addr: &str, proto: ProtocolId) -> io::Result<Self> {
        let addr = addr.trim_start_matches("tcp://");
        let listener = TcpListener::bind(addr).await?;
        let (stream, _peer) = listener.accept().await?;
        let transport = FramedTransport::connect(TokioTcpStream(stream), proto)
            .await
            .map_err(transport_error_to_io)?;
        Ok(Self::new(transport))
    }

    /// Dial a TCP address and perform the SP handshake.
    pub async fn dial(addr: &str, proto: ProtocolId) -> io::Result<Self> {
        let addr = addr.trim_start_matches("tcp://");
        let stream = TcpStream::connect(addr).await?;
        let transport = FramedTransport::connect(TokioTcpStream(stream), proto)
            .await
            .map_err(transport_error_to_io)?;
        Ok(Self::new(transport))
    }

    pub async fn send_raw(&mut self, msg: &Message) -> io::Result<()> {
        self.transport.send(msg).await.map_err(transport_error_to_io)
    }

    pub async fn recv_raw(&mut self) -> io::Result<Message> {
        self.transport.recv().await.map_err(transport_error_to_io)
    }
}

// ── Protocol marker types ──

pub mod pubsub0 {
    //! PUB0 / SUB0 socket API.
    //!
    //! `Pub0` listens for subscriber connections and fans out each published
    //! message to all currently connected subscribers.  `Sub0` dials a
    //! publisher, registers topic-prefix subscriptions, and reads messages
    //! that match.

    use std::io;

    use tokio::net::{TcpListener, TcpStream};

    use crate::{
        Message,
        codec::ProtocolId,
        protocols::pubsub::Sub0State,
        transport::{FramedTransport, TransportError, tcp::TokioTcpStream},
    };

    fn te(e: TransportError) -> io::Error {
        io::Error::other(e.to_string())
    }

    /// Publish socket: listens for subscriber connections, fans out messages.
    pub struct Pub0 {
        listener: TcpListener,
        subscribers: Vec<FramedTransport<TokioTcpStream>>,
    }

    impl Pub0 {
        /// Bind to `addr` and start accepting subscriber connections.
        pub async fn listen(addr: &str) -> io::Result<Self> {
            let addr = addr.trim_start_matches("tcp://");
            let listener = TcpListener::bind(addr).await?;
            Ok(Self { listener, subscribers: Vec::new() })
        }

        /// Block until at least `n` subscribers have completed the SP handshake.
        pub async fn wait_for_subscribers(&mut self, n: usize) -> io::Result<()> {
            while self.subscribers.len() < n {
                let (stream, _) = self.listener.accept().await?;
                match FramedTransport::connect(TokioTcpStream(stream), ProtocolId::PUB0).await {
                    Ok(t) => self.subscribers.push(t),
                    Err(_) => {}
                }
            }
            Ok(())
        }

        /// Accept any connections that are already waiting without blocking.
        async fn drain_incoming(&mut self) {
            loop {
                tokio::select! {
                    biased;
                    conn = self.listener.accept() => {
                        if let Ok((stream, _)) = conn {
                            if let Ok(t) = FramedTransport::connect(
                                TokioTcpStream(stream), ProtocolId::PUB0,
                            ).await {
                                self.subscribers.push(t);
                            }
                        }
                    }
                    _ = std::future::ready(()) => break,
                }
            }
        }

        /// Publish `msg` to all connected subscribers.
        ///
        /// New connections that arrived since the last publish are accepted
        /// first.  Subscribers whose connections have broken are silently
        /// removed.
        pub async fn publish(&mut self, msg: Message) -> io::Result<()> {
            self.drain_incoming().await;
            let mut i = 0;
            while i < self.subscribers.len() {
                if self.subscribers[i].send(&msg).await.is_err() {
                    self.subscribers.swap_remove(i);
                } else {
                    i += 1;
                }
            }
            Ok(())
        }

        /// Number of currently connected subscribers.
        pub fn subscriber_count(&self) -> usize {
            self.subscribers.len()
        }
    }

    /// Subscribe socket: connects to a publisher, filters by topic prefix.
    pub struct Sub0 {
        transport: FramedTransport<TokioTcpStream>,
        state: Sub0State,
    }

    impl Sub0 {
        /// Connect to the publisher at `addr`.
        pub async fn dial(addr: &str) -> io::Result<Self> {
            let addr = addr.trim_start_matches("tcp://");
            let stream = TcpStream::connect(addr).await?;
            let transport = FramedTransport::connect(TokioTcpStream(stream), ProtocolId::SUB0)
                .await
                .map_err(te)?;
            Ok(Self { transport, state: Sub0State::new() })
        }

        /// Subscribe to messages whose body starts with `prefix`.
        ///
        /// An empty prefix (`b""`) matches every message.  Until at least one
        /// subscription is added, `next()` will never return.
        pub fn subscribe_to(&mut self, prefix: &[u8]) {
            self.state.subscribe(prefix);
        }

        /// Remove a previously added subscription.
        pub fn unsubscribe_from(&mut self, prefix: &[u8]) {
            self.state.unsubscribe(prefix);
        }

        /// Return the next message that matches any active subscription.
        ///
        /// Non-matching messages are silently discarded.  Blocks until a
        /// matching message arrives or the connection is closed.
        pub async fn next(&mut self) -> io::Result<Message> {
            loop {
                let msg = self.transport.recv().await.map_err(te)?;
                if self.state.matches(&msg) {
                    return Ok(msg);
                }
            }
        }
    }
}

pub mod survey0 {
    //! SURVEYOR0 / RESPONDENT0 socket API.
    //!
    //! The surveyor broadcasts a question to all connected respondents and
    //! collects answers within a deadline.  The respondent receives surveys
    //! and may reply via the returned `SurveyHandle`.

    use std::{io, time::Duration};

    use tokio::net::{TcpListener, TcpStream};

    use crate::{
        Message,
        codec::ProtocolId,
        protocols::survey::{Respondent0State, Surveyor0State, SurveyRoutingInfo},
        transport::{FramedTransport, TransportError, tcp::TokioTcpStream},
    };

    fn te(e: TransportError) -> io::Error {
        io::Error::other(e.to_string())
    }

    /// Surveyor socket: broadcasts surveys to multiple respondents.
    pub struct Surveyor0 {
        listener: TcpListener,
        respondents: Vec<FramedTransport<TokioTcpStream>>,
        state: Surveyor0State,
    }

    impl Surveyor0 {
        /// Bind and start accepting respondent connections.
        pub async fn listen(addr: &str) -> io::Result<Self> {
            let addr = addr.trim_start_matches("tcp://");
            let listener = TcpListener::bind(addr).await?;
            Ok(Self { listener, respondents: Vec::new(), state: Surveyor0State::new() })
        }

        /// Block until at least `n` respondents have connected.
        pub async fn wait_for_respondents(&mut self, n: usize) -> io::Result<()> {
            while self.respondents.len() < n {
                let (stream, _) = self.listener.accept().await?;
                match FramedTransport::connect(TokioTcpStream(stream), ProtocolId::SURVEYOR0).await {
                    Ok(t) => self.respondents.push(t),
                    Err(_) => {}
                }
            }
            Ok(())
        }

        /// Broadcast `msg` as a survey; collect all responses arriving within
        /// `timeout`.  Returns the set of application-level response bodies.
        ///
        /// Responses are collected from each respondent sequentially; the
        /// remaining timeout budget is shared across all of them.
        pub async fn survey(&mut self, msg: Message, timeout: Duration) -> io::Result<Vec<Message>> {
            let mut outgoing = msg;
            self.state.prepare_survey(&mut outgoing);

            let deadline = tokio::time::Instant::now() + timeout;

            // Fan out to all respondents.
            let mut active: Vec<usize> = Vec::new();
            for (i, resp) in self.respondents.iter_mut().enumerate() {
                if resp.send(&outgoing).await.is_ok() {
                    active.push(i);
                }
            }

            // Collect one response per active respondent within the deadline.
            let mut responses = Vec::new();
            for i in active {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match tokio::time::timeout(remaining, self.respondents[i].recv()).await {
                    Ok(Ok(mut raw)) => {
                        if self.state.process_response(&mut raw).is_ok() {
                            responses.push(raw);
                        }
                    }
                    _ => {}
                }
            }

            Ok(responses)
        }
    }

    /// One-shot handle that allows sending a single response to the active survey.
    pub struct SurveyHandle<'a> {
        transport: &'a mut FramedTransport<TokioTcpStream>,
        routing: SurveyRoutingInfo,
    }

    impl<'a> SurveyHandle<'a> {
        /// Send a response to the surveyor.
        pub async fn respond(self, msg: Message) -> io::Result<()> {
            let state = Respondent0State::new();
            let mut outgoing = msg;
            state.prepare_response(&mut outgoing, &self.routing);
            self.transport.send(&outgoing).await.map_err(te)
        }
    }

    /// Respondent socket: dials a surveyor, receives surveys, sends responses.
    pub struct Respondent0 {
        transport: FramedTransport<TokioTcpStream>,
        state: Respondent0State,
    }

    impl Respondent0 {
        /// Connect to a surveyor at `addr`.
        pub async fn dial(addr: &str) -> io::Result<Self> {
            let addr = addr.trim_start_matches("tcp://");
            let stream = TcpStream::connect(addr).await?;
            let transport =
                FramedTransport::connect(TokioTcpStream(stream), ProtocolId::RESPONDENT0)
                    .await
                    .map_err(te)?;
            Ok(Self { transport, state: Respondent0State::new() })
        }

        /// Receive the next survey.  Returns the application message and a
        /// `SurveyHandle` that must be used to respond (or dropped to skip).
        pub async fn receive(&mut self) -> io::Result<(Message, SurveyHandle<'_>)> {
            let mut msg = self.transport.recv().await.map_err(te)?;
            let routing = self
                .state
                .process_incoming(&mut msg)
                .map_err(|e| io::Error::other(e.to_string()))?;
            Ok((msg, SurveyHandle { transport: &mut self.transport, routing }))
        }
    }
}

pub mod bus0 {
    //! BUS0 socket API — many-to-many broadcast.
    //!
    //! Every `Bus0` node can broadcast messages to all connected peers and
    //! receive from any of them.  BUS is stateless at the protocol level.
    //!
    //! `recv_any` polls peers in round-robin with cooperative yielding; it is
    //! suitable for demonstrations but should not be used when messages may
    //! arrive concurrently from multiple peers at high rate.

    use std::io;

    use tokio::net::{TcpListener, TcpStream};

    use crate::{
        Message,
        codec::ProtocolId,
        transport::{FramedTransport, TransportError, tcp::TokioTcpStream},
    };

    fn te(e: TransportError) -> io::Error {
        io::Error::other(e.to_string())
    }

    /// A BUS0 node that can be connected to any number of peers.
    pub struct Bus0 {
        peers: Vec<FramedTransport<TokioTcpStream>>,
    }

    impl Bus0 {
        /// Bind and accept exactly one peer connection.
        pub async fn listen(addr: &str) -> io::Result<Self> {
            Self::listen_and_accept(addr, 1).await
        }

        /// Bind and accept `n` peer connections before returning.
        pub async fn listen_and_accept(addr: &str, n: usize) -> io::Result<Self> {
            let addr = addr.trim_start_matches("tcp://");
            let listener = TcpListener::bind(addr).await?;
            let mut peers = Vec::with_capacity(n);
            while peers.len() < n {
                let (stream, _) = listener.accept().await?;
                match FramedTransport::connect(TokioTcpStream(stream), ProtocolId::BUS0).await {
                    Ok(t) => peers.push(t),
                    Err(_) => {}
                }
            }
            Ok(Self { peers })
        }

        /// Dial one peer and return a `Bus0` with that single connection.
        pub async fn dial(addr: &str) -> io::Result<Self> {
            let addr = addr.trim_start_matches("tcp://");
            let stream = TcpStream::connect(addr).await?;
            let transport =
                FramedTransport::connect(TokioTcpStream(stream), ProtocolId::BUS0).await.map_err(te)?;
            Ok(Self { peers: vec![transport] })
        }

        /// Broadcast `msg` to all connected peers.  Best-effort: broken
        /// connections are silently removed.
        pub async fn broadcast(&mut self, msg: Message) -> io::Result<()> {
            let mut i = 0;
            while i < self.peers.len() {
                if self.peers[i].send(&msg).await.is_err() {
                    self.peers.swap_remove(i);
                } else {
                    i += 1;
                }
            }
            Ok(())
        }

        /// Receive the next message from any connected peer.
        ///
        /// Polls peers in round-robin order with cooperative yielding between
        /// passes.  Each iteration polls each peer non-blockingly using a
        /// `biased` select; if no peer has data immediately available the task
        /// yields before trying again.
        pub async fn recv_any(&mut self) -> io::Result<Message> {
            loop {
                if self.peers.is_empty() {
                    return Err(io::Error::other("all peers disconnected"));
                }
                let mut i = 0;
                while i < self.peers.len() {
                    let poll_result = tokio::select! {
                        biased;
                        result = self.peers[i].recv() => Some(result),
                        _ = std::future::ready(()) => None,
                    };
                    match poll_result {
                        Some(Ok(msg)) => return Ok(msg),
                        Some(Err(_)) => {
                            // Dead connection — remove and try remaining peers.
                            self.peers.swap_remove(i);
                        }
                        None => {
                            i += 1;
                        }
                    }
                }
                tokio::task::yield_now().await;
            }
        }

        /// Receive from a specific peer by index.
        pub async fn recv_from(&mut self, peer_idx: usize) -> io::Result<Message> {
            self.peers
                .get_mut(peer_idx)
                .ok_or_else(|| io::Error::other("peer index out of range"))?
                .recv()
                .await
                .map_err(te)
        }

        /// Number of currently connected peers.
        pub fn peer_count(&self) -> usize {
            self.peers.len()
        }
    }
}

pub mod pipeline0 {
    //! PUSH0 / PULL0 socket API (pipeline pattern).
    //!
    //! `Push0` sends work items; `Pull0` receives them.  No headers are added;
    //! framing is handled by `FramedTransport`.

    use std::io;

    use crate::{
        Message,
        codec::ProtocolId,
        protocols::pipeline::{Pull0State, Push0State},
        socket::Socket,
    };

    /// Push socket: sends messages to a connected pull endpoint.
    pub struct Push0(Socket<Push0State>);

    impl Push0 {
        pub async fn listen(addr: &str) -> io::Result<Self> {
            Socket::listen(addr, ProtocolId::PUSH0).await.map(Self)
        }

        pub async fn dial(addr: &str) -> io::Result<Self> {
            Socket::dial(addr, ProtocolId::PUSH0).await.map(Self)
        }

        pub async fn push(&mut self, msg: Message) -> io::Result<()> {
            self.0.send_raw(&msg).await
        }
    }

    /// Pull socket: receives messages from a connected push endpoint.
    pub struct Pull0(Socket<Pull0State>);

    impl Pull0 {
        pub async fn listen(addr: &str) -> io::Result<Self> {
            Socket::listen(addr, ProtocolId::PULL0).await.map(Self)
        }

        pub async fn dial(addr: &str) -> io::Result<Self> {
            Socket::dial(addr, ProtocolId::PULL0).await.map(Self)
        }

        pub async fn pull(&mut self) -> io::Result<Message> {
            self.0.recv_raw().await
        }
    }
}

pub mod pair0 {
    //! PAIR0 socket API (bidirectional point-to-point).
    //!
    //! Both ends can send and receive freely with no protocol headers.

    use std::io;

    use crate::{
        Message,
        codec::ProtocolId,
        protocols::pair::Pair0State,
        socket::Socket,
    };

    /// Pair socket: bidirectional point-to-point messaging.
    pub struct Pair0(Socket<Pair0State>);

    impl Pair0 {
        pub async fn listen(addr: &str) -> io::Result<Self> {
            Socket::listen(addr, ProtocolId::PAIR0).await.map(Self)
        }

        pub async fn dial(addr: &str) -> io::Result<Self> {
            Socket::dial(addr, ProtocolId::PAIR0).await.map(Self)
        }

        pub async fn send(&mut self, msg: Message) -> io::Result<()> {
            self.0.send_raw(&msg).await
        }

        pub async fn recv(&mut self) -> io::Result<Message> {
            self.0.recv_raw().await
        }
    }
}

pub mod reqrep0 {
    //! REQ0 / REP0 socket API.
    //!
    //! Mirrors the `anng::protocols::reqrep0` interface closely.

    use std::io;

    use crate::{
        Message,
        codec::ProtocolId,
        protocols::reqrep::{Rep0State, Req0State, RoutingInfo},
        socket::Socket,
    };

    /// Request socket: connects to a reply server, sends requests, awaits replies.
    pub struct Req0(Socket<Req0State>);

    impl Req0 {
        pub async fn dial(addr: &str) -> io::Result<Self> {
            Socket::dial(addr, ProtocolId::REQ0).await.map(Self)
        }

        /// Send `msg` and receive the reply.  The request ID is managed
        /// transparently.
        pub async fn request(&mut self, msg: Message) -> io::Result<Message> {
            let mut state = Req0State::new();

            let mut outgoing = msg;
            let sent_id = state.prepare_outgoing(&mut outgoing);
            self.0.send_raw(&outgoing).await?;

            let mut reply = self.0.recv_raw().await?;
            state
                .process_incoming(&mut reply, sent_id)
                .map_err(|e| io::Error::other(e.to_string()))?;
            Ok(reply)
        }
    }

    /// Reply socket: listens for connections from requesters, handles one
    /// connection at a time.
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
        pub async fn reply(self, msg: Message) -> io::Result<()> {
            let state = Rep0State::new();
            let mut outgoing = msg;
            state.prepare_reply(&mut outgoing, &self.routing);
            self.socket.send_raw(&outgoing).await
        }
    }

    impl Rep0 {
        pub async fn listen(addr: &str) -> io::Result<Self> {
            Socket::listen(addr, ProtocolId::REP0)
                .await
                .map(|s| Self { inner: s, state: Rep0State::new() })
        }

        /// Receive the next request.  Returns the application message plus a
        /// `Responder` that must be used to send the reply.
        pub async fn receive(&mut self) -> io::Result<(Message, Responder<'_>)> {
            let mut msg = self.inner.recv_raw().await?;
            let routing = self.state
                .process_incoming(&mut msg)
                .map_err(|e| io::Error::other(e.to_string()))?;
            Ok((msg, Responder { socket: &mut self.inner, routing }))
        }
    }
}
