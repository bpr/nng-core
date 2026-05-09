//! High-level socket API for nng-core (requires `std` / tokio).
//!
//! Supported URL schemes: `tcp://host:port` and (Unix only) `ipc:///path`.
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

use embedded_io_async::{ErrorType, Read as EioRead, Write as EioWrite};

use crate::{
    Message,
    codec::ProtocolId,
    transport::{FrameFormat, FramedTransport, TransportError, tcp::TokioTcpStream},
};

#[cfg(unix)]
use crate::transport::ipc::TokioUnixStream;
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};

fn transport_error_to_io(e: TransportError) -> io::Error {
    io::Error::other(e.to_string())
}

/// Perform the SP handshake on `stream`, selecting the frame format from the
/// stream's transport type (TCP → 8-byte header, IPC → 9-byte NNG 1.5.x header).
pub(crate) async fn connect_framed(
    stream: AnyStream,
    proto: ProtocolId,
) -> Result<FramedTransport<AnyStream>, TransportError> {
    let format = stream.frame_format();
    FramedTransport::connect(stream, proto, format).await
}

// ── AnyStream: TCP or IPC behind a single trait impl ──

pub(crate) enum AnyStream {
    Tcp(TokioTcpStream),
    #[cfg(unix)]
    Ipc(TokioUnixStream),
}

impl ErrorType for AnyStream {
    type Error = std::io::Error;
}

impl EioRead for AnyStream {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        match self {
            Self::Tcp(s) => EioRead::read(s, buf).await,
            #[cfg(unix)]
            Self::Ipc(s) => EioRead::read(s, buf).await,
        }
    }
}

impl EioWrite for AnyStream {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        match self {
            Self::Tcp(s) => EioWrite::write(s, buf).await,
            #[cfg(unix)]
            Self::Ipc(s) => EioWrite::write(s, buf).await,
        }
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        match self {
            Self::Tcp(s) => EioWrite::flush(s).await,
            #[cfg(unix)]
            Self::Ipc(s) => EioWrite::flush(s).await,
        }
    }
}

impl AnyStream {
    fn frame_format(&self) -> FrameFormat {
        match self {
            Self::Tcp(_) => FrameFormat::Tcp,
            #[cfg(unix)]
            Self::Ipc(_) => FrameFormat::Ipc,
        }
    }
}

// ── AnyListener: TCP or IPC, with a unified accept() ──

pub(crate) enum AnyListener {
    Tcp(TcpListener),
    #[cfg(unix)]
    Ipc(UnixListener),
}

impl AnyListener {
    pub(crate) async fn accept(&self) -> io::Result<AnyStream> {
        match self {
            Self::Tcp(l) => {
                let (stream, _) = l.accept().await?;
                Ok(AnyStream::Tcp(TokioTcpStream(stream)))
            }
            #[cfg(unix)]
            Self::Ipc(l) => {
                let (stream, _) = l.accept().await?;
                Ok(AnyStream::Ipc(TokioUnixStream(stream)))
            }
        }
    }
}

// ── URL dispatch helpers ──

pub(crate) async fn bind_listener(addr: &str) -> io::Result<AnyListener> {
    if let Some(tcp_addr) = addr.strip_prefix("tcp://") {
        TcpListener::bind(tcp_addr).await.map(AnyListener::Tcp)
    } else if let Some(ipc_path) = addr.strip_prefix("ipc://") {
        bind_ipc_listener(ipc_path)
    } else {
        Err(io::Error::other(format!("unsupported URL scheme: {addr}")))
    }
}

pub(crate) async fn connect_stream(addr: &str) -> io::Result<AnyStream> {
    if let Some(tcp_addr) = addr.strip_prefix("tcp://") {
        TcpStream::connect(tcp_addr)
            .await
            .map(|s| AnyStream::Tcp(TokioTcpStream(s)))
    } else if let Some(ipc_path) = addr.strip_prefix("ipc://") {
        connect_ipc_stream(ipc_path).await
    } else {
        Err(io::Error::other(format!("unsupported URL scheme: {addr}")))
    }
}

#[cfg(unix)]
fn bind_ipc_listener(path: &str) -> io::Result<AnyListener> {
    // Remove a stale socket file left by a prior crash, matching libnng behaviour.
    let _ = std::fs::remove_file(path);
    UnixListener::bind(path).map(AnyListener::Ipc)
}

#[cfg(not(unix))]
fn bind_ipc_listener(_path: &str) -> io::Result<AnyListener> {
    Err(io::Error::other(
        "IPC (Unix domain sockets) is not supported on this platform",
    ))
}

#[cfg(unix)]
async fn connect_ipc_stream(path: &str) -> io::Result<AnyStream> {
    UnixStream::connect(path)
        .await
        .map(|s| AnyStream::Ipc(TokioUnixStream(s)))
}

#[cfg(not(unix))]
async fn connect_ipc_stream(_path: &str) -> io::Result<AnyStream> {
    Err(io::Error::other(
        "IPC (Unix domain sockets) is not supported on this platform",
    ))
}

// ── Socket<P> ──

/// A connected socket wrapping a single `FramedTransport`.
pub struct Socket<P> {
    transport: FramedTransport<AnyStream>,
    _protocol: core::marker::PhantomData<P>,
}

impl<P> Socket<P> {
    fn new(transport: FramedTransport<AnyStream>) -> Self {
        Self {
            transport,
            _protocol: core::marker::PhantomData,
        }
    }

    /// Bind and wait for the first incoming connection, then perform the SP
    /// handshake.  The listener is dropped after accepting one connection.
    pub async fn listen(addr: &str, proto: ProtocolId) -> io::Result<Self> {
        let listener = bind_listener(addr).await?;
        let stream = listener.accept().await?;
        let transport = connect_framed(stream, proto)
            .await
            .map_err(transport_error_to_io)?;
        Ok(Self::new(transport))
    }

    /// Connect to `addr` and perform the SP handshake.
    pub async fn dial(addr: &str, proto: ProtocolId) -> io::Result<Self> {
        let stream = connect_stream(addr).await?;
        let transport = connect_framed(stream, proto)
            .await
            .map_err(transport_error_to_io)?;
        Ok(Self::new(transport))
    }

    pub async fn send_raw(&mut self, msg: &Message) -> io::Result<()> {
        self.transport
            .send(msg)
            .await
            .map_err(transport_error_to_io)
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

    use crate::{
        Message,
        codec::ProtocolId,
        protocols::pubsub::Sub0State,
        transport::{FramedTransport, TransportError},
    };

    use super::{AnyListener, AnyStream, bind_listener, connect_framed, connect_stream};

    fn te(e: TransportError) -> io::Error {
        io::Error::other(e.to_string())
    }

    /// Publish socket: listens for subscriber connections, fans out messages.
    pub struct Pub0 {
        listener: AnyListener,
        subscribers: Vec<FramedTransport<AnyStream>>,
    }

    impl Pub0 {
        /// Bind to `addr` and start accepting subscriber connections.
        pub async fn listen(addr: &str) -> io::Result<Self> {
            let listener = bind_listener(addr).await?;
            Ok(Self {
                listener,
                subscribers: Vec::new(),
            })
        }

        /// Block until at least `n` subscribers have completed the SP handshake.
        pub async fn wait_for_subscribers(&mut self, n: usize) -> io::Result<()> {
            while self.subscribers.len() < n {
                let stream = self.listener.accept().await?;
                match connect_framed(stream, ProtocolId::PUB0).await {
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
                    stream = self.listener.accept() => {
                        if let Ok(stream) = stream {
                            if let Ok(t) = connect_framed(stream, ProtocolId::PUB0).await {
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
        transport: FramedTransport<AnyStream>,
        state: Sub0State,
    }

    impl Sub0 {
        /// Connect to the publisher at `addr`.
        pub async fn dial(addr: &str) -> io::Result<Self> {
            let stream = connect_stream(addr).await?;
            let transport = connect_framed(stream, ProtocolId::SUB0).await.map_err(te)?;
            Ok(Self {
                transport,
                state: Sub0State::new(),
            })
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

    use crate::{
        Message,
        codec::ProtocolId,
        protocols::survey::{Respondent0State, SurveyRoutingInfo, Surveyor0State},
        transport::{FramedTransport, TransportError},
    };

    use super::{AnyListener, AnyStream, bind_listener, connect_framed, connect_stream};

    fn te(e: TransportError) -> io::Error {
        io::Error::other(e.to_string())
    }

    /// Surveyor socket: broadcasts surveys to multiple respondents.
    pub struct Surveyor0 {
        listener: AnyListener,
        respondents: Vec<FramedTransport<AnyStream>>,
        state: Surveyor0State,
    }

    impl Surveyor0 {
        /// Bind and start accepting respondent connections.
        pub async fn listen(addr: &str) -> io::Result<Self> {
            let listener = bind_listener(addr).await?;
            Ok(Self {
                listener,
                respondents: Vec::new(),
                state: Surveyor0State::new(),
            })
        }

        /// Block until at least `n` respondents have connected.
        pub async fn wait_for_respondents(&mut self, n: usize) -> io::Result<()> {
            while self.respondents.len() < n {
                let stream = self.listener.accept().await?;
                match connect_framed(stream, ProtocolId::SURVEYOR0).await {
                    Ok(t) => self.respondents.push(t),
                    Err(_) => {}
                }
            }
            Ok(())
        }

        /// Accept any respondents that connected since the last call.
        ///
        /// Returns immediately when the kernel's accept queue is empty.
        /// Safe to cancel: `TcpListener::accept` does not consume partial data.
        pub async fn accept_pending(&mut self) {
            loop {
                tokio::select! {
                    biased;
                    result = self.listener.accept() => {
                        if let Ok(stream) = result {
                            if let Ok(t) = connect_framed(stream, ProtocolId::SURVEYOR0).await {
                                self.respondents.push(t);
                            }
                        }
                    }
                    _ = std::future::ready(()) => break,
                }
            }
        }

        /// Broadcast `msg` as a survey; collect all responses arriving within
        /// `timeout`.  Returns the set of application-level response bodies.
        ///
        /// Responses are collected from each respondent sequentially; the
        /// remaining timeout budget is shared across all of them.
        pub async fn survey(
            &mut self,
            msg: Message,
            timeout: Duration,
        ) -> io::Result<Vec<Message>> {
            let mut outgoing = msg;
            self.state.prepare_survey(&mut outgoing);

            let deadline = tokio::time::Instant::now() + timeout;

            let mut active: Vec<usize> = Vec::new();
            for (i, resp) in self.respondents.iter_mut().enumerate() {
                if resp.send(&outgoing).await.is_ok() {
                    active.push(i);
                }
            }

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
        transport: &'a mut FramedTransport<AnyStream>,
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
        transport: FramedTransport<AnyStream>,
        state: Respondent0State,
    }

    impl Respondent0 {
        /// Connect to a surveyor at `addr`.
        pub async fn dial(addr: &str) -> io::Result<Self> {
            let stream = connect_stream(addr).await?;
            let transport = connect_framed(stream, ProtocolId::RESPONDENT0)
                .await
                .map_err(te)?;
            Ok(Self {
                transport,
                state: Respondent0State::new(),
            })
        }

        /// Receive the next survey.  Returns the application message and a
        /// `SurveyHandle` that must be used to respond (or dropped to skip).
        pub async fn receive(&mut self) -> io::Result<(Message, SurveyHandle<'_>)> {
            let mut msg = self.transport.recv().await.map_err(te)?;
            let routing = self
                .state
                .process_incoming(&mut msg)
                .map_err(|e| io::Error::other(e.to_string()))?;
            Ok((
                msg,
                SurveyHandle {
                    transport: &mut self.transport,
                    routing,
                },
            ))
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

    use crate::{
        Message,
        codec::ProtocolId,
        transport::{FramedTransport, TransportError},
    };

    use super::{AnyListener, AnyStream, bind_listener, connect_framed, connect_stream};

    fn te(e: TransportError) -> io::Error {
        io::Error::other(e.to_string())
    }

    /// A BUS0 node that can be connected to any number of peers.
    pub struct Bus0 {
        peers: Vec<FramedTransport<AnyStream>>,
    }

    impl Bus0 {
        /// Bind and accept exactly one peer connection.
        pub async fn listen(addr: &str) -> io::Result<Self> {
            Self::listen_and_accept(addr, 1).await
        }

        /// Bind and accept `n` peer connections before returning.
        pub async fn listen_and_accept(addr: &str, n: usize) -> io::Result<Self> {
            let listener: AnyListener = bind_listener(addr).await?;
            let mut peers = Vec::with_capacity(n);
            while peers.len() < n {
                let stream = listener.accept().await?;
                match connect_framed(stream, ProtocolId::BUS0).await {
                    Ok(t) => peers.push(t),
                    Err(_) => {}
                }
            }
            Ok(Self { peers })
        }

        /// Dial one peer and return a `Bus0` with that single connection.
        pub async fn dial(addr: &str) -> io::Result<Self> {
            let stream = connect_stream(addr).await?;
            let transport = connect_framed(stream, ProtocolId::BUS0).await.map_err(te)?;
            Ok(Self {
                peers: vec![transport],
            })
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
    //! `Push0` / `Pull0` handle a single connection each.  `Push0Fan` and
    //! `Pull0Fan` handle N connections: `Push0Fan` round-robins outgoing
    //! messages across N pullers; `Pull0Fan` receives from whichever of N
    //! pushers has data available first.

    use std::io;

    use crate::{
        Message,
        codec::ProtocolId,
        protocols::pipeline::{Pull0State, Push0State},
        socket::Socket,
        transport::{FramedTransport, TransportError},
    };

    use super::{AnyStream, bind_listener, connect_framed};

    fn te(e: TransportError) -> io::Error {
        io::Error::other(e.to_string())
    }

    /// Push socket: sends messages to a single connected pull endpoint.
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

    /// Pull socket: receives messages from a single connected push endpoint.
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

    /// Multi-worker push socket: distributes messages across N connected
    /// pullers in round-robin order.
    pub struct Push0Fan {
        workers: Vec<FramedTransport<AnyStream>>,
        next: usize,
    }

    impl Push0Fan {
        /// Bind to `addr` and block until exactly `n` PULL connections have
        /// completed the SP handshake.
        pub async fn listen_and_accept(addr: &str, n: usize) -> io::Result<Self> {
            let listener = bind_listener(addr).await?;
            let mut workers = Vec::with_capacity(n);
            while workers.len() < n {
                let stream = listener.accept().await?;
                if let Ok(t) = connect_framed(stream, ProtocolId::PUSH0).await {
                    workers.push(t);
                }
            }
            Ok(Self { workers, next: 0 })
        }

        /// Send `msg` to the next worker in round-robin order.
        pub async fn push(&mut self, msg: Message) -> io::Result<()> {
            if self.workers.is_empty() {
                return Err(io::Error::other("no workers connected"));
            }
            let i = self.next;
            self.next = (self.next + 1) % self.workers.len();
            self.workers[i].send(&msg).await.map_err(te)
        }

        /// Number of connected workers.
        pub fn worker_count(&self) -> usize {
            self.workers.len()
        }
    }

    /// Multi-sender pull socket: accepts N pushers and receives from whichever
    /// has data available first.
    ///
    /// Each connected sender runs in its own tokio task so `recv()` is never
    /// cancelled mid-read (which would corrupt TCP frame boundaries).
    pub struct Pull0Fan {
        rx: tokio::sync::mpsc::Receiver<io::Result<Message>>,
        n: usize,
    }

    impl Pull0Fan {
        /// Bind to `addr` and block until exactly `n` PUSH connections have
        /// completed the SP handshake.
        pub async fn listen_and_accept(addr: &str, n: usize) -> io::Result<Self> {
            let listener = bind_listener(addr).await?;
            let (tx, rx) = tokio::sync::mpsc::channel(n * 4);
            let mut count = 0;
            while count < n {
                let stream = listener.accept().await?;
                if let Ok(mut transport) = connect_framed(stream, ProtocolId::PULL0).await {
                    let tx2 = tx.clone();
                    tokio::spawn(async move {
                        loop {
                            match transport.recv().await {
                                Ok(msg) => {
                                    if tx2.send(Ok(msg)).await.is_err() {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    let _ = tx2.send(Err(te(e))).await;
                                    break;
                                }
                            }
                        }
                    });
                    count += 1;
                }
            }
            Ok(Self { rx, n })
        }

        /// Receive the next message from any connected sender.
        ///
        /// Skips sender-disconnect errors so callers keep receiving from the
        /// remaining live senders.  Returns `Err` only when all senders are gone.
        pub async fn pull_any(&mut self) -> io::Result<Message> {
            loop {
                match self.rx.recv().await {
                    None => return Err(io::Error::other("all senders disconnected")),
                    Some(Ok(msg)) => return Ok(msg),
                    Some(Err(_)) => {} // one sender disconnected, drain remaining
                }
            }
        }

        /// Number of senders accepted at construction time.
        pub fn sender_count(&self) -> usize {
            self.n
        }
    }
}

pub mod pair0 {
    //! PAIR0 socket API (bidirectional point-to-point).
    //!
    //! Both ends can send and receive freely with no protocol headers.

    use std::io;

    use crate::{Message, codec::ProtocolId, protocols::pair::Pair0State, socket::Socket};

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
            Socket::listen(addr, ProtocolId::REP0).await.map(|s| Self {
                inner: s,
                state: Rep0State::new(),
            })
        }

        /// Receive the next request.  Returns the application message plus a
        /// `Responder` that must be used to send the reply.
        pub async fn receive(&mut self) -> io::Result<(Message, Responder<'_>)> {
            let mut msg = self.inner.recv_raw().await?;
            let routing = self
                .state
                .process_incoming(&mut msg)
                .map_err(|e| io::Error::other(e.to_string()))?;
            Ok((
                msg,
                Responder {
                    socket: &mut self.inner,
                    routing,
                },
            ))
        }
    }
}
