//! DTLS transport — DTLS 1.2/1.3 over UDP via `dimpl`.
//!
//! [`DtlsTransport`] bridges the `dimpl` sans-IO state machine to Tokio's
//! async UDP I/O by running a background task that owns the socket and the
//! state machine.  The public `send` / `recv` surface communicates with that
//! task over bounded channels.
//!
//! # Wire model
//!
//! DTLS preserves datagram boundaries, so no length-prefix framing is needed.
//! The payload convention is the same as [`UdpTransport`] and [`WsTransport`]:
//! `msg.header()` bytes followed by `msg.body()` bytes in one datagram; the
//! receiver places all datagram bytes into the returned message's body.
//!
//! # Certificate model
//!
//! Both sides must supply a [`dimpl::DtlsCertificate`].  The `dimpl` engine
//! emits [`Output::PeerCert`] during the handshake but performs no PKI
//! validation — the initial implementation accepts any self-signed certificate.
//!
//! For tests, generate a certificate with `rcgen`:
//!
//! ```rust,ignore
//! let rcgen::CertifiedKey { cert, signing_key } =
//!     rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
//! let dtls_cert = dimpl::DtlsCertificate {
//!     certificate: cert.der().to_vec(),
//!     private_key: signing_key.serialize_der(),
//! };
//! ```
//!
//! # Peer discovery
//!
//! The server-side transport has no peer address until the first datagram
//! arrives.  The background task records the sender's address from the first
//! `recv_from` call and uses it for all subsequent `send_to` calls.
//!
//! [`UdpTransport`]: crate::transport::udp::UdpTransport
//! [`WsTransport`]: crate::transport::ws::WsTransport

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use dimpl::{Config, Dtls, DtlsCertificate, Output};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, watch};

use crate::Message;

/// Maximum UDP payload we allocate for `recv_from`.
const MAX_UDP: usize = 65_507;
/// Output buffer handed to `dimpl::Dtls::poll_output`.
const OUT_BUF: usize = 65_507;

// ── Background task ───────────────────────────────────────────────────────────

async fn run_dtls_task(
    socket: UdpSocket,
    mut dtls: Dtls,
    mut peer: Option<SocketAddr>,
    plaintext_tx: mpsc::Sender<io::Result<Vec<u8>>>,
    mut app_rx: mpsc::Receiver<Vec<u8>>,
    mut cancel: watch::Receiver<()>,
) {
    let mut recv_buf = vec![0u8; MAX_UDP];
    let mut out_buf = vec![0u8; OUT_BUF];
    // Buffered outgoing payloads queued before the handshake completed.
    let mut pending_sends: Vec<Vec<u8>> = Vec::new();
    let mut connected = false;

    // Kick the state machine so the client sends its initial ClientHello.
    dtls.handle_timeout(Instant::now()).ok();

    // Flat event loop: `poll_output` is called after every state change.
    // `Output::Timeout` is always the last variant in a poll cycle, so it
    // is the only branch that enters `select!`.
    //
    // Correctness:
    //
    // 1. dimpl transiently returns an already-expired `Timeout` during
    //    handshake flight transitions (Unarmed/Armed mixed state).  In that
    //    case `sleep_until(stale)` fires immediately and we call
    //    `handle_timeout` without ever returning Poll::Pending — starving
    //    other tasks on the current_thread executor.  After `handle_timeout`
    //    we therefore do a `yield_now` so that peer bg-tasks can run before
    //    we loop back for the next `poll_output`.
    //
    // 2. After all retransmit attempts are exhausted, `handle_timeout`
    //    returns `Err` and stops advancing the deadline.  We surface the
    //    first error and exit so the task does not spin indefinitely.
    loop {
        match dtls.poll_output(&mut out_buf) {
            Output::Packet(p) => {
                if let Some(addr) = peer {
                    socket.send_to(p, addr).await.ok();
                }
            }
            Output::Timeout(t) => {
                let wake = {
                    let dur = t.saturating_duration_since(Instant::now());
                    tokio::time::Instant::now() + dur
                };
                tokio::select! {
                    result = socket.recv_from(&mut recv_buf) => {
                        match result {
                            Ok((n, from)) => {
                                // Learn the peer from the first datagram.
                                if peer.is_none() {
                                    peer = Some(from);
                                }
                                dtls.handle_packet(&recv_buf[..n]).ok();
                            }
                            Err(e) => {
                                let _ = plaintext_tx.send(Err(e)).await;
                                return;
                            }
                        }
                    }
                    Some(data) = app_rx.recv() => {
                        if connected {
                            dtls.send_application_data(&data).ok();
                        } else {
                            pending_sends.push(data);
                        }
                    }
                    _ = tokio::time::sleep_until(wake) => {
                        if dtls.handle_timeout(Instant::now()).is_err() {
                            let _ = plaintext_tx
                                .send(Err(io::Error::other("dtls handshake timed out")))
                                .await;
                            return;
                        }
                        // Yield after handling a timeout so peer bg-tasks
                        // get CPU time before we loop back to poll_output.
                        // This is critical for the stale-timeout case where
                        // sleep_until fired immediately (dur == 0), because
                        // select! never returned Poll::Pending in that case.
                        tokio::task::yield_now().await;
                    }
                    // Fires (with Err) when the DtlsTransport owner is dropped.
                    _ = cancel.changed() => return,
                }
            }
            Output::Connected => {
                connected = true;
                for data in pending_sends.drain(..) {
                    dtls.send_application_data(&data).ok();
                }
            }
            Output::ApplicationData(d) => {
                if plaintext_tx.send(Ok(d.to_vec())).await.is_err() {
                    return;
                }
            }
            Output::CloseNotify => {
                let _ = plaintext_tx
                    .send(Err(io::Error::other("dtls close_notify")))
                    .await;
                return;
            }
            // PeerCert, KeyingMaterial — no validation in initial impl.
            _ => {}
        }
    }
}

fn spawn_task(
    socket: UdpSocket,
    cert: DtlsCertificate,
    peer: Option<SocketAddr>,
    active: bool,
) -> io::Result<DtlsTransport> {
    let cfg = Arc::new(Config::default());
    let mut dtls = Dtls::new_12(cfg, cert, Instant::now());
    if active {
        dtls.set_active(true);
    }
    let (send_tx, app_rx) = mpsc::channel::<Vec<u8>>(64);
    let (plaintext_tx, recv_rx) = mpsc::channel::<io::Result<Vec<u8>>>(64);
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    tokio::spawn(run_dtls_task(
        socket,
        dtls,
        peer,
        plaintext_tx,
        app_rx,
        shutdown_rx,
    ));
    Ok(DtlsTransport {
        send_tx,
        recv_rx,
        _shutdown: shutdown_tx,
    })
}

// ── Public transport type ─────────────────────────────────────────────────────

/// SP transport over DTLS (DTLS 1.2 via `dimpl`).
///
/// Constructed via [`bind`](Self::bind) (server) or
/// [`connect`](Self::connect) (client).
///
/// Both sides must supply a [`DtlsCertificate`].  Peer certificate validation
/// is skipped in the initial implementation — any self-signed certificate is
/// accepted.
///
/// Dropping `DtlsTransport` sends a cancellation signal to the background
/// task, which exits promptly even if the peer is idle.
pub struct DtlsTransport {
    send_tx: mpsc::Sender<Vec<u8>>,
    recv_rx: mpsc::Receiver<io::Result<Vec<u8>>>,
    /// Dropping this sender closes the watch channel and cancels the task.
    _shutdown: watch::Sender<()>,
}

impl DtlsTransport {
    /// Bind to `addr` and wait for a DTLS client to initiate the handshake.
    ///
    /// `addr` must be in `host:port` form (e.g. `"0.0.0.0:5555"`).
    /// The server's peer address is learned from the first incoming datagram.
    pub async fn bind(addr: &str, cert: DtlsCertificate) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr).await?;
        spawn_task(socket, cert, None, false)
    }

    /// Wrap an already-bound [`UdpSocket`] as a DTLS server.
    ///
    /// Useful in tests to get the OS-assigned port before handing the socket
    /// to the transport, avoiding a bind-probe TOCTOU race.
    pub fn from_server_socket(socket: UdpSocket, cert: DtlsCertificate) -> io::Result<Self> {
        spawn_task(socket, cert, None, false)
    }

    /// Bind to an ephemeral port and connect to `remote` as a DTLS client.
    ///
    /// `remote` must be in `host:port` form.  Hostname resolution is attempted
    /// if `remote` is not a bare IP.  The client immediately sends a
    /// `ClientHello` to initiate the DTLS handshake.
    pub async fn connect(remote: &str, cert: DtlsCertificate) -> io::Result<Self> {
        let peer = tokio::net::lookup_host(remote)
            .await?
            .next()
            .ok_or_else(|| io::Error::other(format!("could not resolve DTLS peer: {remote}")))?;
        let bind_addr: SocketAddr = if peer.is_ipv6() {
            "[::]:0".parse().unwrap()
        } else {
            "0.0.0.0:0".parse().unwrap()
        };
        let socket = UdpSocket::bind(bind_addr).await?;
        spawn_task(socket, cert, Some(peer), true)
    }

    /// Send `msg` over the DTLS session.
    ///
    /// The wire payload is `msg.header()` followed by `msg.body()`, matching
    /// the convention used by [`UdpTransport`] and [`WsTransport`].
    ///
    /// If the DTLS handshake has not yet completed, the payload is buffered
    /// inside the background task and sent once `Connected` fires.
    ///
    /// [`UdpTransport`]: crate::transport::udp::UdpTransport
    /// [`WsTransport`]: crate::transport::ws::WsTransport
    pub async fn send(&mut self, msg: &Message) -> io::Result<()> {
        let header = msg.header();
        let body = msg.body();
        let mut payload = Vec::with_capacity(header.len() + body.len());
        payload.extend_from_slice(header);
        payload.extend_from_slice(body);
        self.send_tx
            .send(payload)
            .await
            .map_err(|_| io::Error::other("dtls task closed"))
    }

    /// Receive the next plaintext datagram as an SP message.
    ///
    /// All datagram bytes land in `msg.body()`.  Returns an error if the
    /// DTLS session closes or if the background task exits.
    pub async fn recv(&mut self) -> io::Result<Message> {
        let data = self
            .recv_rx
            .recv()
            .await
            .ok_or_else(|| io::Error::other("dtls task closed"))??;
        let mut msg = Message::new();
        msg.push_back(&data);
        Ok(msg)
    }
}
