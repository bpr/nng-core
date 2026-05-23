# Changelog

All notable changes to `nng-core` are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

## [0.3.0] - 2026-05-22

### Added

- **VSOCK transport** (`--features vsock`, Linux only) — AF_VSOCK via
  `tokio-vsock` 0.7.  Enables SP messaging between a VM guest and its host
  (or between two VMs on the same hypervisor) without a network stack.
  Uses the TCP frame format (8-byte length header) and the standard SP
  handshake.  URL scheme: `vsock://CID:port`.  The CID component accepts
  numeric values or the aliases `any` (wildcard listener), `host` (CID 2),
  and `local` (CID 1, loopback).  All typed sockets support the scheme via
  the existing `listen` / `dial` constructors — no dedicated `listen_vsock`
  / `dial_vsock` methods are needed.  Loopback testing (guest-to-guest on
  the same VM) requires the `vsock_loopback` kernel module.
- **KCP transport** (`--features kcp`) — reliable, ordered ARQ over UDP
  via `kcp-tokio` 0.5.  Each SP connection maps to one KCP session over a
  managed UDP socket; the SP handshake and 8-byte TCP frame format run
  over that session unchanged.  URL scheme: `kcp://host:port`.  KCP is not
  part of the NNG/nanomsg ecosystem; this transport is for nng-core ↔
  nng-core communication only (use `quic://` for encrypted reliable UDP).
  Default config via `Socket::dial(addr)` / `Socket::listen(addr)`; for
  caller-supplied tuning use `Push0::dial_kcp_with(addr, KcpConfig)` etc.
  on Push0/Pull0/Pair0/Req0/Rep0.  Both peers must use the same config for
  correct ARQ behavior.
- **`Req0::builder()`** — builder API for initial configuration:
  ```rust
  let req = Req0::builder()
      .resend_time(Duration::from_secs(2))
      .dial("tcp://...")
      .await?;
  ```
  Replaces the `dial → set_resend_time` two-step at construction time.
  `Req0::set_resend_time` is **kept**, not deprecated, for runtime
  adjustment of an already-dialed socket (e.g. the `req0_resend` bench
  switches the resend interval between bench phases on a single shared
  `Req0`).
- **Stream-based accept API** on `Bus0`, `Pull0Fan`, and `Surveyor0` —
  each gains a `bind(addr) -> (Self, AcceptStream)` constructor plus a
  matching `add_peer` / `add_pusher` / `add_respondent` method:
  ```rust
  let (mut hub, mut accepts) = Bus0::bind(addr).await?;
  let peer = accepts.accept().await?;
  hub.add_peer(peer);
  ```
  Callers control per-peer admission policy (filter by peer addr,
  rate-limit, batch).  The classic
  `listen_and_accept(addr, n)` / `wait_for_respondents(n)` API is
  unchanged for backwards compatibility.
- **Typestate-pattern examples** — `examples/auth_then_data.rs` (a
  two-state authenticated PAIR0 protocol) and `examples/tictactoe.rs`
  (turn-based PAIR0 game with `Game<MyTurn>` / `Game<OpponentTurn>`
  session types) demonstrate compile-time enforcement of protocol-state
  invariants on top of nng-core sockets.

### Changed

- **Error types now use `thiserror`** — `NngError`, `TransportError`,
  `CodecError`, `ReqRepError`, and `WsError` switched from hand-rolled
  `Display` / `From` impls to the `thiserror` derive.  No user-visible
  behavior change *except*: `TransportError::Io` now carries a
  source-error string rather than being a unit variant, so the
  underlying I/O error message is preserved across the
  `TransportError` → `NngError` boundary (previously the message was
  replaced with the literal string `"transport I/O error"`).  Code that
  pattern-matches `TransportError::Io` without a binding must update to
  `TransportError::Io(_)`.

### Changed (internal, non-breaking)

- **`socket.rs` split** into a `socket/` directory: one file per
  per-protocol module (`pubsub0`, `survey0`, `bus0`, `pipeline0`,
  `pair0`, `reqrep0`, `tower_svc`).  Module paths are unchanged
  (`nng_core::socket::reqrep0::Req0`).
- **`forward_socket_method!` declarative macro** collapses the
  per-protocol wrapper methods (`Push0::dial_quic`,
  `Pull0::listen_kcp_with`, etc.) that forward to `Socket::*` into
  one-line invocations.
- **`adapt_async_io!` declarative macro** replaces the per-transport
  `embedded-io-async` adapter boilerplate (`tcp.rs`, `ipc.rs`,
  `vsock.rs`, `kcp.rs`) with one canonical implementation.

### Fixed

- **`Pull0Fan` zombie tasks** — when a `Pull0Fan` was dropped, its
  per-sender reader tasks could outlive it because they only exited on
  channel-close, which the dropped `Pull0Fan`'s receiver triggered only
  after every sender had emitted at least one item.  Fixed by replacing
  the channel-close shutdown signal with a `tokio::sync::watch` channel
  observed via `cancel.changed()`.  Dropping `Pull0Fan` now promptly
  cancels every reader task, even ones whose peers are idle.

## [0.2.2]

### Added

- **QUIC transport** (`--features quic`) — QUIC via `quinn` 0.11 + `rustls` 0.23.
  Each SP connection maps to one QUIC connection with a single bidirectional
  stream; the SP handshake and 8-byte TCP frame format run over that stream
  unchanged. All typed sockets that support TCP TLS also support QUIC:
  `Push0::listen_quic`/`dial_quic`, `Pull0::listen_quic`/`dial_quic`,
  `Pair0::listen_quic`/`dial_quic`, `Rep0::listen_quic`, `Req0::dial_quic`.
  Server sockets take PEM cert/key paths; client sockets take a
  `Arc<rustls::ClientConfig>` (use `quic::build_custom_client_config` to wrap
  it with the `"nng/1"` ALPN identifier). URL scheme: `quic://host:port`.
- **`BufferPool`** and **`FramedTransport::recv_pooled`** — opt-in buffer reuse
  for hot recv loops. Callers maintain a `BufferPool`, pass it to
  `recv_pooled`, and return body buffers with `pool.recycle(msg)`; subsequent
  receives reuse the recycled `Vec` in place when capacity allows.
  Defaults: 16 buffers, 64 KiB each, both configurable via
  `BufferPool::with_capacity`. Re-exported at the crate root.
- **Criterion benchmark suite** (`benches/`) — seven benchmark binaries:
  `latency` (REQ/REP round-trip over TCP and IPC, Rust-only + vs nngcat),
  `throughput` (PUSH/PULL pipeline), `codec` (frame encode/decode
  micro-benchmarks), `bus0_broadcast` (broadcast throughput vs peer count:
  2, 4, 8 peers), `req0_resend` (resend-path correctness stress test: four
  resend deadlines from disabled down to 20 µs, below TCP RTT, with
  per-iteration payload assertions to detect stale-reply acceptance),
  `pubsub` (pure `Sub0State::matches` filter micro-benchmarks across
  1/10/100 subscriptions × match-first/match-last/no-match/empty-prefix,
  plus end-to-end Pub0→Sub0 throughput with a message-count assertion to
  detect silent drops), and `large_msg` (PUSH/PULL and REQ/REP at 1/4/16
  MiB; reply-body length assertion detects silent recv truncation). `nngcat`
  from the system NNG package is used as the C libnng peer for the vs-C
  comparisons.
- **`scripts/bench_large_msg_nngcat.sh`** — shell script to measure C
  libnng (nngcat) PUSH throughput for 1/4/16 MiB payloads using
  `nngcat --file` with a `dd`-generated temp file; uses the same marginal
  subtraction methodology as `scripts/bench_c_vs_c.sh`.
- **`scripts/bench_c_vs_c.sh`** — shell script to measure C libnng (nngcat)
  PUSH/PULL marginal per-message cost via repeated runs at different message
  counts, for comparison against the Criterion benchmark results.

### Changed (internal, non-breaking)

- `FramedTransport::recv` no longer copies the payload buffer into a fresh
  `Message` body. The buffer is moved directly via `Message::from_parts`,
  eliminating one allocation and one full-payload memcpy per receive. The
  returned `Message` is byte-identical to before.

---

## [0.2.1] - 2026-05-17

### Fixed

- **TCP latency** — `TCP_NODELAY` is now set on all TCP connections (both dial
  and accept). Previously, the multi-write `send` path (length header + message
  header + body as separate `write_all` calls) interacted with Nagle's algorithm
  and Linux's 40 ms delayed-ACK timer to produce ~80 ms per-message latency for
  small payloads instead of the expected ~40 µs. This matches the default
  behavior of the NNG C library.

---

## [0.2.0] - 2026-05-17

### Breaking

- **Error type**: All socket methods now return `Result<_, NngError>` instead of
  `std::io::Result`. `From<NngError> for io::Error` is provided so `?` still compiles
  in functions that return `io::Result`, but call sites that name the error type
  explicitly must be updated.
- **`Surveyor0::survey`**: The signature changed from `survey(msg, timeout: Duration)`
  to `survey(msg)` (uses the per-socket `survey_time`). The old single-method API is
  replaced by `survey()` + `survey_with_timeout(msg, timeout)`.

### Added

- **`NngError`** — structured error enum (`src/error.rs`) with `#[non_exhaustive]`
  and `From<NngError> for io::Error`. Variants cover all failure modes: `Io`,
  `HandshakeFailed`, `ConnectionClosed`, `FrameTooLarge`, `BadFrameType`, `NoPeers`,
  `ReconnectExhausted`, `UnsupportedScheme`, `FeatureNotEnabled`, `ProtocolViolation`.
- **WebSocket transport** (`ws` feature, `ws://` scheme) — `WsTransport` wraps
  `tokio-tungstenite`; SP messages are sent as binary WebSocket frames.
- **TLS WebSocket transport** (`wss` feature, `wss://` scheme) — TLS layer via
  `tokio-tungstenite/rustls-tls-native-roots`.
- **NNG 2.x IPC URL scheme** (`ipc2://`) — Unix domain socket transport using the
  8-byte TCP frame format, compatible with NNG ≥ 2.0 IPC.
- **TLS-over-TCP transport** (`tls-tcp` feature, `tls+tcp://` scheme) — `TlsTcpStream`
  wraps `tokio-rustls`; `listen_tls_tcp` / `dial_tls_tcp` constructors on all socket
  types that support TCP.
- **UDP transport** (`udp` feature, `udp://` scheme) — `UdpTransport` over
  `tokio::net::UdpSocket`; single-peer, connectionless.
- **`tower` feature** — `Req0Service` implements `tower_service::Service<Message>`,
  wrapping `Arc<Mutex<Req0>>` so clones share one socket. Composable with Tower
  middleware (timeouts, retries, load-shedding).
- **`streams` feature** — async stream/sink adapters:
  - `Pull0::into_stream()`, `Sub0::into_stream()`, `Pair0::into_stream()` →
    `Pin<Box<dyn Stream<Item = Result<Message, NngError>> + Send>>`
  - `Push0::into_sink()`, `Pub0::into_sink()`, `Pair0::into_sink()` →
    `Pin<Box<dyn Sink<Message, Error = NngError> + Send>>`
  - `Pull0Fan` now implements `futures_core::Stream<Item = Message>` directly
    (per-sender disconnects are skipped; the stream ends when all senders close).
- **`Req0::set_resend_time` / automatic retransmit** — `Req0` gains a per-socket
  request-ID counter. When `set_resend_time(d)` is configured, `request()` retransmits
  after `d` and silently discards stale replies by ID, matching NNG's
  `NNG_OPT_REQ_RESENDTIME` semantics.
- **`Surveyor0::set_survey_time` / `survey_with_timeout`** — per-socket survey deadline
  (default 1 s, matching NNG's `NNG_OPT_SURVEYOR_SURVEYTIME`). `survey()` uses the
  stored deadline; `survey_with_timeout(msg, d)` accepts an explicit duration. Both poll
  all respondents concurrently so slow respondents do not starve fast ones.
- **Reconnect API** — `dial_reconnecting(addr)` (exponential back-off, default attempts)
  and `dial_with_reconnect(addr, opts)` (custom `ReconnectOptions`) added to all socket
  types that support `dial`.
- **`Sub0` subscriptions backed by `BTreeSet`** — duplicate prefixes are silently
  deduplicated; iteration order is now deterministic.

### Fixed

- `cargo doc --no-deps --features streams,tower,ws,wss,tls-tcp,udp` now produces zero
  warnings (all private-item intra-doc links replaced with plain text).

---

## [0.1.0] - 2026-05-09

### Added

- **All six SP protocol pairs**: REQ/REP (`reqrep0`), PUB/SUB (`pubsub0`),
  PUSH/PULL (`pipeline0`), PAIR (`pair0`), SURVEY/RESPONDENT (`survey0`),
  BUS (`bus0`).
- **TCP transport** (`tcp://` scheme) and **NNG 1.5.x IPC transport** (`ipc://` scheme,
  9-byte frame format).
- **`FramedTransport<T>`** — generic framed transport over any `embedded-io-async`
  `Read + Write` stream; supports `FrameFormat::Tcp` and `FrameFormat::Ipc`.
- **`ZeroCopyMessage<N>`** — stack-allocated message with O(1) `trim_front` (same
  trick as Linux `sk_buff`); compatible with the `no_std` target.
- **`MessageBuf` trait** — shared interface for `Message` (heap) and
  `ZeroCopyMessage` (stack).
- **`no_std` support** — `src/codec.rs`, `src/message.rs`, and `src/protocols/`
  compile with `--no-default-features`.
- **Interop tests** (`tests/interop_nngcat.rs`) — 8 tests against `nngcat` from
  system NNG 1.5.2, covering REQ/REP, PUSH/PULL, and PUB/SUB over TCP and IPC.
- **Kani formal verification** — 9 harnesses in `src/codec.rs` and `src/message.rs`.
- **Fuzz targets** — `codec_handshake`, `codec_frame`, `transport_recv_tcp`,
  `transport_recv_ipc` (libFuzzer via `cargo-fuzz`).
- **Property tests** — `proptest` suites for codec, message, and all three protocol
  state machines.
- **21 examples** across REQ/REP, PUB/SUB, PUSH/PULL, PAIR, SURVEY, and BUS patterns.

### Fixed

- **`Pull0Fan` cancellation safety** — the old biased-select poll loop dropped
  `FramedTransport::recv` futures mid-read, corrupting the byte stream. Fixed by
  running each sender in a dedicated `tokio::spawn` task that drives `recv` to
  completion before forwarding over an `mpsc` channel.
- **`Bus0` cancellation safety** — same root cause as `Pull0Fan`. Fixed by making
  `FramedTransport::recv` itself cancellation-safe via a `RecvBuf` field that
  preserves partial reads across future drops.
- **`Bus0` dynamic membership** — the `TcpListener` was dropped after
  `listen_and_accept(addr, n)` returned; late-connecting peers were never accepted.
  Fixed by keeping the listener alive in `Bus0` and exposing `accept_pending()`.
- **`Surveyor0` dynamic membership** — same listener-drop problem; fixed with
  `accept_pending()`.
- **REQ0 backtrace header** — the high bit (`0x8000_0000`) is now set in the wire
  request ID, matching NNG's required format for REP-side scanning.
- **NNG 1.5.x IPC framing** — 9-byte header (`0x01` type byte + 8-byte BE u64
  length) correctly handled for both send and receive.

[Unreleased]: https://github.com/bpr/nng-core/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/bpr/nng-core/compare/v0.2.2...v0.3.0
[0.2.2]: https://github.com/bpr/nng-core/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/bpr/nng-core/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/bpr/nng-core/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/bpr/nng-core/releases/tag/v0.1.0
