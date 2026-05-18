# Changelog

All notable changes to `nng-core` are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

- **`BufferPool`** and **`FramedTransport::recv_pooled`** — opt-in buffer reuse
  for hot recv loops. Callers maintain a `BufferPool`, pass it to
  `recv_pooled`, and return body buffers with `pool.recycle(msg)`; subsequent
  receives reuse the recycled `Vec` in place when capacity allows.
  Defaults: 16 buffers, 64 KiB each, both configurable via
  `BufferPool::with_capacity`. Re-exported at the crate root.
- **Criterion benchmark suite** (`benches/`) — five benchmark binaries:
  `latency` (REQ/REP round-trip over TCP and IPC, Rust-only + vs nngcat),
  `throughput` (PUSH/PULL pipeline), `codec` (frame encode/decode
  micro-benchmarks), `bus0_broadcast` (broadcast throughput vs peer count:
  2, 4, 8 peers), `req0_resend` (resend-path correctness stress test: four
  resend deadlines from disabled down to 20 µs, below TCP RTT, with
  per-iteration payload assertions to detect stale-reply acceptance), and
  `pubsub` (pure `Sub0State::matches` filter micro-benchmarks across
  1/10/100 subscriptions × match-first/match-last/no-match/empty-prefix,
  plus end-to-end Pub0→Sub0 throughput with a message-count assertion to
  detect silent drops). `nngcat` from the system NNG package is used as the
  C libnng peer for the vs-C comparisons.
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

[Unreleased]: https://github.com/bpr/nng-core/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/bpr/nng-core/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/bpr/nng-core/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/bpr/nng-core/releases/tag/v0.1.0
