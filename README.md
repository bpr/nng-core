# nng-core

A pure-Rust reimplementation of the [NNG Scalability Protocols](https://nng.nanomsg.org/).

## Goals

`nng-sys`, `nng`, and `anng` all depend on `libnng`, a C library built via CMake. This blocks use in WebAssembly, bare-metal embedded targets, and environments without a C toolchain.

`nng-core` replaces the C core with a pure-Rust implementation:

- **No C dependency.** No `cmake`, no `cc`, no `bindgen`. Pure Rust all the way down.
- **`no_std` + `alloc` core.** The wire codec and protocol state machines compile without the standard library. Only the TCP socket layer (behind the `std` feature) requires tokio.
- **Runtime-agnostic transport.** The framing layer is generic over `embedded-io-async`'s `Read + Write` traits, so the same protocol code runs on tokio (Linux/macOS/Windows) and Embassy (bare metal).
- **Interoperable wire format.** The SP (Scalability Protocol) handshake and message framing are byte-for-byte compatible with `libnng`, so `nng-core` peers talk to native NNG nodes over TCP.

## Protocols

All six NNG protocol families are implemented:

| Protocol | Socket types | Pattern |
|---|---|---|
| REQ/REP | `reqrep0::Req0`, `reqrep0::Rep0` | Request/reply with automatic request-ID tracking |
| PUB/SUB | `pubsub0::Pub0`, `pubsub0::Sub0` | Fan-out broadcast with byte-prefix topic filtering |
| PUSH/PULL | `pipeline0::Push0`, `pipeline0::Pull0` | Pipeline / work distribution |
| PAIR | `pair0::Pair0` | Bidirectional point-to-point |
| SURVEYOR/RESPONDENT | `survey0::Surveyor0`, `survey0::Respondent0` | Timed broadcast with collected replies |
| BUS | `bus0::Bus0` | Many-to-many broadcast |

## Usage

```toml
[dependencies]
nng-core = { path = "../nng-core" }          # std + tokio (default)
# nng-core = { path = "../nng-core", default-features = false }  # no_std core only
```

### REQ/REP

```rust
use std::fmt::Write;
use nng_core::{Message, socket::reqrep0};

// Server
let mut rep = reqrep0::Rep0::listen("tcp://127.0.0.1:5555").await?;
let (request, responder) = rep.receive().await?;
let mut reply = Message::new();
write!(reply, "Hello, {}!", String::from_utf8_lossy(request.body()))?;
responder.reply(reply).await?;

// Client
let mut req = reqrep0::Req0::dial("tcp://127.0.0.1:5555").await?;
let mut msg = Message::new();
write!(msg, "world")?;
let reply = req.request(msg).await?;
println!("{}", String::from_utf8_lossy(reply.body())); // "Hello, world!"
```

### PUB/SUB

```rust
use nng_core::{Message, socket::pubsub0};

// Publisher
let mut pub0 = pubsub0::Pub0::listen("tcp://127.0.0.1:5556").await?;
pub0.wait_for_subscribers(1).await?;
let mut msg = Message::new();
msg.push_back(b"news: breaking update");
pub0.publish(msg).await?;

// Subscriber
let mut sub = pubsub0::Sub0::dial("tcp://127.0.0.1:5556").await?;
sub.subscribe_to(b"news:");          // only receive messages starting with "news:"
let msg = sub.next().await?;
```

### QUIC (requires `--features quic`)

```toml
# Cargo.toml
nng-core = { version = "...", features = ["quic"] }
rustls = "0.23"   # only needed when constructing a custom ClientConfig
```

```rust
use nng_core::{Message, socket::reqrep0};
use std::{path::Path, sync::Arc};

// Server — cert and key are PEM files
let mut rep = reqrep0::Rep0::listen_quic(
    "quic://0.0.0.0:5555",
    Path::new("server.crt"),
    Path::new("server.key"),
).await?;
let (request, responder) = rep.receive().await?;
let mut reply = Message::new();
reply.push_back(b"pong");
responder.reply(reply).await?;

// Client with CA-signed certificate — no rustls dependency needed
let mut req = reqrep0::Req0::dial("quic://myserver.example.com:5555").await?;

// Client with self-signed certificate — supply a custom rustls::ClientConfig
let mut root_store = rustls::RootCertStore::empty();
root_store.add(cert_der).unwrap();
let config = Arc::new(
    rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth(),
);
let mut req = reqrep0::Req0::dial_quic("quic://127.0.0.1:5555", config).await?;
let reply = req.request(msg).await?;
```

See `examples/quic_reqrep.rs` for a complete, self-contained runnable example.

See `examples/` for complete, runnable examples of every protocol.

## Features

### Transport support

The table compares nng-core against libnng (the reference C implementation) and
mangos (the reference Go implementation). libnng v2 is currently in alpha.

| Transport | Scheme | nng-core | libnng 1.5 | libnng 2 | mangos |
|---|---|:---:|:---:|:---:|:---:|
| TCP | `tcp://` | ✓ | ✓ | ✓ | ✓ |
| IPC — NNG 1.5 wire | `ipc://` | ✓ | ✓ | — | ✓ |
| IPC — NNG 2 wire | `ipc://` / `ipc2://` | ✓ (as `ipc2://`) | — | ✓ | — |
| TLS over TCP | `tls+tcp://` | ✓ | ✓ | ✓ | ✓ |
| WebSocket | `ws://` | ✓ | ✓ | ✓ | ✓ |
| WebSocket over TLS | `wss://` | ✓ | ✓ | ✓ | ✓ |
| QUIC | `quic://` | ✓ | — | — | — |
| UDP (datagram) | `udp://` | ✓ | — | ✓ | — |
| VSOCK (Linux VM) | `vsock://` | ✓ | — | — | — |
| DTLS | `dtls://` | aborted | — | ✓ | — |
| In-process | `inproc://` | not yet | ✓ | ✓ | ✓ |
| ZeroTier | `zt://` | — | ✓ | — | — |
| Socket / fd passthrough | `socket://` | — | — | ✓ | — |

The two IPC rows reflect a wire-format break between NNG generations: NNG 1.5
uses a 9-byte frame header (`\x01` type byte + 8-byte length); NNG 2 uses the
same 8-byte header as TCP. nng-core implements both: `ipc://` speaks the 1.5
wire format (interoperable with libnng 1.5.x and nngcat), `ipc2://` speaks the
NNG 2 format.

`inproc://` is not yet implemented. Its main appeal is URL portability: code
that uses `inproc://` in tests can switch to `tcp://` in production by changing
one string, with no other changes — mangos supports it for exactly this reason.
The obstacle is architectural: `inproc://` requires a process-global name
registry (a static table that `listen` inserts into and `dial` looks up), which
is at odds with nng-core's otherwise stateless transport layer. It could be
added, but has not been prioritized.

`socket://` (libnng v2's fd-passthrough transport, e.g. `socket://fd/5`) is
also omitted, for different reasons. The escape hatch already exists:
`FramedTransport<T>` is generic over any `T: Read + Write`, so a caller with a
raw fd can convert it to a tokio `TcpStream` via `FromRawFd` and construct a
`FramedTransport` directly without any URL dispatch. Hiding that behind a URL
string would obscure the `unsafe` that `FromRawFd` requires. There is also no
interop pressure yet — `socket://` only exists in libnng v2 alpha.

### Cargo features

| Feature | Default | Description |
|---|---|---|
| `std` | yes | Enables tokio TCP transport and the high-level socket API |
| `quic` | no | QUIC transport via `quinn` + `rustls` (TLS 1.3 built-in). Adds `listen_quic` / `dial_quic` to all socket types; `dial("quic://...")` uses the system's native root store |
| `vsock` | no | VSOCK VM transport, Linux only. Adds `listen("vsock://any:port")` / `dial("vsock://2:port")` to all socket types |
| `tls-tcp` | no | TLS over TCP via `rustls`. Adds `listen_tls_tcp` / `dial_tls_tcp` |
| `ws` | no | WebSocket transport via `tokio-tungstenite`. Adds `dial("ws://...")` |
| `wss` | no | WebSocket over TLS. Adds `listen_tls` / `dial("wss://...")` |
| `udp` | no | UDP transport. Adds `dial("udp://...")` — no SP handshake, datagram-only |
| `streams` | no | `futures_core::Stream` / `futures_sink::Sink` adapters for socket types |
| `tower` | no | `tower-service` integration |

With `--no-default-features`, only `codec`, `message`, and `transport` (generic over any `embedded-io-async` stream) are compiled. The `protocols/` state machines are always available.

## Design

See [`src/README.md`](src/README.md) for a layer-by-layer code overview.

The key design decisions:

**Layered architecture.** Codec → transport → state machines → socket API. Each layer is independently testable. The codec and state machines have zero I/O dependency and compile in `no_std`.

**Protocol state machines own no I/O.** `Req0State`, `Sub0State`, etc. are plain structs that manipulate `Message` headers in memory. They are completely decoupled from sockets or futures. This makes them trivial to unit-test and easy to port to new transports.

**`embedded-io-async` for transport polymorphism.** `FramedTransport<T>` is generic over any `T: Read + Write` from the `embedded-io-async` crate. The tokio TCP adapter is one thin wrapper; an Embassy UART adapter would be another.

**Header/body separation in `Message`.** Each `Message` carries a protocol header and an application body as separate `Vec<u8>` buffers. On send, `FramedTransport` writes header then body contiguously. On receive, all wire bytes land in the body; the protocol state machine then strips its header fields from the front of the body. This mirrors NNG's internal message layout without requiring unsafe pointer arithmetic.

## Wire compatibility

The SP wire protocol is documented in the NNG source. `nng-core` uses the same:

- 8-byte handshake: `\x00SP\x00` + own protocol ID (u16 BE) + `\x00\x00`
- Per-message framing: u64 BE payload length + (header bytes)(body bytes)
- Protocol IDs from `NNI_PROTO(major, minor) = major * 16 + minor`

To verify interoperability, run any example against a native `libnng` peer, or run the interop test suite (requires `nngcat` from NNG 1.5.x): `cargo test --test interop_nngcat`.

## Running the tests

```bash
cargo test                              # 144 tests across all suites
cargo build --no-default-features       # verify no_std core compiles
cargo test --test interop_nngcat        # NNG 1.5.x wire-compat (needs nngcat in PATH)
```

## Formal verification (Kani)

Nine [Kani](https://model-checking.github.io/kani/) harnesses provide mathematical proofs of correctness for the codec and message buffer types. They live inline in `src/codec.rs` and `src/message.rs` under `#[cfg(kani)]` and require [`cargo-kani`](https://github.com/model-checking/kani) (`cargo install cargo-kani`).

```bash
RUSTC_WRAPPER="" cargo kani    # verify all 9 harnesses
```

| Harness | What is proved |
|---|---|
| `decode_handshake_never_panics` | `decode_handshake` never panics on any 8-byte input |
| `encode_decode_handshake_roundtrip` | Any `ProtocolId` survives an encode→decode round-trip |
| `decode_frame_never_panics` | `decode_frame` never panics on inputs up to 16 bytes |
| `decode_frame_oversized_length_is_incomplete` | Any non-zero declared length with no payload bytes returns `Incomplete` — proves the integer-overflow fix is correct for **all** possible `u64` length values |
| `zcm_push_back_body_correct` | `ZeroCopyMessage::push_back` stores exactly the bytes that were passed |
| `zcm_trim_front_body_correct` | `ZeroCopyMessage::trim_front(n)` removes exactly the first `n` bytes |
| `zcm_header_does_not_alias_body` | Writing to the header region and body region are completely independent |
| `message_push_back_trim_front_correct` | `Message::push_back` + `trim_front(n)` leaves the suffix starting at byte `n` |
| `message_header_body_independent` | `Message` header and body do not alias |

The `RUSTC_WRAPPER=""` prefix disables `sccache`, which does not recognise the Kani compiler wrapper.

## Property-based tests

Three [proptest](https://github.com/proptest-rs/proptest) suites run as part of `cargo test` and exercise the codec and protocol state machines with arbitrary inputs.

| Test file | What it covers |
|---|---|
| `tests/proptest_codec.rs` | `encode_frame` + `decode_frame` round-trip; `encode_handshake` + `decode_handshake` round-trip; oversized length fields return `Incomplete` (regression for a fuzzing find) |
| `tests/proptest_reqrep0.rs` | REQ/REP round-trip for any body; wire ID always has the high backtrace-chain bit set; wrong ID always rejected; ID counters are unique and never zero across up to 199 consecutive calls; same ID-sequence properties for `Surveyor0State` |
| `tests/proptest_sub0.rs` | `Sub0State` modelled as a [proptest-state-machine](https://github.com/proptest-rs/proptest/tree/main/proptest-state-machine) against a `BTreeSet` reference — arbitrary sequences of `subscribe` / `unsubscribe` / `matches` must agree with the reference on every `matches` call and on `is_empty()` |

The state-machine test (`proptest_sub0`) is the most thorough: it generates sequences of up to 20 operations, biasing toward unsubscribing existing topics to exercise deduplication and the empty-subscriptions path.

## Fuzz testing

Four [libFuzzer](https://llvm.org/docs/LibFuzzer.html) targets live in `fuzz/`. They require a nightly toolchain (`rustup toolchain install nightly`) and [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz) (`cargo install cargo-fuzz`).

| Target | What it exercises |
|---|---|
| `codec_handshake` | `decode_handshake` with arbitrary 8-byte input |
| `codec_frame` | `decode_frame` with arbitrary byte slices |
| `transport_recv_tcp` | `FramedTransport::recv` (TCP framing) fed arbitrary post-handshake bytes |
| `transport_recv_ipc` | `FramedTransport::recv` (IPC framing) — focuses on the type-byte check |

```bash
cargo +nightly fuzz build                    # compile all targets
cargo +nightly fuzz run codec_handshake      # run until Ctrl-C or a finding
cargo +nightly fuzz run transport_recv_tcp
```

The `transport_recv_tcp` and `transport_recv_ipc` targets prepend a valid SP handshake so that `FramedTransport::connect` succeeds; the fuzz bytes are then fed as the message stream. Any panic or out-of-bounds access is reported as a finding and saved to `fuzz/artifacts/<target>/`.

Seed inputs are in `fuzz/corpus/<target>/` and cover valid frames, empty frames, and (for the IPC target) a frame with a wrong type byte.

---

## Examples

Examples are grouped into two tiers:

- **Introductory** — one binary per role, one protocol per example. Run them in two terminals.
- **Pattern** — realistic multi-process designs showing how the primitives compose.

All examples use pre-allocated ports starting at 10000. See the per-example docs below for exact ports.

### Introductory examples

---

#### REQ/REP — `req_server` / `req_client`

Port 10000. Demonstrates the request/reply pattern: the server receives a named greeting request and sends a personalised reply.

```bash
# terminal 1
cargo run --example req_server

# terminal 2
cargo run --example req_client
```

`Req0` sends one request and waits; `Rep0` receives it and replies via a one-shot `Responder` handle. The handle enforces that exactly one reply is sent per request.

---

#### PUB/SUB — `pub_server` / `sub_client`

Port 10001. Demonstrates fan-out: the publisher broadcasts numbered messages and the subscriber filters by a prefix.

```bash
# terminal 1
cargo run --example pub_server

# terminal 2
cargo run --example sub_client
```

`Sub0::subscribe_to(b"prefix:")` installs a byte-prefix filter. Messages that do not begin with the subscribed prefix are dropped by the socket before delivery. Multiple subscribers with different prefixes can run concurrently.

---

#### PUSH/PULL — `push_node` / `pull_worker`

Port 10002. Demonstrates a simple one-hop pipeline: one pusher sends numbered messages, one puller processes them.

```bash
# terminal 1
cargo run --example pull_worker

# terminal 2
cargo run --example push_node
```

`Push0::push` writes into the pipeline; `Pull0::pull` reads. With multiple pullers connected to the same pusher, NNG distributes messages round-robin.

---

#### SURVEYOR/RESPONDENT — `surveyor` / `respondent`

Port 10003. Demonstrates timed broadcast: the surveyor sends a ROLL_CALL query and collects all replies that arrive within a deadline.

```bash
# terminal 1
cargo run --example surveyor

# terminal 2 (or more)
cargo run --example respondent
```

`Surveyor0::survey(msg, timeout)` broadcasts and returns a `Vec<Message>` of all replies received before the deadline. Respondents that miss the window are silently ignored.

---

#### PAIR — `pair_node`

Port 10004. Demonstrates bidirectional point-to-point: node A echoes everything it receives; node B sends numbered pings and prints the echoed replies.

```bash
# terminal 1
cargo run --example pair_node -- A

# terminal 2
cargo run --example pair_node -- B
```

`Pair0` has no protocol-level framing; each side can send and receive freely. PAIR is the only NNG pattern with no queue policy.

---

#### BUS — `bus`

Port 54326. Demonstrates many-to-many broadcast in a single process. A hub node accepts two spoke nodes, broadcasts a message to both, and then collects one reply from each spoke.

```bash
cargo run --example bus
```

`Bus0::broadcast` sends to all connected peers. `recv_any` receives from whichever peer has a message ready. Unlike PUB/SUB there is no filtering: every peer receives every broadcast. This example uses `tokio::spawn` internally so it runs as one process with three concurrent nodes.

---

### Pattern examples

These require three or more terminals and a specific start order. Read the header comment in each file for the full start sequence.

---

#### Pipeline: ventilator / workers / sink

Files: `pipeline_ventilator`, `pipeline_worker`, `pipeline_sink`  
Ports: ventilator 11000, sink 11001

Demonstrates the parallel-pipeline pattern from the ZeroMQ guide. The ventilator distributes 15 tasks across 3 workers in round-robin order; each worker sleeps for the encoded duration and then reports completion to the sink. The sink measures wall time to confirm that 3× parallelism reduces total time from ~1500 ms to ~500 ms.

```bash
# terminal 1 — start sink first so workers can connect before ventilator begins pushing
cargo run --example pipeline_sink

# terminal 2
cargo run --example pipeline_ventilator

# terminals 3, 4, 5
cargo run --example pipeline_worker -- alpha
cargo run --example pipeline_worker -- beta
cargo run --example pipeline_worker -- gamma
```

The sink exits after all 15 results arrive. Workers and the ventilator can then be killed.

**Library types used:** `Push0Fan` (ventilator, listener-side round-robin push to N pullers), `Pull0` (worker input), `Push0` (worker output), `Pull0Fan` (sink, listener-side pull from N pushers). `Push0Fan` and `Pull0Fan` are `pipeline0` extensions not in the base NNG API.

---

#### Topic-filtered pub/sub — `topic_pub` / `topic_sub`

Port 11002

Demonstrates per-topic subscription. The publisher cycles through three topic prefixes (`SPORTS:`, `WEATHER:`, `FINANCE:`) every 300 ms; each subscriber filters to one topic.

```bash
# terminal 1
cargo run --example topic_pub

# terminals 2, 3, 4 (any subset)
cargo run --example topic_sub -- SPORTS
cargo run --example topic_sub -- WEATHER
cargo run --example topic_sub -- FINANCE
```

Each subscriber receives only messages whose body begins with its subscribed prefix. Subscribers can be started or stopped at any time; `Pub0` silently drops messages with no matching subscribers.

---

#### Service discovery — `discovery_registry` / `discovery_node`

Port 11003

Demonstrates dynamic service membership using the surveyor pattern. The registry broadcasts a ROLL_CALL survey every 3 seconds and prints all nodes that respond within the 500 ms window. Nodes can join or leave between survey rounds.

```bash
# terminal 1
cargo run --example discovery_registry

# terminals 2, 3, … (start at any time)
cargo run --example discovery_node -- alpha tcp://127.0.0.1:9001
cargo run --example discovery_node -- beta  tcp://127.0.0.1:9002
```

Each node dials the registry and responds with its name and address. The registry calls `accept_pending()` before each survey to pick up nodes that connected since the previous round, so membership changes are reflected in the next survey.

---

#### Paranoid Pirate — `pp_worker` / `pp_client`

Port 11004

Demonstrates fault-tolerant request/reply. The worker crashes every 4th request (by dropping its `Rep0` socket without replying) and immediately re-listens. The client uses a per-attempt dial-timeout strategy: each request creates a fresh `Req0`, allows 2 seconds for a reply, and retries up to 3 times before giving up.

```bash
# terminal 1
cargo run --example pp_worker

# terminal 2
cargo run --example pp_client
```

With N_REQUESTS = 10 and a crash every 4 served requests, the client retries on tasks 4, 7, and 10. All 10 tasks complete. The "connection refused" messages on early dial attempts are expected: the worker needs ~50 ms to re-bind the port after closing its previous `Rep0`.

The pattern shows how to build at-least-once delivery over NNG's at-most-once transport without any broker.

---

#### Work-queue broker — `wq_worker` / `wq_broker` / `wq_client`

Ports: workers 12000–12002, broker frontend 11005

Demonstrates a transparent REQ/REP proxy. The broker connects to 3 workers at startup, then listens for clients. Each client request is forwarded to the next worker in round-robin order; the worker's reply is forwarded back to the client. The client sees a single endpoint; the workers see a single requester.

```bash
# terminals 1, 2, 3
cargo run --example wq_worker -- 0
cargo run --example wq_worker -- 1
cargo run --example wq_worker -- 2

# terminal 4
cargo run --example wq_broker

# terminal 5
cargo run --example wq_client
```

The reply payloads (`worker-N:job-M:done`) confirm which worker handled each job. Worker index in the reply should cycle 0→1→2→0→… across the 8 client requests.

---

#### QUIC REQ/REP — `quic_reqrep`

Port 15000. Demonstrates REQ/REP over QUIC in a single process. Generates a
self-signed TLS certificate at startup, runs server and client with
`tokio::spawn`, and exchanges five requests.

```bash
cargo run --example quic_reqrep --features quic
```

`Rep0::listen_quic` binds a QUIC endpoint; `Req0::dial_quic` connects with a
custom `rustls::ClientConfig` that trusts the self-signed certificate. For
production use with CA-signed certificates replace `dial_quic` with
`Req0::dial("quic://host:port")`, which uses the system's native root store.
