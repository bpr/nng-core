# nng-pure

A pure-Rust, `no_std`-compatible implementation of the [NNG Scalability Protocols](https://nng.nanomsg.org/).

## Goals

`nng-sys`, `nng`, and `anng` all depend on `libnng`, a C library built via CMake. This blocks use in WebAssembly, bare-metal embedded targets, and environments without a C toolchain.

`nng-pure` replaces the C core with a clean-room Rust implementation:

- **No C dependency.** No `cmake`, no `cc`, no `bindgen`. Pure Rust all the way down.
- **`no_std` + `alloc` core.** The wire codec and protocol state machines compile without the standard library. Only the TCP socket layer (behind the `std` feature) requires tokio.
- **Runtime-agnostic transport.** The framing layer is generic over `embedded-io-async`'s `Read + Write` traits, so the same protocol code runs on tokio (Linux/macOS/Windows) and Embassy (bare metal).
- **Interoperable wire format.** The SP (Scalability Protocol) handshake and message framing are byte-for-byte compatible with `libnng`, so `nng-pure` peers talk to native NNG nodes over TCP.

The long-term goal is for `nng` and `anng` to use `nng-pure` instead of `nng-sys`, removing the C dependency from the whole workspace.

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
nng-pure = { path = "../nng-pure" }          # std + tokio (default)
# nng-pure = { path = "../nng-pure", default-features = false }  # no_std core only
```

### REQ/REP

```rust
use std::fmt::Write;
use nng_pure::{Message, socket::reqrep0};

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
use nng_pure::{Message, socket::pubsub0};

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

See `examples/` for complete, runnable examples of every protocol.

## Features

| Feature | Default | Description |
|---|---|---|
| `std` | yes | Enables tokio TCP transport and the high-level socket API |

With `--no-default-features`, only `codec`, `message`, and `transport` (generic over any `embedded-io-async` stream) are compiled. The `protocols/` state machines are always available.

## Design

See [`src/README.md`](src/README.md) for a layer-by-layer code overview.

The key design decisions:

**Layered architecture.** Codec → transport → state machines → socket API. Each layer is independently testable. The codec and state machines have zero I/O dependency and compile in `no_std`.

**Protocol state machines own no I/O.** `Req0State`, `Sub0State`, etc. are plain structs that manipulate `Message` headers in memory. They are completely decoupled from sockets or futures. This makes them trivial to unit-test and easy to port to new transports.

**`embedded-io-async` for transport polymorphism.** `FramedTransport<T>` is generic over any `T: Read + Write` from the `embedded-io-async` crate. The tokio TCP adapter is one thin wrapper; an Embassy UART adapter would be another.

**Header/body separation in `Message`.** Each `Message` carries a protocol header and an application body as separate `Vec<u8>` buffers. On send, `FramedTransport` writes header then body contiguously. On receive, all wire bytes land in the body; the protocol state machine then strips its header fields from the front of the body. This mirrors NNG's internal message layout without requiring unsafe pointer arithmetic.

## Wire compatibility

The SP wire protocol is documented in the NNG source. `nng-pure` uses the same:

- 8-byte handshake: `\x00SP\x00` + own protocol ID (u16 BE) + `\x00\x00`
- Per-message framing: u64 BE payload length + (header bytes)(body bytes)
- Protocol IDs from `NNI_PROTO(major, minor) = major * 16 + minor`

To verify interoperability, run the `req-rep` example against a native `libnng` server.

## Running the examples

```bash
cargo run -p nng-pure --example req-rep
cargo run -p nng-pure --example pubsub
cargo run -p nng-pure --example pipeline
cargo run -p nng-pure --example pair
cargo run -p nng-pure --example survey
cargo run -p nng-pure --example bus
```

## Running the tests

```bash
cargo test -p nng-pure          # all 75 tests
cargo build -p nng-pure --no-default-features   # verify no_std core
```
