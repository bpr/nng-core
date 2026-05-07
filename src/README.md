# nng-core source overview

The source is organized in four layers, each depending only on the layers below it. The bottom two layers compile under `#![no_std]`; the top two require the `std` feature (tokio).

```
socket.rs           ← high-level async socket API (std only)
    │
transport.rs        ← SP framing over any embedded-io-async stream (std: TCP adapter)
    │
protocols/          ← pure-logic state machines, no I/O (no_std)
    │
codec.rs            ← SP wire format: handshake encode/decode, frame encode/decode (no_std)
message.rs          ← Message type: header + body buffers (no_std)
```

---

## `message.rs` — `Message`

`Message` is a two-part buffer: a protocol `header` and an application `body`, each a `Vec<u8>`.

The header carries SP protocol metadata (request IDs, routing tokens, TTL counts, survey IDs). The body carries application payload. Both support prepend / append / trim operations without copying the other half.

`FramedTransport::send` serializes header-then-body contiguously. `FramedTransport::recv` reads all wire bytes into the body; the protocol state machine then strips its header fields from the front of the body using `trim_front`.

`Message` implements `core::fmt::Write` (appends UTF-8 to body) and `embedded_io_async::Write` (appends raw bytes to body).

---

## `codec.rs` — SP wire codec

Defines `ProtocolId(u16)` constants for all 11 protocol variants and three codec operations:

**Handshake** — exchanged once on connect, both directions:
```
[0x00] ['S'] ['P'] [0x00]  — magic
[hi]   [lo]                — own protocol ID, big-endian u16
[0x00] [0x00]              — reserved
```
`encode_handshake` builds the 8-byte array; `decode_handshake` parses it; `check_peer` verifies the remote's ID is the expected complement (e.g., REQ0 expects REP0).

**Message frame** — per message, after handshake:
```
[8-byte u64 BE total length] [header bytes] [body bytes]
```
`encode_frame` serializes a `Message`; `decode_frame` parses a frame from a byte slice and returns the consumed length. The transport layer uses streaming `read_exact` instead of buffering, so `decode_frame` is mainly used in tests.

Protocol IDs are derived from NNG's `NNI_PROTO(major, minor) = major * 16 + minor` macro, confirmed against the NNG C source.

---

## `protocols/` — state machines

Six modules, one per protocol family. Each contains plain structs with no async code and no I/O — just `Message` manipulation. They are independently unit-tested in `tests/protocols.rs`.

### `reqrep.rs`

`Req0State { next_id: u32 }`:
- `prepare_outgoing(&mut self, msg)` — wraps the current request ID into the message header; returns the ID.
- `process_incoming(&self, msg, sent_id)` — strips the 4-byte ID from the front of the body; returns `Err(IdMismatch)` if it doesn't match.

`Rep0State` (stateless):
- `process_incoming(&self, msg)` — strips routing bytes from the body front; returns a `RoutingInfo` token.
- `prepare_reply(&self, msg, routing)` — attaches the saved routing info to the reply header.

The routing info is opaque to the application; it is simply stored and echoed back.

### `pubsub.rs`

`Sub0State { subscriptions: Vec<Vec<u8>> }`:
- `subscribe(prefix)` / `unsubscribe(prefix)` — manage the prefix list.
- `matches(msg)` — returns `true` if `msg.body()` starts with any registered prefix. An empty prefix matches everything; an empty subscription list matches nothing.

`Pub0State` — unit struct, stateless send.

### `pipeline.rs`, `bus.rs`

Both are unit structs. PUSH/PULL and BUS add no protocol headers to messages; framing alone is sufficient.

### `pair.rs`

`Pair0State` — unit struct (no headers).

`Pair1State { max_ttl: u8 }`:
- `attach_ttl(msg)` — prepends a 4-byte TTL field (high bit set = "end of path") to the header.
- `process_incoming(msg)` — strips the TTL field from the body front; returns `Err(TtlExpired)` if the count reached zero, preventing routing loops in device-forwarding topologies.

### `survey.rs`

`Surveyor0State { next_id, current_id }`:
- `prepare_survey(msg)` — attaches a survey ID to the header; saves it as the "current" survey.
- `process_response(msg)` — strips the 4-byte ID from the body front; returns `Err(StaleSurveyId)` if it doesn't match the current survey.

`Respondent0State` (stateless):
- `process_incoming(msg)` — strips the survey ID from the body; returns a `SurveyRoutingInfo` token.
- `prepare_response(msg, routing)` — attaches the routing token to the reply header.

---

## `transport.rs` — `FramedTransport<T>`

Generic over any `T: embedded_io_async::Read + Write`.

`FramedTransport::connect(inner, local_proto)`:
1. Sends the 8-byte SP handshake for `local_proto`.
2. Reads the remote's 8-byte handshake.
3. Validates peer compatibility (`check_peer`).
4. Returns `Self` on success; `Err(TransportError)` otherwise.

`send(msg)` — writes the 8-byte length prefix, then the header bytes, then the body bytes.

`recv()` — reads the 8-byte length, allocates a buffer, reads the full payload into it, and returns a `Message` with all payload bytes in the body. The protocol state machine splits the header out afterwards.

**Submodules:**

`transport::tcp` (std only) — `TokioTcpStream(tokio::net::TcpStream)` that adapts `tokio::io::AsyncRead/Write` to `embedded-io-async`'s `Read + Write` traits.

`transport::loopback` (std only) — `inproc_pair(local, peer)` creates two `FramedTransport`s connected by a `tokio::io::duplex` channel, running both handshakes concurrently via `tokio::try_join!`. Used in unit tests to avoid network sockets.

---

## `socket.rs` — high-level socket API (std only)

Provides a thin async API over `FramedTransport<TokioTcpStream>` for each protocol.

**`Socket<P>`** — the base type. Holds one `FramedTransport` and a `PhantomData<P>` protocol marker. Provides `listen(addr, proto)` (bind + accept one connection), `dial(addr, proto)`, `send_raw`, and `recv_raw`. Protocol-specific types wrap `Socket<P>` to expose only the methods valid for that protocol.

**`reqrep0`** — `Req0` wraps `Socket<Req0State>`: `dial` + `request(msg) -> Message` (manages request ID transparently). `Rep0` wraps `Socket<Rep0State>`: `listen` + `receive() -> (Message, Responder)`. `Responder` is a one-shot type that enforces exactly one reply per received request.

**`pubsub0`** — `Pub0` holds a `TcpListener` and a `Vec<FramedTransport>`. `wait_for_subscribers(n)` blocks until n handshakes complete. `publish(msg)` first calls `drain_incoming` (a non-blocking accept loop using `biased` select + `std::future::ready`) to pick up any new connections, then fans out to all live subscribers. `Sub0` holds one transport + a `Sub0State`; `next()` loops discarding non-matching messages.

**`pipeline0`** — `Push0` and `Pull0` are thin newtype wrappers over `Socket<_>` with `push` / `pull` methods. No protocol headers involved.

**`pair0`** — `Pair0` wraps `Socket<Pair0State>` with `send` and `recv`. Fully bidirectional.

**`survey0`** — `Surveyor0` holds a listener + `Vec<FramedTransport>` + `Surveyor0State`. `survey(msg, timeout)` fans out to all respondents then collects one reply per active respondent within the shared deadline. `Respondent0` holds one transport + `Respondent0State`; `receive()` returns `(Message, SurveyHandle)` where `SurveyHandle::respond(msg)` sends the reply.

**`bus0`** — `Bus0` holds a `Vec<FramedTransport>`. `broadcast(msg)` sends to all, pruning dead connections. `recv_any()` polls each peer in round-robin using a non-blocking `biased` select, yielding between complete passes. `recv_from(i)` receives from a specific peer by index.

---

## Test layout

```
tests/
  message.rs    — Message prepend/append/trim/clone (14 tests)
  codec.rs      — handshake and frame encode/decode (13 tests)
  protocols.rs  — all six state machines (15 tests)
  transport.rs  — FramedTransport over loopback, including large messages (5 tests)
  req-rep.rs    — single roundtrip, 100 roundtrips, 50 KB message (3 tests)
  pubsub.rs     — single subscriber, topic filtering, non-matching skip (3 tests)
  pipeline.rs   — push-pull 10 messages, both listen/dial directions (2 tests)
  pair.rs       — echo, independent bidirectional (2 tests)
  survey.rs     — single respondent, 3 respondents, timeout (3 tests)
  bus.rs        — broadcast to 2 peers, bidirectional 2-node (2 tests)
```
