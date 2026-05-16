# WebSocket Transport Implementation Plan for nng-core

Goal: implement `ws://` (and later `wss://`) as a first-class transport, achieving
wire-level interoperability with C NNG's WebSocket transport across all six SP
protocol families.

---

## Background

### How SP-over-WebSocket differs from SP-over-TCP

| Concern | TCP transport | WebSocket transport |
|---|---|---|
| Protocol identification | 8-byte SP handshake | `Sec-WebSocket-Protocol` HTTP header |
| Message framing | 8-byte u64 BE length prefix + payload | WebSocket message (WS handles framing internally) |
| SP protocol headers (REQ IDs, etc.) | Stripped by protocol state machines | Identical — state machines are unchanged |

The SP-over-WebSocket RFC (sp-websocket-mapping-01) specifies:
- Standard WebSocket handshake per RFC 6455, with `Sec-WebSocket-Protocol` identifying the SP protocol
- Each SP message maps 1:1 to one WebSocket message; WebSocket frame boundaries are invisible to the SP layer
- Binary frames MUST be used (text frames require valid UTF-8, which SP payloads are not guaranteed to be)

There is **no** 8-byte SP handshake and **no** per-message length prefix over WebSocket.
The 8-byte SP handshake is replaced entirely by the HTTP upgrade headers.

### `Sec-WebSocket-Protocol` convention (from NNG source)

Both sides use the **server's (listener's)** protocol short name, suffixed with
`.sp.nanomsg.org`.  Concretely, from `src/sp/transport/ws/websocket.c`:

```c
// Dialer: sends the PEER protocol's name
snprintf(name, sizeof(name), "%s.sp.nanomsg.org", nni_sock_peer_name(s));

// Listener: sends its OWN protocol's name
snprintf(name, sizeof(name), "%s.sp.nanomsg.org", nni_sock_proto_name(s));
```

Since the dialer's peer is the listener, both sides arrive at the same string.

Full protocol name table:

| ProtocolId | Short name | `Sec-WebSocket-Protocol` |
|---|---|---|
| REQ0 | `req` | `req.sp.nanomsg.org` |
| REP0 | `rep` | `rep.sp.nanomsg.org` |
| PUB0 | `pub` | `pub.sp.nanomsg.org` |
| SUB0 | `sub` | `sub.sp.nanomsg.org` |
| PUSH0 | `push` | `push.sp.nanomsg.org` |
| PULL0 | `pull` | `pull.sp.nanomsg.org` |
| PAIR0 / PAIR1 | `pair` | `pair.sp.nanomsg.org` |
| SURVEYOR0 | `surveyor` | `surveyor.sp.nanomsg.org` |
| RESPONDENT0 | `respondent` | `respondent.sp.nanomsg.org` |
| BUS0 | `bus` | `bus.sp.nanomsg.org` |

A REQ dialer connects to a REP listener.
- REQ dialer sends: `rep.sp.nanomsg.org` (its peer's name)
- REP listener expects: `rep.sp.nanomsg.org` (its own name)

---

## Recommended implementation order

If you are implementing this incrementally ("by hand"), do the steps in this
order — each step is independently testable:

1. Step 2 — codec names (~30 min, self-contained, no I/O)
2. Step 3a + 3c + 3d — dial + send + recv (test against `websocat` immediately)
3. Step 3b — accept side (enables the loopback unit test)
4. Step 1 + 4 — feature flag wiring (mechanical)
5. Step 5a — add `ws_dial`/`ws_listen` methods to each socket (end-to-end working)
6. Step 5b — unify under URL-scheme dispatch (clean-up refactor)
7. Step 6 — tests
8. Step 7 — TLS (`wss://`)

---

## Step 1 — Feature flag and dependency (`Cargo.toml`)

Add a `ws` feature that implies `std` and pulls in `tokio-tungstenite`.

```toml
[features]
default = ["std"]
std = ["dep:tokio", "embedded-io-async/std"]
ws  = ["std", "dep:tokio-tungstenite", "dep:futures-util"]

[dependencies]
# existing deps …
tokio-tungstenite = { version = "0.24", optional = true }
futures-util      = { version = "0.3",  optional = true }
```

Notes:
- `tokio-tungstenite` transitively brings in `tungstenite` (the synchronous
  WebSocket core) and `http`.
- `futures-util` is needed for `StreamExt::next` and `SinkExt::send`.
  `tokio-tungstenite` already depends on it, but it must be a direct dependency
  for us to use its traits directly in our code.
- Do not enable `tokio-tungstenite`'s TLS features yet — that is Step 7.

---

## Step 2 — Protocol name mapping (`src/codec.rs`)

Add two methods to `ProtocolId`. Place them alongside `expected_peer`.

```rust
/// Short name used in the `Sec-WebSocket-Protocol` header.
///
/// This is the lowercase ASCII name NNG assigns to each protocol in its
/// `proto_self` / `proto_peer` descriptors (e.g. `"req"`, `"rep"`).
pub fn ws_protocol_name(self) -> &'static str {
    match self {
        Self::REQ0        => "req",
        Self::REP0        => "rep",
        Self::PUB0        => "pub",
        Self::SUB0        => "sub",
        Self::PUSH0       => "push",
        Self::PULL0       => "pull",
        Self::PAIR0 |
        Self::PAIR1       => "pair",
        Self::SURVEYOR0   => "surveyor",
        Self::RESPONDENT0 => "respondent",
        Self::BUS0        => "bus",
        _                 => "unknown",
    }
}

/// Full `Sec-WebSocket-Protocol` header value for this protocol.
///
/// Both the dialer and the listener use the *listener's* protocol name:
/// - Listener sets: `self.ws_subprotocol()`
/// - Dialer   sets: `self.expected_peer().ws_subprotocol()`
///
/// Example: a REQ dialer calls `ProtocolId::REQ0.expected_peer().ws_subprotocol()`
/// which yields `"rep.sp.nanomsg.org"`, matching what the REP listener expects.
#[cfg(feature = "ws")]
pub fn ws_subprotocol(self) -> String {
    format!("{}.sp.nanomsg.org", self.ws_protocol_name())
}
```

The `ws_subprotocol` method is gated on the `ws` feature because it allocates a
`String` and is only used by the WebSocket transport code. `ws_protocol_name`
is not gated — it's a `&'static str` match with no allocation and might be
useful for diagnostics in any build.

### Tests for Step 2

Add a small unit test in `codec.rs` (or `tests/codec.rs`) verifying the table:

```rust
#[test]
#[cfg(feature = "ws")]
fn ws_subprotocol_values() {
    use crate::codec::ProtocolId;
    assert_eq!(ProtocolId::REQ0.expected_peer().ws_subprotocol(), "rep.sp.nanomsg.org");
    assert_eq!(ProtocolId::REP0.ws_subprotocol(), "rep.sp.nanomsg.org");
    assert_eq!(ProtocolId::PUB0.ws_subprotocol(), "pub.sp.nanomsg.org");
    assert_eq!(ProtocolId::SUB0.expected_peer().ws_subprotocol(), "sub.sp.nanomsg.org");
    assert_eq!(ProtocolId::SURVEYOR0.expected_peer().ws_subprotocol(), "respondent.sp.nanomsg.org");
}
```

---

## Step 3 — Implement `WsTransport` (`src/transport/ws.rs`)

This is the main implementation work. Create a new file `src/transport/ws.rs`.

### 3a — Struct definition and imports

```rust
//! WebSocket SP transport.
//!
//! Implements the SP-over-WebSocket mapping from sp-websocket-mapping-01:
//! protocol identification via `Sec-WebSocket-Protocol`, one SP message per
//! WebSocket binary message, no SP handshake bytes on the wire.

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message as WsMessage,
        handshake::server::{Request as WsRequest, Response as WsResponse},
        http::{HeaderValue, StatusCode},
    },
};

use crate::{Message, codec::ProtocolId};

/// WebSocket transport for the SP protocol.
///
/// Wraps a `WebSocketStream` and maps SP messages to WebSocket binary frames.
/// There is no length prefix: WebSocket itself provides message framing.
pub struct WsTransport<S> {
    inner: WebSocketStream<S>,
}
```

Making `WsTransport` generic over `S` (the underlying stream type) avoids
needing separate structs for the dialer (`MaybeTlsStream<TcpStream>`) and the
acceptor (`TcpStream`). Both implement the `tokio::io::AsyncRead + AsyncWrite`
bounds that `WebSocketStream` requires.

### 3b — Dial (client / dialer)

```rust
impl WsTransport<tokio_tungstenite::MaybeTlsStream<TcpStream>> {
    /// Open a WebSocket connection to `url` as the given `local_proto`.
    ///
    /// The dialer sends its *peer's* subprotocol name in the HTTP upgrade
    /// request and validates that the server echoes the same value back.
    pub async fn connect(url: &str, local_proto: ProtocolId) -> Result<Self, WsError> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;

        let peer_subproto = local_proto.expected_peer().ws_subprotocol();

        let mut request = url.into_client_request()?;
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_str(&peer_subproto)
                .map_err(|e| WsError::InvalidHeader(e.to_string()))?,
        );

        let (ws, response) = tokio_tungstenite::connect_async(request).await?;

        // Server must echo the exact subprotocol string we requested.
        let server_proto = response
            .headers()
            .get("Sec-WebSocket-Protocol")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if server_proto != peer_subproto {
            return Err(WsError::SubprotocolMismatch {
                expected: peer_subproto,
                got: server_proto.to_owned(),
            });
        }

        Ok(Self { inner: ws })
    }
}
```

### 3c — Accept (server / listener)

The server side accepts a raw `TcpStream` from `TcpListener::accept()` and
performs the WebSocket HTTP upgrade, inspecting and echoing the subprotocol
header from the HTTP handshake callback.

```rust
impl WsTransport<TcpStream> {
    /// Upgrade an accepted TCP connection to a WebSocket SP transport.
    ///
    /// Validates that the client's `Sec-WebSocket-Protocol` matches the
    /// listener's own protocol.  Rejects with HTTP 400 on mismatch (the
    /// RFC specifies close code 1002, but that requires a fully established
    /// WebSocket; HTTP 400 is the correct rejection at the upgrade stage).
    pub async fn accept(tcp: TcpStream, local_proto: ProtocolId) -> Result<Self, WsError> {
        let expected = local_proto.ws_subprotocol();
        let expected_clone = expected.clone();

        // The callback runs synchronously inside the HTTP handshake.
        let callback = move |req: &WsRequest, mut resp: WsResponse| {
            let client_proto = req
                .headers()
                .get("Sec-WebSocket-Protocol")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");

            if client_proto != expected_clone {
                *resp.status_mut() = StatusCode::BAD_REQUEST;
                return Err(resp);
            }

            resp.headers_mut().insert(
                "Sec-WebSocket-Protocol",
                HeaderValue::from_str(&expected_clone).unwrap(),
            );
            Ok(resp)
        };

        let ws = tokio_tungstenite::accept_hdr_async(tcp, callback).await?;
        Ok(Self { inner: ws })
    }
}
```

### 3d — Send

Concatenate the SP message header bytes and body bytes into a single Vec, then
send as one WebSocket binary message.

```rust
impl<S> WsTransport<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    /// Send an SP message as a single WebSocket binary frame.
    ///
    /// The SP protocol header bytes (e.g. the REQ request ID) are prepended to
    /// the body, exactly as `FramedTransport` does on the TCP path.  The
    /// receiver will see all bytes as the message body and the protocol state
    /// machine will strip the header from the front.
    pub async fn send(&mut self, msg: &Message) -> Result<(), WsError> {
        let header = msg.header();
        let body   = msg.body();
        let mut payload = Vec::with_capacity(header.len() + body.len());
        payload.extend_from_slice(header);
        payload.extend_from_slice(body);
        self.inner.send(WsMessage::Binary(payload.into())).await?;
        Ok(())
    }

    /// Receive the next SP message from the WebSocket stream.
    ///
    /// Skips WebSocket control frames (Ping, Pong) and text frames.
    /// `tokio-tungstenite` responds to Ping frames automatically; they still
    /// appear in the stream and must be explicitly skipped here.
    ///
    /// The full WebSocket message payload lands in `msg.body()`.  The protocol
    /// state machine will strip its header bytes from the front of the body,
    /// identical to how the TCP path works.
    ///
    /// # Cancellation safety
    ///
    /// This future is cancellation-safe.  Unlike `FramedTransport::recv` (which
    /// reads the frame header and payload in two separate `read_exact` calls),
    /// `tungstenite` reassembles fragmented WebSocket frames internally before
    /// yielding a message.  Dropping this future between `next()` calls does
    /// not lose partial data — the next call will resume from the same position.
    pub async fn recv(&mut self) -> Result<Message, WsError> {
        loop {
            match self.inner.next().await {
                Some(Ok(WsMessage::Binary(data))) => {
                    let mut msg = Message::new();
                    msg.push_back(&data);
                    return Ok(msg);
                }
                Some(Ok(WsMessage::Close(_))) | None => {
                    return Err(WsError::Closed);
                }
                Some(Ok(_)) => continue, // Ping, Pong, Text — ignore
                Some(Err(e)) => return Err(WsError::Tungstenite(e)),
            }
        }
    }
}
```

### 3e — Error type

```rust
#[derive(Debug)]
pub enum WsError {
    /// Underlying tungstenite / WebSocket error.
    Tungstenite(tokio_tungstenite::tungstenite::Error),
    /// Server responded with a different `Sec-WebSocket-Protocol` than requested.
    SubprotocolMismatch { expected: String, got: String },
    /// Invalid HTTP header value (should not happen with well-formed protocol names).
    InvalidHeader(String),
    /// The WebSocket connection was closed cleanly.
    Closed,
}

impl From<tokio_tungstenite::tungstenite::Error> for WsError {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::Tungstenite(e)
    }
}

impl core::fmt::Display for WsError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Tungstenite(e) => write!(f, "WebSocket error: {e}"),
            Self::SubprotocolMismatch { expected, got } => {
                write!(f, "WebSocket subprotocol mismatch: expected {expected:?}, got {got:?}")
            }
            Self::InvalidHeader(s) => write!(f, "invalid WebSocket header value: {s}"),
            Self::Closed => write!(f, "WebSocket connection closed"),
        }
    }
}
```

---

## Step 4 — Wire up the module (`src/transport.rs`)

Add the `ws` submodule and re-export its public types.

```rust
#[cfg(feature = "ws")]
pub mod ws;
#[cfg(feature = "ws")]
pub use ws::{WsTransport, WsError};
```

---

## Step 5 — Socket layer integration (`src/socket.rs`)

This is the largest refactor. There are two sub-phases.

### 5a — Add `ws_dial` / `ws_listen` methods (no refactor, immediately usable)

As a first pass, add dedicated WebSocket constructors alongside the existing
`dial` / `listen` methods on each socket type.  This avoids touching the
existing `AnyStream` / `AnyListener` infrastructure.

Example for `Req0`:

```rust
#[cfg(feature = "ws")]
impl Req0 {
    pub async fn ws_dial(url: &str) -> Result<Self, WsError> {
        use crate::transport::ws::WsTransport;
        let transport = WsTransport::connect(url, ProtocolId::REQ0).await?;
        Ok(Self { transport: AnyTransport::Ws(transport) })
    }
}
```

Repeat for every socket type: `Rep0`, `Pub0`, `Sub0`, `Push0`, `Pull0`,
`Pair0`, `Surveyor0`, `Respondent0`, `Bus0`.  The listener side follows the
same pattern but creates a `TcpListener`, accepts a connection, and calls
`WsTransport::accept`.

### 5b — Unified URL dispatch (refactor, after 5a works)

Replace `AnyStream` / `AnyListener` with an `AnyTransport` enum so that all
socket types can use a single `dial` / `listen` entry point for both TCP and
WebSocket URLs.

```rust
/// Unified transport handle, covering all supported URL schemes.
enum AnyTransport {
    /// TCP or IPC, using the SP 8-byte handshake + length-prefix framing.
    Framed(FramedTransport<AnyStream>),
    /// WebSocket, using HTTP upgrade + per-message WebSocket framing.
    #[cfg(feature = "ws")]
    Ws(WsTransport</* concrete stream type */>),
}

impl AnyTransport {
    async fn send(&mut self, msg: &Message) -> io::Result<()> {
        match self {
            Self::Framed(t) => t.send(msg).await.map_err(io::Error::other),
            #[cfg(feature = "ws")]
            Self::Ws(t)     => t.send(msg).await.map_err(io::Error::other),
        }
    }

    async fn recv(&mut self) -> io::Result<Message> {
        match self {
            Self::Framed(t) => t.recv().await.map_err(io::Error::other),
            #[cfg(feature = "ws")]
            Self::Ws(t)     => t.recv().await.map_err(io::Error::other),
        }
    }
}
```

Update `bind_listener` and the connect helper to detect the `ws://` scheme:

```rust
pub(crate) async fn connect_transport(
    addr: &str,
    proto: ProtocolId,
    format: FrameFormat,
) -> io::Result<AnyTransport> {
    if let Some(_) = addr.strip_prefix("ws://") {
        #[cfg(feature = "ws")]
        {
            let t = WsTransport::connect(addr, proto)
                .await
                .map_err(io::Error::other)?;
            return Ok(AnyTransport::Ws(t));
        }
        #[cfg(not(feature = "ws"))]
        return Err(io::Error::other("ws:// requires the `ws` feature"));
    }
    // existing TCP / IPC dispatch …
    let stream = connect_stream(addr).await?;
    let framed = connect_framed(stream, proto, format).await?;
    Ok(AnyTransport::Framed(framed))
}
```

The `WsTransport` generic parameter is a challenge here: the dialer produces
`WsTransport<MaybeTlsStream<TcpStream>>` and the acceptor produces
`WsTransport<TcpStream>`.  The simplest resolution is to box the inner
`WebSocketStream` with a trait object, or to define an `AnyWsTransport` enum
internally.  A concrete recommendation:

```rust
// In transport/ws.rs — erase the stream type with a boxed trait object.
use tokio::io::{AsyncRead, AsyncWrite};

pub struct WsTransport {
    inner: WebSocketStream<Box<dyn AsyncReadWrite>>,
}

trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncReadWrite for T {}
```

This adds one heap allocation per connection (the box), which is negligible
compared to the TCP connection setup cost.

---

## Step 6 — Tests

### Unit test: loopback (`tests/ws_transport.rs`)

```rust
#[cfg(feature = "ws")]
#[tokio::test]
async fn ws_transport_loopback() {
    use nng_core::{Message, codec::ProtocolId, transport::ws::WsTransport};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("ws://127.0.0.1:{}", listener.local_addr().unwrap().port());

    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut transport = WsTransport::accept(tcp, ProtocolId::REP0).await.unwrap();
        let msg = transport.recv().await.unwrap();
        transport.send(&msg).await.unwrap(); // echo
    });

    let mut client = WsTransport::connect(&addr, ProtocolId::REQ0).await.unwrap();
    let mut req = Message::new();
    req.push_back(b"hello websocket");
    client.send(&req).await.unwrap();
    let reply = client.recv().await.unwrap();
    assert_eq!(reply.body(), b"hello websocket");

    server.await.unwrap();
}
```

### Interop smoke test: `websocat` (`tests/interop_ws.rs`)

[`websocat`](https://github.com/vi/websocat) is a CLI WebSocket tool analogous
to `nngcat`.  It supports setting the `Sec-WebSocket-Protocol` header, which
makes it suitable for a smoke test.

```rust
// Example: nng-core REP server, websocat as client
// websocat --protocol rep.sp.nanomsg.org ws://127.0.0.1:PORT
```

Gate these tests on `WEBSOCAT` in PATH, similar to how `interop_nngcat.rs`
gates on `nngcat`.

### Interop against C NNG (`INTEROP_PLAN.md` extension)

Add a `ws` column to the coverage matrix in `INTEROP_PLAN.md` once the
`nng-sys` infrastructure is in place.  The WS interop tests follow the same
structure as the TCP tests but use `ws://` URLs on both sides.

---

## Step 7 — TLS (`wss://`, defer until Steps 1–6 are complete)

Enable TLS by adding a `wss` Cargo feature:

```toml
[features]
wss = ["ws", "tokio-tungstenite/native-tls"]
# or: wss = ["ws", "tokio-tungstenite/rustls-tls-native-roots"]
```

On the **dialer** side, `tokio_tungstenite::connect_async` already handles
`wss://` URLs automatically when a TLS feature is enabled.  No code change
needed in `WsTransport::connect`.

On the **listener** side, wrap the accepted `TcpStream` in a `TlsAcceptor`
before passing it to `WsTransport::accept`.  This requires a TLS certificate
and private key, which the caller must supply.  A reasonable API:

```rust
#[cfg(feature = "wss")]
impl WsTransport {
    pub async fn accept_tls(
        tcp: TcpStream,
        local_proto: ProtocolId,
        tls: tokio_native_tls::TlsAcceptor, // or rustls equivalent
    ) -> Result<Self, WsError> {
        let tls_stream = tls.accept(tcp).await?;
        // then proceed as in accept() but with tls_stream
    }
}
```

URL scheme dispatch in `connect_transport` checks for `wss://` and routes to
the TLS path; the rest of the socket API is unchanged.

---

## Files created or modified

| File | Change |
|---|---|
| `Cargo.toml` | Add `ws` feature, `tokio-tungstenite`, `futures-util` dependencies |
| `src/codec.rs` | Add `ProtocolId::ws_protocol_name` and `ws_subprotocol` |
| `src/transport/ws.rs` | New file: `WsTransport<S>`, `WsError` |
| `src/transport.rs` | Re-export `ws` module |
| `src/socket.rs` | Add `AnyTransport`, `ws_dial`/`ws_listen` (5a), then URL dispatch (5b) |
| `tests/ws_transport.rs` | New file: loopback unit test |
| `tests/interop_ws.rs` | New file: `websocat` interop smoke test |
| `INTEROP_PLAN.md` | Add `ws` column to coverage matrix |
| `README.md` | Document `ws` feature, URL scheme, new examples |

---

## Non-goals (out of scope for this plan)

- `wss://` (TLS) — Step 7 above, deferred
- UDP transport — separate effort, no RFC overlap with WebSocket
- WebSocket path matching — NNG validates the URL path on the listener side;
  nng-core can start by accepting any path and add path validation later
- HTTP/2 WebSocket upgrades — not used by any NNG implementation
- Per-message text-frame mode — the `NN_WS_MSG_TYPE` option in C NNG;
  not needed for SP interop since binary is always preferred
