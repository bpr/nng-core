# Using nng-core with Embassy

Embassy is the natural fit for `nng-core` on bare-metal targets. Both crates
build on `embedded-io-async`: Embassy's TCP stack already implements the same
`Read + Write` traits that `FramedTransport<T>` is parameterized over, so the
core of `nng-core` requires no adaptation at all.

---

## What works without modification

The bottom three layers of `nng-core` compile in `no_std` + `alloc` mode and
have no runtime dependency:

| Layer | Files | Status |
|---|---|---|
| Message type | `message.rs` | Works as-is |
| Wire codec | `codec.rs` | Works as-is |
| Protocol state machines | `protocols/` | Works as-is |
| Framed transport core | `transport.rs` | Works as-is |

`embassy-net::tcp::TcpSocket` implements `embedded_io_async::Read` and
`Write` directly, so `FramedTransport<TcpSocket<'_>>` compiles without any
adapter layer. This is not a coincidence — `embedded-io-async` was chosen
precisely because it is the shared I/O abstraction between Embassy and other
embedded runtimes.

The `socket.rs` that ships with `nng-core` is tokio-specific and is not used
in an Embassy build. You work with `FramedTransport` and the state machines
directly, or write a thin Embassy socket layer (see
[Writing a socket layer](#writing-an-embassy-socket-layer)).

---

## Setup

### `Cargo.toml`

Disable the `std` feature so the tokio-dependent code is excluded:

```toml
[dependencies]
nng-core = { path = "…/nng-core", default-features = false }

embassy-executor  = { version = "0.6", features = ["arch-cortex-m", "executor-thread"] }
embassy-net       = { version = "0.4", features = ["tcp", "dhcpv4"] }
embassy-time      = { version = "0.3" }
embassy-futures   = { version = "0.1" }
embedded-io-async = { version = "0.7" }

# nng-core's Message type uses Vec<u8> and needs a heap allocator
embedded-alloc    = { version = "0.6" }
```

### Heap allocator

`embassy-net` itself is heapless, but `nng-core`'s `Message` type uses
`Vec<u8>` internally. A global allocator must be initialized before any
`Message` is constructed:

```rust
use embedded_alloc::LlffHeap as Heap;

#[global_allocator]
static HEAP: Heap = Heap::empty();

const HEAP_SIZE: usize = 32 * 1024; // tune to your target's available RAM
static mut HEAP_MEM: [u8; HEAP_SIZE] = [0; HEAP_SIZE];

#[embassy_executor::main]
async fn main(spawner: Spawner) {
    // Must happen before any nng-core code runs.
    unsafe { HEAP.init(HEAP_MEM.as_ptr() as usize, HEAP_SIZE) }

    // … initialize peripherals, network stack, spawn tasks …
}
```

---

## Using FramedTransport and state machines directly

Rather than going through a high-level socket API, Embassy code typically
works with `FramedTransport` and the protocol state machines directly. This
matches Embassy's style: tasks are static, buffers are explicit, and
allocations are intentional.

### REQ side

```rust
use embassy_net::{Stack, tcp::TcpSocket};
use nng_core::{
    Message,
    codec::ProtocolId,
    protocols::reqrep::Req0State,
    transport::FramedTransport,
};

#[embassy_executor::task]
async fn req_task(stack: &'static Stack<impl Driver + 'static>) {
    let mut rx = [0u8; 1024];
    let mut tx = [0u8; 1024];
    let mut socket = TcpSocket::new(stack, &mut rx, &mut tx);

    socket.connect((Ipv4Address::new(192, 168, 1, 1), 5555))
        .await
        .expect("connect failed");

    // TcpSocket satisfies FramedTransport's Read + Write bound directly.
    let mut transport = FramedTransport::connect(socket, ProtocolId::REQ0)
        .await
        .expect("handshake failed");

    let mut state = Req0State::new();

    // Build and send a request.
    let mut msg = Message::new();
    msg.push_back(b"hello");
    let request_id = state.prepare_outgoing(&mut msg);
    transport.send(&msg).await.expect("send failed");

    // Receive and validate the reply.
    let mut reply = transport.recv().await.expect("recv failed");
    state.process_incoming(&mut reply, request_id).expect("bad reply");

    // reply.body() is now the application payload.
    defmt::info!("reply: {:?}", reply.body());
}
```

### REP side

```rust
use embassy_net::tcp::{IpListenEndpoint, TcpSocket};
use nng_core::{
    Message,
    codec::ProtocolId,
    protocols::reqrep::Rep0State,
    transport::FramedTransport,
};

#[embassy_executor::task]
async fn rep_task(stack: &'static Stack<impl Driver + 'static>) {
    let mut rx = [0u8; 1024];
    let mut tx = [0u8; 1024];
    let mut socket = TcpSocket::new(stack, &mut rx, &mut tx);

    socket.accept(IpListenEndpoint { addr: None, port: 5555 })
        .await
        .expect("accept failed");

    let mut transport = FramedTransport::connect(socket, ProtocolId::REP0)
        .await
        .expect("handshake failed");

    let mut state = Rep0State::new();

    loop {
        // Receive a request; strip the routing header.
        let mut msg = transport.recv().await.expect("recv failed");
        let routing = state.process_incoming(&mut msg).expect("bad request");

        // msg.body() is now the application payload.
        defmt::info!("request: {:?}", msg.body());

        // Build and send the reply; attach the routing header.
        let mut reply = Message::new();
        reply.push_back(b"world");
        state.prepare_reply(&mut reply, &routing);
        transport.send(&reply).await.expect("send failed");
    }
}
```

### PUB/SUB

PUB and SUB have no protocol headers; they only differ in how the subscriber
filters messages. Use `Sub0State::matches` to decide whether to pass each
received message to the application:

```rust
use nng_core::{
    Message,
    codec::ProtocolId,
    protocols::pubsub::Sub0State,
    transport::FramedTransport,
};

#[embassy_executor::task]
async fn sub_task(stack: &'static Stack<impl Driver + 'static>) {
    let mut rx = [0u8; 2048];
    let mut tx = [0u8; 256];
    let mut socket = TcpSocket::new(stack, &mut rx, &mut tx);
    socket.connect((Ipv4Address::new(192, 168, 1, 1), 5556)).await.unwrap();

    let mut transport = FramedTransport::connect(socket, ProtocolId::SUB0)
        .await.unwrap();

    let mut state = Sub0State::new();
    state.subscribe(b"sensors:"); // receive only "sensors:…" messages

    loop {
        let msg = transport.recv().await.unwrap();
        if state.matches(&msg) {
            defmt::info!("received: {:?}", msg.body());
        }
        // Non-matching messages are silently dropped.
    }
}
```

### Timeouts with `embassy_time`

`tokio::time::timeout` is replaced by `embassy_time::with_timeout`. Note that
`embassy_time::Duration` is a distinct type from `core::time::Duration`:

```rust
use embassy_time::{Duration, with_timeout};

// Wait up to 500 ms for the next message.
match with_timeout(Duration::from_millis(500), transport.recv()).await {
    Ok(Ok(msg))      => { /* got a message */ }
    Ok(Err(e))       => { /* transport error */ }
    Err(_timed_out)  => { /* deadline exceeded */ }
}
```

### Selecting across two futures

`embassy_futures::select::select` replaces `tokio::select!`. It is a
function (not a macro) and works in `no_std`:

```rust
use embassy_futures::select::{select, Either};

match select(transport.recv(), cancel_signal.wait()).await {
    Either::First(Ok(msg)) => { /* message arrived */ }
    Either::First(Err(e))  => { /* transport error */ }
    Either::Second(_)      => { /* cancelled */ }
}
```

---

## Multiple simultaneous connections

Embassy tasks are statically allocated. Each connection gets its own task
with its own stack-allocated buffers. Use `pool_size` to reserve space for
`N` concurrent instances of the same task:

```rust
// Up to 4 REP connections may exist simultaneously.
#[embassy_executor::task(pool_size = 4)]
async fn rep_connection(stack: &'static Stack<impl Driver + 'static>) {
    let mut rx = [0u8; 1024];
    let mut tx = [0u8; 1024];
    let mut socket = TcpSocket::new(stack, &mut rx, &mut tx);
    socket.accept(IpListenEndpoint { addr: None, port: 5555 }).await.unwrap();
    // … handle the connection …
}

// Accept loop spawns a new instance for each incoming connection.
#[embassy_executor::task]
async fn accept_loop(
    spawner: Spawner,
    stack: &'static Stack<impl Driver + 'static>,
) {
    loop {
        spawner.spawn(rep_connection(stack)).unwrap();
        // rep_connection will call accept() itself; give it time to do so
        // before the next spawn races to the same port.
        Timer::after(Duration::from_millis(10)).await;
    }
}
```

Because each task holds its own `TcpSocket` with independent buffers, there
is no shared-handle or `Mutex` needed — the socket ownership model matches
Embassy's static task model exactly.

---

## Writing an Embassy socket layer

If you want a higher-level API comparable to `socket.rs`, the mapping from
tokio to Embassy is mechanical:

| `socket.rs` (tokio) | Embassy equivalent |
|---|---|
| `TcpStream::connect(addr).await` | `TcpSocket::connect(endpoint).await` |
| `TcpListener::bind(addr).await` + `.accept().await` | `TcpSocket::accept(IpListenEndpoint { addr: None, port }).await` |
| `tokio::time::timeout(dur, f).await` | `embassy_time::with_timeout(Duration::from_…, f).await` |
| `tokio::select! { biased; f => …, _ = ready(()) => … }` | `embassy_futures::select::select(f, ready_future)` |
| `tokio::task::yield_now().await` | `embassy_time::Timer::after(Duration::from_ticks(0)).await` |

The logic inside each method — handshake, framing, state machine calls — is
identical. Only the TCP and timer primitives change.

A sketch of what `Req0::dial` would look like in an Embassy socket layer:

```rust
pub struct Req0<'d, D: Driver> {
    transport: FramedTransport<TcpSocket<'d>>,
    state: Req0State,
    _driver: PhantomData<D>,
}

impl<'d, D: Driver> Req0<'d, D> {
    pub async fn connect(
        socket: TcpSocket<'d>,   // caller provides the socket + buffers
        endpoint: impl Into<IpEndpoint>,
    ) -> Result<Self, ConnectError> {
        let mut socket = socket;
        socket.connect(endpoint.into()).await?;
        let transport = FramedTransport::connect(socket, ProtocolId::REQ0)
            .await
            .map_err(|_| ConnectError::HandshakeFailed)?;
        Ok(Self { transport, state: Req0State::new(), _driver: PhantomData })
    }

    pub async fn request(&mut self, msg: Message) -> Result<Message, TransportError> {
        let mut outgoing = msg;
        let id = self.state.prepare_outgoing(&mut outgoing);
        self.transport.send(&outgoing).await?;
        let mut reply = self.transport.recv().await?;
        self.state.process_incoming(&mut reply, id).map_err(|_| TransportError::Io)?;
        Ok(reply)
    }
}
```

Note that the caller owns the `TcpSocket` and its buffers rather than the
socket layer constructing them internally. This is the Embassy convention:
buffers are always visible in the caller's stack frame so the linker can
account for their memory at compile time.

---

## Key differences from the tokio socket layer

| Concern | tokio (`socket.rs`) | Embassy |
|---|---|---|
| TCP type | `tokio::net::TcpStream` (heap) | `TcpSocket<'d>` (stack buffers, explicit lifetime) |
| Listener | `TcpListener` holds the OS socket | Each `TcpSocket` accepts one connection then is reused |
| Concurrency | `tokio::spawn` + `Arc<Mutex<…>>` | `pool_size` tasks, each owning its socket |
| Timeout | `tokio::time::timeout` | `embassy_time::with_timeout` |
| Select | `tokio::select!` (macro) | `embassy_futures::select::select` (function) |
| Yield | `tokio::task::yield_now()` | `Timer::after(Duration::from_ticks(0)).await` |
| Heap | Implicit (standard allocator) | Explicit (`embedded-alloc`, sized at link time) |
| Entry point | `#[tokio::main]` | `#[embassy_executor::main]` |

---

## Buffer sizing

`TcpSocket` requires separate receive and transmit byte arrays. Size them to
at least the largest single message you expect to send or receive, plus the
8-byte SP frame length prefix, plus headroom for TCP retransmit buffers:

```rust
// For messages up to 512 bytes of payload:
let mut rx = [0u8; 1024]; // 2× payload is a reasonable default
let mut tx = [0u8; 1024];
let mut socket = TcpSocket::new(stack, &mut rx, &mut tx);
```

If a message arrives that is larger than `rx`, the TCP stack will drop bytes
and `FramedTransport::recv` will return a framing error. Size conservatively
for your target's RAM budget.
