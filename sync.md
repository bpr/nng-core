# Using nng-pure from synchronous code

`nng-pure` is async-only: every socket operation is an `async fn`. The `nng`
crate has a blocking synchronous API. This document explains how to call
`nng-pure` from a `fn main()` or other sync context, covers the API
differences protocol by protocol, and explains the runtime constraints.

---

## Driving async code from a sync entry point

There are two patterns. Both require tokio; see [Runtime portability](#runtime-portability) below for why.

### Option A — `Runtime::block_on`

Create a runtime once, then call `rt.block_on(...)` for each top-level async
function. The structure of `main` stays mostly the same; only the helper
functions become `async fn`.

```rust
fn main() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let args: Vec<_> = std::env::args().collect();
    match args[1].as_str() {
        "req" => rt.block_on(request(&args[2])),
        "rep" => rt.block_on(reply(&args[2])),
        _     => eprintln!("Usage: reqrep req|rep <URL>"),
    }
}

async fn request(url: &str) { /* ... */ }
async fn reply(url: &str)   { /* ... */ }
```

### Option B — `#[tokio::main]`

Turn `main` itself into `async fn` and let the attribute macro set up the
runtime. This is the cleanest option when there is no reason to keep `main`
synchronous.

```rust
#[tokio::main]
async fn main() {
    match args[1].as_str() {
        "req" => request(&args[2]).await,
        "rep" => reply(&args[2]).await,
        _     => eprintln!("Usage: reqrep req|rep <URL>"),
    }
}
```

The async helper functions are identical in both options.

---

## API mapping

The table below shows the `nng` call on the left and its `nng-pure`
equivalent on the right. Construction and connection are combined into a
single constructor in `nng-pure` so there is no separate `dial`/`listen`
step.

### REQ/REP

| `nng` | `nng-pure` |
|---|---|
| `Socket::new(Protocol::Req0)?` + `s.dial(url)?` | `Req0::dial(url).await?` |
| `s.send(bytes)?` + `s.recv()?` | `req.request(msg).await?` |
| `Socket::new(Protocol::Rep0)?` + `s.listen(url)?` | `Rep0::listen(url).await?` |
| `s.recv()?` | `let (msg, responder) = rep.receive().await?` |
| `s.send(reply)?` | `responder.reply(reply).await?` |

`Responder` is a one-shot type consumed by `reply()`, enforcing the
one-response-per-request invariant at compile time. In `nng` this is a
runtime check.

### PUB/SUB

| `nng` | `nng-pure` |
|---|---|
| `Socket::new(Protocol::Pub0)?` + `s.listen(url)?` | `Pub0::listen(url).await?` |
| `s.pipe_notify(move \|_, ev\| { count... })?` | `pub0.wait_for_subscribers(n).await?` or `pub0.subscriber_count()` |
| `s.send(bytes)?` | `pub0.publish(msg).await?` |
| `Socket::new(Protocol::Sub0)?` + `s.dial(url)?` | `Sub0::dial(url).await?` |
| `s.set_opt::<Subscribe>(topics)?` | `sub.subscribe_to(b"prefix")` |
| `s.recv()?` | `sub.next().await?` |

`nng`'s `pipe_notify` callback fires whenever a subscriber connects or
disconnects, making it straightforward to count live subscribers. `nng-pure`
has no callback mechanism. Use `wait_for_subscribers(n)` to block until `n`
connections exist, or call `subscriber_count()` after publishing to see how
many connections are still alive. Failed sends are pruned automatically.

### PUSH/PULL

| `nng` | `nng-pure` |
|---|---|
| `Socket::new(Protocol::Push0)?` + `s.listen/dial(url)?` | `Push0::listen(url).await?` / `Push0::dial(url).await?` |
| `s.send(bytes)?` | `push.push(msg).await?` |
| `Socket::new(Protocol::Pull0)?` + `s.listen/dial(url)?` | `Pull0::listen(url).await?` / `Pull0::dial(url).await?` |
| `s.recv()?` | `pull.pull().await?` |

### PAIR

| `nng` | `nng-pure` |
|---|---|
| `Socket::new(Protocol::Pair0)?` + `s.listen/dial(url)?` | `Pair0::listen(url).await?` / `Pair0::dial(url).await?` |
| `s.send(msg)?` | `pair.send(msg).await?` |
| `s.recv()?` | `pair.recv().await?` |
| `s.set_opt::<RecvTimeout>(Some(dur))?` | `tokio::time::timeout(dur, pair.recv()).await` |

`nng` attaches a timeout to the socket with `set_opt` and then every `recv`
respects it. In `nng-pure` you wrap individual futures with
`tokio::time::timeout`:

```rust
// nng
s.set_opt::<RecvTimeout>(Some(Duration::from_millis(100)))?;
match s.recv() {
    Ok(m)             => { /* got message */ }
    Err(Error::TimedOut) => { /* timed out */ }
    Err(e)            => return Err(e),
}

// nng-pure
match tokio::time::timeout(Duration::from_millis(100), pair.recv()).await {
    Ok(Ok(msg))   => { /* got message */ }
    Err(_elapsed) => { /* timed out */ }
    Ok(Err(e))    => return Err(e),
}
```

### SURVEYOR/RESPONDENT

| `nng` | `nng-pure` |
|---|---|
| `Socket::new(Protocol::Surveyor0)?` + `s.listen(url)?` | `Surveyor0::listen(url).await?` |
| _(wait for respondents with `thread::sleep`)_ | `surveyor.wait_for_respondents(n).await?` |
| `s.send(query)?` + `loop { s.recv() until TimedOut }` | `surveyor.survey(query, timeout).await?` → `Vec<Message>` |
| `Socket::new(Protocol::Respondent0)?` + `s.dial(url)?` | `Respondent0::dial(url).await?` |
| `s.recv()?` | `let (msg, handle) = resp.receive().await?` |
| `s.send(reply)?` | `handle.respond(reply).await?` |

`nng`'s surveyor calls `send` once then loops on `recv` until it gets
`Error::TimedOut`, handling the timeout window manually. `nng-pure` wraps
this entirely in `survey(msg, timeout)` which fans out the question, collects
all responses that arrive before the deadline, and returns them as a
`Vec<Message>`. The `SurveyHandle` from `receive()` plays the same role as
`Responder` in REP: consuming it enforces one response per received survey.

### BUS

| `nng` | `nng-pure` |
|---|---|
| `Socket::new(Protocol::Bus0)?` + `s.listen(url)?` | `Bus0::listen_and_accept(url, n).await?` |
| `s.dial(peer)?` _(multiple calls)_ | `Bus0::dial(url).await?` _(single peer per call)_ |
| `s.send(bytes)?` | `bus.broadcast(msg).await?` |
| `s.recv()?` | `bus.recv_any().await?` / `bus.recv_from(peer_idx).await?` |

`nng`'s bus socket can both listen and dial multiple peers from the same
handle, building a mesh. `nng-pure` currently supports either listening
(accepting `n` inbound connections via `listen_and_accept`) or dialing (one
outbound connection per `Bus0::dial`). To connect a node to two peers you
would call `dial` twice and merge the two `Bus0` instances, or construct the
`Bus0` with both transports manually via `FramedTransport`. True arbitrary
mesh topologies are not yet supported at the socket API level.

---

## Worked example: REQ/REP

Here is the `nng/examples/reqrep.rs` rewritten side-by-side.

**Before (`nng`, synchronous):**

```rust
use nng::{Error, Protocol, Socket};

fn main() -> Result<(), Error> {
    let args: Vec<_> = std::env::args().take(3).collect();
    match &args[..] {
        [_, t, url] if t == "req" => request(url),
        [_, t, url] if t == "rep" => reply(url),
        _ => { eprintln!("Usage: reqrep req|rep <URL>"); Ok(()) }
    }
}

fn request(url: &str) -> Result<(), Error> {
    let s = Socket::new(Protocol::Req0)?;
    s.dial(url)?;
    s.send(DATE_REQUEST.to_le_bytes())?;
    let msg = s.recv()?;
    let epoch = u64::from_le_bytes(msg[..].try_into().unwrap());
    println!("EPOCH WAS {} SECONDS AGO", epoch);
    Ok(())
}

fn reply(url: &str) -> Result<(), Error> {
    let s = Socket::new(Protocol::Rep0)?;
    s.listen(url)?;
    loop {
        let msg = s.recv()?;
        let cmd = u64::from_le_bytes(msg[..].try_into().unwrap());
        if cmd != DATE_REQUEST { continue; }
        let secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        let mut reply = nng::Message::new();
        reply.push_back(&secs.to_le_bytes());
        s.send(reply)?;
    }
}
```

**After (`nng-pure`, async with `#[tokio::main]`):**

```rust
use nng_pure::{Message, socket::reqrep0};

#[tokio::main]
async fn main() {
    let args: Vec<_> = std::env::args().take(3).collect();
    match args[1].as_str() {
        "req" => request(&args[2]).await.unwrap(),
        "rep" => reply(&args[2]).await.unwrap(),
        _     => eprintln!("Usage: reqrep req|rep <URL>"),
    }
}

async fn request(url: &str) -> std::io::Result<()> {
    let mut req = reqrep0::Req0::dial(url).await?;
    let mut msg = Message::new();
    msg.push_back(&DATE_REQUEST.to_le_bytes());
    let reply = req.request(msg).await?;
    let epoch = u64::from_le_bytes(reply.body().try_into().unwrap());
    println!("EPOCH WAS {} SECONDS AGO", epoch);
    Ok(())
}

async fn reply(url: &str) -> std::io::Result<()> {
    let mut rep = reqrep0::Rep0::listen(url).await?;
    loop {
        let (msg, responder) = rep.receive().await?;
        let cmd = u64::from_le_bytes(msg.body().try_into().unwrap());
        if cmd != DATE_REQUEST { continue; }
        let secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        let mut reply = Message::new();
        reply.push_back(&secs.to_le_bytes());
        responder.reply(reply).await?;
    }
}
```

**After (`nng-pure`, async with `Runtime::block_on`):**

```rust
fn main() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let args: Vec<_> = std::env::args().take(3).collect();
    match args[1].as_str() {
        "req" => rt.block_on(request(&args[2])).unwrap(),
        "rep" => rt.block_on(reply(&args[2])).unwrap(),
        _     => eprintln!("Usage: reqrep req|rep <URL>"),
    }
    // `request` and `reply` are identical to the #[tokio::main] version above.
}
```

The async helper functions are identical in both options; only `main` differs.

---

## Shared-socket concurrency

`nng::Socket` implements `Clone` and is internally reference-counted, so it
can be passed to multiple threads directly:

```rust
// nng: clone the socket, hand it to a thread
let s2 = s.clone();
thread::spawn(move || { s2.recv().unwrap(); });
s.send(b"hello")?;
```

`nng-pure` sockets take `&mut self`, so they cannot be shared across tasks
without a wrapper. The idiomatic async approach is to give each task its own
socket end and communicate through the protocol itself rather than sharing a
handle. When sharing is genuinely necessary, wrap the socket in
`Arc<tokio::sync::Mutex<_>>`:

```rust
let pair = Arc::new(tokio::sync::Mutex::new(
    pair0::Pair0::dial(url).await?
));
let pair2 = pair.clone();
tokio::spawn(async move {
    let msg = pair2.lock().await.recv().await.unwrap();
});
pair.lock().await.send(msg).await?;
```

---

## Runtime portability

`block_on` itself is not runtime-specific — it simply drives a future to
completion on the current thread. However, **the futures produced by
`nng-pure`'s socket layer depend on tokio** and will not work when driven by
a different runtime's executor.

The socket layer uses:

- `tokio::net::TcpStream` / `TcpListener` — register I/O interest with
  **tokio's reactor**; another runtime's executor cannot service those events
- `tokio::time::timeout` — requires tokio's **time driver**
- `tokio::task::yield_now` — requires a **tokio task context**

Calling `async_std::task::block_on(reqrep0::Req0::dial(...))` will hang or
panic because there is no tokio reactor running to complete the TCP connect.

**Where the architecture is actually runtime-agnostic**

The bottom two layers have no tokio imports. `FramedTransport<T>` is generic
over any `T: embedded_io_async::Read + Write`. To support a second runtime
you only need to replace two things:

1. A TCP adapter — a newtype around your runtime's stream that implements
   `embedded_io_async::Read + Write`:

   ```rust
   // Example: async-std adapter
   pub struct AsyncStdTcp(async_std::net::TcpStream);

   impl embedded_io_async::Read for AsyncStdTcp { ... }
   impl embedded_io_async::Write for AsyncStdTcp { ... }
   ```

2. A socket layer — rewrite `socket.rs` using your runtime's
   `TcpListener::bind`, `TcpStream::connect`, timeout primitive, and yield
   primitive. The codec, state machines, and `FramedTransport` core are
   identical.

The two entry-point patterns (`block_on` and `#[tokio::main]`) map directly
onto each runtime once the socket layer is ported:

| Runtime | Block-on | Attribute |
|---|---|---|
| tokio | `tokio::runtime::Runtime::new().unwrap().block_on(f)` | `#[tokio::main]` |
| async-std | `async_std::task::block_on(f)` | `#[async_std::main]` |
| smol | `smol::block_on(f)` | _(manual)_ |
