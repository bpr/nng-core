# DTLS Transport — Implementation Status

**Status: Aborted / Preserved**

The DTLS transport (`dtls://`) was implemented using the `dimpl` 0.6.1 crate (a
sans-IO DTLS 1.2 state machine).  The code compiles and the tests pass when run
in isolation, but the implementation is intermittently flaky under parallel
execution and has been left in a `#[ignore]`d state pending a deeper fix or a
replacement strategy.

---

## What Was Implemented

### `src/transport/dtls.rs`

A standalone `DtlsTransport` type (not a `FramedTransport<T>` wrapper — DTLS is
datagram-based and does not fit the framed-stream model).  The design mirrors
`UdpTransport` but adds TLS security via `dimpl`.

**Public API:**
- `DtlsTransport::bind(addr, cert)` — server, learns peer from first datagram
- `DtlsTransport::from_server_socket(socket, cert)` — server variant that
  accepts a pre-bound `UdpSocket` (used in tests to avoid TOCTOU races)
- `DtlsTransport::connect(remote, cert)` — client, sends `ClientHello`
  immediately
- `DtlsTransport::send(&mut self, msg)` — sends header+body as one datagram
- `DtlsTransport::recv(&mut self)` — receives one decrypted datagram

**Background task (`run_dtls_task`):**

The transport spawns one `tokio::task` per connection.  That task owns the
`UdpSocket` and the `dimpl::Dtls` state machine, and communicates with the
caller via two bounded `mpsc` channels (capacity 64) plus a `watch` channel for
cancellation.

The task runs a flat `poll_output` → `select!` event loop:

```
loop {
    match dtls.poll_output(&mut out_buf) {
        Output::Packet(p)          => socket.send_to(p, peer).await
        Output::Timeout(deadline)  => select! {
            recv_from               => dtls.handle_packet(...)
            app_rx.recv()          => dtls.send_application_data(...) or buffer
            sleep_until(deadline)  => dtls.handle_timeout(...); yield_now()
            cancel.changed()       => return
        }
        Output::Connected          => flush pending_sends
        Output::ApplicationData(d) => plaintext_tx.send(Ok(d))
        Output::CloseNotify        => plaintext_tx.send(Err(...)); return
        _                          => {}   // PeerCert, KeyingMaterial ignored
    }
}
```

Pre-handshake sends are buffered in `pending_sends` and flushed when
`Output::Connected` fires.

**Certificate model:** Both sides supply a `dimpl::DtlsCertificate` containing
a DER-encoded self-signed certificate and its private key.  The `dimpl` engine
emits `Output::PeerCert` but performs no PKI validation; any certificate is
accepted.

**Socket layer (`src/socket.rs`):**

`AnyTransport::Dtls(DtlsTransport)` was added alongside the existing
`AnyTransport::Udp`.  Each socket type that previously had `listen_udp` /
`dial_udp` gained matching `listen_dtls_socket` / `dial_dtls` constructors
(pair0, pipeline0, reqrep0, pubsub0, survey0, bus0).

---

## Tests (`tests/dtls_transport.rs`)

Five tests, all currently `#[ignore]`d:

| Test | What it exercises |
|------|-------------------|
| `raw_echo_single_message` | One round-trip on raw `DtlsTransport` |
| `raw_echo_many_sequential` | Ten sequential round-trips on raw transport |
| `pair_dtls_bidirectional` | Five bidirectional exchanges via `Pair0` socket |
| `req_rep_dtls` | Single REQ/REP exchange via `Req0` / `Rep0` |
| `push_pull_dtls` | Five pushed messages via `Push0` / `Pull0` |

All five pass when run alone (`--test-threads=1`).  Under parallel execution
(`--test-threads=4`) they are intermittently flaky: tests hang for over 60
seconds before the test harness kills them.

---

## Root Causes Identified

### 1. `dimpl` stale-timeout starvation (primary cause)

**Where:** `run_dtls_task` / `dimpl::Dtls::poll_output` in `engine.rs`

During DTLS handshake flight transitions, `dimpl` calls an internal
`flight_begin()` which sets the flight timer to `Unarmed`.  When
`poll_output` subsequently returns `Output::Timeout(t)`, the deadline `t` is
`last_now` — a timestamp from the *previous* call to `handle_timeout`, not a
future instant.

In the `run_dtls_task` event loop, this stale deadline reaches
`tokio::time::sleep_until(wake)` where `wake` corresponds to a duration of
zero.  `sleep_until` with a past instant fires immediately **without ever
returning `Poll::Pending`**.  On the `current_thread` Tokio runtime (the
default for `#[tokio::test]`), the executor parks the OS thread only when
every queued future returns `Poll::Pending` — that is the moment it calls
`epoll_wait` to collect I/O events.  A hot loop that never returns
`Poll::Pending` therefore starves all other tasks on the same thread,
including the peer's background task, which consequently cannot receive or
transmit DTLS packets.

**Attempted fix:** Added `tokio::task::yield_now().await` inside the
`sleep_until` branch (after `handle_timeout` returns).  `yield_now`
re-queues the current task at the back of the run queue, giving other tasks a
turn.  This reliably fixed the stale-timeout starvation *in isolation*, but
did not fully eliminate the parallel-test flakiness (see §3 below).

### 2. Infinite spin after retransmit exhaustion (fixed)

**Where:** `run_dtls_task` → `dtls.handle_timeout` in `engine.rs`

After the DTLS retransmit limit is exhausted (default: 4 retries, ~31 seconds
total RTO), `handle_timeout` returns `Err(Timeout)`.  The expired deadline is
never updated.  `poll_output` therefore keeps returning `Output::Timeout` with
the same stale instant → `sleep_until(stale)` fires immediately → back to
`poll_output` → repeat.  With no `yield_now` this is a permanent CPU spin
that stalls all other tasks.

**Fix (in place):** The `sleep_until` branch now checks
`handle_timeout(...).is_err()`, sends an error through `plaintext_tx`, and
returns from the task.

### 3. Residual parallel-test flakiness (unfixed)

Even with fixes 1 and 2, the test suite remains intermittently flaky when
four tests run in parallel.  The most likely remaining cause is an interaction
between the `current_thread` executor and the UDP socket readiness model:

- Tokio uses edge-triggered epoll (EPOLLET).  A readiness event is delivered
  once, when the socket transitions from not-ready to ready.  The background
  task must drain all pending datagrams before returning `Poll::Pending`;
  otherwise the edge event is consumed and no further wakeup arrives until
  new data appears.

- In the stale-timeout scenario, the `select!` arm that wins is the
  `sleep_until` arm (not `recv_from`), so `recv_from` is polled once and
  dropped.  If a datagram had already arrived in the kernel buffer, the
  socket is in the "ready" state but the edge event has already been
  delivered.  The next `select!` iteration polls `recv_from` again, which
  may or may not see the datagram depending on Tokio's internal readiness
  cache.

- `yield_now` does not guarantee an epoll poll; it only yields to other
  *application* tasks.  If the peer's background task also holds a socket in
  a ready-but-unread state, `yield_now` + re-poll may still miss the window
  during which both sides need to see each other's handshake packets.

The combination of multiple `yield_now` interleaved across four parallel
test pairs creates a non-deterministic scheduling sequence that occasionally
leads to a handshake stall longer than the test timeout.

---

## Potential Fix Directions

### A. Use `tokio::runtime::Builder::new_multi_thread()` in tests

The stale-timeout starvation problem is specific to `current_thread`.  On a
multi-thread runtime each background task runs on its own OS thread, so a
hot-looping task cannot starve its peer.  Switching the test attribute from
`#[tokio::test]` to a custom runtime:

```rust
#[test]
fn raw_echo_single_message() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { /* ... */ });
}
```

This sidesteps the problem rather than fixing it, but is a pragmatic path to
a green test suite.  It does not address correctness on single-threaded
executors (e.g., embedded targets).

### B. Explicit readiness loop inside `run_dtls_task`

After any `handle_timeout` call, drain all pending incoming datagrams before
re-entering `select!`:

```rust
// After handle_timeout:
loop {
    match socket.try_recv_from(&mut recv_buf) {
        Ok((n, from)) => { /* handle_packet */ }
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
        Err(e) => { /* error path */ }
    }
}
```

`try_recv_from` is non-blocking; calling it in a loop drains the kernel
buffer and leaves the socket in a provably empty state before the next
`select!`, ensuring the next edge event fires correctly.

### C. Replace `dimpl` with a different DTLS library

`dimpl` 0.6.1 is a young sans-IO crate.  The stale-timeout behavior is
arguably a `dimpl` bug (returning a past deadline is semantically incorrect;
`None` or a zero duration would be cleaner).  Alternatives:

- **`rustls` + UDP socket**: not directly supported (rustls is TLS, not DTLS)
- **`webrtc-dtls`** (from the `webrtc-rs` project): more mature, async-native
- **`openssl` bindings with DTLS**: heavier dependency, not `no_std` friendly
- **`boringssl` via `boring`**: similar tradeoffs

### D. File a `dimpl` issue / patch

The fix in `dimpl` itself would be: in `poll_timeout` (in `engine.rs`), when
the timer is `Unarmed`, return `now + rto` (the next expected retransmit
deadline) rather than `last_now`.  This would turn stale zero-duration
timeouts into real future deadlines, eliminating the immediate-fire case
entirely.

---

## Files Modified / Created

| File | Change |
|------|--------|
| `Cargo.toml` | `dtls` feature added; `dimpl`, `rcgen` optional deps |
| `src/transport/dtls.rs` | New — `DtlsTransport`, `run_dtls_task`, `spawn_task` |
| `src/transport.rs` | Re-exports `DtlsTransport`; `mod dtls` under `#[cfg(feature="dtls")]` |
| `src/socket.rs` | `AnyTransport::Dtls`; `listen_dtls_socket` / `dial_dtls` on all socket types |
| `tests/dtls_transport.rs` | New — 5 tests, all `#[ignore]`d |
| `README.md` | DTLS row changed from `planned` to `aborted` |
| `docs/dtls_status.md` | This document |
