# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`nng-core` is a pure-Rust, `no_std`-compatible implementation of the [NNG Scalability Protocols](https://nng.nanomsg.org/). It was spun out of a fork of `nng-rs` where it lived as the `nng-pure` crate. The rename from `nng-pure` to `nng-core` happened in the spin-out commit.

GitHub: https://github.com/bpr/nng-core  
Default branch: **`master`** (not `main`)

## Commands

```bash
# Format (always run before committing; cargo fmt must produce no changes)
cargo fmt

# Build
cargo build

# Run all tests (129 tests across all suites)
cargo test

# Run a specific test file
cargo test --test zerocopy
cargo test --test protocols
cargo test --test transport

# Run interop tests (require nngcat from system NNG 1.5.x)
cargo test --test interop_nngcat

# Run a single example (see README.md for start order of multi-process examples)
cargo run --example req_server
cargo run --example req_client

# Verify no_std core compiles without std/alloc
cargo build --no-default-features

# Fuzz (requires nightly; targets are in fuzz/fuzz_targets/)
cargo +nightly fuzz build                          # compile all targets
cargo +nightly fuzz run codec_handshake            # arbitrary 8-byte handshake input
cargo +nightly fuzz run codec_frame                # arbitrary frame bytes → decode_frame
cargo +nightly fuzz run transport_recv_tcp         # arbitrary bytes after TCP handshake
cargo +nightly fuzz run transport_recv_ipc         # same, IPC 9-byte frame format
# Seed corpus lives in fuzz/corpus/<target>/.  Findings are saved to fuzz/artifacts/<target>/.
```

Note: there is no workspace — this is a standalone crate. Do **not** use `-p nng-core` flags.

## Style

Use American English spelling throughout all code, comments, and documentation: `color` not `colour`, `center` not `centre`, `-ize` not `-ise`, `serialize` not `serialise`, etc.

## Architecture

Four layers, each with a single responsibility:

1. **`src/codec.rs`** — SP wire codec: 8-byte handshake encode/decode, per-message frame encode/decode, `ProtocolId` constants. No I/O, no alloc beyond what the caller supplies.

2. **`src/transport.rs`** — `FramedTransport<T: Read+Write>` generic over any `embedded-io-async` stream. Runs the handshake and exchanges framed messages. `FrameFormat::Tcp` (8-byte length header) vs `FrameFormat::Ipc` (9-byte, NNG 1.5.x IPC). Submodules: `transport/tcp.rs`, `transport/ipc.rs` (both `std`-gated).

3. **`src/protocols/`** — One submodule per SP protocol pair. Each is a pure state machine that manipulates `MessageBuf` headers — no I/O, no async. All protocol methods are generic over `M: MessageBuf` so they work with both `Message` (heap) and `ZeroCopyMessage<N>` (stack).

4. **`src/socket.rs`** — High-level async socket API (tokio, `std`-gated). One submodule per protocol: `reqrep0`, `pubsub0`, `pipeline0`, `pair0`, `survey0`, `bus0`. Each exposes a typed socket (`Req0`, `Rep0`, etc.) with `dial`/`listen` constructors.

## Key types

- **`Message`** — heap-backed two-part message (header + body `Vec<u8>`). `trim_front` is O(n).
- **`ZeroCopyMessage<const N: usize>`** — stack-allocated `[u8; N]` buffer. Header at `[0..N/4]`, body at `[N/4..N]` with `b_start` pointer for O(1) `trim_front`. Same trick as Linux `sk_buff`.
- **`MessageBuf` trait** — minimal interface (`body`, `header`, `push_back`, `header_push_back`, `trim_front`) implemented by both message types.

## Wire compatibility notes

- **REQ0 backtrace header**: The high bit (`0x8000_0000`) must be set in the wire request ID. Without it, NNG's REP side scans past the ID into payload bytes looking for the end-of-backtrace marker. `Req0State::prepare_outgoing` sets it; `process_incoming` strips it before comparison.
- **NNG 1.5.x IPC framing**: IPC frames use a 9-byte header: `[0x01 type byte][8-byte BE u64 length]`. TCP uses only the 8-byte length. `FrameFormat::Ipc` handles the 1.5.x format; `FrameFormat::Tcp` handles everything else.
- **Protocol IDs**: Computed by `NNI_PROTO(major, minor) = major * 16 + minor`, confirmed against NNG C source.

## Interop tests

`tests/interop_nngcat.rs` runs 8 tests against `nngcat` (the NNG 1.5.2 CLI tool from the system package). These require `nngcat` to be in `PATH`. They cover REQ/REP, PUSH/PULL, and PUB/SUB over both TCP and IPC.

## Bugs found and fixed during example testing

### `Pull0Fan::pull_any` — cancellation-unsafe poll loop

**Location:** `src/socket.rs`, `pipeline0::Pull0Fan`

**Symptom:** The sink process aborted with `memory allocation of 3684054924945334370 bytes failed` as soon as the first worker sent a message.

**Root cause:** The original implementation polled each sender with:
```rust
tokio::select! {
    biased;
    r = self.senders[i].recv() => Some(r),
    _ = std::future::ready(()) => None,
}
```
`FramedTransport::recv` performs multiple `read_exact` calls (8 bytes for the length header, then N bytes for the payload). With `biased` and `ready(())` as the fallback, the `recv` future is polled once and then dropped if it returns `Pending`. `tokio::io::read_exact` is not cancellation-safe: it advances the stream position as it reads, so dropping the future mid-read consumes partial bytes from the TCP socket. The next `recv` call then reads the remaining payload bytes as the start of a new frame header, producing a garbage length field.

**Fix:** Each sender now runs in its own `tokio::spawn` task that drives `recv` to completion and forwards messages (or a disconnect error) into an `mpsc::Receiver`. `pull_any` simply awaits the channel — no future is ever cancelled mid-read.

---

### `Surveyor0::accept_pending` — new connections ignored after first survey round

**Location:** `src/socket.rs`, `survey0::Surveyor0`; `examples/discovery_registry.rs`

**Symptom:** When two nodes connected to the discovery registry simultaneously, only one appeared in survey responses. Nodes that connected after the first survey round were never seen.

**Root cause:** `wait_for_respondents(n)` accepts exactly `n` connections and then returns. Subsequent connections arrive at the OS TCP layer and sit in the kernel accept queue, but the application never calls `accept()` again, so no SP handshake runs and those connections are invisible to `survey()`.

**Fix:** Added `Surveyor0::accept_pending()`, which drains the kernel accept queue in a non-blocking loop using `select! { biased; listener.accept() => ..., ready(()) => break }`. Unlike `FramedTransport::recv`, `TcpListener::accept` does not consume partial data, so cancelling it between iterations is safe. The `discovery_registry` example calls `accept_pending()` at the top of each survey loop, so nodes that join between rounds are included in the next survey.

---

### `Bus0::recv_any` — cancellation-unsafe poll loop

**Location:** `src/transport.rs` (`FramedTransport`); `src/socket.rs`, `bus0::Bus0`; `tests/bus0.rs`

**Symptom:** Under concurrent senders a garbled frame-length field would cause a panic allocating an impossibly large buffer (e.g. `memory allocation of N bytes failed` for a huge N).

**Root cause:** `recv_any` polled each peer transport with the same biased-select pattern:
```rust
tokio::select! {
    biased;
    result = self.peers[i].recv() => Some(result),
    _ = std::future::ready(()) => None,
}
```
`FramedTransport::recv` issued multiple `read` calls (8 bytes for the length header, then N bytes for the payload). If `ready(())` fired before `recv` completed — which happened whenever the peer transport returned `Pending` mid-read — the future was dropped. Any bytes already consumed from the TCP stream were lost. The next `recv` call then treated mid-frame bytes as a new frame-length header, producing a garbage length.

**Fix:** Made `FramedTransport::recv` itself cancellation-safe by storing partial-read state in a `RecvBuf` field on the struct (`len_buf`, `len_filled`, `body`, `body_filled`, `phase`). Dropping the future mid-read and retrying on the next poll resumes from the saved position rather than re-reading from the stream. The `recv_any` biased-select loop is now correct without any change to its structure.

**Tests:** `tests/bus0.rs::framed_recv_cancellation_safe_small_buffer` (unit, reproduces via a 9-byte duplex buffer) and `tests/bus0.rs::bus0_recv_any_concurrent_senders` (end-to-end, 3 peers × 20 msgs).

---

### `Bus0` bounded membership — listener dropped after `listen_and_accept`

**Location:** `src/socket.rs`, `bus0::Bus0`; `examples/bus_chat.rs`; `tests/bus0.rs`

**Symptom:** Peers that dialled a hub after `listen_and_accept(addr, n)` returned received "connection refused" or were silently ignored and never received broadcasts.

**Root cause:** The original `listen_and_accept` accepted exactly `n` connections and then dropped the `TcpListener`. Any peer that connected later hit either a closed port (connection refused) or sat in the kernel accept queue with no application-level accept() call ever draining it.

**Fix:** `Bus0` now stores `listener: Option<AnyListener>` and keeps the listener alive for the entire lifetime of the struct. `accept_pending()` drains the kernel accept queue non-blockingly using `select! { biased; listener.accept() => ..., ready(()) => break }`. Callers invoke `accept_pending()` at the top of each event loop iteration to pick up peers that connected since the previous round. `TcpListener::accept` does not consume partial data, so cancelling it when `ready(())` wins is safe.

**Test:** `tests/bus0.rs::bus0_dynamic_membership`.

## no_std status

The `std` feature is enabled by default. With `--no-default-features`:
- `src/codec.rs`, `src/message.rs`, `src/protocols/` compile (require `alloc` implicitly via `extern crate alloc`)
- `ZeroCopyMessage` requires no allocator at all
- `src/transport.rs`, `src/socket.rs` are excluded

CI-style check: `cargo build --no-default-features`
