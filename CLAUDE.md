# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`nng-core` is a pure-Rust, `no_std`-compatible implementation of the [NNG Scalability Protocols](https://nng.nanomsg.org/). It was spun out of the `nng-rs` workspace (at `/home/bpr/src/rust/repositories/nng-rs`) where it lived as the `nng-pure` crate. The rename from `nng-pure` → `nng-core` happened in the spin-out commit.

GitHub: https://github.com/bpr/nng-core  
Default branch: **`master`** (not `main`)

## Commands

```bash
# Build
cargo build

# Run all tests
cargo test

# Run a specific test file
cargo test --test zerocopy
cargo test --test protocols
cargo test --test transport

# Run interop tests (require nngcat from system NNG 1.5.x)
cargo test --test interop_nngcat

# Run a single example
cargo run --example req-rep
cargo run --example pubsub
cargo run --example pipeline
cargo run --example pair
cargo run --example survey
cargo run --example bus

# Verify no_std core compiles without std/alloc
cargo build --no-default-features
```

Note: there is no workspace — this is a standalone crate. Do **not** use `-p nng-core` flags.

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

## Known issues / pending work

The `README.md` needs fixes before the next release:

1. **"clean-room" (line 9)** — inaccurate; we read NNG source to figure out the IPC frame format and protocol IDs. Change to "pure-Rust reimplementation".
2. **Workspace reference (line 16)** — "long-term goal is for `nng` and `anng` to use `nng-core`…" refers to the old `nng-rs` workspace. Remove or reframe for a standalone crate.
3. **`-p nng-core` flags (lines 115–128)** — workspace flag, wrong for a standalone crate. Drop the `-p nng-core`.
4. **Test count "75 tests" (line 127)** — now 98 tests.

## no_std status

The `std` feature is enabled by default. With `--no-default-features`:
- `src/codec.rs`, `src/message.rs`, `src/protocols/` compile (require `alloc` implicitly via `extern crate alloc`)
- `ZeroCopyMessage` requires no allocator at all
- `src/transport.rs`, `src/socket.rs` are excluded

CI-style check: `cargo build --no-default-features`
