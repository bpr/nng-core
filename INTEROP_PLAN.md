# Interop Test Plan: nng-core ↔ C NNG

Goal: verify that nng-core speaks correct NNG wire protocol against the
reference C implementation across all supported protocols, transports, and
message characteristics.

---

## Infrastructure

**Recommended approach: `nng-sys` as a dev-dependency.**

Add `nng-sys` (the low-level Rust bindings to the C NNG library) as a
dev-dependency. This allows both sides of each test to run in the same
process, eliminating process-spawn overhead and making it easy to write
property-based interop tests (e.g. send arbitrary messages through nng-core,
receive them via nng-sys, assert byte-for-byte equality).

```toml
[dev-dependencies]
nng-sys = "1"   # or whichever version ships with NNG 1.5.x on the CI host
```

Gate all interop tests on the `nng-sys` library being available:

```rust
// tests/interop_c.rs
#[cfg(feature = "interop")]   // or a separate Cargo feature / env var
```

**Alternative (no nng-sys):** write small C helper programs, compile them
via `build.rs` using `cc`, and spawn them as subprocesses. More portable
but harder to write rich tests. Use this only if linking nng-sys proves
difficult on CI.

**Existing nngcat tests** (`tests/interop_nngcat.rs`) remain as a lightweight
smoke-test tier that requires only `nngcat` in PATH.

---

## Coverage Matrix

| Protocol          | TCP nng→C | TCP C→nng | IPC nng→C | IPC C→nng | Notes |
|-------------------|:---------:|:---------:|:---------:|:---------:|-------|
| REQ/REP           | existing  | existing  | existing  | existing  | nngcat already covers basics |
| PUSH/PULL         | existing  | ✗         | existing  | ✗         | add reverse direction |
| PUB/SUB           | existing  | ✗         | ✗         | ✗         | add IPC + reverse |
| PAIR              | ✗         | ✗         | ✗         | ✗         | not covered at all |
| SURVEYOR/RESP.    | ✗         | ✗         | ✗         | ✗         | not covered at all |
| BUS               | ✗         | ✗         | ✗         | ✗         | mixed C+Rust node mesh |

---

## Test Scenarios (priority order)

### Tier 1 — Correctness (implement first)

1. **REQ/REP round-trip, both initiator directions**
   - nng-core REQ dials C REP server; send N requests, verify each reply body
     matches request body.
   - C REQ dials nng-core REP server; same check from the other side.
   - Verify request IDs are correctly stripped from the received body (the
     high-bit backtrace marker is a known interop hazard).

2. **PUSH/PULL pipeline, both directions**
   - nng-core PUSH → C PULL: send 1000 messages, verify all received in order.
   - C PUSH → nng-core PULL: same.

3. **PUB/SUB topic filtering**
   - C PUB publishes on topics A, B, C; nng-core SUB subscribes to A only;
     verify only A messages arrive.
   - nng-core PUB; C SUB: same.
   - Test empty-prefix subscription (matches all).

4. **PAIR bidirectional**
   - Simultaneous send from both sides; verify all messages arrive intact.

5. **SURVEYOR/RESPONDENT**
   - nng-core Surveyor, C Respondents: verify all responses collected.
   - C Surveyor, nng-core Respondents: same.
   - Verify stale-survey-ID rejection across the language boundary.

6. **BUS mesh**
   - 2 C nodes + 1 nng-core node; broadcast from each, verify all others
     receive.

### Tier 2 — Stress and edge cases

7. **Large messages**: 1 byte, 1 KB, 64 KB, 1 MB, 16 MB. Verify no framing
   corruption across all sizes for REQ/REP and PUSH/PULL.

8. **Binary bodies**: arbitrary non-UTF8 bytes. Confirm nng-core does not
   assume text encoding anywhere on the wire path.

9. **High-volume pipeline**: 100k messages through PUSH/PULL; verify count
   and absence of duplicates or drops.

10. **Reconnect**: C server restarts mid-stream; nng-core client reconnects
    and resumes. Verify no state corruption.

11. **Concurrent senders**: multiple nng-core PUSH sockets → single C PULL;
    verify total message count (exercises the Pull0Fan recv path under real
    C traffic).

### Tier 3 — Protocol-specific edge cases

12. **REQ/REP ID wraparound**: drive Req0State to wrap next_id past u32::MAX
    with C REP on the other side; verify IDs never zero, replies always
    accepted.

13. **SUB with duplicate subscriptions**: subscribe to the same topic twice
    from C, unsubscribe once; verify messages still arrive (C NNG behaviour
    should match Sub0State deduplication).

14. **SURVEYOR timeout**: C Surveyor sets a short deadline; some nng-core
    Respondents are slow; verify only timely responses are returned.

---

## IPC-specific notes

- IPC tests must run on Unix only (`#[cfg(unix)]`).
- Use `FrameFormat::Ipc` (9-byte header) for nng 1.5.x; use `FrameFormat::Tcp`
  for nng 2.x. The test suite should detect the installed nng version and
  select the correct format, or parameterize over both.
- Socket paths: use `std::env::temp_dir()` + a unique suffix (e.g. test name
  + PID) to avoid collisions between parallel test runs.

---

## Property-based interop (stretch goal)

Once the basic scenarios pass, add a proptest layer:

```rust
proptest! {
    fn reqrep_roundtrip_arbitrary_body(
        body in proptest::collection::vec(any::<u8>(), 0..4096)
    ) {
        // nng-core REQ sends body, C REP echoes it, nng-core REQ receives —
        // assert byte-for-byte equality.
    }
}
```

This catches framing bugs that only appear at specific message lengths
(e.g., exactly at a TCP MSS boundary or at the 8-byte frame-header boundary).

---

## CI notes

- Gate the full interop suite behind a `INTEROP=1` environment variable or a
  Cargo feature so it doesn't block `cargo test` on machines without NNG
  installed.
- The existing nngcat tier (`--test interop_nngcat`) remains the default
  interop smoke test; the new nng-sys tier runs in a separate CI job.
- Record the NNG version under test in each CI run (nng-sys exposes
  `NNG_VERSION`); fail loudly if it differs from the expected 1.5.x.
