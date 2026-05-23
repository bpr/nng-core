# Post-NNG-API design notes

Captured during the 2026-05-22 style review.  These are changes that would
make the Rust API more idiomatic at the cost of breaking surface
compatibility with the NNG / nanomsg naming and shape.  We are deliberately
not doing them today — kept here as future-direction notes.

The wire protocol (codec, framing, handshake, REQ backtrace marker, IPC
9-byte header) is dictated by NNG and must stay.  These notes only concern
the **Rust-facing API**.

---

## `ProtocolId` should be an enum, not a transparent newtype

```rust
// Current:
pub struct ProtocolId(pub u16);

impl ProtocolId {
    pub const REQ0: Self = Self(0x30);
    // ...

    pub fn expected_peer(self) -> Self {
        match self {
            Self::REQ0 => Self::REP0,
            // ...
            _ => self,       // <-- fallback for unknown u16 from wire
        }
    }
}
```

The transparent newtype exists because the codec must accept any `u16`
that arrives on the wire (a misbehaving or malicious peer could send
anything).  The cost is that every match over `ProtocolId` needs a `_ =>`
arm, so the compiler doesn't warn you when you add a new protocol and
forget to update `expected_peer`, `ws_protocol_name`, or any other
per-protocol table.

Idiomatic Rust:

```rust
#[repr(u16)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum KnownProtocol {
    Pair0 = 0x10,
    Pair1 = 0x11,
    Pub0  = 0x20,
    // ...
}

impl TryFrom<u16> for KnownProtocol {
    type Error = u16;     // returns the unknown id for diagnostics
    fn try_from(v: u16) -> Result<Self, u16> { /* ... */ }
}
```

Then `expected_peer`/`ws_protocol_name` become compile-time-exhaustive
matches.  The wire codec still deals in raw `u16` and only converts at the
validation boundary.

**Why not now:** mechanical refactor with hundreds of `ProtocolId::REQ0`
call sites to update; not a behavior change, just churn.

---

## ~~`Req0`/`Rep0` setters should be a builder~~ ✅ done (2026-05-22)

[`Req0Builder`](../src/socket/reqrep0.rs) is now the preferred API for
initial configuration:

```rust
let req = Req0::builder()
    .resend_time(Duration::from_secs(2))
    .dial("tcp://...")
    .await?;
```

`Req0::set_resend_time` is kept (not deprecated) for runtime adjustment of
an already-dialed socket — the bench at `benches/req0_resend.rs` switches
the resend interval between bench phases on a single shared `Req0`, which
the builder pattern alone cannot express.  Rep0 has no settable options
today, so no `Rep0Builder` was added; one can be introduced when the first
option appears.

---

## ~~`Bus0::listen_and_accept(addr, n)` should expose accepts as a stream~~ ✅ done (2026-05-22)

Added [`Bus0::bind`](../src/socket/bus0.rs) returning `(Bus0, AcceptStream)`:

```rust
let (mut hub, mut accepts) = Bus0::bind("tcp://127.0.0.1:5555").await?;

for _ in 0..3 {
    let peer = accepts.accept().await?;
    hub.add_peer(peer);     // policy decision belongs to the caller
}
// `accepts` can be dropped, kept for later, or moved into another task
// that filters by peer addr / rate-limits.
```

Named `bind` rather than `listen` to keep the existing
`Bus0::listen(addr)` (a one-line convenience for
`listen_and_accept(addr, 1)`) source-compatible.  The original
`Bus0::listen` / `Bus0::listen_and_accept` API is untouched.

`AcceptStream::accept(&mut self).await -> Result<AcceptedPeer, NngError>`
is a plain async method, not yet a `futures::Stream` impl — the
`futures::Stream` form can be added behind the existing `streams` feature
later if a user wants to compose accepts with stream combinators.

The same shape was applied to [`Pull0Fan::bind`](../src/socket/pipeline0.rs)
and [`Surveyor0::bind`](../src/socket/survey0.rs) on 2026-05-22 — both
return `(Self, AcceptStream)` with a per-protocol `Accepted{Pusher,
Respondent}` newtype and a matching `add_pusher` / `add_respondent`
method.  Existing `listen_and_accept` / `wait_for_respondents` are
unchanged.  Each protocol's `AcceptStream` is a sibling type in its own
module — there's no shared trait yet, but the three impls are
deliberately uniform in case one becomes useful.

---

## Message shape: keep `(header, body)`, **do not** move to `BytesMut`

Originally listed as a candidate for migration to a `BytesMut`-cursor
shape that would hide the header/body split from users.  Removed
2026-05-22 after recognizing the conflict with the embedded direction:
`ZeroCopyMessage`'s static `[u8; N]` layout (header at `[0..N/4]`, body
at `[N/4..N]`) is exactly what makes it stack-allocatable and
`no_alloc`-friendly.  A `BytesMut` shape would force heap allocation for
the heap variant and break the stack variant equivalence — both of which
conflict with the drone-network / embedded roadmap.  The slightly leaky
API is the price of `no_std` portability and is worth paying.

---

## Status

Only one item still open (top of file): converting `ProtocolId` from a
transparent newtype to an enum.  Low priority — mostly mechanical churn
for compile-time exhaustiveness over a closed set of ~10 stable
protocols.  Wait for a major version bump that's already touching public
surface.
