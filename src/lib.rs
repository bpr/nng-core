//! Pure-Rust implementation of the SP (Scalability Protocol) wire format.
//!
//! `nng-core` provides the message types, protocol state machines, and
//! transport layer needed to speak NNG's SP protocol from Rust without
//! linking to the NNG C library. It targets `no_std + alloc` for the core
//! types (protocol state machines, codec, [`ZeroCopyMessage`]) and gates
//! TCP/IPC transport and the higher-level socket API behind the `std` feature.
//!
//! # Crate layout
//!
//! | Module | What it contains |
//! |---|---|
//! | [`message`] | [`Message`] (heap), [`ZeroCopyMessage`] (stack), [`MessageBuf`] trait |
//! | [`codec`] | SP handshake encoding/decoding, message frame codec, [`codec::ProtocolId`] |
//! | [`transport`] | [`transport::FramedTransport`], [`transport::FrameFormat`], loopback pair |
//! | [`protocols`] | One submodule per SP protocol — state machines only, no I/O |
//! | [`socket`] | High-level async socket API (requires `std` + tokio) |
//!
//! # Design: separation of concerns
//!
//! Each layer has exactly one job:
//!
//! 1. **Codec** (`codec`) — encodes/decodes raw bytes; knows nothing about I/O.
//! 2. **Transport** (`transport`) — performs I/O and framing; knows nothing about
//!    protocol semantics.
//! 3. **Protocol state machines** (`protocols`) — manipulate message headers;
//!    know nothing about I/O or byte streams.
//! 4. **Socket API** (`socket`) — composes the above into an ergonomic async API.
//!
//! This separation makes it straightforward to use the state machines with any
//! transport (TCP, IPC, loopback, custom embedded bus) and with either
//! heap-allocated [`Message`] or stack-allocated [`ZeroCopyMessage`] values.
//!
//! # `no_std` support
//!
//! The `std` feature is enabled by default. To build without it:
//!
//! ```toml
//! nng-core = { default-features = false }
//! ```
//!
//! With `no_std` the transport, socket, and TCP/IPC modules are unavailable,
//! but all codec and protocol state machine types compile and work. The
//! [`ZeroCopyMessage`] type requires no allocator at all.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;

pub mod codec;
#[cfg(feature = "std")]
pub mod error;
pub mod message;
pub mod protocols;
#[cfg(feature = "std")]
pub mod socket;
pub mod transport;

#[cfg(feature = "std")]
pub use error::NngError;
pub use message::{Message, MessageBuf, ZeroCopyMessage};
#[cfg(feature = "tower")]
pub use socket::tower_svc::Req0Service;
pub use transport::BufferPool;
