//! SP protocol state machines.
//!
//! Each submodule implements the header-manipulation logic for one SP protocol
//! pair. The state machines are pure functions over [`MessageBuf`] values —
//! they perform no I/O and allocate nothing beyond what is already in the
//! message buffer.
//!
//! | Module | Protocols | Pattern |
//! |---|---|---|
//! | [`reqrep`] | REQ0, REP0 | Request / reply with correlation IDs |
//! | [`pubsub`] | PUB0, SUB0 | Fan-out publish / prefix-filtered subscribe |
//! | [`pipeline`] | PUSH0, PULL0 | One-way pipeline (no headers) |
//! | [`pair`] | PAIR0, PAIR1 | Point-to-point; PAIR1 adds TTL hop limiting |
//! | [`survey`] | SURVEYOR0, RESPONDENT0 | Broadcast query / collect replies |
//! | [`bus`] | BUS0 | Many-to-many broadcast (no headers) |
//!
//! [`MessageBuf`]: crate::MessageBuf

pub mod bus;
pub mod pair;
pub mod pipeline;
pub mod pubsub;
pub mod reqrep;
pub mod survey;
