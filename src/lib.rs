#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(not(feature = "std"))]
extern crate alloc;

pub mod codec;
pub mod message;
pub mod protocols;
#[cfg(feature = "std")]
pub mod socket;
pub mod transport;

pub use message::Message;
