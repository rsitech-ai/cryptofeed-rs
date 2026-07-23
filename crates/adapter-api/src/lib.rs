//! Adapter contracts: venues are deterministic input→action state machines.
//!
//! Adapters MUST NOT open sockets, spawn tasks, sleep, log globally, or write disk.

#![forbid(unsafe_code)]

mod error;
mod factory;
mod session;
mod subscription;
mod venue;

pub use error::*;
pub use factory::*;
pub use session::*;
pub use subscription::*;
pub use venue::*;
