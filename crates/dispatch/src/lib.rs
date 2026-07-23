//! Bounded queues for the data plane. Unbounded channels are forbidden.

#![forbid(unsafe_code)]

mod queue;

pub use queue::*;
