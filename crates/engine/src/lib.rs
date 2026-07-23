//! Engine ownership of I/O, recording taps, dispatch, and session supervision.

// deny (not forbid): latency_runtime::affinity needs sched_setaffinity on Linux.
#![deny(unsafe_code)]

mod control;
mod error;
mod latency_runtime;
mod live;
mod metrics;
mod reconnect;
mod runner;
mod state;
mod supervisor;

pub use control::*;
pub use error::*;
pub use latency_runtime::{
    LatencyRuntimeError, RuntimeProfile, apply_runtime_profile, pin_worker_to_core,
};
pub use live::*;
pub use metrics::*;
pub use reconnect::*;
pub use runner::*;
pub use state::*;
pub use supervisor::*;
