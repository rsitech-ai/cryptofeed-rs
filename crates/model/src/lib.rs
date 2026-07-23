//! Canonical market-data domain model.
//!
//! Exact arithmetic (`Fixed`) is the only source of truth for prices and sizes.
//! `f64` conversions are convenience-only and never canonical.

#![forbid(unsafe_code)]

mod events;
mod fixed;
mod flags;
mod ids;
mod instrument;
mod overflow;
mod time;

pub use events::*;
pub use fixed::*;
pub use flags::*;
pub use ids::*;
pub use instrument::*;
pub use overflow::*;
pub use time::*;
