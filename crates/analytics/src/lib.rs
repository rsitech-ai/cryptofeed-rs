//! Deterministic exact market-profile and order-flow analytics.
//!
//! This crate is pure and rendering-neutral: it consumes canonical marketfeed
//! events and emits bounded, serializable analytics state.

#![forbid(unsafe_code)]

#[allow(unused_imports)]
mod bubbles;
mod config;
mod error;
mod flow;
mod grid;
mod profile;

pub use bubbles::*;
pub use config::*;
pub use error::*;
pub use flow::*;
pub use grid::*;
pub use profile::*;
