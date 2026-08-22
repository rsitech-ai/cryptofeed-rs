//! Pure, offline prospective EventPulse artifact assembly.
//!
//! This leaf crate has no filesystem, transport, capture, credential, replay,
//! evidence-authoring, paper, canary, live, or execution authority.

#![forbid(unsafe_code)]

pub mod system;

pub use system::{TruthfulEmptySystemAssemblerV1, TruthfulEmptySystemError};
