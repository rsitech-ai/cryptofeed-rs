//! Pure, offline prospective EventPulse artifact assembly.
//!
//! This leaf crate has no filesystem, transport, capture, credential, replay,
//! evidence-authoring, paper, canary, live, or execution authority.

#![forbid(unsafe_code)]

mod fixture_v4;
pub mod system;

pub use fixture_v4::{
    FixtureV4Assembler, FixtureV4Error, FixtureV4Request, InMemoryFixtureFileV4, InMemoryFixtureV4,
};
pub use system::{TruthfulEmptySystemAssemblerV1, TruthfulEmptySystemError};
