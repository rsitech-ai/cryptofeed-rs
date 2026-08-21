//! Deterministic, research-only EventPulse contract consumption and mechanics.
//!
//! This crate has no adapter, network, credential, account, order, risk, paper,
//! canary, or live authority.

#![forbid(unsafe_code)]

mod provenance;

pub use provenance::{
    ArtifactProvenance, EXPECTED_ROOT_COMMIT, ProvenanceError, ProvenanceManifest,
    VerifiedArtifact, embedded_provenance, verify_embedded_contracts, verify_manifest,
};
