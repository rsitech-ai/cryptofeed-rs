//! Deterministic, research-only EventPulse contract consumption and mechanics.
//!
//! This crate has no adapter, network, credential, account, order, risk, paper,
//! canary, or live authority.

#![forbid(unsafe_code)]

mod contract;
mod cursor;
pub mod features;
pub mod mechanics;
mod prospective;
mod provenance;
mod replay;
pub mod snapshot;
pub mod window;
pub mod wire;

pub use contract::{
    ContractBundle, ContractError, EventPulseErrorCode, ValidatedContract, canonical_json,
    content_hash, try_content_hash, validate_context_revision, validate_e2_mechanics_profile,
    validate_revision_transition,
};
pub use cursor::{
    CursorError, CursorView, IngestOutcome, Invalidity, SlotState, SourceStateMachine,
};
pub use prospective::{ProspectiveAdmissionError, ProspectiveCaptureAdmissionV1};
pub use provenance::{
    ArtifactProvenance, EXPECTED_ROOT_COMMIT, ProvenanceError, ProvenanceManifest,
    VerifiedArtifact, embedded_provenance, verify_artifact_bytes, verify_embedded_contracts,
    verify_manifest,
};
pub use replay::{EpinJson1Reader, EpinJson1Writer, ReplayInputError};
