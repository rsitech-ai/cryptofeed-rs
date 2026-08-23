//! Deterministic, research-only EventPulse contract consumption and mechanics.
//!
//! This crate has no adapter, network, credential, account, order, risk, paper,
//! canary, or live authority.

#![forbid(unsafe_code)]

mod contract;
mod cursor;
pub mod features;
pub mod mechanics;
mod preflight;
mod preflight_v4;
mod prospective;
mod prospective_v2;
mod provenance;
mod replay;
mod replay_v2;
pub mod snapshot;
pub mod window;
pub mod wire;
mod wire_v2;

pub use contract::{
    ContractBundle, ContractError, EventPulseErrorCode, ValidatedContract, canonical_json,
    content_hash, try_content_hash, validate_context_revision, validate_e2_mechanics_profile,
    validate_revision_transition,
};
pub use cursor::{
    CursorError, CursorView, IngestOutcome, Invalidity, SlotState, SourceStateMachine,
    SourceStateMachineV2,
};
pub use preflight::{
    ArtifactRoleV1, InMemoryArtifactV1, InMemoryArtifactV3, OfflineArtifactError,
    OfflineArtifactPreflightV1, OfflineArtifactPreflightV3,
};
pub use preflight_v4::{InMemoryArtifactV4, OfflineArtifactErrorV4, OfflineArtifactPreflightV4};
pub use prospective::{
    ProspectiveAdmissionError, ProspectiveCaptureAdmissionV1, ProspectiveSystemArtifactPolicyV1,
};
pub use prospective_v2::{
    ProspectiveAdmissionErrorV2, ProspectiveCaptureAdmissionV2, ProspectiveSystemArtifactPolicyV2,
};
pub use provenance::{
    ArtifactProvenance, EXPECTED_ROOT_COMMIT, ProvenanceError, ProvenanceManifest,
    VerifiedArtifact, embedded_provenance, verify_artifact_bytes, verify_embedded_contracts,
    verify_embedded_risk_decision_contracts, verify_manifest, verify_risk_decision_artifact_bytes,
};
pub use replay::{EpinJson1Reader, EpinJson1Writer, ReplayInputError};
pub use replay_v2::{MechanicsInputV2JsonlReader, MechanicsInputV2JsonlWriter};
pub use wire_v2::{MarketCursorV2, MechanicsInputRefV2, MechanicsInputV2, SourceProvenanceV2};
