//! Frozen truthful-empty SYSTEM artifact assembly.

use marketfeed_event_pulse::{
    OfflineArtifactError, OfflineArtifactPreflightV3, ProspectiveAdmissionError,
    ProspectiveCaptureAdmissionV1, ProspectiveSystemArtifactPolicyV1, wire::Rfc3339Time,
};
use thiserror::Error;

/// Stateless assembler with no API for selecting or inventing faults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TruthfulEmptySystemAssemblerV1;

impl TruthfulEmptySystemAssemblerV1 {
    pub fn assemble(
        admission: &ProspectiveCaptureAdmissionV1,
        frozen_evidence: &[u8],
        decision_time: Rfc3339Time,
        complete_epin_json1_without_system: &[u8],
    ) -> Result<OfflineArtifactPreflightV3, TruthfulEmptySystemError> {
        let policy =
            ProspectiveSystemArtifactPolicyV1::from_frozen_evidence(admission, frozen_evidence)?;
        OfflineArtifactPreflightV3::build(
            admission,
            &policy,
            decision_time,
            complete_epin_json1_without_system,
        )
        .map_err(TruthfulEmptySystemError::Preflight)
    }

    pub const fn evidence_authoring_allowed() -> bool {
        false
    }

    pub const fn blocker() -> &'static str {
        "blocked:fixture-provenance"
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TruthfulEmptySystemError {
    #[error(transparent)]
    Admission(#[from] ProspectiveAdmissionError),
    #[error(transparent)]
    Preflight(OfflineArtifactError),
}
