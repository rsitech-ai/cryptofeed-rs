use std::collections::BTreeSet;
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const EXPECTED_ROOT_COMMIT: &str = "c32fa67c0e5921bd4a9c4da163daf5c8b6bae08d";
const PROVENANCE_VERSION: &str = "marketfeed-event-pulse-provenance/1.0";
const APPROVED_FAMILIES: [&str; 2] = ["event-pulse/1.0", "quant-harness/1.0"];
const PINNED_ARTIFACTS: [PinnedArtifact; 8] = [
    PinnedArtifact {
        family: "quant-harness/1.0",
        source_path: "research_os/schemas/quant-harness/quant_harness_v1.schema.json",
        embedded_path: "contracts/quant-harness/quant_harness_v1.schema.json",
        byte_length: 10_701,
        sha256: "8ab03a198d257bd30d934591776ec2855194ea9e192a64752484740b5998b2ca",
    },
    PinnedArtifact {
        family: "quant-harness/1.0",
        source_path: "research_os/fixtures/quant-harness/quant_harness_v1_golden.json",
        embedded_path: "contracts/quant-harness/quant_harness_v1_golden.json",
        byte_length: 5_291,
        sha256: "9a76e0192b46cee859233c67b3a0413e1b4a05d86367ca28d0b3116fe219d117",
    },
    PinnedArtifact {
        family: "quant-harness/1.0",
        source_path: "research_os/fixtures/quant-harness/quant_harness_v1_rejections.json",
        embedded_path: "contracts/quant-harness/quant_harness_v1_rejections.json",
        byte_length: 22_913,
        sha256: "ceddd2c43858cbbfb102e27af768be1bcc5aeed39b715eab5cec47d9420b0ed4",
    },
    PinnedArtifact {
        family: "quant-harness/1.0",
        source_path: "docs/superpowers/specs/quant-harness-contract-lock.json",
        embedded_path: "contracts/quant-harness/contract-lock.json",
        byte_length: 2_194,
        sha256: "0b9dbc6aab8085951da6d189d191f22ad26a86b55f13f353c889d375de519557",
    },
    PinnedArtifact {
        family: "event-pulse/1.0",
        source_path: "research_os/schemas/event-pulse/event_pulse_v1.schema.json",
        embedded_path: "contracts/event-pulse/event_pulse_v1.schema.json",
        byte_length: 28_158,
        sha256: "a56e39cbc6c27d851aeec20b6923f6320d8740073b637bcfe87cfae463856b30",
    },
    PinnedArtifact {
        family: "event-pulse/1.0",
        source_path: "research_os/fixtures/event-pulse/event_pulse_v1_golden.json",
        embedded_path: "contracts/event-pulse/event_pulse_v1_golden.json",
        byte_length: 14_209,
        sha256: "0170e425247348c16391b2d3575c22664c0e274f08c0f4dbaa79722c2c0572e8",
    },
    PinnedArtifact {
        family: "event-pulse/1.0",
        source_path: "research_os/fixtures/event-pulse/event_pulse_v1_rejections.json",
        embedded_path: "contracts/event-pulse/event_pulse_v1_rejections.json",
        byte_length: 101_104,
        sha256: "74d69764d6db3436f608a28bb90a7fe8888ea1d5b81d527b120d40a53a84bd39",
    },
    PinnedArtifact {
        family: "event-pulse/1.0",
        source_path: "docs/superpowers/specs/event-pulse-contract-lock.json",
        embedded_path: "contracts/event-pulse/contract-lock.json",
        byte_length: 1_392,
        sha256: "f0c85ebb498dd96a1cda0f918733ed7a237a4f16640b4e1d86e0268fd6cb69c2",
    },
];
const RISK_DECISION_ARTIFACTS: [PinnedArtifact; 3] = [
    PinnedArtifact {
        family: "quant-harness/1.0",
        source_path: "research_os/schemas/quant-harness/risk_decision_v1.schema.json",
        embedded_path: "contracts/quant-harness/risk_decision_v1.schema.json",
        byte_length: 6_037,
        sha256: "06a483c06d4186bc05979bcd9f232f0ccc67aee5fbe453ac0e2e9bf74462cf48",
    },
    PinnedArtifact {
        family: "quant-harness/1.0",
        source_path: "research_os/fixtures/quant-harness/risk_decision_v1_golden.json",
        embedded_path: "contracts/quant-harness/risk_decision_v1_golden.json",
        byte_length: 2_859,
        sha256: "97eb8772358470a9885797d19c24e9449245823ec42ec28cd8d62e8004bfe984",
    },
    PinnedArtifact {
        family: "quant-harness/1.0",
        source_path: "research_os/fixtures/quant-harness/risk_decision_v1_rejections.json",
        embedded_path: "contracts/quant-harness/risk_decision_v1_rejections.json",
        byte_length: 13_288,
        sha256: "c706eec6777c9c4f7b6e99db555abf922d5907dc9a2767ade254a36d3c2365a0",
    },
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PinnedArtifact {
    family: &'static str,
    source_path: &'static str,
    embedded_path: &'static str,
    byte_length: u64,
    sha256: &'static str,
}

#[derive(Debug, Deserialize)]
struct ContractLockIndex {
    generated_artifacts: Vec<LockedGeneratedArtifact>,
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct LockedGeneratedArtifact {
    name: String,
    rejection_vector_path: String,
    rejection_vector_sha256: String,
    schema_path: String,
    schema_sha256: String,
    success_vector_path: String,
    success_vector_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProvenanceManifest {
    pub provenance_version: String,
    pub source_root_commit: String,
    pub artifacts: Vec<ArtifactProvenance>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactProvenance {
    pub family: String,
    pub source_path: String,
    pub embedded_path: String,
    pub byte_length: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedArtifact {
    pub family: String,
    pub source_path: String,
    pub embedded_path: String,
    pub bytes: &'static [u8],
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProvenanceError {
    #[error("invalid provenance manifest: {detail}")]
    InvalidManifest { detail: String },
    #[error("wrong source root commit: expected {expected}, got {actual}")]
    WrongRootCommit { expected: String, actual: String },
    #[error("unapproved contract family: {family}")]
    UnapprovedFamily { family: String },
    #[error("unsafe provenance path: {path}")]
    UnsafePath { path: String },
    #[error("duplicate provenance path: {path}")]
    DuplicatePath { path: String },
    #[error("embedded contract artifact is missing: {path}")]
    MissingArtifact { path: String },
    #[error("embedded contract artifact drifted: {path}")]
    ArtifactDrift { path: String },
    #[error("provenance record does not match its independent pin: {path}")]
    PinnedRecordMismatch { path: String },
}

pub fn embedded_provenance() -> Result<ProvenanceManifest, ProvenanceError> {
    serde_json::from_slice(include_bytes!("../contracts/provenance.json")).map_err(|error| {
        ProvenanceError::InvalidManifest {
            detail: error.to_string(),
        }
    })
}

pub fn verify_manifest(
    manifest: &ProvenanceManifest,
) -> Result<Vec<VerifiedArtifact>, ProvenanceError> {
    if manifest.provenance_version != PROVENANCE_VERSION {
        return Err(ProvenanceError::InvalidManifest {
            detail: format!(
                "expected version {PROVENANCE_VERSION}, got {}",
                manifest.provenance_version
            ),
        });
    }
    if manifest.source_root_commit != EXPECTED_ROOT_COMMIT {
        return Err(ProvenanceError::WrongRootCommit {
            expected: EXPECTED_ROOT_COMMIT.into(),
            actual: manifest.source_root_commit.clone(),
        });
    }

    let mut source_paths = BTreeSet::new();
    let mut embedded_paths = BTreeSet::new();
    if manifest.artifacts.len() != PINNED_ARTIFACTS.len() {
        return Err(ProvenanceError::InvalidManifest {
            detail: format!(
                "expected {} artifacts, got {}",
                PINNED_ARTIFACTS.len(),
                manifest.artifacts.len()
            ),
        });
    }

    let mut verified = Vec::with_capacity(PINNED_ARTIFACTS.len());
    for artifact in &manifest.artifacts {
        if !APPROVED_FAMILIES.contains(&artifact.family.as_str()) {
            return Err(ProvenanceError::UnapprovedFamily {
                family: artifact.family.clone(),
            });
        }
        validate_path(&artifact.source_path)?;
        validate_path(&artifact.embedded_path)?;
        if !source_paths.insert(artifact.source_path.as_str()) {
            return Err(ProvenanceError::DuplicatePath {
                path: artifact.source_path.clone(),
            });
        }
        if !embedded_paths.insert(artifact.embedded_path.as_str()) {
            return Err(ProvenanceError::DuplicatePath {
                path: artifact.embedded_path.clone(),
            });
        }

        let bytes = embedded_bytes(&artifact.embedded_path).ok_or_else(|| {
            ProvenanceError::MissingArtifact {
                path: artifact.embedded_path.clone(),
            }
        })?;
        verify_artifact_bytes(artifact, bytes)?;
        verified.push(VerifiedArtifact {
            family: artifact.family.clone(),
            source_path: artifact.source_path.clone(),
            embedded_path: artifact.embedded_path.clone(),
            bytes,
        });
    }

    for pinned in PINNED_ARTIFACTS {
        if !embedded_paths.contains(pinned.embedded_path) {
            return Err(ProvenanceError::MissingArtifact {
                path: pinned.embedded_path.into(),
            });
        }
    }
    Ok(verified)
}

/// Verify one artifact against the independent compiled-in record.
///
/// This is public so contract-boundary tests can inject drifted bytes. Runtime
/// consumers should call [`verify_embedded_contracts`] and receive bytes only
/// after the complete manifest has passed.
#[doc(hidden)]
pub fn verify_artifact_bytes(
    artifact: &ArtifactProvenance,
    bytes: &[u8],
) -> Result<(), ProvenanceError> {
    let pinned = PINNED_ARTIFACTS
        .iter()
        .find(|pinned| pinned.embedded_path == artifact.embedded_path)
        .ok_or_else(|| ProvenanceError::MissingArtifact {
            path: artifact.embedded_path.clone(),
        })?;
    if artifact.family != pinned.family
        || artifact.source_path != pinned.source_path
        || artifact.byte_length != pinned.byte_length
        || artifact.sha256 != pinned.sha256
    {
        return Err(ProvenanceError::PinnedRecordMismatch {
            path: artifact.embedded_path.clone(),
        });
    }

    let actual_sha256 = format!("{:x}", Sha256::digest(bytes));
    if bytes.len() as u64 != pinned.byte_length || actual_sha256 != pinned.sha256 {
        return Err(ProvenanceError::ArtifactDrift {
            path: pinned.embedded_path.into(),
        });
    }
    Ok(())
}

pub fn verify_embedded_contracts() -> Result<Vec<VerifiedArtifact>, ProvenanceError> {
    let manifest = embedded_provenance()?;
    verify_manifest(&manifest)
}

/// Verify the separately published standalone Q1 RiskDecision family.
///
/// These three artifacts are declared by the already pinned Q1 contract lock,
/// but intentionally remain outside the historical eight-artifact E2
/// provenance manifest. The historical manifest is verified first so the lock
/// cannot be substituted together with the standalone artifacts.
pub fn verify_embedded_risk_decision_contracts() -> Result<Vec<VerifiedArtifact>, ProvenanceError> {
    verify_embedded_contracts()?;
    verify_risk_decision_lock_binding()?;

    RISK_DECISION_ARTIFACTS
        .iter()
        .map(|pinned| {
            let bytes = embedded_risk_decision_bytes(pinned.embedded_path).ok_or_else(|| {
                ProvenanceError::MissingArtifact {
                    path: pinned.embedded_path.into(),
                }
            })?;
            verify_pinned_bytes(pinned, bytes)?;
            Ok(VerifiedArtifact {
                family: pinned.family.into(),
                source_path: pinned.source_path.into(),
                embedded_path: pinned.embedded_path.into(),
                bytes,
            })
        })
        .collect()
}

/// Verify one standalone Q1 RiskDecision artifact against its independent pin.
///
/// This is public only for contract-boundary drift tests.
#[doc(hidden)]
pub fn verify_risk_decision_artifact_bytes(
    embedded_path: &str,
    bytes: &[u8],
) -> Result<(), ProvenanceError> {
    let pinned = RISK_DECISION_ARTIFACTS
        .iter()
        .find(|pinned| pinned.embedded_path == embedded_path)
        .ok_or_else(|| ProvenanceError::MissingArtifact {
            path: embedded_path.into(),
        })?;
    verify_pinned_bytes(pinned, bytes)
}

fn verify_risk_decision_lock_binding() -> Result<(), ProvenanceError> {
    let lock: ContractLockIndex = serde_json::from_slice(include_bytes!(
        "../contracts/quant-harness/contract-lock.json"
    ))
    .map_err(|error| ProvenanceError::InvalidManifest {
        detail: format!("invalid pinned Q1 contract lock: {error}"),
    })?;
    let mut matching = lock
        .generated_artifacts
        .into_iter()
        .filter(|artifact| artifact.name == "risk_decision_v1");
    let actual = matching
        .next()
        .ok_or_else(|| ProvenanceError::MissingArtifact {
            path: "contracts/quant-harness/contract-lock.json#risk_decision_v1".into(),
        })?;
    if matching.next().is_some() || actual != expected_risk_decision_lock_record() {
        return Err(ProvenanceError::PinnedRecordMismatch {
            path: "contracts/quant-harness/contract-lock.json#risk_decision_v1".into(),
        });
    }
    Ok(())
}

fn expected_risk_decision_lock_record() -> LockedGeneratedArtifact {
    let schema = RISK_DECISION_ARTIFACTS[0];
    let success = RISK_DECISION_ARTIFACTS[1];
    let rejection = RISK_DECISION_ARTIFACTS[2];
    LockedGeneratedArtifact {
        name: "risk_decision_v1".into(),
        rejection_vector_path: rejection.source_path.into(),
        rejection_vector_sha256: rejection.sha256.into(),
        schema_path: schema.source_path.into(),
        schema_sha256: schema.sha256.into(),
        success_vector_path: success.source_path.into(),
        success_vector_sha256: success.sha256.into(),
    }
}

fn verify_pinned_bytes(pinned: &PinnedArtifact, bytes: &[u8]) -> Result<(), ProvenanceError> {
    let actual_sha256 = format!("{:x}", Sha256::digest(bytes));
    if bytes.len() as u64 != pinned.byte_length || actual_sha256 != pinned.sha256 {
        return Err(ProvenanceError::ArtifactDrift {
            path: pinned.embedded_path.into(),
        });
    }
    Ok(())
}

fn validate_path(path: &str) -> Result<(), ProvenanceError> {
    if path.is_empty()
        || !Path::new(path)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(ProvenanceError::UnsafePath { path: path.into() });
    }
    Ok(())
}

fn embedded_bytes(path: &str) -> Option<&'static [u8]> {
    match path {
        "contracts/quant-harness/quant_harness_v1.schema.json" => Some(include_bytes!(
            "../contracts/quant-harness/quant_harness_v1.schema.json"
        )),
        "contracts/quant-harness/quant_harness_v1_golden.json" => Some(include_bytes!(
            "../contracts/quant-harness/quant_harness_v1_golden.json"
        )),
        "contracts/quant-harness/quant_harness_v1_rejections.json" => Some(include_bytes!(
            "../contracts/quant-harness/quant_harness_v1_rejections.json"
        )),
        "contracts/quant-harness/contract-lock.json" => Some(include_bytes!(
            "../contracts/quant-harness/contract-lock.json"
        )),
        "contracts/event-pulse/event_pulse_v1.schema.json" => Some(include_bytes!(
            "../contracts/event-pulse/event_pulse_v1.schema.json"
        )),
        "contracts/event-pulse/event_pulse_v1_golden.json" => Some(include_bytes!(
            "../contracts/event-pulse/event_pulse_v1_golden.json"
        )),
        "contracts/event-pulse/event_pulse_v1_rejections.json" => Some(include_bytes!(
            "../contracts/event-pulse/event_pulse_v1_rejections.json"
        )),
        "contracts/event-pulse/contract-lock.json" => Some(include_bytes!(
            "../contracts/event-pulse/contract-lock.json"
        )),
        _ => None,
    }
}

fn embedded_risk_decision_bytes(path: &str) -> Option<&'static [u8]> {
    match path {
        "contracts/quant-harness/risk_decision_v1.schema.json" => Some(include_bytes!(
            "../contracts/quant-harness/risk_decision_v1.schema.json"
        )),
        "contracts/quant-harness/risk_decision_v1_golden.json" => Some(include_bytes!(
            "../contracts/quant-harness/risk_decision_v1_golden.json"
        )),
        "contracts/quant-harness/risk_decision_v1_rejections.json" => Some(include_bytes!(
            "../contracts/quant-harness/risk_decision_v1_rejections.json"
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{ContractLockIndex, expected_risk_decision_lock_record};

    #[test]
    fn risk_decision_lock_row_matches_independent_pins() {
        let lock: ContractLockIndex = serde_json::from_slice(include_bytes!(
            "../contracts/quant-harness/contract-lock.json"
        ))
        .unwrap();
        let actual = lock
            .generated_artifacts
            .into_iter()
            .find(|artifact| artifact.name == "risk_decision_v1")
            .unwrap();
        assert_eq!(actual, expected_risk_decision_lock_record());
    }
}
