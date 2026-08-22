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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PinnedArtifact {
    family: &'static str,
    source_path: &'static str,
    embedded_path: &'static str,
    byte_length: u64,
    sha256: &'static str,
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
