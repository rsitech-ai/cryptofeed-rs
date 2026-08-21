use std::collections::BTreeSet;
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const EXPECTED_ROOT_COMMIT: &str = "c32fa67c0e5921bd4a9c4da163daf5c8b6bae08d";
const PROVENANCE_VERSION: &str = "marketfeed-event-pulse-provenance/1.0";
const APPROVED_FAMILIES: [&str; 2] = ["event-pulse/1.0", "quant-harness/1.0"];
const EXPECTED_EMBEDDED_PATHS: [&str; 8] = [
    "contracts/event-pulse/contract-lock.json",
    "contracts/event-pulse/event_pulse_v1.schema.json",
    "contracts/event-pulse/event_pulse_v1_golden.json",
    "contracts/event-pulse/event_pulse_v1_rejections.json",
    "contracts/quant-harness/contract-lock.json",
    "contracts/quant-harness/quant_harness_v1.schema.json",
    "contracts/quant-harness/quant_harness_v1_golden.json",
    "contracts/quant-harness/quant_harness_v1_rejections.json",
];

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
    let mut verified = Vec::with_capacity(EXPECTED_EMBEDDED_PATHS.len());
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
        let actual_sha256 = format!("{:x}", Sha256::digest(bytes));
        if bytes.len() as u64 != artifact.byte_length || actual_sha256 != artifact.sha256 {
            return Err(ProvenanceError::ArtifactDrift {
                path: artifact.embedded_path.clone(),
            });
        }
        verified.push(VerifiedArtifact {
            family: artifact.family.clone(),
            source_path: artifact.source_path.clone(),
            embedded_path: artifact.embedded_path.clone(),
            bytes,
        });
    }

    for expected in EXPECTED_EMBEDDED_PATHS {
        if !embedded_paths.contains(expected) {
            return Err(ProvenanceError::MissingArtifact {
                path: expected.into(),
            });
        }
    }
    Ok(verified)
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
