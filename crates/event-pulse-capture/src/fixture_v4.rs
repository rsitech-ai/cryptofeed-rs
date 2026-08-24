//! Pure in-memory Fixture V4 package assembly and strict readback.

use marketfeed_event_pulse::{
    ArtifactRoleV1, OfflineArtifactErrorV4, OfflineArtifactPreflightV4,
    ProspectiveCaptureAdmissionV2, ProspectiveSystemArtifactPolicyV2,
    wire::{MAX_INPUT_BYTES, Rfc3339Time},
};
use serde::Serialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;

const CONTRACT_BYTES: &[u8] =
    include_bytes!("../contracts/fixture-v4/event-pulse-e2-fixture-v4-contract.json");
const AMENDMENT_BYTES: &[u8] =
    include_bytes!("../contracts/fixture-v4/2026-08-24-event-pulse-e2-fixture-v4-amendment.md");
const CONTRACT_SHA256: &str = "cb899211245fe039f30d9f0d595133365f36d28fff5b508c20e1bf52363a9f47";
const AMENDMENT_SHA256: &str = "2c19540bcc953700318a09738dfdbcf167c591827e8825adcad8003889fff965";
#[derive(Debug, Clone)]
pub struct FixtureV4Assembler {
    admission: ProspectiveCaptureAdmissionV2,
    policy: ProspectiveSystemArtifactPolicyV2,
}

impl FixtureV4Assembler {
    pub fn new(
        admission: ProspectiveCaptureAdmissionV2,
        policy: ProspectiveSystemArtifactPolicyV2,
    ) -> Result<Self, FixtureV4Error> {
        verify_contracts()?;
        if !policy.matches_admission(&admission) {
            return Err(FixtureV4Error::SystemPolicyMismatch);
        }
        Ok(Self { admission, policy })
    }

    pub fn assemble(
        &self,
        request: FixtureV4Request<'_>,
    ) -> Result<InMemoryFixtureV4, FixtureV4Error> {
        validate_request(&self.admission, &request)?;
        let preflight = OfflineArtifactPreflightV4::build(
            &self.admission,
            &self.policy,
            request.decision_time.clone(),
            request.complete_jsonl,
        )?;
        let partitions = preflight
            .artifacts()
            .iter()
            .map(|artifact| (artifact.role(), artifact.bytes()))
            .collect::<Vec<_>>();
        let readback = OfflineArtifactPreflightV4::readback(
            &self.admission,
            &self.policy,
            request.decision_time.clone(),
            &partitions,
        )?;
        if readback != preflight {
            return Err(FixtureV4Error::ReadbackMismatch);
        }

        let contract: Value =
            serde_json::from_slice(CONTRACT_BYTES).map_err(|_| FixtureV4Error::EmbeddedContract)?;
        let admission_bytes = admission_bytes(&self.admission, &contract)?;
        let artifacts = preflight.artifacts();
        let max_available_at = artifacts
            .iter()
            .filter_map(|artifact| artifact.last_available_at())
            .max()
            .ok_or(FixtureV4Error::IncompleteArtifacts)?
            .clone();
        if max_available_at > request.capture_ends_at {
            return Err(FixtureV4Error::InputAfterCaptureEnd);
        }

        let manifest = manifest_value(
            &self.admission,
            &contract,
            request.fixture_id,
            &request.capture_ends_at,
            &request.decision_time,
            request.source_terms,
            &admission_bytes,
            artifacts,
            &max_available_at,
        )?;
        let manifest_bytes = canonical_line(&manifest)?;

        let mut files = Vec::with_capacity(11);
        files.push(InMemoryFixtureFileV4::new("manifest.json", manifest_bytes));
        files.push(InMemoryFixtureFileV4::new(
            "admission.json",
            admission_bytes,
        ));
        for artifact in artifacts {
            files.push(InMemoryFixtureFileV4::new(
                role_path(artifact.role()),
                artifact.bytes().to_vec(),
            ));
        }
        let package = InMemoryFixtureV4 { files };
        package.strict_readback(&self.admission, &self.policy, request.decision_time)?;
        Ok(package)
    }

    /// Strictly validates and adopts an already assembled in-memory package.
    pub fn readback(
        &self,
        files: &[(&str, &[u8])],
        decision_time: Rfc3339Time,
    ) -> Result<InMemoryFixtureV4, FixtureV4Error> {
        let package = InMemoryFixtureV4 {
            files: files
                .iter()
                .map(|(path, bytes)| InMemoryFixtureFileV4::new(path, bytes.to_vec()))
                .collect(),
        };
        package.strict_readback(&self.admission, &self.policy, decision_time)?;
        Ok(package)
    }
}

#[derive(Debug, Clone)]
pub struct FixtureV4Request<'a> {
    pub fixture_id: &'a str,
    pub capture_ends_at: Rfc3339Time,
    pub decision_time: Rfc3339Time,
    pub source_terms: &'a str,
    pub complete_jsonl: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InMemoryFixtureFileV4 {
    path: String,
    bytes: Vec<u8>,
}

impl InMemoryFixtureFileV4 {
    fn new(path: &str, bytes: Vec<u8>) -> Self {
        Self {
            path: path.to_owned(),
            bytes,
        }
    }
    pub fn path(&self) -> &str {
        &self.path
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InMemoryFixtureV4 {
    files: Vec<InMemoryFixtureFileV4>,
}

impl InMemoryFixtureV4 {
    pub fn files(&self) -> &[InMemoryFixtureFileV4] {
        &self.files
    }
    pub fn file(&self, path: &str) -> Option<&[u8]> {
        self.files
            .iter()
            .find(|file| file.path == path)
            .map(|file| file.bytes.as_slice())
    }
    pub const fn status(&self) -> &'static str {
        "STRUCTURAL_V4_CANDIDATE"
    }
    pub const fn blocker(&self) -> &'static str {
        "blocked:fixture-provenance"
    }
    pub const fn evidence_authoring_allowed(&self) -> bool {
        false
    }
    pub const fn capture_allowed(&self) -> bool {
        false
    }
    pub const fn execution_allowed(&self) -> bool {
        false
    }

    fn strict_readback(
        &self,
        admission: &ProspectiveCaptureAdmissionV2,
        policy: &ProspectiveSystemArtifactPolicyV2,
        decision_time: Rfc3339Time,
    ) -> Result<(), FixtureV4Error> {
        if self.files.len() != 11
            || self.files[0].path != "manifest.json"
            || self.files[1].path != "admission.json"
        {
            return Err(FixtureV4Error::ReadbackMismatch);
        }
        let manifest: Value = strict_canonical_line(&self.files[0].bytes)?;
        let admission_value: Value = strict_canonical_line(&self.files[1].bytes)?;
        if manifest.get("schema_version")
            != Some(&Value::String(
                "event-pulse-e2-prospective-fixture/4.0".to_owned(),
            ))
            || admission_value.get("schema")
                != Some(&Value::String(
                    "event-pulse-e2-prospective-admission/2.0".to_owned(),
                ))
        {
            return Err(FixtureV4Error::ReadbackMismatch);
        }
        let mut partitions = Vec::with_capacity(9);
        for (file, role) in self.files[2..].iter().zip(ArtifactRoleV1::ALL) {
            if file.path != role_path(role) {
                return Err(FixtureV4Error::ReadbackMismatch);
            }
            partitions.push((role, file.bytes.as_slice()));
        }
        let preflight = OfflineArtifactPreflightV4::readback(
            admission,
            policy,
            decision_time.clone(),
            &partitions,
        )?;
        let contract: Value =
            serde_json::from_slice(CONTRACT_BYTES).map_err(|_| FixtureV4Error::EmbeddedContract)?;
        let expected_admission = admission_bytes(admission, &contract)?;
        if expected_admission != self.files[1].bytes {
            return Err(FixtureV4Error::ReadbackMismatch);
        }
        let fixture_id = manifest
            .get("fixture_id")
            .and_then(Value::as_str)
            .ok_or(FixtureV4Error::ReadbackMismatch)?;
        let capture_end = manifest
            .pointer("/capture/ended_at")
            .and_then(Value::as_str)
            .and_then(|value| Rfc3339Time::parse(value).ok())
            .ok_or(FixtureV4Error::ReadbackMismatch)?;
        let source_terms = manifest
            .pointer("/retention/source_terms")
            .and_then(Value::as_str)
            .ok_or(FixtureV4Error::ReadbackMismatch)?;
        validate_request(
            admission,
            &FixtureV4Request {
                fixture_id,
                capture_ends_at: capture_end.clone(),
                decision_time: decision_time.clone(),
                source_terms,
                complete_jsonl: &[],
            },
        )?;
        let max_available_at = preflight
            .artifacts()
            .iter()
            .filter_map(|artifact| artifact.last_available_at())
            .max()
            .ok_or(FixtureV4Error::ReadbackMismatch)?;
        if max_available_at > &capture_end {
            return Err(FixtureV4Error::InputAfterCaptureEnd);
        }
        let expected_manifest = manifest_value(
            admission,
            &contract,
            fixture_id,
            &capture_end,
            &decision_time,
            source_terms,
            &expected_admission,
            preflight.artifacts(),
            max_available_at,
        )?;
        if manifest != expected_manifest {
            return Err(FixtureV4Error::ReadbackMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FixtureV4Error {
    #[error(transparent)]
    Preflight(#[from] OfflineArtifactErrorV4),
    #[error("embedded Fixture V4 root contract pin is invalid")]
    EmbeddedContract,
    #[error("truthful-empty SYSTEM policy is not bound to the admission")]
    SystemPolicyMismatch,
    #[error("fixture id must be 3..64 lowercase ASCII identifier characters")]
    FixtureId,
    #[error("source terms must be a nonempty trimmed string")]
    SourceTerms,
    #[error("capture end and decision time are invalid")]
    CaptureInterval,
    #[error("an input is later than capture end")]
    InputAfterCaptureEnd,
    #[error("the preflight did not produce every required artifact")]
    IncompleteArtifacts,
    #[error("assembled Fixture V4 package failed strict readback")]
    ReadbackMismatch,
    #[error("canonical Fixture V4 JSON serialization failed")]
    CanonicalJson,
}

fn verify_contracts() -> Result<(), FixtureV4Error> {
    if CONTRACT_BYTES.len() != 5_527
        || AMENDMENT_BYTES.len() != 10_647
        || sha256(CONTRACT_BYTES) != CONTRACT_SHA256
        || sha256(AMENDMENT_BYTES) != AMENDMENT_SHA256
    {
        return Err(FixtureV4Error::EmbeddedContract);
    }
    Ok(())
}

fn validate_request(
    admission: &ProspectiveCaptureAdmissionV2,
    request: &FixtureV4Request<'_>,
) -> Result<(), FixtureV4Error> {
    let id = request.fixture_id.as_bytes();
    if !(3..=64).contains(&id.len())
        || (!id[0].is_ascii_lowercase() && !id[0].is_ascii_digit())
        || !id.iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        return Err(FixtureV4Error::FixtureId);
    }
    if request.source_terms.is_empty() || request.source_terms.trim() != request.source_terms {
        return Err(FixtureV4Error::SourceTerms);
    }
    if request.complete_jsonl.len() > MAX_INPUT_BYTES
        || request.capture_ends_at <= *admission.capture_starts_at()
        || request.decision_time < request.capture_ends_at
    {
        return Err(FixtureV4Error::CaptureInterval);
    }
    Ok(())
}

fn admission_bytes(
    admission: &ProspectiveCaptureAdmissionV2,
    contract: &Value,
) -> Result<Vec<u8>, FixtureV4Error> {
    let bindings = contract
        .get("bindings")
        .ok_or(FixtureV4Error::EmbeddedContract)?;
    canonical_line(&json!({
        "schema": "event-pulse-e2-prospective-admission/2.0",
        "topology_binding": bindings.get("topology").ok_or(FixtureV4Error::EmbeddedContract)?,
        "wire_contract_binding": bindings.get("wire").ok_or(FixtureV4Error::EmbeddedContract)?,
        "capture_starts_at": admission.capture_starts_at().canonical(),
        "evidence_claim": "PROSPECTIVE_CAUSAL_CAPTURE",
        "source_qualification": "UNVERIFIED",
        "authority": contract.get("authority").ok_or(FixtureV4Error::EmbeddedContract)?,
    }))
}

fn authority(contract: &Value) -> Result<Value, FixtureV4Error> {
    let base = contract
        .get("authority")
        .and_then(Value::as_object)
        .ok_or(FixtureV4Error::EmbeddedContract)?;
    let mut result = Map::new();
    result.insert(
        "source_qualification".to_owned(),
        Value::String("UNVERIFIED".to_owned()),
    );
    result.extend(base.clone());
    Ok(Value::Object(result))
}

#[allow(clippy::too_many_arguments)]
fn manifest_value(
    admission: &ProspectiveCaptureAdmissionV2,
    contract: &Value,
    fixture_id: &str,
    capture_end: &Rfc3339Time,
    decision_time: &Rfc3339Time,
    source_terms: &str,
    admission_bytes: &[u8],
    artifacts: &[marketfeed_event_pulse::InMemoryArtifactV4],
    max_available_at: &Rfc3339Time,
) -> Result<Value, FixtureV4Error> {
    let artifact_rows = artifacts
        .iter()
        .map(|artifact| {
            json!({
                "role": role_name(artifact.role()),
                "path": role_path(artifact.role()),
                "sha256": artifact.sha256(),
                "byte_length": artifact.byte_len(),
                "record_count": artifact.record_count(),
                "first_available_at": artifact.first_available_at().map(Rfc3339Time::canonical),
                "last_available_at": artifact.last_available_at().map(Rfc3339Time::canonical),
                "record_identities": Vec::<String>::new(),
            })
        })
        .collect::<Vec<_>>();
    let bindings = contract
        .get("bindings")
        .cloned()
        .ok_or(FixtureV4Error::EmbeddedContract)?;
    let transformation = bindings
        .get("transformer")
        .cloned()
        .ok_or(FixtureV4Error::EmbeddedContract)?;
    Ok(json!({
        "schema_version": "event-pulse-e2-prospective-fixture/4.0",
        "fixture_id": fixture_id,
        "evidence_claim": "PROSPECTIVE_CAUSAL_CAPTURE",
        "amendment_binding": {
            "repository_url": "https://github.com/s1korrrr/rsibot.git",
            "commit": "24b51a58c670ab722538bec4a3e1def0278b1107",
            "default_reachable_at": "2026-08-22T07:35:52Z",
        },
        "fixture_v4_contract_binding": {
            "path": "docs/superpowers/specs/event-pulse-e2-fixture-v4-contract.json",
            "byte_length": CONTRACT_BYTES.len(),
            "sha256": CONTRACT_SHA256,
        },
        "published_bindings": bindings,
        "admission_binding": {
            "path": "admission.json",
            "byte_length": admission_bytes.len(),
            "sha256": sha256(admission_bytes),
        },
        "authority": authority(contract)?,
        "capture": {
            "mode": "PROSPECTIVE",
            "source_kind": "REAL_PUBLIC_READ_ONLY_CAPTURE",
            "capture_host_clock": "INDEPENDENT_DISCIPLINED_CLOCK",
            "started_at": admission.capture_starts_at().canonical(),
            "ended_at": capture_end.canonical(),
        },
        "artifacts": artifact_rows,
        "causality": {
            "availability_authority": "available_at",
            "decision_time": decision_time.canonical(),
            "max_available_at": max_available_at.canonical(),
            "future_rows_allowed": false,
        },
        "transformation": transformation,
        "retention": {
            "sanitized": true,
            "retention_allowed": true,
            "source_terms": source_terms,
        },
    }))
}

fn canonical_line<T: Serialize>(value: &T) -> Result<Vec<u8>, FixtureV4Error> {
    let value = serde_json::to_value(value).map_err(|_| FixtureV4Error::CanonicalJson)?;
    let mut bytes = serde_json::to_vec(&value).map_err(|_| FixtureV4Error::CanonicalJson)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn strict_canonical_line(bytes: &[u8]) -> Result<Value, FixtureV4Error> {
    let Some(line) = bytes.strip_suffix(b"\n") else {
        return Err(FixtureV4Error::ReadbackMismatch);
    };
    if line.ends_with(b"\n") {
        return Err(FixtureV4Error::ReadbackMismatch);
    }
    let value: Value =
        serde_json::from_slice(line).map_err(|_| FixtureV4Error::ReadbackMismatch)?;
    if canonical_line(&value)? != bytes {
        return Err(FixtureV4Error::ReadbackMismatch);
    }
    Ok(value)
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn role_name(role: ArtifactRoleV1) -> &'static str {
    match role {
        ArtifactRoleV1::Trade => "TRADE",
        ArtifactRoleV1::Quote => "QUOTE",
        ArtifactRoleV1::Book => "BOOK",
        ArtifactRoleV1::OpenInterest => "OPEN_INTEREST",
        ArtifactRoleV1::Liquidation => "LIQUIDATION",
        ArtifactRoleV1::Confirmation => "CONFIRMATION",
        ArtifactRoleV1::Clock => "CLOCK",
        ArtifactRoleV1::Coverage => "COVERAGE",
        ArtifactRoleV1::System => "SYSTEM",
    }
}

fn role_path(role: ArtifactRoleV1) -> &'static str {
    match role {
        ArtifactRoleV1::Trade => "inputs/trade.jsonl",
        ArtifactRoleV1::Quote => "inputs/quote.jsonl",
        ArtifactRoleV1::Book => "inputs/book.jsonl",
        ArtifactRoleV1::OpenInterest => "inputs/open_interest.jsonl",
        ArtifactRoleV1::Liquidation => "inputs/liquidation.jsonl",
        ArtifactRoleV1::Confirmation => "inputs/confirmation.jsonl",
        ArtifactRoleV1::Clock => "inputs/clock.jsonl",
        ArtifactRoleV1::Coverage => "inputs/coverage.jsonl",
        ArtifactRoleV1::System => "inputs/system.jsonl",
    }
}
