//! Pure in-memory Fixture V4 package assembly and strict readback.

use marketfeed_event_pulse::{
    ArtifactRoleV1, OfflineArtifactErrorV4, OfflineArtifactPreflightV4,
    ProspectiveCaptureAdmissionV2, ProspectiveSystemArtifactPolicyV2,
    wire::{MAX_INPUT_BYTES, Rfc3339Time},
};
use std::collections::BTreeMap;

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
        validate_contract_artifacts(
            preflight.artifacts(),
            self.admission.capture_starts_at(),
            &request.capture_ends_at,
            &request.decision_time,
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
        validate_contract_artifacts(
            preflight.artifacts(),
            admission.capture_starts_at(),
            &capture_end,
            &decision_time,
        )?;
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
    #[error("Fixture V4 contract violation: {0}")]
    Contract(&'static str),
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

#[derive(Debug)]
struct MarketContractRecord {
    source: String,
    connection: u64,
    session: u64,
    frame: u64,
    action: u64,
    item: u64,
    receive_ns: i64,
    role: ArtifactRoleV1,
    payload_kind: String,
}

fn validate_contract_artifacts(
    artifacts: &[marketfeed_event_pulse::InMemoryArtifactV4],
    capture_start: &Rfc3339Time,
    capture_end: &Rfc3339Time,
    decision_time: &Rfc3339Time,
) -> Result<(), FixtureV4Error> {
    if artifacts.len() != ArtifactRoleV1::ALL.len() {
        return Err(FixtureV4Error::Contract("artifact cardinality"));
    }
    let start_us = capture_start.utc_micros();
    let end_us = capture_end.utc_micros();
    let decision_us = decision_time.utc_micros();
    let mut market_records = Vec::new();
    for artifact in artifacts {
        if artifact.role() == ArtifactRoleV1::System {
            if !artifact.bytes().is_empty() {
                return Err(FixtureV4Error::Contract("truthful-empty system"));
            }
            continue;
        }
        let mut prior_receive_ns = None;
        let mut prior_coordinate = None;
        let mut prior_trade = None;
        let mut book_last = None;
        let mut book_has_delta = false;
        let mut sidecars: BTreeMap<String, (String, u64, u64)> = BTreeMap::new();
        for line in artifact.bytes().split(|byte| *byte == b'\n') {
            if line.is_empty() {
                continue;
            }
            let value: Value = serde_json::from_slice(line)
                .map_err(|_| FixtureV4Error::Contract("canonical record"))?;
            if serde_json::to_vec(&value)
                .map_err(|_| FixtureV4Error::Contract("canonical record"))?
                != line
            {
                return Err(FixtureV4Error::Contract("canonical record"));
            }
            if is_market_role(artifact.role()) {
                let observed = validate_market_contract_record(
                    &value,
                    artifact.role(),
                    start_us,
                    end_us,
                    decision_us,
                )?;
                if prior_receive_ns.is_some_and(|prior| observed.receive_ns < prior) {
                    return Err(FixtureV4Error::Contract("market receive continuity"));
                }
                let coordinate = (observed.frame, observed.action, observed.item);
                if prior_coordinate.is_some_and(|prior| coordinate <= prior) {
                    return Err(FixtureV4Error::Contract("market raw coordinate continuity"));
                }
                prior_receive_ns = Some(observed.receive_ns);
                prior_coordinate = Some(coordinate);
                if artifact.role() == ArtifactRoleV1::Trade {
                    let trade = u64_field(
                        &value["source_provenance"],
                        "aggregate_trade_id",
                        "trade continuity",
                    )?;
                    if prior_trade
                        .is_some_and(|prior: u64| prior == i64::MAX as u64 || trade != prior + 1)
                    {
                        return Err(FixtureV4Error::Contract("trade continuity"));
                    }
                    prior_trade = Some(trade);
                } else if artifact.role() == ArtifactRoleV1::Book {
                    let provenance = object(&value["source_provenance"], "book provenance")?;
                    match provenance.get("kind").and_then(Value::as_str) {
                        Some("BINANCE_BOOK_SNAPSHOT") => {
                            let snapshot = u64_field(
                                &value["source_provenance"],
                                "last_update_id",
                                "book continuity",
                            )?;
                            if book_last.is_some_and(|last| {
                                snapshot < last || (!book_has_delta && snapshot == last)
                            }) {
                                return Err(FixtureV4Error::Contract("book continuity"));
                            }
                            book_last = Some(snapshot);
                            book_has_delta = false;
                        }
                        Some("BINANCE_BOOK_DELTA") => {
                            let first = u64_field(
                                &value["source_provenance"],
                                "first_update_id",
                                "book continuity",
                            )?;
                            let final_id = u64_field(
                                &value["source_provenance"],
                                "final_update_id",
                                "book continuity",
                            )?;
                            let previous = u64_field(
                                &value["source_provenance"],
                                "previous_final_update_id",
                                "book continuity",
                            )?;
                            let last =
                                book_last.ok_or(FixtureV4Error::Contract("book continuity"))?;
                            let invalid_delta = if book_has_delta {
                                previous != last || final_id <= last
                            } else {
                                first > last || last > final_id
                            };
                            if invalid_delta {
                                return Err(FixtureV4Error::Contract("book continuity"));
                            }
                            book_last = Some(final_id);
                            book_has_delta = true;
                        }
                        _ => return Err(FixtureV4Error::Contract("book provenance")),
                    }
                }
                market_records.push(observed);
            } else {
                validate_sidecar_contract_record(&value, artifact.role(), &mut sidecars)?;
            }
        }
    }
    validate_contributor_replay(&market_records)
}

fn validate_market_contract_record(
    value: &Value,
    role: ArtifactRoleV1,
    capture_start_us: i64,
    capture_end_us: i64,
    decision_us: i64,
) -> Result<MarketContractRecord, FixtureV4Error> {
    exact_keys(
        value,
        &[
            "action_index",
            "catalog",
            "envelope",
            "kind",
            "market_cursor",
            "payload_hash",
            "source_provenance",
        ],
        "market record shape",
    )?;
    if value["kind"] != "MARKET" {
        return Err(FixtureV4Error::Contract("market record kind"));
    }
    let (source, venue, instrument, connection, session, expected_payload, expected_catalog) =
        market_contract(role)?;
    if value["catalog"] != expected_catalog {
        return Err(FixtureV4Error::Contract("source-specific catalog"));
    }
    let envelope = object(&value["envelope"], "market envelope")?;
    if u64_field(&value["envelope"], "venue", "market route")? != venue
        || u64_field(&value["envelope"], "instrument", "market route")? != instrument
        || u64_field(&value["envelope"], "connection", "market route")? != connection
        || u64_field(&value["envelope"], "session", "market route")? != session
    {
        return Err(FixtureV4Error::Contract("market route"));
    }
    let frame = u64_field(&value["envelope"], "frame_seq", "market frame")?;
    let action = value["action_index"]
        .as_u64()
        .filter(|action| *action <= 65_534)
        .ok_or(FixtureV4Error::Contract("market action"))?;
    let item = u64_field(&value["envelope"], "event_index", "market item")?;
    if frame == 0 || item > 65_535 {
        return Err(FixtureV4Error::Contract("market raw coordinate"));
    }
    let receive_ns = i64_field(&value["envelope"], "receive_ts", "market receive time")?;
    let exchange_ns = i64_field(&value["envelope"], "exchange_ts", "market exchange time")?;
    if exchange_ns > receive_ns
        || exchange_ns.div_euclid(1_000) < capture_start_us
        || receive_ns.div_euclid(1_000) < capture_start_us
        || receive_ns.div_euclid(1_000) > capture_end_us
        || receive_ns.div_euclid(1_000) > decision_us
    {
        return Err(FixtureV4Error::Contract("market causal time"));
    }
    let payload = object(
        envelope
            .get("payload")
            .ok_or(FixtureV4Error::Contract("market payload"))?,
        "market payload",
    )?;
    if payload.len() != 1 {
        return Err(FixtureV4Error::Contract("market payload"));
    }
    let payload_kind = payload
        .keys()
        .next()
        .ok_or(FixtureV4Error::Contract("market payload"))?;
    if role != ArtifactRoleV1::Book && payload_kind != expected_payload {
        return Err(FixtureV4Error::Contract("market payload family"));
    }
    if role == ArtifactRoleV1::Book && payload_kind != "BookSnapshot" && payload_kind != "BookDelta"
    {
        return Err(FixtureV4Error::Contract("market payload family"));
    }
    validate_payload_domain(value, role, payload_kind)?;
    Ok(MarketContractRecord {
        source: source.to_owned(),
        connection,
        session,
        frame,
        action,
        item,
        receive_ns,
        role,
        payload_kind: payload_kind.to_owned(),
    })
}

fn validate_payload_domain(
    value: &Value,
    role: ArtifactRoleV1,
    payload_kind: &str,
) -> Result<(), FixtureV4Error> {
    let payload = &value["envelope"]["payload"][payload_kind];
    let flags = value["envelope"]["flags"]
        .as_u64()
        .filter(|flags| *flags <= u32::MAX.into())
        .ok_or(FixtureV4Error::Contract("envelope flags"))?;
    match role {
        ArtifactRoleV1::Trade => {
            exact_keys(
                payload,
                &["aggressor", "price", "quantity", "trade_id"],
                "binance trade payload",
            )?;
            positive_decimal(&payload["price"], "binance trade payload")?;
            positive_decimal(&payload["quantity"], "binance trade payload")?;
            if !matches!(payload["aggressor"].as_str(), Some("Buy" | "Sell")) || flags != 0 {
                return Err(FixtureV4Error::Contract("binance trade payload"));
            }
            let aggregate = u64_field(
                &value["source_provenance"],
                "aggregate_trade_id",
                "binance trade payload",
            )?;
            if payload["trade_id"].as_str() != Some(&aggregate.to_string()) {
                return Err(FixtureV4Error::Contract("binance trade payload"));
            }
        }
        ArtifactRoleV1::Quote => {
            exact_keys(
                payload,
                &["ask_price", "ask_quantity", "bid_price", "bid_quantity"],
                "binance quote payload",
            )?;
            for field in ["ask_price", "ask_quantity", "bid_price", "bid_quantity"] {
                positive_decimal(&payload[field], "binance quote payload")?;
            }
            if flags != 0 {
                return Err(FixtureV4Error::Contract("binance quote payload"));
            }
        }
        ArtifactRoleV1::Book if payload_kind == "BookSnapshot" => {
            exact_keys(
                payload,
                &["asks", "bids", "checksum", "depth"],
                "binance book payload",
            )?;
            if payload["checksum"] != Value::Null || payload["depth"] != 1000 || flags != 1 {
                return Err(FixtureV4Error::Contract("binance book payload"));
            }
            for side in ["asks", "bids"] {
                for level in payload[side]
                    .as_array()
                    .ok_or(FixtureV4Error::Contract("binance book payload"))?
                {
                    exact_keys(level, &["price", "quantity"], "binance book payload")?;
                    positive_decimal(&level["price"], "binance book payload")?;
                    positive_decimal(&level["quantity"], "binance book payload")?;
                }
            }
        }
        ArtifactRoleV1::Book => {
            exact_keys(payload, &["changes", "checksum"], "binance book payload")?;
            if payload["checksum"] != Value::Null || flags != 2 {
                return Err(FixtureV4Error::Contract("binance book payload"));
            }
            for change in payload["changes"]
                .as_array()
                .ok_or(FixtureV4Error::Contract("binance book payload"))?
            {
                exact_keys(
                    change,
                    &["operation", "price", "quantity", "side"],
                    "binance book payload",
                )?;
                positive_decimal(&change["price"], "binance book payload")?;
                if !matches!(change["side"].as_str(), Some("Bid" | "Ask")) {
                    return Err(FixtureV4Error::Contract("binance book payload"));
                }
                match change["operation"].as_str() {
                    Some("Delete") if change["quantity"] == Value::Null => {}
                    Some("Upsert") => {
                        positive_decimal(&change["quantity"], "binance book payload")?;
                    }
                    _ => return Err(FixtureV4Error::Contract("binance book payload")),
                }
            }
        }
        ArtifactRoleV1::OpenInterest => {
            exact_keys(payload, &["quantity"], "binance open interest payload")?;
            decimal(&payload["quantity"], "binance open interest payload")?;
            if flags != 0 {
                return Err(FixtureV4Error::Contract("binance open interest payload"));
            }
        }
        ArtifactRoleV1::Liquidation => {
            exact_keys(
                payload,
                &["price", "quantity", "side"],
                "binance liquidation payload",
            )?;
            decimal(&payload["price"], "binance liquidation payload")?;
            decimal(&payload["quantity"], "binance liquidation payload")?;
            if !matches!(payload["side"].as_str(), Some("Buy" | "Sell")) || flags != 0 {
                return Err(FixtureV4Error::Contract("binance liquidation payload"));
            }
        }
        ArtifactRoleV1::Confirmation => {
            exact_keys(payload, &["price"], "confirmation payload")?;
            decimal(&payload["price"], "confirmation payload")?;
            if flags != 0 {
                return Err(FixtureV4Error::Contract("confirmation payload"));
            }
        }
        _ => return Err(FixtureV4Error::Contract("market payload family")),
    }
    Ok(())
}

fn validate_sidecar_contract_record(
    value: &Value,
    role: ArtifactRoleV1,
    prior: &mut BTreeMap<String, (String, u64, u64)>,
) -> Result<(), FixtureV4Error> {
    let (source_field, cursor_field) = match role {
        ArtifactRoleV1::Clock => ("clock_source", "clock_cursor"),
        ArtifactRoleV1::Coverage => ("coverage_source", "coverage_cursor"),
        _ => return Err(FixtureV4Error::Contract("sidecar role")),
    };
    if value["kind"] != role_name(role) {
        return Err(FixtureV4Error::Contract("sidecar kind"));
    }
    let source = object(&value[source_field], "sidecar source")?;
    let key = object(
        source
            .get("key")
            .ok_or(FixtureV4Error::Contract("sidecar source"))?,
        "sidecar source",
    )?;
    let source_id = key
        .get("source_id")
        .and_then(Value::as_str)
        .ok_or(FixtureV4Error::Contract("sidecar source"))?;
    let epoch = source
        .get("epoch")
        .and_then(Value::as_str)
        .filter(|epoch| epoch.starts_with("epoch_") && epoch.len() <= 70)
        .ok_or(FixtureV4Error::Contract("sidecar source epoch"))?;
    let generation = source
        .get("epoch_generation")
        .and_then(Value::as_u64)
        .filter(|generation| *generation <= 255)
        .ok_or(FixtureV4Error::Contract("sidecar source generation"))?;
    let contributor_generation = value["contributor"]["epoch_generation"]
        .as_u64()
        .ok_or(FixtureV4Error::Contract("sidecar contributor"))?;
    if generation != contributor_generation {
        return Err(FixtureV4Error::Contract("sidecar generation relation"));
    }
    let cursor = object(&value[cursor_field], "sidecar cursor")?;
    if cursor.get("kind").and_then(Value::as_str) != Some("NATIVE_RANGE") {
        return Err(FixtureV4Error::Contract("sidecar cursor"));
    }
    let start = cursor
        .get("start")
        .and_then(Value::as_u64)
        .filter(|value| *value <= i64::MAX as u64)
        .ok_or(FixtureV4Error::Contract("sidecar cursor"))?;
    let end = cursor
        .get("end")
        .and_then(Value::as_u64)
        .filter(|value| *value <= i64::MAX as u64 && start <= *value)
        .ok_or(FixtureV4Error::Contract("sidecar cursor"))?;
    if let Some((prior_epoch, prior_generation, prior_end)) = prior.get(source_id) {
        if epoch != prior_epoch
            || generation != *prior_generation
            || *prior_end == i64::MAX as u64
            || start != *prior_end + 1
        {
            return Err(FixtureV4Error::Contract("sidecar continuity"));
        }
    }
    prior.insert(source_id.to_owned(), (epoch.to_owned(), generation, end));
    if role == ArtifactRoleV1::Clock {
        if !matches!(
            value["clock_state"].as_str(),
            Some("synchronized" | "degraded")
        ) || !matches!(
            value["quality_state"].as_str(),
            Some("validated" | "degraded")
        ) || (value["clock_state"] == "synchronized") != (value["quality_state"] == "validated")
            || value["freshness_limit_ms"]
                .as_u64()
                .is_none_or(|value| value == 0)
        {
            return Err(FixtureV4Error::Contract("clock domain"));
        }
        let skew = value["observed_skew_ms"]
            .as_str()
            .ok_or(FixtureV4Error::Contract("clock domain"))?;
        if !valid_clock_skew(skew) {
            return Err(FixtureV4Error::Contract("clock domain"));
        }
    } else if value["family"] != key["family"] {
        return Err(FixtureV4Error::Contract("coverage domain"));
    }
    Ok(())
}

fn validate_contributor_replay(records: &[MarketContractRecord]) -> Result<(), FixtureV4Error> {
    let mut streams: BTreeMap<(String, u64, u64), Vec<&MarketContractRecord>> = BTreeMap::new();
    for record in records {
        streams
            .entry((record.source.clone(), record.connection, record.session))
            .or_default()
            .push(record);
    }
    for ((source, _, _), mut records) in streams {
        records.sort_by_key(|record| (record.frame, record.action, record.item));
        let mut prior_coordinate = None;
        let mut prior_receive = None;
        let mut frames: BTreeMap<u64, (i64, Vec<&MarketContractRecord>)> = BTreeMap::new();
        for record in records {
            let coordinate = (record.frame, record.action, record.item);
            if prior_coordinate.is_some_and(|prior| coordinate <= prior)
                || prior_receive.is_some_and(|prior| record.receive_ns < prior)
            {
                return Err(FixtureV4Error::Contract("contributor replay continuity"));
            }
            let frame = frames
                .entry(record.frame)
                .or_insert_with(|| (record.receive_ns, Vec::new()));
            if frame.0 != record.receive_ns {
                return Err(FixtureV4Error::Contract("contributor frame receive time"));
            }
            frame.1.push(record);
            prior_coordinate = Some(coordinate);
            prior_receive = Some(record.receive_ns);
        }
        if source == "binance_primary_public" || source == "binance_primary_market" {
            for (_, (_, records)) in frames {
                if records
                    .iter()
                    .enumerate()
                    .any(|(index, record)| record.action != index as u64 || record.item != 0)
                    || records.iter().any(|record| record.role != records[0].role)
                {
                    return Err(FixtureV4Error::Contract("binance frame grammar"));
                }
                if records.len() > 1
                    && (records[0].payload_kind != "BookSnapshot"
                        || records[1..]
                            .iter()
                            .any(|record| record.payload_kind != "BookDelta"))
                {
                    return Err(FixtureV4Error::Contract("binance frame grammar"));
                }
            }
        }
    }
    Ok(())
}

fn market_contract(
    role: ArtifactRoleV1,
) -> Result<(&'static str, u64, u64, u64, u64, &'static str, Value), FixtureV4Error> {
    let binance_instrument = json!({
        "base_asset": "BNB", "market_type": "PERPETUAL", "quote_asset": "USDT",
        "venue": "BINANCE", "venue_symbol": "BNBUSDT"
    });
    let public = json!({
        "connection_epochs": [{"connection_epoch":"epoch_public","connection_id":11,"epoch_generation":0,"session_id":21}],
        "instruments": {"7": binance_instrument.clone()}, "open_interest": {},
        "venue_sources": {"3":{"source_id":"binance_primary_public","venue":"BINANCE"}}
    });
    let market = json!({
        "connection_epochs": [{"connection_epoch":"epoch_market","connection_id":12,"epoch_generation":0,"session_id":22}],
        "instruments": {"7": binance_instrument}, "open_interest": {"7":{"encoding":"CONTRACTS"}},
        "venue_sources": {"3":{"source_id":"binance_primary_market","venue":"BINANCE"}}
    });
    let confirmation = json!({
        "connection_epochs": [
            {"connection_epoch":"epoch_public","connection_id":11,"epoch_generation":0,"session_id":21},
            {"connection_epoch":"epoch_market","connection_id":12,"epoch_generation":0,"session_id":22},
            {"connection_epoch":"epoch_confirmation","connection_id":13,"epoch_generation":0,"session_id":23}
        ],
        "instruments": {
            "1":{"base_asset":"BNB","market_type":"PERPETUAL","quote_asset":"USDT","venue":"BINANCE","venue_symbol":"BNBUSDT"},
            "2":{"base_asset":"BNB","market_type":"PERPETUAL","quote_asset":"USDT","venue":"BINANCE","venue_symbol":"BNBUSDT"},
            "3":{"base_asset":"BNB","market_type":"PERPETUAL","quote_asset":"USDT","venue":"HYPERLIQUID","venue_symbol":"BNB"}
        },
        "open_interest":{"2":{"encoding":"CONTRACTS"}},
        "venue_sources":{
            "1":{"source_id":"binance_primary_public","venue":"BINANCE"},
            "2":{"source_id":"binance_primary_market","venue":"BINANCE"},
            "3":{"source_id":"hyperliquid_confirmation","venue":"HYPERLIQUID"}
        }
    });
    Ok(match role {
        ArtifactRoleV1::Trade => ("binance_primary_market", 3, 7, 12, 22, "Trade", market),
        ArtifactRoleV1::Quote => ("binance_primary_public", 3, 7, 11, 21, "Quote", public),
        ArtifactRoleV1::Book => (
            "binance_primary_public",
            3,
            7,
            11,
            21,
            "BookSnapshot",
            public,
        ),
        ArtifactRoleV1::OpenInterest => (
            "binance_primary_market",
            3,
            7,
            12,
            22,
            "OpenInterest",
            market,
        ),
        ArtifactRoleV1::Liquidation => (
            "binance_primary_market",
            3,
            7,
            12,
            22,
            "Liquidation",
            market,
        ),
        ArtifactRoleV1::Confirmation => (
            "hyperliquid_confirmation",
            3,
            3,
            13,
            23,
            "MarkPrice",
            confirmation,
        ),
        _ => return Err(FixtureV4Error::Contract("market role")),
    })
}

fn is_market_role(role: ArtifactRoleV1) -> bool {
    matches!(
        role,
        ArtifactRoleV1::Trade
            | ArtifactRoleV1::Quote
            | ArtifactRoleV1::Book
            | ArtifactRoleV1::OpenInterest
            | ArtifactRoleV1::Liquidation
            | ArtifactRoleV1::Confirmation
    )
}

fn object<'a>(
    value: &'a Value,
    rule: &'static str,
) -> Result<&'a Map<String, Value>, FixtureV4Error> {
    value.as_object().ok_or(FixtureV4Error::Contract(rule))
}

fn exact_keys(value: &Value, keys: &[&str], rule: &'static str) -> Result<(), FixtureV4Error> {
    let value = object(value, rule)?;
    if value.len() != keys.len() || keys.iter().any(|key| !value.contains_key(*key)) {
        return Err(FixtureV4Error::Contract(rule));
    }
    Ok(())
}

fn u64_field(value: &Value, field: &str, rule: &'static str) -> Result<u64, FixtureV4Error> {
    object(value, rule)?
        .get(field)
        .and_then(Value::as_u64)
        .ok_or(FixtureV4Error::Contract(rule))
}

fn i64_field(value: &Value, field: &str, rule: &'static str) -> Result<i64, FixtureV4Error> {
    object(value, rule)?
        .get(field)
        .and_then(Value::as_i64)
        .ok_or(FixtureV4Error::Contract(rule))
}

fn decimal(value: &Value, rule: &'static str) -> Result<i128, FixtureV4Error> {
    exact_keys(value, &["coefficient", "scale"], rule)?;
    let coefficient = value["coefficient"]
        .as_i64()
        .map(i128::from)
        .or_else(|| value["coefficient"].as_u64().map(i128::from))
        .ok_or(FixtureV4Error::Contract(rule))?;
    value["scale"]
        .as_u64()
        .filter(|scale| *scale <= 255)
        .ok_or(FixtureV4Error::Contract(rule))?;
    Ok(coefficient)
}

fn positive_decimal(value: &Value, rule: &'static str) -> Result<(), FixtureV4Error> {
    if decimal(value, rule)? <= 0 {
        return Err(FixtureV4Error::Contract(rule));
    }
    Ok(())
}

fn valid_clock_skew(value: &str) -> bool {
    let body = value.strip_prefix('-').unwrap_or(value);
    let mut parts = body.split('.');
    let Some(integer) = parts.next() else {
        return false;
    };
    let fraction = parts.next();
    if parts.next().is_some()
        || integer.is_empty()
        || integer.len() > 18
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || integer.len() > 1 && integer.starts_with('0')
        || fraction.is_some_and(|fraction| {
            fraction.is_empty()
                || fraction.len() > 8
                || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        })
    {
        return false;
    }
    !(value.starts_with('-')
        && integer.bytes().all(|byte| byte == b'0')
        && fraction.is_none_or(|fraction| fraction.bytes().all(|byte| byte == b'0')))
}
