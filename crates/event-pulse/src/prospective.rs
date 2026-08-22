//! Fail-closed admission for a future prospective EventPulse capture.
//!
//! This module does not capture data or author evidence. It only proves that a
//! proposed capture topology has the independent sources required by the E2
//! contract and remains below every execution authority boundary.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::wire::{
    ClockSourceKeyV1, ConfiguredTargetKeyV1, ConnectionKeyV1, ContributorKeyV1, ContributorRoleV1,
    ContributorSpecV1, CoverageSourceKeyV1, CursorModeV1, FamilyV1, FaultScopeKindV1,
    InstrumentIdentityV1, MAX_INPUT_BYTES, MechanicsConfigV1, Rfc3339Time, SystemSourceKeyV1,
};

const SCHEMA: &str = "event-pulse-e2-prospective-admission/1.0";
const ROOT_AMENDMENT_COMMIT: &str = "24b51a58c670ab722538bec4a3e1def0278b1107";
const ROOT_DEFAULT_REACHABLE_AT: &str = "2026-08-22T07:35:52Z";
const REPOSITORY_URL: &str = "https://github.com/rsitech-ai/cryptofeed-rs";
const REQUIRED_ROLES: [&str; 9] = [
    "TRADE",
    "QUOTE",
    "BOOK",
    "OPEN_INTEREST",
    "LIQUIDATION",
    "CONFIRMATION",
    "CLOCK",
    "COVERAGE",
    "SYSTEM",
];
const SYSTEM_FREEZE_BYTES: &[u8] =
    include_bytes!("../contracts/prospective/event-pulse-e2-producer-evidence-freeze.json");
const SYSTEM_FREEZE_SHA256: &str =
    "665490e794f72333ba684cdfdcec65494f89cb72d76bd1237a69993e8ea37c29";
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ProspectiveAdmissionError {
    #[error("prospective admission shape is invalid")]
    Shape,
    #[error("root amendment binding is invalid")]
    RootBinding,
    #[error("capture must start strictly after root default reachability")]
    CaptureTiming,
    #[error("required role inventory is invalid")]
    Roles,
    #[error("primary capture source is invalid")]
    PrimarySource,
    #[error("confirmation capture source is invalid")]
    ConfirmationSource,
    #[error("clock evidence must come from an independent sidecar")]
    ClockEvidence,
    #[error("coverage evidence must come from an explicit independent sidecar")]
    CoverageEvidence,
    #[error("stable system evidence mapping is missing")]
    SystemEvidence,
    #[error("truthful-empty system evidence freeze is invalid or unbound")]
    SystemFreeze,
    #[error("immutable source binding is invalid")]
    SourceBinding,
    #[error("capture authority exceeds research-only public observation")]
    Authority,
}

/// Non-forgeable capability for the frozen truthful-empty SYSTEM artifact.
///
/// Construction binds the exact accepted LF bytes and the exact admitted
/// processor source. Callers cannot select a mode, fault, scope, or report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProspectiveSystemArtifactPolicyV1 {
    source_id: String,
    admission_fingerprint: String,
}

impl ProspectiveSystemArtifactPolicyV1 {
    pub fn from_frozen_evidence(
        admission: &ProspectiveCaptureAdmissionV1,
        frozen_evidence: &[u8],
    ) -> Result<Self, ProspectiveAdmissionError> {
        if frozen_evidence.len() > MAX_INPUT_BYTES
            || frozen_evidence != SYSTEM_FREEZE_BYTES
            || format!("{:x}", Sha256::digest(frozen_evidence)) != SYSTEM_FREEZE_SHA256
        {
            return Err(ProspectiveAdmissionError::SystemFreeze);
        }
        let freeze: FrozenProducerEvidence = serde_json::from_slice(frozen_evidence)
            .map_err(|_| ProspectiveAdmissionError::SystemFreeze)?;
        freeze.validate_system()?;
        let system_sources = admission.mechanics_config().system_sources();
        let key = system_sources
            .first()
            .filter(|_| system_sources.len() == 1)
            .ok_or(ProspectiveAdmissionError::SystemFreeze)?;
        if admission.mechanics_config().processor_id() != "event_pulse_e2_prospective"
            || key.source_id() != freeze.system_scenario.source_id
            || key.scope_kind() != FaultScopeKindV1::Processor
            || key.cursor_mode() != CursorModeV1::Derived
            || key.configured_target_key().processor_id()
                != Some(admission.mechanics_config().processor_id())
        {
            return Err(ProspectiveAdmissionError::SystemFreeze);
        }
        Ok(Self {
            source_id: freeze.system_scenario.source_id,
            admission_fingerprint: admission.binding_fingerprint.clone(),
        })
    }

    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    pub const fn mode(&self) -> &'static str {
        "TRUTHFUL_EMPTY"
    }

    pub const fn freeze_sha256(&self) -> &'static str {
        SYSTEM_FREEZE_SHA256
    }

    pub const fn evidence_authoring_allowed(&self) -> bool {
        false
    }

    pub(crate) fn matches(&self, admission: &ProspectiveCaptureAdmissionV1) -> bool {
        self.admission_fingerprint == admission.binding_fingerprint
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FrozenProducerEvidence {
    authority: serde_json::Value,
    binance_primary: serde_json::Value,
    clock_discipline: serde_json::Value,
    coverage_bindings: Vec<serde_json::Value>,
    hyperliquid_confirmation: serde_json::Value,
    schema: String,
    system_scenario: FrozenSystemScenario,
}

impl FrozenProducerEvidence {
    fn validate_system(&self) -> Result<(), ProspectiveAdmissionError> {
        let _retained_exact_fields = (
            &self.authority,
            &self.binance_primary,
            &self.clock_discipline,
            &self.coverage_bindings,
            &self.hyperliquid_confirmation,
        );
        let system = &self.system_scenario;
        if self.schema != "event-pulse-e2-producer-evidence-freeze/1.0"
            || system.status != "FROZEN_CONTRACT"
            || system.mode != "TRUTHFUL_EMPTY"
            || system.source_id != "capture_system"
            || system.scope != "PROCESSOR"
            || system.source_qualification != "UNVERIFIED"
            || system.forbidden_inputs
                != [
                    "FABRICATED_DROP",
                    "INJECTED_FAULT",
                    "RECONNECT_AS_DISCONNECTED",
                ]
            || system.empty_artifact.byte_length != 0
            || system.empty_artifact.record_count != 0
            || system.empty_artifact.sha256 != EMPTY_SHA256
            || system.empty_artifact.first_available_at.is_some()
            || system.empty_artifact.last_available_at.is_some()
            || !system.empty_artifact.record_identities.is_empty()
        {
            return Err(ProspectiveAdmissionError::SystemFreeze);
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FrozenSystemScenario {
    empty_artifact: FrozenEmptyArtifact,
    forbidden_inputs: Vec<String>,
    mode: String,
    scope: String,
    source_id: String,
    source_qualification: String,
    status: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FrozenEmptyArtifact {
    byte_length: u64,
    first_available_at: Option<String>,
    last_available_at: Option<String>,
    record_count: u64,
    record_identities: Vec<serde_json::Value>,
    sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProspectiveCaptureAdmissionV1 {
    primary: SourceBinding,
    confirmation: SourceBinding,
    required_roles: Vec<String>,
    capture_starts_at: Rfc3339Time,
    mechanics_config: MechanicsConfigV1,
    binding_fingerprint: String,
}

impl ProspectiveCaptureAdmissionV1 {
    pub fn from_json(bytes: &[u8]) -> Result<Self, ProspectiveAdmissionError> {
        let raw: RawAdmission =
            serde_json::from_slice(bytes).map_err(|_| ProspectiveAdmissionError::Shape)?;
        let canonical = serde_json::to_vec(&raw).map_err(|_| ProspectiveAdmissionError::Shape)?;
        let binding_fingerprint = format!("{:x}", Sha256::digest(canonical));
        raw.validate(binding_fingerprint)
    }

    pub fn primary_venue(&self) -> &str {
        self.primary.instrument.venue()
    }

    pub fn confirmation_venue(&self) -> &str {
        self.confirmation.instrument.venue()
    }

    pub fn required_role_count(&self) -> usize {
        self.required_roles.len()
    }

    pub fn primary_source_id(&self) -> &str {
        &self.primary.source_id
    }

    pub fn confirmation_source_id(&self) -> &str {
        &self.confirmation.source_id
    }

    pub fn mechanics_config(&self) -> &MechanicsConfigV1 {
        &self.mechanics_config
    }

    pub fn capture_starts_at(&self) -> &Rfc3339Time {
        &self.capture_starts_at
    }

    /// A checked topology is only a prerequisite. Evidence authorship remains
    /// blocked until the bound producers are independently source-locked and a
    /// real post-reachability capture supplies all nine role artifacts.
    pub const fn evidence_authoring_allowed(&self) -> bool {
        false
    }

    pub const fn blocker(&self) -> &'static str {
        "blocked:fixture-provenance"
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAdmission {
    schema: String,
    root_amendment_commit: String,
    root_default_reachable_at: String,
    capture_starts_at: String,
    evidence_claim: String,
    source_qualification: String,
    required_roles: Vec<String>,
    primary: SourceBinding,
    confirmation: SourceBinding,
    clocks: Vec<ClockBinding>,
    coverage: Vec<CoverageBinding>,
    system: SystemBinding,
    authority: AuthorityBoundary,
}

impl RawAdmission {
    fn validate(
        self,
        binding_fingerprint: String,
    ) -> Result<ProspectiveCaptureAdmissionV1, ProspectiveAdmissionError> {
        if self.schema != SCHEMA
            || self.evidence_claim != "PROSPECTIVE_CAUSAL_CAPTURE"
            || self.source_qualification != "UNVERIFIED"
        {
            return Err(ProspectiveAdmissionError::Shape);
        }
        if self.root_amendment_commit != ROOT_AMENDMENT_COMMIT
            || self.root_default_reachable_at != ROOT_DEFAULT_REACHABLE_AT
        {
            return Err(ProspectiveAdmissionError::RootBinding);
        }
        let boundary = Rfc3339Time::parse(ROOT_DEFAULT_REACHABLE_AT)
            .map_err(|_| ProspectiveAdmissionError::RootBinding)?;
        let starts_at = Rfc3339Time::parse(&self.capture_starts_at)
            .map_err(|_| ProspectiveAdmissionError::CaptureTiming)?;
        if starts_at <= boundary
            || !self.capture_starts_at.ends_with('Z')
            || starts_at.canonical() != self.capture_starts_at
        {
            return Err(ProspectiveAdmissionError::CaptureTiming);
        }
        if self
            .required_roles
            .iter()
            .map(String::as_str)
            .ne(REQUIRED_ROLES)
        {
            return Err(ProspectiveAdmissionError::Roles);
        }
        if self.primary.instrument.venue() != "BINANCE"
            || self.primary.format != "MFR1"
            || !self.primary.public_read_only
            || self.primary.roles != ["TRADE", "QUOTE", "BOOK", "OPEN_INTEREST", "LIQUIDATION"]
            || self.primary.families
                != [
                    FamilyV1::Trade,
                    FamilyV1::Quote,
                    FamilyV1::Book,
                    FamilyV1::OpenInterest,
                    FamilyV1::Liquidation,
                ]
        {
            return Err(ProspectiveAdmissionError::PrimarySource);
        }
        if self.confirmation.instrument.venue() != "HYPERLIQUID"
            || self.confirmation.instrument.venue() == self.primary.instrument.venue()
            || self.confirmation.format != "MFR1"
            || !self.confirmation.public_read_only
            || self.confirmation.roles != ["CONFIRMATION"]
            || self.confirmation.families != [FamilyV1::ConfirmationPrice]
            || self.confirmation.instrument.base_asset() != self.primary.instrument.base_asset()
            || self.confirmation.instrument.quote_asset() != self.primary.instrument.quote_asset()
            || self.confirmation.instrument.market_type() != self.primary.instrument.market_type()
        {
            return Err(ProspectiveAdmissionError::ConfirmationSource);
        }
        let expected_clock_subjects = BTreeSet::from([
            self.primary.source_id.as_str(),
            self.confirmation.source_id.as_str(),
        ]);
        let actual_clock_subjects = self
            .clocks
            .iter()
            .map(|clock| clock.subject_source_id.as_str())
            .collect::<BTreeSet<_>>();
        if self.clocks.len() != expected_clock_subjects.len()
            || actual_clock_subjects != expected_clock_subjects
            || self.clocks.iter().any(|clock| {
                clock.evidence_kind != "UTC_MONOTONIC_OBSERVATION"
                    || clock.derivation != "INDEPENDENT_SIDECAR"
            })
        {
            return Err(ProspectiveAdmissionError::ClockEvidence);
        }
        let expected_coverage = self
            .primary
            .families
            .iter()
            .map(|family| (self.primary.source_id.as_str(), *family))
            .chain(
                self.confirmation
                    .families
                    .iter()
                    .map(|family| (self.confirmation.source_id.as_str(), *family)),
            )
            .collect::<BTreeSet<_>>();
        let actual_coverage = self
            .coverage
            .iter()
            .map(|coverage| (coverage.subject_source_id.as_str(), coverage.family))
            .collect::<BTreeSet<_>>();
        if self.coverage.len() != expected_coverage.len()
            || actual_coverage != expected_coverage
            || self.coverage.iter().any(|coverage| {
                coverage.evidence_kind != "EXPLICIT_HEARTBEAT_RANGE"
                    || coverage.derivation != "INDEPENDENT_SIDECAR"
            })
        {
            return Err(ProspectiveAdmissionError::CoverageEvidence);
        }
        if self.system.evidence_kind != "STABLE_SYSTEM_FAULT_MAPPING"
            || self.system.processor_id != "event_pulse_e2_prospective"
            || self.system.target != "PROCESSOR"
            || self.system.fault_scope != "PROCESSOR"
            || self.system.cursor_mode != "DERIVED"
        {
            return Err(ProspectiveAdmissionError::SystemEvidence);
        }
        self.primary.validate()?;
        self.confirmation.validate()?;
        for clock in &self.clocks {
            clock.validate()?;
        }
        for coverage in &self.coverage {
            coverage.validate()?;
        }
        self.system.validate()?;
        let mut source_ids = vec![
            self.primary.source_id.as_str(),
            self.confirmation.source_id.as_str(),
            self.system.source_id.as_str(),
        ];
        source_ids.extend(self.clocks.iter().map(|value| value.source_id.as_str()));
        source_ids.extend(self.coverage.iter().map(|value| value.source_id.as_str()));
        let mut paths = vec![
            self.primary.producer_path.as_str(),
            self.confirmation.producer_path.as_str(),
            self.system.producer_path.as_str(),
        ];
        paths.extend(self.clocks.iter().map(|value| value.producer_path.as_str()));
        paths.extend(
            self.coverage
                .iter()
                .map(|value| value.producer_path.as_str()),
        );
        let mut blobs = vec![
            self.primary.producer_blob_sha256.as_str(),
            self.confirmation.producer_blob_sha256.as_str(),
            self.system.producer_blob_sha256.as_str(),
        ];
        blobs.extend(
            self.clocks
                .iter()
                .map(|value| value.producer_blob_sha256.as_str()),
        );
        blobs.extend(
            self.coverage
                .iter()
                .map(|value| value.producer_blob_sha256.as_str()),
        );
        if source_ids.iter().copied().collect::<BTreeSet<_>>().len() != source_ids.len()
            || paths.iter().copied().collect::<BTreeSet<_>>().len() != paths.len()
            || blobs.iter().copied().collect::<BTreeSet<_>>().len() != blobs.len()
            || self.primary.connection_id == self.confirmation.connection_id
        {
            return Err(ProspectiveAdmissionError::SourceBinding);
        }
        let mechanics_config = self.mechanics_config()?;
        self.authority.validate()?;

        Ok(ProspectiveCaptureAdmissionV1 {
            primary: self.primary,
            confirmation: self.confirmation,
            required_roles: self.required_roles,
            capture_starts_at: starts_at,
            mechanics_config,
            binding_fingerprint,
        })
    }

    fn mechanics_config(&self) -> Result<MechanicsConfigV1, ProspectiveAdmissionError> {
        let primary_key =
            ContributorKeyV1::new(&self.primary.source_id, self.primary.instrument.clone())
                .map_err(|_| ProspectiveAdmissionError::SourceBinding)?;
        let confirmation_key = ContributorKeyV1::new(
            &self.confirmation.source_id,
            self.confirmation.instrument.clone(),
        )
        .map_err(|_| ProspectiveAdmissionError::SourceBinding)?;
        let primary_connection = ConnectionKeyV1::new(&self.primary.connection_id)
            .map_err(|_| ProspectiveAdmissionError::SourceBinding)?;
        let confirmation_connection = ConnectionKeyV1::new(&self.confirmation.connection_id)
            .map_err(|_| ProspectiveAdmissionError::SourceBinding)?;
        let contributors = vec![
            ContributorSpecV1::new(
                primary_key.clone(),
                ContributorRoleV1::Primary,
                self.primary.families.iter().copied(),
            )
            .map_err(|_| ProspectiveAdmissionError::SourceBinding)?,
            ContributorSpecV1::new(
                confirmation_key.clone(),
                ContributorRoleV1::Confirmation,
                self.confirmation.families.iter().copied(),
            )
            .map_err(|_| ProspectiveAdmissionError::SourceBinding)?,
        ];
        let contributor_connections = BTreeMap::from([
            (primary_key.clone(), primary_connection.clone()),
            (confirmation_key.clone(), confirmation_connection.clone()),
        ]);
        let contributor_for = |source_id: &str| match source_id {
            id if id == self.primary.source_id => Some(primary_key.clone()),
            id if id == self.confirmation.source_id => Some(confirmation_key.clone()),
            _ => None,
        };
        let clocks = self
            .clocks
            .iter()
            .map(|clock| {
                ClockSourceKeyV1::new(
                    &clock.source_id,
                    contributor_for(&clock.subject_source_id)
                        .ok_or(ProspectiveAdmissionError::ClockEvidence)?,
                )
                .map_err(|_| ProspectiveAdmissionError::ClockEvidence)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let coverage = self
            .coverage
            .iter()
            .map(|coverage| {
                CoverageSourceKeyV1::new(
                    &coverage.source_id,
                    contributor_for(&coverage.subject_source_id)
                        .ok_or(ProspectiveAdmissionError::CoverageEvidence)?,
                    coverage.family,
                )
                .map_err(|_| ProspectiveAdmissionError::CoverageEvidence)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let system = SystemSourceKeyV1::new(
            &self.system.source_id,
            FaultScopeKindV1::Processor,
            ConfiguredTargetKeyV1::processor(&self.system.processor_id)
                .map_err(|_| ProspectiveAdmissionError::SystemEvidence)?,
            CursorModeV1::Derived,
        )
        .map_err(|_| ProspectiveAdmissionError::SystemEvidence)?;
        MechanicsConfigV1::new(
            &self.system.processor_id,
            vec![primary_connection, confirmation_connection],
            contributors,
            contributor_connections,
            clocks,
            coverage,
            vec![system],
        )
        .map_err(|_| ProspectiveAdmissionError::SourceBinding)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceBinding {
    source_id: String,
    connection_id: String,
    format: String,
    instrument: InstrumentIdentityV1,
    roles: Vec<String>,
    families: Vec<FamilyV1>,
    public_read_only: bool,
    repository_url: String,
    producer_commit: String,
    producer_path: String,
    producer_blob_sha256: String,
}

impl SourceBinding {
    fn validate(&self) -> Result<(), ProspectiveAdmissionError> {
        if self.repository_url != REPOSITORY_URL
            || !valid_source_id(&self.source_id)
            || ConnectionKeyV1::new(&self.connection_id).is_err()
            || self.instrument.validate().is_err()
            || !is_lower_hex(&self.producer_commit, 40)
            || !valid_producer_path(&self.producer_path)
            || !is_lower_hex(&self.producer_blob_sha256, 64)
        {
            return Err(ProspectiveAdmissionError::SourceBinding);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClockBinding {
    source_id: String,
    subject_source_id: String,
    evidence_kind: String,
    derivation: String,
    producer_commit: String,
    producer_path: String,
    producer_blob_sha256: String,
}

impl ClockBinding {
    fn validate(&self) -> Result<(), ProspectiveAdmissionError> {
        validate_local_binding(
            &self.source_id,
            &self.producer_commit,
            &self.producer_path,
            &self.producer_blob_sha256,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CoverageBinding {
    source_id: String,
    subject_source_id: String,
    family: FamilyV1,
    evidence_kind: String,
    derivation: String,
    producer_commit: String,
    producer_path: String,
    producer_blob_sha256: String,
}

impl CoverageBinding {
    fn validate(&self) -> Result<(), ProspectiveAdmissionError> {
        validate_local_binding(
            &self.source_id,
            &self.producer_commit,
            &self.producer_path,
            &self.producer_blob_sha256,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SystemBinding {
    source_id: String,
    processor_id: String,
    target: String,
    fault_scope: String,
    cursor_mode: String,
    evidence_kind: String,
    producer_commit: String,
    producer_path: String,
    producer_blob_sha256: String,
}

impl SystemBinding {
    fn validate(&self) -> Result<(), ProspectiveAdmissionError> {
        validate_local_binding(
            &self.source_id,
            &self.producer_commit,
            &self.producer_path,
            &self.producer_blob_sha256,
        )
    }
}

fn validate_local_binding(
    source_id: &str,
    commit: &str,
    path: &str,
    blob: &str,
) -> Result<(), ProspectiveAdmissionError> {
    if !valid_source_id(source_id)
        || !is_lower_hex(commit, 40)
        || !valid_producer_path(path)
        || !is_lower_hex(blob, 64)
    {
        return Err(ProspectiveAdmissionError::SourceBinding);
    }
    Ok(())
}

fn valid_source_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_lowercase()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn valid_producer_path(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value.starts_with('/')
        && !value.contains('\\')
        && value.split('/').all(|component| {
            !component.is_empty()
                && !matches!(component, "." | "..")
                && component
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        })
}

fn is_lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorityBoundary {
    credentials_allowed: bool,
    private_endpoints_allowed: bool,
    orders_allowed: bool,
    execution_authority: bool,
    paper_authority: bool,
    promotion_authority: bool,
}

impl AuthorityBoundary {
    fn validate(&self) -> Result<(), ProspectiveAdmissionError> {
        if self.credentials_allowed
            || self.private_endpoints_allowed
            || self.orders_allowed
            || self.execution_authority
            || self.paper_authority
            || self.promotion_authority
        {
            return Err(ProspectiveAdmissionError::Authority);
        }
        Ok(())
    }
}
