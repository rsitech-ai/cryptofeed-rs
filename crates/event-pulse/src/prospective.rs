//! Fail-closed admission for a future prospective EventPulse capture.
//!
//! This module does not capture data or author evidence. It only proves that a
//! proposed capture topology has the independent sources required by the E2
//! contract and remains below every execution authority boundary.

use std::collections::BTreeSet;

use serde::Deserialize;
use thiserror::Error;

use crate::wire::{FamilyV1, Rfc3339Time};

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
    #[error("immutable source binding is invalid")]
    SourceBinding,
    #[error("capture authority exceeds research-only public observation")]
    Authority,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProspectiveCaptureAdmissionV1 {
    primary: SourceBinding,
    confirmation: SourceBinding,
    required_roles: Vec<String>,
}

impl ProspectiveCaptureAdmissionV1 {
    pub fn from_json(bytes: &[u8]) -> Result<Self, ProspectiveAdmissionError> {
        let raw: RawAdmission =
            serde_json::from_slice(bytes).map_err(|_| ProspectiveAdmissionError::Shape)?;
        raw.validate()
    }

    pub fn primary_venue(&self) -> &str {
        &self.primary.venue
    }

    pub fn confirmation_venue(&self) -> &str {
        &self.confirmation.venue
    }

    pub fn required_role_count(&self) -> usize {
        self.required_roles.len()
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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
    fn validate(self) -> Result<ProspectiveCaptureAdmissionV1, ProspectiveAdmissionError> {
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
        if self.primary.venue != "BINANCE"
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
        if self.confirmation.venue != "HYPERLIQUID"
            || self.confirmation.venue == self.primary.venue
            || self.confirmation.format != "MFR1"
            || !self.confirmation.public_read_only
            || self.confirmation.roles != ["CONFIRMATION"]
            || self.confirmation.families != [FamilyV1::ConfirmationPrice]
            || self.confirmation.instrument != self.primary.instrument
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
        {
            return Err(ProspectiveAdmissionError::SourceBinding);
        }
        self.authority.validate()?;

        Ok(ProspectiveCaptureAdmissionV1 {
            primary: self.primary,
            confirmation: self.confirmation,
            required_roles: self.required_roles,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceBinding {
    source_id: String,
    venue: String,
    format: String,
    instrument: CaptureInstrument,
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
        self.instrument.validate()?;
        if self.repository_url != REPOSITORY_URL
            || !valid_source_id(&self.source_id)
            || !is_lower_hex(&self.producer_commit, 40)
            || !valid_producer_path(&self.producer_path)
            || !is_lower_hex(&self.producer_blob_sha256, 64)
        {
            return Err(ProspectiveAdmissionError::SourceBinding);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct CaptureInstrument {
    base_asset: String,
    quote_asset: String,
    market_type: String,
}

impl CaptureInstrument {
    fn validate(&self) -> Result<(), ProspectiveAdmissionError> {
        if !matches!(
            self.base_asset.as_str(),
            "BTC" | "ETH" | "SOL" | "BNB" | "HYPE"
        ) || !matches!(self.quote_asset.as_str(), "USD" | "USDC" | "USDT")
            || self.market_type != "PERPETUAL"
            || self.base_asset == self.quote_asset
        {
            return Err(ProspectiveAdmissionError::SourceBinding);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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
