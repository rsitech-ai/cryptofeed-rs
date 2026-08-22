//! Fail-closed admission for a future prospective EventPulse capture.
//!
//! This module does not capture data or author evidence. It only proves that a
//! proposed capture topology has the independent sources required by the E2
//! contract and remains below every execution authority boundary.

use serde::Deserialize;
use thiserror::Error;

use crate::wire::Rfc3339Time;

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
    clock: ClockBinding,
    coverage: CoverageBinding,
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
        if starts_at <= boundary {
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
        {
            return Err(ProspectiveAdmissionError::PrimarySource);
        }
        if self.confirmation.venue != "HYPERLIQUID"
            || self.confirmation.venue == self.primary.venue
            || self.confirmation.format != "MFR1"
            || !self.confirmation.public_read_only
        {
            return Err(ProspectiveAdmissionError::ConfirmationSource);
        }
        if self.clock.evidence_kind != "UTC_MONOTONIC_OBSERVATION"
            || self.clock.derivation != "INDEPENDENT_SIDECAR"
        {
            return Err(ProspectiveAdmissionError::ClockEvidence);
        }
        if self.coverage.evidence_kind != "EXPLICIT_HEARTBEAT_RANGE"
            || self.coverage.derivation != "INDEPENDENT_SIDECAR"
        {
            return Err(ProspectiveAdmissionError::CoverageEvidence);
        }
        if self.system.evidence_kind != "STABLE_SYSTEM_FAULT_MAPPING" {
            return Err(ProspectiveAdmissionError::SystemEvidence);
        }
        self.primary.validate()?;
        self.confirmation.validate()?;
        self.clock.validate()?;
        self.coverage.validate()?;
        self.system.validate()?;
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
    venue: String,
    format: String,
    public_read_only: bool,
    repository_url: String,
    producer_commit: String,
    producer_blob_sha256: String,
}

impl SourceBinding {
    fn validate(&self) -> Result<(), ProspectiveAdmissionError> {
        if self.repository_url != REPOSITORY_URL
            || !is_lower_hex(&self.producer_commit, 40)
            || !is_lower_hex(&self.producer_blob_sha256, 64)
        {
            return Err(ProspectiveAdmissionError::SourceBinding);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClockBinding {
    evidence_kind: String,
    derivation: String,
    producer_commit: String,
    producer_blob_sha256: String,
}

impl ClockBinding {
    fn validate(&self) -> Result<(), ProspectiveAdmissionError> {
        validate_local_binding(&self.producer_commit, &self.producer_blob_sha256)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct CoverageBinding {
    evidence_kind: String,
    derivation: String,
    producer_commit: String,
    producer_blob_sha256: String,
}

impl CoverageBinding {
    fn validate(&self) -> Result<(), ProspectiveAdmissionError> {
        validate_local_binding(&self.producer_commit, &self.producer_blob_sha256)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct SystemBinding {
    evidence_kind: String,
    producer_commit: String,
    producer_blob_sha256: String,
}

impl SystemBinding {
    fn validate(&self) -> Result<(), ProspectiveAdmissionError> {
        validate_local_binding(&self.producer_commit, &self.producer_blob_sha256)
    }
}

fn validate_local_binding(commit: &str, blob: &str) -> Result<(), ProspectiveAdmissionError> {
    if !is_lower_hex(commit, 40) || !is_lower_hex(blob, 64) {
        return Err(ProspectiveAdmissionError::SourceBinding);
    }
    Ok(())
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
