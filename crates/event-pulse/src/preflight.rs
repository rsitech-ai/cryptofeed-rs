//! Pure, offline preflight for a complete prospective EPIN-JSON1 stream.
//!
//! The output is deliberately in memory and remains blocked from evidence
//! authorship. This module performs no capture, transport, persistence, or
//! manifest/package work.

use std::collections::BTreeSet;

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    CursorError, EpinJson1Reader, EpinJson1Writer, ProspectiveCaptureAdmissionV1,
    ProspectiveSystemArtifactPolicyV1, ReplayInputError, SourceStateMachine,
    wire::{MAX_INPUT_BYTES, MechanicsInputRefV1, MechanicsInputV1, Rfc3339Time},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ArtifactRoleV1 {
    Trade,
    Quote,
    Book,
    OpenInterest,
    Liquidation,
    Confirmation,
    Clock,
    Coverage,
    System,
}

impl ArtifactRoleV1 {
    pub const ALL: [Self; 9] = [
        Self::Trade,
        Self::Quote,
        Self::Book,
        Self::OpenInterest,
        Self::Liquidation,
        Self::Confirmation,
        Self::Clock,
        Self::Coverage,
        Self::System,
    ];

    const fn index(self) -> usize {
        self as usize
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trade => "TRADE",
            Self::Quote => "QUOTE",
            Self::Book => "BOOK",
            Self::OpenInterest => "OPEN_INTEREST",
            Self::Liquidation => "LIQUIDATION",
            Self::Confirmation => "CONFIRMATION",
            Self::Clock => "CLOCK",
            Self::Coverage => "COVERAGE",
            Self::System => "SYSTEM",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InMemoryArtifactV1 {
    role: ArtifactRoleV1,
    bytes: Vec<u8>,
    record_count: u64,
    byte_len: u64,
    sha256: String,
    first_available_at: Rfc3339Time,
    last_available_at: Rfc3339Time,
}

impl InMemoryArtifactV1 {
    pub const fn role(&self) -> ArtifactRoleV1 {
        self.role
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub const fn record_count(&self) -> u64 {
        self.record_count
    }

    pub const fn byte_len(&self) -> u64 {
        self.byte_len
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub fn first_available_at(&self) -> &Rfc3339Time {
        &self.first_available_at
    }

    pub fn last_available_at(&self) -> &Rfc3339Time {
        &self.last_available_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OfflineArtifactPreflightV1 {
    artifacts: Vec<InMemoryArtifactV1>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InMemoryArtifactV3 {
    role: ArtifactRoleV1,
    bytes: Vec<u8>,
    record_count: u64,
    byte_len: u64,
    sha256: String,
    first_available_at: Option<Rfc3339Time>,
    last_available_at: Option<Rfc3339Time>,
    record_identities: Vec<String>,
}

impl InMemoryArtifactV3 {
    pub const fn role(&self) -> ArtifactRoleV1 {
        self.role
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub const fn record_count(&self) -> u64 {
        self.record_count
    }

    pub const fn byte_len(&self) -> u64 {
        self.byte_len
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub fn first_available_at(&self) -> Option<&Rfc3339Time> {
        self.first_available_at.as_ref()
    }

    pub fn last_available_at(&self) -> Option<&Rfc3339Time> {
        self.last_available_at.as_ref()
    }

    pub fn record_identities(&self) -> &[String] {
        &self.record_identities
    }
}

/// Append-only preflight for the v3 truthful-empty SYSTEM policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OfflineArtifactPreflightV3 {
    artifacts: Vec<InMemoryArtifactV3>,
}

impl OfflineArtifactPreflightV3 {
    pub fn build(
        admission: &ProspectiveCaptureAdmissionV1,
        system_policy: &ProspectiveSystemArtifactPolicyV1,
        decision_time: Rfc3339Time,
        complete_epin_json1: &[u8],
    ) -> Result<Self, OfflineArtifactError> {
        if complete_epin_json1.len() > MAX_INPUT_BYTES {
            return Err(OfflineArtifactError::AggregateTooLarge);
        }
        if !system_policy.matches(admission) {
            return Err(OfflineArtifactError::SystemPolicyMismatch);
        }
        if decision_time < *admission.capture_starts_at() {
            return Err(OfflineArtifactError::DecisionBeforeCaptureStart);
        }
        let inputs = EpinJson1Reader::new(complete_epin_json1, decision_time).read_all()?;
        let mut classified = Vec::with_capacity(inputs.len());
        for input in &inputs {
            let (role, source_id) = classify(admission, input)?;
            if role == ArtifactRoleV1::System {
                return Err(OfflineArtifactError::NonEmptyTruthfulEmptySystem);
            }
            if available_at(input)? < *admission.capture_starts_at() {
                return Err(OfflineArtifactError::InputBeforeCaptureStart(role));
            }
            classified.push((role, source_id));
        }

        let expected_sources = admitted_sources(admission)
            .into_iter()
            .filter(|source| source != system_policy.source_id())
            .collect::<BTreeSet<_>>();
        let mut seen_sources = BTreeSet::new();
        let mut state = SourceStateMachine::new(admission.mechanics_config().clone());
        let mut partitions: [Vec<MechanicsInputV1>; 9] = std::array::from_fn(|_| Vec::new());
        for (input, (role, source_id)) in inputs.into_iter().zip(classified) {
            state
                .ingest(&input)
                .map_err(OfflineArtifactError::Topology)?;
            seen_sources.insert(source_id);
            partitions[role.index()].push(input);
        }
        if seen_sources != expected_sources {
            return Err(OfflineArtifactError::IncompleteTopology);
        }

        let mut artifacts = Vec::with_capacity(ArtifactRoleV1::ALL.len());
        for role in ArtifactRoleV1::ALL {
            let records = std::mem::take(&mut partitions[role.index()]);
            if role == ArtifactRoleV1::System {
                artifacts.push(empty_system_artifact());
            } else {
                if records.is_empty() {
                    return Err(OfflineArtifactError::MissingRole(role));
                }
                artifacts.push(build_artifact_v3(role, &records)?);
            }
        }
        Ok(Self { artifacts })
    }

    pub fn artifacts(&self) -> &[InMemoryArtifactV3] {
        &self.artifacts
    }

    pub const fn evidence_authoring_allowed(&self) -> bool {
        false
    }

    pub const fn blocker(&self) -> &'static str {
        "blocked:fixture-provenance"
    }
}

impl OfflineArtifactPreflightV1 {
    pub fn build(
        admission: &ProspectiveCaptureAdmissionV1,
        decision_time: Rfc3339Time,
        complete_epin_json1: &[u8],
    ) -> Result<Self, OfflineArtifactError> {
        if decision_time < *admission.capture_starts_at() {
            return Err(OfflineArtifactError::DecisionBeforeCaptureStart);
        }
        let inputs = EpinJson1Reader::new(complete_epin_json1, decision_time).read_all()?;
        let mut classified = Vec::with_capacity(inputs.len());
        for input in &inputs {
            let (role, source_id) = classify(admission, input)?;
            if available_at(input)? < *admission.capture_starts_at() {
                return Err(OfflineArtifactError::InputBeforeCaptureStart(role));
            }
            classified.push((role, source_id));
        }

        let expected_sources = admitted_sources(admission);
        let mut seen_sources = BTreeSet::new();
        let mut state = SourceStateMachine::new(admission.mechanics_config().clone());
        let mut partitions: [Vec<MechanicsInputV1>; 9] = std::array::from_fn(|_| Vec::new());

        for (input, (role, source_id)) in inputs.into_iter().zip(classified) {
            state
                .ingest(&input)
                .map_err(OfflineArtifactError::Topology)?;
            seen_sources.insert(source_id);
            partitions[role.index()].push(input);
        }
        if seen_sources != expected_sources {
            return Err(OfflineArtifactError::IncompleteTopology);
        }

        let mut artifacts = Vec::with_capacity(ArtifactRoleV1::ALL.len());
        for role in ArtifactRoleV1::ALL {
            let records = std::mem::take(&mut partitions[role.index()]);
            if records.is_empty() {
                return Err(OfflineArtifactError::MissingRole(role));
            }
            artifacts.push(build_artifact(role, &records)?);
        }
        Ok(Self { artifacts })
    }

    pub fn artifacts(&self) -> &[InMemoryArtifactV1] {
        &self.artifacts
    }

    pub const fn evidence_authoring_allowed(&self) -> bool {
        false
    }

    pub const fn blocker(&self) -> &'static str {
        "blocked:fixture-provenance"
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum OfflineArtifactError {
    #[error(transparent)]
    Replay(#[from] ReplayInputError),
    #[error("EPIN input does not match the admitted topology: {0}")]
    Topology(CursorError),
    #[error("market input cannot be assigned to an admitted artifact role")]
    UnsupportedMarketRole,
    #[error("EPIN input omits one or more configured topology sources")]
    IncompleteTopology,
    #[error("decision time precedes the admitted prospective capture start")]
    DecisionBeforeCaptureStart,
    #[error("{0:?} input availability precedes the admitted prospective capture start")]
    InputBeforeCaptureStart(ArtifactRoleV1),
    #[error("artifact role is empty: {0:?}")]
    MissingRole(ArtifactRoleV1),
    #[error("artifact report arithmetic overflowed")]
    ReportOverflow,
    #[error("aggregate EPIN-JSON1 input exceeds 16 MiB")]
    AggregateTooLarge,
    #[error("truthful-empty SYSTEM policy is not bound to this admission")]
    SystemPolicyMismatch,
    #[error("truthful-empty SYSTEM policy rejects all SYSTEM input")]
    NonEmptyTruthfulEmptySystem,
}

fn classify(
    admission: &ProspectiveCaptureAdmissionV1,
    input: &MechanicsInputV1,
) -> Result<(ArtifactRoleV1, String), OfflineArtifactError> {
    match input.view() {
        MechanicsInputRefV1::Market {
            envelope, catalog, ..
        } => {
            let source = catalog
                .venue_source(envelope.venue.0)
                .ok_or(OfflineArtifactError::UnsupportedMarketRole)?
                .source_id();
            if source == admission.primary_source_id() {
                let role = match envelope.payload {
                    marketfeed_model::MarketEvent::Trade(_) => ArtifactRoleV1::Trade,
                    marketfeed_model::MarketEvent::Quote(_) => ArtifactRoleV1::Quote,
                    marketfeed_model::MarketEvent::BookSnapshot(_)
                    | marketfeed_model::MarketEvent::BookDelta(_) => ArtifactRoleV1::Book,
                    marketfeed_model::MarketEvent::OpenInterest(_) => ArtifactRoleV1::OpenInterest,
                    marketfeed_model::MarketEvent::Liquidation(_) => ArtifactRoleV1::Liquidation,
                    _ => return Err(OfflineArtifactError::UnsupportedMarketRole),
                };
                Ok((role, source.to_owned()))
            } else if source == admission.confirmation_source_id()
                && matches!(
                    envelope.payload,
                    marketfeed_model::MarketEvent::Trade(_)
                        | marketfeed_model::MarketEvent::Quote(_)
                )
            {
                Ok((ArtifactRoleV1::Confirmation, source.to_owned()))
            } else {
                Err(OfflineArtifactError::UnsupportedMarketRole)
            }
        }
        MechanicsInputRefV1::Clock { clock_source, .. } => Ok((
            ArtifactRoleV1::Clock,
            clock_source.key().source_id().to_owned(),
        )),
        MechanicsInputRefV1::Coverage {
            coverage_source, ..
        } => Ok((
            ArtifactRoleV1::Coverage,
            coverage_source.key().source_id().to_owned(),
        )),
        MechanicsInputRefV1::System { system_source, .. } => Ok((
            ArtifactRoleV1::System,
            system_source.key().source_id().to_owned(),
        )),
    }
}

fn admitted_sources(admission: &ProspectiveCaptureAdmissionV1) -> BTreeSet<String> {
    let config = admission.mechanics_config();
    config
        .contributors()
        .iter()
        .map(|source| source.key().source_id().to_owned())
        .chain(
            config
                .clock_sources()
                .iter()
                .map(|source| source.source_id().to_owned()),
        )
        .chain(
            config
                .coverage_sources()
                .iter()
                .map(|source| source.source_id().to_owned()),
        )
        .chain(
            config
                .system_sources()
                .iter()
                .map(|source| source.source_id().to_owned()),
        )
        .collect()
}

fn available_at(input: &MechanicsInputV1) -> Result<Rfc3339Time, OfflineArtifactError> {
    match input.view() {
        MechanicsInputRefV1::Market { envelope, .. } => {
            Rfc3339Time::from_unix_nanos(envelope.receive_ts.0)
                .map_err(|_| OfflineArtifactError::ReportOverflow)
        }
        MechanicsInputRefV1::System { available_at, .. }
        | MechanicsInputRefV1::Coverage { available_at, .. }
        | MechanicsInputRefV1::Clock { available_at, .. } => Ok(available_at.clone()),
    }
}

fn build_artifact(
    role: ArtifactRoleV1,
    records: &[MechanicsInputV1],
) -> Result<InMemoryArtifactV1, OfflineArtifactError> {
    let mut writer = EpinJson1Writer::new(Vec::new());
    for record in records {
        writer.write_input(record)?;
    }
    let bytes = writer.finish();
    let record_count =
        u64::try_from(records.len()).map_err(|_| OfflineArtifactError::ReportOverflow)?;
    let byte_len = u64::try_from(bytes.len()).map_err(|_| OfflineArtifactError::ReportOverflow)?;
    let first_available_at = available_at(&records[0])?;
    let last_available_at = available_at(&records[records.len() - 1])?;
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    Ok(InMemoryArtifactV1 {
        role,
        bytes,
        record_count,
        byte_len,
        sha256,
        first_available_at,
        last_available_at,
    })
}

fn build_artifact_v3(
    role: ArtifactRoleV1,
    records: &[MechanicsInputV1],
) -> Result<InMemoryArtifactV3, OfflineArtifactError> {
    let artifact = build_artifact(role, records)?;
    let decoded =
        EpinJson1Reader::new(artifact.bytes(), artifact.last_available_at().clone()).read_all()?;
    if decoded != records {
        return Err(OfflineArtifactError::Replay(
            ReplayInputError::InvalidInput(
                "artifact strict readback differs from staged records".to_owned(),
            ),
        ));
    }
    Ok(InMemoryArtifactV3 {
        role,
        bytes: artifact.bytes,
        record_count: artifact.record_count,
        byte_len: artifact.byte_len,
        sha256: artifact.sha256,
        first_available_at: Some(artifact.first_available_at),
        last_available_at: Some(artifact.last_available_at),
        record_identities: records
            .iter()
            .map(|record| record.payload_hash().to_owned())
            .collect(),
    })
}

fn empty_system_artifact() -> InMemoryArtifactV3 {
    InMemoryArtifactV3 {
        role: ArtifactRoleV1::System,
        bytes: Vec::new(),
        record_count: 0,
        byte_len: 0,
        sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_owned(),
        first_available_at: None,
        last_available_at: None,
        record_identities: Vec::new(),
    }
}
