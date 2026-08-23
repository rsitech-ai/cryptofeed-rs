//! Pure in-memory preflight for the additive MechanicsInput V2 contract.

use std::collections::BTreeSet;

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    ArtifactRoleV1, CursorError, IngestOutcome, MechanicsInputRefV2, MechanicsInputV2,
    MechanicsInputV2JsonlReader, MechanicsInputV2JsonlWriter, ProspectiveCaptureAdmissionV2,
    ProspectiveSystemArtifactPolicyV2, ReplayInputError, SourceStateMachineV2,
    wire::{MAX_INPUT_BYTES, MechanicsInputRefV1, Rfc3339Time},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InMemoryArtifactV4 {
    role: ArtifactRoleV1,
    bytes: Vec<u8>,
    record_count: u64,
    byte_len: u64,
    sha256: String,
    first_available_at: Option<Rfc3339Time>,
    last_available_at: Option<Rfc3339Time>,
    record_identities: Vec<String>,
}

impl InMemoryArtifactV4 {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OfflineArtifactPreflightV4 {
    artifacts: Vec<InMemoryArtifactV4>,
}

impl OfflineArtifactPreflightV4 {
    pub fn build(
        admission: &ProspectiveCaptureAdmissionV2,
        policy: &ProspectiveSystemArtifactPolicyV2,
        decision_time: Rfc3339Time,
        complete_jsonl: &[u8],
    ) -> Result<Self, OfflineArtifactErrorV4> {
        if complete_jsonl.len() > MAX_INPUT_BYTES {
            return Err(OfflineArtifactErrorV4::AggregateTooLarge);
        }
        if !policy.matches(admission) {
            return Err(OfflineArtifactErrorV4::SystemPolicyMismatch);
        }
        if decision_time < *admission.capture_starts_at() {
            return Err(OfflineArtifactErrorV4::DecisionBeforeCaptureStart);
        }

        let inputs = MechanicsInputV2JsonlReader::new(complete_jsonl, decision_time).read_all()?;
        let mut classified = Vec::with_capacity(inputs.len());
        for input in &inputs {
            let (role, source) = classify(admission, input)?;
            if role == ArtifactRoleV1::System {
                return Err(OfflineArtifactErrorV4::NonEmptyTruthfulEmptySystem);
            }
            if starts_before(input, admission.capture_starts_at())? {
                return Err(OfflineArtifactErrorV4::InputBeforeCaptureStart(role));
            }
            classified.push((role, source));
        }

        let expected = admitted_sources(admission);
        let mut seen = BTreeSet::new();
        let mut identities = BTreeSet::new();
        let mut state = SourceStateMachineV2::new(admission.mechanics_config().clone());
        let mut partitions: [Vec<MechanicsInputV2>; 9] = std::array::from_fn(|_| Vec::new());
        for (input, (role, source)) in inputs.into_iter().zip(classified) {
            if !identities.insert(input.payload_hash().to_owned()) {
                return Err(OfflineArtifactErrorV4::DuplicateRecord);
            }
            if state
                .ingest(&input)
                .map_err(OfflineArtifactErrorV4::Topology)?
                == IngestOutcome::IgnoredDuplicate
            {
                return Err(OfflineArtifactErrorV4::DuplicateRecord);
            }
            seen.insert(source);
            partitions[role as usize].push(input);
        }
        if seen != expected {
            return Err(OfflineArtifactErrorV4::IncompleteTopology);
        }

        let mut artifacts = Vec::with_capacity(9);
        for role in ArtifactRoleV1::ALL {
            let records = std::mem::take(&mut partitions[role as usize]);
            if role == ArtifactRoleV1::System {
                artifacts.push(empty_system());
            } else if records.is_empty() {
                return Err(OfflineArtifactErrorV4::MissingRole(role));
            } else {
                artifacts.push(build_artifact(role, &records)?);
            }
        }
        Ok(Self { artifacts })
    }

    pub fn artifacts(&self) -> &[InMemoryArtifactV4] {
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
pub enum OfflineArtifactErrorV4 {
    #[error(transparent)]
    Replay(#[from] ReplayInputError),
    #[error("V2 input does not match admitted topology: {0}")]
    Topology(CursorError),
    #[error("market input cannot be assigned to an admitted artifact role")]
    UnsupportedMarketRole,
    #[error("input omits one or more configured topology sources")]
    IncompleteTopology,
    #[error("decision time precedes capture start")]
    DecisionBeforeCaptureStart,
    #[error("{0:?} input availability precedes capture start")]
    InputBeforeCaptureStart(ArtifactRoleV1),
    #[error("artifact role is empty: {0:?}")]
    MissingRole(ArtifactRoleV1),
    #[error("artifact arithmetic overflowed")]
    ReportOverflow,
    #[error("aggregate MechanicsInput V2 JSONL exceeds 16 MiB")]
    AggregateTooLarge,
    #[error("truthful-empty SYSTEM policy is not bound to this admission")]
    SystemPolicyMismatch,
    #[error("truthful-empty SYSTEM policy rejects all SYSTEM input")]
    NonEmptyTruthfulEmptySystem,
    #[error("duplicate input record is not a complete fixture")]
    DuplicateRecord,
}

fn classify(
    admission: &ProspectiveCaptureAdmissionV2,
    input: &MechanicsInputV2,
) -> Result<(ArtifactRoleV1, String), OfflineArtifactErrorV4> {
    match input.view() {
        MechanicsInputRefV2::Market {
            envelope, catalog, ..
        } => {
            let source = catalog
                .venue_source(envelope.venue.0)
                .ok_or(OfflineArtifactErrorV4::UnsupportedMarketRole)?
                .source_id();
            let role = match envelope.payload {
                marketfeed_model::MarketEvent::Trade(_) => ArtifactRoleV1::Trade,
                marketfeed_model::MarketEvent::Quote(_) => ArtifactRoleV1::Quote,
                marketfeed_model::MarketEvent::BookSnapshot(_)
                | marketfeed_model::MarketEvent::BookDelta(_) => ArtifactRoleV1::Book,
                marketfeed_model::MarketEvent::OpenInterest(_) => ArtifactRoleV1::OpenInterest,
                marketfeed_model::MarketEvent::Liquidation(_) => ArtifactRoleV1::Liquidation,
                marketfeed_model::MarketEvent::MarkPrice(_)
                | marketfeed_model::MarketEvent::IndexPrice(_) => ArtifactRoleV1::Confirmation,
                _ => return Err(OfflineArtifactErrorV4::UnsupportedMarketRole),
            };
            let configured = admission
                .mechanics_config()
                .contributors()
                .iter()
                .any(|spec| {
                    spec.key().source_id() == source
                        && spec.allowed_families().contains(&family(role))
                });
            if !configured {
                return Err(OfflineArtifactErrorV4::UnsupportedMarketRole);
            }
            Ok((role, source.to_owned()))
        }
        MechanicsInputRefV2::NonMarket(view) => match view {
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
            MechanicsInputRefV1::Market { .. } => {
                Err(OfflineArtifactErrorV4::UnsupportedMarketRole)
            }
        },
    }
}

fn family(role: ArtifactRoleV1) -> crate::wire::FamilyV1 {
    use crate::wire::FamilyV1;
    match role {
        ArtifactRoleV1::Trade => FamilyV1::Trade,
        ArtifactRoleV1::Quote => FamilyV1::Quote,
        ArtifactRoleV1::Book => FamilyV1::Book,
        ArtifactRoleV1::OpenInterest => FamilyV1::OpenInterest,
        ArtifactRoleV1::Liquidation => FamilyV1::Liquidation,
        ArtifactRoleV1::Confirmation => FamilyV1::ConfirmationPrice,
        ArtifactRoleV1::Clock | ArtifactRoleV1::Coverage | ArtifactRoleV1::System => {
            unreachable!("market roles only")
        }
    }
}

fn admitted_sources(admission: &ProspectiveCaptureAdmissionV2) -> BTreeSet<String> {
    let config = admission.mechanics_config();
    config
        .contributors()
        .iter()
        .map(|v| v.key().source_id().to_owned())
        .chain(
            config
                .clock_sources()
                .iter()
                .map(|v| v.source_id().to_owned()),
        )
        .chain(
            config
                .coverage_sources()
                .iter()
                .map(|v| v.source_id().to_owned()),
        )
        .collect()
}

fn available_at(input: &MechanicsInputV2) -> Result<Rfc3339Time, OfflineArtifactErrorV4> {
    match input.view() {
        MechanicsInputRefV2::Market { envelope, .. } => {
            Rfc3339Time::from_unix_nanos(envelope.receive_ts.0)
                .map_err(|_| OfflineArtifactErrorV4::ReportOverflow)
        }
        MechanicsInputRefV2::NonMarket(
            MechanicsInputRefV1::Clock { available_at, .. }
            | MechanicsInputRefV1::Coverage { available_at, .. }
            | MechanicsInputRefV1::System { available_at, .. },
        ) => Ok(available_at.clone()),
        MechanicsInputRefV2::NonMarket(MechanicsInputRefV1::Market { .. }) => {
            Err(OfflineArtifactErrorV4::UnsupportedMarketRole)
        }
    }
}

fn starts_before(
    input: &MechanicsInputV2,
    start: &Rfc3339Time,
) -> Result<bool, OfflineArtifactErrorV4> {
    let start_us = start.utc_micros();
    Ok(match input.view() {
        MechanicsInputRefV2::Market {
            envelope,
            source_provenance,
            ..
        } => {
            let exchange_us = envelope
                .exchange_ts
                .ok_or(OfflineArtifactErrorV4::ReportOverflow)?
                .0
                .div_euclid(1_000);
            let source_ms = match source_provenance {
                crate::SourceProvenanceV2::None => None,
                crate::SourceProvenanceV2::BinanceBookTicker {
                    transaction_time_ms,
                    ..
                }
                | crate::SourceProvenanceV2::BinanceBookDelta {
                    transaction_time_ms,
                    ..
                }
                | crate::SourceProvenanceV2::BinanceBookSnapshot {
                    transaction_time_ms,
                    ..
                } => Some(*transaction_time_ms),
                crate::SourceProvenanceV2::BinanceAggregateTrade { trade_time_ms, .. } => {
                    Some(*trade_time_ms)
                }
                crate::SourceProvenanceV2::BinanceOpenInterest { source_time_ms } => {
                    Some(*source_time_ms)
                }
                crate::SourceProvenanceV2::BinanceForceOrder {
                    order_trade_time_ms,
                    ..
                } => Some(*order_trade_time_ms),
            };
            envelope.receive_ts.0.div_euclid(1_000) < start_us
                || exchange_us < start_us
                || source_ms.is_some_and(|ms| {
                    i64::try_from(ms)
                        .ok()
                        .and_then(|ms| ms.checked_mul(1_000))
                        .is_none_or(|source_us| source_us < start_us)
                })
        }
        MechanicsInputRefV2::NonMarket(MechanicsInputRefV1::Clock {
            observed_at,
            available_at,
            ..
        }) => observed_at < start || available_at < start,
        MechanicsInputRefV2::NonMarket(MechanicsInputRefV1::Coverage {
            covered_from,
            covered_through,
            available_at,
            ..
        }) => covered_from < start || covered_through < start || available_at < start,
        MechanicsInputRefV2::NonMarket(MechanicsInputRefV1::System {
            occurred_at,
            available_at,
            ..
        }) => occurred_at < start || available_at < start,
        MechanicsInputRefV2::NonMarket(MechanicsInputRefV1::Market { .. }) => true,
    })
}

fn build_artifact(
    role: ArtifactRoleV1,
    records: &[MechanicsInputV2],
) -> Result<InMemoryArtifactV4, OfflineArtifactErrorV4> {
    let mut writer = MechanicsInputV2JsonlWriter::new(Vec::new());
    for record in records {
        writer.write_input(record)?;
    }
    let bytes = writer.finish();
    let first = available_at(&records[0])?;
    let last = available_at(&records[records.len() - 1])?;
    let decoded = MechanicsInputV2JsonlReader::new(bytes.as_slice(), last.clone()).read_all()?;
    if decoded != records {
        return Err(OfflineArtifactErrorV4::Replay(
            ReplayInputError::InvalidInput(
                "artifact strict readback differs from staged records".to_owned(),
            ),
        ));
    }
    Ok(InMemoryArtifactV4 {
        role,
        record_count: u64::try_from(records.len())
            .map_err(|_| OfflineArtifactErrorV4::ReportOverflow)?,
        byte_len: u64::try_from(bytes.len()).map_err(|_| OfflineArtifactErrorV4::ReportOverflow)?,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        bytes,
        first_available_at: Some(first),
        last_available_at: Some(last),
        record_identities: records
            .iter()
            .map(|record| record.payload_hash().to_owned())
            .collect(),
    })
}

fn empty_system() -> InMemoryArtifactV4 {
    InMemoryArtifactV4 {
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
