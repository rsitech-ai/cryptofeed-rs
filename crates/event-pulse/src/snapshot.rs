//! Atomic canonical EventPulse mechanics snapshot authorship.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};
use thiserror::Error;

use crate::{
    ContractBundle, ValidatedContract, content_hash,
    features::{
        Direction, EnvelopeQuality, FeatureName, FeatureQuality, FeatureSet, FlagConditions,
        MechanicsFlag, SCALE, canonical_decimal, envelope_quality, mechanics_flags,
    },
    mechanics::{FamilyFlags, MechanicsEvidence, Phase, PhaseError, PhaseMachine},
    wire::{Rfc3339Time, SnapshotAuthoringV1},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketAnchor {
    pub source_event_time: Rfc3339Time,
    pub received_at: Rfc3339Time,
    pub normalized_at: Rfc3339Time,
    pub available_at: Rfc3339Time,
    pub payload_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotCursor {
    pub source_id: String,
    pub connection_epoch: String,
    pub sequence_start: u64,
    pub sequence_end: u64,
    pub available_at: Rfc3339Time,
    pub payload_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClockEvidence {
    pub source_id: String,
    pub available_at: Rfc3339Time,
    /// Fixed point S=1e8 milliseconds.
    pub observed_skew_ms: i128,
    pub freshness_limit_ms: u64,
    pub degraded: bool,
}

#[derive(Debug, Clone)]
pub struct SnapshotObservation {
    pub available_at: Rfc3339Time,
    pub features: FeatureSet,
    pub flag_conditions: FlagConditions,
    pub liquidation_confirms_direction: bool,
    pub fully_warmed: bool,
    pub anchor: Option<MarketAnchor>,
    pub cursors: Vec<SnapshotCursor>,
    pub required_clock_sources: Vec<String>,
    pub clocks: Vec<ClockEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SnapshotError {
    #[error("input availability decreased")]
    InputTimeRegression,
    #[error("input belongs to an already sealed prefix")]
    SealedInput,
    #[error("snapshot decision time decreased")]
    DecisionTimeRegression,
    #[error("snapshot decision precedes contributing availability")]
    FutureAvailability,
    #[error("snapshot has no causal market anchor")]
    MissingCausalAnchor,
    #[error("retained causal market anchor is stale")]
    StaleCausalAnchor,
    #[error("complete fresh clock evidence is missing")]
    MissingClockEvidence,
    #[error("causal timestamps are not monotonic")]
    InvalidCausalTime,
    #[error("source cursor provenance is ambiguous")]
    CursorConflict,
    #[error("revision overflow")]
    RevisionOverflow,
    #[error("mechanics phase update failed: {0}")]
    Phase(String),
    #[error("canonical contract authorship failed: {0}")]
    Contract(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuthoredSnapshot {
    contract: ValidatedContract,
    revision: u64,
    predecessor: Option<String>,
}

impl AuthoredSnapshot {
    pub fn canonical_json(&self) -> String {
        self.contract.canonical_json()
    }
    pub fn content_hash(&self) -> &str {
        self.contract.value()["content_hash"]
            .as_str()
            .expect("validated E1 hash is a string")
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn predecessor_content_hash(&self) -> Option<&str> {
        self.predecessor.as_deref()
    }
    pub fn value(&self) -> &Value {
        self.contract.value()
    }
}

#[derive(Debug, Clone)]
struct SuccessfulCache {
    decision_micros: i64,
    snapshot: AuthoredSnapshot,
}

#[derive(Debug, Clone)]
pub struct MechanicsProcessor {
    authoring: SnapshotAuthoringV1,
    pending: Vec<SnapshotObservation>,
    current: Option<SnapshotObservation>,
    phase: PhaseMachine,
    last_input_micros: Option<i64>,
    sealed_micros: Option<i64>,
    last_decision_micros: Option<i64>,
    next_revision: u64,
    predecessor: Option<String>,
    cache: Option<SuccessfulCache>,
}

impl MechanicsProcessor {
    pub fn new(authoring: SnapshotAuthoringV1) -> Self {
        let next_revision = authoring.revision_start();
        let predecessor = authoring.predecessor_content_hash().map(str::to_owned);
        Self {
            authoring,
            pending: Vec::new(),
            current: None,
            phase: PhaseMachine::new(),
            last_input_micros: None,
            sealed_micros: None,
            last_decision_micros: None,
            next_revision,
            predecessor,
            cache: None,
        }
    }

    pub fn next_revision(&self) -> u64 {
        self.next_revision
    }

    pub fn ingest(&mut self, observation: SnapshotObservation) -> Result<(), SnapshotError> {
        let at = observation.available_at.utc_micros();
        if self.sealed_micros.is_some_and(|sealed| at <= sealed) {
            return Err(SnapshotError::SealedInput);
        }
        if self.last_input_micros.is_some_and(|last| at < last) {
            return Err(SnapshotError::InputTimeRegression);
        }
        self.last_input_micros = Some(at);
        self.pending.push(observation);
        Ok(())
    }

    pub fn snapshot(
        &mut self,
        decision_time: Rfc3339Time,
    ) -> Result<AuthoredSnapshot, SnapshotError> {
        let decision_micros = decision_time.utc_micros();
        if let Some(cache) = &self.cache {
            if cache.decision_micros == decision_micros {
                return Ok(cache.snapshot.clone());
            }
        }
        if self
            .last_decision_micros
            .is_some_and(|last| decision_micros < last)
        {
            return Err(SnapshotError::DecisionTimeRegression);
        }

        let eligible = self
            .pending
            .iter()
            .filter(|observation| observation.available_at.utc_micros() <= decision_micros)
            .cloned()
            .collect::<Vec<_>>();
        if eligible.is_empty() && self.current.is_none() {
            return Err(SnapshotError::MissingCausalAnchor);
        }
        let mut phase = self.phase.clone();
        for group in equal_time_groups(&eligible) {
            let aggregate = aggregate_group(group)?;
            phase
                .observe(&derive_evidence(&aggregate)?)
                .map_err(phase_error)?;
        }
        phase
            .advance_to(
                decision_micros
                    .checked_mul(1_000)
                    .ok_or_else(|| SnapshotError::Phase("decision nanoseconds overflow".into()))?,
            )
            .map_err(phase_error)?;
        let aggregate = match &self.current {
            Some(current) => {
                let mut observations = Vec::with_capacity(eligible.len() + 1);
                observations.push(current.clone());
                observations.extend(eligible.iter().cloned());
                aggregate_all(&observations)?
            }
            None => aggregate_all(&eligible)?,
        };
        let following_revision = self
            .next_revision
            .checked_add(1)
            .ok_or(SnapshotError::RevisionOverflow)?;
        let snapshot = self.author(&decision_time, &aggregate, &phase)?;

        let sealed = decision_micros;
        self.pending
            .retain(|observation| observation.available_at.utc_micros() > sealed);
        self.phase = phase;
        self.current = Some(aggregate);
        self.sealed_micros = Some(sealed);
        self.last_decision_micros = Some(decision_micros);
        self.predecessor = Some(snapshot.content_hash().to_owned());
        self.next_revision = following_revision;
        self.cache = Some(SuccessfulCache {
            decision_micros,
            snapshot: snapshot.clone(),
        });
        Ok(snapshot)
    }

    fn author(
        &self,
        decision_time: &Rfc3339Time,
        observation: &SnapshotObservation,
        phase: &PhaseMachine,
    ) -> Result<AuthoredSnapshot, SnapshotError> {
        let anchor = observation
            .anchor
            .as_ref()
            .ok_or(SnapshotError::MissingCausalAnchor)?;
        let available_micros = observation.available_at.utc_micros();
        if decision_time.utc_micros() < available_micros {
            return Err(SnapshotError::FutureAvailability);
        }
        if !(anchor.source_event_time.utc_micros() <= anchor.received_at.utc_micros()
            && anchor.received_at.utc_micros() <= anchor.normalized_at.utc_micros()
            && anchor.normalized_at.utc_micros() <= available_micros)
        {
            return Err(SnapshotError::InvalidCausalTime);
        }
        let clock = aggregate_clock(observation, decision_time, anchor)?;
        let evidence = derive_evidence(observation)?;
        let flags = mechanics_flags(&observation.flag_conditions);
        let feature_quality = envelope_quality(&observation.features);
        let invalid = !evidence.valid || phase.phase() == Phase::Invalid;
        if invalid && flags.is_empty() {
            return Err(SnapshotError::Contract(
                "invalid mechanics has no truthful E1 flag".into(),
            ));
        }
        let quality = if invalid {
            "INVALID"
        } else if feature_quality == EnvelopeQuality::Degraded || clock.degraded {
            "DEGRADED"
        } else {
            "VALIDATED"
        };
        let effective_flags = {
            let mut flags = flags;
            if clock.degraded {
                flags.push(MechanicsFlag::ClockUncertain);
                flags.sort();
                flags.dedup();
            }
            flags
        };
        let (intensity, confidence, reversal) = if invalid {
            (0, 0, 0)
        } else {
            (
                evidence.intensity,
                evidence.confidence,
                evidence.reversal_risk,
            )
        };
        let mut value = json!({
            "schema_version": "event-pulse/1.0",
            "contract_type": "mechanics",
            "contract_id": self.authoring.contract_id(),
            "lineage_id": self.authoring.lineage_id(),
            "revision": self.next_revision,
            "predecessor_content_hash": self.predecessor,
            "causal_time": {
                "source_event_time": anchor.source_event_time.canonical(),
                "received_at": anchor.received_at.canonical(),
                "normalized_at": anchor.normalized_at.canonical(),
                "available_at": observation.available_at.canonical(),
                "decision_time": decision_time.canonical(),
                "clock_quality": {
                    "source_id": "event_pulse_clock_aggregate",
                    "observed_skew_ms": canonical_decimal(clock.max_abs_skew),
                    "freshness_limit_ms": clock.freshness_limit_ms,
                    "clock_state": if clock.degraded { "degraded" } else { "synchronized" },
                    "quality_state": if clock.degraded { "degraded" } else { "validated" },
                    "reason_code": if clock.degraded { "SOURCE_CLOCK_DEGRADED" } else { "ALL_SOURCE_CLOCKS_WITHIN_TOLERANCE" }
                }
            },
            "producer": "cryptofeed_rs",
            "event_cluster_id": self.authoring.event_cluster_id(),
            "scope": scope(self.authoring.primary_scope()),
            "phase": if invalid { Phase::Invalid.as_str() } else { phase.phase().as_str() },
            "event_type": event_type(if invalid { Phase::Invalid } else { phase.phase() }, &evidence),
            "direction": direction(if invalid { Direction::Unknown } else { evidence.direction }),
            "mechanical_intensity": canonical_decimal(intensity),
            "mechanical_confidence": canonical_decimal(confidence),
            "reversal_risk": canonical_decimal(reversal),
            "quality_state": quality,
            "source_qualification": "UNVERIFIED",
            "quality_flags": effective_flags.iter().map(|flag| flag_string(*flag)).collect::<Vec<_>>(),
            "expected_half_life_ms": self.authoring.expected_half_life_ms(),
            "features": feature_json(&observation.features),
            "source_cursors": cursor_json(&observation.cursors)?,
        });
        let hash =
            content_hash(&value).map_err(|error| SnapshotError::Contract(error.to_string()))?;
        value["content_hash"] = Value::String(hash);
        let bytes = crate::canonical_json(&value);
        let bundle = ContractBundle::load_embedded()
            .map_err(|error| SnapshotError::Contract(error.to_string()))?;
        let contract = bundle
            .validate_e1_json(bytes.as_bytes())
            .map_err(|error| SnapshotError::Contract(error.to_string()))?;
        crate::validate_e2_mechanics_profile(&contract)
            .map_err(|error| SnapshotError::Contract(error.to_string()))?;
        Ok(AuthoredSnapshot {
            contract,
            revision: self.next_revision,
            predecessor: self.predecessor.clone(),
        })
    }
}

fn phase_error(error: PhaseError) -> SnapshotError {
    SnapshotError::Phase(error.to_string())
}

fn equal_time_groups(observations: &[SnapshotObservation]) -> Vec<&[SnapshotObservation]> {
    let mut groups = Vec::new();
    let mut start = 0;
    for index in 1..=observations.len() {
        if index == observations.len()
            || observations[index].available_at.utc_micros()
                != observations[start].available_at.utc_micros()
        {
            groups.push(&observations[start..index]);
            start = index;
        }
    }
    groups
}

fn aggregate_group(group: &[SnapshotObservation]) -> Result<SnapshotObservation, SnapshotError> {
    aggregate_all(group)
}

fn aggregate_all(
    observations: &[SnapshotObservation],
) -> Result<SnapshotObservation, SnapshotError> {
    let latest = observations.last().expect("nonempty aggregate");
    let mut cursors = BTreeMap::new();
    let mut clocks = BTreeMap::new();
    let mut required = BTreeSet::new();
    let mut anchor: Option<MarketAnchor> = None;
    let mut flags = FlagConditions::default();
    for observation in observations {
        if let Some(candidate) = &observation.anchor {
            anchor = Some(match anchor {
                None => candidate.clone(),
                Some(current) => MarketAnchor {
                    source_event_time: max_time(
                        &current.source_event_time,
                        &candidate.source_event_time,
                    ),
                    received_at: max_time(&current.received_at, &candidate.received_at),
                    normalized_at: max_time(&current.normalized_at, &candidate.normalized_at),
                    available_at: max_time(&current.available_at, &candidate.available_at),
                    payload_hash: if candidate.available_at.utc_micros()
                        >= current.available_at.utc_micros()
                    {
                        candidate.payload_hash.clone()
                    } else {
                        current.payload_hash
                    },
                },
            });
        }
        merge_flags(&mut flags, &observation.flag_conditions);
        for cursor in &observation.cursors {
            cursors.insert(cursor.source_id.clone(), cursor.clone());
        }
        for source in &observation.required_clock_sources {
            required.insert(source.clone());
        }
        for clock in &observation.clocks {
            clocks.insert(clock.source_id.clone(), clock.clone());
        }
    }
    Ok(SnapshotObservation {
        available_at: latest.available_at.clone(),
        features: latest.features.clone(),
        flag_conditions: flags,
        liquidation_confirms_direction: latest.liquidation_confirms_direction,
        fully_warmed: latest.fully_warmed,
        anchor,
        cursors: cursors.into_values().collect(),
        required_clock_sources: required.into_iter().collect(),
        clocks: clocks.into_values().collect(),
    })
}

fn max_time(left: &Rfc3339Time, right: &Rfc3339Time) -> Rfc3339Time {
    if left.utc_micros() >= right.utc_micros() {
        left.clone()
    } else {
        right.clone()
    }
}

fn merge_flags(target: &mut FlagConditions, source: &FlagConditions) {
    target.sequence_failure |= source.sequence_failure;
    target.book_resyncing |= source.book_resyncing;
    target.clock_degraded |= source.clock_degraded;
    target.source_stale |= source.source_stale;
    target.oi_stale_or_unavailable |= source.oi_stale_or_unavailable;
    target.queue_drop |= source.queue_drop;
    target.reconnect_warmup |= source.reconnect_warmup;
    target.incomplete_critical |= source.incomplete_critical;
    target.breadth_unavailable_or_divergent |= source.breadth_unavailable_or_divergent;
}

fn derive_evidence(observation: &SnapshotObservation) -> Result<MechanicsEvidence, SnapshotError> {
    let value = |name| {
        observation
            .features
            .rows()
            .iter()
            .find(|row| row.name() == name)
            .and_then(|row| row.value())
    };
    let log_return = value(FeatureName::LogReturn).unwrap_or(0);
    let direction = if log_return > 0 {
        Direction::Up
    } else if log_return < 0 {
        Direction::Down
    } else {
        Direction::Unknown
    };
    let oi = value(FeatureName::OpenInterestChange);
    let liquidation = value(FeatureName::LiquidationNotional);
    let families = FamilyFlags {
        price: log_return.abs() >= 200_000,
        flow: value(FeatureName::TakerImbalance)
            .is_some_and(|v| v.abs() >= 60_000_000 && agrees(v, direction))
            || value(FeatureName::CvdSlope)
                .is_some_and(|v| v.abs() >= 2 * SCALE && agrees(v, direction)),
        book: value(FeatureName::SpreadBps).is_some_and(|v| v >= 8 * SCALE),
        derivatives: oi.is_some_and(|v| v <= -100 * SCALE)
            || liquidation.is_some_and(|v| v >= 1_000_000 * SCALE)
                && observation.liquidation_confirms_direction
                && direction != Direction::Unknown,
        breadth: value(FeatureName::CrossVenueBreadth).is_some_and(|v| v >= 67_000_000),
    };
    let quality = envelope_quality(&observation.features);
    let invalid = quality == EnvelopeQuality::Invalid
        || observation.flag_conditions.sequence_failure
        || observation.flag_conditions.book_resyncing
        || observation.flag_conditions.queue_drop
        || observation.flag_conditions.incomplete_critical
        || observation.flag_conditions.reconnect_warmup;
    let degraded = quality == EnvelopeQuality::Degraded
        || observation.flag_conditions.clock_degraded
        || observation.flag_conditions.oi_stale_or_unavailable
        || observation.flag_conditions.breadth_unavailable_or_divergent;
    Ok(MechanicsEvidence {
        available_at_ns: observation
            .available_at
            .utc_micros()
            .checked_mul(1_000)
            .ok_or_else(|| SnapshotError::Phase("availability nanoseconds overflow".into()))?,
        direction,
        families,
        intensity: families.intensity(),
        confidence: if invalid {
            0
        } else if degraded {
            80_000_000
        } else {
            SCALE
        },
        reversal_risk: value(FeatureName::ReversalFromExtreme).unwrap_or(0),
        valid: !invalid,
        fully_warmed: observation.fully_warmed,
        spread_bps: value(FeatureName::SpreadBps).unwrap_or(0),
    })
}

fn agrees(value: i128, direction: Direction) -> bool {
    matches!(
        (value.signum(), direction),
        (1, Direction::Up) | (-1, Direction::Down)
    )
}

struct AggregateClock {
    max_abs_skew: i128,
    freshness_limit_ms: u64,
    degraded: bool,
}

fn aggregate_clock(
    observation: &SnapshotObservation,
    decision: &Rfc3339Time,
    anchor: &MarketAnchor,
) -> Result<AggregateClock, SnapshotError> {
    let clocks = observation
        .clocks
        .iter()
        .map(|clock| (clock.source_id.as_str(), clock))
        .collect::<BTreeMap<_, _>>();
    if clocks.len() != observation.clocks.len() || observation.required_clock_sources.is_empty() {
        return Err(SnapshotError::MissingClockEvidence);
    }
    let mut max_abs_skew = 0;
    let mut limit = u64::MAX;
    let mut degraded = false;
    for source in &observation.required_clock_sources {
        let clock = clocks
            .get(source.as_str())
            .ok_or(SnapshotError::MissingClockEvidence)?;
        let age = decision.utc_micros() - clock.available_at.utc_micros();
        if !(0..=1_000_000).contains(&age) || clock.freshness_limit_ms == 0 {
            return Err(SnapshotError::MissingClockEvidence);
        }
        max_abs_skew = max_abs_skew.max(
            clock
                .observed_skew_ms
                .checked_abs()
                .ok_or_else(|| SnapshotError::Contract("clock skew arithmetic invalid".into()))?,
        );
        limit = limit.min(clock.freshness_limit_ms);
        degraded |= clock.degraded;
    }
    let anchor_age = decision.utc_micros() - anchor.source_event_time.utc_micros();
    if anchor_age < 0 || i128::from(anchor_age) > i128::from(limit) * 1_000 {
        return Err(SnapshotError::StaleCausalAnchor);
    }
    Ok(AggregateClock {
        max_abs_skew,
        freshness_limit_ms: limit,
        degraded,
    })
}

fn scope(instrument: &crate::wire::InstrumentIdentityV1) -> Value {
    json!({
        "kind": "PAIR",
        "asset": instrument.base_asset(),
        "venue": instrument.venue(),
        "instrument": {
            "base_asset": instrument.base_asset(),
            "quote_asset": instrument.quote_asset(),
            "market_type": instrument.market_type(),
            "venue": instrument.venue(),
            "venue_symbol": instrument.venue_symbol(),
        }
    })
}

fn feature_json(features: &FeatureSet) -> Vec<Value> {
    features
        .rows()
        .iter()
        .map(|row| {
            json!({
                "name": row.name().as_str(),
                "horizon_ms": row.horizon_ms(),
                "unit": feature_unit(row.name()),
                "value": row.value().map(canonical_decimal),
                "quality_state": feature_quality(row.quality()),
                "reason_code": row.reason().as_str(),
            })
        })
        .collect()
}

fn cursor_json(cursors: &[SnapshotCursor]) -> Result<Vec<Value>, SnapshotError> {
    let mut ordered = cursors.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        (
            left.available_at.utc_micros(),
            left.source_id.as_str(),
            left.connection_epoch.as_str(),
            left.sequence_start,
            left.sequence_end,
            left.payload_hash.as_str(),
        )
            .cmp(&(
                right.available_at.utc_micros(),
                right.source_id.as_str(),
                right.connection_epoch.as_str(),
                right.sequence_start,
                right.sequence_end,
                right.payload_hash.as_str(),
            ))
    });
    if ordered.is_empty()
        || ordered.windows(2).any(|pair| {
            pair[0].available_at.utc_micros() == pair[1].available_at.utc_micros()
                && pair[0].source_id == pair[1].source_id
                && pair[0].connection_epoch == pair[1].connection_epoch
                && pair[0].sequence_start == pair[1].sequence_start
                && pair[0].sequence_end == pair[1].sequence_end
                && pair[0].payload_hash == pair[1].payload_hash
        })
    {
        return Err(SnapshotError::CursorConflict);
    }
    Ok(ordered
        .into_iter()
        .map(|cursor| {
            json!({
                "available_at": cursor.available_at.canonical(),
                "connection_epoch": cursor.connection_epoch,
                "sequence_start": cursor.sequence_start,
                "sequence_end": cursor.sequence_end,
                "source_id": cursor.source_id,
                "source_payload_hash": cursor.payload_hash,
            })
        })
        .collect())
}

fn feature_unit(name: FeatureName) -> &'static str {
    match name {
        FeatureName::BookDepth10bps | FeatureName::LiquidationNotional => "USDC",
        FeatureName::CrossVenueBreadth | FeatureName::ReversalFromExtreme => "RATIO",
        FeatureName::CvdSlope => "BASE_PER_SECOND",
        FeatureName::LogReturn => "LOG_RETURN",
        FeatureName::OpenInterestChange => "CONTRACTS",
        FeatureName::SpreadBps => "BPS",
        FeatureName::TakerImbalance => "RATIO",
    }
}

fn feature_quality(quality: FeatureQuality) -> &'static str {
    match quality {
        FeatureQuality::Invalid => "INVALID",
        FeatureQuality::Degraded => "DEGRADED",
        FeatureQuality::Unavailable => "UNAVAILABLE",
        FeatureQuality::Validated => "VALIDATED",
    }
}

fn direction(direction: Direction) -> &'static str {
    match direction {
        Direction::Up => "UP",
        Direction::Down => "DOWN",
        Direction::Unknown => "UNKNOWN",
    }
}

fn event_type(phase: Phase, evidence: &MechanicsEvidence) -> &'static str {
    if matches!(phase, Phase::Normal | Phase::Buildup | Phase::Invalid) {
        "UNKNOWN"
    } else if evidence.families.derivatives {
        match evidence.direction {
            Direction::Up => "SHORT_SQUEEZE",
            Direction::Down => "LONG_LIQUIDATION",
            Direction::Unknown => "UNKNOWN",
        }
    } else if evidence.families.price && evidence.families.flow {
        "FLOW_SHOCK"
    } else if evidence.families.book {
        "BOOK_DISLOCATION"
    } else {
        "UNKNOWN"
    }
}

fn flag_string(flag: MechanicsFlag) -> &'static str {
    match flag {
        MechanicsFlag::BookResyncing => "BOOK_RESYNCING",
        MechanicsFlag::ClockUncertain => "CLOCK_UNCERTAIN",
        MechanicsFlag::CrossVenueDivergence => "CROSS_VENUE_DIVERGENCE",
        MechanicsFlag::InsufficientCoverage => "INSUFFICIENT_COVERAGE",
        MechanicsFlag::OiStale => "OI_STALE",
        MechanicsFlag::QueueDrop => "QUEUE_DROP",
        MechanicsFlag::ReconnectWarmup => "RECONNECT_WARMUP",
        MechanicsFlag::SequenceGap => "SEQUENCE_GAP",
        MechanicsFlag::SourceStale => "SOURCE_STALE",
    }
}
