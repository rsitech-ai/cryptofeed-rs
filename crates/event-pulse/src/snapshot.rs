//! Atomic canonical EventPulse mechanics snapshot authorship.

use std::collections::BTreeMap;

use marketfeed_model::{AggressorSide, MarketEvent};
use serde_json::{Value, json};
use thiserror::Error;

use crate::{
    ContractBundle, CursorError, IngestOutcome, SlotState, SourceStateMachine, ValidatedContract,
    content_hash,
    features::{
        BookProjection, Direction, EnvelopeQuality, FeatureCondition, FeatureConditions,
        FeatureName, FeatureQuality, FeatureSet, FlagConditions, MechanicsFlag, ReversalPolicy,
        SCALE, VenueReturn, canonical_decimal, configured_cross_venue_breadth, cvd_slope,
        envelope_quality, evaluate_feature, evaluate_reversal, liquidation_notional, log_return,
        mechanics_flags, open_interest_change, open_interest_contracts, rescale, spread_bps,
        taker_imbalance,
    },
    mechanics::{FamilyFlags, MechanicsEvidence, Phase, PhaseError, PhaseMachine},
    window::{
        CoverageInterval, FixedWindow, PROCESSOR_RECORD_CAPACITY, WindowBank, WindowKey,
        WindowKind, WindowSource, WindowSpec, has_exact_coverage,
    },
    wire::{
        ClockQualityV1, ClockStateV1, ContributorKeyV1, ContributorRoleV1, CoverageSourceKeyV1,
        CursorV1, FamilyV1, MechanicsConfigV1, MechanicsInputRefV1, MechanicsInputV1,
        OpenInterestEncodingRefV1, Rfc3339Time, SnapshotAuthoringV1,
    },
};

#[derive(Debug, Clone)]
enum FeatureSample {
    Trade {
        price: i128,
        quantity: i128,
        side: AggressorSide,
    },
    Quote {
        bid: i128,
        ask: i128,
    },
    Book,
    OpenInterest(i128),
    Liquidation {
        price: i128,
        quantity: i128,
        side: AggressorSide,
    },
    ConfirmationPrice(i128),
}

#[derive(Debug, Clone)]
struct CoverageState {
    generation: u8,
    window: FixedWindow<CoverageInterval>,
}

#[derive(Debug, Clone)]
struct FeatureRuntime {
    windows: WindowBank<FeatureSample>,
    coverage: BTreeMap<CoverageSourceKeyV1, CoverageState>,
    books: BTreeMap<ContributorKeyV1, BookProjection>,
    generations: BTreeMap<ContributorKeyV1, u8>,
}

impl FeatureRuntime {
    fn new(config: &MechanicsConfigV1) -> Result<Self, SnapshotError> {
        let mut specs = Vec::with_capacity(config.contributors().len() * 6);
        let mut books = BTreeMap::new();
        for contributor in config.contributors() {
            let source = WindowSource::new(contributor.key().source_id())
                .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
            for family in contributor.allowed_families() {
                let windows: &[(i64, WindowKind)] = match family {
                    FamilyV1::Trade => &[
                        (1_000_000_000, WindowKind::Trade),
                        (5_000_000_000, WindowKind::Trade),
                    ],
                    FamilyV1::Quote => &[(250_000_000, WindowKind::Quote)],
                    FamilyV1::Book => &[(250_000_000, WindowKind::Book)],
                    FamilyV1::OpenInterest => &[(5_000_000_000, WindowKind::OpenInterest)],
                    FamilyV1::Liquidation => &[(5_000_000_000, WindowKind::Liquidation)],
                    FamilyV1::ConfirmationPrice => {
                        &[(1_000_000_000, WindowKind::ConfirmationPrice)]
                    }
                };
                for (horizon_ns, kind) in windows {
                    specs.push(WindowSpec {
                        key: WindowKey::new(source.clone(), *horizon_ns, *kind)
                            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
                        epoch_generation: 0,
                        epoch_first_available_ns: i64::MIN,
                    });
                }
                if *family == FamilyV1::Book {
                    books.insert(contributor.key().clone(), BookProjection::new(8, 8, None));
                }
            }
        }
        let windows = WindowBank::new(specs)
            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
        let coverage = config
            .coverage_sources()
            .iter()
            .map(|key| {
                Ok((
                    key.clone(),
                    CoverageState {
                        generation: 0,
                        window: FixedWindow::new(60_000_000_000, i64::MIN)
                            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>, SnapshotError>>()?;
        Ok(Self {
            windows,
            coverage,
            books,
            generations: BTreeMap::new(),
        })
    }

    fn key(
        contributor: &ContributorKeyV1,
        horizon_ns: i64,
        kind: WindowKind,
    ) -> Result<WindowKey, SnapshotError> {
        WindowKey::new(
            WindowSource::new(contributor.source_id())
                .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
            horizon_ns,
            kind,
        )
        .map_err(|error| SnapshotError::InvalidInput(error.to_string()))
    }

    fn sync_generation(
        &mut self,
        contributor: &ContributorKeyV1,
        generation: u8,
        at_ns: i64,
    ) -> Result<(), SnapshotError> {
        match self.generations.get(contributor).copied() {
            None => {
                if generation > 0 {
                    let source = WindowSource::new(contributor.source_id())
                        .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
                    self.windows
                        .advance_source_epoch(&source, generation, at_ns)
                        .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
                }
                self.generations.insert(contributor.clone(), generation);
            }
            Some(current) if generation > current => {
                let source = WindowSource::new(contributor.source_id())
                    .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
                self.windows
                    .advance_source_epoch(&source, generation, at_ns)
                    .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
                if let Some(book) = self.books.get_mut(contributor) {
                    *book = BookProjection::new(8, 8, None);
                }
                self.generations.insert(contributor.clone(), generation);
            }
            _ => {}
        }
        Ok(())
    }

    fn push(
        &mut self,
        contributor: &ContributorKeyV1,
        horizon_ns: i64,
        kind: WindowKind,
        at_ns: i64,
        value: FeatureSample,
    ) -> Result<(), SnapshotError> {
        self.windows
            .push(&Self::key(contributor, horizon_ns, kind)?, at_ns, value)
            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))
    }

    fn ingest(&mut self, input: &MechanicsInputV1) -> Result<(), SnapshotError> {
        match input.view() {
            MechanicsInputRefV1::Market {
                envelope, catalog, ..
            } => {
                let venue = catalog
                    .venue_source(envelope.venue.0)
                    .ok_or_else(|| SnapshotError::InvalidInput("venue mapping".into()))?;
                let instrument_id = envelope
                    .instrument
                    .ok_or_else(|| SnapshotError::InvalidInput("instrument mapping".into()))?;
                let instrument = catalog
                    .instrument(instrument_id.0)
                    .ok_or_else(|| SnapshotError::InvalidInput("instrument mapping".into()))?;
                let contributor = ContributorKeyV1::new(venue.source_id(), instrument.clone())
                    .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
                let epoch = catalog
                    .connection_epochs()
                    .iter()
                    .find(|entry| {
                        entry.connection_id() == envelope.connection.0
                            && entry.session_id() == envelope.session.0
                    })
                    .ok_or_else(|| SnapshotError::InvalidInput("epoch mapping".into()))?;
                let at_ns = envelope.receive_ts.0;
                self.sync_generation(&contributor, epoch.epoch_generation(), at_ns)?;
                match &envelope.payload {
                    MarketEvent::Trade(trade) => {
                        let sample = FeatureSample::Trade {
                            price: rescale(trade.price.0)
                                .map_err(|error| SnapshotError::Contract(error.to_string()))?,
                            quantity: rescale(trade.quantity.0)
                                .map_err(|error| SnapshotError::Contract(error.to_string()))?,
                            side: trade.aggressor,
                        };
                        self.push(
                            &contributor,
                            1_000_000_000,
                            WindowKind::Trade,
                            at_ns,
                            sample.clone(),
                        )?;
                        self.push(
                            &contributor,
                            5_000_000_000,
                            WindowKind::Trade,
                            at_ns,
                            sample,
                        )?;
                    }
                    MarketEvent::Quote(quote) => self.push(
                        &contributor,
                        250_000_000,
                        WindowKind::Quote,
                        at_ns,
                        FeatureSample::Quote {
                            bid: rescale(quote.bid_price.0)
                                .map_err(|error| SnapshotError::Contract(error.to_string()))?,
                            ask: rescale(quote.ask_price.0)
                                .map_err(|error| SnapshotError::Contract(error.to_string()))?,
                        },
                    )?,
                    MarketEvent::BookSnapshot(snapshot) => {
                        let projection = self.books.get_mut(&contributor).ok_or_else(|| {
                            SnapshotError::InvalidInput("unconfigured book family".into())
                        })?;
                        match envelope.source_sequence {
                            Some(sequence) => projection.snapshot_native(snapshot, sequence, at_ns),
                            None => projection.snapshot_derived(snapshot, at_ns),
                        }
                        .map_err(|error| SnapshotError::Contract(error.to_string()))?;
                        self.push(
                            &contributor,
                            250_000_000,
                            WindowKind::Book,
                            at_ns,
                            FeatureSample::Book,
                        )?;
                    }
                    MarketEvent::BookDelta(delta) => {
                        let projection = self.books.get_mut(&contributor).ok_or_else(|| {
                            SnapshotError::InvalidInput("unconfigured book family".into())
                        })?;
                        match envelope.source_sequence {
                            Some(sequence) => projection.delta_native(delta, sequence, at_ns),
                            None => projection.delta_derived(delta, at_ns),
                        }
                        .map_err(|error| SnapshotError::Contract(error.to_string()))?;
                        self.push(
                            &contributor,
                            250_000_000,
                            WindowKind::Book,
                            at_ns,
                            FeatureSample::Book,
                        )?;
                    }
                    MarketEvent::OpenInterest(value) => {
                        let conversion = match catalog
                            .open_interest_encoding(instrument_id.0)
                            .ok_or_else(|| SnapshotError::InvalidInput("OI encoding".into()))?
                            .view()
                        {
                            OpenInterestEncodingRefV1::Contracts => None,
                            OpenInterestEncodingRefV1::Base { contracts_per_base } => {
                                Some(parse_scaled(contracts_per_base.as_str())?)
                            }
                        };
                        let contracts = open_interest_contracts(value.quantity.0, conversion)
                            .map_err(|error| SnapshotError::Contract(error.to_string()))?;
                        self.push(
                            &contributor,
                            5_000_000_000,
                            WindowKind::OpenInterest,
                            at_ns,
                            FeatureSample::OpenInterest(contracts),
                        )?;
                    }
                    MarketEvent::Liquidation(value) => self.push(
                        &contributor,
                        5_000_000_000,
                        WindowKind::Liquidation,
                        at_ns,
                        FeatureSample::Liquidation {
                            price: rescale(value.price.0)
                                .map_err(|error| SnapshotError::Contract(error.to_string()))?,
                            quantity: rescale(value.quantity.0)
                                .map_err(|error| SnapshotError::Contract(error.to_string()))?,
                            side: value.side,
                        },
                    )?,
                    MarketEvent::MarkPrice(value) | MarketEvent::IndexPrice(value) => self.push(
                        &contributor,
                        1_000_000_000,
                        WindowKind::ConfirmationPrice,
                        at_ns,
                        FeatureSample::ConfirmationPrice(
                            rescale(value.price.0)
                                .map_err(|error| SnapshotError::Contract(error.to_string()))?,
                        ),
                    )?,
                    _ => {}
                }
            }
            MechanicsInputRefV1::Coverage {
                coverage_source,
                covered_from,
                covered_through,
                available_at,
                ..
            } => {
                let state = self
                    .coverage
                    .get_mut(coverage_source.key())
                    .ok_or_else(|| SnapshotError::InvalidInput("unconfigured coverage".into()))?;
                if coverage_source.epoch_generation() > state.generation {
                    state.generation = coverage_source.epoch_generation();
                    state.window.clear_for_new_epoch(time_to_ns(available_at)?);
                }
                state
                    .window
                    .push(
                        time_to_ns(available_at)?,
                        CoverageInterval {
                            covered_from_ns: time_to_ns(covered_from)?,
                            covered_through_ns: time_to_ns(covered_through)?,
                            available_at_ns: time_to_ns(available_at)?,
                        },
                    )
                    .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
            }
            MechanicsInputRefV1::System { fault, .. } => match fault.view() {
                crate::wire::SystemFaultRefV1::BookInvalidated
                | crate::wire::SystemFaultRefV1::ChecksumMismatch => {
                    for book in self.books.values_mut() {
                        book.invalidate();
                    }
                }
                crate::wire::SystemFaultRefV1::BookResynchronized => {
                    for book in self.books.values_mut() {
                        book.permit_resnapshot();
                    }
                }
                _ => {}
            },
            MechanicsInputRefV1::Clock { .. } => {}
        }
        Ok(())
    }

    fn records(
        &self,
        contributor: &ContributorKeyV1,
        horizon_ns: i64,
        kind: WindowKind,
    ) -> Result<&std::collections::VecDeque<crate::window::Timed<FeatureSample>>, SnapshotError>
    {
        self.windows
            .get(&Self::key(contributor, horizon_ns, kind)?)
            .map(FixedWindow::records)
            .ok_or_else(|| SnapshotError::InvalidInput("unconfigured feature window".into()))
    }

    fn covered(
        &self,
        config: &MechanicsConfigV1,
        contributor: &ContributorKeyV1,
        family: FamilyV1,
        decision_ns: i64,
        horizon_ns: i64,
    ) -> Result<bool, SnapshotError> {
        let key = config
            .coverage_sources()
            .iter()
            .find(|key| key.subject() == contributor && key.family() == family)
            .ok_or_else(|| SnapshotError::InvalidInput("missing configured coverage".into()))?;
        let state = self
            .coverage
            .get(key)
            .ok_or_else(|| SnapshotError::InvalidInput("missing coverage state".into()))?;
        let mut intervals = Vec::with_capacity(state.window.len());
        intervals.extend(state.window.records().iter().map(|record| record.value));
        has_exact_coverage(&intervals, decision_ns, horizon_ns)
            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MarketAnchor {
    pub source_event_time: Rfc3339Time,
    pub received_at: Rfc3339Time,
    pub normalized_at: Rfc3339Time,
    pub available_at: Rfc3339Time,
    pub payload_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SnapshotCursor {
    pub source_id: String,
    pub connection_epoch: String,
    pub sequence_start: u64,
    pub sequence_end: u64,
    pub available_at: Rfc3339Time,
    pub payload_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClockEvidence {
    pub source_id: String,
    pub available_at: Rfc3339Time,
    /// Fixed point S=1e8 milliseconds.
    pub observed_skew_ms: i128,
    pub freshness_limit_ms: u64,
    pub degraded: bool,
}

#[derive(Debug, Clone)]
struct SnapshotObservation {
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
    #[error("input cursor ordering regressed")]
    InputOrderRegression,
    #[error("validated mechanics input was rejected: {0}")]
    InvalidInput(String),
    #[error("bounded processor capacity was breached")]
    Capacity,
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
    #[error("configured coverage has no evidence")]
    MissingCoverageEvidence,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cause {
    None,
    Sequence,
    Book,
    QueueDrop,
    Warmup,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct InputOrderKey {
    available_micros: i64,
    source_id: String,
    epoch: String,
    sequence_start: u64,
    sequence_end: u64,
    payload_hash: String,
}

#[derive(Debug, Clone)]
pub struct MechanicsProcessor {
    config: MechanicsConfigV1,
    authoring: SnapshotAuthoringV1,
    sources: SourceStateMachine,
    feature_runtime: FeatureRuntime,
    records: FixedWindow<MechanicsInputV1>,
    active_causes: BTreeMap<ContributorKeyV1, Cause>,
    pending: Option<SnapshotObservation>,
    current: Option<SnapshotObservation>,
    phase: PhaseMachine,
    last_input_micros: Option<i64>,
    last_order: Option<InputOrderKey>,
    sealed_micros: Option<i64>,
    last_decision_micros: Option<i64>,
    next_revision: u64,
    predecessor: Option<String>,
    cache: Option<SuccessfulCache>,
}

impl MechanicsProcessor {
    pub fn new(
        config: MechanicsConfigV1,
        authoring: SnapshotAuthoringV1,
    ) -> Result<Self, SnapshotError> {
        let primary = config
            .contributors()
            .iter()
            .find(|spec| spec.role() == ContributorRoleV1::Primary)
            .ok_or_else(|| SnapshotError::InvalidInput("missing primary".into()))?;
        if primary.key().instrument() != authoring.primary_scope() {
            return Err(SnapshotError::InvalidInput("scope/config mismatch".into()));
        }
        let next_revision = authoring.revision_start();
        let predecessor = authoring.predecessor_content_hash().map(str::to_owned);
        let active_causes = config
            .contributors()
            .iter()
            .map(|spec| (spec.key().clone(), Cause::None))
            .collect();
        let feature_runtime = FeatureRuntime::new(&config)?;
        Ok(Self {
            sources: SourceStateMachine::new(config.clone()),
            feature_runtime,
            config,
            authoring,
            records: FixedWindow::new(60_000_000_000, i64::MIN)
                .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
            active_causes,
            pending: None,
            current: None,
            phase: PhaseMachine::new(),
            last_input_micros: None,
            last_order: None,
            sealed_micros: None,
            last_decision_micros: None,
            next_revision,
            predecessor,
            cache: None,
        })
    }

    pub fn next_revision(&self) -> u64 {
        self.next_revision
    }

    pub fn ingest(&mut self, input: &MechanicsInputV1) -> Result<IngestOutcome, SnapshotError> {
        input
            .validate_static()
            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
        let order = input_order(input)?;
        let at = order.available_micros;
        if self.sealed_micros.is_some_and(|sealed| at <= sealed) {
            return Err(SnapshotError::SealedInput);
        }
        if self.last_input_micros.is_some_and(|last| at < last) {
            return Err(SnapshotError::InputTimeRegression);
        }
        if self
            .last_order
            .as_ref()
            .is_some_and(|last| last.available_micros == at && order < *last)
        {
            return Err(SnapshotError::InputOrderRegression);
        }
        if self.last_order.as_ref() != Some(&order) {
            ensure_record_capacity(self.records.len())?;
        }
        let mut candidate = self.clone();
        let outcome = match candidate.sources.ingest(input) {
            Ok(outcome) => outcome,
            Err(error) => {
                candidate.record_failure(input, &error);
                if error.invalidates_state() {
                    ensure_record_capacity(candidate.records.len())?;
                    candidate
                        .records
                        .push(
                            at.checked_mul(1_000).ok_or(SnapshotError::Capacity)?,
                            input.clone(),
                        )
                        .map_err(|window| SnapshotError::InvalidInput(window.to_string()))?;
                    candidate.last_input_micros = Some(at);
                    candidate.last_order = Some(order);
                }
                *self = candidate;
                return Err(SnapshotError::InvalidInput(error.to_string()));
            }
        };
        if outcome != IngestOutcome::IgnoredDuplicate {
            ensure_record_capacity(candidate.records.len())?;
            candidate.feature_runtime.ingest(input)?;
            candidate
                .records
                .push(
                    at.checked_mul(1_000).ok_or(SnapshotError::Capacity)?,
                    input.clone(),
                )
                .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
        }
        candidate.apply_input_cause(input);
        candidate.clear_recovered_causes();
        candidate.pending = match candidate.derive_owned_observation(at) {
            Ok(observation) => Some(observation),
            Err(SnapshotError::MissingCoverageEvidence) => None,
            Err(error) => return Err(error),
        };
        candidate.last_input_micros = Some(at);
        candidate.last_order = Some(order);
        *self = candidate;
        Ok(outcome)
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

        if self.records.is_empty() && self.current.is_none() {
            return Err(SnapshotError::MissingCausalAnchor);
        }
        let mut candidate = self.clone();
        let aggregate = candidate.derive_owned_observation(decision_micros)?;
        candidate.phase = candidate.phase_at(decision_micros)?;
        let mut decision_evidence = derive_evidence(&aggregate)?;
        decision_evidence.available_at_ns = decision_micros
            .checked_mul(1_000)
            .ok_or(SnapshotError::Capacity)?;
        candidate
            .phase
            .observe(&decision_evidence)
            .map_err(phase_error)?;
        let following_revision = self
            .next_revision
            .checked_add(1)
            .ok_or(SnapshotError::RevisionOverflow)?;
        let snapshot = candidate.author(&decision_time, &aggregate, &candidate.phase)?;

        let sealed = decision_micros;
        if candidate
            .last_input_micros
            .is_some_and(|last| last <= decision_micros)
        {
            candidate.pending = None;
        }
        candidate.current = Some(aggregate);
        candidate.sealed_micros = Some(sealed);
        candidate.last_decision_micros = Some(decision_micros);
        candidate.predecessor = Some(snapshot.content_hash().to_owned());
        candidate.next_revision = following_revision;
        candidate.cache = Some(SuccessfulCache {
            decision_micros,
            snapshot: snapshot.clone(),
        });
        *self = candidate;
        Ok(snapshot)
    }

    fn phase_at(&self, decision_micros: i64) -> Result<PhaseMachine, SnapshotError> {
        let mut phase = PhaseMachine::new();
        let mut previous = None;
        for at in self
            .records
            .records()
            .iter()
            .map(|record| record.available_at_ns / 1_000)
            .filter(|at| *at <= decision_micros)
        {
            if previous == Some(at) {
                continue;
            }
            let observation = match self.derive_owned_observation(at) {
                Ok(observation) => observation,
                Err(SnapshotError::MissingCoverageEvidence) => {
                    previous = Some(at);
                    continue;
                }
                Err(error) => return Err(error),
            };
            phase
                .observe(&derive_evidence(&observation)?)
                .map_err(phase_error)?;
            previous = Some(at);
        }
        phase
            .advance_to(
                decision_micros
                    .checked_mul(1_000)
                    .ok_or_else(|| SnapshotError::Phase("decision nanoseconds overflow".into()))?,
            )
            .map_err(phase_error)?;
        Ok(phase)
    }

    fn record_failure(&mut self, input: &MechanicsInputV1, error: &CursorError) {
        if !error.invalidates_state() {
            return;
        }
        for key in input_subjects(input, &self.config) {
            if let Some(cause) = self.active_causes.get_mut(&key) {
                *cause = Cause::Sequence;
            }
        }
    }

    fn apply_input_cause(&mut self, input: &MechanicsInputV1) {
        let Some(cause) = input_cause(input) else {
            return;
        };
        for key in input_subjects(input, &self.config) {
            if let Some(current) = self.active_causes.get_mut(&key) {
                *current = cause;
            }
        }
    }

    fn clear_recovered_causes(&mut self) {
        for spec in self.config.contributors() {
            if self.sources.contributor_state(spec.key()) == Some(SlotState::Live) {
                if let Some(cause) = self.active_causes.get_mut(spec.key()) {
                    *cause = Cause::None;
                }
            }
        }
    }

    fn derive_owned_observation(
        &self,
        decision_micros: i64,
    ) -> Result<SnapshotObservation, SnapshotError> {
        let decision_ns = decision_micros
            .checked_mul(1_000)
            .ok_or(SnapshotError::Capacity)?;
        let records = self.records.records();
        let (sources, runtime, active_causes) = self.replay_state(decision_ns)?;
        if runtime
            .coverage
            .values()
            .all(|state| state.window.is_empty())
        {
            return Err(SnapshotError::MissingCoverageEvidence);
        }
        let primary = self
            .config
            .contributors()
            .iter()
            .find(|spec| spec.role() == ContributorRoleV1::Primary)
            .ok_or_else(|| SnapshotError::InvalidInput("missing primary".into()))?;
        let primary_key = primary.key();

        let retained_anchor = records
            .iter()
            .filter(|record| record.available_at_ns <= decision_ns)
            .try_fold(None, |latest: Option<MarketAnchor>, record| {
                let candidate = market_anchor(&record.value)?;
                Ok::<_, SnapshotError>(match (latest, candidate) {
                    (Some(current), Some(candidate))
                        if candidate.available_at > current.available_at =>
                    {
                        Some(candidate)
                    }
                    (None, candidate) => candidate,
                    (current, _) => current,
                })
            })?;
        let mut anchor = None;
        let mut max_source_event_ns = i64::MIN;
        let mut max_receive_ns = i64::MIN;
        let mut max_normalized_ns = i64::MIN;
        let mut clocks = BTreeMap::new();
        let mut available_micros = i64::MIN;
        for record in records.iter().filter(|record| {
            record.available_at_ns <= decision_ns && self.input_is_current(&sources, &record.value)
        }) {
            match record.value.view() {
                MechanicsInputRefV1::Market {
                    envelope,
                    catalog,
                    payload_hash,
                    ..
                } => {
                    let _ = catalog;
                    let source_event = envelope
                        .exchange_ts
                        .ok_or_else(|| SnapshotError::InvalidInput("source event time".into()))?
                        .0;
                    let event_anchor = MarketAnchor {
                        source_event_time: Rfc3339Time::from_unix_nanos(source_event)
                            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
                        received_at: Rfc3339Time::from_unix_nanos(envelope.receive_ts.0)
                            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
                        normalized_at: Rfc3339Time::from_unix_nanos(envelope.receive_ts.0)
                            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
                        available_at: Rfc3339Time::from_unix_nanos(envelope.receive_ts.0)
                            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
                        payload_hash: payload_hash.to_owned(),
                    };
                    max_source_event_ns = max_source_event_ns.max(source_event);
                    max_receive_ns = max_receive_ns.max(envelope.receive_ts.0);
                    max_normalized_ns = max_normalized_ns.max(envelope.receive_ts.0);
                    if anchor.as_ref().is_none_or(|current: &MarketAnchor| {
                        current.available_at < event_anchor.available_at
                    }) {
                        anchor = Some(event_anchor);
                    }
                    available_micros = available_micros.max(envelope.receive_ts.0 / 1_000);
                }
                MechanicsInputRefV1::Clock {
                    clock_source,
                    available_at,
                    observed_skew_ms,
                    freshness_limit_ms,
                    clock_state,
                    quality_state,
                    ..
                } => {
                    let clock = ClockEvidence {
                        source_id: clock_source.key().source_id().to_owned(),
                        available_at: available_at.clone(),
                        observed_skew_ms: parse_scaled(observed_skew_ms.as_str())?,
                        freshness_limit_ms,
                        degraded: clock_state == ClockStateV1::Degraded
                            || quality_state == ClockQualityV1::Degraded,
                    };
                    available_micros = available_micros.max(available_at.utc_micros());
                    clocks.insert(clock.source_id.clone(), clock);
                }
                MechanicsInputRefV1::Coverage { available_at, .. }
                | MechanicsInputRefV1::System { available_at, .. } => {
                    available_micros = available_micros.max(available_at.utc_micros());
                }
            }
        }
        if let Some(current) = anchor.as_mut() {
            current.source_event_time = Rfc3339Time::from_unix_nanos(max_source_event_ns)
                .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
            current.received_at = Rfc3339Time::from_unix_nanos(max_receive_ns)
                .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
            current.normalized_at = Rfc3339Time::from_unix_nanos(max_normalized_ns)
                .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
        } else {
            anchor = retained_anchor;
        }

        let one_second = decision_ns.saturating_sub(1_000_000_000);
        let five_seconds = decision_ns.saturating_sub(5_000_000_000);
        let trade_1s_covered = runtime.covered(
            &self.config,
            primary_key,
            FamilyV1::Trade,
            decision_ns,
            1_000_000_000,
        )?;
        let trade_5s_covered = runtime.covered(
            &self.config,
            primary_key,
            FamilyV1::Trade,
            decision_ns,
            5_000_000_000,
        )?;
        let trade_window_1s = runtime.records(primary_key, 1_000_000_000, WindowKind::Trade)?;
        let mut trade_1s = Vec::with_capacity(trade_window_1s.len());
        trade_1s.extend(trade_window_1s.iter().filter_map(|record| {
            if record.available_at_ns < one_second {
                return None;
            }
            match record.value {
                FeatureSample::Trade {
                    price,
                    quantity,
                    side,
                } => Some((record.available_at_ns, price, quantity, side)),
                _ => None,
            }
        }));
        let trade_window_5s = runtime.records(primary_key, 5_000_000_000, WindowKind::Trade)?;
        let mut trade_5s = Vec::with_capacity(trade_window_5s.len());
        trade_5s.extend(trade_window_5s.iter().filter_map(|record| {
            if record.available_at_ns < five_seconds {
                return None;
            }
            match record.value {
                FeatureSample::Trade {
                    price,
                    quantity,
                    side,
                } => Some((record.available_at_ns, price, quantity, side)),
                _ => None,
            }
        }));
        let log = if trade_1s_covered && trade_1s.len() >= 2 {
            Some(
                log_return(trade_1s[0].1, trade_1s.last().expect("two trades").1)
                    .map_err(|error| SnapshotError::Contract(error.to_string()))?,
            )
        } else {
            None
        };
        let direction = match log.unwrap_or(0).signum() {
            1 => Direction::Up,
            -1 => Direction::Down,
            _ => Direction::Unknown,
        };
        let buy = checked_sum(
            trade_1s
                .iter()
                .filter(|(_, _, _, side)| *side == AggressorSide::Buy)
                .map(|(_, _, quantity, _)| *quantity),
        )?;
        let sell = checked_sum(
            trade_1s
                .iter()
                .filter(|(_, _, _, side)| *side == AggressorSide::Sell)
                .map(|(_, _, quantity, _)| *quantity),
        )?;
        let flow_known = trade_1s
            .iter()
            .all(|(_, _, _, side)| *side != AggressorSide::Unknown);
        let flow_total = buy.checked_add(sell).ok_or_else(arithmetic_overflow)?;
        let imbalance = if trade_1s_covered && flow_known && flow_total > 0 {
            Some(
                taker_imbalance(buy, sell)
                    .map_err(|error| SnapshotError::Contract(error.to_string()))?,
            )
        } else {
            None
        };
        let cvd = if trade_1s_covered && flow_known && trade_1s.len() >= 2 {
            let signed = |quantity: i128, side: AggressorSide| {
                if side == AggressorSide::Sell {
                    -quantity
                } else {
                    quantity
                }
            };
            let first = signed(trade_1s[0].2, trade_1s[0].3);
            let last = checked_sum(
                trade_1s
                    .iter()
                    .map(|(_, _, quantity, side)| signed(*quantity, *side)),
            )?;
            let elapsed_ns = trade_1s
                .last()
                .expect("two")
                .0
                .checked_sub(trade_1s[0].0)
                .ok_or_else(arithmetic_overflow)?;
            Some(
                cvd_slope(first, last, i128::from(elapsed_ns) / 1_000)
                    .map_err(|error| SnapshotError::Contract(error.to_string()))?,
            )
        } else {
            None
        };
        let quote_boundary = decision_ns.saturating_sub(250_000_000);
        let quote_covered = runtime.covered(
            &self.config,
            primary_key,
            FamilyV1::Quote,
            decision_ns,
            250_000_000,
        )?;
        let quote = runtime
            .records(primary_key, 250_000_000, WindowKind::Quote)?
            .iter()
            .filter(|record| record.available_at_ns >= quote_boundary)
            .filter_map(|record| match record.value {
                FeatureSample::Quote { bid, ask } => Some((record.available_at_ns, bid, ask)),
                _ => None,
            })
            .next_back();
        let spread = quote
            .filter(|_| quote_covered)
            .map(|(_, bid, ask)| spread_bps(bid, ask))
            .transpose()
            .map_err(|error| SnapshotError::Contract(error.to_string()))?;
        let book_covered = runtime.covered(
            &self.config,
            primary_key,
            FamilyV1::Book,
            decision_ns,
            250_000_000,
        )?;
        let depth = runtime
            .books
            .get(primary_key)
            .filter(|_| book_covered)
            .and_then(|projection| projection.depth_10bps(decision_ns).ok());
        let owns_oi = primary.allowed_families().contains(&FamilyV1::OpenInterest);
        let mut oi_5s = Vec::new();
        if owns_oi {
            let oi_window =
                runtime.records(primary_key, 5_000_000_000, WindowKind::OpenInterest)?;
            oi_5s.reserve(oi_window.len());
            oi_5s.extend(oi_window.iter().filter_map(|record| {
                if record.available_at_ns < five_seconds {
                    return None;
                }
                match record.value {
                    FeatureSample::OpenInterest(value) => Some((record.available_at_ns, value)),
                    _ => None,
                }
            }));
        }
        let oi_covered = owns_oi
            && runtime.covered(
                &self.config,
                primary_key,
                FamilyV1::OpenInterest,
                decision_ns,
                5_000_000_000,
            )?;
        let oi_change = if oi_covered && oi_5s.len() >= 2 {
            Some(
                open_interest_change(oi_5s[0].1, oi_5s.last().expect("two").1)
                    .map_err(|error| SnapshotError::Contract(error.to_string()))?,
            )
        } else {
            None
        };
        let owns_liquidation = primary.allowed_families().contains(&FamilyV1::Liquidation);
        let mut liq_5s = Vec::new();
        if owns_liquidation {
            let liquidation_window =
                runtime.records(primary_key, 5_000_000_000, WindowKind::Liquidation)?;
            liq_5s.reserve(liquidation_window.len());
            liq_5s.extend(liquidation_window.iter().filter_map(|record| {
                if record.available_at_ns < five_seconds {
                    return None;
                }
                match record.value {
                    FeatureSample::Liquidation {
                        price,
                        quantity,
                        side,
                    } => Some((record.available_at_ns, price, quantity, side)),
                    _ => None,
                }
            }));
        }
        let liquidation_covered = owns_liquidation
            && runtime.covered(
                &self.config,
                primary_key,
                FamilyV1::Liquidation,
                decision_ns,
                5_000_000_000,
            )?;
        let liquidation = if liq_5s.is_empty() && liquidation_covered {
            Some(0)
        } else if liq_5s.is_empty() {
            None
        } else {
            let mut notional_inputs = Vec::with_capacity(liq_5s.len());
            notional_inputs.extend(
                liq_5s
                    .iter()
                    .map(|(_, price, quantity, _)| (*price, *quantity)),
            );
            Some(
                liquidation_notional(&notional_inputs)
                    .map_err(|error| SnapshotError::Contract(error.to_string()))?,
            )
        };
        let mut venue_returns = Vec::with_capacity(self.config.contributors().len());
        for contributor in self.config.contributors() {
            let (family, kind) = if contributor.role() == ContributorRoleV1::Primary {
                (FamilyV1::Trade, WindowKind::Trade)
            } else {
                (FamilyV1::ConfirmationPrice, WindowKind::ConfirmationPrice)
            };
            if !runtime.covered(
                &self.config,
                contributor.key(),
                family,
                decision_ns,
                1_000_000_000,
            )? {
                continue;
            }
            let window = runtime.records(contributor.key(), 1_000_000_000, kind)?;
            let mut prices = Vec::with_capacity(window.len());
            prices.extend(
                window
                    .iter()
                    .filter(|record| record.available_at_ns >= one_second)
                    .filter_map(|record| match record.value {
                        FeatureSample::Trade { price, .. }
                        | FeatureSample::ConfirmationPrice(price) => Some(price),
                        _ => None,
                    }),
            );
            if prices.len() >= 2 {
                venue_returns.push(VenueReturn {
                    contributor: contributor.key().clone(),
                    log_return: log_return(prices[0], *prices.last().expect("two"))
                        .map_err(|error| SnapshotError::Contract(error.to_string()))?,
                    complete: true,
                });
            }
        }
        let breadth =
            configured_cross_venue_breadth(&self.config, &sources, direction, &venue_returns).ok();
        let reversal_policy = if matches!(self.phase.phase(), Phase::Normal | Phase::Invalid) {
            ReversalPolicy::PreEventZero
        } else if direction == Direction::Up {
            ReversalPolicy::ReversalRequired {
                direction: crate::features::KnownDirection::Up,
            }
        } else if direction == Direction::Down {
            ReversalPolicy::ReversalRequired {
                direction: crate::features::KnownDirection::Down,
            }
        } else {
            ReversalPolicy::UnknownNormalZero
        };
        let degraded_clock = clocks.values().any(|clock| clock.degraded);
        let critical_invalid = sources.contributor_state(primary_key) != Some(SlotState::Live)
            || active_causes
                .get(primary_key)
                .copied()
                .unwrap_or(Cause::None)
                != Cause::None;
        let stale = quote.is_none();
        let mut flag_conditions = FlagConditions::default();
        match active_causes
            .get(primary_key)
            .copied()
            .unwrap_or(Cause::None)
        {
            Cause::Sequence => flag_conditions.sequence_failure = true,
            Cause::Book => flag_conditions.book_resyncing = true,
            Cause::QueueDrop => flag_conditions.queue_drop = true,
            Cause::Warmup => flag_conditions.reconnect_warmup = true,
            Cause::None => {}
        }
        flag_conditions.reconnect_warmup |=
            sources.contributor_state(primary_key) != Some(SlotState::Live);
        flag_conditions.source_stale = stale;
        flag_conditions.clock_degraded = degraded_clock;
        flag_conditions.incomplete_critical = [log, imbalance, cvd, spread, depth]
            .iter()
            .any(Option::is_none);
        flag_conditions.oi_stale_or_unavailable = oi_change.is_none();
        flag_conditions.breadth_unavailable_or_divergent = breadth.is_none();

        let mut rows = Vec::with_capacity(9);
        for (name, value) in [
            (FeatureName::BookDepth10bps, depth),
            (FeatureName::CrossVenueBreadth, breadth),
            (FeatureName::CvdSlope, cvd),
            (FeatureName::LiquidationNotional, liquidation),
            (FeatureName::LogReturn, log),
            (FeatureName::OpenInterestChange, oi_change),
            (FeatureName::SpreadBps, spread),
            (FeatureName::TakerImbalance, imbalance),
        ] {
            let optional = name.is_optional();
            let conditions = feature_conditions(
                name,
                value,
                critical_invalid,
                stale,
                degraded_clock,
                optional,
            );
            rows.push(
                evaluate_feature(name, value, &conditions, reversal_policy)
                    .map_err(|error| SnapshotError::Contract(error.to_string()))?,
            );
        }
        let reversal_conditions = feature_conditions(
            FeatureName::ReversalFromExtreme,
            Some(0),
            critical_invalid,
            false,
            degraded_clock,
            false,
        );
        let reversal = if trade_5s_covered && trade_5s.len() >= 2 {
            let anchor_price = trade_5s[0].1;
            let extreme = if direction == Direction::Down {
                trade_5s
                    .iter()
                    .map(|(_, price, ..)| *price)
                    .min()
                    .expect("two")
            } else {
                trade_5s
                    .iter()
                    .map(|(_, price, ..)| *price)
                    .max()
                    .expect("two")
            };
            let current = trade_5s.last().expect("two").1;
            evaluate_reversal(
                reversal_policy,
                anchor_price,
                extreme,
                current,
                &reversal_conditions,
            )
            .map_err(|error| SnapshotError::Contract(error.to_string()))?
        } else {
            evaluate_reversal(ReversalPolicy::PreEventZero, 1, 1, 1, &reversal_conditions)
                .map_err(|error| SnapshotError::Contract(error.to_string()))?
        };
        rows.push(reversal);
        let features = FeatureSet::new(rows, reversal_policy)
            .map_err(|error| SnapshotError::Contract(error.to_string()))?;

        let cursors = self.current_cursors(&sources)?;
        let mut required_clock_sources = Vec::with_capacity(self.config.clock_sources().len());
        required_clock_sources.extend(
            self.config
                .clock_sources()
                .iter()
                .filter(|key| sources.contributor_cursor(key.subject()).is_some())
                .map(|key| key.source_id().to_owned()),
        );
        available_micros = available_micros.max(
            cursors
                .iter()
                .map(|cursor| cursor.available_at.utc_micros())
                .max()
                .unwrap_or(i64::MIN),
        );
        let fully_warmed = !flag_conditions.reconnect_warmup;
        Ok(SnapshotObservation {
            available_at: Rfc3339Time::from_unix_nanos(
                available_micros
                    .checked_mul(1_000)
                    .ok_or(SnapshotError::Capacity)?,
            )
            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
            features,
            flag_conditions,
            liquidation_confirms_direction: liq_5s.iter().any(|(_, _, _, side)| {
                matches!(
                    (direction, side),
                    (Direction::Up, AggressorSide::Buy) | (Direction::Down, AggressorSide::Sell)
                )
            }),
            fully_warmed,
            anchor,
            cursors,
            required_clock_sources,
            clocks: clocks.into_values().collect(),
        })
    }

    fn replay_state(
        &self,
        decision_ns: i64,
    ) -> Result<
        (
            SourceStateMachine,
            FeatureRuntime,
            BTreeMap<ContributorKeyV1, Cause>,
        ),
        SnapshotError,
    > {
        let mut sources = SourceStateMachine::new(self.config.clone());
        let mut runtime = FeatureRuntime::new(&self.config)?;
        let mut causes = self
            .config
            .contributors()
            .iter()
            .map(|spec| (spec.key().clone(), Cause::None))
            .collect::<BTreeMap<_, _>>();
        for record in self
            .records
            .records()
            .iter()
            .filter(|record| record.available_at_ns <= decision_ns)
        {
            match sources.ingest(&record.value) {
                Ok(IngestOutcome::IgnoredDuplicate) => {}
                Ok(_) => {
                    runtime.ingest(&record.value)?;
                    if let Some(cause) = input_cause(&record.value) {
                        for subject in input_subjects(&record.value, &self.config) {
                            if let Some(current) = causes.get_mut(&subject) {
                                *current = cause;
                            }
                        }
                    }
                }
                Err(error) if error.invalidates_state() => {
                    for subject in input_subjects(&record.value, &self.config) {
                        if let Some(cause) = causes.get_mut(&subject) {
                            *cause = Cause::Sequence;
                        }
                    }
                }
                Err(_) => {}
            }
            for spec in self.config.contributors() {
                if sources.contributor_state(spec.key()) == Some(SlotState::Live) {
                    if let Some(cause) = causes.get_mut(spec.key()) {
                        *cause = Cause::None;
                    }
                }
            }
        }
        Ok((sources, runtime, causes))
    }

    fn input_is_current(&self, sources: &SourceStateMachine, input: &MechanicsInputV1) -> bool {
        match input.view() {
            MechanicsInputRefV1::Market {
                envelope, catalog, ..
            } => {
                let Some(venue) = catalog.venue_source(envelope.venue.0) else {
                    return false;
                };
                let Some(instrument) = envelope.instrument.and_then(|id| catalog.instrument(id.0))
                else {
                    return false;
                };
                let Ok(key) = ContributorKeyV1::new(venue.source_id(), instrument.clone()) else {
                    return false;
                };
                let Some(epoch) = catalog.connection_epochs().iter().find(|entry| {
                    entry.connection_id() == envelope.connection.0
                        && entry.session_id() == envelope.session.0
                }) else {
                    return false;
                };
                sources.contributor_cursor(&key).is_some_and(|current| {
                    current.epoch == epoch.connection_epoch()
                        && current.epoch_generation == epoch.epoch_generation()
                })
            }
            MechanicsInputRefV1::Clock { clock_source, .. } => sources
                .clock_cursor(clock_source.key())
                .is_some_and(|current| {
                    current.epoch == clock_source.epoch()
                        && current.epoch_generation == clock_source.epoch_generation()
                }),
            MechanicsInputRefV1::Coverage {
                coverage_source, ..
            } => sources
                .coverage_cursor(coverage_source.key())
                .is_some_and(|current| {
                    current.epoch == coverage_source.epoch()
                        && current.epoch_generation == coverage_source.epoch_generation()
                }),
            MechanicsInputRefV1::System { system_source, .. } => sources
                .system_cursor(system_source.key())
                .is_some_and(|current| {
                    current.epoch == system_source.epoch()
                        && current.epoch_generation == system_source.epoch_generation()
                }),
        }
    }

    fn current_cursors(
        &self,
        sources: &SourceStateMachine,
    ) -> Result<Vec<SnapshotCursor>, SnapshotError> {
        let mut cursors = Vec::with_capacity(
            self.config.contributors().len()
                + self.config.clock_sources().len()
                + self.config.coverage_sources().len()
                + self.config.system_sources().len(),
        );
        for spec in self.config.contributors() {
            if let Some(view) = sources.contributor_cursor(spec.key()) {
                cursors.push(snapshot_cursor(spec.key().source_id(), view)?);
            }
        }
        for key in self.config.clock_sources() {
            if let Some(view) = sources.clock_cursor(key) {
                cursors.push(snapshot_cursor(key.source_id(), view)?);
            }
        }
        for key in self.config.coverage_sources() {
            if let Some(view) = sources.coverage_cursor(key) {
                cursors.push(snapshot_cursor(key.source_id(), view)?);
            }
        }
        for key in self.config.system_sources() {
            if let Some(view) = sources.system_cursor(key) {
                cursors.push(snapshot_cursor(key.source_id(), view)?);
            }
        }
        Ok(cursors)
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

fn market_anchor(input: &MechanicsInputV1) -> Result<Option<MarketAnchor>, SnapshotError> {
    let MechanicsInputRefV1::Market {
        envelope,
        payload_hash,
        ..
    } = input.view()
    else {
        return Ok(None);
    };
    let source_event = envelope
        .exchange_ts
        .ok_or_else(|| SnapshotError::InvalidInput("source event time".into()))?
        .0;
    Ok(Some(MarketAnchor {
        source_event_time: Rfc3339Time::from_unix_nanos(source_event)
            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
        received_at: Rfc3339Time::from_unix_nanos(envelope.receive_ts.0)
            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
        normalized_at: Rfc3339Time::from_unix_nanos(envelope.receive_ts.0)
            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
        available_at: Rfc3339Time::from_unix_nanos(envelope.receive_ts.0)
            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
        payload_hash: payload_hash.to_owned(),
    }))
}

fn ensure_record_capacity(current: usize) -> Result<(), SnapshotError> {
    if current == crate::window::PER_WINDOW_CAPACITY || current == PROCESSOR_RECORD_CAPACITY {
        Err(SnapshotError::Capacity)
    } else {
        Ok(())
    }
}

fn phase_error(error: PhaseError) -> SnapshotError {
    SnapshotError::Phase(error.to_string())
}

fn arithmetic_overflow() -> SnapshotError {
    SnapshotError::Contract("checked arithmetic overflowed".into())
}

fn checked_sum(values: impl IntoIterator<Item = i128>) -> Result<i128, SnapshotError> {
    values
        .into_iter()
        .try_fold(0i128, i128::checked_add)
        .ok_or_else(arithmetic_overflow)
}

fn input_cause(input: &MechanicsInputV1) -> Option<Cause> {
    let MechanicsInputRefV1::System { fault, .. } = input.view() else {
        return None;
    };
    Some(match fault.view() {
        crate::wire::SystemFaultRefV1::ChecksumMismatch
        | crate::wire::SystemFaultRefV1::BookInvalidated
        | crate::wire::SystemFaultRefV1::BookResynchronized => Cause::Book,
        crate::wire::SystemFaultRefV1::EventsDropped { .. } => Cause::QueueDrop,
        crate::wire::SystemFaultRefV1::Disconnected => Cause::Warmup,
        crate::wire::SystemFaultRefV1::SequenceGap { .. }
        | crate::wire::SystemFaultRefV1::ClockJump { .. } => Cause::Sequence,
    })
}

fn time_to_ns(time: &Rfc3339Time) -> Result<i64, SnapshotError> {
    time.utc_micros()
        .checked_mul(1_000)
        .ok_or(SnapshotError::Capacity)
}

fn input_order(input: &MechanicsInputV1) -> Result<InputOrderKey, SnapshotError> {
    let (available_micros, source_id, epoch, cursor) = match input.view() {
        MechanicsInputRefV1::Market {
            envelope,
            action_index,
            catalog,
            ..
        } => {
            let venue = catalog
                .venue_source(envelope.venue.0)
                .ok_or_else(|| SnapshotError::InvalidInput("venue mapping".into()))?;
            let epoch = catalog
                .connection_epochs()
                .iter()
                .find(|entry| {
                    entry.connection_id() == envelope.connection.0
                        && entry.session_id() == envelope.session.0
                })
                .ok_or_else(|| SnapshotError::InvalidInput("epoch mapping".into()))?;
            let cursor = match envelope.source_sequence {
                Some(range) => CursorV1::native(range.first, range.last),
                None => CursorV1::derived(
                    envelope.frame_seq,
                    action_index,
                    u32::from(envelope.event_index),
                ),
            }
            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
            (
                envelope.receive_ts.0 / 1_000,
                venue.source_id(),
                epoch.connection_epoch(),
                cursor,
            )
        }
        MechanicsInputRefV1::System {
            system_source,
            available_at,
            system_cursor,
            ..
        } => (
            available_at.utc_micros(),
            system_source.key().source_id(),
            system_source.epoch(),
            system_cursor.clone(),
        ),
        MechanicsInputRefV1::Coverage {
            coverage_source,
            available_at,
            coverage_cursor,
            ..
        } => (
            available_at.utc_micros(),
            coverage_source.key().source_id(),
            coverage_source.epoch(),
            coverage_cursor.cursor().clone(),
        ),
        MechanicsInputRefV1::Clock {
            clock_source,
            available_at,
            clock_cursor,
            ..
        } => (
            available_at.utc_micros(),
            clock_source.key().source_id(),
            clock_source.epoch(),
            clock_cursor.cursor().clone(),
        ),
    };
    let sequence = cursor
        .display_sequence()
        .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
    let (sequence_start, sequence_end) = cursor.native_range().unwrap_or((sequence, sequence));
    Ok(InputOrderKey {
        available_micros,
        source_id: source_id.to_owned(),
        epoch: epoch.to_owned(),
        sequence_start,
        sequence_end,
        payload_hash: input.payload_hash().to_owned(),
    })
}

fn input_subjects(input: &MechanicsInputV1, config: &MechanicsConfigV1) -> Vec<ContributorKeyV1> {
    match input.view() {
        MechanicsInputRefV1::Market {
            envelope, catalog, ..
        } => catalog
            .venue_source(envelope.venue.0)
            .zip(envelope.instrument.and_then(|id| catalog.instrument(id.0)))
            .and_then(|(venue, instrument)| {
                ContributorKeyV1::new(venue.source_id(), instrument.clone()).ok()
            })
            .into_iter()
            .collect(),
        MechanicsInputRefV1::Clock { contributor, .. }
        | MechanicsInputRefV1::Coverage { contributor, .. } => vec![contributor.key().clone()],
        MechanicsInputRefV1::System { scope, .. } => match scope.view() {
            crate::wire::FaultScopeRefV1::Contributor { contributor } => {
                vec![contributor.key().clone()]
            }
            crate::wire::FaultScopeRefV1::ConnectionEpoch { connection_key, .. } => config
                .contributor_connections()
                .iter()
                .filter(|(_, connection)| *connection == connection_key)
                .map(|(key, _)| key.clone())
                .collect(),
            crate::wire::FaultScopeRefV1::Processor { .. } => config
                .contributors()
                .iter()
                .map(|spec| spec.key().clone())
                .collect(),
        },
    }
}

fn snapshot_cursor(
    source_id: &str,
    view: crate::CursorView,
) -> Result<SnapshotCursor, SnapshotError> {
    let (start, end) = view.cursor.native_range().unwrap_or((
        view.cursor
            .display_sequence()
            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
        view.cursor
            .display_sequence()
            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
    ));
    Ok(SnapshotCursor {
        source_id: source_id.to_owned(),
        connection_epoch: view.epoch,
        sequence_start: start,
        sequence_end: end,
        available_at: Rfc3339Time::from_unix_nanos(view.available_at_ns)
            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
        payload_hash: view.payload_hash,
    })
}

fn feature_conditions(
    name: FeatureName,
    value: Option<i128>,
    invalid: bool,
    stale: bool,
    degraded: bool,
    optional: bool,
) -> FeatureConditions {
    let mut conditions = Vec::new();
    if invalid {
        conditions.push(FeatureCondition::SourceInvalidated);
    } else if stale && matches!(name, FeatureName::SpreadBps | FeatureName::BookDepth10bps) {
        conditions.push(FeatureCondition::SourceStale);
    } else if value.is_none() {
        conditions.push(if optional {
            FeatureCondition::OptionalSourceUnavailable
        } else {
            FeatureCondition::InsufficientCoverage
        });
    }
    if degraded {
        conditions.push(FeatureCondition::ClockDegraded);
    }
    FeatureConditions::new(name, conditions).expect("conditions match feature")
}

fn parse_scaled(value: &str) -> Result<i128, SnapshotError> {
    let negative = value.starts_with('-');
    let unsigned = value.trim_start_matches('-');
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let whole = whole
        .parse::<i128>()
        .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
    let mut fraction = fraction.to_owned();
    fraction.push_str(&"0".repeat(8usize.saturating_sub(fraction.len())));
    let fraction = fraction
        .parse::<i128>()
        .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
    let scaled = whole
        .checked_mul(SCALE)
        .and_then(|whole| whole.checked_add(fraction))
        .ok_or(SnapshotError::Capacity)?;
    Ok(if negative { -scaled } else { scaled })
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

#[cfg(test)]
mod bounded_processor_tests {
    use super::{SnapshotError, checked_sum, ensure_record_capacity};
    use crate::window::PER_WINDOW_CAPACITY;

    #[test]
    fn processor_capacity_is_rejected_before_ingest_mutation() {
        assert_eq!(
            ensure_record_capacity(PER_WINDOW_CAPACITY),
            Err(SnapshotError::Capacity)
        );
    }

    #[test]
    fn feature_folds_fail_closed_on_checked_overflow() {
        assert!(matches!(
            checked_sum([i128::MAX, 1]),
            Err(SnapshotError::Contract(_))
        ));
    }
}
