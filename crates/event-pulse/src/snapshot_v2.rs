use std::collections::{BTreeMap, VecDeque};

use marketfeed_model::{MarketEvent, SequenceRange};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::*;
use crate::{
    MarketCursorV2, MechanicsInputRefV2, MechanicsInputV2, SourceStateMachineV2,
    features::{FeatureName, ReversalPolicy, evaluate_feature},
    prospective_v2::{ProspectiveCaptureAdmissionV2, ProspectiveSystemArtifactPolicyV2},
};

pub const SNAPSHOT_V2_ROOT_MERGE: &str = "4d3e0f0398d3e113a79df7ac901f38912eaa8edd";
pub const SNAPSHOT_V2_ROOT_TREE: &str = "273163e3d06578065f7327a90a1b9fbfcded3a6d";
pub const SNAPSHOT_V2_CONTRACT_SHA256: &str =
    "b9062e8e8bdc08e61f92b7890fe4d1dcebbb2eb975cc145c34ddf19f94be28af";

const MAX_E1_DERIVED_FRAME: u64 = 2_147_483_647;
const SNAPSHOT_V2_CONTRACT_BYTES: &[u8] =
    include_bytes!("../contracts/snapshot-v2/event-pulse-e2-snapshot-v2-contract.json");

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SnapshotV2Error {
    #[error("SNAPSHOT_V2_CURSOR_NOT_E1_REPRESENTABLE")]
    CursorNotE1Representable,
    #[error("truthful-empty V2 topology rejects System records")]
    SystemInput,
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
}

#[derive(Debug, Clone)]
pub struct SnapshotProcessorV2 {
    config: MechanicsConfigV1,
    authoring: SnapshotAuthoringV1,
    sources: SourceStateMachineV2,
    runtime: FeatureRuntime,
    records: VecDeque<MechanicsInputV2>,
    last_order: Option<crate::replay_v2::ReplayOrderKeyV2>,
    sealed_micros: Option<i64>,
    last_decision_micros: Option<i64>,
    next_revision: u64,
    predecessor: Option<String>,
    cache: Option<SuccessfulCache>,
    capture_start_micros: i64,
}

impl SnapshotProcessorV2 {
    pub fn new(
        admission: &ProspectiveCaptureAdmissionV2,
        system_policy: &ProspectiveSystemArtifactPolicyV2,
        authoring: SnapshotAuthoringV1,
    ) -> Result<Self, SnapshotV2Error> {
        verify_snapshot_v2_contract()?;
        if !system_policy.matches(admission) || system_policy.mode() != "TRUTHFUL_EMPTY" {
            return Err(
                SnapshotError::InvalidInput("Snapshot V2 System policy mismatch".into()).into(),
            );
        }
        let config = admission.mechanics_config().clone();
        // Reuse the accepted V1 constructor validation without retaining its state.
        MechanicsProcessor::new(config.clone(), authoring.clone())?;
        Ok(Self {
            sources: SourceStateMachineV2::new(config.clone()),
            runtime: FeatureRuntime::new(&config)?,
            records: VecDeque::with_capacity(PROCESSOR_RECORD_CAPACITY),
            last_order: None,
            sealed_micros: None,
            last_decision_micros: None,
            next_revision: authoring.revision_start(),
            predecessor: authoring.predecessor_content_hash().map(str::to_owned),
            config,
            authoring,
            cache: None,
            capture_start_micros: admission.capture_starts_at().utc_micros(),
        })
    }

    pub fn next_revision(&self) -> u64 {
        self.next_revision
    }

    pub fn buffered_record_count(&self) -> usize {
        self.records.len()
    }

    pub fn ingest(&mut self, input: &MechanicsInputV2) -> Result<IngestOutcome, SnapshotV2Error> {
        input
            .validate_static()
            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
        if matches!(
            input.view(),
            MechanicsInputRefV2::NonMarket(MechanicsInputRefV1::System { .. })
        ) {
            return Err(SnapshotV2Error::SystemInput);
        }
        validate_capture_time(input, self.capture_start_micros)?;
        let order = order_v2(input)?;
        if self
            .sealed_micros
            .is_some_and(|sealed| order.available_micros() <= sealed)
        {
            return Err(SnapshotError::SealedInput.into());
        }
        if self.last_order.as_ref().is_some_and(|last| order < *last) {
            return Err(if self
                .last_order
                .as_ref()
                .is_some_and(|last| order.available_micros() < last.available_micros())
            {
                SnapshotError::InputTimeRegression
            } else {
                SnapshotError::InputOrderRegression
            }
            .into());
        }
        if self.records.len() >= PROCESSOR_RECORD_CAPACITY {
            return Err(SnapshotError::Capacity.into());
        }
        let mut candidate_sources = self.sources.clone();
        let outcome = candidate_sources
            .ingest(input)
            .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
        let mut candidate_runtime = self.runtime.clone();
        if outcome != IngestOutcome::IgnoredDuplicate {
            candidate_runtime.ingest_v2(input, &self.config, &candidate_sources)?;
            self.records.push_back(input.clone());
        }
        self.sources = candidate_sources;
        self.runtime = candidate_runtime;
        self.last_order = Some(order);
        self.cache = None;
        Ok(outcome)
    }

    pub fn snapshot(
        &mut self,
        decision_time: Rfc3339Time,
    ) -> Result<AuthoredSnapshot, SnapshotV2Error> {
        let decision_micros = decision_time.utc_micros();
        if decision_micros < self.capture_start_micros {
            return Err(SnapshotError::FutureAvailability.into());
        }
        if let Some(cache) = &self.cache {
            if cache.decision_micros == decision_micros {
                return Ok(cache.snapshot.clone());
            }
        }
        if self
            .last_decision_micros
            .is_some_and(|last| decision_micros < last)
        {
            return Err(SnapshotError::DecisionTimeRegression.into());
        }

        let (sources, runtime, clocks, available_micros, mut phase) =
            self.replay_prefix(decision_micros)?;
        let cursors = project_cursors(&self.config, &sources)?;
        if cursors.len() != 15 {
            return Err(SnapshotError::InvalidInput(
                "complete V2 prefix must project exactly fifteen cursors".into(),
            )
            .into());
        }
        phase
            .advance_to(
                decision_micros
                    .checked_mul(1_000)
                    .ok_or(SnapshotError::Capacity)?,
            )
            .map_err(phase_error)?;
        let mut observation = observation_from_state(
            &self.config,
            &self.authoring,
            &sources,
            &runtime,
            &phase,
            decision_micros,
            available_micros,
            clocks,
        )?;
        observation.cursors = cursors;
        let mut decision_evidence = derive_evidence(&observation)?;
        decision_evidence.available_at_ns = decision_micros
            .checked_mul(1_000)
            .ok_or(SnapshotError::Capacity)?;
        phase.observe(&decision_evidence).map_err(phase_error)?;
        observation = observation_from_state(
            &self.config,
            &self.authoring,
            &sources,
            &runtime,
            &phase,
            decision_micros,
            available_micros,
            observation.clocks,
        )?;
        observation.cursors = project_cursors(&self.config, &sources)?;

        let mut author = MechanicsProcessor::new(self.config.clone(), self.authoring.clone())?;
        author.next_revision = self.next_revision;
        author.predecessor = self.predecessor.clone();
        let following_revision = self
            .next_revision
            .checked_add(1)
            .ok_or(SnapshotError::RevisionOverflow)?;
        let snapshot = author.author(&decision_time, &observation, &phase)?;

        // The four success fields commit together only after canonical E1 validation.
        self.sealed_micros = Some(decision_micros);
        self.last_decision_micros = Some(decision_micros);
        self.predecessor = Some(snapshot.content_hash().to_owned());
        self.next_revision = following_revision;
        self.cache = Some(SuccessfulCache {
            decision_micros,
            snapshot: snapshot.clone(),
        });
        Ok(snapshot)
    }

    fn replay_prefix(&self, decision_micros: i64) -> Result<ReplayStateV2, SnapshotV2Error> {
        let mut sources = SourceStateMachineV2::new(self.config.clone());
        let mut runtime = FeatureRuntime::new(&self.config)?;
        let mut clocks = BTreeMap::new();
        let mut available_micros = i64::MIN;
        let mut phase = PhaseMachine::new();
        let mut group_at = None;
        for input in self.records.iter().filter(|input| {
            order_v2(input).is_ok_and(|order| order.available_micros() <= decision_micros)
        }) {
            let order = order_v2(input)?;
            if group_at.is_some_and(|at| at != order.available_micros()) {
                let observation = observation_from_state(
                    &self.config,
                    &self.authoring,
                    &sources,
                    &runtime,
                    &phase,
                    group_at.expect("group exists"),
                    available_micros,
                    clocks.values().cloned().collect(),
                )?;
                phase
                    .observe(&derive_evidence(&observation)?)
                    .map_err(phase_error)?;
            }
            group_at = Some(order.available_micros());
            let outcome = sources.ingest(input).map_err(|error| {
                SnapshotError::Contract(format!("accepted V2 replay failed: {error}"))
            })?;
            if outcome != IngestOutcome::IgnoredDuplicate {
                runtime.ingest_v2(input, &self.config, &sources)?;
                update_clock_and_availability(input, &mut clocks, &mut available_micros)?;
            }
        }
        if let Some(at) = group_at.filter(|at| *at < decision_micros) {
            let observation = observation_from_state(
                &self.config,
                &self.authoring,
                &sources,
                &runtime,
                &phase,
                at,
                available_micros,
                clocks.values().cloned().collect(),
            )?;
            phase
                .observe(&derive_evidence(&observation)?)
                .map_err(phase_error)?;
        }
        if group_at.is_none() {
            return Err(SnapshotError::MissingCausalAnchor.into());
        }
        Ok((
            sources,
            runtime,
            clocks.into_values().collect(),
            available_micros,
            phase,
        ))
    }
}

fn verify_snapshot_v2_contract() -> Result<(), SnapshotV2Error> {
    if format!("{:x}", Sha256::digest(SNAPSHOT_V2_CONTRACT_BYTES)) != SNAPSHOT_V2_CONTRACT_SHA256 {
        return Err(SnapshotError::Contract("embedded Snapshot V2 contract drift".into()).into());
    }
    let value: serde_json::Value = serde_json::from_slice(SNAPSHOT_V2_CONTRACT_BYTES)
        .map_err(|error| SnapshotError::Contract(error.to_string()))?;
    if value["schema"] != "event-pulse-e2-snapshot-v2-contract/1.0"
        || value["status"] != "SNAPSHOT_V2_CONTRACT_ONLY"
        || value["input_prefix"]["maximum_total_records"] != 65_536
        || value["snapshot_cursor_cardinality"]["total"] != 15
        || value["authority"].as_object().is_none_or(|authority| {
            authority.len() != 12 || authority.values().any(|item| item != false)
        })
    {
        return Err(SnapshotError::Contract(
            "embedded Snapshot V2 contract semantics drift".into(),
        )
        .into());
    }
    Ok(())
}

type ReplayStateV2 = (
    SourceStateMachineV2,
    FeatureRuntime,
    Vec<ClockEvidence>,
    i64,
    PhaseMachine,
);

impl FeatureRuntime {
    fn ingest_v2(
        &mut self,
        input: &MechanicsInputV2,
        config: &MechanicsConfigV1,
        sources: &SourceStateMachineV2,
    ) -> Result<(), SnapshotError> {
        if let Some(non_market) = input.as_v1_non_market() {
            return self.ingest(non_market, config, sources.v1_state());
        }
        let MechanicsInputRefV2::Market {
            envelope,
            catalog,
            market_cursor,
            payload_hash,
            ..
        } = input.view()
        else {
            unreachable!("non-market returned above")
        };
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
        let at_ns = envelope
            .receive_ts
            .0
            .div_euclid(1_000)
            .checked_mul(1_000)
            .ok_or(SnapshotError::Capacity)?;
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
                let mut projection = self.books.get(&contributor).cloned().ok_or_else(|| {
                    SnapshotError::InvalidInput("unconfigured book family".into())
                })?;
                let (first, last) = market_cursor.native_range().ok_or_else(|| {
                    SnapshotError::InvalidInput("book snapshot requires native cursor".into())
                })?;
                projection
                    .snapshot_native(snapshot, SequenceRange { first, last }, at_ns)
                    .map_err(|error| SnapshotError::Contract(error.to_string()))?;
                self.push(
                    &contributor,
                    250_000_000,
                    WindowKind::Book,
                    at_ns,
                    FeatureSample::Book,
                )?;
                self.books.insert(contributor.clone(), projection);
            }
            MarketEvent::BookDelta(delta) => {
                let mut projection = self.books.get(&contributor).cloned().ok_or_else(|| {
                    SnapshotError::InvalidInput("unconfigured book family".into())
                })?;
                let (first, last) = market_cursor.native_range().ok_or_else(|| {
                    SnapshotError::InvalidInput("book delta requires native cursor".into())
                })?;
                projection
                    .delta_native(delta, SequenceRange { first, last }, at_ns)
                    .map_err(|error| SnapshotError::Contract(error.to_string()))?;
                self.push(
                    &contributor,
                    250_000_000,
                    WindowKind::Book,
                    at_ns,
                    FeatureSample::Book,
                )?;
                self.books.insert(contributor.clone(), projection);
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
            _ => {
                return Err(SnapshotError::InvalidInput(
                    "unsupported V2 market family".into(),
                ));
            }
        }
        let horizon_ns = match envelope.payload {
            MarketEvent::Trade(_) | MarketEvent::OpenInterest(_) | MarketEvent::Liquidation(_) => {
                5_000_000_000
            }
            MarketEvent::Quote(_) | MarketEvent::BookSnapshot(_) | MarketEvent::BookDelta(_) => {
                250_000_000
            }
            MarketEvent::MarkPrice(_) | MarketEvent::IndexPrice(_) => 1_000_000_000,
            _ => unreachable!("unsupported returned above"),
        };
        let source_event_ns = envelope
            .exchange_ts
            .ok_or_else(|| SnapshotError::InvalidInput("source event time".into()))?
            .0;
        let anchor = MarketAnchor {
            source_event_time: Rfc3339Time::from_unix_nanos(source_event_ns)
                .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
            received_at: Rfc3339Time::from_unix_nanos(envelope.receive_ts.0)
                .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
            normalized_at: Rfc3339Time::from_unix_nanos(envelope.receive_ts.0)
                .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
            available_at: Rfc3339Time::from_unix_nanos(envelope.receive_ts.0)
                .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
            payload_hash: payload_hash.to_owned(),
        };
        self.push_causal(
            &contributor,
            CausalRecord {
                family: match envelope.payload {
                    MarketEvent::Trade(_) => FamilyV1::Trade,
                    MarketEvent::Quote(_) => FamilyV1::Quote,
                    MarketEvent::BookSnapshot(_) | MarketEvent::BookDelta(_) => FamilyV1::Book,
                    MarketEvent::OpenInterest(_) => FamilyV1::OpenInterest,
                    MarketEvent::Liquidation(_) => FamilyV1::Liquidation,
                    MarketEvent::MarkPrice(_) | MarketEvent::IndexPrice(_) => {
                        FamilyV1::ConfirmationPrice
                    }
                    _ => unreachable!("unsupported returned above"),
                },
                available_at_ns: at_ns,
                horizon_ns,
                source_event_ns,
                receive_ns: envelope.receive_ts.0,
                normalized_ns: envelope.receive_ts.0,
                exact_anchor: anchor.clone(),
            },
        )?;
        self.retained_anchor = Some(RetainedMarketAnchor {
            owner: contributor.clone(),
            anchor,
            fallback_eligible: sources.market_state(
                &contributor,
                match envelope.payload {
                    MarketEvent::Trade(_) => FamilyV1::Trade,
                    MarketEvent::Quote(_) => FamilyV1::Quote,
                    MarketEvent::BookSnapshot(_) | MarketEvent::BookDelta(_) => FamilyV1::Book,
                    MarketEvent::OpenInterest(_) => FamilyV1::OpenInterest,
                    MarketEvent::Liquidation(_) => FamilyV1::Liquidation,
                    MarketEvent::MarkPrice(_) | MarketEvent::IndexPrice(_) => {
                        FamilyV1::ConfirmationPrice
                    }
                    _ => unreachable!("unsupported returned above"),
                },
            ) != Some(SlotState::Live),
        });
        Ok(())
    }
}

fn order_v2(
    input: &MechanicsInputV2,
) -> Result<crate::replay_v2::ReplayOrderKeyV2, SnapshotV2Error> {
    crate::replay_v2::replay_order_v2(input)
        .map_err(|error| SnapshotError::InvalidInput(error.to_string()).into())
}

fn update_clock_and_availability(
    input: &MechanicsInputV2,
    clocks: &mut BTreeMap<String, ClockEvidence>,
    available_micros: &mut i64,
) -> Result<(), SnapshotError> {
    match input.view() {
        MechanicsInputRefV2::Market { envelope, .. } => {
            *available_micros = (*available_micros).max(envelope.receive_ts.0.div_euclid(1_000));
        }
        MechanicsInputRefV2::NonMarket(MechanicsInputRefV1::Clock {
            clock_source,
            available_at,
            observed_skew_ms,
            freshness_limit_ms,
            clock_state,
            quality_state,
            ..
        }) => {
            let evidence = ClockEvidence {
                source_id: clock_source.key().source_id().to_owned(),
                available_at: available_at.clone(),
                observed_skew_ms: parse_scaled(observed_skew_ms.as_str())?,
                freshness_limit_ms,
                degraded: clock_state == ClockStateV1::Degraded
                    || quality_state == ClockQualityV1::Degraded,
            };
            *available_micros = (*available_micros).max(available_at.utc_micros());
            clocks.insert(evidence.source_id.clone(), evidence);
        }
        MechanicsInputRefV2::NonMarket(MechanicsInputRefV1::Coverage { available_at, .. }) => {
            *available_micros = (*available_micros).max(available_at.utc_micros())
        }
        MechanicsInputRefV2::NonMarket(MechanicsInputRefV1::System { .. }) => {
            return Err(SnapshotError::InvalidInput("truthful-empty System".into()));
        }
        MechanicsInputRefV2::NonMarket(MechanicsInputRefV1::Market { .. }) => {
            return Err(SnapshotError::InvalidInput(
                "V1 market lowering is forbidden".into(),
            ));
        }
    }
    Ok(())
}

fn validate_capture_time(
    input: &MechanicsInputV2,
    capture_start_micros: i64,
) -> Result<(), SnapshotV2Error> {
    let valid = match input.view() {
        MechanicsInputRefV2::Market { envelope, .. } => {
            envelope.receive_ts.0.div_euclid(1_000) >= capture_start_micros
                && envelope
                    .exchange_ts
                    .is_some_and(|time| time.0.div_euclid(1_000) >= capture_start_micros)
        }
        MechanicsInputRefV2::NonMarket(MechanicsInputRefV1::Clock {
            observed_at,
            available_at,
            ..
        }) => {
            observed_at.utc_micros() >= capture_start_micros
                && available_at.utc_micros() >= capture_start_micros
        }
        MechanicsInputRefV2::NonMarket(MechanicsInputRefV1::Coverage {
            covered_from,
            covered_through,
            available_at,
            ..
        }) => {
            covered_from.utc_micros() >= capture_start_micros
                && covered_through.utc_micros() >= capture_start_micros
                && available_at.utc_micros() >= capture_start_micros
        }
        MechanicsInputRefV2::NonMarket(MechanicsInputRefV1::System { .. })
        | MechanicsInputRefV2::NonMarket(MechanicsInputRefV1::Market { .. }) => false,
    };
    if valid {
        Ok(())
    } else {
        Err(SnapshotError::InvalidInput("input precedes admitted capture start".into()).into())
    }
}

fn observation_from_state(
    config: &MechanicsConfigV1,
    authoring: &SnapshotAuthoringV1,
    sources: &SourceStateMachineV2,
    runtime: &FeatureRuntime,
    phase: &PhaseMachine,
    decision_micros: i64,
    available_micros: i64,
    clocks: Vec<ClockEvidence>,
) -> Result<SnapshotObservation, SnapshotV2Error> {
    let mut proxy = MechanicsProcessor::new(config.clone(), authoring.clone())?;
    let placeholder = placeholder_observation(available_micros, clocks)?;
    proxy.checkpoint = Some(ReplayCheckpoint {
        at_ns: decision_micros
            .checked_mul(1_000)
            .ok_or(SnapshotError::Capacity)?,
        sources: sources.v1_state().clone(),
        runtime: runtime.clone(),
        causes: configured_cause_keys(config)
            .into_iter()
            .map(|key| (key, Cause::None))
            .collect(),
        master_queue_drops: configured_cause_keys(config)
            .into_iter()
            .map(|key| (key, None))
            .collect(),
        phase: phase.clone(),
        observation: placeholder,
    });
    proxy.family_eligibility = Some(
        config
            .contributors()
            .iter()
            .flat_map(|spec| {
                spec.allowed_families().iter().map(move |family| {
                    let cursor = sources.market_cursor(spec.key(), *family);
                    (
                        (spec.key().clone(), *family),
                        FamilyEligibility {
                            state: sources
                                .market_state(spec.key(), *family)
                                .unwrap_or(SlotState::Cold),
                            invalid: sources.market_invalidity(spec.key(), *family).is_some(),
                            generation: cursor.as_ref().map_or(0, |view| view.epoch_generation),
                        },
                    )
                })
            })
            .collect(),
    );
    proxy
        .derive_owned_observation(decision_micros, phase.phase())
        .map_err(Into::into)
}

fn placeholder_observation(
    available_micros: i64,
    clocks: Vec<ClockEvidence>,
) -> Result<SnapshotObservation, SnapshotV2Error> {
    let policy = ReversalPolicy::PreEventZero;
    let rows = FeatureName::CANONICAL
        .iter()
        .map(|(name, _)| {
            let conditions =
                feature_conditions(*name, FeatureOutcomeCause::InsufficientCoverage, false);
            if *name == FeatureName::ReversalFromExtreme {
                evaluate_reversal(policy, 1, 1, 1, &conditions)
            } else {
                evaluate_feature(*name, None, &conditions, policy)
            }
            .map_err(|error| SnapshotError::Contract(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SnapshotObservation {
        available_at: Rfc3339Time::from_unix_nanos(
            available_micros
                .checked_mul(1_000)
                .ok_or(SnapshotError::Capacity)?,
        )
        .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
        features: FeatureSet::new(rows, policy)
            .map_err(|error| SnapshotError::Contract(error.to_string()))?,
        flag_conditions: FlagConditions::default(),
        liquidation_confirms_direction: false,
        fully_warmed: false,
        critical_fault: false,
        anchor: None,
        cursors: Vec::new(),
        required_clock_sources: Vec::new(),
        clocks,
    })
}

fn project_cursors(
    config: &MechanicsConfigV1,
    sources: &SourceStateMachineV2,
) -> Result<Vec<SnapshotCursor>, SnapshotV2Error> {
    let mut cursors = Vec::with_capacity(15);
    for (source, family, expected_kind) in [
        (
            "binance_primary_public_quote",
            FamilyV1::Quote,
            "binance_primary_public",
        ),
        (
            "binance_primary_public_book",
            FamilyV1::Book,
            "binance_primary_public",
        ),
        (
            "binance_primary_market_trade",
            FamilyV1::Trade,
            "binance_primary_market",
        ),
        (
            "binance_primary_market_open_interest",
            FamilyV1::OpenInterest,
            "binance_primary_market",
        ),
        (
            "binance_primary_market_liquidation",
            FamilyV1::Liquidation,
            "binance_primary_market",
        ),
        (
            "hyperliquid_confirmation_price",
            FamilyV1::ConfirmationPrice,
            "hyperliquid_confirmation",
        ),
    ] {
        let contributor = config
            .contributors()
            .iter()
            .find(|spec| {
                spec.key().source_id() == expected_kind && spec.allowed_families().contains(&family)
            })
            .ok_or_else(|| SnapshotError::InvalidInput("Snapshot V2 topology mismatch".into()))?;
        let view = sources
            .market_cursor(contributor.key(), family)
            .ok_or_else(|| {
                SnapshotError::InvalidInput("missing current V2 family cursor".into())
            })?;
        let (start, end) = e1_range(&view.cursor)?;
        cursors.push(SnapshotCursor {
            source_id: source.to_owned(),
            connection_epoch: view.epoch,
            sequence_start: start,
            sequence_end: end,
            available_at: Rfc3339Time::from_unix_nanos(view.available_at_ns)
                .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?,
            payload_hash: view.payload_hash,
        });
    }
    let v1 = sources.v1_state();
    for key in config.clock_sources() {
        if let Some(view) = v1.clock_cursor(key) {
            cursors.push(snapshot_cursor(key.source_id(), view)?);
        }
    }
    for key in config.coverage_sources() {
        if let Some(view) = v1.coverage_cursor(key) {
            cursors.push(snapshot_cursor(key.source_id(), view)?);
        }
    }
    cursors.sort_by(|left, right| {
        left.available_at
            .utc_micros()
            .cmp(&right.available_at.utc_micros())
            .then_with(|| left.source_id.cmp(&right.source_id))
            .then_with(|| left.connection_epoch.cmp(&right.connection_epoch))
            .then_with(|| left.sequence_start.cmp(&right.sequence_start))
            .then_with(|| left.sequence_end.cmp(&right.sequence_end))
            .then_with(|| left.payload_hash.cmp(&right.payload_hash))
    });
    if cursors.windows(2).any(|pair| {
        pair[0].available_at == pair[1].available_at
            && pair[0].source_id == pair[1].source_id
            && pair[0].connection_epoch == pair[1].connection_epoch
            && pair[0].sequence_start == pair[1].sequence_start
            && pair[0].sequence_end == pair[1].sequence_end
            && pair[0].payload_hash == pair[1].payload_hash
    }) {
        return Err(SnapshotError::CursorConflict.into());
    }
    Ok(cursors)
}

fn e1_range(cursor: &MarketCursorV2) -> Result<(u64, u64), SnapshotV2Error> {
    match cursor {
        MarketCursorV2::Native {
            first_sequence,
            last_sequence,
        } => Ok((*first_sequence, *last_sequence)),
        MarketCursorV2::Derived {
            raw_frame_seq,
            action_index,
            item_index,
        } => {
            if *raw_frame_seq > MAX_E1_DERIVED_FRAME {
                return Err(SnapshotV2Error::CursorNotE1Representable);
            }
            let value = raw_frame_seq
                .checked_mul(1u64 << 32)
                .and_then(|value| value.checked_add(u64::from(*action_index) << 16))
                .and_then(|value| value.checked_add(u64::from(*item_index)))
                .filter(|value| *value <= i64::MAX as u64)
                .ok_or(SnapshotV2Error::CursorNotE1Representable)?;
            Ok((value, value))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_e1_boundary_and_euclidean_timestamp_floor_are_exact() {
        let maximum = MarketCursorV2::Derived {
            raw_frame_seq: MAX_E1_DERIVED_FRAME,
            action_index: 65_534,
            item_index: 65_535,
        };
        assert_eq!(
            e1_range(&maximum).unwrap(),
            (9_223_372_036_854_710_271, 9_223_372_036_854_710_271)
        );
        assert_eq!(
            e1_range(&MarketCursorV2::Derived {
                raw_frame_seq: MAX_E1_DERIVED_FRAME + 1,
                action_index: 0,
                item_index: 0,
            }),
            Err(SnapshotV2Error::CursorNotE1Representable)
        );
        assert_eq!(
            Rfc3339Time::from_unix_nanos(1_001).unwrap().canonical(),
            "1970-01-01T00:00:00.000001Z"
        );
        assert_eq!(
            Rfc3339Time::from_unix_nanos(-1).unwrap().canonical(),
            "1969-12-31T23:59:59.999999Z"
        );
    }
}
