use std::collections::{BTreeMap, VecDeque};

use marketfeed_model::{MarketEvent, SequenceRange};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::*;
use crate::{
    MarketCursorV2, MechanicsInputRefV2, MechanicsInputV2, SourceStateMachineV2,
    features::{FeatureName, ReversalPolicy, evaluate_feature},
    prospective_v2::{ProspectiveCaptureAdmissionV2, ProspectiveSystemArtifactPolicyV2},
    wire::ConnectionKeyV1,
};

pub const SNAPSHOT_V2_ROOT_MERGE: &str = "4d3e0f0398d3e113a79df7ac901f38912eaa8edd";
pub const SNAPSHOT_V2_ROOT_TREE: &str = "273163e3d06578065f7327a90a1b9fbfcded3a6d";
pub const SNAPSHOT_V2_CONTRACT_SHA256: &str =
    "b9062e8e8bdc08e61f92b7890fe4d1dcebbb2eb975cc145c34ddf19f94be28af";

const MAX_E1_DERIVED_FRAME: u64 = 2_147_483_647;
const MAX_FAULT_EVENTS: usize = 15 * 256;
const SNAPSHOT_V2_CONTRACT_BYTES: &[u8] =
    include_bytes!("../contracts/snapshot-v2/event-pulse-e2-snapshot-v2-contract.json");

type FamilyKeyV2 = (ContributorKeyV1, FamilyV1);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum FaultKeyV2 {
    MarketFamily(FamilyKeyV2),
    Clock(ClockSourceKeyV1),
    Coverage(CoverageSourceKeyV1),
}

#[derive(Debug, Clone)]
struct FaultEventV2 {
    order: crate::replay_v2::ReplayOrderKeyV2,
    key: FaultKeyV2,
    generation: u8,
    cause: Cause,
    kind: FaultKindV2,
}

#[derive(Debug, Clone)]
enum FaultKindV2 {
    RejectedInput(MechanicsInputV2),
    QueueDrop { epoch: String, available_at_ns: i64 },
}

#[derive(Debug)]
struct FaultIdentityV2 {
    key: FaultKeyV2,
    epoch: String,
    generation: u8,
    connection: Option<ConnectionKeyV1>,
    subject_generation: Option<u8>,
    available_at_ns: i64,
}

#[derive(Debug, Clone)]
struct RecoverySessionV2 {
    fault_generation: u8,
    recovery_generation: Option<u8>,
    remaining: u8,
    connection: Option<ConnectionKeyV1>,
    connection_generation: Option<u8>,
    connection_trigger: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SnapshotV2Error {
    #[error("SNAPSHOT_V2_CURSOR_NOT_E1_REPRESENTABLE")]
    CursorNotE1Representable,
    #[error("truthful-empty V2 topology rejects System records")]
    SystemInput,
    #[error("SNAPSHOT_V2_RECOVERY_RESERVE_EXHAUSTED")]
    RecoveryReserveExhausted,
    #[error("SNAPSHOT_V2_FAULT_RESERVE_EXHAUSTED")]
    FaultReserveExhausted,
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
    fault_events: VecDeque<FaultEventV2>,
    active_causes: BTreeMap<FaultKeyV2, Cause>,
    recovery_sessions: BTreeMap<FaultKeyV2, RecoverySessionV2>,
    reserved_recovery_used: BTreeMap<FaultKeyV2, u8>,
    reserved_fault_used: BTreeMap<FaultKeyV2, bool>,
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
        let fault_keys = configured_fault_keys_v2(&config);
        Ok(Self {
            sources: SourceStateMachineV2::new(config.clone()),
            runtime: FeatureRuntime::new(&config)?,
            records: VecDeque::with_capacity(PROCESSOR_RECORD_CAPACITY),
            fault_events: VecDeque::with_capacity(MAX_FAULT_EVENTS),
            active_causes: fault_keys
                .iter()
                .cloned()
                .map(|key| (key, Cause::None))
                .collect(),
            recovery_sessions: BTreeMap::new(),
            reserved_recovery_used: fault_keys.iter().cloned().map(|key| (key, 0)).collect(),
            reserved_fault_used: fault_keys.into_iter().map(|key| (key, false)).collect(),
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
        self.records.len() + self.fault_events.len()
    }

    pub fn ordinary_record_capacity(&self) -> usize {
        PROCESSOR_RECORD_CAPACITY - self.recovery_record_reserve() - self.active_causes.len()
    }

    pub fn recovery_record_reserve(&self) -> usize {
        self.active_causes
            .keys()
            .map(|key| match key {
                FaultKeyV2::MarketFamily(_) => 2,
                FaultKeyV2::Clock(_) | FaultKeyV2::Coverage(_) => 1,
            })
            .sum()
    }

    fn recovery_reserve_for(key: &FaultKeyV2) -> u8 {
        match key {
            FaultKeyV2::MarketFamily(_) => 2,
            FaultKeyV2::Clock(_) | FaultKeyV2::Coverage(_) => 1,
        }
    }

    fn ordinary_record_usage(&self) -> usize {
        let reserved_recoveries = self
            .reserved_recovery_used
            .values()
            .map(|used| usize::from(*used))
            .sum::<usize>();
        let reserved_faults = self
            .reserved_fault_used
            .values()
            .filter(|used| **used)
            .count();
        self.buffered_record_count() - reserved_recoveries - reserved_faults
    }

    fn recovery_session_matches(
        &self,
        identity: &FaultIdentityV2,
        input: &MechanicsInputV2,
    ) -> bool {
        let Some(session) = self.recovery_sessions.get(&identity.key) else {
            return false;
        };
        if session.remaining == 0 {
            return false;
        }
        if session.connection.is_some() {
            return session.connection_generation.map_or_else(
                || {
                    (session.connection_trigger && identity.generation > session.fault_generation)
                        || (session.connection_trigger
                            && identity.generation == session.fault_generation
                            && is_book_snapshot(input)
                            && matches!(
                                self.active_causes.get(&identity.key),
                                Some(Cause::Sequence(_))
                            ))
                },
                |generation| match &identity.key {
                    FaultKeyV2::MarketFamily(_) => identity.generation == generation,
                    FaultKeyV2::Clock(_) | FaultKeyV2::Coverage(_) => {
                        identity.subject_generation == Some(generation)
                            && self.active_causes.get(&identity.key).is_none_or(|cause| {
                                *cause == Cause::None
                                    || identity.generation > cause_generation(*cause)
                            })
                    }
                },
            );
        }
        session.recovery_generation.map_or_else(
            || {
                identity.generation > session.fault_generation
                    || (identity.generation == session.fault_generation
                        && is_book_snapshot(input)
                        && matches!(
                            self.active_causes.get(&identity.key),
                            Some(Cause::Sequence(_))
                        ))
            },
            |generation| identity.generation == generation,
        )
    }

    fn recovery_scope_keys(
        &self,
        identity: &FaultIdentityV2,
    ) -> Result<Vec<FaultKeyV2>, SnapshotV2Error> {
        let FaultKeyV2::MarketFamily(_) = &identity.key else {
            return Ok(vec![identity.key.clone()]);
        };
        let connection = identity.connection.as_ref().ok_or_else(|| {
            SnapshotError::InvalidInput("MARKET recovery connection is not configured".into())
        })?;
        let contributors = self
            .config
            .contributor_connections()
            .iter()
            .filter(|(_, configured)| *configured == connection)
            .map(|(contributor, _)| contributor)
            .collect::<BTreeSet<_>>();
        let mut keys = BTreeSet::new();
        for spec in self
            .config
            .contributors()
            .iter()
            .filter(|spec| contributors.contains(spec.key()))
        {
            keys.extend(
                spec.allowed_families()
                    .iter()
                    .map(|family| FaultKeyV2::MarketFamily((spec.key().clone(), *family))),
            );
        }
        keys.extend(
            self.config
                .clock_sources()
                .iter()
                .filter(|key| contributors.contains(key.subject()))
                .cloned()
                .map(FaultKeyV2::Clock),
        );
        keys.extend(
            self.config
                .coverage_sources()
                .iter()
                .filter(|key| contributors.contains(key.subject()))
                .cloned()
                .map(FaultKeyV2::Coverage),
        );
        Ok(keys.into_iter().collect())
    }

    fn install_recovery_plan(
        &self,
        sessions: &mut BTreeMap<FaultKeyV2, RecoverySessionV2>,
        identity: &FaultIdentityV2,
    ) -> Result<(), SnapshotV2Error> {
        let keys = self.recovery_scope_keys(identity)?;
        let scoped_connection = matches!(identity.key, FaultKeyV2::MarketFamily(_))
            .then(|| identity.connection.clone())
            .flatten();
        if let Some(connection) = &scoped_connection {
            if sessions
                .values()
                .any(|session| session.connection.as_ref() == Some(connection))
            {
                return Err(SnapshotV2Error::RecoveryReserveExhausted);
            }
        } else if sessions.contains_key(&identity.key) {
            return Err(SnapshotV2Error::RecoveryReserveExhausted);
        }
        for key in &keys {
            let used = self
                .reserved_recovery_used
                .get(key)
                .copied()
                .expect("recovery reserve is preallocated for every configured key");
            if used
                .checked_add(Self::recovery_reserve_for(key))
                .is_none_or(|required| required > Self::recovery_reserve_for(key))
            {
                return Err(SnapshotV2Error::RecoveryReserveExhausted);
            }
        }
        for key in keys {
            sessions.insert(
                key.clone(),
                RecoverySessionV2 {
                    fault_generation: identity.generation,
                    recovery_generation: None,
                    remaining: Self::recovery_reserve_for(&key),
                    connection: scoped_connection.clone(),
                    connection_generation: None,
                    connection_trigger: key == identity.key,
                },
            );
        }
        Ok(())
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
        let identity = fault_identity(input, &self.config)?;
        let recovering = identity
            .as_ref()
            .is_some_and(|identity| self.recovery_session_matches(identity, input));
        let uses_reserved_recovery =
            recovering && self.ordinary_record_usage() >= self.ordinary_record_capacity();
        if uses_reserved_recovery {
            let identity = identity.as_ref().expect("recovery identity was checked");
            let used = self
                .reserved_recovery_used
                .get(&identity.key)
                .copied()
                .expect("recovery reserve is preallocated for every configured key");
            if used >= Self::recovery_reserve_for(&identity.key) {
                return Err(SnapshotV2Error::RecoveryReserveExhausted);
            }
        }
        if self.buffered_record_count() >= PROCESSOR_RECORD_CAPACITY {
            return Err(SnapshotError::Capacity.into());
        }
        if !recovering && self.ordinary_record_usage() >= self.ordinary_record_capacity() {
            if let Some(identity) = identity {
                self.latch_queue_drop(&order, &identity)?;
                self.last_order = Some(order);
            }
            return Err(SnapshotError::Capacity.into());
        }
        let connection_advance = market_connection_advance(input, &self.config, &self.sources)?;
        let mut candidate_sources = self.sources.clone();
        let outcome = match candidate_sources.ingest(input) {
            Ok(outcome) => outcome,
            Err(error) => {
                if recovering {
                    return Err(SnapshotError::InvalidInput(error.to_string()).into());
                }
                if error.invalidates_state() {
                    if let Some(identity) = identity {
                        let mut runtime = self.runtime.clone();
                        invalidate_fault_key(
                            &mut candidate_sources,
                            &mut runtime,
                            &identity,
                            false,
                        )?;
                        let cause = Cause::Sequence(identity.generation);
                        let mut fault_events = self.fault_events.clone();
                        let mut active_causes = self.active_causes.clone();
                        let fault_count = fault_events.len();
                        push_fault(
                            &mut fault_events,
                            &mut active_causes,
                            self.records.len(),
                            FaultEventV2 {
                                order: order.clone(),
                                key: identity.key.clone(),
                                generation: identity.generation,
                                cause,
                                kind: FaultKindV2::RejectedInput(input.clone()),
                            },
                        )?;
                        let mut recovery_sessions = self.recovery_sessions.clone();
                        if fault_events.len() > fault_count {
                            self.install_recovery_plan(&mut recovery_sessions, &identity)?;
                        }
                        self.sources = candidate_sources;
                        self.runtime = runtime;
                        self.fault_events = fault_events;
                        self.active_causes = active_causes;
                        self.recovery_sessions = recovery_sessions;
                        self.last_order = Some(order);
                    }
                }
                return Err(SnapshotError::InvalidInput(error.to_string()).into());
            }
        };
        let mut candidate_runtime = self.runtime.clone();
        if outcome != IngestOutcome::IgnoredDuplicate {
            if let Some((connection, at_ns)) = &connection_advance {
                invalidate_runtime_connection(
                    &mut candidate_runtime,
                    &self.config,
                    connection,
                    *at_ns,
                )?;
            }
            if is_book_snapshot(input) {
                if let Some(FaultIdentityV2 {
                    key: key @ FaultKeyV2::MarketFamily((contributor, FamilyV1::Book)),
                    ..
                }) = identity.as_ref()
                {
                    if matches!(self.active_causes.get(key), Some(Cause::Sequence(_))) {
                        let at_ns = match input.view() {
                            MechanicsInputRefV2::Market { envelope, .. } => envelope.receive_ts.0,
                            _ => unreachable!(),
                        };
                        candidate_runtime.recover_book_family(contributor, at_ns)?;
                    }
                }
            }
            if let Err(error) = candidate_runtime.ingest_v2(input, &self.config, &candidate_sources)
            {
                if error == SnapshotError::FeatureQueueDrop && !recovering {
                    let identity = identity.ok_or_else(|| {
                        SnapshotError::InvalidInput(
                            "non-market feature capacity has no family slot".into(),
                        )
                    })?;
                    self.latch_queue_drop(&order, &identity)?;
                    self.last_order = Some(order);
                }
                return Err(error.into());
            }
        }
        let mut active_causes = self.active_causes.clone();
        if let Some(identity) = &identity {
            if active_causes
                .get(&identity.key)
                .copied()
                .is_some_and(|cause| {
                    cause != Cause::None && identity.generation > cause_generation(cause)
                })
            {
                active_causes.insert(identity.key.clone(), Cause::None);
            }
            if is_book_snapshot(input)
                && matches!(active_causes.get(&identity.key), Some(Cause::Sequence(_)))
            {
                active_causes.insert(identity.key.clone(), Cause::None);
            }
        }
        let mut recovery_sessions = self.recovery_sessions.clone();
        let mut reserved_recovery_used = self.reserved_recovery_used.clone();
        if recovering && outcome != IngestOutcome::IgnoredDuplicate {
            let identity = identity.as_ref().expect("recovery identity was checked");
            let session = recovery_sessions
                .get_mut(&identity.key)
                .expect("recovery session was checked");
            let same_generation_book_recovery = identity.generation == session.fault_generation;
            let recovery_connection = session.connection.clone();
            if let Some(connection) = &recovery_connection {
                if same_generation_book_recovery {
                    recovery_sessions
                        .retain(|_, candidate| candidate.connection.as_ref() != Some(connection));
                } else if session.connection_generation.is_none() {
                    for candidate in recovery_sessions
                        .values_mut()
                        .filter(|candidate| candidate.connection.as_ref() == Some(connection))
                    {
                        candidate.connection_generation = Some(identity.generation);
                    }
                }
            }
            let session = recovery_sessions.get_mut(&identity.key);
            if let Some(session) = session {
                session
                    .recovery_generation
                    .get_or_insert(identity.generation);
                session.remaining -= 1;
                if same_generation_book_recovery {
                    session.remaining = 0;
                }
                if session.remaining == 0 {
                    recovery_sessions.remove(&identity.key);
                }
            }
            if uses_reserved_recovery {
                *reserved_recovery_used
                    .get_mut(&identity.key)
                    .expect("recovery reserve is preallocated for every configured key") += 1;
            }
        }
        if outcome != IngestOutcome::IgnoredDuplicate {
            self.records.push_back(input.clone());
        }
        self.sources = candidate_sources;
        self.runtime = candidate_runtime;
        self.active_causes = active_causes;
        self.recovery_sessions = recovery_sessions;
        self.reserved_recovery_used = reserved_recovery_used;
        self.last_order = Some(order);
        Ok(outcome)
    }

    fn latch_queue_drop(
        &mut self,
        order: &crate::replay_v2::ReplayOrderKeyV2,
        identity: &FaultIdentityV2,
    ) -> Result<(), SnapshotV2Error> {
        let appends_fault = !self.active_causes.get(&identity.key).is_some_and(|cause| {
            matches!(cause, Cause::QueueDrop(_)) && cause_generation(*cause) >= identity.generation
        });
        let mut recovery_sessions = self.recovery_sessions.clone();
        if appends_fault {
            // A fault may become durable only when its complete, immutable recovery scope is
            // still available.  In particular, do not consume the per-key fault slot and then
            // discover that a sibling on the same connection cannot recover.
            self.install_recovery_plan(&mut recovery_sessions, identity)?;
        }
        let uses_reserved_fault =
            appends_fault && self.ordinary_record_usage() >= self.ordinary_record_capacity();
        if uses_reserved_fault
            && self
                .reserved_fault_used
                .get(&identity.key)
                .copied()
                .expect("fault reserve is preallocated for every configured key")
        {
            return Err(SnapshotV2Error::FaultReserveExhausted);
        }
        let mut sources = self.sources.clone();
        let mut runtime = self.runtime.clone();
        invalidate_fault_key(&mut sources, &mut runtime, identity, true)?;
        let mut fault_events = self.fault_events.clone();
        let mut active_causes = self.active_causes.clone();
        push_fault(
            &mut fault_events,
            &mut active_causes,
            self.records.len(),
            FaultEventV2 {
                order: order.clone(),
                key: identity.key.clone(),
                generation: identity.generation,
                cause: Cause::QueueDrop(identity.generation),
                kind: FaultKindV2::QueueDrop {
                    epoch: identity.epoch.clone(),
                    available_at_ns: identity.available_at_ns,
                },
            },
        )?;
        self.sources = sources;
        self.runtime = runtime;
        self.fault_events = fault_events;
        self.active_causes = active_causes;
        self.recovery_sessions = recovery_sessions;
        if appends_fault && uses_reserved_fault {
            *self
                .reserved_fault_used
                .get_mut(&identity.key)
                .expect("fault reserve is preallocated for every configured key") = true;
        }
        Ok(())
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

        let (sources, runtime, clocks, available_micros, mut phase, replay_causes) =
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
            &replay_causes,
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
            &replay_causes,
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
        let mut replay_causes = self
            .active_causes
            .keys()
            .cloned()
            .map(|key| (key, Cause::None))
            .collect::<BTreeMap<_, _>>();
        let mut available_micros = i64::MIN;
        let mut phase = PhaseMachine::new();
        let mut group_at = None;
        enum ReplayItemV2<'a> {
            Accepted(&'a MechanicsInputV2),
            Fault(&'a FaultEventV2),
        }
        let timeline_capacity = self
            .records
            .len()
            .checked_add(self.fault_events.len())
            .ok_or(SnapshotError::Capacity)?;
        let mut timeline = Vec::with_capacity(timeline_capacity);
        for input in &self.records {
            timeline.push((order_v2(input)?, ReplayItemV2::Accepted(input)));
        }
        for fault in &self.fault_events {
            timeline.push((fault.order.clone(), ReplayItemV2::Fault(fault)));
        }
        timeline.sort_by(|left, right| left.0.cmp(&right.0));
        for (order, item) in timeline
            .into_iter()
            .filter(|(order, _)| order.available_micros() <= decision_micros)
        {
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
                    &replay_causes,
                )?;
                phase
                    .observe(&derive_evidence(&observation)?)
                    .map_err(phase_error)?;
            }
            group_at = Some(order.available_micros());
            match item {
                ReplayItemV2::Accepted(input) => {
                    let connection_advance =
                        market_connection_advance(input, &self.config, &sources)?;
                    let outcome = sources.ingest(input).map_err(|error| {
                        SnapshotError::Contract(format!("accepted V2 replay failed: {error}"))
                    })?;
                    if outcome != IngestOutcome::IgnoredDuplicate {
                        if let Some((connection, at_ns)) = &connection_advance {
                            invalidate_runtime_connection(
                                &mut runtime,
                                &self.config,
                                connection,
                                *at_ns,
                            )?;
                        }
                        if is_book_snapshot(input) {
                            if let Some(identity) = fault_identity(input, &self.config)? {
                                if matches!(
                                    replay_causes.get(&identity.key),
                                    Some(Cause::Sequence(_))
                                ) {
                                    let at_ns = match input.view() {
                                        MechanicsInputRefV2::Market { envelope, .. } => {
                                            envelope.receive_ts.0
                                        }
                                        _ => unreachable!(),
                                    };
                                    if let FaultKeyV2::MarketFamily((contributor, _)) =
                                        &identity.key
                                    {
                                        runtime.recover_book_family(contributor, at_ns)?;
                                    }
                                }
                            }
                        }
                        runtime.ingest_v2(input, &self.config, &sources)?;
                        update_clock_and_availability(input, &mut clocks, &mut available_micros)?;
                    }
                    if let Some(identity) = fault_identity(input, &self.config)? {
                        if replay_causes
                            .get(&identity.key)
                            .copied()
                            .is_some_and(|cause| {
                                cause != Cause::None
                                    && identity.generation > cause_generation(cause)
                            })
                        {
                            replay_causes.insert(identity.key.clone(), Cause::None);
                        }
                        if is_book_snapshot(input)
                            && matches!(replay_causes.get(&identity.key), Some(Cause::Sequence(_)))
                        {
                            replay_causes.insert(identity.key, Cause::None);
                        }
                    }
                }
                ReplayItemV2::Fault(fault) => {
                    match &fault.kind {
                        FaultKindV2::RejectedInput(input) => {
                            if sources.ingest(input).is_ok() {
                                return Err(SnapshotError::Contract(
                                    "rejected V2 state replay unexpectedly succeeded".into(),
                                )
                                .into());
                            }
                        }
                        FaultKindV2::QueueDrop {
                            epoch,
                            available_at_ns,
                        } => {
                            let identity = FaultIdentityV2 {
                                key: fault.key.clone(),
                                epoch: epoch.clone(),
                                generation: fault.generation,
                                connection: connection_for_fault_key(&self.config, &fault.key),
                                subject_generation: None,
                                available_at_ns: *available_at_ns,
                            };
                            invalidate_fault_key(&mut sources, &mut runtime, &identity, true)?;
                            available_micros =
                                available_micros.max(available_at_ns.div_euclid(1_000));
                        }
                    }
                    if matches!(fault.kind, FaultKindV2::RejectedInput(_)) {
                        let identity = fault_identity(
                            match &fault.kind {
                                FaultKindV2::RejectedInput(input) => input,
                                FaultKindV2::QueueDrop { .. } => unreachable!(),
                            },
                            &self.config,
                        )?
                        .ok_or_else(|| SnapshotError::Contract("fault identity missing".into()))?;
                        invalidate_fault_key(&mut sources, &mut runtime, &identity, false)?;
                    }
                    replay_causes.insert(fault.key.clone(), fault.cause);
                }
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
                &replay_causes,
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
            replay_causes,
        ))
    }
}

fn is_book_snapshot(input: &MechanicsInputV2) -> bool {
    matches!(
        input.view(),
        MechanicsInputRefV2::Market { envelope, .. }
            if matches!(envelope.payload, MarketEvent::BookSnapshot(_))
    )
}

fn push_fault(
    events: &mut VecDeque<FaultEventV2>,
    active: &mut BTreeMap<FaultKeyV2, Cause>,
    accepted_count: usize,
    event: FaultEventV2,
) -> Result<(), SnapshotV2Error> {
    let current = active
        .get(&event.key)
        .copied()
        .ok_or_else(|| SnapshotError::InvalidInput("unconfigured family cause slot".into()))?;
    if current != Cause::None
        && cause_generation(current) >= event.generation
        && !(matches!(event.cause, Cause::QueueDrop(_))
            && !matches!(current, Cause::QueueDrop(_))
            && cause_generation(current) == event.generation)
    {
        return Ok(());
    }
    if events.len() >= MAX_FAULT_EVENTS
        || accepted_count
            .checked_add(events.len())
            .is_none_or(|count| count >= PROCESSOR_RECORD_CAPACITY)
    {
        return Err(SnapshotError::Capacity.into());
    }
    active.insert(event.key.clone(), event.cause);
    events.push_back(event);
    Ok(())
}

fn cause_generation(cause: Cause) -> u8 {
    match cause {
        Cause::Sequence(generation)
        | Cause::Book(generation)
        | Cause::QueueDrop(generation)
        | Cause::Warmup(generation) => generation,
        Cause::None => 0,
    }
}

fn fault_identity(
    input: &MechanicsInputV2,
    config: &MechanicsConfigV1,
) -> Result<Option<FaultIdentityV2>, SnapshotV2Error> {
    let MechanicsInputRefV2::Market {
        envelope, catalog, ..
    } = input.view()
    else {
        return match input.view() {
            MechanicsInputRefV2::NonMarket(MechanicsInputRefV1::Clock {
                contributor,
                clock_source,
                available_at,
                ..
            }) => Ok(Some(FaultIdentityV2 {
                key: FaultKeyV2::Clock(clock_source.key().clone()),
                epoch: clock_source.epoch().to_owned(),
                generation: clock_source.epoch_generation(),
                connection: config
                    .contributor_connections()
                    .get(contributor.key())
                    .cloned(),
                subject_generation: Some(contributor.epoch_generation()),
                available_at_ns: available_at
                    .utc_micros()
                    .checked_mul(1_000)
                    .ok_or(SnapshotError::Capacity)?,
            })),
            MechanicsInputRefV2::NonMarket(MechanicsInputRefV1::Coverage {
                contributor,
                coverage_source,
                available_at,
                ..
            }) => Ok(Some(FaultIdentityV2 {
                key: FaultKeyV2::Coverage(coverage_source.key().clone()),
                epoch: coverage_source.epoch().to_owned(),
                generation: coverage_source.epoch_generation(),
                connection: config
                    .contributor_connections()
                    .get(contributor.key())
                    .cloned(),
                subject_generation: Some(contributor.epoch_generation()),
                available_at_ns: available_at
                    .utc_micros()
                    .checked_mul(1_000)
                    .ok_or(SnapshotError::Capacity)?,
            })),
            _ => Ok(None),
        };
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
    let family = match envelope.payload {
        MarketEvent::Trade(_) => FamilyV1::Trade,
        MarketEvent::Quote(_) => FamilyV1::Quote,
        MarketEvent::BookSnapshot(_) | MarketEvent::BookDelta(_) => FamilyV1::Book,
        MarketEvent::OpenInterest(_) => FamilyV1::OpenInterest,
        MarketEvent::Liquidation(_) => FamilyV1::Liquidation,
        MarketEvent::MarkPrice(_) | MarketEvent::IndexPrice(_) => FamilyV1::ConfirmationPrice,
        _ => {
            return Err(SnapshotError::InvalidInput("unsupported V2 market family".into()).into());
        }
    };
    let epoch = catalog
        .connection_epochs()
        .iter()
        .find(|entry| {
            entry.connection_id() == envelope.connection.0
                && entry.session_id() == envelope.session.0
        })
        .ok_or_else(|| SnapshotError::InvalidInput("epoch mapping".into()))?;
    let available_at_ns = envelope
        .receive_ts
        .0
        .div_euclid(1_000)
        .checked_mul(1_000)
        .ok_or(SnapshotError::Capacity)?;
    let connection = config
        .contributor_connections()
        .get(&contributor)
        .cloned()
        .ok_or_else(|| SnapshotError::InvalidInput("connection mapping".into()))?;
    Ok(Some(FaultIdentityV2 {
        key: FaultKeyV2::MarketFamily((contributor, family)),
        epoch: epoch.connection_epoch().to_owned(),
        generation: epoch.epoch_generation(),
        connection: Some(connection),
        subject_generation: Some(epoch.epoch_generation()),
        available_at_ns,
    }))
}

fn configured_fault_keys_v2(config: &MechanicsConfigV1) -> Vec<FaultKeyV2> {
    let mut keys = config
        .contributors()
        .iter()
        .flat_map(|spec| {
            spec.allowed_families()
                .iter()
                .map(move |family| FaultKeyV2::MarketFamily((spec.key().clone(), *family)))
        })
        .collect::<Vec<_>>();
    keys.extend(
        config
            .clock_sources()
            .iter()
            .cloned()
            .map(FaultKeyV2::Clock),
    );
    keys.extend(
        config
            .coverage_sources()
            .iter()
            .cloned()
            .map(FaultKeyV2::Coverage),
    );
    keys
}

fn connection_for_fault_key(
    config: &MechanicsConfigV1,
    key: &FaultKeyV2,
) -> Option<ConnectionKeyV1> {
    let contributor = match key {
        FaultKeyV2::MarketFamily((contributor, _)) => contributor,
        FaultKeyV2::Clock(key) => key.subject(),
        FaultKeyV2::Coverage(key) => key.subject(),
    };
    config.contributor_connections().get(contributor).cloned()
}

fn market_connection_advance(
    input: &MechanicsInputV2,
    config: &MechanicsConfigV1,
    sources: &SourceStateMachineV2,
) -> Result<Option<(ConnectionKeyV1, i64)>, SnapshotV2Error> {
    let Some(identity) = fault_identity(input, config)? else {
        return Ok(None);
    };
    if !matches!(identity.key, FaultKeyV2::MarketFamily(_)) {
        return Ok(None);
    }
    let connection = identity.connection.ok_or_else(|| {
        SnapshotError::InvalidInput("MARKET recovery connection is not configured".into())
    })?;
    Ok(sources
        .connection_generation(&connection)
        .is_some_and(|current| identity.generation > current)
        .then_some((connection, identity.available_at_ns)))
}

fn invalidate_runtime_connection(
    runtime: &mut FeatureRuntime,
    config: &MechanicsConfigV1,
    connection: &ConnectionKeyV1,
    at_ns: i64,
) -> Result<(), SnapshotV2Error> {
    let contributors = config
        .contributor_connections()
        .iter()
        .filter(|(_, configured)| *configured == connection)
        .map(|(contributor, _)| contributor)
        .collect::<BTreeSet<_>>();
    for key in config
        .coverage_sources()
        .iter()
        .filter(|key| contributors.contains(key.subject()))
    {
        runtime.invalidate_coverage(key, at_ns)?;
    }
    Ok(())
}

fn invalidate_fault_key(
    sources: &mut SourceStateMachineV2,
    runtime: &mut FeatureRuntime,
    identity: &FaultIdentityV2,
    queue_drop: bool,
) -> Result<(), SnapshotV2Error> {
    match &identity.key {
        FaultKeyV2::MarketFamily((contributor, family)) => {
            if queue_drop {
                sources
                    .invalidate_market_family_for_queue_drop(
                        contributor,
                        *family,
                        &identity.epoch,
                        identity.generation,
                        identity.available_at_ns,
                    )
                    .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
            }
            runtime.invalidate_family(contributor, *family)?;
        }
        FaultKeyV2::Clock(key) => {
            if queue_drop {
                sources
                    .invalidate_clock_for_queue_drop(key)
                    .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
            }
        }
        FaultKeyV2::Coverage(key) => {
            if queue_drop {
                sources
                    .invalidate_coverage_for_queue_drop(key)
                    .map_err(|error| SnapshotError::InvalidInput(error.to_string()))?;
            }
            runtime.invalidate_coverage(key, identity.available_at_ns)?;
        }
    }
    Ok(())
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
    BTreeMap<FaultKeyV2, Cause>,
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
                    .delta_native_v2(delta, SequenceRange { first, last }, at_ns)
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
    replay_causes: &BTreeMap<FaultKeyV2, Cause>,
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
            .map(|key| {
                let cause = match &key {
                    CauseKey::Clock(clock) => replay_causes
                        .get(&FaultKeyV2::Clock(clock.clone()))
                        .copied()
                        .unwrap_or(Cause::None),
                    CauseKey::Coverage(coverage) => replay_causes
                        .get(&FaultKeyV2::Coverage(coverage.clone()))
                        .copied()
                        .unwrap_or(Cause::None),
                    _ => Cause::None,
                };
                (key, cause)
            })
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
                            cause: replay_causes
                                .get(&FaultKeyV2::MarketFamily((spec.key().clone(), *family)))
                                .copied()
                                .unwrap_or(Cause::None),
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
