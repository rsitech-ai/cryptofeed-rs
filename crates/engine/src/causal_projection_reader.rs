use super::*;

#[derive(Debug)]
pub struct ProjectionReader {
    reader: FeedReader,
    limits: ProjectionLimits,
    policy: Option<ProjectionPolicy>,
    source: Option<FeedCursor>,
    owners: BTreeMap<u64, Owner>,
    bootstrap_bytes: usize,
    catalog_entries: usize,
    sealed: bool,
    poisoned: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionProgress {
    pub target_cursor: FeedCursor,
    pub source_cursor: Option<FeedCursor>,
    pub policy_id: Option<String>,
    pub sealed: bool,
    pub owners: Vec<ProjectionOwnerProgress>,
}
#[derive(Debug)]
struct Owner {
    bootstrap: Box<BootstrapDto>,
    identity: OperationIdentity,
    ordinal: u64,
    generation: u64,
    observed: u64,
    origin: Option<i64>,
    native: u64,
    accepted: u64,
    fence: u64,
    terminal: bool,
    source: u64,
    source_active: bool,
    physical: u64,
    physical_active: bool,
    runner_connection: u64,
    frame_seq: u64,
    wire: Option<WireLedger>,
    pending: BTreeMap<u64, Pending>,
    permit: Option<Permit>,
    active: Option<Active>,
}
#[derive(Debug)]
struct Pending {
    connection: u64,
    deadline: u64,
    owned_bytes: u64,
    input: ProjectionInput,
}
#[derive(Debug)]
struct Permit {
    input: u64,
    ordinal: u64,
    native: u64,
    generation: u64,
    indeterminate: bool,
}
#[derive(Debug)]
struct Active {
    input: u64,
    ordinal: u64,
    route: CausalRoute,
    release_blocked: bool,
    connection: u64,
    frame_seq: u64,
    intent: Option<(u64, bool)>,
}
impl ProjectionInput {
    fn frame(&self) -> bool {
        matches!(
            self,
            Self::Text(_)
                | Self::Binary(_)
                | Self::Ping(_)
                | Self::Pong(_)
                | Self::Close(_)
                | Self::Http { .. }
        )
    }
    fn safety(&self) -> bool {
        matches!(
            self,
            Self::Connected { .. }
                | Self::Disconnected { .. }
                | Self::TransportLost { .. }
                | Self::Close(_)
                | Self::Stop
                | Self::Control {
                    command: ProjectionCommand::Stop,
                    ..
                }
        )
    }
    fn received(&self) -> Option<FrameStamp> {
        match self {
            Self::Text(stamp)
            | Self::Binary(stamp)
            | Self::Ping(stamp)
            | Self::Pong(stamp)
            | Self::Close(stamp)
            | Self::Http {
                received: stamp, ..
            } => Some(*stamp),
            _ => None,
        }
    }
    fn now(&self) -> Option<i64> {
        match self {
            Self::Connected { now, .. }
            | Self::Disconnected { now, .. }
            | Self::TransportLost { now, .. }
            | Self::TimerPoll { now }
            | Self::Control { now, .. } => Some(*now),
            Self::Stop => None,
            _ => self.received().map(|stamp| stamp.receive_ts.0),
        }
    }
}
impl Owner {
    fn new(bootstrap: Box<BootstrapDto>, identity: OperationIdentity) -> Self {
        let runner_connection = bootstrap.runner_policy.initial_connection;
        Self {
            wire: bootstrap
                .public_inputs
                .iter()
                .any(|p| p.role == WIRE_POLICY_ROLE)
                .then(|| bootstrap.wire_policy().map(WireLedger::new))
                .transpose()
                .expect("validated projection bootstrap"),
            bootstrap,
            identity,
            ordinal: 0,
            generation: 0,
            observed: 0,
            origin: None,
            native: 0,
            accepted: 0,
            fence: 0,
            terminal: false,
            source: 0,
            source_active: false,
            physical: 0,
            physical_active: false,
            runner_connection,
            frame_seq: 0,
            pending: BTreeMap::new(),
            permit: None,
            active: None,
        }
    }
    fn progress(&self) -> ProjectionOwnerProgress {
        ProjectionOwnerProgress {
            identity: self.identity.clone(),
            journal_ordinal: self.ordinal,
            native_ordinal: self.native,
            terminal: self.terminal,
            uncertainties: self
                .wire
                .as_ref()
                .and_then(WireLedger::uncertain_input)
                .map(|input_id| OwnerPrefixUncertainty::WireUnsettled { input_id })
                .into_iter()
                .chain(self.pending.keys().map(|id| {
                    match self.active.as_ref().filter(|a| a.input == *id) {
                        Some(Active {
                            intent: Some((intent, ..)),
                            ..
                        }) => OwnerPrefixUncertainty::HandoffUnconfirmed {
                            input_id: *id,
                            intent_ordinal: *intent,
                        },
                        Some(a) => OwnerPrefixUncertainty::CommittedWithoutRelease {
                            input_id: *id,
                            operation_ordinal: a.ordinal,
                        },
                        None => OwnerPrefixUncertainty::ExecutionUnknown { input_id: *id },
                    }
                }))
                .collect(),
        }
    }
    fn physical_matches(&self, p: &Pending) -> bool {
        match p.input {
            ProjectionInput::Connected { connection, .. } => {
                connection == p.connection && connection > self.physical
            }
            _ if p.input.frame() => self.physical_active && p.connection == self.physical,
            _ => p.connection == self.physical,
        }
    }
    fn apply(
        &mut self,
        route: &CausalRoute,
        ns: u64,
        event: &ProjectionOwnerEvent,
        group: &ProjectionGroup,
    ) -> Result<(), CodecError> {
        if route.venue != self.identity.venue
            || route.session != self.identity.session
            || route.bootstrap_id != self.identity.bootstrap_id
            || Some(route.ordinal) != self.ordinal.checked_add(1)
            || ns < self.observed
            || (route.generation != self.generation
                && (!matches!(event, ProjectionOwnerEvent::ExecutionCompleted { .. })
                    || Some(route.generation) != self.generation.checked_add(1)))
            || (self.origin.is_none() && !matches!(event, ProjectionOwnerEvent::Started { .. }))
        {
            return Err(invalid("projection owner order/generation/origin"));
        }
        match event {
            ProjectionOwnerEvent::Wire(event) => {
                self.wire
                    .as_mut()
                    .ok_or_else(|| invalid("wire event without wire bootstrap"))?
                    .apply(
                        event,
                        route.ordinal,
                        ns,
                        self.source,
                        self.fence,
                        self.source_active && !self.terminal,
                    )?;
            }
            ProjectionOwnerEvent::Started { unix_ns } => {
                if self.origin.is_some() || ns != 0 {
                    return Err(invalid("projection duplicate/nonzero origin"));
                }
                self.origin = Some(*unix_ns);
            }
            ProjectionOwnerEvent::Ingress {
                input_id,
                connection,
                deadline_ns,
                owned_bytes_bound,
                input,
            } => {
                if self.wire.is_some() && matches!(input, ProjectionInput::Http { .. }) {
                    return Err(invalid("runtime HTTP outside wire projection"));
                }
                let policy = self
                    .bootstrap
                    .owner_policy
                    .as_ref()
                    .ok_or_else(|| invalid("projection missing owner policy"))?;
                let max_age = u64::try_from(policy.max_ingress_age.duration()?.as_nanos())
                    .map_err(|_| invalid("projection ingress age"))?;
                let source = match input {
                    ProjectionInput::Connected { connection: c, .. } => {
                        c == connection && *connection > self.source
                    }
                    _ if input.frame() => self.source_active && *connection == self.source,
                    _ => *connection == self.source,
                };
                // Completion retires the native reservation, while its compact
                // input remains until handoff/withholding. Mirror source policy
                // without charging that retained reference a second time.
                let reservations = self
                    .pending
                    .iter()
                    .filter(|(id, _)| self.active.as_ref().is_none_or(|a| a.input != **id))
                    .try_fold((0usize, 0u64), |(count, bytes), (_, p)| {
                        Some((count.checked_add(1)?, bytes.checked_add(p.owned_bytes)?))
                    })
                    .ok_or_else(|| invalid("projection reservation overflow"))?;
                if self.terminal
                    || Some(*input_id) != self.accepted.checked_add(1)
                    || !source
                    || *deadline_ns <= ns
                    || *deadline_ns - ns > max_age
                    || *owned_bytes_bound == 0
                    || *owned_bytes_bound > policy.max_input_bytes
                    || reservations.0 >= policy.max_pending_operations as usize
                    || reservations
                        .1
                        .checked_add(*owned_bytes_bound)
                        .is_none_or(|bytes| bytes > policy.max_pending_bytes)
                    || self.pending.len() >= group.policy.limits.max_pending_inputs
                {
                    return Err(invalid("projection ingress source/deadline/bound"));
                }
                if input.safety() {
                    self.fence = self.fence.max(*input_id);
                    self.source_active = matches!(input, ProjectionInput::Connected { .. });
                    if self.source_active {
                        self.source = *connection;
                    }
                }
                self.pending.insert(
                    *input_id,
                    Pending {
                        connection: *connection,
                        deadline: *deadline_ns,
                        owned_bytes: *owned_bytes_bound,
                        input: input.clone(),
                    },
                );
                self.accepted = *input_id;
            }
            ProjectionOwnerEvent::ExecutionPermitted {
                input_id,
                native_ordinal,
                generation,
            } => {
                let p = self
                    .pending
                    .get(input_id)
                    .ok_or_else(|| invalid("projection permit without ingress"))?;
                let generation_expected = if matches!(p.input, ProjectionInput::Connected { .. }) {
                    self.generation.checked_add(1)
                } else {
                    Some(self.generation)
                };
                if self.wire.as_ref().is_some_and(|wire| !wire.clean())
                    || self.terminal
                    || self.permit.is_some()
                    || self.active.is_some()
                    || self.pending.first_key_value().map(|(id, _)| *id) != Some(*input_id)
                    || Some(*native_ordinal) != self.native.checked_add(1)
                    || Some(*generation) != generation_expected
                    || !self.physical_matches(p)
                    || ns >= p.deadline
                {
                    return Err(invalid("projection permit lineage/source"));
                }
                self.permit = Some(Permit {
                    input: *input_id,
                    ordinal: route.ordinal,
                    native: *native_ordinal,
                    generation: *generation,
                    indeterminate: false,
                });
            }
            ProjectionOwnerEvent::ExecutionCompleted {
                input_id,
                permit_ordinal,
                operation_route,
                input,
                outcome,
                stopped,
                reconnect_requested,
                unsafe_state,
                connection,
                frame_seq,
            } => {
                let p = self
                    .pending
                    .get(input_id)
                    .ok_or_else(|| invalid("projection completion without ingress"))?;
                if self.permit.as_ref().is_none_or(|p| {
                    p.indeterminate
                        || p.input != *input_id
                        || p.ordinal != *permit_ordinal
                        || p.native != operation_route.ordinal
                        || p.generation != operation_route.generation
                }) || self.active.is_some()
                    || self.pending.first_key_value().map(|(id, _)| *id) != Some(*input_id)
                    || p.input != *input
                    || operation_route.venue != route.venue
                    || operation_route.session != route.session
                    || operation_route.bootstrap_id != route.bootstrap_id
                    || operation_route.generation != route.generation
                    || Some(operation_route.ordinal) != self.native.checked_add(1)
                    || !self.physical_matches(p)
                    || *connection
                        != match input {
                            ProjectionInput::Connected { connection, .. } => *connection,
                            _ => self.runner_connection,
                        }
                    || Some(*frame_seq) != self.frame_seq.checked_add(u64::from(input.frame()))
                {
                    return Err(invalid("projection completion permit/input/route"));
                }
                if *unsafe_state && *input_id >= self.fence {
                    self.fence = *input_id;
                    if *stopped || *reconnect_requested {
                        self.source_active = false;
                    }
                }
                if let ProjectionInput::Connected { connection, .. } = input {
                    self.physical = *connection;
                    self.physical_active = true;
                } else if input.safety() {
                    self.physical_active = false;
                }
                if *stopped || *reconnect_requested {
                    self.physical_active = false;
                }
                self.terminal |= outcome.is_err() || *stopped || ns >= p.deadline;
                self.native = operation_route.ordinal;
                self.runner_connection = *connection;
                self.frame_seq = *frame_seq;
                self.permit = None;
                self.active = Some(Active {
                    input: *input_id,
                    ordinal: route.ordinal,
                    route: operation_route.clone(),
                    release_blocked: *stopped || *reconnect_requested,
                    connection: *connection,
                    frame_seq: *frame_seq,
                    intent: None,
                });
            }
            ProjectionOwnerEvent::ExecutionNotInvoked {
                input_id,
                permit_ordinal,
                cause,
            } => {
                let p = self
                    .pending
                    .get(input_id)
                    .ok_or_else(|| invalid("projection noninvocation without input"))?;
                if self.permit.as_ref().is_none_or(|p| {
                    p.indeterminate || p.input != *input_id || p.ordinal != *permit_ordinal
                }) || match cause {
                    OwnerNotExecutedCause::Preflight => false,
                    OwnerNotExecutedCause::Deadline => ns < p.deadline,
                    OwnerNotExecutedCause::Terminal => !self.terminal,
                    OwnerNotExecutedCause::SourceConnection => true,
                } {
                    return Err(invalid("projection noninvocation lineage/cause"));
                }
                self.terminal |= matches!(
                    cause,
                    OwnerNotExecutedCause::Deadline | OwnerNotExecutedCause::Preflight
                );
                self.permit = None;
                self.pending.remove(input_id);
            }
            ProjectionOwnerEvent::ExecutionIndeterminate {
                input_id,
                permit_ordinal,
                ..
            } => {
                let p = self
                    .permit
                    .as_mut()
                    .ok_or_else(|| invalid("projection uncertainty without permit"))?;
                if p.indeterminate || p.input != *input_id || p.ordinal != *permit_ordinal {
                    return Err(invalid("projection uncertainty lineage"));
                }
                p.indeterminate = true;
                self.terminal = true;
            }
            ProjectionOwnerEvent::ReleaseIntent {
                input_id,
                operation_ordinal,
                outputs_eligible,
            } => {
                let a = self
                    .active
                    .as_ref()
                    .ok_or_else(|| invalid("projection intent without completion"))?;
                let p = self
                    .pending
                    .get(input_id)
                    .ok_or_else(|| invalid("projection intent without ingress"))?;
                if self.terminal
                    || a.input != *input_id
                    || a.ordinal != *operation_ordinal
                    || a.intent.is_some()
                    || *input_id < self.fence
                    || ns >= p.deadline
                    || (*outputs_eligible
                        && (a.release_blocked
                            || !self.source_active
                            || self.source != p.connection))
                {
                    return Err(invalid("projection intent eligibility/lineage"));
                }
                self.active.as_mut().expect("checked").intent =
                    Some((route.ordinal, *outputs_eligible));
            }
            ProjectionOwnerEvent::HandoffObserved {
                input_id,
                intent_ordinal,
            } => {
                let a = self
                    .active
                    .as_ref()
                    .ok_or_else(|| invalid("projection handoff without completion"))?;
                let p = self
                    .pending
                    .get(input_id)
                    .ok_or_else(|| invalid("projection handoff without ingress"))?;
                let r = group
                    .release
                    .as_ref()
                    .ok_or_else(|| invalid("projection missing release"))?;
                if self.terminal
                    || a.input != *input_id
                    || a.intent != Some((*intent_ordinal, r.outputs_eligible))
                    || r.operation_route != a.route
                    || ns >= p.deadline
                    || *input_id < self.fence
                {
                    return Err(invalid("projection handoff eligibility/lineage"));
                }
                self.validate_items(&group.items, p, a)?;
                if let Some(wire) = self.wire.as_mut() {
                    let mut candidates = Vec::new();
                    if r.outputs_eligible {
                        for e in &group.outbound_evidence {
                            let ProjectionOutbound::Write(frame) = &e.value else {
                                return Err(invalid("wire handoff HTTP"));
                            };
                            if !matches!(frame.opcode, OpcodeDto::Text | OpcodeDto::Binary) {
                                continue;
                            }
                            if candidates.len() >= wire.policy().max_pending_writes {
                                return Err(CodecError::TooLarge);
                            }
                            candidates.push(WireBindingDto {
                                input_id: *input_id,
                                completion_ordinal: a.ordinal,
                                handoff_ordinal: route.ordinal,
                                operation: a.route.clone(),
                                connection: a.connection,
                                frame_seq: a.frame_seq,
                                input_stamp: p.input.received(),
                                drain_position: e.source_index,
                                frame: frame.clone(),
                            });
                        }
                    }
                    if candidates.len() > group.policy.limits.max_pending_inputs {
                        return Err(CodecError::TooLarge);
                    }
                    wire.install(candidates, p.deadline)?;
                }
                self.active = None;
                self.pending.remove(input_id);
            }
            ProjectionOwnerEvent::Withheld { input_id } => {
                if self.active.as_ref().is_none_or(|a| a.input != *input_id) {
                    return Err(invalid("projection withholding without completion"));
                }
                let p = self
                    .pending
                    .remove(input_id)
                    .ok_or_else(|| invalid("projection withholding without input"))?;
                self.terminal |= ns >= p.deadline;
                self.active = None;
            }
            ProjectionOwnerEvent::NotExecuted { input_id, cause } => {
                let p = self
                    .pending
                    .get(input_id)
                    .ok_or_else(|| invalid("projection nonexecution without ingress"))?;
                if self.active.is_some()
                    || self.permit.as_ref().is_some_and(|p| p.input == *input_id)
                    || (!(self.terminal && *cause == OwnerNotExecutedCause::Terminal)
                        && self.pending.first_key_value().map(|(id, _)| *id) != Some(*input_id))
                    || match cause {
                        OwnerNotExecutedCause::SourceConnection => self.physical_matches(p),
                        OwnerNotExecutedCause::Deadline => ns < p.deadline,
                        OwnerNotExecutedCause::Terminal => !self.terminal,
                        OwnerNotExecutedCause::Preflight => self.terminal,
                    }
                {
                    return Err(invalid("projection nonexecution cause/order"));
                }
                self.terminal |= matches!(
                    cause,
                    OwnerNotExecutedCause::Deadline | OwnerNotExecutedCause::Preflight
                );
                self.pending.remove(input_id);
            }
            ProjectionOwnerEvent::Revoked {
                through_input_id, ..
            } => {
                if *through_input_id < self.fence
                    || *through_input_id > self.accepted.saturating_add(1)
                {
                    return Err(invalid("projection revocation boundary"));
                }
                self.fence = *through_input_id;
                self.terminal = true;
            }
        }
        self.ordinal = route.ordinal;
        self.generation = route.generation;
        self.observed = ns;
        if group.clock
            != ProjectionClock::owner(
                self.origin
                    .ok_or_else(|| invalid("projection missing origin"))?,
                ns,
            )
        {
            return Err(invalid("projection original clock"));
        }
        Ok(())
    }
    fn validate_items(
        &self,
        items: &[ItemDto],
        input: &Pending,
        active: &Active,
    ) -> Result<(), CodecError> {
        let selected: BTreeSet<_> = self
            .bootstrap
            .session
            .subscriptions
            .iter()
            .map(|s| s.instrument)
            .collect();
        let check_batch = |batch: &EventBatch| -> Result<(), CodecError> {
            if batch.session.0 != self.identity.session
                || batch.frame_seq != active.frame_seq
                || batch.events.iter().enumerate().any(|(index, e)| {
                    e.venue.0 != self.identity.venue
                        || e.session != batch.session
                        || e.frame_seq != batch.frame_seq
                        || e.connection.0 != active.connection
                        || input.input.now() != Some(e.receive_ts.0)
                        || usize::from(e.event_index) != index
                        || e.instrument.is_some_and(|id| !selected.contains(&id.0))
                })
            {
                return Err(invalid("projection canonical item scope"));
            }
            Ok(())
        };
        for item in items {
            match item {
                ItemDto::Market(leaf) => {
                    let FeedRecord::Batch(batch) = leaf.record()? else {
                        return Err(invalid("projection batch kind"));
                    };
                    check_batch(&batch)?;
                }
                ItemDto::ObservedMarket(leaf) => {
                    let batch = leaf.batch()?;
                    check_batch(batch.canonical())?;
                    let NativeObservationPolicyDto::HyperliquidV1 { limits } =
                        self.bootstrap.observation_policy()?
                    else {
                        return Err(invalid("projection native family policy"));
                    };
                    if batch.source().venue.0 != self.identity.venue
                        || batch.source().connection.0 != active.connection
                        || input.input.received() != Some(batch.source().received)
                        || batch.canonical().session.0 != self.identity.session
                        || batch.limits() != limits
                        || batch.native().rows.iter().any(|r| {
                            matches!(r.observation, NativeObservation::HyperliquidAllMids(_))
                        })
                    {
                        return Err(invalid("projection native scope/limits/family"));
                    }
                    for row in &batch.native().rows {
                        let coin = match &row.observation {
                            NativeObservation::HyperliquidTrade(v) => &v.coin,
                            NativeObservation::HyperliquidBbo(v) => &v.coin,
                            NativeObservation::HyperliquidBook(v) => &v.coin,
                            NativeObservation::HyperliquidCandle(v) => &v.coin,
                            NativeObservation::HyperliquidContext(v) => &v.coin,
                            NativeObservation::HyperliquidAllMids(_) => {
                                return Err(invalid("projection unadmitted all mids"));
                            }
                        };
                        let instrument = self
                            .bootstrap
                            .catalog
                            .instruments
                            .iter()
                            .find(|i| selected.contains(&i.id) && &i.key.native_symbol == coin)
                            .ok_or_else(|| {
                                invalid("projection native coin outside selected catalog")
                            })?;
                        if row.canonical_positions.iter().any(|position| {
                            batch
                                .canonical()
                                .events
                                .get(usize::from(*position))
                                .is_none_or(|event| {
                                    event.instrument != Some(InstrumentId(instrument.id))
                                })
                        }) {
                            return Err(invalid("projection native/canonical instrument differs"));
                        }
                    }
                }
                ItemDto::System(leaf) => {
                    let FeedRecord::System { session, event } = leaf.record()? else {
                        return Err(invalid("projection system kind"));
                    };
                    if session.0 != self.identity.session {
                        return Err(invalid("projection system scope"));
                    }
                    let valid = match event {
                        SystemEvent::QuoteInvalidated {
                            venue,
                            session,
                            connection,
                            instrument,
                            ..
                        } => {
                            venue == Some(VenueId(self.identity.venue))
                                && session == Some(SessionId(self.identity.session))
                                && connection == Some(ConnectionId(active.connection))
                                && selected.contains(&instrument.0)
                        }
                        SystemEvent::TradeRecordExcluded(record) => {
                            record.venue.0 == self.identity.venue
                                && record.session.0 == self.identity.session
                                && record.connection.0 == active.connection
                                && record.frame_seq == active.frame_seq
                                && record.instrument.is_none_or(|i| selected.contains(&i.0))
                        }
                        SystemEvent::BookInvalidated { instrument, .. }
                        | SystemEvent::BookSnapshotRejected { instrument, .. }
                        | SystemEvent::BookResynchronized { instrument } => {
                            selected.contains(&instrument.0)
                        }
                        _ => true,
                    };
                    if !valid {
                        return Err(invalid("projection scoped system item differs"));
                    }
                }
                _ => return Err(invalid("projection outbound item")),
            }
        }
        Ok(())
    }
}
impl ProjectionReader {
    pub fn open(path: impl AsRef<Path>, limits: ProjectionLimits) -> Result<Self, CodecError> {
        limits.validate()?;
        Ok(Self {
            reader: FeedReader::open_profile(path, None, FeedProfile::ObservationProjectionV1)?,
            limits,
            policy: None,
            source: None,
            owners: BTreeMap::new(),
            bootstrap_bytes: 0,
            catalog_entries: 0,
            sealed: false,
            poisoned: false,
        })
    }
    /// Rebuild target metadata/validation state, not consumer feature state.
    /// A consumer checkpoint must independently bind this exact cursor pair.
    pub fn resume(
        path: impl AsRef<Path>,
        target: FeedCursor,
        source: Option<FeedCursor>,
        limits: ProjectionLimits,
    ) -> Result<Self, CodecError> {
        let mut result = Self::open(path, limits)?;
        let bound = result.reader.observed_commit();
        if target.feed_id != bound.feed_id
            || target.sequence > bound.sequence
            || target.offset > bound.offset
            || target.offset < result.reader.cursor().offset
        {
            return Err(invalid("projection resume bound/feed"));
        }
        while result.reader.cursor().sequence < target.sequence {
            if result.advance(Some(&target))?.is_none() {
                return Err(invalid("projection resume exceeds prefix"));
            }
        }
        if result.reader.cursor() != &target || result.source != source {
            return Err(invalid("projection resume exact cursor pair"));
        }
        Ok(result)
    }
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }
    pub fn progress(&self) -> Result<ProjectionProgress, CodecError> {
        if self.poisoned {
            return Err(invalid("projection reader poisoned"));
        }
        Ok(ProjectionProgress {
            target_cursor: self.reader.cursor().clone(),
            source_cursor: self.source.clone(),
            policy_id: self.policy.as_ref().map(ProjectionPolicy::id).transpose()?,
            sealed: self.sealed,
            owners: self.owners.values().map(Owner::progress).collect(),
        })
    }
    pub fn metadata(&self, session: u64) -> Result<Option<&BootstrapDto>, CodecError> {
        if self.poisoned {
            return Err(invalid("projection reader poisoned"));
        }
        Ok(self.owners.get(&session).map(|o| o.bootstrap.as_ref()))
    }
    pub fn read_next(&mut self) -> Result<Option<ProjectionGroup>, CodecError> {
        self.read_bounded(None)
    }
    /// Durable bound last observed by the underlying reader (initially open).
    /// Capture it before a one-shot comparison so a growing live feed cannot
    /// silently extend that proof's prefix.
    pub fn committed_bound(&self) -> Result<&FeedCursor, CodecError> {
        if self.poisoned {
            return Err(invalid("projection reader poisoned"));
        }
        Ok(self.reader.observed_commit())
    }
    /// Read a whole group within a previously observed durable boundary.
    pub fn read_next_through(
        &mut self,
        ceiling: &FeedCursor,
    ) -> Result<Option<ProjectionGroup>, CodecError> {
        self.read_bounded(Some(ceiling))
    }
    fn read_bounded(
        &mut self,
        ceiling: Option<&FeedCursor>,
    ) -> Result<Option<ProjectionGroup>, CodecError> {
        if self.poisoned {
            return Err(invalid("projection reader poisoned"));
        }
        let result = (|| {
            if let Some(c) = ceiling {
                let cursor = self.reader.cursor();
                let bound = self.reader.observed_commit();
                if c.feed_id != cursor.feed_id
                    || c.sequence < cursor.sequence
                    || c.offset < cursor.offset
                    || c.sequence > bound.sequence
                    || c.offset > bound.offset
                {
                    return Err(invalid("projection snapshot bound"));
                }
                if cursor == c {
                    return Ok(None);
                }
            }
            self.advance(ceiling)
        })();
        if result.is_err() {
            self.poisoned = true;
            self.owners.clear();
            self.bootstrap_bytes = 0;
            self.catalog_entries = 0;
        }
        result
    }
    fn advance(
        &mut self,
        ceiling: Option<&FeedCursor>,
    ) -> Result<Option<ProjectionGroup>, CodecError> {
        let Some(record) = self.reader.read_next()? else {
            return Ok(None);
        };
        if ceiling.is_some_and(|c| {
            self.reader.cursor().sequence > c.sequence
                || self.reader.cursor().offset > c.offset
                || (self.reader.cursor().sequence == c.sequence && self.reader.cursor() != c)
        }) {
            return Err(invalid("projection record crosses resume cursor"));
        }
        let FeedRecord::Projection(envelope) = record else {
            return Err(invalid("non-projection record in target"));
        };
        let group = ProjectionGroup::decode(&envelope, self.limits.max_group_bytes)?;
        if !group.policy.limits.within(&self.limits)
            || self.policy.as_ref().is_some_and(|p| p != &group.policy)
            || self
                .source
                .as_ref()
                .is_some_and(|c| c != &group.source_before)
        {
            return Err(invalid("projection policy/source chain changed"));
        }
        match &group.source {
            ProjectionSource::Bootstrap(dto) => {
                let id = group
                    .owner
                    .clone()
                    .ok_or_else(|| invalid("projection missing bootstrap owner"))?;
                let bytes = serde_json::to_vec(dto)?.len();
                let next_bytes = self
                    .bootstrap_bytes
                    .checked_add(bytes)
                    .ok_or(CodecError::TooLarge)?;
                let next_entries = self
                    .catalog_entries
                    .checked_add(dto.catalog.instruments.len())
                    .ok_or(CodecError::TooLarge)?;
                if self.owners.contains_key(&id.session)
                    || self.owners.len() >= group.policy.limits.max_owners
                    || next_bytes > group.policy.limits.max_bootstrap_bytes
                    || next_entries > group.policy.limits.max_catalog_entries
                {
                    return Err(invalid(
                        "projection aggregate metadata budget/duplicate owner",
                    ));
                }
                self.owners.insert(id.session, Owner::new(dto.clone(), id));
                self.bootstrap_bytes = next_bytes;
                self.catalog_entries = next_entries;
            }
            ProjectionSource::Owner {
                route,
                observed_ns,
                event,
                after,
            } => {
                let owner = self
                    .owners
                    .get_mut(&route.session)
                    .ok_or_else(|| invalid("projection owner before bootstrap"))?;
                owner.apply(route, *observed_ns, event, &group)?;
                if owner.progress() != **after {
                    return Err(invalid("projection owner prefix state differs"));
                }
                if self
                    .owners
                    .values()
                    .try_fold(0usize, |n, o| {
                        o.pending
                            .len()
                            .checked_add(o.wire.as_ref().map_or(0, WireLedger::count))
                            .and_then(|v| n.checked_add(v))
                    })
                    .is_none_or(|n| n > group.policy.limits.max_pending_inputs)
                {
                    return Err(invalid("projection aggregate pending budget"));
                }
            }
            ProjectionSource::Seal { owners, .. } => {
                if self
                    .owners
                    .values()
                    .map(Owner::progress)
                    .collect::<Vec<_>>()
                    != *owners
                {
                    return Err(invalid("projection seal loses owner uncertainty"));
                }
                if self
                    .owners
                    .values()
                    .any(|o| o.wire.as_ref().is_some_and(|w| !w.clean()))
                {
                    return Err(invalid("projection seal with unsettled wire effects"));
                }
                self.sealed = true;
            }
        }
        self.policy = Some(group.policy.clone());
        self.source = Some(group.source_after.clone());
        Ok(Some(group))
    }
}
