//! Versioned owner observations for MFCJ2. Journal and native ordinals are
//! independent. This is a physical-order validator, not actual-engine replay.
use super::*;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerHistoryDto {
    pub schema_version: u16,
    pub route: CausalRoute,
    /// Owner-local monotonic nanoseconds from Started. Not exchange time.
    pub observed_ns: u64,
    pub event: OwnerEventDto,
}

/// Externally tagged: no buffered untyped JSON lies between financial leaves
/// and their exact typed operation representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnerEventDto {
    Wire(WireEventDto),
    Started {
        unix_ns: i64,
    },
    Ingress {
        input_id: u64,
        connection: u64,
        deadline_ns: u64,
        /// Source-reported owned ingress capacity, distinct from encoded size.
        owned_bytes_bound: u64,
        input: InputDto,
    },
    Executed {
        input_id: u64,
        operation: Box<OperationDto>,
    },
    ExecutionPermitted {
        input_id: u64,
        native_ordinal: u64,
        generation: u64,
    },
    ExecutionCompleted {
        input_id: u64,
        permit_ordinal: u64,
        operation: Box<OperationDto>,
    },
    ExecutionNotInvoked {
        input_id: u64,
        permit_ordinal: u64,
        cause: OwnerNotExecutedCause,
    },
    ExecutionIndeterminate {
        input_id: u64,
        permit_ordinal: u64,
        cause: OwnerIndeterminateCause,
    },
    ReleaseIntent {
        input_id: u64,
        operation_ordinal: u64,
        outputs_eligible: bool,
    },
    HandoffObserved {
        input_id: u64,
        intent_ordinal: u64,
    },
    Withheld {
        input_id: u64,
    },
    NotExecuted {
        input_id: u64,
        cause: OwnerNotExecutedCause,
    },
    /// Observed revocation; no claim that previously returned data is unsent.
    Fence {
        through_input_id: u64,
    },
    Revoked {
        through_input_id: u64,
        cause: OwnerTerminalCause,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnerIndeterminateCause {
    Incomplete,
    Encoding,
    Panic,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnerTerminalCause {
    Revoked,
    IngressFull,
    InputTooLarge,
    UnsupportedInput,
    OrdinalExhausted,
    ObservationFull,
    WriterUnavailable,
    Deadline,
    Encoding,
    Incomplete,
    NativeError,
    NativeStopped,
    WorkerPanicked,
    InvalidReceipt,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnerNotExecutedCause {
    SourceConnection,
    Deadline,
    Preflight,
    Terminal,
}
impl OwnerEventDto {
    pub fn ingress(
        input_id: u64,
        connection: ConnectionId,
        deadline_ns: u64,
        owned_bytes_bound: u64,
        input: &CaptureInput,
        identity: &OperationIdentity,
    ) -> Result<Self, CodecError> {
        let input = InputDto::input(input, identity.session)?;
        if !supported(&input) {
            return Err(invalid("unsupported owner-history ingress"));
        }
        Ok(Self::Ingress {
            input_id,
            connection: connection.0,
            deadline_ns,
            owned_bytes_bound,
            input,
        })
    }
    pub fn executed(
        input_id: u64,
        operation: &CapturedOperation,
        identity: &OperationIdentity,
    ) -> Result<Self, CodecError> {
        Ok(Self::Executed {
            input_id,
            operation: Box::new(OperationDto::from_capture(operation, identity)?),
        })
    }
}
fn supported(input: &InputDto) -> bool {
    matches!(
        input,
        InputDto::Connected { .. }
            | InputDto::Disconnected { .. }
            | InputDto::TransportLost { .. }
            | InputDto::Text { .. }
            | InputDto::Binary { .. }
            | InputDto::Ping { .. }
            | InputDto::Pong { .. }
            | InputDto::Close { .. }
            | InputDto::Http { .. }
            | InputDto::TimerPoll { .. }
            | InputDto::Stop
            | InputDto::Control {
                command: CommandDto::Resync(_) | CommandDto::Stop,
                ..
            }
    )
}
fn safety(input: &InputDto) -> bool {
    matches!(
        input,
        InputDto::Connected { .. }
            | InputDto::Disconnected { .. }
            | InputDto::TransportLost { .. }
            | InputDto::Close { .. }
            | InputDto::Stop
            | InputDto::Control {
                command: CommandDto::Stop,
                ..
            }
    )
}
fn frame(input: &InputDto) -> bool {
    matches!(
        input,
        InputDto::Text { .. }
            | InputDto::Binary { .. }
            | InputDto::Ping { .. }
            | InputDto::Pong { .. }
            | InputDto::Close { .. }
            | InputDto::Http { .. }
    )
}
fn minimum_owned(input: &InputDto) -> usize {
    fn bytes(value: &BytesDto) -> usize {
        value.0.len() / 4 * 3 - value.0.bytes().rev().take_while(|b| *b == b'=').count()
    }
    match input {
        InputDto::Text { payload, .. }
        | InputDto::Binary { payload, .. }
        | InputDto::Ping { payload, .. }
        | InputDto::Pong { payload, .. }
        | InputDto::Close { payload, .. } => bytes(payload).max(1),
        InputDto::Http { response, .. } => (bytes(&response.body)
            + response
                .headers
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>())
        .max(1),
        _ => 1,
    }
}
fn same_owner(route: &CausalRoute, identity: &OperationIdentity) -> bool {
    route.venue == identity.venue
        && route.session == identity.session
        && route.bootstrap_id == identity.bootstrap_id
}
impl OwnerHistoryDto {
    fn validate(&self, identity: &OperationIdentity) -> Result<(), CodecError> {
        if !matches!(self.schema_version, 1..=4)
            || self.route.ordinal == 0
            || !same_owner(&self.route, identity)
        {
            return Err(invalid("owner-history schema or identity mismatch"));
        }
        if matches!(self.event, OwnerEventDto::Wire(_)) && self.schema_version != 4 {
            return Err(invalid("wire event requires MFCJ5"));
        }
        if let OwnerEventDto::Wire(event) = &self.event {
            event.validate(&self.route)?;
        }
        let modern = matches!(
            self.event,
            OwnerEventDto::ExecutionPermitted { .. }
                | OwnerEventDto::ExecutionCompleted { .. }
                | OwnerEventDto::ExecutionNotInvoked { .. }
                | OwnerEventDto::ExecutionIndeterminate { .. }
                | OwnerEventDto::Revoked { .. }
        );
        if (self.schema_version == 1 && modern)
            || (self.schema_version >= 2
                && matches!(
                    self.event,
                    OwnerEventDto::Executed { .. } | OwnerEventDto::Fence { .. }
                ))
        {
            return Err(invalid("owner event requires negotiated permit semantics"));
        }
        if let OwnerEventDto::ExecutionCompleted { permit_ordinal, .. } = &self.event {
            if *permit_ordinal == 0 || *permit_ordinal >= self.route.ordinal {
                return Err(invalid("invalid execution completion reference"));
            }
        }
        match &self.event {
            OwnerEventDto::Ingress {
                input_id,
                deadline_ns,
                owned_bytes_bound,
                input,
                ..
            } => {
                if *input_id == 0
                    || *deadline_ns <= self.observed_ns
                    || *owned_bytes_bound == 0
                    || !supported(input)
                {
                    return Err(invalid("owner-history ingress bounds or kind"));
                }
            }
            OwnerEventDto::Executed {
                input_id,
                operation,
            }
            | OwnerEventDto::ExecutionCompleted {
                input_id,
                operation,
                ..
            } => {
                if *input_id == 0
                    || !same_owner(&operation.route, identity)
                    || operation.route.generation != self.route.generation
                {
                    return Err(invalid("owner-history native identity mismatch"));
                }
                if operation.schema_version != if self.schema_version >= 3 { 2 } else { 1 } {
                    return Err(invalid("operation requires matching owner-history profile"));
                }
                operation.validate()?;
            }
            OwnerEventDto::ExecutionPermitted {
                input_id,
                native_ordinal,
                ..
            } => {
                if *input_id == 0 || *native_ordinal == 0 {
                    return Err(invalid("invalid execution permit"));
                }
            }
            OwnerEventDto::ExecutionNotInvoked {
                input_id,
                permit_ordinal,
                ..
            }
            | OwnerEventDto::ExecutionIndeterminate {
                input_id,
                permit_ordinal,
                ..
            } => {
                if *input_id == 0 || *permit_ordinal == 0 || *permit_ordinal >= self.route.ordinal {
                    return Err(invalid("invalid execution settlement reference"));
                }
            }
            OwnerEventDto::ReleaseIntent {
                input_id,
                operation_ordinal,
                ..
            } => {
                if *input_id == 0
                    || *operation_ordinal == 0
                    || *operation_ordinal >= self.route.ordinal
                {
                    return Err(invalid("owner-history intent reference"));
                }
            }
            OwnerEventDto::HandoffObserved {
                input_id,
                intent_ordinal,
            } => {
                if *input_id == 0 || *intent_ordinal == 0 || *intent_ordinal >= self.route.ordinal {
                    return Err(invalid("owner-history handoff reference"));
                }
            }
            OwnerEventDto::Withheld { input_id } | OwnerEventDto::NotExecuted { input_id, .. }
                if *input_id == 0 =>
            {
                return Err(invalid("owner-history zero input ID"));
            }
            _ => {}
        }
        Ok(())
    }
}
pub fn encode_owner_history(
    dto: &OwnerHistoryDto,
    identity: &OperationIdentity,
    max_bytes: usize,
) -> Result<CausalEnvelope, CodecError> {
    dto.validate(identity)?;
    if max_bytes == 0 || max_bytes > MAX_OPERATION_BYTES {
        return Err(CodecError::TooLarge);
    }
    let mut buffer = CappedBuffer {
        bytes: Vec::new(),
        max: max_bytes,
        overflow: false,
    };
    if let Err(error) = serde_json::to_writer(&mut buffer, dto) {
        return Err(if buffer.overflow {
            CodecError::TooLarge
        } else {
            CodecError::Json(error)
        });
    }
    Ok(CausalEnvelope::for_profile(
        match dto.schema_version {
            1 => FeedProfile::EngineCausalV2,
            2 => FeedProfile::EngineCausalV3,
            3 => FeedProfile::EngineCausalV4,
            4 => FeedProfile::EngineCausalV5,
            _ => unreachable!("validated owner-history version"),
        },
        if matches!(
            dto.event,
            OwnerEventDto::Executed { .. } | OwnerEventDto::ExecutionCompleted { .. }
        ) {
            CausalPhase::Operation
        } else {
            CausalPhase::Owner
        },
        dto.route.clone(),
        &buffer.bytes,
    )?)
}
pub fn decode_owner_history(
    envelope: &CausalEnvelope,
    identity: &OperationIdentity,
) -> Result<OwnerHistoryDto, CodecError> {
    decode_limited(envelope, identity, MAX_OPERATION_BYTES)
}
fn decode_limited(
    envelope: &CausalEnvelope,
    identity: &OperationIdentity,
    max_bytes: usize,
) -> Result<OwnerHistoryDto, CodecError> {
    if !matches!(envelope.schema_version, 2..=5) || envelope.phase == CausalPhase::Bootstrap {
        return Err(invalid("expected negotiated owner-history record"));
    }
    let payload = envelope.payload()?;
    if payload.len() > max_bytes {
        return Err(CodecError::TooLarge);
    }
    let dto: OwnerHistoryDto = serde_json::from_slice(&payload)?;
    dto.validate(identity)?;
    if dto.schema_version + 1 != envelope.schema_version
        || dto.route != envelope.route
        || (envelope.phase == CausalPhase::Operation)
            != matches!(
                dto.event,
                OwnerEventDto::Executed { .. } | OwnerEventDto::ExecutionCompleted { .. }
            )
    {
        return Err(invalid("owner-history route or phase mismatch"));
    }
    let mut check = Canonical {
        expected: &payload,
        offset: 0,
    };
    serde_json::to_writer(&mut check, &dto)?;
    if check.offset != payload.len() {
        return Err(invalid("noncanonical owner history"));
    }
    Ok(dto)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum OwnerPrefixUncertainty {
    WireUnsettled {
        input_id: u64,
    },
    /// An ingress record alone cannot prove whether a callback ran before crash.
    ExecutionUnknown {
        input_id: u64,
    },
    CommittedWithoutRelease {
        input_id: u64,
        operation_ordinal: u64,
    },
    HandoffUnconfirmed {
        input_id: u64,
        intent_ordinal: u64,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedOwnerHandoff {
    pub input_id: u64,
    pub intent_ordinal: u64,
    pub observed_ns: u64,
    pub outputs_eligible: bool,
    pub operation: Box<OperationDto>,
}
impl RecordedOwnerHandoff {
    /// Only completed actual drains from this observed handoff. An intent or
    /// native commit alone never produces these values. No external I/O claim.
    pub fn released_items(&self) -> impl Iterator<Item = &ItemDto> {
        self.operation.entries.iter().flat_map(|e| match &e.effect {
            EffectDto::QueueDrain {
                items,
                completed: true,
                ..
            } if self.outputs_eligible => items.as_slice(),
            _ => &[],
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingInput {
    connection: u64,
    deadline_ns: u64,
    input: InputDto,
    bytes: usize,
    owned_bytes: usize,
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveOperation {
    input_id: u64,
    journal_ordinal: u64,
    operation: Box<OperationDto>,
    intent: Option<(u64, bool)>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct ExecutionPermit {
    input_id: u64,
    journal_ordinal: u64,
    native_ordinal: u64,
    generation: u64,
    indeterminate: bool,
}
/// Consumes already committed records in physical order. Validating this ledger
/// does not execute a factory, establish native truth or prove external delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerHistory {
    envelope_version: u16,
    record_bytes: usize,
    identity: OperationIdentity,
    policy: JournalOwnerPolicyDto,
    limits: OwnerHistoryLimits,
    capture: CaptureConfigDto,
    runner_policy: CaptureRuntimePolicy,
    observation_policy: Option<NativeObservationPolicyDto>,
    ordinal: u64,
    native_ordinal: u64,
    generation: u64,
    observed_ns: u64,
    started: bool,
    terminal: bool,
    accepted: u64,
    fence: u64,
    source: u64,
    source_active: bool,
    physical_source: u64,
    physical_active: bool,
    pending_bytes: usize,
    pending_owned_bytes: usize,
    native_pending_operations: usize,
    pending: BTreeMap<u64, PendingInput>,
    active: Option<ActiveOperation>,
    permit: Option<ExecutionPermit>,
    previous_after: Option<StateDto>,
    wire: Option<WireLedger>,
}
/// Explicit reader retention caps. Native owner budgets measure source-owned
/// capacities; canonical/base64 JSON size is a different quantity. Neither is RSS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerHistoryLimits {
    pub max_retained_input_bytes: usize,
    pub max_input_bytes: usize,
    pub max_record_bytes: usize,
}
/// Logical quantities retained by this structural ledger. Canonical bytes are
/// serialized lengths, not allocator usage. The native machine and temporary
/// decoding/comparison work are outside these counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OwnerHistoryRetention {
    /// Retained native inputs plus MFCJ5 wire-candidate references.
    pub pending_inputs: usize,
    /// Canonical native ingress bytes plus bounded wire payload/identity reservations.
    pub pending_input_bytes: usize,
    pub active_operation_and_state_bytes: usize,
    pub source_owned_pending_inputs: usize,
    pub source_owned_pending_bytes: usize,
}
impl OwnerHistoryLimits {
    pub fn validate(&self) -> Result<(), CodecError> {
        if self.max_retained_input_bytes == 0
            || self.max_retained_input_bytes > 128 * 1024 * 1024
            || self.max_input_bytes == 0
            || self.max_input_bytes > self.max_retained_input_bytes
            || self.max_input_bytes > MAX_OPERATION_BYTES
            || self.max_record_bytes == 0
            || self.max_record_bytes > MAX_OPERATION_BYTES
        {
            return Err(invalid("owner-history reader retention bounds"));
        }
        Ok(())
    }
}
impl OwnerHistory {
    pub fn from_bootstrap(
        envelope: &CausalEnvelope,
        limits: OwnerHistoryLimits,
    ) -> Result<Self, CodecError> {
        limits.validate()?;
        if !matches!(envelope.schema_version, 2..=5) {
            return Err(invalid("owner history requires negotiated bootstrap"));
        }
        let dto = decode_bootstrap(envelope)?;
        let observation_policy = if envelope.schema_version >= 4 {
            Some(dto.observation_policy()?)
        } else {
            None
        };
        if dto.schema_version != 2 {
            return Err(invalid("owner history requires bound owner policy"));
        }
        let record_bytes = if envelope.schema_version >= 3 {
            dto.history_policy()?.max_record_bytes as usize
        } else {
            dto.owner_policy
                .as_ref()
                .expect("bound V2")
                .max_operation_bytes as usize
        };
        let wire = if envelope.schema_version == 5 {
            Some(WireLedger::new(dto.wire_policy()?))
        } else {
            None
        };
        let policy = dto
            .owner_policy
            .ok_or_else(|| invalid("missing owner policy"))?;
        Ok(Self {
            envelope_version: envelope.schema_version,
            record_bytes,
            identity: OperationIdentity {
                venue: envelope.route.venue,
                session: envelope.route.session,
                bootstrap_id: envelope.route.bootstrap_id.clone(),
            },
            policy,
            limits,
            capture: dto.capture,
            runner_policy: dto.runner_policy,
            observation_policy,
            ordinal: 0,
            native_ordinal: 0,
            generation: 0,
            observed_ns: 0,
            started: false,
            terminal: false,
            accepted: 0,
            fence: 0,
            source: 0,
            source_active: false,
            physical_source: 0,
            physical_active: false,
            pending_bytes: 0,
            pending_owned_bytes: 0,
            native_pending_operations: 0,
            pending: BTreeMap::new(),
            active: None,
            permit: None,
            previous_after: None,
            wire,
        })
    }
    pub fn identity(&self) -> &OperationIdentity {
        &self.identity
    }
    pub fn ordinal(&self) -> u64 {
        self.ordinal
    }
    pub fn native_ordinal(&self) -> u64 {
        self.native_ordinal
    }
    pub fn pending_inputs(&self) -> usize {
        self.pending.len() + self.wire.as_ref().map_or(0, WireLedger::count)
    }
    /// Count the active operation and separately retained previous state as
    /// well as pending inputs. Counting serializes into a bounded counter,
    /// without allocating another operation-sized buffer or cloning histories.
    pub fn retention(&self) -> Result<OwnerHistoryRetention, CodecError> {
        struct Count(usize);
        impl Write for Count {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0 = self
                    .0
                    .checked_add(bytes.len())
                    .filter(|n| *n <= MAX_OPERATION_BYTES)
                    .ok_or_else(|| io::Error::other("owner canonical retention overflow"))?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        fn size(value: &impl Serialize) -> Result<usize, CodecError> {
            let mut count = Count(0);
            serde_json::to_writer(&mut count, value)?;
            Ok(count.0)
        }
        let active = self
            .active
            .as_ref()
            .map(|a| size(&a.operation))
            .transpose()?
            .unwrap_or(0);
        let previous = self
            .previous_after
            .as_ref()
            .map(size)
            .transpose()?
            .unwrap_or(0);
        Ok(OwnerHistoryRetention {
            pending_inputs: self.pending.len() + self.wire.as_ref().map_or(0, WireLedger::count),
            pending_input_bytes: self
                .pending_bytes
                .checked_add(self.wire.as_ref().map_or(0, WireLedger::bytes))
                .ok_or(CodecError::TooLarge)?,
            active_operation_and_state_bytes: active
                .checked_add(previous)
                .ok_or_else(|| invalid("owner canonical retention sum overflow"))?,
            source_owned_pending_inputs: self.native_pending_operations,
            source_owned_pending_bytes: self.pending_owned_bytes,
        })
    }
    pub fn wire_clean(&self) -> bool {
        self.wire.as_ref().is_none_or(WireLedger::clean)
    }
    pub fn is_terminal(&self) -> bool {
        self.terminal
    }
    pub fn uncertainties(&self) -> Vec<OwnerPrefixUncertainty> {
        self.wire
            .as_ref()
            .and_then(WireLedger::uncertain_input)
            .map(|input_id| OwnerPrefixUncertainty::WireUnsettled { input_id })
            .into_iter()
            .chain(self.pending.keys().map(|id| {
                if let Some(a) = self.active.as_ref().filter(|a| a.input_id == *id) {
                    if let Some((intent_ordinal, _)) = a.intent {
                        OwnerPrefixUncertainty::HandoffUnconfirmed {
                            input_id: *id,
                            intent_ordinal,
                        }
                    } else {
                        OwnerPrefixUncertainty::CommittedWithoutRelease {
                            input_id: *id,
                            operation_ordinal: a.journal_ordinal,
                        }
                    }
                } else {
                    OwnerPrefixUncertainty::ExecutionUnknown { input_id: *id }
                }
            }))
            .collect()
    }
    pub fn apply(
        &mut self,
        envelope: &CausalEnvelope,
    ) -> Result<Option<RecordedOwnerHandoff>, CodecError> {
        if envelope.schema_version != self.envelope_version {
            return Err(invalid("owner profile changed"));
        }
        let dto = decode_limited(
            envelope,
            &self.identity,
            self.limits
                .max_record_bytes
                .min(self.record_bytes)
                .min(self.policy.max_operation_bytes as usize),
        )?;
        if Some(dto.route.ordinal) != self.ordinal.checked_add(1)
            || dto.observed_ns < self.observed_ns
        {
            return Err(invalid("owner journal order or clock regression"));
        }
        let executing = matches!(
            dto.event,
            OwnerEventDto::Executed { .. } | OwnerEventDto::ExecutionCompleted { .. }
        );
        if dto.route.generation != self.generation
            && (!executing || Some(dto.route.generation) != self.generation.checked_add(1))
        {
            return Err(invalid("owner journal generation discontinuity"));
        }
        if !self.started && !matches!(dto.event, OwnerEventDto::Started { .. }) {
            return Err(invalid("owner record before Started"));
        }
        let mut handoff = None;
        match dto.event {
            OwnerEventDto::Wire(ref event) => {
                self.wire
                    .as_mut()
                    .ok_or_else(|| invalid("wire event without wire profile"))?
                    .apply(
                        event,
                        dto.route.ordinal,
                        dto.observed_ns,
                        self.source,
                        self.fence,
                        self.source_active && !self.terminal,
                    )?;
            }
            OwnerEventDto::Started { .. } => {
                if self.started || dto.observed_ns != 0 {
                    return Err(invalid("duplicate or nonzero owner origin"));
                }
                self.started = true;
            }
            OwnerEventDto::Ingress {
                input_id,
                connection,
                deadline_ns,
                owned_bytes_bound,
                input,
            } => {
                if self.wire.is_some() && matches!(input, InputDto::Http { .. }) {
                    return Err(invalid("runtime HTTP outside wire profile"));
                }
                if self.terminal
                    || Some(input_id) != self.accepted.checked_add(1)
                    || self.native_pending_operations >= self.policy.max_pending_operations as usize
                    || deadline_ns - dto.observed_ns > duration_ns(self.policy.max_ingress_age)?
                    || owned_bytes_bound > self.policy.max_input_bytes
                    || owned_bytes_bound < minimum_owned(&input) as u64
                    || owned_bytes_bound
                        > (self.policy.max_pending_bytes as usize - self.pending_owned_bytes) as u64
                {
                    return Err(invalid("owner ingress sequence, count or deadline"));
                }
                let correct = match &input {
                    InputDto::Connected {
                        connection: incoming,
                        ..
                    } => *incoming == connection && connection > self.source,
                    _ if frame(&input) => self.source_active && connection == self.source,
                    _ => connection == self.source,
                };
                if !correct {
                    return Err(invalid("owner ingress physical source mismatch"));
                }
                let mut count = CappedBuffer {
                    bytes: Vec::new(),
                    max: self.limits.max_retained_input_bytes - self.pending_bytes,
                    overflow: false,
                };
                serde_json::to_writer(&mut count, &input)
                    .map_err(|_| invalid("owner retained input byte bound"))?;
                let bytes = count.bytes.len();
                if bytes > self.limits.max_input_bytes {
                    return Err(invalid("owner input byte bound"));
                }
                if safety(&input) {
                    self.fence = self.fence.max(input_id);
                    self.source_active = matches!(input, InputDto::Connected { .. });
                    if self.source_active {
                        self.source = connection;
                    }
                }
                self.pending.insert(
                    input_id,
                    PendingInput {
                        connection,
                        deadline_ns,
                        input,
                        bytes,
                        owned_bytes: owned_bytes_bound as usize,
                    },
                );
                self.pending_bytes += bytes;
                self.pending_owned_bytes += owned_bytes_bound as usize;
                self.native_pending_operations += 1;
                self.accepted = input_id;
            }
            OwnerEventDto::ExecutionPermitted {
                input_id,
                native_ordinal,
                generation,
            } => {
                let p = self
                    .pending
                    .get(&input_id)
                    .ok_or_else(|| invalid("permit without ingress"))?;
                let expected_generation = if matches!(p.input, InputDto::Connected { .. }) {
                    self.generation.checked_add(1)
                } else {
                    Some(self.generation)
                };
                let source_matches = match &p.input {
                    InputDto::Connected { connection, .. } => {
                        *connection == p.connection && *connection > self.physical_source
                    }
                    _ if frame(&p.input) => {
                        self.physical_active && p.connection == self.physical_source
                    }
                    _ => p.connection == self.physical_source,
                };
                if self.wire.as_ref().is_some_and(|wire| !wire.clean())
                    || self.terminal
                    || self.permit.is_some()
                    || self.active.is_some()
                    || self.pending.first_key_value().map(|(id, _)| *id) != Some(input_id)
                    || Some(native_ordinal) != self.native_ordinal.checked_add(1)
                    || Some(generation) != expected_generation
                    || !source_matches
                    || dto.observed_ns >= p.deadline_ns
                {
                    return Err(invalid("execution permit order, source, deadline or token"));
                }
                self.permit = Some(ExecutionPermit {
                    input_id,
                    journal_ordinal: dto.route.ordinal,
                    native_ordinal,
                    generation,
                    indeterminate: false,
                });
            }
            event @ (OwnerEventDto::Executed { .. } | OwnerEventDto::ExecutionCompleted { .. }) => {
                let (input_id, operation, permit_ordinal) = match event {
                    OwnerEventDto::Executed {
                        input_id,
                        operation,
                    } => (input_id, operation, None),
                    OwnerEventDto::ExecutionCompleted {
                        input_id,
                        permit_ordinal,
                        operation,
                    } => (input_id, operation, Some(permit_ordinal)),
                    _ => unreachable!(),
                };
                if self.envelope_version >= 3
                    && self.permit.as_ref().is_none_or(|p| {
                        p.indeterminate
                            || p.input_id != input_id
                            || Some(p.journal_ordinal) != permit_ordinal
                            || p.native_ordinal != operation.route.ordinal
                            || p.generation != operation.route.generation
                    })
                {
                    return Err(invalid("completion without matching execution permit"));
                }
                let pending = self
                    .pending
                    .get(&input_id)
                    .ok_or_else(|| invalid("operation without ingress"))?;
                operation.validate_observation_policy(self.observation_policy)?;
                if (self.envelope_version == 2 && self.terminal)
                    || self.active.is_some()
                    || operation.retained_bytes_bound > self.capture.max_bytes
                    || operation.observed_entries > self.capture.max_entries
                    || [&operation.before, &operation.after].iter().any(|s| {
                        s.pending_market > self.capture.dispatch_capacity
                            || s.pending_system > self.capture.dispatch_capacity
                            || s.pending_writes > self.runner_policy.max_pending_writes
                            || s.pending_http > self.runner_policy.max_pending_http
                            || s.timers.len() as u64 > self.runner_policy.max_timers
                    })
                    || self.pending.first_key_value().map(|(id, _)| *id) != Some(input_id)
                    || operation.input != pending.input
                    || Some(operation.route.ordinal) != self.native_ordinal.checked_add(1)
                    || self
                        .previous_after
                        .as_ref()
                        .is_some_and(|state| state != &operation.before)
                    || operation.drain_policy != DrainDto::NullDispatchTakeOutbound
                {
                    return Err(invalid(
                        "native operation order, input, state or drain mismatch",
                    ));
                }
                let source_matches = match &operation.input {
                    InputDto::Connected { connection, .. } => {
                        *connection == pending.connection && *connection > self.physical_source
                    }
                    _ if frame(&operation.input) => {
                        self.physical_active && pending.connection == self.physical_source
                    }
                    _ => pending.connection == self.physical_source,
                };
                if !source_matches {
                    return Err(invalid("native execution on stale physical source"));
                }
                if self.native_ordinal == 0 {
                    let s = &operation.before;
                    if s.lifecycle != LifecycleDto::Planned
                        || s.connection != self.runner_policy.initial_connection
                        || s.frame_seq != 0
                        || s.stop_requested
                        || s.reconnect_requested
                        || !s.timers.is_empty()
                        || s.valid_books != 0
                        || s.pending_market != 0
                        || s.pending_system != 0
                        || s.pending_writes != 0
                        || s.pending_http != 0
                        || s.queues.iter().any(|(_, items)| !items.is_empty())
                    {
                        return Err(invalid(
                            "native first state differs from bound fresh runner",
                        ));
                    }
                }
                let expected_generation = if matches!(operation.input, InputDto::Connected { .. }) {
                    self.generation.checked_add(1)
                } else {
                    Some(self.generation)
                };
                if Some(operation.route.generation) != expected_generation {
                    return Err(invalid("native generation does not match connected input"));
                }
                let connection_matches = match &operation.input {
                    InputDto::Connected { connection, .. } => {
                        operation.after.connection == *connection
                    }
                    _ => operation.after.connection == operation.before.connection,
                };
                if !connection_matches {
                    return Err(invalid("native physical connection mismatch"));
                }
                let unsafe_state = operation.outcome.is_err()
                    || (operation.after.stop_requested && !operation.before.stop_requested)
                    || (operation.after.reconnect_requested
                        && !operation.before.reconnect_requested)
                    || operation.after.valid_books < operation.before.valid_books
                    || (operation.before.lifecycle == LifecycleDto::Live
                        && operation.after.lifecycle != LifecycleDto::Live);
                if unsafe_state && input_id >= self.fence {
                    self.fence = input_id;
                    if operation.after.reconnect_requested || operation.after.stop_requested {
                        self.source_active = false;
                    }
                }
                if let InputDto::Connected { connection, .. } = &operation.input {
                    self.physical_source = *connection;
                    self.physical_active = true;
                } else if safety(&operation.input) {
                    self.physical_active = false;
                }
                if operation.after.reconnect_requested || operation.after.stop_requested {
                    self.physical_active = false;
                }
                self.terminal |= operation.outcome.is_err()
                    || operation.after.stop_requested
                    || dto.observed_ns >= pending.deadline_ns;
                self.native_ordinal = operation.route.ordinal;
                self.permit = None;
                self.pending_owned_bytes -= pending.owned_bytes;
                self.native_pending_operations -= 1;
                self.previous_after = Some(operation.after.clone());
                self.active = Some(ActiveOperation {
                    input_id,
                    journal_ordinal: dto.route.ordinal,
                    operation,
                    intent: None,
                });
            }
            OwnerEventDto::ExecutionNotInvoked {
                input_id,
                permit_ordinal,
                cause,
            } => {
                let permit = self
                    .permit
                    .as_ref()
                    .ok_or_else(|| invalid("noninvocation without permit"))?;
                let p = self
                    .pending
                    .get(&input_id)
                    .ok_or_else(|| invalid("noninvocation without input"))?;
                if permit.indeterminate
                    || permit.input_id != input_id
                    || permit.journal_ordinal != permit_ordinal
                    || match cause {
                        OwnerNotExecutedCause::Preflight => false,
                        OwnerNotExecutedCause::Terminal => !self.terminal,
                        OwnerNotExecutedCause::Deadline => dto.observed_ns < p.deadline_ns,
                        OwnerNotExecutedCause::SourceConnection => true,
                    }
                {
                    return Err(invalid("noninvocation token or cause mismatch"));
                }
                if matches!(
                    cause,
                    OwnerNotExecutedCause::Preflight | OwnerNotExecutedCause::Deadline
                ) {
                    self.terminal = true;
                }
                self.permit = None;
                let p = self.pending.remove(&input_id).expect("checked input");
                self.pending_bytes -= p.bytes;
                self.pending_owned_bytes -= p.owned_bytes;
                self.native_pending_operations -= 1;
            }
            OwnerEventDto::ExecutionIndeterminate {
                input_id,
                permit_ordinal,
                ..
            } => {
                let permit = self
                    .permit
                    .as_mut()
                    .ok_or_else(|| invalid("uncertainty without permit"))?;
                if permit.indeterminate
                    || permit.input_id != input_id
                    || permit.journal_ordinal != permit_ordinal
                {
                    return Err(invalid("uncertainty token mismatch"));
                }
                permit.indeterminate = true;
                self.terminal = true;
            }
            OwnerEventDto::ReleaseIntent {
                input_id,
                operation_ordinal,
                outputs_eligible,
            } => {
                let a = self
                    .active
                    .as_ref()
                    .ok_or_else(|| invalid("intent without operation"))?;
                let p = self
                    .pending
                    .get(&input_id)
                    .ok_or_else(|| invalid("intent without input"))?;
                if self.terminal
                    || a.input_id != input_id
                    || a.journal_ordinal != operation_ordinal
                    || a.intent.is_some()
                    || input_id < self.fence
                    || dto.observed_ns >= p.deadline_ns
                    || a.operation.outcome.is_err()
                    || (outputs_eligible
                        && (!self.source_active
                            || self.source != p.connection
                            || a.operation.after.stop_requested
                            || a.operation.after.reconnect_requested))
                {
                    return Err(invalid("invalid or revoked release intent"));
                }
                self.active.as_mut().expect("checked active").intent =
                    Some((dto.route.ordinal, outputs_eligible));
            }
            OwnerEventDto::HandoffObserved {
                input_id,
                intent_ordinal,
            } => {
                let a = self
                    .active
                    .as_ref()
                    .ok_or_else(|| invalid("handoff without intent"))?;
                let p = self
                    .pending
                    .get(&input_id)
                    .ok_or_else(|| invalid("handoff without input"))?;
                let Some((intent, outputs_eligible)) = a.intent else {
                    return Err(invalid("handoff without intent"));
                };
                if self.terminal
                    || a.input_id != input_id
                    || intent != intent_ordinal
                    || input_id < self.fence
                    || dto.observed_ns >= p.deadline_ns
                {
                    return Err(invalid("handoff reference, fence or deadline mismatch"));
                }
                let a = self.active.take().expect("checked active");
                if let Some(wire) = self.wire.as_mut() {
                    let candidates = if outputs_eligible {
                        wire_candidates(
                            &a.operation,
                            input_id,
                            a.journal_ordinal,
                            dto.route.ordinal,
                            wire.policy(),
                        )?
                    } else {
                        Vec::new()
                    };
                    wire.install(candidates, p.deadline_ns)?;
                }
                let pending = self.pending.remove(&input_id).expect("checked input");
                self.pending_bytes -= pending.bytes;
                handoff = Some(RecordedOwnerHandoff {
                    input_id,
                    intent_ordinal,
                    observed_ns: dto.observed_ns,
                    outputs_eligible,
                    operation: a.operation,
                });
            }
            OwnerEventDto::Withheld { input_id } => {
                if self.active.as_ref().is_none_or(|a| a.input_id != input_id) {
                    return Err(invalid("withholding without matching committed operation"));
                }
                self.active = None;
                let pending = self.pending.remove(&input_id).expect("active input");
                if dto.observed_ns >= pending.deadline_ns {
                    self.terminal = true;
                }
                self.pending_bytes -= pending.bytes;
            }
            OwnerEventDto::NotExecuted { input_id, cause } => {
                let p = self
                    .pending
                    .get(&input_id)
                    .ok_or_else(|| invalid("nonexecution without ingress"))?;
                if self.active.is_some()
                    || self.permit.as_ref().is_some_and(|p| p.input_id == input_id)
                    || (!(self.envelope_version >= 3
                        && self.terminal
                        && cause == OwnerNotExecutedCause::Terminal)
                        && self.pending.first_key_value().map(|(id, _)| *id) != Some(input_id))
                {
                    return Err(invalid("nonexecution skips active or earlier input"));
                }
                let source_matches = match &p.input {
                    InputDto::Connected { connection, .. } => {
                        *connection == p.connection && *connection > self.physical_source
                    }
                    _ if frame(&p.input) => {
                        self.physical_active && p.connection == self.physical_source
                    }
                    _ => p.connection == self.physical_source,
                };
                if match cause {
                    OwnerNotExecutedCause::SourceConnection => source_matches,
                    OwnerNotExecutedCause::Deadline => dto.observed_ns < p.deadline_ns,
                    OwnerNotExecutedCause::Terminal => !self.terminal,
                    OwnerNotExecutedCause::Preflight => self.terminal,
                } {
                    return Err(invalid("nonexecution disposition does not match prefix"));
                }
                if matches!(
                    cause,
                    OwnerNotExecutedCause::Deadline | OwnerNotExecutedCause::Preflight
                ) {
                    self.terminal = true;
                }
                let pending = self.pending.remove(&input_id).expect("checked input");
                self.pending_bytes -= pending.bytes;
                self.pending_owned_bytes -= pending.owned_bytes;
                self.native_pending_operations -= 1;
            }
            OwnerEventDto::Fence { through_input_id }
            | OwnerEventDto::Revoked {
                through_input_id, ..
            } => {
                if through_input_id < self.fence
                    || through_input_id > self.accepted.saturating_add(1)
                {
                    return Err(invalid("owner fence reference"));
                }
                self.fence = through_input_id;
                self.terminal = true;
            }
        }
        if self.wire.is_some()
            && self.retention()?.pending_input_bytes > self.limits.max_retained_input_bytes
        {
            return Err(CodecError::TooLarge);
        }
        self.ordinal = dto.route.ordinal;
        self.generation = dto.route.generation;
        self.observed_ns = dto.observed_ns;
        Ok(handoff)
    }
}
fn duration_ns(value: DurationDto) -> Result<u64, CodecError> {
    u64::try_from(value.duration()?.as_nanos()).map_err(|_| invalid("owner duration overflow"))
}

#[cfg(test)]
#[path = "causal_owner_history_tests.rs"]
mod tests;
