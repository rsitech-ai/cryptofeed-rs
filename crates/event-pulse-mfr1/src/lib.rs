//! Pure offline MFR1-to-EventPulse input transformation.
//!
//! This crate has no adapter, network, filesystem, evidence, snapshot, risk,
//! order, paper, canary, or live authority.

#![forbid(unsafe_code)]

use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, SessionAction, SessionInput, SessionMachine,
};
use marketfeed_dispatch::{DispatchError, EventDispatcher, PushOutcome};
use marketfeed_event_pulse::{
    EpinJson1Writer, ProspectiveCaptureAdmissionV1, ReplayInputError,
    wire::{
        ConfiguredTargetKeyV1, ConnectionKeyV1, CursorModeV1, CursorV1, DropCategoryV1,
        FaultScopeKindV1, FaultScopeV1, MechanicsInputV1, ReplayCatalogV1, Rfc3339Time,
        SystemChainPreimage, SystemFaultV1, SystemSourceV1, WireError,
    },
};
use marketfeed_model::{EventEnvelope, FrameStamp, OverflowPolicy, TimestampNs};
use marketfeed_recording::{
    Direction, FrameOpcode, HEADER_SIZE, RawSegmentReader, RecordingError, decode_http_response,
    decode_metadata, decode_subscription_command,
};
use thiserror::Error;

const MAX_ORDINARY_ACTIONS: usize = u16::MAX as usize;
const MAX_ITEMS: usize = u16::MAX as usize + 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mfr1SessionBindingV1 {
    connection: ConnectionKeyV1,
    connection_id: u64,
    session_id: u64,
}

impl Mfr1SessionBindingV1 {
    pub const fn new(connection: ConnectionKeyV1, connection_id: u64, session_id: u64) -> Self {
        Self {
            connection,
            connection_id,
            session_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Mfr1TransformContextV1 {
    admission: ProspectiveCaptureAdmissionV1,
    catalog: ReplayCatalogV1,
    session: Mfr1SessionBindingV1,
    system_source: SystemSourceV1,
    action_capacity: usize,
    dispatch_capacity: usize,
    overflow: OverflowPolicy,
}

impl Mfr1TransformContextV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        admission: ProspectiveCaptureAdmissionV1,
        catalog: ReplayCatalogV1,
        session: Mfr1SessionBindingV1,
        system_source: SystemSourceV1,
        action_capacity: usize,
        dispatch_capacity: usize,
        overflow: OverflowPolicy,
    ) -> Result<Self, Mfr1TransformError> {
        if action_capacity == 0
            || action_capacity > MAX_ORDINARY_ACTIONS
            || dispatch_capacity == 0
            || dispatch_capacity > MAX_ORDINARY_ACTIONS
        {
            return Err(Mfr1TransformError::InvalidExecutionMetadata);
        }
        catalog.validate()?;
        let config = admission.mechanics_config();
        if !config.connections().contains(&session.connection) {
            return Err(Mfr1TransformError::TopologyMismatch);
        }
        let epoch_count = catalog
            .connection_epochs()
            .iter()
            .filter(|epoch| {
                epoch.connection_id() == session.connection_id
                    && epoch.session_id() == session.session_id
            })
            .count();
        if epoch_count != 1 {
            return Err(Mfr1TransformError::CatalogMismatch);
        }
        let target = ConfiguredTargetKeyV1::processor(config.processor_id())?;
        if system_source.key().scope_kind() != FaultScopeKindV1::Processor
            || system_source.key().cursor_mode() != CursorModeV1::Derived
            || system_source.key().configured_target_key() != &target
            || config
                .system_sources()
                .iter()
                .filter(|source| {
                    source.scope_kind() == FaultScopeKindV1::Processor
                        && source.cursor_mode() == CursorModeV1::Derived
                        && source.configured_target_key() == &target
                })
                .count()
                != 1
            || !config
                .system_sources()
                .contains(&system_source.key().clone())
        {
            return Err(Mfr1TransformError::TopologyMismatch);
        }
        Ok(Self {
            admission,
            catalog,
            session,
            system_source,
            action_capacity,
            dispatch_capacity,
            overflow,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mfr1MechanicsFrameV1 {
    frame_seq: u64,
    available_at: TimestampNs,
    inputs: Vec<MechanicsInputV1>,
}

impl Mfr1MechanicsFrameV1 {
    pub const fn frame_seq(&self) -> u64 {
        self.frame_seq
    }

    pub const fn available_at(&self) -> TimestampNs {
        self.available_at
    }

    pub fn inputs(&self) -> &[MechanicsInputV1] {
        &self.inputs
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mfr1TransformOutputV1 {
    frames: Vec<Mfr1MechanicsFrameV1>,
    inputs: Vec<MechanicsInputV1>,
    epin_json1: Vec<u8>,
    frames_applied: u64,
    dropped_action_buffer: u64,
    dropped_market_dispatch: u64,
}

impl Mfr1TransformOutputV1 {
    pub fn frames(&self) -> &[Mfr1MechanicsFrameV1] {
        &self.frames
    }

    pub fn inputs(&self) -> &[MechanicsInputV1] {
        &self.inputs
    }

    pub fn epin_json1(&self) -> &[u8] {
        &self.epin_json1
    }

    pub const fn frames_applied(&self) -> u64 {
        self.frames_applied
    }

    pub const fn dropped_action_buffer(&self) -> u64 {
        self.dropped_action_buffer
    }

    pub const fn dropped_market_dispatch(&self) -> u64 {
        self.dropped_market_dispatch
    }

    pub const fn evidence_authoring_allowed(&self) -> bool {
        false
    }

    pub const fn blocker(&self) -> &'static str {
        "blocked:fixture-provenance"
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Mfr1TransformError {
    #[error("MFR1 recording is invalid: {0}")]
    Recording(String),
    #[error("session replay failed: {0}")]
    Adapter(String),
    #[error("bounded dispatch failed: {0}")]
    Dispatch(String),
    #[error("strict EventPulse input is invalid: {0}")]
    Wire(String),
    #[error("canonical EPIN output failed: {0}")]
    Epin(String),
    #[error("immutable replay execution metadata is invalid")]
    InvalidExecutionMetadata,
    #[error("MFR1 availability regressed")]
    AvailabilityRegression,
    #[error("connect, selected record, or decision time is outside the admitted capture window")]
    OutsideAdmissionWindow,
    #[error("action-producing inbound frame zero collides with replay-start")]
    MechanicsFrameZero,
    #[error("action-producing inbound frame coordinate did not increase")]
    MechanicsFrameRegression { previous: u64, current: u64 },
    #[error("record does not match the immutable replay catalog")]
    CatalogMismatch,
    #[error("record does not match the checked EventPulse topology")]
    TopologyMismatch,
    #[error("frame/action/item coordinate overflowed")]
    CoordinateOverflow,
    #[error("empty market batch has no lossless mechanics mapping")]
    EmptyBatch,
    #[error("ordinary system output is not bound by the prospective topology")]
    UnsupportedSystemAction,
    #[error("reconnect request is not proof of a disconnected event")]
    UnsupportedReconnect,
}

impl From<RecordingError> for Mfr1TransformError {
    fn from(value: RecordingError) -> Self {
        Self::Recording(value.to_string())
    }
}

impl From<AdapterError> for Mfr1TransformError {
    fn from(value: AdapterError) -> Self {
        Self::Adapter(value.to_string())
    }
}

impl From<DispatchError> for Mfr1TransformError {
    fn from(value: DispatchError) -> Self {
        Self::Dispatch(value.to_string())
    }
}

impl From<WireError> for Mfr1TransformError {
    fn from(value: WireError) -> Self {
        Self::Wire(value.to_string())
    }
}

impl From<ReplayInputError> for Mfr1TransformError {
    fn from(value: ReplayInputError) -> Self {
        Self::Epin(value.to_string())
    }
}

pub struct Mfr1TransformerV1 {
    context: Mfr1TransformContextV1,
}

impl Mfr1TransformerV1 {
    pub fn new(context: Mfr1TransformContextV1) -> Self {
        Self { context }
    }

    pub fn transform(
        &self,
        machine: &mut dyn SessionMachine,
        mfr1_bytes: &[u8],
        connect_at: TimestampNs,
        not_after: Rfc3339Time,
    ) -> Result<Mfr1TransformOutputV1, Mfr1TransformError> {
        let capture_start = self.context.admission.capture_starts_at().utc_micros();
        let connect_micros = connect_at.0.div_euclid(1_000);
        if not_after < *self.context.admission.capture_starts_at()
            || connect_micros < capture_start
            || connect_micros > not_after.utc_micros()
        {
            return Err(Mfr1TransformError::OutsideAdmissionWindow);
        }
        let mut state = TransformState {
            actions: ActionBuffer::with_capacity(self.context.action_capacity),
            dispatch: EventDispatcher::new(
                self.context.dispatch_capacity,
                self.context.dispatch_capacity,
                self.context.overflow,
            ),
            frames: Vec::new(),
            inputs: Vec::new(),
            frames_applied: 0,
            last_available: connect_at,
            last_mechanics_frame: None,
            system_chain_head: None,
            dropped_action_buffer: 0,
            dropped_market_dispatch: 0,
        };

        state.apply_frame(
            &self.context,
            machine,
            0,
            connect_at,
            true,
            |machine, actions| machine.on_replay_start(connect_at, actions),
        )?;

        let mut consumed = HEADER_SIZE;
        let mut reader = RawSegmentReader::from_bytes(mfr1_bytes.to_vec())?;
        while let Some(record) = reader.read_record()? {
            consumed = consumed
                .checked_add(
                    usize::try_from(record.header.record_len)
                        .map_err(|_| Mfr1TransformError::CoordinateOverflow)?,
                )
                .ok_or(Mfr1TransformError::CoordinateOverflow)?;
            if record.header.session.0 != self.context.session.session_id {
                continue;
            }
            let available_at = TimestampNs(record.header.receive_ts_ns);
            let available_micros = available_at.0.div_euclid(1_000);
            if available_micros < capture_start || available_micros > not_after.utc_micros() {
                return Err(Mfr1TransformError::OutsideAdmissionWindow);
            }
            if available_at < state.last_available {
                return Err(Mfr1TransformError::AvailabilityRegression);
            }
            state.last_available = available_at;
            if record.header.direction != Direction::Inbound {
                continue;
            }
            let stamp = FrameStamp {
                receive_ts: available_at,
                mono_ns: record.header.monotonic_ns,
            };
            let frame_seq = record.header.frame_seq;
            let mut payload = record.payload;
            match record.header.opcode {
                FrameOpcode::Text | FrameOpcode::Binary | FrameOpcode::Pong => {
                    state.apply_frame(
                        &self.context,
                        machine,
                        frame_seq,
                        available_at,
                        false,
                        |machine, actions| match record.header.opcode {
                            FrameOpcode::Text => machine.on_input(
                                SessionInput::TextFrame {
                                    bytes: &mut payload,
                                    received: stamp,
                                },
                                actions,
                            ),
                            FrameOpcode::Binary => machine.on_input(
                                SessionInput::BinaryFrame {
                                    bytes: &mut payload,
                                    received: stamp,
                                },
                                actions,
                            ),
                            FrameOpcode::Pong => machine.on_input(
                                SessionInput::Pong {
                                    payload: &payload,
                                    received: stamp,
                                },
                                actions,
                            ),
                            _ => unreachable!(),
                        },
                    )?;
                    state.bump_frames()?;
                }
                FrameOpcode::HttpResponse => {
                    let (request_id, response) = decode_http_response(&payload)?;
                    state.apply_frame(
                        &self.context,
                        machine,
                        frame_seq,
                        available_at,
                        false,
                        |machine, actions| {
                            machine.on_input(
                                SessionInput::HttpResponse {
                                    request_id,
                                    response: &response,
                                    received: stamp,
                                },
                                actions,
                            )
                        },
                    )?;
                    state.bump_frames()?;
                }
                FrameOpcode::Metadata => {
                    decode_metadata(&payload)?;
                }
                FrameOpcode::SubscriptionCommand => {
                    let (command, recorded_wire) = decode_subscription_command(&payload)?;
                    state.apply_frame(
                        &self.context,
                        machine,
                        frame_seq,
                        available_at,
                        false,
                        |machine, _actions| {
                            let prepared = machine.prepare_dynamic_subscription(&command)?;
                            if prepared != recorded_wire {
                                return Err(AdapterError::Parse(
                                    "recorded subscription wire action mismatch".into(),
                                ));
                            }
                            machine.commit_dynamic_subscription(&command);
                            Ok(())
                        },
                    )?;
                    state.bump_frames()?;
                }
                FrameOpcode::Ping | FrameOpcode::Close => {}
            }
        }
        if consumed != mfr1_bytes.len() {
            return Err(Mfr1TransformError::Recording(
                "trailing or truncated MFR1 bytes".into(),
            ));
        }

        let mut writer = EpinJson1Writer::new(Vec::new());
        for input in &state.inputs {
            writer.write_input(input)?;
        }
        Ok(Mfr1TransformOutputV1 {
            frames: state.frames,
            inputs: state.inputs,
            epin_json1: writer.finish(),
            frames_applied: state.frames_applied,
            dropped_action_buffer: state.dropped_action_buffer,
            dropped_market_dispatch: state.dropped_market_dispatch,
        })
    }
}

struct TransformState {
    actions: ActionBuffer,
    dispatch: EventDispatcher,
    frames: Vec<Mfr1MechanicsFrameV1>,
    inputs: Vec<MechanicsInputV1>,
    frames_applied: u64,
    last_available: TimestampNs,
    last_mechanics_frame: Option<u64>,
    system_chain_head: Option<String>,
    dropped_action_buffer: u64,
    dropped_market_dispatch: u64,
}

impl TransformState {
    fn bump_frames(&mut self) -> Result<(), Mfr1TransformError> {
        self.frames_applied = self
            .frames_applied
            .checked_add(1)
            .ok_or(Mfr1TransformError::CoordinateOverflow)?;
        Ok(())
    }

    fn apply_frame<F>(
        &mut self,
        context: &Mfr1TransformContextV1,
        machine: &mut dyn SessionMachine,
        frame_seq: u64,
        available_at: TimestampNs,
        allow_zero: bool,
        apply: F,
    ) -> Result<(), Mfr1TransformError>
    where
        F: FnOnce(&mut dyn SessionMachine, &mut ActionBuffer) -> Result<(), AdapterError>,
    {
        self.actions.clear();
        let _ = self.actions.take_dropped();
        apply(machine, &mut self.actions)?;
        let retained = self.actions.as_slice();
        if retained
            .iter()
            .any(|action| matches!(action, SessionAction::EmitSystem(_)))
        {
            return Err(Mfr1TransformError::UnsupportedSystemAction);
        }
        if retained
            .iter()
            .any(|action| matches!(action, SessionAction::Reconnect(_)))
        {
            return Err(Mfr1TransformError::UnsupportedReconnect);
        }
        if retained.iter().any(
            |action| matches!(action, SessionAction::EmitBatch(batch) if batch.events.is_empty()),
        ) {
            return Err(Mfr1TransformError::EmptyBatch);
        }

        let action_drops = self.actions.take_dropped();
        let retained: Vec<_> = self.actions.drain().collect();
        let has_market = retained
            .iter()
            .any(|action| matches!(action, SessionAction::EmitBatch(_)));
        let mut market_drops = 0u64;
        for action in &retained {
            if let SessionAction::EmitBatch(batch) = action {
                market_drops = market_drops
                    .checked_add(push_drop(self.dispatch.push_batch(batch.clone())?)?)
                    .ok_or(Mfr1TransformError::CoordinateOverflow)?;
            }
        }
        let has_mechanics = has_market || action_drops > 0 || market_drops > 0;
        if !has_mechanics {
            return Ok(());
        }
        if frame_seq == 0 && !allow_zero {
            return Err(Mfr1TransformError::MechanicsFrameZero);
        }
        if frame_seq != 0 {
            if let Some(previous) = self.last_mechanics_frame {
                if frame_seq <= previous {
                    return Err(Mfr1TransformError::MechanicsFrameRegression {
                        previous,
                        current: frame_seq,
                    });
                }
            }
            self.last_mechanics_frame = Some(frame_seq);
        } else if self.last_mechanics_frame.is_some() || !self.frames.is_empty() {
            return Err(Mfr1TransformError::MechanicsFrameZero);
        }

        let mut frame_inputs = Vec::new();
        for (action_index, action) in retained.into_iter().enumerate() {
            let action_index =
                u32::try_from(action_index).map_err(|_| Mfr1TransformError::CoordinateOverflow)?;
            if let SessionAction::EmitBatch(batch) = action {
                if batch.session.0 != context.session.session_id {
                    return Err(Mfr1TransformError::CatalogMismatch);
                }
                if batch.events.len() > MAX_ITEMS {
                    return Err(Mfr1TransformError::CoordinateOverflow);
                }
                for (item_index, mut envelope) in batch.events.into_iter().enumerate() {
                    envelope.frame_seq = frame_seq;
                    envelope.receive_ts = available_at;
                    envelope.event_index = u16::try_from(item_index)
                        .map_err(|_| Mfr1TransformError::CoordinateOverflow)?;
                    validate_market(context, &envelope)?;
                    frame_inputs.push(MechanicsInputV1::market(
                        envelope,
                        action_index,
                        context.catalog.clone(),
                    )?);
                }
            }
        }
        for (category, item_index, count) in [
            (DropCategoryV1::ActionBuffer, 0, action_drops),
            (DropCategoryV1::MarketDispatch, 1, market_drops),
        ] {
            if count == 0 {
                continue;
            }
            frame_inputs.push(self.drop_input(
                context,
                frame_seq,
                item_index,
                available_at,
                count,
                category,
            )?);
        }
        self.dropped_action_buffer = self
            .dropped_action_buffer
            .checked_add(action_drops)
            .ok_or(Mfr1TransformError::CoordinateOverflow)?;
        self.dropped_market_dispatch = self
            .dropped_market_dispatch
            .checked_add(market_drops)
            .ok_or(Mfr1TransformError::CoordinateOverflow)?;
        self.inputs.extend(frame_inputs.iter().cloned());
        self.frames.push(Mfr1MechanicsFrameV1 {
            frame_seq,
            available_at,
            inputs: frame_inputs,
        });
        Ok(())
    }

    fn drop_input(
        &mut self,
        context: &Mfr1TransformContextV1,
        frame_seq: u64,
        item_index: u32,
        available_at: TimestampNs,
        count: u64,
        category: DropCategoryV1,
    ) -> Result<MechanicsInputV1, Mfr1TransformError> {
        let time = Rfc3339Time::from_unix_nanos(available_at.0)?;
        let input = MechanicsInputV1::system(
            context.system_source.clone(),
            FaultScopeV1::processor(context.admission.mechanics_config().processor_id())?,
            time.clone(),
            time,
            CursorV1::derived_drop(frame_seq, item_index)?,
            SystemFaultV1::events_dropped(count, category)?,
            self.system_chain_head.clone(),
        )?;
        self.system_chain_head = Some(match self.system_chain_head.as_deref() {
            Some(previous) => SystemChainPreimage::hash_next(previous, input.payload_hash())?,
            None => SystemChainPreimage::hash_first(input.payload_hash())?,
        });
        Ok(input)
    }
}

fn validate_market(
    context: &Mfr1TransformContextV1,
    envelope: &EventEnvelope,
) -> Result<(), Mfr1TransformError> {
    if envelope.connection.0 != context.session.connection_id
        || envelope.session.0 != context.session.session_id
    {
        return Err(Mfr1TransformError::CatalogMismatch);
    }
    let venue = context
        .catalog
        .venue_source(envelope.venue.0)
        .ok_or(Mfr1TransformError::CatalogMismatch)?;
    let instrument = envelope
        .instrument
        .and_then(|id| context.catalog.instrument(id.0))
        .ok_or(Mfr1TransformError::CatalogMismatch)?;
    let count = context
        .admission
        .mechanics_config()
        .contributors()
        .iter()
        .filter(|contributor| {
            contributor.key().source_id() == venue.source_id()
                && contributor.key().instrument() == instrument
                && context
                    .admission
                    .mechanics_config()
                    .contributor_connections()
                    .get(contributor.key())
                    == Some(&context.session.connection)
        })
        .count();
    if count != 1 {
        return Err(Mfr1TransformError::TopologyMismatch);
    }
    Ok(())
}

fn push_drop(outcome: PushOutcome) -> Result<u64, Mfr1TransformError> {
    match outcome {
        PushOutcome::Accepted => Ok(0),
        PushOutcome::DroppedNewest => Ok(1),
        PushOutcome::DroppedOldest { dropped } => {
            u64::try_from(dropped).map_err(|_| Mfr1TransformError::CoordinateOverflow)
        }
    }
}
