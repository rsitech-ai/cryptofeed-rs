//! Pure offline MFR1-to-EventPulse input transformation.
//!
//! This crate has no adapter, network, filesystem, evidence, snapshot, risk,
//! order, paper, canary, or live authority.

#![forbid(unsafe_code)]

use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, DEFAULT_ACTION_BUFFER_CAPACITY, SessionAction, SessionInput,
    SessionMachine,
};
use marketfeed_dispatch::{DispatchError, EventDispatcher, PushOutcome};
use marketfeed_event_pulse::{
    EpinJson1Reader, EpinJson1Writer, ProspectiveCaptureAdmissionV1, ReplayInputError,
    wire::{
        ConfiguredTargetKeyV1, ConnectionKeyV1, CursorModeV1, CursorV1, DropCategoryV1,
        FaultScopeKindV1, FaultScopeV1, MechanicsInputV1, ReplayCatalogV1, Rfc3339Time,
        SystemChainPreimage, SystemFaultV1, SystemSourceV1, WireError,
    },
};
use marketfeed_model::{EventEnvelope, FrameStamp, OverflowPolicy, TimestampNs};
use marketfeed_recording::{
    BuildMetadata, Direction, FORMAT_VERSION, FrameOpcode, HEADER_SIZE, MetadataRecord, RawRecord,
    RawSegmentReader, RecordingError, SessionRecordingMetadata, decode_http_response,
    decode_metadata, decode_subscription_command,
};
use std::io::{self, Cursor, Write};
use thiserror::Error;

const MAX_ORDINARY_ACTIONS: usize = u16::MAX as usize;
const MAX_ITEMS: usize = u16::MAX as usize + 1;
const MAX_RAW_RECORDS: usize = 65_536;
const MAX_AUTHORED_INPUTS: usize = 65_536;
const MAX_EPIN_BYTES: usize = marketfeed_event_pulse::wire::MAX_INPUT_BYTES;
const MAX_MFR1_BYTES: usize = 256 * 1024 * 1024;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mfr1MetadataBindingV1 {
    build: BuildMetadata,
    session: SessionRecordingMetadata,
}

impl Mfr1MetadataBindingV1 {
    pub fn new(
        build: BuildMetadata,
        session: SessionRecordingMetadata,
    ) -> Result<Self, Mfr1TransformError> {
        if build.schema_version != 1
            || build.package_name.trim().is_empty()
            || build.package_version.trim().is_empty()
            || build.target_os.trim().is_empty()
            || build.target_arch.trim().is_empty()
            || session.schema_version != 1
            || session.adapter.trim().is_empty()
            || session.environment.trim().is_empty()
            || session.endpoint.trim().is_empty()
            || session.catalog_version == 0
            || session.catalog.is_empty()
        {
            return Err(Mfr1TransformError::InvalidExecutionMetadata);
        }
        Ok(Self { build, session })
    }
}

#[derive(Debug, Clone)]
pub struct Mfr1TransformContextV1 {
    admission: ProspectiveCaptureAdmissionV1,
    catalog: ReplayCatalogV1,
    session: Mfr1SessionBindingV1,
    metadata: Mfr1MetadataBindingV1,
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
        metadata: Mfr1MetadataBindingV1,
        dispatch_capacity: usize,
        overflow: OverflowPolicy,
    ) -> Result<Self, Mfr1TransformError> {
        let action_capacity = dispatch_capacity
            .checked_mul(4)
            .map(|capacity| capacity.max(DEFAULT_ACTION_BUFFER_CAPACITY))
            .filter(|capacity| *capacity <= MAX_ORDINARY_ACTIONS)
            .ok_or(Mfr1TransformError::InvalidExecutionMetadata)?;
        if dispatch_capacity == 0 || dispatch_capacity > MAX_ORDINARY_ACTIONS {
            return Err(Mfr1TransformError::InvalidExecutionMetadata);
        }
        if !matches!(
            overflow,
            OverflowPolicy::DropNewest | OverflowPolicy::FailEngine
        ) {
            return Err(Mfr1TransformError::UnsupportedOverflowPolicy);
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
        if metadata.session.session_id != session.session_id {
            return Err(Mfr1TransformError::InvalidExecutionMetadata);
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
            metadata,
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
    #[error("replay overflow policy is not supported by the immutable transform contract")]
    UnsupportedOverflowPolicy,
    #[error("selected session metadata is missing")]
    MissingSessionMetadata,
    #[error("exact build metadata is missing")]
    MissingBuildMetadata,
    #[error("build metadata conflicts with the immutable replay binding")]
    BuildMetadataMismatch,
    #[error("selected session metadata conflicts with the immutable replay binding")]
    SessionMetadataMismatch,
    #[error("MFR1 record count exceeds 65536")]
    RawRecordCapacity,
    #[error("MFR1 segment exceeds 256 MiB")]
    Mfr1Capacity,
    #[error("MFR1 must use format version 3")]
    FormatVersion,
    #[error("MFR1 header start does not equal the admitted connect coordinate")]
    HeaderStartMismatch,
    #[error("selected session monotonic clock regressed")]
    MonotonicRegression,
    #[error("authored mechanics input count exceeds 65536")]
    InputCapacity,
    #[error("canonical EPIN output exceeds 16 MiB")]
    EpinCapacity,
    #[error("canonical EPIN strict readback differs from staged inputs")]
    EpinReadbackMismatch,
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

    pub fn transform<M>(
        self,
        mut machine: M,
        mfr1_bytes: &[u8],
        connect_at: TimestampNs,
        not_after: Rfc3339Time,
    ) -> Result<Mfr1TransformOutputV1, Mfr1TransformError>
    where
        M: SessionMachine,
    {
        self.transform_owned(&mut machine, mfr1_bytes, connect_at, not_after)
    }

    pub fn transform_boxed(
        self,
        mut machine: Box<dyn SessionMachine>,
        mfr1_bytes: &[u8],
        connect_at: TimestampNs,
        not_after: Rfc3339Time,
    ) -> Result<Mfr1TransformOutputV1, Mfr1TransformError> {
        self.transform_owned(machine.as_mut(), mfr1_bytes, connect_at, not_after)
    }

    fn transform_owned(
        self,
        machine: &mut dyn SessionMachine,
        mfr1_bytes: &[u8],
        connect_at: TimestampNs,
        not_after: Rfc3339Time,
    ) -> Result<Mfr1TransformOutputV1, Mfr1TransformError> {
        ensure_mfr1_capacity(mfr1_bytes.len())?;
        let capture_start = self.context.admission.capture_starts_at().utc_micros();
        let connect_micros = connect_at.0.div_euclid(1_000);
        if not_after < *self.context.admission.capture_starts_at()
            || connect_micros < capture_start
            || connect_micros > not_after.utc_micros()
        {
            return Err(Mfr1TransformError::OutsideAdmissionWindow);
        }
        let records = self.prevalidate(mfr1_bytes, connect_at, &not_after)?;
        let mut state = TransformState {
            actions: ActionBuffer::with_capacity(MAX_AUTHORED_INPUTS),
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

        for record in records {
            if record.header.session.0 != self.context.session.session_id {
                continue;
            }
            let available_at = TimestampNs(record.header.receive_ts_ns);
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
                    // Session metadata was decoded and bound before replay began.
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
        let epin_json1 = canonical_epin(&state.inputs, not_after)?;
        Ok(Mfr1TransformOutputV1 {
            frames: state.frames,
            inputs: state.inputs,
            epin_json1,
            frames_applied: state.frames_applied,
            dropped_action_buffer: state.dropped_action_buffer,
            dropped_market_dispatch: state.dropped_market_dispatch,
        })
    }

    fn prevalidate(
        &self,
        mfr1_bytes: &[u8],
        connect_at: TimestampNs,
        not_after: &Rfc3339Time,
    ) -> Result<Vec<RawRecord>, Mfr1TransformError> {
        let capture_start = self.context.admission.capture_starts_at().utc_micros();
        let mut consumed = HEADER_SIZE;
        let mut reader = RawSegmentReader::open(Cursor::new(mfr1_bytes))?;
        if reader.format_version != FORMAT_VERSION {
            return Err(Mfr1TransformError::FormatVersion);
        }
        if reader.start_ts_ns != connect_at.0 {
            return Err(Mfr1TransformError::HeaderStartMismatch);
        }
        let mut records = Vec::new();
        let mut last_available = connect_at;
        let mut last_monotonic = None;
        let mut build_metadata = None;
        let mut selected_metadata = None;
        while let Some(record) = reader.read_record()? {
            if records.len() == MAX_RAW_RECORDS {
                return Err(Mfr1TransformError::RawRecordCapacity);
            }
            consumed = consumed
                .checked_add(
                    usize::try_from(record.header.record_len)
                        .map_err(|_| Mfr1TransformError::CoordinateOverflow)?,
                )
                .ok_or(Mfr1TransformError::CoordinateOverflow)?;
            if record.header.session.0 == self.context.session.session_id {
                let available_at = TimestampNs(record.header.receive_ts_ns);
                let available_micros = available_at.0.div_euclid(1_000);
                if available_micros < capture_start || available_micros > not_after.utc_micros() {
                    return Err(Mfr1TransformError::OutsideAdmissionWindow);
                }
                if available_at < last_available {
                    return Err(Mfr1TransformError::AvailabilityRegression);
                }
                last_available = available_at;
                if last_monotonic.is_some_and(|previous| record.header.monotonic_ns < previous) {
                    return Err(Mfr1TransformError::MonotonicRegression);
                }
                last_monotonic = Some(record.header.monotonic_ns);
                match record.header.opcode {
                    FrameOpcode::Metadata => match decode_metadata(&record.payload)? {
                        MetadataRecord::Session(metadata) => {
                            if record.header.direction != Direction::Inbound {
                                return Err(Mfr1TransformError::SessionMetadataMismatch);
                            }
                            if selected_metadata.replace(metadata).is_some() {
                                return Err(Mfr1TransformError::SessionMetadataMismatch);
                            }
                        }
                        MetadataRecord::Build(_) => {
                            return Err(Mfr1TransformError::BuildMetadataMismatch);
                        }
                    },
                    FrameOpcode::HttpResponse => {
                        decode_http_response(&record.payload)?;
                    }
                    FrameOpcode::SubscriptionCommand => {
                        decode_subscription_command(&record.payload)?;
                    }
                    _ => {}
                }
            } else if record.header.opcode == FrameOpcode::Metadata {
                match decode_metadata(&record.payload)? {
                    MetadataRecord::Build(build) => {
                        if record.header.session.0 != 0
                            || record.header.direction != Direction::Inbound
                            || record.header.receive_ts_ns != reader.start_ts_ns
                            || build_metadata.replace(build).is_some()
                        {
                            return Err(Mfr1TransformError::BuildMetadataMismatch);
                        }
                    }
                    MetadataRecord::Session(metadata) => {
                        if metadata.session_id == self.context.session.session_id {
                            return Err(Mfr1TransformError::SessionMetadataMismatch);
                        }
                    }
                }
            }
            records.push(record);
        }
        if consumed != mfr1_bytes.len() {
            return Err(Mfr1TransformError::Recording(
                "trailing or truncated MFR1 bytes".into(),
            ));
        }
        let build = build_metadata.ok_or(Mfr1TransformError::MissingBuildMetadata)?;
        if build != self.context.metadata.build {
            return Err(Mfr1TransformError::BuildMetadataMismatch);
        }
        let metadata = selected_metadata.ok_or(Mfr1TransformError::MissingSessionMetadata)?;
        if metadata != self.context.metadata.session {
            return Err(Mfr1TransformError::SessionMetadataMismatch);
        }
        self.validate_session_metadata(&metadata)?;
        Ok(records)
    }

    fn validate_session_metadata(
        &self,
        metadata: &SessionRecordingMetadata,
    ) -> Result<(), Mfr1TransformError> {
        if metadata.schema_version != 1
            || metadata.session_id != self.context.session.session_id
            || metadata.catalog.is_empty()
            || metadata.adapter.trim().is_empty()
            || metadata.environment.trim().is_empty()
            || metadata.endpoint.trim().is_empty()
            || metadata.catalog_version == 0
        {
            return Err(Mfr1TransformError::SessionMetadataMismatch);
        }
        let venue = self
            .context
            .catalog
            .venue_source(metadata.venue_id)
            .ok_or(Mfr1TransformError::SessionMetadataMismatch)?;
        let mut ids = std::collections::BTreeSet::new();
        let mut relevant_rows = 0usize;
        for row in &metadata.catalog {
            if !ids.insert(row.instrument_id) {
                return Err(Mfr1TransformError::SessionMetadataMismatch);
            }
            let Some(instrument) = self.context.catalog.instrument(row.instrument_id) else {
                continue;
            };
            if !topology_contains(
                &self.context,
                venue.source_id(),
                instrument,
                &self.context.session.connection,
            ) {
                continue;
            }
            relevant_rows = relevant_rows
                .checked_add(1)
                .ok_or(Mfr1TransformError::SessionMetadataMismatch)?;
            if row.native_symbol != instrument.venue_symbol()
                || row.base != instrument.base_asset()
                || row.quote != instrument.quote_asset()
                || row.kind.to_ascii_uppercase() != instrument.market_type()
                || instrument.venue() != venue.venue()
            {
                return Err(Mfr1TransformError::SessionMetadataMismatch);
            }
        }
        if metadata
            .initial_subscriptions
            .iter()
            .any(|subscription| !ids.contains(&subscription.instrument_id))
        {
            return Err(Mfr1TransformError::SessionMetadataMismatch);
        }
        if relevant_rows == 0 {
            return Err(Mfr1TransformError::SessionMetadataMismatch);
        }
        Ok(())
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
        if self.actions.take_dropped() > 0 {
            return Err(Mfr1TransformError::CoordinateOverflow);
        }
        let observed: Vec<_> = self.actions.drain().collect();
        if observed
            .iter()
            .any(|action| matches!(action, SessionAction::EmitSystem(_)))
        {
            return Err(Mfr1TransformError::UnsupportedSystemAction);
        }
        if observed
            .iter()
            .any(|action| matches!(action, SessionAction::Reconnect(_)))
        {
            return Err(Mfr1TransformError::UnsupportedReconnect);
        }
        if observed.iter().any(
            |action| matches!(action, SessionAction::EmitBatch(batch) if batch.events.is_empty()),
        ) {
            return Err(Mfr1TransformError::EmptyBatch);
        }

        let dropped = observed.len().saturating_sub(context.action_capacity);
        let action_drops =
            u64::try_from(dropped).map_err(|_| Mfr1TransformError::CoordinateOverflow)?;
        let retained: Vec<_> = observed.into_iter().take(context.action_capacity).collect();
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
        let _accepted_batches = self.dispatch.drain_batches();
        let _accepted_systems = self.dispatch.drain_systems();
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
                    ensure_input_capacity(
                        self.inputs.len(),
                        frame_inputs
                            .len()
                            .checked_add(1)
                            .ok_or(Mfr1TransformError::InputCapacity)?,
                    )?;
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
            ensure_input_capacity(
                self.inputs.len(),
                frame_inputs
                    .len()
                    .checked_add(1)
                    .ok_or(Mfr1TransformError::InputCapacity)?,
            )?;
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
        ensure_input_capacity(self.inputs.len(), frame_inputs.len())?;
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
    if !topology_contains(
        context,
        venue.source_id(),
        instrument,
        &context.session.connection,
    ) {
        return Err(Mfr1TransformError::TopologyMismatch);
    }
    Ok(())
}

fn topology_contains(
    context: &Mfr1TransformContextV1,
    source_id: &str,
    instrument: &marketfeed_event_pulse::wire::InstrumentIdentityV1,
    connection: &ConnectionKeyV1,
) -> bool {
    context
        .admission
        .mechanics_config()
        .contributors()
        .iter()
        .filter(|contributor| {
            contributor.key().source_id() == source_id
                && contributor.key().instrument() == instrument
                && context
                    .admission
                    .mechanics_config()
                    .contributor_connections()
                    .get(contributor.key())
                    == Some(connection)
        })
        .count()
        == 1
}

fn ensure_input_capacity(current: usize, additional: usize) -> Result<(), Mfr1TransformError> {
    if current
        .checked_add(additional)
        .is_none_or(|count| count > MAX_AUTHORED_INPUTS)
    {
        return Err(Mfr1TransformError::InputCapacity);
    }
    Ok(())
}

fn ensure_mfr1_capacity(size: usize) -> Result<(), Mfr1TransformError> {
    if size > MAX_MFR1_BYTES {
        return Err(Mfr1TransformError::Mfr1Capacity);
    }
    Ok(())
}

fn canonical_epin(
    inputs: &[MechanicsInputV1],
    not_after: Rfc3339Time,
) -> Result<Vec<u8>, Mfr1TransformError> {
    let mut writer = EpinJson1Writer::new(BoundedEpin::default());
    for input in inputs {
        if let Err(error) = writer.write_input(input) {
            return Err(match error {
                ReplayInputError::Io(_) => Mfr1TransformError::EpinCapacity,
                other => Mfr1TransformError::Epin(other.to_string()),
            });
        }
    }
    let bytes = writer.finish().bytes;
    let decoded = EpinJson1Reader::new(bytes.as_slice(), not_after).read_all()?;
    if decoded != inputs {
        return Err(Mfr1TransformError::EpinReadbackMismatch);
    }
    Ok(bytes)
}

#[derive(Default)]
struct BoundedEpin {
    bytes: Vec<u8>,
}

impl Write for BoundedEpin {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(buffer.len())
            .is_none_or(|size| size > MAX_EPIN_BYTES)
        {
            return Err(io::Error::other("canonical EPIN aggregate capacity"));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
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

#[cfg(test)]
mod capacity_tests {
    use super::*;

    #[test]
    fn authored_input_capacity_accepts_exact_bound_and_rejects_one_over() {
        assert_eq!(ensure_input_capacity(65_535, 1), Ok(()));
        assert_eq!(
            ensure_input_capacity(65_536, 1),
            Err(Mfr1TransformError::InputCapacity)
        );
    }

    #[test]
    fn mfr1_capacity_accepts_exact_bound_and_rejects_one_over_without_allocation() {
        assert_eq!(ensure_mfr1_capacity(MAX_MFR1_BYTES), Ok(()));
        assert_eq!(
            ensure_mfr1_capacity(MAX_MFR1_BYTES + 1),
            Err(Mfr1TransformError::Mfr1Capacity)
        );
    }

    #[test]
    fn epin_sink_accepts_exact_byte_bound_and_rejects_one_over() {
        let mut sink = BoundedEpin::default();
        assert_eq!(
            sink.write(&vec![0; MAX_EPIN_BYTES]).unwrap(),
            MAX_EPIN_BYTES
        );
        assert!(sink.write(b"x").is_err());
        assert_eq!(sink.bytes.len(), MAX_EPIN_BYTES);
    }
}
