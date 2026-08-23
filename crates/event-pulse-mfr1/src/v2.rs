//! Routed Binance MFR1 -> MechanicsInputV2 transformation.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, Cursor, Write},
};

use marketfeed_adapter_api::{ActionBuffer, SessionAction, SessionInput, SessionMachine};
use marketfeed_adapter_binance::{
    BinanceUsdmRouteV4, BinanceUsdmSession, UsdmDecoded, UsdmRoutedV4Decoded,
    decode_usdm_routed_v4_text,
};
use marketfeed_dispatch::{EventDispatcher, PushOutcome};
use marketfeed_event_pulse::{
    MarketCursorV2, MechanicsInputV2, MechanicsInputV2JsonlReader, MechanicsInputV2JsonlWriter,
    ProspectiveCaptureAdmissionV2, SourceProvenanceV2,
    wire::{
        ConnectionKeyV1, CursorV1, DropCategoryV1, FaultScopeV1, MechanicsInputV1, ReplayCatalogV1,
        Rfc3339Time, SystemChainPreimage, SystemFaultV1, SystemSourceV1,
    },
};
use marketfeed_model::{EventEnvelope, FrameStamp, MarketEvent, OverflowPolicy, TimestampNs};
use marketfeed_recording::{
    BuildMetadata, Direction, FORMAT_VERSION, FrameOpcode, HEADER_SIZE, MetadataRecord, RawRecord,
    RawSegmentReader, SessionRecordingMetadata, decode_http_response, decode_metadata,
    decode_subscription_command,
};
use thiserror::Error;

const MAX_MFR1_BYTES: usize = 256 * 1024 * 1024;
const MAX_REPLAY_RECORDS: usize = 65_536;
const MAX_AUTHORED_INPUTS: usize = 65_536;
const MAX_JSONL_BYTES: usize = 16 * 1024 * 1024;
const MAX_ACTIONS_PER_FRAME: usize = 65_536;

fn ensure_at_most(value: usize, limit: usize) -> Result<(), Mfr1TransformErrorV2> {
    if value > limit {
        Err(Mfr1TransformErrorV2::Capacity)
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinanceMfr1RouteV2 {
    Public,
    Market,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mfr1SessionBindingV2 {
    pub(crate) connection: ConnectionKeyV1,
    pub(crate) connection_id: u64,
    pub(crate) session_id: u64,
    pub(crate) route: BinanceMfr1RouteV2,
}

impl Mfr1SessionBindingV2 {
    pub const fn new(
        connection: ConnectionKeyV1,
        connection_id: u64,
        session_id: u64,
        route: BinanceMfr1RouteV2,
    ) -> Self {
        Self {
            connection,
            connection_id,
            session_id,
            route,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mfr1MetadataBindingV2 {
    pub(crate) build: BuildMetadata,
    pub(crate) session: SessionRecordingMetadata,
}

impl Mfr1MetadataBindingV2 {
    pub fn new(
        build: BuildMetadata,
        session: SessionRecordingMetadata,
    ) -> Result<Self, Mfr1TransformErrorV2> {
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
            || !session.initial_subscriptions.is_empty()
        {
            return Err(Mfr1TransformErrorV2::InvalidExecutionMetadata);
        }
        Ok(Self { build, session })
    }
}

#[derive(Debug, Clone)]
pub struct Mfr1TransformContextV2 {
    pub(crate) admission: ProspectiveCaptureAdmissionV2,
    pub(crate) catalog: ReplayCatalogV1,
    pub(crate) session: Mfr1SessionBindingV2,
    pub(crate) metadata: Mfr1MetadataBindingV2,
    pub(crate) system_source: SystemSourceV1,
    pub(crate) action_capacity: usize,
    pub(crate) dispatch_capacity: usize,
    pub(crate) overflow: OverflowPolicy,
}

impl Mfr1TransformContextV2 {
    pub fn new(
        admission: ProspectiveCaptureAdmissionV2,
        catalog: ReplayCatalogV1,
        session: Mfr1SessionBindingV2,
        metadata: Mfr1MetadataBindingV2,
        system_source: SystemSourceV1,
        dispatch_capacity: usize,
        overflow: OverflowPolicy,
    ) -> Result<Self, Mfr1TransformErrorV2> {
        let action_capacity = dispatch_capacity
            .checked_mul(4)
            .map(|value| value.max(marketfeed_adapter_api::DEFAULT_ACTION_BUFFER_CAPACITY))
            .filter(|value| *value <= MAX_ACTIONS_PER_FRAME)
            .ok_or(Mfr1TransformErrorV2::InvalidExecutionMetadata)?;
        if dispatch_capacity == 0
            || dispatch_capacity > u16::MAX as usize
            || !matches!(
                overflow,
                OverflowPolicy::DropNewest | OverflowPolicy::FailEngine
            )
        {
            return Err(Mfr1TransformErrorV2::InvalidExecutionMetadata);
        }
        catalog
            .validate()
            .map_err(|_| Mfr1TransformErrorV2::CatalogMismatch)?;
        let expected_source = match session.route {
            BinanceMfr1RouteV2::Public => "binance_primary_public",
            BinanceMfr1RouteV2::Market => "binance_primary_market",
        };
        let expected_endpoint = match session.route {
            BinanceMfr1RouteV2::Public => "wss://fstream.binance.com/public/ws",
            BinanceMfr1RouteV2::Market => "wss://fstream.binance.com/market/ws",
        };
        let expected_epoch = match session.route {
            BinanceMfr1RouteV2::Public => "epoch_public",
            BinanceMfr1RouteV2::Market => "epoch_market",
        };
        let config = admission.mechanics_config();
        let contributor = config
            .contributors()
            .iter()
            .find(|spec| spec.key().source_id() == expected_source);
        let expected_instrument = contributor.map(|spec| spec.key().instrument());
        let metadata_instrument = metadata
            .session
            .catalog
            .iter()
            .filter(|row| row.instrument_id == 7)
            .collect::<Vec<_>>();
        let metadata_ids = metadata
            .session
            .catalog
            .iter()
            .map(|row| row.instrument_id)
            .collect::<BTreeSet<_>>();
        if !config.connections().contains(&session.connection)
            || contributor.is_none()
            || contributor.and_then(|spec| config.contributor_connections().get(spec.key()))
                != Some(&session.connection)
            || metadata.session.session_id != session.session_id
            || metadata.session.venue_id != 3
            || metadata.session.adapter != "binance-usdm"
            || metadata.session.environment != "public"
            || metadata.session.endpoint != expected_endpoint
            || metadata.session.catalog_version != 1
            || metadata_ids.len() != metadata.session.catalog.len()
            || metadata
                .session
                .initial_subscriptions
                .iter()
                .any(|subscription| !metadata_ids.contains(&subscription.instrument_id))
            || metadata_instrument.len() != 1
            || metadata_instrument.first().is_none_or(|row| {
                row.native_symbol != "BNBUSDT"
                    || row.kind != "PerpetualLinear"
                    || row.base != "BNB"
                    || row.quote != "USDT"
                    || row.settlement.as_deref() != Some("USDT")
                    || row.price_scale != 2
                    || row.quantity_scale != 3
                    || row.price_increment.coefficient != "1"
                    || row.price_increment.scale != 2
                    || row.quantity_increment.coefficient != "1"
                    || row.quantity_increment.scale != 3
                    || row.min_quantity.is_some()
                    || row.max_quantity.is_some()
                    || row.min_notional.is_some()
                    || row.contract_size.is_some()
                    || row.expiry_ns.is_some()
                    || row.status != "Active"
                    || row.inverse
            })
            || catalog
                .connection_epochs()
                .iter()
                .filter(|epoch| {
                    epoch.connection_id() == session.connection_id
                        && epoch.session_id() == session.session_id
                        && epoch.connection_epoch() == expected_epoch
                        && epoch.epoch_generation() == 0
                })
                .count()
                != 1
            || catalog.venue_source(3).is_none_or(|venue| {
                venue.source_id() != expected_source || venue.venue() != "BINANCE"
            })
            || catalog.instrument(7) != expected_instrument
            || !config.system_sources().contains(system_source.key())
            || system_source.epoch() != "epoch_system_0"
            || system_source.epoch_generation() != 0
        {
            return Err(Mfr1TransformErrorV2::TopologyMismatch);
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
pub struct Mfr1MechanicsFrameV2 {
    frame_seq: u64,
    available_at: TimestampNs,
    inputs: Vec<MechanicsInputV2>,
}

impl Mfr1MechanicsFrameV2 {
    pub const fn frame_seq(&self) -> u64 {
        self.frame_seq
    }
    pub const fn available_at(&self) -> TimestampNs {
        self.available_at
    }
    pub fn inputs(&self) -> &[MechanicsInputV2] {
        &self.inputs
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mfr1TransformOutputV2 {
    frames: Vec<Mfr1MechanicsFrameV2>,
    inputs: Vec<MechanicsInputV2>,
    jsonl: Vec<u8>,
    frames_applied: u64,
    dropped_action_buffer: u64,
    dropped_market_dispatch: u64,
}

impl Mfr1TransformOutputV2 {
    pub fn frames(&self) -> &[Mfr1MechanicsFrameV2] {
        &self.frames
    }
    pub fn inputs(&self) -> &[MechanicsInputV2] {
        &self.inputs
    }
    pub fn canonical_jsonl(&self) -> &[u8] {
        &self.jsonl
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
pub enum Mfr1TransformErrorV2 {
    #[error("MFR1 recording is invalid: {0}")]
    Recording(String),
    #[error("session replay failed: {0}")]
    Adapter(String),
    #[error("strict EventPulse V2 input is invalid: {0}")]
    Wire(String),
    #[error("canonical V2 JSONL failed: {0}")]
    Jsonl(String),
    #[error("immutable replay execution metadata is invalid")]
    InvalidExecutionMetadata,
    #[error("record does not match the immutable replay catalog")]
    CatalogMismatch,
    #[error("record does not match the checked EventPulse topology")]
    TopologyMismatch,
    #[error("selected session metadata is missing or conflicts")]
    MetadataMismatch,
    #[error("MFR1 framing, timestamp, coordinate, or aggregate capacity failed")]
    Capacity,
    #[error("MFR1 availability, monotonic time, or mechanics frame regressed")]
    Order,
    #[error("routed Binance payload role, provenance, or native coordinate is invalid")]
    Provenance,
    #[error("routed Binance provenance is duplicate, ambiguous, missing, or unconsumed")]
    ProvenanceLedger,
    #[error("ordinary system or reconnect action is unsupported")]
    UnsupportedAction,
    #[error("owned Binance routed-v4 machine is not pristine or does not match the context")]
    MachineIdentity,
}

pub struct Mfr1TransformerV2 {
    context: Mfr1TransformContextV2,
}

impl Mfr1TransformerV2 {
    pub fn new(context: Mfr1TransformContextV2) -> Self {
        Self { context }
    }

    pub fn transform(
        self,
        mut machine: BinanceUsdmSession,
        mfr1_bytes: &[u8],
        connect_at: TimestampNs,
        not_after: Rfc3339Time,
    ) -> Result<Mfr1TransformOutputV2, Mfr1TransformErrorV2> {
        let identity = machine
            .pristine_routed_v4_identity()
            .map_err(|_| Mfr1TransformErrorV2::MachineIdentity)?;
        let expected_route = match self.context.session.route {
            BinanceMfr1RouteV2::Public => BinanceUsdmRouteV4::Public,
            BinanceMfr1RouteV2::Market => BinanceUsdmRouteV4::Market,
        };
        if identity.route() != expected_route
            || identity.connection().0 != self.context.session.connection_id
            || identity.session().0 != self.context.session.session_id
            || identity.instrument().0 != 7
            || identity.symbol() != "BNBUSDT"
        {
            return Err(Mfr1TransformErrorV2::MachineIdentity);
        }
        let (records, mut ledger) = self.prevalidate(mfr1_bytes, connect_at, &not_after)?;
        let mut state = TransformStateV2::new(&self.context, connect_at)?;
        state.apply(
            &self.context,
            &mut machine,
            0,
            connect_at,
            None,
            |machine, actions| machine.on_replay_start(connect_at, actions),
            &mut ledger,
        )?;
        for prepared in records {
            if prepared.record.header.session.0 != self.context.session.session_id
                || prepared.record.header.direction != Direction::Inbound
            {
                continue;
            }
            let frame_seq = prepared.record.header.frame_seq;
            let available_at = TimestampNs(prepared.record.header.receive_ts_ns);
            let stamp = FrameStamp {
                receive_ts: available_at,
                mono_ns: prepared.record.header.monotonic_ns,
            };
            let mut payload = prepared.record.payload;
            match prepared.record.header.opcode {
                FrameOpcode::Text | FrameOpcode::Binary | FrameOpcode::Pong => {
                    let opcode = prepared.record.header.opcode;
                    state.apply(
                        &self.context,
                        &mut machine,
                        frame_seq,
                        available_at,
                        prepared.ledger_key.as_ref(),
                        |machine, actions| match opcode {
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
                        &mut ledger,
                    )?;
                    state.bump_frames()?;
                }
                FrameOpcode::HttpResponse => {
                    let (request_id, response) =
                        decode_http_response(&payload).map_err(recording)?;
                    state.apply(
                        &self.context,
                        &mut machine,
                        frame_seq,
                        available_at,
                        prepared.ledger_key.as_ref(),
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
                        &mut ledger,
                    )?;
                    state.bump_frames()?;
                }
                FrameOpcode::SubscriptionCommand => {
                    let (command, recorded_wire) =
                        decode_subscription_command(&payload).map_err(recording)?;
                    state.apply(
                        &self.context,
                        &mut machine,
                        frame_seq,
                        available_at,
                        None,
                        |machine, _| {
                            let prepared = machine.prepare_dynamic_subscription(&command)?;
                            if prepared != recorded_wire {
                                return Err(marketfeed_adapter_api::AdapterError::Parse(
                                    "recorded subscription wire action mismatch".into(),
                                ));
                            }
                            machine.commit_dynamic_subscription(&command);
                            Ok(())
                        },
                        &mut ledger,
                    )?;
                    state.bump_frames()?;
                }
                FrameOpcode::Metadata => {}
                FrameOpcode::Ping | FrameOpcode::Close => {}
            }
        }
        if !ledger.is_empty() {
            return Err(Mfr1TransformErrorV2::ProvenanceLedger);
        }
        let jsonl = canonical_jsonl(&state.inputs, not_after)?;
        Ok(Mfr1TransformOutputV2 {
            frames: state.frames,
            inputs: state.inputs,
            jsonl,
            frames_applied: state.frames_applied,
            dropped_action_buffer: state.dropped_action_buffer,
            dropped_market_dispatch: state.dropped_market_dispatch,
        })
    }

    fn prevalidate(
        &self,
        bytes: &[u8],
        connect_at: TimestampNs,
        not_after: &Rfc3339Time,
    ) -> Result<(Vec<PreparedRecord>, BTreeMap<LedgerKey, SourceProvenanceV2>), Mfr1TransformErrorV2>
    {
        ensure_at_most(bytes.len(), MAX_MFR1_BYTES)?;
        if bytes.len() < HEADER_SIZE {
            return Err(Mfr1TransformErrorV2::Capacity);
        }
        let mut reader = RawSegmentReader::open(Cursor::new(bytes)).map_err(recording)?;
        if reader.format_version != FORMAT_VERSION || reader.start_ts_ns != connect_at.0 {
            return Err(Mfr1TransformErrorV2::Recording(
                "format version or start coordinate mismatch".into(),
            ));
        }
        let reserved = bytes
            .get(14..22)
            .and_then(|value| <[u8; 8]>::try_from(value).ok())
            .ok_or(Mfr1TransformErrorV2::Capacity)?;
        if u64::from_le_bytes(reserved) != 0 {
            return Err(Mfr1TransformErrorV2::Recording(
                "reserved MFR1 header word is nonzero".into(),
            ));
        }
        let start = self.context.admission.capture_starts_at().utc_micros();
        if connect_at.0.div_euclid(1_000) < start
            || connect_at.0.div_euclid(1_000) > not_after.utc_micros()
            || not_after < self.context.admission.capture_starts_at()
        {
            return Err(Mfr1TransformErrorV2::Order);
        }
        let mut consumed = HEADER_SIZE;
        let mut records = Vec::new();
        let mut ledger = BTreeMap::new();
        let mut last_available = connect_at;
        let mut last_mono = None;
        let mut build = None;
        let mut session_metadata = None;
        let mut last_market_frame = None;
        while let Some(record) = reader.read_record().map_err(recording)? {
            if records.len() == MAX_REPLAY_RECORDS {
                return Err(Mfr1TransformErrorV2::Capacity);
            }
            consumed = consumed
                .checked_add(
                    usize::try_from(record.header.record_len)
                        .map_err(|_| Mfr1TransformErrorV2::Capacity)?,
                )
                .ok_or(Mfr1TransformErrorV2::Capacity)?;
            let mut key = None;
            if record.header.session.0 == self.context.session.session_id {
                let available = TimestampNs(record.header.receive_ts_ns);
                if available < last_available
                    || available.0.div_euclid(1_000) < start
                    || available.0.div_euclid(1_000) > not_after.utc_micros()
                    || last_mono.is_some_and(|previous| record.header.monotonic_ns < previous)
                {
                    return Err(Mfr1TransformErrorV2::Order);
                }
                last_available = available;
                last_mono = Some(record.header.monotonic_ns);
                if record.header.opcode == FrameOpcode::Metadata {
                    match decode_metadata(&record.payload).map_err(recording)? {
                        MetadataRecord::Session(value)
                            if record.header.direction == Direction::Inbound
                                && session_metadata.is_none() =>
                        {
                            session_metadata = Some(value);
                        }
                        _ => return Err(Mfr1TransformErrorV2::MetadataMismatch),
                    }
                } else if record.header.direction == Direction::Inbound {
                    key = decode_record_provenance(&self.context, &record)?;
                    if key.is_some()
                        && (record.header.frame_seq == 0
                            || last_market_frame
                                .is_some_and(|previous| record.header.frame_seq <= previous))
                    {
                        return Err(Mfr1TransformErrorV2::Order);
                    }
                    if key.is_some() {
                        last_market_frame = Some(record.header.frame_seq);
                    }
                    if let Some((ledger_key, provenance)) =
                        key.as_ref().map(|value| (value.0.clone(), value.1.clone()))
                    {
                        if ledger.insert(ledger_key, provenance).is_some() {
                            return Err(Mfr1TransformErrorV2::ProvenanceLedger);
                        }
                    }
                }
            } else if record.header.opcode == FrameOpcode::Metadata {
                match decode_metadata(&record.payload).map_err(recording)? {
                    MetadataRecord::Build(value) => {
                        if record.header.session.0 != 0
                            || record.header.direction != Direction::Inbound
                            || record.header.receive_ts_ns != reader.start_ts_ns
                            || value.schema_version != 1
                            || build.replace(value).is_some()
                        {
                            return Err(Mfr1TransformErrorV2::MetadataMismatch);
                        }
                    }
                    MetadataRecord::Session(value)
                        if value.session_id == self.context.session.session_id =>
                    {
                        return Err(Mfr1TransformErrorV2::MetadataMismatch);
                    }
                    MetadataRecord::Session(_) => {}
                }
            }
            records.push(PreparedRecord {
                record,
                ledger_key: key.map(|value| value.0),
            });
        }
        if consumed != bytes.len()
            || build.as_ref() != Some(&self.context.metadata.build)
            || session_metadata.as_ref() != Some(&self.context.metadata.session)
        {
            return Err(Mfr1TransformErrorV2::MetadataMismatch);
        }
        Ok((records, ledger))
    }
}

fn recording(error: impl std::fmt::Display) -> Mfr1TransformErrorV2 {
    Mfr1TransformErrorV2::Recording(error.to_string())
}

#[derive(Debug)]
struct PreparedRecord {
    record: RawRecord,
    ledger_key: Option<LedgerKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum LedgerKey {
    Direct(u64),
    BookDelta(u64, u64, u64),
}

fn checked_ms(value: Option<i64>) -> Result<u64, Mfr1TransformErrorV2> {
    value
        .and_then(|value| u64::try_from(value).ok())
        .filter(|value| *value <= 9_223_372_036_854)
        .ok_or(Mfr1TransformErrorV2::Provenance)
}

fn decode_record_provenance(
    context: &Mfr1TransformContextV2,
    record: &RawRecord,
) -> Result<Option<(LedgerKey, SourceProvenanceV2)>, Mfr1TransformErrorV2> {
    let decoded = match record.header.opcode {
        FrameOpcode::Text => decode_usdm_routed_v4_text(&record.payload)
            .map_err(|_| Mfr1TransformErrorV2::Provenance)?,
        FrameOpcode::HttpResponse => {
            let (_, response) = decode_http_response(&record.payload).map_err(recording)?;
            decode_usdm_routed_v4_text(&response.body)
                .map_err(|_| Mfr1TransformErrorV2::Provenance)?
        }
        FrameOpcode::Binary => return Err(Mfr1TransformErrorV2::Provenance),
        _ => return Ok(None),
    };
    let result = provenance_for(
        context.session.route,
        context.session.session_id,
        record.header.frame_seq,
        decoded,
    )?;
    if let Some((_, provenance)) = &result {
        validate_provenance_time(context, record, provenance)?;
    }
    Ok(result)
}

fn validate_provenance_time(
    context: &Mfr1TransformContextV2,
    record: &RawRecord,
    provenance: &SourceProvenanceV2,
) -> Result<(), Mfr1TransformErrorV2> {
    let start_ms = u64::try_from(
        context
            .admission
            .capture_starts_at()
            .utc_micros()
            .div_euclid(1_000),
    )
    .map_err(|_| Mfr1TransformErrorV2::Provenance)?;
    let available_ms = u64::try_from(record.header.receive_ts_ns.div_euclid(1_000_000))
        .map_err(|_| Mfr1TransformErrorV2::Provenance)?;
    let causal_time = match provenance {
        SourceProvenanceV2::None => None,
        SourceProvenanceV2::BinanceBookTicker {
            transaction_time_ms,
            ..
        }
        | SourceProvenanceV2::BinanceBookDelta {
            transaction_time_ms,
            ..
        }
        | SourceProvenanceV2::BinanceBookSnapshot {
            transaction_time_ms,
            ..
        } => Some(*transaction_time_ms),
        SourceProvenanceV2::BinanceAggregateTrade { trade_time_ms, .. } => Some(*trade_time_ms),
        SourceProvenanceV2::BinanceOpenInterest { source_time_ms } => Some(*source_time_ms),
        SourceProvenanceV2::BinanceForceOrder {
            order_trade_time_ms,
            ..
        } => Some(*order_trade_time_ms),
    };
    if causal_time.is_some_and(|value| value < start_ms || value > available_ms) {
        return Err(Mfr1TransformErrorV2::Provenance);
    }
    Ok(())
}

fn provenance_for(
    route: BinanceMfr1RouteV2,
    session_id: u64,
    frame: u64,
    value: UsdmRoutedV4Decoded,
) -> Result<Option<(LedgerKey, SourceProvenanceV2)>, Mfr1TransformErrorV2> {
    let event = checked_ms(value.source_times.event_time_ms);
    let transaction = checked_ms(value.source_times.transaction_time_ms);
    let direct = LedgerKey::Direct(frame);
    match (route, value.decoded) {
        (
            BinanceMfr1RouteV2::Public,
            UsdmDecoded::Quote {
                symbol, update_id, ..
            },
        ) if symbol == "BNBUSDT" => Ok(Some((
            direct,
            SourceProvenanceV2::BinanceBookTicker {
                update_id,
                event_time_ms: event?,
                transaction_time_ms: transaction?,
            },
        ))),
        (
            BinanceMfr1RouteV2::Public,
            UsdmDecoded::DepthUpdate {
                symbol,
                first_update_id,
                final_update_id,
                prev_final_update_id,
                ..
            },
        ) if symbol == "BNBUSDT"
            && first_update_id <= final_update_id
            && final_update_id <= i64::MAX as u64
            && prev_final_update_id <= i64::MAX as u64 =>
        {
            Ok(Some((
                LedgerKey::BookDelta(session_id, first_update_id, final_update_id),
                SourceProvenanceV2::BinanceBookDelta {
                    first_update_id,
                    final_update_id,
                    previous_final_update_id: prev_final_update_id,
                    event_time_ms: event?,
                    transaction_time_ms: transaction?,
                },
            )))
        }
        (BinanceMfr1RouteV2::Public, UsdmDecoded::DepthSnapshot { last_update_id, .. })
            if last_update_id <= i64::MAX as u64 =>
        {
            Ok(Some((
                direct,
                SourceProvenanceV2::BinanceBookSnapshot {
                    last_update_id,
                    event_time_ms: event?,
                    transaction_time_ms: transaction?,
                },
            )))
        }
        (BinanceMfr1RouteV2::Market, UsdmDecoded::AggTrade { symbol, agg_id, .. })
            if symbol == "BNBUSDT" && agg_id <= i64::MAX as u64 =>
        {
            Ok(Some((
                direct,
                SourceProvenanceV2::BinanceAggregateTrade {
                    aggregate_trade_id: agg_id,
                    event_time_ms: event?,
                    trade_time_ms: transaction?,
                },
            )))
        }
        (
            BinanceMfr1RouteV2::Market,
            UsdmDecoded::OpenInterest {
                symbol,
                exchange_ts_ms,
                ..
            },
        ) if symbol == "BNBUSDT" => Ok(Some((
            direct,
            SourceProvenanceV2::BinanceOpenInterest {
                source_time_ms: checked_ms(Some(exchange_ts_ms))?,
            },
        ))),
        (BinanceMfr1RouteV2::Market, UsdmDecoded::ForceOrder { symbol, .. })
            if symbol == "BNBUSDT" =>
        {
            Ok(Some((
                direct,
                SourceProvenanceV2::BinanceForceOrder {
                    event_time_ms: event?,
                    order_trade_time_ms: transaction?,
                },
            )))
        }
        (_, UsdmDecoded::SubscribeAck { id: Some(1) }) => Ok(None),
        _ => Err(Mfr1TransformErrorV2::Provenance),
    }
}

struct TransformStateV2 {
    actions: ActionBuffer,
    dispatch: EventDispatcher,
    frames: Vec<Mfr1MechanicsFrameV2>,
    inputs: Vec<MechanicsInputV2>,
    frames_applied: u64,
    last_mechanics_frame: Option<u64>,
    system_chain_head: Option<String>,
    dropped_action_buffer: u64,
    dropped_market_dispatch: u64,
}

impl TransformStateV2 {
    fn new(
        context: &Mfr1TransformContextV2,
        _connect_at: TimestampNs,
    ) -> Result<Self, Mfr1TransformErrorV2> {
        Ok(Self {
            actions: ActionBuffer::with_capacity(MAX_ACTIONS_PER_FRAME),
            dispatch: EventDispatcher::new(
                context.dispatch_capacity,
                context.dispatch_capacity,
                context.overflow,
            ),
            frames: Vec::new(),
            inputs: Vec::new(),
            frames_applied: 0,
            last_mechanics_frame: None,
            system_chain_head: None,
            dropped_action_buffer: 0,
            dropped_market_dispatch: 0,
        })
    }

    fn bump_frames(&mut self) -> Result<(), Mfr1TransformErrorV2> {
        self.frames_applied = self
            .frames_applied
            .checked_add(1)
            .ok_or(Mfr1TransformErrorV2::Capacity)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn apply<F>(
        &mut self,
        context: &Mfr1TransformContextV2,
        machine: &mut BinanceUsdmSession,
        frame_seq: u64,
        available_at: TimestampNs,
        direct_key: Option<&LedgerKey>,
        apply: F,
        ledger: &mut BTreeMap<LedgerKey, SourceProvenanceV2>,
    ) -> Result<(), Mfr1TransformErrorV2>
    where
        F: FnOnce(
            &mut BinanceUsdmSession,
            &mut ActionBuffer,
        ) -> Result<(), marketfeed_adapter_api::AdapterError>,
    {
        self.actions.clear();
        let _ = self.actions.take_dropped();
        apply(machine, &mut self.actions)
            .map_err(|error| Mfr1TransformErrorV2::Adapter(error.to_string()))?;
        if self.actions.take_dropped() != 0 {
            return Err(Mfr1TransformErrorV2::Capacity);
        }
        let observed: Vec<_> = self.actions.drain().collect();
        if observed.iter().any(|action| {
            matches!(
                action,
                SessionAction::EmitSystem(_) | SessionAction::Reconnect(_)
            )
        }) {
            return Err(Mfr1TransformErrorV2::UnsupportedAction);
        }
        if observed.iter().any(
            |action| matches!(action, SessionAction::EmitBatch(batch) if batch.events.is_empty()),
        ) {
            return Err(Mfr1TransformErrorV2::Provenance);
        }
        let dropped_actions = observed.len().saturating_sub(context.action_capacity);
        for (action_index, action) in observed.iter().enumerate().skip(context.action_capacity) {
            if let SessionAction::EmitBatch(batch) = action {
                consume_batch_provenance(batch, direct_key, action_index, ledger)?;
            }
        }
        let retained: Vec<_> = observed.into_iter().take(context.action_capacity).collect();
        let mut accepted = Vec::new();
        let mut market_drops = 0u64;
        for (action_index, action) in retained.into_iter().enumerate() {
            if let SessionAction::EmitBatch(batch) = action {
                match self
                    .dispatch
                    .push_batch(batch.clone())
                    .map_err(|error| Mfr1TransformErrorV2::Adapter(error.to_string()))?
                {
                    PushOutcome::Accepted => accepted.push((action_index, batch)),
                    PushOutcome::DroppedNewest => {
                        consume_batch_provenance(&batch, direct_key, action_index, ledger)?;
                        market_drops = market_drops
                            .checked_add(1)
                            .ok_or(Mfr1TransformErrorV2::Capacity)?
                    }
                    PushOutcome::DroppedOldest { .. } => {
                        return Err(Mfr1TransformErrorV2::InvalidExecutionMetadata);
                    }
                }
            }
        }
        let _ = self.dispatch.drain_batches();
        let _ = self.dispatch.drain_systems();
        if accepted.is_empty() && dropped_actions == 0 && market_drops == 0 {
            return Ok(());
        }
        if frame_seq == 0 {
            return Err(Mfr1TransformErrorV2::Order);
        }
        if self
            .last_mechanics_frame
            .is_some_and(|previous| frame_seq <= previous)
        {
            return Err(Mfr1TransformErrorV2::Order);
        }
        self.last_mechanics_frame = Some(frame_seq);
        let mut frame_inputs = Vec::new();
        for (action_index, batch) in accepted {
            if batch.session.0 != context.session.session_id {
                return Err(Mfr1TransformErrorV2::CatalogMismatch);
            }
            for (item_index, mut envelope) in batch.events.into_iter().enumerate() {
                let action_index =
                    u32::try_from(action_index).map_err(|_| Mfr1TransformErrorV2::Capacity)?;
                envelope.frame_seq = frame_seq;
                envelope.event_index =
                    u16::try_from(item_index).map_err(|_| Mfr1TransformErrorV2::Capacity)?;
                envelope.receive_ts = available_at;
                let (key, cursor) = market_key_cursor(&envelope, direct_key, action_index)?;
                let provenance = ledger
                    .remove(&key)
                    .ok_or(Mfr1TransformErrorV2::ProvenanceLedger)?;
                let input = MechanicsInputV2::market(
                    envelope,
                    action_index,
                    context.catalog.clone(),
                    cursor,
                    provenance,
                )
                .map_err(|error| Mfr1TransformErrorV2::Wire(error.to_string()))?;
                if self
                    .inputs
                    .len()
                    .checked_add(frame_inputs.len())
                    .and_then(|count| count.checked_add(1))
                    .is_none_or(|count| count > MAX_AUTHORED_INPUTS)
                {
                    return Err(Mfr1TransformErrorV2::Capacity);
                }
                frame_inputs.push(input);
            }
        }
        for (category, item, count) in [
            (
                DropCategoryV1::ActionBuffer,
                0,
                u64::try_from(dropped_actions).map_err(|_| Mfr1TransformErrorV2::Capacity)?,
            ),
            (DropCategoryV1::MarketDispatch, 1, market_drops),
        ] {
            if count == 0 {
                continue;
            }
            frame_inputs.push(self.drop_input(
                context,
                frame_seq,
                item,
                available_at,
                count,
                category,
            )?);
        }
        self.dropped_action_buffer = self
            .dropped_action_buffer
            .checked_add(
                u64::try_from(dropped_actions).map_err(|_| Mfr1TransformErrorV2::Capacity)?,
            )
            .ok_or(Mfr1TransformErrorV2::Capacity)?;
        self.dropped_market_dispatch = self
            .dropped_market_dispatch
            .checked_add(market_drops)
            .ok_or(Mfr1TransformErrorV2::Capacity)?;
        if self
            .inputs
            .len()
            .checked_add(frame_inputs.len())
            .is_none_or(|count| count > MAX_AUTHORED_INPUTS)
        {
            return Err(Mfr1TransformErrorV2::Capacity);
        }
        self.inputs.extend(frame_inputs.iter().cloned());
        self.frames.push(Mfr1MechanicsFrameV2 {
            frame_seq,
            available_at,
            inputs: frame_inputs,
        });
        Ok(())
    }

    fn drop_input(
        &mut self,
        context: &Mfr1TransformContextV2,
        frame_seq: u64,
        item_index: u32,
        available_at: TimestampNs,
        count: u64,
        category: DropCategoryV1,
    ) -> Result<MechanicsInputV2, Mfr1TransformErrorV2> {
        let time = Rfc3339Time::from_unix_nanos(available_at.0)
            .map_err(|error| Mfr1TransformErrorV2::Wire(error.to_string()))?;
        let v1 = MechanicsInputV1::system(
            context.system_source.clone(),
            FaultScopeV1::processor(context.admission.mechanics_config().processor_id())
                .map_err(|error| Mfr1TransformErrorV2::Wire(error.to_string()))?,
            time.clone(),
            time,
            CursorV1::derived_drop(frame_seq, item_index)
                .map_err(|error| Mfr1TransformErrorV2::Wire(error.to_string()))?,
            SystemFaultV1::events_dropped(count, category)
                .map_err(|error| Mfr1TransformErrorV2::Wire(error.to_string()))?,
            self.system_chain_head.clone(),
        )
        .map_err(|error| Mfr1TransformErrorV2::Wire(error.to_string()))?;
        self.system_chain_head = Some(
            match self.system_chain_head.as_deref() {
                Some(previous) => SystemChainPreimage::hash_next(previous, v1.payload_hash()),
                None => SystemChainPreimage::hash_first(v1.payload_hash()),
            }
            .map_err(|error| Mfr1TransformErrorV2::Wire(error.to_string()))?,
        );
        MechanicsInputV2::from_v1_non_market(v1)
            .map_err(|error| Mfr1TransformErrorV2::Wire(error.to_string()))
    }
}

fn consume_batch_provenance(
    batch: &marketfeed_adapter_api::EventBatch,
    direct_key: Option<&LedgerKey>,
    action_index: usize,
    ledger: &mut BTreeMap<LedgerKey, SourceProvenanceV2>,
) -> Result<(), Mfr1TransformErrorV2> {
    let action_index = u32::try_from(action_index).map_err(|_| Mfr1TransformErrorV2::Capacity)?;
    for (item_index, envelope) in batch.events.iter().enumerate() {
        let mut envelope = envelope.clone();
        envelope.event_index =
            u16::try_from(item_index).map_err(|_| Mfr1TransformErrorV2::Capacity)?;
        let (key, _) = market_key_cursor(&envelope, direct_key, action_index)?;
        ledger
            .remove(&key)
            .ok_or(Mfr1TransformErrorV2::ProvenanceLedger)?;
    }
    Ok(())
}

fn market_key_cursor(
    envelope: &EventEnvelope,
    direct_key: Option<&LedgerKey>,
    action_index: u32,
) -> Result<(LedgerKey, MarketCursorV2), Mfr1TransformErrorV2> {
    match &envelope.payload {
        MarketEvent::BookDelta(_) => {
            let sequence = envelope
                .source_sequence
                .ok_or(Mfr1TransformErrorV2::Provenance)?;
            Ok((
                LedgerKey::BookDelta(envelope.session.0, sequence.first, sequence.last),
                MarketCursorV2::Native {
                    first_sequence: sequence.first,
                    last_sequence: sequence.last,
                },
            ))
        }
        MarketEvent::BookSnapshot(_) | MarketEvent::Trade(_) => {
            let key = direct_key
                .cloned()
                .ok_or(Mfr1TransformErrorV2::ProvenanceLedger)?;
            let sequence = envelope
                .source_sequence
                .ok_or(Mfr1TransformErrorV2::Provenance)?;
            Ok((
                key,
                MarketCursorV2::Native {
                    first_sequence: sequence.first,
                    last_sequence: sequence.last,
                },
            ))
        }
        MarketEvent::Quote(_) | MarketEvent::OpenInterest(_) | MarketEvent::Liquidation(_) => {
            let key = direct_key
                .cloned()
                .ok_or(Mfr1TransformErrorV2::ProvenanceLedger)?;
            Ok((
                key,
                MarketCursorV2::Derived {
                    raw_frame_seq: envelope.frame_seq,
                    action_index,
                    item_index: u32::from(envelope.event_index),
                },
            ))
        }
        _ => Err(Mfr1TransformErrorV2::Provenance),
    }
}

#[derive(Default)]
struct BoundedJsonl {
    bytes: Vec<u8>,
}

impl Write for BoundedJsonl {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(buffer.len())
            .is_none_or(|size| size > MAX_JSONL_BYTES)
        {
            return Err(io::Error::other("canonical V2 JSONL aggregate capacity"));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn canonical_jsonl(
    inputs: &[MechanicsInputV2],
    not_after: Rfc3339Time,
) -> Result<Vec<u8>, Mfr1TransformErrorV2> {
    let mut writer = MechanicsInputV2JsonlWriter::new(BoundedJsonl::default());
    for input in inputs {
        writer
            .write_input(input)
            .map_err(|error| Mfr1TransformErrorV2::Jsonl(error.to_string()))?;
    }
    let bytes = writer.finish().bytes;
    let decoded = MechanicsInputV2JsonlReader::new(bytes.as_slice(), not_after)
        .read_all()
        .map_err(|error| Mfr1TransformErrorV2::Jsonl(error.to_string()))?;
    if decoded != inputs {
        return Err(Mfr1TransformErrorV2::Jsonl(
            "strict readback differs".into(),
        ));
    }
    Ok(bytes)
}

#[cfg(test)]
mod capacity_tests {
    use super::*;

    #[test]
    fn aggregate_caps_accept_exact_boundary_and_reject_one_over() {
        for limit in [
            MAX_MFR1_BYTES,
            MAX_REPLAY_RECORDS,
            MAX_AUTHORED_INPUTS,
            MAX_ACTIONS_PER_FRAME,
            MAX_JSONL_BYTES,
        ] {
            assert_eq!(ensure_at_most(limit, limit), Ok(()));
            assert_eq!(
                ensure_at_most(limit + 1, limit),
                Err(Mfr1TransformErrorV2::Capacity)
            );
        }

        let mut exact = BoundedJsonl::default();
        assert_eq!(
            exact.write(&vec![0; MAX_JSONL_BYTES]).unwrap(),
            MAX_JSONL_BYTES
        );
        assert!(exact.write(&[0]).is_err());
    }
}
