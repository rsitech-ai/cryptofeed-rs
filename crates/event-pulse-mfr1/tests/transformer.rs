use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, EventBatch, HttpResponse, ReconnectReason, SessionAction,
    SessionCommand, SessionInput, SessionMachine, SubscriptionWireAction,
};
use marketfeed_event_pulse::{
    EpinJson1Reader, ProspectiveCaptureAdmissionV1, SourceStateMachine,
    wire::{
        ConnectionKeyV1, CursorV1, DropCategoryV1, InstrumentIdentityV1, MechanicsInputRefV1,
        MechanicsInputRefV1 as InputRef, ReplayCatalogV1, ReplayEpochEntryV1, Rfc3339Time,
        SystemFaultRefV1, SystemSourceV1, VenueCatalogEntryV1,
    },
};
use marketfeed_event_pulse_mfr1::{
    Mfr1MetadataBindingV1, Mfr1SessionBindingV1, Mfr1TransformContextV1, Mfr1TransformError,
    Mfr1TransformerV1,
};
use marketfeed_model::{
    AggressorSide, ConnectionId, EventEnvelope, EventFlags, Fixed, InstrumentId, MarketEvent,
    OverflowPolicy, Price, Quantity, SessionId, SystemEvent, TimestampNs, Trade, VenueId,
};
use marketfeed_recording::{
    CatalogInstrumentMetadata, Direction, FixedMetadata, FrameOpcode, MetadataRecord,
    RawSegmentWriter, SessionRecordingMetadata, encode_http_response, encode_metadata,
    encode_subscription_command,
};
use marketfeed_replay::ReplayRunner;
use serde_json::{Value, json};

fn topology() -> (
    ProspectiveCaptureAdmissionV1,
    ReplayCatalogV1,
    ConnectionKeyV1,
    SystemSourceV1,
) {
    let admission = admission();
    let config = admission.mechanics_config();
    let connection = config
        .connections()
        .iter()
        .find(|connection| connection.source_id() == "binance_primary_connection")
        .unwrap()
        .clone();
    let catalog = ReplayCatalogV1::new(
        BTreeMap::from([(
            1,
            VenueCatalogEntryV1::new("BINANCE", "binance_primary").unwrap(),
        )]),
        BTreeMap::from([(
            1,
            InstrumentIdentityV1::new("BTC", "USDT", "PERPETUAL", "BINANCE", "BTCUSDT").unwrap(),
        )]),
        vec![ReplayEpochEntryV1::new(1, 1, "epoch_capture_0", 0).unwrap()],
        BTreeMap::new(),
    )
    .unwrap();
    let system =
        SystemSourceV1::new(config.system_sources()[0].clone(), "epoch_system_0", 0).unwrap();
    (admission, catalog, connection, system)
}

fn sha(byte: char, len: usize) -> String {
    std::iter::repeat_n(byte, len).collect()
}

fn source_binding(source_id: &str, venue: &str, blob: char, roles: &[&str]) -> Value {
    json!({
        "source_id": source_id,
        "connection_id": format!("{source_id}_connection"),
        "format": "MFR1",
        "instrument": {
            "base_asset": "BTC", "quote_asset": "USDT", "market_type": "PERPETUAL",
            "venue": venue, "venue_symbol": if venue == "BINANCE" { "BTCUSDT" } else { "BTC" }
        },
        "roles": roles,
        "families": if venue == "HYPERLIQUID" { json!(["CONFIRMATION_PRICE"]) } else {
            json!(["TRADE", "QUOTE", "BOOK", "OPEN_INTEREST", "LIQUIDATION"])
        },
        "public_read_only": true,
        "repository_url": "https://github.com/rsitech-ai/cryptofeed-rs",
        "producer_commit": sha('a', 40),
        "producer_path": format!("crates/event-pulse-capture/src/{source_id}.rs"),
        "producer_blob_sha256": sha(blob, 64)
    })
}

fn local_binding(source_id: &str, subject: &str, family: Option<&str>, blob: char) -> Value {
    let mut value = json!({
        "source_id": source_id,
        "subject_source_id": subject,
        "evidence_kind": if family.is_some() { "EXPLICIT_HEARTBEAT_RANGE" } else { "UTC_MONOTONIC_OBSERVATION" },
        "derivation": "INDEPENDENT_SIDECAR",
        "producer_commit": sha('d', 40),
        "producer_path": format!("crates/event-pulse-capture/src/{source_id}.rs"),
        "producer_blob_sha256": sha(blob, 64)
    });
    if let Some(family) = family {
        value["family"] = json!(family);
    }
    value
}

fn admission() -> ProspectiveCaptureAdmissionV1 {
    let value = json!({
        "schema": "event-pulse-e2-prospective-admission/1.0",
        "root_amendment_commit": "24b51a58c670ab722538bec4a3e1def0278b1107",
        "root_default_reachable_at": "2026-08-22T07:35:52Z",
        "capture_starts_at": "2026-08-22T07:35:52.000001Z",
        "evidence_claim": "PROSPECTIVE_CAUSAL_CAPTURE",
        "source_qualification": "UNVERIFIED",
        "required_roles": ["TRADE", "QUOTE", "BOOK", "OPEN_INTEREST", "LIQUIDATION", "CONFIRMATION", "CLOCK", "COVERAGE", "SYSTEM"],
        "primary": source_binding("binance_primary", "BINANCE", 'b', &["TRADE", "QUOTE", "BOOK", "OPEN_INTEREST", "LIQUIDATION"]),
        "confirmation": source_binding("hyperliquid_confirmation", "HYPERLIQUID", 'c', &["CONFIRMATION"]),
        "clocks": [
            local_binding("primary_clock", "binance_primary", None, 'e'),
            local_binding("confirmation_clock", "hyperliquid_confirmation", None, 'f')
        ],
        "coverage": [
            local_binding("primary_trade_coverage", "binance_primary", Some("TRADE"), '1'),
            local_binding("primary_quote_coverage", "binance_primary", Some("QUOTE"), '2'),
            local_binding("primary_book_coverage", "binance_primary", Some("BOOK"), '3'),
            local_binding("primary_oi_coverage", "binance_primary", Some("OPEN_INTEREST"), '4'),
            local_binding("primary_liq_coverage", "binance_primary", Some("LIQUIDATION"), '5'),
            local_binding("confirmation_price_coverage", "hyperliquid_confirmation", Some("CONFIRMATION_PRICE"), '6')
        ],
        "system": {
            "source_id": "capture_system", "processor_id": "event_pulse_e2_prospective",
            "target": "PROCESSOR", "fault_scope": "PROCESSOR", "cursor_mode": "DERIVED",
            "evidence_kind": "STABLE_SYSTEM_FAULT_MAPPING", "producer_commit": sha('7', 40),
            "producer_path": "crates/event-pulse-capture/src/system.rs", "producer_blob_sha256": sha('a', 64)
        },
        "authority": { "credentials_allowed": false, "private_endpoints_allowed": false, "orders_allowed": false,
            "execution_authority": false, "paper_authority": false, "promotion_authority": false }
    });
    ProspectiveCaptureAdmissionV1::from_json(&serde_json::to_vec(&value).unwrap()).unwrap()
}

fn mfr1(records: &[(u64, i64, FrameOpcode, &[u8])]) -> Vec<u8> {
    mfr1_with_start(records[0].1, records)
}

fn mfr1_with_start(start_ns: i64, records: &[(u64, i64, FrameOpcode, &[u8])]) -> Vec<u8> {
    let mut writer = RawSegmentWriter::create(Vec::new(), start_ns).unwrap();
    writer
        .write_metadata(&MetadataRecord::Build(build_metadata()), start_ns)
        .unwrap();
    writer
        .write_metadata(&session_metadata(), start_ns)
        .unwrap();
    for (frame_seq, receive_ns, opcode, payload) in records {
        writer
            .write_record(
                SessionId(1),
                *frame_seq,
                *receive_ns,
                u64::try_from(*receive_ns).unwrap(),
                Direction::Inbound,
                *opcode,
                0,
                payload,
            )
            .unwrap();
    }
    writer.into_inner()
}

fn empty_mfr1(start_ns: i64) -> Vec<u8> {
    let mut writer = RawSegmentWriter::create(Vec::new(), start_ns).unwrap();
    writer
        .write_metadata(&MetadataRecord::Build(build_metadata()), start_ns)
        .unwrap();
    writer
        .write_metadata(&session_metadata(), start_ns)
        .unwrap();
    writer.into_inner()
}

fn session_metadata() -> MetadataRecord {
    MetadataRecord::Session(session_recording_metadata())
}

fn build_metadata() -> marketfeed_recording::BuildMetadata {
    let MetadataRecord::Build(build) = MetadataRecord::current_build() else {
        unreachable!();
    };
    build
}

fn session_recording_metadata() -> SessionRecordingMetadata {
    SessionRecordingMetadata {
        schema_version: 1,
        session_id: 1,
        venue_id: 1,
        adapter: "test-market".into(),
        environment: "public".into(),
        endpoint: "offline".into(),
        catalog_version: 1,
        catalog: vec![CatalogInstrumentMetadata {
            instrument_id: 1,
            native_symbol: "BTCUSDT".into(),
            kind: "Perpetual".into(),
            base: "BTC".into(),
            quote: "USDT".into(),
            settlement: Some("USDT".into()),
            price_scale: 2,
            quantity_scale: 2,
            price_increment: FixedMetadata {
                coefficient: "1".into(),
                scale: 2,
            },
            quantity_increment: FixedMetadata {
                coefficient: "1".into(),
                scale: 2,
            },
            min_quantity: None,
            max_quantity: None,
            min_notional: None,
            contract_size: None,
            expiry_ns: None,
            status: "Trading".into(),
            inverse: false,
        }],
        initial_subscriptions: vec![],
    }
}

fn context(dispatch_capacity: usize, overflow: OverflowPolicy) -> (Mfr1TransformerV1, i64) {
    let (admission, catalog, connection, system) = topology();
    let start_ns = admission.capture_starts_at().utc_micros() * 1_000;
    let context = Mfr1TransformContextV1::new(
        admission,
        catalog,
        Mfr1SessionBindingV1::new(connection, 1, 1),
        system,
        Mfr1MetadataBindingV1::new(build_metadata(), session_recording_metadata()).unwrap(),
        dispatch_capacity,
        overflow,
    )
    .unwrap();
    (Mfr1TransformerV1::new(context), start_ns)
}

fn decision(ns: i64) -> Rfc3339Time {
    Rfc3339Time::from_unix_nanos(ns).unwrap()
}

#[derive(Default)]
struct MarketMachine {
    subscribed: bool,
}

fn trade_event(receive: TimestampNs, event_index: u16) -> EventEnvelope {
    EventEnvelope {
        schema_version: 1,
        venue: VenueId(1),
        instrument: Some(InstrumentId(1)),
        connection: ConnectionId(1),
        session: SessionId(1),
        frame_seq: 999,
        event_index,
        exchange_ts: Some(receive),
        receive_ts: TimestampNs(receive.0 + 777),
        source_sequence: None,
        flags: EventFlags::empty(),
        payload: MarketEvent::Trade(Trade {
            price: Price(Fixed::new(10_000, 2)),
            quantity: Quantity(Fixed::new(100, 2)),
            aggressor: AggressorSide::Buy,
            trade_id: None,
        }),
    }
}

impl SessionMachine for MarketMachine {
    fn on_replay_start(
        &mut self,
        _now: TimestampNs,
        _output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        Ok(())
    }

    fn prepare_dynamic_subscription(
        &self,
        command: &SessionCommand,
    ) -> Result<SubscriptionWireAction, AdapterError> {
        if matches!(command, SessionCommand::Subscribe(_)) {
            Ok(SubscriptionWireAction::Text(
                b"SUB BTC-USD\n".as_slice().to_vec().into(),
            ))
        } else {
            Err(AdapterError::UnsupportedCapability("test command".into()))
        }
    }

    fn commit_dynamic_subscription(&mut self, command: &SessionCommand) {
        self.subscribed = matches!(command, SessionCommand::Subscribe(_));
    }

    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if let SessionInput::TextFrame { received, .. } = input {
            if !self.subscribed {
                return Err(AdapterError::Parse("market before subscription".into()));
            }
            output.push(SessionAction::EmitBatch(EventBatch {
                session: SessionId(1),
                frame_seq: 999,
                events: vec![trade_event(received.receive_ts, 9)],
            }));
        }
        Ok(())
    }
}

struct BurstMachine {
    actions: usize,
    items: usize,
    replay_start: bool,
}

impl SessionMachine for BurstMachine {
    fn on_replay_start(
        &mut self,
        now: TimestampNs,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if self.replay_start {
            self.emit(now, output);
        }
        Ok(())
    }

    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if let SessionInput::TextFrame { received, .. } = input {
            self.emit(received.receive_ts, output);
        }
        Ok(())
    }
}

impl BurstMachine {
    fn emit(&self, receive: TimestampNs, output: &mut ActionBuffer) {
        for _ in 0..self.actions {
            output.push(SessionAction::EmitBatch(EventBatch {
                session: SessionId(1),
                frame_seq: 777,
                events: (0..self.items).map(|_| trade_event(receive, 0)).collect(),
            }));
        }
    }
}

enum UnsupportedKind {
    System,
    Reconnect,
}

struct UnsupportedMachine(UnsupportedKind);

impl SessionMachine for UnsupportedMachine {
    fn on_replay_start(
        &mut self,
        _now: TimestampNs,
        _output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        Ok(())
    }

    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if matches!(input, SessionInput::TextFrame { .. }) {
            match self.0 {
                UnsupportedKind::System => {
                    output.push(SessionAction::EmitSystem(SystemEvent::SequenceGap {
                        expected: 1,
                        actual: 3,
                    }))
                }
                UnsupportedKind::Reconnect => {
                    output.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
                }
            }
        }
        Ok(())
    }
}

struct OverflowHiddenUnsupportedMachine(UnsupportedKind);

impl SessionMachine for OverflowHiddenUnsupportedMachine {
    fn on_replay_start(
        &mut self,
        _now: TimestampNs,
        _output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        Ok(())
    }

    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if matches!(input, SessionInput::TextFrame { .. }) {
            output.push(SessionAction::SendText(b"benign".to_vec().into()));
            match self.0 {
                UnsupportedKind::System => {
                    output.push(SessionAction::EmitSystem(SystemEvent::SequenceGap {
                        expected: 1,
                        actual: 3,
                    }));
                }
                UnsupportedKind::Reconnect => {
                    output.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
                }
            }
        }
        Ok(())
    }
}

struct CountingMachine(Arc<AtomicUsize>);

impl SessionMachine for CountingMachine {
    fn on_replay_start(
        &mut self,
        _now: TimestampNs,
        _output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn on_input(
        &mut self,
        _input: SessionInput<'_>,
        _output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct PostStartFailureMachine {
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    fail: bool,
}

impl Drop for PostStartFailureMachine {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl SessionMachine for PostStartFailureMachine {
    fn on_replay_start(
        &mut self,
        _now: TimestampNs,
        _output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        _output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if matches!(input, SessionInput::TextFrame { .. }) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(AdapterError::Parse("intentional post-start failure".into()));
            }
        }
        Ok(())
    }
}

struct HttpMarketMachine;

impl SessionMachine for HttpMarketMachine {
    fn on_replay_start(
        &mut self,
        _now: TimestampNs,
        _output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        Ok(())
    }

    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if let SessionInput::HttpResponse {
            request_id,
            response,
            received,
        } = input
        {
            if request_id != 77 || response.status != 206 || response.body.as_ref() != b"market" {
                return Err(AdapterError::Parse("unexpected HTTP replay".into()));
            }
            output.push(SessionAction::EmitBatch(EventBatch {
                session: SessionId(1),
                frame_seq: 900,
                events: vec![trade_event(received.receive_ts, 4)],
            }));
        }
        Ok(())
    }
}

#[test]
fn authentic_mfr1_market_is_strictly_normalized_to_raw_coordinates_and_epin() {
    let (admission, catalog, connection, system) = topology();
    let start_ns = admission.capture_starts_at().utc_micros() * 1_000;
    let context = Mfr1TransformContextV1::new(
        admission,
        catalog,
        Mfr1SessionBindingV1::new(connection, 1, 1),
        system,
        Mfr1MetadataBindingV1::new(build_metadata(), session_recording_metadata()).unwrap(),
        8,
        OverflowPolicy::FailEngine,
    )
    .unwrap();
    let transformer = Mfr1TransformerV1::new(context);
    let command = SessionCommand::Subscribe(vec!["BTC-USD".into()]);
    let wire = SubscriptionWireAction::Text(b"SUB BTC-USD\n".as_slice().to_vec().into());
    let control = encode_subscription_command(&command, &wire).unwrap();
    let bytes = mfr1(&[
        (
            1,
            start_ns,
            FrameOpcode::SubscriptionCommand,
            control.as_slice(),
        ),
        (
            2,
            start_ns + 1_000,
            FrameOpcode::Text,
            b"TRADE 1 100.00 1.000 BUY",
        ),
    ]);
    let decision = Rfc3339Time::from_unix_nanos(start_ns + 2_000).unwrap();

    let output = transformer
        .transform(
            MarketMachine::default(),
            &bytes,
            TimestampNs(start_ns),
            decision,
        )
        .unwrap();

    assert_eq!(output.frames_applied(), 2);
    assert_eq!(output.inputs().len(), 1);
    let MechanicsInputRefV1::Market {
        envelope,
        action_index,
        ..
    } = output.inputs()[0].view()
    else {
        panic!("expected market input");
    };
    assert_eq!(envelope.frame_seq, 2);
    assert_eq!(envelope.event_index, 0);
    assert_eq!(envelope.receive_ts, TimestampNs(start_ns + 1_000));
    assert_eq!(action_index, 0);

    let decoded = EpinJson1Reader::new(
        output.epin_json1(),
        Rfc3339Time::from_unix_nanos(start_ns + 2_000).unwrap(),
    )
    .read_all()
    .unwrap();
    assert_eq!(decoded, output.inputs());
    assert!(!output.evidence_authoring_allowed());
    assert_eq!(output.blocker(), "blocked:fixture-provenance");
}

#[test]
fn metadata_is_validated_and_http_response_uses_the_authoritative_raw_group() {
    let (transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
    let response = encode_http_response(
        77,
        &HttpResponse {
            status: 206,
            headers: vec![("content-type".into(), "application/octet-stream".into())],
            body: b"market".as_slice().to_vec().into(),
        },
    )
    .unwrap();
    let bytes = mfr1_with_start(
        start_ns,
        &[(19, start_ns + 1_000, FrameOpcode::HttpResponse, &response)],
    );
    let output = transformer
        .transform(
            HttpMarketMachine,
            &bytes,
            TimestampNs(start_ns),
            decision(start_ns + 1_000),
        )
        .unwrap();
    assert_eq!(output.frames_applied(), 1);
    assert_eq!(output.frames()[0].frame_seq(), 19);
    let InputRef::Market { envelope, .. } = output.inputs()[0].view() else {
        panic!("expected HTTP-derived market");
    };
    assert_eq!(envelope.frame_seq, 19);
    assert_eq!(envelope.event_index, 0);
    assert_eq!(envelope.receive_ts, TimestampNs(start_ns + 1_000));
}

#[test]
fn multi_item_batches_use_raw_frame_and_checked_item_coordinates() {
    let (transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
    let bytes = mfr1(&[(5, start_ns, FrameOpcode::Text, b"batch")]);
    let output = transformer
        .transform(
            BurstMachine {
                actions: 1,
                items: 2,
                replay_start: false,
            },
            &bytes,
            TimestampNs(start_ns),
            decision(start_ns + 1_000),
        )
        .unwrap();
    assert_eq!(output.frames()[0].frame_seq(), 5);
    assert_eq!(output.frames()[0].available_at(), TimestampNs(start_ns));
    assert_eq!(output.inputs().len(), 2);
    for (index, input) in output.inputs().iter().enumerate() {
        let InputRef::Market {
            envelope,
            action_index,
            ..
        } = input.view()
        else {
            panic!("expected market");
        };
        assert_eq!(envelope.frame_seq, 5);
        assert_eq!(usize::from(envelope.event_index), index);
        assert_eq!(action_index, 0);
        assert_eq!(envelope.receive_ts, TimestampNs(start_ns));
    }
    assert_ne!(
        output.inputs()[0].payload_hash(),
        output.inputs()[1].payload_hash()
    );
}

#[test]
fn real_action_and_market_dispatch_losses_are_reserved_and_chained_after_market() {
    let (transformer, start_ns) = context(1, OverflowPolicy::DropNewest);
    let bytes = mfr1(&[(8, start_ns, FrameOpcode::Text, b"burst")]);
    let output = transformer
        .transform(
            BurstMachine {
                actions: 1_025,
                items: 1,
                replay_start: false,
            },
            &bytes,
            TimestampNs(start_ns),
            decision(start_ns + 1_000),
        )
        .unwrap();
    assert_eq!(output.dropped_action_buffer(), 1);
    assert_eq!(output.dropped_market_dispatch(), 1_023);
    assert_eq!(output.inputs().len(), 1_026);
    for (offset, (item, category)) in [
        (0, DropCategoryV1::ActionBuffer),
        (1, DropCategoryV1::MarketDispatch),
    ]
    .into_iter()
    .enumerate()
    {
        let InputRef::System {
            system_cursor,
            fault,
            predecessor_system_chain_hash,
            ..
        } = output.inputs()[1_024 + offset].view()
        else {
            panic!("expected reserved drop");
        };
        assert_eq!(system_cursor, &CursorV1::derived_drop(8, item).unwrap());
        assert_eq!(
            fault.view(),
            SystemFaultRefV1::EventsDropped {
                count: if category == DropCategoryV1::ActionBuffer {
                    1
                } else {
                    1_023
                },
                category,
            }
        );
        assert_eq!(predecessor_system_chain_hash.is_some(), offset == 1);
    }
    let decoded = EpinJson1Reader::new(output.epin_json1(), decision(start_ns + 1_000))
        .read_all()
        .unwrap();
    assert_eq!(decoded, output.inputs());
}

#[test]
fn replay_start_owns_coordinate_zero_and_inbound_zero_fails_closed() {
    let (transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
    let startup = transformer
        .transform(
            BurstMachine {
                actions: 1,
                items: 1,
                replay_start: true,
            },
            &empty_mfr1(start_ns),
            TimestampNs(start_ns),
            decision(start_ns),
        )
        .unwrap();
    assert_eq!(startup.frames()[0].frame_seq(), 0);
    let InputRef::Market { envelope, .. } = startup.inputs()[0].view() else {
        panic!("expected startup market");
    };
    assert_eq!(envelope.frame_seq, 0);

    let (transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
    let inbound_zero = mfr1(&[(0, start_ns, FrameOpcode::Text, b"market")]);
    assert_eq!(
        transformer.transform(
            BurstMachine {
                actions: 1,
                items: 1,
                replay_start: false,
            },
            &inbound_zero,
            TimestampNs(start_ns),
            decision(start_ns),
        ),
        Err(Mfr1TransformError::MechanicsFrameZero)
    );
}

#[test]
fn empty_subscription_controls_may_use_zero_and_reuse_market_frame() {
    let (transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
    let command = SessionCommand::Subscribe(vec!["BTC-USD".into()]);
    let wire = SubscriptionWireAction::Text(b"SUB BTC-USD\n".as_slice().to_vec().into());
    let control = encode_subscription_command(&command, &wire).unwrap();
    let bytes = mfr1(&[
        (0, start_ns, FrameOpcode::SubscriptionCommand, &control),
        (5, start_ns + 1_000, FrameOpcode::Text, b"market"),
        (
            5,
            start_ns + 2_000,
            FrameOpcode::SubscriptionCommand,
            &control,
        ),
        (6, start_ns + 3_000, FrameOpcode::Text, b"market"),
    ]);
    let output = transformer
        .transform(
            MarketMachine::default(),
            &bytes,
            TimestampNs(start_ns),
            decision(start_ns + 4_000),
        )
        .unwrap();
    assert_eq!(output.frames_applied(), 4);
    assert_eq!(
        output
            .frames()
            .iter()
            .map(|frame| frame.frame_seq())
            .collect::<Vec<_>>(),
        vec![5, 6]
    );
}

#[test]
fn ordinary_system_and_reconnect_actions_fail_without_partial_output() {
    for (kind, expected) in [
        (
            UnsupportedKind::System,
            Mfr1TransformError::UnsupportedSystemAction,
        ),
        (
            UnsupportedKind::Reconnect,
            Mfr1TransformError::UnsupportedReconnect,
        ),
    ] {
        let (transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
        let bytes = mfr1(&[(7, start_ns, FrameOpcode::Text, b"fault")]);
        assert_eq!(
            transformer.transform(
                UnsupportedMachine(kind),
                &bytes,
                TimestampNs(start_ns),
                decision(start_ns),
            ),
            Err(expected)
        );
    }
}

#[test]
fn action_overflow_cannot_hide_unsupported_system_or_reconnect_actions() {
    for (kind, expected) in [
        (
            UnsupportedKind::System,
            Mfr1TransformError::UnsupportedSystemAction,
        ),
        (
            UnsupportedKind::Reconnect,
            Mfr1TransformError::UnsupportedReconnect,
        ),
    ] {
        let (transformer, start_ns) = context(8, OverflowPolicy::DropNewest);
        let bytes = mfr1(&[(7, start_ns, FrameOpcode::Text, b"fault")]);
        assert_eq!(
            transformer.transform(
                OverflowHiddenUnsupportedMachine(kind),
                &bytes,
                TimestampNs(start_ns),
                decision(start_ns),
            ),
            Err(expected)
        );
    }
}

#[test]
fn complete_selected_session_metadata_is_required_before_machine_mutation() {
    let (transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
    let mut writer = RawSegmentWriter::create(Vec::new(), start_ns).unwrap();
    writer
        .write_metadata(&MetadataRecord::Build(build_metadata()), start_ns)
        .unwrap();
    let bytes = writer.into_inner();
    let calls = Arc::new(AtomicUsize::new(0));
    let result = transformer.transform(
        CountingMachine(Arc::clone(&calls)),
        &bytes,
        TimestampNs(start_ns),
        decision(start_ns),
    );
    assert!(result.is_err(), "missing selected metadata must fail");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn selected_session_metadata_must_match_session_venue_catalog_and_topology() {
    let (transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
    let MetadataRecord::Session(mut wrong) = session_metadata() else {
        unreachable!();
    };
    wrong.venue_id = 2;
    let payload = encode_metadata(&MetadataRecord::Session(wrong)).unwrap();
    let mut writer = RawSegmentWriter::create(Vec::new(), start_ns).unwrap();
    writer
        .write_metadata(&MetadataRecord::Build(build_metadata()), start_ns)
        .unwrap();
    writer
        .write_record(
            SessionId(1),
            0,
            start_ns,
            0,
            Direction::Inbound,
            FrameOpcode::Metadata,
            0,
            &payload,
        )
        .unwrap();
    assert_eq!(
        transformer.transform(
            BurstMachine {
                actions: 0,
                items: 0,
                replay_start: false,
            },
            &writer.into_inner(),
            TimestampNs(start_ns),
            decision(start_ns),
        ),
        Err(Mfr1TransformError::SessionMetadataMismatch)
    );

    let (transformer, _) = context(8, OverflowPolicy::FailEngine);
    let MetadataRecord::Session(mut wrong) = session_metadata() else {
        unreachable!();
    };
    wrong.session_id = 2;
    let payload = encode_metadata(&MetadataRecord::Session(wrong)).unwrap();
    let mut writer = RawSegmentWriter::create(Vec::new(), start_ns).unwrap();
    writer
        .write_metadata(&MetadataRecord::Build(build_metadata()), start_ns)
        .unwrap();
    writer
        .write_record(
            SessionId(1),
            0,
            start_ns,
            0,
            Direction::Inbound,
            FrameOpcode::Metadata,
            0,
            &payload,
        )
        .unwrap();
    assert_eq!(
        transformer.transform(
            BurstMachine {
                actions: 0,
                items: 0,
                replay_start: false,
            },
            &writer.into_inner(),
            TimestampNs(start_ns),
            decision(start_ns),
        ),
        Err(Mfr1TransformError::SessionMetadataMismatch)
    );

    let (transformer, _) = context(8, OverflowPolicy::FailEngine);
    let mut writer = RawSegmentWriter::create(Vec::new(), start_ns).unwrap();
    writer
        .write_metadata(&MetadataRecord::Build(build_metadata()), start_ns)
        .unwrap();
    writer
        .write_metadata(&session_metadata(), start_ns)
        .unwrap();
    writer
        .write_metadata(&session_metadata(), start_ns + 1_000)
        .unwrap();
    assert_eq!(
        transformer.transform(
            BurstMachine {
                actions: 0,
                items: 0,
                replay_start: false,
            },
            &writer.into_inner(),
            TimestampNs(start_ns),
            decision(start_ns + 1_000),
        ),
        Err(Mfr1TransformError::SessionMetadataMismatch)
    );
}

#[test]
fn build_and_every_selected_decoding_metadata_field_are_exactly_bound() {
    let (transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
    let mut wrong_build = build_metadata();
    wrong_build.package_version.push_str("-tampered");
    let mut writer = RawSegmentWriter::create(Vec::new(), start_ns).unwrap();
    writer
        .write_metadata(&MetadataRecord::Build(wrong_build), start_ns)
        .unwrap();
    writer
        .write_metadata(&session_metadata(), start_ns)
        .unwrap();
    assert_eq!(
        transformer.transform(
            CountingMachine(Arc::new(AtomicUsize::new(0))),
            &writer.into_inner(),
            TimestampNs(start_ns),
            decision(start_ns),
        ),
        Err(Mfr1TransformError::BuildMetadataMismatch)
    );

    let (transformer, _) = context(8, OverflowPolicy::FailEngine);
    let mut wrong_session = session_recording_metadata();
    wrong_session.catalog[0].price_scale += 1;
    let mut writer = RawSegmentWriter::create(Vec::new(), start_ns).unwrap();
    writer
        .write_metadata(&MetadataRecord::Build(build_metadata()), start_ns)
        .unwrap();
    writer
        .write_metadata(&MetadataRecord::Session(wrong_session), start_ns)
        .unwrap();
    assert_eq!(
        transformer.transform(
            CountingMachine(Arc::new(AtomicUsize::new(0))),
            &writer.into_inner(),
            TimestampNs(start_ns),
            decision(start_ns),
        ),
        Err(Mfr1TransformError::SessionMetadataMismatch)
    );
}

#[test]
fn exact_unrelated_catalog_rows_are_retained_but_not_required_in_topology() {
    let (admission, _, connection, system) = topology();
    let start_ns = admission.capture_starts_at().utc_micros() * 1_000;
    let catalog = ReplayCatalogV1::new(
        BTreeMap::from([(
            1,
            VenueCatalogEntryV1::new("BINANCE", "binance_primary").unwrap(),
        )]),
        BTreeMap::from([
            (
                1,
                InstrumentIdentityV1::new("BTC", "USDT", "PERPETUAL", "BINANCE", "BTCUSDT")
                    .unwrap(),
            ),
            (
                2,
                InstrumentIdentityV1::new("ETH", "USDT", "PERPETUAL", "BINANCE", "ETHUSDT")
                    .unwrap(),
            ),
        ]),
        vec![ReplayEpochEntryV1::new(1, 1, "epoch_capture_0", 0).unwrap()],
        BTreeMap::new(),
    )
    .unwrap();
    let mut expected_session = session_recording_metadata();
    let mut unrelated = expected_session.catalog[0].clone();
    unrelated.instrument_id = 2;
    unrelated.native_symbol = "ETHUSDT".into();
    unrelated.base = "ETH".into();
    expected_session.catalog.push(unrelated);
    for offset in 0..33u32 {
        let mut unrelated = expected_session.catalog[0].clone();
        unrelated.instrument_id = 100 + offset;
        unrelated.native_symbol = format!("UNRELATED{offset}");
        unrelated.base = "ETH".into();
        expected_session.catalog.push(unrelated);
    }
    let context = Mfr1TransformContextV1::new(
        admission,
        catalog,
        Mfr1SessionBindingV1::new(connection, 1, 1),
        system,
        Mfr1MetadataBindingV1::new(build_metadata(), expected_session.clone()).unwrap(),
        8,
        OverflowPolicy::FailEngine,
    )
    .unwrap();
    let mut writer = RawSegmentWriter::create(Vec::new(), start_ns).unwrap();
    writer
        .write_metadata(&MetadataRecord::Build(build_metadata()), start_ns)
        .unwrap();
    writer
        .write_metadata(&MetadataRecord::Session(expected_session), start_ns)
        .unwrap();
    writer
        .write_record(
            SessionId(1),
            1,
            start_ns,
            1,
            Direction::Inbound,
            FrameOpcode::Text,
            0,
            b"market",
        )
        .unwrap();
    let output = Mfr1TransformerV1::new(context)
        .transform(
            BurstMachine {
                actions: 1,
                items: 1,
                replay_start: false,
            },
            &writer.into_inner(),
            TimestampNs(start_ns),
            decision(start_ns),
        )
        .unwrap();
    assert_eq!(output.inputs().len(), 1);
}

#[test]
fn selected_event_pulse_catalog_row_cannot_be_missing_or_semantically_mismatched() {
    for mutation in ["missing", "mismatched"] {
        let (admission, catalog, connection, system) = topology();
        let start_ns = admission.capture_starts_at().utc_micros() * 1_000;
        let mut expected_session = session_recording_metadata();
        if mutation == "missing" {
            let mut unrelated = expected_session.catalog[0].clone();
            unrelated.instrument_id = 100;
            unrelated.native_symbol = "UNRELATED".into();
            expected_session.catalog = vec![unrelated];
        } else {
            expected_session.catalog[0].native_symbol = "ETHUSDT".into();
        }
        let context = Mfr1TransformContextV1::new(
            admission,
            catalog,
            Mfr1SessionBindingV1::new(connection, 1, 1),
            system,
            Mfr1MetadataBindingV1::new(build_metadata(), expected_session.clone()).unwrap(),
            8,
            OverflowPolicy::FailEngine,
        )
        .unwrap();
        let mut writer = RawSegmentWriter::create(Vec::new(), start_ns).unwrap();
        writer
            .write_metadata(&MetadataRecord::Build(build_metadata()), start_ns)
            .unwrap();
        writer
            .write_metadata(&MetadataRecord::Session(expected_session), start_ns)
            .unwrap();
        assert_eq!(
            Mfr1TransformerV1::new(context).transform(
                BurstMachine {
                    actions: 0,
                    items: 0,
                    replay_start: false,
                },
                &writer.into_inner(),
                TimestampNs(start_ns),
                decision(start_ns),
            ),
            Err(Mfr1TransformError::SessionMetadataMismatch),
            "{mutation} selected row must reject"
        );
    }
}

#[test]
fn full_mfr_validation_finishes_before_replay_start() {
    let (transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
    let mut bytes = empty_mfr1(start_ns);
    bytes.push(0xa5);
    let calls = Arc::new(AtomicUsize::new(0));
    assert!(matches!(
        transformer.transform(
            CountingMachine(Arc::clone(&calls)),
            &bytes,
            TimestampNs(start_ns),
            decision(start_ns),
        ),
        Err(Mfr1TransformError::Recording(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn post_start_error_consumes_failed_machine_and_retry_requires_a_fresh_machine() {
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let (transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
    let bytes = mfr1(&[(7, start_ns, FrameOpcode::Text, b"fail-after-start")]);
    let failed = transformer.transform_boxed(
        Box::new(PostStartFailureMachine {
            calls: Arc::clone(&calls),
            drops: Arc::clone(&drops),
            fail: true,
        }),
        &bytes,
        TimestampNs(start_ns),
        decision(start_ns),
    );
    assert!(matches!(failed, Err(Mfr1TransformError::Adapter(_))));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    let (transformer, _) = context(8, OverflowPolicy::FailEngine);
    transformer
        .transform_boxed(
            Box::new(PostStartFailureMachine {
                calls: Arc::clone(&calls),
                drops: Arc::clone(&drops),
                fail: false,
            }),
            &bytes,
            TimestampNs(start_ns),
            decision(start_ns),
        )
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

#[test]
fn canonical_factory_boxed_machine_is_consumed_and_runs() {
    let (transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
    let machine: Box<dyn SessionMachine> = Box::new(BurstMachine {
        actions: 1,
        items: 1,
        replay_start: false,
    });
    let output = transformer
        .transform_boxed(
            machine,
            &mfr1(&[(7, start_ns, FrameOpcode::Text, b"boxed")]),
            TimestampNs(start_ns),
            decision(start_ns),
        )
        .unwrap();
    assert_eq!(output.inputs().len(), 1);
}

#[test]
fn selected_segment_requires_build_metadata_before_machine_start() {
    let (transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
    let mut writer = RawSegmentWriter::create(Vec::new(), start_ns).unwrap();
    writer
        .write_metadata(&session_metadata(), start_ns)
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let result = transformer.transform(
        CountingMachine(Arc::clone(&calls)),
        &writer.into_inner(),
        TimestampNs(start_ns),
        decision(start_ns),
    );
    assert!(result.is_err(), "missing build metadata must fail");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn dispatcher_capacity_is_scoped_to_each_transport_frame() {
    let (transformer, start_ns) = context(1, OverflowPolicy::DropNewest);
    let bytes = mfr1(&[
        (7, start_ns, FrameOpcode::Text, b"first"),
        (8, start_ns + 1_000, FrameOpcode::Text, b"second"),
    ]);
    let output = transformer
        .transform(
            BurstMachine {
                actions: 1,
                items: 1,
                replay_start: false,
            },
            &bytes,
            TimestampNs(start_ns),
            decision(start_ns + 1_000),
        )
        .unwrap();
    assert_eq!(output.dropped_market_dispatch(), 0);
    assert_eq!(output.inputs().len(), 2);
}

#[test]
fn unsupported_overflow_policies_are_rejected_at_context_construction() {
    for overflow in [
        OverflowPolicy::BlockWithDeadline,
        OverflowPolicy::DropOldest,
        OverflowPolicy::LatestPerKey,
        OverflowPolicy::SpillToDisk,
        OverflowPolicy::DisableSink,
    ] {
        let (admission, catalog, connection, system) = topology();
        assert!(
            Mfr1TransformContextV1::new(
                admission,
                catalog,
                Mfr1SessionBindingV1::new(connection, 1, 1),
                system,
                Mfr1MetadataBindingV1::new(build_metadata(), session_recording_metadata()).unwrap(),
                8,
                overflow,
            )
            .is_err()
        );
    }
}

#[test]
fn raw_record_count_accepts_exact_bound_and_rejects_one_over() {
    fn recording(start_ns: i64, ping_count: usize) -> Vec<u8> {
        let mut writer = RawSegmentWriter::create(Vec::new(), start_ns).unwrap();
        writer
            .write_metadata(&MetadataRecord::Build(build_metadata()), start_ns)
            .unwrap();
        writer
            .write_metadata(&session_metadata(), start_ns)
            .unwrap();
        for _ in 0..ping_count {
            writer
                .write_record(
                    SessionId(1),
                    0,
                    start_ns,
                    0,
                    Direction::Inbound,
                    FrameOpcode::Ping,
                    0,
                    b"",
                )
                .unwrap();
        }
        writer.into_inner()
    }

    let (transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
    let exact = transformer
        .transform(
            BurstMachine {
                actions: 0,
                items: 0,
                replay_start: false,
            },
            &recording(start_ns, 65_534),
            TimestampNs(start_ns),
            decision(start_ns),
        )
        .unwrap();
    assert!(exact.inputs().is_empty());

    let (transformer, _) = context(8, OverflowPolicy::FailEngine);
    assert_eq!(
        transformer.transform(
            BurstMachine {
                actions: 0,
                items: 0,
                replay_start: false,
            },
            &recording(start_ns, 65_535),
            TimestampNs(start_ns),
            decision(start_ns),
        ),
        Err(Mfr1TransformError::RawRecordCapacity)
    );
}

#[test]
fn authored_input_count_fails_at_the_public_one_over_boundary() {
    let (transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
    let bytes = mfr1(&[(7, start_ns, FrameOpcode::Text, b"large-batch")]);
    assert_eq!(
        transformer.transform(
            BurstMachine {
                actions: 2,
                items: 32_769,
                replay_start: false,
            },
            &bytes,
            TimestampNs(start_ns),
            decision(start_ns),
        ),
        Err(Mfr1TransformError::InputCapacity)
    );
}

#[test]
fn selected_session_and_admitted_time_window_are_enforced() {
    let (transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
    let mut writer = RawSegmentWriter::create(Vec::new(), start_ns).unwrap();
    writer
        .write_metadata(&MetadataRecord::Build(build_metadata()), start_ns)
        .unwrap();
    writer
        .write_metadata(&session_metadata(), start_ns)
        .unwrap();
    writer
        .write_record(
            SessionId(2),
            1,
            start_ns - 10_000,
            1,
            Direction::Inbound,
            FrameOpcode::Text,
            0,
            b"ignored-other-session",
        )
        .unwrap();
    writer
        .write_record(
            SessionId(1),
            2,
            start_ns,
            2,
            Direction::Inbound,
            FrameOpcode::Text,
            0,
            b"selected",
        )
        .unwrap();
    let output = transformer
        .transform(
            BurstMachine {
                actions: 1,
                items: 1,
                replay_start: false,
            },
            &writer.into_inner(),
            TimestampNs(start_ns),
            decision(start_ns),
        )
        .unwrap();
    assert_eq!(output.frames_applied(), 1);
    assert_eq!(output.frames()[0].frame_seq(), 2);

    let (transformer, _) = context(8, OverflowPolicy::FailEngine);
    let mut selected_outbound = RawSegmentWriter::create(Vec::new(), start_ns).unwrap();
    selected_outbound
        .write_metadata(&MetadataRecord::Build(build_metadata()), start_ns)
        .unwrap();
    selected_outbound
        .write_metadata(&session_metadata(), start_ns)
        .unwrap();
    selected_outbound
        .write_record(
            SessionId(1),
            1,
            start_ns - 1_000,
            1,
            Direction::Outbound,
            FrameOpcode::Text,
            0,
            b"selected-outbound",
        )
        .unwrap();
    assert_eq!(
        transformer.transform(
            BurstMachine {
                actions: 1,
                items: 1,
                replay_start: false,
            },
            &selected_outbound.into_inner(),
            TimestampNs(start_ns),
            decision(start_ns),
        ),
        Err(Mfr1TransformError::OutsideAdmissionWindow)
    );

    for (record_ns, not_after) in [
        (start_ns - 1_000, decision(start_ns)),
        (start_ns + 2_000, decision(start_ns + 1_000)),
    ] {
        let (transformer, _) = context(8, OverflowPolicy::FailEngine);
        let bytes = mfr1_with_start(start_ns, &[(1, record_ns, FrameOpcode::Text, b"market")]);
        assert_eq!(
            transformer.transform(
                BurstMachine {
                    actions: 1,
                    items: 1,
                    replay_start: false,
                },
                &bytes,
                TimestampNs(start_ns),
                not_after,
            ),
            Err(Mfr1TransformError::OutsideAdmissionWindow)
        );
    }
}

#[test]
fn crc_tamper_full_truncation_and_one_to_three_byte_tails_fail_closed() {
    let (base_transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
    let valid = mfr1(&[(1, start_ns, FrameOpcode::Text, b"market")]);
    let mut tampered = valid.clone();
    *tampered.last_mut().unwrap() ^= 1;
    assert!(matches!(
        base_transformer.transform(
            BurstMachine {
                actions: 1,
                items: 1,
                replay_start: false
            },
            &tampered,
            TimestampNs(start_ns),
            decision(start_ns),
        ),
        Err(Mfr1TransformError::Recording(_))
    ));

    for tail_len in 1..=3 {
        let (transformer, _) = context(8, OverflowPolicy::FailEngine);
        let mut with_tail = valid.clone();
        with_tail.extend(std::iter::repeat_n(0xa5, tail_len));
        assert!(matches!(
            transformer.transform(
                BurstMachine {
                    actions: 1,
                    items: 1,
                    replay_start: false
                },
                &with_tail,
                TimestampNs(start_ns),
                decision(start_ns),
            ),
            Err(Mfr1TransformError::Recording(_))
        ));
    }
    let (transformer, _) = context(8, OverflowPolicy::FailEngine);
    assert!(matches!(
        transformer.transform(
            BurstMachine {
                actions: 1,
                items: 1,
                replay_start: false
            },
            &valid[..valid.len() - 1],
            TimestampNs(start_ns),
            decision(start_ns),
        ),
        Err(Mfr1TransformError::Recording(_))
    ));
}

#[test]
fn format_header_start_and_selected_monotonic_progression_are_bound() {
    let (transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
    let mut old_format = empty_mfr1(start_ns);
    old_format[4..6].copy_from_slice(&2u16.to_le_bytes());
    assert_eq!(
        transformer.transform(
            BurstMachine {
                actions: 0,
                items: 0,
                replay_start: false,
            },
            &old_format,
            TimestampNs(start_ns),
            decision(start_ns),
        ),
        Err(Mfr1TransformError::FormatVersion)
    );

    let (transformer, _) = context(8, OverflowPolicy::FailEngine);
    let wrong_start = empty_mfr1(start_ns + 1_000);
    assert_eq!(
        transformer.transform(
            BurstMachine {
                actions: 0,
                items: 0,
                replay_start: false,
            },
            &wrong_start,
            TimestampNs(start_ns),
            decision(start_ns + 1_000),
        ),
        Err(Mfr1TransformError::HeaderStartMismatch)
    );

    let (transformer, _) = context(8, OverflowPolicy::FailEngine);
    let mut writer = RawSegmentWriter::create(Vec::new(), start_ns).unwrap();
    writer
        .write_metadata(&MetadataRecord::Build(build_metadata()), start_ns)
        .unwrap();
    writer
        .write_metadata(&session_metadata(), start_ns)
        .unwrap();
    for (frame_seq, monotonic_ns) in [(1, 2), (2, 1)] {
        writer
            .write_record(
                SessionId(1),
                frame_seq,
                start_ns,
                monotonic_ns,
                Direction::Inbound,
                FrameOpcode::Ping,
                0,
                b"",
            )
            .unwrap();
    }
    assert_eq!(
        transformer.transform(
            BurstMachine {
                actions: 0,
                items: 0,
                replay_start: false,
            },
            &writer.into_inner(),
            TimestampNs(start_ns),
            decision(start_ns),
        ),
        Err(Mfr1TransformError::MonotonicRegression)
    );
}

#[test]
fn reserved_v3_session_count_header_word_must_be_zero_before_machine_start() {
    let (transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
    let mut bytes = empty_mfr1(start_ns);
    bytes[14..22].copy_from_slice(&1u64.to_le_bytes());
    let calls = Arc::new(AtomicUsize::new(0));
    assert_eq!(
        transformer.transform(
            CountingMachine(Arc::clone(&calls)),
            &bytes,
            TimestampNs(start_ns),
            decision(start_ns),
        ),
        Err(Mfr1TransformError::ReservedHeader)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn replay_runner_counts_and_fresh_strict_processor_results_match() {
    let (admission, catalog, connection, system) = topology();
    let config = admission.mechanics_config().clone();
    let start_ns = admission.capture_starts_at().utc_micros() * 1_000;
    let context = Mfr1TransformContextV1::new(
        admission,
        catalog,
        Mfr1SessionBindingV1::new(connection, 1, 1),
        system,
        Mfr1MetadataBindingV1::new(build_metadata(), session_recording_metadata()).unwrap(),
        8,
        OverflowPolicy::FailEngine,
    )
    .unwrap();
    let command = SessionCommand::Subscribe(vec!["BTC-USD".into()]);
    let wire = SubscriptionWireAction::Text(b"SUB BTC-USD\n".as_slice().to_vec().into());
    let control = encode_subscription_command(&command, &wire).unwrap();
    let bytes = mfr1(&[
        (1, start_ns, FrameOpcode::SubscriptionCommand, &control),
        (2, start_ns + 1_000, FrameOpcode::Text, b"market"),
    ]);
    let output = Mfr1TransformerV1::new(context)
        .transform(
            MarketMachine::default(),
            &bytes,
            TimestampNs(start_ns),
            decision(start_ns + 1_000),
        )
        .unwrap();
    let ordinary = ReplayRunner::new(8)
        .replay_bytes(&mut MarketMachine::default(), bytes, TimestampNs(start_ns))
        .unwrap();
    assert_eq!(ordinary.frames_applied, output.frames_applied());
    assert_eq!(ordinary.market_batches.len(), 1);

    let decoded = EpinJson1Reader::new(output.epin_json1(), decision(start_ns + 1_000))
        .read_all()
        .unwrap();
    let mut direct = SourceStateMachine::new(config.clone());
    let mut replayed = SourceStateMachine::new(config);
    let direct_results = output
        .inputs()
        .iter()
        .map(|input| direct.ingest(input).map_err(|error| error.to_string()))
        .collect::<Vec<_>>();
    let replayed_results = decoded
        .iter()
        .map(|input| replayed.ingest(input).map_err(|error| error.to_string()))
        .collect::<Vec<_>>();
    assert_eq!(direct_results, replayed_results);
}

#[test]
fn action_coordinates_and_selected_availability_must_strictly_increase() {
    let (transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
    let reused = mfr1(&[
        (5, start_ns, FrameOpcode::Text, b"market"),
        (5, start_ns + 1_000, FrameOpcode::Text, b"market"),
    ]);
    assert_eq!(
        transformer.transform(
            BurstMachine {
                actions: 1,
                items: 1,
                replay_start: false
            },
            &reused,
            TimestampNs(start_ns),
            decision(start_ns + 1_000),
        ),
        Err(Mfr1TransformError::MechanicsFrameRegression {
            previous: 5,
            current: 5,
        })
    );

    let (transformer, _) = context(8, OverflowPolicy::FailEngine);
    let regressing_time = mfr1_with_start(
        start_ns,
        &[
            (5, start_ns + 2_000, FrameOpcode::Text, b"market"),
            (6, start_ns + 1_000, FrameOpcode::Text, b"market"),
        ],
    );
    assert_eq!(
        transformer.transform(
            BurstMachine {
                actions: 1,
                items: 1,
                replay_start: false
            },
            &regressing_time,
            TimestampNs(start_ns),
            decision(start_ns + 2_000),
        ),
        Err(Mfr1TransformError::AvailabilityRegression)
    );
}

#[test]
fn constructor_and_market_mapping_reject_unbound_metadata() {
    for dispatch_capacity in [0, 16_384, 65_536] {
        let (admission, catalog, connection, system) = topology();
        assert_eq!(
            Mfr1TransformContextV1::new(
                admission,
                catalog,
                Mfr1SessionBindingV1::new(connection, 1, 1),
                system,
                Mfr1MetadataBindingV1::new(build_metadata(), session_recording_metadata()).unwrap(),
                dispatch_capacity,
                OverflowPolicy::DropNewest,
            )
            .unwrap_err(),
            Mfr1TransformError::InvalidExecutionMetadata
        );
    }

    let (admission, _catalog, connection, system) = topology();
    let start_ns = admission.capture_starts_at().utc_micros() * 1_000;
    let wrong_catalog = ReplayCatalogV1::new(
        BTreeMap::from([(
            1,
            VenueCatalogEntryV1::new("BINANCE", "unconfigured_source").unwrap(),
        )]),
        BTreeMap::from([(
            1,
            InstrumentIdentityV1::new("BTC", "USDT", "PERPETUAL", "BINANCE", "BTCUSDT").unwrap(),
        )]),
        vec![ReplayEpochEntryV1::new(1, 1, "epoch_capture_0", 0).unwrap()],
        BTreeMap::new(),
    )
    .unwrap();
    let context = Mfr1TransformContextV1::new(
        admission,
        wrong_catalog,
        Mfr1SessionBindingV1::new(connection, 1, 1),
        system,
        Mfr1MetadataBindingV1::new(build_metadata(), session_recording_metadata()).unwrap(),
        8,
        OverflowPolicy::FailEngine,
    )
    .unwrap();
    let bytes = mfr1(&[(1, start_ns, FrameOpcode::Text, b"market")]);
    assert_eq!(
        Mfr1TransformerV1::new(context).transform(
            BurstMachine {
                actions: 1,
                items: 1,
                replay_start: false
            },
            &bytes,
            TimestampNs(start_ns),
            decision(start_ns),
        ),
        Err(Mfr1TransformError::SessionMetadataMismatch)
    );
}

#[test]
fn connect_and_decision_bounds_are_checked_before_machine_mutation() {
    let (transformer, start_ns) = context(8, OverflowPolicy::FailEngine);
    assert_eq!(
        transformer.transform(
            BurstMachine {
                actions: 1,
                items: 1,
                replay_start: true
            },
            &empty_mfr1(start_ns),
            TimestampNs(start_ns - 1_000),
            decision(start_ns),
        ),
        Err(Mfr1TransformError::OutsideAdmissionWindow)
    );
    let (transformer, _) = context(8, OverflowPolicy::FailEngine);
    assert_eq!(
        transformer.transform(
            BurstMachine {
                actions: 1,
                items: 1,
                replay_start: true
            },
            &empty_mfr1(start_ns),
            TimestampNs(start_ns),
            decision(start_ns - 1_000),
        ),
        Err(Mfr1TransformError::OutsideAdmissionWindow)
    );
}
