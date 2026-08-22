use std::collections::BTreeMap;

use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, ConcreteSubscriptionSet, EventBatch, HttpResponse, SessionAction,
    SessionCommand, SessionInput, SessionMachine, SessionSpec, SubscriptionWireAction,
    VenueFactory,
};
use marketfeed_adapter_synthetic::SyntheticFactory;
use marketfeed_dispatch::{EventDispatcher, PushOutcome};
use marketfeed_event_pulse::{
    EpinJson1Reader, EpinJson1Writer, IngestOutcome, ReplayInputError,
    snapshot::{AuthoredSnapshot, MechanicsProcessor, SnapshotError},
    wire::{
        CanonicalDecimal, ClockCursorV1, ClockQualityV1, ClockSourceKeyV1, ClockSourceV1,
        ClockStateV1, ConfiguredTargetKeyV1, ConnectionKeyV1, ContributorKeyV1, ContributorRoleV1,
        ContributorSpecV1, ContributorV1, CoverageCursorV1, CoverageSourceKeyV1, CoverageSourceV1,
        CursorModeV1, CursorV1, DropCategoryV1, FamilyV1, FaultScopeKindV1, FaultScopeV1,
        InstrumentIdentityV1, MechanicsConfigV1, MechanicsInputV1, ReplayCatalogV1,
        ReplayEpochEntryV1, Rfc3339Time, SnapshotAuthoringV1, SystemChainPreimage, SystemFaultV1,
        SystemSourceKeyV1, SystemSourceV1, VenueCatalogEntryV1,
    },
};
use marketfeed_model::{
    AggressorSide, BookChange, BookDelta, BookLevel, BookOperation, BookSide, BookSnapshot,
    CatalogVersion, CatalogView, ConnectionId, EventEnvelope, EventFlags, Fixed, FrameStamp,
    InstrumentId, MarketEvent, OverflowPolicy, Price, Quantity, Quote, SequenceRange, SessionId,
    SystemEvent, TimestampNs, Trade, VenueId,
};
use marketfeed_recording::{
    Direction, FrameOpcode, MetadataRecord, RawSegmentReader, RawSegmentWriter,
    decode_http_response, decode_metadata, decode_subscription_command, encode_http_response,
    encode_metadata, encode_subscription_command,
};
use marketfeed_replay::ReplayRunner;

fn time_ns(ns: i64) -> Rfc3339Time {
    Rfc3339Time::from_unix_nanos(ns).unwrap()
}

fn catalog() -> ReplayCatalogV1 {
    ReplayCatalogV1::new(
        BTreeMap::from([
            (
                1,
                VenueCatalogEntryV1::new("BINANCE", "binance_source").unwrap(),
            ),
            (
                2,
                VenueCatalogEntryV1::new("HYPERLIQUID", "hyperliquid_source").unwrap(),
            ),
        ]),
        BTreeMap::from([(
            7,
            InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "HYPERLIQUID", "BNB-USDC")
                .unwrap(),
        )]),
        vec![ReplayEpochEntryV1::new(3, 4, "epoch_one", 0).unwrap()],
        BTreeMap::new(),
    )
    .unwrap()
}

fn market(sequence: u64, ns: i64, price: i128) -> MechanicsInputV1 {
    MechanicsInputV1::market(
        EventEnvelope {
            schema_version: 1,
            venue: VenueId(2),
            instrument: Some(InstrumentId(7)),
            connection: ConnectionId(3),
            session: SessionId(4),
            frame_seq: sequence,
            event_index: 0,
            exchange_ts: Some(TimestampNs(ns)),
            receive_ts: TimestampNs(ns),
            source_sequence: None,
            flags: EventFlags::empty(),
            payload: MarketEvent::Trade(Trade {
                price: Price(Fixed::new(price, 0)),
                quantity: Quantity(Fixed::new(1, 0)),
                aggressor: AggressorSide::Buy,
                trade_id: None,
            }),
        },
        0,
        catalog(),
    )
    .unwrap()
}

#[derive(Clone)]
struct SnapshotReplayFixture {
    config: MechanicsConfigV1,
    contributor: ContributorKeyV1,
    clock: ClockSourceKeyV1,
}

fn snapshot_fixture() -> SnapshotReplayFixture {
    let instrument =
        InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "HYPERLIQUID", "BNB-USDC").unwrap();
    let contributor = ContributorKeyV1::new("hyperliquid_source", instrument).unwrap();
    let connection = ConnectionKeyV1::new("market_connection").unwrap();
    let clock = ClockSourceKeyV1::new("clock_source", contributor.clone()).unwrap();
    let families = [FamilyV1::Trade, FamilyV1::Quote, FamilyV1::Book];
    let config = MechanicsConfigV1::new(
        "event_pulse_replay_processor",
        vec![connection.clone()],
        vec![
            ContributorSpecV1::new(contributor.clone(), ContributorRoleV1::Primary, families)
                .unwrap(),
        ],
        BTreeMap::from([(contributor.clone(), connection)]),
        vec![clock.clone()],
        families
            .into_iter()
            .enumerate()
            .map(|(index, family)| {
                CoverageSourceKeyV1::new(&format!("coverage_{index}"), contributor.clone(), family)
                    .unwrap()
            })
            .collect(),
        vec![],
    )
    .unwrap();
    SnapshotReplayFixture {
        config,
        contributor,
        clock,
    }
}

fn snapshot_authoring() -> SnapshotAuthoringV1 {
    SnapshotAuthoringV1::new(
        "event_pulse_mechanics_replay",
        "lineage_event_pulse_replay",
        "event_cluster_replay",
        InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "HYPERLIQUID", "BNB-USDC").unwrap(),
        1,
        None,
        15_000,
        "test-v1",
    )
    .unwrap()
}

fn snapshot_catalog(epoch: &str, generation: u8) -> ReplayCatalogV1 {
    snapshot_catalog_for(7, 9, epoch, generation)
}

fn snapshot_catalog_for(
    connection: u64,
    session: u64,
    epoch: &str,
    generation: u8,
) -> ReplayCatalogV1 {
    snapshot_catalog_for_venue(1, connection, session, epoch, generation)
}

fn snapshot_catalog_for_venue(
    venue: u16,
    connection: u64,
    session: u64,
    epoch: &str,
    generation: u8,
) -> ReplayCatalogV1 {
    ReplayCatalogV1::new(
        BTreeMap::from([(
            venue,
            VenueCatalogEntryV1::new("HYPERLIQUID", "hyperliquid_source").unwrap(),
        )]),
        BTreeMap::from([(
            1,
            InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "HYPERLIQUID", "BNB-USDC")
                .unwrap(),
        )]),
        vec![ReplayEpochEntryV1::new(connection, session, epoch, generation).unwrap()],
        BTreeMap::new(),
    )
    .unwrap()
}

fn snapshot_market(
    sequence: u64,
    ns: i64,
    epoch: &str,
    generation: u8,
    payload: MarketEvent,
) -> MechanicsInputV1 {
    MechanicsInputV1::market(
        EventEnvelope {
            schema_version: 1,
            venue: VenueId(1),
            instrument: Some(InstrumentId(1)),
            connection: ConnectionId(7),
            session: SessionId(9),
            frame_seq: sequence,
            event_index: 0,
            exchange_ts: Some(TimestampNs(ns)),
            receive_ts: TimestampNs(ns),
            source_sequence: Some(SequenceRange {
                first: sequence,
                last: sequence,
            }),
            flags: EventFlags::empty(),
            payload,
        },
        0,
        snapshot_catalog(epoch, generation),
    )
    .unwrap()
}

fn scaled_trade(price: i128) -> MarketEvent {
    MarketEvent::Trade(Trade {
        price: Price(Fixed::new(price, 8)),
        quantity: Quantity(Fixed::new(100_000_000, 8)),
        aggressor: AggressorSide::Buy,
        trade_id: None,
    })
}

fn system_drop(ns: i64) -> MechanicsInputV1 {
    let key = SystemSourceKeyV1::new(
        "system_drop_source",
        FaultScopeKindV1::Processor,
        ConfiguredTargetKeyV1::processor("event_pulse_replay_processor").unwrap(),
        CursorModeV1::Derived,
    )
    .unwrap();
    MechanicsInputV1::system(
        SystemSourceV1::new(key, "epoch_system_a", 0).unwrap(),
        FaultScopeV1::processor("event_pulse_replay_processor").unwrap(),
        time_ns(ns),
        time_ns(ns),
        CursorV1::derived_drop(3, 0).unwrap(),
        SystemFaultV1::events_dropped(1, DropCategoryV1::ActionBuffer).unwrap(),
        None,
    )
    .unwrap()
}

fn snapshot_controls(
    fixture: &SnapshotReplayFixture,
    first_available_ns: i64,
    native: u64,
) -> Vec<MechanicsInputV1> {
    let contributor = ContributorV1::new(fixture.contributor.clone(), "epoch_a", 0).unwrap();
    let mut inputs = vec![
        MechanicsInputV1::clock(
            contributor.clone(),
            ClockSourceV1::new(fixture.clock.clone(), "epoch_clock_a", 0).unwrap(),
            time_ns(first_available_ns),
            time_ns(first_available_ns),
            ClockCursorV1::native(native, native).unwrap(),
            ClockStateV1::Synchronized,
            CanonicalDecimal::parse("0.25", 18, 8).unwrap(),
            2_000,
            ClockQualityV1::Validated,
            "SOURCE_CLOCK_WITHIN_TOLERANCE",
        )
        .unwrap(),
    ];
    for (offset, key) in fixture.config.coverage_sources().iter().enumerate() {
        let available = first_available_ns + i64::try_from(offset + 1).unwrap() * 1_000;
        inputs.push(
            MechanicsInputV1::coverage(
                contributor.clone(),
                CoverageSourceV1::new(key.clone(), "epoch_coverage_a", 0).unwrap(),
                key.family(),
                time_ns(0),
                time_ns(available),
                time_ns(available),
                CoverageCursorV1::native(native, native).unwrap(),
            )
            .unwrap(),
        );
    }
    inputs
}

#[test]
fn epin_json1_roundtrips_only_canonical_ordered_input_records() {
    let inputs = vec![
        market(1, 1_000, 100),
        market(2, 2_000, 101),
        system_drop(3_000),
    ];
    let mut writer = EpinJson1Writer::new(Vec::new());
    for input in &inputs {
        writer.write_input(input).unwrap();
    }
    let bytes = writer.finish();
    assert!(bytes.ends_with(b"\n"));
    assert_eq!(
        bytes.iter().filter(|byte| **byte == b'\n').count(),
        inputs.len()
    );

    let decoded = EpinJson1Reader::new(bytes.as_slice(), time_ns(3_000))
        .read_all()
        .unwrap();
    assert_eq!(decoded, inputs);
}

#[test]
fn epin_json1_rejects_future_noncanonical_hash_order_and_oversize() {
    let first = market(1, 1_000, 100);
    let second = market(2, 2_000, 101);
    let canonical = format!(
        "{}\n{}\n",
        serde_json::to_string(&first).unwrap(),
        serde_json::to_string(&second).unwrap()
    );
    assert_eq!(
        EpinJson1Reader::new(canonical.as_bytes(), time_ns(1_000)).read_all(),
        Err(ReplayInputError::FutureInput)
    );

    let whitespace = format!("{} \n", serde_json::to_string(&first).unwrap());
    assert!(matches!(
        EpinJson1Reader::new(whitespace.as_bytes(), time_ns(1_000)).read_all(),
        Err(ReplayInputError::InvalidInput(_))
    ));

    let mut wrong_hash = serde_json::to_value(&first).unwrap();
    wrong_hash["payload_hash"] = serde_json::Value::String("0".repeat(64));
    let wrong_hash = format!("{}\n", serde_json::to_string(&wrong_hash).unwrap());
    assert!(matches!(
        EpinJson1Reader::new(wrong_hash.as_bytes(), time_ns(1_000)).read_all(),
        Err(ReplayInputError::InvalidInput(_))
    ));

    let reordered = format!(
        "{}\n{}\n",
        serde_json::to_string(&second).unwrap(),
        serde_json::to_string(&first).unwrap()
    );
    assert_eq!(
        EpinJson1Reader::new(reordered.as_bytes(), time_ns(2_000)).read_all(),
        Err(ReplayInputError::OrderViolation)
    );

    let oversized = vec![b'x'; marketfeed_event_pulse::wire::MAX_INPUT_BYTES + 2];
    assert_eq!(
        EpinJson1Reader::new(oversized.as_slice(), time_ns(2_000)).read_all(),
        Err(ReplayInputError::LineTooLarge)
    );

    let mut unknown = serde_json::to_value(&first).unwrap();
    unknown["unknown"] = serde_json::Value::Bool(true);
    let unknown = format!("{}\n", serde_json::to_string(&unknown).unwrap());
    assert!(matches!(
        EpinJson1Reader::new(unknown.as_bytes(), time_ns(1_000)).read_all(),
        Err(ReplayInputError::InvalidInput(_))
    ));

    let equal_time_reordered = format!(
        "{}\n{}\n",
        serde_json::to_string(&market(2, 1_000, 101)).unwrap(),
        serde_json::to_string(&first).unwrap()
    );
    assert_eq!(
        EpinJson1Reader::new(equal_time_reordered.as_bytes(), time_ns(1_000)).read_all(),
        Err(ReplayInputError::OrderViolation)
    );

    let missing_newline = serde_json::to_vec(&first).unwrap();
    assert_eq!(
        EpinJson1Reader::new(missing_newline.as_slice(), time_ns(1_000)).read_all(),
        Err(ReplayInputError::MissingNewline)
    );
}

#[test]
fn epin_json1_replay_authors_byte_identical_complete_snapshot_sequences() {
    let fixture = snapshot_fixture();
    let mut inputs = vec![
        snapshot_market(1, 0, "epoch_a", 0, scaled_trade(10_000_000_000)),
        snapshot_market(
            2,
            60_000_000_000,
            "epoch_a",
            0,
            scaled_trade(10_000_000_000),
        ),
        snapshot_market(
            3,
            60_010_000_000,
            "epoch_a",
            0,
            scaled_trade(10_100_000_000),
        ),
        snapshot_market(
            4,
            60_020_000_000,
            "epoch_a",
            0,
            MarketEvent::Quote(Quote {
                bid_price: Price(Fixed::new(10_000_000_000, 8)),
                bid_quantity: Some(Quantity(Fixed::new(100_000_000, 8))),
                ask_price: Price(Fixed::new(10_010_000_000, 8)),
                ask_quantity: Some(Quantity(Fixed::new(100_000_000, 8))),
            }),
        ),
        snapshot_market(
            5,
            60_030_000_000,
            "epoch_a",
            0,
            MarketEvent::BookSnapshot(BookSnapshot {
                bids: vec![BookLevel {
                    price: Price(Fixed::new(10_000_000_000, 8)),
                    quantity: Quantity(Fixed::new(100_000_000, 8)),
                }],
                asks: vec![BookLevel {
                    price: Price(Fixed::new(10_010_000_000, 8)),
                    quantity: Quantity(Fixed::new(100_000_000, 8)),
                }],
                depth: None,
                checksum: None,
            }),
        ),
        snapshot_market(
            6,
            60_040_000_000,
            "epoch_a",
            0,
            MarketEvent::BookDelta(BookDelta {
                changes: vec![BookChange {
                    side: BookSide::Bid,
                    operation: BookOperation::Upsert,
                    price: Price(Fixed::new(10_000_000_000, 8)),
                    quantity: Some(Quantity(Fixed::new(200_000_000, 8))),
                }],
                checksum: None,
            }),
        ),
    ];
    inputs.extend(snapshot_controls(&fixture, 60_080_000_000, 1));
    inputs.push(snapshot_market(
        7,
        60_100_000_000,
        "epoch_a",
        0,
        scaled_trade(10_200_000_000),
    ));
    inputs.push(snapshot_market(
        8,
        60_500_000_000,
        "epoch_a",
        0,
        scaled_trade(10_300_000_000),
    ));
    inputs.extend(snapshot_controls(&fixture, 60_550_000_000, 2));
    inputs.push(snapshot_market(
        9,
        61_000_000_000,
        "epoch_a",
        0,
        scaled_trade(10_400_000_000),
    ));

    let mut writer = EpinJson1Writer::new(Vec::new());
    for input in &inputs {
        writer.write_input(input).unwrap();
    }
    let encoded = writer.finish();
    let decoded = EpinJson1Reader::new(encoded.as_slice(), time_ns(61_000_000_000))
        .read_all()
        .unwrap();
    assert_eq!(decoded, inputs);

    let mut original =
        MechanicsProcessor::new(fixture.config.clone(), snapshot_authoring()).unwrap();
    let mut replayed = MechanicsProcessor::new(fixture.config, snapshot_authoring()).unwrap();
    let mut original_snapshots = Vec::new();
    let mut replayed_snapshots = Vec::new();
    let mut ingested = 0usize;
    for (through, decision) in [
        (10, 60_090_000_000),
        (11, 60_120_000_000),
        (16, 60_600_000_000),
        (17, 61_100_000_000),
    ] {
        for (left, right) in inputs[ingested..through]
            .iter()
            .zip(&decoded[ingested..through])
        {
            assert_eq!(
                original.ingest(left).map_err(|error| error.to_string()),
                replayed.ingest(right).map_err(|error| error.to_string())
            );
        }
        ingested = through;
        let left = original
            .snapshot(time_ns(decision))
            .unwrap_or_else(|error| panic!("snapshot {decision}: {error}"));
        let right = replayed
            .snapshot(time_ns(decision))
            .unwrap_or_else(|error| panic!("replayed snapshot {decision}: {error}"));
        original_snapshots.push((left.canonical_json(), left.content_hash().to_owned()));
        replayed_snapshots.push((right.canonical_json(), right.content_hash().to_owned()));
    }
    assert_eq!(replayed_snapshots, original_snapshots);
    assert!(
        original_snapshots
            .windows(2)
            .all(|pair| pair[0].1 != pair[1].1)
    );
}

#[test]
fn epin_book_snapshot_survives_a_250ms_advance_and_contiguous_delta() {
    let fixture = snapshot_fixture();
    let mut inputs = vec![
        snapshot_market(1, 0, "epoch_a", 0, scaled_trade(10_000_000_000)),
        snapshot_market(
            2,
            60_000_000_000,
            "epoch_a",
            0,
            scaled_trade(10_000_000_000),
        ),
        snapshot_market(
            3,
            60_010_000_000,
            "epoch_a",
            0,
            MarketEvent::BookSnapshot(BookSnapshot {
                bids: vec![BookLevel {
                    price: Price(Fixed::new(10_000_000_000, 8)),
                    quantity: Quantity(Fixed::new(100_000_000, 8)),
                }],
                asks: vec![BookLevel {
                    price: Price(Fixed::new(10_010_000_000, 8)),
                    quantity: Quantity(Fixed::new(100_000_000, 8)),
                }],
                depth: None,
                checksum: None,
            }),
        ),
        snapshot_market(
            4,
            60_310_000_001,
            "epoch_a",
            0,
            MarketEvent::BookDelta(BookDelta {
                changes: vec![BookChange {
                    side: BookSide::Bid,
                    operation: BookOperation::Upsert,
                    price: Price(Fixed::new(10_000_000_000, 8)),
                    quantity: Some(Quantity(Fixed::new(300_000_000, 8))),
                }],
                checksum: None,
            }),
        ),
        snapshot_market(
            5,
            60_320_000_000,
            "epoch_a",
            0,
            MarketEvent::Quote(Quote {
                bid_price: Price(Fixed::new(10_000_000_000, 8)),
                bid_quantity: None,
                ask_price: Price(Fixed::new(10_010_000_000, 8)),
                ask_quantity: None,
            }),
        ),
    ];
    inputs.extend(snapshot_controls(&fixture, 60_330_000_000, 1));

    let mut writer = EpinJson1Writer::new(Vec::new());
    for input in &inputs {
        writer.write_input(input).unwrap();
    }
    let decision = 60_330_003_000;
    let decoded = EpinJson1Reader::new(writer.finish().as_slice(), time_ns(decision))
        .read_all()
        .unwrap();
    let mut direct = MechanicsProcessor::new(fixture.config.clone(), snapshot_authoring()).unwrap();
    let mut replayed = MechanicsProcessor::new(fixture.config, snapshot_authoring()).unwrap();
    for (left, right) in inputs.iter().zip(&decoded) {
        direct.ingest(left).unwrap();
        replayed.ingest(right).unwrap();
    }
    let left = direct.snapshot(time_ns(decision)).unwrap();
    let right = replayed.snapshot(time_ns(decision)).unwrap();
    assert_eq!(left.canonical_json(), right.canonical_json());
    assert_eq!(left.content_hash(), right.content_hash());
    let depth = left.value()["features"]
        .as_array()
        .unwrap()
        .iter()
        .find(|feature| feature["name"] == "book_depth_10bps")
        .unwrap();
    assert!(depth["value"].is_string());
    assert_eq!(depth["quality_state"], "VALIDATED");
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CapturedAction {
    action_index: u32,
    item_index: u32,
    available_at: TimestampNs,
    action: SessionAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CapturedDropCategory {
    ActionBuffer,
    MarketDispatch,
    SystemDispatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CapturedDrop {
    action_index: u32,
    item_index: u32,
    available_at: TimestampNs,
    category: CapturedDropCategory,
    count: u64,
    event: SystemEvent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CapturedFrame {
    frame_seq: u64,
    available_at: TimestampNs,
    ordinary: Vec<CapturedAction>,
    drops: Vec<CapturedDrop>,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct OrderedCaptureOutcome {
    replay_start: Option<CapturedFrame>,
    frames: Vec<CapturedFrame>,
    market_batches: Vec<EventBatch>,
    system_events: Vec<SystemEvent>,
    metadata: Vec<MetadataRecord>,
    frames_applied: u64,
    max_actions: usize,
    max_market_dispatch: usize,
    max_system_dispatch: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CaptureError {
    Recording(String),
    Adapter(String),
    Dispatch(String),
    AvailabilityRegression,
    MechanicsFrameZero,
    MechanicsFrameRegression { previous: u64, current: u64 },
    ActionIndexOverflow,
    ItemIndexOverflow,
}

/// Test-only MFR1 oracle. It intentionally captures actions before ReplayRunner lane separation.
struct OrderedReplayCapture {
    actions: ActionBuffer,
    dispatch: EventDispatcher,
    last_available_at: Option<TimestampNs>,
}

impl OrderedReplayCapture {
    fn new(dispatch_capacity: usize, overflow: OverflowPolicy) -> Self {
        Self {
            actions: ActionBuffer::new(),
            dispatch: EventDispatcher::new(dispatch_capacity, dispatch_capacity, overflow),
            last_available_at: None,
        }
    }

    fn replay(
        &mut self,
        machine: &mut dyn SessionMachine,
        bytes: Vec<u8>,
        connect_at: TimestampNs,
    ) -> Result<OrderedCaptureOutcome, CaptureError> {
        let mut outcome = OrderedCaptureOutcome::default();
        self.last_available_at = Some(connect_at);
        let mut last_mechanics_frame_seq = None;
        outcome.replay_start = Some(self.begin_frame(
            machine,
            0,
            connect_at,
            |machine, actions| machine.on_replay_start(connect_at, actions),
            &mut outcome,
        )?);

        let mut reader = RawSegmentReader::from_bytes(bytes)
            .map_err(|error| CaptureError::Recording(error.to_string()))?;
        for record in reader
            .read_all()
            .map_err(|error| CaptureError::Recording(error.to_string()))?
        {
            if record.header.direction != Direction::Inbound {
                continue;
            }
            let available_at = TimestampNs(record.header.receive_ts_ns);
            if self
                .last_available_at
                .is_some_and(|last| available_at < last)
            {
                return Err(CaptureError::AvailabilityRegression);
            }
            self.last_available_at = Some(available_at);
            let frame_seq = record.header.frame_seq;
            let stamp = FrameStamp {
                receive_ts: available_at,
                mono_ns: record.header.monotonic_ns,
            };
            let opcode = record.header.opcode;
            let mut payload = record.payload;
            match opcode {
                FrameOpcode::Text | FrameOpcode::Binary | FrameOpcode::Pong => {
                    let frame = self.begin_frame(
                        machine,
                        frame_seq,
                        available_at,
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
                        &mut outcome,
                    )?;
                    reserve_mechanics_coordinate(&mut last_mechanics_frame_seq, &frame)?;
                    outcome.frames.push(frame);
                }
                FrameOpcode::HttpResponse => {
                    let (request_id, response) = decode_http_response(&payload)
                        .map_err(|error| CaptureError::Recording(error.to_string()))?;
                    let frame = self.begin_frame(
                        machine,
                        frame_seq,
                        available_at,
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
                        &mut outcome,
                    )?;
                    reserve_mechanics_coordinate(&mut last_mechanics_frame_seq, &frame)?;
                    outcome.frames.push(frame);
                }
                FrameOpcode::Metadata => {
                    outcome.metadata.push(
                        decode_metadata(&payload)
                            .map_err(|error| CaptureError::Recording(error.to_string()))?,
                    );
                    continue;
                }
                FrameOpcode::SubscriptionCommand => {
                    let frame = self.begin_frame(
                        machine,
                        frame_seq,
                        available_at,
                        |machine, _actions| {
                            let (command, recorded_wire) = decode_subscription_command(&payload)
                                .map_err(|error| AdapterError::Parse(error.to_string()))?;
                            let prepared_wire = machine.prepare_dynamic_subscription(&command)?;
                            if prepared_wire != recorded_wire {
                                return Err(AdapterError::Parse(
                                    "recorded subscription wire action mismatch".into(),
                                ));
                            }
                            machine.commit_dynamic_subscription(&command);
                            Ok(())
                        },
                        &mut outcome,
                    )?;
                    reserve_mechanics_coordinate(&mut last_mechanics_frame_seq, &frame)?;
                    outcome.frames.push(frame);
                }
                FrameOpcode::Ping | FrameOpcode::Close => continue,
            }
            outcome.frames_applied += 1;
        }
        outcome.market_batches = self.dispatch.drain_batches();
        outcome.system_events = self.dispatch.drain_systems();
        Ok(outcome)
    }

    fn begin_frame<F>(
        &mut self,
        machine: &mut dyn SessionMachine,
        frame_seq: u64,
        available_at: TimestampNs,
        apply: F,
        outcome: &mut OrderedCaptureOutcome,
    ) -> Result<CapturedFrame, CaptureError>
    where
        F: FnOnce(&mut dyn SessionMachine, &mut ActionBuffer) -> Result<(), AdapterError>,
    {
        self.actions.clear();
        let _ = self.actions.take_dropped();
        apply(machine, &mut self.actions)
            .map_err(|error| CaptureError::Adapter(error.to_string()))?;
        outcome.max_actions = outcome.max_actions.max(self.actions.len());

        let mut ordinary = Vec::new();
        for (action_index, action) in self.actions.as_slice().iter().cloned().enumerate() {
            let action_index =
                u32::try_from(action_index).map_err(|_| CaptureError::ActionIndexOverflow)?;
            match &action {
                SessionAction::EmitBatch(batch) => {
                    for (item_index, _event) in batch.events.iter().enumerate() {
                        ordinary.push(CapturedAction {
                            action_index,
                            item_index: u32::try_from(item_index)
                                .map_err(|_| CaptureError::ItemIndexOverflow)?,
                            available_at,
                            action: action.clone(),
                        });
                    }
                    if batch.events.is_empty() {
                        ordinary.push(CapturedAction {
                            action_index,
                            item_index: 0,
                            available_at,
                            action: action.clone(),
                        });
                    }
                }
                _ => ordinary.push(CapturedAction {
                    action_index,
                    item_index: 0,
                    available_at,
                    action,
                }),
            }
        }

        let action_drops = self.actions.take_dropped();
        let retained: Vec<_> = self.actions.drain().collect();
        let mut market_drops = (0u64, "DropNewest");
        let mut system_drops = (0u64, "DropNewest");
        for action in retained {
            match action {
                SessionAction::EmitBatch(batch) => {
                    accumulate_drop(
                        &mut market_drops,
                        self.dispatch
                            .push_batch(batch)
                            .map_err(|error| CaptureError::Dispatch(error.to_string()))?,
                    );
                }
                SessionAction::EmitSystem(event) => {
                    accumulate_drop(
                        &mut system_drops,
                        self.dispatch
                            .push_system(event)
                            .map_err(|error| CaptureError::Dispatch(error.to_string()))?,
                    );
                }
                _ => {}
            }
        }
        outcome.max_market_dispatch = outcome
            .max_market_dispatch
            .max(self.dispatch.batches().len());
        outcome.max_system_dispatch = outcome
            .max_system_dispatch
            .max(self.dispatch.systems().len());
        let mut drops = Vec::new();
        for (category, (count, policy)) in [
            (
                CapturedDropCategory::ActionBuffer,
                (action_drops, "DropNewest"),
            ),
            (CapturedDropCategory::MarketDispatch, market_drops),
            (CapturedDropCategory::SystemDispatch, system_drops),
        ] {
            if count > 0 {
                drops.push(CapturedDrop {
                    action_index: u32::from(u16::MAX),
                    item_index: match category {
                        CapturedDropCategory::ActionBuffer => 0,
                        CapturedDropCategory::MarketDispatch => 1,
                        CapturedDropCategory::SystemDispatch => 2,
                    },
                    available_at,
                    category,
                    count,
                    event: SystemEvent::EventsDropped {
                        count,
                        detail: format!(
                            "{} {policy}",
                            match category {
                                CapturedDropCategory::ActionBuffer => "ActionBuffer",
                                CapturedDropCategory::MarketDispatch => "market_batch",
                                CapturedDropCategory::SystemDispatch => "system_event",
                            }
                        ),
                    },
                });
            }
        }
        Ok(CapturedFrame {
            frame_seq,
            available_at,
            ordinary,
            drops,
        })
    }
}

fn reserve_mechanics_coordinate(
    previous: &mut Option<u64>,
    frame: &CapturedFrame,
) -> Result<(), CaptureError> {
    if !frame_has_mechanics(frame) {
        return Ok(());
    }
    if frame.frame_seq == 0 {
        return Err(CaptureError::MechanicsFrameZero);
    }
    if let Some(prior) = *previous {
        if frame.frame_seq <= prior {
            return Err(CaptureError::MechanicsFrameRegression {
                previous: prior,
                current: frame.frame_seq,
            });
        }
    }
    *previous = Some(frame.frame_seq);
    Ok(())
}

fn frame_has_mechanics(frame: &CapturedFrame) -> bool {
    !frame.drops.is_empty()
        || frame.ordinary.iter().any(|record| {
            matches!(
                record.action,
                SessionAction::EmitBatch(_) | SessionAction::EmitSystem(_)
            )
        })
}

fn captured_actions(
    captured: &OrderedCaptureOutcome,
) -> impl Iterator<Item = (&CapturedFrame, &CapturedAction)> {
    captured
        .replay_start
        .iter()
        .chain(&captured.frames)
        .flat_map(|frame| frame.ordinary.iter().map(move |action| (frame, action)))
}

fn captured_drops(
    captured: &OrderedCaptureOutcome,
) -> impl Iterator<Item = (&CapturedFrame, &CapturedDrop)> {
    captured
        .replay_start
        .iter()
        .chain(&captured.frames)
        .flat_map(|frame| frame.drops.iter().map(move |drop| (frame, drop)))
}

fn accumulate_drop(total: &mut (u64, &'static str), outcome: PushOutcome) {
    match outcome {
        PushOutcome::Accepted => {}
        PushOutcome::DroppedNewest => {
            total.0 += 1;
            total.1 = "DropNewest";
        }
        PushOutcome::DroppedOldest { dropped } => {
            total.0 += dropped as u64;
            total.1 = "DropOldest";
        }
    }
}

fn mfr1(frames: &[(i64, &str)]) -> Vec<u8> {
    let mut writer = RawSegmentWriter::create(Vec::new(), frames[0].0).unwrap();
    for (index, (receive_ns, payload)) in frames.iter().enumerate() {
        writer
            .write_record(
                SessionId(1),
                u64::try_from(index + 1).unwrap(),
                *receive_ns,
                u64::try_from(*receive_ns).unwrap(),
                Direction::Inbound,
                FrameOpcode::Text,
                0,
                payload.as_bytes(),
            )
            .unwrap();
    }
    writer.into_inner()
}

fn mfr1_records(records: &[(u64, i64, FrameOpcode, Vec<u8>)]) -> Vec<u8> {
    let mut writer = RawSegmentWriter::create(Vec::new(), records[0].1).unwrap();
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

fn synthetic_machine() -> Box<dyn SessionMachine> {
    SyntheticFactory
        .create_session(
            SessionSpec {
                endpoint_name: "ws".into(),
                subscriptions: ConcreteSubscriptionSet::default(),
            },
            CatalogView::new(VenueId(1), CatalogVersion(1)),
        )
        .unwrap()
}

#[derive(Default)]
struct SubscriptionCaptureMachine {
    active: bool,
}

impl SessionMachine for SubscriptionCaptureMachine {
    fn prepare_dynamic_subscription(
        &self,
        command: &SessionCommand,
    ) -> Result<SubscriptionWireAction, AdapterError> {
        match command {
            SessionCommand::Subscribe(symbols) if symbols == &["BTC-USD".to_owned()] => Ok(
                SubscriptionWireAction::Text(b"SUB BTC-USD".as_slice().to_vec().into()),
            ),
            _ => Err(AdapterError::UnsupportedCapability(
                "unexpected command".into(),
            )),
        }
    }

    fn commit_dynamic_subscription(&mut self, command: &SessionCommand) {
        self.active = matches!(command, SessionCommand::Subscribe(_));
    }

    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if matches!(input, SessionInput::TextFrame { .. }) && self.active {
            output.push(SessionAction::EmitSystem(
                SystemEvent::SubscriptionStateChanged {
                    state: "replayed-subscription-active".into(),
                },
            ));
        }
        Ok(())
    }
}

#[test]
fn subscription_command_counts_as_an_applied_authoritative_frame() {
    let command = SessionCommand::Subscribe(vec!["BTC-USD".into()]);
    let wire = SubscriptionWireAction::Text(b"SUB BTC-USD".as_slice().to_vec().into());
    let payload = encode_subscription_command(&command, &wire).unwrap();
    let recording = mfr1_records(&[
        (17, 10, FrameOpcode::SubscriptionCommand, payload),
        (42, 11, FrameOpcode::Text, b"OUTPUT".to_vec()),
    ]);
    let mut capture_machine = SubscriptionCaptureMachine::default();
    let captured = OrderedReplayCapture::new(8, OverflowPolicy::FailEngine)
        .replay(&mut capture_machine, recording.clone(), TimestampNs(9))
        .unwrap();
    let mut replay_machine = SubscriptionCaptureMachine::default();
    let ordinary = ReplayRunner::new(8)
        .replay_bytes(&mut replay_machine, recording, TimestampNs(9))
        .unwrap();

    assert_eq!(captured.frames_applied, 2);
    assert_eq!(captured.frames_applied, ordinary.frames_applied);
    assert_eq!(
        captured
            .frames
            .iter()
            .map(|frame| frame.frame_seq)
            .collect::<Vec<_>>(),
        vec![17, 42]
    );
    assert!(captured.frames[0].ordinary.is_empty());
    assert_eq!(captured.system_events, ordinary.system_events);
    assert_eq!(captured.frames[1].ordinary[0].action_index, 0);
}

#[test]
fn empty_controls_may_use_zero_or_reuse_a_market_raw_coordinate() {
    let command = SessionCommand::Subscribe(vec!["BTC-USD".into()]);
    let wire = SubscriptionWireAction::Text(b"SUB BTC-USD".as_slice().to_vec().into());
    let control = encode_subscription_command(&command, &wire).unwrap();
    let metadata = MetadataRecord::current_build();
    let recording = mfr1_records(&[
        (0, 10, FrameOpcode::SubscriptionCommand, control.clone()),
        (
            0,
            10,
            FrameOpcode::Metadata,
            encode_metadata(&metadata).unwrap(),
        ),
        (5, 11, FrameOpcode::Text, b"OUTPUT".to_vec()),
        (5, 12, FrameOpcode::SubscriptionCommand, control),
        (6, 13, FrameOpcode::Text, b"OUTPUT".to_vec()),
    ]);
    let mut capture_machine = SubscriptionCaptureMachine::default();
    let captured = OrderedReplayCapture::new(8, OverflowPolicy::FailEngine)
        .replay(&mut capture_machine, recording.clone(), TimestampNs(9))
        .unwrap();
    let mut replay_machine = SubscriptionCaptureMachine::default();
    let ordinary = ReplayRunner::new(8)
        .replay_bytes(&mut replay_machine, recording, TimestampNs(9))
        .unwrap();

    assert_eq!(captured.frames_applied, 4);
    assert_eq!(captured.frames_applied, ordinary.frames_applied);
    assert_eq!(captured.metadata, vec![metadata]);
    assert_eq!(captured.system_events, ordinary.system_events);
    assert_eq!(
        captured
            .frames
            .iter()
            .map(|frame| frame.frame_seq)
            .collect::<Vec<_>>(),
        vec![0, 5, 5, 6]
    );
    assert!(!frame_has_mechanics(&captured.frames[0]));
    assert!(!frame_has_mechanics(&captured.frames[2]));

    let mut prior = Some(5);
    let colliding_output = CapturedFrame {
        frame_seq: 5,
        available_at: TimestampNs(12),
        ordinary: vec![CapturedAction {
            action_index: 0,
            item_index: 0,
            available_at: TimestampNs(12),
            action: SessionAction::EmitSystem(SystemEvent::ClockJump { delta_ns: 1 }),
        }],
        drops: vec![],
    };
    assert_eq!(
        reserve_mechanics_coordinate(&mut prior, &colliding_output),
        Err(CaptureError::MechanicsFrameRegression {
            previous: 5,
            current: 5,
        })
    );
    let colliding_drop = CapturedFrame {
        frame_seq: 5,
        available_at: TimestampNs(12),
        ordinary: vec![],
        drops: vec![CapturedDrop {
            action_index: u32::from(u16::MAX),
            item_index: 0,
            available_at: TimestampNs(12),
            category: CapturedDropCategory::ActionBuffer,
            count: 1,
            event: SystemEvent::EventsDropped {
                count: 1,
                detail: "control overflow".into(),
            },
        }],
    };
    assert_eq!(
        reserve_mechanics_coordinate(&mut prior, &colliding_drop),
        Err(CaptureError::MechanicsFrameRegression {
            previous: 5,
            current: 5,
        })
    );
}

#[test]
fn mfr1_capture_preserves_pre_lane_coordinates_and_matches_ordinary_replay() {
    let recording = mfr1_records(&[
        (1, 1_000_000_000, FrameOpcode::Text, b"SUB BTC-USD".to_vec()),
        (
            2,
            1_000_000_001,
            FrameOpcode::Text,
            b"BOOK_SNAP 10 BID 100.00:1.000 ASK 101.00:1.500".to_vec(),
        ),
        (
            3,
            1_000_000_002,
            FrameOpcode::Text,
            b"BOOK_DELTA 11 BID UPSERT 100.50 0.500".to_vec(),
        ),
        (
            4,
            1_000_000_003,
            FrameOpcode::Text,
            b"BOOK_DELTA 11 BID UPSERT 100.50 0.750".to_vec(),
        ),
        (
            5,
            1_000_000_004,
            FrameOpcode::Text,
            b"BOOK_DELTA 13 ASK DELETE 101.00".to_vec(),
        ),
        (
            6,
            1_000_000_005,
            FrameOpcode::Text,
            b"BOOK_SNAP 20 BID 99.00:1.000 ASK 102.00:1.000".to_vec(),
        ),
        (
            7,
            1_250_000_006,
            FrameOpcode::Text,
            b"QUOTE 99.50 101.50 1.000 1.000".to_vec(),
        ),
        (8, 1_500_000_007, FrameOpcode::Text, b"DISCONNECT".to_vec()),
        (9, 1_750_000_008, FrameOpcode::Text, b"SUB BTC-USD".to_vec()),
    ]);
    let mut capture_machine = synthetic_machine();
    let captured = OrderedReplayCapture::new(1_024, OverflowPolicy::FailEngine)
        .replay(
            &mut *capture_machine,
            recording.clone(),
            TimestampNs(999_999_999),
        )
        .unwrap();
    let mut replay_machine = synthetic_machine();
    let ordinary = ReplayRunner::new(1_024)
        .replay_bytes(&mut *replay_machine, recording, TimestampNs(999_999_999))
        .unwrap();

    assert_eq!(captured.frames_applied, ordinary.frames_applied);
    assert_eq!(captured.market_batches, ordinary.market_batches);
    assert_eq!(captured.system_events, ordinary.system_events);
    assert_eq!(
        captured_drops(&captured)
            .map(|(_, drop)| drop.count)
            .sum::<u64>(),
        ordinary.dropped_events
    );
    assert!(
        captured
            .frames
            .iter()
            .all(|frame| frame.ordinary.windows(2).all(|pair| (
                pair[0].action_index,
                pair[0].item_index
            ) <= (
                pair[1].action_index,
                pair[1].item_index
            )))
    );
    assert!(
        captured_actions(&captured)
            .map(|(_, record)| record)
            .all(|record| record.available_at.0 <= 1_750_000_008)
    );
    let captured_other = captured_actions(&captured)
        .map(|(_, record)| record)
        .filter_map(|record| match &record.action {
            SessionAction::EmitBatch(_) | SessionAction::EmitSystem(_) => None,
            action => Some(action.clone()),
        })
        .collect::<Vec<_>>();
    assert_eq!(captured_other, ordinary.other_actions);
    let market_events = captured
        .market_batches
        .iter()
        .flat_map(|batch| &batch.events)
        .collect::<Vec<_>>();
    assert!(
        market_events
            .iter()
            .any(|event| event.source_sequence.is_none())
    );
    assert!(captured.system_events.iter().any(|event| matches!(
        event,
        SystemEvent::SequenceGap { .. } | SystemEvent::BookInvalidated { .. }
    )));
    assert!(
        captured
            .system_events
            .iter()
            .any(|event| matches!(event, SystemEvent::BookResynchronized { .. }))
    );
    assert!(
        captured_actions(&captured)
            .map(|(_, record)| record)
            .any(|record| matches!(record.action, SessionAction::Reconnect(_)))
    );
    assert!(
        captured_actions(&captured)
            .map(|(_, record)| record.available_at)
            .collect::<Vec<_>>()
            .windows(2)
            .any(|pair| pair[1].0 - pair[0].0 > 250_000_000)
    );
}

struct BurstMachine;

impl SessionMachine for BurstMachine {
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
        let SessionInput::TextFrame { received, .. } = input else {
            return Ok(());
        };
        for index in 0..1_030u64 {
            if index % 2 == 0 {
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: SessionId(1),
                    frame_seq: index,
                    events: Vec::new(),
                }));
            } else {
                output.push(SessionAction::EmitSystem(SystemEvent::ClockJump {
                    delta_ns: received.receive_ts.0,
                }));
            }
        }
        Ok(())
    }
}

#[test]
fn fixed_burst_reports_every_real_action_and_dispatch_loss_with_bounded_queues() {
    let recording = mfr1(&[(10, "BURST")]);
    let mut capture_machine = BurstMachine;
    let captured = OrderedReplayCapture::new(4, OverflowPolicy::DropNewest)
        .replay(&mut capture_machine, recording.clone(), TimestampNs(9))
        .unwrap();
    let mut replay_machine = BurstMachine;
    let ordinary = ReplayRunner::with_overflow(4, OverflowPolicy::DropNewest)
        .replay_bytes(&mut replay_machine, recording, TimestampNs(9))
        .unwrap();

    assert_eq!(captured.frames_applied, ordinary.frames_applied);
    assert_eq!(
        captured_drops(&captured)
            .map(|(_, drop)| drop.count)
            .sum::<u64>(),
        ordinary.dropped_events
    );
    assert_eq!(
        captured_drops(&captured)
            .map(|(_, drop)| drop)
            .map(|drop| drop.category)
            .collect::<Vec<_>>(),
        vec![
            CapturedDropCategory::ActionBuffer,
            CapturedDropCategory::MarketDispatch,
            CapturedDropCategory::SystemDispatch,
        ]
    );
    assert_eq!(
        captured_drops(&captured)
            .map(|(_, drop)| drop)
            .map(|drop| drop.count)
            .collect::<Vec<_>>(),
        vec![6, 508, 508]
    );
    assert!(
        captured_drops(&captured)
            .map(|(_, drop)| drop)
            .all(|drop| drop.action_index == u32::from(u16::MAX))
    );
    assert!(
        captured_drops(&captured)
            .map(|(_, drop)| drop)
            .all(|drop| drop.available_at == TimestampNs(10))
    );
    assert_eq!(
        captured_drops(&captured)
            .map(|(_, drop)| drop)
            .map(|drop| drop.item_index)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    let replay_drop_events = ordinary
        .system_events
        .iter()
        .filter(|event| matches!(event, SystemEvent::EventsDropped { .. }))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        captured_drops(&captured)
            .map(|(_, drop)| drop)
            .map(|drop| drop.event.clone())
            .collect::<Vec<_>>(),
        replay_drop_events
    );
    assert_eq!(
        captured_actions(&captured).count() as u64
            + captured_drops(&captured).next().unwrap().1.count,
        1_030
    );
    assert!(captured.max_actions <= 1_024);
    assert!(captured.max_market_dispatch <= 4);
    assert!(captured.max_system_dispatch <= 4);
    assert_eq!(ordinary.market_batches.len(), 4);
    assert_eq!(
        ordinary
            .system_events
            .iter()
            .filter(|event| matches!(event, SystemEvent::ClockJump { .. }))
            .count(),
        4
    );
}

#[test]
fn mfr1_capture_rejects_nonmonotonic_availability_before_replay() {
    let recording = mfr1(&[(20, "SUB BTC-USD"), (19, "QUOTE 99.00 101.00")]);
    let mut machine = synthetic_machine();
    assert_eq!(
        OrderedReplayCapture::new(8, OverflowPolicy::FailEngine)
            .replay(&mut *machine, recording, TimestampNs(18))
            .unwrap_err(),
        CaptureError::AvailabilityRegression
    );
}

#[derive(Clone)]
struct CaptureMechanicsFixture {
    base: SnapshotReplayFixture,
    frame_system: SystemSourceKeyV1,
    lifecycle_system: SystemSourceKeyV1,
    connection_system: SystemSourceKeyV1,
}

fn capture_mechanics_fixture() -> CaptureMechanicsFixture {
    let base = snapshot_fixture();
    let target = ConfiguredTargetKeyV1::processor(base.config.processor_id()).unwrap();
    let frame_system = SystemSourceKeyV1::new(
        "z_frame_fault",
        FaultScopeKindV1::Processor,
        target,
        CursorModeV1::Derived,
    )
    .unwrap();
    let lifecycle_system = SystemSourceKeyV1::new(
        "y_lifecycle_fault",
        FaultScopeKindV1::Contributor,
        ConfiguredTargetKeyV1::contributor(base.contributor.clone()),
        CursorModeV1::Derived,
    )
    .unwrap();
    let connection_system = SystemSourceKeyV1::new(
        "zz_connection_fault",
        FaultScopeKindV1::ConnectionEpoch,
        ConfiguredTargetKeyV1::connection(base.config.connections()[0].clone()),
        CursorModeV1::Derived,
    )
    .unwrap();
    let config = MechanicsConfigV1::new(
        base.config.processor_id(),
        base.config.connections().to_vec(),
        base.config.contributors().to_vec(),
        base.config.contributor_connections().clone(),
        base.config.clock_sources().to_vec(),
        base.config.coverage_sources().to_vec(),
        vec![
            lifecycle_system.clone(),
            frame_system.clone(),
            connection_system.clone(),
        ],
    )
    .unwrap();
    CaptureMechanicsFixture {
        base: SnapshotReplayFixture {
            config,
            contributor: base.contributor,
            clock: base.clock,
        },
        frame_system,
        lifecycle_system,
        connection_system,
    }
}

fn derived_market(
    frame_ordinal: u64,
    action_index: u32,
    ns: i64,
    epoch: &str,
    generation: u8,
) -> MechanicsInputV1 {
    MechanicsInputV1::market(
        mechanics_frame_event(frame_ordinal, ns),
        action_index,
        snapshot_catalog(epoch, generation),
    )
    .unwrap()
}

fn mechanics_frame_event(frame_ordinal: u64, ns: i64) -> EventEnvelope {
    EventEnvelope {
        schema_version: 1,
        venue: VenueId(1),
        instrument: Some(InstrumentId(1)),
        connection: ConnectionId(7),
        session: SessionId(9),
        frame_seq: frame_ordinal,
        event_index: 0,
        exchange_ts: Some(TimestampNs(ns)),
        receive_ts: TimestampNs(ns),
        source_sequence: None,
        flags: EventFlags::empty(),
        payload: scaled_trade(10_000_000_000),
    }
}

fn contributor_drop_input(
    key: &SystemSourceKeyV1,
    fixture: &CaptureMechanicsFixture,
    cursor: CursorV1,
    ns: i64,
    count: u64,
    category: DropCategoryV1,
    predecessor: Option<&str>,
) -> MechanicsInputV1 {
    MechanicsInputV1::system(
        SystemSourceV1::new(key.clone(), "epoch_system_a", 0).unwrap(),
        FaultScopeV1::processor(fixture.base.config.processor_id()).unwrap(),
        time_ns(ns),
        time_ns(ns),
        cursor,
        SystemFaultV1::events_dropped(count, category).unwrap(),
        predecessor.map(str::to_owned),
    )
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
fn push_chained_fault(
    output: &mut Vec<MechanicsInputV1>,
    head: &mut Option<String>,
    fixture: &CaptureMechanicsFixture,
    cursor: CursorV1,
    ns: i64,
    count: u64,
    category: DropCategoryV1,
) {
    let input = contributor_drop_input(
        &fixture.frame_system,
        fixture,
        cursor,
        ns,
        count,
        category,
        head.as_deref(),
    );
    *head = Some(match head.as_deref() {
        Some(previous) => SystemChainPreimage::hash_next(previous, input.payload_hash()).unwrap(),
        None => SystemChainPreimage::hash_first(input.payload_hash()).unwrap(),
    });
    output.push(input);
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MechanicsMapError {
    ItemIndexOverflow(u32),
    Wire(String),
}

fn map_captured_frames(
    captured: &OrderedCaptureOutcome,
    fixture: &CaptureMechanicsFixture,
) -> Result<Vec<(Vec<MechanicsInputV1>, Vec<MechanicsInputV1>)>, MechanicsMapError> {
    let mut system_head = None;
    captured
        .replay_start
        .iter()
        .filter(|frame| frame_has_mechanics(frame))
        .chain(&captured.frames)
        .filter(|frame| frame_has_mechanics(frame))
        .map(|frame| -> Result<_, MechanicsMapError> {
            let mut ordinary = Vec::with_capacity(frame.ordinary.len());
            for record in &frame.ordinary {
                let input = match &record.action {
                    SessionAction::EmitBatch(batch) => {
                        let mut normalized =
                            batch.events[usize::try_from(record.item_index).unwrap()].clone();
                        normalized.frame_seq = frame.frame_seq;
                        normalized.receive_ts = frame.available_at;
                        normalized.event_index = u16::try_from(record.item_index)
                            .map_err(|_| MechanicsMapError::ItemIndexOverflow(record.item_index))?;
                        MechanicsInputV1::market(
                            normalized,
                            record.action_index,
                            snapshot_catalog("epoch_a", 0),
                        )
                        .map_err(|error| MechanicsMapError::Wire(error.to_string()))?
                    }
                    SessionAction::EmitSystem(SystemEvent::EventsDropped { count, .. }) => {
                        contributor_drop_input(
                            &fixture.frame_system,
                            fixture,
                            CursorV1::derived(
                                frame.frame_seq,
                                record.action_index,
                                record.item_index,
                            )
                            .unwrap(),
                            record.available_at.0,
                            *count,
                            DropCategoryV1::ActionBuffer,
                            system_head.as_deref(),
                        )
                    }
                    action => panic!("unmappable captured action: {action:?}"),
                };
                if matches!(record.action, SessionAction::EmitSystem(_)) {
                    system_head = Some(match system_head.as_deref() {
                        Some(head) => {
                            SystemChainPreimage::hash_next(head, input.payload_hash()).unwrap()
                        }
                        None => SystemChainPreimage::hash_first(input.payload_hash()).unwrap(),
                    });
                }
                ordinary.push(input);
            }
            let mut drops = Vec::with_capacity(frame.drops.len());
            for drop in &frame.drops {
                let category = match drop.category {
                    CapturedDropCategory::ActionBuffer => DropCategoryV1::ActionBuffer,
                    CapturedDropCategory::MarketDispatch => DropCategoryV1::MarketDispatch,
                    CapturedDropCategory::SystemDispatch => DropCategoryV1::SystemDispatch,
                };
                let input = contributor_drop_input(
                    &fixture.frame_system,
                    fixture,
                    CursorV1::derived_drop(frame.frame_seq, drop.item_index).unwrap(),
                    drop.available_at.0,
                    drop.count,
                    category,
                    system_head.as_deref(),
                );
                system_head = Some(match system_head.as_deref() {
                    Some(head) => {
                        SystemChainPreimage::hash_next(head, input.payload_hash()).unwrap()
                    }
                    None => SystemChainPreimage::hash_first(input.payload_hash()).unwrap(),
                });
                drops.push(input);
            }
            Ok((ordinary, drops))
        })
        .collect()
}

#[derive(Debug, PartialEq, Eq)]
enum StableMechanicsMapError {
    UnmappableSystemEvent(SystemEvent),
    ItemIndexOverflow(u32),
    Wire(String),
}

fn map_stable_synthetic_lifecycle(
    captured: &OrderedCaptureOutcome,
    fixture: &CaptureMechanicsFixture,
) -> Result<Vec<Vec<MechanicsInputV1>>, StableMechanicsMapError> {
    let mut target_generation = 0u8;
    let mut system_head: Option<String> = None;
    let mut connection_head: Option<String> = None;
    let mut output = Vec::with_capacity(captured.frames.len());
    for frame in &captured.frames {
        let mut inputs = Vec::new();
        for record in &frame.ordinary {
            let input = match &record.action {
                SessionAction::EmitBatch(batch) => {
                    let mut normalized =
                        batch.events[usize::try_from(record.item_index).unwrap()].clone();
                    // Synthetic has no exchange clock; the frozen offline adapter mapping uses
                    // receive time as its explicit normalized market timestamp. The market's
                    // native cursor remains intact; derived system/action coordinates come only
                    // from the authoritative captured raw group.
                    normalized.frame_seq = frame.frame_seq;
                    normalized.receive_ts = frame.available_at;
                    if normalized.exchange_ts.is_none() {
                        normalized.exchange_ts = Some(frame.available_at);
                    }
                    normalized.event_index = u16::try_from(record.item_index).map_err(|_| {
                        StableMechanicsMapError::ItemIndexOverflow(record.item_index)
                    })?;
                    Some(
                        MechanicsInputV1::market(
                            normalized,
                            record.action_index,
                            snapshot_catalog_for_venue(
                                batch.events[usize::try_from(record.item_index).unwrap()]
                                    .venue
                                    .0,
                                batch.events[usize::try_from(record.item_index).unwrap()]
                                    .connection
                                    .0,
                                batch.session.0,
                                if target_generation == 0 {
                                    "epoch_a"
                                } else {
                                    "epoch_b"
                                },
                                target_generation,
                            ),
                        )
                        .map_err(|error| StableMechanicsMapError::Wire(error.to_string()))?,
                    )
                }
                SessionAction::EmitSystem(event) => {
                    let fault = match event {
                        SystemEvent::SequenceGap { expected, actual } => {
                            SystemFaultV1::sequence_gap(*expected, *actual)
                        }
                        SystemEvent::BookInvalidated { .. } => SystemFaultV1::book_invalidated(),
                        SystemEvent::BookResynchronized { .. } => {
                            SystemFaultV1::book_resynchronized()
                        }
                        other => {
                            return Err(StableMechanicsMapError::UnmappableSystemEvent(
                                other.clone(),
                            ));
                        }
                    };
                    Some(lifecycle_system_input(
                        fixture,
                        target_generation,
                        frame.frame_seq,
                        record.action_index,
                        record.available_at.0,
                        fault,
                        &mut system_head,
                    ))
                }
                SessionAction::Reconnect(_) => {
                    let input = connection_system_input(
                        fixture,
                        target_generation,
                        frame.frame_seq,
                        record.action_index,
                        record.available_at.0,
                        SystemFaultV1::disconnected(),
                        &mut connection_head,
                    );
                    target_generation = target_generation.saturating_add(1);
                    system_head = None;
                    Some(input)
                }
                SessionAction::SendText(_)
                | SessionAction::SendSensitiveText(_)
                | SessionAction::SendBinary(_)
                | SessionAction::SendPing(_)
                | SessionAction::RequestHttp(_)
                | SessionAction::ScheduleTimer(_)
                | SessionAction::CancelTimer(_)
                | SessionAction::ResyncInstrument(_)
                | SessionAction::MarkLive
                | SessionAction::MarkDegraded
                | SessionAction::DisableSubscription(_)
                | SessionAction::StopSession(_) => None,
            };
            if let Some(input) = input {
                inputs.push(input);
            }
        }
        if !inputs.is_empty() {
            output.push(inputs);
        }
    }
    Ok(output)
}

#[allow(clippy::too_many_arguments)]
fn connection_system_input(
    fixture: &CaptureMechanicsFixture,
    generation: u8,
    frame_seq: u64,
    action_index: u32,
    ns: i64,
    fault: SystemFaultV1,
    head: &mut Option<String>,
) -> MechanicsInputV1 {
    let epoch = if generation == 0 {
        "epoch_a"
    } else {
        "epoch_b"
    };
    let input = MechanicsInputV1::system(
        SystemSourceV1::new(
            fixture.connection_system.clone(),
            "epoch_connection_system_a",
            0,
        )
        .unwrap(),
        FaultScopeV1::connection(
            fixture.base.config.connections()[0].clone(),
            epoch,
            generation,
        )
        .unwrap(),
        time_ns(ns),
        time_ns(ns),
        CursorV1::derived(frame_seq, action_index, 0).unwrap(),
        fault,
        head.clone(),
    )
    .unwrap();
    *head = Some(match head.as_deref() {
        Some(previous) => SystemChainPreimage::hash_next(previous, input.payload_hash()).unwrap(),
        None => SystemChainPreimage::hash_first(input.payload_hash()).unwrap(),
    });
    input
}

#[allow(clippy::too_many_arguments)]
fn lifecycle_system_input(
    fixture: &CaptureMechanicsFixture,
    target_generation: u8,
    frame_seq: u64,
    action_index: u32,
    ns: i64,
    fault: SystemFaultV1,
    head: &mut Option<String>,
) -> MechanicsInputV1 {
    let target = ContributorV1::new(
        fixture.base.contributor.clone(),
        if target_generation == 0 {
            "epoch_a"
        } else {
            "epoch_b"
        },
        target_generation,
    )
    .unwrap();
    let input = MechanicsInputV1::system(
        SystemSourceV1::new(fixture.lifecycle_system.clone(), "epoch_system_a", 0).unwrap(),
        FaultScopeV1::contributor(target),
        time_ns(ns),
        time_ns(ns),
        CursorV1::derived(frame_seq, action_index, 0).unwrap(),
        fault,
        head.clone(),
    )
    .unwrap_or_else(|error| {
        panic!(
            "lifecycle system mapping failed at frame {frame_seq} action {action_index}: {error:?}"
        )
    });
    *head = Some(match head.as_deref() {
        Some(previous) => SystemChainPreimage::hash_next(previous, input.payload_hash()).unwrap(),
        None => SystemChainPreimage::hash_first(input.payload_hash()).unwrap(),
    });
    input
}

#[derive(Debug, PartialEq)]
enum FrameSnapshotError {
    Unsealed,
    Snapshot(SnapshotError),
}

enum FrameInputState {
    Pending {
        ordinary: Vec<MechanicsInputV1>,
        drops: Vec<MechanicsInputV1>,
    },
    Sealed,
}

struct MechanicsFrameTransaction<'a> {
    processor: &'a mut MechanicsProcessor,
    state: FrameInputState,
}

impl<'a> MechanicsFrameTransaction<'a> {
    fn new(
        processor: &'a mut MechanicsProcessor,
        ordinary: Vec<MechanicsInputV1>,
        drops: Vec<MechanicsInputV1>,
    ) -> Self {
        Self {
            processor,
            state: FrameInputState::Pending { ordinary, drops },
        }
    }

    fn snapshot(&mut self, decision: Rfc3339Time) -> Result<AuthoredSnapshot, FrameSnapshotError> {
        match self.state {
            FrameInputState::Pending { .. } => Err(FrameSnapshotError::Unsealed),
            FrameInputState::Sealed => self
                .processor
                .snapshot(decision)
                .map_err(FrameSnapshotError::Snapshot),
        }
    }

    fn seal(&mut self) -> Result<Vec<IngestOutcome>, SnapshotError> {
        let FrameInputState::Pending { ordinary, drops } =
            std::mem::replace(&mut self.state, FrameInputState::Sealed)
        else {
            return Err(SnapshotError::InvalidInput(
                "MFR1 frame was already sealed".into(),
            ));
        };
        let mut outcomes = Vec::with_capacity(ordinary.len() + drops.len());
        for input in ordinary.iter().chain(&drops) {
            outcomes.push(self.processor.ingest(input)?);
        }
        Ok(outcomes)
    }
}

fn recovery_controls(
    fixture: &CaptureMechanicsFixture,
    first_available_ns: i64,
) -> Vec<MechanicsInputV1> {
    let contributor = ContributorV1::new(fixture.base.contributor.clone(), "epoch_b", 1).unwrap();
    let mut inputs = vec![
        MechanicsInputV1::clock(
            contributor.clone(),
            ClockSourceV1::new(fixture.base.clock.clone(), "epoch_clock_b", 1).unwrap(),
            time_ns(first_available_ns),
            time_ns(first_available_ns),
            ClockCursorV1::native(1, 1).unwrap(),
            ClockStateV1::Synchronized,
            CanonicalDecimal::parse("0.25", 18, 8).unwrap(),
            2_000,
            ClockQualityV1::Validated,
            "SOURCE_CLOCK_WITHIN_TOLERANCE",
        )
        .unwrap(),
    ];
    for (offset, key) in fixture.base.config.coverage_sources().iter().enumerate() {
        let available_at = first_available_ns + i64::try_from(offset + 1).unwrap() * 1_000;
        inputs.push(
            MechanicsInputV1::coverage(
                contributor.clone(),
                CoverageSourceV1::new(key.clone(), "epoch_coverage_b", 1).unwrap(),
                key.family(),
                time_ns(available_at - 60_000_000_000),
                time_ns(available_at),
                time_ns(available_at),
                CoverageCursorV1::native(1, 1).unwrap(),
            )
            .unwrap(),
        );
    }
    inputs
}

fn initialize_capture_processor(
    processor: &mut MechanicsProcessor,
    fixture: &CaptureMechanicsFixture,
) -> String {
    assert_eq!(
        processor.ingest(&derived_market(1, 0, 0, "epoch_a", 0)),
        Ok(IngestOutcome::AcceptedWarming)
    );
    assert_eq!(
        processor.ingest(&derived_market(2, 0, 60_000_000_000, "epoch_a", 0,)),
        Ok(IngestOutcome::AcceptedLive)
    );
    let controls = snapshot_controls(&fixture.base, 60_080_000_000, 1);
    for input in &controls {
        processor.ingest(input).unwrap();
    }
    processor
        .snapshot(time_ns(60_080_003_000))
        .unwrap()
        .canonical_json()
}

struct MechanicsOverflowMachine {
    frame_seq: u64,
}

struct TwoFrameOverflowMachine {
    frame_seqs: std::collections::VecDeque<u64>,
}

struct MarketAuthorityMachine {
    frame_seq: u64,
    receive_ts: TimestampNs,
    exchange_ts: TimestampNs,
}

struct MultiEventMachine;

impl SessionMachine for MultiEventMachine {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        let SessionInput::TextFrame { received, .. } = input else {
            return Ok(());
        };
        output.push(SessionAction::EmitBatch(EventBatch {
            session: SessionId(9),
            frame_seq: 1,
            events: vec![
                mechanics_frame_event(1, received.receive_ts.0),
                mechanics_frame_event(1, received.receive_ts.0),
            ],
        }));
        Ok(())
    }
}

struct ReplayStartOverflowMachine;

impl SessionMachine for ReplayStartOverflowMachine {
    fn on_replay_start(
        &mut self,
        now: TimestampNs,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        for _ in 0..1_025 {
            output.push(SessionAction::EmitBatch(EventBatch {
                session: SessionId(9),
                frame_seq: 0,
                events: vec![mechanics_frame_event(0, now.0)],
            }));
        }
        Ok(())
    }

    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        let SessionInput::TextFrame { received, .. } = input else {
            return Ok(());
        };
        output.push(SessionAction::EmitBatch(EventBatch {
            session: SessionId(9),
            frame_seq: 9,
            events: vec![mechanics_frame_event(9, received.receive_ts.0)],
        }));
        Ok(())
    }
}

#[test]
fn replay_start_is_an_authoritative_sealed_mechanics_group_before_raw_frames() {
    let connect_at = TimestampNs(60_100_000_000);
    let raw_at = connect_at.0 + 1_000;
    let recording = mfr1_records(&[(9, raw_at, FrameOpcode::Text, b"RAW".to_vec())]);
    let mut capture_machine = ReplayStartOverflowMachine;
    let captured = OrderedReplayCapture::new(2_048, OverflowPolicy::DropNewest)
        .replay(&mut capture_machine, recording.clone(), connect_at)
        .unwrap();
    let mut replay_machine = ReplayStartOverflowMachine;
    let ordinary = ReplayRunner::with_overflow(2_048, OverflowPolicy::DropNewest)
        .replay_bytes(&mut replay_machine, recording, connect_at)
        .unwrap();

    let start = captured.replay_start.as_ref().unwrap();
    assert_eq!(start.frame_seq, 0);
    assert_eq!(start.available_at, connect_at);
    assert_eq!(start.ordinary.len(), 1_024);
    assert_eq!(start.drops.len(), 1);
    assert_eq!(start.drops[0].category, CapturedDropCategory::ActionBuffer);
    assert_eq!(start.drops[0].count, 1);
    assert_eq!(captured.frames[0].frame_seq, 9);
    assert_eq!(captured.market_batches, ordinary.market_batches);
    assert_eq!(
        captured_drops(&captured)
            .map(|(_, drop)| drop.count)
            .sum::<u64>(),
        ordinary.dropped_events
    );

    let fixture = capture_mechanics_fixture();
    let mapped = map_captured_frames(&captured, &fixture).unwrap();
    assert_eq!(mapped.len(), 2);
    assert!(matches!(mapped[0].0[0].view(),
        marketfeed_event_pulse::wire::MechanicsInputRefV1::Market { envelope, .. }
        if envelope.frame_seq == 0 && envelope.receive_ts == connect_at));
    assert!(matches!(mapped[0].1[0].view(),
        marketfeed_event_pulse::wire::MechanicsInputRefV1::System { system_cursor, .. }
        if system_cursor == &CursorV1::derived_drop(0, 0).unwrap()));

    let mut processor = MechanicsProcessor::new(fixture.base.config, snapshot_authoring()).unwrap();
    let mut start_transaction =
        MechanicsFrameTransaction::new(&mut processor, mapped[0].0.clone(), mapped[0].1.clone());
    start_transaction.seal().unwrap();
    assert_eq!(
        start_transaction.snapshot(time_ns(connect_at.0)),
        Err(FrameSnapshotError::Snapshot(
            SnapshotError::MissingClockEvidence
        ))
    );
    drop(start_transaction);
    assert!(processor.ingest(&mapped[1].0[0]).is_err());
}

impl SessionMachine for MarketAuthorityMachine {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if matches!(input, SessionInput::TextFrame { .. }) {
            let mut event = mechanics_frame_event(self.frame_seq, self.receive_ts.0);
            event.exchange_ts = Some(self.exchange_ts);
            output.push(SessionAction::EmitBatch(EventBatch {
                session: SessionId(9),
                frame_seq: self.frame_seq,
                events: vec![event],
            }));
        }
        Ok(())
    }
}

#[test]
fn raw_market_authority_normalizes_independent_adapter_coordinates() {
    let recording = mfr1_records(&[(77, 100, FrameOpcode::Text, b"MARKET".to_vec())]);
    let mut independent = MarketAuthorityMachine {
        frame_seq: 78,
        receive_ts: TimestampNs(101),
        exchange_ts: TimestampNs(100),
    };
    let captured = OrderedReplayCapture::new(8, OverflowPolicy::FailEngine)
        .replay(&mut independent, recording, TimestampNs(99))
        .unwrap();
    assert_eq!(captured.frames[0].frame_seq, 77);
    assert_eq!(
        captured.frames[0].ordinary[0].available_at,
        TimestampNs(100)
    );
    let fixture = capture_mechanics_fixture();
    let mapped = map_captured_frames(&captured, &fixture).unwrap();
    assert!(matches!(mapped[0].0[0].view(),
        marketfeed_event_pulse::wire::MechanicsInputRefV1::Market { envelope, .. }
        if envelope.frame_seq == 77 && envelope.receive_ts == TimestampNs(100)));
    assert_eq!(
        mapped[0].0[0].payload_hash(),
        derived_market(77, 0, 100, "epoch_a", 0).payload_hash()
    );

    let command = SessionCommand::Subscribe(vec!["BTC-USD".into()]);
    let wire = SubscriptionWireAction::Text(b"SUB BTC-USD\n".as_slice().to_vec().into());
    let engine_shaped = mfr1_records(&[
        (
            1,
            200,
            FrameOpcode::SubscriptionCommand,
            encode_subscription_command(&command, &wire).unwrap(),
        ),
        (
            2,
            201,
            FrameOpcode::Text,
            b"BOOK_SNAP 10 BID 100.00:1.000 ASK 101.00:1.000".to_vec(),
        ),
    ]);
    let mut synthetic = synthetic_machine();
    let mut captured = OrderedReplayCapture::new(8, OverflowPolicy::FailEngine)
        .replay(&mut *synthetic, engine_shaped, TimestampNs(199))
        .unwrap();
    assert!(matches!(captured.frames[1].ordinary[0].action,
        SessionAction::EmitBatch(ref batch) if batch.events[0].frame_seq == 1));
    let SessionAction::EmitBatch(batch) = &mut captured.frames[1].ordinary[0].action else {
        unreachable!();
    };
    batch.events[0].exchange_ts = Some(TimestampNs(150));
    let mapped = map_stable_synthetic_lifecycle(&captured, &fixture).unwrap();
    assert_eq!(mapped.len(), 1);
    assert!(matches!(mapped[0][0].view(),
        marketfeed_event_pulse::wire::MechanicsInputRefV1::Market { envelope, .. }
        if envelope.frame_seq == 2
            && envelope.receive_ts == TimestampNs(201)
            && envelope.exchange_ts == Some(TimestampNs(150))
            && envelope.source_sequence == Some(SequenceRange { first: 10, last: 10 })));
    assert_eq!(
        mapped[0][0].expected_payload_hash().unwrap(),
        mapped[0][0].payload_hash()
    );
}

#[test]
fn market_items_receive_distinct_authoritative_event_indexes_and_bounded_conversion() {
    let mut machine = MultiEventMachine;
    let captured = OrderedReplayCapture::new(8, OverflowPolicy::FailEngine)
        .replay(
            &mut machine,
            mfr1_records(&[(77, 100, FrameOpcode::Text, b"TWO".to_vec())]),
            TimestampNs(99),
        )
        .unwrap();
    assert!(matches!(captured.frames[0].ordinary[0].action,
        SessionAction::EmitBatch(ref batch)
            if batch.events[0].event_index == 0 && batch.events[1].event_index == 0));
    let fixture = capture_mechanics_fixture();
    let mapped = map_captured_frames(&captured, &fixture).unwrap();
    assert_eq!(mapped[0].0.len(), 2);
    assert!(matches!(mapped[0].0[0].view(),
        marketfeed_event_pulse::wire::MechanicsInputRefV1::Market { envelope, .. }
            if envelope.frame_seq == 77 && envelope.event_index == 0));
    assert!(matches!(mapped[0].0[1].view(),
        marketfeed_event_pulse::wire::MechanicsInputRefV1::Market { envelope, .. }
            if envelope.frame_seq == 77 && envelope.event_index == 1));
    assert_ne!(mapped[0].0[0].payload_hash(), mapped[0].0[1].payload_hash());
    let mapped_cursors = mapped[0]
        .0
        .iter()
        .map(|input| match input.view() {
            marketfeed_event_pulse::wire::MechanicsInputRefV1::Market {
                envelope,
                action_index,
                ..
            } => CursorV1::derived(
                envelope.frame_seq,
                action_index,
                u32::from(envelope.event_index),
            )
            .unwrap(),
            _ => unreachable!(),
        })
        .collect::<Vec<_>>();
    assert_eq!(mapped_cursors[0], CursorV1::derived(77, 0, 0).unwrap());
    assert_eq!(mapped_cursors[1], CursorV1::derived(77, 0, 1).unwrap());
    assert_ne!(mapped_cursors[0], mapped_cursors[1]);

    let oversized_batch = EventBatch {
        session: SessionId(9),
        frame_seq: 1,
        events: vec![mechanics_frame_event(1, 100); usize::from(u16::MAX) + 2],
    };
    let overflow = OrderedCaptureOutcome {
        frames: vec![CapturedFrame {
            frame_seq: 78,
            available_at: TimestampNs(100),
            ordinary: vec![CapturedAction {
                action_index: 0,
                item_index: u32::from(u16::MAX) + 1,
                available_at: TimestampNs(100),
                action: SessionAction::EmitBatch(oversized_batch),
            }],
            drops: vec![],
        }],
        ..OrderedCaptureOutcome::default()
    };
    assert_eq!(
        map_captured_frames(&overflow, &fixture),
        Err(MechanicsMapError::ItemIndexOverflow(
            u32::from(u16::MAX) + 1
        ))
    );
}

#[test]
fn action_producing_frame_sequence_reserves_zero_and_rejects_reuse_or_regression() {
    let mut machine = MarketAuthorityMachine {
        frame_seq: 1,
        receive_ts: TimestampNs(100),
        exchange_ts: TimestampNs(100),
    };
    assert_eq!(
        OrderedReplayCapture::new(8, OverflowPolicy::FailEngine).replay(
            &mut machine,
            mfr1_records(&[(0, 100, FrameOpcode::Text, b"ZERO".to_vec())]),
            TimestampNs(99),
        ),
        Err(CaptureError::MechanicsFrameZero)
    );
    for (first, second, expected) in [
        (
            1,
            1,
            CaptureError::MechanicsFrameRegression {
                previous: 1,
                current: 1,
            },
        ),
        (
            2,
            1,
            CaptureError::MechanicsFrameRegression {
                previous: 2,
                current: 1,
            },
        ),
    ] {
        let mut machine = MarketAuthorityMachine {
            frame_seq: first,
            receive_ts: TimestampNs(100),
            exchange_ts: TimestampNs(100),
        };
        assert_eq!(
            OrderedReplayCapture::new(8, OverflowPolicy::FailEngine).replay(
                &mut machine,
                mfr1_records(&[
                    (first, 100, FrameOpcode::Text, b"FIRST".to_vec()),
                    (second, 101, FrameOpcode::Text, b"SECOND".to_vec()),
                ]),
                TimestampNs(99),
            ),
            Err(expected)
        );
    }
}

impl SessionMachine for TwoFrameOverflowMachine {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        let SessionInput::TextFrame { received, .. } = input else {
            return Ok(());
        };
        let frame_seq = self.frame_seqs.pop_front().unwrap();
        let count = if self.frame_seqs.len() == 1 { 1_025 } else { 1 };
        for _ in 0..count {
            output.push(SessionAction::EmitBatch(EventBatch {
                session: SessionId(9),
                frame_seq,
                events: vec![mechanics_frame_event(frame_seq, received.receive_ts.0)],
            }));
        }
        Ok(())
    }
}

#[test]
fn frame_groups_seal_drops_before_later_evidence_and_preserve_raw_frame_seq() {
    let first_ns = 60_100_000_000;
    let second_ns = first_ns + 1_000;
    let recording = mfr1_records(&[
        (41, first_ns, FrameOpcode::Text, b"OVERFLOW".to_vec()),
        (99, second_ns, FrameOpcode::Text, b"RETAINED".to_vec()),
    ]);
    let mut machine = TwoFrameOverflowMachine {
        frame_seqs: std::collections::VecDeque::from([41, 99]),
    };
    let captured = OrderedReplayCapture::new(2_048, OverflowPolicy::DropNewest)
        .replay(&mut machine, recording, TimestampNs(first_ns - 1))
        .unwrap();

    assert_eq!(
        captured
            .frames
            .iter()
            .map(|frame| frame.frame_seq)
            .collect::<Vec<_>>(),
        vec![41, 99]
    );
    assert_eq!(captured.frames[0].drops.len(), 1);
    assert!(captured.frames[1].drops.is_empty());
    let fixture = capture_mechanics_fixture();
    let mapped = map_captured_frames(&captured, &fixture).unwrap();
    assert_eq!(mapped.len(), 2);
    assert!(
        matches!(mapped[0].1[0].view(), marketfeed_event_pulse::wire::MechanicsInputRefV1::System { system_cursor, .. }
        if system_cursor == &CursorV1::derived_drop(41, 0).unwrap())
    );
    assert!(
        matches!(mapped[1].0[0].view(), marketfeed_event_pulse::wire::MechanicsInputRefV1::Market { envelope, .. }
        if envelope.frame_seq == 99)
    );
    assert_eq!(
        mapped[1].0[0].payload_hash(),
        derived_market(99, 0, second_ns, "epoch_a", 0).payload_hash()
    );

    let mut processor =
        MechanicsProcessor::new(fixture.base.config.clone(), snapshot_authoring()).unwrap();
    initialize_capture_processor(&mut processor, &fixture);
    let mut first =
        MechanicsFrameTransaction::new(&mut processor, mapped[0].0.clone(), mapped[0].1.clone());
    first.seal().unwrap();
    let invalid = first.snapshot(time_ns(first_ns)).unwrap();
    assert_eq!(invalid.value()["quality_state"], "INVALID");
    assert!(
        invalid.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "QUEUE_DROP")
    );
    drop(first);

    let mut second =
        MechanicsFrameTransaction::new(&mut processor, mapped[1].0.clone(), mapped[1].1.clone());
    let outcomes = second.seal();
    assert!(
        matches!(
            &outcomes,
            Err(SnapshotError::InvalidInput(message))
                if message == "source epoch regressed or changed without a greater generation"
        ),
        "{outcomes:?}"
    );
    let still_invalid = second.snapshot(time_ns(second_ns)).unwrap();
    assert_eq!(still_invalid.value()["quality_state"], "INVALID");
}

#[test]
fn synthetic_faults_map_to_typed_mechanics_and_recover_warming_to_live() {
    let command = SessionCommand::Subscribe(vec!["BTC-USD".into()]);
    let wire = SubscriptionWireAction::Text(b"SUB BTC-USD\n".as_slice().to_vec().into());
    let control = encode_subscription_command(&command, &wire).unwrap();
    let live_at = 60_000_000_000;
    let recording = mfr1_records(&[
        (1, 1, FrameOpcode::SubscriptionCommand, control),
        (
            2,
            1_000,
            FrameOpcode::Text,
            b"BOOK_SNAP 1 BID 100.00:1.000 ASK 101.00:1.000".to_vec(),
        ),
        (
            3,
            live_at,
            FrameOpcode::Text,
            b"TRADE 2 100.00 1.000 BUY warm-live".to_vec(),
        ),
        (
            4,
            live_at + 500_000,
            FrameOpcode::Text,
            b"TRADE 3 100.50 0.500 BUY duplicate".to_vec(),
        ),
        (
            5,
            live_at + 1_000_000,
            FrameOpcode::Text,
            b"TRADE 3 100.75 0.750 BUY duplicate-mutated".to_vec(),
        ),
        (
            6,
            live_at + 1_500_000,
            FrameOpcode::Text,
            b"BOOK_DELTA 13 ASK DELETE 101.00".to_vec(),
        ),
        (
            7,
            live_at + 2_000_000,
            FrameOpcode::Text,
            b"BOOK_SNAP 20 BID 98.00:1.000 ASK 103.00:1.000".to_vec(),
        ),
        (
            8,
            live_at + 60_002_000_000,
            FrameOpcode::Text,
            b"BOOK_SNAP 21 BID 97.00:1.000 ASK 104.00:1.000".to_vec(),
        ),
    ]);
    let mut machine = synthetic_machine();
    let captured = OrderedReplayCapture::new(64, OverflowPolicy::FailEngine)
        .replay(&mut *machine, recording, TimestampNs(0))
        .unwrap();
    let fixture = capture_mechanics_fixture();
    let mapped = map_stable_synthetic_lifecycle(&captured, &fixture).unwrap();
    let mapped_flat = mapped.iter().flatten().cloned().collect::<Vec<_>>();
    assert!(mapped_flat.iter().any(|input| matches!(input.view(),
        marketfeed_event_pulse::wire::MechanicsInputRefV1::System { fault, .. }
            if matches!(fault.view(), marketfeed_event_pulse::wire::SystemFaultRefV1::SequenceGap { expected: 2, actual: 13 })
    )));
    assert!(mapped_flat.iter().any(|input| matches!(input.view(),
        marketfeed_event_pulse::wire::MechanicsInputRefV1::System { fault, .. }
            if matches!(fault.view(), marketfeed_event_pulse::wire::SystemFaultRefV1::BookInvalidated)
    )));
    assert!(mapped_flat.iter().any(|input| matches!(input.view(),
        marketfeed_event_pulse::wire::MechanicsInputRefV1::System { fault, .. }
            if matches!(fault.view(), marketfeed_event_pulse::wire::SystemFaultRefV1::BookResynchronized)
    )));

    let mut authored = Vec::new();
    for (index, frame) in mapped.into_iter().enumerate() {
        authored.extend(frame);
        if index == 1 {
            authored.extend(snapshot_controls(&fixture.base, live_at + 100_000, 1));
        }
    }
    let final_controls_at = live_at + 60_015_000_000;
    authored.extend(recovery_controls(&fixture, final_controls_at));

    let mut direct =
        MechanicsProcessor::new(fixture.base.config.clone(), snapshot_authoring()).unwrap();
    let direct_results = authored
        .iter()
        .map(|input| direct.ingest(input))
        .collect::<Vec<_>>();
    let errors = direct_results
        .iter()
        .filter_map(|result| result.as_ref().err())
        .collect::<Vec<_>>();
    assert_eq!(
        errors,
        vec![&SnapshotError::InvalidInput(
            "cursor coordinate was reused with different payload".into()
        )]
    );
    assert!(
        direct_results
            .iter()
            .filter(|result| matches!(result, Ok(IngestOutcome::AcceptedWarming)))
            .count()
            >= 2
    );
    assert!(
        direct_results
            .iter()
            .filter(|result| matches!(result, Ok(IngestOutcome::AcceptedLive)))
            .count()
            >= 2
    );
    assert!(
        direct_results
            .iter()
            .any(|result| matches!(result, Ok(IngestOutcome::Invalidated)))
    );
    let decision = final_controls_at + 3_000;
    let direct_snapshot = direct.snapshot(time_ns(decision)).unwrap();
    let book = direct_snapshot.value()["features"]
        .as_array()
        .unwrap()
        .iter()
        .find(|feature| feature["name"] == "book_depth_10bps")
        .unwrap();
    assert_eq!(book["quality_state"], "UNAVAILABLE");
    assert!(
        direct_snapshot.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "BOOK_RESYNCING")
    );

    let mut writer = EpinJson1Writer::new(Vec::new());
    for input in &authored {
        writer.write_input(input).unwrap();
    }
    let bytes = writer.finish();
    let reconstructed = EpinJson1Reader::new(bytes.as_slice(), time_ns(decision))
        .read_all()
        .unwrap();
    let mut independent =
        MechanicsProcessor::new(fixture.base.config, snapshot_authoring()).unwrap();
    let independent_results = reconstructed
        .iter()
        .map(|input| independent.ingest(input))
        .collect::<Vec<_>>();
    assert_eq!(direct_results, independent_results);
    let independent_snapshot = independent.snapshot(time_ns(decision)).unwrap();
    assert_eq!(
        direct_snapshot.canonical_json(),
        independent_snapshot.canonical_json()
    );
    assert_eq!(
        direct_snapshot.content_hash(),
        independent_snapshot.content_hash()
    );
}

impl SessionMachine for MechanicsOverflowMachine {
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
        let SessionInput::TextFrame { received, .. } = input else {
            return Ok(());
        };
        for action_index in 0..1_022u64 {
            output.push(SessionAction::EmitBatch(EventBatch {
                session: SessionId(9),
                frame_seq: self.frame_seq,
                events: vec![mechanics_frame_event(self.frame_seq, received.receive_ts.0)],
            }));
            assert_eq!(action_index + 1, output.len() as u64);
        }
        output.push(SessionAction::EmitSystem(SystemEvent::EventsDropped {
            count: 1,
            detail: "ordinary zero".into(),
        }));
        output.push(SessionAction::EmitSystem(SystemEvent::EventsDropped {
            count: 1,
            detail: "ordinary one".into(),
        }));
        output.push(SessionAction::EmitBatch(EventBatch {
            session: SessionId(9),
            frame_seq: self.frame_seq,
            events: vec![mechanics_frame_event(self.frame_seq, received.receive_ts.0)],
        }));
        Ok(())
    }
}

#[test]
fn captured_frame_maps_to_atomic_mechanics_faults_and_greater_generation_recovery() {
    let frame_ns = 60_100_000_000;
    let frame_seq = 73;
    let recording = mfr1_records(&[(
        frame_seq,
        frame_ns,
        FrameOpcode::Text,
        b"MECHANICS_OVERFLOW".to_vec(),
    )]);
    let mut machine = MechanicsOverflowMachine { frame_seq };
    let captured = OrderedReplayCapture::new(1, OverflowPolicy::DropNewest)
        .replay(&mut machine, recording, TimestampNs(frame_ns - 1))
        .unwrap();
    let fixture = capture_mechanics_fixture();
    let mut mapped = map_captured_frames(&captured, &fixture).unwrap();
    assert_eq!(mapped.len(), 1);
    let (ordinary, drops) = mapped.remove(0);
    assert_eq!(drops.len(), 3);
    assert!(
        drops
            .iter()
            .zip(&captured.frames[0].drops)
            .all(|(input, drop)| {
                match input.view() {
                    marketfeed_event_pulse::wire::MechanicsInputRefV1::System {
                        available_at,
                        ..
                    } => available_at.utc_micros() == drop.available_at.0.div_euclid(1_000),
                    _ => false,
                }
            })
    );

    let mut processor =
        MechanicsProcessor::new(fixture.base.config.clone(), snapshot_authoring()).unwrap();
    let baseline = initialize_capture_processor(&mut processor, &fixture);
    let mut frame = MechanicsFrameTransaction::new(&mut processor, ordinary.clone(), drops.clone());
    assert_eq!(
        frame.snapshot(time_ns(frame_ns)),
        Err(FrameSnapshotError::Unsealed),
        "the real replay authoring boundary must reject the incomplete frame"
    );
    let outcomes = frame.seal().unwrap();
    assert!(
        outcomes[outcomes.len() - drops.len()..]
            .iter()
            .all(|outcome| *outcome == IngestOutcome::Invalidated)
    );
    let invalidated = frame.snapshot(time_ns(frame_ns)).unwrap();
    assert_eq!(invalidated.value()["quality_state"], "INVALID");
    assert!(
        invalidated.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "QUEUE_DROP")
    );
    drop(frame);

    let recovery = [
        derived_market(4, 0, frame_ns + 1_000_000, "epoch_b", 1),
        derived_market(5, 0, frame_ns + 60_001_000_000, "epoch_b", 1),
    ];
    assert_eq!(
        processor.ingest(&recovery[0]),
        Ok(IngestOutcome::AcceptedWarming)
    );
    assert_eq!(
        processor.ingest(&recovery[1]),
        Ok(IngestOutcome::AcceptedLive)
    );
    let controls_at = frame_ns + 60_010_000_000;
    let controls = recovery_controls(&fixture, controls_at);
    for input in &controls {
        processor.ingest(input).unwrap();
    }
    let decision = controls_at + 3_000;
    let recovered = processor.snapshot(time_ns(decision)).unwrap();
    assert_eq!(recovered.value()["quality_state"], "INVALID");
    assert!(
        recovered.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "QUEUE_DROP")
    );

    let mut independently_reconstructed = Vec::new();
    independently_reconstructed.extend(
        (0..1_022u32).map(|action| derived_market(frame_seq, action, frame_ns, "epoch_a", 0)),
    );
    let mut independent_head = None;
    push_chained_fault(
        &mut independently_reconstructed,
        &mut independent_head,
        &fixture,
        CursorV1::derived(frame_seq, 1_022, 0).unwrap(),
        frame_ns,
        1,
        DropCategoryV1::ActionBuffer,
    );
    push_chained_fault(
        &mut independently_reconstructed,
        &mut independent_head,
        &fixture,
        CursorV1::derived(frame_seq, 1_023, 0).unwrap(),
        frame_ns,
        1,
        DropCategoryV1::ActionBuffer,
    );
    for (index, (count, category)) in [
        (1, DropCategoryV1::ActionBuffer),
        (1_021, DropCategoryV1::MarketDispatch),
        (1, DropCategoryV1::SystemDispatch),
    ]
    .into_iter()
    .enumerate()
    {
        push_chained_fault(
            &mut independently_reconstructed,
            &mut independent_head,
            &fixture,
            CursorV1::derived_drop(frame_seq, u32::try_from(index).unwrap()).unwrap(),
            frame_ns,
            count,
            category,
        );
    }
    assert_eq!(
        independently_reconstructed,
        ordinary.iter().chain(&drops).cloned().collect::<Vec<_>>()
    );
    let mut independent =
        MechanicsProcessor::new(fixture.base.config.clone(), snapshot_authoring()).unwrap();
    assert_eq!(
        initialize_capture_processor(&mut independent, &fixture),
        baseline
    );
    for input in &independently_reconstructed {
        independent.ingest(input).unwrap();
    }
    let independent_invalidated = independent.snapshot(time_ns(frame_ns)).unwrap();
    assert_eq!(
        invalidated.canonical_json(),
        independent_invalidated.canonical_json()
    );
    assert_eq!(
        invalidated.content_hash(),
        independent_invalidated.content_hash()
    );
    for input in recovery.iter().chain(&controls) {
        independent.ingest(input).unwrap();
    }
    let comparison = independent.snapshot(time_ns(decision)).unwrap();
    assert_eq!(recovered.canonical_json(), comparison.canonical_json());
    assert_eq!(recovered.content_hash(), comparison.content_hash());
}

#[derive(Default)]
struct HttpResponseMachine {
    seen: Option<(u64, u16, Vec<u8>)>,
}

impl SessionMachine for HttpResponseMachine {
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
            ..
        } = input
        {
            self.seen = Some((request_id, response.status, response.body.to_vec()));
            output.push(SessionAction::MarkLive);
        }
        Ok(())
    }
}

#[test]
fn mfr1_capture_applies_http_response_and_rejects_frames_before_connect_at() {
    let response = HttpResponse {
        status: 206,
        headers: vec![("content-type".into(), "application/json".into())],
        body: Vec::from(&b"partial"[..]).into(),
    };
    let payload = encode_http_response(77, &response).unwrap();
    let mut writer = RawSegmentWriter::create(Vec::new(), 100).unwrap();
    writer
        .write_record(
            SessionId(1),
            1,
            101,
            101,
            Direction::Inbound,
            FrameOpcode::HttpResponse,
            0,
            &payload,
        )
        .unwrap();
    let recording = writer.into_inner();

    let mut machine = HttpResponseMachine::default();
    let outcome = OrderedReplayCapture::new(8, OverflowPolicy::FailEngine)
        .replay(&mut machine, recording, TimestampNs(100))
        .unwrap();
    assert_eq!(machine.seen, Some((77, 206, b"partial".to_vec())));
    assert_eq!(outcome.frames_applied, 1);
    assert!(
        captured_actions(&outcome)
            .map(|(_, record)| record)
            .any(|record| matches!(record.action, SessionAction::MarkLive))
    );

    let early = mfr1(&[(99, "SUB BTC-USD")]);
    let mut synthetic = synthetic_machine();
    assert_eq!(
        OrderedReplayCapture::new(8, OverflowPolicy::FailEngine)
            .replay(&mut *synthetic, early, TimestampNs(100))
            .unwrap_err(),
        CaptureError::AvailabilityRegression
    );
}
