use std::collections::BTreeMap;

use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, ConcreteSubscriptionSet, EventBatch, SessionAction, SessionInput,
    SessionMachine, SessionSpec, VenueFactory,
};
use marketfeed_adapter_synthetic::SyntheticFactory;
use marketfeed_dispatch::{EventDispatcher, PushOutcome};
use marketfeed_event_pulse::{
    EpinJson1Reader, EpinJson1Writer, ReplayInputError,
    snapshot::MechanicsProcessor,
    wire::{
        CanonicalDecimal, ClockCursorV1, ClockQualityV1, ClockSourceKeyV1, ClockSourceV1,
        ClockStateV1, ConfiguredTargetKeyV1, ConnectionKeyV1, ContributorKeyV1, ContributorRoleV1,
        ContributorSpecV1, ContributorV1, CoverageCursorV1, CoverageSourceKeyV1, CoverageSourceV1,
        CursorModeV1, CursorV1, DropCategoryV1, FamilyV1, FaultScopeKindV1, FaultScopeV1,
        InstrumentIdentityV1, MechanicsConfigV1, MechanicsInputV1, ReplayCatalogV1,
        ReplayEpochEntryV1, Rfc3339Time, SnapshotAuthoringV1, SystemFaultV1, SystemSourceKeyV1,
        SystemSourceV1, VenueCatalogEntryV1,
    },
};
use marketfeed_model::{
    AggressorSide, BookChange, BookDelta, BookLevel, BookOperation, BookSide, BookSnapshot,
    CatalogVersion, CatalogView, ConnectionId, EventEnvelope, EventFlags, Fixed, FrameStamp,
    InstrumentId, MarketEvent, OverflowPolicy, Price, Quantity, Quote, SequenceRange, SessionId,
    SystemEvent, TimestampNs, Trade, VenueId,
};
use marketfeed_recording::{Direction, FrameOpcode, RawSegmentReader, RawSegmentWriter};
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
    ReplayCatalogV1::new(
        BTreeMap::from([(
            1,
            VenueCatalogEntryV1::new("HYPERLIQUID", "hyperliquid_source").unwrap(),
        )]),
        BTreeMap::from([(
            1,
            InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "HYPERLIQUID", "BNB-USDC")
                .unwrap(),
        )]),
        vec![ReplayEpochEntryV1::new(7, 9, epoch, generation).unwrap()],
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct CapturedAction {
    frame_ordinal: u64,
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
    frame_ordinal: u64,
    action_index: u32,
    item_index: u32,
    category: CapturedDropCategory,
    count: u64,
    event: SystemEvent,
}

#[derive(Debug, Default)]
struct OrderedCaptureOutcome {
    ordinary: Vec<CapturedAction>,
    drops: Vec<CapturedDrop>,
    market_batches: Vec<EventBatch>,
    system_events: Vec<SystemEvent>,
    frames_applied: u64,
    pre_clear_snapshot_rejections: u64,
    max_actions: usize,
    max_market_dispatch: usize,
    max_system_dispatch: usize,
}

/// Test-only MFR1 oracle. It intentionally captures actions before ReplayRunner lane separation.
struct OrderedReplayCapture {
    actions: ActionBuffer,
    dispatch: EventDispatcher,
    last_available_at: Option<TimestampNs>,
    frame_open: bool,
}

impl OrderedReplayCapture {
    fn new(dispatch_capacity: usize, overflow: OverflowPolicy) -> Self {
        Self {
            actions: ActionBuffer::new(),
            dispatch: EventDispatcher::new(dispatch_capacity, dispatch_capacity, overflow),
            last_available_at: None,
            frame_open: false,
        }
    }

    fn snapshot_boundary(&self) -> Result<(), &'static str> {
        if self.frame_open {
            Err("frame is not sealed")
        } else {
            Ok(())
        }
    }

    fn replay(
        &mut self,
        machine: &mut dyn SessionMachine,
        bytes: Vec<u8>,
        connect_at: TimestampNs,
        exercise_pre_clear_rejection: bool,
    ) -> Result<OrderedCaptureOutcome, String> {
        let mut outcome = OrderedCaptureOutcome::default();
        self.begin_frame(
            machine,
            0,
            connect_at,
            |machine, actions| machine.on_replay_start(connect_at, actions),
            &mut outcome,
            false,
        )?;

        let mut reader = RawSegmentReader::from_bytes(bytes).map_err(|error| error.to_string())?;
        for record in reader.read_all().map_err(|error| error.to_string())? {
            if record.header.direction != Direction::Inbound {
                continue;
            }
            if !matches!(
                record.header.opcode,
                FrameOpcode::Text | FrameOpcode::Binary | FrameOpcode::Pong
            ) {
                continue;
            }
            let available_at = TimestampNs(record.header.receive_ts_ns);
            if self
                .last_available_at
                .is_some_and(|last| available_at < last)
            {
                return Err("MFR1 availability regressed".into());
            }
            self.last_available_at = Some(available_at);
            let frame_ordinal = outcome.frames_applied + 1;
            let stamp = FrameStamp {
                receive_ts: available_at,
                mono_ns: record.header.monotonic_ns,
            };
            let opcode = record.header.opcode;
            let mut payload = record.payload;
            self.begin_frame(
                machine,
                frame_ordinal,
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
                exercise_pre_clear_rejection,
            )?;
            outcome.frames_applied += 1;
        }
        outcome.market_batches = self.dispatch.drain_batches();
        outcome.system_events = self.dispatch.drain_systems();
        Ok(outcome)
    }

    fn begin_frame<F>(
        &mut self,
        machine: &mut dyn SessionMachine,
        frame_ordinal: u64,
        available_at: TimestampNs,
        apply: F,
        outcome: &mut OrderedCaptureOutcome,
        exercise_pre_clear_rejection: bool,
    ) -> Result<(), String>
    where
        F: FnOnce(&mut dyn SessionMachine, &mut ActionBuffer) -> Result<(), AdapterError>,
    {
        self.actions.clear();
        let _ = self.actions.take_dropped();
        apply(machine, &mut self.actions).map_err(|error| error.to_string())?;
        self.frame_open = true;
        outcome.max_actions = outcome.max_actions.max(self.actions.len());

        if exercise_pre_clear_rejection && self.snapshot_boundary().is_err() {
            outcome.pre_clear_snapshot_rejections += 1;
        }

        for (action_index, action) in self.actions.as_slice().iter().cloned().enumerate() {
            let action_index = u32::try_from(action_index).map_err(|_| "action index overflow")?;
            match &action {
                SessionAction::EmitBatch(batch) => {
                    for (item_index, event) in batch.events.iter().enumerate() {
                        outcome.ordinary.push(CapturedAction {
                            frame_ordinal,
                            action_index,
                            item_index: u32::try_from(item_index)
                                .map_err(|_| "item index overflow")?,
                            available_at: event.receive_ts,
                            action: action.clone(),
                        });
                    }
                    if batch.events.is_empty() {
                        outcome.ordinary.push(CapturedAction {
                            frame_ordinal,
                            action_index,
                            item_index: 0,
                            available_at,
                            action: action.clone(),
                        });
                    }
                }
                _ => outcome.ordinary.push(CapturedAction {
                    frame_ordinal,
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
                        self.dispatch.push_batch(batch).map_err(|e| e.to_string())?,
                    );
                }
                SessionAction::EmitSystem(event) => {
                    accumulate_drop(
                        &mut system_drops,
                        self.dispatch
                            .push_system(event)
                            .map_err(|e| e.to_string())?,
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
        for (category, (count, policy)) in [
            (
                CapturedDropCategory::ActionBuffer,
                (action_drops, "DropNewest"),
            ),
            (CapturedDropCategory::MarketDispatch, market_drops),
            (CapturedDropCategory::SystemDispatch, system_drops),
        ] {
            if count > 0 {
                outcome.drops.push(CapturedDrop {
                    frame_ordinal,
                    action_index: u32::from(u16::MAX),
                    item_index: match category {
                        CapturedDropCategory::ActionBuffer => 0,
                        CapturedDropCategory::MarketDispatch => 1,
                        CapturedDropCategory::SystemDispatch => 2,
                    },
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
        self.frame_open = false;
        self.snapshot_boundary()?;
        Ok(())
    }
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

#[test]
fn mfr1_capture_preserves_pre_lane_coordinates_and_matches_ordinary_replay() {
    let recording = mfr1(&[
        (1_000_000_000, "SUB BTC-USD"),
        (
            1_000_000_001,
            "BOOK_SNAP 10 BID 100.00:1.000 ASK 101.00:1.500",
        ),
        (1_000_000_002, "BOOK_DELTA 11 BID UPSERT 100.50 0.500"),
        (1_000_000_003, "BOOK_DELTA 11 BID UPSERT 100.50 0.750"),
        (1_000_000_004, "BOOK_DELTA 13 ASK DELETE 101.00"),
        (
            1_000_000_005,
            "BOOK_SNAP 20 BID 99.00:1.000 ASK 102.00:1.000",
        ),
        (1_250_000_006, "QUOTE 99.50 101.50 1.000 1.000"),
        (1_500_000_007, "DISCONNECT"),
        (1_750_000_008, "SUB BTC-USD"),
    ]);
    let mut capture_machine = synthetic_machine();
    let captured = OrderedReplayCapture::new(1_024, OverflowPolicy::FailEngine)
        .replay(
            &mut *capture_machine,
            recording.clone(),
            TimestampNs(999_999_999),
            true,
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
        captured.drops.iter().map(|drop| drop.count).sum::<u64>(),
        ordinary.dropped_events
    );
    assert_eq!(
        captured.pre_clear_snapshot_rejections,
        captured.frames_applied
    );
    assert!(captured.ordinary.windows(2).all(|pair| {
        (
            pair[0].frame_ordinal,
            pair[0].action_index,
            pair[0].item_index,
        ) <= (
            pair[1].frame_ordinal,
            pair[1].action_index,
            pair[1].item_index,
        )
    }));
    assert!(
        captured
            .ordinary
            .iter()
            .all(|record| record.available_at.0 <= 1_750_000_008)
    );
    let captured_other = captured
        .ordinary
        .iter()
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
        captured
            .ordinary
            .iter()
            .any(|record| matches!(record.action, SessionAction::Reconnect(_)))
    );
    assert!(
        captured
            .ordinary
            .windows(2)
            .any(|pair| pair[1].available_at.0 - pair[0].available_at.0 > 250_000_000)
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
        .replay(
            &mut capture_machine,
            recording.clone(),
            TimestampNs(9),
            true,
        )
        .unwrap();
    let mut replay_machine = BurstMachine;
    let ordinary = ReplayRunner::with_overflow(4, OverflowPolicy::DropNewest)
        .replay_bytes(&mut replay_machine, recording, TimestampNs(9))
        .unwrap();

    assert_eq!(captured.frames_applied, ordinary.frames_applied);
    assert_eq!(
        captured.drops.iter().map(|drop| drop.count).sum::<u64>(),
        ordinary.dropped_events
    );
    assert_eq!(
        captured
            .drops
            .iter()
            .map(|drop| drop.category)
            .collect::<Vec<_>>(),
        vec![
            CapturedDropCategory::ActionBuffer,
            CapturedDropCategory::MarketDispatch,
            CapturedDropCategory::SystemDispatch,
        ]
    );
    assert_eq!(
        captured
            .drops
            .iter()
            .map(|drop| drop.count)
            .collect::<Vec<_>>(),
        vec![6, 508, 508]
    );
    assert!(
        captured
            .drops
            .iter()
            .all(|drop| drop.action_index == u32::from(u16::MAX))
    );
    assert_eq!(
        captured
            .drops
            .iter()
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
        captured
            .drops
            .iter()
            .map(|drop| drop.event.clone())
            .collect::<Vec<_>>(),
        replay_drop_events
    );
    assert_eq!(
        captured.ordinary.len() as u64 + captured.drops[0].count,
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
            .replay(&mut *machine, recording, TimestampNs(18), false)
            .unwrap_err(),
        "MFR1 availability regressed"
    );
}
