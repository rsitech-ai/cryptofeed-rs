use std::collections::BTreeMap;

use marketfeed_event_pulse::{
    ContractBundle, IngestOutcome,
    features::{Direction, SCALE},
    mechanics::{FamilyFlags, MechanicsEvidence, Phase, PhaseMachine},
    snapshot::{MechanicsProcessor, SnapshotError},
    window::PROCESSOR_RECORD_CAPACITY,
    wire::{
        CanonicalDecimal, ClockCursorV1, ClockQualityV1, ClockSourceKeyV1, ClockSourceV1,
        ClockStateV1, ConfiguredTargetKeyV1, ConnectionKeyV1, ContributorKeyV1, ContributorRoleV1,
        ContributorSpecV1, ContributorV1, CoverageCursorV1, CoverageSourceKeyV1, CoverageSourceV1,
        CursorModeV1, CursorV1, DropCategoryV1, FamilyV1, FaultScopeKindV1, FaultScopeV1,
        InstrumentIdentityV1, MechanicsConfigV1, MechanicsInputV1, OpenInterestEncodingV1,
        ReplayCatalogV1, ReplayEpochEntryV1, Rfc3339Time, SnapshotAuthoringV1, SystemFaultV1,
        SystemSourceKeyV1, SystemSourceV1, VenueCatalogEntryV1,
    },
};
use marketfeed_model::{
    AggressorSide, BookChange, BookDelta, BookLevel, BookOperation, BookSide, BookSnapshot,
    ConnectionId, EventEnvelope, EventFlags, Fixed, InstrumentId, Liquidation, MarketEvent,
    OpenInterest, Price, PricePoint, Quantity, Quote, SequenceRange, SessionId, TimestampNs, Trade,
    VenueId,
};

fn evidence(intensity: i128, reversal: i128) -> MechanicsEvidence {
    MechanicsEvidence {
        available_at_ns: 0,
        direction: Direction::Up,
        families: FamilyFlags {
            price: true,
            flow: true,
            book: intensity >= 65_000_000,
            derivatives: intensity >= 85_000_000,
            breadth: false,
        },
        intensity,
        confidence: SCALE,
        reversal_risk: reversal,
        valid: true,
        fully_warmed: true,
        spread_bps: 9 * SCALE,
    }
}

#[test]
fn advance_to_processes_every_transition_deadline_not_only_the_target() {
    let mut machine = PhaseMachine::new();
    machine.observe(&evidence(85_000_000, 0)).unwrap();
    machine.advance_to(350_000_000).unwrap();
    assert_eq!(machine.phase(), Phase::Cascade);
}

#[test]
fn phase_hysteresis_invalid_recovery_and_aftermath_paths_are_exact() {
    let mut machine = PhaseMachine::new();
    let mut ignition = evidence(85_000_000, 0);
    machine.observe(&ignition).unwrap();
    machine.advance_to(350_000_000).unwrap();
    let mut reversal = evidence(85_000_000, 65_000_000);
    reversal.available_at_ns = 350_000_000;
    machine.observe(&reversal).unwrap();
    machine.advance_to(600_000_000).unwrap();
    assert_eq!(machine.phase(), Phase::Exhaustion);
    ignition.available_at_ns = 600_000_000;
    ignition.valid = false;
    machine.observe(&ignition).unwrap();
    assert_eq!(machine.phase(), Phase::Invalid);
    let mut recovery = evidence(0, 0);
    recovery.families = FamilyFlags::default();
    recovery.intensity = 0;
    recovery.available_at_ns = 600_000_001;
    machine.observe(&recovery).unwrap();
    machine.advance_to(1_600_000_001).unwrap();
    assert_eq!(machine.phase(), Phase::Normal);
}

#[derive(Clone)]
struct Fixture {
    config: MechanicsConfigV1,
    contributor: ContributorKeyV1,
    clock: ClockSourceKeyV1,
}

#[derive(Clone)]
struct SplitFixture {
    config: MechanicsConfigV1,
    contributors: Vec<ContributorKeyV1>,
    clocks: Vec<ClockSourceKeyV1>,
}

#[derive(Clone)]
struct CapacityFixture {
    config: MechanicsConfigV1,
    contributors: Vec<ContributorKeyV1>,
    clocks: Vec<ClockSourceKeyV1>,
}

fn capacity_fixture() -> CapacityFixture {
    let contributors = vec![
        ContributorKeyV1::new(
            "a_primary",
            InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "HYPERLIQUID", "BNB-USDC")
                .unwrap(),
        )
        .unwrap(),
        ContributorKeyV1::new(
            "b_confirmation",
            InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "BINANCE", "BNB-USDC").unwrap(),
        )
        .unwrap(),
    ];
    let specs = vec![
        ContributorSpecV1::new(
            contributors[0].clone(),
            ContributorRoleV1::Primary,
            [FamilyV1::Trade, FamilyV1::Quote, FamilyV1::Book],
        )
        .unwrap(),
        ContributorSpecV1::new(
            contributors[1].clone(),
            ContributorRoleV1::Confirmation,
            [FamilyV1::ConfirmationPrice],
        )
        .unwrap(),
    ];
    let connections = vec![
        ConnectionKeyV1::new("capacity_connection_00").unwrap(),
        ConnectionKeyV1::new("capacity_connection_01").unwrap(),
    ];
    let bindings = contributors
        .iter()
        .cloned()
        .zip(connections.iter().cloned())
        .collect();
    let clocks = contributors
        .iter()
        .enumerate()
        .map(|(index, contributor)| {
            ClockSourceKeyV1::new(&format!("x_clock_{index:02}"), contributor.clone()).unwrap()
        })
        .collect::<Vec<_>>();
    let coverage = vec![
        CoverageSourceKeyV1::new("y_trade", contributors[0].clone(), FamilyV1::Trade).unwrap(),
        CoverageSourceKeyV1::new("y_quote", contributors[0].clone(), FamilyV1::Quote).unwrap(),
        CoverageSourceKeyV1::new("y_book", contributors[0].clone(), FamilyV1::Book).unwrap(),
        CoverageSourceKeyV1::new(
            "y_confirmation",
            contributors[1].clone(),
            FamilyV1::ConfirmationPrice,
        )
        .unwrap(),
    ];
    let config = MechanicsConfigV1::new(
        "event_pulse_processor",
        connections.clone(),
        specs,
        bindings,
        clocks.clone(),
        coverage,
        vec![],
    )
    .unwrap();
    CapacityFixture {
        config,
        contributors,
        clocks,
    }
}

fn split_fixture() -> SplitFixture {
    let instrument =
        InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "HYPERLIQUID", "BNB-USDC").unwrap();
    let definitions = [
        ("a_trade", vec![FamilyV1::Trade]),
        ("b_quote", vec![FamilyV1::Quote]),
        ("c_book", vec![FamilyV1::Book]),
        ("d_oi", vec![FamilyV1::OpenInterest]),
        ("e_liq", vec![FamilyV1::Liquidation]),
    ];
    let contributors = definitions
        .iter()
        .map(|(source, _)| ContributorKeyV1::new(source, instrument.clone()).unwrap())
        .collect::<Vec<_>>();
    let specs = definitions
        .iter()
        .zip(&contributors)
        .map(|((_, families), key)| {
            ContributorSpecV1::new(
                key.clone(),
                ContributorRoleV1::Primary,
                families.iter().copied(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let connections = contributors
        .iter()
        .enumerate()
        .map(|(index, _)| ConnectionKeyV1::new(&format!("connection_{index}")).unwrap())
        .collect::<Vec<_>>();
    let bindings = contributors
        .iter()
        .cloned()
        .zip(connections.iter().cloned())
        .collect();
    let clocks = contributors
        .iter()
        .enumerate()
        .map(|(index, key)| ClockSourceKeyV1::new(&format!("clock_{index}"), key.clone()).unwrap())
        .collect::<Vec<_>>();
    let coverage = specs
        .iter()
        .enumerate()
        .flat_map(|(index, spec)| {
            spec.allowed_families().iter().map(move |family| {
                CoverageSourceKeyV1::new(&format!("coverage_{index}"), spec.key().clone(), *family)
                    .unwrap()
            })
        })
        .collect();
    let config = MechanicsConfigV1::new(
        "event_pulse_processor",
        connections,
        specs,
        bindings,
        clocks.clone(),
        coverage,
        vec![],
    )
    .unwrap();
    SplitFixture {
        config,
        contributors,
        clocks,
    }
}

fn with_system_sources(
    fixture: &SplitFixture,
    system_sources: Vec<SystemSourceKeyV1>,
) -> SplitFixture {
    SplitFixture {
        config: MechanicsConfigV1::new(
            fixture.config.processor_id(),
            fixture.config.connections().to_vec(),
            fixture.config.contributors().to_vec(),
            fixture.config.contributor_connections().clone(),
            fixture.config.clock_sources().to_vec(),
            fixture.config.coverage_sources().to_vec(),
            system_sources,
        )
        .unwrap(),
        contributors: fixture.contributors.clone(),
        clocks: fixture.clocks.clone(),
    }
}

fn fixture() -> Fixture {
    let instrument =
        InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "HYPERLIQUID", "BNB-USDC").unwrap();
    let contributor = ContributorKeyV1::new("market_source", instrument).unwrap();
    let connection = ConnectionKeyV1::new("market_connection").unwrap();
    let clock = ClockSourceKeyV1::new("z_clock_source", contributor.clone()).unwrap();
    let families = [
        FamilyV1::Trade,
        FamilyV1::Quote,
        FamilyV1::Book,
        FamilyV1::OpenInterest,
        FamilyV1::Liquidation,
    ];
    let specs = vec![
        ContributorSpecV1::new(contributor.clone(), ContributorRoleV1::Primary, families).unwrap(),
    ];
    let bindings = BTreeMap::from([(contributor.clone(), connection.clone())]);
    let coverage = families
        .into_iter()
        .enumerate()
        .map(|(index, family)| {
            CoverageSourceKeyV1::new(&format!("z_coverage_{index}"), contributor.clone(), family)
                .unwrap()
        })
        .collect();
    let config = MechanicsConfigV1::new(
        "event_pulse_processor",
        vec![connection],
        specs,
        bindings,
        vec![clock.clone()],
        coverage,
        vec![],
    )
    .unwrap();
    Fixture {
        config,
        contributor,
        clock,
    }
}

fn catalog_epoch(epoch: &str, generation: u8) -> ReplayCatalogV1 {
    catalog_epoch_source(epoch, generation, "market_source")
}

fn catalog_epoch_source(epoch: &str, generation: u8, source: &str) -> ReplayCatalogV1 {
    catalog_epoch_source_oi(
        epoch,
        generation,
        source,
        OpenInterestEncodingV1::contracts(),
    )
}

fn catalog_epoch_source_oi(
    epoch: &str,
    generation: u8,
    source: &str,
    oi_encoding: OpenInterestEncodingV1,
) -> ReplayCatalogV1 {
    ReplayCatalogV1::new(
        BTreeMap::from([(1, VenueCatalogEntryV1::new("HYPERLIQUID", source).unwrap())]),
        BTreeMap::from([(
            1,
            InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "HYPERLIQUID", "BNB-USDC")
                .unwrap(),
        )]),
        vec![ReplayEpochEntryV1::new(7, 9, epoch, generation).unwrap()],
        BTreeMap::from([(1, oi_encoding)]),
    )
    .unwrap()
}

fn market(sequence: u64, ns: i64, payload: MarketEvent) -> MechanicsInputV1 {
    market_in_epoch(sequence, ns, payload, "epoch_a", 0)
}

fn market_in_epoch(
    sequence: u64,
    ns: i64,
    payload: MarketEvent,
    epoch: &str,
    generation: u8,
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
        catalog_epoch(epoch, generation),
    )
    .unwrap()
}

fn market_from_source(
    source: &str,
    sequence: u64,
    ns: i64,
    payload: MarketEvent,
) -> MechanicsInputV1 {
    let base = market(sequence, ns, payload);
    let envelope = match base.view() {
        marketfeed_event_pulse::wire::MechanicsInputRefV1::Market { envelope, .. } => {
            envelope.clone()
        }
        _ => unreachable!(),
    };
    MechanicsInputV1::market(envelope, 0, catalog_epoch_source("epoch_a", 0, source)).unwrap()
}

fn market_from_source_in_epoch(
    source: &str,
    sequence: u64,
    ns: i64,
    payload: MarketEvent,
    epoch: &str,
    generation: u8,
) -> MechanicsInputV1 {
    let base = market_in_epoch(sequence, ns, payload, epoch, generation);
    let envelope = match base.view() {
        marketfeed_event_pulse::wire::MechanicsInputRefV1::Market { envelope, .. } => {
            envelope.clone()
        }
        _ => unreachable!(),
    };
    MechanicsInputV1::market(envelope, 0, catalog_epoch_source(epoch, generation, source)).unwrap()
}

fn market_from_source_times(
    source: &str,
    sequence: u64,
    exchange_ns: i64,
    receive_ns: i64,
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
            exchange_ts: Some(TimestampNs(exchange_ns)),
            receive_ts: TimestampNs(receive_ns),
            source_sequence: Some(SequenceRange {
                first: sequence,
                last: sequence,
            }),
            flags: EventFlags::empty(),
            payload,
        },
        0,
        catalog_epoch_source("epoch_a", 0, source),
    )
    .unwrap()
}

fn capacity_market(
    fixture: &CapacityFixture,
    contributor_index: usize,
    sequence: u64,
    ns: i64,
    epoch: &str,
    generation: u8,
    payload: MarketEvent,
) -> MechanicsInputV1 {
    let contributor = &fixture.contributors[contributor_index];
    let connection_id = u64::try_from(contributor_index + 1).unwrap();
    let venue = contributor.instrument().venue();
    let source = contributor.source_id();
    let catalog = ReplayCatalogV1::new(
        BTreeMap::from([(1, VenueCatalogEntryV1::new(venue, source).unwrap())]),
        BTreeMap::from([(1, contributor.instrument().clone())]),
        vec![ReplayEpochEntryV1::new(connection_id, 9, epoch, generation).unwrap()],
        BTreeMap::from([(1, OpenInterestEncodingV1::contracts())]),
    )
    .unwrap();
    MechanicsInputV1::market(
        EventEnvelope {
            schema_version: 1,
            venue: VenueId(1),
            instrument: Some(InstrumentId(1)),
            connection: ConnectionId(connection_id),
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
        catalog,
    )
    .unwrap()
}

fn ingest_capacity_controls(
    processor: &mut MechanicsProcessor,
    fixture: &CapacityFixture,
    at: i64,
    sequence: u64,
    generations: [u8; 2],
) {
    for (index, clock) in fixture.clocks.iter().enumerate() {
        let generation = generations[index];
        processor
            .ingest(
                &MechanicsInputV1::clock(
                    ContributorV1::new(
                        fixture.contributors[index].clone(),
                        if generation == 0 {
                            "epoch_a"
                        } else {
                            "epoch_b"
                        },
                        generation,
                    )
                    .unwrap(),
                    ClockSourceV1::new(
                        clock.clone(),
                        if generation == 0 {
                            "epoch_clock_a"
                        } else {
                            "epoch_clock_b"
                        },
                        generation,
                    )
                    .unwrap(),
                    time_ns(at),
                    time_ns(at),
                    ClockCursorV1::native(sequence, sequence).unwrap(),
                    ClockStateV1::Synchronized,
                    CanonicalDecimal::parse("0.25", 18, 8).unwrap(),
                    2_000,
                    ClockQualityV1::Validated,
                    "SOURCE_CLOCK_WITHIN_TOLERANCE",
                )
                .unwrap(),
            )
            .unwrap();
    }
    let mut coverage = fixture.config.coverage_sources().to_vec();
    coverage.sort_by_key(|key| key.source_id().to_owned());
    for key in coverage {
        let index = usize::from(key.subject() == &fixture.contributors[1]);
        let generation = generations[index];
        processor
            .ingest(
                &MechanicsInputV1::coverage(
                    ContributorV1::new(
                        key.subject().clone(),
                        if generation == 0 {
                            "epoch_a"
                        } else {
                            "epoch_b"
                        },
                        generation,
                    )
                    .unwrap(),
                    CoverageSourceV1::new(
                        key.clone(),
                        if generation == 0 {
                            "epoch_coverage_a"
                        } else {
                            "epoch_coverage_b"
                        },
                        generation,
                    )
                    .unwrap(),
                    key.family(),
                    time_ns(at - 5_000_000_000),
                    time_ns(at),
                    time_ns(at),
                    CoverageCursorV1::native(sequence, sequence).unwrap(),
                )
                .unwrap(),
            )
            .unwrap();
    }
}

fn trade(price: i128) -> MarketEvent {
    trade_with_side(price, AggressorSide::Buy)
}

fn trade_with_side(price: i128, aggressor: AggressorSide) -> MarketEvent {
    MarketEvent::Trade(Trade {
        price: Price(Fixed::new(price, 8)),
        quantity: Quantity(Fixed::new(SCALE, 8)),
        aggressor,
        trade_id: None,
    })
}

fn time_ns(ns: i64) -> Rfc3339Time {
    Rfc3339Time::from_unix_nanos(ns).unwrap()
}

fn authoring() -> SnapshotAuthoringV1 {
    SnapshotAuthoringV1::new(
        "event_pulse_mechanics_test",
        "lineage_event_pulse_test",
        "event_cluster_test",
        InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "HYPERLIQUID", "BNB-USDC").unwrap(),
        1,
        None,
        15_000,
        "test-v1",
    )
    .unwrap()
}

fn ingest_coverage_round(
    processor: &mut MechanicsProcessor,
    fixture: &Fixture,
    at_ns: i64,
    sequence: u64,
    contributor_epoch: &str,
    generation: u8,
) {
    for key in fixture.config.coverage_sources() {
        let input = MechanicsInputV1::coverage(
            ContributorV1::new(fixture.contributor.clone(), contributor_epoch, generation).unwrap(),
            CoverageSourceV1::new(
                key.clone(),
                if generation == 0 {
                    "epoch_coverage_a"
                } else {
                    "epoch_coverage_b"
                },
                generation,
            )
            .unwrap(),
            key.family(),
            time_ns(0),
            time_ns(at_ns),
            time_ns(at_ns),
            CoverageCursorV1::native(sequence, sequence).unwrap(),
        )
        .unwrap();
        processor.ingest(&input).unwrap();
    }
}

fn warmed_processor_with_clock(
    clock_ns: i64,
    clock_state: ClockStateV1,
    quality_state: ClockQualityV1,
    oi_base_conversion: Option<&str>,
) -> MechanicsProcessor {
    let fixture = fixture();
    let mut processor = MechanicsProcessor::new(fixture.config.clone(), authoring()).unwrap();
    processor.ingest(&market(1, 0, trade(100 * SCALE))).unwrap();
    processor
        .ingest(&market(2, 60_000_000_000, trade(100 * SCALE)))
        .unwrap();
    processor
        .ingest(&market(3, 60_010_000_000, trade(101 * SCALE)))
        .unwrap();
    processor
        .ingest(&market(
            4,
            60_020_000_000,
            MarketEvent::Quote(Quote {
                bid_price: Price(Fixed::new(100 * SCALE, 8)),
                bid_quantity: None,
                ask_price: Price(Fixed::new(100 * SCALE + 10_000_000, 8)),
                ask_quantity: None,
            }),
        ))
        .unwrap();
    processor
        .ingest(&market(
            5,
            60_030_000_000,
            MarketEvent::BookSnapshot(BookSnapshot {
                bids: vec![BookLevel {
                    price: Price(Fixed::new(100 * SCALE, 8)),
                    quantity: Quantity(Fixed::new(SCALE, 8)),
                }],
                asks: vec![BookLevel {
                    price: Price(Fixed::new(100 * SCALE + 10_000_000, 8)),
                    quantity: Quantity(Fixed::new(SCALE, 8)),
                }],
                depth: None,
                checksum: None,
            }),
        ))
        .unwrap();
    processor
        .ingest(&market(
            6,
            60_040_000_000,
            MarketEvent::BookDelta(BookDelta {
                changes: vec![BookChange {
                    side: BookSide::Bid,
                    operation: BookOperation::Upsert,
                    price: Price(Fixed::new(100 * SCALE, 8)),
                    quantity: Some(Quantity(Fixed::new(2 * SCALE, 8))),
                }],
                checksum: None,
            }),
        ))
        .unwrap();
    for (sequence, ns, quantity) in [
        (7, 60_050_000_000, 2 * SCALE),
        (8, 60_060_000_000, 3 * SCALE),
    ] {
        let base = market(
            sequence,
            ns,
            MarketEvent::OpenInterest(OpenInterest {
                quantity: Quantity(Fixed::new(quantity, 8)),
            }),
        );
        let envelope = match base.view() {
            marketfeed_event_pulse::wire::MechanicsInputRefV1::Market { envelope, .. } => {
                envelope.clone()
            }
            _ => unreachable!(),
        };
        let catalog = catalog_epoch_source_oi(
            "epoch_a",
            0,
            "market_source",
            match oi_base_conversion {
                Some(conversion) => OpenInterestEncodingV1::base(conversion).unwrap(),
                None => OpenInterestEncodingV1::contracts(),
            },
        );
        processor
            .ingest(&MechanicsInputV1::market(envelope, 0, catalog).unwrap())
            .unwrap();
    }
    let contributor = ContributorV1::new(fixture.contributor.clone(), "epoch_a", 0).unwrap();
    let clock = MechanicsInputV1::clock(
        contributor,
        ClockSourceV1::new(fixture.clock.clone(), "epoch_clock_a", 0).unwrap(),
        time_ns(clock_ns),
        time_ns(clock_ns),
        ClockCursorV1::native(1, 1).unwrap(),
        clock_state,
        CanonicalDecimal::parse("0.25", 18, 8).unwrap(),
        2_000,
        quality_state,
        "SOURCE_CLOCK_WITHIN_TOLERANCE",
    )
    .unwrap();
    processor.ingest(&clock).unwrap();
    ingest_coverage_round(
        &mut processor,
        &fixture,
        clock_ns + 10_000_000,
        1,
        "epoch_a",
        0,
    );
    processor
}

fn warmed_processor(clock_ns: i64) -> MechanicsProcessor {
    warmed_processor_with_clock(
        clock_ns,
        ClockStateV1::Synchronized,
        ClockQualityV1::Validated,
        None,
    )
}

fn warmed_split_processor() -> (MechanicsProcessor, SplitFixture) {
    warmed_split_processor_for(split_fixture())
}

fn warmed_split_processor_for(fixture: SplitFixture) -> (MechanicsProcessor, SplitFixture) {
    let mut processor = MechanicsProcessor::new(fixture.config.clone(), authoring()).unwrap();
    for (source, ns, payload) in [
        ("a_trade", 0, trade(100 * SCALE)),
        (
            "b_quote",
            1_000_000,
            MarketEvent::Quote(Quote {
                bid_price: Price(Fixed::new(100 * SCALE, 8)),
                bid_quantity: None,
                ask_price: Price(Fixed::new(100 * SCALE + 10_000_000, 8)),
                ask_quantity: None,
            }),
        ),
        (
            "c_book",
            2_000_000,
            MarketEvent::BookSnapshot(BookSnapshot {
                bids: vec![],
                asks: vec![],
                depth: None,
                checksum: None,
            }),
        ),
        (
            "d_oi",
            3_000_000,
            MarketEvent::OpenInterest(OpenInterest {
                quantity: Quantity(Fixed::new(SCALE, 8)),
            }),
        ),
        (
            "e_liq",
            4_000_000,
            MarketEvent::Liquidation(Liquidation {
                price: Price(Fixed::new(100 * SCALE, 8)),
                quantity: Quantity(Fixed::new(SCALE, 8)),
                side: AggressorSide::Buy,
            }),
        ),
    ] {
        processor
            .ingest(&market_from_source(source, 1, ns, payload))
            .unwrap();
    }
    for (sequence, ns, side) in [
        (2, 60_000_000_000, AggressorSide::Buy),
        (3, 60_010_000_000, AggressorSide::Sell),
    ] {
        processor
            .ingest(&market_from_source(
                "a_trade",
                sequence,
                ns,
                trade_with_side(100 * SCALE, side),
            ))
            .unwrap();
    }
    processor
        .ingest(&market_from_source(
            "b_quote",
            2,
            60_020_000_000,
            MarketEvent::Quote(Quote {
                bid_price: Price(Fixed::new(100 * SCALE, 8)),
                bid_quantity: None,
                ask_price: Price(Fixed::new(100 * SCALE + 1_000_000, 8)),
                ask_quantity: None,
            }),
        ))
        .unwrap();
    processor
        .ingest(&market_from_source(
            "c_book",
            2,
            60_030_000_000,
            MarketEvent::BookSnapshot(BookSnapshot {
                bids: vec![BookLevel {
                    price: Price(Fixed::new(100 * SCALE, 8)),
                    quantity: Quantity(Fixed::new(2 * SCALE, 8)),
                }],
                asks: vec![BookLevel {
                    price: Price(Fixed::new(100 * SCALE + 1_000_000, 8)),
                    quantity: Quantity(Fixed::new(2 * SCALE, 8)),
                }],
                depth: None,
                checksum: None,
            }),
        ))
        .unwrap();
    processor
        .ingest(&market_from_source(
            "d_oi",
            2,
            60_040_000_000,
            MarketEvent::OpenInterest(OpenInterest {
                quantity: Quantity(Fixed::new(2 * SCALE, 8)),
            }),
        ))
        .unwrap();
    processor
        .ingest(&market_from_source(
            "e_liq",
            2,
            60_050_000_000,
            MarketEvent::Liquidation(Liquidation {
                price: Price(Fixed::new(101 * SCALE, 8)),
                quantity: Quantity(Fixed::new(SCALE, 8)),
                side: AggressorSide::Buy,
            }),
        ))
        .unwrap();
    for (index, (clock, contributor)) in
        fixture.clocks.iter().zip(&fixture.contributors).enumerate()
    {
        let at = 60_060_000_000 + i64::try_from(index).unwrap() * 1_000_000;
        processor
            .ingest(
                &MechanicsInputV1::clock(
                    ContributorV1::new(contributor.clone(), "epoch_a", 0).unwrap(),
                    ClockSourceV1::new(clock.clone(), "epoch_clock_a", 0).unwrap(),
                    time_ns(at),
                    time_ns(at),
                    ClockCursorV1::native(1, 1).unwrap(),
                    ClockStateV1::Synchronized,
                    CanonicalDecimal::parse("0.25", 18, 8).unwrap(),
                    2_000,
                    ClockQualityV1::Validated,
                    "SOURCE_CLOCK_WITHIN_TOLERANCE",
                )
                .unwrap(),
            )
            .unwrap();
    }
    refresh_split_coverage(&mut processor, &fixture, 60_070_000_000, 1);
    for round in 0u64..=5 {
        let at = 60_090_000_000 + i64::try_from(round).unwrap() * 200_000_000;
        processor
            .ingest(&market_from_source(
                "a_trade",
                4 + round,
                at,
                trade_with_side(
                    100 * SCALE,
                    if round % 2 == 0 {
                        AggressorSide::Buy
                    } else {
                        AggressorSide::Sell
                    },
                ),
            ))
            .unwrap();
        processor
            .ingest(&market_from_source(
                "b_quote",
                3 + round,
                at,
                MarketEvent::Quote(Quote {
                    bid_price: Price(Fixed::new(100 * SCALE, 8)),
                    bid_quantity: None,
                    ask_price: Price(Fixed::new(100 * SCALE + 1_000_000, 8)),
                    ask_quantity: None,
                }),
            ))
            .unwrap();
        processor
            .ingest(&market_from_source(
                "c_book",
                3 + round,
                at,
                MarketEvent::BookSnapshot(BookSnapshot {
                    bids: vec![BookLevel {
                        price: Price(Fixed::new(100 * SCALE, 8)),
                        quantity: Quantity(Fixed::new(2 * SCALE, 8)),
                    }],
                    asks: vec![BookLevel {
                        price: Price(Fixed::new(100 * SCALE + 1_000_000, 8)),
                        quantity: Quantity(Fixed::new(2 * SCALE, 8)),
                    }],
                    depth: None,
                    checksum: None,
                }),
            ))
            .unwrap();
        if round == 5 {
            for (clock, contributor) in fixture.clocks.iter().zip(&fixture.contributors) {
                processor
                    .ingest(
                        &MechanicsInputV1::clock(
                            ContributorV1::new(contributor.clone(), "epoch_a", 0).unwrap(),
                            ClockSourceV1::new(clock.clone(), "epoch_clock_a", 0).unwrap(),
                            time_ns(at),
                            time_ns(at),
                            ClockCursorV1::native(2, 2).unwrap(),
                            ClockStateV1::Synchronized,
                            CanonicalDecimal::parse("0.25", 18, 8).unwrap(),
                            2_000,
                            ClockQualityV1::Validated,
                            "SOURCE_CLOCK_WITHIN_TOLERANCE",
                        )
                        .unwrap(),
                    )
                    .unwrap();
            }
        }
        refresh_split_coverage(&mut processor, &fixture, at, 2 + round);
    }
    (processor, fixture)
}

fn system_input(
    key: &SystemSourceKeyV1,
    scope: FaultScopeV1,
    sequence: u64,
    at: i64,
    fault: SystemFaultV1,
) -> MechanicsInputV1 {
    MechanicsInputV1::system(
        SystemSourceV1::new(key.clone(), "epoch_system_a", 0).unwrap(),
        scope,
        time_ns(at),
        time_ns(at),
        CursorV1::native(sequence, sequence).unwrap(),
        fault,
        None,
    )
    .unwrap()
}

fn refresh_split_coverage(
    processor: &mut MechanicsProcessor,
    fixture: &SplitFixture,
    at: i64,
    sequence: u64,
) {
    for coverage in fixture.config.coverage_sources() {
        processor
            .ingest(
                &MechanicsInputV1::coverage(
                    ContributorV1::new(coverage.subject().clone(), "epoch_a", 0).unwrap(),
                    CoverageSourceV1::new(coverage.clone(), "epoch_coverage_a", 0).unwrap(),
                    coverage.family(),
                    time_ns(at - 5_000_000_000),
                    time_ns(at),
                    time_ns(at),
                    CoverageCursorV1::native(sequence, sequence).unwrap(),
                )
                .unwrap(),
            )
            .unwrap();
    }
}

fn clock_input(
    fixture: &Fixture,
    available_ns: i64,
    epoch: &str,
    generation: u8,
    native: u64,
    skew: &str,
) -> MechanicsInputV1 {
    MechanicsInputV1::clock(
        ContributorV1::new(
            fixture.contributor.clone(),
            if generation == 0 {
                "epoch_a"
            } else {
                "epoch_b"
            },
            generation,
        )
        .unwrap(),
        ClockSourceV1::new(fixture.clock.clone(), epoch, generation).unwrap(),
        time_ns(available_ns),
        time_ns(available_ns),
        ClockCursorV1::native(native, native).unwrap(),
        ClockStateV1::Synchronized,
        CanonicalDecimal::parse(skew, 18, 8).unwrap(),
        2_000,
        ClockQualityV1::Validated,
        "SOURCE_CLOCK_WITHIN_TOLERANCE",
    )
    .unwrap()
}

#[test]
fn public_processor_derives_snapshot_from_validated_inputs_and_causal_maximum() {
    let clock_ns = 60_080_000_000;
    let mut processor = warmed_processor(clock_ns);
    let snapshot = processor.snapshot(time_ns(60_090_000_000)).unwrap();
    assert_eq!(
        snapshot.value()["causal_time"]["available_at"],
        time_ns(60_090_000_000).canonical()
    );
    ContractBundle::load_embedded()
        .unwrap()
        .validate_e1_json(snapshot.canonical_json().as_bytes())
        .unwrap();
    if let Some(path) = std::env::var_os("EVENT_PULSE_SNAPSHOT_OUTPUT") {
        std::fs::write(path, snapshot.canonical_json()).unwrap();
    }
}

#[test]
fn split_primary_family_owners_author_their_own_feature_rows() {
    let fixture = split_fixture();
    let mut processor = MechanicsProcessor::new(fixture.config.clone(), authoring()).unwrap();
    for (source, ns, payload) in [
        ("a_trade", 0, trade(99 * SCALE)),
        (
            "b_quote",
            1_000_000,
            MarketEvent::Quote(Quote {
                bid_price: Price(Fixed::new(99 * SCALE, 8)),
                bid_quantity: None,
                ask_price: Price(Fixed::new(99 * SCALE + 10_000_000, 8)),
                ask_quantity: None,
            }),
        ),
        (
            "c_book",
            2_000_000,
            MarketEvent::BookSnapshot(BookSnapshot {
                bids: vec![],
                asks: vec![],
                depth: None,
                checksum: None,
            }),
        ),
        (
            "d_oi",
            3_000_000,
            MarketEvent::OpenInterest(OpenInterest {
                quantity: Quantity(Fixed::new(SCALE, 8)),
            }),
        ),
        (
            "e_liq",
            4_000_000,
            MarketEvent::Liquidation(Liquidation {
                price: Price(Fixed::new(99 * SCALE, 8)),
                quantity: Quantity(Fixed::new(SCALE, 8)),
                side: AggressorSide::Buy,
            }),
        ),
    ] {
        processor
            .ingest(&market_from_source(source, 1, ns, payload))
            .unwrap();
    }
    for (sequence, ns, price) in [(2, 60_000_000_000, 100), (3, 60_001_000_000, 101)] {
        processor
            .ingest(&market_from_source(
                "a_trade",
                sequence,
                ns,
                trade(price * SCALE),
            ))
            .unwrap();
    }
    for (sequence, ns, offset) in [(2, 60_010_000_000, 1), (3, 60_011_000_000, 2)] {
        processor
            .ingest(&market_from_source(
                "b_quote",
                sequence,
                ns,
                MarketEvent::Quote(Quote {
                    bid_price: Price(Fixed::new(100 * SCALE + offset, 8)),
                    bid_quantity: None,
                    ask_price: Price(Fixed::new(100 * SCALE + 10_000_000 + offset, 8)),
                    ask_quantity: None,
                }),
            ))
            .unwrap();
    }
    for (sequence, ns) in [(2, 60_020_000_000), (3, 60_021_000_000)] {
        processor
            .ingest(&market_from_source(
                "c_book",
                sequence,
                ns,
                MarketEvent::BookSnapshot(BookSnapshot {
                    bids: vec![BookLevel {
                        price: Price(Fixed::new(100 * SCALE, 8)),
                        quantity: Quantity(Fixed::new(2 * SCALE, 8)),
                    }],
                    asks: vec![BookLevel {
                        price: Price(Fixed::new(100 * SCALE + 10_000_000, 8)),
                        quantity: Quantity(Fixed::new(2 * SCALE, 8)),
                    }],
                    depth: None,
                    checksum: None,
                }),
            ))
            .unwrap();
    }
    for (sequence, ns, quantity) in [
        (2, 60_030_000_000, 2 * SCALE),
        (3, 60_031_000_000, 3 * SCALE),
    ] {
        processor
            .ingest(&market_from_source(
                "d_oi",
                sequence,
                ns,
                MarketEvent::OpenInterest(OpenInterest {
                    quantity: Quantity(Fixed::new(quantity, 8)),
                }),
            ))
            .unwrap();
    }
    for (sequence, ns) in [(2, 60_040_000_000), (3, 60_041_000_000)] {
        processor
            .ingest(&market_from_source(
                "e_liq",
                sequence,
                ns,
                MarketEvent::Liquidation(Liquidation {
                    price: Price(Fixed::new(100 * SCALE, 8)),
                    quantity: Quantity(Fixed::new(SCALE, 8)),
                    side: AggressorSide::Buy,
                }),
            ))
            .unwrap();
    }
    for (index, (key, contributor)) in fixture.clocks.iter().zip(&fixture.contributors).enumerate()
    {
        let at = 60_050_000_000 + index as i64 * 1_000_000;
        processor
            .ingest(
                &MechanicsInputV1::clock(
                    ContributorV1::new(contributor.clone(), "epoch_a", 0).unwrap(),
                    ClockSourceV1::new(key.clone(), "epoch_clock_a", 0).unwrap(),
                    time_ns(at),
                    time_ns(at),
                    ClockCursorV1::native(1, 1).unwrap(),
                    ClockStateV1::Synchronized,
                    CanonicalDecimal::parse("0.25", 18, 8).unwrap(),
                    2_000,
                    ClockQualityV1::Validated,
                    "SOURCE_CLOCK_WITHIN_TOLERANCE",
                )
                .unwrap(),
            )
            .unwrap();
    }
    let decision = 60_060_000_000;
    for key in fixture.config.coverage_sources() {
        let contributor = fixture
            .contributors
            .iter()
            .find(|contributor| *contributor == key.subject())
            .unwrap();
        processor
            .ingest(
                &MechanicsInputV1::coverage(
                    ContributorV1::new(contributor.clone(), "epoch_a", 0).unwrap(),
                    CoverageSourceV1::new(key.clone(), "epoch_coverage_a", 0).unwrap(),
                    key.family(),
                    time_ns(0),
                    time_ns(decision),
                    time_ns(decision),
                    CoverageCursorV1::native(1, 1).unwrap(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    let snapshot = processor.snapshot(time_ns(decision)).unwrap();
    for name in [
        "log_return",
        "taker_imbalance",
        "cvd_slope",
        "spread_bps",
        "book_depth_10bps",
        "open_interest_change",
        "liquidation_notional",
    ] {
        let row = snapshot.value()["features"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["name"] == name)
            .unwrap();
        assert!(row["value"].is_string(), "{name}: {row}");
    }
}

#[test]
fn optional_oi_fault_degrades_without_invalidating_critical_phase() {
    let (mut processor, _fixture) = warmed_split_processor();
    let decision = 61_090_000_000;
    let mut baseline = processor.clone();
    let baseline = baseline.snapshot(time_ns(decision)).unwrap();
    assert_ne!(
        baseline.value()["quality_state"],
        "INVALID",
        "{}",
        baseline.canonical_json()
    );
    assert!(
        processor
            .ingest(&market_from_source(
                "d_oi",
                10,
                decision,
                MarketEvent::OpenInterest(OpenInterest {
                    quantity: Quantity(Fixed::new(3 * SCALE, 8)),
                }),
            ))
            .is_err()
    );
    let snapshot = processor.snapshot(time_ns(decision)).unwrap();
    assert_eq!(
        snapshot.value()["quality_state"],
        "DEGRADED",
        "{}",
        snapshot.canonical_json()
    );
    assert_eq!(snapshot.value()["mechanical_confidence"], "0.8");
    assert_ne!(snapshot.value()["phase"], "INVALID");
    let oi = snapshot.value()["features"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["name"] == "open_interest_change")
        .unwrap();
    assert_eq!(oi["quality_state"], "INVALID");
    assert_eq!(oi["reason_code"], "SOURCE_INVALIDATED");
    assert!(
        snapshot.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "SEQUENCE_GAP")
    );
}

#[test]
fn optional_liquidation_fault_degrades_without_invalidating_critical_phase() {
    let (mut processor, _fixture) = warmed_split_processor();
    let decision = 61_090_000_000;
    assert!(
        processor
            .ingest(&market_from_source(
                "e_liq",
                10,
                decision,
                MarketEvent::Liquidation(Liquidation {
                    price: Price(Fixed::new(101 * SCALE, 8)),
                    quantity: Quantity(Fixed::new(SCALE, 8)),
                    side: AggressorSide::Buy,
                }),
            ))
            .is_err()
    );
    let snapshot = processor.snapshot(time_ns(decision)).unwrap();
    assert_eq!(
        snapshot.value()["quality_state"],
        "DEGRADED",
        "{}",
        snapshot.canonical_json()
    );
    assert_eq!(snapshot.value()["mechanical_confidence"], "0.8");
    assert_ne!(snapshot.value()["phase"], "INVALID");
    let liquidation = snapshot.value()["features"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["name"] == "liquidation_notional")
        .unwrap();
    assert_eq!(liquidation["quality_state"], "INVALID");
    assert_eq!(liquidation["reason_code"], "SOURCE_INVALIDATED");
    assert!(
        snapshot.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "SEQUENCE_GAP")
    );
}

#[test]
fn split_critical_owner_faults_invalidate_their_exact_feature_and_phase() {
    for (source, payload, feature) in [
        (
            "a_trade",
            trade_with_side(100 * SCALE, AggressorSide::Buy),
            "log_return",
        ),
        (
            "b_quote",
            MarketEvent::Quote(Quote {
                bid_price: Price(Fixed::new(100 * SCALE, 8)),
                bid_quantity: None,
                ask_price: Price(Fixed::new(100 * SCALE + 1_000_000, 8)),
                ask_quantity: None,
            }),
            "spread_bps",
        ),
        (
            "c_book",
            MarketEvent::BookSnapshot(BookSnapshot {
                bids: vec![],
                asks: vec![],
                depth: None,
                checksum: None,
            }),
            "book_depth_10bps",
        ),
    ] {
        let (mut processor, _fixture) = warmed_split_processor();
        let decision = 61_100_000_000;
        assert!(
            processor
                .ingest(&market_from_source(source, 20, decision, payload))
                .is_err()
        );
        let snapshot = processor.snapshot(time_ns(decision)).unwrap();
        assert_eq!(snapshot.value()["quality_state"], "INVALID", "{source}");
        assert_eq!(snapshot.value()["mechanical_confidence"], "0", "{source}");
        assert_eq!(snapshot.value()["phase"], "INVALID", "{source}");
        let row = snapshot.value()["features"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["name"] == feature)
            .unwrap();
        assert_eq!(row["quality_state"], "INVALID", "{source}: {row}");
        assert_eq!(row["reason_code"], "SOURCE_INVALIDATED", "{source}: {row}");
        assert!(
            snapshot.value()["quality_flags"]
                .as_array()
                .unwrap()
                .iter()
                .any(|flag| flag == "SEQUENCE_GAP"),
            "{source}"
        );
    }
}

#[test]
fn confirmation_fault_removes_breadth_and_degrades_without_global_invalidity() {
    let fixture = capacity_fixture();
    let mut processor = MechanicsProcessor::new(fixture.config.clone(), authoring()).unwrap();
    let primary_events = |base_sequence: u64, at: i64, side: AggressorSide| {
        [
            capacity_market(
                &fixture,
                0,
                base_sequence,
                at,
                "epoch_a",
                0,
                trade_with_side(100 * SCALE, side),
            ),
            capacity_market(
                &fixture,
                0,
                base_sequence + 1,
                at,
                "epoch_a",
                0,
                MarketEvent::Quote(Quote {
                    bid_price: Price(Fixed::new(100 * SCALE, 8)),
                    bid_quantity: None,
                    ask_price: Price(Fixed::new(100 * SCALE + 1_000_000, 8)),
                    ask_quantity: None,
                }),
            ),
            capacity_market(
                &fixture,
                0,
                base_sequence + 2,
                at,
                "epoch_a",
                0,
                MarketEvent::BookSnapshot(BookSnapshot {
                    bids: vec![BookLevel {
                        price: Price(Fixed::new(100 * SCALE, 8)),
                        quantity: Quantity(Fixed::new(2 * SCALE, 8)),
                    }],
                    asks: vec![BookLevel {
                        price: Price(Fixed::new(100 * SCALE + 1_000_000, 8)),
                        quantity: Quantity(Fixed::new(2 * SCALE, 8)),
                    }],
                    depth: None,
                    checksum: None,
                }),
            ),
        ]
    };
    for input in primary_events(1, 0, AggressorSide::Buy) {
        processor.ingest(&input).unwrap();
    }
    processor
        .ingest(&capacity_market(
            &fixture,
            1,
            1,
            0,
            "epoch_a",
            0,
            MarketEvent::MarkPrice(PricePoint {
                price: Price(Fixed::new(100 * SCALE, 8)),
            }),
        ))
        .unwrap();
    ingest_capacity_controls(&mut processor, &fixture, 0, 1, [0, 0]);
    for input in primary_events(4, 60_000_000_000, AggressorSide::Buy) {
        processor.ingest(&input).unwrap();
    }
    processor
        .ingest(&capacity_market(
            &fixture,
            1,
            2,
            60_000_000_000,
            "epoch_a",
            0,
            MarketEvent::MarkPrice(PricePoint {
                price: Price(Fixed::new(100 * SCALE, 8)),
            }),
        ))
        .unwrap();
    ingest_capacity_controls(&mut processor, &fixture, 60_000_000_000, 2, [0, 0]);
    for round in 0u64..=5 {
        let at = 60_090_000_000 + i64::try_from(round).unwrap() * 200_000_000;
        for input in primary_events(
            7 + round * 3,
            at,
            if round % 2 == 0 {
                AggressorSide::Sell
            } else {
                AggressorSide::Buy
            },
        ) {
            processor.ingest(&input).unwrap();
        }
        if round < 5 {
            processor
                .ingest(&capacity_market(
                    &fixture,
                    1,
                    3 + round,
                    at,
                    "epoch_a",
                    0,
                    MarketEvent::MarkPrice(PricePoint {
                        price: Price(Fixed::new(100 * SCALE, 8)),
                    }),
                ))
                .unwrap();
        } else {
            assert!(
                processor
                    .ingest(&capacity_market(
                        &fixture,
                        1,
                        20,
                        at,
                        "epoch_a",
                        0,
                        MarketEvent::MarkPrice(PricePoint {
                            price: Price(Fixed::new(100 * SCALE, 8)),
                        }),
                    ))
                    .is_err()
            );
        }
        ingest_capacity_controls(&mut processor, &fixture, at, 3 + round, [0, 0]);
    }
    let snapshot = processor.snapshot(time_ns(61_090_000_000)).unwrap();
    assert_eq!(snapshot.value()["quality_state"], "DEGRADED");
    assert_eq!(snapshot.value()["mechanical_confidence"], "0.8");
    assert_ne!(snapshot.value()["phase"], "INVALID");
    let breadth = snapshot.value()["features"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["name"] == "cross_venue_breadth")
        .unwrap();
    assert_eq!(breadth["quality_state"], "INVALID");
    assert_eq!(breadth["reason_code"], "SOURCE_INVALIDATED");
    assert!(
        snapshot.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "SEQUENCE_GAP")
    );
}

#[test]
fn contributor_system_fault_is_scoped_to_its_book_owner_and_retires_on_greater_generation() {
    let base = split_fixture();
    let target = base.contributors[2].clone();
    let key = SystemSourceKeyV1::new(
        "z_book_system",
        FaultScopeKindV1::Contributor,
        ConfiguredTargetKeyV1::contributor(target.clone()),
        CursorModeV1::Native,
    )
    .unwrap();
    let fixture = with_system_sources(&base, vec![key.clone()]);
    let (mut processor, _) = warmed_split_processor_for(fixture);
    let fault_at = 61_100_000_000;
    processor
        .ingest(&system_input(
            &key,
            FaultScopeV1::contributor(ContributorV1::new(target.clone(), "epoch_a", 0).unwrap()),
            1,
            fault_at,
            SystemFaultV1::book_invalidated(),
        ))
        .unwrap();

    let snapshot = processor.snapshot(time_ns(fault_at)).unwrap();
    assert_eq!(snapshot.value()["quality_state"], "INVALID");
    let features = snapshot.value()["features"].as_array().unwrap();
    let book = features
        .iter()
        .find(|row| row["name"] == "book_depth_10bps")
        .unwrap();
    let trade = features
        .iter()
        .find(|row| row["name"] == "log_return")
        .unwrap();
    assert_eq!(book["reason_code"], "SOURCE_INVALIDATED");
    assert_ne!(trade["reason_code"], "SOURCE_INVALIDATED");
    assert!(
        snapshot.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "BOOK_RESYNCING")
    );

    processor
        .ingest(&market_from_source_in_epoch(
            "c_book",
            1,
            fault_at + 1_000_000,
            MarketEvent::BookSnapshot(BookSnapshot {
                bids: vec![],
                asks: vec![],
                depth: None,
                checksum: None,
            }),
            "epoch_b",
            1,
        ))
        .unwrap();
    let recovered = processor.snapshot(time_ns(fault_at + 1_000_000)).unwrap();
    assert!(
        recovered.value()["source_cursors"]
            .as_array()
            .unwrap()
            .iter()
            .all(|cursor| cursor["source_id"] != "z_book_system")
    );
    assert!(
        recovered.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .all(|flag| flag != "BOOK_RESYNCING")
    );
}

#[test]
fn connection_system_fault_expands_only_to_bound_optional_owner() {
    let base = split_fixture();
    let target = base.contributors[3].clone();
    let connection = base.config.contributor_connections()[&target].clone();
    let key = SystemSourceKeyV1::new(
        "z_connection_system",
        FaultScopeKindV1::ConnectionEpoch,
        ConfiguredTargetKeyV1::connection(connection.clone()),
        CursorModeV1::Native,
    )
    .unwrap();
    let fixture = with_system_sources(&base, vec![key.clone()]);
    let (mut processor, _) = warmed_split_processor_for(fixture);
    let decision = 61_090_000_000;
    processor
        .ingest(&system_input(
            &key,
            FaultScopeV1::connection(connection, "epoch_a", 0).unwrap(),
            1,
            decision,
            SystemFaultV1::disconnected(),
        ))
        .unwrap();
    let snapshot = processor.snapshot(time_ns(decision)).unwrap();
    assert_eq!(
        snapshot.value()["quality_state"],
        "DEGRADED",
        "{}",
        snapshot.canonical_json()
    );
    assert_eq!(snapshot.value()["mechanical_confidence"], "0.8");
    assert_ne!(snapshot.value()["phase"], "INVALID");
    let features = snapshot.value()["features"].as_array().unwrap();
    assert_eq!(
        features
            .iter()
            .find(|row| row["name"] == "open_interest_change")
            .unwrap()["reason_code"],
        "SOURCE_INVALIDATED"
    );
    assert_ne!(
        features
            .iter()
            .find(|row| row["name"] == "liquidation_notional")
            .unwrap()["reason_code"],
        "SOURCE_INVALIDATED"
    );
}

#[test]
fn processor_system_fault_expands_to_every_configured_contributor() {
    let base = split_fixture();
    let key = SystemSourceKeyV1::new(
        "z_processor_system",
        FaultScopeKindV1::Processor,
        ConfiguredTargetKeyV1::processor(base.config.processor_id()).unwrap(),
        CursorModeV1::Native,
    )
    .unwrap();
    let fixture = with_system_sources(&base, vec![key.clone()]);
    let (mut processor, _) = warmed_split_processor_for(fixture);
    let decision = 61_100_000_000;
    processor
        .ingest(&system_input(
            &key,
            FaultScopeV1::processor(base.config.processor_id()).unwrap(),
            1,
            decision,
            SystemFaultV1::clock_jump(1_000_000),
        ))
        .unwrap();
    assert!(matches!(
        processor.snapshot(time_ns(decision)),
        Err(SnapshotError::MissingClockEvidence)
    ));
}

#[test]
fn clearing_all_current_causal_state_falls_back_to_one_exact_market_record() {
    let base = split_fixture();
    let key = SystemSourceKeyV1::new(
        "z_processor_drop",
        FaultScopeKindV1::Processor,
        ConfiguredTargetKeyV1::processor(base.config.processor_id()).unwrap(),
        CursorModeV1::Native,
    )
    .unwrap();
    let fixture = with_system_sources(&base, vec![key.clone()]);
    let (mut processor, _) = warmed_split_processor_for(fixture);
    processor
        .ingest(&market_from_source_times(
            "a_trade",
            10,
            61_091_000_000,
            61_091_000_000,
            trade_with_side(100 * SCALE, AggressorSide::Buy),
        ))
        .unwrap();
    processor
        .ingest(&market_from_source_times(
            "b_quote",
            9,
            61_080_000_000,
            61_092_000_000,
            MarketEvent::Quote(Quote {
                bid_price: Price(Fixed::new(100 * SCALE, 8)),
                bid_quantity: None,
                ask_price: Price(Fixed::new(100 * SCALE + 1_000_000, 8)),
                ask_quantity: None,
            }),
        ))
        .unwrap();
    let decision = 61_093_000_000;
    processor
        .ingest(&system_input(
            &key,
            FaultScopeV1::processor(base.config.processor_id()).unwrap(),
            1,
            decision,
            SystemFaultV1::events_dropped(1, DropCategoryV1::MarketDispatch).unwrap(),
        ))
        .unwrap();

    let snapshot = processor.snapshot(time_ns(decision)).unwrap();
    assert_eq!(snapshot.value()["quality_state"], "INVALID");
    assert_eq!(
        snapshot.value()["causal_time"]["source_event_time"],
        "1970-01-01T00:01:01.080000Z"
    );
    assert_eq!(
        snapshot.value()["causal_time"]["received_at"],
        "1970-01-01T00:01:01.092000Z"
    );
    assert_eq!(
        snapshot.value()["causal_time"]["normalized_at"],
        "1970-01-01T00:01:01.092000Z"
    );
}

#[test]
fn contiguous_book_delta_updates_the_persistent_task5_projection() {
    let mut processor = warmed_processor(60_080_000_000);
    let snapshot = processor.snapshot(time_ns(60_090_000_000)).unwrap();
    let depth = snapshot.value()["features"]
        .as_array()
        .unwrap()
        .iter()
        .find(|feature| feature["name"] == "book_depth_10bps")
        .unwrap();
    assert_eq!(depth["value"], "300.1");
}

#[test]
fn base_open_interest_uses_catalog_contract_conversion() {
    let mut processor = warmed_processor_with_clock(
        60_080_000_000,
        ClockStateV1::Synchronized,
        ClockQualityV1::Validated,
        Some("5"),
    );
    let snapshot = processor.snapshot(time_ns(60_090_000_000)).unwrap();
    let oi = snapshot.value()["features"]
        .as_array()
        .unwrap()
        .iter()
        .find(|feature| feature["name"] == "open_interest_change")
        .unwrap();
    assert_eq!(oi["value"], "5");
}

#[test]
fn empty_liquidation_window_is_zero_only_with_complete_coverage() {
    let mut processor = warmed_processor(60_080_000_000);
    let snapshot = processor.snapshot(time_ns(60_090_000_000)).unwrap();
    let liquidation = snapshot.value()["features"]
        .as_array()
        .unwrap()
        .iter()
        .find(|feature| feature["name"] == "liquidation_notional")
        .unwrap();
    assert_eq!(liquidation["value"], "0");
    assert_eq!(liquidation["quality_state"], "VALIDATED");
}

#[test]
fn confirmation_price_inputs_can_author_cross_venue_breadth() {
    let primary_instrument =
        InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "HYPERLIQUID", "BNB-USDC").unwrap();
    let confirmation_instrument =
        InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "BINANCE", "BNBUSDC").unwrap();
    let primary = ContributorKeyV1::new("a_primary", primary_instrument.clone()).unwrap();
    let confirmation =
        ContributorKeyV1::new("b_confirmation", confirmation_instrument.clone()).unwrap();
    let primary_connection = ConnectionKeyV1::new("primary_connection").unwrap();
    let confirmation_connection = ConnectionKeyV1::new("confirmation_connection").unwrap();
    let primary_clock = ClockSourceKeyV1::new("z_clock_primary", primary.clone()).unwrap();
    let confirmation_clock =
        ClockSourceKeyV1::new("z_clock_confirmation", confirmation.clone()).unwrap();
    let coverage = [
        (primary.clone(), FamilyV1::Trade),
        (primary.clone(), FamilyV1::Quote),
        (primary.clone(), FamilyV1::Book),
        (confirmation.clone(), FamilyV1::ConfirmationPrice),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (subject, family))| {
        CoverageSourceKeyV1::new(&format!("z_coverage_{index}"), subject, family).unwrap()
    })
    .collect::<Vec<_>>();
    let config = MechanicsConfigV1::new(
        "event_pulse_confirmation",
        vec![primary_connection.clone(), confirmation_connection.clone()],
        vec![
            ContributorSpecV1::new(
                primary.clone(),
                ContributorRoleV1::Primary,
                [FamilyV1::Trade, FamilyV1::Quote, FamilyV1::Book],
            )
            .unwrap(),
            ContributorSpecV1::new(
                confirmation.clone(),
                ContributorRoleV1::Confirmation,
                [FamilyV1::ConfirmationPrice],
            )
            .unwrap(),
        ],
        BTreeMap::from([
            (primary.clone(), primary_connection),
            (confirmation.clone(), confirmation_connection),
        ]),
        vec![confirmation_clock.clone(), primary_clock.clone()],
        coverage.clone(),
        vec![],
    )
    .unwrap();
    let catalog = ReplayCatalogV1::new(
        BTreeMap::from([
            (
                1,
                VenueCatalogEntryV1::new("HYPERLIQUID", "a_primary").unwrap(),
            ),
            (
                2,
                VenueCatalogEntryV1::new("BINANCE", "b_confirmation").unwrap(),
            ),
        ]),
        BTreeMap::from([(1, primary_instrument), (2, confirmation_instrument)]),
        vec![
            ReplayEpochEntryV1::new(7, 9, "epoch_primary", 0).unwrap(),
            ReplayEpochEntryV1::new(8, 10, "epoch_confirmation", 0).unwrap(),
        ],
        BTreeMap::from([
            (1, OpenInterestEncodingV1::contracts()),
            (2, OpenInterestEncodingV1::contracts()),
        ]),
    )
    .unwrap();
    let event = |venue, instrument, connection, session, sequence, ns, payload| {
        MechanicsInputV1::market(
            EventEnvelope {
                schema_version: 1,
                venue: VenueId(venue),
                instrument: Some(InstrumentId(instrument)),
                connection: ConnectionId(connection),
                session: SessionId(session),
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
            catalog.clone(),
        )
        .unwrap()
    };
    let mut processor = MechanicsProcessor::new(config.clone(), authoring()).unwrap();
    for input in [
        event(1, 1, 7, 9, 1, 0, trade(100 * SCALE)),
        event(
            2,
            2,
            8,
            10,
            1,
            1_000_000,
            MarketEvent::MarkPrice(PricePoint {
                price: Price(Fixed::new(100 * SCALE, 8)),
            }),
        ),
        event(1, 1, 7, 9, 2, 59_100_000_000, trade(100 * SCALE)),
        event(
            2,
            2,
            8,
            10,
            2,
            59_101_000_000,
            MarketEvent::MarkPrice(PricePoint {
                price: Price(Fixed::new(100 * SCALE, 8)),
            }),
        ),
        event(1, 1, 7, 9, 3, 60_000_000_000, trade(101 * SCALE)),
        event(
            2,
            2,
            8,
            10,
            3,
            60_001_000_000,
            MarketEvent::MarkPrice(PricePoint {
                price: Price(Fixed::new(101 * SCALE, 8)),
            }),
        ),
        event(
            1,
            1,
            7,
            9,
            4,
            60_010_000_000,
            MarketEvent::Quote(Quote {
                bid_price: Price(Fixed::new(100 * SCALE, 8)),
                bid_quantity: None,
                ask_price: Price(Fixed::new(100 * SCALE + 10_000_000, 8)),
                ask_quantity: None,
            }),
        ),
        event(
            1,
            1,
            7,
            9,
            5,
            60_020_000_000,
            MarketEvent::BookSnapshot(BookSnapshot {
                bids: vec![BookLevel {
                    price: Price(Fixed::new(100 * SCALE, 8)),
                    quantity: Quantity(Fixed::new(SCALE, 8)),
                }],
                asks: vec![BookLevel {
                    price: Price(Fixed::new(100 * SCALE + 10_000_000, 8)),
                    quantity: Quantity(Fixed::new(SCALE, 8)),
                }],
                depth: None,
                checksum: None,
            }),
        ),
    ] {
        processor.ingest(&input).unwrap();
    }
    for (subject, key, epoch, at) in [
        (
            confirmation.clone(),
            confirmation_clock,
            "epoch_confirmation",
            60_030_000_000,
        ),
        (
            primary.clone(),
            primary_clock,
            "epoch_primary",
            60_031_000_000,
        ),
    ] {
        processor
            .ingest(
                &MechanicsInputV1::clock(
                    ContributorV1::new(subject, epoch, 0).unwrap(),
                    ClockSourceV1::new(key, "epoch_clock_a", 0).unwrap(),
                    time_ns(at),
                    time_ns(at),
                    ClockCursorV1::native(1, 1).unwrap(),
                    ClockStateV1::Synchronized,
                    CanonicalDecimal::parse("0.1", 18, 8).unwrap(),
                    2_000,
                    ClockQualityV1::Validated,
                    "SOURCE_CLOCK_WITHIN_TOLERANCE",
                )
                .unwrap(),
            )
            .unwrap();
    }
    let decision_ns = 60_050_000_000;
    for key in coverage {
        let epoch = if key.subject() == &primary {
            "epoch_primary"
        } else {
            "epoch_confirmation"
        };
        processor
            .ingest(
                &MechanicsInputV1::coverage(
                    ContributorV1::new(key.subject().clone(), epoch, 0).unwrap(),
                    CoverageSourceV1::new(key.clone(), "epoch_coverage_a", 0).unwrap(),
                    key.family(),
                    time_ns(0),
                    time_ns(decision_ns),
                    time_ns(decision_ns),
                    CoverageCursorV1::native(1, 1).unwrap(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    let snapshot = processor.snapshot(time_ns(decision_ns)).unwrap();
    let breadth = snapshot.value()["features"]
        .as_array()
        .unwrap()
        .iter()
        .find(|feature| feature["name"] == "cross_venue_breadth")
        .unwrap();
    assert_eq!(breadth["value"], "1");
}

#[test]
fn equal_time_cursor_order_is_fail_closed_and_success_seals_decision_prefix() {
    let fixture = fixture();
    let mut processor = MechanicsProcessor::new(fixture.config, authoring()).unwrap();
    processor
        .ingest(&market(2, 1_000, trade(100 * SCALE)))
        .unwrap();
    assert_eq!(
        processor.ingest(&market(1, 1_000, trade(100 * SCALE))),
        Err(SnapshotError::InputOrderRegression)
    );

    let exact_duplicate = market(2, 1_000, trade(100 * SCALE));
    assert_eq!(
        processor.ingest(&exact_duplicate).unwrap(),
        IngestOutcome::IgnoredDuplicate
    );

    let mut warmed = warmed_processor(60_200_000_000);
    let first = warmed.snapshot(time_ns(60_210_000_000)).unwrap();
    let cached = warmed.snapshot(time_ns(60_210_000_000)).unwrap();
    assert_eq!(first.canonical_json(), cached.canonical_json());
    assert_eq!(
        warmed.ingest(&market(9, 60_210_000_000, trade(102 * SCALE))),
        Err(SnapshotError::SealedInput)
    );
}

#[test]
fn unconfigured_identities_never_allocate_causes_or_mutate_cursor_state() {
    let fixture = fixture();
    let mut processor = MechanicsProcessor::new(fixture.config, authoring()).unwrap();
    for index in 0..100 {
        let base = market(1, 1_000, trade(100 * SCALE));
        let input = MechanicsInputV1::market(
            match base.view() {
                marketfeed_event_pulse::wire::MechanicsInputRefV1::Market { envelope, .. } => {
                    envelope.clone()
                }
                _ => unreachable!(),
            },
            0,
            catalog_epoch_source("epoch_a", 0, &format!("rejected_source_{index}")),
        )
        .unwrap();
        assert!(matches!(
            processor.ingest(&input),
            Err(SnapshotError::InvalidInput(_))
        ));
    }
    assert_eq!(
        processor.ingest(&market(1, 1_000, trade(100 * SCALE))),
        Ok(IngestOutcome::AcceptedWarming)
    );
    assert_eq!(processor.next_revision(), 1);
}

#[test]
fn degraded_clock_owns_quality_flag_and_feature_degradation() {
    let mut processor = warmed_processor_with_clock(
        60_080_000_000,
        ClockStateV1::Degraded,
        ClockQualityV1::Degraded,
        None,
    );
    let snapshot = processor.snapshot(time_ns(60_090_000_000)).unwrap();
    assert_ne!(snapshot.value()["quality_state"], "VALIDATED");
    assert!(
        snapshot.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "CLOCK_UNCERTAIN")
    );
    assert!(
        snapshot.value()["features"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|feature| {
                feature["value"].is_string() && feature["name"] != "reversal_from_extreme"
            })
            .all(|feature| feature["quality_state"] != "VALIDATED")
    );
}

#[test]
fn stale_current_sources_are_invalidated_with_their_own_flag() {
    let mut processor = warmed_processor(60_390_000_000);
    let snapshot = processor.snapshot(time_ns(60_400_000_000)).unwrap();
    assert_eq!(snapshot.value()["quality_state"], "INVALID");
    assert!(
        snapshot.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "SOURCE_STALE")
    );
}

#[test]
fn recovered_generation_retires_prior_failure_and_provenance() {
    let fixture = fixture();
    let mut processor = warmed_processor(60_080_000_000);
    assert!(
        processor
            .ingest(&market(10, 60_091_000_000, trade(102 * SCALE)))
            .is_err()
    );

    processor
        .ingest(&market_in_epoch(
            1,
            61_000_000_000,
            trade(102 * SCALE),
            "epoch_b",
            1,
        ))
        .unwrap();
    processor
        .ingest(&market_in_epoch(
            2,
            121_000_000_000,
            trade(103 * SCALE),
            "epoch_b",
            1,
        ))
        .unwrap();
    processor
        .ingest(&market_in_epoch(
            3,
            121_010_000_000,
            MarketEvent::Quote(Quote {
                bid_price: Price(Fixed::new(102 * SCALE, 8)),
                bid_quantity: None,
                ask_price: Price(Fixed::new(102 * SCALE + 10_000_000, 8)),
                ask_quantity: None,
            }),
            "epoch_b",
            1,
        ))
        .unwrap();
    processor
        .ingest(&market_in_epoch(
            4,
            121_020_000_000,
            MarketEvent::BookSnapshot(BookSnapshot {
                bids: vec![BookLevel {
                    price: Price(Fixed::new(102 * SCALE, 8)),
                    quantity: Quantity(Fixed::new(SCALE, 8)),
                }],
                asks: vec![BookLevel {
                    price: Price(Fixed::new(102 * SCALE + 10_000_000, 8)),
                    quantity: Quantity(Fixed::new(SCALE, 8)),
                }],
                depth: None,
                checksum: None,
            }),
            "epoch_b",
            1,
        ))
        .unwrap();
    let clock = MechanicsInputV1::clock(
        ContributorV1::new(fixture.contributor, "epoch_b", 1).unwrap(),
        ClockSourceV1::new(fixture.clock, "epoch_clock_b", 1).unwrap(),
        time_ns(121_030_000_000),
        time_ns(121_030_000_000),
        ClockCursorV1::native(1, 1).unwrap(),
        ClockStateV1::Synchronized,
        CanonicalDecimal::parse("0.25", 18, 8).unwrap(),
        2_000,
        ClockQualityV1::Validated,
        "SOURCE_CLOCK_WITHIN_TOLERANCE",
    )
    .unwrap();
    processor.ingest(&clock).unwrap();
    let snapshot = processor.snapshot(time_ns(121_040_000_000)).unwrap();
    assert!(
        !snapshot.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "SEQUENCE_GAP")
    );
    assert!(
        snapshot.value()["source_cursors"]
            .as_array()
            .unwrap()
            .iter()
            .all(|cursor| cursor["connection_epoch"] != "epoch_a")
    );
}

#[test]
fn rejected_clock_mutation_never_replaces_evidence_and_recovers_only_on_greater_generation() {
    let fixture = fixture();
    let mut processor = warmed_processor(60_080_000_000);
    assert!(
        processor
            .ingest(&clock_input(
                &fixture,
                60_100_000_000,
                "epoch_clock_a",
                0,
                1,
                "1.25",
            ))
            .is_err()
    );
    assert_eq!(
        processor.snapshot(time_ns(60_110_000_000)),
        Err(SnapshotError::MissingClockEvidence)
    );
    processor
        .ingest(&market_in_epoch(
            1,
            60_115_000_000,
            trade(102 * SCALE),
            "epoch_b",
            1,
        ))
        .unwrap();
    processor
        .ingest(&market_in_epoch(
            2,
            60_116_000_000,
            trade(103 * SCALE),
            "epoch_b",
            1,
        ))
        .unwrap();
    processor
        .ingest(&clock_input(
            &fixture,
            60_120_000_000,
            "epoch_clock_b",
            1,
            1,
            "0.25",
        ))
        .unwrap();
    ingest_coverage_round(&mut processor, &fixture, 60_125_000_000, 1, "epoch_b", 1);
    let snapshot = processor.snapshot(time_ns(60_130_000_000)).unwrap();
    assert_eq!(
        snapshot.value()["causal_time"]["available_at"],
        time_ns(60_125_000_000).canonical()
    );
    assert!(
        !snapshot.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "SEQUENCE_GAP")
    );
}

#[test]
fn rejected_market_gap_is_fault_only_not_anchor_or_availability_evidence() {
    let mut processor = warmed_processor(60_080_000_000);
    assert!(
        processor
            .ingest(&market(10, 60_100_000_000, trade(999 * SCALE)))
            .is_err()
    );
    let snapshot = processor.snapshot(time_ns(60_110_000_000)).unwrap();
    assert_eq!(
        snapshot.value()["causal_time"]["available_at"],
        time_ns(60_090_000_000).canonical()
    );
    assert_eq!(
        snapshot.value()["causal_time"]["source_event_time"],
        time_ns(60_060_000_000).canonical()
    );
    assert!(
        snapshot.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "SEQUENCE_GAP")
    );
}

#[test]
fn invalid_coverage_slot_blocks_retained_intervals_and_market_cannot_clear_it() {
    let fixture = fixture();
    let mut processor = warmed_processor(60_080_000_000);
    let trade_coverage = fixture
        .config
        .coverage_sources()
        .iter()
        .find(|key| key.family() == FamilyV1::Trade)
        .unwrap()
        .clone();
    let gap = MechanicsInputV1::coverage(
        ContributorV1::new(fixture.contributor, "epoch_a", 0).unwrap(),
        CoverageSourceV1::new(trade_coverage, "epoch_coverage_a", 0).unwrap(),
        FamilyV1::Trade,
        time_ns(0),
        time_ns(60_100_000_000),
        time_ns(60_100_000_000),
        CoverageCursorV1::native(3, 3).unwrap(),
    )
    .unwrap();
    assert!(processor.ingest(&gap).is_err());
    processor
        .ingest(&market(9, 60_110_000_000, trade(102 * SCALE)))
        .unwrap();
    let snapshot = processor.snapshot(time_ns(60_120_000_000)).unwrap();
    let log = snapshot.value()["features"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["name"] == "log_return")
        .unwrap();
    assert_eq!(log["value"], serde_json::Value::Null);
    assert_eq!(log["reason_code"], "SOURCE_INVALIDATED");
    assert!(
        snapshot.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "SEQUENCE_GAP")
    );
}

#[test]
fn snapshot_result_at_t_is_independent_of_intermediate_snapshot() {
    let direct = warmed_processor(60_080_000_000);
    let mut with_intermediate = direct.clone();
    let mut direct = direct;
    with_intermediate.snapshot(time_ns(60_085_000_000)).unwrap();
    let direct_at_t = direct.snapshot(time_ns(60_500_000_000)).unwrap();
    let intermediate_at_t = with_intermediate.snapshot(time_ns(60_500_000_000)).unwrap();
    assert_eq!(
        direct_at_t.value()["phase"],
        intermediate_at_t.value()["phase"]
    );
    assert_eq!(
        direct_at_t.value()["features"],
        intermediate_at_t.value()["features"]
    );
    assert_eq!(
        direct_at_t.value()["quality_state"],
        intermediate_at_t.value()["quality_state"]
    );
}

#[test]
fn sealed_checkpoint_evicts_old_master_records_and_later_ingest_continues() {
    let fixture = fixture();
    let mut processor = warmed_processor(60_080_000_000);
    let mut sequence = 8_u64;
    let mut at_ns = 60_100_000_000_i64;
    for batch in 0..4_u64 {
        for _ in 0..250 {
            sequence += 1;
            processor
                .ingest(&market(sequence, at_ns, trade(102 * SCALE)))
                .unwrap();
            at_ns += 100_000_000;
        }
        processor
            .ingest(&clock_input(
                &fixture,
                at_ns,
                "epoch_clock_a",
                0,
                batch + 2,
                "0.25",
            ))
            .unwrap();
        at_ns += 1_000_000;
        ingest_coverage_round(&mut processor, &fixture, at_ns, batch + 2, "epoch_a", 0);
        processor.snapshot(time_ns(at_ns)).unwrap();
        at_ns += 1_000_000;
    }
    let snapshot = processor.snapshot(time_ns(at_ns)).unwrap();
    let market_cursor = snapshot.value()["source_cursors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|cursor| cursor["source_id"] == "market_source")
        .unwrap();
    assert_eq!(market_cursor["sequence_end"], sequence);
}

#[test]
fn successful_revisions_chain_and_regressing_decisions_are_atomic() {
    let mut processor = warmed_processor(60_080_000_000);
    let first = processor.snapshot(time_ns(60_090_000_000)).unwrap();
    let second = processor.snapshot(time_ns(60_091_000_000)).unwrap();
    assert_eq!(second.revision(), 2);
    assert_eq!(
        second.value()["predecessor_content_hash"],
        first.content_hash()
    );
    assert_eq!(
        processor.snapshot(time_ns(60_089_000_000)),
        Err(SnapshotError::DecisionTimeRegression)
    );
    assert_eq!(processor.next_revision(), 3);
}

#[test]
fn causal_market_anchor_uses_componentwise_current_maxima() {
    let fixture = fixture();
    let mut processor = warmed_processor(60_080_000_000);
    for (sequence, receive_ns, exchange_ns) in [
        (9, 60_100_000_000, 60_070_000_000),
        (10, 60_110_000_000, 60_060_000_000),
    ] {
        let base = market(sequence, receive_ns, trade(102 * SCALE));
        let mut envelope = match base.view() {
            marketfeed_event_pulse::wire::MechanicsInputRefV1::Market { envelope, .. } => {
                envelope.clone()
            }
            _ => unreachable!(),
        };
        envelope.exchange_ts = Some(TimestampNs(exchange_ns));
        processor
            .ingest(&MechanicsInputV1::market(envelope, 0, catalog_epoch("epoch_a", 0)).unwrap())
            .unwrap();
    }
    ingest_coverage_round(&mut processor, &fixture, 60_120_000_000, 2, "epoch_a", 0);
    let snapshot = processor.snapshot(time_ns(60_120_000_000)).unwrap();
    assert_eq!(
        snapshot.value()["causal_time"]["source_event_time"],
        time_ns(60_070_000_000).canonical()
    );
    assert_eq!(
        snapshot.value()["causal_time"]["received_at"],
        time_ns(60_110_000_000).canonical()
    );
    assert_eq!(
        snapshot.value()["causal_time"]["normalized_at"],
        time_ns(60_110_000_000).canonical()
    );
}

#[test]
fn snapshot_seals_only_the_requested_prefix_and_retains_future_groups() {
    let mut processor = warmed_processor(60_080_000_000);
    processor
        .ingest(&market(9, 60_100_000_000, trade(102 * SCALE)))
        .unwrap();
    processor
        .ingest(&market(10, 60_200_000_000, trade(103 * SCALE)))
        .unwrap();

    let at_150 = processor.snapshot(time_ns(60_150_000_000)).unwrap();
    let market_cursor = at_150.value()["source_cursors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|cursor| cursor["source_id"] == "market_source")
        .unwrap();
    assert_eq!(market_cursor["sequence_end"], 9);

    let at_210 = processor.snapshot(time_ns(60_210_000_000)).unwrap();
    let market_cursor = at_210.value()["source_cursors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|cursor| cursor["source_id"] == "market_source")
        .unwrap();
    assert_eq!(market_cursor["sequence_end"], 10);
}

#[test]
fn missing_configured_coverage_authors_truthful_invalid_snapshot() {
    let fixture = fixture();
    let mut processor = MechanicsProcessor::new(fixture.config.clone(), authoring()).unwrap();
    processor.ingest(&market(1, 0, trade(100 * SCALE))).unwrap();
    processor
        .ingest(&market(2, 60_000_000_000, trade(101 * SCALE)))
        .unwrap();
    processor
        .ingest(
            &MechanicsInputV1::clock(
                ContributorV1::new(fixture.contributor, "epoch_a", 0).unwrap(),
                ClockSourceV1::new(fixture.clock, "epoch_clock_a", 0).unwrap(),
                time_ns(60_080_000_000),
                time_ns(60_080_000_000),
                ClockCursorV1::native(1, 1).unwrap(),
                ClockStateV1::Synchronized,
                CanonicalDecimal::parse("0.25", 18, 8).unwrap(),
                2_000,
                ClockQualityV1::Validated,
                "SOURCE_CLOCK_WITHIN_TOLERANCE",
            )
            .unwrap(),
        )
        .unwrap();
    let snapshot = processor.snapshot(time_ns(60_100_000_000)).unwrap();
    assert_eq!(snapshot.value()["quality_state"], "INVALID");
    assert!(
        snapshot.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "INSUFFICIENT_COVERAGE")
    );
    assert_eq!(processor.next_revision(), 2);
}

#[test]
fn absent_aggressive_volume_is_typed_as_insufficient_samples() {
    let fixture = fixture();
    let mut processor = MechanicsProcessor::new(fixture.config.clone(), authoring()).unwrap();
    processor.ingest(&market(1, 0, trade(100 * SCALE))).unwrap();
    processor
        .ingest(&market(
            2,
            60_000_000_000,
            trade_with_side(101 * SCALE, AggressorSide::Unknown),
        ))
        .unwrap();
    processor
        .ingest(&market(
            3,
            60_010_000_000,
            trade_with_side(102 * SCALE, AggressorSide::Unknown),
        ))
        .unwrap();
    processor
        .ingest(&clock_input(
            &fixture,
            60_080_000_000,
            "epoch_clock_a",
            0,
            1,
            "0.25",
        ))
        .unwrap();
    ingest_coverage_round(&mut processor, &fixture, 60_090_000_000, 1, "epoch_a", 0);
    let snapshot = processor.snapshot(time_ns(60_090_000_000)).unwrap();
    for name in ["taker_imbalance", "cvd_slope"] {
        let row = snapshot.value()["features"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["name"] == name)
            .unwrap();
        assert_eq!(row["value"], serde_json::Value::Null);
        assert_eq!(row["reason_code"], "INSUFFICIENT_SAMPLES");
    }
}

#[test]
fn zero_elapsed_cvd_is_typed_as_insufficient_samples_before_formula() {
    let fixture = fixture();
    let mut processor = MechanicsProcessor::new(fixture.config.clone(), authoring()).unwrap();
    processor.ingest(&market(1, 0, trade(100 * SCALE))).unwrap();
    processor
        .ingest(&market(2, 60_000_000_000, trade(101 * SCALE)))
        .unwrap();
    processor
        .ingest(&market(3, 60_000_000_000, trade(102 * SCALE)))
        .unwrap();
    processor
        .ingest(&clock_input(
            &fixture,
            60_080_000_000,
            "epoch_clock_a",
            0,
            1,
            "0.25",
        ))
        .unwrap();
    ingest_coverage_round(&mut processor, &fixture, 60_090_000_000, 1, "epoch_a", 0);
    let snapshot = processor.snapshot(time_ns(60_090_000_000)).unwrap();
    let cvd = snapshot.value()["features"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["name"] == "cvd_slope")
        .unwrap();
    assert_eq!(cvd["value"], serde_json::Value::Null);
    assert_eq!(cvd["reason_code"], "INSUFFICIENT_SAMPLES");
}

#[test]
fn negative_submicrosecond_availability_uses_floor_for_ordering_and_seal() {
    let fixture = fixture();
    let mut processor = MechanicsProcessor::new(fixture.config.clone(), authoring()).unwrap();
    processor
        .ingest(&market(1, -500_000_000, trade(100 * SCALE)))
        .unwrap();
    processor
        .ingest(&market(2, -1, trade(101 * SCALE)))
        .unwrap();
    processor
        .ingest(&market(
            3,
            -1,
            MarketEvent::Quote(Quote {
                bid_price: Price(Fixed::new(100 * SCALE, 8)),
                bid_quantity: None,
                ask_price: Price(Fixed::new(100 * SCALE + 10_000_000, 8)),
                ask_quantity: None,
            }),
        ))
        .unwrap();
    processor
        .ingest(&market(
            4,
            -1,
            MarketEvent::BookSnapshot(BookSnapshot {
                bids: vec![BookLevel {
                    price: Price(Fixed::new(100 * SCALE, 8)),
                    quantity: Quantity(Fixed::new(SCALE, 8)),
                }],
                asks: vec![BookLevel {
                    price: Price(Fixed::new(100 * SCALE + 10_000_000, 8)),
                    quantity: Quantity(Fixed::new(SCALE, 8)),
                }],
                depth: None,
                checksum: None,
            }),
        ))
        .unwrap();
    processor
        .ingest(&clock_input(&fixture, -1, "epoch_clock_a", 0, 1, "0.25"))
        .unwrap();
    for key in fixture.config.coverage_sources() {
        processor
            .ingest(
                &MechanicsInputV1::coverage(
                    ContributorV1::new(fixture.contributor.clone(), "epoch_a", 0).unwrap(),
                    CoverageSourceV1::new(key.clone(), "epoch_coverage_a", 0).unwrap(),
                    key.family(),
                    time_ns(-5_000_001_000),
                    time_ns(-1),
                    time_ns(-1),
                    CoverageCursorV1::native(1, 1).unwrap(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    let snapshot = processor.snapshot(time_ns(-1)).unwrap();
    assert_eq!(
        snapshot.value()["causal_time"]["available_at"],
        "1969-12-31T23:59:59.999999Z"
    );
    assert_eq!(
        processor.ingest(&market(5, -1, trade(102 * SCALE))),
        Err(SnapshotError::SealedInput)
    );
}

#[test]
fn public_master_capacity_drop_is_source_scoped_and_requires_greater_generation() {
    let fixture = capacity_fixture();
    let mut processor = MechanicsProcessor::new(fixture.config.clone(), authoring()).unwrap();
    processor
        .ingest(&capacity_market(
            &fixture,
            0,
            1,
            0,
            "epoch_a",
            0,
            MarketEvent::Quote(Quote {
                bid_price: Price(Fixed::new(100 * SCALE, 8)),
                bid_quantity: None,
                ask_price: Price(Fixed::new(100 * SCALE + 10_000_000, 8)),
                ask_quantity: None,
            }),
        ))
        .unwrap();
    processor
        .ingest(&capacity_market(
            &fixture,
            1,
            1,
            0,
            "epoch_a",
            0,
            MarketEvent::MarkPrice(PricePoint {
                price: Price(Fixed::new(100 * SCALE, 8)),
            }),
        ))
        .unwrap();
    for contributor_index in 0..2 {
        for sequence in 1..=32_767 {
            processor
                .ingest(
                    &MechanicsInputV1::clock(
                        ContributorV1::new(
                            fixture.contributors[contributor_index].clone(),
                            "epoch_a",
                            0,
                        )
                        .unwrap(),
                        ClockSourceV1::new(
                            fixture.clocks[contributor_index].clone(),
                            "epoch_clock_a",
                            0,
                        )
                        .unwrap(),
                        time_ns(1_000_000),
                        time_ns(1_000_000),
                        ClockCursorV1::native(sequence, sequence).unwrap(),
                        ClockStateV1::Synchronized,
                        CanonicalDecimal::parse("0.25", 18, 8).unwrap(),
                        2_000,
                        ClockQualityV1::Validated,
                        "SOURCE_CLOCK_WITHIN_TOLERANCE",
                    )
                    .unwrap(),
                )
                .unwrap();
        }
    }
    assert_eq!(2 + 2 * 32_767, PROCESSOR_RECORD_CAPACITY);

    let overflow = capacity_market(
        &fixture,
        1,
        2,
        2_000_000,
        "epoch_a",
        0,
        MarketEvent::MarkPrice(PricePoint {
            price: Price(Fixed::new(101 * SCALE, 8)),
        }),
    );
    assert!(matches!(
        processor.ingest(&overflow),
        Err(SnapshotError::InvalidInput(message))
            if message == "bounded processor queue dropped the unaccepted input"
    ));
    assert!(processor.ingest(&overflow).is_err());
    processor
        .ingest(&capacity_market(
            &fixture,
            1,
            1,
            3_000_000,
            "epoch_b",
            1,
            MarketEvent::MarkPrice(PricePoint {
                price: Price(Fixed::new(101 * SCALE, 8)),
            }),
        ))
        .unwrap();
    processor
        .ingest(&capacity_market(
            &fixture,
            1,
            2,
            60_003_000_000,
            "epoch_b",
            1,
            MarketEvent::MarkPrice(PricePoint {
                price: Price(Fixed::new(101 * SCALE + 1, 8)),
            }),
        ))
        .unwrap();
    processor
        .ingest(&capacity_market(
            &fixture,
            0,
            2,
            60_004_000_000,
            "epoch_a",
            0,
            MarketEvent::Quote(Quote {
                bid_price: Price(Fixed::new(100 * SCALE, 8)),
                bid_quantity: None,
                ask_price: Price(Fixed::new(100 * SCALE + 10_000_000, 8)),
                ask_quantity: None,
            }),
        ))
        .unwrap();

    processor
        .ingest(
            &MechanicsInputV1::clock(
                ContributorV1::new(fixture.contributors[0].clone(), "epoch_a", 0).unwrap(),
                ClockSourceV1::new(fixture.clocks[0].clone(), "epoch_clock_a", 0).unwrap(),
                time_ns(60_009_000_000),
                time_ns(60_009_000_000),
                ClockCursorV1::native(32_768, 32_768).unwrap(),
                ClockStateV1::Synchronized,
                CanonicalDecimal::parse("0.25", 18, 8).unwrap(),
                2_000,
                ClockQualityV1::Validated,
                "SOURCE_CLOCK_WITHIN_TOLERANCE",
            )
            .unwrap(),
        )
        .unwrap();
    processor
        .ingest(
            &MechanicsInputV1::clock(
                ContributorV1::new(fixture.contributors[1].clone(), "epoch_b", 1).unwrap(),
                ClockSourceV1::new(fixture.clocks[1].clone(), "epoch_clock_b", 1).unwrap(),
                time_ns(60_010_000_000),
                time_ns(60_010_000_000),
                ClockCursorV1::native(1, 1).unwrap(),
                ClockStateV1::Synchronized,
                CanonicalDecimal::parse("0.25", 18, 8).unwrap(),
                2_000,
                ClockQualityV1::Validated,
                "SOURCE_CLOCK_WITHIN_TOLERANCE",
            )
            .unwrap(),
        )
        .unwrap();
    let decision = 60_100_000_000;
    for (index, coverage) in fixture.config.coverage_sources().iter().enumerate() {
        let contributor_index = fixture
            .contributors
            .iter()
            .position(|candidate| candidate == coverage.subject())
            .unwrap();
        let recovered = contributor_index == 1;
        let at = 60_040_000_000 + i64::try_from(index).unwrap() * 1_000_000;
        processor
            .ingest(
                &MechanicsInputV1::coverage(
                    ContributorV1::new(
                        coverage.subject().clone(),
                        if recovered { "epoch_b" } else { "epoch_a" },
                        u8::from(recovered),
                    )
                    .unwrap(),
                    CoverageSourceV1::new(
                        coverage.clone(),
                        if recovered {
                            "epoch_coverage_b"
                        } else {
                            "epoch_coverage_a"
                        },
                        u8::from(recovered),
                    )
                    .unwrap(),
                    coverage.family(),
                    time_ns(55_000_000_000),
                    time_ns(at),
                    time_ns(at),
                    CoverageCursorV1::native(1, 1).unwrap(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    let snapshot = processor.snapshot(time_ns(decision)).unwrap();
    let cursors = snapshot.value()["source_cursors"].as_array().unwrap();
    let unrelated = cursors
        .iter()
        .find(|cursor| cursor["source_id"] == "x_clock_00")
        .unwrap();
    assert_eq!(unrelated["sequence_end"], 32_768);
    let recovered = cursors
        .iter()
        .find(|cursor| cursor["source_id"] == "b_confirmation")
        .expect("recovered contributor cursor");
    assert_eq!(recovered["connection_epoch"], "epoch_b");
    assert_eq!(recovered["sequence_end"], 2);
    assert!(
        !snapshot.value()["quality_flags"]
            .as_array()
            .unwrap()
            .iter()
            .any(|flag| flag == "QUEUE_DROP")
    );
}
