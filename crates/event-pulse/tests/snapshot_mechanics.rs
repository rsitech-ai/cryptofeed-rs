use std::collections::BTreeMap;

use marketfeed_event_pulse::{
    ContractBundle, IngestOutcome,
    features::{Direction, SCALE},
    mechanics::{FamilyFlags, MechanicsEvidence, Phase, PhaseMachine},
    snapshot::{MechanicsProcessor, SnapshotError},
    wire::{
        CanonicalDecimal, ClockCursorV1, ClockQualityV1, ClockSourceKeyV1, ClockSourceV1,
        ClockStateV1, ConnectionKeyV1, ContributorKeyV1, ContributorRoleV1, ContributorSpecV1,
        ContributorV1, CoverageCursorV1, CoverageSourceKeyV1, CoverageSourceV1, FamilyV1,
        InstrumentIdentityV1, MechanicsConfigV1, MechanicsInputV1, OpenInterestEncodingV1,
        ReplayCatalogV1, ReplayEpochEntryV1, Rfc3339Time, SnapshotAuthoringV1, VenueCatalogEntryV1,
    },
};
use marketfeed_model::{
    AggressorSide, BookChange, BookDelta, BookLevel, BookOperation, BookSide, BookSnapshot,
    ConnectionId, EventEnvelope, EventFlags, Fixed, InstrumentId, MarketEvent, OpenInterest, Price,
    PricePoint, Quantity, Quote, SequenceRange, SessionId, TimestampNs, Trade, VenueId,
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

fn trade(price: i128) -> MarketEvent {
    MarketEvent::Trade(Trade {
        price: Price(Fixed::new(price, 8)),
        quantity: Quantity(Fixed::new(SCALE, 8)),
        aggressor: AggressorSide::Buy,
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
