use std::collections::BTreeMap;

use marketfeed_event_pulse::{
    ContractBundle, IngestOutcome,
    features::{Direction, SCALE},
    mechanics::{FamilyFlags, MechanicsEvidence, Phase, PhaseMachine},
    snapshot::{MechanicsProcessor, SnapshotError},
    wire::{
        CanonicalDecimal, ClockCursorV1, ClockQualityV1, ClockSourceKeyV1, ClockSourceV1,
        ClockStateV1, ConnectionKeyV1, ContributorKeyV1, ContributorRoleV1, ContributorSpecV1,
        ContributorV1, CoverageSourceKeyV1, FamilyV1, InstrumentIdentityV1, MechanicsConfigV1,
        MechanicsInputV1, OpenInterestEncodingV1, ReplayCatalogV1, ReplayEpochEntryV1, Rfc3339Time,
        SnapshotAuthoringV1, VenueCatalogEntryV1,
    },
};
use marketfeed_model::{
    AggressorSide, BookLevel, BookSnapshot, ConnectionId, EventEnvelope, EventFlags, Fixed,
    InstrumentId, MarketEvent, Price, Quantity, Quote, SequenceRange, SessionId, TimestampNs,
    Trade, VenueId,
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
    ReplayCatalogV1::new(
        BTreeMap::from([(
            1,
            VenueCatalogEntryV1::new("HYPERLIQUID", "market_source").unwrap(),
        )]),
        BTreeMap::from([(
            1,
            InstrumentIdentityV1::new("BNB", "USDC", "PERPETUAL", "HYPERLIQUID", "BNB-USDC")
                .unwrap(),
        )]),
        vec![ReplayEpochEntryV1::new(7, 9, epoch, generation).unwrap()],
        BTreeMap::from([(1, OpenInterestEncodingV1::contracts())]),
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

fn warmed_processor_with_clock(
    clock_ns: i64,
    clock_state: ClockStateV1,
    quality_state: ClockQualityV1,
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
    let contributor = ContributorV1::new(fixture.contributor, "epoch_a", 0).unwrap();
    let clock = MechanicsInputV1::clock(
        contributor,
        ClockSourceV1::new(fixture.clock, "epoch_clock_a", 0).unwrap(),
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
    processor
}

fn warmed_processor(clock_ns: i64) -> MechanicsProcessor {
    warmed_processor_with_clock(
        clock_ns,
        ClockStateV1::Synchronized,
        ClockQualityV1::Validated,
    )
}

#[test]
fn public_processor_derives_snapshot_from_validated_inputs_and_clock_maximum() {
    let clock_ns = 60_080_000_000;
    let mut processor = warmed_processor(clock_ns);
    let snapshot = processor.snapshot(time_ns(60_090_000_000)).unwrap();
    assert_eq!(
        snapshot.value()["causal_time"]["available_at"],
        time_ns(clock_ns).canonical()
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
        warmed.ingest(&market(6, 60_210_000_000, trade(102 * SCALE))),
        Err(SnapshotError::SealedInput)
    );
}

#[test]
fn degraded_clock_owns_quality_flag_and_feature_degradation() {
    let mut processor = warmed_processor_with_clock(
        60_080_000_000,
        ClockStateV1::Degraded,
        ClockQualityV1::Degraded,
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
            .ingest(&market(7, 60_091_000_000, trade(102 * SCALE)))
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
fn missing_clock_failure_is_atomic_and_does_not_consume_revision() {
    let fixture = fixture();
    let mut processor = MechanicsProcessor::new(fixture.config, authoring()).unwrap();
    processor.ingest(&market(1, 0, trade(100 * SCALE))).unwrap();
    assert_eq!(
        processor.snapshot(time_ns(100_000_000)),
        Err(SnapshotError::MissingClockEvidence)
    );
    assert_eq!(processor.next_revision(), 1);
}
