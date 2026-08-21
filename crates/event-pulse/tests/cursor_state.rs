use std::collections::BTreeMap;

use marketfeed_event_pulse::{
    CursorError, IngestOutcome, SlotState, SourceStateMachine,
    wire::{
        ClockCursorV1, ClockQualityV1, ClockSourceKeyV1, ClockSourceV1, ClockStateV1,
        ConfiguredTargetKeyV1, ConnectionKeyV1, ContributorKeyV1, ContributorRoleV1,
        ContributorSpecV1, ContributorV1, CoverageCursorV1, CoverageSourceKeyV1, CoverageSourceV1,
        CursorModeV1, CursorV1, FamilyV1, FaultScopeKindV1, FaultScopeV1, InstrumentIdentityV1,
        MechanicsConfigV1, MechanicsInputV1, OpenInterestEncodingV1, ReplayCatalogV1,
        ReplayEpochEntryV1, Rfc3339Time, SystemChainPreimage, SystemFaultV1, SystemSourceKeyV1,
        SystemSourceV1, VenueCatalogEntryV1,
    },
};
use marketfeed_model::{
    AggressorSide, ConnectionId, EventEnvelope, EventFlags, Fixed, InstrumentId, MarketEvent,
    Price, Quantity, SequenceRange, SessionId, TimestampNs, Trade, VenueId,
};

#[derive(Clone)]
struct Fixture {
    config: MechanicsConfigV1,
    primary: ContributorKeyV1,
    sibling: ContributorKeyV1,
    connection: ConnectionKeyV1,
    clock: ClockSourceKeyV1,
    coverage: CoverageSourceKeyV1,
    system: SystemSourceKeyV1,
    processor_system: SystemSourceKeyV1,
    book_system: SystemSourceKeyV1,
}

fn fixture() -> Fixture {
    let primary_instrument =
        InstrumentIdentityV1::new("BTC", "USDT", "PERPETUAL", "BINANCE", "BTCUSDT").unwrap();
    let sibling_instrument =
        InstrumentIdentityV1::new("BTC", "USDT", "PERPETUAL", "HYPERLIQUID", "BTC").unwrap();
    let primary = ContributorKeyV1::new("binance_market", primary_instrument).unwrap();
    let sibling = ContributorKeyV1::new("hyper_market", sibling_instrument).unwrap();
    let connection = ConnectionKeyV1::new("market_connection").unwrap();
    let clock = ClockSourceKeyV1::new("binance_clock", primary.clone()).unwrap();
    let sibling_clock = ClockSourceKeyV1::new("hyper_clock", sibling.clone()).unwrap();
    let coverage =
        CoverageSourceKeyV1::new("binance_trade_coverage", primary.clone(), FamilyV1::Trade)
            .unwrap();
    let coverage_quote =
        CoverageSourceKeyV1::new("binance_quote_coverage", primary.clone(), FamilyV1::Quote)
            .unwrap();
    let coverage_book =
        CoverageSourceKeyV1::new("binance_book_coverage", primary.clone(), FamilyV1::Book).unwrap();
    let coverage_confirmation = CoverageSourceKeyV1::new(
        "hyper_confirmation_coverage",
        sibling.clone(),
        FamilyV1::ConfirmationPrice,
    )
    .unwrap();
    let system = SystemSourceKeyV1::new(
        "connection_system",
        FaultScopeKindV1::ConnectionEpoch,
        ConfiguredTargetKeyV1::connection(connection.clone()),
        CursorModeV1::Native,
    )
    .unwrap();
    let processor_system = SystemSourceKeyV1::new(
        "processor_system",
        FaultScopeKindV1::Processor,
        ConfiguredTargetKeyV1::processor("event_pulse_processor").unwrap(),
        CursorModeV1::Derived,
    )
    .unwrap();
    let book_system = SystemSourceKeyV1::new(
        "book_system",
        FaultScopeKindV1::Contributor,
        ConfiguredTargetKeyV1::contributor(primary.clone()),
        CursorModeV1::Native,
    )
    .unwrap();
    let specs = vec![
        ContributorSpecV1::new(
            primary.clone(),
            ContributorRoleV1::Primary,
            [FamilyV1::Trade, FamilyV1::Quote, FamilyV1::Book],
        )
        .unwrap(),
        ContributorSpecV1::new(
            sibling.clone(),
            ContributorRoleV1::Confirmation,
            [FamilyV1::ConfirmationPrice],
        )
        .unwrap(),
    ];
    let mut bindings = BTreeMap::new();
    bindings.insert(primary.clone(), connection.clone());
    bindings.insert(sibling.clone(), connection.clone());
    let config = MechanicsConfigV1::new(
        "event_pulse_processor",
        vec![connection.clone()],
        specs,
        bindings,
        vec![clock.clone(), sibling_clock],
        vec![
            coverage.clone(),
            coverage_quote,
            coverage_book,
            coverage_confirmation,
        ],
        vec![
            system.clone(),
            processor_system.clone(),
            book_system.clone(),
        ],
    )
    .unwrap();
    Fixture {
        config,
        primary,
        sibling,
        connection,
        clock,
        coverage,
        system,
        processor_system,
        book_system,
    }
}

fn catalog(epoch: &str, generation: u8) -> ReplayCatalogV1 {
    ReplayCatalogV1::new(
        BTreeMap::from([
            (
                1,
                VenueCatalogEntryV1::new("BINANCE", "binance_market").unwrap(),
            ),
            (
                2,
                VenueCatalogEntryV1::new("HYPERLIQUID", "hyper_market").unwrap(),
            ),
        ]),
        BTreeMap::from([
            (
                1,
                InstrumentIdentityV1::new("BTC", "USDT", "PERPETUAL", "BINANCE", "BTCUSDT")
                    .unwrap(),
            ),
            (
                2,
                InstrumentIdentityV1::new("BTC", "USDT", "PERPETUAL", "HYPERLIQUID", "BTC")
                    .unwrap(),
            ),
        ]),
        vec![ReplayEpochEntryV1::new(7, 9, epoch, generation).unwrap()],
        BTreeMap::from([(1, OpenInterestEncodingV1::contracts())]),
    )
    .unwrap()
}

fn market(
    venue: u16,
    instrument: u32,
    sequence: (u64, u64),
    ns: i64,
    epoch: &str,
    generation: u8,
) -> MechanicsInputV1 {
    market_with_payload(
        venue,
        instrument,
        sequence,
        ns,
        epoch,
        generation,
        MarketEvent::Trade(Trade {
            price: Price(Fixed::new(100, 0)),
            quantity: Quantity(Fixed::new(1, 0)),
            aggressor: AggressorSide::Buy,
            trade_id: None,
        }),
    )
}

fn market_with_payload(
    venue: u16,
    instrument: u32,
    sequence: (u64, u64),
    ns: i64,
    epoch: &str,
    generation: u8,
    payload: MarketEvent,
) -> MechanicsInputV1 {
    MechanicsInputV1::market(
        EventEnvelope {
            schema_version: 1,
            venue: VenueId(venue),
            instrument: Some(InstrumentId(instrument)),
            connection: ConnectionId(7),
            session: SessionId(9),
            frame_seq: sequence.0,
            event_index: 0,
            exchange_ts: Some(TimestampNs(ns)),
            receive_ts: TimestampNs(ns),
            source_sequence: Some(SequenceRange {
                first: sequence.0,
                last: sequence.1,
            }),
            flags: EventFlags::empty(),
            payload,
        },
        0,
        catalog(epoch, generation),
    )
    .unwrap()
}

fn time(second: u8) -> Rfc3339Time {
    Rfc3339Time::parse(&format!("2026-08-21T10:00:{second:02}Z")).unwrap()
}

#[test]
fn connection_fanout_sibling_attach_and_exact_warmup_are_bounded() {
    let f = fixture();
    let mut state = SourceStateMachine::new(f.config.clone());
    assert_eq!(
        state
            .ingest(&market(1, 1, (1, 1), 0, "epoch_a", 0))
            .unwrap(),
        IngestOutcome::AcceptedWarming
    );
    assert_eq!(
        state.contributor_state(&f.primary),
        Some(SlotState::Warming)
    );
    assert_eq!(state.contributor_state(&f.sibling), Some(SlotState::Cold));
    assert_eq!(
        state
            .ingest(&market(2, 2, (1, 1), 1, "epoch_a", 0))
            .unwrap(),
        IngestOutcome::AcceptedWarming
    );
    assert_eq!(
        state
            .ingest(&market(1, 1, (2, 2), 59_999_999_999, "epoch_a", 0))
            .unwrap(),
        IngestOutcome::AcceptedWarming
    );
    assert_eq!(
        state
            .ingest(&market(1, 1, (3, 3), 60_000_000_000, "epoch_a", 0))
            .unwrap(),
        IngestOutcome::AcceptedLive
    );

    assert_eq!(
        state
            .ingest(&market(1, 1, (1, 1), 61_000_000_000, "epoch_b", 1))
            .unwrap(),
        IngestOutcome::AcceptedWarming
    );
    assert_eq!(state.contributor_state(&f.sibling), Some(SlotState::Cold));
    assert_eq!(
        state
            .ingest(&market(2, 2, (2, 2), 61_000_000_001, "epoch_b", 1))
            .unwrap(),
        IngestOutcome::AcceptedWarming
    );
}

#[test]
fn native_duplicate_is_ignored_but_mutation_overlap_gap_and_time_regression_invalidate() {
    let f = fixture();
    let mut state = SourceStateMachine::new(f.config.clone());
    let first = market(1, 1, (1, 1), 0, "epoch_a", 0);
    state.ingest(&first).unwrap();
    assert_eq!(
        state.ingest(&first).unwrap(),
        IngestOutcome::IgnoredDuplicate
    );
    let gap = market(1, 1, (3, 3), 1, "epoch_a", 0);
    assert_eq!(state.ingest(&gap), Err(CursorError::NativeGap));
    assert_eq!(
        state.contributor_state(&f.primary),
        Some(SlotState::Invalid)
    );
    assert_eq!(
        state.ingest(&market(1, 1, (2, 2), 2, "epoch_a", 0)),
        Err(CursorError::EpochMismatch)
    );
    assert_eq!(
        state
            .ingest(&market(1, 1, (1, 1), 3, "epoch_b", 1))
            .unwrap(),
        IngestOutcome::AcceptedWarming
    );

    let mut state = SourceStateMachine::new(f.config);
    state
        .ingest(&market(1, 1, (10, 12), 10, "epoch_a", 0))
        .unwrap();
    assert_eq!(
        state.ingest(&market(1, 1, (12, 13), 11, "epoch_a", 0)),
        Err(CursorError::NativeOverlap)
    );
}

#[test]
fn native_mutation_regression_and_availability_regression_are_distinct() {
    let f = fixture();
    let first = market(1, 1, (5, 5), 10, "epoch_a", 0);

    let mut mutation_state = SourceStateMachine::new(f.config.clone());
    mutation_state.ingest(&first).unwrap();
    let mutation = market(1, 1, (5, 5), 11, "epoch_a", 0);
    assert_eq!(
        mutation_state.ingest(&mutation),
        Err(CursorError::MutatedDuplicate)
    );

    let mut regression_state = SourceStateMachine::new(f.config.clone());
    regression_state.ingest(&first).unwrap();
    assert_eq!(
        regression_state.ingest(&market(1, 1, (3, 3), 11, "epoch_a", 0)),
        Err(CursorError::NativeRegression)
    );

    let mut time_state = SourceStateMachine::new(f.config);
    time_state.ingest(&first).unwrap();
    assert_eq!(
        time_state.ingest(&market(1, 1, (6, 6), 9, "epoch_a", 0)),
        Err(CursorError::AvailabilityRegression)
    );
}

#[test]
fn coverage_and_clock_cannot_initialize_or_cross_subject_epochs() {
    let f = fixture();
    let contributor = ContributorV1::new(f.primary.clone(), "epoch_a", 0).unwrap();
    let coverage = MechanicsInputV1::coverage(
        contributor.clone(),
        CoverageSourceV1::new(f.coverage.clone(), "epoch_cov", 0).unwrap(),
        FamilyV1::Trade,
        time(0),
        time(1),
        time(1),
        CoverageCursorV1::native(1, 1).unwrap(),
    )
    .unwrap();
    let clock = MechanicsInputV1::clock(
        contributor,
        ClockSourceV1::new(f.clock.clone(), "epoch_clock", 0).unwrap(),
        time(1),
        time(1),
        ClockCursorV1::native(1, 1).unwrap(),
        ClockStateV1::Synchronized,
        marketfeed_event_pulse::wire::CanonicalDecimal::parse("0", 18, 8).unwrap(),
        1000,
        ClockQualityV1::Validated,
        "SOURCE_CLOCK_VALID",
    )
    .unwrap();
    let mut state = SourceStateMachine::new(f.config);
    assert_eq!(
        state.ingest(&coverage),
        Err(CursorError::SubjectNotInitialized)
    );
    assert_eq!(
        state.ingest(&clock),
        Err(CursorError::SubjectNotInitialized)
    );
    state
        .ingest(&market(1, 1, (1, 1), 0, "epoch_a", 0))
        .unwrap();
    state.ingest(&coverage).unwrap();
    state.ingest(&clock).unwrap();
    assert!(state.coverage_cursor(&f.coverage).is_some());
    assert!(state.clock_cursor(&f.clock).is_some());
}

#[test]
fn connection_advance_clears_evidence_but_same_reporting_epoch_can_continue() {
    let f = fixture();
    let mut state = SourceStateMachine::new(f.config);
    state
        .ingest(&market(1, 1, (1, 1), 0, "epoch_a", 0))
        .unwrap();
    let old_subject = ContributorV1::new(f.primary.clone(), "epoch_a", 0).unwrap();
    let first_clock = MechanicsInputV1::clock(
        old_subject,
        ClockSourceV1::new(f.clock.clone(), "epoch_clock", 0).unwrap(),
        time(1),
        time(1),
        ClockCursorV1::native(1, 1).unwrap(),
        ClockStateV1::Synchronized,
        marketfeed_event_pulse::wire::CanonicalDecimal::parse("0", 18, 8).unwrap(),
        1000,
        ClockQualityV1::Validated,
        "SOURCE_CLOCK_VALID",
    )
    .unwrap();
    state.ingest(&first_clock).unwrap();
    state
        .ingest(&market(1, 1, (1, 1), 2, "epoch_b", 1))
        .unwrap();
    assert_eq!(state.clock_cursor(&f.clock), None);

    let current_subject = ContributorV1::new(f.primary.clone(), "epoch_b", 1).unwrap();
    let continued_clock = MechanicsInputV1::clock(
        current_subject,
        ClockSourceV1::new(f.clock.clone(), "epoch_clock", 0).unwrap(),
        time(2),
        time(2),
        ClockCursorV1::native(2, 2).unwrap(),
        ClockStateV1::Synchronized,
        marketfeed_event_pulse::wire::CanonicalDecimal::parse("0", 18, 8).unwrap(),
        1000,
        ClockQualityV1::Validated,
        "SOURCE_CLOCK_VALID",
    )
    .unwrap();
    state.ingest(&continued_clock).unwrap();
    assert!(state.clock_cursor(&f.clock).is_some());
}

#[test]
fn system_chain_checks_duplicate_before_predecessor_and_disconnect_expands_atomically() {
    let f = fixture();
    let mut state = SourceStateMachine::new(f.config);
    state
        .ingest(&market(1, 1, (1, 1), 0, "epoch_a", 0))
        .unwrap();
    state
        .ingest(&market(2, 2, (1, 1), 1, "epoch_a", 0))
        .unwrap();
    let source = SystemSourceV1::new(f.system.clone(), "epoch_system", 0).unwrap();
    let scope = FaultScopeV1::connection(f.connection.clone(), "epoch_a", 0).unwrap();
    let first = MechanicsInputV1::system(
        source.clone(),
        scope.clone(),
        time(1),
        time(1),
        CursorV1::native(1, 1).unwrap(),
        SystemFaultV1::disconnected(),
        None,
    )
    .unwrap();
    let head = SystemChainPreimage::hash_first(first.payload_hash()).unwrap();
    assert_eq!(state.ingest(&first).unwrap(), IngestOutcome::Invalidated);
    assert_eq!(
        state.ingest(&first).unwrap(),
        IngestOutcome::IgnoredDuplicate
    );
    assert_eq!(state.system_chain_head(&f.system), Some(head.as_str()));
    assert_eq!(
        state.contributor_state(&f.primary),
        Some(SlotState::Invalid)
    );
    assert_eq!(
        state.contributor_state(&f.sibling),
        Some(SlotState::Invalid)
    );

    let bad = MechanicsInputV1::system(
        source,
        scope,
        time(2),
        time(2),
        CursorV1::native(2, 2).unwrap(),
        SystemFaultV1::disconnected(),
        Some("aa".repeat(32)),
    )
    .unwrap();
    assert_eq!(state.ingest(&bad), Err(CursorError::SystemPredecessor));
    assert_eq!(state.system_chain_head(&f.system), Some(head.as_str()));
}

#[test]
fn contributor_cursor_mode_change_invalidates_without_borrowing_contiguity() {
    let f = fixture();
    let mut state = SourceStateMachine::new(f.config);
    state
        .ingest(&market(1, 1, (1, 1), 0, "epoch_a", 0))
        .unwrap();
    let mut derived = market(1, 1, (2, 2), 1, "epoch_a", 0);
    let mut value = serde_json::to_value(&derived).unwrap();
    value["envelope"]["source_sequence"] = serde_json::Value::Null;
    value.as_object_mut().unwrap().remove("payload_hash");
    derived = MechanicsInputV1::market(
        serde_json::from_value(value["envelope"].clone()).unwrap(),
        0,
        catalog("epoch_a", 0),
    )
    .unwrap();
    assert_eq!(state.ingest(&derived), Err(CursorError::CursorMode));
    assert_eq!(
        state.contributor_state(&f.primary),
        Some(SlotState::Invalid)
    );
}

#[test]
fn derived_coordinates_allow_jumps_but_reject_regression() {
    let f = fixture();
    let mut first = market(1, 1, (1, 1), 1, "epoch_a", 0);
    let mut value = serde_json::to_value(&first).unwrap();
    value["envelope"]["source_sequence"] = serde_json::Value::Null;
    first = MechanicsInputV1::market(
        serde_json::from_value(value["envelope"].clone()).unwrap(),
        0,
        catalog("epoch_a", 0),
    )
    .unwrap();
    let mut state = SourceStateMachine::new(f.config);
    state.ingest(&first).unwrap();

    let mut later_envelope = match first.view() {
        marketfeed_event_pulse::wire::MechanicsInputRefV1::Market { envelope, .. } => {
            envelope.clone()
        }
        _ => unreachable!(),
    };
    later_envelope.frame_seq = 100;
    later_envelope.receive_ts = TimestampNs(2);
    later_envelope.exchange_ts = Some(TimestampNs(2));
    let later =
        MechanicsInputV1::market(later_envelope.clone(), 20, catalog("epoch_a", 0)).unwrap();
    assert_eq!(
        state.ingest(&later).unwrap(),
        IngestOutcome::AcceptedWarming
    );
    later_envelope.frame_seq = 50;
    later_envelope.receive_ts = TimestampNs(3);
    later_envelope.exchange_ts = Some(TimestampNs(3));
    let regression = MechanicsInputV1::market(later_envelope, 20, catalog("epoch_a", 0)).unwrap();
    assert_eq!(
        state.ingest(&regression),
        Err(CursorError::DerivedRegression)
    );
}

#[test]
fn all_256_epoch_names_are_bounded_and_reuse_fails_closed() {
    let f = fixture();
    let mut state = SourceStateMachine::new(f.config);
    for generation in 0..=u8::MAX {
        let epoch = format!("epoch_{generation}");
        assert_eq!(
            state
                .ingest(&market(
                    1,
                    1,
                    (1, 1),
                    i64::from(generation),
                    &epoch,
                    generation
                ))
                .unwrap(),
            IngestOutcome::AcceptedWarming
        );
    }
    assert_eq!(
        state.ingest(&market(1, 1, (1, 1), 300, "epoch_0", u8::MAX)),
        Err(CursorError::EpochMismatch)
    );
    assert_eq!(
        state.contributor_state(&f.primary),
        Some(SlotState::Invalid)
    );
}

#[test]
fn reporting_epoch_carries_chain_and_target_recovery_retires_it() {
    let f = fixture();
    let mut state = SourceStateMachine::new(f.config);
    state
        .ingest(&market(1, 1, (1, 1), 0, "epoch_a", 0))
        .unwrap();
    let scope = FaultScopeV1::connection(f.connection.clone(), "epoch_a", 0).unwrap();
    let first = MechanicsInputV1::system(
        SystemSourceV1::new(f.system.clone(), "epoch_system_0", 0).unwrap(),
        scope.clone(),
        time(1),
        time(1),
        CursorV1::native(1, 1).unwrap(),
        SystemFaultV1::disconnected(),
        None,
    )
    .unwrap();
    state.ingest(&first).unwrap();
    let first_head = state.system_chain_head(&f.system).unwrap().to_owned();
    let reporting = MechanicsInputV1::system(
        SystemSourceV1::new(f.system.clone(), "epoch_system_1", 1).unwrap(),
        scope,
        time(2),
        time(2),
        CursorV1::native(1, 1).unwrap(),
        SystemFaultV1::disconnected(),
        Some(first_head),
    )
    .unwrap();
    state.ingest(&reporting).unwrap();
    assert!(state.system_chain_head(&f.system).is_some());
    state
        .ingest(&market(1, 1, (1, 1), 3, "epoch_b", 1))
        .unwrap();
    assert_eq!(state.system_chain_head(&f.system), None);
    let recovered_scope = FaultScopeV1::connection(f.connection.clone(), "epoch_b", 1).unwrap();
    let restarted_chain = MechanicsInputV1::system(
        SystemSourceV1::new(f.system.clone(), "epoch_system_1", 1).unwrap(),
        recovered_scope,
        time(3),
        time(3),
        CursorV1::native(2, 2).unwrap(),
        SystemFaultV1::disconnected(),
        None,
    )
    .unwrap();
    state.ingest(&restarted_chain).unwrap();
    assert!(state.system_chain_head(&f.system).is_some());
}

#[test]
fn processor_clock_jump_uses_reserved_derived_cursor_and_clears_clock_observations() {
    let f = fixture();
    let mut state = SourceStateMachine::new(f.config);
    state
        .ingest(&market(1, 1, (1, 1), 0, "epoch_a", 0))
        .unwrap();
    let contributor = ContributorV1::new(f.primary.clone(), "epoch_a", 0).unwrap();
    let clock = MechanicsInputV1::clock(
        contributor,
        ClockSourceV1::new(f.clock.clone(), "epoch_clock", 0).unwrap(),
        time(1),
        time(1),
        ClockCursorV1::native(1, 1).unwrap(),
        ClockStateV1::Synchronized,
        marketfeed_event_pulse::wire::CanonicalDecimal::parse("0", 18, 8).unwrap(),
        1000,
        ClockQualityV1::Validated,
        "SOURCE_CLOCK_VALID",
    )
    .unwrap();
    state.ingest(&clock).unwrap();
    let fault = MechanicsInputV1::system(
        SystemSourceV1::new(f.processor_system.clone(), "epoch_system", 0).unwrap(),
        FaultScopeV1::processor("event_pulse_processor").unwrap(),
        time(2),
        time(2),
        CursorV1::derived(7, 0, 0).unwrap(),
        SystemFaultV1::clock_jump(1_000),
        None,
    )
    .unwrap();
    assert_eq!(state.ingest(&fault).unwrap(), IngestOutcome::Invalidated);
    assert_eq!(state.clock_cursor(&f.clock), None);
    assert_eq!(
        state.contributor_state(&f.primary),
        Some(SlotState::Invalid)
    );
    let same_epoch_clock = MechanicsInputV1::clock(
        ContributorV1::new(f.primary.clone(), "epoch_a", 0).unwrap(),
        ClockSourceV1::new(f.clock.clone(), "epoch_clock", 0).unwrap(),
        time(3),
        time(3),
        ClockCursorV1::native(2, 2).unwrap(),
        ClockStateV1::Synchronized,
        marketfeed_event_pulse::wire::CanonicalDecimal::parse("0", 18, 8).unwrap(),
        1000,
        ClockQualityV1::Validated,
        "SOURCE_CLOCK_VALID",
    )
    .unwrap();
    assert_eq!(
        state.ingest(&same_epoch_clock),
        Err(CursorError::EpochMismatch)
    );
}

#[test]
fn book_invalidation_requires_resync_before_a_later_snapshot_is_permitted() {
    let f = fixture();
    let mut state = SourceStateMachine::new(f.config);
    state
        .ingest(&market(1, 1, (1, 1), 0, "epoch_a", 0))
        .unwrap();
    let contributor = ContributorV1::new(f.primary.clone(), "epoch_a", 0).unwrap();
    let invalidation = MechanicsInputV1::system(
        SystemSourceV1::new(f.book_system.clone(), "epoch_book", 0).unwrap(),
        FaultScopeV1::contributor(contributor.clone()),
        time(1),
        time(1),
        CursorV1::native(1, 1).unwrap(),
        SystemFaultV1::book_invalidated(),
        None,
    )
    .unwrap();
    state.ingest(&invalidation).unwrap();
    assert_eq!(state.book_eligible(&f.primary), Some(false));
    assert_eq!(state.book_snapshot_permitted(&f.primary), Some(false));
    let premature = market_with_payload(
        1,
        1,
        (2, 2),
        2,
        "epoch_a",
        0,
        MarketEvent::BookSnapshot(marketfeed_model::BookSnapshot {
            bids: vec![],
            asks: vec![],
            depth: Some(0),
            checksum: None,
        }),
    );
    assert_eq!(state.ingest(&premature), Err(CursorError::EpochMismatch));
    let head = state.system_chain_head(&f.book_system).unwrap().to_owned();
    let resync = MechanicsInputV1::system(
        SystemSourceV1::new(f.book_system.clone(), "epoch_book", 0).unwrap(),
        FaultScopeV1::contributor(contributor),
        time(2),
        time(2),
        CursorV1::native(2, 2).unwrap(),
        SystemFaultV1::book_resynchronized(),
        Some(head),
    )
    .unwrap();
    state.ingest(&resync).unwrap();
    assert_eq!(state.book_eligible(&f.primary), Some(false));
    assert_eq!(state.book_snapshot_permitted(&f.primary), Some(true));
    assert_eq!(
        state.contributor_state(&f.primary),
        Some(SlotState::Invalid)
    );
    let snapshot = market_with_payload(
        1,
        1,
        (2, 2),
        3,
        "epoch_a",
        0,
        MarketEvent::BookSnapshot(marketfeed_model::BookSnapshot {
            bids: vec![],
            asks: vec![],
            depth: Some(0),
            checksum: None,
        }),
    );
    assert_eq!(state.ingest(&snapshot).unwrap(), IngestOutcome::Invalidated);
    assert_eq!(state.book_eligible(&f.primary), Some(true));
    assert_eq!(state.book_snapshot_permitted(&f.primary), Some(false));
    assert_eq!(
        state.contributor_state(&f.primary),
        Some(SlotState::Invalid)
    );
}

#[test]
fn disconnect_latches_connection_and_invalidates_cold_sibling_until_recovery() {
    let f = fixture();
    let mut state = SourceStateMachine::new(f.config);
    state
        .ingest(&market(1, 1, (1, 1), 0, "epoch_a", 0))
        .unwrap();
    assert_eq!(state.contributor_state(&f.sibling), Some(SlotState::Cold));
    let disconnect = MechanicsInputV1::system(
        SystemSourceV1::new(f.system.clone(), "epoch_system", 0).unwrap(),
        FaultScopeV1::connection(f.connection.clone(), "epoch_a", 0).unwrap(),
        time(1),
        time(1),
        CursorV1::native(1, 1).unwrap(),
        SystemFaultV1::disconnected(),
        None,
    )
    .unwrap();
    state.ingest(&disconnect).unwrap();
    assert_eq!(
        state.connection_state(&f.connection),
        Some(SlotState::Invalid)
    );
    assert_eq!(
        state.contributor_state(&f.primary),
        Some(SlotState::Invalid)
    );
    assert_eq!(
        state.contributor_state(&f.sibling),
        Some(SlotState::Invalid)
    );
    assert_eq!(
        state.ingest(&market(2, 2, (1, 1), 2, "epoch_a", 0)),
        Err(CursorError::EpochMismatch)
    );
    assert_eq!(
        state
            .ingest(&market(1, 1, (1, 1), 3, "epoch_b", 1))
            .unwrap(),
        IngestOutcome::AcceptedWarming
    );
}

#[test]
fn reused_greater_connection_epoch_latches_invalid_and_prior_epoch_stays_rejected() {
    let f = fixture();
    let mut state = SourceStateMachine::new(f.config);
    state
        .ingest(&market(1, 1, (1, 1), 0, "epoch_a", 0))
        .unwrap();
    state
        .ingest(&market(1, 1, (1, 1), 1, "epoch_b", 1))
        .unwrap();
    assert_eq!(
        state.ingest(&market(1, 1, (1, 1), 2, "epoch_a", 2)),
        Err(CursorError::EpochReused)
    );
    assert_eq!(
        state.connection_state(&f.connection),
        Some(SlotState::Invalid)
    );
    assert_eq!(
        state.ingest(&market(2, 2, (1, 1), 3, "epoch_b", 1)),
        Err(CursorError::EpochMismatch)
    );
}

#[test]
fn epoch_reset_preflights_market_time_and_elapsed_overflow_invalidates() {
    let f = fixture();
    let mut backward = SourceStateMachine::new(f.config.clone());
    backward
        .ingest(&market(1, 1, (1, 1), 10, "epoch_a", 0))
        .unwrap();
    assert_eq!(
        backward.ingest(&market(1, 1, (1, 1), 9, "epoch_b", 1)),
        Err(CursorError::AvailabilityRegression)
    );
    assert_eq!(
        backward.contributor_state(&f.primary),
        Some(SlotState::Invalid)
    );

    let mut overflow = SourceStateMachine::new(f.config);
    overflow
        .ingest(&market(1, 1, (1, 1), i64::MIN, "epoch_a", 0))
        .unwrap();
    assert_eq!(
        overflow.ingest(&market(1, 1, (2, 2), i64::MAX, "epoch_a", 0)),
        Err(CursorError::TimeOverflow)
    );
    assert_eq!(
        overflow.contributor_state(&f.primary),
        Some(SlotState::Invalid)
    );
}

#[test]
fn clock_epoch_reset_preflights_backward_time_and_conversion_overflow() {
    let f = fixture();
    let contributor = ContributorV1::new(f.primary.clone(), "epoch_a", 0).unwrap();
    let mut state = SourceStateMachine::new(f.config.clone());
    state
        .ingest(&market(1, 1, (1, 1), 0, "epoch_a", 0))
        .unwrap();
    let first = MechanicsInputV1::clock(
        contributor.clone(),
        ClockSourceV1::new(f.clock.clone(), "epoch_clock_a", 0).unwrap(),
        time(2),
        time(2),
        ClockCursorV1::native(1, 1).unwrap(),
        ClockStateV1::Synchronized,
        marketfeed_event_pulse::wire::CanonicalDecimal::parse("0", 18, 8).unwrap(),
        1000,
        ClockQualityV1::Validated,
        "SOURCE_CLOCK_VALID",
    )
    .unwrap();
    state.ingest(&first).unwrap();
    let backward = MechanicsInputV1::clock(
        contributor.clone(),
        ClockSourceV1::new(f.clock.clone(), "epoch_clock_b", 1).unwrap(),
        time(1),
        time(1),
        ClockCursorV1::native(1, 1).unwrap(),
        ClockStateV1::Synchronized,
        marketfeed_event_pulse::wire::CanonicalDecimal::parse("0", 18, 8).unwrap(),
        1000,
        ClockQualityV1::Validated,
        "SOURCE_CLOCK_VALID",
    )
    .unwrap();
    assert_eq!(
        state.ingest(&backward),
        Err(CursorError::AvailabilityRegression)
    );
    assert_eq!(state.clock_state(&f.clock), Some(SlotState::Invalid));

    let mut overflow = SourceStateMachine::new(f.config);
    overflow
        .ingest(&market(1, 1, (1, 1), 0, "epoch_a", 0))
        .unwrap();
    let far = Rfc3339Time::parse("9999-12-31T23:59:59Z").unwrap();
    let input = MechanicsInputV1::clock(
        contributor,
        ClockSourceV1::new(f.clock.clone(), "epoch_clock_a", 0).unwrap(),
        far.clone(),
        far,
        ClockCursorV1::native(1, 1).unwrap(),
        ClockStateV1::Synchronized,
        marketfeed_event_pulse::wire::CanonicalDecimal::parse("0", 18, 8).unwrap(),
        1000,
        ClockQualityV1::Validated,
        "SOURCE_CLOCK_VALID",
    )
    .unwrap();
    assert_eq!(overflow.ingest(&input), Err(CursorError::TimeOverflow));
    assert_eq!(overflow.clock_state(&f.clock), Some(SlotState::Invalid));
}
