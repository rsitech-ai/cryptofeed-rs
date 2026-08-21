use marketfeed_event_pulse::{
    ContractBundle,
    features::{
        FeatureConditions, FeatureName, FeatureSet, FlagConditions, KnownDirection, ReversalPolicy,
        SCALE, evaluate_feature, evaluate_reversal,
    },
    mechanics::{FamilyFlags, MechanicsEvidence, Phase, PhaseMachine},
    snapshot::{
        ClockEvidence, MarketAnchor, MechanicsProcessor, SnapshotCursor, SnapshotError,
        SnapshotObservation,
    },
    wire::{InstrumentIdentityV1, Rfc3339Time, SnapshotAuthoringV1},
};

fn features(
    log_return: i128,
    taker: i128,
    cvd: i128,
    spread: i128,
    oi: Option<i128>,
    liquidation: Option<i128>,
    breadth: Option<i128>,
    reversal: i128,
    event: bool,
) -> FeatureSet {
    let policy = if event {
        ReversalPolicy::ReversalRequired {
            direction: KnownDirection::Up,
        }
    } else {
        ReversalPolicy::PreEventZero
    };
    let value = |name, value| {
        evaluate_feature(name, Some(value), &FeatureConditions::valid(name), policy).unwrap()
    };
    let optional = |name, optional_value: Option<i128>| match optional_value {
        Some(optional_value) => value(name, optional_value),
        None => evaluate_feature(
            name,
            None,
            &FeatureConditions::new(
                name,
                [marketfeed_event_pulse::features::FeatureCondition::OptionalSourceUnavailable],
            )
            .unwrap(),
            policy,
        )
        .unwrap(),
    };
    FeatureSet::new(
        vec![
            value(FeatureName::BookDepth10bps, SCALE),
            optional(FeatureName::CrossVenueBreadth, breadth),
            value(FeatureName::CvdSlope, cvd),
            optional(FeatureName::LiquidationNotional, liquidation),
            value(FeatureName::LogReturn, log_return),
            optional(FeatureName::OpenInterestChange, oi),
            evaluate_reversal(
                policy,
                100 * SCALE,
                120 * SCALE,
                120 * SCALE - reversal * 20,
                &FeatureConditions::valid(FeatureName::ReversalFromExtreme),
            )
            .unwrap(),
            value(FeatureName::SpreadBps, spread),
            value(FeatureName::TakerImbalance, taker),
        ],
        policy,
    )
    .unwrap()
}

fn evidence(intensity: i128, reversal: i128) -> MechanicsEvidence {
    MechanicsEvidence {
        available_at_ns: 0,
        direction: marketfeed_event_pulse::features::Direction::Up,
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
fn phase_machine_enforces_direct_extremes_dwell_hysteresis_and_invalid_recovery() {
    let mut machine = PhaseMachine::new();
    let ignition = evidence(85_000_000, 0);
    machine.observe(&ignition).unwrap();
    machine.advance_to(99_999_999).unwrap();
    assert_eq!(machine.phase(), Phase::Normal);
    machine.advance_to(100_000_000).unwrap();
    assert_eq!(machine.phase(), Phase::Ignition);

    let mut cascade = evidence(85_000_000, 0);
    cascade.available_at_ns = 100_000_000;
    machine.observe(&cascade).unwrap();
    machine.advance_to(350_000_000).unwrap();
    assert_eq!(machine.phase(), Phase::Cascade);

    let mut exhaustion = evidence(85_000_000, 65_000_000);
    exhaustion.available_at_ns = 350_000_000;
    machine.observe(&exhaustion).unwrap();
    machine.advance_to(600_000_000).unwrap();
    assert_eq!(machine.phase(), Phase::Exhaustion);

    let mut invalid = exhaustion.clone();
    invalid.available_at_ns = 600_000_001;
    invalid.valid = false;
    machine.observe(&invalid).unwrap();
    assert_eq!(machine.phase(), Phase::Invalid);

    let mut recovery = evidence(0, 0);
    recovery.families = FamilyFlags::default();
    recovery.intensity = 0;
    recovery.available_at_ns = 600_000_002;
    recovery.fully_warmed = true;
    machine.observe(&recovery).unwrap();
    machine.advance_to(1_600_000_001).unwrap();
    assert_eq!(machine.phase(), Phase::Invalid);
    machine.advance_to(1_600_000_002).unwrap();
    assert_eq!(machine.phase(), Phase::Normal);
}

#[test]
fn phase_machine_covers_aftermath_reentry_and_resets_false_candidate_dwell() {
    let mut machine = PhaseMachine::new();
    let mut buildup = evidence(65_000_000, 0);
    buildup.families.derivatives = false;
    buildup.intensity = buildup.families.intensity();
    machine.observe(&buildup).unwrap();
    machine.advance_to(249_000_000).unwrap();
    assert_eq!(machine.phase(), Phase::Normal);

    let mut quiet = evidence(0, 0);
    quiet.families = FamilyFlags::default();
    quiet.intensity = 0;
    quiet.available_at_ns = 249_000_001;
    machine.observe(&quiet).unwrap();
    buildup.available_at_ns = 250_000_000;
    machine.observe(&buildup).unwrap();
    machine.advance_to(499_999_999).unwrap();
    assert_eq!(machine.phase(), Phase::Normal);
    machine.advance_to(500_000_000).unwrap();
    assert_eq!(machine.phase(), Phase::Buildup);

    let mut ignition = evidence(85_000_000, 0);
    ignition.available_at_ns = 500_000_000;
    machine.observe(&ignition).unwrap();
    machine.advance_to(600_000_000).unwrap();
    assert_eq!(machine.phase(), Phase::Ignition);
    machine.advance_to(850_000_000).unwrap();
    assert_eq!(machine.phase(), Phase::Cascade);

    let mut exhausted = evidence(85_000_000, 65_000_000);
    exhausted.available_at_ns = 850_000_000;
    machine.observe(&exhausted).unwrap();
    machine.advance_to(1_100_000_000).unwrap();
    assert_eq!(machine.phase(), Phase::Exhaustion);

    let mut aftermath = evidence(0, 50_000_000);
    aftermath.families = FamilyFlags::default();
    aftermath.intensity = 0;
    aftermath.available_at_ns = 1_100_000_000;
    machine.observe(&aftermath).unwrap();
    machine.advance_to(2_100_000_000).unwrap();
    assert_eq!(machine.phase(), Phase::Aftermath);

    buildup.available_at_ns = 2_100_000_000;
    machine.observe(&buildup).unwrap();
    machine.advance_to(2_600_000_000).unwrap();
    assert_eq!(machine.phase(), Phase::Buildup);
}

fn at(millis: u32) -> Rfc3339Time {
    Rfc3339Time::parse(&format!(
        "2026-08-21T10:00:{:02}.{:03}Z",
        millis / 1_000,
        millis % 1_000
    ))
    .unwrap()
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

fn observation(millis: u32, include_clock: bool) -> SnapshotObservation {
    let available_at = at(millis);
    SnapshotObservation {
        available_at: available_at.clone(),
        features: features(
            300_000,
            70_000_000,
            3 * SCALE,
            9 * SCALE,
            Some(-110 * SCALE),
            Some(1_100_000 * SCALE),
            Some(70_000_000),
            0,
            false,
        ),
        flag_conditions: FlagConditions::default(),
        liquidation_confirms_direction: true,
        fully_warmed: true,
        anchor: Some(MarketAnchor {
            source_event_time: at(1),
            received_at: at(2),
            normalized_at: at(3),
            available_at: available_at.clone(),
            payload_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        }),
        cursors: vec![SnapshotCursor {
            source_id: "hyperliquid_market".into(),
            connection_epoch: "epoch_a".into(),
            sequence_start: 1,
            sequence_end: u64::from(millis) + 1,
            available_at: available_at.clone(),
            payload_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        }],
        required_clock_sources: vec!["hyperliquid_clock".into()],
        clocks: include_clock
            .then(|| ClockEvidence {
                source_id: "hyperliquid_clock".into(),
                available_at: available_at.clone(),
                observed_skew_ms: 250_000_000,
                freshness_limit_ms: 2_000,
                degraded: false,
            })
            .into_iter()
            .collect(),
    }
}

#[test]
fn failed_same_time_authorship_is_atomic_and_repairable_then_seals() {
    let mut processor = MechanicsProcessor::new(authoring());
    processor.ingest(observation(10, false)).unwrap();
    assert_eq!(
        processor.snapshot(at(110)),
        Err(SnapshotError::MissingClockEvidence)
    );
    assert_eq!(processor.next_revision(), 1);
    processor.ingest(observation(10, true)).unwrap();
    let first = processor.snapshot(at(110)).unwrap();
    let cached = processor.snapshot(at(110)).unwrap();
    assert_eq!(first.canonical_json(), cached.canonical_json());
    assert_eq!(processor.next_revision(), 2);
    assert_eq!(
        processor.ingest(observation(10, true)),
        Err(SnapshotError::SealedInput)
    );
    ContractBundle::load_embedded()
        .unwrap()
        .validate_e1_json(first.canonical_json().as_bytes())
        .unwrap();

    let mut fresh = MechanicsProcessor::new(authoring());
    fresh.ingest(observation(10, false)).unwrap();
    fresh.ingest(observation(10, true)).unwrap();
    let independently_authored = fresh.snapshot(at(110)).unwrap();
    assert_eq!(
        first.canonical_json(),
        independently_authored.canonical_json()
    );
    if let Some(path) = std::env::var_os("EVENT_PULSE_SNAPSHOT_OUTPUT") {
        std::fs::write(path, first.canonical_json()).unwrap();
    }
}

#[test]
fn revisions_bind_predecessors_and_decreasing_snapshot_time_rejects() {
    let mut processor = MechanicsProcessor::new(authoring());
    processor.ingest(observation(10, true)).unwrap();
    let first = processor.snapshot(at(110)).unwrap();
    processor.ingest(observation(120, true)).unwrap();
    let second = processor.snapshot(at(220)).unwrap();
    assert_eq!(second.revision(), 2);
    assert_eq!(
        second.predecessor_content_hash(),
        Some(first.content_hash())
    );
    assert_eq!(
        processor.snapshot(at(219)),
        Err(SnapshotError::DecisionTimeRegression)
    );
}

#[test]
fn successful_snapshot_seals_through_decision_and_retains_anchor_for_later_state() {
    let mut processor = MechanicsProcessor::new(authoring());
    processor.ingest(observation(10, true)).unwrap();
    processor.snapshot(at(110)).unwrap();
    assert_eq!(
        processor.ingest(observation(110, true)),
        Err(SnapshotError::SealedInput)
    );

    let mut later = observation(120, true);
    later.anchor = None;
    processor.ingest(later).unwrap();
    let second = processor.snapshot(at(220)).unwrap();
    assert_eq!(second.revision(), 2);
}

#[test]
fn cold_anchor_stale_clock_and_stale_anchor_fail_without_consuming_revision() {
    let mut missing = MechanicsProcessor::new(authoring());
    let mut no_anchor = observation(10, true);
    no_anchor.anchor = None;
    missing.ingest(no_anchor).unwrap();
    assert_eq!(
        missing.snapshot(at(110)),
        Err(SnapshotError::MissingCausalAnchor)
    );

    let mut stale_clock = MechanicsProcessor::new(authoring());
    stale_clock.ingest(observation(10, true)).unwrap();
    assert_eq!(
        stale_clock.snapshot(Rfc3339Time::parse("2026-08-21T10:00:01.011Z").unwrap()),
        Err(SnapshotError::MissingClockEvidence)
    );
    assert_eq!(stale_clock.next_revision(), 1);

    let mut stale_anchor = MechanicsProcessor::new(authoring());
    stale_anchor.ingest(observation(10, true)).unwrap();
    stale_anchor.snapshot(at(110)).unwrap();
    let mut post_clear = observation(2_000, true);
    post_clear.anchor = None;
    stale_anchor.ingest(post_clear).unwrap();
    assert_eq!(
        stale_anchor.snapshot(at(2_100)),
        Err(SnapshotError::StaleCausalAnchor)
    );
    assert_eq!(stale_anchor.next_revision(), 2);
}

#[test]
fn invalid_snapshot_zeros_scores_and_maps_every_truthful_flag() {
    let mut obs = observation(10, true);
    obs.features = features(0, 0, 0, 9 * SCALE, None, None, None, 0, false);
    obs.flag_conditions.sequence_failure = true;
    let mut processor = MechanicsProcessor::new(authoring());
    processor.ingest(obs).unwrap();
    let snapshot = processor.snapshot(at(110)).unwrap();
    let value = snapshot.value();
    assert_eq!(value["phase"], "INVALID");
    assert_eq!(value["mechanical_intensity"], "0");
    assert_eq!(value["mechanical_confidence"], "0");
    assert_eq!(value["quality_flags"], serde_json::json!(["SEQUENCE_GAP"]));
}
