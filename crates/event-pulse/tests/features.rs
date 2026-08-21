use marketfeed_event_pulse::features::*;
use marketfeed_model::{BookDelta, BookLevel, BookSnapshot, SequenceRange};

#[test]
fn exact_arithmetic_rounds_half_away_from_zero_and_separates_division_kinds() {
    assert_eq!(div_integer(5, 2).unwrap(), 3);
    assert_eq!(div_integer(-5, 2).unwrap(), -3);
    assert_eq!(mul_scaled(50_000_000, 50_000_001).unwrap(), 25_000_001);
    assert_eq!(div_scaled(1, 2).unwrap(), 50_000_000);
    assert!(mul_scaled(i128::MAX, 2).is_err());
}

#[test]
fn log_return_matches_independent_decimal_goldens_within_one_e_minus_seven() {
    // Values frozen from Python Decimal(80), not from binary floating point.
    let cases = [
        (100 * SCALE, 101 * SCALE, 995_033i128),
        (100 * SCALE, 90 * SCALE, -10_536_052i128),
        (100 * SCALE, 125 * SCALE, 22_314_355i128),
        (100 * SCALE, 80 * SCALE, -22_314_355i128),
    ];
    for (p0, p1, expected) in cases {
        assert!((log_return(p0, p1).unwrap() - expected).abs() <= 10);
    }
    assert_eq!(
        log_return(100 * SCALE, 125 * SCALE + 1),
        Err(ArithmeticError::OutOfDomain)
    );
    assert_eq!(log_return(0, SCALE), Err(ArithmeticError::OutOfDomain));
}

#[test]
fn all_non_book_formulas_cover_boundaries() {
    assert_eq!(taker_imbalance(3 * SCALE, SCALE).unwrap(), 50_000_000);
    assert!(taker_imbalance(0, 0).is_err());
    assert_eq!(cvd_slope(0, SCALE, 500_000).unwrap(), 2 * SCALE);
    assert!(cvd_slope(0, SCALE, 0).is_err());
    assert_eq!(spread_bps(99 * SCALE, 101 * SCALE).unwrap(), 200 * SCALE);
    assert_eq!(
        open_interest_change(125 * SCALE, 100 * SCALE).unwrap(),
        -25 * SCALE
    );
    assert_eq!(
        open_interest_contracts(marketfeed_model::Fixed::new(2, 0), Some(5 * SCALE)).unwrap(),
        10 * SCALE
    );
    assert!(open_interest_contracts(marketfeed_model::Fixed::new(2, 0), Some(0)).is_err());
    assert_eq!(
        liquidation_notional(&[(100 * SCALE, 2 * SCALE)]).unwrap(),
        200 * SCALE
    );
    assert_eq!(
        liquidation_notional(&[(SCALE, i128::MIN)]),
        Err(ArithmeticError::Overflow)
    );
    assert_eq!(cross_venue_breadth(2, 3).unwrap(), 66_666_667);
    assert_eq!(
        reversal_from_extreme(Direction::Up, 100 * SCALE, 120 * SCALE, 110 * SCALE).unwrap(),
        50_000_000
    );
    assert_eq!(
        reversal_from_extreme(Direction::Down, 100 * SCALE, 80 * SCALE, 90 * SCALE).unwrap(),
        50_000_000
    );
}

#[test]
fn book_projection_is_atomic_fresh_and_resync_gated() {
    let mut book = BookProjection::new(8, 8, None);
    let snapshot = BookSnapshot {
        bids: vec![BookLevel {
            price: price(10_045_000_000),
            quantity: quantity(2 * SCALE),
        }],
        asks: vec![BookLevel {
            price: price(10_055_000_000),
            quantity: quantity(3 * SCALE),
        }],
        depth: None,
        checksum: None,
    };
    book.snapshot_native(&snapshot, SequenceRange { first: 1, last: 5 }, 0)
        .unwrap();
    book.delta_native(
        &BookDelta {
            changes: vec![],
            checksum: None,
        },
        SequenceRange { first: 6, last: 9 },
        1,
    )
    .unwrap();
    book.delta_native(
        &BookDelta {
            changes: vec![],
            checksum: None,
        },
        SequenceRange {
            first: 10,
            last: 10,
        },
        2,
    )
    .unwrap();
    assert_eq!(book.depth_10bps(250_000_000).unwrap(), 50_255_000_000);
    let crossed = BookSnapshot {
        bids: vec![BookLevel {
            price: price(102 * SCALE),
            quantity: quantity(SCALE),
        }],
        ..snapshot.clone()
    };
    assert!(
        book.snapshot_native(
            &crossed,
            SequenceRange {
                first: 11,
                last: 11
            },
            1
        )
        .is_err()
    );
    assert!(book.depth_10bps(250_000_000).is_err());
    book.permit_resnapshot();
    book.snapshot_native(&snapshot, SequenceRange { first: 1, last: 1 }, 0)
        .unwrap();
    assert!(book.depth_10bps(250_000_001).is_err());
    book.invalidate();
    assert!(book.depth_10bps(0).is_err());
    book.permit_resnapshot();
    assert!(
        book.delta_native(
            &BookDelta {
                changes: vec![],
                checksum: None
            },
            SequenceRange { first: 2, last: 2 },
            1
        )
        .is_err()
    );

    book.permit_resnapshot();
    book.snapshot_derived(&snapshot, 0).unwrap();
    book.delta_derived(
        &BookDelta {
            changes: vec![],
            checksum: None,
        },
        1,
    )
    .unwrap();
    assert!(
        book.delta_native(
            &BookDelta {
                changes: vec![],
                checksum: None
            },
            SequenceRange { first: 1, last: 2 },
            2
        )
        .is_err()
    );

    book.permit_resnapshot();
    book.snapshot_native(&snapshot, SequenceRange { first: 1, last: 5 }, 0)
        .unwrap();
    assert!(
        book.delta_native(
            &BookDelta {
                changes: vec![],
                checksum: None
            },
            SequenceRange { first: 7, last: 8 },
            1
        )
        .is_err()
    );
}

#[test]
fn feature_order_classification_and_reason_precedence_are_frozen() {
    let pre_event = ReversalPolicy::pre_event_normal(Direction::Unknown);
    let event = ReversalPolicy::new(CurrentPhase::Buildup, Direction::Up, true);
    assert_eq!(FeatureName::CANONICAL.len(), 9);
    assert!(FeatureName::LogReturn.is_critical(pre_event));
    assert!(!FeatureName::ReversalFromExtreme.is_critical(pre_event));
    assert!(FeatureName::ReversalFromExtreme.is_critical(event));
    assert!(FeatureName::OpenInterestChange.is_optional());
    let conditions = FeatureConditions {
        clock_degraded: true,
        insufficient_samples: true,
        ..Default::default()
    };
    let row = evaluate_feature(
        FeatureName::OpenInterestChange,
        None,
        &conditions,
        pre_event,
    )
    .unwrap();
    assert_eq!(
        (row.reason(), row.quality()),
        (
            FeatureReason::InsufficientSamples,
            FeatureQuality::Unavailable
        )
    );
    let degraded = evaluate_feature(
        FeatureName::LogReturn,
        Some(1),
        &FeatureConditions {
            clock_degraded: true,
            ..Default::default()
        },
        pre_event,
    )
    .unwrap();
    assert_eq!(
        (degraded.reason(), degraded.quality()),
        (FeatureReason::ClockDegraded, FeatureQuality::Degraded)
    );
    assert_eq!(degraded.horizon_ms(), 1_000);
    assert_eq!(
        evaluate_feature(
            FeatureName::LogReturn,
            None,
            &FeatureConditions {
                out_of_domain: true,
                ..Default::default()
            },
            pre_event
        ),
        Err(FeatureAuthoringError::CriticalFeatureAuthoringError)
    );
    let zero = evaluate_reversal(
        pre_event,
        Err(ArithmeticError::OutOfDomain),
        &FeatureConditions::default(),
    )
    .unwrap();
    assert_eq!(
        (zero.value(), zero.reason()),
        (Some(0), FeatureReason::ObservationValid)
    );
    assert_eq!(
        evaluate_reversal(
            ReversalPolicy::new(CurrentPhase::Normal, Direction::Unknown, true),
            Err(ArithmeticError::OutOfDomain),
            &FeatureConditions::default()
        )
        .unwrap()
        .quality(),
        FeatureQuality::Validated
    );
    assert_eq!(
        evaluate_reversal(
            ReversalPolicy::new(CurrentPhase::Buildup, Direction::Unknown, true),
            Err(ArithmeticError::OutOfDomain),
            &FeatureConditions::default()
        )
        .unwrap()
        .quality(),
        FeatureQuality::Unavailable
    );
    assert_eq!(
        evaluate_reversal(
            ReversalPolicy::new(CurrentPhase::Normal, Direction::Up, true),
            Err(ArithmeticError::OutOfDomain),
            &FeatureConditions::default()
        ),
        Err(FeatureAuthoringError::CriticalFeatureAuthoringError)
    );
    assert_eq!(
        mechanics_flags(&FlagConditions {
            sequence_failure: true,
            book_resyncing: true,
            queue_drop: true,
            ..Default::default()
        }),
        [
            MechanicsFlag::BookResyncing,
            MechanicsFlag::QueueDrop,
            MechanicsFlag::SequenceGap,
        ]
    );
    assert_eq!(
        evaluate_feature(
            FeatureName::LogReturn,
            None,
            &FeatureConditions::default(),
            pre_event
        ),
        Err(FeatureAuthoringError::InvalidFeatureObservation)
    );

    let mut rows = FeatureName::CANONICAL
        .iter()
        .map(|(name, _)| {
            let conditions = if *name == FeatureName::OpenInterestChange {
                FeatureConditions {
                    optional_source_unavailable: true,
                    ..Default::default()
                }
            } else {
                FeatureConditions::default()
            };
            if *name == FeatureName::ReversalFromExtreme {
                evaluate_reversal(pre_event, Err(ArithmeticError::OutOfDomain), &conditions)
                    .unwrap()
            } else {
                evaluate_feature(
                    *name,
                    (*name != FeatureName::OpenInterestChange).then_some(0),
                    &conditions,
                    pre_event,
                )
                .unwrap()
            }
        })
        .collect::<Vec<_>>();
    let set = FeatureSet::new(rows.clone()).unwrap();
    assert_eq!(envelope_quality(&set, pre_event), EnvelopeQuality::Degraded);
    let mut duplicate = rows.clone();
    duplicate[8] = duplicate[0].clone();
    assert_eq!(
        FeatureSet::new(duplicate),
        Err(FeatureAuthoringError::InvalidFeatureSet)
    );
    rows.pop();
    assert_eq!(
        FeatureSet::new(rows),
        Err(FeatureAuthoringError::InvalidFeatureSet)
    );
    assert_eq!(
        FeatureSet::new(Vec::new()),
        Err(FeatureAuthoringError::InvalidFeatureSet)
    );
}

#[test]
fn canonical_decimal_never_emits_exponents_or_negative_zero() {
    assert_eq!(canonical_decimal(0), "0");
    assert_eq!(canonical_decimal(100_000_000), "1");
    assert_eq!(canonical_decimal(-100_000_001), "-1.00000001");
    assert_eq!(canonical_decimal(10), "0.0000001");
}
