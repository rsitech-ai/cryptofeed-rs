use std::sync::Arc;

use marketfeed_analytics::{
    AdaptivePreset, AdaptiveThreshold, BubbleConfig, BubbleDetector, BubbleFilter, BubbleMode,
    BubbleShape, BubbleStyle, BubbleTier, DetectionPhase, FlowConfig, FlowSource, GridSpec,
    LabelMode, MarketSegment, MergeConfig, PerformanceMode, SourceSelector, ThresholdMode,
    TimeframeSpec, TradeInput,
};
use marketfeed_model::{AggressorSide, Fixed, InstrumentId, Price, Quantity, VenueId};

fn grid() -> GridSpec {
    GridSpec::new(0, 0, Fixed::new(1, 0), 1).unwrap()
}

fn time() -> TimeframeSpec {
    TimeframeSpec::new(60, 60, 300, 0, 900).unwrap()
}

fn qty(value: i128) -> Quantity {
    Quantity(Fixed::new(value, 0))
}

fn style() -> BubbleStyle {
    BubbleStyle::new(
        BubbleShape::Circle,
        10,
        50,
        qty(20),
        qty(20),
        LabelMode::Raw,
    )
    .unwrap()
}

fn off(tier: BubbleTier) -> BubbleFilter {
    BubbleFilter::new(
        tier,
        BubbleMode::Volume,
        ThresholdMode::Off,
        SourceSelector::all(),
        16,
        style(),
    )
    .unwrap()
}

fn manual(tier: BubbleTier, mode: BubbleMode, minimum: i128) -> BubbleFilter {
    BubbleFilter::new(
        tier,
        mode,
        ThresholdMode::Manual(qty(minimum)),
        SourceSelector::all(),
        16,
        style(),
    )
    .unwrap()
}

fn candle(
    trades: &[(u16, MarketSegment, i128, i128, AggressorSide)],
) -> marketfeed_analytics::CandleFlow {
    candle_at(0, trades)
}

fn candle_at(
    candle_start: i64,
    trades: &[(u16, MarketSegment, i128, i128, AggressorSide)],
) -> marketfeed_analytics::CandleFlow {
    let mut builder = marketfeed_analytics::CandleFlowBuilder::new(
        InstrumentId(7),
        grid(),
        time(),
        FlowConfig::new(16, 64, 512).unwrap(),
    )
    .unwrap();
    for (index, (venue, segment, price, quantity, side)) in trades.iter().enumerate() {
        builder
            .ingest(TradeInput {
                instrument: InstrumentId(7),
                source: FlowSource {
                    venue: VenueId(*venue),
                    segment: *segment,
                },
                timestamp_ns: candle_start + i64::try_from(index + 1).unwrap(),
                price: Price(Fixed::new(*price, 0)),
                quantity: qty(*quantity),
                aggressor: *side,
            })
            .unwrap();
    }
    builder.finish().unwrap().unwrap()
}

#[test]
fn three_tiers_apply_strict_f3_f2_f1_priority() {
    let config = BubbleConfig::new(
        manual(BubbleTier::F1, BubbleMode::Volume, 3),
        manual(BubbleTier::F2, BubbleMode::Volume, 5),
        manual(BubbleTier::F3, BubbleMode::Delta, 6),
        MergeConfig::disabled(),
        PerformanceMode::Full,
        16,
    )
    .unwrap();
    let detector = BubbleDetector::new(grid(), config).unwrap();
    let flow = candle(&[
        (1, MarketSegment::Spot, 100, 10, AggressorSide::Buy),
        (1, MarketSegment::Spot, 101, 6, AggressorSide::Buy),
        (1, MarketSegment::Spot, 101, 2, AggressorSide::Sell),
        (1, MarketSegment::Spot, 102, 4, AggressorSide::Buy),
    ]);

    let bubbles = detector.detect(&flow, DetectionPhase::Final).unwrap();
    assert_eq!(bubbles.len(), 3);
    assert_eq!(
        bubbles
            .iter()
            .find(|bubble| bubble.anchor_price == Price(Fixed::new(100, 0)))
            .unwrap()
            .tier,
        BubbleTier::F3
    );
    assert_eq!(
        bubbles
            .iter()
            .find(|bubble| bubble.anchor_price == Price(Fixed::new(101, 0)))
            .unwrap()
            .tier,
        BubbleTier::F2
    );
    assert_eq!(
        bubbles
            .iter()
            .find(|bubble| bubble.anchor_price == Price(Fixed::new(102, 0)))
            .unwrap()
            .tier,
        BubbleTier::F1
    );
}

#[test]
fn higher_tier_qualifiers_do_not_fall_through_when_its_output_limit_is_reached() {
    let mut f3 = manual(BubbleTier::F3, BubbleMode::Volume, 5);
    f3.max_bubbles_per_candle = 1;
    let config = BubbleConfig::new(
        manual(BubbleTier::F1, BubbleMode::Volume, 1),
        manual(BubbleTier::F2, BubbleMode::Volume, 1),
        f3,
        MergeConfig::disabled(),
        PerformanceMode::Full,
        16,
    )
    .unwrap();
    let detector = BubbleDetector::new(grid(), config).unwrap();
    let flow = candle(&[
        (1, MarketSegment::Spot, 100, 10, AggressorSide::Buy),
        (1, MarketSegment::Spot, 101, 8, AggressorSide::Buy),
    ]);

    let bubbles = detector.detect(&flow, DetectionPhase::Final).unwrap();
    assert_eq!(bubbles.len(), 1);
    assert_eq!(bubbles[0].tier, BubbleTier::F3);
    assert_eq!(bubbles[0].anchor_price, Price(Fixed::new(100, 0)));
}

#[test]
fn venues_aggregate_within_segment_but_spot_and_perpetual_stay_separate() {
    let config = BubbleConfig::new(
        manual(BubbleTier::F1, BubbleMode::Volume, 1),
        off(BubbleTier::F2),
        off(BubbleTier::F3),
        MergeConfig::disabled(),
        PerformanceMode::Full,
        16,
    )
    .unwrap();
    let detector = BubbleDetector::new(grid(), config).unwrap();
    let flow = candle(&[
        (1, MarketSegment::Spot, 100, 3, AggressorSide::Buy),
        (2, MarketSegment::Spot, 100, 4, AggressorSide::Sell),
        (
            3,
            MarketSegment::LinearPerpetual,
            100,
            5,
            AggressorSide::Buy,
        ),
    ]);

    let bubbles = detector.detect(&flow, DetectionPhase::Final).unwrap();
    assert_eq!(bubbles.len(), 2);
    let spot = bubbles
        .iter()
        .find(|bubble| bubble.segment == MarketSegment::Spot)
        .unwrap();
    assert_eq!(spot.total_volume, qty(7));
    assert_eq!(spot.sources, vec![VenueId(1), VenueId(2)]);

    let perpetual = bubbles
        .iter()
        .find(|bubble| bubble.segment == MarketSegment::LinearPerpetual)
        .unwrap();
    assert_eq!(perpetual.total_volume, qty(5));
    assert_eq!(perpetual.sources, vec![VenueId(3)]);
}

#[test]
fn adaptive_threshold_uses_only_bounded_finalized_history() {
    let adaptive =
        AdaptiveThreshold::new(AdaptivePreset::Balanced, 7_500, None, 4, 2, 10_000, 0).unwrap();
    let filter = BubbleFilter::new(
        BubbleTier::F1,
        BubbleMode::Volume,
        ThresholdMode::Adaptive(adaptive),
        SourceSelector::all(),
        16,
        style(),
    )
    .unwrap();
    let config = BubbleConfig::new(
        filter,
        off(BubbleTier::F2),
        off(BubbleTier::F3),
        MergeConfig::disabled(),
        PerformanceMode::Full,
        2,
    )
    .unwrap();
    let mut detector = BubbleDetector::new(grid(), config).unwrap();

    let history_one = candle_at(
        0,
        &[
            (1, MarketSegment::Spot, 100, 1, AggressorSide::Buy),
            (1, MarketSegment::Spot, 101, 2, AggressorSide::Buy),
        ],
    );
    let history_two = candle_at(
        60,
        &[
            (1, MarketSegment::Spot, 100, 3, AggressorSide::Buy),
            (1, MarketSegment::Spot, 101, 100, AggressorSide::Buy),
        ],
    );
    detector.record_finalized(&history_one).unwrap();
    detector.record_finalized(&history_two).unwrap();

    let current = candle_at(
        120,
        &[
            (1, MarketSegment::Spot, 100, 2, AggressorSide::Buy),
            (1, MarketSegment::Spot, 101, 3, AggressorSide::Buy),
        ],
    );
    let bubbles = detector.detect(&current, DetectionPhase::Final).unwrap();
    assert_eq!(bubbles.len(), 1);
    assert_eq!(bubbles[0].anchor_price, Price(Fixed::new(101, 0)));
    assert_eq!(detector.history_len(), 2);
}

#[test]
fn finalized_history_rejects_duplicate_or_regressing_candles_atomically() {
    let adaptive =
        AdaptiveThreshold::new(AdaptivePreset::Balanced, 7_500, None, 1, 2, 10_000, 0).unwrap();
    let filter = BubbleFilter::new(
        BubbleTier::F1,
        BubbleMode::Volume,
        ThresholdMode::Adaptive(adaptive),
        SourceSelector::all(),
        16,
        style(),
    )
    .unwrap();
    let config = BubbleConfig::new(
        filter,
        off(BubbleTier::F2),
        off(BubbleTier::F3),
        MergeConfig::disabled(),
        PerformanceMode::Full,
        2,
    )
    .unwrap();
    let mut detector = BubbleDetector::new(grid(), config).unwrap();
    let first = candle_at(60, &[(1, MarketSegment::Spot, 100, 2, AggressorSide::Buy)]);
    detector.record_finalized(&first).unwrap();
    let before = serde_json::to_vec(&detector).unwrap();

    assert!(detector.record_finalized(&first).is_err());
    assert_eq!(serde_json::to_vec(&detector).unwrap(), before);

    let older = candle_at(0, &[(1, MarketSegment::Spot, 100, 2, AggressorSide::Buy)]);
    assert!(detector.record_finalized(&older).is_err());
    assert_eq!(serde_json::to_vec(&detector).unwrap(), before);
}

#[test]
fn finalized_history_can_share_one_immutable_candle_across_detectors() {
    let config = BubbleConfig::new(
        manual(BubbleTier::F1, BubbleMode::Volume, 1),
        off(BubbleTier::F2),
        off(BubbleTier::F3),
        MergeConfig::disabled(),
        PerformanceMode::Full,
        2,
    )
    .unwrap();
    let mut volume = BubbleDetector::new(grid(), config.clone()).unwrap();
    let mut delta = BubbleDetector::new(grid(), config).unwrap();
    let shared = Arc::new(candle_at(
        60,
        &[(1, MarketSegment::Spot, 100, 2, AggressorSide::Buy)],
    ));

    volume.record_finalized_shared(Arc::clone(&shared)).unwrap();
    delta.record_finalized_shared(Arc::clone(&shared)).unwrap();

    assert_eq!(Arc::strong_count(&shared), 3);
    assert_eq!(volume.history_len(), 1);
    assert_eq!(delta.history_len(), 1);
}

#[test]
fn bubble_limits_sizing_and_labels_are_deterministic() {
    let mut filter = manual(BubbleTier::F1, BubbleMode::Volume, 1);
    filter.max_bubbles_per_candle = 2;
    let config = BubbleConfig::new(
        filter,
        off(BubbleTier::F2),
        off(BubbleTier::F3),
        MergeConfig::disabled(),
        PerformanceMode::Full,
        16,
    )
    .unwrap();
    let detector = BubbleDetector::new(grid(), config).unwrap();
    let flow = candle(&[
        (1, MarketSegment::Spot, 100, 5, AggressorSide::Buy),
        (1, MarketSegment::Spot, 101, 20, AggressorSide::Buy),
        (1, MarketSegment::Spot, 102, 10, AggressorSide::Buy),
    ]);

    let bubbles = detector.detect(&flow, DetectionPhase::Final).unwrap();
    assert_eq!(bubbles.len(), 2);
    assert_eq!(bubbles[0].anchor_price, Price(Fixed::new(101, 0)));
    assert_eq!(bubbles[0].visual_size, 50);
    assert_eq!(bubbles[0].label, Some(Fixed::new(20, 0)));
    assert_eq!(bubbles[1].anchor_price, Price(Fixed::new(102, 0)));
    assert_eq!(bubbles[1].visual_size, 30);
}

#[test]
fn eligible_adjacent_bubbles_merge_without_crossing_segments() {
    let merge = MergeConfig::new(
        [true, false, false],
        1,
        BubbleStyle::new(
            BubbleShape::Diamond,
            12,
            60,
            qty(30),
            qty(30),
            LabelMode::Raw,
        )
        .unwrap(),
    )
    .unwrap();
    let config = BubbleConfig::new(
        manual(BubbleTier::F1, BubbleMode::Volume, 1),
        off(BubbleTier::F2),
        off(BubbleTier::F3),
        merge,
        PerformanceMode::Full,
        16,
    )
    .unwrap();
    let detector = BubbleDetector::new(grid(), config).unwrap();
    let flow = candle(&[
        (1, MarketSegment::Spot, 100, 5, AggressorSide::Buy),
        (1, MarketSegment::Spot, 101, 7, AggressorSide::Buy),
        (1, MarketSegment::Spot, 103, 10, AggressorSide::Buy),
        (
            1,
            MarketSegment::LinearPerpetual,
            101,
            9,
            AggressorSide::Buy,
        ),
    ]);

    let bubbles = detector.detect(&flow, DetectionPhase::Final).unwrap();
    assert_eq!(bubbles.len(), 3);
    let merged = bubbles
        .iter()
        .find(|bubble| bubble.merged_count == 2)
        .unwrap();
    assert_eq!(merged.low_price, Price(Fixed::new(100, 0)));
    assert_eq!(merged.high_price, Price(Fixed::new(101, 0)));
    assert_eq!(merged.anchor_price, Price(Fixed::new(101, 0)));
    assert_eq!(merged.total_volume, qty(12));
    assert_eq!(merged.shape, BubbleShape::Diamond);
}

#[test]
fn high_performance_mode_reduces_live_work_but_not_final_output() {
    let config = BubbleConfig::new(
        manual(BubbleTier::F1, BubbleMode::Volume, 1),
        manual(BubbleTier::F2, BubbleMode::Volume, 5),
        manual(BubbleTier::F3, BubbleMode::Volume, 10),
        MergeConfig::disabled(),
        PerformanceMode::High {
            live_tier_count: 1,
            defer_merging: true,
        },
        16,
    )
    .unwrap();
    let detector = BubbleDetector::new(grid(), config).unwrap();
    let flow = candle(&[
        (1, MarketSegment::Spot, 100, 10, AggressorSide::Buy),
        (1, MarketSegment::Spot, 101, 6, AggressorSide::Buy),
        (1, MarketSegment::Spot, 102, 2, AggressorSide::Buy),
    ]);

    let live = detector.detect(&flow, DetectionPhase::Live).unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].tier, BubbleTier::F3);

    let finalized = detector.detect(&flow, DetectionPhase::Final).unwrap();
    assert_eq!(finalized.len(), 3);
}
