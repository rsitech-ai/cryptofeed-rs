use marketfeed_analytics::{
    BubbleDirection, BubbleMode, BubbleShape, BubbleTier, CandleFlow, FlowState, GridSpec,
    MarketSegment, OrderFlowBubble, StructuralLevelConfig, StructuralLevelEngine,
    StructuralLevelKind, StructuralLevelState,
};
use marketfeed_model::{Fixed, InstrumentId, Price, Quantity, VenueId};

fn price(value: i128) -> Price {
    Price(Fixed::new(value, 0))
}

fn qty(value: i128) -> Quantity {
    Quantity(Fixed::new(value, 0))
}

fn candle(start: i64, high: i128, low: i128) -> CandleFlow {
    CandleFlow {
        schema_version: 1,
        state: FlowState::Final,
        instrument: InstrumentId(7),
        start_ts: start,
        end_ts: start + 60,
        high: Some(price(high)),
        low: Some(price(low)),
        total_volume: qty(10),
        trade_count: 1,
        sources: Vec::new(),
    }
}

fn bubble(candle_start: i64, id: u64, at: i128, strength: i128) -> OrderFlowBubble {
    OrderFlowBubble {
        id,
        instrument: InstrumentId(7),
        candle_start_ns: candle_start,
        candle_end_ns: candle_start + 60,
        segment: MarketSegment::LinearPerpetual,
        sources: vec![VenueId(1)],
        tier: BubbleTier::F3,
        mode: BubbleMode::Volume,
        direction: BubbleDirection::Buy,
        anchor_price: price(at),
        low_price: price(at),
        high_price: price(at),
        buy_volume: qty(strength),
        sell_volume: qty(0),
        unknown_volume: qty(0),
        total_volume: qty(strength),
        delta: Fixed::new(strength, 0),
        strength: Fixed::new(strength, 0),
        threshold: Fixed::new(1, 0),
        visual_size: 20,
        label: Some(Fixed::new(strength, 0)),
        shape: BubbleShape::Diamond,
        merged_count: 1,
    }
}

fn engine() -> StructuralLevelEngine {
    StructuralLevelEngine::new(
        GridSpec::new(0, 0, Fixed::new(1, 0), 1).unwrap(),
        StructuralLevelConfig::new(0, 64, 2).unwrap(),
    )
    .unwrap()
}

#[test]
fn naked_line_activates_only_when_next_candle_does_not_touch() {
    let mut engine = engine();
    engine
        .ingest_finalized(&candle(0, 105, 95), &[bubble(0, 1, 100, 50)])
        .unwrap();
    assert!(
        engine
            .snapshot()
            .iter()
            .all(|level| level.kind != StructuralLevelKind::Naked)
    );

    engine.ingest_finalized(&candle(60, 110, 101), &[]).unwrap();
    let naked = engine
        .snapshot()
        .into_iter()
        .find(|level| level.kind == StructuralLevelKind::Naked)
        .unwrap();
    assert_eq!(naked.price, price(100));
    assert_eq!(naked.state, StructuralLevelState::Active);

    engine.ingest_finalized(&candle(120, 101, 99), &[]).unwrap();
    let touched = engine
        .snapshot()
        .into_iter()
        .find(|level| level.id == naked.id)
        .unwrap();
    assert_eq!(touched.state, StructuralLevelState::Touched);
    assert_eq!(touched.touched_at_ns, Some(180));
}

#[test]
fn next_candle_touch_prevents_naked_line() {
    let mut engine = engine();
    engine
        .ingest_finalized(&candle(0, 105, 95), &[bubble(0, 1, 100, 50)])
        .unwrap();
    engine.ingest_finalized(&candle(60, 103, 99), &[]).unwrap();
    assert!(
        engine
            .snapshot()
            .iter()
            .all(|level| level.kind != StructuralLevelKind::Naked)
    );
}

#[test]
fn reaction_line_requires_a_confirmed_three_candle_swing() {
    let mut engine = engine();
    engine.ingest_finalized(&candle(0, 103, 98), &[]).unwrap();
    engine
        .ingest_finalized(&candle(60, 110, 100), &[bubble(60, 2, 108, 70)])
        .unwrap();
    assert!(
        engine
            .snapshot()
            .iter()
            .all(|level| level.kind != StructuralLevelKind::ReactionHigh)
    );

    engine.ingest_finalized(&candle(120, 106, 99), &[]).unwrap();
    let reaction = engine
        .snapshot()
        .into_iter()
        .find(|level| level.kind == StructuralLevelKind::ReactionHigh)
        .unwrap();
    assert_eq!(reaction.price, price(110));
    assert_eq!(reaction.source_bubble_id, 2);
}

#[test]
fn reaction_line_uses_one_strongest_center_bubble() {
    let mut engine = engine();
    engine.ingest_finalized(&candle(0, 103, 98), &[]).unwrap();
    engine
        .ingest_finalized(
            &candle(60, 110, 100),
            &[bubble(60, 2, 108, 70), bubble(60, 3, 109, 90)],
        )
        .unwrap();
    engine.ingest_finalized(&candle(120, 106, 99), &[]).unwrap();

    let reactions: Vec<_> = engine
        .snapshot()
        .into_iter()
        .filter(|level| level.kind == StructuralLevelKind::ReactionHigh)
        .collect();
    assert_eq!(reactions.len(), 1);
    assert_eq!(reactions[0].source_bubble_id, 3);
}

#[test]
fn top_day_and_week_keep_only_strongest_bubbles_per_side() {
    let mut engine = engine();
    engine
        .ingest_finalized(
            &candle(0, 110, 90),
            &[
                bubble(0, 1, 100, 10),
                bubble(0, 2, 101, 30),
                bubble(0, 3, 102, 20),
            ],
        )
        .unwrap();

    for kind in [StructuralLevelKind::TopDay, StructuralLevelKind::TopWeek] {
        let mut strengths: Vec<i128> = engine
            .snapshot()
            .into_iter()
            .filter(|level| level.kind == kind)
            .map(|level| level.strength.coefficient)
            .collect();
        strengths.sort_unstable();
        assert_eq!(strengths, vec![20, 30]);
    }

    engine
        .ingest_finalized(
            &candle(60, 112, 91),
            &[
                bubble(60, 4, 103, 25),
                bubble(60, 5, 104, 40),
                bubble(60, 6, 105, 5),
            ],
        )
        .unwrap();
    for kind in [StructuralLevelKind::TopDay, StructuralLevelKind::TopWeek] {
        let mut strengths: Vec<i128> = engine
            .snapshot()
            .into_iter()
            .filter(|level| level.kind == kind)
            .map(|level| level.strength.coefficient)
            .collect();
        strengths.sort_unstable();
        assert_eq!(strengths, vec![30, 40]);
    }
}

#[test]
fn duplicate_or_regressing_candles_are_rejected_atomically() {
    let mut engine = engine();
    let first = candle(60, 110, 90);
    engine
        .ingest_finalized(&first, &[bubble(60, 1, 100, 10)])
        .unwrap();
    let before = serde_json::to_vec(&engine).unwrap();

    assert!(engine.ingest_finalized(&first, &[]).is_err());
    assert_eq!(serde_json::to_vec(&engine).unwrap(), before);
    assert!(engine.ingest_finalized(&candle(0, 110, 90), &[]).is_err());
    assert_eq!(serde_json::to_vec(&engine).unwrap(), before);
}
