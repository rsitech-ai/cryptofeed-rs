use marketfeed_analytics::{
    AnalyticsError, CandleFlowBuilder, FlowConfig, FlowSource, FlowState, GridSpec, MarketSegment,
    TimeframeSpec, TradeInput,
};
use marketfeed_model::{
    AggressorSide, ConnectionId, EventEnvelope, EventFlags, Fixed, InstrumentId, MarketEvent,
    Price, Quantity, SessionId, TimestampNs, Trade, VenueId,
};

fn grid() -> GridSpec {
    GridSpec::new(0, 0, Fixed::new(1, 0), 1).unwrap()
}

fn time() -> TimeframeSpec {
    TimeframeSpec::new(60, 60, 300, 0, 900).unwrap()
}

fn config() -> FlowConfig {
    FlowConfig::new(8, 32, 128).unwrap()
}

fn source(venue: u16, segment: MarketSegment) -> FlowSource {
    FlowSource {
        venue: VenueId(venue),
        segment,
    }
}

fn input(
    timestamp_ns: i64,
    source: FlowSource,
    price: i128,
    quantity: i128,
    aggressor: AggressorSide,
) -> TradeInput {
    TradeInput {
        instrument: InstrumentId(7),
        source,
        timestamp_ns,
        price: Price(Fixed::new(price, 0)),
        quantity: Quantity(Fixed::new(quantity, 0)),
        aggressor,
    }
}

#[test]
fn candle_flow_tracks_buy_sell_unknown_delta_and_timestamps() {
    let mut builder = CandleFlowBuilder::new(InstrumentId(7), grid(), time(), config()).unwrap();
    let spot = source(1, MarketSegment::Spot);

    builder
        .ingest(input(1, spot, 100, 5, AggressorSide::Buy))
        .unwrap();
    builder
        .ingest(input(2, spot, 100, 2, AggressorSide::Sell))
        .unwrap();
    builder
        .ingest(input(3, spot, 100, 1, AggressorSide::Unknown))
        .unwrap();

    let candle = builder.live_snapshot().unwrap().unwrap();
    assert_eq!(candle.state, FlowState::Live);
    assert_eq!(candle.start_ts, 0);
    assert_eq!(candle.end_ts, 60);
    assert_eq!(candle.trade_count, 3);
    assert_eq!(candle.total_volume, Quantity(Fixed::new(8, 0)));
    assert_eq!(candle.sources.len(), 1);

    let level = &candle.sources[0].levels[0];
    assert_eq!(level.price, Price(Fixed::new(100, 0)));
    assert_eq!(level.buy_volume, Quantity(Fixed::new(5, 0)));
    assert_eq!(level.sell_volume, Quantity(Fixed::new(2, 0)));
    assert_eq!(level.unknown_volume, Quantity(Fixed::new(1, 0)));
    assert_eq!(level.total_volume, Quantity(Fixed::new(8, 0)));
    assert_eq!(level.delta, Fixed::new(3, 0));
    assert_eq!(level.trade_count, 3);
    assert_eq!(level.first_timestamp_ns, 1);
    assert_eq!(level.last_timestamp_ns, 3);
}

#[test]
fn identical_prices_remain_partitioned_by_market_segment() {
    let mut builder = CandleFlowBuilder::new(InstrumentId(7), grid(), time(), config()).unwrap();

    builder
        .ingest(input(
            1,
            source(1, MarketSegment::Spot),
            100,
            5,
            AggressorSide::Buy,
        ))
        .unwrap();
    builder
        .ingest(input(
            2,
            source(1, MarketSegment::LinearPerpetual),
            100,
            7,
            AggressorSide::Sell,
        ))
        .unwrap();

    let candle = builder.finish().unwrap().unwrap();
    assert_eq!(candle.sources.len(), 2);
    assert_eq!(candle.sources[0].source.segment, MarketSegment::Spot);
    assert_eq!(
        candle.sources[1].source.segment,
        MarketSegment::LinearPerpetual
    );
    assert_eq!(
        candle.sources[0].levels[0].total_volume,
        Quantity(Fixed::new(5, 0))
    );
    assert_eq!(
        candle.sources[1].levels[0].total_volume,
        Quantity(Fixed::new(7, 0))
    );
}

#[test]
fn candle_rollover_finalizes_prior_flow_and_starts_next() {
    let mut builder = CandleFlowBuilder::new(InstrumentId(7), grid(), time(), config()).unwrap();
    let spot = source(1, MarketSegment::Spot);
    builder
        .ingest(input(1, spot, 100, 1, AggressorSide::Buy))
        .unwrap();

    let finalized = builder
        .ingest(input(61, spot, 101, 2, AggressorSide::Sell))
        .unwrap()
        .unwrap();
    assert_eq!(finalized.state, FlowState::Final);
    assert_eq!(finalized.start_ts, 0);
    assert_eq!(finalized.end_ts, 60);

    let live = builder.live_snapshot().unwrap().unwrap();
    assert_eq!(live.start_ts, 60);
    assert_eq!(live.total_volume, Quantity(Fixed::new(2, 0)));
}

#[test]
fn grouped_flow_preserves_exact_extrema_while_aggregating_levels() {
    let grouped_grid = GridSpec::new(2, 0, Fixed::new(25, 2), 4).unwrap();
    let mut builder =
        CandleFlowBuilder::new(InstrumentId(7), grouped_grid, time(), config()).unwrap();
    let spot = source(1, MarketSegment::Spot);

    builder
        .ingest(TradeInput {
            instrument: InstrumentId(7),
            source: spot,
            timestamp_ns: 1,
            price: Price(Fixed::new(100_25, 2)),
            quantity: Quantity(Fixed::new(1, 0)),
            aggressor: AggressorSide::Buy,
        })
        .unwrap();
    builder
        .ingest(TradeInput {
            instrument: InstrumentId(7),
            source: spot,
            timestamp_ns: 2,
            price: Price(Fixed::new(100_75, 2)),
            quantity: Quantity(Fixed::new(2, 0)),
            aggressor: AggressorSide::Sell,
        })
        .unwrap();

    let candle = builder.finish().unwrap().unwrap();
    assert_eq!(candle.low, Some(Price(Fixed::new(100_25, 2))));
    assert_eq!(candle.high, Some(Price(Fixed::new(100_75, 2))));
    assert_eq!(candle.sources[0].levels.len(), 1);
    assert_eq!(
        candle.sources[0].levels[0].price,
        Price(Fixed::new(100_00, 2))
    );
    assert_eq!(
        candle.sources[0].levels[0].total_volume,
        Quantity(Fixed::new(3, 0))
    );
}

#[test]
fn envelope_adapter_uses_exchange_time_and_rejects_invalid_events() {
    let envelope = EventEnvelope {
        schema_version: 1,
        venue: VenueId(3),
        instrument: Some(InstrumentId(7)),
        connection: ConnectionId(1),
        session: SessionId(1),
        frame_seq: 1,
        event_index: 0,
        exchange_ts: Some(TimestampNs(11)),
        receive_ts: TimestampNs(19),
        source_sequence: None,
        flags: EventFlags::empty(),
        payload: MarketEvent::Trade(Trade {
            price: Price(Fixed::new(100, 0)),
            quantity: Quantity(Fixed::new(4, 0)),
            aggressor: AggressorSide::Buy,
            trade_id: None,
        }),
    };
    let converted = TradeInput::from_envelope(&envelope, MarketSegment::LinearPerpetual).unwrap();
    assert_eq!(converted.timestamp_ns, 11);
    assert_eq!(converted.source.venue, VenueId(3));
    assert_eq!(converted.instrument, InstrumentId(7));

    let mut missing = envelope.clone();
    missing.instrument = None;
    assert!(matches!(
        TradeInput::from_envelope(&missing, MarketSegment::Spot),
        Err(AnalyticsError::MissingInstrument)
    ));

    let mut non_trade = envelope;
    non_trade.payload = MarketEvent::VenueStatus(marketfeed_model::VenueStatus {
        message: "ok".to_owned(),
    });
    assert!(matches!(
        TradeInput::from_envelope(&non_trade, MarketSegment::Spot),
        Err(AnalyticsError::NonTradeEvent)
    ));
}

#[test]
fn capacity_instrument_and_lateness_errors_are_atomic() {
    let limited = FlowConfig::new(1, 1, 2).unwrap();
    let mut builder = CandleFlowBuilder::new(InstrumentId(7), grid(), time(), limited).unwrap();
    let spot = source(1, MarketSegment::Spot);
    builder
        .ingest(input(1, spot, 100, 1, AggressorSide::Buy))
        .unwrap();
    let before = serde_json::to_vec(&builder).unwrap();

    assert!(matches!(
        builder.ingest(input(2, spot, 101, 1, AggressorSide::Buy)),
        Err(AnalyticsError::CapacityExceeded {
            resource: "price levels per source candle",
            limit: 1
        })
    ));
    assert_eq!(serde_json::to_vec(&builder).unwrap(), before);

    assert!(matches!(
        builder.ingest(input(
            2,
            source(2, MarketSegment::Spot),
            100,
            1,
            AggressorSide::Buy
        )),
        Err(AnalyticsError::CapacityExceeded {
            resource: "sources per candle",
            limit: 1
        })
    ));
    assert_eq!(serde_json::to_vec(&builder).unwrap(), before);

    assert!(matches!(
        builder.ingest(TradeInput {
            instrument: InstrumentId(8),
            ..input(2, spot, 100, 1, AggressorSide::Buy)
        }),
        Err(AnalyticsError::InstrumentMismatch { .. })
    ));
    assert_eq!(serde_json::to_vec(&builder).unwrap(), before);

    builder
        .ingest(input(2, spot, 100, 1, AggressorSide::Sell))
        .unwrap();
    let full = serde_json::to_vec(&builder).unwrap();
    assert!(matches!(
        builder.ingest(input(3, spot, 100, 1, AggressorSide::Buy)),
        Err(AnalyticsError::CapacityExceeded {
            resource: "trades per candle",
            limit: 2
        })
    ));
    assert_eq!(serde_json::to_vec(&builder).unwrap(), full);

    assert!(builder.advance_to(61).unwrap().is_some());
    let advanced = serde_json::to_vec(&builder).unwrap();
    assert!(matches!(
        builder.ingest(input(59, spot, 100, 1, AggressorSide::Buy)),
        Err(AnalyticsError::LateTrade { .. })
    ));
    assert_eq!(serde_json::to_vec(&builder).unwrap(), advanced);
}
