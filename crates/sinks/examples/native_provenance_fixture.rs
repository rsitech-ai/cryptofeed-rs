//! Synthetic exact wire fixtures for an independent generated protobuf reader.
use marketfeed_model::*;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = std::path::PathBuf::from(std::env::args().nth(1).ok_or("directory required")?);
    std::fs::create_dir(&directory)?;
    let candle = Candle {
        provenance: Some(Box::new(CandleProvenance {
            source_channel: "kline.1.BTCUSD".into(),
            native_attributes: std::collections::BTreeMap::from([
                ("timestamp".into(), "1700000000000".into()),
                ("turnover".into(), "0.000000000000000001".into()),
            ]),
        })),
        native_volume_ccy: Some(Quantity(Fixed::new(17, 3))),
        open: Price(Fixed::new(100, 0)),
        high: Price(Fixed::new(101, 0)),
        low: Price(Fixed::new(99, 0)),
        close: Price(Fixed::new(100, 0)),
        volume: Quantity(Fixed::new(2, 0)),
        interval_ns: 60_000_000_000,
        start_ts: TimestampNs(1),
        is_closed: Some(false),
        quote_volume: Some(Quantity(Fixed::new(i128::MAX, 255))),
    };
    let trade = Trade {
        price: Price(Fixed::new(i128::MAX, 38)),
        quantity: Quantity(Fixed::new(1, 18)),
        aggressor: AggressorSide::Buy,
        trade_id: Some(SourceId("9007199254740993".into())),
        provenance: Some(Box::new(TradeProvenance {
            source_channel: "unit-aggregate".into(),
            granularity: TradeGranularity::NativeAggregate,
            constituent_first_id: Some(SourceId("9007199254740993".into())),
            constituent_last_id: Some(SourceId(u64::MAX.to_string())),
            native_count: Some(u64::MAX),
            native_attributes: std::collections::BTreeMap::from([(
                "count_meaning".into(),
                "unverified-native-only".into(),
            )]),
        })),
    };
    let mut empty_id = trade.clone();
    empty_id.trade_id = Some(SourceId(String::new()));
    let source = empty_id.provenance.as_mut().unwrap();
    source.constituent_first_id = Some(SourceId(String::new()));
    source.constituent_last_id = Some(SourceId(String::new()));
    source.native_count = Some(0);
    let mut legacy = candle.clone();
    legacy.native_volume_ccy = None;
    legacy.provenance = None;
    legacy.is_closed = None;
    legacy.quote_volume = None;
    for (index, payload) in [
        MarketEvent::Trade(trade),
        MarketEvent::Candle(candle),
        MarketEvent::Candle(legacy),
        MarketEvent::Trade(empty_id),
    ]
    .into_iter()
    .enumerate()
    {
        let event = EventEnvelope {
            schema_version: 1,
            venue: VenueId(4),
            instrument: Some(InstrumentId(1)),
            connection: ConnectionId(1),
            session: SessionId(1),
            frame_seq: 1,
            event_index: index as u16,
            exchange_ts: Some(TimestampNs(1)),
            receive_ts: TimestampNs(2),
            source_sequence: None,
            flags: EventFlags::empty(),
            payload,
        };
        std::fs::write(
            directory.join(format!("{index}.pb")),
            marketfeed_sinks::encode_event_envelope(&event),
        )?;
        std::fs::write(
            directory.join(format!("{index}.json")),
            serde_json::to_vec(&marketfeed_recording::event_envelope_json(&event))?,
        )?;
    }
    Ok(())
}
