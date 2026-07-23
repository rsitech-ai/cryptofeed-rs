//! Fixture-driven Kraken Spot decode + session tests (offline).

use marketfeed_adapter_api::{
    ActionBuffer, ConcreteSubscriptionSet, EventBatch, SessionAction, SessionInput, SessionMachine,
    SessionSpec,
};
use marketfeed_adapter_kraken::{KrakenSessionConfig, KrakenSpotSession};
use marketfeed_model::{
    AggressorSide, CatalogVersion, CatalogView, FrameStamp, InstrumentId, MarketEvent, SessionId,
    SystemEvent, TimestampNs, VenueId,
};
use std::collections::HashMap;

fn stamp(n: i64) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(n),
        mono_ns: n as u64,
    }
}

fn session() -> KrakenSpotSession {
    let mut ids = HashMap::new();
    ids.insert("BTC/USD".into(), InstrumentId(1));
    KrakenSpotSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(7), CatalogVersion(1)),
        KrakenSessionConfig {
            symbols: vec!["BTC/USD".into()],
            instrument_ids: ids,
            session: SessionId(1),
            ..KrakenSessionConfig::default()
        },
    )
}

fn drive_text(s: &mut KrakenSpotSession, text: &str, ts: i64) -> ActionBuffer {
    let mut buf = ActionBuffer::new();
    let mut bytes = text.as_bytes().to_vec();
    s.on_input(
        SessionInput::TextFrame {
            bytes: &mut bytes,
            received: stamp(ts),
        },
        &mut buf,
    )
    .unwrap();
    buf
}

fn markets(buf: &ActionBuffer) -> Vec<MarketEvent> {
    buf.as_slice()
        .iter()
        .filter_map(|a| match a {
            SessionAction::EmitBatch(EventBatch { events, .. }) => Some(events),
            _ => None,
        })
        .flatten()
        .map(|e| e.payload.clone())
        .collect()
}

#[test]
fn trade_and_quote_fixtures() {
    let mut s = session();
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let sends: Vec<_> = out
        .as_slice()
        .iter()
        .filter(|a| matches!(a, SessionAction::SendText(_)))
        .collect();
    assert_eq!(sends.len(), 2);

    let trade = r#"{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":65000.12,"qty":0.001,"ord_type":"limit","trade_id":42,"timestamp":"2023-09-25T07:49:37.708706Z"}]}"#;
    let m = markets(&drive_text(&mut s, trade, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::Trade(t) if t.aggressor == AggressorSide::Buy
    ));

    let quote = r#"{"channel":"ticker","type":"update","data":[{"symbol":"BTC/USD","bid":65000.0,"bid_qty":1.2,"ask":65000.1,"ask_qty":0.8,"last":65000.05,"volume":1.5,"vwap":1,"low":64000.0,"high":66000.0,"change":0,"change_pct":0}]}"#;
    let m = markets(&drive_text(&mut s, quote, 3));
    assert_eq!(m.len(), 2);
    assert!(matches!(&m[0], MarketEvent::Quote(_)));
    assert!(matches!(
        &m[1],
        MarketEvent::Statistics24h(st)
            if st.high.as_ref().unwrap().0 == marketfeed_model::Fixed::parse_str("66000.0").unwrap()
                && st.low.as_ref().unwrap().0 == marketfeed_model::Fixed::parse_str("64000.0").unwrap()
                && st.volume.as_ref().unwrap().0 == marketfeed_model::Fixed::parse_str("1.5").unwrap()
                && st.close.as_ref().unwrap().0 == marketfeed_model::Fixed::parse_str("65000.05").unwrap()
    ));
}

#[test]
fn initial_status_frame_is_normalized_as_venue_status() {
    let mut s = session();
    let out = drive_text(
        &mut s,
        r#"{"channel":"status","type":"update","data":[{"version":"2.0.10","system":"online","api_version":"v2","connection_id":3699665231700023789}]}"#,
        2,
    );

    assert!(matches!(
        markets(&out).as_slice(),
        [MarketEvent::VenueStatus(status)] if status.message == "online"
    ));
    assert!(!out.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(SystemEvent::UnknownMessage { .. })
    )));
}

#[test]
fn ohlc_candle_fixture_exact_fixed() {
    use marketfeed_adapter_api::CandleInterval;
    use marketfeed_model::Fixed;

    let mut ids = HashMap::new();
    ids.insert("BTC/USD".into(), InstrumentId(1));
    let mut s = KrakenSpotSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(7), CatalogVersion(1)),
        KrakenSessionConfig {
            symbols: vec!["BTC/USD".into()],
            instrument_ids: ids,
            session: SessionId(1),
            candle_intervals: vec![CandleInterval::M1],
            ..KrakenSessionConfig::default()
        },
    );
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let sub = out
        .as_slice()
        .iter()
        .find_map(|a| match a {
            SessionAction::SendText(b) => {
                let s = String::from_utf8_lossy(b);
                if s.contains("ohlc") {
                    Some(s.into_owned())
                } else {
                    None
                }
            }
            _ => None,
        })
        .expect("ohlc subscribe");
    assert!(
        sub.contains(r#""channel":"ohlc""#) || sub.contains("\"channel\": \"ohlc\""),
        "sub={sub}"
    );
    assert!(
        sub.contains("\"interval\":1") || sub.contains("\"interval\": 1"),
        "sub={sub}"
    );

    let raw = r#"{"channel":"ohlc","type":"update","data":[{"symbol":"BTC/USD","open":65000.1,"high":65020.5,"low":64990.0,"close":65015.2,"trades":12,"volume":1.234,"vwap":65010.0,"interval_begin":"2023-10-04T15:30:00.000000000Z","interval":1,"timestamp":"2023-10-04T15:31:00.000000Z"}]}"#;
    let m = markets(&drive_text(&mut s, raw, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::Candle(c)
            if c.open.0 == Fixed::parse_str("65000.1").unwrap()
                && c.high.0 == Fixed::parse_str("65020.5").unwrap()
                && c.low.0 == Fixed::parse_str("64990.0").unwrap()
                && c.close.0 == Fixed::parse_str("65015.2").unwrap()
                && c.volume.0 == Fixed::parse_str("1.234").unwrap()
                && c.interval_ns == 60_000_000_000
                && c.start_ts.0 > 0
    ));
}

#[test]
fn heartbeat_frame_is_accepted_without_panic() {
    let mut s = session();
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    let out = drive_text(&mut s, r#"{"channel":"heartbeat"}"#, 2);
    // Heartbeat is a pure no-op: no market events, no system events, no panic.
    assert!(out.is_empty());
}

#[test]
fn record_replay_kraken_trade_quote_identical() {
    use marketfeed_engine::{SessionRunner, SessionRunnerConfig};
    use marketfeed_model::OverflowPolicy;
    use marketfeed_replay::ReplayRunner;

    let mut live = SessionRunner::new(
        Box::new(session()),
        SessionRunnerConfig {
            session: SessionId(1),
            overflow: OverflowPolicy::FailEngine,
            record: true,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();
    live.on_connected(TimestampNs(100)).unwrap();

    let frames = [
        r#"{"method":"subscribe","success":true,"result":{"channel":"trade","symbol":"BTC/USD"}}"#,
        r#"{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"sell","price":1.00,"qty":1,"ord_type":"market","trade_id":1,"timestamp":"2023-09-25T07:49:37.708706Z"}]}"#,
        r#"{"channel":"ticker","type":"update","data":[{"symbol":"BTC/USD","bid":1.00,"bid_qty":1,"ask":1.01,"ask_qty":1,"last":1,"volume":1,"vwap":1,"low":1,"high":1,"change":0,"change_pct":0}]}"#,
    ];
    let mut ts = 100i64;
    for f in frames {
        ts += 1;
        let mut b = f.as_bytes().to_vec();
        live.on_text_frame(&mut b, stamp(ts)).unwrap();
    }
    let live_events: Vec<_> = live
        .market_batches
        .iter()
        .flat_map(|b| b.events.iter().map(|e| e.payload.clone()))
        .filter(|e| !matches!(e, MarketEvent::VenueStatus(_)))
        .collect();
    let bytes = live.recording_bytes().unwrap();

    let mut replay_machine = session();
    let mut replay = ReplayRunner::new(64);
    let outcome = replay
        .replay_bytes(&mut replay_machine, bytes, TimestampNs(100))
        .unwrap();
    let replay_events: Vec<_> = outcome
        .market_batches
        .iter()
        .flat_map(|b| b.events.iter().map(|e| e.payload.clone()))
        .collect();
    assert_eq!(live_events, replay_events);
}
