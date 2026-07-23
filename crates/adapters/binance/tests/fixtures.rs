//! Fixture-driven Binance Spot decode + session tests (offline).

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, ConcreteSubscriptionSet, HttpResponse, SessionAction, SessionInput,
    SessionMachine, SessionSpec,
};
use marketfeed_adapter_binance::{BinanceSessionConfig, BinanceSpotSession};
use marketfeed_adapter_testkit::{assert_trade_aggressor, markets};
use marketfeed_model::{
    AggressorSide, CatalogVersion, CatalogView, FrameStamp, InstrumentId, MarketEvent, SessionId,
    TimestampNs, VenueId,
};
use std::collections::HashMap;

fn stamp(n: i64) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(n),
        mono_ns: n as u64,
    }
}

fn session(enable_l2: bool) -> BinanceSpotSession {
    let mut ids = HashMap::new();
    ids.insert("BTCUSDT".into(), InstrumentId(1));
    BinanceSpotSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(2), CatalogVersion(1)),
        BinanceSessionConfig {
            symbols: vec!["BTCUSDT".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2,
            ..BinanceSessionConfig::default()
        },
    )
}

fn drive_text(s: &mut BinanceSpotSession, text: &str, ts: i64) -> ActionBuffer {
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

#[test]
fn trade_and_quote_fixtures() {
    let mut s = session(false);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::SendText(_)))
    );

    let trade = r#"{"e":"trade","E":1000,"s":"BTCUSDT","t":42,"p":"65000.12","q":"0.001","T":1001,"m":false,"M":true}"#;
    let m = markets(&drive_text(&mut s, trade, 2));
    assert_trade_aggressor(&m[0], AggressorSide::Buy);

    let quote = r#"{"u":9,"s":"BTCUSDT","b":"65000.00","B":"1.2","a":"65000.10","A":"0.8"}"#;
    let m = markets(&drive_text(&mut s, quote, 3));
    assert!(matches!(&m[0], MarketEvent::Quote(_)));

    let sub = out
        .as_slice()
        .iter()
        .find_map(|a| match a {
            SessionAction::SendText(b) => Some(std::str::from_utf8(b).unwrap().to_string()),
            _ => None,
        })
        .expect("subscribe");
    assert!(sub.contains("btcusdt@ticker"), "subscribe={sub}");
}

#[test]
fn ticker_24h_stats_fixture_exact_fixed() {
    use marketfeed_model::Fixed;

    let mut s = session(false);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    let ticker = r#"{"e":"24hrTicker","E":1000,"s":"BTCUSDT","p":"100","P":"1.00","w":"100","x":"99","c":"65000.12","Q":"0.1","b":"65000.00","B":"1.2","a":"65000.10","A":"0.8","o":"64000.00","h":"66000.50","l":"63000.25","v":"12.5","q":"812500.00","O":0,"C":86400000,"F":0,"L":1,"n":1}"#;
    let m = markets(&drive_text(&mut s, ticker, 2));
    assert_eq!(m.len(), 2);
    let MarketEvent::Quote(q) = &m[0] else {
        panic!("expected Quote");
    };
    assert_eq!(q.bid_price.0, Fixed::new(6500000, 2));
    assert_eq!(q.ask_price.0, Fixed::new(6500010, 2));
    let MarketEvent::Statistics24h(stats) = &m[1] else {
        panic!("expected Statistics24h");
    };
    assert_eq!(stats.open.as_ref().unwrap().0, Fixed::new(6400000, 2));
    assert_eq!(stats.high.as_ref().unwrap().0, Fixed::new(6600050, 2));
    assert_eq!(stats.low.as_ref().unwrap().0, Fixed::new(6300025, 2));
    assert_eq!(stats.close.as_ref().unwrap().0, Fixed::new(6500012, 2));
    assert_eq!(stats.volume.as_ref().unwrap().0, Fixed::new(125, 1));
    assert_eq!(
        stats.quote_volume.as_ref().unwrap().0,
        Fixed::new(81250000, 2)
    );
}

#[test]
fn kline_candle_fixture_exact_fixed() {
    use marketfeed_adapter_api::CandleInterval;
    use marketfeed_model::Fixed;

    let mut ids = HashMap::new();
    ids.insert("BTCUSDT".into(), InstrumentId(1));
    let mut s = BinanceSpotSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(2), CatalogVersion(1)),
        BinanceSessionConfig {
            symbols: vec!["BTCUSDT".into()],
            instrument_ids: ids,
            session: SessionId(1),
            candle_intervals: vec![CandleInterval::M1],
            ..BinanceSessionConfig::default()
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
            SessionAction::SendText(b) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        })
        .expect("subscribe");
    assert!(sub.contains("btcusdt@kline_1m"), "subscribe={sub}");

    let kline = r#"{"e":"kline","E":123456789,"s":"BTCUSDT","k":{"t":123400000,"T":123460000,"s":"BTCUSDT","i":"1m","f":100,"L":200,"o":"0.0010","c":"0.0020","h":"0.0025","l":"0.0015","v":"1000","n":100,"x":true,"q":"1.0000","V":"500","Q":"0.500","B":"123456"}}"#;
    let m = markets(&drive_text(&mut s, kline, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::Candle(c)
            if c.open.0 == Fixed::new(10, 4)
                && c.high.0 == Fixed::new(25, 4)
                && c.low.0 == Fixed::new(15, 4)
                && c.close.0 == Fixed::new(20, 4)
                && c.volume.0 == Fixed::new(1000, 0)
                && c.interval_ns == 60_000_000_000
                && c.start_ts == TimestampNs(123_400_000_000_000)
    ));
}

#[test]
fn l2_snapshot_then_delta_and_gap_reconnect() {
    let mut s = session(true);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::RequestHttp(_)))
    );

    // Inject REST snapshot as HttpResponse id=1 (first request).
    let snap = br#"{"lastUpdateId":100,"bids":[["100.00","1.0"],["99.00","2.0"]],"asks":[["101.00","1.5"]]}"#;
    out.clear();
    s.on_input(
        SessionInput::HttpResponse {
            request_id: 1,
            response: &HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(snap),
            },
            received: stamp(3),
        },
        &mut out,
    )
    .unwrap();
    assert!(
        markets(&out)
            .iter()
            .any(|e| matches!(e, MarketEvent::BookSnapshot(_)))
    );

    // Contiguous delta (U <= expected, u advances).
    let delta =
        r#"{"e":"depthUpdate","E":2,"s":"BTCUSDT","U":101,"u":101,"b":[["100.00","1.5"]],"a":[]}"#;
    let m = markets(&drive_text(&mut s, delta, 5));
    assert!(matches!(&m[0], MarketEvent::BookDelta(_)));

    // Gap: first_u jumps.
    let gap =
        r#"{"e":"depthUpdate","E":3,"s":"BTCUSDT","U":200,"u":200,"b":[["100.00","2"]],"a":[]}"#;
    let buf = drive_text(&mut s, gap, 6);
    assert!(buf.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::Reconnect(marketfeed_adapter_api::ReconnectReason::SequenceGap)
    )));
}

#[test]
fn l2_duplicate_delta_is_noop() {
    let mut s = session(true);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let snap = br#"{"lastUpdateId":100,"bids":[["100.00","1.0"]],"asks":[["101.00","1.5"]]}"#;
    out.clear();
    s.on_input(
        SessionInput::HttpResponse {
            request_id: 1,
            response: &HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(snap),
            },
            received: stamp(3),
        },
        &mut out,
    )
    .unwrap();
    let delta =
        r#"{"e":"depthUpdate","E":2,"s":"BTCUSDT","U":101,"u":101,"b":[["100.00","1.5"]],"a":[]}"#;
    assert!(matches!(
        &markets(&drive_text(&mut s, delta, 5))[0],
        MarketEvent::BookDelta(_)
    ));
    // Stale/duplicate: final_u <= last_u → discard, no reconnect.
    let dup =
        r#"{"e":"depthUpdate","E":3,"s":"BTCUSDT","U":100,"u":101,"b":[["100.00","9"]],"a":[]}"#;
    let buf = drive_text(&mut s, dup, 6);
    assert!(markets(&buf).is_empty());
    assert!(
        !buf.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::Reconnect(_)))
    );
}

#[test]
fn l2_depth_buffer_overflow_reconnects() {
    let mut ids = HashMap::new();
    ids.insert("BTCUSDT".into(), InstrumentId(1));
    let mut s = BinanceSpotSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(2), CatalogVersion(1)),
        BinanceSessionConfig {
            symbols: vec!["BTCUSDT".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2: true,
            max_buffered_depth_events: 2,
            max_buffered_depth_bytes: 4 * 1024 * 1024,
            max_buffered_depth_span_ns: 5_000_000_000,
            ..BinanceSessionConfig::default()
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
    // Pre-snapshot: fill buffer beyond max_buffered_depth_events.
    for (i, ts) in [(90u64, 2i64), (91, 3), (92, 4)] {
        let text = format!(
            r#"{{"e":"depthUpdate","E":{i},"s":"BTCUSDT","U":{i},"u":{i},"b":[["100.00","1"]],"a":[]}}"#
        );
        let mut bytes = text.into_bytes();
        out.clear();
        s.on_input(
            SessionInput::TextFrame {
                bytes: &mut bytes,
                received: stamp(ts),
            },
            &mut out,
        )
        .unwrap();
    }
    assert!(out.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::Reconnect(marketfeed_adapter_api::ReconnectReason::SequenceGap)
    )));
}

#[test]
fn l2_depth_buffer_time_span_overflow_reconnects() {
    let mut ids = HashMap::new();
    ids.insert("BTCUSDT".into(), InstrumentId(1));
    let mut s = BinanceSpotSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(2), CatalogVersion(1)),
        BinanceSessionConfig {
            symbols: vec!["BTCUSDT".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2: true,
            max_buffered_depth_events: 10,
            max_buffered_depth_bytes: 4 * 1024 * 1024,
            max_buffered_depth_span_ns: 1,
            ..BinanceSessionConfig::default()
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
    let first =
        r#"{"e":"depthUpdate","E":90,"s":"BTCUSDT","U":90,"u":90,"b":[["100.00","1"]],"a":[]}"#;
    let _ = drive_text(&mut s, first, 2);
    let second =
        r#"{"e":"depthUpdate","E":91,"s":"BTCUSDT","U":91,"u":91,"b":[["100.00","1"]],"a":[]}"#;
    let out = drive_text(&mut s, second, 4);
    assert!(out.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::Reconnect(marketfeed_adapter_api::ReconnectReason::SequenceGap)
    )));
}

#[test]
fn silence_watchdog_timer_reconnects() {
    let mut s = session(false);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::ScheduleTimer(_))),
        "connect schedules heartbeat timer"
    );
    out.clear();
    s.on_input(
        SessionInput::Timer {
            timer_id: marketfeed_adapter_binance::HEARTBEAT_TIMER_ID,
            now: TimestampNs(1_000_000_000),
        },
        &mut out,
    )
    .unwrap();
    assert!(out.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::EmitSystem(marketfeed_model::SystemEvent::HeartbeatMissed)
    )));
    assert!(out.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::Reconnect(marketfeed_adapter_api::ReconnectReason::Heartbeat)
    )));
}

#[test]
fn record_replay_binance_trade_quote_identical() {
    use marketfeed_engine::{SessionRunner, SessionRunnerConfig};
    use marketfeed_model::OverflowPolicy;
    use marketfeed_replay::ReplayRunner;

    let mut live = SessionRunner::new(
        Box::new(session(false)),
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
        r#"{"result":null,"id":1}"#,
        r#"{"e":"trade","E":1,"s":"BTCUSDT","t":1,"p":"1.00","q":"1","T":1,"m":true,"M":true}"#,
        r#"{"u":1,"s":"BTCUSDT","b":"1.00","B":"1","a":"1.01","A":"1"}"#,
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
        // SessionRunner injects VenueStatus; ReplayRunner does not — compare MD only.
        .filter(|e| !matches!(e, MarketEvent::VenueStatus(_)))
        .collect();
    let bytes = live.recording_bytes().unwrap();

    let mut replay_machine = session(false);
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
