//! Fixture-driven Coinbase decode + session tests (offline).

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, CandleInterval, ConcreteSubscriptionSet, EventBatch, HttpResponse,
    ReconnectReason, SessionAction, SessionInput, SessionMachine, SessionSpec,
};
use marketfeed_adapter_coinbase::{
    CANDLE_POLL_INTERVAL_MS, CANDLE_TIMER_ID, CoinbaseSessionConfig, CoinbaseSpotSession,
    HEARTBEAT_TIMER_ID,
};
use marketfeed_model::{
    AggressorSide, CatalogVersion, CatalogView, Fixed, FrameStamp, InstrumentId, MarketEvent,
    SessionId, SystemEvent, TimestampNs, VenueId,
};
use std::collections::HashMap;

fn stamp(n: i64) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(n),
        mono_ns: n as u64,
    }
}

fn session() -> CoinbaseSpotSession {
    session_with_candles(Vec::new())
}

fn session_with_candles(candle_intervals: Vec<CandleInterval>) -> CoinbaseSpotSession {
    let mut ids = HashMap::new();
    ids.insert("BTC-USD".into(), InstrumentId(1));
    CoinbaseSpotSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(16), CatalogVersion(1)),
        CoinbaseSessionConfig {
            products: vec!["BTC-USD".into()],
            instrument_ids: ids,
            session: SessionId(1),
            candle_intervals,
            ..CoinbaseSessionConfig::default()
        },
    )
}

fn drive_text(s: &mut CoinbaseSpotSession, text: &str, ts: i64) -> ActionBuffer {
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

fn http_ids(buf: &ActionBuffer) -> Vec<(u64, String)> {
    buf.as_slice()
        .iter()
        .filter_map(|a| match a {
            SessionAction::RequestHttp(r) => Some((r.id, r.url.clone())),
            _ => None,
        })
        .collect()
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
fn connect_sends_subscribe_matches_ticker() {
    let mut s = session();
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let sent = out
        .as_slice()
        .iter()
        .find_map(|a| match a {
            SessionAction::SendText(b) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        })
        .expect("subscribe");
    assert!(sent.contains("\"matches\""), "{sent}");
    assert!(sent.contains("\"ticker\""), "{sent}");
    assert!(sent.contains("BTC-USD"), "{sent}");
    assert!(!sent.contains("level2"), "{sent}");
}

#[test]
fn match_and_ticker_fixtures_exact_fixed() {
    let mut s = session();
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    let trade = r#"{"type":"match","trade_id":10,"sequence":50,"time":"2014-11-07T08:19:27.028459Z","product_id":"BTC-USD","size":"5.23512","price":"400.23","side":"sell"}"#;
    let m = markets(&drive_text(&mut s, trade, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::Trade(t)
            if t.aggressor == AggressorSide::Buy
                && t.price.0 == Fixed::parse_str("400.23").unwrap()
                && t.quantity.0 == Fixed::parse_str("5.23512").unwrap()
    ));

    let ticker = r#"{"type":"ticker","sequence":1,"product_id":"BTC-USD","price":"10.01","best_bid":"9.99","best_ask":"10.01","best_bid_size":"1.5","best_ask_size":"2.25","time":"2023-09-25T07:49:37.708706Z"}"#;
    let m = markets(&drive_text(&mut s, ticker, 3));
    assert!(matches!(
        &m[0],
        MarketEvent::Quote(q)
            if q.bid_price.0 == Fixed::parse_str("9.99").unwrap()
                && q.ask_price.0 == Fixed::parse_str("10.01").unwrap()
                && q.bid_quantity.as_ref().map(|x| x.0) == Some(Fixed::parse_str("1.5").unwrap())
                && q.ask_quantity.as_ref().map(|x| x.0) == Some(Fixed::parse_str("2.25").unwrap())
    ));

    let incomplete_ack =
        r#"{"type":"subscriptions","channels":[{"name":"matches","product_ids":["BTC-USD"]}]}"#;
    let buf = drive_text(&mut s, incomplete_ack, 4);
    assert!(
        buf.as_slice()
            .iter()
            .all(|a| !matches!(a, SessionAction::MarkLive))
    );

    let complete_ack = r#"{"type":"subscriptions","channels":[{"name":"matches","product_ids":["BTC-USD"]},{"name":"ticker","product_ids":["BTC-USD"]},{"name":"heartbeat","product_ids":["BTC-USD"]},{"name":"status","product_ids":[]}]}"#;
    let buf = drive_text(&mut s, complete_ack, 5);
    assert!(
        buf.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::MarkLive))
    );
}

#[test]
fn heartbeat_metadata_reschedules_watchdog_and_timeout_reconnects() {
    let mut s = session();
    let mut connected = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut connected,
    )
    .unwrap();
    assert!(connected.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::ScheduleTimer(timer)
            if timer.timer_id == HEARTBEAT_TIMER_ID
                && timer.fire_at == TimestampNs(15_000_000_001)
    )));

    let heartbeat = r#"{"type":"heartbeat","sequence":90,"last_trade_id":20,"product_id":"BTC-USD","time":"2014-11-07T08:19:28.000000Z"}"#;
    let heartbeat_actions = drive_text(&mut s, heartbeat, 10);
    assert!(heartbeat_actions.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::ScheduleTimer(timer)
            if timer.timer_id == HEARTBEAT_TIMER_ID && timer.fire_at.0 > 10
    )));
    let state = s
        .heartbeat_state("BTC-USD")
        .expect("heartbeat metadata retained");
    assert_eq!(state.sequence, 90);
    assert_eq!(state.last_trade_id, 20);

    let mut timeout = ActionBuffer::new();
    s.on_input(
        SessionInput::Timer {
            timer_id: HEARTBEAT_TIMER_ID,
            now: TimestampNs(i64::MAX),
        },
        &mut timeout,
    )
    .unwrap();
    assert!(timeout.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(marketfeed_model::SystemEvent::HeartbeatMissed)
    )));
    assert!(
        timeout
            .as_slice()
            .iter()
            .any(|action| matches!(action, SessionAction::Reconnect(ReconnectReason::Heartbeat)))
    );
}

#[test]
fn record_replay_match_ticker_identical() {
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
        r#"{"type":"subscriptions","channels":[{"name":"matches","product_ids":["BTC-USD"]},{"name":"ticker","product_ids":["BTC-USD"]},{"name":"heartbeat","product_ids":["BTC-USD"]},{"name":"status","product_ids":[]}]}"#,
        r#"{"type":"match","trade_id":1,"sequence":1,"time":"2023-09-25T07:49:37.708706Z","product_id":"BTC-USD","size":"1.0","price":"1.00","side":"buy"}"#,
        r#"{"type":"ticker","product_id":"BTC-USD","best_bid":"1.00","best_ask":"1.10","best_bid_size":"1","best_ask_size":"1","time":"2023-09-25T07:49:38.000000Z"}"#,
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

#[test]
fn candles_rest_timer_fixture_exact_fixed() {
    let mut s = session_with_candles(vec![CandleInterval::M1]);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let (req_id, url) = http_ids(&out)
        .into_iter()
        .find(|(_, u)| u.contains("/candles"))
        .expect("candle request");
    assert!(
        url.contains("products/BTC-USD/candles") && url.contains("granularity=60"),
        "{url}"
    );
    assert!(out.as_slice().iter().any(|a| matches!(
        a, SessionAction::ScheduleTimer(t) if t.timer_id == CANDLE_TIMER_ID
            && t.fire_at.0 == 1 + CANDLE_POLL_INTERVAL_MS * 1_000_000
    )));
    let mut candle_out = ActionBuffer::new();
    s.on_input(
        SessionInput::HttpResponse {
            request_id: req_id,
            response: &HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(
                    br#"[[1609459200,"0.0015","0.0025","0.0010","0.0020","1000"]]"#,
                ),
            },
            received: stamp(7),
        },
        &mut candle_out,
    )
    .unwrap();
    assert!(matches!(&markets(&candle_out)[0], MarketEvent::Candle(c)
        if c.open.0 == Fixed::parse_str("0.0010").unwrap()
            && c.high.0 == Fixed::parse_str("0.0025").unwrap()
            && c.low.0 == Fixed::parse_str("0.0015").unwrap()
            && c.close.0 == Fixed::parse_str("0.0020").unwrap()
            && c.volume.0 == Fixed::parse_str("1000").unwrap()
            && c.interval_ns == 60_000_000_000
            && c.start_ts == TimestampNs(1_609_459_200_000_000_000)));
    let fire_at = TimestampNs(1 + CANDLE_POLL_INTERVAL_MS * 1_000_000);
    let mut tick = ActionBuffer::new();
    s.on_input(
        SessionInput::Timer {
            timer_id: CANDLE_TIMER_ID,
            now: fire_at,
        },
        &mut tick,
    )
    .unwrap();
    assert!(http_ids(&tick).iter().any(|(_, u)| u.contains("/candles")));
    assert!(tick.as_slice().iter().any(|a| matches!(
        a, SessionAction::ScheduleTimer(t) if t.timer_id == CANDLE_TIMER_ID
            && t.fire_at.0 == fire_at.0 + CANDLE_POLL_INTERVAL_MS * 1_000_000
    )));
}

#[test]
fn candles_http_failure_preserves_a_bounded_response_preview() {
    let mut s = session_with_candles(vec![CandleInterval::M1]);
    let mut connected = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut connected,
    )
    .unwrap();
    let (req_id, _) = http_ids(&connected)
        .into_iter()
        .find(|(_, url)| url.contains("/candles"))
        .expect("candle request");

    let mut response = ActionBuffer::new();
    s.on_input(
        SessionInput::HttpResponse {
            request_id: req_id,
            response: &HttpResponse {
                status: 400,
                headers: Vec::new(),
                body: Bytes::from(vec![b'x'; 300]),
            },
            received: stamp(2),
        },
        &mut response,
    )
    .unwrap();

    let detail = response
        .as_slice()
        .iter()
        .find_map(|action| match action {
            SessionAction::EmitSystem(SystemEvent::ParseError { detail }) => Some(detail.as_str()),
            _ => None,
        })
        .expect("HTTP diagnostic");
    assert!(
        detail.starts_with("coinbase candles HTTP 400 body="),
        "{detail}"
    );
    assert!(detail.ends_with('…'), "{detail}");
    assert!(detail.len() < 300, "{detail}");
}
