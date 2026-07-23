//! Fixture-driven Coinbase Advanced Trade T/Q/L2 + candles (offline SessionMachine).

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, CandleInterval, ConcreteSubscriptionSet, EventBatch, HttpResponse, SessionAction,
    SessionInput, SessionMachine, SessionSpec,
};
use marketfeed_adapter_coinbase::{
    ADV_CANDLE_POLL_INTERVAL_MS, ADV_CANDLE_TIMER_ID, CoinbaseAdvSession, CoinbaseAdvSessionConfig,
};
use marketfeed_model::{
    AggressorSide, BookOperation, BookSide, CatalogVersion, CatalogView, Fixed, FrameStamp,
    InstrumentId, InstrumentStatus, MarketEvent, SessionId, TimestampNs, VenueId,
};
use std::collections::HashMap;

fn stamp(n: i64) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(n),
        mono_ns: n as u64,
    }
}

fn session() -> CoinbaseAdvSession {
    session_with(Vec::new(), false)
}

fn session_with(candle_intervals: Vec<CandleInterval>, enable_l2: bool) -> CoinbaseAdvSession {
    let mut ids = HashMap::new();
    ids.insert("BTC-USD".into(), InstrumentId(1));
    CoinbaseAdvSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(18), CatalogVersion(1)),
        CoinbaseAdvSessionConfig {
            products: vec!["BTC-USD".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2,
            candle_intervals,
            ..CoinbaseAdvSessionConfig::default()
        },
    )
}

fn drive_text(s: &mut CoinbaseAdvSession, text: &str, ts: i64) -> ActionBuffer {
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

fn sent_texts(buf: &ActionBuffer) -> Vec<String> {
    buf.as_slice()
        .iter()
        .filter_map(|a| match a {
            SessionAction::SendText(b) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        })
        .collect()
}

#[test]
fn connect_sends_public_channel_subscribes() {
    let mut s = session();
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let sent = sent_texts(&out);
    assert_eq!(sent.len(), 4, "{sent:?}");
    assert!(
        sent.iter().any(|s| s.contains("\"heartbeats\"")),
        "{sent:?}"
    );
    assert!(sent.iter().any(|s| s.contains("\"status\"")), "{sent:?}");
    assert!(
        sent.iter().any(|s| s.contains("\"market_trades\"")),
        "{sent:?}"
    );
    assert!(sent.iter().any(|s| s.contains("\"ticker\"")), "{sent:?}");
    assert!(sent.iter().all(|s| s.contains("BTC-USD")), "{sent:?}");
    assert!(!sent.iter().any(|s| s.contains("\"level2\"")), "{sent:?}");
    assert!(!sent.iter().any(|s| s.contains("\"candles\"")), "{sent:?}");
    assert!(!sent.iter().any(|s| s.contains("matches")), "{sent:?}");
}

#[test]
fn connect_with_l2_includes_level2_channel() {
    let mut s = session_with(Vec::new(), true);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let sent = sent_texts(&out);
    assert!(sent.iter().any(|s| s.contains("\"level2\"")), "{sent:?}");
}

#[test]
fn market_trades_and_ticker_fixtures_exact_fixed() {
    let mut s = session();
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    let trade = r#"{"channel":"market_trades","timestamp":"2023-02-09T20:19:35.39625135Z","sequence_num":0,"events":[{"type":"update","trades":[{"trade_id":"10","product_id":"BTC-USD","price":"400.23","size":"5.23512","side":"SELL","time":"2014-11-07T08:19:27.028459Z"}]}]}"#;
    let m = markets(&drive_text(&mut s, trade, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::Trade(t)
            if t.aggressor == AggressorSide::Buy
                && t.price.0 == Fixed::parse_str("400.23").unwrap()
                && t.quantity.0 == Fixed::parse_str("5.23512").unwrap()
    ));

    let ticker = r#"{"channel":"ticker","timestamp":"2023-02-09T20:30:37.167359596Z","sequence_num":0,"events":[{"type":"snapshot","tickers":[{"type":"ticker","product_id":"BTC-USD","price":"10.01","best_bid":"9.99","best_ask":"10.01","best_bid_quantity":"1.5","best_ask_quantity":"2.25"}]}]}"#;
    let m = markets(&drive_text(&mut s, ticker, 3));
    assert!(matches!(
        &m[0],
        MarketEvent::Quote(q)
            if q.bid_price.0 == Fixed::parse_str("9.99").unwrap()
                && q.ask_price.0 == Fixed::parse_str("10.01").unwrap()
                && q.bid_quantity.as_ref().map(|x| x.0) == Some(Fixed::parse_str("1.5").unwrap())
                && q.ask_quantity.as_ref().map(|x| x.0) == Some(Fixed::parse_str("2.25").unwrap())
    ));

    let ack =
        r#"{"channel":"subscriptions","events":[{"subscriptions":{"market_trades":["BTC-USD"]}}]}"#;
    let buf = drive_text(&mut s, ack, 4);
    assert!(
        buf.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::MarkLive))
    );
}

#[test]
fn l2_snapshot_and_update_exact_fixed() {
    let mut s = session_with(Vec::new(), true);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    let snap = r#"{"channel":"l2_data","timestamp":"2023-02-09T20:32:50.714964855Z","sequence_num":0,"events":[{"type":"snapshot","product_id":"BTC-USD","updates":[{"side":"bid","event_time":"1970-01-01T00:00:00Z","price_level":"101.10","new_quantity":"1.5"},{"side":"bid","event_time":"1970-01-01T00:00:00Z","price_level":"101.00","new_quantity":"2.0"},{"side":"ask","event_time":"1970-01-01T00:00:00Z","price_level":"101.20","new_quantity":"3.0"},{"side":"ask","event_time":"1970-01-01T00:00:00Z","price_level":"101.30","new_quantity":"0.5"}]}]}"#;
    let snap_out = drive_text(&mut s, snap, 2);
    assert!(
        snap_out
            .as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::MarkLive))
    );
    assert!(
        markets(&snap_out)
            .iter()
            .any(|e| matches!(e, MarketEvent::BookSnapshot(_)))
    );

    let upd = r#"{"channel":"l2_data","timestamp":"2023-02-09T20:32:51Z","sequence_num":1,"events":[{"type":"update","product_id":"BTC-USD","updates":[{"side":"bid","event_time":"2014-11-07T08:19:27.028459Z","price_level":"101.10","new_quantity":"0"},{"side":"ask","event_time":"2014-11-07T08:19:27.028459Z","price_level":"101.25","new_quantity":"1.25"}]}]}"#;
    let delta = markets(&drive_text(&mut s, upd, 3))
        .into_iter()
        .find_map(|e| match e {
            MarketEvent::BookDelta(d) => Some(d),
            _ => None,
        })
        .expect("book delta");
    assert_eq!(delta.changes.len(), 2);
    assert_eq!(delta.changes[0].side, BookSide::Bid);
    assert_eq!(delta.changes[0].operation, BookOperation::Delete);
    assert_eq!(
        delta.changes[0].price.0,
        Fixed::parse_str("101.10").unwrap()
    );
    assert_eq!(delta.changes[1].side, BookSide::Ask);
    assert_eq!(delta.changes[1].operation, BookOperation::Upsert);
    assert_eq!(
        delta.changes[1].price.0,
        Fixed::parse_str("101.25").unwrap()
    );
    assert_eq!(
        delta.changes[1].quantity.unwrap().0,
        Fixed::parse_str("1.25").unwrap()
    );
}

#[test]
fn l2_update_commits_atomically_before_cross_validation() {
    let mut session = session_with(Vec::new(), true);
    let snapshot = r#"{"channel":"l2_data","timestamp":"2023-02-09T20:32:50Z","sequence_num":0,"events":[{"type":"snapshot","product_id":"BTC-USD","updates":[{"side":"bid","event_time":"2023-02-09T20:32:50Z","price_level":"100.00","new_quantity":"1.0"},{"side":"ask","event_time":"2023-02-09T20:32:50Z","price_level":"101.00","new_quantity":"1.0"}]}]}"#;
    drive_text(&mut session, snapshot, 1);

    let update = r#"{"channel":"l2_data","timestamp":"2023-02-09T20:32:51Z","sequence_num":1,"events":[{"type":"update","product_id":"BTC-USD","updates":[{"side":"bid","event_time":"2023-02-09T20:32:51Z","price_level":"102.00","new_quantity":"1.0"},{"side":"ask","event_time":"2023-02-09T20:32:51Z","price_level":"101.00","new_quantity":"0"},{"side":"ask","event_time":"2023-02-09T20:32:51Z","price_level":"103.00","new_quantity":"1.0"}]}]}"#;
    let output = drive_text(&mut session, update, 2);

    assert!(
        markets(&output).iter().any(
            |event| matches!(event, MarketEvent::BookDelta(delta) if delta.changes.len() == 3)
        )
    );
    assert!(!output.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(marketfeed_model::SystemEvent::BookInvalidated { .. })
    )));
}

#[test]
fn status_channel_emits_instrument_update() {
    let mut s = session();
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    let status = r#"{"channel":"status","timestamp":"2023-02-09T20:29:49.753424311Z","sequence_num":0,"events":[{"type":"snapshot","products":[{"product_type":"SPOT","id":"BTC-USD","status":"online"}]}]}"#;
    let m = markets(&drive_text(&mut s, status, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::InstrumentUpdate(u) if u.status == InstrumentStatus::Active
    ));
}

#[test]
fn ws_candles_decode_emits_without_subscribe() {
    let mut s = session();
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    assert!(!sent_texts(&out).iter().any(|t| t.contains("\"candles\"")));

    let candle = r#"{"channel":"candles","timestamp":"2023-06-09T20:19:35.39625135Z","sequence_num":0,"events":[{"type":"snapshot","candles":[{"start":"1688998200","high":"1867.72","low":"1865.63","open":"1867.38","close":"1866.81","volume":"0.20269406","product_id":"BTC-USD"}]}]}"#;
    let m = markets(&drive_text(&mut s, candle, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::Candle(c)
            if c.interval_ns == 300_000_000_000
                && c.open.0 == Fixed::parse_str("1867.38").unwrap()
    ));
}

#[test]
fn candles_rest_timer_fixture_exact_fixed() {
    let mut s = session_with(vec![CandleInterval::M1], false);
    let mut out = ActionBuffer::new();
    let now = TimestampNs(1_609_460_000_000_000_000);
    s.on_input(SessionInput::Connected { now }, &mut out)
        .unwrap();
    let (req_id, url) = http_ids(&out)
        .into_iter()
        .find(|(_, u)| u.contains("/candles"))
        .expect("candle request");
    assert!(
        url.contains("/market/products/BTC-USD/candles") && url.contains("granularity=ONE_MINUTE"),
        "{url}"
    );
    assert!(url.contains("start=") && url.contains("end="), "{url}");
    assert!(out.as_slice().iter().any(|a| matches!(
        a, SessionAction::ScheduleTimer(t) if t.timer_id == ADV_CANDLE_TIMER_ID
            && t.fire_at.0 == now.0 + ADV_CANDLE_POLL_INTERVAL_MS * 1_000_000
    )));

    let mut candle_out = ActionBuffer::new();
    s.on_input(
        SessionInput::HttpResponse {
            request_id: req_id,
            response: &HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(
                    br#"{"candles":[{"start":"1609459980","low":"28800","high":"28902.46","open":"28901.57","close":"28800.01","volume":"49.3149836"}]}"#,
                ),
            },
            received: stamp(7),
        },
        &mut candle_out,
    )
    .unwrap();
    let events = markets(&candle_out);
    assert!(matches!(
        &events[0],
        MarketEvent::Candle(c)
            if c.open.0 == Fixed::parse_str("28901.57").unwrap()
                && c.high.0 == Fixed::parse_str("28902.46").unwrap()
                && c.low.0 == Fixed::parse_str("28800").unwrap()
                && c.close.0 == Fixed::parse_str("28800.01").unwrap()
                && c.volume.0 == Fixed::parse_str("49.3149836").unwrap()
                && c.interval_ns == 60_000_000_000
                && c.start_ts == TimestampNs(1_609_459_980_000_000_000)
    ));
    let venue = candle_out.as_slice().iter().find_map(|a| match a {
        SessionAction::EmitBatch(EventBatch { events, .. }) => events.first().map(|e| e.venue),
        _ => None,
    });
    assert_eq!(venue, Some(VenueId(18)));

    let fire_at = TimestampNs(now.0 + ADV_CANDLE_POLL_INTERVAL_MS * 1_000_000);
    let mut tick = ActionBuffer::new();
    s.on_input(
        SessionInput::Timer {
            timer_id: ADV_CANDLE_TIMER_ID,
            now: fire_at,
        },
        &mut tick,
    )
    .unwrap();
    assert!(http_ids(&tick).iter().any(|(_, u)| u.contains("/candles")));
    assert!(tick.as_slice().iter().any(|a| matches!(
        a, SessionAction::ScheduleTimer(t) if t.timer_id == ADV_CANDLE_TIMER_ID
            && t.fire_at.0 == fire_at.0 + ADV_CANDLE_POLL_INTERVAL_MS * 1_000_000
    )));
}
