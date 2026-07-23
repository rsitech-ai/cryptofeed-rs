//! Fixture-driven Bybit decode + session tests (offline).

use marketfeed_adapter_api::{
    ActionBuffer, ConcreteSubscriptionSet, EventBatch, SessionAction, SessionInput, SessionMachine,
    SessionSpec,
};
use marketfeed_adapter_bybit::{BybitCategory, BybitSession, BybitSessionConfig};
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

fn session(enable_l2: bool) -> BybitSession {
    category_session(BybitCategory::Linear, VenueId(5), "BTCUSDT", enable_l2)
}

fn category_session(
    category: BybitCategory,
    venue: VenueId,
    symbol: &str,
    enable_l2: bool,
) -> BybitSession {
    let mut ids = HashMap::new();
    ids.insert(symbol.to_string(), InstrumentId(1));
    BybitSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(venue, CatalogVersion(1)),
        BybitSessionConfig {
            category,
            symbols: vec![symbol.to_string()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2,
            ..BybitSessionConfig::default()
        },
    )
}

fn drive_text(s: &mut BybitSession, text: &str, ts: i64) -> ActionBuffer {
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
    // Proves ScheduleTimer(ping) is emitted on connect. Engine timer *fulfillment* (actually
    // Engine delivers SessionInput::Timer on live connections (PR #10 on main).
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::ScheduleTimer(_)))
    );
    let sub = out
        .as_slice()
        .iter()
        .find_map(|a| match a {
            SessionAction::SendText(b) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        })
        .expect("subscribe");
    assert!(
        sub.contains("tickers.BTCUSDT"),
        "linear must subscribe tickers: {sub}"
    );
    assert!(
        sub.contains("allLiquidation.BTCUSDT"),
        "linear must subscribe allLiquidation: {sub}"
    );

    let trade = r#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1000,"data":[{"T":1001,"s":"BTCUSDT","S":"Buy","v":"0.001","p":"65000.12","L":"PlusTick","i":"t-42","seq":7}]}"#;
    let m = markets(&drive_text(&mut s, trade, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::Trade(t) if t.aggressor == AggressorSide::Buy
    ));

    let quote = r#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":3,"data":{"s":"BTCUSDT","b":[["65000.00","1.2"]],"a":[["65000.10","0.8"]],"u":9,"seq":90}}"#;
    let m = markets(&drive_text(&mut s, quote, 3));
    assert!(matches!(&m[0], MarketEvent::Quote(_)));
}

#[test]
fn spot_trade_and_quote_fixtures() {
    let mut s = category_session(BybitCategory::Spot, VenueId(6), "BTCUSDT", false);
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
    assert!(
        sub.contains("tickers.BTCUSDT"),
        "spot must subscribe tickers: {sub}"
    );

    let trade = r#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1000,"data":[{"T":1001,"s":"BTCUSDT","S":"Buy","v":"0.01","p":"65000.00","L":"PlusTick","i":"spot-1","seq":1}]}"#;
    let m = markets(&drive_text(&mut s, trade, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::Trade(t) if t.aggressor == AggressorSide::Buy
    ));

    let quote = r#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":3,"data":{"s":"BTCUSDT","b":[["65000.00","1.0"]],"a":[["65000.10","1.0"]],"u":1,"seq":1}}"#;
    let m = markets(&drive_text(&mut s, quote, 3));
    assert!(matches!(&m[0], MarketEvent::Quote(_)));
}

#[test]
fn inverse_trade_fixture() {
    let mut s = category_session(BybitCategory::Inverse, VenueId(11), "BTCUSD", false);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    let trade = r#"{"topic":"publicTrade.BTCUSD","type":"snapshot","ts":1000,"data":[{"T":1001,"s":"BTCUSD","S":"Sell","v":"100","p":"65000.5","L":"MinusTick","i":"inv-1","seq":1}]}"#;
    let m = markets(&drive_text(&mut s, trade, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::Trade(t) if t.aggressor == AggressorSide::Sell
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

    let snap = r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100.00","1.0"],["99.00","2.0"]],"a":[["101.00","1.5"]],"u":100,"seq":1000}}"#;
    let m = markets(&drive_text(&mut s, snap, 2));
    assert!(matches!(&m[0], MarketEvent::BookSnapshot(_)));

    let delta = r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":2,"data":{"s":"BTCUSDT","b":[["100.00","1.5"]],"a":[],"u":101,"seq":1001}}"#;
    let m = markets(&drive_text(&mut s, delta, 3));
    assert!(matches!(&m[0], MarketEvent::BookDelta(_)));

    let gap = r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":3,"data":{"s":"BTCUSDT","b":[["100.00","2"]],"a":[],"u":200,"seq":1100}}"#;
    let buf = drive_text(&mut s, gap, 4);
    assert!(buf.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::Reconnect(marketfeed_adapter_api::ReconnectReason::SequenceGap)
    )));
}

#[test]
fn ping_timer_sends_ping() {
    let mut s = session(false);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    out.clear();
    s.on_input(
        SessionInput::Timer {
            timer_id: 1,
            now: TimestampNs(20_000_000_001),
        },
        &mut out,
    )
    .unwrap();
    assert!(out.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::SendText(b) if b.as_ref() == br#"{"op":"ping"}"#
    )));
}

#[test]
fn record_replay_bybit_trade_quote_identical() {
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
        r#"{"success":true,"ret_msg":"","op":"subscribe"}"#,
        r#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1,"data":[{"T":1,"s":"BTCUSDT","S":"Sell","v":"1","p":"1.00","L":"MinusTick","i":"t1","seq":1}]}"#,
        r#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":2,"data":{"s":"BTCUSDT","b":[["1.00","1"]],"a":[["1.01","1"]],"u":1,"seq":1}}"#,
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
        // SessionRunner injects VenueStatus; ReplayRunner does not.
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

#[test]
fn kline_candle_fixture_exact_fixed() {
    use marketfeed_adapter_api::CandleInterval;
    use marketfeed_model::Fixed;

    let mut ids = HashMap::new();
    ids.insert("BTCUSDT".into(), InstrumentId(1));
    let mut s = BybitSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(5), CatalogVersion(1)),
        BybitSessionConfig {
            category: BybitCategory::Linear,
            symbols: vec!["BTCUSDT".into()],
            instrument_ids: ids,
            session: SessionId(1),
            candle_intervals: vec![CandleInterval::M1],
            ..BybitSessionConfig::default()
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
    assert!(sub.contains("kline.1.BTCUSDT"), "sub={sub}");

    let raw = r#"{"topic":"kline.1.BTCUSDT","type":"snapshot","ts":1672324988887,"data":[{"start":1672324800000,"end":1672324859999,"interval":"1","open":"16649.5","close":"16695","high":"16699","low":"16642","volume":"2.081","turnover":"34666.4005","confirm":true,"timestamp":1672324859999}]}"#;
    let m = markets(&drive_text(&mut s, raw, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::Candle(c)
            if c.open.0 == Fixed::new(166495, 1)
                && c.high.0 == Fixed::new(16699, 0)
                && c.low.0 == Fixed::new(16642, 0)
                && c.close.0 == Fixed::new(16695, 0)
                && c.volume.0 == Fixed::new(2081, 3)
                && c.interval_ns == 60_000_000_000
                && c.start_ts == TimestampNs(1_672_324_800_000_000_000)
    ));
}

#[test]
fn linear_tickers_mark_funding_oi_fixture() {
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

    let raw = r#"{"topic":"tickers.BTCUSDT","type":"snapshot","ts":1672376495650,"data":{"symbol":"BTCUSDT","lastPrice":"16595.00","prevPrice24h":"16000.00","highPrice24h":"17000.00","lowPrice24h":"15500.00","volume24h":"1234.5","turnover24h":"20000000.0","markPrice":"16595.00","indexPrice":"16596.54","fundingRate":"0.0001","nextFundingTime":"1672387200000","openInterest":"458153.0"}}"#;
    let m = markets(&drive_text(&mut s, raw, 2));
    assert_eq!(m.len(), 5);
    assert!(matches!(&m[0], MarketEvent::MarkPrice(p) if p.price.0 == Fixed::new(1659500, 2)));
    assert!(matches!(&m[1], MarketEvent::IndexPrice(p) if p.price.0 == Fixed::new(1659654, 2)));
    assert!(matches!(
        &m[2],
        MarketEvent::Funding(f)
            if f.rate.0 == Fixed::new(1, 4)
                && f.next_funding_ts == Some(TimestampNs(1_672_387_200_000_000_000))
    ));
    assert!(matches!(
        &m[3],
        MarketEvent::OpenInterest(oi) if oi.quantity.0 == Fixed::new(4581530, 1)
    ));
    assert!(matches!(
        &m[4],
        MarketEvent::Statistics24h(st)
            if st.open.as_ref().unwrap().0 == Fixed::parse_str("16000.00").unwrap()
                && st.high.as_ref().unwrap().0 == Fixed::parse_str("17000.00").unwrap()
                && st.low.as_ref().unwrap().0 == Fixed::parse_str("15500.00").unwrap()
                && st.close.as_ref().unwrap().0 == Fixed::parse_str("16595.00").unwrap()
                && st.volume.as_ref().unwrap().0 == Fixed::parse_str("1234.5").unwrap()
                && st.quote_volume.as_ref().unwrap().0 == Fixed::parse_str("20000000.0").unwrap()
    ));
}

#[test]
fn linear_all_liquidation_fixture() {
    use marketfeed_model::{AggressorSide, Fixed};

    let mut s = session(false);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    let raw = r#"{"topic":"allLiquidation.BTCUSDT","type":"snapshot","ts":1739502303204,"data":[{"T":1739502302929,"s":"BTCUSDT","S":"Sell","v":"0.01","p":"65000.5"}]}"#;
    let m = markets(&drive_text(&mut s, raw, 2));
    assert_eq!(m.len(), 1);
    assert!(matches!(
        &m[0],
        MarketEvent::Liquidation(l)
            if l.side == AggressorSide::Buy
                && l.price.0 == Fixed::new(650005, 1)
                && l.quantity.0 == Fixed::new(1, 2)
    ));
}

#[test]
fn inverse_tickers_and_spot_candle_subscribe() {
    use marketfeed_adapter_api::CandleInterval;

    let mut inv = category_session(BybitCategory::Inverse, VenueId(11), "BTCUSD", false);
    let mut out = ActionBuffer::new();
    inv.on_input(
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
    assert!(sub.contains("tickers.BTCUSD"), "inverse tickers: {sub}");
    assert!(
        sub.contains("allLiquidation.BTCUSD"),
        "inverse allLiquidation: {sub}"
    );

    let mut ids = HashMap::new();
    ids.insert("BTCUSDT".into(), InstrumentId(1));
    let mut spot = BybitSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(6), CatalogVersion(1)),
        BybitSessionConfig {
            category: BybitCategory::Spot,
            symbols: vec!["BTCUSDT".into()],
            instrument_ids: ids,
            session: SessionId(1),
            candle_intervals: vec![CandleInterval::M1],
            ..BybitSessionConfig::default()
        },
    );
    let mut out = ActionBuffer::new();
    spot.on_input(
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
    assert!(sub.contains("kline.1.BTCUSDT"), "spot candles: {sub}");
    assert!(sub.contains("tickers.BTCUSDT"), "spot tickers: {sub}");
}

#[test]
fn spot_tickers_stats24h_fixture() {
    use marketfeed_model::Fixed;

    let mut s = category_session(BybitCategory::Spot, VenueId(6), "BTCUSDT", false);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    let raw = r#"{"topic":"tickers.BTCUSDT","type":"snapshot","ts":1672376495650,"data":{"symbol":"BTCUSDT","lastPrice":"16595.00","prevPrice24h":"16000.00","highPrice24h":"17000.00","lowPrice24h":"15500.00","volume24h":"1234.5","turnover24h":"20000000.0"}}"#;
    let m = markets(&drive_text(&mut s, raw, 2));
    assert_eq!(m.len(), 1);
    assert!(matches!(
        &m[0],
        MarketEvent::Statistics24h(st)
            if st.open.as_ref().unwrap().0 == Fixed::parse_str("16000.00").unwrap()
                && st.high.as_ref().unwrap().0 == Fixed::parse_str("17000.00").unwrap()
                && st.low.as_ref().unwrap().0 == Fixed::parse_str("15500.00").unwrap()
                && st.close.as_ref().unwrap().0 == Fixed::parse_str("16595.00").unwrap()
                && st.volume.as_ref().unwrap().0 == Fixed::parse_str("1234.5").unwrap()
                && st.quote_volume.as_ref().unwrap().0 == Fixed::parse_str("20000000.0").unwrap()
    ));
}
