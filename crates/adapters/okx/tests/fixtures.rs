//! Fixture-driven OKX Spot/SWAP/Futures decode + session tests (offline).

use marketfeed_adapter_api::{
    ActionBuffer, ConcreteSubscriptionSet, EventBatch, SessionAction, SessionInput, SessionMachine,
    SessionSpec,
};
use marketfeed_adapter_okx::{OKX_SWAP_VENUE_ID, OkxSession, OkxSessionConfig};
use marketfeed_model::{
    AggressorSide, CatalogVersion, CatalogView, Fixed, FrameStamp, InstrumentId, MarketEvent,
    SessionId, TimestampNs, VenueId,
};
use std::collections::HashMap;

fn stamp(n: i64) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(n),
        mono_ns: n as u64,
    }
}

fn session(enable_l2: bool) -> OkxSession {
    let mut ids = HashMap::new();
    ids.insert("BTC-USDT".into(), InstrumentId(1));
    OkxSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(4), CatalogVersion(1)),
        OkxSessionConfig {
            symbols: vec!["BTC-USDT".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2,
            ..OkxSessionConfig::default()
        },
    )
}

/// SWAP-flavored session: derivative venue id, mark/index/funding subscribed.
fn swap_session(symbol: &str, enable_l2: bool) -> OkxSession {
    let mut ids = HashMap::new();
    ids.insert(symbol.to_string(), InstrumentId(1));
    OkxSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(OKX_SWAP_VENUE_ID, CatalogVersion(1)),
        OkxSessionConfig {
            symbols: vec![symbol.to_string()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2,
            venue: OKX_SWAP_VENUE_ID,
            subscribe_mark_funding: true,
            ..OkxSessionConfig::default()
        },
    )
}

fn drive_text(s: &mut OkxSession, text: &str, ts: i64) -> ActionBuffer {
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

fn connect(s: &mut OkxSession) -> ActionBuffer {
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    out
}

#[test]
fn connect_subscribes_trades_and_tickers() {
    let mut s = session(false);
    let out = connect(&mut s);
    let send = out
        .as_slice()
        .iter()
        .find_map(|a| match a {
            SessionAction::SendText(b) => Some(std::str::from_utf8(b).unwrap().to_string()),
            _ => None,
        })
        .expect("subscribe");
    assert!(send.contains("\"channel\":\"trades\""));
    assert!(send.contains("\"channel\":\"tickers\""));
    assert!(!send.contains("\"channel\":\"books\""));
    assert!(!send.contains("\"channel\":\"mark-price\""));
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::MarkLive))
    );
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::ScheduleTimer(_)))
    );
}

#[test]
fn swap_connect_subscribes_derivative_channels() {
    let mut s = swap_session("BTC-USDT-SWAP", false);
    let out = connect(&mut s);
    let send = out
        .as_slice()
        .iter()
        .find_map(|a| match a {
            SessionAction::SendText(b) => Some(std::str::from_utf8(b).unwrap().to_string()),
            _ => None,
        })
        .expect("subscribe");
    assert!(send.contains(r#""channel":"mark-price","instId":"BTC-USDT-SWAP""#));
    // Index instId is the underlying pair, not the SWAP symbol.
    assert!(send.contains(r#""channel":"index-tickers","instId":"BTC-USDT""#));
    assert!(send.contains(r#""channel":"funding-rate","instId":"BTC-USDT-SWAP""#));
    assert!(send.contains(r#""channel":"open-interest","instId":"BTC-USDT-SWAP""#));
    assert!(send.contains(r#""channel":"liquidation-orders","instType":"SWAP""#));
}

#[test]
fn trade_and_quote_fixtures() {
    let mut s = session(false);
    connect(&mut s);

    let trade = r#"{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"42","px":"65000.1","sz":"0.001","side":"buy","ts":"1001","seqId":7}]}"#;
    let m = markets(&drive_text(&mut s, trade, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::Trade(t) if t.aggressor == AggressorSide::Buy
    ));

    let quote = r#"{"arg":{"channel":"tickers","instId":"BTC-USDT"},"data":[{"instType":"SPOT","instId":"BTC-USDT","last":"65000.1","lastSz":"0.1","askPx":"65000.2","askSz":"0.8","bidPx":"65000.0","bidSz":"1.2","open24h":"64000.0","high24h":"66000.0","low24h":"63000.0","volCcy24h":"2500000.0","vol24h":"38.5","sodUtc0":"0","sodUtc8":"0","ts":"1002"}]}"#;
    let m = markets(&drive_text(&mut s, quote, 3));
    assert_eq!(m.len(), 2);
    assert!(matches!(&m[0], MarketEvent::Quote(_)));
    assert!(matches!(
        &m[1],
        MarketEvent::Statistics24h(st)
            if st.open.as_ref().unwrap().0 == marketfeed_model::Fixed::parse_str("64000.0").unwrap()
                && st.high.as_ref().unwrap().0 == marketfeed_model::Fixed::parse_str("66000.0").unwrap()
                && st.low.as_ref().unwrap().0 == marketfeed_model::Fixed::parse_str("63000.0").unwrap()
                && st.close.as_ref().unwrap().0 == marketfeed_model::Fixed::parse_str("65000.1").unwrap()
                && st.volume.as_ref().unwrap().0 == marketfeed_model::Fixed::parse_str("38.5").unwrap()
                && st.quote_volume.as_ref().unwrap().0
                    == marketfeed_model::Fixed::parse_str("2500000.0").unwrap()
    ));
}

#[test]
fn tickers_stats24h_fixture_exact_fixed() {
    use marketfeed_model::Fixed;

    let mut s = session(false);
    connect(&mut s);

    let ticker = r#"{"arg":{"channel":"tickers","instId":"BTC-USDT"},"data":[{"instType":"SPOT","instId":"BTC-USDT","last":"65000.12","lastSz":"0.1","askPx":"65000.20","askSz":"0.8","bidPx":"65000.00","bidSz":"1.2","open24h":"64000.00","high24h":"66000.50","low24h":"63000.25","volCcy24h":"812500.00","vol24h":"12.5","sodUtc0":"0","sodUtc8":"0","ts":"1002"}]}"#;
    let m = markets(&drive_text(&mut s, ticker, 2));
    assert_eq!(m.len(), 2);
    let MarketEvent::Quote(q) = &m[0] else {
        panic!("expected Quote");
    };
    assert_eq!(q.bid_price.0, Fixed::new(6500000, 2));
    assert_eq!(q.ask_price.0, Fixed::new(6500020, 2));
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
fn ping_replies_pong() {
    let mut s = session(false);
    let buf = drive_text(&mut s, "ping", 1);
    assert!(buf.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::SendText(b) if b.as_ref() == b"pong"
    )));
}

#[test]
fn books_snapshot_delta_and_gap() {
    let mut s = session(true);
    let out = connect(&mut s);
    let send = out
        .as_slice()
        .iter()
        .find_map(|a| match a {
            SessionAction::SendText(b) => Some(std::str::from_utf8(b).unwrap().to_string()),
            _ => None,
        })
        .unwrap();
    assert!(send.contains("\"channel\":\"books\""));

    let snap = r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[["101.0","1.5","0","1"]],"bids":[["100.0","1.0","0","1"],["99.0","2.0","0","1"]],"ts":"10","checksum":0,"prevSeqId":-1,"seqId":100}]}"#;
    let m = markets(&drive_text(&mut s, snap, 2));
    assert!(matches!(&m[0], MarketEvent::BookSnapshot(_)));

    let delta = r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[{"asks":[],"bids":[["100.0","1.5","0","1"]],"ts":"11","checksum":0,"prevSeqId":100,"seqId":101}]}"#;
    let m = markets(&drive_text(&mut s, delta, 3));
    assert!(matches!(&m[0], MarketEvent::BookDelta(_)));

    let gap = r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[{"asks":[],"bids":[["100.0","2","0","1"]],"ts":"12","checksum":0,"prevSeqId":200,"seqId":201}]}"#;
    let buf = drive_text(&mut s, gap, 4);
    assert!(buf.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::Reconnect(marketfeed_adapter_api::ReconnectReason::SequenceGap)
    )));
}

#[test]
fn books_multi_level_update_validates_only_the_final_book() {
    let mut session = session(true);
    connect(&mut session);
    drive_text(
        &mut session,
        r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[["101.0","1.0","0","1"]],"bids":[["100.0","1.0","0","1"]],"ts":"10","checksum":0,"prevSeqId":-1,"seqId":100}]}"#,
        1,
    );

    let output = drive_text(
        &mut session,
        r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[{"asks":[["101.0","0","0","1"],["103.0","1.0","0","1"]],"bids":[["102.0","1.0","0","1"]],"ts":"11","checksum":0,"prevSeqId":100,"seqId":101}]}"#,
        2,
    );

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
fn books_duplicate_prev_seq_is_gap() {
    // After seqId=101, a second update still claiming prevSeqId=100 is a gap.
    let mut s = session(true);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let snap = r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[["101.0","1.5","0","1"]],"bids":[["100.0","1.0","0","1"]],"ts":"10","checksum":0,"prevSeqId":-1,"seqId":100}]}"#;
    drive_text(&mut s, snap, 2);
    let delta = r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[{"asks":[],"bids":[["100.0","1.5","0","1"]],"ts":"11","checksum":0,"prevSeqId":100,"seqId":101}]}"#;
    drive_text(&mut s, delta, 3);
    let dup = r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[{"asks":[],"bids":[["100.0","2","0","1"]],"ts":"12","checksum":0,"prevSeqId":100,"seqId":101}]}"#;
    let buf = drive_text(&mut s, dup, 4);
    assert!(buf.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::Reconnect(marketfeed_adapter_api::ReconnectReason::SequenceGap)
    )));
}

#[test]
fn books_nonzero_checksum_reconnects() {
    let mut s = session(true);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let snap = r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[["101.0","1.5","0","1"]],"bids":[["100.0","1.0","0","1"]],"ts":"10","checksum":12345,"prevSeqId":-1,"seqId":100}]}"#;
    let buf = drive_text(&mut s, snap, 2);
    assert!(buf.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::EmitSystem(marketfeed_model::SystemEvent::ChecksumMismatch { .. })
    )));
    assert!(buf.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::Reconnect(marketfeed_adapter_api::ReconnectReason::ChecksumMismatch)
    )));
}

#[test]
fn client_ping_timer_sends_ping_and_reschedules() {
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
            timer_id: marketfeed_adapter_okx::PING_TIMER_ID,
            now: TimestampNs(20_000_000_000),
        },
        &mut out,
    )
    .unwrap();
    assert!(out.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::SendText(b) if b.as_ref() == b"ping"
    )));
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::ScheduleTimer(_))),
        "timer fire reschedules next ping"
    );
}

#[test]
fn record_replay_okx_trade_quote_identical() {
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
        r#"{"id":"1","event":"subscribe","arg":{"channel":"trades","instId":"BTC-USDT"},"connId":"x"}"#,
        r#"{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"1","px":"1.0","sz":"1","side":"sell","ts":"1","seqId":1}]}"#,
        r#"{"arg":{"channel":"tickers","instId":"BTC-USDT"},"data":[{"instType":"SPOT","instId":"BTC-USDT","last":"1.0","lastSz":"1","askPx":"1.1","askSz":"1","bidPx":"1.0","bidSz":"1","open24h":"0","high24h":"0","low24h":"0","volCcy24h":"0","vol24h":"0","sodUtc0":"0","sodUtc8":"0","ts":"2"}]}"#,
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

// -- Raw-JSON fixtures under tests/fixtures/ -------------------------------
//
// These exercise real (de-identified) OKX wire payloads rather than inline
// strings, covering the protocol shapes goal 4 asks for: unknown message,
// derivatives mark/index/funding, a SWAP trade, and an L2 snapshot+gap.

#[test]
fn unknown_message_is_reported() {
    let mut s = session(false);
    connect(&mut s);
    let raw = include_str!("fixtures/unknown_message.json");
    let buf = drive_text(&mut s, raw, 2);
    assert!(buf.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::EmitSystem(marketfeed_model::SystemEvent::UnknownMessage { .. })
    )));
}

#[test]
fn candle1m_fixture_exact_fixed() {
    use marketfeed_adapter_api::CandleInterval;
    use marketfeed_model::Fixed;

    let mut ids = HashMap::new();
    ids.insert("BTC-USDT".into(), InstrumentId(1));
    let mut s = OkxSession::new(
        SessionSpec {
            endpoint_name: "wss://ws.okx.com:8443/ws/v5/business".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(4), CatalogVersion(1)),
        OkxSessionConfig {
            symbols: vec!["BTC-USDT".into()],
            instrument_ids: ids,
            session: SessionId(1),
            candle_intervals: vec![CandleInterval::M1],
            ..OkxSessionConfig::default()
        },
    );
    let out = connect(&mut s);
    let sub = out
        .as_slice()
        .iter()
        .find_map(|a| match a {
            SessionAction::SendText(b) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        })
        .expect("subscribe");
    assert!(sub.contains("candle1m"), "subscribe={sub}");

    let raw = include_str!("fixtures/candle1m.json");
    let m = markets(&drive_text(&mut s, raw, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::Candle(c)
            if c.open.0 == Fixed::new(3721, 3)
                && c.high.0 == Fixed::new(3743, 3)
                && c.low.0 == Fixed::new(3677, 3)
                && c.close.0 == Fixed::new(3708, 3)
                && c.volume.0 == Fixed::new(8422410, 0)
                && c.interval_ns == 60_000_000_000
                && c.start_ts == TimestampNs(1_597_026_383_085_000_000)
    ));
}

#[test]
fn business_candle_session_does_not_subscribe_public_channels() {
    use marketfeed_adapter_api::CandleInterval;

    let mut s = OkxSession::new(
        SessionSpec {
            endpoint_name: "wss://ws.okx.com:8443/ws/v5/business".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(4), CatalogVersion(1)),
        OkxSessionConfig {
            symbols: vec!["BTC-USDT".into()],
            candle_intervals: vec![CandleInterval::M1],
            ..OkxSessionConfig::default()
        },
    );

    let connected = connect(&mut s);
    let subscribe = connected
        .as_slice()
        .iter()
        .find_map(|action| match action {
            SessionAction::SendText(payload) => Some(String::from_utf8_lossy(payload).into_owned()),
            _ => None,
        })
        .expect("subscribe frame");

    assert!(subscribe.contains("candle1m"), "{subscribe}");
    assert!(!subscribe.contains("\"channel\":\"trades\""), "{subscribe}");
    assert!(
        !subscribe.contains("\"channel\":\"tickers\""),
        "{subscribe}"
    );
}

#[test]
fn swap_candle1m_fixture_exact_fixed() {
    use marketfeed_adapter_api::CandleInterval;
    use marketfeed_model::Fixed;

    let mut ids = HashMap::new();
    ids.insert("BTC-USDT-SWAP".into(), InstrumentId(1));
    let mut s = OkxSession::new(
        SessionSpec {
            endpoint_name: "wss://ws.okx.com:8443/ws/v5/business".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(OKX_SWAP_VENUE_ID, CatalogVersion(1)),
        OkxSessionConfig {
            symbols: vec!["BTC-USDT-SWAP".into()],
            instrument_ids: ids,
            session: SessionId(1),
            venue: OKX_SWAP_VENUE_ID,
            subscribe_mark_funding: true,
            candle_intervals: vec![CandleInterval::M1],
            ..OkxSessionConfig::default()
        },
    );
    let out = connect(&mut s);
    let sub = out
        .as_slice()
        .iter()
        .find_map(|a| match a {
            SessionAction::SendText(b) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        })
        .expect("subscribe");
    assert!(
        sub.contains(r#""channel":"candle1m","instId":"BTC-USDT-SWAP""#)
            || (sub.contains("candle1m") && sub.contains("BTC-USDT-SWAP")),
        "subscribe={sub}"
    );

    let raw = include_str!("fixtures/candle1m_swap.json");
    let m = markets(&drive_text(&mut s, raw, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::Candle(c)
            if c.open.0 == Fixed::parse_str("65010.1").unwrap()
                && c.high.0 == Fixed::parse_str("65020.5").unwrap()
                && c.low.0 == Fixed::parse_str("65000.0").unwrap()
                && c.close.0 == Fixed::parse_str("65015.2").unwrap()
                && c.volume.0 == Fixed::new(1234, 0)
                && c.interval_ns == 60_000_000_000
                && c.start_ts == TimestampNs(1_700_000_000_000_000_000)
    ));
}

#[test]
fn swap_mark_index_and_funding_fixtures() {
    let mut s = swap_session("BTC-USDT-SWAP", false);
    connect(&mut s);

    let mark = include_str!("fixtures/mark_price.json");
    let m = markets(&drive_text(&mut s, mark, 2));
    assert!(matches!(&m[0], MarketEvent::MarkPrice(p) if p.price.0.coefficient == 650124));

    let index = include_str!("fixtures/index_tickers.json");
    let m = markets(&drive_text(&mut s, index, 3));
    // Index instId ("BTC-USDT") maps back to the SWAP instrument via the
    // underlying-pair heuristic, so it still carries an instrument id.
    assert!(matches!(&m[0], MarketEvent::IndexPrice(_)));

    let funding = include_str!("fixtures/funding_rate.json");
    let m = markets(&drive_text(&mut s, funding, 4));
    assert!(matches!(
        &m[0],
        MarketEvent::Funding(f) if f.next_funding_ts == Some(TimestampNs(1_700_028_800_000_000_000))
    ));

    let oi = include_str!("fixtures/open_interest.json");
    let m = markets(&drive_text(&mut s, oi, 5));
    assert!(matches!(&m[0], MarketEvent::OpenInterest(_)));

    let liq = include_str!("fixtures/liquidation_orders.json");
    let m = markets(&drive_text(&mut s, liq, 6));
    assert!(matches!(
        &m[0],
        MarketEvent::Liquidation(l)
            if l.side == AggressorSide::Buy
                && l.price.0 == Fixed::new(235239, 1)
                && l.quantity.0 == Fixed::new(1, 2)
    ));
}

#[test]
fn swap_trade_fixture_decodes_as_trade() {
    let mut s = swap_session("BTC-USDT-SWAP", false);
    connect(&mut s);
    let raw = include_str!("fixtures/swap_trade.json");
    let m = markets(&drive_text(&mut s, raw, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::Trade(t) if t.aggressor == AggressorSide::Buy
    ));
}

#[test]
fn l2_snapshot_then_gap_fixtures_trigger_resync() {
    let mut s = session(true);
    connect(&mut s);

    let snap = include_str!("fixtures/l2_snapshot.json");
    let m = markets(&drive_text(&mut s, snap, 2));
    assert!(matches!(&m[0], MarketEvent::BookSnapshot(_)));

    // prevSeqId=1500 does not follow snapshot's seqId=1000 -> gap.
    let gap = include_str!("fixtures/l2_update_gap.json");
    let buf = drive_text(&mut s, gap, 3);
    assert!(buf.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::Reconnect(marketfeed_adapter_api::ReconnectReason::SequenceGap)
    )));
    assert!(buf.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::EmitSystem(marketfeed_model::SystemEvent::BookInvalidated { .. })
    )));
}
