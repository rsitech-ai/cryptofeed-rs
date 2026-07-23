//! Fixture-driven Deribit decode + session tests (offline).

use marketfeed_adapter_api::{
    ActionBuffer, ConcreteSubscriptionSet, EventBatch, SessionAction, SessionInput, SessionMachine,
    SessionSpec,
};
use marketfeed_adapter_deribit::{DeribitSession, DeribitSessionConfig};
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

fn session() -> DeribitSession {
    let mut ids = HashMap::new();
    ids.insert("BTC-PERPETUAL".into(), InstrumentId(1));
    DeribitSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(8), CatalogVersion(1)),
        DeribitSessionConfig {
            instruments: vec!["BTC-PERPETUAL".into()],
            instrument_ids: ids,
            session: SessionId(1),
            ..DeribitSessionConfig::default()
        },
    )
}

fn drive_text(s: &mut DeribitSession, text: &str, ts: i64) -> ActionBuffer {
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
fn trade_ticker_and_heartbeat_fixtures() {
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
    let sub = sends
        .iter()
        .find_map(|a| match a {
            SessionAction::SendText(b) => {
                let s = std::str::from_utf8(b).ok()?;
                s.contains("public/subscribe").then(|| s.to_string())
            }
            _ => None,
        })
        .expect("public/subscribe");
    assert!(
        sub.contains("trades.BTC-PERPETUAL.100ms"),
        "public trades must use .100ms, not auth-only .raw: {sub}"
    );
    assert!(
        !sub.contains("trades.BTC-PERPETUAL.raw"),
        "public trades must not use .raw (Deribit 13778): {sub}"
    );
    assert!(
        sub.contains("deribit_price_index.btc_usd"),
        "dedicated index stream (peer OKX index-tickers): {sub}"
    );

    let trade = r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.BTC-PERPETUAL.100ms","data":[{"trade_seq":9,"trade_id":"555","timestamp":1623060194301,"price":36457.5,"amount":10,"direction":"sell","instrument_name":"BTC-PERPETUAL"}]}}"#;
    let m = markets(&drive_text(&mut s, trade, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::Trade(t) if t.aggressor == AggressorSide::Sell
    ));

    let ticker = r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"ticker.BTC-PERPETUAL.100ms","data":{"timestamp":1623060194301,"instrument_name":"BTC-PERPETUAL","best_bid_price":36442.5,"best_bid_amount":5000,"best_ask_price":36443,"best_ask_amount":100,"mark_price":36446.51,"index_price":36441.64,"funding_8h":0.0000211,"open_interest":502097590,"last_price":36450.0,"stats":{"high":37000.0,"low":35000.0,"volume":1234.5,"volume_usd":45000000.0}}}}"#;
    let m = markets(&drive_text(&mut s, ticker, 3));
    assert!(m.iter().any(|e| matches!(e, MarketEvent::Quote(_))));
    assert!(m.iter().any(|e| matches!(e, MarketEvent::MarkPrice(_))));
    assert!(m.iter().any(|e| matches!(e, MarketEvent::IndexPrice(_))));
    assert!(m.iter().any(|e| matches!(e, MarketEvent::Funding(_))));
    assert!(m.iter().any(|e| matches!(e, MarketEvent::OpenInterest(_))));
    assert!(matches!(
        m.iter().find(|e| matches!(e, MarketEvent::Statistics24h(_))),
        Some(MarketEvent::Statistics24h(st))
            if st.high.as_ref().unwrap().0 == Fixed::parse_str("37000.0").unwrap()
                && st.low.as_ref().unwrap().0 == Fixed::parse_str("35000.0").unwrap()
                && st.close.as_ref().unwrap().0 == Fixed::parse_str("36450.0").unwrap()
                && st.volume.as_ref().unwrap().0 == Fixed::parse_str("1234.5").unwrap()
                && st.quote_volume.as_ref().unwrap().0 == Fixed::parse_str("45000000.0").unwrap()
    ));

    let idx = r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"deribit_price_index.btc_usd","data":{"timestamp":1623060194400,"price":36440.5,"index_name":"btc_usd"}}}"#;
    let m = markets(&drive_text(&mut s, idx, 4));
    assert!(matches!(
        &m[0],
        MarketEvent::IndexPrice(p) if p.price.0 == Fixed::parse_str("36440.5").unwrap()
    ));

    let hb = r#"{"jsonrpc":"2.0","method":"heartbeat","params":{"type":"test_request"}}"#;
    let buf = drive_text(&mut s, hb, 5);
    assert!(
        buf.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::SendText(_)))
    );
}

#[test]
fn heartbeat_test_request_replies_with_public_test() {
    let mut s = session();
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    let hb = r#"{"jsonrpc":"2.0","method":"heartbeat","params":{"type":"test_request"}}"#;
    let out = drive_text(&mut s, hb, 2);
    let sent = out
        .as_slice()
        .iter()
        .find_map(|a| match a {
            SessionAction::SendText(bytes) => Some(bytes.clone()),
            _ => None,
        })
        .expect("public/test reply sent");
    let body: serde_json::Value = serde_json::from_slice(&sent).unwrap();
    assert_eq!(body["method"], "public/test");
}

#[test]
fn chart_trades_candle_fixture_exact_fixed() {
    use marketfeed_adapter_api::CandleInterval;
    use marketfeed_model::Fixed;

    let mut ids = HashMap::new();
    ids.insert("BTC-PERPETUAL".into(), InstrumentId(1));
    let mut s = DeribitSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(8), CatalogVersion(1)),
        DeribitSessionConfig {
            instruments: vec!["BTC-PERPETUAL".into()],
            instrument_ids: ids,
            session: SessionId(1),
            candle_intervals: vec![CandleInterval::M1],
            ..DeribitSessionConfig::default()
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
                if s.contains("chart.trades") {
                    Some(s.into_owned())
                } else {
                    None
                }
            }
            _ => None,
        })
        .expect("chart.trades subscribe");
    assert!(sub.contains("chart.trades.BTC-PERPETUAL.1"), "sub={sub}");

    let raw = r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"chart.trades.BTC-PERPETUAL.1","data":{"volume":0.05219351,"tick":1573645080000,"open":8869.79,"low":8788.25,"high":8870.31,"cost":460,"close":8791.25}}}"#;
    let m = markets(&drive_text(&mut s, raw, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::Candle(c)
            if c.open.0 == Fixed::parse_str("8869.79").unwrap()
                && c.high.0 == Fixed::parse_str("8870.31").unwrap()
                && c.low.0 == Fixed::parse_str("8788.25").unwrap()
                && c.close.0 == Fixed::parse_str("8791.25").unwrap()
                && c.volume.0 == Fixed::parse_str("0.05219351").unwrap()
                && c.interval_ns == 60_000_000_000
                && c.start_ts == TimestampNs(1_573_645_080_000_000_000)
    ));
}

#[test]
fn record_replay_deribit_trade_ticker_identical() {
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
        r#"{"jsonrpc":"2.0","id":1,"result":"ok"}"#,
        r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.BTC-PERPETUAL.raw","data":[{"trade_seq":1,"trade_id":"1","timestamp":1,"price":1.0,"amount":1,"direction":"buy","instrument_name":"BTC-PERPETUAL"}]}}"#,
        r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"ticker.BTC-PERPETUAL.100ms","data":{"timestamp":2,"instrument_name":"BTC-PERPETUAL","best_bid_price":1.0,"best_bid_amount":1,"best_ask_price":1.1,"best_ask_amount":1,"mark_price":1.05,"index_price":1.04,"funding_8h":0.0001,"open_interest":10}}}"#,
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
fn liquidation_tagged_trade_emits_trade_and_liquidation() {
    let mut s = session();
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    // Deribit has no dedicated public liq channel; `liquidation: "T"` tags a trade.
    let liq = r#"{"jsonrpc":"2.0","method":"subscription","params":{"channel":"trades.BTC-PERPETUAL.raw","data":[{"trade_seq":10,"trade_id":"999","timestamp":1623060194301,"price":36450.0,"amount":5,"direction":"buy","instrument_name":"BTC-PERPETUAL","liquidation":"T"}]}}"#;
    let m = markets(&drive_text(&mut s, liq, 2));
    assert_eq!(m.len(), 2);
    assert!(matches!(
        &m[0],
        MarketEvent::Trade(t)
            if t.aggressor == AggressorSide::Buy
                && t.price.0 == Fixed::parse_str("36450.0").unwrap()
                && t.quantity.0 == Fixed::parse_str("5").unwrap()
    ));
    assert!(matches!(
        &m[1],
        MarketEvent::Liquidation(l)
            if l.side == AggressorSide::Buy
                && l.price.0 == Fixed::parse_str("36450.0").unwrap()
                && l.quantity.0 == Fixed::parse_str("5").unwrap()
    ));
}
