//! Offline fixtures for Bitfinex public trade / quote / L2 / WS candles / Stats24h.

use marketfeed_adapter_api::{
    ActionBuffer, CandleInterval, ConcreteSubscriptionSet, SessionAction, SessionInput,
    SessionMachine, SessionSpec,
};
use marketfeed_adapter_bitfinex::{BitfinexSession, BitfinexSessionConfig};
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

fn session(enable_l2: bool) -> BitfinexSession {
    session_with(enable_l2, Vec::new())
}

fn session_with(enable_l2: bool, candle_intervals: Vec<CandleInterval>) -> BitfinexSession {
    let mut ids = HashMap::new();
    ids.insert("tBTCUSD".into(), InstrumentId(1));
    BitfinexSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(17), CatalogVersion(1)),
        BitfinexSessionConfig {
            symbols: vec!["tBTCUSD".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2,
            candle_intervals,
            ..BitfinexSessionConfig::default()
        },
    )
}

fn drive(s: &mut BitfinexSession, text: &str, ts: i64) -> ActionBuffer {
    let mut out = ActionBuffer::new();
    let mut bytes = text.as_bytes().to_vec();
    s.on_input(
        SessionInput::TextFrame {
            bytes: &mut bytes,
            received: stamp(ts),
        },
        &mut out,
    )
    .unwrap();
    out
}

fn markets(buf: &ActionBuffer) -> Vec<MarketEvent> {
    buf.as_slice()
        .iter()
        .filter_map(|a| match a {
            SessionAction::EmitBatch(b) => Some(b),
            _ => None,
        })
        .flat_map(|b| b.events.iter().map(|e| e.payload.clone()))
        .collect()
}

#[test]
fn subscribe_trades_ticker_and_optional_book() {
    let mut s = session(true);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let sends: Vec<String> = out
        .as_slice()
        .iter()
        .filter_map(|a| match a {
            SessionAction::SendText(b) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        })
        .collect();
    assert!(
        sends
            .iter()
            .any(|s| s.contains(r#""channel":"trades""#) && s.contains("tBTCUSD")),
        "{sends:?}"
    );
    assert!(
        sends.iter().any(|s| s.contains(r#""channel":"ticker""#)),
        "{sends:?}"
    );
    assert!(
        sends
            .iter()
            .any(|s| s.contains(r#""channel":"book""#) && s.contains(r#""prec":"P0""#)),
        "{sends:?}"
    );
}

#[test]
fn trade_and_quote_exact_fixed_via_chan_id() {
    let mut s = session(false);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    drive(
        &mut s,
        r#"{"event":"subscribed","channel":"trades","chanId":10,"symbol":"tBTCUSD","pair":"BTCUSD"}"#,
        2,
    );
    drive(
        &mut s,
        r#"{"event":"subscribed","channel":"ticker","chanId":11,"symbol":"tBTCUSD","pair":"BTCUSD"}"#,
        3,
    );

    let m = markets(&drive(
        &mut s,
        r#"[10,"te",[401597395,1574694478808,0.005,7245.3]]"#,
        4,
    ));
    assert!(matches!(
        &m[0],
        MarketEvent::Trade(t)
            if t.aggressor == AggressorSide::Buy
                && t.price.0 == Fixed::parse_str("7245.3").unwrap()
                && t.quantity.0 == Fixed::parse_str("0.005").unwrap()
    ));

    out = drive(&mut s, r#"[11,[29000.12,1.5,29001.00,2.0,0,0,0,0,0,0]]"#, 5);
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::MarkLive))
    );
    let quotes: Vec<_> = markets(&out)
        .into_iter()
        .filter_map(|e| match e {
            MarketEvent::Quote(q) => Some(q),
            _ => None,
        })
        .collect();
    assert_eq!(quotes.len(), 1);
    let q = &quotes[0];
    assert_eq!(q.bid_price.0, Fixed::parse_str("29000.12").unwrap());
    assert_eq!(q.ask_price.0, Fixed::parse_str("29001.0").unwrap());
    assert_eq!(
        q.bid_quantity.as_ref().unwrap().0,
        Fixed::parse_str("1.5").unwrap()
    );
    assert_eq!(
        q.ask_quantity.as_ref().unwrap().0,
        Fixed::parse_str("2.0").unwrap()
    );
    assert!(
        !markets(&out)
            .iter()
            .any(|e| matches!(e, MarketEvent::Statistics24h(_)))
    );
    assert!(
        !markets(&out)
            .iter()
            .any(|e| matches!(e, MarketEvent::BookSnapshot(_)))
    );
}

#[test]
fn ticker_nonzero_stats_emits_statistics24h() {
    let mut s = session(false);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    drive(
        &mut s,
        r#"{"event":"subscribed","channel":"ticker","chanId":11,"symbol":"tBTCUSD"}"#,
        2,
    );
    let m = markets(&drive(
        &mut s,
        r#"[11,[29000.12,1.5,29001.00,2.0,0,0,29050.5,100.25,29100.0,28900.0]]"#,
        3,
    ));
    assert_eq!(m.len(), 2);
    assert!(matches!(&m[0], MarketEvent::Quote(_)));
    assert!(matches!(
        &m[1],
        MarketEvent::Statistics24h(st)
            if st.close.as_ref().unwrap().0 == Fixed::parse_str("29050.5").unwrap()
                && st.volume.as_ref().unwrap().0 == Fixed::parse_str("100.25").unwrap()
                && st.high.as_ref().unwrap().0 == Fixed::parse_str("29100.0").unwrap()
                && st.low.as_ref().unwrap().0 == Fixed::parse_str("28900.0").unwrap()
                && st.open.is_none()
                && st.quote_volume.is_none()
    ));
}

#[test]
fn l2_snapshot_then_delta_goes_live() {
    let mut s = session(true);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    drive(
        &mut s,
        r#"{"event":"subscribed","channel":"book","chanId":20,"symbol":"tBTCUSD","pair":"BTCUSD"}"#,
        2,
    );

    out = drive(&mut s, r#"[20,[[29000.0,2,1.5],[29001.0,1,-2.0]]]"#, 3);
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::MarkLive))
    );
    assert!(
        markets(&out)
            .iter()
            .any(|e| matches!(e, MarketEvent::BookSnapshot(_)))
    );

    out = drive(&mut s, r#"[20,[29000.0,3,1.8]]"#, 4);
    assert!(
        markets(&out)
            .iter()
            .any(|e| matches!(e, MarketEvent::BookDelta(_)))
    );

    out = drive(&mut s, r#"[20,[29001.0,0,-1]]"#, 5);
    assert!(matches!(
        markets(&out).iter().find(|e| matches!(e, MarketEvent::BookDelta(_))),
        Some(MarketEvent::BookDelta(d))
            if d.changes[0].operation == marketfeed_model::BookOperation::Delete
                && d.changes[0].side == marketfeed_model::BookSide::Ask
    ));
}

#[test]
fn tu_does_not_emit_second_trade() {
    let mut s = session(false);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    drive(
        &mut s,
        r#"{"event":"subscribed","channel":"trades","chanId":10,"symbol":"tBTCUSD"}"#,
        2,
    );
    assert!(
        !markets(&drive(&mut s, r#"[10,"tu",[1,1,-0.1,100]]"#, 3))
            .iter()
            .any(|e| matches!(e, MarketEvent::Trade(_)))
    );
}

#[test]
fn candles_ws_subscribe_and_exact_fixed() {
    let mut s = session_with(false, vec![CandleInterval::M1]);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let sends: Vec<String> = out
        .as_slice()
        .iter()
        .filter_map(|a| match a {
            SessionAction::SendText(b) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        })
        .collect();
    assert!(
        sends
            .iter()
            .any(|s| s.contains(r#""channel":"candles""#)
                && s.contains(r#""key":"trade:1m:tBTCUSD""#)),
        "{sends:?}"
    );
    assert!(
        !out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::RequestHttp(_))),
        "no REST candle timer path"
    );

    drive(
        &mut s,
        r#"{"event":"subscribed","channel":"candles","chanId":341561,"key":"trade:1m:tBTCUSD"}"#,
        2,
    );
    let m = markets(&drive(
        &mut s,
        r#"[341561,[1609459200000,28901.57,28800.01,28902.46,28800,49.3149836]]"#,
        3,
    ));
    assert!(matches!(&m[0], MarketEvent::Candle(c)
        if c.open.0 == Fixed::parse_str("28901.57").unwrap()
            && c.high.0 == Fixed::parse_str("28902.46").unwrap()
            && c.low.0 == Fixed::parse_str("28800").unwrap()
            && c.close.0 == Fixed::parse_str("28800.01").unwrap()
            && c.volume.0 == Fixed::parse_str("49.3149836").unwrap()
            && c.interval_ns == 60_000_000_000
            && c.start_ts == TimestampNs(1_609_459_200_000_000_000)));
}
