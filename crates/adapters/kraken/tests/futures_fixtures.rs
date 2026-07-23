//! Offline fixtures for Kraken Futures public trade / ticker / L2 / REST candles.

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, CandleInterval, Capability, ConcreteSubscriptionSet, HttpResponse, SessionAction,
    SessionInput, SessionMachine, SessionSpec, StopReason,
};
use marketfeed_adapter_kraken::{
    FUTURES_CANDLE_POLL_INTERVAL_MS, FUTURES_CANDLE_TIMER_ID, KRAKEN_FUTURES_SPEC,
    KrakenFuturesSession, KrakenFuturesSessionConfig,
};
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

fn session(enable_l2: bool) -> KrakenFuturesSession {
    session_with(enable_l2, Vec::new())
}

fn session_with(enable_l2: bool, candle_intervals: Vec<CandleInterval>) -> KrakenFuturesSession {
    let mut ids = HashMap::new();
    ids.insert("PF_XBTUSD".into(), InstrumentId(1));
    KrakenFuturesSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(13), CatalogVersion(1)),
        KrakenFuturesSessionConfig {
            symbols: vec!["PF_XBTUSD".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2,
            candle_intervals,
            ..KrakenFuturesSessionConfig::default()
        },
    )
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

fn drive(s: &mut KrakenFuturesSession, text: &str, ts: i64) -> ActionBuffer {
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
fn futures_spec_claims_ticker_enrich_and_liq_caps() {
    let caps = KRAKEN_FUTURES_SPEC.capabilities;
    for need in [
        Capability::Trades,
        Capability::Quote,
        Capability::L2Book,
        Capability::MarkPrice,
        Capability::IndexPrice,
        Capability::Funding,
        Capability::OpenInterest,
        Capability::Liquidations,
        Capability::Candles,
        Capability::Statistics24h,
    ] {
        assert!(caps.contains(&need), "KRAKEN_FUTURES_SPEC missing {need:?}");
    }
}

#[test]
fn subscribe_trade_ticker_and_optional_book() {
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
        sends.iter().any(|s| s.contains(r#""feed":"trade""#)),
        "{sends:?}"
    );
    assert!(
        sends.iter().any(|s| s.contains(r#""feed":"ticker""#)),
        "{sends:?}"
    );
    assert!(
        sends.iter().any(|s| s.contains(r#""feed":"book""#)),
        "{sends:?}"
    );
    assert!(sends.iter().any(|s| s.contains("PF_XBTUSD")), "{sends:?}");
}

#[test]
fn subscribed_failed_is_fatal_and_never_marks_live() {
    let mut s = session(false);
    let failed = drive(
        &mut s,
        r#"{"event":"subscribed_failed","feed":"book","product_ids":["PF_XBTUSD"],"message":"invalid product"}"#,
        1,
    );

    assert!(failed.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(marketfeed_model::SystemEvent::SubscriptionStateChanged { state })
            if state.contains("failed")
    )));
    assert!(failed.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::StopSession(StopReason::FatalProtocol)
    )));
    assert!(
        !failed
            .as_slice()
            .iter()
            .any(|action| matches!(action, SessionAction::MarkLive))
    );
}

#[test]
fn trade_and_ticker_quote_exact_fixed() {
    let mut s = session(false);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    let trade = r#"{"feed":"trade","product_id":"PF_XBTUSD","uid":"caa9c653-420b-4c24-a9f1-462a054d86f1","side":"sell","type":"fill","seq":655508,"time":1612269657781,"qty":440,"price":34893}"#;
    let m = markets(&drive(&mut s, trade, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::Trade(t)
            if t.aggressor == AggressorSide::Sell
                && t.price.0 == Fixed::parse_str("34893").unwrap()
                && t.quantity.0 == Fixed::parse_str("440").unwrap()
    ));

    let ticker = r#"{"feed":"ticker","product_id":"PF_XBTUSD","bid":21978.5,"ask":21987.0,"bid_size":2536.0,"ask_size":13948.0,"time":1676393235406}"#;
    let m = markets(&drive(&mut s, ticker, 3));
    assert!(matches!(
        &m[0],
        MarketEvent::Quote(q)
            if q.bid_price.0 == Fixed::parse_str("21978.5").unwrap()
                && q.ask_price.0 == Fixed::parse_str("21987.0").unwrap()
    ));
}

#[test]
fn ticker_mark_index_funding_oi_exact_fixed() {
    let mut s = session(false);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    // Official docs snapshot shape (funding as plain decimal for Fixed exactness).
    let ticker = r#"{"time":1676393235406,"product_id":"PF_XBTUSD","funding_rate":0.0001,"next_funding_rate_time":1676394000000,"feed":"ticker","bid":21978.5,"ask":21987.0,"bid_size":2536.0,"ask_size":13948.0,"index":21984.54,"openInterest":30072580.0,"markPrice":21979.5,"open":21000.0,"high":23000.0,"low":20000.0,"last":21980.0,"volume":1234.5,"volumeQuote":27000000.0}"#;
    let m = markets(&drive(&mut s, ticker, 2));
    assert!(
        m.iter().any(|e| matches!(
            e,
            MarketEvent::Quote(q)
                if q.bid_price.0 == Fixed::parse_str("21978.5").unwrap()
        )),
        "{m:?}"
    );
    assert!(
        m.iter().any(|e| matches!(
            e,
            MarketEvent::MarkPrice(p) if p.price.0 == Fixed::parse_str("21979.5").unwrap()
        )),
        "{m:?}"
    );
    assert!(
        m.iter().any(|e| matches!(
            e,
            MarketEvent::IndexPrice(p) if p.price.0 == Fixed::parse_str("21984.54").unwrap()
        )),
        "{m:?}"
    );
    assert!(
        m.iter().any(|e| matches!(
            e,
            MarketEvent::Funding(f)
                if f.rate.0 == Fixed::parse_str("0.0001").unwrap()
                    && f.next_funding_ts == Some(TimestampNs(1_676_394_000_000_000_000))
        )),
        "{m:?}"
    );
    assert!(
        m.iter().any(|e| matches!(
            e,
            MarketEvent::OpenInterest(oi)
                if oi.quantity.0 == Fixed::parse_str("30072580.0").unwrap()
        )),
        "{m:?}"
    );
    assert!(
        m.iter().any(|e| matches!(
            e,
            MarketEvent::Statistics24h(st)
                if st.open.as_ref().unwrap().0 == Fixed::parse_str("21000.0").unwrap()
                    && st.high.as_ref().unwrap().0 == Fixed::parse_str("23000.0").unwrap()
                    && st.low.as_ref().unwrap().0 == Fixed::parse_str("20000.0").unwrap()
                    && st.close.as_ref().unwrap().0 == Fixed::parse_str("21980.0").unwrap()
                    && st.volume.as_ref().unwrap().0 == Fixed::parse_str("1234.5").unwrap()
                    && st.quote_volume.as_ref().unwrap().0
                        == Fixed::parse_str("27000000.0").unwrap()
        )),
        "{m:?}"
    );
}

#[test]
fn liquidation_trade_emits_trade_and_liquidation() {
    let mut s = session(false);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    // No dedicated public liq channel; `type=liquidation` tags a trade.
    let liq = r#"{"feed":"trade","product_id":"PF_XBTUSD","uid":"liq-uid-1","side":"buy","type":"liquidation","seq":42,"time":1612269657781,"qty":100,"price":35000}"#;
    let m = markets(&drive(&mut s, liq, 2));
    assert_eq!(m.len(), 2, "{m:?}");
    assert!(matches!(
        &m[0],
        MarketEvent::Trade(t)
            if t.aggressor == AggressorSide::Buy
                && t.price.0 == Fixed::parse_str("35000").unwrap()
                && t.quantity.0 == Fixed::parse_str("100").unwrap()
    ));
    assert!(matches!(
        &m[1],
        MarketEvent::Liquidation(l)
            if l.side == AggressorSide::Buy
                && l.price.0 == Fixed::parse_str("35000").unwrap()
                && l.quantity.0 == Fixed::parse_str("100").unwrap()
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

    let snap = r#"{"feed":"book_snapshot","product_id":"PF_XBTUSD","timestamp":1612269825817,"seq":10,"bids":[{"price":34892.5,"qty":6385}],"asks":[{"price":34911.5,"qty":20598}]}"#;
    out = drive(&mut s, snap, 2);
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

    let delta = r#"{"feed":"book","product_id":"PF_XBTUSD","side":"buy","seq":11,"price":34892.5,"qty":7000,"timestamp":1612269953629}"#;
    out = drive(&mut s, delta, 3);
    assert!(
        markets(&out)
            .iter()
            .any(|e| matches!(e, MarketEvent::BookDelta(_)))
    );

    // qty=0 deletes the level.
    let del = r#"{"feed":"book","product_id":"PF_XBTUSD","side":"sell","seq":12,"price":34911.5,"qty":0,"timestamp":1612269953630}"#;
    out = drive(&mut s, del, 4);
    assert!(matches!(
        &markets(&out)[0],
        MarketEvent::BookDelta(d) if d.changes[0].operation == marketfeed_model::BookOperation::Delete
    ));
}

#[test]
fn charts_rest_timer_fixture_exact_fixed() {
    let mut s = session_with(false, vec![CandleInterval::M1]);
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
        .find(|(_, u)| u.contains("/api/charts/v1/trade/"))
        .expect("charts");
    assert!(
        url.contains("/trade/PF_XBTUSD/1m") && url.contains("count=1"),
        "{url}"
    );
    assert!(out.as_slice().iter().any(|a| matches!(
        a, SessionAction::ScheduleTimer(t) if t.timer_id == FUTURES_CANDLE_TIMER_ID
            && t.fire_at.0 == 1 + FUTURES_CANDLE_POLL_INTERVAL_MS * 1_000_000
    )));
    let mut candle_out = ActionBuffer::new();
    s.on_input(
        SessionInput::HttpResponse {
            request_id: req_id,
            response: &HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(
                    br#"{"candles":[{"time":1609459200000,"open":"28050.0","high":"28150","low":"27983.0","close":"28126.0","volume":"1089794.00000000"}],"more_candles":false}"#,
                ),
            },
            received: stamp(7),
        },
        &mut candle_out,
    )
    .unwrap();
    assert!(matches!(
        &markets(&candle_out)[0],
        MarketEvent::Candle(c)
            if c.open.0 == Fixed::parse_str("28050.0").unwrap()
                && c.high.0 == Fixed::parse_str("28150").unwrap()
                && c.low.0 == Fixed::parse_str("27983.0").unwrap()
                && c.close.0 == Fixed::parse_str("28126.0").unwrap()
                && c.volume.0 == Fixed::parse_str("1089794.00000000").unwrap()
                && c.interval_ns == 60_000_000_000
                && c.start_ts == TimestampNs(1_609_459_200_000_000_000)
    ));
    let fire_at = TimestampNs(1 + FUTURES_CANDLE_POLL_INTERVAL_MS * 1_000_000);
    let mut tick = ActionBuffer::new();
    s.on_input(
        SessionInput::Timer {
            timer_id: FUTURES_CANDLE_TIMER_ID,
            now: fire_at,
        },
        &mut tick,
    )
    .unwrap();
    assert!(
        http_ids(&tick)
            .iter()
            .any(|(_, u)| u.contains("/api/charts/v1/trade/"))
    );
}
