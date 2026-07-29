//! Offline fixtures for Binance Coin-M trades / quote / mark / funding / OI / liq / L2 (`pu`).

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, ConcreteSubscriptionSet, EventBatch, HttpResponse, SessionAction, SessionInput,
    SessionMachine, SessionSpec,
};
use marketfeed_adapter_binance::{
    BinanceCoinmSession, BinanceCoinmSessionConfig, COINM_OI_POLL_INTERVAL_MS, COINM_OI_TIMER_ID,
};
use marketfeed_model::{
    CatalogVersion, CatalogView, FrameStamp, InstrumentId, MarketEvent, SessionId, TimestampNs,
    VenueId,
};
use std::collections::HashMap;

fn stamp(n: i64) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(n),
        mono_ns: n as u64,
    }
}

fn session(enable_l2: bool) -> BinanceCoinmSession {
    let mut ids = HashMap::new();
    ids.insert("BTCUSD_PERP".into(), InstrumentId(1));
    BinanceCoinmSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(12), CatalogVersion(1)),
        BinanceCoinmSessionConfig {
            symbols: vec!["BTCUSD_PERP".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2,
            price_scale: 1,
            qty_scale: 0,
            ..BinanceCoinmSessionConfig::default()
        },
    )
}

fn drive_text(s: &mut BinanceCoinmSession, text: &str, ts: i64) -> ActionBuffer {
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

fn http_ids(buf: &ActionBuffer) -> Vec<(u64, String)> {
    buf.as_slice()
        .iter()
        .filter_map(|a| match a {
            SessionAction::RequestHttp(r) => Some((r.id, r.url.clone())),
            _ => None,
        })
        .collect()
}

#[test]
fn coinm_agg_trade_quote_and_mark_bundle() {
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
    let sub = out
        .as_slice()
        .iter()
        .find_map(|a| match a {
            SessionAction::SendText(b) => Some(String::from_utf8_lossy(b).into_owned()),
            _ => None,
        })
        .expect("subscribe");
    assert!(sub.contains("btcusd_perp@trade"), "subscribe={sub}");
    assert!(sub.contains("btcusd_perp@bookTicker"), "subscribe={sub}");
    assert!(sub.contains("btcusd_perp@markPrice@1s"), "subscribe={sub}");
    assert!(
        sub.contains("btcusd@indexPrice@1s"),
        "dedicated Coin-M index pair stream: {sub}"
    );
    assert!(sub.contains("btcusd_perp@ticker"), "subscribe={sub}");
    assert!(sub.contains("btcusd_perp@forceOrder"), "subscribe={sub}");
    assert!(
        !sub.contains("@depth"),
        "trades-only session must not subscribe depth: {sub}"
    );
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::MarkLive))
    );
    assert!(
        http_ids(&out)
            .iter()
            .any(|(_, u)| u.contains("openInterest"))
    );
    assert!(
        out.as_slice().iter().any(|a| matches!(
            a,
            SessionAction::ScheduleTimer(t) if t.timer_id == COINM_OI_TIMER_ID
                && t.fire_at.0 == 1 + COINM_OI_POLL_INTERVAL_MS * 1_000_000
        )),
        "expected OI ScheduleTimer on connect, got {:?}",
        out.as_slice()
    );

    let trade = r#"{"e":"aggTrade","E":1000,"s":"BTCUSD_PERP","a":42,"p":"65000.1","q":"1","f":1,"l":2,"T":1001,"m":false}"#;
    let m = markets(&drive_text(&mut s, trade, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::Trade(t)
            if t.aggressor == AggressorSide::Buy && t.price.0 == Fixed::new(650001, 1)
    ));

    let quote = r#"{"u":9,"s":"BTCUSD_PERP","b":"65000.0","B":"1","a":"65000.1","A":"2"}"#;
    let m = markets(&drive_text(&mut s, quote, 3));
    assert!(matches!(&m[0], MarketEvent::Quote(_)));

    let mark = r#"{"e":"markPriceUpdate","E":10,"s":"BTCUSD_PERP","p":"65000.00","i":"64990.00","P":"65001.00","r":"0.00010000","T":20}"#;
    let m = markets(&drive_text(&mut s, mark, 4));
    assert_eq!(m.len(), 3);
    assert!(matches!(&m[0], MarketEvent::MarkPrice(_)));
    assert!(matches!(&m[1], MarketEvent::IndexPrice(_)));
    assert!(matches!(&m[2], MarketEvent::Funding(f) if f.next_funding_ts.is_some()));

    let idx = r#"{"e":"indexPriceUpdate","E":11,"i":"BTCUSD","p":"64991.00"}"#;
    let m = markets(&drive_text(&mut s, idx, 5));
    assert!(matches!(
        &m[0],
        MarketEvent::IndexPrice(p) if p.price.0 == Fixed::new(6499100, 2)
    ));
}

#[test]
fn coinm_ticker_24h_stats_fixture_exact_fixed() {
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

    // Captured from the production COIN-M stream on 2026-07-23. Unlike the
    // USD-M ticker, this payload does not contain best-bid/ask fields; quote
    // data arrives independently on `@bookTicker`.
    let ticker = r#"{"e":"24hrTicker","E":1784817230130,"s":"BTCUSD_PERP","ps":"BTCUSD","p":"-664.1","P":"-1.012","w":"65618.86747127","c":"64971.5","Q":"3","o":"65635.6","h":"66324.1","l":"64750.0","v":"4502681","q":"6861.9","O":1784730780000,"C":1784817230005,"F":1144361347,"L":1144501772,"n":140426,"st":2}"#;
    let m = markets(&drive_text(&mut s, ticker, 2));
    assert_eq!(m.len(), 1);
    let MarketEvent::Statistics24h(stats) = &m[0] else {
        panic!("expected Statistics24h");
    };
    assert_eq!(stats.open.as_ref().unwrap().0, Fixed::new(656356, 1));
    assert_eq!(stats.high.as_ref().unwrap().0, Fixed::new(663241, 1));
    assert_eq!(stats.low.as_ref().unwrap().0, Fixed::new(647500, 1));
    assert_eq!(stats.close.as_ref().unwrap().0, Fixed::new(649715, 1));
    assert_eq!(stats.volume.as_ref().unwrap().0, Fixed::new(4502681, 0));
    assert_eq!(stats.quote_volume.as_ref().unwrap().0, Fixed::new(68619, 1));
}

#[test]
fn coinm_book_ticker_accepts_current_event_tagged_shape() {
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

    let ticker = r#"{"e":"bookTicker","u":1144501772,"s":"BTCUSD_PERP","ps":"BTCUSD","b":"64971.4","B":"23","a":"64971.5","A":"3","T":1784817230005,"E":1784817230130}"#;
    let m = markets(&drive_text(&mut s, ticker, 2));
    assert_eq!(m.len(), 1);
    let MarketEvent::Quote(quote) = &m[0] else {
        panic!("expected Quote");
    };
    assert_eq!(quote.bid_price.0, Fixed::new(649714, 1));
    assert_eq!(quote.ask_price.0, Fixed::new(649715, 1));
}

#[test]
fn coinm_force_order_and_oi_http() {
    let mut s = session(false);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let oi_id = http_ids(&out)
        .into_iter()
        .find(|(_, u)| u.contains("openInterest"))
        .map(|(id, _)| id)
        .expect("oi request");

    let mut oi_out = ActionBuffer::new();
    s.on_input(
        SessionInput::HttpResponse {
            request_id: oi_id,
            response: &HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(
                    br#"{"symbol":"BTCUSD_PERP","openInterest":"10659.509","time":1589437530011}"#,
                ),
            },
            received: stamp(20),
        },
        &mut oi_out,
    )
    .unwrap();
    assert!(matches!(&markets(&oi_out)[0], MarketEvent::OpenInterest(_)));

    let liq = r#"{"e":"forceOrder","E":5,"o":{"s":"BTCUSD_PERP","S":"SELL","o":"LIMIT","f":"IOC","q":"1","p":"9900","ap":"9910","X":"FILLED","l":"1","z":"1","T":5}}"#;
    let m = markets(&drive_text(&mut s, liq, 6));
    assert!(matches!(&m[0], MarketEvent::Liquidation(_)));
}

#[test]
fn coinm_l2_snapshot_pu_drain_and_gap() {
    let mut s = session(true);
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
    assert!(sub.contains("btcusd_perp@depth@100ms"), "subscribe={sub}");
    let depth_id = http_ids(&out)
        .into_iter()
        .find(|(_, u)| u.contains("/dapi/v1/depth?"))
        .map(|(id, _)| id)
        .expect("dapi depth snapshot request");

    let _ = drive_text(
        &mut s,
        r#"{"e":"depthUpdate","E":1,"T":1,"s":"BTCUSD_PERP","U":99,"u":102,"pu":98,"b":[["100.0","1"]],"a":[["101.0","2"]]}"#,
        2,
    );

    let mut snap_out = ActionBuffer::new();
    s.on_input(
        SessionInput::HttpResponse {
            request_id: depth_id,
            response: &HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(
                    br#"{"lastUpdateId":100,"bids":[["100.0","1"]],"asks":[["101.0","2"]]}"#,
                ),
            },
            received: stamp(21),
        },
        &mut snap_out,
    )
    .unwrap();
    let m = markets(&snap_out);
    assert!(m.iter().any(|e| matches!(e, MarketEvent::BookSnapshot(_))));
    assert!(m.iter().any(|e| matches!(e, MarketEvent::BookDelta(_))));
    assert!(
        snap_out
            .as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::MarkLive))
    );

    let live = markets(&drive_text(
        &mut s,
        r#"{"e":"depthUpdate","E":3,"T":3,"s":"BTCUSD_PERP","U":103,"u":104,"pu":102,"b":[["99.0","1"]],"a":[]}"#,
        3,
    ));
    assert!(matches!(&live[0], MarketEvent::BookDelta(_)));

    let gap = drive_text(
        &mut s,
        r#"{"e":"depthUpdate","E":4,"T":4,"s":"BTCUSD_PERP","U":200,"u":201,"pu":150,"b":[["98.0","1"]],"a":[]}"#,
        4,
    );
    assert!(
        gap.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::Reconnect(_)))
    );
}

#[test]
fn coinm_first_ws_event_bridges_snapshot_without_matching_pu() {
    let mut session = session(true);
    let mut connected = ActionBuffer::new();
    session
        .on_input(
            SessionInput::Connected {
                now: TimestampNs(1),
            },
            &mut connected,
        )
        .unwrap();
    let depth_id = http_ids(&connected)
        .into_iter()
        .find(|(_, url)| url.contains("/dapi/v1/depth?"))
        .map(|(id, _)| id)
        .unwrap();

    let mut snapshot = ActionBuffer::new();
    session
        .on_input(
            SessionInput::HttpResponse {
                request_id: depth_id,
                response: &HttpResponse {
                    status: 200,
                    headers: Vec::new(),
                    body: Bytes::from_static(
                        br#"{"lastUpdateId":100,"bids":[["100.0","1"]],"asks":[["101.0","2"]]}"#,
                    ),
                },
                received: stamp(2),
            },
            &mut snapshot,
        )
        .unwrap();

    let first = drive_text(
        &mut session,
        r#"{"e":"depthUpdate","E":3,"T":3,"s":"BTCUSD_PERP","U":99,"u":102,"pu":98,"b":[["100.0","2"]],"a":[]}"#,
        3,
    );
    assert!(
        markets(&first)
            .iter()
            .any(|event| matches!(event, MarketEvent::BookDelta(_)))
    );
    assert!(
        !first
            .as_slice()
            .iter()
            .any(|action| matches!(action, SessionAction::Reconnect(_)))
    );
}

#[test]
fn coinm_kline_candle_fixture_exact_fixed() {
    use marketfeed_adapter_api::CandleInterval;
    use marketfeed_model::Fixed;

    let mut ids = HashMap::new();
    ids.insert("BTCUSD_PERP".into(), InstrumentId(1));
    let mut s = BinanceCoinmSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(12), CatalogVersion(1)),
        BinanceCoinmSessionConfig {
            symbols: vec!["BTCUSD_PERP".into()],
            instrument_ids: ids,
            session: SessionId(1),
            candle_intervals: vec![CandleInterval::M1],
            price_scale: 1,
            qty_scale: 0,
            ..BinanceCoinmSessionConfig::default()
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
            SessionAction::SendText(b) => Some(std::str::from_utf8(b).unwrap().to_string()),
            _ => None,
        })
        .expect("subscribe");
    assert!(sub.contains("btcusd_perp@kline_1m"), "sub={sub}");

    let kline = r#"{"e":"kline","E":123456789,"s":"BTCUSD_PERP","k":{"t":123400000,"T":123460000,"s":"BTCUSD_PERP","i":"1m","f":100,"L":200,"o":"0.0010","c":"0.0020","h":"0.0025","l":"0.0015","v":"1000","n":100,"x":true,"q":"1.0000","V":"500","Q":"0.500","B":"123456"}}"#;
    let m = markets(&drive_text(&mut s, kline, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::Candle(c)
            if c.interval_ns == 60_000_000_000
                && c.open.0 == Fixed::new(10, 4)
                && c.close.0 == Fixed::new(20, 4)
                && c.start_ts == TimestampNs(123_400_000_000_000)
    ));
}
