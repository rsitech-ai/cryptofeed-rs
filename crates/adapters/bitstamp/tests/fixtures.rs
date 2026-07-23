//! Offline fixtures for Bitstamp public trade / quote / L2.

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, CandleInterval, ConcreteSubscriptionSet, HttpResponse, SessionAction,
    SessionInput, SessionMachine, SessionSpec,
};
use marketfeed_adapter_bitstamp::{
    BitstampSession, BitstampSessionConfig, CANDLE_POLL_INTERVAL_MS, CANDLE_TIMER_ID,
    STATS_POLL_INTERVAL_MS, STATS_TIMER_ID,
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

fn session(enable_l2: bool) -> BitstampSession {
    session_with(enable_l2, Vec::new())
}

fn session_with(enable_l2: bool, candle_intervals: Vec<CandleInterval>) -> BitstampSession {
    let mut ids = HashMap::new();
    ids.insert("btcusd".into(), InstrumentId(1));
    BitstampSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(14), CatalogVersion(1)),
        BitstampSessionConfig {
            symbols: vec!["btcusd".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2,
            candle_intervals,
            ..BitstampSessionConfig::default()
        },
    )
}

fn drive(s: &mut BitstampSession, text: &str, ts: i64) -> ActionBuffer {
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
            SessionAction::EmitBatch(b) => Some(b),
            _ => None,
        })
        .flat_map(|b| b.events.iter().map(|e| e.payload.clone()))
        .collect()
}

#[test]
fn live_l2_uses_the_continuous_full_book_channel() {
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
        sends.iter().any(|s| s.contains("live_trades_btcusd")),
        "{sends:?}"
    );
    assert!(
        sends.iter().any(|s| s.contains("order_book_btcusd")),
        "{sends:?}"
    );
    assert!(
        !sends.iter().any(|s| s.contains("diff_order_book_btcusd")),
        "the unsequenced full and diff channels cannot safely be merged: {sends:?}"
    );
}

#[test]
fn trade_and_quote_exact_fixed() {
    let mut s = session(false);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    let trade = r#"{"channel":"live_trades_btcusd","event":"trade","data":{"id":123,"amount":"0.10000000","amount_str":"0.10000000","price":"29000.12","price_str":"29000.12","type":0,"timestamp":"1609459200","microtimestamp":"1609459200123456"}}"#;
    let m = markets(&drive(&mut s, trade, 2));
    assert!(matches!(
        &m[0],
        MarketEvent::Trade(t)
            if t.aggressor == AggressorSide::Buy
                && t.price.0 == Fixed::parse_str("29000.12").unwrap()
                && t.quantity.0 == Fixed::parse_str("0.10000000").unwrap()
    ));

    let book = r#"{"channel":"order_book_btcusd","event":"data","data":{"timestamp":"1609459200","microtimestamp":"1609459200123456","bids":[["29000.00","1.50000000"]],"asks":[["29001.00","2.00000000"]]}}"#;
    out = drive(&mut s, book, 3);
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::MarkLive))
    );
    assert!(matches!(
        markets(&out).iter().find(|e| matches!(e, MarketEvent::Quote(_))),
        Some(MarketEvent::Quote(q))
            if q.bid_price.0 == Fixed::parse_str("29000.00").unwrap()
                && q.ask_price.0 == Fixed::parse_str("29001.00").unwrap()
                && q.bid_quantity.as_ref().unwrap().0 == Fixed::parse_str("1.50000000").unwrap()
    ));
    // Quotes without L2 must not emit BookSnapshot.
    assert!(
        !markets(&out)
            .iter()
            .any(|e| matches!(e, MarketEvent::BookSnapshot(_)))
    );
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

    let snap = r#"{"channel":"order_book_btcusd","event":"data","data":{"timestamp":"1609459200","microtimestamp":"1609459200123456","bids":[["29000.00","1.50000000"]],"asks":[["29001.00","2.00000000"]]}}"#;
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
    assert!(
        markets(&out)
            .iter()
            .any(|e| matches!(e, MarketEvent::Quote(_)))
    );

    let delta = r#"{"channel":"diff_order_book_btcusd","event":"data","data":{"timestamp":"1609459201","microtimestamp":"1609459201123456","bids":[["29000.00","1.80000000"]],"asks":[]}}"#;
    out = drive(&mut s, delta, 3);
    assert!(
        markets(&out)
            .iter()
            .any(|e| matches!(e, MarketEvent::BookDelta(_)))
    );

    let del = r#"{"channel":"diff_order_book_btcusd","event":"data","data":{"timestamp":"1609459202","microtimestamp":"1609459202123456","bids":[],"asks":[["29001.00","0"]]}}"#;
    out = drive(&mut s, del, 4);
    assert!(matches!(
        markets(&out).iter().find(|e| matches!(e, MarketEvent::BookDelta(_))),
        Some(MarketEvent::BookDelta(d))
            if d.changes[0].operation == marketfeed_model::BookOperation::Delete
    ));
}

#[test]
fn multi_level_delta_is_validated_after_the_complete_message() {
    let mut s = session(true);
    let mut connected = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut connected,
    )
    .unwrap();
    drive(
        &mut s,
        r#"{"channel":"order_book_btcusd","event":"data","data":{"timestamp":"1","microtimestamp":"1000000","bids":[["100.00","1.0"]],"asks":[["101.00","1.0"]]}}"#,
        2,
    );

    let out = drive(
        &mut s,
        r#"{"channel":"diff_order_book_btcusd","event":"data","data":{"timestamp":"2","microtimestamp":"2000000","bids":[["102.00","1.0"]],"asks":[["101.00","0"],["103.00","1.0"]]}}"#,
        3,
    );

    assert!(
        markets(&out)
            .iter()
            .any(|event| matches!(event, MarketEvent::BookDelta(_)))
    );
    assert!(!out.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(SystemEvent::BookInvalidated { .. })
    )));
}

#[test]
fn continuous_full_book_frames_refresh_the_live_l2_book() {
    let mut s = session(true);
    let mut connected = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut connected,
    )
    .unwrap();
    let snapshot = r#"{"channel":"order_book_btcusd","event":"data","data":{"timestamp":"1","microtimestamp":"1000000","bids":[["100.00","1.0"]],"asks":[["101.00","1.0"]]}}"#;
    drive(&mut s, snapshot, 2);

    let duplicate = drive(&mut s, snapshot, 3);

    assert!(
        markets(&duplicate)
            .iter()
            .any(|event| matches!(event, MarketEvent::BookSnapshot(_)))
    );
    assert!(!duplicate.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(SystemEvent::BookResynchronized { .. })
    )));
}

#[test]
fn crossed_replacement_snapshot_is_rejected_without_invalidating_live_book() {
    let mut s = session(true);
    let mut connected = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut connected,
    )
    .unwrap();
    drive(
        &mut s,
        r#"{"channel":"order_book_btcusd","event":"data","data":{"timestamp":"1","microtimestamp":"1000000","bids":[["100.00","1.0"]],"asks":[["101.00","1.0"]]}}"#,
        2,
    );

    let rejected = drive(
        &mut s,
        r#"{"channel":"order_book_btcusd","event":"data","data":{"timestamp":"2","microtimestamp":"2000000","bids":[["102.00","1.0"]],"asks":[["101.00","1.0"]]}}"#,
        3,
    );
    assert!(rejected.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(SystemEvent::BookSnapshotRejected {
            instrument: InstrumentId(1),
            ..
        })
    )));
    assert!(!rejected.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(SystemEvent::BookInvalidated { .. })
    )));

    let recovered = drive(
        &mut s,
        r#"{"channel":"order_book_btcusd","event":"data","data":{"timestamp":"3","microtimestamp":"3000000","bids":[["102.00","1.0"]],"asks":[["103.00","1.0"]]}}"#,
        4,
    );
    assert!(
        markets(&recovered)
            .iter()
            .any(|event| matches!(event, MarketEvent::BookSnapshot(_)))
    );
}

#[test]
fn ohlc_rest_timer_fixture_exact_fixed() {
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
        .find(|(_, u)| u.contains("/ohlc/"))
        .expect("ohlc");
    assert!(
        url.contains("/ohlc/btcusd/") && url.contains("step=60"),
        "{url}"
    );
    assert!(out.as_slice().iter().any(|a| matches!(
        a, SessionAction::ScheduleTimer(t) if t.timer_id == CANDLE_TIMER_ID
            && t.fire_at.0 == 1 + CANDLE_POLL_INTERVAL_MS * 1_000_000
    )));
    let mut candle_out = ActionBuffer::new();
    s.on_input(SessionInput::HttpResponse {
        request_id: req_id,
        response: &HttpResponse {
            status: 200, headers: Vec::new(),
            body: Bytes::from_static(br#"{"data":{"pair":"BTC/USD","ohlc":[{"timestamp":"1609459200","open":"0.0010","high":"0.0025","low":"0.0015","close":"0.0020","volume":"1000"}]}}"#),
        },
        received: stamp(7),
    }, &mut candle_out).unwrap();
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
    assert!(http_ids(&tick).iter().any(|(_, u)| u.contains("/ohlc/")));
}

#[test]
fn ticker_stats_rest_timer_fixture_exact_fixed() {
    let mut s = session(false);
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
        .find(|(_, u)| u.contains("/ticker/"))
        .expect("ticker");
    assert!(url.contains("/ticker/btcusd/"), "{url}");
    assert!(out.as_slice().iter().any(|a| matches!(
        a, SessionAction::ScheduleTimer(t) if t.timer_id == STATS_TIMER_ID
            && t.fire_at.0 == 1 + STATS_POLL_INTERVAL_MS * 1_000_000
    )));
    let mut stats_out = ActionBuffer::new();
    s.on_input(
        SessionInput::HttpResponse {
            request_id: req_id,
            response: &HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(
                    br#"{"timestamp":"1609459200","open":"64000.00","open_24":"64100.00","high":"66000.50","low":"63000.25","last":"65000.12","volume":"12.5","vwap":"64500.00","bid":"65000.00","ask":"65000.10"}"#,
                ),
            },
            received: stamp(7),
        },
        &mut stats_out,
    )
    .unwrap();
    assert!(matches!(
        &markets(&stats_out)[0],
        MarketEvent::Statistics24h(st)
            if st.open.as_ref().unwrap().0 == Fixed::parse_str("64100.00").unwrap()
                && st.high.as_ref().unwrap().0 == Fixed::parse_str("66000.50").unwrap()
                && st.low.as_ref().unwrap().0 == Fixed::parse_str("63000.25").unwrap()
                && st.close.as_ref().unwrap().0 == Fixed::parse_str("65000.12").unwrap()
                && st.volume.as_ref().unwrap().0 == Fixed::parse_str("12.5").unwrap()
                && st.quote_volume.is_none()
    ));
    let fire_at = TimestampNs(1 + STATS_POLL_INTERVAL_MS * 1_000_000);
    let mut tick = ActionBuffer::new();
    s.on_input(
        SessionInput::Timer {
            timer_id: STATS_TIMER_ID,
            now: fire_at,
        },
        &mut tick,
    )
    .unwrap();
    assert!(http_ids(&tick).iter().any(|(_, u)| u.contains("/ticker/")));
    assert!(tick.as_slice().iter().any(|a| matches!(
        a, SessionAction::ScheduleTimer(t) if t.timer_id == STATS_TIMER_ID
    )));
}
