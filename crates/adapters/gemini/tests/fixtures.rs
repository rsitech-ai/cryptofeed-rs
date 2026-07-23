//! Offline fixtures for Gemini public trade / quote / L2.

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, CandleInterval, Channel, ConcreteSubscription, ConcreteSubscriptionSet,
    DeliveryOptions, HttpResponse, ReconnectReason, SessionAction, SessionInput, SessionMachine,
    SessionSpec,
};
use marketfeed_adapter_gemini::{
    CANDLE_POLL_INTERVAL_MS, CANDLE_TIMER_ID, GeminiSession, GeminiSessionConfig,
    STATS_POLL_INTERVAL_MS, STATS_TIMER_ID,
};
use marketfeed_model::{
    AggressorSide, CatalogVersion, CatalogView, EventEnvelope, Fixed, FrameStamp, InstrumentId,
    MarketEvent, SessionId, SystemEvent, TimestampNs, VenueId,
};
use std::collections::HashMap;

fn stamp(n: i64) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(n),
        mono_ns: n as u64,
    }
}

fn session(enable_l2: bool) -> GeminiSession {
    session_with(enable_l2, Vec::new())
}

fn session_with(enable_l2: bool, candle_intervals: Vec<CandleInterval>) -> GeminiSession {
    let mut ids = HashMap::new();
    ids.insert("BTCUSD".into(), InstrumentId(1));
    GeminiSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(15), CatalogVersion(1)),
        GeminiSessionConfig {
            symbols: vec!["BTCUSD".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2,
            candle_intervals,
            ..GeminiSessionConfig::default()
        },
    )
}

fn drive(s: &mut GeminiSession, text: &str, ts: i64) -> ActionBuffer {
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

fn envelopes(buf: &ActionBuffer) -> Vec<EventEnvelope> {
    buf.as_slice()
        .iter()
        .filter_map(|action| match action {
            SessionAction::EmitBatch(batch) => Some(batch),
            _ => None,
        })
        .flat_map(|batch| batch.events.iter().cloned())
        .collect()
}

#[test]
fn subscribes_current_public_streams() {
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
        sends.iter().any(|s| s.contains(r#""method":"SUBSCRIBE""#)),
        "{sends:?}"
    );
    assert!(
        sends.iter().any(|s| s.contains("btcusd@trade")),
        "{sends:?}"
    );
    assert!(
        sends.iter().any(|s| s.contains("btcusd@bookTicker")),
        "{sends:?}"
    );
    assert!(
        sends.iter().any(|s| s.contains("btcusd@depth@100ms")),
        "{sends:?}"
    );
    let payload: serde_json::Value = serde_json::from_str(&sends[0]).expect("subscription JSON");
    assert_eq!(payload["id"], "1");
    assert_eq!(payload["method"], "SUBSCRIBE");
    assert_eq!(
        payload["params"],
        serde_json::json!(["btcusd@trade", "btcusd@bookTicker", "btcusd@depth@100ms"])
    );
}

#[test]
fn explicit_trade_request_does_not_subscribe_unrequested_streams() {
    let mut ids = HashMap::new();
    ids.insert("BTCUSD".into(), InstrumentId(1));
    let mut s = GeminiSession::new(
        SessionSpec {
            endpoint_name: "wss://ws.gemini.com/?snapshot=-1".into(),
            subscriptions: ConcreteSubscriptionSet {
                items: vec![ConcreteSubscription {
                    instrument: InstrumentId(1),
                    channel: Channel::Trades,
                    delivery: DeliveryOptions::default(),
                }],
            },
        },
        CatalogView::new(VenueId(15), CatalogVersion(1)),
        GeminiSessionConfig {
            symbols: vec!["BTCUSD".into()],
            instrument_ids: ids,
            session: SessionId(1),
            poll_stats: false,
            ..GeminiSessionConfig::default()
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

    let subscribe = out
        .as_slice()
        .iter()
        .find_map(|action| match action {
            SessionAction::SendText(payload) => Some(String::from_utf8_lossy(payload).into_owned()),
            _ => None,
        })
        .expect("subscribe");
    assert!(subscribe.contains("btcusd@trade"), "{subscribe}");
    assert!(!subscribe.contains("bookTicker"), "{subscribe}");
    assert!(!subscribe.contains("@depth"), "{subscribe}");
    assert!(http_ids(&out).is_empty());
    assert!(!out.as_slice().iter().any(
        |action| matches!(action, SessionAction::ScheduleTimer(timer) if timer.timer_id == STATS_TIMER_ID)
    ));
}

fn requested_session(
    items: Vec<ConcreteSubscription>,
    symbols: &[(&str, InstrumentId)],
    candle_intervals: Vec<CandleInterval>,
) -> GeminiSession {
    let instrument_ids = symbols
        .iter()
        .map(|(symbol, id)| ((*symbol).to_string(), *id))
        .collect();
    let enable_l2 = items
        .iter()
        .any(|item| matches!(item.channel, Channel::L2Book { .. }));
    let poll_stats = items
        .iter()
        .any(|item| matches!(item.channel, Channel::Statistics24h));
    GeminiSession::new(
        SessionSpec {
            endpoint_name: "wss://ws.gemini.com/?snapshot=-1".into(),
            subscriptions: ConcreteSubscriptionSet { items },
        },
        CatalogView::new(VenueId(15), CatalogVersion(1)),
        GeminiSessionConfig {
            symbols: symbols
                .iter()
                .map(|(symbol, _)| (*symbol).to_string())
                .collect(),
            instrument_ids,
            session: SessionId(1),
            enable_l2,
            candle_intervals,
            poll_stats,
            ..GeminiSessionConfig::default()
        },
    )
}

#[test]
fn mixed_trade_and_l2_request_waits_only_for_the_requested_book() {
    let mut s = requested_session(
        vec![
            ConcreteSubscription {
                instrument: InstrumentId(2),
                channel: Channel::Trades,
                delivery: DeliveryOptions::default(),
            },
            ConcreteSubscription {
                instrument: InstrumentId(1),
                channel: Channel::L2Book {
                    depth: None,
                    cadence: None,
                },
                delivery: DeliveryOptions::default(),
            },
        ],
        &[("BTCUSD", InstrumentId(1)), ("ETHUSD", InstrumentId(2))],
        Vec::new(),
    );
    let mut connected = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut connected,
    )
    .unwrap();
    let out = drive(
        &mut s,
        r#"{"e":"depthUpdate","E":2000000000,"s":"btcusd","U":10,"u":10,"b":[["100.00","1.0"]],"a":[["101.00","1.0"]]}"#,
        2,
    );

    assert!(
        out.as_slice()
            .iter()
            .any(|action| matches!(action, SessionAction::MarkLive)),
        "an unrequested ETH depth snapshot must not block readiness: {out:?}"
    );
}

#[test]
fn candle_requests_preserve_instrument_interval_pairs() {
    let mut s = requested_session(
        vec![
            ConcreteSubscription {
                instrument: InstrumentId(2),
                channel: Channel::Candles {
                    interval: CandleInterval::M1,
                },
                delivery: DeliveryOptions::default(),
            },
            ConcreteSubscription {
                instrument: InstrumentId(1),
                channel: Channel::Candles {
                    interval: CandleInterval::M5,
                },
                delivery: DeliveryOptions::default(),
            },
        ],
        &[("BTCUSD", InstrumentId(1)), ("ETHUSD", InstrumentId(2))],
        vec![CandleInterval::M1, CandleInterval::M5],
    );
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let urls: Vec<_> = http_ids(&out).into_iter().map(|(_, url)| url).collect();

    assert_eq!(urls.len(), 2, "{urls:?}");
    assert!(urls.iter().any(|url| url.ends_with("/ethusd/1m")));
    assert!(urls.iter().any(|url| url.ends_with("/btcusd/5m")));
}

#[test]
fn statistics_requests_preserve_instrument_scope() {
    let mut s = requested_session(
        vec![
            ConcreteSubscription {
                instrument: InstrumentId(2),
                channel: Channel::Statistics24h,
                delivery: DeliveryOptions::default(),
            },
            ConcreteSubscription {
                instrument: InstrumentId(1),
                channel: Channel::Trades,
                delivery: DeliveryOptions::default(),
            },
        ],
        &[("BTCUSD", InstrumentId(1)), ("ETHUSD", InstrumentId(2))],
        Vec::new(),
    );
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let urls: Vec<_> = http_ids(&out).into_iter().map(|(_, url)| url).collect();

    assert_eq!(urls.len(), 2, "{urls:?}");
    assert!(urls.iter().all(|url| url.ends_with("/ethusd")));
}

#[test]
fn rest_only_session_marks_live_after_first_successful_requested_response() {
    let mut s = requested_session(
        vec![ConcreteSubscription {
            instrument: InstrumentId(1),
            channel: Channel::Candles {
                interval: CandleInterval::M1,
            },
            delivery: DeliveryOptions::default(),
        }],
        &[("BTCUSD", InstrumentId(1))],
        vec![CandleInterval::M1],
    );
    let mut connected = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut connected,
    )
    .unwrap();
    assert!(
        !connected
            .as_slice()
            .iter()
            .any(|action| matches!(action, SessionAction::MarkLive))
    );
    let request_id = http_ids(&connected)[0].0;
    let mut response = ActionBuffer::new();
    s.on_input(
        SessionInput::HttpResponse {
            request_id,
            response: &HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(br#"[[1609459200000,"1.0","2.0","0.5","1.5","10.0"]]"#),
            },
            received: stamp(7),
        },
        &mut response,
    )
    .unwrap();
    assert!(
        response
            .as_slice()
            .iter()
            .any(|action| matches!(action, SessionAction::MarkLive)),
        "{response:?}"
    );
}

#[test]
fn rest_failure_after_live_marks_session_degraded() {
    let mut s = requested_session(
        vec![ConcreteSubscription {
            instrument: InstrumentId(1),
            channel: Channel::Candles {
                interval: CandleInterval::M1,
            },
            delivery: DeliveryOptions::default(),
        }],
        &[("BTCUSD", InstrumentId(1))],
        vec![CandleInterval::M1],
    );
    let mut connected = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut connected,
    )
    .unwrap();
    let first_id = http_ids(&connected)[0].0;
    let mut success = ActionBuffer::new();
    s.on_input(
        SessionInput::HttpResponse {
            request_id: first_id,
            response: &HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(br#"[[1609459200000,"1.0","2.0","0.5","1.5","10.0"]]"#),
            },
            received: stamp(7),
        },
        &mut success,
    )
    .unwrap();
    assert!(
        success
            .as_slice()
            .iter()
            .any(|action| matches!(action, SessionAction::MarkLive))
    );
    let mut timer = ActionBuffer::new();
    s.on_input(
        SessionInput::Timer {
            timer_id: CANDLE_TIMER_ID,
            now: TimestampNs(2),
        },
        &mut timer,
    )
    .unwrap();
    let failed_id = http_ids(&timer)[0].0;
    let mut failure = ActionBuffer::new();
    s.on_input(
        SessionInput::HttpResponse {
            request_id: failed_id,
            response: &HttpResponse {
                status: 503,
                headers: Vec::new(),
                body: Bytes::new(),
            },
            received: stamp(8),
        },
        &mut failure,
    )
    .unwrap();

    assert!(
        failure
            .as_slice()
            .iter()
            .any(|action| matches!(action, SessionAction::MarkDegraded)),
        "{failure:?}"
    );
}

#[test]
fn trade_quote_exact_fixed_and_l2() {
    let mut s = session(true);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    let snap = r#"{"e":"depthUpdate","E":2000000000,"s":"btcusd","U":10,"u":10,"b":[["29000.12","1.50000000"]],"a":[["29001.00","2.00000000"]]}"#;
    out = drive(&mut s, snap, 2);
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::MarkLive))
    );
    let m = markets(&out);
    assert!(
        m.iter()
            .any(|event| matches!(event, MarketEvent::BookSnapshot(_)))
    );
    let snapshot_envelope = envelopes(&out)
        .into_iter()
        .find(|event| matches!(event.payload, MarketEvent::BookSnapshot(_)))
        .expect("snapshot envelope");
    assert_eq!(
        snapshot_envelope.exchange_ts,
        Some(TimestampNs(2_000_000_000))
    );
    assert_eq!(
        snapshot_envelope
            .source_sequence
            .map(|sequence| (sequence.first, sequence.last)),
        Some((10, 10))
    );

    let trade = r#"{"E":2100000000,"s":"btcusd","t":99,"p":"29000.12","q":"0.10000000","m":false}"#;
    out = drive(&mut s, trade, 3);
    let m = markets(&out);
    assert!(matches!(
        m.iter().find(|e| matches!(e, MarketEvent::Trade(_))),
        Some(MarketEvent::Trade(t))
            if t.aggressor == AggressorSide::Buy
                && t.price.0 == Fixed::parse_str("29000.12").unwrap()
                && t.quantity.0 == Fixed::parse_str("0.10000000").unwrap()
    ));
    let trade_envelope = envelopes(&out)
        .into_iter()
        .find(|event| matches!(event.payload, MarketEvent::Trade(_)))
        .expect("trade envelope");
    assert_eq!(trade_envelope.exchange_ts, Some(TimestampNs(2_100_000_000)));
    assert_eq!(trade_envelope.source_sequence, None);

    let quote = r#"{"u":11,"E":2200000000,"s":"btcusd","b":"29000.12","B":"1.50000000","a":"29001.00","A":"2.00000000"}"#;
    out = drive(&mut s, quote, 4);
    let m = markets(&out);
    assert!(matches!(
        m.iter().find(|e| matches!(e, MarketEvent::Quote(_))),
        Some(MarketEvent::Quote(q))
            if q.bid_price.0 == Fixed::parse_str("29000.12").unwrap()
                && q.ask_price.0 == Fixed::parse_str("29001.00").unwrap()
    ));
    let quote_envelope = envelopes(&out)
        .into_iter()
        .find(|event| matches!(event.payload, MarketEvent::Quote(_)))
        .expect("quote envelope");
    assert_eq!(quote_envelope.exchange_ts, Some(TimestampNs(2_200_000_000)));
    assert_eq!(
        quote_envelope
            .source_sequence
            .map(|sequence| (sequence.first, sequence.last)),
        Some((11, 11))
    );

    let delta = r#"{"e":"depthUpdate","E":2300000000,"s":"btcusd","U":11,"u":11,"b":[["29000.12","1.80000000"]],"a":[]}"#;
    out = drive(&mut s, delta, 5);
    assert!(
        markets(&out)
            .iter()
            .any(|e| matches!(e, MarketEvent::BookDelta(_)))
    );

    let del = r#"{"e":"depthUpdate","E":2400000000,"s":"btcusd","U":12,"u":12,"b":[],"a":[["29001.00","0"]]}"#;
    out = drive(&mut s, del, 6);
    assert!(matches!(
        markets(&out).iter().find(|e| matches!(e, MarketEvent::BookDelta(_))),
        Some(MarketEvent::BookDelta(d))
            if d.changes[0].operation == marketfeed_model::BookOperation::Delete
    ));
}

#[test]
fn multi_level_depth_update_is_validated_after_the_complete_message() {
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
        r#"{"e":"depthUpdate","E":1,"s":"btcusd","U":10,"u":10,"b":[["100.00","1.0"]],"a":[["101.00","1.0"]]}"#,
        2,
    );

    // The bid upsert crosses the old ask until later levels in this same
    // exchange update remove it and install the replacement ask.
    let out = drive(
        &mut s,
        r#"{"e":"depthUpdate","E":2,"s":"btcusd","U":11,"u":11,"b":[["102.00","1.0"]],"a":[["101.00","0"],["103.00","1.0"]]}"#,
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
            | SessionAction::Reconnect(_)
    )));
}

#[test]
fn quotes_without_l2_book_events() {
    let mut s = session(false);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let quote = r#"{"u":1,"E":2000000000,"s":"btcusd","b":"100.00","B":"1.00000000","a":"101.00","A":"1.00000000"}"#;
    out = drive(&mut s, quote, 2);
    assert!(
        out.as_slice()
            .iter()
            .any(|action| matches!(action, SessionAction::MarkLive))
    );
    assert!(
        markets(&out)
            .iter()
            .any(|e| matches!(e, MarketEvent::Quote(_)))
    );
    assert!(
        !markets(&out)
            .iter()
            .any(|e| matches!(e, MarketEvent::BookSnapshot(_) | MarketEvent::BookDelta(_)))
    );
}

#[test]
fn first_depth_frame_must_be_a_single_update_snapshot() {
    let mut s = session(true);
    let mut connected = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut connected,
    )
    .unwrap();

    let out = drive(
        &mut s,
        r#"{"e":"depthUpdate","E":2000000000,"s":"btcusd","U":10,"u":11,"b":[["100.00","1.0"]],"a":[["101.00","1.0"]]}"#,
        2,
    );

    assert!(markets(&out).is_empty());
    assert!(out.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(SystemEvent::BookInvalidated {
            instrument: InstrumentId(1),
            ..
        })
    )));
    assert!(
        out.as_slice()
            .iter()
            .any(|action| matches!(action, SessionAction::Reconnect(ReconnectReason::Protocol)))
    );
    assert!(
        !out.as_slice()
            .iter()
            .any(|action| matches!(action, SessionAction::MarkLive))
    );
}

#[test]
fn depth_gap_invalidates_book_and_reconnects() {
    let mut s = session(true);
    let mut connected = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut connected,
    )
    .unwrap();
    let _ = drive(
        &mut s,
        r#"{"e":"depthUpdate","E":2000000000,"s":"btcusd","U":10,"u":10,"b":[["100.00","1.0"]],"a":[["101.00","1.0"]]}"#,
        2,
    );

    let out = drive(
        &mut s,
        r#"{"e":"depthUpdate","E":2100000000,"s":"btcusd","U":12,"u":12,"b":[["100.00","2.0"]],"a":[]}"#,
        3,
    );

    assert!(markets(&out).is_empty());
    assert!(out.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(SystemEvent::SequenceGap {
            expected: 11,
            actual: 12
        })
    )));
    assert!(out.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(SystemEvent::BookInvalidated {
            instrument: InstrumentId(1),
            ..
        })
    )));
    assert!(out.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::Reconnect(ReconnectReason::SequenceGap)
    )));
}

#[test]
fn overlapping_depth_range_covering_next_update_is_accepted() {
    let mut s = session(true);
    let mut connected = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut connected,
    )
    .unwrap();
    let _ = drive(
        &mut s,
        r#"{"e":"depthUpdate","E":2000000000,"s":"btcusd","U":10,"u":10,"b":[["100.00","1.0"]],"a":[["101.00","1.0"]]}"#,
        2,
    );

    let out = drive(
        &mut s,
        r#"{"e":"depthUpdate","E":2100000000,"s":"btcusd","U":9,"u":11,"b":[["100.00","2.0"]],"a":[]}"#,
        3,
    );

    let envelope = envelopes(&out)
        .into_iter()
        .find(|event| matches!(event.payload, MarketEvent::BookDelta(_)))
        .expect("overlapping depth delta");
    assert_eq!(
        envelope
            .source_sequence
            .map(|sequence| (sequence.first, sequence.last)),
        Some((9, 11))
    );
    assert!(
        !out.as_slice()
            .iter()
            .any(|action| matches!(action, SessionAction::Reconnect(_)))
    );
}

#[test]
fn candles_rest_timer_fixture_exact_fixed() {
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
        .find(|(_, u)| u.contains("/candles/"))
        .expect("candle");
    assert!(url.contains("/candles/btcusd/1m"), "{url}");
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
                    br#"[[1609459200000,"0.0010","0.0025","0.0015","0.0020","1000"]]"#,
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
    assert!(http_ids(&tick).iter().any(|(_, u)| u.contains("/candles/")));
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
    let ids = http_ids(&out);
    let (v2_id, v2_url) = ids
        .iter()
        .find(|(_, u)| u.contains("/v2/ticker/"))
        .cloned()
        .expect("v2 ticker");
    let (pub_id, pub_url) = ids
        .iter()
        .find(|(_, u)| u.contains("/pubticker/"))
        .cloned()
        .expect("pubticker");
    assert!(v2_url.contains("/ticker/btcusd"), "{v2_url}");
    assert!(pub_url.contains("/pubticker/btcusd"), "{pub_url}");
    assert!(out.as_slice().iter().any(|a| matches!(
        a, SessionAction::ScheduleTimer(t) if t.timer_id == STATS_TIMER_ID
            && t.fire_at.0 == 1 + STATS_POLL_INTERVAL_MS * 1_000_000
    )));

    let mut stats_out = ActionBuffer::new();
    s.on_input(
        SessionInput::HttpResponse {
            request_id: v2_id,
            response: &HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(
                    br#"{"symbol":"BTCUSD","open":"64000.00","high":"66000.50","low":"63000.25","close":"65000.12","changes":[],"bid":"65000.00","ask":"65000.10"}"#,
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
            if st.open.as_ref().unwrap().0 == Fixed::parse_str("64000.00").unwrap()
                && st.close.as_ref().unwrap().0 == Fixed::parse_str("65000.12").unwrap()
                && st.volume.is_none()
    ));

    let mut vol_out = ActionBuffer::new();
    s.on_input(
        SessionInput::HttpResponse {
            request_id: pub_id,
            response: &HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(
                    br#"{"bid":"65000.00","ask":"65000.10","last":"65000.12","volume":{"BTC":"12.5","USD":"812500.00","timestamp":1609459200000}}"#,
                ),
            },
            received: stamp(8),
        },
        &mut vol_out,
    )
    .unwrap();
    assert!(matches!(
        &markets(&vol_out)[0],
        MarketEvent::Statistics24h(st)
            if st.open.as_ref().unwrap().0 == Fixed::parse_str("64000.00").unwrap()
                && st.high.as_ref().unwrap().0 == Fixed::parse_str("66000.50").unwrap()
                && st.low.as_ref().unwrap().0 == Fixed::parse_str("63000.25").unwrap()
                && st.close.as_ref().unwrap().0 == Fixed::parse_str("65000.12").unwrap()
                && st.volume.as_ref().unwrap().0 == Fixed::parse_str("12.5").unwrap()
                && st.quote_volume.as_ref().unwrap().0 == Fixed::parse_str("812500.00").unwrap()
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
    let tick_urls = http_ids(&tick);
    assert!(tick_urls.iter().any(|(_, u)| u.contains("/v2/ticker/")));
    assert!(tick_urls.iter().any(|(_, u)| u.contains("/pubticker/")));
    assert!(tick.as_slice().iter().any(|a| matches!(
        a, SessionAction::ScheduleTimer(t) if t.timer_id == STATS_TIMER_ID
    )));
}
