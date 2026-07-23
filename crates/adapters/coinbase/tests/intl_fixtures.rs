//! Fixture-driven Coinbase International auth MD T/Q/L2 (offline SessionMachine).

use marketfeed_adapter_api::{
    ActionBuffer, ConcreteSubscriptionSet, EventBatch, ReconnectReason, SessionAction,
    SessionInput, SessionMachine, SessionSpec,
};
use marketfeed_adapter_coinbase::{CoinbaseIntlSession, CoinbaseIntlSessionConfig};
use marketfeed_model::{
    AggressorSide, BookOperation, BookSide, CatalogVersion, CatalogView, Fixed, FrameStamp,
    InstrumentId, MarketEvent, SessionId, SystemEvent, TimestampNs, VenueId,
};
use std::collections::HashMap;

fn stamp(n: i64) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(n),
        mono_ns: n as u64,
    }
}

fn session(enable_l2: bool) -> CoinbaseIntlSession {
    let mut ids = HashMap::new();
    ids.insert("BTC-PERP".into(), InstrumentId(1));
    CoinbaseIntlSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(19), CatalogVersion(1)),
        CoinbaseIntlSessionConfig {
            products: vec!["BTC-PERP".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2,
            ..CoinbaseIntlSessionConfig::default()
        },
    )
}

fn drive_text(s: &mut CoinbaseIntlSession, text: &str, ts: i64) -> ActionBuffer {
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

fn source_sequences(buf: &ActionBuffer) -> Vec<Option<u64>> {
    buf.as_slice()
        .iter()
        .filter_map(|action| match action {
            SessionAction::EmitBatch(EventBatch { events, .. }) => Some(events),
            _ => None,
        })
        .flatten()
        .map(|event| event.source_sequence.map(|sequence| sequence.first))
        .collect()
}

fn sent_texts(buf: &ActionBuffer) -> Vec<String> {
    buf.as_slice()
        .iter()
        .filter_map(|a| match a {
            SessionAction::SendSensitiveText(b) => {
                Some(String::from_utf8_lossy(b.expose()).into_owned())
            }
            _ => None,
        })
        .collect()
}

#[test]
fn connect_sends_auth_subscribe_match_level1() {
    let mut s = session(false);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1_683_730_727_000_000_000),
        },
        &mut out,
    )
    .unwrap();
    let sent = sent_texts(&out);
    assert_eq!(sent.len(), 1, "{sent:?}");
    let sub = &sent[0];
    assert!(sub.contains("SUBSCRIBE"), "{sub}");
    assert!(sub.contains("MATCH"), "{sub}");
    assert!(sub.contains("LEVEL1"), "{sub}");
    assert!(sub.contains("BTC-PERP"), "{sub}");
    assert!(sub.contains("1683730727"), "{sub}");
    assert!(sub.contains("\"key\""), "{sub}");
    assert!(sub.contains("\"passphrase\""), "{sub}");
    assert!(sub.contains("\"signature\""), "{sub}");
    assert!(!sub.contains("LEVEL2"), "{sub}");
}

#[test]
fn connect_with_l2_includes_level2_channel() {
    let mut s = session(true);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1_683_730_727_000_000_000),
        },
        &mut out,
    )
    .unwrap();
    assert!(sent_texts(&out)[0].contains("LEVEL2"));
}

#[test]
fn match_emits_trade_and_marks_live() {
    let mut s = session(false);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let trade = r#"{"sequence":0,"product_id":"BTC-PERP","time":"2023-05-10T14:58:47.002Z","match_id":"177101110052388865","trade_qty":"0.006","aggressor_side":"BUY","trade_price":"28833.1","channel":"MATCH","type":"UPDATE"}"#;
    let buf = drive_text(&mut s, trade, 2);
    let m = markets(&buf);
    assert!(matches!(
        &m[0],
        MarketEvent::Trade(t)
            if t.aggressor == AggressorSide::Buy
                && t.price.0 == Fixed::parse_str("28833.1").unwrap()
    ));
    assert!(
        buf.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::MarkLive))
    );
}

#[test]
fn level1_emits_quote() {
    let mut s = session(false);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let quote = r#"{"sequence":1,"product_id":"BTC-PERP","time":"2023-05-10T14:58:47.547Z","bid_price":"28787.8","bid_qty":"0.466","ask_price":"28788.8","ask_qty":"1.566","channel":"LEVEL1","type":"UPDATE"}"#;
    let out = drive_text(&mut s, quote, 3);
    let m = markets(&out);
    assert!(
        matches!(&m[0], MarketEvent::Quote(q) if q.bid_price.0 == Fixed::parse_str("28787.8").unwrap())
    );
    assert_eq!(source_sequences(&out), vec![Some(1)]);
}

#[test]
fn level2_snapshot_then_delta() {
    let mut s = session(true);
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    let snap = r#"{"sequence":0,"product_id":"BTC-PERP","time":"2023-05-10T14:58:47.000Z","bids":[["29100","0.02"]],"asks":[["29267.8","18"]],"channel":"LEVEL2","type":"SNAPSHOT"}"#;
    let snap_out = drive_text(&mut s, snap, 2);
    assert!(
        snap_out
            .as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::MarkLive))
    );
    let upd = r#"{"sequence":1,"product_id":"BTC-PERP","time":"2023-05-10T14:58:47.375Z","changes":[["BUY","28787.7","6"]],"channel":"LEVEL2","type":"UPDATE"}"#;
    let delta = markets(&drive_text(&mut s, upd, 3))
        .into_iter()
        .find_map(|e| match e {
            MarketEvent::BookDelta(d) => Some(d),
            _ => None,
        })
        .expect("book delta");
    assert_eq!(delta.changes[0].side, BookSide::Bid);
    assert_eq!(delta.changes[0].operation, BookOperation::Upsert);
    assert_eq!(delta.checksum, None);
}

#[test]
fn session_global_sequence_gap_across_channels_reconnects_before_emitting() {
    let mut s = session(false);
    let mut connected = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut connected,
    )
    .unwrap();
    let trade = r#"{"sequence":10,"product_id":"BTC-PERP","time":"2023-05-10T14:58:47.002Z","match_id":"177101110052388865","trade_qty":"0.006","aggressor_side":"BUY","trade_price":"28833.1","channel":"MATCH","type":"UPDATE"}"#;
    assert_eq!(markets(&drive_text(&mut s, trade, 2)).len(), 1);

    let quote = r#"{"sequence":12,"product_id":"BTC-PERP","time":"2023-05-10T14:58:47.547Z","bid_price":"28787.8","bid_qty":"0.466","ask_price":"28788.8","ask_qty":"1.566","channel":"LEVEL1","type":"UPDATE"}"#;
    let gap = drive_text(&mut s, quote, 3);

    assert!(markets(&gap).is_empty(), "{gap:?}");
    assert!(gap.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(SystemEvent::SequenceGap {
            expected: 11,
            actual: 12
        })
    )));
    assert!(gap.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::Reconnect(ReconnectReason::SequenceGap)
    )));
}

#[test]
fn sequenced_data_channel_rejects_missing_sequence() {
    let mut s = session(false);
    let quote = r#"{"product_id":"BTC-PERP","time":"2023-05-10T14:58:47.547Z","bid_price":"28787.8","bid_qty":"0.466","ask_price":"28788.8","ask_qty":"1.566","channel":"LEVEL1","type":"UPDATE"}"#;

    let invalid = drive_text(&mut s, quote, 1);

    assert!(markets(&invalid).is_empty(), "{invalid:?}");
    assert!(invalid.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(SystemEvent::ParseError { detail })
            if detail.contains("missing session sequence")
    )));
    assert!(
        invalid
            .as_slice()
            .iter()
            .any(|action| matches!(action, SessionAction::Reconnect(ReconnectReason::Protocol)))
    );
}

#[test]
fn new_connection_starts_a_fresh_session_sequence() {
    let mut s = session(false);
    let trade = r#"{"sequence":10,"product_id":"BTC-PERP","time":"2023-05-10T14:58:47.002Z","match_id":"177101110052388865","trade_qty":"0.006","aggressor_side":"BUY","trade_price":"28833.1","channel":"MATCH","type":"UPDATE"}"#;
    assert_eq!(markets(&drive_text(&mut s, trade, 1)).len(), 1);

    let mut lifecycle = ActionBuffer::new();
    s.on_input(
        SessionInput::Disconnected {
            reason: marketfeed_adapter_api::DisconnectReason::RemoteClose,
            now: TimestampNs(2),
        },
        &mut lifecycle,
    )
    .unwrap();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(3),
        },
        &mut lifecycle,
    )
    .unwrap();

    let quote = r#"{"sequence":50,"product_id":"BTC-PERP","time":"2023-05-10T14:58:47.547Z","bid_price":"28787.8","bid_qty":"0.466","ask_price":"28788.8","ask_qty":"1.566","channel":"LEVEL1","type":"UPDATE"}"#;
    let fresh = drive_text(&mut s, quote, 4);
    assert_eq!(markets(&fresh).len(), 1, "{fresh:?}");
    assert!(!fresh.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(SystemEvent::SequenceGap { .. })
    )));
}

#[test]
fn one_sided_level1_advances_session_sequence_without_emitting_quote() {
    let mut s = session(false);
    let trade_10 = r#"{"sequence":10,"product_id":"BTC-PERP","time":"2023-05-10T14:58:47.002Z","match_id":"m10","trade_qty":"0.006","aggressor_side":"BUY","trade_price":"28833.1","channel":"MATCH","type":"UPDATE"}"#;
    assert_eq!(markets(&drive_text(&mut s, trade_10, 1)).len(), 1);

    let one_sided = r#"{"sequence":11,"product_id":"BTC-PERP","time":"2023-05-10T14:58:47.547Z","bid_price":"28787.8","bid_qty":"0.466","channel":"LEVEL1","type":"SNAPSHOT"}"#;
    let level1 = drive_text(&mut s, one_sided, 2);
    assert!(markets(&level1).is_empty(), "{level1:?}");
    assert!(
        !level1
            .as_slice()
            .iter()
            .any(|action| matches!(action, SessionAction::Reconnect(_)))
    );

    let trade_12 = r#"{"sequence":12,"product_id":"BTC-PERP","time":"2023-05-10T14:58:48.002Z","match_id":"m12","trade_qty":"0.007","aggressor_side":"SELL","trade_price":"28834.1","channel":"MATCH","type":"UPDATE"}"#;
    let contiguous = drive_text(&mut s, trade_12, 3);
    assert_eq!(markets(&contiguous).len(), 1, "{contiguous:?}");
    assert!(!contiguous.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(SystemEvent::SequenceGap { .. })
    )));
}

#[test]
fn documented_subscription_snapshot_is_acknowledged_without_sequence() {
    let mut s = session(false);
    let confirmation = r#"{"channels":[{"name":"MATCH","product_ids":["BTC-PERP"]}],"authenticated":true,"channel":"SUBSCRIPTIONS","type":"SNAPSHOT","time":"2023-05-30T16:53:46.847Z"}"#;

    let ack = drive_text(&mut s, confirmation, 1);

    assert!(ack.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(SystemEvent::SubscriptionStateChanged { state })
            if state == "subscribed"
    )));
    assert!(!ack.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(SystemEvent::UnknownMessage { .. })
    )));
}

#[test]
fn level2_sequence_gap_invalidates_book_before_reconnect() {
    let mut s = session(true);
    let snapshot = r#"{"sequence":10,"product_id":"BTC-PERP","time":"2023-05-10T14:58:47.000Z","bids":[["29100","0.02"]],"asks":[["29267.8","18"]],"channel":"LEVEL2","type":"SNAPSHOT"}"#;
    assert_eq!(markets(&drive_text(&mut s, snapshot, 1)).len(), 1);

    let skipped_delta = r#"{"sequence":12,"product_id":"BTC-PERP","time":"2023-05-10T14:58:47.375Z","changes":[["BUY","28787.7","6"]],"channel":"LEVEL2","type":"UPDATE"}"#;
    let gap = drive_text(&mut s, skipped_delta, 2);

    assert!(markets(&gap).is_empty(), "{gap:?}");
    assert!(gap.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(SystemEvent::BookInvalidated {
            instrument: InstrumentId(1),
            ..
        })
    )));
    assert!(gap.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::Reconnect(ReconnectReason::SequenceGap)
    )));
}
