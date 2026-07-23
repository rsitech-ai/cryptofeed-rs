//! Offline Coinbase L2: snapshot → l2update upsert/delete → MarkLive.

use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, ConcreteSubscriptionSet, SessionAction, SessionInput,
    SessionMachine, SessionSpec,
};
use marketfeed_adapter_coinbase::{CoinbaseSessionConfig, CoinbaseSpotSession};
use marketfeed_model::{
    BookOperation, BookSide, CatalogVersion, CatalogView, Fixed, FrameStamp, InstrumentId,
    MarketEvent, SessionId, TimestampNs, VenueId,
};
use std::collections::HashMap;

fn stamp(n: i64) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(n),
        mono_ns: n as u64,
    }
}

fn l2_session() -> CoinbaseSpotSession {
    let mut ids = HashMap::new();
    ids.insert("BTC-USD".into(), InstrumentId(1));
    CoinbaseSpotSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(16), CatalogVersion(1)),
        CoinbaseSessionConfig {
            products: vec!["BTC-USD".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2: true,
            price_scale: 2,
            qty_scale: 8,
            ..CoinbaseSessionConfig::default()
        },
    )
}

fn offline_l2_session() -> CoinbaseSpotSession {
    let mut ids = HashMap::new();
    ids.insert("BTC-USD".into(), InstrumentId(1));
    CoinbaseSpotSession::new(
        SessionSpec {
            endpoint_name: "offline-replay".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(16), CatalogVersion(1)),
        CoinbaseSessionConfig {
            products: vec!["BTC-USD".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2: true,
            price_scale: 2,
            qty_scale: 8,
            ..CoinbaseSessionConfig::default()
        },
    )
}

fn drive(s: &mut CoinbaseSpotSession, text: &str, ts: i64) -> ActionBuffer {
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

fn prime_offline_l2_decoder(s: &mut CoinbaseSpotSession) {
    let ack = r#"{"type":"subscriptions","channels":[{"name":"matches","product_ids":["BTC-USD"]},{"name":"ticker","product_ids":["BTC-USD"]},{"name":"heartbeat","product_ids":["BTC-USD"]},{"name":"status","product_ids":[]},{"name":"level2","product_ids":["BTC-USD"]}]}"#;
    let ack_actions = drive(s, ack, 1);
    assert!(
        ack_actions
            .as_slice()
            .iter()
            .all(|action| !matches!(action, SessionAction::MarkLive))
    );
}

#[test]
fn unauthenticated_l2_session_fails_before_wire_io() {
    let mut s = l2_session();
    let mut out = ActionBuffer::new();
    let err = s
        .on_input(
            SessionInput::Connected {
                now: TimestampNs(1),
            },
            &mut out,
        )
        .expect_err("Coinbase Exchange level2 requires credentials");
    assert!(
        matches!(err, AdapterError::Subscription(ref detail) if detail.contains("credentials")),
        "{err}"
    );
    assert!(out.as_slice().is_empty(), "{:?}", out.as_slice());
}

const SNAPSHOT: &str = r#"{"type":"snapshot","product_id":"BTC-USD","bids":[["101.10","1.5"],["101.00","2.0"]],"asks":[["101.20","3.0"],["101.30","0.5"]]}"#;

#[test]
fn snapshot_marks_live_and_emits_book() {
    let mut s = offline_l2_session();
    prime_offline_l2_decoder(&mut s);
    let out = drive(&mut s, SNAPSHOT, 2);
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::MarkLive))
    );
    assert!(
        out.as_slice()
            .iter()
            .filter_map(|a| match a {
                SessionAction::EmitBatch(b) => Some(b),
                _ => None,
            })
            .flat_map(|b| &b.events)
            .any(|e| matches!(e.payload, MarketEvent::BookSnapshot(_)))
    );
}

#[test]
fn l2update_upsert_and_delete_exact_fixed() {
    let mut s = offline_l2_session();
    prime_offline_l2_decoder(&mut s);
    let _ = drive(&mut s, SNAPSHOT, 2);

    let upd = r#"{"type":"l2update","product_id":"BTC-USD","time":"2014-11-07T08:19:27.028459Z","changes":[["buy","101.10","0"],["sell","101.25","1.25"]]}"#;
    let out = drive(&mut s, upd, 3);
    let delta = out
        .as_slice()
        .iter()
        .filter_map(|a| match a {
            SessionAction::EmitBatch(b) => Some(b),
            _ => None,
        })
        .flat_map(|b| &b.events)
        .find_map(|e| match &e.payload {
            MarketEvent::BookDelta(d) => Some(d.clone()),
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
