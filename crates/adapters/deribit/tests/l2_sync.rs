//! Offline Deribit L2: WS snapshot → change_id-verified delta → gap reconnect.

use marketfeed_adapter_api::{
    ActionBuffer, ConcreteSubscriptionSet, ReconnectReason, SessionAction, SessionInput,
    SessionMachine, SessionSpec,
};
use marketfeed_adapter_deribit::{DeribitSession, DeribitSessionConfig};
use marketfeed_model::{
    CatalogVersion, CatalogView, FrameStamp, InstrumentId, MarketEvent, SessionId, SystemEvent,
    TimestampNs, VenueId,
};
use std::collections::HashMap;

fn stamp(n: i64) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(n),
        mono_ns: n as u64,
    }
}

fn l2_session() -> DeribitSession {
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
            enable_l2: true,
            price_scale: 1,
            qty_scale: 0,
            ..DeribitSessionConfig::default()
        },
    )
}

fn drive(s: &mut DeribitSession, text: &str, ts: i64) -> ActionBuffer {
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

fn connect(s: &mut DeribitSession) {
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
}

const SNAPSHOT: &str = r#"{
  "jsonrpc":"2.0","method":"subscription",
  "params":{"channel":"book.BTC-PERPETUAL.100ms","data":{
    "type":"snapshot","timestamp":1554373962454,"instrument_name":"BTC-PERPETUAL",
    "change_id":297217,
    "bids":[["new",5042.3,30],["new",5041.9,20]],
    "asks":[["new",5042.6,40],["new",5043.3,40]]
  }}
}"#;

#[test]
fn ws_snapshot_marks_ready_and_emits_book_snapshot() {
    let mut s = l2_session();
    connect(&mut s);
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
fn change_with_matching_prev_change_id_applies_delta() {
    let mut s = l2_session();
    connect(&mut s);
    drive(&mut s, SNAPSHOT, 2);

    let change = r#"{
      "jsonrpc":"2.0","method":"subscription",
      "params":{"channel":"book.BTC-PERPETUAL.100ms","data":{
        "timestamp":1554373962554,"instrument_name":"BTC-PERPETUAL",
        "prev_change_id":297217,"change_id":297218,
        "bids":[["change",5042.3,9]],
        "asks":[["delete",5043.3,0]]
      }}
    }"#;
    let out = drive(&mut s, change, 3);
    assert!(
        out.as_slice()
            .iter()
            .filter_map(|a| match a {
                SessionAction::EmitBatch(b) => Some(b),
                _ => None,
            })
            .flat_map(|b| &b.events)
            .any(|e| matches!(e.payload, MarketEvent::BookDelta(_)))
    );
    assert!(!out.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::EmitSystem(SystemEvent::BookInvalidated { .. })
    )));
}

#[test]
fn multi_level_change_is_validated_after_the_complete_message() {
    let mut s = l2_session();
    connect(&mut s);
    let snapshot = r#"{
      "jsonrpc":"2.0","method":"subscription",
      "params":{"channel":"book.BTC-PERPETUAL.100ms","data":{
        "type":"snapshot","timestamp":1,"instrument_name":"BTC-PERPETUAL",
        "change_id":10,
        "bids":[["new",100.0,1]],
        "asks":[["new",101.0,1]]
      }}
    }"#;
    drive(&mut s, snapshot, 2);

    // The bid upsert crosses the old ask until the same wire message removes
    // that ask and installs the replacement.
    let change = r#"{
      "jsonrpc":"2.0","method":"subscription",
      "params":{"channel":"book.BTC-PERPETUAL.100ms","data":{
        "timestamp":2,"instrument_name":"BTC-PERPETUAL",
        "prev_change_id":10,"change_id":11,
        "bids":[["new",102.0,1]],
        "asks":[["delete",101.0,0],["new",103.0,1]]
      }}
    }"#;
    let out = drive(&mut s, change, 3);

    assert!(
        out.as_slice()
            .iter()
            .filter_map(|action| match action {
                SessionAction::EmitBatch(batch) => Some(batch),
                _ => None,
            })
            .flat_map(|batch| &batch.events)
            .any(|event| matches!(event.payload, MarketEvent::BookDelta(_)))
    );
    assert!(!out.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(SystemEvent::BookInvalidated { .. })
            | SessionAction::Reconnect(_)
    )));
}

#[test]
fn change_id_gap_invalidates_and_reconnects() {
    let mut s = l2_session();
    connect(&mut s);
    drive(&mut s, SNAPSHOT, 2);

    // prev_change_id (999999) doesn't match the snapshot's change_id (297217).
    let change = r#"{
      "jsonrpc":"2.0","method":"subscription",
      "params":{"channel":"book.BTC-PERPETUAL.100ms","data":{
        "timestamp":1554373962554,"instrument_name":"BTC-PERPETUAL",
        "prev_change_id":999999,"change_id":999999,
        "bids":[],"asks":[]
      }}
    }"#;
    let out = drive(&mut s, change, 3);
    assert!(out.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::EmitSystem(SystemEvent::SequenceGap { .. })
    )));
    assert!(out.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::EmitSystem(SystemEvent::BookInvalidated { .. })
    )));
    assert!(
        out.as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::Reconnect(ReconnectReason::SequenceGap)))
    );
}
