//! Offline Kraken L2: WS snapshot → checksum-verified delta → checksum-mismatch reconnect.

use marketfeed_adapter_api::{
    ActionBuffer, ConcreteSubscriptionSet, ReconnectReason, SessionAction, SessionInput,
    SessionMachine, SessionSpec,
};
use marketfeed_adapter_kraken::{KrakenSessionConfig, KrakenSpotSession};
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

fn l2_session() -> KrakenSpotSession {
    let mut ids = HashMap::new();
    ids.insert("BTC/USD".into(), InstrumentId(1));
    KrakenSpotSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(7), CatalogVersion(1)),
        KrakenSessionConfig {
            symbols: vec!["BTC/USD".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2: true,
            price_scale: 1,
            qty_scale: 8,
            ..KrakenSessionConfig::default()
        },
    )
}

fn drive(s: &mut KrakenSpotSession, text: &str, ts: i64) -> ActionBuffer {
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

/// Golden BTC/USD book from Kraken's book-checksum-v2 guide (checksum 3310070434).
const SNAPSHOT: &str = r#"{
  "channel":"book","type":"snapshot",
  "data":[{"symbol":"BTC/USD",
    "bids":[
      {"price":"45283.5","qty":"0.10000000"},
      {"price":"45283.4","qty":"1.54582015"},
      {"price":"45282.1","qty":"0.10000000"},
      {"price":"45281.0","qty":"0.10000000"},
      {"price":"45280.3","qty":"1.54592586"},
      {"price":"45279.0","qty":"0.07990000"},
      {"price":"45277.6","qty":"0.03310103"},
      {"price":"45277.5","qty":"0.30000000"},
      {"price":"45277.3","qty":"1.54602737"},
      {"price":"45276.6","qty":"0.15445238"}
    ],
    "asks":[
      {"price":"45285.2","qty":"0.00100000"},
      {"price":"45286.4","qty":"1.54571953"},
      {"price":"45286.6","qty":"1.54571109"},
      {"price":"45289.6","qty":"1.54560911"},
      {"price":"45290.2","qty":"0.15890660"},
      {"price":"45291.8","qty":"1.54553491"},
      {"price":"45294.7","qty":"0.04454749"},
      {"price":"45296.1","qty":"0.35380000"},
      {"price":"45297.5","qty":"0.09945542"},
      {"price":"45299.5","qty":"0.18772827"}
    ],
    "checksum":3310070434,"timestamp":"2023-10-06T17:35:55.000000Z"}]
}"#;

fn connect(s: &mut KrakenSpotSession) {
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
}

#[test]
fn ws_snapshot_verifies_checksum_and_marks_ready() {
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
    assert!(!out.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::EmitSystem(SystemEvent::BookInvalidated { .. })
    )));
}

#[test]
fn bare_number_snapshot_preserves_lexical_checksum_precision() {
    let mut ids = HashMap::new();
    ids.insert("BTC/USD".into(), InstrumentId(1));
    let mut s = KrakenSpotSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(7), CatalogVersion(1)),
        KrakenSessionConfig {
            symbols: vec!["BTC/USD".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2: true,
            price_scale: 4,
            qty_scale: 4,
            ..KrakenSessionConfig::default()
        },
    );
    connect(&mut s);
    let out = drive(
        &mut s,
        r#"{
          "channel":"book","type":"snapshot",
          "data":[{"symbol":"BTC/USD",
            "bids":[{"price":1.2200,"qty":3.4500}],
            "asks":[{"price":1.2300,"qty":2.3400}],
            "checksum":1721120643}]
        }"#,
        2,
    );

    assert!(
        out.as_slice()
            .iter()
            .any(|action| matches!(action, SessionAction::MarkLive)),
        "{out:?}"
    );
    assert!(!out.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(SystemEvent::BookInvalidated { .. })
    )));
}

#[test]
fn update_with_correct_checksum_applies_and_emits_delta() {
    let mut s = l2_session();
    connect(&mut s);
    drive(&mut s, SNAPSHOT, 2);

    // Bid at 45283.5 changes qty 0.10000000 -> 0.20000000; checksum recomputed offline.
    let update = r#"{
      "channel":"book","type":"update",
      "data":[{"symbol":"BTC/USD",
        "bids":[{"price":"45283.5","qty":"0.20000000"}],
        "asks":[],
        "checksum":38355977,"timestamp":"2023-10-06T17:35:56.000000Z"}]
    }"#;
    let out = drive(&mut s, update, 3);
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
fn multi_level_update_is_validated_after_the_complete_message() {
    let mut s = l2_session();
    connect(&mut s);
    drive(
        &mut s,
        r#"{
          "channel":"book","type":"snapshot",
          "data":[{"symbol":"BTC/USD",
            "bids":[{"price":"100.0","qty":"1.00000000"}],
            "asks":[{"price":"101.0","qty":"1.00000000"}],
            "checksum":1838246772}]
        }"#,
        2,
    );

    let out = drive(
        &mut s,
        r#"{
          "channel":"book","type":"update",
          "data":[{"symbol":"BTC/USD",
            "bids":[
              {"price":"102.0","qty":"1.00000000"},
              {"price":"100.0","qty":"0.00000000"}
            ],
            "asks":[
              {"price":"101.0","qty":"0.00000000"},
              {"price":"103.0","qty":"1.00000000"}
            ],
            "checksum":3972406970}]
        }"#,
        3,
    );

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
fn checksum_mismatch_invalidates_and_reconnects() {
    let mut s = l2_session();
    connect(&mut s);
    drive(&mut s, SNAPSHOT, 2);

    // No-op update (empty bids/asks) but a deliberately wrong checksum.
    let bad_update = r#"{
      "channel":"book","type":"update",
      "data":[{"symbol":"BTC/USD","bids":[],"asks":[],
        "checksum":1,"timestamp":"2023-10-06T17:35:57.000000Z"}]
    }"#;
    let out = drive(&mut s, bad_update, 4);
    assert!(out.as_slice().iter().any(|a| matches!(
        a,
        SessionAction::EmitSystem(SystemEvent::ChecksumMismatch { .. })
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
