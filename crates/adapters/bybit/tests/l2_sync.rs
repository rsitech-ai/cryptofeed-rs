//! Offline L2: WS snapshot → contiguous delta → gap/control reconnect → fresh WS snapshot.

use marketfeed_adapter_api::{
    ActionBuffer, ConcreteSubscriptionSet, DisconnectReason, ReconnectReason, SessionAction,
    SessionCommand, SessionInput, SessionMachine, SessionSpec,
};
use marketfeed_adapter_bybit::{BybitCategory, BybitSession, BybitSessionConfig};
use marketfeed_engine::{SessionLifecycle, SessionRunner, SessionRunnerConfig};
use marketfeed_model::{
    CatalogVersion, CatalogView, FrameStamp, InstrumentId, MarketEvent, OverflowPolicy, SessionId,
    TimestampNs, VenueId,
};
use std::collections::HashMap;

fn stamp(n: i64) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(n),
        mono_ns: n as u64,
    }
}

fn l2_session() -> BybitSession {
    l2_session_for(BybitCategory::Linear, VenueId(5), "BTCUSDT")
}

fn l2_session_for(category: BybitCategory, venue: VenueId, symbol: &str) -> BybitSession {
    l2_session_for_depth(category, venue, symbol, 50)
}

fn l2_session_for_depth(
    category: BybitCategory,
    venue: VenueId,
    symbol: &str,
    depth: u32,
) -> BybitSession {
    let mut ids = HashMap::new();
    ids.insert(symbol.into(), InstrumentId(1));
    BybitSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(venue, CatalogVersion(1)),
        BybitSessionConfig {
            category,
            symbols: vec![symbol.into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2: true,
            l2_depth: depth,
            price_scale: 2,
            qty_scale: 3,
            ..BybitSessionConfig::default()
        },
    )
}

fn drive(s: &mut BybitSession, text: &str, ts: i64) -> ActionBuffer {
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

#[test]
fn ws_snapshot_then_deltas_go_live() {
    let mut s = l2_session();
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    out = drive(
        &mut s,
        r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100.00","1.0"],["99.50","1.0"]],"a":[["101.00","1.5"]],"u":10,"seq":100}}"#,
        2,
    );
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

    out = drive(
        &mut s,
        r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":2,"data":{"s":"BTCUSDT","b":[["100.00","1.2"]],"a":[],"u":11,"seq":101}}"#,
        3,
    );
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
}

#[test]
fn multi_level_delta_validates_only_the_final_book() {
    let mut session = l2_session();
    drive(
        &mut session,
        r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100.00","1.0"]],"a":[["101.00","1.0"]],"u":10,"seq":100}}"#,
        1,
    );

    let output = drive(
        &mut session,
        r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":2,"data":{"s":"BTCUSDT","b":[["102.00","1.0"]],"a":[["101.00","0"],["103.00","1.0"]],"u":11,"seq":101}}"#,
        2,
    );

    assert!(output.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitBatch(batch)
            if batch.events.iter().any(|event| matches!(
                &event.payload,
                MarketEvent::BookDelta(delta) if delta.changes.len() == 3
            ))
    )));
    assert!(!output.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(marketfeed_model::SystemEvent::BookInvalidated { .. })
    )));
}

#[test]
fn spot_ws_snapshot_then_delta_goes_live() {
    let mut s = l2_session_for(BybitCategory::Spot, VenueId(6), "BTCUSDT");
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();

    out = drive(
        &mut s,
        r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100.00","1.0"]],"a":[["101.00","1.5"]],"u":10,"seq":100}}"#,
        2,
    );
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

    out = drive(
        &mut s,
        r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":2,"data":{"s":"BTCUSDT","b":[["100.00","1.2"]],"a":[],"u":11,"seq":101}}"#,
        3,
    );
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
}

#[test]
fn stale_u_delta_discarded_without_reconnect() {
    let mut s = l2_session();
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
        r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100.00","1.0"]],"a":[["101.00","1.0"]],"u":10,"seq":100}}"#,
        2,
    );
    drive(
        &mut s,
        r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":2,"data":{"s":"BTCUSDT","b":[["100.00","1.2"]],"a":[],"u":11,"seq":101}}"#,
        3,
    );

    // u == last_u (11): duplicate, must be silently discarded.
    let out = drive(
        &mut s,
        r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":3,"data":{"s":"BTCUSDT","b":[["100.00","9.9"]],"a":[],"u":11,"seq":101}}"#,
        4,
    );
    assert!(
        out.as_slice().is_empty(),
        "stale u==last_u must emit nothing"
    );

    // u < last_u (5): also stale, must be silently discarded, no reconnect.
    let out = drive(
        &mut s,
        r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":4,"data":{"s":"BTCUSDT","b":[["100.00","9.9"]],"a":[],"u":5,"seq":50}}"#,
        5,
    );
    assert!(
        out.as_slice().is_empty(),
        "stale u<last_u must emit nothing"
    );

    // Book must still accept the correct next delta (u=12) afterward — not invalidated.
    let out = drive(
        &mut s,
        r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":5,"data":{"s":"BTCUSDT","b":[["100.00","1.3"]],"a":[],"u":12,"seq":102}}"#,
        6,
    );
    assert!(
        out.as_slice()
            .iter()
            .filter_map(|a| match a {
                SessionAction::EmitBatch(b) => Some(b),
                _ => None,
            })
            .flat_map(|b| &b.events)
            .any(|e| matches!(e.payload, MarketEvent::BookDelta(_))),
        "book should still be live and accept u=last_u+1 after discarding stale updates"
    );
}

#[test]
fn u_equals_one_forces_snapshot_reset() {
    let mut s = l2_session();
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
        r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100.00","1"]],"a":[["101.00","1"]],"u":50,"seq":1}}"#,
        2,
    );
    // Even if typed delta, u==1 resets as snapshot.
    out = drive(
        &mut s,
        r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":2,"data":{"s":"BTCUSDT","b":[["100.00","2"]],"a":[["101.00","2"]],"u":1,"seq":2}}"#,
        3,
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
fn control_resync_reconnects_for_fresh_ws_snapshot() {
    let mut s = l2_session();
    let mut out = ActionBuffer::new();
    s.on_input(
        SessionInput::Connected {
            now: TimestampNs(1),
        },
        &mut out,
    )
    .unwrap();
    out.clear();
    s.on_input(
        SessionInput::Control {
            command: &SessionCommand::Resync(InstrumentId(1)),
        },
        &mut out,
    )
    .unwrap();
    assert!(
        out.as_slice()
            .iter()
            .any(|action| matches!(action, SessionAction::Reconnect(ReconnectReason::Control)))
    );
    assert!(out.as_slice().iter().any(|action| matches!(
        action,
        SessionAction::EmitSystem(marketfeed_model::SystemEvent::BookInvalidated { .. })
    )));
    assert!(
        !out.as_slice()
            .iter()
            .any(|action| matches!(action, SessionAction::RequestHttp(_)))
    );
}

#[tokio::test]
async fn runner_ws_snapshot_then_live() {
    let mut runner = SessionRunner::new(
        Box::new(l2_session()),
        SessionRunnerConfig {
            session: SessionId(1),
            record: true,
            overflow: OverflowPolicy::FailEngine,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();
    runner.on_connected(TimestampNs(1)).unwrap();

    let mut snap =
        br#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100.00","1"]],"a":[["101.00","1"]],"u":10,"seq":100}}"#
            .to_vec();
    runner.on_text_frame(&mut snap, stamp(2)).unwrap();
    assert_eq!(runner.lifecycle, SessionLifecycle::Live);
    assert!(
        runner
            .market_batches
            .iter()
            .flat_map(|b| &b.events)
            .any(|e| matches!(e.payload, MarketEvent::BookSnapshot(_)))
    );
}

#[tokio::test]
async fn rejected_ws_snapshot_requests_transport_reconnect() {
    let mut runner = SessionRunner::new(
        Box::new(l2_session()),
        SessionRunnerConfig {
            session: SessionId(1),
            record: true,
            overflow: OverflowPolicy::FailEngine,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();
    runner.on_connected(TimestampNs(1)).unwrap();

    let mut crossed =
        br#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["102.00","1"]],"a":[["101.00","1"]],"u":10,"seq":100}}"#
            .to_vec();
    runner.on_text_frame(&mut crossed, stamp(2)).unwrap();

    assert!(runner.reconnect_requested);
    assert_ne!(runner.lifecycle, SessionLifecycle::Live);
}

#[tokio::test]
async fn control_resync_reconnects_and_waits_for_configured_depth_snapshot() {
    let session = l2_session_for_depth(BybitCategory::Linear, VenueId(5), "BTCUSDT", 200);
    let mut runner = SessionRunner::new(
        Box::new(session),
        SessionRunnerConfig {
            session: SessionId(1),
            record: true,
            overflow: OverflowPolicy::FailEngine,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();
    runner.on_connected(TimestampNs(1)).unwrap();
    let initial_subscriptions = runner.take_pending_writes();
    assert!(initial_subscriptions.iter().any(|frame| {
        String::from_utf8_lossy(&frame.payload).contains("orderbook.200.BTCUSDT")
    }));

    let mut initial_snapshot =
        br#"{"topic":"orderbook.200.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100.00","1"]],"a":[["101.00","1"]],"u":10,"seq":100}}"#
            .to_vec();
    runner
        .on_text_frame(&mut initial_snapshot, stamp(2))
        .unwrap();
    assert_eq!(runner.lifecycle, SessionLifecycle::Live);

    runner
        .deliver_control(SessionCommand::Resync(InstrumentId(1)), TimestampNs(3))
        .unwrap();
    assert!(runner.reconnect_requested);

    runner
        .on_transport_lost(DisconnectReason::RemoteClose, TimestampNs(4))
        .unwrap();
    runner.on_connected(TimestampNs(5)).unwrap();
    let reconnect_subscriptions = runner.take_pending_writes();
    assert!(reconnect_subscriptions.iter().any(|frame| {
        String::from_utf8_lossy(&frame.payload).contains("orderbook.200.BTCUSDT")
    }));

    let batches_before_delta = runner.market_batches.len();
    let mut premature_delta =
        br#"{"topic":"orderbook.200.BTCUSDT","type":"delta","ts":2,"data":{"s":"BTCUSDT","b":[["100.00","2"]],"a":[],"u":11,"seq":101}}"#
            .to_vec();
    runner
        .on_text_frame(&mut premature_delta, stamp(6))
        .unwrap();
    assert_eq!(runner.market_batches.len(), batches_before_delta);
    assert_ne!(runner.lifecycle, SessionLifecycle::Live);

    let mut replacement_snapshot =
        br#"{"topic":"orderbook.200.BTCUSDT","type":"snapshot","ts":3,"data":{"s":"BTCUSDT","b":[["100.00","2"]],"a":[["101.00","1"]],"u":20,"seq":200}}"#
            .to_vec();
    runner
        .on_text_frame(&mut replacement_snapshot, stamp(7))
        .unwrap();
    assert_eq!(runner.lifecycle, SessionLifecycle::Live);
}
