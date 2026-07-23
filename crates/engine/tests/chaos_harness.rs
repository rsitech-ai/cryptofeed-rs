//! Offline chaos harness (audit C7): malformed, snapshot-fail, slow-sink, clock/timer jump.
//!
//! Does not claim soak / live chaos — unit injection only.

use std::collections::HashMap;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, ConcreteSubscriptionSet, EventBatch, HttpResponse, SessionAction,
    SessionInput, SessionMachine, SessionSpec, TimerSpec,
};
use marketfeed_adapter_binance::{BinanceSessionConfig, BinanceSpotSession};
use marketfeed_engine::{
    SessionRunner, SessionRunnerConfig, WALL_CLOCK_JUMP_THRESHOLD_NS, wall_clock_jump_delta,
};
use marketfeed_model::{
    CatalogVersion, CatalogView, FrameStamp, InstrumentId, OverflowPolicy, SessionId, SystemEvent,
    TimestampNs, VenueId,
};
use marketfeed_recording::{Direction, EnqueueOutcome, FrameOpcode, PendingFrame, RecordingQueue};

fn stamp(n: i64) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(n),
        mono_ns: n as u64,
    }
}

fn spot_l2() -> BinanceSpotSession {
    let mut ids = HashMap::new();
    ids.insert("BTCUSDT".into(), InstrumentId(1));
    BinanceSpotSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(2), CatalogVersion(1)),
        BinanceSessionConfig {
            symbols: vec!["BTCUSDT".into()],
            instrument_ids: ids,
            session: SessionId(1),
            enable_l2: true,
            price_scale: 2,
            qty_scale: 8,
            ..BinanceSessionConfig::default()
        },
    )
}

struct EmitN {
    n: u64,
}

impl SessionMachine for EmitN {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if let SessionInput::TextFrame { .. } = input {
            for i in 0..self.n {
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: SessionId(1),
                    frame_seq: i,
                    events: Vec::new(),
                }));
            }
        }
        Ok(())
    }
}

struct TimerJumpMachine;

impl SessionMachine for TimerJumpMachine {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Connected { now } => {
                output.push(SessionAction::ScheduleTimer(TimerSpec {
                    timer_id: 1,
                    fire_at: TimestampNs(now.0 + 1_000),
                }));
                Ok(())
            }
            SessionInput::Timer { .. } => {
                output.push(SessionAction::EmitSystem(SystemEvent::HeartbeatMissed));
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[test]
fn malformed_frame_emits_parse_error_not_panic() {
    let mut runner = SessionRunner::new(
        Box::new(spot_l2()),
        SessionRunnerConfig {
            session: SessionId(1),
            record: false,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();
    runner.on_connected(TimestampNs(1)).unwrap();

    let mut junk = b"{not-json!!!!".to_vec();
    runner.on_text_frame(&mut junk, stamp(2)).unwrap();

    assert!(
        runner
            .system_events
            .iter()
            .any(|e| matches!(e, SystemEvent::ParseError { .. })),
        "expected ParseError, got {:?}",
        runner.system_events
    );
    assert!(runner.metrics.parse_failures.load(Ordering::Relaxed) >= 1);
}

#[test]
fn snapshot_http_fail_requests_reconnect() {
    let mut runner = SessionRunner::new(
        Box::new(spot_l2()),
        SessionRunnerConfig {
            session: SessionId(1),
            record: false,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();
    runner.on_connected(TimestampNs(1)).unwrap();
    let snap = runner
        .take_pending_http()
        .into_iter()
        .find(|r| r.url.contains("/depth"))
        .expect("depth snapshot request");

    let resp = HttpResponse {
        status: 500,
        headers: Vec::new(),
        body: Bytes::from_static(b"nope"),
    };
    runner.on_http_response(snap.id, &resp, stamp(2)).unwrap();

    assert!(
        runner.reconnect_requested,
        "snapshot HTTP fail must request reconnect"
    );
    assert!(
        runner.system_events.iter().any(|e| {
            matches!(e, SystemEvent::ParseError { detail } if detail.contains("HTTP 500"))
        }),
        "expected ParseError for HTTP 500, got {:?}",
        runner.system_events
    );
}

#[test]
fn slow_sink_overflow_emits_events_dropped() {
    let mut runner = SessionRunner::new(
        Box::new(EmitN { n: 4 }),
        SessionRunnerConfig {
            session: SessionId(1),
            dispatch_capacity: 1,
            overflow: OverflowPolicy::DropNewest,
            mirror_capacity: 8,
            record: false,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();
    let mut bytes = b"x".to_vec();
    runner.on_text_frame(&mut bytes, stamp(1)).unwrap();
    assert!(
        runner
            .system_events
            .iter()
            .any(|e| matches!(e, SystemEvent::EventsDropped { .. })),
        "expected EventsDropped from slow dispatch sink"
    );
    assert!(runner.metrics.events_dropped.load(Ordering::Relaxed) >= 1);

    let mut q = RecordingQueue::new(1, OverflowPolicy::DropOldest);
    let frame = |seq| PendingFrame {
        session: SessionId(1),
        frame_seq: seq,
        receive_ts_ns: 0,
        monotonic_ns: 0,
        direction: Direction::Inbound,
        opcode: FrameOpcode::Text,
        flags: 0,
        payload: vec![b'x'],
    };
    assert!(matches!(q.push(frame(1)), Ok(EnqueueOutcome::Accepted)));
    let outcome = q.push(frame(2)).unwrap();
    let evs = RecordingQueue::overflow_events(outcome, q.dropped_total);
    assert!(
        evs.iter()
            .any(|e| matches!(e, SystemEvent::EventsDropped { .. })),
        "recording overflow must surface EventsDropped"
    );
}

#[test]
fn wall_clock_jump_detection() {
    let prev = TimestampNs(1_000_000_000);
    assert!(
        wall_clock_jump_delta(
            prev,
            TimestampNs(prev.0 + 2_000_000_000),
            2_000_000_000,
            WALL_CLOCK_JUMP_THRESHOLD_NS
        )
        .is_none()
    );
    let forward = wall_clock_jump_delta(
        prev,
        TimestampNs(prev.0 + 3_000_000_000),
        2_000_000_000,
        WALL_CLOCK_JUMP_THRESHOLD_NS,
    );
    assert_eq!(forward, Some(WALL_CLOCK_JUMP_THRESHOLD_NS));
    let backward = wall_clock_jump_delta(
        prev,
        TimestampNs(prev.0 + 1_000_000_000),
        2_000_000_000,
        WALL_CLOCK_JUMP_THRESHOLD_NS,
    );
    assert_eq!(backward, Some(-WALL_CLOCK_JUMP_THRESHOLD_NS));
}

#[test]
fn timer_survives_large_now_jump() {
    let mut runner = SessionRunner::new(
        Box::new(TimerJumpMachine),
        SessionRunnerConfig {
            session: SessionId(1),
            record: false,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();
    runner.on_connected(TimestampNs(0)).unwrap();
    assert_eq!(runner.timer_count(), 1);

    runner.poll_timers(TimestampNs(10_000_000_000)).unwrap();
    assert!(
        runner
            .system_events
            .iter()
            .any(|e| matches!(e, SystemEvent::HeartbeatMissed)),
        "timer must fire after large now jump, got {:?}",
        runner.system_events
    );
}

#[test]
fn snapshot_bad_body_emits_parse_error() {
    let mut runner = SessionRunner::new(
        Box::new(spot_l2()),
        SessionRunnerConfig {
            session: SessionId(1),
            record: false,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();
    runner.on_connected(TimestampNs(1)).unwrap();
    let snap = runner
        .take_pending_http()
        .into_iter()
        .find(|r| r.url.contains("/depth"))
        .expect("depth");

    let resp = HttpResponse {
        status: 200,
        headers: Vec::new(),
        body: Bytes::from_static(br#"{"oops":true}"#),
    };
    runner.on_http_response(snap.id, &resp, stamp(2)).unwrap();
    assert!(
        runner.system_events.iter().any(|e| {
            matches!(e, SystemEvent::ParseError { detail } if detail.contains("bad depth"))
        }),
        "expected bad snapshot ParseError, got {:?}",
        runner.system_events
    );
}
