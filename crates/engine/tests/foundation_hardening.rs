//! Foundation hardening: no silent Drop*, bounded mirrors, Ping/Pong, metrics.

use std::sync::atomic::Ordering;

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, EventBatch, SessionAction, SessionInput, SessionMachine,
};
use marketfeed_engine::{EngineError, SessionRunner, SessionRunnerConfig};
use marketfeed_model::{FrameStamp, OverflowPolicy, SessionId, SystemEvent, TimestampNs};

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

struct PongMachine;

impl SessionMachine for PongMachine {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Pong { .. } => {
                output.push(SessionAction::SendText(Bytes::from_static(b"acked")));
            }
            SessionInput::BinaryFrame { bytes, .. } => {
                assert_eq!(bytes, b"bin");
                output.push(SessionAction::SendBinary(Bytes::from_static(b"echo")));
            }
            _ => {}
        }
        Ok(())
    }
}

fn stamp() -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(1),
        mono_ns: 1,
    }
}

#[test]
fn drop_newest_emits_events_dropped() {
    let mut runner = SessionRunner::new(
        Box::new(EmitN { n: 3 }),
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
    runner.on_text_frame(&mut bytes, stamp()).unwrap();

    let drops: Vec<_> = runner
        .system_events
        .iter()
        .filter_map(|e| match e {
            SystemEvent::EventsDropped { count, detail } => Some((*count, detail.clone())),
            _ => None,
        })
        .collect();
    assert!(
        !drops.is_empty(),
        "expected EventsDropped, got {:?}",
        runner.system_events
    );
    assert!(
        drops
            .iter()
            .any(|(c, d)| *c >= 1 && d.contains("DropNewest"))
    );
    assert!(runner.metrics.events_dropped.load(Ordering::Relaxed) >= 1);
    assert!(runner.metrics.queue_overflows.load(Ordering::Relaxed) >= 1);
}

#[test]
fn drop_oldest_emits_events_dropped() {
    let mut runner = SessionRunner::new(
        Box::new(EmitN { n: 3 }),
        SessionRunnerConfig {
            session: SessionId(1),
            dispatch_capacity: 1,
            overflow: OverflowPolicy::DropOldest,
            mirror_capacity: 8,
            record: false,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();

    let mut bytes = b"x".to_vec();
    runner.on_text_frame(&mut bytes, stamp()).unwrap();

    assert!(
        runner
            .system_events
            .iter()
            .any(|e| matches!(e, SystemEvent::EventsDropped { .. })),
        "expected EventsDropped"
    );
    assert!(runner.metrics.events_dropped.load(Ordering::Relaxed) >= 1);
}

#[test]
fn mirror_fail_engine_when_full() {
    let mut runner = SessionRunner::new(
        Box::new(EmitN { n: 2 }),
        SessionRunnerConfig {
            session: SessionId(1),
            dispatch_capacity: 8,
            overflow: OverflowPolicy::FailEngine,
            mirror_capacity: 1,
            record: false,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();

    let mut bytes = b"x".to_vec();
    let err = runner.on_text_frame(&mut bytes, stamp()).unwrap_err();
    assert!(matches!(err, EngineError::Dispatch(_)));
}

#[test]
fn mirror_capacity_zero_disables_mirrors() {
    let mut runner = SessionRunner::new(
        Box::new(EmitN { n: 2 }),
        SessionRunnerConfig {
            session: SessionId(1),
            dispatch_capacity: 8,
            overflow: OverflowPolicy::FailEngine,
            mirror_capacity: 0,
            record: false,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();

    let mut bytes = b"x".to_vec();
    runner.on_text_frame(&mut bytes, stamp()).unwrap();
    assert!(runner.market_batches.is_empty());
    assert_eq!(runner.metrics.events_dispatched.load(Ordering::Relaxed), 2);
}

#[test]
fn pong_and_binary_forwarded_to_machine() {
    let mut runner = SessionRunner::new(
        Box::new(PongMachine),
        SessionRunnerConfig {
            session: SessionId(1),
            record: false,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();

    runner.on_pong_frame(b"pong-payload", stamp()).unwrap();
    let writes = runner.take_pending_writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].payload, b"acked");

    let mut bin = b"bin".to_vec();
    runner.on_binary_frame(&mut bin, stamp()).unwrap();
    let writes = runner.take_pending_writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].payload, b"echo");

    assert_eq!(runner.metrics.frames_received.load(Ordering::Relaxed), 2);
}

#[test]
fn metrics_track_normalized_and_queue_occupancy() {
    let mut runner = SessionRunner::new(
        Box::new(EmitN { n: 2 }),
        SessionRunnerConfig {
            session: SessionId(1),
            dispatch_capacity: 8,
            record: false,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();

    let mut bytes = b"x".to_vec();
    runner.on_text_frame(&mut bytes, stamp()).unwrap();
    assert_eq!(runner.metrics.events_normalized.load(Ordering::Relaxed), 0);
    assert_eq!(runner.metrics.events_dispatched.load(Ordering::Relaxed), 2);
    assert_eq!(
        runner.metrics.batch_queue_occupancy.load(Ordering::Relaxed),
        2
    );
    assert_eq!(
        runner.metrics.batch_queue_capacity.load(Ordering::Relaxed),
        8
    );
    let text = runner.metrics.prometheus_text();
    assert!(text.contains("marketfeed_events_dispatched_total 2"));
}

struct FloodWrites {
    n: usize,
}

impl SessionMachine for FloodWrites {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if matches!(input, SessionInput::Connected { .. }) {
            for i in 0..self.n {
                output.push(SessionAction::SendText(Bytes::from(format!("w{i}"))));
            }
        }
        Ok(())
    }
}

#[test]
fn pending_writes_fail_engine_when_full() {
    let err = SessionRunner::new(
        Box::new(FloodWrites { n: 257 }),
        SessionRunnerConfig {
            session: SessionId(1),
            overflow: OverflowPolicy::FailEngine,
            record: false,
            mirror_capacity: 0,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap()
    .on_connected(TimestampNs(1))
    .unwrap_err();
    assert!(
        matches!(err, EngineError::Dispatch(_)),
        "expected dispatch FailEngine, got {err:?}"
    );
}

#[test]
fn pending_writes_drop_newest_emits_events_dropped() {
    let mut runner = SessionRunner::new(
        Box::new(FloodWrites { n: 260 }),
        SessionRunnerConfig {
            session: SessionId(1),
            overflow: OverflowPolicy::DropNewest,
            record: false,
            mirror_capacity: 1024,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();
    runner.on_connected(TimestampNs(1)).unwrap();
    assert_eq!(runner.take_pending_writes().len(), 256);
    assert!(
        runner.system_events.iter().any(|e| {
            matches!(
                e,
                SystemEvent::EventsDropped { detail, .. } if detail.contains("pending_writes")
            )
        }),
        "expected pending_writes EventsDropped"
    );
}

#[test]
fn action_buffer_drop_newest_emits_events_dropped() {
    // Capacity = max(dispatch_capacity * 4, DEFAULT_ACTION_BUFFER_CAPACITY=1024).
    let mut runner = SessionRunner::new(
        Box::new(EmitN { n: 1025 }),
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
    runner.on_text_frame(&mut bytes, stamp()).unwrap();

    let drops: Vec<_> = runner
        .system_events
        .iter()
        .filter_map(|e| match e {
            SystemEvent::EventsDropped { count, detail } => Some((*count, detail.clone())),
            _ => None,
        })
        .collect();
    assert!(
        drops
            .iter()
            .any(|(c, d)| *c >= 1 && d.contains("ActionBuffer") && d.contains("DropNewest")),
        "expected ActionBuffer EventsDropped, got {drops:?}"
    );
    assert!(
        runner
            .metrics
            .action_buffer_overflows
            .load(Ordering::Relaxed)
            >= 1
    );
    assert!(runner.metrics.events_dropped.load(Ordering::Relaxed) >= 1);
}
