//! Offline: live loop null-sink drains dispatch so FailEngine does not trip without a consumer.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use marketfeed_adapter_api::ReconnectPolicy;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, EventBatch, SessionAction, SessionInput, SessionMachine,
};
use marketfeed_engine::{
    EngineMetrics, EngineSupervisor, SessionRunner, SessionRunnerConfig, run_session_with_reconnect,
};
use marketfeed_model::{InstrumentId, OverflowPolicy, SessionId, SystemEvent};
use marketfeed_transport::{MemoryWebSocket, StubHttpTransport, WebSocketSpec};

struct EmitOne;
struct LiveBookThenFail {
    frames: usize,
}

impl SessionMachine for EmitOne {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if let SessionInput::TextFrame { .. } = input {
            output.push(SessionAction::EmitBatch(EventBatch {
                session: SessionId(1),
                frame_seq: 0,
                events: Vec::new(),
            }));
        }
        Ok(())
    }
}

impl SessionMachine for LiveBookThenFail {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if matches!(input, SessionInput::TextFrame { .. }) {
            self.frames += 1;
            if self.frames == 1 {
                output.push(SessionAction::EmitSystem(SystemEvent::BookResynchronized {
                    instrument: InstrumentId(7),
                }));
                output.push(SessionAction::MarkLive);
            } else {
                return Err(AdapterError::Parse("injected frame failure".into()));
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn live_loop_drains_dispatch_under_fail_engine() {
    let mut supervisor = EngineSupervisor::new();
    supervisor.mark_running();
    let session = SessionId(1);
    supervisor
        .insert_session(
            Box::new(EmitOne),
            SessionRunnerConfig {
                session,
                record: false,
                dispatch_capacity: 4,
                overflow: OverflowPolicy::FailEngine,
                // Production live path disables mirrors.
                mirror_capacity: 0,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    let mut ws = MemoryWebSocket::new();
    // More frames than dispatch_capacity; without per-frame drain this FailEngine-dies.
    for i in 0..20 {
        ws.push_text(format!("f{i}").into_bytes());
    }

    let policy = ReconnectPolicy {
        min_delay_ms: 1,
        max_delay_ms: 1,
        reset_after_live_ms: 1_000,
    };
    supervisor
        .run_session_loop_ws_only(
            session,
            &mut ws,
            &WebSocketSpec {
                url: "memory://drain".into(),
                ..WebSocketSpec::default()
            },
            policy,
            0,
        )
        .await
        .expect("live loop should drain dispatch and finish without FailEngine");

    let runner = supervisor.session_mut(session).unwrap();
    assert!(
        runner
            .metrics
            .events_dispatched
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 20,
        "all frames should have been normalized/dispatched"
    );
}

#[tokio::test]
async fn live_loop_error_invalidates_session_readiness_on_every_exit() {
    let metrics = Arc::new(EngineMetrics::new());
    let live = Arc::new(AtomicBool::new(false));
    let mut runner = SessionRunner::new(
        Box::new(LiveBookThenFail { frames: 0 }),
        SessionRunnerConfig {
            session: SessionId(9),
            record: false,
            metrics: Some(Arc::clone(&metrics)),
            live_signal: Some(Arc::clone(&live)),
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();
    let mut ws = MemoryWebSocket::new();
    ws.push_text(b"live".to_vec());
    ws.push_text(b"fail".to_vec());

    let error = run_session_with_reconnect(
        &mut runner,
        &mut ws,
        &StubHttpTransport,
        &WebSocketSpec {
            url: "memory://error-cleanup".into(),
            ..WebSocketSpec::default()
        },
        ReconnectPolicy {
            min_delay_ms: 1,
            max_delay_ms: 1,
            reset_after_live_ms: 1_000,
        },
        0,
    )
    .await
    .expect_err("the injected parser error must escape the live loop");

    assert!(error.to_string().contains("injected frame failure"));
    assert!(!live.load(Ordering::Relaxed));
    assert_eq!(metrics.valid_books.load(Ordering::Relaxed), 0);
}
