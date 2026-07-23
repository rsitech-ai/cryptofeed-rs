//! Offline: live loop null-sink drains dispatch so FailEngine does not trip without a consumer.

use marketfeed_adapter_api::ReconnectPolicy;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, EventBatch, SessionAction, SessionInput, SessionMachine,
};
use marketfeed_engine::{EngineSupervisor, SessionRunnerConfig};
use marketfeed_model::{OverflowPolicy, SessionId};
use marketfeed_transport::{MemoryWebSocket, WebSocketSpec};

struct EmitOne;

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
