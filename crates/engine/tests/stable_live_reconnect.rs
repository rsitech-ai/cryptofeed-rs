use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, ReconnectPolicy, ReconnectReason, SessionAction, SessionInput,
    SessionMachine, TimerSpec,
};
use marketfeed_engine::{
    EngineMetrics, SessionRunner, SessionRunnerConfig, run_session_with_reconnect,
};
use marketfeed_model::TimestampNs;
use marketfeed_transport::{
    CloseReason, FrameBuffer, InboundFrame, OutboundFrame, StubHttpTransport, TransportError,
    WebSocketSpec, WebSocketTransport,
};

struct LiveOnConnect;

impl SessionMachine for LiveOnConnect {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if matches!(input, SessionInput::Connected { .. }) {
            output.push(SessionAction::MarkLive);
        }
        Ok(())
    }
}

struct StableThenClosed {
    connects: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
}

struct LiveWithSlowWrite;

impl SessionMachine for LiveWithSlowWrite {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if matches!(input, SessionInput::Connected { .. }) {
            output.push(SessionAction::MarkLive);
            output.push(SessionAction::SendText(Bytes::from_static(b"subscribe")));
        }
        Ok(())
    }
}

struct LiveUntilReconnectTimer;

impl SessionMachine for LiveUntilReconnectTimer {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Connected { now } => {
                output.push(SessionAction::MarkLive);
                output.push(SessionAction::ScheduleTimer(TimerSpec {
                    timer_id: 1,
                    fire_at: TimestampNs(now.0 + 15_000_000),
                }));
            }
            SessionInput::Timer { timer_id: 1, .. } => {
                output.push(SessionAction::Reconnect(ReconnectReason::Control));
            }
            _ => {}
        }
        Ok(())
    }
}

struct InitialFailureThenSlowWrite {
    connects: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
}

impl WebSocketTransport for InitialFailureThenSlowWrite {
    async fn connect(&mut self, _spec: &WebSocketSpec) -> Result<(), TransportError> {
        let connect = self.connects.fetch_add(1, Ordering::Relaxed) + 1;
        if connect == 1 {
            return Err(TransportError::Io("injected connect failure".into()));
        }
        if connect >= 3 {
            self.stop.store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn read_frame(
        &mut self,
        _buffer: &mut FrameBuffer,
    ) -> Result<InboundFrame, TransportError> {
        Err(TransportError::Closed)
    }

    async fn write_frame(&mut self, _frame: OutboundFrame) -> Result<(), TransportError> {
        tokio::time::sleep(Duration::from_millis(15)).await;
        Ok(())
    }

    async fn close(&mut self, _reason: CloseReason) -> Result<(), TransportError> {
        Ok(())
    }
}

struct InitialFailureThenPending {
    connects: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
}

impl WebSocketTransport for InitialFailureThenPending {
    async fn connect(&mut self, _spec: &WebSocketSpec) -> Result<(), TransportError> {
        let connect = self.connects.fetch_add(1, Ordering::Relaxed) + 1;
        if connect == 1 {
            return Err(TransportError::Io("injected connect failure".into()));
        }
        if connect >= 3 {
            self.stop.store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn read_frame(
        &mut self,
        _buffer: &mut FrameBuffer,
    ) -> Result<InboundFrame, TransportError> {
        std::future::pending().await
    }

    async fn write_frame(&mut self, _frame: OutboundFrame) -> Result<(), TransportError> {
        Ok(())
    }

    async fn close(&mut self, _reason: CloseReason) -> Result<(), TransportError> {
        Ok(())
    }
}

impl WebSocketTransport for StableThenClosed {
    async fn connect(&mut self, _spec: &WebSocketSpec) -> Result<(), TransportError> {
        self.connects.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn read_frame(
        &mut self,
        _buffer: &mut FrameBuffer,
    ) -> Result<InboundFrame, TransportError> {
        tokio::time::sleep(Duration::from_millis(15)).await;
        if self.connects.load(Ordering::Relaxed) >= 3 {
            self.stop.store(true, Ordering::Relaxed);
        }
        Err(TransportError::Closed)
    }

    async fn write_frame(&mut self, _frame: OutboundFrame) -> Result<(), TransportError> {
        Ok(())
    }

    async fn close(&mut self, _reason: CloseReason) -> Result<(), TransportError> {
        Ok(())
    }
}

#[tokio::test]
async fn stable_live_sessions_restore_reconnect_budget_before_remote_close() {
    let connects = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut transport = StableThenClosed {
        connects: Arc::clone(&connects),
        stop: Arc::clone(&stop),
    };
    let mut runner = SessionRunner::new(
        Box::new(LiveOnConnect),
        SessionRunnerConfig {
            record: false,
            stop_signal: Some(stop),
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();

    run_session_with_reconnect(
        &mut runner,
        &mut transport,
        &StubHttpTransport,
        &WebSocketSpec::default(),
        ReconnectPolicy {
            min_delay_ms: 1,
            max_delay_ms: 1,
            reset_after_live_ms: 5,
        },
        1,
    )
    .await
    .unwrap();

    assert_eq!(
        connects.load(Ordering::Relaxed),
        3,
        "each stable live interval should restore the reconnect budget"
    );
}

#[tokio::test]
async fn live_interval_includes_awaited_side_effect_time() {
    let connects = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut transport = InitialFailureThenSlowWrite {
        connects: Arc::clone(&connects),
        stop: Arc::clone(&stop),
    };
    let mut runner = SessionRunner::new(
        Box::new(LiveWithSlowWrite),
        SessionRunnerConfig {
            record: false,
            stop_signal: Some(stop),
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();

    run_session_with_reconnect(
        &mut runner,
        &mut transport,
        &StubHttpTransport,
        &WebSocketSpec::default(),
        ReconnectPolicy {
            min_delay_ms: 1,
            max_delay_ms: 1,
            reset_after_live_ms: 5,
        },
        1,
    )
    .await
    .unwrap();

    assert_eq!(connects.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn matured_live_interval_resets_budget_before_timer_reconnect() {
    let connects = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut transport = InitialFailureThenPending {
        connects: Arc::clone(&connects),
        stop: Arc::clone(&stop),
    };
    let metrics = Arc::new(EngineMetrics::default());
    let mut runner = SessionRunner::new(
        Box::new(LiveUntilReconnectTimer),
        SessionRunnerConfig {
            record: false,
            stop_signal: Some(stop),
            metrics: Some(Arc::clone(&metrics)),
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();

    run_session_with_reconnect(
        &mut runner,
        &mut transport,
        &StubHttpTransport,
        &WebSocketSpec::default(),
        ReconnectPolicy {
            min_delay_ms: 1,
            max_delay_ms: 1,
            reset_after_live_ms: 5,
        },
        1,
    )
    .await
    .unwrap();

    assert_eq!(connects.load(Ordering::Relaxed), 3);
    assert_eq!(
        metrics.reconnects.load(Ordering::Relaxed),
        1,
        "one executed timer reconnect must increment the metric once"
    );
}
