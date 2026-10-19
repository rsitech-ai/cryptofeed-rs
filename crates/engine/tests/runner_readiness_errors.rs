//! Stateful runner callbacks must revoke admission on errors, including errors
//! after a successful adapter callback. These tests use no filesystem backend.
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
};

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, DisconnectReason, HttpResponse, SessionAction, SessionCommand,
    SessionInput, SessionMachine, SubscriptionWireAction, TimerSpec,
};
use marketfeed_engine::{EngineError, SessionLifecycle, SessionRunner, SessionRunnerConfig};
use marketfeed_model::{FrameStamp, InstrumentId, SystemEvent, TimestampNs};
use marketfeed_recording::{MAX_RAW_RECORD_LEN, RecordingError};

const NORMAL: u8 = 0;
const ADAPTER_ERROR: u8 = 1;
const OUTBOUND_RECORD_ERROR: u8 = 2;
const NO_ADMISSION: u8 = 3;
const TIMER: u64 = 17;

struct ScriptedMachine {
    mode: Arc<AtomicU8>,
    calls: Arc<AtomicUsize>,
}

impl SessionMachine for ScriptedMachine {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        actions: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        match self.mode.load(Ordering::Relaxed) {
            ADAPTER_ERROR => {
                actions.push(SessionAction::MarkLive);
                actions.push(SessionAction::SendText(Bytes::from_static(b"uncommitted")));
                return Err(AdapterError::Protocol("injected adapter error".into()));
            }
            OUTBOUND_RECORD_ERROR => {
                actions.push(SessionAction::MarkLive);
                // The bounded raw writer rejects this record before writing it.
                // MarkLive has already been applied when the error is returned.
                actions.push(SessionAction::SendText(Bytes::from(vec![
                    b'x';
                    MAX_RAW_RECORD_LEN
                        as usize
                ])));
            }
            NO_ADMISSION => {}
            NORMAL => {
                if matches!(input, SessionInput::Connected { .. }) {
                    actions.push(SessionAction::EmitSystem(SystemEvent::BookResynchronized {
                        instrument: InstrumentId(1),
                    }));
                    actions.push(SessionAction::MarkLive);
                    actions.push(SessionAction::ScheduleTimer(TimerSpec {
                        timer_id: TIMER,
                        fire_at: TimestampNs(10),
                    }));
                }
            }
            _ => unreachable!("test mode"),
        }
        Ok(())
    }
}

fn ready_runner() -> (
    SessionRunner,
    Arc<AtomicBool>,
    Arc<AtomicU8>,
    Arc<AtomicUsize>,
) {
    let live = Arc::new(AtomicBool::new(false));
    let mode = Arc::new(AtomicU8::new(NORMAL));
    let calls = Arc::new(AtomicUsize::new(0));
    let runner = admitted_runner(
        Box::new(ScriptedMachine {
            mode: Arc::clone(&mode),
            calls: Arc::clone(&calls),
        }),
        Arc::clone(&live),
    );
    (runner, live, mode, calls)
}

fn admitted_runner(machine: Box<dyn SessionMachine>, live: Arc<AtomicBool>) -> SessionRunner {
    let mut runner = SessionRunner::new(
        machine,
        SessionRunnerConfig {
            live_signal: Some(Arc::clone(&live)),
            ..Default::default()
        },
    )
    .unwrap();
    runner.on_connected(TimestampNs(1)).unwrap();
    assert!(live.load(Ordering::Relaxed));
    assert_eq!(runner.metrics.valid_books.load(Ordering::Relaxed), 1);
    runner.drain_dispatch();
    runner.market_batches.clear();
    runner.system_events.clear();
    runner.other_actions.clear();
    assert!(runner.take_pending_writes().is_empty());
    runner
}

fn stamp() -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(20),
        mono_ns: 20,
    }
}

fn assert_revoked(runner: &SessionRunner, live: &AtomicBool) {
    assert!(!live.load(Ordering::Relaxed));
    assert_ne!(runner.lifecycle, SessionLifecycle::Live);
    assert_eq!(runner.metrics.valid_books.load(Ordering::Relaxed), 0);
}

#[derive(Clone, Copy, Debug)]
enum Callback {
    Connected,
    Disconnected,
    TransportLost,
    Text,
    Binary,
    Pong,
    Http,
    Timer,
    Control,
}

impl Callback {
    fn invoke(self, runner: &mut SessionRunner) -> Result<(), EngineError> {
        match self {
            Self::Connected => runner.on_connected(TimestampNs(20)),
            Self::Disconnected => {
                runner.on_disconnected(DisconnectReason::RemoteClose, TimestampNs(20))
            }
            Self::TransportLost => {
                runner.on_transport_lost(DisconnectReason::TransportError, TimestampNs(20))
            }
            Self::Text => runner.on_text_frame(&mut b"text".to_vec(), stamp()),
            Self::Binary => runner.on_binary_frame(&mut b"binary".to_vec(), stamp()),
            Self::Pong => runner.on_pong_frame(b"pong", stamp()),
            Self::Http => runner.on_http_response(
                1,
                &HttpResponse {
                    status: 200,
                    headers: Vec::new(),
                    body: Bytes::from_static(b"response"),
                },
                stamp(),
            ),
            Self::Timer => runner.poll_timers(TimestampNs(20)),
            Self::Control => {
                runner.deliver_control(SessionCommand::Resync(InstrumentId(1)), TimestampNs(20))
            }
        }
    }
}

#[test]
fn every_adapter_input_error_discards_partial_actions_and_revokes_previous_admission() {
    for callback in [
        Callback::Connected,
        Callback::Disconnected,
        Callback::TransportLost,
        Callback::Text,
        Callback::Binary,
        Callback::Pong,
        Callback::Http,
        Callback::Timer,
        Callback::Control,
    ] {
        let (mut runner, live, mode, calls) = ready_runner();
        mode.store(ADAPTER_ERROR, Ordering::Relaxed);
        let error = callback.invoke(&mut runner).unwrap_err();
        assert!(
            matches!(error, EngineError::Adapter(AdapterError::Protocol(ref message))
                if message == "injected adapter error"),
            "{callback:?}: {error}"
        );
        assert_eq!(calls.load(Ordering::Relaxed), 2, "{callback:?}");
        assert_revoked(&runner, &live);
        assert!(runner.take_pending_writes().is_empty(), "{callback:?}");
        assert!(runner.other_actions.is_empty(), "{callback:?}");
        assert_eq!(runner.dispatch_pending(), (0, 0), "{callback:?}");
    }
}

#[test]
fn failure_after_mark_live_revokes_the_admission_from_the_same_callback() {
    let (mut runner, live, mode, calls) = ready_runner();
    mode.store(OUTBOUND_RECORD_ERROR, Ordering::Relaxed);
    let error = runner
        .on_text_frame(&mut b"trigger".to_vec(), stamp())
        .unwrap_err();
    assert!(matches!(
        error,
        EngineError::Recording(RecordingError::RecordTooLarge { .. })
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    assert!(
        runner
            .other_actions
            .iter()
            .any(|action| matches!(action, SessionAction::MarkLive)),
        "the failure must occur after MarkLive was actually applied"
    );
    assert_revoked(&runner, &live);
    assert!(runner.take_pending_writes().is_empty());
}

#[test]
fn raw_recording_failures_revoke_readiness_before_any_adapter_callback() {
    let mut oversized = vec![b'x'; MAX_RAW_RECORD_LEN as usize];
    for kind in ["text", "binary", "pong", "ping", "close"] {
        let (mut runner, live, _mode, calls) = ready_runner();
        let error = match kind {
            "text" => runner.on_text_frame(&mut oversized, stamp()),
            "binary" => runner.on_binary_frame(&mut oversized, stamp()),
            "pong" => runner.on_pong_frame(&oversized, stamp()),
            "ping" => runner.on_ping_frame(&oversized, stamp()),
            "close" => runner.on_close_frame(&oversized, stamp()),
            _ => unreachable!(),
        }
        .unwrap_err();
        assert!(
            matches!(
                error,
                EngineError::Recording(RecordingError::RecordTooLarge { .. })
            ),
            "{kind}: {error}"
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1, "{kind}");
        assert_revoked(&runner, &live);
        assert!(runner.take_pending_writes().is_empty());
    }
}

#[test]
fn a_new_connection_or_remote_close_requires_new_admission() {
    let (mut runner, live, mode, _calls) = ready_runner();
    mode.store(NO_ADMISSION, Ordering::Relaxed);
    runner.on_connected(TimestampNs(20)).unwrap();
    assert_revoked(&runner, &live);
    mode.store(NORMAL, Ordering::Relaxed);
    runner.on_connected(TimestampNs(30)).unwrap();
    assert!(live.load(Ordering::Relaxed));
    runner.on_close_frame(b"close", stamp()).unwrap();
    assert_revoked(&runner, &live);
}

#[test]
fn refused_side_effect_free_dynamic_preparation_preserves_existing_admission() {
    for command in [
        SessionCommand::Subscribe(vec!["BTCUSDT".into()]),
        SessionCommand::Unsubscribe(vec!["BTCUSDT".into()]),
        SessionCommand::Replace(vec!["BTCUSDT".into()]),
    ] {
        let (mut runner, live, _mode, calls) = ready_runner();
        let recording_before = runner.recording_bytes();
        let error = runner
            .deliver_control(command, TimestampNs(20))
            .unwrap_err();
        assert!(matches!(
            error,
            EngineError::Adapter(AdapterError::UnsupportedCapability(_))
        ));
        assert!(live.load(Ordering::Relaxed));
        assert_eq!(runner.lifecycle, SessionLifecycle::Live);
        assert_eq!(runner.metrics.valid_books.load(Ordering::Relaxed), 1);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(runner.take_pending_writes().is_empty());
        assert_eq!(runner.dispatch_pending(), (0, 0));
        assert_eq!(runner.recording_bytes(), recording_before);
    }
}

struct OversizedDynamicWire {
    inner: ScriptedMachine,
    commits: Arc<AtomicUsize>,
}

impl SessionMachine for OversizedDynamicWire {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        actions: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        self.inner.on_input(input, actions)
    }

    fn prepare_dynamic_subscription(
        &self,
        _command: &SessionCommand,
    ) -> Result<SubscriptionWireAction, AdapterError> {
        // Successfully prepared, but too large for the durable control format.
        Ok(SubscriptionWireAction::Text(Bytes::from(vec![
            b'x';
            256 * 1024
                + 1
        ])))
    }

    fn commit_dynamic_subscription(&mut self, _command: &SessionCommand) {
        self.commits.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn dynamic_recording_failure_after_successful_preparation_revokes_admission_without_commit() {
    let live = Arc::new(AtomicBool::new(false));
    let commits = Arc::new(AtomicUsize::new(0));
    let mut runner = admitted_runner(
        Box::new(OversizedDynamicWire {
            inner: ScriptedMachine {
                mode: Arc::new(AtomicU8::new(NORMAL)),
                calls: Arc::new(AtomicUsize::new(0)),
            },
            commits: Arc::clone(&commits),
        }),
        Arc::clone(&live),
    );
    let error = runner
        .deliver_control(
            SessionCommand::Subscribe(vec!["BTCUSDT".into()]),
            TimestampNs(20),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        EngineError::Recording(RecordingError::InvalidControlCommand(ref message))
            if message == "subscription wire payload exceeds limit"
    ));
    assert_revoked(&runner, &live);
    assert_eq!(commits.load(Ordering::Relaxed), 0);
    assert!(runner.take_pending_writes().is_empty());
}
