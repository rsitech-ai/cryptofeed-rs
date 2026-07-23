//! R5: dynamic subscriptions via EngineControl (synthetic venue, no networking).

use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, ConcreteSubscriptionSet, SessionAction, SessionCommand,
    SessionInput, SessionMachine, SessionSpec, SubscriptionPatch, SubscriptionWireAction,
};
use marketfeed_adapter_synthetic::{SYNTHETIC_VENUE_ID, SyntheticFactory, SyntheticSession};
use marketfeed_engine::{EngineControl, EngineSupervisor, SessionRunnerConfig};
use marketfeed_model::{
    CatalogVersion, CatalogView, FrameStamp, OverflowPolicy, SessionId, SystemEvent, TimestampNs,
    VenueId,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

struct CaptureWrites {
    inner: SyntheticSession,
}

struct RejectSubscribeMachine;
struct IgnoringControlMachine;
struct PrefilledDynamicMachine {
    commits: Arc<AtomicUsize>,
}

impl SessionMachine for RejectSubscribeMachine {
    fn prepare_dynamic_subscription(
        &self,
        command: &SessionCommand,
    ) -> Result<SubscriptionWireAction, AdapterError> {
        if matches!(command, SessionCommand::Subscribe(_)) {
            return Err(AdapterError::Subscription(
                "injected subscribe rejection".into(),
            ));
        }
        Err(AdapterError::UnsupportedCapability(
            "test machine only rejects subscribe".into(),
        ))
    }

    fn on_input(
        &mut self,
        _input: SessionInput<'_>,
        _output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        Ok(())
    }
}

impl SessionMachine for PrefilledDynamicMachine {
    fn prepare_dynamic_subscription(
        &self,
        command: &SessionCommand,
    ) -> Result<SubscriptionWireAction, AdapterError> {
        if matches!(
            command,
            SessionCommand::Subscribe(_)
                | SessionCommand::Unsubscribe(_)
                | SessionCommand::Replace(_)
        ) {
            Ok(SubscriptionWireAction::Text(bytes::Bytes::from_static(
                b"CONTROL",
            )))
        } else {
            Err(AdapterError::UnsupportedCapability(
                "subscription command required".into(),
            ))
        }
    }

    fn commit_dynamic_subscription(&mut self, _command: &SessionCommand) {
        self.commits.fetch_add(1, Ordering::Relaxed);
    }

    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if matches!(input, SessionInput::Connected { .. }) {
            for _ in 0..256 {
                output.push(SessionAction::SendText(bytes::Bytes::from_static(
                    b"prefill",
                )));
            }
        }
        Ok(())
    }
}

impl SessionMachine for IgnoringControlMachine {
    fn on_input(
        &mut self,
        _input: SessionInput<'_>,
        _output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        Ok(())
    }
}

struct RejectSymbolMachine {
    commands: Arc<Mutex<Vec<String>>>,
    reject_symbol: &'static str,
}

impl SessionMachine for RejectSymbolMachine {
    fn prepare_dynamic_subscription(
        &self,
        command: &SessionCommand,
    ) -> Result<SubscriptionWireAction, AdapterError> {
        let (verb, symbols) = match command {
            SessionCommand::Subscribe(symbols) => ("subscribe", symbols),
            SessionCommand::Unsubscribe(symbols) => ("unsubscribe", symbols),
            SessionCommand::Replace(symbols) => ("replace", symbols),
            SessionCommand::Resync(_) | SessionCommand::Stop => {
                return Err(AdapterError::UnsupportedCapability(
                    "subscription command required".into(),
                ));
            }
        };
        if symbols.iter().any(|symbol| symbol == self.reject_symbol) {
            return Err(AdapterError::Subscription(
                "injected replacement rejection".into(),
            ));
        }
        Ok(SubscriptionWireAction::Text(bytes::Bytes::from(format!(
            "{verb}:{}",
            symbols.join(",")
        ))))
    }

    fn commit_dynamic_subscription(&mut self, command: &SessionCommand) {
        let (verb, symbols) = match command {
            SessionCommand::Subscribe(symbols) => ("subscribe", symbols),
            SessionCommand::Unsubscribe(symbols) => ("unsubscribe", symbols),
            SessionCommand::Replace(symbols) => ("replace", symbols),
            SessionCommand::Resync(_) | SessionCommand::Stop => return,
        };
        self.commands
            .lock()
            .unwrap()
            .push(format!("{verb}:{}", symbols.join(",")));
    }

    fn on_input(
        &mut self,
        _input: SessionInput<'_>,
        _output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        Ok(())
    }
}

impl CaptureWrites {
    fn wrap(inner: SyntheticSession) -> Self {
        Self { inner }
    }
}

impl SessionMachine for CaptureWrites {
    fn prepare_dynamic_subscription(
        &self,
        command: &SessionCommand,
    ) -> Result<SubscriptionWireAction, AdapterError> {
        self.inner.prepare_dynamic_subscription(command)
    }

    fn commit_dynamic_subscription(&mut self, command: &SessionCommand) {
        self.inner.commit_dynamic_subscription(command);
    }

    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        self.inner.on_input(input, output)
    }
}

fn synth_session() -> SyntheticSession {
    let factory = SyntheticFactory;
    let catalog = CatalogView::new(SYNTHETIC_VENUE_ID, CatalogVersion(1));
    let spec = SessionSpec {
        endpoint_name: "ws".into(),
        subscriptions: ConcreteSubscriptionSet::default(),
    };
    let _ = factory;
    SyntheticSession::new(spec, catalog)
}

#[test]
fn apply_subscriptions_add_remove_bumps_plan_version() {
    let mut engine = EngineSupervisor::new();
    engine.mark_running();
    let session = SessionId(1);
    engine
        .insert_session(
            Box::new(CaptureWrites::wrap(synth_session())),
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session,
                record: false,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    let now = TimestampNs(1);
    let v1 = engine
        .apply_subscriptions(
            SubscriptionPatch::Add {
                session,
                symbols: vec!["BTC-USD".into(), "ETH-USD".into()],
            },
            now,
        )
        .unwrap();
    assert_eq!(v1.0, 1);
    assert_eq!(
        engine.desired_symbols(session),
        Some(["BTC-USD".into(), "ETH-USD".into()].as_slice())
    );

    let v2 = engine
        .apply_subscriptions(
            SubscriptionPatch::Remove {
                session,
                symbols: vec!["ETH-USD".into()],
            },
            TimestampNs(2),
        )
        .unwrap();
    assert_eq!(v2.0, 2);
    assert_eq!(
        engine.desired_symbols(session),
        Some(["BTC-USD".into()].as_slice())
    );

    let runner = engine.session_mut(session).unwrap();
    assert!(
        runner
            .system_events
            .iter()
            .any(|e| matches!(e, SystemEvent::SubscriptionStateChanged { .. })),
        "expected subscription state events"
    );
    let pending: Vec<_> = runner
        .take_pending_writes()
        .into_iter()
        .filter_map(|f| String::from_utf8(f.payload).ok())
        .collect();
    assert!(
        pending.iter().any(|w| w.contains("SUB BTC-USD,ETH-USD")),
        "expected subscribe write, got {pending:?}"
    );
    assert!(
        pending.iter().any(|w| w.contains("UNSUB ETH-USD")),
        "expected unsubscribe write, got {pending:?}"
    );
}

#[test]
fn rejected_subscription_does_not_mutate_desired_state_or_plan_version() {
    let mut engine = EngineSupervisor::new();
    engine.mark_running();
    let session = SessionId(2);
    engine
        .insert_session(
            Box::new(RejectSubscribeMachine),
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session,
                record: false,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    let error = engine
        .apply_subscriptions(
            SubscriptionPatch::Add {
                session,
                symbols: vec!["BTC-USD".into()],
            },
            TimestampNs(1),
        )
        .unwrap_err();

    assert!(matches!(
        error,
        marketfeed_engine::ControlError::Engine(message)
            if message.contains("injected subscribe rejection")
    ));
    assert_eq!(engine.desired_symbols(session), None);
    assert_eq!(engine.plan_version().0, 0);
}

#[test]
fn atomic_replace_failure_preserves_previous_desired_state() {
    let mut engine = EngineSupervisor::new();
    engine.mark_running();
    let session = SessionId(4);
    let commands = Arc::new(Mutex::new(Vec::new()));
    engine
        .insert_session(
            Box::new(RejectSymbolMachine {
                commands: Arc::clone(&commands),
                reject_symbol: "B",
            }),
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session,
                record: false,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();
    engine
        .apply_subscriptions(
            SubscriptionPatch::Add {
                session,
                symbols: vec!["A".into()],
            },
            TimestampNs(1),
        )
        .unwrap();
    commands.lock().unwrap().clear();
    let before = engine.plan_version();

    let error = engine
        .apply_subscriptions(
            SubscriptionPatch::Replace {
                session,
                symbols: vec!["B".into()],
            },
            TimestampNs(2),
        )
        .expect_err("adapter rejects the atomic replacement");

    assert!(matches!(
        error,
        marketfeed_engine::ControlError::Engine(message)
            if message.contains("injected replacement rejection")
    ));
    assert!(commands.lock().unwrap().is_empty());
    assert_eq!(
        engine.desired_symbols(session),
        Some(["A".into()].as_slice())
    );
    assert_eq!(engine.plan_version(), before);
}

#[test]
fn adapters_must_opt_in_to_dynamic_subscription_control() {
    let mut engine = EngineSupervisor::new();
    engine.mark_running();
    let session = SessionId(6);
    engine
        .insert_session(
            Box::new(IgnoringControlMachine),
            SessionRunnerConfig {
                venue: VenueId(999),
                session,
                record: false,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    let error = engine
        .apply_subscriptions(
            SubscriptionPatch::Add {
                session,
                symbols: vec!["BTC-USD".into()],
            },
            TimestampNs(1),
        )
        .expect_err("wildcard control handlers must fail closed");

    assert!(matches!(
        error,
        marketfeed_engine::ControlError::Engine(message)
            if message.contains("unsupported capability: dynamic subscriptions")
    ));
    assert_eq!(engine.desired_symbols(session), None);
    assert_eq!(engine.plan_version().0, 0);
}

#[test]
fn full_pending_write_queue_rejects_control_before_adapter_commit() {
    for overflow in [
        OverflowPolicy::FailEngine,
        OverflowPolicy::DropNewest,
        OverflowPolicy::DropOldest,
    ] {
        let mut engine = EngineSupervisor::new();
        engine.mark_running();
        let session = SessionId(60 + overflow as u64);
        let commits = Arc::new(AtomicUsize::new(0));
        engine
            .insert_session(
                Box::new(PrefilledDynamicMachine {
                    commits: Arc::clone(&commits),
                }),
                SessionRunnerConfig {
                    venue: SYNTHETIC_VENUE_ID,
                    session,
                    overflow,
                    record: false,
                    mirror_capacity: 0,
                    ..SessionRunnerConfig::default()
                },
            )
            .unwrap();
        engine
            .session_mut(session)
            .unwrap()
            .on_connected(TimestampNs(1))
            .unwrap();

        let error = engine
            .apply_subscriptions(
                SubscriptionPatch::Add {
                    session,
                    symbols: vec!["BTC-USD".into()],
                },
                TimestampNs(2),
            )
            .expect_err("full authoritative write queue must fail closed");

        assert!(
            matches!(error, marketfeed_engine::ControlError::Engine(_)),
            "{error:?}"
        );
        assert_eq!(commits.load(Ordering::Relaxed), 0);
        assert_eq!(engine.desired_symbols(session), None);
        assert_eq!(engine.plan_version().0, 0);
        let writes = engine.session_mut(session).unwrap().take_pending_writes();
        assert_eq!(writes.len(), 256);
        assert!(
            writes
                .iter()
                .all(|frame| frame.payload.as_slice() == b"prefill")
        );
    }
}

#[test]
fn full_diagnostic_queue_does_not_hide_successful_subscription_control() {
    let mut engine = EngineSupervisor::new();
    engine.mark_running();
    let session = SessionId(5);
    engine
        .insert_session(
            Box::new(synth_session()),
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session,
                record: false,
                dispatch_capacity: 1,
                overflow: OverflowPolicy::FailEngine,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();
    engine
        .session_mut(session)
        .unwrap()
        .push_system(SystemEvent::HeartbeatMissed)
        .unwrap();

    let plan = engine
        .apply_subscriptions(
            SubscriptionPatch::Add {
                session,
                symbols: vec!["BTC-USD".into()],
            },
            TimestampNs(1),
        )
        .expect("control mutation is authoritative over diagnostic notification");

    assert_eq!(plan.0, 1);
    assert_eq!(
        engine.desired_symbols(session),
        Some(["BTC-USD".into()].as_slice())
    );
    assert!(
        engine
            .session_mut(session)
            .unwrap()
            .metrics
            .events_dropped
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0,
        "diagnostic loss must remain observable in metrics"
    );
    let pending = engine.session_mut(session).unwrap().take_pending_writes();
    assert!(pending.iter().any(|frame| {
        std::str::from_utf8(&frame.payload).is_ok_and(|text| text.contains("SUB BTC-USD"))
    }));
}

#[test]
fn full_action_mirror_does_not_hide_successful_wire_control() {
    let mut engine = EngineSupervisor::new();
    engine.mark_running();
    let session = SessionId(8);
    engine
        .insert_session(
            Box::new(synth_session()),
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session,
                record: false,
                dispatch_capacity: 1,
                mirror_capacity: 1,
                overflow: OverflowPolicy::FailEngine,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();
    engine
        .apply_subscriptions(
            SubscriptionPatch::Add {
                session,
                symbols: vec!["BTC-USD".into()],
            },
            TimestampNs(1),
        )
        .unwrap();

    let plan = engine
        .apply_subscriptions(
            SubscriptionPatch::Add {
                session,
                symbols: vec!["ETH-USD".into()],
            },
            TimestampNs(2),
        )
        .expect("diagnostic mirror saturation must not roll back queued wire control");

    assert_eq!(plan.0, 2);
    assert_eq!(
        engine.desired_symbols(session),
        Some(["BTC-USD".into(), "ETH-USD".into()].as_slice())
    );
    let runner = engine.session_mut(session).unwrap();
    let writes = runner.take_pending_writes();
    assert!(writes.iter().any(|frame| {
        std::str::from_utf8(&frame.payload).is_ok_and(|text| text.contains("SUB ETH-USD"))
    }));
    assert!(
        runner
            .metrics
            .events_dropped
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    );
}

#[test]
fn pause_resume_venue_and_health() {
    let mut engine = EngineSupervisor::new();
    engine.mark_running();
    let session = SessionId(7);
    engine
        .insert_session(
            Box::new(synth_session()),
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session,
                record: false,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    engine
        .apply_subscriptions(
            SubscriptionPatch::Add {
                session,
                symbols: vec!["BTC-USD".into()],
            },
            TimestampNs(1),
        )
        .unwrap();

    let plan = engine
        .apply_subscriptions(
            SubscriptionPatch::PauseVenue {
                venue: SYNTHETIC_VENUE_ID,
            },
            TimestampNs(2),
        )
        .unwrap();
    assert!(plan.0 >= 2);
    assert!(engine.is_venue_paused(SYNTHETIC_VENUE_ID));

    let err = engine
        .apply_subscriptions(
            SubscriptionPatch::Add {
                session,
                symbols: vec!["ETH-USD".into()],
            },
            TimestampNs(3),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        marketfeed_engine::ControlError::VenuePaused(_)
    ));

    engine
        .apply_subscriptions(
            SubscriptionPatch::ResumeVenue {
                venue: SYNTHETIC_VENUE_ID,
            },
            TimestampNs(4),
        )
        .unwrap();
    assert!(!engine.is_venue_paused(SYNTHETIC_VENUE_ID));

    let health = engine.health().unwrap();
    assert_eq!(health.plan_version, engine.plan_version());
    assert!(health.sessions.iter().any(|s| s.session == session));
}

#[test]
fn rolling_replace_skeleton_swaps_sessions() {
    let mut engine = EngineSupervisor::new();
    engine.mark_running();
    let old = SessionId(1);
    let new = SessionId(2);
    engine
        .insert_session(
            Box::new(synth_session()),
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session: old,
                record: false,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();
    engine
        .apply_subscriptions(
            SubscriptionPatch::Add {
                session: old,
                symbols: vec!["BTC-USD".into()],
            },
            TimestampNs(1),
        )
        .unwrap();

    let pair = engine
        .begin_rolling_replace(
            old,
            Box::new(synth_session()),
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session: new,
                record: false,
                ..SessionRunnerConfig::default()
            },
            TimestampNs(2),
        )
        .unwrap();
    assert_eq!(pair.old, old);
    assert_eq!(pair.new, new);
    assert_eq!(
        engine.desired_symbols(new),
        Some(["BTC-USD".into()].as_slice())
    );
    let replacement_writes = engine.session_mut(new).unwrap().take_pending_writes();
    assert!(replacement_writes.iter().any(|frame| {
        std::str::from_utf8(&frame.payload).is_ok_and(|text| text.contains("SUB BTC-USD"))
    }));

    let not_live = engine.complete_rolling_replace(pair).unwrap_err();
    assert!(matches!(
        not_live,
        marketfeed_engine::ControlError::Unsupported(message)
            if message.contains("replacement session is not live")
    ));
    assert!(engine.session_mut(old).is_ok());
    let mut snapshot = b"BOOK_SNAP 1 BID 100.00:1.000 ASK 101.00:1.000".to_vec();
    engine
        .session_mut(new)
        .unwrap()
        .on_text_frame(
            &mut snapshot,
            FrameStamp {
                receive_ts: TimestampNs(3),
                mono_ns: 3,
            },
        )
        .unwrap();
    engine.complete_rolling_replace(pair).unwrap();
    assert!(engine.session_mut(old).is_err());
    assert!(engine.session_mut(new).is_ok());
}

#[test]
fn a_session_cannot_have_two_concurrent_rolling_replacements() {
    let mut engine = EngineSupervisor::new();
    engine.mark_running();
    let old = SessionId(21);
    engine
        .insert_session(
            Box::new(synth_session()),
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session: old,
                record: false,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    engine
        .begin_rolling_replace(
            old,
            Box::new(synth_session()),
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session: SessionId(22),
                record: false,
                ..SessionRunnerConfig::default()
            },
            TimestampNs(1),
        )
        .unwrap();
    let error = engine
        .begin_rolling_replace(
            old,
            Box::new(synth_session()),
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session: SessionId(23),
                record: false,
                ..SessionRunnerConfig::default()
            },
            TimestampNs(2),
        )
        .expect_err("old session already has a tracked replacement");

    assert!(matches!(
        error,
        marketfeed_engine::ControlError::Unsupported(message)
            if message.contains("already in progress")
    ));
    assert!(engine.session_mut(SessionId(23)).is_err());
}

#[test]
fn untracked_rolling_replace_cannot_remove_a_session() {
    let mut engine = EngineSupervisor::new();
    engine.mark_running();
    let old = SessionId(11);
    let new = SessionId(12);
    for session in [old, new] {
        engine
            .insert_session(
                Box::new(synth_session()),
                SessionRunnerConfig {
                    venue: SYNTHETIC_VENUE_ID,
                    session,
                    record: false,
                    ..SessionRunnerConfig::default()
                },
            )
            .unwrap();
    }
    engine
        .session_mut(new)
        .unwrap()
        .mark_live_with_status(TimestampNs(1))
        .unwrap();

    let error = engine
        .complete_rolling_replace(marketfeed_engine::RollingReplace { old, new })
        .unwrap_err();

    assert!(matches!(
        error,
        marketfeed_engine::ControlError::Unsupported(message)
            if message.contains("not tracked")
    ));
    assert!(engine.session_mut(old).is_ok());
    assert!(engine.session_mut(new).is_ok());
}

#[test]
fn failed_replacement_subscription_removes_the_new_session() {
    let mut engine = EngineSupervisor::new();
    engine.mark_running();
    let old = SessionId(31);
    let new = SessionId(32);
    engine
        .insert_session(
            Box::new(synth_session()),
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session: old,
                record: false,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();
    engine
        .apply_subscriptions(
            SubscriptionPatch::Add {
                session: old,
                symbols: vec!["BTC-USD".into()],
            },
            TimestampNs(1),
        )
        .unwrap();
    let before = engine.plan_version();

    let error = engine
        .begin_rolling_replace(
            old,
            Box::new(RejectSubscribeMachine),
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session: new,
                record: false,
                ..SessionRunnerConfig::default()
            },
            TimestampNs(2),
        )
        .expect_err("replacement subscribe must succeed before tracking");

    assert!(matches!(
        error,
        marketfeed_engine::ControlError::Engine(message)
            if message.contains("injected subscribe rejection")
    ));
    assert!(engine.session_mut(old).is_ok());
    assert!(engine.session_mut(new).is_err());
    assert_eq!(engine.desired_symbols(new), None);
    assert_eq!(engine.plan_version(), before);
}

#[test]
fn rolling_completion_rejects_subscription_drift() {
    let mut engine = EngineSupervisor::new();
    engine.mark_running();
    let old = SessionId(41);
    let new = SessionId(42);
    engine
        .insert_session(
            Box::new(synth_session()),
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session: old,
                record: false,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();
    engine
        .apply_subscriptions(
            SubscriptionPatch::Add {
                session: old,
                symbols: vec!["BTC-USD".into()],
            },
            TimestampNs(1),
        )
        .unwrap();
    let pair = engine
        .begin_rolling_replace(
            old,
            Box::new(synth_session()),
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session: new,
                record: false,
                ..SessionRunnerConfig::default()
            },
            TimestampNs(2),
        )
        .unwrap();
    engine
        .apply_subscriptions(
            SubscriptionPatch::Add {
                session: new,
                symbols: vec!["ETH-USD".into()],
            },
            TimestampNs(3),
        )
        .unwrap();
    engine
        .session_mut(new)
        .unwrap()
        .mark_live_with_status(TimestampNs(4))
        .unwrap();

    let error = engine
        .complete_rolling_replace(pair)
        .expect_err("replacement must preserve the old session's desired set");

    assert!(matches!(
        error,
        marketfeed_engine::ControlError::Unsupported(message)
            if message.contains("desired subscriptions do not match")
    ));
    assert!(engine.session_mut(old).is_ok());
    assert!(engine.session_mut(new).is_ok());
}

#[test]
fn replace_patch_is_monotonic() {
    let mut engine = EngineSupervisor::new();
    engine.mark_running();
    let session = SessionId(3);
    engine
        .insert_session(
            Box::new(synth_session()),
            SessionRunnerConfig {
                venue: VenueId(1),
                session,
                record: false,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    let a = engine
        .apply_subscriptions(
            SubscriptionPatch::Replace {
                session,
                symbols: vec!["A".into()],
            },
            TimestampNs(1),
        )
        .unwrap();
    let b = engine
        .apply_subscriptions(
            SubscriptionPatch::Replace {
                session,
                symbols: vec!["B".into(), "C".into()],
            },
            TimestampNs(2),
        )
        .unwrap();
    assert!(b.0 > a.0);
    assert_eq!(
        engine.desired_symbols(session),
        Some(["B".into(), "C".into()].as_slice())
    );
}
