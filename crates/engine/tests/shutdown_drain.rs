//! Offline: graceful shutdown drains dispatch and honors stop_signal.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use marketfeed_adapter_api::{ConcreteSubscriptionSet, ReconnectPolicy, VenueFactory};
use marketfeed_adapter_synthetic::{SYNTHETIC_VENUE_ID, SyntheticFactory};
use marketfeed_engine::{EngineLifecycle, EngineSupervisor, SessionRunnerConfig};
use marketfeed_model::{
    CatalogVersion, CatalogView, OverflowPolicy, SessionId, SystemEvent, TimestampNs,
};
use marketfeed_sinks::MemorySink;
use marketfeed_transport::{MemoryWebSocket, WebSocketSpec};

#[test]
fn begin_shutdown_drains_dispatch_and_emits_events() {
    let factory = SyntheticFactory;
    let catalog = CatalogView::new(SYNTHETIC_VENUE_ID, CatalogVersion(1));
    let plan = factory
        .plan(&ConcreteSubscriptionSet::default(), &catalog)
        .unwrap();
    let machine = factory
        .create_session(plan.into_iter().next().unwrap(), catalog)
        .unwrap();

    let mut supervisor = EngineSupervisor::new();
    supervisor.mark_running();
    let session = SessionId(42);
    supervisor
        .insert_session(
            machine,
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session,
                record: false,
                overflow: OverflowPolicy::FailEngine,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    let runner = supervisor.session_mut(session).unwrap();
    runner.on_connected(TimestampNs(1)).unwrap();
    runner.push_system(SystemEvent::HeartbeatMissed).unwrap();

    supervisor.begin_shutdown().unwrap();
    let runner = supervisor.session_mut(session).unwrap();
    assert!(runner.is_stop_requested());
    assert!(
        runner
            .system_events
            .iter()
            .any(|e| matches!(e, SystemEvent::ShutdownStarted))
    );

    let mut sink = MemorySink::new(8, 8, OverflowPolicy::FailEngine);
    supervisor
        .session_mut(session)
        .unwrap()
        .consume_dispatch(Some(&mut sink))
        .unwrap();
    supervisor.finish_shutdown_to(Some(&mut sink)).unwrap();
    assert_eq!(supervisor.lifecycle, EngineLifecycle::Stopped);
    let systems = std::iter::from_fn(|| sink.pop_system()).collect::<Vec<_>>();
    let started = systems
        .iter()
        .position(|event| matches!(event, SystemEvent::ShutdownStarted))
        .expect("ShutdownStarted delivered");
    let completed = systems
        .iter()
        .position(|event| matches!(event, SystemEvent::ShutdownCompleted))
        .expect("ShutdownCompleted delivered");
    assert!(started < completed);
}

#[tokio::test]
async fn stop_signal_ends_reconnect_loop() {
    let factory = SyntheticFactory;
    let catalog = CatalogView::new(SYNTHETIC_VENUE_ID, CatalogVersion(1));
    let plan = factory
        .plan(&ConcreteSubscriptionSet::default(), &catalog)
        .unwrap();
    let machine = factory
        .create_session(plan.into_iter().next().unwrap(), catalog)
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let mut supervisor = EngineSupervisor::new();
    supervisor.mark_running();
    let session = SessionId(7);
    supervisor
        .insert_session(
            machine,
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session,
                record: false,
                stop_signal: Some(Arc::clone(&stop)),
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    let mut ws = MemoryWebSocket::new();
    // Empty inbound → Closed each attempt; loop would reconnect forever without stop.
    let spec = WebSocketSpec {
        url: "memory://stop".into(),
        ..WebSocketSpec::default()
    };

    let run = supervisor.run_session_loop_ws_only(
        session,
        &mut ws,
        &spec,
        ReconnectPolicy {
            min_delay_ms: 5_000,
            max_delay_ms: 5_000,
            reset_after_live_ms: 1_000,
        },
        u32::MAX,
    );

    let stopper = Arc::clone(&stop);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(40)).await;
        stopper.store(true, Ordering::Relaxed);
    });

    let res = tokio::time::timeout(Duration::from_millis(500), run).await;
    assert!(
        res.is_ok(),
        "session loop should interrupt post-disconnect backoff for stop_signal"
    );
    assert!(res.unwrap().is_ok());
    let runner = supervisor.session_mut(session).unwrap();
    assert!(
        runner
            .system_events
            .iter()
            .any(|e| matches!(e, SystemEvent::ShutdownCompleted))
            || runner.is_stop_requested()
    );
}

#[tokio::test]
async fn graceful_live_stop_forwards_completion_after_started() {
    let factory = SyntheticFactory;
    let catalog = CatalogView::new(SYNTHETIC_VENUE_ID, CatalogVersion(1));
    let plan = factory
        .plan(&ConcreteSubscriptionSet::default(), &catalog)
        .unwrap();
    let machine = factory
        .create_session(plan.into_iter().next().unwrap(), catalog)
        .unwrap();

    let stop = Arc::new(AtomicBool::new(true));
    let mut supervisor = EngineSupervisor::new();
    supervisor.mark_running();
    let session = SessionId(8);
    supervisor
        .insert_session(
            machine,
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session,
                record: false,
                stop_signal: Some(stop),
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();
    let mut ws = MemoryWebSocket::new();
    let spec = WebSocketSpec {
        url: "memory://graceful-stop".into(),
        ..WebSocketSpec::default()
    };
    let mut sink = MemorySink::new(8, 8, OverflowPolicy::FailEngine);
    supervisor
        .run_session_loop_to(
            session,
            &mut ws,
            &marketfeed_transport::StubHttpTransport,
            &spec,
            ReconnectPolicy {
                min_delay_ms: 1,
                max_delay_ms: 1,
                reset_after_live_ms: 1,
            },
            0,
            Some(&mut sink),
        )
        .await
        .unwrap();

    let systems = std::iter::from_fn(|| sink.pop_system()).collect::<Vec<_>>();
    let started = systems
        .iter()
        .position(|event| matches!(event, SystemEvent::ShutdownStarted))
        .expect("ShutdownStarted delivered");
    let completed = systems
        .iter()
        .position(|event| matches!(event, SystemEvent::ShutdownCompleted))
        .expect("ShutdownCompleted delivered");
    assert!(started < completed, "systems={systems:?}");
    assert_eq!(completed, systems.len() - 1, "completion must be last");
}
