//! Live network smoke (ignored by default — keep CI offline).
//! Alpha only — does **not** unlock beta.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use marketfeed_adapter_api::{ReconnectPolicy, VenueFactory};
use marketfeed_adapter_gemini::GeminiFactory;
use marketfeed_engine::{EngineSupervisor, SessionRunnerConfig};
use marketfeed_model::{
    CatalogVersion, CatalogView, MarketEvent, OverflowPolicy, SessionId, SystemEvent, VenueId,
};
use marketfeed_transport::{ReqwestHttpTransport, TungsteniteWebSocket, WebSocketSpec};

#[tokio::test]
#[ignore = "live network: cargo test -p marketfeed-adapter-gemini --test live_ignored -- --ignored --nocapture"]
async fn live_gemini_spot_trade_or_quote() {
    // Current Gemini uses independent trade and book-ticker streams. L2 additionally
    // subscribes to differential depth with a full snapshot requested in the URL.
    let factory = GeminiFactory {
        enable_l2: false,
        live_details_max: 0,
    };
    let plan = factory
        .plan(
            &Default::default(),
            &CatalogView::new(VenueId(15), CatalogVersion(1)),
        )
        .unwrap();
    let url = plan[0].endpoint_name.clone();
    let machine = factory
        .create_session(
            plan.into_iter().next().unwrap(),
            CatalogView::new(VenueId(15), CatalogVersion(1)),
        )
        .unwrap();

    let mut supervisor = EngineSupervisor::new();
    supervisor.mark_running();
    let session = SessionId(1);
    let stop_signal = Arc::new(AtomicBool::new(false));
    supervisor
        .insert_session(
            machine,
            SessionRunnerConfig {
                session,
                record: true,
                overflow: OverflowPolicy::DropOldest,
                mirror_capacity: 64,
                stop_signal: Some(Arc::clone(&stop_signal)),
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    let mut ws = TungsteniteWebSocket::new();
    let http = ReqwestHttpTransport::new().expect("reqwest rustls");
    let spec = WebSocketSpec {
        url,
        max_frame_bytes: 4 * 1024 * 1024,
        ..WebSocketSpec::default()
    };
    let policy = ReconnectPolicy {
        min_delay_ms: 200,
        max_delay_ms: 2_000,
        reset_after_live_ms: 5_000,
    };

    let stop_after_observation = Arc::clone(&stop_signal);
    let stop_task = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(20)).await;
        stop_after_observation.store(true, Ordering::Relaxed);
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(25),
        supervisor.run_session_loop(session, &mut ws, &http, &spec, policy, 2),
    )
    .await
    .expect("Gemini live smoke stopped before its shutdown deadline")
    .expect("Gemini live smoke completed cleanly");
    stop_task.await.expect("stop task");

    let runner = supervisor.session_mut(session).unwrap();
    let has_market = runner.market_batches.iter().any(|b| {
        b.events
            .iter()
            .any(|e| matches!(e.payload, MarketEvent::Trade(_) | MarketEvent::Quote(_)))
    });
    assert!(
        has_market,
        "expected at least one live trade or quote within 20s"
    );
    let errors: Vec<_> = runner
        .system_events
        .iter()
        .filter(|event| {
            matches!(
                event,
                SystemEvent::ParseError { .. }
                    | SystemEvent::HeartbeatMissed
                    | SystemEvent::SequenceGap { .. }
                    | SystemEvent::BookInvalidated { .. }
            )
        })
        .collect();
    assert!(
        errors.is_empty(),
        "unexpected live system errors: {errors:?}"
    );
    assert!(
        runner
            .system_events
            .iter()
            .any(|event| matches!(event, SystemEvent::ShutdownCompleted)),
        "expected graceful live-smoke shutdown"
    );
}
