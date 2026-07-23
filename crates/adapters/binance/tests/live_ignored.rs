//! Live network smoke (ignored by default — keep CI offline).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use marketfeed_adapter_api::{ReconnectPolicy, VenueFactory};
use marketfeed_adapter_binance::BinanceSpotFactory;
use marketfeed_engine::{EngineMetrics, EngineSupervisor, SessionLifecycle, SessionRunnerConfig};
use marketfeed_model::{
    CatalogVersion, CatalogView, MarketEvent, OverflowPolicy, SessionId, VenueId,
};
use marketfeed_transport::{
    KillSwitchWebSocket, ReqwestHttpTransport, TungsteniteWebSocket, WebSocketSpec,
};

#[tokio::test]
#[ignore = "live network: cargo test -p marketfeed-adapter-binance -- --ignored --nocapture"]
async fn live_binance_spot_trade_or_quote() {
    let factory = BinanceSpotFactory { enable_l2: false };
    let plan = factory
        .plan(
            &Default::default(),
            &CatalogView::new(VenueId(2), CatalogVersion(1)),
        )
        .unwrap();
    let url = plan[0].endpoint_name.clone();
    let machine = factory
        .create_session(
            plan.into_iter().next().unwrap(),
            CatalogView::new(VenueId(2), CatalogVersion(1)),
        )
        .unwrap();

    let mut supervisor = EngineSupervisor::new();
    supervisor.mark_running();
    let session = SessionId(1);
    supervisor
        .insert_session(
            machine,
            SessionRunnerConfig {
                session,
                record: true,
                // Live smoke has no consumer; DropOldest keeps high-rate venues up while
                // mirrors retain recent events for assertions.
                overflow: OverflowPolicy::DropOldest,
                mirror_capacity: 64,
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

    // Run briefly: stop after first market event via timeout wrapper.
    let run = supervisor.run_session_loop(session, &mut ws, &http, &spec, policy, 2);
    tokio::select! {
        res = run => { res.unwrap(); }
        _ = tokio::time::sleep(std::time::Duration::from_secs(15)) => {}
    }

    let runner = supervisor.session_mut(session).unwrap();
    let has_market = runner.market_batches.iter().any(|b| {
        b.events
            .iter()
            .any(|e| matches!(e.payload, MarketEvent::Trade(_) | MarketEvent::Quote(_)))
    });
    assert!(
        has_market,
        "expected at least one live trade or quote within 15s"
    );
    assert!(
        runner
            .recording_bytes()
            .map(|b| !b.is_empty())
            .unwrap_or(false),
        "live frames should be recorded"
    );
}

/// Checklist item 5: force transport Closed after Live; session must reconnect and return Live.
#[tokio::test]
#[ignore = "live network: cargo test -p marketfeed-adapter-binance -- --ignored --nocapture"]
async fn live_binance_spot_reconnect_probe() {
    let factory = BinanceSpotFactory { enable_l2: false };
    let plan = factory
        .plan(
            &Default::default(),
            &CatalogView::new(VenueId(2), CatalogVersion(1)),
        )
        .unwrap();
    let url = plan[0].endpoint_name.clone();
    let machine = factory
        .create_session(
            plan.into_iter().next().unwrap(),
            CatalogView::new(VenueId(2), CatalogVersion(1)),
        )
        .unwrap();

    let metrics = Arc::new(EngineMetrics::new());
    let live = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let kill = Arc::new(AtomicBool::new(false));

    let mut supervisor = EngineSupervisor::new();
    supervisor.mark_running();
    let session = SessionId(1);
    supervisor
        .insert_session(
            machine,
            SessionRunnerConfig {
                session,
                record: false,
                overflow: OverflowPolicy::DropOldest,
                mirror_capacity: 64,
                metrics: Some(Arc::clone(&metrics)),
                live_signal: Some(Arc::clone(&live)),
                stop_signal: Some(Arc::clone(&stop)),
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    let mut ws = KillSwitchWebSocket::new(TungsteniteWebSocket::new(), Arc::clone(&kill));
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

    {
        let mut run =
            std::pin::pin!(supervisor.run_session_loop(session, &mut ws, &http, &spec, policy, 5,));

        // Drive probe alongside the session loop until recovery, then stop_signal.
        let probe = async {
            wait_until(Duration::from_secs(20), || {
                live.load(Ordering::Relaxed)
                    && metrics.events_dispatched.load(Ordering::Relaxed) > 0
            })
            .await;
            assert!(
                live.load(Ordering::Relaxed),
                "binance must reach Live before kill"
            );
            let frames_before = metrics.frames_received.load(Ordering::Relaxed);

            kill.store(true, Ordering::Relaxed);

            wait_until(Duration::from_secs(25), || {
                metrics.reconnects.load(Ordering::Relaxed) >= 1
                    && live.load(Ordering::Relaxed)
                    && metrics.frames_received.load(Ordering::Relaxed) > frames_before
            })
            .await;
            assert!(
                metrics.reconnects.load(Ordering::Relaxed) >= 1,
                "expected ≥1 reconnect after kill"
            );
            assert!(
                live.load(Ordering::Relaxed),
                "session must return Live after reconnect"
            );
            assert!(
                metrics.frames_received.load(Ordering::Relaxed) > frames_before,
                "expected frames after recovery"
            );
            stop.store(true, Ordering::Relaxed);
        };

        tokio::select! {
            res = &mut run => { res.expect("session loop"); }
            _ = probe => {}
        }
        // Probe set stop_signal; allow graceful_stop to finish.
        let _ = tokio::time::timeout(Duration::from_secs(10), &mut run).await;
    }

    let runner = supervisor.session_mut(session).unwrap();
    assert_eq!(runner.lifecycle, SessionLifecycle::Stopped);
    let has_market = runner.market_batches.iter().any(|b| {
        b.events
            .iter()
            .any(|e| matches!(e.payload, MarketEvent::Trade(_) | MarketEvent::Quote(_)))
    });
    assert!(has_market, "expected trade/quote across reconnect window");
    assert!(
        metrics.reconnects.load(Ordering::Relaxed) >= 1,
        "reconnect metric must be recorded"
    );
}

async fn wait_until(limit: Duration, mut pred: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + limit;
    while tokio::time::Instant::now() < deadline {
        if pred() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
