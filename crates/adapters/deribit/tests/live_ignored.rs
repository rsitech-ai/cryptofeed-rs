//! Live network smoke (ignored by default — keep CI offline).

use marketfeed_adapter_api::{ReconnectPolicy, VenueFactory};
use marketfeed_adapter_deribit::DeribitFactory;
use marketfeed_engine::{EngineSupervisor, SessionRunnerConfig};
use marketfeed_model::{
    CatalogVersion, CatalogView, MarketEvent, OverflowPolicy, SessionId, VenueId,
};
use marketfeed_transport::{ReqwestHttpTransport, TungsteniteWebSocket, WebSocketSpec};

#[tokio::test]
#[ignore = "live network: cargo test -p marketfeed-adapter-deribit -- --ignored --nocapture"]
async fn live_deribit_trade_or_ticker() {
    let factory = DeribitFactory::default();
    let plan = factory
        .plan(
            &Default::default(),
            &CatalogView::new(VenueId(8), CatalogVersion(1)),
        )
        .unwrap();
    let url = plan[0].endpoint_name.clone();
    let machine = factory
        .create_session(
            plan.into_iter().next().unwrap(),
            CatalogView::new(VenueId(8), CatalogVersion(1)),
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

    let run = supervisor.run_session_loop(session, &mut ws, &http, &spec, policy, 2);
    tokio::select! {
        res = run => { res.unwrap(); }
        _ = tokio::time::sleep(std::time::Duration::from_secs(20)) => {}
    }

    let runner = supervisor.session_mut(session).unwrap();
    let has_market = runner.market_batches.iter().any(|b| {
        b.events.iter().any(|e| {
            matches!(
                e.payload,
                MarketEvent::Trade(_)
                    | MarketEvent::Quote(_)
                    | MarketEvent::MarkPrice(_)
                    | MarketEvent::IndexPrice(_)
            )
        })
    });
    assert!(
        has_market,
        "expected at least one live trade/ticker field within 20s"
    );
}
