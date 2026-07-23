//! Offline reconnect: adapter Reconnect action stops the drain loop with flag set.

use marketfeed_adapter_api::{ConcreteSubscriptionSet, SessionSpec, VenueFactory};
use marketfeed_adapter_synthetic::SyntheticFactory;
use marketfeed_engine::{EngineSupervisor, SessionRunnerConfig};
use marketfeed_model::{CatalogVersion, CatalogView, SessionId, VenueId};
use marketfeed_transport::{MemoryWebSocket, WebSocketSpec};

#[tokio::test]
async fn memory_loop_surfaces_reconnect_request() {
    let machine = SyntheticFactory
        .create_session(
            SessionSpec {
                endpoint_name: "ws".into(),
                subscriptions: ConcreteSubscriptionSet::default(),
            },
            CatalogView::new(VenueId(1), CatalogVersion(1)),
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
                record: false,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    let mut ws = MemoryWebSocket::new();
    ws.push_text(b"SUB BTC-USD".to_vec());
    ws.push_text(b"DISCONNECT".to_vec());

    supervisor
        .drain_memory_ws(
            session,
            &mut ws,
            &WebSocketSpec {
                url: "memory://".into(),
                ..WebSocketSpec::default()
            },
            1,
        )
        .await
        .unwrap();

    let runner = supervisor.session_mut(session).unwrap();
    assert!(runner.reconnect_requested);
}
