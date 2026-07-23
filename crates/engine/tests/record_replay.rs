//! Synthetic venue: live inject → raw record → replay → identical market events.

use marketfeed_adapter_api::{ConcreteSubscriptionSet, SessionSpec, VenueFactory};
use marketfeed_adapter_synthetic::SyntheticFactory;
use marketfeed_engine::{EngineSupervisor, SessionRunnerConfig};
use marketfeed_model::{
    CatalogVersion, CatalogView, FrameStamp, MarketEvent, OverflowPolicy, SessionId, TimestampNs,
    VenueId,
};
use marketfeed_replay::ReplayRunner;

fn new_machine() -> Box<dyn marketfeed_adapter_api::SessionMachine> {
    SyntheticFactory
        .create_session(
            SessionSpec {
                endpoint_name: "ws".into(),
                subscriptions: ConcreteSubscriptionSet::default(),
            },
            CatalogView::new(VenueId(1), CatalogVersion(1)),
        )
        .expect("synthetic session")
}

fn flatten_markets(batches: &[marketfeed_adapter_api::EventBatch]) -> Vec<MarketEvent> {
    batches
        .iter()
        .flat_map(|b| b.events.iter().map(|e| e.payload.clone()))
        .collect()
}

/// Engine-emitted status events are not on the wire recording (R6); strip for identity compare.
fn wire_derived(events: Vec<MarketEvent>) -> Vec<MarketEvent> {
    events
        .into_iter()
        .filter(|e| {
            !matches!(
                e,
                MarketEvent::VenueStatus(_) | MarketEvent::InstrumentUpdate(_)
            )
        })
        .collect()
}

#[test]
fn synthetic_record_replay_identical_market_events() {
    let mut supervisor = EngineSupervisor::new();
    supervisor.mark_running();

    let session = SessionId(1);
    supervisor
        .insert_session(
            new_machine(),
            SessionRunnerConfig {
                venue: VenueId(1),
                session,
                overflow: OverflowPolicy::FailEngine,
                record: true,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    let runner = supervisor.session_mut(session).unwrap();
    runner.on_connected(TimestampNs(1_000)).unwrap();

    // Mirror synthetic subscribe confirm + market script (same as Phase 0 drive_script).
    let frames = [
        "SUB BTC-USD",
        "BOOK_SNAP 10 BID 100.00:1.000 ASK 101.00:1.500",
        "TRADE 11 100.50 0.250 BUY t1",
        "BOOK_DELTA 11 BID UPSERT 100.50 0.500",
    ];
    let mut ts = 1_000i64;
    for frame in frames {
        ts += 1;
        let mut bytes = frame.as_bytes().to_vec();
        runner
            .on_text_frame(
                &mut bytes,
                FrameStamp {
                    receive_ts: TimestampNs(ts),
                    mono_ns: ts as u64,
                },
            )
            .unwrap();
    }

    let live_markets = wire_derived(flatten_markets(&runner.market_batches));
    let recording = runner.recording_bytes().expect("recording enabled");
    assert!(!recording.is_empty());

    // Replay through a fresh machine + ReplayRunner.
    let mut machine = new_machine();
    let mut replay = ReplayRunner::new(1024);
    let outcome = replay
        .replay_bytes(&mut *machine, recording, TimestampNs(1_000))
        .unwrap();
    let replay_markets = wire_derived(flatten_markets(&outcome.market_batches));

    assert_eq!(
        live_markets, replay_markets,
        "replay must reproduce identical wire-derived market events"
    );
    assert!(
        live_markets
            .iter()
            .any(|e| matches!(e, MarketEvent::BookSnapshot(_)))
    );
    assert!(
        live_markets
            .iter()
            .any(|e| matches!(e, MarketEvent::Trade(_)))
    );
    assert!(
        live_markets
            .iter()
            .any(|e| matches!(e, MarketEvent::BookDelta(_)))
    );
}

#[tokio::test]
async fn supervisor_drains_memory_transport() {
    use marketfeed_transport::{MemoryWebSocket, WebSocketSpec};

    let mut supervisor = EngineSupervisor::new();
    supervisor.mark_running();
    let session = SessionId(9);
    supervisor
        .insert_session(
            new_machine(),
            SessionRunnerConfig {
                session,
                record: false,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    let mut ws = MemoryWebSocket::new();
    ws.push_text(b"SUB BTC-USD".to_vec());
    ws.push_text(b"TRADE 1 100.00 1.000 BUY".to_vec());

    supervisor
        .drain_memory_ws(
            session,
            &mut ws,
            &WebSocketSpec {
                url: "memory://synthetic".into(),
                max_frame_bytes: 64 * 1024,
                ..WebSocketSpec::default()
            },
            500,
        )
        .await
        .unwrap();

    let runner = supervisor.session_mut(session).unwrap();
    let markets = flatten_markets(&runner.market_batches);
    assert!(markets.iter().any(|e| matches!(e, MarketEvent::Trade(_))));
}
