//! Deliberately wrong adapter identities prove that the collector is the owner.
use marketfeed_adapter_api::*;
use marketfeed_engine::{SessionRunner, SessionRunnerConfig};
use marketfeed_model::*;
use marketfeed_recording::shared_feed::*;
struct WrongIdentity;
impl SessionMachine for WrongIdentity {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        out: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if let SessionInput::TextFrame { received, .. } = input {
            out.push(SessionAction::EmitBatch(EventBatch {
                session: SessionId(1),
                frame_seq: 999,
                events: vec![EventEnvelope {
                    schema_version: 1,
                    venue: VenueId(9),
                    instrument: Some(InstrumentId(1)),
                    connection: ConnectionId(999),
                    session: SessionId(1),
                    frame_seq: 999,
                    event_index: 0,
                    exchange_ts: Some(TimestampNs(1)),
                    receive_ts: received.receive_ts,
                    source_sequence: None,
                    flags: EventFlags::empty(),
                    payload: MarketEvent::Trade(Trade {
                        price: Price(Fixed::new(123, 2)),
                        quantity: Quantity(Fixed::new(4, 3)),
                        aggressor: AggressorSide::Buy,
                        trade_id: Some(SourceId("t1".into())),
                    }),
                }],
            }));
        }
        Ok(())
    }
}
#[test]
fn runtime_identity_original_ordinal_and_reconnect_generation_are_recorded() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("feed");
    let feed = FeedHandle::new(FeedWriter::create(&dir, 1_000_000).unwrap());
    let mut runner = SessionRunner::new(
        Box::new(WrongIdentity),
        SessionRunnerConfig {
            venue: VenueId(21),
            session: SessionId(33),
            shared_feed: Some(feed),
            ..Default::default()
        },
    )
    .unwrap();
    let stamp = FrameStamp {
        receive_ts: TimestampNs(22),
        mono_ns: 17,
    };
    runner.on_connected(TimestampNs(10)).unwrap();
    runner.drain_dispatch();
    runner.market_batches.clear();
    runner.on_ping_frame(b"p", stamp).unwrap();
    runner
        .on_text_frame(&mut b"original bytes".to_vec(), stamp)
        .unwrap();
    let batch = &runner.market_batches[0];
    let event = &batch.events[0];
    assert_eq!((batch.session, batch.frame_seq), (SessionId(33), 2));
    assert_eq!(
        (event.venue, event.session, event.frame_seq),
        (VenueId(21), SessionId(33), 2)
    );
    assert_ne!(event.connection, ConnectionId(999));
    let first = event.connection;
    runner
        .on_disconnected(DisconnectReason::RemoteClose, TimestampNs(30))
        .unwrap();
    runner.on_connected(TimestampNs(40)).unwrap();
    runner.drain_dispatch();
    runner.market_batches.clear();
    runner
        .on_text_frame(&mut b"next bytes".to_vec(), stamp)
        .unwrap();
    assert_ne!(runner.market_batches[0].events[0].connection, first);
    let mut reader = FeedReader::open(&dir, None).unwrap();
    let mut original = false;
    let mut accepted = false;
    let mut batches = 0;
    while let Some(record) = reader.read_next().unwrap() {
        match record {
            FeedRecord::Frame {
                session,
                frame_seq: 2,
                opcode: 1,
                direction: 0,
                ..
            } => {
                assert_eq!(session, SessionId(33));
                original = true;
            }
            FeedRecord::FrameAccepted {
                session,
                frame_seq: 2,
            } => {
                assert!(original);
                assert_eq!(session, SessionId(33));
                accepted = true;
            }
            FeedRecord::Batch(b) => {
                assert!(original);
                assert_eq!(b.session, SessionId(33));
                batches += 1;
            }
            _ => {}
        }
    }
    assert!(accepted);
    assert_eq!(batches, 2);
}
#[test]
fn exhausted_archive_budget_prevents_native_frame_acceptance() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("feed");
    let feed = FeedHandle::new(FeedWriter::create(&dir, 2000).unwrap());
    let mut runner = SessionRunner::new(
        Box::new(WrongIdentity),
        SessionRunnerConfig {
            venue: VenueId(21),
            session: SessionId(33),
            shared_feed: Some(feed),
            ..Default::default()
        },
    )
    .unwrap();
    runner.on_connected(TimestampNs(10)).unwrap();
    runner.drain_dispatch();
    runner.market_batches.clear();
    assert!(
        runner
            .on_text_frame(
                &mut vec![b'x'; 3000],
                FrameStamp {
                    receive_ts: TimestampNs(22),
                    mono_ns: 17
                }
            )
            .is_err()
    );
    assert!(runner.market_batches.is_empty());
    let mut reader = FeedReader::open(&dir, None).unwrap();
    while let Some(record) = reader.read_next().unwrap() {
        assert!(!matches!(
            record,
            FeedRecord::FrameAccepted { .. } | FeedRecord::Batch(_)
        ));
    }
}

#[tokio::test]
async fn continuously_ready_input_does_not_starve_parent_health_and_stop() {
    use marketfeed_transport::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    struct ReadyBurst {
        reads: Arc<AtomicUsize>,
    }
    impl WebSocketTransport for ReadyBurst {
        async fn connect(&mut self, _: &WebSocketSpec) -> Result<(), TransportError> {
            Ok(())
        }
        async fn read_frame(
            &mut self,
            _: &mut FrameBuffer,
        ) -> Result<InboundFrame, TransportError> {
            if self.reads.fetch_add(1, Ordering::Relaxed) >= 10_000 {
                return Err(TransportError::Closed);
            }
            Ok(InboundFrame {
                opcode: FrameOpcode::Text,
                payload: vec![],
            })
        }
        async fn write_frame(&mut self, _: OutboundFrame) -> Result<(), TransportError> {
            Ok(())
        }
        async fn close(&mut self, _: CloseReason) -> Result<(), TransportError> {
            Ok(())
        }
    }
    let reads = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut runner = SessionRunner::new(
        Box::new(WrongIdentity),
        SessionRunnerConfig {
            record: false,
            mirror_capacity: 0,
            stop_signal: Some(Arc::clone(&stop)),
            ..Default::default()
        },
    )
    .unwrap();
    let mut ws = ReadyBurst {
        reads: Arc::clone(&reads),
    };
    let spec = WebSocketSpec::default();
    let run = marketfeed_engine::run_session_with_reconnect(
        &mut runner,
        &mut ws,
        &StubHttpTransport,
        &spec,
        ReconnectPolicy {
            min_delay_ms: 1,
            max_delay_ms: 1,
            reset_after_live_ms: 1,
        },
        0,
    );
    let parent = async {
        let observed = reads.load(Ordering::Relaxed);
        stop.store(true, Ordering::Relaxed);
        observed
    };
    let (_result, observed) = tokio::join!(run, parent);
    assert!(
        observed < 10_000,
        "continuously ready transport starved its parent until the burst ended"
    );
}
