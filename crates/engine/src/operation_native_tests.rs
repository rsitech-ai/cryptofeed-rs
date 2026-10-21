use super::*;
use marketfeed_adapter_api::*;
use marketfeed_model::*;

fn config() -> CaptureConfig {
    CaptureConfig {
        venue: VenueId(21),
        session: SessionId(77),
        dispatch_capacity: 4,
        overflow: OverflowPolicy::FailEngine,
        max_bytes: 8 * 1024 * 1024,
        max_entries: 8192,
    }
}
fn text() -> CaptureInput {
    CaptureInput::Text {
        payload: b"native fixture".to_vec(),
        received: FrameStamp {
            receive_ts: TimestampNs(123),
            mono_ns: 10,
        },
    }
}
fn observed(id: i64, extra_bytes: usize) -> ObservedEventBatch {
    // Native-only one-sided BBO: it must consume a real market slot despite
    // having no canonical quote. The huge price/count guard against coercion.
    ObservedEventBatch::try_new(
        EventBatch {
            session: SessionId(999),
            frame_seq: 999,
            events: vec![],
        },
        NativeObservationAttachment {
            schema_version: 1,
            rows: vec![NativeObservationRow {
                canonical_positions: vec![],
                observation: NativeObservation::HyperliquidBbo(HyperliquidBboObservation {
                    coin: "x".repeat(extra_bytes.max(1)),
                    time_ms: id,
                    sides: [
                        None,
                        Some(HyperliquidLevelObservation {
                            price: Fixed::new(i128::MAX, 255),
                            size: Fixed::new(1, 20),
                            orders: u64::MAX,
                        }),
                    ],
                }),
            }],
        },
        ObservedBatchSource {
            venue: VenueId(999),
            connection: ConnectionId(999),
            received: FrameStamp {
                receive_ts: TimestampNs(i64::MIN),
                mono_ns: u64::MAX,
            },
        },
        ObservedBatchLimits::HARD_MAX,
    )
    .unwrap()
}
struct Emit {
    count: usize,
    fail: bool,
    extra_bytes: usize,
}
impl SessionMachine for Emit {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        out: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if matches!(input, SessionInput::TextFrame { .. }) {
            for id in 0..self.count {
                out.push(SessionAction::EmitObservedBatch(observed(
                    id as i64,
                    self.extra_bytes,
                )));
            }
            if self.fail {
                return Err(AdapterError::Parse("late native failure".into()));
            }
        }
        Ok(())
    }
}
fn emitter(count: usize) -> Box<dyn SessionMachine> {
    Box::new(Emit {
        count,
        fail: false,
        extra_bytes: 0,
    })
}
fn id(item: &CaptureQueueItem) -> i64 {
    let CaptureQueueItem::ObservedMarket(batch) = item else {
        panic!("native unit stripped")
    };
    assert_eq!(batch.canonical().session, SessionId(77));
    assert_eq!(batch.canonical().frame_seq, 1);
    assert_eq!(
        batch.source(),
        ObservedBatchSource {
            venue: VenueId(21),
            connection: ConnectionId(1),
            received: FrameStamp {
                receive_ts: TimestampNs(123),
                mono_ns: 10
            },
        }
    );
    assert!(batch.canonical().events.is_empty());
    let NativeObservation::HyperliquidBbo(bbo) = &batch.native().rows[0].observation else {
        panic!()
    };
    assert_eq!(
        bbo.sides[1].as_ref().unwrap().price,
        Fixed::new(i128::MAX, 255)
    );
    assert_eq!(bbo.sides[1].as_ref().unwrap().orders, u64::MAX);
    bbo.time_ms
}
fn pending(op: &CapturedOperation) -> &[CaptureQueueItem] {
    &op.after.queues.as_ref().unwrap()[0].1
}

#[test]
fn native_only_units_share_exact_market_overflow_and_eviction_fate() {
    for policy in [OverflowPolicy::DropNewest, OverflowPolicy::DropOldest] {
        let mut owner = CaptureSession::new(
            emitter(3),
            CaptureConfig {
                dispatch_capacity: 2,
                overflow: policy,
                ..config()
            },
        )
        .unwrap();
        let op = owner.execute(text()).unwrap();
        assert_eq!(op.completeness, CaptureCompleteness::Complete);
        assert_eq!(op.outcome, CaptureOutcome::Returned(Ok(())));
        let attempts: Vec<_> = op
            .entries
            .iter()
            .filter_map(|entry| match &entry.effect {
                CaptureEntry::QueueAttempt {
                    lane: CaptureQueueLane::Market,
                    incoming,
                    evicted_front,
                    result,
                } => Some((id(incoming), evicted_front.as_ref().map(id), result.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(attempts[0], (0, None, Some(Ok(PushOutcome::Accepted))));
        assert_eq!(attempts[1], (1, None, Some(Ok(PushOutcome::Accepted))));
        if policy == OverflowPolicy::DropNewest {
            assert_eq!(attempts[2], (2, None, Some(Ok(PushOutcome::DroppedNewest))));
            assert_eq!(pending(&op).iter().map(id).collect::<Vec<_>>(), [0, 1]);
        } else {
            assert_eq!(
                attempts[2],
                (
                    2,
                    Some(0),
                    Some(Ok(PushOutcome::DroppedOldest { dropped: 1 }))
                )
            );
            assert_eq!(pending(&op).iter().map(id).collect::<Vec<_>>(), [1, 2]);
        }
    }
}

#[test]
fn native_partial_forward_retains_rejected_whole_front_and_unattempted_suffix() {
    let mut owner = CaptureSession::new(emitter(3), config()).unwrap();
    owner.execute(text()).unwrap();
    let op = owner
        .execute(CaptureInput::ForwardDispatch {
            results: vec![
                Ok(PushOutcome::Accepted),
                Err(SinkError::Io("fixture failure".into())),
            ],
        })
        .unwrap();
    assert!(matches!(op.outcome, CaptureOutcome::Returned(Err(_))));
    assert_eq!(pending(&op).iter().map(id).collect::<Vec<_>>(), [1, 2]);
    let attempts: Vec<_> = op
        .entries
        .iter()
        .filter_map(|entry| match &entry.effect {
            CaptureEntry::SinkAttempt {
                lane: CaptureQueueLane::Market,
                incoming,
                result,
            } => Some((id(incoming), result)),
            _ => None,
        })
        .collect();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0], (0, &Some(Ok(PushOutcome::Accepted))));
    assert_eq!(attempts[1].0, 1);
    let null = owner.execute(CaptureInput::NullDispatch).unwrap();
    assert!(pending(&null).is_empty());
    assert!(null.entries.iter().any(|entry| matches!(&entry.effect,
        CaptureEntry::QueueDrain { lane: CaptureQueueLane::Market, items, completed: true }
        if items.iter().map(id).collect::<Vec<_>>() == [1, 2])));
}

#[test]
fn native_unit_is_refused_by_legacy_sink_without_consuming_front() {
    let mut owner = CaptureSession::new(emitter(2), config()).unwrap();
    owner.execute(text()).unwrap();
    let mut legacy = marketfeed_sinks::MemorySink::new(4, 4, OverflowPolicy::FailEngine);
    assert!(owner.runner.consume_dispatch(Some(&mut legacy)).is_err());
    assert_eq!(owner.runner.dispatch_pending().0, 2);
    assert_eq!(legacy.batch_len(), 0);
}

#[test]
fn native_action_overflow_and_callback_error_never_resurrect_market_rows() {
    let mut owner = CaptureSession::new(
        Box::new(Emit {
            count: 2,
            fail: true,
            extra_bytes: 0,
        }),
        config(),
    )
    .unwrap();
    let op = owner.execute(text()).unwrap();
    assert!(matches!(op.outcome, CaptureOutcome::Returned(Err(_))));
    assert!(pending(&op).is_empty());
    let actions: Vec<_> = op
        .entries
        .iter()
        .flat_map(|entry| match &entry.effect {
            CaptureEntry::Invocation { actions, .. } => actions.as_slice(),
            _ => &[],
        })
        .collect();
    assert_eq!(actions.len(), 2);
    assert!(
        actions
            .iter()
            .all(|a| a.disposition == ActionDisposition::DiscardedAfterCallbackError)
    );
    let mut owner = CaptureSession::new(
        emitter(1025),
        CaptureConfig {
            dispatch_capacity: 1,
            overflow: OverflowPolicy::DropNewest,
            ..config()
        },
    )
    .unwrap();
    let op = owner.execute(text()).unwrap();
    assert_eq!(op.completeness, CaptureCompleteness::Complete);
    assert_eq!(pending(&op).iter().map(id).collect::<Vec<_>>(), [0]);
    assert!(
        op.entries
            .iter()
            .any(|entry| matches!(entry.effect, CaptureEntry::ActionBufferDropsConsumed(1)))
    );
    let attempts = op
        .entries
        .iter()
        .filter(|entry| {
            matches!(
                entry.effect,
                CaptureEntry::QueueAttempt {
                    lane: CaptureQueueLane::Market,
                    ..
                }
            )
        })
        .count();
    assert_eq!(attempts, 1024);
}

#[test]
fn native_capture_charges_attachment_bytes_and_poison_prevents_further_execution() {
    let mut owner = CaptureSession::new(
        Box::new(Emit {
            count: 1,
            fail: false,
            extra_bytes: 32_768,
        }),
        CaptureConfig {
            max_bytes: 8192,
            ..config()
        },
    )
    .unwrap();
    let op = owner.execute(text()).unwrap();
    assert!(matches!(
        op.completeness,
        CaptureCompleteness::Incomplete(CaptureFault::Bytes)
    ));
    assert!(owner.is_poisoned());
    assert!(matches!(owner.execute(text()), Err(EngineError::Stopped)));
}

#[test]
fn existing_causal_codec_refuses_native_actions_and_retained_native_state() {
    let identity = crate::causal::OperationIdentity {
        venue: 21,
        session: 77,
        bootstrap_id: "a".repeat(64),
    };
    let mut owner = CaptureSession::new(emitter(1), config()).unwrap();
    let emitted = owner.execute(text()).unwrap();
    let retained = owner
        .execute(CaptureInput::TimerPoll {
            now: TimestampNs(1000),
        })
        .unwrap();
    for op in [emitted, retained] {
        let error =
            crate::causal::encode_operation(&op, &identity, crate::causal::MAX_OPERATION_BYTES)
                .unwrap_err();
        assert!(error.to_string().contains("enriched causal profile"));
    }
}

#[test]
fn legacy_durable_feed_refuses_native_unit_before_dispatch_or_frame_acceptance() {
    use marketfeed_recording::shared_feed::*;
    let temp = tempfile::tempdir().unwrap();
    let feed_dir = temp.path().join("legacy");
    let feed = FeedHandle::new(FeedWriter::create(&feed_dir, 1_000_000).unwrap());
    let mut runner = SessionRunner::new(
        emitter(1),
        SessionRunnerConfig {
            venue: VenueId(21),
            session: SessionId(77),
            shared_feed: Some(feed),
            record: false,
            ..Default::default()
        },
    )
    .unwrap();
    let error = runner
        .on_text_frame(
            &mut b"native fixture".to_vec(),
            FrameStamp {
                receive_ts: TimestampNs(123),
                mono_ns: 10,
            },
        )
        .unwrap_err();
    assert!(error.to_string().contains("enriched durable profile"));
    assert_eq!(runner.dispatch_pending().0, 0);
    let mut reader = FeedReader::open(&feed_dir, None).unwrap();
    while let Some(record) = reader.read_next().unwrap() {
        assert!(!matches!(
            record,
            FeedRecord::Batch(_) | FeedRecord::FrameAccepted { .. }
        ));
    }
}

#[test]
fn legacy_raw_replay_refuses_new_native_action_instead_of_classifying_it_as_other() {
    struct OnStart;
    impl SessionMachine for OnStart {
        fn on_input(
            &mut self,
            _: SessionInput<'_>,
            out: &mut ActionBuffer,
        ) -> Result<(), AdapterError> {
            out.push(SessionAction::EmitObservedBatch(observed(1, 0)));
            Ok(())
        }
    }
    let error = marketfeed_replay::ReplayRunner::new(4)
        .replay_records(&mut OnStart, &[], TimestampNs(0))
        .unwrap_err();
    assert!(error.to_string().contains("enriched replay profile"));
}

#[test]
fn native_only_queued_source_identity_and_original_clocks_survive_reconnect() {
    let mut owner = CaptureSession::new(emitter(1), config()).unwrap();
    owner
        .execute(CaptureInput::Connected {
            connection: ConnectionId(11),
            now: TimestampNs(100),
        })
        .unwrap();
    let first = owner.execute(text()).unwrap();
    let first = pending(&first)
        .iter()
        .find_map(|item| match item {
            CaptureQueueItem::ObservedMarket(batch) => Some(batch),
            _ => None,
        })
        .unwrap();
    assert_eq!(first.source().connection, ConnectionId(11));
    owner
        .execute(CaptureInput::Connected {
            connection: ConnectionId(12),
            now: TimestampNs(-100),
        })
        .unwrap();
    let receipt = FrameStamp {
        receive_ts: TimestampNs(i64::MIN),
        mono_ns: u64::MAX,
    };
    let op = owner
        .execute(CaptureInput::Text {
            payload: b"later frame".to_vec(),
            received: receipt,
        })
        .unwrap();
    let batches: Vec<_> = pending(&op)
        .iter()
        .filter_map(|item| match item {
            CaptureQueueItem::ObservedMarket(batch) => Some(batch),
            _ => None,
        })
        .collect();
    assert_eq!(batches.len(), 2);
    let (old, new) = (batches[0], batches[1]);
    assert_eq!(old, first);
    assert_eq!(
        old.source(),
        ObservedBatchSource {
            venue: VenueId(21),
            connection: ConnectionId(11),
            received: FrameStamp {
                receive_ts: TimestampNs(123),
                mono_ns: 10
            }
        }
    );
    assert_eq!(
        new.source(),
        ObservedBatchSource {
            venue: VenueId(21),
            connection: ConnectionId(12),
            received: receipt
        }
    );
    assert_eq!(
        (old.canonical().frame_seq, new.canonical().frame_seq),
        (1, 2)
    );
    assert_ne!(old, new);
}

#[test]
fn native_unit_cannot_borrow_a_prior_frame_receipt_from_timer_callback() {
    struct TimerEmit;
    impl SessionMachine for TimerEmit {
        fn on_input(
            &mut self,
            input: SessionInput<'_>,
            out: &mut ActionBuffer,
        ) -> Result<(), AdapterError> {
            match input {
                SessionInput::TextFrame { .. } => {
                    out.push(SessionAction::ScheduleTimer(TimerSpec {
                        timer_id: 7,
                        fire_at: TimestampNs(124),
                    }))
                }
                SessionInput::Timer { .. } => {
                    out.push(SessionAction::EmitObservedBatch(observed(1, 0)))
                }
                _ => {}
            }
            Ok(())
        }
    }
    let mut owner = CaptureSession::new(Box::new(TimerEmit), config()).unwrap();
    owner.execute(text()).unwrap();
    let op = owner
        .execute(CaptureInput::TimerPoll {
            now: TimestampNs(125),
        })
        .unwrap();
    assert!(matches!(op.outcome, CaptureOutcome::Returned(Err(_))));
    assert!(pending(&op).is_empty());
}
