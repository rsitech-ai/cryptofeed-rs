use super::*;
use marketfeed_adapter_api::{SensitiveBytes, TimerSpec};
use marketfeed_model::*;
use marketfeed_recording::shared_feed::FeedRecord;

fn config() -> CaptureConfig {
    CaptureConfig {
        venue: VenueId(21),
        session: SessionId(77),
        dispatch_capacity: 8,
        overflow: OverflowPolicy::FailEngine,
        max_bytes: 8 * 1024 * 1024,
        max_entries: 8192,
    }
}
fn stamp() -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(12345),
        mono_ns: 999,
    }
}
fn text(value: &str) -> CaptureInput {
    CaptureInput::Text {
        payload: value.as_bytes().to_vec(),
        received: stamp(),
    }
}
fn batch(value: i128) -> EventBatch {
    EventBatch {
        session: SessionId(999),
        frame_seq: 999,
        events: vec![EventEnvelope {
            schema_version: 1,
            venue: VenueId(999),
            instrument: Some(InstrumentId(1)),
            connection: ConnectionId(999),
            session: SessionId(999),
            frame_seq: 999,
            event_index: 999,
            exchange_ts: None,
            receive_ts: stamp().receive_ts,
            source_sequence: None,
            flags: EventFlags::empty(),
            payload: MarketEvent::Trade(Trade {
                provenance: None,
                price: Price(Fixed::new(value, 0)),
                quantity: Quantity(Fixed::new(1, 0)),
                aggressor: AggressorSide::Buy,
                trade_id: Some(SourceId(value.to_string())),
            }),
        }],
    }
}
fn records(operation: &CapturedOperation) -> Vec<FeedRecord> {
    operation
        .entries
        .iter()
        .filter_map(|entry| match entry {
            CaptureEntry::ProducedRecord(bytes) => Some(serde_json::from_slice(bytes).unwrap()),
            _ => None,
        })
        .collect()
}
fn actions(operation: &CapturedOperation) -> Vec<&BufferedAction> {
    operation
        .entries
        .iter()
        .flat_map(|entry| match entry {
            CaptureEntry::Invocation { actions, .. } => actions.as_slice(),
            _ => &[],
        })
        .collect()
}
struct Emit {
    count: usize,
    fail: bool,
    mutate: bool,
}
impl SessionMachine for Emit {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        out: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if let SessionInput::TextFrame { bytes, .. } | SessionInput::BinaryFrame { bytes, .. } =
            input
        {
            if self.mutate {
                bytes.fill(0);
            }
            for value in 1..=self.count {
                out.push(SessionAction::EmitBatch(batch(value as i128)));
            }
            if self.fail {
                return Err(AdapterError::Parse(
                    "after producing buffered actions".into(),
                ));
            }
        }
        Ok(())
    }
}
#[test]
fn original_input_is_retained_before_mutable_text_and_binary_parsers() {
    for input in [
        text("original text"),
        CaptureInput::Binary {
            payload: vec![255, 1, 2],
            received: stamp(),
        },
    ] {
        let mut session = CaptureSession::new(
            Box::new(Emit {
                count: 1,
                fail: false,
                mutate: true,
            }),
            config(),
        )
        .unwrap();
        let operation = session.execute(input.clone()).unwrap();
        assert_eq!(operation.original_input, input);
        assert_eq!(operation.completeness, CaptureCompleteness::Complete);
        assert_eq!(operation.outcome, CaptureOutcome::Returned(Ok(())));
        let original = match &input {
            CaptureInput::Text { payload, .. } | CaptureInput::Binary { payload, .. } => payload,
            _ => unreachable!(),
        };
        let recs = records(&operation);
        assert_eq!(recs[0].payload_bytes().unwrap().unwrap(), *original);
        assert!(matches!(
            recs.last(),
            Some(FeedRecord::FrameAccepted { frame_seq: 1, .. })
        ));
        assert!(matches!(
            actions(&operation)[0].disposition,
            ActionDisposition::Completed(Ok(()))
        ));
        assert_eq!(operation.after.pending_market, 1);
    }
}

#[test]
fn failed_callback_retains_buffered_actions_before_the_runner_discards_them() {
    let mut session = CaptureSession::new(
        Box::new(Emit {
            count: 2,
            fail: true,
            mutate: false,
        }),
        config(),
    )
    .unwrap();
    let operation = session.execute(text("bad")).unwrap();
    assert!(matches!(
        operation.outcome,
        CaptureOutcome::Returned(Err(EngineError::Adapter(AdapterError::Parse(_))))
    ));
    assert_eq!(operation.completeness, CaptureCompleteness::Complete);
    assert_eq!(actions(&operation).len(), 2);
    assert!(
        actions(&operation)
            .iter()
            .all(|a| a.disposition == ActionDisposition::DiscardedAfterCallbackError)
    );
    assert_eq!(operation.after.lifecycle, SessionLifecycle::Degraded);
    assert_eq!(operation.after.pending_market, 0);
    assert!(
        !records(&operation)
            .iter()
            .any(|r| matches!(r, FeedRecord::Batch(_) | FeedRecord::FrameAccepted { .. }))
    );
}

#[test]
fn prefix_failure_keeps_produced_records_distinct_from_queue_admission_and_suffix() {
    let mut session = CaptureSession::new(
        Box::new(Emit {
            count: 3,
            fail: false,
            mutate: false,
        }),
        CaptureConfig {
            dispatch_capacity: 1,
            ..config()
        },
    )
    .unwrap();
    let operation = session.execute(text("three")).unwrap();
    let buffered = actions(&operation);
    assert_eq!(
        buffered[0].disposition,
        ActionDisposition::Completed(Ok(()))
    );
    assert_eq!(
        buffered[1].disposition,
        ActionDisposition::Completed(Err(EngineError::Dispatch(DispatchError::FailEngine)))
    );
    assert_eq!(buffered[2].disposition, ActionDisposition::Buffered);
    assert_eq!(
        records(&operation)
            .iter()
            .filter(|r| matches!(r, FeedRecord::Batch(_)))
            .count(),
        2
    );
    let queue_results: Vec<_> = operation
        .entries
        .iter()
        .filter_map(|e| match e {
            CaptureEntry::QueueAttempt {
                lane: CaptureQueueLane::Market,
                result,
                ..
            } => Some(result.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        queue_results,
        vec![
            Some(Ok(PushOutcome::Accepted)),
            Some(Err(EngineError::Dispatch(DispatchError::FailEngine)))
        ]
    );
    assert_eq!(operation.after.pending_market, 1);
    assert_eq!(operation.after.lifecycle, SessionLifecycle::Degraded);
    assert!(
        !records(&operation)
            .iter()
            .any(|r| matches!(r, FeedRecord::FrameAccepted { .. }))
    );
}

#[test]
fn drop_policies_capture_new_admission_and_the_exact_evicted_front() {
    for overflow in [OverflowPolicy::DropNewest, OverflowPolicy::DropOldest] {
        let mut session = CaptureSession::new(
            Box::new(Emit {
                count: 2,
                fail: false,
                mutate: false,
            }),
            CaptureConfig {
                dispatch_capacity: 1,
                overflow,
                ..config()
            },
        )
        .unwrap();
        let op = session.execute(text("two")).unwrap();
        assert_eq!(op.outcome, CaptureOutcome::Returned(Ok(())));
        let attempts: Vec<_> = op
            .entries
            .iter()
            .filter_map(|e| match e {
                CaptureEntry::QueueAttempt {
                    lane: CaptureQueueLane::Market,
                    incoming,
                    evicted_front,
                    result,
                } => Some((incoming, evicted_front, result)),
                _ => None,
            })
            .collect();
        if overflow == OverflowPolicy::DropNewest {
            assert_eq!(*attempts[1].2, Some(Ok(PushOutcome::DroppedNewest)));
            assert!(attempts[1].1.is_none());
        } else {
            assert_eq!(
                *attempts[1].2,
                Some(Ok(PushOutcome::DroppedOldest { dropped: 1 }))
            );
            let Some(CaptureQueueItem::Market(old)) = attempts[1].1 else {
                panic!("evicted front missing")
            };
            let MarketEvent::Trade(trade) = &old.events[0].payload else {
                unreachable!()
            };
            assert_eq!(trade.price, Price(Fixed::new(1, 0)));
        }
        let (_, market) = &op.after.queues.as_ref().unwrap()[0];
        let CaptureQueueItem::Market(retained) = &market[0] else {
            unreachable!()
        };
        let MarketEvent::Trade(trade) = &retained.events[0].payload else {
            unreachable!()
        };
        assert_eq!(
            trade.price,
            Price(Fixed::new(
                if overflow == OverflowPolicy::DropNewest {
                    1
                } else {
                    2
                },
                0
            ))
        );
    }
}

struct ControlSystem;
impl SessionMachine for ControlSystem {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        out: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if matches!(input, SessionInput::Control { .. }) {
            out.push(SessionAction::EmitSystem(
                SystemEvent::SubscriptionStateChanged {
                    state: "new".into(),
                },
            ));
        }
        Ok(())
    }
}
#[test]
fn suppressed_queue_error_is_not_mistaken_for_engine_failure_or_admission() {
    let mut session = CaptureSession::new(
        Box::new(ControlSystem),
        CaptureConfig {
            dispatch_capacity: 1,
            ..config()
        },
    )
    .unwrap();
    session
        .execute(CaptureInput::System(
            SystemEvent::SubscriptionStateChanged {
                state: "occupied".into(),
            },
        ))
        .unwrap();
    let operation = session
        .execute(CaptureInput::Control {
            command: SessionCommand::Resync(InstrumentId(1)),
            now: TimestampNs(1),
        })
        .unwrap();
    assert_eq!(operation.outcome, CaptureOutcome::Returned(Ok(())));
    assert_eq!(operation.completeness, CaptureCompleteness::Complete);
    assert!(operation.entries.iter().any(|e| matches!(
        e,
        CaptureEntry::SuppressedDiagnostic(EngineError::Dispatch(DispatchError::FailEngine))
    )));
    assert!(operation.entries.iter().any(|e| matches!(
        e,
        CaptureEntry::QueueAttempt {
            result: Some(Err(EngineError::Dispatch(DispatchError::FailEngine))),
            ..
        }
    )));
    assert_eq!(
        actions(&operation)[0].disposition,
        ActionDisposition::Completed(Ok(()))
    );
    assert_eq!(operation.before.queues, operation.after.queues);
}

struct MarkAfterBatch;
impl SessionMachine for MarkAfterBatch {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        out: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if matches!(input, SessionInput::TextFrame { .. }) {
            out.extend([
                SessionAction::EmitBatch(batch(1)),
                SessionAction::MarkLive,
                SessionAction::SendText(Bytes::from_static(b"not reached")),
            ]);
        }
        Ok(())
    }
}
#[test]
fn mark_live_partial_mutation_then_status_failure_is_captured_without_rollback() {
    let mut session = CaptureSession::new(
        Box::new(MarkAfterBatch),
        CaptureConfig {
            dispatch_capacity: 1,
            ..config()
        },
    )
    .unwrap();
    let op = session.execute(text("go")).unwrap();
    assert!(matches!(
        actions(&op)[1].disposition,
        ActionDisposition::Completed(Err(EngineError::Dispatch(DispatchError::FailEngine)))
    ));
    assert_eq!(actions(&op)[2].disposition, ActionDisposition::Buffered);
    assert_eq!(op.after.pending_market, 1);
    assert_eq!(op.after.pending_writes, 0);
    assert_eq!(op.after.lifecycle, SessionLifecycle::Degraded);
}

#[test]
fn capture_entry_limit_is_latched_independently_of_successful_execution() {
    let mut session = CaptureSession::new(
        Box::new(Emit {
            count: 1,
            fail: false,
            mutate: false,
        }),
        CaptureConfig {
            max_entries: 1,
            ..config()
        },
    )
    .unwrap();
    let op = session.execute(text("go")).unwrap();
    assert_eq!(op.outcome, CaptureOutcome::Returned(Ok(())));
    assert_eq!(
        op.completeness,
        CaptureCompleteness::Incomplete(CaptureFault::Entries)
    );
    assert_eq!(op.after.pending_market, 1);
    assert!(op.after.queues.is_none());
    assert_eq!(op.entries.len(), 1);
    assert!(session.is_poisoned());
    assert!(matches!(
        session.execute(text("no further execution")),
        Err(EngineError::Stopped)
    ));
}

struct OverflowThenQuiet {
    calls: usize,
}
impl SessionMachine for OverflowThenQuiet {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        out: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if matches!(input, SessionInput::TextFrame { .. }) {
            self.calls += 1;
            if self.calls == 1 {
                for _ in 0..1030 {
                    out.push(SessionAction::CancelTimer(1));
                }
                return Err(AdapterError::Parse("overflow before failure".into()));
            }
        }
        Ok(())
    }
}
#[test]
fn overflow_observation_does_not_consume_the_counter_before_native_reporting() {
    let mut session = CaptureSession::new(
        Box::new(OverflowThenQuiet { calls: 0 }),
        CaptureConfig {
            dispatch_capacity: 1,
            ..config()
        },
    )
    .unwrap();
    let first = session.execute(text("overflow")).unwrap();
    let invocation = first
        .entries
        .iter()
        .find(|e| matches!(e, CaptureEntry::Invocation { .. }))
        .unwrap();
    assert!(matches!(
        invocation,
        CaptureEntry::Invocation {
            retained_actions: 1024,
            dropped_before: 0,
            dropped_after: 6,
            ..
        }
    ));
    let second = session.execute(text("quiet")).unwrap();
    assert!(second.entries.iter().any(|e| matches!(
        e,
        CaptureEntry::Invocation {
            dropped_before: 6,
            dropped_after: 6,
            ..
        }
    )));
    assert!(
        second
            .entries
            .iter()
            .any(|e| matches!(e, CaptureEntry::ActionBufferDropsConsumed(6)))
    );
}

struct Dynamic {
    committed: usize,
}
impl SessionMachine for Dynamic {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        out: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if matches!(input, SessionInput::TextFrame { .. }) {
            out.push(SessionAction::EmitBatch(batch(self.committed as i128)));
        }
        Ok(())
    }
    fn prepare_dynamic_subscription(
        &self,
        command: &SessionCommand,
    ) -> Result<SubscriptionWireAction, AdapterError> {
        if matches!(command,SessionCommand::Subscribe(s) if s==&["reject"]) {
            return Err(AdapterError::Subscription("refused".into()));
        }
        Ok(SubscriptionWireAction::Text(Bytes::from_static(
            b"public subscription",
        )))
    }
    fn commit_dynamic_subscription(&mut self, _: &SessionCommand) {
        self.committed += 1;
    }
}
#[test]
fn dynamic_refusal_and_commit_preserve_wire_evidence_with_no_legacy_recorder() {
    let mut session = CaptureSession::new(Box::new(Dynamic { committed: 0 }), config()).unwrap();
    let refused = session
        .execute(CaptureInput::Control {
            command: SessionCommand::Subscribe(vec!["reject".into()]),
            now: TimestampNs(1),
        })
        .unwrap();
    assert_eq!(refused.before, refused.after);
    assert!(refused.entries.iter().any(|e| matches!(
        e,
        CaptureEntry::SubscriptionPrepared(Err(AdapterError::Subscription(_)))
    )));
    assert!(
        !refused
            .entries
            .iter()
            .any(|e| matches!(e, CaptureEntry::SubscriptionCommitted))
    );
    let accepted = session
        .execute(CaptureInput::Control {
            command: SessionCommand::Subscribe(vec!["BTC".into()]),
            now: TimestampNs(2),
        })
        .unwrap();
    assert!(
        accepted
            .entries
            .iter()
            .any(|e| matches!(e, CaptureEntry::SubscriptionCommitted))
    );
    assert!(
        records(&accepted)
            .iter()
            .any(|r| matches!(r, FeedRecord::Frame { opcode: 8, .. }))
    );
    assert_eq!(accepted.after.pending_writes, 1);
    let next = session.execute(text("probe commit")).unwrap();
    let batch = records(&next)
        .into_iter()
        .find_map(|r| match r {
            FeedRecord::Batch(b) => Some(b),
            _ => None,
        })
        .unwrap();
    let MarketEvent::Trade(t) = &batch.events[0].payload else {
        unreachable!()
    };
    assert_eq!(t.price, Price(Fixed::new(1, 0)));
}

#[test]
fn http_input_is_present_even_when_encoding_fails_before_the_callback() {
    let mut session = CaptureSession::new(
        Box::new(Emit {
            count: 1,
            fail: false,
            mutate: false,
        }),
        config(),
    )
    .unwrap();
    let input = CaptureInput::Http {
        request_id: 55,
        response: HttpResponse {
            status: 200,
            headers: vec![("x".into(), "v".into()); 1025],
            body: Bytes::from_static(b"original response"),
        },
        received: stamp(),
    };
    let op = session.execute(input.clone()).unwrap();
    assert_eq!(op.original_input, input);
    assert_eq!(
        op.outcome,
        CaptureOutcome::Returned(Err(EngineError::Recording(
            marketfeed_recording::RecordingError::InvalidHeader
        )))
    );
    assert_eq!(op.after.frame_seq, 1);
    assert!(
        !op.entries
            .iter()
            .any(|e| matches!(e, CaptureEntry::Invocation { .. }))
    );
}

struct TimerMachine {
    forever: bool,
}
impl SessionMachine for TimerMachine {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        out: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Connected { .. } => out.extend([
                SessionAction::ScheduleTimer(TimerSpec {
                    timer_id: 1,
                    fire_at: TimestampNs(10),
                }),
                SessionAction::ScheduleTimer(TimerSpec {
                    timer_id: 2,
                    fire_at: TimestampNs(10),
                }),
                SessionAction::ScheduleTimer(TimerSpec {
                    timer_id: 3,
                    fire_at: TimestampNs(10),
                }),
            ]),
            SessionInput::Timer { timer_id: 1, .. } if !self.forever => out.extend([
                SessionAction::CancelTimer(2),
                SessionAction::ScheduleTimer(TimerSpec {
                    timer_id: 3,
                    fire_at: TimestampNs(100),
                }),
            ]),
            SessionInput::Timer { timer_id, now } if self.forever => {
                out.push(SessionAction::ScheduleTimer(TimerSpec {
                    timer_id,
                    fire_at: now,
                }))
            }
            _ => {}
        }
        Ok(())
    }
}
#[test]
fn one_timer_poll_preserves_cancellation_postponement_and_sixty_four_fire_cap() {
    for forever in [false, true] {
        let mut session =
            CaptureSession::new(Box::new(TimerMachine { forever }), config()).unwrap();
        session
            .execute(CaptureInput::Connected {
                connection: ConnectionId(70),
                now: TimestampNs(1),
            })
            .unwrap();
        let op = session
            .execute(CaptureInput::TimerPoll {
                now: TimestampNs(10),
            })
            .unwrap();
        let fired: Vec<_> = op
            .entries
            .iter()
            .filter_map(|e| match e {
                CaptureEntry::Invocation {
                    kind: InvocationKind::Timer { timer_id, .. },
                    ..
                } => Some(*timer_id),
                _ => None,
            })
            .collect();
        if forever {
            assert_eq!(fired.len(), 64);
            assert_eq!(fired[..6], [1, 2, 3, 1, 2, 3]);
        } else {
            assert_eq!(fired, vec![1]);
            assert_eq!(op.after.timers, vec![(3, TimestampNs(100))]);
        }
        assert_eq!(op.completeness, CaptureCompleteness::Complete);
    }
}

struct Stateful {
    calls: i128,
}
impl SessionMachine for Stateful {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        out: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if let SessionInput::TextFrame { bytes, .. } = input {
            self.calls += 1;
            out.push(SessionAction::EmitBatch(batch(self.calls)));
            if bytes == b"fail" {
                return Err(AdapterError::Parse("after mutation".into()));
            }
        }
        Ok(())
    }
}
#[test]
fn actual_runner_reexecution_matches_failed_prefix_and_persistent_reconnect_machine() {
    let inputs = vec![
        CaptureInput::Connected {
            connection: ConnectionId(700),
            now: TimestampNs(1),
        },
        text("fail"),
        text("probe"),
        CaptureInput::TransportLost {
            reason: DisconnectReason::TransportError,
            now: TimestampNs(2),
        },
        CaptureInput::Connected {
            connection: ConnectionId(701),
            now: TimestampNs(3),
        },
        text("probe after reconnect"),
    ];
    let mut original = CaptureSession::new(Box::new(Stateful { calls: 0 }), config()).unwrap();
    let mut replay = CaptureSession::new(Box::new(Stateful { calls: 0 }), config()).unwrap();
    let mut captures = Vec::new();
    for input in inputs {
        let capture = original.execute(input).unwrap();
        assert_eq!(
            replay.execute(capture.original_input.clone()).unwrap(),
            capture
        );
        captures.push(capture);
    }
    assert_eq!(
        (
            captures[5].ordinal,
            captures[5].generation,
            captures[5].after.frame_seq,
            captures[5].after.connection
        ),
        (6, 2, 3, ConnectionId(701))
    );
    let records = records(&captures[5]);
    let batch = records
        .iter()
        .find_map(|r| match r {
            FeedRecord::Batch(b) => Some(b),
            _ => None,
        })
        .unwrap();
    let MarketEvent::Trade(trade) = &batch.events[0].payload else {
        unreachable!()
    };
    assert_eq!(trade.price, Price(Fixed::new(3, 0)));
}

struct Sensitive;
impl SessionMachine for Sensitive {
    fn on_input(
        &mut self,
        _: SessionInput<'_>,
        out: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        out.push(SessionAction::SendSensitiveText(SensitiveBytes::new(
            Bytes::from_static(b"fake-sensitive-payload-never-copy"),
        )));
        Ok(())
    }
}
#[test]
fn explicit_sensitive_action_is_not_cloned_into_trace_or_state_and_poisoning_prevents_later_disclosure()
 {
    let mut session = CaptureSession::new(Box::new(Sensitive), config()).unwrap();
    let op = session.execute(text("public trigger")).unwrap();
    assert_eq!(
        op.completeness,
        CaptureCompleteness::Incomplete(CaptureFault::SensitiveAction)
    );
    assert_eq!(op.after.pending_writes, 1);
    assert!(op.after.queues.is_none());
    assert!(!format!("{op:?}").contains("fake-sensitive-payload-never-copy"));
    assert!(matches!(
        session.execute(text("must not expose old queue")),
        Err(EngineError::Stopped)
    ));
}

#[test]
fn rejected_preflight_input_does_not_consume_operation_or_connection_generation() {
    let mut session = CaptureSession::new(
        Box::new(Emit {
            count: 0,
            fail: false,
            mutate: false,
        }),
        CaptureConfig {
            max_bytes: 8192,
            ..config()
        },
    )
    .unwrap();
    assert!(
        session
            .execute(CaptureInput::Text {
                payload: vec![1; 10_000],
                received: stamp()
            })
            .is_err()
    );
    assert!(
        session
            .execute(CaptureInput::Connected {
                connection: ConnectionId(0),
                now: TimestampNs(0)
            })
            .is_err()
    );
    let op = session
        .execute(CaptureInput::Connected {
            connection: ConnectionId(80),
            now: TimestampNs(1),
        })
        .unwrap();
    assert_eq!((op.ordinal, op.generation), (1, 1));
    assert_eq!(op.before.frame_seq, 0);
}

struct TerminalFault {
    panic: bool,
}
impl SessionMachine for TerminalFault {
    fn on_input(
        &mut self,
        _: SessionInput<'_>,
        out: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        out.push(SessionAction::EmitBatch(batch(1)));
        if self.panic {
            panic!("fake panic detail must not be copied into the capture");
        }
        Err(AdapterError::Protocol(
            "oversized-native-error".repeat(65_536),
        ))
    }
}
#[test]
fn oversized_native_error_is_omitted_instead_of_bypassing_retention_budget() {
    let cfg = CaptureConfig {
        max_bytes: 8192,
        ..config()
    };
    let mut session = CaptureSession::new(Box::new(TerminalFault { panic: false }), cfg).unwrap();
    let op = session.execute(text("small public input")).unwrap();
    assert_eq!(
        op.outcome,
        CaptureOutcome::ReturnedErrorDetailsOmitted(CaptureErrorClass::Adapter)
    );
    assert_eq!(
        op.completeness,
        CaptureCompleteness::Incomplete(CaptureFault::Bytes)
    );
    assert!(op.retained_bytes_bound <= cfg.max_bytes);
    assert!(!format!("{op:?}").contains("oversized-native-error"));
    assert_eq!(op.after.lifecycle, SessionLifecycle::Degraded);
    assert_eq!(op.after.pending_market, 0);
    assert!(op.after.queues.is_none());
    assert!(matches!(
        session.execute(text("no retry")),
        Err(EngineError::Stopped)
    ));
}
#[test]
fn callback_panic_poisoning_is_explicit_and_never_claims_complete_capture() {
    let mut session =
        CaptureSession::new(Box::new(TerminalFault { panic: true }), config()).unwrap();
    let op = session.execute(text("panic input")).unwrap();
    assert_eq!(op.outcome, CaptureOutcome::Panicked);
    assert_eq!(
        op.completeness,
        CaptureCompleteness::Incomplete(CaptureFault::Panicked)
    );
    assert!(op.after.queues.is_none());
    assert!(actions(&op).is_empty()); // unwound buffered actions are not claimed captured
    assert!(!format!("{op:?}").contains("fake panic detail"));
    assert!(matches!(
        session.execute(text("no retry")),
        Err(EngineError::Stopped)
    ));
}

struct ExtremeValues;
impl SessionMachine for ExtremeValues {
    fn on_input(
        &mut self,
        _: SessionInput<'_>,
        out: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        for value in [i128::MIN, i128::MAX] {
            out.push(SessionAction::EmitBatch(batch(value)));
        }
        Ok(())
    }
}
#[test]
fn captured_native_records_and_queue_snapshots_preserve_i128_extrema() {
    let mut session = CaptureSession::new(Box::new(ExtremeValues), config()).unwrap();
    let op = session.execute(text("extrema")).unwrap();
    assert_eq!(op.completeness, CaptureCompleteness::Complete);
    let prices: Vec<_> = records(&op)
        .into_iter()
        .filter_map(|record| {
            if let FeedRecord::Batch(batch) = record {
                if let MarketEvent::Trade(trade) = &batch.events[0].payload {
                    return Some(trade.price);
                }
            }
            None
        })
        .collect();
    assert_eq!(
        prices,
        vec![
            Price(Fixed::new(i128::MIN, 0)),
            Price(Fixed::new(i128::MAX, 0))
        ]
    );
    let retained: Vec<_> = op
        .after
        .queues
        .as_ref()
        .unwrap()
        .iter()
        .flat_map(|(_, items)| items)
        .filter_map(|item| match item {
            CaptureQueueItem::Market(batch) => match &batch.events[0].payload {
                MarketEvent::Trade(trade) => Some(trade.price),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(retained, prices);
}

struct LargeWrite;
impl SessionMachine for LargeWrite {
    fn on_input(
        &mut self,
        _: SessionInput<'_>,
        out: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        out.push(SessionAction::SendText(Bytes::from(vec![b'x'; 32_768])));
        Ok(())
    }
}
#[test]
fn byte_limit_does_not_fake_native_failure_or_expose_unbudgeted_postmutation_queue() {
    let cfg = CaptureConfig {
        max_bytes: 8192,
        ..config()
    };
    let mut session = CaptureSession::new(Box::new(LargeWrite), cfg).unwrap();
    let op = session.execute(text("small input")).unwrap();
    assert_eq!(op.outcome, CaptureOutcome::Returned(Ok(())));
    assert_eq!(
        op.completeness,
        CaptureCompleteness::Incomplete(CaptureFault::Bytes)
    );
    assert_eq!(op.after.pending_writes, 1); // actual engine execution still occurred privately
    assert!(op.after.queues.is_none());
    assert!(op.retained_bytes_bound <= cfg.max_bytes);
    assert!(matches!(
        session.execute(text("no retry")),
        Err(EngineError::Stopped)
    ));
}
