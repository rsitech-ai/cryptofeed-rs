use super::*;
use crate::causal::tests::{Emit, config, stamp};

fn history3() -> OwnerHistory {
    let mut h = history();
    h.envelope_version = 3;
    h
}
fn permit(h: &mut OwnerHistory, id: u64, native: u64, generation: u64, now: u64) -> u64 {
    apply(
        h,
        OwnerEventDto::ExecutionPermitted {
            input_id: id,
            native_ordinal: native,
            generation,
        },
        now,
    );
    h.ordinal
}
fn completion(
    h: &OwnerHistory,
    id: u64,
    token: u64,
    operation: &CapturedOperation,
) -> OwnerEventDto {
    OwnerEventDto::ExecutionCompleted {
        input_id: id,
        permit_ordinal: token,
        operation: Box::new(OperationDto::from_capture(operation, &h.identity).unwrap()),
    }
}
#[test]
fn permitted_completion_after_terminal_fence_is_retained_with_terminal_latched() {
    let (first, _) = operations(false);
    let mut h = history3();
    start(&mut h);
    let input = ingress(&h, 1, &first.original_input);
    apply(&mut h, input, 1);
    let token = permit(&mut h, 1, 1, 1, 2);
    apply(
        &mut h,
        OwnerEventDto::Revoked {
            through_input_id: 1,
            cause: OwnerTerminalCause::Revoked,
        },
        3,
    );
    let event = completion(&h, 1, token, &first);
    apply(&mut h, event, 4);
    assert!(h.is_terminal());
    assert_eq!(h.native_ordinal(), 1);
    assert_eq!(
        h.uncertainties(),
        vec![OwnerPrefixUncertainty::CommittedWithoutRelease {
            input_id: 1,
            operation_ordinal: 5
        }]
    );
    reject_unchanged(
        &mut h,
        OwnerEventDto::ReleaseIntent {
            input_id: 1,
            operation_ordinal: 5,
            outputs_eligible: false,
        },
        5,
        1,
    );
    apply(&mut h, OwnerEventDto::Withheld { input_id: 1 }, 5);
    assert!(h.is_terminal());
    assert!(h.uncertainties().is_empty());
}
#[test]
fn permit_wrong_duplicate_and_completion_tokens_reject_without_prefix_changes() {
    let (first, _) = operations(false);
    let mut h = history3();
    start(&mut h);
    let input = ingress(&h, 1, &first.original_input);
    apply(&mut h, input, 1);
    reject_unchanged(
        &mut h,
        OwnerEventDto::ExecutionPermitted {
            input_id: 1,
            native_ordinal: 2,
            generation: 1,
        },
        2,
        0,
    );
    reject_unchanged(
        &mut h,
        OwnerEventDto::ExecutionPermitted {
            input_id: 1,
            native_ordinal: 1,
            generation: 0,
        },
        2,
        0,
    );
    reject_unchanged(
        &mut h,
        OwnerEventDto::ExecutionPermitted {
            input_id: 1,
            native_ordinal: 1,
            generation: 1,
        },
        1000,
        0,
    );
    let event = completion(&h, 1, 1, &first);
    reject_unchanged(&mut h, event, 2, 1);
    let token = permit(&mut h, 1, 1, 1, 2);
    reject_unchanged(
        &mut h,
        OwnerEventDto::ExecutionPermitted {
            input_id: 1,
            native_ordinal: 1,
            generation: 1,
        },
        3,
        0,
    );
    let event = completion(&h, 1, token - 1, &first);
    reject_unchanged(&mut h, event, 3, 1);
    let mut event = completion(&h, 1, token, &first);
    if let OwnerEventDto::ExecutionCompleted { operation, .. } = &mut event {
        operation.route.ordinal = 2;
    }
    reject_unchanged(&mut h, event, 3, 1);
    let event = completion(&h, 1, token, &first);
    apply(&mut h, event, 3);
    let event = completion(&h, 1, token, &first);
    reject_unchanged(&mut h, event, 4, 1);
}
#[test]
fn completion_codec_requires_a_prior_nonzero_permit_reference() {
    let (first, _) = operations(false);
    let h = history3();
    for token in [0, 4, u64::MAX] {
        let dto = OwnerHistoryDto {
            schema_version: 2,
            route: CausalRoute {
                ordinal: 4,
                generation: first.generation,
                venue: h.identity.venue,
                session: h.identity.session,
                bootstrap_id: h.identity.bootstrap_id.clone(),
            },
            observed_ns: 3,
            event: completion(&h, 1, token, &first),
        };
        assert!(encode_owner_history(&dto, &h.identity, MAX_OPERATION_BYTES).is_err());
    }
}
#[test]
fn permit_only_is_unknown_and_proved_preflight_can_settle_after_revoke() {
    let (first, _) = operations(false);
    let mut h = history3();
    start(&mut h);
    let input = ingress(&h, 1, &first.original_input);
    apply(&mut h, input, 1);
    let token = permit(&mut h, 1, 1, 1, 2);
    assert_eq!(
        h.uncertainties(),
        vec![OwnerPrefixUncertainty::ExecutionUnknown { input_id: 1 }]
    );
    apply(
        &mut h,
        OwnerEventDto::Revoked {
            through_input_id: 1,
            cause: OwnerTerminalCause::Revoked,
        },
        3,
    );
    apply(
        &mut h,
        OwnerEventDto::ExecutionNotInvoked {
            input_id: 1,
            permit_ordinal: token,
            cause: OwnerNotExecutedCause::Preflight,
        },
        4,
    );
    assert!(h.is_terminal());
    assert_eq!(h.native_ordinal(), 0);
    assert!(h.uncertainties().is_empty());
}
#[test]
fn indeterminate_execution_remains_unknown_while_queued_inputs_settle_without_execution() {
    let (first, _) = operations(false);
    let mut h = history3();
    start(&mut h);
    let input = ingress(&h, 1, &first.original_input);
    apply(&mut h, input, 1);
    let token = permit(&mut h, 1, 1, 1, 2);
    let input = ingress(&h, 2, &CaptureInput::Stop);
    apply(&mut h, input, 3);
    apply(
        &mut h,
        OwnerEventDto::ExecutionIndeterminate {
            input_id: 1,
            permit_ordinal: token,
            cause: OwnerIndeterminateCause::Panic,
        },
        4,
    );
    let event = completion(&h, 1, token, &first);
    reject_unchanged(&mut h, event, 5, 1);
    apply(
        &mut h,
        OwnerEventDto::NotExecuted {
            input_id: 2,
            cause: OwnerNotExecutedCause::Terminal,
        },
        5,
    );
    reject_unchanged(
        &mut h,
        OwnerEventDto::NotExecuted {
            input_id: 1,
            cause: OwnerNotExecutedCause::Terminal,
        },
        6,
        0,
    );
    assert_eq!(
        h.uncertainties(),
        vec![OwnerPrefixUncertainty::ExecutionUnknown { input_id: 1 }]
    );
    assert_eq!(h.native_pending_operations, 1);
    assert_eq!(h.pending_inputs(), 1);
}
#[test]
fn runtime_policy_rejects_malformed_owner_and_oversized_numbers_without_panicking() {
    let mut owner = JournalOwnerPolicyDto {
        max_input_bytes: 8192,
        ..Default::default()
    };
    let policy = HistoryRuntimePolicyDto {
        schema_version: 1,
        max_pending_observations: 16,
        max_pending_observation_bytes: 2 << 20,
        max_record_bytes: 128 << 10,
        max_finalization_age: std::time::Duration::from_secs(2).into(),
    };
    policy.validate(&owner).unwrap();
    owner.max_input_bytes = u64::MAX;
    assert!(policy.validate(&owner).is_err());
    owner.max_input_bytes = 8192;
    let mut bad = policy.clone();
    bad.max_record_bytes = u64::MAX;
    assert!(bad.validate(&owner).is_err());
    let mut bad = policy.clone();
    bad.max_pending_observation_bytes = u64::MAX;
    assert!(bad.validate(&owner).is_err());
    let mut bad = policy.clone();
    bad.max_finalization_age = std::time::Duration::ZERO.into();
    assert!(bad.validate(&owner).is_err());
}
#[test]
fn old_and_new_owner_event_semantics_cannot_be_mixed() {
    let (first, _) = operations(false);
    let old = history();
    let modern = history3();
    let new_event = OwnerHistoryDto {
        schema_version: 1,
        route: CausalRoute {
            venue: old.identity.venue,
            session: old.identity.session,
            bootstrap_id: old.identity.bootstrap_id.clone(),
            ordinal: 1,
            generation: 0,
        },
        observed_ns: 0,
        event: OwnerEventDto::ExecutionPermitted {
            input_id: 1,
            native_ordinal: 1,
            generation: 1,
        },
    };
    assert!(encode_owner_history(&new_event, &old.identity, MAX_OPERATION_BYTES).is_err());
    let mut bad = new_event;
    bad.schema_version = 2;
    bad.event = OwnerEventDto::executed(1, &first, &modern.identity).unwrap();
    assert!(encode_owner_history(&bad, &modern.identity, MAX_OPERATION_BYTES).is_err());
}
#[test]
fn permitted_native_extrema_and_actual_drains_roundtrip_without_numeric_projection() {
    let (first, text) = operations(false);
    let mut h = history3();
    start(&mut h);
    for (id, op) in [(1, &first), (2, &text)] {
        let now = h.observed_ns + 1;
        let input = ingress(&h, id, &op.original_input);
        apply(&mut h, input, now);
        let token = permit(&mut h, id, op.ordinal, op.generation, now + 1);
        let event = completion(&h, id, token, op);
        let wire = encoded(&h, event.clone(), now + 2, op.generation);
        assert_eq!(
            decode_owner_history(&wire, &h.identity).unwrap().event,
            event
        );
        h.apply(&wire).unwrap();
        let operation_ordinal = h.ordinal;
        apply(
            &mut h,
            OwnerEventDto::ReleaseIntent {
                input_id: id,
                operation_ordinal,
                outputs_eligible: true,
            },
            now + 3,
        );
        let intent_ordinal = h.ordinal;
        let result = apply(
            &mut h,
            OwnerEventDto::HandoffObserved {
                input_id: id,
                intent_ordinal,
            },
            now + 4,
        )
        .unwrap();
        assert_eq!(
            *result.operation,
            OperationDto::from_capture(op, &h.identity).unwrap()
        );
        if id == 2 {
            assert!(
                result
                    .released_items()
                    .any(|i| matches!(i, ItemDto::Market(_)))
            );
        }
    }
    assert!(h.uncertainties().is_empty());
    assert_eq!(h.native_ordinal(), 2);
}

fn history() -> OwnerHistory {
    OwnerHistory {
        envelope_version: 2,
        record_bytes: MAX_OPERATION_BYTES,
        permit: None,
        identity: OperationIdentity {
            venue: 21,
            session: 77,
            bootstrap_id: "a".repeat(64),
        },
        policy: JournalOwnerPolicyDto::default(),
        limits: OwnerHistoryLimits {
            max_retained_input_bytes: 16 * 1024 * 1024,
            max_input_bytes: 2 * 1024 * 1024,
            max_record_bytes: MAX_OPERATION_BYTES,
        },
        ordinal: 0,
        native_ordinal: 0,
        generation: 0,
        observed_ns: 0,
        started: false,
        terminal: false,
        accepted: 0,
        fence: 0,
        source: 0,
        source_active: false,
        physical_source: 0,
        physical_active: false,
        capture: config(OverflowPolicy::DropOldest).into(),
        runner_policy: config(OverflowPolicy::DropOldest).runtime_policy(),
        pending_bytes: 0,
        pending_owned_bytes: 0,
        native_pending_operations: 0,
        pending: BTreeMap::new(),
        active: None,
        previous_after: None,
    }
}
fn operations(fail: bool) -> (CapturedOperation, CapturedOperation) {
    let mut capture =
        CaptureSession::new(Box::new(Emit { fail }), config(OverflowPolicy::DropOldest)).unwrap();
    let first = capture
        .execute_turn(
            CaptureInput::Connected {
                connection: ConnectionId(7),
                now: TimestampNs(1),
            },
            CaptureDrainPolicy::NullDispatchTakeOutbound,
        )
        .unwrap();
    let second = capture
        .execute_turn(
            CaptureInput::Text {
                payload: b"original\0\xff".to_vec(),
                received: stamp(),
            },
            CaptureDrainPolicy::NullDispatchTakeOutbound,
        )
        .unwrap();
    (first, second)
}
fn encoded(h: &OwnerHistory, event: OwnerEventDto, now: u64, generation: u64) -> CausalEnvelope {
    encode_owner_history(
        &OwnerHistoryDto {
            schema_version: h.envelope_version - 1,
            route: CausalRoute {
                venue: h.identity.venue,
                session: h.identity.session,
                bootstrap_id: h.identity.bootstrap_id.clone(),
                ordinal: h.ordinal + 1,
                generation,
            },
            observed_ns: now,
            event,
        },
        &h.identity,
        MAX_OPERATION_BYTES,
    )
    .unwrap()
}
fn apply(h: &mut OwnerHistory, event: OwnerEventDto, now: u64) -> Option<RecordedOwnerHandoff> {
    let generation = match &event {
        OwnerEventDto::Executed { operation, .. }
        | OwnerEventDto::ExecutionCompleted { operation, .. } => operation.route.generation,
        _ => h.generation,
    };
    let wire = encoded(h, event, now, generation);
    h.apply(&wire).unwrap()
}
fn ingress(h: &OwnerHistory, id: u64, input: &CaptureInput) -> OwnerEventDto {
    OwnerEventDto::ingress(id, ConnectionId(7), 1000, 1024, input, &h.identity).unwrap()
}
fn start(h: &mut OwnerHistory) {
    assert!(apply(h, OwnerEventDto::Started { unix_ns: i64::MIN }, 0).is_none());
}
fn connected(h: &mut OwnerHistory, operation: &CapturedOperation) {
    start(h);
    let input = ingress(h, 1, &operation.original_input);
    apply(h, input, 1);
    let op = OwnerEventDto::executed(1, operation, &h.identity).unwrap();
    apply(h, op, 2);
    apply(
        h,
        OwnerEventDto::ReleaseIntent {
            input_id: 1,
            operation_ordinal: 3,
            outputs_eligible: true,
        },
        3,
    );
    assert!(
        apply(
            h,
            OwnerEventDto::HandoffObserved {
                input_id: 1,
                intent_ordinal: 4
            },
            4
        )
        .is_some()
    );
}
fn text_intent(h: &mut OwnerHistory, text: &CapturedOperation) {
    let input = ingress(h, 2, &text.original_input);
    apply(h, input, 5);
    let op = OwnerEventDto::executed(2, text, &h.identity).unwrap();
    apply(h, op, 6);
    apply(
        h,
        OwnerEventDto::ReleaseIntent {
            input_id: 2,
            operation_ordinal: 7,
            outputs_eligible: true,
        },
        7,
    );
}
fn reject_unchanged(h: &mut OwnerHistory, event: OwnerEventDto, now: u64, generation: u64) {
    let before = h.clone();
    let wire = encoded(h, event, now, generation);
    assert!(h.apply(&wire).is_err());
    assert_eq!(
        *h, before,
        "rejected physical record must not change ledger"
    );
}

#[test]
fn physical_prefixes_keep_intent_uncertainty_and_separate_native_ordinals() {
    let (first, text) = operations(false);
    let mut h = history();
    connected(&mut h, &first);
    let input = ingress(&h, 2, &text.original_input);
    apply(&mut h, input, 5);
    assert_eq!(
        h.uncertainties(),
        [OwnerPrefixUncertainty::ExecutionUnknown { input_id: 2 }]
    );
    let op = OwnerEventDto::executed(2, &text, &h.identity).unwrap();
    apply(&mut h, op, 6);
    assert_eq!(
        h.uncertainties(),
        [OwnerPrefixUncertainty::CommittedWithoutRelease {
            input_id: 2,
            operation_ordinal: 7
        }]
    );
    apply(
        &mut h,
        OwnerEventDto::ReleaseIntent {
            input_id: 2,
            operation_ordinal: 7,
            outputs_eligible: true,
        },
        7,
    );
    assert_eq!(
        h.uncertainties(),
        [OwnerPrefixUncertainty::HandoffUnconfirmed {
            input_id: 2,
            intent_ordinal: 8
        }]
    );
    let released = apply(
        &mut h,
        OwnerEventDto::HandoffObserved {
            input_id: 2,
            intent_ordinal: 8,
        },
        8,
    )
    .unwrap();
    assert_eq!(
        (h.ordinal(), h.native_ordinal(), h.pending_inputs()),
        (9, 2, 0)
    );
    assert!(h.uncertainties().is_empty());
    assert!(
        released
            .operation
            .matches_capture(&text, h.identity())
            .unwrap()
    );
    let InputDto::Text { payload, .. } = &released.operation.input else {
        panic!()
    };
    assert_eq!(payload.bytes().unwrap(), b"original\0\xff");
    let mut values = Vec::new();
    for trace in &released.operation.entries {
        if let EffectDto::Invocation { actions, .. } = &trace.effect {
            for action in actions {
                if let ActionDto::EmitBatch(leaf) = &action.action {
                    let FeedRecord::Batch(batch) = leaf.record().unwrap() else {
                        panic!()
                    };
                    let MarketEvent::Trade(trade) = &batch.events[0].payload else {
                        panic!()
                    };
                    values.push((trade.price.0.coefficient, trade.price.0.scale));
                }
            }
        }
    }
    assert_eq!(values, [(i128::MIN, 255), (i128::MAX, 255)]);
    // DropOldest produced two batches, but only the actual remaining drain is released.
    assert_eq!(
        released
            .released_items()
            .filter(|v| matches!(v, ItemDto::Market(_)))
            .count(),
        1
    );
}

#[test]
fn fence_or_safety_ingress_prevents_old_intent_becoming_observed() {
    let (first, text) = operations(false);
    for safety_ingress in [false, true] {
        let mut h = history();
        connected(&mut h, &first);
        text_intent(&mut h, &text);
        if safety_ingress {
            let input = ingress(
                &h,
                3,
                &CaptureInput::Close {
                    payload: vec![0, 255],
                    received: stamp(),
                },
            );
            apply(&mut h, input, 8);
        } else {
            apply(
                &mut h,
                OwnerEventDto::Fence {
                    through_input_id: 3,
                },
                8,
            );
        }
        reject_unchanged(
            &mut h,
            OwnerEventDto::HandoffObserved {
                input_id: 2,
                intent_ordinal: 8,
            },
            9,
            1,
        );
        assert!(apply(&mut h, OwnerEventDto::Withheld { input_id: 2 }, 9).is_none());
        assert!(h.active.is_none());
        assert_eq!(h.pending_inputs(), usize::from(safety_ingress));
    }
}

#[test]
fn terminal_fence_at_same_input_id_never_releases_existing_intent() {
    let (first, text) = operations(false);
    let mut h = history();
    connected(&mut h, &first);
    text_intent(&mut h, &text);
    apply(
        &mut h,
        OwnerEventDto::Fence {
            through_input_id: 2,
        },
        8,
    );
    assert!(h.is_terminal());
    reject_unchanged(
        &mut h,
        OwnerEventDto::HandoffObserved {
            input_id: 2,
            intent_ordinal: 8,
        },
        9,
        1,
    );
}

#[test]
fn declared_native_capacity_and_consumer_encoded_retention_are_distinct() {
    let (first, text) = operations(false);
    let mut h = history();
    start(&mut h);
    let input = ingress(&h, 1, &first.original_input);
    apply(&mut h, input, 1);
    assert_eq!(h.pending_owned_bytes, 1024);
    assert!(h.pending_bytes < h.pending_owned_bytes);
    h.policy.max_pending_bytes = 1536;
    h.policy.max_input_bytes = 1024;
    let input = ingress(
        &h,
        2,
        &CaptureInput::TimerPoll {
            now: TimestampNs(2),
        },
    );
    reject_unchanged(&mut h, input, 2, 0);
    let mut h = history();
    start(&mut h);
    h.limits.max_input_bytes = 1;
    let input = ingress(&h, 1, &first.original_input);
    reject_unchanged(&mut h, input, 1, 0);
    let mut h = history();
    connected(&mut h, &first);
    let mut input = ingress(&h, 2, &text.original_input);
    if let OwnerEventDto::Ingress {
        owned_bytes_bound, ..
    } = &mut input
    {
        *owned_bytes_bound = 1;
    }
    reject_unchanged(&mut h, input, 5, 1);
}

#[test]
fn nonexecution_dispositions_preserve_native_counter_and_terminal_backlog() {
    let (first, _) = operations(false);
    for preflight in [false, true] {
        let mut h = history();
        start(&mut h);
        let input = ingress(&h, 1, &first.original_input);
        apply(&mut h, input, 1);
        let input = ingress(
            &h,
            2,
            &CaptureInput::TimerPoll {
                now: TimestampNs(9),
            },
        );
        apply(&mut h, input, 2);
        reject_unchanged(
            &mut h,
            OwnerEventDto::NotExecuted {
                input_id: 1,
                cause: OwnerNotExecutedCause::SourceConnection,
            },
            3,
            0,
        );
        apply(
            &mut h,
            OwnerEventDto::NotExecuted {
                input_id: 1,
                cause: if preflight {
                    OwnerNotExecutedCause::Preflight
                } else {
                    OwnerNotExecutedCause::Deadline
                },
            },
            if preflight { 3 } else { 1000 },
        );
        apply(
            &mut h,
            OwnerEventDto::NotExecuted {
                input_id: 2,
                cause: OwnerNotExecutedCause::Terminal,
            },
            1001,
        );
        assert_eq!(
            (h.native_ordinal(), h.pending_inputs(), h.is_terminal()),
            (0, 0, true)
        );
    }
}

#[test]
fn native_reservations_retire_at_commit_while_reader_retains_release() {
    let (first, _) = operations(false);
    for byte_cap in [false, true] {
        let mut h = history();
        h.policy.max_pending_operations = if byte_cap { 2 } else { 1 };
        if byte_cap {
            h.policy.max_pending_bytes = 1024;
            h.policy.max_input_bytes = 1024;
        }
        h.policy.validate().unwrap();
        start(&mut h);
        let input = ingress(&h, 1, &first.original_input);
        apply(&mut h, input, 1);
        let next = ingress(
            &h,
            2,
            &CaptureInput::TimerPoll {
                now: TimestampNs(2),
            },
        );
        reject_unchanged(&mut h, next, 2, 0);
        let op = OwnerEventDto::executed(1, &first, h.identity()).unwrap();
        apply(&mut h, op, 2);
        assert_eq!(
            (
                h.native_pending_operations,
                h.pending_owned_bytes,
                h.pending_inputs()
            ),
            (0, 0, 1)
        );
        let next = ingress(
            &h,
            2,
            &CaptureInput::TimerPoll {
                now: TimestampNs(2),
            },
        );
        apply(&mut h, next, 3);
        assert_eq!(
            (
                h.native_pending_operations,
                h.pending_owned_bytes,
                h.pending_inputs()
            ),
            (1, 1024, 2)
        );
        apply(
            &mut h,
            OwnerEventDto::ReleaseIntent {
                input_id: 1,
                operation_ordinal: 3,
                outputs_eligible: true,
            },
            4,
        );
        assert!(
            apply(
                &mut h,
                OwnerEventDto::HandoffObserved {
                    input_id: 1,
                    intent_ordinal: 5
                },
                5
            )
            .is_some()
        );
        assert_eq!(
            (
                h.native_pending_operations,
                h.pending_owned_bytes,
                h.pending_inputs()
            ),
            (1, 1024, 1)
        );
    }
}

#[test]
fn native_capture_limits_are_bound_separately_from_generic_codec_caps() {
    let (first, text) = operations(false);
    for entries in [false, true] {
        let mut h = history();
        connected(&mut h, &first);
        let input = ingress(&h, 2, &text.original_input);
        apply(&mut h, input, 5);
        if entries {
            h.capture.max_entries = text.observed_entries - 1;
        } else {
            h.capture.max_bytes = u64::try_from(text.retained_bytes_bound).unwrap() - 1;
        }
        let op = OwnerEventDto::executed(2, &text, h.identity()).unwrap();
        reject_unchanged(&mut h, op, 6, 1);
    }
    let mut h = history();
    connected(&mut h, &first);
    let input = ingress(&h, 2, &text.original_input);
    apply(&mut h, input, 5);
    let mut op = OperationDto::from_capture(&text, h.identity()).unwrap();
    let item = op
        .entries
        .iter()
        .find_map(|t| match &t.effect {
            EffectDto::QueueDrain { items, .. } => items
                .iter()
                .find(|x| matches!(x, ItemDto::Market(_)))
                .cloned(),
            _ => None,
        })
        .unwrap();
    let queue = op
        .after
        .queues
        .iter_mut()
        .find(|(lane, _)| *lane == LaneDto::Market)
        .unwrap();
    queue.1 = vec![item.clone(), item];
    op.after.pending_market = 2;
    op.validate().unwrap();
    reject_unchanged(
        &mut h,
        OwnerEventDto::Executed {
            input_id: 2,
            operation: Box::new(op),
        },
        6,
        1,
    );
}

#[test]
fn invalid_orders_references_deadlines_and_state_leave_prefix_unchanged() {
    let (first, text) = operations(false);
    let mut h = history();
    connected(&mut h, &first);
    reject_unchanged(
        &mut h,
        OwnerEventDto::ReleaseIntent {
            input_id: 2,
            operation_ordinal: 3,
            outputs_eligible: true,
        },
        5,
        1,
    );
    let input = ingress(&h, 2, &text.original_input);
    apply(&mut h, input, 5);
    for mutation in 0..4 {
        let mut dto = OperationDto::from_capture(&text, h.identity()).unwrap();
        match mutation {
            0 => dto.route.ordinal += 1,
            1 => dto.before.frame_seq += 1,
            2 => dto.input = InputDto::TimerPoll { now: 123 },
            _ => dto.drain_policy = DrainDto::Retain,
        }
        reject_unchanged(
            &mut h,
            OwnerEventDto::Executed {
                input_id: 2,
                operation: Box::new(dto),
            },
            6,
            1,
        );
    }
    let op = OwnerEventDto::executed(2, &text, &h.identity).unwrap();
    apply(&mut h, op, 6);
    reject_unchanged(
        &mut h,
        OwnerEventDto::HandoffObserved {
            input_id: 2,
            intent_ordinal: 4,
        },
        7,
        1,
    );
    reject_unchanged(
        &mut h,
        OwnerEventDto::ReleaseIntent {
            input_id: 2,
            operation_ordinal: 3,
            outputs_eligible: true,
        },
        7,
        1,
    );
    reject_unchanged(
        &mut h,
        OwnerEventDto::ReleaseIntent {
            input_id: 2,
            operation_ordinal: 7,
            outputs_eligible: true,
        },
        1000,
        1,
    );
    apply(
        &mut h,
        OwnerEventDto::ReleaseIntent {
            input_id: 2,
            operation_ordinal: 7,
            outputs_eligible: true,
        },
        7,
    );
    reject_unchanged(
        &mut h,
        OwnerEventDto::ReleaseIntent {
            input_id: 2,
            operation_ordinal: 7,
            outputs_eligible: true,
        },
        8,
        1,
    );
    reject_unchanged(
        &mut h,
        OwnerEventDto::HandoffObserved {
            input_id: 2,
            intent_ordinal: 4,
        },
        8,
        1,
    );
    reject_unchanged(
        &mut h,
        OwnerEventDto::HandoffObserved {
            input_id: 2,
            intent_ordinal: 8,
        },
        1000,
        1,
    );
    reject_unchanged(
        &mut h,
        OwnerEventDto::Fence {
            through_input_id: 1,
        },
        6,
        1,
    );
    reject_unchanged(
        &mut h,
        OwnerEventDto::Fence {
            through_input_id: 4,
        },
        8,
        1,
    );
}

#[test]
fn failed_native_turn_can_be_preserved_but_never_released() {
    let (first, text) = operations(true);
    let mut h = history();
    connected(&mut h, &first);
    let input = ingress(&h, 2, &text.original_input);
    apply(&mut h, input, 5);
    let op = OwnerEventDto::executed(2, &text, &h.identity).unwrap();
    apply(&mut h, op, 6);
    assert!(h.active.as_ref().unwrap().operation.outcome.is_err());
    reject_unchanged(
        &mut h,
        OwnerEventDto::ReleaseIntent {
            input_id: 2,
            operation_ordinal: 7,
            outputs_eligible: true,
        },
        7,
        1,
    );
    apply(&mut h, OwnerEventDto::Withheld { input_id: 2 }, 7);
    assert!(h.uncertainties().is_empty());
    let input = ingress(&h, 3, &text.original_input);
    reject_unchanged(&mut h, input, 8, 1);
}

#[test]
fn first_native_state_cannot_invent_prior_connection_or_readiness() {
    let (first, _) = operations(false);
    let mut h = history();
    start(&mut h);
    let input = ingress(&h, 1, &first.original_input);
    apply(&mut h, input, 1);
    for mutation in 0..3 {
        let mut op = OperationDto::from_capture(&first, h.identity()).unwrap();
        match mutation {
            0 => op.before.connection = 999,
            1 => op.before.lifecycle = LifecycleDto::Live,
            _ => op.before.frame_seq = 9,
        }
        reject_unchanged(
            &mut h,
            OwnerEventDto::Executed {
                input_id: 1,
                operation: Box::new(op),
            },
            2,
            1,
        );
    }
}

#[test]
fn discovered_reconnect_settles_queued_stale_input_without_inventing_callback() {
    struct Reconnect;
    impl SessionMachine for Reconnect {
        fn on_input(
            &mut self,
            input: SessionInput<'_>,
            out: &mut ActionBuffer,
        ) -> Result<(), AdapterError> {
            if matches!(input, SessionInput::TextFrame { .. }) {
                out.push(SessionAction::Reconnect(ReconnectReason::Protocol));
            }
            Ok(())
        }
    }
    let mut capture =
        CaptureSession::new(Box::new(Reconnect), config(OverflowPolicy::FailEngine)).unwrap();
    let first = capture
        .execute_turn(
            CaptureInput::Connected {
                connection: ConnectionId(7),
                now: TimestampNs(1),
            },
            CaptureDrainPolicy::NullDispatchTakeOutbound,
        )
        .unwrap();
    let input = || CaptureInput::Text {
        payload: b"reconnect".to_vec(),
        received: stamp(),
    };
    let second = capture
        .execute_turn(input(), CaptureDrainPolicy::NullDispatchTakeOutbound)
        .unwrap();
    let bypass = capture
        .execute_turn(input(), CaptureDrainPolicy::NullDispatchTakeOutbound)
        .unwrap();
    assert!(second.after.reconnect_requested);
    let mut h = history();
    connected(&mut h, &first);
    let queued = ingress(&h, 2, &second.original_input);
    apply(&mut h, queued, 5);
    let queued = ingress(&h, 3, &bypass.original_input);
    apply(&mut h, queued, 6);
    let op = OwnerEventDto::executed(2, &second, h.identity()).unwrap();
    apply(&mut h, op, 7);
    apply(&mut h, OwnerEventDto::Withheld { input_id: 2 }, 8);
    let op = OwnerEventDto::executed(3, &bypass, h.identity()).unwrap();
    reject_unchanged(&mut h, op, 9, 1);
    let input = ingress(&h, 4, &bypass.original_input);
    reject_unchanged(&mut h, input, 9, 1);
    apply(
        &mut h,
        OwnerEventDto::NotExecuted {
            input_id: 3,
            cause: OwnerNotExecutedCause::SourceConnection,
        },
        9,
    );
    assert_eq!(
        (h.ordinal(), h.native_ordinal(), h.pending_inputs()),
        (10, 2, 0)
    );
}

#[test]
fn late_native_commit_and_expired_release_remain_evidence_but_terminate_owner() {
    let (first, text) = operations(false);
    for late_commit in [false, true] {
        let mut h = history();
        connected(&mut h, &first);
        let input = ingress(&h, 2, &text.original_input);
        apply(&mut h, input, 5);
        let input = ingress(
            &h,
            3,
            &CaptureInput::TimerPoll {
                now: TimestampNs(9),
            },
        );
        apply(&mut h, input, 6);
        let op = OwnerEventDto::executed(2, &text, h.identity()).unwrap();
        apply(&mut h, op, if late_commit { 1000 } else { 7 });
        if !late_commit {
            apply(
                &mut h,
                OwnerEventDto::ReleaseIntent {
                    input_id: 2,
                    operation_ordinal: 8,
                    outputs_eligible: true,
                },
                8,
            );
        }
        apply(&mut h, OwnerEventDto::Withheld { input_id: 2 }, 1001);
        let mut input = ingress(&h, 4, &text.original_input);
        if let OwnerEventDto::Ingress { deadline_ns, .. } = &mut input {
            *deadline_ns = 2000;
        }
        reject_unchanged(&mut h, input, 1002, 1);
        apply(
            &mut h,
            OwnerEventDto::NotExecuted {
                input_id: 3,
                cause: OwnerNotExecutedCause::Terminal,
            },
            1002,
        );
        assert!(h.uncertainties().is_empty());
    }
}

#[test]
fn count_bytes_and_whole_record_limits_are_checked_without_prefix_mutation() {
    let (first, _) = operations(false);
    let mut h = history();
    start(&mut h);
    h.policy.max_pending_operations = 1;
    let input = ingress(&h, 1, &first.original_input);
    apply(&mut h, input, 1);
    let input = ingress(
        &h,
        2,
        &CaptureInput::TimerPoll {
            now: TimestampNs(2),
        },
    );
    reject_unchanged(&mut h, input, 2, 0);
    let mut h = history();
    start(&mut h);
    h.policy.max_input_bytes = 1;
    let input = ingress(&h, 1, &first.original_input);
    reject_unchanged(&mut h, input, 1, 0);
    let mut h = history();
    start(&mut h);
    h.policy.max_pending_bytes = 1;
    let input = ingress(&h, 1, &first.original_input);
    reject_unchanged(&mut h, input, 1, 0);
    let mut h = history();
    start(&mut h);
    h.policy.max_operation_bytes = 1;
    let input = ingress(&h, 1, &first.original_input);
    reject_unchanged(&mut h, input, 1, 0);
}

#[test]
fn codec_rejects_cross_profile_route_phase_and_noncanonical_bytes() {
    let mut h = history();
    start(&mut h);
    let wire = encoded(
        &h,
        OwnerEventDto::Fence {
            through_input_id: 0,
        },
        1,
        0,
    );
    for mutation in 0..5 {
        let mut changed = wire.clone();
        match mutation {
            0 => changed.schema_version = 1,
            1 => changed.route.session += 1,
            2 => changed.phase = CausalPhase::Operation,
            3 => {
                changed.payload_base64 =
                    STANDARD.encode([wire.payload().unwrap(), b" ".to_vec()].concat())
            }
            _ => changed.route.ordinal += 1,
        }
        assert!(decode_owner_history(&changed, h.identity()).is_err());
    }
    assert!(decode_operation(&wire, h.identity()).is_err());
    let dto = decode_owner_history(&wire, h.identity()).unwrap();
    assert!(matches!(
        encode_owner_history(&dto, h.identity(), 1),
        Err(CodecError::TooLarge)
    ));
}
