//! A bounded continuation for the Completion's own ACK and one preissued
//! Intent. Neither a prepared record nor a cached receipt grants authority.
use super::*;

#[cfg(all(test, feature = "causal-journal"))]
#[derive(Default)]
pub(super) struct PipelineHooks {
    pub before_intent: Mutex<Option<Arc<TextPauseState>>>,
    pub after_completion: Mutex<Option<Arc<TextPauseState>>>,
    pub panic_after_completion: std::sync::atomic::AtomicBool,
}

pub(super) struct CompletionSettlement {
    pub ordinal: u64,
    pub id: u64,
    pub bytes: usize,
    pub sender: Option<mpsc::Sender<InputSettlement>>,
    pub receipt: Option<CommitReceipt>,
}
pub(super) struct PreissuedIntent {
    pub ordinal: u64,
    pub completion: u64,
    pub tx: Option<PreparedFeedTransaction>,
    pub result: Option<Option<CommitReceipt>>,
}

// Count distinct logical one-record continuations, including preparation and
// cached own ACKs. An admitted ticket and its continuation name the same record;
// popping a ticket never frees the cached continuation's reservation.
pub(super) fn descriptor_count(registry: &ReceiptRegistry) -> usize {
    let mut count = registry.pending.len();
    for ordinal in [
        registry.completion.as_ref().map(|c| c.ordinal),
        registry.intent.as_ref().map(|i| i.ordinal),
    ]
    .into_iter()
    .flatten()
    {
        if !registry.pending.iter().any(|p| p.ordinal == ordinal) {
            count += 1;
        }
    }
    count
}

pub(super) fn has_descriptor(registry: &ReceiptRegistry, ordinal: u64) -> bool {
    registry.pending.iter().any(|p| p.ordinal == ordinal)
        || registry
            .completion
            .as_ref()
            .is_some_and(|c| c.ordinal == ordinal)
        || registry
            .intent
            .as_ref()
            .is_some_and(|i| i.ordinal == ordinal)
}

pub(super) fn register_completion(shared: &HistoryShared, ordinal: u64, input: &QueuedInput) {
    let mut registry = tickets(shared);
    assert!(registry.completion.is_none(), "one native continuation");
    registry.completion = Some(CompletionSettlement {
        ordinal,
        id: input.id,
        bytes: input.bytes,
        sender: Some(input.settlement.clone()),
        receipt: None,
    });
}

// Called under Registry -> Data, only after validating the complete earlier
// FIFO prefix. Compute this with the other checked counters before mutation.
pub(super) fn completion_credit(
    registry: &ReceiptRegistry,
    s: &Data,
    ordinal: u64,
) -> Option<(usize, usize, u64)> {
    match registry
        .completion
        .as_ref()
        .filter(|c| c.ordinal == ordinal)
    {
        Some(c) => s
            .status
            .pending_operations
            .checked_sub(1)
            .zip(s.status.pending_bytes.checked_sub(c.bytes))
            .zip(s.status.durable.checked_add(1))
            .map(|((count, bytes), durable)| (count, bytes, durable)),
        None => Some((
            s.status.pending_operations,
            s.status.pending_bytes,
            s.status.durable,
        )),
    }
}

pub(super) fn completion_acked(
    registry: &mut ReceiptRegistry,
    ordinal: u64,
    receipt: &CommitReceipt,
) {
    if let Some(c) = registry
        .completion
        .as_mut()
        .filter(|c| c.ordinal == ordinal)
    {
        c.receipt = Some(receipt.clone());
        if let Some(sender) = c.sender.take() {
            let _ = sender.send(InputSettlement::Committed {
                id: c.id,
                receipt: receipt.clone(),
            });
        }
    }
}

pub(super) fn finish_completion(shared: &HistoryShared) {
    let mut registry = tickets(shared);
    if let Some(mut c) = registry.completion.take() {
        if let Some(sender) = c.sender.take() {
            let _ = sender.send(InputSettlement::Unknown { id: c.id });
        }
    }
    registry.intent = None;
}

pub(super) fn reserve_intent(shared: &HistoryShared, dto: &OwnerHistoryDto, physical_active: bool) {
    let OwnerEventDto::ExecutionCompleted {
        input_id,
        operation,
        ..
    } = &dto.event
    else {
        return;
    };
    // Safety/error/reconnect turns retain the original serial path. The
    // optimization never changes which output is eligible for release.
    if !physical_active
        || operation.after.reconnect_requested
        || operation.after.stop_requested
        || operation.outcome.is_err()
    {
        return;
    }
    let mut registry = tickets(shared);
    if registry.pending.len() > 1 || registry.intent.is_some() {
        return;
    }
    let mut s = state(shared);
    refresh(shared, &mut s);
    if s.status.failure.is_some()
        || !s.observations.is_empty()
        || !s.inputs.is_empty()
        || s.active.is_some()
        || s.ready.is_some()
        || s.disposition_queued
        || *input_id < s.visibility_fence
    {
        return;
    }
    if let Some(wire) = s.wire.as_ref() {
        if wire_candidates(
            operation,
            *input_id,
            dto.route.ordinal,
            dto.route.ordinal,
            wire.ledger.policy(),
        )
        .is_err()
        {
            return;
        }
    }
    let (Some(ordinal), Some(observed_ns)) = (s.next_ordinal.checked_add(1), now_ns(shared)) else {
        return;
    };
    let event = OwnerEventDto::ReleaseIntent {
        input_id: *input_id,
        operation_ordinal: dto.route.ordinal,
        outputs_eligible: true,
    };
    let intent = OwnerHistoryDto {
        schema_version: shared.schema_version,
        route: CausalRoute {
            ordinal,
            ..dto.route.clone()
        },
        observed_ns,
        event,
    };
    let Ok(envelope) = encode_owner_history(
        &intent,
        &shared.identity,
        shared.history.max_record_bytes as usize,
    ) else {
        return;
    };
    let Ok(tx) = PreparedFeedTransaction::new(vec![FeedRecord::Causal(envelope)]) else {
        return;
    };
    // Advance only after successful encoding; subsequent concurrent ingress or
    // revocation observations receive ordinals after this exact Intent.
    s.next_ordinal = ordinal;
    s.observations.push_back(Observation {
        ordinal,
        observed_ns,
        ingress_bytes: 0,
        event: Event::Control(intent.event),
    });
    registry.intent = Some(PreissuedIntent {
        ordinal,
        completion: dto.route.ordinal,
        tx: Some(tx),
        result: None,
    });
}

pub(super) fn is_preissued(shared: &HistoryShared, ordinal: u64) -> bool {
    tickets(shared)
        .intent
        .as_ref()
        .is_some_and(|i| i.ordinal == ordinal)
}

pub(super) fn commit_completion(
    shared: &HistoryShared,
    tx: PreparedFeedTransaction,
    ordinal: u64,
) -> Option<Option<CommitReceipt>> {
    if !submit_record(shared, tx, ordinal, 0, true) {
        return None;
    }
    let prepared = {
        let mut registry = tickets(shared);
        registry.intent.as_mut().and_then(|i| {
            (i.completion == ordinal)
                .then(|| i.tx.take().map(|tx| (i.ordinal, tx)))
                .flatten()
        })
    };
    if let Some((intent_ordinal, tx)) = prepared {
        #[cfg(all(test, feature = "causal-journal"))]
        pause_owner_worker(&shared.pipeline_hooks.before_intent);
        // Full may settle Completion here, but never frees its continuation or
        // prepares a different Intent. The exact transaction is retried.
        if !submit_record(shared, tx, intent_ordinal, 0, false) {
            tickets(shared).intent.as_mut().unwrap().result = Some(None);
        }
    }
    #[cfg(all(test, feature = "causal-journal"))]
    if tickets(shared).pending.len() >= 2
        && shared
            .panic_after_prefix_admission
            .swap(false, std::sync::atomic::Ordering::Relaxed)
    {
        panic!("test-only outer owner panic with retained Completion/Intent");
    }
    settle_outstanding(shared);
    // A failed later suffix cannot retroactively erase this validated own ACK.
    tickets(shared)
        .completion
        .as_ref()
        .and_then(|c| c.receipt.clone())
        .map(Some)
}

pub(super) fn consume_intent(
    shared: &HistoryShared,
    ordinal: u64,
) -> Option<Option<CommitReceipt>> {
    let mut registry = tickets(shared);
    let intent = registry.intent.take().expect("preissued observation");
    assert_eq!(intent.ordinal, ordinal);
    // commit_completion already joined every accepted ticket. A reserved but
    // unaccepted suffix ends this prefix, rather than leaving an ordinal hole.
    intent.result.flatten().map(Some)
}

pub(super) fn consume_completion(shared: &HistoryShared, input_id: u64, ordinal: u64) -> bool {
    let mut registry = tickets(shared);
    let c = registry.completion.take().expect("Completion continuation");
    assert_eq!(c.id, input_id);
    assert_eq!(c.ordinal, ordinal);
    assert!(c.receipt.is_some(), "own ACK before native retirement");
    registry
        .intent
        .as_ref()
        .is_some_and(|i| i.completion == ordinal)
}
