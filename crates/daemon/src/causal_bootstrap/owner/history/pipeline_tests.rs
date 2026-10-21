//! Completion settlement and bounded Intent admission regressions.
use super::*;

fn arm(slot: &Mutex<Option<Arc<TextPauseState>>>) -> HistoryTextPause {
    let pause = Arc::new(TextPauseState::default());
    assert!(slot.lock().unwrap().replace(pause.clone()).is_none());
    HistoryTextPause(pause)
}

fn assert_owned_ordinals(registry: &ReceiptRegistry) {
    let mut ordinals = registry
        .pending
        .iter()
        .map(|p| p.ordinal)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ordinals.len(), registry.pending.len());
    if let Some(c) = registry.completion.as_ref() {
        ordinals.insert(c.ordinal);
    }
    if let Some(i) = registry.intent.as_ref() {
        ordinals.insert(i.ordinal);
        let c = registry.completion.as_ref().unwrap();
        assert_eq!(i.completion, c.ordinal);
        assert_eq!(i.ordinal, c.ordinal + 1);
        if i.tx.is_some() || i.result.is_some() {
            assert!(!registry.pending.iter().any(|p| p.ordinal == i.ordinal));
        }
    }
    assert_eq!(ordinals.len(), descriptor_count(registry));
    assert!(ordinals.len() <= MAX_DEFERRED_PREFIX + 1);
}

fn full_actor(path: &std::path::Path) -> (FeedBatchActor, PreparedBootstrap) {
    let mut dto = fixture();
    dto.capture.session += 1;
    dto.public_inputs.push(PublicReproductionInputDto::new(
        "bounded-filler",
        &vec![0u8; 256 << 10],
    ));
    let filler = PreparedBootstrap::new_history(dto, MAX_OPERATION_BYTES).unwrap();
    let cap = filler.encoded_bytes_bound() + 512;
    let actor = FeedBatchActor::spawn(
        FeedWriter::create_profile(path, 64 << 20, FeedProfile::EngineCausalV3).unwrap(),
        BatchConfig {
            max_pending_operations: 32,
            max_pending_bytes: cap,
            max_group_operations: 32,
            max_group_bytes: cap,
            max_age: Duration::from_millis(300),
        },
    )
    .unwrap();
    (actor, filler)
}

#[test]
fn complete_and_intent_are_both_owned_before_first_receipt_observation() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(100));
    let (owner, entered, resume, calls) = blocked(&actor, fixture(), false);
    let admission = owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let receipt = owner.ingress().pause_next_receipt_for_test();
    resume.send(()).unwrap();
    wait(|| receipt.entered());
    let ordinals = tickets(&owner.ingress.shared)
        .pending
        .iter()
        .map(|p| p.ordinal)
        .collect::<Vec<_>>();
    {
        let registry = tickets(&owner.ingress.shared);
        assert_owned_ordinals(&registry);
        let intent = registry.intent.as_ref().unwrap();
        assert!(intent.tx.is_none() && intent.result.is_none());
        assert!(registry.pending.iter().any(|p| p.ordinal == intent.ordinal));
    }
    let before = owner.status();
    let ready_before_ack = owner.try_next().is_some();
    receipt.release();
    let release = release(&owner);
    let settlement = admission.wait();
    let status = finish(owner);
    actor.shutdown().unwrap();
    let (history, events, handoffs) = inspect(&path);
    // Cleanup precedes the old-runtime failing assertion; no paused thread or
    // unjoined actor can turn a semantic RED into a cleanup hang.
    assert_eq!(
        ordinals.len(),
        2,
        "Completion and Intent must be admitted before observing either own ACK"
    );
    assert_eq!(ordinals[0] + 1, ordinals[1]);
    assert_eq!(before.owner.durable, 1);
    assert!(!ready_before_ack);
    assert_eq!(release.input_id(), 2);
    assert!(matches!(
        settlement,
        InputSettlement::Committed { id: 2, .. }
    ));
    assert!(status.owner.failure.is_none());
    assert_eq!(status.owner.durable, 2);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(history.native_ordinal(), 2);
    assert!(history.uncertainties().is_empty());
    assert_eq!(handoffs.len(), 2);
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(
                e.event,
                OwnerEventDto::ExecutionCompleted { input_id: 2, .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.event, OwnerEventDto::ReleaseIntent { input_id: 2, .. }))
            .count(),
        1
    );
}

#[test]
fn intent_full_retires_completion_once_and_retries_exact_reserved_record() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let (actor, filler) = full_actor(&path);
    let (owner, entered, resume, calls) = blocked(&actor, fixture(), false);
    let admission = owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let before_intent = arm(&owner.ingress.shared.pipeline_hooks.before_intent);
    let after_completion = arm(&owner.ingress.shared.pipeline_hooks.after_completion);
    let before = actor.handle().status().committed_operations;
    resume.send(()).unwrap();
    wait(|| before_intent.entered());
    wait(|| actor.handle().status().committed_operations == before + 1);
    let filler = filler.try_admit(&actor.handle()).unwrap();
    before_intent.release();
    wait(|| after_completion.entered());
    let during = owner.status();
    let registry = tickets(&owner.ingress.shared);
    assert_owned_ordinals(&registry);
    assert!(registry.completion.as_ref().unwrap().receipt.is_some());
    assert!(registry.intent.as_ref().unwrap().result.is_none());
    assert!(registry.pending.is_empty()); // Full drained the owned predecessor
    assert_eq!(descriptor_count(&registry), 2); // cached Completion + exact local Intent retry
    drop(registry);
    assert!(during.owner.admission_retries > 0);
    assert_eq!(during.owner.durable, 2);
    assert_eq!(during.owner.pending_operations, 0);
    assert_eq!(during.owner.pending_bytes, 0);
    assert_eq!(during.owner.phase, OwnerPhase::AwaitingCommit);
    assert!(owner.try_next().is_none());
    let committed = admission.wait();
    assert!(matches!(
        committed,
        InputSettlement::Committed { id: 2, .. }
    ));
    after_completion.release();
    filler.wait().unwrap();
    assert_eq!(release(&owner).input_id(), 2);
    let status = finish(owner);
    actor.shutdown().unwrap();
    assert!(status.owner.failure.is_none());
    assert_eq!(status.owner.durable, 2);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (_, events, _) = inspect(&path);
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.event, OwnerEventDto::ReleaseIntent { input_id: 2, .. }))
            .count(),
        1
    );
}

#[test]
fn failed_intent_suffix_never_rewrites_committed_completion_or_publishes_reserved_hole() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(100));
    let (owner, entered, resume, calls) = blocked(&actor, fixture(), false);
    let admission = owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let pause = arm(&owner.ingress.shared.pipeline_hooks.before_intent);
    let before = actor.handle().status().committed_operations;
    resume.send(()).unwrap();
    wait(|| pause.entered());
    wait(|| actor.handle().status().committed_operations == before + 1);
    // Completion has crossed its real barrier. Force the later publication's
    // actual staging-path failure; no synthetic successful receipt is injected.
    std::fs::write(path.join("committed.tmp"), b"occupied staging path").unwrap();
    pause.release();
    assert!(matches!(
        admission.wait(),
        InputSettlement::Committed { id: 2, .. }
    ));
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    assert!(owner.try_next().is_none());
    let status = owner.shutdown();
    assert!(actor.shutdown().is_err());
    assert_eq!(status.owner.durable, 2);
    assert_eq!(status.owner.pending_operations, 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (history, events, handoffs) = inspect(&path);
    assert_eq!(history.native_ordinal(), 2);
    assert_eq!(handoffs.len(), 1);
    assert!(matches!(
        events.last().unwrap().event,
        OwnerEventDto::ExecutionCompleted { input_id: 2, .. }
    ));
    assert!(!events.iter().any(|e| matches!(
        e.event,
        OwnerEventDto::ReleaseIntent { input_id: 2, .. } | OwnerEventDto::Revoked { .. }
    )));
}

#[test]
fn revoke_after_completion_ack_keeps_settlement_and_withholds_preissued_intent() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(100));
    let (owner, entered, resume, calls) = blocked(&actor, fixture(), false);
    let admission = owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let pause = arm(&owner.ingress.shared.pipeline_hooks.after_completion);
    resume.send(()).unwrap();
    wait(|| pause.entered());
    assert!(matches!(
        admission.wait(),
        InputSettlement::Committed { id: 2, .. }
    ));
    owner.ingress().revoke();
    assert!(owner.try_next().is_none());
    assert!(owner.committed_snapshot().is_none());
    pause.release();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    let status = owner.shutdown();
    actor.shutdown().unwrap();
    assert_eq!(status.owner.failure, Some(OwnerFailure::Revoked));
    assert_eq!(status.owner.durable, 2);
    assert_eq!(status.owner.withheld, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (history, events, handoffs) = inspect(&path);
    assert_eq!(handoffs.len(), 1);
    assert!(history.uncertainties().is_empty());
    let intent = events
        .iter()
        .position(|e| matches!(e.event, OwnerEventDto::ReleaseIntent { input_id: 2, .. }))
        .unwrap();
    let revoked = events
        .iter()
        .position(|e| matches!(e.event, OwnerEventDto::Revoked { .. }))
        .unwrap();
    assert!(intent < revoked);
}

#[test]
fn panic_after_pair_admission_drains_tickets_and_commits_native_once_without_release() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(100));
    let (owner, entered, resume, calls) = blocked(&actor, fixture(), false);
    let admission = owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    owner
        .ingress
        .shared
        .panic_after_prefix_admission
        .store(true, Ordering::SeqCst);
    resume.send(()).unwrap();
    assert!(matches!(
        admission.wait(),
        InputSettlement::Committed { id: 2, .. }
    ));
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    assert!(tickets(&owner.ingress.shared).pending.is_empty());
    assert!(owner.try_next().is_none());
    let status = owner.shutdown();
    actor.shutdown().unwrap();
    assert_eq!(status.owner.failure, Some(OwnerFailure::WorkerPanicked));
    assert_eq!(status.owner.durable, 2);
    assert_eq!(status.owner.pending_operations, 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (history, _, handoffs) = inspect(&path);
    assert_eq!(history.native_ordinal(), 2);
    assert_eq!(handoffs.len(), 1);
    assert!(!history.uncertainties().is_empty());
}

#[test]
fn actual_factory_connected_generation_and_every_record_resume_reexecute_identically() {
    use super::super::super::super::replay::HistoryReplay;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(100));
    let owner = boot(fixture(), &actor).spawn_history_owner().unwrap();
    let mut actual = Vec::new();
    for (id, input) in [
        (1, connected(1)),
        (1, CaptureInput::Text {payload:br#"{"topic":"publicTrade.C01USDT","type":"snapshot","ts":1000,"data":[{"T":1000,"s":"C01USDT","S":"Buy","v":"0.001","p":"1.25","i":"pipeline-trade1","BT":false}]}"#.to_vec(),received:FrameStamp{receive_ts:TimestampNs(1_000_000_001),mono_ns:1}}),
        (2, connected(2)),
        (2, CaptureInput::Text {payload:br#"{"topic":"publicTrade.C01USDT","type":"snapshot","ts":1001,"data":[{"T":1001,"s":"C01USDT","S":"Sell","v":"0.002","p":"1.26","i":"pipeline-trade2","BT":false}]}"#.to_vec(),received:FrameStamp{receive_ts:TimestampNs(1_001_000_001),mono_ns:2}}),
    ] {
        let admission = owner
            .ingress()
            .try_submit(ConnectionId(id), input)
            .unwrap();
        let release = release(&owner);
        if actual.len() % 2 == 1 {
            assert!(release.released_items().any(|item| matches!(item, ItemDto::Market(_))));
        }
        actual.push(release.evidence().clone());
        assert!(matches!(
            admission.wait(),
            InputSettlement::Committed { .. }
        ));
        wait(|| owner.status().owner.phase == OwnerPhase::Idle);
    }
    assert!(finish(owner).owner.failure.is_none());
    let report = actor.shutdown().unwrap();
    let limits = || OwnerHistoryLimits {
        max_retained_input_bytes: 4 << 20,
        max_input_bytes: 256 << 10,
        max_record_bytes: 256 << 10,
    };
    let mut replay = HistoryReplay::open(&path, limits()).unwrap();
    let mut boundaries = vec![replay.progress().unwrap().cursor];
    let mut seen = Vec::new();
    while let Some(step) = replay.read_next().unwrap() {
        boundaries.push(step.cursor);
        if let Some(handoff) = step.handoff {
            seen.push(*handoff.operation);
        }
    }
    assert_eq!(seen, actual);
    assert_eq!(actual[0].route.generation, 1);
    assert_eq!(actual[1].route.generation, 1);
    assert_eq!(actual[2].route.generation, 2);
    assert_eq!(actual[3].route.generation, 2);
    for cursor in boundaries {
        let mut replay = HistoryReplay::resume(&path, cursor, limits()).unwrap();
        while replay.read_next().unwrap().is_some() {}
        assert_eq!(replay.progress().unwrap().native_ordinal, 4);
        assert_eq!(replay.progress().unwrap().cursor, report.published);
    }
}

#[test]
fn shared_publication_failure_keeps_both_pair_records_unknown_and_drains_tickets() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(100));
    let (owner, entered, resume, calls) = blocked(&actor, fixture(), false);
    let admission = owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let pause = owner.ingress().pause_next_receipt_for_test();
    std::fs::write(path.join("committed.tmp"), b"occupied staging path").unwrap();
    resume.send(()).unwrap();
    wait(|| pause.entered());
    let pending = tickets(&owner.ingress.shared).pending.len();
    wait(|| actor.handle().status().failure.is_some());
    let attempted = {
        let mut registry = tickets(&owner.ingress.shared);
        registry
            .pending
            .iter_mut()
            .map(|pending| {
                pending
                    .ticket
                    .wait_result(WAIT)
                    .expect("actor settled failed group")
                    .unwrap_err()
            })
            .collect::<Vec<_>>()
    };
    pause.release();
    let settlement = admission.wait();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    assert!(tickets(&owner.ingress.shared).pending.is_empty());
    let status = owner.shutdown();
    assert!(actor.shutdown().is_err());
    assert_eq!(pending, 2);
    assert!(
        attempted.iter().all(|d| matches!(
            d.disposition,
            marketfeed_recording::shared_batch::CommitDisposition::Attempted { .. }
        )),
        "both records must be in the same failed active publication, not its unattempted suffix: {attempted:?}"
    );
    assert_eq!(attempted[0].failure, attempted[1].failure);
    match (&attempted[0].disposition, &attempted[1].disposition) {
        (
            marketfeed_recording::shared_batch::CommitDisposition::Attempted {
                first_sequence: a,
                last_sequence: b,
            },
            marketfeed_recording::shared_batch::CommitDisposition::Attempted {
                first_sequence: c,
                last_sequence: d,
            },
        ) => {
            assert_eq!(a, b);
            assert_eq!(c, d);
            assert_eq!(b.checked_add(1), Some(*c));
        }
        _ => unreachable!("both active attempts asserted above"),
    }
    assert!(matches!(settlement, InputSettlement::Unknown { id: 2 }));
    assert_eq!(status.owner.durable, 1);
    assert_eq!(status.owner.pending_operations, 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (history, events, handoffs) = inspect(&path);
    assert_eq!(history.native_ordinal(), 1);
    assert_eq!(handoffs.len(), 1);
    assert!(matches!(
        events.last().unwrap().event,
        OwnerEventDto::ExecutionPermitted { input_id: 2, .. }
    ));
}

#[test]
fn panic_after_completion_ack_preserves_known_commit_and_joins_remaining_intent() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(100));
    let (owner, entered, resume, calls) = blocked(&actor, fixture(), false);
    let admission = owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    owner
        .ingress
        .shared
        .pipeline_hooks
        .panic_after_completion
        .store(true, Ordering::SeqCst);
    resume.send(()).unwrap();
    let settlement = admission.wait();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    assert!(tickets(&owner.ingress.shared).pending.is_empty());
    let status = owner.shutdown();
    actor.shutdown().unwrap();
    assert!(matches!(
        settlement,
        InputSettlement::Committed { id: 2, .. }
    ));
    assert_eq!(status.owner.failure, Some(OwnerFailure::WorkerPanicked));
    assert_eq!(status.owner.durable, 2);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (history, events, handoffs) = inspect(&path);
    assert_eq!(history.native_ordinal(), 2);
    assert_eq!(handoffs.len(), 1);
    assert!(matches!(
        events.last().unwrap().event,
        OwnerEventDto::ReleaseIntent { input_id: 2, .. }
    ));
}

#[test]
fn invalid_prior_fifo_state_prevents_literal_successful_completion_ack_from_committing() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(100));
    let (owner, entered, resume, calls) = blocked(&actor, fixture(), false);
    let admission = owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let pause = owner.ingress().pause_next_receipt_for_test();
    resume.send(()).unwrap();
    wait(|| pause.entered());
    // Validator challenge, not a disk-corruption claim: real actor tickets
    // return Ok, but a broken remembered prefix must poison the entire suffix.
    tickets(&owner.ingress.shared).previous_ordinal -= 1;
    pause.release();
    let settlement = admission.wait();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    let status = owner.shutdown();
    actor.shutdown().unwrap();
    assert!(matches!(settlement, InputSettlement::Unknown { id: 2 }));
    assert_eq!(status.owner.failure, Some(OwnerFailure::InvalidReceipt));
    assert_eq!(status.owner.durable, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // Physical bytes remain a valid literal-success prefix; runtime validation
    // conservatively denied it and never exposed a handoff.
    let (history, events, handoffs) = inspect(&path);
    assert_eq!(history.native_ordinal(), 2);
    assert_eq!(handoffs.len(), 1);
    assert!(matches!(
        events.last().unwrap().event,
        OwnerEventDto::ReleaseIntent { input_id: 2, .. }
    ));
}

#[test]
fn actual_five_second_deadline_after_reservation_never_releases_expired_intent() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(100));
    let (owner, entered, resume, calls) = blocked(&actor, fixture(), false);
    let admission = owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let pause = arm(&owner.ingress.shared.pipeline_hooks.after_completion);
    resume.send(()).unwrap();
    wait(|| pause.entered());
    let deadline = state(&owner.ingress.shared).current_deadline.unwrap();
    // The actual unchanged five-second ingress deadline, no artificial clock
    // subtraction or shortened policy. The fixture's ten-second watchdog stays.
    wait(|| Instant::now() >= deadline);
    let expired = owner.status();
    assert!(owner.try_next().is_none());
    pause.release();
    let settlement = admission.wait();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    let status = owner.shutdown();
    actor.shutdown().unwrap();
    assert!(matches!(
        settlement,
        InputSettlement::Committed { id: 2, .. }
    ));
    assert!(matches!(
        expired.owner.failure,
        Some(OwnerFailure::Deadline(_))
    ));
    assert_eq!(status.owner.durable, 2);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (history, _, handoffs) = inspect(&path);
    assert_eq!(handoffs.len(), 1);
    assert!(history.uncertainties().is_empty());
}

#[test]
fn unaccepted_intent_after_writer_closure_ends_at_known_completion_without_ordinal_hole() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(100));
    let (owner, entered, resume, calls) = blocked(&actor, fixture(), false);
    let admission = owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let pause = arm(&owner.ingress.shared.pipeline_hooks.before_intent);
    let before = actor.handle().status().committed_operations;
    resume.send(()).unwrap();
    wait(|| pause.entered());
    wait(|| actor.handle().status().committed_operations == before + 1);
    actor.handle().close();
    pause.release();
    let settlement = admission.wait();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    assert!(tickets(&owner.ingress.shared).pending.is_empty());
    let status = owner.shutdown();
    actor.shutdown().unwrap();
    assert!(matches!(
        settlement,
        InputSettlement::Committed { id: 2, .. }
    ));
    assert_eq!(status.owner.durable, 2);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (history, events, handoffs) = inspect(&path);
    assert_eq!(history.native_ordinal(), 2);
    assert_eq!(handoffs.len(), 1);
    assert!(matches!(
        events.last().unwrap().event,
        OwnerEventDto::ExecutionCompleted { input_id: 2, .. }
    ));
    assert!(!events.iter().any(|e| matches!(
        e.event,
        OwnerEventDto::ReleaseIntent { input_id: 2, .. } | OwnerEventDto::Revoked { .. }
    )));
}

#[test]
fn earlier_queued_ingress_and_reconnection_fence_force_serial_fallback_without_holes() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(100));
    let (owner, entered, resume, calls) = blocked(&actor, fixture(), false);
    let current = owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let newer = owner
        .ingress()
        .try_submit(ConnectionId(2), connected(2))
        .unwrap();
    let pause = owner.ingress().pause_next_receipt_for_test();
    resume.send(()).unwrap();
    wait(|| pause.entered());
    // Earlier ingress observation is processed first; its pending prefix and
    // queued input preclude speculative reservation around that observation.
    assert!(tickets(&owner.ingress.shared).intent.is_none());
    pause.release();
    let release = release(&owner);
    assert_eq!(release.input_id(), 3);
    assert!(matches!(
        current.wait(),
        InputSettlement::Committed { id: 2, .. }
    ));
    assert!(matches!(
        newer.wait(),
        InputSettlement::Committed { id: 3, .. }
    ));
    let status = finish(owner);
    actor.shutdown().unwrap();
    assert!(status.owner.failure.is_none());
    assert_eq!(status.owner.durable, 3);
    assert_eq!(status.owner.withheld, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (history, events, handoffs) = inspect(&path);
    assert_eq!(history.native_ordinal(), 3);
    assert!(history.uncertainties().is_empty());
    assert_eq!(handoffs.len(), 2);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e.event, OwnerEventDto::ReleaseIntent { input_id: 2, .. }))
    );
    for pair in events.windows(2) {
        assert_eq!(pair[0].route.ordinal + 1, pair[1].route.ordinal);
    }
}
