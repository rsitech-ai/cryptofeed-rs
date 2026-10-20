use super::*;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, SessionAction, SessionInput, SessionMachine,
};
use marketfeed_model::{FrameStamp, TimestampNs};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

const WAIT: Duration = Duration::from_secs(10);
fn fixture() -> BootstrapDto {
    let mut dto = super::super::super::tests::fixture();
    let policy = dto.owner_policy.as_mut().unwrap();
    policy.max_input_bytes = 8192;
    policy.max_operation_bytes = 256 << 10;
    policy.max_ingress_age = Duration::from_secs(5).into();
    dto.bind_history_policy(HistoryRuntimePolicyDto {
        schema_version: 1,
        max_pending_observations: 32,
        max_pending_observation_bytes: 4 << 20,
        max_record_bytes: 256 << 10,
        max_finalization_age: Duration::from_secs(2).into(),
    })
    .unwrap();
    dto
}
fn actor(path: &std::path::Path, age: Duration) -> FeedBatchActor {
    FeedBatchActor::spawn(
        FeedWriter::create_profile(path, 64 << 20, FeedProfile::EngineCausalV3).unwrap(),
        BatchConfig {
            max_pending_operations: 32,
            max_pending_bytes: 8 << 20,
            max_group_operations: 32,
            max_group_bytes: 8 << 20,
            max_age: age,
        },
    )
    .unwrap()
}
fn boot(dto: BootstrapDto, actor: &FeedBatchActor) -> BootstrappedSession {
    PreparedBootstrap::new_history(dto, MAX_OPERATION_BYTES)
        .unwrap()
        .try_admit(&actor.handle())
        .unwrap()
        .wait()
        .unwrap()
        .instantiate()
        .unwrap()
}
fn wait(mut check: impl FnMut() -> bool) {
    let until = Instant::now() + WAIT;
    while !check() {
        assert!(Instant::now() < until, "history owner test watchdog");
        thread::sleep(Duration::from_millis(1));
    }
}
fn connected(id: u64) -> CaptureInput {
    CaptureInput::Connected {
        connection: ConnectionId(id),
        now: TimestampNs(1),
    }
}
fn frame() -> CaptureInput {
    CaptureInput::Text {
        payload: b"original-frame".to_vec(),
        received: FrameStamp {
            receive_ts: TimestampNs(2),
            mono_ns: 3,
        },
    }
}
fn release(owner: &HistoryJournalOwner) -> CommittedRelease {
    let mut result = None;
    wait(|| {
        result = owner.try_next();
        result.is_some()
    });
    result.unwrap()
}
fn finish(owner: HistoryJournalOwner) -> HistoryStatus {
    owner.close_ingress();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    owner.shutdown()
}
fn inspect(
    path: &std::path::Path,
) -> (
    OwnerHistory,
    Vec<OwnerHistoryDto>,
    Vec<RecordedOwnerHandoff>,
) {
    let mut reader = FeedReader::open_profile(path, None, FeedProfile::EngineCausalV3).unwrap();
    let Some(FeedRecord::Causal(bootstrap)) = reader.read_next().unwrap() else {
        panic!()
    };
    let mut history = OwnerHistory::from_bootstrap(
        &bootstrap,
        OwnerHistoryLimits {
            max_retained_input_bytes: 4 << 20,
            max_input_bytes: 256 << 10,
            max_record_bytes: 256 << 10,
        },
    )
    .unwrap();
    let mut events = Vec::new();
    let mut handoffs = Vec::new();
    while let Some(record) = reader.read_next().unwrap() {
        let FeedRecord::Causal(record) = record else {
            panic!()
        };
        if record.route.session != history.identity().session {
            continue;
        }
        events.push(decode_owner_history(&record, history.identity()).unwrap());
        if let Some(handoff) = history.apply(&record).unwrap() {
            handoffs.push(handoff);
        }
    }
    (history, events, handoffs)
}
#[derive(Clone, Copy)]
enum Mode {
    Normal,
    Fail,
    Panic,
    Large,
    Reconnect,
}
struct Blocked {
    entered: mpsc::SyncSender<()>,
    resume: mpsc::Receiver<()>,
    calls: Arc<AtomicUsize>,
    mode: Mode,
}
impl SessionMachine for Blocked {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Connected { .. } => {
                output.push(SessionAction::MarkLive);
                output.push(SessionAction::SendText(bytes::Bytes::from_static(
                    b"subscribe",
                )));
            }
            SessionInput::TextFrame { bytes, .. } => {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.entered.send(()).unwrap();
                self.resume.recv().unwrap();
                bytes.fill(0);
                if matches!(self.mode, Mode::Panic) {
                    panic!("controlled native panic");
                }
                let reply = if matches!(self.mode, Mode::Large) {
                    bytes::Bytes::from(vec![b'x'; 128 << 10])
                } else {
                    bytes::Bytes::from_static(b"reply")
                };
                output.push(SessionAction::SendText(reply));
                if matches!(self.mode, Mode::Reconnect) {
                    output.push(SessionAction::Reconnect(
                        marketfeed_adapter_api::ReconnectReason::Protocol,
                    ));
                }
                if matches!(self.mode, Mode::Fail) {
                    return Err(AdapterError::Parse("controlled native failure".into()));
                }
            }
            _ => {}
        }
        Ok(())
    }
}
fn blocked(
    actor: &FeedBatchActor,
    dto: BootstrapDto,
    fail: bool,
) -> (
    HistoryJournalOwner,
    mpsc::Receiver<()>,
    mpsc::SyncSender<()>,
    Arc<AtomicUsize>,
) {
    blocked_mode(actor, dto, if fail { Mode::Fail } else { Mode::Normal })
}
fn blocked_mode(
    actor: &FeedBatchActor,
    dto: BootstrapDto,
    mode: Mode,
) -> (
    HistoryJournalOwner,
    mpsc::Receiver<()>,
    mpsc::SyncSender<()>,
    Arc<AtomicUsize>,
) {
    let config = dto.capture.config().unwrap();
    let mut initial = boot(dto, actor);
    let (entered_tx, entered) = mpsc::sync_channel(0);
    let (resume, resume_rx) = mpsc::sync_channel(0);
    let calls = Arc::new(AtomicUsize::new(0));
    // Test-only controlled machine through the actual persistent CaptureSession;
    // this injection is not Bybit native-semantic or actual factory replay proof.
    initial.capture = CaptureSession::new(
        Box::new(Blocked {
            entered: entered_tx,
            resume: resume_rx,
            calls: calls.clone(),
            mode,
        }),
        config,
    )
    .unwrap();
    let owner = initial.spawn_history_owner().unwrap();
    owner
        .ingress()
        .try_submit(ConnectionId(1), connected(1))
        .unwrap();
    release(&owner);
    (owner, entered, resume, calls)
}

#[test]
fn actual_bybit_factory_ingress_and_durable_handoff_share_one_valid_history() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(2));
    let owner = boot(fixture(), &actor).spawn_history_owner().unwrap();
    let admission = owner
        .ingress()
        .try_submit(ConnectionId(1), connected(1))
        .unwrap();
    let first = release(&owner);
    assert_eq!(first.input_id(), 1);
    assert!(
        first
            .released_items()
            .any(|i| matches!(i, ItemDto::Write(_)))
    );
    assert!(matches!(
        admission.wait(),
        InputSettlement::Committed { id: 1, .. }
    ));
    let status = finish(owner);
    actor.shutdown().unwrap();
    assert!(status.owner.failure.is_none());
    assert_eq!(
        (
            status.owner.accepted,
            status.owner.executed,
            status.owner.handed_off
        ),
        (1, 1, 1)
    );
    let (history, events, handoffs) = inspect(&path);
    assert!(history.uncertainties().is_empty());
    assert_eq!(handoffs.len(), 1);
    assert!(matches!(
        events[2].event,
        OwnerEventDto::ExecutionPermitted { input_id: 1, .. }
    ));
    assert!(matches!(
        events[3].event,
        OwnerEventDto::ExecutionCompleted {
            input_id: 1,
            permit_ordinal: 3,
            ..
        }
    ));
    assert_eq!(handoffs[0].operation, Box::new(first.evidence().clone()));
    // Legacy/V1/V2 refuse the new profile at negotiation, before a watermark.
    std::fs::remove_file(path.join("committed.json")).unwrap();
    for old in [
        FeedProfile::Legacy,
        FeedProfile::EngineCausalV1,
        FeedProfile::EngineCausalV2,
    ] {
        assert!(
            FeedReader::open_profile(&path, None, old)
                .unwrap_err()
                .to_string()
                .contains("header")
        );
    }
}

#[test]
fn terminal_fence_before_callback_completion_preserves_exact_late_evidence_and_no_output() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(2));
    let (owner, entered, resume, calls) = blocked(&actor, fixture(), false);
    let current = owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let queued = owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    owner.ingress().revoke();
    assert!(owner.try_next().is_none());
    assert!(owner.committed_snapshot().is_none());
    resume.send(()).unwrap();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    let status = owner.shutdown();
    actor.shutdown().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        (
            status.owner.executed,
            status.owner.durable,
            status.owner.withheld,
            status.owner.abandoned
        ),
        (2, 2, 1, 1)
    );
    assert!(matches!(
        current.wait(),
        InputSettlement::Committed { id: 2, .. }
    ));
    assert!(matches!(
        queued.wait(),
        InputSettlement::NotExecutedCommitted { id: 3, .. }
    ));
    let (history, events, handoffs) = inspect(&path);
    assert!(history.is_terminal());
    assert!(history.uncertainties().is_empty());
    assert_eq!(handoffs.len(), 1);
    let fence = events
        .iter()
        .position(|r| matches!(r.event, OwnerEventDto::Revoked { .. }))
        .unwrap();
    let completion = events
        .iter()
        .position(|r| {
            matches!(
                r.event,
                OwnerEventDto::ExecutionCompleted { input_id: 2, .. }
            )
        })
        .unwrap();
    assert!(fence < completion);
    let OwnerEventDto::ExecutionCompleted { operation, .. } = &events[completion].event else {
        panic!()
    };
    let InputDto::Text { payload, .. } = &operation.input else {
        panic!()
    };
    assert_eq!(payload.bytes().unwrap(), b"original-frame");
    assert!(operation.entries.iter().any(|e| matches!(
        e.effect,
        EffectDto::QueueDrain {
            completed: true,
            ..
        }
    )));
}

#[test]
fn later_source_fence_withholds_old_callback_but_keeps_new_recovery_usable() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(2));
    let (owner, entered, resume, calls) = blocked(&actor, fixture(), false);
    owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    owner
        .ingress()
        .try_submit(ConnectionId(2), connected(2))
        .unwrap();
    let rejected = owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap_err();
    assert_eq!(rejected.reason, OwnerFailure::SourceConnection);
    assert!(owner.status().owner.failure.is_none());
    resume.send(()).unwrap();
    let next = release(&owner);
    assert_eq!(next.input_id(), 3);
    assert_eq!(next.evidence().route.generation, 2);
    let status = finish(owner);
    actor.shutdown().unwrap();
    assert!(status.owner.failure.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (history, events, handoffs) = inspect(&path);
    assert!(!history.is_terminal());
    assert!(history.uncertainties().is_empty());
    assert_eq!(handoffs.len(), 2);
    assert!(
        events
            .iter()
            .any(|r| matches!(r.event, OwnerEventDto::Withheld { input_id: 2 }))
    );
}

#[test]
fn deadline_observed_during_callback_still_commits_completion_as_withheld() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(1));
    let mut dto = fixture();
    dto.owner_policy.as_mut().unwrap().max_ingress_age = Duration::from_secs(1).into();
    let (owner, entered, resume, calls) = blocked(&actor, dto, false);
    owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    wait(|| owner.status().owner.failure.is_some());
    assert!(matches!(
        owner.status().owner.failure,
        Some(OwnerFailure::Deadline(DeadlineStage::Executing))
    ));
    resume.send(()).unwrap();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    let status = owner.shutdown();
    actor.shutdown().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(status.owner.durable, 2);
    let (history, _, handoffs) = inspect(&path);
    assert!(history.is_terminal());
    assert!(history.uncertainties().is_empty());
    assert_eq!(handoffs.len(), 1);
}

#[test]
fn native_failure_is_complete_evidence_and_never_a_second_execution() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(1));
    let (owner, entered, resume, calls) = blocked(&actor, fixture(), true);
    owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    resume.send(()).unwrap();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    let status = owner.shutdown();
    actor.shutdown().unwrap();
    assert!(matches!(
        status.owner.failure,
        Some(OwnerFailure::Native(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (history, _, handoffs) = inspect(&path);
    assert!(history.is_terminal());
    assert!(history.uncertainties().is_empty());
    assert_eq!(handoffs.len(), 1);
}

#[test]
fn deadline_clock_overflow_rejects_before_acceptance_and_preserves_owned_input() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(1));
    let owner = boot(fixture(), &actor).spawn_history_owner().unwrap();
    wait(|| owner.status().committed_records >= 1);
    owner
        .ingress
        .shared
        .clock_offset_ns
        .store(u64::MAX - 1_000_000_000, Ordering::Relaxed);
    let rejected = owner
        .ingress()
        .try_submit(ConnectionId(1), connected(1))
        .unwrap_err();
    assert_eq!(rejected.reason, OwnerFailure::ClockExhausted);
    assert!(matches!(rejected.input, CaptureInput::Connected { .. }));
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    let status = owner.shutdown();
    actor.shutdown().unwrap();
    assert_eq!(
        (
            status.owner.accepted,
            status.owner.executed,
            status.owner.pending_operations
        ),
        (0, 0, 0)
    );
    let (history, _, handoffs) = inspect(&path);
    assert!(history.is_terminal());
    assert!(handoffs.is_empty());
}

#[test]
fn canonical_policy_role_and_profile_cannot_be_ignored_or_rebound() {
    let dto = fixture();
    assert!(PreparedBootstrap::new(dto.clone(), MAX_OPERATION_BYTES).is_err());
    assert!(
        encode_bootstrap_for_profile(&dto, MAX_OPERATION_BYTES, FeedProfile::EngineCausalV2)
            .is_err()
    );
    let wire = encode_bootstrap_for_profile(&dto, MAX_OPERATION_BYTES, FeedProfile::EngineCausalV3)
        .unwrap();
    PreparedBootstrap::from_envelope(wire).unwrap();
    let mut duplicate = dto.clone();
    duplicate
        .public_inputs
        .push(duplicate.public_inputs.last().unwrap().clone());
    assert!(PreparedBootstrap::new_history(duplicate, MAX_OPERATION_BYTES).is_err());
    let mut wrong = dto;
    wrong.public_inputs.last_mut().unwrap().sha256 = "a".repeat(64);
    assert!(wrong.history_policy().is_err());
    assert!(PreparedBootstrap::new_history(wrong, MAX_OPERATION_BYTES).is_err());
}

#[test]
fn panic_and_encoding_overflow_preserve_permit_uncertainty_and_settle_unexecuted_queue() {
    for mode in [Mode::Panic, Mode::Large] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("feed");
        let actor = actor(&path, Duration::from_millis(1));
        let (owner, entered, resume, calls) = blocked_mode(&actor, fixture(), mode);
        let current = owner
            .ingress()
            .try_submit(ConnectionId(1), frame())
            .unwrap();
        entered.recv_timeout(WAIT).unwrap();
        let queued = owner
            .ingress()
            .try_submit(ConnectionId(1), frame())
            .unwrap();
        resume.send(()).unwrap();
        wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
        let status = owner.shutdown();
        actor.shutdown().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(status.owner.durable, 1);
        assert!(matches!(current.wait(), InputSettlement::Unknown { id: 2 }));
        assert!(matches!(
            queued.wait(),
            InputSettlement::NotExecutedCommitted { id: 3, .. }
        ));
        let (history, events, handoffs) = inspect(&path);
        assert!(history.is_terminal());
        assert_eq!(handoffs.len(), 1);
        assert_eq!(
            history.uncertainties(),
            vec![OwnerPrefixUncertainty::ExecutionUnknown { input_id: 2 }]
        );
        let permit = events
            .iter()
            .find(|r| {
                matches!(
                    r.event,
                    OwnerEventDto::ExecutionPermitted { input_id: 2, .. }
                )
            })
            .unwrap()
            .route
            .ordinal;
        let uncertain = events
            .iter()
            .find(|r| {
                matches!(
                    r.event,
                    OwnerEventDto::ExecutionIndeterminate { input_id: 2, .. }
                )
            })
            .unwrap();
        let OwnerEventDto::ExecutionIndeterminate {
            permit_ordinal,
            cause,
            ..
        } = uncertain.event
        else {
            panic!()
        };
        assert_eq!(permit_ordinal, permit);
        assert_eq!(
            cause,
            if matches!(mode, Mode::Panic) {
                OwnerIndeterminateCause::Panic
            } else {
                OwnerIndeterminateCause::Encoding
            }
        );
        assert!(
            uncertain.route.ordinal > permit + 1,
            "interleaved ingress must not be mistaken for permit token"
        );
    }
}

#[test]
fn original_writer_failure_during_callback_keeps_uncertain_prefix_without_fabricated_completion() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = FeedBatchActor::spawn(
        FeedWriter::create_profile(&path, 256 << 10, FeedProfile::EngineCausalV3).unwrap(),
        BatchConfig {
            max_pending_operations: 32,
            max_pending_bytes: 8 << 20,
            max_group_operations: 32,
            max_group_bytes: 8 << 20,
            max_age: Duration::from_millis(1),
        },
    )
    .unwrap();
    let (owner, entered, resume, calls) = blocked(&actor, fixture(), false);
    let current = owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let mut other = fixture();
    other.capture.session += 1;
    other.public_inputs.push(PublicReproductionInputDto::new(
        "too-large-for-disk",
        &vec![0u8; 256 << 10],
    ));
    let failed = PreparedBootstrap::new_history(other, MAX_OPERATION_BYTES)
        .unwrap()
        .try_admit(&actor.handle())
        .unwrap()
        .wait();
    assert!(matches!(failed, Err(BootstrapError::Commit(_))));
    assert!(matches!(
        owner.status().owner.failure,
        Some(OwnerFailure::Admission(AdmissionFailure::Failed(_)))
    ));
    assert!(owner.try_next().is_none());
    resume.send(()).unwrap();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    let status = owner.shutdown();
    assert!(actor.shutdown().is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(status.owner.durable, 1);
    assert!(matches!(current.wait(), InputSettlement::Unknown { id: 2 }));
    let (history, events, handoffs) = inspect(&path);
    assert_eq!(handoffs.len(), 1);
    assert_eq!(
        history.uncertainties(),
        vec![OwnerPrefixUncertainty::ExecutionUnknown { input_id: 2 }]
    );
    assert!(!events.iter().any(|r| matches!(
        r.event,
        OwnerEventDto::ExecutionCompleted { input_id: 2, .. } | OwnerEventDto::Revoked { .. }
    )));
}

#[test]
fn stale_queued_frame_after_native_reconnect_does_not_advance_callback_counter() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(1));
    let (owner, entered, resume, calls) = blocked_mode(&actor, fixture(), Mode::Reconnect);
    owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let stale = owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    resume.send(()).unwrap();
    let transition = release(&owner);
    assert!(!transition.outputs_eligible());
    assert_eq!(transition.released_items().count(), 0);
    owner
        .ingress()
        .try_submit(ConnectionId(2), connected(2))
        .unwrap();
    let recovered = release(&owner);
    assert_eq!(
        (
            recovered.input_id(),
            recovered.evidence().route.ordinal,
            recovered.evidence().route.generation
        ),
        (4, 3, 2)
    );
    assert!(matches!(
        stale.wait(),
        InputSettlement::NotExecutedCommitted {
            id: 3,
            reason: OwnerFailure::SourceConnection,
            ..
        }
    ));
    let status = finish(owner);
    actor.shutdown().unwrap();
    assert!(status.owner.failure.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (history, _, handoffs) = inspect(&path);
    assert!(history.uncertainties().is_empty());
    assert_eq!(handoffs.len(), 3);
}

#[test]
fn full_actor_retries_same_completion_without_reexecuting_callback() {
    let mut filler = fixture();
    filler.capture.session += 1;
    filler.public_inputs.push(PublicReproductionInputDto::new(
        "large-public-input",
        &vec![0u8; 256 << 10],
    ));
    let filler = PreparedBootstrap::new_history(filler, MAX_OPERATION_BYTES).unwrap();
    let cap = filler.encoded_bytes_bound() + 512;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = FeedBatchActor::spawn(
        FeedWriter::create_profile(&path, 64 << 20, FeedProfile::EngineCausalV3).unwrap(),
        BatchConfig {
            max_pending_operations: 32,
            max_pending_bytes: cap,
            max_group_operations: 32,
            max_group_bytes: cap,
            max_age: Duration::from_millis(300),
        },
    )
    .unwrap();
    let (owner, entered, resume, calls) = blocked(&actor, fixture(), false);
    owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let pending = filler.try_admit(&actor.handle()).unwrap();
    resume.send(()).unwrap();
    wait(|| owner.status().owner.admission_retries > 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(owner.try_next().is_none());
    pending.wait().unwrap();
    let output = release(&owner);
    assert_eq!(output.input_id(), 2);
    assert_eq!(output.evidence().route.ordinal, 2);
    let status = finish(owner);
    actor.shutdown().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(status.owner.failure.is_none());
    let (history, _, handoffs) = inspect(&path);
    assert!(history.uncertainties().is_empty());
    assert_eq!(handoffs.len(), 2);
}

#[test]
fn delayed_operation_and_intent_ack_never_expose_new_state_and_dropped_waiter_does_not_cancel() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(200));
    let (owner, entered, resume, calls) = blocked(&actor, fixture(), false);
    let admission = owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    // Losing this caller cannot cancel accepted ingress or native execution.
    drop(admission);
    entered.recv_timeout(WAIT).unwrap();
    resume.send(()).unwrap();
    wait(|| {
        let s = owner.status().owner;
        s.executed == 2 && s.durable == 1 && s.phase == OwnerPhase::AwaitingCommit
    });
    assert!(owner.try_next().is_none());
    assert_eq!(owner.committed_snapshot().unwrap().route.ordinal, 1);
    let (prefix, events, _) = inspect(&path);
    assert_eq!(prefix.native_ordinal(), 1);
    assert!(!events.iter().any(|e| matches!(
        e.event,
        OwnerEventDto::ExecutionCompleted { input_id: 2, .. }
    )));
    wait(|| {
        let s = owner.status().owner;
        s.durable == 2 && s.phase == OwnerPhase::AwaitingCommit
    });
    assert!(owner.try_next().is_none());
    assert_eq!(owner.committed_snapshot().unwrap().route.ordinal, 1);
    let (prefix, events, _) = inspect(&path);
    assert_eq!(prefix.native_ordinal(), 2);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e.event, OwnerEventDto::ReleaseIntent { input_id: 2, .. }))
    );
    let output = release(&owner);
    assert_eq!(output.input_id(), 2);
    assert!(
        output
            .released_items()
            .any(|i| matches!(i, ItemDto::Write(_)))
    );
    let status = finish(owner);
    actor.shutdown().unwrap();
    assert!(status.owner.failure.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (history, _, handoffs) = inspect(&path);
    assert!(history.uncertainties().is_empty());
    assert_eq!(handoffs.len(), 2);
}

#[test]
fn native_and_observation_limits_return_owned_input_and_durably_settle_accepted_queue() {
    for observation_limit in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("feed");
        let actor = actor(&path, Duration::from_millis(2));
        let mut dto = fixture();
        if observation_limit {
            let mut policy = dto.history_policy().unwrap();
            policy.max_pending_observations = 6;
            dto.public_inputs
                .retain(|p| p.role != "owner-history-runtime-v1");
            dto.bind_history_policy(policy).unwrap();
        } else {
            dto.owner_policy.as_mut().unwrap().max_pending_operations = 2;
        }
        let (owner, entered, resume, calls) = blocked(&actor, dto, false);
        let running = owner
            .ingress()
            .try_submit(ConnectionId(1), frame())
            .unwrap();
        entered.recv_timeout(WAIT).unwrap();
        let queued = owner
            .ingress()
            .try_submit(ConnectionId(1), frame())
            .unwrap();
        let original = frame();
        let rejected = owner
            .ingress()
            .try_submit(ConnectionId(1), original.clone())
            .unwrap_err();
        assert_eq!(
            rejected.reason,
            if observation_limit {
                OwnerFailure::ObservationFull
            } else {
                OwnerFailure::IngressFull
            }
        );
        assert_eq!(rejected.input, original);
        assert_eq!(owner.status().owner.accepted, 3);
        assert!(owner.try_next().is_none());
        resume.send(()).unwrap();
        assert!(matches!(
            running.wait(),
            InputSettlement::Committed { id: 2, .. }
        ));
        assert!(matches!(
            queued.wait(),
            InputSettlement::NotExecutedCommitted { id: 3, .. }
        ));
        wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
        let status = owner.shutdown();
        actor.shutdown().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(status.owner.pending_operations, 0);
        assert_eq!(status.ingress_observations, 0);
        let (history, _, handoffs) = inspect(&path);
        assert!(history.is_terminal());
        assert!(history.uncertainties().is_empty());
        assert_eq!(handoffs.len(), 1);
    }
}

#[test]
fn finalization_expiry_does_not_cancel_an_accepted_late_completion_ticket_or_revive_output() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(200));
    let mut dto = fixture();
    let mut policy = dto.history_policy().unwrap();
    policy.max_finalization_age = Duration::from_millis(50).into();
    dto.public_inputs
        .retain(|p| p.role != "owner-history-runtime-v1");
    dto.bind_history_policy(policy).unwrap();
    let (owner, entered, resume, calls) = blocked(&actor, dto, false);
    let running = owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    resume.send(()).unwrap();
    wait(|| {
        let s = owner.status().owner;
        s.executed == 2 && s.durable == 1 && s.phase == OwnerPhase::AwaitingCommit
    });
    owner.ingress().revoke();
    wait(|| owner.status().finalization_expired);
    assert!(ticket(&owner.ingress.shared).is_some());
    assert!(owner.try_next().is_none());
    assert!(owner.committed_snapshot().is_none());
    assert!(matches!(
        running.wait(),
        InputSettlement::Committed { id: 2, .. }
    ));
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    let status = owner.shutdown();
    actor.shutdown().unwrap();
    assert!(status.finalization_expired);
    assert_eq!(status.owner.failure, Some(OwnerFailure::Revoked));
    assert_eq!(status.owner.durable, 2);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (history, events, handoffs) = inspect(&path);
    // The accepted completion and its ordered finalization remain durable
    // after expiry; neither time nor a lost waiter cancels original work.
    assert_eq!(history.native_ordinal(), 2);
    assert!(history.is_terminal());
    assert!(history.uncertainties().is_empty());
    assert!(
        events
            .iter()
            .any(|e| matches!(e.event, OwnerEventDto::Revoked { .. }))
    );
    assert_eq!(handoffs.len(), 1);
}

#[test]
fn external_io_and_live_control_inputs_are_rejected_before_native_acceptance() {
    for input in [
        CaptureInput::Live {
            now: TimestampNs(1),
        },
        CaptureInput::TakeWrites,
        CaptureInput::ForwardDispatch { results: vec![] },
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("feed");
        let actor = actor(&path, Duration::from_millis(2));
        let owner = boot(fixture(), &actor).spawn_history_owner().unwrap();
        let rejected = owner
            .ingress()
            .try_submit(ConnectionId(0), input.clone())
            .unwrap_err();
        assert_eq!(rejected.reason, OwnerFailure::UnsupportedInput);
        assert_eq!(rejected.input, input);
        wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
        let status = owner.shutdown();
        actor.shutdown().unwrap();
        assert_eq!(status.owner.accepted, 0);
        assert_eq!(status.owner.executed, 0);
        let (history, _, handoffs) = inspect(&path);
        assert!(history.is_terminal());
        assert!(history.uncertainties().is_empty());
        assert!(handoffs.is_empty());
    }
}

#[test]
fn healthy_full_past_finalization_expiry_retains_complete_evidence_and_ordered_predecessors() {
    for blocked_predecessor in [false, true] {
        let mut filler = fixture();
        filler.capture.session += 1;
        filler.public_inputs.push(PublicReproductionInputDto::new(
            "large-public-input",
            &vec![0u8; 256 << 10],
        ));
        let filler = PreparedBootstrap::new_history(filler, MAX_OPERATION_BYTES).unwrap();
        let cap = filler.encoded_bytes_bound() + 512;
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("feed");
        let actor = FeedBatchActor::spawn(
            FeedWriter::create_profile(&path, 64 << 20, FeedProfile::EngineCausalV3).unwrap(),
            BatchConfig {
                max_pending_operations: 32,
                max_pending_bytes: cap,
                max_group_operations: 32,
                max_group_bytes: cap,
                max_age: Duration::from_millis(250),
            },
        )
        .unwrap();
        let mut dto = fixture();
        let mut policy = dto.history_policy().unwrap();
        policy.max_finalization_age = Duration::from_millis(50).into();
        dto.public_inputs
            .retain(|p| p.role != "owner-history-runtime-v1");
        dto.bind_history_policy(policy).unwrap();
        let (owner, entered, resume, calls) = blocked(&actor, dto, false);
        let running = owner
            .ingress()
            .try_submit(ConnectionId(1), frame())
            .unwrap();
        entered.recv_timeout(WAIT).unwrap();
        // This uncommitted ingress precedes Revoked and the available completion.
        // Abandoning any predecessor would drop that completion too.
        let queued = blocked_predecessor.then(|| {
            owner
                .ingress()
                .try_submit(ConnectionId(1), frame())
                .unwrap()
        });
        let filler_ticket = filler.try_admit(&actor.handle()).unwrap();
        if blocked_predecessor {
            owner.ingress().revoke();
            resume.send(()).unwrap();
        } else {
            resume.send(()).unwrap();
            wait(|| owner.status().owner.admission_retries > 0);
            owner.ingress().revoke();
        }
        wait(|| {
            let s = owner.status();
            s.finalization_expired && s.owner.admission_retries > 0
        });
        assert!(owner.try_next().is_none());
        assert!(owner.committed_snapshot().is_none());
        filler_ticket.wait().unwrap();
        assert!(matches!(
            running.wait(),
            InputSettlement::Committed { id: 2, .. }
        ));
        if let Some(queued) = queued {
            assert!(matches!(
                queued.wait(),
                InputSettlement::NotExecutedCommitted { id: 3, .. }
            ));
        }
        wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
        let status = owner.shutdown();
        actor.shutdown().unwrap();
        assert!(status.finalization_expired);
        assert_eq!(status.owner.failure, Some(OwnerFailure::Revoked));
        assert_eq!(status.owner.pending_operations, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let (history, events, handoffs) = inspect(&path);
        assert!(history.is_terminal());
        assert!(history.uncertainties().is_empty());
        assert_eq!(history.native_ordinal(), 2);
        assert_eq!(handoffs.len(), 1);
        let revoked = events
            .iter()
            .position(|r| matches!(r.event, OwnerEventDto::Revoked { .. }))
            .unwrap();
        let completion = events
            .iter()
            .position(|r| {
                matches!(
                    r.event,
                    OwnerEventDto::ExecutionCompleted { input_id: 2, .. }
                )
            })
            .unwrap();
        assert_eq!(revoked < completion, blocked_predecessor);
        let OwnerEventDto::ExecutionCompleted { operation, .. } = &events[completion].event else {
            panic!()
        };
        let InputDto::Text { payload, .. } = &operation.input else {
            panic!()
        };
        assert_eq!(payload.bytes().unwrap(), b"original-frame");
    }
}
