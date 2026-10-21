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
fn source_prefix_reduces_publications_and_permission_is_durable_at_callback() {
    struct InspectPermission {
        path: std::path::PathBuf,
        identity: OperationIdentity,
        entered: mpsc::SyncSender<(FeedCursor, Vec<&'static str>)>,
        calls: Arc<AtomicUsize>,
    }
    impl SessionMachine for InspectPermission {
        fn on_input(
            &mut self,
            input: SessionInput<'_>,
            output: &mut ActionBuffer,
        ) -> Result<(), AdapterError> {
            if matches!(input, SessionInput::Connected { .. }) {
                self.calls.fetch_add(1, Ordering::SeqCst);
                // Observe actual barrier-published disk bytes at callback entry;
                // actor queue credits or the owner's RAM are not permission.
                let mut reader =
                    FeedReader::open_profile(&self.path, None, FeedProfile::EngineCausalV3)
                        .unwrap();
                assert!(matches!(
                    reader.read_next().unwrap(),
                    Some(FeedRecord::Causal(record)) if record.phase == CausalPhase::Bootstrap
                ));
                let mut events = Vec::new();
                while let Some(record) = reader.read_next().unwrap() {
                    let FeedRecord::Causal(record) = record else {
                        panic!("only committed owner records before callback")
                    };
                    let event = decode_owner_history(&record, &self.identity).unwrap();
                    events.push(match event.event {
                        OwnerEventDto::Started { .. } => "started",
                        OwnerEventDto::Ingress { input_id: 1, .. } => "ingress1",
                        OwnerEventDto::ExecutionPermitted { input_id: 1, .. } => "permit1",
                        other => panic!("unexpected pre-callback event {other:?}"),
                    });
                }
                self.entered
                    .send((reader.cursor().clone(), events))
                    .unwrap();
                output.push(SessionAction::MarkLive);
            }
            Ok(())
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    // Test-only grouping hold; source/owner limits and the10s watchdog stay
    // unchanged. Aggregate grouping reduction does not identify a specific
    // pair of records sharing a watermark.
    let actor = actor(&path, Duration::from_millis(100));
    let dto = fixture();
    let config = dto.capture.config().unwrap();
    let mut initial = boot(dto, &actor);
    let (entered_tx, entered) = mpsc::sync_channel(1);
    let calls = Arc::new(AtomicUsize::new(0));
    // Controlled machine probes owner/writer ordering, not native factory
    // semantics. Actual Bybit/HL native/replay tests remain separate.
    initial.capture = CaptureSession::new(
        Box::new(InspectPermission {
            path: path.clone(),
            identity: initial.identity().clone(),
            entered: entered_tx,
            calls: calls.clone(),
        }),
        config,
    )
    .unwrap();
    let owner = initial.spawn_history_owner().unwrap();
    let admission = owner
        .ingress()
        .try_submit(ConnectionId(1), connected(1))
        .unwrap();
    let (durable_at_callback, prefix) = entered.recv_timeout(WAIT).unwrap();
    assert_eq!(prefix, ["started", "ingress1", "permit1"]);
    assert_eq!(durable_at_callback.sequence, 4); // includes bootstrap
    let handoff = release(&owner);
    assert!(matches!(
        admission.wait(),
        InputSettlement::Committed { id: 1, .. }
    ));
    let status = finish(owner);
    let report = actor.shutdown().unwrap();
    assert!(status.owner.failure.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        (
            status.owner.accepted,
            status.owner.executed,
            status.owner.handed_off
        ),
        (1, 1, 1)
    );
    let (history, events, handoffs) = inspect(&path);
    assert_eq!(history.native_ordinal(), 1);
    assert!(history.uncertainties().is_empty());
    assert_eq!(handoffs.len(), 1);
    assert_eq!(handoffs[0].operation, Box::new(handoff.evidence().clone()));
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.event, OwnerEventDto::ExecutionPermitted { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.event, OwnerEventDto::ExecutionCompleted { .. }))
            .count(),
        1
    );
    assert_eq!(report.committed_operations, report.timings.accepted_records);
    assert_eq!(report.published.sequence, report.timings.accepted_records);
    assert!(
        report.groups < report.timings.accepted_records,
        "non-authorizing prefix deferral must reduce aggregate physical publications: {report:?}"
    );
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
    let first = owner.ingress().pause_next_receipt_for_test();
    let after = Arc::new(TextPauseState::default());
    *owner
        .ingress
        .shared
        .pipeline_hooks
        .after_completion
        .lock()
        .unwrap() = Some(after.clone());
    let after = HistoryTextPause(after);
    resume.send(()).unwrap();
    wait(|| first.entered());
    let before_completion = owner.status();
    let first_no_ready = owner.try_next().is_none();
    let first_snapshot = owner.committed_snapshot().map(|s| s.route.ordinal);
    let first_pending = {
        let registry = tickets(&owner.ingress.shared);
        let completion = registry.completion.as_ref().unwrap();
        let intent = registry.intent.as_ref().unwrap();
        completion.receipt.is_none()
            && registry
                .pending
                .iter()
                .any(|p| p.ordinal == completion.ordinal)
            && registry.pending.iter().any(|p| p.ordinal == intent.ordinal)
    };
    // A physical publication may already contain both records. Owner visibility
    // still waits for each validated own ACK; physical presence is not release.
    let (first_prefix, _, first_handoffs) = inspect(&path);
    first.release();
    wait(|| after.entered());
    let after_completion = owner.status();
    let second_no_ready = owner.try_next().is_none();
    let second_snapshot = owner.committed_snapshot().map(|s| s.route.ordinal);
    let (cached_completion, unobserved_intent) = {
        let registry = tickets(&owner.ingress.shared);
        (
            registry.completion.as_ref().and_then(|c| c.receipt.clone()),
            registry.intent.as_ref().is_some_and(|i| i.result.is_none()),
        )
    };
    let (second_prefix, _, second_handoffs) = inspect(&path);
    after.release();
    let output = release(&owner);
    let status = finish(owner);
    actor.shutdown().unwrap();
    // All fixture barriers and workers are joined before assertions, including
    // the intentional dropped ingress waiter.
    assert_eq!(before_completion.owner.executed, 2);
    assert_eq!(before_completion.owner.durable, 1);
    assert_eq!(before_completion.owner.phase, OwnerPhase::AwaitingCommit);
    assert!(first_no_ready);
    assert!(first_pending);
    assert_eq!(first_snapshot, Some(1));
    assert!(matches!(first_prefix.native_ordinal(), 1 | 2));
    assert_eq!(first_handoffs.len(), 1);
    assert_eq!(after_completion.owner.durable, 2);
    assert_eq!(after_completion.owner.phase, OwnerPhase::AwaitingCommit);
    assert!(cached_completion.is_some());
    assert!(unobserved_intent);
    assert!(second_no_ready);
    assert_eq!(second_snapshot, Some(1));
    assert_eq!(second_prefix.native_ordinal(), 2);
    assert_eq!(second_handoffs.len(), 1);
    assert_eq!(output.input_id(), 2);
    assert!(
        output
            .released_items()
            .any(|i| matches!(i, ItemDto::Write(_)))
    );
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
    let acknowledgement = owner.ingress().pause_next_receipt_for_test();
    resume.send(()).unwrap();
    wait(|| acknowledgement.entered());
    wait(|| {
        let s = owner.status().owner;
        s.executed == 2 && s.durable == 1 && s.phase == OwnerPhase::AwaitingCommit
    });
    owner.ingress().revoke();
    wait(|| owner.status().finalization_expired);
    assert!(!tickets(&owner.ingress.shared).pending.is_empty());
    assert!(owner.try_next().is_none());
    assert!(owner.committed_snapshot().is_none());
    acknowledgement.release();
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

// Controlled machine only. This probe measures owner authorization barriers;
// it does not substitute for native-factory replay equivalence.
struct PrefixCounter(Arc<AtomicUsize>);
impl SessionMachine for PrefixCounter {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if matches!(
            input,
            SessionInput::Connected { .. } | SessionInput::TextFrame { .. }
        ) {
            self.0.fetch_add(1, Ordering::SeqCst);
            output.push(SessionAction::MarkLive);
        }
        Ok(())
    }
}
fn prefix_counter_boot(
    dto: BootstrapDto,
    actor: &FeedBatchActor,
) -> (BootstrappedSession, Arc<AtomicUsize>) {
    let config = dto.capture.config().unwrap();
    let mut initial = boot(dto, actor);
    let calls = Arc::new(AtomicUsize::new(0));
    initial.capture = CaptureSession::new(Box::new(PrefixCounter(calls.clone())), config).unwrap();
    (initial, calls)
}

#[test]
fn source_prefix_authority_waits_for_every_retained_ack_and_late_revoke_never_invokes() {
    for fence in 0..3 {
        let revoke = fence != 0;
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("feed");
        let actor = actor(&path, Duration::from_millis(100));
        let (initial, calls) = prefix_counter_boot(fixture(), &actor);
        let (owner, started) = initial
            .spawn_history_owner_with_receipt_pause_for_test()
            .unwrap();
        wait(|| started.entered());
        let authority = owner.ingress().pause_next_receipt_for_test();
        let admission = owner
            .ingress()
            .try_submit(ConnectionId(1), connected(1))
            .unwrap();
        started.release();
        wait(|| authority.entered());
        {
            let registry = tickets(&owner.ingress.shared);
            assert_eq!(registry.pending.len(), 2); // Ingress and permission, Started already observed
            assert_eq!(registry.pending[0].ordinal, 2);
            assert_eq!(registry.pending[1].ordinal, 3);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(owner.status().owner.executed, 0);
        assert!(owner.try_next().is_none());
        assert!(owner.committed_snapshot().is_none());
        if fence == 1 {
            owner.ingress().revoke();
        } else if fence == 2 {
            // Keep the actual fixture's5s ingress deadline and the10s
            // watchdog; retained authority ACK observation cannot extend it.
            wait(|| owner.status().owner.failure.is_some());
            assert!(matches!(
                owner.status().owner.failure,
                Some(OwnerFailure::Deadline(_))
            ));
        }
        authority.release();
        if revoke {
            assert!(matches!(
                admission.wait(),
                InputSettlement::NotExecutedCommitted { id: 1, .. }
            ));
            wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert!(owner.try_next().is_none());
            owner.shutdown();
        } else {
            release(&owner);
            assert!(matches!(
                admission.wait(),
                InputSettlement::Committed { id: 1, .. }
            ));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            finish(owner);
        }
        actor.shutdown().unwrap();
        let (history, events, handoffs) = inspect(&path);
        assert!(history.uncertainties().is_empty());
        assert_eq!(handoffs.len(), usize::from(!revoke));
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e.event, OwnerEventDto::ExecutionCompleted { .. }))
                .count(),
            usize::from(!revoke)
        );
        if revoke {
            assert!(events.iter().any(|e| matches!(
                e.event,
                OwnerEventDto::ExecutionNotInvoked { input_id: 1, .. }
            )));
            assert!(history.is_terminal());
        }
    }
}

#[test]
fn source_prefix_started_idle_drain_rechecks_concurrent_revocation() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(100));
    let (initial, calls) = prefix_counter_boot(fixture(), &actor);
    let (owner, acknowledgement) = initial
        .spawn_history_owner_with_receipt_pause_for_test()
        .unwrap();
    wait(|| acknowledgement.entered());
    assert_eq!(tickets(&owner.ingress.shared).pending.len(), 1);
    owner.close_ingress();
    owner.ingress().revoke();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    acknowledgement.release();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    let status = owner.shutdown();
    actor.shutdown().unwrap();
    assert_eq!(status.owner.failure, Some(OwnerFailure::Revoked));
    let (history, events, handoffs) = inspect(&path);
    assert!(history.is_terminal());
    assert!(history.uncertainties().is_empty());
    assert!(handoffs.is_empty());
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0].event, OwnerEventDto::Started { .. }));
    assert!(matches!(events[1].event, OwnerEventDto::Revoked { .. }));
    assert_eq!(status.committed_records, 2);
}

#[test]
fn source_prefix_outer_worker_panic_drains_all_shared_tickets_without_native_invocation() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(100));
    let (initial, calls) = prefix_counter_boot(fixture(), &actor);
    let (owner, started) = initial
        .spawn_history_owner_with_receipt_pause_for_test()
        .unwrap();
    wait(|| started.entered());
    owner
        .ingress
        .shared
        .panic_after_prefix_admission
        .store(true, Ordering::SeqCst);
    let admission = owner
        .ingress()
        .try_submit(ConnectionId(1), connected(1))
        .unwrap();
    started.release();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    assert!(matches!(
        admission.wait(),
        InputSettlement::Unknown { id: 1 }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(tickets(&owner.ingress.shared).pending.is_empty());
    let status = owner.shutdown();
    let report = actor.shutdown().unwrap();
    assert_eq!(status.owner.failure, Some(OwnerFailure::WorkerPanicked));
    assert_eq!(status.committed_records, 3); // Started, Ingress and permit all settled after panic
    assert_eq!(status.ingress_observations, 0);
    assert_eq!(status.owner.pending_operations, 0);
    assert_eq!(report.timings.accepted_records, 4); // bootstrap plus the retained prefix
    let (history, events, handoffs) = inspect(&path);
    assert!(handoffs.is_empty());
    assert_eq!(events.len(), 3);
    assert!(matches!(
        events[2].event,
        OwnerEventDto::ExecutionPermitted { input_id: 1, .. }
    ));
    assert_eq!(
        history.uncertainties(),
        vec![OwnerPrefixUncertainty::ExecutionUnknown { input_id: 1 }]
    );
}

#[test]
fn source_prefix_started_only_idle_and_close_observe_actual_ack_without_input() {
    for close_before_ack in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("feed");
        let actor = actor(&path, Duration::from_millis(100));
        let (initial, calls) = prefix_counter_boot(fixture(), &actor);
        let (owner, acknowledgement) = initial
            .spawn_history_owner_with_receipt_pause_for_test()
            .unwrap();
        wait(|| acknowledgement.entered());
        if close_before_ack {
            owner.close_ingress();
        }
        acknowledgement.release();
        if !close_before_ack {
            wait(|| {
                owner.status().committed_records == 1
                    && owner.status().owner.phase == OwnerPhase::Idle
            });
        }
        let status = finish(owner);
        let report = actor.shutdown().unwrap();
        assert!(status.owner.failure.is_none());
        assert_eq!(status.committed_records, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(report.timings.accepted_records, 2);
        let (_, events, handoffs) = inspect(&path);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0].event, OwnerEventDto::Started { .. }));
        assert!(handoffs.is_empty());
    }
}

#[test]
fn source_prefix_closing_return_window_preserves_concurrent_revocation_record() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(2));
    let (initial, calls) = prefix_counter_boot(fixture(), &actor);
    let owner = initial.spawn_history_owner().unwrap();
    wait(|| {
        owner.status().committed_records == 1 && owner.status().owner.phase == OwnerPhase::Idle
    });
    let closing = owner.ingress().pause_next_closing_for_test();
    owner.close_ingress();
    wait(|| closing.entered()); // closing branch finished ACK drain, not blocked disk I/O
    owner.ingress().revoke();
    closing.release();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    let status = owner.shutdown();
    actor.shutdown().unwrap();
    let (history, events, handoffs) = inspect(&path);
    assert_eq!(status.owner.failure, Some(OwnerFailure::Revoked));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(status.committed_records, 2);
    assert!(history.is_terminal());
    assert!(history.uncertainties().is_empty());
    assert!(handoffs.is_empty());
    assert!(matches!(events[1].event, OwnerEventDto::Revoked { .. }));
}

#[test]
fn source_prefix_full_ingress_retry_never_duplicates_input_or_invokes_before_authority() {
    let mut filler = fixture();
    filler.capture.session += 1;
    filler.public_inputs.push(PublicReproductionInputDto::new(
        "large-public-input",
        &vec![0u8; 256 << 10],
    ));
    let filler = PreparedBootstrap::new_history(filler, MAX_OPERATION_BYTES).unwrap();
    let cap = filler.encoded_bytes_bound() + 512; // same actual Full layout as existing test
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
    let (initial, calls) = prefix_counter_boot(fixture(), &actor);
    let (owner, started) = initial
        .spawn_history_owner_with_receipt_pause_for_test()
        .unwrap();
    wait(|| started.entered());
    wait(|| actor.handle().status().committed_operations == 2); // bootstrap + actual Started; observer still paused
    let filler_ticket = filler.try_admit(&actor.handle()).unwrap();
    let admission = owner
        .ingress()
        .try_submit(ConnectionId(1), connected(1))
        .unwrap();
    started.release();
    wait(|| owner.status().owner.admission_retries > 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(owner.try_next().is_none());
    assert!(owner.committed_snapshot().is_none());
    filler_ticket.wait().unwrap();
    let handoff = release(&owner);
    assert_eq!(handoff.input_id(), 1);
    assert!(matches!(
        admission.wait(),
        InputSettlement::Committed { id: 1, .. }
    ));
    let status = finish(owner);
    actor.shutdown().unwrap();
    assert!(status.owner.failure.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (history, events, handoffs) = inspect(&path);
    assert!(history.uncertainties().is_empty());
    assert_eq!(handoffs.len(), 1);
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.event, OwnerEventDto::Ingress { input_id: 1, .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(
                e.event,
                OwnerEventDto::ExecutionPermitted { input_id: 1, .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(
                e.event,
                OwnerEventDto::ExecutionCompleted { input_id: 1, .. }
            ))
            .count(),
        1
    );
}

#[test]
fn source_prefix_actual_writer_failure_settles_queued_authority_without_invoking() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(300));
    let (initial, calls) = prefix_counter_boot(fixture(), &actor);
    let (owner, started) = initial
        .spawn_history_owner_with_receipt_pause_for_test()
        .unwrap();
    wait(|| started.entered());
    wait(|| actor.handle().status().committed_operations == 2);
    std::fs::write(path.join("committed.tmp"), b"occupied staging path").unwrap();
    let authority = owner.ingress().pause_next_receipt_for_test();
    let admission = owner
        .ingress()
        .try_submit(ConnectionId(1), connected(1))
        .unwrap();
    started.release();
    wait(|| authority.entered());
    assert_eq!(tickets(&owner.ingress.shared).pending.len(), 2);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    authority.release();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    assert!(matches!(
        admission.wait(),
        InputSettlement::Unknown { id: 1 }
    ));
    assert!(tickets(&owner.ingress.shared).pending.is_empty());
    let status = owner.shutdown();
    assert!(actor.shutdown().is_err());
    assert!(status.owner.failure.is_some());
    assert_eq!(status.owner.executed, 0);
    assert_eq!(status.owner.pending_operations, 0);
    assert_eq!(status.ingress_observations, 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let (_, events, handoffs) = inspect(&path);
    assert_eq!(events.len(), 1); // failed Ingress/authority bytes are outside actual watermark
    assert!(handoffs.is_empty());
    assert!(
        std::fs::metadata(path.join("events.mfsf")).unwrap().len() > actor_watermark_offset(&path)
    );
}
fn actor_watermark_offset(path: &std::path::Path) -> u64 {
    let mut reader = FeedReader::open_profile(path, None, FeedProfile::EngineCausalV3).unwrap();
    while reader.read_next().unwrap().is_some() {}
    reader.cursor().offset
}

#[test]
fn source_prefix_interleaved_owners_accept_admission_gaps_and_preserve_each_logical_boundary() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(100));
    let first_dto = fixture();
    let first_session = first_dto.capture.session;
    let mut second_dto = first_dto.clone();
    second_dto.capture.session += 1;
    let second_session = second_dto.capture.session;
    let (initial1, calls1) = prefix_counter_boot(first_dto, &actor);
    let (owner1, started1) = initial1
        .spawn_history_owner_with_receipt_pause_for_test()
        .unwrap();
    wait(|| started1.entered());
    let (initial2, calls2) = prefix_counter_boot(second_dto, &actor);
    let (owner2, started2) = initial2
        .spawn_history_owner_with_receipt_pause_for_test()
        .unwrap();
    wait(|| started2.entered());
    let authority1 = owner1.ingress().pause_next_receipt_for_test();
    let authority2 = owner2.ingress().pause_next_receipt_for_test();
    let a1 = owner1
        .ingress()
        .try_submit(ConnectionId(1), connected(1))
        .unwrap();
    let a2 = owner2
        .ingress()
        .try_submit(ConnectionId(1), connected(1))
        .unwrap();
    started1.release();
    started2.release();
    wait(|| authority1.entered() && authority2.entered());
    {
        let registry = tickets(&owner1.ingress.shared);
        assert!(
            registry.pending.front().unwrap().ticket.admission() > registry.previous_admission + 1,
            "other owner's actual bootstrap/Started occupy intervening admissions"
        );
    }
    assert_eq!(
        (calls1.load(Ordering::SeqCst), calls2.load(Ordering::SeqCst)),
        (0, 0)
    );
    authority1.release();
    authority2.release();
    release(&owner1);
    release(&owner2);
    assert!(matches!(
        a1.wait(),
        InputSettlement::Committed { id: 1, .. }
    ));
    assert!(matches!(
        a2.wait(),
        InputSettlement::Committed { id: 1, .. }
    ));
    assert!(finish(owner1).owner.failure.is_none());
    assert!(finish(owner2).owner.failure.is_none());
    let report = actor.shutdown().unwrap();
    assert_eq!(
        (calls1.load(Ordering::SeqCst), calls2.load(Ordering::SeqCst)),
        (1, 1)
    );
    assert!(report.groups < report.timings.accepted_records);
    let mut reader = FeedReader::open_profile(&path, None, FeedProfile::EngineCausalV3).unwrap();
    let mut histories = std::collections::BTreeMap::new();
    let mut handoffs = std::collections::BTreeMap::<u64, usize>::new();
    let mut boundaries = 0;
    while let Some(record) = reader.read_next().unwrap() {
        let FeedRecord::Causal(record) = record else {
            panic!()
        };
        if record.phase == CausalPhase::Bootstrap {
            let history = OwnerHistory::from_bootstrap(
                &record,
                OwnerHistoryLimits {
                    max_retained_input_bytes: 4 << 20,
                    max_input_bytes: 256 << 10,
                    max_record_bytes: 256 << 10,
                },
            )
            .unwrap();
            assert!(histories.insert(record.route.session, history).is_none());
        } else {
            let history = histories.get_mut(&record.route.session).unwrap();
            if history.apply(&record).unwrap().is_some() {
                *handoffs.entry(record.route.session).or_default() += 1;
            }
        }
        boundaries += 1;
    }
    assert_eq!(boundaries, report.timings.accepted_records);
    assert_eq!(handoffs.get(&first_session), Some(&1));
    assert_eq!(handoffs.get(&second_session), Some(&1));
    assert!(
        histories
            .values()
            .all(|h| h.uncertainties().is_empty() && h.native_ordinal() == 1)
    );
}

#[test]
fn source_prefix_actual_factory_replay_preserves_one_native_turn_and_every_record_resume() {
    use super::super::super::replay::HistoryReplay;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(100));
    let owner = boot(fixture(), &actor).spawn_history_owner().unwrap();
    let admission = owner
        .ingress()
        .try_submit(ConnectionId(1), connected(1))
        .unwrap();
    let actual = release(&owner);
    assert!(matches!(
        admission.wait(),
        InputSettlement::Committed { id: 1, .. }
    ));
    assert!(finish(owner).owner.failure.is_none());
    let report = actor.shutdown().unwrap();
    let replay_limits = || OwnerHistoryLimits {
        max_retained_input_bytes: 4 << 20,
        max_input_bytes: 256 << 10,
        max_record_bytes: 256 << 10,
    };
    let mut replay = HistoryReplay::open(&path, replay_limits()).unwrap();
    let mut boundaries = vec![replay.progress().unwrap().cursor];
    let mut native_turns = 0;
    let mut released = Vec::new();
    while let Some(step) = replay.read_next().unwrap() {
        assert_eq!(
            step.cursor.sequence,
            boundaries.last().unwrap().sequence + 1
        );
        let native = replay.progress().unwrap().native_ordinal;
        assert!(native == native_turns || native == native_turns + 1);
        native_turns = native;
        if let Some(handoff) = step.handoff {
            released.push(handoff);
        }
        boundaries.push(step.cursor);
    }
    assert_eq!(native_turns, 1);
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].operation, Box::new(actual.evidence().clone()));
    assert_eq!(boundaries.len() as u64, report.timings.accepted_records);
    for boundary in boundaries {
        let mut resumed = HistoryReplay::resume(&path, boundary, replay_limits()).unwrap();
        while resumed.read_next().unwrap().is_some() {}
        let progress = resumed.progress().unwrap();
        assert_eq!(progress.native_ordinal, 1);
        assert_eq!(progress.cursor, report.published);
        assert!(progress.uncertainties.is_empty());
    }
}

#[test]
fn source_prefix_two_ingresses_and_one_authority_form_bounded_retained_window() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(100));
    let (initial, calls) = prefix_counter_boot(fixture(), &actor);
    let (owner, started) = initial
        .spawn_history_owner_with_receipt_pause_for_test()
        .unwrap();
    wait(|| started.entered());
    let authority = owner.ingress().pause_next_receipt_for_test();
    let first = owner
        .ingress()
        .try_submit(ConnectionId(1), connected(1))
        .unwrap();
    let second = owner
        .ingress()
        .try_submit(ConnectionId(1), frame())
        .unwrap();
    started.release();
    wait(|| authority.entered());
    {
        let registry = tickets(&owner.ingress.shared);
        assert_eq!(registry.pending.len(), 3);
        assert_eq!(
            registry
                .pending
                .iter()
                .map(|p| p.ordinal)
                .collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        assert_ne!(registry.pending[0].ingress_bytes, 0);
        assert_ne!(registry.pending[1].ingress_bytes, 0);
        assert_eq!(registry.pending[2].ingress_bytes, 0);
    }
    assert_eq!(owner.status().ingress_observations, 2);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    authority.release();
    assert_eq!(release(&owner).input_id(), 1);
    assert!(matches!(
        first.wait(),
        InputSettlement::Committed { id: 1, .. }
    ));
    assert_eq!(release(&owner).input_id(), 2);
    assert!(matches!(
        second.wait(),
        InputSettlement::Committed { id: 2, .. }
    ));
    let status = finish(owner);
    actor.shutdown().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(status.ingress_observations, 0);
    assert_eq!(status.owner.pending_operations, 0);
    assert!(status.owner.failure.is_none());
    let (history, events, handoffs) = inspect(&path);
    assert!(history.uncertainties().is_empty());
    assert_eq!(history.native_ordinal(), 2);
    assert_eq!(handoffs.len(), 2);
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.event, OwnerEventDto::Ingress { .. }))
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.event, OwnerEventDto::ExecutionCompleted { .. }))
            .count(),
        2
    );
}

#[test]
fn closing_drive_return_is_already_stopped_and_late_revoke_cannot_change_status_without_journal() {
    struct DropGateMachine {
        calls: Arc<AtomicUsize>,
        armed: Arc<std::sync::atomic::AtomicBool>,
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    }
    impl SessionMachine for DropGateMachine {
        fn on_input(
            &mut self,
            input: SessionInput<'_>,
            output: &mut ActionBuffer,
        ) -> Result<(), AdapterError> {
            if matches!(input, SessionInput::Connected { .. }) {
                self.calls.fetch_add(1, Ordering::SeqCst);
                output.push(SessionAction::MarkLive);
            }
            Ok(())
        }
    }
    impl Drop for DropGateMachine {
        fn drop(&mut self) {
            if !self.armed.load(Ordering::SeqCst) {
                return;
            }
            self.entered.send(()).unwrap();
            self.release.recv().unwrap();
        }
    }
    struct ReleaseOnDrop(Option<mpsc::Sender<()>>);
    impl ReleaseOnDrop {
        fn release(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(()); // unbounded send never waits for Drop
            }
        }
    }
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.release();
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(2));
    let dto = fixture();
    let config = dto.capture.config().unwrap();
    let mut initial = boot(dto, &actor);
    let calls = Arc::new(AtomicUsize::new(0));
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (entered_tx, entered) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    // Controlled machine destruction exposes the actual drive-return to outer
    // cleanup window. No runtime hook, live transport or native proof is added.
    initial.capture = CaptureSession::new(
        Box::new(DropGateMachine {
            calls: calls.clone(),
            armed: armed.clone(),
            entered: entered_tx,
            release: release_rx,
        }),
        config,
    )
    .unwrap();
    let owner = initial.spawn_history_owner().unwrap();
    // Declared after owner: assertion unwinding releases the gate before the
    // owner's Drop joins. All final regression assertions follow explicit join.
    let mut unblock = ReleaseOnDrop(Some(release_tx));
    armed.store(true, Ordering::SeqCst);
    let admission = owner
        .ingress()
        .try_submit(ConnectionId(1), connected(1))
        .unwrap();
    let output = release(&owner);
    assert_eq!(output.input_id(), 1);
    assert!(matches!(
        admission.wait(),
        InputSettlement::Committed { id: 1, .. }
    ));
    wait(|| owner.status().owner.phase == OwnerPhase::Idle);
    let before_snapshot = owner.committed_snapshot().unwrap();
    assert_eq!(before_snapshot.route.ordinal, 1);
    owner.close_ingress();
    entered.recv_timeout(WAIT).unwrap();
    let at_drive_return = owner.status();
    let snapshot_at_return = owner.committed_snapshot().map(|s| s.route.ordinal);
    owner.ingress().revoke();
    let after_late_revoke = owner.status();
    let snapshot_after_revoke = owner.committed_snapshot().map(|s| s.route.ordinal);
    unblock.release();
    let final_status = owner.shutdown();
    let report = actor.shutdown().unwrap();
    let (history, events, handoffs) = inspect(&path);
    // Retain actual discrepancy evidence before any failure, after releasing
    // and joining every worker. No false green from a destructor panic/hang.
    eprintln!(
        "closing_boundary_observed before={at_drive_return:?} after={after_late_revoke:?} final={final_status:?} snapshot_at_return={snapshot_at_return:?} snapshot_after_revoke={snapshot_after_revoke:?} revoked_records={} journal_records={} physical_records={}",
        events
            .iter()
            .filter(|e| matches!(e.event, OwnerEventDto::Revoked { .. }))
            .count(),
        events.len(),
        report.timings.accepted_records
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(handoffs.len(), 1);
    assert_eq!(history.native_ordinal(), 1);
    assert!(history.uncertainties().is_empty());
    assert!(
        !events
            .iter()
            .any(|e| matches!(e.event, OwnerEventDto::Revoked { .. }))
    );
    assert_eq!(at_drive_return.owner.phase, OwnerPhase::Stopped);
    assert!(at_drive_return.owner.failure.is_none());
    assert!(snapshot_at_return.is_none());
    assert_eq!(after_late_revoke.owner.phase, OwnerPhase::Stopped);
    assert!(after_late_revoke.owner.failure.is_none());
    assert!(snapshot_after_revoke.is_none());
    assert!(final_status.owner.failure.is_none());
}

#[test]
fn stopped_wire_http_rejection_preserves_stopped_state_without_unjournaled_revocation() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let dto = super::super::super::capture::projection_test_bootstraps()
        .into_iter()
        .next()
        .unwrap();
    let limits = OwnerHistoryLimits {
        max_retained_input_bytes: usize::try_from(
            dto.owner_policy.as_ref().unwrap().max_pending_bytes,
        )
        .unwrap(),
        max_input_bytes: usize::try_from(dto.owner_policy.as_ref().unwrap().max_input_bytes)
            .unwrap(),
        max_record_bytes: usize::try_from(dto.history_policy().unwrap().max_record_bytes).unwrap(),
    };
    // Same writer bounds as actual public capture; no transport/native callback
    // is invoked. This exercises the public MFCJ5 unsupported-HTTP branch.
    let actor = FeedBatchActor::spawn(
        FeedWriter::create_profile(&path, 64 << 20, FeedProfile::EngineCausalV5).unwrap(),
        BatchConfig {
            max_pending_operations: 16,
            max_pending_bytes: 32 << 20,
            max_group_operations: 8,
            max_group_bytes: 16 << 20,
            max_age: Duration::from_millis(1),
        },
    )
    .unwrap();
    let owner = PreparedBootstrap::new_wire_history(dto, MAX_OPERATION_BYTES)
        .unwrap()
        .try_admit(&actor.handle())
        .unwrap()
        .wait()
        .unwrap()
        .instantiate()
        .unwrap()
        .spawn_history_owner()
        .unwrap();
    wait(|| {
        owner.status().committed_records == 1 && owner.status().owner.phase == OwnerPhase::Idle
    });
    let ingress = owner.ingress();
    owner.close_ingress();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    let before = owner.status();
    let cursor_before = actor.handle().status().last_acknowledged;
    let input = CaptureInput::Http {
        request_id: 1,
        response: marketfeed_adapter_api::HttpResponse {
            status: 200,
            headers: vec![],
            body: bytes::Bytes::from_static(b"{}"),
        },
        received: FrameStamp {
            receive_ts: TimestampNs(2),
            mono_ns: 3,
        },
    };
    let rejected = ingress
        .try_submit(ConnectionId(0), input.clone())
        .unwrap_err();
    let after = owner.status();
    let queued_after = state(&ingress.shared).observations.len();
    let final_status = owner.shutdown();
    let report = actor.shutdown().unwrap();
    let mut reader = FeedReader::open_profile(&path, None, FeedProfile::EngineCausalV5).unwrap();
    let Some(FeedRecord::Causal(bootstrap)) = reader.read_next().unwrap() else {
        panic!()
    };
    let mut history = OwnerHistory::from_bootstrap(&bootstrap, limits).unwrap();
    let mut events = Vec::new();
    while let Some(record) = reader.read_next().unwrap() {
        let FeedRecord::Causal(record) = record else {
            panic!()
        };
        events.push(decode_owner_history(&record, history.identity()).unwrap());
        assert!(history.apply(&record).unwrap().is_none());
    }
    eprintln!(
        "stopped_wire_http_observed before={before:?} after={after:?} joined={final_status:?} queued_after={queued_after} recorded_owner_events={} published_sequence={}",
        events.len(),
        report.published.sequence
    );
    // Inspect/join first, then assert the old stopped-state mutation cannot be
    // hidden by worker cleanup or a correct UnsupportedInput return label.
    assert_eq!(rejected.reason, OwnerFailure::UnsupportedInput);
    assert_eq!(rejected.input, input);
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0].event, OwnerEventDto::Started { .. }));
    assert!(history.uncertainties().is_empty());
    assert_eq!(history.native_ordinal(), 0);
    assert_eq!(report.published, cursor_before);
    assert_eq!(before.owner.phase, OwnerPhase::Stopped);
    assert!(before.owner.failure.is_none());
    assert_eq!(after.owner.phase, OwnerPhase::Stopped);
    assert!(after.owner.failure.is_none());
    assert_eq!(queued_after, 0);
    assert!(final_status.owner.failure.is_none());
    assert_eq!(after.owner.accepted, 0);
}

#[path = "pipeline_tests.rs"]
mod pipeline_tests;
