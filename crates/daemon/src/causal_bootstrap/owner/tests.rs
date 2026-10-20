use super::*;
use marketfeed_model::{FrameStamp, TimestampNs};
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(5);
fn fixture() -> BootstrapDto {
    let mut dto = super::super::tests::fixture();
    dto.owner_policy.as_mut().unwrap().max_operation_bytes = 128 * 1024;
    dto.owner_policy.as_mut().unwrap().max_ingress_age = Duration::from_secs(3).into();
    dto
}
fn actor(path: &std::path::Path, limit: u64, bytes: usize, age: Duration) -> FeedBatchActor {
    FeedBatchActor::spawn(
        FeedWriter::create_profile(path, limit, FeedProfile::EngineCausalV1).unwrap(),
        BatchConfig {
            max_pending_operations: 16,
            max_pending_bytes: bytes,
            max_group_operations: 16,
            max_group_bytes: bytes,
            max_age: age,
        },
    )
    .unwrap()
}
fn start(dto: BootstrapDto, actor: &FeedBatchActor) -> JournalOwner {
    boot(dto, actor).spawn_owner().unwrap()
}
fn boot(dto: BootstrapDto, actor: &FeedBatchActor) -> BootstrappedSession {
    PreparedBootstrap::new(dto, MAX_OPERATION_BYTES)
        .unwrap()
        .try_admit(&actor.handle())
        .unwrap()
        .wait()
        .unwrap()
        .instantiate()
        .unwrap()
}
fn wait(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !predicate() {
        assert!(Instant::now() < deadline, "owner test watchdog");
        thread::sleep(Duration::from_millis(1));
    }
}
fn release(owner: &JournalOwner) -> CommittedRelease {
    let mut value = None;
    wait(|| {
        value = owner.try_next();
        value.is_some()
    });
    value.unwrap()
}
fn connected(connection: u64) -> CaptureInput {
    CaptureInput::Connected {
        connection: ConnectionId(connection),
        now: TimestampNs(1),
    }
}
fn trade() -> CaptureInput {
    CaptureInput::Text {
        payload: br#"{"topic":"publicTrade.C01USDT","type":"snapshot","ts":1000,"data":[{"T":1000,"s":"C01USDT","S":"Buy","v":"0.001","p":"1.25","i":"owner-trade","BT":false}]}"#.to_vec(),
        received: FrameStamp { receive_ts: TimestampNs(1_000_000_000), mono_ns: 1_000_000_000 },
    }
}
fn records(path: &std::path::Path) -> Vec<FeedRecord> {
    let mut reader = FeedReader::open_profile(path, None, FeedProfile::EngineCausalV1).unwrap();
    let mut records = Vec::new();
    while let Some(record) = reader.read_next().unwrap() {
        records.push(record);
    }
    records
}

#[test]
fn delayed_ack_withholds_all_state_and_drains_and_release_slot_blocks_next_callback() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, 64 << 20, 8 << 20, Duration::from_millis(150));
    let owner = start(fixture(), &actor);
    let ingress = owner.ingress();
    ingress.try_submit(ConnectionId(1), connected(1)).unwrap();
    wait(|| owner.status().phase == OwnerPhase::AwaitingCommit);
    assert!(owner.try_next().is_none());
    assert!(owner.committed_snapshot().is_none());
    ingress.try_submit(ConnectionId(1), trade()).unwrap();
    ingress.try_submit(ConnectionId(1), trade()).unwrap();
    wait(|| owner.status().phase == OwnerPhase::ReleasePending);
    assert_eq!(owner.status().executed, 1);
    assert_eq!(owner.status().pending_operations, 2);
    let snapshot = owner.committed_snapshot().unwrap();
    assert_eq!(snapshot.route.ordinal, 1);
    let first = release(&owner);
    assert_eq!(first.input_id(), 1);
    assert_eq!(first.evidence().route.ordinal, 1);
    assert!(
        first
            .released_items()
            .any(|i| matches!(i, ItemDto::Write(_)))
    );
    let second = release(&owner);
    assert_eq!(second.input_id(), 2);
    assert_eq!(second.evidence().route.ordinal, 2);
    assert!(
        second
            .released_items()
            .any(|i| matches!(i, ItemDto::Market(_)))
    );
    let third = release(&owner);
    assert_eq!(third.input_id(), 3);
    assert_eq!(third.evidence().route.ordinal, 3);
    assert_eq!(owner.status().executed, 3);
    let status = owner.shutdown();
    assert_eq!(status.durable, 3);
    actor.shutdown().unwrap();
    assert_eq!(records(&path).len(), 4);
}

#[test]
fn real_actor_full_retries_one_executed_prepared_operation_only() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let mut filler = fixture();
    filler.capture.session += 1;
    filler.public_inputs.push(PublicReproductionInputDto::new(
        "large-public-input",
        &vec![0u8; 256 << 10],
    ));
    let filler = PreparedBootstrap::new(filler, MAX_OPERATION_BYTES).unwrap();
    let cap = filler.encoded_bytes_bound() + 1024;
    let actor = actor(&path, 64 << 20, cap, Duration::from_millis(200));
    let owner = start(fixture(), &actor);
    let filler = filler.try_admit(&actor.handle()).unwrap();
    owner
        .ingress()
        .try_submit(ConnectionId(1), connected(1))
        .unwrap();
    wait(|| owner.status().admission_retries > 0);
    assert_eq!(owner.status().executed, 1);
    assert!(owner.try_next().is_none());
    filler.wait().unwrap();
    let committed = release(&owner);
    assert_eq!(committed.evidence().route.ordinal, 1);
    assert_eq!(owner.status().executed, 1);
    owner.shutdown();
    actor.shutdown().unwrap();
    assert_eq!(
        records(&path)
            .iter()
            .filter(|r| matches!(r, FeedRecord::Causal(e) if e.phase==CausalPhase::Operation))
            .count(),
        1
    );
}

#[test]
fn admission_deadline_revokes_but_late_ack_settles_without_revival() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, 64 << 20, 8 << 20, Duration::from_millis(250));
    let mut dto = fixture();
    dto.owner_policy.as_mut().unwrap().max_ingress_age = Duration::from_millis(40).into();
    let owner = start(dto, &actor);
    owner
        .ingress()
        .try_submit(ConnectionId(1), connected(1))
        .unwrap();
    wait(|| owner.status().phase == OwnerPhase::AwaitingCommit);
    wait(|| owner.status().failure.is_some());
    assert_eq!(
        owner.status().failure,
        Some(OwnerFailure::Deadline(DeadlineStage::Admitted))
    );
    assert!(owner.try_next().is_none());
    assert!(owner.committed_snapshot().is_none());
    wait(|| owner.status().phase == OwnerPhase::Stopped);
    let status = owner.shutdown();
    assert_eq!(status.executed, 1);
    assert_eq!(status.durable, 1);
    assert_eq!(status.withheld, 1);
    assert_eq!(status.handed_off, 0);
    assert!(matches!(
        status.last_settlement,
        Some(CommitSettlement::Committed(_))
    ));
    actor.shutdown().unwrap();
    assert_eq!(records(&path).len(), 2);
}

#[test]
fn explicit_revocation_during_commit_cannot_release_and_settles_queued_suffix() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, 64 << 20, 8 << 20, Duration::from_millis(150));
    let owner = start(fixture(), &actor);
    let ingress = owner.ingress();
    ingress.try_submit(ConnectionId(1), connected(1)).unwrap();
    wait(|| owner.status().phase == OwnerPhase::AwaitingCommit);
    ingress.try_submit(ConnectionId(1), trade()).unwrap();
    ingress.revoke();
    assert!(owner.committed_snapshot().is_none());
    assert!(owner.try_next().is_none());
    let status = owner.shutdown();
    assert_eq!(status.failure, Some(OwnerFailure::Revoked));
    assert_eq!(
        (
            status.accepted,
            status.executed,
            status.durable,
            status.abandoned
        ),
        (2, 1, 1, 1)
    );
    assert_eq!(status.pending_bytes, 0);
    assert_eq!(status.withheld, 1);
    actor.shutdown().unwrap();
    assert_eq!(records(&path).len(), 2);
}

#[test]
fn stale_source_frame_after_reconnect_is_rejected_without_restamping() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, 64 << 20, 8 << 20, Duration::from_millis(2));
    let owner = start(fixture(), &actor);
    let ingress = owner.ingress();
    ingress.try_submit(ConnectionId(1), connected(1)).unwrap();
    release(&owner);
    ingress.try_submit(ConnectionId(2), connected(2)).unwrap();
    let rejected = ingress.try_submit(ConnectionId(1), trade()).unwrap_err();
    assert_eq!(rejected.reason, OwnerFailure::SourceConnection);
    let reconnect = release(&owner);
    assert_eq!(reconnect.evidence().route.generation, 2);
    assert!(owner.status().failure.is_none());
    assert_eq!(owner.status().rejected_stale, 1);
    let status = owner.shutdown();
    assert_eq!(
        (status.executed, status.durable, status.abandoned),
        (2, 2, 0)
    );
    actor.shutdown().unwrap();
    assert_eq!(records(&path).len(), 3);
}

#[test]
fn complete_native_turn_failure_is_journaled_before_terminal_withholding() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, 64 << 20, 8 << 20, Duration::from_millis(20));
    let mut config = fixture();
    config.capture.dispatch_capacity = 1;
    config.runner_policy = config.capture.config().unwrap().runtime_policy();
    let identity_envelope = encode_bootstrap(&config, MAX_OPERATION_BYTES).unwrap();
    let identity = OperationIdentity {
        venue: identity_envelope.route.venue,
        session: identity_envelope.route.session,
        bootstrap_id: identity_envelope.route.bootstrap_id,
    };
    let owner = start(config, &actor);
    let admitted = owner
        .ingress()
        .try_submit(ConnectionId(1), connected(1))
        .unwrap();
    wait(|| owner.status().phase == OwnerPhase::Stopped);
    assert!(matches!(admitted.wait(), InputSettlement::Committed { .. }));
    assert!(owner.try_next().is_none());
    assert!(owner.committed_snapshot().is_none());
    let status = owner.shutdown();
    assert!(
        matches!(status.failure, Some(OwnerFailure::Native(_))),
        "{status:?}"
    );
    assert_eq!(
        (status.executed, status.durable, status.withheld),
        (1, 1, 1)
    );
    actor.shutdown().unwrap();
    let saved = records(&path);
    assert_eq!(saved.len(), 2);
    let FeedRecord::Causal(failed) = &saved[1] else {
        panic!()
    };
    let dto = decode_operation(failed, &identity).unwrap();
    assert!(dto.outcome.is_err());
    assert!(
        !dto.entries
            .iter()
            .any(|e| matches!(e.effect, EffectDto::QueueDrain { .. }))
    );
}

#[test]
fn post_execution_encoding_overflow_and_incomplete_capture_are_terminal_not_retryable() {
    for incomplete in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("feed");
        let actor = actor(&path, 64 << 20, 8 << 20, Duration::from_millis(2));
        let mut dto = fixture();
        if incomplete {
            dto.capture.max_entries = 1;
        } else {
            dto.owner_policy.as_mut().unwrap().max_operation_bytes = 128;
        }
        let owner = start(dto, &actor);
        owner
            .ingress()
            .try_submit(ConnectionId(1), connected(1))
            .unwrap();
        wait(|| owner.status().phase == OwnerPhase::Stopped);
        let status = owner.shutdown();
        assert!(if incomplete {
            matches!(status.failure, Some(OwnerFailure::Incomplete(_)))
        } else {
            matches!(status.failure, Some(OwnerFailure::Encoding(_)))
        });
        assert_eq!(
            (status.executed, status.durable, status.handed_off),
            (1, 0, 0)
        );
        actor.shutdown().unwrap();
        assert_eq!(records(&path).len(), 1);
    }
}

#[test]
fn actual_publication_failure_retains_attempted_disposition_and_releases_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let dto = fixture();
    let limit = PreparedBootstrap::new(dto.clone(), MAX_OPERATION_BYTES)
        .unwrap()
        .encoded_bytes_bound() as u64
        + 128;
    let actor = actor(&path, limit, 8 << 20, Duration::from_millis(2));
    let owner = start(dto, &actor);
    owner
        .ingress()
        .try_submit(ConnectionId(1), connected(1))
        .unwrap();
    wait(|| owner.status().phase == OwnerPhase::Stopped);
    assert!(owner.try_next().is_none());
    let status = owner.shutdown();
    let Some(CommitSettlement::Failed(failure)) = status.last_settlement else {
        panic!("{status:?}")
    };
    assert!(matches!(
        failure.disposition,
        CommitDisposition::Attempted { .. }
    ));
    assert_eq!(status.durable, 0);
    assert!(actor.shutdown().is_err());
    assert_eq!(records(&path).len(), 1);
}

#[test]
fn input_capacity_limits_and_admin_or_sink_bypasses_fail_before_execution() {
    for input in [
        CaptureInput::Live {
            now: TimestampNs(1),
        },
        CaptureInput::ForwardDispatch { results: vec![] },
        CaptureInput::Text {
            payload: Vec::with_capacity(2 << 20),
            received: FrameStamp {
                receive_ts: TimestampNs(1),
                mono_ns: 1,
            },
        },
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("feed");
        let actor = actor(&path, 64 << 20, 8 << 20, Duration::from_millis(2));
        let owner = start(fixture(), &actor);
        let rejected = owner
            .ingress()
            .try_submit(ConnectionId(1), input)
            .unwrap_err();
        assert!(matches!(
            rejected.reason,
            OwnerFailure::UnsupportedInput | OwnerFailure::InputTooLarge
        ));
        let status = owner.shutdown();
        assert_eq!(status.executed, 0);
        assert_eq!(status.accepted, 0);
        actor.shutdown().unwrap();
        assert_eq!(records(&path).len(), 1);
    }
}

#[test]
fn dropping_owner_reaps_accepted_commit_without_waiter_cancellation() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, 64 << 20, 8 << 20, Duration::from_millis(100));
    let owner = start(fixture(), &actor);
    let ingress = owner.ingress();
    ingress.try_submit(ConnectionId(1), connected(1)).unwrap();
    wait(|| ingress.status().phase == OwnerPhase::AwaitingCommit);
    drop(owner);
    assert_eq!(ingress.status().phase, OwnerPhase::Stopped);
    assert_eq!(ingress.status().durable, 1);
    assert_eq!(ingress.status().withheld, 1);
    actor.shutdown().unwrap();
    assert_eq!(records(&path).len(), 2);
}

#[test]
fn legacy_v1_is_canonical_inspectable_but_cannot_supply_silent_owner_defaults() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, 64 << 20, 8 << 20, Duration::from_millis(2));
    let mut dto = fixture();
    dto.schema_version = 1;
    dto.owner_policy = None;
    let envelope = encode_bootstrap(&dto, MAX_OPERATION_BYTES).unwrap();
    assert!(
        !String::from_utf8(envelope.payload().unwrap())
            .unwrap()
            .contains("owner_policy")
    );
    assert_eq!(decode_bootstrap(&envelope).unwrap(), dto);
    assert!(matches!(
        boot(dto.clone(), &actor).spawn_owner(),
        Err(OwnerStartError::MissingPolicy)
    ));
    dto.owner_policy = Some(JournalOwnerPolicyDto::default());
    assert!(dto.validate().is_err());
    dto.schema_version = 2;
    dto.owner_policy = None;
    assert!(dto.validate().is_err());
    actor.shutdown().unwrap();
}

#[test]
fn known_incompatible_actor_bounds_are_rejected_before_first_callback() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let dto = fixture();
    let bytes = PreparedBootstrap::new(dto.clone(), MAX_OPERATION_BYTES)
        .unwrap()
        .encoded_bytes_bound()
        + 256;
    let actor = actor(&path, 64 << 20, bytes, Duration::from_millis(2));
    assert!(matches!(
        boot(dto, &actor).spawn_owner(),
        Err(OwnerStartError::IncompatibleBounds)
    ));
    actor.shutdown().unwrap();
    assert_eq!(records(&path).len(), 1);
}

#[test]
fn observed_safety_events_fence_old_ack_before_the_transition_itself_commits() {
    for input in [
        CaptureInput::Close {
            payload: vec![],
            received: FrameStamp {
                receive_ts: TimestampNs(2),
                mono_ns: 2,
            },
        },
        CaptureInput::TransportLost {
            reason: marketfeed_adapter_api::DisconnectReason::TransportError,
            now: TimestampNs(2),
        },
        CaptureInput::Disconnected {
            reason: marketfeed_adapter_api::DisconnectReason::RemoteClose,
            now: TimestampNs(2),
        },
        CaptureInput::Stop,
    ] {
        let stopping = matches!(input, CaptureInput::Stop);
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("feed");
        let actor = actor(&path, 64 << 20, 8 << 20, Duration::from_millis(150));
        let owner = start(fixture(), &actor);
        let ingress = owner.ingress();
        let first = ingress.try_submit(ConnectionId(1), connected(1)).unwrap();
        wait(|| owner.status().phase == OwnerPhase::AwaitingCommit);
        let transition = ingress.try_submit(ConnectionId(1), input).unwrap();
        assert!(owner.committed_snapshot().is_none());
        assert!(owner.try_next().is_none());
        assert!(matches!(
            first.wait(),
            InputSettlement::Committed { id: 1, .. }
        ));
        assert!(matches!(
            transition.wait(),
            InputSettlement::Committed { id: 2, .. }
        ));
        if stopping {
            wait(|| owner.status().phase == OwnerPhase::Stopped);
            assert!(owner.try_next().is_none());
        } else {
            let transition = release(&owner);
            assert_eq!(transition.input_id(), 2);
            assert!(!transition.outputs_eligible());
            assert_eq!(transition.released_items().count(), 0);
        }
        assert!(owner.committed_snapshot().is_none());
        let status = owner.shutdown();
        assert_eq!((status.executed, status.durable), (2, 2));
        assert!(status.withheld >= 1);
        actor.shutdown().unwrap();
        assert_eq!(records(&path).len(), 3);
    }
}

#[test]
fn closed_source_rejects_late_text_and_http_until_a_new_physical_connection() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, 64 << 20, 8 << 20, Duration::from_millis(5));
    let owner = start(fixture(), &actor);
    let ingress = owner.ingress();
    ingress.try_submit(ConnectionId(1), connected(1)).unwrap();
    release(&owner);
    ingress
        .try_submit(
            ConnectionId(1),
            CaptureInput::Close {
                payload: vec![],
                received: FrameStamp {
                    receive_ts: TimestampNs(2),
                    mono_ns: 2,
                },
            },
        )
        .unwrap();
    let closed = release(&owner);
    assert!(!closed.outputs_eligible());
    let text = ingress.try_submit(ConnectionId(1), trade()).unwrap_err();
    assert_eq!(text.reason, OwnerFailure::SourceConnection);
    let http = ingress
        .try_submit(
            ConnectionId(1),
            CaptureInput::Http {
                request_id: 1,
                response: marketfeed_adapter_api::HttpResponse {
                    status: 200,
                    headers: vec![],
                    body: b"{}".to_vec().into(),
                },
                received: FrameStamp {
                    receive_ts: TimestampNs(3),
                    mono_ns: 3,
                },
            },
        )
        .unwrap_err();
    assert_eq!(http.reason, OwnerFailure::SourceConnection);
    assert_eq!(owner.status().executed, 2);
    ingress.try_submit(ConnectionId(2), connected(2)).unwrap();
    let reconnect = release(&owner);
    assert!(reconnect.outputs_eligible());
    assert_eq!(reconnect.evidence().route.ordinal, 3);
    assert_eq!(reconnect.evidence().route.generation, 2);
    let status = owner.shutdown();
    assert_eq!(status.rejected_stale, 2);
    actor.shutdown().unwrap();
    assert_eq!(records(&path).len(), 4);
}

#[test]
fn native_reconnect_discovery_immediately_fences_and_settles_stale_accepted_callbacks() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, 64 << 20, 8 << 20, Duration::from_millis(80));
    let owner = start(fixture(), &actor);
    let ingress = owner.ingress();
    ingress.try_submit(ConnectionId(1), connected(1)).unwrap();
    release(&owner);
    let bad = ingress
        .try_submit(
            ConnectionId(1),
            CaptureInput::Text {
                payload: b"{".to_vec(),
                received: FrameStamp {
                    receive_ts: TimestampNs(2),
                    mono_ns: 2,
                },
            },
        )
        .unwrap();
    let stale = ingress.try_submit(ConnectionId(1), trade());
    wait(|| owner.status().phase == OwnerPhase::AwaitingCommit);
    assert!(owner.committed_snapshot().is_none());
    assert!(matches!(bad.wait(), InputSettlement::Committed { .. }));
    let evidence = release(&owner);
    assert!(evidence.evidence().outcome.is_ok());
    assert!(evidence.evidence().after.reconnect_requested);
    assert!(!evidence.outputs_eligible());
    assert_eq!(evidence.released_items().count(), 0);
    match stale {
        Ok(admitted) => assert!(matches!(
            admitted.wait(),
            InputSettlement::NotExecuted {
                reason: OwnerFailure::SourceConnection,
                ..
            }
        )),
        Err(rejected) => assert_eq!(rejected.reason, OwnerFailure::SourceConnection),
    }
    assert_eq!(owner.status().executed, 2);
    ingress.try_submit(ConnectionId(2), connected(2)).unwrap();
    assert_eq!(release(&owner).evidence().route.ordinal, 3);
    owner.shutdown();
    actor.shutdown().unwrap();
    assert_eq!(records(&path).len(), 4);
}

#[test]
fn bounded_ingress_includes_inflight_and_overflow_never_executes_the_rejected_input() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, 64 << 20, 8 << 20, Duration::from_millis(100));
    let mut dto = fixture();
    dto.owner_policy.as_mut().unwrap().max_pending_operations = 1;
    let owner = start(dto, &actor);
    let ingress = owner.ingress();
    let first = ingress.try_submit(ConnectionId(1), connected(1)).unwrap();
    wait(|| owner.status().phase == OwnerPhase::AwaitingCommit);
    let rejected = ingress.try_submit(ConnectionId(1), trade()).unwrap_err();
    assert_eq!(rejected.reason, OwnerFailure::IngressFull);
    assert!(matches!(first.wait(), InputSettlement::Committed { .. }));
    let status = owner.shutdown();
    assert_eq!(
        (
            status.accepted,
            status.executed,
            status.durable,
            status.withheld
        ),
        (1, 1, 1, 1)
    );
    actor.shutdown().unwrap();
    assert_eq!(records(&path).len(), 2);
}

#[test]
fn full_admission_retries_share_one_absolute_ingress_deadline() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let mut filler = fixture();
    filler.capture.session += 1;
    filler.public_inputs.push(PublicReproductionInputDto::new(
        "large-public-input",
        &vec![0u8; 256 << 10],
    ));
    let filler = PreparedBootstrap::new(filler, MAX_OPERATION_BYTES).unwrap();
    let actor = actor(
        &path,
        64 << 20,
        filler.encoded_bytes_bound() + 1024,
        Duration::from_millis(600),
    );
    let mut config = fixture();
    config.owner_policy.as_mut().unwrap().max_ingress_age = Duration::from_millis(150).into();
    let owner = start(config, &actor);
    let filler = filler.try_admit(&actor.handle()).unwrap();
    let accepted = owner
        .ingress()
        .try_submit(ConnectionId(1), connected(1))
        .unwrap();
    wait(|| owner.status().admission_retries > 0);
    let settled = accepted.wait();
    assert!(
        matches!(
            settled,
            InputSettlement::ExecutedNotAdmitted {
                reason: OwnerFailure::Deadline(DeadlineStage::Executed),
                ..
            }
        ),
        "{settled:?}"
    );
    let status = owner.shutdown();
    assert_eq!((status.executed, status.durable), (1, 0));
    assert!(status.admission_retries > 0);
    filler.wait().unwrap();
    actor.shutdown().unwrap();
    assert_eq!(records(&path).len(), 2);
}

#[test]
fn capture_preflight_rejection_settles_never_executed_without_second_ordinal() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, 64 << 20, 8 << 20, Duration::from_millis(5));
    let mut dto = fixture();
    dto.capture.max_bytes = 1 << 20;
    let owner = start(dto, &actor);
    let ingress = owner.ingress();
    ingress.try_submit(ConnectionId(1), connected(1)).unwrap();
    release(&owner);
    let admitted = ingress
        .try_submit(
            ConnectionId(1),
            CaptureInput::Text {
                payload: vec![b' '; 600 << 10],
                received: FrameStamp {
                    receive_ts: TimestampNs(2),
                    mono_ns: 2,
                },
            },
        )
        .unwrap();
    let result = admitted.wait();
    assert!(
        matches!(
            result,
            InputSettlement::NotExecuted {
                reason: OwnerFailure::Execution(_),
                ..
            }
        ),
        "{result:?}"
    );
    let status = owner.shutdown();
    assert_eq!(
        (
            status.accepted,
            status.executed,
            status.durable,
            status.abandoned
        ),
        (2, 1, 1, 1)
    );
    actor.shutdown().unwrap();
    assert_eq!(records(&path).len(), 2);
}

#[test]
fn real_resync_then_new_connection_clears_prior_reconnect_and_releases_new_subscription() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, 64 << 20, 8 << 20, Duration::from_millis(5));
    let owner = start(fixture(), &actor);
    let ingress = owner.ingress();
    ingress.try_submit(ConnectionId(1), connected(1)).unwrap();
    release(&owner);
    ingress
        .try_submit(
            ConnectionId(1),
            CaptureInput::Control {
                command: SessionCommand::Resync(marketfeed_model::InstrumentId(1)),
                now: TimestampNs(2),
            },
        )
        .unwrap();
    let resync = release(&owner);
    assert!(resync.evidence().after.reconnect_requested);
    assert!(!resync.outputs_eligible());
    ingress.try_submit(ConnectionId(2), connected(2)).unwrap();
    ingress.try_submit(ConnectionId(2), trade()).unwrap();
    let reconnect = release(&owner);
    assert!(!reconnect.evidence().after.reconnect_requested);
    assert_eq!(reconnect.evidence().route.generation, 2);
    assert!(
        reconnect
            .released_items()
            .any(|i| matches!(i, ItemDto::Write(_)))
    );
    assert!(
        release(&owner)
            .released_items()
            .any(|i| matches!(i, ItemDto::Market(_)))
    );
    owner.shutdown();
    actor.shutdown().unwrap();
    assert_eq!(records(&path).len(), 5);
}

#[test]
fn actual_timer_poll_is_one_committed_turn_and_preserves_firing_order() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, 64 << 20, 8 << 20, Duration::from_millis(5));
    let owner = start(fixture(), &actor);
    let ingress = owner.ingress();
    ingress.try_submit(ConnectionId(1), connected(1)).unwrap();
    let connected = release(&owner);
    assert_eq!(connected.evidence().after.timers.len(), 2);
    ingress
        .try_submit(
            ConnectionId(1),
            CaptureInput::TimerPoll {
                now: TimestampNs(20_000_000_002),
            },
        )
        .unwrap();
    let timer = release(&owner);
    assert_eq!(timer.evidence().route.ordinal, 2);
    assert!(matches!(
        timer.evidence().input,
        InputDto::TimerPoll {
            now: 20_000_000_002
        }
    ));
    let fired: Vec<_> = timer
        .evidence()
        .entries
        .iter()
        .filter_map(|entry| match &entry.effect {
            EffectDto::Invocation {
                kind: InvocationDto::Timer { timer_id, now },
                ..
            } => Some((*timer_id, *now)),
            _ => None,
        })
        .collect();
    assert_eq!(fired, vec![(1, 20_000_000_002)]);
    assert!(timer.evidence().after.reconnect_requested);
    assert!(!timer.outputs_eligible());
    owner.shutdown();
    actor.shutdown().unwrap();
    assert_eq!(records(&path).len(), 3);
}

#[test]
fn overdue_accepted_queue_entry_settles_never_executed_before_machine_entry() {
    // Inject the state a descheduled owner can observe: accepted ingress whose
    // absolute deadline passed before it could run. The native factory/writer
    // are real; only scheduler timing is made deterministic at this private seam.
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, 64 << 20, 8 << 20, Duration::from_millis(5));
    let owner = start(fixture(), &actor);
    let shared = &owner.ingress.shared;
    let input = connected(1);
    let bytes = input_bytes(&input);
    let (sender, receiver) = mpsc::channel();
    {
        let mut state = lock(shared);
        assert_eq!(state.status.phase, OwnerPhase::Idle);
        state.status.accepted = 1;
        state.status.pending_operations = 1;
        state.status.pending_bytes = bytes;
        state.source_connection = ConnectionId(1);
        state.source_active = true;
        fence(&mut state, 1);
        state.queue.push_back(QueuedInput {
            id: 1,
            connection: ConnectionId(1),
            input,
            bytes,
            deadline: Instant::now() - Duration::from_millis(1),
            settlement: sender,
        });
        shared.wake.notify_all();
    }
    let result = receiver.recv_timeout(WAIT).unwrap();
    assert!(matches!(
        result,
        InputSettlement::NotExecuted {
            reason: OwnerFailure::Deadline(DeadlineStage::Queued),
            ..
        }
    ));
    let status = owner.shutdown();
    assert_eq!(
        (status.executed, status.durable, status.abandoned),
        (0, 0, 1)
    );
    actor.shutdown().unwrap();
    assert_eq!(records(&path).len(), 1);
}

#[test]
fn original_writer_closure_poison_is_observed_while_idle_without_another_input() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, 64 << 20, 8 << 20, Duration::from_millis(5));
    let owner = start(fixture(), &actor);
    let ingress = owner.ingress();
    ingress.try_submit(ConnectionId(1), connected(1)).unwrap();
    release(&owner);
    wait(|| owner.status().phase == OwnerPhase::Idle);
    actor.handle().close();
    actor.shutdown().unwrap();
    // Inspect the raw phase: calling status() would itself observe closure and
    // would not prove the idle worker independently woke without new ingress.
    wait(|| lock(&ingress.shared).status.phase == OwnerPhase::Stopped);
    assert_eq!(
        owner.status().failure,
        Some(OwnerFailure::Admission(AdmissionFailure::Closed))
    );
    assert!(owner.committed_snapshot().is_none());
    assert!(owner.try_next().is_none());
    let rejected = ingress.try_submit(ConnectionId(1), trade()).unwrap_err();
    assert_eq!(
        rejected.reason,
        OwnerFailure::Admission(AdmissionFailure::Closed)
    );
    let status = owner.shutdown();
    assert_eq!(
        (status.accepted, status.executed, status.durable),
        (1, 1, 1)
    );
    assert_eq!(records(&path).len(), 2);
}

#[test]
fn closure_revokes_visibility_but_reaps_the_already_accepted_ticket() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, 64 << 20, 8 << 20, Duration::from_millis(250));
    let owner = start(fixture(), &actor);
    let ingress = owner.ingress();
    let accepted = ingress.try_submit(ConnectionId(1), connected(1)).unwrap();
    wait(|| owner.status().phase == OwnerPhase::AwaitingCommit);
    actor.handle().close();
    assert!(owner.committed_snapshot().is_none());
    assert!(owner.try_next().is_none());
    assert!(matches!(accepted.wait(), InputSettlement::Committed { .. }));
    wait(|| owner.status().phase == OwnerPhase::Stopped);
    let status = owner.shutdown();
    assert_eq!(
        status.failure,
        Some(OwnerFailure::Admission(AdmissionFailure::Closed))
    );
    assert_eq!(
        (status.durable, status.withheld, status.handed_off),
        (1, 1, 0)
    );
    assert!(matches!(
        status.last_settlement,
        Some(CommitSettlement::Committed(_))
    ));
    actor.shutdown().unwrap();
    assert_eq!(records(&path).len(), 2);
}

#[test]
fn external_writer_failure_poison_is_observed_idle_with_typed_failure() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, 256 << 10, 8 << 20, Duration::from_millis(5));
    let owner = start(fixture(), &actor);
    let ingress = owner.ingress();
    ingress.try_submit(ConnectionId(1), connected(1)).unwrap();
    release(&owner);
    wait(|| owner.status().phase == OwnerPhase::Idle);
    let mut other = fixture();
    other.capture.session += 1;
    other.public_inputs.push(PublicReproductionInputDto::new(
        "oversized-for-disk",
        &vec![0u8; 256 << 10],
    ));
    let failed = PreparedBootstrap::new(other, MAX_OPERATION_BYTES)
        .unwrap()
        .try_admit(&actor.handle())
        .unwrap()
        .wait();
    let Err(BootstrapError::Commit(failure)) = failed else {
        panic!("expected real disk publication failure")
    };
    wait(|| lock(&ingress.shared).status.phase == OwnerPhase::Stopped);
    assert_eq!(
        owner.status().failure,
        Some(OwnerFailure::Admission(AdmissionFailure::Failed(
            failure.failure
        )))
    );
    assert!(owner.committed_snapshot().is_none());
    assert!(owner.try_next().is_none());
    let status = owner.shutdown();
    assert_eq!(
        (status.accepted, status.executed, status.durable),
        (1, 1, 1)
    );
    assert!(actor.shutdown().is_err());
    assert_eq!(records(&path).len(), 2);
}
