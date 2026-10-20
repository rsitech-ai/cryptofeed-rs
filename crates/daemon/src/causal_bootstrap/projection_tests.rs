use super::*;
use std::time::{Duration, Instant};

fn limits() -> ProjectionLimits {
    ProjectionLimits {
        max_group_bytes: 1 << 20,
        max_items: 1024,
        max_owners: 8,
        max_bootstrap_bytes: 8 << 20,
        max_catalog_entries: 128,
        max_pending_inputs: 128,
    }
}
fn source_limits() -> MultiReplayLimits {
    MultiReplayLimits {
        per_owner: OwnerHistoryLimits {
            max_retained_input_bytes: 4 << 20,
            max_input_bytes: 256 << 10,
            max_record_bytes: 256 << 10,
        },
        maximum: ReplayResources {
            owners: 8,
            bootstrap_payload_bytes: 8 << 20,
            pending_inputs: 128,
            pending_input_bytes: 8 << 20,
            active_operation_and_state_bytes: 8 << 20,
            source_owned_pending_inputs: 128,
            source_owned_pending_bytes: 8 << 20,
            capture_trace_reservation_bytes: 16 << 20,
            catalog_entries: 128,
            selected_instruments: 128,
            maintained_book_levels: 20000,
            cached_book_levels: 81940,
        },
    }
}
fn batch() -> BatchConfig {
    BatchConfig {
        max_pending_operations: 1,
        max_pending_bytes: 2 << 20,
        max_group_operations: 1,
        max_group_bytes: 2 << 20,
        max_age: Duration::from_millis(30),
    }
}
fn source(path: &Path) -> MultiHistoryReplay {
    let mut dto = super::super::tests::fixture();
    let owner = dto.owner_policy.as_mut().unwrap();
    owner.max_input_bytes = 8192;
    owner.max_operation_bytes = 256 << 10;
    dto.bind_history_policy(HistoryRuntimePolicyDto {
        schema_version: 1,
        max_pending_observations: 32,
        max_pending_observation_bytes: 4 << 20,
        max_record_bytes: 256 << 10,
        max_finalization_age: Duration::from_secs(2).into(),
    })
    .unwrap();
    let bootstrap =
        encode_bootstrap_for_profile(&dto, MAX_OPERATION_BYTES, FeedProfile::EngineCausalV3)
            .unwrap();
    FeedWriter::create_profile(path, 64 << 20, FeedProfile::EngineCausalV3)
        .unwrap()
        .append(FeedRecord::Causal(bootstrap))
        .unwrap();
    MultiHistoryReplay::open(path, source_limits()).unwrap()
}
fn pump(temp: &tempfile::TempDir, max_bytes: u64) -> ProjectionPump {
    ProjectionPump::create(
        source(&temp.path().join("source")),
        ProjectionPolicy {
            schema_version: 1,
            source_profile: 3,
            clock_policy: ProjectionClockPolicy::PerOwnerStartedPhysicalOrderV1,
            limits: limits(),
        },
        temp.path().join("target"),
        max_bytes,
        batch(),
    )
    .unwrap()
}
fn wait(mut check: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(10);
    while !check() {
        assert!(Instant::now() < end, "projection unit watchdog");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn projection_full_retains_exact_prepared_allocation_and_ack_owns_source_progress() {
    let temp = tempfile::tempdir().unwrap();
    let mut pump = pump(&temp, 64 << 20);
    let before = pump.last_acknowledged().clone();
    assert_eq!(pump.poll().unwrap(), ProjectionPoll::Prepared);
    let pointer = |p: &ProjectionPump| {
        let FeedRecord::Projection(e) = &p.prepared.as_ref().unwrap().transaction.records()[0]
        else {
            panic!()
        };
        e.payload_base64.as_ptr() as usize
    };
    let original = pointer(&pump);
    assert_eq!(
        pump.poll_with(|transaction| Err(RejectedTransaction {
            reason: AdmissionFailure::Full,
            transaction
        }))
        .unwrap(),
        ProjectionPoll::Full
    );
    assert_eq!(pointer(&pump), original);
    assert_eq!(pump.prepared_groups(), 1);
    assert_eq!(pump.last_acknowledged(), &before);
    assert_eq!(pump.poll().unwrap(), ProjectionPoll::Writing);
    assert_eq!(pump.last_acknowledged(), &before);
    wait(|| pump.poll().unwrap() == ProjectionPoll::Committed);
    assert_eq!(pump.last_acknowledged().source.sequence, 1);
    assert_eq!(pump.last_acknowledged().target.sequence, 1);
    assert_eq!(pump.prepared_groups(), 1);
    assert_eq!(pump.poll().unwrap(), ProjectionPoll::CaughtUp);
    pump.shutdown().unwrap();
    let mut target = ProjectionReader::open(temp.path().join("target"), limits()).unwrap();
    let group = target.read_next().unwrap().unwrap();
    assert_eq!(group.source_before, before.source);
    assert_eq!(group.source_after.sequence, 1);
    assert!(matches!(group.source, ProjectionSource::Bootstrap(_)));
    assert!(group.items.is_empty());
}

#[test]
fn projection_accepted_cancellation_and_commit_before_bookkeeping_leave_whole_cursor_group() {
    for wait_for_commit in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let mut pump = pump(&temp, 64 << 20);
        let before = pump.last_acknowledged().clone();
        assert_eq!(pump.poll().unwrap(), ProjectionPoll::Prepared);
        assert_eq!(pump.poll().unwrap(), ProjectionPoll::Writing);
        if wait_for_commit {
            wait(|| pump.target.handle().status().committed_operations == 1);
        }
        assert_eq!(pump.last_acknowledged(), &before);
        drop(pump);
        let mut target = ProjectionReader::open(temp.path().join("target"), limits()).unwrap();
        let group = target.read_next().unwrap().unwrap();
        assert_eq!(group.source_before, before.source);
        assert_eq!(group.source_after.sequence, 1);
        assert!(target.read_next().unwrap().is_none());
        // Existing target directory never grants append takeover.
        let reopened =
            MultiHistoryReplay::open(temp.path().join("source"), source_limits()).unwrap();
        assert!(
            ProjectionPump::create(
                reopened,
                group.policy,
                temp.path().join("target"),
                64 << 20,
                batch()
            )
            .is_err()
        );
    }
}

#[test]
fn projection_actual_writer_failure_and_whole_group_overflow_cannot_advance_ack() {
    for overflow in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let mut pump = pump(&temp, if overflow { 64 << 20 } else { 48 });
        let before = pump.last_acknowledged().clone();
        if overflow {
            pump.policy.limits.max_group_bytes = 1;
            assert!(pump.poll().is_err());
        } else {
            assert_eq!(pump.poll().unwrap(), ProjectionPoll::Prepared);
            assert_eq!(pump.poll().unwrap(), ProjectionPoll::Writing);
            wait(|| match pump.poll() {
                Err(ProjectionError::Commit(_)) => true,
                Ok(ProjectionPoll::Writing) => false,
                other => panic!("expected actual writer failure {other:?}"),
            });
        }
        assert!(pump.is_poisoned());
        assert_eq!(pump.last_acknowledged(), &before);
        assert!(matches!(pump.poll(), Err(ProjectionError::Poisoned)));
        drop(pump);
        let mut target = ProjectionReader::open(temp.path().join("target"), limits()).unwrap();
        assert!(target.read_next().unwrap().is_none());
    }
}

#[test]
fn projection_pre_advanced_source_is_rejected_before_target_creation() {
    let temp = tempfile::tempdir().unwrap();
    let mut source = source(&temp.path().join("source"));
    source.read_next_verified().unwrap().unwrap();
    let path = temp.path().join("target");
    assert!(
        ProjectionPump::create(
            source,
            ProjectionPolicy {
                schema_version: 1,
                source_profile: 3,
                clock_policy: ProjectionClockPolicy::PerOwnerStartedPhysicalOrderV1,
                limits: limits()
            },
            &path,
            64 << 20,
            batch()
        )
        .is_err()
    );
    assert!(!path.exists());
}

#[test]
fn projection_invalid_actor_policy_is_rejected_before_target_creation() {
    for invalid in 0..3 {
        let temp = tempfile::tempdir().unwrap();
        let source = source(&temp.path().join("source"));
        let mut config = batch();
        match invalid {
            0 => config.max_pending_operations = 0,
            1 => config.max_group_operations = 2,
            _ => config.max_age = Duration::MAX,
        }
        let path = temp.path().join("target");
        assert!(
            ProjectionPump::create(
                source,
                ProjectionPolicy {
                    schema_version: 1,
                    source_profile: 3,
                    clock_policy: ProjectionClockPolicy::PerOwnerStartedPhysicalOrderV1,
                    limits: limits()
                },
                &path,
                64 << 20,
                config
            )
            .is_err()
        );
        assert!(!path.exists());
    }
}
