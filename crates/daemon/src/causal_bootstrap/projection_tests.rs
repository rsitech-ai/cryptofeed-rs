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

fn stop_source(path: &Path, sealed: bool) -> BatchConfig {
    let config = BatchConfig {
        max_pending_operations: 16,
        max_pending_bytes: 32 << 20,
        max_group_operations: 8,
        max_group_bytes: 16 << 20,
        max_age: Duration::from_millis(1),
    };
    let actor = FeedBatchActor::spawn(
        FeedWriter::create_profile(path, 64 << 20, FeedProfile::EngineCausalV5).unwrap(),
        config,
    )
    .unwrap();
    for dto in super::super::capture::projection_test_bootstraps() {
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
        assert!(matches!(
            owner
                .ingress()
                .try_submit(
                    marketfeed_model::ConnectionId(0),
                    marketfeed_engine::CaptureInput::Stop,
                )
                .unwrap()
                .wait(),
            super::super::owner::InputSettlement::Committed { .. }
        ));
        owner.close_ingress();
        wait(|| owner.status().owner.phase == super::super::owner::OwnerPhase::Stopped);
        assert!(owner.wire_progress().unwrap().2);
        owner.shutdown();
    }
    if sealed {
        actor
            .handle()
            .try_submit(
                PreparedFeedTransaction::new(vec![FeedRecord::Seal {
                    schema_version: 1,
                    now_ns: i64::MAX,
                    reason: "closed offline native Stop correctness fixture".into(),
                }])
                .unwrap(),
            )
            .unwrap()
            .wait()
            .unwrap();
    }
    actor.shutdown().unwrap();
    config
}
fn capture_policy() -> ProjectionPolicy {
    ProjectionPolicy {
        schema_version: 1,
        source_profile: 5,
        clock_policy: ProjectionClockPolicy::PerOwnerStartedPhysicalOrderV1,
        limits: super::super::capture::projection_limits(),
    }
}
fn capture_source(path: &Path) -> MultiHistoryReplay {
    MultiHistoryReplay::open_profile(
        path,
        super::super::capture::replay_limits(),
        FeedProfile::EngineCausalV5,
    )
    .unwrap()
}
fn capture_pump(path: &Path, target: &Path, config: BatchConfig, groups: usize) -> ProjectionPump {
    ProjectionPump::create_batched(
        capture_source(path),
        capture_policy(),
        target,
        64 << 20,
        config,
        groups,
    )
    .unwrap()
}

#[test]
fn projection_multi_record_receipt_advances_exact_contiguous_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("source");
    let config = stop_source(&path, false);
    let mut pump = capture_pump(&path, &temp.path().join("target"), config, 8);
    let before = pump.last_acknowledged().clone();
    let mut records = Vec::new();
    let mut after = before.source.clone();
    for _ in 0..8 {
        let step = pump.source.read_next_verified().unwrap().unwrap();
        assert_eq!(step.before_cursor(), &after);
        after = step.after_cursor().clone();
        records.push(FeedRecord::Projection(
            group(&step, &pump.policy).unwrap().encode().unwrap(),
        ));
    }
    pump.prepared = Some(Prepared {
        after: after.clone(),
        sealed: false,
        transaction: PreparedFeedTransaction::new(records).unwrap(),
    });
    pump.prepared_groups = 8;
    assert_eq!(pump.poll().unwrap(), ProjectionPoll::Writing);
    assert_eq!(pump.last_acknowledged(), &before);
    assert!(pump.last_publication().is_none());
    wait(|| pump.poll().unwrap() == ProjectionPoll::Committed);
    assert_eq!(pump.last_acknowledged().source, after);
    assert_eq!(pump.last_acknowledged().target.sequence, 8);
    let receipt = pump.last_publication().unwrap();
    assert_eq!(receipt.groups, 8);
    assert_eq!(receipt.source_before, before.source);
    assert_eq!(receipt.source_after, after);
    assert_eq!(receipt.target_before, before.target);
    assert_eq!(receipt.target_after, pump.last_acknowledged().target);
    let report = pump.shutdown().unwrap();
    assert_eq!(report.committed_operations, 1);
    assert_eq!(report.groups, 1);
    let mut reader = ProjectionReader::open(
        temp.path().join("target"),
        super::super::capture::projection_limits(),
    )
    .unwrap();
    for sequence in 1..=8 {
        let group = reader.read_next().unwrap().unwrap();
        assert_eq!(group.source_after.sequence, sequence);
        assert_eq!(reader.progress().unwrap().target_cursor.sequence, sequence);
    }
    assert!(reader.read_next().unwrap().is_none());
}

#[test]
fn projection_batch_full_keeps_all_allocations_and_source_frontier_until_ack() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("source");
    let config = stop_source(&path, false);
    let mut pump = capture_pump(&path, &temp.path().join("target"), config, 8);
    let before = pump.last_acknowledged().clone();
    // A deterministic preparation clock isolates count from filesystem speed;
    // the actual actor still retains the production1ms grouping deadline.
    assert_eq!(
        pump.poll_with_preparation_age(|_| panic!("prepare must not admit"), || Duration::ZERO)
            .unwrap(),
        ProjectionPoll::Prepared
    );
    let allocation = |p: &ProjectionPump| {
        p.prepared
            .as_ref()
            .unwrap()
            .transaction
            .records()
            .iter()
            .map(|r| {
                let FeedRecord::Projection(e) = r else {
                    panic!()
                };
                e.payload_base64.as_ptr() as usize
            })
            .collect::<Vec<_>>()
    };
    let pointers = allocation(&pump);
    let source_after = pump.source.progress().unwrap().cursor;
    let count = pointers.len();
    assert_eq!(count, 8);
    for _ in 0..3 {
        assert_eq!(
            pump.poll_with(|transaction| Err(RejectedTransaction {
                reason: AdmissionFailure::Full,
                transaction
            }))
            .unwrap(),
            ProjectionPoll::Full
        );
        assert_eq!(allocation(&pump), pointers);
        assert_eq!(pump.source.progress().unwrap().cursor, source_after);
        assert_eq!(pump.prepared_groups(), count as u64);
        assert_eq!(pump.last_acknowledged(), &before);
    }
    assert_eq!(pump.poll().unwrap(), ProjectionPoll::Writing);
    assert_eq!(pump.last_acknowledged(), &before);
    wait(|| pump.poll().unwrap() == ProjectionPoll::Committed);
    assert_eq!(pump.last_acknowledged().source, source_after);
    assert_eq!(pump.last_acknowledged().target.sequence, count as u64);
    pump.shutdown().unwrap();
}

#[test]
fn projection_count_byte_age_and_caught_up_flush_without_held_lookahead() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("source");
    let config = stop_source(&path, false);
    for (index, mode) in ["count", "bytes", "age"].into_iter().enumerate() {
        let mut config = config;
        if mode == "bytes" {
            config.max_group_bytes = capture_policy().limits.max_group_bytes.div_ceil(3) * 4 + 4096;
        }
        let count = if mode == "count" { 2 } else { 8 };
        let mut pump = capture_pump(
            &path,
            &temp.path().join(format!("target-{index}")),
            config,
            count,
        );
        let age = if mode == "age" {
            Duration::MAX
        } else {
            Duration::ZERO
        };
        assert_eq!(
            pump.poll_with_preparation_age(|_| panic!("prepare must not admit"), || age)
                .unwrap(),
            ProjectionPoll::Prepared
        );
        let prepared = pump.prepared.as_ref().unwrap().transaction.record_count();
        if mode == "count" {
            assert_eq!(prepared, 2);
        }
        if mode == "bytes" || mode == "age" {
            assert_eq!(prepared, 1);
        }
        assert_eq!(
            pump.source.progress().unwrap().cursor.sequence,
            prepared as u64
        );
        wait(|| {
            let poll = pump.poll().unwrap();
            if poll == ProjectionPoll::Prepared {
                let n = pump.prepared.as_ref().unwrap().transaction.record_count();
                assert!(n <= count);
                assert_eq!(
                    pump.source.progress().unwrap().cursor.sequence,
                    pump.last_acknowledged().source.sequence + n as u64
                );
            }
            poll == ProjectionPoll::CaughtUp
        });
        assert_eq!(
            pump.last_acknowledged().source,
            pump.source.progress().unwrap().cursor
        );
        pump.shutdown().unwrap();
    }
}

#[test]
fn projection_multi_record_unobserved_ack_and_pre_watermark_failure_rebuild_exact_prefix() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let config = stop_source(&source, true);
    // A prepared but unaccepted cancellation never publishes or invents ACK.
    let mut prepared = capture_pump(&source, &temp.path().join("unaccepted"), config, 8);
    assert_eq!(prepared.poll().unwrap(), ProjectionPoll::Prepared);
    drop(prepared);
    assert!(
        ProjectionReader::open(
            temp.path().join("unaccepted"),
            capture_policy().limits.clone()
        )
        .unwrap()
        .read_next()
        .unwrap()
        .is_none()
    );
    for observed in [false, true] {
        let target = temp.path().join(format!("cancel-{observed}"));
        let mut pump = capture_pump(&source, &target, config, 8);
        let before = pump.last_acknowledged().clone();
        assert_eq!(pump.poll().unwrap(), ProjectionPoll::Prepared);
        let groups = pump.prepared.as_ref().unwrap().transaction.record_count();
        assert_eq!(pump.poll().unwrap(), ProjectionPoll::Writing);
        if observed {
            wait(|| pump.target.handle().status().committed_operations == 1);
        }
        assert_eq!(pump.last_acknowledged(), &before);
        drop(pump); // joins accepted complete transaction even without pump ACK
        let mut reader = ProjectionReader::open(&target, capture_policy().limits.clone()).unwrap();
        let mut count = 0;
        while reader.read_next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, groups);
    }
    let target = temp.path().join("failed");
    let mut failed = capture_pump(&source, &target, config, 8);
    let before = failed.last_acknowledged().clone();
    assert_eq!(failed.poll().unwrap(), ProjectionPoll::Prepared);
    // Actual filesystem fault after data write+sync, before watermark staging.
    // Original header watermark remains valid; uncommitted bytes are excluded.
    std::fs::write(target.join("committed.tmp"), b"occupied fresh staging name").unwrap();
    assert_eq!(failed.poll().unwrap(), ProjectionPoll::Writing);
    wait(|| match failed.poll() {
        Err(ProjectionError::Commit(_)) => true,
        Ok(ProjectionPoll::Writing) => false,
        other => panic!("expected actual pre-watermark failure {other:?}"),
    });
    assert!(failed.is_poisoned());
    assert_eq!(failed.last_acknowledged(), &before);
    assert!(matches!(failed.poll(), Err(ProjectionError::Poisoned)));
    drop(failed);
    assert!(std::fs::metadata(target.join("events.mfsf")).unwrap().len() > 48);
    assert!(
        ProjectionReader::open(&target, capture_policy().limits.clone())
            .unwrap()
            .read_next()
            .unwrap()
            .is_none()
    );
    let mut rebuilt = capture_pump(&source, &temp.path().join("rebuilt"), config, 8);
    wait(|| rebuilt.poll().unwrap() == ProjectionPoll::Sealed);
    let checkpoint = rebuilt.last_acknowledged().clone();
    rebuilt.shutdown().unwrap();
    let compared = compare_with_source(
        capture_source(&source),
        ProjectionReader::open(temp.path().join("rebuilt"), capture_policy().limits.clone())
            .unwrap(),
    )
    .unwrap();
    assert!(compared.sealed);
    assert_eq!(compared.groups, checkpoint.target.sequence);
}

#[test]
fn projection_invalid_batch_count_rejects_before_creating_target() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let config = stop_source(&source, false);
    for groups in [0, 9] {
        let target = temp.path().join(format!("invalid-{groups}"));
        assert!(
            ProjectionPump::create_batched(
                capture_source(&source),
                capture_policy(),
                &target,
                64 << 20,
                config,
                groups
            )
            .is_err()
        );
        assert!(!target.exists());
    }
    let mut smaller = config;
    smaller.max_group_operations = 1;
    let target = temp.path().join("actor-count");
    assert!(
        ProjectionPump::create_batched(
            capture_source(&source),
            capture_policy(),
            &target,
            64 << 20,
            smaller,
            2
        )
        .is_err()
    );
    assert!(!target.exists());
}

#[test]
fn projection_partial_batch_flushes_at_available_source_frontier() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = batch();
    config.max_group_operations = 8;
    config.max_pending_operations = 8;
    let mut pump = ProjectionPump::create_batched(
        source(&temp.path().join("source")),
        ProjectionPolicy {
            schema_version: 1,
            source_profile: 3,
            clock_policy: ProjectionClockPolicy::PerOwnerStartedPhysicalOrderV1,
            limits: limits(),
        },
        temp.path().join("target"),
        64 << 20,
        config,
        8,
    )
    .unwrap();
    assert_eq!(
        pump.poll_with_preparation_age(|_| panic!("prepare must not admit"), || Duration::ZERO)
            .unwrap(),
        ProjectionPoll::Prepared
    );
    assert_eq!(
        pump.prepared.as_ref().unwrap().transaction.record_count(),
        1
    );
    assert_eq!(pump.source.progress().unwrap().cursor.sequence, 1);
    assert_eq!(pump.poll().unwrap(), ProjectionPoll::Writing);
    wait(|| pump.poll().unwrap() == ProjectionPoll::Committed);
    assert_eq!(pump.poll().unwrap(), ProjectionPoll::CaughtUp);
    pump.shutdown().unwrap();
}

async fn native_value_source(path: &Path) -> BatchConfig {
    use super::super::owner::{InputSettlement, OwnerPhase, history::HistoryJournalOwner};
    use marketfeed_engine::CaptureInput;
    use marketfeed_model::{ConnectionId, FrameStamp, TimestampNs};
    use marketfeed_transport::{MemoryWebSocket, WebSocketSpec, WebSocketTransport};
    fn handoff(owner: &HistoryJournalOwner, input: CaptureInput) {
        assert!(matches!(
            owner
                .ingress()
                .try_submit(ConnectionId(1), input)
                .unwrap()
                .wait(),
            InputSettlement::Committed { .. }
        ));
        wait(|| owner.try_next().is_some());
    }
    async fn flush(owner: &HistoryJournalOwner, transport: &mut MemoryWebSocket) {
        let end = Instant::now() + Duration::from_secs(10);
        loop {
            while let Some(token) = owner.try_take_wire() {
                assert_eq!(
                    token.invoke(transport).await.unwrap().outcome,
                    WireOutcomeDto::Succeeded
                );
            }
            let status = owner.status();
            if status.owner.phase == OwnerPhase::Idle
                && status.owner.pending_operations == 0
                && status.ingress_observations == 0
                && owner.wire_progress() == Some((0, 0, true))
            {
                break;
            }
            assert!(status.owner.failure.is_none(), "{status:?}");
            assert!(
                Instant::now() < end,
                "native fixture wire settlement watchdog"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    fn text(payload: String, n: i64) -> CaptureInput {
        CaptureInput::Text {
            payload: payload.into_bytes(),
            received: FrameStamp {
                receive_ts: TimestampNs(1_710_000_000_000_000_000 + n),
                mono_ns: n as u64,
            },
        }
    }
    let config = BatchConfig {
        max_pending_operations: 16,
        max_pending_bytes: 32 << 20,
        max_group_operations: 8,
        max_group_bytes: 16 << 20,
        max_age: Duration::from_millis(1),
    };
    let actor = FeedBatchActor::spawn(
        FeedWriter::create_profile(path, 64 << 20, FeedProfile::EngineCausalV5).unwrap(),
        config,
    )
    .unwrap();
    for dto in super::super::capture::projection_test_bootstraps() {
        let owner = PreparedBootstrap::new_wire_history(dto.clone(), MAX_OPERATION_BYTES)
            .unwrap()
            .try_admit(&actor.handle())
            .unwrap()
            .wait()
            .unwrap()
            .instantiate()
            .unwrap()
            .spawn_history_owner()
            .unwrap();
        let mut transport = MemoryWebSocket::new();
        transport.connect(&WebSocketSpec::default()).await.unwrap();
        handoff(
            &owner,
            CaptureInput::Connected {
                connection: ConnectionId(1),
                now: TimestampNs(1_710_000_000_000_000_000),
            },
        );
        flush(&owner, &mut transport).await;
        for topic in &dto.protocol_topics {
            handoff(
                &owner,
                text(
                    format!(
                        r#"{{"channel":"subscriptionResponse","data":{{"method":"subscribe","subscription":{topic}}}}}"#
                    ),
                    1,
                ),
            );
            flush(&owner, &mut transport).await;
        }
        let coin = &dto.catalog.instruments[0].key.native_symbol;
        for tid in 1..=2 {
            handoff(
                &owner,
                text(
                    format!(
                        r#"{{"channel":"trades","data":[{{"coin":"{coin}","side":"B","px":"10.25","sz":"2","hash":"h{tid}","time":1710000000001,"tid":{tid}}}]}}"#
                    ),
                    tid + 1,
                ),
            );
            flush(&owner, &mut transport).await;
        }
        assert_eq!(transport.outbound.len(), dto.protocol_topics.len());
        assert!(matches!(
            owner
                .ingress()
                .try_submit(ConnectionId(1), CaptureInput::Stop)
                .unwrap()
                .wait(),
            InputSettlement::Committed { .. }
        ));
        owner.close_ingress();
        wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
        assert_eq!(owner.wire_progress(), Some((0, 0, true)));
        owner.shutdown();
    }
    actor
        .handle()
        .try_submit(
            PreparedFeedTransaction::new(vec![FeedRecord::Seal {
                schema_version: 1,
                now_ns: i64::MAX,
                reason: "closed native financial correctness fixture; no network".into(),
            }])
            .unwrap(),
        )
        .unwrap()
        .wait()
        .unwrap();
    actor.shutdown().unwrap();
    config
}

#[tokio::test]
async fn projection_default_one_and_eight_preserve_exact_native_records_seal_and_every_resume_pair()
{
    let _publication = super::super::native_publication_fixture_permit().await;
    use marketfeed_adapter_api::NativeObservation;
    use marketfeed_model::Fixed;
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let config = native_value_source(&source).await;
    let source_bytes = std::fs::read(source.join("events.mfsf")).unwrap();
    let source_watermark = std::fs::read(source.join("committed.json")).unwrap();
    let mut published = Vec::new();
    for maximum in [1, 8] {
        let target = temp.path().join(format!("target-{maximum}"));
        let replay = capture_source(&source);
        let mut pump = if maximum == 1 {
            ProjectionPump::create(replay, capture_policy(), &target, 64 << 20, config).unwrap()
        } else {
            ProjectionPump::create_batched(
                replay,
                capture_policy(),
                &target,
                64 << 20,
                config,
                maximum,
            )
            .unwrap()
        };
        assert_eq!(pump.max_groups_per_publication(), maximum);
        wait(|| pump.poll().unwrap() == ProjectionPoll::Sealed);
        let checkpoint = pump.last_acknowledged().clone();
        let logical = pump.prepared_groups();
        let report = pump.shutdown().unwrap();
        assert!(checkpoint.sealed);
        assert_eq!(checkpoint.source.sequence, logical);
        assert_eq!(checkpoint.target.sequence, logical);
        assert_eq!(report.committed_operations, report.groups);
        assert!(report.groups <= logical);
        eprintln!(
            "physical batching correctness census: configured_max={maximum} logical_records={logical} target_transactions={} target_publications={}",
            report.committed_operations, report.groups
        );
        if maximum == 1 {
            assert_eq!(report.groups, logical);
        }
        let compared = compare_with_source(
            capture_source(&source),
            ProjectionReader::open(&target, capture_policy().limits.clone()).unwrap(),
        )
        .unwrap();
        assert_eq!(compared.groups, logical);
        assert!(compared.sealed);
        let mut reader = ProjectionReader::open(&target, capture_policy().limits.clone()).unwrap();
        let mut frontiers = vec![reader.progress().unwrap()];
        let mut trades = Vec::new();
        while let Some(group) = reader.read_next().unwrap() {
            for item in &group.items {
                if let ItemDto::ObservedMarket(leaf) = item {
                    for row in &leaf.batch().unwrap().native().rows {
                        if let NativeObservation::HyperliquidTrade(trade) = &row.observation {
                            assert_eq!(trade.price, Fixed::new(1025, 2));
                            assert_eq!(trade.size, Fixed::new(2, 0));
                            trades.push((trade.coin.clone(), trade.tid, trade.hash.clone()));
                        }
                    }
                }
            }
            frontiers.push(reader.progress().unwrap());
        }
        assert_eq!(
            trades,
            vec![
                ("@12".into(), 1, "h1".into()),
                ("@12".into(), 2, "h2".into()),
                ("BTC".into(), 1, "h1".into()),
                ("BTC".into(), 2, "h2".into())
            ]
        );
        for (index, pair) in frontiers.into_iter().enumerate() {
            let mut resumed = ProjectionReader::resume(
                &target,
                pair.target_cursor,
                pair.source_cursor,
                capture_policy().limits.clone(),
            )
            .unwrap();
            let mut suffix = 0;
            while resumed.read_next().unwrap().is_some() {
                suffix += 1;
            }
            assert_eq!(suffix, logical - index as u64);
            assert!(resumed.progress().unwrap().sealed);
        }
        // Target identity lives in the48-byte header. All framed logical record
        // bytes, sequences, CRCs, source pairs and native values are identical.
        published.push(std::fs::read(target.join("events.mfsf")).unwrap()[48..].to_vec());
    }
    assert_eq!(published[0], published[1]);
    assert_eq!(
        std::fs::read(source.join("events.mfsf")).unwrap(),
        source_bytes
    );
    assert_eq!(
        std::fs::read(source.join("committed.json")).unwrap(),
        source_watermark
    );
}

#[test]
fn projection_late_invalid_source_discards_unadmitted_batch_and_poisons_without_skip() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let config = stop_source(&source, true);
    let original = std::fs::read(source.join("events.mfsf")).unwrap();
    let corrupt = temp.path().join("corrupt-copy");
    std::fs::create_dir(&corrupt).unwrap();
    std::fs::copy(
        source.join("committed.json"),
        corrupt.join("committed.json"),
    )
    .unwrap();
    let mut bytes = original.clone();
    let second = 48 + 8 + u32::from_le_bytes(bytes[48..52].try_into().unwrap()) as usize;
    bytes[second + 4] ^= 1; // retained copy only: second record CRC invalid
    std::fs::write(corrupt.join("events.mfsf"), bytes).unwrap();
    for maximum in [1, 8] {
        let target = temp.path().join(format!("failed-{maximum}"));
        let mut pump = capture_pump(&corrupt, &target, config, maximum);
        let before = pump.last_acknowledged().clone();
        if maximum == 1 {
            assert_eq!(pump.poll().unwrap(), ProjectionPoll::Prepared);
            assert_eq!(pump.poll().unwrap(), ProjectionPoll::Writing);
            wait(|| pump.poll().unwrap() == ProjectionPoll::Committed);
            assert_eq!(pump.last_acknowledged().source.sequence, 1);
        }
        assert!(matches!(
            pump.poll_with_preparation_age(
                |_| panic!("invalid candidate must not admit"),
                || Duration::ZERO
            ),
            Err(ProjectionError::Replay(_))
        ));
        assert!(pump.is_poisoned());
        assert!(pump.prepared.is_none() && pump.accepted.is_none());
        if maximum == 8 {
            assert_eq!(pump.last_acknowledged(), &before);
            assert_eq!(pump.prepared_groups(), 0);
            assert!(pump.last_publication().is_none());
        }
        assert!(matches!(pump.poll(), Err(ProjectionError::Poisoned)));
        drop(pump);
        let mut reader = ProjectionReader::open(&target, capture_policy().limits.clone()).unwrap();
        let mut visible = 0;
        while reader.read_next().unwrap().is_some() {
            visible += 1;
        }
        assert_eq!(visible, usize::from(maximum == 1));
    }
    assert_eq!(std::fs::read(source.join("events.mfsf")).unwrap(), original);
    let target = temp.path().join("fresh-rebuild");
    let mut rebuilt = capture_pump(&source, &target, config, 8);
    wait(|| rebuilt.poll().unwrap() == ProjectionPoll::Sealed);
    rebuilt.shutdown().unwrap();
    assert!(
        compare_with_source(
            capture_source(&source),
            ProjectionReader::open(&target, capture_policy().limits).unwrap()
        )
        .unwrap()
        .sealed
    );
}

#[test]
fn projection_actual_multi_record_ack_cannot_authorize_wrong_count_or_source_boundary() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let config = stop_source(&source, false);
    for mutation in 0..4 {
        let target = temp.path().join(format!("bad-boundary-{mutation}"));
        let mut pump = capture_pump(&source, &target, config, 8);
        let before = pump.last_acknowledged().clone();
        assert_eq!(
            pump.poll_with_preparation_age(
                |_| panic!("preparation must not admit"),
                || Duration::ZERO
            )
            .unwrap(),
            ProjectionPoll::Prepared
        );
        assert_eq!(
            pump.prepared.as_ref().unwrap().transaction.record_count(),
            8
        );
        assert_eq!(pump.poll().unwrap(), ProjectionPoll::Writing);
        let accepted = pump.accepted.as_mut().unwrap();
        match mutation {
            0 => accepted.groups = 0,
            1 => accepted.groups = 9,
            2 => accepted.after.sequence += 1,
            _ => accepted.after.offset = before.source.offset,
        }
        wait(|| match pump.poll() {
            Err(ProjectionError::Invalid("target publication receipt/source boundary")) => true,
            Ok(ProjectionPoll::Writing) => false,
            other => panic!("unexpected malformed boundary result {other:?}"),
        });
        assert!(pump.is_poisoned());
        assert_eq!(pump.last_acknowledged(), &before);
        drop(pump);
        let mut reader = ProjectionReader::open(&target, capture_policy().limits.clone()).unwrap();
        let mut visible = 0;
        while reader.read_next().unwrap().is_some() {
            visible += 1;
        }
        assert_eq!(visible, 8); // durable whole batch is separate from pump ACK
    }
}
