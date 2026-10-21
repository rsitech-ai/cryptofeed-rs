//! Actual public Hyperliquid MFCJ5 owner, memory-only transport. No live I/O.
use super::hyperliquid_fixture::{WAIT, inputs, native_fixture, replay_limits, wait, wait_inspect};
use marketfeed_adapter_api::*;
use marketfeed_daemon::causal_bootstrap::{
    PreparedBootstrap,
    owner::{InputSettlement, OwnerPhase, history::HistoryJournalOwner},
};
use marketfeed_daemon::causal_bootstrap::{
    owner::history::WireAuthorization,
    projection::{ProjectionPoll, ProjectionPump, compare_with_source},
    replay::MultiHistoryReplay,
};
use marketfeed_engine::causal::projection::{
    ProjectionClockPolicy, ProjectionLimits, ProjectionPolicy, ProjectionReader,
};
use marketfeed_engine::{CaptureInput, causal::*};
use marketfeed_model::*;
use marketfeed_recording::{shared_batch::*, shared_feed::*};
use marketfeed_transport::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
fn dto(spot: bool) -> BootstrapDto {
    let mut dto = native_fixture(spot);
    dto.bind_wire_policy(WirePolicyDto {
        schema_version: 1,
        max_pending_writes: 16,
        max_pending_bytes: 64 << 10,
        transport_controls: TransportControlsDto::ManagedConnectReadAutomaticPongCloseUnjournaledV1,
    })
    .unwrap();
    dto
}
fn actor(path: &std::path::Path, age: u64) -> FeedBatchActor {
    FeedBatchActor::spawn(
        FeedWriter::create_profile(path, 64 << 20, FeedProfile::EngineCausalV5).unwrap(),
        BatchConfig {
            max_pending_operations: 1,
            max_pending_bytes: 8 << 20,
            max_group_operations: 1,
            max_group_bytes: 8 << 20,
            max_age: Duration::from_millis(age),
        },
    )
    .unwrap()
}
fn owner(dto: BootstrapDto, actor: &FeedBatchActor) -> HistoryJournalOwner {
    PreparedBootstrap::new_wire_history(dto, MAX_OPERATION_BYTES)
        .unwrap()
        .try_admit(&actor.handle())
        .unwrap()
        .wait()
        .unwrap()
        .instantiate()
        .unwrap()
        .spawn_history_owner()
        .unwrap()
}
fn handoff(
    owner: &HistoryJournalOwner,
    connection: u64,
    input: CaptureInput,
) -> marketfeed_daemon::causal_bootstrap::owner::CommittedRelease {
    assert!(matches!(
        owner
            .ingress()
            .try_submit(ConnectionId(connection), input)
            .unwrap()
            .wait(),
        InputSettlement::Committed { .. }
    ));
    let mut release = None;
    wait_inspect(
        || {
            release = owner.try_next();
            release.is_some()
        },
        || format!("wire handoff {:?}", owner.status()),
    );
    release.unwrap()
}
fn take(owner: &HistoryJournalOwner) -> WireAuthorization {
    let mut token = None;
    wait_inspect(
        || {
            token = owner.try_take_wire();
            token.is_some()
        },
        || {
            format!(
                "wire authority {:?} {:?}",
                owner.status(),
                owner.wire_progress()
            )
        },
    );
    token.unwrap()
}
async fn drain(
    owner: &HistoryJournalOwner,
    release: &marketfeed_daemon::causal_bootstrap::owner::CommittedRelease,
    ws: &mut MemoryWebSocket,
) {
    let expected = release
        .released_items()
        .filter_map(|item| match item {
            ItemDto::Write(frame)
                if matches!(frame.opcode, OpcodeDto::Text | OpcodeDto::Binary) =>
            {
                Some(frame.clone())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    for frame in expected {
        let before = ws.outbound.len();
        let result = take(owner).invoke(ws).await.unwrap();
        assert_eq!(result.outcome, WireOutcomeDto::Succeeded);
        assert_eq!(result.receipt.first_sequence, result.receipt.last_sequence);
        assert_eq!(ws.outbound.len(), before + 1);
        assert_eq!(WriteDto::from(ws.outbound.last().unwrap()), frame);
    }
}
fn finish(owner: HistoryJournalOwner, actor: FeedBatchActor) {
    owner.close_ingress();
    wait_inspect(
        || owner.status().owner.phase == OwnerPhase::Stopped,
        || {
            format!(
                "wire finish {:?} {:?}",
                owner.status(),
                owner.wire_progress()
            )
        },
    );
    assert!(owner.shutdown().owner.failure.is_none());
    let ticket = actor
        .handle()
        .try_submit(
            PreparedFeedTransaction::new(vec![FeedRecord::Seal {
                schema_version: 1,
                now_ns: 7,
                reason: "settled application-write test; transport controls unjournaled".into(),
            }])
            .unwrap(),
        )
        .unwrap();
    ticket.wait().unwrap();
    actor.shutdown().unwrap();
}
#[tokio::test]
async fn wire_actual_spot_perps_send_exactly_once_and_replay_projection_every_prefix() {
    for spot in [true, false] {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        let actor = actor(&source, 1);
        let dto = dto(spot);
        let owner = owner(dto.clone(), &actor);
        let mut ws = MemoryWebSocket::new();
        ws.connect(&WebSocketSpec {
            url: "memory://wire".into(),
            max_frame_bytes: 1 << 20,
            ..Default::default()
        })
        .await
        .unwrap();
        for (connection, input) in inputs(&dto) {
            let release = handoff(&owner, connection, input);
            drain(&owner, &release, &mut ws).await;
        }
        finish(owner, actor);
        assert!(FeedReader::open_profile(&source, None, FeedProfile::EngineCausalV4).is_err());
        let mut raw = FeedReader::open_profile(&source, None, FeedProfile::EngineCausalV5).unwrap();
        let mut records = Vec::new();
        let mut mutated = false;
        while let Some(mut record) = raw.read_next().unwrap() {
            if let FeedRecord::Causal(e) = &mut record {
                if e.phase != CausalPhase::Bootstrap && !mutated {
                    let identity = OperationIdentity {
                        venue: e.route.venue,
                        session: e.route.session,
                        bootstrap_id: e.route.bootstrap_id.clone(),
                    };
                    let mut dto = decode_owner_history(e, &identity).unwrap();
                    if let OwnerEventDto::Wire(WireEventDto::Intent { binding, .. }) =
                        &mut dto.event
                    {
                        binding.frame.payload = serde_json::from_str("\"Zm9yZ2Vk\"").unwrap();
                        *e = encode_owner_history(&dto, &identity, MAX_OPERATION_BYTES).unwrap();
                        mutated = true;
                    }
                }
            }
            records.push(record);
        }
        assert!(mutated);
        let altered = temp.path().join("altered");
        let mut writer =
            FeedWriter::create_profile(&altered, 64 << 20, FeedProfile::EngineCausalV5).unwrap();
        writer.append_many(records).unwrap();
        let mut invalid = MultiHistoryReplay::open_profile(
            &altered,
            replay_limits(),
            FeedProfile::EngineCausalV5,
        )
        .unwrap();
        let mut rejected = false;
        loop {
            match invalid.read_next() {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(_) => {
                    rejected = true;
                    break;
                }
            }
        }
        assert!(
            rejected,
            "CRC-valid forged payload must differ from actual native handoff"
        );

        let mut replay =
            MultiHistoryReplay::open_profile(&source, replay_limits(), FeedProfile::EngineCausalV5)
                .unwrap();
        let mut pairs = Vec::new();
        let mut wire = 0;
        while let Some(step) = replay.read_next_verified().unwrap() {
            if let marketfeed_daemon::causal_bootstrap::replay::VerifiedSourceRecord::Owner {
                observation,
                ..
            } = step.record()
            {
                if matches!(observation.event, OwnerEventDto::Wire(_)) {
                    wire += 1;
                }
            }
            pairs.push(step.after_cursor().clone());
        }
        assert!(wire > 0);
        assert!(replay.progress().unwrap().sealed);
        for cursor in &pairs {
            let resumed = MultiHistoryReplay::resume_profile(
                &source,
                cursor.clone(),
                replay_limits(),
                FeedProfile::EngineCausalV5,
            )
            .unwrap();
            assert_eq!(resumed.progress().unwrap().cursor, *cursor);
        }
        let source_reader =
            MultiHistoryReplay::open_profile(&source, replay_limits(), FeedProfile::EngineCausalV5)
                .unwrap();
        let limits = ProjectionLimits {
            max_group_bytes: 1 << 20,
            max_items: 1024,
            max_owners: 8,
            max_bootstrap_bytes: 8 << 20,
            max_catalog_entries: 128,
            max_pending_inputs: 128,
        };
        let target = temp.path().join("target");
        let mut pump = ProjectionPump::create(
            source_reader,
            ProjectionPolicy {
                schema_version: 1,
                source_profile: 5,
                clock_policy: ProjectionClockPolicy::PerOwnerStartedPhysicalOrderV1,
                limits: limits.clone(),
            },
            &target,
            64 << 20,
            BatchConfig {
                max_pending_operations: 1,
                max_pending_bytes: 4 << 20,
                max_group_operations: 1,
                max_group_bytes: 4 << 20,
                max_age: Duration::from_millis(1),
            },
        )
        .unwrap();
        wait(|| matches!(pump.poll().unwrap(), ProjectionPoll::Sealed));
        pump.shutdown().unwrap();
        let checked = compare_with_source(
            MultiHistoryReplay::open_profile(&source, replay_limits(), FeedProfile::EngineCausalV5)
                .unwrap(),
            ProjectionReader::open(&target, limits).unwrap(),
        )
        .unwrap();
        assert!(checked.sealed);
    }
}
#[tokio::test]
async fn wire_stale_before_method_call_never_invokes_synchronous_transport() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let actor = actor(&source, 1);
    let dto = dto(false);
    let owner = owner(dto.clone(), &actor);
    handoff(
        &owner,
        1,
        CaptureInput::Connected {
            connection: ConnectionId(1),
            now: TimestampNs(0),
        },
    );
    let token = take(&owner);
    let stop = owner
        .ingress()
        .try_submit(
            ConnectionId(1),
            CaptureInput::TransportLost {
                reason: DisconnectReason::RemoteClose,
                now: TimestampNs(1),
            },
        )
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut ws = SyncCallTransport(calls.clone());
    assert!(token.invoke(&mut ws).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(stop);
    owner.ingress().revoke();
    owner.shutdown();
    actor.shutdown().unwrap();
    let mut replay =
        MultiHistoryReplay::open_profile(&source, replay_limits(), FeedProfile::EngineCausalV5)
            .unwrap();
    while replay.read_next().unwrap().is_some() {}
}
struct SyncCallTransport(Arc<AtomicUsize>);
impl WebSocketTransport for SyncCallTransport {
    async fn connect(&mut self, _: &WebSocketSpec) -> Result<(), TransportError> {
        Ok(())
    }
    async fn read_frame(&mut self, _: &mut FrameBuffer) -> Result<InboundFrame, TransportError> {
        Err(TransportError::Closed)
    }
    fn write_frame(
        &mut self,
        _: OutboundFrame,
    ) -> impl std::future::Future<Output = Result<(), TransportError>> + Send {
        self.0.fetch_add(1, Ordering::SeqCst);
        std::future::pending()
    }
    async fn close(&mut self, _: CloseReason) -> Result<(), TransportError> {
        Ok(())
    }
}
#[tokio::test]
async fn wire_cancel_during_actual_invocation_is_unknown_and_seal_refused() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let actor = actor(&source, 1);
    let owner = owner(dto(false), &actor);
    handoff(
        &owner,
        1,
        CaptureInput::Connected {
            connection: ConnectionId(1),
            now: TimestampNs(0),
        },
    );
    let token = take(&owner);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut ws = SyncCallTransport(calls.clone());
    let mut invocation = Box::pin(token.invoke(&mut ws));
    // Cancellation must follow actual transport METHOD entry, not an assumed
    // time for the durable invocation-permit ACK under unrelated disk load.
    tokio::time::timeout(
        WAIT,
        std::future::poll_fn(
            |cx| match std::future::Future::poll(invocation.as_mut(), cx) {
                std::task::Poll::Ready(result) => {
                    panic!("invocation completed before pending transport barrier: {result:?}")
                }
                std::task::Poll::Pending => {
                    if calls.load(Ordering::SeqCst) == 1 {
                        std::task::Poll::Ready(())
                    } else {
                        std::task::Poll::Pending
                    }
                }
            },
        ),
    )
    .await
    .expect("actual transport entry before the existing test watchdog");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(invocation);
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    assert!(owner.wire_progress().is_some_and(|(_, _, clean)| !clean));
    owner.shutdown();
    actor
        .handle()
        .try_submit(
            PreparedFeedTransaction::new(vec![FeedRecord::Seal {
                schema_version: 1,
                now_ns: 8,
                reason: "must be rejected by semantic owner reader".into(),
            }])
            .unwrap(),
        )
        .unwrap()
        .wait()
        .unwrap();
    actor.shutdown().unwrap();
    let mut replay =
        MultiHistoryReplay::open_profile(&source, replay_limits(), FeedProfile::EngineCausalV5)
            .unwrap();
    let mut failed = false;
    loop {
        match replay.read_next() {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => {
                failed = true;
                break;
            }
        }
    }
    assert!(failed);
}
#[test]
fn wire_policy_refuses_old_profiles_and_runtime_http() {
    let dto = dto(false);
    assert!(PreparedBootstrap::new_observed_history(dto.clone(), MAX_OPERATION_BYTES).is_err());
    assert!(PreparedBootstrap::new_history(dto, MAX_OPERATION_BYTES).is_err());
}
#[tokio::test]
async fn wire_full_admission_and_delayed_ack_expose_no_authority_or_effect() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let actor = actor(&source, 300);
    let base = dto(false);
    let owner = owner(base.clone(), &actor);
    assert!(matches!(
        owner
            .ingress()
            .try_submit(
                ConnectionId(1),
                CaptureInput::Connected {
                    connection: ConnectionId(1),
                    now: TimestampNs(0)
                }
            )
            .unwrap()
            .wait(),
        InputSettlement::Committed { .. }
    ));
    wait(|| owner.status().owner.phase == OwnerPhase::ReleasePending);
    let mut extra = base.clone();
    extra.capture.session = 32;
    let occupied = PreparedBootstrap::new_wire_history(extra, MAX_OPERATION_BYTES)
        .unwrap()
        .try_admit(&actor.handle())
        .unwrap();
    let release = owner.try_next().unwrap();
    assert!(owner.try_take_wire().is_none());
    wait(|| owner.status().owner.admission_retries > 0);
    assert!(owner.try_take_wire().is_none());
    occupied.wait().unwrap();
    let first = take(&owner);
    let mut reader = FeedReader::open_profile(&source, None, FeedProfile::EngineCausalV5).unwrap();
    let mut found = false;
    while let Some(record) = reader.read_next().unwrap() {
        if let FeedRecord::Causal(e) = record {
            if e.phase != CausalPhase::Bootstrap && e.route.session == 31 {
                let id = OperationIdentity {
                    venue: e.route.venue,
                    session: e.route.session,
                    bootstrap_id: e.route.bootstrap_id.clone(),
                };
                let dto = decode_owner_history(&e, &id).unwrap();
                if let OwnerEventDto::Wire(WireEventDto::Intent { wire_id, .. }) = dto.event {
                    found |= wire_id == first.id();
                }
            }
        }
    }
    assert!(
        found,
        "authorization must have an actual published intent record"
    );
    let calls = Arc::new(AtomicUsize::new(0));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(first);
    let count=release.released_items().filter(|x|matches!(x,ItemDto::Write(w) if matches!(w.opcode,OpcodeDto::Text|OpcodeDto::Binary))).count();
    let mut ws = MemoryWebSocket::new();
    ws.connect(&WebSocketSpec {
        url: "memory://wire".into(),
        max_frame_bytes: 1 << 20,
        ..Default::default()
    })
    .await
    .unwrap();
    for _ in 1..count {
        assert_eq!(
            take(&owner).invoke(&mut ws).await.unwrap().outcome,
            WireOutcomeDto::Succeeded
        );
    }
    wait(|| {
        owner
            .wire_progress()
            .is_some_and(|(count, _, _)| count == 0)
    });
    assert_eq!(ws.outbound.len(), count - 1);
    finish(owner, actor);
}
#[test]
fn wire_runtime_http_is_refused_before_native_acceptance() {
    let temp = tempfile::tempdir().unwrap();
    let actor = actor(&temp.path().join("source"), 1);
    let owner = owner(dto(false), &actor);
    let input = CaptureInput::Http {
        request_id: 7,
        response: marketfeed_adapter_api::HttpResponse {
            status: 200,
            headers: vec![],
            body: b"{}".to_vec().into(),
        },
        received: FrameStamp {
            receive_ts: TimestampNs(1),
            mono_ns: 1,
        },
    };
    assert!(owner.ingress().try_submit(ConnectionId(0), input).is_err());
    assert_eq!(owner.status().owner.accepted, 0);
    owner.shutdown();
    actor.shutdown().unwrap();
}
#[tokio::test]
async fn wire_observed_error_details_are_exact_or_explicit_capacity_unknown() {
    for oversized in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        let actor = actor(&source, 1);
        let owner = owner(dto(false), &actor);
        handoff(
            &owner,
            1,
            CaptureInput::Connected {
                connection: ConnectionId(1),
                now: TimestampNs(0),
            },
        );
        let text = if oversized {
            "x".repeat(MAX_WIRE_ERROR_UTF8 + 1)
        } else {
            "exact protocol failure: λ".into()
        };
        let mut ws = ErrorTransport(text.clone());
        let result = take(&owner).invoke(&mut ws).await.unwrap();
        if oversized {
            assert_eq!(result.outcome, WireOutcomeDto::Unknown);
            assert!(
                matches!(result.detail,Some(WireErrorDto::CapacityExceeded{utf8_bytes,..}) if utf8_bytes==text.len() as u64)
            );
        } else {
            assert_eq!(result.outcome, WireOutcomeDto::Failed);
            assert_eq!(
                result.detail,
                Some(WireErrorDto::Transport(TransportErrorDto::Protocol(text)))
            );
        }
        wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
        assert!(owner.wire_progress().is_some_and(|(_, _, clean)| !clean));
        owner.shutdown();
        actor.shutdown().unwrap();
        let mut replay =
            MultiHistoryReplay::open_profile(&source, replay_limits(), FeedProfile::EngineCausalV5)
                .unwrap();
        while replay.read_next().unwrap().is_some() {}
        assert!(
            !replay.progress().unwrap().owners[0]
                .uncertainties
                .is_empty()
        );
    }
}
struct ErrorTransport(String);
impl WebSocketTransport for ErrorTransport {
    async fn connect(&mut self, _: &WebSocketSpec) -> Result<(), TransportError> {
        Ok(())
    }
    async fn read_frame(&mut self, _: &mut FrameBuffer) -> Result<InboundFrame, TransportError> {
        Err(TransportError::Closed)
    }
    async fn write_frame(&mut self, _: OutboundFrame) -> Result<(), TransportError> {
        Err(TransportError::Protocol(self.0.clone()))
    }
    async fn close(&mut self, _: CloseReason) -> Result<(), TransportError> {
        Ok(())
    }
}
#[tokio::test]
async fn wire_intentional_stop_commits_its_own_input_and_settles_uninvoked_lease() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let actor = actor(&source, 1);
    let owner = owner(dto(false), &actor);
    handoff(
        &owner,
        1,
        CaptureInput::Connected {
            connection: ConnectionId(1),
            now: TimestampNs(0),
        },
    );
    let token = take(&owner);
    let stop = owner
        .ingress()
        .try_submit(ConnectionId(1), CaptureInput::Stop)
        .unwrap();
    let result = stop.wait();
    assert!(matches!(result, InputSettlement::Committed { .. }));
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    assert_eq!(
        owner.status().owner.failure,
        Some(marketfeed_daemon::causal_bootstrap::owner::OwnerFailure::NativeStopped)
    );
    assert_eq!(owner.wire_progress(), Some((0, 0, true)));
    let calls = Arc::new(AtomicUsize::new(0));
    assert!(
        token
            .invoke(&mut SyncCallTransport(calls.clone()))
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    owner.shutdown();
    actor
        .handle()
        .try_submit(
            PreparedFeedTransaction::new(vec![FeedRecord::Seal {
                schema_version: 1,
                now_ns: 9,
                reason: "intentional committed Stop; wire settled".into(),
            }])
            .unwrap(),
        )
        .unwrap()
        .wait()
        .unwrap();
    actor.shutdown().unwrap();
    let mut replay =
        MultiHistoryReplay::open_profile(&source, replay_limits(), FeedProfile::EngineCausalV5)
            .unwrap();
    while replay.read_next().unwrap().is_some() {}
    assert!(replay.progress().unwrap().sealed);
}

#[tokio::test]
async fn wire_new_connection_retires_old_lease_without_revival_and_replays() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let actor = actor(&source, 1);
    let owner = owner(dto(false), &actor);
    handoff(
        &owner,
        1,
        CaptureInput::Connected {
            connection: ConnectionId(1),
            now: TimestampNs(0),
        },
    );
    let old = take(&owner);
    let old_id = old.id();
    // This acceptance synchronously fences the previous bundle before the
    // worker can finish its old intent/result ACKs or execute Connected2.
    let admission = owner
        .ingress()
        .try_submit(
            ConnectionId(2),
            CaptureInput::Connected {
                connection: ConnectionId(2),
                now: TimestampNs(-1),
            },
        )
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    assert!(
        old.invoke(&mut SyncCallTransport(calls.clone()))
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        admission.wait(),
        InputSettlement::Committed { .. }
    ));
    let mut release = None;
    wait(|| {
        release = owner.try_next();
        release.is_some()
    });
    let release = release.unwrap();
    assert_eq!(release.evidence().after.connection, 2);
    assert_eq!(release.evidence().route.generation, 2);
    let count = release
        .released_items()
        .filter(|item| {
            matches!(item,
                ItemDto::Write(frame) if matches!(frame.opcode, OpcodeDto::Text | OpcodeDto::Binary)
            )
        })
        .count();
    assert!(count > 0);
    let mut ws = MemoryWebSocket::new();
    ws.connect(&WebSocketSpec {
        url: "memory://new-connection".into(),
        max_frame_bytes: 1 << 20,
        ..Default::default()
    })
    .await
    .unwrap();
    for _ in 0..count {
        let token = take(&owner);
        assert!(token.id() > old_id);
        assert_eq!(
            token.invoke(&mut ws).await.unwrap().outcome,
            WireOutcomeDto::Succeeded
        );
    }
    assert_eq!(ws.outbound.len(), count);
    finish(owner, actor);
    let mut replay =
        MultiHistoryReplay::open_profile(&source, replay_limits(), FeedProfile::EngineCausalV5)
            .unwrap();
    while replay.read_next().unwrap().is_some() {}
    assert!(replay.progress().unwrap().sealed);
}

// These adversaries stop exactly at an extra native permit, before any later
// handoff or Seal could accidentally hide premature callback admission.
async fn challenged_permits(projected: bool) {
    use marketfeed_engine::causal::projection::{
        ProjectionClock, ProjectionOwnerEvent, ProjectionSource,
    };
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("original");
    let actor = actor(&source, 1);
    let owner = owner(dto(false), &actor);
    let release = handoff(
        &owner,
        1,
        CaptureInput::Connected {
            connection: ConnectionId(1),
            now: TimestampNs(0),
        },
    );
    let mut ws = MemoryWebSocket::new();
    ws.connect(&WebSocketSpec {
        url: "memory://prefix-challenge".into(),
        max_frame_bytes: 1 << 20,
        ..Default::default()
    })
    .await
    .unwrap();
    drain(&owner, &release, &mut ws).await;
    finish(owner, actor);
    let mut raw = FeedReader::open_profile(&source, None, FeedProfile::EngineCausalV5).unwrap();
    let mut records = Vec::new();
    while let Some(record) = raw.read_next().unwrap() {
        records.push(record);
    }
    let limits = ProjectionLimits {
        max_group_bytes: 1 << 20,
        max_items: 1024,
        max_owners: 8,
        max_bootstrap_bytes: 8 << 20,
        max_catalog_entries: 128,
        max_pending_inputs: 128,
    };
    let target = temp.path().join("original-target");
    let mut pump = ProjectionPump::create(
        MultiHistoryReplay::open_profile(&source, replay_limits(), FeedProfile::EngineCausalV5)
            .unwrap(),
        ProjectionPolicy {
            schema_version: 1,
            source_profile: 5,
            clock_policy: ProjectionClockPolicy::PerOwnerStartedPhysicalOrderV1,
            limits: limits.clone(),
        },
        &target,
        64 << 20,
        BatchConfig {
            max_pending_operations: 1,
            max_pending_bytes: 4 << 20,
            max_group_operations: 1,
            max_group_bytes: 4 << 20,
            max_age: Duration::from_millis(1),
        },
    )
    .unwrap();
    wait(|| matches!(pump.poll().unwrap(), ProjectionPoll::Sealed));
    pump.shutdown().unwrap();
    let mut original = ProjectionReader::open(&target, limits.clone()).unwrap();
    let mut groups = Vec::new();
    while let Some(group) = original.read_next().unwrap() {
        groups.push(group);
    }
    assert_eq!(groups.len(), records.len());
    let first =
        |predicate: fn(&ProjectionOwnerEvent) -> bool| {
            groups.iter().position(|g|
        matches!(&g.source, ProjectionSource::Owner { event, .. } if predicate(event))).unwrap()
        };
    let handoff = first(|e| matches!(e, ProjectionOwnerEvent::HandoffObserved { .. }));
    let intent = first(|e| matches!(e, ProjectionOwnerEvent::Wire(WireEventDto::Intent { .. })));
    let invocation = first(|e| {
        matches!(
            e,
            ProjectionOwnerEvent::Wire(WireEventDto::InvocationPermitted { .. })
        )
    });
    let settled = groups
        .iter()
        .rposition(|g| {
            matches!(
                &g.source,
                ProjectionSource::Owner {
                    event: ProjectionOwnerEvent::Wire(WireEventDto::Settled { .. }),
                    ..
                }
            )
        })
        .unwrap();
    let mut mismatches = Vec::new();
    for (case, prefix, outcome) in [
        ("candidates", handoff, None),
        ("intent", intent, None),
        ("invocation", invocation, None),
        ("failed-empty-bundle", settled, Some(WireOutcomeDto::Failed)),
        (
            "unknown-empty-bundle",
            settled,
            Some(WireOutcomeDto::Unknown),
        ),
        ("clean-positive", settled, None),
    ] {
        let should_reject = case != "clean-positive";
        let mut source_records = records[..=prefix].to_vec();
        let mut target_groups = groups[..=prefix].to_vec();
        let FeedRecord::Causal(last) = source_records.last_mut().unwrap() else {
            panic!()
        };
        let id = OperationIdentity {
            venue: last.route.venue,
            session: last.route.session,
            bootstrap_id: last.route.bootstrap_id.clone(),
        };
        let mut last_dto = decode_owner_history(last, &id).unwrap();
        if let Some(replacement) = outcome {
            let OwnerEventDto::Wire(WireEventDto::Settled {
                outcome, detail, ..
            }) = &mut last_dto.event
            else {
                panic!()
            };
            *outcome = replacement;
            *detail = (replacement == WireOutcomeDto::Failed)
                .then_some(WireErrorDto::Transport(TransportErrorDto::Closed));
            *last = encode_owner_history(&last_dto, &id, MAX_OPERATION_BYTES).unwrap();
            let ProjectionSource::Owner { event, after, .. } =
                &mut target_groups.last_mut().unwrap().source
            else {
                panic!()
            };
            *event = ProjectionOwnerEvent::from_event(&last_dto.event).unwrap();
            after.uncertainties = vec![OwnerPrefixUncertainty::WireUnsettled { input_id: 1 }];
        }
        let mut next = last_dto.clone();
        next.route.ordinal += 1;
        next.observed_ns += 1;
        next.event = OwnerEventDto::ingress(
            2,
            ConnectionId(1),
            next.observed_ns + 1_000_000_000,
            1024,
            &CaptureInput::TimerPoll {
                now: TimestampNs(1),
            },
            &id,
        )
        .unwrap();
        let ingress = next.clone();
        source_records.push(FeedRecord::Causal(
            encode_owner_history(&next, &id, MAX_OPERATION_BYTES).unwrap(),
        ));
        next.route.ordinal += 1;
        next.observed_ns += 1;
        next.event = OwnerEventDto::ExecutionPermitted {
            input_id: 2,
            native_ordinal: 2,
            generation: 1,
        };
        source_records.push(FeedRecord::Causal(
            encode_owner_history(&next, &id, MAX_OPERATION_BYTES).unwrap(),
        ));
        for observation in [&ingress, &next] {
            let mut group = target_groups.last().unwrap().clone();
            group.source_before = group.source_after.clone();
            group.source_after.sequence += 1;
            group.source_after.offset += 100;
            group.release = None;
            group.items.clear();
            group.item_positions.clear();
            group.outbound_evidence.clear();
            let ProjectionSource::Owner { after, .. } = &group.source else {
                panic!()
            };
            let mut after = after.clone();
            after.journal_ordinal = observation.route.ordinal;
            if matches!(observation.event, OwnerEventDto::Ingress { .. }) {
                after
                    .uncertainties
                    .push(OwnerPrefixUncertainty::ExecutionUnknown { input_id: 2 });
            }
            group.source = ProjectionSource::Owner {
                route: observation.route.clone(),
                observed_ns: observation.observed_ns,
                event: ProjectionOwnerEvent::from_event(&observation.event).unwrap(),
                after,
            };
            let ProjectionClock::Owner {
                started_unix_ns, ..
            } = group.clock
            else {
                panic!()
            };
            group.clock = ProjectionClock::owner(started_unix_ns, observation.observed_ns);
            target_groups.push(group);
        }
        if projected {
            let path = temp.path().join(format!("target-{case}"));
            let mut writer =
                FeedWriter::create_profile(&path, 64 << 20, FeedProfile::ObservationProjectionV1)
                    .unwrap();
            for group in target_groups {
                writer
                    .append(FeedRecord::Projection(group.encode().unwrap()))
                    .unwrap();
            }
            let mut reader = ProjectionReader::open(&path, limits.clone()).unwrap();
            for _ in 0..prefix + 2 {
                assert!(
                    reader.read_next().unwrap().is_some(),
                    "valid projection prefix {case}"
                );
            }
            let before = reader.progress().unwrap();
            let result = reader.read_next();
            if result.is_err() != should_reject {
                mismatches.push(format!("projection permit {case}"));
            }
            if should_reject && result.is_err() {
                assert!(reader.is_poisoned());
                assert!(reader.progress().is_err());
                assert_eq!(before.owners[0].native_ordinal, 1);
            }
        } else {
            let path = temp.path().join(format!("source-{case}"));
            let mut writer =
                FeedWriter::create_profile(&path, 64 << 20, FeedProfile::EngineCausalV5).unwrap();
            let mut cursors = Vec::new();
            for record in source_records {
                cursors.push(writer.append(record).unwrap());
            }
            let mut reader = MultiHistoryReplay::open_profile(
                &path,
                replay_limits(),
                FeedProfile::EngineCausalV5,
            )
            .unwrap();
            for _ in 0..prefix + 2 {
                assert!(
                    reader.read_next().unwrap().is_some(),
                    "valid source prefix {case}"
                );
            }
            let result = reader.read_next();
            if result.is_err() != should_reject {
                mismatches.push(format!("source permit {case}"));
            }
            let rejected_resume = MultiHistoryReplay::resume_profile(
                &path,
                cursors.last().unwrap().clone(),
                replay_limits(),
                FeedProfile::EngineCausalV5,
            )
            .is_err();
            if rejected_resume != should_reject {
                mismatches.push(format!("source resume {case}"));
            }
        }
    }
    assert!(mismatches.is_empty(), "premature permits: {mismatches:?}");
}
#[tokio::test]
async fn wire_source_prefix_refuses_native_permit_until_clean() {
    challenged_permits(false).await;
}
#[tokio::test]
async fn wire_projection_prefix_refuses_native_permit_until_clean() {
    challenged_permits(true).await;
}

struct DelayedWireError {
    started: Arc<AtomicUsize>,
    complete: Arc<std::sync::atomic::AtomicBool>,
}
impl WebSocketTransport for DelayedWireError {
    async fn connect(&mut self, _: &WebSocketSpec) -> Result<(), TransportError> {
        Ok(())
    }
    async fn read_frame(&mut self, _: &mut FrameBuffer) -> Result<InboundFrame, TransportError> {
        Err(TransportError::Closed)
    }
    async fn write_frame(&mut self, _: OutboundFrame) -> Result<(), TransportError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        while !self.complete.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        Err(TransportError::Protocol("exact delayed failure λ".into()))
    }
    async fn close(&mut self, _: CloseReason) -> Result<(), TransportError> {
        Ok(())
    }
}
#[tokio::test]
async fn wire_observed_failure_or_cancel_fences_before_full_result_ack_without_losing_it() {
    let mut gaps = Vec::new();
    for cancelled in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        let base = dto(false);
        let filler = PreparedFeedTransaction::new(
            (32..=33)
                .map(|session| {
                    let mut extra = base.clone();
                    extra.capture.session = session;
                    extra.public_inputs.push(PublicReproductionInputDto::new(
                        "wire-test-reservation",
                        &vec![b'x'; 128 << 10],
                    ));
                    FeedRecord::Causal(
                        encode_bootstrap_for_profile(
                            &extra,
                            MAX_OPERATION_BYTES,
                            FeedProfile::EngineCausalV5,
                        )
                        .unwrap(),
                    )
                })
                .collect(),
        )
        .unwrap();
        // A single byte remains: no owner record fits, while the one-item
        // partial group cannot flush by exact count/bytes before its age.
        let cap = filler.encoded_bytes_bound() + 1;
        let actor = FeedBatchActor::spawn(
            FeedWriter::create_profile(&source, 64 << 20, FeedProfile::EngineCausalV5).unwrap(),
            BatchConfig {
                max_pending_operations: 2,
                max_pending_bytes: cap,
                max_group_operations: 2,
                max_group_bytes: cap,
                max_age: Duration::from_millis(500),
            },
        )
        .unwrap();
        let owner = owner(base.clone(), &actor);
        handoff(
            &owner,
            1,
            CaptureInput::Connected {
                connection: ConnectionId(1),
                now: TimestampNs(0),
            },
        );
        let token = take(&owner);
        assert!(owner.committed_snapshot().is_some());
        // A real accepted input proves the owner was admitting useful work
        // before the transport outcome. It must not execute behind this bundle.
        let queued = owner
            .ingress()
            .try_submit(
                ConnectionId(1),
                CaptureInput::TimerPoll {
                    now: TimestampNs(1),
                },
            )
            .unwrap();
        let started = Arc::new(AtomicUsize::new(0));
        let complete = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut ws = DelayedWireError {
            started: started.clone(),
            complete: complete.clone(),
        };
        let mut invocation = Some(Box::pin(token.invoke(&mut ws)));
        // Drive through the real InvocationPermitted ACK and into pending I/O.
        let invocation_deadline = Instant::now() + WAIT;
        loop {
            assert!(
                Instant::now() < invocation_deadline,
                "invocation permit test watchdog"
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(20), invocation.as_mut().unwrap())
                    .await
                    .is_err()
            );
            if started.load(Ordering::SeqCst) == 1 {
                break;
            }
        }
        let occupied = actor.handle().try_submit(filler).unwrap();
        let retries = owner.status().owner.admission_retries;
        if cancelled {
            drop(invocation.take());
        } else {
            complete.store(true, Ordering::SeqCst);
            assert!(
                tokio::time::timeout(Duration::from_millis(20), invocation.as_mut().unwrap())
                    .await
                    .is_err()
            );
        }
        wait(|| owner.status().owner.admission_retries > retries);
        let hidden = owner.committed_snapshot().is_none();
        let later = owner.ingress().try_submit(
            ConnectionId(1),
            CaptureInput::TimerPoll {
                now: TimestampNs(2),
            },
        );
        let refused = later.is_err();
        let mut raw = FeedReader::open_profile(&source, None, FeedProfile::EngineCausalV5).unwrap();
        let mut settled_before_ack = false;
        while let Some(record) = raw.read_next().unwrap() {
            if let FeedRecord::Causal(e) = record {
                if e.phase != CausalPhase::Bootstrap && e.route.session == 31 {
                    let id = OperationIdentity {
                        venue: e.route.venue,
                        session: e.route.session,
                        bootstrap_id: e.route.bootstrap_id.clone(),
                    };
                    settled_before_ack |= matches!(
                        decode_owner_history(&e, &id).unwrap().event,
                        OwnerEventDto::Wire(WireEventDto::Settled { .. })
                    );
                }
            }
        }
        assert!(
            !settled_before_ack,
            "must observe the safety window before the actual result ACK"
        );
        occupied.wait().unwrap();
        if let Some(future) = invocation.take() {
            let result = tokio::time::timeout(WAIT, future).await.unwrap().unwrap();
            assert_eq!(result.outcome, WireOutcomeDto::Failed);
            assert_eq!(
                result.detail,
                Some(WireErrorDto::Transport(TransportErrorDto::Protocol(
                    "exact delayed failure λ".into()
                )))
            );
        }
        wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
        assert_eq!(started.load(Ordering::SeqCst), 1);
        assert!(matches!(
            queued.wait(),
            InputSettlement::NotExecutedCommitted { .. }
        ));
        if let Ok(admission) = later {
            assert!(matches!(
                admission.wait(),
                InputSettlement::NotExecutedCommitted { .. }
            ));
        }
        assert!(owner.wire_progress().is_some_and(|(_, _, clean)| !clean));
        owner.shutdown();
        actor
            .handle()
            .try_submit(
                PreparedFeedTransaction::new(vec![FeedRecord::Seal {
                    schema_version: 1,
                    now_ns: 9,
                    reason: "deliberately invalid uncertain wire seal".into(),
                }])
                .unwrap(),
            )
            .unwrap()
            .wait()
            .unwrap();
        actor.shutdown().unwrap();
        let mut raw = FeedReader::open_profile(&source, None, FeedProfile::EngineCausalV5).unwrap();
        let mut record_count = 0;
        let mut revocation = None;
        let mut settlement = None;
        while let Some(record) = raw.read_next().unwrap() {
            record_count += 1;
            if let FeedRecord::Causal(e) = record {
                if e.phase != CausalPhase::Bootstrap && e.route.session == 31 {
                    let id = OperationIdentity {
                        venue: e.route.venue,
                        session: e.route.session,
                        bootstrap_id: e.route.bootstrap_id.clone(),
                    };
                    let dto = decode_owner_history(&e, &id).unwrap();
                    match dto.event {
                        OwnerEventDto::Revoked { .. } => revocation = Some(dto.route.ordinal),
                        OwnerEventDto::Wire(WireEventDto::Settled {
                            outcome, detail, ..
                        }) => {
                            assert!(settlement.is_none());
                            assert_eq!(
                                outcome,
                                if cancelled {
                                    WireOutcomeDto::Unknown
                                } else {
                                    WireOutcomeDto::Failed
                                }
                            );
                            if cancelled {
                                assert!(detail.is_none());
                            } else {
                                assert_eq!(
                                    detail,
                                    Some(WireErrorDto::Transport(TransportErrorDto::Protocol(
                                        "exact delayed failure λ".into()
                                    )))
                                );
                            }
                            settlement = Some(dto.route.ordinal);
                        }
                        _ => {}
                    }
                }
            }
        }
        let settled = settlement.expect("complete actual outcome must survive finalization");
        let revoked = revocation.expect("terminal observation must be retained");
        if !(hidden && refused && revoked < settled) {
            gaps.push((cancelled, hidden, refused, revoked, settled));
        }
        let mut replay =
            MultiHistoryReplay::open_profile(&source, replay_limits(), FeedProfile::EngineCausalV5)
                .unwrap();
        let mut rejected_seal = false;
        let mut valid_prefix = 0;
        loop {
            match replay.read_next() {
                Ok(Some(_)) => {
                    valid_prefix += 1;
                }
                Ok(None) => break,
                Err(error) => {
                    assert!(
                        error
                            .to_string()
                            .contains("seal with unsettled/uncertain wire effects"),
                        "specific wire Seal refusal: {error}"
                    );
                    rejected_seal = true;
                    break;
                }
            }
        }
        assert!(rejected_seal);
        assert_eq!(
            valid_prefix + 1,
            record_count,
            "only the deliberately appended Seal may fail"
        );
    }
    assert!(
        gaps.is_empty(),
        "observed outcome safety gaps before ACK: {gaps:?}"
    );
}
