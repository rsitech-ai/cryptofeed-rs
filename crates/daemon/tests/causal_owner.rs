//! Independently authored adversaries for the public journal-owner boundary.
//! Uses the complete native Bybit spot bundle; no private owner/test hooks.
#![cfg(feature = "causal-journal")]

use marketfeed_adapter_api::{DisconnectReason, HttpResponse};
use marketfeed_daemon::causal_bootstrap::{
    PreparedBootstrap, current_build,
    owner::{
        IngressAdmission, InputSettlement, JournalOwner, OwnerFailure, OwnerPhase, OwnerStartError,
    },
};
use marketfeed_engine::{CaptureConfig, CaptureInput, causal::*};
use marketfeed_model::*;
use marketfeed_recording::{shared_batch::*, shared_feed::*};
use std::{
    path::Path,
    sync::OnceLock,
    thread,
    time::{Duration, Instant},
};

const WATCHDOG: Duration = Duration::from_secs(10);

fn owner_policy() -> JournalOwnerPolicyDto {
    JournalOwnerPolicyDto {
        max_pending_operations: 16,
        max_pending_bytes: 2 * 1024 * 1024,
        max_input_bytes: 128 * 1024,
        max_operation_bytes: 256 * 1024,
        max_ingress_age: Duration::from_secs(8).into(),
        receipt_poll_interval: Duration::from_millis(1).into(),
    }
}

fn fixture(session: u64) -> BootstrapDto {
    static BUILD: OnceLock<BootstrapBuildDto> = OnceLock::new();
    let catalog = CatalogView::with_instruments(
        VenueId(6),
        CatalogVersion(9),
        ["BTCUSDT", "ETHUSDT"]
            .into_iter()
            .enumerate()
            .map(|(index, symbol)| Instrument {
                id: InstrumentId(index as u32 + 1),
                key: InstrumentKey {
                    venue: VenueCode("bybit-spot".into()),
                    native_symbol: symbol.into(),
                    kind: InstrumentKind::Spot,
                    settlement: None,
                    expiry_ns: None,
                },
                base: AssetCode(symbol[..3].into()),
                quote: AssetCode("USDT".into()),
                settlement: None,
                price_scale: 2,
                quantity_scale: 3,
                price_increment: Fixed::new(1, 2),
                quantity_increment: Fixed::new(1, 3),
                min_quantity: None,
                max_quantity: None,
                min_notional: None,
                contract_size: None,
                expiry_ns: None,
                status: InstrumentStatus::Active,
                inverse: false,
                catalog_version: CatalogVersion(9),
            })
            .collect::<Vec<_>>(),
    );
    let capture = CaptureConfig {
        venue: catalog.venue,
        session: SessionId(session),
        dispatch_capacity: 32,
        overflow: OverflowPolicy::FailEngine,
        max_bytes: 1024 * 1024,
        max_entries: 1024,
    };
    BootstrapDto {
        schema_version: 2,
        build: BUILD.get_or_init(|| current_build().unwrap()).clone(),
        factory: FactoryDto::BybitPublic {
            category: BybitCategoryDto::Spot,
            enable_l2: false,
        },
        environment: BootstrapEnvironmentDto::Production,
        output_profile: OutputProfileDto::DiskNullDispatchV1,
        adapter_policy: BybitEmissionPolicyDto::CompleteNativeBundleV1,
        // Explicit native topics independently specify the supported spot bundle.
        protocol_topics: topics(false),
        session: SessionSpecDto {
            endpoint_name: "wss://stream.bybit.com/v5/public/spot".into(),
            subscriptions: [1, 2]
                .into_iter()
                .flat_map(|instrument| {
                    [
                        ChannelDto::Trades,
                        ChannelDto::Quote,
                        ChannelDto::Statistics24h,
                    ]
                    .into_iter()
                    .map(move |channel| SubscriptionDto {
                        instrument,
                        channel,
                        delivery: DeliveryDto {
                            emit_book_snapshots: true,
                            emit_book_deltas: true,
                            emit_bbo: true,
                        },
                    })
                })
                .collect(),
        },
        catalog: (&catalog).into(),
        capture: capture.into(),
        runner_policy: capture.runtime_policy(),
        owner_policy: Some(owner_policy()),
        public_inputs: vec![PublicReproductionInputDto::new(
            "public-original-bytes",
            b"native\0\xff",
        )],
    }
}
fn topics(candles: bool) -> Vec<String> {
    ["BTCUSDT", "ETHUSDT"]
        .into_iter()
        .flat_map(|symbol| {
            let mut topics = vec![
                format!("publicTrade.{symbol}"),
                format!("orderbook.1.{symbol}"),
                format!("tickers.{symbol}"),
            ];
            if candles {
                topics.push(format!("kline.1.{symbol}"));
            }
            topics
        })
        .collect()
}

fn actor(path: &Path, age: Duration) -> FeedBatchActor {
    FeedBatchActor::spawn(
        FeedWriter::create_profile(path, 64 * 1024 * 1024, FeedProfile::EngineCausalV1).unwrap(),
        BatchConfig {
            max_pending_operations: 16,
            max_pending_bytes: 8 * 1024 * 1024,
            max_group_operations: 16,
            max_group_bytes: 8 * 1024 * 1024,
            max_age: age,
        },
    )
    .unwrap()
}

fn boot(
    dto: BootstrapDto,
    actor: &FeedBatchActor,
) -> marketfeed_daemon::causal_bootstrap::BootstrappedSession {
    PreparedBootstrap::new(dto, MAX_OPERATION_BYTES)
        .unwrap()
        .try_admit(&actor.handle())
        .unwrap()
        .wait()
        .unwrap()
        .instantiate()
        .unwrap()
}

fn start(dto: BootstrapDto, actor: &FeedBatchActor) -> (JournalOwner, OperationIdentity) {
    let boot = boot(dto, actor);
    let identity = boot.identity().clone();
    (boot.spawn_owner().unwrap(), identity)
}

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + WATCHDOG;
    while !condition() {
        assert!(Instant::now() < deadline, "public owner adversary watchdog");
        thread::sleep(Duration::from_millis(1));
    }
}

fn connected(connection: u64) -> CaptureInput {
    CaptureInput::Connected {
        connection: ConnectionId(connection),
        now: TimestampNs(1),
    }
}

fn stamp() -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(1_000_000_000),
        mono_ns: 1_000_000_000,
    }
}

fn trade() -> CaptureInput {
    CaptureInput::Text {
        payload: br#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1000,"data":[{"T":1000,"s":"BTCUSDT","S":"Buy","v":"0.001","p":"1.25","i":"independent-owner-trade","BT":false}]}"#.to_vec(),
        received: stamp(),
    }
}

fn stale_http() -> CaptureInput {
    CaptureInput::Http {
        request_id: 987,
        response: HttpResponse {
            status: 200,
            headers: vec![("x-public-evidence".into(), "original".into())],
            body: b"original-http-body\0\xff".to_vec().into(),
        },
        received: stamp(),
    }
}

fn close() -> CaptureInput {
    CaptureInput::Close {
        payload: b"remote-close".to_vec(),
        received: stamp(),
    }
}

fn next(owner: &JournalOwner) -> marketfeed_daemon::causal_bootstrap::owner::CommittedRelease {
    let mut result = None;
    wait_until(|| {
        result = owner.try_next();
        result.is_some()
    });
    result.unwrap()
}

fn committed(admission: &mut IngressAdmission) -> bool {
    match admission.try_result() {
        None => false,
        Some(InputSettlement::Committed { id, .. }) => {
            assert_eq!(id, admission.id);
            true
        }
        Some(other) => panic!("accepted input did not commit: {other:?}"),
    }
}

fn operations(path: &Path, identity: &OperationIdentity) -> Vec<OperationDto> {
    let mut reader = FeedReader::open_profile(path, None, FeedProfile::EngineCausalV1).unwrap();
    let mut result = Vec::new();
    while let Some(record) = reader.read_next().unwrap() {
        match record {
            FeedRecord::Causal(envelope) if envelope.phase == CausalPhase::Bootstrap => {
                let dto = decode_bootstrap(&envelope).unwrap();
                assert_eq!(dto.schema_version, 2);
                assert_eq!(dto.owner_policy, Some(owner_policy()));
            }
            FeedRecord::Causal(envelope) => {
                result.push(decode_operation(&envelope, identity).unwrap())
            }
            other => panic!("unexpected compatibility record: {other:?}"),
        }
    }
    result
}

#[derive(Clone, Copy, Debug)]
enum Safety {
    Close,
    TransportLost,
    Stop,
}
impl Safety {
    fn input(self) -> CaptureInput {
        match self {
            Self::Close => close(),
            Self::TransportLost => CaptureInput::TransportLost {
                reason: DisconnectReason::TransportError,
                now: TimestampNs(2),
            },
            Self::Stop => CaptureInput::Stop,
        }
    }
    fn matches(self, input: &InputDto) -> bool {
        matches!(
            (self, input),
            (Self::Close, InputDto::Close { .. })
                | (Self::TransportLost, InputDto::TransportLost { .. })
                | (Self::Stop, InputDto::Stop)
        )
    }
}

fn safety_fences_delayed_ack(safety: Safety) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(500));
    let (owner, identity) = start(fixture(71), &actor);
    let ingress = owner.ingress();
    let mut first = ingress.try_submit(ConnectionId(41), connected(41)).unwrap();
    wait_until(|| owner.status().phase == OwnerPhase::AwaitingCommit);
    assert!(
        first.try_result().is_none(),
        "must actually exercise delayed durability"
    );
    let mut older = ingress.try_submit(ConnectionId(41), trade()).unwrap();
    let mut transition = ingress
        .try_submit(ConnectionId(41), safety.input())
        .unwrap();
    assert!(owner.committed_snapshot().is_none(), "{safety:?}");
    assert!(
        owner.try_next().is_none(),
        "older handoff survived observed {safety:?}"
    );
    // Arrival, not eventual callback execution, closes the source. Rejection
    // must preserve the original payload/stamp/HTTP identity for its caller.
    for input in [trade(), stale_http()] {
        let original = input.clone();
        let rejected = ingress.try_submit(ConnectionId(41), input).unwrap_err();
        assert_eq!(rejected.reason, OwnerFailure::SourceConnection);
        assert_eq!(rejected.connection, ConnectionId(41));
        assert_eq!(rejected.input, original);
    }
    wait_until(|| {
        assert!(
            owner.committed_snapshot().is_none(),
            "snapshot revived after {safety:?}"
        );
        if let Some(release) = owner.try_next() {
            assert_eq!(
                release.input_id(),
                transition.id,
                "older turn handed off after {safety:?}"
            );
            assert!(safety.matches(&release.evidence().input));
            assert!(!release.outputs_eligible());
            assert_eq!(release.released_items().count(), 0);
        }
        committed(&mut first)
            && committed(&mut older)
            && committed(&mut transition)
            && owner.status().durable == 3
    });
    assert!(owner.committed_snapshot().is_none());
    let status = owner.shutdown();
    assert_eq!(
        (status.accepted, status.executed, status.durable),
        (3, 3, 3)
    );
    assert_eq!(status.rejected_stale, 2);
    assert!(status.withheld >= 2);
    assert_eq!((status.pending_operations, status.pending_bytes), (0, 0));
    actor.shutdown().unwrap();
    let rows = operations(&path, &identity);
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows.iter().map(|r| r.route.ordinal).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(rows.iter().all(|r| r.route.generation == 1));
    assert!(matches!(
        rows[0].input,
        InputDto::Connected { connection: 41, .. }
    ));
    assert!(matches!(rows[1].input, InputDto::Text { .. }));
    assert!(
        safety.matches(&rows[2].input),
        "safety transition must still be durable"
    );
}

#[test]
fn observed_close_fences_an_older_ack_and_rejects_text_and_http() {
    safety_fences_delayed_ack(Safety::Close);
}

#[test]
fn startup_fence_preserves_multiple_future_inputs_queued_before_its_ack() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(500));
    let (owner, identity) = start(fixture(76), &actor);
    let ingress = owner.ingress();
    let mut admissions = vec![ingress.try_submit(ConnectionId(1), connected(1)).unwrap()];
    wait_until(|| owner.status().phase == OwnerPhase::AwaitingCommit);
    assert!(admissions[0].try_result().is_none());
    assert!(owner.try_next().is_none());
    assert!(owner.committed_snapshot().is_none());

    let mut original_payloads = Vec::new();
    for index in 0..3 {
        let CaptureInput::Text { payload, received } = trade() else {
            unreachable!()
        };
        let payload = String::from_utf8(payload)
            .unwrap()
            .replace(
                "independent-owner-trade",
                &format!("independent-future-trade-{index}"),
            )
            .into_bytes();
        original_payloads.push(payload.clone());
        admissions.push(
            ingress
                .try_submit(ConnectionId(1), CaptureInput::Text { payload, received })
                .unwrap(),
        );
    }
    // Establish the adversary rather than accepting an ordinary serial run:
    // every future input was admitted before the startup receipt was observed.
    assert!(admissions[0].try_result().is_none());
    assert_eq!(owner.status().accepted, 4);

    for (index, admission) in admissions.iter_mut().enumerate() {
        let release = next(&owner);
        assert_eq!(release.input_id(), index as u64 + 1);
        assert_eq!(release.evidence().route.ordinal, index as u64 + 1);
        assert_eq!(release.evidence().route.generation, 1);
        assert_eq!(release.evidence().after.connection, 1);
        assert!(release.outputs_eligible());
        assert!(committed(admission));
        if index == 0 {
            assert!(
                release
                    .released_items()
                    .any(|item| matches!(item, ItemDto::Write(_)))
            );
        } else {
            assert!(
                release
                    .released_items()
                    .any(|item| matches!(item, ItemDto::Market(_)))
            );
        }
    }
    let status = owner.shutdown();
    assert_eq!(
        (
            status.accepted,
            status.executed,
            status.durable,
            status.handed_off
        ),
        (4, 4, 4, 4)
    );
    assert_eq!(
        (status.withheld, status.abandoned, status.stale_accepted),
        (0, 0, 0)
    );
    assert_eq!(status.failure, Some(OwnerFailure::Revoked));
    actor.shutdown().unwrap();
    let rows = operations(&path, &identity);
    assert_eq!(rows.len(), 4);
    assert_eq!(
        rows.iter().map(|row| row.route.ordinal).collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    for (row, original) in rows[1..].iter().zip(original_payloads) {
        let InputDto::Text { payload, .. } = &row.input else {
            panic!("future trade was not journaled as the accepted input")
        };
        assert_eq!(payload.bytes().unwrap(), original);
    }
}

#[test]
fn observed_transport_loss_fences_an_older_ack_and_rejects_text_and_http() {
    safety_fences_delayed_ack(Safety::TransportLost);
}
#[test]
fn observed_stop_fences_an_older_ack_and_still_journals_the_stop() {
    safety_fences_delayed_ack(Safety::Stop);
}

#[test]
fn observed_close_immediately_removes_an_already_committed_release_and_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(10));
    let (owner, identity) = start(fixture(72), &actor);
    let ingress = owner.ingress();
    let mut connected = ingress.try_submit(ConnectionId(5), connected(5)).unwrap();
    wait_until(|| owner.status().phase == OwnerPhase::ReleasePending);
    assert!(committed(&mut connected));
    assert_eq!(owner.committed_snapshot().unwrap().route.ordinal, 1);
    // Do not take the older subscribe/write release. Close must discard it
    // synchronously while permitting the close operation itself to be journaled.
    let mut close = ingress.try_submit(ConnectionId(5), close()).unwrap();
    assert!(owner.committed_snapshot().is_none());
    if let Some(release) = owner.try_next() {
        assert_eq!(release.input_id(), close.id);
        assert!(!release.outputs_eligible());
    }
    wait_until(|| {
        assert!(owner.committed_snapshot().is_none());
        if let Some(release) = owner.try_next() {
            assert_eq!(release.input_id(), close.id);
            assert!(!release.outputs_eligible());
            assert_eq!(release.released_items().count(), 0);
        }
        committed(&mut close) && owner.status().durable == 2
    });
    let status = owner.shutdown();
    assert!(status.withheld >= 1);
    actor.shutdown().unwrap();
    let rows = operations(&path, &identity);
    assert_eq!(rows.len(), 2);
    assert!(matches!(rows[1].input, InputDto::Close { .. }));
}

#[test]
fn connection_identity_is_monotonic_while_generation_counts_accepted_reconnects() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = actor(&path, Duration::from_millis(2));
    let (owner, identity) = start(fixture(73), &actor);
    let ingress = owner.ingress();
    ingress.try_submit(ConnectionId(40), connected(40)).unwrap();
    let first = next(&owner);
    assert_eq!(
        (
            first.evidence().route.generation,
            first.evidence().after.connection
        ),
        (1, 40)
    );
    for bad in [0, 39, 40] {
        let rejected = ingress
            .try_submit(ConnectionId(bad), connected(bad))
            .unwrap_err();
        assert_eq!(rejected.reason, OwnerFailure::SourceConnection);
    }
    let mut close = ingress.try_submit(ConnectionId(40), close()).unwrap();
    for input in [trade(), stale_http()] {
        assert_eq!(
            ingress
                .try_submit(ConnectionId(40), input)
                .unwrap_err()
                .reason,
            OwnerFailure::SourceConnection
        );
    }
    ingress.try_submit(ConnectionId(41), connected(41)).unwrap();
    let second = next(&owner);
    assert_eq!(
        (
            second.evidence().route.generation,
            second.evidence().after.connection
        ),
        (2, 41)
    );
    assert_eq!(second.input_id(), 3);
    assert!(committed(&mut close));
    assert!(owner.status().failure.is_none());
    ingress.try_submit(ConnectionId(41), trade()).unwrap();
    let trade = next(&owner);
    assert_eq!(trade.evidence().route.generation, 2);
    assert!(
        trade
            .released_items()
            .any(|item| matches!(item, ItemDto::Market(_)))
    );
    ingress
        .try_submit(ConnectionId(u64::MAX), connected(u64::MAX))
        .unwrap();
    let last = next(&owner);
    assert_eq!(
        (
            last.evidence().route.generation,
            last.evidence().after.connection
        ),
        (3, u64::MAX)
    );
    for bad in [0, 41, u64::MAX] {
        assert_eq!(
            ingress
                .try_submit(ConnectionId(bad), connected(bad))
                .unwrap_err()
                .reason,
            OwnerFailure::SourceConnection
        );
    }
    assert!(owner.status().failure.is_none());
    let status = owner.shutdown();
    assert_eq!((status.executed, status.durable), (5, 5));
    actor.shutdown().unwrap();
    let rows = operations(&path, &identity);
    assert_eq!(
        rows.iter().map(|r| r.route.generation).collect::<Vec<_>>(),
        vec![1, 1, 2, 2, 3]
    );
}

#[test]
fn legacy_v1_inspection_cannot_silently_instantiate_a_journal_owner() {
    let mut dto = fixture(74);
    dto.schema_version = 1;
    dto.owner_policy = None;
    let envelope = encode_bootstrap(&dto, MAX_OPERATION_BYTES).unwrap();
    assert_eq!(decode_bootstrap(&envelope).unwrap(), dto);
    let temp = tempfile::tempdir().unwrap();
    let actor = actor(&temp.path().join("feed"), Duration::from_millis(2));
    assert!(matches!(
        boot(dto, &actor).spawn_owner(),
        Err(OwnerStartError::MissingPolicy)
    ));
    actor.shutdown().unwrap();
}

#[test]
fn v2_binds_the_owner_policy_and_preserves_the_full_native_bundle() {
    let original = fixture(75);
    let first = encode_bootstrap(&original, MAX_OPERATION_BYTES).unwrap();
    let mut changed = original.clone();
    changed
        .owner_policy
        .as_mut()
        .unwrap()
        .max_pending_operations -= 1;
    let second = encode_bootstrap(&changed, MAX_OPERATION_BYTES).unwrap();
    assert_ne!(first.route.bootstrap_id, second.route.bootstrap_id);
    assert_eq!(
        decode_bootstrap(&first).unwrap().owner_policy,
        Some(owner_policy())
    );
    assert_eq!(
        original.adapter_policy,
        BybitEmissionPolicyDto::CompleteNativeBundleV1
    );
    assert_eq!(
        original.output_profile,
        OutputProfileDto::DiskNullDispatchV1
    );
    assert_eq!(original.protocol_topics, topics(false));
    assert_eq!(original.session.subscriptions.len(), 6);
    let mut mismatched = original.clone();
    mismatched.schema_version = 1;
    assert!(PreparedBootstrap::new(mismatched, MAX_OPERATION_BYTES).is_err());
    changed.owner_policy = None;
    assert!(PreparedBootstrap::new(changed, MAX_OPERATION_BYTES).is_err());
}
