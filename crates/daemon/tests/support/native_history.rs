use super::*;
use marketfeed_daemon::causal_bootstrap::owner::CommittedRelease;
use marketfeed_daemon::causal_bootstrap::replay::{HistoryReplay, MultiHistoryReplay, ReplayError};
use std::path::Path;

const PROFILE: FeedProfile = FeedProfile::EngineCausalV4;
fn layout() -> ObservedBatchLimits {
    ObservedBatchLimits {
        layout: NativeObservationLimits {
            rows: 64,
            associations: 128,
            book_levels: 40,
            mids: 0,
            users: 128,
            string_bytes: 4096,
        },
        encoded_bytes: 64 << 10,
    }
}
fn reader_limits() -> OwnerHistoryLimits {
    OwnerHistoryLimits {
        max_retained_input_bytes: 4 << 20,
        max_input_bytes: 256 << 10,
        max_record_bytes: 256 << 10,
    }
}
pub(super) fn native_fixture(spot: bool) -> BootstrapDto {
    let mut dto = fixture(spot);
    dto.bind_observation_policy(NativeObservationPolicyDto::HyperliquidV1 { limits: layout() })
        .unwrap();
    dto
}
fn start(dto: BootstrapDto, actor: &FeedBatchActor) -> (HistoryJournalOwner, OperationIdentity) {
    let committed = PreparedBootstrap::new_observed_history(dto, MAX_OPERATION_BYTES)
        .unwrap()
        .try_admit(&actor.handle())
        .unwrap()
        .wait()
        .unwrap();
    let session = committed.instantiate().unwrap();
    let id = session.identity().clone();
    (session.spawn_history_owner().unwrap(), id)
}
fn actor(path: &Path) -> FeedBatchActor {
    FeedBatchActor::spawn(
        FeedWriter::create_profile(path, 64 << 20, PROFILE).unwrap(),
        BatchConfig {
            max_pending_operations: 32,
            max_pending_bytes: 8 << 20,
            max_group_operations: 32,
            max_group_bytes: 8 << 20,
            max_age: Duration::from_millis(1),
        },
    )
    .unwrap()
}
fn finish(owner: HistoryJournalOwner) {
    owner.close_ingress();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    assert!(owner.shutdown().owner.failure.is_none());
}
fn submit(owner: &HistoryJournalOwner, connection: u64, input: CaptureInput) -> CommittedRelease {
    assert!(matches!(
        owner
            .ingress()
            .try_submit(ConnectionId(connection), input)
            .unwrap()
            .wait(),
        InputSettlement::Committed { .. }
    ));
    let mut release = None;
    wait(|| {
        release = owner.try_next();
        release.is_some()
    });
    // This is the owner's internal handoff, before its observation ACK. Only
    // the committed reader below may authorize projected market observations.
    release.unwrap()
}
fn read(path: &Path) -> Vec<FeedRecord> {
    let mut reader = FeedReader::open_profile(path, None, PROFILE).unwrap();
    let mut records = Vec::new();
    while let Some(record) = reader.read_next().unwrap() {
        records.push(record);
    }
    records
}
fn write(path: &Path, records: &[FeedRecord]) {
    let mut writer = FeedWriter::create_profile(path, 64 << 20, PROFILE).unwrap();
    writer.append_many(records.to_vec()).unwrap();
}
fn observed_inputs(dto: &BootstrapDto) -> Vec<(u64, CaptureInput)> {
    let coin = &dto.catalog.instruments[0].key.native_symbol;
    let mut result = inputs(dto);
    for (_, input) in &mut result {
        let CaptureInput::Text { payload, .. } = input else {
            continue;
        };
        let text = String::from_utf8(payload.clone()).unwrap();
        *payload = if text.contains("\"channel\":\"trades\"") {
            format!(r#"{{"channel":"trades","data":[{{"coin":"{coin}","side":"B","px":"9007199254740993.123456789012345678","sz":"0.00000000000000000001","time":1,"tid":18446744073709551615,"hash":"hash-1","users":["public-1","public-2"]}},{{"coin":"{coin}","side":"A","px":"2.00","sz":"3","time":2,"tid":18446744073709551615,"hash":"hash-2"}}]}}"#).into_bytes()
        } else if text.contains("\"channel\":\"candle\"") {
            format!(r#"{{"channel":"candle","data":{{"t":0,"T":17,"s":"{coin}","i":"1m","o":"10","h":"11","l":"9","c":"10.1","v":"123.45","n":18446744073709551615}}}}"#).into_bytes()
        } else if text.contains("\"channel\":\"activeAssetCtx\"") {
            format!(r#"{{"channel":"activeAssetCtx","data":{{"coin":"{coin}","ctx":{{"dayNtlVlm":"100.01","prevDayPx":"9","markPx":"10.02","midPx":null,"circulatingSupply":"0.00","funding":"-0.00000125","openInterest":"12345.12345","oraclePx":"10.03","dayBaseVlm":"4.00","premium":"-0.25","impactPxs":["10","11"]}}}}}}"#).into_bytes()
        } else {
            payload.clone()
        };
    }
    // Add a native-only unit after all five families, then a reconnect. Its
    // regressing receive clock must never be replaced by the projector's clock.
    let position = result
        .iter()
        .position(|(_, input)| matches!(input, CaptureInput::TransportLost { .. }))
        .unwrap();
    result.insert(position, (1, CaptureInput::Text {
        payload: format!(r#"{{"channel":"bbo","data":{{"coin":"{coin}","time":2,"bbo":[null,{{"px":"11","sz":"2","n":18446744073709551615}}]}}}}"#).into_bytes(),
        received: FrameStamp { receive_ts: TimestampNs(-123), mono_ns: u64::MAX },
    }));
    result
}
pub(super) fn recorded(spot: bool) -> (tempfile::TempDir, Vec<FeedRecord>, Vec<OperationDto>) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("original");
    let dto = native_fixture(spot);
    let mut reference = CaptureSession::new(
        Box::new(
            marketfeed_adapter_hyperliquid::HyperliquidSession::new_with_observations(
                spot,
                dto.session.session_spec().unwrap().subscriptions,
                dto.catalog.catalog().unwrap(),
                layout(),
            )
            .unwrap(),
        ),
        dto.capture.config().unwrap(),
    )
    .unwrap();
    let actor = actor(&path);
    let (owner, identity) = start(dto.clone(), &actor);
    let mut expected = Vec::new();
    for (connection, input) in observed_inputs(&dto) {
        let native = reference
            .execute_turn(input.clone(), CaptureDrainPolicy::NullDispatchTakeOutbound)
            .unwrap();
        let expected_op = OperationDto::from_observed_capture(&native, &identity).unwrap();
        let release = submit(&owner, connection, input);
        assert_eq!(release.evidence(), &expected_op);
        expected.push(expected_op);
    }
    finish(owner);
    actor
        .handle()
        .try_submit(
            PreparedFeedTransaction::new(vec![FeedRecord::Seal {
                schema_version: 1,
                now_ns: 999,
                reason: "closed".into(),
            }])
            .unwrap(),
        )
        .unwrap()
        .wait()
        .unwrap();
    actor.shutdown().unwrap();
    (temp, read(&path), expected)
}

#[test]
fn native_actual_spot_perps_persist_all_fields_and_every_physical_resume() {
    for spot in [true, false] {
        let (temp, records, expected) = recorded(spot);
        let path = temp.path().join("original");
        assert!(HistoryReplay::open(&path, reader_limits()).is_err());
        assert!(MultiHistoryReplay::open(&path, multi_owner_tests::limits()).is_err());
        let mut replay = HistoryReplay::open_profile(&path, reader_limits(), PROFILE).unwrap();
        let mut steps = Vec::new();
        let mut progress = Vec::new();
        let mut completed = Vec::new();
        while let Some(step) = replay.read_next().unwrap() {
            let record = &records[(step.cursor.sequence - 1) as usize];
            if let FeedRecord::Causal(envelope) = record {
                let event = decode_owner_history(envelope, &identity_from(&records)).unwrap();
                assert_eq!(
                    step.handoff.is_some(),
                    matches!(event.event, OwnerEventDto::HandoffObserved { .. })
                );
            }
            if let Some(handoff) = &step.handoff {
                completed.push((*handoff.operation).clone());
            }
            steps.push(step);
            progress.push(replay.progress().unwrap());
        }
        assert_eq!(completed, expected);
        assert!(replay.progress().unwrap().sealed);
        assert!(replay.progress().unwrap().uncertainties.is_empty());
        for (index, step) in steps.iter().enumerate() {
            let mut resumed =
                HistoryReplay::resume_profile(&path, step.cursor.clone(), reader_limits(), PROFILE)
                    .unwrap();
            assert_eq!(resumed.progress().unwrap(), progress[index]);
            assert_eq!(resumed.read_next().unwrap(), steps.get(index + 1).cloned());
        }
        let batches: Vec<_> = steps
            .iter()
            .filter_map(|step| step.handoff.as_ref())
            .flat_map(|handoff| handoff.released_items())
            .filter_map(|item| match item {
                ItemDto::ObservedMarket(leaf) => Some(leaf.batch().unwrap()),
                _ => None,
            })
            .collect();
        assert_eq!(batches.len(), 6);
        let all: Vec<_> = batches.iter().flat_map(|b| &b.native().rows).collect();
        let trade = all
            .iter()
            .find_map(|r| match &r.observation {
                NativeObservation::HyperliquidTrade(t) => Some(t),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            trade.price,
            Fixed::new(9007199254740993123456789012345678, 18)
        );
        assert_eq!(
            (trade.tid, trade.hash.as_str(), trade.users.len()),
            (u64::MAX, "hash-1", 2)
        );
        let candle = all
            .iter()
            .find_map(|r| match &r.observation {
                NativeObservation::HyperliquidCandle(c) => Some(c),
                _ => None,
            })
            .unwrap();
        assert_eq!((candle.end_ms, candle.trades), (17, u64::MAX));
        let ctx = all
            .iter()
            .find_map(|r| match &r.observation {
                NativeObservation::HyperliquidContext(c) => Some(c),
                _ => None,
            })
            .unwrap();
        assert_eq!(ctx.circulating_supply, Some(Fixed::new(0, 2)));
        assert_eq!(ctx.mid_price, None);
        assert_eq!(ctx.premium, Some(Fixed::new(-25, 2)));
        assert_eq!(
            ctx.impact_prices,
            Some([Fixed::new(10, 0), Fixed::new(11, 0)])
        );
        let empty = batches
            .iter()
            .find(|b| b.canonical().events.is_empty())
            .unwrap();
        assert_eq!(
            empty.source(),
            ObservedBatchSource {
                venue: if spot { VenueId(21) } else { VenueId(22) },
                connection: ConnectionId(1),
                received: FrameStamp {
                    receive_ts: TimestampNs(-123),
                    mono_ns: u64::MAX
                }
            }
        );
        assert!(empty.native().rows[0].canonical_positions.is_empty());
    }
}
fn identity_from(records: &[FeedRecord]) -> OperationIdentity {
    let FeedRecord::Causal(envelope) = &records[0] else {
        panic!()
    };
    OperationIdentity {
        venue: envelope.route.venue,
        session: envelope.route.session,
        bootstrap_id: envelope.route.bootstrap_id.clone(),
    }
}

#[test]
fn native_bootstrap_rejects_missing_duplicate_wrong_or_legacy_policy_before_admission() {
    let valid = native_fixture(false);
    assert!(PreparedBootstrap::new_history(valid.clone(), MAX_OPERATION_BYTES).is_err());
    assert!(PreparedBootstrap::new_observed_history(fixture(false), MAX_OPERATION_BYTES).is_err());
    let row = valid
        .public_inputs
        .iter()
        .position(|p| p.role == NATIVE_OBSERVATION_POLICY_ROLE)
        .unwrap();
    for index in 0..5 {
        let mut dto = valid.clone();
        match index {
            0 => dto.public_inputs.push(dto.public_inputs[row].clone()),
            1 => {
                dto.public_inputs[row] = PublicReproductionInputDto::new(
                    NATIVE_OBSERVATION_POLICY_ROLE,
                    &serde_json::to_vec(&NativeObservationPolicyDto::CanonicalOnlyV1).unwrap(),
                )
            }
            2 => dto.public_inputs[row].sha256 = "a".repeat(64),
            3 => {
                let mut bytes = dto.public_inputs[row].payload.bytes().unwrap();
                bytes.push(b' ');
                dto.public_inputs[row] =
                    PublicReproductionInputDto::new(NATIVE_OBSERVATION_POLICY_ROLE, &bytes);
            }
            _ => {
                dto.public_inputs[row] = PublicReproductionInputDto::new(
                    NATIVE_OBSERVATION_POLICY_ROLE,
                    &serde_json::to_vec(&NativeObservationPolicyDto::HyperliquidV1 {
                        limits: ObservedBatchLimits {
                            encoded_bytes: 17 << 20,
                            ..layout()
                        },
                    })
                    .unwrap(),
                )
            }
        }
        assert!(
            PreparedBootstrap::new_observed_history(dto, MAX_OPERATION_BYTES).is_err(),
            "mutation {index}"
        );
    }
}

#[test]
fn native_only_field_mutations_pass_structure_but_poison_actual_replay() {
    let (temp, original, _) = recorded(false);
    for family in 0..4 {
        let mut records = original.clone();
        let mut changed = false;
        for record in &mut records {
            let FeedRecord::Causal(envelope) = record else {
                continue;
            };
            if envelope.phase != CausalPhase::Operation {
                continue;
            }
            let identity = identity_from(&original);
            let mut dto = decode_owner_history(envelope, &identity).unwrap();
            let OwnerEventDto::ExecutionCompleted { operation, .. } = &mut dto.event else {
                continue;
            };
            for entry in &mut operation.entries {
                let EffectDto::QueueDrain { items, .. } = &mut entry.effect else {
                    continue;
                };
                for item in items {
                    let ItemDto::ObservedMarket(leaf) = item else {
                        continue;
                    };
                    let batch = leaf.batch().unwrap();
                    let mut native = batch.native().clone();
                    let found =
                        native
                            .rows
                            .iter_mut()
                            .any(|row| match (&mut row.observation, family) {
                                (NativeObservation::HyperliquidTrade(t), 0) => {
                                    t.hash.push_str("-changed");
                                    true
                                }
                                (NativeObservation::HyperliquidBook(b), 1) => {
                                    b.sides[0][0].orders += 1;
                                    true
                                }
                                (NativeObservation::HyperliquidCandle(c), 2) => {
                                    c.end_ms += 1;
                                    true
                                }
                                (NativeObservation::HyperliquidContext(c), 3) => {
                                    c.mid_price = Some(Fixed::new(1003, 2));
                                    true
                                }
                                _ => false,
                            });
                    if found {
                        let mutated = ObservedEventBatch::try_new(
                            batch.canonical().clone(),
                            native,
                            batch.source(),
                            batch.limits(),
                        )
                        .unwrap();
                        *leaf = serde_json::from_str(
                            &serde_json::to_string(&serde_json::to_string(&mutated).unwrap())
                                .unwrap(),
                        )
                        .unwrap();
                        changed = true;
                        break;
                    }
                }
                if changed {
                    break;
                }
            }
            if changed {
                *envelope = encode_owner_history(&dto, &identity, MAX_OPERATION_BYTES).unwrap();
                break;
            }
        }
        assert!(changed);
        let identity = identity_from(&records);
        let FeedRecord::Causal(bootstrap) = &records[0] else {
            panic!()
        };
        let mut structural = OwnerHistory::from_bootstrap(bootstrap, reader_limits()).unwrap();
        for record in &records[1..] {
            if let FeedRecord::Causal(e) = record {
                structural.apply(e).unwrap();
            }
        }
        assert!(structural.uncertainties().is_empty());
        let path = temp.path().join(format!("mutated-{family}"));
        write(&path, &records);
        let mut replay = HistoryReplay::open_profile(&path, reader_limits(), PROFILE).unwrap();
        loop {
            match replay.read_next() {
                Err(ReplayError::NativeMismatch { .. }) => break,
                Ok(Some(_)) => {}
                other => panic!("expected native mismatch: {other:?} {identity:?}"),
            }
        }
        assert!(replay.is_poisoned());
        assert!(replay.read_next().is_err());
    }
}

#[test]
fn native_multi_owner_negotiates_spot_perps_and_canonical_bybit_without_crossing_scope() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("mixed");
    let actor = actor(&path);
    let spot = native_fixture(true);
    let mut perps = native_fixture(false);
    perps.capture.session = 32;
    let mut bybit = multi_owner_tests::bybit_fixture();
    bybit
        .bind_observation_policy(NativeObservationPolicyDto::CanonicalOnlyV1)
        .unwrap();
    let mut owners = Vec::new();
    for dto in [spot.clone(), perps.clone(), bybit] {
        owners.push(start(dto, &actor).0);
    }
    for owner in &owners {
        submit(
            owner,
            1,
            CaptureInput::Connected {
                connection: ConnectionId(1),
                now: TimestampNs(100),
            },
        );
    }
    for (owner, dto) in owners.iter().zip([&spot, &perps]) {
        let coin = &dto.catalog.instruments[0].key.native_symbol;
        submit(
            owner,
            1,
            text(
                format!(
                    r#"{{"channel":"bbo","data":{{"coin":"{coin}","time":1,"bbo":[null,null]}}}}"#
                ),
                -1,
            ),
        );
    }
    submit(&owners[2], 1, text(r#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1,"data":[{"T":1,"s":"BTCUSDT","S":"Buy","v":"1","p":"10","i":"bybit-1","BT":false}]}"#.into(), 10));
    for owner in owners {
        finish(owner);
    }
    actor.shutdown().unwrap();
    let mut replay =
        MultiHistoryReplay::open_profile(&path, multi_owner_tests::limits(), PROFILE).unwrap();
    let mut native = Vec::new();
    let mut canonical_bybit = false;
    while let Some(step) = replay.read_next().unwrap() {
        if let Some(handoff) = step.handoff {
            assert_eq!(handoff.operation.schema_version, 2);
            for item in handoff.released_items() {
                match item {
                    ItemDto::ObservedMarket(leaf) => {
                        native.push(leaf.batch().unwrap().source().venue)
                    }
                    ItemDto::Market(leaf) if handoff.operation.route.session == 33 => {
                        canonical_bybit |= matches!(leaf.record().unwrap(), FeedRecord::Batch(b) if b.events.iter().any(|e| matches!(e.payload, MarketEvent::Trade(_))));
                    }
                    _ => {}
                }
            }
        }
    }
    native.sort_by_key(|v| v.0);
    assert_eq!(native, [VenueId(21), VenueId(22)]);
    assert!(canonical_bybit);
    assert_eq!(replay.progress().unwrap().owners.len(), 3);
}
