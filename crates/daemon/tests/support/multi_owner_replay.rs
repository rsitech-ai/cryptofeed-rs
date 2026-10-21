use super::*;
use marketfeed_daemon::causal_bootstrap::replay::{
    HistoryReplay, MultiHistoryReplay, ReplayError, ReplayResources,
};
use std::{
    collections::{BTreeMap, VecDeque},
    path::Path,
};

pub(super) use super::hyperliquid_fixture::replay_limits as limits;

#[test]
fn multi_replay_bootstraps_physically_and_retains_independent_idle_owners() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("owners");
    let actor = FeedBatchActor::spawn(
        FeedWriter::create_profile(&path, 64 << 20, FeedProfile::EngineCausalV3).unwrap(),
        BatchConfig {
            max_pending_operations: 8,
            max_pending_bytes: 8 << 20,
            max_group_operations: 8,
            max_group_bytes: 8 << 20,
            max_age: Duration::from_millis(1),
        },
    )
    .unwrap();
    let mut replay = MultiHistoryReplay::open(&path, limits()).unwrap();
    let origin = replay.progress().unwrap();
    assert!(origin.owners.is_empty());
    assert_eq!(origin.resources, ReplayResources::default());
    let mut resumed = MultiHistoryReplay::resume(&path, origin.cursor, limits()).unwrap();
    assert!(resumed.read_next().unwrap().is_none());
    for (spot, session) in [(true, 31), (false, 32)] {
        let mut dto = fixture(spot);
        dto.capture.session = session;
        let committed = PreparedBootstrap::new_history(dto, MAX_OPERATION_BYTES)
            .unwrap()
            .try_admit(&actor.handle())
            .unwrap()
            .wait()
            .unwrap();
        let expected = committed.instantiate().unwrap().identity().clone();
        let step = replay.read_next().unwrap().unwrap();
        assert_eq!(step.owner, Some(expected));
        assert!(step.handoff.is_none());
        let progress = replay.progress().unwrap();
        assert_eq!(progress.owners.len(), (session - 30) as usize);
        assert!(progress.owners.iter().all(|o| o.native_ordinal == 0));
        assert_eq!(
            progress.resources.selected_instruments,
            progress.owners.len()
        );
        assert_eq!(
            progress.resources.maintained_book_levels,
            40 * progress.owners.len()
        );
        assert_eq!(progress.resources.cached_book_levels, 0);
    }
    actor
        .handle()
        .try_submit(
            PreparedFeedTransaction::new(vec![FeedRecord::Seal {
                schema_version: 1,
                now_ns: 99,
                reason: "idle owner test closure".into(),
            }])
            .unwrap(),
        )
        .unwrap()
        .wait()
        .unwrap();
    actor.shutdown().unwrap();
    while replay.read_next().unwrap().is_some() {}
    assert!(replay.progress().unwrap().sealed);
}

#[test]
fn multi_replay_checks_aggregate_bootstrap_capture_reservation_before_second_factory() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("limited");
    let mut writer =
        FeedWriter::create_profile(&path, 64 << 20, FeedProfile::EngineCausalV3).unwrap();
    for (spot, session) in [(true, 31), (false, 32)] {
        let mut dto = fixture(spot);
        dto.capture.session = session;
        let envelope =
            encode_bootstrap_for_profile(&dto, MAX_OPERATION_BYTES, FeedProfile::EngineCausalV3)
                .unwrap();
        writer.append(FeedRecord::Causal(envelope)).unwrap();
    }
    let mut cap = limits();
    cap.maximum.capture_trace_reservation_bytes = 1 << 20;
    let mut replay = MultiHistoryReplay::open(&path, cap).unwrap();
    assert!(replay.read_next().unwrap().is_some());
    assert_eq!(
        replay
            .progress()
            .unwrap()
            .resources
            .capture_trace_reservation_bytes,
        1 << 20
    );
    assert!(replay.read_next().is_err());
    assert!(replay.is_poisoned());
    assert!(replay.progress().is_err());
    assert!(replay.read_next().is_err());
}

pub(super) fn bybit_fixture() -> BootstrapDto {
    let mut dto = fixture(true);
    let catalog = CatalogView::with_instruments(
        VenueId(6),
        CatalogVersion(7),
        vec![Instrument {
            id: InstrumentId(1),
            key: InstrumentKey {
                venue: VenueCode("bybit-spot".into()),
                native_symbol: "BTCUSDT".into(),
                kind: InstrumentKind::Spot,
                settlement: None,
                expiry_ns: None,
            },
            base: AssetCode("BTC".into()),
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
            catalog_version: CatalogVersion(7),
        }],
    );
    dto.factory = FactoryDto::BybitPublic {
        category: BybitCategoryDto::Spot,
        enable_l2: false,
    };
    dto.adapter_policy = AdapterEmissionPolicyDto::CompleteNativeBundleV1;
    dto.capture.venue = 6;
    dto.capture.session = 33;
    dto.catalog = (&catalog).into();
    dto.session = SessionSpecDto {
        endpoint_name: "wss://stream.bybit.com/v5/public/spot".into(),
        subscriptions: [
            ChannelDto::Trades,
            ChannelDto::Quote,
            ChannelDto::Statistics24h,
        ]
        .into_iter()
        .map(|channel| SubscriptionDto {
            instrument: 1,
            channel,
            delivery: DeliveryDto {
                emit_book_snapshots: true,
                emit_book_deltas: true,
                emit_bbo: true,
            },
        })
        .collect(),
    };
    dto.protocol_topics = [
        "publicTrade.BTCUSDT",
        "orderbook.1.BTCUSDT",
        "tickers.BTCUSDT",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    dto.public_inputs
        .retain(|p| p.role == HISTORY_RUNTIME_POLICY_ROLE);
    dto
}
fn trades_only(spot: bool, session: u64) -> BootstrapDto {
    let mut dto = fixture(spot);
    dto.capture.session = session;
    dto.session
        .subscriptions
        .retain(|s| s.channel == ChannelDto::Trades);
    dto.protocol_topics = HyperliquidFactory::public_plan_topics(
        spot,
        &dto.session.session_spec().unwrap(),
        &dto.catalog.catalog().unwrap(),
    )
    .unwrap();
    dto
}
fn recorded(dto: BootstrapDto) -> Vec<FeedRecord> {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("source");
    let (actor, owner, _) = boot(dto.clone(), &path);
    let bybit = matches!(dto.factory, FactoryDto::BybitPublic { .. });
    let trade = |now: i64| {
        if bybit {
            text(r#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1,"data":[{"T":1,"s":"BTCUSDT","S":"Buy","v":"1.000","p":"10.01","i":"native-id"}]}"#.into(),now)
        } else {
            let coin = &dto.catalog.instruments[0].key.native_symbol;
            text(
                format!(
                    r#"{{"channel":"trades","data":[{{"coin":"{coin}","side":"B","px":"9007199254740993.123456789012345678","sz":"0.00000000000000000001","time":1,"tid":1,"hash":"h1"}}]}}"#
                ),
                now,
            )
        }
    };
    for connection in [1, 2] {
        let clock = (connection - 1) * 10;
        let mut work = vec![CaptureInput::Connected {
            connection: ConnectionId(connection),
            now: TimestampNs(clock as i64 + 1),
        }];
        if !bybit {
            work.extend(dto.protocol_topics.iter().map(|topic| text(format!(r#"{{"channel":"subscriptionResponse","data":{{"method":"subscribe","subscription":{topic}}}}}"#),clock as i64+2)));
        }
        work.push(trade(clock as i64 + 3));
        if connection == 1 {
            work.push(CaptureInput::TransportLost {
                reason: DisconnectReason::TransportError,
                now: TimestampNs(4),
            });
        }
        for input in work {
            assert!(matches!(
                owner
                    .ingress()
                    .try_submit(ConnectionId(connection), input)
                    .unwrap()
                    .wait(),
                InputSettlement::Committed { .. }
            ));
            wait(|| owner.try_next().is_some());
        }
    }
    owner.close_ingress();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    assert!(owner.shutdown().owner.failure.is_none());
    actor.shutdown().unwrap();
    let mut reader = FeedReader::open_profile(path, None, FeedProfile::EngineCausalV3).unwrap();
    let mut out = Vec::new();
    while let Some(record) = reader.read_next().unwrap() {
        out.push(record);
    }
    out
}
fn event(record: &FeedRecord, identity: &OperationIdentity) -> Option<OwnerHistoryDto> {
    let FeedRecord::Causal(e) = record else {
        return None;
    };
    if e.phase == CausalPhase::Bootstrap {
        None
    } else {
        Some(decode_owner_history(e, identity).unwrap())
    }
}
fn identity(records: &[FeedRecord]) -> OperationIdentity {
    let FeedRecord::Causal(e) = &records[0] else {
        panic!()
    };
    OwnerHistory::from_bootstrap(e, limits().per_owner)
        .unwrap()
        .identity()
        .clone()
}
fn seal() -> FeedRecord {
    FeedRecord::Seal {
        schema_version: 1,
        now_ns: 99,
        reason: "multi-owner fixture complete".into(),
    }
}
fn write(path: &Path, records: &[FeedRecord]) {
    let mut writer =
        FeedWriter::create_profile(path, 64 << 20, FeedProfile::EngineCausalV3).unwrap();
    writer.append_many(records.to_vec()).unwrap();
}
fn mixed() -> Vec<FeedRecord> {
    let a = recorded(trades_only(true, 31));
    let b = recorded(trades_only(false, 32));
    let c = recorded(bybit_fixture());
    let a_id = identity(&a);
    let b_id = identity(&b);
    let a_end = a
        .iter()
        .position(|r| {
            matches!(
                event(r, &a_id).map(|d| d.event),
                Some(OwnerEventDto::HandoffObserved { .. })
            )
        })
        .unwrap();
    let b_end=b.iter().position(|r| matches!(event(r,&b_id).map(|d|d.event),Some(OwnerEventDto::ExecutionCompleted { operation,.. }) if operation.route.ordinal==2)).unwrap()+1;
    let mut out = vec![a[0].clone(), b[0].clone(), c[0].clone()];
    // Owner A's first release is withheld in physical order until B has already
    // completed two native callbacks. Other owners must continue independently.
    out.extend_from_slice(&a[1..a_end]);
    out.extend_from_slice(&b[1..b_end]);
    let mut queues = [
        VecDeque::from(a[a_end..].to_vec()),
        VecDeque::from(b[b_end..].to_vec()),
        VecDeque::from(c[1..].to_vec()),
    ];
    while queues.iter().any(|q| !q.is_empty()) {
        for q in &mut queues {
            if let Some(r) = q.pop_front() {
                out.push(r);
            }
        }
    }
    out.push(seal());
    out
}

#[test]
fn multi_replay_actual_three_factories_preserve_interleaved_prefixes_and_global_resume() {
    let records = mixed();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("prefix");
    let mut writer =
        FeedWriter::create_profile(&path, 64 << 20, FeedProfile::EngineCausalV3).unwrap();
    let mut replay = MultiHistoryReplay::open(&path, limits()).unwrap();
    let mut expected = BTreeMap::<u64, OwnerHistory>::new();
    let mut resumed: Option<MultiHistoryReplay> = None;
    let mut expected_releases = Vec::new();
    let mut actual = Vec::new();
    let mut previous_state_bytes = BTreeMap::new();
    for record in &records {
        writer.append(record.clone()).unwrap();
        let before = replay.progress().unwrap().cursor;
        let step = replay.read_next_verified().unwrap().unwrap();
        assert_eq!(step.before_cursor(), &before);
        if let Some(r) = resumed.as_mut() {
            assert_eq!(r.read_next_verified().unwrap().unwrap(), step);
            assert!(r.read_next().unwrap().is_none());
        }
        match record {
            FeedRecord::Causal(e) if e.phase == CausalPhase::Bootstrap => {
                assert_eq!(step.owner().unwrap().session, e.route.session);
                assert!(
                    expected
                        .insert(
                            e.route.session,
                            OwnerHistory::from_bootstrap(e, limits().per_owner).unwrap()
                        )
                        .is_none()
                );
            }
            FeedRecord::Causal(e) => {
                let h = expected.get_mut(&e.route.session).unwrap();
                if let Some(d) = event(record, h.identity()) {
                    if let OwnerEventDto::ExecutionCompleted { operation, .. } = d.event {
                        previous_state_bytes.insert(
                            e.route.session,
                            serde_json::to_vec(&operation.after).unwrap().len(),
                        );
                    }
                }
                if let Some(release) = h.apply(e).unwrap() {
                    expected_releases.push((h.identity().clone(), release));
                }
            }
            _ => {}
        }
        if let Some(h) = step.handoff() {
            actual.push((step.owner().unwrap().clone(), h.clone()));
        }
        let progress = replay.progress().unwrap();
        assert_eq!(step.after_cursor(), &progress.cursor);
        assert_eq!(progress.owners.len(), expected.len());
        for owner in &progress.owners {
            let ledger = &expected[&owner.identity.session];
            assert_eq!(owner.identity, *ledger.identity());
            assert_eq!(owner.native_ordinal, ledger.native_ordinal());
            assert_eq!(owner.journal_ordinal, ledger.ordinal());
            assert_eq!(owner.uncertainties, ledger.uncertainties());
        }
        assert!(replay.read_next().unwrap().is_none());
        let mut r = MultiHistoryReplay::resume(&path, progress.cursor.clone(), limits()).unwrap();
        assert_eq!(r.progress().unwrap(), progress);
        assert!(r.read_next().unwrap().is_none());
        resumed = Some(r);
    }
    assert_eq!(actual, expected_releases);
    let p = replay.progress().unwrap();
    assert!(p.sealed);
    assert_eq!(p.owners.len(), 3);
    assert!(
        p.owners
            .iter()
            .all(|o| o.native_ordinal >= 5 && o.uncertainties.is_empty())
    );
    assert_eq!(p.resources.pending_inputs, 0);
    assert_eq!(p.resources.pending_input_bytes, 0);
    assert_eq!(p.resources.source_owned_pending_inputs, 0);
    assert_eq!(p.resources.source_owned_pending_bytes, 0);
    assert_eq!(
        p.resources.active_operation_and_state_bytes,
        previous_state_bytes.values().sum::<usize>()
    );
    assert!(
        p.resources.active_operation_and_state_bytes > 0,
        "previous state remains retained after last handoff"
    );
    assert_eq!(
        p.resources.cached_book_levels, 2,
        "Bybit quote pair, even without L2"
    );
    assert_eq!(
        p.resources.selected_instruments, 3,
        "equal instrument IDs are scoped by owner"
    );
    let mut old = HistoryReplay::open(&path, limits().per_owner).unwrap();
    assert!(
        old.read_next().is_err(),
        "single-owner API remains restrictive"
    );
}

#[test]
fn multi_replay_uncertain_owner_does_not_block_known_other_owner_even_after_seal() {
    let a = recorded(trades_only(true, 31));
    let b = recorded(bybit_fixture());
    let id = identity(&a);
    let end = a
        .iter()
        .position(|r| {
            matches!(
                event(r, &id).map(|d| d.event),
                Some(OwnerEventDto::ExecutionPermitted { .. })
            )
        })
        .unwrap();
    for indeterminate in [false, true] {
        let mut records = a[..=end].to_vec();
        if indeterminate {
            let mut dto = event(&a[end], &id).unwrap();
            let permit = dto.route.ordinal;
            dto.route.ordinal += 1;
            dto.observed_ns += 1;
            dto.event = OwnerEventDto::ExecutionIndeterminate {
                input_id: 1,
                permit_ordinal: permit,
                cause: OwnerIndeterminateCause::Panic,
            };
            records.push(FeedRecord::Causal(
                encode_owner_history(&dto, &id, MAX_OPERATION_BYTES).unwrap(),
            ));
        }
        records.extend(b.clone());
        records.push(seal());
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("unknown");
        write(&path, &records);
        let mut replay = MultiHistoryReplay::open(&path, limits()).unwrap();
        let mut releases = 0;
        while let Some(step) = replay.read_next().unwrap() {
            if step.handoff.is_some() {
                assert_eq!(step.owner.unwrap().session, 33);
                releases += 1;
            }
        }
        let p = replay.progress().unwrap();
        assert!(p.sealed);
        assert_eq!(releases, 5);
        assert_eq!(p.owners[0].native_ordinal, 0);
        assert_eq!(
            p.owners[0].uncertainties,
            vec![OwnerPrefixUncertainty::ExecutionUnknown { input_id: 1 }]
        );
        assert_eq!(p.owners[1].native_ordinal, 5);
        assert!(p.owners[1].uncertainties.is_empty());
        assert_eq!(p.resources.source_owned_pending_inputs, 1);
        assert_eq!(p.resources.pending_inputs, 1);
    }
}

#[test]
fn multi_replay_native_mismatch_globally_poisons_after_other_owner_handoffs() {
    let a = recorded(bybit_fixture());
    let mut b = recorded(trades_only(true, 31));
    let id = identity(&b);
    let mut changed = false;
    for record in &mut b {
        let FeedRecord::Causal(e) = record else {
            continue;
        };
        if e.phase != CausalPhase::Operation {
            continue;
        }
        let dto = decode_owner_history(e, &id).unwrap();
        let json = serde_json::to_string(&dto).unwrap();
        let altered = json.replace(
            "9007199254740993123456789012345678",
            "9007199254740993123456789012345679",
        );
        if altered != json {
            let dto: OwnerHistoryDto = serde_json::from_str(&altered).unwrap();
            *e = encode_owner_history(&dto, &id, MAX_OPERATION_BYTES).unwrap();
            changed = true;
            break;
        }
    }
    assert!(changed);
    let mut records = a;
    records.extend(b);
    records.push(seal());
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("mismatch");
    write(&path, &records);
    let mut replay = MultiHistoryReplay::open(&path, limits()).unwrap();
    let mut prior_bybit = 0;
    loop {
        match replay.read_next() {
            Ok(Some(step)) => {
                if step.handoff.is_some() && step.owner.unwrap().session == 33 {
                    prior_bybit += 1;
                }
            }
            Err(ReplayError::NativeMismatch { .. }) => break,
            other => panic!("expected real native mismatch, got {other:?}"),
        }
    }
    assert_eq!(prior_bybit, 5);
    assert!(replay.is_poisoned());
    assert!(matches!(replay.progress(), Err(ReplayError::Poisoned)));
    assert!(matches!(replay.read_next(), Err(ReplayError::Poisoned)));
}

type ResourceField = fn(&mut ReplayResources) -> &mut usize;
fn exact_budget(records: &[FeedRecord], field: ResourceField, exact: usize) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("budget");
    write(&path, records);
    for accepted in [true, false] {
        let mut cap = limits();
        *field(&mut cap.maximum) = exact - usize::from(!accepted);
        let mut replay = MultiHistoryReplay::open(&path, cap).unwrap();
        let mut error = None;
        loop {
            match replay.read_next() {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(e) => {
                    error = Some(e);
                    break;
                }
            }
        }
        if accepted {
            assert!(error.is_none(), "exact resource cap rejected: {error:?}");
            let mut resources = replay.progress().unwrap().resources;
            assert_eq!(*field(&mut resources), exact);
        } else {
            assert!(
                matches!(
                    error,
                    Some(ReplayError::Invalid(
                        "aggregate replay bootstrap budget"
                            | "aggregate replay retained-state budget"
                    ))
                ),
                "wrong cap failure: {error:?}"
            );
            assert!(replay.is_poisoned());
            assert!(replay.progress().is_err());
        }
    }
}
#[test]
fn multi_replay_exact_static_aggregate_caps_and_native_book_cache_reservations() {
    let dtos = [fixture(true), {
        let mut d = fixture(false);
        d.capture.session = 32;
        d
    }];
    let mut records = dtos
        .iter()
        .map(|d| {
            FeedRecord::Causal(
                encode_bootstrap_for_profile(d, MAX_OPERATION_BYTES, FeedProfile::EngineCausalV3)
                    .unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let bytes = records
        .iter()
        .map(|r| match r {
            FeedRecord::Causal(e) => e.payload().unwrap().len(),
            _ => unreachable!(),
        })
        .sum();
    records.push(seal());
    for (field, max) in [
        (
            (|r: &mut ReplayResources| &mut r.owners) as ResourceField,
            2,
        ),
        (|r| &mut r.bootstrap_payload_bytes, bytes),
        (|r| &mut r.capture_trace_reservation_bytes, 2 << 20),
        (|r| &mut r.catalog_entries, 2),
        (|r| &mut r.selected_instruments, 2),
        (|r| &mut r.maintained_book_levels, 80),
    ] {
        exact_budget(&records, field, max);
    }
    let mut dto = bybit_fixture();
    dto.factory = FactoryDto::BybitPublic {
        category: BybitCategoryDto::Spot,
        enable_l2: true,
    };
    dto.session.subscriptions.push(SubscriptionDto {
        instrument: 1,
        channel: ChannelDto::L2Book {
            depth: Some(200),
            cadence: None,
        },
        delivery: dto.session.subscriptions[0].delivery,
    });
    dto.protocol_topics = marketfeed_adapter_bybit::BybitFactory::public_plan_topics(
        marketfeed_adapter_bybit::BybitCategory::Spot,
        true,
        &dto.session.session_spec().unwrap(),
        &dto.catalog.catalog().unwrap(),
    )
    .unwrap();
    let records = [
        FeedRecord::Causal(
            encode_bootstrap_for_profile(&dto, MAX_OPERATION_BYTES, FeedProfile::EngineCausalV3)
                .unwrap(),
        ),
        seal(),
    ];
    exact_budget(&records, |r| &mut r.maintained_book_levels, 400);
    exact_budget(&records, |r| &mut r.cached_book_levels, 8194);
}
fn prefix_through(
    records: &[FeedRecord],
    predicate: impl Fn(&OwnerEventDto) -> bool,
) -> Vec<FeedRecord> {
    let id = identity(records);
    let end = records
        .iter()
        .position(|r| event(r, &id).is_some_and(|d| predicate(&d.event)))
        .unwrap();
    records[..=end].to_vec()
}
#[test]
fn multi_replay_exact_pending_source_and_active_canonical_aggregate_caps() {
    let a = recorded(trades_only(true, 31));
    let b = recorded(bybit_fixture());
    let mut ingress = prefix_through(&a, |e| matches!(e, OwnerEventDto::Ingress { .. }));
    ingress.extend(prefix_through(&b, |e| {
        matches!(e, OwnerEventDto::Ingress { .. })
    }));
    ingress.push(seal());
    let mut input_bytes = 0;
    let mut source_bytes = 0;
    let identities = BTreeMap::from([(31, identity(&a)), (33, identity(&b))]);
    for r in &ingress {
        if let FeedRecord::Causal(e) = r {
            if let Some(d) = event(r, &identities[&e.route.session]) {
                if let OwnerEventDto::Ingress {
                    input,
                    owned_bytes_bound,
                    ..
                } = d.event
                {
                    input_bytes += serde_json::to_vec(&input).unwrap().len();
                    source_bytes += owned_bytes_bound as usize;
                }
            }
        }
    }
    exact_budget(&ingress, |r| &mut r.pending_inputs, 2);
    exact_budget(&ingress, |r| &mut r.pending_input_bytes, input_bytes);
    exact_budget(&ingress, |r| &mut r.source_owned_pending_inputs, 2);
    exact_budget(
        &ingress,
        |r| &mut r.source_owned_pending_bytes,
        source_bytes,
    );
    let mut complete = prefix_through(&a, |e| {
        matches!(e, OwnerEventDto::ExecutionCompleted { .. })
    });
    complete.extend(prefix_through(&b, |e| {
        matches!(e, OwnerEventDto::ExecutionCompleted { .. })
    }));
    complete.push(seal());
    let mut retained = 0;
    for r in &complete {
        if let FeedRecord::Causal(e) = r {
            if let Some(d) = event(r, &identities[&e.route.session]) {
                if let OwnerEventDto::ExecutionCompleted { operation, .. } = d.event {
                    retained += serde_json::to_vec(&operation).unwrap().len()
                        + serde_json::to_vec(&operation.after).unwrap().len();
                }
            }
        }
    }
    assert!(retained > 0);
    exact_budget(
        &complete,
        |r| &mut r.active_operation_and_state_bytes,
        retained,
    );
}

#[test]
fn multi_replay_budget_failure_precedes_actual_native_comparison() {
    let records = recorded(bybit_fixture());
    let id = identity(&records);
    let mut prefix = prefix_through(&records, |e| {
        matches!(e, OwnerEventDto::ExecutionCompleted { .. })
    });
    let FeedRecord::Causal(e) = prefix.last_mut().unwrap() else {
        panic!()
    };
    let mut dto = decode_owner_history(e, &id).unwrap();
    let OwnerEventDto::ExecutionCompleted { operation, .. } = &mut dto.event else {
        panic!()
    };
    let mut removed = false;
    for trace in &mut operation.entries {
        if let EffectDto::QueueDrain {
            lane: LaneDto::Write,
            items,
            ..
        } = &mut trace.effect
        {
            if !items.is_empty() {
                items.remove(0);
                removed = true;
                break;
            }
        }
    }
    assert!(removed);
    let bytes = serde_json::to_vec(operation).unwrap().len()
        + serde_json::to_vec(&operation.after).unwrap().len();
    *e = encode_owner_history(&dto, &id, MAX_OPERATION_BYTES).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("preflight");
    write(&path, &prefix);
    let mut cap = limits();
    cap.maximum.active_operation_and_state_bytes = bytes - 1;
    let mut replay = MultiHistoryReplay::open(&path, cap).unwrap();
    loop {
        match replay.read_next() {
            Ok(Some(_)) => {}
            Err(ReplayError::Invalid("aggregate replay retained-state budget")) => break,
            other => panic!("expected budget before native mismatch, got {other:?}"),
        }
    }
    let mut replay = MultiHistoryReplay::open(&path, limits()).unwrap();
    loop {
        match replay.read_next() {
            Ok(Some(_)) => {}
            Err(ReplayError::NativeMismatch { .. }) => break,
            other => panic!(
                "same structurally valid record must require actual native comparison, got {other:?}"
            ),
        }
    }
}

#[test]
fn multi_replay_wrong_later_artifact_fails_whole_feed_after_prior_releases() {
    let mut records = recorded(bybit_fixture());
    let mut dto = trades_only(true, 31);
    dto.build.executable_sha256 = "0".repeat(64);
    records.push(FeedRecord::Causal(
        encode_bootstrap_for_profile(&dto, MAX_OPERATION_BYTES, FeedProfile::EngineCausalV3)
            .unwrap(),
    ));
    records.push(FeedRecord::Causal(
        encode_bootstrap_for_profile(
            &trades_only(false, 32),
            MAX_OPERATION_BYTES,
            FeedProfile::EngineCausalV3,
        )
        .unwrap(),
    ));
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("wrong-artifact");
    write(&path, &records);
    let mut replay = MultiHistoryReplay::open(&path, limits()).unwrap();
    let mut releases = 0;
    loop {
        match replay.read_next() {
            Ok(Some(s)) => {
                if s.handoff.is_some() {
                    releases += 1;
                }
            }
            Err(ReplayError::Bootstrap(_)) => break,
            other => panic!("expected later artifact rejection, got {other:?}"),
        }
    }
    assert_eq!(releases, 5);
    assert!(replay.is_poisoned());
    assert!(replay.read_next().is_err());
    assert!(replay.progress().is_err());
}

#[test]
fn multi_replay_resume_limits_and_future_corruption_do_not_prescan_later_owners() {
    use std::io::{Read, Seek, SeekFrom, Write};
    let a = recorded(bybit_fixture());
    let b = recorded(trades_only(true, 31));
    let mut records = a.clone();
    records.extend_from_slice(&b[..2]);
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("future");
    write(&path, &records);
    let mut replay = MultiHistoryReplay::open(&path, limits()).unwrap();
    for _ in &a {
        replay.read_next().unwrap().unwrap();
    }
    let saved = replay.progress().unwrap();
    assert_eq!(saved.owners.len(), 1);
    for kind in 0..3 {
        let mut cursor = saved.cursor.clone();
        match kind {
            0 => cursor.feed_id = "foreign".into(),
            1 => cursor.sequence += 1_000_000,
            _ => cursor.offset += 1,
        };
        assert!(MultiHistoryReplay::resume(&path, cursor, limits()).is_err());
    }
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.join("events.mfsf"))
        .unwrap();
    file.seek(SeekFrom::End(-1)).unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    file.seek(SeekFrom::End(-1)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
    let mut resumed = MultiHistoryReplay::resume(&path, saved.cursor.clone(), limits())
        .expect("corrupt future owner must not be prescanned");
    assert_eq!(resumed.progress().unwrap(), saved);
    assert_eq!(
        resumed.read_next().unwrap().unwrap().owner.unwrap().session,
        31
    );
    assert!(matches!(
        resumed.read_next(),
        Err(ReplayError::Recording(_))
    ));
    assert!(resumed.is_poisoned());
    assert!(resumed.progress().is_err());
}

#[test]
fn multi_replay_invalid_limits_reject_before_filesystem_access() {
    let mut cap = limits();
    cap.maximum.owners = 0;
    assert!(matches!(
        MultiHistoryReplay::open("missing-multi-owner-feed", cap),
        Err(ReplayError::Invalid("aggregate replay reader bounds"))
    ));
}

/// Deliberately bypass writer routing for hostile on-disk evidence. Only the
/// storage wrapper is serialized here; all financial payloads stay opaque.
fn forged(path: &Path, records: &[FeedRecord]) {
    #[derive(serde::Serialize)]
    struct Wire<'a> {
        sequence: u64,
        record: &'a FeedRecord,
    }
    let writer = FeedWriter::create_profile(path, 64 << 20, FeedProfile::EngineCausalV3).unwrap();
    let mut cursor = writer.cursor().clone();
    drop(writer);
    assert_eq!(cursor.offset, 48);
    let mut bytes = std::fs::read(path.join("events.mfsf")).unwrap();
    assert_eq!(bytes.len(), 48);
    for (index, record) in records.iter().enumerate() {
        let body = serde_json::to_vec(&Wire {
            sequence: index as u64 + 1,
            record,
        })
        .unwrap();
        bytes.extend_from_slice(&(body.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&marketfeed_recording::crc32c(&body).to_le_bytes());
        bytes.extend_from_slice(&body);
    }
    cursor.sequence = records.len() as u64;
    cursor.offset = bytes.len() as u64;
    std::fs::write(path.join("events.mfsf"), bytes).unwrap();
    #[derive(serde::Serialize)]
    struct Watermark {
        schema_version: u16,
        cursor: FeedCursor,
    }
    std::fs::write(
        path.join("committed.json"),
        serde_json::to_vec(&Watermark {
            schema_version: 1,
            cursor,
        })
        .unwrap(),
    )
    .unwrap();
}
#[test]
fn multi_replay_crc_correct_unknown_and_conflicting_global_owners_poison() {
    let a = recorded(bybit_fixture());
    let b = recorded(trades_only(true, 31));
    let id = identity(&b);
    let operation = b
        .iter()
        .find(|r| {
            matches!(
                event(r, &id).map(|d| d.event),
                Some(OwnerEventDto::ExecutionCompleted { .. })
            )
        })
        .unwrap()
        .clone();
    let mut conflict = trades_only(true, 33);
    conflict.capture.session = 33;
    let conflict = FeedRecord::Causal(
        encode_bootstrap_for_profile(&conflict, MAX_OPERATION_BYTES, FeedProfile::EngineCausalV3)
            .unwrap(),
    );
    for (case, records, good_prefix, prior_handoffs) in [
        ("operation-before-bootstrap", vec![operation.clone()], 0, 0),
        (
            "later-unknown-operation",
            {
                let mut r = a.clone();
                r.push(operation);
                r
            },
            a.len(),
            5,
        ),
        (
            "same-session-different-venue",
            vec![a[0].clone(), conflict, b[0].clone()],
            1,
            0,
        ),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(case);
        forged(&path, &records);
        let mut replay = MultiHistoryReplay::open(&path, limits()).unwrap();
        let mut handoffs = 0;
        for _ in 0..good_prefix {
            if replay.read_next().unwrap().unwrap().handoff.is_some() {
                handoffs += 1;
            }
        }
        assert_eq!(handoffs, prior_handoffs);
        let Err(ReplayError::Recording(error)) = replay.read_next() else {
            panic!("CRC-correct hostile routing must be rejected");
        };
        assert!(
            error.to_string().contains("causal"),
            "not a routing rejection: {error}"
        );
        assert!(replay.is_poisoned());
        assert!(matches!(replay.progress(), Err(ReplayError::Poisoned)));
        assert!(matches!(replay.read_next(), Err(ReplayError::Poisoned)));
    }
}
#[test]
fn multi_replay_rejects_all_other_profiles_before_watermark_access() {
    for profile in [
        FeedProfile::Legacy,
        FeedProfile::EngineCausalV1,
        FeedProfile::EngineCausalV2,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("wrong-profile");
        drop(FeedWriter::create_profile(&path, 64 << 20, profile).unwrap());
        std::fs::remove_file(path.join("committed.json")).unwrap();
        let Err(ReplayError::Recording(error)) = MultiHistoryReplay::open(&path, limits()) else {
            panic!("other profile must be rejected");
        };
        assert!(
            error.to_string().contains("unsupported shared feed header"),
            "profile check must precede absent watermark: {error}"
        );
    }
}
