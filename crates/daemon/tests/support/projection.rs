use super::*;
use marketfeed_daemon::causal_bootstrap::{projection::*, replay::*};
use marketfeed_engine::causal::projection::*;
use std::path::Path;
const PROFILE: FeedProfile = FeedProfile::EngineCausalV4;
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
fn policy() -> ProjectionPolicy {
    ProjectionPolicy {
        schema_version: 1,
        source_profile: 4,
        clock_policy: ProjectionClockPolicy::PerOwnerStartedPhysicalOrderV1,
        limits: limits(),
    }
}
fn batch() -> BatchConfig {
    BatchConfig {
        max_pending_operations: 1,
        max_pending_bytes: 2 << 20,
        max_group_operations: 1,
        max_group_bytes: 2 << 20,
        max_age: Duration::from_millis(1),
    }
}
fn source(path: &Path) -> MultiHistoryReplay {
    MultiHistoryReplay::open_profile(path, multi_owner_tests::limits(), PROFILE).unwrap()
}
fn project(path: &Path, target: &Path) -> ProjectionCheckpoint {
    let mut pump =
        ProjectionPump::create(source(path), policy(), target, 64 << 20, batch()).unwrap();
    wait(|| {
        pump.poll().unwrap();
        pump.last_acknowledged().sealed
    });
    let checkpoint = pump.last_acknowledged().clone();
    pump.shutdown().unwrap();
    checkpoint
}
fn groups(path: &Path) -> Vec<ProjectionGroup> {
    let mut r = ProjectionReader::open(path, limits()).unwrap();
    let mut out = Vec::new();
    while let Some(g) = r.read_next().unwrap() {
        out.push(g)
    }
    out
}
fn write(path: &Path, groups: &[ProjectionGroup]) {
    let mut writer =
        FeedWriter::create_profile(path, 64 << 20, FeedProfile::ObservationProjectionV1).unwrap();
    for g in groups {
        writer
            .append(FeedRecord::Projection(g.encode().unwrap()))
            .unwrap();
    }
}
fn all_outputs(group: &ProjectionGroup) -> Vec<ItemDto> {
    let mut items: Vec<_> = group
        .item_positions
        .iter()
        .zip(&group.items)
        .map(|(index, item)| (*index, item.clone()))
        .collect();
    items.extend(group.outbound_evidence.iter().map(|e| {
        (
            e.source_index,
            match &e.value {
                ProjectionOutbound::Write(v) => ItemDto::Write(v.clone()),
                ProjectionOutbound::Http(v) => ItemDto::Http(v.clone()),
            },
        )
    }));
    items.sort_by_key(|v| v.0);
    items.into_iter().map(|v| v.1).collect()
}

fn assert_reader_rejects(path: &Path) {
    let mut reader = ProjectionReader::open(path, limits()).unwrap();
    loop {
        match reader.read_next() {
            Ok(Some(_)) => {}
            Err(_) => break,
            Ok(None) => panic!("rewritten target accepted"),
        }
    }
    assert!(reader.is_poisoned());
    assert!(reader.progress().is_err());
    assert!(reader.metadata(31).is_err());
    assert!(reader.read_next().is_err());
}

#[test]
fn projection_large_unsupported_control_vectors_fail_before_pending_retention() {
    let (temp, _, _) = native_history_tests::recorded(true);
    let target = temp.path().join("target");
    project(&temp.path().join("original"), &target);
    let original = groups(&target);
    let index = original
        .iter()
        .position(|g| {
            matches!(
                g.source,
                ProjectionSource::Owner {
                    event: ProjectionOwnerEvent::Ingress { .. },
                    ..
                }
            )
        })
        .unwrap();
    let g = &original[index];
    let ProjectionSource::Owner {
        event: ProjectionOwnerEvent::Ingress { input, .. },
        ..
    } = &g.source
    else {
        panic!()
    };
    for command in ["subscribe", "unsubscribe", "replace"] {
        let list = serde_json::to_string(&vec!["x".repeat(2048); 128]).unwrap();
        let replacement = format!(
            r#"{{"control":{{"command":{{"kind":"{command}","data":{list}}},"now":100}}}}"#
        );
        let raw = serde_json::to_string(g)
            .unwrap()
            .replace(&serde_json::to_string(input).unwrap(), &replacement);
        assert!(raw.len() > 256 << 10);
        assert!(raw.len() < limits().max_group_bytes);
        let envelope = ProjectionEnvelope::new(
            g.source_before.clone(),
            g.source_after.clone(),
            false,
            raw.as_bytes(),
        )
        .unwrap();
        let path = temp.path().join(command);
        let mut writer =
            FeedWriter::create_profile(&path, 64 << 20, FeedProfile::ObservationProjectionV1)
                .unwrap();
        for before in &original[..index] {
            writer
                .append(FeedRecord::Projection(before.encode().unwrap()))
                .unwrap();
        }
        // Actual writer supplies canonical framing, checksums and durable cursor.
        writer.append(FeedRecord::Projection(envelope)).unwrap();
        assert_reader_rejects(&path);
    }
}

#[test]
fn projection_completed_safety_connection_frame_and_receipt_cannot_be_rewritten() {
    let (temp, _, _) = native_history_tests::recorded(true);
    let target = temp.path().join("target");
    project(&temp.path().join("original"), &target);
    let original = groups(&target);
    for mutation in 0..6 {
        let mut changed = original.clone();
        if mutation < 3 {
            let g = changed
                .iter_mut()
                .find(|g| {
                    matches!(
                        g.source,
                        ProjectionSource::Owner {
                            event: ProjectionOwnerEvent::ExecutionCompleted { .. },
                            ..
                        }
                    )
                })
                .unwrap();
            let ProjectionSource::Owner {
                event:
                    ProjectionOwnerEvent::ExecutionCompleted {
                        reconnect_requested,
                        unsafe_state,
                        connection,
                        frame_seq,
                        ..
                    },
                ..
            } = &mut g.source
            else {
                panic!()
            };
            match mutation {
                0 => {
                    *reconnect_requested = true;
                    *unsafe_state = false;
                }
                1 => *connection += 1,
                _ => *frame_seq += 1,
            }
        } else if mutation < 5 {
            let g = changed
                .iter_mut()
                .find(|g| {
                    g.items
                        .iter()
                        .any(|item| matches!(item, ItemDto::ObservedMarket(_)))
                })
                .unwrap();
            let item = g
                .items
                .iter_mut()
                .find(|item| matches!(item, ItemDto::ObservedMarket(_)))
                .unwrap();
            let ItemDto::ObservedMarket(leaf) = item else {
                panic!()
            };
            let batch = leaf.batch().unwrap();
            let mut canonical = batch.canonical().clone();
            let mut source = batch.source();
            if mutation == 3 {
                source.connection.0 += 1;
                for event in &mut canonical.events {
                    event.connection = source.connection;
                }
            } else {
                source.received.mono_ns = source.received.mono_ns.wrapping_add(1);
            }
            let altered = ObservedEventBatch::try_new(
                canonical,
                batch.native().clone(),
                source,
                batch.limits(),
            )
            .unwrap();
            *leaf = serde_json::from_str(
                &serde_json::to_string(&serde_json::to_string(&altered).unwrap()).unwrap(),
            )
            .unwrap();
        } else {
            let scoped = |item: &ItemDto| {
                matches!(item, ItemDto::System(leaf)
                if matches!(leaf.record().unwrap(), FeedRecord::System { event: SystemEvent::QuoteInvalidated { .. }, .. }))
            };
            let g = changed
                .iter_mut()
                .find(|g| g.items.iter().any(scoped))
                .unwrap();
            let ItemDto::System(leaf) = g.items.iter_mut().find(|i| scoped(i)).unwrap() else {
                panic!()
            };
            let mut record = leaf.record().unwrap();
            let FeedRecord::System {
                event: SystemEvent::QuoteInvalidated { connection, .. },
                ..
            } = &mut record
            else {
                panic!()
            };
            *connection = Some(ConnectionId(999));
            *leaf = serde_json::from_str(
                &serde_json::to_string(&serde_json::to_string(&record).unwrap()).unwrap(),
            )
            .unwrap();
        }
        let path = temp.path().join(format!("safety-{mutation}"));
        write(&path, &changed);
        assert_reader_rejects(&path);
    }
}

#[test]
fn projection_comparison_pins_open_durable_bound_while_target_grows() {
    let (temp, _, _) = native_history_tests::recorded(false);
    let source_path = temp.path().join("original");
    let target = temp.path().join("target");
    project(&source_path, &target);
    let original = groups(&target);
    let growing = temp.path().join("growing");
    let mut writer =
        FeedWriter::create_profile(&growing, 64 << 20, FeedProfile::ObservationProjectionV1)
            .unwrap();
    writer
        .append(FeedRecord::Projection(original[0].encode().unwrap()))
        .unwrap();
    let reader = ProjectionReader::open(&growing, limits()).unwrap();
    let bound = reader.committed_bound().unwrap().clone();
    for g in &original[1..] {
        writer
            .append(FeedRecord::Projection(g.encode().unwrap()))
            .unwrap();
    }
    let result = compare_with_source(source(&source_path), reader).unwrap();
    assert_eq!(result.groups, 1);
    assert_eq!(result.target_after, bound);
    assert_eq!(result.source_after, Some(original[0].source_after.clone()));
    assert!(!result.sealed);
    assert!(
        groups(&growing)
            .last()
            .is_some_and(|g| matches!(g.source, ProjectionSource::Seal { .. }))
    );
}

#[test]
fn projection_actual_preconnected_timer_and_control_keep_runner_and_physical_scope_separate() {
    let (temp, _, _) = native_history_tests::recorded_preconnected();
    let path = temp.path().join("original");
    let target = temp.path().join("target");
    project(&path, &target);
    let mut preconnected = 0;
    for g in groups(&target) {
        if let ProjectionSource::Owner {
            event:
                ProjectionOwnerEvent::ExecutionCompleted {
                    input:
                        ProjectionInput::TimerPoll { now: 1 } | ProjectionInput::Control { now: 2, .. },
                    operation_route,
                    connection,
                    frame_seq,
                    ..
                },
            ..
        } = &g.source
        {
            assert_eq!(operation_route.generation, 0);
            assert_eq!(*connection, 1);
            assert_eq!(*frame_seq, 0);
            preconnected += 1;
        }
    }
    assert_eq!(preconnected, 2);
    assert!(
        compare_with_source(
            source(&path),
            ProjectionReader::open(&target, limits()).unwrap()
        )
        .unwrap()
        .sealed
    );
}

#[test]
fn projection_actual_mixed_spot_perps_bybit_preserves_separate_metadata_and_native_only_items() {
    let (temp, path) = native_history_tests::mixed_recorded(true);
    let target = temp.path().join("target");
    project(&path, &target);
    let mut reader = ProjectionReader::open(&target, limits()).unwrap();
    let mut native = Vec::new();
    let mut canonical_bybit = false;
    while let Some(g) = reader.read_next().unwrap() {
        for item in &g.items {
            match item {
                ItemDto::ObservedMarket(leaf) => {
                    let b = leaf.batch().unwrap();
                    assert!(b.canonical().events.is_empty());
                    native.push(b.source().venue.0);
                }
                ItemDto::Market(leaf) if g.owner.as_ref().is_some_and(|o| o.session == 33) => {
                    canonical_bybit |= matches!(leaf.record().unwrap(), FeedRecord::Batch(b) if b.events.iter().any(|e| matches!(e.payload, MarketEvent::Trade(_))));
                }
                _ => {}
            }
        }
    }
    native.sort_unstable();
    assert_eq!(native, [21, 22]);
    assert!(canonical_bybit);
    let progress = reader.progress().unwrap();
    assert_eq!(progress.owners.len(), 3);
    assert!(progress.sealed);
    for session in [31, 32, 33] {
        assert_eq!(
            reader.metadata(session).unwrap().unwrap().capture.session,
            session
        );
    }
    assert_eq!(
        compare_with_source(
            source(&path),
            ProjectionReader::open(&target, limits()).unwrap()
        )
        .unwrap()
        .target_after,
        progress.target_cursor
    );
}

#[test]
fn projection_native_coin_must_match_its_associated_canonical_instrument() {
    let (temp, _, _) = native_history_tests::recorded_two_symbols();
    let target = temp.path().join("target");
    project(&temp.path().join("original"), &target);
    let mut changed = groups(&target);
    let is_trade = |i: &ItemDto| {
        matches!(i, ItemDto::ObservedMarket(leaf)
        if leaf.batch().unwrap().native().rows.iter().any(|r| matches!(r.observation, NativeObservation::HyperliquidTrade(_))))
    };
    let g = changed
        .iter_mut()
        .find(|g| g.items.iter().any(is_trade))
        .unwrap();
    let ItemDto::ObservedMarket(leaf) = g.items.iter_mut().find(|i| is_trade(i)).unwrap() else {
        panic!()
    };
    let batch = leaf.batch().unwrap();
    let mut native = batch.native().clone();
    let NativeObservation::HyperliquidTrade(trade) = &mut native.rows[0].observation else {
        panic!()
    };
    assert_eq!(trade.coin, "BTC");
    trade.coin = "ETH".into();
    let altered = ObservedEventBatch::try_new(
        batch.canonical().clone(),
        native,
        batch.source(),
        batch.limits(),
    )
    .unwrap();
    *leaf = serde_json::from_str(
        &serde_json::to_string(&serde_json::to_string(&altered).unwrap()).unwrap(),
    )
    .unwrap();
    let bad = temp.path().join("cross-symbol");
    write(&bad, &changed);
    assert_reader_rejects(&bad);
}

#[test]
fn projection_mirrors_source_pending_count_and_declared_byte_reservations() {
    let (temp, _, _) = native_history_tests::recorded(true);
    let target = temp.path().join("target");
    project(&temp.path().join("original"), &target);
    let original = groups(&target);
    let index = original
        .iter()
        .position(|g| {
            matches!(
                g.source,
                ProjectionSource::Owner {
                    event: ProjectionOwnerEvent::Ingress { .. },
                    ..
                }
            )
        })
        .unwrap();
    for bytes_case in [false, true] {
        let mut prefix = original[..index].to_vec();
        if bytes_case {
            let ProjectionSource::Bootstrap(dto) = &mut prefix[0].source else {
                panic!()
            };
            dto.owner_policy.as_mut().unwrap().max_pending_bytes = 8192;
            let id = encode_bootstrap_for_profile(dto, MAX_OPERATION_BYTES, PROFILE)
                .unwrap()
                .route
                .bootstrap_id;
            for g in &mut prefix {
                g.owner.as_mut().unwrap().bootstrap_id = id.clone();
                if let ProjectionSource::Owner { route, after, .. } = &mut g.source {
                    route.bootstrap_id = id.clone();
                    after.identity.bootstrap_id = id.clone();
                }
            }
        }
        let total = if bytes_case { 2 } else { 17 };
        let identity = prefix[0].owner.clone().unwrap();
        for input_id in 1..=total {
            let mut g = original[index].clone();
            g.owner = Some(identity.clone());
            g.source_before = prefix.last().unwrap().source_after.clone();
            g.source_after = FeedCursor {
                sequence: g.source_before.sequence + 1,
                offset: g.source_before.offset + 1,
                ..g.source_before.clone()
            };
            let ProjectionSource::Owner {
                route,
                event,
                after,
                ..
            } = &mut g.source
            else {
                panic!()
            };
            route.bootstrap_id = identity.bootstrap_id.clone();
            route.ordinal += input_id - 1;
            let ProjectionOwnerEvent::Ingress {
                input_id: id,
                connection,
                owned_bytes_bound,
                input,
                ..
            } = event
            else {
                panic!()
            };
            *id = input_id;
            *connection = input_id;
            *owned_bytes_bound = if bytes_case { 8192 } else { 1 };
            let ProjectionInput::Connected { connection: c, .. } = input else {
                panic!()
            };
            *c = input_id;
            after.identity = identity.clone();
            after.journal_ordinal = route.ordinal;
            after.uncertainties = (1..=input_id)
                .map(|input_id| OwnerPrefixUncertainty::ExecutionUnknown { input_id })
                .collect();
            prefix.push(g);
        }
        let bad = temp.path().join(if bytes_case {
            "byte-reservations"
        } else {
            "count-reservations"
        });
        write(&bad, &prefix);
        let mut reader = ProjectionReader::open(&bad, limits()).unwrap();
        for _ in &prefix[..prefix.len() - 1] {
            assert!(reader.read_next().unwrap().is_some());
        }
        assert!(
            reader.read_next().is_err(),
            "source reservation cap must reject the last ingress"
        );
        assert!(reader.is_poisoned());
    }
}

#[test]
fn projection_actual_spot_perps_atomic_groups_match_all_releases_and_every_resume() {
    for spot in [true, false] {
        let (temp, original, _) = native_history_tests::recorded(spot);
        let path = temp.path().join("original");
        let target = temp.path().join("target");
        let checkpoint = project(&path, &target);
        assert!(FeedReader::open(&target, None).is_err());
        assert!(FeedReader::open_profile(&target, None, PROFILE).is_err());
        let mut original_source = source(&path);
        let mut reader = ProjectionReader::open(&target, limits()).unwrap();
        let mut points = Vec::new();
        let mut seen = Vec::new();
        let mut native_only = false;
        let mut evidences = 0;
        while let Some(g) = reader.read_next().unwrap() {
            let step = original_source.read_next_verified().unwrap().unwrap();
            assert_eq!(
                (&g.source_before, &g.source_after),
                (step.before_cursor(), step.after_cursor())
            );
            assert_eq!(g.owner.as_ref(), step.owner());
            assert_eq!(
                all_outputs(&g),
                step.handoff()
                    .map(|h| h.released_items().cloned().collect::<Vec<_>>())
                    .unwrap_or_default()
            );
            for item in &g.items {
                if let ItemDto::ObservedMarket(leaf) = item {
                    let b = leaf.batch().unwrap();
                    if b.canonical().events.is_empty() {
                        assert_eq!(
                            b.source().received,
                            FrameStamp {
                                receive_ts: TimestampNs(-123),
                                mono_ns: u64::MAX
                            }
                        );
                        native_only = true;
                    }
                }
            }
            evidences += g.outbound_evidence.len();
            if let ProjectionSource::Bootstrap(dto) = &g.source {
                let FeedRecord::Causal(e) = &original[0] else {
                    panic!()
                };
                assert_eq!(**dto, decode_bootstrap(e).unwrap());
                assert_eq!(
                    reader.metadata(dto.capture.session).unwrap(),
                    Some(dto.as_ref())
                );
            }
            points.push(reader.progress().unwrap());
            seen.push(g);
        }
        assert!(native_only);
        assert!(
            evidences > 0,
            "outbound subscription writes remain separately labeled evidence"
        );
        assert_eq!(seen.len(), original.len());
        assert_eq!(
            reader.progress().unwrap().source_cursor,
            Some(checkpoint.source.clone())
        );
        assert!(reader.progress().unwrap().sealed);
        for (index, p) in points.iter().enumerate() {
            let mut resumed = ProjectionReader::resume(
                &target,
                p.target_cursor.clone(),
                p.source_cursor.clone(),
                limits(),
            )
            .unwrap();
            assert_eq!(resumed.progress().unwrap(), *p);
            assert_eq!(resumed.read_next().unwrap().as_ref(), seen.get(index + 1));
            let mut wrong = p.target_cursor.clone();
            wrong.offset += 1;
            assert!(
                ProjectionReader::resume(&target, wrong, p.source_cursor.clone(), limits())
                    .is_err()
            );
            let mut wrong_source = p.source_cursor.clone().unwrap();
            wrong_source.sequence += 1;
            assert!(
                ProjectionReader::resume(
                    &target,
                    p.target_cursor.clone(),
                    Some(wrong_source),
                    limits()
                )
                .is_err()
            );
        }
        assert_eq!(
            compare_with_source(
                source(&path),
                ProjectionReader::open(&target, limits()).unwrap()
            )
            .unwrap()
            .groups,
            seen.len() as u64
        );
        // A rebuild owns a new target identity and identical logical source groups.
        let rebuilt = temp.path().join("rebuilt");
        let other = project(&path, &rebuilt);
        assert_ne!(other.target.feed_id, checkpoint.target.feed_id);
        assert_eq!(groups(&rebuilt), seen);
    }
}

#[test]
fn projection_clock_lineage_policy_and_owner_mutations_poison_structural_reader() {
    let (temp, _, _) = native_history_tests::recorded(false);
    let target = temp.path().join("target");
    project(&temp.path().join("original"), &target);
    let original = groups(&target);
    for mutation in 0..5 {
        let mut changed = original.clone();
        let g = changed
            .iter_mut()
            .find(|g| {
                matches!(
                    g.source,
                    ProjectionSource::Owner {
                        event: ProjectionOwnerEvent::ReleaseIntent { .. },
                        ..
                    }
                )
            })
            .unwrap();
        match mutation {
            0 => {
                if let ProjectionClock::Owner {
                    approximate_unix_ns,
                    ..
                } = &mut g.clock
                {
                    *approximate_unix_ns = None;
                }
            }
            1 => {
                if let ProjectionSource::Owner {
                    event:
                        ProjectionOwnerEvent::ReleaseIntent {
                            operation_ordinal, ..
                        },
                    ..
                } = &mut g.source
                {
                    *operation_ordinal += 1;
                }
            }
            2 => {
                g.policy.clock_policy = ProjectionClockPolicy::PerOwnerStartedPhysicalOrderV1;
                g.policy.limits.max_items += 1;
                g.policy_id = g.policy.id().unwrap();
            }
            3 => {
                if let ProjectionSource::Owner { after, .. } = &mut g.source {
                    after.uncertainties.clear();
                }
            }
            _ => {
                if let ProjectionSource::Owner { route, .. } = &mut g.source {
                    route.generation += 1;
                }
            }
        }
        let path = temp.path().join(format!("mutated-{mutation}"));
        write(&path, &changed);
        let mut r = ProjectionReader::open(
            &path,
            ProjectionLimits {
                max_items: 2048,
                ..limits()
            },
        )
        .unwrap();
        loop {
            match r.read_next() {
                Ok(Some(_)) => {}
                Err(_) => break,
                other => panic!("mutation accepted {mutation}: {other:?}"),
            }
        }
        assert!(r.is_poisoned());
        assert!(r.progress().is_err());
        assert!(r.read_next().is_err());
    }
}

#[test]
fn projection_native_value_authenticity_requires_original_source_comparison() {
    let (temp, _, _) = native_history_tests::recorded(true);
    let path = temp.path().join("original");
    let target = temp.path().join("target");
    project(&path, &target);
    let mut original = groups(&target);
    let mut changed = false;
    for g in &mut original {
        let text = serde_json::to_string(g).unwrap();
        if text.contains("hash-1") {
            *g =
                serde_json::from_str(&text.replace("hash-1", "financial-native-mutation")).unwrap();
            changed = true;
            break;
        }
    }
    assert!(changed);
    let altered = temp.path().join("altered-native");
    write(&altered, &original);
    // CRC-valid rewritten financial values are structurally well formed, not
    // authenticated by the compact target itself. Native source comparison fails.
    assert_eq!(groups(&altered).len(), original.len());
    assert!(matches!(
        compare_with_source(
            source(&path),
            ProjectionReader::open(&altered, limits()).unwrap()
        ),
        Err(ProjectionError::Invalid(
            "projection differs from actual verified source"
        ))
    ));
}

#[test]
fn projection_sealed_unknown_owner_stays_unknown_without_market_resurrection() {
    let (temp, all, _) = native_history_tests::recorded(true);
    let FeedRecord::Causal(first) = &all[0] else {
        panic!()
    };
    let id = OperationIdentity {
        venue: first.route.venue,
        session: first.route.session,
        bootstrap_id: first.route.bootstrap_id.clone(),
    };
    let end=all.iter().position(|record|matches!(record,FeedRecord::Causal(e) if e.phase!=CausalPhase::Bootstrap && matches!(decode_owner_history(e,&id).unwrap().event,OwnerEventDto::ExecutionPermitted{..}))).unwrap();
    let mut records = all[..=end].to_vec();
    records.push(FeedRecord::Seal {
        schema_version: 1,
        now_ns: i64::MAX,
        reason: "unknown permit".into(),
    });
    let path = temp.path().join("unknown-source");
    FeedWriter::create_profile(&path, 64 << 20, PROFILE)
        .unwrap()
        .append_many(records)
        .unwrap();
    let target = temp.path().join("target");
    project(&path, &target);
    let actual = groups(&target);
    assert!(
        actual
            .iter()
            .all(|g| g.items.is_empty() && g.release.is_none())
    );
    let last = actual.last().unwrap();
    let ProjectionSource::Seal { owners, .. } = &last.source else {
        panic!()
    };
    assert_eq!(
        owners[0].uncertainties,
        vec![OwnerPrefixUncertainty::ExecutionUnknown { input_id: 1 }]
    );
    assert_eq!(
        last.clock,
        ProjectionClock::StorageSeal {
            recorded_now_ns: i64::MAX
        }
    );
    let mut dropped = actual.clone();
    let ProjectionSource::Seal { owners, .. } = &mut dropped.last_mut().unwrap().source else {
        panic!()
    };
    owners[0].uncertainties.clear();
    let bad = temp.path().join("bad-seal");
    write(&bad, &dropped);
    let mut r = ProjectionReader::open(&bad, limits()).unwrap();
    assert!(
        std::iter::from_fn(|| match r.read_next() {
            Ok(Some(_)) => Some(()),
            Ok(None) => None,
            Err(_) => None,
        })
        .count()
            < actual.len()
    );
    assert!(r.is_poisoned());
}
