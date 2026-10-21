use super::*;
use marketfeed_daemon::causal_bootstrap::replay::{
    ArchiveClockPolicy, MultiHistoryReplay, ReplayError, VerifiedSourceClock, VerifiedSourceRecord,
};
use std::path::Path;

const PROFILE: FeedProfile = FeedProfile::EngineCausalV4;
fn write(path: &Path, records: &[FeedRecord]) {
    FeedWriter::create_profile(path, 64 << 20, PROFILE)
        .unwrap()
        .append_many(records.to_vec())
        .unwrap();
}
fn identity(record: &FeedRecord) -> OperationIdentity {
    let FeedRecord::Causal(e) = record else {
        panic!()
    };
    OperationIdentity {
        venue: e.route.venue,
        session: e.route.session,
        bootstrap_id: e.route.bootstrap_id.clone(),
    }
}

#[test]
fn verified_source_native_metadata_clocks_control_and_handoffs_match_original_records() {
    for spot in [true, false] {
        let (temp, records, expected) = native_history_tests::recorded(spot);
        let mut replay = MultiHistoryReplay::open_profile(
            temp.path().join("original"),
            multi_owner_tests::limits(),
            PROFILE,
        )
        .unwrap();
        let id = identity(&records[0]);
        let mut before = replay.progress().unwrap().cursor;
        let mut origin = None;
        let mut operations = Vec::new();
        let mut native_only = false;
        for raw in &records {
            let step = replay.read_next_verified().unwrap().unwrap();
            assert_eq!(step.profile(), PROFILE);
            assert_eq!(
                step.clock_policy(),
                ArchiveClockPolicy::OwnerStartedObservationV1
            );
            assert_eq!(step.before_cursor(), &before);
            assert_eq!(step.after_cursor().sequence, before.sequence + 1);
            assert!(step.after_cursor().offset > before.offset);
            assert_eq!(step.after_cursor(), &replay.progress().unwrap().cursor);
            before = step.after_cursor().clone();
            match (raw, step.record()) {
                (FeedRecord::Causal(e), VerifiedSourceRecord::Bootstrap(dto)) => {
                    assert_eq!(**dto, decode_bootstrap(e).unwrap());
                    assert_eq!(step.owner(), Some(&id));
                    assert_eq!(step.clock(), VerifiedSourceClock::Unstarted);
                    assert!(step.handoff().is_none());
                }
                (FeedRecord::Causal(e), VerifiedSourceRecord::Owner { observation, after }) => {
                    assert_eq!(**observation, decode_owner_history(e, &id).unwrap());
                    assert_eq!(step.owner(), Some(&id));
                    assert_eq!(after, &replay.progress().unwrap().owners[0]);
                    if let OwnerEventDto::Started { unix_ns } = observation.event {
                        origin = Some(unix_ns);
                    }
                    assert_eq!(
                        step.clock(),
                        VerifiedSourceClock::Owner {
                            started_unix_ns: origin.unwrap(),
                            observed_ns: observation.observed_ns,
                            approximate_unix_ns: i64::try_from(
                                i128::from(origin.unwrap()) + i128::from(observation.observed_ns)
                            )
                            .ok(),
                        }
                    );
                    assert_eq!(
                        step.handoff().is_some(),
                        matches!(observation.event, OwnerEventDto::HandoffObserved { .. })
                    );
                    if let OwnerEventDto::Ingress {
                        input: InputDto::Text { received, .. },
                        connection,
                        ..
                    } = &observation.event
                    {
                        if received.receive_ts == TimestampNs(-123) {
                            assert_eq!((*connection, received.mono_ns), (1, u64::MAX));
                            native_only = true;
                        }
                    }
                }
                (
                    FeedRecord::Seal {
                        schema_version,
                        now_ns,
                        reason,
                    },
                    VerifiedSourceRecord::Seal {
                        schema_version: v,
                        now_ns: n,
                        reason: r,
                        owners,
                    },
                ) => {
                    assert_eq!((v, n, r), (schema_version, now_ns, reason));
                    assert_eq!(owners, &replay.progress().unwrap().owners);
                    assert!(step.owner().is_none() && step.handoff().is_none());
                    assert_eq!(
                        step.clock(),
                        VerifiedSourceClock::StorageSeal {
                            recorded_now_ns: *now_ns
                        }
                    );
                }
                other => panic!("source record changed: {other:?}"),
            }
            if let Some(handoff) = step.handoff() {
                operations.push((*handoff.operation).clone());
            }
        }
        assert!(native_only);
        assert_eq!(operations, expected);
        assert!(replay.read_next_verified().unwrap().is_none());
    }
}

#[test]
fn verified_source_seal_keeps_unknown_execution_and_never_advances_owner_time() {
    let (temp, all, _) = native_history_tests::recorded(true);
    let id = identity(&all[0]);
    let end = all
        .iter()
        .position(|r| match r {
            FeedRecord::Causal(e) if e.phase != CausalPhase::Bootstrap => matches!(
                decode_owner_history(e, &id).unwrap().event,
                OwnerEventDto::ExecutionPermitted { .. }
            ),
            _ => false,
        })
        .unwrap();
    let mut records = all[..=end].to_vec();
    records.push(FeedRecord::Seal {
        schema_version: 1,
        now_ns: i64::MAX,
        reason: "uncertain source".into(),
    });
    let path = temp.path().join("unknown");
    write(&path, &records);
    let mut replay =
        MultiHistoryReplay::open_profile(&path, multi_owner_tests::limits(), PROFILE).unwrap();
    let mut last_owner_clock = None;
    while let Some(step) = replay.read_next_verified().unwrap() {
        assert!(step.handoff().is_none());
        match step.record() {
            VerifiedSourceRecord::Owner { .. } => last_owner_clock = Some(step.clock()),
            VerifiedSourceRecord::Seal { owners, reason, .. } => {
                assert_eq!(reason, "uncertain source");
                assert_eq!(owners[0].native_ordinal, 0);
                assert_eq!(
                    owners[0].uncertainties,
                    vec![OwnerPrefixUncertainty::ExecutionUnknown { input_id: 1 }]
                );
                assert_ne!(Some(step.clock()), last_owner_clock);
                let cursor = step.after_cursor().clone();
                let mut resumed = MultiHistoryReplay::resume_profile(
                    &path,
                    cursor,
                    multi_owner_tests::limits(),
                    PROFILE,
                )
                .unwrap();
                assert_eq!(resumed.progress().unwrap(), replay.progress().unwrap());
                assert!(resumed.read_next_verified().unwrap().is_none());
            }
            _ => {}
        }
    }
    assert!(matches!(
        last_owner_clock,
        Some(VerifiedSourceClock::Owner { .. })
    ));
}

#[test]
fn verified_source_clock_extrema_and_interleaved_regressions_resume_exactly() {
    let temp = tempfile::tempdir().unwrap();
    let mut records = Vec::new();
    for (session, origin, observed) in [(51, i64::MAX, 1), (52, i64::MIN, u64::MAX)] {
        let mut dto = native_history_tests::native_fixture(true);
        dto.capture.session = session;
        let bootstrap = FeedRecord::Causal(
            encode_bootstrap_for_profile(&dto, MAX_OPERATION_BYTES, PROFILE).unwrap(),
        );
        let id = identity(&bootstrap);
        records.push(bootstrap);
        for (ordinal, ns, event) in [
            (1, 0, OwnerEventDto::Started { unix_ns: origin }),
            (
                2,
                observed,
                OwnerEventDto::Revoked {
                    through_input_id: 0,
                    cause: OwnerTerminalCause::Revoked,
                },
            ),
        ] {
            let d = OwnerHistoryDto {
                schema_version: 3,
                route: CausalRoute {
                    venue: id.venue,
                    session,
                    ordinal,
                    generation: 0,
                    bootstrap_id: id.bootstrap_id.clone(),
                },
                observed_ns: ns,
                event,
            };
            records.push(FeedRecord::Causal(
                encode_owner_history(&d, &id, MAX_OPERATION_BYTES).unwrap(),
            ));
        }
    }
    // Both bootstraps precede both origins; physical order is deliberately not
    // sortable by the two owner clocks. Later control clocks include overflow.
    let order = [0, 3, 1, 4, 2, 5];
    let records: Vec<_> = order.into_iter().map(|i| records[i].clone()).collect();
    let path = temp.path().join("clocks");
    write(&path, &records);
    let mut replay =
        MultiHistoryReplay::open_profile(&path, multi_owner_tests::limits(), PROFILE).unwrap();
    let mut steps = Vec::new();
    while let Some(step) = replay.read_next_verified().unwrap() {
        steps.push(step);
    }
    for (index, step) in steps.iter().enumerate() {
        let mut resumed = MultiHistoryReplay::resume_profile(
            &path,
            step.after_cursor().clone(),
            multi_owner_tests::limits(),
            PROFILE,
        )
        .unwrap();
        assert_eq!(
            resumed.read_next_verified().unwrap().as_ref(),
            steps.get(index + 1)
        );
    }
    assert!(matches!(
        steps[2].clock(),
        VerifiedSourceClock::Owner {
            approximate_unix_ns: Some(i64::MAX),
            ..
        }
    ));
    assert!(matches!(
        steps[3].clock(),
        VerifiedSourceClock::Owner {
            approximate_unix_ns: Some(i64::MIN),
            ..
        }
    ));
    assert!(matches!(
        steps[4].clock(),
        VerifiedSourceClock::Owner {
            approximate_unix_ns: None,
            ..
        }
    ));
    assert!(matches!(
        steps[5].clock(),
        VerifiedSourceClock::Owner {
            approximate_unix_ns: Some(i64::MAX),
            ..
        }
    ));
}

#[test]
fn verified_source_native_mismatch_exposes_no_mutated_completion_and_poisons() {
    let (temp, mut records, _) = native_history_tests::recorded(false);
    let id = identity(&records[0]);
    let mut altered = false;
    for record in &mut records {
        let FeedRecord::Causal(e) = record else {
            continue;
        };
        if e.phase != CausalPhase::Operation {
            continue;
        }
        let dto = decode_owner_history(e, &id).unwrap();
        let text = serde_json::to_string(&dto).unwrap();
        if text.contains("hash-1") {
            let dto: OwnerHistoryDto =
                serde_json::from_str(&text.replace("hash-1", "native-only-corruption")).unwrap();
            *e = encode_owner_history(&dto, &id, MAX_OPERATION_BYTES).unwrap();
            altered = true;
            break;
        }
    }
    assert!(altered);
    let path = temp.path().join("corrupt-native");
    write(&path, &records);
    let mut replay =
        MultiHistoryReplay::open_profile(&path, multi_owner_tests::limits(), PROFILE).unwrap();
    loop {
        match replay.read_next_verified() {
            Ok(Some(step)) => {
                if let VerifiedSourceRecord::Owner { observation, .. } = step.record() {
                    assert!(
                        !serde_json::to_string(observation)
                            .unwrap()
                            .contains("native-only-corruption")
                    );
                }
            }
            Err(ReplayError::NativeMismatch { .. }) => break,
            other => panic!("missing native mismatch: {other:?}"),
        }
    }
    assert!(replay.is_poisoned());
    assert!(matches!(
        replay.read_next_verified(),
        Err(ReplayError::Poisoned)
    ));
    assert!(matches!(replay.read_next(), Err(ReplayError::Poisoned)));
    assert!(matches!(replay.progress(), Err(ReplayError::Poisoned)));
}
