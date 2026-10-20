use super::*;
use marketfeed_daemon::causal_bootstrap::replay::{HistoryReplay, ReplayError};
use std::path::Path;

fn limits() -> OwnerHistoryLimits {
    OwnerHistoryLimits {
        max_retained_input_bytes: 4 << 20,
        max_input_bytes: 256 << 10,
        max_record_bytes: 256 << 10,
    }
}
fn recorded(spot: bool) -> (tempfile::TempDir, Vec<FeedRecord>) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("original");
    let dto = fixture(spot);
    let (actor, owner, _) = boot(dto.clone(), &path);
    for (connection, input) in inputs(&dto) {
        let ticket = owner
            .ingress()
            .try_submit(ConnectionId(connection), input)
            .unwrap();
        assert!(matches!(ticket.wait(), InputSettlement::Committed { .. }));
        wait(|| owner.try_next().is_some());
    }
    owner.close_ingress();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    assert!(owner.shutdown().owner.failure.is_none());
    actor.shutdown().unwrap();
    let mut reader = FeedReader::open_profile(path, None, FeedProfile::EngineCausalV3).unwrap();
    let mut records = Vec::new();
    while let Some(record) = reader.read_next().unwrap() {
        records.push(record);
    }
    (temp, records)
}
fn write(path: &Path, records: &[FeedRecord]) {
    let mut writer =
        FeedWriter::create_profile(path, 64 << 20, FeedProfile::EngineCausalV3).unwrap();
    writer.append_many(records.to_vec()).unwrap();
}
fn structural(records: &[FeedRecord]) -> OwnerHistory {
    let FeedRecord::Causal(bootstrap) = &records[0] else {
        panic!()
    };
    let mut history = OwnerHistory::from_bootstrap(bootstrap, limits()).unwrap();
    for record in &records[1..] {
        if let FeedRecord::Causal(envelope) = record {
            history.apply(envelope).unwrap();
        }
    }
    history
}

#[test]
fn actual_replay_spot_and_perps_preserve_every_physical_prefix_and_resume() {
    for spot in [true, false] {
        let (temp, records) = recorded(spot);
        let path = temp.path().join("incremental");
        let mut writer =
            FeedWriter::create_profile(&path, 64 << 20, FeedProfile::EngineCausalV3).unwrap();
        writer.append(records[0].clone()).unwrap();
        let mut replay = HistoryReplay::open(&path, limits()).unwrap();
        assert!(replay.read_next().unwrap().is_none());
        let mut handoffs = Vec::new();
        let mut previous_resume: Option<HistoryReplay> = None;
        for (index, record) in records.iter().enumerate().skip(1) {
            writer.append(record.clone()).unwrap();
            let step = replay
                .read_next()
                .unwrap()
                .expect("one newly committed physical record");
            if let Some(resumed) = previous_resume.as_mut() {
                assert_eq!(resumed.read_next().unwrap().unwrap(), step);
                assert!(resumed.read_next().unwrap().is_none());
            }
            if let Some(handoff) = step.handoff {
                handoffs.push(handoff);
            }
            assert!(replay.read_next().unwrap().is_none());
            let prefix = structural(&records[..=index]);
            let progress = replay.progress().unwrap();
            assert_eq!(progress.native_ordinal, prefix.native_ordinal());
            assert_eq!(progress.journal_ordinal, prefix.ordinal());
            assert_eq!(progress.uncertainties, prefix.uncertainties());
            let mut resumed =
                HistoryReplay::resume(&path, progress.cursor.clone(), limits()).unwrap();
            assert_eq!(resumed.progress().unwrap(), progress);
            assert!(
                resumed.read_next().unwrap().is_none(),
                "resume never releases historical handoffs twice"
            );
            previous_resume = Some(resumed);
        }
        let mut expected = Vec::new();
        let FeedRecord::Causal(bootstrap) = &records[0] else {
            panic!()
        };
        let mut history = OwnerHistory::from_bootstrap(bootstrap, limits()).unwrap();
        for record in &records[1..] {
            if let FeedRecord::Causal(envelope) = record {
                if let Some(handoff) = history.apply(envelope).unwrap() {
                    expected.push(handoff);
                }
            }
        }
        assert_eq!(handoffs, expected);
        assert_eq!(
            replay.progress().unwrap().native_ordinal,
            inputs(&fixture(spot)).len() as u64
        );
        assert!(replay.progress().unwrap().uncertainties.is_empty());
    }
}

#[test]
fn actual_replay_rejects_missing_actual_wire_drain_accepted_by_structural_validator() {
    let (temp, mut records) = recorded(false);
    let FeedRecord::Causal(bootstrap) = &records[0] else {
        panic!()
    };
    let identity = OwnerHistory::from_bootstrap(bootstrap, limits())
        .unwrap()
        .identity()
        .clone();
    let mut mutated = false;
    for record in &mut records[1..] {
        let FeedRecord::Causal(envelope) = record else {
            continue;
        };
        let mut dto = decode_owner_history(envelope, &identity).unwrap();
        if let OwnerEventDto::ExecutionCompleted { operation, .. } = &mut dto.event {
            for entry in &mut operation.entries {
                if let EffectDto::QueueDrain {
                    lane: LaneDto::Write,
                    items,
                    ..
                } = &mut entry.effect
                {
                    if !items.is_empty() {
                        items.remove(0);
                        mutated = true;
                        break;
                    }
                }
            }
        }
        if mutated {
            *envelope = encode_owner_history(&dto, &identity, MAX_OPERATION_BYTES).unwrap();
            break;
        }
    }
    assert!(mutated);
    assert!(structural(&records).uncertainties().is_empty());
    let path = temp.path().join("missing-drain");
    write(&path, &records);
    let mut replay = HistoryReplay::open(path, limits()).unwrap();
    loop {
        match replay.read_next() {
            Ok(Some(_)) => {}
            Err(ReplayError::NativeMismatch { .. }) => break,
            other => panic!("expected full drain mismatch, got {other:?}"),
        }
    }
    assert!(replay.is_poisoned());
    assert!(replay.progress().is_err());
}

#[test]
fn actual_replay_rejects_financial_effect_mutation_accepted_by_structural_validator() {
    let (temp, mut records) = recorded(true);
    let FeedRecord::Causal(bootstrap) = &records[0] else {
        panic!()
    };
    let identity = OwnerHistory::from_bootstrap(bootstrap, limits())
        .unwrap()
        .identity()
        .clone();
    let mut mutated = false;
    for record in &mut records[1..] {
        let FeedRecord::Causal(envelope) = record else {
            continue;
        };
        let dto = decode_owner_history(envelope, &identity).unwrap();
        if !matches!(dto.event, OwnerEventDto::ExecutionCompleted { .. }) {
            continue;
        }
        let bytes = serde_json::to_string(&dto).unwrap();
        let replacement = bytes.replace(
            "9007199254740993123456789012345678",
            "9007199254740993123456789012345679",
        );
        if replacement != bytes {
            let altered: OwnerHistoryDto = serde_json::from_str(&replacement).unwrap();
            *envelope = encode_owner_history(&altered, &identity, MAX_OPERATION_BYTES).unwrap();
            mutated = true;
            break;
        }
    }
    assert!(
        mutated,
        "fixture must contain the exact unrounded trade price"
    );
    assert!(structural(&records).uncertainties().is_empty());
    let path = temp.path().join("mutated");
    write(&path, &records);
    let mut replay = HistoryReplay::open(path, limits()).unwrap();
    loop {
        match replay.read_next() {
            Ok(Some(_)) => {}
            Err(ReplayError::NativeMismatch { .. }) => break,
            other => panic!("expected actual-engine financial mismatch, got {other:?}"),
        }
    }
    assert!(replay.is_poisoned());
    assert!(matches!(replay.read_next(), Err(ReplayError::Poisoned)));
    assert!(matches!(replay.progress(), Err(ReplayError::Poisoned)));
}

#[test]
fn actual_replay_unknown_permit_is_not_executed_or_resolved_by_sealing() {
    let (temp, records) = recorded(false);
    let FeedRecord::Causal(bootstrap) = &records[0] else {
        panic!()
    };
    let identity = OwnerHistory::from_bootstrap(bootstrap, limits())
        .unwrap()
        .identity()
        .clone();
    let end = records.iter().position(|record| matches!(record, FeedRecord::Causal(e) if matches!(decode_owner_history(e,&identity).map(|d|d.event),Ok(OwnerEventDto::ExecutionPermitted { .. })))).unwrap();
    let FeedRecord::Causal(permit) = &records[end] else {
        panic!()
    };
    let permit_dto = decode_owner_history(permit, &identity).unwrap();
    let OwnerEventDto::ExecutionPermitted { input_id, .. } = permit_dto.event else {
        panic!()
    };
    for indeterminate in [false, true] {
        let mut prefix = records[..=end].to_vec();
        if indeterminate {
            let mut unknown = permit_dto.clone();
            unknown.route.ordinal += 1;
            unknown.observed_ns += 1;
            unknown.event = OwnerEventDto::ExecutionIndeterminate {
                input_id,
                permit_ordinal: permit.route.ordinal,
                cause: OwnerIndeterminateCause::Panic,
            };
            prefix.push(FeedRecord::Causal(
                encode_owner_history(&unknown, &identity, MAX_OPERATION_BYTES).unwrap(),
            ));
        }
        prefix.push(FeedRecord::Seal {
            schema_version: 1,
            now_ns: 99,
            reason: "uncertain crash evidence".into(),
        });
        let path = temp.path().join(format!("unknown-{indeterminate}"));
        write(&path, &prefix);
        let mut replay = HistoryReplay::open(path, limits()).unwrap();
        while replay.read_next().unwrap().is_some() {}
        let progress = replay.progress().unwrap();
        assert!(progress.sealed);
        assert_eq!(progress.native_ordinal, 0);
        assert_eq!(
            progress.uncertainties,
            vec![OwnerPrefixUncertainty::ExecutionUnknown { input_id: 1 }]
        );
    }
}

#[test]
fn actual_replay_reexecutes_available_late_completion_after_revocation_without_handoff() {
    let (temp, records) = recorded(true);
    let FeedRecord::Causal(bootstrap) = &records[0] else {
        panic!()
    };
    let identity = OwnerHistory::from_bootstrap(bootstrap, limits())
        .unwrap()
        .identity()
        .clone();
    let end=records.iter().position(|record|matches!(record,FeedRecord::Causal(e) if matches!(decode_owner_history(e,&identity).map(|d|d.event),Ok(OwnerEventDto::ExecutionCompleted{..})))).unwrap();
    let FeedRecord::Causal(completed) = &records[end] else {
        panic!()
    };
    let mut completion = decode_owner_history(completed, &identity).unwrap();
    let OwnerEventDto::ExecutionCompleted {
        input_id,
        operation,
        ..
    } = &completion.event
    else {
        panic!()
    };
    let id = *input_id;
    let mut revoked = completion.clone();
    revoked.route.generation = operation.route.generation - 1;
    revoked.event = OwnerEventDto::Revoked {
        through_input_id: id,
        cause: OwnerTerminalCause::Revoked,
    };
    let mut prefix = records[..end].to_vec();
    prefix.push(FeedRecord::Causal(
        encode_owner_history(&revoked, &identity, MAX_OPERATION_BYTES).unwrap(),
    ));
    completion.route.ordinal += 1;
    completion.observed_ns += 1;
    prefix.push(FeedRecord::Causal(
        encode_owner_history(&completion, &identity, MAX_OPERATION_BYTES).unwrap(),
    ));
    let mut withheld = completion.clone();
    withheld.route.ordinal += 1;
    withheld.observed_ns += 1;
    withheld.event = OwnerEventDto::Withheld { input_id: id };
    prefix.push(FeedRecord::Causal(
        encode_owner_history(&withheld, &identity, MAX_OPERATION_BYTES).unwrap(),
    ));
    assert!(structural(&prefix).uncertainties().is_empty());
    let path = temp.path().join("late-completion");
    write(&path, &prefix);
    let mut replay = HistoryReplay::open(path, limits()).unwrap();
    while let Some(step) = replay.read_next().unwrap() {
        assert!(step.handoff.is_none());
    }
    let p = replay.progress().unwrap();
    assert_eq!(p.native_ordinal, 1);
    assert!(p.terminal && p.uncertainties.is_empty());
}

#[test]
fn actual_replay_rejects_second_owner_and_wrong_profile_or_bootstrap_budget() {
    let (temp, records) = recorded(true);
    let mut other = fixture(true);
    other.capture.session += 1;
    let extra =
        encode_bootstrap_for_profile(&other, MAX_OPERATION_BYTES, FeedProfile::EngineCausalV3)
            .unwrap();
    let path = temp.path().join("two-owners");
    write(&path, &[records[0].clone(), FeedRecord::Causal(extra)]);
    let mut replay = HistoryReplay::open(&path, limits()).unwrap();
    assert!(matches!(replay.read_next(), Err(ReplayError::Invalid(_))));
    assert!(replay.is_poisoned());
    let legacy = temp.path().join("legacy");
    FeedWriter::create(&legacy, 1 << 20).unwrap();
    assert!(matches!(
        HistoryReplay::open(legacy, limits()),
        Err(ReplayError::Recording(_))
    ));
    let mut capped = limits();
    capped.max_record_bytes = 1;
    assert!(matches!(
        HistoryReplay::open(temp.path().join("original"), capped),
        Err(ReplayError::Codec(CodecError::TooLarge))
    ));
}

#[test]
fn actual_replay_storage_rewind_or_growth_after_seal_poison_verified_progress() {
    #[derive(serde::Serialize)]
    struct Watermark {
        schema_version: u16,
        cursor: FeedCursor,
    }
    let (temp, mut records) = recorded(false);
    records.push(FeedRecord::Seal {
        schema_version: 1,
        now_ns: 99,
        reason: "test finished".into(),
    });
    for grow in [false, true] {
        let path = temp.path().join(format!("watermark-{grow}"));
        write(&path, &records);
        let mut replay = HistoryReplay::open(&path, limits()).unwrap();
        let first = replay.progress().unwrap().cursor;
        let mut handoffs = 0;
        while let Some(step) = replay.read_next().unwrap() {
            if step.handoff.is_some() {
                handoffs += 1;
            }
        }
        assert!(handoffs > 0);
        assert!(replay.progress().unwrap().sealed);
        let mut cursor = if grow {
            replay.progress().unwrap().cursor
        } else {
            first
        };
        if grow {
            cursor.sequence += 1;
            cursor.offset += 1;
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
        assert!(matches!(replay.read_next(), Err(ReplayError::Recording(_))));
        assert!(replay.progress().is_err());
        assert!(replay.is_poisoned());
    }
}

#[test]
fn actual_replay_reads_corrupt_future_only_when_reaching_that_physical_record() {
    use std::io::{Read, Seek, SeekFrom, Write};
    let (temp, records) = recorded(true);
    let path = temp.path().join("corrupt");
    write(&path, &records[..2]);
    let file = path.join("events.mfsf");
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(file)
        .unwrap();
    f.seek(SeekFrom::End(-1)).unwrap();
    let mut b = [0];
    f.read_exact(&mut b).unwrap();
    b[0] ^= 1;
    f.seek(SeekFrom::End(-1)).unwrap();
    f.write_all(&b).unwrap();
    f.sync_all().unwrap();
    let mut replay =
        HistoryReplay::open(path, limits()).expect("bootstrap must not prescan future records");
    assert_eq!(replay.progress().unwrap().journal_ordinal, 0);
    assert!(matches!(replay.read_next(), Err(ReplayError::Recording(_))));
    assert!(replay.is_poisoned());
    assert!(replay.read_next().is_err());
}

#[test]
fn actual_replay_rejects_wrong_artifact_and_fabricated_resume_boundary() {
    let (temp, mut records) = recorded(true);
    let FeedRecord::Causal(envelope) = &records[0] else {
        panic!()
    };
    let mut dto = decode_bootstrap(envelope).unwrap();
    dto.build.executable_sha256 = "0".repeat(64);
    let bad = encode_bootstrap_for_profile(&dto, MAX_OPERATION_BYTES, FeedProfile::EngineCausalV3)
        .unwrap();
    records[0] = FeedRecord::Causal(bad);
    let path = temp.path().join("bad-build");
    write(&path, &records[..1]);
    assert!(matches!(
        HistoryReplay::open(path, limits()),
        Err(ReplayError::Bootstrap(_))
    ));
    let original = temp.path().join("original");
    let mut replay = HistoryReplay::open(&original, limits()).unwrap();
    replay.read_next().unwrap();
    let cursor = replay.progress().unwrap().cursor;
    let mut fabricated = cursor.clone();
    fabricated.offset += 1;
    assert!(HistoryReplay::resume(&original, fabricated, limits()).is_err());
    let mut foreign = cursor;
    foreign.feed_id = "0".repeat(40);
    assert!(HistoryReplay::resume(&original, foreign, limits()).is_err());
    let mut crossed = replay.progress().unwrap().cursor;
    crossed.sequence += 1;
    crossed.offset -= 1;
    assert!(HistoryReplay::resume(&original, crossed, limits()).is_err());
    while replay.read_next().unwrap().is_some() {}
    let mut future = replay.progress().unwrap().cursor;
    future.sequence += 1;
    future.offset += 8;
    assert!(HistoryReplay::resume(&original, future, limits()).is_err());
}
