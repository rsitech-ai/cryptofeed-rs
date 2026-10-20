use super::*;
use crate::shared_batch::{AdmissionFailure, BatchConfig, FeedBatchActor, PreparedFeedTransaction};
use std::time::Duration;

const CAUSAL: FeedProfile = FeedProfile::EngineCausalV1;
fn causal(session: u64, ordinal: u64, generation: u64) -> FeedRecord {
    FeedRecord::Causal(
        CausalEnvelope::new(
            if ordinal == 0 {
                CausalPhase::Bootstrap
            } else {
                CausalPhase::Operation
            },
            CausalRoute {
                venue: 5,
                session,
                ordinal,
                generation,
                bootstrap_id: "a".repeat(64),
            },
            // Opaque evidence: no roundtrip through a generic financial JSON value.
            br#"{"coefficient":"-170141183460469231731687303715884105728","scale":18}"#,
        )
        .unwrap(),
    )
}
fn change(record: &mut FeedRecord) -> &mut CausalEnvelope {
    let FeedRecord::Causal(envelope) = record else {
        panic!("causal fixture")
    };
    envelope
}
fn seal() -> FeedRecord {
    FeedRecord::Seal {
        schema_version: 1,
        now_ns: 9,
        reason: "complete".into(),
    }
}
fn legacy() -> FeedRecord {
    FeedRecord::artifact("legacy", b"bytes")
}
fn fixture() -> (tempfile::TempDir, PathBuf, FeedWriter) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let writer = FeedWriter::create_profile(&path, 64 * 1024 * 1024, CAUSAL).unwrap();
    (temp, path, writer)
}
fn files(path: &Path) -> (Vec<u8>, Vec<u8>) {
    (
        fs::read(path.join("events.mfsf")).unwrap(),
        fs::read(path.join("committed.json")).unwrap(),
    )
}
/// Write deliberately invalid but CRC-correct records, bypassing writer checks.
fn forged(path: &Path, records: Vec<FeedRecord>) -> FeedCursor {
    let mut bytes = fs::read(path.join("events.mfsf")).unwrap()[..48].to_vec();
    let mut cursor = FeedReader::watermark(path).unwrap();
    cursor.sequence = records.len() as u64;
    for (index, record) in records.into_iter().enumerate() {
        let body = serde_json::to_vec(&WireRecord {
            sequence: index as u64 + 1,
            record,
        })
        .unwrap();
        bytes.extend_from_slice(&(body.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&crc32c(&body).to_le_bytes());
        bytes.extend_from_slice(&body);
    }
    cursor.offset = bytes.len() as u64;
    fs::write(path.join("events.mfsf"), bytes).unwrap();
    fs::write(
        path.join("committed.json"),
        serde_json::to_vec(&Watermark {
            schema_version: 1,
            cursor: cursor.clone(),
        })
        .unwrap(),
    )
    .unwrap();
    cursor
}

#[test]
fn format_negotiation_precedes_records_and_is_symmetric() {
    let (_temp, path, mut writer) = fixture();
    assert_eq!(writer.profile(), CAUSAL);
    assert_eq!(&files(&path).0[..8], b"MFCJ1\0\0\0");
    assert!(FeedReader::open(&path, None).is_err());
    // Header mismatch wins even if the watermark cannot be read.
    fs::remove_file(path.join("committed.json")).unwrap();
    assert!(
        FeedReader::open(&path, None)
            .unwrap_err()
            .to_string()
            .contains("header")
    );
    writer.append_many(vec![causal(1, 0, 0), seal()]).unwrap();
    let bytes = files(&path).0;
    assert!(SealedFeedReader::new(&bytes, writer.cursor().clone()).is_err());
    assert!(SealedFeedReader::new_profile(&bytes, writer.cursor().clone(), CAUSAL).is_ok());
    let legacy_path = path.with_file_name("legacy");
    let mut old = FeedWriter::create(&legacy_path, 1_000_000).unwrap();
    old.append(seal()).unwrap();
    assert!(FeedReader::open_profile(&legacy_path, None, CAUSAL).is_err());
    assert!(
        SealedFeedReader::new_profile(&files(&legacy_path).0, old.cursor().clone(), CAUSAL)
            .is_err()
    );
}

#[test]
fn owner_profile_negotiates_before_watermark_and_keeps_one_journal_ordinal() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("owner");
    let profile = FeedProfile::EngineCausalV2;
    let mut writer = FeedWriter::create_profile(&path, 64 * 1024 * 1024, profile).unwrap();
    assert_eq!(&files(&path).0[..8], b"MFCJ2\0\0\0");
    for old in [FeedProfile::Legacy, CAUSAL] {
        assert!(FeedReader::open_profile(&path, None, old).is_err());
    }
    fs::remove_file(path.join("committed.json")).unwrap();
    for old in [FeedProfile::Legacy, CAUSAL] {
        assert!(
            FeedReader::open_profile(&path, None, old)
                .unwrap_err()
                .to_string()
                .contains("header")
        );
    }
    let mut bootstrap = causal(1, 0, 0);
    change(&mut bootstrap).schema_version = 2;
    writer.append(bootstrap).unwrap();
    let mut first = causal(1, 1, 0);
    change(&mut first).schema_version = 2;
    change(&mut first).phase = CausalPhase::Owner;
    writer.append(first).unwrap();
    let prefix = writer.cursor().clone();
    let mut operation = causal(1, 2, 1);
    change(&mut operation).schema_version = 2;
    writer.append(operation).unwrap();
    let before = (files(&path), writer.cursor().clone());
    let mut illegal = causal(1, 3, 2);
    change(&mut illegal).schema_version = 2;
    change(&mut illegal).phase = CausalPhase::Owner;
    assert!(writer.append(illegal.clone()).is_err());
    assert_eq!((files(&path), writer.cursor().clone()), before);
    change(&mut illegal).route.generation = 1;
    writer.append_many(vec![illegal, seal()]).unwrap();
    let bytes = files(&path).0;
    assert!(SealedFeedReader::new_profile(&bytes, writer.cursor().clone(), CAUSAL).is_err());
    let mut sealed =
        SealedFeedReader::new_profile(&bytes, writer.cursor().clone(), profile).unwrap();
    let mut count = 0;
    while sealed.read_next().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 5);
    let mut resumed = FeedReader::open_profile(&path, Some(prefix), profile).unwrap();
    while resumed.read_next().unwrap().is_some() {}
    assert_eq!(resumed.cursor(), writer.cursor());
    assert_eq!(resumed.routes, writer.routes);
}

#[test]
fn owner_envelope_and_legacy_causal_profile_cannot_be_mixed() {
    let (_temp, path, mut writer) = fixture();
    let mut owner = causal(1, 0, 0);
    change(&mut owner).schema_version = 2;
    let before = files(&path);
    assert!(writer.append(owner).is_err());
    assert_eq!(files(&path), before);
    assert!(
        CausalEnvelope::new(
            CausalPhase::Owner,
            change(&mut causal(1, 1, 0)).route.clone(),
            b"x"
        )
        .is_err()
    );
    assert!(
        CausalEnvelope::for_profile(
            FeedProfile::Legacy,
            CausalPhase::Operation,
            change(&mut causal(1, 1, 0)).route.clone(),
            b"x"
        )
        .is_err()
    );
}

#[test]
fn opaque_payload_and_interleaved_routes_survive_live_sealed_and_storage_resume() {
    let (_temp, path, mut writer) = fixture();
    let bootstrap = writer.append(causal(1, 0, 0)).unwrap();
    writer
        .append_many(vec![causal(2, 0, 0), causal(1, 1, 1), causal(2, 1, 0)])
        .unwrap();
    let middle = writer.cursor().clone();
    writer
        .append_many(vec![causal(1, 2, 1), causal(2, 2, 1), seal()])
        .unwrap();
    let mut resumed = FeedReader::open_profile(&path, Some(bootstrap), CAUSAL).unwrap();
    assert_eq!(resumed.routes.0.len(), 1);
    let mut resumed_middle = FeedReader::open_profile(&path, Some(middle), CAUSAL).unwrap();
    assert_eq!(resumed_middle.routes.0.len(), 2);
    for reader in [&mut resumed, &mut resumed_middle] {
        while reader.read_next().unwrap().is_some() {}
        assert_eq!(reader.routes, writer.routes);
        assert_eq!(reader.cursor(), writer.cursor());
    }
    let bytes = files(&path).0;
    let mut immutable =
        SealedFeedReader::new_profile(&bytes, writer.cursor().clone(), CAUSAL).unwrap();
    let mut count = 0;
    while let Some(record) = immutable.read_next().unwrap() {
        if let FeedRecord::Causal(envelope) = record {
            assert_eq!(
                envelope.payload().unwrap(),
                change(&mut causal(1, 0, 0)).payload().unwrap()
            );
            count += 1;
        }
    }
    assert_eq!(count, 6);
    assert_eq!(immutable.routes, writer.routes);
}

#[test]
fn a_late_invalid_member_leaves_all_writer_state_and_bytes_unchanged() {
    let (_temp, path, mut writer) = fixture();
    let before = (writer.cursor().clone(), writer.routes.clone(), files(&path));
    assert!(
        writer
            .append_many(vec![causal(1, 0, 0), causal(1, 1, 0), causal(2, 1, 0)])
            .is_err()
    );
    assert_eq!(writer.cursor(), &before.0);
    assert_eq!(writer.routes, before.1);
    assert_eq!(files(&path), before.2);
    assert!(!writer.poisoned);
    writer
        .append_many(vec![causal(1, 0, 0), causal(1, 1, 0)])
        .unwrap();
    assert_eq!(writer.routes.0[&1].ordinal, 1);
}

#[test]
fn invalid_routing_and_envelopes_reject_before_writing_without_poison() {
    let (_temp, path, mut writer) = fixture();
    writer
        .append_many(vec![causal(1, 0, 0), causal(1, 1, 1)])
        .unwrap();
    let mut bad = vec![
        causal(1, 0, 0),
        causal(2, 1, 0),
        causal(1, 1, 1),
        causal(1, 3, 1),
        causal(1, 2, 0),
        causal(1, 2, 3),
        legacy(),
    ];
    for mutation in 0..8 {
        let mut record = causal(1, 2, 1);
        let e = change(&mut record);
        match mutation {
            0 => e.route.venue = 6,
            1 => e.route.bootstrap_id = "b".repeat(64),
            2 => e.schema_version = 2,
            3 => e.route.session = 0,
            4 => e.route.bootstrap_id = "A".repeat(64),
            5 => e.payload_base64 = "%%%=".into(),
            6 => e.payload_base64.clear(),
            7 => e.phase = CausalPhase::Bootstrap,
            _ => unreachable!(),
        }
        bad.push(record);
    }
    let before = (writer.cursor().clone(), writer.routes.clone(), files(&path));
    for record in bad {
        assert!(writer.append(record).is_err());
        assert_eq!(writer.cursor(), &before.0);
        assert_eq!(writer.routes, before.1);
        assert_eq!(files(&path), before.2);
        assert!(!writer.poisoned);
    }
    writer.append(causal(1, 2, 2)).unwrap();
}

#[test]
fn phase_zero_and_bootstrap_identity_cannot_be_rebound() {
    let (_temp, path, mut writer) = fixture();
    let mut zero_op = causal(1, 0, 0);
    change(&mut zero_op).phase = CausalPhase::Operation;
    let mut nonzero_boot = causal(1, 0, 0);
    change(&mut nonzero_boot).route.generation = 1;
    for bad in [zero_op, nonzero_boot] {
        assert!(writer.append(bad).is_err());
    }
    writer.append(causal(1, 0, 0)).unwrap();
    let before = files(&path);
    let mut rebound = causal(1, 0, 0);
    change(&mut rebound).route.venue = 6;
    assert!(writer.append(rebound).is_err());
    assert_eq!(files(&path), before);
}

#[test]
fn both_readers_reject_crc_correct_bad_routing_without_advancing_any_state() {
    for bad in [causal(1, 2, 0), causal(2, 1, 0), legacy()] {
        let (_temp, path, _writer) = fixture();
        let checkpoint = forged(&path, vec![causal(1, 0, 0), bad, seal()]);
        let mut reader = FeedReader::open_profile(&path, None, CAUSAL).unwrap();
        reader.read_next().unwrap();
        let before = (
            reader.cursor.clone(),
            reader.routes.clone(),
            reader.seen_commit.clone(),
            reader.sealed,
        );
        let bytes = files(&path).0;
        let mut immutable = SealedFeedReader::new_profile(&bytes, checkpoint, CAUSAL).unwrap();
        immutable.read_next().unwrap();
        for _ in 0..2 {
            assert!(reader.read_next().is_err());
            assert_eq!(
                (
                    reader.cursor.clone(),
                    reader.routes.clone(),
                    reader.seen_commit.clone(),
                    reader.sealed
                ),
                before
            );
            assert!(immutable.read_next().is_err());
            assert_eq!(immutable.cursor, before.0);
            assert_eq!(immutable.routes, before.1);
            assert!(!immutable.sealed);
        }
    }
}

#[test]
fn malformed_seal_cannot_commit_staged_reader_state() {
    let (_temp, path, _writer) = fixture();
    let mut bad = seal();
    let FeedRecord::Seal { schema_version, .. } = &mut bad else {
        unreachable!()
    };
    *schema_version = 2;
    let checkpoint = forged(&path, vec![causal(1, 0, 0), bad]);
    let mut reader = FeedReader::open_profile(&path, None, CAUSAL).unwrap();
    reader.read_next().unwrap();
    let before = reader.cursor().clone();
    assert!(reader.read_next().is_err());
    assert_eq!(reader.cursor(), &before);
    assert!(!reader.sealed);
    let bytes = files(&path).0;
    let mut immutable = SealedFeedReader::new_profile(&bytes, checkpoint, CAUSAL).unwrap();
    immutable.read_next().unwrap();
    assert!(immutable.read_next().is_err());
    assert_eq!(immutable.cursor, before);
    assert!(!immutable.sealed);
}

#[test]
fn mismatched_final_sequence_and_offset_reject_before_exposure_in_both_profiles() {
    for profile in [FeedProfile::Legacy, CAUSAL] {
        for records_in_file in [1, 2] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("feed");
            let _writer = FeedWriter::create_profile(&path, 1_000_000, profile).unwrap();
            let records = if profile == CAUSAL {
                vec![causal(1, 0, 0), causal(1, 1, 0)]
            } else {
                vec![legacy(), legacy()]
            };
            let mut checkpoint = forged(&path, records.into_iter().take(records_in_file).collect());
            checkpoint.sequence = if records_in_file == 1 { 2 } else { 1 };
            fs::write(
                path.join("committed.json"),
                serde_json::to_vec(&Watermark {
                    schema_version: 1,
                    cursor: checkpoint.clone(),
                })
                .unwrap(),
            )
            .unwrap();
            let mut live = FeedReader::open_profile(&path, None, profile).unwrap();
            let before = live.cursor.clone();
            let bytes = files(&path).0;
            let mut immutable = SealedFeedReader::new_profile(&bytes, checkpoint, profile).unwrap();
            for _ in 0..2 {
                assert!(live.read_next().is_err());
                assert_eq!(live.cursor, before);
                assert!(live.routes.0.is_empty());
                assert!(!live.sealed);
                assert!(immutable.read_next().is_err());
                assert_eq!(immutable.cursor, before);
                assert!(immutable.routes.0.is_empty());
                assert!(!immutable.sealed);
            }
        }
    }
}

#[test]
fn post_replace_failure_exposes_whole_route_without_ack_or_writer_routing_advance() {
    let (_temp, path, mut writer) = fixture();
    let before = writer.cursor().clone();
    writer.failpoint = Some(PublicationFailpoint::DirectorySync);
    assert!(matches!(
        writer.append_many(vec![causal(1, 0, 0), causal(1, 1, 1)]),
        Err(RecordingError::PublicationIndeterminate(_))
    ));
    assert_eq!(writer.cursor(), &before);
    assert!(writer.routes.0.is_empty());
    assert!(writer.poisoned);
    let mut reader = FeedReader::open_profile(&path, None, CAUSAL).unwrap();
    assert!(reader.read_next().unwrap().is_some());
    assert!(reader.read_next().unwrap().is_some());
    assert!(reader.read_next().unwrap().is_none());
    assert_eq!(reader.routes.0[&1].ordinal, 1);
    writer.failpoint = None;
    assert!(writer.append(causal(1, 2, 1)).is_err());
}

#[test]
fn route_budget_retains_old_sessions_and_rejects_4097th_before_mutation() {
    let (_temp, path, mut writer) = fixture();
    writer
        .append_many(
            (1..=MAX_CAUSAL_ROUTES as u64)
                .map(|s| causal(s, 0, 0))
                .collect(),
        )
        .unwrap();
    assert_eq!(writer.routes.0.len(), MAX_CAUSAL_ROUTES);
    let before = files(&path);
    assert!(
        writer
            .append(causal(MAX_CAUSAL_ROUTES as u64 + 1, 0, 0))
            .is_err()
    );
    assert_eq!(files(&path), before);
    writer.append(causal(1, 1, 0)).unwrap();
    assert_eq!(writer.routes.0.len(), MAX_CAUSAL_ROUTES);
}

#[test]
fn checked_route_extrema_accept_unchanged_max_generation_and_reject_wraparound() {
    let mut routes = CausalRoutes::default();
    let mut last = causal(1, u64::MAX - 1, u64::MAX);
    routes.0.insert(1, change(&mut last).route.clone());
    let changes = routes
        .prepare(CAUSAL, [&causal(1, u64::MAX, u64::MAX)])
        .unwrap();
    routes.install(changes);
    assert!(routes.prepare(CAUSAL, [&causal(1, 1, 0)]).is_err());
    assert_eq!(routes.0[&1].ordinal, u64::MAX);
    let mut last = causal(2, 1, u64::MAX - 1);
    routes.0.insert(2, change(&mut last).route.clone());
    let changes = routes.prepare(CAUSAL, [&causal(2, 2, u64::MAX)]).unwrap();
    routes.install(changes);
    assert!(routes.prepare(CAUSAL, [&causal(2, 3, 0)]).is_err());
}

#[test]
fn legacy_writers_and_crc_correct_legacy_readers_reject_causal_records() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("legacy");
    let mut writer = FeedWriter::create(&path, 1_000_000).unwrap();
    let before = files(&path);
    assert!(writer.append(causal(1, 0, 0)).is_err());
    assert_eq!(files(&path), before);
    let checkpoint = forged(&path, vec![causal(1, 0, 0), seal()]);
    let mut reader = FeedReader::open(&path, None).unwrap();
    assert!(reader.read_next().is_err());
    assert_eq!(reader.cursor().sequence, 0);
    let bytes = files(&path).0;
    assert!(
        SealedFeedReader::new(&bytes, checkpoint)
            .unwrap()
            .read_next()
            .is_err()
    );
}

#[test]
fn prepared_transactions_validate_intrinsics_and_actor_rejects_profile_before_admission() {
    let (_temp, _path, writer) = fixture();
    let actor = FeedBatchActor::spawn(
        writer,
        BatchConfig {
            max_pending_operations: 4,
            max_pending_bytes: 1_000_000,
            max_group_operations: 1,
            max_group_bytes: 1_000_000,
            max_age: Duration::ZERO,
        },
    )
    .unwrap();
    let handle = actor.handle();
    assert_eq!(handle.profile(), CAUSAL);
    let rejected = handle
        .try_submit(PreparedFeedTransaction::new(vec![legacy()]).unwrap())
        .unwrap_err();
    assert_eq!(rejected.reason, AdmissionFailure::ProfileMismatch);
    assert_eq!(handle.status().admitted_operations, 0);
    assert!(PreparedFeedTransaction::new(vec![causal(1, 0, 0), legacy()]).is_err());
    let mut bad = causal(1, 0, 0);
    change(&mut bad).schema_version = 2;
    let rejected = handle
        .try_submit(PreparedFeedTransaction::new(vec![bad]).unwrap())
        .unwrap_err();
    assert_eq!(rejected.reason, AdmissionFailure::ProfileMismatch);
    assert_eq!(handle.status().admitted_operations, 0);
    let mut bad = causal(1, 0, 0);
    change(&mut bad).schema_version = 3;
    assert!(PreparedFeedTransaction::new(vec![bad]).is_err());
    let ticket = handle
        .try_submit(
            PreparedFeedTransaction::new(vec![causal(1, 0, 0), causal(1, 1, 1), seal()]).unwrap(),
        )
        .unwrap();
    let receipt = ticket.wait().unwrap();
    assert_eq!((receipt.first_sequence, receipt.last_sequence), (1, 3));
    assert_eq!(actor.shutdown().unwrap().published, receipt.published);
}

#[test]
fn strict_envelope_schema_and_bounded_base64_preserve_all_opaque_bytes() {
    let mut envelope = change(&mut causal(1, 0, 0)).clone();
    for len in [1, 2, 3, 3071, 3072, 3073, 6144, 6145] {
        let bytes: Vec<u8> = (0..len).map(|n| (n % 256) as u8).collect();
        envelope.payload_base64 = STANDARD.encode(&bytes);
        envelope.validate().unwrap();
        assert_eq!(envelope.payload().unwrap(), bytes);
    }
    for invalid in ["YQ", "YR==", "YQ==YQ==", "!!!!"] {
        envelope.payload_base64 = invalid.into();
        assert!(envelope.validate().is_err());
    }
    envelope.payload_base64 = format!(
        "{}{}",
        STANDARD.encode(vec![1; 3071]),
        STANDARD.encode(b"x")
    );
    assert!(envelope.validate().is_err());
    envelope.payload_base64 = "A".repeat(MAX_CAUSAL_ENCODED_PAYLOAD + 1);
    assert!(envelope.validate().is_err());
    assert!(envelope.payload().is_err());
    let record = causal(1, 0, 0);
    let json = serde_json::to_string(&record).unwrap();
    assert!(
        serde_json::from_str::<FeedRecord>(&json.replace(
            "\"schema_version\":1",
            "\"schema_version\":1,\"unknown\":true"
        ))
        .is_err()
    );
    assert!(
        serde_json::from_str::<FeedRecord>(
            &json.replace("\"venue\":5", "\"venue\":5,\"unknown\":true")
        )
        .is_err()
    );
    assert!(
        serde_json::from_str::<FeedRecord>(&json.replace("\"bootstrap\"", "\"unknown_phase\""))
            .is_err()
    );
}
