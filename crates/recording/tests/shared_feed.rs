use marketfeed_adapter_api::EventBatch;
use marketfeed_model::*;
use marketfeed_recording::shared_feed::*;
use std::{
    fs::{self, OpenOptions},
    io::{Seek, SeekFrom, Write},
};
fn state(s: &str) -> FeedRecord {
    FeedRecord::Lifecycle {
        session: SessionId(7),
        now_ns: 1,
        state: s.into(),
    }
}
fn check_stage(stage: IoStageTiming, calls: u64, errors: u64) {
    assert_eq!(stage.completed_calls, calls);
    assert_eq!(stage.errors, errors);
    assert_eq!(stage.latency_buckets.iter().sum::<u64>(), calls);
    assert!(stage.max_ns <= stage.total_ns);
}

#[test]
fn io_observations_distinguish_validation_from_acknowledged_publication() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("feed");
    let handle = FeedHandle::new(FeedWriter::create(&dir, 100_000).unwrap());
    assert_eq!(handle.timings().unwrap().append_attempts, 0);
    handle
        .append_many(vec![state("one"), state("two")])
        .unwrap();
    assert!(handle.append_many(vec![]).is_err());
    assert!(
        handle
            .append_many(vec![
                FeedRecord::Seal {
                    schema_version: 1,
                    now_ns: 1,
                    reason: "invalid non-final seal".into(),
                },
                state("after seal")
            ])
            .is_err()
    );
    handle
        .append(FeedRecord::Seal {
            schema_version: 1,
            now_ns: 2,
            reason: "complete".into(),
        })
        .unwrap();
    assert!(handle.append(state("late")).is_err());
    let timing = handle.timings().unwrap();
    assert_eq!(timing.append_attempts, 5);
    assert_eq!(timing.accepted_transactions, 2);
    assert_eq!(timing.accepted_records, 3);
    assert_eq!(timing.failed_attempts, 3);
    check_stage(timing.lock_wait, 5, 0);
    check_stage(timing.append, 5, 3);
    check_stage(timing.encode, 3, 1);
    for stage in [
        timing.write,
        timing.data_sync,
        timing.watermark_write_sync,
        timing.replace,
        timing.directory_sync,
    ] {
        check_stage(stage, 2, 0);
    }
    let mut reader = FeedReader::open(&dir, None).unwrap();
    for _ in 0..3 {
        assert!(reader.read_next().unwrap().is_some());
    }
    assert!(reader.read_next().unwrap().is_none());
}

#[test]
fn failed_watermark_observations_never_count_a_durable_acknowledgment() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("feed");
    let handle = FeedHandle::new(FeedWriter::create(&dir, 100_000).unwrap());
    fs::write(dir.join("committed.tmp"), b"collision").unwrap();
    assert!(handle.append(state("uncommitted")).is_err());
    assert!(handle.append(state("poisoned")).is_err());
    let timing = handle.timings().unwrap();
    assert_eq!(timing.append_attempts, 2);
    assert_eq!(timing.accepted_records, 0);
    assert_eq!(timing.accepted_transactions, 0);
    assert_eq!(timing.failed_attempts, 2);
    check_stage(timing.append, 2, 2);
    for stage in [timing.encode, timing.write, timing.data_sync] {
        check_stage(stage, 1, 0);
    }
    check_stage(timing.watermark_write_sync, 1, 1);
    check_stage(timing.replace, 0, 0);
    check_stage(timing.directory_sync, 0, 0);
    assert!(
        FeedReader::open(&dir, None)
            .unwrap()
            .read_next()
            .unwrap()
            .is_none()
    );
}
#[test]
fn readers_only_observe_synced_prefix_and_resume_exactly() {
    let t = tempfile::tempdir().unwrap();
    let dir = t.path().join("feed");
    let mut w = FeedWriter::create(&dir, 1_000_000).unwrap();
    let mut r = FeedReader::open(&dir, None).unwrap();
    assert!(r.read_next().unwrap().is_none());
    w.append(state("connected")).unwrap();
    assert!(matches!(
        r.read_next().unwrap(),
        Some(FeedRecord::Lifecycle { .. })
    ));
    let cursor = r.cursor().clone();
    w.append(state("disconnected")).unwrap();
    let mut resumed = FeedReader::open(&dir, Some(cursor.clone())).unwrap();
    assert!(resumed.read_next().unwrap().is_some());
    assert!(resumed.read_next().unwrap().is_none());
    let mut wrong = cursor.clone();
    wrong.feed_id = "other".into();
    assert!(FeedReader::open(&dir, Some(wrong)).is_err());
    let mut wrong = cursor;
    wrong.offset += 1;
    assert!(FeedReader::open(&dir, Some(wrong)).is_err());
    let before = fs::read(dir.join("events.mfsf")).unwrap();
    assert!(FeedWriter::create(&dir, 1_000_000).is_err());
    assert_eq!(before, fs::read(dir.join("events.mfsf")).unwrap());
}
#[test]
fn torn_uncommitted_tail_is_ignored_but_committed_corruption_fails() {
    let t = tempfile::tempdir().unwrap();
    let dir = t.path().join("feed");
    let mut w = FeedWriter::create(&dir, 1_000_000).unwrap();
    w.append(state("one")).unwrap();
    drop(w);
    OpenOptions::new()
        .append(true)
        .open(dir.join("events.mfsf"))
        .unwrap()
        .write_all(&[9, 1, 0])
        .unwrap();
    let mut r = FeedReader::open(&dir, None).unwrap();
    assert!(r.read_next().unwrap().is_some());
    assert!(r.read_next().unwrap().is_none());
    let mut f = OpenOptions::new()
        .write(true)
        .open(dir.join("events.mfsf"))
        .unwrap();
    f.seek(SeekFrom::Start(60)).unwrap();
    f.write_all(b"!").unwrap();
    assert!(FeedReader::open(&dir, None).unwrap().read_next().is_err());
}
#[test]
fn watermark_failure_poisoning_prevents_later_false_commit() {
    let t = tempfile::tempdir().unwrap();
    let dir = t.path().join("feed");
    let mut w = FeedWriter::create(&dir, 1_000_000).unwrap();
    fs::write(dir.join("committed.tmp"), b"leftover-or-collision").unwrap();
    assert!(w.append(state("uncommitted")).is_err());
    assert!(w.append(state("must-not-commit-tail")).is_err());
    let mut r = FeedReader::open(&dir, None).unwrap();
    assert!(r.read_next().unwrap().is_none());
}
#[test]
fn bytes_are_original_and_signed_i128_extrema_roundtrip() {
    let t = tempfile::tempdir().unwrap();
    let dir = t.path().join("feed");
    let mut w = FeedWriter::create(&dir, 1_000_000).unwrap();
    let original = b" { \"px\" : 1.000000000000000000001 }\n";
    w.append(FeedRecord::frame(SessionId(3), 5, 6, 7, 0, 1, original))
        .unwrap();
    let batch = EventBatch {
        session: SessionId(3),
        frame_seq: 5,
        events: vec![EventEnvelope {
            schema_version: 1,
            venue: VenueId(21),
            instrument: Some(InstrumentId(1)),
            connection: ConnectionId(3),
            session: SessionId(3),
            frame_seq: 5,
            event_index: 0,
            exchange_ts: None,
            receive_ts: TimestampNs(6),
            source_sequence: None,
            flags: EventFlags::empty(),
            payload: MarketEvent::Trade(Trade {
                provenance: None,
                price: Price(Fixed::new(i128::MAX, 38)),
                quantity: Quantity(Fixed::new(i128::MIN, 255)),
                aggressor: AggressorSide::Unknown,
                trade_id: Some(SourceId("90071992547409931234567890".into())),
            }),
        }],
    };
    w.append(FeedRecord::Batch(batch.clone())).unwrap();
    let mut r = FeedReader::open(&dir, None).unwrap();
    assert_eq!(
        r.read_next()
            .unwrap()
            .unwrap()
            .payload_bytes()
            .unwrap()
            .unwrap(),
        original
    );
    let FeedRecord::Batch(actual) = r.read_next().unwrap().unwrap() else {
        panic!()
    };
    assert_eq!(actual, batch);
}
#[test]
fn byte_budget_and_commit_rewind_fail_closed() {
    let t = tempfile::tempdir().unwrap();
    let dir = t.path().join("feed");
    let mut w = FeedWriter::create(&dir, 2000).unwrap();
    let initial = fs::read(dir.join("committed.json")).unwrap();
    w.append(state("one")).unwrap();
    let mut r = FeedReader::open(&dir, None).unwrap();
    r.read_next().unwrap();
    fs::write(dir.join("committed.json"), initial).unwrap();
    assert!(r.read_next().is_err());
    assert!(w.append(state(&"x".repeat(3000))).is_err());
    assert!(w.append(state("later")).is_err());
}

#[test]
fn a_seal_is_final_and_cannot_be_followed_by_more_records() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("feed");
    let mut writer = FeedWriter::create(&dir, 1_000_000).unwrap();
    writer
        .append(FeedRecord::Seal {
            schema_version: 1,
            now_ns: 1,
            reason: "clean shutdown".into(),
        })
        .unwrap();
    assert!(writer.append(state("late")).is_err());
    let mut reader = FeedReader::open(&dir, None).unwrap();
    assert!(matches!(
        reader.read_next().unwrap(),
        Some(FeedRecord::Seal { .. })
    ));
    assert!(reader.read_next().unwrap().is_none());
}

#[test]
fn multi_record_budget_or_publish_failure_exposes_none_of_the_transaction() {
    for publication_failure in [false, true] {
        let t = tempfile::tempdir().unwrap();
        let dir = t.path().join("feed");
        let mut w =
            FeedWriter::create(&dir, if publication_failure { 10000 } else { 500 }).unwrap();
        w.append(state("prior")).unwrap();
        let committed = w.cursor().clone();
        if publication_failure {
            fs::write(dir.join("committed.tmp"), b"failure").unwrap();
        }
        assert!(
            w.append_many(vec![state("first"), state(&"large".repeat(100))])
                .is_err()
        );
        assert_eq!(w.cursor(), &committed);
        assert!(w.append(state("late")).is_err());
        let mut r = FeedReader::open(&dir, None).unwrap();
        assert!(r.read_next().unwrap().is_some());
        assert!(r.read_next().unwrap().is_none());
    }
}

#[test]
fn concurrently_created_capture_ids_have_globally_scoped_random_identifiers() {
    let root = tempfile::tempdir().unwrap();
    let mut tasks = Vec::new();
    for index in 0..16 {
        let path = root.path().join(index.to_string());
        tasks.push(std::thread::spawn(move || {
            FeedWriter::create(path, 10000)
                .unwrap()
                .cursor()
                .feed_id
                .clone()
        }));
    }
    let ids: std::collections::BTreeSet<_> =
        tasks.into_iter().map(|task| task.join().unwrap()).collect();
    assert_eq!(ids.len(), 16);
    assert!(
        ids.iter()
            .all(|id| id.len() == 40 && id.bytes().all(|b| b.is_ascii_hexdigit()))
    );
}

#[test]
fn immutable_reader_requires_the_pinned_complete_sealed_prefix() {
    let t = tempfile::tempdir().unwrap();
    let directory = t.path().join("feed");
    let mut writer = FeedWriter::create(&directory, 1_000_000).unwrap();
    writer.append(state("connected")).unwrap();
    let unsealed = fs::read(directory.join("events.mfsf")).unwrap();
    let mut reader = SealedFeedReader::new(&unsealed, writer.cursor().clone()).unwrap();
    assert!(reader.read_next().unwrap().is_some());
    assert!(reader.read_next().is_err());
    writer
        .append(FeedRecord::Seal {
            schema_version: 1,
            now_ns: 2,
            reason: "unit".into(),
        })
        .unwrap();
    let bytes = fs::read(directory.join("events.mfsf")).unwrap();
    let checkpoint = writer.cursor().clone();
    let mut reader = SealedFeedReader::new(&bytes, checkpoint.clone()).unwrap();
    assert!(reader.read_next().unwrap().is_some());
    assert!(matches!(
        reader.read_next().unwrap(),
        Some(FeedRecord::Seal { .. })
    ));
    assert!(reader.read_next().unwrap().is_none());
    assert!(SealedFeedReader::new(&bytes[..bytes.len() - 1], checkpoint.clone()).is_err());
    let mut wrong = checkpoint.clone();
    wrong.sequence += 1;
    let mut reader = SealedFeedReader::new(&bytes, wrong).unwrap();
    assert!(reader.read_next().unwrap().is_some());
    assert!(reader.read_next().is_err());
    let mut corrupted = bytes;
    corrupted[60] ^= 1;
    let mut reader = SealedFeedReader::new(&corrupted, checkpoint).unwrap();
    assert!(reader.read_next().is_err());
}
