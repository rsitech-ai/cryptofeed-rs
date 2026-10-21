use super::*;
use crate::shared_batch::*;
use std::time::Duration;
const PROFILE: FeedProfile = FeedProfile::ObservationProjectionV1;
fn envelope(sequence: u64, sealed: bool) -> ProjectionEnvelope {
    ProjectionEnvelope::new(
        FeedCursor {
            feed_id: "a".repeat(40),
            sequence,
            offset: 48 + sequence * 100,
        },
        FeedCursor {
            feed_id: "a".repeat(40),
            sequence: sequence + 1,
            offset: 148 + sequence * 100,
        },
        sealed,
        b"opaque typed group lives above recording",
    )
    .unwrap()
}
fn config() -> BatchConfig {
    BatchConfig {
        max_pending_operations: 2,
        max_pending_bytes: 65536,
        max_group_operations: 2,
        max_group_bytes: 65536,
        max_age: Duration::from_millis(1),
    }
}

#[test]
fn projection_profile_negotiates_before_watermark_and_refuses_independent_seal_admission() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("target");
    let writer = FeedWriter::create_profile(&path, 65536, PROFILE).unwrap();
    let watermark = std::fs::read(path.join("committed.json")).unwrap();
    std::fs::remove_file(path.join("committed.json")).unwrap();
    for old in [
        FeedProfile::Legacy,
        FeedProfile::EngineCausalV1,
        FeedProfile::EngineCausalV2,
        FeedProfile::EngineCausalV3,
        FeedProfile::EngineCausalV4,
    ] {
        assert!(
            matches!(FeedReader::open_profile(&path,None,old),Err(RecordingError::Io(message)) if message.contains("unsupported shared feed header"))
        );
    }
    std::fs::write(path.join("committed.json"), watermark).unwrap();
    let seal = FeedRecord::Seal {
        schema_version: 1,
        now_ns: 1,
        reason: "forbidden independent seal".into(),
    };
    assert!(
        PreparedFeedTransaction::new(vec![
            FeedRecord::Projection(envelope(0, false)),
            seal.clone()
        ])
        .is_err()
    );
    let actor = FeedBatchActor::spawn(writer, config()).unwrap();
    let handle = actor.handle();
    let rejected = handle
        .try_submit(PreparedFeedTransaction::new(vec![seal]).unwrap())
        .unwrap_err();
    assert_eq!(rejected.reason, AdmissionFailure::ProfileMismatch);
    assert_eq!(handle.status().admitted_operations, 0);
    assert!(handle.status().failure.is_none());
    handle
        .try_submit(
            PreparedFeedTransaction::new(vec![FeedRecord::Projection(envelope(0, true))]).unwrap(),
        )
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(handle.status().committed_operations, 1);
    actor.shutdown().unwrap();
    let mut reader = FeedReader::open_profile(&path, None, PROFILE).unwrap();
    assert!(
        matches!(reader.read_next().unwrap(),Some(FeedRecord::Projection(e)) if e.source_sealed)
    );
    assert!(reader.read_next().unwrap().is_none());
}

#[test]
fn projection_atomic_cursor_chain_rejects_bad_groups_without_partial_publication() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("target");
    let mut writer = FeedWriter::create_profile(&path, 65536, PROFILE).unwrap();
    let header = writer.cursor().clone();
    assert!(
        writer
            .append_many(vec![
                FeedRecord::Projection(envelope(0, false)),
                FeedRecord::Projection(envelope(2, true))
            ])
            .is_err()
    );
    assert_eq!(writer.cursor(), &header);
    assert_eq!(
        std::fs::metadata(path.join("events.mfsf")).unwrap().len(),
        48
    );
    let first = writer
        .append(FeedRecord::Projection(envelope(0, false)))
        .unwrap();
    let last = writer
        .append(FeedRecord::Projection(envelope(1, true)))
        .unwrap();
    assert!(
        writer
            .append(FeedRecord::Projection(envelope(2, false)))
            .is_err()
    );
    let mut resumed = FeedReader::open_profile(&path, Some(first), PROFILE).unwrap();
    assert!(
        matches!(resumed.read_next().unwrap(),Some(FeedRecord::Projection(e)) if e.source_after.sequence==2)
    );
    assert!(resumed.read_next().unwrap().is_none());
    let bytes = std::fs::read(path.join("events.mfsf")).unwrap();
    let mut sealed = SealedFeedReader::new_profile(&bytes, last, PROFILE).unwrap();
    let mut count = 0;
    while sealed.read_next().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 2);
    for wrong in [
        FeedProfile::Legacy,
        FeedProfile::EngineCausalV3,
        FeedProfile::EngineCausalV4,
    ] {
        let p = temp.path().join(format!("wrong-{wrong:?}"));
        let mut old = FeedWriter::create_profile(p, 65536, wrong).unwrap();
        assert!(
            old.append(FeedRecord::Projection(envelope(0, false)))
                .is_err()
        );
        assert_eq!(old.cursor().sequence, 0);
    }
}

#[test]
fn projection_actual_publication_failpoints_never_expose_half_group_or_ack() {
    for point in [
        PublicationFailpoint::Write,
        PublicationFailpoint::PartialWrite,
        PublicationFailpoint::DataSync,
        PublicationFailpoint::WatermarkWrite,
        PublicationFailpoint::WatermarkSync,
        PublicationFailpoint::Replace,
        PublicationFailpoint::DirectorySync,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("target");
        let mut writer = FeedWriter::create_profile(&path, 65536, PROFILE).unwrap();
        let initial = writer.cursor().clone();
        writer.failpoint = Some(point);
        let actor = FeedBatchActor::spawn(writer, config()).unwrap();
        let handle = actor.handle();
        let expected = envelope(0, true);
        let failed = handle
            .try_submit(
                PreparedFeedTransaction::new(vec![FeedRecord::Projection(expected.clone())])
                    .unwrap(),
            )
            .unwrap()
            .wait()
            .unwrap_err();
        assert!(matches!(
            failed.disposition,
            CommitDisposition::Attempted {
                first_sequence: 1,
                last_sequence: 1
            }
        ));
        assert_eq!(failed.failure.last_acknowledged, initial);
        assert_eq!(handle.status().committed_operations, 0);
        let mut reader = FeedReader::open_profile(&path, None, PROFILE).unwrap();
        let observed = reader.read_next().unwrap();
        if point == PublicationFailpoint::DirectorySync {
            assert!(matches!(observed,Some(FeedRecord::Projection(e)) if e==expected));
            assert!(matches!(
                failed.failure.cause,
                BatchFailureCause::Recording(RecordingError::PublicationIndeterminate(_))
            ));
        } else {
            assert!(observed.is_none());
        }
        assert!(reader.read_next().unwrap().is_none());
        assert!(matches!(
            handle.try_submit(
                PreparedFeedTransaction::new(vec![FeedRecord::Projection(expected)]).unwrap()
            ),
            Err(RejectedTransaction {
                reason: AdmissionFailure::Failed(_),
                ..
            })
        ));
        assert!(actor.shutdown().is_err());
    }
}
