//! Independent public-API checks for causal routing across actor submissions.
//! Opaque fixture bytes do not establish bootstrap hashes or engine semantics.
use std::{fs, time::Duration};

use marketfeed_recording::{
    RecordingError,
    shared_batch::{
        AdmissionFailure, BatchConfig, BatchFailureCause, CommitDisposition, FeedBatchActor,
        PreparedFeedTransaction,
    },
    shared_feed::{
        CausalEnvelope, CausalPhase, CausalRoute, FeedProfile, FeedReader, FeedRecord, FeedWriter,
    },
};

const PROFILE: FeedProfile = FeedProfile::EngineCausalV1;

fn transaction(session: u64, ordinal: u64, payload: &[u8]) -> PreparedFeedTransaction {
    PreparedFeedTransaction::new(vec![FeedRecord::Causal(
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
                generation: u64::from(ordinal != 0),
                bootstrap_id: "a".repeat(64),
            },
            payload,
        )
        .unwrap(),
    )])
    .unwrap()
}

fn config() -> BatchConfig {
    BatchConfig {
        max_pending_operations: 4,
        max_pending_bytes: 1_000_000,
        max_group_operations: 2,
        max_group_bytes: 1_000_000,
        max_age: Duration::from_secs(3600),
    }
}

#[test]
fn separate_prepared_bootstrap_and_operation_share_one_validated_durable_group() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let writer = FeedWriter::create_profile(&path, 1_000_000, PROFILE).unwrap();
    let actor = FeedBatchActor::spawn(writer, config()).unwrap();
    let handle = actor.handle();
    let bootstrap_bytes: &[u8] = b"opaque-bootstrap\0\xff";
    let operation_bytes: &[u8] = b"opaque-operation\0\xfe";

    let bootstrap = handle
        .try_submit(transaction(7, 0, bootstrap_bytes))
        .unwrap();
    let operation = handle
        .try_submit(transaction(7, 1, operation_bytes))
        .unwrap();
    handle.close();

    let bootstrap = bootstrap.wait().unwrap();
    let operation = operation.wait().unwrap();
    let report = actor.shutdown().unwrap();
    assert_eq!(report.groups, 1);
    assert_eq!(report.committed_operations, 2);
    assert_eq!((bootstrap.first_sequence, bootstrap.last_sequence), (1, 1));
    assert_eq!((operation.first_sequence, operation.last_sequence), (2, 2));
    assert_eq!(bootstrap.published, operation.published);
    assert_eq!(report.published, operation.published);
    assert_eq!(handle.status().pending_operations, 0);
    assert_eq!(handle.status().pending_bytes, 0);

    let mut reader = FeedReader::open_profile(&path, None, PROFILE).unwrap();
    for (ordinal, expected) in [(0, bootstrap_bytes), (1, operation_bytes)] {
        let Some(FeedRecord::Causal(envelope)) = reader.read_next().unwrap() else {
            panic!("expected causal record");
        };
        assert_eq!(envelope.route.session, 7);
        assert_eq!(envelope.route.ordinal, ordinal);
        assert_eq!(envelope.payload().unwrap(), expected);
    }
    assert!(reader.read_next().unwrap().is_none());
    assert_eq!(reader.cursor(), &report.published);
}

#[test]
fn invalid_later_submission_rejects_whole_group_without_publishing_bootstrap() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let writer = FeedWriter::create_profile(&path, 1_000_000, PROFILE).unwrap();
    let initial = writer.cursor().clone();
    let initial_data = fs::read(path.join("events.mfsf")).unwrap();
    let initial_watermark = fs::read(path.join("committed.json")).unwrap();
    let actor = FeedBatchActor::spawn(writer, config()).unwrap();
    let handle = actor.handle();

    let bootstrap = handle.try_submit(transaction(7, 0, b"bootstrap")).unwrap();
    // Intrinsically valid envelope, but session 8 has no preceding bootstrap.
    let invalid = handle.try_submit(transaction(8, 1, b"operation")).unwrap();
    handle.close();

    let bootstrap = bootstrap.wait().unwrap_err();
    let invalid = invalid.wait().unwrap_err();
    assert_eq!(bootstrap.failure, invalid.failure);
    assert!(matches!(
        &bootstrap.failure.cause,
        BatchFailureCause::Recording(RecordingError::Io(detail))
            if detail == "causal operation before bootstrap"
    ));
    assert_eq!(bootstrap.failure.last_acknowledged, initial);
    assert_eq!(
        bootstrap.disposition,
        CommitDisposition::Attempted {
            first_sequence: 1,
            last_sequence: 1,
        }
    );
    assert_eq!(
        invalid.disposition,
        CommitDisposition::Attempted {
            first_sequence: 2,
            last_sequence: 2,
        }
    );
    assert_eq!(actor.shutdown().unwrap_err(), bootstrap.failure);
    let status = handle.status();
    assert_eq!(status.admitted_operations, 2);
    assert_eq!(status.committed_operations, 0);
    assert_eq!((status.pending_operations, status.pending_bytes), (0, 0));
    assert!(status.closed);
    assert_eq!(status.last_acknowledged, initial);
    assert!(matches!(
        handle
            .try_submit(transaction(7, 0, b"not retried"))
            .unwrap_err()
            .reason,
        AdmissionFailure::Failed(_)
    ));
    assert_eq!(fs::read(path.join("events.mfsf")).unwrap(), initial_data);
    assert_eq!(
        fs::read(path.join("committed.json")).unwrap(),
        initial_watermark
    );
    let mut reader = FeedReader::open_profile(&path, None, PROFILE).unwrap();
    assert!(reader.read_next().unwrap().is_none());
    assert_eq!(reader.cursor(), &initial);
}
