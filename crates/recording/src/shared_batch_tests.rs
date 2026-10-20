use super::*;
use crate::shared_feed::{FeedReader, PublicationFailpoint};
use marketfeed_model::SessionId;
use std::sync::Barrier;

const WAIT: Duration = Duration::from_secs(10);
fn record(text: &str) -> FeedRecord {
    FeedRecord::Lifecycle {
        session: SessionId(7),
        now_ns: 1,
        state: text.into(),
    }
}
fn tx(text: &str) -> PreparedFeedTransaction {
    PreparedFeedTransaction::new(vec![record(text)]).unwrap()
}
fn seal() -> PreparedFeedTransaction {
    PreparedFeedTransaction::new(vec![FeedRecord::Seal {
        schema_version: 1,
        now_ns: 2,
        reason: "complete".into(),
    }])
    .unwrap()
}

#[test]
fn preparation_rejects_mixed_causal_versions_and_admits_owner_profile() {
    use crate::shared_feed::{CausalEnvelope, CausalPhase, CausalRoute};
    let envelope = |version| {
        FeedRecord::Causal(
            CausalEnvelope::for_profile(
                if version == 2 {
                    FeedProfile::EngineCausalV2
                } else {
                    FeedProfile::EngineCausalV1
                },
                CausalPhase::Bootstrap,
                CausalRoute {
                    venue: 5,
                    session: 1,
                    ordinal: 0,
                    generation: 0,
                    bootstrap_id: "a".repeat(64),
                },
                b"opaque bootstrap",
            )
            .unwrap(),
        )
    };
    assert!(PreparedFeedTransaction::new(vec![envelope(1), envelope(2)]).is_err());
    assert!(PreparedFeedTransaction::new(vec![envelope(2), envelope(1)]).is_err());
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("owner");
    let actor = FeedBatchActor::spawn(
        FeedWriter::create_profile(&path, 1_000_000, FeedProfile::EngineCausalV2).unwrap(),
        config(),
    )
    .unwrap();
    let ticket = actor
        .handle()
        .try_submit(PreparedFeedTransaction::new(vec![envelope(2)]).unwrap())
        .unwrap();
    actor.handle().close();
    assert_eq!(ticket.wait().unwrap().last_sequence, 1);
    actor.shutdown().unwrap();
    let mut reader = FeedReader::open_profile(&path, None, FeedProfile::EngineCausalV2).unwrap();
    let Some(FeedRecord::Causal(actual)) = reader.read_next().unwrap() else {
        panic!()
    };
    let FeedRecord::Causal(expected) = envelope(2) else {
        panic!()
    };
    assert_eq!(actual, expected);
}
fn config() -> BatchConfig {
    BatchConfig {
        max_pending_operations: 8,
        max_pending_bytes: 10_000,
        max_group_operations: 1,
        max_group_bytes: 10_000,
        max_age: Duration::from_secs(30),
    }
}
#[derive(Debug)]
enum Reply {
    Ok,
    Error(RecordingError),
    Panic,
    AfterPublishPanic,
    BadCursor,
}
struct GateTarget {
    writer: FeedWriter,
    started: mpsc::Sender<usize>,
    replies: mpsc::Receiver<Reply>,
}
impl PublicationTarget for GateTarget {
    fn cursor(&self) -> &FeedCursor {
        self.writer.cursor()
    }
    fn timings(&self) -> FeedIoTimings {
        self.writer.timings()
    }
    fn publish(&mut self, records: Vec<FeedRecord>) -> Result<FeedCursor, RecordingError> {
        self.started.send(records.len()).unwrap();
        match self
            .replies
            .recv_timeout(WAIT)
            .expect("test must release publication")
        {
            Reply::Ok => self.writer.append_many(records),
            Reply::Error(e) => Err(e),
            Reply::Panic => panic!("injected worker publication panic"),
            Reply::AfterPublishPanic => {
                self.writer.append_many(records)?;
                panic!("injected panic after successful publication");
            }
            Reply::BadCursor => {
                let mut c = self.writer.append_many(records)?;
                c.feed_id = "wrong identity".into();
                Ok(c)
            }
        }
    }
}
fn gated(
    temp: &tempfile::TempDir,
    cfg: BatchConfig,
) -> (FeedBatchActor, mpsc::Receiver<usize>, mpsc::Sender<Reply>) {
    let (started, calls) = mpsc::channel();
    let (replies, receive) = mpsc::channel();
    let target = GateTarget {
        writer: FeedWriter::create(temp.path().join("feed"), 1_000_000).unwrap(),
        started,
        replies: receive,
    };
    (
        FeedBatchActor::spawn_target(target, cfg).unwrap(),
        calls,
        replies,
    )
}
fn received(ticket: CommitTicket) -> Result<CommitReceipt, CommitFailure> {
    ticket
        .receiver
        .recv_timeout(WAIT)
        .expect("every ticket settles within test watchdog")
}

fn polled(ticket: &mut CommitTicket) -> Result<CommitReceipt, CommitFailure> {
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(result) = ticket.try_result() {
            return result;
        }
        assert!(Instant::now() < deadline, "ticket poll watchdog");
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn nonblocking_receipt_is_empty_before_barrier_and_stable_after_success() {
    let temp = tempfile::tempdir().unwrap();
    let (actor, calls, replies) = gated(&temp, config());
    let mut ticket = actor.handle().try_submit(tx("polled")).unwrap();
    calls.recv_timeout(WAIT).unwrap();
    assert!(ticket.try_result().is_none());
    replies.send(Reply::Ok).unwrap();
    let result = polled(&mut ticket).unwrap();
    assert_eq!(ticket.try_result(), Some(Ok(result.clone())));
    assert_eq!(ticket.try_result(), Some(Ok(result.clone())));
    assert_eq!(ticket.wait(), Ok(result));
    actor.shutdown().unwrap();
}

#[test]
fn nonblocking_receipts_preserve_attempted_panic_and_unattempted_suffix() {
    let temp = tempfile::tempdir().unwrap();
    let (actor, calls, replies) = gated(&temp, config());
    let mut active = actor.handle().try_submit(tx("active")).unwrap();
    calls.recv_timeout(WAIT).unwrap();
    let mut queued = actor.handle().try_submit(tx("queued")).unwrap();
    replies.send(Reply::Panic).unwrap();
    let first = polled(&mut active).unwrap_err();
    let second = polled(&mut queued).unwrap_err();
    assert!(matches!(
        first.disposition,
        CommitDisposition::Attempted { .. }
    ));
    assert_eq!(second.disposition, CommitDisposition::NotAttempted);
    assert_eq!(active.try_result(), Some(Err(first.clone())));
    assert_eq!(queued.wait(), Err(second));
    assert_eq!(active.wait(), Err(first));
    assert!(actor.shutdown().is_err());
}

#[test]
fn disconnected_receipt_poll_is_unknown_and_does_not_cancel_publication() {
    let temp = tempfile::tempdir().unwrap();
    let (actor, calls, replies) = gated(&temp, config());
    let mut ticket = actor.handle().try_submit(tx("accepted")).unwrap();
    calls.recv_timeout(WAIT).unwrap();
    let (sender, receiver) = mpsc::channel();
    drop(sender);
    ticket.receiver = receiver;
    let failure = ticket.try_result().unwrap().unwrap_err();
    assert_eq!(failure.disposition, CommitDisposition::Unknown);
    assert_eq!(failure.failure.cause, BatchFailureCause::ReceiptUnavailable);
    assert_eq!(ticket.wait(), Err(failure));
    replies.send(Reply::Ok).unwrap();
    let report = actor.shutdown().unwrap();
    assert_eq!(report.committed_operations, 1);
    assert_eq!(report.published.sequence, 1);
}

#[test]
fn preparation_validates_count_seal_and_full_envelope_sizing() {
    assert!(PreparedFeedTransaction::new(vec![]).is_err());
    assert!(
        PreparedFeedTransaction::new((0..MAX_RECORDS + 1).map(|_| record("x")).collect()).is_err()
    );
    assert!(
        PreparedFeedTransaction::new(vec![FeedRecord::Seal {
            schema_version: 2,
            now_ns: 1,
            reason: "bad".into()
        }])
        .is_err()
    );
    assert!(PreparedFeedTransaction::new(vec![seal().records.remove(0), record("after")]).is_err());
    let prepared = tx("\"\n\u{0000}");
    let expected = format!(
        "{{\"sequence\":18446744073709551615,\"record\":{}}}",
        serde_json::to_string(&prepared.records[0]).unwrap()
    );
    assert_eq!(prepared.encoded_bytes_bound(), expected.len() + 8);
    assert_eq!(prepared.record_count(), 1);
    let mut counter = CappedCounter { bytes: 0, limit: 3 };
    counter.write_all(b"abc").unwrap();
    assert!(counter.write_all(b"d").is_err());
}

#[test]
fn prepared_record_and_transaction_boundaries_include_framing() {
    // These are exact boundary cases, including the worst-case sequence digits.
    let overhead = tx("").bytes - 8;
    let exact =
        PreparedFeedTransaction::new(vec![record(&"x".repeat(MAX_RECORD - overhead))]).unwrap();
    assert_eq!(exact.bytes, MAX_RECORD + 8);
    assert!(
        PreparedFeedTransaction::new(vec![record(&"x".repeat(MAX_RECORD - overhead + 1))]).is_err()
    );
    // Two maximum records exceed the transaction limit by their 16 framing bytes.
    let a = record(&"x".repeat(MAX_RECORD - overhead));
    let b = record(&"x".repeat(MAX_RECORD - overhead - 16));
    assert_eq!(
        PreparedFeedTransaction::new(vec![a, b]).unwrap().bytes,
        MAX_TRANSACTION
    );
    let a = record(&"x".repeat(MAX_RECORD - overhead));
    let b = record(&"x".repeat(MAX_RECORD - overhead - 15));
    assert!(PreparedFeedTransaction::new(vec![a, b]).is_err());
}

#[test]
fn invalid_bounds_refuse_startup() {
    let base = config();
    for cfg in [
        BatchConfig {
            max_group_operations: 0,
            ..base
        },
        BatchConfig {
            max_pending_operations: 0,
            ..base
        },
        BatchConfig {
            max_group_operations: 9,
            ..base
        },
        BatchConfig {
            max_group_bytes: 0,
            ..base
        },
        BatchConfig {
            max_group_bytes: MAX_TRANSACTION + 1,
            max_pending_bytes: MAX_TRANSACTION + 1,
            ..base
        },
        BatchConfig {
            max_pending_bytes: 1,
            ..base
        },
        BatchConfig {
            max_age: Duration::MAX,
            ..base
        },
    ] {
        assert!(cfg.validate().is_err());
    }
}

#[test]
fn inflight_count_and_byte_reservations_survive_queue_removal() {
    for by_bytes in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let bytes = tx("one").bytes;
        let cfg = BatchConfig {
            max_pending_operations: if by_bytes { 8 } else { 2 },
            max_pending_bytes: if by_bytes { 2 * bytes } else { 10_000 },
            max_group_bytes: bytes,
            ..config()
        };
        let (actor, calls, replies) = gated(&temp, cfg);
        let handle = actor.handle();
        let one = handle.try_submit(tx("one")).unwrap();
        assert_eq!(calls.recv_timeout(WAIT).unwrap(), 1);
        assert!(matches!(
            one.receiver.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        let two = handle.try_submit(tx("two")).unwrap();
        assert_eq!(handle.status().pending_operations, 2);
        assert_eq!(handle.status().pending_bytes, 2 * bytes);
        assert_eq!(
            handle.try_submit(tx("six")).unwrap_err().reason,
            AdmissionFailure::Full
        );
        replies.send(Reply::Ok).unwrap();
        assert_eq!(received(one).unwrap().first_sequence, 1);
        calls.recv_timeout(WAIT).unwrap();
        replies.send(Reply::Ok).unwrap();
        received(two).unwrap();
        let report = actor.shutdown().unwrap();
        assert_eq!(report.committed_operations, 2);
        assert_eq!(handle.status().pending_bytes, 0);
    }
}

#[test]
fn count_group_acknowledges_exact_ranges_after_one_publication() {
    let temp = tempfile::tempdir().unwrap();
    let (actor, calls, replies) = gated(
        &temp,
        BatchConfig {
            max_group_operations: 3,
            ..config()
        },
    );
    let h = actor.handle();
    let a = h.try_submit(tx("a")).unwrap();
    let b = h
        .try_submit(PreparedFeedTransaction::new(vec![record("b1"), record("b2")]).unwrap())
        .unwrap();
    let c = h.try_submit(tx("c")).unwrap();
    assert_eq!(calls.recv_timeout(WAIT).unwrap(), 4);
    assert!(matches!(
        a.receiver.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    let mut reader = FeedReader::open(temp.path().join("feed"), None).unwrap();
    assert!(reader.read_next().unwrap().is_none());
    replies.send(Reply::Ok).unwrap();
    let receipts = [
        received(a).unwrap(),
        received(b).unwrap(),
        received(c).unwrap(),
    ];
    assert_eq!(
        receipts
            .iter()
            .map(|r| (r.admission, r.first_sequence, r.last_sequence))
            .collect::<Vec<_>>(),
        vec![(1, 1, 1), (2, 2, 3), (3, 4, 4)]
    );
    assert!(
        receipts
            .iter()
            .all(|r| r.published == receipts[0].published)
    );
    for text in ["a", "b1", "b2", "c"] {
        assert!(
            matches!(reader.read_next().unwrap(),Some(FeedRecord::Lifecycle{state,..}) if state==text)
        );
    }
    assert!(reader.read_next().unwrap().is_none());
    let report = actor.shutdown().unwrap();
    assert_eq!(report.groups, 1);
    assert_eq!(report.timings.accepted_transactions, 1);
}

#[test]
fn byte_limit_flushes_prefix_without_waiting_for_count_or_age() {
    let temp = tempfile::tempdir().unwrap();
    let bound = tx("a").bytes;
    let (actor, calls, replies) = gated(
        &temp,
        BatchConfig {
            max_group_operations: 4,
            max_group_bytes: 2 * bound,
            ..config()
        },
    );
    let h = actor.handle();
    let a = h.try_submit(tx("a")).unwrap();
    let b = h.try_submit(tx("b")).unwrap();
    assert_eq!(calls.recv_timeout(WAIT).unwrap(), 2);
    let c = h.try_submit(tx("c")).unwrap();
    replies.send(Reply::Ok).unwrap();
    received(a).unwrap();
    received(b).unwrap();
    h.close();
    assert_eq!(calls.recv_timeout(WAIT).unwrap(), 1);
    replies.send(Reply::Ok).unwrap();
    received(c).unwrap();
    assert_eq!(actor.shutdown().unwrap().groups, 2);
}

#[test]
fn oversized_transaction_rejection_preserves_open_admission() {
    let temp = tempfile::tempdir().unwrap();
    let bytes = tx("a").bytes;
    let (actor, calls, replies) = gated(
        &temp,
        BatchConfig {
            max_group_bytes: bytes,
            ..config()
        },
    );
    let h = actor.handle();
    let rejected = h.try_submit(tx("larger")).unwrap_err();
    assert_eq!(rejected.reason, AdmissionFailure::TransactionTooLarge);
    assert_eq!(rejected.transaction.record_count(), 1);
    assert!(!h.status().closed);
    assert_eq!(h.status().admitted_operations, 0);
    let ticket = h.try_submit(tx("a")).unwrap();
    calls.recv_timeout(WAIT).unwrap();
    replies.send(Reply::Ok).unwrap();
    received(ticket).unwrap();
    actor.shutdown().unwrap();
}

struct SequenceTarget {
    cursor: FeedCursor,
}
impl PublicationTarget for SequenceTarget {
    fn cursor(&self) -> &FeedCursor {
        &self.cursor
    }
    fn timings(&self) -> FeedIoTimings {
        FeedIoTimings::default()
    }
    fn publish(&mut self, records: Vec<FeedRecord>) -> Result<FeedCursor, RecordingError> {
        assert!(records.len() <= MAX_RECORDS);
        self.cursor.sequence = self
            .cursor
            .sequence
            .checked_add(records.len() as u64)
            .unwrap();
        self.cursor.offset += 1;
        Ok(self.cursor.clone())
    }
}
#[test]
fn receipt_ranges_allow_final_u64_sequence_then_refuse_overflow_without_publication() {
    let actor = FeedBatchActor::spawn_target(
        SequenceTarget {
            cursor: FeedCursor {
                feed_id: "test-identity".into(),
                sequence: u64::MAX - 1,
                offset: 48,
            },
        },
        config(),
    )
    .unwrap();
    let h = actor.handle();
    let receipt = received(h.try_submit(tx("last possible sequence")).unwrap()).unwrap();
    assert_eq!(receipt.first_sequence, u64::MAX);
    assert_eq!(receipt.last_sequence, u64::MAX);
    let overflow = received(h.try_submit(tx("overflow")).unwrap()).unwrap_err();
    assert_eq!(overflow.disposition, CommitDisposition::NotAttempted);
    assert_eq!(overflow.failure.last_acknowledged, receipt.published);
    assert_eq!(h.status().committed_operations, 1);
    assert_eq!(actor.shutdown().unwrap_err(), overflow.failure);
}
#[test]
fn combined_record_limit_splits_complete_operations() {
    let actor = FeedBatchActor::spawn_target(
        SequenceTarget {
            cursor: FeedCursor {
                feed_id: "test-identity".into(),
                sequence: 100,
                offset: 48,
            },
        },
        BatchConfig {
            max_group_operations: 2,
            max_group_bytes: MAX_TRANSACTION,
            max_pending_bytes: MAX_TRANSACTION * 2,
            ..config()
        },
    )
    .unwrap();
    let h = actor.handle();
    let transaction = || {
        PreparedFeedTransaction::new((0..MAX_RECORDS / 2 + 1).map(|_| record("x")).collect())
            .unwrap()
    };
    let a = h.try_submit(transaction()).unwrap();
    let b = h.try_submit(transaction()).unwrap();
    h.close();
    let a = received(a).unwrap();
    let b = received(b).unwrap();
    assert_eq!((a.first_sequence, a.last_sequence), (101, 32_869));
    assert_eq!((b.first_sequence, b.last_sequence), (32_870, 65_638));
    assert_ne!(a.published, b.published);
    assert_eq!(actor.shutdown().unwrap().groups, 2);
}

#[test]
fn lone_maximum_record_count_flushes_without_another_submission() {
    let actor = FeedBatchActor::spawn_target(
        SequenceTarget {
            cursor: FeedCursor {
                feed_id: "test-identity".into(),
                sequence: 10,
                offset: 48,
            },
        },
        BatchConfig {
            max_group_operations: 2,
            max_group_bytes: MAX_TRANSACTION,
            max_pending_bytes: MAX_TRANSACTION * 2,
            ..config()
        },
    )
    .unwrap();
    let transaction =
        PreparedFeedTransaction::new((0..MAX_RECORDS).map(|_| record("x")).collect()).unwrap();
    let receipt = received(actor.handle().try_submit(transaction).unwrap()).unwrap();
    assert_eq!(
        (receipt.first_sequence, receipt.last_sequence),
        (11, 65_546)
    );
    assert_eq!(actor.shutdown().unwrap().groups, 1);
}

struct CursorPanicTarget {
    target: GateTarget,
    reads: std::sync::atomic::AtomicUsize,
}
impl PublicationTarget for CursorPanicTarget {
    fn cursor(&self) -> &FeedCursor {
        if self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 2 {
            panic!("injected cursor panic before second publication");
        }
        self.target.cursor()
    }
    fn timings(&self) -> FeedIoTimings {
        self.target.timings()
    }
    fn publish(&mut self, records: Vec<FeedRecord>) -> Result<FeedCursor, RecordingError> {
        self.target.publish(records)
    }
}
#[test]
fn panic_before_second_publication_settles_new_work_and_preserves_first_ack() {
    let temp = tempfile::tempdir().unwrap();
    let (started, calls) = mpsc::channel();
    let (replies, receive) = mpsc::channel();
    let target = CursorPanicTarget {
        target: GateTarget {
            writer: FeedWriter::create(temp.path().join("feed"), 1_000_000).unwrap(),
            started,
            replies: receive,
        },
        reads: std::sync::atomic::AtomicUsize::new(0),
    };
    let actor = FeedBatchActor::spawn_target(target, config()).unwrap();
    let h = actor.handle();
    let first = h.try_submit(tx("committed")).unwrap();
    calls.recv_timeout(WAIT).unwrap();
    replies.send(Reply::Ok).unwrap();
    let receipt = received(first).unwrap();
    let second = received(h.try_submit(tx("never published")).unwrap()).unwrap_err();
    assert_eq!(second.failure.cause, BatchFailureCause::WorkerPanicked);
    assert_eq!(second.disposition, CommitDisposition::NotAttempted);
    assert_eq!(second.failure.last_acknowledged, receipt.published);
    assert_eq!(actor.shutdown().unwrap_err(), second.failure);
}

#[test]
fn idle_partial_group_flushes_on_age() {
    let temp = tempfile::tempdir().unwrap();
    let (actor, calls, replies) = gated(
        &temp,
        BatchConfig {
            max_group_operations: 4,
            max_age: Duration::from_millis(10),
            ..config()
        },
    );
    let ticket = actor
        .handle()
        .try_submit(tx("only active session"))
        .unwrap();
    assert_eq!(calls.recv_timeout(WAIT).unwrap(), 1);
    replies.send(Reply::Ok).unwrap();
    received(ticket).unwrap();
    actor.shutdown().unwrap();
}

#[test]
fn queued_age_is_preserved_across_prior_io_and_new_arrivals() {
    let temp = tempfile::tempdir().unwrap();
    let (actor, calls, replies) = gated(
        &temp,
        BatchConfig {
            max_group_operations: 4,
            ..config()
        },
    );
    let h = actor.handle();
    // Force the first group by count, then age the first queued admission while
    // publication is stalled. A newer arrival must not reset its deadline.
    let initial: Vec<_> = (0..4).map(|_| h.try_submit(tx("first")).unwrap()).collect();
    calls.recv_timeout(WAIT).unwrap();
    let old = h.try_submit(tx("old")).unwrap();
    {
        let mut state = lock(&h.inner);
        state.queue.front_mut().unwrap().admitted_at = Instant::now() - Duration::from_secs(60);
    }
    let newer = h.try_submit(tx("new")).unwrap();
    replies.send(Reply::Ok).unwrap();
    for t in initial {
        received(t).unwrap();
    }
    assert_eq!(calls.recv_timeout(WAIT).unwrap(), 2);
    replies.send(Reply::Ok).unwrap();
    received(old).unwrap();
    received(newer).unwrap();
    actor.shutdown().unwrap();
}

#[test]
fn dropped_ticket_does_not_cancel_and_owner_drop_joins() {
    let temp = tempfile::tempdir().unwrap();
    let (actor, calls, replies) = gated(&temp, config());
    let h = actor.handle();
    drop(h.try_submit(tx("retained despite dropped waiter")).unwrap());
    calls.recv_timeout(WAIT).unwrap();
    replies.send(Reply::Ok).unwrap();
    drop(actor);
    assert_eq!(h.status().committed_operations, 1);
    assert!(h.status().closed);
    let mut reader = FeedReader::open(temp.path().join("feed"), None).unwrap();
    assert!(reader.read_next().unwrap().is_some());
    assert!(reader.read_next().unwrap().is_none());
}

#[test]
fn seal_closes_admission_and_remains_final() {
    let temp = tempfile::tempdir().unwrap();
    let writer = FeedWriter::create(temp.path().join("feed"), 1_000_000).unwrap();
    let actor = FeedBatchActor::spawn(
        writer,
        BatchConfig {
            max_group_operations: 4,
            ..config()
        },
    )
    .unwrap();
    let h = actor.handle();
    let a = h.try_submit(tx("before seal")).unwrap();
    let s = h.try_submit(seal()).unwrap();
    assert_eq!(
        h.try_submit(tx("after seal")).unwrap_err().reason,
        AdmissionFailure::Closed
    );
    received(a).unwrap();
    received(s).unwrap();
    actor.shutdown().unwrap();
    let mut reader = FeedReader::open(temp.path().join("feed"), None).unwrap();
    assert!(reader.read_next().unwrap().is_some());
    assert!(matches!(
        reader.read_next().unwrap(),
        Some(FeedRecord::Seal { .. })
    ));
    assert!(reader.read_next().unwrap().is_none());
}

#[test]
fn rejected_seal_does_not_close_or_cancel_accepted_work() {
    let temp = tempfile::tempdir().unwrap();
    let (actor, calls, replies) = gated(
        &temp,
        BatchConfig {
            max_pending_operations: 1,
            ..config()
        },
    );
    let h = actor.handle();
    let first = h.try_submit(tx("a")).unwrap();
    calls.recv_timeout(WAIT).unwrap();
    assert_eq!(
        h.try_submit(seal()).unwrap_err().reason,
        AdmissionFailure::Full
    );
    assert!(!h.status().closed);
    replies.send(Reply::Ok).unwrap();
    received(first).unwrap();
    let final_ticket = h.try_submit(seal()).unwrap();
    calls.recv_timeout(WAIT).unwrap();
    replies.send(Reply::Ok).unwrap();
    received(final_ticket).unwrap();
    actor.shutdown().unwrap();
}

#[test]
fn failed_or_panicked_active_group_differs_from_never_attempted_suffix() {
    for reply in [
        Reply::Error(error("before watermark")),
        Reply::Error(RecordingError::PublicationIndeterminate(
            "directory barrier".into(),
        )),
        Reply::Panic,
        Reply::AfterPublishPanic,
        Reply::BadCursor,
    ] {
        let visible = matches!(&reply, Reply::AfterPublishPanic | Reply::BadCursor);
        let expected = match &reply {
            Reply::Error(e) => BatchFailureCause::Recording(e.clone()),
            Reply::Panic | Reply::AfterPublishPanic => BatchFailureCause::WorkerPanicked,
            Reply::BadCursor => BatchFailureCause::InvalidPublicationReceipt,
            Reply::Ok => unreachable!(),
        };
        let temp = tempfile::tempdir().unwrap();
        let (actor, calls, replies) = gated(&temp, config());
        let h = actor.handle();
        let first = h.try_submit(tx("active")).unwrap();
        calls.recv_timeout(WAIT).unwrap();
        let suffix = h.try_submit(tx("queued")).unwrap();
        replies.send(reply).unwrap();
        let a = received(first).unwrap_err();
        let b = received(suffix).unwrap_err();
        assert_eq!(
            a.disposition,
            CommitDisposition::Attempted {
                first_sequence: 1,
                last_sequence: 1
            }
        );
        assert_eq!(b.disposition, CommitDisposition::NotAttempted);
        assert_eq!(a.failure, b.failure);
        assert_eq!(a.failure.cause, expected);
        assert_eq!(a.failure.last_acknowledged.sequence, 0);
        assert_eq!(h.status().pending_operations, 0);
        assert_eq!(h.status().pending_bytes, 0);
        assert!(matches!(
            h.try_submit(tx("late")).unwrap_err().reason,
            AdmissionFailure::Failed(_)
        ));
        assert_eq!(actor.shutdown().unwrap_err(), a.failure);
        let mut reader = FeedReader::open(temp.path().join("feed"), None).unwrap();
        if visible {
            assert!(
                matches!(reader.read_next().unwrap(),Some(FeedRecord::Lifecycle{state,..}) if state=="active")
            );
        }
        assert!(reader.read_next().unwrap().is_none());
    }
}

#[test]
fn actual_writer_failure_stages_never_acknowledge_a_partial_group() {
    for stage in [
        PublicationFailpoint::Write,
        PublicationFailpoint::PartialWrite,
        PublicationFailpoint::DataSync,
        PublicationFailpoint::WatermarkWrite,
        PublicationFailpoint::WatermarkSync,
        PublicationFailpoint::Replace,
        PublicationFailpoint::DirectorySync,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let directory = temp.path().join("feed");
        let mut writer = FeedWriter::create(&directory, 1_000_000).unwrap();
        let acknowledged = writer.append(record("acknowledged")).unwrap();
        writer.failpoint = Some(stage);
        let actor = FeedBatchActor::spawn(
            writer,
            BatchConfig {
                max_group_operations: 2,
                ..config()
            },
        )
        .unwrap();
        let h = actor.handle();
        let a = h.try_submit(tx("first")).unwrap();
        let b = h.try_submit(tx("second")).unwrap();
        let a = received(a).unwrap_err();
        let b = received(b).unwrap_err();
        assert_eq!(a.failure, b.failure);
        assert_eq!(a.failure.last_acknowledged, acknowledged);
        assert_eq!(
            a.disposition,
            CommitDisposition::Attempted {
                first_sequence: 2,
                last_sequence: 2
            }
        );
        assert_eq!(
            b.disposition,
            CommitDisposition::Attempted {
                first_sequence: 3,
                last_sequence: 3
            }
        );
        assert_eq!(
            matches!(
                a.failure.cause,
                BatchFailureCause::Recording(RecordingError::PublicationIndeterminate(_))
            ),
            stage == PublicationFailpoint::DirectorySync
        );
        assert_eq!(actor.shutdown().unwrap_err(), a.failure);
        let mut reader = FeedReader::open(&directory, Some(acknowledged)).unwrap();
        if stage == PublicationFailpoint::DirectorySync {
            assert!(
                matches!(reader.read_next().unwrap(),Some(FeedRecord::Lifecycle{state,..}) if state=="first")
            );
            assert!(
                matches!(reader.read_next().unwrap(),Some(FeedRecord::Lifecycle{state,..}) if state=="second")
            );
        }
        assert!(reader.read_next().unwrap().is_none());
    }
}

#[test]
fn concurrent_close_seal_and_submissions_have_one_linearized_order() {
    let temp = tempfile::tempdir().unwrap();
    let actor = FeedBatchActor::spawn(
        FeedWriter::create(temp.path().join("feed"), 1_000_000).unwrap(),
        config(),
    )
    .unwrap();
    let h = actor.handle();
    let barrier = Arc::new(Barrier::new(5));
    let mut workers = Vec::new();
    for index in 0..4 {
        let (h, barrier) = (h.clone(), barrier.clone());
        workers.push(thread::spawn(move || {
            barrier.wait();
            if index == 3 {
                h.close();
                None
            } else {
                Some(h.try_submit(if index == 2 { seal() } else { tx("data") }))
            }
        }));
    }
    barrier.wait();
    let mut receipts = Vec::new();
    for worker in workers {
        if let Some(result) = worker.join().unwrap() {
            match result {
                Ok(ticket) => receipts.push(received(ticket).unwrap()),
                Err(rejected) => assert_eq!(rejected.reason, AdmissionFailure::Closed),
            }
        }
    }
    let report = actor.shutdown().unwrap();
    assert_eq!(report.committed_operations, receipts.len() as u64);
    receipts.sort_by_key(|r| r.admission);
    for (index, r) in receipts.iter().enumerate() {
        assert_eq!(r.first_sequence, index as u64 + 1);
    }
    let mut reader = FeedReader::open(temp.path().join("feed"), None).unwrap();
    let mut count = 0;
    while reader.read_next().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, receipts.len());
}
