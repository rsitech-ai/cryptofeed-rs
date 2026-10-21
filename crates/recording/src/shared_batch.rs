//! Bounded, single-owner durable grouping for complete shared-feed transactions.
//!
//! Admission is not a durability acknowledgment. An accepted transaction cannot
//! be cancelled by dropping its ticket. Pending limits include work being written,
//! and shutdown drains accepted work before joining the writer thread. The actor
//! owns the actual writer exclusively; it never waits for submissions while
//! holding a writer lock. The oldest admission sets the grouping deadline.
//!
//! This module does not make legacy runner callbacks atomic, gate engine state,
//! capture causal outcomes, or establish exactly-once external side effects. An
//! engine owner must supply complete operations and withhold release until ACK.
use crate::{
    RecordingError,
    shared_feed::{
        FeedCursor, FeedIoTimings, FeedProfile, FeedRecord, FeedWriter, MAX_RECORD, MAX_TRANSACTION,
    },
};
use serde::Serialize;
use std::{
    collections::VecDeque,
    io::{self, Write},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Condvar, Mutex, MutexGuard, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[cfg(test)]
#[path = "shared_batch_tests.rs"]
mod tests;

const MAX_RECORDS: usize = 65_536;
fn error(message: impl Into<String>) -> RecordingError {
    RecordingError::Io(message.into())
}

/// Explicit bounds. Byte limits cover serialized WireRecords including their
/// length/CRC framing, conservatively using 20-digit sequence numbers. Caller
/// preparation memory and the writer's bounded encoding buffer are additional.
#[derive(Debug, Clone, Copy)]
pub struct BatchConfig {
    pub max_pending_operations: usize,
    pub max_pending_bytes: usize,
    pub max_group_operations: usize,
    pub max_group_bytes: usize,
    /// Maximum grouping wait from oldest admission, excluding scheduling and I/O.
    pub max_age: Duration,
}
impl BatchConfig {
    /// Validate bounds before creating a destination or transferring its writer.
    pub fn validate(self) -> Result<(), RecordingError> {
        if self.max_pending_operations == 0
            || self.max_group_operations == 0
            || self.max_group_operations > self.max_pending_operations
            || self.max_group_operations > MAX_RECORDS
            || self.max_group_bytes == 0
            || self.max_group_bytes > self.max_pending_bytes
            || self.max_group_bytes > MAX_TRANSACTION
            || Instant::now().checked_add(self.max_age).is_none()
        {
            return Err(error("invalid shared batch bounds"));
        }
        Ok(())
    }
}

/// Immutable, prevalidated complete input supplied by its owner. No record can
/// be added after sizing/admission. Construct before mutating an engine where
/// possible; owner capture-budget failures after execution require explicit
/// failure settlement, never a retry pretending the operation did not execute.
#[derive(Debug)]
pub struct PreparedFeedTransaction {
    records: Vec<FeedRecord>,
    bytes: usize,
    sealing: bool,
    profile: Option<FeedProfile>,
}
impl PreparedFeedTransaction {
    pub fn new(records: Vec<FeedRecord>) -> Result<Self, RecordingError> {
        if records.is_empty() || records.len() > MAX_RECORDS {
            return Err(error("invalid prepared shared transaction count"));
        }
        #[derive(Serialize)]
        struct WireRef<'a> {
            sequence: u64,
            record: &'a FeedRecord,
        }
        let mut bytes = 0usize;
        let mut sealing = false;
        let mut profile = None;
        for (index, record) in records.iter().enumerate() {
            let record_profile = match record {
                FeedRecord::Projection(envelope) => {
                    envelope.validate()?;
                    Some(FeedProfile::ObservationProjectionV1)
                }
                FeedRecord::Causal(envelope) => {
                    envelope.validate()?;
                    Some(match envelope.schema_version {
                        1 => FeedProfile::EngineCausalV1,
                        2 => FeedProfile::EngineCausalV2,
                        3 => FeedProfile::EngineCausalV3,
                        4 => FeedProfile::EngineCausalV4,
                        5 => FeedProfile::EngineCausalV5,
                        _ => unreachable!("validated causal version"),
                    })
                }
                FeedRecord::Seal { .. } => None,
                _ => Some(FeedProfile::Legacy),
            };
            if let Some(record_profile) = record_profile {
                if profile.is_some_and(|p| p != record_profile) {
                    return Err(error("mixed prepared feed profiles"));
                }
                profile = Some(record_profile);
            }
            if record.seals_feed()? {
                if matches!(record, FeedRecord::Seal { .. })
                    && profile == Some(FeedProfile::ObservationProjectionV1)
                {
                    return Err(error("standalone seal in prepared projection feed"));
                }
                if index + 1 != records.len() {
                    return Err(error("unsupported or non-final prepared seal"));
                }
                sealing = true;
            }
            // Measure without allocating a second serialized payload. Stop the
            // serializer immediately at either per-record or transaction limit.
            let mut counter = CappedCounter {
                bytes: 0,
                limit: MAX_RECORD.min(MAX_TRANSACTION.saturating_sub(bytes + 8)),
            };
            serde_json::to_writer(
                &mut counter,
                &WireRef {
                    sequence: u64::MAX,
                    record,
                },
            )
            .map_err(|e| error(e.to_string()))?;
            bytes = bytes
                .checked_add(counter.bytes + 8)
                .filter(|n| *n <= MAX_TRANSACTION)
                .ok_or_else(|| error("prepared shared transaction byte budget"))?;
        }
        Ok(Self {
            records,
            bytes,
            sealing,
            profile,
        })
    }
    pub fn encoded_bytes_bound(&self) -> usize {
        self.bytes
    }
    pub fn record_count(&self) -> usize {
        self.records.len()
    }
    /// Immutable prepared content; retry callers cannot alter it after sizing.
    pub fn records(&self) -> &[FeedRecord] {
        &self.records
    }
}
struct CappedCounter {
    bytes: usize,
    limit: usize,
}
impl Write for CappedCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|n| *n <= self.limit)
            .ok_or_else(|| io::Error::other("prepared shared record byte budget"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionFailure {
    ProfileMismatch,
    Full,
    TransactionTooLarge,
    Closed,
    Failed(BatchFailure),
    OrdinalExhausted,
}
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BatchFailureCause {
    #[error("{0}")]
    Recording(RecordingError),
    /// The active group may or may not have published. Never retry it as if
    /// absence of acknowledgment proved absence of durable records.
    #[error("shared batch worker panicked; active publication unknown")]
    WorkerPanicked,
    #[error("shared batch target returned inconsistent cursor")]
    InvalidPublicationReceipt,
    #[error("shared batch receipt channel closed without settlement")]
    ReceiptUnavailable,
}
impl From<RecordingError> for BatchFailureCause {
    fn from(error: RecordingError) -> Self {
        Self::Recording(error)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("shared batch failed after durable sequence {last_acknowledged:?}: {cause}")]
pub struct BatchFailure {
    pub cause: BatchFailureCause,
    /// Last successfully barrier-acknowledged watermark, not a claim about an
    /// indeterminate/unknown active group that readers might subsequently see.
    pub last_acknowledged: FeedCursor,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitDisposition {
    NotAttempted,
    /// An invariant failure disconnected the receipt channel. This is never
    /// evidence that accepted work was cancelled or absent from the journal.
    Unknown,
    /// This operation was in the active publication call. The typed cause
    /// distinguishes ordinary failure, post-replace indeterminacy and panic.
    Attempted {
        first_sequence: u64,
        last_sequence: u64,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("shared batch ticket {disposition:?}: {failure}")]
pub struct CommitFailure {
    pub admission: u64,
    pub failure: BatchFailure,
    pub disposition: CommitDisposition,
}
/// Rejected transactions remain owned by the caller. Rejection says nothing
/// about whether the caller's engine operation has already executed.
#[derive(Debug)]
pub struct RejectedTransaction {
    pub reason: AdmissionFailure,
    pub transaction: PreparedFeedTransaction,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitReceipt {
    /// Actor-local admission order, not an exchange sequence or durable cursor.
    pub admission: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    /// Covering group watermark. All records in this receipt's range are durable.
    pub published: FeedCursor,
}
#[derive(Debug)]
pub struct CommitTicket {
    admission: u64,
    receiver: mpsc::Receiver<Result<CommitReceipt, CommitFailure>>,
    inner: Arc<Inner>,
    settled: Option<Result<CommitReceipt, CommitFailure>>,
}
impl CommitTicket {
    pub fn admission(&self) -> u64 {
        self.admission
    }
    /// Blocking wait; use a dedicated owner thread or blocking bridge in async
    /// callers. Dropping the waiter never removes accepted work.
    pub fn wait(mut self) -> Result<CommitReceipt, CommitFailure> {
        if let Some(result) = self.settled.take() {
            return result;
        }
        self.receiver.recv().unwrap_or_else(|_| self.disconnected())
    }
    /// Nonblocking observation for an owner that must revoke safety while I/O
    /// is outstanding. Empty is not failure. A terminal result is cached so
    /// observing it twice cannot turn success into a disconnected-channel error.
    pub fn try_result(&mut self) -> Option<Result<CommitReceipt, CommitFailure>> {
        if self.settled.is_none() {
            self.settled = match self.receiver.try_recv() {
                Ok(result) => Some(result),
                Err(mpsc::TryRecvError::Empty) => None,
                Err(mpsc::TryRecvError::Disconnected) => Some(self.disconnected()),
            };
        }
        self.settled.clone()
    }
    /// Wait for an actual settlement, bounded by the owner's next safety poll.
    /// A timeout is only an empty observation and never cancels accepted work.
    /// Like `try_result`, terminal success/failure/disconnection is cached.
    /// Use on a dedicated owner thread, without holding its state mutex.
    pub fn wait_result(
        &mut self,
        timeout: Duration,
    ) -> Option<Result<CommitReceipt, CommitFailure>> {
        if self.settled.is_none() {
            self.settled = match self.receiver.recv_timeout(timeout) {
                Ok(result) => Some(result),
                Err(mpsc::RecvTimeoutError::Timeout) => None,
                Err(mpsc::RecvTimeoutError::Disconnected) => Some(self.disconnected()),
            };
        }
        self.settled.clone()
    }
    fn disconnected(&self) -> Result<CommitReceipt, CommitFailure> {
        let status = self.inner.state.lock().unwrap_or_else(|p| p.into_inner());
        Err(CommitFailure {
            admission: self.admission,
            disposition: CommitDisposition::Unknown,
            failure: status
                .status
                .failure
                .clone()
                .unwrap_or_else(|| BatchFailure {
                    cause: BatchFailureCause::ReceiptUnavailable,
                    last_acknowledged: status.status.last_acknowledged.clone(),
                }),
        })
    }
}
#[derive(Debug, Clone)]
pub struct BatchStatus {
    pub admitted_operations: u64,
    pub committed_operations: u64,
    pub pending_operations: usize,
    pub pending_bytes: usize,
    pub closed: bool,
    pub failure: Option<BatchFailure>,
    pub last_acknowledged: FeedCursor,
}
#[derive(Debug)]
struct Pending {
    admission: u64,
    admitted_at: Instant,
    transaction: PreparedFeedTransaction,
    sender: mpsc::Sender<Result<CommitReceipt, CommitFailure>>,
}
#[derive(Debug)]
struct State {
    queue: VecDeque<Pending>,
    status: BatchStatus,
}
#[derive(Debug)]
struct Inner {
    config: BatchConfig,
    profile: FeedProfile,
    state: Mutex<State>,
    wake: Condvar,
}
fn lock(inner: &Inner) -> MutexGuard<'_, State> {
    // A worker panic is caught and all retained tickets are settled. Recover the
    // mutex only to perform that settlement, never to retry publication.
    inner
        .state
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}
#[derive(Debug, Clone)]
pub struct BatchFeedHandle {
    inner: Arc<Inner>,
}
impl BatchFeedHandle {
    /// Immutable writer format, available before constructing an engine owner.
    pub fn profile(&self) -> FeedProfile {
        self.inner.profile
    }
    /// Immutable admission/grouping caps for preflight owner compatibility.
    pub fn config(&self) -> BatchConfig {
        self.inner.config
    }
    pub fn status(&self) -> BatchStatus {
        lock(&self.inner).status.clone()
    }
    pub fn try_submit(
        &self,
        transaction: PreparedFeedTransaction,
    ) -> Result<CommitTicket, RejectedTransaction> {
        let mut state = lock(&self.inner);
        let cfg = self.inner.config;
        let reason = if let Some(failure) = &state.status.failure {
            Some(AdmissionFailure::Failed(failure.clone()))
        } else if state.status.closed {
            Some(AdmissionFailure::Closed)
        } else if transaction.profile.is_some_and(|p| p != self.inner.profile)
            || (self.inner.profile == FeedProfile::ObservationProjectionV1
                && transaction.profile != Some(FeedProfile::ObservationProjectionV1))
        {
            Some(AdmissionFailure::ProfileMismatch)
        } else if transaction.bytes > cfg.max_group_bytes {
            Some(AdmissionFailure::TransactionTooLarge)
        } else if state.status.pending_operations >= cfg.max_pending_operations
            || transaction.bytes > cfg.max_pending_bytes - state.status.pending_bytes
        {
            Some(AdmissionFailure::Full)
        } else if state.status.admitted_operations == u64::MAX {
            Some(AdmissionFailure::OrdinalExhausted)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(RejectedTransaction {
                reason,
                transaction,
            });
        }
        let admission = state.status.admitted_operations + 1;
        let (sender, receiver) = mpsc::channel();
        state.status.admitted_operations = admission;
        state.status.pending_operations += 1;
        state.status.pending_bytes += transaction.bytes;
        if transaction.sealing {
            state.status.closed = true;
        }
        state.queue.push_back(Pending {
            admission,
            admitted_at: Instant::now(),
            transaction,
            sender,
        });
        self.inner.wake.notify_one();
        Ok(CommitTicket {
            admission,
            receiver,
            inner: self.inner.clone(),
            settled: None,
        })
    }
    /// Stop admission and flush every accepted transaction, including a partial
    /// group. This does not acknowledge publication; wait tickets or join owner.
    pub fn close(&self) {
        lock(&self.inner).status.closed = true;
        self.inner.wake.notify_all();
    }
}

#[derive(Debug)]
pub struct BatchReport {
    pub published: FeedCursor,
    pub committed_operations: u64,
    pub groups: u64,
    pub timings: FeedIoTimings,
}
/// Unique lifetime owner. Explicit shutdown returns publication failures; Drop
/// also closes/joins rather than detaching an active writer. Failed tickets and
/// handle status retain errors even if the owner is dropped. Filesystem calls
/// can block: neither close nor join promises a bounded shutdown duration.
#[derive(Debug)]
pub struct FeedBatchActor {
    handle: BatchFeedHandle,
    worker: Option<JoinHandle<Result<BatchReport, BatchFailure>>>,
}
impl FeedBatchActor {
    pub fn spawn(writer: FeedWriter, config: BatchConfig) -> Result<Self, RecordingError> {
        Self::spawn_target(writer, config)
    }
    fn spawn_target<T: PublicationTarget>(
        writer: T,
        config: BatchConfig,
    ) -> Result<Self, RecordingError> {
        config.validate()?;
        let inner = Arc::new(Inner {
            config,
            profile: writer.profile(),
            state: Mutex::new(State {
                queue: VecDeque::new(),
                status: BatchStatus {
                    admitted_operations: 0,
                    committed_operations: 0,
                    pending_operations: 0,
                    pending_bytes: 0,
                    closed: false,
                    failure: None,
                    last_acknowledged: writer.cursor().clone(),
                },
            }),
            wake: Condvar::new(),
        });
        let worker_inner = inner.clone();
        let worker = thread::Builder::new()
            .name("shared-feed-writer".into())
            .spawn(move || {
                // Cover even a panic in final settlement/destruction. Active
                // disconnected receipts report Unknown; retained queue entries
                // are explicitly settled as never attempted.
                catch_unwind(AssertUnwindSafe(|| {
                    worker_entry(writer, worker_inner.clone())
                }))
                .unwrap_or_else(|_| Err(poison_queued(&worker_inner)))
            })
            .map_err(|e| error(e.to_string()))?;
        Ok(Self {
            handle: BatchFeedHandle { inner },
            worker: Some(worker),
        })
    }
    pub fn handle(&self) -> BatchFeedHandle {
        self.handle.clone()
    }
    pub fn shutdown(mut self) -> Result<BatchReport, BatchFailure> {
        self.finish()
    }
    fn finish(&mut self) -> Result<BatchReport, BatchFailure> {
        self.handle.close();
        self.worker
            .take()
            .expect("unique owner joins once")
            .join()
            .unwrap_or_else(|_| Err(poison_queued(&self.handle.inner)))
    }
}

fn poison_queued(inner: &Inner) -> BatchFailure {
    let mut state = lock(inner);
    let failure = BatchFailure {
        cause: BatchFailureCause::WorkerPanicked,
        last_acknowledged: state.status.last_acknowledged.clone(),
    };
    state.status.failure = Some(failure.clone());
    state.status.closed = true;
    state.status.pending_operations = 0;
    state.status.pending_bytes = 0;
    for pending in state.queue.drain(..) {
        let _ = pending.sender.send(Err(CommitFailure {
            admission: pending.admission,
            failure: failure.clone(),
            disposition: CommitDisposition::NotAttempted,
        }));
    }
    inner.wake.notify_all();
    failure
}
impl Drop for FeedBatchActor {
    fn drop(&mut self) {
        if self.worker.is_some() {
            let _ = self.finish();
        }
    }
}

trait PublicationTarget: Send + 'static {
    fn profile(&self) -> FeedProfile {
        FeedProfile::Legacy
    }
    fn cursor(&self) -> &FeedCursor;
    fn publish(&mut self, records: Vec<FeedRecord>) -> Result<FeedCursor, RecordingError>;
    fn timings(&self) -> FeedIoTimings;
}
impl PublicationTarget for FeedWriter {
    fn profile(&self) -> FeedProfile {
        self.profile()
    }
    fn cursor(&self) -> &FeedCursor {
        self.cursor()
    }
    fn publish(&mut self, records: Vec<FeedRecord>) -> Result<FeedCursor, RecordingError> {
        self.append_many(records)
    }
    fn timings(&self) -> FeedIoTimings {
        self.timings()
    }
}

struct ActiveGroup {
    pending: Vec<Pending>,
    ranges: Vec<(u64, u64)>,
    attempted: bool,
}
fn worker_entry<T: PublicationTarget>(
    mut writer: T,
    inner: Arc<Inner>,
) -> Result<BatchReport, BatchFailure> {
    // Retain senders outside the catch scope. Active work and the untouched
    // queue suffix receive distinct dispositions after a fatal error.
    let mut active = ActiveGroup {
        pending: Vec::new(),
        ranges: Vec::new(),
        attempted: false,
    };
    let result = catch_unwind(AssertUnwindSafe(|| {
        worker_loop(&mut writer, &inner, &mut active)
    }))
    .unwrap_or(Err(BatchFailureCause::WorkerPanicked));
    if let Err(cause) = result {
        let mut state = lock(&inner);
        let failure = BatchFailure {
            cause,
            last_acknowledged: state.status.last_acknowledged.clone(),
        };
        state.status.closed = true;
        state.status.failure = Some(failure.clone());
        let queued: Vec<_> = state.queue.drain(..).collect();
        state.status.pending_operations = 0;
        state.status.pending_bytes = 0;
        drop(state);
        for (index, pending) in active.pending.drain(..).enumerate() {
            let disposition = if active.attempted {
                active.ranges.get(index).map_or(
                    CommitDisposition::Unknown,
                    |&(first_sequence, last_sequence)| CommitDisposition::Attempted {
                        first_sequence,
                        last_sequence,
                    },
                )
            } else {
                CommitDisposition::NotAttempted
            };
            let _ = pending.sender.send(Err(CommitFailure {
                admission: pending.admission,
                failure: failure.clone(),
                disposition,
            }));
        }
        for pending in queued {
            let _ = pending.sender.send(Err(CommitFailure {
                admission: pending.admission,
                failure: failure.clone(),
                disposition: CommitDisposition::NotAttempted,
            }));
        }
        inner.wake.notify_all();
        return Err(failure);
    }
    result.map_err(|_| unreachable!("failure handled above"))
}

fn worker_loop<T: PublicationTarget>(
    writer: &mut T,
    inner: &Inner,
    active: &mut ActiveGroup,
) -> Result<BatchReport, BatchFailureCause> {
    let mut groups = 0u64;
    loop {
        let mut state = lock(inner);
        loop {
            if state.queue.is_empty() {
                if state.status.closed {
                    return Ok(BatchReport {
                        published: writer.cursor().clone(),
                        committed_operations: state.status.committed_operations,
                        groups,
                        timings: writer.timings(),
                    });
                }
                state = inner.wake.wait(state).unwrap_or_else(|p| p.into_inner());
                continue;
            }
            let (mut count, mut bytes, mut records) = (0, 0, 0);
            for pending in &state.queue {
                let tx = &pending.transaction;
                if count == inner.config.max_group_operations
                    || tx.bytes > inner.config.max_group_bytes - bytes
                    || tx.records.len() > MAX_RECORDS - records
                {
                    break;
                }
                count += 1;
                bytes += tx.bytes;
                records += tx.records.len();
            }
            let age = state
                .queue
                .front()
                .expect("nonempty queue")
                .admitted_at
                .elapsed();
            let flush = state.status.closed
                || count == inner.config.max_group_operations
                || bytes == inner.config.max_group_bytes
                || records == MAX_RECORDS
                || count < state.queue.len()
                || age >= inner.config.max_age;
            if flush {
                active.attempted = false;
                active.ranges.clear();
                active.pending.extend(state.queue.drain(..count));
                break;
            }
            let (next, _) = inner
                .wake
                .wait_timeout(state, inner.config.max_age - age)
                .unwrap_or_else(|p| p.into_inner());
            state = next;
        }
        drop(state);
        let initial = writer.cursor().clone();
        let mut previous = initial.sequence;
        let mut records = Vec::new();
        for pending in &mut active.pending {
            let first = previous
                .checked_add(1)
                .ok_or_else(|| error("shared batch sequence exhausted"))?;
            let end = previous
                .checked_add(pending.transaction.records.len() as u64)
                .ok_or_else(|| error("shared batch sequence exhausted"))?;
            active.ranges.push((first, end));
            previous = end;
            records.append(&mut pending.transaction.records);
        }
        active.attempted = true;
        let published = writer.publish(records)?;
        if published.sequence != previous
            || published.feed_id != initial.feed_id
            || published.offset <= initial.offset
        {
            return Err(BatchFailureCause::InvalidPublicationReceipt);
        }
        groups += 1;
        let mut state = lock(inner);
        state.status.committed_operations += active.pending.len() as u64;
        state.status.last_acknowledged = published.clone();
        state.status.pending_operations -= active.pending.len();
        state.status.pending_bytes -= active
            .pending
            .iter()
            .map(|p| p.transaction.bytes)
            .sum::<usize>();
        drop(state);
        for (pending, (first_sequence, last_sequence)) in
            active.pending.drain(..).zip(active.ranges.drain(..))
        {
            let _ = pending.sender.send(Ok(CommitReceipt {
                admission: pending.admission,
                first_sequence,
                last_sequence,
                published: published.clone(),
            }));
        }
    }
}
