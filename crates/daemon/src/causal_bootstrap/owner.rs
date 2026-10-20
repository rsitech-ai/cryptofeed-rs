//! Private persistent operation owner for the opt-in disk/null profile.
//!
//! This driver does not open transports, forward external sinks or project a
//! compatibility feed. It exposes only post-barrier evidence and actual drains.
use super::*;
use marketfeed_adapter_api::SessionCommand;
use marketfeed_engine::{CaptureDrainPolicy, CaptureInput};
use marketfeed_model::ConnectionId;
use std::{
    collections::VecDeque,
    mem::size_of,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Condvar, Mutex, MutexGuard, mpsc},
    thread::{self, JoinHandle},
    time::Instant,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerPhase {
    Idle,
    Executing,
    AwaitingAdmission,
    AwaitingCommit,
    ReleasePending,
    Stopped,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadlineStage {
    Queued,
    Executing,
    Executed,
    Admitted,
    Release,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerFailure {
    Revoked,
    IngressFull,
    InputTooLarge,
    UnsupportedInput,
    InputOrdinalExhausted,
    SourceConnection,
    Deadline(DeadlineStage),
    Execution(String),
    Encoding(String),
    Incomplete(marketfeed_engine::CaptureCompleteness),
    Admission(AdmissionFailure),
    Commit(CommitFailure),
    InvalidReceipt,
    Native(EngineErrorDto),
    NativeStopped,
    WorkerPanicked,
    ObservationFull,
    ClockExhausted,
    FinalizationDeadline,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitSettlement {
    Committed(CommitReceipt),
    Failed(CommitFailure),
    /// A driver panic lost its waiter; actor acceptance is never cancellation.
    Unknown,
}
#[derive(Debug, Clone)]
pub struct OwnerStatus {
    pub phase: OwnerPhase,
    pub closing: bool,
    pub failure: Option<OwnerFailure>,
    pub accepted: u64,
    pub executed: u64,
    pub durable: u64,
    pub handed_off: u64,
    pub withheld: u64,
    pub abandoned: u64,
    pub rejected_stale: u64,
    pub stale_accepted: u64,
    pub pending_operations: usize,
    pub pending_bytes: usize,
    pub admission_retries: u64,
    pub last_settlement: Option<CommitSettlement>,
}
#[derive(Debug, Clone)]
pub struct CommittedSnapshot {
    pub route: CausalRoute,
    pub receipt: CommitReceipt,
    /// Captured lifecycle/counts/timers/queues only. Not a full book snapshot.
    pub state: StateDto,
    pub expires_at: Instant,
}
#[derive(Debug)]
pub struct CommittedRelease {
    input_id: u64,
    operation: Arc<OperationDto>,
    receipt: CommitReceipt,
    observed_at: Instant,
    outputs_eligible: bool,
}
impl CommittedRelease {
    pub fn input_id(&self) -> u64 {
        self.input_id
    }
    pub fn receipt(&self) -> &CommitReceipt {
        &self.receipt
    }
    /// Local monotonic handoff observation, not exchange time or a value that
    /// could have been placed in the earlier durable operation retroactively.
    pub fn observed_at(&self) -> Instant {
        self.observed_at
    }
    pub fn evidence(&self) -> &OperationDto {
        &self.operation
    }
    /// Actual complete drains in execution order. Produced and queue-admitted
    /// items are deliberately not turned into eligible output here.
    pub fn released_items(&self) -> impl Iterator<Item = &ItemDto> {
        self.operation
            .entries
            .iter()
            .flat_map(|entry| match &entry.effect {
                EffectDto::QueueDrain {
                    items,
                    completed: true,
                    ..
                } if self.outputs_eligible => items.as_slice(),
                _ => &[],
            })
    }
    /// Safety-transition evidence may be committed without making its drained
    /// values eligible for output. Neither disposition proves external delivery.
    pub fn outputs_eligible(&self) -> bool {
        self.outputs_eligible
    }
}
#[derive(Debug)]
pub struct RejectedIngress {
    pub reason: OwnerFailure,
    pub connection: ConnectionId,
    pub input: CaptureInput,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputSettlement {
    NotExecuted {
        id: u64,
        reason: OwnerFailure,
    },
    ExecutedNotAdmitted {
        id: u64,
        reason: OwnerFailure,
    },
    Committed {
        id: u64,
        receipt: CommitReceipt,
    },
    CommitFailed {
        id: u64,
        failure: CommitFailure,
    },
    Unknown {
        id: u64,
    },
    NotExecutedCommitted {
        id: u64,
        reason: OwnerFailure,
        receipt: CommitReceipt,
    },
}
/// Settlement is evidence about this accepted input, not an output permit.
/// Dropping the waiter never cancels accepted input or storage work.
#[derive(Debug)]
pub struct IngressAdmission {
    pub id: u64,
    receiver: mpsc::Receiver<InputSettlement>,
    settled: Option<InputSettlement>,
}
impl IngressAdmission {
    pub fn wait(mut self) -> InputSettlement {
        self.settled.take().unwrap_or_else(|| {
            self.receiver
                .recv()
                .unwrap_or(InputSettlement::Unknown { id: self.id })
        })
    }
    pub fn try_result(&mut self) -> Option<InputSettlement> {
        if self.settled.is_none() {
            self.settled = match self.receiver.try_recv() {
                Ok(result) => Some(result),
                Err(mpsc::TryRecvError::Empty) => None,
                Err(mpsc::TryRecvError::Disconnected) => {
                    Some(InputSettlement::Unknown { id: self.id })
                }
            };
        }
        self.settled.clone()
    }
}

struct QueuedInput {
    id: u64,
    connection: ConnectionId,
    input: CaptureInput,
    bytes: usize,
    deadline: Instant,
    settlement: mpsc::Sender<InputSettlement>,
}
struct ReleaseSlot {
    release: CommittedRelease,
    deadline: Instant,
}
struct State {
    queue: VecDeque<QueuedInput>,
    release: Option<ReleaseSlot>,
    snapshot: Option<CommittedSnapshot>,
    status: OwnerStatus,
    visibility_fence: u64,
    source_connection: ConnectionId,
    source_active: bool,
}
struct Shared {
    writer: BatchFeedHandle,
    state: Mutex<State>,
    wake: Condvar,
    policy: JournalOwnerPolicyDto,
}
fn lock(shared: &Shared) -> MutexGuard<'_, State> {
    shared.state.lock().unwrap_or_else(|p| p.into_inner())
}
fn observe_writer(state: &mut State, shared: &Shared) {
    if state.status.phase == OwnerPhase::Stopped || state.status.failure.is_some() {
        return;
    }
    let writer = shared.writer.status();
    let reason = if let Some(failure) = writer.failure {
        Some(AdmissionFailure::Failed(failure))
    } else if writer.closed {
        Some(AdmissionFailure::Closed)
    } else {
        None
    };
    if let Some(reason) = reason {
        revoke(state, OwnerFailure::Admission(reason));
        shared.wake.notify_all();
    }
}
fn revoke(state: &mut State, reason: OwnerFailure) {
    if state.status.failure.is_none() {
        state.status.failure = Some(reason);
    }
    state.status.closing = true;
    state.snapshot = None;
    if state.release.take().is_some() {
        state.status.withheld += 1;
    }
}
fn fence(state: &mut State, input_id: u64) {
    // Fence older turns, not valid later inputs already queued on the same
    // source. Monotonic ingress IDs also handle safety discovered at execution.
    state.visibility_fence = state.visibility_fence.max(input_id);
    state.snapshot = None;
    if state.release.take().is_some() {
        state.status.withheld += 1;
    }
}

/// Clones accept bounded source-stamped input while the private worker waits
/// for durability. Rejected input remains owned by the caller; it is never run.
#[derive(Clone)]
pub struct CausalIngress {
    shared: Arc<Shared>,
}
impl CausalIngress {
    pub fn try_submit(
        &self,
        connection: ConnectionId,
        mut input: CaptureInput,
    ) -> Result<IngressAdmission, Box<RejectedIngress>> {
        let now = Instant::now();
        let bytes = input_bytes(&input);
        let mut state = lock(&self.shared);
        observe_writer(&mut state, &self.shared);
        let policy = &self.shared.policy;
        let source_ok = match &input {
            CaptureInput::Connected {
                connection: incoming,
                ..
            } => *incoming == connection && connection.0 > state.source_connection.0,
            _ if frame_input(&input) => {
                state.source_active && connection == state.source_connection
            }
            _ => connection == state.source_connection,
        };
        let reason = if let Some(reason) = &state.status.failure {
            Some(reason.clone())
        } else if state.status.closing || state.status.phase == OwnerPhase::Stopped {
            Some(OwnerFailure::Revoked)
        } else if !supported(&input) {
            Some(OwnerFailure::UnsupportedInput)
        } else if bytes > policy.max_input_bytes as usize {
            Some(OwnerFailure::InputTooLarge)
        } else if !source_ok {
            Some(OwnerFailure::SourceConnection)
        } else if state.status.pending_operations >= policy.max_pending_operations as usize
            || bytes
                > (policy.max_pending_bytes as usize).saturating_sub(state.status.pending_bytes)
        {
            Some(OwnerFailure::IngressFull)
        } else if state.status.accepted == u64::MAX {
            Some(OwnerFailure::InputOrdinalExhausted)
        } else {
            None
        };
        if let Some(reason) = reason {
            if reason == OwnerFailure::SourceConnection {
                state.status.rejected_stale += 1;
            } else if state.status.phase != OwnerPhase::Stopped {
                revoke(&mut state, reason.clone());
            }
            self.shared.wake.notify_all();
            return Err(Box::new(RejectedIngress {
                reason,
                connection,
                input,
            }));
        }
        let deadline = now
            .checked_add(policy.max_ingress_age.duration().expect("validated policy"))
            .expect("bounded one-hour deadline");
        state.status.accepted += 1;
        let id = state.status.accepted;
        state.status.pending_operations += 1;
        state.status.pending_bytes += bytes;
        // Bytes can be a tiny slice retaining an arbitrarily large backing
        // allocation. Copy only after admission budget checks, releasing that
        // backing before the bounded owner queue retains the input.
        if let CaptureInput::Http { response, .. } = &mut input {
            response.body = response.body.to_vec().into();
        }
        if matches!(input, CaptureInput::Connected { .. }) {
            fence(&mut state, id);
            state.source_connection = connection;
            state.source_active = true;
        } else if safety_input(&input) {
            fence(&mut state, id);
            state.source_active = false;
        }
        let (settlement, receiver) = mpsc::channel();
        state.queue.push_back(QueuedInput {
            id,
            connection,
            input,
            bytes,
            deadline,
            settlement,
        });
        self.shared.wake.notify_all();
        Ok(IngressAdmission {
            id,
            receiver,
            settled: None,
        })
    }
    /// Immediate safety revocation is independent of a blocked writer. Future
    /// handoffs and snapshots are denied; accepted storage still settles.
    pub fn revoke(&self) {
        let mut state = lock(&self.shared);
        if state.status.phase != OwnerPhase::Stopped {
            revoke(&mut state, OwnerFailure::Revoked);
        }
        self.shared.wake.notify_all();
    }
    pub fn status(&self) -> OwnerStatus {
        let mut state = lock(&self.shared);
        observe_writer(&mut state, &self.shared);
        state.status.clone()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OwnerStartError {
    #[error("owner requires bootstrap V2 with an explicit bound policy")]
    MissingPolicy,
    #[error("bootstrap writer no longer available or does not cover bootstrap receipt")]
    WriterUnavailable,
    #[error("declared operation bound cannot fit original writer admission caps")]
    IncompatibleBounds,
    #[error("owner startup selected a different negotiated journal profile")]
    Profile,
    #[error(transparent)]
    Io(#[from] io::Error),
}
pub struct JournalOwner {
    ingress: CausalIngress,
    worker: Option<JoinHandle<OwnerStatus>>,
}
impl BootstrappedSession {
    pub fn spawn_owner(self) -> Result<JournalOwner, OwnerStartError> {
        if self.history_policy.is_some() {
            return Err(OwnerStartError::Profile);
        }
        let policy = self.owner_policy.ok_or(OwnerStartError::MissingPolicy)?;
        let writer = self.handle.status();
        if writer.closed
            || writer.failure.is_some()
            || writer.last_acknowledged.feed_id != self.receipt.published.feed_id
            || writer.last_acknowledged.sequence < self.receipt.last_sequence
        {
            return Err(OwnerStartError::WriterUnavailable);
        }
        let caps = self.handle.config();
        let probe = CausalEnvelope::new(
            CausalPhase::Operation,
            CausalRoute {
                venue: self.identity.venue,
                session: self.identity.session,
                bootstrap_id: self.identity.bootstrap_id.clone(),
                ordinal: u64::MAX,
                generation: u64::MAX,
            },
            b"x",
        )
        .map_err(|_| OwnerStartError::IncompatibleBounds)?;
        let fixed = PreparedFeedTransaction::new(vec![FeedRecord::Causal(probe)])
            .map_err(|_| OwnerStartError::IncompatibleBounds)?
            .encoded_bytes_bound()
            - 4;
        let wire_bound = fixed + (policy.max_operation_bytes as usize).div_ceil(3) * 4;
        if wire_bound > caps.max_group_bytes || wire_bound > caps.max_pending_bytes {
            return Err(OwnerStartError::IncompatibleBounds);
        }
        let shared = Arc::new(Shared {
            writer: self.handle.clone(),
            policy,
            wake: Condvar::new(),
            state: Mutex::new(State {
                queue: VecDeque::new(),
                release: None,
                snapshot: None,
                visibility_fence: 0,
                source_connection: ConnectionId(0),
                source_active: false,
                status: OwnerStatus {
                    phase: OwnerPhase::Idle,
                    closing: false,
                    failure: None,
                    accepted: 0,
                    executed: 0,
                    durable: 0,
                    handed_off: 0,
                    withheld: 0,
                    abandoned: 0,
                    rejected_stale: 0,
                    stale_accepted: 0,
                    pending_operations: 0,
                    pending_bytes: 0,
                    admission_retries: 0,
                    last_settlement: None,
                },
            }),
        });
        let worker_shared = shared.clone();
        let worker = thread::Builder::new()
            .name("causal-operation-owner".into())
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    drive(
                        self.capture,
                        self.handle,
                        self.identity,
                        self.receipt,
                        &worker_shared,
                    )
                }));
                if result.is_err() {
                    let mut state = lock(&worker_shared);
                    revoke(&mut state, OwnerFailure::WorkerPanicked);
                    state.status.last_settlement = Some(CommitSettlement::Unknown);
                }
                let mut state = lock(&worker_shared);
                let abandoned = state.queue.len() as u64;
                let reason = state
                    .status
                    .failure
                    .clone()
                    .unwrap_or(OwnerFailure::Revoked);
                for pending in state.queue.drain(..) {
                    let _ = pending.settlement.send(InputSettlement::NotExecuted {
                        id: pending.id,
                        reason: reason.clone(),
                    });
                }
                state.status.abandoned += abandoned;
                state.status.pending_operations = 0;
                state.status.pending_bytes = 0;
                state.status.phase = OwnerPhase::Stopped;
                state.snapshot = None;
                worker_shared.wake.notify_all();
                state.status.clone()
            })?;
        Ok(JournalOwner {
            ingress: CausalIngress { shared },
            worker: Some(worker),
        })
    }
}
impl JournalOwner {
    pub fn ingress(&self) -> CausalIngress {
        self.ingress.clone()
    }
    pub fn status(&self) -> OwnerStatus {
        self.ingress.status()
    }
    pub fn committed_snapshot(&self) -> Option<CommittedSnapshot> {
        let mut state = lock(&self.ingress.shared);
        observe_writer(&mut state, &self.ingress.shared);
        let now = Instant::now();
        if state
            .release
            .as_ref()
            .is_some_and(|slot| now >= slot.deadline)
        {
            revoke(&mut state, OwnerFailure::Deadline(DeadlineStage::Release));
            self.ingress.shared.wake.notify_all();
        }
        if state
            .snapshot
            .as_ref()
            .is_some_and(|snapshot| now >= snapshot.expires_at)
        {
            state.snapshot = None;
        }
        if state.status.failure.is_some() {
            None
        } else {
            state.snapshot.clone()
        }
    }
    /// Handoff and revocation are serialized under one lock. Already handed-off
    /// data cannot be unsent; live I/O owners must honor their own stop boundary.
    pub fn try_next(&self) -> Option<CommittedRelease> {
        let mut state = lock(&self.ingress.shared);
        observe_writer(&mut state, &self.ingress.shared);
        let now = Instant::now();
        if state.status.failure.is_some() {
            return None;
        }
        if state
            .release
            .as_ref()
            .is_some_and(|slot| now >= slot.deadline)
        {
            revoke(&mut state, OwnerFailure::Deadline(DeadlineStage::Release));
            self.ingress.shared.wake.notify_all();
            return None;
        }
        let mut release = state.release.take()?.release;
        release.observed_at = now;
        state.status.handed_off += 1;
        self.ingress.shared.wake.notify_all();
        Some(release)
    }
    /// Gracefully stop ingress; keep consuming releases until Stopped, then
    /// shutdown can join. No seal is written by this owner.
    pub fn close_ingress(&self) {
        lock(&self.ingress.shared).status.closing = true;
        self.ingress.shared.wake.notify_all();
    }
    pub fn shutdown(mut self) -> OwnerStatus {
        self.ingress.revoke();
        self.worker
            .take()
            .expect("owned worker")
            .join()
            .expect("worker catches panic")
    }
}
impl Drop for JournalOwner {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            self.ingress.revoke();
            // A caller disappearing cannot detach an accepted storage operation.
            // Filesystem stalls retain the writer's unbounded wait limitation.
            let _ = worker.join();
        }
    }
}

fn fail(shared: &Shared, reason: OwnerFailure) {
    revoke(&mut lock(shared), reason);
    shared.wake.notify_all();
}
fn expired(shared: &Shared, deadline: Instant, stage: DeadlineStage) -> bool {
    let mut state = lock(shared);
    observe_writer(&mut state, shared);
    if Instant::now() >= deadline {
        revoke(&mut state, OwnerFailure::Deadline(stage));
    }
    state.status.failure.is_some()
}
fn pause(shared: &Shared, deadline: Instant) {
    let duration = deadline.saturating_duration_since(Instant::now()).min(
        shared
            .policy
            .receipt_poll_interval
            .duration()
            .expect("validated poll"),
    );
    let duration = if duration.is_zero() {
        shared
            .policy
            .receipt_poll_interval
            .duration()
            .expect("validated poll")
    } else {
        duration
    };
    let state = lock(shared);
    let _ = shared
        .wake
        .wait_timeout(state, duration)
        .unwrap_or_else(|p| p.into_inner());
}
fn retire(shared: &Shared, bytes: usize) {
    let mut state = lock(shared);
    state.status.pending_operations -= 1;
    state.status.pending_bytes -= bytes;
}
struct SettlementGuard<'a> {
    shared: &'a Shared,
    id: u64,
    sender: Option<mpsc::Sender<InputSettlement>>,
    executed: bool,
    admitted: bool,
}
impl SettlementGuard<'_> {
    fn settle(&mut self, value: InputSettlement) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(value);
        }
    }
}
impl Drop for SettlementGuard<'_> {
    fn drop(&mut self) {
        if self.sender.is_some() {
            let reason = lock(self.shared)
                .status
                .failure
                .clone()
                .unwrap_or(OwnerFailure::WorkerPanicked);
            let value = if self.admitted {
                InputSettlement::Unknown { id: self.id }
            } else if self.executed {
                InputSettlement::ExecutedNotAdmitted {
                    id: self.id,
                    reason,
                }
            } else {
                InputSettlement::NotExecuted {
                    id: self.id,
                    reason,
                }
            };
            self.settle(value);
        }
    }
}
fn drive(
    mut capture: CaptureSession,
    handle: BatchFeedHandle,
    identity: OperationIdentity,
    bootstrap: CommitReceipt,
    shared: &Shared,
) {
    let mut connection = ConnectionId(0);
    let mut source_active = false;
    let mut last_sequence = bootstrap.last_sequence;
    loop {
        let item = {
            let mut state = lock(shared);
            loop {
                observe_writer(&mut state, shared);
                if state.status.failure.is_some() {
                    return;
                }
                if state.release.is_some() {
                    let deadline = state.release.as_ref().expect("slot").deadline;
                    if Instant::now() >= deadline {
                        revoke(&mut state, OwnerFailure::Deadline(DeadlineStage::Release));
                        return;
                    }
                    state.status.phase = OwnerPhase::ReleasePending;
                    let wait = deadline.saturating_duration_since(Instant::now()).min(
                        shared
                            .policy
                            .receipt_poll_interval
                            .duration()
                            .expect("policy"),
                    );
                    state = shared
                        .wake
                        .wait_timeout(state, wait)
                        .unwrap_or_else(|p| p.into_inner())
                        .0;
                } else if let Some(item) = state.queue.pop_front() {
                    break item;
                } else if state.status.closing {
                    return;
                } else {
                    state.status.phase = OwnerPhase::Idle;
                    state = shared
                        .wake
                        .wait_timeout(
                            state,
                            shared
                                .policy
                                .receipt_poll_interval
                                .duration()
                                .expect("validated poll"),
                        )
                        .unwrap_or_else(|p| p.into_inner())
                        .0;
                }
            }
        };
        let QueuedInput {
            id,
            connection: source,
            input,
            bytes,
            deadline,
            settlement,
        } = item;
        let mut settlement = SettlementGuard {
            shared,
            id,
            sender: Some(settlement),
            executed: false,
            admitted: false,
        };
        if expired(shared, deadline, DeadlineStage::Queued) {
            retire(shared, bytes);
            lock(shared).status.abandoned += 1;
            return;
        }
        let correct = match &input {
            CaptureInput::Connected {
                connection: incoming,
                ..
            } => *incoming == source && source.0 > connection.0,
            _ if frame_input(&input) => source == connection && source_active,
            _ => source == connection,
        };
        if !correct {
            retire(shared, bytes);
            let mut state = lock(shared);
            state.status.abandoned += 1;
            state.status.stale_accepted += 1;
            settlement.settle(InputSettlement::NotExecuted {
                id,
                reason: OwnerFailure::SourceConnection,
            });
            continue;
        }
        let writer = handle.status();
        if writer.closed || writer.failure.is_some() {
            fail(
                shared,
                OwnerFailure::Admission(
                    writer
                        .failure
                        .map_or(AdmissionFailure::Closed, AdmissionFailure::Failed),
                ),
            );
            retire(shared, bytes);
            lock(shared).status.abandoned += 1;
            return;
        }
        {
            let mut state = lock(shared);
            if Instant::now() >= deadline {
                revoke(&mut state, OwnerFailure::Deadline(DeadlineStage::Queued));
            }
            if state.status.failure.is_some() {
                state.status.abandoned += 1;
                drop(state);
                retire(shared, bytes);
                return;
            }
            state.status.phase = OwnerPhase::Executing;
            state.status.executed += 1;
            settlement.executed = true;
        }
        let operation =
            match capture.execute_turn(input, CaptureDrainPolicy::NullDispatchTakeOutbound) {
                Ok(operation) => operation,
                Err(error) => {
                    // CaptureSession returns Err only from preflight; actual
                    // callback failures are complete CapturedOperation outcomes.
                    settlement.executed = false;
                    {
                        let mut state = lock(shared);
                        state.status.executed -= 1;
                        state.status.abandoned += 1;
                    }
                    fail(shared, OwnerFailure::Execution(error.to_string()));
                    retire(shared, bytes);
                    return;
                }
            };
        if matches!(operation.original_input, CaptureInput::Connected { .. }) {
            source_active = true;
        } else if safety_input(&operation.original_input) {
            source_active = false;
        }
        let unsafe_state = !matches!(
            &operation.outcome,
            marketfeed_engine::CaptureOutcome::Returned(Ok(()))
        ) || (operation.after.stop_requested
            && !operation.before.stop_requested)
            || (operation.after.reconnect_requested && !operation.before.reconnect_requested)
            || operation.after.valid_books < operation.before.valid_books
            || (operation.before.lifecycle == marketfeed_engine::SessionLifecycle::Live
                && operation.after.lifecycle != marketfeed_engine::SessionLifecycle::Live);
        if unsafe_state {
            let mut state = lock(shared);
            // A later observed source event already fenced this operation. Do
            // not overwrite its source state or let this older turn clear it.
            if id >= state.visibility_fence {
                fence(&mut state, id);
                if operation.after.reconnect_requested || operation.after.stop_requested {
                    state.source_active = false;
                }
            }
        }
        if operation.after.reconnect_requested || operation.after.stop_requested {
            source_active = false;
        }
        if operation.completeness != marketfeed_engine::CaptureCompleteness::Complete {
            fail(shared, OwnerFailure::Incomplete(operation.completeness));
            retire(shared, bytes);
            return;
        }
        let prepared = (|| -> Result<_, String> {
            let envelope = encode_operation(
                &operation,
                &identity,
                shared.policy.max_operation_bytes as usize,
            )
            .map_err(|e| e.to_string())?;
            let dto = decode_operation(&envelope, &identity).map_err(|e| e.to_string())?;
            let tx = PreparedFeedTransaction::new(vec![FeedRecord::Causal(envelope)])
                .map_err(|e| e.to_string())?;
            Ok((dto, tx))
        })();
        drop(operation);
        let (dto, mut tx) = match prepared {
            Ok(value) => value,
            Err(error) => {
                fail(shared, OwnerFailure::Encoding(error));
                retire(shared, bytes);
                return;
            }
        };
        if expired(shared, deadline, DeadlineStage::Executed) {
            retire(shared, bytes);
            return;
        }
        lock(shared).status.phase = OwnerPhase::AwaitingAdmission;
        let mut ticket = loop {
            if expired(shared, deadline, DeadlineStage::Executed) {
                retire(shared, bytes);
                return;
            }
            match handle.try_submit(tx) {
                Ok(ticket) => break ticket,
                Err(rejected) => {
                    tx = rejected.transaction;
                    if rejected.reason != AdmissionFailure::Full {
                        fail(shared, OwnerFailure::Admission(rejected.reason));
                        retire(shared, bytes);
                        return;
                    }
                    lock(shared).status.admission_retries += 1;
                    pause(shared, deadline);
                }
            }
        };
        settlement.admitted = true;
        lock(shared).status.phase = OwnerPhase::AwaitingCommit;
        let admission = ticket.admission();
        let result = loop {
            // Revocation/deadline never discards the ticket or reruns execution.
            expired(shared, deadline, DeadlineStage::Admitted);
            if let Some(result) = ticket.try_result() {
                break result;
            }
            pause(shared, deadline);
        };
        let receipt = match result {
            Ok(receipt) => receipt,
            Err(error) => {
                lock(shared).status.last_settlement = Some(CommitSettlement::Failed(error.clone()));
                fail(shared, OwnerFailure::Commit(error));
                if let Some(CommitSettlement::Failed(failure)) =
                    &lock(shared).status.last_settlement
                {
                    settlement.settle(InputSettlement::CommitFailed {
                        id,
                        failure: failure.clone(),
                    });
                }
                retire(shared, bytes);
                return;
            }
        };
        if receipt.admission != admission
            || receipt.first_sequence != receipt.last_sequence
            || receipt.first_sequence <= last_sequence
            || receipt.published.sequence < receipt.last_sequence
            || receipt.published.feed_id != bootstrap.published.feed_id
        {
            fail(shared, OwnerFailure::InvalidReceipt);
            retire(shared, bytes);
            return;
        }
        last_sequence = receipt.last_sequence;
        settlement.settle(InputSettlement::Committed {
            id,
            receipt: receipt.clone(),
        });
        connection = ConnectionId(dto.after.connection);
        retire(shared, bytes);
        let mut state = lock(shared);
        state.status.durable += 1;
        state.status.last_settlement = Some(CommitSettlement::Committed(receipt.clone()));
        if Instant::now() >= deadline {
            revoke(&mut state, OwnerFailure::Deadline(DeadlineStage::Admitted));
        }
        if let Err(error) = &dto.outcome {
            revoke(&mut state, OwnerFailure::Native(error.clone()));
        }
        if dto.after.stop_requested {
            revoke(&mut state, OwnerFailure::NativeStopped);
        }
        if state.status.failure.is_some() {
            state.status.withheld += 1;
            return;
        }
        if id < state.visibility_fence {
            state.status.withheld += 1;
            continue;
        }
        // A degraded/synchronizing adapter still needs its committed subscribe
        // and recovery requests. A known closed/reconnecting source does not.
        // The snapshot preserves its actual lifecycle; it never claims Live.
        let outputs_eligible = source_active
            && !dto.after.reconnect_requested
            && !dto.after.stop_requested
            && dto.outcome.is_ok();
        state.snapshot = outputs_eligible.then(|| CommittedSnapshot {
            route: dto.route.clone(),
            receipt: receipt.clone(),
            state: dto.after.clone(),
            expires_at: deadline,
        });
        state.release = Some(ReleaseSlot {
            release: CommittedRelease {
                input_id: id,
                operation: Arc::new(dto),
                receipt,
                observed_at: Instant::now(),
                outputs_eligible,
            },
            deadline,
        });
        state.status.phase = OwnerPhase::ReleasePending;
        shared.wake.notify_all();
    }
}

fn frame_input(input: &CaptureInput) -> bool {
    matches!(
        input,
        CaptureInput::Text { .. }
            | CaptureInput::Binary { .. }
            | CaptureInput::Ping { .. }
            | CaptureInput::Pong { .. }
            | CaptureInput::Close { .. }
            | CaptureInput::Http { .. }
    )
}
fn safety_input(input: &CaptureInput) -> bool {
    matches!(
        input,
        CaptureInput::Disconnected { .. }
            | CaptureInput::TransportLost { .. }
            | CaptureInput::Close { .. }
            | CaptureInput::Stop
            | CaptureInput::Control {
                command: SessionCommand::Stop,
                ..
            }
    )
}

fn supported(input: &CaptureInput) -> bool {
    matches!(
        input,
        CaptureInput::Connected { .. }
            | CaptureInput::Disconnected { .. }
            | CaptureInput::TransportLost { .. }
            | CaptureInput::Text { .. }
            | CaptureInput::Binary { .. }
            | CaptureInput::Ping { .. }
            | CaptureInput::Pong { .. }
            | CaptureInput::Close { .. }
            | CaptureInput::Http { .. }
            | CaptureInput::TimerPoll { .. }
            | CaptureInput::Stop
            | CaptureInput::Control {
                command: SessionCommand::Resync(_) | SessionCommand::Stop,
                ..
            }
    )
}
/// Conservatively count owned ingress capacities as well as fixed enum/storage
/// overhead. Captures/encoded records/release slot have separate explicit caps;
/// allocator metadata, caller allocation and process RSS are not measured.
fn input_bytes(input: &CaptureInput) -> usize {
    let extra = match input {
        CaptureInput::Text { payload, .. }
        | CaptureInput::Binary { payload, .. }
        | CaptureInput::Ping { payload, .. }
        | CaptureInput::Pong { payload, .. }
        | CaptureInput::Close { payload, .. } => payload.capacity(),
        CaptureInput::Http { response, .. } => response.headers.iter().fold(
            response.body.len().saturating_add(
                response
                    .headers
                    .capacity()
                    .saturating_mul(size_of::<(String, String)>()),
            ),
            |sum, (k, v)| {
                sum.saturating_add(k.capacity())
                    .saturating_add(v.capacity())
            },
        ),
        _ => 0,
    };
    size_of::<QueuedInput>().saturating_add(extra)
}

#[cfg(all(test, feature = "causal-journal"))]
mod tests;

pub mod history;
