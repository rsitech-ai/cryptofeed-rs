//! Private disk/null native owner (MFCJ3/4), with opt-in MFCJ5 application-write
//! authorization. The owner never opens a transport or external market sink.
//! Ordered observations and native execution have separate counters and bounds.
//! Internal handoff precedes its observation's durable ACK: that crash interval
//! remains explicitly uncertain, never an external-delivery acknowledgement.
use super::*;
use marketfeed_engine::{CaptureCompleteness, CaptureOutcome, CapturedOperation};
use serde::Serialize;
use std::{
    io::Write,
    time::{SystemTime, UNIX_EPOCH},
};

mod wire;
use wire::WireRuntime;
pub use wire::{WireAuthorization, WireSendCompletion, WireSendError};

const SMALL: usize = 1024;
const RESERVED_SLOTS: usize = 5;
fn shared_wire_http(s: &Data, input: &CaptureInput) -> bool {
    s.wire.is_some() && matches!(input, CaptureInput::Http { .. })
}

enum Event {
    Control(OwnerEventDto),
    Ingress {
        id: u64,
        source: u64,
        deadline_ns: u64,
        bytes: usize,
        input_json: String,
    },
    Completion {
        id: u64,
        permit: u64,
        operation: Box<CapturedOperation>,
    },
}
struct Observation {
    ordinal: u64,
    observed_ns: u64,
    ingress_bytes: usize,
    event: Event,
}
struct Active {
    id: u64,
    operation: Arc<OperationDto>,
    receipt: CommitReceipt,
    deadline: Instant,
    completion_ordinal: u64,
    outputs_eligible: bool,
}
struct Ready {
    intent: u64,
    release: CommittedRelease,
    deadline: Instant,
}
struct Data {
    inputs: VecDeque<QueuedInput>,
    observations: VecDeque<Observation>,
    next_ordinal: u64,
    status: OwnerStatus,
    source: ConnectionId,
    source_active: bool,
    visibility_fence: u64,
    active: Option<Active>,
    disposition_queued: bool,
    cannot_record: bool,
    finalization_expired: bool,
    ready: Option<Ready>,
    snapshot: Option<CommittedSnapshot>,
    current_deadline: Option<Instant>,
    deadline_stage: DeadlineStage,
    finalization_deadline: Option<Instant>,
    ingress_observations: usize,
    ingress_observation_bytes: usize,
    preparing: usize,
    preparing_bytes: usize,
    preparing_observation_bytes: usize,
    journal_records: u64,
    wire: Option<WireRuntime>,
}
struct HistoryShared {
    writer: BatchFeedHandle,
    identity: OperationIdentity,
    schema_version: u16,
    policy: JournalOwnerPolicyDto,
    history: HistoryRuntimePolicyDto,
    origin: Instant,
    #[cfg(test)]
    clock_offset_ns: std::sync::atomic::AtomicU64,
    #[cfg(all(test, feature = "causal-journal"))]
    text_pause: Mutex<Option<Arc<TextPauseState>>>,
    state: Mutex<Data>,
    // Acceptance cannot be cancelled by a panic or a disappearing waiter. Keep
    // the ticket outside the worker stack until its actual result is known.
    ticket: Mutex<Option<CommitTicket>>,
    wake: Condvar,
}
// Per-owner, one-shot fixture barrier. No lock protecting owner admission or
// source/fence state is held while the actual worker is paused.
#[cfg(all(test, feature = "causal-journal"))]
#[derive(Default)]
struct TextPauseState {
    state: Mutex<(bool, bool)>,
    wake: Condvar,
}
#[cfg(all(test, feature = "causal-journal"))]
pub(crate) struct HistoryTextPause(Arc<TextPauseState>);
#[cfg(all(test, feature = "causal-journal"))]
impl HistoryTextPause {
    pub(crate) fn entered(&self) -> bool {
        self.0.state.lock().unwrap().0
    }
    pub(crate) fn release(&self) {
        self.0.state.lock().unwrap().1 = true;
        self.0.wake.notify_all();
    }
}
#[cfg(all(test, feature = "causal-journal"))]
impl Drop for HistoryTextPause {
    fn drop(&mut self) {
        self.release();
    }
}
#[cfg(all(test, feature = "causal-journal"))]
fn pause_text_worker(shared: &HistoryShared, observation: &Observation) {
    let mut slot = shared.text_pause.lock().unwrap();
    if slot.is_none() {
        return;
    }
    let Event::Ingress { input_json, .. } = &observation.event else {
        return;
    };
    if !matches!(
        serde_json::from_str::<InputDto>(input_json),
        Ok(InputDto::Text { .. })
    ) {
        return;
    }
    let pause = slot.take().unwrap();
    drop(slot);
    let mut state = pause.state.lock().unwrap();
    state.0 = true;
    pause.wake.notify_all();
    while !state.1 {
        state = pause.wake.wait(state).unwrap();
    }
}
fn state(shared: &HistoryShared) -> MutexGuard<'_, Data> {
    shared.state.lock().unwrap_or_else(|p| p.into_inner())
}
fn ticket(shared: &HistoryShared) -> MutexGuard<'_, Option<CommitTicket>> {
    shared.ticket.lock().unwrap_or_else(|p| p.into_inner())
}
fn elapsed_ns(shared: &HistoryShared, duration: std::time::Duration) -> Option<u64> {
    #[cfg(not(test))]
    let _ = shared;
    let value = u64::try_from(duration.as_nanos()).ok()?;
    #[cfg(test)]
    {
        value.checked_add(
            shared
                .clock_offset_ns
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }
    #[cfg(not(test))]
    {
        Some(value)
    }
}
fn now_ns(shared: &HistoryShared) -> Option<u64> {
    elapsed_ns(shared, shared.origin.elapsed())
}

fn push(shared: &HistoryShared, s: &mut Data, event: Event, ingress_bytes: usize) -> Option<u64> {
    let Some(observed_ns) = now_ns(shared) else {
        s.status.failure.get_or_insert(OwnerFailure::ClockExhausted);
        s.status.closing = true;
        s.snapshot = None;
        s.ready = None;
        s.cannot_record = true;
        return None;
    };
    let Some(next) = s.next_ordinal.checked_add(1) else {
        s.status
            .failure
            .get_or_insert(OwnerFailure::InputOrdinalExhausted);
        s.status.closing = true;
        s.snapshot = None;
        s.ready = None;
        s.cannot_record = true;
        return None;
    };
    s.next_ordinal = next;
    s.observations.push_back(Observation {
        ordinal: next,
        observed_ns,
        ingress_bytes,
        event,
    });
    shared.wake.notify_all();
    Some(next)
}
fn cause(reason: &OwnerFailure) -> OwnerTerminalCause {
    match reason {
        OwnerFailure::Revoked => OwnerTerminalCause::Revoked,
        OwnerFailure::IngressFull => OwnerTerminalCause::IngressFull,
        OwnerFailure::InputTooLarge => OwnerTerminalCause::InputTooLarge,
        OwnerFailure::UnsupportedInput => OwnerTerminalCause::UnsupportedInput,
        OwnerFailure::InputOrdinalExhausted | OwnerFailure::ClockExhausted => {
            OwnerTerminalCause::OrdinalExhausted
        }
        OwnerFailure::ObservationFull => OwnerTerminalCause::ObservationFull,
        OwnerFailure::Deadline(_) | OwnerFailure::FinalizationDeadline => {
            OwnerTerminalCause::Deadline
        }
        OwnerFailure::Encoding(_) | OwnerFailure::Execution(_) => OwnerTerminalCause::Encoding,
        OwnerFailure::Incomplete(_) => OwnerTerminalCause::Incomplete,
        OwnerFailure::Native(_) => OwnerTerminalCause::NativeError,
        OwnerFailure::NativeStopped => OwnerTerminalCause::NativeStopped,
        OwnerFailure::WorkerPanicked => OwnerTerminalCause::WorkerPanicked,
        OwnerFailure::InvalidReceipt => OwnerTerminalCause::InvalidReceipt,
        _ => OwnerTerminalCause::WriterUnavailable,
    }
}
fn revoke_history(shared: &HistoryShared, s: &mut Data, reason: OwnerFailure) {
    if s.status.failure.is_some() {
        return;
    }
    let event = OwnerEventDto::Revoked {
        through_input_id: s.status.accepted,
        cause: cause(&reason),
    };
    s.status.failure = Some(reason);
    s.status.closing = true;
    s.finalization_deadline = Instant::now().checked_add(
        shared
            .history
            .max_finalization_age
            .duration()
            .expect("validated policy"),
    );
    s.snapshot = None;
    push(shared, s, Event::Control(event), 0);
    withhold_active(shared, s);
}
fn withhold_active(shared: &HistoryShared, s: &mut Data) {
    if s.disposition_queued {
        return;
    }
    if let Some(id) = s.active.as_ref().map(|a| a.id) {
        s.ready = None;
        s.current_deadline = None;
        s.disposition_queued = true;
        s.status.withheld += 1;
        push(
            shared,
            s,
            Event::Control(OwnerEventDto::Withheld { input_id: id }),
            0,
        );
    }
}
fn refresh(shared: &HistoryShared, s: &mut Data) {
    if s.finalization_deadline.is_some_and(|d| Instant::now() >= d) {
        // Expiry is observable even while an already accepted ticket must
        // retain ownership until its actual result arrives.
        s.finalization_expired = true;
    }
    if s.status.phase == OwnerPhase::Stopped || s.status.failure.is_some() {
        return;
    }
    let writer = shared.writer.status();
    if writer.closed || writer.failure.is_some() {
        revoke_history(
            shared,
            s,
            OwnerFailure::Admission(
                writer
                    .failure
                    .map_or(AdmissionFailure::Closed, AdmissionFailure::Failed),
            ),
        );
        return;
    }
    let deadline = s
        .current_deadline
        .or_else(|| s.inputs.front().map(|i| i.deadline));
    if deadline.is_some_and(|d| Instant::now() >= d) {
        let stage = s.deadline_stage;
        revoke_history(shared, s, OwnerFailure::Deadline(stage));
    }
}
fn sleep_poll(shared: &HistoryShared) {
    let s = state(shared);
    let _ = shared
        .wake
        .wait_timeout(
            s,
            shared
                .policy
                .receipt_poll_interval
                .duration()
                .expect("policy"),
        )
        .unwrap_or_else(|p| p.into_inner());
}

/// Additional counters describe reserved canonical observation/preparation
/// bytes, not allocator memory or RSS. The original actor bounds wire buffers;
/// the bootstrap capture/runner caps additionally bound one native operation.
#[derive(Debug, Clone)]
pub struct HistoryStatus {
    pub owner: OwnerStatus,
    pub committed_records: u64,
    pub finalization_expired: bool,
    pub ingress_observations: usize,
    pub ingress_observation_bytes: usize,
}
#[derive(Clone)]
pub struct HistoryIngress {
    shared: Arc<HistoryShared>,
}
struct Preparation {
    shared: Arc<HistoryShared>,
    bytes: usize,
    observation_bytes: usize,
    live: bool,
}
impl Drop for Preparation {
    fn drop(&mut self) {
        if self.live {
            let mut s = state(&self.shared);
            release_preparation(&mut s, self.bytes, self.observation_bytes);
            s.status.pending_operations -= 1;
            s.status.pending_bytes -= self.bytes;
            self.shared.wake.notify_all();
        }
    }
}
fn release_preparation(s: &mut Data, bytes: usize, observation_bytes: usize) {
    s.preparing -= 1;
    s.preparing_bytes -= bytes;
    s.preparing_observation_bytes -= observation_bytes;
    s.ingress_observations -= 1;
    s.ingress_observation_bytes -= observation_bytes;
}
struct LimitedJson {
    bytes: Vec<u8>,
    cap: usize,
}
impl Write for LimitedJson {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.cap.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("bounded history JSON capacity"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn bounded_json(value: &impl Serialize, cap: usize) -> Result<String, String> {
    let mut out = LimitedJson {
        bytes: Vec::new(),
        cap,
    };
    serde_json::to_writer(&mut out, value).map_err(|e| e.to_string())?;
    String::from_utf8(out.bytes).map_err(|e| e.to_string())
}
impl HistoryIngress {
    #[cfg(all(test, feature = "causal-journal"))]
    pub(crate) fn pause_next_text_for_test(&self) -> HistoryTextPause {
        let pause = Arc::new(TextPauseState::default());
        let mut slot = self.shared.text_pause.lock().unwrap();
        if slot.is_some() {
            // A bad fixture must not install an orphaned, unreleasable gate or
            // poison the owner's barrier mutex during assertion unwinding.
            drop(slot);
            panic!("one test pause may be installed per owner at a time");
        }
        *slot = Some(pause.clone());
        HistoryTextPause(pause)
    }
    pub fn try_submit(
        &self,
        connection: ConnectionId,
        mut input: CaptureInput,
    ) -> Result<IngressAdmission, Box<RejectedIngress>> {
        let received = Instant::now();
        let bytes = input_bytes(&input);
        // Worst-case escaped JSON plus a second preparation copy. Reservation
        // occurs before copying/encoding and includes concurrent preparations.
        let json_bound = bytes.saturating_mul(6).saturating_add(SMALL);
        let preparation_bytes = json_bound.saturating_mul(2);
        {
            let mut s = state(&self.shared);
            refresh(&self.shared, &mut s);
            let p = &self.shared.policy;
            let h = &self.shared.history;
            let source_ok = match &input {
                CaptureInput::Connected {
                    connection: incoming,
                    ..
                } => *incoming == connection && connection.0 > s.source.0,
                _ if frame_input(&input) => s.source_active && connection == s.source,
                _ => connection == s.source,
            };
            let reason = if shared_wire_http(&s, &input) {
                Some(OwnerFailure::UnsupportedInput)
            } else if let Some(reason) = &s.status.failure {
                Some(reason.clone())
            } else if s.status.closing || s.status.phase == OwnerPhase::Stopped {
                Some(OwnerFailure::Revoked)
            } else if !supported(&input) {
                Some(OwnerFailure::UnsupportedInput)
            } else if bytes > p.max_input_bytes as usize {
                Some(OwnerFailure::InputTooLarge)
            } else if !source_ok {
                Some(OwnerFailure::SourceConnection)
            } else if s.status.pending_operations >= p.max_pending_operations as usize
                || bytes > (p.max_pending_bytes as usize).saturating_sub(s.status.pending_bytes)
            {
                Some(OwnerFailure::IngressFull)
            } else if s.ingress_observations >= h.max_pending_observations as usize - RESERVED_SLOTS
                || preparation_bytes
                    > (h.max_pending_observation_bytes as usize
                        - h.max_record_bytes as usize
                        - 4 * SMALL)
                        .saturating_sub(s.ingress_observation_bytes)
            {
                Some(OwnerFailure::ObservationFull)
            } else {
                None
            };
            if let Some(reason) = reason {
                if reason == OwnerFailure::SourceConnection {
                    s.status.rejected_stale += 1;
                } else if !(reason == OwnerFailure::Revoked
                    && s.status.closing
                    && s.status.failure.is_none())
                {
                    revoke_history(&self.shared, &mut s, reason.clone());
                }
                return Err(Box::new(RejectedIngress {
                    reason,
                    connection,
                    input,
                }));
            }
            s.status.pending_operations += 1;
            s.status.pending_bytes += bytes;
            s.preparing += 1;
            s.preparing_bytes += bytes;
            s.preparing_observation_bytes += preparation_bytes;
            s.ingress_observations += 1;
            s.ingress_observation_bytes += preparation_bytes;
        }
        let mut reservation = Preparation {
            shared: self.shared.clone(),
            bytes,
            observation_bytes: preparation_bytes,
            live: true,
        };
        if let CaptureInput::Http { response, .. } = &mut input {
            response.body = response.body.to_vec().into();
        }
        let encoded = (|| {
            let event = OwnerEventDto::ingress(
                1,
                connection,
                u64::MAX,
                bytes as u64,
                &input,
                &self.shared.identity,
            )
            .map_err(|e| e.to_string())?;
            let OwnerEventDto::Ingress { input, .. } = event else {
                unreachable!()
            };
            bounded_json(&input, json_bound)
        })();
        let mut s = state(&self.shared);
        refresh(&self.shared, &mut s);
        reservation.live = false;
        release_preparation(&mut s, bytes, preparation_bytes);
        let source_ok = match &input {
            CaptureInput::Connected {
                connection: incoming,
                ..
            } => *incoming == connection && connection.0 > s.source.0,
            _ if frame_input(&input) => s.source_active && connection == s.source,
            _ => connection == s.source,
        };
        let deadline = received
            .checked_add(
                self.shared
                    .policy
                    .max_ingress_age
                    .duration()
                    .expect("policy"),
            )
            .expect("one hour bound");
        let deadline_ns = deadline
            .checked_duration_since(self.shared.origin)
            .and_then(|d| elapsed_ns(&self.shared, d));
        let reason = if let Some(reason) = &s.status.failure {
            Some(reason.clone())
        } else if deadline_ns.is_none() {
            Some(OwnerFailure::ClockExhausted)
        } else if s.status.closing {
            Some(OwnerFailure::Revoked)
        } else if !source_ok {
            Some(OwnerFailure::SourceConnection)
        } else if Instant::now() >= deadline {
            Some(OwnerFailure::Deadline(DeadlineStage::Queued))
        } else if s.status.accepted == u64::MAX {
            Some(OwnerFailure::InputOrdinalExhausted)
        } else if encoded.as_ref().is_err()
            || encoded
                .as_ref()
                .is_ok_and(|v| v.len() + SMALL > self.shared.history.max_record_bytes as usize)
        {
            Some(OwnerFailure::Encoding("ingress encoding/bound".into()))
        } else {
            None
        };
        if let Some(reason) = reason {
            s.status.pending_operations -= 1;
            s.status.pending_bytes -= bytes;
            if reason == OwnerFailure::SourceConnection {
                s.status.rejected_stale += 1;
            } else if !(reason == OwnerFailure::Revoked
                && s.status.closing
                && s.status.failure.is_none())
            {
                revoke_history(&self.shared, &mut s, reason.clone());
            }
            return Err(Box::new(RejectedIngress {
                reason,
                connection,
                input,
            }));
        }
        let input_json = encoded.expect("checked encoding");
        let observation_bytes = input_json.len() + SMALL;
        s.ingress_observations += 1;
        s.ingress_observation_bytes += observation_bytes;
        let id = s.status.accepted + 1;
        let deadline_ns = deadline_ns.expect("checked before acceptance");
        if push(
            &self.shared,
            &mut s,
            Event::Ingress {
                id,
                source: connection.0,
                deadline_ns,
                bytes,
                input_json,
            },
            observation_bytes,
        )
        .is_none()
        {
            s.status.pending_operations -= 1;
            s.status.pending_bytes -= bytes;
            s.ingress_observations -= 1;
            s.ingress_observation_bytes -= observation_bytes;
            return Err(Box::new(RejectedIngress {
                reason: OwnerFailure::InputOrdinalExhausted,
                connection,
                input,
            }));
        }
        s.status.accepted = id;
        if matches!(input, CaptureInput::Connected { .. }) || safety_input(&input) {
            s.visibility_fence = id;
            s.snapshot = None;
            s.source_active = matches!(input, CaptureInput::Connected { .. });
            if s.source_active {
                s.source = connection;
            }
            withhold_active(&self.shared, &mut s);
        }
        let (settlement, receiver) = mpsc::channel();
        s.inputs.push_back(QueuedInput {
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
    pub fn revoke(&self) {
        let mut s = state(&self.shared);
        if s.status.phase != OwnerPhase::Stopped {
            revoke_history(&self.shared, &mut s, OwnerFailure::Revoked);
        }
    }
    pub fn status(&self) -> HistoryStatus {
        let mut s = state(&self.shared);
        refresh(&self.shared, &mut s);
        HistoryStatus {
            owner: s.status.clone(),
            committed_records: s.journal_records,
            finalization_expired: s.finalization_expired,
            ingress_observations: s.ingress_observations,
            ingress_observation_bytes: s.ingress_observation_bytes,
        }
    }
}

pub struct HistoryJournalOwner {
    ingress: HistoryIngress,
    worker: Option<JoinHandle<HistoryStatus>>,
}
impl BootstrappedSession {
    pub fn spawn_history_owner(self) -> Result<HistoryJournalOwner, OwnerStartError> {
        let history = self.history_policy.ok_or(OwnerStartError::Profile)?;
        let policy = self.owner_policy.ok_or(OwnerStartError::MissingPolicy)?;
        history
            .validate(&policy)
            .map_err(|_| OwnerStartError::IncompatibleBounds)?;
        let writer = self.handle.status();
        if writer.closed
            || writer.failure.is_some()
            || writer.last_acknowledged.feed_id != self.receipt.published.feed_id
            || writer.last_acknowledged.sequence < self.receipt.last_sequence
        {
            return Err(OwnerStartError::WriterUnavailable);
        }
        let caps = self.handle.config();
        // Worst-case wire framing/route plus base64, independently from native
        // capture and canonical JSON observation budgets.
        let bound = (history.max_record_bytes as usize).div_ceil(3) * 4 + 4096;
        if bound > caps.max_group_bytes || bound > caps.max_pending_bytes {
            return Err(OwnerStartError::IncompatibleBounds);
        }
        let unix_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|d| i64::try_from(d.as_nanos()).ok())
            .ok_or_else(|| io::Error::other("owner Unix clock outside supported range"))?;
        let shared = Arc::new(HistoryShared {
            writer: self.handle,
            identity: self.identity,
            schema_version: if self.wire_policy.is_some() {
                4
            } else if self.native_profile {
                3
            } else {
                2
            },
            policy,
            history,
            origin: Instant::now(),
            #[cfg(test)]
            clock_offset_ns: std::sync::atomic::AtomicU64::new(0),
            #[cfg(all(test, feature = "causal-journal"))]
            text_pause: Mutex::new(None),
            wake: Condvar::new(),
            ticket: Mutex::new(None),
            state: Mutex::new(Data {
                inputs: VecDeque::new(),
                observations: VecDeque::from([Observation {
                    ordinal: 1,
                    observed_ns: 0,
                    ingress_bytes: 0,
                    event: Event::Control(OwnerEventDto::Started { unix_ns }),
                }]),
                next_ordinal: 1,
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
                source: ConnectionId(0),
                source_active: false,
                visibility_fence: 0,
                active: None,
                ready: None,
                snapshot: None,
                current_deadline: None,
                deadline_stage: DeadlineStage::Queued,
                finalization_deadline: None,
                disposition_queued: false,
                cannot_record: false,
                finalization_expired: false,
                ingress_observations: 0,
                ingress_observation_bytes: 0,
                preparing: 0,
                preparing_bytes: 0,
                preparing_observation_bytes: 0,
                journal_records: 0,
                wire: self.wire_policy.map(WireRuntime::new),
            }),
        });
        let worker_shared = shared.clone();
        let worker = thread::Builder::new()
            .name("causal-history-owner".into())
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| {
                    drive_history(self.capture, self.receipt, &worker_shared)
                }));
                if result.is_err() {
                    let mut s = state(&worker_shared);
                    revoke_history(&worker_shared, &mut s, OwnerFailure::WorkerPanicked);
                    s.status.last_settlement = Some(CommitSettlement::Unknown);
                }
                // The panic path retains its accepted ticket in Shared too.
                settle_outstanding(&worker_shared);
                let mut s = state(&worker_shared);
                for input in s.inputs.drain(..) {
                    let _ = input
                        .settlement
                        .send(InputSettlement::Unknown { id: input.id });
                }
                s.observations.clear();
                s.ready = None;
                s.active = None;
                s.snapshot = None;
                s.status.pending_operations = s.preparing;
                s.status.pending_bytes = s.preparing_bytes;
                s.ingress_observations = s.preparing;
                s.ingress_observation_bytes = s.preparing_observation_bytes;
                s.status.phase = OwnerPhase::Stopped;
                worker_shared.wake.notify_all();
                HistoryStatus {
                    owner: s.status.clone(),
                    committed_records: s.journal_records,
                    finalization_expired: s.finalization_expired,
                    ingress_observations: s.ingress_observations,
                    ingress_observation_bytes: s.ingress_observation_bytes,
                }
            })?;
        Ok(HistoryJournalOwner {
            ingress: HistoryIngress { shared },
            worker: Some(worker),
        })
    }
}
impl HistoryJournalOwner {
    pub fn ingress(&self) -> HistoryIngress {
        self.ingress.clone()
    }
    pub fn status(&self) -> HistoryStatus {
        self.ingress.status()
    }
    pub fn committed_snapshot(&self) -> Option<CommittedSnapshot> {
        let mut s = state(&self.ingress.shared);
        refresh(&self.ingress.shared, &mut s);
        if s.snapshot
            .as_ref()
            .is_some_and(|v| Instant::now() >= v.expires_at)
        {
            s.snapshot = None;
        }
        if s.status.failure.is_some() {
            None
        } else {
            s.snapshot.clone()
        }
    }
    pub fn try_next(&self) -> Option<CommittedRelease> {
        let mut s = state(&self.ingress.shared);
        refresh(&self.ingress.shared, &mut s);
        if s.status.failure.is_some() {
            return None;
        }
        let mut ready = s.ready.take()?;
        if Instant::now() >= ready.deadline {
            s.ready = Some(ready);
            revoke_history(
                &self.ingress.shared,
                &mut s,
                OwnerFailure::Deadline(DeadlineStage::Release),
            );
            return None;
        }
        ready.release.observed_at = Instant::now();
        push(
            &self.ingress.shared,
            &mut s,
            Event::Control(OwnerEventDto::HandoffObserved {
                input_id: ready.release.input_id,
                intent_ordinal: ready.intent,
            }),
            0,
        )?;
        s.status.handed_off += 1;
        s.disposition_queued = true;
        s.current_deadline = None;
        Some(ready.release)
    }
    pub fn close_ingress(&self) {
        state(&self.ingress.shared).status.closing = true;
        self.ingress.shared.wake.notify_all();
    }
    pub fn shutdown(mut self) -> HistoryStatus {
        self.ingress.revoke();
        self.worker
            .take()
            .expect("owner worker")
            .join()
            .expect("worker catches panic")
    }
}
impl Drop for HistoryJournalOwner {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            self.ingress.revoke();
            let _ = worker.join();
        }
    }
}

fn settle_outstanding(shared: &HistoryShared) {
    loop {
        let result = {
            let mut t = ticket(shared);
            match t.as_mut() {
                None => return,
                Some(t) => t.try_result(),
            }
        };
        if let Some(result) = result {
            ticket(shared).take();
            state(shared).status.last_settlement = Some(match result {
                Ok(r) => CommitSettlement::Committed(r),
                Err(e) => CommitSettlement::Failed(e),
            });
            return;
        }
        sleep_poll(shared);
    }
}
fn commit(
    shared: &HistoryShared,
    mut tx: PreparedFeedTransaction,
    bootstrap: &CommitReceipt,
    previous: &mut u64,
    native_completion: bool,
) -> Option<CommitReceipt> {
    loop {
        {
            let mut s = state(shared);
            refresh(shared, &mut s);
            // Finalization expiry never cancels complete available evidence.
            // Keep this exact ordered transaction while a healthy
            // writer is merely Full; a predecessor can block a completion too.
            s.status.phase = OwnerPhase::AwaitingAdmission;
        }
        match shared.writer.try_submit(tx) {
            Ok(t) => {
                *ticket(shared) = Some(t);
                if native_completion {
                    state(shared).deadline_stage = DeadlineStage::Admitted;
                }
                break;
            }
            Err(rejected) => {
                tx = rejected.transaction;
                let mut s = state(shared);
                if rejected.reason != AdmissionFailure::Full {
                    revoke_history(shared, &mut s, OwnerFailure::Admission(rejected.reason));
                    return None;
                }
                s.status.admission_retries += 1;
                drop(s);
                sleep_poll(shared);
            }
        }
    }
    let admission = ticket(shared)
        .as_ref()
        .expect("accepted ticket")
        .admission();
    state(shared).status.phase = OwnerPhase::AwaitingCommit;
    let result = loop {
        {
            let mut s = state(shared);
            refresh(shared, &mut s);
        }
        if let Some(result) = ticket(shared)
            .as_mut()
            .expect("owned accepted ticket")
            .try_result()
        {
            break result;
        }
        sleep_poll(shared);
    };
    ticket(shared).take();
    let receipt = match result {
        Ok(receipt) => receipt,
        Err(error) => {
            let mut s = state(shared);
            s.status.last_settlement = Some(CommitSettlement::Failed(error.clone()));
            revoke_history(shared, &mut s, OwnerFailure::Commit(error));
            return None;
        }
    };
    if receipt.admission != admission
        || receipt.first_sequence != receipt.last_sequence
        || receipt.first_sequence <= *previous
        || receipt.published.sequence < receipt.last_sequence
        || receipt.published.feed_id != bootstrap.published.feed_id
    {
        revoke_history(shared, &mut state(shared), OwnerFailure::InvalidReceipt);
        return None;
    }
    *previous = receipt.last_sequence;
    let mut s = state(shared);
    s.journal_records += 1;
    s.status.last_settlement = Some(CommitSettlement::Committed(receipt.clone()));
    Some(receipt)
}
fn retire_native(
    shared: &HistoryShared,
    current: &mut Option<QueuedInput>,
    settlement: InputSettlement,
) {
    let input = current.take().expect("owned input");
    let _ = input.settlement.send(settlement);
    let mut s = state(shared);
    s.status.pending_operations -= 1;
    s.status.pending_bytes -= input.bytes;
}

fn drive_history(mut capture: CaptureSession, bootstrap: CommitReceipt, shared: &HistoryShared) {
    let mut previous = bootstrap.last_sequence;
    let mut generation = 0;
    let mut native_ordinal = 0;
    let mut physical_source = ConnectionId(0);
    let mut physical_active = false;
    let mut current: Option<QueuedInput> = None;
    loop {
        let observation = {
            let mut s = state(shared);
            refresh(shared, &mut s);
            if s.cannot_record {
                return;
            }
            s.observations.pop_front()
        };
        if let Some(observation) = observation {
            #[cfg(all(test, feature = "causal-journal"))]
            pause_text_worker(shared, &observation);
            let Observation {
                ordinal,
                observed_ns,
                ingress_bytes,
                event,
            } = observation;
            let fallback = match &event {
                Event::Completion { id, permit, .. } => Some((*id, *permit)),
                _ => None,
            };
            let encoded = (|| -> Result<_, String> {
                let event = match event {
                    Event::Control(event) => event,
                    Event::Ingress {
                        id,
                        source,
                        deadline_ns,
                        bytes,
                        input_json,
                    } => OwnerEventDto::Ingress {
                        input_id: id,
                        connection: source,
                        deadline_ns,
                        owned_bytes_bound: bytes as u64,
                        input: serde_json::from_str(&input_json).map_err(|e| e.to_string())?,
                    },
                    Event::Completion {
                        id,
                        permit,
                        operation,
                    } => {
                        let operation = Box::new(
                            if shared.schema_version >= 3 {
                                OperationDto::from_observed_capture(&operation, &shared.identity)
                            } else {
                                OperationDto::from_capture(&operation, &shared.identity)
                            }
                            .map_err(|e| e.to_string())?,
                        );
                        OwnerEventDto::ExecutionCompleted {
                            input_id: id,
                            permit_ordinal: permit,
                            operation,
                        }
                    }
                };
                let record_generation = match &event {
                    OwnerEventDto::ExecutionCompleted { operation, .. } => {
                        operation.route.generation
                    }
                    _ => generation,
                };
                let dto = OwnerHistoryDto {
                    schema_version: shared.schema_version,
                    route: CausalRoute {
                        venue: shared.identity.venue,
                        session: shared.identity.session,
                        bootstrap_id: shared.identity.bootstrap_id.clone(),
                        ordinal,
                        generation: record_generation,
                    },
                    observed_ns,
                    event,
                };
                let envelope = encode_owner_history(
                    &dto,
                    &shared.identity,
                    shared.history.max_record_bytes as usize,
                )
                .map_err(|e| e.to_string())?;
                let tx = PreparedFeedTransaction::new(vec![FeedRecord::Causal(envelope)])
                    .map_err(|e| e.to_string())?;
                Ok((dto, tx))
            })();
            let (dto, tx) = match encoded {
                Ok(value) => value,
                Err(error) => {
                    // No placeholder/ordinal hole or retry execution. Preserve a
                    // typed indeterminate record at this observation position.
                    let Some((input_id, permit)) = fallback else {
                        revoke_history(shared, &mut state(shared), OwnerFailure::Encoding(error));
                        return;
                    };
                    let dto = OwnerHistoryDto {
                        schema_version: shared.schema_version,
                        route: CausalRoute {
                            venue: shared.identity.venue,
                            session: shared.identity.session,
                            bootstrap_id: shared.identity.bootstrap_id.clone(),
                            ordinal,
                            generation,
                        },
                        observed_ns,
                        event: OwnerEventDto::ExecutionIndeterminate {
                            input_id,
                            permit_ordinal: permit,
                            cause: OwnerIndeterminateCause::Encoding,
                        },
                    };
                    let Ok(envelope) = encode_owner_history(
                        &dto,
                        &shared.identity,
                        shared.history.max_record_bytes as usize,
                    ) else {
                        return;
                    };
                    let Ok(tx) = PreparedFeedTransaction::new(vec![FeedRecord::Causal(envelope)])
                    else {
                        return;
                    };
                    revoke_history(shared, &mut state(shared), OwnerFailure::Encoding(error));
                    (dto, tx)
                }
            };
            let Some(receipt) = commit(
                shared,
                tx,
                &bootstrap,
                &mut previous,
                matches!(dto.event, OwnerEventDto::ExecutionCompleted { .. }),
            ) else {
                return;
            };
            if ingress_bytes != 0 {
                let mut s = state(shared);
                s.ingress_observations -= 1;
                s.ingress_observation_bytes -= ingress_bytes;
            }
            match dto.event {
                OwnerEventDto::Wire(event) => {
                    wire::committed(shared, &mut state(shared), &event, receipt);
                }
                OwnerEventDto::ExecutionPermitted {
                    input_id,
                    generation: intended_generation,
                    native_ordinal: intended_ordinal,
                } => {
                    let execute = {
                        let mut s = state(shared);
                        refresh(shared, &mut s);
                        if s.status.failure.is_some() {
                            push(
                                shared,
                                &mut s,
                                Event::Control(OwnerEventDto::ExecutionNotInvoked {
                                    input_id,
                                    permit_ordinal: ordinal,
                                    cause: OwnerNotExecutedCause::Terminal,
                                }),
                                0,
                            );
                            false
                        } else {
                            s.status.phase = OwnerPhase::Executing;
                            s.deadline_stage = DeadlineStage::Executing;
                            s.status.executed += 1;
                            true
                        }
                    };
                    if execute {
                        let input = current.as_mut().expect("permitted input");
                        let result = capture.execute_turn(
                            std::mem::replace(&mut input.input, CaptureInput::Stop),
                            CaptureDrainPolicy::NullDispatchTakeOutbound,
                        );
                        let mut s = state(shared);
                        match result {
                            Ok(operation) => {
                                s.deadline_stage = DeadlineStage::Executed;
                                debug_assert_eq!(operation.ordinal, intended_ordinal);
                                debug_assert_eq!(operation.generation, intended_generation);
                                let unsafe_state =
                                    !matches!(&operation.outcome, CaptureOutcome::Returned(Ok(())))
                                        || (operation.after.stop_requested
                                            && !operation.before.stop_requested)
                                        || (operation.after.reconnect_requested
                                            && !operation.before.reconnect_requested)
                                        || operation.after.valid_books
                                            < operation.before.valid_books
                                        || (operation.before.lifecycle
                                            == marketfeed_engine::SessionLifecycle::Live
                                            && operation.after.lifecycle
                                                != marketfeed_engine::SessionLifecycle::Live);
                                if unsafe_state && input_id >= s.visibility_fence {
                                    s.visibility_fence = input_id;
                                    s.snapshot = None;
                                    if operation.after.reconnect_requested
                                        || operation.after.stop_requested
                                    {
                                        s.source_active = false;
                                    }
                                }
                                let reconnect = operation.after.reconnect_requested
                                    || operation.after.stop_requested;
                                if matches!(
                                    operation.original_input,
                                    CaptureInput::Connected { .. }
                                ) {
                                    physical_source = input.connection;
                                    physical_active = true;
                                } else if safety_input(&operation.original_input) {
                                    physical_active = false;
                                }
                                if reconnect {
                                    physical_active = false;
                                }
                                let failure = if operation.completeness
                                    != CaptureCompleteness::Complete
                                {
                                    Some(OwnerFailure::Incomplete(operation.completeness.clone()))
                                } else if let CaptureOutcome::Returned(Err(error)) =
                                    &operation.outcome
                                {
                                    Some(OwnerFailure::Native(error.into()))
                                } else if operation.after.stop_requested {
                                    Some(OwnerFailure::NativeStopped)
                                } else {
                                    None
                                };
                                let uncertainty =
                                    if matches!(operation.outcome, CaptureOutcome::Panicked) {
                                        OwnerIndeterminateCause::Panic
                                    } else {
                                        OwnerIndeterminateCause::Incomplete
                                    };
                                if operation.completeness == CaptureCompleteness::Complete {
                                    push(
                                        shared,
                                        &mut s,
                                        Event::Completion {
                                            id: input_id,
                                            permit: ordinal,
                                            operation: Box::new(operation),
                                        },
                                        0,
                                    );
                                } else {
                                    push(
                                        shared,
                                        &mut s,
                                        Event::Control(OwnerEventDto::ExecutionIndeterminate {
                                            input_id,
                                            permit_ordinal: ordinal,
                                            cause: uncertainty,
                                        }),
                                        0,
                                    );
                                }
                                if let Some(reason) = failure {
                                    revoke_history(shared, &mut s, reason);
                                }
                            }
                            Err(error) => {
                                s.status.executed -= 1;
                                let cause = if s.status.failure.is_some() {
                                    OwnerNotExecutedCause::Terminal
                                } else {
                                    OwnerNotExecutedCause::Preflight
                                };
                                push(
                                    shared,
                                    &mut s,
                                    Event::Control(OwnerEventDto::ExecutionNotInvoked {
                                        input_id,
                                        permit_ordinal: ordinal,
                                        cause,
                                    }),
                                    0,
                                );
                                revoke_history(
                                    shared,
                                    &mut s,
                                    OwnerFailure::Execution(error.to_string()),
                                );
                            }
                        }
                    }
                }
                OwnerEventDto::ExecutionCompleted {
                    input_id,
                    operation,
                    ..
                } => {
                    native_ordinal = operation.route.ordinal;
                    generation = operation.route.generation;
                    let deadline = current.as_ref().expect("completed input").deadline;
                    retire_native(
                        shared,
                        &mut current,
                        InputSettlement::Committed {
                            id: input_id,
                            receipt: receipt.clone(),
                        },
                    );
                    let mut s = state(shared);
                    s.status.durable += 1;
                    if let Err(error) = &operation.outcome {
                        revoke_history(shared, &mut s, OwnerFailure::Native(error.clone()));
                    }
                    if operation.after.stop_requested {
                        revoke_history(shared, &mut s, OwnerFailure::NativeStopped);
                    }
                    if operation.after.reconnect_requested && input_id >= s.visibility_fence {
                        s.source_active = false;
                    }
                    let eligible = physical_active
                        && !operation.after.reconnect_requested
                        && !operation.after.stop_requested
                        && operation.outcome.is_ok();
                    s.disposition_queued = false;
                    s.active = Some(Active {
                        id: input_id,
                        operation: Arc::from(operation),
                        receipt,
                        deadline,
                        completion_ordinal: ordinal,
                        outputs_eligible: eligible,
                    });
                    refresh(shared, &mut s);
                    if let Some(wire) = s.wire.as_ref() {
                        let active = s.active.as_ref().expect("just installed");
                        if wire_candidates(
                            &active.operation,
                            input_id,
                            ordinal,
                            ordinal,
                            wire.ledger.policy(),
                        )
                        .is_err()
                        {
                            revoke_history(shared, &mut s, OwnerFailure::UnsupportedInput);
                        }
                    }
                    if s.status.failure.is_some() || input_id < s.visibility_fence {
                        withhold_active(shared, &mut s);
                    } else {
                        push(
                            shared,
                            &mut s,
                            Event::Control(OwnerEventDto::ReleaseIntent {
                                input_id,
                                operation_ordinal: ordinal,
                                outputs_eligible: eligible,
                            }),
                            0,
                        );
                    }
                }
                OwnerEventDto::ReleaseIntent {
                    input_id,
                    outputs_eligible,
                    ..
                } => {
                    let mut s = state(shared);
                    refresh(shared, &mut s);
                    if s.disposition_queued {
                        continue;
                    }
                    if s.status.failure.is_some() || input_id < s.visibility_fence {
                        withhold_active(shared, &mut s);
                    } else {
                        let a = s.active.as_ref().expect("committed operation");
                        let release = CommittedRelease {
                            input_id,
                            operation: a.operation.clone(),
                            receipt: a.receipt.clone(),
                            observed_at: Instant::now(),
                            outputs_eligible,
                        };
                        let deadline = a.deadline;
                        let snapshot = outputs_eligible.then(|| CommittedSnapshot {
                            route: a.operation.route.clone(),
                            receipt: a.receipt.clone(),
                            state: a.operation.after.clone(),
                            expires_at: a.deadline,
                        });
                        s.snapshot = snapshot;
                        s.ready = Some(Ready {
                            intent: ordinal,
                            release,
                            deadline,
                        });
                        s.status.phase = OwnerPhase::ReleasePending;
                        s.deadline_stage = DeadlineStage::Release;
                    }
                }
                OwnerEventDto::HandoffObserved { input_id, .. } => {
                    let mut s = state(shared);
                    if let Some(active) = s.active.take() {
                        if let Some(wire) = s.wire.as_mut() {
                            let candidates = if active.outputs_eligible {
                                wire_candidates(
                                    &active.operation,
                                    input_id,
                                    active.completion_ordinal,
                                    ordinal,
                                    wire.ledger.policy(),
                                )
                            } else {
                                Ok(Vec::new())
                            };
                            let deadline = elapsed_ns(
                                shared,
                                active.deadline.saturating_duration_since(shared.origin),
                            );
                            match (candidates, deadline) {
                                (Ok(candidates), Some(deadline)) => {
                                    wire.deadline = active.deadline;
                                    if wire.ledger.install(candidates, deadline).is_err() {
                                        revoke_history(
                                            shared,
                                            &mut s,
                                            OwnerFailure::InvalidReceipt,
                                        );
                                    }
                                }
                                _ => revoke_history(shared, &mut s, OwnerFailure::InvalidReceipt),
                            }
                        }
                    }
                    s.current_deadline = None;
                    s.disposition_queued = false;
                }
                OwnerEventDto::Withheld { .. } => {
                    let mut s = state(shared);
                    s.active = None;
                    s.current_deadline = None;
                    s.disposition_queued = false;
                }
                OwnerEventDto::ExecutionNotInvoked {
                    input_id, cause, ..
                }
                | OwnerEventDto::NotExecuted { input_id, cause } => {
                    let reason = match cause {
                        OwnerNotExecutedCause::SourceConnection => OwnerFailure::SourceConnection,
                        OwnerNotExecutedCause::Deadline => {
                            OwnerFailure::Deadline(DeadlineStage::Queued)
                        }
                        _ => state(shared)
                            .status
                            .failure
                            .clone()
                            .unwrap_or(OwnerFailure::Revoked),
                    };
                    retire_native(
                        shared,
                        &mut current,
                        InputSettlement::NotExecutedCommitted {
                            id: input_id,
                            reason,
                            receipt,
                        },
                    );
                    let mut s = state(shared);
                    s.status.abandoned += 1;
                    s.current_deadline = None;
                }
                OwnerEventDto::ExecutionIndeterminate { input_id, .. } => {
                    retire_native(
                        shared,
                        &mut current,
                        InputSettlement::Unknown { id: input_id },
                    );
                    state(shared).current_deadline = None;
                }
                _ => {}
            }
            continue;
        }
        let mut s = state(shared);
        refresh(shared, &mut s);
        if s.active.is_some() {
            drop(s);
            sleep_poll(shared);
            continue;
        }
        if current.is_some() {
            drop(s);
            sleep_poll(shared);
            continue;
        }
        if wire::tick(shared, &mut s) {
            drop(s);
            sleep_poll(shared);
            continue;
        }
        if let Some(input) = s.inputs.pop_front() {
            let id = input.id;
            let source_matches = match &input.input {
                CaptureInput::Connected { connection, .. } => {
                    *connection == input.connection && connection.0 > physical_source.0
                }
                _ if frame_input(&input.input) => {
                    physical_active && input.connection == physical_source
                }
                _ => input.connection == physical_source,
            };
            s.current_deadline = Some(input.deadline);
            s.deadline_stage = DeadlineStage::Queued;
            let event = if s.status.failure.is_some() {
                OwnerEventDto::NotExecuted {
                    input_id: id,
                    cause: OwnerNotExecutedCause::Terminal,
                }
            } else if !source_matches {
                s.status.stale_accepted += 1;
                OwnerEventDto::NotExecuted {
                    input_id: id,
                    cause: OwnerNotExecutedCause::SourceConnection,
                }
            } else {
                let Some(next_native) = native_ordinal.checked_add(1) else {
                    revoke_history(shared, &mut s, OwnerFailure::InputOrdinalExhausted);
                    s.inputs.push_front(input);
                    continue;
                };
                let Some(next_generation) = generation.checked_add(u64::from(matches!(
                    input.input,
                    CaptureInput::Connected { .. }
                ))) else {
                    revoke_history(shared, &mut s, OwnerFailure::InputOrdinalExhausted);
                    s.inputs.push_front(input);
                    continue;
                };
                OwnerEventDto::ExecutionPermitted {
                    input_id: id,
                    native_ordinal: next_native,
                    generation: next_generation,
                }
            };
            current = Some(input);
            push(shared, &mut s, Event::Control(event), 0);
        } else if s.status.closing {
            return;
        } else {
            s.status.phase = OwnerPhase::Idle;
            drop(s);
            sleep_poll(shared);
        }
    }
}

#[cfg(all(test, feature = "causal-journal"))]
mod tests;
