//! In-memory execution/capture seam for the actual runner. This is not a live
//! journal profile: it has no external I/O/readiness handles, durable ACK or
//! release API. A future journaled owner must reject incomplete captures and
//! add negotiated storage, commit gating, drains and actual-engine replay.
use super::*;
use marketfeed_adapter_api::AdapterError;
use std::io::{self, Write};

const MAX_CAPTURE_BYTES: usize = 64 * 1024 * 1024;
const MAX_CAPTURE_ENTRIES: usize = 65_536;
const OUTCOME_RESERVE: usize = 4096;

#[cfg(test)]
#[path = "operation_native_tests.rs"]
mod native_tests;
#[cfg(test)]
#[path = "operation_tests.rs"]
mod tests;

/// Public-evidence sessions only. Input configuration, URLs, headers and bodies
/// must be public reproduction material. No generic credential-redaction policy
/// is invented here. Explicit sensitive actions poison capture without copying
/// their payload, and the owner cannot subsequently expose a queued secret.
#[derive(Debug, Clone, Copy)]
pub struct CaptureConfig {
    pub venue: VenueId,
    pub session: SessionId,
    pub dispatch_capacity: usize,
    pub overflow: OverflowPolicy,
    /// Conservative logical retention budget for this trace, including its
    /// input execution copy. This is not a peak-allocation/RSS limit: Vec spare
    /// capacity and the actual machine/runner's own working memory are separate.
    pub max_bytes: usize,
    pub max_entries: usize,
}

impl CaptureConfig {
    /// Validate without creating a factory or machine. Bootstrap admission must
    /// reject impossible bounds before executing any adapter code.
    pub fn validate(&self) -> Result<(), EngineError> {
        if self.venue.0 == 0
            || self.session.0 == 0
            || self.dispatch_capacity == 0
            || self.dispatch_capacity > MAX_CAPTURE_ENTRIES
            || self.max_bytes < OUTCOME_RESERVE * 2
            || self.max_bytes > MAX_CAPTURE_BYTES
            || self.max_entries == 0
            || self.max_entries > MAX_CAPTURE_ENTRIES
        {
            return Err(EngineError::Internal(
                "invalid capture limits or identity".into(),
            ));
        }
        Ok(())
    }

    /// Exact effective policy of the private capture runner, including fixed
    /// limits otherwise hidden behind SessionRunnerConfig defaults.
    pub fn runtime_policy(&self) -> CaptureRuntimePolicy {
        CaptureRuntimePolicy {
            initial_connection: SessionRunnerConfig::default().connection.0,
            action_buffer_capacity: self
                .dispatch_capacity
                .saturating_mul(4)
                .max(DEFAULT_ACTION_BUFFER_CAPACITY) as u64,
            max_timers: MAX_SESSION_TIMERS as u64,
            max_timer_fires_per_poll: MAX_TIMER_FIRES_PER_POLL as u64,
            max_pending_writes: MAX_PENDING_WRITES as u64,
            max_pending_http: MAX_PENDING_HTTP as u64,
            record: false,
            mirror_capacity: 0,
            external_handles: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureRuntimePolicy {
    pub initial_connection: u64,
    pub action_buffer_capacity: u64,
    pub max_timers: u64,
    pub max_timer_fires_per_poll: u64,
    pub max_pending_writes: u64,
    pub max_pending_http: u64,
    pub record: bool,
    pub mirror_capacity: u64,
    pub external_handles: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaptureInput {
    Connected {
        connection: ConnectionId,
        now: TimestampNs,
    },
    Disconnected {
        reason: DisconnectReason,
        now: TimestampNs,
    },
    TransportLost {
        reason: DisconnectReason,
        now: TimestampNs,
    },
    Text {
        payload: Vec<u8>,
        received: FrameStamp,
    },
    Binary {
        payload: Vec<u8>,
        received: FrameStamp,
    },
    Ping {
        payload: Vec<u8>,
        received: FrameStamp,
    },
    Pong {
        payload: Vec<u8>,
        received: FrameStamp,
    },
    Close {
        payload: Vec<u8>,
        received: FrameStamp,
    },
    Http {
        request_id: u64,
        response: HttpResponse,
        received: FrameStamp,
    },
    /// Pure observation of previously performed sink calls, in actual native
    /// forwarding order. A future live owner must commit an intent before I/O.
    ForwardDispatch {
        results: Vec<Result<PushOutcome, SinkError>>,
    },
    NullDispatch,
    TakeWrites,
    TakeHttp,
    TimerPoll {
        now: TimestampNs,
    },
    Control {
        command: SessionCommand,
        now: TimestampNs,
    },
    System(SystemEvent),
    Degraded {
        message: String,
        now: TimestampNs,
    },
    Live {
        now: TimestampNs,
    },
    Catalog {
        catalog: CatalogView,
        now: TimestampNs,
    },
    Stop,
    Lifecycle(SessionLifecycle),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvocationKind {
    Connected,
    Disconnected(DisconnectReason),
    Text(FrameStamp),
    Binary(FrameStamp),
    Pong(FrameStamp),
    Http {
        request_id: u64,
        received: FrameStamp,
    },
    Timer {
        timer_id: u64,
        now: TimestampNs,
    },
    Control(SessionCommand),
}
impl InvocationKind {
    pub(super) fn from_input(input: &SessionInput<'_>) -> Self {
        match input {
            SessionInput::Connected { .. } => Self::Connected,
            SessionInput::Disconnected { reason, .. } => Self::Disconnected(reason.clone()),
            SessionInput::TextFrame { received, .. } => Self::Text(*received),
            SessionInput::BinaryFrame { received, .. } => Self::Binary(*received),
            SessionInput::Pong { received, .. } => Self::Pong(*received),
            SessionInput::HttpResponse {
                request_id,
                received,
                ..
            } => Self::Http {
                request_id: *request_id,
                received: *received,
            },
            SessionInput::Timer { timer_id, now } => Self::Timer {
                timer_id: *timer_id,
                now: *now,
            },
            SessionInput::Control { command } => Self::Control((*command).clone()),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallbackOutcome {
    Returned(Result<(), AdapterError>),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionDisposition {
    Buffered,
    DiscardedAfterCallbackError,
    Started,
    Completed(Result<(), EngineError>),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BufferedAction {
    pub action: SessionAction,
    pub disposition: ActionDisposition,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaptureQueueLane {
    Market,
    System,
    Write,
    Http,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaptureQueueItem {
    Market(EventBatch),
    ObservedMarket(ObservedEventBatch),
    System(SystemEvent),
    Write(OutboundFrame),
    Http(HttpRequestSpec),
}
#[derive(Clone, Copy)]
pub(super) enum QueueItemRef<'a> {
    Market(&'a EventBatch),
    ObservedMarket(&'a ObservedEventBatch),
    System(&'a SystemEvent),
    Write(&'a OutboundFrame),
    Http(&'a HttpRequestSpec),
}
impl<'a> QueueItemRef<'a> {
    pub(super) fn market(batch: &'a MarketBatch) -> Self {
        match batch {
            MarketBatch::Canonical(batch) => Self::Market(batch),
            MarketBatch::Observed(batch) => Self::ObservedMarket(batch),
        }
    }
    fn cost(self) -> usize {
        match self {
            Self::Market(b) => batch_cost(b),
            Self::ObservedMarket(b) => observed_batch_cost(b),
            Self::System(e) => system_cost(e),
            Self::Write(w) => w.payload.len().saturating_add(256),
            Self::Http(h) => http_cost(h),
        }
    }
    fn owned(self) -> CaptureQueueItem {
        match self {
            Self::Market(b) => CaptureQueueItem::Market(b.clone()),
            Self::ObservedMarket(b) => CaptureQueueItem::ObservedMarket(b.clone()),
            Self::System(e) => CaptureQueueItem::System(e.clone()),
            Self::Write(w) => CaptureQueueItem::Write(w.clone()),
            Self::Http(h) => CaptureQueueItem::Http(h.clone()),
        }
    }
}
/// An origin is local to one operation. Invocation ordinals follow actual
/// callback order (including repeated timer firings); action indices refer to
/// the retained buffer in that invocation. Runner effects are outside callbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureOrigin {
    Runner,
    Invocation { ordinal: u64 },
    Action { invocation: u64, index: usize },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureTraceEntry {
    pub origin: CaptureOrigin,
    pub effect: CaptureEntry,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaptureEntry {
    /// Exact FeedRecord JSON before any queue decision; never called admission.
    ProducedRecord(Vec<u8>),
    Invocation {
        kind: InvocationKind,
        retained_actions: usize,
        dropped_before: u64,
        dropped_after: u64,
        actions: Vec<BufferedAction>,
        outcome: CallbackOutcome,
    },
    QueueAttempt {
        lane: CaptureQueueLane,
        incoming: CaptureQueueItem,
        evicted_front: Option<CaptureQueueItem>,
        result: Option<Result<PushOutcome, EngineError>>,
    },
    QueueDrain {
        lane: CaptureQueueLane,
        items: Vec<CaptureQueueItem>,
        completed: bool,
    },
    SinkAttempt {
        lane: CaptureQueueLane,
        incoming: CaptureQueueItem,
        result: Option<Result<PushOutcome, SinkError>>,
    },
    ActionBufferDropsConsumed(u64),
    SuppressedDiagnostic(EngineError),
    SubscriptionPrepared(Result<SubscriptionWireAction, AdapterError>),
    SubscriptionReservation(Result<(), DispatchError>),
    SubscriptionCommitted,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaptureFault {
    Bytes,
    Entries,
    SensitiveAction,
    Panicked,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaptureCompleteness {
    Complete,
    Incomplete(CaptureFault),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaptureOutcome {
    Returned(Result<(), EngineError>),
    /// The native operation failed, but its details could not be retained
    /// within the capture budget. This always accompanies incomplete capture.
    ReturnedErrorDetailsOmitted(CaptureErrorClass),
    Panicked,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureErrorClass {
    Adapter,
    Dispatch,
    Recording,
    Transport,
    Stopped,
    SessionNotFound,
    Internal,
}
impl CaptureErrorClass {
    fn of(error: &EngineError) -> Self {
        match error {
            EngineError::Adapter(_) => Self::Adapter,
            EngineError::Dispatch(_) => Self::Dispatch,
            EngineError::Recording(_) => Self::Recording,
            EngineError::Transport(_) => Self::Transport,
            EngineError::Stopped => Self::Stopped,
            EngineError::SessionNotFound => Self::SessionNotFound,
            EngineError::Internal(_) => Self::Internal,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedState {
    pub lifecycle: SessionLifecycle,
    pub connection: ConnectionId,
    pub frame_seq: u64,
    pub stop_requested: bool,
    pub reconnect_requested: bool,
    pub timers: Vec<(u64, TimestampNs)>,
    pub pending_market: usize,
    pub pending_system: usize,
    pub pending_writes: usize,
    pub pending_http: usize,
    pub valid_books: u64,
    /// Absent on incomplete capture; counts/flags above still show partial state.
    pub queues: Option<Vec<(CaptureQueueLane, Vec<CaptureQueueItem>)>>,
}
/// Deterministic drains performed in the SAME captured turn after a successful
/// entry. Removed values remain capture evidence, not permission for external I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureDrainPolicy {
    Retain,
    NullDispatchTakeOutbound,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedOperation {
    /// Owning runner identity, independent of adapter-supplied batch fields.
    pub venue: VenueId,
    pub session: SessionId,
    pub ordinal: u64,
    pub generation: u64,
    pub original_input: CaptureInput,
    pub drain_policy: CaptureDrainPolicy,
    pub before: CapturedState,
    pub entries: Vec<CaptureTraceEntry>,
    pub outcome: CaptureOutcome,
    pub after: CapturedState,
    pub completeness: CaptureCompleteness,
    pub observed_entries: u64,
    /// Conservative logical accounting, subject to CaptureConfig::max_bytes;
    /// not an allocator-capacity, peak-memory or process-RSS measurement.
    pub retained_bytes_bound: usize,
}

/// Owns the real, persistent machine across operations/reconnects. There is no
/// runner/dispatcher/metrics/stop-atomic escape. This executor observes internal
/// effects; nothing here sends queued wire/HTTP requests or publishes dispatch.
pub struct CaptureSession {
    runner: SessionRunner,
    config: CaptureConfig,
    ordinal: u64,
    generation: u64,
    poisoned: bool,
}
impl CaptureSession {
    pub fn new(
        machine: Box<dyn SessionMachine>,
        config: CaptureConfig,
    ) -> Result<Self, EngineError> {
        config.validate()?;
        let runner = SessionRunner::new(
            machine,
            SessionRunnerConfig {
                venue: config.venue,
                session: config.session,
                dispatch_capacity: config.dispatch_capacity,
                overflow: config.overflow,
                record: false,
                mirror_capacity: 0,
                ..Default::default()
            },
        )?;
        Ok(Self {
            runner,
            config,
            ordinal: 0,
            generation: 0,
            poisoned: false,
        })
    }
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Executes one complete typed entry through the actual runner. Rejected
    /// preflight inputs leave the machine untouched. Incomplete capture is
    /// terminal for this owner and is never a prepared durable transaction.
    pub fn execute(&mut self, input: CaptureInput) -> Result<CapturedOperation, EngineError> {
        self.execute_turn(input, CaptureDrainPolicy::Retain)
    }

    pub fn execute_turn(
        &mut self,
        input: CaptureInput,
        drain_policy: CaptureDrainPolicy,
    ) -> Result<CapturedOperation, EngineError> {
        if self.poisoned {
            return Err(EngineError::Stopped);
        }
        if self.runner.operation_capture.is_some() || self.runner.feed_transaction.is_some() {
            return Err(EngineError::Internal("nested operation capture".into()));
        }
        if matches!(
            input,
            CaptureInput::Connected {
                connection: ConnectionId(0),
                ..
            }
        ) {
            return Err(EngineError::Internal(
                "capture connection identity must be nonzero".into(),
            ));
        }
        if let CaptureInput::ForwardDispatch { results } = &input {
            let pending = self.runner.dispatch_pending();
            let available = pending.0 + pending.1;
            let needed = results
                .iter()
                .position(Result::is_err)
                .map_or(available, |i| i + 1);
            if results.len() != needed || needed > available {
                return Err(EngineError::Internal(
                    "sink completion count/order does not match pending prefix".into(),
                ));
            }
        }
        let mut capture = ActiveCapture::new(self.config);
        let cost = input_cost(&input);
        // Original input plus mutable execution copy, and bounded owner state.
        if !capture.charge(cost.saturating_mul(2)) {
            return Err(EngineError::Internal("capture input byte budget".into()));
        }
        let before = snapshot(&self.runner, &mut capture);
        if capture.fault.is_some() {
            return Err(EngineError::Internal(
                "capture initial state byte budget".into(),
            ));
        }
        let ordinal = self
            .ordinal
            .checked_add(1)
            .ok_or_else(|| EngineError::Internal("capture ordinal exhausted".into()))?;
        let generation = if matches!(input, CaptureInput::Connected { .. }) {
            self.generation
                .checked_add(1)
                .ok_or_else(|| EngineError::Internal("capture generation exhausted".into()))?
        } else {
            self.generation
        };
        self.ordinal = ordinal;
        self.generation = generation;
        self.runner.operation_capture = Some(capture);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let result = execute(&mut self.runner, &input);
            if result.is_ok() && drain_policy == CaptureDrainPolicy::NullDispatchTakeOutbound {
                // Execute the actual take/drain methods; no synthetic queue clear
                // and no external I/O. They now record exact removed items.
                self.runner.drain_dispatch();
                self.runner.take_pending_writes();
                self.runner.take_pending_http();
            }
            result
        }));
        let mut capture = self
            .runner
            .operation_capture
            .take()
            .expect("outer capture remains owned");
        let outcome = match result {
            Ok(Err(error)) if !capture.charge(error_cost(&error)) => {
                CaptureOutcome::ReturnedErrorDetailsOmitted(CaptureErrorClass::of(&error))
            }
            Ok(result) => CaptureOutcome::Returned(result),
            Err(_) => {
                capture.fail(CaptureFault::Panicked);
                CaptureOutcome::Panicked
            }
        };
        let after = snapshot(&self.runner, &mut capture);
        self.poisoned = capture.fault.is_some();
        let completeness = capture.fault.clone().map_or(
            CaptureCompleteness::Complete,
            CaptureCompleteness::Incomplete,
        );
        Ok(CapturedOperation {
            venue: self.config.venue,
            session: self.config.session,
            ordinal,
            generation,
            original_input: input,
            drain_policy,
            before,
            entries: capture.entries,
            outcome,
            after,
            completeness,
            observed_entries: capture.observed_entries,
            retained_bytes_bound: capture.used + OUTCOME_RESERVE,
        })
    }
}

fn execute(runner: &mut SessionRunner, input: &CaptureInput) -> Result<(), EngineError> {
    match input {
        CaptureInput::Connected { connection, now } => {
            runner.on_connected_identity(*now, Some(*connection))
        }
        CaptureInput::Disconnected { reason, now } => runner.on_disconnected(reason.clone(), *now),
        CaptureInput::TransportLost { reason, now } => {
            runner.on_transport_lost(reason.clone(), *now)
        }
        CaptureInput::Text { payload, received } => {
            runner.on_text_frame(&mut payload.clone(), *received)
        }
        CaptureInput::Binary { payload, received } => {
            runner.on_binary_frame(&mut payload.clone(), *received)
        }
        CaptureInput::Ping { payload, received } => runner.on_ping_frame(payload, *received),
        CaptureInput::Pong { payload, received } => runner.on_pong_frame(payload, *received),
        CaptureInput::Close { payload, received } => runner.on_close_frame(payload, *received),
        CaptureInput::Http {
            request_id,
            response,
            received,
        } => runner.on_http_response(*request_id, response, *received),
        CaptureInput::ForwardDispatch { results } => {
            let mut sink = ScriptedSink { results, next: 0 };
            let result = runner.consume_dispatch(Some(&mut sink));
            assert_eq!(sink.next, results.len(), "prevalidated native sink prefix");
            result
        }
        CaptureInput::NullDispatch => {
            runner.drain_dispatch();
            Ok(())
        }
        CaptureInput::TakeWrites => {
            runner.take_pending_writes();
            Ok(())
        }
        CaptureInput::TakeHttp => {
            runner.take_pending_http();
            Ok(())
        }
        CaptureInput::TimerPoll { now } => runner.poll_timers(*now),
        CaptureInput::Control { command, now } => runner.deliver_control(command.clone(), *now),
        CaptureInput::System(event) => runner.push_system(event.clone()),
        CaptureInput::Degraded { message, now } => runner.mark_degraded_with_status(message, *now),
        CaptureInput::Live { now } => runner.mark_live_with_status(*now),
        CaptureInput::Catalog { catalog, now } => {
            runner.publish_catalog_refresh(catalog.clone(), *now)
        }
        CaptureInput::Stop => {
            runner.request_stop();
            Ok(())
        }
        CaptureInput::Lifecycle(state) => {
            runner.lifecycle = *state;
            Ok(())
        }
    }
}

pub(super) struct ActiveCapture {
    entries: Vec<CaptureTraceEntry>,
    used: usize,
    limit: usize,
    max_entries: usize,
    observed_entries: u64,
    fault: Option<CaptureFault>,
    invocation_index: Option<usize>,
    invocation_ordinal: u64,
    origin: CaptureOrigin,
}
impl ActiveCapture {
    pub(super) fn invocation_kind(&mut self, input: &SessionInput<'_>) -> Option<InvocationKind> {
        self.invocation_ordinal += 1; // native timer polls cap callbacks at 64
        self.origin = CaptureOrigin::Invocation {
            ordinal: self.invocation_ordinal,
        };
        let cost = match input {
            SessionInput::Control { command } => command_cost(command),
            _ => 256,
        };
        self.charge(cost).then(|| InvocationKind::from_input(input))
    }
    fn new(config: CaptureConfig) -> Self {
        Self {
            entries: Vec::new(),
            used: 0,
            limit: config.max_bytes - OUTCOME_RESERVE,
            max_entries: config.max_entries,
            observed_entries: 0,
            fault: None,
            invocation_index: None,
            invocation_ordinal: 0,
            origin: CaptureOrigin::Runner,
        }
    }
    fn fail(&mut self, fault: CaptureFault) {
        if self.fault.is_none() {
            self.fault = Some(fault);
        }
    }
    fn charge(&mut self, bytes: usize) -> bool {
        if self.fault.is_some() {
            return false;
        }
        if bytes > self.limit - self.used {
            self.fail(CaptureFault::Bytes);
            return false;
        }
        self.used += bytes;
        true
    }
    fn reserve_entry(&mut self, cost: usize) -> bool {
        self.observed_entries = self.observed_entries.saturating_add(1);
        if self.entries.len() >= self.max_entries {
            self.fail(CaptureFault::Entries);
            return false;
        }
        self.charge(cost.saturating_add(256))
    }
    pub(super) fn produced_record(
        &mut self,
        record: marketfeed_recording::shared_feed::FeedRecord,
    ) {
        if !self.reserve_entry(0) {
            return;
        }
        let mut buffer = LimitedBytes {
            bytes: Vec::new(),
            limit: self.limit - self.used,
        };
        if serde_json::to_writer(&mut buffer, &record).is_err() {
            self.fail(CaptureFault::Bytes);
            return;
        }
        self.used += buffer.bytes.len();
        self.push_entry(CaptureEntry::ProducedRecord(buffer.bytes));
    }
    pub(super) fn invocation(
        &mut self,
        kind: InvocationKind,
        actions: &ActionBuffer,
        dropped_before: u64,
        result: &Result<(), AdapterError>,
    ) {
        self.invocation_index = None;
        if actions
            .as_slice()
            .iter()
            .any(|a| matches!(a, SessionAction::SendSensitiveText(_)))
        {
            self.fail(CaptureFault::SensitiveAction);
            return;
        }
        let action_bytes = actions
            .as_slice()
            .iter()
            .map(action_cost)
            .fold(0usize, usize::saturating_add);
        let error_bytes = result.as_ref().err().map_or(0, adapter_error_cost);
        if !self.reserve_entry(action_bytes.saturating_add(error_bytes).saturating_add(512)) {
            return;
        }
        let disposition = if result.is_ok() {
            ActionDisposition::Buffered
        } else {
            ActionDisposition::DiscardedAfterCallbackError
        };
        self.invocation_index = Some(self.entries.len());
        self.push_entry(CaptureEntry::Invocation {
            kind,
            retained_actions: actions.len(),
            dropped_before,
            dropped_after: actions.dropped(),
            actions: actions
                .as_slice()
                .iter()
                .map(|a| BufferedAction {
                    action: a.clone(),
                    disposition: disposition.clone(),
                })
                .collect(),
            outcome: CallbackOutcome::Returned(result.clone()),
        });
    }
    fn push_entry(&mut self, effect: CaptureEntry) {
        self.entries.push(CaptureTraceEntry {
            origin: self.origin,
            effect,
        });
    }
    pub(super) fn finish_invocation(&mut self) {
        self.origin = CaptureOrigin::Runner;
        self.invocation_index = None;
    }
    pub(super) fn action_started(&mut self, index: usize) {
        self.origin = CaptureOrigin::Action {
            invocation: self.invocation_ordinal,
            index,
        };
        if let Some(CaptureEntry::Invocation { actions, .. }) = self
            .invocation_index
            .and_then(|i| self.entries.get_mut(i))
            .map(|entry| &mut entry.effect)
        {
            if let Some(action) = actions.get_mut(index) {
                action.disposition = ActionDisposition::Started;
            }
        }
    }
    pub(super) fn action_finished(&mut self, index: usize, result: &Result<(), EngineError>) {
        self.origin = CaptureOrigin::Invocation {
            ordinal: self.invocation_ordinal,
        };
        if !self.charge(result.as_ref().err().map_or(0, error_cost)) {
            return;
        }
        if let Some(CaptureEntry::Invocation { actions, .. }) = self
            .invocation_index
            .and_then(|i| self.entries.get_mut(i))
            .map(|entry| &mut entry.effect)
        {
            if let Some(action) = actions.get_mut(index) {
                action.disposition = ActionDisposition::Completed(result.clone());
            }
        }
    }
    pub(super) fn drops(&mut self, count: u64) {
        if self.reserve_entry(0) {
            self.push_entry(CaptureEntry::ActionBufferDropsConsumed(count));
        }
    }
    pub(super) fn suppressed(&mut self, error: &EngineError) {
        if self.reserve_entry(error_cost(error)) {
            self.push_entry(CaptureEntry::SuppressedDiagnostic(error.clone()));
        }
    }
    pub(super) fn subscription_prepared(
        &mut self,
        prepared: &Result<SubscriptionWireAction, AdapterError>,
    ) {
        let bytes = match prepared {
            Ok(
                SubscriptionWireAction::Text(b)
                | SubscriptionWireAction::Binary(b)
                | SubscriptionWireAction::Ping(b),
            ) => b.len(),
            Err(e) => adapter_error_cost(e),
        };
        if self.reserve_entry(bytes) {
            self.push_entry(CaptureEntry::SubscriptionPrepared(prepared.clone()));
        }
    }
    pub(super) fn subscription_committed(&mut self) {
        if self.reserve_entry(0) {
            self.push_entry(CaptureEntry::SubscriptionCommitted);
        }
    }
    pub(super) fn subscription_reservation(&mut self, result: &Result<(), DispatchError>) {
        if self.reserve_entry(256) {
            self.push_entry(CaptureEntry::SubscriptionReservation(result.clone()));
        }
    }
    pub(super) fn queue_start(
        &mut self,
        lane: CaptureQueueLane,
        incoming: QueueItemRef<'_>,
        evicted: Option<QueueItemRef<'_>>,
    ) -> Option<usize> {
        let cost = incoming
            .cost()
            .saturating_add(evicted.map_or(0, QueueItemRef::cost));
        if !self.reserve_entry(cost) {
            return None;
        }
        let index = self.entries.len();
        self.push_entry(CaptureEntry::QueueAttempt {
            lane,
            incoming: incoming.owned(),
            evicted_front: evicted.map(QueueItemRef::owned),
            result: None,
        });
        Some(index)
    }
    pub(super) fn drain_start<'a>(
        &mut self,
        lane: CaptureQueueLane,
        items: impl Iterator<Item = QueueItemRef<'a>>,
    ) -> Option<usize> {
        if !self.reserve_entry(0) {
            return None;
        }
        let mut retained = Vec::new();
        for item in items {
            if !self.charge(item.cost()) {
                return None;
            }
            retained.push(item.owned());
        }
        let index = self.entries.len();
        self.push_entry(CaptureEntry::QueueDrain {
            lane,
            items: retained,
            completed: false,
        });
        Some(index)
    }
    pub(super) fn drain_finished(&mut self, index: Option<usize>) {
        if let Some(CaptureEntry::QueueDrain { completed, .. }) = index
            .and_then(|i| self.entries.get_mut(i))
            .map(|entry| &mut entry.effect)
        {
            *completed = true;
        }
    }
    fn sink_start(&mut self, lane: CaptureQueueLane, incoming: QueueItemRef<'_>) -> Option<usize> {
        if !self.reserve_entry(incoming.cost()) {
            return None;
        }
        let index = self.entries.len();
        self.push_entry(CaptureEntry::SinkAttempt {
            lane,
            incoming: incoming.owned(),
            result: None,
        });
        Some(index)
    }
    fn sink_finished(&mut self, index: Option<usize>, result: &Result<PushOutcome, SinkError>) {
        if !self.charge(result.as_ref().err().map_or(0, sink_error_cost)) {
            return;
        }
        if let Some(CaptureEntry::SinkAttempt { result: target, .. }) = index
            .and_then(|i| self.entries.get_mut(i))
            .map(|entry| &mut entry.effect)
        {
            *target = Some(result.clone());
        }
    }
    pub(super) fn queue_finished(
        &mut self,
        index: Option<usize>,
        result: &Result<PushOutcome, EngineError>,
    ) {
        if let Some(CaptureEntry::QueueAttempt { result: target, .. }) = index
            .and_then(|i| self.entries.get_mut(i))
            .map(|entry| &mut entry.effect)
        {
            *target = Some(result.clone());
        }
    }
}

struct LimitedBytes {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for LimitedBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit - self.bytes.len() {
            return Err(io::Error::other("capture byte limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct Counter {
    bytes: usize,
}
impl Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self.bytes.saturating_add(bytes.len());
        if self.bytes > MAX_CAPTURE_BYTES {
            return Err(io::Error::other("capture size limit"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn batch_cost(batch: &EventBatch) -> usize {
    let mut counter = Counter { bytes: 0 };
    if serde_json::to_writer(&mut counter, batch).is_err() {
        usize::MAX
    } else {
        counter.bytes.saturating_add(256)
    }
}
fn observed_batch_cost(batch: &ObservedEventBatch) -> usize {
    batch
        .encoded_len(marketfeed_adapter_api::ObservedBatchLimits::HARD_MAX.encoded_bytes)
        .map_or(usize::MAX, |size| size.saturating_add(256))
}
fn market_batch_cost(batch: &MarketBatch) -> usize {
    match batch {
        MarketBatch::Canonical(batch) => batch_cost(batch),
        MarketBatch::Observed(batch) => observed_batch_cost(batch),
    }
}
fn system_cost(event: &SystemEvent) -> usize {
    let mut counter = Counter { bytes: 0 };
    if serde_json::to_writer(&mut counter, event).is_err() {
        usize::MAX
    } else {
        counter.bytes.saturating_add(256)
    }
}
fn http_cost(spec: &HttpRequestSpec) -> usize {
    spec.url
        .len()
        .saturating_add(
            spec.headers
                .iter()
                .map(|(a, b)| a.len().saturating_add(b.len()).saturating_add(64))
                .fold(0usize, usize::saturating_add),
        )
        .saturating_add(spec.body.as_ref().map_or(0, |b| b.len()))
        .saturating_add(512)
}
fn action_cost(action: &SessionAction) -> usize {
    match action {
        SessionAction::SendText(b) | SessionAction::SendBinary(b) | SessionAction::SendPing(b) => {
            b.len().saturating_add(256)
        }
        SessionAction::SendSensitiveText(_) => usize::MAX,
        SessionAction::EmitBatch(b) => batch_cost(b),
        SessionAction::EmitObservedBatch(b) => observed_batch_cost(b),
        SessionAction::EmitSystem(e) => system_cost(e),
        SessionAction::RequestHttp(h) => http_cost(h),
        _ => 256,
    }
}
fn command_cost(command: &SessionCommand) -> usize {
    match command {
        SessionCommand::Subscribe(s)
        | SessionCommand::Unsubscribe(s)
        | SessionCommand::Replace(s) => s
            .iter()
            .map(|s| s.len().saturating_add(64))
            .fold(256usize, usize::saturating_add),
        _ => 256,
    }
}
fn input_cost(input: &CaptureInput) -> usize {
    match input {
        CaptureInput::ForwardDispatch { results } => results
            .iter()
            .map(|r| r.as_ref().err().map_or(256, sink_error_cost))
            .fold(256usize, usize::saturating_add),
        CaptureInput::Text { payload, .. }
        | CaptureInput::Binary { payload, .. }
        | CaptureInput::Ping { payload, .. }
        | CaptureInput::Pong { payload, .. }
        | CaptureInput::Close { payload, .. } => payload.len().saturating_add(256),
        CaptureInput::Http { response, .. } => response.body.len().saturating_add(
            response
                .headers
                .iter()
                .map(|(a, b)| a.len().saturating_add(b.len()).saturating_add(64))
                .fold(512usize, usize::saturating_add),
        ),
        CaptureInput::Control { command, .. } => command_cost(command),
        CaptureInput::System(event) => system_cost(event),
        CaptureInput::Degraded { message, .. } => message.len().saturating_add(256),
        CaptureInput::Catalog { catalog, .. } => catalog
            .instruments
            .iter()
            .map(|i| {
                1024usize
                    .saturating_add(i.key.native_symbol.len())
                    .saturating_add(i.key.venue.0.len())
                    .saturating_add(i.base.0.len())
                    .saturating_add(i.quote.0.len())
                    .saturating_add(i.key.settlement.as_ref().map_or(0, |s| s.0.len()))
                    .saturating_add(i.settlement.as_ref().map_or(0, |s| s.0.len()))
            })
            .fold(256usize, usize::saturating_add),
        _ => 256,
    }
}
fn sink_error_cost(error: &SinkError) -> usize {
    match error {
        SinkError::Io(s) => s.len().saturating_add(256),
        SinkError::Unsupported(s) => s.len().saturating_add(256),
        _ => 256,
    }
}

struct ScriptedSink<'a> {
    results: &'a [Result<PushOutcome, SinkError>],
    next: usize,
}
impl ScriptedSink<'_> {
    fn next(&mut self) -> Result<PushOutcome, SinkError> {
        let result = self.results[self.next].clone();
        self.next += 1;
        result
    }
}
impl EventSink for ScriptedSink<'_> {
    fn push_observed_batch(&mut self, _: ObservedEventBatch) -> Result<PushOutcome, SinkError> {
        self.next()
    }
    fn push_batch(&mut self, _: EventBatch) -> Result<PushOutcome, SinkError> {
        self.next()
    }
    fn push_system(&mut self, _: SystemEvent) -> Result<PushOutcome, SinkError> {
        self.next()
    }
}

pub(super) struct CapturingSink<'a, S: EventSink + ?Sized> {
    pub(super) sink: &'a mut S,
    pub(super) capture: &'a mut ActiveCapture,
}
impl<S: EventSink + ?Sized> EventSink for CapturingSink<'_, S> {
    fn push_observed_batch(&mut self, batch: ObservedEventBatch) -> Result<PushOutcome, SinkError> {
        let index = self.capture.sink_start(
            CaptureQueueLane::Market,
            QueueItemRef::ObservedMarket(&batch),
        );
        let result = self.sink.push_observed_batch(batch);
        self.capture.sink_finished(index, &result);
        result
    }
    fn push_batch(&mut self, batch: EventBatch) -> Result<PushOutcome, SinkError> {
        let index = self
            .capture
            .sink_start(CaptureQueueLane::Market, QueueItemRef::Market(&batch));
        let result = self.sink.push_batch(batch);
        self.capture.sink_finished(index, &result);
        result
    }
    fn push_system(&mut self, event: SystemEvent) -> Result<PushOutcome, SinkError> {
        let index = self
            .capture
            .sink_start(CaptureQueueLane::System, QueueItemRef::System(&event));
        let result = self.sink.push_system(event);
        self.capture.sink_finished(index, &result);
        result
    }
}

fn adapter_error_cost(error: &AdapterError) -> usize {
    match error {
        AdapterError::UnsupportedCapability(s)
        | AdapterError::Protocol(s)
        | AdapterError::Parse(s)
        | AdapterError::Subscription(s)
        | AdapterError::Catalog(s)
        | AdapterError::BookInvariant(s)
        | AdapterError::Internal(s) => s.len().saturating_add(256),
    }
}
fn error_cost(error: &EngineError) -> usize {
    match error {
        EngineError::Adapter(e) => adapter_error_cost(e),
        EngineError::Internal(s) => s.len().saturating_add(256),
        EngineError::Recording(
            marketfeed_recording::RecordingError::Io(s)
            | marketfeed_recording::RecordingError::PublicationIndeterminate(s)
            | marketfeed_recording::RecordingError::InvalidControlCommand(s)
            | marketfeed_recording::RecordingError::UnsupportedOverflow(s)
            | marketfeed_recording::RecordingError::MetadataConflict { key: s },
        ) => s.len().saturating_add(256),
        EngineError::Transport(
            marketfeed_transport::TransportError::Tls(s)
            | marketfeed_transport::TransportError::Protocol(s)
            | marketfeed_transport::TransportError::Io(s)
            | marketfeed_transport::TransportError::Stub(s),
        ) => s.len().saturating_add(256),
        _ => 256,
    }
}

fn snapshot(runner: &SessionRunner, capture: &mut ActiveCapture) -> CapturedState {
    let mut timers: Vec<_> = runner
        .timers
        .iter()
        .map(|(&id, &time)| (id, time))
        .collect();
    timers.sort_by_key(|(id, _)| *id);
    let mut state = CapturedState {
        lifecycle: runner.lifecycle,
        connection: runner.cfg.connection,
        frame_seq: runner.inbound_frame_seq,
        stop_requested: runner.stop_requested,
        reconnect_requested: runner.reconnect_requested,
        timers,
        pending_market: runner.dispatch.batches().len(),
        pending_system: runner.dispatch.systems().len(),
        pending_writes: runner.pending_writes.len(),
        pending_http: runner.pending_http.len(),
        valid_books: runner.metrics.valid_books.load(Ordering::Relaxed),
        queues: None,
    };
    if capture.fault.is_some() {
        return state;
    }
    let cost = runner
        .dispatch
        .batches()
        .iter()
        .map(market_batch_cost)
        .chain(runner.dispatch.systems().iter().map(system_cost))
        .chain(
            runner
                .pending_writes
                .iter()
                .map(|w| w.payload.len().saturating_add(256)),
        )
        .chain(runner.pending_http.iter().map(http_cost))
        .fold(1024usize, usize::saturating_add);
    if !capture.charge(cost) {
        return state;
    }
    state.queues = Some(vec![
        (
            CaptureQueueLane::Market,
            runner
                .dispatch
                .batches()
                .iter()
                .map(|batch| QueueItemRef::market(batch).owned())
                .collect(),
        ),
        (
            CaptureQueueLane::System,
            runner
                .dispatch
                .systems()
                .iter()
                .cloned()
                .map(CaptureQueueItem::System)
                .collect(),
        ),
        (
            CaptureQueueLane::Write,
            runner
                .pending_writes
                .iter()
                .cloned()
                .map(CaptureQueueItem::Write)
                .collect(),
        ),
        (
            CaptureQueueLane::Http,
            runner
                .pending_http
                .iter()
                .cloned()
                .map(CaptureQueueItem::Http)
                .collect(),
        ),
    ]);
    state
}
