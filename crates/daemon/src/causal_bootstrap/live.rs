//! Opt-in owned public Hyperliquid transport. Application sends require MFCJ5
//! actor receipts; connect/read/automatic Pong/close are managed, unjournaled
//! transport controls. Close payloads are unavailable in the concrete backend.
//! No external sink, runtime HTTP, dynamic subscription or live qualification.
use super::{
    BootstrapError, CommittedBootstrap,
    owner::{
        history::{HistoryIngress, HistoryJournalOwner, HistoryStatus},
        *,
    },
};
use crate::hyperliquid_limits::{BudgetedWebSocket, HyperliquidLimits};
use marketfeed_adapter_api::{DisconnectReason, SessionCommand};
use marketfeed_engine::{CaptureInput, causal::*};
use marketfeed_model::{ConnectionId, FrameStamp, InstrumentId, TimestampNs};
use marketfeed_transport::{
    CloseReason, FrameBuffer, FrameOpcode, TransportError, WebSocketSpec, WebSocketTransport,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Notify;

#[path = "live_diagnostics.rs"]
mod diagnostics;
pub(super) use diagnostics::driver_report_value;
use diagnostics::{GateReason, TimingCounters, observed_read};
pub use diagnostics::{GateWaitTiming, ReadCallTiming, ReadDiagnostics, TimingSpan};

pub const DRIVER_POLICY_ROLE: &str = "hyperliquid-owned-public-driver-v1";
/// Canonical bounds committed BEFORE constructing the driver. Version 1 admits
/// periodic native timer polls. Version 2 admits a poll only when the latest
/// committed native timer schedule has a due deadline. Local wake frequency and
/// all other bounds are identical. These bound cooperative calls/queues, not
/// uninterruptible OS operations or final joins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverPolicy {
    pub schema_version: u16,
    pub max_frame_bytes: u64,
    pub connect_timeout_ms: u64,
    pub write_timeout_ms: u64,
    pub close_timeout_ms: u64,
    pub owner_poll_ms: u64,
    pub timer_interval_ms: u64,
    pub reconnect_delay_ms: u64,
    pub max_reconnects: u16,
    pub max_session_ms: u64,
}
impl DriverPolicy {
    pub fn bind(self, dto: &mut BootstrapDto) -> Result<(), BootstrapError> {
        if dto
            .public_inputs
            .iter()
            .any(|p| p.role == DRIVER_POLICY_ROLE)
        {
            return Err(BootstrapError::Unsupported(
                "duplicate public driver policy",
            ));
        }
        self.validate(dto)?;
        dto.public_inputs.push(PublicReproductionInputDto::new(
            DRIVER_POLICY_ROLE,
            &serde_json::to_vec(&self).map_err(CodecError::from)?,
        ));
        Ok(())
    }
    fn validate(&self, dto: &BootstrapDto) -> Result<(), BootstrapError> {
        if !matches!(dto.factory, FactoryDto::HyperliquidPublic { .. })
            || dto.output_profile != OutputProfileDto::DiskNullDispatchV1
            || dto.environment != BootstrapEnvironmentDto::Production
            || dto.session.endpoint_name != "wss://api.hyperliquid.xyz/ws"
            || dto.runner_policy.initial_connection != 1
            || dto.runner_policy.external_handles
            || dto.runner_policy.record
            || dto.runner_policy.mirror_capacity != 0
            || !matches!(self.schema_version, 1 | 2)
            || self.max_frame_bytes == 0
            || self.max_frame_bytes > 2 * 1024 * 1024
            || !(1..=60_000).contains(&self.connect_timeout_ms)
            || !(1..=60_000).contains(&self.write_timeout_ms)
            || !(1..=60_000).contains(&self.close_timeout_ms)
            || !(1..=100).contains(&self.owner_poll_ms)
            || !(10..=1_000).contains(&self.timer_interval_ms)
            || !(1..=60_000).contains(&self.reconnect_delay_ms)
            || self.max_reconnects > 1000
            || !(1..=86_400_000).contains(&self.max_session_ms)
        {
            return Err(BootstrapError::Unsupported("public driver profile/bounds"));
        }
        let owner = dto
            .owner_policy
            .as_ref()
            .ok_or(BootstrapError::Unsupported("driver owner policy missing"))?;
        // Include native queued-input representation overhead. The owner still
        // performs its exact bound check; this conservative margin is not RSS.
        if self
            .max_frame_bytes
            .checked_add(2048)
            .is_none_or(|n| n > owner.max_input_bytes)
        {
            return Err(BootstrapError::Unsupported(
                "driver frame/input bound mismatch",
            ));
        }
        dto.observation_policy()?;
        dto.history_policy()?;
        dto.wire_policy()?;
        Ok(())
    }
}
/// Called by bootstrap admission whenever the reserved role is present. A later
/// driver constructor also requires it, so no default policy appears post-ACK.
pub(super) fn policy(dto: &BootstrapDto) -> Result<DriverPolicy, BootstrapError> {
    let mut rows = dto
        .public_inputs
        .iter()
        .filter(|p| p.role == DRIVER_POLICY_ROLE);
    let row = rows
        .next()
        .ok_or(BootstrapError::Unsupported("missing public driver policy"))?;
    if rows.next().is_some() || row.bytes > 4096 || !bounded_policy_payload(&row.payload) {
        return Err(BootstrapError::Unsupported(
            "duplicate/oversized public driver policy",
        ));
    }
    let raw = row.payload.bytes()?;
    if raw.len() as u64 != row.bytes || bootstrap_sha256(&raw) != row.sha256 {
        return Err(BootstrapError::Unsupported(
            "driver policy bytes/digest mismatch",
        ));
    }
    let result: DriverPolicy = serde_json::from_slice(&raw).map_err(CodecError::from)?;
    if serde_json::to_vec(&result).map_err(CodecError::from)? != raw {
        return Err(BootstrapError::Unsupported(
            "noncanonical public driver policy",
        ));
    }
    result.validate(dto)?;
    Ok(result)
}

fn bounded_policy_payload(payload: &BytesDto) -> bool {
    // BytesDto deliberately hides its encoding. Count its serialized string
    // without allocating another copy, before decoding any claimed policy.
    struct Limit(usize);
    impl std::io::Write for Limit {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.0 {
                return Err(std::io::Error::other("driver policy encoded bound"));
            }
            self.0 -= bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Limit(5466), payload).is_ok()
}

/// Opaque collector identity. Clones share rolling application-message and
/// connection-attempt budgets AND static market/subscription reservations.
/// Other processes/IP users and automatic controls are outside these budgets.
#[derive(Clone)]
pub struct CollectorBudget {
    limits: Arc<HyperliquidLimits>,
    _collector: Arc<crate::DaemonState>,
}
impl CollectorBudget {
    pub fn from_collector(state: Arc<crate::DaemonState>) -> Result<Self, BootstrapError> {
        if state
            .config
            .venues
            .iter()
            .any(|v| v.adapter == "hyperliquid")
        {
            return Err(BootstrapError::Unsupported(
                "legacy Hyperliquid venues must be replaced, not shadowed",
            ));
        }
        Ok(Self {
            limits: state.hyperliquid_limits.clone(),
            _collector: state,
        })
    }
}
struct Reservation {
    limits: Arc<HyperliquidLimits>,
    spot: bool,
    subscriptions: usize,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.limits.release_causal(self.spot, self.subscriptions);
    }
}

struct ControlState {
    connection: ConnectionId,
    stopping: bool,
    stop: Option<IngressAdmission>,
    fatal: Option<&'static str>,
}
struct SharedControl {
    ingress: HistoryIngress,
    state: Mutex<ControlState>,
    wake: Notify,
    instruments: BTreeSet<u32>,
    wire_completed: AtomicU64,
}
fn lock(shared: &SharedControl) -> MutexGuard<'_, ControlState> {
    shared.state.lock().unwrap_or_else(|p| p.into_inner())
}
/// Bounded controls only. Success is ingress acceptance, not commit/delivery.
/// Stop fences synchronously before waking/cancelling the async driver.
#[derive(Clone)]
pub struct DriverControl {
    shared: Arc<SharedControl>,
}
impl DriverControl {
    pub fn request_stop(&self) -> Result<u64, &'static str> {
        let result = {
            let mut state = lock(&self.shared);
            if let Some(stop) = &state.stop {
                return Ok(stop.id);
            }
            state.stopping = true;
            match self
                .shared
                .ingress
                .try_submit(state.connection, CaptureInput::Stop)
            {
                Ok(admission) => {
                    let id = admission.id;
                    state.stop = Some(admission);
                    Ok(id)
                }
                Err(_) => {
                    state.fatal = Some("Stop ingress refused");
                    self.shared.ingress.revoke();
                    Err("Stop ingress refused")
                }
            }
        };
        self.shared.wake.notify_one();
        result
    }
    pub fn request_resync(&self, instrument: InstrumentId) -> Result<u64, &'static str> {
        if !self.shared.instruments.contains(&instrument.0) {
            return Err("instrument outside committed catalog");
        }
        let state = lock(&self.shared);
        if state.stopping {
            return Err("driver stopping");
        }
        let now = wall_time().ok_or("wall clock overflow")?;
        self.shared
            .ingress
            .try_submit(
                state.connection,
                CaptureInput::Control {
                    command: SessionCommand::Resync(instrument),
                    now,
                },
            )
            .map(|admission| admission.id)
            .map_err(|_| "Resync ingress refused")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitReason {
    RequestedStop,
    SessionBudget,
    OwnerFailure,
    IngressFailure,
    TransportFailure,
    WriteFailure,
    ClockFailure,
}
/// The API stage where an error was observed; this does not attribute which
/// socket/control byte failed. Automatic Pong may flush during read/write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportStage {
    Connect,
    Read,
    Close,
    ApplicationWrite,
    ApplicationWriteDeadline,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportDiagnostic {
    pub stage: TransportStage,
    pub detail: WireErrorDto,
    pub observed_wall: Option<TimestampNs>,
    pub observed_mono_ns: u64,
    pub managed_unjournaled: bool,
}
/// Local operational evidence only. No market value/readiness is published by
/// this driver. Clean intentional stop is a prerequisite, not a Seal API.
#[derive(Debug)]
pub struct DriverReport {
    pub reason: ExitReason,
    pub status: HistoryStatus,
    pub wire_clean: bool,
    pub stop_input_id: Option<u64>,
    pub stop_committed: bool,
    pub clean_stop: bool,
    pub connections: u64,
    pub handoffs: u64,
    pub inbound_text: u64,
    pub inbound_binary: u64,
    /// Acquired frames refused by ingress/bounds. Their bytes are not complete
    /// journal evidence, so any nonzero value denies clean semantic completion.
    pub unaccepted_frames: u64,
    pub native_controls: u64,
    pub wire_completions: u64,
    pub duration: Duration,
    pub transport_error_count: u64,
    pub last_transport_error: Option<TransportDiagnostic>,
    pub read_diagnostics: ReadDiagnostics,
}
pub struct CausalHyperliquidDriver {
    core: Core<BudgetedWebSocket>,
    _collector: Arc<crate::DaemonState>,
}
impl CausalHyperliquidDriver {
    pub fn new(
        bootstrap: CommittedBootstrap,
        budget: CollectorBudget,
    ) -> Result<(Self, DriverControl), BootstrapError> {
        let bound = policy(&bootstrap.dto)?;
        let FactoryDto::HyperliquidPublic { market } = bootstrap.dto.factory else {
            unreachable!("validated Hyperliquid policy")
        };
        let spot = market == HyperliquidMarketDto::Spot;
        let subscriptions = bootstrap.dto.protocol_topics.len();
        budget
            .limits
            .reserve_causal(spot, subscriptions)
            .map_err(|_| {
                BootstrapError::Unsupported("collector market/subscription reservation refused")
            })?;
        let reservation = Reservation {
            limits: budget.limits.clone(),
            spot,
            subscriptions,
        };
        let spec = WebSocketSpec {
            url: bootstrap.dto.session.endpoint_name.clone(),
            max_frame_bytes: bound.max_frame_bytes as usize,
            connect_timeout_ms: bound.connect_timeout_ms,
            ..WebSocketSpec::default()
        };
        let instruments = bootstrap
            .dto
            .catalog
            .instruments
            .iter()
            .map(|i| i.id)
            .collect();
        // This consumes the only bootstrap exactly once; the public API cannot
        // substitute a transport, endpoint or caller-generated receipt.
        let owner = bootstrap
            .instantiate()?
            .spawn_history_owner()
            .map_err(|_| BootstrapError::Unsupported("history owner start failed"))?;
        let (core, control) = Core::new(
            owner,
            BudgetedWebSocket::new_causal(budget.limits),
            bound,
            spec,
            instruments,
            reservation,
        );
        Ok((
            Self {
                core,
                _collector: budget._collector,
            },
            control,
        ))
    }
    pub fn run(self) -> DriverRun {
        let ingress = self.core.control.shared.ingress.clone();
        DriverRun {
            inner: Box::pin(async move {
                let _collector = self._collector;
                self.core.run().await
            }),
            ingress,
            completed: false,
        }
    }
}
/// Cancelling this future synchronously fences the owner BEFORE dropping any
/// in-flight wire invocation. Drop may join accepted work without a finite bound.
pub struct DriverRun {
    inner: std::pin::Pin<Box<dyn std::future::Future<Output = DriverReport> + Send>>,
    ingress: HistoryIngress,
    completed: bool,
}
impl std::future::Future for DriverRun {
    type Output = DriverReport;
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let result = self.inner.as_mut().poll(cx);
        if result.is_ready() {
            self.completed = true;
        }
        result
    }
}
impl Drop for DriverRun {
    fn drop(&mut self) {
        if !self.completed {
            self.ingress.revoke();
        }
    }
}
fn wall_time() -> Option<TimestampNs> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_nanos(),
    )
    .ok()
    .map(TimestampNs)
}

/// Replaced only from an observed committed native release. Unknown startup
/// state differs from a known empty schedule; neither invents a native poll.
#[derive(Default)]
struct CommittedTimerSchedule {
    known: bool,
    next: Option<i64>,
}
impl CommittedTimerSchedule {
    fn replace(&mut self, timers: &[(u64, i64)]) {
        self.known = true;
        self.next = timers.iter().map(|(_, deadline)| *deadline).min();
    }
    fn due(&self, now: TimestampNs) -> bool {
        self.known && self.next.is_some_and(|deadline| deadline <= now.0)
    }
}
struct Core<T> {
    owner: HistoryJournalOwner,
    transport: T,
    policy: DriverPolicy,
    spec: WebSocketSpec,
    control: DriverControl,
    _reservation: Reservation,
    #[cfg(all(test, feature = "causal-journal"))]
    wall_clock: Option<Arc<std::sync::atomic::AtomicI64>>,
}
impl<T: WebSocketTransport> Core<T> {
    fn new(
        owner: HistoryJournalOwner,
        transport: T,
        policy: DriverPolicy,
        spec: WebSocketSpec,
        instruments: BTreeSet<u32>,
        reservation: Reservation,
    ) -> (Self, DriverControl) {
        let control = DriverControl {
            shared: Arc::new(SharedControl {
                ingress: owner.ingress(),
                state: Mutex::new(ControlState {
                    connection: ConnectionId(0),
                    stopping: false,
                    stop: None,
                    fatal: None,
                }),
                wake: Notify::new(),
                instruments,
                wire_completed: AtomicU64::new(0),
            }),
        };
        (
            Self {
                owner,
                transport,
                policy,
                spec,
                control: control.clone(),
                _reservation: reservation,
                #[cfg(all(test, feature = "causal-journal"))]
                wall_clock: None,
            },
            control,
        )
    }
    fn stopping(&self) -> bool {
        lock(&self.control.shared).stopping
    }
    fn wall_time(&self) -> Option<TimestampNs> {
        #[cfg(all(test, feature = "causal-journal"))]
        if let Some(clock) = &self.wall_clock {
            return Some(TimestampNs(clock.load(Ordering::SeqCst)));
        }
        wall_time()
    }
    fn owner_quiescent(&self) -> bool {
        self.read_gate_reason().is_none()
    }
    fn read_gate_reason(&self) -> Option<GateReason> {
        let status = self.owner.status();
        match status.owner.phase {
            OwnerPhase::Executing => return Some(GateReason::Executing),
            OwnerPhase::AwaitingAdmission => return Some(GateReason::AwaitingAdmission),
            OwnerPhase::AwaitingCommit => return Some(GateReason::AwaitingCommit),
            OwnerPhase::ReleasePending => return Some(GateReason::ReleasePending),
            OwnerPhase::Stopped => return Some(GateReason::Stopped),
            OwnerPhase::Idle => {}
        }
        if status.owner.pending_operations != 0 || status.ingress_observations != 0 {
            return Some(GateReason::Pending);
        }
        (!self.owner.wire_progress().is_some_and(|p| p.0 == 0 && p.2)).then_some(GateReason::Wire)
    }
    fn submit(&self, input: CaptureInput) -> bool {
        self.admit(input).is_some()
    }
    fn admit(&self, input: CaptureInput) -> Option<IngressAdmission> {
        let mut state = lock(&self.control.shared);
        if state.stopping {
            return None;
        }
        let connection = match input {
            CaptureInput::Connected { connection, .. } => connection,
            _ => state.connection,
        };
        match self.control.shared.ingress.try_submit(connection, input) {
            Ok(admission) => {
                state.connection = connection;
                Some(admission)
            }
            Err(_) => {
                state.fatal = Some("driver ingress refused");
                self.control.shared.ingress.revoke();
                None
            }
        }
    }
    async fn run(mut self) -> DriverReport {
        use tokio::time::{Instant as TokioInstant, MissedTickBehavior, interval, timeout};
        let started = Instant::now();
        let mut report = Counters::default();
        let mut buffer = FrameBuffer::with_capacity(self.policy.max_frame_bytes as usize);
        let mut poll = interval(Duration::from_millis(self.policy.owner_poll_ms));
        let mut timer = interval(Duration::from_millis(self.policy.timer_interval_ms));
        poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut timer_pending: Option<IngressAdmission> = None;
        let mut committed_timers = CommittedTimerSchedule::default();
        let mut prefer_read = false;
        let mut connected = false;
        let mut next_connection = 1u64;
        let mut reconnects = 0u16;
        let mut reconnect_at = TokioInstant::now();
        let mut reason = ExitReason::RequestedStop;
        'drive: loop {
            if timer_pending
                .as_mut()
                .is_some_and(|input| input.try_result().is_some())
            {
                timer_pending = None;
            }
            let mut reconnect_requested = false;
            // Empty/control handoffs are required observations, not disposable
            // notifications. No release value escapes as authoritative output.
            while let Some(release) = self.owner.try_next() {
                report.handoffs += 1;
                // This is the actual post-ACK release, never a prepared trace,
                // provisional runner peek, or inferred metadata timer.
                committed_timers.replace(&release.evidence().after.timers);
                reconnect_requested |= release.evidence().after.reconnect_requested;
                if release.evidence().after.pending_http != 0 {
                    self.control.shared.ingress.revoke();
                    reason = ExitReason::OwnerFailure;
                    break 'drive;
                }
            }
            if self.stopping() {
                break;
            }
            if started.elapsed() >= Duration::from_millis(self.policy.max_session_ms) {
                reason = ExitReason::SessionBudget;
                let _ = self.control.request_stop();
                break;
            }
            if self.owner.status().owner.failure.is_some() {
                reason = ExitReason::OwnerFailure;
                break;
            }
            if reconnect_requested && connected {
                report.read_timing.close_gate_run(Instant::now());
                let Some(now) = self.wall_time() else {
                    reason = ExitReason::ClockFailure;
                    break;
                };
                if !self.submit(CaptureInput::Disconnected {
                    reason: DisconnectReason::ReconnectRequested,
                    now,
                }) {
                    reason = ExitReason::IngressFailure;
                    break;
                }
                // Accepted safety transition precedes managed close/cancellation.
                managed_close(
                    &mut self.transport,
                    self.policy.close_timeout_ms,
                    CloseReason::GoingAway,
                    &mut report,
                    started,
                )
                .await;
                connected = false;
                reconnects += 1;
                reconnect_at =
                    TokioInstant::now() + Duration::from_millis(self.policy.reconnect_delay_ms);
            }
            if reconnects > self.policy.max_reconnects {
                reason = ExitReason::TransportFailure;
                break;
            }
            if let Some(token) = self.owner.try_take_wire() {
                enum Write {
                    Completed(
                        Result<
                            super::owner::history::WireSendCompletion,
                            super::owner::history::WireSendError,
                        >,
                    ),
                    Stop,
                    Timeout,
                }
                let outcome = {
                    let invocation = token.invoke(&mut self.transport);
                    tokio::pin!(invocation);
                    tokio::select! {
                        biased;
                        _ = self.control.shared.wake.notified() => Write::Stop,
                        result = &mut invocation => Write::Completed(result),
                        _ = tokio::time::sleep(Duration::from_millis(self.policy.write_timeout_ms)) => {
                            // Fence while the invocation future still exists;
                            // its subsequent Drop records Unknown, never retry.
                            self.control.shared.ingress.revoke();
                            Write::Timeout
                        }
                    }
                };
                match outcome {
                    Write::Completed(Ok(completion))
                        if completion.outcome == WireOutcomeDto::Succeeded =>
                    {
                        self.control
                            .shared
                            .wire_completed
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Write::Stop => break,
                    Write::Completed(Ok(completion)) => {
                        if let Some(detail) = completion.detail {
                            report.error_detail(TransportStage::ApplicationWrite, detail, started);
                        }
                        reason = ExitReason::WriteFailure;
                        break;
                    }
                    Write::Timeout => {
                        report.error(
                            TransportStage::ApplicationWriteDeadline,
                            TransportError::Timeout,
                            started,
                        );
                        reason = ExitReason::WriteFailure;
                        break;
                    }
                    Write::Completed(Err(_)) => {
                        reason = ExitReason::WriteFailure;
                        break;
                    }
                }
                continue;
            }
            if !connected {
                if !self.owner_quiescent() || TokioInstant::now() < reconnect_at {
                    tokio::select! { biased; _ = self.control.shared.wake.notified() => {}, _ = poll.tick() => {} }
                    continue;
                }
                enum Connect {
                    Ready(Result<(), TransportError>),
                    Stop,
                }
                let outcome = tokio::select! {
                    biased;
                    _ = self.control.shared.wake.notified() => Connect::Stop,
                    result = timeout(Duration::from_millis(self.policy.connect_timeout_ms), self.transport.connect(&self.spec)) => Connect::Ready(result.unwrap_or(Err(TransportError::Timeout))),
                };
                match outcome {
                    Connect::Stop => break,
                    Connect::Ready(Ok(())) => {
                        let Some(now) = self.wall_time() else {
                            reason = ExitReason::ClockFailure;
                            break;
                        };
                        if !self.submit(CaptureInput::Connected {
                            connection: ConnectionId(next_connection),
                            now,
                        }) {
                            reason = ExitReason::IngressFailure;
                            break;
                        }
                        next_connection += 1;
                        report.connections += 1;
                        connected = true;
                    }
                    Connect::Ready(Err(error)) => {
                        report.error(TransportStage::Connect, error, started);
                        let Some(now) = self.wall_time() else {
                            reason = ExitReason::ClockFailure;
                            break;
                        };
                        if !self.submit(CaptureInput::TransportLost {
                            reason: DisconnectReason::TransportError,
                            now,
                        }) {
                            reason = ExitReason::IngressFailure;
                            break;
                        }
                        managed_close(
                            &mut self.transport,
                            self.policy.close_timeout_ms,
                            CloseReason::GoingAway,
                            &mut report,
                            started,
                        )
                        .await;
                        reconnects += 1;
                        reconnect_at = TokioInstant::now()
                            + Duration::from_millis(self.policy.reconnect_delay_ms);
                    }
                }
                continue;
            }
            enum Read {
                Frame(
                    Result<
                        marketfeed_transport::InboundFrame,
                        marketfeed_transport::TransportError,
                    >,
                ),
                Timer,
                Poll,
                Stop,
            }
            // Admission counters reserve synchronously, but can retire before
            // ReleaseIntent/HandoffObserved and wire candidate installation.
            // Idle additionally requires those dispositions to have drained.
            // Do not acquire another physical frame while that owner work is
            // pending: an accepted input is not a native queue capacity permit.
            let read = if let Some(gate_reason) = self.read_gate_reason() {
                let wait_started = Instant::now();
                report.read_timing.begin_gate_run(wait_started);
                let read = tokio::select! {
                    biased;
                    _ = self.control.shared.wake.notified() => Read::Stop,
                    _ = poll.tick() => Read::Poll,
                };
                report
                    .read_timing
                    .gate_wait(gate_reason, wait_started.elapsed());
                read
            } else if prefer_read {
                report.read_timing.close_gate_run(Instant::now());
                tokio::select! {
                    biased;
                    _ = self.control.shared.wake.notified() => Read::Stop,
                    result = observed_read(self.transport.read_frame(&mut buffer), &mut report.read_timing) => Read::Frame(result),
                    _ = timer.tick() => Read::Timer,
                    _ = poll.tick() => Read::Poll,
                }
            } else {
                report.read_timing.close_gate_run(Instant::now());
                tokio::select! {
                    biased;
                    _ = self.control.shared.wake.notified() => Read::Stop,
                    _ = timer.tick() => Read::Timer,
                    result = observed_read(self.transport.read_frame(&mut buffer), &mut report.read_timing) => Read::Frame(result),
                    _ = poll.tick() => Read::Poll,
                }
            };
            match read {
                Read::Stop => break,
                Read::Poll => {}
                Read::Timer => {
                    let Some(now) = self.wall_time() else {
                        reason = ExitReason::ClockFailure;
                        break;
                    };
                    // Readiness ticks coalesce while the previous actual poll
                    // is outstanding. No acquired native frame is discarded.
                    if timer_pending.is_none()
                        && (self.policy.schema_version == 1 || committed_timers.due(now))
                    {
                        timer_pending = self.admit(CaptureInput::TimerPoll { now });
                        if timer_pending.is_none() {
                            reason = ExitReason::IngressFailure;
                            break;
                        }
                        // Give an already-ready frame first chance after this
                        // poll, even if durable work outlasted the interval.
                        // An idle socket still permits the next timer to run.
                        prefer_read = true;
                    }
                }
                Read::Frame(Ok(frame)) => {
                    // Reserve one unaccepted acquisition before ANY fallible
                    // bound/clock/admission work; retire it only on acceptance.
                    report.unaccepted_frames += 1;
                    if frame.payload.len() > self.policy.max_frame_bytes as usize {
                        reason = ExitReason::IngressFailure;
                        break;
                    }
                    let (Some(receive_ts), Ok(mono_ns)) = (
                        self.wall_time(),
                        u64::try_from(started.elapsed().as_nanos()),
                    ) else {
                        reason = ExitReason::ClockFailure;
                        break;
                    };
                    let received = FrameStamp {
                        receive_ts,
                        mono_ns,
                    };
                    let input = match frame.opcode {
                        FrameOpcode::Text => {
                            report.inbound_text += 1;
                            CaptureInput::Text {
                                payload: frame.payload,
                                received,
                            }
                        }
                        FrameOpcode::Binary => {
                            report.inbound_binary += 1;
                            CaptureInput::Binary {
                                payload: frame.payload,
                                received,
                            }
                        }
                        FrameOpcode::Ping => {
                            report.native_controls += 1;
                            CaptureInput::Ping {
                                payload: frame.payload,
                                received,
                            }
                        }
                        FrameOpcode::Pong => {
                            report.native_controls += 1;
                            CaptureInput::Pong {
                                payload: frame.payload,
                                received,
                            }
                        }
                        FrameOpcode::Close => {
                            report.native_controls += 1;
                            CaptureInput::Close {
                                payload: frame.payload,
                                received,
                            }
                        }
                    };
                    if !self.submit(input) {
                        reason = ExitReason::IngressFailure;
                        break;
                    }
                    report.unaccepted_frames -= 1;
                    prefer_read = false;
                }
                Read::Frame(Err(error)) => {
                    report.read_timing.close_gate_run(Instant::now());
                    report.error(TransportStage::Read, error, started);
                    let Some(now) = self.wall_time() else {
                        reason = ExitReason::ClockFailure;
                        break;
                    };
                    if !self.submit(CaptureInput::TransportLost {
                        reason: DisconnectReason::TransportError,
                        now,
                    }) {
                        reason = ExitReason::IngressFailure;
                        break;
                    }
                    managed_close(
                        &mut self.transport,
                        self.policy.close_timeout_ms,
                        CloseReason::GoingAway,
                        &mut report,
                        started,
                    )
                    .await;
                    connected = false;
                    reconnects += 1;
                    reconnect_at =
                        TokioInstant::now() + Duration::from_millis(self.policy.reconnect_delay_ms);
                }
            }
        }
        report.read_timing.close_gate_run(Instant::now());
        let intentional = self.stopping();
        if intentional
            && reason == ExitReason::OwnerFailure
            && matches!(
                self.owner.status().owner.failure,
                Some(OwnerFailure::NativeStopped)
            )
        {
            reason = ExitReason::RequestedStop;
        }
        if !intentional {
            self.control.shared.ingress.revoke();
        }
        // Stop was accepted/fenced by the control handle, or revoke fenced this
        // failure path. Only now close ingress, cancel/close the owned socket.
        self.owner.close_ingress();
        managed_close(
            &mut self.transport,
            self.policy.close_timeout_ms,
            CloseReason::LocalStop,
            &mut report,
            started,
        )
        .await;
        loop {
            while self.owner.try_next().is_some() {
                report.handoffs += 1;
            }
            if self.owner.status().owner.phase == OwnerPhase::Stopped {
                break;
            }
            tokio::time::sleep(Duration::from_millis(self.policy.owner_poll_ms)).await;
        }
        let wire_clean = self.owner.wire_progress().is_some_and(|p| p.0 == 0 && p.2);
        let (stop_input_id, stop_committed) = {
            let mut state = lock(&self.control.shared);
            match state.stop.as_mut() {
                Some(stop) => (
                    Some(stop.id),
                    matches!(stop.try_result(), Some(InputSettlement::Committed { id, .. }) if id == stop.id),
                ),
                None => (None, false),
            }
        };
        // The owner always revokes+joins. This wait intentionally has no finite
        // deadline: an accepted durable ticket/callback cannot be abandoned.
        let status = tokio::task::spawn_blocking(move || self.owner.shutdown())
            .await
            .expect("owner join task");
        let clean_stop = intentional
            && matches!(
                reason,
                ExitReason::RequestedStop | ExitReason::SessionBudget
            )
            && report.unaccepted_frames == 0
            && stop_committed
            && wire_clean
            && status.owner.phase == OwnerPhase::Stopped
            && matches!(
                status.owner.failure,
                None | Some(OwnerFailure::NativeStopped)
            );
        DriverReport {
            reason,
            status,
            wire_clean,
            stop_input_id,
            stop_committed,
            clean_stop,
            connections: report.connections,
            handoffs: report.handoffs,
            inbound_text: report.inbound_text,
            inbound_binary: report.inbound_binary,
            unaccepted_frames: report.unaccepted_frames,
            native_controls: report.native_controls,
            wire_completions: self.control.shared.wire_completed.load(Ordering::Relaxed),
            duration: started.elapsed(),
            transport_error_count: report.transport_error_count,
            last_transport_error: report.last_transport_error,
            read_diagnostics: report.read_timing.data,
        }
    }
}
#[derive(Default)]
struct Counters {
    connections: u64,
    handoffs: u64,
    inbound_text: u64,
    inbound_binary: u64,
    unaccepted_frames: u64,
    native_controls: u64,
    transport_error_count: u64,
    last_transport_error: Option<TransportDiagnostic>,
    read_timing: TimingCounters,
}
impl Counters {
    fn error(&mut self, stage: TransportStage, error: TransportError, started: Instant) {
        self.error_detail(stage, WireErrorDto::from_transport(error), started);
    }
    fn error_detail(&mut self, stage: TransportStage, detail: WireErrorDto, started: Instant) {
        self.transport_error_count += 1;
        self.last_transport_error = Some(TransportDiagnostic {
            stage,
            detail,
            observed_wall: wall_time(),
            observed_mono_ns: u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            managed_unjournaled: matches!(
                stage,
                TransportStage::Connect | TransportStage::Read | TransportStage::Close
            ),
        });
    }
}
async fn managed_close<T: WebSocketTransport>(
    transport: &mut T,
    timeout_ms: u64,
    reason: CloseReason,
    report: &mut Counters,
    started: Instant,
) {
    let result = tokio::time::timeout(Duration::from_millis(timeout_ms), transport.close(reason))
        .await
        .unwrap_or(Err(TransportError::Timeout));
    if let Err(error) = result {
        report.error(TransportStage::Close, error, started);
    }
}

#[cfg(all(test, feature = "causal-journal"))]
#[path = "live_tests.rs"]
mod tests;
