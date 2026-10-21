//! Fixed-size local application-read diagnostics. These do not observe socket,
//! TLS or kernel arrival, and never authorize native input or release.
use super::DriverReport;
use marketfeed_engine::causal::HyperliquidMarketDto;
use marketfeed_transport::{InboundFrame, TransportError};
use serde::Serialize;
use serde_json::{Value, json};
use std::time::{Duration, Instant};

pub(in crate::causal_bootstrap) fn driver_report_value(
    market: &HyperliquidMarketDto,
    r: &DriverReport,
) -> Value {
    json!({"market":market,"reason":format!("{:?}",r.reason),"owner_phase":format!("{:?}",r.status.owner.phase),"owner_failure":r.status.owner.failure.as_ref().map(|failure|format!("{failure:?}")),"finalization_expired":r.status.finalization_expired,"accepted_inputs":r.status.owner.accepted,"executed_inputs":r.status.owner.executed,"withheld_inputs":r.status.owner.withheld,"abandoned_inputs":r.status.owner.abandoned,"clean_stop":r.clean_stop,"stop_committed":r.stop_committed,"wire_clean":r.wire_clean,"connections":r.connections,"handoffs":r.handoffs,"inbound_text":r.inbound_text,"inbound_binary":r.inbound_binary,"unaccepted_frames":r.unaccepted_frames,"wire_completions":r.wire_completions,"transport_error_count":r.transport_error_count,"last_transport_error":format!("{:?}",r.last_transport_error),"read_diagnostics":r.read_diagnostics,"read_diagnostics_scope":"local monotonic application-read and observed owner-gate timing; wait-entry reasons are not phase residency; gate waits and runs overlap and are non-additive; no kernel/TLS arrival, native freshness or availability evidence"})
}

#[derive(Debug, Default, Clone, Serialize, PartialEq, Eq)]
pub struct TimingSpan {
    pub intervals: u64,
    pub total_ns: u64,
    pub max_ns: u64,
}
impl TimingSpan {
    fn observe(&mut self, duration: Duration, saturated: &mut bool) {
        let ns = u64::try_from(duration.as_nanos()).unwrap_or_else(|_| {
            *saturated = true;
            u64::MAX
        });
        self.intervals = self.intervals.checked_add(1).unwrap_or_else(|| {
            *saturated = true;
            u64::MAX
        });
        self.total_ns = self.total_ns.checked_add(ns).unwrap_or_else(|| {
            *saturated = true;
            u64::MAX
        });
        self.max_ns = self.max_ns.max(ns);
    }
}

#[derive(Debug, Default, Clone, Serialize, PartialEq, Eq)]
pub struct GateWaitTiming {
    pub owner_executing: TimingSpan,
    pub owner_awaiting_admission: TimingSpan,
    pub owner_awaiting_commit: TimingSpan,
    pub owner_release_pending: TimingSpan,
    pub owner_stopped: TimingSpan,
    pub idle_pending_inputs_or_observations: TimingSpan,
    pub wire_not_quiescent: TimingSpan,
}
#[derive(Clone, Copy)]
pub(super) enum GateReason {
    Executing,
    AwaitingAdmission,
    AwaitingCommit,
    ReleasePending,
    Stopped,
    Pending,
    Wire,
}
#[derive(Debug, Default, Clone, Serialize, PartialEq, Eq)]
pub struct ReadCallTiming {
    pub frames: TimingSpan,
    pub errors: TimingSpan,
    /// A timeout returned by the existing transport, not a new driver deadline.
    pub returned_timeouts: TimingSpan,
    /// A first-polled read future dropped before returning, including select
    /// cancellation, outer-future drop or unwinding. This is not a timeout.
    pub polled_future_dropped: TimingSpan,
}
/// Wait reasons are sampled at wait entry, not continuous owner-phase residency.
/// Observed gate runs include loop work between false and true gate observations.
/// Read calls include library buffering/copying and local scheduling. None of
/// these clocks establish native availability or network delivery latency.
/// Gate runs and gate waits overlap intentionally and must not be added.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReadDiagnostics {
    pub diagnostics_version: u16,
    pub gate_wait: GateWaitTiming,
    pub observed_gate_runs: TimingSpan,
    pub read_calls: ReadCallTiming,
    pub saturated: bool,
}
impl Default for ReadDiagnostics {
    fn default() -> Self {
        Self {
            diagnostics_version: 1,
            gate_wait: GateWaitTiming::default(),
            observed_gate_runs: TimingSpan::default(),
            read_calls: ReadCallTiming::default(),
            saturated: false,
        }
    }
}
#[derive(Debug, Default)]
pub(super) struct TimingCounters {
    pub data: ReadDiagnostics,
    gate_run_started: Option<Instant>,
}
impl TimingCounters {
    pub fn begin_gate_run(&mut self, now: Instant) {
        self.gate_run_started.get_or_insert(now);
    }
    pub fn close_gate_run(&mut self, now: Instant) {
        if let Some(start) = self.gate_run_started.take() {
            self.data.observed_gate_runs.observe(
                now.saturating_duration_since(start),
                &mut self.data.saturated,
            );
        }
    }
    pub fn gate_wait(&mut self, reason: GateReason, duration: Duration) {
        let span = match reason {
            GateReason::Executing => &mut self.data.gate_wait.owner_executing,
            GateReason::AwaitingAdmission => &mut self.data.gate_wait.owner_awaiting_admission,
            GateReason::AwaitingCommit => &mut self.data.gate_wait.owner_awaiting_commit,
            GateReason::ReleasePending => &mut self.data.gate_wait.owner_release_pending,
            GateReason::Stopped => &mut self.data.gate_wait.owner_stopped,
            GateReason::Pending => &mut self.data.gate_wait.idle_pending_inputs_or_observations,
            GateReason::Wire => &mut self.data.gate_wait.wire_not_quiescent,
        };
        span.observe(duration, &mut self.data.saturated);
    }
}
enum ReadOutcome {
    Frame,
    Error,
    ReturnedTimeout,
    Cancelled,
}
struct ReadAttempt<'a> {
    counters: &'a mut TimingCounters,
    started: Instant,
    completed: bool,
}
impl ReadAttempt<'_> {
    fn finish(mut self, outcome: ReadOutcome) {
        self.record(outcome);
        self.completed = true;
    }
    fn record(&mut self, outcome: ReadOutcome) {
        let span = match outcome {
            ReadOutcome::Frame => &mut self.counters.data.read_calls.frames,
            ReadOutcome::Error => &mut self.counters.data.read_calls.errors,
            ReadOutcome::ReturnedTimeout => &mut self.counters.data.read_calls.returned_timeouts,
            ReadOutcome::Cancelled => &mut self.counters.data.read_calls.polled_future_dropped,
        };
        span.observe(self.started.elapsed(), &mut self.counters.data.saturated);
    }
}
impl Drop for ReadAttempt<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.record(ReadOutcome::Cancelled);
        }
    }
}
pub(super) async fn observed_read<F>(
    read: F,
    counters: &mut TimingCounters,
) -> Result<InboundFrame, TransportError>
where
    F: std::future::Future<Output = Result<InboundFrame, TransportError>>,
{
    // An async function body starts on first poll. A biased select branch that
    // was never polled cannot create an attempt or a cancellation observation.
    let attempt = ReadAttempt {
        counters,
        started: Instant::now(),
        completed: false,
    };
    let result = read.await;
    attempt.finish(match &result {
        Ok(_) => ReadOutcome::Frame,
        Err(TransportError::Timeout) => ReadOutcome::ReturnedTimeout,
        Err(_) => ReadOutcome::Error,
    });
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn constructing_an_unpolled_read_is_not_a_diagnostic_attempt() {
        let mut constructions = 0;
        let mut counters = TimingCounters::default();
        let read = {
            constructions += 1;
            std::future::pending::<Result<InboundFrame, TransportError>>()
        };
        drop(observed_read(read, &mut counters));
        assert_eq!(constructions, 1);
        assert_eq!(counters.data, ReadDiagnostics::default());
        tokio::select! {
            biased;
            _ = std::future::ready(()) => {},
            _ = observed_read({
                constructions += 1;
                std::future::pending::<Result<InboundFrame, TransportError>>()
            }, &mut counters) => panic!("biased read branch must not run"),
        }
        assert_eq!(
            constructions, 2,
            "inner future is still eagerly constructed"
        );
        assert_eq!(counters.data, ReadDiagnostics::default());
    }

    #[test]
    fn exact_duration_accumulation_partition_and_saturation() {
        let mut counters = TimingCounters::default();
        counters.gate_wait(GateReason::AwaitingCommit, Duration::from_nanos(100));
        counters.gate_wait(GateReason::AwaitingCommit, Duration::from_nanos(200));
        counters.gate_wait(GateReason::Wire, Duration::from_nanos(50));
        assert_eq!(
            counters.data.gate_wait.owner_awaiting_commit,
            TimingSpan {
                intervals: 2,
                total_ns: 300,
                max_ns: 200,
            }
        );
        assert_eq!(counters.data.gate_wait.wire_not_quiescent.total_ns, 50);
        assert_eq!(
            counters.data.gate_wait.owner_executing,
            TimingSpan::default()
        );
        assert!(!counters.data.saturated);
        counters.data.gate_wait.owner_stopped.intervals = u64::MAX;
        counters.data.gate_wait.owner_stopped.total_ns = u64::MAX;
        counters.gate_wait(GateReason::Stopped, Duration::from_nanos(1));
        assert!(counters.data.saturated);
        assert_eq!(counters.data.gate_wait.owner_stopped.intervals, u64::MAX);
        assert_eq!(counters.data.gate_wait.owner_stopped.total_ns, u64::MAX);
    }

    #[test]
    fn observed_gate_run_closes_once_including_terminal_partial_run() {
        let mut counters = TimingCounters::default();
        let start = Instant::now();
        counters.begin_gate_run(start);
        counters.begin_gate_run(start + Duration::from_nanos(10));
        counters.close_gate_run(start + Duration::from_nanos(30));
        counters.close_gate_run(start + Duration::from_nanos(40));
        counters.begin_gate_run(start + Duration::from_nanos(50));
        counters.close_gate_run(start + Duration::from_nanos(70));
        assert_eq!(
            counters.data.observed_gate_runs,
            TimingSpan {
                intervals: 2,
                total_ns: 50,
                max_ns: 30,
            }
        );
    }
}
