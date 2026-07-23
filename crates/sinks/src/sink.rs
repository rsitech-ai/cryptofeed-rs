//! Sink trait and dispatcher forward helper.

use marketfeed_adapter_api::EventBatch;
use marketfeed_dispatch::{DispatchError, EventDispatcher, PushOutcome};
use marketfeed_model::SystemEvent;
use thiserror::Error;

/// Failure from a sink push (maps from dispatch overflow / I/O).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SinkError {
    #[error("sink queue full under FailEngine policy")]
    FailEngine,
    #[error("sink queue full and BlockWithDeadline timed out")]
    DeadlineExceeded,
    #[error("overflow policy not implemented for this sink: {0:?}")]
    UnsupportedPolicy(marketfeed_model::OverflowPolicy),
    #[error("sink I/O: {0}")]
    Io(String),
    /// Broker / feature not wired (Kafka/NATS stubs, optional backends).
    #[error("sink unsupported: {0}")]
    Unsupported(&'static str),
}

impl From<DispatchError> for SinkError {
    fn from(value: DispatchError) -> Self {
        match value {
            DispatchError::FailEngine => Self::FailEngine,
            DispatchError::DeadlineExceeded => Self::DeadlineExceeded,
            DispatchError::UnsupportedPolicy(p) => Self::UnsupportedPolicy(p),
        }
    }
}

/// Consumer of normalized market batches and system events (spec §17.4).
///
/// Implementations MUST apply a bounded ingress queue and an explicit
/// [`marketfeed_model::OverflowPolicy`]. A slow sink must not require the
/// producer to grow unbounded memory.
pub trait EventSink {
    fn push_batch(&mut self, batch: EventBatch) -> Result<PushOutcome, SinkError>;
    fn push_system(&mut self, event: SystemEvent) -> Result<PushOutcome, SinkError>;
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ForwardReport {
    pub dropped_batches: u64,
    pub dropped_systems: u64,
}

impl ForwardReport {
    pub fn dropped_total(self) -> u64 {
        self.dropped_batches + self.dropped_systems
    }
}

/// Drain a dispatcher into a sink (session → external consumer glue).
pub fn forward_dispatcher<S: EventSink + ?Sized>(
    src: &mut EventDispatcher,
    sink: &mut S,
) -> Result<ForwardReport, SinkError> {
    let mut report = ForwardReport::default();
    while let Some(batch) = src.batches().front().cloned() {
        let outcome = sink.push_batch(batch)?;
        let _ = src.pop_batch();
        if outcome != PushOutcome::Accepted {
            report.dropped_batches += 1;
        }
    }
    while let Some(event) = src.systems().front().cloned() {
        let outcome = sink.push_system(event)?;
        let _ = src.pop_system();
        if outcome != PushOutcome::Accepted {
            report.dropped_systems += 1;
        }
    }
    Ok(report)
}
