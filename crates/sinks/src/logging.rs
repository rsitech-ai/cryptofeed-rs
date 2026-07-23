//! Tracing-backed sink with a bounded ingress queue.

use marketfeed_adapter_api::EventBatch;
use marketfeed_dispatch::PushOutcome;
use marketfeed_model::{OverflowPolicy, SystemEvent};
use tracing::{debug, warn};

use crate::memory::MemorySink;
use crate::sink::{EventSink, SinkError};

/// Bounded sink that logs accepted items, then drops them from the queue.
///
/// Sync log-then-drain: under steady sync use the ingress queue stays empty.
/// Overflow still applies if a caller fills `MemorySink` without draining first
/// via [`crate::forward_dispatcher`] into a non-draining sink.
// ponytail: sync log I/O blocks the caller; ceiling = stall under slow tracing.
// Upgrade = background worker. Kafka/NATS EventSinks: feature-gated TCP clients
// in `kafka` / `nats` modules (see crate README).
#[derive(Debug)]
pub struct LoggingSink {
    inner: MemorySink,
}

impl LoggingSink {
    pub fn new(batch_capacity: usize, system_capacity: usize, policy: OverflowPolicy) -> Self {
        Self {
            inner: MemorySink::new(batch_capacity, system_capacity, policy),
        }
    }

    pub fn dropped_batches(&self) -> u64 {
        self.inner.dropped_batches()
    }

    pub fn dropped_systems(&self) -> u64 {
        self.inner.dropped_systems()
    }
}

impl EventSink for LoggingSink {
    fn push_batch(&mut self, batch: EventBatch) -> Result<PushOutcome, SinkError> {
        let outcome = self.inner.push_batch(batch)?;
        match outcome {
            PushOutcome::Accepted | PushOutcome::DroppedOldest { .. } => {
                while let Some(b) = self.inner.pop_batch() {
                    debug!(
                        session = b.session.0,
                        frame_seq = b.frame_seq,
                        n = b.events.len(),
                        "sink batch"
                    );
                }
            }
            PushOutcome::DroppedNewest => {
                warn!("logging sink dropped newest batch");
            }
        }
        Ok(outcome)
    }

    fn push_system(&mut self, event: SystemEvent) -> Result<PushOutcome, SinkError> {
        let outcome = self.inner.push_system(event)?;
        match outcome {
            PushOutcome::Accepted | PushOutcome::DroppedOldest { .. } => {
                while let Some(ev) = self.inner.pop_system() {
                    debug!(?ev, "sink system");
                }
            }
            PushOutcome::DroppedNewest => {
                warn!("logging sink dropped newest system event");
            }
        }
        Ok(outcome)
    }
}
