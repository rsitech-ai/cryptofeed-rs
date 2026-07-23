//! In-memory bounded sink (tests / embedded consumers).

use marketfeed_adapter_api::EventBatch;
use marketfeed_dispatch::{EventDispatcher, PushOutcome};
use marketfeed_model::{OverflowPolicy, SystemEvent};

use crate::sink::{EventSink, SinkError};

/// Bounded in-process sink: retains events until popped (or dropped by policy).
#[derive(Debug)]
pub struct MemorySink {
    inner: EventDispatcher,
}

impl MemorySink {
    pub fn new(batch_capacity: usize, system_capacity: usize, policy: OverflowPolicy) -> Self {
        Self {
            inner: EventDispatcher::new(batch_capacity, system_capacity, policy),
        }
    }

    pub fn pop_batch(&mut self) -> Option<EventBatch> {
        self.inner.pop_batch()
    }

    pub fn pop_system(&mut self) -> Option<SystemEvent> {
        self.inner.pop_system()
    }

    pub fn batch_len(&self) -> usize {
        self.inner.batches().len()
    }

    pub fn system_len(&self) -> usize {
        self.inner.systems().len()
    }

    pub fn dropped_batches(&self) -> u64 {
        self.inner.batches().dropped_total
    }

    pub fn dropped_systems(&self) -> u64 {
        self.inner.systems().dropped_total
    }

    pub fn policy(&self) -> OverflowPolicy {
        self.inner.batches().policy()
    }
}

impl EventSink for MemorySink {
    fn push_batch(&mut self, batch: EventBatch) -> Result<PushOutcome, SinkError> {
        self.inner.push_batch(batch).map_err(SinkError::from)
    }

    fn push_system(&mut self, event: SystemEvent) -> Result<PushOutcome, SinkError> {
        self.inner.push_system(event).map_err(SinkError::from)
    }
}
