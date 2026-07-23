//! Bounded recording enqueue (producer → disk writer).

use std::collections::VecDeque;

use marketfeed_model::{OverflowPolicy, SystemEvent};

use crate::format::{Direction, FrameOpcode, RecordingError};
use marketfeed_model::SessionId;

/// Frame waiting to hit disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingFrame {
    pub session: SessionId,
    pub frame_seq: u64,
    pub receive_ts_ns: i64,
    pub monotonic_ns: u64,
    pub direction: Direction,
    pub opcode: FrameOpcode,
    pub flags: u8,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueOutcome {
    Accepted,
    DroppedNewest,
    DroppedOldest { dropped: usize },
}

/// Bounded pending-frame queue with explicit overflow policy.
#[derive(Debug)]
pub struct RecordingQueue {
    capacity: usize,
    policy: OverflowPolicy,
    items: VecDeque<PendingFrame>,
    pub dropped_total: u64,
}

impl RecordingQueue {
    pub fn new(capacity: usize, policy: OverflowPolicy) -> Self {
        assert!(capacity > 0, "recording queue capacity must be > 0");
        Self {
            capacity,
            policy,
            items: VecDeque::with_capacity(capacity),
            dropped_total: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn push(&mut self, frame: PendingFrame) -> Result<EnqueueOutcome, RecordingError> {
        if self.items.len() < self.capacity {
            self.items.push_back(frame);
            return Ok(EnqueueOutcome::Accepted);
        }
        match self.policy {
            OverflowPolicy::DropNewest => {
                self.dropped_total += 1;
                Ok(EnqueueOutcome::DroppedNewest)
            }
            OverflowPolicy::DropOldest => {
                let _ = self.items.pop_front();
                self.items.push_back(frame);
                self.dropped_total += 1;
                Ok(EnqueueOutcome::DroppedOldest { dropped: 1 })
            }
            OverflowPolicy::FailEngine => Err(RecordingError::QueueFull),
            other => Err(RecordingError::UnsupportedOverflow(format!("{other:?}"))),
        }
    }

    pub fn pop_front(&mut self) -> Option<PendingFrame> {
        self.items.pop_front()
    }

    pub fn drain_front(&mut self, max: usize) -> Vec<PendingFrame> {
        let count = max.min(self.items.len());
        self.items.drain(..count).collect()
    }

    /// System events for drop / pressure (caller decides rate limits).
    pub fn overflow_events(outcome: EnqueueOutcome, dropped_total: u64) -> Vec<SystemEvent> {
        match outcome {
            EnqueueOutcome::Accepted => Vec::new(),
            EnqueueOutcome::DroppedNewest | EnqueueOutcome::DroppedOldest { .. } => vec![
                SystemEvent::QueuePressure {
                    detail: "recording queue full".into(),
                },
                SystemEvent::EventsDropped {
                    count: 1,
                    detail: format!("recording_queue dropped_total={dropped_total}"),
                },
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(seq: u64) -> PendingFrame {
        PendingFrame {
            session: SessionId(1),
            frame_seq: seq,
            receive_ts_ns: 0,
            monotonic_ns: 0,
            direction: Direction::Inbound,
            opcode: FrameOpcode::Text,
            flags: 0,
            payload: vec![b'x'],
        }
    }

    #[test]
    fn drop_oldest_under_pressure() {
        let mut q = RecordingQueue::new(1, OverflowPolicy::DropOldest);
        assert!(matches!(q.push(frame(1)), Ok(EnqueueOutcome::Accepted)));
        assert!(matches!(
            q.push(frame(2)),
            Ok(EnqueueOutcome::DroppedOldest { dropped: 1 })
        ));
        assert_eq!(q.pop_front().unwrap().frame_seq, 2);
        assert_eq!(q.dropped_total, 1);
    }

    #[test]
    fn fail_engine_when_full() {
        let mut q = RecordingQueue::new(1, OverflowPolicy::FailEngine);
        q.push(frame(1)).unwrap();
        assert!(matches!(q.push(frame(2)), Err(RecordingError::QueueFull)));
    }
}
