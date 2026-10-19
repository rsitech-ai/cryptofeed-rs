//! Bounded recording enqueue (producer → disk writer).

use std::collections::VecDeque;

use marketfeed_model::{OverflowPolicy, SystemEvent};

use crate::format::{
    Direction, FrameOpcode, MAX_RAW_RECORD_LEN, RAW_HEADER_BODY_LEN, RecordingError,
};
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
    byte_capacity: usize,
    bytes: usize,
    policy: OverflowPolicy,
    items: VecDeque<PendingFrame>,
    pub dropped_total: u64,
}

impl RecordingQueue {
    pub fn new(capacity: usize, policy: OverflowPolicy) -> Self {
        Self::with_byte_capacity(capacity, 128 * 1024 * 1024, policy)
    }
    pub fn with_byte_capacity(
        capacity: usize,
        byte_capacity: usize,
        policy: OverflowPolicy,
    ) -> Self {
        assert!(capacity > 0, "recording queue capacity must be > 0");
        assert!(byte_capacity > 0, "recording byte capacity must be > 0");
        Self {
            capacity,
            byte_capacity,
            bytes: 0,
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
    pub fn bytes(&self) -> usize {
        self.bytes
    }
    pub fn front(&self) -> Option<&PendingFrame> {
        self.items.front()
    }

    pub fn validate_payload(payload: &[u8]) -> Result<(), RecordingError> {
        let record_len = payload.len().saturating_add(4 + RAW_HEADER_BODY_LEN);
        if record_len > MAX_RAW_RECORD_LEN as usize {
            return Err(RecordingError::RecordTooLarge {
                record_len: u32::try_from(record_len).unwrap_or(u32::MAX),
                max: MAX_RAW_RECORD_LEN,
            });
        }
        Ok(())
    }
    fn charge(frame: &PendingFrame) -> usize {
        frame
            .payload
            .capacity()
            .saturating_add(std::mem::size_of::<PendingFrame>())
    }

    pub fn push(&mut self, frame: PendingFrame) -> Result<EnqueueOutcome, RecordingError> {
        Self::validate_payload(&frame.payload)?;
        let charge = Self::charge(&frame);
        if self.items.len() < self.capacity
            && charge <= self.byte_capacity.saturating_sub(self.bytes)
        {
            self.bytes += charge;
            self.items.push_back(frame);
            return Ok(EnqueueOutcome::Accepted);
        }
        match self.policy {
            OverflowPolicy::DropNewest => {
                self.dropped_total += 1;
                Ok(EnqueueOutcome::DroppedNewest)
            }
            OverflowPolicy::DropOldest => {
                if charge > self.byte_capacity {
                    self.dropped_total += 1;
                    return Ok(EnqueueOutcome::DroppedNewest);
                }
                let mut dropped = 0;
                while self.items.len() >= self.capacity
                    || charge > self.byte_capacity.saturating_sub(self.bytes)
                {
                    self.pop_front();
                    dropped += 1;
                }
                self.bytes += charge;
                self.items.push_back(frame);
                self.dropped_total += dropped as u64;
                Ok(EnqueueOutcome::DroppedOldest { dropped })
            }
            OverflowPolicy::FailEngine => Err(RecordingError::QueueFull),
            other => Err(RecordingError::UnsupportedOverflow(format!("{other:?}"))),
        }
    }

    pub fn pop_front(&mut self) -> Option<PendingFrame> {
        let frame = self.items.pop_front()?;
        self.bytes -= Self::charge(&frame);
        Some(frame)
    }

    pub fn drain_front(&mut self, max: usize) -> Vec<PendingFrame> {
        let count = max.min(self.items.len());
        (0..count)
            .map(|_| self.pop_front().expect("bounded drain"))
            .collect()
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
                    count: match outcome {
                        EnqueueOutcome::DroppedOldest { dropped } => dropped as u64,
                        _ => 1,
                    },
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
    #[test]
    fn byte_limit_can_drop_multiple_records_and_charges_allocated_capacity() {
        let charge = RecordingQueue::charge(&frame(1));
        let mut q = RecordingQueue::with_byte_capacity(10, charge * 3, OverflowPolicy::DropOldest);
        for seq in 1..=3 {
            q.push(frame(seq)).unwrap();
        }
        let mut large = frame(4);
        large.payload = Vec::with_capacity(charge * 2);
        large.payload.push(1);
        let expected = RecordingQueue::charge(&large);
        assert_eq!(
            q.push(large).unwrap(),
            EnqueueOutcome::DroppedOldest { dropped: 3 }
        );
        assert_eq!(q.bytes(), expected);
        q.pop_front();
        assert_eq!(q.bytes(), 0);
    }
    #[test]
    fn oversized_record_is_rejected_before_queue_acceptance() {
        let mut q = RecordingQueue::new(1, OverflowPolicy::FailEngine);
        let mut oversized = frame(1);
        oversized.payload = vec![0; MAX_RAW_RECORD_LEN as usize];
        assert!(matches!(
            q.push(oversized),
            Err(RecordingError::RecordTooLarge { .. })
        ));
        assert!(q.is_empty());
        assert_eq!(q.bytes(), 0);
    }
}
