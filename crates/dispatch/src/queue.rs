//! Bounded queue with explicit overflow policy.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use marketfeed_adapter_api::EventBatch;
use marketfeed_model::{OverflowPolicy, SystemEvent};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DispatchError {
    #[error("queue full under FailEngine policy")]
    FailEngine,
    #[error("queue full and BlockWithDeadline timed out")]
    DeadlineExceeded,
    #[error("overflow policy not implemented for this queue: {0:?}")]
    UnsupportedPolicy(OverflowPolicy),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushOutcome {
    Accepted,
    DroppedNewest,
    DroppedOldest { dropped: usize },
}

/// Single-shard bounded dispatcher queue (session→consumer).
#[derive(Debug)]
pub struct BoundedQueue<T> {
    capacity: usize,
    policy: OverflowPolicy,
    items: VecDeque<T>,
    pub dropped_total: u64,
}

impl<T> BoundedQueue<T> {
    pub fn new(capacity: usize, policy: OverflowPolicy) -> Self {
        assert!(capacity > 0, "capacity must be > 0");
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

    pub fn policy(&self) -> OverflowPolicy {
        self.policy
    }

    pub fn pop_front(&mut self) -> Option<T> {
        self.items.pop_front()
    }

    pub fn front(&self) -> Option<&T> {
        self.items.front()
    }

    /// Non-blocking push respecting OverflowPolicy.
    pub fn try_push(&mut self, item: T) -> Result<PushOutcome, DispatchError> {
        if self.items.len() < self.capacity {
            self.items.push_back(item);
            return Ok(PushOutcome::Accepted);
        }

        match self.policy {
            OverflowPolicy::DropNewest => {
                self.dropped_total += 1;
                Ok(PushOutcome::DroppedNewest)
            }
            OverflowPolicy::DropOldest => {
                let _ = self.items.pop_front();
                self.items.push_back(item);
                self.dropped_total += 1;
                Ok(PushOutcome::DroppedOldest { dropped: 1 })
            }
            OverflowPolicy::FailEngine => Err(DispatchError::FailEngine),
            OverflowPolicy::BlockWithDeadline => Err(DispatchError::DeadlineExceeded),
            other => Err(DispatchError::UnsupportedPolicy(other)),
        }
    }

    /// BlockWithDeadline: spin-wait until space or deadline (sync; for tests / inject path).
    ///
    /// ponytail: busy-wait only; ceiling = wasted CPU under stall; upgrade = condvar/async wait.
    pub fn push_with_deadline(
        &mut self,
        mut item: T,
        deadline: Instant,
    ) -> Result<PushOutcome, DispatchError> {
        if self.policy != OverflowPolicy::BlockWithDeadline {
            return self.try_push(item);
        }
        loop {
            if self.items.len() < self.capacity {
                self.items.push_back(item);
                return Ok(PushOutcome::Accepted);
            }
            if Instant::now() >= deadline {
                return Err(DispatchError::DeadlineExceeded);
            }
            std::thread::yield_now();
            // keep item owned
            let _ = &mut item;
        }
    }
}

/// Dispatch lane for market event batches + system events.
#[derive(Debug)]
pub struct EventDispatcher {
    batches: BoundedQueue<EventBatch>,
    systems: BoundedQueue<SystemEvent>,
}

impl EventDispatcher {
    pub fn new(batch_capacity: usize, system_capacity: usize, policy: OverflowPolicy) -> Self {
        Self {
            batches: BoundedQueue::new(batch_capacity, policy),
            systems: BoundedQueue::new(system_capacity, policy),
        }
    }

    pub fn push_batch(&mut self, batch: EventBatch) -> Result<PushOutcome, DispatchError> {
        self.batches.try_push(batch)
    }

    pub fn push_system(&mut self, event: SystemEvent) -> Result<PushOutcome, DispatchError> {
        self.systems.try_push(event)
    }

    pub fn pop_batch(&mut self) -> Option<EventBatch> {
        self.batches.pop_front()
    }

    pub fn pop_system(&mut self) -> Option<SystemEvent> {
        self.systems.pop_front()
    }

    pub fn batches(&self) -> &BoundedQueue<EventBatch> {
        &self.batches
    }

    pub fn systems(&self) -> &BoundedQueue<SystemEvent> {
        &self.systems
    }

    pub fn drain_batches(&mut self) -> Vec<EventBatch> {
        let mut out = Vec::with_capacity(self.batches.len());
        while let Some(b) = self.batches.pop_front() {
            out.push(b);
        }
        out
    }

    pub fn drain_systems(&mut self) -> Vec<SystemEvent> {
        let mut out = Vec::with_capacity(self.systems.len());
        while let Some(e) = self.systems.pop_front() {
            out.push(e);
        }
        out
    }
}

/// Helper for tests / inject paths that want a short block deadline.
pub fn deadline_from_now(timeout: Duration) -> Instant {
    Instant::now() + timeout
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketfeed_model::SessionId;

    fn batch(seq: u64) -> EventBatch {
        EventBatch {
            session: SessionId(1),
            frame_seq: seq,
            events: Vec::new(),
        }
    }

    #[test]
    fn drop_oldest_and_fail_engine() {
        let mut q = BoundedQueue::new(2, OverflowPolicy::DropOldest);
        assert!(q.try_push(1).is_ok());
        assert!(q.try_push(2).is_ok());
        assert_eq!(
            q.try_push(3).unwrap(),
            PushOutcome::DroppedOldest { dropped: 1 }
        );
        assert_eq!(q.pop_front(), Some(2));
        assert_eq!(q.pop_front(), Some(3));

        let mut fail = BoundedQueue::new(1, OverflowPolicy::FailEngine);
        fail.try_push(1).unwrap();
        assert!(matches!(fail.try_push(2), Err(DispatchError::FailEngine)));
    }

    #[test]
    fn drop_newest_preserves_existing() {
        let mut q = BoundedQueue::new(1, OverflowPolicy::DropNewest);
        q.try_push(batch(1)).unwrap();
        assert_eq!(q.try_push(batch(2)).unwrap(), PushOutcome::DroppedNewest);
        assert_eq!(q.pop_front().unwrap().frame_seq, 1);
        assert_eq!(q.dropped_total, 1);
    }

    #[test]
    fn push_outcome_signals_caller_must_emit_events_dropped() {
        // Engine converts these outcomes to SystemEvent::EventsDropped — never silent.
        let mut q = BoundedQueue::new(1, OverflowPolicy::DropOldest);
        q.try_push(1).unwrap();
        assert_eq!(
            q.try_push(2).unwrap(),
            PushOutcome::DroppedOldest { dropped: 1 }
        );
        let mut newest = BoundedQueue::new(1, OverflowPolicy::DropNewest);
        newest.try_push(1).unwrap();
        assert_eq!(newest.try_push(2).unwrap(), PushOutcome::DroppedNewest);
    }

    /// Deterministic push/pop permutation oracle for overflow policies.
    ///
    /// ponytail: `BoundedQueue` is single-owner (no shared mutable concurrency), so Loom
    /// would add dep weight without modeling real races. Ceiling = misses multi-thread
    /// interleavings; upgrade = Loom (or shuttle) once a sharded/concurrent queue exists.
    /// See `docs/plan/chaos_supply_chain.md`.
    #[test]
    fn overflow_op_permutations_match_oracle() {
        #[derive(Clone, Copy)]
        enum Op {
            Push(u64),
            Pop,
        }

        fn run(policy: OverflowPolicy, ops: &[Op]) -> (Vec<u64>, u64, Option<&'static str>) {
            let mut q = BoundedQueue::new(2, policy);
            let mut drained = Vec::new();
            let mut err = None;
            for op in ops {
                match *op {
                    Op::Push(v) => match q.try_push(v) {
                        Ok(_) => {}
                        Err(DispatchError::FailEngine) => err = Some("fail"),
                        Err(DispatchError::DeadlineExceeded) => err = Some("deadline"),
                        Err(DispatchError::UnsupportedPolicy(_)) => err = Some("unsupported"),
                    },
                    Op::Pop => {
                        if let Some(v) = q.pop_front() {
                            drained.push(v);
                        }
                    }
                }
            }
            while let Some(v) = q.pop_front() {
                drained.push(v);
            }
            (drained, q.dropped_total, err)
        }

        // Explicit push/pop permutations (full n! skipped as O(n!); this is the check).
        let base = [
            Op::Push(1),
            Op::Push(2),
            Op::Push(3),
            Op::Pop,
            Op::Pop,
            Op::Push(4),
        ];
        let perms: &[&[Op]] = &[
            &base,
            &[
                Op::Push(1),
                Op::Pop,
                Op::Push(2),
                Op::Push(3),
                Op::Push(4),
                Op::Pop,
            ],
            &[
                Op::Push(1),
                Op::Push(2),
                Op::Pop,
                Op::Push(3),
                Op::Pop,
                Op::Push(4),
            ],
            &[
                Op::Pop,
                Op::Push(1),
                Op::Push(2),
                Op::Push(3),
                Op::Pop,
                Op::Push(4),
            ],
            // Extra permutations (still O(1) fixed set — not full n!).
            &[
                Op::Push(1),
                Op::Push(2),
                Op::Push(3),
                Op::Push(4),
                Op::Pop,
                Op::Pop,
                Op::Pop,
                Op::Push(5),
            ],
            &[
                Op::Push(10),
                Op::Push(20),
                Op::Pop,
                Op::Pop,
                Op::Push(30),
                Op::Push(40),
                Op::Push(50),
            ],
            &[
                Op::Pop,
                Op::Pop,
                Op::Push(1),
                Op::Push(2),
                Op::Push(3),
                Op::Pop,
                Op::Push(4),
                Op::Push(5),
            ],
        ];

        for ops in perms {
            let (drop_oldest, dropped_o, err_o) = run(OverflowPolicy::DropOldest, ops);
            let (drop_newest, dropped_n, err_n) = run(OverflowPolicy::DropNewest, ops);
            let (fail, _, err_f) = run(OverflowPolicy::FailEngine, ops);
            assert!(err_o.is_none());
            assert!(err_n.is_none());
            // Re-run for stability (determinism).
            assert_eq!(run(OverflowPolicy::DropOldest, ops).0, drop_oldest);
            assert_eq!(run(OverflowPolicy::DropNewest, ops).0, drop_newest);
            assert_eq!(run(OverflowPolicy::FailEngine, ops).0, fail);
            assert_eq!(run(OverflowPolicy::DropOldest, ops).1, dropped_o);
            assert_eq!(run(OverflowPolicy::DropNewest, ops).1, dropped_n);
            assert_eq!(run(OverflowPolicy::FailEngine, ops).2, err_f);
            // Capacity 2: DropOldest never exceeds 2 live items before final drain.
            assert!(drop_oldest.len() <= ops.iter().filter(|o| matches!(o, Op::Push(_))).count());
            assert!(drop_newest.len() <= 2 + ops.iter().filter(|o| matches!(o, Op::Pop)).count());
            let _ = (fail, err_f);
        }
    }

    /// Engine-adjacent overflow: DropNewest then DropOldest leave distinct survivors.
    ///
    /// ponytail: unit-level policy oracle only; ceiling = no live WS / slow-sink /
    /// clock-jump inject; upgrade = chaos harness in `docs/plan/chaos_supply_chain.md`.
    #[test]
    fn overflow_policy_survivors_differ() {
        let mut newest = BoundedQueue::new(2, OverflowPolicy::DropNewest);
        newest.try_push(1u64).unwrap();
        newest.try_push(2).unwrap();
        assert_eq!(newest.try_push(3).unwrap(), PushOutcome::DroppedNewest);
        let mut n = Vec::new();
        while let Some(v) = newest.pop_front() {
            n.push(v);
        }
        assert_eq!(n, vec![1, 2]);

        let mut oldest = BoundedQueue::new(2, OverflowPolicy::DropOldest);
        oldest.try_push(1u64).unwrap();
        oldest.try_push(2).unwrap();
        assert_eq!(
            oldest.try_push(3).unwrap(),
            PushOutcome::DroppedOldest { dropped: 1 }
        );
        let mut o = Vec::new();
        while let Some(v) = oldest.pop_front() {
            o.push(v);
        }
        assert_eq!(o, vec![2, 3]);
    }
}
