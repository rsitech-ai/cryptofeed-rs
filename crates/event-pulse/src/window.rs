//! Bounded availability-time windows used by deterministic mechanics.

use std::collections::{BTreeMap, VecDeque};

use thiserror::Error;

pub const PER_WINDOW_CAPACITY: usize = 4_096;
pub const PROCESSOR_RECORD_CAPACITY: usize = 65_536;
pub const SUPPORTED_HORIZONS_NS: [i64; 7] = [
    100_000_000,
    250_000_000,
    1_000_000_000,
    5_000_000_000,
    15_000_000_000,
    30_000_000_000,
    60_000_000_000,
];

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WindowError {
    #[error("unsupported mechanics horizon")]
    UnsupportedHorizon,
    #[error("availability time regressed")]
    AvailabilityRegression,
    #[error("checked time arithmetic overflowed")]
    TimeOverflow,
    #[error("bounded window capacity breached; affected source is invalid")]
    QueueDrop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timed<T> {
    pub available_at_ns: i64,
    pub value: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoverageInterval {
    pub covered_from_ns: i64,
    pub covered_through_ns: i64,
    pub available_at_ns: i64,
}

/// True only when admitted intervals form a gapless union over the requested
/// availability-time window. Empty families therefore require explicit proof.
pub fn has_exact_coverage(
    intervals: &[CoverageInterval],
    decision_time_ns: i64,
    horizon_ns: i64,
) -> Result<bool, WindowError> {
    if !SUPPORTED_HORIZONS_NS.contains(&horizon_ns) {
        return Err(WindowError::UnsupportedHorizon);
    }
    let start = decision_time_ns
        .checked_sub(horizon_ns)
        .ok_or(WindowError::TimeOverflow)?;
    let mut through = start;
    for interval in intervals
        .iter()
        .filter(|x| x.available_at_ns <= decision_time_ns)
    {
        if interval.covered_from_ns > interval.covered_through_ns
            || interval.covered_through_ns > interval.available_at_ns
        {
            return Ok(false);
        }
        if interval.covered_through_ns < through {
            continue;
        }
        if interval.covered_from_ns > through {
            return Ok(false);
        }
        through = through.max(interval.covered_through_ns);
        if through >= decision_time_ns {
            return Ok(true);
        }
    }
    Ok(false)
}

#[derive(Debug, Clone)]
pub struct FixedWindow<T> {
    horizon_ns: i64,
    epoch_first_available_ns: i64,
    records: VecDeque<Timed<T>>,
    invalid: bool,
}

impl<T> FixedWindow<T> {
    pub fn new(horizon_ns: i64, epoch_first_available_ns: i64) -> Result<Self, WindowError> {
        if !SUPPORTED_HORIZONS_NS.contains(&horizon_ns) {
            return Err(WindowError::UnsupportedHorizon);
        }
        Ok(Self {
            horizon_ns,
            epoch_first_available_ns,
            records: VecDeque::with_capacity(PER_WINDOW_CAPACITY),
            invalid: false,
        })
    }

    pub fn push(&mut self, available_at_ns: i64, value: T) -> Result<(), WindowError> {
        if self.invalid {
            return Err(WindowError::QueueDrop);
        }
        if self
            .records
            .back()
            .is_some_and(|last| available_at_ns < last.available_at_ns)
        {
            return Err(WindowError::AvailabilityRegression);
        }
        if self.records.len() == PER_WINDOW_CAPACITY {
            self.records.clear();
            self.invalid = true;
            return Err(WindowError::QueueDrop);
        }
        self.records.push_back(Timed {
            available_at_ns,
            value,
        });
        Ok(())
    }

    pub fn evict(&mut self, decision_time_ns: i64) -> Result<usize, WindowError> {
        let boundary = decision_time_ns
            .checked_sub(self.horizon_ns)
            .ok_or(WindowError::TimeOverflow)?;
        let mut removed = 0;
        while self
            .records
            .front()
            .is_some_and(|record| record.available_at_ns < boundary)
        {
            self.records.pop_front();
            removed += 1;
        }
        Ok(removed)
    }

    pub fn is_complete(&self, decision_time_ns: i64) -> Result<bool, WindowError> {
        let boundary = decision_time_ns
            .checked_sub(self.horizon_ns)
            .ok_or(WindowError::TimeOverflow)?;
        Ok(!self.invalid && self.epoch_first_available_ns <= boundary)
    }

    pub fn is_fresh(&self, decision_time_ns: i64, limit_ns: i64) -> Result<bool, WindowError> {
        let boundary = decision_time_ns
            .checked_sub(limit_ns)
            .ok_or(WindowError::TimeOverflow)?;
        Ok(!self.invalid
            && self
                .records
                .back()
                .is_some_and(|record| record.available_at_ns >= boundary))
    }

    pub fn records(&self) -> &VecDeque<Timed<T>> {
        &self.records
    }
    pub fn len(&self) -> usize {
        self.records.len()
    }
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
    pub fn is_invalid(&self) -> bool {
        self.invalid
    }
    pub fn clear_for_new_epoch(&mut self, epoch_first_available_ns: i64) {
        self.records.clear();
        self.epoch_first_available_ns = epoch_first_available_ns;
        self.invalid = false;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct WindowKey {
    pub source_id: String,
    pub horizon_ns: i64,
    pub kind: String,
}

#[derive(Debug, Clone)]
pub struct WindowBank<T> {
    windows: BTreeMap<WindowKey, FixedWindow<T>>,
    total_records: usize,
}

impl<T> Default for WindowBank<T> {
    fn default() -> Self {
        Self {
            windows: BTreeMap::new(),
            total_records: 0,
        }
    }
}

impl<T> WindowBank<T> {
    pub fn insert_window(
        &mut self,
        key: WindowKey,
        epoch_first_available_ns: i64,
    ) -> Result<(), WindowError> {
        if !SUPPORTED_HORIZONS_NS.contains(&key.horizon_ns) {
            return Err(WindowError::UnsupportedHorizon);
        }
        self.windows
            .entry(key.clone())
            .or_insert(FixedWindow::new(key.horizon_ns, epoch_first_available_ns)?);
        Ok(())
    }

    pub fn push(&mut self, key: &WindowKey, at_ns: i64, value: T) -> Result<(), WindowError> {
        let window = self
            .windows
            .get_mut(key)
            .ok_or(WindowError::UnsupportedHorizon)?;
        if self.total_records == PROCESSOR_RECORD_CAPACITY {
            self.total_records -= window.len();
            window.records.clear();
            window.invalid = true;
            return Err(WindowError::QueueDrop);
        }
        let before = window.len();
        let result = window.push(at_ns, value);
        match result {
            Ok(()) => self.total_records += 1,
            Err(WindowError::QueueDrop) => self.total_records -= before,
            Err(_) => {}
        }
        result
    }

    pub fn evict(&mut self, key: &WindowKey, decision_time_ns: i64) -> Result<(), WindowError> {
        let removed = self
            .windows
            .get_mut(key)
            .ok_or(WindowError::UnsupportedHorizon)?
            .evict(decision_time_ns)?;
        self.total_records -= removed;
        Ok(())
    }

    pub fn get(&self, key: &WindowKey) -> Option<&FixedWindow<T>> {
        self.windows.get(key)
    }
    pub fn total_records(&self) -> usize {
        self.total_records
    }
}
