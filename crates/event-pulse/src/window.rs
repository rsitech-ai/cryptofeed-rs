//! Bounded availability-time windows used by deterministic mechanics.

use std::collections::{BTreeMap, VecDeque};

use thiserror::Error;

pub const PER_WINDOW_CAPACITY: usize = 4_096;
pub const PROCESSOR_RECORD_CAPACITY: usize = 65_536;
pub const MAX_WINDOW_SOURCES: usize = 32;
pub const MAX_WINDOW_TOPOLOGY: usize = MAX_WINDOW_SOURCES * SUPPORTED_HORIZONS_NS.len() * 6;
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
    #[error("window topology contains an invalid or duplicate key")]
    InvalidTopology,
    #[error("window key is not preconfigured")]
    UnconfiguredKey,
    #[error("source epoch generation must strictly increase")]
    EpochNotGreater,
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
pub struct WindowSource(String);
impl WindowSource {
    pub fn new(value: &str) -> Result<Self, WindowError> {
        if value.is_empty() || value.len() > 128 || !value.is_ascii() {
            return Err(WindowError::InvalidTopology);
        }
        Ok(Self(value.to_owned()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WindowKind {
    Trade,
    Quote,
    Book,
    OpenInterest,
    Liquidation,
    ConfirmationPrice,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct WindowKey {
    source: WindowSource,
    horizon_ns: i64,
    kind: WindowKind,
}
impl WindowKey {
    pub fn new(
        source: WindowSource,
        horizon_ns: i64,
        kind: WindowKind,
    ) -> Result<Self, WindowError> {
        if !SUPPORTED_HORIZONS_NS.contains(&horizon_ns) {
            return Err(WindowError::UnsupportedHorizon);
        }
        Ok(Self {
            source,
            horizon_ns,
            kind,
        })
    }
    pub fn source(&self) -> &WindowSource {
        &self.source
    }
    pub fn horizon_ns(&self) -> i64 {
        self.horizon_ns
    }
    pub fn kind(&self) -> WindowKind {
        self.kind
    }
}

#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub key: WindowKey,
    pub epoch_generation: u8,
    pub epoch_first_available_ns: i64,
}

#[derive(Debug, Clone)]
pub struct WindowBank<T> {
    windows: BTreeMap<WindowKey, FixedWindow<T>>,
    source_generations: BTreeMap<WindowSource, u8>,
    total_records: usize,
}

impl<T> WindowBank<T> {
    pub fn new(specs: impl IntoIterator<Item = WindowSpec>) -> Result<Self, WindowError> {
        let mut windows = BTreeMap::new();
        let mut source_generations = BTreeMap::new();
        for spec in specs {
            if windows.len() == MAX_WINDOW_TOPOLOGY {
                return Err(WindowError::InvalidTopology);
            }
            if source_generations
                .get(&spec.key.source)
                .is_some_and(|generation| *generation != spec.epoch_generation)
            {
                return Err(WindowError::InvalidTopology);
            }
            source_generations.insert(spec.key.source.clone(), spec.epoch_generation);
            if source_generations.len() > MAX_WINDOW_SOURCES {
                return Err(WindowError::InvalidTopology);
            }
            let horizon = spec.key.horizon_ns;
            if windows
                .insert(
                    spec.key,
                    FixedWindow::new(horizon, spec.epoch_first_available_ns)?,
                )
                .is_some()
            {
                return Err(WindowError::InvalidTopology);
            }
        }
        if windows.is_empty() {
            return Err(WindowError::InvalidTopology);
        }
        Ok(Self {
            windows,
            source_generations,
            total_records: 0,
        })
    }

    pub fn push(&mut self, key: &WindowKey, at_ns: i64, value: T) -> Result<(), WindowError> {
        if !self.windows.contains_key(key) {
            return Err(WindowError::UnconfiguredKey);
        }
        if self.total_records == PROCESSOR_RECORD_CAPACITY {
            self.invalidate_source(&key.source);
            return Err(WindowError::QueueDrop);
        }
        let total_before = self.total_records;
        let source_records_before = self
            .windows
            .iter()
            .filter(|(candidate, _)| candidate.source == key.source)
            .map(|(_, window)| window.len())
            .sum::<usize>();
        let result = self
            .windows
            .get_mut(key)
            .expect("preconfigured key was checked")
            .push(at_ns, value);
        match result {
            Ok(()) => self.total_records += 1,
            Err(WindowError::QueueDrop) => {
                self.invalidate_source(&key.source);
                self.total_records = total_before - source_records_before;
            }
            Err(_) => {}
        }
        result
    }

    pub fn evict(&mut self, key: &WindowKey, decision_time_ns: i64) -> Result<(), WindowError> {
        let removed = self
            .windows
            .get_mut(key)
            .ok_or(WindowError::UnconfiguredKey)?
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

    pub(crate) fn invalidate_configured_source(
        &mut self,
        source: &WindowSource,
    ) -> Result<(), WindowError> {
        if !self.source_generations.contains_key(source) {
            return Err(WindowError::UnconfiguredKey);
        }
        self.invalidate_source(source);
        Ok(())
    }

    pub(crate) fn invalidate_configured_key(&mut self, key: &WindowKey) -> Result<(), WindowError> {
        let window = self
            .windows
            .get_mut(key)
            .ok_or(WindowError::UnconfiguredKey)?;
        self.total_records -= window.len();
        window.records.clear();
        window.invalid = true;
        Ok(())
    }

    pub(crate) fn recover_configured_key(
        &mut self,
        key: &WindowKey,
        epoch_first_available_ns: i64,
    ) -> Result<(), WindowError> {
        let window = self
            .windows
            .get_mut(key)
            .ok_or(WindowError::UnconfiguredKey)?;
        self.total_records -= window.len();
        window.clear_for_new_epoch(epoch_first_available_ns);
        Ok(())
    }

    pub fn advance_source_epoch(
        &mut self,
        source: &WindowSource,
        generation: u8,
        epoch_first_available_ns: i64,
    ) -> Result<(), WindowError> {
        let current = self
            .source_generations
            .get_mut(source)
            .ok_or(WindowError::UnconfiguredKey)?;
        if generation <= *current {
            return Err(WindowError::EpochNotGreater);
        }
        *current = generation;
        let mut removed = 0usize;
        for (key, window) in &mut self.windows {
            if &key.source == source {
                removed += window.len();
                window.clear_for_new_epoch(epoch_first_available_ns);
            }
        }
        self.total_records -= removed;
        Ok(())
    }

    fn invalidate_source(&mut self, source: &WindowSource) {
        let mut removed = 0usize;
        for (key, window) in &mut self.windows {
            if &key.source == source {
                removed += window.len();
                window.records.clear();
                window.invalid = true;
            }
        }
        self.total_records -= removed;
    }
}
