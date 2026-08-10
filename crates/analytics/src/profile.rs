use std::collections::{BTreeMap, BTreeSet};

use marketfeed_model::{Fixed, Price, Quantity};
use serde::{Deserialize, Serialize};

use crate::{
    AnalyticsError, GridSpec, PriceBucket, QuantityUnits, TimeframeSpec, invalid_config, overflow,
};

/// Activity used to select POC and expand the value area.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueAreaBasis {
    Volume,
    Tpo,
}

/// Validated profile limits and value-area semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileConfig {
    pub basis: ValueAreaBasis,
    pub value_area_bps: u16,
    pub max_levels: usize,
    pub max_tpo_levels_per_period: usize,
}

impl ProfileConfig {
    pub fn new(
        basis: ValueAreaBasis,
        value_area_bps: u16,
        max_levels: usize,
        max_tpo_levels_per_period: usize,
    ) -> Result<Self, AnalyticsError> {
        if !(1..=10_000).contains(&value_area_bps) {
            return Err(invalid_config(
                "value_area_bps",
                "must be between 1 and 10000",
            ));
        }
        if max_levels == 0 {
            return Err(invalid_config("max_levels", "must be greater than zero"));
        }
        if max_tpo_levels_per_period == 0 {
            return Err(invalid_config(
                "max_tpo_levels_per_period",
                "must be greater than zero",
            ));
        }
        if max_tpo_levels_per_period > max_levels {
            return Err(invalid_config(
                "max_tpo_levels_per_period",
                "must not exceed max_levels",
            ));
        }
        Ok(Self {
            basis,
            value_area_bps,
            max_levels,
            max_tpo_levels_per_period,
        })
    }

    pub fn validate(&self) -> Result<(), AnalyticsError> {
        Self::new(
            self.basis,
            self.value_area_bps,
            self.max_levels,
            self.max_tpo_levels_per_period,
        )
        .map(|_| ())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProfileState {
    Live,
    Final,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileLevel {
    pub price: Price,
    pub volume: Quantity,
    pub tpo_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionProfile {
    pub schema_version: u16,
    pub state: ProfileState,
    pub basis: ValueAreaBasis,
    pub value_area_bps: u16,
    pub start_ts: i64,
    pub end_ts: i64,
    pub high: Option<Price>,
    pub low: Option<Price>,
    pub range: Option<Fixed>,
    pub total_volume: Quantity,
    pub poc: Option<Price>,
    pub vah: Option<Price>,
    pub val: Option<Price>,
    pub tpo_count: u64,
    pub rotation_factor: i32,
    pub levels: Vec<ProfileLevel>,
}

/// Bounded exact session-profile accumulator.
///
/// Input timestamps must be nondecreasing. The builder never creates synthetic
/// activity for empty periods or sessions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionProfileBuilder {
    grid: GridSpec,
    time: TimeframeSpec,
    config: ProfileConfig,
    session_start: Option<i64>,
    current_tpo_start: Option<i64>,
    last_timestamp_ns: Option<i64>,
    session_high: Option<Price>,
    session_low: Option<Price>,
    volume_by_level: BTreeMap<PriceBucket, i128>,
    tpo_by_level: BTreeMap<PriceBucket, u64>,
    total_volume_units: i128,
    current_tpo_levels: BTreeSet<PriceBucket>,
    current_tpo_high: Option<PriceBucket>,
    current_tpo_low: Option<PriceBucket>,
    previous_tpo_high: Option<PriceBucket>,
    previous_tpo_low: Option<PriceBucket>,
    rotation_factor: i32,
}

impl SessionProfileBuilder {
    pub const SCHEMA_VERSION: u16 = 1;

    pub fn new(
        grid: GridSpec,
        time: TimeframeSpec,
        config: ProfileConfig,
    ) -> Result<Self, AnalyticsError> {
        grid.validate()?;
        time.validate()?;
        config.validate()?;
        Ok(Self {
            grid,
            time,
            config,
            session_start: None,
            current_tpo_start: None,
            last_timestamp_ns: None,
            session_high: None,
            session_low: None,
            volume_by_level: BTreeMap::new(),
            tpo_by_level: BTreeMap::new(),
            total_volume_units: 0,
            current_tpo_levels: BTreeSet::new(),
            current_tpo_high: None,
            current_tpo_low: None,
            previous_tpo_high: None,
            previous_tpo_low: None,
            rotation_factor: 0,
        })
    }

    pub fn grid(&self) -> GridSpec {
        self.grid
    }

    pub fn timeframes(&self) -> TimeframeSpec {
        self.time
    }

    pub fn config(&self) -> ProfileConfig {
        self.config
    }

    /// Ingest one exact trade and return a finalized prior session on rollover.
    pub fn ingest(
        &mut self,
        timestamp_ns: i64,
        price: Price,
        quantity: Quantity,
    ) -> Result<Option<SessionProfile>, AnalyticsError> {
        self.reject_late(timestamp_ns)?;
        let bucket = self.grid.price_bucket(price)?;
        let price = self.grid.price_at_tick(self.grid.price_tick(price)?)?;
        let quantity = self.grid.quantity_units(quantity)?;
        let target_session = self.time.checked_session_start(timestamp_ns)?;
        let target_tpo = self.time.checked_tpo_start(timestamp_ns)?;

        match self.session_start {
            None => {
                self.preflight_add(bucket, quantity, true)?;
                self.start_session(target_session, target_tpo);
                self.apply_add(bucket, price, quantity)?;
                self.last_timestamp_ns = Some(timestamp_ns);
                Ok(None)
            }
            Some(current_session) if target_session > current_session => {
                let finalized = self.finalized_clone()?;
                let mut replacement = Self::new(self.grid, self.time, self.config)?;
                replacement.preflight_add(bucket, quantity, true)?;
                replacement.start_session(target_session, target_tpo);
                replacement.apply_add(bucket, price, quantity)?;
                replacement.last_timestamp_ns = Some(timestamp_ns);
                *self = replacement;
                Ok(Some(finalized))
            }
            Some(current_session) if target_session < current_session => {
                Err(AnalyticsError::LateTrade {
                    timestamp_ns,
                    finalized_before_ns: current_session,
                })
            }
            Some(_) => {
                let current_tpo =
                    self.current_tpo_start
                        .ok_or_else(|| AnalyticsError::CorruptSnapshot {
                            detail: "active session is missing current TPO".to_owned(),
                        })?;
                if target_tpo < current_tpo {
                    return Err(AnalyticsError::LateTrade {
                        timestamp_ns,
                        finalized_before_ns: current_tpo,
                    });
                }
                self.preflight_add(bucket, quantity, target_tpo != current_tpo)?;
                if target_tpo > current_tpo {
                    self.preflight_close_current_tpo()?;
                    self.apply_close_current_tpo()?;
                    self.current_tpo_start = Some(target_tpo);
                }
                self.apply_add(bucket, price, quantity)?;
                self.last_timestamp_ns = Some(timestamp_ns);
                Ok(None)
            }
        }
    }

    /// Advance event time, closing elapsed TPO/session state without fabricating activity.
    pub fn advance_to(
        &mut self,
        timestamp_ns: i64,
    ) -> Result<Option<SessionProfile>, AnalyticsError> {
        self.reject_late(timestamp_ns)?;
        let Some(session_start) = self.session_start else {
            self.last_timestamp_ns = Some(timestamp_ns);
            return Ok(None);
        };
        let session_end = session_start
            .checked_add(self.time.session_ns)
            .ok_or_else(|| overflow("calculating session end"))?;
        if timestamp_ns >= session_end {
            self.preflight_close_current_tpo()?;
            self.apply_close_current_tpo()?;
            let finalized = self.snapshot(ProfileState::Final)?;
            self.clear_active();
            self.last_timestamp_ns = Some(timestamp_ns);
            return Ok(Some(finalized));
        }

        let target_tpo = self.time.checked_tpo_start(timestamp_ns)?;
        let current_tpo =
            self.current_tpo_start
                .ok_or_else(|| AnalyticsError::CorruptSnapshot {
                    detail: "active session is missing current TPO".to_owned(),
                })?;
        if target_tpo > current_tpo {
            self.preflight_close_current_tpo()?;
            self.apply_close_current_tpo()?;
            self.current_tpo_start = Some(target_tpo);
        }
        self.last_timestamp_ns = Some(timestamp_ns);
        Ok(None)
    }

    pub fn live_snapshot(&self) -> Result<Option<SessionProfile>, AnalyticsError> {
        if self.session_start.is_none() {
            return Ok(None);
        }
        let mut snapshot = self.clone();
        snapshot.preflight_close_current_tpo()?;
        snapshot.apply_close_current_tpo()?;
        snapshot.snapshot(ProfileState::Live).map(Some)
    }

    pub fn finish(&mut self) -> Result<Option<SessionProfile>, AnalyticsError> {
        if self.session_start.is_none() {
            return Ok(None);
        }
        self.preflight_close_current_tpo()?;
        self.apply_close_current_tpo()?;
        let finalized = self.snapshot(ProfileState::Final)?;
        self.clear_active();
        Ok(Some(finalized))
    }

    pub fn validate(&self) -> Result<(), AnalyticsError> {
        self.grid.validate()?;
        self.time.validate()?;
        self.config.validate()?;
        if self.volume_by_level.len() > self.config.max_levels
            || self.tpo_by_level.len() > self.config.max_levels
        {
            return Err(AnalyticsError::CorruptSnapshot {
                detail: "profile level count exceeds configured capacity".to_owned(),
            });
        }
        if self.current_tpo_levels.len() > self.config.max_tpo_levels_per_period {
            return Err(AnalyticsError::CorruptSnapshot {
                detail: "current TPO level count exceeds configured capacity".to_owned(),
            });
        }
        if self.total_volume_units < 0 || self.volume_by_level.values().any(|value| *value < 0) {
            return Err(AnalyticsError::CorruptSnapshot {
                detail: "negative profile volume".to_owned(),
            });
        }
        let summed = self
            .volume_by_level
            .values()
            .try_fold(0i128, |sum, value| {
                sum.checked_add(*value)
                    .ok_or_else(|| overflow("validating total profile volume"))
            })?;
        if summed != self.total_volume_units {
            return Err(AnalyticsError::CorruptSnapshot {
                detail: "profile total volume does not equal level sum".to_owned(),
            });
        }
        match (self.session_start, self.current_tpo_start) {
            (Some(session), Some(tpo))
                if tpo >= session
                    && tpo
                        < session
                            .checked_add(self.time.session_ns)
                            .ok_or_else(|| overflow("validating session end"))? => {}
            (None, None) => {}
            _ => {
                return Err(AnalyticsError::CorruptSnapshot {
                    detail: "session and TPO lifecycle state is inconsistent".to_owned(),
                });
            }
        }
        Ok(())
    }

    fn reject_late(&self, timestamp_ns: i64) -> Result<(), AnalyticsError> {
        if let Some(last) = self.last_timestamp_ns {
            if timestamp_ns < last {
                return Err(AnalyticsError::LateTrade {
                    timestamp_ns,
                    finalized_before_ns: last,
                });
            }
        }
        Ok(())
    }

    fn start_session(&mut self, session_start: i64, tpo_start: i64) {
        self.session_start = Some(session_start);
        self.current_tpo_start = Some(tpo_start);
    }

    fn preflight_add(
        &self,
        bucket: PriceBucket,
        quantity: QuantityUnits,
        starts_new_tpo: bool,
    ) -> Result<(), AnalyticsError> {
        if !self.volume_by_level.contains_key(&bucket)
            && self.volume_by_level.len() >= self.config.max_levels
        {
            return Err(AnalyticsError::CapacityExceeded {
                resource: "session profile levels",
                limit: self.config.max_levels,
            });
        }
        let tpo_len = if starts_new_tpo {
            0
        } else {
            self.current_tpo_levels.len()
        };
        let already_in_tpo = !starts_new_tpo && self.current_tpo_levels.contains(&bucket);
        if !already_in_tpo && tpo_len >= self.config.max_tpo_levels_per_period {
            return Err(AnalyticsError::CapacityExceeded {
                resource: "TPO levels per period",
                limit: self.config.max_tpo_levels_per_period,
            });
        }
        self.total_volume_units
            .checked_add(quantity.0)
            .ok_or_else(|| overflow("adding total profile volume"))?;
        self.volume_by_level
            .get(&bucket)
            .copied()
            .unwrap_or(0)
            .checked_add(quantity.0)
            .ok_or_else(|| overflow("adding profile level volume"))?;
        Ok(())
    }

    fn apply_add(
        &mut self,
        bucket: PriceBucket,
        price: Price,
        quantity: QuantityUnits,
    ) -> Result<(), AnalyticsError> {
        self.total_volume_units = self
            .total_volume_units
            .checked_add(quantity.0)
            .ok_or_else(|| overflow("adding total profile volume"))?;
        let level = self.volume_by_level.entry(bucket).or_insert(0);
        *level = level
            .checked_add(quantity.0)
            .ok_or_else(|| overflow("adding profile level volume"))?;
        self.session_high = Some(
            self.session_high
                .map_or(price, |current| crate::max_price(current, price)),
        );
        self.session_low = Some(
            self.session_low
                .map_or(price, |current| crate::min_price(current, price)),
        );
        self.current_tpo_levels.insert(bucket);
        self.current_tpo_high = Some(
            self.current_tpo_high
                .map_or(bucket, |current| current.max(bucket)),
        );
        self.current_tpo_low = Some(
            self.current_tpo_low
                .map_or(bucket, |current| current.min(bucket)),
        );
        Ok(())
    }

    fn preflight_close_current_tpo(&self) -> Result<(), AnalyticsError> {
        for bucket in &self.current_tpo_levels {
            self.tpo_by_level
                .get(bucket)
                .copied()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| overflow("adding a TPO count"))?;
        }
        let delta = self.rotation_delta();
        self.rotation_factor
            .checked_add(delta)
            .ok_or_else(|| overflow("adding rotation factor"))?;
        Ok(())
    }

    fn apply_close_current_tpo(&mut self) -> Result<(), AnalyticsError> {
        if self.current_tpo_levels.is_empty() {
            self.current_tpo_high = None;
            self.current_tpo_low = None;
            return Ok(());
        }
        for bucket in &self.current_tpo_levels {
            let count = self.tpo_by_level.entry(*bucket).or_insert(0);
            *count = count
                .checked_add(1)
                .ok_or_else(|| overflow("adding a TPO count"))?;
        }
        self.rotation_factor = self
            .rotation_factor
            .checked_add(self.rotation_delta())
            .ok_or_else(|| overflow("adding rotation factor"))?;
        self.previous_tpo_high = self.current_tpo_high;
        self.previous_tpo_low = self.current_tpo_low;
        self.current_tpo_levels.clear();
        self.current_tpo_high = None;
        self.current_tpo_low = None;
        Ok(())
    }

    fn rotation_delta(&self) -> i32 {
        match (
            self.previous_tpo_high,
            self.previous_tpo_low,
            self.current_tpo_high,
            self.current_tpo_low,
        ) {
            (Some(previous_high), Some(previous_low), Some(high), Some(low)) => {
                ordering_score(high, previous_high) + ordering_score(low, previous_low)
            }
            _ => 0,
        }
    }

    fn finalized_clone(&self) -> Result<SessionProfile, AnalyticsError> {
        let mut old = self.clone();
        old.preflight_close_current_tpo()?;
        old.apply_close_current_tpo()?;
        old.snapshot(ProfileState::Final)
    }

    fn snapshot(&self, state: ProfileState) -> Result<SessionProfile, AnalyticsError> {
        let start_ts = self
            .session_start
            .ok_or_else(|| AnalyticsError::CorruptSnapshot {
                detail: "cannot snapshot an inactive profile".to_owned(),
            })?;
        let end_ts = start_ts
            .checked_add(self.time.session_ns)
            .ok_or_else(|| overflow("calculating session end"))?;
        let low = self.session_low;
        let high = self.session_high;
        let range = match (low, high) {
            (Some(low), Some(high)) => {
                let coefficient = high
                    .0
                    .coefficient
                    .checked_sub(low.0.coefficient)
                    .ok_or_else(|| overflow("calculating session range"))?;
                Some(Fixed::new(coefficient, self.grid.price_scale))
            }
            _ => None,
        };
        let (poc_bucket, val_bucket, vah_bucket) = self.value_area()?;
        let levels = self
            .volume_by_level
            .iter()
            .map(|(bucket, units)| {
                Ok(ProfileLevel {
                    price: self.grid.price_at(*bucket)?,
                    volume: self.grid.quantity_at(QuantityUnits(*units))?,
                    tpo_count: self.tpo_by_level.get(bucket).copied().unwrap_or(0),
                })
            })
            .collect::<Result<Vec<_>, AnalyticsError>>()?;
        let tpo_count = self.tpo_by_level.values().try_fold(0u64, |sum, count| {
            sum.checked_add(*count)
                .ok_or_else(|| overflow("summing session TPO count"))
        })?;
        Ok(SessionProfile {
            schema_version: Self::SCHEMA_VERSION,
            state,
            basis: self.config.basis,
            value_area_bps: self.config.value_area_bps,
            start_ts,
            end_ts,
            high,
            low,
            range,
            total_volume: self
                .grid
                .quantity_at(QuantityUnits(self.total_volume_units))?,
            poc: poc_bucket
                .map(|bucket| self.grid.price_at(bucket))
                .transpose()?,
            vah: vah_bucket
                .map(|bucket| self.grid.price_at(bucket))
                .transpose()?,
            val: val_bucket
                .map(|bucket| self.grid.price_at(bucket))
                .transpose()?,
            tpo_count,
            rotation_factor: self.rotation_factor,
            levels,
        })
    }

    fn value_area(
        &self,
    ) -> Result<
        (
            Option<PriceBucket>,
            Option<PriceBucket>,
            Option<PriceBucket>,
        ),
        AnalyticsError,
    > {
        if self.volume_by_level.is_empty() {
            return Ok((None, None, None));
        }
        let buckets = self.volume_by_level.keys().copied().collect::<Vec<_>>();
        let low = buckets[0];
        let high = *buckets
            .last()
            .ok_or_else(|| AnalyticsError::CorruptSnapshot {
                detail: "non-empty profile has no high bucket".to_owned(),
            })?;
        let midpoint_twice = low
            .0
            .checked_add(high.0)
            .ok_or_else(|| overflow("calculating POC midpoint"))?;
        let mut poc_index = 0usize;
        let mut poc_activity = -1i128;
        let mut poc_distance = u128::MAX;
        for (index, bucket) in buckets.iter().enumerate() {
            let activity = self.activity(*bucket)?;
            let distance = bucket
                .0
                .checked_mul(2)
                .and_then(|twice| twice.checked_sub(midpoint_twice))
                .ok_or_else(|| overflow("calculating POC tie distance"))?
                .unsigned_abs();
            if activity > poc_activity
                || (activity == poc_activity && distance < poc_distance)
                || (activity == poc_activity
                    && distance == poc_distance
                    && bucket.0 < buckets[poc_index].0)
            {
                poc_index = index;
                poc_activity = activity;
                poc_distance = distance;
            }
        }

        let total_activity = buckets.iter().try_fold(0i128, |total, bucket| {
            total
                .checked_add(self.activity(*bucket)?)
                .ok_or_else(|| overflow("summing value-area activity"))
        })?;
        let scaled_target = total_activity
            .checked_mul(i128::from(self.config.value_area_bps))
            .ok_or_else(|| overflow("calculating value-area target"))?;
        let target = scaled_target
            .checked_add(9_999)
            .ok_or_else(|| overflow("rounding value-area target"))?
            / 10_000;

        let mut selected = poc_activity;
        let mut lower = poc_index;
        let mut upper = poc_index;
        while selected < target {
            let lower_activity = if lower > 0 {
                Some(self.activity(buckets[lower - 1])?)
            } else {
                None
            };
            let upper_activity = if upper + 1 < buckets.len() {
                Some(self.activity(buckets[upper + 1])?)
            } else {
                None
            };
            match (lower_activity, upper_activity) {
                (Some(lower_value), Some(upper_value)) if lower_value == upper_value => {
                    lower -= 1;
                    selected = selected
                        .checked_add(lower_value)
                        .ok_or_else(|| overflow("expanding value area"))?;
                    if selected < target {
                        upper += 1;
                        selected = selected
                            .checked_add(upper_value)
                            .ok_or_else(|| overflow("expanding value area"))?;
                    }
                }
                (Some(lower_value), Some(upper_value)) if lower_value > upper_value => {
                    lower -= 1;
                    selected = selected
                        .checked_add(lower_value)
                        .ok_or_else(|| overflow("expanding value area"))?;
                }
                (Some(_), Some(upper_value)) => {
                    upper += 1;
                    selected = selected
                        .checked_add(upper_value)
                        .ok_or_else(|| overflow("expanding value area"))?;
                }
                (Some(lower_value), None) => {
                    lower -= 1;
                    selected = selected
                        .checked_add(lower_value)
                        .ok_or_else(|| overflow("expanding value area"))?;
                }
                (None, Some(upper_value)) => {
                    upper += 1;
                    selected = selected
                        .checked_add(upper_value)
                        .ok_or_else(|| overflow("expanding value area"))?;
                }
                (None, None) => break,
            }
        }
        Ok((
            Some(buckets[poc_index]),
            Some(buckets[lower]),
            Some(buckets[upper]),
        ))
    }

    fn activity(&self, bucket: PriceBucket) -> Result<i128, AnalyticsError> {
        match self.config.basis {
            ValueAreaBasis::Volume => Ok(self.volume_by_level.get(&bucket).copied().unwrap_or(0)),
            ValueAreaBasis::Tpo => Ok(i128::from(
                self.tpo_by_level.get(&bucket).copied().unwrap_or(0),
            )),
        }
    }

    fn clear_active(&mut self) {
        self.session_start = None;
        self.current_tpo_start = None;
        self.session_high = None;
        self.session_low = None;
        self.volume_by_level.clear();
        self.tpo_by_level.clear();
        self.total_volume_units = 0;
        self.current_tpo_levels.clear();
        self.current_tpo_high = None;
        self.current_tpo_low = None;
        self.previous_tpo_high = None;
        self.previous_tpo_low = None;
        self.rotation_factor = 0;
    }
}

fn ordering_score(current: PriceBucket, previous: PriceBucket) -> i32 {
    match current.cmp(&previous) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}
