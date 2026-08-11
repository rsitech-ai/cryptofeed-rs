//! Deterministic, bounded, strict-priority order-flow bubble detection.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use marketfeed_model::{Fixed, InstrumentId, Price, Quantity, VenueId};
use serde::{Deserialize, Serialize};

use crate::{
    AnalyticsError, CandleFlow, FlowSource, FlowState, GridSpec, MarketSegment, PriceBucket,
    QuantityUnits, SourceSelector, invalid_config,
};

/// Bubble signal strength mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BubbleMode {
    Volume,
    Delta,
}

/// Strict priority tier. F3 always outranks F2, which outranks F1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum BubbleTier {
    F1,
    F2,
    F3,
}

impl BubbleTier {
    pub const fn priority(self) -> u8 {
        match self {
            Self::F1 => 1,
            Self::F2 => 2,
            Self::F3 => 3,
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::F1 => 0,
            Self::F2 => 1,
            Self::F3 => 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BubbleShape {
    Circle,
    Square,
    Diamond,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LabelMode {
    Off,
    Raw,
}

/// Rendering metadata. Colors remain a UI concern; size and shape are deterministic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BubbleStyle {
    pub shape: BubbleShape,
    pub min_size: u16,
    pub max_size: u16,
    pub volume_cap: Quantity,
    pub delta_cap: Quantity,
    pub label_mode: LabelMode,
}

impl BubbleStyle {
    pub fn new(
        shape: BubbleShape,
        min_size: u16,
        max_size: u16,
        volume_cap: Quantity,
        delta_cap: Quantity,
        label_mode: LabelMode,
    ) -> Result<Self, AnalyticsError> {
        if min_size == 0 || max_size < min_size {
            return Err(invalid_config(
                "bubble size",
                "min_size must be positive and no greater than max_size",
            ));
        }
        if volume_cap.0.coefficient <= 0 || delta_cap.0.coefficient <= 0 {
            return Err(invalid_config(
                "bubble strength cap",
                "volume and delta caps must be positive",
            ));
        }
        Ok(Self {
            shape,
            min_size,
            max_size,
            volume_cap,
            delta_cap,
            label_mode,
        })
    }

    fn validate(&self) -> Result<(), AnalyticsError> {
        Self::new(
            self.shape,
            self.min_size,
            self.max_size,
            self.volume_cap,
            self.delta_cap,
            self.label_mode,
        )
        .map(|_| ())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdaptivePreset {
    UltraPermissive,
    Permissive,
    Balanced,
    Strict,
    VeryStrict,
    UltraStrict,
    Extreme,
    Custom,
}

impl AdaptivePreset {
    const fn percentile_bps(self, custom: u16) -> u16 {
        match self {
            Self::UltraPermissive => 4_500,
            Self::Permissive => 6_000,
            Self::Balanced => 7_500,
            Self::Strict => 8_500,
            Self::VeryStrict => 9_000,
            Self::UltraStrict => 9_500,
            Self::Extreme => 9_900,
            Self::Custom => custom,
        }
    }
}

/// Bounded adaptive threshold configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveThreshold {
    pub preset: AdaptivePreset,
    pub custom_percentile_bps: u16,
    pub minimum_floor: Option<Quantity>,
    pub minimum_samples: usize,
    pub calibration_candles: usize,
    pub outlier_cap_percentile_bps: u16,
    pub spike_adaptation_bps: u16,
}

impl AdaptiveThreshold {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        preset: AdaptivePreset,
        custom_percentile_bps: u16,
        minimum_floor: Option<Quantity>,
        minimum_samples: usize,
        calibration_candles: usize,
        outlier_cap_percentile_bps: u16,
        spike_adaptation_bps: u16,
    ) -> Result<Self, AnalyticsError> {
        for (field, value) in [
            ("custom_percentile_bps", custom_percentile_bps),
            ("outlier_cap_percentile_bps", outlier_cap_percentile_bps),
            ("spike_adaptation_bps", spike_adaptation_bps),
        ] {
            if value > 10_000 {
                return Err(invalid_config(field, "must be between 0 and 10,000"));
            }
        }
        if minimum_samples == 0 || calibration_candles == 0 {
            return Err(invalid_config(
                "adaptive history",
                "minimum_samples and calibration_candles must be positive",
            ));
        }
        if minimum_floor.is_some_and(|value| value.0.coefficient <= 0) {
            return Err(invalid_config(
                "minimum_floor",
                "must be positive when configured",
            ));
        }
        Ok(Self {
            preset,
            custom_percentile_bps,
            minimum_floor,
            minimum_samples,
            calibration_candles,
            outlier_cap_percentile_bps,
            spike_adaptation_bps,
        })
    }

    fn validate(&self) -> Result<(), AnalyticsError> {
        Self::new(
            self.preset,
            self.custom_percentile_bps,
            self.minimum_floor,
            self.minimum_samples,
            self.calibration_candles,
            self.outlier_cap_percentile_bps,
            self.spike_adaptation_bps,
        )
        .map(|_| ())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ThresholdMode {
    Off,
    Manual(Quantity),
    Adaptive(AdaptiveThreshold),
}

/// One independent filter tier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BubbleFilter {
    pub tier: BubbleTier,
    pub mode: BubbleMode,
    pub threshold: ThresholdMode,
    pub sources: SourceSelector,
    pub max_bubbles_per_candle: usize,
    pub style: BubbleStyle,
}

impl BubbleFilter {
    pub fn new(
        tier: BubbleTier,
        mode: BubbleMode,
        threshold: ThresholdMode,
        sources: SourceSelector,
        max_bubbles_per_candle: usize,
        style: BubbleStyle,
    ) -> Result<Self, AnalyticsError> {
        if max_bubbles_per_candle == 0 {
            return Err(invalid_config("max_bubbles_per_candle", "must be positive"));
        }
        if matches!(threshold, ThresholdMode::Manual(value) if value.0.coefficient <= 0) {
            return Err(invalid_config("manual threshold", "must be positive"));
        }
        if let ThresholdMode::Adaptive(config) = &threshold {
            config.validate()?;
        }
        sources.validate()?;
        style.validate()?;
        Ok(Self {
            tier,
            mode,
            threshold,
            sources,
            max_bubbles_per_candle,
            style,
        })
    }

    fn validate(&self, expected: BubbleTier) -> Result<(), AnalyticsError> {
        if self.tier != expected {
            return Err(invalid_config(
                "bubble tier",
                "filter tier does not match its configuration slot",
            ));
        }
        Self::new(
            self.tier,
            self.mode,
            self.threshold.clone(),
            self.sources.clone(),
            self.max_bubbles_per_candle,
            self.style.clone(),
        )
        .map(|_| ())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeConfig {
    pub enabled_tiers: [bool; 3],
    pub max_distance_buckets: u32,
    pub style: BubbleStyle,
}

impl MergeConfig {
    pub fn new(
        enabled_tiers: [bool; 3],
        max_distance_buckets: u32,
        style: BubbleStyle,
    ) -> Result<Self, AnalyticsError> {
        style.validate()?;
        Ok(Self {
            enabled_tiers,
            max_distance_buckets,
            style,
        })
    }

    pub fn disabled() -> Self {
        Self {
            enabled_tiers: [false; 3],
            max_distance_buckets: 0,
            style: BubbleStyle {
                shape: BubbleShape::Circle,
                min_size: 8,
                max_size: 48,
                volume_cap: Quantity(Fixed::new(1, 0)),
                delta_cap: Quantity(Fixed::new(1, 0)),
                label_mode: LabelMode::Off,
            },
        }
    }

    fn enabled(&self, tier: BubbleTier) -> bool {
        self.enabled_tiers[tier.index()]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PerformanceMode {
    Full,
    High {
        live_tier_count: u8,
        defer_merging: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BubbleConfig {
    pub f1: BubbleFilter,
    pub f2: BubbleFilter,
    pub f3: BubbleFilter,
    pub merge: MergeConfig,
    pub performance: PerformanceMode,
    pub max_history_candles: usize,
}

impl BubbleConfig {
    pub fn new(
        f1: BubbleFilter,
        f2: BubbleFilter,
        f3: BubbleFilter,
        merge: MergeConfig,
        performance: PerformanceMode,
        max_history_candles: usize,
    ) -> Result<Self, AnalyticsError> {
        if max_history_candles == 0 {
            return Err(invalid_config("max_history_candles", "must be positive"));
        }
        f1.validate(BubbleTier::F1)?;
        f2.validate(BubbleTier::F2)?;
        f3.validate(BubbleTier::F3)?;
        merge.style.validate()?;
        if matches!(
            performance,
            PerformanceMode::High {
                live_tier_count: 0 | 4..,
                ..
            }
        ) {
            return Err(invalid_config(
                "live_tier_count",
                "must be between one and three",
            ));
        }
        Ok(Self {
            f1,
            f2,
            f3,
            merge,
            performance,
            max_history_candles,
        })
    }

    pub fn validate(&self) -> Result<(), AnalyticsError> {
        Self::new(
            self.f1.clone(),
            self.f2.clone(),
            self.f3.clone(),
            self.merge.clone(),
            self.performance,
            self.max_history_candles,
        )
        .map(|_| ())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DetectionPhase {
    Live,
    Final,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum BubbleDirection {
    Sell,
    Neutral,
    Buy,
}

/// Rendering-neutral deterministic bubble output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderFlowBubble {
    pub id: u64,
    pub instrument: InstrumentId,
    pub candle_start_ns: i64,
    pub candle_end_ns: i64,
    pub segment: MarketSegment,
    pub sources: Vec<VenueId>,
    pub tier: BubbleTier,
    pub mode: BubbleMode,
    pub direction: BubbleDirection,
    pub anchor_price: Price,
    pub low_price: Price,
    pub high_price: Price,
    pub buy_volume: Quantity,
    pub sell_volume: Quantity,
    pub unknown_volume: Quantity,
    pub total_volume: Quantity,
    pub delta: Fixed,
    pub strength: Fixed,
    pub threshold: Fixed,
    pub visual_size: u16,
    pub label: Option<Fixed>,
    pub shape: BubbleShape,
    pub merged_count: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BubbleBatch {
    pub candle_start_ns: i64,
    pub candle_end_ns: i64,
    pub phase: DetectionPhase,
    pub bubbles: Vec<OrderFlowBubble>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct CandidateKey {
    segment: MarketSegment,
    bucket: PriceBucket,
}

#[derive(Debug, Clone)]
struct Aggregate {
    segment: MarketSegment,
    bucket: PriceBucket,
    sources: Vec<VenueId>,
    buy_units: i128,
    sell_units: i128,
    unknown_units: i128,
    total_units: i128,
}

impl Aggregate {
    fn delta_units(&self) -> Result<i128, AnalyticsError> {
        self.buy_units
            .checked_sub(self.sell_units)
            .ok_or_else(|| crate::overflow("calculating bubble delta"))
    }

    fn strength_units(&self, mode: BubbleMode) -> Result<u128, AnalyticsError> {
        let value = match mode {
            BubbleMode::Volume => self.total_units,
            BubbleMode::Delta => self
                .delta_units()?
                .checked_abs()
                .ok_or_else(|| crate::overflow("calculating absolute bubble delta"))?,
        };
        u128::try_from(value).map_err(|_| crate::overflow("converting bubble strength"))
    }

    fn direction(&self) -> Result<BubbleDirection, AnalyticsError> {
        Ok(match self.delta_units()?.cmp(&0) {
            std::cmp::Ordering::Less => BubbleDirection::Sell,
            std::cmp::Ordering::Equal => BubbleDirection::Neutral,
            std::cmp::Ordering::Greater => BubbleDirection::Buy,
        })
    }
}

/// Stateful detector. Only `record_finalized` mutates adaptive history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BubbleDetector {
    grid: GridSpec,
    config: BubbleConfig,
    history: VecDeque<Arc<CandleFlow>>,
    last_finalized_start_ns: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
enum ThresholdBaseline {
    Off,
    Manual(u128),
    FloorOnly(Option<u128>),
    Adaptive {
        floor: u128,
        cap: u128,
        historical: u128,
        historical_weight_bps: u16,
        spike_weight_bps: u16,
    },
}

impl BubbleDetector {
    pub fn new(grid: GridSpec, config: BubbleConfig) -> Result<Self, AnalyticsError> {
        grid.validate()?;
        config.validate()?;
        Ok(Self {
            grid,
            config,
            history: VecDeque::new(),
            last_finalized_start_ns: None,
        })
    }

    pub fn config(&self) -> &BubbleConfig {
        &self.config
    }

    pub fn history_len(&self) -> usize {
        self.history.len()
    }

    pub fn detect(
        &self,
        candle: &CandleFlow,
        phase: DetectionPhase,
    ) -> Result<Vec<OrderFlowBubble>, AnalyticsError> {
        let keys = candidate_keys(candle, &self.grid)?;
        let active_tiers = self.active_tiers(phase);
        let mut by_tier: [Vec<OrderFlowBubble>; 3] = [Vec::new(), Vec::new(), Vec::new()];
        let mut baselines = BTreeMap::new();

        for key in keys {
            for tier in active_tiers.iter().copied() {
                let filter = self.filter(tier);
                if matches!(filter.threshold, ThresholdMode::Off) {
                    continue;
                }
                let aggregate = aggregate_for(candle, key, &filter.sources, &self.grid)?;
                if aggregate.sources.is_empty() {
                    continue;
                }
                let strength = aggregate.strength_units(filter.mode)?;
                if filter.mode == BubbleMode::Delta && strength == 0 {
                    continue;
                }
                let baseline_key = (tier, key.segment);
                let baseline = match baselines.entry(baseline_key) {
                    std::collections::btree_map::Entry::Occupied(entry) => *entry.get(),
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        *entry.insert(self.threshold_baseline(filter, key.segment)?)
                    }
                };
                let Some(threshold) = Self::threshold_units(baseline, strength)? else {
                    continue;
                };
                if strength >= threshold {
                    by_tier[tier.index()].push(
                        self.materialize(candle, tier, filter, aggregate, strength, threshold)?,
                    );
                    break;
                }
            }
        }

        for tier in [BubbleTier::F1, BubbleTier::F2, BubbleTier::F3] {
            let rows = &mut by_tier[tier.index()];
            rows.sort_by(|left, right| {
                right
                    .strength
                    .coefficient
                    .cmp(&left.strength.coefficient)
                    .then_with(|| crate::compare_price(left.anchor_price, right.anchor_price))
                    .then_with(|| left.id.cmp(&right.id))
            });
            rows.truncate(self.filter(tier).max_bubbles_per_candle);
        }

        let mut output = Vec::new();
        output.append(&mut by_tier[BubbleTier::F3.index()]);
        output.append(&mut by_tier[BubbleTier::F2.index()]);
        output.append(&mut by_tier[BubbleTier::F1.index()]);

        let defer_merging = matches!(
            self.config.performance,
            PerformanceMode::High {
                defer_merging: true,
                ..
            }
        ) && phase == DetectionPhase::Live;
        if !defer_merging && self.config.merge.enabled_tiers.iter().any(|value| *value) {
            output = self.merge(output)?;
        }
        output.sort_by(|left, right| {
            right
                .tier
                .priority()
                .cmp(&left.tier.priority())
                .then_with(|| right.strength.coefficient.cmp(&left.strength.coefficient))
                .then_with(|| left.segment.cmp(&right.segment))
                .then_with(|| crate::compare_price(left.anchor_price, right.anchor_price))
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(output)
    }

    pub fn detect_batch(
        &self,
        candle: &CandleFlow,
        phase: DetectionPhase,
    ) -> Result<BubbleBatch, AnalyticsError> {
        Ok(BubbleBatch {
            candle_start_ns: candle.start_ts,
            candle_end_ns: candle.end_ts,
            phase,
            bubbles: self.detect(candle, phase)?,
        })
    }

    /// Records one strictly newer finalized candle atomically.
    pub fn record_finalized(&mut self, candle: &CandleFlow) -> Result<(), AnalyticsError> {
        self.record_finalized_shared(Arc::new(candle.clone()))
    }

    /// Records a shared immutable candle so detectors with different modes can
    /// reuse the same bounded price-level history.
    pub fn record_finalized_shared(
        &mut self,
        candle: Arc<CandleFlow>,
    ) -> Result<(), AnalyticsError> {
        if candle.state != FlowState::Final {
            return Err(invalid_config(
                "finalized bubble history",
                "only finalized candles may be recorded",
            ));
        }
        if self
            .last_finalized_start_ns
            .is_some_and(|last| candle.start_ts <= last)
        {
            return Err(AnalyticsError::LateTrade {
                timestamp_ns: candle.start_ts,
                finalized_before_ns: self.last_finalized_start_ns.unwrap_or(candle.start_ts),
            });
        }
        // All fallible validation is complete above. Mutating in place keeps
        // the append atomic without cloning the detector's full history on
        // every candle rollover.
        let candle_start_ns = candle.start_ts;
        self.history.push_back(candle);
        while self.history.len() > self.config.max_history_candles {
            self.history.pop_front();
        }
        self.last_finalized_start_ns = Some(candle_start_ns);
        Ok(())
    }

    pub fn validate(&self) -> Result<(), AnalyticsError> {
        self.grid.validate()?;
        self.config.validate()?;
        if self.history.len() > self.config.max_history_candles {
            return Err(AnalyticsError::CorruptSnapshot {
                detail: "bubble history exceeds capacity".to_owned(),
            });
        }
        let mut previous = None;
        for candle in &self.history {
            if candle.state != FlowState::Final {
                return Err(AnalyticsError::CorruptSnapshot {
                    detail: "bubble history contains a live candle".to_owned(),
                });
            }
            if previous.is_some_and(|value| value >= candle.start_ts) {
                return Err(AnalyticsError::CorruptSnapshot {
                    detail: "bubble history is not strictly ordered".to_owned(),
                });
            }
            previous = Some(candle.start_ts);
        }
        Ok(())
    }

    fn active_tiers(&self, phase: DetectionPhase) -> Vec<BubbleTier> {
        let tiers = [BubbleTier::F3, BubbleTier::F2, BubbleTier::F1];
        match (phase, self.config.performance) {
            (
                DetectionPhase::Live,
                PerformanceMode::High {
                    live_tier_count, ..
                },
            ) => tiers[..usize::from(live_tier_count)].to_vec(),
            _ => tiers.to_vec(),
        }
    }

    const fn filter(&self, tier: BubbleTier) -> &BubbleFilter {
        match tier {
            BubbleTier::F1 => &self.config.f1,
            BubbleTier::F2 => &self.config.f2,
            BubbleTier::F3 => &self.config.f3,
        }
    }

    fn threshold_baseline(
        &self,
        filter: &BubbleFilter,
        segment: MarketSegment,
    ) -> Result<ThresholdBaseline, AnalyticsError> {
        match &filter.threshold {
            ThresholdMode::Off => Ok(ThresholdBaseline::Off),
            ThresholdMode::Manual(quantity) => {
                let units = self.grid.quantity_units(*quantity)?.0;
                Ok(ThresholdBaseline::Manual(u128::try_from(units).map_err(
                    |_| crate::overflow("converting manual threshold"),
                )?))
            }
            ThresholdMode::Adaptive(config) => {
                let floor = config
                    .minimum_floor
                    .map(|quantity| self.grid.quantity_units(quantity))
                    .transpose()?
                    .map(|units| {
                        u128::try_from(units.0)
                            .map_err(|_| crate::overflow("converting adaptive floor"))
                    })
                    .transpose()?
                    .unwrap_or(0);
                let mut samples = Vec::new();
                for candle in self.history.iter().rev().take(config.calibration_candles) {
                    for key in candidate_keys(candle, &self.grid)? {
                        if key.segment != segment {
                            continue;
                        }
                        let aggregate = aggregate_for(candle, key, &filter.sources, &self.grid)?;
                        if aggregate.sources.is_empty() {
                            continue;
                        }
                        let strength = aggregate.strength_units(filter.mode)?;
                        if filter.mode != BubbleMode::Delta || strength > 0 {
                            samples.push(strength);
                        }
                    }
                }
                if samples.len() < config.minimum_samples {
                    return Ok(ThresholdBaseline::FloorOnly((floor > 0).then_some(floor)));
                }
                samples.sort_unstable();
                let cap = percentile(&samples, config.outlier_cap_percentile_bps)?;
                for sample in &mut samples {
                    *sample = (*sample).min(cap);
                }
                samples.sort_unstable();
                let historical = percentile(
                    &samples,
                    config.preset.percentile_bps(config.custom_percentile_bps),
                )?;
                Ok(ThresholdBaseline::Adaptive {
                    floor,
                    cap,
                    historical,
                    historical_weight_bps: 10_000_u16.saturating_sub(config.spike_adaptation_bps),
                    spike_weight_bps: config.spike_adaptation_bps,
                })
            }
        }
    }

    fn threshold_units(
        baseline: ThresholdBaseline,
        current_strength: u128,
    ) -> Result<Option<u128>, AnalyticsError> {
        match baseline {
            ThresholdBaseline::Off => Ok(None),
            ThresholdBaseline::Manual(threshold) => Ok(Some(threshold)),
            ThresholdBaseline::FloorOnly(threshold) => Ok(threshold),
            ThresholdBaseline::Adaptive {
                floor,
                cap,
                historical,
                historical_weight_bps,
                spike_weight_bps,
            } => Ok(Some(
                weighted_bps(
                    historical,
                    historical_weight_bps,
                    current_strength.min(cap),
                    spike_weight_bps,
                )?
                .max(floor),
            )),
        }
    }

    fn materialize(
        &self,
        candle: &CandleFlow,
        tier: BubbleTier,
        filter: &BubbleFilter,
        aggregate: Aggregate,
        strength: u128,
        threshold: u128,
    ) -> Result<OrderFlowBubble, AnalyticsError> {
        let delta_units = aggregate.delta_units()?;
        let cap_quantity = match filter.mode {
            BubbleMode::Volume => filter.style.volume_cap,
            BubbleMode::Delta => filter.style.delta_cap,
        };
        let cap = u128::try_from(self.grid.quantity_units(cap_quantity)?.0)
            .map_err(|_| crate::overflow("converting bubble strength cap"))?;
        let strength_i128 =
            i128::try_from(strength).map_err(|_| crate::overflow("converting bubble strength"))?;
        let threshold_i128 = i128::try_from(threshold)
            .map_err(|_| crate::overflow("converting bubble threshold"))?;
        let anchor_price = self.grid.price_at(aggregate.bucket)?;
        let label = match filter.style.label_mode {
            LabelMode::Off => None,
            LabelMode::Raw => Some(match filter.mode {
                BubbleMode::Volume => Fixed::new(strength_i128, self.grid.quantity_scale),
                BubbleMode::Delta => Fixed::new(delta_units, self.grid.quantity_scale),
            }),
        };
        Ok(OrderFlowBubble {
            id: bubble_id(
                candle.start_ts,
                candle.instrument,
                aggregate.segment,
                aggregate.bucket,
                tier,
            ),
            instrument: candle.instrument,
            candle_start_ns: candle.start_ts,
            candle_end_ns: candle.end_ts,
            segment: aggregate.segment,
            tier,
            mode: filter.mode,
            direction: aggregate.direction()?,
            anchor_price,
            low_price: anchor_price,
            high_price: anchor_price,
            buy_volume: self.grid.quantity_at(QuantityUnits(aggregate.buy_units))?,
            sell_volume: self.grid.quantity_at(QuantityUnits(aggregate.sell_units))?,
            unknown_volume: self
                .grid
                .quantity_at(QuantityUnits(aggregate.unknown_units))?,
            total_volume: self
                .grid
                .quantity_at(QuantityUnits(aggregate.total_units))?,
            delta: Fixed::new(delta_units, self.grid.quantity_scale),
            strength: Fixed::new(strength_i128, self.grid.quantity_scale),
            threshold: Fixed::new(threshold_i128, self.grid.quantity_scale),
            visual_size: scaled_size(strength, cap, filter.style.min_size, filter.style.max_size)?,
            sources: aggregate.sources,
            label,
            shape: filter.style.shape,
            merged_count: 1,
        })
    }

    fn merge(
        &self,
        mut bubbles: Vec<OrderFlowBubble>,
    ) -> Result<Vec<OrderFlowBubble>, AnalyticsError> {
        bubbles.sort_by(|left, right| {
            left.segment
                .cmp(&right.segment)
                .then_with(|| left.direction.cmp(&right.direction))
                .then_with(|| crate::compare_price(left.anchor_price, right.anchor_price))
                .then_with(|| right.tier.priority().cmp(&left.tier.priority()))
                .then_with(|| left.id.cmp(&right.id))
        });
        let mut out = Vec::with_capacity(bubbles.len());
        let mut index = 0;
        while index < bubbles.len() {
            if !self.config.merge.enabled(bubbles[index].tier) {
                out.push(bubbles[index].clone());
                index += 1;
                continue;
            }
            let mut end = index + 1;
            while end < bubbles.len() {
                let previous = &bubbles[end - 1];
                let next = &bubbles[end];
                if previous.segment != next.segment
                    || previous.direction != next.direction
                    || !self.config.merge.enabled(next.tier)
                {
                    break;
                }
                let previous_bucket = self.grid.price_bucket(previous.anchor_price)?;
                let next_bucket = self.grid.price_bucket(next.anchor_price)?;
                let distance = previous_bucket
                    .0
                    .checked_sub(next_bucket.0)
                    .and_then(i128::checked_abs)
                    .ok_or_else(|| crate::overflow("calculating bubble merge distance"))?;
                if u128::try_from(distance)
                    .map_err(|_| crate::overflow("converting bubble merge distance"))?
                    > u128::from(self.config.merge.max_distance_buckets)
                {
                    break;
                }
                end += 1;
            }
            if end - index == 1 {
                out.push(bubbles[index].clone());
            } else {
                out.push(self.merge_group(&bubbles[index..end])?);
            }
            index = end;
        }
        Ok(out)
    }

    fn merge_group(&self, group: &[OrderFlowBubble]) -> Result<OrderFlowBubble, AnalyticsError> {
        let anchor = group
            .iter()
            .max_by(|left, right| {
                left.strength
                    .coefficient
                    .cmp(&right.strength.coefficient)
                    .then_with(|| crate::compare_price(right.anchor_price, left.anchor_price))
            })
            .ok_or_else(|| invalid_config("merge group", "must not be empty"))?;
        let mut sources = BTreeSet::new();
        let mut buy = 0_i128;
        let mut sell = 0_i128;
        let mut unknown = 0_i128;
        let mut total = 0_i128;
        let mut strength = 0_i128;
        let mut low = group[0].low_price;
        let mut high = group[0].high_price;
        let mut tier = BubbleTier::F1;
        for bubble in group {
            sources.extend(bubble.sources.iter().copied());
            buy = buy
                .checked_add(self.grid.non_negative_quantity_units(bubble.buy_volume)?.0)
                .ok_or_else(|| crate::overflow("merging buy volume"))?;
            sell = sell
                .checked_add(self.grid.non_negative_quantity_units(bubble.sell_volume)?.0)
                .ok_or_else(|| crate::overflow("merging sell volume"))?;
            unknown = unknown
                .checked_add(
                    self.grid
                        .non_negative_quantity_units(bubble.unknown_volume)?
                        .0,
                )
                .ok_or_else(|| crate::overflow("merging unknown volume"))?;
            total = total
                .checked_add(self.grid.quantity_units(bubble.total_volume)?.0)
                .ok_or_else(|| crate::overflow("merging total volume"))?;
            strength = strength
                .checked_add(bubble.strength.coefficient)
                .ok_or_else(|| crate::overflow("merging bubble strength"))?;
            low = crate::min_price(low, bubble.low_price);
            high = crate::max_price(high, bubble.high_price);
            if bubble.tier.priority() > tier.priority() {
                tier = bubble.tier;
            }
        }
        let delta = buy
            .checked_sub(sell)
            .ok_or_else(|| crate::overflow("merging bubble delta"))?;
        let mode = anchor.mode;
        let cap_quantity = match mode {
            BubbleMode::Volume => self.config.merge.style.volume_cap,
            BubbleMode::Delta => self.config.merge.style.delta_cap,
        };
        let cap = u128::try_from(self.grid.quantity_units(cap_quantity)?.0)
            .map_err(|_| crate::overflow("converting merged bubble cap"))?;
        let strength_unsigned = u128::try_from(strength)
            .map_err(|_| crate::overflow("converting merged bubble strength"))?;
        let label = match self.config.merge.style.label_mode {
            LabelMode::Off => None,
            LabelMode::Raw => Some(Fixed::new(
                if mode == BubbleMode::Volume {
                    total
                } else {
                    delta
                },
                self.grid.quantity_scale,
            )),
        };
        let mut id = 14_695_981_039_346_656_037_u64;
        for bubble in group {
            hash_bytes(&mut id, &bubble.id.to_le_bytes());
        }
        Ok(OrderFlowBubble {
            id,
            instrument: anchor.instrument,
            candle_start_ns: anchor.candle_start_ns,
            candle_end_ns: anchor.candle_end_ns,
            segment: anchor.segment,
            sources: sources.into_iter().collect(),
            tier,
            mode,
            direction: match delta.cmp(&0) {
                std::cmp::Ordering::Less => BubbleDirection::Sell,
                std::cmp::Ordering::Equal => BubbleDirection::Neutral,
                std::cmp::Ordering::Greater => BubbleDirection::Buy,
            },
            anchor_price: anchor.anchor_price,
            low_price: low,
            high_price: high,
            buy_volume: self.grid.quantity_at(QuantityUnits(buy))?,
            sell_volume: self.grid.quantity_at(QuantityUnits(sell))?,
            unknown_volume: self.grid.quantity_at(QuantityUnits(unknown))?,
            total_volume: self.grid.quantity_at(QuantityUnits(total))?,
            delta: Fixed::new(delta, self.grid.quantity_scale),
            strength: Fixed::new(strength, self.grid.quantity_scale),
            threshold: group
                .iter()
                .map(|bubble| bubble.threshold)
                .max_by_key(|threshold| threshold.coefficient)
                .unwrap_or(Fixed::ZERO),
            visual_size: scaled_size(
                strength_unsigned,
                cap,
                self.config.merge.style.min_size,
                self.config.merge.style.max_size,
            )?,
            label,
            shape: self.config.merge.style.shape,
            merged_count: u16::try_from(group.len())
                .map_err(|_| crate::overflow("converting merged bubble count"))?,
        })
    }
}

fn candidate_keys(
    candle: &CandleFlow,
    grid: &GridSpec,
) -> Result<BTreeSet<CandidateKey>, AnalyticsError> {
    let mut keys = BTreeSet::new();
    for source in &candle.sources {
        for level in &source.levels {
            keys.insert(CandidateKey {
                segment: source.source.segment,
                bucket: grid.price_bucket(level.price)?,
            });
        }
    }
    Ok(keys)
}

fn aggregate_for(
    candle: &CandleFlow,
    key: CandidateKey,
    selector: &SourceSelector,
    grid: &GridSpec,
) -> Result<Aggregate, AnalyticsError> {
    let mut sources = BTreeSet::new();
    let mut buy = 0_i128;
    let mut sell = 0_i128;
    let mut unknown = 0_i128;
    let mut total = 0_i128;
    for source in &candle.sources {
        if source.source.segment != key.segment
            || !selector.matches(source.source.venue, source.source.segment)
        {
            continue;
        }
        for level in &source.levels {
            if grid.price_bucket(level.price)? != key.bucket {
                continue;
            }
            sources.insert(source.source.venue);
            buy = buy
                .checked_add(grid.non_negative_quantity_units(level.buy_volume)?.0)
                .ok_or_else(|| crate::overflow("aggregating buy volume"))?;
            sell = sell
                .checked_add(grid.non_negative_quantity_units(level.sell_volume)?.0)
                .ok_or_else(|| crate::overflow("aggregating sell volume"))?;
            unknown = unknown
                .checked_add(grid.non_negative_quantity_units(level.unknown_volume)?.0)
                .ok_or_else(|| crate::overflow("aggregating unknown volume"))?;
            total = total
                .checked_add(grid.quantity_units(level.total_volume)?.0)
                .ok_or_else(|| crate::overflow("aggregating total volume"))?;
        }
    }
    Ok(Aggregate {
        segment: key.segment,
        bucket: key.bucket,
        sources: sources.into_iter().collect(),
        buy_units: buy,
        sell_units: sell,
        unknown_units: unknown,
        total_units: total,
    })
}

fn percentile(values: &[u128], bps: u16) -> Result<u128, AnalyticsError> {
    if values.is_empty() {
        return Err(invalid_config("percentile", "requires at least one sample"));
    }
    let numerator = usize::from(bps)
        .checked_mul(values.len())
        .ok_or_else(|| crate::overflow("calculating percentile rank"))?;
    let rank = numerator
        .checked_add(9_999)
        .ok_or_else(|| crate::overflow("rounding percentile rank"))?
        / 10_000;
    Ok(values[rank.saturating_sub(1).min(values.len() - 1)])
}

fn weighted_bps(
    left: u128,
    left_bps: u16,
    right: u128,
    right_bps: u16,
) -> Result<u128, AnalyticsError> {
    left.checked_mul(u128::from(left_bps))
        .and_then(|value| {
            right
                .checked_mul(u128::from(right_bps))
                .and_then(|other| value.checked_add(other))
        })
        .map(|value| value / 10_000)
        .ok_or_else(|| crate::overflow("calculating adaptive threshold"))
}

fn scaled_size(
    strength: u128,
    cap: u128,
    min_size: u16,
    max_size: u16,
) -> Result<u16, AnalyticsError> {
    let cap = cap.max(1);
    let range = u128::from(max_size - min_size);
    let scaled = strength
        .min(cap)
        .checked_mul(range)
        .ok_or_else(|| crate::overflow("calculating bubble size"))?
        / cap;
    u16::try_from(u128::from(min_size) + scaled)
        .map_err(|_| crate::overflow("converting bubble size"))
}

fn bubble_id(
    candle_start_ns: i64,
    instrument: InstrumentId,
    segment: MarketSegment,
    bucket: PriceBucket,
    tier: BubbleTier,
) -> u64 {
    let mut hash = 14_695_981_039_346_656_037_u64;
    hash_bytes(&mut hash, &candle_start_ns.to_le_bytes());
    hash_bytes(&mut hash, &instrument.0.to_le_bytes());
    hash_bytes(&mut hash, &[segment as u8]);
    hash_bytes(&mut hash, &bucket.0.to_le_bytes());
    hash_bytes(&mut hash, &[tier.priority()]);
    hash
}

fn hash_bytes(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(1_099_511_628_211);
    }
}
