use std::collections::VecDeque;

use marketfeed_model::{Fixed, Price};
use serde::{Deserialize, Serialize};

use crate::{
    AnalyticsError, BubbleDirection, BubbleTier, CandleFlow, FlowState, GridSpec, OrderFlowBubble,
    invalid_config,
};

const DAY_NS: i64 = 86_400_000_000_000;
const WEEK_NS: i64 = 604_800_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StructuralLevelKind {
    Naked,
    ReactionHigh,
    ReactionLow,
    TopDay,
    TopWeek,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StructuralLevelState {
    Active,
    Touched,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuralLevel {
    pub id: u64,
    pub kind: StructuralLevelKind,
    pub state: StructuralLevelState,
    pub source_bubble_id: u64,
    pub direction: BubbleDirection,
    pub tier: BubbleTier,
    pub price: Price,
    pub strength: Fixed,
    pub created_at_ns: i64,
    pub touched_at_ns: Option<i64>,
    pub window_start_ns: Option<i64>,
    pub expires_at_ns: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuralLevelConfig {
    pub touch_tolerance_ticks: u32,
    pub max_levels: usize,
    pub top_per_side: usize,
}

impl StructuralLevelConfig {
    pub fn new(
        touch_tolerance_ticks: u32,
        max_levels: usize,
        top_per_side: usize,
    ) -> Result<Self, AnalyticsError> {
        if max_levels == 0 {
            return Err(invalid_config("max_levels", "must be greater than zero"));
        }
        if top_per_side == 0 {
            return Err(invalid_config("top_per_side", "must be greater than zero"));
        }
        let reserved_top_levels = top_per_side.checked_mul(4).ok_or_else(|| {
            invalid_config(
                "top_per_side",
                "is too large to reserve daily and weekly levels",
            )
        })?;
        if reserved_top_levels > max_levels {
            return Err(invalid_config(
                "max_levels",
                "must fit daily and weekly top levels for both sides",
            ));
        }
        Ok(Self {
            touch_tolerance_ticks,
            max_levels,
            top_per_side,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CandleFrame {
    candle: CandleFlow,
    bubbles: Vec<OrderFlowBubble>,
}

/// Bounded deterministic state machine for structural levels derived only from
/// finalized candles and finalized server bubbles.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuralLevelEngine {
    grid: GridSpec,
    config: StructuralLevelConfig,
    last_candle_start_ns: Option<i64>,
    frames: VecDeque<CandleFrame>,
    pending_naked: Vec<OrderFlowBubble>,
    levels: VecDeque<StructuralLevel>,
}

impl StructuralLevelEngine {
    pub fn new(grid: GridSpec, config: StructuralLevelConfig) -> Result<Self, AnalyticsError> {
        grid.validate()?;
        StructuralLevelConfig::new(
            config.touch_tolerance_ticks,
            config.max_levels,
            config.top_per_side,
        )?;
        Ok(Self {
            grid,
            config,
            last_candle_start_ns: None,
            frames: VecDeque::with_capacity(3),
            pending_naked: Vec::new(),
            levels: VecDeque::with_capacity(config.max_levels.min(256)),
        })
    }

    pub fn ingest_finalized(
        &mut self,
        candle: &CandleFlow,
        bubbles: &[OrderFlowBubble],
    ) -> Result<(), AnalyticsError> {
        let mut next = self.clone();
        next.ingest_checked(candle, bubbles)?;
        *self = next;
        Ok(())
    }

    pub fn snapshot(&self) -> Vec<StructuralLevel> {
        self.levels.iter().cloned().collect()
    }

    fn ingest_checked(
        &mut self,
        candle: &CandleFlow,
        bubbles: &[OrderFlowBubble],
    ) -> Result<(), AnalyticsError> {
        if candle.state != FlowState::Final {
            return Err(AnalyticsError::CorruptSnapshot {
                detail: "structural levels require a finalized candle".into(),
            });
        }
        if let Some(previous) = self.last_candle_start_ns {
            if candle.start_ts <= previous {
                return Err(AnalyticsError::LateTrade {
                    timestamp_ns: candle.start_ts,
                    finalized_before_ns: previous.saturating_add(1),
                });
            }
        }
        for bubble in bubbles {
            if bubble.instrument != candle.instrument
                || bubble.candle_start_ns != candle.start_ts
                || bubble.candle_end_ns != candle.end_ts
            {
                return Err(AnalyticsError::CorruptSnapshot {
                    detail: "bubble does not belong to finalized candle".into(),
                });
            }
            self.grid.price_tick(bubble.anchor_price)?;
        }
        let high = candle.high.ok_or_else(|| AnalyticsError::CorruptSnapshot {
            detail: "finalized candle is missing high".into(),
        })?;
        let low = candle.low.ok_or_else(|| AnalyticsError::CorruptSnapshot {
            detail: "finalized candle is missing low".into(),
        })?;
        let high_tick = self.grid.price_tick(high)?.0;
        let low_tick = self.grid.price_tick(low)?.0;
        if low_tick > high_tick {
            return Err(AnalyticsError::CorruptSnapshot {
                detail: "finalized candle low exceeds high".into(),
            });
        }

        self.touch_existing(candle, low_tick, high_tick)?;
        self.activate_naked(candle, low_tick, high_tick)?;

        self.frames.push_back(CandleFrame {
            candle: candle.clone(),
            bubbles: bubbles.to_vec(),
        });
        while self.frames.len() > 3 {
            self.frames.pop_front();
        }
        self.confirm_reactions()?;
        self.update_top_levels(candle, bubbles)?;

        self.pending_naked = bubbles.to_vec();
        self.last_candle_start_ns = Some(candle.start_ts);
        self.enforce_capacity();
        Ok(())
    }

    fn touch_existing(
        &mut self,
        candle: &CandleFlow,
        low_tick: i128,
        high_tick: i128,
    ) -> Result<(), AnalyticsError> {
        let tolerance = i128::from(self.config.touch_tolerance_ticks);
        for level in &mut self.levels {
            if level.state != StructuralLevelState::Active
                || matches!(
                    level.kind,
                    StructuralLevelKind::TopDay | StructuralLevelKind::TopWeek
                )
                || candle.end_ts <= level.created_at_ns
            {
                continue;
            }
            let tick = self.grid.price_tick(level.price)?.0;
            if low_tick <= tick.saturating_add(tolerance)
                && high_tick >= tick.saturating_sub(tolerance)
            {
                level.state = StructuralLevelState::Touched;
                level.touched_at_ns = Some(candle.end_ts);
            }
        }
        Ok(())
    }

    fn activate_naked(
        &mut self,
        candle: &CandleFlow,
        low_tick: i128,
        high_tick: i128,
    ) -> Result<(), AnalyticsError> {
        let tolerance = i128::from(self.config.touch_tolerance_ticks);
        let pending = std::mem::take(&mut self.pending_naked);
        for bubble in pending {
            let tick = self.grid.price_tick(bubble.anchor_price)?.0;
            let touched = low_tick <= tick.saturating_add(tolerance)
                && high_tick >= tick.saturating_sub(tolerance);
            if !touched {
                self.levels.push_back(level_from_bubble(
                    StructuralLevelKind::Naked,
                    bubble.anchor_price,
                    candle.end_ts,
                    None,
                    None,
                    &bubble,
                ));
            }
        }
        Ok(())
    }

    fn confirm_reactions(&mut self) -> Result<(), AnalyticsError> {
        if self.frames.len() != 3 {
            return Ok(());
        }
        let left = &self.frames[0];
        let center = &self.frames[1];
        let right = &self.frames[2];
        let (Some(left_high), Some(center_high), Some(right_high)) =
            (left.candle.high, center.candle.high, right.candle.high)
        else {
            return Ok(());
        };
        let (Some(left_low), Some(center_low), Some(right_low)) =
            (left.candle.low, center.candle.low, right.candle.low)
        else {
            return Ok(());
        };
        let lh = self.grid.price_tick(left_high)?.0;
        let ch = self.grid.price_tick(center_high)?.0;
        let rh = self.grid.price_tick(right_high)?.0;
        let ll = self.grid.price_tick(left_low)?.0;
        let cl = self.grid.price_tick(center_low)?.0;
        let rl = self.grid.price_tick(right_low)?.0;
        let is_high = ch > lh && ch > rh;
        let is_low = cl < ll && cl < rl;
        let strongest = center.bubbles.iter().max_by(|left, right| {
            compare_fixed(left.strength, right.strength).then_with(|| right.id.cmp(&left.id))
        });
        if let Some(bubble) = strongest {
            if is_high {
                self.levels.push_back(level_from_bubble(
                    StructuralLevelKind::ReactionHigh,
                    center_high,
                    right.candle.end_ts,
                    None,
                    None,
                    bubble,
                ));
            }
            if is_low {
                self.levels.push_back(level_from_bubble(
                    StructuralLevelKind::ReactionLow,
                    center_low,
                    right.candle.end_ts,
                    None,
                    None,
                    bubble,
                ));
            }
        }
        Ok(())
    }

    fn update_top_levels(
        &mut self,
        candle: &CandleFlow,
        bubbles: &[OrderFlowBubble],
    ) -> Result<(), AnalyticsError> {
        for (kind, window_ns) in [
            (StructuralLevelKind::TopDay, DAY_NS),
            (StructuralLevelKind::TopWeek, WEEK_NS),
        ] {
            let window_start = candle.start_ts.div_euclid(window_ns) * window_ns;
            let expires =
                window_start
                    .checked_add(window_ns)
                    .ok_or(AnalyticsError::ArithmeticOverflow {
                        operation: "calculating structural level expiry",
                    })?;
            self.levels
                .retain(|level| level.kind != kind || level.window_start_ns == Some(window_start));
            for bubble in bubbles {
                if bubble.direction == BubbleDirection::Neutral {
                    continue;
                }
                self.levels.push_back(level_from_bubble(
                    kind,
                    bubble.anchor_price,
                    candle.end_ts,
                    Some(window_start),
                    Some(expires),
                    bubble,
                ));
            }
            self.trim_top(kind, window_start);
        }
        Ok(())
    }

    fn trim_top(&mut self, kind: StructuralLevelKind, window_start: i64) {
        for direction in [BubbleDirection::Buy, BubbleDirection::Sell] {
            let mut matches: Vec<(usize, Fixed, u64)> = self
                .levels
                .iter()
                .enumerate()
                .filter(|(_, level)| {
                    level.kind == kind
                        && level.window_start_ns == Some(window_start)
                        && level.direction == direction
                })
                .map(|(index, level)| (index, level.strength, level.source_bubble_id))
                .collect();
            matches.sort_by(|a, b| {
                compare_fixed(b.1, a.1)
                    .then_with(|| a.2.cmp(&b.2))
                    .then_with(|| a.0.cmp(&b.0))
            });
            let mut remove: Vec<usize> = matches
                .into_iter()
                .skip(self.config.top_per_side)
                .map(|(index, _, _)| index)
                .collect();
            remove.sort_unstable_by(|left, right| right.cmp(left));
            for index in remove {
                self.levels.remove(index);
            }
        }
    }

    fn enforce_capacity(&mut self) {
        while self.levels.len() > self.config.max_levels {
            let remove = self
                .levels
                .iter()
                .position(|level| level.state == StructuralLevelState::Touched)
                .unwrap_or(0);
            self.levels.remove(remove);
        }
    }
}

fn compare_fixed(left: Fixed, right: Fixed) -> std::cmp::Ordering {
    if left.scale == right.scale {
        return left.coefficient.cmp(&right.coefficient);
    }
    let scale = left.scale.max(right.scale);
    match (
        left.rescale(scale, marketfeed_model::RoundingMode::ExactOnly),
        right.rescale(scale, marketfeed_model::RoundingMode::ExactOnly),
    ) {
        (Ok(left), Ok(right)) => left.coefficient.cmp(&right.coefficient),
        _ => left
            .scale
            .cmp(&right.scale)
            .then_with(|| left.coefficient.cmp(&right.coefficient)),
    }
}

fn level_from_bubble(
    kind: StructuralLevelKind,
    price: Price,
    created_at_ns: i64,
    window_start_ns: Option<i64>,
    expires_at_ns: Option<i64>,
    bubble: &OrderFlowBubble,
) -> StructuralLevel {
    StructuralLevel {
        id: level_id(kind, bubble.id, window_start_ns.unwrap_or(created_at_ns)),
        kind,
        state: StructuralLevelState::Active,
        source_bubble_id: bubble.id,
        direction: bubble.direction,
        tier: bubble.tier,
        price,
        strength: bubble.strength,
        created_at_ns,
        touched_at_ns: None,
        window_start_ns,
        expires_at_ns,
    }
}

fn level_id(kind: StructuralLevelKind, bubble_id: u64, salt: i64) -> u64 {
    let kind = match kind {
        StructuralLevelKind::Naked => 1u64,
        StructuralLevelKind::ReactionHigh => 2,
        StructuralLevelKind::ReactionLow => 3,
        StructuralLevelKind::TopDay => 4,
        StructuralLevelKind::TopWeek => 5,
    };
    let mut value = 0xcbf2_9ce4_8422_2325u64;
    for part in [kind, bubble_id, salt as u64] {
        value ^= part;
        value = value.wrapping_mul(0x0000_0100_0000_01b3);
    }
    value
}
