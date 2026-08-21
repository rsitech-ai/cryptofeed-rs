//! Exact fixed-point EventPulse feature mechanics.

use std::collections::{BTreeMap, BTreeSet};

use marketfeed_book::{BookError, BookValidity, OrderBook};
use marketfeed_model::{BookDelta, BookSnapshot, Fixed, Price, Quantity, RoundingMode};
use thiserror::Error;

use crate::wire::{ContributorKeyV1, ContributorRoleV1, FamilyV1, MechanicsConfigV1};
use crate::{SlotState, SourceStateMachine};

pub const SCALE: i128 = 100_000_000;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ArithmeticError {
    #[error("checked arithmetic overflowed")]
    Overflow,
    #[error("division denominator must be positive")]
    Division,
    #[error("value cannot be rescaled")]
    Rescale,
    #[error("value is outside the formula domain")]
    OutOfDomain,
}

fn rounded_div(numerator: i128, denominator: i128) -> Result<i128, ArithmeticError> {
    if denominator <= 0 {
        return Err(ArithmeticError::Division);
    }
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    let twice = remainder
        .unsigned_abs()
        .checked_mul(2)
        .ok_or(ArithmeticError::Overflow)?;
    if twice >= denominator as u128 {
        quotient
            .checked_add(if numerator < 0 { -1 } else { 1 })
            .ok_or(ArithmeticError::Overflow)
    } else {
        Ok(quotient)
    }
}

pub fn rescale(value: Fixed) -> Result<i128, ArithmeticError> {
    value
        .rescale(8, RoundingMode::HalfAwayFromZero)
        .map(|v| v.coefficient)
        .map_err(|_| ArithmeticError::Rescale)
}

pub fn mul_scaled(a: i128, b: i128) -> Result<i128, ArithmeticError> {
    rounded_div(a.checked_mul(b).ok_or(ArithmeticError::Overflow)?, SCALE)
}
pub fn div_scaled(a: i128, b: i128) -> Result<i128, ArithmeticError> {
    rounded_div(a.checked_mul(SCALE).ok_or(ArithmeticError::Overflow)?, b)
}
pub fn div_integer(a: i128, k: i128) -> Result<i128, ArithmeticError> {
    rounded_div(a, k)
}

pub fn canonical_decimal(value: i128) -> String {
    if value == 0 {
        return "0".into();
    }
    let negative = value < 0;
    let magnitude = value.unsigned_abs();
    let integer = magnitude / SCALE as u128;
    let fraction = magnitude % SCALE as u128;
    let sign = if negative { "-" } else { "" };
    if fraction == 0 {
        format!("{sign}{integer}")
    } else {
        let mut fraction = format!("{fraction:08}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        format!("{sign}{integer}.{fraction}")
    }
}

pub fn log_return(p0: i128, p1: i128) -> Result<i128, ArithmeticError> {
    if p0 <= 0 || p1 <= 0 {
        return Err(ArithmeticError::OutOfDomain);
    }
    let above_lower = p1
        .checked_mul(5)
        .and_then(|value| p0.checked_mul(4).map(|lower| value >= lower))
        .ok_or(ArithmeticError::Overflow)?;
    let below_upper = p1
        .checked_mul(4)
        .and_then(|value| p0.checked_mul(5).map(|upper| value <= upper))
        .ok_or(ArithmeticError::Overflow)?;
    if !above_lower || !below_upper {
        return Err(ArithmeticError::OutOfDomain);
    }
    let z = div_scaled(
        p1.checked_sub(p0).ok_or(ArithmeticError::Overflow)?,
        p1.checked_add(p0).ok_or(ArithmeticError::Overflow)?,
    )?;
    let z2 = mul_scaled(z, z)?;
    let mut power = z;
    let mut sum = 0i128;
    for k in (1..=15).step_by(2) {
        sum = sum
            .checked_add(div_integer(power, k)?)
            .ok_or(ArithmeticError::Overflow)?;
        if k < 15 {
            power = mul_scaled(power, z2)?;
        }
    }
    sum.checked_mul(2).ok_or(ArithmeticError::Overflow)
}

pub fn taker_imbalance(buy: i128, sell: i128) -> Result<i128, ArithmeticError> {
    if buy < 0 || sell < 0 {
        return Err(ArithmeticError::OutOfDomain);
    }
    let total = buy.checked_add(sell).ok_or(ArithmeticError::Overflow)?;
    if total == 0 {
        return Err(ArithmeticError::OutOfDomain);
    }
    Ok(div_scaled(
        buy.checked_sub(sell).ok_or(ArithmeticError::Overflow)?,
        total,
    )?
    .clamp(-SCALE, SCALE))
}

pub fn cvd_slope(first: i128, last: i128, elapsed_micros: i128) -> Result<i128, ArithmeticError> {
    if elapsed_micros <= 0 {
        return Err(ArithmeticError::Division);
    }
    rounded_div(
        last.checked_sub(first)
            .and_then(|v| v.checked_mul(1_000_000))
            .ok_or(ArithmeticError::Overflow)?,
        elapsed_micros,
    )
}

pub fn spread_bps(bid: i128, ask: i128) -> Result<i128, ArithmeticError> {
    if bid <= 0 || ask <= bid {
        return Err(ArithmeticError::OutOfDomain);
    }
    rounded_div(
        ask.checked_sub(bid)
            .and_then(|v| v.checked_mul(2))
            .and_then(|v| v.checked_mul(10_000))
            .and_then(|v| v.checked_mul(SCALE))
            .ok_or(ArithmeticError::Overflow)?,
        ask.checked_add(bid).ok_or(ArithmeticError::Overflow)?,
    )
}

pub fn open_interest_change(first: i128, last: i128) -> Result<i128, ArithmeticError> {
    last.checked_sub(first).ok_or(ArithmeticError::Overflow)
}

/// Convert a catalog-admitted OI sample to CONTRACTS at the canonical scale.
/// `None` is the CONTRACTS encoding; `Some` is the positive BASE conversion.
pub fn open_interest_contracts(
    quantity: Fixed,
    contracts_per_base: Option<i128>,
) -> Result<i128, ArithmeticError> {
    let quantity = rescale(quantity)?;
    if quantity < 0 {
        return Err(ArithmeticError::OutOfDomain);
    }
    match contracts_per_base {
        None => Ok(quantity),
        Some(conversion) if conversion > 0 => mul_scaled(quantity, conversion),
        Some(_) => Err(ArithmeticError::OutOfDomain),
    }
}

pub fn liquidation_notional(values: &[(i128, i128)]) -> Result<i128, ArithmeticError> {
    values.iter().try_fold(0i128, |sum, &(price, quantity)| {
        if price <= 0 || quantity == i128::MIN {
            return Err(ArithmeticError::OutOfDomain);
        }
        sum.checked_add(mul_scaled(price, quantity.abs())?)
            .ok_or(ArithmeticError::Overflow)
    })
}

pub fn cross_venue_breadth(confirming: usize, eligible: usize) -> Result<i128, ArithmeticError> {
    if eligible < 2 || confirming > eligible {
        return Err(ArithmeticError::OutOfDomain);
    }
    div_integer(
        (confirming as i128)
            .checked_mul(SCALE)
            .ok_or(ArithmeticError::Overflow)?,
        eligible as i128,
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenueReturn {
    pub contributor: ContributorKeyV1,
    pub log_return: i128,
    pub complete: bool,
}

/// Compute breadth from configured family owners and Task 4's current Live
/// eligibility. Unconfigured or duplicate observations fail closed.
pub fn configured_cross_venue_breadth(
    config: &MechanicsConfigV1,
    cursors: &SourceStateMachine,
    direction: Direction,
    returns: &[VenueReturn],
) -> Result<i128, ArithmeticError> {
    if direction == Direction::Unknown {
        return Err(ArithmeticError::OutOfDomain);
    }
    let mut supplied = BTreeMap::new();
    for observation in returns {
        if supplied
            .insert(&observation.contributor, observation)
            .is_some()
        {
            return Err(ArithmeticError::OutOfDomain);
        }
    }
    let owners = config
        .contributors()
        .iter()
        .filter(|spec| match spec.role() {
            ContributorRoleV1::Primary => spec.allowed_families().contains(&FamilyV1::Trade),
            ContributorRoleV1::Confirmation => true,
        });
    let mut configured_venues = BTreeSet::new();
    let mut usable = 0usize;
    let mut confirming = 0usize;
    for owner in owners {
        configured_venues.insert(owner.key().instrument().venue().to_owned());
        let Some(observation) = supplied.get(owner.key()) else {
            continue;
        };
        if !observation.complete || cursors.contributor_state(owner.key()) != Some(SlotState::Live)
        {
            continue;
        }
        usable += 1;
        let confirms = match direction {
            Direction::Up => observation.log_return >= 200_000,
            Direction::Down => observation.log_return <= -200_000,
            Direction::Unknown => false,
        };
        confirming += usize::from(confirms);
    }
    if usable < 2 || configured_venues.len() < 2 {
        return Err(ArithmeticError::OutOfDomain);
    }
    cross_venue_breadth(confirming, configured_venues.len())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Up,
    Down,
    Unknown,
}

pub fn reversal_from_extreme(
    direction: Direction,
    anchor: i128,
    extreme: i128,
    current: i128,
) -> Result<i128, ArithmeticError> {
    let value = match direction {
        Direction::Up if extreme > anchor => div_scaled(
            extreme
                .checked_sub(current)
                .ok_or(ArithmeticError::Overflow)?,
            extreme
                .checked_sub(anchor)
                .ok_or(ArithmeticError::Overflow)?,
        )?,
        Direction::Down if anchor > extreme => div_scaled(
            current
                .checked_sub(extreme)
                .ok_or(ArithmeticError::Overflow)?,
            anchor
                .checked_sub(extreme)
                .ok_or(ArithmeticError::Overflow)?,
        )?,
        _ => return Err(ArithmeticError::OutOfDomain),
    };
    Ok(value.clamp(0, SCALE))
}

#[derive(Debug, Clone)]
pub struct BookProjection {
    book: OrderBook,
    price_scale: u8,
    quantity_scale: u8,
    available_at_ns: Option<i64>,
    resyncing: bool,
}

impl BookProjection {
    pub fn new(price_scale: u8, quantity_scale: u8, depth: Option<u32>) -> Self {
        Self {
            book: OrderBook::new(price_scale, quantity_scale, depth),
            price_scale,
            quantity_scale,
            available_at_ns: None,
            resyncing: true,
        }
    }
    pub fn snapshot(
        &mut self,
        value: &BookSnapshot,
        sequence: Option<u64>,
        at_ns: i64,
    ) -> Result<(), ProjectionError> {
        if !strictly_sorted(value, self.price_scale, self.quantity_scale) {
            self.invalidate();
            return Err(ProjectionError::Unsorted);
        }
        let bids = value
            .bids
            .iter()
            .map(|l| (l.price, l.quantity))
            .collect::<Vec<_>>();
        let asks = value
            .asks
            .iter()
            .map(|l| (l.price, l.quantity))
            .collect::<Vec<_>>();
        if let Err(error) = self.book.apply_snapshot(&bids, &asks, sequence) {
            self.invalidate();
            return Err(error.into());
        }
        self.available_at_ns = Some(at_ns);
        self.resyncing = false;
        Ok(())
    }
    pub fn delta(
        &mut self,
        value: &BookDelta,
        sequence: Option<u64>,
        at_ns: i64,
    ) -> Result<(), ProjectionError> {
        if let (Some(previous), Some(next)) = (self.book.sequence(), sequence) {
            if previous.checked_add(1) != Some(next) {
                self.invalidate();
                return Err(ProjectionError::SequenceGap);
            }
        }
        if let Err(error) = self.book.apply_changes_atomic(&value.changes) {
            self.invalidate();
            return Err(error.into());
        }
        if let Some(sequence) = sequence {
            self.book.set_sequence(sequence);
        }
        self.available_at_ns = Some(at_ns);
        Ok(())
    }
    pub fn invalidate(&mut self) {
        self.book.clear();
        self.book.set_validity(BookValidity::Invalid);
        self.available_at_ns = None;
        self.resyncing = true;
    }
    pub fn permit_resnapshot(&mut self) {
        self.book.clear();
        self.book.set_validity(BookValidity::Synchronizing);
        self.available_at_ns = None;
        self.resyncing = true;
    }
    pub fn depth_10bps(&self, decision_time_ns: i64) -> Result<i128, ArithmeticError> {
        if self.resyncing
            || self.available_at_ns.is_none_or(|at| {
                decision_time_ns
                    .checked_sub(at)
                    .is_none_or(|age| !(0..=250_000_000).contains(&age))
            })
        {
            return Err(ArithmeticError::OutOfDomain);
        }
        let (bid, _) = self.book.best_bid().ok_or(ArithmeticError::OutOfDomain)?;
        let (ask, _) = self.book.best_ask().ok_or(ArithmeticError::OutOfDomain)?;
        let bid = rescale(bid.0)?;
        let ask = rescale(ask.0)?;
        if bid <= 0 || ask <= bid {
            return Err(ArithmeticError::OutOfDomain);
        }
        let mid_numerator = bid.checked_add(ask).ok_or(ArithmeticError::Overflow)?;
        let (bids, asks) = self
            .book
            .snapshot_levels()
            .ok_or(ArithmeticError::OutOfDomain)?;
        bids.into_iter().chain(asks).try_fold(0i128, |sum, level| {
            let price = rescale(level.price.0)?;
            let quantity = rescale(level.quantity.0)?;
            let admitted = if price <= bid {
                price.checked_mul(10_000).ok_or(ArithmeticError::Overflow)?
                    >= mid_numerator
                        .checked_mul(4_995)
                        .ok_or(ArithmeticError::Overflow)?
            } else {
                price.checked_mul(10_000).ok_or(ArithmeticError::Overflow)?
                    <= mid_numerator
                        .checked_mul(5_005)
                        .ok_or(ArithmeticError::Overflow)?
            };
            if admitted {
                sum.checked_add(mul_scaled(price, quantity)?)
                    .ok_or(ArithmeticError::Overflow)
            } else {
                Ok(sum)
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProjectionError {
    #[error(transparent)]
    Book(#[from] BookError),
    #[error("book levels are not strictly sorted")]
    Unsorted,
    #[error("book delta sequence is not contiguous")]
    SequenceGap,
}

fn strictly_sorted(snapshot: &BookSnapshot, price_scale: u8, quantity_scale: u8) -> bool {
    let valid_level = |level: &marketfeed_model::BookLevel| {
        level
            .price
            .0
            .rescale(price_scale, RoundingMode::ExactOnly)
            .is_ok()
            && level
                .quantity
                .0
                .rescale(quantity_scale, RoundingMode::ExactOnly)
                .is_ok_and(|quantity| quantity.coefficient > 0)
    };
    let ordered = |levels: &[marketfeed_model::BookLevel], descending: bool| {
        levels.windows(2).all(|pair| {
            let left = pair[0]
                .price
                .0
                .rescale(price_scale, RoundingMode::ExactOnly)
                .ok()
                .map(|value| value.coefficient);
            let right = pair[1]
                .price
                .0
                .rescale(price_scale, RoundingMode::ExactOnly)
                .ok()
                .map(|value| value.coefficient);
            match (left, right, descending) {
                (Some(left), Some(right), true) => left > right,
                (Some(left), Some(right), false) => left < right,
                _ => false,
            }
        })
    };
    snapshot.bids.iter().all(valid_level)
        && snapshot.asks.iter().all(valid_level)
        && ordered(&snapshot.bids, true)
        && ordered(&snapshot.asks, false)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FeatureName {
    BookDepth10bps,
    CrossVenueBreadth,
    CvdSlope,
    LiquidationNotional,
    LogReturn,
    OpenInterestChange,
    ReversalFromExtreme,
    SpreadBps,
    TakerImbalance,
}

impl FeatureName {
    pub const CANONICAL: [(Self, u64); 9] = [
        (Self::BookDepth10bps, 250),
        (Self::CrossVenueBreadth, 1_000),
        (Self::CvdSlope, 1_000),
        (Self::LiquidationNotional, 5_000),
        (Self::LogReturn, 1_000),
        (Self::OpenInterestChange, 5_000),
        (Self::ReversalFromExtreme, 5_000),
        (Self::SpreadBps, 250),
        (Self::TakerImbalance, 1_000),
    ];
    pub fn is_critical(self, event_started: bool) -> bool {
        matches!(
            self,
            Self::LogReturn
                | Self::TakerImbalance
                | Self::CvdSlope
                | Self::SpreadBps
                | Self::BookDepth10bps
        ) || event_started && self == Self::ReversalFromExtreme
    }
    pub fn is_optional(self) -> bool {
        matches!(
            self,
            Self::OpenInterestChange | Self::LiquidationNotional | Self::CrossVenueBreadth
        )
    }
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BookDepth10bps => "book_depth_10bps",
            Self::CrossVenueBreadth => "cross_venue_breadth",
            Self::CvdSlope => "cvd_slope",
            Self::LiquidationNotional => "liquidation_notional",
            Self::LogReturn => "log_return",
            Self::OpenInterestChange => "open_interest_change",
            Self::ReversalFromExtreme => "reversal_from_extreme",
            Self::SpreadBps => "spread_bps",
            Self::TakerImbalance => "taker_imbalance",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureReason {
    ArithmeticInvalid,
    SourceInvalidated,
    BookResyncing,
    ClockDegraded,
    ReconnectWarmup,
    SourceStale,
    InsufficientCoverage,
    InsufficientSamples,
    DirectionUnknown,
    OutOfDomain,
    OptionalSourceUnavailable,
    ObservationValid,
}
impl FeatureReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ArithmeticInvalid => "ARITHMETIC_INVALID",
            Self::SourceInvalidated => "SOURCE_INVALIDATED",
            Self::BookResyncing => "BOOK_RESYNCING",
            Self::ClockDegraded => "CLOCK_DEGRADED",
            Self::ReconnectWarmup => "RECONNECT_WARMUP",
            Self::SourceStale => "SOURCE_STALE",
            Self::InsufficientCoverage => "INSUFFICIENT_COVERAGE",
            Self::InsufficientSamples => "INSUFFICIENT_SAMPLES",
            Self::DirectionUnknown => "DIRECTION_UNKNOWN",
            Self::OutOfDomain => "OUT_OF_DOMAIN",
            Self::OptionalSourceUnavailable => "OPTIONAL_SOURCE_UNAVAILABLE",
            Self::ObservationValid => "OBSERVATION_VALID",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureQuality {
    Invalid,
    Degraded,
    Unavailable,
    Validated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MechanicsFlag {
    BookResyncing,
    ClockUncertain,
    CrossVenueDivergence,
    InsufficientCoverage,
    OiStale,
    QueueDrop,
    ReconnectWarmup,
    SequenceGap,
    SourceStale,
}

#[derive(Debug, Clone, Default)]
pub struct FlagConditions {
    pub sequence_failure: bool,
    pub book_resyncing: bool,
    pub clock_degraded: bool,
    pub source_stale: bool,
    pub oi_stale_or_unavailable: bool,
    pub queue_drop: bool,
    pub reconnect_warmup: bool,
    pub incomplete_critical: bool,
    pub breadth_unavailable_or_divergent: bool,
}

pub fn mechanics_flags(conditions: &FlagConditions) -> Vec<MechanicsFlag> {
    let mut flags = Vec::with_capacity(9);
    if conditions.book_resyncing {
        flags.push(MechanicsFlag::BookResyncing);
    }
    if conditions.clock_degraded {
        flags.push(MechanicsFlag::ClockUncertain);
    }
    if conditions.breadth_unavailable_or_divergent {
        flags.push(MechanicsFlag::CrossVenueDivergence);
    }
    if conditions.incomplete_critical {
        flags.push(MechanicsFlag::InsufficientCoverage);
    }
    if conditions.oi_stale_or_unavailable {
        flags.push(MechanicsFlag::OiStale);
    }
    if conditions.queue_drop {
        flags.push(MechanicsFlag::QueueDrop);
    }
    if conditions.reconnect_warmup {
        flags.push(MechanicsFlag::ReconnectWarmup);
    }
    if conditions.sequence_failure {
        flags.push(MechanicsFlag::SequenceGap);
    }
    if conditions.source_stale {
        flags.push(MechanicsFlag::SourceStale);
    }
    flags.sort();
    flags
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvelopeQuality {
    Invalid,
    Degraded,
    Validated,
}

pub fn envelope_quality(rows: &[FeatureObservation], event_started: bool) -> EnvelopeQuality {
    if rows.iter().any(|row| {
        row.name.is_critical(event_started)
            && matches!(
                row.quality,
                FeatureQuality::Invalid | FeatureQuality::Unavailable
            )
    }) {
        EnvelopeQuality::Invalid
    } else if rows.iter().any(|row| {
        row.quality == FeatureQuality::Degraded
            || row.name.is_optional()
                && matches!(
                    row.quality,
                    FeatureQuality::Invalid | FeatureQuality::Unavailable
                )
    }) {
        EnvelopeQuality::Degraded
    } else {
        EnvelopeQuality::Validated
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureObservation {
    pub name: FeatureName,
    pub horizon_ms: u64,
    pub value: Option<i128>,
    pub quality: FeatureQuality,
    pub reason: FeatureReason,
}
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FeatureAuthoringError {
    #[error("arithmetic feature authorship failed")]
    ArithmeticAuthoringError,
    #[error("critical feature cannot truthfully be authored")]
    CriticalFeatureAuthoringError,
}

#[derive(Debug, Clone, Default)]
pub struct FeatureConditions {
    pub arithmetic_invalid: bool,
    pub source_invalidated: bool,
    pub book_resyncing: bool,
    pub clock_degraded: bool,
    pub reconnect_warmup: bool,
    pub source_stale: bool,
    pub insufficient_coverage: bool,
    pub insufficient_samples: bool,
    pub direction_unknown: bool,
    pub out_of_domain: bool,
    pub optional_source_unavailable: bool,
}

pub fn evaluate_feature(
    name: FeatureName,
    horizon_ms: u64,
    value: Option<i128>,
    c: &FeatureConditions,
    event_started: bool,
) -> Result<FeatureObservation, FeatureAuthoringError> {
    if c.arithmetic_invalid {
        return Err(FeatureAuthoringError::ArithmeticAuthoringError);
    }
    let (reason, quality) = if c.source_invalidated {
        (FeatureReason::SourceInvalidated, FeatureQuality::Invalid)
    } else if c.book_resyncing {
        (FeatureReason::BookResyncing, FeatureQuality::Invalid)
    } else if c.reconnect_warmup {
        (FeatureReason::ReconnectWarmup, FeatureQuality::Unavailable)
    } else if c.source_stale {
        (FeatureReason::SourceStale, FeatureQuality::Unavailable)
    } else if c.insufficient_coverage {
        (
            FeatureReason::InsufficientCoverage,
            FeatureQuality::Unavailable,
        )
    } else if c.insufficient_samples {
        (
            FeatureReason::InsufficientSamples,
            FeatureQuality::Unavailable,
        )
    } else if c.direction_unknown {
        (FeatureReason::DirectionUnknown, FeatureQuality::Unavailable)
    } else if c.out_of_domain {
        if name.is_critical(event_started) {
            return Err(FeatureAuthoringError::CriticalFeatureAuthoringError);
        }
        (FeatureReason::OutOfDomain, FeatureQuality::Unavailable)
    } else if c.optional_source_unavailable {
        (
            FeatureReason::OptionalSourceUnavailable,
            FeatureQuality::Unavailable,
        )
    } else if c.clock_degraded {
        (FeatureReason::ClockDegraded, FeatureQuality::Degraded)
    } else {
        (FeatureReason::ObservationValid, FeatureQuality::Validated)
    };
    Ok(FeatureObservation {
        name,
        horizon_ms,
        value: matches!(
            quality,
            FeatureQuality::Validated | FeatureQuality::Degraded
        )
        .then_some(value)
        .flatten(),
        quality,
        reason,
    })
}

pub fn evaluate_reversal(
    direction: Direction,
    event_started: bool,
    computed: Result<i128, ArithmeticError>,
    conditions: &FeatureConditions,
) -> Result<FeatureObservation, FeatureAuthoringError> {
    if !event_started {
        return Ok(FeatureObservation {
            name: FeatureName::ReversalFromExtreme,
            horizon_ms: 5_000,
            value: Some(0),
            quality: FeatureQuality::Validated,
            reason: FeatureReason::ObservationValid,
        });
    }
    let mut conditions = conditions.clone();
    let value = match computed {
        Ok(value) => Some(value),
        Err(ArithmeticError::OutOfDomain) if direction == Direction::Unknown => {
            conditions.direction_unknown = true;
            None
        }
        Err(ArithmeticError::OutOfDomain) => {
            conditions.out_of_domain = true;
            None
        }
        Err(_) => {
            conditions.arithmetic_invalid = true;
            None
        }
    };
    evaluate_feature(
        FeatureName::ReversalFromExtreme,
        5_000,
        value,
        &conditions,
        true,
    )
}

pub fn price(value: i128) -> Price {
    Price(Fixed::new(value, 8))
}
pub fn quantity(value: i128) -> Quantity {
    Quantity(Fixed::new(value, 8))
}
