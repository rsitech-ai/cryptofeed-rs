use marketfeed_model::{InstrumentKind, VenueId};
use serde::{Deserialize, Serialize};

use crate::{AnalyticsError, invalid_config};

const MAX_SELECTOR_VENUES: usize = 1_024;
const MAX_SELECTOR_SEGMENTS: usize = 16;

/// Economic market segment. Different segments are never collapsed into one bubble.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum MarketSegment {
    Spot,
    LinearPerpetual,
    InversePerpetual,
    LinearFuture,
    InverseFuture,
    Option,
    Unknown,
}

impl From<InstrumentKind> for MarketSegment {
    fn from(value: InstrumentKind) -> Self {
        match value {
            InstrumentKind::Spot => Self::Spot,
            InstrumentKind::PerpetualLinear => Self::LinearPerpetual,
            InstrumentKind::PerpetualInverse => Self::InversePerpetual,
            InstrumentKind::FutureLinear => Self::LinearFuture,
            InstrumentKind::FutureInverse => Self::InverseFuture,
            InstrumentKind::Option => Self::Option,
        }
    }
}

/// Venue and economic segment attached to one normalized trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FlowSource {
    pub venue: VenueId,
    pub segment: MarketSegment,
}

/// Deterministic source allow-list. An empty dimension means all values in that dimension.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSelector {
    pub venues: Vec<VenueId>,
    pub segments: Vec<MarketSegment>,
}

impl SourceSelector {
    pub fn new(
        mut venues: Vec<VenueId>,
        mut segments: Vec<MarketSegment>,
    ) -> Result<Self, AnalyticsError> {
        if venues.len() > MAX_SELECTOR_VENUES {
            return Err(invalid_config(
                "source selector venues",
                format!("must contain at most {MAX_SELECTOR_VENUES} entries"),
            ));
        }
        if segments.len() > MAX_SELECTOR_SEGMENTS {
            return Err(invalid_config(
                "source selector segments",
                format!("must contain at most {MAX_SELECTOR_SEGMENTS} entries"),
            ));
        }
        venues.sort_unstable();
        venues.dedup();
        segments.sort_unstable();
        segments.dedup();
        Ok(Self { venues, segments })
    }

    pub const fn all() -> Self {
        Self {
            venues: Vec::new(),
            segments: Vec::new(),
        }
    }

    pub fn validate(&self) -> Result<(), AnalyticsError> {
        Self::new(self.venues.clone(), self.segments.clone()).map(|_| ())
    }

    pub fn matches(&self, venue: VenueId, segment: MarketSegment) -> bool {
        (self.venues.is_empty() || self.venues.contains(&venue))
            && (self.segments.is_empty() || self.segments.contains(&segment))
    }
}

impl Default for SourceSelector {
    fn default() -> Self {
        Self::all()
    }
}

/// Event-time boundaries used by candles, TPOs, sessions, and top zones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeframeSpec {
    pub candle_ns: i64,
    pub tpo_ns: i64,
    pub session_ns: i64,
    pub anchor_ns: i64,
    pub top_zone_window_ns: i64,
}

impl TimeframeSpec {
    pub fn new(
        candle_ns: i64,
        tpo_ns: i64,
        session_ns: i64,
        anchor_ns: i64,
        top_zone_window_ns: i64,
    ) -> Result<Self, AnalyticsError> {
        for (field, value) in [
            ("candle_ns", candle_ns),
            ("tpo_ns", tpo_ns),
            ("session_ns", session_ns),
            ("top_zone_window_ns", top_zone_window_ns),
        ] {
            if value <= 0 {
                return Err(AnalyticsError::NonPositive { field });
            }
        }
        if candle_ns > session_ns || session_ns % candle_ns != 0 {
            return Err(invalid_config("candle_ns", "must evenly divide session_ns"));
        }
        if tpo_ns > session_ns || session_ns % tpo_ns != 0 {
            return Err(invalid_config("tpo_ns", "must evenly divide session_ns"));
        }
        Ok(Self {
            candle_ns,
            tpo_ns,
            session_ns,
            anchor_ns,
            top_zone_window_ns,
        })
    }

    pub fn validate(&self) -> Result<(), AnalyticsError> {
        Self::new(
            self.candle_ns,
            self.tpo_ns,
            self.session_ns,
            self.anchor_ns,
            self.top_zone_window_ns,
        )
        .map(|_| ())
    }

    pub fn candle_start(&self, timestamp_ns: i64) -> i64 {
        period_start_saturating(timestamp_ns, self.anchor_ns, self.candle_ns)
    }

    pub fn tpo_start(&self, timestamp_ns: i64) -> i64 {
        period_start_saturating(timestamp_ns, self.anchor_ns, self.tpo_ns)
    }

    pub fn session_start(&self, timestamp_ns: i64) -> i64 {
        period_start_saturating(timestamp_ns, self.anchor_ns, self.session_ns)
    }

    pub(crate) fn checked_candle_start(&self, timestamp_ns: i64) -> Result<i64, AnalyticsError> {
        period_start_checked(timestamp_ns, self.anchor_ns, self.candle_ns)
    }

    pub(crate) fn checked_tpo_start(&self, timestamp_ns: i64) -> Result<i64, AnalyticsError> {
        period_start_checked(timestamp_ns, self.anchor_ns, self.tpo_ns)
    }

    pub(crate) fn checked_session_start(&self, timestamp_ns: i64) -> Result<i64, AnalyticsError> {
        period_start_checked(timestamp_ns, self.anchor_ns, self.session_ns)
    }
}

fn period_start_saturating(timestamp_ns: i64, anchor_ns: i64, duration_ns: i64) -> i64 {
    let start = period_start_i128(timestamp_ns, anchor_ns, duration_ns);
    start.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

fn period_start_checked(
    timestamp_ns: i64,
    anchor_ns: i64,
    duration_ns: i64,
) -> Result<i64, AnalyticsError> {
    i64::try_from(period_start_i128(timestamp_ns, anchor_ns, duration_ns))
        .map_err(|_| crate::overflow("calculating an event-time period boundary"))
}

fn period_start_i128(timestamp_ns: i64, anchor_ns: i64, duration_ns: i64) -> i128 {
    let timestamp = i128::from(timestamp_ns);
    let anchor = i128::from(anchor_ns);
    let duration = i128::from(duration_ns);
    (timestamp - anchor).div_euclid(duration) * duration + anchor
}
