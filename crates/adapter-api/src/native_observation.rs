//! Exact native feature values accompanying one market dispatch unit.
//!
//! These data types grant no publication authority. The caller must separately
//! validate the native protocol, subscription, source identity and negotiated
//! capability, then retain the attachment through the canonical queue/release
//! path. This module is not a raw-frame decoder or a consumer projection.
use crate::EventBatch;
use marketfeed_model::Fixed;
use serde::{Deserialize, Serialize};
use std::io::{self, Write};

/// One bounded market unit. Native-only observations still occupy a queue slot.
/// Private fields prevent later canonical edits from invalidating associations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ObservedEventBatch {
    canonical: EventBatch,
    native: NativeObservationAttachment,
    source: ObservedBatchSource,
    limits: ObservedBatchLimits,
}

/// Session/frame remain in the batch header even when canonical events is
/// empty. These additional source coordinates and clocks must survive too.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservedBatchSource {
    pub venue: marketfeed_model::VenueId,
    pub connection: marketfeed_model::ConnectionId,
    pub received: marketfeed_model::FrameStamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservedBatchLimits {
    pub layout: NativeObservationLimits,
    pub encoded_bytes: usize,
}

impl ObservedBatchLimits {
    pub fn validate(self) -> Result<(), NativeObservationError> {
        self.layout.validate()?;
        if self.encoded_bytes == 0 || self.encoded_bytes > Self::HARD_MAX.encoded_bytes {
            return Err(NativeObservationError("encoded byte bounds"));
        }
        Ok(())
    }
    /// Structural ceilings, not an admission policy or an allocator/RSS bound.
    pub const HARD_MAX: Self = Self {
        layout: NativeObservationLimits {
            rows: u16::MAX as usize,
            associations: u16::MAX as usize,
            book_levels: u16::MAX as usize * 40,
            mids: u16::MAX as usize,
            users: u16::MAX as usize,
            string_bytes: 4 * 1024 * 1024,
        },
        encoded_bytes: 16 * 1024 * 1024,
    };
}

struct SizeCounter {
    size: usize,
    max: usize,
}
impl Write for SizeCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.size = self
            .size
            .checked_add(bytes.len())
            .filter(|size| *size <= self.max)
            .ok_or_else(|| io::Error::other("observed batch byte budget"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl ObservedEventBatch {
    pub fn try_new(
        canonical: EventBatch,
        native: NativeObservationAttachment,
        source: ObservedBatchSource,
        limits: ObservedBatchLimits,
    ) -> Result<Self, NativeObservationError> {
        limits.validate()?;
        native.validate_layout(canonical.events.len(), limits.layout)?;
        if canonical.events.iter().any(|e| {
            e.venue != source.venue
                || e.connection != source.connection
                || e.receive_ts != source.received.receive_ts
                || e.session != canonical.session
                || e.frame_seq != canonical.frame_seq
        }) {
            return Err(NativeObservationError("canonical/source identity mismatch"));
        }
        let batch = Self {
            canonical,
            native,
            source,
            limits,
        };
        batch.encoded_len(limits.encoded_bytes)?;
        Ok(batch)
    }

    /// Exact encoded unit size without allocating another payload-sized buffer.
    pub fn encoded_len(&self, max: usize) -> Result<usize, NativeObservationError> {
        if max == 0 || max > ObservedBatchLimits::HARD_MAX.encoded_bytes {
            return Err(NativeObservationError("encoded byte bounds"));
        }
        let mut counter = SizeCounter { size: 0, max };
        serde_json::to_writer(&mut counter, self)
            .map_err(|_| NativeObservationError("encoded byte budget"))?;
        Ok(counter.size)
    }
    pub fn canonical(&self) -> &EventBatch {
        &self.canonical
    }
    pub fn native(&self) -> &NativeObservationAttachment {
        &self.native
    }
    pub fn source(&self) -> ObservedBatchSource {
        self.source
    }
    pub fn limits(&self) -> ObservedBatchLimits {
        self.limits
    }
}

impl<'de> Deserialize<'de> for ObservedEventBatch {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            canonical: EventBatch,
            native: NativeObservationAttachment,
            source: ObservedBatchSource,
            limits: ObservedBatchLimits,
        }
        // Enclosing codecs must bound input bytes before parsing. Validate the
        // decoded structure too; deserialization cannot bypass the constructor.
        let wire = Wire::deserialize(d)?;
        Self::try_new(wire.canonical, wire.native, wire.source, wire.limits)
            .map_err(serde::de::Error::custom)
    }
}

/// The market lane always owns the whole unit. There is no native side queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketBatch {
    Canonical(EventBatch),
    Observed(ObservedEventBatch),
}
impl From<EventBatch> for MarketBatch {
    fn from(batch: EventBatch) -> Self {
        Self::Canonical(batch)
    }
}
impl std::ops::Deref for MarketBatch {
    type Target = EventBatch;
    fn deref(&self) -> &EventBatch {
        match self {
            Self::Canonical(batch) => batch,
            Self::Observed(batch) => batch.canonical(),
        }
    }
}
impl MarketBatch {
    /// Only collector identity/ordinal fields change. Native associations refer
    /// to positions, so stamping cannot split or reorder their canonical rows.
    pub fn stamp_runtime(
        &mut self,
        venue: marketfeed_model::VenueId,
        session: marketfeed_model::SessionId,
        connection: marketfeed_model::ConnectionId,
        frame_seq: u64,
        received: Option<marketfeed_model::FrameStamp>,
        first_event_index: u32,
    ) -> Result<u32, NativeObservationError> {
        let len = u32::try_from(self.events.len())
            .map_err(|_| NativeObservationError("event index overflow"))?;
        let end = first_event_index
            .checked_add(len)
            .filter(|end| *end <= u32::from(u16::MAX) + 1)
            .ok_or(NativeObservationError("event index overflow"))?;
        // Only unquoted JSON integers change. Account for their exact
        // decimal lengths before mutation; a refusal leaves the unit untouched
        // and needs neither a canonical nor a native payload clone.
        let expected_size = if let Self::Observed(batch) = self {
            let received = received.ok_or(NativeObservationError(
                "native unit requires input receipt stamp",
            ))?;
            fn digits(value: u64) -> usize {
                value.checked_ilog10().unwrap_or(0) as usize + 1
            }
            let mut size = batch.encoded_len(batch.limits.encoded_bytes)?;
            let mut replace = |old: u64, new: u64| {
                size = size - digits(old) + digits(new);
            };
            replace(batch.canonical.session.0, session.0);
            replace(batch.canonical.frame_seq, frame_seq);
            replace(u64::from(batch.source.venue.0), u64::from(venue.0));
            replace(batch.source.connection.0, connection.0);
            replace(batch.source.received.mono_ns, received.mono_ns);
            for (offset, event) in batch.canonical.events.iter().enumerate() {
                replace(u64::from(event.venue.0), u64::from(venue.0));
                replace(event.session.0, session.0);
                replace(event.connection.0, connection.0);
                replace(event.frame_seq, frame_seq);
                replace(
                    u64::from(event.event_index),
                    u64::from(first_event_index) + offset as u64,
                );
            }
            fn signed_digits(value: i64) -> usize {
                value.unsigned_abs().checked_ilog10().unwrap_or(0) as usize
                    + 1
                    + usize::from(value < 0)
            }
            size = size - signed_digits(batch.source.received.receive_ts.0)
                + signed_digits(received.receive_ts.0);
            for event in &batch.canonical.events {
                size =
                    size - signed_digits(event.receive_ts.0) + signed_digits(received.receive_ts.0);
            }
            if size > batch.limits.encoded_bytes {
                return Err(NativeObservationError("encoded byte budget after stamping"));
            }
            Some(size)
        } else {
            None
        };
        let observed = matches!(self, Self::Observed(_));
        let batch = match self {
            Self::Canonical(batch) => batch,
            Self::Observed(batch) => &mut batch.canonical,
        };
        batch.session = session;
        batch.frame_seq = frame_seq;
        for (offset, event) in batch.events.iter_mut().enumerate() {
            event.venue = venue;
            event.session = session;
            event.connection = connection;
            event.frame_seq = frame_seq;
            event.event_index = (first_event_index + offset as u32) as u16;
            if observed {
                event.receive_ts = received.unwrap().receive_ts;
            }
        }
        if let Self::Observed(batch) = self {
            batch.source = ObservedBatchSource {
                venue,
                connection,
                received: received.unwrap(),
            };
            debug_assert_eq!(
                batch.encoded_len(batch.limits.encoded_bytes),
                Ok(expected_size.unwrap())
            );
        }
        Ok(end)
    }
}

/// Externally tagged variants deliberately avoid serde's internally buffered
/// tagged-enum representation, which cannot preserve all i128 coefficients.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeObservation {
    HyperliquidTrade(HyperliquidTradeObservation),
    HyperliquidBbo(HyperliquidBboObservation),
    HyperliquidBook(HyperliquidBookObservation),
    HyperliquidCandle(HyperliquidCandleObservation),
    HyperliquidContext(Box<HyperliquidContextObservation>),
    HyperliquidAllMids(HyperliquidMidsObservation),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HyperliquidTradeSide {
    #[serde(rename = "B")]
    Buy,
    #[serde(rename = "A")]
    Sell,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperliquidTradeObservation {
    pub coin: String,
    pub side: HyperliquidTradeSide,
    pub price: Fixed,
    pub size: Fixed,
    pub hash: String,
    pub time_ms: i64,
    pub tid: u64,
    /// Public native participants, as decoded; never inferred from trade side.
    pub users: Vec<String>,
}
impl HyperliquidTradeObservation {
    pub fn unique_id(&self) -> String {
        format!("{}:{}:{}", self.coin, self.time_ms, self.tid)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperliquidLevelObservation {
    pub price: Fixed,
    pub size: Fixed,
    pub orders: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperliquidBboObservation {
    pub coin: String,
    pub time_ms: i64,
    pub sides: [Option<HyperliquidLevelObservation>; 2],
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperliquidBookObservation {
    pub coin: String,
    pub time_ms: i64,
    /// Native order/depth, before the requested canonical depth projection.
    pub sides: [Vec<HyperliquidLevelObservation>; 2],
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperliquidCandleObservation {
    pub coin: String,
    pub interval: String,
    pub open_ms: i64,
    /// Original native T, never reconstructed from open_ms and interval.
    pub end_ms: i64,
    pub open: Fixed,
    pub high: Fixed,
    pub low: Fixed,
    pub close: Fixed,
    pub volume: Fixed,
    pub trades: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperliquidContextObservation {
    pub coin: String,
    pub day_notional_volume: Fixed,
    pub previous_day_price: Fixed,
    pub mark_price: Fixed,
    pub mid_price: Option<Fixed>,
    pub circulating_supply: Option<Fixed>,
    pub funding: Option<Fixed>,
    pub open_interest: Option<Fixed>,
    pub oracle_price: Option<Fixed>,
    pub day_base_volume: Option<Fixed>,
    pub premium: Option<Fixed>,
    pub impact_prices: Option<[Fixed; 2]>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperliquidMidObservation {
    pub coin: String,
    pub mid: Fixed,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperliquidMidsObservation {
    /// A canonical ordering of an unordered native map: strictly increasing coin.
    /// Explicit rows prevent duplicate JSON object keys from being overwritten.
    pub entries: Vec<HyperliquidMidObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeObservationRow {
    /// Positions in this dispatch unit's canonical Vec, not envelope indices
    /// authored later by the runner. One context may own several positions.
    pub canonical_positions: Vec<u16>,
    pub observation: NativeObservation,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeObservationAttachment {
    pub schema_version: u16,
    pub rows: Vec<NativeObservationRow>,
}

/// Additional logical layout bounds. The caller must also bound serialized
/// record bytes and raw input before decoding; these are not allocator bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeObservationLimits {
    pub rows: usize,
    pub associations: usize,
    pub book_levels: usize,
    pub mids: usize,
    pub users: usize,
    pub string_bytes: usize,
}
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NativeObservationUsage {
    pub rows: usize,
    pub associations: usize,
    pub book_levels: usize,
    pub mids: usize,
    pub users: usize,
    pub string_bytes: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid native observation attachment: {0}")]
pub struct NativeObservationError(pub &'static str);

fn add(n: &mut usize, count: usize, limit: usize) -> Result<(), NativeObservationError> {
    *n = n
        .checked_add(count)
        .filter(|n| *n <= limit)
        .ok_or(NativeObservationError("layout budget"))?;
    Ok(())
}
impl NativeObservationAttachment {
    /// Validate a complete partition of canonical positions and native layout
    /// budgets. This does not establish financial validity or release authority.
    pub fn validate_layout(
        &self,
        canonical_len: usize,
        limits: NativeObservationLimits,
    ) -> Result<NativeObservationUsage, NativeObservationError> {
        limits.validate()?;
        if self.schema_version != 1
            || self.rows.is_empty()
            || self.rows.len() > limits.rows
            || canonical_len > u16::MAX as usize
        {
            return Err(NativeObservationError("version or bounds"));
        }
        let mut usage = NativeObservationUsage {
            rows: self.rows.len(),
            ..Default::default()
        };
        let mut covered = vec![false; canonical_len];
        for row in &self.rows {
            add(
                &mut usage.associations,
                row.canonical_positions.len(),
                limits.associations,
            )?;
            if row.canonical_positions.windows(2).any(|w| w[0] >= w[1]) {
                return Err(NativeObservationError(
                    "positions must be ordered and unique",
                ));
            }
            for &position in &row.canonical_positions {
                let Some(seen) = covered.get_mut(usize::from(position)) else {
                    return Err(NativeObservationError("position outside canonical unit"));
                };
                if std::mem::replace(seen, true) {
                    return Err(NativeObservationError(
                        "position has multiple native owners",
                    ));
                }
            }
            let mut string = |s: &str| add(&mut usage.string_bytes, s.len(), limits.string_bytes);
            let exact_associations = match &row.observation {
                NativeObservation::HyperliquidTrade(t) => {
                    string(&t.coin)?;
                    string(&t.hash)?;
                    add(&mut usage.users, t.users.len(), limits.users)?;
                    for user in &t.users {
                        string(user)?;
                    }
                    Some(1)
                }
                NativeObservation::HyperliquidBbo(b) => {
                    string(&b.coin)?;
                    add(
                        &mut usage.book_levels,
                        b.sides.iter().flatten().count(),
                        limits.book_levels,
                    )?;
                    Some(if b.sides.iter().any(Option::is_none) {
                        0
                    } else {
                        1
                    })
                }
                NativeObservation::HyperliquidBook(b) => {
                    string(&b.coin)?;
                    for side in &b.sides {
                        if side.len() > 20 {
                            return Err(NativeObservationError("native book depth"));
                        }
                        add(&mut usage.book_levels, side.len(), limits.book_levels)?;
                    }
                    Some(1)
                }
                NativeObservation::HyperliquidCandle(c) => {
                    string(&c.coin)?;
                    string(&c.interval)?;
                    Some(1)
                }
                NativeObservation::HyperliquidContext(c) => {
                    string(&c.coin)?;
                    None
                }
                NativeObservation::HyperliquidAllMids(m) => {
                    add(&mut usage.mids, m.entries.len(), limits.mids)?;
                    if m.entries.windows(2).any(|w| w[0].coin >= w[1].coin) {
                        return Err(NativeObservationError(
                            "mids must have unique canonical keys",
                        ));
                    }
                    for mid in &m.entries {
                        string(&mid.coin)?;
                    }
                    Some(0)
                }
            };
            if exact_associations.map_or(row.canonical_positions.is_empty(), |n| {
                row.canonical_positions.len() != n
            }) {
                return Err(NativeObservationError("native/canonical association arity"));
            }
        }
        if covered.iter().any(|seen| !seen) {
            return Err(NativeObservationError("unassociated canonical position"));
        }
        Ok(usage)
    }
}

impl NativeObservationLimits {
    pub fn validate(self) -> Result<(), NativeObservationError> {
        const MAX_ITEMS: usize = u16::MAX as usize;
        if self.rows == 0
            || self.rows > MAX_ITEMS
            || self.associations > MAX_ITEMS
            || self.book_levels > MAX_ITEMS * 40
            || self.mids > MAX_ITEMS
            || self.users > MAX_ITEMS
            || self.string_bytes == 0
            || self.string_bytes > 4 * 1024 * 1024
        {
            return Err(NativeObservationError("layout bounds"));
        }
        Ok(())
    }
}
