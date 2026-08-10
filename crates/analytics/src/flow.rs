use std::collections::BTreeMap;

use marketfeed_model::{
    AggressorSide, EventEnvelope, Fixed, InstrumentId, MarketEvent, Price, Quantity,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

use crate::{
    AnalyticsError, FlowSource, GridSpec, MarketSegment, PriceBucket, QuantityUnits, TimeframeSpec,
    invalid_config, overflow,
};

/// Bounded candle-flow capacities.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowConfig {
    pub max_sources: usize,
    pub max_levels_per_source: usize,
    pub max_trades_per_candle: u64,
}

impl FlowConfig {
    pub fn new(
        max_sources: usize,
        max_levels_per_source: usize,
        max_trades_per_candle: u64,
    ) -> Result<Self, AnalyticsError> {
        if max_sources == 0 {
            return Err(invalid_config("max_sources", "must be greater than zero"));
        }
        if max_levels_per_source == 0 {
            return Err(invalid_config(
                "max_levels_per_source",
                "must be greater than zero",
            ));
        }
        if max_trades_per_candle == 0 {
            return Err(invalid_config(
                "max_trades_per_candle",
                "must be greater than zero",
            ));
        }
        Ok(Self {
            max_sources,
            max_levels_per_source,
            max_trades_per_candle,
        })
    }

    pub fn validate(&self) -> Result<(), AnalyticsError> {
        Self::new(
            self.max_sources,
            self.max_levels_per_source,
            self.max_trades_per_candle,
        )
        .map(|_| ())
    }
}

/// Canonical direct input for one trade-derived flow update.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeInput {
    pub instrument: InstrumentId,
    pub source: FlowSource,
    pub timestamp_ns: i64,
    pub price: Price,
    pub quantity: Quantity,
    pub aggressor: AggressorSide,
}

impl TradeInput {
    /// Convert a normalized trade envelope, preferring exchange event time.
    pub fn from_envelope(
        envelope: &EventEnvelope,
        segment: MarketSegment,
    ) -> Result<Self, AnalyticsError> {
        let instrument = envelope
            .instrument
            .ok_or(AnalyticsError::MissingInstrument)?;
        let MarketEvent::Trade(trade) = &envelope.payload else {
            return Err(AnalyticsError::NonTradeEvent);
        };
        Ok(Self {
            instrument,
            source: FlowSource {
                venue: envelope.venue,
                segment,
            },
            timestamp_ns: envelope.exchange_ts.unwrap_or(envelope.receive_ts).0,
            price: trade.price,
            quantity: trade.quantity,
            aggressor: trade.aggressor,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlowState {
    Live,
    Final,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceLevelFlow {
    pub price: Price,
    pub buy_volume: Quantity,
    pub sell_volume: Quantity,
    pub unknown_volume: Quantity,
    pub total_volume: Quantity,
    pub delta: Fixed,
    pub trade_count: u64,
    pub first_timestamp_ns: i64,
    pub last_timestamp_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceCandleFlow {
    pub source: FlowSource,
    pub high: Option<Price>,
    pub low: Option<Price>,
    pub buy_volume: Quantity,
    pub sell_volume: Quantity,
    pub unknown_volume: Quantity,
    pub total_volume: Quantity,
    pub delta: Fixed,
    pub trade_count: u64,
    pub levels: Vec<PriceLevelFlow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandleFlow {
    pub schema_version: u16,
    pub state: FlowState,
    pub instrument: InstrumentId,
    pub start_ts: i64,
    pub end_ts: i64,
    pub high: Option<Price>,
    pub low: Option<Price>,
    pub total_volume: Quantity,
    pub trade_count: u64,
    pub sources: Vec<SourceCandleFlow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LevelAccumulator {
    buy_units: i128,
    sell_units: i128,
    unknown_units: i128,
    total_units: i128,
    trade_count: u64,
    first_timestamp_ns: i64,
    last_timestamp_ns: i64,
}

impl LevelAccumulator {
    fn new(timestamp_ns: i64) -> Self {
        Self {
            buy_units: 0,
            sell_units: 0,
            unknown_units: 0,
            total_units: 0,
            trade_count: 0,
            first_timestamp_ns: timestamp_ns,
            last_timestamp_ns: timestamp_ns,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SourceAccumulator {
    high: Option<Price>,
    low: Option<Price>,
    buy_units: i128,
    sell_units: i128,
    unknown_units: i128,
    total_units: i128,
    trade_count: u64,
    levels: BTreeMap<PriceBucket, LevelAccumulator>,
}

impl SourceAccumulator {
    fn new() -> Self {
        Self {
            high: None,
            low: None,
            buy_units: 0,
            sell_units: 0,
            unknown_units: 0,
            total_units: 0,
            trade_count: 0,
            levels: BTreeMap::new(),
        }
    }
}

/// Exact bounded price-level flow for one instrument.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandleFlowBuilder {
    instrument: InstrumentId,
    grid: GridSpec,
    time: TimeframeSpec,
    config: FlowConfig,
    candle_start: Option<i64>,
    last_timestamp_ns: Option<i64>,
    high: Option<Price>,
    low: Option<Price>,
    total_units: i128,
    trade_count: u64,
    #[serde(
        serialize_with = "serialize_sources",
        deserialize_with = "deserialize_sources"
    )]
    sources: BTreeMap<FlowSource, SourceAccumulator>,
}

fn serialize_sources<S>(
    sources: &BTreeMap<FlowSource, SourceAccumulator>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    sources.iter().collect::<Vec<_>>().serialize(serializer)
}

fn deserialize_sources<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<FlowSource, SourceAccumulator>, D::Error>
where
    D: Deserializer<'de>,
{
    let pairs = Vec::<(FlowSource, SourceAccumulator)>::deserialize(deserializer)?;
    let mut sources = BTreeMap::new();
    for (source, accumulator) in pairs {
        if sources.insert(source, accumulator).is_some() {
            return Err(D::Error::custom("duplicate flow source"));
        }
    }
    Ok(sources)
}

impl CandleFlowBuilder {
    pub const SCHEMA_VERSION: u16 = 1;

    pub fn new(
        instrument: InstrumentId,
        grid: GridSpec,
        time: TimeframeSpec,
        config: FlowConfig,
    ) -> Result<Self, AnalyticsError> {
        grid.validate()?;
        time.validate()?;
        config.validate()?;
        Ok(Self {
            instrument,
            grid,
            time,
            config,
            candle_start: None,
            last_timestamp_ns: None,
            high: None,
            low: None,
            total_units: 0,
            trade_count: 0,
            sources: BTreeMap::new(),
        })
    }

    pub fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    pub fn config(&self) -> FlowConfig {
        self.config
    }

    pub fn ingest(&mut self, trade: TradeInput) -> Result<Option<CandleFlow>, AnalyticsError> {
        if trade.instrument != self.instrument {
            return Err(AnalyticsError::InstrumentMismatch {
                expected: self.instrument.0,
                actual: trade.instrument.0,
            });
        }
        self.reject_late(trade.timestamp_ns)?;
        let bucket = self.grid.price_bucket(trade.price)?;
        let price = self
            .grid
            .price_at_tick(self.grid.price_tick(trade.price)?)?;
        let quantity = self.grid.quantity_units(trade.quantity)?;
        let target_candle = self.time.checked_candle_start(trade.timestamp_ns)?;

        match self.candle_start {
            None => {
                self.preflight_add(trade.source, bucket, quantity, trade.aggressor)?;
                self.candle_start = Some(target_candle);
                self.apply_add(
                    trade.source,
                    bucket,
                    price,
                    quantity,
                    trade.aggressor,
                    trade.timestamp_ns,
                )?;
                self.last_timestamp_ns = Some(trade.timestamp_ns);
                Ok(None)
            }
            Some(current) if target_candle > current => {
                let finalized = self.snapshot(FlowState::Final)?;
                let mut replacement =
                    Self::new(self.instrument, self.grid, self.time, self.config)?;
                replacement.preflight_add(trade.source, bucket, quantity, trade.aggressor)?;
                replacement.candle_start = Some(target_candle);
                replacement.apply_add(
                    trade.source,
                    bucket,
                    price,
                    quantity,
                    trade.aggressor,
                    trade.timestamp_ns,
                )?;
                replacement.last_timestamp_ns = Some(trade.timestamp_ns);
                *self = replacement;
                Ok(Some(finalized))
            }
            Some(current) if target_candle < current => Err(AnalyticsError::LateTrade {
                timestamp_ns: trade.timestamp_ns,
                finalized_before_ns: current,
            }),
            Some(_) => {
                self.preflight_add(trade.source, bucket, quantity, trade.aggressor)?;
                self.apply_add(
                    trade.source,
                    bucket,
                    price,
                    quantity,
                    trade.aggressor,
                    trade.timestamp_ns,
                )?;
                self.last_timestamp_ns = Some(trade.timestamp_ns);
                Ok(None)
            }
        }
    }

    pub fn advance_to(&mut self, timestamp_ns: i64) -> Result<Option<CandleFlow>, AnalyticsError> {
        self.reject_late(timestamp_ns)?;
        let Some(current) = self.candle_start else {
            self.last_timestamp_ns = Some(timestamp_ns);
            return Ok(None);
        };
        let target = self.time.checked_candle_start(timestamp_ns)?;
        if target > current {
            let finalized = self.snapshot(FlowState::Final)?;
            self.clear_active();
            self.last_timestamp_ns = Some(timestamp_ns);
            Ok(Some(finalized))
        } else {
            self.last_timestamp_ns = Some(timestamp_ns);
            Ok(None)
        }
    }

    pub fn live_snapshot(&self) -> Result<Option<CandleFlow>, AnalyticsError> {
        self.candle_start
            .map(|_| self.snapshot(FlowState::Live))
            .transpose()
    }

    pub fn finish(&mut self) -> Result<Option<CandleFlow>, AnalyticsError> {
        if self.candle_start.is_none() {
            return Ok(None);
        }
        let finalized = self.snapshot(FlowState::Final)?;
        self.clear_active();
        Ok(Some(finalized))
    }

    pub fn validate(&self) -> Result<(), AnalyticsError> {
        self.grid.validate()?;
        self.time.validate()?;
        self.config.validate()?;
        if self.sources.len() > self.config.max_sources {
            return Err(AnalyticsError::CorruptSnapshot {
                detail: "source count exceeds configured capacity".to_owned(),
            });
        }
        if self
            .sources
            .values()
            .any(|source| source.levels.len() > self.config.max_levels_per_source)
        {
            return Err(AnalyticsError::CorruptSnapshot {
                detail: "source level count exceeds configured capacity".to_owned(),
            });
        }
        if self.trade_count > self.config.max_trades_per_candle {
            return Err(AnalyticsError::CorruptSnapshot {
                detail: "trade count exceeds configured capacity".to_owned(),
            });
        }
        if self.total_units < 0 {
            return Err(AnalyticsError::CorruptSnapshot {
                detail: "negative candle volume".to_owned(),
            });
        }
        let source_total = self.sources.values().try_fold(0i128, |sum, source| {
            if source.total_units < 0
                || source.buy_units < 0
                || source.sell_units < 0
                || source.unknown_units < 0
            {
                return Err(AnalyticsError::CorruptSnapshot {
                    detail: "negative source volume".to_owned(),
                });
            }
            let level_total = source.levels.values().try_fold(0i128, |level_sum, level| {
                if level.total_units < 0
                    || level.buy_units < 0
                    || level.sell_units < 0
                    || level.unknown_units < 0
                {
                    return Err(AnalyticsError::CorruptSnapshot {
                        detail: "negative price-level volume".to_owned(),
                    });
                }
                let sides = level
                    .buy_units
                    .checked_add(level.sell_units)
                    .and_then(|value| value.checked_add(level.unknown_units))
                    .ok_or_else(|| overflow("validating price-level volume"))?;
                if sides != level.total_units {
                    return Err(AnalyticsError::CorruptSnapshot {
                        detail: "price-level total does not equal side sum".to_owned(),
                    });
                }
                level_sum
                    .checked_add(level.total_units)
                    .ok_or_else(|| overflow("validating source volume"))
            })?;
            if level_total != source.total_units {
                return Err(AnalyticsError::CorruptSnapshot {
                    detail: "source total does not equal level sum".to_owned(),
                });
            }
            sum.checked_add(source.total_units)
                .ok_or_else(|| overflow("validating candle volume"))
        })?;
        if source_total != self.total_units {
            return Err(AnalyticsError::CorruptSnapshot {
                detail: "candle total does not equal source sum".to_owned(),
            });
        }
        if self.candle_start.is_none()
            && (!self.sources.is_empty() || self.trade_count != 0 || self.total_units != 0)
        {
            return Err(AnalyticsError::CorruptSnapshot {
                detail: "inactive candle retains flow state".to_owned(),
            });
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

    fn preflight_add(
        &self,
        source: FlowSource,
        bucket: PriceBucket,
        quantity: QuantityUnits,
        aggressor: AggressorSide,
    ) -> Result<(), AnalyticsError> {
        if self.trade_count >= self.config.max_trades_per_candle {
            return Err(AnalyticsError::CapacityExceeded {
                resource: "trades per candle",
                limit: usize::try_from(self.config.max_trades_per_candle).unwrap_or(usize::MAX),
            });
        }
        let source_state = self.sources.get(&source);
        if source_state.is_none() && self.sources.len() >= self.config.max_sources {
            return Err(AnalyticsError::CapacityExceeded {
                resource: "sources per candle",
                limit: self.config.max_sources,
            });
        }
        if source_state.is_some_and(|state| {
            !state.levels.contains_key(&bucket)
                && state.levels.len() >= self.config.max_levels_per_source
        }) {
            return Err(AnalyticsError::CapacityExceeded {
                resource: "price levels per source candle",
                limit: self.config.max_levels_per_source,
            });
        }

        self.trade_count
            .checked_add(1)
            .ok_or_else(|| overflow("adding candle trade count"))?;
        self.total_units
            .checked_add(quantity.0)
            .ok_or_else(|| overflow("adding candle volume"))?;

        if let Some(state) = source_state {
            state
                .trade_count
                .checked_add(1)
                .ok_or_else(|| overflow("adding source trade count"))?;
            state
                .total_units
                .checked_add(quantity.0)
                .ok_or_else(|| overflow("adding source volume"))?;
            if let Some(level) = state.levels.get(&bucket) {
                level
                    .trade_count
                    .checked_add(1)
                    .ok_or_else(|| overflow("adding price-level trade count"))?;
                level
                    .total_units
                    .checked_add(quantity.0)
                    .ok_or_else(|| overflow("adding price-level volume"))?;
                preflight_side(
                    level.buy_units,
                    level.sell_units,
                    level.unknown_units,
                    quantity,
                    aggressor,
                )?;
            }
            preflight_side(
                state.buy_units,
                state.sell_units,
                state.unknown_units,
                quantity,
                aggressor,
            )?;
        }
        Ok(())
    }

    fn apply_add(
        &mut self,
        source: FlowSource,
        bucket: PriceBucket,
        price: Price,
        quantity: QuantityUnits,
        aggressor: AggressorSide,
        timestamp_ns: i64,
    ) -> Result<(), AnalyticsError> {
        self.trade_count = self
            .trade_count
            .checked_add(1)
            .ok_or_else(|| overflow("adding candle trade count"))?;
        self.total_units = self
            .total_units
            .checked_add(quantity.0)
            .ok_or_else(|| overflow("adding candle volume"))?;
        self.high = Some(
            self.high
                .map_or(price, |current| crate::max_price(current, price)),
        );
        self.low = Some(
            self.low
                .map_or(price, |current| crate::min_price(current, price)),
        );

        let source_state = self
            .sources
            .entry(source)
            .or_insert_with(SourceAccumulator::new);
        source_state.trade_count = source_state
            .trade_count
            .checked_add(1)
            .ok_or_else(|| overflow("adding source trade count"))?;
        source_state.total_units = source_state
            .total_units
            .checked_add(quantity.0)
            .ok_or_else(|| overflow("adding source volume"))?;
        apply_side(
            &mut source_state.buy_units,
            &mut source_state.sell_units,
            &mut source_state.unknown_units,
            quantity,
            aggressor,
        )?;
        source_state.high = Some(
            source_state
                .high
                .map_or(price, |current| crate::max_price(current, price)),
        );
        source_state.low = Some(
            source_state
                .low
                .map_or(price, |current| crate::min_price(current, price)),
        );

        let level = source_state
            .levels
            .entry(bucket)
            .or_insert_with(|| LevelAccumulator::new(timestamp_ns));
        level.trade_count = level
            .trade_count
            .checked_add(1)
            .ok_or_else(|| overflow("adding price-level trade count"))?;
        level.total_units = level
            .total_units
            .checked_add(quantity.0)
            .ok_or_else(|| overflow("adding price-level volume"))?;
        apply_side(
            &mut level.buy_units,
            &mut level.sell_units,
            &mut level.unknown_units,
            quantity,
            aggressor,
        )?;
        level.last_timestamp_ns = timestamp_ns;
        Ok(())
    }

    fn snapshot(&self, state: FlowState) -> Result<CandleFlow, AnalyticsError> {
        let start_ts = self
            .candle_start
            .ok_or_else(|| AnalyticsError::CorruptSnapshot {
                detail: "cannot snapshot an inactive candle".to_owned(),
            })?;
        let end_ts = start_ts
            .checked_add(self.time.candle_ns)
            .ok_or_else(|| overflow("calculating candle end"))?;
        let sources = self
            .sources
            .iter()
            .map(|(source, accumulator)| self.source_snapshot(*source, accumulator))
            .collect::<Result<Vec<_>, AnalyticsError>>()?;
        Ok(CandleFlow {
            schema_version: Self::SCHEMA_VERSION,
            state,
            instrument: self.instrument,
            start_ts,
            end_ts,
            high: self.high,
            low: self.low,
            total_volume: self.grid.quantity_at(QuantityUnits(self.total_units))?,
            trade_count: self.trade_count,
            sources,
        })
    }

    fn source_snapshot(
        &self,
        source: FlowSource,
        accumulator: &SourceAccumulator,
    ) -> Result<SourceCandleFlow, AnalyticsError> {
        let levels = accumulator
            .levels
            .iter()
            .map(|(bucket, level)| {
                let delta = level
                    .buy_units
                    .checked_sub(level.sell_units)
                    .ok_or_else(|| overflow("calculating price-level delta"))?;
                Ok(PriceLevelFlow {
                    price: self.grid.price_at(*bucket)?,
                    buy_volume: self.grid.quantity_at(QuantityUnits(level.buy_units))?,
                    sell_volume: self.grid.quantity_at(QuantityUnits(level.sell_units))?,
                    unknown_volume: self.grid.quantity_at(QuantityUnits(level.unknown_units))?,
                    total_volume: self.grid.quantity_at(QuantityUnits(level.total_units))?,
                    delta: self
                        .grid
                        .signed_quantity_at(crate::SignedQuantityUnits(delta)),
                    trade_count: level.trade_count,
                    first_timestamp_ns: level.first_timestamp_ns,
                    last_timestamp_ns: level.last_timestamp_ns,
                })
            })
            .collect::<Result<Vec<_>, AnalyticsError>>()?;
        let delta = accumulator
            .buy_units
            .checked_sub(accumulator.sell_units)
            .ok_or_else(|| overflow("calculating source delta"))?;
        Ok(SourceCandleFlow {
            source,
            high: accumulator.high,
            low: accumulator.low,
            buy_volume: self
                .grid
                .quantity_at(QuantityUnits(accumulator.buy_units))?,
            sell_volume: self
                .grid
                .quantity_at(QuantityUnits(accumulator.sell_units))?,
            unknown_volume: self
                .grid
                .quantity_at(QuantityUnits(accumulator.unknown_units))?,
            total_volume: self
                .grid
                .quantity_at(QuantityUnits(accumulator.total_units))?,
            delta: self
                .grid
                .signed_quantity_at(crate::SignedQuantityUnits(delta)),
            trade_count: accumulator.trade_count,
            levels,
        })
    }

    fn clear_active(&mut self) {
        self.candle_start = None;
        self.high = None;
        self.low = None;
        self.total_units = 0;
        self.trade_count = 0;
        self.sources.clear();
    }
}

fn preflight_side(
    buy_units: i128,
    sell_units: i128,
    unknown_units: i128,
    quantity: QuantityUnits,
    aggressor: AggressorSide,
) -> Result<(), AnalyticsError> {
    match aggressor {
        AggressorSide::Buy => buy_units
            .checked_add(quantity.0)
            .ok_or_else(|| overflow("adding buy volume")),
        AggressorSide::Sell => sell_units
            .checked_add(quantity.0)
            .ok_or_else(|| overflow("adding sell volume")),
        AggressorSide::Unknown => unknown_units
            .checked_add(quantity.0)
            .ok_or_else(|| overflow("adding unknown volume")),
    }
    .map(|_| ())
}

fn apply_side(
    buy_units: &mut i128,
    sell_units: &mut i128,
    unknown_units: &mut i128,
    quantity: QuantityUnits,
    aggressor: AggressorSide,
) -> Result<(), AnalyticsError> {
    match aggressor {
        AggressorSide::Buy => {
            *buy_units = buy_units
                .checked_add(quantity.0)
                .ok_or_else(|| overflow("adding buy volume"))?;
        }
        AggressorSide::Sell => {
            *sell_units = sell_units
                .checked_add(quantity.0)
                .ok_or_else(|| overflow("adding sell volume"))?;
        }
        AggressorSide::Unknown => {
            *unknown_units = unknown_units
                .checked_add(quantity.0)
                .ok_or_else(|| overflow("adding unknown volume"))?;
        }
    }
    Ok(())
}
