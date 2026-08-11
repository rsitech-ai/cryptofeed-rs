//! Server-side book cache + drop_newest tape rings.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use marketfeed_adapter_api::EventBatch;
use marketfeed_analytics::{
    AdaptivePreset, AdaptiveThreshold, BubbleConfig, BubbleDetector, BubbleFilter, BubbleMode,
    BubbleShape, BubbleStyle, BubbleTier, CandleFlowBuilder, DetectionPhase, FlowConfig,
    FlowSource, GridSpec, LabelMode, MarketSegment, MergeConfig, OrderFlowBubble, PerformanceMode,
    ProfileConfig, ProfileState, SessionProfile, SessionProfileBuilder, SourceSelector,
    StructuralLevel, StructuralLevelConfig, StructuralLevelEngine, StructuralLevelKind,
    StructuralLevelState, ThresholdMode, TimeframeSpec, TradeInput, ValueAreaBasis,
};
use marketfeed_book::OrderBook;
use marketfeed_dispatch::PushOutcome;
use marketfeed_model::{
    AggressorSide, BookDelta, BookLevel, BookSide, BookSnapshot, CatalogView, Fixed, InstrumentId,
    MarketEvent, Price, Quantity, SystemEvent, VenueId,
};
use marketfeed_sinks::{EventSink, SinkError};
use serde::Serialize;

use crate::config::DaemonConfig;
use crate::state::DaemonState;

const DEPTH_SAMPLE_INTERVAL_MS: u64 = 100;
// The UI requests at most 600 warm-start columns and then maintains its longer,
// tiered history client-side. Keeping more complete snapshots only increases
// resident memory without changing the rendered heatmap.
const DEPTH_HISTORY_CAPACITY: usize = 600;
const UI_BUBBLE_CALIBRATION_CANDLES: usize = 8;

/// Tunables for tape rings (from `[telemetry]`).
#[derive(Debug, Clone, Copy)]
pub struct ViewPlaneConfig {
    pub tape_capacity: usize,
    pub tape_max_per_sec: u32,
}

impl Default for ViewPlaneConfig {
    fn default() -> Self {
        Self {
            tape_capacity: 256,
            tape_max_per_sec: 50,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewStatus {
    pub live: bool,
    pub ready: bool,
    pub uptime_secs: u64,
    pub lifecycle: String,
    pub disk_pressure: bool,
    pub shutdown_draining: bool,
    pub recording: ViewRecordingStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grafana_base_url: Option<String>,
    pub alert_webhook_configured: bool,
    pub venues: Vec<ViewVenueStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewRecordingStatus {
    pub enabled: bool,
    pub healthy: bool,
    pub queue_len: u64,
    pub frames_dropped: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewVenueStatus {
    pub id: String,
    pub adapter: String,
    pub required: bool,
    pub live: bool,
    pub symbols: Vec<String>,
    pub events_dispatched: u64,
    pub events_dropped: u64,
    pub reconnects: u64,
    pub book_invalidations: u64,
    pub valid_books: u64,
    pub queue_occupancy: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feed_lag_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_ts_ns: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_trade_ts_ns: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_quote_ts_ns: Option<i64>,
    pub tape_trades: u64,
    pub tape_quotes: u64,
    pub tape_trades_dropped: u64,
    pub tape_quotes_dropped: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewBookSnapshot {
    pub venue: String,
    pub instrument: u32,
    pub symbol: Option<String>,
    pub depth: Option<u32>,
    pub bids: Vec<ViewLevel>,
    pub asks: Vec<ViewLevel>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewLevel {
    pub price: String,
    pub quantity: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewDepthSample {
    pub event_ts_ns: i64,
    pub epoch: u64,
    pub bids: Vec<ViewLevel>,
    pub asks: Vec<ViewLevel>,
}

#[derive(Debug, Clone)]
struct StoredDepthSample {
    event_ts_ns: i64,
    epoch: u64,
    price_scale: u8,
    quantity_scale: u8,
    bids: Vec<StoredDepthLevel>,
    asks: Vec<StoredDepthLevel>,
}

#[derive(Debug, Clone, Copy)]
struct StoredDepthLevel {
    price_coefficient: i128,
    quantity_coefficient: i128,
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewDepthHistory {
    pub schema_version: u16,
    pub venue: String,
    pub instrument: u32,
    pub symbol: Option<String>,
    pub sample_interval_ms: u64,
    pub capacity: usize,
    pub coalesced_samples: u64,
    pub evicted_samples: u64,
    pub samples: Vec<ViewDepthSample>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewDomRow {
    pub price: String,
    pub bid_quantity: String,
    pub ask_quantity: String,
    pub bid_cumulative_notional: String,
    pub ask_cumulative_notional: String,
    pub imbalance_bps: i32,
    pub mbp_delta_quantity: String,
    pub mbp_delta_notional: String,
    pub buy_executed_notional: String,
    pub sell_executed_notional: String,
    pub unknown_executed_notional: String,
    pub total_executed_notional: String,
    pub executed_delta_notional: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewDomSnapshot {
    pub schema_version: u16,
    pub venue: String,
    pub instrument: u32,
    pub symbol: Option<String>,
    pub revision: u64,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub execution_window_sec: u64,
    pub epoch: u64,
    pub rows: Vec<ViewDomRow>,
}

/// Whether catalog grid metadata came from the venue or from a placeholder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogAuthority {
    Authoritative,
    Placeholder,
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewProfileSnapshot {
    pub schema_version: u16,
    pub venue: String,
    pub instrument: u32,
    pub symbol: Option<String>,
    pub revision: u64,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub basis: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_area_bps: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_ts_ns: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_ts_ns: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub high: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub low: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_volume: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub poc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vah: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub val: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tpo_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rotation_factor: Option<i32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewBubble {
    pub id: u64,
    pub phase: String,
    pub candle_start_ns: i64,
    pub candle_end_ns: i64,
    pub tier: String,
    pub mode: String,
    pub direction: String,
    pub anchor_price: String,
    pub low_price: String,
    pub high_price: String,
    pub total_volume: String,
    pub delta: String,
    pub strength: String,
    pub threshold: String,
    pub visual_size: u16,
    pub shape: String,
    pub merged_count: u16,
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewBubbleSnapshot {
    pub schema_version: u16,
    pub venue: String,
    pub instrument: u32,
    pub symbol: Option<String>,
    pub revision: u64,
    pub status: String,
    pub mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub bubbles: Vec<ViewBubble>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewStructuralLevel {
    pub id: u64,
    pub kind: String,
    pub state: String,
    pub source_bubble_id: u64,
    pub direction: String,
    pub tier: String,
    pub price: String,
    pub strength: String,
    pub created_at_ns: i64,
    pub touched_at_ns: Option<i64>,
    pub window_start_ns: Option<i64>,
    pub expires_at_ns: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewStructuralLevelSnapshot {
    pub schema_version: u16,
    pub venue: String,
    pub instrument: u32,
    pub symbol: Option<String>,
    pub revision: u64,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub levels: Vec<ViewStructuralLevel>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewFunding {
    pub rate: String,
    pub event_ts_ns: i64,
    pub next_funding_ts_ns: Option<i64>,
    pub age_ms: u64,
    pub stale: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewOpenInterest {
    pub quantity: String,
    pub event_ts_ns: i64,
    pub age_ms: u64,
    pub stale: bool,
    pub change: Option<String>,
    pub change_interval_ns: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewLiquidation {
    pub price: String,
    pub quantity: String,
    pub side: String,
    pub event_ts_ns: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewFundingDivergence {
    pub compatible_venues: usize,
    pub min_rate: String,
    pub max_rate: String,
    pub spread: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewDerivativesSnapshot {
    pub schema_version: u16,
    pub venue: String,
    pub instrument: u32,
    pub symbol: Option<String>,
    pub revision: u64,
    pub status: String,
    pub funding: Option<ViewFunding>,
    pub open_interest: Option<ViewOpenInterest>,
    pub funding_divergence: Option<ViewFundingDivergence>,
    pub liquidations: Vec<ViewLiquidation>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TapeEntry {
    Trade {
        venue: String,
        instrument: u32,
        symbol: Option<String>,
        price: String,
        quantity: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        notional: Option<String>,
        aggressor: String,
        trade_id: Option<String>,
        exchange_ts_ns: Option<i64>,
        receive_ts_ns: i64,
    },
    Quote {
        venue: String,
        instrument: u32,
        symbol: Option<String>,
        bid_price: String,
        bid_quantity: Option<String>,
        ask_price: String,
        ask_quantity: Option<String>,
        exchange_ts_ns: Option<i64>,
        receive_ts_ns: i64,
    },
}

#[derive(Debug)]
struct TapeRing {
    entries: VecDeque<TapeEntry>,
    capacity: usize,
    /// Sliding window for sampling.
    window_start: Instant,
    window_count: u32,
    max_per_sec: u32,
    dropped: u64,
}

impl TapeRing {
    fn new(capacity: usize, max_per_sec: u32) -> Self {
        Self {
            entries: VecDeque::with_capacity(capacity.min(1024)),
            capacity: capacity.max(1),
            window_start: Instant::now(),
            window_count: 0,
            max_per_sec,
            dropped: 0,
        }
    }

    fn push(&mut self, entry: TapeEntry) {
        if self.max_per_sec > 0 {
            let now = Instant::now();
            if now.duration_since(self.window_start) >= Duration::from_secs(1) {
                self.window_start = now;
                self.window_count = 0;
            }
            if self.window_count >= self.max_per_sec {
                self.dropped = self.dropped.saturating_add(1);
                return;
            }
            self.window_count = self.window_count.saturating_add(1);
        }
        if self.entries.len() >= self.capacity {
            self.entries.pop_front();
            self.dropped = self.dropped.saturating_add(1);
        }
        self.entries.push_back(entry);
    }

    fn snapshot(&self, limit: usize) -> Vec<TapeEntry> {
        let n = limit.min(self.entries.len());
        self.entries.iter().rev().take(n).cloned().collect()
    }

    fn len(&self) -> u64 {
        self.entries.len() as u64
    }

    fn dropped_count(&self) -> u64 {
        self.dropped
    }
}

/// Per-instrument dual rings so quote floods cannot evict trades (and vice versa).
#[derive(Debug)]
struct InstrumentTape {
    trades: TapeRing,
    quotes: TapeRing,
}

#[derive(Debug, Clone)]
struct TimedFunding {
    rate: Fixed,
    event_ts_ns: i64,
    next_funding_ts_ns: Option<i64>,
}

#[derive(Debug, Clone)]
struct TimedOpenInterest {
    quantity: Quantity,
    event_ts_ns: i64,
    previous: Option<(Quantity, i64)>,
}

#[derive(Debug, Clone)]
struct TimedLiquidation {
    price: Price,
    quantity: Quantity,
    side: AggressorSide,
    event_ts_ns: i64,
}

#[derive(Debug, Default)]
struct DerivativeProjection {
    funding: Option<TimedFunding>,
    open_interest: Option<TimedOpenInterest>,
    liquidations: VecDeque<TimedLiquidation>,
    revision: u64,
}

impl InstrumentTape {
    fn new(capacity: usize, max_per_sec: u32) -> Self {
        Self {
            trades: TapeRing::new(capacity, max_per_sec),
            quotes: TapeRing::new(capacity, max_per_sec),
        }
    }

    fn push_trade(&mut self, entry: TapeEntry) -> u64 {
        let before = self.trades.dropped;
        self.trades.push(entry);
        self.trades.dropped.saturating_sub(before)
    }

    fn push_quote(&mut self, entry: TapeEntry) -> u64 {
        let before = self.quotes.dropped;
        self.quotes.push(entry);
        self.quotes.dropped.saturating_sub(before)
    }

    /// Newest-first snapshot. `kind` filters to `"trade"` / `"quote"`; `None` merges both.
    fn snapshot(&self, limit: usize, kind: Option<&str>) -> Vec<TapeEntry> {
        let limit = limit.max(1);
        match kind {
            Some("trade") | Some("trades") => self.trades.snapshot(limit),
            Some("quote") | Some("quotes") => self.quotes.snapshot(limit),
            _ => merge_newest(
                self.trades.snapshot(limit),
                self.quotes.snapshot(limit),
                limit,
            ),
        }
    }

    fn trade_stats(&self) -> (u64, u64) {
        (self.trades.len(), self.trades.dropped_count())
    }

    fn quote_stats(&self) -> (u64, u64) {
        (self.quotes.len(), self.quotes.dropped_count())
    }
}

fn tape_receive_ts(entry: &TapeEntry) -> i64 {
    match entry {
        TapeEntry::Trade { receive_ts_ns, .. } => *receive_ts_ns,
        TapeEntry::Quote { receive_ts_ns, .. } => *receive_ts_ns,
    }
}

fn merge_newest(a: Vec<TapeEntry>, b: Vec<TapeEntry>, limit: usize) -> Vec<TapeEntry> {
    let mut i = 0;
    let mut j = 0;
    let mut out = Vec::with_capacity(limit.min(a.len() + b.len()));
    while out.len() < limit && (i < a.len() || j < b.len()) {
        let take_a = match (a.get(i), b.get(j)) {
            (Some(ea), Some(eb)) => tape_receive_ts(ea) >= tape_receive_ts(eb),
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        if take_a {
            out.push(a[i].clone());
            i += 1;
        } else {
            out.push(b[j].clone());
            j += 1;
        }
    }
    out
}

#[derive(Debug)]
struct LiveBook {
    book: OrderBook,
}

#[derive(Debug)]
struct ProfileProjection {
    builder: Option<SessionProfileBuilder>,
    revision: u64,
    unavailable_reason: Option<String>,
    last_error: Option<String>,
}

#[derive(Debug)]
struct BubbleProjection {
    flow: Option<CandleFlowBuilder>,
    volume: Option<BubbleDetector>,
    delta: Option<BubbleDetector>,
    levels: Option<StructuralLevelEngine>,
    segment: MarketSegment,
    finalized_volume: VecDeque<ViewBubble>,
    finalized_delta: VecDeque<ViewBubble>,
    revision: u64,
    unavailable_reason: Option<String>,
    last_error: Option<String>,
}

struct DeferredTradeAnalytics {
    profile: Option<Arc<Mutex<ProfileProjection>>>,
    bubbles: Option<Arc<Mutex<BubbleProjection>>>,
    venue: VenueId,
    instrument: InstrumentId,
    timestamp_ns: i64,
    price: Price,
    quantity: Quantity,
    aggressor: AggressorSide,
}

impl BubbleProjection {
    fn from_catalog(
        instrument: InstrumentId,
        segment: MarketSegment,
        price_scale: u8,
        quantity_scale: u8,
        tick_size: Fixed,
        quantity_increment: Fixed,
        authority: CatalogAuthority,
    ) -> Self {
        let unavailable = |reason: String| Self {
            flow: None,
            volume: None,
            delta: None,
            levels: None,
            segment,
            finalized_volume: VecDeque::new(),
            finalized_delta: VecDeque::new(),
            revision: 0,
            unavailable_reason: Some(reason),
            last_error: None,
        };
        if authority == CatalogAuthority::Placeholder {
            return unavailable("catalog_not_authoritative".into());
        }
        let built = (|| {
            let grid = GridSpec::new(price_scale, quantity_scale, tick_size, 1)?;
            let time = TimeframeSpec::new(
                60_000_000_000,
                1_800_000_000_000,
                86_400_000_000_000,
                0,
                604_800_000_000_000,
            )?;
            let flow = CandleFlowBuilder::new(
                instrument,
                grid,
                time,
                FlowConfig::new(1, 100_000, 1_000_000)?,
            )?;
            let volume = BubbleDetector::new(
                grid,
                default_bubble_config(quantity_increment, segment, BubbleMode::Volume)?,
            )?;
            let delta = BubbleDetector::new(
                grid,
                default_bubble_config(quantity_increment, segment, BubbleMode::Delta)?,
            )?;
            let levels = StructuralLevelEngine::new(grid, StructuralLevelConfig::new(1, 256, 3)?)?;
            Ok::<_, marketfeed_analytics::AnalyticsError>((flow, volume, delta, levels))
        })();
        match built {
            Ok((flow, volume, delta, levels)) => Self {
                flow: Some(flow),
                volume: Some(volume),
                delta: Some(delta),
                levels: Some(levels),
                segment,
                finalized_volume: VecDeque::new(),
                finalized_delta: VecDeque::new(),
                revision: 0,
                unavailable_reason: None,
                last_error: None,
            },
            Err(error) => unavailable(format!("invalid_catalog_grid: {error}")),
        }
    }

    fn ingest(
        &mut self,
        venue: VenueId,
        instrument: InstrumentId,
        timestamp_ns: i64,
        price: Price,
        quantity: Quantity,
        aggressor: AggressorSide,
    ) {
        let Some(flow) = self.flow.as_mut() else {
            return;
        };
        let input = TradeInput {
            instrument,
            source: FlowSource {
                venue,
                segment: self.segment,
            },
            timestamp_ns,
            price,
            quantity,
            aggressor,
        };
        match flow.ingest(input) {
            Ok(finalized) => {
                if let Some(finalized) = finalized {
                    let result = (|| {
                        let volume = self
                            .volume
                            .as_mut()
                            .ok_or_else(|| "volume detector unavailable".to_string())?;
                        let delta = self
                            .delta
                            .as_mut()
                            .ok_or_else(|| "delta detector unavailable".to_string())?;
                        let volume_rows = volume
                            .detect(&finalized, DetectionPhase::Final)
                            .map_err(|error| error.to_string())?;
                        let delta_rows = delta
                            .detect(&finalized, DetectionPhase::Final)
                            .map_err(|error| error.to_string())?;
                        self.levels
                            .as_mut()
                            .ok_or_else(|| "structural level engine unavailable".to_string())?
                            .ingest_finalized(&finalized, &volume_rows)
                            .map_err(|error| error.to_string())?;
                        volume
                            .record_finalized(&finalized)
                            .map_err(|error| error.to_string())?;
                        delta
                            .record_finalized(&finalized)
                            .map_err(|error| error.to_string())?;
                        append_bubbles(&mut self.finalized_volume, volume_rows, "final");
                        append_bubbles(&mut self.finalized_delta, delta_rows, "final");
                        Ok::<_, String>(())
                    })();
                    if let Err(error) = result {
                        self.last_error = Some(error);
                        return;
                    }
                }
                self.revision = self.revision.saturating_add(1);
                self.last_error = None;
            }
            Err(error) => self.last_error = Some(error.to_string()),
        }
    }

    fn snapshot(&self, mode: BubbleMode) -> Result<Vec<ViewBubble>, String> {
        let flow = self
            .flow
            .as_ref()
            .ok_or_else(|| "flow unavailable".to_string())?;
        let detector = match mode {
            BubbleMode::Volume => self.volume.as_ref(),
            BubbleMode::Delta => self.delta.as_ref(),
        }
        .ok_or_else(|| "detector unavailable".to_string())?;
        let mut rows: Vec<ViewBubble> = match mode {
            BubbleMode::Volume => self.finalized_volume.iter().cloned().collect(),
            BubbleMode::Delta => self.finalized_delta.iter().cloned().collect(),
        };
        if let Some(live) = flow.live_snapshot().map_err(|error| error.to_string())? {
            rows.extend(
                detector
                    .detect(&live, DetectionPhase::Live)
                    .map_err(|error| error.to_string())?
                    .into_iter()
                    .map(|bubble| view_bubble(bubble, "live")),
            );
        }
        Ok(rows)
    }

    fn level_snapshot(&self) -> Result<Vec<ViewStructuralLevel>, String> {
        let levels = self
            .levels
            .as_ref()
            .ok_or_else(|| "structural level engine unavailable".to_string())?;
        Ok(levels
            .snapshot()
            .into_iter()
            .map(view_structural_level)
            .collect())
    }
}

impl ProfileProjection {
    fn from_catalog(
        price_scale: u8,
        quantity_scale: u8,
        tick_size: Fixed,
        authority: CatalogAuthority,
    ) -> Self {
        if authority == CatalogAuthority::Placeholder {
            return Self {
                builder: None,
                revision: 0,
                unavailable_reason: Some("catalog_not_authoritative".into()),
                last_error: None,
            };
        }
        let builder = GridSpec::new(price_scale, quantity_scale, tick_size, 1).and_then(|grid| {
            TimeframeSpec::new(
                60_000_000_000,
                1_800_000_000_000,
                86_400_000_000_000,
                0,
                604_800_000_000_000,
            )
            .and_then(|time| {
                ProfileConfig::new(ValueAreaBasis::Volume, 7_000, 100_000, 50_000)
                    .and_then(|config| SessionProfileBuilder::new(grid, time, config))
            })
        });
        match builder {
            Ok(builder) => Self {
                builder: Some(builder),
                revision: 0,
                unavailable_reason: None,
                last_error: None,
            },
            Err(error) => Self {
                builder: None,
                revision: 0,
                unavailable_reason: Some(format!("invalid_catalog_grid: {error}")),
                last_error: None,
            },
        }
    }

    fn ingest(&mut self, timestamp_ns: i64, price: Price, quantity: Quantity) {
        let Some(builder) = self.builder.as_mut() else {
            return;
        };
        match builder.ingest(timestamp_ns, price, quantity) {
            Ok(_) => {
                self.revision = self.revision.saturating_add(1);
                self.last_error = None;
            }
            Err(error) => {
                self.last_error = Some(error.to_string());
            }
        }
    }

    fn snapshot(&self, basis: ValueAreaBasis) -> Result<Option<SessionProfile>, String> {
        let Some(builder) = self.builder.as_ref() else {
            return Ok(None);
        };
        builder
            .live_snapshot_with_basis(basis)
            .map_err(|error| error.to_string())
    }
}

impl LiveBook {
    fn from_snapshot(snap: &BookSnapshot) -> Option<Self> {
        let price_scale = snap
            .bids
            .first()
            .or(snap.asks.first())
            .map(|l| l.price.0.scale)
            .unwrap_or(8);
        let qty_scale = snap
            .bids
            .first()
            .or(snap.asks.first())
            .map(|l| l.quantity.0.scale)
            .unwrap_or(8);
        let mut book = OrderBook::new(price_scale, qty_scale, snap.depth);
        let bids: Vec<_> = snap.bids.iter().map(|l| (l.price, l.quantity)).collect();
        let asks: Vec<_> = snap.asks.iter().map(|l| (l.price, l.quantity)).collect();
        book.apply_snapshot(&bids, &asks, None).ok()?;
        Some(Self { book })
    }

    fn apply_delta(&mut self, delta: &BookDelta) -> bool {
        self.book.apply_changes_atomic(&delta.changes).is_ok()
    }

    fn snapshot(&self, depth: Option<u32>) -> Option<BookSnapshot> {
        let (bids, asks) = match depth {
            Some(depth) => self.book.snapshot_levels_bounded(depth as usize)?,
            None => self.book.snapshot_levels()?,
        };
        Some(BookSnapshot {
            bids,
            asks,
            depth,
            checksum: None,
        })
    }
}

#[derive(Debug, Default)]
struct ViewInner {
    /// config venue id → numeric VenueId
    venue_ids: HashMap<String, VenueId>,
    /// numeric VenueId → config venue id
    id_to_venue: HashMap<VenueId, String>,
    /// (config venue id, instrument) → native symbol
    symbols: HashMap<(String, u32), String>,
    books: HashMap<(VenueId, InstrumentId), LiveBook>,
    tapes: HashMap<(VenueId, InstrumentId), InstrumentTape>,
    profiles: HashMap<(VenueId, InstrumentId), Arc<Mutex<ProfileProjection>>>,
    bubbles: HashMap<(VenueId, InstrumentId), Arc<Mutex<BubbleProjection>>>,
    derivatives: HashMap<(VenueId, InstrumentId), DerivativeProjection>,
    depth_history: HashMap<(VenueId, InstrumentId), VecDeque<StoredDepthSample>>,
    depth_last_sample_ns: HashMap<(VenueId, InstrumentId), i64>,
    depth_epochs: HashMap<(VenueId, InstrumentId), u64>,
    depth_coalesced_samples: HashMap<(VenueId, InstrumentId), u64>,
    depth_evicted_samples: HashMap<(VenueId, InstrumentId), u64>,
    /// Last receive timestamp (ns) per venue from view ingest.
    venue_last_event_ts: HashMap<VenueId, i64>,
    venue_last_trade_ts: HashMap<VenueId, i64>,
    venue_last_quote_ts: HashMap<VenueId, i64>,
}

/// Shared view plane: EventSink fan-in + HTTP query surface.
#[derive(Debug)]
pub struct ViewPlane {
    cfg: ViewPlaneConfig,
    inner: Mutex<ViewInner>,
    batches_seen: AtomicU64,
    tape_dropped: AtomicU64,
    /// Venue context for the direct `EventSink` implementation.
    sink_venue: Option<VenueId>,
}

impl ViewPlane {
    pub fn new(cfg: ViewPlaneConfig) -> Self {
        Self {
            cfg,
            inner: Mutex::new(ViewInner::default()),
            batches_seen: AtomicU64::new(0),
            tape_dropped: AtomicU64::new(0),
            sink_venue: None,
        }
    }

    pub fn from_daemon_config(config: &DaemonConfig) -> Self {
        let cfg = ViewPlaneConfig {
            tape_capacity: config.telemetry.ui_tape_capacity.max(1) as usize,
            tape_max_per_sec: config.telemetry.ui_tape_max_per_sec,
        };
        let plane = Self::new(cfg);
        // Pre-register symbol maps (instrument ids are 1-based catalog indices).
        {
            let mut inner = plane.inner.lock().expect("view lock");
            for v in &config.venues {
                for (i, sym) in v.symbols.iter().enumerate() {
                    inner
                        .symbols
                        .insert((v.id.clone(), (i as u32) + 1), sym.clone());
                }
            }
        }
        plane
    }

    /// Register the process-local VenueId used by a running session.
    pub fn register_venue(&self, venue_id: VenueId, config_id: &str, symbols: &[String]) {
        let mut inner = self.inner.lock().expect("view lock");
        inner.venue_ids.insert(config_id.to_string(), venue_id);
        inner.id_to_venue.insert(venue_id, config_id.to_string());
        for (i, sym) in symbols.iter().enumerate() {
            inner
                .symbols
                .insert((config_id.to_string(), (i as u32) + 1), sym.clone());
        }
        // Synthetic uses InstrumentId(1) / BTC-USD without config symbols.
        if symbols.is_empty() && config_id.contains("synthetic") {
            inner
                .symbols
                .insert((config_id.to_string(), 1), "BTC-USD".into());
        }
    }

    /// Register the exact catalog before session events reach the projection.
    pub fn register_catalog(
        &self,
        config_id: &str,
        catalog: &CatalogView,
        authority: CatalogAuthority,
    ) {
        let mut inner = self.inner.lock().expect("view lock");
        inner.venue_ids.insert(config_id.to_string(), catalog.venue);
        inner
            .id_to_venue
            .insert(catalog.venue, config_id.to_string());
        for instrument in catalog.instruments.iter() {
            inner.symbols.insert(
                (config_id.to_string(), instrument.id.0),
                instrument.key.native_symbol.clone(),
            );
            inner.profiles.insert(
                (catalog.venue, instrument.id),
                Arc::new(Mutex::new(ProfileProjection::from_catalog(
                    instrument.price_scale,
                    instrument.quantity_scale,
                    instrument.price_increment,
                    authority,
                ))),
            );
            inner.bubbles.insert(
                (catalog.venue, instrument.id),
                Arc::new(Mutex::new(BubbleProjection::from_catalog(
                    instrument.id,
                    MarketSegment::from(instrument.key.kind),
                    instrument.price_scale,
                    instrument.quantity_scale,
                    instrument.price_increment,
                    instrument.quantity_increment,
                    authority,
                ))),
            );
        }
    }

    pub fn status(&self, state: &DaemonState) -> ViewStatus {
        let started = state.started_unix_secs;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(started);
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        let lifecycle = match *state.supervisor_lifecycle.lock().expect("lifecycle") {
            marketfeed_engine::EngineLifecycle::Starting => "starting",
            marketfeed_engine::EngineLifecycle::Running => "running",
            marketfeed_engine::EngineLifecycle::Draining => "draining",
            marketfeed_engine::EngineLifecycle::Stopped => "stopped",
        };
        let inner = self.inner.lock().expect("view lock");
        let mut venues = Vec::with_capacity(state.config.venues.len());
        for v in &state.config.venues {
            let live = state
                .venue_flags
                .get(&v.id)
                .map(|f| f.load(Ordering::Relaxed))
                .unwrap_or(false);
            let m = state.venue_metrics.get(&v.id);
            let venue_id = inner.venue_ids.get(&v.id).copied();
            let (tape_trades, tape_trades_dropped, tape_quotes, tape_quotes_dropped) = venue_id
                .map(|vid| inner.venue_tape_stats(vid))
                .unwrap_or((0, 0, 0, 0));
            let last_event_ts_ns =
                venue_id.and_then(|vid| inner.venue_last_event_ts.get(&vid).copied());
            let last_trade_ts_ns =
                venue_id.and_then(|vid| inner.venue_last_trade_ts.get(&vid).copied());
            let last_quote_ts_ns =
                venue_id.and_then(|vid| inner.venue_last_quote_ts.get(&vid).copied());
            let feed_lag_ms = last_event_ts_ns.and_then(|ts| {
                if ts > 0 {
                    Some((now_ns.saturating_sub(ts).max(0) / 1_000_000) as u64)
                } else {
                    None
                }
            });
            venues.push(ViewVenueStatus {
                id: v.id.clone(),
                adapter: v.adapter.clone(),
                required: v.required,
                live,
                symbols: v.symbols.clone(),
                events_dispatched: m
                    .map(|x| x.events_dispatched.load(Ordering::Relaxed))
                    .unwrap_or(0),
                events_dropped: m
                    .map(|x| x.events_dropped.load(Ordering::Relaxed))
                    .unwrap_or(0),
                reconnects: m.map(|x| x.reconnects.load(Ordering::Relaxed)).unwrap_or(0),
                book_invalidations: m
                    .map(|x| x.book_invalidations.load(Ordering::Relaxed))
                    .unwrap_or(0),
                valid_books: m
                    .map(|x| x.valid_books.load(Ordering::Relaxed))
                    .unwrap_or(0),
                queue_occupancy: m
                    .map(|x| x.batch_queue_occupancy.load(Ordering::Relaxed))
                    .unwrap_or(0),
                feed_lag_ms,
                last_event_ts_ns,
                last_trade_ts_ns,
                last_quote_ts_ns,
                tape_trades,
                tape_quotes,
                tape_trades_dropped,
                tape_quotes_dropped,
            });
        }
        ViewStatus {
            live: state.is_live(),
            ready: state.is_ready(),
            uptime_secs: now.saturating_sub(started),
            lifecycle: lifecycle.into(),
            disk_pressure: state.disk_pressure.load(Ordering::Relaxed),
            shutdown_draining: state.shutdown_draining.load(Ordering::Relaxed),
            recording: ViewRecordingStatus {
                enabled: state.config.recording.raw.enabled,
                healthy: state.recording_healthy.load(Ordering::Relaxed),
                queue_len: state.recording_queue_len.load(Ordering::Relaxed),
                frames_dropped: state.recording_dropped.load(Ordering::Relaxed),
            },
            grafana_base_url: state.config.telemetry.grafana_base_url.clone(),
            alert_webhook_configured: state
                .config
                .telemetry
                .alert_webhook_url
                .as_ref()
                .is_some_and(|u| !u.trim().is_empty()),
            venues,
        }
    }

    pub fn instruments_json(&self, state: &DaemonState) -> serde_json::Value {
        let inner = self.inner.lock().expect("view lock");
        let venues: Vec<_> = state
            .config
            .venues
            .iter()
            .map(|v| {
                let symbols: Vec<_> = if v.symbols.is_empty() && v.adapter == "synthetic" {
                    vec![serde_json::json!({
                        "instrument": 1,
                        "symbol": "BTC-USD",
                    })]
                } else {
                    v.symbols
                        .iter()
                        .enumerate()
                        .map(|(i, s)| {
                            serde_json::json!({
                                "instrument": (i as u32) + 1,
                                "symbol": s,
                            })
                        })
                        .collect()
                };
                let venue_id = inner.venue_ids.get(&v.id).map(|id| id.0);
                serde_json::json!({
                    "id": v.id,
                    "adapter": v.adapter,
                    "venue_id": venue_id,
                    "symbols": symbols,
                })
            })
            .collect();
        serde_json::json!({ "venues": venues })
    }

    /// Mirror of [`marketfeed_engine::EngineControl::book_snapshot`] against the view cache.
    pub fn book_snapshot(
        &self,
        venue_config_id: &str,
        instrument: InstrumentId,
        depth: Option<u32>,
    ) -> Option<ViewBookSnapshot> {
        let inner = self.inner.lock().expect("view lock");
        let venue_id = *inner.venue_ids.get(venue_config_id)?;
        let live = inner.books.get(&(venue_id, instrument))?;
        let snap = live.snapshot(depth)?;
        let symbol = inner
            .symbols
            .get(&(venue_config_id.to_string(), instrument.0))
            .cloned();
        Some(ViewBookSnapshot {
            venue: venue_config_id.to_string(),
            instrument: instrument.0,
            symbol,
            depth: snap.depth.or(depth),
            bids: snap.bids.iter().map(level_json).collect(),
            asks: snap.asks.iter().map(level_json).collect(),
        })
    }

    pub fn resolve_instrument(&self, venue_config_id: &str, symbol: &str) -> Option<InstrumentId> {
        let inner = self.inner.lock().expect("view lock");
        inner
            .symbols
            .iter()
            .find(|((v, _), s)| v == venue_config_id && s.as_str() == symbol)
            .map(|((_, inst), _)| InstrumentId(*inst))
    }

    pub fn tape(
        &self,
        venue_config_id: &str,
        instrument: InstrumentId,
        limit: usize,
    ) -> Vec<TapeEntry> {
        self.tape_filtered(venue_config_id, instrument, limit, None)
    }

    pub fn profile_snapshot(
        &self,
        venue_config_id: &str,
        instrument: InstrumentId,
        basis: ValueAreaBasis,
    ) -> ViewProfileSnapshot {
        let inner = self.inner.lock().expect("view lock");
        let symbol = inner
            .symbols
            .get(&(venue_config_id.to_string(), instrument.0))
            .cloned();
        let Some(venue_id) = inner.venue_ids.get(venue_config_id).copied() else {
            return unavailable_profile(
                venue_config_id,
                instrument,
                symbol,
                0,
                "venue_not_registered",
            );
        };
        let Some(projection) = inner.profiles.get(&(venue_id, instrument)).cloned() else {
            return unavailable_profile(
                venue_config_id,
                instrument,
                symbol,
                0,
                "catalog_metadata_unavailable",
            );
        };
        drop(inner);
        let projection = projection.lock().expect("profile projection lock");
        if let Some(reason) = projection.unavailable_reason.as_deref() {
            return unavailable_profile(
                venue_config_id,
                instrument,
                symbol,
                projection.revision,
                reason,
            );
        }
        match projection.snapshot(basis) {
            Ok(Some(profile)) => view_profile(
                venue_config_id,
                instrument,
                symbol,
                projection.revision,
                &profile,
                projection.last_error.as_deref(),
            ),
            Ok(None) => unavailable_profile(
                venue_config_id,
                instrument,
                symbol,
                projection.revision,
                "no_profile_trades",
            ),
            Err(error) => unavailable_profile(
                venue_config_id,
                instrument,
                symbol,
                projection.revision,
                &format!("profile_snapshot_failed: {error}"),
            ),
        }
    }

    pub fn bubble_snapshot(
        &self,
        venue_config_id: &str,
        instrument: InstrumentId,
        mode: BubbleMode,
    ) -> ViewBubbleSnapshot {
        let inner = self.inner.lock().expect("view lock");
        let symbol = inner
            .symbols
            .get(&(venue_config_id.to_string(), instrument.0))
            .cloned();
        let mode_label = bubble_mode_label(mode).to_string();
        let Some(venue_id) = inner.venue_ids.get(venue_config_id).copied() else {
            return unavailable_bubbles(
                venue_config_id,
                instrument,
                symbol,
                0,
                mode_label,
                "venue_not_registered",
            );
        };
        let Some(projection) = inner.bubbles.get(&(venue_id, instrument)).cloned() else {
            return unavailable_bubbles(
                venue_config_id,
                instrument,
                symbol,
                0,
                mode_label,
                "catalog_metadata_unavailable",
            );
        };
        drop(inner);
        let projection = projection.lock().expect("bubble projection lock");
        if let Some(reason) = projection.unavailable_reason.as_deref() {
            return unavailable_bubbles(
                venue_config_id,
                instrument,
                symbol,
                projection.revision,
                mode_label,
                reason,
            );
        }
        match projection.snapshot(mode) {
            Ok(rows) => ViewBubbleSnapshot {
                schema_version: 1,
                venue: venue_config_id.into(),
                instrument: instrument.0,
                symbol,
                revision: projection.revision,
                status: if projection.last_error.is_some() {
                    "degraded".into()
                } else {
                    "live".into()
                },
                mode: mode_label,
                reason: projection.last_error.clone(),
                bubbles: rows,
            },
            Err(error) => unavailable_bubbles(
                venue_config_id,
                instrument,
                symbol,
                projection.revision,
                mode_label,
                &error,
            ),
        }
    }

    pub fn structural_level_snapshot(
        &self,
        venue_config_id: &str,
        instrument: InstrumentId,
    ) -> ViewStructuralLevelSnapshot {
        let inner = self.inner.lock().expect("view lock");
        let symbol = inner
            .symbols
            .get(&(venue_config_id.to_string(), instrument.0))
            .cloned();
        let Some(venue_id) = inner.venue_ids.get(venue_config_id).copied() else {
            return unavailable_structural_levels(
                venue_config_id,
                instrument,
                symbol,
                0,
                "venue_not_registered",
            );
        };
        let Some(projection) = inner.bubbles.get(&(venue_id, instrument)).cloned() else {
            return unavailable_structural_levels(
                venue_config_id,
                instrument,
                symbol,
                0,
                "catalog_metadata_unavailable",
            );
        };
        drop(inner);
        let projection = projection.lock().expect("bubble projection lock");
        if let Some(reason) = projection.unavailable_reason.as_deref() {
            return unavailable_structural_levels(
                venue_config_id,
                instrument,
                symbol,
                projection.revision,
                reason,
            );
        }
        match projection.level_snapshot() {
            Ok(levels) => ViewStructuralLevelSnapshot {
                schema_version: 1,
                venue: venue_config_id.into(),
                instrument: instrument.0,
                symbol,
                revision: projection.revision,
                status: if projection.last_error.is_some() {
                    "degraded".into()
                } else {
                    "live".into()
                },
                reason: projection.last_error.clone(),
                levels,
            },
            Err(error) => unavailable_structural_levels(
                venue_config_id,
                instrument,
                symbol,
                projection.revision,
                &error,
            ),
        }
    }

    pub fn derivatives_snapshot(
        &self,
        venue_config_id: &str,
        instrument: InstrumentId,
    ) -> ViewDerivativesSnapshot {
        const STALE_NS: i64 = 120_000_000_000;
        let inner = self.inner.lock().expect("view lock");
        let symbol = inner
            .symbols
            .get(&(venue_config_id.to_string(), instrument.0))
            .cloned();
        let Some(venue_id) = inner.venue_ids.get(venue_config_id).copied() else {
            return unavailable_derivatives(venue_config_id, instrument, symbol);
        };
        let Some(projection) = inner.derivatives.get(&(venue_id, instrument)) else {
            return unavailable_derivatives(venue_config_id, instrument, symbol);
        };
        let now_ns = unix_now_ns();
        let funding = projection.funding.as_ref().map(|value| {
            let age_ns = now_ns.saturating_sub(value.event_ts_ns).max(0);
            ViewFunding {
                rate: format_fixed(value.rate),
                event_ts_ns: value.event_ts_ns,
                next_funding_ts_ns: value.next_funding_ts_ns,
                age_ms: (age_ns / 1_000_000) as u64,
                stale: age_ns > STALE_NS,
            }
        });
        let open_interest = projection.open_interest.as_ref().map(|value| {
            let age_ns = now_ns.saturating_sub(value.event_ts_ns).max(0);
            let (change, interval) = value
                .previous
                .as_ref()
                .and_then(|(previous, ts)| {
                    fixed_difference(value.quantity.0, previous.0)
                        .map(|change| (format_fixed(change), value.event_ts_ns.saturating_sub(*ts)))
                })
                .map_or((None, None), |(change, interval)| {
                    (Some(change), Some(interval))
                });
            ViewOpenInterest {
                quantity: format_fixed(value.quantity.0),
                event_ts_ns: value.event_ts_ns,
                age_ms: (age_ns / 1_000_000) as u64,
                stale: age_ns > STALE_NS,
                change,
                change_interval_ns: interval,
            }
        });
        let funding_divergence = symbol
            .as_deref()
            .and_then(|focus_symbol| funding_divergence(&inner, focus_symbol, now_ns, STALE_NS));
        ViewDerivativesSnapshot {
            schema_version: 1,
            venue: venue_config_id.into(),
            instrument: instrument.0,
            symbol,
            revision: projection.revision,
            status: if funding.is_some()
                || open_interest.is_some()
                || !projection.liquidations.is_empty()
            {
                "live".into()
            } else {
                "unavailable".into()
            },
            funding,
            open_interest,
            funding_divergence,
            liquidations: projection
                .liquidations
                .iter()
                .rev()
                .take(100)
                .map(|value| ViewLiquidation {
                    price: format_fixed(value.price.0),
                    quantity: format_fixed(value.quantity.0),
                    side: aggressor_str(value.side).into(),
                    event_ts_ns: value.event_ts_ns,
                })
                .collect(),
        }
    }

    pub fn depth_history(
        &self,
        venue_config_id: &str,
        instrument: InstrumentId,
        limit: usize,
    ) -> ViewDepthHistory {
        let inner = self.inner.lock().expect("view lock");
        let symbol = inner
            .symbols
            .get(&(venue_config_id.to_string(), instrument.0))
            .cloned();
        let venue = inner.venue_ids.get(venue_config_id).copied();
        let samples = venue
            .and_then(|venue| inner.depth_history.get(&(venue, instrument)))
            .map(|history| {
                history
                    .iter()
                    .skip(
                        history
                            .len()
                            .saturating_sub(limit.clamp(1, DEPTH_HISTORY_CAPACITY)),
                    )
                    .map(view_depth_sample)
                    .collect()
            })
            .unwrap_or_default();
        ViewDepthHistory {
            schema_version: 1,
            venue: venue_config_id.into(),
            instrument: instrument.0,
            symbol,
            sample_interval_ms: DEPTH_SAMPLE_INTERVAL_MS,
            capacity: DEPTH_HISTORY_CAPACITY,
            coalesced_samples: venue
                .and_then(|venue| {
                    inner
                        .depth_coalesced_samples
                        .get(&(venue, instrument))
                        .copied()
                })
                .unwrap_or(0),
            evicted_samples: venue
                .and_then(|venue| {
                    inner
                        .depth_evicted_samples
                        .get(&(venue, instrument))
                        .copied()
                })
                .unwrap_or(0),
            samples,
        }
    }

    pub fn dom_snapshot(
        &self,
        venue_config_id: &str,
        instrument: InstrumentId,
        depth: usize,
        execution_window_sec: u64,
    ) -> ViewDomSnapshot {
        let inner = self.inner.lock().expect("view lock");
        let symbol = inner
            .symbols
            .get(&(venue_config_id.to_string(), instrument.0))
            .cloned();
        let unavailable = |reason: &str| {
            unavailable_dom(
                venue_config_id,
                instrument,
                symbol.clone(),
                execution_window_sec,
                reason,
            )
        };
        let Some(venue) = inner.venue_ids.get(venue_config_id).copied() else {
            return unavailable("venue_not_registered");
        };
        let Some(book) = inner
            .books
            .get(&(venue, instrument))
            .and_then(|book| book.snapshot(Some(depth.clamp(1, 100) as u32)))
        else {
            return unavailable("book_unavailable");
        };

        let mut prices: Vec<Fixed> = book
            .bids
            .iter()
            .chain(book.asks.iter())
            .map(|level| level.price.0)
            .collect();
        prices.sort_by(|left, right| compare_fixed(*right, *left));
        prices.dedup_by(|left, right| compare_fixed(*left, *right).is_eq());

        let history = inner.depth_history.get(&(venue, instrument));
        let latest = history.and_then(|samples| samples.back());
        let previous = history.and_then(|samples| samples.iter().rev().nth(1));
        let revision = latest
            .and_then(|sample| u64::try_from(sample.event_ts_ns).ok())
            .unwrap_or(0);
        let epoch = latest.map(|sample| sample.epoch).unwrap_or(0);
        let window_sec = execution_window_sec.clamp(1, 3_600);
        let cutoff_ns = unix_now_ns().saturating_sub(
            i64::try_from(window_sec)
                .unwrap_or(i64::MAX)
                .saturating_mul(1_000_000_000),
        );

        let qty_scale = book
            .bids
            .first()
            .or(book.asks.first())
            .map(|level| level.quantity.0.scale)
            .unwrap_or(0);
        let price_scale = book
            .bids
            .first()
            .or(book.asks.first())
            .map(|level| level.price.0.scale)
            .unwrap_or(0);
        let notional_scale = price_scale.saturating_add(qty_scale);

        let mut rows = Vec::with_capacity(prices.len());
        for price in prices {
            let bid_quantity =
                book_quantity_at(&book.bids, price).unwrap_or(Fixed::new(0, qty_scale));
            let ask_quantity =
                book_quantity_at(&book.asks, price).unwrap_or(Fixed::new(0, qty_scale));
            let latest_bid = depth_quantity_at(latest, BookSide::Bid, price, qty_scale);
            let latest_ask = depth_quantity_at(latest, BookSide::Ask, price, qty_scale);
            let previous_bid = depth_quantity_at(previous, BookSide::Bid, price, qty_scale);
            let previous_ask = depth_quantity_at(previous, BookSide::Ask, price, qty_scale);
            let Some(bid_delta) = fixed_sub(latest_bid, previous_bid) else {
                return unavailable("dom_arithmetic_overflow");
            };
            let Some(ask_delta) = fixed_sub(latest_ask, previous_ask) else {
                return unavailable("dom_arithmetic_overflow");
            };
            let Some(mbp_delta) = fixed_sub(bid_delta, ask_delta) else {
                return unavailable("dom_arithmetic_overflow");
            };

            let mut buy = Fixed::new(0, notional_scale);
            let mut sell = Fixed::new(0, notional_scale);
            let mut unknown = Fixed::new(0, notional_scale);
            if let Some(tape) = inner.tapes.get(&(venue, instrument)) {
                for entry in &tape.trades.entries {
                    let TapeEntry::Trade {
                        price: trade_price,
                        notional: Some(notional),
                        aggressor,
                        exchange_ts_ns,
                        receive_ts_ns,
                        ..
                    } = entry
                    else {
                        continue;
                    };
                    if exchange_ts_ns.unwrap_or(*receive_ts_ns) < cutoff_ns {
                        continue;
                    }
                    let (Ok(trade_price), Ok(notional)) =
                        (Fixed::parse_str(trade_price), Fixed::parse_str(notional))
                    else {
                        return unavailable("invalid_internal_tape_value");
                    };
                    if !compare_fixed(trade_price, price).is_eq() {
                        continue;
                    }
                    match aggressor.as_str() {
                        "buy" => {
                            let Some(value) = fixed_add(buy, notional) else {
                                return unavailable("dom_arithmetic_overflow");
                            };
                            buy = value;
                        }
                        "sell" => {
                            let Some(value) = fixed_add(sell, notional) else {
                                return unavailable("dom_arithmetic_overflow");
                            };
                            sell = value;
                        }
                        _ => {
                            let Some(value) = fixed_add(unknown, notional) else {
                                return unavailable("dom_arithmetic_overflow");
                            };
                            unknown = value;
                        }
                    }
                }
            }
            let Some(total) = fixed_add(buy, sell).and_then(|value| fixed_add(value, unknown))
            else {
                return unavailable("dom_arithmetic_overflow");
            };
            let Some(executed_delta) = fixed_sub(buy, sell) else {
                return unavailable("dom_arithmetic_overflow");
            };
            let Some(bid_cumulative) =
                cumulative_notional(&book.bids, price, BookSide::Bid, notional_scale)
            else {
                return unavailable("dom_arithmetic_overflow");
            };
            let Some(ask_cumulative) =
                cumulative_notional(&book.asks, price, BookSide::Ask, notional_scale)
            else {
                return unavailable("dom_arithmetic_overflow");
            };
            let Some(imbalance_bps) = quantity_imbalance_bps(bid_quantity, ask_quantity) else {
                return unavailable("dom_arithmetic_overflow");
            };
            let Some(mbp_delta_notional) = notional_fixed(price, mbp_delta) else {
                return unavailable("dom_arithmetic_overflow");
            };

            rows.push(ViewDomRow {
                price: format_fixed(price),
                bid_quantity: format_fixed(bid_quantity),
                ask_quantity: format_fixed(ask_quantity),
                bid_cumulative_notional: format_fixed(bid_cumulative),
                ask_cumulative_notional: format_fixed(ask_cumulative),
                imbalance_bps,
                mbp_delta_quantity: format_fixed(mbp_delta),
                mbp_delta_notional: format_fixed(mbp_delta_notional),
                buy_executed_notional: format_fixed(buy),
                sell_executed_notional: format_fixed(sell),
                unknown_executed_notional: format_fixed(unknown),
                total_executed_notional: format_fixed(total),
                executed_delta_notional: format_fixed(executed_delta),
            });
        }

        ViewDomSnapshot {
            schema_version: 1,
            venue: venue_config_id.into(),
            instrument: instrument.0,
            symbol,
            revision,
            status: "live".into(),
            reason: None,
            execution_window_sec: window_sec,
            epoch,
            rows,
        }
    }

    /// Optional `kind` filter: `"trade"` / `"quote"` / `None` (merged newest-first).
    pub fn tape_filtered(
        &self,
        venue_config_id: &str,
        instrument: InstrumentId,
        limit: usize,
        kind: Option<&str>,
    ) -> Vec<TapeEntry> {
        let inner = self.inner.lock().expect("view lock");
        let Some(venue_id) = inner.venue_ids.get(venue_config_id).copied() else {
            return Vec::new();
        };
        inner
            .tapes
            .get(&(venue_id, instrument))
            .map(|t| t.snapshot(limit.max(1), kind))
            .unwrap_or_default()
    }

    pub fn stats(&self) -> (u64, u64) {
        (
            self.batches_seen.load(Ordering::Relaxed),
            self.tape_dropped.load(Ordering::Relaxed),
        )
    }

    /// Focus book + tape summary for SSE `/v1/stream`.
    pub fn stream_focus(
        &self,
        venue_config_id: &str,
        instrument: InstrumentId,
        book_depth: Option<u32>,
        tape_limit: usize,
    ) -> serde_json::Value {
        let book = self
            .book_snapshot(venue_config_id, instrument, book_depth)
            .map(|b| serde_json::to_value(b).unwrap_or(serde_json::Value::Null))
            .unwrap_or(serde_json::Value::Null);
        let tape = self.tape_filtered(venue_config_id, instrument, tape_limit, None);
        let profile = self.profile_snapshot(venue_config_id, instrument, ValueAreaBasis::Volume);
        let bubbles_volume = self.bubble_snapshot(venue_config_id, instrument, BubbleMode::Volume);
        let bubbles_delta = self.bubble_snapshot(venue_config_id, instrument, BubbleMode::Delta);
        let structural_levels = self.structural_level_snapshot(venue_config_id, instrument);
        let derivatives = self.derivatives_snapshot(venue_config_id, instrument);
        serde_json::json!({
            "venue": venue_config_id,
            "instrument": instrument.0,
            "book": book,
            "tape": tape,
            "profile": profile,
            "bubbles_volume": bubbles_volume,
            "bubbles_delta": bubbles_delta,
            "structural_levels": structural_levels,
            "derivatives": derivatives,
        })
    }

    fn ingest_batch(&self, batch: &EventBatch) -> Option<VenueId> {
        self.batches_seen.fetch_add(1, Ordering::Relaxed);
        let sink_venue = batch
            .events
            .first()
            .map(|event| event.venue)
            .filter(|venue| batch.events.iter().all(|event| event.venue == *venue));
        let mut deferred_analytics = Vec::new();
        let mut inner = self.inner.lock().expect("view lock");
        for ev in &batch.events {
            let venue_id = ev.venue;
            let Some(instrument) = ev.instrument else {
                continue;
            };
            let venue_name = inner
                .id_to_venue
                .get(&venue_id)
                .cloned()
                .unwrap_or_else(|| format!("venue-{}", venue_id.0));
            // Ensure reverse map exists even if register_venue raced.
            inner
                .id_to_venue
                .entry(venue_id)
                .or_insert_with(|| venue_name.clone());
            inner
                .venue_ids
                .entry(venue_name.clone())
                .or_insert(venue_id);

            let receive_ts = ev.receive_ts.0;
            inner.venue_last_event_ts.insert(venue_id, receive_ts);

            match &ev.payload {
                MarketEvent::BookSnapshot(snap) => {
                    if let Some(live) = LiveBook::from_snapshot(snap) {
                        if let Some(snapshot) = live.snapshot(Some(64)) {
                            push_depth_sample(
                                &mut inner, venue_id, instrument, receive_ts, snapshot,
                            );
                        }
                        inner.books.insert((venue_id, instrument), live);
                    }
                }
                MarketEvent::BookDelta(delta) => {
                    let snapshot = inner
                        .books
                        .get_mut(&(venue_id, instrument))
                        .and_then(|live| live.apply_delta(delta).then(|| live.snapshot(Some(64))))
                        .flatten();
                    if let Some(snapshot) = snapshot {
                        push_depth_sample(&mut inner, venue_id, instrument, receive_ts, snapshot);
                    }
                }
                MarketEvent::Trade(t) => {
                    inner.venue_last_trade_ts.insert(venue_id, receive_ts);
                    let event_ts = ev.exchange_ts.unwrap_or(ev.receive_ts).0;
                    deferred_analytics.push(DeferredTradeAnalytics {
                        profile: inner.profiles.get(&(venue_id, instrument)).cloned(),
                        bubbles: inner.bubbles.get(&(venue_id, instrument)).cloned(),
                        venue: venue_id,
                        instrument,
                        timestamp_ns: event_ts,
                        price: t.price,
                        quantity: t.quantity,
                        aggressor: t.aggressor,
                    });
                    let symbol = inner
                        .symbols
                        .get(&(venue_name.clone(), instrument.0))
                        .cloned();
                    let price_str = format_fixed(t.price.0);
                    let qty_str = format_fixed(t.quantity.0);
                    let notional = notional_fixed(t.price.0, t.quantity.0).map(format_fixed);
                    let entry = TapeEntry::Trade {
                        venue: venue_name.clone(),
                        instrument: instrument.0,
                        symbol,
                        price: price_str,
                        quantity: qty_str,
                        notional,
                        aggressor: aggressor_str(t.aggressor).into(),
                        trade_id: t.trade_id.as_ref().map(|s| s.0.clone()),
                        exchange_ts_ns: ev.exchange_ts.map(|t| t.0),
                        receive_ts_ns: receive_ts,
                    };
                    let ring = inner
                        .tapes
                        .entry((venue_id, instrument))
                        .or_insert_with(|| {
                            InstrumentTape::new(self.cfg.tape_capacity, self.cfg.tape_max_per_sec)
                        });
                    let dropped = ring.push_trade(entry);
                    if dropped > 0 {
                        self.tape_dropped.fetch_add(dropped, Ordering::Relaxed);
                    }
                }
                MarketEvent::Quote(q) => {
                    inner.venue_last_quote_ts.insert(venue_id, receive_ts);
                    let symbol = inner
                        .symbols
                        .get(&(venue_name.clone(), instrument.0))
                        .cloned();
                    let entry = TapeEntry::Quote {
                        venue: venue_name.clone(),
                        instrument: instrument.0,
                        symbol,
                        bid_price: format_fixed(q.bid_price.0),
                        bid_quantity: q.bid_quantity.map(|x| format_fixed(x.0)),
                        ask_price: format_fixed(q.ask_price.0),
                        ask_quantity: q.ask_quantity.map(|x| format_fixed(x.0)),
                        exchange_ts_ns: ev.exchange_ts.map(|t| t.0),
                        receive_ts_ns: receive_ts,
                    };
                    let ring = inner
                        .tapes
                        .entry((venue_id, instrument))
                        .or_insert_with(|| {
                            InstrumentTape::new(self.cfg.tape_capacity, self.cfg.tape_max_per_sec)
                        });
                    let dropped = ring.push_quote(entry);
                    if dropped > 0 {
                        self.tape_dropped.fetch_add(dropped, Ordering::Relaxed);
                    }
                }
                MarketEvent::Funding(value) => {
                    let event_ts = ev.exchange_ts.unwrap_or(ev.receive_ts).0;
                    let projection = inner.derivatives.entry((venue_id, instrument)).or_default();
                    projection.funding = Some(TimedFunding {
                        rate: value.rate.0,
                        event_ts_ns: event_ts,
                        next_funding_ts_ns: value.next_funding_ts.map(|ts| ts.0),
                    });
                    projection.revision = projection.revision.saturating_add(1);
                }
                MarketEvent::OpenInterest(value) => {
                    let event_ts = ev.exchange_ts.unwrap_or(ev.receive_ts).0;
                    let projection = inner.derivatives.entry((venue_id, instrument)).or_default();
                    let previous = projection
                        .open_interest
                        .as_ref()
                        .map(|old| (old.quantity, old.event_ts_ns));
                    projection.open_interest = Some(TimedOpenInterest {
                        quantity: value.quantity,
                        event_ts_ns: event_ts,
                        previous,
                    });
                    projection.revision = projection.revision.saturating_add(1);
                }
                MarketEvent::Liquidation(value) => {
                    let event_ts = ev.exchange_ts.unwrap_or(ev.receive_ts).0;
                    let projection = inner.derivatives.entry((venue_id, instrument)).or_default();
                    while projection.liquidations.len() >= 512 {
                        projection.liquidations.pop_front();
                    }
                    projection.liquidations.push_back(TimedLiquidation {
                        price: value.price,
                        quantity: value.quantity,
                        side: value.side,
                        event_ts_ns: event_ts,
                    });
                    projection.revision = projection.revision.saturating_add(1);
                }
                _ => {}
            }
        }
        drop(inner);
        for deferred in deferred_analytics {
            if let Some(profile) = deferred.profile {
                profile.lock().expect("profile projection lock").ingest(
                    deferred.timestamp_ns,
                    deferred.price,
                    deferred.quantity,
                );
            }
            if let Some(bubbles) = deferred.bubbles {
                bubbles.lock().expect("bubble projection lock").ingest(
                    deferred.venue,
                    deferred.instrument,
                    deferred.timestamp_ns,
                    deferred.price,
                    deferred.quantity,
                    deferred.aggressor,
                );
            }
        }
        sink_venue
    }

    fn ingest_system(&self, sink_venue: Option<VenueId>, event: &SystemEvent) {
        let (Some(venue), SystemEvent::BookInvalidated { instrument, .. }) = (sink_venue, event)
        else {
            return;
        };
        let mut inner = self.inner.lock().expect("view lock");
        inner.books.remove(&(venue, *instrument));
        let epoch = inner.depth_epochs.entry((venue, *instrument)).or_default();
        *epoch = epoch.saturating_add(1);
        inner.depth_last_sample_ns.remove(&(venue, *instrument));
    }
}

fn push_depth_sample(
    inner: &mut ViewInner,
    venue: VenueId,
    instrument: InstrumentId,
    event_ts_ns: i64,
    snapshot: BookSnapshot,
) {
    const SAMPLE_NS: i64 = (DEPTH_SAMPLE_INTERVAL_MS as i64) * 1_000_000;
    let key = (venue, instrument);
    if inner
        .depth_last_sample_ns
        .get(&key)
        .is_some_and(|last| event_ts_ns.saturating_sub(*last) < SAMPLE_NS)
    {
        let counter = inner.depth_coalesced_samples.entry(key).or_default();
        *counter = counter.saturating_add(1);
        return;
    }
    inner.depth_last_sample_ns.insert(key, event_ts_ns);
    let epoch = inner.depth_epochs.get(&key).copied().unwrap_or(0);
    let history = inner.depth_history.entry(key).or_default();
    let recycled = if history.len() >= DEPTH_HISTORY_CAPACITY {
        let counter = inner.depth_evicted_samples.entry(key).or_default();
        *counter = counter.saturating_add(1);
        history.pop_front()
    } else {
        None
    };
    let price_scale = snapshot
        .bids
        .first()
        .or(snapshot.asks.first())
        .map(|level| level.price.0.scale)
        .unwrap_or(0);
    let quantity_scale = snapshot
        .bids
        .first()
        .or(snapshot.asks.first())
        .map(|level| level.quantity.0.scale)
        .unwrap_or(0);
    let store_level = |level: BookLevel| StoredDepthLevel {
        price_coefficient: level.price.0.coefficient,
        quantity_coefficient: level.quantity.0.coefficient,
    };
    let (mut bids, mut asks) = recycled
        .map(|sample| (sample.bids, sample.asks))
        .unwrap_or_else(|| {
            (
                Vec::with_capacity(snapshot.bids.len()),
                Vec::with_capacity(snapshot.asks.len()),
            )
        });
    bids.clear();
    bids.extend(snapshot.bids.into_iter().map(store_level));
    asks.clear();
    asks.extend(snapshot.asks.into_iter().map(store_level));
    history.push_back(StoredDepthSample {
        event_ts_ns,
        epoch,
        price_scale,
        quantity_scale,
        bids,
        asks,
    });
}

fn unavailable_profile(
    venue: &str,
    instrument: InstrumentId,
    symbol: Option<String>,
    revision: u64,
    reason: &str,
) -> ViewProfileSnapshot {
    ViewProfileSnapshot {
        schema_version: 1,
        venue: venue.into(),
        instrument: instrument.0,
        symbol,
        revision,
        status: "unavailable".into(),
        reason: Some(reason.into()),
        basis: None,
        value_area_bps: None,
        start_ts_ns: None,
        end_ts_ns: None,
        high: None,
        low: None,
        range: None,
        total_volume: None,
        poc: None,
        vah: None,
        val: None,
        tpo_count: None,
        rotation_factor: None,
    }
}

fn fixed_multiple(
    value: Fixed,
    multiple: i128,
) -> Result<Quantity, marketfeed_analytics::AnalyticsError> {
    let coefficient = value.coefficient.checked_mul(multiple).ok_or(
        marketfeed_analytics::AnalyticsError::ArithmeticOverflow {
            operation: "scaling bubble threshold",
        },
    )?;
    Ok(Quantity(Fixed::new(coefficient, value.scale)))
}

fn default_bubble_config(
    quantity_increment: Fixed,
    segment: MarketSegment,
    mode: BubbleMode,
) -> Result<BubbleConfig, marketfeed_analytics::AnalyticsError> {
    let selector = SourceSelector::new(Vec::new(), vec![segment])?;
    let cap = fixed_multiple(quantity_increment, 10_000)?;
    let filter = |tier, preset, multiple, shape, max| {
        BubbleFilter::new(
            tier,
            mode,
            ThresholdMode::Adaptive(AdaptiveThreshold::new(
                preset,
                7_500,
                Some(fixed_multiple(quantity_increment, multiple)?),
                4,
                UI_BUBBLE_CALIBRATION_CANDLES,
                9_900,
                2_500,
            )?),
            selector.clone(),
            max,
            BubbleStyle::new(shape, 8, 42, cap, cap, LabelMode::Raw)?,
        )
    };
    BubbleConfig::new(
        filter(
            BubbleTier::F1,
            AdaptivePreset::Permissive,
            100,
            BubbleShape::Circle,
            18,
        )?,
        filter(
            BubbleTier::F2,
            AdaptivePreset::Strict,
            500,
            BubbleShape::Square,
            12,
        )?,
        filter(
            BubbleTier::F3,
            AdaptivePreset::UltraStrict,
            1_000,
            BubbleShape::Diamond,
            8,
        )?,
        MergeConfig::disabled(),
        PerformanceMode::Full,
        UI_BUBBLE_CALIBRATION_CANDLES,
    )
}

fn append_bubbles(target: &mut VecDeque<ViewBubble>, rows: Vec<OrderFlowBubble>, phase: &str) {
    for row in rows {
        while target.len() >= 2_000 {
            target.pop_front();
        }
        target.push_back(view_bubble(row, phase));
    }
}

fn view_bubble(bubble: OrderFlowBubble, phase: &str) -> ViewBubble {
    ViewBubble {
        id: bubble.id,
        phase: phase.into(),
        candle_start_ns: bubble.candle_start_ns,
        candle_end_ns: bubble.candle_end_ns,
        tier: match bubble.tier {
            BubbleTier::F1 => "f1",
            BubbleTier::F2 => "f2",
            BubbleTier::F3 => "f3",
        }
        .into(),
        mode: bubble_mode_label(bubble.mode).into(),
        direction: match bubble.direction {
            marketfeed_analytics::BubbleDirection::Buy => "buy",
            marketfeed_analytics::BubbleDirection::Sell => "sell",
            marketfeed_analytics::BubbleDirection::Neutral => "neutral",
        }
        .into(),
        anchor_price: format_fixed(bubble.anchor_price.0),
        low_price: format_fixed(bubble.low_price.0),
        high_price: format_fixed(bubble.high_price.0),
        total_volume: format_fixed(bubble.total_volume.0),
        delta: format_fixed(bubble.delta),
        strength: format_fixed(bubble.strength),
        threshold: format_fixed(bubble.threshold),
        visual_size: bubble.visual_size,
        shape: match bubble.shape {
            BubbleShape::Circle => "circle",
            BubbleShape::Square => "square",
            BubbleShape::Diamond => "diamond",
        }
        .into(),
        merged_count: bubble.merged_count,
    }
}

fn bubble_mode_label(mode: BubbleMode) -> &'static str {
    match mode {
        BubbleMode::Volume => "volume",
        BubbleMode::Delta => "delta",
    }
}

fn view_structural_level(level: StructuralLevel) -> ViewStructuralLevel {
    ViewStructuralLevel {
        id: level.id,
        kind: match level.kind {
            StructuralLevelKind::Naked => "naked",
            StructuralLevelKind::ReactionHigh => "reaction_high",
            StructuralLevelKind::ReactionLow => "reaction_low",
            StructuralLevelKind::TopDay => "top_day",
            StructuralLevelKind::TopWeek => "top_week",
        }
        .into(),
        state: match level.state {
            StructuralLevelState::Active => "active",
            StructuralLevelState::Touched => "touched",
        }
        .into(),
        source_bubble_id: level.source_bubble_id,
        direction: match level.direction {
            marketfeed_analytics::BubbleDirection::Buy => "buy",
            marketfeed_analytics::BubbleDirection::Sell => "sell",
            marketfeed_analytics::BubbleDirection::Neutral => "neutral",
        }
        .into(),
        tier: match level.tier {
            BubbleTier::F1 => "f1",
            BubbleTier::F2 => "f2",
            BubbleTier::F3 => "f3",
        }
        .into(),
        price: format_fixed(level.price.0),
        strength: format_fixed(level.strength),
        created_at_ns: level.created_at_ns,
        touched_at_ns: level.touched_at_ns,
        window_start_ns: level.window_start_ns,
        expires_at_ns: level.expires_at_ns,
    }
}

fn unavailable_structural_levels(
    venue: &str,
    instrument: InstrumentId,
    symbol: Option<String>,
    revision: u64,
    reason: &str,
) -> ViewStructuralLevelSnapshot {
    ViewStructuralLevelSnapshot {
        schema_version: 1,
        venue: venue.into(),
        instrument: instrument.0,
        symbol,
        revision,
        status: "unavailable".into(),
        reason: Some(reason.into()),
        levels: Vec::new(),
    }
}

fn unavailable_bubbles(
    venue: &str,
    instrument: InstrumentId,
    symbol: Option<String>,
    revision: u64,
    mode: String,
    reason: &str,
) -> ViewBubbleSnapshot {
    ViewBubbleSnapshot {
        schema_version: 1,
        venue: venue.into(),
        instrument: instrument.0,
        symbol,
        revision,
        status: "unavailable".into(),
        mode,
        reason: Some(reason.into()),
        bubbles: Vec::new(),
    }
}

fn unix_now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn fixed_difference(left: Fixed, right: Fixed) -> Option<Fixed> {
    let scale = left.scale.max(right.scale);
    let left = left
        .rescale(scale, marketfeed_model::RoundingMode::ExactOnly)
        .ok()?;
    let right = right
        .rescale(scale, marketfeed_model::RoundingMode::ExactOnly)
        .ok()?;
    Some(Fixed::new(
        left.coefficient.checked_sub(right.coefficient)?,
        scale,
    ))
}

fn compare_fixed(left: Fixed, right: Fixed) -> std::cmp::Ordering {
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

fn fixed_add(left: Fixed, right: Fixed) -> Option<Fixed> {
    let scale = left.scale.max(right.scale);
    let left = left
        .rescale(scale, marketfeed_model::RoundingMode::ExactOnly)
        .ok()?;
    let right = right
        .rescale(scale, marketfeed_model::RoundingMode::ExactOnly)
        .ok()?;
    Some(Fixed::new(
        left.coefficient.checked_add(right.coefficient)?,
        scale,
    ))
}

fn fixed_sub(left: Fixed, right: Fixed) -> Option<Fixed> {
    let scale = left.scale.max(right.scale);
    let left = left
        .rescale(scale, marketfeed_model::RoundingMode::ExactOnly)
        .ok()?;
    let right = right
        .rescale(scale, marketfeed_model::RoundingMode::ExactOnly)
        .ok()?;
    Some(Fixed::new(
        left.coefficient.checked_sub(right.coefficient)?,
        scale,
    ))
}

fn book_quantity_at(levels: &[BookLevel], price: Fixed) -> Option<Fixed> {
    levels
        .iter()
        .find(|level| compare_fixed(level.price.0, price).is_eq())
        .map(|level| level.quantity.0)
}

fn depth_quantity_at(
    sample: Option<&StoredDepthSample>,
    side: BookSide,
    price: Fixed,
    quantity_scale: u8,
) -> Fixed {
    let Some(sample) = sample else {
        return Fixed::new(0, quantity_scale);
    };
    let levels = match side {
        BookSide::Bid => &sample.bids,
        BookSide::Ask => &sample.asks,
    };
    levels
        .iter()
        .find_map(|level| {
            compare_fixed(
                Fixed::new(level.price_coefficient, sample.price_scale),
                price,
            )
            .is_eq()
            .then_some(Fixed::new(
                level.quantity_coefficient,
                sample.quantity_scale,
            ))
        })
        .unwrap_or(Fixed::new(0, quantity_scale))
}

fn cumulative_notional(
    levels: &[BookLevel],
    price: Fixed,
    side: BookSide,
    notional_scale: u8,
) -> Option<Fixed> {
    levels
        .iter()
        .filter(|level| match side {
            BookSide::Bid => !compare_fixed(level.price.0, price).is_lt(),
            BookSide::Ask => !compare_fixed(level.price.0, price).is_gt(),
        })
        .try_fold(Fixed::new(0, notional_scale), |sum, level| {
            fixed_add(sum, notional_fixed(level.price.0, level.quantity.0)?)
        })
}

fn quantity_imbalance_bps(bid: Fixed, ask: Fixed) -> Option<i32> {
    let scale = bid.scale.max(ask.scale);
    let (Ok(bid), Ok(ask)) = (
        bid.rescale(scale, marketfeed_model::RoundingMode::ExactOnly),
        ask.rescale(scale, marketfeed_model::RoundingMode::ExactOnly),
    ) else {
        return None;
    };
    let total = bid.coefficient.checked_add(ask.coefficient)?;
    if total <= 0 {
        return Some(0);
    }
    let value = bid
        .coefficient
        .checked_sub(ask.coefficient)
        .and_then(|difference| difference.checked_mul(10_000))
        .map(|numerator| numerator / total)?;
    Some(
        i32::try_from(value.clamp(i128::from(i32::MIN), i128::from(i32::MAX)))
            .expect("clamped imbalance fits i32"),
    )
}

fn funding_divergence(
    inner: &ViewInner,
    symbol: &str,
    now_ns: i64,
    stale_ns: i64,
) -> Option<ViewFundingDivergence> {
    let mut rates = Vec::new();
    for ((venue, instrument), projection) in &inner.derivatives {
        let Some(venue_name) = inner.id_to_venue.get(venue) else {
            continue;
        };
        if inner
            .symbols
            .get(&(venue_name.clone(), instrument.0))
            .map(String::as_str)
            != Some(symbol)
        {
            continue;
        }
        let Some(funding) = projection.funding.as_ref() else {
            continue;
        };
        if now_ns.saturating_sub(funding.event_ts_ns) <= stale_ns {
            rates.push(funding.rate);
        }
    }
    if rates.len() < 2 {
        return None;
    }
    let scale = rates.iter().map(|rate| rate.scale).max()?;
    let mut normalized = rates
        .into_iter()
        .map(|rate| {
            rate.rescale(scale, marketfeed_model::RoundingMode::ExactOnly)
                .ok()
        })
        .collect::<Option<Vec<_>>>()?;
    normalized.sort_by_key(|rate| rate.coefficient);
    let min = *normalized.first()?;
    let max = *normalized.last()?;
    Some(ViewFundingDivergence {
        compatible_venues: normalized.len(),
        min_rate: format_fixed(min),
        max_rate: format_fixed(max),
        spread: format_fixed(Fixed::new(
            max.coefficient.checked_sub(min.coefficient)?,
            scale,
        )),
    })
}

fn unavailable_derivatives(
    venue: &str,
    instrument: InstrumentId,
    symbol: Option<String>,
) -> ViewDerivativesSnapshot {
    ViewDerivativesSnapshot {
        schema_version: 1,
        venue: venue.into(),
        instrument: instrument.0,
        symbol,
        revision: 0,
        status: "unavailable".into(),
        funding: None,
        open_interest: None,
        funding_divergence: None,
        liquidations: Vec::new(),
    }
}

fn unavailable_dom(
    venue: &str,
    instrument: InstrumentId,
    symbol: Option<String>,
    execution_window_sec: u64,
    reason: &str,
) -> ViewDomSnapshot {
    ViewDomSnapshot {
        schema_version: 1,
        venue: venue.into(),
        instrument: instrument.0,
        symbol,
        revision: 0,
        status: "unavailable".into(),
        reason: Some(reason.into()),
        execution_window_sec: execution_window_sec.clamp(1, 3_600),
        epoch: 0,
        rows: Vec::new(),
    }
}

fn view_profile(
    venue: &str,
    instrument: InstrumentId,
    symbol: Option<String>,
    revision: u64,
    profile: &SessionProfile,
    error: Option<&str>,
) -> ViewProfileSnapshot {
    ViewProfileSnapshot {
        schema_version: 1,
        venue: venue.into(),
        instrument: instrument.0,
        symbol,
        revision,
        status: if error.is_some() {
            "degraded".into()
        } else {
            match profile.state {
                ProfileState::Live => "live".into(),
                ProfileState::Final => "final".into(),
            }
        },
        reason: error.map(str::to_owned),
        basis: Some(match profile.basis {
            ValueAreaBasis::Volume => "volume".into(),
            ValueAreaBasis::Tpo => "tpo".into(),
        }),
        value_area_bps: Some(profile.value_area_bps),
        start_ts_ns: Some(profile.start_ts),
        end_ts_ns: Some(profile.end_ts),
        high: profile.high.map(|value| format_fixed(value.0)),
        low: profile.low.map(|value| format_fixed(value.0)),
        range: profile.range.map(format_fixed),
        total_volume: Some(format_fixed(profile.total_volume.0)),
        poc: profile.poc.map(|value| format_fixed(value.0)),
        vah: profile.vah.map(|value| format_fixed(value.0)),
        val: profile.val.map(|value| format_fixed(value.0)),
        tpo_count: Some(profile.tpo_count),
        rotation_factor: Some(profile.rotation_factor),
    }
}

impl ViewInner {
    fn venue_tape_stats(&self, venue_id: VenueId) -> (u64, u64, u64, u64) {
        let mut trades = 0u64;
        let mut trades_dropped = 0u64;
        let mut quotes = 0u64;
        let mut quotes_dropped = 0u64;
        for ((vid, _), tape) in &self.tapes {
            if *vid != venue_id {
                continue;
            }
            let (t, td) = tape.trade_stats();
            let (q, qd) = tape.quote_stats();
            trades = trades.saturating_add(t);
            trades_dropped = trades_dropped.saturating_add(td);
            quotes = quotes.saturating_add(q);
            quotes_dropped = quotes_dropped.saturating_add(qd);
        }
        (trades, trades_dropped, quotes, quotes_dropped)
    }
}

impl EventSink for ViewPlane {
    fn push_batch(&mut self, batch: EventBatch) -> Result<PushOutcome, SinkError> {
        if let Some(venue) = self.ingest_batch(&batch) {
            self.sink_venue = Some(venue);
        }
        Ok(PushOutcome::Accepted)
    }

    fn push_system(&mut self, event: SystemEvent) -> Result<PushOutcome, SinkError> {
        self.ingest_system(self.sink_venue, &event);
        Ok(PushOutcome::Accepted)
    }
}

/// Shared `Arc` wrapper so venues can fan-out without owning the plane.
#[derive(Debug, Clone)]
pub struct SharedViewPlane(pub std::sync::Arc<ViewPlane>, Option<VenueId>);

impl SharedViewPlane {
    pub fn new(inner: std::sync::Arc<ViewPlane>) -> Self {
        Self(inner, None)
    }

    pub fn for_venue(inner: std::sync::Arc<ViewPlane>, venue: VenueId) -> Self {
        Self(inner, Some(venue))
    }
}

impl EventSink for SharedViewPlane {
    fn push_batch(&mut self, batch: EventBatch) -> Result<PushOutcome, SinkError> {
        if let Some(venue) = self.0.ingest_batch(&batch) {
            self.1 = Some(venue);
        }
        Ok(PushOutcome::Accepted)
    }

    fn push_system(&mut self, event: SystemEvent) -> Result<PushOutcome, SinkError> {
        self.0.ingest_system(self.1, &event);
        Ok(PushOutcome::Accepted)
    }
}

fn level_json(level: &BookLevel) -> ViewLevel {
    ViewLevel {
        price: format_fixed(level.price.0),
        quantity: format_fixed(level.quantity.0),
    }
}

fn view_depth_sample(sample: &StoredDepthSample) -> ViewDepthSample {
    let level_json = |level: &StoredDepthLevel| ViewLevel {
        price: format_fixed(Fixed::new(level.price_coefficient, sample.price_scale)),
        quantity: format_fixed(Fixed::new(
            level.quantity_coefficient,
            sample.quantity_scale,
        )),
    };
    ViewDepthSample {
        event_ts_ns: sample.event_ts_ns,
        epoch: sample.epoch,
        bids: sample.bids.iter().map(level_json).collect(),
        asks: sample.asks.iter().map(level_json).collect(),
    }
}

fn aggressor_str(side: AggressorSide) -> &'static str {
    match side {
        AggressorSide::Buy => "buy",
        AggressorSide::Sell => "sell",
        AggressorSide::Unknown => "unknown",
    }
}

fn format_fixed(f: Fixed) -> String {
    let neg = f.coefficient < 0;
    let mag = f.coefficient.unsigned_abs();
    let scale = f.scale as usize;
    let digits = mag.to_string();
    let (int_part, frac_part) = if scale == 0 {
        (digits, String::new())
    } else if digits.len() <= scale {
        ("0".to_string(), format!("{digits:0>scale$}"))
    } else {
        let split = digits.len() - scale;
        (digits[..split].to_string(), digits[split..].to_string())
    };
    let mut s = String::new();
    if neg {
        s.push('-');
    }
    s.push_str(&int_part);
    if !frac_part.is_empty() {
        s.push('.');
        s.push_str(&frac_part);
    }
    s
}

fn notional_fixed(price: Fixed, qty: Fixed) -> Option<Fixed> {
    let scale = price.scale.checked_add(qty.scale)?;
    let coefficient = price.coefficient.checked_mul(qty.coefficient)?;
    Some(Fixed { coefficient, scale })
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketfeed_model::{
        AssetCode, CatalogVersion, CatalogView, ConnectionId, EventEnvelope, EventFlags, Funding,
        InstrumentDefinition, InstrumentKey, InstrumentKind, InstrumentStatus, Liquidation,
        OpenInterest, Price, Quantity, Rate, SessionId, SourceId, TimestampNs, Trade, VenueCode,
    };

    fn authoritative_catalog(venue: VenueId) -> CatalogView {
        let definition = InstrumentDefinition {
            key: InstrumentKey {
                venue: VenueCode("test".into()),
                native_symbol: "BTCUSDT".into(),
                kind: InstrumentKind::PerpetualLinear,
                settlement: Some(AssetCode("USDT".into())),
                expiry_ns: None,
            },
            base: AssetCode("BTC".into()),
            quote: AssetCode("USDT".into()),
            settlement: Some(AssetCode("USDT".into())),
            price_scale: 2,
            quantity_scale: 3,
            price_increment: Fixed::new(1, 2),
            quantity_increment: Fixed::new(1, 3),
            min_quantity: None,
            max_quantity: None,
            min_notional: None,
            contract_size: None,
            expiry_ns: None,
            status: InstrumentStatus::Active,
            inverse: false,
        };
        CatalogView::with_instruments(
            venue,
            CatalogVersion(1),
            std::sync::Arc::from([definition.into_instrument(InstrumentId(1), CatalogVersion(1))]),
        )
    }

    fn trade_batch(venue: VenueId, instrument: InstrumentId, price: &str, qty: &str) -> EventBatch {
        trade_batch_at(venue, instrument, price, qty, 1)
    }

    fn trade_batch_at(
        venue: VenueId,
        instrument: InstrumentId,
        price: &str,
        qty: &str,
        timestamp_ns: i64,
    ) -> EventBatch {
        EventBatch {
            session: SessionId(1),
            frame_seq: 1,
            events: vec![EventEnvelope {
                schema_version: 1,
                venue,
                instrument: Some(instrument),
                connection: ConnectionId(1),
                session: SessionId(1),
                frame_seq: 1,
                event_index: 0,
                exchange_ts: Some(TimestampNs(timestamp_ns)),
                receive_ts: TimestampNs(timestamp_ns.saturating_add(1)),
                source_sequence: None,
                flags: EventFlags::default(),
                payload: MarketEvent::Trade(Trade {
                    price: Price(Fixed::parse_str(price).unwrap()),
                    quantity: Quantity(Fixed::parse_str(qty).unwrap()),
                    aggressor: AggressorSide::Buy,
                    trade_id: Some(SourceId("t1".into())),
                }),
            }],
        }
    }

    #[test]
    fn tape_drop_newest_and_rate_cap() {
        let plane = ViewPlane::new(ViewPlaneConfig {
            tape_capacity: 2,
            tape_max_per_sec: 0,
        });
        plane.register_venue(VenueId(7), "syn", &["BTC-USD".into()]);
        let mut sink = SharedViewPlane::new(std::sync::Arc::new(plane));
        // Re-get plane via sink
        let plane = std::sync::Arc::clone(&sink.0);
        sink.push_batch(trade_batch(VenueId(7), InstrumentId(1), "100", "1"))
            .unwrap();
        sink.push_batch(trade_batch(VenueId(7), InstrumentId(1), "101", "1"))
            .unwrap();
        sink.push_batch(trade_batch(VenueId(7), InstrumentId(1), "102", "1"))
            .unwrap();
        let tape = plane.tape("syn", InstrumentId(1), 10);
        assert_eq!(tape.len(), 2);
        match &tape[0] {
            TapeEntry::Trade { price, .. } => assert_eq!(price, "102"),
            _ => panic!("expected trade"),
        }
    }

    #[test]
    fn authoritative_catalog_projects_exact_live_market_profile() {
        let plane = std::sync::Arc::new(ViewPlane::new(ViewPlaneConfig::default()));
        plane.register_venue(VenueId(7), "perp", &["BTCUSDT".into()]);
        plane.register_catalog(
            "perp",
            &authoritative_catalog(VenueId(7)),
            CatalogAuthority::Authoritative,
        );
        let mut sink = SharedViewPlane::for_venue(std::sync::Arc::clone(&plane), VenueId(7));

        sink.push_batch(trade_batch(VenueId(7), InstrumentId(1), "100.00", "2.000"))
            .unwrap();
        sink.push_batch(trade_batch(VenueId(7), InstrumentId(1), "101.00", "1.000"))
            .unwrap();

        let profile = plane.profile_snapshot("perp", InstrumentId(1), ValueAreaBasis::Volume);
        assert_eq!(profile.status, "live");
        assert_eq!(profile.basis.as_deref(), Some("volume"));
        assert_eq!(profile.poc.as_deref(), Some("100.00"));
        assert_eq!(profile.val.as_deref(), Some("100.00"));
        assert_eq!(profile.vah.as_deref(), Some("101.00"));
        assert_eq!(profile.high.as_deref(), Some("101.00"));
        assert_eq!(profile.low.as_deref(), Some("100.00"));
        assert_eq!(profile.range.as_deref(), Some("1.00"));
        assert_eq!(profile.total_volume.as_deref(), Some("3.000"));
        assert_eq!(profile.tpo_count, Some(2));
        assert_eq!(profile.rotation_factor, Some(0));

        let tpo_profile = plane.profile_snapshot("perp", InstrumentId(1), ValueAreaBasis::Tpo);
        assert_eq!(tpo_profile.status, "live");
        assert_eq!(tpo_profile.basis.as_deref(), Some("tpo"));
        assert_eq!(tpo_profile.total_volume.as_deref(), Some("3.000"));
        assert_eq!(tpo_profile.tpo_count, Some(2));

        let bubbles = plane.bubble_snapshot("perp", InstrumentId(1), BubbleMode::Volume);
        assert_eq!(bubbles.status, "live");
        assert_eq!(bubbles.mode, "volume");
        assert_eq!(bubbles.bubbles.len(), 2);
        assert!(bubbles.bubbles.iter().all(|bubble| bubble.tier == "f3"));
        assert!(bubbles.bubbles.iter().all(|bubble| bubble.phase == "live"));
    }

    #[test]
    fn analytics_rollover_does_not_hold_global_view_lock() {
        let plane = Arc::new(ViewPlane::new(ViewPlaneConfig::default()));
        plane.register_venue(VenueId(7), "perp", &["BTCUSDT".into()]);
        plane.register_catalog(
            "perp",
            &authoritative_catalog(VenueId(7)),
            CatalogAuthority::Authoritative,
        );
        let profile = {
            let inner = plane.inner.lock().expect("view lock");
            Arc::clone(
                inner
                    .profiles
                    .get(&(VenueId(7), InstrumentId(1)))
                    .expect("profile projection"),
            )
        };
        let profile_guard = profile.lock().expect("profile projection lock");
        let mut sink = SharedViewPlane::for_venue(Arc::clone(&plane), VenueId(7));
        let worker = std::thread::spawn(move || {
            sink.push_batch(trade_batch(VenueId(7), InstrumentId(1), "100.00", "1.000"))
                .unwrap();
        });

        let deadline = Instant::now() + Duration::from_secs(1);
        while plane.tape("perp", InstrumentId(1), 1).is_empty() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert_eq!(plane.tape("perp", InstrumentId(1), 1).len(), 1);
        assert!(plane.inner.try_lock().is_ok());

        drop(profile_guard);
        worker.join().unwrap();
    }

    #[test]
    fn ui_bubble_history_matches_the_bounded_calibration_window() {
        let config = default_bubble_config(
            Fixed::new(1, 3),
            MarketSegment::LinearPerpetual,
            BubbleMode::Volume,
        )
        .unwrap();
        assert_eq!(config.max_history_candles, UI_BUBBLE_CALIBRATION_CANDLES);
        for filter in [&config.f1, &config.f2, &config.f3] {
            let ThresholdMode::Adaptive(adaptive) = &filter.threshold else {
                panic!("default UI filter must remain adaptive");
            };
            assert_eq!(adaptive.calibration_candles, UI_BUBBLE_CALIBRATION_CANDLES);
        }
    }

    #[test]
    fn finalized_server_bubbles_project_structural_levels() {
        let plane = std::sync::Arc::new(ViewPlane::new(ViewPlaneConfig::default()));
        plane.register_venue(VenueId(7), "perp", &["BTCUSDT".into()]);
        plane.register_catalog(
            "perp",
            &authoritative_catalog(VenueId(7)),
            CatalogAuthority::Authoritative,
        );
        let mut sink = SharedViewPlane::for_venue(std::sync::Arc::clone(&plane), VenueId(7));
        for (price, timestamp) in [
            ("100.00", 1_000_000_000),
            ("110.00", 61_000_000_000),
            ("111.00", 121_000_000_000),
        ] {
            sink.push_batch(trade_batch_at(
                VenueId(7),
                InstrumentId(1),
                price,
                "2.000",
                timestamp,
            ))
            .unwrap();
        }

        let levels = plane.structural_level_snapshot("perp", InstrumentId(1));
        assert_eq!(levels.status, "live");
        assert!(levels.levels.iter().any(|level| {
            level.kind == "naked" && level.state == "active" && level.price == "100.00"
        }));
        assert!(levels.levels.iter().any(|level| level.kind == "top_day"));
        assert!(levels.levels.iter().any(|level| level.kind == "top_week"));
    }

    fn quote_batch(venue: VenueId, instrument: InstrumentId, bid: &str, ask: &str) -> EventBatch {
        use marketfeed_model::Quote;
        EventBatch {
            session: SessionId(1),
            frame_seq: 1,
            events: vec![EventEnvelope {
                schema_version: 1,
                venue,
                instrument: Some(instrument),
                connection: ConnectionId(1),
                session: SessionId(1),
                frame_seq: 1,
                event_index: 0,
                exchange_ts: Some(TimestampNs(1)),
                receive_ts: TimestampNs(2),
                source_sequence: None,
                flags: EventFlags::default(),
                payload: MarketEvent::Quote(Quote {
                    bid_price: Price(Fixed::parse_str(bid).unwrap()),
                    bid_quantity: Some(Quantity(Fixed::parse_str("1").unwrap())),
                    ask_price: Price(Fixed::parse_str(ask).unwrap()),
                    ask_quantity: Some(Quantity(Fixed::parse_str("1").unwrap())),
                }),
            }],
        }
    }

    fn market_event_batch(
        venue: VenueId,
        instrument: InstrumentId,
        timestamp_ns: i64,
        payload: MarketEvent,
    ) -> EventBatch {
        EventBatch {
            session: SessionId(1),
            frame_seq: 1,
            events: vec![EventEnvelope {
                schema_version: 1,
                venue,
                instrument: Some(instrument),
                connection: ConnectionId(1),
                session: SessionId(1),
                frame_seq: 1,
                event_index: 0,
                exchange_ts: Some(TimestampNs(timestamp_ns)),
                receive_ts: TimestampNs(timestamp_ns),
                source_sequence: None,
                flags: EventFlags::default(),
                payload,
            }],
        }
    }

    #[test]
    fn derivatives_projection_preserves_funding_oi_change_and_liquidations() {
        let plane = std::sync::Arc::new(ViewPlane::new(ViewPlaneConfig::default()));
        plane.register_venue(VenueId(7), "perp", &["BTCUSDT".into()]);
        let mut sink = SharedViewPlane::for_venue(std::sync::Arc::clone(&plane), VenueId(7));
        let now = unix_now_ns();
        for event in [
            MarketEvent::Funding(Funding {
                rate: Rate(Fixed::parse_str("0.0001").unwrap()),
                next_funding_ts: Some(TimestampNs(now + 1_000_000_000)),
            }),
            MarketEvent::OpenInterest(OpenInterest {
                quantity: Quantity(Fixed::parse_str("100.000").unwrap()),
            }),
        ] {
            sink.push_batch(market_event_batch(VenueId(7), InstrumentId(1), now, event))
                .unwrap();
        }
        sink.push_batch(market_event_batch(
            VenueId(7),
            InstrumentId(1),
            now + 1,
            MarketEvent::OpenInterest(OpenInterest {
                quantity: Quantity(Fixed::parse_str("102.500").unwrap()),
            }),
        ))
        .unwrap();
        sink.push_batch(market_event_batch(
            VenueId(7),
            InstrumentId(1),
            now + 2,
            MarketEvent::Liquidation(Liquidation {
                price: Price(Fixed::parse_str("100.00").unwrap()),
                quantity: Quantity(Fixed::parse_str("0.250").unwrap()),
                side: AggressorSide::Sell,
            }),
        ))
        .unwrap();

        let snapshot = plane.derivatives_snapshot("perp", InstrumentId(1));
        assert_eq!(snapshot.status, "live");
        assert_eq!(snapshot.funding.unwrap().rate, "0.0001");
        let oi = snapshot.open_interest.unwrap();
        assert_eq!(oi.quantity, "102.500");
        assert_eq!(oi.change.as_deref(), Some("2.500"));
        assert_eq!(snapshot.liquidations.len(), 1);
        assert_eq!(snapshot.liquidations[0].side, "sell");
    }

    #[test]
    fn dom_snapshot_preserves_exact_book_flow_and_mbp_columns() {
        let plane = std::sync::Arc::new(ViewPlane::new(ViewPlaneConfig::default()));
        plane.register_venue(VenueId(7), "perp", &["BTCUSDT".into()]);
        plane.register_catalog(
            "perp",
            &authoritative_catalog(VenueId(7)),
            CatalogAuthority::Authoritative,
        );
        let mut sink = SharedViewPlane::for_venue(std::sync::Arc::clone(&plane), VenueId(7));
        let now = unix_now_ns();

        sink.push_batch(market_event_batch(
            VenueId(7),
            InstrumentId(1),
            now.saturating_sub(200_000_000),
            MarketEvent::BookSnapshot(BookSnapshot {
                bids: vec![marketfeed_model::BookLevel {
                    price: Price(Fixed::parse_str("100.00").unwrap()),
                    quantity: Quantity(Fixed::parse_str("2.000").unwrap()),
                }],
                asks: vec![marketfeed_model::BookLevel {
                    price: Price(Fixed::parse_str("101.00").unwrap()),
                    quantity: Quantity(Fixed::parse_str("3.000").unwrap()),
                }],
                depth: Some(1),
                checksum: None,
            }),
        ))
        .unwrap();
        sink.push_batch(market_event_batch(
            VenueId(7),
            InstrumentId(1),
            now,
            MarketEvent::BookDelta(BookDelta {
                changes: vec![marketfeed_model::BookChange {
                    side: marketfeed_model::BookSide::Bid,
                    price: Price(Fixed::parse_str("100.00").unwrap()),
                    quantity: Some(Quantity(Fixed::parse_str("3.000").unwrap())),
                    operation: marketfeed_model::BookOperation::Upsert,
                }],
                checksum: None,
            }),
        ))
        .unwrap();
        sink.push_batch(market_event_batch(
            VenueId(7),
            InstrumentId(1),
            now,
            MarketEvent::Trade(Trade {
                price: Price(Fixed::parse_str("100.00").unwrap()),
                quantity: Quantity(Fixed::parse_str("0.250").unwrap()),
                aggressor: AggressorSide::Buy,
                trade_id: Some(SourceId("dom-1".into())),
            }),
        ))
        .unwrap();

        let dom = plane.dom_snapshot("perp", InstrumentId(1), 8, 300);
        assert_eq!(dom.status, "live");
        let bid = dom
            .rows
            .iter()
            .find(|row| row.price == "100.00")
            .expect("100 bid row");
        assert_eq!(bid.bid_quantity, "3.000");
        assert_eq!(bid.ask_quantity, "0.000");
        assert_eq!(bid.mbp_delta_quantity, "1.000");
        assert_eq!(bid.buy_executed_notional, "25.00000");
        assert_eq!(bid.sell_executed_notional, "0.00000");
        assert_eq!(bid.executed_delta_notional, "25.00000");
    }

    fn book_batch(venue: VenueId, instrument: InstrumentId, bid: &str, ask: &str) -> EventBatch {
        let snap = BookSnapshot {
            bids: vec![BookLevel {
                price: Price(Fixed::parse_str(bid).unwrap()),
                quantity: Quantity(Fixed::parse_str("1.5").unwrap()),
            }],
            asks: vec![BookLevel {
                price: Price(Fixed::parse_str(ask).unwrap()),
                quantity: Quantity(Fixed::parse_str("2.0").unwrap()),
            }],
            depth: Some(50),
            checksum: None,
        };
        EventBatch {
            session: SessionId(1),
            frame_seq: 1,
            events: vec![EventEnvelope {
                schema_version: 1,
                venue,
                instrument: Some(instrument),
                connection: ConnectionId(1),
                session: SessionId(1),
                frame_seq: 1,
                event_index: 0,
                exchange_ts: None,
                receive_ts: TimestampNs(1),
                source_sequence: None,
                flags: EventFlags::default(),
                payload: MarketEvent::BookSnapshot(snap),
            }],
        }
    }

    #[test]
    fn quote_flood_does_not_evict_trades() {
        let plane = ViewPlane::new(ViewPlaneConfig {
            tape_capacity: 2,
            tape_max_per_sec: 0,
        });
        plane.register_venue(VenueId(7), "syn", &["BTC-USD".into()]);
        let mut sink = SharedViewPlane::new(std::sync::Arc::new(plane));
        let plane = std::sync::Arc::clone(&sink.0);
        sink.push_batch(trade_batch(VenueId(7), InstrumentId(1), "100", "0.5"))
            .unwrap();
        for i in 0..20 {
            let bid = format!("{}", 99 + i);
            let ask = format!("{}", 101 + i);
            sink.push_batch(quote_batch(VenueId(7), InstrumentId(1), &bid, &ask))
                .unwrap();
        }
        let trades = plane.tape_filtered("syn", InstrumentId(1), 10, Some("trade"));
        assert_eq!(trades.len(), 1);
        match &trades[0] {
            TapeEntry::Trade {
                price, quantity, ..
            } => {
                assert_eq!(price, "100");
                assert_eq!(quantity, "0.5");
            }
            _ => panic!("expected trade"),
        }
        let quotes = plane.tape_filtered("syn", InstrumentId(1), 10, Some("quote"));
        assert_eq!(quotes.len(), 2);
    }

    #[test]
    fn book_snapshot_from_events() {
        assert_eq!(std::mem::size_of::<StoredDepthLevel>(), 32);
        let plane = ViewPlane::new(ViewPlaneConfig::default());
        plane.register_venue(VenueId(1), "syn", &["BTC-USD".into()]);
        let mut sink = SharedViewPlane::new(std::sync::Arc::new(plane));
        sink.push_batch(book_batch(VenueId(1), InstrumentId(1), "100.00", "101.00"))
            .unwrap();
        let view = sink
            .0
            .book_snapshot("syn", InstrumentId(1), Some(1))
            .unwrap();
        assert_eq!(view.bids.len(), 1);
        assert_eq!(view.asks[0].price, "101.00");
        assert_eq!(view.symbol.as_deref(), Some("BTC-USD"));
        let history = sink.0.depth_history("syn", InstrumentId(1), 10);
        assert_eq!(history.samples.len(), 1);
        assert_eq!(history.capacity, DEPTH_HISTORY_CAPACITY);
        assert_eq!(history.samples[0].bids[0].price, "100.00");
        assert_eq!(history.coalesced_samples, 0);
        assert_eq!(history.evicted_samples, 0);
        {
            let inner = sink.0.inner.lock().expect("view lock");
            let stored = inner
                .depth_history
                .get(&(VenueId(1), InstrumentId(1)))
                .and_then(|samples| samples.front())
                .expect("stored depth sample");
            assert_eq!(stored.price_scale, 2);
            assert_eq!(stored.bids[0].price_coefficient, 10_000);
        }

        sink.push_batch(book_batch(VenueId(1), InstrumentId(1), "99.00", "102.00"))
            .unwrap();
        let history = sink.0.depth_history("syn", InstrumentId(1), 10);
        assert_eq!(history.samples.len(), 1);
        assert_eq!(history.coalesced_samples, 1);
    }

    #[test]
    fn depth_ring_reuses_evicted_level_buffers() {
        let plane = ViewPlane::new(ViewPlaneConfig::default());
        let snapshot = BookSnapshot {
            bids: vec![BookLevel {
                price: Price(Fixed::parse_str("100.00").unwrap()),
                quantity: Quantity(Fixed::parse_str("1.000").unwrap()),
            }],
            asks: vec![BookLevel {
                price: Price(Fixed::parse_str("101.00").unwrap()),
                quantity: Quantity(Fixed::parse_str("2.000").unwrap()),
            }],
            depth: Some(1),
            checksum: None,
        };
        let key = (VenueId(1), InstrumentId(1));
        let sample_ns = (DEPTH_SAMPLE_INTERVAL_MS as i64) * 1_000_000;
        let first_bid_ptr = {
            let mut inner = plane.inner.lock().expect("view lock");
            for index in 0..DEPTH_HISTORY_CAPACITY {
                push_depth_sample(
                    &mut inner,
                    key.0,
                    key.1,
                    index as i64 * sample_ns,
                    snapshot.clone(),
                );
            }
            inner.depth_history[&key].front().unwrap().bids.as_ptr()
        };

        {
            let mut inner = plane.inner.lock().expect("view lock");
            push_depth_sample(
                &mut inner,
                key.0,
                key.1,
                DEPTH_HISTORY_CAPACITY as i64 * sample_ns,
                snapshot,
            );
            let history = &inner.depth_history[&key];
            assert_eq!(history.len(), DEPTH_HISTORY_CAPACITY);
            assert_eq!(history.back().unwrap().bids.as_ptr(), first_bid_ptr);
            assert_eq!(inner.depth_evicted_samples[&key], 1);
        }
    }

    #[test]
    fn direct_sink_book_invalidation_removes_only_current_venue_instrument() {
        let mut plane = ViewPlane::new(ViewPlaneConfig::default());
        plane.register_venue(VenueId(1), "one", &["BTC-USD".into()]);
        plane.register_venue(VenueId(2), "two", &["BTC-USD".into()]);
        plane
            .push_batch(book_batch(VenueId(2), InstrumentId(1), "200", "201"))
            .unwrap();
        plane
            .push_batch(book_batch(VenueId(1), InstrumentId(1), "100", "101"))
            .unwrap();

        plane
            .push_system(SystemEvent::BookInvalidated {
                instrument: InstrumentId(1),
                reason: "sequence gap".into(),
            })
            .unwrap();

        assert!(plane.book_snapshot("one", InstrumentId(1), None).is_none());
        assert!(plane.book_snapshot("two", InstrumentId(1), None).is_some());
    }

    #[test]
    fn shared_sink_book_invalidation_removes_only_current_venue_instrument() {
        let plane = std::sync::Arc::new(ViewPlane::new(ViewPlaneConfig::default()));
        plane.register_venue(VenueId(1), "one", &["BTC-USD".into()]);
        plane.register_venue(VenueId(2), "two", &["BTC-USD".into()]);
        let mut one = SharedViewPlane::for_venue(std::sync::Arc::clone(&plane), VenueId(1));
        let mut two = SharedViewPlane::for_venue(std::sync::Arc::clone(&plane), VenueId(2));
        one.push_batch(book_batch(VenueId(1), InstrumentId(1), "100", "101"))
            .unwrap();
        two.push_batch(book_batch(VenueId(2), InstrumentId(1), "200", "201"))
            .unwrap();

        let mut fresh_one = SharedViewPlane::for_venue(std::sync::Arc::clone(&plane), VenueId(1));
        fresh_one
            .push_system(SystemEvent::BookInvalidated {
                instrument: InstrumentId(1),
                reason: "checksum".into(),
            })
            .unwrap();

        assert!(plane.book_snapshot("one", InstrumentId(1), None).is_none());
        assert!(plane.book_snapshot("two", InstrumentId(1), None).is_some());
    }

    #[test]
    fn trade_notional_computed() {
        let plane = ViewPlane::new(ViewPlaneConfig::default());
        plane.register_venue(VenueId(7), "syn", &["BTC-USD".into()]);
        let mut sink = SharedViewPlane::new(std::sync::Arc::new(plane));
        let plane = std::sync::Arc::clone(&sink.0);
        sink.push_batch(trade_batch(VenueId(7), InstrumentId(1), "100.50", "2"))
            .unwrap();
        let tape = plane.tape("syn", InstrumentId(1), 1);
        match &tape[0] {
            TapeEntry::Trade {
                notional,
                price,
                quantity,
                ..
            } => {
                assert_eq!(price, "100.50");
                assert_eq!(quantity, "2");
                assert_eq!(notional.as_deref(), Some("201.00"));
            }
            _ => panic!("expected trade"),
        }
    }

    #[test]
    fn status_tracks_last_event_ts() {
        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [readiness]
            require_required_venues = false
            [[venues]]
            id = "syn"
            adapter = "synthetic"
            required = false
            "#,
        )
        .unwrap();
        let state = crate::state::DaemonState::new(cfg);
        let plane = ViewPlane::from_daemon_config(&state.config);
        plane.register_venue(VenueId(1), "syn", &["BTC-USD".into()]);
        let mut sink = SharedViewPlane::new(std::sync::Arc::new(plane));
        sink.push_batch(trade_batch(VenueId(1), InstrumentId(1), "100", "1"))
            .unwrap();
        let status = sink.0.status(&state);
        let venue = status.venues.iter().find(|v| v.id == "syn").unwrap();
        assert!(venue.last_event_ts_ns.is_some());
        assert!(venue.last_trade_ts_ns.is_some());
        assert_eq!(venue.tape_trades, 1);

        // Exchange/receive clocks can lead the subsequent wall-clock sample by
        // a few nanoseconds. The UI must show zero lag, never a u64 wraparound.
        sink.0
            .inner
            .lock()
            .expect("view lock")
            .venue_last_event_ts
            .insert(VenueId(1), i64::MAX);
        let status = sink.0.status(&state);
        let venue = status.venues.iter().find(|v| v.id == "syn").unwrap();
        assert_eq!(venue.feed_lag_ms, Some(0));
    }
}
