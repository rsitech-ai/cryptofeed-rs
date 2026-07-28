//! Server-side book cache + drop_newest tape rings.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use marketfeed_adapter_api::EventBatch;
use marketfeed_book::OrderBook;
use marketfeed_dispatch::PushOutcome;
use marketfeed_model::{
    AggressorSide, BookDelta, BookLevel, BookSnapshot, Fixed, InstrumentId, MarketEvent,
    SystemEvent, VenueId,
};
use marketfeed_sinks::{EventSink, SinkError};
use serde::Serialize;

use crate::config::DaemonConfig;
use crate::state::DaemonState;

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
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TapeEntry {
    Trade {
        venue: String,
        instrument: u32,
        symbol: Option<String>,
        price: String,
        quantity: String,
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
}

#[derive(Debug)]
struct LiveBook {
    book: OrderBook,
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
        let (mut bids, mut asks) = self.book.snapshot_levels()?;
        if let Some(d) = depth {
            let n = d as usize;
            bids.truncate(n);
            asks.truncate(n);
        }
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
    tapes: HashMap<(VenueId, InstrumentId), TapeRing>,
}

/// Shared view plane: EventSink fan-in + HTTP query surface.
#[derive(Debug)]
pub struct ViewPlane {
    cfg: ViewPlaneConfig,
    inner: Mutex<ViewInner>,
    batches_seen: AtomicU64,
    tape_dropped: AtomicU64,
}

impl ViewPlane {
    pub fn new(cfg: ViewPlaneConfig) -> Self {
        Self {
            cfg,
            inner: Mutex::new(ViewInner::default()),
            batches_seen: AtomicU64::new(0),
            tape_dropped: AtomicU64::new(0),
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

    pub fn status(&self, state: &DaemonState) -> ViewStatus {
        let started = state.started_unix_secs;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(started);
        let lifecycle = match *state.supervisor_lifecycle.lock().expect("lifecycle") {
            marketfeed_engine::EngineLifecycle::Starting => "starting",
            marketfeed_engine::EngineLifecycle::Running => "running",
            marketfeed_engine::EngineLifecycle::Draining => "draining",
            marketfeed_engine::EngineLifecycle::Stopped => "stopped",
        };
        let mut venues = Vec::with_capacity(state.config.venues.len());
        for v in &state.config.venues {
            let live = state
                .venue_flags
                .get(&v.id)
                .map(|f| f.load(Ordering::Relaxed))
                .unwrap_or(false);
            let m = state.venue_metrics.get(&v.id);
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
                valid_books: m.map(|x| x.valid_books.load(Ordering::Relaxed)).unwrap_or(0),
                queue_occupancy: m
                    .map(|x| x.batch_queue_occupancy.load(Ordering::Relaxed))
                    .unwrap_or(0),
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
        let inner = self.inner.lock().expect("view lock");
        let Some(venue_id) = inner.venue_ids.get(venue_config_id).copied() else {
            return Vec::new();
        };
        inner
            .tapes
            .get(&(venue_id, instrument))
            .map(|t| t.snapshot(limit.max(1)))
            .unwrap_or_default()
    }

    pub fn stats(&self) -> (u64, u64) {
        (
            self.batches_seen.load(Ordering::Relaxed),
            self.tape_dropped.load(Ordering::Relaxed),
        )
    }

    fn ingest_batch(&self, batch: &EventBatch) {
        self.batches_seen.fetch_add(1, Ordering::Relaxed);
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

            match &ev.payload {
                MarketEvent::BookSnapshot(snap) => {
                    if let Some(live) = LiveBook::from_snapshot(snap) {
                        inner.books.insert((venue_id, instrument), live);
                    }
                }
                MarketEvent::BookDelta(delta) => {
                    if let Some(live) = inner.books.get_mut(&(venue_id, instrument)) {
                        let _ = live.apply_delta(delta);
                    }
                }
                MarketEvent::Trade(t) => {
                    let symbol = inner
                        .symbols
                        .get(&(venue_name.clone(), instrument.0))
                        .cloned();
                    let entry = TapeEntry::Trade {
                        venue: venue_name.clone(),
                        instrument: instrument.0,
                        symbol,
                        price: format_fixed(t.price.0),
                        quantity: format_fixed(t.quantity.0),
                        aggressor: aggressor_str(t.aggressor).into(),
                        trade_id: t.trade_id.as_ref().map(|s| s.0.clone()),
                        exchange_ts_ns: ev.exchange_ts.map(|t| t.0),
                        receive_ts_ns: ev.receive_ts.0,
                    };
                    let ring = inner.tapes.entry((venue_id, instrument)).or_insert_with(|| {
                        TapeRing::new(self.cfg.tape_capacity, self.cfg.tape_max_per_sec)
                    });
                    let before = ring.dropped;
                    ring.push(entry);
                    if ring.dropped > before {
                        self.tape_dropped
                            .fetch_add(ring.dropped - before, Ordering::Relaxed);
                    }
                }
                MarketEvent::Quote(q) => {
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
                        receive_ts_ns: ev.receive_ts.0,
                    };
                    let ring = inner.tapes.entry((venue_id, instrument)).or_insert_with(|| {
                        TapeRing::new(self.cfg.tape_capacity, self.cfg.tape_max_per_sec)
                    });
                    let before = ring.dropped;
                    ring.push(entry);
                    if ring.dropped > before {
                        self.tape_dropped
                            .fetch_add(ring.dropped - before, Ordering::Relaxed);
                    }
                }
                _ => {}
            }
        }
    }
}

impl EventSink for ViewPlane {
    fn push_batch(&mut self, batch: EventBatch) -> Result<PushOutcome, SinkError> {
        self.ingest_batch(&batch);
        Ok(PushOutcome::Accepted)
    }

    fn push_system(&mut self, _event: SystemEvent) -> Result<PushOutcome, SinkError> {
        Ok(PushOutcome::Accepted)
    }
}

/// Shared `Arc` wrapper so venues can fan-out without owning the plane.
#[derive(Debug, Clone)]
pub struct SharedViewPlane(pub std::sync::Arc<ViewPlane>);

impl SharedViewPlane {
    pub fn new(inner: std::sync::Arc<ViewPlane>) -> Self {
        Self(inner)
    }
}

impl EventSink for SharedViewPlane {
    fn push_batch(&mut self, batch: EventBatch) -> Result<PushOutcome, SinkError> {
        self.0.ingest_batch(&batch);
        Ok(PushOutcome::Accepted)
    }

    fn push_system(&mut self, _event: SystemEvent) -> Result<PushOutcome, SinkError> {
        Ok(PushOutcome::Accepted)
    }
}

fn level_json(level: &BookLevel) -> ViewLevel {
    ViewLevel {
        price: format_fixed(level.price.0),
        quantity: format_fixed(level.quantity.0),
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

#[cfg(test)]
mod tests {
    use super::*;
    use marketfeed_model::{
        ConnectionId, EventEnvelope, EventFlags, Price, Quantity, SessionId, SourceId, TimestampNs,
        Trade,
    };

    fn trade_batch(venue: VenueId, instrument: InstrumentId, price: &str, qty: &str) -> EventBatch {
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
    fn book_snapshot_from_events() {
        let plane = ViewPlane::new(ViewPlaneConfig::default());
        plane.register_venue(VenueId(1), "syn", &["BTC-USD".into()]);
        let snap = BookSnapshot {
            bids: vec![BookLevel {
                price: Price(Fixed::parse_str("100.00").unwrap()),
                quantity: Quantity(Fixed::parse_str("1.5").unwrap()),
            }],
            asks: vec![BookLevel {
                price: Price(Fixed::parse_str("101.00").unwrap()),
                quantity: Quantity(Fixed::parse_str("2.0").unwrap()),
            }],
            depth: Some(50),
            checksum: None,
        };
        let batch = EventBatch {
            session: SessionId(1),
            frame_seq: 1,
            events: vec![EventEnvelope {
                schema_version: 1,
                venue: VenueId(1),
                instrument: Some(InstrumentId(1)),
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
        };
        let mut sink = SharedViewPlane::new(std::sync::Arc::new(plane));
        sink.push_batch(batch).unwrap();
        let view = sink.0.book_snapshot("syn", InstrumentId(1), Some(1)).unwrap();
        assert_eq!(view.bids.len(), 1);
        assert_eq!(view.asks[0].price, "101.00");
        assert_eq!(view.symbol.as_deref(), Some("BTC-USD"));
    }
}
