//! Binance Coin-M SessionMachine — trades/quote/mark/forceOrder + pair indexPrice + L2 (`pu`) + OI REST + klines.

use std::collections::HashMap;

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, CandleInterval, EventBatch, HttpMethod, HttpRequestSpec,
    ReconnectReason, SessionAction, SessionInput, SessionMachine, SessionSpec, StopReason,
    TimerSpec,
};
use marketfeed_book::{BookSynchronizer, OrderBook, SyncLimits, SyncState};
use marketfeed_model::{
    BookChange, BookDelta, BookSide, BookSnapshot, Candle, CatalogView, ConnectionId,
    EventEnvelope, EventFlags, FrameStamp, Funding, InstrumentId, Liquidation, MarketEvent,
    OpenInterest, Price, PricePoint, Quantity, Quote, SequenceRange, SessionId, Statistics24h,
    SystemEvent, TimestampNs, Trade,
};

use crate::coinm_messages::{
    CoinmDecoded, agg_id_source, coinm_index_pair, decode_text, level_op, to_book_levels,
};
use crate::coinm_specification::{BINANCE_COINM_VENUE_ID, OI_POLL_INTERVAL_MS, OI_TIMER_ID};
use crate::messages::kline_stream_interval;

const SCHEMA_VERSION: u16 = 1;
const MAX_BUFFERED_DEPTH_SPAN_NS: u64 = 5_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingHttp {
    DepthSnapshot,
    OpenInterest,
}

#[derive(Debug, Clone)]
pub struct BinanceCoinmSessionConfig {
    pub symbols: Vec<String>,
    pub instrument_ids: HashMap<String, InstrumentId>,
    pub connection: ConnectionId,
    pub session: SessionId,
    pub enable_l2: bool,
    /// Native `@kline_*` intervals. Empty = no candles.
    pub candle_intervals: Vec<CandleInterval>,
    pub price_scale: u8,
    pub qty_scale: u8,
}

impl Default for BinanceCoinmSessionConfig {
    fn default() -> Self {
        let mut instrument_ids = HashMap::new();
        instrument_ids.insert("BTCUSD_PERP".into(), InstrumentId(1));
        Self {
            symbols: vec!["BTCUSD_PERP".into()],
            instrument_ids,
            connection: ConnectionId(1),
            session: SessionId(1),
            enable_l2: false,
            candle_intervals: Vec::new(),
            price_scale: 1,
            qty_scale: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct BufferedDepthEvent {
    first_u: u64,
    final_u: u64,
    prev_u: u64,
    bids: Vec<(Price, Quantity)>,
    asks: Vec<(Price, Quantity)>,
    exchange_ts_ms: i64,
    bytes_len: usize,
    received_mono_ns: u64,
}

#[derive(Debug)]
struct SymbolBook {
    sync: BookSynchronizer,
    snapshot_req_id: Option<u64>,
    buffering: bool,
    awaiting_bridge: bool,
    depth_buffer: Vec<BufferedDepthEvent>,
    buffered_bytes: usize,
    max_buffered_events: usize,
    max_buffered_bytes: usize,
    buffered_mono_min: Option<u64>,
    buffered_mono_max: Option<u64>,
}

impl SymbolBook {
    fn push_depth(&mut self, ev: BufferedDepthEvent) -> Result<(), String> {
        let mono_min = self
            .buffered_mono_min
            .map_or(ev.received_mono_ns, |value| value.min(ev.received_mono_ns));
        let mono_max = self
            .buffered_mono_max
            .map_or(ev.received_mono_ns, |value| value.max(ev.received_mono_ns));
        if self.depth_buffer.len() >= self.max_buffered_events
            || self.buffered_bytes + ev.bytes_len > self.max_buffered_bytes
            || mono_max.saturating_sub(mono_min) > MAX_BUFFERED_DEPTH_SPAN_NS
        {
            return Err("depth buffer overflow".into());
        }
        self.buffered_bytes += ev.bytes_len;
        self.buffered_mono_min = Some(mono_min);
        self.buffered_mono_max = Some(mono_max);
        self.depth_buffer.push(ev);
        Ok(())
    }

    fn clear_depth_buffer(&mut self) {
        self.depth_buffer.clear();
        self.buffered_bytes = 0;
        self.buffered_mono_min = None;
        self.buffered_mono_max = None;
    }

    fn recompute_depth_buffer_limits(&mut self) {
        self.buffered_bytes = self.depth_buffer.iter().map(|event| event.bytes_len).sum();
        self.buffered_mono_min = self
            .depth_buffer
            .iter()
            .map(|event| event.received_mono_ns)
            .min();
        self.buffered_mono_max = self
            .depth_buffer
            .iter()
            .map(|event| event.received_mono_ns)
            .max();
    }
}

#[derive(Debug)]
pub struct BinanceCoinmSession {
    #[allow(dead_code)]
    catalog: CatalogView,
    cfg: BinanceCoinmSessionConfig,
    frame_seq: u64,
    next_http_id: u64,
    books: HashMap<String, SymbolBook>,
    /// ponytail: one pending map until typed HTTP correlation lands in adapter-api.
    pending_http: HashMap<u64, (String, PendingHttp)>,
    live: bool,
}

impl BinanceCoinmSession {
    pub fn new(spec: SessionSpec, catalog: CatalogView, cfg: BinanceCoinmSessionConfig) -> Self {
        let _ = spec;
        let mut books = HashMap::new();
        if cfg.enable_l2 {
            for (sym, id) in &cfg.instrument_ids {
                let (price_scale, qty_scale) = catalog
                    .find_by_native(sym)
                    .map(|i| (i.price_scale, i.quantity_scale))
                    .unwrap_or((cfg.price_scale, cfg.qty_scale));
                let book = OrderBook::new(price_scale, qty_scale, Some(1000));
                let mut sync = BookSynchronizer::new(*id, book, SyncLimits::default());
                sync.request_snapshot();
                books.insert(
                    sym.clone(),
                    SymbolBook {
                        sync,
                        snapshot_req_id: None,
                        buffering: true,
                        awaiting_bridge: true,
                        depth_buffer: Vec::new(),
                        buffered_bytes: 0,
                        max_buffered_events: 10_000,
                        max_buffered_bytes: 4 * 1024 * 1024,
                        buffered_mono_min: None,
                        buffered_mono_max: None,
                    },
                );
            }
        }
        Self {
            catalog,
            cfg,
            frame_seq: 0,
            next_http_id: 1,
            books,
            pending_http: HashMap::new(),
            live: false,
        }
    }

    fn next_frame(&mut self) -> u64 {
        self.frame_seq += 1;
        self.frame_seq
    }

    fn instrument_for(&self, symbol: &str) -> Option<InstrumentId> {
        self.cfg.instrument_ids.get(symbol).copied()
    }

    fn envelope(
        &self,
        instrument: Option<InstrumentId>,
        frame_seq: u64,
        event_index: u16,
        received: FrameStamp,
        exchange_ts_ms: Option<i64>,
        seq: Option<SequenceRange>,
        flags: EventFlags,
        payload: MarketEvent,
    ) -> EventEnvelope {
        EventEnvelope {
            schema_version: SCHEMA_VERSION,
            venue: BINANCE_COINM_VENUE_ID,
            instrument,
            connection: self.cfg.connection,
            session: self.cfg.session,
            frame_seq,
            event_index,
            exchange_ts: exchange_ts_ms.map(|ms| TimestampNs(ms.saturating_mul(1_000_000))),
            receive_ts: received.receive_ts,
            source_sequence: seq,
            flags,
            payload,
        }
    }

    fn emit_one(&mut self, env: EventEnvelope, output: &mut ActionBuffer) {
        let frame_seq = env.frame_seq;
        output.push(SessionAction::EmitBatch(EventBatch {
            session: self.cfg.session,
            frame_seq,
            events: vec![env],
        }));
    }

    fn alloc_http(&mut self, symbol: &str, kind: PendingHttp) -> u64 {
        let id = self.next_http_id;
        self.next_http_id += 1;
        self.pending_http.insert(id, (symbol.to_string(), kind));
        id
    }

    fn request_depth_snapshot(&mut self, symbol: &str, output: &mut ActionBuffer) {
        let Some(book) = self.books.get_mut(symbol) else {
            return;
        };
        let id = self.next_http_id;
        self.next_http_id += 1;
        book.snapshot_req_id = Some(id);
        book.buffering = true;
        book.awaiting_bridge = true;
        book.sync.begin_resync();
        book.sync.request_snapshot();
        self.pending_http
            .insert(id, (symbol.to_string(), PendingHttp::DepthSnapshot));
        output.push(SessionAction::RequestHttp(HttpRequestSpec {
            id,
            method: HttpMethod::Get,
            url: format!("https://dapi.binance.com/dapi/v1/depth?symbol={symbol}&limit=1000"),
            headers: Vec::new(),
            body: None,
        }));
    }

    fn request_open_interest(&mut self, symbol: &str, output: &mut ActionBuffer) {
        let id = self.alloc_http(symbol, PendingHttp::OpenInterest);
        output.push(SessionAction::RequestHttp(HttpRequestSpec {
            id,
            method: HttpMethod::Get,
            url: format!("https://dapi.binance.com/dapi/v1/openInterest?symbol={symbol}"),
            headers: Vec::new(),
            body: None,
        }));
    }

    fn schedule_oi_timer(&self, now: TimestampNs, output: &mut ActionBuffer) {
        output.push(SessionAction::ScheduleTimer(TimerSpec {
            timer_id: OI_TIMER_ID,
            fire_at: TimestampNs(
                now.0
                    .saturating_add(OI_POLL_INTERVAL_MS.saturating_mul(1_000_000)),
            ),
        }));
    }

    fn poll_open_interest_all(&mut self, output: &mut ActionBuffer) {
        let symbols: Vec<_> = self.cfg.symbols.clone();
        for sym in &symbols {
            self.request_open_interest(sym, output);
        }
    }

    fn handle_decoded(
        &mut self,
        decoded: CoinmDecoded,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match decoded {
            CoinmDecoded::AggTrade {
                symbol,
                agg_id,
                price,
                quantity,
                aggressor,
                exchange_ts_ms,
            } => {
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                self.emit_one(
                    self.envelope(
                        instrument,
                        frame_seq,
                        0,
                        received,
                        Some(exchange_ts_ms),
                        Some(SequenceRange {
                            first: agg_id,
                            last: agg_id,
                        }),
                        EventFlags::empty(),
                        MarketEvent::Trade(Trade {
                            price,
                            quantity,
                            aggressor,
                            trade_id: Some(agg_id_source(agg_id)),
                        }),
                    ),
                    output,
                );
            }
            CoinmDecoded::Quote {
                symbol,
                update_id,
                bid_price,
                bid_qty,
                ask_price,
                ask_qty,
            } => {
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                self.emit_one(
                    self.envelope(
                        instrument,
                        frame_seq,
                        0,
                        received,
                        None,
                        Some(SequenceRange {
                            first: update_id,
                            last: update_id,
                        }),
                        EventFlags::empty(),
                        MarketEvent::Quote(Quote {
                            bid_price,
                            bid_quantity: Some(bid_qty),
                            ask_price,
                            ask_quantity: Some(ask_qty),
                        }),
                    ),
                    output,
                );
            }
            CoinmDecoded::Ticker24h {
                symbol,
                open,
                high,
                low,
                close,
                volume,
                quote_volume,
                exchange_ts_ms,
            } => {
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                self.emit_one(
                    self.envelope(
                        instrument,
                        frame_seq,
                        0,
                        received,
                        Some(exchange_ts_ms),
                        None,
                        EventFlags::empty(),
                        MarketEvent::Statistics24h(Statistics24h {
                            open: Some(open),
                            high: Some(high),
                            low: Some(low),
                            close: Some(close),
                            volume: Some(volume),
                            quote_volume: Some(quote_volume),
                        }),
                    ),
                    output,
                );
            }
            CoinmDecoded::MarkPrice {
                symbol,
                mark,
                index,
                funding_rate,
                next_funding_ts,
                exchange_ts_ms,
            } => {
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                let make = |event_index: u16, payload: MarketEvent| EventEnvelope {
                    schema_version: SCHEMA_VERSION,
                    venue: BINANCE_COINM_VENUE_ID,
                    instrument,
                    connection: self.cfg.connection,
                    session: self.cfg.session,
                    frame_seq,
                    event_index,
                    exchange_ts: Some(TimestampNs(exchange_ts_ms.saturating_mul(1_000_000))),
                    receive_ts: received.receive_ts,
                    source_sequence: None,
                    flags: EventFlags::empty(),
                    payload,
                };
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events: vec![
                        make(0, MarketEvent::MarkPrice(PricePoint { price: mark })),
                        make(1, MarketEvent::IndexPrice(PricePoint { price: index })),
                        make(
                            2,
                            MarketEvent::Funding(Funding {
                                rate: funding_rate,
                                next_funding_ts: Some(next_funding_ts),
                            }),
                        ),
                    ],
                }));
            }
            CoinmDecoded::IndexPrice {
                pair,
                price,
                exchange_ts_ms,
            } => {
                // Pair stream fans out to every subscribed contract on that pair.
                let targets: Vec<_> = self
                    .cfg
                    .symbols
                    .iter()
                    .filter(|s| coinm_index_pair(s).eq_ignore_ascii_case(&pair))
                    .cloned()
                    .collect();
                if targets.is_empty() {
                    return Ok(());
                }
                let frame_seq = self.next_frame();
                let mut events = Vec::with_capacity(targets.len());
                for (idx, sym) in targets.into_iter().enumerate() {
                    let instrument = self.instrument_for(&sym);
                    events.push(self.envelope(
                        instrument,
                        frame_seq,
                        idx as u16,
                        received,
                        Some(exchange_ts_ms),
                        None,
                        EventFlags::empty(),
                        MarketEvent::IndexPrice(PricePoint { price }),
                    ));
                }
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events,
                }));
            }
            CoinmDecoded::Candle {
                symbol,
                open,
                high,
                low,
                close,
                volume,
                interval_ns,
                start_ts,
                exchange_ts_ms,
                is_closed: _,
            } => {
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                let env = self.envelope(
                    instrument,
                    frame_seq,
                    0,
                    received,
                    Some(exchange_ts_ms),
                    None,
                    EventFlags::empty(),
                    MarketEvent::Candle(Candle {
                        open,
                        high,
                        low,
                        close,
                        volume,
                        interval_ns,
                        start_ts,
                    }),
                );
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events: vec![env],
                }));
            }
            CoinmDecoded::DepthUpdate {
                symbol,
                first_update_id,
                final_update_id,
                prev_final_update_id,
                bids,
                asks,
                exchange_ts_ms,
            } => {
                self.on_depth_update(
                    &symbol,
                    first_update_id,
                    final_update_id,
                    prev_final_update_id,
                    bids,
                    asks,
                    exchange_ts_ms,
                    received,
                    output,
                )?;
            }
            CoinmDecoded::DepthSnapshot {
                last_update_id,
                bids,
                asks,
            } => {
                if let Some(sym) = self.cfg.symbols.first().cloned() {
                    self.apply_snapshot(&sym, last_update_id, &bids, &asks, received, output)?;
                }
            }
            CoinmDecoded::OpenInterest {
                symbol,
                quantity,
                exchange_ts_ms,
            } => {
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                self.emit_one(
                    self.envelope(
                        instrument,
                        frame_seq,
                        0,
                        received,
                        Some(exchange_ts_ms),
                        None,
                        EventFlags::empty(),
                        MarketEvent::OpenInterest(OpenInterest { quantity }),
                    ),
                    output,
                );
            }
            CoinmDecoded::ForceOrder {
                symbol,
                price,
                quantity,
                side,
                exchange_ts_ms,
            } => {
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                self.emit_one(
                    self.envelope(
                        instrument,
                        frame_seq,
                        0,
                        received,
                        Some(exchange_ts_ms),
                        None,
                        EventFlags::empty(),
                        MarketEvent::Liquidation(Liquidation {
                            price,
                            quantity,
                            side,
                        }),
                    ),
                    output,
                );
            }
            CoinmDecoded::SubscribeAck { .. } => {
                output.push(SessionAction::EmitSystem(
                    SystemEvent::SubscriptionStateChanged {
                        state: "subscribed".into(),
                    },
                ));
                if !self.cfg.enable_l2 && !self.live {
                    self.live = true;
                    output.push(SessionAction::MarkLive);
                }
            }
            CoinmDecoded::Unknown => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "binance-coinm".into(),
                }));
            }
        }
        Ok(())
    }

    fn on_depth_update(
        &mut self,
        symbol: &str,
        first_u: u64,
        final_u: u64,
        prev_u: u64,
        bids: Vec<(Price, Quantity)>,
        asks: Vec<(Price, Quantity)>,
        exchange_ts_ms: i64,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if !self.cfg.enable_l2 {
            return Ok(());
        }
        let Some(book) = self.books.get_mut(symbol) else {
            return Ok(());
        };
        let event = BufferedDepthEvent {
            first_u,
            final_u,
            prev_u,
            bytes_len: 64 + bids.len().saturating_mul(24) + asks.len().saturating_mul(24),
            bids,
            asks,
            exchange_ts_ms,
            received_mono_ns: received.mono_ns,
        };

        if book.buffering || book.sync.state != SyncState::Live {
            if let Err(reason) = book.push_depth(event) {
                let instrument = book.sync.instrument;
                book.sync.invalidate(&reason);
                book.clear_depth_buffer();
                output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                    instrument,
                    reason,
                }));
                output.push(SessionAction::ResyncInstrument(instrument));
                output.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
            }
            return Ok(());
        }

        if book.awaiting_bridge {
            let snapshot_u = book.sync.book.sequence().unwrap_or(0);
            if event.final_u < snapshot_u {
                return Ok(());
            }
            if event.first_u > snapshot_u {
                let instrument = book.sync.instrument;
                book.sync.note_gap();
                book.buffering = true;
                book.awaiting_bridge = true;
                book.clear_depth_buffer();
                output.push(SessionAction::EmitSystem(SystemEvent::SequenceGap {
                    expected: snapshot_u,
                    actual: event.first_u,
                }));
                output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                    instrument,
                    reason: "first depth event does not bridge snapshot".into(),
                }));
                output.push(SessionAction::ResyncInstrument(instrument));
                output.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
                return Ok(());
            }
            book.awaiting_bridge = false;
            return self.apply_live_depth(symbol, &event, false, received, output);
        }

        self.apply_live_depth(symbol, &event, true, received, output)
    }

    fn apply_live_depth(
        &mut self,
        symbol: &str,
        event: &BufferedDepthEvent,
        enforce_prev_u: bool,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        let Some(book) = self.books.get_mut(symbol) else {
            return Ok(());
        };
        let last_u = book.sync.book.sequence().unwrap_or(0);
        if event.final_u <= last_u {
            return Ok(()); // stale
        }
        // Futures continuity: pu must equal previous applied u (not spot's U == last+1).
        if enforce_prev_u && event.prev_u != last_u {
            let instrument = book.sync.instrument;
            book.sync.note_gap();
            book.buffering = true;
            book.clear_depth_buffer();
            output.push(SessionAction::EmitSystem(SystemEvent::SequenceGap {
                expected: last_u,
                actual: event.prev_u,
            }));
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument,
                reason: "pu discontinuity".into(),
            }));
            output.push(SessionAction::ResyncInstrument(instrument));
            output.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
            return Ok(());
        }

        let mut changes = Vec::new();
        for (side, levels) in [
            (BookSide::Bid, event.bids.as_slice()),
            (BookSide::Ask, event.asks.as_slice()),
        ] {
            for (price, qty) in levels {
                let (op, q) = level_op(*qty);
                changes.push(BookChange {
                    side,
                    operation: op,
                    price: *price,
                    quantity: q,
                });
            }
        }
        if let Err(err) = book.sync.book.apply_changes_atomic(&changes) {
            let instrument = book.sync.instrument;
            book.sync.invalidate("live delta apply failed");
            book.buffering = true;
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument,
                reason: format!("live delta apply failed: {err}"),
            }));
            output.push(SessionAction::ResyncInstrument(instrument));
            output.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
            return Ok(());
        }
        book.sync.book.set_sequence(event.final_u);
        book.sync.expected_sequence = Some(event.final_u);

        let instrument = self.instrument_for(symbol);
        let frame_seq = self.next_frame();
        self.emit_one(
            self.envelope(
                instrument,
                frame_seq,
                0,
                received,
                Some(event.exchange_ts_ms),
                Some(SequenceRange {
                    first: event.first_u,
                    last: event.final_u,
                }),
                EventFlags::DELTA,
                MarketEvent::BookDelta(BookDelta {
                    changes,
                    checksum: None,
                }),
            ),
            output,
        );
        Ok(())
    }

    /// Apply REST snapshot, then drain buffered depth with `pu` chain checks.
    fn apply_snapshot(
        &mut self,
        symbol: &str,
        last_update_id: u64,
        bids: &[(Price, Quantity)],
        asks: &[(Price, Quantity)],
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        let Some(book) = self.books.get_mut(symbol) else {
            return Ok(());
        };

        // Snapshot older than first buffered event → re-fetch.
        if let Some(first_u) = book.depth_buffer.first().map(|e| e.first_u) {
            if last_update_id < first_u {
                let sym = symbol.to_string();
                self.request_depth_snapshot(&sym, output);
                return Ok(());
            }
        }

        // Binance futures keeps the inclusive bridge event: U <= snapshot <= u.
        book.depth_buffer.retain(|e| e.final_u >= last_update_id);
        book.recompute_depth_buffer_limits();

        // First remaining: U <= lastUpdateId <= u (bridge).
        if let Some(first) = book.depth_buffer.first() {
            if !(first.first_u <= last_update_id && first.final_u >= last_update_id) {
                let instrument = book.sync.instrument;
                book.sync.invalidate("snapshot/buffer bridge failed");
                book.clear_depth_buffer();
                book.buffering = true;
                output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                    instrument,
                    reason: "snapshot does not bridge buffered depth".into(),
                }));
                output.push(SessionAction::ResyncInstrument(instrument));
                output.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
                return Ok(());
            }
        }

        book.sync.begin_resync();
        book.sync.request_snapshot();
        if let Err(err) = book
            .sync
            .book
            .apply_snapshot(bids, asks, Some(last_update_id))
        {
            let instrument = book.sync.instrument;
            book.sync.invalidate(&err.to_string());
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument,
                reason: err.to_string(),
            }));
            output.push(SessionAction::ResyncInstrument(instrument));
            return Ok(());
        }
        book.sync.state = SyncState::Live;
        book.sync.expected_sequence = Some(last_update_id);

        let instrument = book.sync.instrument;
        let pending = std::mem::take(&mut book.depth_buffer);
        book.buffered_bytes = 0;
        book.buffered_mono_min = None;
        book.buffered_mono_max = None;
        book.buffering = false;
        book.awaiting_bridge = pending.is_empty();
        book.snapshot_req_id = None;

        let levels_b = to_book_levels(BookSide::Bid, bids);
        let levels_a = to_book_levels(BookSide::Ask, asks);
        let frame_seq = self.next_frame();
        self.emit_one(
            self.envelope(
                Some(instrument),
                frame_seq,
                0,
                received,
                None,
                Some(SequenceRange {
                    first: last_update_id,
                    last: last_update_id,
                }),
                EventFlags::SNAPSHOT,
                MarketEvent::BookSnapshot(BookSnapshot {
                    bids: levels_b,
                    asks: levels_a,
                    depth: Some(1000),
                    checksum: None,
                }),
            ),
            output,
        );
        output.push(SessionAction::EmitSystem(SystemEvent::BookResynchronized {
            instrument,
        }));

        for (index, ev) in pending.into_iter().enumerate() {
            self.apply_live_depth(symbol, &ev, index != 0, received, output)?;
            if self
                .books
                .get(symbol)
                .map(|b| b.sync.state != SyncState::Live)
                .unwrap_or(true)
            {
                return Ok(());
            }
        }

        self.live = true;
        output.push(SessionAction::MarkLive);
        Ok(())
    }

    fn maybe_mark_all_live(&mut self, output: &mut ActionBuffer) {
        if self.cfg.enable_l2 {
            return;
        }
        if !self.live {
            self.live = true;
            output.push(SessionAction::MarkLive);
        }
    }
}

impl SessionMachine for BinanceCoinmSession {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Connected { now } => {
                self.live = false;
                let mut params = Vec::new();
                for s in &self.cfg.symbols {
                    let lower = s.to_ascii_lowercase();
                    params.push(format!("{lower}@aggTrade"));
                    params.push(format!("{lower}@bookTicker"));
                    params.push(format!("{lower}@ticker"));
                    params.push(format!("{lower}@markPrice@1s"));
                    let pair = coinm_index_pair(s).to_ascii_lowercase();
                    params.push(format!("{pair}@indexPrice@1s"));
                    params.push(format!("{lower}@forceOrder"));
                    if self.cfg.enable_l2 {
                        params.push(format!("{lower}@depth@100ms"));
                    }
                    for interval in &self.cfg.candle_intervals {
                        let suffix = kline_stream_interval(*interval);
                        params.push(format!("{lower}@kline_{suffix}"));
                    }
                }
                let body = serde_json::json!({
                    "method": "SUBSCRIBE",
                    "params": params,
                    "id": 1
                });
                output.push(SessionAction::SendText(Bytes::from(body.to_string())));
                output.push(SessionAction::EmitSystem(
                    SystemEvent::ConnectionStateChanged {
                        state: "connected".into(),
                    },
                ));

                self.poll_open_interest_all(output);
                self.schedule_oi_timer(now, output);

                let symbols: Vec<_> = self.cfg.symbols.clone();
                if self.cfg.enable_l2 {
                    for sym in symbols {
                        if let Some(book) = self.books.get_mut(&sym) {
                            book.clear_depth_buffer();
                            book.buffering = true;
                        }
                        self.request_depth_snapshot(&sym, output);
                    }
                } else {
                    self.maybe_mark_all_live(output);
                }
                Ok(())
            }
            SessionInput::Disconnected { .. } => {
                self.live = false;
                for book in self.books.values_mut() {
                    book.sync.invalidate("disconnected");
                    book.buffering = true;
                    book.clear_depth_buffer();
                }
                self.pending_http.clear();
                output.push(SessionAction::CancelTimer(OI_TIMER_ID));
                output.push(SessionAction::EmitSystem(
                    SystemEvent::ConnectionStateChanged {
                        state: "disconnected".into(),
                    },
                ));
                Ok(())
            }
            SessionInput::TextFrame { bytes, received } => match decode_text(bytes) {
                Ok(decoded) => self.handle_decoded(decoded, received, output),
                Err(e) => {
                    output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                        detail: e,
                    }));
                    Ok(())
                }
            },
            SessionInput::HttpResponse {
                request_id,
                response,
                received,
            } => {
                let Some((symbol, kind)) = self.pending_http.remove(&request_id) else {
                    return Ok(());
                };
                if response.status != 200 {
                    output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                        detail: format!("coinm HTTP {} for {:?}", response.status, kind),
                    }));
                    if kind == PendingHttp::DepthSnapshot {
                        output.push(SessionAction::Reconnect(ReconnectReason::Protocol));
                    }
                    return Ok(());
                }
                match kind {
                    PendingHttp::DepthSnapshot => match decode_text(&response.body) {
                        Ok(CoinmDecoded::DepthSnapshot {
                            last_update_id,
                            bids,
                            asks,
                        }) => {
                            if let Some(book) = self.books.get_mut(&symbol) {
                                book.snapshot_req_id = None;
                            }
                            self.apply_snapshot(
                                &symbol,
                                last_update_id,
                                &bids,
                                &asks,
                                received,
                                output,
                            )
                        }
                        Ok(_) | Err(_) => {
                            output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                                detail: "bad coinm depth snapshot body".into(),
                            }));
                            Ok(())
                        }
                    },
                    PendingHttp::OpenInterest => match decode_text(&response.body) {
                        Ok(decoded @ CoinmDecoded::OpenInterest { .. }) => {
                            self.handle_decoded(decoded, received, output)
                        }
                        Ok(_) | Err(_) => {
                            output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                                detail: "bad coinm openInterest body".into(),
                            }));
                            Ok(())
                        }
                    },
                }
            }
            SessionInput::BinaryFrame { .. } => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "binary".into(),
                }));
                Ok(())
            }
            SessionInput::Pong { .. } => Ok(()),
            SessionInput::Timer { timer_id, now } => {
                if timer_id == OI_TIMER_ID {
                    self.poll_open_interest_all(output);
                    self.schedule_oi_timer(now, output);
                }
                Ok(())
            }
            SessionInput::Control { command } => {
                match command {
                    marketfeed_adapter_api::SessionCommand::Stop => {
                        output.push(SessionAction::StopSession(StopReason::Control));
                    }
                    marketfeed_adapter_api::SessionCommand::Resync(id) => {
                        if let Some((sym, _)) =
                            self.cfg.instrument_ids.iter().find(|(_, iid)| **iid == *id)
                        {
                            let sym = sym.clone();
                            if let Some(book) = self.books.get_mut(&sym) {
                                book.clear_depth_buffer();
                                book.buffering = true;
                            }
                            if self.cfg.enable_l2 {
                                self.request_depth_snapshot(&sym, output);
                            }
                        }
                    }
                    _ => {}
                }
                Ok(())
            }
        }
    }
}
