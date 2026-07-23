//! Binance Spot SessionMachine with official L2 snapshot/buffer rules.

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
    EventEnvelope, EventFlags, FrameStamp, InstrumentId, MarketEvent, Price, Quantity, Quote,
    SequenceRange, SessionId, Statistics24h, SystemEvent, TimestampNs, Trade,
};

use crate::messages::{
    DecodedEvent, decode_text, kline_stream_interval, level_op, to_book_levels, trade_id_source,
};
use crate::specification::{BINANCE_SPOT_VENUE_ID, HEARTBEAT_TIMEOUT_MS, HEARTBEAT_TIMER_ID};

const SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone)]
pub struct BinanceSessionConfig {
    pub symbols: Vec<String>,
    pub instrument_ids: HashMap<String, InstrumentId>,
    pub connection: ConnectionId,
    pub session: SessionId,
    pub enable_l2: bool,
    /// Native kline intervals to subscribe (`@kline_*`). Empty = no candles.
    pub candle_intervals: Vec<CandleInterval>,
    pub price_scale: u8,
    pub qty_scale: u8,
    /// Pre-snapshot `@depth` buffer bound (events). Override in tests for overflow proofs.
    pub max_buffered_depth_events: usize,
    /// Pre-snapshot `@depth` buffer bound (approx payload bytes).
    pub max_buffered_depth_bytes: usize,
    /// Pre-snapshot `@depth` buffer bound (monotonic receive-time span).
    pub max_buffered_depth_span_ns: u64,
}

impl Default for BinanceSessionConfig {
    fn default() -> Self {
        let mut instrument_ids = HashMap::new();
        instrument_ids.insert("BTCUSDT".into(), InstrumentId(1));
        Self {
            symbols: vec!["BTCUSDT".into()],
            instrument_ids,
            connection: ConnectionId(1),
            session: SessionId(1),
            enable_l2: false,
            candle_intervals: Vec::new(),
            price_scale: 2,
            qty_scale: 8,
            max_buffered_depth_events: 10_000,
            max_buffered_depth_bytes: 4 * 1024 * 1024,
            max_buffered_depth_span_ns: 5_000_000_000,
        }
    }
}

/// One buffered `@depth` event (Binance range `[U,u]`).
#[derive(Debug, Clone)]
struct BufferedDepthEvent {
    first_u: u64,
    final_u: u64,
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
    /// True until snapshot applied and buffered events drained.
    buffering: bool,
    depth_buffer: Vec<BufferedDepthEvent>,
    buffered_bytes: usize,
    max_buffered_events: usize,
    max_buffered_bytes: usize,
    max_buffered_span_ns: u64,
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
            || mono_max.saturating_sub(mono_min) > self.max_buffered_span_ns
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
pub struct BinanceSpotSession {
    #[allow(dead_code)]
    catalog: CatalogView,
    cfg: BinanceSessionConfig,
    frame_seq: u64,
    next_http_id: u64,
    books: HashMap<String, SymbolBook>,
    live: bool,
}

impl BinanceSpotSession {
    pub fn new(spec: SessionSpec, catalog: CatalogView, cfg: BinanceSessionConfig) -> Self {
        let _ = spec;
        let mut books = HashMap::new();
        if cfg.enable_l2 {
            for (sym, id) in &cfg.instrument_ids {
                // Catalog scales win when present; cfg is fallback for fixture-only sessions.
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
                        depth_buffer: Vec::new(),
                        buffered_bytes: 0,
                        max_buffered_events: cfg.max_buffered_depth_events,
                        max_buffered_bytes: cfg.max_buffered_depth_bytes,
                        max_buffered_span_ns: cfg.max_buffered_depth_span_ns,
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
            live: false,
        }
    }

    fn schedule_heartbeat(&self, now: TimestampNs, output: &mut ActionBuffer) {
        output.push(SessionAction::ScheduleTimer(TimerSpec {
            timer_id: HEARTBEAT_TIMER_ID,
            fire_at: TimestampNs(
                now.0
                    .saturating_add(HEARTBEAT_TIMEOUT_MS.saturating_mul(1_000_000)),
            ),
        }));
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
            venue: BINANCE_SPOT_VENUE_ID,
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

    fn request_depth_snapshot(&mut self, symbol: &str, output: &mut ActionBuffer) {
        let Some(book) = self.books.get_mut(symbol) else {
            return;
        };
        let id = self.next_http_id;
        self.next_http_id += 1;
        book.snapshot_req_id = Some(id);
        book.buffering = true;
        // Keep depth_buffer across snapshot retries (Binance step 4: re-fetch if stale).
        book.sync.begin_resync();
        // begin_resync clears book state; restore SnapshotRequested without wiping our depth_buffer.
        book.sync.request_snapshot();
        output.push(SessionAction::RequestHttp(HttpRequestSpec {
            id,
            method: HttpMethod::Get,
            url: format!("https://api.binance.com/api/v3/depth?symbol={symbol}&limit=1000"),
            headers: Vec::new(),
            body: None,
        }));
    }

    fn handle_decoded(
        &mut self,
        decoded: DecodedEvent,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match decoded {
            DecodedEvent::Trade {
                symbol,
                trade_id,
                price,
                quantity,
                aggressor,
                exchange_ts_ms,
            } => {
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                let env = self.envelope(
                    instrument,
                    frame_seq,
                    0,
                    received,
                    Some(exchange_ts_ms),
                    Some(SequenceRange {
                        first: trade_id,
                        last: trade_id,
                    }),
                    EventFlags::empty(),
                    MarketEvent::Trade(Trade {
                        price,
                        quantity,
                        aggressor,
                        trade_id: Some(trade_id_source(trade_id)),
                    }),
                );
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events: vec![env],
                }));
            }
            DecodedEvent::Quote {
                symbol,
                update_id,
                bid_price,
                bid_qty,
                ask_price,
                ask_qty,
            } => {
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                let env = self.envelope(
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
                );
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events: vec![env],
                }));
            }
            DecodedEvent::Ticker24h {
                symbol,
                bid_price,
                bid_qty,
                ask_price,
                ask_qty,
                open,
                high,
                low,
                close,
                volume,
                quote_volume,
                exchange_ts_ms,
            } => {
                // ponytail: keep `@bookTicker` as sequenced BBO; `@ticker` adds Stats24h (+ secondary Quote).
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                let quote = self.envelope(
                    instrument,
                    frame_seq,
                    0,
                    received,
                    Some(exchange_ts_ms),
                    None,
                    EventFlags::empty(),
                    MarketEvent::Quote(Quote {
                        bid_price,
                        bid_quantity: Some(bid_qty),
                        ask_price,
                        ask_quantity: Some(ask_qty),
                    }),
                );
                let stats = self.envelope(
                    instrument,
                    frame_seq,
                    1,
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
                );
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events: vec![quote, stats],
                }));
            }
            DecodedEvent::Candle {
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
            DecodedEvent::DepthUpdate {
                symbol,
                first_update_id,
                final_update_id,
                bids,
                asks,
                exchange_ts_ms,
            } => {
                self.on_depth_update(
                    &symbol,
                    first_update_id,
                    final_update_id,
                    bids,
                    asks,
                    exchange_ts_ms,
                    received,
                    output,
                )?;
            }
            DecodedEvent::DepthSnapshot {
                last_update_id,
                bids,
                asks,
            } => {
                if let Some(sym) = self.cfg.symbols.first().cloned() {
                    self.apply_snapshot(&sym, last_update_id, &bids, &asks, received, output)?;
                }
            }
            DecodedEvent::SubscribeAck { .. } => {
                output.push(SessionAction::EmitSystem(
                    SystemEvent::SubscriptionStateChanged {
                        state: "subscribed".into(),
                    },
                ));
                if !self.cfg.enable_l2 {
                    self.live = true;
                    output.push(SessionAction::MarkLive);
                }
            }
            DecodedEvent::Unknown => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "binance".into(),
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

        if book.buffering || book.sync.state != SyncState::Live {
            let bytes_len = 64 + bids.len().saturating_mul(24) + asks.len().saturating_mul(24);
            if let Err(reason) = book.push_depth(BufferedDepthEvent {
                first_u,
                final_u,
                bids,
                asks,
                exchange_ts_ms,
                bytes_len,
                received_mono_ns: received.mono_ns,
            }) {
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

        self.apply_live_depth(
            symbol,
            first_u,
            final_u,
            &bids,
            &asks,
            exchange_ts_ms,
            received,
            output,
        )
    }

    fn apply_live_depth(
        &mut self,
        symbol: &str,
        first_u: u64,
        final_u: u64,
        bids: &[(Price, Quantity)],
        asks: &[(Price, Quantity)],
        exchange_ts_ms: i64,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        let Some(book) = self.books.get_mut(symbol) else {
            return Ok(());
        };
        // Local book update id is last applied `u`; expected next first is last_u+1.
        let last_u = book.sync.book.sequence().unwrap_or(0);
        if final_u <= last_u {
            return Ok(()); // stale
        }
        if first_u > last_u + 1 {
            let instrument = book.sync.instrument;
            book.sync.note_gap();
            book.buffering = true;
            book.clear_depth_buffer();
            output.push(SessionAction::EmitSystem(SystemEvent::SequenceGap {
                expected: last_u + 1,
                actual: first_u,
            }));
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument,
                reason: "depth gap".into(),
            }));
            output.push(SessionAction::ResyncInstrument(instrument));
            output.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
            return Ok(());
        }

        let mut changes = Vec::new();
        for (side, levels) in [(BookSide::Bid, bids), (BookSide::Ask, asks)] {
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
        book.sync.book.set_sequence(final_u);
        book.sync.expected_sequence = Some(final_u + 1);

        let instrument = self.instrument_for(symbol);
        let frame_seq = self.next_frame();
        let env = self.envelope(
            instrument,
            frame_seq,
            0,
            received,
            Some(exchange_ts_ms),
            Some(SequenceRange {
                first: first_u,
                last: final_u,
            }),
            EventFlags::DELTA,
            MarketEvent::BookDelta(BookDelta {
                changes,
                checksum: None,
            }),
        );
        output.push(SessionAction::EmitBatch(EventBatch {
            session: self.cfg.session,
            frame_seq,
            events: vec![env],
        }));
        Ok(())
    }

    /// Apply REST snapshot, then drain buffered depth events per Binance rules.
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

        // Step 4: snapshot too old vs first buffered event → re-request.
        if let Some(first_u) = book.depth_buffer.first().map(|e| e.first_u) {
            if last_update_id < first_u {
                let sym = symbol.to_string();
                self.request_depth_snapshot(&sym, output);
                return Ok(());
            }
        }

        // Step 5: discard u <= lastUpdateId.
        book.depth_buffer.retain(|e| e.final_u > last_update_id);
        book.recompute_depth_buffer_limits();

        // First remaining should have lastUpdateId within [U, u].
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

        // Apply snapshot atomically.
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
        book.sync.expected_sequence = Some(last_update_id + 1);

        let instrument = book.sync.instrument;
        let pending = std::mem::take(&mut book.depth_buffer);
        book.buffered_bytes = 0;
        book.buffered_mono_min = None;
        book.buffered_mono_max = None;
        book.buffering = false;
        book.snapshot_req_id = None;

        let levels_b = to_book_levels(BookSide::Bid, bids);
        let levels_a = to_book_levels(BookSide::Ask, asks);
        let frame_seq = self.next_frame();
        let env = self.envelope(
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
        );
        output.push(SessionAction::EmitBatch(EventBatch {
            session: self.cfg.session,
            frame_seq,
            events: vec![env],
        }));
        output.push(SessionAction::EmitSystem(SystemEvent::BookResynchronized {
            instrument,
        }));

        // End borrow of `book` before draining via &mut self.
        let _ = &pending;

        // Drain buffered events in order (Binance steps 6–7).
        for ev in pending {
            self.apply_live_depth(
                symbol,
                ev.first_u,
                ev.final_u,
                &ev.bids,
                &ev.asks,
                ev.exchange_ts_ms,
                received,
                output,
            )?;
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
}

impl SessionMachine for BinanceSpotSession {
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
                    params.push(format!("{lower}@trade"));
                    params.push(format!("{lower}@bookTicker"));
                    params.push(format!("{lower}@ticker"));
                    if self.cfg.enable_l2 {
                        params.push(format!("{lower}@depth@100ms"));
                    }
                    for interval in &self.cfg.candle_intervals {
                        params.push(format!(
                            "{lower}@kline_{}",
                            kline_stream_interval(*interval)
                        ));
                    }
                }
                let body = serde_json::json!({
                    "method": "SUBSCRIBE",
                    "params": params,
                    "id": 1
                });
                output.push(SessionAction::SendText(Bytes::from(body.to_string())));
                self.schedule_heartbeat(now, output);
                output.push(SessionAction::EmitSystem(
                    SystemEvent::ConnectionStateChanged {
                        state: "connected".into(),
                    },
                ));
                if self.cfg.enable_l2 {
                    let symbols: Vec<_> = self.cfg.symbols.clone();
                    for sym in symbols {
                        if let Some(book) = self.books.get_mut(&sym) {
                            book.clear_depth_buffer();
                            book.buffering = true;
                        }
                        self.request_depth_snapshot(&sym, output);
                    }
                } else {
                    output.push(SessionAction::MarkLive);
                    self.live = true;
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
                output.push(SessionAction::CancelTimer(HEARTBEAT_TIMER_ID));
                output.push(SessionAction::EmitSystem(
                    SystemEvent::ConnectionStateChanged {
                        state: "disconnected".into(),
                    },
                ));
                Ok(())
            }
            SessionInput::TextFrame { bytes, received } => {
                self.schedule_heartbeat(received.receive_ts, output);
                match decode_text(bytes) {
                    Ok(decoded) => self.handle_decoded(decoded, received, output),
                    Err(e) => {
                        output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                            detail: e,
                        }));
                        Ok(())
                    }
                }
            }
            SessionInput::HttpResponse {
                request_id,
                response,
                received,
            } => {
                let symbol = self
                    .books
                    .iter()
                    .find(|(_, b)| b.snapshot_req_id == Some(request_id))
                    .map(|(s, _)| s.clone());
                let Some(symbol) = symbol else {
                    return Ok(());
                };
                if response.status != 200 {
                    output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                        detail: format!("depth snapshot HTTP {}", response.status),
                    }));
                    output.push(SessionAction::Reconnect(ReconnectReason::Protocol));
                    return Ok(());
                }
                match decode_text(&response.body) {
                    Ok(DecodedEvent::DepthSnapshot {
                        last_update_id,
                        bids,
                        asks,
                    }) => {
                        self.apply_snapshot(&symbol, last_update_id, &bids, &asks, received, output)
                    }
                    Ok(_) | Err(_) => {
                        output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                            detail: "bad depth snapshot body".into(),
                        }));
                        Ok(())
                    }
                }
            }
            SessionInput::BinaryFrame { .. } => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "binary".into(),
                }));
                Ok(())
            }
            SessionInput::Pong { .. } => Ok(()),
            SessionInput::Timer { timer_id, .. } => {
                if timer_id == HEARTBEAT_TIMER_ID {
                    output.push(SessionAction::EmitSystem(SystemEvent::HeartbeatMissed));
                    output.push(SessionAction::Reconnect(ReconnectReason::Heartbeat));
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
                            self.request_depth_snapshot(&sym, output);
                        }
                    }
                    _ => {}
                }
                Ok(())
            }
        }
    }
}
