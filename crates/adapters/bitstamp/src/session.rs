//! Bitstamp Spot SessionMachine — public trade / BBO quote / L2 / REST OHLC / REST Stats24h.
//! Candles: REST poll `GET /ohlc/{pair}/` on `CANDLE_TIMER_ID` (no public candle WS).
//! Stats24h: REST poll `GET /ticker/{pair}/` on `STATS_TIMER_ID` (no free WS 24h fields).
//! # ponytail: candle/stats poll re-emits latest bar/stats each tick (no close-only filter).

use std::collections::HashMap;

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, CandleInterval, EventBatch, HttpMethod, HttpRequestSpec,
    SessionAction, SessionInput, SessionMachine, SessionSpec, StopReason, TimerSpec,
};
use marketfeed_book::{BookSynchronizer, OrderBook, SyncLimits, SyncState};
use marketfeed_model::{
    BookChange, BookDelta, BookOperation, BookSide, BookSnapshot, Candle, CatalogView,
    ConnectionId, EventEnvelope, EventFlags, FrameStamp, InstrumentId, MarketEvent, Price,
    Quantity, Quote, SessionId, Statistics24h, SystemEvent, TimestampNs, Trade,
};

use crate::messages::{
    Decoded, candle_step_secs, decode_ohlc_rest, decode_text, decode_ticker_rest, trade_id_source,
    us_to_ts,
};
use crate::specification::{
    BITSTAMP_VENUE_ID, CANDLE_POLL_INTERVAL_MS, CANDLE_TIMER_ID, PING_INTERVAL_MS, PING_TIMER_ID,
    REST_BASE, STATS_POLL_INTERVAL_MS, STATS_TIMER_ID,
};

const SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone)]
pub struct BitstampSessionConfig {
    pub symbols: Vec<String>,
    pub instrument_ids: HashMap<String, InstrumentId>,
    pub connection: ConnectionId,
    pub session: SessionId,
    pub enable_l2: bool,
    pub candle_intervals: Vec<CandleInterval>,
    pub price_scale: u8,
    pub qty_scale: u8,
}

impl Default for BitstampSessionConfig {
    fn default() -> Self {
        let mut instrument_ids = HashMap::new();
        instrument_ids.insert("btcusd".into(), InstrumentId(1));
        Self {
            symbols: vec!["btcusd".into()],
            instrument_ids,
            connection: ConnectionId(1),
            session: SessionId(1),
            enable_l2: false,
            candle_intervals: Vec::new(),
            price_scale: 2,
            qty_scale: 8,
        }
    }
}

#[derive(Debug)]
struct SymbolBook {
    sync: BookSynchronizer,
}

#[derive(Debug)]
pub struct BitstampSession {
    #[allow(dead_code)]
    catalog: CatalogView,
    cfg: BitstampSessionConfig,
    frame_seq: u64,
    books: HashMap<String, SymbolBook>,
    live: bool,
    next_http_id: u64,
    pending_candles: HashMap<u64, (String, CandleInterval)>,
    pending_stats: HashMap<u64, String>,
}

impl BitstampSession {
    pub fn new(spec: SessionSpec, catalog: CatalogView, cfg: BitstampSessionConfig) -> Self {
        let _ = spec;
        let mut books = HashMap::new();
        // Always track book for BBO quotes; emit Book* only when enable_l2.
        for (sym, id) in &cfg.instrument_ids {
            let book = OrderBook::new(cfg.price_scale, cfg.qty_scale, None);
            let mut sync = BookSynchronizer::new(*id, book, SyncLimits::default());
            sync.request_snapshot();
            books.insert(sym.clone(), SymbolBook { sync });
        }
        Self {
            catalog,
            cfg,
            frame_seq: 0,
            books,
            live: false,
            next_http_id: 1,
            pending_candles: HashMap::new(),
            pending_stats: HashMap::new(),
        }
    }
    fn request_candle(&mut self, pair: &str, interval: CandleInterval, output: &mut ActionBuffer) {
        let id = self.next_http_id;
        self.next_http_id += 1;
        self.pending_candles
            .insert(id, (pair.to_string(), interval));
        let step = candle_step_secs(interval);
        output.push(SessionAction::RequestHttp(HttpRequestSpec {
            id,
            method: HttpMethod::Get,
            url: format!("{REST_BASE}/ohlc/{pair}/?step={step}&limit=1"),
            headers: Vec::new(),
            body: None,
        }));
    }
    fn poll_candles_all(&mut self, output: &mut ActionBuffer) {
        if self.cfg.candle_intervals.is_empty() {
            return;
        }
        let symbols = self.cfg.symbols.clone();
        let intervals = self.cfg.candle_intervals.clone();
        for pair in &symbols {
            for &interval in &intervals {
                self.request_candle(pair, interval, output);
            }
        }
    }
    fn schedule_candle_timer(&self, now: TimestampNs, output: &mut ActionBuffer) {
        if self.cfg.candle_intervals.is_empty() {
            return;
        }
        output.push(SessionAction::ScheduleTimer(TimerSpec {
            timer_id: CANDLE_TIMER_ID,
            fire_at: TimestampNs(
                now.0
                    .saturating_add(CANDLE_POLL_INTERVAL_MS.saturating_mul(1_000_000)),
            ),
        }));
    }
    fn request_stats(&mut self, pair: &str, output: &mut ActionBuffer) {
        let id = self.next_http_id;
        self.next_http_id += 1;
        self.pending_stats.insert(id, pair.to_string());
        output.push(SessionAction::RequestHttp(HttpRequestSpec {
            id,
            method: HttpMethod::Get,
            url: format!("{REST_BASE}/ticker/{pair}/"),
            headers: Vec::new(),
            body: None,
        }));
    }
    fn poll_stats_all(&mut self, output: &mut ActionBuffer) {
        let symbols = self.cfg.symbols.clone();
        for pair in &symbols {
            self.request_stats(pair, output);
        }
    }
    fn schedule_stats_timer(&self, now: TimestampNs, output: &mut ActionBuffer) {
        output.push(SessionAction::ScheduleTimer(TimerSpec {
            timer_id: STATS_TIMER_ID,
            fire_at: TimestampNs(
                now.0
                    .saturating_add(STATS_POLL_INTERVAL_MS.saturating_mul(1_000_000)),
            ),
        }));
    }
    fn emit_stats(
        &mut self,
        pair: &str,
        stats: Statistics24h,
        exchange_ts: Option<TimestampNs>,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) {
        let instrument = self.instrument_for(pair);
        let frame_seq = self.next_frame();
        let env = self.envelope(
            instrument,
            frame_seq,
            0,
            received,
            exchange_ts,
            EventFlags::empty(),
            MarketEvent::Statistics24h(stats),
        );
        output.push(SessionAction::EmitBatch(EventBatch {
            session: self.cfg.session,
            frame_seq,
            events: vec![env],
        }));
    }
    fn emit_candle(
        &mut self,
        pair: &str,
        candle: Candle,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) {
        let instrument = self.instrument_for(pair);
        let frame_seq = self.next_frame();
        let start_ts = candle.start_ts;
        let env = self.envelope(
            instrument,
            frame_seq,
            0,
            received,
            Some(start_ts),
            EventFlags::empty(),
            MarketEvent::Candle(candle),
        );
        output.push(SessionAction::EmitBatch(EventBatch {
            session: self.cfg.session,
            frame_seq,
            events: vec![env],
        }));
    }
    fn next_frame(&mut self) -> u64 {
        self.frame_seq += 1;
        self.frame_seq
    }

    fn instrument_for(&self, pair: &str) -> Option<InstrumentId> {
        self.cfg.instrument_ids.get(pair).copied()
    }

    fn envelope(
        &self,
        instrument: Option<InstrumentId>,
        frame_seq: u64,
        event_index: u16,
        received: FrameStamp,
        exchange_ts: Option<TimestampNs>,
        flags: EventFlags,
        payload: MarketEvent,
    ) -> EventEnvelope {
        EventEnvelope {
            schema_version: SCHEMA_VERSION,
            venue: BITSTAMP_VENUE_ID,
            instrument,
            connection: self.cfg.connection,
            session: self.cfg.session,
            frame_seq,
            event_index,
            exchange_ts,
            receive_ts: received.receive_ts,
            source_sequence: None,
            flags,
            payload,
        }
    }

    fn subscribe_channel(channel: &str) -> Bytes {
        Bytes::from(
            serde_json::json!({
                "event": "bts:subscribe",
                "data": { "channel": channel },
            })
            .to_string(),
        )
    }

    fn maybe_mark_live(&mut self, output: &mut ActionBuffer) {
        if self.live {
            return;
        }
        let all_live = self.books.values().all(|b| b.sync.state == SyncState::Live);
        if !all_live {
            return;
        }
        self.live = true;
        output.push(SessionAction::MarkLive);
    }

    fn bbo(&self, pair: &str) -> Option<(Price, Quantity, Price, Quantity)> {
        let book = self.books.get(pair)?;
        let (bids, asks) = book.sync.book.snapshot_levels()?;
        let bid = bids.first()?;
        let ask = asks.first()?;
        Some((bid.price, bid.quantity, ask.price, ask.quantity))
    }

    fn emit_quote_from_book(
        &mut self,
        pair: &str,
        exchange_ts_us: i64,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) {
        let Some((bid_price, bid_qty, ask_price, ask_qty)) = self.bbo(pair) else {
            return;
        };
        let instrument = self.instrument_for(pair);
        let frame_seq = self.next_frame();
        let env = self.envelope(
            instrument,
            frame_seq,
            0,
            received,
            Some(us_to_ts(exchange_ts_us)),
            EventFlags::empty(),
            MarketEvent::Quote(Quote {
                bid_price,
                ask_price,
                bid_quantity: Some(bid_qty),
                ask_quantity: Some(ask_qty),
            }),
        );
        output.push(SessionAction::EmitBatch(EventBatch {
            session: self.cfg.session,
            frame_seq,
            events: vec![env],
        }));
    }

    fn apply_snapshot(
        &mut self,
        pair: &str,
        bids: &[(Price, Quantity)],
        asks: &[(Price, Quantity)],
        exchange_ts_us: i64,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        let enable_l2 = self.cfg.enable_l2;
        let snap_levels = {
            let Some(book) = self.books.get_mut(pair) else {
                return Ok(());
            };
            let was_live = book.sync.state == SyncState::Live;
            if !was_live {
                book.sync.begin_resync();
                book.sync.request_snapshot();
            }
            if let Err(err) = book.sync.book.apply_snapshot(bids, asks, None) {
                let instrument = book.sync.instrument;
                if was_live {
                    output.push(SessionAction::EmitSystem(
                        SystemEvent::BookSnapshotRejected {
                            instrument,
                            reason: format!("replacement snapshot rejected: {err}"),
                        },
                    ));
                    return Ok(());
                }
                book.sync.invalidate(&err.to_string());
                output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                    instrument,
                    reason: err.to_string(),
                }));
                return Ok(());
            }
            book.sync.state = SyncState::Live;
            let instrument = book.sync.instrument;
            if enable_l2 {
                book.sync
                    .book
                    .snapshot_levels()
                    .map(|levels| (instrument, levels, !was_live))
            } else {
                None
            }
        };
        if let Some((instrument, (book_bids, book_asks), resynchronized)) = snap_levels {
            let frame_seq = self.next_frame();
            let env = self.envelope(
                Some(instrument),
                frame_seq,
                0,
                received,
                Some(us_to_ts(exchange_ts_us)),
                EventFlags::SNAPSHOT,
                MarketEvent::BookSnapshot(BookSnapshot {
                    bids: book_bids,
                    asks: book_asks,
                    depth: None,
                    checksum: None,
                }),
            );
            output.push(SessionAction::EmitBatch(EventBatch {
                session: self.cfg.session,
                frame_seq,
                events: vec![env],
            }));
            if resynchronized {
                output.push(SessionAction::EmitSystem(SystemEvent::BookResynchronized {
                    instrument,
                }));
            }
        }
        self.emit_quote_from_book(pair, exchange_ts_us, received, output);
        self.maybe_mark_live(output);
        Ok(())
    }

    fn apply_delta_levels(
        &mut self,
        pair: &str,
        bids: &[(Price, Quantity)],
        asks: &[(Price, Quantity)],
        exchange_ts_us: i64,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        let enable_l2 = self.cfg.enable_l2;
        let changes = {
            let Some(book) = self.books.get_mut(pair) else {
                return Ok(());
            };
            if book.sync.state != SyncState::Live {
                return Ok(());
            }
            let mut changes = Vec::new();
            for (side, levels) in [(BookSide::Bid, bids), (BookSide::Ask, asks)] {
                for &(price, quantity) in levels {
                    let (op, qty) = if quantity.0.coefficient == 0 {
                        (BookOperation::Delete, None)
                    } else {
                        (BookOperation::Upsert, Some(quantity))
                    };
                    changes.push(BookChange {
                        side,
                        operation: op,
                        price,
                        quantity: qty,
                    });
                }
            }
            if let Err(err) = book.sync.book.apply_changes_atomic(&changes) {
                let instrument = book.sync.instrument;
                book.sync.invalidate("live change apply failed");
                output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                    instrument,
                    reason: format!("live change apply failed: {err}"),
                }));
                return Ok(());
            }
            changes
        };
        if enable_l2 && !changes.is_empty() {
            let instrument = self.instrument_for(pair);
            let frame_seq = self.next_frame();
            let env = self.envelope(
                instrument,
                frame_seq,
                0,
                received,
                Some(us_to_ts(exchange_ts_us)),
                EventFlags::empty(),
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
        }
        self.emit_quote_from_book(pair, exchange_ts_us, received, output);
        Ok(())
    }

    fn handle_decoded(
        &mut self,
        decoded: Decoded,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match decoded {
            Decoded::Trade(t) => {
                let instrument = self.instrument_for(&t.pair);
                let frame_seq = self.next_frame();
                let env = self.envelope(
                    instrument,
                    frame_seq,
                    0,
                    received,
                    Some(us_to_ts(t.exchange_ts_us)),
                    EventFlags::empty(),
                    MarketEvent::Trade(Trade {
                        price: t.price,
                        quantity: t.quantity,
                        aggressor: t.aggressor,
                        trade_id: Some(trade_id_source(&t.trade_id)),
                    }),
                );
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events: vec![env],
                }));
            }
            Decoded::BookSnapshot {
                pair,
                bids,
                asks,
                exchange_ts_us,
            } => {
                self.apply_snapshot(&pair, &bids, &asks, exchange_ts_us, received, output)?;
            }
            Decoded::BookDelta {
                pair,
                bids,
                asks,
                exchange_ts_us,
            } => {
                self.apply_delta_levels(&pair, &bids, &asks, exchange_ts_us, received, output)?;
            }
            Decoded::Candle { .. } | Decoded::Statistics24h { .. } => {}
            Decoded::SubscribeAck => {
                output.push(SessionAction::EmitSystem(
                    SystemEvent::SubscriptionStateChanged {
                        state: "subscribed".into(),
                    },
                ));
            }
            Decoded::Heartbeat => {}
            Decoded::Reconnect => {
                output.push(SessionAction::Reconnect(
                    marketfeed_adapter_api::ReconnectReason::Protocol,
                ));
            }
            Decoded::Unknown => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "bitstamp".into(),
                }));
            }
        }
        Ok(())
    }
}

impl SessionMachine for BitstampSession {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Connected { now } => {
                self.live = false;
                self.pending_candles.clear();
                self.pending_stats.clear();
                for book in self.books.values_mut() {
                    book.sync.begin_resync();
                    book.sync.request_snapshot();
                }
                for sym in &self.cfg.symbols {
                    output.push(SessionAction::SendText(Self::subscribe_channel(&format!(
                        "live_trades_{sym}"
                    ))));
                    output.push(SessionAction::SendText(Self::subscribe_channel(&format!(
                        "order_book_{sym}"
                    ))));
                }
                output.push(SessionAction::ScheduleTimer(TimerSpec {
                    timer_id: PING_TIMER_ID,
                    fire_at: TimestampNs(
                        now.0
                            .saturating_add(PING_INTERVAL_MS.saturating_mul(1_000_000)),
                    ),
                }));
                self.poll_candles_all(output);
                self.schedule_candle_timer(now, output);
                self.poll_stats_all(output);
                self.schedule_stats_timer(now, output);
                output.push(SessionAction::EmitSystem(
                    SystemEvent::ConnectionStateChanged {
                        state: "connected".into(),
                    },
                ));
                Ok(())
            }
            SessionInput::Disconnected { .. } => {
                self.live = false;
                self.pending_candles.clear();
                self.pending_stats.clear();
                for book in self.books.values_mut() {
                    book.sync.invalidate("disconnected");
                }
                output.push(SessionAction::CancelTimer(PING_TIMER_ID));
                output.push(SessionAction::CancelTimer(STATS_TIMER_ID));
                if !self.cfg.candle_intervals.is_empty() {
                    output.push(SessionAction::CancelTimer(CANDLE_TIMER_ID));
                }
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
            SessionInput::BinaryFrame { .. } => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "binary".into(),
                }));
                Ok(())
            }
            SessionInput::HttpResponse {
                request_id,
                response,
                received,
            } => {
                if let Some(pair) = self.pending_stats.remove(&request_id) {
                    if response.status != 200 {
                        output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                            detail: format!("bitstamp ticker HTTP {}", response.status),
                        }));
                        return Ok(());
                    }
                    match decode_ticker_rest(&response.body) {
                        Ok(Decoded::Statistics24h {
                            open,
                            high,
                            low,
                            close,
                            volume,
                            quote_volume,
                            exchange_ts,
                        }) => {
                            self.emit_stats(
                                &pair,
                                Statistics24h {
                                    open,
                                    high,
                                    low,
                                    close,
                                    volume,
                                    quote_volume,
                                },
                                exchange_ts,
                                received,
                                output,
                            );
                        }
                        Ok(_) | Err(_) => {
                            output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                                detail: "bad bitstamp ticker body".into(),
                            }));
                        }
                    }
                    return Ok(());
                }
                let Some((pair, interval)) = self.pending_candles.remove(&request_id) else {
                    return Ok(());
                };
                if response.status != 200 {
                    output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                        detail: format!("bitstamp ohlc HTTP {}", response.status),
                    }));
                    return Ok(());
                }
                match decode_ohlc_rest(&response.body, interval) {
                    Ok(Decoded::Candle {
                        open,
                        high,
                        low,
                        close,
                        volume,
                        interval_ns,
                        start_ts,
                    }) => {
                        self.emit_candle(
                            &pair,
                            Candle {
                                open,
                                high,
                                low,
                                close,
                                volume,
                                interval_ns,
                                start_ts,
                            },
                            received,
                            output,
                        );
                    }
                    Ok(_) | Err(_) => {
                        output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                            detail: "bad bitstamp ohlc body".into(),
                        }));
                    }
                }
                Ok(())
            }
            SessionInput::Pong { .. } => Ok(()),
            SessionInput::Timer { timer_id, now } => {
                if timer_id == PING_TIMER_ID {
                    output.push(SessionAction::SendText(Bytes::from_static(
                        br#"{"event":"bts:heartbeat"}"#,
                    )));
                    output.push(SessionAction::ScheduleTimer(TimerSpec {
                        timer_id: PING_TIMER_ID,
                        fire_at: TimestampNs(
                            now.0
                                .saturating_add(PING_INTERVAL_MS.saturating_mul(1_000_000)),
                        ),
                    }));
                } else if timer_id == CANDLE_TIMER_ID {
                    self.poll_candles_all(output);
                    self.schedule_candle_timer(now, output);
                } else if timer_id == STATS_TIMER_ID {
                    self.poll_stats_all(output);
                    self.schedule_stats_timer(now, output);
                }
                Ok(())
            }
            SessionInput::Control { command } => {
                match command {
                    marketfeed_adapter_api::SessionCommand::Stop => {
                        output.push(SessionAction::StopSession(StopReason::Control));
                    }
                    marketfeed_adapter_api::SessionCommand::Resync(id) => {
                        if self.cfg.instrument_ids.values().any(|iid| iid == id) {
                            output.push(SessionAction::Reconnect(
                                marketfeed_adapter_api::ReconnectReason::Control,
                            ));
                        }
                    }
                    _ => {}
                }
                Ok(())
            }
        }
    }
}
