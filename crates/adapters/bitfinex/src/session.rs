//! Bitfinex Spot SessionMachine — public trades / ticker quotes / book L2 / WS candles.
//!
//! Candles: WS `candles` with key `trade:{tf}:{symbol}` (no REST poll).
//! Ticker LAST/VOLUME/HIGH/LOW → `Statistics24h` when non-zero.
//! Derivatives: REST `status/deriv` mark/index/funding/OI; WS `status`/`liq:global` liquidations.
//!
//! # ponytail
//! Channel-ID map is connection-scoped (cleared on disconnect). Ceiling: 30
//! public channels / connection (venue limit); upgrade = multi-session plan.

use std::collections::HashMap;

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, CandleInterval, EventBatch, HttpMethod, HttpRequestSpec,
    SessionAction, SessionInput, SessionMachine, SessionSpec, StopReason, TimerSpec,
};
use marketfeed_book::{BookSynchronizer, OrderBook, SyncLimits, SyncState};
use marketfeed_model::{
    BookChange, BookDelta, BookOperation, BookSide, BookSnapshot, Candle, CatalogView,
    ConnectionId, EventEnvelope, EventFlags, FrameStamp, Funding, InstrumentId, Liquidation,
    MarketEvent, OpenInterest, PricePoint, Quantity, Quote, SessionId, Statistics24h, SystemEvent,
    TimestampNs, Trade, VenueId,
};

use crate::messages::{
    BookEntry, ChanBinding, Decoded, LiquidationRow, TradeRow, candle_time_frame,
    decode_status_deriv, decode_text, ms_to_ts, trade_id_source,
};
use crate::specification::{
    BITFINEX_VENUE_ID, PING_INTERVAL_MS, PING_TIMER_ID, REST_BASE, STATUS_POLL_INTERVAL_MS,
    STATUS_TIMER_ID,
};

const SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone)]
pub struct BitfinexSessionConfig {
    pub venue: VenueId,
    pub symbols: Vec<String>,
    pub instrument_ids: HashMap<String, InstrumentId>,
    pub connection: ConnectionId,
    pub session: SessionId,
    pub enable_l2: bool,
    pub candle_intervals: Vec<CandleInterval>,
    pub poll_deriv_status: bool,
    pub price_scale: u8,
    pub qty_scale: u8,
}

impl Default for BitfinexSessionConfig {
    fn default() -> Self {
        let mut instrument_ids = HashMap::new();
        instrument_ids.insert("tBTCUSD".into(), InstrumentId(1));
        Self {
            venue: BITFINEX_VENUE_ID,
            symbols: vec!["tBTCUSD".into()],
            instrument_ids,
            connection: ConnectionId(1),
            session: SessionId(1),
            enable_l2: false,
            candle_intervals: Vec::new(),
            poll_deriv_status: false,
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
pub struct BitfinexSession {
    #[allow(dead_code)]
    catalog: CatalogView,
    cfg: BitfinexSessionConfig,
    frame_seq: u64,
    books: HashMap<String, SymbolBook>,
    channels: HashMap<u32, ChanBinding>,
    live: bool,
    saw_ticker: bool,
    next_http_id: u64,
    pending_status: HashMap<u64, ()>,
}

impl BitfinexSession {
    pub fn new(spec: SessionSpec, catalog: CatalogView, cfg: BitfinexSessionConfig) -> Self {
        let _ = spec;
        let mut books = HashMap::new();
        if cfg.enable_l2 {
            for (sym, id) in &cfg.instrument_ids {
                let book = OrderBook::new(cfg.price_scale, cfg.qty_scale, None);
                let mut sync = BookSynchronizer::new(*id, book, SyncLimits::default());
                sync.request_snapshot();
                books.insert(sym.clone(), SymbolBook { sync });
            }
        }
        Self {
            catalog,
            cfg,
            frame_seq: 0,
            books,
            channels: HashMap::new(),
            live: false,
            saw_ticker: false,
            next_http_id: 1,
            pending_status: HashMap::new(),
        }
    }

    fn request_deriv_status(&mut self, output: &mut ActionBuffer) {
        if !self.cfg.poll_deriv_status || self.cfg.symbols.is_empty() {
            return;
        }
        let id = self.next_http_id;
        self.next_http_id += 1;
        self.pending_status.insert(id, ());
        let keys = self.cfg.symbols.join(",");
        output.push(SessionAction::RequestHttp(HttpRequestSpec {
            id,
            method: HttpMethod::Get,
            url: format!("{REST_BASE}/status/deriv?keys={keys}"),
            headers: Vec::new(),
            body: None,
        }));
    }

    fn schedule_status_timer(&self, now: TimestampNs, output: &mut ActionBuffer) {
        if !self.cfg.poll_deriv_status {
            return;
        }
        output.push(SessionAction::ScheduleTimer(TimerSpec {
            timer_id: STATUS_TIMER_ID,
            fire_at: TimestampNs(
                now.0
                    .saturating_add(STATUS_POLL_INTERVAL_MS.saturating_mul(1_000_000)),
            ),
        }));
    }

    fn emit_deriv_status(
        &mut self,
        row: crate::messages::DerivStatusRow,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) {
        if !self.cfg.instrument_ids.contains_key(&row.symbol) {
            return;
        }
        let instrument = self.instrument_for(&row.symbol);
        for payload in [
            MarketEvent::MarkPrice(PricePoint {
                price: row.mark_price,
            }),
            MarketEvent::IndexPrice(PricePoint {
                price: row.index_price,
            }),
            MarketEvent::Funding(Funding {
                rate: row.funding_rate,
                next_funding_ts: row.next_funding_ts,
            }),
            MarketEvent::OpenInterest(OpenInterest {
                quantity: row.open_interest,
            }),
        ] {
            let frame_seq = self.next_frame();
            let env = self.envelope(
                instrument,
                frame_seq,
                0,
                received,
                None,
                EventFlags::empty(),
                payload,
            );
            output.push(SessionAction::EmitBatch(EventBatch {
                session: self.cfg.session,
                frame_seq,
                events: vec![env],
            }));
        }
    }

    fn emit_candle(
        &mut self,
        symbol: &str,
        candle: Candle,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) {
        let instrument = self.instrument_for(symbol);
        let frame_seq = self.next_frame();
        let env = self.envelope(
            instrument,
            frame_seq,
            0,
            received,
            Some(candle.start_ts),
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

    fn instrument_for(&self, symbol: &str) -> Option<InstrumentId> {
        self.cfg.instrument_ids.get(symbol).copied()
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
            venue: self.cfg.venue,
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

    fn subscribe_json(channel: &str, symbol: &str) -> Bytes {
        let mut obj = serde_json::json!({
            "event": "subscribe",
            "channel": channel,
            "symbol": symbol,
        });
        if channel == "book" {
            obj["prec"] = serde_json::json!("P0");
            obj["freq"] = serde_json::json!("F0");
            obj["len"] = serde_json::json!("25");
        }
        Bytes::from(obj.to_string())
    }

    fn subscribe_candles(key: &str) -> Bytes {
        Bytes::from(
            serde_json::json!({
                "event": "subscribe",
                "channel": "candles",
                "key": key,
            })
            .to_string(),
        )
    }

    fn subscribe_status(key: &str) -> Bytes {
        Bytes::from(
            serde_json::json!({
                "event": "subscribe",
                "channel": "status",
                "key": key,
            })
            .to_string(),
        )
    }

    fn emit_liquidation(
        &mut self,
        row: LiquidationRow,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) {
        // Global feed — only emit for symbols this session subscribed.
        if !self.cfg.instrument_ids.contains_key(&row.symbol) {
            return;
        }
        let instrument = self.instrument_for(&row.symbol);
        let frame_seq = self.next_frame();
        let env = self.envelope(
            instrument,
            frame_seq,
            0,
            received,
            Some(ms_to_ts(row.exchange_ts_ms)),
            EventFlags::empty(),
            MarketEvent::Liquidation(Liquidation {
                price: row.price,
                quantity: row.quantity,
                side: row.side,
            }),
        );
        output.push(SessionAction::EmitBatch(EventBatch {
            session: self.cfg.session,
            frame_seq,
            events: vec![env],
        }));
    }

    fn maybe_mark_live(&mut self, output: &mut ActionBuffer) {
        if self.live {
            return;
        }
        let ready = if self.cfg.enable_l2 {
            !self.books.is_empty() && self.books.values().all(|b| b.sync.state == SyncState::Live)
        } else {
            self.saw_ticker
        };
        if ready {
            self.live = true;
            output.push(SessionAction::MarkLive);
        }
    }

    fn emit_trade(&mut self, t: TradeRow, received: FrameStamp, output: &mut ActionBuffer) {
        let instrument = self.instrument_for(&t.symbol);
        let frame_seq = self.next_frame();
        let env = self.envelope(
            instrument,
            frame_seq,
            0,
            received,
            Some(ms_to_ts(t.exchange_ts_ms)),
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

    fn emit_quote_and_stats(
        &mut self,
        q: crate::messages::TickerRow,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) {
        let instrument = self.instrument_for(&q.symbol);
        let frame_seq = self.next_frame();
        let mut events = vec![self.envelope(
            instrument,
            frame_seq,
            0,
            received,
            None,
            EventFlags::empty(),
            MarketEvent::Quote(Quote {
                bid_price: q.bid_price,
                ask_price: q.ask_price,
                bid_quantity: Some(q.bid_qty),
                ask_quantity: Some(q.ask_qty),
            }),
        )];
        if q.has_stats24h() {
            events.push(self.envelope(
                instrument,
                frame_seq,
                1,
                received,
                None,
                EventFlags::empty(),
                MarketEvent::Statistics24h(Statistics24h {
                    open: None,
                    high: q.high,
                    low: q.low,
                    close: q.last,
                    volume: q.volume,
                    quote_volume: None,
                }),
            ));
        }
        output.push(SessionAction::EmitBatch(EventBatch {
            session: self.cfg.session,
            frame_seq,
            events,
        }));
        self.saw_ticker = true;
        self.maybe_mark_live(output);
    }

    fn apply_book_snapshot(
        &mut self,
        symbol: &str,
        entries: &[BookEntry],
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        let enable_l2 = self.cfg.enable_l2;
        let snap_levels = {
            let Some(book) = self.books.get_mut(symbol) else {
                return Ok(());
            };
            book.sync.begin_resync();
            book.sync.request_snapshot();
            let mut bids = Vec::new();
            let mut asks = Vec::new();
            for e in entries {
                let qty = Quantity(abs_amount(e.amount));
                match e.amount.coefficient.cmp(&0) {
                    std::cmp::Ordering::Greater => bids.push((e.price, qty)),
                    std::cmp::Ordering::Less => asks.push((e.price, qty)),
                    std::cmp::Ordering::Equal => {}
                }
            }
            if let Err(err) = book.sync.book.apply_snapshot(&bids, &asks, None) {
                let instrument = book.sync.instrument;
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
                    .map(|levels| (instrument, levels))
            } else {
                None
            }
        };
        if let Some((instrument, (book_bids, book_asks))) = snap_levels {
            let frame_seq = self.next_frame();
            let env = self.envelope(
                Some(instrument),
                frame_seq,
                0,
                received,
                None,
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
            output.push(SessionAction::EmitSystem(SystemEvent::BookResynchronized {
                instrument,
            }));
        }
        self.maybe_mark_live(output);
        Ok(())
    }

    fn apply_book_update(
        &mut self,
        symbol: &str,
        entry: &BookEntry,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        let enable_l2 = self.cfg.enable_l2;
        let change = {
            let Some(book) = self.books.get_mut(symbol) else {
                return Ok(());
            };
            if book.sync.state != SyncState::Live {
                return Ok(());
            }
            // count=0: amount +1 → delete bid, -1 → delete ask (Bitfinex docs).
            // count>0: amount >0 → bid upsert, <0 → ask upsert.
            let side = if entry.amount.coefficient < 0 {
                BookSide::Ask
            } else {
                BookSide::Bid
            };
            let (op, qty) = if entry.count == 0 {
                (BookOperation::Delete, None)
            } else {
                (
                    BookOperation::Upsert,
                    Some(Quantity(abs_amount(entry.amount))),
                )
            };
            if let Err(err) = book.sync.book.apply_change(side, op, entry.price, qty) {
                let instrument = book.sync.instrument;
                book.sync.invalidate("live change apply failed");
                output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                    instrument,
                    reason: format!("live change apply failed: {err}"),
                }));
                return Ok(());
            }
            if enable_l2 {
                Some(BookChange {
                    side,
                    operation: op,
                    price: entry.price,
                    quantity: qty,
                })
            } else {
                None
            }
        };
        if let Some(change) = change {
            let instrument = self.instrument_for(symbol);
            let frame_seq = self.next_frame();
            let env = self.envelope(
                instrument,
                frame_seq,
                0,
                received,
                None,
                EventFlags::empty(),
                MarketEvent::BookDelta(BookDelta {
                    changes: vec![change],
                    checksum: None,
                }),
            );
            output.push(SessionAction::EmitBatch(EventBatch {
                session: self.cfg.session,
                frame_seq,
                events: vec![env],
            }));
        }
        Ok(())
    }

    fn handle_decoded(
        &mut self,
        decoded: Decoded,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match decoded {
            Decoded::Info { code, .. } => {
                if code == Some(20051) || code == Some(20060) {
                    output.push(SessionAction::Reconnect(
                        marketfeed_adapter_api::ReconnectReason::Protocol,
                    ));
                }
            }
            Decoded::Subscribed {
                chan_id,
                kind,
                symbol,
                candle_interval,
            } => {
                self.channels.insert(
                    chan_id,
                    ChanBinding {
                        kind,
                        symbol: symbol.clone(),
                        candle_interval,
                    },
                );
                output.push(SessionAction::EmitSystem(
                    SystemEvent::SubscriptionStateChanged {
                        state: format!("subscribed:{symbol}"),
                    },
                ));
            }
            Decoded::Error { msg, code } => {
                output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                    detail: format!("bitfinex error code={code:?}: {msg}"),
                }));
            }
            Decoded::Pong | Decoded::Heartbeat { .. } => {}
            Decoded::Trade(t) => self.emit_trade(t, received, output),
            Decoded::Ticker(q) => self.emit_quote_and_stats(q, received, output),
            Decoded::BookSnapshot { symbol, entries } => {
                self.apply_book_snapshot(&symbol, &entries, received, output)?;
            }
            Decoded::BookUpdate { symbol, entry } => {
                self.apply_book_update(&symbol, &entry, received, output)?;
            }
            Decoded::Unknown => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "bitfinex".into(),
                }));
            }
            Decoded::Candle {
                symbol,
                open,
                high,
                low,
                close,
                volume,
                interval_ns,
                start_ts,
            } => {
                self.emit_candle(
                    &symbol,
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
            Decoded::Liquidation(row) => self.emit_liquidation(row, received, output),
        }
        Ok(())
    }
}

fn abs_amount(f: marketfeed_model::Fixed) -> marketfeed_model::Fixed {
    marketfeed_model::Fixed {
        coefficient: f.coefficient.unsigned_abs() as i128,
        scale: f.scale,
    }
}

impl SessionMachine for BitfinexSession {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Connected { now } => {
                self.live = false;
                self.saw_ticker = false;
                self.channels.clear();
                self.pending_status.clear();
                for book in self.books.values_mut() {
                    book.sync.begin_resync();
                    book.sync.request_snapshot();
                }
                for sym in &self.cfg.symbols {
                    output.push(SessionAction::SendText(Self::subscribe_json("trades", sym)));
                    output.push(SessionAction::SendText(Self::subscribe_json("ticker", sym)));
                    if self.cfg.enable_l2 {
                        output.push(SessionAction::SendText(Self::subscribe_json("book", sym)));
                    }
                    for &interval in &self.cfg.candle_intervals {
                        let key = format!("trade:{}:{sym}", candle_time_frame(interval));
                        output.push(SessionAction::SendText(Self::subscribe_candles(&key)));
                    }
                }
                if self.cfg.poll_deriv_status {
                    output.push(SessionAction::SendText(Self::subscribe_status(
                        "liq:global",
                    )));
                }
                output.push(SessionAction::ScheduleTimer(TimerSpec {
                    timer_id: PING_TIMER_ID,
                    fire_at: TimestampNs(
                        now.0
                            .saturating_add(PING_INTERVAL_MS.saturating_mul(1_000_000)),
                    ),
                }));
                self.request_deriv_status(output);
                self.schedule_status_timer(now, output);
                output.push(SessionAction::EmitSystem(
                    SystemEvent::ConnectionStateChanged {
                        state: "connected".into(),
                    },
                ));
                Ok(())
            }
            SessionInput::Disconnected { .. } => {
                self.live = false;
                self.saw_ticker = false;
                self.channels.clear();
                self.pending_status.clear();
                for book in self.books.values_mut() {
                    book.sync.invalidate("disconnected");
                }
                output.push(SessionAction::CancelTimer(PING_TIMER_ID));
                if self.cfg.poll_deriv_status {
                    output.push(SessionAction::CancelTimer(STATUS_TIMER_ID));
                }
                output.push(SessionAction::EmitSystem(
                    SystemEvent::ConnectionStateChanged {
                        state: "disconnected".into(),
                    },
                ));
                Ok(())
            }
            SessionInput::TextFrame { bytes, received } => match decode_text(bytes, &self.channels)
            {
                Ok(decoded) => {
                    for d in decoded {
                        self.handle_decoded(d, received, output)?;
                    }
                    Ok(())
                }
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
                if self.pending_status.remove(&request_id).is_none() {
                    return Ok(());
                }
                if response.status != 200 {
                    output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                        detail: format!("bitfinex status/deriv HTTP {}", response.status),
                    }));
                    return Ok(());
                }
                match decode_status_deriv(&response.body) {
                    Ok(rows) => {
                        for row in rows {
                            self.emit_deriv_status(row, received, output);
                        }
                    }
                    Err(_) => {
                        output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                            detail: "bad bitfinex status/deriv body".into(),
                        }));
                    }
                }
                Ok(())
            }
            SessionInput::Pong { .. } => Ok(()),
            SessionInput::Timer { timer_id, now } => {
                if timer_id == PING_TIMER_ID {
                    output.push(SessionAction::SendText(Bytes::from_static(
                        br#"{"event":"ping","cid":1}"#,
                    )));
                    output.push(SessionAction::ScheduleTimer(TimerSpec {
                        timer_id: PING_TIMER_ID,
                        fire_at: TimestampNs(
                            now.0
                                .saturating_add(PING_INTERVAL_MS.saturating_mul(1_000_000)),
                        ),
                    }));
                } else if timer_id == STATUS_TIMER_ID {
                    self.request_deriv_status(output);
                    self.schedule_status_timer(now, output);
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
