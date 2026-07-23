//! Kraken Futures SessionMachine — public trade / ticker / book (WS v1) + REST charts candles.
//!
//! # L2 book sync
//!
//! Venue sends `book_snapshot` then incremental `book` deltas (`qty=0` deletes).
//! `seq` is feed-global (not per-product contiguous), so continuity is
//! snapshot-then-apply with stale-seq drop only.
//!
//! # Ticker derivatives
//!
//! Same `ticker` feed carries mark (`markPrice`), index, funding (`funding_rate` +
//! `next_funding_rate_time`), and OI (`openInterest`). Liquidations arrive as trade
//! `type=liquidation` (Deribit-style trade tag).
//!
//! # Candles
//!
//! No public candle WS. REST poll `GET /api/charts/v1/trade/{symbol}/{resolution}`
//! on `CANDLE_TIMER_ID` (Bitstamp/Coinbase pattern). Exact Fixed OHLCV.
//!
//! # ponytail
//! No checksum / prev-seq on futures books. Ceiling = silent apply after snapshot;
//! upgrade = REST resync on crossed book if venue adds continuity.
//! Candle poll re-emits latest bar each tick (no close-only filter).

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
    MarketEvent, OpenInterest, Price, PricePoint, Quantity, Quote, SequenceRange, SessionId,
    Statistics24h, SystemEvent, TimestampNs, Trade,
};

use crate::futures_messages::{
    BookSideWire, FuturesDecoded, candle_resolution, decode_charts_rest, decode_futures_text,
    ms_to_ts, trade_id_source,
};
use crate::futures_specification::{
    CANDLE_POLL_INTERVAL_MS, CANDLE_TIMER_ID, FUTURES_CHARTS_REST_BASE, KRAKEN_FUTURES_VENUE_ID,
    PING_INTERVAL_MS, PING_TIMER_ID,
};

const SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone)]
pub struct KrakenFuturesSessionConfig {
    pub symbols: Vec<String>,
    pub instrument_ids: HashMap<String, InstrumentId>,
    pub connection: ConnectionId,
    pub session: SessionId,
    pub enable_l2: bool,
    pub candle_intervals: Vec<CandleInterval>,
    pub price_scale: u8,
    pub qty_scale: u8,
}

impl Default for KrakenFuturesSessionConfig {
    fn default() -> Self {
        let mut instrument_ids = HashMap::new();
        instrument_ids.insert("PF_XBTUSD".into(), InstrumentId(1));
        Self {
            symbols: vec!["PF_XBTUSD".into()],
            instrument_ids,
            connection: ConnectionId(1),
            session: SessionId(1),
            enable_l2: false,
            candle_intervals: Vec::new(),
            // PF_XBTUSD tick 0.5, contract qty integers.
            price_scale: 1,
            qty_scale: 0,
        }
    }
}

#[derive(Debug)]
struct SymbolBook {
    sync: BookSynchronizer,
    last_seq: Option<u64>,
}

#[derive(Debug)]
pub struct KrakenFuturesSession {
    #[allow(dead_code)]
    catalog: CatalogView,
    cfg: KrakenFuturesSessionConfig,
    frame_seq: u64,
    books: HashMap<String, SymbolBook>,
    live: bool,
    next_http_id: u64,
    pending_candles: HashMap<u64, (String, CandleInterval)>,
}

impl KrakenFuturesSession {
    pub fn new(spec: SessionSpec, catalog: CatalogView, cfg: KrakenFuturesSessionConfig) -> Self {
        let _ = spec;
        let mut books = HashMap::new();
        if cfg.enable_l2 {
            for (sym, id) in &cfg.instrument_ids {
                let book = OrderBook::new(cfg.price_scale, cfg.qty_scale, None);
                let mut sync = BookSynchronizer::new(*id, book, SyncLimits::default());
                sync.request_snapshot();
                books.insert(
                    sym.clone(),
                    SymbolBook {
                        sync,
                        last_seq: None,
                    },
                );
            }
        }
        Self {
            catalog,
            cfg,
            frame_seq: 0,
            books,
            live: false,
            next_http_id: 1,
            pending_candles: HashMap::new(),
        }
    }

    fn request_candle(
        &mut self,
        symbol: &str,
        interval: CandleInterval,
        output: &mut ActionBuffer,
    ) {
        let id = self.next_http_id;
        self.next_http_id += 1;
        self.pending_candles
            .insert(id, (symbol.to_string(), interval));
        let resolution = candle_resolution(interval);
        output.push(SessionAction::RequestHttp(HttpRequestSpec {
            id,
            method: HttpMethod::Get,
            url: format!("{FUTURES_CHARTS_REST_BASE}/trade/{symbol}/{resolution}?count=1"),
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
        for symbol in &symbols {
            for &interval in &intervals {
                self.request_candle(symbol, interval, output);
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

    fn emit_candle(
        &mut self,
        symbol: &str,
        candle: Candle,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) {
        let instrument = self.instrument_for(symbol);
        let frame_seq = self.next_frame();
        let start_ts = candle.start_ts;
        let env = self.envelope(
            instrument,
            frame_seq,
            0,
            received,
            Some(start_ts),
            None,
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

    fn instrument_for(&self, name: &str) -> Option<InstrumentId> {
        self.cfg.instrument_ids.get(name).copied()
    }

    fn envelope(
        &self,
        instrument: Option<InstrumentId>,
        frame_seq: u64,
        event_index: u16,
        received: FrameStamp,
        exchange_ts: Option<TimestampNs>,
        seq: Option<SequenceRange>,
        flags: EventFlags,
        payload: MarketEvent,
    ) -> EventEnvelope {
        EventEnvelope {
            schema_version: SCHEMA_VERSION,
            venue: KRAKEN_FUTURES_VENUE_ID,
            instrument,
            connection: self.cfg.connection,
            session: self.cfg.session,
            frame_seq,
            event_index,
            exchange_ts,
            receive_ts: received.receive_ts,
            source_sequence: seq,
            flags,
            payload,
        }
    }

    fn subscribe_json(feed: &str, products: &[String]) -> Bytes {
        Bytes::from(
            serde_json::json!({
                "event": "subscribe",
                "feed": feed,
                "product_ids": products,
            })
            .to_string(),
        )
    }

    fn maybe_mark_live(&mut self, output: &mut ActionBuffer) {
        if self.live {
            return;
        }
        if self.cfg.enable_l2 {
            let all_live = self.books.values().all(|b| b.sync.state == SyncState::Live);
            if !all_live {
                return;
            }
        }
        self.live = true;
        output.push(SessionAction::MarkLive);
    }

    fn handle_decoded(
        &mut self,
        decoded: FuturesDecoded,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match decoded {
            FuturesDecoded::Trades(rows) => {
                if rows.is_empty() {
                    return Ok(());
                }
                let frame_seq = self.next_frame();
                let mut events = Vec::with_capacity(rows.len() * 2);
                let mut idx = 0u16;
                for t in &rows {
                    let instrument = self.instrument_for(&t.product_id);
                    let seq = t.seq.map(|s| SequenceRange { first: s, last: s });
                    let ts = Some(ms_to_ts(t.exchange_ts_ms));
                    events.push(self.envelope(
                        instrument,
                        frame_seq,
                        idx,
                        received,
                        ts,
                        seq,
                        EventFlags::empty(),
                        MarketEvent::Trade(Trade {
                            price: t.price,
                            quantity: t.quantity,
                            aggressor: t.aggressor,
                            trade_id: Some(trade_id_source(&t.uid)),
                        }),
                    ));
                    idx = idx.saturating_add(1);
                    // No dedicated public liq channel; Kraken tags liq via trade `type`.
                    if t.liquidation {
                        events.push(self.envelope(
                            instrument,
                            frame_seq,
                            idx,
                            received,
                            ts,
                            seq,
                            EventFlags::empty(),
                            MarketEvent::Liquidation(Liquidation {
                                price: t.price,
                                quantity: t.quantity,
                                side: t.aggressor,
                            }),
                        ));
                        idx = idx.saturating_add(1);
                    }
                }
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events,
                }));
                self.maybe_mark_live(output);
            }
            FuturesDecoded::Ticker {
                product_id,
                bid_price,
                bid_qty,
                ask_price,
                ask_qty,
                mark,
                index,
                funding_rate,
                next_funding_ts,
                open_interest,
                open,
                high,
                low,
                close,
                volume,
                quote_volume,
                exchange_ts_ms,
            } => {
                let instrument = self.instrument_for(&product_id);
                let frame_seq = self.next_frame();
                let mut events = Vec::new();
                let mut idx = 0u16;
                let mut push = |payload: MarketEvent| {
                    events.push(self.envelope(
                        instrument,
                        frame_seq,
                        idx,
                        received,
                        Some(ms_to_ts(exchange_ts_ms)),
                        None,
                        EventFlags::empty(),
                        payload,
                    ));
                    idx = idx.saturating_add(1);
                };
                if let (Some(bid_price), Some(ask_price)) = (bid_price, ask_price) {
                    push(MarketEvent::Quote(Quote {
                        bid_price,
                        ask_price,
                        bid_quantity: bid_qty,
                        ask_quantity: ask_qty,
                    }));
                }
                if let Some(price) = mark {
                    push(MarketEvent::MarkPrice(PricePoint { price }));
                }
                if let Some(price) = index {
                    push(MarketEvent::IndexPrice(PricePoint { price }));
                }
                if let Some(rate) = funding_rate {
                    push(MarketEvent::Funding(Funding {
                        rate,
                        next_funding_ts,
                    }));
                }
                if let Some(quantity) = open_interest {
                    push(MarketEvent::OpenInterest(OpenInterest { quantity }));
                }
                if open.is_some()
                    || high.is_some()
                    || low.is_some()
                    || close.is_some()
                    || volume.is_some()
                    || quote_volume.is_some()
                {
                    push(MarketEvent::Statistics24h(Statistics24h {
                        open,
                        high,
                        low,
                        close,
                        volume,
                        quote_volume,
                    }));
                }
                if events.is_empty() {
                    return Ok(());
                }
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events,
                }));
                self.maybe_mark_live(output);
            }
            FuturesDecoded::BookSnapshot {
                product_id,
                seq,
                bids,
                asks,
                exchange_ts_ms,
            } => {
                self.apply_book_snapshot(
                    &product_id,
                    seq,
                    &bids,
                    &asks,
                    exchange_ts_ms,
                    received,
                    output,
                )?;
            }
            FuturesDecoded::BookDelta {
                product_id,
                seq,
                side,
                price,
                quantity,
                exchange_ts_ms,
            } => {
                self.apply_book_delta(
                    &product_id,
                    seq,
                    side,
                    price,
                    quantity,
                    exchange_ts_ms,
                    received,
                    output,
                )?;
            }
            FuturesDecoded::SubscriptionState { state, success } => {
                output.push(SessionAction::EmitSystem(
                    SystemEvent::SubscriptionStateChanged {
                        state: state.clone(),
                    },
                ));
                if !success {
                    self.live = false;
                    output.push(SessionAction::StopSession(StopReason::FatalProtocol));
                } else if state == "subscribed" && !self.cfg.enable_l2 {
                    self.maybe_mark_live(output);
                }
            }
            FuturesDecoded::Heartbeat | FuturesDecoded::Info | FuturesDecoded::Candle { .. } => {}
            FuturesDecoded::Unknown => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "kraken-futures".into(),
                }));
            }
        }
        Ok(())
    }

    fn apply_book_snapshot(
        &mut self,
        product_id: &str,
        seq: u64,
        bids: &[(Price, Quantity)],
        asks: &[(Price, Quantity)],
        exchange_ts_ms: i64,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if !self.cfg.enable_l2 {
            return Ok(());
        }
        let Some(book) = self.books.get_mut(product_id) else {
            return Ok(());
        };
        book.sync.begin_resync();
        book.sync.request_snapshot();
        if let Err(err) = book.sync.book.apply_snapshot(bids, asks, Some(seq)) {
            let instrument = book.sync.instrument;
            book.sync.invalidate(&err.to_string());
            book.last_seq = None;
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument,
                reason: err.to_string(),
            }));
            return Ok(());
        }
        book.sync.state = SyncState::Live;
        book.last_seq = Some(seq);
        let instrument = book.sync.instrument;
        let Some((book_bids, book_asks)) = book.sync.book.snapshot_levels() else {
            return Ok(());
        };
        let frame_seq = self.next_frame();
        let env = self.envelope(
            Some(instrument),
            frame_seq,
            0,
            received,
            Some(ms_to_ts(exchange_ts_ms)),
            Some(SequenceRange {
                first: seq,
                last: seq,
            }),
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
        self.maybe_mark_live(output);
        Ok(())
    }

    fn apply_book_delta(
        &mut self,
        product_id: &str,
        seq: u64,
        side: BookSideWire,
        price: Price,
        quantity: Quantity,
        exchange_ts_ms: i64,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if !self.cfg.enable_l2 {
            return Ok(());
        }
        let Some(book) = self.books.get_mut(product_id) else {
            return Ok(());
        };
        if book.sync.state != SyncState::Live {
            return Ok(());
        }
        // Feed-global seq: drop stale only (not contiguous per product).
        if book.last_seq.is_some_and(|last| seq <= last) {
            return Ok(());
        }
        let book_side = match side {
            BookSideWire::Bid => BookSide::Bid,
            BookSideWire::Ask => BookSide::Ask,
        };
        let (op, qty) = if quantity.0.coefficient == 0 {
            (BookOperation::Delete, None)
        } else {
            (BookOperation::Upsert, Some(quantity))
        };
        if let Err(err) = book.sync.book.apply_change(book_side, op, price, qty) {
            let instrument = book.sync.instrument;
            book.sync.invalidate("live change apply failed");
            book.last_seq = None;
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument,
                reason: format!("live change apply failed: {err}"),
            }));
            return Ok(());
        }
        book.sync.book.set_sequence(seq);
        book.last_seq = Some(seq);
        let instrument = self.instrument_for(product_id);
        let frame_seq = self.next_frame();
        let env = self.envelope(
            instrument,
            frame_seq,
            0,
            received,
            Some(ms_to_ts(exchange_ts_ms)),
            Some(SequenceRange {
                first: seq,
                last: seq,
            }),
            EventFlags::empty(),
            MarketEvent::BookDelta(BookDelta {
                changes: vec![BookChange {
                    side: book_side,
                    operation: op,
                    price,
                    quantity: qty,
                }],
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
}

impl SessionMachine for KrakenFuturesSession {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Connected { now } => {
                self.live = false;
                self.pending_candles.clear();
                for book in self.books.values_mut() {
                    book.sync.begin_resync();
                    book.sync.request_snapshot();
                    book.last_seq = None;
                }
                let products = self.cfg.symbols.clone();
                output.push(SessionAction::SendText(Self::subscribe_json(
                    "trade", &products,
                )));
                output.push(SessionAction::SendText(Self::subscribe_json(
                    "ticker", &products,
                )));
                if self.cfg.enable_l2 {
                    output.push(SessionAction::SendText(Self::subscribe_json(
                        "book", &products,
                    )));
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
                for book in self.books.values_mut() {
                    book.sync.invalidate("disconnected");
                    book.last_seq = None;
                }
                output.push(SessionAction::CancelTimer(PING_TIMER_ID));
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
            SessionInput::TextFrame { bytes, received } => match decode_futures_text(bytes) {
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
                let Some((symbol, interval)) = self.pending_candles.remove(&request_id) else {
                    return Ok(());
                };
                if response.status != 200 {
                    output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                        detail: format!("kf charts HTTP {}", response.status),
                    }));
                    return Ok(());
                }
                match decode_charts_rest(&response.body, interval) {
                    Ok(FuturesDecoded::Candle {
                        open,
                        high,
                        low,
                        close,
                        volume,
                        interval_ns,
                        start_ts,
                    }) => {
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
                    Ok(_) | Err(_) => {
                        output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                            detail: "bad kf charts body".into(),
                        }));
                    }
                }
                Ok(())
            }
            SessionInput::Pong { .. } => Ok(()),
            SessionInput::Timer { timer_id, now } => {
                if timer_id == PING_TIMER_ID {
                    output.push(SessionAction::SendText(Bytes::from_static(
                        br#"{"event":"ping"}"#,
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
                }
                Ok(())
            }
            SessionInput::Control { command } => {
                match command {
                    marketfeed_adapter_api::SessionCommand::Stop => {
                        output.push(SessionAction::StopSession(StopReason::Control));
                    }
                    marketfeed_adapter_api::SessionCommand::Resync(id) => {
                        if self.cfg.enable_l2
                            && self.cfg.instrument_ids.values().any(|iid| iid == id)
                        {
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
