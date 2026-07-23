//! Coinbase Advanced Trade SessionMachine (public T/Q/L2 + REST candles).
//!
//! Public WS (`wss://advanced-trade-ws.coinbase.com`): one subscribe message per
//! channel (`heartbeats`, `status`, `market_trades`, `ticker`, optional `level2`).
//! Candles use public REST poll on `CANDLE_TIMER_ID` (WS `candles` is 5m-only;
//! decode emits if received, SessionMachine does not subscribe).
//! SessionMachine emits `SendText` / `RequestHttp` / `ScheduleTimer` only — no networking.
//!
//! # ponytail
//! Candle poll re-emits latest bar each tick (no close-only filter).
//! No continuity check on l2 update. Ceiling = silent apply after snapshot.
//! Classic Exchange VenueId 16 remains a separate protocol (do not delete).

use std::collections::HashMap;

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, CandleInterval, EventBatch, HttpMethod, HttpRequestSpec,
    SessionAction, SessionInput, SessionMachine, SessionSpec, StopReason, TimerSpec,
};
use marketfeed_book::{BookSynchronizer, OrderBook, SyncLimits, SyncState};
use marketfeed_model::{
    BookChange, BookDelta, BookOperation, BookSide, BookSnapshot, Candle, CatalogView,
    ConnectionId, EventEnvelope, EventFlags, FrameStamp, InstrumentId, InstrumentUpdate,
    MarketEvent, Price, Quantity, Quote, SequenceRange, SessionId, Statistics24h, SystemEvent,
    TimestampNs, Trade,
};

use crate::advanced::messages::{
    BookSideWire, DecodedEvent, candles_url, decode_candles_rest, decode_text, ns_to_ts,
    trade_id_source,
};
use crate::advanced::specification::{
    CANDLE_POLL_INTERVAL_MS, CANDLE_TIMER_ID, COINBASE_ADV_VENUE_ID, REST_BASE,
};

const SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone)]
pub struct CoinbaseAdvSessionConfig {
    pub products: Vec<String>,
    pub instrument_ids: HashMap<String, InstrumentId>,
    pub connection: ConnectionId,
    pub session: SessionId,
    pub enable_l2: bool,
    pub candle_intervals: Vec<CandleInterval>,
    pub price_scale: u8,
    pub qty_scale: u8,
}

impl Default for CoinbaseAdvSessionConfig {
    fn default() -> Self {
        let mut instrument_ids = HashMap::new();
        instrument_ids.insert("BTC-USD".into(), InstrumentId(1));
        Self {
            products: vec!["BTC-USD".into()],
            instrument_ids,
            connection: ConnectionId(1),
            session: SessionId(1),
            enable_l2: false,
            candle_intervals: Vec::new(),
            // BTC-USD: penny prices, base qty to 8 dp typically.
            price_scale: 2,
            qty_scale: 8,
        }
    }
}

#[derive(Debug)]
struct ProductBook {
    sync: BookSynchronizer,
}

#[derive(Debug)]
pub struct CoinbaseAdvSession {
    #[allow(dead_code)]
    catalog: CatalogView,
    cfg: CoinbaseAdvSessionConfig,
    frame_seq: u64,
    books: HashMap<String, ProductBook>,
    live: bool,
    next_http_id: u64,
    pending_candles: HashMap<u64, (String, CandleInterval)>,
}

impl CoinbaseAdvSession {
    pub fn new(spec: SessionSpec, catalog: CatalogView, cfg: CoinbaseAdvSessionConfig) -> Self {
        let _ = spec;
        let mut books = HashMap::new();
        if cfg.enable_l2 {
            for (name, id) in &cfg.instrument_ids {
                let book = OrderBook::new(cfg.price_scale, cfg.qty_scale, None);
                let mut sync = BookSynchronizer::new(*id, book, SyncLimits::default());
                sync.request_snapshot();
                books.insert(name.clone(), ProductBook { sync });
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
        product: &str,
        interval: CandleInterval,
        now: TimestampNs,
        output: &mut ActionBuffer,
    ) {
        let id = self.next_http_id;
        self.next_http_id += 1;
        self.pending_candles
            .insert(id, (product.to_string(), interval));
        output.push(SessionAction::RequestHttp(HttpRequestSpec {
            id,
            method: HttpMethod::Get,
            url: candles_url(REST_BASE, product, interval, now),
            headers: Vec::new(),
            body: None,
        }));
    }

    fn poll_candles_all(&mut self, now: TimestampNs, output: &mut ActionBuffer) {
        if self.cfg.candle_intervals.is_empty() {
            return;
        }
        let products = self.cfg.products.clone();
        let intervals = self.cfg.candle_intervals.clone();
        for product in &products {
            for &interval in &intervals {
                self.request_candle(product, interval, now, output);
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
        product_id: &str,
        candle: Candle,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) {
        let instrument = self.instrument_for(product_id);
        let frame_seq = self.next_frame();
        let env = self.envelope(
            instrument,
            frame_seq,
            0,
            received,
            Some(candle.start_ts),
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
            venue: COINBASE_ADV_VENUE_ID,
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

    /// Advanced Trade: one `channel` per subscribe message (no multi-channel batch).
    fn subscribe_payloads(&self) -> Vec<Bytes> {
        let mut channels = vec!["heartbeats", "status", "market_trades", "ticker"];
        if self.cfg.enable_l2 {
            channels.push("level2");
        }
        channels
            .into_iter()
            .map(|channel| {
                Bytes::from(
                    serde_json::json!({
                        "type": "subscribe",
                        "product_ids": self.cfg.products,
                        "channel": channel,
                    })
                    .to_string(),
                )
            })
            .collect()
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
        decoded: DecodedEvent,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match decoded {
            DecodedEvent::Trade(row) => {
                let instrument = self.instrument_for(&row.product_id);
                let frame_seq = self.next_frame();
                let ts = row.exchange_ts_ns.map(ns_to_ts);
                let seq = row.sequence.map(|s| SequenceRange { first: s, last: s });
                let env = self.envelope(
                    instrument,
                    frame_seq,
                    0,
                    received,
                    ts,
                    seq,
                    EventFlags::empty(),
                    MarketEvent::Trade(Trade {
                        price: row.price,
                        quantity: row.quantity,
                        aggressor: row.aggressor,
                        trade_id: Some(trade_id_source(&row.trade_id)),
                    }),
                );
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events: vec![env],
                }));
            }
            DecodedEvent::Quote {
                product_id,
                bid_price,
                bid_qty,
                ask_price,
                ask_qty,
                exchange_ts_ns,
            } => {
                let instrument = self.instrument_for(&product_id);
                let frame_seq = self.next_frame();
                let env = self.envelope(
                    instrument,
                    frame_seq,
                    0,
                    received,
                    exchange_ts_ns.map(ns_to_ts),
                    None,
                    EventFlags::empty(),
                    MarketEvent::Quote(Quote {
                        bid_price,
                        bid_quantity: bid_qty,
                        ask_price,
                        ask_quantity: ask_qty,
                    }),
                );
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events: vec![env],
                }));
            }
            DecodedEvent::Statistics24h {
                product_id,
                open,
                high,
                low,
                close,
                volume,
                exchange_ts_ns,
            } => {
                let instrument = self.instrument_for(&product_id);
                let frame_seq = self.next_frame();
                let env = self.envelope(
                    instrument,
                    frame_seq,
                    0,
                    received,
                    exchange_ts_ns.map(ns_to_ts),
                    None,
                    EventFlags::empty(),
                    MarketEvent::Statistics24h(Statistics24h {
                        open,
                        high,
                        low,
                        close,
                        volume,
                        quote_volume: None,
                    }),
                );
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events: vec![env],
                }));
            }
            DecodedEvent::BookSnapshot {
                product_id,
                bids,
                asks,
            } => {
                self.apply_book_snapshot(&product_id, &bids, &asks, received, output)?;
            }
            DecodedEvent::BookDelta {
                product_id,
                changes,
                exchange_ts_ns,
            } => {
                self.apply_book_delta(&product_id, &changes, exchange_ts_ns, received, output)?;
            }
            DecodedEvent::Candle {
                product_id,
                open,
                high,
                low,
                close,
                volume,
                interval_ns,
                start_ts,
            } => {
                // REST path uses empty product_id (filled via pending map + HttpResponse).
                if !product_id.is_empty() {
                    self.emit_candle(
                        &product_id,
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
            }
            DecodedEvent::InstrumentStatus { product_id, status } => {
                let instrument = self.instrument_for(&product_id);
                let frame_seq = self.next_frame();
                let env = self.envelope(
                    instrument,
                    frame_seq,
                    0,
                    received,
                    None,
                    None,
                    EventFlags::empty(),
                    MarketEvent::InstrumentUpdate(InstrumentUpdate { status }),
                );
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events: vec![env],
                }));
            }
            DecodedEvent::SubscribeAck => {
                output.push(SessionAction::EmitSystem(
                    SystemEvent::SubscriptionStateChanged {
                        state: "subscribed".into(),
                    },
                ));
                if !self.cfg.enable_l2 {
                    self.maybe_mark_live(output);
                }
            }
            DecodedEvent::Heartbeat => {}
            DecodedEvent::Error(msg) => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: format!("coinbase-adv error: {msg}"),
                }));
            }
            DecodedEvent::Unknown => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "coinbase-adv".into(),
                }));
            }
        }
        Ok(())
    }

    fn apply_book_snapshot(
        &mut self,
        product_id: &str,
        bids: &[(Price, Quantity)],
        asks: &[(Price, Quantity)],
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
        if let Err(err) = book.sync.book.apply_snapshot(bids, asks, None) {
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
            None,
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
        self.maybe_mark_live(output);
        Ok(())
    }

    fn apply_book_delta(
        &mut self,
        product_id: &str,
        changes: &[crate::advanced::messages::BookLevelChange],
        exchange_ts_ns: Option<i64>,
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

        let mut out_changes = Vec::with_capacity(changes.len());
        for c in changes {
            let side = match c.side {
                BookSideWire::Bid => BookSide::Bid,
                BookSideWire::Ask => BookSide::Ask,
            };
            let (op, qty) = if c.quantity.0.coefficient == 0 {
                (BookOperation::Delete, None)
            } else {
                (BookOperation::Upsert, Some(c.quantity))
            };
            out_changes.push(BookChange {
                side,
                operation: op,
                price: c.price,
                quantity: qty,
            });
        }
        if let Err(err) = book.sync.book.apply_changes_atomic(&out_changes) {
            let instrument = book.sync.instrument;
            book.sync.invalidate("live change apply failed");
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument,
                reason: format!("live change apply failed: {err}"),
            }));
            output.push(SessionAction::ResyncInstrument(instrument));
            return Ok(());
        }

        let instrument = self.instrument_for(product_id);
        let frame_seq = self.next_frame();
        let env = self.envelope(
            instrument,
            frame_seq,
            0,
            received,
            exchange_ts_ns.map(ns_to_ts),
            None,
            EventFlags::DELTA,
            MarketEvent::BookDelta(BookDelta {
                changes: out_changes,
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

impl SessionMachine for CoinbaseAdvSession {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Connected { now } => {
                self.live = false;
                self.pending_candles.clear();
                if self.cfg.enable_l2 {
                    for book in self.books.values_mut() {
                        book.sync.begin_resync();
                        book.sync.request_snapshot();
                    }
                }
                for payload in self.subscribe_payloads() {
                    output.push(SessionAction::SendText(payload));
                }
                self.poll_candles_all(now, output);
                self.schedule_candle_timer(now, output);
                Ok(())
            }
            SessionInput::Disconnected { .. } => {
                self.live = false;
                self.pending_candles.clear();
                for book in self.books.values_mut() {
                    book.sync.invalidate("disconnected");
                }
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
                    detail: "coinbase-adv unexpected binary".into(),
                }));
                Ok(())
            }
            SessionInput::HttpResponse {
                request_id,
                response,
                received,
            } => {
                let Some((product, interval)) = self.pending_candles.remove(&request_id) else {
                    return Ok(());
                };
                if response.status != 200 {
                    output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                        detail: format!("coinbase-adv candles HTTP {}", response.status),
                    }));
                    return Ok(());
                }
                match decode_candles_rest(&response.body, interval) {
                    Ok(DecodedEvent::Candle {
                        open,
                        high,
                        low,
                        close,
                        volume,
                        interval_ns,
                        start_ts,
                        ..
                    }) => {
                        self.emit_candle(
                            &product,
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
                            detail: "bad coinbase-adv candles body".into(),
                        }));
                    }
                }
                Ok(())
            }
            SessionInput::Pong { .. } => Ok(()),
            SessionInput::Timer { timer_id, now } => {
                if timer_id == CANDLE_TIMER_ID {
                    self.poll_candles_all(now, output);
                    self.schedule_candle_timer(now, output);
                }
                Ok(())
            }
            SessionInput::Control { command } => {
                if matches!(command, marketfeed_adapter_api::SessionCommand::Stop) {
                    output.push(SessionAction::StopSession(StopReason::Control));
                }
                Ok(())
            }
        }
    }
}
