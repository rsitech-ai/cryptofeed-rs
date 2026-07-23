//! Coinbase Exchange SessionMachine (matches + ticker BBO + level2 + REST candles).
//!
//! Exchange WS has no candle channel; poll REST `/products/{id}/candles` on
//! `CANDLE_TIMER_ID` (Binance OI pattern) when `candle_intervals` is non-empty.
//!
//! # ponytail
//! No continuity check on l2update. Ceiling = silent apply after snapshot.
//! Candle poll re-emits latest bar each tick (no close-only filter).

use std::collections::HashMap;

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, CandleInterval, EventBatch, HttpMethod, HttpRequestSpec,
    SensitiveBytes, SessionAction, SessionInput, SessionMachine, SessionSpec, StopReason,
    TimerSpec,
};
use marketfeed_book::{BookSynchronizer, OrderBook, SyncLimits, SyncState};
use marketfeed_model::{
    BookChange, BookDelta, BookOperation, BookSide, BookSnapshot, Candle, CatalogView,
    ConnectionId, EventEnvelope, EventFlags, FrameStamp, InstrumentId, InstrumentUpdate,
    MarketEvent, Price, Quantity, Quote, SequenceRange, SessionId, Statistics24h, SystemEvent,
    TimestampNs, Trade,
};

use crate::CoinbaseExchangeCredentials;
use crate::messages::{
    BookSideWire, DecodedEvent, Heartbeat, SubscriptionChannel, candle_granularity_secs,
    decode_candles_rest, decode_text, ns_to_ts, trade_id_source,
};
use crate::specification::{
    CANDLE_POLL_INTERVAL_MS, CANDLE_TIMER_ID, COINBASE_SPOT_VENUE_ID, HEARTBEAT_TIMEOUT_MS,
    HEARTBEAT_TIMER_ID, REST_BASE,
};

const SCHEMA_VERSION: u16 = 1;
const HTTP_ERROR_PREVIEW_BYTES: usize = 256;
const HTTP_ERROR_PREVIEW_CHARS: usize = 160;

fn http_error_detail(context: &str, status: u16, body: &[u8]) -> String {
    let bounded = &body[..body.len().min(HTTP_ERROR_PREVIEW_BYTES)];
    let lossy = String::from_utf8_lossy(bounded);
    let mut preview = String::with_capacity(HTTP_ERROR_PREVIEW_CHARS);
    let mut chars = lossy.chars();
    for ch in chars.by_ref().take(HTTP_ERROR_PREVIEW_CHARS) {
        preview.push(if ch.is_control() { ' ' } else { ch });
    }
    let truncated = body.len() > bounded.len() || chars.next().is_some();
    if truncated {
        preview.push('…');
    }
    format!("{context} HTTP {status} body={preview}")
}

#[derive(Debug, Clone)]
pub struct CoinbaseSessionConfig {
    pub products: Vec<String>,
    pub instrument_ids: HashMap<String, InstrumentId>,
    pub connection: ConnectionId,
    pub session: SessionId,
    pub enable_l2: bool,
    pub credentials: Option<CoinbaseExchangeCredentials>,
    pub candle_intervals: Vec<CandleInterval>,
    pub price_scale: u8,
    pub qty_scale: u8,
}

impl Default for CoinbaseSessionConfig {
    fn default() -> Self {
        let mut instrument_ids = HashMap::new();
        instrument_ids.insert("BTC-USD".into(), InstrumentId(1));
        Self {
            products: vec!["BTC-USD".into()],
            instrument_ids,
            connection: ConnectionId(1),
            session: SessionId(1),
            enable_l2: false,
            credentials: None,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoinbaseHeartbeatState {
    pub sequence: u64,
    pub last_trade_id: u64,
    pub exchange_ts: Option<TimestampNs>,
    pub received_at: TimestampNs,
}

#[derive(Debug)]
pub struct CoinbaseSpotSession {
    #[allow(dead_code)]
    catalog: CatalogView,
    cfg: CoinbaseSessionConfig,
    frame_seq: u64,
    books: HashMap<String, ProductBook>,
    live: bool,
    next_http_id: u64,
    pending_candles: HashMap<u64, (String, CandleInterval)>,
    subscription_acknowledged: bool,
    heartbeats: HashMap<String, CoinbaseHeartbeatState>,
}

impl CoinbaseSpotSession {
    pub fn new(spec: SessionSpec, catalog: CatalogView, cfg: CoinbaseSessionConfig) -> Self {
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
            subscription_acknowledged: false,
            heartbeats: HashMap::new(),
        }
    }

    fn reset_connection_state(&mut self) {
        self.live = false;
        self.subscription_acknowledged = false;
        self.heartbeats.clear();
        self.pending_candles.clear();
        if self.cfg.enable_l2 {
            for book in self.books.values_mut() {
                book.sync.begin_resync();
                book.sync.request_snapshot();
            }
        }
    }

    pub fn heartbeat_state(&self, product: &str) -> Option<&CoinbaseHeartbeatState> {
        self.heartbeats.get(product)
    }

    fn schedule_heartbeat_watchdog(&self, now: TimestampNs, output: &mut ActionBuffer) {
        output.push(SessionAction::ScheduleTimer(TimerSpec {
            timer_id: HEARTBEAT_TIMER_ID,
            fire_at: TimestampNs(
                now.0
                    .saturating_add(HEARTBEAT_TIMEOUT_MS.saturating_mul(1_000_000)),
            ),
        }));
    }

    fn subscription_ack_complete(&self, channels: &[SubscriptionChannel]) -> bool {
        let mut required = vec!["matches", "ticker", "heartbeat", "status"];
        if self.cfg.enable_l2 {
            required.push("level2");
        }
        required.into_iter().all(|required_name| {
            channels.iter().any(|channel| {
                channel.name == required_name
                    && (required_name == "status"
                        || self
                            .cfg
                            .products
                            .iter()
                            .all(|product| channel.product_ids.contains(product)))
            })
        })
    }

    fn request_candle(
        &mut self,
        product: &str,
        interval: CandleInterval,
        output: &mut ActionBuffer,
    ) {
        let id = self.next_http_id;
        self.next_http_id += 1;
        self.pending_candles
            .insert(id, (product.to_string(), interval));
        let gran = candle_granularity_secs(interval);
        output.push(SessionAction::RequestHttp(HttpRequestSpec {
            id,
            method: HttpMethod::Get,
            url: format!("{REST_BASE}/products/{product}/candles?granularity={gran}"),
            headers: Vec::new(),
            body: None,
        }));
    }
    fn poll_candles_all(&mut self, output: &mut ActionBuffer) {
        if self.cfg.candle_intervals.is_empty() {
            return;
        }
        let products = self.cfg.products.clone();
        let intervals = self.cfg.candle_intervals.clone();
        for product in &products {
            for &interval in &intervals {
                self.request_candle(product, interval, output);
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
            venue: COINBASE_SPOT_VENUE_ID,
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

    fn subscribe_payload(&self, now: TimestampNs) -> Result<Bytes, AdapterError> {
        let mut channels: Vec<serde_json::Value> = vec![
            "matches".into(),
            "ticker".into(),
            "heartbeat".into(),
            "status".into(),
        ];
        if self.cfg.enable_l2 {
            channels.push("level2".into());
        }
        let mut payload = serde_json::json!({
                "type": "subscribe",
                "product_ids": self.cfg.products,
                "channels": channels,
        });
        if self.cfg.enable_l2 {
            let credentials = self.cfg.credentials.as_ref().ok_or_else(|| {
                AdapterError::Subscription(
                    "Coinbase Exchange level2 credentials are required".into(),
                )
            })?;
            let auth = credentials
                .sign_subscribe(now.0.div_euclid(1_000_000_000))
                .map_err(|error| AdapterError::Subscription(error.to_string()))?;
            payload["timestamp"] = auth.timestamp.into();
            payload["key"] = auth.key.into();
            payload["passphrase"] = auth.passphrase.into();
            payload["signature"] = auth.signature.into();
        }
        Ok(Bytes::from(payload.to_string()))
    }

    fn maybe_mark_live(&mut self, output: &mut ActionBuffer) {
        if self.live {
            return;
        }
        if !self.subscription_acknowledged {
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
                        trade_id: Some(trade_id_source(row.trade_id)),
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
            DecodedEvent::Candle { .. } => {}
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
            DecodedEvent::ProductStatus {
                product_id,
                status,
                exchange_ts_ns,
            } => {
                // Only emit for subscribed products.
                let Some(instrument) = self.instrument_for(&product_id) else {
                    return Ok(());
                };
                let frame_seq = self.next_frame();
                let env = self.envelope(
                    Some(instrument),
                    frame_seq,
                    0,
                    received,
                    exchange_ts_ns.map(ns_to_ts),
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
            DecodedEvent::SubscribeAck(channels) => {
                self.subscription_acknowledged = self.subscription_ack_complete(&channels);
                output.push(SessionAction::EmitSystem(
                    SystemEvent::SubscriptionStateChanged {
                        state: if self.subscription_acknowledged {
                            "subscribed".into()
                        } else {
                            "subscription_incomplete".into()
                        },
                    },
                ));
                if self.subscription_acknowledged {
                    self.maybe_mark_live(output);
                }
            }
            DecodedEvent::Heartbeat(Heartbeat {
                product_id,
                sequence,
                last_trade_id,
                exchange_ts_ns,
            }) => {
                self.heartbeats.insert(
                    product_id,
                    CoinbaseHeartbeatState {
                        sequence,
                        last_trade_id,
                        exchange_ts: exchange_ts_ns.map(ns_to_ts),
                        received_at: received.receive_ts,
                    },
                );
                self.schedule_heartbeat_watchdog(received.receive_ts, output);
            }
            DecodedEvent::Error(msg) => {
                self.live = false;
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: format!("coinbase error: {msg}"),
                }));
                output.push(SessionAction::Reconnect(
                    marketfeed_adapter_api::ReconnectReason::Protocol,
                ));
            }
            DecodedEvent::Unknown => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "coinbase".into(),
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
        changes: &[crate::messages::BookLevelChange],
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
            if let Err(err) = book.sync.book.apply_change(side, op, c.price, qty) {
                let instrument = book.sync.instrument;
                book.sync.invalidate("live change apply failed");
                output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                    instrument,
                    reason: format!("live change apply failed: {err}"),
                }));
                output.push(SessionAction::ResyncInstrument(instrument));
                return Ok(());
            }
            out_changes.push(BookChange {
                side,
                operation: op,
                price: c.price,
                quantity: qty,
            });
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

impl SessionMachine for CoinbaseSpotSession {
    fn on_replay_start(
        &mut self,
        now: TimestampNs,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if !self.cfg.enable_l2 {
            return self.on_input(SessionInput::Connected { now }, output);
        }
        self.reset_connection_state();
        Ok(())
    }

    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Connected { now } => {
                self.reset_connection_state();
                let payload = self.subscribe_payload(now)?;
                if self.cfg.enable_l2 {
                    output.push(SessionAction::SendSensitiveText(SensitiveBytes::new(
                        payload,
                    )));
                } else {
                    output.push(SessionAction::SendText(payload));
                }
                self.poll_candles_all(output);
                self.schedule_candle_timer(now, output);
                self.schedule_heartbeat_watchdog(now, output);
                Ok(())
            }
            SessionInput::Disconnected { .. } => {
                self.live = false;
                self.subscription_acknowledged = false;
                self.heartbeats.clear();
                self.pending_candles.clear();
                for book in self.books.values_mut() {
                    book.sync.invalidate("disconnected");
                }
                if !self.cfg.candle_intervals.is_empty() {
                    output.push(SessionAction::CancelTimer(CANDLE_TIMER_ID));
                }
                output.push(SessionAction::CancelTimer(HEARTBEAT_TIMER_ID));
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
                    detail: "coinbase unexpected binary".into(),
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
                        detail: http_error_detail(
                            "coinbase candles",
                            response.status,
                            &response.body,
                        ),
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
                            detail: "bad coinbase candles body".into(),
                        }));
                    }
                }
                Ok(())
            }
            SessionInput::Pong { .. } => Ok(()),
            SessionInput::Timer { timer_id, now } => {
                if timer_id == CANDLE_TIMER_ID {
                    self.poll_candles_all(output);
                    self.schedule_candle_timer(now, output);
                }
                if timer_id == HEARTBEAT_TIMER_ID {
                    self.live = false;
                    output.push(SessionAction::EmitSystem(SystemEvent::HeartbeatMissed));
                    output.push(SessionAction::Reconnect(
                        marketfeed_adapter_api::ReconnectReason::Heartbeat,
                    ));
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
