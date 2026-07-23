//! OKX SessionMachine — shared by Spot/SWAP/Futures: trades/tickers, optional
//! `books` L2 via WS snapshot, and (derivatives) mark-price/index-tickers/
//! funding-rate/open-interest/liquidation-orders.

use std::collections::HashMap;

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, CandleInterval, EventBatch, ReconnectReason, SessionAction,
    SessionInput, SessionMachine, SessionSpec, StopReason, TimerSpec,
};
use marketfeed_book::{BookSynchronizer, OrderBook, SyncLimits, SyncState};
use marketfeed_model::{
    BookChange, BookDelta, BookSide, BookSnapshot, Candle, CatalogView, ConnectionId,
    EventEnvelope, EventFlags, FrameStamp, Funding, InstrumentId, Liquidation, MarketEvent,
    OpenInterest, Price, PricePoint, Quantity, Quote, SequenceRange, SessionId, Statistics24h,
    SystemEvent, TimestampNs, Trade, VenueId,
};

use crate::messages::{
    DecodedEvent, candle_channel, decode_text, level_op, to_book_levels, trade_id_source,
    trade_id_u64,
};
use crate::specification::{
    BUSINESS_WS_URL, OKX_FUTURES_VENUE_ID, OKX_SPOT_VENUE_ID, OKX_SWAP_VENUE_ID, PING_INTERVAL_MS,
    PING_TIMER_ID,
};

const SCHEMA_VERSION: u16 = 1;
/// OKX `books` channel depth.
const BOOKS_DEPTH: u32 = 400;

#[derive(Debug, Clone)]
pub struct OkxSessionConfig {
    pub symbols: Vec<String>,
    pub instrument_ids: HashMap<String, InstrumentId>,
    pub connection: ConnectionId,
    pub session: SessionId,
    pub enable_l2: bool,
    pub price_scale: u8,
    pub qty_scale: u8,
    /// Envelope venue id; distinguishes okx-spot/okx-swap/okx-futures sessions.
    pub venue: VenueId,
    /// Subscribe mark/index/funding/OI/liquidation channels (SWAP/Futures only).
    pub subscribe_mark_funding: bool,
    /// Native candle channels (`candle1m`, …). Empty = no candles.
    pub candle_intervals: Vec<CandleInterval>,
}

impl Default for OkxSessionConfig {
    fn default() -> Self {
        let mut instrument_ids = HashMap::new();
        instrument_ids.insert("BTC-USDT".into(), InstrumentId(1));
        Self {
            symbols: vec!["BTC-USDT".into()],
            instrument_ids,
            connection: ConnectionId(1),
            session: SessionId(1),
            enable_l2: false,
            // BTC-USDT tickSz=0.1, lotSz=1e-8
            price_scale: 1,
            qty_scale: 8,
            venue: OKX_SPOT_VENUE_ID,
            subscribe_mark_funding: false,
            candle_intervals: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct SymbolBook {
    sync: BookSynchronizer,
}

/// Derive the `index-tickers` instId (underlying pair) from a SWAP/Futures
/// native symbol, e.g. `"BTC-USDT-SWAP"` / `"BTC-USDT-250328"` -> `"BTC-USDT"`.
///
/// ponytail: relies on OKX's `BASE-QUOTE[-SUFFIX]` naming instead of plumbing
/// the `uly` field through the catalog; ceiling = wrong index id for exotic
/// underlyings; upgrade = carry `uly` in `OkxSessionConfig` per symbol.
fn underlying_index_id(symbol: &str) -> String {
    let mut parts = symbol.split('-');
    match (parts.next(), parts.next()) {
        (Some(base), Some(quote)) => format!("{base}-{quote}"),
        _ => symbol.to_string(),
    }
}

/// OKX `liquidation-orders` is an instType firehose (SWAP / FUTURES), not per-instId.
fn derivative_inst_type(venue: VenueId) -> Option<&'static str> {
    if venue == OKX_SWAP_VENUE_ID {
        Some("SWAP")
    } else if venue == OKX_FUTURES_VENUE_ID {
        Some("FUTURES")
    } else {
        None
    }
}

#[derive(Debug)]
pub struct OkxSession {
    #[allow(dead_code)]
    catalog: CatalogView,
    cfg: OkxSessionConfig,
    frame_seq: u64,
    books: HashMap<String, SymbolBook>,
    live: bool,
    business_endpoint: bool,
    #[allow(dead_code)]
    subscribed: bool,
}

impl OkxSession {
    pub fn new(spec: SessionSpec, catalog: CatalogView, cfg: OkxSessionConfig) -> Self {
        let business_endpoint = spec.endpoint_name == BUSINESS_WS_URL;
        let mut books = HashMap::new();
        if cfg.enable_l2 {
            for (sym, id) in &cfg.instrument_ids {
                let book = OrderBook::new(cfg.price_scale, cfg.qty_scale, Some(BOOKS_DEPTH));
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
            live: false,
            business_endpoint,
            subscribed: false,
        }
    }

    fn next_frame(&mut self) -> u64 {
        self.frame_seq += 1;
        self.frame_seq
    }

    fn instrument_for(&self, inst_id: &str) -> Option<InstrumentId> {
        self.cfg.instrument_ids.get(inst_id).copied()
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
            venue: self.cfg.venue,
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

    fn schedule_ping(&self, now: TimestampNs, output: &mut ActionBuffer) {
        output.push(SessionAction::ScheduleTimer(TimerSpec {
            timer_id: PING_TIMER_ID,
            fire_at: TimestampNs(
                now.0
                    .saturating_add(PING_INTERVAL_MS.saturating_mul(1_000_000)),
            ),
        }));
    }

    /// Non-zero checksum after OKX deprecation is unexpected; fail closed.
    fn reject_legacy_checksum(&mut self, inst_id: &str, checksum: i64, output: &mut ActionBuffer) {
        if let Some(book) = self.books.get_mut(inst_id) {
            let instrument = book.sync.instrument;
            book.sync.invalidate("legacy checksum non-zero");
            output.push(SessionAction::EmitSystem(SystemEvent::ChecksumMismatch {
                detail: format!("okx books checksum={checksum} (deprecated field must be 0)"),
            }));
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument,
                reason: "legacy checksum non-zero".into(),
            }));
            output.push(SessionAction::ResyncInstrument(instrument));
        } else {
            output.push(SessionAction::EmitSystem(SystemEvent::ChecksumMismatch {
                detail: format!("okx books checksum={checksum} (deprecated field must be 0)"),
            }));
        }
        output.push(SessionAction::Reconnect(ReconnectReason::ChecksumMismatch));
    }

    fn subscribe_message(&self) -> Bytes {
        let mut args = Vec::new();
        for s in &self.cfg.symbols {
            if self.business_endpoint {
                for interval in &self.cfg.candle_intervals {
                    args.push(serde_json::json!({
                        "channel": candle_channel(*interval),
                        "instId": s
                    }));
                }
            } else {
                args.push(serde_json::json!({"channel": "trades", "instId": s}));
                args.push(serde_json::json!({"channel": "tickers", "instId": s}));
                if self.cfg.enable_l2 {
                    args.push(serde_json::json!({"channel": "books", "instId": s}));
                }
                if self.cfg.subscribe_mark_funding {
                    args.push(serde_json::json!({"channel": "mark-price", "instId": s}));
                    args.push(serde_json::json!({
                        "channel": "index-tickers",
                        "instId": underlying_index_id(s)
                    }));
                    args.push(serde_json::json!({"channel": "funding-rate", "instId": s}));
                    args.push(serde_json::json!({"channel": "open-interest", "instId": s}));
                }
            }
        }
        // instType firehose (not per-instId): one arg covers all instruments of the segment.
        if !self.business_endpoint && self.cfg.subscribe_mark_funding {
            if let Some(inst_type) = derivative_inst_type(self.cfg.venue) {
                args.push(serde_json::json!({
                    "channel": "liquidation-orders",
                    "instType": inst_type
                }));
            }
        }
        let body = serde_json::json!({
            "id": "1",
            "op": "subscribe",
            "args": args
        });
        Bytes::from(body.to_string())
    }

    fn handle_decoded(
        &mut self,
        decoded: DecodedEvent,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match decoded {
            DecodedEvent::Ping => {
                output.push(SessionAction::SendText(Bytes::from_static(b"pong")));
            }
            DecodedEvent::Pong => {}
            DecodedEvent::Trade {
                inst_id,
                trade_id,
                price,
                quantity,
                aggressor,
                exchange_ts_ms,
                seq_id,
            } => {
                let instrument = self.instrument_for(&inst_id);
                let frame_seq = self.next_frame();
                let seq = seq_id
                    .or_else(|| trade_id_u64(&trade_id))
                    .map(|id| SequenceRange {
                        first: id,
                        last: id,
                    });
                let env = self.envelope(
                    instrument,
                    frame_seq,
                    0,
                    received,
                    Some(exchange_ts_ms),
                    seq,
                    EventFlags::empty(),
                    MarketEvent::Trade(Trade {
                        price,
                        quantity,
                        aggressor,
                        trade_id: Some(trade_id_source(&trade_id)),
                    }),
                );
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events: vec![env],
                }));
            }
            DecodedEvent::Quote {
                inst_id,
                bid_price,
                bid_qty,
                ask_price,
                ask_qty,
                exchange_ts_ms,
                open,
                high,
                low,
                close,
                volume,
                quote_volume,
            } => {
                let instrument = self.instrument_for(&inst_id);
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
                inst_id,
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
                let instrument = self.instrument_for(&inst_id);
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
            DecodedEvent::BookSnapshot {
                inst_id,
                bids,
                asks,
                seq_id,
                exchange_ts_ms,
                checksum,
            } => {
                if checksum != 0 {
                    self.reject_legacy_checksum(&inst_id, checksum, output);
                    return Ok(());
                }
                self.apply_book_snapshot(
                    &inst_id,
                    &bids,
                    &asks,
                    seq_id,
                    exchange_ts_ms,
                    received,
                    output,
                )?;
            }
            DecodedEvent::BookUpdate {
                inst_id,
                bids,
                asks,
                prev_seq_id,
                seq_id,
                exchange_ts_ms,
                checksum,
            } => {
                if checksum != 0 {
                    self.reject_legacy_checksum(&inst_id, checksum, output);
                    return Ok(());
                }
                self.apply_book_update(
                    &inst_id,
                    &bids,
                    &asks,
                    prev_seq_id,
                    seq_id,
                    exchange_ts_ms,
                    received,
                    output,
                )?;
            }
            DecodedEvent::MarkPrice {
                inst_id,
                mark_px,
                exchange_ts_ms,
            } => {
                let instrument = self.instrument_for(&inst_id);
                let frame_seq = self.next_frame();
                let env = self.envelope(
                    instrument,
                    frame_seq,
                    0,
                    received,
                    Some(exchange_ts_ms),
                    None,
                    EventFlags::empty(),
                    MarketEvent::MarkPrice(PricePoint { price: mark_px }),
                );
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events: vec![env],
                }));
            }
            DecodedEvent::IndexPrice {
                inst_id,
                idx_px,
                exchange_ts_ms,
            } => {
                // Index instId is the underlying pair, not the SWAP/Futures symbol;
                // route it back to the derivative instrument(s) sharing that index.
                let instrument = self.instrument_for(&inst_id).or_else(|| {
                    self.cfg
                        .symbols
                        .iter()
                        .find(|s| underlying_index_id(s) == inst_id)
                        .and_then(|s| self.instrument_for(s))
                });
                let frame_seq = self.next_frame();
                let env = self.envelope(
                    instrument,
                    frame_seq,
                    0,
                    received,
                    Some(exchange_ts_ms),
                    None,
                    EventFlags::empty(),
                    MarketEvent::IndexPrice(PricePoint { price: idx_px }),
                );
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events: vec![env],
                }));
            }
            DecodedEvent::Funding {
                inst_id,
                rate,
                next_funding_ts_ms,
                exchange_ts_ms,
            } => {
                let instrument = self.instrument_for(&inst_id);
                let frame_seq = self.next_frame();
                let env = self.envelope(
                    instrument,
                    frame_seq,
                    0,
                    received,
                    Some(exchange_ts_ms),
                    None,
                    EventFlags::empty(),
                    MarketEvent::Funding(Funding {
                        rate,
                        next_funding_ts: next_funding_ts_ms
                            .map(|ms| TimestampNs(ms.saturating_mul(1_000_000))),
                    }),
                );
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events: vec![env],
                }));
            }
            DecodedEvent::OpenInterest {
                inst_id,
                quantity,
                exchange_ts_ms,
            } => {
                let instrument = self.instrument_for(&inst_id);
                let frame_seq = self.next_frame();
                let env = self.envelope(
                    instrument,
                    frame_seq,
                    0,
                    received,
                    Some(exchange_ts_ms),
                    None,
                    EventFlags::empty(),
                    MarketEvent::OpenInterest(OpenInterest { quantity }),
                );
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events: vec![env],
                }));
            }
            DecodedEvent::Liquidations(rows) => {
                if rows.is_empty() {
                    return Ok(());
                }
                let frame_seq = self.next_frame();
                let mut events = Vec::with_capacity(rows.len());
                for (i, row) in rows.into_iter().enumerate() {
                    let instrument = self.instrument_for(&row.inst_id);
                    events.push(self.envelope(
                        instrument,
                        frame_seq,
                        i as u16,
                        received,
                        Some(row.exchange_ts_ms),
                        None,
                        EventFlags::empty(),
                        MarketEvent::Liquidation(Liquidation {
                            price: row.price,
                            quantity: row.quantity,
                            side: row.side,
                        }),
                    ));
                }
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events,
                }));
            }
            DecodedEvent::SubscribeAck { .. } => {
                self.subscribed = true;
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
            DecodedEvent::Error { code, msg } => {
                output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                    detail: format!("okx error code={code:?} msg={msg:?}"),
                }));
                output.push(SessionAction::Reconnect(ReconnectReason::Protocol));
            }
            DecodedEvent::Unknown => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "okx".into(),
                }));
            }
        }
        Ok(())
    }

    fn apply_book_snapshot(
        &mut self,
        inst_id: &str,
        bids: &[(Price, Quantity)],
        asks: &[(Price, Quantity)],
        seq_id: u64,
        exchange_ts_ms: i64,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if !self.cfg.enable_l2 {
            return Ok(());
        }
        let Some(book) = self.books.get_mut(inst_id) else {
            return Ok(());
        };
        book.sync.begin_resync();
        book.sync.request_snapshot();
        if let Err(err) = book.sync.book.apply_snapshot(bids, asks, Some(seq_id)) {
            let instrument = book.sync.instrument;
            book.sync.invalidate(&err.to_string());
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument,
                reason: err.to_string(),
            }));
            output.push(SessionAction::ResyncInstrument(instrument));
            output.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
            return Ok(());
        }
        book.sync.state = SyncState::Live;
        book.sync.expected_sequence = Some(seq_id);
        let instrument = book.sync.instrument;

        let frame_seq = self.next_frame();
        let env = self.envelope(
            Some(instrument),
            frame_seq,
            0,
            received,
            Some(exchange_ts_ms),
            Some(SequenceRange {
                first: seq_id,
                last: seq_id,
            }),
            EventFlags::SNAPSHOT,
            MarketEvent::BookSnapshot(BookSnapshot {
                bids: to_book_levels(BookSide::Bid, bids),
                asks: to_book_levels(BookSide::Ask, asks),
                depth: Some(BOOKS_DEPTH),
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
        if !self.live {
            self.live = true;
            output.push(SessionAction::MarkLive);
        }
        Ok(())
    }

    fn apply_book_update(
        &mut self,
        inst_id: &str,
        bids: &[(Price, Quantity)],
        asks: &[(Price, Quantity)],
        prev_seq_id: u64,
        seq_id: u64,
        exchange_ts_ms: i64,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if !self.cfg.enable_l2 {
            return Ok(());
        }
        let Some(book) = self.books.get_mut(inst_id) else {
            return Ok(());
        };
        // ponytail: drop pre-snapshot updates; OKX sends snapshot first after subscribe.
        if book.sync.state != SyncState::Live {
            return Ok(());
        }
        let last = book.sync.book.sequence().unwrap_or(0);
        if prev_seq_id != last {
            let instrument = book.sync.instrument;
            book.sync.note_gap();
            output.push(SessionAction::EmitSystem(SystemEvent::SequenceGap {
                expected: last,
                actual: prev_seq_id,
            }));
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument,
                reason: "books prevSeqId gap".into(),
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
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument,
                reason: format!("live delta apply failed: {err}"),
            }));
            output.push(SessionAction::ResyncInstrument(instrument));
            output.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
            return Ok(());
        }
        book.sync.book.set_sequence(seq_id);
        book.sync.expected_sequence = Some(seq_id);

        let instrument = self.instrument_for(inst_id);
        let frame_seq = self.next_frame();
        let env = self.envelope(
            instrument,
            frame_seq,
            0,
            received,
            Some(exchange_ts_ms),
            Some(SequenceRange {
                first: prev_seq_id,
                last: seq_id,
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
}

impl SessionMachine for OkxSession {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Connected { now } => {
                self.live = false;
                self.subscribed = false;
                for book in self.books.values_mut() {
                    book.sync.begin_resync();
                    book.sync.request_snapshot();
                }
                output.push(SessionAction::SendText(self.subscribe_message()));
                self.schedule_ping(now, output);
                output.push(SessionAction::EmitSystem(
                    SystemEvent::ConnectionStateChanged {
                        state: "connected".into(),
                    },
                ));
                if !self.cfg.enable_l2 {
                    self.live = true;
                    output.push(SessionAction::MarkLive);
                }
                Ok(())
            }
            SessionInput::Disconnected { .. } => {
                self.live = false;
                self.subscribed = false;
                for book in self.books.values_mut() {
                    book.sync.invalidate("disconnected");
                }
                output.push(SessionAction::CancelTimer(PING_TIMER_ID));
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
            SessionInput::Pong { .. } => Ok(()),
            SessionInput::HttpResponse { .. } => {
                // ponytail: L2 sync is WS snapshot/update only; REST depth unused in v1.
                Ok(())
            }
            SessionInput::Timer { timer_id, now } => {
                if timer_id == PING_TIMER_ID {
                    output.push(SessionAction::SendText(Bytes::from_static(b"ping")));
                    self.schedule_ping(now, output);
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
                            // Re-subscribe path: reconnect so OKX re-sends books snapshot.
                            output.push(SessionAction::Reconnect(ReconnectReason::Control));
                        }
                    }
                    _ => {}
                }
                Ok(())
            }
        }
    }
}
