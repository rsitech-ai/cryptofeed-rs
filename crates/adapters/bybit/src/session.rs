//! Bybit V5 SessionMachine with WS orderbook snapshot + contiguous `u` sync.

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
    SystemEvent, TimestampNs, Trade,
};

use crate::messages::{
    DecodedEvent, OrderbookKind, kline_topic_interval, level_op, to_book_levels, trade_id_source,
};
use crate::specification::{
    BYBIT_INVERSE_VENUE_ID, BYBIT_LINEAR_VENUE_ID, BYBIT_SPOT_VENUE_ID, BybitCategory,
};

const SCHEMA_VERSION: u16 = 1;
const PING_TIMER_ID: u64 = 1;
const PING_INTERVAL_NS: i64 = 20_000_000_000; // 20s

#[derive(Debug, Clone)]
pub struct BybitSessionConfig {
    pub category: BybitCategory,
    pub symbols: Vec<String>,
    pub instrument_ids: HashMap<String, InstrumentId>,
    pub connection: ConnectionId,
    pub session: SessionId,
    pub enable_l2: bool,
    pub l2_depth: u32,
    pub price_scale: u8,
    pub qty_scale: u8,
    /// Exact catalog scales per native symbol. The scalar fields remain the
    /// single-symbol fallback for programmatic/test configurations.
    pub book_scales: HashMap<String, (u8, u8)>,
    /// Native `kline.{interval}.{symbol}` subscriptions. Empty = no candles.
    pub candle_intervals: Vec<CandleInterval>,
}

impl Default for BybitSessionConfig {
    fn default() -> Self {
        let mut instrument_ids = HashMap::new();
        instrument_ids.insert("BTCUSDT".into(), InstrumentId(1));
        Self {
            category: BybitCategory::Linear,
            symbols: vec!["BTCUSDT".into()],
            instrument_ids,
            connection: ConnectionId(1),
            session: SessionId(1),
            enable_l2: false,
            l2_depth: 50,
            price_scale: 2,
            qty_scale: 3,
            book_scales: HashMap::new(),
            candle_intervals: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct SymbolBook {
    sync: BookSynchronizer,
    last_u: Option<u64>,
}

#[derive(Debug)]
pub struct BybitSession {
    #[allow(dead_code)]
    catalog: CatalogView,
    cfg: BybitSessionConfig,
    frame_seq: u64,
    books: HashMap<String, SymbolBook>,
    /// Best bid/ask from orderbook.1 for quote emission.
    quotes: HashMap<String, (Option<(Price, Quantity)>, Option<(Price, Quantity)>)>,
    live: bool,
}

impl BybitSession {
    pub fn new(spec: SessionSpec, catalog: CatalogView, cfg: BybitSessionConfig) -> Self {
        let _ = spec;
        let mut books = HashMap::new();
        if cfg.enable_l2 {
            for (sym, id) in &cfg.instrument_ids {
                let (price_scale, qty_scale) = cfg
                    .book_scales
                    .get(sym)
                    .copied()
                    .unwrap_or((cfg.price_scale, cfg.qty_scale));
                let book = OrderBook::new(price_scale, qty_scale, Some(cfg.l2_depth));
                let mut sync = BookSynchronizer::new(*id, book, SyncLimits::default());
                sync.request_snapshot();
                books.insert(sym.clone(), SymbolBook { sync, last_u: None });
            }
        }
        Self {
            catalog,
            cfg,
            frame_seq: 0,
            books,
            quotes: HashMap::new(),
            live: false,
        }
    }

    fn venue_id(&self) -> marketfeed_model::VenueId {
        match self.cfg.category {
            BybitCategory::Linear => BYBIT_LINEAR_VENUE_ID,
            BybitCategory::Spot => BYBIT_SPOT_VENUE_ID,
            BybitCategory::Inverse => BYBIT_INVERSE_VENUE_ID,
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
            venue: self.venue_id(),
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
        // Engine fulfills ScheduleTimer → SessionInput::Timer (merged via PR #10).
        // Offline: tests/fixtures.rs covers connect ScheduleTimer + Timer→ping.
        output.push(SessionAction::ScheduleTimer(TimerSpec {
            timer_id: PING_TIMER_ID,
            fire_at: TimestampNs(now.0.saturating_add(PING_INTERVAL_NS)),
        }));
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
            DecodedEvent::Trades(trades) => {
                if trades.is_empty() {
                    return Ok(());
                }
                let frame_seq = self.next_frame();
                let mut events = Vec::with_capacity(trades.len());
                for (i, t) in trades.into_iter().enumerate() {
                    let instrument = self.instrument_for(&t.symbol);
                    let seq = t.seq.map(|s| SequenceRange { first: s, last: s });
                    events.push(self.envelope(
                        instrument,
                        frame_seq,
                        i as u16,
                        received,
                        Some(t.exchange_ts_ms),
                        seq,
                        EventFlags::empty(),
                        MarketEvent::Trade(Trade {
                            price: t.price,
                            quantity: t.quantity,
                            aggressor: t.aggressor,
                            trade_id: Some(trade_id_source(&t.trade_id)),
                        }),
                    ));
                }
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events,
                }));
            }
            DecodedEvent::Orderbook {
                symbol,
                depth,
                kind,
                update_id,
                seq,
                bids,
                asks,
                exchange_ts_ms,
            } => {
                if depth == 1 {
                    self.on_quote_book(
                        &symbol,
                        kind,
                        update_id,
                        seq,
                        &bids,
                        &asks,
                        exchange_ts_ms,
                        received,
                        output,
                    )?;
                } else if self.cfg.enable_l2 && depth == self.cfg.l2_depth {
                    self.on_l2_book(
                        &symbol,
                        kind,
                        update_id,
                        seq,
                        &bids,
                        &asks,
                        exchange_ts_ms,
                        received,
                        output,
                    )?;
                }
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
            DecodedEvent::Tickers {
                symbol,
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
                // Emit only fields present (delta-safe). Spot uses 24h stats;
                // linear/inverse also emit mark/funding/OI when present.
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                let mut events = Vec::new();
                let mut idx = 0u16;
                let mut push = |payload: MarketEvent| {
                    events.push(self.envelope(
                        instrument,
                        frame_seq,
                        idx,
                        received,
                        Some(exchange_ts_ms),
                        None,
                        EventFlags::empty(),
                        payload,
                    ));
                    idx = idx.saturating_add(1);
                };
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
            }
            DecodedEvent::Liquidations(rows) => {
                if rows.is_empty() {
                    return Ok(());
                }
                let frame_seq = self.next_frame();
                let mut events = Vec::with_capacity(rows.len());
                for (i, row) in rows.into_iter().enumerate() {
                    let instrument = self.instrument_for(&row.symbol);
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
            DecodedEvent::SubscribeAck { success } => {
                output.push(SessionAction::EmitSystem(
                    SystemEvent::SubscriptionStateChanged {
                        state: if success {
                            "subscribed".into()
                        } else {
                            "subscribe_failed".into()
                        },
                    },
                ));
                if success && !self.cfg.enable_l2 {
                    self.maybe_mark_live(output);
                }
            }
            DecodedEvent::Pong => {}
            DecodedEvent::Unknown => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "bybit".into(),
                }));
            }
        }
        Ok(())
    }

    fn on_quote_book(
        &mut self,
        symbol: &str,
        kind: OrderbookKind,
        update_id: u64,
        seq: Option<u64>,
        bids: &[(Price, Quantity)],
        asks: &[(Price, Quantity)],
        exchange_ts_ms: i64,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        let entry = self.quotes.entry(symbol.to_string()).or_default();
        match kind {
            OrderbookKind::Snapshot => {
                entry.0 = bids.first().copied();
                entry.1 = asks.first().copied();
            }
            OrderbookKind::Delta => {
                for (price, qty) in bids {
                    if qty.0.coefficient == 0 {
                        if entry.0.map(|(p, _)| p) == Some(*price) {
                            entry.0 = None;
                        }
                    } else {
                        // ponytail: top-of-book only; full L1 ladder if multi-level orderbook.1 needed
                        entry.0 = Some((*price, *qty));
                    }
                }
                for (price, qty) in asks {
                    if qty.0.coefficient == 0 {
                        if entry.1.map(|(p, _)| p) == Some(*price) {
                            entry.1 = None;
                        }
                    } else {
                        entry.1 = Some((*price, *qty));
                    }
                }
            }
        }
        let (Some((bid_price, bid_qty)), Some((ask_price, ask_qty))) = *entry else {
            return Ok(());
        };
        let instrument = self.instrument_for(symbol);
        let frame_seq = self.next_frame();
        let src = seq.unwrap_or(update_id);
        let env = self.envelope(
            instrument,
            frame_seq,
            0,
            received,
            Some(exchange_ts_ms),
            Some(SequenceRange {
                first: src,
                last: src,
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
        Ok(())
    }

    fn on_l2_book(
        &mut self,
        symbol: &str,
        kind: OrderbookKind,
        update_id: u64,
        seq: Option<u64>,
        bids: &[(Price, Quantity)],
        asks: &[(Price, Quantity)],
        exchange_ts_ms: i64,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        // Venue reset: u==1 means treat as snapshot (Bybit docs).
        let kind = if update_id == 1 {
            OrderbookKind::Snapshot
        } else {
            kind
        };

        match kind {
            OrderbookKind::Snapshot => self.apply_l2_snapshot(
                symbol,
                update_id,
                seq,
                bids,
                asks,
                exchange_ts_ms,
                received,
                output,
            ),
            OrderbookKind::Delta => self.apply_l2_delta(
                symbol,
                update_id,
                seq,
                bids,
                asks,
                exchange_ts_ms,
                received,
                output,
            ),
        }
    }

    fn apply_l2_snapshot(
        &mut self,
        symbol: &str,
        update_id: u64,
        seq: Option<u64>,
        bids: &[(Price, Quantity)],
        asks: &[(Price, Quantity)],
        exchange_ts_ms: i64,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        let Some(book) = self.books.get_mut(symbol) else {
            return Ok(());
        };
        book.sync.begin_resync();
        book.sync.request_snapshot();
        if let Err(err) = book.sync.book.apply_snapshot(bids, asks, Some(update_id)) {
            let instrument = book.sync.instrument;
            book.sync.invalidate(&err.to_string());
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument,
                reason: err.to_string(),
            }));
            output.push(SessionAction::ResyncInstrument(instrument));
            output.push(SessionAction::Reconnect(ReconnectReason::Protocol));
            return Ok(());
        }
        book.sync.state = SyncState::Live;
        book.sync.expected_sequence = Some(update_id + 1);
        book.last_u = Some(update_id);

        let instrument = book.sync.instrument;
        let frame_seq = self.next_frame();
        let src = seq.unwrap_or(update_id);
        let env = self.envelope(
            Some(instrument),
            frame_seq,
            0,
            received,
            Some(exchange_ts_ms),
            Some(SequenceRange {
                first: src,
                last: src,
            }),
            EventFlags::SNAPSHOT,
            MarketEvent::BookSnapshot(BookSnapshot {
                bids: to_book_levels(BookSide::Bid, bids),
                asks: to_book_levels(BookSide::Ask, asks),
                depth: Some(self.cfg.l2_depth),
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

    fn apply_l2_delta(
        &mut self,
        symbol: &str,
        update_id: u64,
        seq: Option<u64>,
        bids: &[(Price, Quantity)],
        asks: &[(Price, Quantity)],
        exchange_ts_ms: i64,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        let Some(book) = self.books.get_mut(symbol) else {
            return Ok(());
        };
        if book.sync.state != SyncState::Live {
            // Waiting for WS/REST snapshot — drop deltas (Bybit pushes snapshot first).
            return Ok(());
        }
        let Some(last_u) = book.last_u else {
            return Ok(());
        };
        if update_id <= last_u {
            return Ok(()); // stale/duplicate
        }
        if update_id != last_u + 1 {
            let instrument = book.sync.instrument;
            book.sync.note_gap();
            book.last_u = None;
            output.push(SessionAction::EmitSystem(SystemEvent::SequenceGap {
                expected: last_u + 1,
                actual: update_id,
            }));
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument,
                reason: "orderbook u gap".into(),
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
            book.last_u = None;
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument,
                reason: format!("live delta apply failed: {err}"),
            }));
            output.push(SessionAction::ResyncInstrument(instrument));
            output.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
            return Ok(());
        }
        book.last_u = Some(update_id);
        book.sync.book.set_sequence(update_id);
        book.sync.expected_sequence = Some(update_id + 1);

        let instrument = self.instrument_for(symbol);
        let frame_seq = self.next_frame();
        let src = seq.unwrap_or(update_id);
        let env = self.envelope(
            instrument,
            frame_seq,
            0,
            received,
            Some(exchange_ts_ms),
            Some(SequenceRange {
                first: src,
                last: src,
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

impl SessionMachine for BybitSession {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Connected { now } => {
                self.live = false;
                let mut args = Vec::new();
                for s in &self.cfg.symbols {
                    args.push(format!("publicTrade.{s}"));
                    args.push(format!("orderbook.1.{s}"));
                    if self.cfg.enable_l2 {
                        args.push(format!("orderbook.{}.{s}", self.cfg.l2_depth));
                    }
                    // Spot: tickers carry 24h stats. Linear/inverse: mark/index/
                    // funding/OI (+ 24h when present). allLiquidation is der-only.
                    args.push(format!("tickers.{s}"));
                    if matches!(
                        self.cfg.category,
                        BybitCategory::Linear | BybitCategory::Inverse
                    ) {
                        args.push(format!("allLiquidation.{s}"));
                    }
                    for interval in &self.cfg.candle_intervals {
                        args.push(format!("kline.{}.{s}", kline_topic_interval(*interval)));
                    }
                }
                // Bybit spot rejects subscribe when args.len() > 10 (`args size >10`).
                // Linear/inverse are safer when chunked the same way.
                const MAX_ARGS_PER_SUBSCRIBE: usize = 10;
                for chunk in args.chunks(MAX_ARGS_PER_SUBSCRIBE.max(1)) {
                    let body = serde_json::json!({
                        "op": "subscribe",
                        "args": chunk
                    });
                    output.push(SessionAction::SendText(Bytes::from(body.to_string())));
                }
                // ScheduleTimer(PING_TIMER_ID) is emitted here on every connect — see
                // tests/fixtures.rs::trade_and_quote_fixtures for the offline assertion.
                self.schedule_ping(now, output);
                output.push(SessionAction::EmitSystem(
                    SystemEvent::ConnectionStateChanged {
                        state: "connected".into(),
                    },
                ));
                if self.cfg.enable_l2 {
                    for book in self.books.values_mut() {
                        book.last_u = None;
                        book.sync.request_snapshot();
                    }
                } else {
                    self.maybe_mark_live(output);
                }
                Ok(())
            }
            SessionInput::Disconnected { .. } => {
                self.live = false;
                for book in self.books.values_mut() {
                    book.sync.invalidate("disconnected");
                    book.last_u = None;
                }
                self.quotes.clear();
                output.push(SessionAction::CancelTimer(PING_TIMER_ID));
                output.push(SessionAction::EmitSystem(
                    SystemEvent::ConnectionStateChanged {
                        state: "disconnected".into(),
                    },
                ));
                Ok(())
            }
            SessionInput::TextFrame { bytes, received } => {
                match crate::messages::decode_text(bytes) {
                    Ok(decoded) => self.handle_decoded(decoded, received, output),
                    Err(e) => {
                        output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                            detail: e,
                        }));
                        Ok(())
                    }
                }
            }
            SessionInput::HttpResponse { .. } => Ok(()),
            SessionInput::BinaryFrame { .. } => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "binary".into(),
                }));
                Ok(())
            }
            SessionInput::Pong { .. } => Ok(()),
            SessionInput::Timer { timer_id, now } => {
                if timer_id == PING_TIMER_ID {
                    output.push(SessionAction::SendText(Bytes::from_static(
                        br#"{"op":"ping"}"#,
                    )));
                    self.schedule_ping(now, output);
                }
                Ok(())
            }
            SessionInput::Control { command } => {
                match command {
                    marketfeed_adapter_api::SessionCommand::Stop => {
                        output.push(SessionAction::CancelTimer(PING_TIMER_ID));
                        output.push(SessionAction::StopSession(StopReason::Control));
                    }
                    marketfeed_adapter_api::SessionCommand::Resync(id) => {
                        // Bybit documents REST `u` parity only with the 1000-level
                        // WebSocket stream. This session may use another depth, so
                        // reconnect and consume the guaranteed fresh WS snapshot
                        // instead of comparing incompatible update-ID domains.
                        // https://bybit-exchange.github.io/docs/v5/market/orderbook
                        if let Some(book) = self
                            .books
                            .values_mut()
                            .find(|book| book.sync.instrument == *id)
                        {
                            book.sync.invalidate("control resync");
                            book.last_u = None;
                            self.live = false;
                            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                                instrument: *id,
                                reason: "control resync requires fresh WebSocket snapshot".into(),
                            }));
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
