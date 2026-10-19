//! Bybit V5 book reconstruction in observed order; source continuity is unknown.

use std::collections::{BTreeMap, BTreeSet, HashMap};

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
    BookDecoded, DecodedEvent, OrderbookKind, kline_topic_interval, level_op, to_book_levels,
    trade_id_source,
};
use crate::specification::{
    BYBIT_INVERSE_VENUE_ID, BYBIT_LINEAR_VENUE_ID, BYBIT_SPOT_VENUE_ID, BybitCategory,
};

const SCHEMA_VERSION: u16 = 1;
const PING_TIMER_ID: u64 = 1;
const PING_INTERVAL_NS: i64 = 20_000_000_000; // 20s
const STARTUP_TIMER_ID: u64 = 2;
const STARTUP_TIMEOUT_NS: i64 = 20_000_000_000;
const MAX_ARGS_PER_SUBSCRIBE: usize = 10;
// Conservative local UTF-8 JSON-byte interpretation of the native connection
// args-character limit. Applied across all requests, not independently per chunk.
const MAX_CONNECTION_ARGS_BYTES: usize = 21_000;

#[derive(Debug)]
struct SubscriptionRequest {
    topics: Vec<String>,
    acknowledged: bool,
    ack_message: Option<String>,
}

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
    last_message: Option<BookDecoded>,
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
    running: bool,
    failed: bool,
    generation: u64,
    requests: BTreeMap<String, SubscriptionRequest>,
    server_connection_id: Option<String>,
    startup_deadline: Option<TimestampNs>,
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
                books.insert(
                    sym.clone(),
                    SymbolBook {
                        sync,
                        last_message: None,
                    },
                );
            }
        }
        Self {
            catalog,
            cfg,
            frame_seq: 0,
            books,
            quotes: HashMap::new(),
            live: false,
            running: false,
            failed: false,
            generation: 0,
            requests: BTreeMap::new(),
            server_connection_id: None,
            startup_deadline: None,
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
        if self.live
            || !self.running
            || self.failed
            || self.requests.is_empty()
            || self.requests.values().any(|request| !request.acknowledged)
            || self.cfg.symbols.iter().any(|symbol| {
                !self
                    .quotes
                    .get(symbol)
                    .is_some_and(|(bid, ask)| bid.is_some() && ask.is_some())
            })
        {
            return;
        }
        if self.cfg.enable_l2 {
            let all_live = self.books.values().all(|b| b.sync.state == SyncState::Live);
            if !all_live {
                return;
            }
        }
        self.live = true;
        self.startup_deadline = None;
        output.push(SessionAction::CancelTimer(STARTUP_TIMER_ID));
        output.push(SessionAction::MarkLive);
    }

    fn subscription_topics(&self) -> Result<Vec<String>, AdapterError> {
        let mut topics = Vec::new();
        let mut seen_symbols = BTreeSet::new();
        if self.cfg.symbols.is_empty()
            || self.cfg.symbols.len() != self.cfg.instrument_ids.len()
            || self.cfg.candle_intervals.len() > 5
        {
            return Err(AdapterError::Protocol(
                "Bybit selected symbols/catalog must match and be nonempty".into(),
            ));
        }
        for symbol in &self.cfg.symbols {
            if symbol.is_empty()
                || symbol.len() > 256
                || symbol.contains('.')
                || symbol.chars().any(|c| c.is_whitespace() || c.is_control())
                || !seen_symbols.insert(symbol)
                || self.instrument_for(symbol).is_none()
            {
                return Err(AdapterError::Protocol(
                    "invalid or duplicate Bybit subscription symbol".into(),
                ));
            }
            topics.push(format!("publicTrade.{symbol}"));
            topics.push(format!("orderbook.1.{symbol}"));
            if self.cfg.enable_l2 {
                if !matches!(self.cfg.l2_depth, 50 | 200 | 1000) {
                    return Err(AdapterError::Protocol("unsupported Bybit L2 depth".into()));
                }
                topics.push(format!("orderbook.{}.{symbol}", self.cfg.l2_depth));
            }
            topics.push(format!("tickers.{symbol}"));
            if !matches!(self.cfg.category, BybitCategory::Spot) {
                topics.push(format!("allLiquidation.{symbol}"));
            }
            for interval in &self.cfg.candle_intervals {
                topics.push(format!(
                    "kline.{}.{symbol}",
                    kline_topic_interval(*interval)
                ));
            }
            if serde_json::to_vec(&topics)
                .map_err(|e| AdapterError::Protocol(e.to_string()))?
                .len()
                > MAX_CONNECTION_ARGS_BYTES
            {
                return Err(AdapterError::Protocol(
                    "Bybit connection args exceed local21000 JSON-byte bound".into(),
                ));
            }
        }
        if topics.iter().collect::<BTreeSet<_>>().len() != topics.len() {
            return Err(AdapterError::Protocol(
                "duplicate native Bybit subscription topic".into(),
            ));
        }
        Ok(topics)
    }

    fn arm_startup(
        &mut self,
        now: TimestampNs,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        let Some(deadline) = now.0.checked_add(STARTUP_TIMEOUT_NS) else {
            self.invalidate_book_state("startup timestamp overflow", output);
            return Err(AdapterError::Protocol(
                "Bybit startup timestamp overflow".into(),
            ));
        };
        self.startup_deadline = Some(TimestampNs(deadline));
        output.push(SessionAction::ScheduleTimer(TimerSpec {
            timer_id: STARTUP_TIMER_ID,
            fire_at: TimestampNs(deadline),
        }));
        Ok(())
    }

    fn startup_expired(&mut self, now: TimestampNs, output: &mut ActionBuffer) -> bool {
        if self
            .startup_deadline
            .is_some_and(|deadline| now.0 >= deadline.0)
            && !self.live
            && !self.failed
        {
            let pending = self
                .requests
                .values()
                .filter(|request| !request.acknowledged)
                .count();
            output.push(SessionAction::EmitSystem(SystemEvent::ParseError { detail: format!("Bybit startup timeout: {pending} pending requests; initial requested BBO/books incomplete") }));
            self.invalidate_book_state("startup readiness deadline expired", output);
            return true;
        }
        false
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
                if trades
                    .iter()
                    .any(|row| self.instrument_for(&row.symbol).is_none())
                {
                    output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                        detail: "trade symbol absent from session catalog".into(),
                    }));
                    return Ok(());
                }
                let frame_seq = self.next_frame();
                let mut events = Vec::with_capacity(trades.len());
                for (i, t) in trades.into_iter().enumerate() {
                    let instrument = self.instrument_for(&t.symbol);
                    events.push(self.envelope(
                        instrument,
                        frame_seq,
                        i as u16,
                        received,
                        Some(t.exchange_ts_ms),
                        None,
                        EventFlags::empty(),
                        MarketEvent::Trade(Trade {
                            provenance: Some(t.provenance),
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
            DecodedEvent::Orderbook(row) => {
                if self.instrument_for(&row.symbol).is_none()
                    || (row.depth != 1 && (!self.cfg.enable_l2 || row.depth != self.cfg.l2_depth))
                {
                    output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                        detail: "unrequested book scope".into(),
                    }));
                    self.invalidate_book_state("unrequested book scope", output);
                } else if row.depth == 1 {
                    self.on_quote_book(&row, received, output)?;
                } else {
                    self.on_l2_book(row, received, output)?;
                }
            }
            DecodedEvent::Candles(rows) => {
                if rows
                    .iter()
                    .any(|row| self.instrument_for(&row.symbol).is_none())
                {
                    output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                        detail: "candle symbol absent from session catalog".into(),
                    }));
                    return Ok(());
                }
                let frame_seq = self.next_frame();
                let mut events = Vec::with_capacity(rows.len());
                for (index, row) in rows.into_iter().enumerate() {
                    // Preserve the native volume and counter-volume. WS docs name
                    // turnover but do not explicitly state category units; do not
                    // promote the REST unit analogy to qualified WS quote volume.
                    let quote_volume = None;
                    events.push(self.envelope(
                        self.instrument_for(&row.symbol),
                        frame_seq,
                        index as u16,
                        received,
                        Some(row.exchange_ts_ms),
                        None,
                        EventFlags::empty(),
                        MarketEvent::Candle(Candle {
                            provenance: Some(row.provenance),
                            native_volume_ccy: row.turnover,
                            is_closed: Some(row.is_closed),
                            quote_volume,
                            open: row.open,
                            high: row.high,
                            low: row.low,
                            close: row.close,
                            volume: row.volume,
                            interval_ns: row.interval_ns,
                            start_ts: row.start_ts,
                        }),
                    ));
                }
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events,
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
            DecodedEvent::Liquidations(batch) => {
                if matches!(self.cfg.category, BybitCategory::Spot)
                    || self.instrument_for(&batch.symbol).is_none()
                    || batch
                        .rows
                        .iter()
                        .any(|row| self.instrument_for(&row.symbol).is_none())
                {
                    output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                        detail:
                            "liquidation product or symbol absent from derivative session catalog"
                                .into(),
                    }));
                    return Ok(());
                }
                let rows = batch.rows;
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
                            provenance: Some(row.provenance),
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
            DecodedEvent::SubscribeAck {
                success,
                req_id,
                conn_id,
                ret_msg,
            } => {
                let request = req_id.as_ref().and_then(|id| self.requests.get(id));
                let valid = self.running
                    && !self.failed
                    && success
                    && request.is_some_and(|request| !request.topics.is_empty())
                    && conn_id.as_ref().is_some_and(|id| !id.is_empty())
                    && self
                        .server_connection_id
                        .as_ref()
                        .is_none_or(|id| Some(id) == conn_id.as_ref())
                    && matches!(ret_msg.as_str(), "" | "subscribe")
                    && request.is_none_or(|request| {
                        request
                            .ack_message
                            .as_ref()
                            .is_none_or(|prior| prior == &ret_msg)
                    });
                if !valid {
                    output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                        detail: format!("uncorrelated, failed or conflicting Bybit subscription response: req={req_id:?}, success={success}, message={ret_msg:?}"),
                    }));
                    self.invalidate_book_state("subscription acknowledgement rejected", output);
                    return Ok(());
                }
                self.server_connection_id = conn_id;
                let request = self
                    .requests
                    .get_mut(req_id.as_ref().expect("validated request"))
                    .expect("validated request");
                if request.acknowledged {
                    return Ok(());
                }
                request.acknowledged = true;
                request.ack_message = Some(ret_msg);
                if self.requests.values().all(|request| request.acknowledged) {
                    output.push(SessionAction::EmitSystem(
                        SystemEvent::SubscriptionStateChanged {
                            state: "subscribed".into(),
                        },
                    ));
                    self.maybe_mark_live(output);
                }
            }
            DecodedEvent::UnexpectedControl { operation } => {
                output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                    detail: format!("unrequested Bybit control operation: {operation}"),
                }));
                self.invalidate_book_state("unexpected native control response", output);
            }
            DecodedEvent::Pong => {}
            DecodedEvent::Unknown => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "bybit".into(),
                }));
                if self.running || self.cfg.enable_l2 || !self.quotes.is_empty() {
                    self.invalidate_book_state(
                        "unknown native frame; book state uncertain",
                        output,
                    );
                }
            }
        }
        Ok(())
    }

    fn invalidate_book_state(&mut self, reason: &str, output: &mut ActionBuffer) {
        if self.failed {
            return;
        }
        self.failed = true;
        self.live = false;
        self.startup_deadline = None;
        output.push(SessionAction::CancelTimer(STARTUP_TIMER_ID));
        output.push(SessionAction::CancelTimer(PING_TIMER_ID));
        output.push(SessionAction::MarkDegraded);
        for book in self.books.values_mut() {
            book.sync.invalidate(reason);
            book.last_message = None;
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument: book.sync.instrument,
                reason: reason.into(),
            }));
        }
        let symbols: Vec<_> = self.quotes.keys().cloned().collect();
        self.quotes.clear();
        for symbol in symbols {
            if let Some(instrument) = self.instrument_for(&symbol) {
                output.push(SessionAction::EmitSystem(SystemEvent::QuoteInvalidated {
                    venue: Some(self.venue_id()),
                    session: Some(self.cfg.session),
                    connection: Some(self.cfg.connection),
                    instrument,
                    reason: reason.into(),
                }));
            }
        }
        output.push(SessionAction::Reconnect(ReconnectReason::Protocol));
    }

    fn on_quote_book(
        &mut self,
        row: &BookDecoded,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        // This native channel is snapshot-only; an unknown partial form cannot
        // refresh a BBO assembled from uncertain state.
        if row.kind != OrderbookKind::Snapshot && row.update_id != 1 {
            self.invalidate_book_state("unexpected level1 delta", output);
            return Ok(());
        }
        let bid = row.bids.first().copied();
        let ask = row.asks.first().copied();
        self.quotes.insert(row.symbol.clone(), (bid, ask));
        let instrument = self.instrument_for(&row.symbol);
        let (Some((bid_price, bid_qty)), Some((ask_price, ask_qty))) = (bid, ask) else {
            if self.live {
                self.live = false;
                output.push(SessionAction::MarkDegraded);
                self.arm_startup(received.receive_ts, output)?;
            }
            if let Some(instrument) = instrument {
                output.push(SessionAction::EmitSystem(SystemEvent::QuoteInvalidated {
                    venue: Some(self.venue_id()),
                    session: Some(self.cfg.session),
                    connection: Some(self.cfg.connection),
                    instrument,
                    reason: "native quote side unavailable".into(),
                }));
            }
            return Ok(());
        };
        if !bid_price.0.numeric_cmp(ask_price.0).is_lt() {
            self.invalidate_book_state("crossed native quote", output);
            return Ok(());
        }
        let frame_seq = self.next_frame();
        let event = self.envelope(
            instrument,
            frame_seq,
            0,
            received,
            Some(row.exchange_ts_ms),
            None,
            EventFlags::SNAPSHOT,
            MarketEvent::Quote(Quote {
                provenance: Some(row.provenance.clone()),
                bid_price,
                bid_quantity: Some(bid_qty),
                ask_price,
                ask_quantity: Some(ask_qty),
            }),
        );
        output.push(SessionAction::EmitBatch(EventBatch {
            session: self.cfg.session,
            frame_seq,
            events: vec![event],
        }));
        self.maybe_mark_live(output);
        Ok(())
    }

    fn on_l2_book(
        &mut self,
        row: BookDecoded,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        let snapshot = row.kind == OrderbookKind::Snapshot || row.update_id == 1;
        let Some(book) = self.books.get_mut(&row.symbol) else {
            return Ok(());
        };
        if !snapshot && book.sync.state != SyncState::Live {
            return Ok(());
        }
        if !snapshot {
            if book.last_message.as_ref() == Some(&row) {
                return Ok(());
            } // exact repeat; original frame remains durable
            let anomaly = book.last_message.as_ref().is_none_or(|previous| {
                row.update_id <= previous.update_id
                    || previous
                        .seq
                        .zip(row.seq)
                        .is_some_and(|(old, new)| new < old)
            });
            if anomaly {
                self.invalidate_book_state(
                    "native book ordering anomaly or conflicting repeat",
                    output,
                );
                return Ok(());
            }
        }
        let mut changes = Vec::new();
        if snapshot {
            book.sync.begin_resync();
            book.sync.request_snapshot();
            if let Err(error) =
                book.sync
                    .book
                    .apply_snapshot(&row.bids, &row.asks, Some(row.update_id))
            {
                self.invalidate_book_state(
                    &format!("native snapshot apply failed: {error}"),
                    output,
                );
                return Ok(());
            }
        } else {
            for (side, levels) in [(BookSide::Bid, &row.bids), (BookSide::Ask, &row.asks)] {
                for (price, quantity) in levels {
                    let (operation, quantity) = level_op(*quantity);
                    changes.push(BookChange {
                        side,
                        operation,
                        price: *price,
                        quantity,
                    });
                }
            }
            if let Err(error) = book.sync.book.apply_changes_atomic(&changes) {
                self.invalidate_book_state(&format!("native delta apply failed: {error}"), output);
                return Ok(());
            }
            book.sync.book.set_sequence(row.update_id);
        }
        book.sync.state = SyncState::Live;
        book.sync.expected_sequence = None; // no native +1 contract
        book.last_message = Some(row.clone());
        let instrument = book.sync.instrument;
        let payload = if snapshot {
            // Consumers clear their old book on this lifecycle action; notify
            // before the replacement snapshot, not after it.
            output.push(SessionAction::EmitSystem(SystemEvent::BookResynchronized {
                instrument,
            }));
            MarketEvent::BookSnapshot(BookSnapshot {
                provenance: Some(row.provenance),
                bids: to_book_levels(BookSide::Bid, &row.bids),
                asks: to_book_levels(BookSide::Ask, &row.asks),
                depth: Some(row.depth),
                checksum: None,
            })
        } else {
            MarketEvent::BookDelta(BookDelta {
                provenance: Some(row.provenance),
                changes,
                checksum: None,
            })
        };
        let frame_seq = self.next_frame();
        let event = self.envelope(
            Some(instrument),
            frame_seq,
            0,
            received,
            Some(row.exchange_ts_ms),
            None,
            if snapshot {
                EventFlags::SNAPSHOT
            } else {
                EventFlags::DELTA
            },
            payload,
        );
        output.push(SessionAction::EmitBatch(EventBatch {
            session: self.cfg.session,
            frame_seq,
            events: vec![event],
        }));
        self.maybe_mark_live(output);
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
                // A new physical connection revokes prior readiness even if a
                // caller did not first deliver Disconnected.
                output.push(SessionAction::MarkDegraded);
                self.running = false;
                self.failed = true;
                self.requests.clear();
                self.startup_deadline = None;
                self.server_connection_id = None;
                self.quotes.clear();
                for book in self.books.values_mut() {
                    book.last_message = None;
                    book.sync.begin_resync();
                    book.sync.request_snapshot();
                }
                let args = self.subscription_topics()?;
                let generation = self.generation.checked_add(1).ok_or_else(|| {
                    AdapterError::Protocol("Bybit request generation overflow".into())
                })?;
                self.arm_startup(now, output)?;
                self.generation = generation;
                self.running = true;
                self.failed = false;
                for (index, chunk) in args.chunks(MAX_ARGS_PER_SUBSCRIBE).enumerate() {
                    let req_id = format!("b{generation:016x}-{index:04x}");
                    self.requests.insert(
                        req_id.clone(),
                        SubscriptionRequest {
                            topics: chunk.to_vec(),
                            acknowledged: false,
                            ack_message: None,
                        },
                    );
                    let body = serde_json::json!({"op":"subscribe", "req_id":req_id,"args":chunk});
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
                Ok(())
            }
            SessionInput::Disconnected { .. } => {
                self.live = false;
                self.running = false;
                self.failed = true;
                self.requests.clear();
                self.server_connection_id = None;
                self.startup_deadline = None;
                output.push(SessionAction::MarkDegraded);
                output.push(SessionAction::CancelTimer(STARTUP_TIMER_ID));
                for book in self.books.values_mut() {
                    book.sync.invalidate("disconnected");
                    book.last_message = None;
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
                if !self.running || self.failed || self.startup_expired(received.receive_ts, output)
                {
                    return Ok(());
                }
                match crate::messages::decode_text(bytes) {
                    Ok(decoded) => self.handle_decoded(decoded, received, output),
                    Err(e) => {
                        output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                            detail: e,
                        }));
                        // A malformed frame cannot be proved unrelated to the
                        // book; fail closed rather than applying later deltas on
                        // potentially missing state.
                        if self.running || self.cfg.enable_l2 || !self.quotes.is_empty() {
                            self.invalidate_book_state(
                                "native decode failure; book state uncertain",
                                output,
                            );
                        }
                        Ok(())
                    }
                }
            }
            SessionInput::HttpResponse { .. } => Ok(()),
            SessionInput::BinaryFrame { .. } => {
                if !self.running || self.failed {
                    return Ok(());
                }
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "binary".into(),
                }));
                if self.running || self.cfg.enable_l2 || !self.quotes.is_empty() {
                    self.invalidate_book_state(
                        "unexpected binary frame; book state uncertain",
                        output,
                    );
                }
                Ok(())
            }
            SessionInput::Pong { .. } => Ok(()),
            SessionInput::Timer { timer_id, now } => {
                if !self.running || self.failed || self.startup_expired(now, output) {
                    return Ok(());
                }
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
                        self.live = false;
                        self.running = false;
                        self.failed = true;
                        self.startup_deadline = None;
                        output.push(SessionAction::MarkDegraded);
                        output.push(SessionAction::CancelTimer(STARTUP_TIMER_ID));
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
                            book.last_message = None;
                            self.live = false;
                            self.failed = true;
                            self.startup_deadline = None;
                            output.push(SessionAction::MarkDegraded);
                            output.push(SessionAction::CancelTimer(STARTUP_TIMER_ID));
                            output.push(SessionAction::CancelTimer(PING_TIMER_ID));
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
