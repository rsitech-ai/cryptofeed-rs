//! Gemini Spot SessionMachine — current public trade/book-ticker/depth streams
//! plus REST candles and REST Stats24h.
//! Candles: REST poll `GET /v2/candles/{symbol}/{tf}` on `CANDLE_TIMER_ID`.
//! Stats24h: REST poll `/v2/ticker` (OHLC) + `/v1/pubticker` (volume) on `STATS_TIMER_ID`.
//! # ponytail: candle/stats poll re-emits latest bar/stats each tick (no close-only filter).

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, CandleInterval, Channel, ConcreteSubscriptionSet, EventBatch,
    HttpMethod, HttpRequestSpec, ReconnectReason, SessionAction, SessionInput, SessionMachine,
    SessionSpec, StopReason, TimerSpec,
};
use marketfeed_book::{BookSynchronizer, OrderBook, SyncLimits, SyncState};
use marketfeed_model::{
    BookChange, BookDelta, BookOperation, BookSide, BookSnapshot, Candle, CatalogView,
    ConnectionId, EventEnvelope, EventFlags, FrameStamp, InstrumentId, MarketEvent, Price,
    Quantity, Quote, SequenceRange, SessionId, Statistics24h, SystemEvent, TimestampNs, Trade,
};

use crate::messages::{
    BookLevel, Decoded, candle_time_frame, decode_candles_rest, decode_pubticker_rest, decode_text,
    decode_ticker_rest, trade_id_source,
};
use crate::specification::{
    CANDLE_POLL_INTERVAL_MS, CANDLE_TIMER_ID, CANDLES_REST_BASE, GEMINI_VENUE_ID, REST_BASE,
    STATS_POLL_INTERVAL_MS, STATS_TIMER_ID, TICKER_REST_BASE,
};

const SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum PendingStatsKind {
    TickerV2,
    PubTicker,
}

#[derive(Debug, Clone, Default)]
struct StatsAcc {
    open: Option<Price>,
    high: Option<Price>,
    low: Option<Price>,
    close: Option<Price>,
    volume: Option<Quantity>,
    quote_volume: Option<Quantity>,
}

#[derive(Debug, Clone)]
pub struct GeminiSessionConfig {
    pub symbols: Vec<String>,
    pub instrument_ids: HashMap<String, InstrumentId>,
    pub connection: ConnectionId,
    pub session: SessionId,
    pub enable_l2: bool,
    pub candle_intervals: Vec<CandleInterval>,
    pub poll_stats: bool,
    pub price_scale: u8,
    pub qty_scale: u8,
}

impl Default for GeminiSessionConfig {
    fn default() -> Self {
        let mut instrument_ids = HashMap::new();
        instrument_ids.insert("BTCUSD".into(), InstrumentId(1));
        Self {
            symbols: vec!["BTCUSD".into()],
            instrument_ids,
            connection: ConnectionId(1),
            session: SessionId(1),
            enable_l2: false,
            candle_intervals: Vec::new(),
            poll_stats: true,
            price_scale: 2,
            qty_scale: 8,
        }
    }
}

#[derive(Debug)]
struct SymbolBook {
    sync: BookSynchronizer,
    has_snapshot: bool,
    last_update_id: Option<u64>,
}

#[derive(Debug)]
pub struct GeminiSession {
    #[allow(dead_code)]
    catalog: CatalogView,
    cfg: GeminiSessionConfig,
    subscriptions: ConcreteSubscriptionSet,
    frame_seq: u64,
    books: HashMap<String, SymbolBook>,
    live: bool,
    ws_data_seen: bool,
    ready_candles: HashSet<(String, CandleInterval)>,
    ready_stats: HashSet<(String, PendingStatsKind)>,
    next_http_id: u64,
    pending_candles: HashMap<u64, (String, CandleInterval)>,
    pending_stats: HashMap<u64, (String, PendingStatsKind)>,
    stats_acc: HashMap<String, StatsAcc>,
}

impl GeminiSession {
    pub fn new(spec: SessionSpec, catalog: CatalogView, cfg: GeminiSessionConfig) -> Self {
        let mut books = HashMap::new();
        if cfg.enable_l2 {
            for (sym, id) in &cfg.instrument_ids {
                let requested = spec.subscriptions.items.is_empty()
                    || spec.subscriptions.items.iter().any(|item| {
                        item.instrument == *id && matches!(item.channel, Channel::L2Book { .. })
                    });
                if !requested {
                    continue;
                }
                let book = OrderBook::new(cfg.price_scale, cfg.qty_scale, None);
                let mut sync = BookSynchronizer::new(*id, book, SyncLimits::default());
                sync.request_snapshot();
                books.insert(
                    sym.clone(),
                    SymbolBook {
                        sync,
                        has_snapshot: false,
                        last_update_id: None,
                    },
                );
            }
        }
        Self {
            catalog,
            cfg,
            subscriptions: spec.subscriptions,
            frame_seq: 0,
            books,
            live: false,
            ws_data_seen: false,
            ready_candles: HashSet::new(),
            ready_stats: HashSet::new(),
            next_http_id: 1,
            pending_candles: HashMap::new(),
            pending_stats: HashMap::new(),
            stats_acc: HashMap::new(),
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
        let tf = candle_time_frame(interval);
        let sym = symbol.to_ascii_lowercase();
        output.push(SessionAction::RequestHttp(HttpRequestSpec {
            id,
            method: HttpMethod::Get,
            url: format!("{CANDLES_REST_BASE}/{sym}/{tf}"),
            headers: Vec::new(),
            body: None,
        }));
    }
    fn poll_candles_all(&mut self, output: &mut ActionBuffer) {
        for (symbol, interval) in self.candle_targets() {
            self.request_candle(&symbol, interval, output);
        }
    }
    fn schedule_candle_timer(&self, now: TimestampNs, output: &mut ActionBuffer) {
        if self.candle_targets().is_empty() {
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
    fn request_stats_kind(
        &mut self,
        symbol: &str,
        kind: PendingStatsKind,
        output: &mut ActionBuffer,
    ) {
        let id = self.next_http_id;
        self.next_http_id += 1;
        self.pending_stats.insert(id, (symbol.to_string(), kind));
        let sym = symbol.to_ascii_lowercase();
        let url = match kind {
            PendingStatsKind::TickerV2 => format!("{TICKER_REST_BASE}/{sym}"),
            PendingStatsKind::PubTicker => format!("{REST_BASE}/pubticker/{sym}"),
        };
        output.push(SessionAction::RequestHttp(HttpRequestSpec {
            id,
            method: HttpMethod::Get,
            url,
            headers: Vec::new(),
            body: None,
        }));
    }
    fn poll_stats_all(&mut self, output: &mut ActionBuffer) {
        if !self.cfg.poll_stats {
            return;
        }
        let symbols = self.stats_targets();
        for symbol in &symbols {
            self.request_stats_kind(symbol, PendingStatsKind::TickerV2, output);
            self.request_stats_kind(symbol, PendingStatsKind::PubTicker, output);
        }
    }
    fn schedule_stats_timer(&self, now: TimestampNs, output: &mut ActionBuffer) {
        if self.stats_targets().is_empty() {
            return;
        }
        output.push(SessionAction::ScheduleTimer(TimerSpec {
            timer_id: STATS_TIMER_ID,
            fire_at: TimestampNs(
                now.0
                    .saturating_add(STATS_POLL_INTERVAL_MS.saturating_mul(1_000_000)),
            ),
        }));
    }
    fn merge_stats(
        &mut self,
        symbol: &str,
        open: Option<Price>,
        high: Option<Price>,
        low: Option<Price>,
        close: Option<Price>,
        volume: Option<Quantity>,
        quote_volume: Option<Quantity>,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) {
        let acc = self.stats_acc.entry(symbol.to_string()).or_default();
        if open.is_some() {
            acc.open = open;
        }
        if high.is_some() {
            acc.high = high;
        }
        if low.is_some() {
            acc.low = low;
        }
        if close.is_some() {
            acc.close = close;
        }
        if volume.is_some() {
            acc.volume = volume;
        }
        if quote_volume.is_some() {
            acc.quote_volume = quote_volume;
        }
        let stats = Statistics24h {
            open: acc.open,
            high: acc.high,
            low: acc.low,
            close: acc.close,
            volume: acc.volume,
            quote_volume: acc.quote_volume,
        };
        let instrument = self.instrument_for(symbol);
        let frame_seq = self.next_frame();
        let env = self.envelope(
            instrument,
            frame_seq,
            0,
            received,
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
        symbol: &str,
        candle: Candle,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) {
        let instrument = self.instrument_for(symbol);
        let frame_seq = self.next_frame();
        let start_ts = candle.start_ts;
        let mut env = self.envelope(
            instrument,
            frame_seq,
            0,
            received,
            EventFlags::empty(),
            MarketEvent::Candle(candle),
        );
        env.exchange_ts = Some(start_ts);
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

    fn symbol_for_instrument(&self, instrument: InstrumentId) -> Option<String> {
        self.cfg
            .instrument_ids
            .iter()
            .find_map(|(symbol, id)| (*id == instrument).then(|| symbol.clone()))
    }

    fn candle_targets(&self) -> Vec<(String, CandleInterval)> {
        if self.subscriptions.items.is_empty() {
            return self
                .cfg
                .symbols
                .iter()
                .flat_map(|symbol| {
                    self.cfg
                        .candle_intervals
                        .iter()
                        .map(move |interval| (symbol.clone(), *interval))
                })
                .collect();
        }
        let mut targets = Vec::new();
        for item in &self.subscriptions.items {
            let Channel::Candles { interval } = item.channel else {
                continue;
            };
            if let Some(symbol) = self.symbol_for_instrument(item.instrument) {
                let target = (symbol, interval);
                if !targets.contains(&target) {
                    targets.push(target);
                }
            }
        }
        targets
    }

    fn stats_targets(&self) -> Vec<String> {
        if self.subscriptions.items.is_empty() {
            return if self.cfg.poll_stats {
                self.cfg.symbols.clone()
            } else {
                Vec::new()
            };
        }
        let mut targets = Vec::new();
        for item in &self.subscriptions.items {
            if !matches!(item.channel, Channel::Statistics24h) {
                continue;
            }
            if let Some(symbol) = self.symbol_for_instrument(item.instrument) {
                if !targets.contains(&symbol) {
                    targets.push(symbol);
                }
            }
        }
        targets
    }

    fn envelope(
        &self,
        instrument: Option<InstrumentId>,
        frame_seq: u64,
        event_index: u16,
        received: FrameStamp,
        flags: EventFlags,
        payload: MarketEvent,
    ) -> EventEnvelope {
        EventEnvelope {
            schema_version: SCHEMA_VERSION,
            venue: GEMINI_VENUE_ID,
            instrument,
            connection: self.cfg.connection,
            session: self.cfg.session,
            frame_seq,
            event_index,
            exchange_ts: None,
            receive_ts: received.receive_ts,
            source_sequence: None,
            flags,
            payload,
        }
    }

    fn maybe_mark_live(&mut self, output: &mut ActionBuffer) {
        if self.live {
            return;
        }
        let all_live = self.books.values().all(|b| b.sync.state == SyncState::Live);
        if !all_live {
            return;
        }
        if !self.subscriptions.items.is_empty() {
            let websocket_required = self.subscriptions.items.iter().any(|item| {
                matches!(
                    item.channel,
                    Channel::Trades | Channel::Quote | Channel::L2Book { .. }
                )
            });
            if websocket_required && !self.ws_data_seen {
                return;
            }
            if self
                .candle_targets()
                .iter()
                .any(|target| !self.ready_candles.contains(target))
            {
                return;
            }
            if self.stats_targets().iter().any(|symbol| {
                [PendingStatsKind::TickerV2, PendingStatsKind::PubTicker]
                    .iter()
                    .any(|kind| !self.ready_stats.contains(&(symbol.clone(), *kind)))
            }) {
                return;
            }
        }
        self.live = true;
        output.push(SessionAction::MarkLive);
    }

    fn subscription_streams(&self) -> Result<Vec<String>, AdapterError> {
        if self.subscriptions.items.is_empty() {
            let mut streams = Vec::with_capacity(
                self.cfg
                    .symbols
                    .len()
                    .saturating_mul(if self.cfg.enable_l2 { 3 } else { 2 }),
            );
            for symbol in &self.cfg.symbols {
                let symbol = symbol.to_ascii_lowercase();
                streams.push(format!("{symbol}@trade"));
                streams.push(format!("{symbol}@bookTicker"));
                if self.cfg.enable_l2 {
                    streams.push(format!("{symbol}@depth@100ms"));
                }
            }
            return Ok(streams);
        }

        let mut streams = Vec::new();
        for item in &self.subscriptions.items {
            let symbol = self
                .cfg
                .instrument_ids
                .iter()
                .find_map(|(symbol, id)| (*id == item.instrument).then_some(symbol))
                .ok_or_else(|| {
                    AdapterError::Subscription(format!(
                        "Gemini subscription instrument {} is not configured",
                        item.instrument.0
                    ))
                })?
                .to_ascii_lowercase();
            let stream = match &item.channel {
                Channel::Trades => Some(format!("{symbol}@trade")),
                Channel::Quote => Some(format!("{symbol}@bookTicker")),
                Channel::L2Book { .. } => Some(format!("{symbol}@depth@100ms")),
                Channel::Candles { .. } | Channel::Statistics24h => None,
                unsupported => {
                    return Err(AdapterError::UnsupportedCapability(format!(
                        "Gemini does not support {unsupported:?}"
                    )));
                }
            };
            if let Some(stream) = stream {
                if !streams.contains(&stream) {
                    streams.push(stream);
                }
            }
        }
        Ok(streams)
    }

    fn apply_depth_snapshot(
        &mut self,
        symbol: &str,
        first_update_id: u64,
        last_update_id: u64,
        bids: &[BookLevel],
        asks: &[BookLevel],
        exchange_ts_ns: i64,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if first_update_id != last_update_id {
            self.invalidate_book(symbol, "first Gemini depth frame is not a snapshot", output);
            output.push(SessionAction::Reconnect(ReconnectReason::Protocol));
            return Ok(());
        }

        let snap = {
            let Some(book) = self.books.get_mut(symbol) else {
                return Ok(());
            };
            let bid_levels: Vec<_> = bids
                .iter()
                .map(|level| (level.price, level.quantity))
                .collect();
            let ask_levels: Vec<_> = asks
                .iter()
                .map(|level| (level.price, level.quantity))
                .collect();
            book.sync.begin_resync();
            book.sync.request_snapshot();
            if let Err(err) =
                book.sync
                    .book
                    .apply_snapshot(&bid_levels, &ask_levels, Some(last_update_id))
            {
                let instrument = book.sync.instrument;
                book.sync.invalidate(&err.to_string());
                book.has_snapshot = false;
                book.last_update_id = None;
                output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                    instrument,
                    reason: err.to_string(),
                }));
                output.push(SessionAction::Reconnect(ReconnectReason::Protocol));
                return Ok(());
            }
            book.sync.state = SyncState::Live;
            book.has_snapshot = true;
            book.last_update_id = Some(last_update_id);
            let instrument = book.sync.instrument;
            book.sync
                .book
                .snapshot_levels()
                .map(|levels| (instrument, levels))
        };
        if let Some((instrument, (book_bids, book_asks))) = snap {
            let frame_seq = self.next_frame();
            let mut env = self.envelope(
                Some(instrument),
                frame_seq,
                0,
                received,
                EventFlags::SNAPSHOT,
                MarketEvent::BookSnapshot(BookSnapshot {
                    bids: book_bids,
                    asks: book_asks,
                    depth: None,
                    checksum: None,
                }),
            );
            env.exchange_ts = Some(TimestampNs(exchange_ts_ns));
            env.source_sequence = Some(SequenceRange {
                first: first_update_id,
                last: last_update_id,
            });
            output.push(SessionAction::EmitBatch(EventBatch {
                session: self.cfg.session,
                frame_seq,
                events: vec![env],
            }));
            output.push(SessionAction::EmitSystem(SystemEvent::BookResynchronized {
                instrument,
            }));
        }
        if self
            .books
            .values()
            .all(|book| book.sync.state == SyncState::Live)
        {
            self.ws_data_seen = true;
        }
        self.maybe_mark_live(output);
        Ok(())
    }

    fn apply_depth_delta(
        &mut self,
        symbol: &str,
        first_update_id: u64,
        last_update_id: u64,
        bids: &[BookLevel],
        asks: &[BookLevel],
        exchange_ts_ns: i64,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        let book_changes = {
            let Some(book) = self.books.get_mut(symbol) else {
                return Ok(());
            };
            if book.sync.state != SyncState::Live {
                return Ok(());
            }
            let Some(previous_update_id) = book.last_update_id else {
                return Ok(());
            };
            if last_update_id <= previous_update_id {
                return Ok(());
            }
            let expected = previous_update_id.saturating_add(1);
            if first_update_id > expected {
                let instrument = book.sync.instrument;
                book.sync.note_gap();
                book.has_snapshot = false;
                book.last_update_id = None;
                output.push(SessionAction::EmitSystem(SystemEvent::SequenceGap {
                    expected,
                    actual: first_update_id,
                }));
                output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                    instrument,
                    reason: "Gemini depth update gap".into(),
                }));
                output.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
                return Ok(());
            }

            let mut book_changes = Vec::new();
            for (side, levels) in [(BookSide::Bid, bids), (BookSide::Ask, asks)] {
                for level in levels {
                    let (op, qty) = if level.quantity.0.coefficient == 0 {
                        (BookOperation::Delete, None)
                    } else {
                        (BookOperation::Upsert, Some(level.quantity))
                    };
                    book_changes.push(BookChange {
                        side,
                        operation: op,
                        price: level.price,
                        quantity: qty,
                    });
                }
            }
            if let Err(err) = book.sync.book.apply_changes_atomic(&book_changes) {
                let instrument = book.sync.instrument;
                book.sync.invalidate("live change apply failed");
                book.has_snapshot = false;
                book.last_update_id = None;
                output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                    instrument,
                    reason: format!("live change apply failed: {err}"),
                }));
                output.push(SessionAction::Reconnect(ReconnectReason::Protocol));
                return Ok(());
            }
            book.last_update_id = Some(last_update_id);
            book_changes
        };
        if !book_changes.is_empty() {
            let instrument = self.instrument_for(symbol);
            let frame_seq = self.next_frame();
            let mut env = self.envelope(
                instrument,
                frame_seq,
                0,
                received,
                EventFlags::empty(),
                MarketEvent::BookDelta(BookDelta {
                    changes: book_changes,
                    checksum: None,
                }),
            );
            env.exchange_ts = Some(TimestampNs(exchange_ts_ns));
            env.source_sequence = Some(SequenceRange {
                first: first_update_id,
                last: last_update_id,
            });
            output.push(SessionAction::EmitBatch(EventBatch {
                session: self.cfg.session,
                frame_seq,
                events: vec![env],
            }));
        }
        Ok(())
    }

    fn invalidate_book(&mut self, symbol: &str, reason: &str, output: &mut ActionBuffer) {
        let Some(book) = self.books.get_mut(symbol) else {
            return;
        };
        let instrument = book.sync.instrument;
        book.sync.invalidate(reason);
        book.has_snapshot = false;
        book.last_update_id = None;
        output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
            instrument,
            reason: reason.into(),
        }));
    }

    fn handle_decoded(
        &mut self,
        decoded: Decoded,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match decoded {
            Decoded::DepthUpdate {
                symbol,
                first_update_id,
                last_update_id,
                bids,
                asks,
                exchange_ts_ns,
            } => {
                let is_first = self.books.get(&symbol).is_some_and(|b| !b.has_snapshot);
                if is_first {
                    self.apply_depth_snapshot(
                        &symbol,
                        first_update_id,
                        last_update_id,
                        &bids,
                        &asks,
                        exchange_ts_ns,
                        received,
                        output,
                    )?;
                } else {
                    self.apply_depth_delta(
                        &symbol,
                        first_update_id,
                        last_update_id,
                        &bids,
                        &asks,
                        exchange_ts_ns,
                        received,
                        output,
                    )?;
                }
            }
            Decoded::Trade {
                symbol,
                trade_id,
                price,
                quantity,
                aggressor,
                exchange_ts_ns,
            } => {
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                let mut env = self.envelope(
                    instrument,
                    frame_seq,
                    0,
                    received,
                    EventFlags::empty(),
                    MarketEvent::Trade(Trade {
                        price,
                        quantity,
                        aggressor,
                        trade_id: Some(trade_id_source(&trade_id)),
                    }),
                );
                env.exchange_ts = Some(TimestampNs(exchange_ts_ns));
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events: vec![env],
                }));
                self.ws_data_seen = true;
                self.maybe_mark_live(output);
            }
            Decoded::Quote {
                symbol,
                update_id,
                bid_price,
                bid_qty,
                ask_price,
                ask_qty,
                exchange_ts_ns,
            } => {
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                let mut env = self.envelope(
                    instrument,
                    frame_seq,
                    0,
                    received,
                    EventFlags::empty(),
                    MarketEvent::Quote(Quote {
                        bid_price,
                        bid_quantity: Some(bid_qty),
                        ask_price,
                        ask_quantity: Some(ask_qty),
                    }),
                );
                env.exchange_ts = Some(TimestampNs(exchange_ts_ns));
                env.source_sequence = Some(SequenceRange {
                    first: update_id,
                    last: update_id,
                });
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events: vec![env],
                }));
                self.ws_data_seen = true;
                self.maybe_mark_live(output);
            }
            Decoded::SubscribeAck => {
                output.push(SessionAction::EmitSystem(
                    SystemEvent::SubscriptionStateChanged {
                        state: "subscribed".into(),
                    },
                ));
            }
            Decoded::Error { code, detail } => {
                output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                    detail: format!("Gemini WebSocket error code={code:?}: {detail}"),
                }));
                output.push(SessionAction::Reconnect(ReconnectReason::Protocol));
            }
            Decoded::Candle { .. } | Decoded::Statistics24h { .. } => {}
            Decoded::Ignored => {}
            Decoded::Unknown => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "gemini".into(),
                }));
            }
        }
        Ok(())
    }
}

impl SessionMachine for GeminiSession {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Connected { now } => {
                self.live = false;
                self.ws_data_seen = false;
                self.ready_candles.clear();
                self.ready_stats.clear();
                self.pending_candles.clear();
                self.pending_stats.clear();
                self.stats_acc.clear();
                for book in self.books.values_mut() {
                    book.sync.begin_resync();
                    book.sync.request_snapshot();
                    book.has_snapshot = false;
                    book.last_update_id = None;
                }
                let streams = self.subscription_streams()?;
                if !streams.is_empty() {
                    output.push(SessionAction::SendText(Bytes::from(
                        serde_json::json!({
                            "id": "1",
                            "method": "SUBSCRIBE",
                            "params": streams,
                        })
                        .to_string(),
                    )));
                }
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
                self.ws_data_seen = false;
                self.ready_candles.clear();
                self.ready_stats.clear();
                self.pending_candles.clear();
                self.pending_stats.clear();
                self.stats_acc.clear();
                for book in self.books.values_mut() {
                    book.sync.invalidate("disconnected");
                    book.has_snapshot = false;
                    book.last_update_id = None;
                }
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
                if let Some((symbol, kind)) = self.pending_stats.remove(&request_id) {
                    if response.status != 200 {
                        self.ready_stats.remove(&(symbol.clone(), kind));
                        self.live = false;
                        output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                            detail: format!("gemini stats HTTP {}", response.status),
                        }));
                        output.push(SessionAction::MarkDegraded);
                        return Ok(());
                    }
                    let decoded = match kind {
                        PendingStatsKind::TickerV2 => decode_ticker_rest(&response.body),
                        PendingStatsKind::PubTicker => {
                            decode_pubticker_rest(&response.body, &symbol)
                        }
                    };
                    match decoded {
                        Ok(Decoded::Statistics24h {
                            open,
                            high,
                            low,
                            close,
                            volume,
                            quote_volume,
                        }) => {
                            self.ready_stats.insert((symbol.clone(), kind));
                            self.merge_stats(
                                &symbol,
                                open,
                                high,
                                low,
                                close,
                                volume,
                                quote_volume,
                                received,
                                output,
                            );
                            self.maybe_mark_live(output);
                        }
                        Ok(_) | Err(_) => {
                            self.ready_stats.remove(&(symbol.clone(), kind));
                            self.live = false;
                            output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                                detail: "bad gemini stats body".into(),
                            }));
                            output.push(SessionAction::MarkDegraded);
                        }
                    }
                    return Ok(());
                }
                let Some((symbol, interval)) = self.pending_candles.remove(&request_id) else {
                    return Ok(());
                };
                if response.status != 200 {
                    self.ready_candles.remove(&(symbol.clone(), interval));
                    self.live = false;
                    output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                        detail: format!("gemini candles HTTP {}", response.status),
                    }));
                    output.push(SessionAction::MarkDegraded);
                    return Ok(());
                }
                match decode_candles_rest(&response.body, interval) {
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
                        self.ready_candles.insert((symbol, interval));
                        self.maybe_mark_live(output);
                    }
                    Ok(_) | Err(_) => {
                        self.ready_candles.remove(&(symbol, interval));
                        self.live = false;
                        output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                            detail: "bad gemini candles body".into(),
                        }));
                        output.push(SessionAction::MarkDegraded);
                    }
                }
                Ok(())
            }
            SessionInput::Pong { .. } => Ok(()),
            SessionInput::Timer { timer_id, now } => {
                if timer_id == CANDLE_TIMER_ID {
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
