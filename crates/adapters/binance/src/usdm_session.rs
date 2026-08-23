//! Binance USD-M SessionMachine — trades/quote/mark + dedicated indexPrice + L2 (`pu`) + OI REST + forceOrder.

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
    EventEnvelope, EventFlags, FrameStamp, Funding, InstrumentId, InstrumentKind, Liquidation,
    MarketEvent, OpenInterest, Price, PricePoint, Quantity, Quote, SequenceRange, SessionId,
    Statistics24h, SystemEvent, TimestampNs, Trade,
};

use crate::messages::kline_stream_interval;
use crate::usdm_messages::{
    UsdmDecoded, UsdmRoutedV4SourceTimes, agg_id_source, decode_routed_v4_text, decode_text,
    level_op, to_book_levels,
};
use crate::usdm_specification::{BINANCE_USDM_VENUE_ID, OI_POLL_INTERVAL_MS, OI_TIMER_ID};

const SCHEMA_VERSION: u16 = 1;
const MAX_BUFFERED_DEPTH_SPAN_NS: u64 = 5_000_000_000;
const ROUTED_V4_SYMBOL: &str = "BNBUSDT";
const ROUTED_V4_PUBLIC_ENDPOINT: &str = "wss://fstream.binance.com/public/ws";
const ROUTED_V4_MARKET_ENDPOINT: &str = "wss://fstream.binance.com/market/ws";
const MAX_ROUTED_SOURCE_MS: i64 = i64::MAX / 1_000_000;

fn valid_routed_source_ms(value: i64) -> bool {
    (0..=MAX_ROUTED_SOURCE_MS).contains(&value)
}

/// Immutable role of an E2 Binance USD-M routed session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinanceUsdmRouteV4 {
    Public,
    Market,
}

/// Immutable identity of a factory-validated routed-v4 session before replay begins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinanceUsdmPristineIdentityV4 {
    route: BinanceUsdmRouteV4,
    connection: ConnectionId,
    session: SessionId,
    instrument: InstrumentId,
}

impl BinanceUsdmPristineIdentityV4 {
    pub const fn route(&self) -> BinanceUsdmRouteV4 {
        self.route
    }
    pub const fn connection(&self) -> ConnectionId {
        self.connection
    }
    pub const fn session(&self) -> SessionId {
        self.session
    }
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }
    pub const fn symbol(&self) -> &'static str {
        ROUTED_V4_SYMBOL
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UsdmMode {
    Legacy,
    RoutedV4(BinanceUsdmRouteV4),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingHttp {
    DepthSnapshot,
    OpenInterest,
}

#[derive(Debug, Clone)]
pub struct BinanceUsdmSessionConfig {
    pub symbols: Vec<String>,
    pub instrument_ids: HashMap<String, InstrumentId>,
    pub connection: ConnectionId,
    pub session: SessionId,
    pub enable_l2: bool,
    pub candle_intervals: Vec<CandleInterval>,
    pub price_scale: u8,
    pub qty_scale: u8,
}

impl Default for BinanceUsdmSessionConfig {
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
            qty_scale: 3,
        }
    }
}

type SourceTimes = UsdmRoutedV4SourceTimes;

#[derive(Debug, Clone)]
struct BufferedDepthEvent {
    first_u: u64,
    final_u: u64,
    prev_u: u64,
    bids: Vec<(Price, Quantity)>,
    asks: Vec<(Price, Quantity)>,
    source_times: SourceTimes,
    bytes_len: usize,
    received_mono_ns: u64,
}

#[derive(Debug)]
struct SymbolBook {
    sync: BookSynchronizer,
    snapshot_req_id: Option<u64>,
    buffering: bool,
    awaiting_bridge: bool,
    depth_buffer: Vec<BufferedDepthEvent>,
    buffered_bytes: usize,
    max_buffered_events: usize,
    max_buffered_bytes: usize,
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
            || mono_max.saturating_sub(mono_min) > MAX_BUFFERED_DEPTH_SPAN_NS
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
pub struct BinanceUsdmSession {
    #[allow(dead_code)]
    catalog: CatalogView,
    cfg: BinanceUsdmSessionConfig,
    frame_seq: u64,
    next_http_id: u64,
    books: HashMap<String, SymbolBook>,
    /// ponytail: one pending map until typed HTTP correlation lands in adapter-api.
    pending_http: HashMap<u64, (String, PendingHttp)>,
    live: bool,
    mode: UsdmMode,
    pristine: bool,
}

impl BinanceUsdmSession {
    pub fn new(spec: SessionSpec, catalog: CatalogView, cfg: BinanceUsdmSessionConfig) -> Self {
        let _ = spec;
        let mut books = HashMap::new();
        if cfg.enable_l2 {
            for (sym, id) in &cfg.instrument_ids {
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
                        awaiting_bridge: true,
                        depth_buffer: Vec::new(),
                        buffered_bytes: 0,
                        max_buffered_events: 10_000,
                        max_buffered_bytes: 4 * 1024 * 1024,
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
            pending_http: HashMap::new(),
            live: false,
            mode: UsdmMode::Legacy,
            pristine: true,
        }
    }

    /// Construct the split, public-read-only BNBUSDT E2 route without changing
    /// the legacy factory/session behavior.
    fn try_new_routed_v4(
        spec: SessionSpec,
        catalog: CatalogView,
        cfg: BinanceUsdmSessionConfig,
        route: BinanceUsdmRouteV4,
    ) -> Result<Self, AdapterError> {
        let expected_endpoint = match route {
            BinanceUsdmRouteV4::Public => ROUTED_V4_PUBLIC_ENDPOINT,
            BinanceUsdmRouteV4::Market => ROUTED_V4_MARKET_ENDPOINT,
        };
        if spec.endpoint_name != expected_endpoint
            || !spec.subscriptions.items.is_empty()
            || cfg.symbols.as_slice() != [ROUTED_V4_SYMBOL]
            || cfg.instrument_ids.len() != 1
            || !cfg.instrument_ids.contains_key(ROUTED_V4_SYMBOL)
            || cfg.connection.0 == 0
            || cfg.session.0 == 0
            || !cfg.candle_intervals.is_empty()
            || cfg.enable_l2 != matches!(route, BinanceUsdmRouteV4::Public)
        {
            return Err(AdapterError::Subscription(
                "invalid Binance USD-M routed v4 identity or topology".into(),
            ));
        }
        let mut session = Self::new(spec, catalog, cfg);
        session.mode = UsdmMode::RoutedV4(route);
        Ok(session)
    }

    /// Construct the supported E2 routed topology as one checked pair.
    pub fn try_new_routed_pair_v4(
        public_spec: SessionSpec,
        market_spec: SessionSpec,
        catalog: CatalogView,
        public_cfg: BinanceUsdmSessionConfig,
        market_cfg: BinanceUsdmSessionConfig,
    ) -> Result<(Self, Self), AdapterError> {
        let configured_id = public_cfg.instrument_ids.get(ROUTED_V4_SYMBOL);
        let catalog_row = catalog.find_by_native(ROUTED_V4_SYMBOL);
        if public_cfg.connection == market_cfg.connection
            || public_cfg.session == market_cfg.session
            || public_cfg.instrument_ids != market_cfg.instrument_ids
            || catalog.venue != BINANCE_USDM_VENUE_ID
            || catalog_row.is_none_or(|instrument| {
                Some(instrument.id) != configured_id.copied()
                    || instrument.catalog_version != catalog.version
                    || instrument.key.venue.0 != "binance-usdm"
                    || instrument.base.0 != "BNB"
                    || instrument.quote.0 != "USDT"
                    || instrument.key.kind != InstrumentKind::PerpetualLinear
                    || instrument
                        .settlement
                        .as_ref()
                        .is_none_or(|asset| asset.0 != "USDT")
                    || instrument
                        .key
                        .settlement
                        .as_ref()
                        .is_none_or(|asset| asset.0 != "USDT")
                    || instrument.inverse
                    || instrument.expiry_ns.is_some()
                    || instrument.key.expiry_ns.is_some()
            })
        {
            return Err(AdapterError::Subscription(
                "routed v4 pair identities must be distinct and instrument-equal".into(),
            ));
        }
        let public = Self::try_new_routed_v4(
            public_spec,
            catalog.clone(),
            public_cfg,
            BinanceUsdmRouteV4::Public,
        )?;
        let market =
            Self::try_new_routed_v4(market_spec, catalog, market_cfg, BinanceUsdmRouteV4::Market)?;
        Ok((public, market))
    }

    fn routed_v4(&self) -> Option<BinanceUsdmRouteV4> {
        match self.mode {
            UsdmMode::Legacy => None,
            UsdmMode::RoutedV4(route) => Some(route),
        }
    }

    /// Return the exact routed identity only while the session is factory-pristine.
    pub fn pristine_routed_v4_identity(
        &self,
    ) -> Result<BinanceUsdmPristineIdentityV4, AdapterError> {
        let route = self.routed_v4().ok_or_else(|| {
            AdapterError::Protocol("session is not Binance USD-M routed v4".into())
        })?;
        let instrument = self.cfg.instrument_ids.get(ROUTED_V4_SYMBOL).copied();
        let public_books_pristine = self.books.len() == 1
            && self.books.get(ROUTED_V4_SYMBOL).is_some_and(|book| {
                book.snapshot_req_id.is_none()
                    && book.buffering
                    && book.awaiting_bridge
                    && book.depth_buffer.is_empty()
                    && book.buffered_bytes == 0
                    && book.buffered_mono_min.is_none()
                    && book.buffered_mono_max.is_none()
            });
        let state_pristine = self.pristine
            && self.frame_seq == 0
            && self.next_http_id == 1
            && self.pending_http.is_empty()
            && !self.live
            && self.cfg.symbols.as_slice() == [ROUTED_V4_SYMBOL]
            && self.cfg.instrument_ids.len() == 1
            && self.cfg.candle_intervals.is_empty()
            && self.cfg.enable_l2 == matches!(route, BinanceUsdmRouteV4::Public)
            && match route {
                BinanceUsdmRouteV4::Public => public_books_pristine,
                BinanceUsdmRouteV4::Market => self.books.is_empty(),
            };
        if !state_pristine {
            return Err(AdapterError::Protocol(
                "routed v4 session is not pristine".into(),
            ));
        }
        let instrument = instrument.ok_or_else(|| {
            AdapterError::Protocol("routed v4 instrument identity is missing".into())
        })?;
        Ok(BinanceUsdmPristineIdentityV4 {
            route,
            connection: self.cfg.connection,
            session: self.cfg.session,
            instrument,
        })
    }

    fn validate_routed_ws(
        &self,
        decoded: &UsdmDecoded,
        source_times: SourceTimes,
    ) -> Result<(), AdapterError> {
        let Some(route) = self.routed_v4() else {
            return Ok(());
        };
        let valid = match (route, decoded) {
            (BinanceUsdmRouteV4::Public, UsdmDecoded::Quote { symbol, .. }) => {
                symbol == ROUTED_V4_SYMBOL
                    && source_times
                        .event_time_ms
                        .is_some_and(valid_routed_source_ms)
                    && source_times
                        .transaction_time_ms
                        .is_some_and(valid_routed_source_ms)
            }
            (
                BinanceUsdmRouteV4::Public,
                UsdmDecoded::DepthUpdate {
                    symbol,
                    first_update_id,
                    final_update_id,
                    prev_final_update_id,
                    ..
                },
            ) => {
                symbol == ROUTED_V4_SYMBOL
                    && first_update_id <= final_update_id
                    && *first_update_id <= i64::MAX as u64
                    && *final_update_id <= i64::MAX as u64
                    && *prev_final_update_id <= i64::MAX as u64
                    && source_times
                        .event_time_ms
                        .is_some_and(valid_routed_source_ms)
                    && source_times
                        .transaction_time_ms
                        .is_some_and(valid_routed_source_ms)
            }
            (BinanceUsdmRouteV4::Market, UsdmDecoded::AggTrade { symbol, agg_id, .. }) => {
                symbol == ROUTED_V4_SYMBOL
                    && *agg_id <= i64::MAX as u64
                    && source_times
                        .event_time_ms
                        .is_some_and(valid_routed_source_ms)
                    && source_times
                        .transaction_time_ms
                        .is_some_and(valid_routed_source_ms)
            }
            (BinanceUsdmRouteV4::Market, UsdmDecoded::OpenInterest { symbol, .. }) => {
                symbol == ROUTED_V4_SYMBOL
                    && source_times
                        .transaction_time_ms
                        .is_some_and(valid_routed_source_ms)
            }
            (BinanceUsdmRouteV4::Market, UsdmDecoded::ForceOrder { symbol, .. }) => {
                symbol == ROUTED_V4_SYMBOL
                    && source_times
                        .event_time_ms
                        .is_some_and(valid_routed_source_ms)
                    && source_times
                        .transaction_time_ms
                        .is_some_and(valid_routed_source_ms)
            }
            (_, UsdmDecoded::SubscribeAck { id: Some(1) }) => true,
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(AdapterError::Protocol(
                "message does not match Binance USD-M routed v4 role, symbol, or timestamp shape"
                    .into(),
            ))
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
            venue: BINANCE_USDM_VENUE_ID,
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

    fn emit_one(&mut self, env: EventEnvelope, output: &mut ActionBuffer) {
        let frame_seq = env.frame_seq;
        output.push(SessionAction::EmitBatch(EventBatch {
            session: self.cfg.session,
            frame_seq,
            events: vec![env],
        }));
    }

    fn alloc_http(&mut self, symbol: &str, kind: PendingHttp) -> u64 {
        let id = self.next_http_id;
        self.next_http_id += 1;
        self.pending_http.insert(id, (symbol.to_string(), kind));
        id
    }

    fn request_depth_snapshot(&mut self, symbol: &str, output: &mut ActionBuffer) {
        let Some(book) = self.books.get_mut(symbol) else {
            return;
        };
        let id = self.next_http_id;
        self.next_http_id += 1;
        book.snapshot_req_id = Some(id);
        book.buffering = true;
        book.awaiting_bridge = true;
        book.sync.begin_resync();
        book.sync.request_snapshot();
        self.pending_http
            .insert(id, (symbol.to_string(), PendingHttp::DepthSnapshot));
        output.push(SessionAction::RequestHttp(HttpRequestSpec {
            id,
            method: HttpMethod::Get,
            url: format!("https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=1000"),
            headers: Vec::new(),
            body: None,
        }));
    }

    fn request_open_interest(&mut self, symbol: &str, output: &mut ActionBuffer) {
        if self.routed_v4() == Some(BinanceUsdmRouteV4::Market)
            && self.pending_http.values().any(|(pending_symbol, kind)| {
                pending_symbol == symbol && *kind == PendingHttp::OpenInterest
            })
        {
            return;
        }
        let id = self.alloc_http(symbol, PendingHttp::OpenInterest);
        output.push(SessionAction::RequestHttp(HttpRequestSpec {
            id,
            method: HttpMethod::Get,
            url: format!("https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}"),
            headers: Vec::new(),
            body: None,
        }));
    }

    fn schedule_oi_timer(&self, now: TimestampNs, output: &mut ActionBuffer) {
        output.push(SessionAction::ScheduleTimer(TimerSpec {
            timer_id: OI_TIMER_ID,
            fire_at: TimestampNs(
                now.0
                    .saturating_add(OI_POLL_INTERVAL_MS.saturating_mul(1_000_000)),
            ),
        }));
    }

    fn poll_open_interest_all(&mut self, output: &mut ActionBuffer) {
        let symbols: Vec<_> = self.cfg.symbols.clone();
        for sym in &symbols {
            self.request_open_interest(sym, output);
        }
    }

    fn handle_decoded(
        &mut self,
        decoded: UsdmDecoded,
        routed_source_times: Option<SourceTimes>,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match decoded {
            UsdmDecoded::AggTrade {
                symbol,
                agg_id,
                price,
                quantity,
                aggressor,
                exchange_ts_ms,
            } => {
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                self.emit_one(
                    self.envelope(
                        instrument,
                        frame_seq,
                        0,
                        received,
                        routed_source_times
                            .and_then(|times| times.transaction_time_ms)
                            .or(Some(exchange_ts_ms)),
                        Some(SequenceRange {
                            first: agg_id,
                            last: agg_id,
                        }),
                        EventFlags::empty(),
                        MarketEvent::Trade(Trade {
                            price,
                            quantity,
                            aggressor,
                            trade_id: Some(agg_id_source(agg_id)),
                        }),
                    ),
                    output,
                );
            }
            UsdmDecoded::Candle {
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
            UsdmDecoded::Quote {
                symbol,
                update_id,
                bid_price,
                bid_qty,
                ask_price,
                ask_qty,
            } => {
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                self.emit_one(
                    self.envelope(
                        instrument,
                        frame_seq,
                        0,
                        received,
                        routed_source_times.and_then(|times| times.transaction_time_ms),
                        self.routed_v4().is_none().then_some(SequenceRange {
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
                    ),
                    output,
                );
            }
            UsdmDecoded::Ticker24h {
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
            UsdmDecoded::MarkPrice {
                symbol,
                mark,
                index,
                funding_rate,
                next_funding_ts,
                exchange_ts_ms,
            } => {
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                let make = |event_index: u16, payload: MarketEvent| EventEnvelope {
                    schema_version: SCHEMA_VERSION,
                    venue: BINANCE_USDM_VENUE_ID,
                    instrument,
                    connection: self.cfg.connection,
                    session: self.cfg.session,
                    frame_seq,
                    event_index,
                    exchange_ts: Some(TimestampNs(exchange_ts_ms.saturating_mul(1_000_000))),
                    receive_ts: received.receive_ts,
                    source_sequence: None,
                    flags: EventFlags::empty(),
                    payload,
                };
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events: vec![
                        make(0, MarketEvent::MarkPrice(PricePoint { price: mark })),
                        make(1, MarketEvent::IndexPrice(PricePoint { price: index })),
                        make(
                            2,
                            MarketEvent::Funding(Funding {
                                rate: funding_rate,
                                next_funding_ts: Some(next_funding_ts),
                            }),
                        ),
                    ],
                }));
            }
            UsdmDecoded::IndexPrice {
                symbol,
                price,
                exchange_ts_ms,
            } => {
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                self.emit_one(
                    self.envelope(
                        instrument,
                        frame_seq,
                        0,
                        received,
                        Some(exchange_ts_ms),
                        None,
                        EventFlags::empty(),
                        MarketEvent::IndexPrice(PricePoint { price }),
                    ),
                    output,
                );
            }
            UsdmDecoded::DepthUpdate {
                symbol,
                first_update_id,
                final_update_id,
                prev_final_update_id,
                bids,
                asks,
                exchange_ts_ms,
            } => {
                self.on_depth_update(
                    &symbol,
                    first_update_id,
                    final_update_id,
                    prev_final_update_id,
                    bids,
                    asks,
                    routed_source_times.unwrap_or(SourceTimes {
                        event_time_ms: Some(exchange_ts_ms),
                        transaction_time_ms: None,
                    }),
                    received,
                    output,
                )?;
            }
            UsdmDecoded::DepthSnapshot {
                last_update_id,
                bids,
                asks,
            } => {
                if let Some(sym) = self.cfg.symbols.first().cloned() {
                    self.apply_snapshot(
                        &sym,
                        last_update_id,
                        routed_source_times.unwrap_or(SourceTimes {
                            event_time_ms: None,
                            transaction_time_ms: None,
                        }),
                        &bids,
                        &asks,
                        received,
                        output,
                    )?;
                }
            }
            UsdmDecoded::OpenInterest {
                symbol,
                quantity,
                exchange_ts_ms,
            } => {
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                self.emit_one(
                    self.envelope(
                        instrument,
                        frame_seq,
                        0,
                        received,
                        Some(exchange_ts_ms),
                        None,
                        EventFlags::empty(),
                        MarketEvent::OpenInterest(OpenInterest { quantity }),
                    ),
                    output,
                );
            }
            UsdmDecoded::ForceOrder {
                symbol,
                price,
                quantity,
                side,
                exchange_ts_ms,
            } => {
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                self.emit_one(
                    self.envelope(
                        instrument,
                        frame_seq,
                        0,
                        received,
                        routed_source_times
                            .and_then(|times| times.transaction_time_ms)
                            .or(Some(exchange_ts_ms)),
                        None,
                        EventFlags::empty(),
                        MarketEvent::Liquidation(Liquidation {
                            price,
                            quantity,
                            side,
                        }),
                    ),
                    output,
                );
            }
            UsdmDecoded::SubscribeAck { .. } => {
                if self.routed_v4().is_none() {
                    output.push(SessionAction::EmitSystem(
                        SystemEvent::SubscriptionStateChanged {
                            state: "subscribed".into(),
                        },
                    ));
                }
                if !self.cfg.enable_l2 && !self.live {
                    self.live = true;
                    output.push(SessionAction::MarkLive);
                }
            }
            UsdmDecoded::Ignored => {}
            UsdmDecoded::Unknown => {
                if self.routed_v4().is_some() {
                    return Err(AdapterError::Protocol(
                        "unknown Binance USD-M routed v4 message".into(),
                    ));
                }
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "binance-usdm".into(),
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
        prev_u: u64,
        bids: Vec<(Price, Quantity)>,
        asks: Vec<(Price, Quantity)>,
        source_times: SourceTimes,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if !self.cfg.enable_l2 {
            return Ok(());
        }
        let Some(book) = self.books.get_mut(symbol) else {
            return Ok(());
        };
        let event = BufferedDepthEvent {
            first_u,
            final_u,
            prev_u,
            bytes_len: 64 + bids.len().saturating_mul(24) + asks.len().saturating_mul(24),
            bids,
            asks,
            source_times,
            received_mono_ns: received.mono_ns,
        };

        if book.buffering || book.sync.state != SyncState::Live {
            if let Err(reason) = book.push_depth(event) {
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

        if book.awaiting_bridge {
            let snapshot_u = book.sync.book.sequence().unwrap_or(0);
            if event.final_u < snapshot_u {
                return Ok(());
            }
            if event.first_u > snapshot_u {
                let instrument = book.sync.instrument;
                book.sync.note_gap();
                book.buffering = true;
                book.awaiting_bridge = true;
                book.clear_depth_buffer();
                output.push(SessionAction::EmitSystem(SystemEvent::SequenceGap {
                    expected: snapshot_u,
                    actual: event.first_u,
                }));
                output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                    instrument,
                    reason: "first depth event does not bridge snapshot".into(),
                }));
                output.push(SessionAction::ResyncInstrument(instrument));
                output.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
                return Ok(());
            }
            book.awaiting_bridge = false;
            return self.apply_live_depth(symbol, &event, false, received, output);
        }

        self.apply_live_depth(symbol, &event, true, received, output)
    }

    fn apply_live_depth(
        &mut self,
        symbol: &str,
        event: &BufferedDepthEvent,
        enforce_prev_u: bool,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        let Some(book) = self.books.get_mut(symbol) else {
            return Ok(());
        };
        let last_u = book.sync.book.sequence().unwrap_or(0);
        if event.final_u <= last_u {
            return Ok(()); // stale
        }
        // Futures continuity: pu must equal previous applied u (not spot's U == last+1).
        if enforce_prev_u && event.prev_u != last_u {
            let instrument = book.sync.instrument;
            book.sync.note_gap();
            book.buffering = true;
            book.clear_depth_buffer();
            output.push(SessionAction::EmitSystem(SystemEvent::SequenceGap {
                expected: last_u,
                actual: event.prev_u,
            }));
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument,
                reason: "pu discontinuity".into(),
            }));
            output.push(SessionAction::ResyncInstrument(instrument));
            output.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
            return Ok(());
        }

        let mut changes = Vec::new();
        for (side, levels) in [
            (BookSide::Bid, event.bids.as_slice()),
            (BookSide::Ask, event.asks.as_slice()),
        ] {
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
        book.sync.book.set_sequence(event.final_u);
        book.sync.expected_sequence = Some(event.final_u);

        let instrument = self.instrument_for(symbol);
        let frame_seq = self.next_frame();
        let authored_time_ms = if self.routed_v4() == Some(BinanceUsdmRouteV4::Public) {
            event.source_times.transaction_time_ms
        } else {
            event.source_times.event_time_ms
        };
        self.emit_one(
            self.envelope(
                instrument,
                frame_seq,
                0,
                received,
                authored_time_ms,
                Some(SequenceRange {
                    first: event.first_u,
                    last: event.final_u,
                }),
                EventFlags::DELTA,
                MarketEvent::BookDelta(BookDelta {
                    changes,
                    checksum: None,
                }),
            ),
            output,
        );
        Ok(())
    }

    /// Apply REST snapshot, then drain buffered depth with `pu` chain checks.
    fn apply_snapshot(
        &mut self,
        symbol: &str,
        last_update_id: u64,
        source_times: SourceTimes,
        bids: &[(Price, Quantity)],
        asks: &[(Price, Quantity)],
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        let authored_time_ms = if self.routed_v4() == Some(BinanceUsdmRouteV4::Public) {
            source_times.transaction_time_ms
        } else {
            source_times.event_time_ms
        };
        let Some(book) = self.books.get_mut(symbol) else {
            return Ok(());
        };

        // Snapshot older than first buffered event → re-fetch.
        if let Some(first_u) = book.depth_buffer.first().map(|e| e.first_u) {
            if last_update_id < first_u {
                let sym = symbol.to_string();
                self.request_depth_snapshot(&sym, output);
                return Ok(());
            }
        }

        // Binance futures keeps the inclusive bridge event: U <= snapshot <= u.
        book.depth_buffer.retain(|e| e.final_u >= last_update_id);
        book.recompute_depth_buffer_limits();

        // First remaining: U <= lastUpdateId <= u (bridge).
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
        book.sync.expected_sequence = Some(last_update_id);

        let instrument = book.sync.instrument;
        let pending = std::mem::take(&mut book.depth_buffer);
        book.buffered_bytes = 0;
        book.buffered_mono_min = None;
        book.buffered_mono_max = None;
        book.buffering = false;
        book.awaiting_bridge = pending.is_empty();
        book.snapshot_req_id = None;

        let levels_b = to_book_levels(BookSide::Bid, bids);
        let levels_a = to_book_levels(BookSide::Ask, asks);
        let frame_seq = self.next_frame();
        self.emit_one(
            self.envelope(
                Some(instrument),
                frame_seq,
                0,
                received,
                authored_time_ms,
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
            ),
            output,
        );
        if self.routed_v4().is_none() {
            output.push(SessionAction::EmitSystem(SystemEvent::BookResynchronized {
                instrument,
            }));
        }

        for (index, ev) in pending.into_iter().enumerate() {
            self.apply_live_depth(symbol, &ev, index != 0, received, output)?;
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

    fn maybe_mark_all_live(&mut self, output: &mut ActionBuffer) {
        if self.cfg.enable_l2 {
            return;
        }
        if !self.live {
            self.live = true;
            output.push(SessionAction::MarkLive);
        }
    }
}

impl SessionMachine for BinanceUsdmSession {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        self.pristine = false;
        match input {
            SessionInput::Connected { now } => {
                self.live = false;
                let mut params = Vec::new();
                for s in &self.cfg.symbols {
                    let lower = s.to_ascii_lowercase();
                    match self.routed_v4() {
                        Some(BinanceUsdmRouteV4::Public) => {
                            params.push(format!("{lower}@bookTicker"));
                            params.push(format!("{lower}@depth@100ms"));
                        }
                        Some(BinanceUsdmRouteV4::Market) => {
                            params.push(format!("{lower}@aggTrade"));
                            params.push(format!("{lower}@forceOrder"));
                        }
                        None => {
                            // Prefer `@trade` — `@aggTrade` produces no frames on fstream (2026).
                            params.push(format!("{lower}@trade"));
                            params.push(format!("{lower}@bookTicker"));
                            params.push(format!("{lower}@ticker"));
                            params.push(format!("{lower}@markPrice@1s"));
                            params.push(format!("{lower}@indexPrice@1s"));
                            params.push(format!("{lower}@forceOrder"));
                            if self.cfg.enable_l2 {
                                params.push(format!("{lower}@depth@100ms"));
                            }
                            for interval in &self.cfg.candle_intervals {
                                let suffix = kline_stream_interval(*interval);
                                params.push(format!("{lower}@kline_{suffix}"));
                            }
                        }
                    }
                }
                let body = serde_json::json!({
                    "method": "SUBSCRIBE",
                    "params": params,
                    "id": 1
                });
                output.push(SessionAction::SendText(Bytes::from(body.to_string())));
                if self.routed_v4().is_none() {
                    output.push(SessionAction::EmitSystem(
                        SystemEvent::ConnectionStateChanged {
                            state: "connected".into(),
                        },
                    ));
                }

                if self.routed_v4() != Some(BinanceUsdmRouteV4::Public) {
                    self.poll_open_interest_all(output);
                    self.schedule_oi_timer(now, output);
                }

                let symbols: Vec<_> = self.cfg.symbols.clone();
                if self.cfg.enable_l2 {
                    for sym in symbols {
                        if let Some(book) = self.books.get_mut(&sym) {
                            book.clear_depth_buffer();
                            book.buffering = true;
                        }
                        self.request_depth_snapshot(&sym, output);
                    }
                } else {
                    self.maybe_mark_all_live(output);
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
                self.pending_http.clear();
                if self.routed_v4() != Some(BinanceUsdmRouteV4::Public) {
                    output.push(SessionAction::CancelTimer(OI_TIMER_ID));
                }
                if self.routed_v4().is_none() {
                    output.push(SessionAction::EmitSystem(
                        SystemEvent::ConnectionStateChanged {
                            state: "disconnected".into(),
                        },
                    ));
                }
                Ok(())
            }
            SessionInput::TextFrame { bytes, received } => {
                if self.routed_v4().is_some() {
                    let routed = decode_routed_v4_text(bytes).map_err(AdapterError::Parse)?;
                    self.validate_routed_ws(&routed.decoded, routed.source_times)?;
                    self.handle_decoded(routed.decoded, Some(routed.source_times), received, output)
                } else {
                    match decode_text(bytes) {
                        Ok(decoded) => self.handle_decoded(decoded, None, received, output),
                        Err(e) => {
                            output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                                detail: e,
                            }));
                            Ok(())
                        }
                    }
                }
            }
            SessionInput::HttpResponse {
                request_id,
                response,
                received,
            } => {
                let Some((symbol, kind)) = self.pending_http.get(&request_id).cloned() else {
                    if self.routed_v4().is_some() {
                        return Err(AdapterError::Protocol(format!(
                            "unknown or retired routed v4 HTTP request id {request_id}"
                        )));
                    }
                    return Ok(());
                };
                if self.routed_v4().is_none() {
                    self.pending_http.remove(&request_id);
                }
                if response.status != 200 {
                    if self.routed_v4().is_some() {
                        return Err(AdapterError::Protocol(format!(
                            "routed v4 HTTP {} for {:?}",
                            response.status, kind
                        )));
                    }
                    output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                        detail: format!("usdm HTTP {} for {:?}", response.status, kind),
                    }));
                    if kind == PendingHttp::DepthSnapshot {
                        output.push(SessionAction::Reconnect(ReconnectReason::Protocol));
                    }
                    return Ok(());
                }
                match kind {
                    PendingHttp::DepthSnapshot => {
                        let decoded = if self.routed_v4().is_some() {
                            decode_routed_v4_text(&response.body)
                                .map(|routed| (routed.decoded, Some(routed.source_times)))
                        } else {
                            decode_text(&response.body).map(|decoded| (decoded, None))
                        };
                        match decoded {
                            Ok((
                                UsdmDecoded::DepthSnapshot {
                                    last_update_id,
                                    bids,
                                    asks,
                                },
                                routed_source_times,
                            )) => {
                                let source_times = routed_source_times.unwrap_or(SourceTimes {
                                    event_time_ms: None,
                                    transaction_time_ms: None,
                                });
                                if self.routed_v4().is_some()
                                    && (last_update_id > i64::MAX as u64
                                        || !source_times
                                            .event_time_ms
                                            .is_some_and(valid_routed_source_ms)
                                        || !source_times
                                            .transaction_time_ms
                                            .is_some_and(valid_routed_source_ms))
                                {
                                    return Err(AdapterError::Protocol(
                                    "routed v4 depth snapshot requires bounded E, T, and lastUpdateId"
                                        .into(),
                                ));
                                }
                                self.pending_http.remove(&request_id);
                                if let Some(book) = self.books.get_mut(&symbol) {
                                    book.snapshot_req_id = None;
                                }
                                self.apply_snapshot(
                                    &symbol,
                                    last_update_id,
                                    source_times,
                                    &bids,
                                    &asks,
                                    received,
                                    output,
                                )
                            }
                            Ok(_) | Err(_) => {
                                if self.routed_v4().is_some() {
                                    return Err(AdapterError::Parse(
                                        "bad routed v4 depth snapshot body".into(),
                                    ));
                                }
                                output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                                    detail: "bad usdm depth snapshot body".into(),
                                }));
                                Ok(())
                            }
                        }
                    }
                    PendingHttp::OpenInterest => {
                        let decoded = if self.routed_v4().is_some() {
                            decode_routed_v4_text(&response.body)
                                .map(|routed| (routed.decoded, Some(routed.source_times)))
                        } else {
                            decode_text(&response.body).map(|decoded| (decoded, None))
                        };
                        match decoded {
                            Ok((
                                decoded @ UsdmDecoded::OpenInterest { .. },
                                routed_source_times,
                            )) => {
                                if let Some(source_times) = routed_source_times {
                                    self.validate_routed_ws(&decoded, source_times)?;
                                }
                                self.pending_http.remove(&request_id);
                                self.handle_decoded(decoded, routed_source_times, received, output)
                            }
                            Ok(_) | Err(_) => {
                                if self.routed_v4().is_some() {
                                    return Err(AdapterError::Parse(
                                        "bad routed v4 openInterest body".into(),
                                    ));
                                }
                                output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                                    detail: "bad usdm openInterest body".into(),
                                }));
                                Ok(())
                            }
                        }
                    }
                }
            }
            SessionInput::BinaryFrame { .. } => {
                if self.routed_v4().is_some() {
                    return Err(AdapterError::Protocol(
                        "binary frame is not admitted by Binance USD-M routed v4".into(),
                    ));
                }
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "binary".into(),
                }));
                Ok(())
            }
            SessionInput::Pong { .. } => Ok(()),
            SessionInput::Timer { timer_id, now } => {
                if timer_id == OI_TIMER_ID && self.routed_v4() != Some(BinanceUsdmRouteV4::Public) {
                    self.poll_open_interest_all(output);
                    self.schedule_oi_timer(now, output);
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
                            if self.routed_v4() != Some(BinanceUsdmRouteV4::Market)
                                && self.cfg.enable_l2
                            {
                                self.request_depth_snapshot(&sym, output);
                            }
                            if self.routed_v4() != Some(BinanceUsdmRouteV4::Public) {
                                self.request_open_interest(&sym, output);
                            }
                        }
                    }
                    _ => {}
                }
                Ok(())
            }
        }
    }
}
