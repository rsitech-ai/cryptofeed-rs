//! Kraken Spot SessionMachine (WS v2 trade + ticker + `book` L2/checksum).

use std::collections::HashMap;

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, CandleInterval, EventBatch, ReconnectReason, SessionAction,
    SessionInput, SessionMachine, SessionSpec, StopReason,
};
use marketfeed_book::{BookSynchronizer, OrderBook, SyncLimits, SyncState};
use marketfeed_model::{
    BookChange, BookDelta, BookLevel, BookOperation, BookSide, BookSnapshot, Candle, CatalogView,
    ConnectionId, EventEnvelope, EventFlags, Fixed, FrameStamp, InstrumentId, MarketEvent, Price,
    Quantity, Quote, RoundingMode, SequenceRange, SessionId, SourceId, Statistics24h, SystemEvent,
    TimestampNs, Trade, VenueStatus,
};

use crate::checksum::book_checksum;
use crate::messages::{
    DecodedEvent, RawLevel, decode_text, ohlc_interval_minutes, trade_id_source,
};
use crate::specification::KRAKEN_SPOT_VENUE_ID;

const SCHEMA_VERSION: u16 = 1;
/// Checksum is always computed over top-10 regardless of subscribed depth (Kraken docs).
const BOOK_DEPTH: u32 = 10;

#[derive(Debug, Clone)]
pub struct KrakenSessionConfig {
    pub symbols: Vec<String>,
    pub instrument_ids: HashMap<String, InstrumentId>,
    pub connection: ConnectionId,
    pub session: SessionId,
    pub enable_l2: bool,
    /// Native `ohlc` intervals. Empty = no candles.
    pub candle_intervals: Vec<CandleInterval>,
    pub price_scale: u8,
    pub qty_scale: u8,
}

impl Default for KrakenSessionConfig {
    fn default() -> Self {
        let mut instrument_ids = HashMap::new();
        instrument_ids.insert("BTC/USD".into(), InstrumentId(1));
        Self {
            symbols: vec!["BTC/USD".into()],
            instrument_ids,
            connection: ConnectionId(1),
            session: SessionId(1),
            enable_l2: false,
            candle_intervals: Vec::new(),
            // BTC/USD on Kraken: pair_decimals=1, lot_decimals=8.
            price_scale: 1,
            qty_scale: 8,
        }
    }
}

/// Mirrors the exact-decimal book for gap/crossed-book validity, plus a sidecar of
/// literal wire strings for the checksum (see `checksum.rs` for why strings, not
/// reformatted `Fixed`, are required).
#[derive(Debug)]
struct KrakenBook {
    sync: BookSynchronizer,
    wire: HashMap<i128, (String, String)>,
}

struct ChecksumVerification {
    matches: bool,
    actual: u32,
    levels: (Vec<BookLevel>, Vec<BookLevel>),
}

#[derive(Debug)]
pub struct KrakenSpotSession {
    #[allow(dead_code)]
    catalog: CatalogView,
    cfg: KrakenSessionConfig,
    frame_seq: u64,
    books: HashMap<String, KrakenBook>,
    live: bool,
}

impl KrakenSpotSession {
    pub fn new(spec: SessionSpec, catalog: CatalogView, cfg: KrakenSessionConfig) -> Self {
        let _ = spec;
        let mut books = HashMap::new();
        if cfg.enable_l2 {
            for (sym, id) in &cfg.instrument_ids {
                let book = OrderBook::new(cfg.price_scale, cfg.qty_scale, Some(BOOK_DEPTH));
                let mut sync = BookSynchronizer::new(*id, book, SyncLimits::default());
                sync.request_snapshot();
                books.insert(
                    sym.clone(),
                    KrakenBook {
                        sync,
                        wire: HashMap::new(),
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
        exchange_ts: Option<TimestampNs>,
        seq: Option<SequenceRange>,
        flags: EventFlags,
        payload: MarketEvent,
    ) -> EventEnvelope {
        EventEnvelope {
            schema_version: SCHEMA_VERSION,
            venue: KRAKEN_SPOT_VENUE_ID,
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

    fn subscribe_payloads(&self) -> Vec<Bytes> {
        let symbols = self.cfg.symbols.clone();
        let mut out = vec![
            Bytes::from(
                serde_json::json!({
                    "method": "subscribe",
                    "params": { "channel": "trade", "symbol": symbols, "snapshot": false }
                })
                .to_string(),
            ),
            Bytes::from(
                serde_json::json!({
                    "method": "subscribe",
                    "params": { "channel": "ticker", "symbol": symbols }
                })
                .to_string(),
            ),
        ];
        if self.cfg.enable_l2 {
            out.push(Bytes::from(
                serde_json::json!({
                    "method": "subscribe",
                    "params": { "channel": "book", "symbol": symbols, "depth": BOOK_DEPTH }
                })
                .to_string(),
            ));
        }
        for interval in &self.cfg.candle_intervals {
            let mins = ohlc_interval_minutes(*interval);
            out.push(Bytes::from(
                serde_json::json!({
                    "method": "subscribe",
                    "params": {
                        "channel": "ohlc",
                        "symbol": symbols,
                        "interval": mins
                    }
                })
                .to_string(),
            ));
        }
        out
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
                    events.push(self.envelope(
                        instrument,
                        frame_seq,
                        i as u16,
                        received,
                        t.exchange_ts_ns.map(TimestampNs),
                        Some(SequenceRange {
                            first: t.trade_id,
                            last: t.trade_id,
                        }),
                        EventFlags::empty(),
                        MarketEvent::Trade(Trade {
                            price: t.price,
                            quantity: t.quantity,
                            aggressor: t.aggressor,
                            trade_id: Some(trade_id_source(t.trade_id)),
                        }),
                    ));
                }
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events,
                }));
            }
            DecodedEvent::Quote {
                symbol,
                bid_price,
                bid_qty,
                ask_price,
                ask_qty,
                high,
                low,
                volume,
                last,
            } => {
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                let mut events = vec![self.envelope(
                    instrument,
                    frame_seq,
                    0,
                    received,
                    None,
                    None,
                    EventFlags::empty(),
                    MarketEvent::Quote(Quote {
                        bid_price,
                        bid_quantity: Some(bid_qty),
                        ask_price,
                        ask_quantity: Some(ask_qty),
                    }),
                )];
                if high.is_some() || low.is_some() || volume.is_some() || last.is_some() {
                    events.push(self.envelope(
                        instrument,
                        frame_seq,
                        1,
                        received,
                        None,
                        None,
                        EventFlags::empty(),
                        MarketEvent::Statistics24h(Statistics24h {
                            open: None,
                            high,
                            low,
                            close: last,
                            volume,
                            quote_volume: None,
                        }),
                    ));
                }
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events,
                }));
            }
            DecodedEvent::BookSnapshot {
                symbol,
                bids,
                asks,
                checksum,
                exchange_ts_ns,
            } => {
                self.apply_book_snapshot(
                    &symbol,
                    &bids,
                    &asks,
                    checksum,
                    exchange_ts_ns,
                    received,
                    output,
                )?;
            }
            DecodedEvent::BookUpdate {
                symbol,
                bids,
                asks,
                checksum,
                exchange_ts_ns,
            } => {
                self.apply_book_update(
                    &symbol,
                    &bids,
                    &asks,
                    checksum,
                    exchange_ts_ns,
                    received,
                    output,
                )?;
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
                exchange_ts_ns,
            } => {
                let instrument = self.instrument_for(&symbol);
                let frame_seq = self.next_frame();
                let env = self.envelope(
                    instrument,
                    frame_seq,
                    0,
                    received,
                    exchange_ts_ns.map(TimestampNs),
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
            DecodedEvent::VenueStatus { status } => {
                let frame_seq = self.next_frame();
                let env = self.envelope(
                    None,
                    frame_seq,
                    0,
                    received,
                    None,
                    None,
                    EventFlags::empty(),
                    MarketEvent::VenueStatus(VenueStatus { message: status }),
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
            DecodedEvent::Unknown => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "kraken".into(),
                }));
            }
        }
        Ok(())
    }

    fn apply_book_snapshot(
        &mut self,
        symbol: &str,
        bids: &[RawLevel],
        asks: &[RawLevel],
        checksum: u32,
        exchange_ts_ns: Option<i64>,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if !self.cfg.enable_l2 {
            return Ok(());
        }
        let Some(book) = self.books.get_mut(symbol) else {
            return Ok(());
        };
        book.sync.begin_resync();
        book.sync.request_snapshot();
        let bid_pairs: Vec<(Price, Quantity)> =
            bids.iter().map(|l| (l.price, l.quantity)).collect();
        let ask_pairs: Vec<(Price, Quantity)> =
            asks.iter().map(|l| (l.price, l.quantity)).collect();
        if let Err(err) = book.sync.book.apply_snapshot(&bid_pairs, &ask_pairs, None) {
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
        book.wire.clear();
        for l in bids.iter().chain(asks.iter()) {
            if let Some(key) = price_key(l.price, self.cfg.price_scale) {
                book.wire
                    .insert(key, (l.price_str.clone(), l.qty_str.clone()));
            }
        }
        book.sync.state = SyncState::Live;

        let instrument = book.sync.instrument;
        let verification = verify_checksum(book, self.cfg.price_scale, checksum);
        if !verification.matches {
            book.sync.invalidate("checksum mismatch on snapshot");
            output.push(SessionAction::EmitSystem(SystemEvent::ChecksumMismatch {
                detail: format!(
                    "kraken spot snapshot expected={checksum} actual={}",
                    verification.actual
                ),
            }));
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument,
                reason: "checksum mismatch on snapshot".into(),
            }));
            output.push(SessionAction::ResyncInstrument(instrument));
            output.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
            return Ok(());
        }
        let (book_bids, book_asks) = verification.levels;

        let frame_seq = self.next_frame();
        let env = self.envelope(
            Some(instrument),
            frame_seq,
            0,
            received,
            exchange_ts_ns.map(TimestampNs),
            None,
            EventFlags::SNAPSHOT,
            MarketEvent::BookSnapshot(BookSnapshot {
                bids: book_bids,
                asks: book_asks,
                depth: Some(BOOK_DEPTH),
                checksum: Some(SourceId(checksum.to_string())),
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

    fn apply_book_update(
        &mut self,
        symbol: &str,
        bids: &[RawLevel],
        asks: &[RawLevel],
        checksum: u32,
        exchange_ts_ns: Option<i64>,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if !self.cfg.enable_l2 {
            return Ok(());
        }
        let Some(book) = self.books.get_mut(symbol) else {
            return Ok(());
        };
        if book.sync.state != SyncState::Live {
            // Waiting for the WS snapshot — Kraken always sends it first after subscribe.
            return Ok(());
        }
        let price_scale = self.cfg.price_scale;

        let mut changes = Vec::new();
        for (side, levels) in [(BookSide::Bid, bids), (BookSide::Ask, asks)] {
            for l in levels {
                let is_delete = l.quantity.0.coefficient == 0;
                changes.push(BookChange {
                    side,
                    operation: if is_delete {
                        BookOperation::Delete
                    } else {
                        BookOperation::Upsert
                    },
                    price: l.price,
                    quantity: if is_delete { None } else { Some(l.quantity) },
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

        for levels in [bids, asks] {
            for l in levels {
                let is_delete = l.quantity.0.coefficient == 0;
                if let Some(key) = price_key(l.price, price_scale) {
                    if is_delete {
                        book.wire.remove(&key);
                    } else {
                        book.wire
                            .insert(key, (l.price_str.clone(), l.qty_str.clone()));
                    }
                }
            }
        }
        // Docs: levels that fall out of the top-N are dropped silently (no qty:0) —
        // prune the wire cache to whatever OrderBook's own trim actually kept.
        prune_wire_cache(book, price_scale);

        let instrument = book.sync.instrument;
        let verification = verify_checksum(book, price_scale, checksum);
        if !verification.matches {
            book.sync.note_gap();
            output.push(SessionAction::EmitSystem(SystemEvent::ChecksumMismatch {
                detail: format!(
                    "kraken spot update expected={checksum} actual={}",
                    verification.actual
                ),
            }));
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument,
                reason: "checksum mismatch on update".into(),
            }));
            output.push(SessionAction::ResyncInstrument(instrument));
            output.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
            return Ok(());
        }

        let frame_seq = self.next_frame();
        let env = self.envelope(
            self.instrument_for(symbol),
            frame_seq,
            0,
            received,
            exchange_ts_ns.map(TimestampNs),
            None,
            EventFlags::DELTA,
            MarketEvent::BookDelta(BookDelta {
                changes,
                checksum: Some(SourceId(checksum.to_string())),
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

/// Exact price key at the catalog scale — the join key between the `Fixed` book
/// and the wire-string checksum sidecar.
fn price_key(price: Price, scale: u8) -> Option<i128> {
    price
        .0
        .rescale(scale, RoundingMode::ExactOnly)
        .ok()
        .map(|f| f.coefficient)
}

/// Compare the current book's top-10 checksum against `expected`. Returns the
/// current (bids, asks) `BookLevel`s either way so callers can reuse them for the
/// snapshot event without recomputing.
fn verify_checksum(book: &KrakenBook, price_scale: u8, expected: u32) -> ChecksumVerification {
    let (bids, asks) = book.sync.book.snapshot_levels().unwrap_or_default();
    let bid_strs: Vec<(String, String)> = bids
        .iter()
        .map(|l| wire_strings_for(book, price_scale, l))
        .collect();
    let ask_strs: Vec<(String, String)> = asks
        .iter()
        .map(|l| wire_strings_for(book, price_scale, l))
        .collect();
    let computed = book_checksum(
        ask_strs.iter().map(|(p, q)| (p.as_str(), q.as_str())),
        bid_strs.iter().map(|(p, q)| (p.as_str(), q.as_str())),
    );
    ChecksumVerification {
        matches: computed == expected,
        actual: computed,
        levels: (bids, asks),
    }
}

fn wire_strings_for(book: &KrakenBook, price_scale: u8, level: &BookLevel) -> (String, String) {
    price_key(level.price, price_scale)
        .and_then(|k| book.wire.get(&k).cloned())
        .unwrap_or_else(|| (format_fixed(level.price.0), format_fixed(level.quantity.0)))
}

/// Prune the wire-string cache to exactly the price keys the book actually kept
/// after trimming to depth — bounds memory and matches Kraken's "no qty:0 for
/// dropped levels" rule.
fn prune_wire_cache(book: &mut KrakenBook, price_scale: u8) {
    let Some((bids, asks)) = book.sync.book.snapshot_levels() else {
        book.wire.clear();
        return;
    };
    let valid: std::collections::HashSet<i128> = bids
        .iter()
        .chain(asks.iter())
        .filter_map(|l| price_key(l.price, price_scale))
        .collect();
    book.wire.retain(|k, _| valid.contains(k));
}

/// Fallback formatter for a level whose wire string wasn't cached (should not
/// happen once synced — snapshots always seed the cache for every level).
fn format_fixed(f: Fixed) -> String {
    let neg = f.coefficient < 0;
    let mag = f.coefficient.unsigned_abs();
    let scale = f.scale as usize;
    let digits = mag.to_string();
    let (int_part, frac_part) = if scale == 0 {
        (digits, String::new())
    } else if digits.len() <= scale {
        ("0".to_string(), format!("{digits:0>scale$}"))
    } else {
        let split = digits.len() - scale;
        (digits[..split].to_string(), digits[split..].to_string())
    };
    let mut s = String::new();
    if neg {
        s.push('-');
    }
    s.push_str(&int_part);
    if !frac_part.is_empty() {
        s.push('.');
        s.push_str(&frac_part);
    }
    s
}

impl SessionMachine for KrakenSpotSession {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Connected { .. } => {
                self.live = false;
                if self.cfg.enable_l2 {
                    for book in self.books.values_mut() {
                        book.sync.begin_resync();
                        book.sync.request_snapshot();
                        book.wire.clear();
                    }
                }
                for msg in self.subscribe_payloads() {
                    output.push(SessionAction::SendText(msg));
                }
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
                for book in self.books.values_mut() {
                    book.sync.invalidate("disconnected");
                    book.wire.clear();
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
            SessionInput::HttpResponse { .. }
            | SessionInput::Pong { .. }
            | SessionInput::Timer { .. } => Ok(()),
            SessionInput::Control { command } => {
                match command {
                    marketfeed_adapter_api::SessionCommand::Stop => {
                        output.push(SessionAction::StopSession(StopReason::Control));
                    }
                    marketfeed_adapter_api::SessionCommand::Resync(id) => {
                        if self.cfg.enable_l2
                            && self.cfg.instrument_ids.values().any(|iid| iid == id)
                        {
                            // Re-subscribe path: reconnect so Kraken re-sends the book snapshot.
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
