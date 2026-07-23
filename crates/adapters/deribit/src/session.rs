//! Deribit SessionMachine (trades + ticker derivatives fields + `book` L2 via change_id).

use std::collections::HashMap;

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, CandleInterval, EventBatch, ReconnectReason, SessionAction,
    SessionInput, SessionMachine, SessionSpec, StopReason,
};
use marketfeed_book::{BookSynchronizer, OrderBook, SyncLimits, SyncState};
use marketfeed_model::{
    BookChange, BookDelta, BookOperation, BookSide, BookSnapshot, Candle, CatalogView,
    ConnectionId, EventEnvelope, EventFlags, FrameStamp, InstrumentId, Liquidation, MarketEvent,
    Price, PricePoint, Quantity, SequenceRange, SessionId, Statistics24h, SystemEvent, TimestampNs,
};

use crate::messages::{
    BookLevelAction, BookLevelChange, DecodedEvent, chart_resolution, decode_text,
    deribit_index_name, ms_to_ts, to_market_trade,
};
use crate::specification::DERIBIT_VENUE_ID;

const SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone)]
pub struct DeribitSessionConfig {
    pub instruments: Vec<String>,
    pub instrument_ids: HashMap<String, InstrumentId>,
    pub connection: ConnectionId,
    pub session: SessionId,
    pub heartbeat_interval_secs: u64,
    pub enable_l2: bool,
    /// Native `chart.trades.{instrument}.{resolution}` intervals. Empty = no candles.
    pub candle_intervals: Vec<CandleInterval>,
    pub price_scale: u8,
    pub qty_scale: u8,
}

impl Default for DeribitSessionConfig {
    fn default() -> Self {
        let mut instrument_ids = HashMap::new();
        instrument_ids.insert("BTC-PERPETUAL".into(), InstrumentId(1));
        Self {
            instruments: vec!["BTC-PERPETUAL".into()],
            instrument_ids,
            connection: ConnectionId(1),
            session: SessionId(1),
            heartbeat_interval_secs: 30,
            enable_l2: false,
            candle_intervals: Vec::new(),
            // BTC-PERPETUAL: tick_size=0.5, min_trade_amount=10 (USD, whole contracts).
            price_scale: 1,
            qty_scale: 0,
        }
    }
}

#[derive(Debug)]
struct DeribitBook {
    sync: BookSynchronizer,
    last_change_id: Option<u64>,
}

#[derive(Debug)]
pub struct DeribitSession {
    #[allow(dead_code)]
    catalog: CatalogView,
    cfg: DeribitSessionConfig,
    frame_seq: u64,
    next_rpc_id: u64,
    books: HashMap<String, DeribitBook>,
    live: bool,
}

impl DeribitSession {
    pub fn new(spec: SessionSpec, catalog: CatalogView, cfg: DeribitSessionConfig) -> Self {
        let _ = spec;
        let mut books = HashMap::new();
        if cfg.enable_l2 {
            for (name, id) in &cfg.instrument_ids {
                // Full-depth book channel — no artificial trim (Deribit sends the complete book).
                let book = OrderBook::new(cfg.price_scale, cfg.qty_scale, None);
                let mut sync = BookSynchronizer::new(*id, book, SyncLimits::default());
                sync.request_snapshot();
                books.insert(
                    name.clone(),
                    DeribitBook {
                        sync,
                        last_change_id: None,
                    },
                );
            }
        }
        Self {
            catalog,
            cfg,
            frame_seq: 0,
            next_rpc_id: 1,
            books,
            live: false,
        }
    }

    fn next_frame(&mut self) -> u64 {
        self.frame_seq += 1;
        self.frame_seq
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_rpc_id;
        self.next_rpc_id += 1;
        id
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
            venue: DERIBIT_VENUE_ID,
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

    fn rpc(&mut self, method: &str, params: serde_json::Value) -> Bytes {
        let id = self.next_id();
        Bytes::from(
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params
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
        decoded: DecodedEvent,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match decoded {
            DecodedEvent::Trades(rows) => {
                let mut events = Vec::with_capacity(rows.len() * 2);
                let frame_seq = self.next_frame();
                let mut idx = 0u16;
                for row in &rows {
                    let instrument = self.instrument_for(&row.instrument);
                    let seq = row.trade_seq.map(|s| SequenceRange { first: s, last: s });
                    let ts = Some(ms_to_ts(row.exchange_ts_ms));
                    events.push(self.envelope(
                        instrument,
                        frame_seq,
                        idx,
                        received,
                        ts,
                        seq,
                        EventFlags::empty(),
                        MarketEvent::Trade(to_market_trade(row)),
                    ));
                    idx = idx.saturating_add(1);
                    // No dedicated public liquidation channel; Deribit tags liq trades.
                    if row.liquidation {
                        events.push(self.envelope(
                            instrument,
                            frame_seq,
                            idx,
                            received,
                            ts,
                            seq,
                            EventFlags::empty(),
                            MarketEvent::Liquidation(Liquidation {
                                price: row.price,
                                quantity: row.quantity,
                                side: row.aggressor,
                            }),
                        ));
                        idx = idx.saturating_add(1);
                    }
                }
                if !events.is_empty() {
                    output.push(SessionAction::EmitBatch(EventBatch {
                        session: self.cfg.session,
                        frame_seq,
                        events,
                    }));
                }
            }
            DecodedEvent::Ticker(t) => {
                let instrument = self.instrument_for(&t.instrument);
                let mut events = Vec::new();
                let frame_seq = self.next_frame();
                let ts = Some(ms_to_ts(t.exchange_ts_ms));
                let mut idx = 0u16;
                if let Some(q) = t.quote {
                    events.push(self.envelope(
                        instrument,
                        frame_seq,
                        idx,
                        received,
                        ts,
                        None,
                        EventFlags::empty(),
                        MarketEvent::Quote(q),
                    ));
                    idx += 1;
                }
                if let Some(m) = t.mark {
                    events.push(self.envelope(
                        instrument,
                        frame_seq,
                        idx,
                        received,
                        ts,
                        None,
                        EventFlags::empty(),
                        MarketEvent::MarkPrice(m),
                    ));
                    idx += 1;
                }
                if let Some(ix) = t.index {
                    events.push(self.envelope(
                        instrument,
                        frame_seq,
                        idx,
                        received,
                        ts,
                        None,
                        EventFlags::empty(),
                        MarketEvent::IndexPrice(ix),
                    ));
                    idx += 1;
                }
                if let Some(f) = t.funding {
                    events.push(self.envelope(
                        instrument,
                        frame_seq,
                        idx,
                        received,
                        ts,
                        None,
                        EventFlags::empty(),
                        MarketEvent::Funding(f),
                    ));
                    idx += 1;
                }
                if let Some(oi) = t.open_interest {
                    events.push(self.envelope(
                        instrument,
                        frame_seq,
                        idx,
                        received,
                        ts,
                        None,
                        EventFlags::empty(),
                        MarketEvent::OpenInterest(oi),
                    ));
                    idx += 1;
                }
                if t.open.is_some()
                    || t.high.is_some()
                    || t.low.is_some()
                    || t.close.is_some()
                    || t.volume.is_some()
                    || t.quote_volume.is_some()
                {
                    events.push(self.envelope(
                        instrument,
                        frame_seq,
                        idx,
                        received,
                        ts,
                        None,
                        EventFlags::empty(),
                        MarketEvent::Statistics24h(Statistics24h {
                            open: t.open,
                            high: t.high,
                            low: t.low,
                            close: t.close,
                            volume: t.volume,
                            quote_volume: t.quote_volume,
                        }),
                    ));
                }
                if !events.is_empty() {
                    output.push(SessionAction::EmitBatch(EventBatch {
                        session: self.cfg.session,
                        frame_seq,
                        events,
                    }));
                }
            }
            DecodedEvent::IndexPrice {
                index_name,
                price,
                exchange_ts_ms,
            } => {
                // Index channel is shared; fan out to subscribed instruments on that index.
                let targets: Vec<_> = self
                    .cfg
                    .instruments
                    .iter()
                    .filter(|inst| deribit_index_name(inst) == index_name)
                    .cloned()
                    .collect();
                if targets.is_empty() {
                    return Ok(());
                }
                let frame_seq = self.next_frame();
                let ts = Some(ms_to_ts(exchange_ts_ms));
                let mut events = Vec::with_capacity(targets.len());
                for (idx, inst) in targets.into_iter().enumerate() {
                    let instrument = self.instrument_for(&inst);
                    events.push(self.envelope(
                        instrument,
                        frame_seq,
                        idx as u16,
                        received,
                        ts,
                        None,
                        EventFlags::empty(),
                        MarketEvent::IndexPrice(PricePoint { price }),
                    ));
                }
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: self.cfg.session,
                    frame_seq,
                    events,
                }));
            }
            DecodedEvent::BookSnapshot {
                instrument,
                change_id,
                bids,
                asks,
                exchange_ts_ms,
            } => {
                self.apply_book_snapshot(
                    &instrument,
                    change_id,
                    &bids,
                    &asks,
                    exchange_ts_ms,
                    received,
                    output,
                )?;
            }
            DecodedEvent::BookChange {
                instrument,
                change_id,
                prev_change_id,
                bids,
                asks,
                exchange_ts_ms,
            } => {
                self.apply_book_change(
                    &instrument,
                    change_id,
                    prev_change_id,
                    &bids,
                    &asks,
                    exchange_ts_ms,
                    received,
                    output,
                )?;
            }
            DecodedEvent::Candle {
                instrument,
                open,
                high,
                low,
                close,
                volume,
                interval_ns,
                start_ts,
                exchange_ts_ms,
            } => {
                let instrument_id = self.instrument_for(&instrument);
                let frame_seq = self.next_frame();
                let env = self.envelope(
                    instrument_id,
                    frame_seq,
                    0,
                    received,
                    Some(ms_to_ts(exchange_ts_ms)),
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
            DecodedEvent::Heartbeat { needs_test } => {
                if needs_test {
                    output.push(SessionAction::SendText(
                        self.rpc("public/test", serde_json::json!({})),
                    ));
                }
            }
            DecodedEvent::Unknown => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "deribit".into(),
                }));
            }
        }
        Ok(())
    }

    fn apply_book_snapshot(
        &mut self,
        instrument_name: &str,
        change_id: u64,
        bids: &[BookLevelChange],
        asks: &[BookLevelChange],
        exchange_ts_ms: i64,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if !self.cfg.enable_l2 {
            return Ok(());
        }
        let Some(book) = self.books.get_mut(instrument_name) else {
            return Ok(());
        };
        book.sync.begin_resync();
        book.sync.request_snapshot();
        let bid_pairs: Vec<(Price, Quantity)> =
            bids.iter().map(|l| (l.price, l.quantity)).collect();
        let ask_pairs: Vec<(Price, Quantity)> =
            asks.iter().map(|l| (l.price, l.quantity)).collect();
        if let Err(err) = book
            .sync
            .book
            .apply_snapshot(&bid_pairs, &ask_pairs, Some(change_id))
        {
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
        book.last_change_id = Some(change_id);
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
                first: change_id,
                last: change_id,
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

    fn apply_book_change(
        &mut self,
        instrument_name: &str,
        change_id: u64,
        prev_change_id: u64,
        bids: &[BookLevelChange],
        asks: &[BookLevelChange],
        exchange_ts_ms: i64,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if !self.cfg.enable_l2 {
            return Ok(());
        }
        let Some(book) = self.books.get_mut(instrument_name) else {
            return Ok(());
        };
        if book.sync.state != SyncState::Live {
            // Waiting for the snapshot — Deribit always sends it first.
            return Ok(());
        }
        if book.last_change_id != Some(prev_change_id) {
            let instrument = book.sync.instrument;
            let expected = book.last_change_id.unwrap_or(0);
            book.sync.note_gap();
            book.last_change_id = None;
            output.push(SessionAction::EmitSystem(SystemEvent::SequenceGap {
                expected,
                actual: prev_change_id,
            }));
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument,
                reason: "book prev_change_id gap".into(),
            }));
            output.push(SessionAction::ResyncInstrument(instrument));
            output.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
            return Ok(());
        }

        let mut changes = Vec::new();
        for (side, levels) in [(BookSide::Bid, bids), (BookSide::Ask, asks)] {
            for l in levels {
                let (op, qty) = match l.action {
                    BookLevelAction::Delete => (BookOperation::Delete, None),
                    BookLevelAction::New | BookLevelAction::Change => {
                        (BookOperation::Upsert, Some(l.quantity))
                    }
                };
                changes.push(BookChange {
                    side,
                    operation: op,
                    price: l.price,
                    quantity: qty,
                });
            }
        }
        if let Err(err) = book.sync.book.apply_changes_atomic(&changes) {
            let instrument = book.sync.instrument;
            book.sync.invalidate("live change apply failed");
            book.last_change_id = None;
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument,
                reason: format!("live change apply failed: {err}"),
            }));
            output.push(SessionAction::ResyncInstrument(instrument));
            output.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
            return Ok(());
        }
        book.sync.book.set_sequence(change_id);
        book.last_change_id = Some(change_id);

        let instrument = self.instrument_for(instrument_name);
        let frame_seq = self.next_frame();
        let env = self.envelope(
            instrument,
            frame_seq,
            0,
            received,
            Some(ms_to_ts(exchange_ts_ms)),
            Some(SequenceRange {
                first: prev_change_id,
                last: change_id,
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

impl SessionMachine for DeribitSession {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Connected { .. } => {
                self.live = false;
                let interval = self.cfg.heartbeat_interval_secs;
                output.push(SessionAction::SendText(self.rpc(
                    "public/set_heartbeat",
                    serde_json::json!({ "interval": interval }),
                )));

                if self.cfg.enable_l2 {
                    for book in self.books.values_mut() {
                        book.sync.begin_resync();
                        book.sync.request_snapshot();
                        book.last_change_id = None;
                    }
                }

                let mut channels = Vec::new();
                let mut index_channels = std::collections::BTreeSet::new();
                for inst in &self.cfg.instruments {
                    // Public sessions cannot use `.raw` for trades or book (auth-only);
                    // `.100ms` is the public interval (Deribit error 13778 otherwise).
                    channels.push(format!("trades.{inst}.100ms"));
                    channels.push(format!("ticker.{inst}.100ms"));
                    index_channels
                        .insert(format!("deribit_price_index.{}", deribit_index_name(inst)));
                    if self.cfg.enable_l2 {
                        channels.push(format!("book.{inst}.100ms"));
                    }
                    for interval in &self.cfg.candle_intervals {
                        channels.push(format!(
                            "chart.trades.{inst}.{}",
                            chart_resolution(*interval)
                        ));
                    }
                }
                channels.extend(index_channels);
                output.push(SessionAction::SendText(self.rpc(
                    "public/subscribe",
                    serde_json::json!({ "channels": channels }),
                )));

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
                    book.last_change_id = None;
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
            SessionInput::HttpResponse { .. } | SessionInput::Pong { .. } => Ok(()),
            SessionInput::Timer { .. } => Ok(()),
            SessionInput::Control { command } => {
                match command {
                    marketfeed_adapter_api::SessionCommand::Stop => {
                        output.push(SessionAction::StopSession(StopReason::Control));
                    }
                    marketfeed_adapter_api::SessionCommand::Resync(id) => {
                        if self.cfg.enable_l2
                            && self.cfg.instrument_ids.values().any(|iid| iid == id)
                        {
                            // Re-subscribe path: reconnect so Deribit re-sends the book snapshot.
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
