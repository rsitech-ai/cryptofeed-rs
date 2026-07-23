//! Coinbase International authenticated MD SessionMachine.

use std::collections::HashMap;

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, EventBatch, ReconnectReason, SensitiveBytes, SessionAction,
    SessionCommand, SessionInput, SessionMachine, SessionSpec, StopReason,
};
use marketfeed_book::{BookSynchronizer, OrderBook, SyncLimits, SyncState};
use marketfeed_model::{
    BookChange, BookDelta, BookOperation, BookSide, BookSnapshot, CatalogView, ConnectionId,
    EventEnvelope, EventFlags, FrameStamp, InstrumentId, MarketEvent, Price, Quantity, Quote,
    SequenceRange, SessionId, SystemEvent, TimestampNs, Trade,
};

use crate::intl::credentials::CoinbaseIntlCredentials;
use crate::intl::messages::{BookSideWire, DecodedEvent, decode_text, ns_to_ts, trade_id_source};
use crate::intl::specification::COINBASE_INTL_VENUE_ID;

const SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone)]
pub struct CoinbaseIntlSessionConfig {
    pub products: Vec<String>,
    pub instrument_ids: HashMap<String, InstrumentId>,
    pub connection: ConnectionId,
    pub session: SessionId,
    pub enable_l2: bool,
    pub price_scale: u8,
    pub qty_scale: u8,
    pub credentials: CoinbaseIntlCredentials,
}

impl Default for CoinbaseIntlSessionConfig {
    fn default() -> Self {
        let mut instrument_ids = HashMap::new();
        instrument_ids.insert("BTC-PERP".into(), InstrumentId(1));
        Self {
            products: vec!["BTC-PERP".into()],
            instrument_ids,
            connection: ConnectionId(1),
            session: SessionId(1),
            enable_l2: false,
            price_scale: 1,
            qty_scale: 4,
            credentials: CoinbaseIntlCredentials::fixture(),
        }
    }
}

#[derive(Debug)]
struct ProductBook {
    sync: BookSynchronizer,
}

#[derive(Debug)]
pub struct CoinbaseIntlSession {
    #[allow(dead_code)]
    catalog: CatalogView,
    cfg: CoinbaseIntlSessionConfig,
    frame_seq: u64,
    /// Coinbase INTX sequence numbers are global to one WebSocket session, not
    /// per channel or product:
    /// <https://docs.cdp.coinbase.com/international-exchange/websocket-feed/websocket-overview#sequence-numbers>
    last_sequence: Option<u64>,
    books: HashMap<String, ProductBook>,
    live: bool,
}

impl CoinbaseIntlSession {
    pub fn new(spec: SessionSpec, catalog: CatalogView, cfg: CoinbaseIntlSessionConfig) -> Self {
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
            last_sequence: None,
            books,
            live: false,
        }
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
            venue: COINBASE_INTL_VENUE_ID,
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

    fn subscribe_payload(&self, now: TimestampNs) -> Bytes {
        let ts_secs = now.0 / 1_000_000_000;
        let auth = self.cfg.credentials.sign_subscribe(ts_secs);
        let mut channels = vec!["MATCH", "LEVEL1"];
        if self.cfg.enable_l2 {
            channels.push("LEVEL2");
        }
        Bytes::from(
            serde_json::json!({
                "type": "SUBSCRIBE",
                "product_ids": self.cfg.products,
                "channels": channels,
                "time": auth.time,
                "key": auth.key,
                "passphrase": auth.passphrase,
                "signature": auth.signature,
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

    fn accept_sequence(&mut self, sequence: u64, output: &mut ActionBuffer) -> bool {
        let Some(last) = self.last_sequence else {
            self.last_sequence = Some(sequence);
            return true;
        };
        let expected = last.saturating_add(1);
        if sequence == expected {
            self.last_sequence = Some(sequence);
            return true;
        }

        self.live = false;
        output.push(SessionAction::EmitSystem(SystemEvent::SequenceGap {
            expected,
            actual: sequence,
        }));
        for book in self.books.values_mut() {
            let instrument = book.sync.instrument;
            book.sync.invalidate("session sequence discontinuity");
            output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                instrument,
                reason: "coinbase-intl session sequence discontinuity".into(),
            }));
        }
        output.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
        false
    }

    fn handle_decoded(
        &mut self,
        decoded: DecodedEvent,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match decoded.sequence() {
            Some(sequence) => {
                if !self.accept_sequence(sequence, output) {
                    return Ok(());
                }
            }
            None if decoded.requires_sequence() => {
                output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                    detail: "coinbase-intl data message missing session sequence".into(),
                }));
                output.push(SessionAction::Reconnect(ReconnectReason::Protocol));
                return Ok(());
            }
            None => {}
        }
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
                if !self.cfg.enable_l2 {
                    self.maybe_mark_live(output);
                }
            }
            DecodedEvent::Quote {
                sequence,
                product_id,
                bid_price,
                bid_qty,
                ask_price,
                ask_qty,
                exchange_ts_ns,
            } => {
                let (Some(bid_price), Some(ask_price)) = (bid_price, ask_price) else {
                    // Coinbase International legitimately sends one-sided LEVEL1
                    // snapshots. They advance the session-global sequence, but the
                    // canonical Quote model requires both sides.
                    return Ok(());
                };
                let instrument = self.instrument_for(&product_id);
                let frame_seq = self.next_frame();
                let env = self.envelope(
                    instrument,
                    frame_seq,
                    0,
                    received,
                    exchange_ts_ns.map(ns_to_ts),
                    sequence.map(|value| SequenceRange {
                        first: value,
                        last: value,
                    }),
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
                if !self.cfg.enable_l2 {
                    self.maybe_mark_live(output);
                }
            }
            DecodedEvent::BookSnapshot {
                sequence,
                product_id,
                bids,
                asks,
            } => {
                self.apply_book_snapshot(&product_id, &bids, &asks, sequence, received, output)?;
            }
            DecodedEvent::BookDelta {
                sequence,
                product_id,
                changes,
                exchange_ts_ns,
            } => {
                self.apply_book_delta(
                    &product_id,
                    &changes,
                    exchange_ts_ns,
                    sequence,
                    received,
                    output,
                )?;
            }
            DecodedEvent::SubscribeAck => {
                output.push(SessionAction::EmitSystem(
                    SystemEvent::SubscriptionStateChanged {
                        state: "subscribed".into(),
                    },
                ));
            }
            DecodedEvent::Error(msg) => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: format!("coinbase-intl error: {msg}"),
                }));
            }
            DecodedEvent::Unknown => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "coinbase-intl".into(),
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
        sequence: Option<u64>,
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
            sequence.map(|value| SequenceRange {
                first: value,
                last: value,
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
        changes: &[crate::intl::messages::BookLevelChange],
        exchange_ts_ns: Option<i64>,
        sequence: Option<u64>,
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
            sequence.map(|value| SequenceRange {
                first: value,
                last: value,
            }),
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

impl SessionMachine for CoinbaseIntlSession {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Connected { now } => {
                self.live = false;
                self.last_sequence = None;
                if self.cfg.enable_l2 {
                    for book in self.books.values_mut() {
                        book.sync.begin_resync();
                        book.sync.request_snapshot();
                    }
                }
                output.push(SessionAction::SendSensitiveText(SensitiveBytes::new(
                    self.subscribe_payload(now),
                )));
                Ok(())
            }
            SessionInput::Disconnected { .. } => {
                self.live = false;
                self.last_sequence = None;
                for book in self.books.values_mut() {
                    book.sync.invalidate("disconnected");
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
                    detail: "coinbase-intl unexpected binary".into(),
                }));
                Ok(())
            }
            SessionInput::HttpResponse { .. } => Ok(()),
            SessionInput::Pong { .. } => Ok(()),
            SessionInput::Timer { .. } => Ok(()),
            SessionInput::Control { command } => {
                if matches!(command, SessionCommand::Stop) {
                    output.push(SessionAction::StopSession(StopReason::Control));
                }
                Ok(())
            }
        }
    }
}
