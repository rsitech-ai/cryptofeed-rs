//! Synthetic session state machine.

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, EventBatch, ReconnectReason, SessionAction, SessionInput,
    SessionMachine, SessionSpec, StopReason,
};
use marketfeed_book::{BookSynchronizer, OrderBook, SyncLimits, SyncState};
use marketfeed_model::{
    AggressorSide, BookChange, BookDelta, BookLevel, BookOperation, BookSide, BookSnapshot, Candle,
    CatalogView, ConnectionId, EventEnvelope, EventFlags, Fixed, FrameStamp, InstrumentId,
    MarketEvent, Price, Quantity, Quote, SequenceRange, SessionId, SourceId, Statistics24h,
    SystemEvent, TimestampNs, Trade,
};

use crate::proto;
use crate::specification::SYNTHETIC_VENUE_ID;

const SCHEMA_VERSION: u16 = 1;
const DEFAULT_INSTRUMENT: InstrumentId = InstrumentId(1);
const DEFAULT_CONNECTION: ConnectionId = ConnectionId(1);
const DEFAULT_SESSION: SessionId = SessionId(1);

#[derive(Debug)]
pub struct SyntheticSession {
    #[allow(dead_code)]
    catalog: CatalogView,
    subscribed: bool,
    frame_seq: u64,
    sync: BookSynchronizer,
    live: bool,
}

impl SyntheticSession {
    pub fn new(_spec: SessionSpec, catalog: CatalogView) -> Self {
        let book = OrderBook::new(2, 3, Some(50));
        let mut sync = BookSynchronizer::new(DEFAULT_INSTRUMENT, book, SyncLimits::default());
        sync.request_snapshot();
        Self {
            catalog,
            subscribed: false,
            frame_seq: 0,
            sync,
            live: false,
        }
    }

    fn next_frame_seq(&mut self) -> u64 {
        self.frame_seq += 1;
        self.frame_seq
    }

    fn envelope(
        &self,
        frame_seq: u64,
        event_index: u16,
        received: FrameStamp,
        seq: Option<u64>,
        flags: EventFlags,
        payload: MarketEvent,
    ) -> EventEnvelope {
        EventEnvelope {
            schema_version: SCHEMA_VERSION,
            venue: SYNTHETIC_VENUE_ID,
            instrument: Some(DEFAULT_INSTRUMENT),
            connection: DEFAULT_CONNECTION,
            session: DEFAULT_SESSION,
            frame_seq,
            event_index,
            exchange_ts: None,
            receive_ts: received.receive_ts,
            source_sequence: seq.map(|s| SequenceRange { first: s, last: s }),
            flags: flags.union(EventFlags::SYNTHETIC),
            payload,
        }
    }

    fn handle_text(
        &mut self,
        text: &str,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        let line = text.trim();
        if line.is_empty() {
            return Ok(());
        }

        if line == proto::DISCONNECT {
            output.push(SessionAction::Reconnect(ReconnectReason::Control));
            return Ok(());
        }

        if let Some(rest) = line.strip_prefix(proto::SUB_PREFIX) {
            let _symbol = rest.trim();
            self.subscribed = true;
            output.push(SessionAction::SendText(Bytes::from_static(b"ACK SUB\n")));
            output.push(SessionAction::EmitSystem(
                SystemEvent::SubscriptionStateChanged {
                    state: "subscribed".into(),
                },
            ));
            return Ok(());
        }

        if let Some(rest) = line.strip_prefix(proto::UNSUB_PREFIX) {
            let _symbol = rest.trim();
            self.subscribed = false;
            output.push(SessionAction::SendText(Bytes::from_static(b"ACK UNSUB\n")));
            output.push(SessionAction::EmitSystem(
                SystemEvent::SubscriptionStateChanged {
                    state: "unsubscribed".into(),
                },
            ));
            return Ok(());
        }

        if let Some(rest) = line.strip_prefix(proto::STATS24H_PREFIX) {
            return self.on_stats24h(rest, received, output);
        }

        if !self.subscribed {
            output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                detail: "message before subscribe".into(),
            }));
            return Ok(());
        }

        if let Some(rest) = line.strip_prefix(proto::TRADE_PREFIX) {
            return self.on_trade(rest, received, output);
        }
        if let Some(rest) = line.strip_prefix(proto::QUOTE_PREFIX) {
            return self.on_quote(rest, received, output);
        }
        if let Some(rest) = line.strip_prefix(proto::CANDLE_PREFIX) {
            return self.on_candle(rest, received, output);
        }
        if let Some(rest) = line.strip_prefix(proto::BOOK_SNAP_PREFIX) {
            return self.on_book_snap(rest, received, output);
        }
        if let Some(rest) = line.strip_prefix(proto::BOOK_DELTA_PREFIX) {
            return self.on_book_delta(rest, received, output);
        }

        output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
            detail: line.chars().take(64).collect(),
        }));
        Ok(())
    }

    fn on_stats24h(
        &mut self,
        rest: &str,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        // STATS24H <open> <high> <low> <close> <volume> <quote_volume>
        let parts: Vec<&str> = rest.split_whitespace().collect();
        if parts.len() < 6 {
            output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                detail: "STATS24H arity".into(),
            }));
            return Ok(());
        }
        let Some(open) = parse_price(parts[0], output)? else {
            return Ok(());
        };
        let Some(high) = parse_price(parts[1], output)? else {
            return Ok(());
        };
        let Some(low) = parse_price(parts[2], output)? else {
            return Ok(());
        };
        let Some(close) = parse_price(parts[3], output)? else {
            return Ok(());
        };
        let Some(volume) = parse_qty(parts[4], output)? else {
            return Ok(());
        };
        let Some(quote_volume) = parse_qty(parts[5], output)? else {
            return Ok(());
        };
        let frame_seq = self.next_frame_seq();
        let env = self.envelope(
            frame_seq,
            0,
            received,
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
            session: DEFAULT_SESSION,
            frame_seq,
            events: vec![env],
        }));
        Ok(())
    }

    fn on_trade(
        &mut self,
        rest: &str,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        // TRADE <seq> <price> <qty> BUY|SELL [trade_id]
        let parts: Vec<&str> = rest.split_whitespace().collect();
        if parts.len() < 4 {
            output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                detail: "TRADE arity".into(),
            }));
            return Ok(());
        }
        let Some(seq) = parse_u64(parts[0], output)? else {
            return Ok(());
        };
        let Some(price) = parse_price(parts[1], output)? else {
            return Ok(());
        };
        let Some(quantity) = parse_qty(parts[2], output)? else {
            return Ok(());
        };
        let aggressor = match parts[3] {
            "BUY" => AggressorSide::Buy,
            "SELL" => AggressorSide::Sell,
            other => {
                output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                    detail: format!("bad side {other}"),
                }));
                return Ok(());
            }
        };
        let trade_id = parts.get(4).map(|s| SourceId((*s).to_string()));

        let frame_seq = self.next_frame_seq();
        let env = self.envelope(
            frame_seq,
            0,
            received,
            Some(seq),
            EventFlags::empty(),
            MarketEvent::Trade(Trade {
                price,
                quantity,
                aggressor,
                trade_id,
            }),
        );
        output.push(SessionAction::EmitBatch(EventBatch {
            session: DEFAULT_SESSION,
            frame_seq,
            events: vec![env],
        }));
        Ok(())
    }

    fn on_quote(
        &mut self,
        rest: &str,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        // QUOTE <bid> <ask> [<bid_qty> <ask_qty>]
        let parts: Vec<&str> = rest.split_whitespace().collect();
        if parts.len() < 2 {
            output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                detail: "QUOTE arity".into(),
            }));
            return Ok(());
        }
        let Some(bid_price) = parse_price(parts[0], output)? else {
            return Ok(());
        };
        let Some(ask_price) = parse_price(parts[1], output)? else {
            return Ok(());
        };
        let bid_quantity = if parts.len() >= 4 {
            match parse_qty(parts[2], output)? {
                Some(q) => Some(q),
                None => return Ok(()),
            }
        } else {
            None
        };
        let ask_quantity = if parts.len() >= 4 {
            match parse_qty(parts[3], output)? {
                Some(q) => Some(q),
                None => return Ok(()),
            }
        } else {
            None
        };
        let frame_seq = self.next_frame_seq();
        let env = self.envelope(
            frame_seq,
            0,
            received,
            None,
            EventFlags::empty(),
            MarketEvent::Quote(Quote {
                bid_price,
                ask_price,
                bid_quantity,
                ask_quantity,
            }),
        );
        output.push(SessionAction::EmitBatch(EventBatch {
            session: DEFAULT_SESSION,
            frame_seq,
            events: vec![env],
        }));
        Ok(())
    }

    fn on_candle(
        &mut self,
        rest: &str,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        // CANDLE <open> <high> <low> <close> <volume> <interval_ns> <start_ts>
        let parts: Vec<&str> = rest.split_whitespace().collect();
        if parts.len() < 7 {
            output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                detail: "CANDLE arity".into(),
            }));
            return Ok(());
        }
        let Some(open) = parse_price(parts[0], output)? else {
            return Ok(());
        };
        let Some(high) = parse_price(parts[1], output)? else {
            return Ok(());
        };
        let Some(low) = parse_price(parts[2], output)? else {
            return Ok(());
        };
        let Some(close) = parse_price(parts[3], output)? else {
            return Ok(());
        };
        let Some(volume) = parse_qty(parts[4], output)? else {
            return Ok(());
        };
        let Some(interval_ns) = parse_i64(parts[5], output)? else {
            return Ok(());
        };
        let Some(start_ts) = parse_i64(parts[6], output)? else {
            return Ok(());
        };
        let frame_seq = self.next_frame_seq();
        let env = self.envelope(
            frame_seq,
            0,
            received,
            None,
            EventFlags::empty(),
            MarketEvent::Candle(Candle {
                open,
                high,
                low,
                close,
                volume,
                interval_ns,
                start_ts: TimestampNs(start_ts),
            }),
        );
        output.push(SessionAction::EmitBatch(EventBatch {
            session: DEFAULT_SESSION,
            frame_seq,
            events: vec![env],
        }));
        Ok(())
    }

    fn on_book_snap(
        &mut self,
        rest: &str,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        // BOOK_SNAP <seq> BID p:q[,...] ASK p:q[,...]
        let parts: Vec<&str> = rest.split_whitespace().collect();
        if parts.len() < 5 || parts[1] != "BID" || parts[3] != "ASK" {
            output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                detail: "BOOK_SNAP syntax".into(),
            }));
            return Ok(());
        }
        let Some(seq) = parse_u64(parts[0], output)? else {
            return Ok(());
        };
        let Some(bids) = parse_levels(parts[2], output)? else {
            return Ok(());
        };
        let Some(asks) = parse_levels(parts[4], output)? else {
            return Ok(());
        };

        let bid_pairs: Vec<_> = bids.iter().map(|l| (l.price, l.quantity)).collect();
        let ask_pairs: Vec<_> = asks.iter().map(|l| (l.price, l.quantity)).collect();

        match self
            .sync
            .apply_snapshot_and_drain(&bid_pairs, &ask_pairs, seq)
        {
            Ok(()) => {
                self.live = true;
                let frame_seq = self.next_frame_seq();
                let snap = BookSnapshot {
                    bids,
                    asks,
                    depth: Some(50),
                    checksum: None,
                };
                let env = self.envelope(
                    frame_seq,
                    0,
                    received,
                    Some(seq),
                    EventFlags::SNAPSHOT,
                    MarketEvent::BookSnapshot(snap),
                );
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: DEFAULT_SESSION,
                    frame_seq,
                    events: vec![env],
                }));
                output.push(SessionAction::EmitSystem(SystemEvent::BookResynchronized {
                    instrument: DEFAULT_INSTRUMENT,
                }));
                output.push(SessionAction::MarkLive);
            }
            Err(err) => {
                self.live = false;
                output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                    instrument: DEFAULT_INSTRUMENT,
                    reason: err.to_string(),
                }));
                output.push(SessionAction::ResyncInstrument(DEFAULT_INSTRUMENT));
            }
        }
        Ok(())
    }

    fn on_book_delta(
        &mut self,
        rest: &str,
        received: FrameStamp,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        // BOOK_DELTA <seq> BID|ASK UPSERT|DELETE <price> [<qty>]
        let parts: Vec<&str> = rest.split_whitespace().collect();
        if parts.len() < 4 {
            output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                detail: "BOOK_DELTA arity".into(),
            }));
            return Ok(());
        }
        let Some(seq) = parse_u64(parts[0], output)? else {
            return Ok(());
        };
        let side = match parts[1] {
            "BID" => BookSide::Bid,
            "ASK" => BookSide::Ask,
            other => {
                output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                    detail: format!("bad side {other}"),
                }));
                return Ok(());
            }
        };
        let operation = match parts[2] {
            "UPSERT" => BookOperation::Upsert,
            "DELETE" => BookOperation::Delete,
            other => {
                output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                    detail: format!("bad op {other}"),
                }));
                return Ok(());
            }
        };
        let Some(price) = parse_price(parts[3], output)? else {
            return Ok(());
        };
        let quantity = if parts.len() > 4 {
            parse_qty(parts[4], output)?
        } else {
            None
        };

        if self.sync.state != SyncState::Live {
            // Buffer until snapshot (spec: deltas may arrive before snapshot).
            let bytes_len = rest.len();
            if let Err(err) = self.sync.buffer_delta(marketfeed_book::BufferedDelta {
                sequence: seq,
                bytes_len,
                received_mono_ns: received.mono_ns,
                side,
                operation,
                price,
                quantity,
            }) {
                output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                    instrument: DEFAULT_INSTRUMENT,
                    reason: err.to_string(),
                }));
                output.push(SessionAction::ResyncInstrument(DEFAULT_INSTRUMENT));
            }
            return Ok(());
        }

        match self
            .sync
            .on_live_delta(seq, side, operation, price, quantity)
        {
            Ok(()) => {
                let frame_seq = self.next_frame_seq();
                let change = BookChange {
                    side,
                    operation,
                    price,
                    quantity,
                };
                let env = self.envelope(
                    frame_seq,
                    0,
                    received,
                    Some(seq),
                    EventFlags::DELTA,
                    MarketEvent::BookDelta(BookDelta {
                        changes: vec![change],
                        checksum: None,
                    }),
                );
                output.push(SessionAction::EmitBatch(EventBatch {
                    session: DEFAULT_SESSION,
                    frame_seq,
                    events: vec![env],
                }));
            }
            Err(_) => {
                self.live = false;
                let expected = self.sync.expected_sequence.unwrap_or(0);
                output.push(SessionAction::EmitSystem(SystemEvent::SequenceGap {
                    expected,
                    actual: seq,
                }));
                output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                    instrument: DEFAULT_INSTRUMENT,
                    reason: "sequence gap".into(),
                }));
                self.sync.invalidate("sequence gap");
                output.push(SessionAction::ResyncInstrument(DEFAULT_INSTRUMENT));
                output.push(SessionAction::Reconnect(ReconnectReason::SequenceGap));
            }
        }
        Ok(())
    }

    fn on_connected(&mut self, output: &mut ActionBuffer) {
        self.subscribed = false;
        self.live = false;
        self.sync.begin_resync();
        self.sync.request_snapshot();
        output.push(SessionAction::EmitSystem(
            SystemEvent::ConnectionStateChanged {
                state: "connected".into(),
            },
        ));
        // Auto-subscribe the default instrument for the mock venue.
        output.push(SessionAction::SendText(Bytes::from_static(
            b"SUB BTC-USD\n",
        )));
    }

    fn on_disconnected(&mut self, output: &mut ActionBuffer) {
        self.subscribed = false;
        self.live = false;
        self.sync.invalidate("disconnected");
        output.push(SessionAction::EmitSystem(
            SystemEvent::ConnectionStateChanged {
                state: "disconnected".into(),
            },
        ));
        output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
            instrument: DEFAULT_INSTRUMENT,
            reason: "disconnected".into(),
        }));
    }
}

impl SessionMachine for SyntheticSession {
    fn prepare_dynamic_subscription(
        &self,
        command: &marketfeed_adapter_api::SessionCommand,
    ) -> Result<marketfeed_adapter_api::SubscriptionWireAction, AdapterError> {
        use marketfeed_adapter_api::{SessionCommand, SubscriptionWireAction};

        let payload = match command {
            SessionCommand::Subscribe(symbols) => format!("SUB {}\n", symbols.join(",")),
            SessionCommand::Unsubscribe(symbols) => format!("UNSUB {}\n", symbols.join(",")),
            SessionCommand::Replace(symbols) => format!("REPLACE {}\n", symbols.join(",")),
            SessionCommand::Resync(_) | SessionCommand::Stop => {
                return Err(AdapterError::UnsupportedCapability(
                    "prepared subscription control requires a subscription command".into(),
                ));
            }
        };
        Ok(SubscriptionWireAction::Text(Bytes::from(payload)))
    }

    fn commit_dynamic_subscription(&mut self, command: &marketfeed_adapter_api::SessionCommand) {
        use marketfeed_adapter_api::SessionCommand;

        match command {
            SessionCommand::Subscribe(_) => self.subscribed = true,
            SessionCommand::Unsubscribe(symbols) => {
                if symbols.is_empty() {
                    self.subscribed = false;
                }
            }
            SessionCommand::Replace(symbols) => self.subscribed = !symbols.is_empty(),
            SessionCommand::Resync(_) | SessionCommand::Stop => {}
        }
    }

    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Connected { .. } => {
                self.on_connected(output);
                Ok(())
            }
            SessionInput::Disconnected { .. } => {
                self.on_disconnected(output);
                Ok(())
            }
            SessionInput::TextFrame { bytes, received } => {
                let text =
                    std::str::from_utf8(bytes).map_err(|e| AdapterError::Parse(e.to_string()))?;
                // Clone line so we don't hold borrow across mutable self use.
                let line = text.to_owned();
                self.handle_text(&line, received, output)
            }
            SessionInput::BinaryFrame { .. } => {
                output.push(SessionAction::EmitSystem(SystemEvent::UnknownMessage {
                    detail: "binary not supported".into(),
                }));
                Ok(())
            }
            SessionInput::Pong { .. } | SessionInput::HttpResponse { .. } => Ok(()),
            SessionInput::Timer { .. } => Ok(()),
            SessionInput::Control { command } => {
                match command {
                    marketfeed_adapter_api::SessionCommand::Stop => {
                        output.push(SessionAction::StopSession(StopReason::Control));
                    }
                    marketfeed_adapter_api::SessionCommand::Resync(_) => {
                        self.sync.begin_resync();
                        self.sync.request_snapshot();
                        self.live = false;
                        output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                            instrument: DEFAULT_INSTRUMENT,
                            reason: "control resync".into(),
                        }));
                    }
                    marketfeed_adapter_api::SessionCommand::Subscribe(_)
                    | marketfeed_adapter_api::SessionCommand::Unsubscribe(_)
                    | marketfeed_adapter_api::SessionCommand::Replace(_) => {
                        return Err(AdapterError::UnsupportedCapability(
                            "dynamic subscriptions require the runner prepare/commit path".into(),
                        ));
                    }
                }
                Ok(())
            }
        }
    }

    fn book_snapshot(&self, instrument: InstrumentId, depth: Option<u32>) -> Option<BookSnapshot> {
        if instrument != DEFAULT_INSTRUMENT {
            return None;
        }
        let (mut bids, mut asks) = self.sync.book.snapshot_levels()?;
        if let Some(d) = depth {
            let n = d as usize;
            bids.truncate(n);
            asks.truncate(n);
        }
        Some(BookSnapshot {
            bids,
            asks,
            depth: depth.or(Some(50)),
            checksum: None,
        })
    }
}

fn parse_u64(s: &str, output: &mut ActionBuffer) -> Result<Option<u64>, AdapterError> {
    match s.parse::<u64>() {
        Ok(v) => Ok(Some(v)),
        Err(_) => {
            output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                detail: format!("bad u64 {s}"),
            }));
            Ok(None)
        }
    }
}

fn parse_i64(s: &str, output: &mut ActionBuffer) -> Result<Option<i64>, AdapterError> {
    match s.parse::<i64>() {
        Ok(v) => Ok(Some(v)),
        Err(_) => {
            output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                detail: format!("bad i64 {s}"),
            }));
            Ok(None)
        }
    }
}

fn parse_price(s: &str, output: &mut ActionBuffer) -> Result<Option<Price>, AdapterError> {
    match Fixed::parse_str(s) {
        Ok(v) => Ok(Some(Price(v))),
        Err(e) => {
            output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                detail: format!("bad price {s}: {e}"),
            }));
            Ok(None)
        }
    }
}

fn parse_qty(s: &str, output: &mut ActionBuffer) -> Result<Option<Quantity>, AdapterError> {
    match Fixed::parse_str(s) {
        Ok(v) => Ok(Some(Quantity(v))),
        Err(e) => {
            output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                detail: format!("bad qty {s}: {e}"),
            }));
            Ok(None)
        }
    }
}

fn parse_levels(
    s: &str,
    output: &mut ActionBuffer,
) -> Result<Option<Vec<BookLevel>>, AdapterError> {
    if s.is_empty() || s == "-" {
        return Ok(Some(Vec::new()));
    }
    let mut out = Vec::new();
    for part in s.split(',') {
        let Some((p, q)) = part.split_once(':') else {
            output.push(SessionAction::EmitSystem(SystemEvent::ParseError {
                detail: format!("bad level {part}"),
            }));
            return Ok(None);
        };
        let Some(price) = parse_price(p, output)? else {
            return Ok(None);
        };
        let Some(quantity) = parse_qty(q, output)? else {
            return Ok(None);
        };
        out.push(BookLevel { price, quantity });
    }
    Ok(Some(out))
}

/// Drive the machine with a script of text frames; return emitted market payloads + system events.
#[cfg(test)]
pub fn drive_script(
    session: &mut SyntheticSession,
    frames: &[&str],
    start_ts: i64,
) -> Result<(Vec<MarketEvent>, Vec<SystemEvent>, Vec<SessionAction>), AdapterError> {
    use marketfeed_model::TimestampNs;

    let mut markets = Vec::new();
    let mut systems = Vec::new();
    let mut other = Vec::new();
    let mut buf = ActionBuffer::new();
    let mut ts = start_ts;

    session.on_input(
        SessionInput::Connected {
            now: TimestampNs(ts),
        },
        &mut buf,
    )?;
    collect(&mut buf, &mut markets, &mut systems, &mut other);

    // Synthetic auto-sends SUB on connect; feed subscription confirm on the wire.
    ts += 1;
    let mut sub = b"SUB BTC-USD".to_vec();
    session.on_input(
        SessionInput::TextFrame {
            bytes: &mut sub,
            received: FrameStamp {
                receive_ts: TimestampNs(ts),
                mono_ns: ts as u64,
            },
        },
        &mut buf,
    )?;
    collect(&mut buf, &mut markets, &mut systems, &mut other);

    for frame in frames {
        ts += 1;
        let mut bytes = frame.as_bytes().to_vec();
        session.on_input(
            SessionInput::TextFrame {
                bytes: &mut bytes,
                received: FrameStamp {
                    receive_ts: TimestampNs(ts),
                    mono_ns: ts as u64,
                },
            },
            &mut buf,
        )?;
        collect(&mut buf, &mut markets, &mut systems, &mut other);
    }

    Ok((markets, systems, other))
}

#[cfg(test)]
fn collect(
    buf: &mut ActionBuffer,
    markets: &mut Vec<MarketEvent>,
    systems: &mut Vec<SystemEvent>,
    other: &mut Vec<SessionAction>,
) {
    for action in buf.drain() {
        match action {
            SessionAction::EmitBatch(batch) => {
                for env in batch.events {
                    markets.push(env.payload);
                }
            }
            SessionAction::EmitSystem(ev) => systems.push(ev),
            other_action => other.push(other_action),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketfeed_adapter_api::{ConcreteSubscriptionSet, SessionSpec};
    use marketfeed_book::BookValidity;
    use marketfeed_model::{CatalogVersion, TimestampNs, VenueId};

    fn session() -> SyntheticSession {
        SyntheticSession::new(
            SessionSpec {
                endpoint_name: "ws".into(),
                subscriptions: ConcreteSubscriptionSet::default(),
            },
            CatalogView::new(VenueId(1), CatalogVersion(1)),
        )
    }

    #[test]
    fn connect_subscribe_trade_book_gap_reconnect_is_deterministic() {
        let mut a = session();
        let mut b = session();

        let script = [
            "BOOK_SNAP 10 BID 100.00:1.000 ASK 101.00:1.500",
            "TRADE 11 100.50 0.250 BUY t1",
            "BOOK_DELTA 11 BID UPSERT 100.50 0.500",
            "BOOK_DELTA 13 ASK UPSERT 101.50 0.250", // gap: expected 12
        ];

        let (m1, s1, o1) = drive_script(&mut a, &script, 1_000).unwrap();
        let (m2, s2, o2) = drive_script(&mut b, &script, 1_000).unwrap();

        assert_eq!(m1, m2, "market events must match across identical drives");
        assert_eq!(s1, s2, "system events must match");
        assert_eq!(o1, o2, "other actions must match");

        assert!(
            m1.iter().any(|e| matches!(e, MarketEvent::BookSnapshot(_))),
            "expected snapshot"
        );
        assert!(
            m1.iter().any(|e| matches!(e, MarketEvent::Trade(_))),
            "expected trade"
        );
        assert!(
            m1.iter().any(|e| matches!(e, MarketEvent::BookDelta(_))),
            "expected delta"
        );
        assert!(
            s1.iter()
                .any(|e| matches!(e, SystemEvent::SequenceGap { .. })),
            "expected sequence gap system event"
        );
        assert!(
            o1.iter()
                .any(|a| matches!(a, SessionAction::Reconnect(ReconnectReason::SequenceGap))),
            "expected reconnect action"
        );
        assert_eq!(a.sync.book.validity(), BookValidity::Invalid);
    }

    #[test]
    fn quote_and_candle_fixtures_exact_fixed() {
        let mut s = session();
        let (markets, _, _) = drive_script(
            &mut s,
            &[
                "QUOTE 100.00 101.00 1.5 2.0",
                "CANDLE 100.00 102.00 99.50 101.25 10.5 60000000000 1700000000000000000",
            ],
            1_000,
        )
        .unwrap();
        assert!(
            markets.iter().any(|e| matches!(
                e,
                MarketEvent::Quote(q)
                    if q.bid_price.0 == Fixed::parse_str("100.00").unwrap()
                        && q.ask_price.0 == Fixed::parse_str("101.00").unwrap()
                        && q.bid_quantity == Some(Quantity(Fixed::parse_str("1.5").unwrap()))
            )),
            "{markets:?}"
        );
        assert!(
            markets.iter().any(|e| matches!(
                e,
                MarketEvent::Candle(c)
                    if c.open.0 == Fixed::parse_str("100.00").unwrap()
                        && c.close.0 == Fixed::parse_str("101.25").unwrap()
                        && c.interval_ns == 60_000_000_000
                        && c.start_ts == TimestampNs(1_700_000_000_000_000_000)
            )),
            "{markets:?}"
        );
    }

    #[test]
    fn disconnect_invalidates_book() {
        let mut s = session();
        let (markets, systems, _) = drive_script(
            &mut s,
            &["BOOK_SNAP 1 BID 100.00:1.000 ASK 101.00:1.000"],
            0,
        )
        .unwrap();
        assert!(!markets.is_empty());
        assert!(s.sync.book.validity() == BookValidity::Valid);

        let mut buf = ActionBuffer::new();
        s.on_input(
            SessionInput::Disconnected {
                reason: marketfeed_adapter_api::DisconnectReason::RemoteClose,
                now: TimestampNs(99),
            },
            &mut buf,
        )
        .unwrap();
        assert_eq!(s.sync.book.validity(), BookValidity::Invalid);
        assert!(
            systems
                .iter()
                .chain(buf.as_slice().iter().filter_map(|a| match a {
                    SessionAction::EmitSystem(e) => Some(e),
                    _ => None,
                }))
                .any(|e| matches!(e, SystemEvent::BookInvalidated { .. }))
        );
    }
}

#[cfg(test)]
mod control_query_tests {
    use super::*;
    use marketfeed_adapter_api::{ConcreteSubscriptionSet, SessionMachine, SessionSpec};
    use marketfeed_model::{CatalogVersion, CatalogView, FrameStamp, TimestampNs};

    #[test]
    fn control_book_snapshot_after_snap() {
        let mut s = SyntheticSession::new(
            SessionSpec {
                endpoint_name: "ws".into(),
                subscriptions: ConcreteSubscriptionSet::default(),
            },
            CatalogView::new(SYNTHETIC_VENUE_ID, CatalogVersion(1)),
        );
        let mut buf = ActionBuffer::new();
        s.on_input(
            SessionInput::Connected {
                now: TimestampNs(1),
            },
            &mut buf,
        )
        .unwrap();
        // Auto-sub + ACK path: drive SUB manually then BOOK_SNAP
        let mut bytes = b"SUB BTC-USD".to_vec();
        s.on_input(
            SessionInput::TextFrame {
                bytes: &mut bytes,
                received: FrameStamp {
                    receive_ts: TimestampNs(2),
                    mono_ns: 2,
                },
            },
            &mut buf,
        )
        .unwrap();
        let mut bytes = b"BOOK_SNAP 1 BID 100.00:1.000 ASK 101.00:2.000".to_vec();
        s.on_input(
            SessionInput::TextFrame {
                bytes: &mut bytes,
                received: FrameStamp {
                    receive_ts: TimestampNs(3),
                    mono_ns: 3,
                },
            },
            &mut buf,
        )
        .unwrap();
        let snap = s
            .book_snapshot(InstrumentId(1), Some(1))
            .expect("live book");
        assert_eq!(snap.bids.len(), 1);
        assert_eq!(snap.asks.len(), 1);
    }
}
