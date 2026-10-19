use crate::protocol::{self, Message};
use bytes::Bytes;
use marketfeed_adapter_api::*;
use marketfeed_model::*;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn subscription(
    channel: &Channel,
    coin: &str,
    spot: bool,
) -> Result<Value, AdapterError> {
    let mut value = match channel {
        Channel::Trades => json!({"type":"trades"}),
        Channel::Quote => json!({"type":"bbo"}),
        Channel::L2Book { depth, cadence } => {
            if depth.is_some_and(|d| d == 0 || d > 20) || cadence.is_some() {
                return Err(AdapterError::UnsupportedCapability(
                    "Hyperliquid L2 snapshots: depth 1..20, native cadence only".into(),
                ));
            }
            json!({"type":"l2Book"})
        }
        Channel::Candles { interval } => json!({"type":"candle", "interval": match interval {
            CandleInterval::M1 => "1m", CandleInterval::M5 => "5m", CandleInterval::M15 => "15m",
            CandleInterval::H1 => "1h", CandleInterval::D1 => "1d" }}),
        Channel::MarkPrice | Channel::Statistics24h => json!({"type":"activeAssetCtx"}),
        Channel::Funding | Channel::OpenInterest | Channel::IndexPrice if !spot => {
            json!({"type":"activeAssetCtx"})
        }
        c => {
            return Err(AdapterError::UnsupportedCapability(format!(
                "Hyperliquid {c:?}"
            )));
        }
    };
    value["coin"] = json!(coin);
    Ok(value)
}
pub(crate) fn subscriptions(
    request: &ConcreteSubscriptionSet,
    catalog: &CatalogView,
    spot: bool,
) -> Result<Vec<Value>, AdapterError> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for item in &request.items {
        let inst = catalog
            .instruments
            .iter()
            .find(|i| i.id == item.instrument)
            .ok_or_else(|| {
                AdapterError::Catalog("missing requested Hyperliquid instrument".into())
            })?;
        if inst.status != InstrumentStatus::Active
            || (inst.key.kind == InstrumentKind::Spot) != spot
        {
            return Err(AdapterError::Catalog(
                "inactive or wrong product Hyperliquid instrument".into(),
            ));
        }
        if item.delivery.emit_book_deltas {
            return Err(AdapterError::UnsupportedCapability(
                "Hyperliquid offers L2 snapshots, not exchange deltas".into(),
            ));
        }
        let sub = subscription(&item.channel, &inst.key.native_symbol, spot)?;
        if seen.insert(sub.to_string()) {
            out.push(sub);
        }
    }
    Ok(out)
}
// The server expands native L2 defaults in acknowledgement responses. Only
// these semantically equivalent defaults may be removed; an altered precision
// or fast-book subscription must never satisfy the requested ledger.
fn subscription_key(value: &Value) -> Result<String, AdapterError> {
    let object = value
        .as_object()
        .ok_or_else(|| AdapterError::Subscription("invalid subscription object".into()))?;
    let mut fields: std::collections::BTreeMap<_, _> =
        object.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    if fields.get("type").is_some_and(|v| v == "l2Book") {
        for key in ["nSigFigs", "mantissa"] {
            if fields.get(key).is_some_and(Value::is_null) {
                fields.remove(key);
            }
        }
        if fields.get("fast").is_some_and(|v| v == false) {
            fields.remove("fast");
        }
    }
    serde_json::to_string(&fields).map_err(|e| AdapterError::Subscription(e.to_string()))
}

/// Deterministic public session. Acknowledgements cover every requested native
/// subscription. Reconnects expose discontinuity; they do not prove gap repair.
pub struct HyperliquidSession {
    spot: bool,
    request: ConcreteSubscriptionSet,
    catalog: CatalogView,
    wire: Vec<Value>,
    pending: BTreeSet<String>,
    connected: bool,
    live: bool,
    frame_seq: u64,
    last_received: TimestampNs,
    connected_at: TimestampNs,
    books: BTreeMap<InstrumentId, BookSnapshot>,
    trade_deliveries: BTreeSet<String>,
    stream_progress: BTreeMap<String, TimestampNs>,
}
impl HyperliquidSession {
    pub fn new(
        spot: bool,
        request: ConcreteSubscriptionSet,
        catalog: CatalogView,
    ) -> Result<Self, AdapterError> {
        let wire = subscriptions(&request, &catalog, spot)?;
        Ok(Self {
            spot,
            request,
            catalog,
            wire,
            pending: BTreeSet::new(),
            connected: false,
            live: false,
            frame_seq: 0,
            last_received: TimestampNs(0),
            connected_at: TimestampNs(0),
            books: BTreeMap::new(),
            trade_deliveries: BTreeSet::new(),
            stream_progress: BTreeMap::new(),
        })
    }
    fn envelope(
        &self,
        coin: &str,
        ts: Option<i64>,
        received: FrameStamp,
        payload: MarketEvent,
    ) -> Result<EventEnvelope, AdapterError> {
        let inst = self.catalog.find_by_native(coin).ok_or_else(|| {
            AdapterError::Protocol(format!("unrequested Hyperliquid coin {coin}"))
        })?;
        if !self.request.items.iter().any(|s| s.instrument == inst.id) {
            return Err(AdapterError::Protocol(format!(
                "unrequested Hyperliquid instrument {coin}"
            )));
        }
        Ok(EventEnvelope {
            schema_version: 1,
            venue: self.catalog.venue,
            instrument: Some(inst.id),
            connection: ConnectionId(0),
            session: SessionId(0),
            frame_seq: self.frame_seq,
            event_index: 0,
            exchange_ts: ts.map(|v| TimestampNs(v * 1_000_000)),
            receive_ts: received.receive_ts,
            source_sequence: None,
            flags: EventFlags::empty(),
            payload,
        })
    }
    fn wants(&self, coin: &str, channel: &Channel) -> bool {
        self.catalog.find_by_native(coin).is_some_and(|i| {
            self.request
                .items
                .iter()
                .any(|s| s.instrument == i.id && &s.channel == channel)
        })
    }
    fn streams_healthy(&self, now: TimestampNs) -> Result<bool, AdapterError> {
        for sub in &self.wire {
            // Change-driven streams may have no initial delivery (observed for
            // an inactive PURR candle). Their ACK is operational readiness,
            // never evidence of execution/candle continuity or zero activity.
            if !matches!(sub["type"].as_str(), Some("l2Book" | "activeAssetCtx")) {
                continue;
            }
            let Some(last) = self.stream_progress.get(&subscription_key(sub)?) else {
                return Ok(false);
            };
            // Books and asset contexts have recurring updates. Trades and BBO
            // are change driven, candles may be daily: silence alone cannot
            // establish a gap or prove continuity on those streams.
            if matches!(sub["type"].as_str(), Some("l2Book" | "activeAssetCtx"))
                && now.0.saturating_sub(last.0) >= 60_000_000_000
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn events(
        &mut self,
        msg: Message,
        stamp: FrameStamp,
    ) -> Result<Vec<EventEnvelope>, AdapterError> {
        let mut out = Vec::new();
        match msg {
            Message::Trades(rows) => {
                let initial_coins: BTreeSet<_> = rows
                    .iter()
                    .map(|t| t.coin.clone())
                    .filter(|coin| !self.trade_deliveries.contains(coin))
                    .collect();
                for t in rows {
                    if !self.wants(&t.coin, &Channel::Trades) {
                        return Err(AdapterError::Protocol("unrequested trades".into()));
                    }
                    let id = t.unique_id();
                    let mut event = self.envelope(
                        &t.coin,
                        Some(t.time),
                        stamp,
                        MarketEvent::Trade(Trade {
                            price: Price(t.px.0),
                            quantity: Quantity(t.sz.0),
                            aggressor: if t.side == "B" {
                                AggressorSide::Buy
                            } else {
                                AggressorSide::Sell
                            },
                            trade_id: Some(SourceId(id)),
                        }),
                    )?;
                    event
                        .flags
                        .insert(EventFlags::ACQUISITION_ORIGIN_UNVERIFIED);
                    if initial_coins.contains(&t.coin) {
                        event.flags.insert(EventFlags::INITIAL_DELIVERY);
                    }
                    out.push(event);
                    self.trade_deliveries.insert(t.coin);
                }
            }
            Message::Bbo(b) => {
                if !self.wants(&b.coin, &Channel::Quote) {
                    return Err(AdapterError::Protocol("unrequested bbo".into()));
                }
                if let [Some(bid), Some(ask)] = b.bbo {
                    out.push(self.envelope(
                        &b.coin,
                        Some(b.time),
                        stamp,
                        MarketEvent::Quote(Quote {
                            bid_price: Price(bid.px.0),
                            bid_quantity: Some(Quantity(bid.sz.0)),
                            ask_price: Price(ask.px.0),
                            ask_quantity: Some(Quantity(ask.sz.0)),
                        }),
                    )?);
                } // One-sided native BBO is retained raw; no fictitious zero quote.
            }
            Message::Book(b) => {
                let inst = self
                    .catalog
                    .find_by_native(&b.coin)
                    .ok_or_else(|| AdapterError::Protocol("unknown book coin".into()))?;
                let mut depths = self.request.items.iter().filter_map(|s| {
                    if s.instrument == inst.id {
                        if let Channel::L2Book { depth, .. } = s.channel {
                            Some(depth.unwrap_or(20))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                });
                let depth = depths
                    .next()
                    .ok_or_else(|| AdapterError::Protocol("unrequested book".into()))?;
                let depth = depths.fold(depth, u32::max);
                let levels = |side: usize| {
                    b.levels[side]
                        .iter()
                        .take(depth as usize)
                        .map(|l| BookLevel {
                            price: Price(l.px.0),
                            quantity: Quantity(l.sz.0),
                        })
                        .collect()
                };
                out.push(self.envelope(
                    &b.coin,
                    Some(b.time),
                    stamp,
                    MarketEvent::BookSnapshot(BookSnapshot {
                        bids: levels(0),
                        asks: levels(1),
                        depth: Some(depth),
                        checksum: None,
                    }),
                )?);
            }
            Message::Candle(c) => {
                let requested = self.request.items.iter().any(|s| {
                    self.catalog
                        .find_by_native(&c.s)
                        .is_some_and(|i| i.id == s.instrument)
                        && subscription(&s.channel, &c.s, self.spot)
                            .is_ok_and(|v| v["type"] == "candle" && v["interval"] == c.i)
                });
                if !requested {
                    return Err(AdapterError::Protocol("unrequested candle".into()));
                }
                out.push(self.envelope(
                    &c.s,
                    None,
                    stamp,
                    MarketEvent::Candle(Candle {
                        open: Price(c.o.0),
                        high: Price(c.h.0),
                        low: Price(c.l.0),
                        close: Price(c.c.0),
                        volume: Quantity(c.v.0),
                        interval_ns: protocol::interval_ns(&c.i)?,
                        start_ts: TimestampNs(c.t * 1_000_000),
                    }),
                )?);
            }
            Message::Context(c) => {
                for (channel, present) in [
                    (Channel::Funding, c.ctx.funding.is_some()),
                    (Channel::OpenInterest, c.ctx.open_interest.is_some()),
                    (Channel::IndexPrice, c.ctx.oracle_px.is_some()),
                ] {
                    if self.wants(&c.coin, &channel) && !present {
                        return Err(AdapterError::Parse(format!(
                            "required Hyperliquid context field absent: {channel:?}"
                        )));
                    }
                }
                let mut add =
                    |channel: Channel, payload: MarketEvent| -> Result<(), AdapterError> {
                        if self.wants(&c.coin, &channel) {
                            out.push(self.envelope(&c.coin, None, stamp, payload)?);
                        }
                        Ok(())
                    };
                add(
                    Channel::MarkPrice,
                    MarketEvent::MarkPrice(PricePoint {
                        price: Price(c.ctx.mark_px.0),
                    }),
                )?;
                if let Some(p) = c.ctx.oracle_px {
                    add(
                        Channel::IndexPrice,
                        MarketEvent::IndexPrice(PricePoint { price: Price(p.0) }),
                    )?;
                }
                if let Some(p) = c.ctx.funding {
                    add(
                        Channel::Funding,
                        MarketEvent::Funding(Funding {
                            rate: Rate(p.0),
                            next_funding_ts: None,
                        }),
                    )?;
                }
                if let Some(p) = c.ctx.open_interest {
                    add(
                        Channel::OpenInterest,
                        MarketEvent::OpenInterest(OpenInterest {
                            quantity: Quantity(p.0),
                        }),
                    )?;
                }
                add(
                    Channel::Statistics24h,
                    MarketEvent::Statistics24h(Statistics24h {
                        open: (c.ctx.prev_day_px.0.coefficient > 0)
                            .then_some(Price(c.ctx.prev_day_px.0)),
                        // A mark price is not an observed last execution.
                        close: None,
                        high: None,
                        low: None,
                        volume: c.ctx.day_base_vlm.map(|v| Quantity(v.0)),
                        quote_volume: Some(Quantity(c.ctx.day_ntl_vlm.0)),
                    }),
                )?;
                if out.is_empty() {
                    return Err(AdapterError::Protocol(
                        "unrequested or incomplete asset context".into(),
                    ));
                }
            }
            _ => {
                return Err(AdapterError::Protocol(
                    "unexpected public data channel".into(),
                ));
            }
        }
        for (index, e) in out.iter_mut().enumerate() {
            e.event_index = u16::try_from(index)
                .map_err(|_| AdapterError::Protocol("event batch index overflow".into()))?;
        }
        Ok(out)
    }
}
impl SessionMachine for HyperliquidSession {
    fn book_snapshot(&self, instrument: InstrumentId, depth: Option<u32>) -> Option<BookSnapshot> {
        self.books.get(&instrument).map(|b| {
            let mut b = b.clone();
            if let Some(depth) = depth {
                b.bids.truncate(depth as usize);
                b.asks.truncate(depth as usize);
                b.depth = Some(b.depth.map_or(depth, |d| d.min(depth)));
            }
            b
        })
    }

    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Connected { now } => {
                for instrument in self.books.keys().copied() {
                    output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                        instrument,
                        reason: "Hyperliquid connection restarted".into(),
                    }));
                }
                self.books.clear();
                self.trade_deliveries.clear();
                self.stream_progress.clear();
                self.connected = true;
                self.live = false;
                self.connected_at = now;
                self.last_received = now;
                self.pending = self
                    .wire
                    .iter()
                    .map(subscription_key)
                    .collect::<Result<_, _>>()?;
                output.push(SessionAction::MarkDegraded);
                for sub in &self.wire {
                    output.push(SessionAction::SendText(Bytes::from(
                        json!({"method":"subscribe", "subscription":sub}).to_string(),
                    )));
                }
                output.push(SessionAction::ScheduleTimer(TimerSpec {
                    timer_id: 1,
                    fire_at: TimestampNs(now.0.saturating_add(20_000_000_000)),
                }));
            }
            SessionInput::TextFrame { bytes, received } => {
                if !self.connected {
                    return Err(AdapterError::Protocol("data before connected".into()));
                }
                self.frame_seq = self
                    .frame_seq
                    .checked_add(1)
                    .ok_or_else(|| AdapterError::Protocol("frame sequence exhausted".into()))?;
                let msg = protocol::decode(bytes)?;
                self.last_received = received.receive_ts;
                match msg {
                    Message::Subscription(v) => {
                        if v["method"] != "subscribe" {
                            return Err(AdapterError::Subscription(
                                "unexpected acknowledgement method".into(),
                            ));
                        }
                        let key = subscription_key(&v["subscription"])?;
                        if !self
                            .wire
                            .iter()
                            .any(|s| subscription_key(s).is_ok_and(|v| v == key))
                        {
                            return Err(AdapterError::Subscription(
                                "unexpected subscription acknowledgement".into(),
                            ));
                        }
                        self.pending.remove(&key);
                        if self.pending.is_empty() {
                            output.push(SessionAction::EmitSystem(
                                SystemEvent::SubscriptionStateChanged {
                                    state: "all Hyperliquid subscriptions acknowledged".into(),
                                },
                            ));
                        }
                    }
                    Message::Pong => {}
                    data => {
                        let progress = match &data {
                            Message::Trades(rows) => {
                                rows.first().map(|t| ("trades", t.coin.as_str()))
                            }
                            Message::Bbo(b) => Some(("bbo", b.coin.as_str())),
                            Message::Book(b) => Some(("l2Book", b.coin.as_str())),
                            Message::Context(c) => Some(("activeAssetCtx", c.coin.as_str())),
                            Message::Candle(c) => {
                                self.stream_progress.insert(
                                    subscription_key(
                                        &json!({"type":"candle","coin":c.s,"interval":c.i}),
                                    )?,
                                    received.receive_ts,
                                );
                                None
                            }
                            _ => None,
                        };
                        if let Some((kind, coin)) = progress {
                            self.stream_progress.insert(
                                subscription_key(&json!({"type":kind,"coin":coin}))?,
                                received.receive_ts,
                            );
                        }
                        if let Message::Bbo(b) = &data {
                            if b.bbo.iter().any(Option::is_none) {
                                let instrument = self
                                    .catalog
                                    .find_by_native(&b.coin)
                                    .ok_or_else(|| {
                                        AdapterError::Protocol("unknown bbo coin".into())
                                    })?
                                    .id;
                                output.push(SessionAction::EmitSystem(
                                    SystemEvent::QuoteInvalidated {
                                        venue: Some(self.catalog.venue),
                                        session: None,
                                        connection: None,
                                        instrument,
                                        reason: "Hyperliquid BBO has an unavailable side".into(),
                                    },
                                ));
                            }
                        }
                        let events = self.events(data, received)?;
                        for e in &events {
                            if let (Some(instrument), MarketEvent::BookSnapshot(book)) =
                                (e.instrument, &e.payload)
                            {
                                if self.books.insert(instrument, book.clone()).is_none() {
                                    output.push(SessionAction::EmitSystem(
                                        SystemEvent::BookResynchronized { instrument },
                                    ));
                                }
                            }
                        }
                        if !events.is_empty() {
                            output.push(SessionAction::EmitBatch(EventBatch {
                                session: SessionId(0),
                                frame_seq: self.frame_seq,
                                events,
                            }));
                        }
                    }
                }
                if self.pending.is_empty()
                    && !self.live
                    && self.streams_healthy(received.receive_ts)?
                {
                    self.live = true;
                    output.push(SessionAction::MarkLive);
                    output.push(SessionAction::EmitSystem(SystemEvent::SubscriptionStateChanged {
                        state: "required periodic streams observed; change-driven trade/BBO/candle silence is unverified".into(),
                    }));
                }
            }
            SessionInput::Timer { timer_id: 1, now } if self.connected => {
                if now.0.saturating_sub(self.last_received.0) >= 60_000_000_000
                    || ((!self.pending.is_empty() || !self.streams_healthy(now)?)
                        && now.0.saturating_sub(self.connected_at.0) >= 20_000_000_000)
                {
                    self.live = false;
                    output.push(SessionAction::MarkDegraded);
                    output.push(SessionAction::EmitSystem(SystemEvent::HeartbeatMissed));
                    output.push(SessionAction::Reconnect(ReconnectReason::Heartbeat));
                } else {
                    output.push(SessionAction::SendText(Bytes::from_static(
                        br#"{"method":"ping"}"#,
                    )));
                    output.push(SessionAction::ScheduleTimer(TimerSpec {
                        timer_id: 1,
                        fire_at: TimestampNs(now.0.saturating_add(20_000_000_000)),
                    }));
                }
            }
            SessionInput::Disconnected { .. } => {
                for instrument in self.books.keys().copied() {
                    output.push(SessionAction::EmitSystem(SystemEvent::BookInvalidated {
                        instrument,
                        reason: "Hyperliquid disconnected".into(),
                    }));
                }
                self.books.clear();
                self.connected = false;
                self.live = false;
                output.push(SessionAction::CancelTimer(1));
                output.push(SessionAction::MarkDegraded);
                output.push(SessionAction::EmitSystem(
                    SystemEvent::ConnectionStateChanged {
                        state: "disconnected; public trade continuity unverified".into(),
                    },
                ));
            }
            SessionInput::Control {
                command: SessionCommand::Stop,
            } => output.push(SessionAction::StopSession(StopReason::Control)),
            SessionInput::BinaryFrame { .. } => {
                return Err(AdapterError::Protocol(
                    "unexpected Hyperliquid binary payload".into(),
                ));
            }
            SessionInput::HttpResponse { .. } => {
                return Err(AdapterError::Protocol(
                    "unexpected Hyperliquid session HTTP response".into(),
                ));
            }
            _ => {}
        }
        Ok(())
    }
}
