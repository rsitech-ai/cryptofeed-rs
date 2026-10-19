//! Market and system event types.

use crate::{
    ConnectionId, EventFlags, InstrumentId, Price, Quantity, Rate, SequenceRange, SessionId,
    SourceId, TimestampNs, VenueId,
};
use serde::{Deserialize, Serialize};

/// Envelope shared by every normalized market event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub schema_version: u16,
    pub venue: VenueId,
    pub instrument: Option<InstrumentId>,
    pub connection: ConnectionId,
    pub session: SessionId,
    pub frame_seq: u64,
    pub event_index: u16,
    pub exchange_ts: Option<TimestampNs>,
    pub receive_ts: TimestampNs,
    pub source_sequence: Option<SequenceRange>,
    pub flags: EventFlags,
    pub payload: MarketEvent,
}

/// Semantically distinct market-data payloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketEvent {
    Trade(Trade),
    Quote(Quote),
    BookSnapshot(BookSnapshot),
    BookDelta(BookDelta),
    Candle(Candle),
    MarkPrice(PricePoint),
    IndexPrice(PricePoint),
    Funding(Funding),
    OpenInterest(OpenInterest),
    Liquidation(Liquidation),
    Statistics24h(Statistics24h),
    InstrumentUpdate(InstrumentUpdate),
    VenueStatus(VenueStatus),
}

/// Aggressor/taker side. If a venue only provides maker side, invert it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggressorSide {
    Buy,
    Sell,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trade {
    pub price: Price,
    pub quantity: Quantity,
    pub aggressor: AggressorSide,
    pub trade_id: Option<SourceId>,
    /// Native granularity/ranges/attributes. Absence means unverified; never
    /// infer one execution from one normalized row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<Box<TradeProvenance>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeGranularity {
    IndividualExecution,
    NativeAggregate,
    Unverified,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeProvenance {
    pub source_channel: String,
    pub granularity: TradeGranularity,
    pub constituent_first_id: Option<SourceId>,
    pub constituent_last_id: Option<SourceId>,
    /// Native count only. Its venue-specific meaning is in native_attributes;
    /// it is not an inferred count of executions or a range cardinality.
    pub native_count: Option<u64>,
    pub native_attributes: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quote {
    pub bid_price: Price,
    pub bid_quantity: Option<Quantity>,
    pub ask_price: Price,
    pub ask_quantity: Option<Quantity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookLevel {
    pub price: Price,
    pub quantity: Quantity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BookOperation {
    Upsert,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookChange {
    pub side: BookSide,
    pub operation: BookOperation,
    pub price: Price,
    pub quantity: Option<Quantity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookSnapshot {
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    pub depth: Option<u32>,
    pub checksum: Option<SourceId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookDelta {
    pub changes: Vec<BookChange>,
    pub checksum: Option<SourceId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Candle {
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub volume: Quantity,
    pub interval_ns: i64,
    pub start_ts: TimestampNs,
    /// None means that native confirmation was unavailable, not a closed bar.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_closed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote_volume: Option<Quantity>,
    /// Venue volCcy, whose unit depends on the instrument/product. Never
    /// reinterpret it as base quantity without the native metadata contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_volume_ccy: Option<Quantity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PricePoint {
    pub price: Price,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Funding {
    pub rate: Rate,
    pub next_funding_ts: Option<TimestampNs>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenInterest {
    pub quantity: Quantity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Liquidation {
    pub price: Price,
    pub quantity: Quantity,
    pub side: AggressorSide,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Statistics24h {
    pub open: Option<Price>,
    pub high: Option<Price>,
    pub low: Option<Price>,
    pub close: Option<Price>,
    pub volume: Option<Quantity>,
    pub quote_volume: Option<Quantity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstrumentUpdate {
    pub status: crate::InstrumentStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VenueStatus {
    pub message: String,
}

/// Operational events — separate stream from market data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemEvent {
    EngineStateChanged {
        state: String,
    },
    ConnectionStateChanged {
        state: String,
    },
    SubscriptionStateChanged {
        state: String,
    },
    InstrumentCatalogUpdated {
        version: u64,
    },
    HeartbeatMissed,
    RateLimited,
    ParseError {
        detail: String,
    },
    UnknownMessage {
        detail: String,
    },
    SequenceGap {
        expected: u64,
        actual: u64,
    },
    ChecksumMismatch {
        detail: String,
    },
    BookInvalidated {
        instrument: InstrumentId,
        reason: String,
    },
    QuoteInvalidated {
        #[serde(default)]
        venue: Option<VenueId>,
        #[serde(default)]
        session: Option<SessionId>,
        #[serde(default)]
        connection: Option<ConnectionId>,
        instrument: InstrumentId,
        reason: String,
    },
    BookSnapshotRejected {
        instrument: InstrumentId,
        reason: String,
    },
    BookResynchronized {
        instrument: InstrumentId,
    },
    QueuePressure {
        detail: String,
    },
    EventsDropped {
        count: u64,
        detail: String,
    },
    RecordingRotated,
    DiskPressure,
    ClockJump {
        delta_ns: i64,
    },
    SinkStateChanged {
        state: String,
    },
    ShutdownStarted,
    ShutdownCompleted,
}
