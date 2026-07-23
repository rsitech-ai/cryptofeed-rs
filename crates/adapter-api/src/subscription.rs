//! Subscription request types used by planners and factories.

use std::time::Duration;

use marketfeed_model::{InstrumentId, SessionId, VenueId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscription {
    pub venue: VenueId,
    pub selector: InstrumentSelector,
    pub channel: Channel,
    pub delivery: DeliveryOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstrumentSelector {
    Ids(Vec<InstrumentId>),
    NativeSymbols(Vec<String>),
    AllActive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Channel {
    Trades,
    Quote,
    L2Book {
        depth: Option<u32>,
        cadence: Option<Duration>,
    },
    L3Book,
    Candles {
        interval: CandleInterval,
    },
    MarkPrice,
    IndexPrice,
    Funding,
    OpenInterest,
    Liquidations,
    Statistics24h,
    InstrumentStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CandleInterval {
    M1,
    M5,
    M15,
    H1,
    D1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct DeliveryOptions {
    pub emit_book_snapshots: bool,
    pub emit_book_deltas: bool,
    pub emit_bbo: bool,
}

/// Concrete, catalog-expanded subscription set (planner input).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConcreteSubscriptionSet {
    pub items: Vec<ConcreteSubscription>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConcreteSubscription {
    pub instrument: InstrumentId,
    pub channel: Channel,
    pub delivery: DeliveryOptions,
}

/// Planned session produced by a venue factory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSpec {
    pub endpoint_name: String,
    pub subscriptions: ConcreteSubscriptionSet,
}

/// Control-plane subscription change (Spec §10.4).
///
/// Engine owns networking; adapters receive the mapped [`crate::SessionCommand`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionPatch {
    Add {
        session: SessionId,
        symbols: Vec<String>,
    },
    Remove {
        session: SessionId,
        symbols: Vec<String>,
    },
    Replace {
        session: SessionId,
        symbols: Vec<String>,
    },
    PauseVenue {
        venue: VenueId,
    },
    ResumeVenue {
        venue: VenueId,
    },
}
