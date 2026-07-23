//! Public facade for embedded marketfeed consumers (spec §19 / §7.1 R28).
//!
//! Depend on this crate instead of wiring every internal workspace member.
//! Adapters, transport, recording, and dispatch stay out of the default surface;
//! pull those crates explicitly when you need them.
//!
//! # What this re-exports
//!
//! - **Model:** [`Fixed`], market/system events, ids, overflow policy
//! - **Adapter contracts:** [`SessionMachine`], [`VenueFactory`], subscriptions
//! - **Engine control:** [`EngineControl`], [`EngineSupervisor`], health / rotate
//! - **Sinks:** [`sinks::EventSink`] and common sink types under [`sinks`]
//!
//! # Example
//!
//! ```
//! use marketfeed::{EngineControl, EngineSupervisor, Fixed, MarketEvent};
//!
//! let mut eng = EngineSupervisor::new();
//! eng.mark_running();
//! let _ = eng.health();
//! let px = Fixed::new(100_00, 2);
//! let _ = MarketEvent::Trade(marketfeed::Trade {
//!     price: marketfeed::Price(px),
//!     quantity: marketfeed::Quantity(Fixed::new(1, 0)),
//!     aggressor: marketfeed::AggressorSide::Buy,
//!     trade_id: None,
//! });
//! ```

#![forbid(unsafe_code)]

// --- model (events, Fixed, ids) ------------------------------------------------

pub use marketfeed_model::{
    AggressorSide, AssetCode, BookChange, BookDelta, BookLevel, BookOperation, BookSide,
    BookSnapshot, Candle, CatalogVersion, CatalogView, ConnectionId, EventEnvelope, EventFlags,
    Fixed, FixedError, FrameStamp, Funding, Instrument, InstrumentDefinition, InstrumentId,
    InstrumentKey, InstrumentKind, InstrumentStatus, InstrumentUpdate, Liquidation, MarketEvent,
    OpenInterest, OverflowPolicy, PlanVersion, Price, PricePoint, Quantity, Quote, Rate,
    RoundingMode, SequenceRange, SessionId, SourceId, Statistics24h, SubscriptionId, SystemEvent,
    TimestampNs, Trade, VenueCode, VenueId, VenueStatus,
};

// --- adapter API (key traits + subscription surface) ---------------------------

pub use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, CandleInterval, Channel, ConcreteSubscription,
    ConcreteSubscriptionSet, DeliveryOptions, EventBatch, InstrumentSelector, SessionCommand,
    SessionMachine, SessionSpec, Subscription, SubscriptionPatch, VenueFactory, VenueSpecification,
};

// --- engine control (§19.2) ----------------------------------------------------

pub use marketfeed_engine::{
    ControlError, EngineControl, EngineError, EngineLifecycle, EngineSupervisor, HealthSnapshot,
    RecordingRotateHandle, RollingReplace, SessionHealth, SessionLifecycle, SessionRunnerConfig,
};

/// Bounded external sinks (spec §17.4). Prefer these over depending on
/// `marketfeed-sinks` directly for common embed paths.
pub mod sinks {
    pub use marketfeed_sinks::{
        EventSink, FileSink, LoggingSink, MemorySink, SinkError, UdpSink, forward_dispatcher,
    };
}
