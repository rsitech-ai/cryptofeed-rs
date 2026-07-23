//! Adapter error taxonomy.

use thiserror::Error;

use marketfeed_model::InstrumentId;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AdapterError {
    #[error("unsupported capability: {0}")]
    UnsupportedCapability(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("subscription error: {0}")]
    Subscription(String),
    #[error("instrument catalog error: {0}")]
    Catalog(String),
    #[error("book invariant: {0}")]
    BookInvariant(String),
    #[error("internal: {0}")]
    Internal(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    Configuration,
    InstrumentCatalog,
    UnsupportedCapability,
    Dns,
    Transport,
    Tls,
    Http,
    RateLimit,
    Authentication,
    Subscription,
    Protocol,
    Parse,
    Decompression,
    SequenceGap,
    ChecksumMismatch,
    BookInvariant,
    Backpressure,
    Recording,
    Disk,
    Sink,
    Serialization,
    Clock,
    InternalInvariant,
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryAction {
    IgnoreMessage,
    DropBestEffortEvent,
    InvalidateInstrument,
    ResyncInstrument,
    ReconnectSession,
    DisableSubscription,
    OpenCircuitForVenue,
    DisableSink,
    MarkNotReady,
    StopEngine,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterFault {
    pub category: ErrorCategory,
    pub recovery: RecoveryAction,
    pub instrument: Option<InstrumentId>,
    pub detail: String,
}
