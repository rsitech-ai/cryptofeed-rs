//! Exact owned operation DTOs for the negotiated causal recording profile.
//!
//! This codec does not create a factory, verify a bootstrap, execute replay or
//! release outputs. Its identity must come from a separately verified bootstrap.
//! Financial records stay in typed JSON leaves; catalog decimals use canonical
//! coefficient strings and scales. No generic JSON value carries money.
//! The encoding cap limits the actual serialized output, not process RSS or DTO
//! construction: the bounded native capture and conversion copies are separate.
use crate::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use marketfeed_adapter_api::*;
use marketfeed_dispatch::{DispatchError, PushOutcome};
use marketfeed_model::*;
use marketfeed_recording::{RecordingError, shared_feed::*};
use marketfeed_sinks::SinkError;
use marketfeed_transport::{FrameOpcode, OutboundFrame, TransportError};
use serde::{Deserialize, Serialize};
use std::io::{self, Write};

#[path = "causal_catalog.rs"]
mod catalog;
pub use catalog::*;
#[path = "causal_bootstrap.rs"]
mod bootstrap;
#[path = "causal_convert.rs"]
mod convert;
#[path = "causal_owner_history.rs"]
mod owner_history;
pub use bootstrap::*;
pub use owner_history::*;
#[cfg(test)]
#[path = "causal_tests.rs"]
mod tests;

pub const MAX_OPERATION_BYTES: usize = MAX_CAUSAL_ENCODED_PAYLOAD / 4 * 3;

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("invalid causal operation: {0}")]
    Invalid(String),
    #[error("operation encoding byte budget")]
    TooLarge,
    #[error(transparent)]
    Recording(#[from] RecordingError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
fn invalid(message: &str) -> CodecError {
    CodecError::Invalid(message.into())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationIdentity {
    pub venue: u16,
    pub session: u64,
    pub bootstrap_id: String,
}

/// Decode is lossless into these owned types, including owned error labels.
/// Native errors with static string fields are never reconstructed by leaking
/// or interning untrusted strings. Actual-engine replay remains a separate API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationDto {
    pub schema_version: u16,
    pub route: CausalRoute,
    pub input: InputDto,
    pub drain_policy: DrainDto,
    pub before: StateDto,
    #[serde(deserialize_with = "bounded_entries")]
    pub entries: Vec<TraceDto>,
    pub outcome: Result<(), EngineErrorDto>,
    pub after: StateDto,
    pub observed_entries: u64,
    pub retained_bytes_bound: u64,
}

/// Private representation prevents an unchecked leaf from being constructed.
/// Decode checks the typed FeedRecord before it reaches any comparison/replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RecordJson(String);
impl RecordJson {
    pub fn record(&self) -> Result<FeedRecord, CodecError> {
        Ok(serde_json::from_str(&self.0)?)
    }
    fn new(record: &FeedRecord) -> Result<Self, CodecError> {
        Ok(Self(serde_json::to_string(record)?))
    }
    fn original(bytes: &[u8]) -> Result<Self, CodecError> {
        let value = Self(
            String::from_utf8(bytes.to_vec()).map_err(|_| invalid("record JSON is not UTF-8"))?,
        );
        value.validate()?;
        Ok(value)
    }
    fn validate(&self) -> Result<(), CodecError> {
        let record = self.record()?;
        if matches!(record, FeedRecord::Causal(_)) {
            return Err(invalid("recursive causal record"));
        }
        let mut check = Canonical {
            expected: self.0.as_bytes(),
            offset: 0,
        };
        serde_json::to_writer(&mut check, &record)?;
        if check.offset != self.0.len() {
            return Err(invalid("noncanonical financial record leaf"));
        }
        Ok(())
    }
}
impl<'de> Deserialize<'de> for RecordJson {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = Self(String::deserialize(d)?);
        value.validate().map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

macro_rules! leaf {
    ($name:ident, $variant:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
        pub struct $name(RecordJson);
        impl $name {
            pub fn record(&self) -> Result<FeedRecord, CodecError> {
                self.0.record()
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let json = RecordJson::deserialize(d)?;
                if !matches!(
                    json.record().map_err(serde::de::Error::custom)?,
                    FeedRecord::$variant(..)
                ) {
                    return Err(serde::de::Error::custom("wrong financial record leaf kind"));
                }
                Ok(Self(json))
            }
        }
    };
}
leaf!(BatchJson, Batch);
// System is a struct variant and is validated separately.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SystemJson(RecordJson);
impl SystemJson {
    pub fn record(&self) -> Result<FeedRecord, CodecError> {
        self.0.record()
    }
}
impl<'de> Deserialize<'de> for SystemJson {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let json = RecordJson::deserialize(d)?;
        if !matches!(
            json.record().map_err(serde::de::Error::custom)?,
            FeedRecord::System { .. }
        ) {
            return Err(serde::de::Error::custom("wrong system record leaf kind"));
        }
        Ok(Self(json))
    }
}

/// Canonical standard base64, bounded by the enclosing operation's byte limit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BytesDto(String);
impl BytesDto {
    pub fn bytes(&self) -> Result<Vec<u8>, CodecError> {
        STANDARD
            .decode(&self.0)
            .map_err(|_| invalid("invalid base64"))
    }
    fn new(bytes: &[u8]) -> Self {
        Self(STANDARD.encode(bytes))
    }
}
impl<'de> Deserialize<'de> for BytesDto {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let value = Self(String::deserialize(d)?);
        value.bytes().map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

macro_rules! unit_dto {
    ($name:ident, $native:ty, $($variant:ident),+ $(,)?) => {
        #[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
        #[serde(rename_all="snake_case")] pub enum $name { $($variant),+ }
        impl From<$native> for $name {
            fn from(value:$native)->Self { match value { $(<$native>::$variant=>Self::$variant),+ } }
        }
    }
}
unit_dto!(
    DisconnectDto,
    DisconnectReason,
    RemoteClose,
    TransportError,
    HeartbeatTimeout,
    LocalStop,
    ReconnectRequested
);
unit_dto!(
    ReconnectDto,
    ReconnectReason,
    Protocol,
    SequenceGap,
    ChecksumMismatch,
    Heartbeat,
    Control
);
unit_dto!(StopDto, StopReason, Control, FatalProtocol, Unsupported);
unit_dto!(MethodDto, HttpMethod, Get, Post, Put, Delete);
unit_dto!(OpcodeDto, FrameOpcode, Text, Binary, Ping, Pong, Close);
unit_dto!(
    OverflowDto,
    OverflowPolicy,
    BlockWithDeadline,
    DropNewest,
    DropOldest,
    LatestPerKey,
    SpillToDisk,
    DisableSink,
    FailEngine
);
unit_dto!(
    LifecycleDto,
    SessionLifecycle,
    Planned,
    Connecting,
    Connected,
    Subscribing,
    Synchronizing,
    Live,
    Degraded,
    Backoff,
    Draining,
    Stopped
);
unit_dto!(LaneDto, CaptureQueueLane, Market, System, Write, Http);
unit_dto!(
    DrainDto,
    CaptureDrainPolicy,
    Retain,
    NullDispatchTakeOutbound
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpRequestDto {
    pub id: u64,
    pub method: MethodDto,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<BytesDto>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpResponseDto {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: BytesDto,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteDto {
    pub opcode: OpcodeDto,
    pub payload: BytesDto,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum CommandDto {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
    Replace(Vec<String>),
    Resync(u32),
    Stop,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum InputDto {
    Connected {
        connection: u64,
        now: i64,
    },
    Disconnected {
        reason: DisconnectDto,
        now: i64,
    },
    TransportLost {
        reason: DisconnectDto,
        now: i64,
    },
    Text {
        payload: BytesDto,
        received: FrameStamp,
    },
    Binary {
        payload: BytesDto,
        received: FrameStamp,
    },
    Ping {
        payload: BytesDto,
        received: FrameStamp,
    },
    Pong {
        payload: BytesDto,
        received: FrameStamp,
    },
    Close {
        payload: BytesDto,
        received: FrameStamp,
    },
    Http {
        request_id: u64,
        response: HttpResponseDto,
        received: FrameStamp,
    },
    ForwardDispatch {
        results: Vec<Result<PushDto, SinkErrorDto>>,
    },
    NullDispatch,
    TakeWrites,
    TakeHttp,
    TimerPoll {
        now: i64,
    },
    Control {
        command: CommandDto,
        now: i64,
    },
    System(SystemJson),
    Degraded {
        message: String,
        now: i64,
    },
    Live {
        now: i64,
    },
    Catalog {
        catalog: CatalogDto,
        now: i64,
    },
    Stop,
    Lifecycle(LifecycleDto),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum InvocationDto {
    Connected,
    Disconnected(DisconnectDto),
    Text(FrameStamp),
    Binary(FrameStamp),
    Pong(FrameStamp),
    Http {
        request_id: u64,
        received: FrameStamp,
    },
    Timer {
        timer_id: u64,
        now: i64,
    },
    Control(CommandDto),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ActionDto {
    SendText(BytesDto),
    SendBinary(BytesDto),
    SendPing(BytesDto),
    RequestHttp(HttpRequestDto),
    ScheduleTimer { timer_id: u64, fire_at: i64 },
    CancelTimer(u64),
    EmitBatch(BatchJson),
    EmitSystem(SystemJson),
    MarkLive,
    MarkDegraded,
    ResyncInstrument(u32),
    Reconnect(ReconnectDto),
    DisableSubscription(u64),
    StopSession(StopDto),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum DispositionDto {
    Buffered,
    DiscardedAfterCallbackError,
    Started,
    Completed(Result<(), EngineErrorDto>),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BufferedDto {
    pub action: ActionDto,
    pub disposition: DispositionDto,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ItemDto {
    Market(BatchJson),
    System(SystemJson),
    Write(WriteDto),
    Http(HttpRequestDto),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum OriginDto {
    Runner,
    Invocation { ordinal: u64 },
    Action { invocation: u64, index: u64 },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraceDto {
    pub origin: OriginDto,
    pub effect: EffectDto,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum WireActionDto {
    Text(BytesDto),
    Binary(BytesDto),
    Ping(BytesDto),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum EffectDto {
    ProducedRecord(RecordJson),
    Invocation {
        kind: InvocationDto,
        retained_actions: u64,
        dropped_before: u64,
        dropped_after: u64,
        actions: Vec<BufferedDto>,
        outcome: Result<(), AdapterErrorDto>,
    },
    QueueAttempt {
        lane: LaneDto,
        incoming: ItemDto,
        evicted_front: Option<ItemDto>,
        result: Option<Result<PushDto, EngineErrorDto>>,
    },
    QueueDrain {
        lane: LaneDto,
        items: Vec<ItemDto>,
        completed: bool,
    },
    SinkAttempt {
        lane: LaneDto,
        incoming: ItemDto,
        result: Option<Result<PushDto, SinkErrorDto>>,
    },
    ActionBufferDropsConsumed(u64),
    SuppressedDiagnostic(EngineErrorDto),
    SubscriptionPrepared(Result<WireActionDto, AdapterErrorDto>),
    SubscriptionReservation(Result<(), DispatchErrorDto>),
    SubscriptionCommitted,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateDto {
    pub lifecycle: LifecycleDto,
    pub connection: u64,
    pub frame_seq: u64,
    pub stop_requested: bool,
    pub reconnect_requested: bool,
    pub timers: Vec<(u64, i64)>,
    pub pending_market: u64,
    pub pending_system: u64,
    pub pending_writes: u64,
    pub pending_http: u64,
    pub valid_books: u64,
    pub queues: Vec<(LaneDto, Vec<ItemDto>)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum PushDto {
    Accepted,
    DroppedNewest,
    DroppedOldest { dropped: u64 },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum AdapterErrorDto {
    UnsupportedCapability(String),
    Protocol(String),
    Parse(String),
    Subscription(String),
    Catalog(String),
    BookInvariant(String),
    Internal(String),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum DispatchErrorDto {
    FailEngine,
    DeadlineExceeded,
    UnsupportedPolicy(OverflowDto),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum SinkErrorDto {
    FailEngine,
    DeadlineExceeded,
    UnsupportedPolicy(OverflowDto),
    Io(String),
    Unsupported(String),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum TransportErrorDto {
    NotConnected,
    Closed,
    FrameTooLarge(u64),
    Timeout,
    Tls(String),
    Protocol(String),
    Io(String),
    Stub(String),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum RecordingErrorDto {
    Io(String),
    PublicationIndeterminate(String),
    BadMagic,
    UnsupportedVersion(u16),
    Truncated,
    CrcMismatch,
    InvalidHeader,
    MetadataConflict { key: String },
    InvalidControlCommand(String),
    RecordTooLarge { record_len: u32, max: u32 },
    QueueFull,
    UnsupportedOverflow(String),
    ShutdownTimeout { remaining: u64 },
    NormalizedBoundExceeded { kind: String, limit: u64 },
    DiskFull,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum EngineErrorDto {
    Adapter(AdapterErrorDto),
    Dispatch(DispatchErrorDto),
    Recording(RecordingErrorDto),
    Transport(TransportErrorDto),
    Stopped,
    SessionNotFound,
    Internal(String),
}

/// Encoding failures after engine execution are terminal for its future owner;
/// they do not imply rollback or permission to execute the input again.
pub fn encode_operation(
    capture: &CapturedOperation,
    identity: &OperationIdentity,
    max_bytes: usize,
) -> Result<CausalEnvelope, CodecError> {
    if max_bytes == 0 || max_bytes > MAX_OPERATION_BYTES {
        return Err(CodecError::TooLarge);
    }
    let dto = OperationDto::from_capture(capture, identity)?;
    let mut buffer = CappedBuffer {
        bytes: Vec::new(),
        max: max_bytes,
        overflow: false,
    };
    if let Err(error) = serde_json::to_writer(&mut buffer, &dto) {
        return Err(if buffer.overflow {
            CodecError::TooLarge
        } else {
            CodecError::Json(error)
        });
    }
    Ok(CausalEnvelope::new(
        CausalPhase::Operation,
        dto.route,
        &buffer.bytes,
    )?)
}
pub fn decode_operation(
    envelope: &CausalEnvelope,
    identity: &OperationIdentity,
) -> Result<OperationDto, CodecError> {
    if envelope.schema_version != 1 || envelope.phase != CausalPhase::Operation {
        return Err(invalid("expected operation phase"));
    }
    if envelope.route.venue != identity.venue
        || envelope.route.session != identity.session
        || envelope.route.bootstrap_id != identity.bootstrap_id
    {
        return Err(invalid("operation identity mismatch"));
    }
    let payload = envelope.payload()?;
    let dto: OperationDto = serde_json::from_slice(&payload)?;
    if dto.schema_version != 1
        || dto.route != envelope.route
        || dto.route.venue != identity.venue
        || dto.route.session != identity.session
        || dto.route.bootstrap_id != identity.bootstrap_id
    {
        return Err(invalid("operation schema or identity mismatch"));
    }
    dto.validate()?;
    let mut check = Canonical {
        expected: &payload,
        offset: 0,
    };
    serde_json::to_writer(&mut check, &dto)?;
    if check.offset != payload.len() {
        return Err(invalid("noncanonical operation"));
    }
    Ok(dto)
}
impl OperationDto {
    pub fn matches_capture(
        &self,
        capture: &CapturedOperation,
        identity: &OperationIdentity,
    ) -> Result<bool, CodecError> {
        Ok(self == &Self::from_capture(capture, identity)?)
    }
}
struct CappedBuffer {
    bytes: Vec<u8>,
    max: usize,
    overflow: bool,
}
impl Write for CappedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|n| n > self.max)
        {
            self.overflow = true;
            return Err(io::Error::other("operation byte budget"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
/// Comparing the canonical stream needs no second payload-sized allocation.
struct Canonical<'a> {
    expected: &'a [u8],
    offset: usize,
}
impl Write for Canonical<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = self
            .offset
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("canonical offset overflow"))?;
        if self.expected.get(self.offset..end) != Some(bytes) {
            return Err(io::Error::other("noncanonical operation/record JSON"));
        }
        self.offset = end;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn bounded_entries<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<TraceDto>, D::Error> {
    struct Entries;
    impl<'de> serde::de::Visitor<'de> for Entries {
        type Value = Vec<TraceDto>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("at most 65536 trace entries")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> Result<Self::Value, A::Error> {
            let mut entries = Vec::new();
            while entries.len() < 65536 {
                let Some(value) = seq.next_element()? else {
                    return Ok(entries);
                };
                entries.push(value);
            }
            if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom("operation entry budget"));
            }
            Ok(entries)
        }
    }
    d.deserialize_seq(Entries)
}
