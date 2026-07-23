//! Session state-machine inputs and actions.

use bytes::Bytes;
use marketfeed_model::{
    BookSnapshot, EventEnvelope, FrameStamp, InstrumentId, SessionId, SubscriptionId, SystemEvent,
    TimestampNs,
};

use crate::AdapterError;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Deterministic venue session protocol machine.
pub trait SessionMachine: Send + 'static {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError>;

    /// Initialize deterministic replay state without granting a live transport
    /// connection.
    ///
    /// The default preserves historical behavior. Adapters whose current live
    /// capability differs from legacy recordings can override this boundary
    /// while keeping `SessionInput::Connected` fail closed.
    fn on_replay_start(
        &mut self,
        now: TimestampNs,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        self.on_input(SessionInput::Connected { now }, output)
    }

    /// Prepare exactly one wire frame for an atomic subscription mutation.
    ///
    /// This phase must be side-effect free. The engine records and reserves the
    /// returned frame before calling [`Self::commit_dynamic_subscription`], so
    /// bounded queues cannot partially apply or silently discard the mutation.
    fn prepare_dynamic_subscription(
        &self,
        command: &SessionCommand,
    ) -> Result<SubscriptionWireAction, AdapterError> {
        let _ = command;
        Err(AdapterError::UnsupportedCapability(
            "dynamic subscriptions".into(),
        ))
    }

    /// Commit adapter-local state after the prepared wire frame is durably
    /// accepted by the runner's pending-write queue.
    ///
    /// Implementations that override [`Self::prepare_dynamic_subscription`]
    /// should keep this phase infallible and must not emit additional actions.
    fn commit_dynamic_subscription(&mut self, command: &SessionCommand) {
        let _ = command;
    }

    /// Embedded control query (§19.2 `book_snapshot`). Default: unsupported.
    ///
    /// # ponytail
    /// Query stays on the machine (owning book) instead of a parallel book cache.
    /// Ceiling: only adapters that override this answer control queries. Upgrade:
    /// engine-side registry when multi-session book fan-out is needed.
    fn book_snapshot(&self, instrument: InstrumentId, depth: Option<u32>) -> Option<BookSnapshot> {
        let _ = (instrument, depth);
        None
    }
}

/// Engine-owned inputs delivered to adapters.
#[derive(Debug)]
pub enum SessionInput<'a> {
    Connected {
        now: TimestampNs,
    },
    Disconnected {
        reason: DisconnectReason,
        now: TimestampNs,
    },
    TextFrame {
        bytes: &'a mut [u8],
        received: FrameStamp,
    },
    BinaryFrame {
        bytes: &'a mut [u8],
        received: FrameStamp,
    },
    Pong {
        payload: &'a [u8],
        received: FrameStamp,
    },
    HttpResponse {
        request_id: u64,
        response: &'a HttpResponse,
        received: FrameStamp,
    },
    Timer {
        timer_id: u64,
        now: TimestampNs,
    },
    Control {
        command: &'a SessionCommand,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DisconnectReason {
    RemoteClose,
    TransportError,
    HeartbeatTimeout,
    LocalStop,
    ReconnectRequested,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionCommand {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
    /// Atomically replace the complete desired symbol set.
    Replace(Vec<String>),
    Resync(InstrumentId),
    Stop,
}

/// The single authoritative wire mutation for a prepared subscription command.
///
/// A one-frame type makes the runner's capacity check exact and prevents
/// partial enqueue of multi-frame subscription mutations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionWireAction {
    Text(Bytes),
    Binary(Bytes),
    Ping(Bytes),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequestSpec {
    pub id: u64,
    pub method: HttpMethod,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Bytes>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HttpMethod {
    Get,
    Post,
    /// listenKey keepalive / similar REST verbs (private streams Phase 1).
    Put,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerSpec {
    pub timer_id: u64,
    pub fire_at: TimestampNs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconnectReason {
    Protocol,
    SequenceGap,
    ChecksumMismatch,
    Heartbeat,
    Control,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReason {
    Control,
    FatalProtocol,
    Unsupported,
}

/// Secret-bearing wire payload that must be sent but never recorded or copied
/// into diagnostic action mirrors.
#[derive(Clone, PartialEq, Eq)]
pub struct SensitiveBytes(Bytes);

impl SensitiveBytes {
    pub fn new(payload: Bytes) -> Self {
        Self(payload)
    }

    /// Expose the payload only at the transport boundary.
    pub fn expose(&self) -> &Bytes {
        &self.0
    }

    pub fn into_inner(self) -> Bytes {
        self.0
    }
}

impl fmt::Debug for SensitiveBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SensitiveBytes")
            .field(&"<redacted>")
            .finish()
    }
}

/// Actions the engine executes on behalf of the adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionAction {
    SendText(Bytes),
    /// Text sent to the transport but excluded from raw recording and mirrors.
    SendSensitiveText(SensitiveBytes),
    SendBinary(Bytes),
    SendPing(Bytes),
    RequestHttp(HttpRequestSpec),
    ScheduleTimer(TimerSpec),
    CancelTimer(u64),
    EmitBatch(EventBatch),
    EmitSystem(SystemEvent),
    MarkLive,
    MarkDegraded,
    ResyncInstrument(InstrumentId),
    Reconnect(ReconnectReason),
    DisableSubscription(SubscriptionId),
    StopSession(StopReason),
}

/// One frame produces one batch where practical.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventBatch {
    pub session: SessionId,
    pub frame_seq: u64,
    /// ponytail: Vec until SmallVec dep justified; ceiling = tiny per-frame alloc; upgrade = smallvec.
    pub events: Vec<EventEnvelope>,
}

/// Default bound for adapter action sinks (DropNewest when full).
pub const DEFAULT_ACTION_BUFFER_CAPACITY: usize = 1024;

/// Bounded action sink reused across `on_input` calls.
#[derive(Debug)]
pub struct ActionBuffer {
    actions: Vec<SessionAction>,
    capacity: usize,
    dropped: u64,
}

impl Default for ActionBuffer {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_ACTION_BUFFER_CAPACITY)
    }
}

impl ActionBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            actions: Vec::with_capacity(capacity.min(1024)),
            capacity,
            dropped: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn push(&mut self, action: SessionAction) {
        if self.actions.len() >= self.capacity {
            self.dropped += 1;
            return;
        }
        self.actions.push(action);
    }

    pub fn extend<I: IntoIterator<Item = SessionAction>>(&mut self, iter: I) {
        for action in iter {
            self.push(action);
        }
    }

    pub fn as_slice(&self) -> &[SessionAction] {
        &self.actions
    }

    pub fn drain(&mut self) -> impl Iterator<Item = SessionAction> + '_ {
        self.actions.drain(..)
    }

    pub fn take_dropped(&mut self) -> u64 {
        std::mem::replace(&mut self.dropped, 0)
    }

    pub fn clear(&mut self) {
        self.actions.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    pub fn len(&self) -> usize {
        self.actions.len()
    }
}

#[cfg(test)]
mod action_buffer_tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn action_buffer_drop_newest_when_full() {
        let mut buf = ActionBuffer::with_capacity(2);
        let stop = SessionAction::StopSession(StopReason::Control);
        buf.push(SessionAction::SendText(Bytes::from_static(b"a")));
        buf.push(SessionAction::SendText(Bytes::from_static(b"b")));
        buf.push(stop.clone());
        assert_eq!(buf.len(), 2);
        assert_eq!(buf.take_dropped(), 1);
        assert!(!buf.as_slice().contains(&stop));
    }

    #[test]
    fn sensitive_bytes_debug_is_redacted() {
        let payload = SensitiveBytes::new(Bytes::from_static(b"sensitive-auth-payload"));

        let debug = format!("{payload:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("sensitive-auth-payload"));
    }
}
