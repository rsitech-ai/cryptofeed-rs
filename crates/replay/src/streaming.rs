//! Bounded, metadata-directed MFR adapter reconstruction.
//!
//! This is deliberately legacy semantic replay, not complete engine causal
//! replay. MFR1 has no physical generations, timer-poll boundaries, callback
//! outcomes or queue-drain decisions. Startup is an explicit caller assumption;
//! adapter identities are not rewritten into invented engine identities.

use std::{
    cell::Cell,
    collections::BTreeMap,
    io::{self, Read, Write},
};

use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, DEFAULT_ACTION_BUFFER_CAPACITY, SessionAction, SessionInput,
    SessionMachine,
};
use marketfeed_model::{FrameStamp, SessionId, TimestampNs};
use marketfeed_recording::{
    BuildMetadata, Direction, FrameOpcode, HEADER_SIZE, MetadataRecord, RawRecord,
    RawSegmentReader, RecordingError, SessionRecordingMetadata, decode_http_response,
    decode_metadata, decode_subscription_command,
};
use thiserror::Error;

#[derive(Debug, Clone, Copy)]
pub struct LegacyReplayLimits {
    pub max_sessions: usize,
    /// Sum of original encoded metadata bytes retained for unique bindings.
    pub max_metadata_bytes: usize,
    pub max_actions_per_step: usize,
    /// Combined market rows and system events emitted by one callback.
    pub max_events_per_step: usize,
    /// JSON bytes for batches/systems, payload bytes for wire/HTTP actions.
    pub max_output_bytes_per_step: usize,
    /// Processing/delivery quota. One additional complete CRC-checked record
    /// may be read (at most the recording reader's 16 MiB record bound), but
    /// is never passed to an adapter or sink. Input bytes are capped separately.
    pub max_records: u64,
    /// Includes segment headers. At most one extra byte is read to detect
    /// exhaustion at an otherwise ambiguous exact EOF/quota boundary.
    pub max_input_bytes: u64,
}

impl Default for LegacyReplayLimits {
    fn default() -> Self {
        Self {
            max_sessions: 256,
            max_metadata_bytes: 16 * 1024 * 1024,
            max_actions_per_step: DEFAULT_ACTION_BUFFER_CAPACITY,
            max_events_per_step: 65536,
            max_output_bytes_per_step: 16 * 1024 * 1024,
            max_records: 1_000_000,
            max_input_bytes: 1024 * 1024 * 1024,
        }
    }
}

/// The factory is called only when this session's metadata is encountered.
/// It must reconstruct locally or reject missing venue-specific inputs; the
/// runner performs no discovery or network operations.
pub struct LegacyReplaySession {
    pub machine: Box<dyn SessionMachine>,
    /// MFR does not record the connection time. Never infer it from metadata's
    /// segment-start timestamp or silently call it observed availability.
    pub assumed_start: TimestampNs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LegacyReplayPosition {
    pub segment: u64,
    /// One-based complete-record ordinal; zero denotes the segment header.
    pub record: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyReplayStepKind {
    Metadata,
    SessionStarted {
        assumed_start: TimestampNs,
    },
    AdapterInput,
    SubscriptionMutation,
    /// Outbound, Ping and Close records are observed, not executed.
    ObservedOnly,
}

/// One synchronous sink call per complete record, before reading the next.
/// The raw record remains unchanged even if the adapter uses a mutable parser.
/// Actions retain adapter order and are observations, never network execution.
/// This is not a redaction boundary: raw records and HTTP headers/body are
/// visible to the sink. Rejecting sensitive wire actions does not sanitize them.
pub struct LegacyReplayStep<'a> {
    pub position: LegacyReplayPosition,
    pub record: &'a RawRecord,
    pub kind: LegacyReplayStepKind,
    pub metadata: Option<&'a MetadataRecord>,
    pub actions: &'a [SessionAction],
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LegacyReplayProgress {
    pub segments_completed: u64,
    pub input_bytes_read: u64,
    pub records_read: u64,
    pub callbacks_invoked: u64,
    pub callbacks_completed: u64,
    pub steps_acknowledged: u64,
    pub sessions_started: usize,
    pub metadata_bytes_retained: usize,
    pub last_read: Option<LegacyReplayPosition>,
    pub last_invoked: Option<LegacyReplayPosition>,
    pub last_completed: Option<LegacyReplayPosition>,
    pub last_acknowledged: Option<LegacyReplayPosition>,
}

#[derive(Debug, Error)]
pub enum LegacyReplayError {
    #[error("invalid legacy stream limits")]
    InvalidLimits,
    #[error("replay is terminal after a failure; reconstruct from the original prefix")]
    Poisoned,
    #[error(transparent)]
    Recording(#[from] RecordingError),
    #[error(transparent)]
    Adapter(#[from] AdapterError),
    #[error("session {0:?} has no preceding metadata")]
    MissingMetadata(SessionId),
    #[error("metadata header identity or schema is invalid")]
    InvalidMetadata,
    #[error("conflicting metadata cannot replace an existing binding")]
    ConflictingMetadata,
    #[error("session budget exhausted")]
    SessionBudget,
    #[error("retained metadata byte budget exhausted")]
    MetadataBudget,
    #[error("input record budget exhausted")]
    RecordBudget,
    #[error("input byte budget exhausted")]
    InputByteBudget,
    #[error("adapter action buffer dropped {0} actions")]
    ActionOverflow(u64),
    #[error("output event budget exhausted")]
    EventBudget,
    #[error("output byte budget exhausted")]
    OutputByteBudget,
    #[error("output serialization failed: {0}")]
    OutputEncoding(String),
    #[error("sensitive actions are unsupported in public legacy reconstruction")]
    SensitiveAction,
    #[error("native observation batches require an enriched replay profile")]
    NativeObservations,
    #[error("recorded subscription wire action differs from the adapter")]
    SubscriptionWireMismatch,
    #[error("sink failed; the current step may be partially visible: {0}")]
    Sink(String),
    #[error("replay counter overflow")]
    CounterOverflow,
}

#[derive(Debug, Error)]
#[error("legacy stream failed at {at:?}: {cause}")]
pub struct LegacyReplayFailure {
    pub at: LegacyReplayPosition,
    pub progress: Box<LegacyReplayProgress>,
    #[source]
    pub cause: LegacyReplayError,
}

struct SessionState {
    metadata: SessionRecordingMetadata,
    machine: Box<dyn SessionMachine>,
}

/// Retains bounded runner state across explicitly ordered segments. It does
/// not retain output histories. Arbitrary adapter/factory/sink allocations and
/// blocking behavior remain their owners' responsibility; output checks occur
/// after the adapter returns and cannot prevent its own temporary allocations.
pub struct LegacyStreamingReplay<F> {
    factory: F,
    limits: LegacyReplayLimits,
    sessions: BTreeMap<SessionId, SessionState>,
    build: Option<BuildMetadata>,
    actions: ActionBuffer,
    progress: LegacyReplayProgress,
    poisoned: bool,
}

impl<F> LegacyStreamingReplay<F>
where
    F: FnMut(&SessionRecordingMetadata) -> Result<LegacyReplaySession, AdapterError>,
{
    pub fn new(limits: LegacyReplayLimits, factory: F) -> Result<Self, LegacyReplayError> {
        if limits.max_sessions == 0
            || limits.max_metadata_bytes == 0
            || limits.max_actions_per_step == 0
            || limits.max_actions_per_step > DEFAULT_ACTION_BUFFER_CAPACITY
            || limits.max_events_per_step == 0
            || limits.max_output_bytes_per_step == 0
            || limits.max_records == 0
            || limits.max_input_bytes < HEADER_SIZE as u64
            || limits.max_input_bytes == u64::MAX
        {
            return Err(LegacyReplayError::InvalidLimits);
        }
        Ok(Self {
            factory,
            limits,
            sessions: BTreeMap::new(),
            build: None,
            actions: ActionBuffer::with_capacity(limits.max_actions_per_step),
            progress: LegacyReplayProgress::default(),
            poisoned: false,
        })
    }

    pub fn progress(&self) -> &LegacyReplayProgress {
        &self.progress
    }

    /// Strict physical read: incomplete headers/payloads and CRC errors stop the
    /// run. Unlike read_all, no tail is silently discarded. Identical repeated
    /// metadata at rotation does not restart a machine. Caller supplies segment
    /// order; MFR cannot prove collector identity, continuity or generations.
    ///
    /// On any failure (including sink failure or unwind), this object is
    /// terminal. Successful callbacks can already have mutated machine state;
    /// a failed sink can have external side effects. No transparent retry,
    /// rollback or exactly-once sink delivery is promised.
    pub fn replay_segment<R: Read, S>(
        &mut self,
        source: R,
        mut sink: S,
    ) -> Result<LegacyReplayProgress, LegacyReplayFailure>
    where
        S: for<'a> FnMut(LegacyReplayStep<'a>) -> Result<(), String>,
    {
        let mut at = LegacyReplayPosition {
            segment: self.progress.segments_completed,
            record: 0,
        };
        if self.poisoned {
            return Err(LegacyReplayFailure {
                at,
                progress: Box::new(self.progress.clone()),
                cause: LegacyReplayError::Poisoned,
            });
        }
        // Also remains poisoned if caller catches a factory/adapter/sink panic.
        self.poisoned = true;
        let result = self.run_segment(source, &mut sink, &mut at);
        match result {
            Ok(()) => {
                self.poisoned = false;
                Ok(self.progress.clone())
            }
            Err(cause) => {
                // Release rejected payloads even if the terminal runner lives
                // on for inspection. Preserve all failure positions/counters.
                self.actions.clear();
                self.actions.take_dropped();
                Err(LegacyReplayFailure {
                    at,
                    progress: Box::new(self.progress.clone()),
                    cause,
                })
            }
        }
    }

    fn run_segment<R: Read, S>(
        &mut self,
        source: R,
        sink: &mut S,
        at: &mut LegacyReplayPosition,
    ) -> Result<(), LegacyReplayError>
    where
        S: for<'a> FnMut(LegacyReplayStep<'a>) -> Result<(), String>,
    {
        let used = Cell::new(self.progress.input_bytes_read);
        let exhausted = Cell::new(false);
        let opened = RawSegmentReader::open(QuotaReader {
            source,
            used: &used,
            exhausted: &exhausted,
            max: self.limits.max_input_bytes,
        });
        self.progress.input_bytes_read = used.get();
        let mut reader = opened.map_err(|e| read_error(e, exhausted.get()))?;
        loop {
            at.record = reader
                .records_read
                .checked_add(1)
                .ok_or(LegacyReplayError::CounterOverflow)?;
            let next = reader.read_record();
            self.progress.input_bytes_read = used.get();
            let Some(record) = next.map_err(|e| read_error(e, exhausted.get()))? else {
                break;
            };
            increment(&mut self.progress.records_read)?;
            self.progress.last_read = Some(*at);
            if self.progress.records_read > self.limits.max_records {
                return Err(LegacyReplayError::RecordBudget);
            }
            self.actions.clear();
            self.actions.take_dropped();
            let (kind, metadata) = self.prepare_record(&record, *at)?;
            self.validate_actions()?;
            sink(LegacyReplayStep {
                position: *at,
                record: &record,
                kind,
                metadata: metadata.as_ref(),
                actions: self.actions.as_slice(),
            })
            .map_err(LegacyReplayError::Sink)?;
            increment(&mut self.progress.steps_acknowledged)?;
            self.progress.last_acknowledged = Some(*at);
            self.actions.clear();
        }
        increment(&mut self.progress.segments_completed)?;
        Ok(())
    }

    fn prepare_record(
        &mut self,
        record: &RawRecord,
        at: LegacyReplayPosition,
    ) -> Result<(LegacyReplayStepKind, Option<MetadataRecord>), LegacyReplayError> {
        let header = &record.header;
        if header.opcode == FrameOpcode::Metadata {
            let metadata = decode_metadata(&record.payload)?;
            if header.direction != Direction::Inbound || header.session != metadata.session_id() {
                return Err(LegacyReplayError::InvalidMetadata);
            }
            let mut kind = LegacyReplayStepKind::Metadata;
            match &metadata {
                MetadataRecord::Build(build) => {
                    if build.schema_version != 1 {
                        return Err(LegacyReplayError::InvalidMetadata);
                    }
                    match &self.build {
                        Some(existing) if existing != build => {
                            return Err(LegacyReplayError::ConflictingMetadata);
                        }
                        Some(_) => {}
                        None => {
                            self.charge_metadata(record.payload.len())?;
                            self.build = Some(build.clone());
                        }
                    }
                }
                MetadataRecord::Session(meta) => {
                    if meta.schema_version != 1 || meta.session_id == 0 {
                        return Err(LegacyReplayError::InvalidMetadata);
                    }
                    if let Some(existing) = self.sessions.get(&header.session) {
                        if existing.metadata != *meta {
                            return Err(LegacyReplayError::ConflictingMetadata);
                        }
                    } else {
                        if self.sessions.len() >= self.limits.max_sessions {
                            return Err(LegacyReplayError::SessionBudget);
                        }
                        let metadata_total = self.metadata_total(record.payload.len())?;
                        let mut session = (self.factory)(meta)?;
                        self.note_invoked(at)?;
                        session
                            .machine
                            .on_replay_start(session.assumed_start, &mut self.actions)?;
                        self.note_completed(at)?;
                        kind = LegacyReplayStepKind::SessionStarted {
                            assumed_start: session.assumed_start,
                        };
                        self.sessions.insert(
                            header.session,
                            SessionState {
                                metadata: meta.clone(),
                                machine: session.machine,
                            },
                        );
                        self.progress.metadata_bytes_retained = metadata_total;
                        self.progress.sessions_started = self.sessions.len();
                    }
                }
            }
            return Ok((kind, Some(metadata)));
        }
        if !self.sessions.contains_key(&header.session) {
            return Err(LegacyReplayError::MissingMetadata(header.session));
        }
        if header.direction == Direction::Outbound
            || matches!(header.opcode, FrameOpcode::Ping | FrameOpcode::Close)
        {
            return Ok((LegacyReplayStepKind::ObservedOnly, None));
        }
        let stamp = FrameStamp {
            receive_ts: TimestampNs(header.receive_ts_ns),
            mono_ns: header.monotonic_ns,
        };
        let http = if header.opcode == FrameOpcode::HttpResponse {
            Some(decode_http_response(&record.payload)?)
        } else {
            None
        };
        let subscription = if header.opcode == FrameOpcode::SubscriptionCommand {
            Some(decode_subscription_command(&record.payload)?)
        } else {
            None
        };
        self.note_invoked(at)?;
        let machine = &mut self
            .sessions
            .get_mut(&header.session)
            .expect("checked session")
            .machine;
        let kind = match header.opcode {
            FrameOpcode::Text | FrameOpcode::Binary => {
                let mut payload = record.payload.clone();
                let input = if header.opcode == FrameOpcode::Text {
                    SessionInput::TextFrame {
                        bytes: &mut payload,
                        received: stamp,
                    }
                } else {
                    SessionInput::BinaryFrame {
                        bytes: &mut payload,
                        received: stamp,
                    }
                };
                machine.on_input(input, &mut self.actions)?;
                LegacyReplayStepKind::AdapterInput
            }
            FrameOpcode::Pong => {
                machine.on_input(
                    SessionInput::Pong {
                        payload: &record.payload,
                        received: stamp,
                    },
                    &mut self.actions,
                )?;
                LegacyReplayStepKind::AdapterInput
            }
            FrameOpcode::HttpResponse => {
                let (request_id, response) = http.expect("decoded HTTP input");
                machine.on_input(
                    SessionInput::HttpResponse {
                        request_id,
                        response: &response,
                        received: stamp,
                    },
                    &mut self.actions,
                )?;
                LegacyReplayStepKind::AdapterInput
            }
            FrameOpcode::SubscriptionCommand => {
                let (command, wire) = subscription.expect("decoded subscription input");
                if machine.prepare_dynamic_subscription(&command)? != wire {
                    return Err(LegacyReplayError::SubscriptionWireMismatch);
                }
                machine.commit_dynamic_subscription(&command);
                LegacyReplayStepKind::SubscriptionMutation
            }
            FrameOpcode::Metadata | FrameOpcode::Ping | FrameOpcode::Close => {
                unreachable!("handled earlier")
            }
        };
        self.note_completed(at)?;
        Ok((kind, None))
    }

    fn note_invoked(&mut self, at: LegacyReplayPosition) -> Result<(), LegacyReplayError> {
        increment(&mut self.progress.callbacks_invoked)?;
        self.progress.last_invoked = Some(at);
        Ok(())
    }
    fn note_completed(&mut self, at: LegacyReplayPosition) -> Result<(), LegacyReplayError> {
        increment(&mut self.progress.callbacks_completed)?;
        self.progress.last_completed = Some(at);
        Ok(())
    }
    fn charge_metadata(&mut self, bytes: usize) -> Result<(), LegacyReplayError> {
        self.progress.metadata_bytes_retained = self.metadata_total(bytes)?;
        Ok(())
    }
    fn metadata_total(&self, bytes: usize) -> Result<usize, LegacyReplayError> {
        let total = self
            .progress
            .metadata_bytes_retained
            .checked_add(bytes)
            .ok_or(LegacyReplayError::MetadataBudget)?;
        if total > self.limits.max_metadata_bytes {
            return Err(LegacyReplayError::MetadataBudget);
        }
        Ok(total)
    }
    fn validate_actions(&mut self) -> Result<(), LegacyReplayError> {
        let dropped = self.actions.take_dropped();
        if dropped != 0 {
            return Err(LegacyReplayError::ActionOverflow(dropped));
        }
        let mut events = 0usize;
        let mut bytes = OutputBudget {
            used: 0,
            max: self.limits.max_output_bytes_per_step,
            exceeded: false,
        };
        for action in self.actions.as_slice() {
            let added = match action {
                SessionAction::EmitBatch(batch) => batch.events.len(),
                SessionAction::EmitObservedBatch(_) => {
                    return Err(LegacyReplayError::NativeObservations);
                }
                SessionAction::EmitSystem(_) => 1,
                _ => 0,
            };
            events = events
                .checked_add(added)
                .ok_or(LegacyReplayError::EventBudget)?;
            if events > self.limits.max_events_per_step {
                return Err(LegacyReplayError::EventBudget);
            }
            match action {
                SessionAction::EmitBatch(batch) => {
                    encode_count(&mut bytes, batch)?;
                }
                SessionAction::EmitSystem(event) => encode_count(&mut bytes, event)?,
                SessionAction::SendText(payload)
                | SessionAction::SendBinary(payload)
                | SessionAction::SendPing(payload) => bytes.charge(payload.len())?,
                SessionAction::SendSensitiveText(_) => {
                    return Err(LegacyReplayError::SensitiveAction);
                }
                SessionAction::RequestHttp(request) => {
                    bytes.charge(request.url.len())?;
                    for (key, value) in &request.headers {
                        // Include HTTP separators so even empty header pairs
                        // consume budget and cannot hide unbounded cardinality.
                        bytes.charge(4)?;
                        bytes.charge(key.len())?;
                        bytes.charge(value.len())?;
                    }
                    if let Some(body) = &request.body {
                        bytes.charge(body.len())?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

fn increment(value: &mut u64) -> Result<(), LegacyReplayError> {
    *value = value
        .checked_add(1)
        .ok_or(LegacyReplayError::CounterOverflow)?;
    Ok(())
}
fn read_error(error: RecordingError, exhausted: bool) -> LegacyReplayError {
    if exhausted {
        LegacyReplayError::InputByteBudget
    } else {
        LegacyReplayError::Recording(error)
    }
}

struct QuotaReader<'a, R> {
    source: R,
    used: &'a Cell<u64>,
    exhausted: &'a Cell<bool>,
    max: u64,
}
impl<R: Read> Read for QuotaReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let remaining = self.max.saturating_sub(self.used.get());
        if remaining == 0 {
            let mut probe = [0];
            if self.source.read(&mut probe)? == 0 {
                return Ok(0);
            }
            self.used.set(self.used.get() + 1);
            self.exhausted.set(true);
            return Err(io::Error::other("legacy replay input byte budget"));
        }
        let cap = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(buf.len());
        let read = self.source.read(&mut buf[..cap])?;
        self.used.set(self.used.get() + read as u64);
        Ok(read)
    }
}

struct OutputBudget {
    used: usize,
    max: usize,
    exceeded: bool,
}
impl OutputBudget {
    fn charge(&mut self, bytes: usize) -> Result<(), LegacyReplayError> {
        if bytes > self.max.saturating_sub(self.used) {
            self.exceeded = true;
            return Err(LegacyReplayError::OutputByteBudget);
        }
        self.used += bytes;
        Ok(())
    }
}
impl Write for OutputBudget {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.charge(buf.len()).map_err(io::Error::other)?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn encode_count<T: serde::Serialize>(
    budget: &mut OutputBudget,
    value: &T,
) -> Result<(), LegacyReplayError> {
    serde_json::to_writer(&mut *budget, value).map_err(|e| {
        if budget.exceeded {
            LegacyReplayError::OutputByteBudget
        } else {
            LegacyReplayError::OutputEncoding(e.to_string())
        }
    })
}
