//! Session runner: stamp → record → SessionMachine → bounded dispatch.
//!
//! # Mirror Vecs (fail policy)
//! `market_batches`, `system_events`, and `other_actions` are **bounded** diagnostic
//! mirrors (`mirror_capacity`). They are not the data plane — consumers MUST read
//! from `EventDispatcher`. Overflow follows `overflow`:
//! - `FailEngine` → `DispatchError::FailEngine` (default; fail loud).
//! - `DropNewest` / `DropOldest` → drop from the mirror and emit
//!   `SystemEvent::EventsDropped` (never silent).
//! - `mirror_capacity == 0` disables mirrors (production live loops).

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, DEFAULT_ACTION_BUFFER_CAPACITY, DisconnectReason, EventBatch, HttpRequestSpec,
    HttpResponse, SessionAction, SessionCommand, SessionInput, SessionMachine,
    SubscriptionWireAction,
};
use marketfeed_dispatch::{DispatchError, EventDispatcher, PushOutcome};
use marketfeed_model::{
    CatalogView, ConnectionId, EventEnvelope, EventFlags, FrameStamp, InstrumentUpdate,
    MarketEvent, OverflowPolicy, SessionId, SystemEvent, TimestampNs, VenueId, VenueStatus,
};
use marketfeed_recording::{
    Direction, EnqueueOutcome, FrameOpcode as RecOpcode, RawSegmentWriter, RecordingHandle,
    encode_http_response, encode_subscription_command,
};
use marketfeed_sinks::{EventSink, SinkError, forward_dispatcher};
use marketfeed_transport::{FrameOpcode, OutboundFrame};

use crate::metrics::EngineMetrics;
use crate::{EngineError, SessionLifecycle};

/// Max outstanding timers per session (id → fire_at map, not a hierarchical wheel).
const MAX_SESSION_TIMERS: usize = 32;
/// Cap due-timer deliveries per poll so a buggy adapter cannot hot-loop the runner.
const MAX_TIMER_FIRES_PER_POLL: usize = 64;
/// Bound outbound frames waiting for the live flush loop.
const MAX_PENDING_WRITES: usize = 256;
/// Bound HTTP request specs waiting for the live HTTP worker.
const MAX_PENDING_HTTP: usize = 64;

enum MirrorPush {
    Ok,
    Dropped { detail: String },
}

fn sink_to_engine(err: SinkError) -> EngineError {
    match err {
        SinkError::FailEngine => EngineError::Dispatch(DispatchError::FailEngine),
        SinkError::DeadlineExceeded => EngineError::Dispatch(DispatchError::DeadlineExceeded),
        SinkError::UnsupportedPolicy(p) => {
            EngineError::Dispatch(DispatchError::UnsupportedPolicy(p))
        }
        SinkError::Io(msg) => EngineError::Internal(format!("sink io: {msg}")),
        SinkError::Unsupported(msg) => EngineError::Internal(format!("sink unsupported: {msg}")),
    }
}

#[derive(Debug, Clone)]
pub struct SessionRunnerConfig {
    pub venue: VenueId,
    pub connection: ConnectionId,
    pub session: SessionId,
    pub dispatch_capacity: usize,
    pub overflow: OverflowPolicy,
    pub record: bool,
    /// Optional process-wide persistent recorder. When absent, `record=true`
    /// retains an in-memory MFR1 segment for deterministic record/replay tests.
    pub recording_pipeline: Option<RecordingHandle>,
    /// Bound for diagnostic mirror Vecs; `0` disables mirroring.
    pub mirror_capacity: usize,
    /// Optional shared flag set true on MarkLive / false on disconnect.
    pub live_signal: Option<Arc<AtomicBool>>,
    /// Shared counters for daemon `/metrics` (created if `None`).
    pub metrics: Option<Arc<EngineMetrics>>,
    /// Shared stop request (daemon shutdown); live loop polls this without exclusive borrow.
    pub stop_signal: Option<Arc<AtomicBool>>,
}

impl Default for SessionRunnerConfig {
    fn default() -> Self {
        Self {
            venue: VenueId(0),
            connection: ConnectionId(1),
            session: SessionId(1),
            dispatch_capacity: 1024,
            overflow: OverflowPolicy::FailEngine,
            record: true,
            recording_pipeline: None,
            // Tests keep short histories; live daemon sets 0.
            mirror_capacity: 1024,
            live_signal: None,
            metrics: None,
            stop_signal: None,
        }
    }
}

/// Owns one adapter session machine and optional raw recorder.
pub struct SessionRunner {
    cfg: SessionRunnerConfig,
    machine: Box<dyn SessionMachine>,
    dispatch: EventDispatcher,
    actions: ActionBuffer,
    recorder: Option<RawSegmentWriter<Cursor<Vec<u8>>>>,
    recording_pipeline: Option<RecordingHandle>,
    pub lifecycle: SessionLifecycle,
    /// Wire-level inbound frame sequence (recording / transport order).
    inbound_frame_seq: u64,
    pub market_batches: Vec<EventBatch>,
    pub system_events: Vec<SystemEvent>,
    pub other_actions: Vec<SessionAction>,
    pending_writes: Vec<OutboundFrame>,
    pending_http: Vec<HttpRequestSpec>,
    /// Outstanding one-shot timers; same `timer_id` replaces; fire removes then delivers `SessionInput::Timer`.
    timers: HashMap<u64, TimestampNs>,
    pub reconnect_requested: bool,
    pub stop_requested: bool,
    pub metrics: Arc<EngineMetrics>,
}

impl SessionRunner {
    /// True if local stop or shared stop_signal is set.
    pub fn is_stop_requested(&self) -> bool {
        self.stop_requested
            || self
                .cfg
                .stop_signal
                .as_ref()
                .is_some_and(|s| s.load(Ordering::Relaxed))
    }

    pub(crate) fn shared_stop_signal(&self) -> Option<Arc<AtomicBool>> {
        self.cfg.stop_signal.clone()
    }

    /// Drain bounded dispatch lanes (batches + system events) into local vecs.
    pub fn drain_dispatch(&mut self) -> (usize, usize) {
        let batches = self.dispatch.drain_batches();
        let systems = self.dispatch.drain_systems();
        let nb = batches.len();
        let ns = systems.len();
        let cap = self.mirror_capacity();
        for batch in batches {
            let _ = Self::try_push_mirror(&mut self.market_batches, cap, self.cfg.overflow, batch);
        }
        for ev in systems {
            let _ = Self::try_push_mirror(&mut self.system_events, cap, self.cfg.overflow, ev);
        }
        (nb, ns)
    }

    /// Mutable access to the data-plane dispatcher (sinks / tests).
    pub fn dispatch_mut(&mut self) -> &mut EventDispatcher {
        &mut self.dispatch
    }

    /// Query the session machine's live L2 book (§19.2).
    pub fn book_snapshot(
        &self,
        instrument: marketfeed_model::InstrumentId,
        depth: Option<u32>,
    ) -> Option<marketfeed_model::BookSnapshot> {
        self.machine.book_snapshot(instrument, depth)
    }

    /// Forward dispatch into `sink`, or null-drain (mirrors) when `sink` is `None`.
    ///
    /// Always empties the dispatcher so `FailEngine` queues cannot fill without a consumer.
    pub fn consume_dispatch<S: EventSink + ?Sized>(
        &mut self,
        sink: Option<&mut S>,
    ) -> Result<(), EngineError> {
        match sink {
            Some(s) => {
                let t0 = std::time::Instant::now();
                let out = forward_dispatcher(&mut self.dispatch, s).map_err(sink_to_engine);
                self.metrics
                    .observe_sink_write_ns(t0.elapsed().as_nanos() as u64);
                let report = out?;
                let dropped = report.dropped_total();
                if dropped > 0 {
                    self.emit_events_dropped(dropped, "configured sink overflow policy")?;
                }
                Ok(())
            }
            None => {
                let _ = self.drain_dispatch();
                Ok(())
            }
        }
    }

    pub fn dispatch_pending(&self) -> (usize, usize) {
        (self.dispatch.batches().len(), self.dispatch.systems().len())
    }

    pub fn request_stop(&mut self) {
        self.stop_requested = true;
        if let Some(s) = &self.cfg.stop_signal {
            s.store(true, Ordering::Relaxed);
        }
    }

    pub fn new(
        machine: Box<dyn SessionMachine>,
        cfg: SessionRunnerConfig,
    ) -> Result<Self, EngineError> {
        let metrics = cfg
            .metrics
            .clone()
            .unwrap_or_else(|| Arc::new(EngineMetrics::new()));
        metrics.set_queue_gauges(0, cfg.dispatch_capacity, 0, cfg.dispatch_capacity);
        let action_buffer_capacity = cfg
            .dispatch_capacity
            .saturating_mul(4)
            .max(DEFAULT_ACTION_BUFFER_CAPACITY);
        let recorder = if cfg.record && cfg.recording_pipeline.is_none() {
            Some(RawSegmentWriter::create(Cursor::new(Vec::new()), 0)?)
        } else {
            None
        };
        let recording_pipeline = cfg.recording_pipeline.clone();
        Ok(Self {
            dispatch: EventDispatcher::new(
                cfg.dispatch_capacity,
                cfg.dispatch_capacity,
                cfg.overflow,
            ),
            cfg,
            machine,
            actions: ActionBuffer::with_capacity(action_buffer_capacity),
            recorder,
            recording_pipeline,
            lifecycle: SessionLifecycle::Planned,
            inbound_frame_seq: 0,
            market_batches: Vec::new(),
            system_events: Vec::new(),
            other_actions: Vec::new(),
            pending_writes: Vec::new(),
            pending_http: Vec::new(),
            timers: HashMap::new(),
            reconnect_requested: false,
            stop_requested: false,
            metrics,
        })
    }

    pub fn on_connected(&mut self, now: TimestampNs) -> Result<(), EngineError> {
        self.lifecycle = SessionLifecycle::Connected;
        self.timers.clear();
        self.actions.clear();
        self.machine
            .on_input(SessionInput::Connected { now }, &mut self.actions)?;
        self.note_action_buffer_drops()?;
        self.apply_actions(now)?;
        // Spec §8.7 / R6: thin VenueStatus on connect (engine-owned, adapters stay I/O-free).
        self.emit_venue_status("connected", now)?;
        Ok(())
    }

    pub fn on_disconnected(
        &mut self,
        reason: DisconnectReason,
        now: TimestampNs,
    ) -> Result<(), EngineError> {
        self.lifecycle = SessionLifecycle::Draining;
        self.set_live_signal(false);
        self.timers.clear();
        self.actions.clear();
        self.machine.on_input(
            SessionInput::Disconnected { reason, now },
            &mut self.actions,
        )?;
        self.note_action_buffer_drops()?;
        self.apply_actions(now)?;
        self.lifecycle = SessionLifecycle::Stopped;
        Ok(())
    }

    pub fn on_text_frame(
        &mut self,
        bytes: &mut [u8],
        received: FrameStamp,
    ) -> Result<(), EngineError> {
        let t0 = std::time::Instant::now();
        self.metrics.record_frame_received(bytes.len());
        self.inbound_frame_seq += 1;
        let frame_seq = self.inbound_frame_seq;
        self.record_frame(
            frame_seq,
            received.receive_ts.0,
            received.mono_ns,
            Direction::Inbound,
            RecOpcode::Text,
            bytes,
        )?;
        self.actions.clear();
        let parse_t0 = std::time::Instant::now();
        self.machine.on_input(
            SessionInput::TextFrame { bytes, received },
            &mut self.actions,
        )?;
        self.metrics
            .observe_parse_duration_ns(parse_t0.elapsed().as_nanos() as u64);
        self.note_action_buffer_drops()?;
        let out = self.apply_actions(received.receive_ts);
        self.metrics
            .observe_frame_to_event_ns(t0.elapsed().as_nanos() as u64);
        out
    }

    pub fn on_binary_frame(
        &mut self,
        bytes: &mut [u8],
        received: FrameStamp,
    ) -> Result<(), EngineError> {
        let t0 = std::time::Instant::now();
        self.metrics.record_frame_received(bytes.len());
        self.inbound_frame_seq += 1;
        let frame_seq = self.inbound_frame_seq;
        self.record_frame(
            frame_seq,
            received.receive_ts.0,
            received.mono_ns,
            Direction::Inbound,
            RecOpcode::Binary,
            bytes,
        )?;
        self.actions.clear();
        let parse_t0 = std::time::Instant::now();
        self.machine.on_input(
            SessionInput::BinaryFrame { bytes, received },
            &mut self.actions,
        )?;
        self.metrics
            .observe_parse_duration_ns(parse_t0.elapsed().as_nanos() as u64);
        self.note_action_buffer_drops()?;
        let out = self.apply_actions(received.receive_ts);
        self.metrics
            .observe_frame_to_event_ns(t0.elapsed().as_nanos() as u64);
        out
    }

    /// Transport already auto-replied with Pong; persist and count the frame.
    pub fn on_ping_frame(
        &mut self,
        payload: &[u8],
        received: FrameStamp,
    ) -> Result<(), EngineError> {
        self.metrics.record_frame_received(payload.len());
        self.inbound_frame_seq += 1;
        self.record_frame(
            self.inbound_frame_seq,
            received.receive_ts.0,
            received.mono_ns,
            Direction::Inbound,
            RecOpcode::Ping,
            payload,
        )
    }

    /// Persist a remote close control frame before reconnect handling.
    pub fn on_close_frame(
        &mut self,
        payload: &[u8],
        received: FrameStamp,
    ) -> Result<(), EngineError> {
        self.metrics.record_frame_received(payload.len());
        self.inbound_frame_seq += 1;
        self.record_frame(
            self.inbound_frame_seq,
            received.receive_ts.0,
            received.mono_ns,
            Direction::Inbound,
            RecOpcode::Close,
            payload,
        )
    }

    /// Deliver `SessionInput::Pong` to the adapter (app-level heartbeat ACKs).
    pub fn on_pong_frame(
        &mut self,
        payload: &[u8],
        received: FrameStamp,
    ) -> Result<(), EngineError> {
        self.metrics.record_frame_received(payload.len());
        self.inbound_frame_seq += 1;
        let frame_seq = self.inbound_frame_seq;
        self.record_frame(
            frame_seq,
            received.receive_ts.0,
            received.mono_ns,
            Direction::Inbound,
            RecOpcode::Pong,
            payload,
        )?;
        self.actions.clear();
        self.machine
            .on_input(SessionInput::Pong { payload, received }, &mut self.actions)?;
        self.note_action_buffer_drops()?;
        self.apply_actions(received.receive_ts)
    }

    pub fn recording_bytes(&self) -> Option<Vec<u8>> {
        self.recorder
            .as_ref()
            .map(|r| r.get_ref().get_ref().clone())
    }

    pub fn take_recording(mut self) -> Option<Vec<u8>> {
        self.recorder.take().map(|w| w.into_inner().into_inner())
    }

    fn record_frame(
        &mut self,
        frame_seq: u64,
        receive_ts_ns: i64,
        monotonic_ns: u64,
        direction: Direction,
        opcode: RecOpcode,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        if let Some(pipeline) = &self.recording_pipeline {
            let outcome = pipeline.enqueue(
                self.cfg.session,
                frame_seq,
                receive_ts_ns,
                monotonic_ns,
                direction,
                opcode,
                0,
                payload,
            )?;
            if !matches!(outcome, EnqueueOutcome::Accepted) {
                self.metrics.record_queue_overflow();
            }
        } else if let Some(recorder) = self.recorder.as_mut() {
            recorder.write_record(
                self.cfg.session,
                frame_seq,
                receive_ts_ns,
                monotonic_ns,
                direction,
                opcode,
                0,
                payload,
            )?;
        }
        Ok(())
    }

    pub fn take_pending_writes(&mut self) -> Vec<OutboundFrame> {
        let writes = std::mem::take(&mut self.pending_writes);
        for w in &writes {
            self.metrics.record_frame_sent(w.payload.len());
        }
        writes
    }

    pub fn take_pending_http(&mut self) -> Vec<HttpRequestSpec> {
        std::mem::take(&mut self.pending_http)
    }

    pub fn next_timer_deadline(&self) -> Option<TimestampNs> {
        self.timers.values().copied().min_by_key(|t| t.0)
    }

    pub fn delay_until_next_timer(&self, now: TimestampNs) -> Option<Duration> {
        let next = self.next_timer_deadline()?;
        let delta_ns = next.0.saturating_sub(now.0);
        if delta_ns == 0 {
            Some(Duration::ZERO)
        } else {
            Some(Duration::from_nanos(delta_ns as u64))
        }
    }

    pub fn timer_count(&self) -> usize {
        self.timers.len()
    }

    pub fn poll_timers(&mut self, now: TimestampNs) -> Result<(), EngineError> {
        let mut fired = 0usize;
        while fired < MAX_TIMER_FIRES_PER_POLL {
            let mut due: Vec<u64> = self
                .timers
                .iter()
                .filter(|(_, fire_at)| fire_at.0 <= now.0)
                .map(|(id, _)| *id)
                .collect();
            if due.is_empty() {
                return Ok(());
            }
            due.sort_unstable();
            for timer_id in due {
                if self.timers.remove(&timer_id).is_none() {
                    continue;
                }
                fired += 1;
                self.actions.clear();
                self.machine
                    .on_input(SessionInput::Timer { timer_id, now }, &mut self.actions)?;
                self.note_action_buffer_drops()?;
                self.apply_actions(now)?;
                if fired >= MAX_TIMER_FIRES_PER_POLL {
                    // ponytail: hard stop after 64 fires/poll; ceiling = starved I/O if adapter
                    // keeps scheduling already-due timers; upgrade = require fire_at > now.
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    pub fn on_http_response(
        &mut self,
        request_id: u64,
        response: &HttpResponse,
        received: FrameStamp,
    ) -> Result<(), EngineError> {
        self.inbound_frame_seq = self.inbound_frame_seq.saturating_add(1);
        let frame_seq = self.inbound_frame_seq;
        let payload = encode_http_response(request_id, response)?;
        self.metrics.record_frame_received(payload.len());
        self.record_frame(
            frame_seq,
            received.receive_ts.0,
            received.mono_ns,
            Direction::Inbound,
            RecOpcode::HttpResponse,
            &payload,
        )?;
        self.actions.clear();
        let parse_t0 = std::time::Instant::now();
        self.machine.on_input(
            SessionInput::HttpResponse {
                request_id,
                response,
                received,
            },
            &mut self.actions,
        )?;
        self.metrics
            .observe_parse_duration_ns(parse_t0.elapsed().as_nanos() as u64);
        self.note_action_buffer_drops()?;
        self.apply_actions(received.receive_ts)
    }

    pub fn on_transport_lost(
        &mut self,
        reason: DisconnectReason,
        now: TimestampNs,
    ) -> Result<(), EngineError> {
        self.lifecycle = SessionLifecycle::Backoff;
        self.set_live_signal(false);
        self.timers.clear();
        self.actions.clear();
        self.machine.on_input(
            SessionInput::Disconnected { reason, now },
            &mut self.actions,
        )?;
        self.note_action_buffer_drops()?;
        self.apply_actions(now)?;
        self.reconnect_requested = false;
        Ok(())
    }

    pub fn note_reconnect(&self) {
        self.metrics.record_reconnect();
    }

    pub fn venue(&self) -> VenueId {
        self.cfg.venue
    }

    /// Deliver a control command to the SessionMachine (engine applies networking side-effects).
    pub fn deliver_control(
        &mut self,
        command: SessionCommand,
        now: TimestampNs,
    ) -> Result<(), EngineError> {
        if matches!(
            command,
            SessionCommand::Subscribe(_)
                | SessionCommand::Unsubscribe(_)
                | SessionCommand::Replace(_)
        ) {
            return self.deliver_subscription_control(command, now);
        }
        self.actions.clear();
        self.machine.on_input(
            SessionInput::Control { command: &command },
            &mut self.actions,
        )?;
        self.note_action_buffer_drops()?;
        self.apply_actions_with_system_policy(now, false)
    }

    fn deliver_subscription_control(
        &mut self,
        command: SessionCommand,
        now: TimestampNs,
    ) -> Result<(), EngineError> {
        if self.pending_writes.len() >= MAX_PENDING_WRITES {
            return Err(DispatchError::FailEngine.into());
        }

        // Preparation is side-effect free by contract. Recording and the
        // bounded queue reservation therefore complete before adapter-local
        // state is committed.
        let wire = self.machine.prepare_dynamic_subscription(&command)?;
        let (pending, mirror) = match &wire {
            SubscriptionWireAction::Text(payload) => (
                OutboundFrame {
                    opcode: FrameOpcode::Text,
                    payload: payload.to_vec(),
                },
                SessionAction::SendText(payload.clone()),
            ),
            SubscriptionWireAction::Binary(payload) => (
                OutboundFrame {
                    opcode: FrameOpcode::Binary,
                    payload: payload.to_vec(),
                },
                SessionAction::SendBinary(payload.clone()),
            ),
            SubscriptionWireAction::Ping(payload) => (
                OutboundFrame {
                    opcode: FrameOpcode::Ping,
                    payload: payload.to_vec(),
                },
                SessionAction::SendPing(payload.clone()),
            ),
        };
        self.record_subscription_command(&command, &wire, now)?;
        self.pending_writes.push(pending);
        self.machine.commit_dynamic_subscription(&command);

        // Mirrors remain diagnostic: once the authoritative frame is queued
        // and adapter state committed, mirror saturation cannot roll it back.
        let result = self.push_mirror_action(mirror);
        self.settle_diagnostic_result(result, false)
    }

    pub fn push_system(&mut self, ev: SystemEvent) -> Result<(), EngineError> {
        self.trace_system_event(&ev);
        self.metrics.observe_system(&ev);
        let outcome = self.dispatch.push_system(ev.clone())?;
        match outcome {
            PushOutcome::Accepted => self.push_mirror_system(ev),
            PushOutcome::DroppedNewest => self.note_push_outcome("dispatch_system", outcome),
            PushOutcome::DroppedOldest { dropped: _ } => {
                self.note_push_outcome("dispatch_system", outcome)?;
                self.push_mirror_system(ev)
            }
        }
    }

    pub fn push_system_best_effort(&mut self, ev: SystemEvent) {
        if self.push_system(ev).is_err() {
            self.metrics.record_queue_overflow();
            self.metrics.record_events_dropped(1);
        }
    }

    pub fn mark_degraded_with_status(
        &mut self,
        message: &str,
        now: TimestampNs,
    ) -> Result<(), EngineError> {
        self.lifecycle = SessionLifecycle::Degraded;
        self.emit_venue_status(message, now)
    }

    /// R6 test/control: emit `VenueStatus{live}` without adapter MarkLive action.
    pub fn mark_live_with_status(&mut self, now: TimestampNs) -> Result<(), EngineError> {
        self.lifecycle = SessionLifecycle::Live;
        self.set_live_signal(true);
        self.emit_venue_status("live", now)
    }

    /// Catalog refresh → `InstrumentUpdate` per instrument + `InstrumentCatalogUpdated` (Spec §25.4).
    pub fn publish_catalog_refresh(
        &mut self,
        catalog: CatalogView,
        now: TimestampNs,
    ) -> Result<(), EngineError> {
        let mut events = Vec::with_capacity(catalog.instruments.len());
        for (i, inst) in catalog.instruments.iter().enumerate() {
            events.push(EventEnvelope {
                schema_version: 1,
                venue: self.cfg.venue,
                instrument: Some(inst.id),
                connection: self.cfg.connection,
                session: self.cfg.session,
                frame_seq: self.inbound_frame_seq,
                event_index: i as u16,
                exchange_ts: None,
                receive_ts: now,
                source_sequence: None,
                flags: EventFlags::empty(),
                payload: MarketEvent::InstrumentUpdate(InstrumentUpdate {
                    status: inst.status,
                }),
            });
        }
        if !events.is_empty() {
            let batch = EventBatch {
                session: self.cfg.session,
                frame_seq: self.inbound_frame_seq,
                events,
            };
            self.metrics
                .events_normalized
                .fetch_add(batch.events.len() as u64, Ordering::Relaxed);
            let outcome = self.dispatch.push_batch(batch.clone())?;
            match outcome {
                PushOutcome::Accepted => {
                    self.metrics.record_batch_dispatched();
                    self.push_mirror_batch(batch)?;
                }
                PushOutcome::DroppedNewest => {
                    self.note_push_outcome("dispatch_batch", outcome)?;
                }
                PushOutcome::DroppedOldest { dropped: _ } => {
                    self.metrics.record_batch_dispatched();
                    self.note_push_outcome("dispatch_batch", outcome)?;
                    self.push_mirror_batch(batch)?;
                }
            }
            self.refresh_queue_gauges();
        }
        self.push_system(SystemEvent::InstrumentCatalogUpdated {
            version: catalog.version.0,
        })
    }

    fn emit_venue_status(&mut self, message: &str, now: TimestampNs) -> Result<(), EngineError> {
        let env = EventEnvelope {
            schema_version: 1,
            venue: self.cfg.venue,
            instrument: None,
            connection: self.cfg.connection,
            session: self.cfg.session,
            frame_seq: self.inbound_frame_seq,
            event_index: 0,
            exchange_ts: None,
            receive_ts: now,
            source_sequence: None,
            flags: EventFlags::empty(),
            payload: MarketEvent::VenueStatus(VenueStatus {
                message: message.to_string(),
            }),
        };
        let batch = EventBatch {
            session: self.cfg.session,
            frame_seq: self.inbound_frame_seq,
            events: vec![env],
        };
        self.metrics
            .events_normalized
            .fetch_add(1, Ordering::Relaxed);
        let outcome = self.dispatch.push_batch(batch.clone())?;
        match outcome {
            PushOutcome::Accepted => {
                self.metrics.record_batch_dispatched();
                self.push_mirror_batch(batch)?;
            }
            PushOutcome::DroppedNewest => {
                self.note_push_outcome("dispatch_batch", outcome)?;
            }
            PushOutcome::DroppedOldest { dropped: _ } => {
                self.metrics.record_batch_dispatched();
                self.note_push_outcome("dispatch_batch", outcome)?;
                self.push_mirror_batch(batch)?;
            }
        }
        self.refresh_queue_gauges();
        Ok(())
    }

    fn note_action_buffer_drops(&mut self) -> Result<(), EngineError> {
        let dropped = self.actions.take_dropped();
        if dropped > 0 {
            // ActionBuffer is always DropNewest; never metrics-only (spec §3.5).
            self.metrics.record_action_buffer_overflow(dropped);
            self.emit_events_dropped(dropped, "ActionBuffer DropNewest")?;
        }
        Ok(())
    }

    fn refresh_queue_gauges(&self) {
        self.metrics.set_queue_gauges(
            self.dispatch.batches().len(),
            self.dispatch.batches().capacity(),
            self.dispatch.systems().len(),
            self.dispatch.systems().capacity(),
        );
    }

    fn note_push_outcome(&mut self, lane: &str, outcome: PushOutcome) -> Result<(), EngineError> {
        match outcome {
            PushOutcome::Accepted => Ok(()),
            PushOutcome::DroppedNewest => {
                self.metrics.record_queue_overflow();
                self.emit_events_dropped(1, &format!("{lane} DropNewest"))
            }
            PushOutcome::DroppedOldest { dropped } => {
                self.metrics.record_queue_overflow();
                self.emit_events_dropped(dropped as u64, &format!("{lane} DropOldest"))
            }
        }
    }

    fn enqueue_pending_write(&mut self, frame: OutboundFrame) -> Result<(), EngineError> {
        match Self::enqueue_bounded(
            &mut self.pending_writes,
            frame,
            MAX_PENDING_WRITES,
            self.cfg.overflow,
        )? {
            PushOutcome::Accepted => Ok(()),
            PushOutcome::DroppedNewest => self.emit_events_dropped(1, "pending_writes DropNewest"),
            PushOutcome::DroppedOldest { dropped } => {
                self.emit_events_dropped(dropped as u64, "pending_writes DropOldest")
            }
        }
    }

    fn enqueue_pending_http(&mut self, spec: HttpRequestSpec) -> Result<(), EngineError> {
        match Self::enqueue_bounded(
            &mut self.pending_http,
            spec,
            MAX_PENDING_HTTP,
            self.cfg.overflow,
        )? {
            PushOutcome::Accepted => Ok(()),
            PushOutcome::DroppedNewest => self.emit_events_dropped(1, "pending_http DropNewest"),
            PushOutcome::DroppedOldest { dropped } => {
                self.emit_events_dropped(dropped as u64, "pending_http DropOldest")
            }
        }
    }

    fn enqueue_bounded<T>(
        buf: &mut Vec<T>,
        item: T,
        capacity: usize,
        policy: OverflowPolicy,
    ) -> Result<PushOutcome, EngineError> {
        if buf.len() < capacity {
            buf.push(item);
            return Ok(PushOutcome::Accepted);
        }
        match policy {
            OverflowPolicy::FailEngine => Err(DispatchError::FailEngine.into()),
            OverflowPolicy::DropNewest => {
                let _ = item;
                Ok(PushOutcome::DroppedNewest)
            }
            OverflowPolicy::DropOldest => {
                let _ = buf.remove(0);
                buf.push(item);
                Ok(PushOutcome::DroppedOldest { dropped: 1 })
            }
            OverflowPolicy::BlockWithDeadline => Err(DispatchError::FailEngine.into()),
            OverflowPolicy::LatestPerKey
            | OverflowPolicy::SpillToDisk
            | OverflowPolicy::DisableSink => Err(DispatchError::UnsupportedPolicy(policy).into()),
        }
    }

    fn mirror_capacity(&self) -> usize {
        if self.cfg.mirror_capacity == 0 {
            0
        } else {
            self.cfg.dispatch_capacity.min(self.cfg.mirror_capacity)
        }
    }

    fn apply_actions(&mut self, now: TimestampNs) -> Result<(), EngineError> {
        self.apply_actions_with_system_policy(now, true)
    }

    fn apply_actions_with_system_policy(
        &mut self,
        now: TimestampNs,
        system_failures_fatal: bool,
    ) -> Result<(), EngineError> {
        let actions: Vec<_> = self.actions.drain().collect();
        for action in actions {
            match action {
                SessionAction::EmitBatch(batch) => {
                    let n = batch.events.len() as u64;
                    self.metrics
                        .events_normalized
                        .fetch_add(n, Ordering::Relaxed);
                    let outcome = self.dispatch.push_batch(batch.clone())?;
                    match outcome {
                        PushOutcome::Accepted => {
                            self.metrics.record_batch_dispatched();
                            self.push_mirror_batch(batch)?;
                        }
                        PushOutcome::DroppedNewest => {
                            self.note_push_outcome("dispatch_batch", outcome)?;
                        }
                        PushOutcome::DroppedOldest { dropped: _ } => {
                            self.metrics.record_batch_dispatched();
                            self.note_push_outcome("dispatch_batch", outcome)?;
                            self.push_mirror_batch(batch)?;
                        }
                    }
                    self.refresh_queue_gauges();
                }
                SessionAction::EmitSystem(ev) => {
                    self.trace_system_event(&ev);
                    self.metrics.observe_system(&ev);
                    let result = match self.dispatch.push_system(ev.clone()) {
                        Ok(PushOutcome::Accepted) => self.push_mirror_system(ev),
                        Ok(PushOutcome::DroppedNewest) => {
                            self.note_push_outcome("dispatch_system", PushOutcome::DroppedNewest)
                        }
                        Ok(PushOutcome::DroppedOldest { dropped }) => self
                            .note_push_outcome(
                                "dispatch_system",
                                PushOutcome::DroppedOldest { dropped },
                            )
                            .and_then(|()| self.push_mirror_system(ev)),
                        Err(error) => Err(error.into()),
                    };
                    self.settle_diagnostic_result(result, system_failures_fatal)?;
                }
                SessionAction::SendText(payload) => {
                    self.record_outbound(RecOpcode::Text, &payload, now)?;
                    self.enqueue_pending_write(OutboundFrame {
                        opcode: FrameOpcode::Text,
                        payload: payload.to_vec(),
                    })?;
                    let result = self.push_mirror_action(SessionAction::SendText(payload));
                    self.settle_diagnostic_result(result, system_failures_fatal)?;
                }
                SessionAction::SendSensitiveText(payload) => {
                    self.enqueue_pending_write(OutboundFrame {
                        opcode: FrameOpcode::Text,
                        payload: payload.into_inner().to_vec(),
                    })?;
                }
                SessionAction::SendBinary(payload) => {
                    self.record_outbound(RecOpcode::Binary, &payload, now)?;
                    self.enqueue_pending_write(OutboundFrame {
                        opcode: FrameOpcode::Binary,
                        payload: payload.to_vec(),
                    })?;
                    let result = self.push_mirror_action(SessionAction::SendBinary(payload));
                    self.settle_diagnostic_result(result, system_failures_fatal)?;
                }
                SessionAction::SendPing(payload) => {
                    self.record_outbound(RecOpcode::Ping, &payload, now)?;
                    self.enqueue_pending_write(OutboundFrame {
                        opcode: FrameOpcode::Ping,
                        payload: payload.to_vec(),
                    })?;
                    let result = self.push_mirror_action(SessionAction::SendPing(payload));
                    self.settle_diagnostic_result(result, system_failures_fatal)?;
                }
                SessionAction::RequestHttp(spec) => {
                    self.enqueue_pending_http(spec.clone())?;
                    let result = self.push_mirror_action(SessionAction::RequestHttp(spec));
                    self.settle_diagnostic_result(result, system_failures_fatal)?;
                }
                SessionAction::ScheduleTimer(spec) => {
                    let replacing = self.timers.contains_key(&spec.timer_id);
                    if !replacing && self.timers.len() >= MAX_SESSION_TIMERS {
                        // ponytail: drop when map full; ceiling = 32 timers/session;
                        // upgrade = hierarchical wheel + explicit eviction policy.
                        tracing::warn!(
                            timer_id = spec.timer_id,
                            max = MAX_SESSION_TIMERS,
                            "dropping ScheduleTimer; session timer map full"
                        );
                        self.emit_events_dropped(1, "schedule_timer map full")?;
                    } else {
                        self.timers.insert(spec.timer_id, spec.fire_at);
                    }
                    self.push_mirror_action(SessionAction::ScheduleTimer(spec))?;
                }
                SessionAction::CancelTimer(timer_id) => {
                    self.timers.remove(&timer_id);
                    self.push_mirror_action(SessionAction::CancelTimer(timer_id))?;
                }
                SessionAction::MarkLive => {
                    self.lifecycle = SessionLifecycle::Live;
                    self.set_live_signal(true);
                    self.push_mirror_action(SessionAction::MarkLive)?;
                    self.emit_venue_status("live", now)?;
                }
                SessionAction::MarkDegraded => {
                    self.lifecycle = SessionLifecycle::Degraded;
                    self.push_mirror_action(SessionAction::MarkDegraded)?;
                    self.emit_venue_status("degraded", now)?;
                }
                SessionAction::Reconnect(_) => {
                    self.reconnect_requested = true;
                    self.lifecycle = SessionLifecycle::Backoff;
                    self.metrics.record_reconnect();
                    self.push_mirror_action(action)?;
                }
                SessionAction::StopSession(_) => {
                    self.stop_requested = true;
                    self.push_mirror_action(action)?;
                }
                other => {
                    let result = self.push_mirror_action(other);
                    self.settle_diagnostic_result(result, system_failures_fatal)?;
                }
            }
        }
        Ok(())
    }

    fn settle_diagnostic_result(
        &self,
        result: Result<(), EngineError>,
        failures_fatal: bool,
    ) -> Result<(), EngineError> {
        match result {
            Ok(()) => Ok(()),
            Err(error) if failures_fatal => Err(error),
            Err(_) => {
                self.metrics.record_queue_overflow();
                self.metrics.record_events_dropped(1);
                Ok(())
            }
        }
    }

    /// Emit `EventsDropped` without recursing if the system queue is also dropping.
    fn emit_events_dropped(&mut self, count: u64, detail: &str) -> Result<(), EngineError> {
        self.metrics.record_events_dropped(count);
        let ev = SystemEvent::EventsDropped {
            count,
            detail: detail.to_string(),
        };
        self.trace_system_event(&ev);
        match self.dispatch.push_system(ev.clone()) {
            Ok(PushOutcome::Accepted) => self.push_mirror_system(ev),
            Ok(PushOutcome::DroppedNewest) => {
                // ponytail: no recursive EventsDropped; metric already recorded original loss.
                self.push_mirror_system(ev)
            }
            Ok(PushOutcome::DroppedOldest { dropped }) => {
                self.metrics.record_events_dropped(dropped as u64);
                self.push_mirror_system(ev)
            }
            Err(e) => Err(e.into()),
        }
    }

    fn push_mirror_batch(&mut self, batch: EventBatch) -> Result<(), EngineError> {
        let cap = self.mirror_capacity();
        match Self::try_push_mirror(&mut self.market_batches, cap, self.cfg.overflow, batch)? {
            MirrorPush::Ok => Ok(()),
            MirrorPush::Dropped { detail } => self.emit_events_dropped(1, &detail),
        }
    }

    fn push_mirror_system(&mut self, ev: SystemEvent) -> Result<(), EngineError> {
        let cap = self.mirror_capacity();
        match Self::try_push_mirror(&mut self.system_events, cap, self.cfg.overflow, ev)? {
            MirrorPush::Ok => Ok(()),
            // Avoid EventsDropped recursion on the system mirror itself.
            MirrorPush::Dropped { .. } => {
                self.metrics.record_events_dropped(1);
                Ok(())
            }
        }
    }

    fn trace_system_event(&self, event: &SystemEvent) {
        match event {
            SystemEvent::ParseError { .. }
            | SystemEvent::UnknownMessage { .. }
            | SystemEvent::SequenceGap { .. }
            | SystemEvent::ChecksumMismatch { .. }
            | SystemEvent::BookInvalidated { .. }
            | SystemEvent::HeartbeatMissed
            | SystemEvent::RateLimited
            | SystemEvent::EventsDropped { .. }
            | SystemEvent::DiskPressure => {
                tracing::warn!(
                    venue = self.cfg.venue.0,
                    session = self.cfg.session.0,
                    ?event,
                    "session system event"
                );
            }
            SystemEvent::BookSnapshotRejected { .. } => {
                tracing::debug!(
                    venue = self.cfg.venue.0,
                    session = self.cfg.session.0,
                    ?event,
                    "session replacement snapshot rejected"
                );
            }
            _ => {}
        }
    }

    fn push_mirror_action(&mut self, action: SessionAction) -> Result<(), EngineError> {
        let cap = self.mirror_capacity();
        match Self::try_push_mirror(&mut self.other_actions, cap, self.cfg.overflow, action)? {
            MirrorPush::Ok => Ok(()),
            MirrorPush::Dropped { .. } => {
                self.metrics.record_events_dropped(1);
                Ok(())
            }
        }
    }

    fn try_push_mirror<T>(
        vec: &mut Vec<T>,
        capacity: usize,
        policy: OverflowPolicy,
        item: T,
    ) -> Result<MirrorPush, EngineError> {
        if capacity == 0 {
            return Ok(MirrorPush::Ok);
        }
        if vec.len() < capacity {
            vec.push(item);
            return Ok(MirrorPush::Ok);
        }
        match policy {
            OverflowPolicy::FailEngine => Err(EngineError::Dispatch(
                marketfeed_dispatch::DispatchError::FailEngine,
            )),
            OverflowPolicy::DropNewest => Ok(MirrorPush::Dropped {
                detail: "mirror DropNewest".into(),
            }),
            OverflowPolicy::DropOldest => {
                vec.remove(0);
                vec.push(item);
                Ok(MirrorPush::Dropped {
                    detail: "mirror DropOldest".into(),
                })
            }
            other => Err(EngineError::Internal(format!(
                "mirror overflow unsupported: {other:?}"
            ))),
        }
    }

    fn set_live_signal(&self, live: bool) {
        if let Some(flag) = &self.cfg.live_signal {
            flag.store(live, Ordering::Relaxed);
        }
    }

    fn record_outbound(
        &mut self,
        opcode: RecOpcode,
        payload: &Bytes,
        now: TimestampNs,
    ) -> Result<(), EngineError> {
        // ponytail: outbound shares seq counter with direction bit; upgrade = separate outbound_seq.
        self.record_frame(
            self.inbound_frame_seq,
            now.0,
            now.0 as u64,
            Direction::Outbound,
            opcode,
            payload,
        )
    }

    fn record_subscription_command(
        &mut self,
        command: &SessionCommand,
        wire: &SubscriptionWireAction,
        now: TimestampNs,
    ) -> Result<(), EngineError> {
        if self.recording_pipeline.is_none() && self.recorder.is_none() {
            return Ok(());
        }
        let payload = encode_subscription_command(command, wire)?;
        self.record_frame(
            self.inbound_frame_seq,
            now.0,
            u64::try_from(now.0).unwrap_or_default(),
            Direction::Inbound,
            RecOpcode::SubscriptionCommand,
            &payload,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use marketfeed_adapter_api::{
        ActionBuffer, AdapterError, EventBatch, SessionAction, SessionCommand, SessionInput,
        SessionMachine, SubscriptionWireAction,
    };
    use marketfeed_model::{FrameStamp, OverflowPolicy, SessionId, SystemEvent, TimestampNs};
    use marketfeed_recording::{
        FrameOpcode as RecordedOpcode, RawSegmentReader, decode_subscription_command,
    };

    use super::*;

    struct EmitN {
        n: u64,
    }

    impl SessionMachine for EmitN {
        fn on_input(
            &mut self,
            input: SessionInput<'_>,
            output: &mut ActionBuffer,
        ) -> Result<(), AdapterError> {
            if let SessionInput::TextFrame { .. } = input {
                for i in 0..self.n {
                    output.push(SessionAction::EmitBatch(EventBatch {
                        session: SessionId(1),
                        frame_seq: i,
                        events: Vec::new(),
                    }));
                }
            }
            Ok(())
        }
    }

    struct EmitSystem {
        ev: SystemEvent,
    }

    struct DynamicSubscriptionMachine;

    impl SessionMachine for EmitSystem {
        fn on_input(
            &mut self,
            input: SessionInput<'_>,
            output: &mut ActionBuffer,
        ) -> Result<(), AdapterError> {
            if matches!(input, SessionInput::TextFrame { .. }) {
                output.push(SessionAction::EmitSystem(self.ev.clone()));
            }
            Ok(())
        }
    }

    impl SessionMachine for DynamicSubscriptionMachine {
        fn on_input(
            &mut self,
            _input: SessionInput<'_>,
            _output: &mut ActionBuffer,
        ) -> Result<(), AdapterError> {
            Ok(())
        }

        fn prepare_dynamic_subscription(
            &self,
            command: &SessionCommand,
        ) -> Result<SubscriptionWireAction, AdapterError> {
            match command {
                SessionCommand::Subscribe(_) => Ok(SubscriptionWireAction::Text(
                    Bytes::from_static(b"wire-subscribe"),
                )),
                _ => Err(AdapterError::UnsupportedCapability("test command".into())),
            }
        }
    }

    fn stamp() -> FrameStamp {
        FrameStamp {
            receive_ts: TimestampNs(1),
            mono_ns: 1,
        }
    }

    #[test]
    fn drop_oldest_emits_events_dropped() {
        let mut runner = SessionRunner::new(
            Box::new(EmitN { n: 3 }),
            SessionRunnerConfig {
                session: SessionId(1),
                dispatch_capacity: 1,
                overflow: OverflowPolicy::DropOldest,
                mirror_capacity: 8,
                record: false,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

        let mut bytes = b"x".to_vec();
        runner.on_text_frame(&mut bytes, stamp()).unwrap();

        assert!(
            runner
                .system_events
                .iter()
                .any(|e| matches!(e, SystemEvent::EventsDropped { .. }))
        );
        assert!(runner.metrics.events_dropped.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn mirrors_bounded_to_dispatch_capacity() {
        let cap = 2;
        let mut runner = SessionRunner::new(
            Box::new(EmitN { n: 5 }),
            SessionRunnerConfig {
                session: SessionId(1),
                dispatch_capacity: cap,
                overflow: OverflowPolicy::DropOldest,
                mirror_capacity: cap,
                record: false,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

        let mut bytes = b"x".to_vec();
        runner.on_text_frame(&mut bytes, stamp()).unwrap();
        assert!(runner.market_batches.len() <= cap);
    }

    #[test]
    fn system_events_update_metrics() {
        let mut runner = SessionRunner::new(
            Box::new(EmitSystem {
                ev: SystemEvent::ChecksumMismatch {
                    detail: "crc".into(),
                },
            }),
            SessionRunnerConfig {
                session: SessionId(1),
                dispatch_capacity: 8,
                record: false,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

        let mut bytes = b"x".to_vec();
        runner.on_text_frame(&mut bytes, stamp()).unwrap();
        assert_eq!(
            runner.metrics.checksum_mismatches.load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn raw_recording_includes_ping_and_close_control_frames() {
        let mut runner = SessionRunner::new(
            Box::new(EmitN { n: 0 }),
            SessionRunnerConfig {
                session: SessionId(1),
                record: true,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

        runner.on_ping_frame(b"ping", stamp()).unwrap();
        runner.on_close_frame(b"close", stamp()).unwrap();
        let bytes = runner.take_recording().unwrap();
        let mut reader = RawSegmentReader::from_bytes(bytes).unwrap();
        let records = reader.read_all().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].header.opcode, RecordedOpcode::Ping);
        assert_eq!(records[0].payload, b"ping");
        assert_eq!(records[1].header.opcode, RecordedOpcode::Close);
        assert_eq!(records[1].payload, b"close");
    }

    #[test]
    fn accepted_dynamic_subscription_record_includes_exact_wire_frame() {
        let mut runner = SessionRunner::new(
            Box::new(DynamicSubscriptionMachine),
            SessionRunnerConfig {
                session: SessionId(1),
                record: true,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();
        let command = SessionCommand::Subscribe(vec!["BTC-USD".into()]);

        runner
            .deliver_control(command.clone(), TimestampNs(42))
            .unwrap();
        let bytes = runner.take_recording().unwrap();
        let records = RawSegmentReader::from_bytes(bytes)
            .unwrap()
            .read_all()
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].header.direction, Direction::Inbound);
        assert_eq!(
            records[0].header.opcode,
            RecordedOpcode::SubscriptionCommand
        );
        assert_eq!(
            decode_subscription_command(&records[0].payload).unwrap(),
            (
                command,
                SubscriptionWireAction::Text(Bytes::from_static(b"wire-subscribe"))
            )
        );
    }

    #[test]
    fn recording_bounds_do_not_restrict_unrecorded_subscription_commands() {
        let mut runner = SessionRunner::new(
            Box::new(DynamicSubscriptionMachine),
            SessionRunnerConfig {
                session: SessionId(1),
                record: false,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

        runner
            .deliver_control(
                SessionCommand::Subscribe(vec!["x".repeat(1025)]),
                TimestampNs(42),
            )
            .unwrap();
        assert_eq!(runner.take_pending_writes().len(), 1);
    }

    #[test]
    fn checksum_and_queue_gauges_in_prometheus() {
        let metrics = EngineMetrics::new();
        metrics.set_queue_gauges(3, 8, 1, 8);
        metrics.observe_system(&SystemEvent::ChecksumMismatch { detail: "x".into() });
        let text = metrics.prometheus_text();
        assert!(text.contains("marketfeed_checksum_mismatches_total 1"));
        assert!(text.contains("marketfeed_batch_queue_capacity 8"));
        assert!(text.contains("marketfeed_system_queue_capacity 8"));
    }
}
