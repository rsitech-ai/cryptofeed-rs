//! Minimal engine supervisor: owns sessions, rejects work when stopped.

use std::collections::HashMap;

use marketfeed_adapter_api::{ReconnectPolicy, SessionMachine};
use marketfeed_model::{PlanVersion, SessionId};
use marketfeed_sinks::EventSink;
use marketfeed_transport::{
    FrameBuffer, FrameOpcode, HttpTransport, MemoryWebSocket, StubHttpTransport, WebSocketSpec,
    WebSocketTransport,
};

use crate::control::{DesiredMap, PausedSet, RecordingRotateHandle, RollingMap};
use crate::live::{run_session_with_reconnect, run_session_with_reconnect_to};
use crate::{EngineError, EngineLifecycle, SessionRunner, SessionRunnerConfig};
use std::sync::Arc;

/// Top-level owner for session runners + control-plane plan versioning.
pub struct EngineSupervisor {
    pub lifecycle: EngineLifecycle,
    pub(crate) sessions: HashMap<SessionId, SessionRunner>,
    pub(crate) plan_version: PlanVersion,
    pub(crate) desired_symbols: DesiredMap,
    pub(crate) paused_venues: PausedSet,
    pub(crate) rolling: RollingMap,
    /// Optional §19.2 recording rotate hook (daemon wires this).
    pub(crate) recording_rotate: Option<Arc<RecordingRotateHandle>>,
}

impl EngineSupervisor {
    pub fn new() -> Self {
        Self {
            lifecycle: EngineLifecycle::Starting,
            sessions: HashMap::new(),
            plan_version: PlanVersion(0),
            desired_symbols: DesiredMap::new(),
            paused_venues: PausedSet::new(),
            rolling: RollingMap::new(),
            recording_rotate: None,
        }
    }

    pub fn set_recording_rotate(&mut self, handle: Arc<RecordingRotateHandle>) {
        self.recording_rotate = Some(handle);
    }

    pub fn mark_running(&mut self) {
        self.lifecycle = EngineLifecycle::Running;
        tracing::info!(lifecycle = "running", "engine supervisor running");
    }

    pub fn insert_session(
        &mut self,
        machine: Box<dyn SessionMachine>,
        cfg: SessionRunnerConfig,
    ) -> Result<SessionId, EngineError> {
        if self.lifecycle == EngineLifecycle::Stopped || self.lifecycle == EngineLifecycle::Draining
        {
            return Err(EngineError::Stopped);
        }
        let id = cfg.session;
        let runner = SessionRunner::new(machine, cfg)?;
        self.sessions.insert(id, runner);
        Ok(id)
    }

    pub fn session_mut(&mut self, id: SessionId) -> Result<&mut SessionRunner, EngineError> {
        self.sessions
            .get_mut(&id)
            .ok_or(EngineError::SessionNotFound)
    }

    pub fn session(&self, id: SessionId) -> Result<&SessionRunner, EngineError> {
        self.sessions.get(&id).ok_or(EngineError::SessionNotFound)
    }

    /// Query a session machine book (§19.2). Missing/empty → `None`.
    pub fn book_snapshot(
        &self,
        session: SessionId,
        instrument: marketfeed_model::InstrumentId,
        depth: Option<u32>,
    ) -> Result<Option<marketfeed_model::BookSnapshot>, EngineError> {
        Ok(self.session(session)?.book_snapshot(instrument, depth))
    }

    pub fn begin_shutdown(&mut self) -> Result<(), EngineError> {
        self.lifecycle = EngineLifecycle::Draining;
        for runner in self.sessions.values_mut() {
            runner.request_stop();
            runner.push_system(marketfeed_model::SystemEvent::ShutdownStarted)?;
        }
        Ok(())
    }

    /// Drain all session dispatch queues; returns (batches, systems) drained.
    pub fn drain_all_dispatch(&mut self) -> (usize, usize) {
        let mut batches = 0;
        let mut systems = 0;
        for runner in self.sessions.values_mut() {
            let (b, s) = runner.drain_dispatch();
            batches += b;
            systems += s;
        }
        (batches, systems)
    }

    pub fn finish_shutdown(&mut self) {
        self.finish_shutdown_to(None::<&mut dyn EventSink>)
            .expect("null sink shutdown drain cannot fail");
    }

    /// Emit `ShutdownCompleted`, forward it after all prior dispatch, then
    /// release session state. Callers with configured sinks must use this
    /// variant so completion cannot be cleared before delivery.
    pub fn finish_shutdown_to<S: EventSink + ?Sized>(
        &mut self,
        mut sink: Option<&mut S>,
    ) -> Result<(), EngineError> {
        for runner in self.sessions.values_mut() {
            runner.push_system(marketfeed_model::SystemEvent::ShutdownCompleted)?;
            runner.consume_dispatch(sink.as_deref_mut())?;
        }
        self.sessions.clear();
        self.desired_symbols.clear();
        self.paused_venues.clear();
        self.rolling.clear();
        self.lifecycle = EngineLifecycle::Stopped;
        Ok(())
    }

    /// Drain a memory websocket into a session runner (one-shot offline/live stub loop).
    pub async fn drain_memory_ws(
        &mut self,
        session: SessionId,
        ws: &mut MemoryWebSocket,
        spec: &WebSocketSpec,
        start_ts_ns: i64,
    ) -> Result<(), EngineError> {
        if self.lifecycle != EngineLifecycle::Running {
            return Err(EngineError::Stopped);
        }
        ws.connect(spec).await?;
        let runner = self.session_mut(session)?;
        runner.on_connected(marketfeed_model::TimestampNs(start_ts_ns))?;

        let mut buf = FrameBuffer::default();
        let mut ts = start_ts_ns;
        loop {
            match ws.read_frame(&mut buf).await {
                Ok(frame) => {
                    ts += 1;
                    let stamp = marketfeed_model::FrameStamp {
                        receive_ts: marketfeed_model::TimestampNs(ts),
                        mono_ns: ts as u64,
                    };
                    match frame.opcode {
                        FrameOpcode::Text => {
                            let mut payload = frame.payload;
                            runner.on_text_frame(&mut payload, stamp)?;
                        }
                        FrameOpcode::Binary
                        | FrameOpcode::Ping
                        | FrameOpcode::Pong
                        | FrameOpcode::Close => {}
                    }
                    for write in runner.take_pending_writes() {
                        ws.write_frame(write).await?;
                    }
                    if runner.reconnect_requested || runner.stop_requested {
                        break;
                    }
                }
                Err(marketfeed_transport::TransportError::Closed) => break,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// Run session with reconnect/backoff against any transport (memory or tungstenite).
    pub async fn run_session_loop<T: WebSocketTransport, H: HttpTransport>(
        &mut self,
        session: SessionId,
        transport: &mut T,
        http: &H,
        spec: &WebSocketSpec,
        policy: ReconnectPolicy,
        max_reconnects: u32,
    ) -> Result<(), EngineError> {
        if self.lifecycle != EngineLifecycle::Running {
            return Err(EngineError::Stopped);
        }
        let runner = self.session_mut(session)?;
        run_session_with_reconnect(runner, transport, http, spec, policy, max_reconnects).await
    }

    /// Like [`Self::run_session_loop`], forwarding dispatch into `sink` (or null-drain when `None`).
    pub async fn run_session_loop_to<T, H, S>(
        &mut self,
        session: SessionId,
        transport: &mut T,
        http: &H,
        spec: &WebSocketSpec,
        policy: ReconnectPolicy,
        max_reconnects: u32,
        sink: Option<&mut S>,
    ) -> Result<(), EngineError>
    where
        T: WebSocketTransport,
        H: HttpTransport,
        S: EventSink + ?Sized,
    {
        if self.lifecycle != EngineLifecycle::Running {
            return Err(EngineError::Stopped);
        }
        let runner = self.session_mut(session)?;
        run_session_with_reconnect_to(runner, transport, http, spec, policy, max_reconnects, sink)
            .await
    }

    /// Convenience: run with stub HTTP (WS-only venues / tests).
    pub async fn run_session_loop_ws_only<T: WebSocketTransport>(
        &mut self,
        session: SessionId,
        transport: &mut T,
        spec: &WebSocketSpec,
        policy: ReconnectPolicy,
        max_reconnects: u32,
    ) -> Result<(), EngineError> {
        self.run_session_loop(
            session,
            transport,
            &StubHttpTransport,
            spec,
            policy,
            max_reconnects,
        )
        .await
    }
}

impl Default for EngineSupervisor {
    fn default() -> Self {
        Self::new()
    }
}
