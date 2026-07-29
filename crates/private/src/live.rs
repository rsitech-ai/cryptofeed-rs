//! Drive [`PrivateSessionMachine`] over engine transports (C6c library path).
//!
//! Enabled with feature `live`. Credentials load only at this private runtime boundary
//! (env vars — never TOML secrets). Account events are drained through an
//! [`AccountEventSink`] every pump step. Fixed-duration smoke wrappers use the
//! explicit drop-all [`NullAccountSink`].
//!
//! Venues:
//! - Binance Spot: blocked pending authenticated WebSocket API migration
//! - OKX / Bybit Spot: private WS + non-recorded HMAC auth (no order placement)

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use marketfeed_adapter_api::{
    DisconnectReason, HttpMethod as AdapterMethod, HttpRequestSpec,
    HttpResponse as AdapterHttpResponse, SessionAction, SessionInput,
};
use marketfeed_model::{FrameStamp, SystemEvent, TimestampNs};
use marketfeed_transport::{
    FrameBuffer, FrameOpcode, HttpMethod, HttpRequest, HttpTransport, OutboundFrame, WebSocketSpec,
    WebSocketTransport,
};
use tokio::time::timeout;

use crate::account::{AccountEvent, AccountEventSink};
#[cfg(test)]
use crate::binance_spot::BinanceSpotUserDataConfig;
use crate::binance_spot::BinanceSpotUserDataSession;
use crate::bybit::{BybitPrivateConfig, BybitPrivateSession};
use crate::credentials::{BybitApiCredentials, OkxApiCredentials, sign};
use crate::error::PrivateError;
use crate::okx::{OkxPrivateConfig, OkxPrivateSession};
use crate::session::{PrivateActionBuffer, PrivateSessionAction, PrivateSessionMachine};

const DEFAULT_PRIVATE_PUMP_CAPACITY: usize = 1024;

fn now_ns() -> TimestampNs {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    TimestampNs(dur.as_nanos() as i64)
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn mono_ns() -> u64 {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

/// Stats from a private live session.
#[derive(Debug, Default, Clone)]
pub struct PrivateLiveStats {
    pub marked_live: bool,
    pub text_frames: u64,
    pub account_events: u64,
    pub system_events: u64,
    pub http_requests: u64,
    pub ws_writes: u64,
}

/// Explicit drop-all sink for fixed-duration library smoke wrappers.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullAccountSink;

impl AccountEventSink for NullAccountSink {
    fn push_account(&mut self, _event: AccountEvent) -> Result<(), PrivateError> {
        Ok(())
    }

    fn push_system(&mut self, _event: SystemEvent) -> Result<(), PrivateError> {
        Ok(())
    }
}

/// Bounded pump: drains SM actions into pending HTTP / WS writes / timers / account sink.
#[derive(Debug)]
struct PrivatePump {
    pending_http: Vec<HttpRequestSpec>,
    pending_ws: Vec<OutboundFrame>,
    timers: HashMap<u64, TimestampNs>,
    capacity: usize,
    account_events: u64,
    system_events: u64,
    marked_live: bool,
    reconnect: bool,
    stop: bool,
}

impl PrivatePump {
    fn new() -> Self {
        Self::with_capacity(DEFAULT_PRIVATE_PUMP_CAPACITY)
    }

    fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            pending_http: Vec::with_capacity(capacity.min(64)),
            pending_ws: Vec::with_capacity(capacity.min(64)),
            timers: HashMap::with_capacity(capacity.min(64)),
            capacity,
            account_events: 0,
            system_events: 0,
            marked_live: false,
            reconnect: false,
            stop: false,
        }
    }

    fn ingest(
        &mut self,
        buf: &mut PrivateActionBuffer,
        sink: &mut impl AccountEventSink,
    ) -> Result<(), PrivateError> {
        for action in buf.drain() {
            match action {
                PrivateSessionAction::Account(ev) => {
                    // Drain immediately — never leave events buffered unbounded.
                    sink.push_account(ev)?;
                    self.account_events = self.account_events.saturating_add(1);
                }
                PrivateSessionAction::Session(a) => match a {
                    SessionAction::RequestHttp(spec) => {
                        if self.pending_http.len() >= self.capacity {
                            return Err(PrivateError::Protocol(format!(
                                "private HTTP request capacity {} exceeded",
                                self.capacity
                            )));
                        }
                        self.pending_http.push(spec);
                    }
                    SessionAction::SendText(payload) => {
                        self.push_ws(OutboundFrame {
                            opcode: FrameOpcode::Text,
                            payload: payload.to_vec(),
                        })?;
                    }
                    SessionAction::SendSensitiveText(payload) => {
                        self.push_ws(OutboundFrame {
                            opcode: FrameOpcode::Text,
                            payload: payload.into_inner().to_vec(),
                        })?;
                    }
                    SessionAction::SendBinary(payload) => {
                        self.push_ws(OutboundFrame {
                            opcode: FrameOpcode::Binary,
                            payload: payload.to_vec(),
                        })?;
                    }
                    SessionAction::SendPing(payload) => {
                        self.push_ws(OutboundFrame {
                            opcode: FrameOpcode::Ping,
                            payload: payload.to_vec(),
                        })?;
                    }
                    SessionAction::ScheduleTimer(spec) => {
                        if !self.timers.contains_key(&spec.timer_id)
                            && self.timers.len() >= self.capacity
                        {
                            return Err(PrivateError::Protocol(format!(
                                "private timer capacity {} exceeded",
                                self.capacity
                            )));
                        }
                        self.timers.insert(spec.timer_id, spec.fire_at);
                    }
                    SessionAction::CancelTimer(id) => {
                        self.timers.remove(&id);
                    }
                    SessionAction::EmitSystem(event) => {
                        sink.push_system(event)?;
                        self.system_events = self.system_events.saturating_add(1);
                    }
                    SessionAction::MarkLive => self.marked_live = true,
                    SessionAction::Reconnect(_) => self.reconnect = true,
                    SessionAction::StopSession(_) => self.stop = true,
                    SessionAction::EmitBatch(_)
                    | SessionAction::MarkDegraded
                    | SessionAction::ResyncInstrument(_)
                    | SessionAction::DisableSubscription(_) => {
                        return Err(PrivateError::Protocol(
                            "unsupported public-market action from private session".into(),
                        ));
                    }
                },
            }
        }
        Ok(())
    }

    fn push_ws(&mut self, frame: OutboundFrame) -> Result<(), PrivateError> {
        if self.pending_ws.len() >= self.capacity {
            return Err(PrivateError::Protocol(format!(
                "private WebSocket write capacity {} exceeded",
                self.capacity
            )));
        }
        self.pending_ws.push(frame);
        Ok(())
    }

    fn drive(
        &mut self,
        session: &mut impl PrivateSessionMachine,
        input: SessionInput<'_>,
        sink: &mut impl AccountEventSink,
    ) -> Result<(), PrivateError> {
        let mut buf = PrivateActionBuffer::new();
        session.on_input(input, &mut buf)?;
        let dropped = buf.take_dropped();
        if dropped != 0 {
            return Err(PrivateError::Protocol(format!(
                "private action buffer overflow: dropped {dropped} action(s)"
            )));
        }
        self.ingest(&mut buf, sink)
    }

    async fn flush_http<H: HttpTransport>(
        &mut self,
        session: &mut impl PrivateSessionMachine,
        http: &H,
        sink: &mut impl AccountEventSink,
    ) -> Result<u64, PrivateError> {
        let mut n = 0u64;
        loop {
            if self.pending_http.is_empty() {
                break;
            }
            let specs = std::mem::take(&mut self.pending_http);
            for spec in specs {
                n += 1;
                let req = HttpRequest {
                    method: match spec.method {
                        AdapterMethod::Get => HttpMethod::Get,
                        AdapterMethod::Post => HttpMethod::Post,
                        AdapterMethod::Put => HttpMethod::Put,
                        AdapterMethod::Delete => HttpMethod::Delete,
                    },
                    url: spec.url,
                    // Headers may carry X-MBX-APIKEY — never log this request.
                    headers: spec.headers,
                    body: spec.body,
                    timeout_ms: 10_000,
                    max_body_bytes: 16 * 1024 * 1024,
                };
                let resp = http
                    .request(req)
                    .await
                    .map_err(|e| PrivateError::Transport(e.to_string()))?;
                let adapter_resp = AdapterHttpResponse {
                    status: resp.status,
                    headers: resp.headers,
                    body: resp.body,
                };
                self.drive(
                    session,
                    SessionInput::HttpResponse {
                        request_id: spec.id,
                        response: &adapter_resp,
                        received: FrameStamp {
                            receive_ts: now_ns(),
                            mono_ns: mono_ns(),
                        },
                    },
                    sink,
                )?;
            }
        }
        Ok(n)
    }

    async fn flush_ws_writes<T: WebSocketTransport>(
        &mut self,
        ws: &mut T,
    ) -> Result<u64, PrivateError> {
        let frames = std::mem::take(&mut self.pending_ws);
        let n = frames.len() as u64;
        for frame in frames {
            // Auth/login bodies contain secrets — never log payload.
            ws.write_frame(frame)
                .await
                .map_err(|e| PrivateError::Transport(e.to_string()))?;
        }
        Ok(n)
    }

    fn poll_timers(
        &mut self,
        session: &mut impl PrivateSessionMachine,
        now: TimestampNs,
        sink: &mut impl AccountEventSink,
    ) -> Result<(), PrivateError> {
        let due: Vec<u64> = self
            .timers
            .iter()
            .filter(|(_, fire)| fire.0 <= now.0)
            .map(|(id, _)| *id)
            .collect();
        for timer_id in due {
            self.timers.remove(&timer_id);
            self.drive(session, SessionInput::Timer { timer_id, now }, sink)?;
        }
        Ok(())
    }
}

/// Binance private streaming is blocked until the authenticated WebSocket API
/// subscription flow replaces the retired listen-key protocol.
pub fn binance_spot_session_from_env() -> Result<BinanceSpotUserDataSession, PrivateError> {
    Err(PrivateError::NotImplemented)
}

/// Create an OKX private session with a freshly signed login payload from env.
pub fn okx_session_from_env() -> Result<OkxPrivateSession, PrivateError> {
    let creds = OkxApiCredentials::from_env()?;
    let login_payload = sign::okx_login_payload(&creds, now_secs());
    Ok(OkxPrivateSession::new(OkxPrivateConfig {
        login_payload,
        ..OkxPrivateConfig::default()
    }))
}

/// Create a Bybit private session with a freshly signed auth payload from env.
pub fn bybit_session_from_env() -> Result<BybitPrivateSession, PrivateError> {
    let creds = BybitApiCredentials::from_env()?;
    // Docs: expires must be > now; use +10s margin.
    let expires_ms = now_ms().saturating_add(10_000);
    let auth_payload = sign::bybit_auth_payload(&creds, expires_ms);
    Ok(BybitPrivateSession::new(BybitPrivateConfig {
        auth_payload,
        ..BybitPrivateConfig::default()
    }))
}

/// Fail closed before transport I/O while Binance private streaming awaits
/// migration to the authenticated WebSocket API subscription protocol.
pub async fn run_binance_spot_user_data_live_until<H, T, S, F>(
    _session: &mut BinanceSpotUserDataSession,
    _http: &H,
    _ws: &mut T,
    _sink: &mut S,
    _should_stop: F,
) -> Result<PrivateLiveStats, PrivateError>
where
    H: HttpTransport,
    T: WebSocketTransport,
    S: AccountEventSink,
    F: FnMut() -> bool,
{
    Err(PrivateError::NotImplemented)
}

/// Compatibility wrapper that fails closed before transport I/O.
///
/// Binance private streaming remains blocked until the authenticated WebSocket
/// API subscription protocol is implemented.
pub async fn run_binance_spot_user_data_live<H, T>(
    session: &mut BinanceSpotUserDataSession,
    http: &H,
    ws: &mut T,
    duration: Duration,
) -> Result<PrivateLiveStats, PrivateError>
where
    H: HttpTransport,
    T: WebSocketTransport,
{
    let deadline = Instant::now() + duration;
    let mut sink = NullAccountSink;
    run_binance_spot_user_data_live_until(session, http, ws, &mut sink, || {
        Instant::now() >= deadline
    })
    .await
}

/// Connect OKX private WS, send a non-recorded HMAC login, idle-read until `should_stop`.
///
/// Success criterion: login → `MarkLive`. Account events optional on an idle account.
pub async fn run_okx_private_live_until<T, S, F>(
    session: &mut OkxPrivateSession,
    ws: &mut T,
    sink: &mut S,
    mut should_stop: F,
) -> Result<PrivateLiveStats, PrivateError>
where
    T: WebSocketTransport,
    S: AccountEventSink,
    F: FnMut() -> bool,
{
    let url = session.ws_url().to_string();
    run_private_ws_auth_until(session, &url, ws, sink, &mut should_stop, "okx").await
}

/// Fixed-duration OKX private live smoke (null-drain).
pub async fn run_okx_private_live<T>(
    session: &mut OkxPrivateSession,
    ws: &mut T,
    duration: Duration,
) -> Result<PrivateLiveStats, PrivateError>
where
    T: WebSocketTransport,
{
    let deadline = Instant::now() + duration;
    let mut sink = NullAccountSink;
    run_okx_private_live_until(session, ws, &mut sink, || Instant::now() >= deadline).await
}

/// Connect Bybit private WS, send non-recorded HMAC auth, idle-read until `should_stop`.
pub async fn run_bybit_private_live_until<T, S, F>(
    session: &mut BybitPrivateSession,
    ws: &mut T,
    sink: &mut S,
    mut should_stop: F,
) -> Result<PrivateLiveStats, PrivateError>
where
    T: WebSocketTransport,
    S: AccountEventSink,
    F: FnMut() -> bool,
{
    let url = session.ws_url().to_string();
    run_private_ws_auth_until(session, &url, ws, sink, &mut should_stop, "bybit").await
}

/// Fixed-duration Bybit private live smoke (null-drain).
pub async fn run_bybit_private_live<T>(
    session: &mut BybitPrivateSession,
    ws: &mut T,
    duration: Duration,
) -> Result<PrivateLiveStats, PrivateError>
where
    T: WebSocketTransport,
{
    let deadline = Instant::now() + duration;
    let mut sink = NullAccountSink;
    run_bybit_private_live_until(session, ws, &mut sink, || Instant::now() >= deadline).await
}

async fn run_private_ws_auth_until<M, T, S, F>(
    session: &mut M,
    url: &str,
    ws: &mut T,
    sink: &mut S,
    should_stop: &mut F,
    venue: &'static str,
) -> Result<PrivateLiveStats, PrivateError>
where
    M: PrivateSessionMachine,
    T: WebSocketTransport,
    S: AccountEventSink,
    F: FnMut() -> bool,
{
    let mut pump = PrivatePump::new();
    let mut stats = PrivateLiveStats::default();
    let http = marketfeed_transport::StubHttpTransport;

    let spec = WebSocketSpec {
        url: url.to_string(),
        max_frame_bytes: 4 * 1024 * 1024,
        ..WebSocketSpec::default()
    };
    ws.connect(&spec)
        .await
        .map_err(|e| PrivateError::Transport(e.to_string()))?;

    // Connected → sensitive auth/login text; flush before reading.
    pump.drive(session, SessionInput::Connected { now: now_ns() }, sink)?;
    stats.ws_writes += pump.flush_ws_writes(ws).await?;

    let idle = pump_ws_idle(session, &http, ws, &mut pump, sink, should_stop).await?;
    stats = merge_ws_idle(stats, idle);

    if !stats.marked_live {
        return Err(PrivateError::Protocol(format!(
            "{venue} private auth did not MarkLive"
        )));
    }
    Ok(stats)
}

async fn pump_ws_idle<M, H, T, S, F>(
    session: &mut M,
    http: &H,
    ws: &mut T,
    pump: &mut PrivatePump,
    sink: &mut S,
    should_stop: &mut F,
) -> Result<PrivateLiveStats, PrivateError>
where
    M: PrivateSessionMachine,
    H: HttpTransport,
    T: WebSocketTransport,
    S: AccountEventSink,
    F: FnMut() -> bool,
{
    let mut stats = PrivateLiveStats::default();
    let mut buf = FrameBuffer::default();
    while !should_stop() && !pump.stop && !pump.reconnect {
        let now = now_ns();
        pump.poll_timers(session, now, sink)?;
        stats.http_requests += pump.flush_http(session, http, sink).await?;
        stats.ws_writes += pump.flush_ws_writes(ws).await?;

        let wait = Duration::from_millis(250);
        match timeout(wait, ws.read_frame(&mut buf)).await {
            Ok(Ok(frame)) => {
                let receive_ts = now_ns();
                let stamp = FrameStamp {
                    receive_ts,
                    mono_ns: mono_ns(),
                };
                match frame.opcode {
                    FrameOpcode::Text => {
                        stats.text_frames += 1;
                        let mut payload = frame.payload;
                        pump.drive(
                            session,
                            SessionInput::TextFrame {
                                bytes: &mut payload,
                                received: stamp,
                            },
                            sink,
                        )?;
                    }
                    FrameOpcode::Binary => {
                        let mut payload = frame.payload;
                        pump.drive(
                            session,
                            SessionInput::BinaryFrame {
                                bytes: &mut payload,
                                received: stamp,
                            },
                            sink,
                        )?;
                    }
                    FrameOpcode::Close => {
                        pump.drive(
                            session,
                            SessionInput::Disconnected {
                                reason: DisconnectReason::RemoteClose,
                                now: now_ns(),
                            },
                            sink,
                        )?;
                        return Err(PrivateError::Transport(
                            "private websocket closed by peer".into(),
                        ));
                    }
                    FrameOpcode::Ping | FrameOpcode::Pong => {}
                }
                // Auth success may queue subscribe SendText + MarkLive; keepalive may queue HTTP.
                stats.http_requests += pump.flush_http(session, http, sink).await?;
                stats.ws_writes += pump.flush_ws_writes(ws).await?;
                if pump.marked_live {
                    stats.marked_live = true;
                }
            }
            Ok(Err(marketfeed_transport::TransportError::Closed)) => {
                pump.drive(
                    session,
                    SessionInput::Disconnected {
                        reason: DisconnectReason::RemoteClose,
                        now: now_ns(),
                    },
                    sink,
                )?;
                return Err(PrivateError::Transport(
                    "private websocket closed by peer".into(),
                ));
            }
            Ok(Err(e)) => {
                pump.drive(
                    session,
                    SessionInput::Disconnected {
                        reason: DisconnectReason::RemoteClose,
                        now: now_ns(),
                    },
                    sink,
                )?;
                return Err(PrivateError::Transport(e.to_string()));
            }
            Err(_elapsed) => {
                if pump.marked_live {
                    stats.marked_live = true;
                }
            }
        }
    }
    if pump.marked_live {
        stats.marked_live = true;
    }
    stats.account_events = pump.account_events;
    stats.system_events = pump.system_events;
    if pump.reconnect {
        let _ = ws.close(marketfeed_transport::CloseReason::GoingAway).await;
        pump.drive(
            session,
            SessionInput::Disconnected {
                reason: DisconnectReason::ReconnectRequested,
                now: now_ns(),
            },
            sink,
        )?;
        return Err(PrivateError::Protocol(
            "private session requested reconnect".into(),
        ));
    }
    let _ = ws.close(marketfeed_transport::CloseReason::LocalStop).await;
    pump.drive(
        session,
        SessionInput::Disconnected {
            reason: DisconnectReason::LocalStop,
            now: now_ns(),
        },
        sink,
    )?;
    stats.system_events = pump.system_events;
    Ok(stats)
}

fn merge_ws_idle(mut base: PrivateLiveStats, idle: PrivateLiveStats) -> PrivateLiveStats {
    base.marked_live |= idle.marked_live;
    base.text_frames += idle.text_frames;
    base.account_events += idle.account_events;
    base.system_events += idle.system_events;
    base.http_requests += idle.http_requests;
    base.ws_writes += idle.ws_writes;
    base
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketfeed_transport::{MemoryWebSocket, ScriptedHttpTransport};

    use crate::bybit::FIXTURE_AUTH_PAYLOAD;
    use crate::okx::FIXTURE_LOGIN_PAYLOAD;

    #[derive(Default)]
    struct CountingSink {
        account: u64,
        system: u64,
    }

    impl AccountEventSink for CountingSink {
        fn push_account(&mut self, _event: AccountEvent) -> Result<(), PrivateError> {
            self.account += 1;
            Ok(())
        }

        fn push_system(&mut self, _event: SystemEvent) -> Result<(), PrivateError> {
            self.system += 1;
            Ok(())
        }
    }

    struct OverflowingSession;

    impl PrivateSessionMachine for OverflowingSession {
        fn on_input(
            &mut self,
            _input: SessionInput<'_>,
            output: &mut PrivateActionBuffer,
        ) -> Result<(), PrivateError> {
            for _ in 0..=crate::session::DEFAULT_PRIVATE_ACTION_BUFFER_CAPACITY {
                output.push_session(SessionAction::MarkLive);
            }
            Ok(())
        }
    }

    #[test]
    fn private_pump_fails_closed_on_action_buffer_overflow() {
        let mut pump = PrivatePump::new();
        let mut session = OverflowingSession;
        let mut sink = CountingSink::default();
        let error = pump
            .drive(
                &mut session,
                SessionInput::Connected {
                    now: TimestampNs(1),
                },
                &mut sink,
            )
            .unwrap_err();
        assert!(error.to_string().contains("action buffer overflow"));
    }

    #[test]
    fn private_pump_fails_closed_when_timer_set_exceeds_bound() {
        let mut pump = PrivatePump::with_capacity(1);
        let mut actions = PrivateActionBuffer::with_capacity(2);
        actions.push_session(SessionAction::ScheduleTimer(
            marketfeed_adapter_api::TimerSpec {
                timer_id: 1,
                fire_at: TimestampNs(10),
            },
        ));
        actions.push_session(SessionAction::ScheduleTimer(
            marketfeed_adapter_api::TimerSpec {
                timer_id: 2,
                fire_at: TimestampNs(20),
            },
        ));
        let mut sink = CountingSink::default();

        let error = pump.ingest(&mut actions, &mut sink).unwrap_err();
        assert!(error.to_string().contains("timer capacity"));
        assert_eq!(pump.timers.len(), 1);
    }

    #[test]
    fn private_pump_fails_closed_when_http_or_websocket_queue_exceeds_bound() {
        let mut sink = CountingSink::default();
        let request = |id| {
            SessionAction::RequestHttp(HttpRequestSpec {
                id,
                method: AdapterMethod::Get,
                url: "https://example.test/private".into(),
                headers: Vec::new(),
                body: None,
            })
        };

        let mut http_pump = PrivatePump::with_capacity(1);
        let mut http_actions = PrivateActionBuffer::with_capacity(2);
        http_actions.push_session(request(1));
        http_actions.push_session(request(2));
        let error = http_pump.ingest(&mut http_actions, &mut sink).unwrap_err();
        assert!(error.to_string().contains("HTTP request capacity"));
        assert_eq!(http_pump.pending_http.len(), 1);

        let mut websocket_pump = PrivatePump::with_capacity(1);
        let mut websocket_actions = PrivateActionBuffer::with_capacity(2);
        websocket_actions
            .push_session(SessionAction::SendText(bytes::Bytes::from_static(b"first")));
        websocket_actions.push_session(SessionAction::SendPing(bytes::Bytes::from_static(
            b"second",
        )));
        let error = websocket_pump
            .ingest(&mut websocket_actions, &mut sink)
            .unwrap_err();
        assert!(error.to_string().contains("WebSocket write capacity"));
        assert_eq!(websocket_pump.pending_ws.len(), 1);
    }

    #[tokio::test]
    async fn retired_binance_live_runner_fails_before_transport_io() {
        let http = ScriptedHttpTransport::new();
        let mut ws = MemoryWebSocket::new();
        let mut session = BinanceSpotUserDataSession::new(BinanceSpotUserDataConfig::default());
        let mut sink = CountingSink::default();

        let err =
            run_binance_spot_user_data_live_until(&mut session, &http, &mut ws, &mut sink, || {
                false
            })
            .await
            .expect_err("retired Binance listen-key flow must fail closed");

        assert_eq!(err, PrivateError::NotImplemented);
        assert!(ws.outbound.is_empty());
        assert!(!session.is_live());
        assert_eq!(sink.account, 0);
    }

    #[tokio::test]
    async fn scripted_okx_login_marks_live_and_drains() {
        let mut ws = MemoryWebSocket::new();
        ws.push_text(br#"{"event":"login","code":"0","msg":"","connId":"fixture"}"#.to_vec());
        let mut session = OkxPrivateSession::new(OkxPrivateConfig {
            login_payload: FIXTURE_LOGIN_PAYLOAD.into(),
            ..OkxPrivateConfig::default()
        });
        let mut sink = CountingSink::default();
        let mut polls = 0;
        let stats = run_okx_private_live_until(&mut session, &mut ws, &mut sink, || {
            polls += 1;
            polls > 1
        })
        .await
        .expect("scripted okx private");
        assert!(stats.marked_live);
        assert!(!session.is_live());
        assert!(stats.ws_writes >= 2); // login + subscribe
        assert!(stats.text_frames >= 1);
        assert_eq!(sink.account, 0);
        assert!(sink.system >= 3);
        assert!(
            ws.outbound
                .iter()
                .any(|f| String::from_utf8_lossy(&f.payload).contains(r#""op":"login""#))
        );
    }

    #[tokio::test]
    async fn scripted_bybit_auth_marks_live_and_drains() {
        let mut ws = MemoryWebSocket::new();
        ws.push_text(br#"{"success":true,"ret_msg":"","op":"auth","conn_id":"fixture"}"#.to_vec());
        let mut session = BybitPrivateSession::new(BybitPrivateConfig {
            auth_payload: FIXTURE_AUTH_PAYLOAD.into(),
            ..BybitPrivateConfig::default()
        });
        let mut sink = CountingSink::default();
        let mut polls = 0;
        let stats = run_bybit_private_live_until(&mut session, &mut ws, &mut sink, || {
            polls += 1;
            polls > 1
        })
        .await
        .expect("scripted bybit private");
        assert!(stats.marked_live);
        assert!(!session.is_live());
        assert!(stats.ws_writes >= 2);
        assert_eq!(sink.account, 0);
        assert!(sink.system >= 3);
        assert!(
            ws.outbound
                .iter()
                .any(|f| String::from_utf8_lossy(&f.payload).contains(r#""op":"auth""#))
        );
    }

    #[tokio::test]
    async fn remote_close_is_an_error_and_marks_private_session_disconnected() {
        let mut ws = MemoryWebSocket::new();
        ws.push_text(br#"{"event":"login","code":"0","msg":"","connId":"fixture"}"#.to_vec());
        let mut session = OkxPrivateSession::new(OkxPrivateConfig {
            login_payload: FIXTURE_LOGIN_PAYLOAD.into(),
            ..OkxPrivateConfig::default()
        });
        let mut sink = CountingSink::default();

        let error = run_okx_private_live_until(&mut session, &mut ws, &mut sink, || false)
            .await
            .expect_err("remote close requires caller reconnect supervision");

        assert!(error.to_string().contains("closed by peer"));
        assert!(!session.is_live());
        assert!(sink.system >= 3, "connected, authed, disconnected");
    }
}
