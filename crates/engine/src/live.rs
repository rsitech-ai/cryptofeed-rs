//! Live session loop: transport I/O owned by engine, adapters stay deterministic.

use std::future::Future;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use marketfeed_adapter_api::{DisconnectReason, HttpMethod as AdapterMethod, ReconnectPolicy};
use marketfeed_model::{FrameStamp, SystemEvent, TimestampNs};
use marketfeed_sinks::EventSink;
use marketfeed_transport::{
    FrameBuffer, FrameOpcode, HttpMethod, HttpRequest, HttpTransport, WebSocketSpec,
    WebSocketTransport,
};
use tokio::time::{sleep, timeout};

use crate::reconnect::{BackoffState, StableLiveReset};
use crate::{EngineError, SessionLifecycle, SessionRunner};

const STOP_POLL_MS: u64 = 250;
const CLOSE_TIMEOUT_MS: u64 = 250;
/// Material wall-clock discontinuity threshold (§25): 1s forward or backward.
pub const WALL_CLOCK_JUMP_THRESHOLD_NS: i64 = 1_000_000_000;

fn now_ns() -> TimestampNs {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    TimestampNs(dur.as_nanos() as i64)
}

fn mono_ns() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_nanos() as u64
}

/// Detect a material wall-clock discontinuity after subtracting real elapsed time.
pub fn wall_clock_jump_delta(
    prev: TimestampNs,
    now: TimestampNs,
    monotonic_elapsed_ns: u64,
    threshold_ns: i64,
) -> Option<i64> {
    let wall_elapsed_ns = now.0.saturating_sub(prev.0);
    let monotonic_elapsed_ns = i64::try_from(monotonic_elapsed_ns).unwrap_or(i64::MAX);
    let delta = wall_elapsed_ns.saturating_sub(monotonic_elapsed_ns);
    if delta.saturating_abs() >= threshold_ns {
        Some(delta)
    } else {
        None
    }
}

#[derive(Debug, Clone, Copy)]
struct ClockSample {
    wall: TimestampNs,
    monotonic_ns: u64,
}

fn note_clock_jump(
    runner: &mut SessionRunner,
    prev: &mut Option<ClockSample>,
    wall: TimestampNs,
    monotonic_ns: u64,
) -> Result<(), EngineError> {
    if let Some(sample) = *prev {
        if let Some(delta_ns) = wall_clock_jump_delta(
            sample.wall,
            wall,
            monotonic_ns.saturating_sub(sample.monotonic_ns),
            WALL_CLOCK_JUMP_THRESHOLD_NS,
        ) {
            runner.push_system(SystemEvent::ClockJump { delta_ns })?;
        }
    }
    *prev = Some(ClockSample { wall, monotonic_ns });
    Ok(())
}

fn consume<S: EventSink + ?Sized>(
    runner: &mut SessionRunner,
    sink: &mut Option<&mut S>,
) -> Result<(), EngineError> {
    // Always drain: `None` null-drains (FailEngine-safe); `Some` forwards to sinks.
    runner.consume_dispatch(sink.as_deref_mut())
}

struct ReconnectState {
    backoff: BackoffState,
    stable_live: StableLiveReset,
    reconnects: u32,
    reset_after_live: Duration,
}

impl ReconnectState {
    fn new(policy: ReconnectPolicy) -> Self {
        Self {
            backoff: BackoffState::new(policy),
            stable_live: StableLiveReset::default(),
            reconnects: 0,
            reset_after_live: Duration::from_millis(policy.reset_after_live_ms),
        }
    }

    fn observe(&mut self, lifecycle: SessionLifecycle) {
        self.apply_observation(lifecycle == SessionLifecycle::Live, Instant::now());
    }

    fn observe_transition(&mut self, before: SessionLifecycle, after: SessionLifecycle) {
        let now = Instant::now();
        if before == SessionLifecycle::Live && after != SessionLifecycle::Live {
            self.apply_observation(true, now);
        }
        self.apply_observation(after == SessionLifecycle::Live, now);
    }

    fn apply_observation(&mut self, is_live: bool, now: Instant) {
        if self
            .stable_live
            .observe(is_live, now, self.reset_after_live)
        {
            self.backoff.reset();
            self.reconnects = 0;
        }
    }

    fn clear_stable_live(&mut self) {
        self.stable_live.clear();
    }

    fn note_failure(&mut self) {
        self.reconnects = self.reconnects.saturating_add(1);
    }

    fn exhausted(&self, max_reconnects: u32) -> bool {
        self.reconnects > max_reconnects
    }

    fn next_delay(&mut self) -> Duration {
        self.backoff.next_delay()
    }
}

async fn graceful_stop<T: WebSocketTransport, S: EventSink + ?Sized>(
    runner: &mut SessionRunner,
    transport: &mut T,
    sink: &mut Option<&mut S>,
) -> Result<(), EngineError> {
    runner.request_stop();
    runner.push_system(SystemEvent::ShutdownStarted)?;
    close_transport_bounded(transport, marketfeed_transport::CloseReason::LocalStop).await;
    runner.on_disconnected(DisconnectReason::LocalStop, now_ns())?;
    consume(runner, sink)?;
    runner.push_system(SystemEvent::ShutdownCompleted)?;
    runner.lifecycle = SessionLifecycle::Stopped;
    consume(runner, sink)?;
    Ok(())
}

async fn close_transport_bounded<T: WebSocketTransport>(
    transport: &mut T,
    reason: marketfeed_transport::CloseReason,
) {
    let _ = timeout(
        Duration::from_millis(CLOSE_TIMEOUT_MS),
        transport.close(reason),
    )
    .await;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopAware<T> {
    Completed(T),
    StopRequested,
}

async fn wait_for_stop(stop_signal: Option<Arc<AtomicBool>>) {
    loop {
        if stop_signal
            .as_ref()
            .is_some_and(|signal| signal.load(Ordering::Relaxed))
        {
            return;
        }
        sleep(Duration::from_millis(STOP_POLL_MS)).await;
    }
}

async fn await_or_stop<F>(future: F, stop_signal: Option<Arc<AtomicBool>>) -> StopAware<F::Output>
where
    F: Future,
{
    tokio::pin!(future);
    tokio::select! {
        biased;
        () = wait_for_stop(stop_signal) => StopAware::StopRequested,
        result = &mut future => StopAware::Completed(result),
    }
}

async fn wait_for_reconnect_or_stop(stop_signal: Option<Arc<AtomicBool>>, delay: Duration) {
    let _ = await_or_stop(sleep(delay), stop_signal).await;
}

/// Run one session against WS + HTTP with reconnect/backoff (null-drain dispatch).
pub async fn run_session_with_reconnect<T: WebSocketTransport, H: HttpTransport>(
    runner: &mut SessionRunner,
    transport: &mut T,
    http: &H,
    spec: &WebSocketSpec,
    policy: ReconnectPolicy,
    max_reconnects: u32,
) -> Result<(), EngineError> {
    run_session_with_reconnect_to(
        runner,
        transport,
        http,
        spec,
        policy,
        max_reconnects,
        None::<&mut dyn EventSink>,
    )
    .await
}

/// Like [`run_session_with_reconnect`], optionally forwarding dispatch into `sink`
/// instead of null-draining. Dispatch is always drained (FailEngine-safe).
pub async fn run_session_with_reconnect_to<
    T: WebSocketTransport,
    H: HttpTransport,
    S: EventSink + ?Sized,
>(
    runner: &mut SessionRunner,
    transport: &mut T,
    http: &H,
    spec: &WebSocketSpec,
    policy: ReconnectPolicy,
    max_reconnects: u32,
    sink: Option<&mut S>,
) -> Result<(), EngineError> {
    let result = run_session_with_reconnect_to_inner(
        runner,
        transport,
        http,
        spec,
        policy,
        max_reconnects,
        sink,
    )
    .await;
    if result.is_err() {
        runner.invalidate_live_readiness();
        runner.lifecycle = SessionLifecycle::Stopped;
    }
    result
}

async fn run_session_with_reconnect_to_inner<
    T: WebSocketTransport,
    H: HttpTransport,
    S: EventSink + ?Sized,
>(
    runner: &mut SessionRunner,
    transport: &mut T,
    http: &H,
    spec: &WebSocketSpec,
    policy: ReconnectPolicy,
    max_reconnects: u32,
    mut sink: Option<&mut S>,
) -> Result<(), EngineError> {
    let mut reconnect = ReconnectState::new(policy);
    let mut last_clock: Option<ClockSample> = None;

    loop {
        if runner.is_stop_requested() {
            return graceful_stop(runner, transport, &mut sink).await;
        }
        runner.lifecycle = SessionLifecycle::Connecting;
        let stop_signal = runner.shared_stop_signal();
        let connect_result = timeout(
            Duration::from_millis(spec.connect_timeout_ms.max(1)),
            await_or_stop(transport.connect(spec), stop_signal),
        )
        .await
        .unwrap_or(StopAware::Completed(Err(
            marketfeed_transport::TransportError::Timeout,
        )));
        match connect_result {
            StopAware::StopRequested => {
                return graceful_stop(runner, transport, &mut sink).await;
            }
            StopAware::Completed(Ok(())) => {}
            StopAware::Completed(Err(e)) => {
                reconnect.note_failure();
                if reconnect.exhausted(max_reconnects) || runner.is_stop_requested() {
                    return Err(e.into());
                }
                wait_for_reconnect_or_stop(runner.shared_stop_signal(), reconnect.next_delay())
                    .await;
                continue;
            }
        }

        let connected_at = now_ns();
        note_clock_jump(runner, &mut last_clock, connected_at, mono_ns())?;
        reconnect.clear_stable_live();
        let lifecycle_before = runner.lifecycle;
        runner.on_connected(connected_at)?;
        reconnect.observe_transition(lifecycle_before, runner.lifecycle);
        let lifecycle_before = runner.lifecycle;
        if flush_side_effects(runner, transport, http).await? == StopAware::StopRequested {
            return graceful_stop(runner, transport, &mut sink).await;
        }
        reconnect.observe_transition(lifecycle_before, runner.lifecycle);

        let mut buf = FrameBuffer::default();
        let mut session_ok = true;
        let mut disconnect_reason = DisconnectReason::RemoteClose;
        let mut close_reason = marketfeed_transport::CloseReason::GoingAway;
        let mut transport_error = None;

        while session_ok {
            if runner.is_stop_requested() {
                return graceful_stop(runner, transport, &mut sink).await;
            }
            if runner.reconnect_requested {
                disconnect_reason = DisconnectReason::ReconnectRequested;
                break;
            }

            let now = now_ns();
            note_clock_jump(runner, &mut last_clock, now, mono_ns())?;
            reconnect.observe(runner.lifecycle);
            let lifecycle_before = runner.lifecycle;
            runner.poll_timers(now)?;
            reconnect.observe_transition(lifecycle_before, runner.lifecycle);
            let lifecycle_before = runner.lifecycle;
            if flush_side_effects(runner, transport, http).await? == StopAware::StopRequested {
                return graceful_stop(runner, transport, &mut sink).await;
            }
            reconnect.observe_transition(lifecycle_before, runner.lifecycle);
            if runner.reconnect_requested || runner.is_stop_requested() {
                continue;
            }

            let stop_cap = Duration::from_millis(STOP_POLL_MS);
            let wait = match runner.delay_until_next_timer(now) {
                Some(delay) if delay < stop_cap => delay,
                Some(_) | None => stop_cap,
            };
            let read = match timeout(wait, transport.read_frame(&mut buf)).await {
                Ok(result) => result,
                Err(_elapsed) => {
                    let fired_at = now_ns();
                    note_clock_jump(runner, &mut last_clock, fired_at, mono_ns())?;
                    reconnect.observe(runner.lifecycle);
                    let lifecycle_before = runner.lifecycle;
                    runner.poll_timers(fired_at)?;
                    reconnect.observe_transition(lifecycle_before, runner.lifecycle);
                    let lifecycle_before = runner.lifecycle;
                    if flush_side_effects(runner, transport, http).await?
                        == StopAware::StopRequested
                    {
                        return graceful_stop(runner, transport, &mut sink).await;
                    }
                    reconnect.observe_transition(lifecycle_before, runner.lifecycle);
                    // Drain every idle tick so FailEngine queues never fill without a consumer.
                    consume(runner, &mut sink)?;
                    reconnect.observe(runner.lifecycle);
                    continue;
                }
            };
            // A live connection can close without delivering a data frame. Count
            // the completed stable interval before handling that terminal read.
            reconnect.observe(runner.lifecycle);

            match read {
                Ok(frame) => {
                    let receive_ts = now_ns();
                    let receive_mono_ns = mono_ns();
                    note_clock_jump(runner, &mut last_clock, receive_ts, receive_mono_ns)?;
                    let stamp = FrameStamp {
                        receive_ts,
                        mono_ns: receive_mono_ns,
                    };
                    let lifecycle_before = runner.lifecycle;
                    match frame.opcode {
                        FrameOpcode::Text => {
                            let mut payload = frame.payload;
                            runner.on_text_frame(&mut payload, stamp)?;
                        }
                        FrameOpcode::Binary => {
                            let mut payload = frame.payload;
                            runner.on_binary_frame(&mut payload, stamp)?;
                        }
                        FrameOpcode::Ping => {
                            runner.on_ping_frame(&frame.payload, stamp)?;
                        }
                        FrameOpcode::Pong => {
                            runner.on_pong_frame(&frame.payload, stamp)?;
                        }
                        FrameOpcode::Close => {
                            runner.on_close_frame(&frame.payload, stamp)?;
                            session_ok = false;
                        }
                    }
                    reconnect.observe_transition(lifecycle_before, runner.lifecycle);
                    let lifecycle_before = runner.lifecycle;
                    if flush_side_effects(runner, transport, http).await?
                        == StopAware::StopRequested
                    {
                        return graceful_stop(runner, transport, &mut sink).await;
                    }
                    reconnect.observe_transition(lifecycle_before, runner.lifecycle);
                    // Sink replaces null-drain; either path empties dispatch (metrics on push).
                    consume(runner, &mut sink)?;
                    reconnect.observe(runner.lifecycle);
                    if runner.reconnect_requested {
                        disconnect_reason = DisconnectReason::ReconnectRequested;
                        break;
                    }
                }
                Err(marketfeed_transport::TransportError::Closed) => {
                    session_ok = false;
                }
                Err(e) => {
                    disconnect_reason = DisconnectReason::TransportError;
                    close_reason = marketfeed_transport::CloseReason::ProtocolError;
                    transport_error = Some(e);
                    session_ok = false;
                }
            }
        }

        runner.on_transport_lost(disconnect_reason, now_ns())?;
        reconnect.clear_stable_live();
        close_transport_bounded(transport, close_reason).await;

        reconnect.note_failure();
        if runner.is_stop_requested() {
            consume(runner, &mut sink)?;
            runner.lifecycle = SessionLifecycle::Stopped;
            return Ok(());
        }
        if reconnect.exhausted(max_reconnects) {
            consume(runner, &mut sink)?;
            runner.lifecycle = SessionLifecycle::Stopped;
            return match transport_error {
                Some(error) => Err(error.into()),
                None => Ok(()),
            };
        }
        runner.note_reconnect();
        runner.lifecycle = SessionLifecycle::Backoff;
        wait_for_reconnect_or_stop(runner.shared_stop_signal(), reconnect.next_delay()).await;
    }
}

async fn flush_side_effects<T: WebSocketTransport, H: HttpTransport>(
    runner: &mut SessionRunner,
    transport: &mut T,
    http: &H,
) -> Result<StopAware<()>, EngineError> {
    if flush_writes(runner, transport).await? == StopAware::StopRequested {
        return Ok(StopAware::StopRequested);
    }
    flush_http(runner, http).await
}

async fn flush_writes<T: WebSocketTransport>(
    runner: &mut SessionRunner,
    transport: &mut T,
) -> Result<StopAware<()>, EngineError> {
    for frame in runner.take_pending_writes() {
        let stop_signal = runner.shared_stop_signal();
        match await_or_stop(transport.write_frame(frame), stop_signal).await {
            StopAware::Completed(result) => result?,
            StopAware::StopRequested => return Ok(StopAware::StopRequested),
        }
    }
    Ok(StopAware::Completed(()))
}

async fn flush_http<H: HttpTransport>(
    runner: &mut SessionRunner,
    http: &H,
) -> Result<StopAware<()>, EngineError> {
    loop {
        let specs = runner.take_pending_http();
        if specs.is_empty() {
            break;
        }
        for spec in specs {
            let req = HttpRequest {
                method: match spec.method {
                    AdapterMethod::Get => HttpMethod::Get,
                    AdapterMethod::Post => HttpMethod::Post,
                    AdapterMethod::Put => HttpMethod::Put,
                    AdapterMethod::Delete => HttpMethod::Delete,
                },
                url: spec.url,
                headers: spec.headers,
                body: spec.body,
                timeout_ms: 10_000,
                max_body_bytes: 16 * 1024 * 1024,
            };
            let rest_t0 = Instant::now();
            let stop_signal = runner.shared_stop_signal();
            let resp = match await_or_stop(http.request(req), stop_signal).await {
                StopAware::Completed(result) => result?,
                StopAware::StopRequested => return Ok(StopAware::StopRequested),
            };
            runner
                .metrics
                .observe_rest_latency_ns(rest_t0.elapsed().as_nanos() as u64);
            let adapter_resp = marketfeed_adapter_api::HttpResponse {
                status: resp.status,
                headers: resp.headers,
                body: resp.body,
            };
            runner.on_http_response(
                spec.id,
                &adapter_resp,
                FrameStamp {
                    receive_ts: now_ns(),
                    mono_ns: mono_ns(),
                },
            )?;
        }
    }
    Ok(StopAware::Completed(()))
}
