use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, DisconnectReason, HttpMethod as AdapterHttpMethod, HttpRequestSpec,
    ReconnectPolicy, SessionAction, SessionInput, SessionMachine,
};
use marketfeed_engine::{SessionRunner, SessionRunnerConfig, run_session_with_reconnect};
use marketfeed_transport::{
    CloseReason, FrameBuffer, FrameOpcode, HttpRequest, HttpResponse, HttpTransport, InboundFrame,
    OutboundFrame, StubHttpTransport, TransportError, WebSocketSpec, WebSocketTransport,
};

struct MarkLiveAndRecordDisconnects {
    reasons: Arc<Mutex<Vec<DisconnectReason>>>,
}

impl SessionMachine for MarkLiveAndRecordDisconnects {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::TextFrame { .. } => output.push(SessionAction::MarkLive),
            SessionInput::Disconnected { reason, .. } => {
                self.reasons.lock().unwrap().push(reason);
            }
            _ => {}
        }
        Ok(())
    }
}

struct IoFailureThenStop {
    connects: Arc<AtomicUsize>,
    reads: usize,
    stop: Arc<AtomicBool>,
}

struct AlwaysFailConnect {
    connects: Arc<AtomicUsize>,
}

struct HangingConnect;

struct HangingClose;

impl WebSocketTransport for HangingConnect {
    async fn connect(&mut self, _spec: &WebSocketSpec) -> Result<(), TransportError> {
        std::future::pending().await
    }

    async fn read_frame(
        &mut self,
        _buffer: &mut FrameBuffer,
    ) -> Result<InboundFrame, TransportError> {
        unreachable!("connect never completes")
    }

    async fn write_frame(&mut self, _frame: OutboundFrame) -> Result<(), TransportError> {
        unreachable!("connect never completes")
    }

    async fn close(&mut self, _reason: CloseReason) -> Result<(), TransportError> {
        Ok(())
    }
}

impl WebSocketTransport for HangingClose {
    async fn connect(&mut self, _spec: &WebSocketSpec) -> Result<(), TransportError> {
        Ok(())
    }

    async fn read_frame(
        &mut self,
        _buffer: &mut FrameBuffer,
    ) -> Result<InboundFrame, TransportError> {
        std::future::pending().await
    }

    async fn write_frame(&mut self, _frame: OutboundFrame) -> Result<(), TransportError> {
        Ok(())
    }

    async fn close(&mut self, _reason: CloseReason) -> Result<(), TransportError> {
        std::future::pending().await
    }
}

struct RequestHttpOnConnect;

impl SessionMachine for RequestHttpOnConnect {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        if matches!(input, SessionInput::Connected { .. }) {
            output.push(SessionAction::RequestHttp(HttpRequestSpec {
                id: 1,
                method: AdapterHttpMethod::Get,
                url: "https://example.test/snapshot".into(),
                headers: Vec::new(),
                body: None,
            }));
        }
        Ok(())
    }
}

struct ConnectedPendingRead;

impl WebSocketTransport for ConnectedPendingRead {
    async fn connect(&mut self, _spec: &WebSocketSpec) -> Result<(), TransportError> {
        Ok(())
    }

    async fn read_frame(
        &mut self,
        _buffer: &mut FrameBuffer,
    ) -> Result<InboundFrame, TransportError> {
        std::future::pending().await
    }

    async fn write_frame(&mut self, _frame: OutboundFrame) -> Result<(), TransportError> {
        Ok(())
    }

    async fn close(&mut self, _reason: CloseReason) -> Result<(), TransportError> {
        Ok(())
    }
}

struct HangingHttp;

impl HttpTransport for HangingHttp {
    async fn request(&self, _req: HttpRequest) -> Result<HttpResponse, TransportError> {
        std::future::pending().await
    }
}

impl WebSocketTransport for AlwaysFailConnect {
    async fn connect(&mut self, _spec: &WebSocketSpec) -> Result<(), TransportError> {
        self.connects.fetch_add(1, Ordering::Relaxed);
        Err(TransportError::Io("injected connect failure".into()))
    }

    async fn read_frame(
        &mut self,
        _buffer: &mut FrameBuffer,
    ) -> Result<InboundFrame, TransportError> {
        unreachable!("connect never succeeds")
    }

    async fn write_frame(&mut self, _frame: OutboundFrame) -> Result<(), TransportError> {
        unreachable!("connect never succeeds")
    }

    async fn close(&mut self, _reason: CloseReason) -> Result<(), TransportError> {
        Ok(())
    }
}

impl WebSocketTransport for IoFailureThenStop {
    async fn connect(&mut self, _spec: &WebSocketSpec) -> Result<(), TransportError> {
        self.connects.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn read_frame(
        &mut self,
        _buffer: &mut FrameBuffer,
    ) -> Result<InboundFrame, TransportError> {
        self.reads += 1;
        if self.reads == 1 {
            return Ok(InboundFrame {
                opcode: FrameOpcode::Text,
                payload: b"live".to_vec(),
            });
        }
        if self.reads == 2 {
            return Err(TransportError::Io("injected read failure".into()));
        }
        self.stop.store(true, Ordering::Relaxed);
        Err(TransportError::Closed)
    }

    async fn write_frame(&mut self, _frame: OutboundFrame) -> Result<(), TransportError> {
        Ok(())
    }

    async fn close(&mut self, _reason: CloseReason) -> Result<(), TransportError> {
        Ok(())
    }
}

#[tokio::test]
async fn transient_read_error_reconnects_with_transport_reason() {
    let connects = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let reasons = Arc::new(Mutex::new(Vec::new()));
    let mut transport = IoFailureThenStop {
        connects: Arc::clone(&connects),
        reads: 0,
        stop: Arc::clone(&stop),
    };
    let mut runner = SessionRunner::new(
        Box::new(MarkLiveAndRecordDisconnects {
            reasons: Arc::clone(&reasons),
        }),
        SessionRunnerConfig {
            record: false,
            stop_signal: Some(stop),
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();

    run_session_with_reconnect(
        &mut runner,
        &mut transport,
        &StubHttpTransport,
        &WebSocketSpec::default(),
        ReconnectPolicy {
            min_delay_ms: 1,
            max_delay_ms: 1,
            reset_after_live_ms: 1_000,
        },
        5,
    )
    .await
    .unwrap();

    assert_eq!(connects.load(Ordering::Relaxed), 2);
    assert_eq!(
        reasons.lock().unwrap().as_slice(),
        [
            DisconnectReason::TransportError,
            DisconnectReason::RemoteClose
        ]
    );
}

#[tokio::test]
async fn stop_interrupts_connect_failure_backoff() {
    let connects = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let stop_after_backoff_starts = Arc::clone(&stop);
    let mut transport = AlwaysFailConnect {
        connects: Arc::clone(&connects),
    };
    let mut runner = SessionRunner::new(
        Box::new(MarkLiveAndRecordDisconnects {
            reasons: Arc::new(Mutex::new(Vec::new())),
        }),
        SessionRunnerConfig {
            record: false,
            stop_signal: Some(Arc::clone(&stop)),
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        stop_after_backoff_starts.store(true, Ordering::Relaxed);
    });

    tokio::time::timeout(
        Duration::from_millis(500),
        run_session_with_reconnect(
            &mut runner,
            &mut transport,
            &StubHttpTransport,
            &WebSocketSpec::default(),
            ReconnectPolicy {
                min_delay_ms: 5_000,
                max_delay_ms: 5_000,
                reset_after_live_ms: 60_000,
            },
            u32::MAX,
        ),
    )
    .await
    .expect("stop must interrupt reconnect backoff")
    .unwrap();

    assert_eq!(connects.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn stop_interrupts_pending_websocket_connect() {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_while_connecting = Arc::clone(&stop);
    let mut transport = HangingConnect;
    let mut runner = SessionRunner::new(
        Box::new(MarkLiveAndRecordDisconnects {
            reasons: Arc::new(Mutex::new(Vec::new())),
        }),
        SessionRunnerConfig {
            record: false,
            stop_signal: Some(Arc::clone(&stop)),
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        stop_while_connecting.store(true, Ordering::Relaxed);
    });

    tokio::time::timeout(
        Duration::from_millis(500),
        run_session_with_reconnect(
            &mut runner,
            &mut transport,
            &StubHttpTransport,
            &WebSocketSpec::default(),
            ReconnectPolicy {
                min_delay_ms: 1,
                max_delay_ms: 1,
                reset_after_live_ms: 1_000,
            },
            u32::MAX,
        ),
    )
    .await
    .expect("stop must interrupt a pending WebSocket connect")
    .unwrap();
}

#[tokio::test]
async fn pending_websocket_connect_is_bounded_by_spec_timeout() {
    let mut runner = SessionRunner::new(
        Box::new(MarkLiveAndRecordDisconnects {
            reasons: Arc::new(Mutex::new(Vec::new())),
        }),
        SessionRunnerConfig {
            record: false,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();
    let mut transport = HangingConnect;
    let result = run_session_with_reconnect(
        &mut runner,
        &mut transport,
        &StubHttpTransport,
        &WebSocketSpec {
            connect_timeout_ms: 10,
            ..WebSocketSpec::default()
        },
        ReconnectPolicy {
            min_delay_ms: 1,
            max_delay_ms: 1,
            reset_after_live_ms: 1_000,
        },
        0,
    )
    .await;

    assert_eq!(result.unwrap_err().to_string(), "timeout");
}

#[tokio::test]
async fn stop_bounds_pending_websocket_close() {
    let stop = Arc::new(AtomicBool::new(true));
    let mut runner = SessionRunner::new(
        Box::new(MarkLiveAndRecordDisconnects {
            reasons: Arc::new(Mutex::new(Vec::new())),
        }),
        SessionRunnerConfig {
            record: false,
            stop_signal: Some(stop),
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();
    let mut transport = HangingClose;

    let result = tokio::time::timeout(
        Duration::from_millis(500),
        run_session_with_reconnect(
            &mut runner,
            &mut transport,
            &StubHttpTransport,
            &WebSocketSpec::default(),
            ReconnectPolicy {
                min_delay_ms: 1,
                max_delay_ms: 1,
                reset_after_live_ms: 1_000,
            },
            u32::MAX,
        ),
    )
    .await;

    assert!(
        result.is_ok(),
        "graceful stop must not wait indefinitely for the WebSocket close handshake"
    );
    assert!(result.unwrap().is_ok());
}

#[tokio::test]
async fn stop_interrupts_pending_http_request() {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_while_requesting = Arc::clone(&stop);
    let mut transport = ConnectedPendingRead;
    let mut runner = SessionRunner::new(
        Box::new(RequestHttpOnConnect),
        SessionRunnerConfig {
            record: false,
            stop_signal: Some(Arc::clone(&stop)),
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        stop_while_requesting.store(true, Ordering::Relaxed);
    });

    tokio::time::timeout(
        Duration::from_millis(500),
        run_session_with_reconnect(
            &mut runner,
            &mut transport,
            &HangingHttp,
            &WebSocketSpec::default(),
            ReconnectPolicy {
                min_delay_ms: 1,
                max_delay_ms: 1,
                reset_after_live_ms: 1_000,
            },
            u32::MAX,
        ),
    )
    .await
    .expect("stop must interrupt a pending HTTP request")
    .unwrap();
}
