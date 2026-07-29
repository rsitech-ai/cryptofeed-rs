//! Offline: synthetic memory venue reaches Live and satisfies readiness;
//! graceful shutdown clears live flags (drain path).

use std::sync::Arc;
use std::time::Duration;

use marketfeed_daemon::{DaemonConfig, DaemonState, serve, spawn_venues};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

async fn http_get(addr: &str, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf);
    let status = text
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

async fn wait_ready(state: &DaemonState, secs: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if state.is_ready() {
            return;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("timed out waiting for synthetic venue ready");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn synthetic_venue_makes_ready() {
    let cfg = DaemonConfig::from_toml_str(
        r#"
        [telemetry]
        bind = "127.0.0.1:0"
        [readiness]
        require_running = true
        require_required_venues = true
        min_live_sessions = 1
        [[venues]]
        id = "synthetic-demo"
        adapter = "synthetic"
        required = true
    "#,
    )
    .unwrap();

    let state = DaemonState::new(cfg);
    state.mark_supervisor_running();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handles = spawn_venues(Arc::clone(&state), shutdown_rx);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serve_state = Arc::clone(&state);
    tokio::spawn(async move {
        let _ = serve(listener, serve_state).await;
    });

    wait_ready(&state, 2).await;

    let (st, body) = http_get(&addr, "/ready").await;
    assert_eq!(st, 200, "{body}");
    let (st, body) = http_get(&addr, "/live").await;
    assert_eq!(st, 200, "{body}");
    let (st, body) = http_get(&addr, "/metrics").await;
    assert_eq!(st, 200);
    assert!(body.contains("marketfeed_ready 1"));
    assert!(body.contains("marketfeed_live_sessions 1"));
    assert!(body.contains("marketfeed_venue_live{id=\"synthetic-demo\"} 1"));
    assert!(body.contains("marketfeed_venue_frames_received_total{id=\"synthetic-demo\"}"));
    assert!(body.contains("marketfeed_frames_received_total"));
    assert!(body.contains("marketfeed_batch_queue_capacity"));

    // Graceful stop: signal drain, join venue tasks, live clears.
    state
        .shutdown_draining
        .store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(
        !state.is_ready(),
        "readiness must fail before shutdown starts draining venues"
    );
    let (st, body) = http_get(&addr, "/ready").await;
    assert_eq!(st, 503, "{body}");
    let (st, body) = http_get(&addr, "/metrics").await;
    assert_eq!(st, 200);
    assert!(body.contains("marketfeed_ready 0"), "{body}");
    assert!(body.contains("marketfeed_shutdown_draining 1"), "{body}");

    state.request_all_stops();
    let _ = shutdown_tx.send(true);
    let join = async {
        for h in handles {
            let _ = h.await;
        }
    };
    tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("venue tasks did not drain within deadline");
    state
        .shutdown_draining
        .store(false, std::sync::atomic::Ordering::Relaxed);

    assert_eq!(state.live_session_count(), 0);
    assert!(!state.is_ready());
    let (st, body) = http_get(&addr, "/metrics").await;
    assert_eq!(st, 200);
    assert!(
        body.contains("marketfeed_venue_live{id=\"synthetic-demo\"} 0"),
        "{body}"
    );
    assert!(body.contains("marketfeed_shutdown_draining 0"), "{body}");
}

#[tokio::test]
async fn synthetic_recording_drains_on_shutdown() {
    let dir = std::env::temp_dir().join(format!("marketfeed-synth-rec-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let dir_toml = toml::Value::String(dir.to_string_lossy().into_owned()).to_string();

    let cfg = DaemonConfig::from_toml_str(&format!(
        r#"
        [engine]
        shutdown_deadline_secs = 2
        [telemetry]
        bind = "127.0.0.1:0"
        [readiness]
        require_running = true
        require_required_venues = true
        min_live_sessions = 1
        require_recording_healthy = true
        [recording.raw]
        enabled = true
        directory = {dir_toml}
        segment_size = "1MiB"
        segment_duration = "1m"
        queue_capacity = 64
        overflow = "fail_engine"
        min_free_space = "0"
        [[venues]]
        id = "synthetic-demo"
        adapter = "synthetic"
        required = true
    "#
    ))
    .unwrap();

    let state = DaemonState::new(cfg);
    state.mark_supervisor_running();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handles = spawn_venues(Arc::clone(&state), shutdown_rx);

    wait_ready(&state, 2).await;
    assert!(
        state
            .recording_healthy
            .load(std::sync::atomic::Ordering::Relaxed)
    );

    state
        .shutdown_draining
        .store(true, std::sync::atomic::Ordering::Relaxed);
    state.request_all_stops();
    let _ = shutdown_tx.send(true);
    let join = async {
        for h in handles {
            let _ = h.await;
        }
    };
    tokio::time::timeout(Duration::from_secs(3), join)
        .await
        .expect("recording + venue drain timed out");
    assert!(
        state
            .recording_healthy
            .load(std::sync::atomic::Ordering::Relaxed),
        "recording should remain healthy after drain"
    );
    assert!(
        state
            .recording_written
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0,
        "recording pipeline must persist at least one synthetic wire frame"
    );
    let mut persisted_records = 0usize;
    let mut persisted_metadata = Vec::new();
    for entry in std::fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("mfr1") {
            continue;
        }
        let bytes = std::fs::read(path).unwrap();
        let records = marketfeed_recording::RawSegmentReader::from_bytes(bytes)
            .unwrap()
            .read_all()
            .unwrap();
        persisted_records += records.len();
        persisted_metadata.extend(
            records
                .into_iter()
                .filter(|record| {
                    record.header.opcode == marketfeed_recording::FrameOpcode::Metadata
                })
                .map(|record| marketfeed_recording::decode_metadata(&record.payload).unwrap()),
        );
    }
    assert!(
        persisted_records > 0,
        "recording directory must contain a readable frame"
    );
    assert!(
        persisted_metadata
            .iter()
            .any(|metadata| matches!(metadata, marketfeed_recording::MetadataRecord::Build(_)))
    );
    assert!(persisted_metadata.iter().any(|metadata| matches!(
        metadata,
        marketfeed_recording::MetadataRecord::Session(session)
            if session.adapter == "synthetic"
                && session.environment == "test"
                && session.endpoint == "memory://synthetic"
    )));
    assert_eq!(state.live_session_count(), 0);
    assert_eq!(
        state
            .active_public_venue_tasks
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
    assert!(
        state
            .shutdown_draining
            .load(std::sync::atomic::Ordering::Relaxed),
        "recording worker must not clear the coordinator-owned shutdown state"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
