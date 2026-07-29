//! Offline: synthetic venue forwards dispatch into a configured memory sink.

use std::sync::Arc;
use std::time::Duration;

use marketfeed_daemon::{DaemonConfig, DaemonState, spawn_venues};
use tokio::sync::watch;

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
async fn synthetic_forwards_batches_to_memory_sink() {
    let cfg = DaemonConfig::from_toml_str(
        r#"
        [telemetry]
        bind = "127.0.0.1:0"
        [readiness]
        require_running = true
        require_required_venues = true
        min_live_sessions = 1
        [[sinks]]
        id = "probe"
        type = "memory"
        capacity = 64
        overflow = "fail_engine"
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

    wait_ready(&state, 2).await;

    // Synthetic emits at least one book batch; memory sink must retain it (not null-drained).
    // Ready can race slightly ahead of consume_dispatch; poll briefly.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let batches = loop {
        let n = {
            let sinks = state.sinks.lock().expect("sinks lock");
            assert_eq!(sinks.memory.len(), 1);
            assert!(sinks.logging.is_empty());
            assert!(sinks.file.is_empty());
            sinks.memory_batch_len()
        };
        if n >= 1 {
            break n;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("expected memory sink to retain synthetic batches, still empty");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(batches >= 1);

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
    assert_eq!(state.live_session_count(), 0);
}

#[tokio::test]
async fn no_sinks_still_reaches_ready() {
    // Regression: empty [[sinks]] keeps null-drain FailEngine-safe path.
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
    assert!(!state.has_sinks());
    state.mark_supervisor_running();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handles = spawn_venues(Arc::clone(&state), shutdown_rx);

    wait_ready(&state, 2).await;

    state.request_all_stops();
    let _ = shutdown_tx.send(true);
    let join = async {
        for h in handles {
            let _ = h.await;
        }
    };
    tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("null-drain path must still stop cleanly");
}

#[tokio::test]
async fn synthetic_forwards_batches_to_file_sink() {
    let dir =
        std::env::temp_dir().join(format!("marketfeed-daemon-filesink-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("events.log");
    let path_toml = toml::Value::String(path.to_string_lossy().into_owned()).to_string();

    let cfg = DaemonConfig::from_toml_str(&format!(
        r#"
        [telemetry]
        bind = "127.0.0.1:0"
        [readiness]
        require_running = true
        require_required_venues = true
        min_live_sessions = 1
        [[sinks]]
        type = "file"
        path = {path_toml}
        capacity = 64
        overflow = "fail_engine"
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

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let lines = loop {
        let n = {
            let sinks = state.sinks.lock().expect("sinks lock");
            assert_eq!(sinks.file.len(), 1);
            assert!(sinks.memory.is_empty());
            sinks.file_lines_written()
        };
        if n >= 1 {
            break n;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("expected file sink to append synthetic batches, still empty");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(lines >= 1);
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.contains(r#""kind":"batch""#), "{text}");
    assert!(text.contains(r#""events":["#), "{text}");

    state.request_all_stops();
    let _ = shutdown_tx.send(true);
    let join = async {
        for h in handles {
            let _ = h.await;
        }
    };
    tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("file sink path must still stop cleanly");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn synthetic_forwards_batches_to_protobuf_file_sink() {
    let dir = std::env::temp_dir().join(format!(
        "marketfeed-daemon-protobuf-filesink-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("events.mfpe");
    let path_toml = toml::Value::String(path.to_string_lossy().into_owned()).to_string();

    let cfg = DaemonConfig::from_toml_str(&format!(
        r#"
        [telemetry]
        bind = "127.0.0.1:0"
        [readiness]
        require_running = true
        require_required_venues = true
        min_live_sessions = 1
        [[sinks]]
        type = "protobuf-file"
        path = {path_toml}
        capacity = 64
        overflow = "fail_engine"
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

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let records = loop {
        let n = {
            let sinks = state.sinks.lock().expect("sinks lock");
            assert_eq!(sinks.protobuf_file.len(), 1);
            assert!(sinks.file.is_empty());
            sinks.protobuf_records_written()
        };
        if n >= 1 {
            break n;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("expected protobuf-file sink to append records, still empty");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(records >= 1);
    let bytes = std::fs::read(&path).unwrap();
    assert!(bytes.len() >= 4, "expected length-prefixed records");

    state.request_all_stops();
    let _ = shutdown_tx.send(true);
    let join = async {
        for h in handles {
            let _ = h.await;
        }
    };
    tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("protobuf-file sink path must still stop cleanly");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn synthetic_forwards_batches_to_protobuf_file_bin_sink() {
    let dir = std::env::temp_dir().join(format!(
        "marketfeed-daemon-protobuf-filebinsink-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("events.mfpeb");
    let path_toml = toml::Value::String(path.to_string_lossy().into_owned()).to_string();

    let cfg = DaemonConfig::from_toml_str(&format!(
        r#"
        [telemetry]
        bind = "127.0.0.1:0"
        [readiness]
        require_running = true
        require_required_venues = true
        min_live_sessions = 1
        [[sinks]]
        type = "protobuf-file-bin"
        path = {path_toml}
        capacity = 64
        overflow = "fail_engine"
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

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let records = loop {
        let n = {
            let sinks = state.sinks.lock().expect("sinks lock");
            assert_eq!(sinks.protobuf_file_bin.len(), 1);
            assert!(sinks.protobuf_file.is_empty());
            sinks.protobuf_bin_records_written()
        };
        if n >= 1 {
            break n;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("expected protobuf-file-bin sink to append records, still empty");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(records >= 1);
    let bytes = std::fs::read(&path).unwrap();
    assert!(bytes.len() >= 4, "expected length-prefixed records");
    // Market bodies are protobuf (not JSON).
    let len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    assert!(len > 0 && bytes.len() >= 4 + len);
    assert_ne!(bytes[4], b'{', "MFPE-PB1 market record must not be JSON");

    state.request_all_stops();
    let _ = shutdown_tx.send(true);
    let join = async {
        for h in handles {
            let _ = h.await;
        }
    };
    tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("protobuf-file-bin sink path must still stop cleanly");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn synthetic_forwards_batches_to_udp_sink() {
    let sock = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind udp probe");
    let dest = sock.local_addr().expect("local addr");
    sock.set_read_timeout(Some(Duration::from_millis(200)))
        .expect("read timeout");

    let cfg = DaemonConfig::from_toml_str(&format!(
        r#"
        [telemetry]
        bind = "127.0.0.1:0"
        [readiness]
        require_running = true
        require_required_venues = true
        min_live_sessions = 1
        [[sinks]]
        type = "udp"
        address = "{dest}"
        capacity = 64
        overflow = "fail_engine"
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

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let sent = loop {
        let n = {
            let sinks = state.sinks.lock().expect("sinks lock");
            assert_eq!(sinks.udp.len(), 1);
            sinks.udp_datagrams_sent()
        };
        if n >= 1 {
            break n;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("expected udp sink to send datagrams, still zero");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(sent >= 1);

    let mut buf = [0u8; 512];
    let n = sock.recv(&mut buf).expect("recv udp datagram");
    let text = std::str::from_utf8(&buf[..n]).expect("utf8");
    assert!(text.contains(r#""kind":"batch""#), "{text}");
    assert!(text.contains(r#""events":["#), "{text}");

    state.request_all_stops();
    let _ = shutdown_tx.send(true);
    let join = async {
        for h in handles {
            let _ = h.await;
        }
    };
    tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("udp sink path must still stop cleanly");
}
