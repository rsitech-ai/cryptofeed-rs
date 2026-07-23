//! Binary E2E: `marketfeed run` with synthetic config → `/live` `/ready` 200,
//! then SIGTERM drains cleanly (offline, no exchange I/O).

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn free_loopback_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    listener.local_addr().expect("local_addr").port()
}

fn http_get(addr: &str, path: &str) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect(addr).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .ok()?;
    stream
        .set_write_timeout(Some(Duration::from_millis(500)))
        .ok()?;
    let req = format!("GET {path} HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).ok()?;
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    let text = String::from_utf8_lossy(&buf);
    let status = text
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())?;
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    Some((status, body))
}

fn wait_http_200(addr: &str, path: &str, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some((200, body)) = http_get(addr, path) {
            return body;
        }
        if Instant::now() > deadline {
            panic!("timed out waiting for {path} on {addr}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn marketfeed_run_synthetic_ready_then_sigterm_drains() {
    let port = free_loopback_port();
    let tmp = std::env::temp_dir().join(format!(
        "marketfeed-run-e2e-{}-{}",
        std::process::id(),
        port
    ));
    std::fs::create_dir_all(&tmp).unwrap();
    let cfg_path: PathBuf = tmp.join("config.toml");
    let cfg = format!(
        r#"
[engine]
runtime_profile = "portable"
shutdown_deadline_secs = 5

[telemetry]
log_format = "json"
log_level = "info"
bind = "127.0.0.1:{port}"

[readiness]
require_running = true
require_required_venues = true
min_live_sessions = 1
require_recording_healthy = false

[recording.raw]
enabled = false
directory = "{dir}/raw"
segment_size = "1MiB"
segment_duration = "1m"
queue_capacity = 64
overflow = "fail_engine"
min_free_space = "0"

[[venues]]
id = "synthetic-demo"
adapter = "synthetic"
required = true
transport = "memory"
"#,
        port = port,
        dir = tmp.display()
    );
    std::fs::write(&cfg_path, cfg).unwrap();

    let bin = env!("CARGO_BIN_EXE_marketfeed");
    let mut child = Command::new(bin)
        .args(["run", "--config"])
        .arg(&cfg_path)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn marketfeed run");

    let addr = format!("127.0.0.1:{port}");
    let _ = wait_http_200(&addr, "/live", Duration::from_secs(10));
    let _ = wait_http_200(&addr, "/ready", Duration::from_secs(5));
    let metrics = wait_http_200(&addr, "/metrics", Duration::from_secs(2));
    assert!(metrics.contains("marketfeed_ready 1"), "{metrics}");
    assert!(metrics.contains("marketfeed_live_sessions 1"), "{metrics}");

    #[cfg(unix)]
    {
        let pid = child.id();
        let status = Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .expect("kill -TERM");
        assert!(status.success(), "kill -TERM failed");
    }
    #[cfg(not(unix))]
    {
        child.kill().expect("kill child");
    }

    let deadline = Instant::now() + Duration::from_secs(8);
    let exit = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() > deadline => {
                let _ = child.kill();
                panic!("marketfeed did not exit after SIGTERM within deadline");
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(50)),
            Err(e) => panic!("try_wait: {e}"),
        }
    };
    assert!(
        exit.success(),
        "expected clean exit after drain, got {exit:?}"
    );
    let _ = std::fs::remove_dir_all(&tmp);
}
