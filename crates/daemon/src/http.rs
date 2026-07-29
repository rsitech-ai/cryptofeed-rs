//! Minimal HTTP/1.1 server for `/live`, `/ready`, `/metrics` (+ `/v1/*` with `ui-api`).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time::timeout;

use crate::state::DaemonState;

const MAX_CONNECTIONS: usize = 64;
const IO_TIMEOUT: Duration = Duration::from_secs(2);

pub async fn serve(listener: TcpListener, state: Arc<DaemonState>) -> std::io::Result<()> {
    let connections = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    loop {
        let (stream, _) = listener.accept().await?;
        let permit = Arc::clone(&connections)
            .acquire_owned()
            .await
            .map_err(|_| std::io::Error::other("health connection limiter closed"))?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(e) = handle_conn(stream, state).await {
                tracing::debug!(error = %e, "health conn closed");
            }
        });
    }
}

async fn handle_conn(mut stream: TcpStream, state: Arc<DaemonState>) -> std::io::Result<()> {
    let mut buf = vec![0u8; 8192];
    let n = timeout(IO_TIMEOUT, stream.read(&mut buf))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "health read timeout"))??;
    if n == 0 {
        return Ok(());
    }
    let req = String::from_utf8_lossy(&buf[..n]);
    let path = request_path(&req).unwrap_or("/");
    state.http_requests.fetch_add(1, Ordering::Relaxed);

    #[cfg(feature = "ui-api")]
    if path.starts_with("/v1/") || path == "/" || path.starts_with("/assets/") {
        // When ui_bind is unset (or equals bind), view routes share this listener.
        let ui_separate = state
            .config
            .telemetry
            .ui_bind
            .as_ref()
            .map(|b| b != &state.config.telemetry.bind)
            .unwrap_or(false);
        if !ui_separate {
            return crate::view::handle_view_conn_with_prefix(stream, state, &buf[..n]).await;
        }
    }

    let (status, content_type, body) = match path {
        "/live" => {
            if state.is_live() {
                (200, "text/plain; charset=utf-8", "live\n".to_string())
            } else {
                (503, "text/plain; charset=utf-8", "not live\n".to_string())
            }
        }
        "/ready" => {
            if state.is_ready() {
                (200, "text/plain; charset=utf-8", "ready\n".to_string())
            } else {
                (503, "text/plain; charset=utf-8", "not ready\n".to_string())
            }
        }
        "/metrics" => (
            200,
            "text/plain; version=0.0.4; charset=utf-8",
            state.prometheus_text(),
        ),
        _ => (404, "text/plain; charset=utf-8", "not found\n".to_string()),
    };

    let resp = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    timeout(IO_TIMEOUT, stream.write_all(resp.as_bytes()))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "health write timeout"))??;
    Ok(())
}

fn request_path(req: &str) -> Option<&str> {
    let line = req.lines().next()?;
    let mut parts = line.split_whitespace();
    parts.next()?; // method
    let target = parts.next()?;
    Some(target.split('?').next().unwrap_or(target))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DaemonConfig;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn get(addr: &str, path: &str) -> (u16, String) {
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

    #[tokio::test]
    async fn live_ready_metrics_endpoints() {
        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:0"
            [readiness]
            require_required_venues = false
            "#,
        )
        .unwrap();
        // bind 127.0.0.1:0 is valid loopback for config; listener picks ephemeral.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let state = DaemonState::new(cfg);
        let serve_state = Arc::clone(&state);
        tokio::spawn(async move {
            let _ = serve(listener, serve_state).await;
        });

        let (st, body) = get(&addr, "/live").await;
        assert_eq!(st, 503);
        assert!(body.contains("not live"));

        state.mark_supervisor_running();
        let (st, body) = get(&addr, "/live").await;
        assert_eq!(st, 200);
        assert!(body.contains("live"));

        let (st, body) = get(&addr, "/ready").await;
        assert_eq!(st, 200, "{body}");

        let (st, body) = get(&addr, "/metrics").await;
        assert_eq!(st, 200);
        assert!(body.contains("marketfeed_up 1"));
        assert!(body.contains("marketfeed_ready 1"));
    }

    #[tokio::test]
    async fn idle_connection_is_closed_by_read_timeout() {
        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:0"
            [readiness]
            require_required_venues = false
            "#,
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = DaemonState::new(cfg);
        tokio::spawn(async move {
            let _ = serve(listener, state).await;
        });

        let mut idle = TcpStream::connect(addr).await.unwrap();
        let mut byte = [0u8; 1];
        let result = timeout(IO_TIMEOUT + Duration::from_secs(1), idle.read(&mut byte))
            .await
            .expect("server must close idle connection within the deadline")
            .unwrap();
        assert_eq!(result, 0);
    }
}
