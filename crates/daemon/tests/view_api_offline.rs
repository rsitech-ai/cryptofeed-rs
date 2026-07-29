//! Offline view API: books + tape populate from synthetic memory venue.

#![cfg(feature = "ui-api")]

use std::sync::Arc;
use std::time::Duration;

use marketfeed_daemon::{DaemonConfig, DaemonState, serve_view, spawn_venues};
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
async fn synthetic_view_books_and_tape() {
    let cfg = DaemonConfig::from_toml_str(
        r#"
        [telemetry]
        bind = "127.0.0.1:0"
        ui_bind = "127.0.0.1:0"
        ui_tape_capacity = 64
        ui_tape_max_per_sec = 100
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
        let _ = serve_view(listener, serve_state).await;
    });

    wait_ready(&state, 3).await;

    let (st, body) = http_get(&addr, "/v1/status").await;
    assert_eq!(st, 200, "{body}");
    assert!(body.contains("\"live\":true"), "{body}");
    assert!(body.contains("synthetic-demo"), "{body}");

    let (st, body) = http_get(&addr, "/v1/instruments").await;
    assert_eq!(st, 200, "{body}");
    assert!(body.contains("BTC-USD"), "{body}");

    // Seeded book + continuous ticks should yield a book quickly.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let book_body = loop {
        let (st, body) =
            http_get(&addr, "/v1/books?venue=synthetic-demo&symbol=BTC-USD&depth=5").await;
        if st == 200 && body.contains("\"bids\"") && body.contains("100.00") {
            break body;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("book unavailable: status={st} body={body}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(book_body.contains("\"asks\""), "{book_body}");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let tape_body = loop {
        let (st, body) =
            http_get(&addr, "/v1/tape?venue=synthetic-demo&symbol=BTC-USD&limit=20").await;
        assert_eq!(st, 200, "{body}");
        if body.contains("\"kind\":\"trade\"") {
            break body;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("tape empty: {body}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(
        tape_body.contains("\"price\"") && tape_body.contains("entries"),
        "{tape_body}"
    );

    let _ = shutdown_tx.send(true);
    for h in handles {
        let _ = h.await;
    }
}

#[cfg(feature = "ui")]
#[tokio::test]
async fn spa_index_served() {
    let cfg = DaemonConfig::from_toml_str(
        r#"
        [telemetry]
        bind = "127.0.0.1:0"
        ui_bind = "127.0.0.1:0"
        [readiness]
        require_required_venues = false
        "#,
    )
    .unwrap();
    let state = DaemonState::new(cfg);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let serve_state = Arc::clone(&state);
    tokio::spawn(async move {
        let _ = serve_view(listener, serve_state).await;
    });

    let (st, body) = http_get(&addr, "/").await;
    assert_eq!(st, 200, "{body}");
    assert!(body.contains("marketfeed") || body.contains("app.js"), "{body}");

    let (st, body) = http_get(&addr, "/assets/app.js").await;
    assert_eq!(st, 200);
    assert!(body.contains("svelte") || body.contains("__svelte"), "not svelte build");
}
