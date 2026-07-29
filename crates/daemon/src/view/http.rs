//! Loopback HTTP for `/v1/*` (+ optional SPA under feature `ui`).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time::timeout;

use marketfeed_model::InstrumentId;

use crate::state::DaemonState;
use crate::view::ViewPlane;
use crate::view::replay::{list_replay_files, read_replay_entries};

const MAX_CONNECTIONS: usize = 64;
const IO_TIMEOUT: Duration = Duration::from_secs(2);
const STREAM_INTERVAL: Duration = Duration::from_millis(100);
const STREAM_HEARTBEAT: Duration = Duration::from_secs(15);
const STREAM_MAX_LIFE: Duration = Duration::from_secs(3600);
const MAX_HTTP_HEAD: usize = 8_192;
const MAX_POST_BODY: usize = 16_384;

pub async fn serve_view(listener: TcpListener, state: Arc<DaemonState>) -> std::io::Result<()> {
    let connections = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    loop {
        let (stream, _) = listener.accept().await?;
        let permit = Arc::clone(&connections)
            .acquire_owned()
            .await
            .map_err(|_| std::io::Error::other("view connection limiter closed"))?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(e) = handle_view_conn(stream, state).await {
                tracing::debug!(error = %e, "view conn closed");
            }
        });
    }
}

/// One HTTP connection on the view bind (includes SSE long-poll paths).
pub async fn handle_view_conn(stream: TcpStream, state: Arc<DaemonState>) -> std::io::Result<()> {
    handle_view_conn_with_prefix(stream, state, &[]).await
}

/// Shared-bind entry: request bytes already read by the health listener.
pub async fn handle_view_conn_with_prefix(
    mut stream: TcpStream,
    state: Arc<DaemonState>,
    prefix: &[u8],
) -> std::io::Result<()> {
    let request = timeout(IO_TIMEOUT, read_http_request(&mut stream, prefix))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "view request timeout"))??;
    if request.is_empty() {
        return Ok(());
    }
    let req = String::from_utf8_lossy(&request).into_owned();
    if prefix.is_empty() {
        state.http_requests.fetch_add(1, Ordering::Relaxed);
    }

    let (method, path, query) = parse_request(&req).unwrap_or(("GET", "/", ""));

    // SPA probes availability with HEAD before opening EventSource; answer 200
    // with SSE content-type (no body) so the panel prefers SSE over poll.
    if method == "HEAD" && path == "/v1/stream" {
        let resp = "HTTP/1.1 200 OK\r\n\
content-type: text/event-stream\r\n\
cache-control: no-store\r\n\
access-control-allow-origin: *\r\n\
content-length: 0\r\n\
connection: close\r\n\r\n";
        timeout(IO_TIMEOUT, stream.write_all(resp.as_bytes()))
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "view write timeout")
            })??;
        return Ok(());
    }
    if method == "GET" && path == "/v1/stream" {
        return handle_sse_stream(stream, &state, query).await;
    }
    if method == "POST" && path == "/v1/alerts/test" {
        return handle_alert_test(stream, &req, &state).await;
    }

    respond_view_request(&mut stream, &req, &state).await
}

async fn read_http_request(stream: &mut TcpStream, prefix: &[u8]) -> std::io::Result<Vec<u8>> {
    let max_request = MAX_HTTP_HEAD.saturating_add(MAX_POST_BODY);
    if prefix.len() > max_request {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "view request exceeds bounded request size",
        ));
    }
    let mut request = Vec::with_capacity(prefix.len().max(1024));
    request.extend_from_slice(prefix);

    loop {
        if let Some(head_end) = find_header_end(&request) {
            if head_end > MAX_HTTP_HEAD {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "view request headers too large",
                ));
            }
            let head = String::from_utf8_lossy(&request[..head_end]);
            let body_len = content_length(&head).unwrap_or(0);
            if body_len > MAX_POST_BODY {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "view request body too large",
                ));
            }
            let expected = head_end.checked_add(body_len).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "view request size overflow",
                )
            })?;
            if request.len() >= expected {
                request.truncate(expected);
                return Ok(request);
            }
        } else if request.len() >= MAX_HTTP_HEAD {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "view request headers too large",
            ));
        }

        let remaining = max_request.saturating_sub(request.len());
        if remaining == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "view request exceeds bounded request size",
            ));
        }
        let mut chunk = [0u8; 4096];
        let read_len = remaining.min(chunk.len());
        let n = timeout(IO_TIMEOUT, stream.read(&mut chunk[..read_len]))
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "view read timeout")
            })??;
        if n == 0 {
            if request.is_empty() {
                return Ok(request);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "view request ended before declared content length",
            ));
        }
        request.extend_from_slice(&chunk[..n]);
    }
}

fn find_header_end(request: &[u8]) -> Option<usize> {
    request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
}

/// Handle one already-read HTTP request on an open stream (shared with health bind).
pub async fn respond_view_request(
    stream: &mut TcpStream,
    req: &str,
    state: &DaemonState,
) -> std::io::Result<()> {
    let (method, path, query) = parse_request(req).unwrap_or(("GET", "/", ""));

    let (status, content_type, body) = match method {
        "HEAD" if path == "/v1/replay" => (200, "application/json; charset=utf-8", Vec::new()),
        "GET" | "HEAD" => route(state, path, query).await,
        "POST" if path == "/v1/alerts/test" => alert_test_route(state, request_body(req)).await,
        _ => (
            405,
            "text/plain; charset=utf-8",
            b"method not allowed\n".to_vec(),
        ),
    };

    let resp_head = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\naccess-control-allow-origin: *\r\ncache-control: no-store\r\nconnection: close\r\n\r\n",
        body.len()
    );
    timeout(IO_TIMEOUT, async {
        stream.write_all(resp_head.as_bytes()).await?;
        if method != "HEAD" {
            stream.write_all(&body).await?;
        }
        Ok::<(), std::io::Error>(())
    })
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "view write timeout"))??;
    Ok(())
}

async fn route(state: &DaemonState, path: &str, query: &str) -> (u16, &'static str, Vec<u8>) {
    let Some(view) = state.view.as_ref() else {
        return (
            503,
            "text/plain; charset=utf-8",
            b"view plane unavailable\n".to_vec(),
        );
    };

    match path {
        "/v1/status" => json_ok(&view.status(state)),
        "/v1/instruments" => json_ok(&view.instruments_json(state)),
        "/v1/books" => books(view, query),
        "/v1/tape" => tape(view, query),
        "/v1/replay/files" | "/v1/replay/list" => replay_files(state).await,
        "/v1/replay" => replay_read(state, query).await,
        "/live" => {
            if state.is_live() {
                (200, "text/plain; charset=utf-8", b"live\n".to_vec())
            } else {
                (503, "text/plain; charset=utf-8", b"not live\n".to_vec())
            }
        }
        "/ready" => {
            if state.is_ready() {
                (200, "text/plain; charset=utf-8", b"ready\n".to_vec())
            } else {
                (503, "text/plain; charset=utf-8", b"not ready\n".to_vec())
            }
        }
        "/metrics" => (
            200,
            "text/plain; version=0.0.4; charset=utf-8",
            state.prometheus_text().into_bytes(),
        ),
        _ => static_or_404(state, path),
    }
}

async fn replay_files(state: &DaemonState) -> (u16, &'static str, Vec<u8>) {
    let config = state.config.clone();
    match tokio::task::spawn_blocking(move || list_replay_files(&config)).await {
        Ok(Ok(resp)) => json_ok(&resp),
        Ok(Err(msg)) => {
            let body = serde_json::json!({ "error": msg }).to_string().into_bytes();
            (500, "application/json; charset=utf-8", body)
        }
        Err(error) => {
            let body = serde_json::json!({
                "error": format!("replay list worker failed: {error}")
            })
            .to_string()
            .into_bytes();
            (500, "application/json; charset=utf-8", body)
        }
    }
}

async fn replay_read(state: &DaemonState, query: &str) -> (u16, &'static str, Vec<u8>) {
    let params = Query::parse(query);
    let Some(file) = params.get("file").map(str::to_owned) else {
        return bad_request("missing file");
    };
    let offset = params
        .get("offset")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let limit = params
        .get("limit")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(100);
    let config = state.config.clone();
    let response =
        tokio::task::spawn_blocking(move || read_replay_entries(&config, &file, offset, limit))
            .await;
    let resp = match response {
        Ok(resp) => resp,
        Err(error) => {
            let body = serde_json::json!({
                "error": format!("replay read worker failed: {error}")
            })
            .to_string()
            .into_bytes();
            return (500, "application/json; charset=utf-8", body);
        }
    };
    if resp.error.is_some() {
        (404, "application/json; charset=utf-8", json_ok_body(&resp))
    } else {
        json_ok(&resp)
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
struct AlertTestBody {
    kind: String,
    #[serde(default)]
    bps: Option<f64>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct AlertTestResponse {
    ok: bool,
    forwarded: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    forward_error: Option<String>,
}

async fn handle_alert_test(
    mut stream: TcpStream,
    req: &str,
    state: &Arc<DaemonState>,
) -> std::io::Result<()> {
    let (status, _content_type, body) = alert_test_route(state, request_body(req)).await;
    let resp_head = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json; charset=utf-8\r\ncontent-length: {}\r\naccess-control-allow-origin: *\r\ncache-control: no-store\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(resp_head.as_bytes()).await?;
    stream.write_all(&body).await?;
    Ok(())
}

async fn alert_test_route(state: &DaemonState, body: Option<&str>) -> (u16, &'static str, Vec<u8>) {
    let Some(raw) = body else {
        return bad_request("missing body");
    };
    let parsed: AlertTestBody = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(e) => return bad_request(&format!("invalid json: {e}")),
    };
    if parsed.kind != "discrepancy" && parsed.kind != "lag" {
        return bad_request("kind must be discrepancy or lag");
    }

    let webhook = state
        .config
        .telemetry
        .alert_webhook_url
        .as_deref()
        .filter(|u| !u.trim().is_empty());
    let mut forwarded = false;
    let mut forward_error = None;

    if let Some(url) = webhook {
        let payload = serde_json::json!({
            "kind": parsed.kind,
            "bps": parsed.bps,
            "message": parsed.message,
            "source": "marketfeed-daemon",
        });
        match forward_alert_webhook(url, &payload).await {
            Ok(()) => forwarded = true,
            Err(e) => forward_error = Some(e),
        }
    }

    json_ok(&AlertTestResponse {
        ok: true,
        forwarded,
        forward_error,
    })
}

async fn forward_alert_webhook(url: &str, payload: &serde_json::Value) -> Result<(), String> {
    let body = payload.to_string();
    let parsed = parse_http_url(url)?;
    let mut stream = TcpStream::connect(parsed.addr)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let req = format!(
        "POST {} HTTP/1.1\r\nhost: {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        parsed.path,
        parsed.host,
        body.len(),
        body
    );
    timeout(Duration::from_secs(5), stream.write_all(req.as_bytes()))
        .await
        .map_err(|_| "write timeout".to_string())?
        .map_err(|e| format!("write: {e}"))?;
    let mut buf = vec![0u8; 1024];
    let n = timeout(Duration::from_secs(5), stream.read(&mut buf))
        .await
        .map_err(|_| "read timeout".to_string())?
        .map_err(|e| format!("read: {e}"))?;
    let resp = String::from_utf8_lossy(&buf[..n]);
    let status_line = resp.lines().next().unwrap_or("");
    if status_line.contains(" 2") {
        Ok(())
    } else {
        Err(format!("webhook status: {status_line}"))
    }
}

struct ParsedHttpUrl {
    addr: String,
    host: String,
    path: String,
}

fn parse_http_url(url: &str) -> Result<ParsedHttpUrl, String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| "only http:// URLs supported".to_string())?;
    let (authority, path) = match rest.split_once('/') {
        Some((a, p)) => (a, format!("/{p}")),
        None => (rest, "/".to_string()),
    };
    Ok(ParsedHttpUrl {
        addr: authority.to_string(),
        host: authority.to_string(),
        path,
    })
}

#[derive(Debug, Clone)]
struct StreamFocus {
    venue: String,
    instrument: InstrumentId,
    symbol: Option<String>,
}

fn resolve_stream_focus(state: &DaemonState, query: &str) -> Option<StreamFocus> {
    let view = state.view.as_ref()?;
    let params = Query::parse(query);
    if let (Some(venue), Some(symbol)) = (params.get("venue"), params.get("symbol")) {
        let instrument = view.resolve_instrument(venue, symbol)?;
        return Some(StreamFocus {
            venue: venue.to_string(),
            instrument,
            symbol: Some(symbol.to_string()),
        });
    }
    if let Some(asset) = params.get("asset") {
        for v in &state.config.venues {
            for (i, sym) in v.symbols.iter().enumerate() {
                if sym.contains(asset) {
                    return Some(StreamFocus {
                        venue: v.id.clone(),
                        instrument: InstrumentId((i as u32) + 1),
                        symbol: Some(sym.clone()),
                    });
                }
            }
            if v.symbols.is_empty() && v.adapter == "synthetic" && asset == "BTC" {
                return Some(StreamFocus {
                    venue: v.id.clone(),
                    instrument: InstrumentId(1),
                    symbol: Some("BTC-USD".into()),
                });
            }
        }
    }
    state.config.venues.first().map(|v| StreamFocus {
        venue: v.id.clone(),
        instrument: InstrumentId(1),
        symbol: v.symbols.first().cloned(),
    })
}

async fn handle_sse_stream(
    mut stream: TcpStream,
    state: &Arc<DaemonState>,
    query: &str,
) -> std::io::Result<()> {
    let Some(view) = state.view.as_ref() else {
        let body = b"view plane unavailable\n";
        let resp = format!(
            "HTTP/1.1 503 Service Unavailable\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(resp.as_bytes()).await?;
        stream.write_all(body).await?;
        return Ok(());
    };

    let focus = resolve_stream_focus(state, query);
    let headers = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-store\r\naccess-control-allow-origin: *\r\ntransfer-encoding: chunked\r\nconnection: keep-alive\r\n\r\n";
    stream.write_all(headers.as_bytes()).await?;

    let started = Instant::now();
    let mut last_heartbeat = Instant::now();
    let mut last_payload = String::new();

    loop {
        if started.elapsed() >= STREAM_MAX_LIFE {
            break;
        }

        if last_heartbeat.elapsed() >= STREAM_HEARTBEAT {
            write_sse_comment(&mut stream, "heartbeat").await?;
            last_heartbeat = Instant::now();
        }

        let status = view.status(state);
        let mut payload = serde_json::json!({
            "status": status,
        });
        if let Some(f) = &focus {
            // Deep L2 (not BBO-only) so Order Flow heatmap / DOM / COB have real walls.
            payload["focus"] = view.stream_focus(&f.venue, f.instrument, Some(48), 200);
            if let Some(sym) = &f.symbol {
                payload["focus"]["symbol"] = serde_json::json!(sym);
            }
        }

        let state_serialized = match serde_json::to_string(&payload) {
            Ok(s) => s,
            Err(_) => {
                tokio::time::sleep(STREAM_INTERVAL).await;
                continue;
            }
        };
        if state_serialized != last_payload {
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0);
            payload["ts_ns"] = serde_json::json!(now_ns);
            let serialized = serde_json::to_string(&payload).unwrap_or(state_serialized.clone());
            write_sse_data(&mut stream, &serialized).await?;
            last_payload = state_serialized;
        }

        tokio::time::sleep(STREAM_INTERVAL).await;
    }

    write_chunk(&mut stream, b"").await?;
    Ok(())
}

async fn write_chunk(stream: &mut TcpStream, data: &[u8]) -> std::io::Result<()> {
    let header = format!("{:x}\r\n", data.len());
    stream.write_all(header.as_bytes()).await?;
    if !data.is_empty() {
        stream.write_all(data).await?;
        stream.write_all(b"\r\n").await?;
    } else {
        stream.write_all(b"\r\n").await?;
    }
    Ok(())
}

async fn write_sse_data(stream: &mut TcpStream, json: &str) -> std::io::Result<()> {
    let event = format!("data: {json}\n\n");
    write_chunk(stream, event.as_bytes()).await
}

async fn write_sse_comment(stream: &mut TcpStream, comment: &str) -> std::io::Result<()> {
    let event = format!(": {comment}\n\n");
    write_chunk(stream, event.as_bytes()).await
}

fn books(view: &ViewPlane, query: &str) -> (u16, &'static str, Vec<u8>) {
    let params = Query::parse(query);
    let Some(venue) = params.get("venue") else {
        return bad_request("missing venue");
    };
    let depth = params
        .get("depth")
        .and_then(|s| s.parse::<u32>().ok())
        .or(Some(25));
    let instrument = match resolve_instrument(view, venue, &params) {
        Ok(id) => id,
        Err(msg) => return bad_request(msg),
    };
    match view.book_snapshot(venue, instrument, depth) {
        Some(snap) => json_ok(&snap),
        None => (
            404,
            "application/json; charset=utf-8",
            br#"{"error":"book unavailable"}"#.to_vec(),
        ),
    }
}

fn tape(view: &ViewPlane, query: &str) -> (u16, &'static str, Vec<u8>) {
    let params = Query::parse(query);
    let Some(venue) = params.get("venue") else {
        return bad_request("missing venue");
    };
    let limit = params
        .get("limit")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(50)
        .clamp(1, 500);
    let kind = params.get("kind").filter(|k| {
        matches!(
            *k,
            "trade" | "trades" | "quote" | "quotes" | "all" | "mixed"
        )
    });
    let kind = match kind {
        Some("all") | Some("mixed") => None,
        other => other,
    };
    let instrument = match resolve_instrument(view, venue, &params) {
        Ok(id) => id,
        Err(msg) => return bad_request(msg),
    };
    let entries = view.tape_filtered(venue, instrument, limit, kind);
    json_ok(&serde_json::json!({
        "venue": venue,
        "instrument": instrument.0,
        "kind": kind.unwrap_or("all"),
        "entries": entries,
    }))
}

fn resolve_instrument(
    view: &ViewPlane,
    venue: &str,
    params: &Query,
) -> Result<InstrumentId, &'static str> {
    if let Some(raw) = params.get("instrument") {
        let id = raw.parse::<u32>().map_err(|_| "invalid instrument")?;
        return Ok(InstrumentId(id));
    }
    if let Some(sym) = params.get("symbol") {
        return view.resolve_instrument(venue, sym).ok_or("unknown symbol");
    }
    Ok(InstrumentId(1))
}

fn static_or_404(state: &DaemonState, path: &str) -> (u16, &'static str, Vec<u8>) {
    #[cfg(feature = "ui")]
    {
        if let Some((ct, body)) = load_static(state, path) {
            return (200, ct, body);
        }
    }
    #[cfg(not(feature = "ui"))]
    {
        let _ = state;
        let _ = path;
    }
    (404, "text/plain; charset=utf-8", b"not found\n".to_vec())
}

#[cfg(feature = "ui")]
fn load_static(state: &DaemonState, path: &str) -> Option<(&'static str, Vec<u8>)> {
    let rel = match path {
        "/" | "/index.html" => "index.html",
        p if p.starts_with('/') => &p[1..],
        _ => return None,
    };
    if !is_safe_static_rel(rel) {
        return None;
    }
    if let Some(dir) = state.config.telemetry.ui_static_dir.as_deref() {
        if let Some(bytes) = read_static_under_root(dir, rel) {
            return Some((content_type(rel), bytes));
        }
    }
    embedded_asset(rel)
}

#[cfg(feature = "ui")]
fn is_safe_static_rel(rel: &str) -> bool {
    if rel.is_empty() || rel.starts_with('/') || rel.starts_with('\\') {
        return false;
    }
    if rel.contains('\0') || rel.contains("..") || rel.contains('\\') {
        return false;
    }
    use std::path::{Component, Path};
    !Path::new(rel).components().any(|c| {
        matches!(
            c,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    })
}

#[cfg(feature = "ui")]
fn read_static_under_root(root: &str, rel: &str) -> Option<Vec<u8>> {
    use std::path::Path;
    let root = Path::new(root);
    let candidate = root.join(rel);
    let root_canon = root.canonicalize().ok()?;
    let file_canon = candidate.canonicalize().ok()?;
    if !file_canon.starts_with(&root_canon) {
        return None;
    }
    std::fs::read(file_canon).ok()
}

#[cfg(feature = "ui")]
fn embedded_asset(rel: &str) -> Option<(&'static str, Vec<u8>)> {
    match rel {
        "index.html" => Some((
            "text/html; charset=utf-8",
            include_bytes!("../../../../ui/dist/index.html").to_vec(),
        )),
        "assets/app.js" => Some((
            "application/javascript; charset=utf-8",
            include_bytes!("../../../../ui/dist/assets/app.js").to_vec(),
        )),
        "assets/app.css" => Some((
            "text/css; charset=utf-8",
            include_bytes!("../../../../ui/dist/assets/app.css").to_vec(),
        )),
        _ => None,
    }
}

#[cfg(feature = "ui")]
fn content_type(rel: &str) -> &'static str {
    if rel.ends_with(".js") {
        "application/javascript; charset=utf-8"
    } else if rel.ends_with(".css") {
        "text/css; charset=utf-8"
    } else if rel.ends_with(".svg") {
        "image/svg+xml"
    } else if rel.ends_with(".html") {
        "text/html; charset=utf-8"
    } else {
        "application/octet-stream"
    }
}

fn json_ok<T: serde::Serialize>(value: &T) -> (u16, &'static str, Vec<u8>) {
    (200, "application/json; charset=utf-8", json_ok_body(value))
}

fn json_ok_body<T: serde::Serialize>(value: &T) -> Vec<u8> {
    match serde_json::to_vec(value) {
        Ok(body) => body,
        Err(_) => br#"{"error":"serialize"}"#.to_vec(),
    }
}

fn bad_request(msg: &str) -> (u16, &'static str, Vec<u8>) {
    let body = serde_json::json!({ "error": msg }).to_string().into_bytes();
    (400, "application/json; charset=utf-8", body)
}

fn parse_request(req: &str) -> Option<(&str, &str, &str)> {
    let line = req.lines().next()?;
    let mut parts = line.split_whitespace();
    let method = parts.next()?;
    let target = parts.next()?;
    let mut t = target.splitn(2, '?');
    let path = t.next()?;
    let query = t.next().unwrap_or("");
    Some((method, path, query))
}

fn request_body(req: &str) -> Option<&str> {
    let (_head, body) = req.split_once("\r\n\r\n")?;
    let len = content_length(req).unwrap_or(body.len());
    if len > MAX_POST_BODY {
        return None;
    }
    if body.len() < len {
        return None;
    }
    Some(&body[..len])
}

fn content_length(req: &str) -> Option<usize> {
    for line in req.lines().skip(1) {
        if line.is_empty() {
            break;
        }
        let lower = line.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-length:") {
            return v.trim().parse().ok();
        }
    }
    None
}

#[derive(Debug, Default)]
struct Query {
    pairs: Vec<(String, String)>,
}

impl Query {
    fn parse(raw: &str) -> Self {
        let mut pairs = Vec::new();
        for part in raw.split('&') {
            if part.is_empty() {
                continue;
            }
            let mut kv = part.splitn(2, '=');
            let k = kv.next().unwrap_or("");
            let v = kv.next().unwrap_or("");
            pairs.push((percent_decode(k), percent_decode(v)));
        }
        Self { pairs }
    }

    fn get(&self, key: &str) -> Option<&str> {
        self.pairs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let h = hex(bytes[i + 1]);
                let l = hex(bytes[i + 2]);
                if let (Some(h), Some(l)) = (h, l) {
                    out.push((h << 4) | l);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DaemonConfig;

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

    async fn post_json(addr: &str, path: &str, body: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "POST {path} HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
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
        let resp_body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
        (status, resp_body)
    }

    fn test_state(cfg: DaemonConfig) -> Arc<DaemonState> {
        let state = DaemonState::new(cfg);
        state.mark_supervisor_running();
        state
    }

    #[tokio::test]
    async fn status_endpoint_enriched_json() {
        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:0"
            ui_bind = "127.0.0.1:0"
            grafana_base_url = "http://127.0.0.1:3000"
            alert_webhook_url = "http://127.0.0.1:9999/hook"
            [readiness]
            require_required_venues = false
            [[venues]]
            id = "syn"
            adapter = "synthetic"
            required = false
            "#,
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let state = test_state(cfg);
        let serve_state = Arc::clone(&state);
        tokio::spawn(async move {
            let _ = serve_view(listener, serve_state).await;
        });

        let (st, body) = get(&addr, "/v1/status").await;
        assert_eq!(st, 200, "{body}");
        assert!(body.contains("\"live\":true"), "{body}");
        assert!(body.contains("\"ready\":true"), "{body}");
        assert!(body.contains("\"grafana_base_url\""), "{body}");
        assert!(body.contains("\"alert_webhook_configured\":true"), "{body}");
        assert!(body.contains("\"tape_trades\""), "{body}");
    }

    #[tokio::test]
    async fn replay_files_empty_ok() {
        let dir = std::env::temp_dir().join(format!(
            "marketfeed-replay-http-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let cfg = DaemonConfig::from_toml_str(&format!(
            r#"
            [telemetry]
            bind = "127.0.0.1:0"
            ui_bind = "127.0.0.1:0"
            replay_dir = "{}"
            [readiness]
            require_required_venues = false
            "#,
            dir.display()
        ))
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let state = test_state(cfg);
        tokio::spawn(async move {
            let _ = serve_view(listener, state).await;
        });

        let (st, body) = get(&addr, "/v1/replay/files").await;
        assert_eq!(st, 200, "{body}");
        assert!(body.contains("\"files\":[]"), "{body}");
    }

    #[tokio::test]
    async fn alert_test_ack_without_webhook() {
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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let state = test_state(cfg);
        tokio::spawn(async move {
            let _ = serve_view(listener, state).await;
        });

        let (st, body) = post_json(
            &addr,
            "/v1/alerts/test",
            r#"{"kind":"lag","bps":12.5,"message":"test"}"#,
        )
        .await;
        assert_eq!(st, 200, "{body}");
        assert!(body.contains("\"ok\":true"), "{body}");
        assert!(body.contains("\"forwarded\":false"), "{body}");
    }

    #[tokio::test]
    async fn fragmented_post_waits_for_declared_content_length() {
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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let state = test_state(cfg);
        tokio::spawn(async move {
            let _ = serve_view(listener, state).await;
        });

        let body = r#"{"kind":"lag","message":"fragmented"}"#;
        let mut stream = TcpStream::connect(&addr).await.unwrap();
        let head = format!(
            "POST /v1/alerts/test HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let split = body.len() / 2;
        stream.write_all(&body.as_bytes()[..split]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;
        stream.write_all(&body.as_bytes()[split..]).await.unwrap();

        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        let text = String::from_utf8_lossy(&response);
        assert!(text.starts_with("HTTP/1.1 200"), "{text}");
        assert!(text.contains("\"ok\":true"), "{text}");
    }

    #[tokio::test]
    async fn replay_head_and_ui_list_route_are_available() {
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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let state = test_state(cfg);
        tokio::spawn(async move {
            let _ = serve_view(listener, state).await;
        });

        let mut stream = TcpStream::connect(&addr).await.unwrap();
        stream
            .write_all(b"HEAD /v1/replay HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        let text = String::from_utf8_lossy(&response);
        assert!(text.starts_with("HTTP/1.1 200"), "{text}");

        let (status, body) = get(&addr, "/v1/replay/list").await;
        assert_eq!(status, 200, "{body}");
        assert!(body.contains("\"files\""), "{body}");
    }

    #[tokio::test]
    async fn alert_test_forwards_to_mock_webhook() {
        let hook = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hook_addr = hook.local_addr().unwrap();
        let hook_url = format!("http://{hook_addr}/hook");
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = hook.accept().await {
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                assert!(req.contains("POST /hook"), "{req}");
                assert!(req.contains("\"kind\":\"discrepancy\""), "{req}");
                let resp =
                    "HTTP/1.1 204 No Content\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
                let _ = stream.write_all(resp.as_bytes()).await;
            }
        });

        let cfg = DaemonConfig::from_toml_str(&format!(
            r#"
            [telemetry]
            bind = "127.0.0.1:0"
            ui_bind = "127.0.0.1:0"
            alert_webhook_url = "{hook_url}"
            [readiness]
            require_required_venues = false
            "#,
        ))
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let state = test_state(cfg);
        tokio::spawn(async move {
            let _ = serve_view(listener, state).await;
        });

        let (st, body) = post_json(
            &addr,
            "/v1/alerts/test",
            r#"{"kind":"discrepancy","bps":3.1,"message":"x"}"#,
        )
        .await;
        assert_eq!(st, 200, "{body}");
        assert!(body.contains("\"forwarded\":true"), "{body}");
    }

    #[tokio::test]
    async fn sse_stream_head_probe_returns_200() {
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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let state = test_state(cfg);
        tokio::spawn(async move {
            let _ = serve_view(listener, state).await;
        });

        let mut stream = TcpStream::connect(&addr).await.unwrap();
        let req = "HEAD /v1/stream?probe=1 HTTP/1.1\r\nhost: localhost\r\n\r\n";
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = vec![0u8; 1024];
        let n = timeout(Duration::from_secs(2), stream.read(&mut buf))
            .await
            .expect("read timeout")
            .expect("read");
        let text = String::from_utf8_lossy(&buf[..n]);
        assert!(text.contains("200 OK"), "{text}");
        assert!(text.contains("text/event-stream"), "{text}");
    }

    #[tokio::test]
    async fn sse_stream_emits_initial_event() {
        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:0"
            ui_bind = "127.0.0.1:0"
            [readiness]
            require_required_venues = false
            [[venues]]
            id = "syn"
            adapter = "synthetic"
            required = false
            symbols = ["BTC-USD"]
            "#,
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let state = test_state(cfg);
        tokio::spawn(async move {
            let _ = serve_view(listener, state).await;
        });

        let mut stream = TcpStream::connect(&addr).await.unwrap();
        let req = "GET /v1/stream?asset=BTC HTTP/1.1\r\nhost: localhost\r\n\r\n";
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = vec![0u8; 16384];
        let mut total = 0usize;
        let deadline = Instant::now() + Duration::from_secs(3);
        let text = loop {
            if Instant::now() >= deadline {
                panic!("timed out waiting for SSE data");
            }
            let n = timeout(Duration::from_millis(500), stream.read(&mut buf[total..]))
                .await
                .expect("read timeout")
                .expect("read");
            if n == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            }
            total += n;
            let text = String::from_utf8_lossy(&buf[..total]);
            if text.contains("data:") {
                break text.into_owned();
            }
        };
        assert!(text.contains("200 OK"), "{text}");
        assert!(text.contains("text/event-stream"), "{text}");
        assert!(text.contains("data:"), "{text}");
        assert!(text.contains("\"status\""), "{text}");
    }

    #[tokio::test]
    async fn sse_does_not_emit_every_tick_when_only_clock_fields_change() {
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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let state = test_state(cfg);
        tokio::spawn(async move {
            let _ = serve_view(listener, state).await;
        });

        let mut stream = TcpStream::connect(&addr).await.unwrap();
        stream
            .write_all(b"GET /v1/stream HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .await
            .unwrap();
        let deadline = Instant::now() + Duration::from_millis(450);
        let mut response = Vec::new();
        let mut buf = [0u8; 4096];
        while Instant::now() < deadline {
            match timeout(Duration::from_millis(75), stream.read(&mut buf)).await {
                Ok(Ok(0)) => break,
                Ok(Ok(n)) => response.extend_from_slice(&buf[..n]),
                Ok(Err(e)) => panic!("stream read failed: {e}"),
                Err(_) => {}
            }
        }
        let text = String::from_utf8_lossy(&response);
        let events = text.matches("data:").count();
        assert!((1..=2).contains(&events), "{text}");
    }

    #[cfg(feature = "ui")]
    #[test]
    fn rejects_unsafe_static_rel_paths() {
        assert!(is_safe_static_rel("index.html"));
        assert!(is_safe_static_rel("assets/app.js"));
        assert!(!is_safe_static_rel("../etc/passwd"));
        assert!(!is_safe_static_rel("assets/../../etc/passwd"));
        assert!(!is_safe_static_rel("/etc/passwd"));
        assert!(!is_safe_static_rel("assets\\app.js"));
        assert!(!is_safe_static_rel(""));
    }

    #[cfg(feature = "ui")]
    #[test]
    fn static_root_read_stays_under_dir() {
        let dir = tempfile_dir();
        std::fs::write(dir.join("ok.js"), b"ok").unwrap();
        std::fs::write(dir.join("secret.txt"), b"secret").unwrap();
        let outside = dir.parent().unwrap().join("outside.txt");
        std::fs::write(&outside, b"leak").unwrap();

        let root = dir.to_str().unwrap();
        assert_eq!(
            read_static_under_root(root, "ok.js").as_deref(),
            Some(b"ok".as_slice())
        );
        assert!(read_static_under_root(root, "../outside.txt").is_none());
        #[cfg(unix)]
        {
            let link = dir.join("escape.js");
            let _ = std::fs::remove_file(&link);
            if std::os::unix::fs::symlink(&outside, &link).is_ok() {
                assert!(
                    read_static_under_root(root, "escape.js").is_none(),
                    "symlink escape must be rejected"
                );
            }
        }
        let _ = outside;
    }

    #[cfg(feature = "ui")]
    fn tempfile_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "marketfeed-ui-static-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
