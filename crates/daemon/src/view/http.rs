//! Loopback HTTP for `/v1/*` (+ optional SPA under feature `ui`).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time::timeout;

use marketfeed_model::InstrumentId;

use crate::state::DaemonState;
use crate::view::ViewPlane;

const MAX_CONNECTIONS: usize = 64;
const IO_TIMEOUT: Duration = Duration::from_secs(2);

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
            if let Err(e) = handle_conn(stream, state).await {
                tracing::debug!(error = %e, "view conn closed");
            }
        });
    }
}

async fn handle_conn(mut stream: TcpStream, state: Arc<DaemonState>) -> std::io::Result<()> {
    let mut buf = vec![0u8; 8192];
    let n = timeout(IO_TIMEOUT, stream.read(&mut buf))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "view read timeout"))??;
    if n == 0 {
        return Ok(());
    }
    let req = String::from_utf8_lossy(&buf[..n]);
    state.http_requests.fetch_add(1, Ordering::Relaxed);
    respond_view_request(&mut stream, &req, &state).await
}

/// Handle one already-read HTTP request on an open stream (shared with health bind).
pub async fn respond_view_request(
    stream: &mut TcpStream,
    req: &str,
    state: &DaemonState,
) -> std::io::Result<()> {
    let (method, path, query) = parse_request(req).unwrap_or(("GET", "/", ""));

    let (status, content_type, body) = if method != "GET" && method != "HEAD" {
        (
            405,
            "text/plain; charset=utf-8",
            b"method not allowed\n".to_vec(),
        )
    } else {
        route(state, path, query).await
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
        return view
            .resolve_instrument(venue, sym)
            .ok_or("unknown symbol");
    }
    // Default to first instrument (1) for single-symbol venues / synthetic.
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

/// Reject path escapes before joining onto `ui_static_dir`.
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

/// Read `rel` only when the resolved path stays under `root` (symlink-safe).
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
    // Fixed Vite output names (see ui/vite.config.js).
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
    match serde_json::to_vec(value) {
        Ok(body) => (200, "application/json; charset=utf-8", body),
        Err(_) => (
            500,
            "application/json; charset=utf-8",
            br#"{"error":"serialize"}"#.to_vec(),
        ),
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

    #[tokio::test]
    async fn status_endpoint_json() {
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
        let state = DaemonState::new(cfg);
        state.mark_supervisor_running();
        let serve_state = Arc::clone(&state);
        tokio::spawn(async move {
            let _ = serve_view(listener, serve_state).await;
        });

        let (st, body) = get(&addr, "/v1/status").await;
        assert_eq!(st, 200, "{body}");
        assert!(body.contains("\"live\":true"), "{body}");
        assert!(body.contains("\"ready\":true"), "{body}");
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
        // Symlink escape (when the OS allows creating one).
        let link = dir.join("escape.js");
        #[cfg(unix)]
        {
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
