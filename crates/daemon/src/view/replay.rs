//! Best-effort replay file listing and JSONL/MFNE tape read (loopback only).

use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Component, Path, PathBuf};

use serde::Serialize;
use serde_json::Value;

use crate::config::DaemonConfig;

const DEFAULT_REPLAY_DIR: &str = ".local/live-ui/raw";
const MAX_REPLAY_RECORD_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Serialize)]
pub struct ReplayFileInfo {
    pub name: String,
    pub size_bytes: u64,
    pub format: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplayFilesResponse {
    pub directory: String,
    pub files: Vec<ReplayFileInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplayEntriesResponse {
    pub file: String,
    pub offset: u64,
    pub limit: u64,
    pub total: u64,
    pub entries: Vec<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub fn replay_root(config: &DaemonConfig) -> PathBuf {
    config
        .telemetry
        .replay_dir
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_REPLAY_DIR))
}

pub fn list_replay_files(config: &DaemonConfig) -> Result<ReplayFilesResponse, String> {
    let root = replay_root(config);
    let directory = root.to_string_lossy().into_owned();
    if !root.exists() {
        return Ok(ReplayFilesResponse {
            directory,
            files: Vec::new(),
        });
    }
    let read_dir = std::fs::read_dir(&root).map_err(|e| format!("read replay dir: {e}"))?;
    let mut files = Vec::new();
    for entry in read_dir {
        let entry = entry.map_err(|e| format!("read replay entry: {e}"))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let format = if name.ends_with(".jsonl") {
            "jsonl"
        } else if name.ends_with(".mfne") {
            "mfne"
        } else {
            continue;
        };
        let size_bytes = entry
            .metadata()
            .map_err(|e| format!("metadata {}: {e}", name))?
            .len();
        files.push(ReplayFileInfo {
            name: name.to_string(),
            format: format.into(),
            size_bytes,
        });
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(ReplayFilesResponse { directory, files })
}

pub fn read_replay_entries(
    config: &DaemonConfig,
    file: &str,
    offset: u64,
    limit: u64,
) -> ReplayEntriesResponse {
    let limit = limit.clamp(1, 500);
    let root = replay_root(config);
    let path = match resolve_replay_file(&root, file) {
        Ok(p) => p,
        Err(msg) => {
            return ReplayEntriesResponse {
                file: file.to_string(),
                offset,
                limit,
                total: 0,
                entries: Vec::new(),
                error: Some(msg),
            };
        }
    };
    let (total, entries) = match scan_replay_file(&path, file, offset, limit) {
        Ok(result) => result,
        Err(msg) => {
            return ReplayEntriesResponse {
                file: file.to_string(),
                offset,
                limit,
                total: 0,
                entries: Vec::new(),
                error: Some(msg),
            };
        }
    };
    ReplayEntriesResponse {
        file: file.to_string(),
        offset,
        limit,
        total,
        entries,
        error: None,
    }
}

fn scan_replay_file(
    path: &Path,
    file: &str,
    offset: u64,
    limit: u64,
) -> Result<(u64, Vec<Value>), String> {
    let input = File::open(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let mut reader = BufReader::new(input);
    if file.ends_with(".mfne") {
        scan_length_prefixed(&mut reader, offset, limit)
    } else {
        scan_jsonl(&mut reader, offset, limit)
    }
}

fn scan_jsonl<R: BufRead>(
    reader: &mut R,
    offset: u64,
    limit: u64,
) -> Result<(u64, Vec<Value>), String> {
    let mut record = Vec::new();
    let mut total = 0u64;
    let mut entries = Vec::with_capacity(limit as usize);
    loop {
        let read = read_bounded_line(reader, &mut record, MAX_REPLAY_RECORD_BYTES)?;
        if read == 0 {
            break;
        }
        let line = trim_ascii_whitespace(&record);
        if line.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_slice(line)
            .map_err(|e| format!("parse jsonl record {}: {e}", total.saturating_add(1)))?;
        collect_page_entry(&mut entries, &value, total, offset, limit);
        total = total.saturating_add(1);
    }
    Ok((total, entries))
}

fn scan_length_prefixed<R: Read>(
    reader: &mut R,
    offset: u64,
    limit: u64,
) -> Result<(u64, Vec<Value>), String> {
    let mut total = 0u64;
    let mut entries = Vec::with_capacity(limit as usize);
    loop {
        let mut length = [0u8; 4];
        match reader.read(&mut length[..1]) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => return Err(format!("read mfne length: {e}")),
        }
        reader
            .read_exact(&mut length[1..])
            .map_err(|e| format!("truncated mfne length prefix: {e}"))?;
        let record_len = u32::from_le_bytes(length) as usize;
        if record_len > MAX_REPLAY_RECORD_BYTES {
            return Err(format!(
                "replay record exceeds {MAX_REPLAY_RECORD_BYTES} bytes"
            ));
        }
        let mut record = vec![0u8; record_len];
        reader
            .read_exact(&mut record)
            .map_err(|e| format!("truncated mfne record body: {e}"))?;
        let value: Value = serde_json::from_slice(&record)
            .map_err(|e| format!("parse mfne record {}: {e}", total.saturating_add(1)))?;
        collect_page_entry(&mut entries, &value, total, offset, limit);
        total = total.saturating_add(1);
    }
    Ok((total, entries))
}

fn collect_page_entry(
    entries: &mut Vec<Value>,
    envelope: &Value,
    index: u64,
    offset: u64,
    limit: u64,
) {
    if index >= offset && index < offset.saturating_add(limit) {
        if let Some(entry) = envelope_to_tape_entry(envelope) {
            entries.push(entry);
        }
    }
}

fn read_bounded_line<R: BufRead>(
    reader: &mut R,
    record: &mut Vec<u8>,
    max_len: usize,
) -> Result<usize, String> {
    record.clear();
    loop {
        let available = reader
            .fill_buf()
            .map_err(|e| format!("read replay record: {e}"))?;
        if available.is_empty() {
            return Ok(record.len());
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |position| position + 1);
        if record.len().saturating_add(take) > max_len {
            return Err(format!("replay record exceeds {max_len} bytes"));
        }
        record.extend_from_slice(&available[..take]);
        let found_delimiter = available.get(take.saturating_sub(1)) == Some(&b'\n');
        reader.consume(take);
        if found_delimiter {
            return Ok(record.len());
        }
    }
}

fn trim_ascii_whitespace(mut bytes: &[u8]) -> &[u8] {
    while bytes.first().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[1..];
    }
    while bytes.last().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

/// Resolve `file` under `root`; reject path traversal.
fn resolve_replay_file(root: &Path, file: &str) -> Result<PathBuf, String> {
    if file.is_empty() || file.contains('\0') || file.contains("..") {
        return Err("invalid file name".into());
    }
    let rel = Path::new(file);
    if rel.components().any(|c| {
        matches!(
            c,
            Component::RootDir | Component::Prefix(_) | Component::ParentDir
        )
    }) {
        return Err("invalid file path".into());
    }
    let candidate = root.join(rel);
    let root_canon = root
        .canonicalize()
        .map_err(|e| format!("replay dir unavailable: {e}"))?;
    let file_canon = candidate
        .canonicalize()
        .map_err(|e| format!("file not found: {e}"))?;
    if !file_canon.starts_with(&root_canon) {
        return Err("file outside replay directory".into());
    }
    if !file_canon.is_file() {
        return Err("not a regular file".into());
    }
    Ok(file_canon)
}

/// Best-effort MFNE/MFPE envelope → tape-like JSON for the SPA scrubber.
fn envelope_to_tape_entry(env: &Value) -> Option<Value> {
    let payload = env.get("payload")?;
    if let Some(trade) = payload.get("trade") {
        let price = fixed_json_to_string(trade.get("price")?)?;
        let quantity = fixed_json_to_string(trade.get("quantity")?)?;
        let notional = notional_from_strings(&price, &quantity);
        let receive_ts_ns = env
            .pointer("/receive_ts/ns")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        return Some(serde_json::json!({
            "kind": "trade",
            "price": price,
            "quantity": quantity,
            "notional": notional,
            "aggressor": trade.get("aggressor").and_then(|v| v.as_str()).unwrap_or("unknown"),
            "trade_id": trade.get("trade_id").and_then(|v| v.as_str()),
            "exchange_ts_ns": env.pointer("/exchange_ts/ns").and_then(|v| v.as_i64()),
            "receive_ts_ns": receive_ts_ns,
        }));
    }
    if let Some(quote) = payload.get("quote") {
        let receive_ts_ns = env
            .pointer("/receive_ts/ns")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        return Some(serde_json::json!({
            "kind": "quote",
            "bid_price": fixed_json_to_string(quote.get("bid_price")?)?,
            "bid_quantity": quote.get("bid_quantity").and_then(fixed_json_to_string),
            "ask_price": fixed_json_to_string(quote.get("ask_price")?)?,
            "ask_quantity": quote.get("ask_quantity").and_then(fixed_json_to_string),
            "exchange_ts_ns": env.pointer("/exchange_ts/ns").and_then(|v| v.as_i64()),
            "receive_ts_ns": receive_ts_ns,
        }));
    }
    None
}

fn fixed_json_to_string(v: &Value) -> Option<String> {
    let inner = v.get("value").unwrap_or(v);
    let lo = inner.get("coefficient_lo")?.as_i64()?;
    let hi = inner
        .get("coefficient_hi")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let scale = inner.get("scale")?.as_u64()? as u8;
    let coefficient = (i128::from(hi) << 64) | i128::from(lo as u64);
    Some(format_fixed(marketfeed_model::Fixed { coefficient, scale }))
}

fn format_fixed(f: marketfeed_model::Fixed) -> String {
    let neg = f.coefficient < 0;
    let mag = f.coefficient.unsigned_abs();
    let scale = f.scale as usize;
    let digits = mag.to_string();
    let (int_part, frac_part) = if scale == 0 {
        (digits, String::new())
    } else if digits.len() <= scale {
        ("0".to_string(), format!("{digits:0>scale$}"))
    } else {
        let split = digits.len() - scale;
        (digits[..split].to_string(), digits[split..].to_string())
    };
    let mut s = String::new();
    if neg {
        s.push('-');
    }
    s.push_str(&int_part);
    if !frac_part.is_empty() {
        s.push('.');
        s.push_str(&frac_part);
    }
    s
}

fn notional_from_strings(price: &str, qty: &str) -> Option<String> {
    let p = marketfeed_model::Fixed::parse_str(price).ok()?;
    let q = marketfeed_model::Fixed::parse_str(qty).ok()?;
    fixed_mul(p, q).map(format_fixed)
}

fn fixed_mul(
    a: marketfeed_model::Fixed,
    b: marketfeed_model::Fixed,
) -> Option<marketfeed_model::Fixed> {
    let scale = a.scale.checked_add(b.scale)?;
    let coefficient = a.coefficient.checked_mul(b.coefficient)?;
    Some(marketfeed_model::Fixed { coefficient, scale })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_empty_dir_ok() {
        let dir = std::env::temp_dir().join(format!(
            "marketfeed-replay-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let cfg = DaemonConfig::from_toml_str(&format!(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            replay_dir = "{}"
            [readiness]
            require_required_venues = false
            "#,
            dir.display()
        ))
        .unwrap();
        let resp = list_replay_files(&cfg).unwrap();
        assert!(resp.files.is_empty(), "{resp:?}");
    }

    #[test]
    fn rejects_path_traversal() {
        let root = std::env::temp_dir();
        assert!(resolve_replay_file(&root, "../etc/passwd").is_err());
        assert!(resolve_replay_file(&root, "/etc/passwd").is_err());
    }

    #[test]
    fn replay_rejects_a_record_larger_than_the_bounded_reader_limit() {
        let dir = std::env::temp_dir().join(format!(
            "marketfeed-replay-bounded-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("oversized.jsonl");
        std::fs::write(&file, vec![b' '; 8 * 1024 * 1024 + 1]).unwrap();
        let cfg = DaemonConfig::from_toml_str(&format!(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            replay_dir = "{}"
            [readiness]
            require_required_venues = false
            "#,
            dir.display()
        ))
        .unwrap();

        let response = read_replay_entries(&cfg, "oversized.jsonl", 0, 100);

        assert!(
            response
                .error
                .as_deref()
                .is_some_and(|error| error.contains("record exceeds")),
            "{response:?}"
        );
        let _ = std::fs::remove_file(file);
        let _ = std::fs::remove_dir(dir);
    }
}
