//! Best-effort replay file listing and JSONL/MFNE tape read (loopback only).

use std::path::{Component, Path, PathBuf};

use marketfeed_recording::{read_length_prefixed_json, read_normalized_jsonl};
use serde::Serialize;
use serde_json::Value;

use crate::config::DaemonConfig;

const DEFAULT_REPLAY_DIR: &str = ".local/live-ui/raw";

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
    let directory = root.to_string_lossy().into_owned();
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
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) => {
            return ReplayEntriesResponse {
                file: file.to_string(),
                offset,
                limit,
                total: 0,
                entries: Vec::new(),
                error: Some(format!("read {}: {e}", path.display())),
            };
        }
    };
    let records = match parse_replay_bytes(&bytes, file) {
        Ok(r) => r,
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
    let total = records.len() as u64;
    let start = (offset as usize).min(records.len());
    let end = start.saturating_add(limit as usize).min(records.len());
    let entries: Vec<Value> = records[start..end]
        .iter()
        .filter_map(envelope_to_tape_entry)
        .collect();
    let _ = directory;
    ReplayEntriesResponse {
        file: file.to_string(),
        offset,
        limit,
        total,
        entries,
        error: None,
    }
}

fn parse_replay_bytes(bytes: &[u8], file: &str) -> Result<Vec<Value>, String> {
    if file.ends_with(".mfne") {
        read_length_prefixed_json(bytes).map_err(|e| format!("parse mfne: {e}"))
    } else {
        read_normalized_jsonl(bytes).map_err(|e| format!("parse jsonl: {e}"))
    }
}

/// Resolve `file` under `root`; reject path traversal.
fn resolve_replay_file(root: &Path, file: &str) -> Result<PathBuf, String> {
    if file.is_empty() || file.contains('\0') || file.contains("..") {
        return Err("invalid file name".into());
    }
    let rel = Path::new(file);
    if rel
        .components()
        .any(|c| matches!(c, Component::RootDir | Component::Prefix(_) | Component::ParentDir))
    {
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
    Some(format_fixed(marketfeed_model::Fixed {
        coefficient,
        scale,
    }))
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

fn fixed_mul(a: marketfeed_model::Fixed, b: marketfeed_model::Fixed) -> Option<marketfeed_model::Fixed> {
    let scale = a.scale.checked_add(b.scale)?;
    let coefficient = a.coefficient.checked_mul(b.coefficient)?;
    Some(marketfeed_model::Fixed {
        coefficient,
        scale,
    })
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
}
