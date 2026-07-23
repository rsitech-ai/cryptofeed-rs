//! Bounded spill-to-disk WAL for [`OverflowPolicy::SpillToDisk`] (spec §17.5–17.6).
//!
//! Live items sit in memory queues. When capacity is full, overflow appends to a
//! length-prefixed WAL file until [`SpillWalConfig::wal_limit_bytes`]. At the
//! limit the sink **fails closed** (`SinkError::FailEngine`) and queues
//! [`SystemEvent::EventsDropped`] / [`SystemEvent::DiskPressure`] for the caller
//! via [`SpillWalSink::take_system_events`].
//!
//! # WAL framing (MFSPILL2)
//!
//! ```text
//! ["MFSPILL2\n"][u8 tag][u32 little-endian body_len][UTF-8 JSON body]
//! tag: 1 = batch, 2 = system
//! ```
//!
//! Bodies are serde-tagged [`SpillItem`] values containing complete normalized
//! envelopes or typed system events. Existing records are loaded on reopen.
//! Recovery is at-least-once: process [`SpillWalSink::pop_recovered`] items,
//! then call [`SpillWalSink::checkpoint_recovery`]. A crash before checkpoint
//! replays the prefix again; a checkpoint preserves records appended meanwhile.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use marketfeed_adapter_api::EventBatch;
use marketfeed_dispatch::PushOutcome;
use marketfeed_model::{OverflowPolicy, SystemEvent};
use serde::{Deserialize, Serialize};

use crate::sink::{EventSink, SinkError};

const TAG_BATCH: u8 = 1;
const TAG_SYSTEM: u8 = 2;
const WAL_MAGIC: &[u8] = b"MFSPILL2\n";
const MAX_RECORD_BYTES: u64 = 64 * 1024 * 1024;

/// One complete, typed spill record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value")]
pub enum SpillItem {
    Batch(EventBatch),
    System(SystemEvent),
}

/// Open knobs for [`SpillWalSink`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillWalConfig {
    /// Append path for the spill WAL file.
    pub path: PathBuf,
    /// In-memory batch queue capacity (must be > 0).
    pub batch_capacity: usize,
    /// In-memory system queue capacity (must be > 0).
    pub system_capacity: usize,
    /// Hard cap on WAL file bytes (including records already written).
    /// Must be > 0. Reaching the cap fails closed.
    pub wal_limit_bytes: u64,
}

/// Bounded lossless-oriented sink: memory first, then disk WAL, then fail-closed.
#[derive(Debug)]
pub struct SpillWalSink {
    batches: VecDeque<EventBatch>,
    systems: VecDeque<SystemEvent>,
    batch_capacity: usize,
    system_capacity: usize,
    path: PathBuf,
    writer: BufWriter<File>,
    wal_limit_bytes: u64,
    wal_bytes: u64,
    spilled_batches: u64,
    spilled_systems: u64,
    pending_system_events: Vec<SystemEvent>,
    recovered_remaining: usize,
    recovery_next_offset: u64,
    recovery_prefix_bytes: u64,
}

impl SpillWalSink {
    /// Create / append a spill WAL at `cfg.path`.
    ///
    /// # Errors
    /// Returns I/O errors from directory create / file open. Panics if capacities
    /// or `wal_limit_bytes` are zero (misconfiguration).
    pub fn open(cfg: SpillWalConfig) -> Result<Self, std::io::Error> {
        assert!(cfg.batch_capacity > 0, "batch_capacity must be > 0");
        assert!(cfg.system_capacity > 0, "system_capacity must be > 0");
        assert!(cfg.wal_limit_bytes > 0, "wal_limit_bytes must be > 0");
        if let Some(parent) = cfg.path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        if cfg.wal_limit_bytes < WAL_MAGIC.len() as u64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "spill WAL limit {} is smaller than MFSPILL2 header",
                    cfg.wal_limit_bytes
                ),
            ));
        }
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&cfg.path)?;
        let file_len = file.metadata()?.len();
        if file_len > cfg.wal_limit_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "spill WAL length {file_len} exceeds configured limit {}",
                    cfg.wal_limit_bytes
                ),
            ));
        }
        if file_len == 0 {
            file.write_all(WAL_MAGIC)?;
            file.sync_data()?;
        }
        drop(file);
        let scan = scan_spill_file(&cfg.path, cfg.wal_limit_bytes)?;
        let file = OpenOptions::new().append(true).open(&cfg.path)?;
        let wal_bytes = file.metadata()?.len();
        Ok(Self {
            batches: VecDeque::with_capacity(cfg.batch_capacity),
            systems: VecDeque::with_capacity(cfg.system_capacity),
            batch_capacity: cfg.batch_capacity,
            system_capacity: cfg.system_capacity,
            path: cfg.path,
            writer: BufWriter::new(file),
            wal_limit_bytes: cfg.wal_limit_bytes,
            wal_bytes,
            spilled_batches: 0,
            spilled_systems: 0,
            pending_system_events: Vec::new(),
            recovered_remaining: scan.record_count,
            recovery_next_offset: WAL_MAGIC.len() as u64,
            recovery_prefix_bytes: if scan.record_count == 0 {
                WAL_MAGIC.len() as u64
            } else {
                wal_bytes
            },
        })
    }

    /// Policy this sink implements.
    pub fn policy(&self) -> OverflowPolicy {
        OverflowPolicy::SpillToDisk
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn wal_bytes(&self) -> u64 {
        self.wal_bytes
    }

    pub fn wal_limit_bytes(&self) -> u64 {
        self.wal_limit_bytes
    }

    pub fn batch_len(&self) -> usize {
        self.batches.len()
    }

    pub fn system_len(&self) -> usize {
        self.systems.len()
    }

    pub fn spilled_batches(&self) -> u64 {
        self.spilled_batches
    }

    pub fn spilled_systems(&self) -> u64 {
        self.spilled_systems
    }

    pub fn pop_batch(&mut self) -> Option<EventBatch> {
        self.batches.pop_front()
    }

    pub fn pop_system(&mut self) -> Option<SystemEvent> {
        self.systems.pop_front()
    }

    /// Number of typed records recovered from the WAL prefix present at open.
    pub fn recovered_len(&self) -> usize {
        self.recovered_remaining
    }

    /// Pop the next recovered record in original append order.
    pub fn pop_recovered(&mut self) -> Result<Option<SpillItem>, SinkError> {
        if self.recovered_remaining == 0 {
            return Ok(None);
        }
        let mut file = File::open(&self.path).map_err(|error| SinkError::Io(error.to_string()))?;
        file.seek(SeekFrom::Start(self.recovery_next_offset))
            .map_err(|error| SinkError::Io(error.to_string()))?;
        let (item, record_len) =
            read_one_item(&mut file, self.wal_limit_bytes).map_err(|error| {
                SinkError::Io(format!(
                    "read recovered spill record at offset {}: {error}",
                    self.recovery_next_offset
                ))
            })?;
        self.recovery_next_offset = self.recovery_next_offset.saturating_add(record_len);
        self.recovered_remaining -= 1;
        Ok(Some(item))
    }

    /// Acknowledge a fully processed recovery prefix.
    ///
    /// Records appended after open are retained. Calling this while recovered
    /// items remain fails closed so callers cannot acknowledge unprocessed data.
    pub fn checkpoint_recovery(&mut self) -> Result<(), SinkError> {
        if self.recovered_remaining != 0 {
            return Err(SinkError::Io(
                "cannot checkpoint spill WAL with unprocessed recovered records".into(),
            ));
        }
        if self.recovery_prefix_bytes == WAL_MAGIC.len() as u64 {
            return Ok(());
        }
        self.writer
            .flush()
            .map_err(|error| Self::note_io(&mut self.pending_system_events, error))?;
        self.writer
            .get_ref()
            .sync_data()
            .map_err(|error| Self::note_io(&mut self.pending_system_events, error))?;

        let mut source = File::open(&self.path)
            .map_err(|error| Self::note_io(&mut self.pending_system_events, error))?;
        let file_len = source
            .metadata()
            .map_err(|error| Self::note_io(&mut self.pending_system_events, error))?
            .len();
        if file_len < self.recovery_prefix_bytes {
            return Err(Self::note_io(
                &mut self.pending_system_events,
                std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "spill WAL shortened before recovery checkpoint",
                ),
            ));
        }
        source
            .seek(SeekFrom::Start(self.recovery_prefix_bytes))
            .map_err(|error| Self::note_io(&mut self.pending_system_events, error))?;
        let tail_len = file_len - self.recovery_prefix_bytes;
        let temp_path = checkpoint_temp_path(&self.path);
        let checkpoint_result = (|| -> Result<(), std::io::Error> {
            let mut temp = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temp_path)?;
            temp.write_all(WAL_MAGIC)?;
            std::io::copy(&mut source, &mut temp)?;
            temp.sync_all()?;
            replace_file_atomically(&temp_path, &self.path)?;
            sync_parent_directory(&self.path)?;
            Ok(())
        })();
        if let Err(error) = checkpoint_result {
            let _ = std::fs::remove_file(&temp_path);
            return Err(Self::note_io(&mut self.pending_system_events, error));
        }
        let file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(|error| Self::note_io(&mut self.pending_system_events, error))?;
        self.writer = BufWriter::new(file);
        self.wal_bytes = WAL_MAGIC.len() as u64 + tail_len;
        self.recovery_prefix_bytes = WAL_MAGIC.len() as u64;
        self.recovery_next_offset = WAL_MAGIC.len() as u64;
        Ok(())
    }

    /// Drain pending policy events (`EventsDropped` / `DiskPressure`).
    pub fn take_system_events(&mut self) -> Vec<SystemEvent> {
        std::mem::take(&mut self.pending_system_events)
    }

    fn spill_record(&mut self, tag: u8, body: &[u8]) -> Result<(), SinkError> {
        let body_len = u32::try_from(body.len())
            .map_err(|_| SinkError::Io("spill record exceeds u32::MAX".into()))?;
        let record_len = 1u64 + 4 + u64::from(body_len);
        if self.wal_bytes.saturating_add(record_len) > self.wal_limit_bytes {
            self.note_wal_exhausted(1);
            return Err(SinkError::FailEngine);
        }
        if let Err(e) = self.writer.write_all(&[tag]) {
            return Err(Self::note_io(&mut self.pending_system_events, e));
        }
        if let Err(e) = self.writer.write_all(&body_len.to_le_bytes()) {
            return Err(Self::note_io(&mut self.pending_system_events, e));
        }
        if let Err(e) = self.writer.write_all(body) {
            return Err(Self::note_io(&mut self.pending_system_events, e));
        }
        if let Err(e) = self.writer.flush() {
            return Err(Self::note_io(&mut self.pending_system_events, e));
        }
        self.wal_bytes = self.wal_bytes.saturating_add(record_len);
        Ok(())
    }

    fn note_io(pending: &mut Vec<SystemEvent>, err: std::io::Error) -> SinkError {
        pending.push(SystemEvent::DiskPressure);
        pending.push(SystemEvent::EventsDropped {
            count: 1,
            detail: format!("spill_wal io: {err}"),
        });
        SinkError::FailEngine
    }

    fn note_wal_exhausted(&mut self, count: u64) {
        self.pending_system_events.push(SystemEvent::DiskPressure);
        self.pending_system_events.push(SystemEvent::EventsDropped {
            count,
            detail: format!(
                "spill_wal limit reached wal_bytes={} limit={}",
                self.wal_bytes, self.wal_limit_bytes
            ),
        });
    }

    fn spill_batch(&mut self, batch: &EventBatch) -> Result<(), SinkError> {
        let body = serde_json::to_vec(&SpillItem::Batch(batch.clone()))
            .map_err(|e| SinkError::Io(e.to_string()))?;
        self.spill_record(TAG_BATCH, &body)?;
        self.spilled_batches += 1;
        Ok(())
    }

    fn spill_system(&mut self, event: &SystemEvent) -> Result<(), SinkError> {
        let body = serde_json::to_vec(&SpillItem::System(event.clone()))
            .map_err(|e| SinkError::Io(e.to_string()))?;
        self.spill_record(TAG_SYSTEM, &body)?;
        self.spilled_systems += 1;
        Ok(())
    }
}

impl EventSink for SpillWalSink {
    fn push_batch(&mut self, batch: EventBatch) -> Result<PushOutcome, SinkError> {
        if self.batches.len() < self.batch_capacity {
            self.batches.push_back(batch);
            return Ok(PushOutcome::Accepted);
        }
        self.spill_batch(&batch)?;
        Ok(PushOutcome::Accepted)
    }

    fn push_system(&mut self, event: SystemEvent) -> Result<PushOutcome, SinkError> {
        if self.systems.len() < self.system_capacity {
            self.systems.push_back(event);
            return Ok(PushOutcome::Accepted);
        }
        self.spill_system(&event)?;
        Ok(PushOutcome::Accepted)
    }
}

#[derive(Debug, Clone, Copy)]
struct SpillScan {
    record_count: usize,
}

fn scan_spill_file(path: &Path, wal_limit_bytes: u64) -> Result<SpillScan, std::io::Error> {
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    let file_len = file.metadata()?.len();
    if file_len > wal_limit_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("spill WAL length {file_len} exceeds configured limit {wal_limit_bytes}"),
        ));
    }
    let mut reader = BufReader::new(file.try_clone()?);
    read_magic(&mut reader)?;
    let mut record_count = 0usize;
    let mut valid_end = WAL_MAGIC.len() as u64;
    loop {
        let record_start = reader.stream_position()?;
        let mut tag = [0u8; 1];
        match reader.read(&mut tag) {
            Ok(0) => break,
            Ok(1) => {}
            Ok(_) => unreachable!("single-byte read returned more than one byte"),
            Err(error) => return Err(error),
        }
        let mut length = [0u8; 4];
        if let Err(error) = reader.read_exact(&mut length) {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                file.set_len(record_start)?;
                file.sync_data()?;
                break;
            }
            return Err(error);
        }
        let body_len = u32::from_le_bytes(length) as u64;
        validate_record_len(body_len, wal_limit_bytes)?;
        let record_end = record_start
            .checked_add(5)
            .and_then(|value| value.checked_add(body_len))
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "spill record length overflow",
                )
            })?;
        if record_end > file_len {
            file.set_len(record_start)?;
            file.sync_data()?;
            break;
        }
        let mut body = vec![0u8; body_len as usize];
        reader.read_exact(&mut body)?;
        decode_item(tag[0], &body).map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid spill record at offset {record_start}: {error}"),
            )
        })?;
        record_count = record_count.checked_add(1).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "spill record count exceeds usize::MAX",
            )
        })?;
        valid_end = record_end;
    }
    debug_assert_eq!(file.metadata()?.len(), valid_end);
    Ok(SpillScan { record_count })
}

fn read_magic(reader: &mut impl Read) -> Result<(), std::io::Error> {
    let mut magic = [0u8; WAL_MAGIC.len()];
    reader.read_exact(&mut magic).map_err(|error| {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "spill WAL is missing the MFSPILL2 header; MFSPILL1 metadata WALs are not replay-compatible with complete-event records; quarantine or migrate the file before startup",
            )
        } else {
            error
        }
    })?;
    if magic != WAL_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "legacy or unknown spill WAL format; MFSPILL1 metadata WALs are not replay-compatible with MFSPILL2 complete-event records; quarantine or migrate the file before startup",
        ));
    }
    Ok(())
}

fn validate_record_len(body_len: u64, wal_limit_bytes: u64) -> Result<(), std::io::Error> {
    let maximum = wal_limit_bytes
        .saturating_sub(WAL_MAGIC.len() as u64 + 5)
        .min(MAX_RECORD_BYTES);
    if body_len > maximum {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("spill record body length {body_len} exceeds maximum {maximum}"),
        ));
    }
    Ok(())
}

fn decode_item(tag: u8, body: &[u8]) -> Result<SpillItem, SinkError> {
    let item: SpillItem =
        serde_json::from_slice(body).map_err(|error| SinkError::Io(error.to_string()))?;
    let tag_matches = matches!(
        (tag, &item),
        (TAG_BATCH, SpillItem::Batch(_)) | (TAG_SYSTEM, SpillItem::System(_))
    );
    if !tag_matches {
        return Err(SinkError::Io("spill record tag/body mismatch".into()));
    }
    Ok(item)
}

fn read_one_item(
    reader: &mut impl Read,
    wal_limit_bytes: u64,
) -> Result<(SpillItem, u64), std::io::Error> {
    let mut tag = [0u8; 1];
    reader.read_exact(&mut tag)?;
    let mut length = [0u8; 4];
    reader.read_exact(&mut length)?;
    let body_len = u32::from_le_bytes(length) as u64;
    validate_record_len(body_len, wal_limit_bytes)?;
    let mut body = vec![0u8; body_len as usize];
    reader.read_exact(&mut body)?;
    let item = decode_item(tag[0], &body)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()))?;
    Ok((item, 5 + body_len))
}

fn checkpoint_temp_path(path: &Path) -> PathBuf {
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("spill.wal");
    path.with_file_name(format!(
        ".{file_name}.checkpoint-{}-{suffix}",
        std::process::id()
    ))
}

#[cfg(unix)]
fn replace_file_atomically(source: &Path, destination: &Path) -> Result<(), std::io::Error> {
    std::fs::rename(source, destination)
}

#[cfg(not(unix))]
fn replace_file_atomically(_source: &Path, _destination: &Path) -> Result<(), std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "atomic spill WAL checkpoint replacement is not implemented on this platform",
    ))
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

/// Decode spilled WAL bytes into `(tag, json_body)` records (tests / inspect).
pub fn read_spill_records(bytes: &[u8]) -> Result<Vec<(u8, serde_json::Value)>, SinkError> {
    if !bytes.starts_with(WAL_MAGIC) {
        return Err(SinkError::Io(
            "missing or unsupported spill WAL header (expected MFSPILL2)".into(),
        ));
    }
    let mut out = Vec::new();
    let mut i = WAL_MAGIC.len();
    while i < bytes.len() {
        let tag = bytes[i];
        i += 1;
        if i + 4 > bytes.len() {
            return Err(SinkError::Io("truncated spill record length".into()));
        }
        let len = u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap()) as usize;
        i += 4;
        if len as u64 > MAX_RECORD_BYTES {
            return Err(SinkError::Io(format!(
                "spill record body length {len} exceeds maximum {MAX_RECORD_BYTES}"
            )));
        }
        let body_end = i
            .checked_add(len)
            .ok_or_else(|| SinkError::Io("spill record length overflow".into()))?;
        if body_end > bytes.len() {
            return Err(SinkError::Io("truncated spill record body".into()));
        }
        let body = &bytes[i..body_end];
        i = body_end;
        let v: serde_json::Value =
            serde_json::from_slice(body).map_err(|e| SinkError::Io(e.to_string()))?;
        out.push((tag, v));
    }
    Ok(out)
}

/// Decode complete typed spill records in append order.
pub fn read_spill_items(bytes: &[u8]) -> Result<Vec<SpillItem>, SinkError> {
    read_spill_records(bytes)?
        .into_iter()
        .map(|(tag, value)| {
            let body = serde_json::to_vec(&value).map_err(|e| SinkError::Io(e.to_string()))?;
            decode_item(tag, &body)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use marketfeed_model::{
        AggressorSide, ConnectionId, EventEnvelope, EventFlags, Fixed, InstrumentId, MarketEvent,
        Price, Quantity, SessionId, SourceId, TimestampNs, Trade, VenueId,
    };

    use super::*;

    fn batch(seq: u64) -> EventBatch {
        EventBatch {
            session: SessionId(1),
            frame_seq: seq,
            events: Vec::new(),
        }
    }

    fn trade_batch(seq: u64) -> EventBatch {
        EventBatch {
            session: SessionId(1),
            frame_seq: seq,
            events: vec![EventEnvelope {
                schema_version: 1,
                venue: VenueId(2),
                instrument: Some(InstrumentId(7)),
                connection: ConnectionId(3),
                session: SessionId(1),
                frame_seq: seq,
                event_index: 0,
                exchange_ts: Some(TimestampNs(10)),
                receive_ts: TimestampNs(11),
                source_sequence: None,
                flags: EventFlags::empty(),
                payload: MarketEvent::Trade(Trade {
                    price: Price(Fixed::new(12345, 2)),
                    quantity: Quantity(Fixed::new(25, 1)),
                    aggressor: AggressorSide::Buy,
                    trade_id: Some(SourceId("trade-1".into())),
                }),
            }],
        }
    }

    fn temp_wal(name: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name);
        (dir, path)
    }

    #[test]
    fn memory_accepts_until_capacity_then_spills() {
        let (_dir, path) = temp_wal("spill.wal");
        let mut sink = SpillWalSink::open(SpillWalConfig {
            path: path.clone(),
            batch_capacity: 1,
            system_capacity: 1,
            wal_limit_bytes: 64 * 1024,
        })
        .unwrap();

        assert_eq!(sink.push_batch(batch(1)).unwrap(), PushOutcome::Accepted);
        assert_eq!(sink.batch_len(), 1);
        assert_eq!(sink.spilled_batches(), 0);

        assert_eq!(sink.push_batch(batch(2)).unwrap(), PushOutcome::Accepted);
        assert_eq!(sink.batch_len(), 1);
        assert_eq!(sink.pop_batch().unwrap().frame_seq, 1);
        assert_eq!(sink.spilled_batches(), 1);
        assert!(sink.wal_bytes() > 0);

        let bytes = std::fs::read(&path).unwrap();
        let recs = read_spill_records(&bytes).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].0, TAG_BATCH);
        assert_eq!(recs[0].1["value"]["frame_seq"], 2);
    }

    #[test]
    fn wal_limit_fail_closed_emits_events_dropped() {
        let (_dir, path) = temp_wal("limit.wal");
        let mut sink = SpillWalSink::open(SpillWalConfig {
            path,
            batch_capacity: 1,
            system_capacity: 1,
            wal_limit_bytes: 40,
        })
        .unwrap();
        assert_eq!(sink.push_batch(batch(1)).unwrap(), PushOutcome::Accepted);

        let mut saw_fail = false;
        for seq in 2..32 {
            match sink.push_batch(batch(seq)) {
                Ok(PushOutcome::Accepted) => {}
                Ok(other) => panic!("unexpected outcome {other:?}"),
                Err(SinkError::FailEngine) => {
                    saw_fail = true;
                    break;
                }
                Err(e) => panic!("unexpected err {e:?}"),
            }
        }
        assert!(saw_fail, "expected FailEngine at WAL limit");
        let events = sink.take_system_events();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, SystemEvent::EventsDropped { .. })),
            "expected EventsDropped, got {events:?}"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, SystemEvent::DiskPressure)),
            "expected DiskPressure, got {events:?}"
        );
        assert_eq!(sink.batch_len(), 1);
    }

    #[test]
    fn system_spill_roundtrip_bytes() {
        let (_dir, path) = temp_wal("sys.wal");
        let mut sink = SpillWalSink::open(SpillWalConfig {
            path: path.clone(),
            batch_capacity: 1,
            system_capacity: 1,
            wal_limit_bytes: 4096,
        })
        .unwrap();
        assert_eq!(
            sink.push_system(SystemEvent::HeartbeatMissed).unwrap(),
            PushOutcome::Accepted
        );
        assert_eq!(
            sink.push_system(SystemEvent::RateLimited).unwrap(),
            PushOutcome::Accepted
        );
        assert_eq!(sink.spilled_systems(), 1);
        let recs = read_spill_records(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(recs[0].0, TAG_SYSTEM);
        assert_eq!(recs[0].1["kind"], "System");
    }

    #[test]
    fn complete_batch_recovers_after_reopen_and_checkpoint_preserves_new_tail() {
        let (_dir, path) = temp_wal("recover.wal");
        let expected = trade_batch(2);
        {
            let mut sink = SpillWalSink::open(SpillWalConfig {
                path: path.clone(),
                batch_capacity: 1,
                system_capacity: 1,
                wal_limit_bytes: 64 * 1024,
            })
            .unwrap();
            sink.push_batch(batch(1)).unwrap();
            sink.push_batch(expected.clone()).unwrap();
        }

        let mut reopened = SpillWalSink::open(SpillWalConfig {
            path: path.clone(),
            batch_capacity: 1,
            system_capacity: 1,
            wal_limit_bytes: 64 * 1024,
        })
        .unwrap();
        assert_eq!(reopened.recovered_len(), 1);
        assert_eq!(
            reopened.pop_recovered(),
            Ok(Some(SpillItem::Batch(expected.clone())))
        );

        reopened.push_batch(batch(3)).unwrap();
        let tail = trade_batch(4);
        reopened.push_batch(tail.clone()).unwrap();
        reopened.checkpoint_recovery().unwrap();
        drop(reopened);

        let mut after_checkpoint = SpillWalSink::open(SpillWalConfig {
            path,
            batch_capacity: 1,
            system_capacity: 1,
            wal_limit_bytes: 64 * 1024,
        })
        .unwrap();
        assert_eq!(after_checkpoint.recovered_len(), 1);
        assert_eq!(
            after_checkpoint.pop_recovered(),
            Ok(Some(SpillItem::Batch(tail)))
        );
    }

    #[test]
    fn mixed_batch_and_system_records_recover_in_append_order() {
        let (_dir, path) = temp_wal("mixed.wal");
        {
            let mut sink = SpillWalSink::open(SpillWalConfig {
                path: path.clone(),
                batch_capacity: 1,
                system_capacity: 1,
                wal_limit_bytes: 64 * 1024,
            })
            .unwrap();
            sink.push_batch(batch(1)).unwrap();
            sink.push_system(SystemEvent::HeartbeatMissed).unwrap();
            sink.push_batch(trade_batch(2)).unwrap();
            sink.push_system(SystemEvent::RateLimited).unwrap();
        }

        let mut reopened = SpillWalSink::open(SpillWalConfig {
            path,
            batch_capacity: 1,
            system_capacity: 1,
            wal_limit_bytes: 64 * 1024,
        })
        .unwrap();
        assert_eq!(reopened.recovered_len(), 2);
        assert!(matches!(
            reopened.pop_recovered().unwrap(),
            Some(SpillItem::Batch(EventBatch { frame_seq: 2, .. }))
        ));
        assert_eq!(
            reopened.pop_recovered().unwrap(),
            Some(SpillItem::System(SystemEvent::RateLimited))
        );
        assert_eq!(reopened.pop_recovered().unwrap(), None);
    }

    #[test]
    fn torn_final_append_is_truncated_to_the_valid_prefix() {
        let (_dir, path) = temp_wal("torn-tail.wal");
        {
            let mut sink = SpillWalSink::open(SpillWalConfig {
                path: path.clone(),
                batch_capacity: 1,
                system_capacity: 1,
                wal_limit_bytes: 64 * 1024,
            })
            .unwrap();
            sink.push_batch(batch(1)).unwrap();
            sink.push_batch(trade_batch(2)).unwrap();
        }
        let valid_len = std::fs::metadata(&path).unwrap().len();
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&[TAG_BATCH]).unwrap();
            file.write_all(&20u32.to_le_bytes()).unwrap();
            file.write_all(br#"{"kind":"#).unwrap();
            file.sync_all().unwrap();
        }
        assert!(std::fs::metadata(&path).unwrap().len() > valid_len);

        let mut reopened = SpillWalSink::open(SpillWalConfig {
            path: path.clone(),
            batch_capacity: 1,
            system_capacity: 1,
            wal_limit_bytes: 64 * 1024,
        })
        .unwrap();
        assert_eq!(std::fs::metadata(path).unwrap().len(), valid_len);
        assert_eq!(reopened.recovered_len(), 1);
        assert!(matches!(
            reopened.pop_recovered().unwrap(),
            Some(SpillItem::Batch(EventBatch { frame_seq: 2, .. }))
        ));
    }

    #[test]
    fn complete_malformed_record_is_rejected_without_truncating() {
        let (_dir, path) = temp_wal("malformed.wal");
        {
            let _sink = SpillWalSink::open(SpillWalConfig {
                path: path.clone(),
                batch_capacity: 1,
                system_capacity: 1,
                wal_limit_bytes: 64 * 1024,
            })
            .unwrap();
        }
        let malformed = br#"not-json"#;
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&[TAG_BATCH]).unwrap();
            file.write_all(&(malformed.len() as u32).to_le_bytes())
                .unwrap();
            file.write_all(malformed).unwrap();
            file.sync_all().unwrap();
        }
        let malformed_len = std::fs::metadata(&path).unwrap().len();

        let error = SpillWalSink::open(SpillWalConfig {
            path: path.clone(),
            batch_capacity: 1,
            system_capacity: 1,
            wal_limit_bytes: 64 * 1024,
        })
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(std::fs::metadata(path).unwrap().len(), malformed_len);
    }

    #[test]
    fn legacy_wal_is_rejected_with_migration_guidance() {
        let (_dir, path) = temp_wal("legacy.wal");
        std::fs::write(&path, [TAG_BATCH, 0, 0, 0, 0]).unwrap();
        let error = SpillWalSink::open(SpillWalConfig {
            path,
            batch_capacity: 1,
            system_capacity: 1,
            wal_limit_bytes: 64 * 1024,
        })
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("MFSPILL1"));
        assert!(error.to_string().contains("quarantine or migrate"));
    }

    #[test]
    fn oversized_existing_wal_fails_before_record_loading() {
        let (_dir, path) = temp_wal("oversized.wal");
        let file = File::create(&path).unwrap();
        file.set_len(1024).unwrap();
        let error = SpillWalSink::open(SpillWalConfig {
            path,
            batch_capacity: 1,
            system_capacity: 1,
            wal_limit_bytes: 128,
        })
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("exceeds configured limit"));
    }
}
