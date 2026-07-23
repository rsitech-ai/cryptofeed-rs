//! Rotating raw-segment pipeline with bounded queue and shutdown drain.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions, create_dir_all, remove_file};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use marketfeed_model::{OverflowPolicy, SessionId, SystemEvent};

use crate::format::{Direction, FrameOpcode, HEADER_SIZE, RAW_HEADER_BODY_LEN, RecordingError};
use crate::queue::{EnqueueOutcome, PendingFrame, RecordingQueue};
use crate::writer::RawSegmentWriter;
use crate::{MetadataRecord, encode_metadata};

/// Size / time rotation knobs (`0` / `ZERO` disables that axis).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RotationConfig {
    pub max_bytes: u64,
    pub max_duration: Duration,
}

impl Default for RotationConfig {
    fn default() -> Self {
        Self {
            max_bytes: 256 * 1024 * 1024,
            max_duration: Duration::from_secs(15 * 60),
        }
    }
}

/// Optional free-space probe. Return `None` to skip the check.
pub type FreeSpaceProbe = Box<dyn FnMut(&Path) -> Option<u64> + Send>;

/// Config for [`RecordingPipeline`].
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub directory: PathBuf,
    pub queue_capacity: usize,
    pub overflow: OverflowPolicy,
    pub rotation: RotationConfig,
    pub min_free_bytes: u64,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("./raw"),
            queue_capacity: 8192,
            overflow: OverflowPolicy::FailEngine,
            rotation: RotationConfig::default(),
            min_free_bytes: 0,
        }
    }
}

/// Append-only rotating recorder with a bounded enqueue front.
pub struct RecordingPipeline {
    cfg: PipelineConfig,
    queue: RecordingQueue,
    writer: Option<RawSegmentWriter<BufWriter<File>>>,
    current_path: Option<PathBuf>,
    current_start_ts_ns: Option<i64>,
    segment_bytes: u64,
    segment_started: Instant,
    segment_seq: u64,
    free_space: Option<FreeSpaceProbe>,
    free_space_check_interval: Duration,
    last_free_space_check: Option<Instant>,
    disk_pressure_active: bool,
    pub system_events: Vec<SystemEvent>,
    pub records_written: u64,
    pub segments_opened: u64,
    pub rotations: u64,
    metadata: Vec<MetadataRecord>,
}

/// Thread-safe producer/drain handle for one process-wide recording pipeline.
///
/// Session runners only enqueue bounded frame copies through this handle. The
/// daemon's recording task owns flushing, rotation, metrics, and shutdown drain.
#[derive(Clone)]
pub struct RecordingHandle {
    ingress: Arc<Mutex<RecordingIngress>>,
    consumer: Arc<Mutex<RecordingConsumer>>,
}

struct RecordingIngress {
    queue: RecordingQueue,
    last_reported_dropped: u64,
}

struct RecordingConsumer {
    pipeline: RecordingPipeline,
    pending: VecDeque<PendingFrame>,
}

impl std::fmt::Debug for RecordingHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordingHandle").finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordingSnapshot {
    pub directory: PathBuf,
    pub queue_len: usize,
    pub rotations: u64,
    pub records_written: u64,
    pub dropped_total: u64,
    pub disk_pressure: bool,
}

impl RecordingHandle {
    pub fn new(pipeline: RecordingPipeline) -> Self {
        let ingress = RecordingIngress {
            queue: RecordingQueue::new(pipeline.cfg.queue_capacity, pipeline.cfg.overflow),
            last_reported_dropped: 0,
        };
        Self {
            ingress: Arc::new(Mutex::new(ingress)),
            consumer: Arc::new(Mutex::new(RecordingConsumer {
                pipeline,
                pending: VecDeque::new(),
            })),
        }
    }

    fn lock_ingress(&self) -> Result<MutexGuard<'_, RecordingIngress>, RecordingError> {
        self.ingress
            .lock()
            .map_err(|_| RecordingError::Io("recording ingress lock poisoned".into()))
    }

    fn lock_consumer(&self) -> Result<MutexGuard<'_, RecordingConsumer>, RecordingError> {
        self.consumer
            .lock()
            .map_err(|_| RecordingError::Io("recording consumer lock poisoned".into()))
    }

    pub fn set_free_space_probe(&self, probe: FreeSpaceProbe) -> Result<(), RecordingError> {
        self.lock_consumer()?.pipeline.set_free_space_probe(probe);
        Ok(())
    }

    pub fn register_metadata(&self, metadata: MetadataRecord) -> Result<(), RecordingError> {
        self.lock_consumer()?.pipeline.register_metadata(metadata)
    }

    pub fn enqueue(
        &self,
        session: SessionId,
        frame_seq: u64,
        receive_ts_ns: i64,
        monotonic_ns: u64,
        direction: Direction,
        opcode: FrameOpcode,
        flags: u8,
        payload: &[u8],
    ) -> Result<EnqueueOutcome, RecordingError> {
        let frame = PendingFrame {
            session,
            frame_seq,
            receive_ts_ns,
            monotonic_ns,
            direction,
            opcode,
            flags,
            payload: payload.to_vec(),
        };
        self.lock_ingress()?.queue.push(frame)
    }

    pub fn flush_pending(&self, max: usize) -> Result<usize, RecordingError> {
        let incoming = self.lock_ingress()?.queue.drain_front(max);
        let mut consumer = self.lock_consumer()?;
        consumer.pending.extend(incoming);
        let mut written = 0;
        while written < max {
            let Some(frame) = consumer.pending.front().cloned() else {
                break;
            };
            consumer.pipeline.write_pending_frame(frame)?;
            let _ = consumer.pending.pop_front();
            written += 1;
        }
        Ok(written)
    }

    pub fn rotate_now(&self, start_ts_ns: i64) -> Result<(), RecordingError> {
        self.lock_consumer()?.pipeline.rotate_now(start_ts_ns)
    }

    pub fn shutdown_drain(&self, deadline: Instant) -> Result<(), RecordingError> {
        loop {
            if Instant::now() >= deadline {
                return Err(RecordingError::ShutdownTimeout {
                    remaining: self.snapshot()?.queue_len,
                });
            }
            let moved = self.lock_ingress()?.queue.drain_front(4096);
            let mut consumer = self.lock_consumer()?;
            consumer.pending.extend(moved);
            let Some(frame) = consumer.pending.front().cloned() else {
                consumer.pipeline.flush_writer()?;
                return Ok(());
            };
            consumer.pipeline.write_pending_frame(frame)?;
            let _ = consumer.pending.pop_front();
        }
    }

    pub fn snapshot(&self) -> Result<RecordingSnapshot, RecordingError> {
        let (ingress_len, dropped_total) = {
            let ingress = self.lock_ingress()?;
            (ingress.queue.len(), ingress.queue.dropped_total)
        };
        let consumer = self.lock_consumer()?;
        Ok(RecordingSnapshot {
            directory: consumer.pipeline.directory().to_path_buf(),
            queue_len: ingress_len + consumer.pending.len(),
            rotations: consumer.pipeline.rotations,
            records_written: consumer.pipeline.records_written,
            dropped_total,
            disk_pressure: consumer.pipeline.disk_pressure(),
        })
    }

    /// Return one rate-limited pressure/drop event pair for losses since the
    /// previous poll. The daemon polls this with recording metrics.
    pub fn take_overflow_events(&self) -> Result<Vec<SystemEvent>, RecordingError> {
        let mut ingress = self.lock_ingress()?;
        let dropped_total = ingress.queue.dropped_total;
        let dropped = dropped_total.saturating_sub(ingress.last_reported_dropped);
        if dropped == 0 {
            return Ok(Vec::new());
        }
        ingress.last_reported_dropped = dropped_total;
        Ok(vec![
            SystemEvent::QueuePressure {
                detail: "recording queue full".into(),
            },
            SystemEvent::EventsDropped {
                count: dropped,
                detail: format!("recording_queue dropped_total={dropped_total}"),
            },
        ])
    }
}

impl RecordingPipeline {
    pub fn open(cfg: PipelineConfig) -> Result<Self, RecordingError> {
        Self::open_with_metadata(cfg, vec![MetadataRecord::current_build()])
    }

    pub fn open_with_metadata(
        cfg: PipelineConfig,
        metadata: Vec<MetadataRecord>,
    ) -> Result<Self, RecordingError> {
        if cfg.queue_capacity == 0 {
            return Err(RecordingError::Io("queue_capacity must be > 0".into()));
        }
        validate_metadata_registry(&metadata)?;
        create_dir_all(&cfg.directory)?;
        probe_directory_writable(&cfg.directory)?;
        Ok(Self {
            queue: RecordingQueue::new(cfg.queue_capacity, cfg.overflow),
            cfg,
            writer: None,
            current_path: None,
            current_start_ts_ns: None,
            segment_bytes: 0,
            segment_started: Instant::now(),
            segment_seq: 0,
            free_space: None,
            free_space_check_interval: Duration::from_secs(1),
            last_free_space_check: None,
            disk_pressure_active: false,
            system_events: Vec::new(),
            records_written: 0,
            segments_opened: 0,
            rotations: 0,
            metadata,
        })
    }

    pub fn register_metadata(&mut self, metadata: MetadataRecord) -> Result<(), RecordingError> {
        let key = metadata.stable_key();
        if let Some(existing) = self
            .metadata
            .iter()
            .find(|existing| existing.stable_key() == key)
        {
            return if existing == &metadata {
                Ok(())
            } else {
                Err(RecordingError::MetadataConflict { key })
            };
        }
        let encoded_len = encode_metadata(&metadata)?.len();
        if let Some(writer) = self.writer.as_mut() {
            let start_ts_ns = self.current_start_ts_ns.unwrap_or_default();
            writer.write_metadata(&metadata, start_ts_ns)?;
            self.segment_bytes = self
                .segment_bytes
                .saturating_add((4 + RAW_HEADER_BODY_LEN + encoded_len) as u64);
        }
        self.metadata.push(metadata);
        Ok(())
    }

    pub fn set_free_space_probe(&mut self, probe: FreeSpaceProbe) {
        self.set_free_space_probe_with_interval(probe, Duration::from_secs(1));
    }

    /// Install a probe with an explicit minimum interval. `Duration::ZERO` is
    /// useful for deterministic tests; production callers should stay throttled.
    pub fn set_free_space_probe_with_interval(
        &mut self,
        probe: FreeSpaceProbe,
        interval: Duration,
    ) {
        self.free_space = Some(probe);
        self.free_space_check_interval = interval;
        self.last_free_space_check = None;
    }

    pub fn directory(&self) -> &Path {
        &self.cfg.directory
    }

    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }

    pub fn current_segment(&self) -> Option<&Path> {
        self.current_path.as_deref()
    }

    pub fn disk_pressure(&self) -> bool {
        self.disk_pressure_active
    }

    /// Enqueue a frame; does not touch disk (call [`Self::flush_pending`]).
    pub fn enqueue(
        &mut self,
        session: SessionId,
        frame_seq: u64,
        receive_ts_ns: i64,
        monotonic_ns: u64,
        direction: Direction,
        opcode: FrameOpcode,
        flags: u8,
        payload: &[u8],
    ) -> Result<EnqueueOutcome, RecordingError> {
        self.refresh_disk_pressure()?;
        self.fail_closed_if_disk_full()?;
        let outcome = self.queue.push(PendingFrame {
            session,
            frame_seq,
            receive_ts_ns,
            monotonic_ns,
            direction,
            opcode,
            flags,
            payload: payload.to_vec(),
        })?;
        for ev in RecordingQueue::overflow_events(outcome, self.queue.dropped_total) {
            self.system_events.push(ev);
        }
        Ok(outcome)
    }

    /// Write queued frames until empty or `max` writes.
    pub fn flush_pending(&mut self, max: usize) -> Result<usize, RecordingError> {
        let mut n = 0;
        while n < max {
            self.refresh_disk_pressure()?;
            self.fail_closed_if_disk_full()?;
            let Some(frame) = self.queue.pop_front() else {
                break;
            };
            self.write_frame(frame)?;
            n += 1;
        }
        Ok(n)
    }

    /// Drain the queue and flush the open segment within `deadline`.
    pub fn shutdown_drain(&mut self, deadline: Instant) -> Result<(), RecordingError> {
        self.system_events.push(SystemEvent::ShutdownStarted);
        while !self.queue.is_empty() {
            if Instant::now() >= deadline {
                return Err(RecordingError::ShutdownTimeout {
                    remaining: self.queue.len(),
                });
            }
            self.flush_pending(64)?;
        }
        if let Some(w) = self.writer.as_mut() {
            w.flush()?;
        }
        self.system_events.push(SystemEvent::ShutdownCompleted);
        Ok(())
    }

    fn write_pending_frame(&mut self, frame: PendingFrame) -> Result<(), RecordingError> {
        self.refresh_disk_pressure()?;
        self.fail_closed_if_disk_full()?;
        self.write_frame(frame)
    }

    fn flush_writer(&mut self) -> Result<(), RecordingError> {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush()?;
        }
        Ok(())
    }

    /// Force-rotate the current segment (no-op if none open).
    pub fn rotate_now(&mut self, start_ts_ns: i64) -> Result<(), RecordingError> {
        if self.writer.is_some() {
            self.close_segment()?;
            self.rotations += 1;
            self.system_events.push(SystemEvent::RecordingRotated);
        }
        self.open_segment(start_ts_ns)
    }

    fn write_frame(&mut self, frame: PendingFrame) -> Result<(), RecordingError> {
        self.refresh_disk_pressure()?;
        self.fail_closed_if_disk_full()?;
        self.ensure_segment(frame.receive_ts_ns)?;
        if self.should_rotate_before_write() {
            self.rotate_now(frame.receive_ts_ns)?;
        }
        let w = self
            .writer
            .as_mut()
            .ok_or_else(|| RecordingError::Io("no open segment".into()))?;
        w.write_record(
            frame.session,
            frame.frame_seq,
            frame.receive_ts_ns,
            frame.monotonic_ns,
            frame.direction,
            frame.opcode,
            frame.flags,
            &frame.payload,
        )?;
        // record_len = 4 + header body + payload
        let written = 4 + crate::format::RAW_HEADER_BODY_LEN + frame.payload.len();
        self.segment_bytes += written as u64;
        self.records_written += 1;
        if self.should_rotate_after_write() {
            // close; next write opens fresh
            self.close_segment()?;
            self.rotations += 1;
            self.system_events.push(SystemEvent::RecordingRotated);
        }
        Ok(())
    }

    fn ensure_segment(&mut self, start_ts_ns: i64) -> Result<(), RecordingError> {
        if self.writer.is_none() {
            self.open_segment(start_ts_ns)?;
        }
        Ok(())
    }

    fn open_segment(&mut self, start_ts_ns: i64) -> Result<(), RecordingError> {
        create_dir_all(&self.cfg.directory)?;
        let name = format!("seg-{}-{:06}.mfr1", start_ts_ns, self.segment_seq);
        let path = self.cfg.directory.join(name);
        let file = File::create(&path)?;
        let mut writer = RawSegmentWriter::create(BufWriter::new(file), start_ts_ns)?;
        let mut segment_bytes = HEADER_SIZE as u64;
        for metadata in &self.metadata {
            let encoded_len = encode_metadata(metadata)?.len();
            writer.write_metadata(metadata, start_ts_ns)?;
            segment_bytes =
                segment_bytes.saturating_add((4 + RAW_HEADER_BODY_LEN + encoded_len) as u64);
        }
        self.writer = Some(writer);
        self.current_path = Some(path);
        self.current_start_ts_ns = Some(start_ts_ns);
        self.segment_bytes = segment_bytes;
        self.segment_started = Instant::now();
        self.segment_seq += 1;
        self.segments_opened += 1;
        Ok(())
    }

    fn close_segment(&mut self) -> Result<(), RecordingError> {
        if let Some(mut w) = self.writer.take() {
            w.flush()?;
        }
        self.current_path = None;
        self.current_start_ts_ns = None;
        self.segment_bytes = 0;
        Ok(())
    }

    fn should_rotate_before_write(&self) -> bool {
        let dur = self.cfg.rotation.max_duration;
        !dur.is_zero() && self.segment_started.elapsed() >= dur && self.writer.is_some()
    }

    fn should_rotate_after_write(&self) -> bool {
        let max = self.cfg.rotation.max_bytes;
        max > 0 && self.segment_bytes >= max && self.writer.is_some()
    }

    fn refresh_disk_pressure(&mut self) -> Result<(), RecordingError> {
        let min = self.cfg.min_free_bytes;
        if min == 0 {
            return Ok(());
        }
        let Some(probe) = self.free_space.as_mut() else {
            return Ok(());
        };
        if self
            .last_free_space_check
            .is_some_and(|last| last.elapsed() < self.free_space_check_interval)
        {
            return Ok(());
        }
        self.last_free_space_check = Some(Instant::now());
        let Some(free) = probe(&self.cfg.directory) else {
            return Ok(());
        };
        if free < min {
            if !self.disk_pressure_active {
                self.disk_pressure_active = true;
                self.system_events.push(SystemEvent::DiskPressure);
            }
        } else if self.disk_pressure_active {
            self.disk_pressure_active = false;
            self.system_events.push(SystemEvent::SinkStateChanged {
                state: "disk_ok".into(),
            });
        }
        Ok(())
    }

    /// Lossless recording (`FailEngine`): refuse enqueue/write while under disk pressure.
    fn fail_closed_if_disk_full(&self) -> Result<(), RecordingError> {
        if self.disk_pressure_active && self.cfg.overflow == OverflowPolicy::FailEngine {
            return Err(RecordingError::DiskFull);
        }
        Ok(())
    }
}

fn validate_metadata_registry(metadata: &[MetadataRecord]) -> Result<(), RecordingError> {
    let mut keys = std::collections::HashSet::new();
    for record in metadata {
        let _ = encode_metadata(record)?;
        if !keys.insert(record.stable_key()) {
            return Err(RecordingError::InvalidHeader);
        }
    }
    Ok(())
}

fn probe_directory_writable(directory: &Path) -> Result<(), RecordingError> {
    static PROBE_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let sequence = PROBE_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = directory.join(format!(
        ".marketfeed-write-probe-{}-{sequence}",
        std::process::id()
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    let write_result = file
        .write_all(b"marketfeed recording probe")
        .and_then(|()| file.sync_all());
    drop(file);
    let remove_result = remove_file(path);
    write_result?;
    remove_result?;
    Ok(())
}

/// Parse `df -kP` available kilobytes for `path` (portable ops probe).
///
/// # ponytail
/// Shells out to `df`; ceiling = fork cost / locale quirks; upgrade = libc statvfs.
pub fn df_free_bytes(path: &Path) -> Option<u64> {
    let out = std::process::Command::new("df")
        .args(["-kP"])
        .arg(path)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let line = text.lines().nth(1)?;
    let avail_kb: u64 = line.split_whitespace().nth(3)?.parse().ok()?;
    Some(avail_kb.saturating_mul(1024))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::RawSegmentReader;
    use crate::{HEADER_SIZE, SessionRecordingMetadata, decode_metadata};
    use marketfeed_adapter_api::{ConcreteSubscriptionSet, SessionSpec};
    use marketfeed_model::{CatalogVersion, CatalogView, VenueId};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    fn unique_dir(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "marketfeed-{prefix}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ))
    }

    fn tiny_cfg(dir: PathBuf, max_bytes: u64) -> PipelineConfig {
        PipelineConfig {
            directory: dir,
            queue_capacity: 64,
            overflow: OverflowPolicy::FailEngine,
            rotation: RotationConfig {
                max_bytes,
                max_duration: Duration::ZERO,
            },
            min_free_bytes: 0,
        }
    }

    #[test]
    fn rotates_on_size_and_emits_system_event() {
        let dir = unique_dir("rot");
        let _ = std::fs::remove_dir_all(&dir);
        let mut p = RecordingPipeline::open(tiny_cfg(dir.clone(), 200)).unwrap();
        for i in 0..20 {
            p.enqueue(
                SessionId(1),
                i,
                1_000 + i as i64,
                i,
                Direction::Inbound,
                FrameOpcode::Text,
                0,
                b"hello-payload-bytes",
            )
            .unwrap();
        }
        p.flush_pending(100).unwrap();
        // Ensure last open segment is flushed to disk before reading.
        p.shutdown_drain(Instant::now() + Duration::from_secs(1))
            .unwrap();
        assert!(p.rotations >= 1, "expected rotation, got {}", p.rotations);
        assert!(
            p.system_events
                .iter()
                .any(|e| matches!(e, SystemEvent::RecordingRotated)),
            "{:?}",
            p.system_events
        );
        let segs: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("mfr1"))
            .collect();
        assert!(segs.len() >= 2, "expected >=2 segments, got {}", segs.len());
        for e in &segs {
            let bytes = std::fs::read(e.path()).unwrap();
            assert!(
                bytes.len() >= HEADER_SIZE,
                "segment too small: {} bytes at {:?}",
                bytes.len(),
                e.path()
            );
            let mut r = RawSegmentReader::from_bytes(bytes).unwrap();
            let _ = r.read_all().unwrap();
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn every_rotated_segment_repeats_build_and_session_metadata() {
        let dir = unique_dir("metadata-rotation");
        let _ = std::fs::remove_dir_all(&dir);
        let build = MetadataRecord::current_build();
        let spec = SessionSpec {
            endpoint_name: "wss://example.invalid/ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        };
        let catalog = CatalogView::new(VenueId(9), CatalogVersion(7));
        let session = MetadataRecord::Session(SessionRecordingMetadata::from_plan(
            SessionId(42),
            VenueId(9),
            "example",
            "test",
            &spec,
            &catalog,
        ));
        let mut pipeline =
            RecordingPipeline::open_with_metadata(tiny_cfg(dir.clone(), 600), vec![build.clone()])
                .unwrap();
        pipeline.register_metadata(session.clone()).unwrap();
        for sequence in 1..=8 {
            pipeline
                .enqueue(
                    SessionId(42),
                    sequence,
                    sequence as i64,
                    sequence,
                    Direction::Inbound,
                    FrameOpcode::Text,
                    0,
                    b"frame-payload",
                )
                .unwrap();
        }
        pipeline.flush_pending(32).unwrap();
        pipeline
            .shutdown_drain(Instant::now() + Duration::from_secs(1))
            .unwrap();

        let segments: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("mfr1"))
            .collect();
        assert!(segments.len() >= 2, "expected metadata rotation");
        for path in segments {
            let mut reader = RawSegmentReader::from_bytes(std::fs::read(path).unwrap()).unwrap();
            let metadata: Vec<_> = reader
                .read_all()
                .unwrap()
                .into_iter()
                .filter(|record| record.header.opcode == FrameOpcode::Metadata)
                .map(|record| decode_metadata(&record.payload).unwrap())
                .collect();
            assert_eq!(metadata, vec![build.clone(), session.clone()]);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn conflicting_session_metadata_is_rejected() {
        let dir = unique_dir("metadata-conflict");
        let _ = std::fs::remove_dir_all(&dir);
        let spec = SessionSpec {
            endpoint_name: "wss://one.invalid".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        };
        let catalog = CatalogView::new(VenueId(1), CatalogVersion(1));
        let first = MetadataRecord::Session(SessionRecordingMetadata::from_plan(
            SessionId(1),
            VenueId(1),
            "example",
            "test",
            &spec,
            &catalog,
        ));
        let mut second = first.clone();
        let MetadataRecord::Session(second_session) = &mut second else {
            unreachable!();
        };
        second_session.endpoint = "wss://two.invalid".into();

        let mut pipeline = RecordingPipeline::open(tiny_cfg(dir.clone(), 0)).unwrap();
        pipeline.register_metadata(first).unwrap();
        assert_eq!(
            pipeline.register_metadata(second),
            Err(RecordingError::MetadataConflict {
                key: "session:1".into()
            })
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn disk_pressure_signal_from_probe() {
        let dir = unique_dir("disk");
        let _ = std::fs::remove_dir_all(&dir);
        let mut cfg = tiny_cfg(dir.clone(), 0);
        cfg.min_free_bytes = 10_000;
        cfg.overflow = OverflowPolicy::DropOldest;
        let mut p = RecordingPipeline::open(cfg).unwrap();
        let free = Arc::new(Mutex::new(100u64));
        let free2 = Arc::clone(&free);
        p.set_free_space_probe_with_interval(
            Box::new(move |_| Some(*free2.lock().unwrap())),
            Duration::ZERO,
        );
        p.enqueue(
            SessionId(1),
            1,
            1,
            1,
            Direction::Inbound,
            FrameOpcode::Text,
            0,
            b"x",
        )
        .unwrap();
        assert!(
            p.system_events
                .iter()
                .any(|e| matches!(e, SystemEvent::DiskPressure))
        );
        assert!(p.disk_pressure());
        *free.lock().unwrap() = 1_000_000;
        p.enqueue(
            SessionId(1),
            2,
            2,
            2,
            Direction::Inbound,
            FrameOpcode::Text,
            0,
            b"y",
        )
        .unwrap();
        assert!(
            p.system_events.iter().any(
                |e| matches!(e, SystemEvent::SinkStateChanged { state } if state == "disk_ok")
            )
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn disk_full_fail_closed_under_fail_engine() {
        let dir = unique_dir("disk-full");
        let _ = std::fs::remove_dir_all(&dir);
        let mut cfg = tiny_cfg(dir.clone(), 0);
        cfg.min_free_bytes = 10_000;
        cfg.overflow = OverflowPolicy::FailEngine;
        let mut p = RecordingPipeline::open(cfg).unwrap();
        p.set_free_space_probe(Box::new(|_| Some(0)));
        let err = p
            .enqueue(
                SessionId(1),
                1,
                1,
                1,
                Direction::Inbound,
                FrameOpcode::Text,
                0,
                b"x",
            )
            .unwrap_err();
        assert_eq!(err, RecordingError::DiskFull);
        assert!(p.disk_pressure());
        assert!(
            p.system_events
                .iter()
                .any(|e| matches!(e, SystemEvent::DiskPressure))
        );
        assert_eq!(p.queue_len(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn free_space_probe_is_throttled_on_hot_enqueue_path() {
        let dir = unique_dir("disk-throttle");
        let _ = std::fs::remove_dir_all(&dir);
        let mut cfg = tiny_cfg(dir.clone(), 0);
        cfg.min_free_bytes = 1;
        let mut pipeline = RecordingPipeline::open(cfg).unwrap();
        let calls = Arc::new(AtomicU64::new(0));
        let calls_probe = Arc::clone(&calls);
        pipeline.set_free_space_probe(Box::new(move |_| {
            calls_probe.fetch_add(1, Ordering::Relaxed);
            Some(u64::MAX)
        }));

        for seq in 0..10 {
            pipeline
                .enqueue(
                    SessionId(1),
                    seq,
                    seq as i64,
                    seq,
                    Direction::Inbound,
                    FrameOpcode::Text,
                    0,
                    b"x",
                )
                .unwrap();
        }

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn disk_pressure_during_flush_keeps_pending_frame_queued() {
        let dir = unique_dir("disk-flush");
        let _ = std::fs::remove_dir_all(&dir);
        let mut cfg = tiny_cfg(dir.clone(), 0);
        cfg.min_free_bytes = 1;
        let mut pipeline = RecordingPipeline::open(cfg).unwrap();
        pipeline
            .enqueue(
                SessionId(1),
                1,
                1,
                1,
                Direction::Inbound,
                FrameOpcode::Text,
                0,
                b"x",
            )
            .unwrap();
        pipeline.set_free_space_probe_with_interval(Box::new(|_| Some(0)), Duration::ZERO);

        assert_eq!(
            pipeline.flush_pending(1).unwrap_err(),
            RecordingError::DiskFull
        );
        assert_eq!(pipeline.queue_len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shutdown_drains_queue() {
        let dir = unique_dir("drain");
        let _ = std::fs::remove_dir_all(&dir);
        let mut p = RecordingPipeline::open(tiny_cfg(dir.clone(), 0)).unwrap();
        for i in 0..10 {
            p.enqueue(
                SessionId(1),
                i,
                i as i64,
                i,
                Direction::Inbound,
                FrameOpcode::Text,
                0,
                b"drain-me",
            )
            .unwrap();
        }
        assert_eq!(p.queue_len(), 10);
        p.shutdown_drain(Instant::now() + Duration::from_secs(2))
            .unwrap();
        assert_eq!(p.queue_len(), 0);
        assert_eq!(p.records_written, 10);
        assert!(
            p.system_events
                .iter()
                .any(|e| matches!(e, SystemEvent::ShutdownStarted))
        );
        assert!(
            p.system_events
                .iter()
                .any(|e| matches!(e, SystemEvent::ShutdownCompleted))
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shutdown_timeout_when_deadline_passed() {
        let dir = unique_dir("to");
        let _ = std::fs::remove_dir_all(&dir);
        let mut p = RecordingPipeline::open(tiny_cfg(dir.clone(), 0)).unwrap();
        p.enqueue(
            SessionId(1),
            1,
            1,
            1,
            Direction::Inbound,
            FrameOpcode::Text,
            0,
            b"x",
        )
        .unwrap();
        let err = p
            .shutdown_drain(Instant::now() - Duration::from_millis(1))
            .unwrap_err();
        assert!(matches!(err, RecordingError::ShutdownTimeout { .. }));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn handle_enqueue_does_not_wait_for_consumer_disk_lock() {
        let dir = unique_dir("handle-nonblocking");
        let _ = std::fs::remove_dir_all(&dir);
        let handle =
            RecordingHandle::new(RecordingPipeline::open(tiny_cfg(dir.clone(), 0)).unwrap());
        let consumer_guard = handle.consumer.lock().unwrap();
        let producer = handle.clone();
        let (sent, received) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let result = producer.enqueue(
                SessionId(1),
                1,
                1,
                1,
                Direction::Inbound,
                FrameOpcode::Text,
                0,
                b"x",
            );
            sent.send(result).unwrap();
        });

        assert_eq!(
            received.recv_timeout(Duration::from_millis(250)).unwrap(),
            Ok(EnqueueOutcome::Accepted)
        );
        drop(consumer_guard);
        thread.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn handle_exposes_rate_limited_drop_metrics_and_events() {
        let dir = unique_dir("handle-drops");
        let _ = std::fs::remove_dir_all(&dir);
        let mut cfg = tiny_cfg(dir.clone(), 0);
        cfg.queue_capacity = 1;
        cfg.overflow = OverflowPolicy::DropNewest;
        let handle = RecordingHandle::new(RecordingPipeline::open(cfg).unwrap());
        assert_eq!(
            handle
                .enqueue(
                    SessionId(1),
                    1,
                    1,
                    1,
                    Direction::Inbound,
                    FrameOpcode::Text,
                    0,
                    b"x",
                )
                .unwrap(),
            EnqueueOutcome::Accepted
        );
        assert_eq!(
            handle
                .enqueue(
                    SessionId(1),
                    2,
                    2,
                    2,
                    Direction::Inbound,
                    FrameOpcode::Text,
                    0,
                    b"y",
                )
                .unwrap(),
            EnqueueOutcome::DroppedNewest
        );

        assert_eq!(handle.snapshot().unwrap().dropped_total, 1);
        assert_eq!(handle.take_overflow_events().unwrap().len(), 2);
        assert!(handle.take_overflow_events().unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn open_rejects_existing_unwritable_recording_directory() {
        use std::os::unix::fs::PermissionsExt;

        let dir = unique_dir("unwritable");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
        let result = RecordingPipeline::open(tiny_cfg(dir.clone(), 0));
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        assert!(result.is_err(), "readiness requires a writable directory");
    }
}
