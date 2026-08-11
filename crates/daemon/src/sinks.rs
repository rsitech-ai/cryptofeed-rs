//! Build and fan-out configured `[[sinks]]` (memory / logging / file /
//! protobuf-file / protobuf-file-bin / udp / spill-wal / kafka / nats).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

use marketfeed_adapter_api::EventBatch;
use marketfeed_dispatch::PushOutcome;
use marketfeed_model::SystemEvent;
#[cfg(feature = "ui-api")]
use marketfeed_model::VenueId;
use marketfeed_sinks::{
    EventSink, FileSink, LoggingSink, MemorySink, ProtobufBinaryFileSink, ProtobufFileSink,
    SinkError, SpillWalConfig, SpillWalSink, UdpSink,
};

#[cfg(feature = "kafka")]
use marketfeed_sinks::KafkaSink;
#[cfg(feature = "nats")]
use marketfeed_sinks::NatsSink;

use crate::config::{DaemonConfig, SinkKind};
#[cfg(feature = "ui-api")]
use crate::view::{SharedViewPlane, ViewPlane};

#[cfg(feature = "ui-api")]
fn run_view_work<R>(work: impl FnOnce() -> R) -> R {
    let on_multi_thread_runtime = tokio::runtime::Handle::try_current()
        .is_ok_and(|handle| handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread);
    if on_multi_thread_runtime {
        tokio::task::block_in_place(work)
    } else {
        work()
    }
}

fn merge_outcome(aggregate: &mut PushOutcome, next: PushOutcome) {
    match (&mut *aggregate, next) {
        (PushOutcome::Accepted, non_accepted) => *aggregate = non_accepted,
        (PushOutcome::DroppedOldest { dropped: total }, PushOutcome::DroppedOldest { dropped }) => {
            *total = total.saturating_add(dropped)
        }
        _ => {}
    }
}

fn merge_result(
    aggregate: &mut PushOutcome,
    first_error: &mut Option<SinkError>,
    result: Result<PushOutcome, SinkError>,
) {
    match result {
        Ok(outcome) => merge_outcome(aggregate, outcome),
        Err(error) if first_error.is_none() => *first_error = Some(error),
        Err(_) => {}
    }
}

#[derive(Debug)]
enum SinkItem {
    Batch(EventBatch),
    System(SystemEvent),
}

#[derive(Debug)]
struct SinkMailboxState {
    items: VecDeque<SinkItem>,
    closed: bool,
    in_flight: u64,
}

/// One bounded FIFO for both event kinds, preserving the order observed by a sink.
#[derive(Debug)]
struct SinkMailbox {
    capacity: usize,
    policy: marketfeed_model::OverflowPolicy,
    state: Mutex<SinkMailboxState>,
    wake: Condvar,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkSnapshot {
    pub id: String,
    pub kind: &'static str,
    pub required: bool,
    pub healthy: bool,
    pub queue_len: usize,
    pub queue_capacity: usize,
    pub in_flight: u64,
    pub enqueued: u64,
    pub dropped: u64,
    pub errors: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Default)]
struct SinkWorkerHealth {
    healthy: AtomicBool,
    enqueued: AtomicU64,
    dropped: AtomicU64,
    errors: AtomicU64,
    last_error: Mutex<Option<String>>,
}

/// Owns one concrete sink on a dedicated thread behind a bounded FIFO mailbox.
pub struct SinkWorker<T: EventSink + Send + 'static> {
    id: String,
    kind: &'static str,
    required: bool,
    mailbox: Arc<SinkMailbox>,
    sink: Arc<Mutex<T>>,
    health: Arc<SinkWorkerHealth>,
    worker: Option<JoinHandle<()>>,
}

impl<T: EventSink + Send + 'static> std::fmt::Debug for SinkWorker<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkWorker")
            .field("snapshot", &self.snapshot())
            .finish_non_exhaustive()
    }
}

impl<T: EventSink + Send + 'static> SinkWorker<T> {
    fn start(
        id: String,
        kind: &'static str,
        required: bool,
        capacity: usize,
        policy: marketfeed_model::OverflowPolicy,
        sink: T,
    ) -> Result<Self, String> {
        let mailbox = Arc::new(SinkMailbox::new(capacity, policy));
        let shared_sink = Arc::new(Mutex::new(sink));
        let health = Arc::new(SinkWorkerHealth::default());
        let worker_mailbox = Arc::clone(&mailbox);
        let worker_sink = Arc::clone(&shared_sink);
        let worker_health = Arc::clone(&health);
        let worker_id = id.clone();
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::Builder::new()
            .name(format!("sink-{id}"))
            .spawn(move || {
                worker_health.healthy.store(true, Ordering::Release);
                if started_tx.send(()).is_err() {
                    worker_health.healthy.store(false, Ordering::Release);
                    return;
                }
                while let Some(item) = worker_mailbox.wait_pop() {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let mut sink = worker_sink.lock().expect("sink implementation lock");
                        match item {
                            SinkItem::Batch(batch) => sink.push_batch(batch),
                            SinkItem::System(event) => sink.push_system(event),
                        }
                    }))
                    .unwrap_or_else(|_| Err(SinkError::Io("sink worker panicked".into())));
                    match result {
                        Ok(PushOutcome::Accepted) => {
                            worker_mailbox.finish_item(false);
                        }
                        Ok(PushOutcome::DroppedNewest) => {
                            worker_health.dropped.fetch_add(1, Ordering::Relaxed);
                            worker_mailbox.finish_item(false);
                        }
                        Ok(PushOutcome::DroppedOldest { dropped }) => {
                            worker_health
                                .dropped
                                .fetch_add(dropped as u64, Ordering::Relaxed);
                            worker_mailbox.finish_item(false);
                        }
                        Err(error) => {
                            *worker_health.last_error.lock().expect("sink error lock") =
                                Some(error.to_string());
                            worker_health.errors.fetch_add(1, Ordering::Relaxed);
                            worker_health.healthy.store(false, Ordering::Release);
                            tracing::error!(sink_id = %worker_id, %error, "sink worker failed");
                            let discarded = worker_mailbox.finish_item(true);
                            worker_health
                                .dropped
                                .fetch_add(discarded as u64 + 1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            })
            .map_err(|error| format!("spawn sink worker {id}: {error}"))?;
        if let Err(error) = started_rx.recv_timeout(std::time::Duration::from_secs(1)) {
            mailbox.close();
            let _ = worker.join();
            return Err(format!("start sink worker {id}: {error}"));
        }
        Ok(Self {
            id,
            kind,
            required,
            mailbox,
            sink: shared_sink,
            health,
            worker: Some(worker),
        })
    }

    fn enqueue(&self, item: SinkItem) -> Result<PushOutcome, SinkError> {
        let result = self.mailbox.push(item);
        match &result {
            Ok(PushOutcome::Accepted) => {
                self.health.enqueued.fetch_add(1, Ordering::Relaxed);
            }
            Ok(PushOutcome::DroppedNewest) => {
                self.health.dropped.fetch_add(1, Ordering::Relaxed);
            }
            Ok(PushOutcome::DroppedOldest { dropped }) => {
                self.health.enqueued.fetch_add(1, Ordering::Relaxed);
                self.health
                    .dropped
                    .fetch_add(*dropped as u64, Ordering::Relaxed);
            }
            Err(SinkError::Io(_)) if !self.required => {
                self.health.dropped.fetch_add(1, Ordering::Relaxed);
                return Ok(PushOutcome::DroppedNewest);
            }
            Err(_) => {}
        }
        result
    }

    fn push_batch(&self, batch: EventBatch) -> Result<PushOutcome, SinkError> {
        self.enqueue(SinkItem::Batch(batch))
    }

    fn push_system(&self, event: SystemEvent) -> Result<PushOutcome, SinkError> {
        self.enqueue(SinkItem::System(event))
    }

    fn snapshot(&self) -> SinkSnapshot {
        let (queue_len, in_flight) = self.mailbox.status();
        SinkSnapshot {
            id: self.id.clone(),
            kind: self.kind,
            required: self.required,
            healthy: self.health.healthy.load(Ordering::Acquire),
            queue_len,
            queue_capacity: self.mailbox.capacity,
            in_flight,
            enqueued: self.health.enqueued.load(Ordering::Relaxed),
            dropped: self.health.dropped.load(Ordering::Relaxed),
            errors: self.health.errors.load(Ordering::Relaxed),
            last_error: self
                .health
                .last_error
                .lock()
                .expect("sink error lock")
                .clone(),
        }
    }

    fn with_sink<R>(&self, inspect: impl FnOnce(&T) -> R) -> R {
        inspect(&self.sink.lock().expect("sink implementation lock"))
    }

    fn wait_for_drain(&self, deadline: std::time::Instant) -> Result<(), String> {
        loop {
            let snapshot = self.snapshot();
            if snapshot.required && !snapshot.healthy {
                return Err(format!(
                    "required sink {} ({}) unhealthy during shutdown: {}",
                    snapshot.id,
                    snapshot.kind,
                    snapshot.last_error.as_deref().unwrap_or("unknown error")
                ));
            }
            if snapshot.queue_len == 0 && snapshot.in_flight == 0 {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                return Err(format!(
                    "sink {} drain deadline exceeded (queue:in_flight={}:{})",
                    snapshot.id, snapshot.queue_len, snapshot.in_flight
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    fn shutdown(&mut self, deadline: std::time::Instant) -> Result<(), String> {
        let drain_result = self.wait_for_drain(deadline);
        self.mailbox.close();
        let Some(worker) = self.worker.take() else {
            return drain_result;
        };
        if (drain_result.is_ok() || worker.is_finished()) && worker.join().is_err() {
            return Err(format!("sink {} worker panicked during shutdown", self.id));
        }
        drain_result
    }
}

impl<T: EventSink + Send + 'static> Drop for SinkWorker<T> {
    fn drop(&mut self) {
        self.mailbox.close();
        if self.worker.as_ref().is_some_and(JoinHandle::is_finished) {
            if let Some(worker) = self.worker.take() {
                let _ = worker.join();
            }
        } else {
            // A worker stuck in external I/O must not make process drop unbounded.
            let _ = self.worker.take();
        }
    }
}

impl<T: EventSink + Send + 'static> EventSink for SinkWorker<T> {
    fn push_batch(&mut self, batch: EventBatch) -> Result<PushOutcome, SinkError> {
        SinkWorker::push_batch(self, batch)
    }

    fn push_system(&mut self, event: SystemEvent) -> Result<PushOutcome, SinkError> {
        SinkWorker::push_system(self, event)
    }
}

impl SinkMailbox {
    fn new(capacity: usize, policy: marketfeed_model::OverflowPolicy) -> Self {
        assert!(capacity > 0, "sink mailbox capacity must be > 0");
        Self {
            capacity,
            policy,
            state: Mutex::new(SinkMailboxState {
                items: VecDeque::with_capacity(capacity),
                closed: false,
                in_flight: 0,
            }),
            wake: Condvar::new(),
        }
    }

    fn push(&self, item: SinkItem) -> Result<PushOutcome, SinkError> {
        let mut state = self.state.lock().expect("sink mailbox lock");
        if state.closed {
            return Err(SinkError::Io("sink worker is closed".into()));
        }
        let outcome = if state.items.len() < self.capacity {
            state.items.push_back(item);
            PushOutcome::Accepted
        } else {
            match self.policy {
                marketfeed_model::OverflowPolicy::DropNewest => PushOutcome::DroppedNewest,
                marketfeed_model::OverflowPolicy::DropOldest => {
                    let _ = state.items.pop_front();
                    state.items.push_back(item);
                    PushOutcome::DroppedOldest { dropped: 1 }
                }
                marketfeed_model::OverflowPolicy::FailEngine => return Err(SinkError::FailEngine),
                marketfeed_model::OverflowPolicy::BlockWithDeadline => {
                    return Err(SinkError::DeadlineExceeded);
                }
                other => return Err(SinkError::UnsupportedPolicy(other)),
            }
        };
        self.wake.notify_one();
        Ok(outcome)
    }

    #[cfg(test)]
    fn pop(&self) -> Option<SinkItem> {
        self.state
            .lock()
            .expect("sink mailbox lock")
            .items
            .pop_front()
    }

    fn wait_pop(&self) -> Option<SinkItem> {
        let mut state = self.state.lock().expect("sink mailbox lock");
        loop {
            if let Some(item) = state.items.pop_front() {
                state.in_flight = state.in_flight.saturating_add(1);
                return Some(item);
            }
            if state.closed {
                return None;
            }
            state = self.wake.wait(state).expect("sink mailbox wait");
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.state.lock().expect("sink mailbox lock").items.len()
    }

    fn status(&self) -> (usize, u64) {
        let state = self.state.lock().expect("sink mailbox lock");
        (state.items.len(), state.in_flight)
    }

    fn finish_item(&self, close_and_discard: bool) -> usize {
        let mut state = self.state.lock().expect("sink mailbox lock");
        let discarded = if close_and_discard {
            state.closed = true;
            let discarded = state.items.len();
            state.items.clear();
            discarded
        } else {
            0
        };
        state.in_flight = state
            .in_flight
            .checked_sub(1)
            .expect("sink item completion without an in-flight item");
        self.wake.notify_all();
        discarded
    }

    fn close(&self) {
        let mut state = self.state.lock().expect("sink mailbox lock");
        state.closed = true;
        self.wake.notify_all();
    }
}

/// Process-wide sink set built from config. Empty ⇒ live loop null-drains.
#[derive(Debug, Default)]
pub struct DaemonSinks {
    pub memory: Vec<SinkWorker<MemorySink>>,
    pub logging: Vec<SinkWorker<LoggingSink>>,
    pub file: Vec<SinkWorker<FileSink>>,
    pub protobuf_file: Vec<SinkWorker<ProtobufFileSink>>,
    pub protobuf_file_bin: Vec<SinkWorker<ProtobufBinaryFileSink>>,
    pub udp: Vec<SinkWorker<UdpSink>>,
    pub spill_wal: Vec<SinkWorker<SpillWalSink>>,
    #[cfg(feature = "kafka")]
    pub kafka: Vec<SinkWorker<KafkaSink>>,
    #[cfg(feature = "nats")]
    pub nats: Vec<SinkWorker<NatsSink>>,
}

impl DaemonSinks {
    pub fn from_config(config: &DaemonConfig) -> Result<Self, String> {
        let mut out = Self::default();
        for (index, sink) in config.sinks.iter().enumerate() {
            let policy = sink.overflow_policy().map_err(|e| e.to_string())?;
            let cap = sink.capacity;
            let id = sink
                .id
                .clone()
                .unwrap_or_else(|| format!("{}-{index}", sink.sink_type));
            let required = sink.required;
            match sink.kind().map_err(|e| e.to_string())? {
                SinkKind::Memory => out.memory.push(SinkWorker::start(
                    id,
                    "memory",
                    required,
                    cap,
                    policy,
                    MemorySink::new(cap, cap, policy),
                )?),
                SinkKind::Logging => out.logging.push(SinkWorker::start(
                    id,
                    "logging",
                    required,
                    cap,
                    policy,
                    LoggingSink::new(cap, cap, policy),
                )?),
                SinkKind::File => {
                    let path = sink.file_path().map_err(|e| e.to_string())?;
                    let inner = FileSink::open(path, cap, cap, policy)
                        .map_err(|e| format!("open file sink {path}: {e}"))?;
                    out.file
                        .push(SinkWorker::start(id, "file", required, cap, policy, inner)?);
                }
                SinkKind::ProtobufFile => {
                    let path = sink.file_path().map_err(|e| e.to_string())?;
                    let inner = ProtobufFileSink::open(path, cap, cap, policy)
                        .map_err(|e| format!("open protobuf-file sink {path}: {e}"))?;
                    out.protobuf_file.push(SinkWorker::start(
                        id,
                        "protobuf-file",
                        required,
                        cap,
                        policy,
                        inner,
                    )?);
                }
                SinkKind::ProtobufFileBin => {
                    let path = sink.file_path().map_err(|e| e.to_string())?;
                    let inner = ProtobufBinaryFileSink::open(path, cap, cap, policy)
                        .map_err(|e| format!("open protobuf-file-bin sink {path}: {e}"))?;
                    out.protobuf_file_bin.push(SinkWorker::start(
                        id,
                        "protobuf-file-bin",
                        required,
                        cap,
                        policy,
                        inner,
                    )?);
                }
                SinkKind::Udp => {
                    let dest = sink.udp_address().map_err(|e| e.to_string())?;
                    let inner = UdpSink::connect(dest, cap, cap, policy)
                        .map_err(|e| format!("open udp sink {dest}: {e}"))?;
                    out.udp
                        .push(SinkWorker::start(id, "udp", required, cap, policy, inner)?);
                }
                SinkKind::SpillWal => {
                    let path = sink.file_path().map_err(|e| e.to_string())?;
                    let wal_limit_bytes = sink.wal_limit_bytes().map_err(|e| e.to_string())?;
                    let inner = SpillWalSink::open(SpillWalConfig {
                        path: path.into(),
                        batch_capacity: cap,
                        system_capacity: cap,
                        wal_limit_bytes,
                    })
                    .map_err(|e| format!("open spill-wal sink {path}: {e}"))?;
                    if inner.recovered_len() != 0 {
                        return Err(format!(
                            "spill-wal sink {} contains {} unacknowledged recovery record(s); daemon startup is fail-closed until an explicit recovery consumer processes SpillWalSink::pop_recovered and checkpoints the prefix",
                            inner.path().display(),
                            inner.recovered_len()
                        ));
                    }
                    out.spill_wal.push(SinkWorker::start(
                        id,
                        "spill-wal",
                        required,
                        cap,
                        marketfeed_model::OverflowPolicy::FailEngine,
                        inner,
                    )?);
                }
                SinkKind::Kafka => {
                    #[cfg(feature = "kafka")]
                    {
                        let dest = sink.socket_address().map_err(|e| e.to_string())?;
                        let topic = sink.kafka_topic().map_err(|e| e.to_string())?;
                        let inner = KafkaSink::connect(dest, topic, cap, cap, policy)
                            .map_err(|e| format!("open kafka sink {dest} topic={topic}: {e}"))?;
                        out.kafka.push(SinkWorker::start(
                            id, "kafka", required, cap, policy, inner,
                        )?);
                    }
                    #[cfg(not(feature = "kafka"))]
                    {
                        let _ = (sink, policy, cap);
                        return Err(
                            "sink type=kafka requires marketfeed-daemon feature `kafka` (TCP Produce v0)"
                                .into(),
                        );
                    }
                }
                SinkKind::Nats => {
                    #[cfg(feature = "nats")]
                    {
                        let dest = sink.socket_address().map_err(|e| e.to_string())?;
                        let subject = sink.nats_subject().map_err(|e| e.to_string())?;
                        let inner = NatsSink::connect(dest, subject, cap, cap, policy)
                            .map_err(|e| format!("open nats sink {dest} subject={subject}: {e}"))?;
                        out.nats
                            .push(SinkWorker::start(id, "nats", required, cap, policy, inner)?);
                    }
                    #[cfg(not(feature = "nats"))]
                    {
                        let _ = (sink, policy, cap);
                        return Err(
                            "sink type=nats requires marketfeed-daemon feature `nats` (TCP PUB)"
                                .into(),
                        );
                    }
                }
            }
        }
        Ok(out)
    }

    pub fn is_empty(&self) -> bool {
        self.memory.is_empty()
            && self.logging.is_empty()
            && self.file.is_empty()
            && self.protobuf_file.is_empty()
            && self.protobuf_file_bin.is_empty()
            && self.udp.is_empty()
            && self.spill_wal.is_empty()
            && {
                #[cfg(feature = "kafka")]
                {
                    self.kafka.is_empty()
                }
                #[cfg(not(feature = "kafka"))]
                {
                    true
                }
            }
            && {
                #[cfg(feature = "nats")]
                {
                    self.nats.is_empty()
                }
                #[cfg(not(feature = "nats"))]
                {
                    true
                }
            }
    }

    pub fn snapshots(&self) -> Vec<SinkSnapshot> {
        let mut snapshots = Vec::new();
        snapshots.extend(self.memory.iter().map(SinkWorker::snapshot));
        snapshots.extend(self.logging.iter().map(SinkWorker::snapshot));
        snapshots.extend(self.file.iter().map(SinkWorker::snapshot));
        snapshots.extend(self.protobuf_file.iter().map(SinkWorker::snapshot));
        snapshots.extend(self.protobuf_file_bin.iter().map(SinkWorker::snapshot));
        snapshots.extend(self.udp.iter().map(SinkWorker::snapshot));
        snapshots.extend(self.spill_wal.iter().map(SinkWorker::snapshot));
        #[cfg(feature = "kafka")]
        snapshots.extend(self.kafka.iter().map(SinkWorker::snapshot));
        #[cfg(feature = "nats")]
        snapshots.extend(self.nats.iter().map(SinkWorker::snapshot));
        snapshots
    }

    pub fn required_healthy(&self) -> bool {
        self.snapshots()
            .iter()
            .all(|sink| !sink.required || sink.healthy)
    }

    pub fn shutdown(&mut self, deadline: std::time::Instant) -> Result<(), String> {
        let mut errors = Vec::new();
        macro_rules! shutdown {
            ($workers:expr) => {
                for worker in $workers {
                    if let Err(error) = worker.shutdown(deadline) {
                        errors.push(error);
                    }
                }
            };
        }
        shutdown!(&mut self.memory);
        shutdown!(&mut self.logging);
        shutdown!(&mut self.file);
        shutdown!(&mut self.protobuf_file);
        shutdown!(&mut self.protobuf_file_bin);
        shutdown!(&mut self.udp);
        shutdown!(&mut self.spill_wal);
        #[cfg(feature = "kafka")]
        shutdown!(&mut self.kafka);
        #[cfg(feature = "nats")]
        shutdown!(&mut self.nats);
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    pub fn memory_batch_len(&self) -> usize {
        self.memory
            .iter()
            .map(|worker| worker.with_sink(MemorySink::batch_len))
            .sum()
    }

    pub fn file_lines_written(&self) -> u64 {
        self.file
            .iter()
            .map(|worker| worker.with_sink(FileSink::lines_written))
            .sum()
    }

    pub fn protobuf_records_written(&self) -> u64 {
        self.protobuf_file
            .iter()
            .map(|worker| worker.with_sink(ProtobufFileSink::records_written))
            .sum()
    }

    pub fn protobuf_bin_records_written(&self) -> u64 {
        self.protobuf_file_bin
            .iter()
            .map(|worker| worker.with_sink(ProtobufBinaryFileSink::records_written))
            .sum()
    }

    pub fn udp_datagrams_sent(&self) -> u64 {
        self.udp
            .iter()
            .map(|worker| worker.with_sink(UdpSink::datagrams_sent))
            .sum()
    }

    pub fn spill_wal_bytes(&self) -> u64 {
        self.spill_wal
            .iter()
            .map(|worker| worker.with_sink(SpillWalSink::wal_bytes))
            .sum()
    }

    #[cfg(feature = "kafka")]
    pub fn kafka_records_sent(&self) -> u64 {
        self.kafka
            .iter()
            .map(|worker| worker.with_sink(KafkaSink::records_sent))
            .sum()
    }

    #[cfg(feature = "nats")]
    pub fn nats_messages_sent(&self) -> u64 {
        self.nats
            .iter()
            .map(|worker| worker.with_sink(NatsSink::messages_sent))
            .sum()
    }
}

impl EventSink for DaemonSinks {
    fn push_batch(&mut self, batch: EventBatch) -> Result<PushOutcome, SinkError> {
        let mut outcome = PushOutcome::Accepted;
        let mut first_error = None;
        for sink in &mut self.logging {
            merge_result(
                &mut outcome,
                &mut first_error,
                sink.push_batch(batch.clone()),
            );
        }
        for sink in &mut self.file {
            merge_result(
                &mut outcome,
                &mut first_error,
                sink.push_batch(batch.clone()),
            );
        }
        for sink in &mut self.protobuf_file {
            merge_result(
                &mut outcome,
                &mut first_error,
                sink.push_batch(batch.clone()),
            );
        }
        for sink in &mut self.protobuf_file_bin {
            merge_result(
                &mut outcome,
                &mut first_error,
                sink.push_batch(batch.clone()),
            );
        }
        for sink in &mut self.udp {
            merge_result(
                &mut outcome,
                &mut first_error,
                sink.push_batch(batch.clone()),
            );
        }
        #[cfg(feature = "kafka")]
        for sink in &mut self.kafka {
            merge_result(
                &mut outcome,
                &mut first_error,
                sink.push_batch(batch.clone()),
            );
        }
        #[cfg(feature = "nats")]
        for sink in &mut self.nats {
            merge_result(
                &mut outcome,
                &mut first_error,
                sink.push_batch(batch.clone()),
            );
        }
        for sink in &mut self.spill_wal {
            merge_result(
                &mut outcome,
                &mut first_error,
                sink.push_batch(batch.clone()),
            );
        }
        for sink in &mut self.memory {
            merge_result(
                &mut outcome,
                &mut first_error,
                sink.push_batch(batch.clone()),
            );
        }
        first_error.map_or(Ok(outcome), Err)
    }

    fn push_system(&mut self, event: SystemEvent) -> Result<PushOutcome, SinkError> {
        let mut outcome = PushOutcome::Accepted;
        let mut first_error = None;
        for sink in &mut self.logging {
            merge_result(
                &mut outcome,
                &mut first_error,
                sink.push_system(event.clone()),
            );
        }
        for sink in &mut self.file {
            merge_result(
                &mut outcome,
                &mut first_error,
                sink.push_system(event.clone()),
            );
        }
        for sink in &mut self.protobuf_file {
            merge_result(
                &mut outcome,
                &mut first_error,
                sink.push_system(event.clone()),
            );
        }
        for sink in &mut self.protobuf_file_bin {
            merge_result(
                &mut outcome,
                &mut first_error,
                sink.push_system(event.clone()),
            );
        }
        for sink in &mut self.udp {
            merge_result(
                &mut outcome,
                &mut first_error,
                sink.push_system(event.clone()),
            );
        }
        #[cfg(feature = "kafka")]
        for sink in &mut self.kafka {
            merge_result(
                &mut outcome,
                &mut first_error,
                sink.push_system(event.clone()),
            );
        }
        #[cfg(feature = "nats")]
        for sink in &mut self.nats {
            merge_result(
                &mut outcome,
                &mut first_error,
                sink.push_system(event.clone()),
            );
        }
        for sink in &mut self.spill_wal {
            merge_result(
                &mut outcome,
                &mut first_error,
                sink.push_system(event.clone()),
            );
        }
        for sink in &mut self.memory {
            merge_result(
                &mut outcome,
                &mut first_error,
                sink.push_system(event.clone()),
            );
        }
        first_error.map_or(Ok(outcome), Err)
    }
}

/// `EventSink` that locks shared daemon sinks per push (venues share one set).
#[derive(Debug, Clone)]
pub struct SharedDaemonSinks {
    pub sinks: Arc<Mutex<DaemonSinks>>,
    #[cfg(feature = "ui-api")]
    view: Option<SharedViewPlane>,
}

impl SharedDaemonSinks {
    pub fn new(inner: Arc<Mutex<DaemonSinks>>) -> Self {
        Self {
            sinks: inner,
            #[cfg(feature = "ui-api")]
            view: None,
        }
    }

    #[cfg(feature = "ui-api")]
    pub fn with_view(
        inner: Arc<Mutex<DaemonSinks>>,
        view: Option<Arc<ViewPlane>>,
        venue: VenueId,
    ) -> Self {
        Self {
            sinks: inner,
            view: view.map(|view| SharedViewPlane::for_venue(view, venue)),
        }
    }
}

impl EventSink for SharedDaemonSinks {
    fn push_batch(&mut self, batch: EventBatch) -> Result<PushOutcome, SinkError> {
        #[cfg(feature = "ui-api")]
        if let Some(view) = &mut self.view {
            let _ = run_view_work(|| view.push_batch(batch.clone()))?;
        }
        self.sinks.lock().expect("sinks lock").push_batch(batch)
    }

    fn push_system(&mut self, event: SystemEvent) -> Result<PushOutcome, SinkError> {
        #[cfg(feature = "ui-api")]
        if let Some(view) = &mut self.view {
            let _ = run_view_work(|| view.push_system(event.clone()))?;
        }
        self.sinks.lock().expect("sinks lock").push_system(event)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use marketfeed_model::{OverflowPolicy, SessionId};

    use super::*;

    fn batch(seq: u64) -> EventBatch {
        EventBatch {
            session: SessionId(1),
            frame_seq: seq,
            events: Vec::new(),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn view_work_falls_back_on_current_thread_runtime() {
        assert_eq!(run_view_work(|| 42), 42);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn view_work_yields_executor_capacity_on_multi_thread_runtime() {
        assert_eq!(run_view_work(|| 42), 42);
    }

    #[test]
    fn fanout_preserves_optional_sink_drop_and_continues() {
        let first = SinkWorker::start(
            "disabled".into(),
            "memory",
            false,
            1,
            OverflowPolicy::DropNewest,
            MemorySink::new(1, 1, OverflowPolicy::DropNewest),
        )
        .unwrap();
        first.mailbox.close();
        let mut sinks = DaemonSinks {
            memory: vec![
                first,
                SinkWorker::start(
                    "healthy".into(),
                    "memory",
                    false,
                    2,
                    OverflowPolicy::DropNewest,
                    MemorySink::new(2, 2, OverflowPolicy::DropNewest),
                )
                .unwrap(),
            ],
            ..DaemonSinks::default()
        };

        assert_eq!(
            sinks.push_batch(batch(1)).unwrap(),
            PushOutcome::DroppedNewest
        );
        for _ in 0..100 {
            if sinks.memory[1].with_sink(MemorySink::batch_len) == 1 {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("healthy sink did not receive fan-out after optional sink closed");
    }

    #[test]
    fn fanout_attempts_every_sink_before_returning_required_error() {
        let failed = SinkWorker::start(
            "required".into(),
            "memory",
            true,
            1,
            OverflowPolicy::FailEngine,
            MemorySink::new(1, 1, OverflowPolicy::FailEngine),
        )
        .unwrap();
        failed.mailbox.close();
        let mut sinks = DaemonSinks {
            memory: vec![
                failed,
                SinkWorker::start(
                    "healthy".into(),
                    "memory",
                    false,
                    2,
                    OverflowPolicy::DropNewest,
                    MemorySink::new(2, 2, OverflowPolicy::DropNewest),
                )
                .unwrap(),
            ],
            ..DaemonSinks::default()
        };

        assert!(matches!(sinks.push_batch(batch(1)), Err(SinkError::Io(_))));
        for _ in 0..100 {
            if sinks.memory[1].with_sink(MemorySink::batch_len) == 1 {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("healthy sink was skipped after required sink failure");
    }

    #[test]
    fn daemon_refuses_unacknowledged_spill_recovery() {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "marketfeed-daemon-spill-recovery-{}-{suffix}.wal",
            std::process::id()
        ));
        {
            let mut spill = SpillWalSink::open(SpillWalConfig {
                path: path.clone(),
                batch_capacity: 1,
                system_capacity: 1,
                wal_limit_bytes: 64 * 1024,
            })
            .unwrap();
            spill.push_batch(batch(1)).unwrap();
            spill.push_batch(batch(2)).unwrap();
        }
        let mut config = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            "#,
        )
        .unwrap();
        config.sinks.push(crate::config::SinkConfig {
            id: Some("recovery".into()),
            required: true,
            sink_type: "spill-wal".into(),
            path: Some(path.display().to_string()),
            address: None,
            topic: None,
            subject: None,
            capacity: 1,
            overflow: "spill_to_disk".into(),
            wal_limit: Some("64KiB".into()),
        });
        let error = DaemonSinks::from_config(&config).unwrap_err();
        assert!(error.contains("unacknowledged recovery record"));
        assert!(error.contains("fail-closed"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn mailbox_preserves_batch_and_system_fifo() {
        let mailbox = SinkMailbox::new(4, OverflowPolicy::FailEngine);
        assert_eq!(
            mailbox.push(SinkItem::Batch(batch(1))).unwrap(),
            PushOutcome::Accepted
        );
        assert_eq!(
            mailbox
                .push(SinkItem::System(SystemEvent::ConnectionStateChanged {
                    state: "test".into(),
                }))
                .unwrap(),
            PushOutcome::Accepted
        );
        assert_eq!(
            mailbox.push(SinkItem::Batch(batch(3))).unwrap(),
            PushOutcome::Accepted
        );

        assert!(matches!(
            mailbox.pop(),
            Some(SinkItem::Batch(EventBatch { frame_seq: 1, .. }))
        ));
        assert!(matches!(
            mailbox.pop(),
            Some(SinkItem::System(SystemEvent::ConnectionStateChanged { .. }))
        ));
        assert!(matches!(
            mailbox.pop(),
            Some(SinkItem::Batch(EventBatch { frame_seq: 3, .. }))
        ));
    }

    #[test]
    fn mailbox_applies_drop_policies_without_growing() {
        let newest = SinkMailbox::new(1, OverflowPolicy::DropNewest);
        newest.push(SinkItem::Batch(batch(1))).unwrap();
        assert_eq!(
            newest.push(SinkItem::Batch(batch(2))).unwrap(),
            PushOutcome::DroppedNewest
        );
        assert_eq!(newest.len(), 1);
        assert!(matches!(
            newest.pop(),
            Some(SinkItem::Batch(EventBatch { frame_seq: 1, .. }))
        ));

        let oldest = SinkMailbox::new(1, OverflowPolicy::DropOldest);
        oldest.push(SinkItem::Batch(batch(1))).unwrap();
        assert_eq!(
            oldest.push(SinkItem::Batch(batch(2))).unwrap(),
            PushOutcome::DroppedOldest { dropped: 1 }
        );
        assert_eq!(oldest.len(), 1);
        assert!(matches!(
            oldest.pop(),
            Some(SinkItem::Batch(EventBatch { frame_seq: 2, .. }))
        ));
    }

    #[test]
    fn mailbox_fail_engine_is_fail_closed() {
        let mailbox = SinkMailbox::new(1, OverflowPolicy::FailEngine);
        mailbox.push(SinkItem::Batch(batch(1))).unwrap();
        assert_eq!(
            mailbox.push(SinkItem::Batch(batch(2))).unwrap_err(),
            SinkError::FailEngine
        );
        assert_eq!(mailbox.len(), 1);
    }

    #[derive(Debug)]
    struct BlockingSink {
        started: Arc<AtomicBool>,
        release: Arc<(Mutex<bool>, Condvar)>,
    }

    impl BlockingSink {
        fn block(&self) {
            self.started.store(true, Ordering::Release);
            let (released, wake) = &*self.release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
        }
    }

    impl EventSink for BlockingSink {
        fn push_batch(&mut self, _batch: EventBatch) -> Result<PushOutcome, SinkError> {
            self.block();
            Ok(PushOutcome::Accepted)
        }

        fn push_system(&mut self, _event: SystemEvent) -> Result<PushOutcome, SinkError> {
            self.block();
            Ok(PushOutcome::Accepted)
        }
    }

    #[test]
    fn slow_sink_io_does_not_block_producer_ingress() {
        let started = Arc::new(AtomicBool::new(false));
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let worker = SinkWorker::start(
            "slow".into(),
            "test",
            true,
            2,
            OverflowPolicy::FailEngine,
            BlockingSink {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
            },
        )
        .unwrap();
        worker.push_batch(batch(1)).unwrap();
        for _ in 0..100 {
            if started.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(started.load(Ordering::Acquire));

        let before = std::time::Instant::now();
        assert_eq!(worker.push_batch(batch(2)).unwrap(), PushOutcome::Accepted);
        assert!(
            before.elapsed() < std::time::Duration::from_millis(50),
            "producer waited for sink I/O"
        );
        let error = worker
            .wait_for_drain(std::time::Instant::now() + std::time::Duration::from_millis(10))
            .unwrap_err();
        assert!(error.contains("drain deadline exceeded"));

        let (released, wake) = &*release;
        *released.lock().unwrap() = true;
        wake.notify_all();
        worker
            .wait_for_drain(std::time::Instant::now() + std::time::Duration::from_secs(1))
            .unwrap();
    }

    #[derive(Debug)]
    struct FailingSink;

    impl EventSink for FailingSink {
        fn push_batch(&mut self, _batch: EventBatch) -> Result<PushOutcome, SinkError> {
            Err(SinkError::Io("simulated failure".into()))
        }

        fn push_system(&mut self, _event: SystemEvent) -> Result<PushOutcome, SinkError> {
            Err(SinkError::Io("simulated failure".into()))
        }
    }

    #[test]
    fn required_worker_failure_is_health_visible() {
        let worker = SinkWorker::start(
            "required".into(),
            "test",
            true,
            2,
            OverflowPolicy::FailEngine,
            FailingSink,
        )
        .unwrap();
        worker.push_batch(batch(1)).unwrap();
        for _ in 0..100 {
            if !worker.snapshot().healthy {
                let snapshot = worker.snapshot();
                assert!(snapshot.required);
                assert_eq!(snapshot.errors, 1);
                assert_eq!(
                    snapshot.last_error.as_deref(),
                    Some("sink I/O: simulated failure")
                );
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("sink failure was not reflected in worker health");
    }

    #[test]
    fn required_sink_failure_changes_aggregate_health() {
        let worker = SinkWorker::start(
            "required".into(),
            "memory",
            true,
            2,
            OverflowPolicy::FailEngine,
            MemorySink::new(2, 2, OverflowPolicy::FailEngine),
        )
        .unwrap();
        let sinks = DaemonSinks {
            memory: vec![worker],
            ..DaemonSinks::default()
        };
        assert!(sinks.required_healthy());
        sinks.memory[0]
            .health
            .healthy
            .store(false, Ordering::Release);
        assert!(!sinks.required_healthy());
    }

    #[test]
    fn optional_worker_failure_discards_backlog_without_poisoning_shutdown() {
        let worker = SinkWorker::start(
            "optional".into(),
            "test",
            false,
            4,
            OverflowPolicy::DropNewest,
            FailingSink,
        )
        .unwrap();
        worker.push_batch(batch(1)).unwrap();
        worker.push_batch(batch(2)).unwrap();
        for _ in 0..100 {
            if !worker.snapshot().healthy {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(!worker.snapshot().healthy);
        assert_eq!(
            worker.snapshot().dropped,
            2,
            "the failed in-flight item and discarded backlog are both dropped"
        );
        worker
            .wait_for_drain(std::time::Instant::now() + std::time::Duration::from_millis(50))
            .expect("an isolated optional failure must not exhaust global shutdown");
        assert_eq!(worker.snapshot().queue_len, 0);
    }

    #[test]
    fn drop_oldest_counts_the_replacement_as_enqueued() {
        let started = Arc::new(AtomicBool::new(false));
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let worker = SinkWorker::start(
            "drop-oldest".into(),
            "test",
            false,
            1,
            OverflowPolicy::DropOldest,
            BlockingSink {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
            },
        )
        .unwrap();
        worker.push_batch(batch(1)).unwrap();
        for _ in 0..100 {
            if started.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(started.load(Ordering::Acquire));
        worker.push_batch(batch(2)).unwrap();
        assert_eq!(
            worker.push_batch(batch(3)).unwrap(),
            PushOutcome::DroppedOldest { dropped: 1 }
        );
        let snapshot = worker.snapshot();
        assert_eq!(snapshot.enqueued, 3);
        assert_eq!(snapshot.dropped, 1);

        let (released, wake) = &*release;
        *released.lock().unwrap() = true;
        wake.notify_all();
        worker
            .wait_for_drain(std::time::Instant::now() + std::time::Duration::from_secs(1))
            .unwrap();
    }

    #[test]
    fn successful_shutdown_joins_worker_thread() {
        let mut worker = SinkWorker::start(
            "joined".into(),
            "test",
            true,
            1,
            OverflowPolicy::FailEngine,
            MemorySink::new(1, 1, OverflowPolicy::FailEngine),
        )
        .unwrap();
        worker.push_batch(batch(1)).unwrap();
        worker
            .shutdown(std::time::Instant::now() + std::time::Duration::from_secs(1))
            .unwrap();
        assert!(worker.worker.is_none());
        assert_eq!(Arc::strong_count(&worker.sink), 1);
    }
}
