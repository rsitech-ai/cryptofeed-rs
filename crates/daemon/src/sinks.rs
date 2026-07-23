//! Build and fan-out configured `[[sinks]]` (memory / logging / file /
//! protobuf-file / protobuf-file-bin / udp / spill-wal / kafka / nats).

use std::sync::{Arc, Mutex};

use marketfeed_adapter_api::EventBatch;
use marketfeed_dispatch::PushOutcome;
use marketfeed_model::SystemEvent;
use marketfeed_sinks::{
    EventSink, FileSink, LoggingSink, MemorySink, ProtobufBinaryFileSink, ProtobufFileSink,
    SinkError, SpillWalConfig, SpillWalSink, UdpSink,
};

#[cfg(feature = "kafka")]
use marketfeed_sinks::KafkaSink;
#[cfg(feature = "nats")]
use marketfeed_sinks::NatsSink;

use crate::config::{DaemonConfig, SinkKind};

fn merge_outcome(aggregate: &mut PushOutcome, next: PushOutcome) {
    match (&mut *aggregate, next) {
        (PushOutcome::Accepted, non_accepted) => *aggregate = non_accepted,
        (PushOutcome::DroppedOldest { dropped: total }, PushOutcome::DroppedOldest { dropped }) => {
            *total = total.saturating_add(dropped)
        }
        _ => {}
    }
}

/// Process-wide sink set built from config. Empty ⇒ live loop null-drains.
#[derive(Debug, Default)]
pub struct DaemonSinks {
    pub memory: Vec<MemorySink>,
    pub logging: Vec<LoggingSink>,
    pub file: Vec<FileSink>,
    pub protobuf_file: Vec<ProtobufFileSink>,
    pub protobuf_file_bin: Vec<ProtobufBinaryFileSink>,
    pub udp: Vec<UdpSink>,
    pub spill_wal: Vec<SpillWalSink>,
    #[cfg(feature = "kafka")]
    pub kafka: Vec<KafkaSink>,
    #[cfg(feature = "nats")]
    pub nats: Vec<NatsSink>,
}

impl DaemonSinks {
    pub fn from_config(config: &DaemonConfig) -> Result<Self, String> {
        let mut out = Self::default();
        for sink in &config.sinks {
            let policy = sink.overflow_policy().map_err(|e| e.to_string())?;
            let cap = sink.capacity;
            match sink.kind().map_err(|e| e.to_string())? {
                SinkKind::Memory => out.memory.push(MemorySink::new(cap, cap, policy)),
                SinkKind::Logging => out.logging.push(LoggingSink::new(cap, cap, policy)),
                SinkKind::File => {
                    let path = sink.file_path().map_err(|e| e.to_string())?;
                    out.file.push(
                        FileSink::open(path, cap, cap, policy)
                            .map_err(|e| format!("open file sink {path}: {e}"))?,
                    );
                }
                SinkKind::ProtobufFile => {
                    let path = sink.file_path().map_err(|e| e.to_string())?;
                    out.protobuf_file.push(
                        ProtobufFileSink::open(path, cap, cap, policy)
                            .map_err(|e| format!("open protobuf-file sink {path}: {e}"))?,
                    );
                }
                SinkKind::ProtobufFileBin => {
                    let path = sink.file_path().map_err(|e| e.to_string())?;
                    out.protobuf_file_bin.push(
                        ProtobufBinaryFileSink::open(path, cap, cap, policy)
                            .map_err(|e| format!("open protobuf-file-bin sink {path}: {e}"))?,
                    );
                }
                SinkKind::Udp => {
                    let dest = sink.udp_address().map_err(|e| e.to_string())?;
                    out.udp.push(
                        UdpSink::connect(dest, cap, cap, policy)
                            .map_err(|e| format!("open udp sink {dest}: {e}"))?,
                    );
                }
                SinkKind::SpillWal => {
                    let path = sink.file_path().map_err(|e| e.to_string())?;
                    let wal_limit_bytes = sink.wal_limit_bytes().map_err(|e| e.to_string())?;
                    out.spill_wal.push(
                        SpillWalSink::open(SpillWalConfig {
                            path: path.into(),
                            batch_capacity: cap,
                            system_capacity: cap,
                            wal_limit_bytes,
                        })
                        .map_err(|e| format!("open spill-wal sink {path}: {e}"))?,
                    );
                }
                SinkKind::Kafka => {
                    #[cfg(feature = "kafka")]
                    {
                        let dest = sink.socket_address().map_err(|e| e.to_string())?;
                        let topic = sink.kafka_topic().map_err(|e| e.to_string())?;
                        out.kafka.push(
                            KafkaSink::connect(dest, topic, cap, cap, policy).map_err(|e| {
                                format!("open kafka sink {dest} topic={topic}: {e}")
                            })?,
                        );
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
                        out.nats
                            .push(NatsSink::connect(dest, subject, cap, cap, policy).map_err(
                                |e| format!("open nats sink {dest} subject={subject}: {e}"),
                            )?);
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
        if let Some(sink) = out.spill_wal.iter().find(|sink| sink.recovered_len() != 0) {
            return Err(format!(
                "spill-wal sink {} contains {} unacknowledged recovery record(s); daemon startup is fail-closed until an explicit recovery consumer processes SpillWalSink::pop_recovered and checkpoints the prefix",
                sink.path().display(),
                sink.recovered_len()
            ));
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

    pub fn memory_batch_len(&self) -> usize {
        self.memory.iter().map(MemorySink::batch_len).sum()
    }

    pub fn file_lines_written(&self) -> u64 {
        self.file.iter().map(FileSink::lines_written).sum()
    }

    pub fn protobuf_records_written(&self) -> u64 {
        self.protobuf_file
            .iter()
            .map(ProtobufFileSink::records_written)
            .sum()
    }

    pub fn protobuf_bin_records_written(&self) -> u64 {
        self.protobuf_file_bin
            .iter()
            .map(ProtobufBinaryFileSink::records_written)
            .sum()
    }

    pub fn udp_datagrams_sent(&self) -> u64 {
        self.udp.iter().map(UdpSink::datagrams_sent).sum()
    }

    pub fn spill_wal_bytes(&self) -> u64 {
        self.spill_wal.iter().map(SpillWalSink::wal_bytes).sum()
    }

    #[cfg(feature = "kafka")]
    pub fn kafka_records_sent(&self) -> u64 {
        self.kafka.iter().map(KafkaSink::records_sent).sum()
    }

    #[cfg(feature = "nats")]
    pub fn nats_messages_sent(&self) -> u64 {
        self.nats.iter().map(NatsSink::messages_sent).sum()
    }
}

impl EventSink for DaemonSinks {
    fn push_batch(&mut self, batch: EventBatch) -> Result<PushOutcome, SinkError> {
        let mut outcome = PushOutcome::Accepted;
        for sink in &mut self.logging {
            merge_outcome(&mut outcome, sink.push_batch(batch.clone())?);
        }
        for sink in &mut self.file {
            merge_outcome(&mut outcome, sink.push_batch(batch.clone())?);
        }
        for sink in &mut self.protobuf_file {
            merge_outcome(&mut outcome, sink.push_batch(batch.clone())?);
        }
        for sink in &mut self.protobuf_file_bin {
            merge_outcome(&mut outcome, sink.push_batch(batch.clone())?);
        }
        for sink in &mut self.udp {
            merge_outcome(&mut outcome, sink.push_batch(batch.clone())?);
        }
        #[cfg(feature = "kafka")]
        for sink in &mut self.kafka {
            merge_outcome(&mut outcome, sink.push_batch(batch.clone())?);
        }
        #[cfg(feature = "nats")]
        for sink in &mut self.nats {
            merge_outcome(&mut outcome, sink.push_batch(batch.clone())?);
        }
        for sink in &mut self.spill_wal {
            merge_outcome(&mut outcome, sink.push_batch(batch.clone())?);
        }
        for sink in &mut self.memory {
            merge_outcome(&mut outcome, sink.push_batch(batch.clone())?);
        }
        Ok(outcome)
    }

    fn push_system(&mut self, event: SystemEvent) -> Result<PushOutcome, SinkError> {
        let mut outcome = PushOutcome::Accepted;
        for sink in &mut self.logging {
            merge_outcome(&mut outcome, sink.push_system(event.clone())?);
        }
        for sink in &mut self.file {
            merge_outcome(&mut outcome, sink.push_system(event.clone())?);
        }
        for sink in &mut self.protobuf_file {
            merge_outcome(&mut outcome, sink.push_system(event.clone())?);
        }
        for sink in &mut self.protobuf_file_bin {
            merge_outcome(&mut outcome, sink.push_system(event.clone())?);
        }
        for sink in &mut self.udp {
            merge_outcome(&mut outcome, sink.push_system(event.clone())?);
        }
        #[cfg(feature = "kafka")]
        for sink in &mut self.kafka {
            merge_outcome(&mut outcome, sink.push_system(event.clone())?);
        }
        #[cfg(feature = "nats")]
        for sink in &mut self.nats {
            merge_outcome(&mut outcome, sink.push_system(event.clone())?);
        }
        for sink in &mut self.spill_wal {
            merge_outcome(&mut outcome, sink.push_system(event.clone())?);
        }
        for sink in &mut self.memory {
            merge_outcome(&mut outcome, sink.push_system(event.clone())?);
        }
        Ok(outcome)
    }
}

/// `EventSink` that locks shared daemon sinks per push (venues share one set).
#[derive(Debug, Clone)]
pub struct SharedDaemonSinks(pub Arc<Mutex<DaemonSinks>>);

impl SharedDaemonSinks {
    pub fn new(inner: Arc<Mutex<DaemonSinks>>) -> Self {
        Self(inner)
    }
}

impl EventSink for SharedDaemonSinks {
    fn push_batch(&mut self, batch: EventBatch) -> Result<PushOutcome, SinkError> {
        self.0.lock().expect("sinks lock").push_batch(batch)
    }

    fn push_system(&mut self, event: SystemEvent) -> Result<PushOutcome, SinkError> {
        self.0.lock().expect("sinks lock").push_system(event)
    }
}

#[cfg(test)]
mod tests {
    use marketfeed_model::{OverflowPolicy, SessionId};

    use super::*;

    fn batch(seq: u64) -> EventBatch {
        EventBatch {
            session: SessionId(1),
            frame_seq: seq,
            events: Vec::new(),
        }
    }

    #[test]
    fn fanout_preserves_any_sink_drop_outcome() {
        let mut sinks = DaemonSinks {
            memory: vec![
                MemorySink::new(1, 1, OverflowPolicy::DropNewest),
                MemorySink::new(2, 2, OverflowPolicy::DropNewest),
            ],
            ..DaemonSinks::default()
        };
        sinks.push_batch(batch(1)).unwrap();

        assert_eq!(
            sinks.push_batch(batch(2)).unwrap(),
            PushOutcome::DroppedNewest
        );
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
        let config = DaemonConfig::from_toml_str(&format!(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[sinks]]
            type = "spill-wal"
            path = "{}"
            capacity = 1
            overflow = "spill_to_disk"
            wal_limit = "64KiB"
            "#,
            path.display()
        ))
        .unwrap();
        let error = DaemonSinks::from_config(&config).unwrap_err();
        assert!(error.contains("unacknowledged recovery record"));
        assert!(error.contains("fail-closed"));
        std::fs::remove_file(path).unwrap();
    }
}
