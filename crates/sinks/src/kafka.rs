//! Kafka `EventSink` — optional TCP Produce client (no `rdkafka`).
//!
//! # Feature `kafka`
//!
//! When **off**, every push returns [`SinkError::Unsupported`].
//! When **on**, [`KafkaSink::connect`] opens a TCP session to a broker and
//! sends Kafka Produce API key `0` / version `0` MessageSet records
//! (`acks=0`, fire-and-forget). Payload is the complete JSON shape shared with
//! [`crate::FileSink`].
//!
//! # Integration
//!
//! Loopback unit tests use a mock TCP peer that accepts length-prefixed frames.
//! Talking to a real broker (Kafka / Redpanda) needs an external process —
//! not started by default CI. Produce v0 MessageSet is widely accepted; newer
//! RecordBatch-only deployments may reject — upgrade = ApiVersions negotiate
//! + Produce v3+ / `rdkafka` if operators need that surface.
//!
//! # ponytail
//! Sync `write_all` on the push path; ceiling = stall under a full TCP window.
//! No compression, idempotent producer, or transactional IDs.

#[cfg(not(feature = "kafka"))]
use marketfeed_adapter_api::EventBatch;
#[cfg(not(feature = "kafka"))]
use marketfeed_dispatch::PushOutcome;
#[cfg(not(feature = "kafka"))]
use marketfeed_model::SystemEvent;

#[cfg(not(feature = "kafka"))]
use crate::sink::{EventSink, SinkError};

#[cfg(feature = "kafka")]
mod real {
    use std::io::{self, Read, Write};
    use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
    use std::time::Duration;

    use marketfeed_adapter_api::EventBatch;
    use marketfeed_dispatch::PushOutcome;
    use marketfeed_model::{OverflowPolicy, SystemEvent};

    use crate::memory::MemorySink;
    use crate::sink::{EventSink, SinkError};
    use crate::wire_json::{batch_json, system_json};

    /// Bounded Kafka Produce sink (TCP, Produce v0 MessageSet, `acks=0`).
    #[derive(Debug)]
    pub struct KafkaSink {
        inner: MemorySink,
        stream: TcpStream,
        topic: String,
        correlation: i32,
        records_sent: u64,
    }

    impl KafkaSink {
        /// TCP connect to `broker` (`host:port` or [`SocketAddr`]), topic required.
        pub fn connect(
            broker: impl ToSocketAddrs,
            topic: impl Into<String>,
            batch_capacity: usize,
            system_capacity: usize,
            policy: OverflowPolicy,
        ) -> io::Result<Self> {
            let topic = topic.into();
            if topic.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "kafka topic must be non-empty",
                ));
            }
            let stream = TcpStream::connect(broker)?;
            stream.set_nodelay(true)?;
            stream.set_write_timeout(Some(Duration::from_secs(5)))?;
            stream.set_read_timeout(Some(Duration::from_millis(50)))?;
            Ok(Self {
                inner: MemorySink::new(batch_capacity, system_capacity, policy),
                stream,
                topic,
                correlation: 1,
                records_sent: 0,
            })
        }

        /// Convenience: parse `host:port` then [`Self::connect`].
        pub fn connect_addr(
            addr: SocketAddr,
            topic: impl Into<String>,
            batch_capacity: usize,
            system_capacity: usize,
            policy: OverflowPolicy,
        ) -> io::Result<Self> {
            Self::connect(addr, topic, batch_capacity, system_capacity, policy)
        }

        pub fn topic(&self) -> &str {
            &self.topic
        }

        pub fn records_sent(&self) -> u64 {
            self.records_sent
        }

        pub fn dropped_batches(&self) -> u64 {
            self.inner.dropped_batches()
        }

        pub fn dropped_systems(&self) -> u64 {
            self.inner.dropped_systems()
        }

        fn next_correlation(&mut self) -> i32 {
            let id = self.correlation;
            self.correlation = self.correlation.wrapping_add(1);
            id
        }

        fn produce(&mut self, payload: &[u8]) -> Result<(), SinkError> {
            let corr = self.next_correlation();
            let frame = encode_produce_v0(&self.topic, corr, payload);
            self.stream
                .write_all(&frame)
                .map_err(|e| SinkError::Io(format!("kafka produce write: {e}")))?;
            // Drain any unexpected response bytes (acks=0 normally sends none).
            let mut scratch = [0u8; 64];
            let _ = self.stream.read(&mut scratch);
            self.records_sent += 1;
            Ok(())
        }

        fn flush_accepted_batches(&mut self) -> Result<(), SinkError> {
            while let Some(b) = self.inner.pop_batch() {
                let payload = batch_json(&b)?;
                self.produce(&payload)?;
            }
            Ok(())
        }

        fn flush_accepted_systems(&mut self) -> Result<(), SinkError> {
            while let Some(ev) = self.inner.pop_system() {
                let payload = system_json(&ev)?;
                self.produce(&payload)?;
            }
            Ok(())
        }
    }

    impl EventSink for KafkaSink {
        fn push_batch(&mut self, batch: EventBatch) -> Result<PushOutcome, SinkError> {
            let outcome = self.inner.push_batch(batch)?;
            match outcome {
                PushOutcome::Accepted | PushOutcome::DroppedOldest { .. } => {
                    self.flush_accepted_batches()?;
                }
                PushOutcome::DroppedNewest => {}
            }
            Ok(outcome)
        }

        fn push_system(&mut self, event: SystemEvent) -> Result<PushOutcome, SinkError> {
            let outcome = self.inner.push_system(event)?;
            match outcome {
                PushOutcome::Accepted | PushOutcome::DroppedOldest { .. } => {
                    self.flush_accepted_systems()?;
                }
                PushOutcome::DroppedNewest => {}
            }
            Ok(outcome)
        }
    }

    /// IEEE CRC-32 (Kafka Message v0).
    fn crc32_ieee(data: &[u8]) -> u32 {
        let mut crc: u32 = 0xffff_ffff;
        for &b in data {
            crc ^= u32::from(b);
            for _ in 0..8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ 0xedb8_8320;
                } else {
                    crc >>= 1;
                }
            }
        }
        !crc
    }

    fn encode_kafka_string(out: &mut Vec<u8>, s: &str) {
        let bytes = s.as_bytes();
        out.extend_from_slice(&(bytes.len() as i16).to_be_bytes());
        out.extend_from_slice(bytes);
    }

    fn encode_kafka_bytes(out: &mut Vec<u8>, data: Option<&[u8]>) {
        match data {
            None => out.extend_from_slice(&(-1_i32).to_be_bytes()),
            Some(b) => {
                out.extend_from_slice(&(b.len() as i32).to_be_bytes());
                out.extend_from_slice(b);
            }
        }
    }

    /// Kafka ProduceRequest API key 0 / version 0, `acks=0`, single partition 0.
    pub(super) fn encode_produce_v0(topic: &str, correlation_id: i32, value: &[u8]) -> Vec<u8> {
        let mut msg_body = Vec::with_capacity(2 + 4 + 4 + value.len());
        msg_body.push(0); // magic v0
        msg_body.push(0); // attributes
        encode_kafka_bytes(&mut msg_body, None); // key
        encode_kafka_bytes(&mut msg_body, Some(value));
        let crc = crc32_ieee(&msg_body);

        let mut message = Vec::with_capacity(4 + msg_body.len());
        message.extend_from_slice(&crc.to_be_bytes());
        message.extend_from_slice(&msg_body);

        let mut message_set = Vec::with_capacity(8 + 4 + message.len());
        message_set.extend_from_slice(&0_i64.to_be_bytes()); // offset
        message_set.extend_from_slice(&(message.len() as i32).to_be_bytes());
        message_set.extend_from_slice(&message);

        let mut request = Vec::with_capacity(64 + topic.len() + message_set.len());
        request.extend_from_slice(&0_i16.to_be_bytes()); // api_key = Produce
        request.extend_from_slice(&0_i16.to_be_bytes()); // api_version = 0
        request.extend_from_slice(&correlation_id.to_be_bytes());
        encode_kafka_string(&mut request, "marketfeed");
        request.extend_from_slice(&0_i16.to_be_bytes()); // required_acks = 0
        request.extend_from_slice(&1_000_i32.to_be_bytes()); // timeout ms
        request.extend_from_slice(&1_i32.to_be_bytes()); // #topics
        encode_kafka_string(&mut request, topic);
        request.extend_from_slice(&1_i32.to_be_bytes()); // #partitions
        request.extend_from_slice(&0_i32.to_be_bytes()); // partition 0
        request.extend_from_slice(&(message_set.len() as i32).to_be_bytes());
        request.extend_from_slice(&message_set);

        let mut frame = Vec::with_capacity(4 + request.len());
        frame.extend_from_slice(&(request.len() as i32).to_be_bytes());
        frame.extend_from_slice(&request);
        frame
    }

    #[cfg(test)]
    mod encode_tests {
        use super::encode_produce_v0;

        #[test]
        fn produce_frame_has_length_prefix_and_topic() {
            let frame = encode_produce_v0("mf-events", 7, b"hello");
            assert!(frame.len() > 8);
            let size = i32::from_be_bytes(frame[0..4].try_into().unwrap()) as usize;
            assert_eq!(size, frame.len() - 4);
            assert_eq!(i16::from_be_bytes(frame[4..6].try_into().unwrap()), 0);
            assert_eq!(i16::from_be_bytes(frame[6..8].try_into().unwrap()), 0);
            assert_eq!(i32::from_be_bytes(frame[8..12].try_into().unwrap()), 7);
            let topic = b"mf-events";
            assert!(
                frame.windows(topic.len()).any(|w| w == topic),
                "topic missing in frame"
            );
        }
    }
}

#[cfg(feature = "kafka")]
pub use real::KafkaSink;

/// Compile-only / feature-off stub.
#[cfg(not(feature = "kafka"))]
#[derive(Debug, Default, Clone, Copy)]
pub struct KafkaSink;

#[cfg(not(feature = "kafka"))]
impl KafkaSink {
    pub fn new() -> Self {
        Self
    }
}

#[cfg(not(feature = "kafka"))]
impl EventSink for KafkaSink {
    fn push_batch(&mut self, _batch: EventBatch) -> Result<PushOutcome, SinkError> {
        Err(SinkError::Unsupported(
            "kafka: enable feature `kafka` (TCP Produce v0 client)",
        ))
    }

    fn push_system(&mut self, _event: SystemEvent) -> Result<PushOutcome, SinkError> {
        Err(SinkError::Unsupported(
            "kafka: enable feature `kafka` (TCP Produce v0 client)",
        ))
    }
}
