//! External sinks for normalized market / system events (spec §17.4–17.5).
//!
//! Every sink is **bounded** and applies an explicit [`OverflowPolicy`](marketfeed_model::OverflowPolicy).
//!
//! Daemon wires optional `[[sinks]]` (`memory` / `logging` / `file` /
//! `protobuf-file` / `protobuf-file-bin` / `udp` / `spill-wal` / feature-gated
//! `kafka` / `nats`) via [`forward_dispatcher`]: when configured, the live loop
//! forwards instead of null-draining; dispatch is always drained so `FailEngine`
//! queues cannot fill without a consumer.
//!
//! Built-in: [`MemorySink`], [`LoggingSink`], [`FileSink`], [`ProtobufFileSink`],
//! [`ProtobufBinaryFileSink`], [`UdpSink`], [`SpillWalSink`].
//! Broker TCP clients (optional features): [`KafkaSink`], [`NatsSink`].
//! Without `kafka` / `nats`, those types return [`SinkError::Unsupported`].
//!
//! # SpillToDisk / WAL
//!
//! [`marketfeed_model::OverflowPolicy::SpillToDisk`] is implemented by
//! [`SpillWalSink`]: memory
//! queues first, then a bounded append-only WAL (`wal_limit_bytes`). Exhaustion
//! fails closed (`FailEngine`) and surfaces `EventsDropped` / `DiskPressure`
//! via [`SpillWalSink::take_system_events`]. Plain [`MemorySink`] still rejects
//! `SpillToDisk` (no path / limit).
//!
//! # Broker features
//!
//! - `kafka` — sync TCP Produce API v0 MessageSet (`acks=0`); no `rdkafka`.
//! - `nats` — sync TCP `INFO`/`CONNECT`/`PUB`; no `async-nats`.
//!
//! Loopback mocks cover wire framing. Real broker integration needs an external
//! process (documented; not default CI).

#![forbid(unsafe_code)]

mod file;
mod kafka;
mod logging;
mod memory;
mod nats;
mod protobuf_file;
mod protobuf_file_bin;
mod protobuf_wire;
mod sink;
mod spill;
mod udp;
mod wire_json;

pub use file::FileSink;
pub use kafka::KafkaSink;
pub use logging::LoggingSink;
pub use memory::MemorySink;
pub use nats::NatsSink;
pub use protobuf_file::{ProtobufFileSink, event_envelope_json, read_length_prefixed_json};
pub use protobuf_file_bin::ProtobufBinaryFileSink;
pub use protobuf_wire::{encode_event_envelope, read_length_prefixed_records};
pub use sink::{EventSink, ForwardReport, SinkError, forward_dispatcher};
pub use spill::{SpillItem, SpillWalConfig, SpillWalSink, read_spill_items, read_spill_records};
pub use udp::UdpSink;

#[cfg(test)]
mod tests {
    use std::net::UdpSocket;
    use std::time::Duration;

    use marketfeed_adapter_api::EventBatch;
    use marketfeed_dispatch::{EventDispatcher, PushOutcome};
    use marketfeed_model::{OverflowPolicy, SessionId, SystemEvent};

    use super::{
        EventSink, FileSink, KafkaSink, LoggingSink, MemorySink, NatsSink, SinkError, UdpSink,
        forward_dispatcher,
    };

    fn batch(seq: u64) -> EventBatch {
        EventBatch {
            session: SessionId(1),
            frame_seq: seq,
            events: Vec::new(),
        }
    }

    #[test]
    fn memory_drop_newest_overflow() {
        let mut sink = MemorySink::new(1, 1, OverflowPolicy::DropNewest);
        assert_eq!(sink.push_batch(batch(1)).unwrap(), PushOutcome::Accepted);
        assert_eq!(
            sink.push_batch(batch(2)).unwrap(),
            PushOutcome::DroppedNewest
        );
        assert_eq!(sink.batch_len(), 1);
        assert_eq!(sink.pop_batch().unwrap().frame_seq, 1);
        assert_eq!(sink.dropped_batches(), 1);
    }

    #[test]
    fn memory_drop_oldest_overflow() {
        let mut sink = MemorySink::new(1, 1, OverflowPolicy::DropOldest);
        assert_eq!(sink.push_batch(batch(1)).unwrap(), PushOutcome::Accepted);
        assert_eq!(
            sink.push_batch(batch(2)).unwrap(),
            PushOutcome::DroppedOldest { dropped: 1 }
        );
        assert_eq!(sink.pop_batch().unwrap().frame_seq, 2);
        assert_eq!(sink.dropped_batches(), 1);
    }

    #[test]
    fn memory_fail_engine_overflow() {
        let mut sink = MemorySink::new(1, 1, OverflowPolicy::FailEngine);
        assert_eq!(sink.push_batch(batch(1)).unwrap(), PushOutcome::Accepted);
        assert_eq!(
            sink.push_batch(batch(2)).unwrap_err(),
            SinkError::FailEngine
        );
        assert_eq!(sink.batch_len(), 1);
    }

    #[test]
    fn memory_system_drop_oldest() {
        let mut sink = MemorySink::new(1, 1, OverflowPolicy::DropOldest);
        assert_eq!(
            sink.push_system(SystemEvent::HeartbeatMissed).unwrap(),
            PushOutcome::Accepted
        );
        assert_eq!(
            sink.push_system(SystemEvent::RateLimited).unwrap(),
            PushOutcome::DroppedOldest { dropped: 1 }
        );
        assert_eq!(sink.pop_system(), Some(SystemEvent::RateLimited));
        assert_eq!(sink.dropped_systems(), 1);
    }

    #[test]
    fn forward_dispatcher_fills_memory_sink() {
        let mut src = EventDispatcher::new(4, 4, OverflowPolicy::FailEngine);
        src.push_batch(batch(7)).unwrap();
        src.push_system(SystemEvent::ShutdownStarted).unwrap();

        let mut sink = MemorySink::new(4, 4, OverflowPolicy::FailEngine);
        forward_dispatcher(&mut src, &mut sink).unwrap();
        assert!(src.batches().is_empty());
        assert!(src.systems().is_empty());
        assert_eq!(sink.pop_batch().unwrap().frame_seq, 7);
        assert_eq!(sink.pop_system(), Some(SystemEvent::ShutdownStarted));
    }

    #[test]
    fn forward_dispatcher_keeps_source_item_when_fail_engine_sink_rejects_it() {
        let mut src = EventDispatcher::new(2, 2, OverflowPolicy::FailEngine);
        src.push_batch(batch(2)).unwrap();

        let mut sink = MemorySink::new(1, 1, OverflowPolicy::FailEngine);
        sink.push_batch(batch(1)).unwrap();

        assert_eq!(
            forward_dispatcher(&mut src, &mut sink).unwrap_err(),
            SinkError::FailEngine
        );
        assert_eq!(src.batches().len(), 1);
        assert_eq!(src.batches().front().unwrap().frame_seq, 2);
    }

    #[test]
    fn forward_dispatcher_reports_sink_drop_without_failing_the_engine() {
        let mut src = EventDispatcher::new(2, 2, OverflowPolicy::FailEngine);
        src.push_batch(batch(2)).unwrap();

        let mut sink = MemorySink::new(1, 1, OverflowPolicy::DropNewest);
        sink.push_batch(batch(1)).unwrap();

        let report = forward_dispatcher(&mut src, &mut sink).unwrap();
        assert_eq!(report.dropped_batches, 1);
        assert_eq!(report.dropped_systems, 0);
        assert!(src.batches().is_empty());
        assert_eq!(sink.pop_batch().unwrap().frame_seq, 1);
    }

    #[test]
    fn logging_sink_accepts_and_drains() {
        let mut sink = LoggingSink::new(2, 2, OverflowPolicy::FailEngine);
        assert_eq!(sink.push_batch(batch(1)).unwrap(), PushOutcome::Accepted);
        assert_eq!(
            sink.push_system(SystemEvent::ShutdownCompleted).unwrap(),
            PushOutcome::Accepted
        );
        assert_eq!(sink.dropped_batches(), 0);
        assert_eq!(sink.dropped_systems(), 0);
    }

    #[test]
    fn memory_sink_rejects_spill_policy_without_wal() {
        let mut sink = MemorySink::new(1, 1, OverflowPolicy::SpillToDisk);
        assert_eq!(sink.push_batch(batch(1)).unwrap(), PushOutcome::Accepted);
        let err = sink.push_batch(batch(2)).unwrap_err();
        assert!(matches!(err, SinkError::UnsupportedPolicy(_)));
    }

    #[test]
    fn spill_wal_sink_overflow_to_tempdir() {
        use super::{SpillWalConfig, SpillWalSink, read_spill_records};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("overflow.wal");
        let mut sink = SpillWalSink::open(SpillWalConfig {
            path: path.clone(),
            batch_capacity: 1,
            system_capacity: 1,
            wal_limit_bytes: 8 * 1024,
        })
        .unwrap();
        assert_eq!(sink.policy(), OverflowPolicy::SpillToDisk);
        assert_eq!(sink.push_batch(batch(1)).unwrap(), PushOutcome::Accepted);
        assert_eq!(sink.push_batch(batch(2)).unwrap(), PushOutcome::Accepted);
        assert_eq!(sink.spilled_batches(), 1);
        let recs = read_spill_records(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].1["value"]["frame_seq"], 2);
    }

    #[test]
    fn file_sink_appends_batch_and_system_lines() {
        let dir = std::env::temp_dir().join(format!("marketfeed-filesink-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("events.log");

        let mut sink = FileSink::open(&path, 4, 4, OverflowPolicy::FailEngine).unwrap();
        assert_eq!(sink.push_batch(batch(9)).unwrap(), PushOutcome::Accepted);
        assert_eq!(
            sink.push_system(SystemEvent::ShutdownStarted).unwrap(),
            PushOutcome::Accepted
        );
        assert_eq!(sink.lines_written(), 2);

        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains(r#""kind":"batch""#));
        assert!(text.contains(r#""frame_seq":9"#));
        assert!(text.contains(r#""event":"ShutdownStarted""#));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(not(feature = "kafka"))]
    #[test]
    fn kafka_stub_returns_unsupported() {
        let mut kafka = KafkaSink::new();
        assert!(matches!(
            kafka.push_batch(batch(1)),
            Err(SinkError::Unsupported(_))
        ));
        assert!(matches!(
            kafka.push_system(SystemEvent::HeartbeatMissed),
            Err(SinkError::Unsupported(_))
        ));
    }

    #[cfg(not(feature = "nats"))]
    #[test]
    fn nats_stub_returns_unsupported() {
        let mut nats = NatsSink::new();
        assert!(matches!(
            nats.push_batch(batch(1)),
            Err(SinkError::Unsupported(_))
        ));
        assert!(matches!(
            nats.push_system(SystemEvent::HeartbeatMissed),
            Err(SinkError::Unsupported(_))
        ));
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn kafka_loopback_produce_frame() {
        use std::io::Read;
        use std::net::TcpListener;
        use std::thread;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut sock, _) = listener.accept().unwrap();
            let mut len_buf = [0u8; 4];
            sock.read_exact(&mut len_buf).unwrap();
            let n = i32::from_be_bytes(len_buf) as usize;
            let mut body = vec![0u8; n];
            sock.read_exact(&mut body).unwrap();
            body
        });

        let mut sink =
            KafkaSink::connect(addr, "mf-events", 4, 4, OverflowPolicy::FailEngine).unwrap();
        assert_eq!(sink.push_batch(batch(9)).unwrap(), PushOutcome::Accepted);
        assert_eq!(sink.records_sent(), 1);

        let body = handle.join().unwrap();
        assert_eq!(i16::from_be_bytes(body[0..2].try_into().unwrap()), 0);
        assert!(
            body.windows(b"mf-events".len()).any(|w| w == b"mf-events"),
            "topic missing"
        );
        assert!(
            body.windows(br#""frame_seq":9"#.len())
                .any(|w| w == br#""frame_seq":9"#),
            "payload missing"
        );
    }

    #[cfg(feature = "nats")]
    #[test]
    fn nats_loopback_pub() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::thread;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut sock, _) = listener.accept().unwrap();
            sock.write_all(b"INFO {\"server_id\":\"mock\"}\r\n")
                .unwrap();
            let mut buf = [0u8; 1024];
            let mut filled = 0usize;
            // Read CONNECT then PUB
            while filled < buf.len() {
                let n = sock.read(&mut buf[filled..]).unwrap();
                if n == 0 {
                    break;
                }
                filled += n;
                let text = std::str::from_utf8(&buf[..filled]).unwrap_or("");
                if text.contains("CONNECT")
                    && text.contains("PUB mf.events")
                    && text.contains(r#""frame_seq":9"#)
                {
                    break;
                }
            }
            buf[..filled].to_vec()
        });

        let mut sink =
            NatsSink::connect(addr, "mf.events", 4, 4, OverflowPolicy::FailEngine).unwrap();
        assert_eq!(sink.push_batch(batch(9)).unwrap(), PushOutcome::Accepted);
        assert_eq!(sink.messages_sent(), 1);

        let bytes = handle.join().unwrap();
        let text = String::from_utf8_lossy(&bytes);
        assert!(text.contains("CONNECT"), "{text}");
        assert!(text.contains("PUB mf.events"), "{text}");
        assert!(text.contains(r#""frame_seq":9"#), "{text}");
    }

    #[test]
    fn udp_sink_sends_batch_and_system_datagrams() {
        let recv = UdpSocket::bind("127.0.0.1:0").unwrap();
        recv.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let dest = recv.local_addr().unwrap();

        let mut sink = UdpSink::connect(dest, 4, 4, OverflowPolicy::FailEngine).unwrap();
        assert_eq!(sink.push_batch(batch(9)).unwrap(), PushOutcome::Accepted);
        assert_eq!(
            sink.push_system(SystemEvent::ShutdownStarted).unwrap(),
            PushOutcome::Accepted
        );
        assert_eq!(sink.datagrams_sent(), 2);
        assert_eq!(sink.send_failures(), 0);

        let mut buf = [0u8; 256];
        let (n, _) = recv.recv_from(&mut buf).unwrap();
        let first = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(first.contains(r#""kind":"batch""#), "{first}");
        assert!(first.contains(r#""frame_seq":9"#), "{first}");

        let (n, _) = recv.recv_from(&mut buf).unwrap();
        let second = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(second.contains(r#""event":"ShutdownStarted""#), "{second}");
    }

    #[test]
    fn udp_sink_drains_so_capacity_one_accepts_again() {
        // Sync drain-after-accept (same as FileSink/LoggingSink): a capacity-1
        // queue does not overflow under consecutive push when policy is DropNewest.
        let recv = UdpSocket::bind("127.0.0.1:0").unwrap();
        recv.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let dest = recv.local_addr().unwrap();

        let mut sink = UdpSink::connect(dest, 1, 1, OverflowPolicy::DropNewest).unwrap();
        assert_eq!(sink.push_batch(batch(1)).unwrap(), PushOutcome::Accepted);
        assert_eq!(sink.push_batch(batch(2)).unwrap(), PushOutcome::Accepted);
        assert_eq!(sink.datagrams_sent(), 2);
        assert_eq!(sink.dropped_batches(), 0);

        let mut buf = [0u8; 256];
        let (n, _) = recv.recv_from(&mut buf).unwrap();
        assert!(
            std::str::from_utf8(&buf[..n])
                .unwrap()
                .contains(r#""frame_seq":1"#)
        );
        let (n, _) = recv.recv_from(&mut buf).unwrap();
        assert!(
            std::str::from_utf8(&buf[..n])
                .unwrap()
                .contains(r#""frame_seq":2"#)
        );
    }
}
