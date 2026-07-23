//! NATS `EventSink` — optional TCP PUB client (no `async-nats`).
//!
//! # Feature `nats`
//!
//! When **off**, every push returns [`SinkError::Unsupported`].
//! When **on**, [`NatsSink::connect`] dials a NATS server, completes the
//! `INFO` / `CONNECT` handshake, and publishes with `PUB <subject> <n>\r\n`.
//! Payload is the complete JSON shape shared with [`crate::FileSink`].
//!
//! # Integration
//!
//! Loopback unit tests use a mock that speaks `INFO` and accepts `PUB`.
//! A full NATS server (`nats-server`) is **not** required for default tests;
//! enable an external broker only for operator integration checks.
//!
//! # ponytail
//! Sync read/write on the push path; ceiling = stall under a full TCP window.
//! No JetStream, TLS, or credentials — upgrade = `async-nats` + auth when needed.

#[cfg(not(feature = "nats"))]
use marketfeed_adapter_api::EventBatch;
#[cfg(not(feature = "nats"))]
use marketfeed_dispatch::PushOutcome;
#[cfg(not(feature = "nats"))]
use marketfeed_model::SystemEvent;

#[cfg(not(feature = "nats"))]
use crate::sink::{EventSink, SinkError};

#[cfg(feature = "nats")]
mod real {
    use std::io::{self, BufRead, BufReader, Write};
    use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
    use std::time::Duration;

    use marketfeed_adapter_api::EventBatch;
    use marketfeed_dispatch::PushOutcome;
    use marketfeed_model::{OverflowPolicy, SystemEvent};

    use crate::memory::MemorySink;
    use crate::sink::{EventSink, SinkError};
    use crate::wire_json::{batch_json, system_json};

    /// Bounded NATS PUB sink (TCP text protocol).
    #[derive(Debug)]
    pub struct NatsSink {
        inner: MemorySink,
        stream: TcpStream,
        subject: String,
        messages_sent: u64,
    }

    impl NatsSink {
        /// TCP connect + `INFO`/`CONNECT` handshake; `subject` required.
        pub fn connect(
            server: impl ToSocketAddrs,
            subject: impl Into<String>,
            batch_capacity: usize,
            system_capacity: usize,
            policy: OverflowPolicy,
        ) -> io::Result<Self> {
            let subject = subject.into();
            if subject.is_empty() || subject.chars().any(char::is_whitespace) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "nats subject must be non-empty and whitespace-free",
                ));
            }
            let stream = TcpStream::connect(server)?;
            stream.set_nodelay(true)?;
            stream.set_read_timeout(Some(Duration::from_secs(5)))?;
            stream.set_write_timeout(Some(Duration::from_secs(5)))?;
            handshake(&stream)?;
            Ok(Self {
                inner: MemorySink::new(batch_capacity, system_capacity, policy),
                stream,
                subject,
                messages_sent: 0,
            })
        }

        pub fn connect_addr(
            addr: SocketAddr,
            subject: impl Into<String>,
            batch_capacity: usize,
            system_capacity: usize,
            policy: OverflowPolicy,
        ) -> io::Result<Self> {
            Self::connect(addr, subject, batch_capacity, system_capacity, policy)
        }

        pub fn subject(&self) -> &str {
            &self.subject
        }

        pub fn messages_sent(&self) -> u64 {
            self.messages_sent
        }

        pub fn dropped_batches(&self) -> u64 {
            self.inner.dropped_batches()
        }

        pub fn dropped_systems(&self) -> u64 {
            self.inner.dropped_systems()
        }

        fn publish(&mut self, payload: &[u8]) -> Result<(), SinkError> {
            let header = format!("PUB {} {}\r\n", self.subject, payload.len());
            self.stream
                .write_all(header.as_bytes())
                .map_err(|e| SinkError::Io(format!("nats PUB header: {e}")))?;
            self.stream
                .write_all(payload)
                .map_err(|e| SinkError::Io(format!("nats PUB payload: {e}")))?;
            self.stream
                .write_all(b"\r\n")
                .map_err(|e| SinkError::Io(format!("nats PUB trailer: {e}")))?;
            self.messages_sent += 1;
            Ok(())
        }

        fn flush_accepted_batches(&mut self) -> Result<(), SinkError> {
            while let Some(b) = self.inner.pop_batch() {
                let payload = batch_json(&b)?;
                self.publish(&payload)?;
            }
            Ok(())
        }

        fn flush_accepted_systems(&mut self) -> Result<(), SinkError> {
            while let Some(ev) = self.inner.pop_system() {
                let payload = system_json(&ev)?;
                self.publish(&payload)?;
            }
            Ok(())
        }
    }

    impl EventSink for NatsSink {
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

    fn handshake(stream: &TcpStream) -> io::Result<()> {
        let reader_stream = stream.try_clone()?;
        let mut reader = BufReader::new(reader_stream);
        let mut line = String::new();
        reader.read_line(&mut line)?;
        if !line.to_ascii_uppercase().starts_with("INFO") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("nats expected INFO, got {line:?}"),
            ));
        }
        let connect = concat!(
            r#"CONNECT {"verbose":false,"pedantic":false,"name":"marketfeed","lang":"rust","version":"0.1.0","protocol":1}"#,
            "\r\n"
        );
        let mut writer = stream.try_clone()?;
        writer.write_all(connect.as_bytes())?;
        writer.flush()?;
        Ok(())
    }

    /// Encode a NATS `PUB` frame (header + payload + CRLF) for tests / docs.
    #[cfg(test)]
    fn encode_pub(subject: &str, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(32 + subject.len() + payload.len());
        out.extend_from_slice(format!("PUB {} {}\r\n", subject, payload.len()).as_bytes());
        out.extend_from_slice(payload);
        out.extend_from_slice(b"\r\n");
        out
    }

    #[cfg(test)]
    mod encode_tests {
        use super::encode_pub;

        #[test]
        fn pub_frame_shape() {
            let frame = encode_pub("mf.events", b"hello");
            assert_eq!(&frame[..], b"PUB mf.events 5\r\nhello\r\n");
        }
    }
}

#[cfg(feature = "nats")]
pub use real::NatsSink;

/// Compile-only / feature-off stub.
#[cfg(not(feature = "nats"))]
#[derive(Debug, Default, Clone, Copy)]
pub struct NatsSink;

#[cfg(not(feature = "nats"))]
impl NatsSink {
    pub fn new() -> Self {
        Self
    }
}

#[cfg(not(feature = "nats"))]
impl EventSink for NatsSink {
    fn push_batch(&mut self, _batch: EventBatch) -> Result<PushOutcome, SinkError> {
        Err(SinkError::Unsupported(
            "nats: enable feature `nats` (TCP PUB client)",
        ))
    }

    fn push_system(&mut self, _event: SystemEvent) -> Result<PushOutcome, SinkError> {
        Err(SinkError::Unsupported(
            "nats: enable feature `nats` (TCP PUB client)",
        ))
    }
}
