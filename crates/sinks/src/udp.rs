//! UDP datagram `EventSink` (best-effort, bounded ingress).

use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

use marketfeed_adapter_api::EventBatch;
use marketfeed_dispatch::PushOutcome;
use marketfeed_model::{OverflowPolicy, SystemEvent};

use crate::memory::MemorySink;
use crate::sink::{EventSink, SinkError};
use crate::wire_json::{batch_json, system_json};

/// Bounded sink that sends accepted items as short UTF-8 datagrams.
///
/// Ingress is a [`MemorySink`] with an explicit [`OverflowPolicy`]. Wire delivery
/// is **best-effort**: `send` failures increment [`UdpSink::send_failures`] and do
/// **not** fail the push (datagrams may be dropped by the kernel or network).
///
/// # ponytail
/// Sync `send` blocks the caller; ceiling = stall under a full OS send buffer.
/// Payload is the complete JSON shape shared with [`crate::FileSink`].
/// Upgrade = non-blocking socket + background sender and fragmentation policy
/// for payloads above the datagram limit.
#[derive(Debug)]
pub struct UdpSink {
    inner: MemorySink,
    socket: UdpSocket,
    datagrams_sent: u64,
    send_failures: u64,
}

impl UdpSink {
    /// Bind an ephemeral local port and `connect` to `dest` (IPv4 or IPv6).
    pub fn connect(
        dest: SocketAddr,
        batch_capacity: usize,
        system_capacity: usize,
        policy: OverflowPolicy,
    ) -> io::Result<Self> {
        let local = match dest {
            SocketAddr::V4(_) => SocketAddr::from(([0, 0, 0, 0], 0)),
            SocketAddr::V6(_) => SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 0)),
        };
        let socket = UdpSocket::bind(local)?;
        socket.connect(dest)?;
        // Avoid indefinite block if the peer is gone (best-effort, not reliable).
        socket.set_write_timeout(Some(Duration::from_millis(50)))?;
        Ok(Self {
            inner: MemorySink::new(batch_capacity, system_capacity, policy),
            socket,
            datagrams_sent: 0,
            send_failures: 0,
        })
    }

    pub fn datagrams_sent(&self) -> u64 {
        self.datagrams_sent
    }

    pub fn send_failures(&self) -> u64 {
        self.send_failures
    }

    pub fn dropped_batches(&self) -> u64 {
        self.inner.dropped_batches()
    }

    pub fn dropped_systems(&self) -> u64 {
        self.inner.dropped_systems()
    }

    fn send_line(&mut self, line: &[u8]) {
        match self.socket.send(line) {
            Ok(_) => self.datagrams_sent += 1,
            Err(_) => self.send_failures += 1,
        }
    }

    fn flush_accepted_batches(&mut self) -> Result<(), SinkError> {
        while let Some(b) = self.inner.pop_batch() {
            let line = batch_json(&b)?;
            self.send_line(&line);
        }
        Ok(())
    }

    fn flush_accepted_systems(&mut self) -> Result<(), SinkError> {
        while let Some(ev) = self.inner.pop_system() {
            let line = system_json(&ev)?;
            self.send_line(&line);
        }
        Ok(())
    }
}

impl EventSink for UdpSink {
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
