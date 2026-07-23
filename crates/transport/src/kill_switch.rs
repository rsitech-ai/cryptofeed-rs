//! One-shot kill switch over any [`WebSocketTransport`].
//!
//! Live reconnect probe: set `kill` after Live, engine sees `Closed`, reconnects.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::{
    CloseReason, FrameBuffer, InboundFrame, OutboundFrame, TransportError, WebSocketSpec,
    WebSocketTransport,
};

/// Wraps an inner transport; when `kill` is true, the next `read_frame` closes and returns
/// [`TransportError::Closed`] (one-shot: flag is cleared).
pub struct KillSwitchWebSocket<T> {
    inner: T,
    kill: Arc<AtomicBool>,
}

impl<T> KillSwitchWebSocket<T> {
    pub fn new(inner: T, kill: Arc<AtomicBool>) -> Self {
        Self { inner, kill }
    }
}

impl<T: WebSocketTransport> WebSocketTransport for KillSwitchWebSocket<T> {
    async fn connect(&mut self, spec: &WebSocketSpec) -> Result<(), TransportError> {
        self.inner.connect(spec).await
    }

    async fn read_frame(
        &mut self,
        buffer: &mut FrameBuffer,
    ) -> Result<InboundFrame, TransportError> {
        // ponytail: one-shot only; ceiling = kill only observed on next read (≤ stop-poll).
        // upgrade = Notify/select if sub-poll latency matters.
        if self.kill.swap(false, Ordering::Relaxed) {
            let _ = self.inner.close(CloseReason::GoingAway).await;
            return Err(TransportError::Closed);
        }
        self.inner.read_frame(buffer).await
    }

    async fn write_frame(&mut self, frame: OutboundFrame) -> Result<(), TransportError> {
        self.inner.write_frame(frame).await
    }

    async fn close(&mut self, reason: CloseReason) -> Result<(), TransportError> {
        self.inner.close(reason).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FrameOpcode, MemoryWebSocket};

    #[tokio::test]
    async fn kill_switch_returns_closed_once() {
        let kill = Arc::new(AtomicBool::new(false));
        let mut ws = KillSwitchWebSocket::new(MemoryWebSocket::new(), Arc::clone(&kill));
        ws.connect(&WebSocketSpec {
            url: "memory://".into(),
            max_frame_bytes: 64,
            ..WebSocketSpec::default()
        })
        .await
        .unwrap();
        ws.inner.push_text(b"a".to_vec());
        let mut buf = FrameBuffer::default();
        assert_eq!(
            ws.read_frame(&mut buf).await.unwrap().opcode,
            FrameOpcode::Text
        );

        kill.store(true, Ordering::Relaxed);
        assert!(matches!(
            ws.read_frame(&mut buf).await,
            Err(TransportError::Closed)
        ));
        assert!(!kill.load(Ordering::Relaxed));

        // After kill, reconnect path can connect again.
        ws.connect(&WebSocketSpec {
            url: "memory://".into(),
            max_frame_bytes: 64,
            ..WebSocketSpec::default()
        })
        .await
        .unwrap();
        ws.inner.push_text(b"b".to_vec());
        assert_eq!(ws.read_frame(&mut buf).await.unwrap().payload, b"b");
    }
}
