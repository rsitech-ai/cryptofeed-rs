//! In-memory transport for tests and offline inject loops.

use std::collections::VecDeque;

use crate::{
    CloseReason, FrameBuffer, FrameOpcode, InboundFrame, OutboundFrame, TransportError,
    WebSocketSpec, WebSocketTransport,
};

/// Queue-backed WebSocket stand-in. No sockets.
#[derive(Debug, Default)]
pub struct MemoryWebSocket {
    connected: bool,
    max_frame_bytes: usize,
    inbound: VecDeque<InboundFrame>,
    pub outbound: Vec<OutboundFrame>,
}

impl MemoryWebSocket {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_inbound(&mut self, frame: InboundFrame) {
        self.inbound.push_back(frame);
    }

    pub fn push_text(&mut self, text: impl Into<Vec<u8>>) {
        self.push_inbound(InboundFrame {
            opcode: FrameOpcode::Text,
            payload: text.into(),
        });
    }
}

impl WebSocketTransport for MemoryWebSocket {
    async fn connect(&mut self, spec: &WebSocketSpec) -> Result<(), TransportError> {
        self.connected = true;
        self.max_frame_bytes = spec.max_frame_bytes;
        Ok(())
    }

    async fn read_frame(
        &mut self,
        buffer: &mut FrameBuffer,
    ) -> Result<InboundFrame, TransportError> {
        if !self.connected {
            return Err(TransportError::NotConnected);
        }
        let frame = self.inbound.pop_front().ok_or(TransportError::Closed)?;
        if frame.payload.len() > self.max_frame_bytes {
            return Err(TransportError::FrameTooLarge(frame.payload.len()));
        }
        buffer.clear();
        buffer.bytes.extend_from_slice(&frame.payload);
        Ok(InboundFrame {
            opcode: frame.opcode,
            payload: buffer.bytes.clone(),
        })
    }

    async fn write_frame(&mut self, frame: OutboundFrame) -> Result<(), TransportError> {
        if !self.connected {
            return Err(TransportError::NotConnected);
        }
        if frame.payload.len() > self.max_frame_bytes {
            return Err(TransportError::FrameTooLarge(frame.payload.len()));
        }
        self.outbound.push(frame);
        Ok(())
    }

    async fn close(&mut self, _reason: CloseReason) -> Result<(), TransportError> {
        self.connected = false;
        self.inbound.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_ws_roundtrip() {
        let mut ws = MemoryWebSocket::new();
        ws.connect(&WebSocketSpec {
            url: "memory://test".into(),
            max_frame_bytes: 1024,
            ..WebSocketSpec::default()
        })
        .await
        .unwrap();
        ws.push_text(b"hello".to_vec());
        let mut buf = FrameBuffer::default();
        let frame = ws.read_frame(&mut buf).await.unwrap();
        assert_eq!(frame.opcode, FrameOpcode::Text);
        assert_eq!(frame.payload, b"hello");
        ws.write_frame(OutboundFrame {
            opcode: FrameOpcode::Text,
            payload: b"out".to_vec(),
        })
        .await
        .unwrap();
        assert_eq!(ws.outbound.len(), 1);
    }
}
