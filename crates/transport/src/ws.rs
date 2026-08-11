//! WebSocket transport trait and frame types.

use crate::TransportError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebSocketSpec {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub max_frame_bytes: usize,
    pub tcp_nodelay: bool,
    pub connect_timeout_ms: u64,
}

impl Default for WebSocketSpec {
    fn default() -> Self {
        Self {
            url: String::new(),
            headers: Vec::new(),
            max_frame_bytes: 16 * 1024 * 1024,
            tcp_nodelay: true,
            connect_timeout_ms: 10_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameOpcode {
    Text,
    Binary,
    Ping,
    Pong,
    Close,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundFrame {
    pub opcode: FrameOpcode,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundFrame {
    pub opcode: FrameOpcode,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloseReason {
    Normal,
    GoingAway,
    ProtocolError,
    LocalStop,
}

/// Reusable receive scratch buffer.
#[derive(Debug, Default)]
pub struct FrameBuffer {
    pub bytes: Vec<u8>,
}

impl FrameBuffer {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(cap),
        }
    }

    pub fn clear(&mut self) {
        self.bytes.clear();
    }
}

/// Engine-owned WebSocket I/O. Adapters never call this.
pub trait WebSocketTransport: Send {
    fn connect(
        &mut self,
        spec: &WebSocketSpec,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    fn read_frame(
        &mut self,
        buffer: &mut FrameBuffer,
    ) -> impl Future<Output = Result<InboundFrame, TransportError>> + Send;

    fn write_frame(
        &mut self,
        frame: OutboundFrame,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    fn close(
        &mut self,
        reason: CloseReason,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;
}
