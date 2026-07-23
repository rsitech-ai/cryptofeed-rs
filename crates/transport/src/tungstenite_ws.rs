//! Production WebSocket transport: tokio-tungstenite + Rustls (webpki roots).

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{
        client::IntoClientRequest,
        error::{CapacityError, Error as TungsteniteError},
        handshake::client::Request,
        http::{HeaderName, HeaderValue},
        protocol::{Message, WebSocketConfig},
    },
};

use crate::{
    CloseReason, FrameBuffer, FrameOpcode, InboundFrame, OutboundFrame, TransportError,
    WebSocketSpec, WebSocketTransport,
};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Real WebSocket client. TLS verifies certificates and hostnames (no insecure mode).
#[derive(Default)]
pub struct TungsteniteWebSocket {
    stream: Option<WsStream>,
    max_frame_bytes: usize,
}

impl TungsteniteWebSocket {
    pub fn new() -> Self {
        Self::default()
    }
}

fn build_connect_parts(spec: &WebSocketSpec) -> Result<(Request, WebSocketConfig), TransportError> {
    let mut request = spec
        .url
        .as_str()
        .into_client_request()
        .map_err(|error| TransportError::Protocol(error.to_string()))?;
    for (name, value) in &spec.headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|error| TransportError::Protocol(error.to_string()))?;
        let value = HeaderValue::from_str(value)
            .map_err(|error| TransportError::Protocol(error.to_string()))?;
        request.headers_mut().append(name, value);
    }

    let mut config = WebSocketConfig::default();
    config.max_frame_size = Some(spec.max_frame_bytes);
    config.max_message_size = Some(spec.max_frame_bytes);
    Ok((request, config))
}

fn map_tungstenite_error(error: TungsteniteError) -> TransportError {
    match error {
        TungsteniteError::ConnectionClosed | TungsteniteError::AlreadyClosed => {
            TransportError::Closed
        }
        TungsteniteError::Io(error) => TransportError::Io(error.to_string()),
        TungsteniteError::Tls(error) => TransportError::Tls(error.to_string()),
        TungsteniteError::Capacity(CapacityError::MessageTooLong { size, .. }) => {
            TransportError::FrameTooLarge(size)
        }
        TungsteniteError::Capacity(error) => TransportError::Protocol(error.to_string()),
        TungsteniteError::Protocol(error) => TransportError::Protocol(error.to_string()),
        TungsteniteError::WriteBufferFull(message) => {
            TransportError::Protocol(format!("write buffer full for {message}"))
        }
        TungsteniteError::Utf8 => TransportError::Protocol("invalid UTF-8".into()),
        TungsteniteError::AttackAttempt => {
            TransportError::Protocol("WebSocket attack attempt detected".into())
        }
        TungsteniteError::Url(error) => TransportError::Protocol(error.to_string()),
        TungsteniteError::Http(response) => {
            TransportError::Protocol(format!("WebSocket HTTP status {}", response.status()))
        }
        TungsteniteError::HttpFormat(error) => TransportError::Protocol(error.to_string()),
    }
}

impl WebSocketTransport for TungsteniteWebSocket {
    async fn connect(&mut self, spec: &WebSocketSpec) -> Result<(), TransportError> {
        let (request, config) = build_connect_parts(spec)?;
        let (ws, _resp) = connect_async_with_config(request, Some(config), spec.tcp_nodelay)
            .await
            .map_err(map_tungstenite_error)?;
        self.max_frame_bytes = spec.max_frame_bytes;
        self.stream = Some(ws);
        Ok(())
    }

    async fn read_frame(
        &mut self,
        buffer: &mut FrameBuffer,
    ) -> Result<InboundFrame, TransportError> {
        let ws = self.stream.as_mut().ok_or(TransportError::NotConnected)?;
        loop {
            let msg = match ws.next().await {
                Some(Ok(m)) => m,
                Some(Err(e)) => return Err(map_tungstenite_error(e)),
                None => return Err(TransportError::Closed),
            };
            match msg {
                Message::Text(text) => {
                    let bytes = text.as_bytes().to_vec();
                    if bytes.len() > self.max_frame_bytes {
                        return Err(TransportError::FrameTooLarge(bytes.len()));
                    }
                    buffer.clear();
                    buffer.bytes.extend_from_slice(&bytes);
                    return Ok(InboundFrame {
                        opcode: FrameOpcode::Text,
                        payload: buffer.bytes.clone(),
                    });
                }
                Message::Binary(bin) => {
                    if bin.len() > self.max_frame_bytes {
                        return Err(TransportError::FrameTooLarge(bin.len()));
                    }
                    buffer.clear();
                    buffer.bytes.extend_from_slice(&bin);
                    return Ok(InboundFrame {
                        opcode: FrameOpcode::Binary,
                        payload: buffer.bytes.clone(),
                    });
                }
                Message::Ping(payload) => {
                    // Auto-pong for transport liveness (venue app heartbeats remain separate).
                    ws.send(Message::Pong(payload.clone()))
                        .await
                        .map_err(map_tungstenite_error)?;
                    return Ok(InboundFrame {
                        opcode: FrameOpcode::Ping,
                        payload: payload.to_vec(),
                    });
                }
                Message::Pong(payload) => {
                    return Ok(InboundFrame {
                        opcode: FrameOpcode::Pong,
                        payload: payload.to_vec(),
                    });
                }
                Message::Close(_) => {
                    self.stream = None;
                    return Err(TransportError::Closed);
                }
                Message::Frame(_) => continue,
            }
        }
    }

    async fn write_frame(&mut self, frame: OutboundFrame) -> Result<(), TransportError> {
        let ws = self.stream.as_mut().ok_or(TransportError::NotConnected)?;
        if frame.payload.len() > self.max_frame_bytes {
            return Err(TransportError::FrameTooLarge(frame.payload.len()));
        }
        let msg = match frame.opcode {
            FrameOpcode::Text => Message::Text(
                String::from_utf8(frame.payload)
                    .map_err(|e| TransportError::Protocol(e.to_string()))?
                    .into(),
            ),
            FrameOpcode::Binary => Message::Binary(frame.payload.into()),
            FrameOpcode::Ping => Message::Ping(frame.payload.into()),
            FrameOpcode::Pong => Message::Pong(frame.payload.into()),
            FrameOpcode::Close => Message::Close(None),
        };
        ws.send(msg).await.map_err(map_tungstenite_error)
    }

    async fn close(&mut self, _reason: CloseReason) -> Result<(), TransportError> {
        if let Some(mut ws) = self.stream.take() {
            let _ = ws.close(None).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_parts_apply_headers_and_wire_size_limits() {
        let spec = WebSocketSpec {
            url: "wss://example.test/feed".into(),
            headers: vec![
                ("authorization".into(), "Bearer test-token".into()),
                ("x-client-id".into(), "cryptofeed".into()),
            ],
            max_frame_bytes: 1_024,
            tcp_nodelay: true,
        };

        let (request, config) = build_connect_parts(&spec).unwrap();

        assert_eq!(
            request.headers().get("authorization").unwrap(),
            "Bearer test-token"
        );
        assert_eq!(request.headers().get("x-client-id").unwrap(), "cryptofeed");
        assert_eq!(config.max_frame_size, Some(1_024));
        assert_eq!(config.max_message_size, Some(1_024));
    }

    #[test]
    fn connect_parts_reject_invalid_header_values() {
        let spec = WebSocketSpec {
            url: "wss://example.test/feed".into(),
            headers: vec![("x-test".into(), "line one\nline two".into())],
            ..WebSocketSpec::default()
        };

        let error = build_connect_parts(&spec).unwrap_err();
        assert!(matches!(error, TransportError::Protocol(_)), "{error}");
    }

    #[test]
    fn tungstenite_errors_preserve_recovery_classification() {
        use tokio_tungstenite::tungstenite::error::CapacityError;

        assert_eq!(
            map_tungstenite_error(tokio_tungstenite::tungstenite::Error::ConnectionClosed),
            TransportError::Closed
        );
        assert_eq!(
            map_tungstenite_error(tokio_tungstenite::tungstenite::Error::Capacity(
                CapacityError::MessageTooLong {
                    size: 2_048,
                    max_size: 1_024,
                }
            )),
            TransportError::FrameTooLarge(2_048)
        );
        assert!(matches!(
            map_tungstenite_error(tokio_tungstenite::tungstenite::Error::Protocol(
                tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
            )),
            TransportError::Protocol(_)
        ));
    }
}
