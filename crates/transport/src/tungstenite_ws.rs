//! Production WebSocket transport: tokio-tungstenite + Rustls (webpki roots).

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    Connector, MaybeTlsStream, WebSocketStream, connect_async_tls_with_config,
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

#[cfg(test)]
#[path = "tls_tests.rs"]
mod tls_tests;

fn tls_config(roots: rustls::RootCertStore) -> Result<rustls::ClientConfig, TransportError> {
    rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|error| TransportError::Tls(error.to_string()))
    .map(|builder| builder.with_root_certificates(roots).with_no_client_auth())
}
fn public_tls_connector() -> Result<Connector, TransportError> {
    let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Ok(Connector::Rustls(std::sync::Arc::new(tls_config(roots)?)))
}

/// Real WebSocket client. TLS verifies certificates and hostnames (no insecure mode).
#[derive(Default)]
pub struct TungsteniteWebSocket {
    stream: Option<WsStream>,
    max_frame_bytes: usize,
    queued_automatic_pong: bool,
}

impl TungsteniteWebSocket {
    pub fn new() -> Self {
        Self::default()
    }

    /// Opt-in mode for the causal driver. A consumed Ping is returned without
    /// another await. Tungstenite 0.26 queues its automatic Pong while reading
    /// the Ping, and flushes that control on a subsequent read or write.
    ///
    /// This does not make arbitrary `WebSocketTransport` implementations cancel
    /// safe. Close payloads remain unavailable: Close is reported as `Closed`.
    pub fn new_causal() -> Self {
        Self {
            queued_automatic_pong: true,
            ..Self::default()
        }
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
        // Keep the audited0.26 frame/cancellation backend, but supply current
        // roots directly: its0.26 roots wrapper has no patched release.
        // The normal Rustls verifier still enforces certificate/hostname checks.
        let (ws, _resp) = connect_async_tls_with_config(
            request,
            Some(config),
            spec.tcp_nodelay,
            Some(public_tls_connector()?),
        )
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
                    if !self.queued_automatic_pong {
                        // Preserve the legacy explicit flush behavior. Causal
                        // mode uses the library's already queued automatic Pong.
                        ws.send(Message::Pong(payload.clone()))
                            .await
                            .map_err(map_tungstenite_error)?;
                    }
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
            if self.queued_automatic_pong {
                ws.close(None).await.map_err(map_tungstenite_error)?;
            } else {
                let _ = ws.close(None).await;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn causal_loopback(flush_with_write: bool) {
        use std::time::Duration;
        use tokio::{net::TcpListener, sync::oneshot, time::timeout};

        timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (send_frames, wait_for_reader) = oneshot::channel();
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
                // The client explicitly polls then drops its first pending read
                // before opening this barrier. No timing sleep establishes it.
                wait_for_reader.await.unwrap();
                ws.send(Message::Ping(b"native-ping".to_vec().into()))
                    .await
                    .unwrap();
                if !flush_with_write {
                    ws.send(Message::Text("after-ping".into())).await.unwrap();
                }
                let mut observed = Vec::new();
                let mut confirmed = false;
                while let Some(message) = ws.next().await {
                    let message = message.unwrap();
                    let closing = matches!(message, Message::Close(_));
                    observed.push(message);
                    if flush_with_write
                        && !confirmed
                        && observed.iter().any(|m| matches!(m, Message::Pong(_)))
                        && observed.iter().any(|m| matches!(m, Message::Text(_)))
                    {
                        ws.send(Message::Text("after-ping".into())).await.unwrap();
                        confirmed = true;
                    }
                    if closing {
                        break;
                    }
                }
                observed
            });
            let mut client = TungsteniteWebSocket::new_causal();
            client
                .connect(&WebSocketSpec {
                    url: format!("ws://{address}/"),
                    ..WebSocketSpec::default()
                })
                .await
                .unwrap();
            let mut buffer = FrameBuffer::with_capacity(1024);
            let mut pending = Box::pin(client.read_frame(&mut buffer));
            std::future::poll_fn(|cx| {
                assert!(std::future::Future::poll(pending.as_mut(), cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            drop(pending);
            send_frames.send(()).unwrap();
            let ping = client.read_frame(&mut buffer).await.unwrap();
            assert_eq!(ping.opcode, FrameOpcode::Ping);
            assert_eq!(ping.payload, b"native-ping");
            let application = OutboundFrame {
                opcode: FrameOpcode::Text,
                payload: b"one-application-write".to_vec(),
            };
            if flush_with_write {
                client.write_frame(application.clone()).await.unwrap();
            }
            let text = client.read_frame(&mut buffer).await.unwrap();
            assert_eq!(text.opcode, FrameOpcode::Text);
            assert_eq!(text.payload, b"after-ping");
            if !flush_with_write {
                client.write_frame(application).await.unwrap();
            }
            client.close(CloseReason::LocalStop).await.unwrap();
            let observed = server.await.unwrap();
            assert_eq!(
                observed
                    .iter()
                    .filter(|m| matches!(m, Message::Pong(_)))
                    .count(),
                1,
                "automatic Pong must not be duplicated: {observed:?}"
            );
            assert!(
                observed
                    .iter()
                    .any(|m| matches!(m, Message::Pong(p) if p.as_ref() == b"native-ping"))
            );
            assert_eq!(
                observed
                    .iter()
                    .filter(|m| matches!(m, Message::Text(_)))
                    .count(),
                1
            );
            assert!(
                observed.iter().any(
                    |m| matches!(m, Message::Text(t) if t.as_str() == "one-application-write")
                )
            );
        })
        .await
        .expect("loopback protocol barriers must complete");
    }

    #[tokio::test]
    async fn causal_pending_read_cancellation_preserves_ping_and_text_next_read_flushes() {
        causal_loopback(false).await;
    }

    #[tokio::test]
    async fn causal_application_write_flushes_one_queued_pong() {
        causal_loopback(true).await;
    }

    #[test]
    fn legacy_constructor_retains_explicit_pong_mode() {
        assert!(!TungsteniteWebSocket::new().queued_automatic_pong);
        assert!(TungsteniteWebSocket::new_causal().queued_automatic_pong);
    }

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
            connect_timeout_ms: 10_000,
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
