//! Shared HTTP client abstraction (one client per engine/venue, not per request).

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::Duration;

use bytes::{Bytes, BytesMut};

use crate::TransportError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    pub method: HttpMethod,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Bytes>,
    pub timeout_ms: u64,
    pub max_body_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

pub trait HttpTransport: Send + Sync {
    fn request(
        &self,
        req: HttpRequest,
    ) -> impl Future<Output = Result<HttpResponse, TransportError>> + Send;
}

fn append_bounded_body(
    body: &mut BytesMut,
    chunk: &[u8],
    max_body_bytes: usize,
) -> Result<(), TransportError> {
    let next_len = body.len().saturating_add(chunk.len());
    if next_len > max_body_bytes {
        return Err(TransportError::FrameTooLarge(next_len));
    }
    body.extend_from_slice(chunk);
    Ok(())
}

/// Unit-test stub: always errors unless replaced.
#[derive(Debug, Default)]
pub struct StubHttpTransport;

impl HttpTransport for StubHttpTransport {
    async fn request(&self, _req: HttpRequest) -> Result<HttpResponse, TransportError> {
        Err(TransportError::Stub("HTTP transport not configured".into()))
    }
}

/// Scripted HTTP for offline tests: match by URL substring, FIFO responses.
#[derive(Debug, Default)]
pub struct ScriptedHttpTransport {
    scripts: Mutex<HashMap<String, VecDeque<HttpResponse>>>,
}

impl ScriptedHttpTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&self, url_substr: impl Into<String>, response: HttpResponse) {
        let key = url_substr.into();
        self.scripts
            .lock()
            .expect("script lock")
            .entry(key)
            .or_default()
            .push_back(response);
    }
}

impl HttpTransport for ScriptedHttpTransport {
    async fn request(&self, req: HttpRequest) -> Result<HttpResponse, TransportError> {
        let mut map = self.scripts.lock().expect("script lock");
        for (substr, q) in map.iter_mut() {
            if req.url.contains(substr.as_str()) {
                if let Some(resp) = q.pop_front() {
                    return Ok(resp);
                }
            }
        }
        Err(TransportError::Stub(format!(
            "no scripted response for {}",
            req.url
        )))
    }
}

/// Production HTTP: shared reqwest client with Rustls (webpki roots).
#[derive(Debug, Clone)]
pub struct ReqwestHttpTransport {
    client: reqwest::Client,
}

impl ReqwestHttpTransport {
    pub fn new() -> Result<Self, TransportError> {
        let client = reqwest::Client::builder()
            .use_rustls_tls()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| TransportError::Tls(e.to_string()))?;
        Ok(Self { client })
    }
}

impl Default for ReqwestHttpTransport {
    fn default() -> Self {
        Self::new().expect("reqwest rustls client")
    }
}

impl HttpTransport for ReqwestHttpTransport {
    async fn request(&self, req: HttpRequest) -> Result<HttpResponse, TransportError> {
        let mut builder = match req.method {
            HttpMethod::Get => self.client.get(&req.url),
            HttpMethod::Post => self.client.post(&req.url),
            HttpMethod::Put => self.client.put(&req.url),
            HttpMethod::Delete => self.client.delete(&req.url),
        };
        for (k, v) in &req.headers {
            builder = builder.header(k.as_str(), v.as_str());
        }
        if let Some(body) = req.body {
            builder = builder.body(body);
        }
        builder = builder.timeout(Duration::from_millis(req.timeout_ms.max(1)));

        let mut resp = builder
            .send()
            .await
            .map_err(|e| TransportError::Io(e.to_string()))?;
        let status = resp.status().as_u16();
        let headers = resp
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        if let Some(content_length) = resp.content_length() {
            let content_length = usize::try_from(content_length).unwrap_or(usize::MAX);
            if content_length > req.max_body_bytes {
                return Err(TransportError::FrameTooLarge(content_length));
            }
        }
        let mut body = BytesMut::new();
        while let Some(chunk) = resp
            .chunk()
            .await
            .map_err(|e| TransportError::Io(e.to_string()))?
        {
            append_bounded_body(&mut body, &chunk, req.max_body_bytes)?;
        }
        Ok(HttpResponse {
            status,
            headers,
            body: body.freeze(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn bounded_body_rejects_oversized_chunk_before_append() {
        let mut body = bytes::BytesMut::from(&b"1234"[..]);

        let error = append_bounded_body(&mut body, b"5678", 6).unwrap_err();

        assert_eq!(body.as_ref(), b"1234");
        assert_eq!(error, TransportError::FrameTooLarge(8));
    }

    #[tokio::test]
    async fn reqwest_transport_enforces_limit_while_streaming_chunked_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 1_024];
            let _ = socket.read(&mut request).await.unwrap();
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n4\r\n1234\r\n4\r\n5678\r\n0\r\n\r\n",
                )
                .await
                .unwrap();
        });
        let transport = ReqwestHttpTransport::new().unwrap();

        let error = transport
            .request(HttpRequest {
                method: HttpMethod::Get,
                url: format!("http://{address}/oversized"),
                headers: Vec::new(),
                body: None,
                timeout_ms: 1_000,
                max_body_bytes: 6,
            })
            .await
            .unwrap_err();

        assert_eq!(error, TransportError::FrameTooLarge(8));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn scripted_http_fifo() {
        let http = ScriptedHttpTransport::new();
        http.push(
            "depth",
            HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(b"{\"ok\":1}"),
            },
        );
        let resp = http
            .request(HttpRequest {
                method: HttpMethod::Get,
                url: "https://api.binance.com/api/v3/depth?symbol=BTCUSDT".into(),
                headers: Vec::new(),
                body: None,
                timeout_ms: 1000,
                max_body_bytes: 1024,
            })
            .await
            .unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body.as_ref(), b"{\"ok\":1}");
    }
}
