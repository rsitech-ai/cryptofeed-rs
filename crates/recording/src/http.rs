//! Bounded binary payload for MFR1 v2 HTTP-response records.

use bytes::Bytes;
use marketfeed_adapter_api::HttpResponse;

use crate::{MAX_RAW_RECORD_LEN, RAW_HEADER_BODY_LEN, RecordingError};

const MAX_HEADERS: usize = 1024;
const MAX_HEADER_FIELD_BYTES: usize = 64 * 1024;
const REDACTED: &str = "[REDACTED]";

pub fn encode_http_response(
    request_id: u64,
    response: &HttpResponse,
) -> Result<Vec<u8>, RecordingError> {
    if response.headers.len() > MAX_HEADERS {
        return Err(RecordingError::InvalidHeader);
    }
    let maximum_payload_len = MAX_RAW_RECORD_LEN as usize - 4 - RAW_HEADER_BODY_LEN;
    if response.body.len() > maximum_payload_len {
        return Err(record_too_large(response.body.len()));
    }
    let mut out = Vec::new();
    out.extend_from_slice(&request_id.to_le_bytes());
    out.extend_from_slice(&response.status.to_le_bytes());
    push_u32(&mut out, response.headers.len())?;
    for (name, value) in &response.headers {
        push_bytes(&mut out, name.as_bytes(), MAX_HEADER_FIELD_BYTES)?;
        let value = if is_sensitive_header(name) {
            REDACTED
        } else {
            value
        };
        push_bytes(&mut out, value.as_bytes(), MAX_HEADER_FIELD_BYTES)?;
    }
    push_bytes(&mut out, &response.body, u32::MAX as usize)?;
    if out.len() > maximum_payload_len {
        return Err(record_too_large(out.len()));
    }
    Ok(out)
}

fn record_too_large(payload_len: usize) -> RecordingError {
    let record_len = u32::try_from(4 + RAW_HEADER_BODY_LEN + payload_len).unwrap_or(u32::MAX);
    RecordingError::RecordTooLarge {
        record_len,
        max: MAX_RAW_RECORD_LEN,
    }
}

fn is_sensitive_header(name: &str) -> bool {
    matches!(
        name.trim().to_ascii_lowercase().as_str(),
        "authorization"
            | "proxy-authorization"
            | "cookie"
            | "set-cookie"
            | "x-api-key"
            | "x-auth-token"
    )
}

pub fn decode_http_response(payload: &[u8]) -> Result<(u64, HttpResponse), RecordingError> {
    let mut cursor = PayloadCursor::new(payload);
    let request_id = cursor.read_u64()?;
    let status = cursor.read_u16()?;
    let header_count = cursor.read_u32()? as usize;
    if header_count > MAX_HEADERS {
        return Err(RecordingError::InvalidHeader);
    }
    let mut headers = Vec::with_capacity(header_count);
    for _ in 0..header_count {
        let name = cursor.read_string(MAX_HEADER_FIELD_BYTES)?;
        let value = cursor.read_string(MAX_HEADER_FIELD_BYTES)?;
        headers.push((name, value));
    }
    let body = Bytes::copy_from_slice(cursor.read_bytes(u32::MAX as usize)?);
    if !cursor.is_empty() {
        return Err(RecordingError::InvalidHeader);
    }
    Ok((
        request_id,
        HttpResponse {
            status,
            headers,
            body,
        },
    ))
}

fn push_u32(out: &mut Vec<u8>, value: usize) -> Result<(), RecordingError> {
    let value = u32::try_from(value).map_err(|_| RecordingError::InvalidHeader)?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn push_bytes(out: &mut Vec<u8>, value: &[u8], maximum: usize) -> Result<(), RecordingError> {
    if value.len() > maximum {
        return Err(RecordingError::InvalidHeader);
    }
    push_u32(out, value.len())?;
    out.extend_from_slice(value);
    Ok(())
}

struct PayloadCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> PayloadCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_u16(&mut self) -> Result<u16, RecordingError> {
        let bytes: [u8; 2] = self.read_exact(2)?.try_into().unwrap();
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_u32(&mut self) -> Result<u32, RecordingError> {
        let bytes: [u8; 4] = self.read_exact(4)?.try_into().unwrap();
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, RecordingError> {
        let bytes: [u8; 8] = self.read_exact(8)?.try_into().unwrap();
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_bytes(&mut self, maximum: usize) -> Result<&'a [u8], RecordingError> {
        let length = self.read_u32()? as usize;
        if length > maximum {
            return Err(RecordingError::InvalidHeader);
        }
        self.read_exact(length)
    }

    fn read_string(&mut self, maximum: usize) -> Result<String, RecordingError> {
        let bytes = self.read_bytes(maximum)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| RecordingError::InvalidHeader)
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], RecordingError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(RecordingError::InvalidHeader)?;
        if end > self.bytes.len() {
            return Err(RecordingError::Truncated);
        }
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_response_roundtrip_preserves_id_headers_and_binary_body() {
        let response = HttpResponse {
            status: 206,
            headers: vec![
                ("content-type".into(), "application/octet-stream".into()),
                ("x-test".into(), "value".into()),
            ],
            body: Bytes::from_static(&[0, 1, 2, 255]),
        };
        let payload = encode_http_response(42, &response).unwrap();
        let (request_id, decoded) = decode_http_response(&payload).unwrap();
        assert_eq!(request_id, 42);
        assert_eq!(decoded, response);
    }

    #[test]
    fn http_response_rejects_truncated_and_trailing_payloads() {
        let response = HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(b"body"),
        };
        let mut payload = encode_http_response(1, &response).unwrap();
        assert_eq!(
            decode_http_response(&payload[..payload.len() - 1]),
            Err(RecordingError::Truncated)
        );
        payload.push(0);
        assert_eq!(
            decode_http_response(&payload),
            Err(RecordingError::InvalidHeader)
        );
    }

    #[test]
    fn http_response_redacts_sensitive_header_values_before_persistence() {
        let response = HttpResponse {
            status: 200,
            headers: vec![
                ("Set-Cookie".into(), "session=secret".into()),
                ("X-Request-Id".into(), "public-id".into()),
            ],
            body: Bytes::new(),
        };
        let payload = encode_http_response(1, &response).unwrap();
        assert!(!payload.windows(b"secret".len()).any(|w| w == b"secret"));
        let (_, decoded) = decode_http_response(&payload).unwrap();
        assert_eq!(decoded.headers[0].1, REDACTED);
        assert_eq!(decoded.headers[1].1, "public-id");
    }

    #[test]
    fn http_response_rejects_payloads_that_cannot_fit_a_raw_record() {
        let response = HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: vec![0; MAX_RAW_RECORD_LEN as usize].into(),
        };
        assert!(matches!(
            encode_http_response(1, &response),
            Err(RecordingError::RecordTooLarge { .. })
        ));
    }
}
