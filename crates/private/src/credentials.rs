//! API credentials loaded only at private/daemon runtime boundaries.
//!
//! Never log or record secret material. [`Debug`] redacts key/secret values.
//! Public market-data paths must not call this module.

use std::env;
use std::fmt;

use thiserror::Error;

/// Missing or empty credential environment variables.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CredentialsError {
    #[error("missing or empty env var {0}")]
    Missing(&'static str),
}

/// Reserved Binance WebSocket API credentials.
///
/// Private streaming is currently fail-closed while authenticated subscription
/// support is implemented. Secret values remain redacted for future use.
#[derive(Clone)]
pub struct BinanceApiCredentials {
    api_key: String,
    api_secret: Option<String>,
}

impl BinanceApiCredentials {
    /// Load from process environment. Returns [`CredentialsError::Missing`] when
    /// `BINANCE_API_KEY` is unset or empty.
    pub fn from_env() -> Result<Self, CredentialsError> {
        let api_key = require_env("BINANCE_API_KEY")?;
        let api_secret = env::var("BINANCE_API_SECRET")
            .ok()
            .filter(|s| !s.is_empty());
        Ok(Self {
            api_key,
            api_secret,
        })
    }

    /// API key. Do not log or record.
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Optional HMAC secret. Do not log or record.
    pub fn api_secret(&self) -> Option<&str> {
        self.api_secret.as_deref()
    }
}

impl fmt::Debug for BinanceApiCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BinanceApiCredentials")
            .field("api_key", &"<redacted>")
            .field(
                "api_secret",
                &self.api_secret.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// OKX private WS credentials (`OKX_API_KEY` / `OKX_API_SECRET` / `OKX_API_PASSPHRASE`).
#[derive(Clone)]
pub struct OkxApiCredentials {
    api_key: String,
    api_secret: String,
    passphrase: String,
}

impl OkxApiCredentials {
    /// Load from process environment. All three vars required and non-empty.
    pub fn from_env() -> Result<Self, CredentialsError> {
        Ok(Self {
            api_key: require_env("OKX_API_KEY")?,
            api_secret: require_env("OKX_API_SECRET")?,
            passphrase: require_env("OKX_API_PASSPHRASE")?,
        })
    }

    /// API key. Do not log or record.
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// HMAC secret. Do not log or record.
    pub fn api_secret(&self) -> &str {
        &self.api_secret
    }

    /// API passphrase. Do not log or record.
    pub fn passphrase(&self) -> &str {
        &self.passphrase
    }
}

impl fmt::Debug for OkxApiCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OkxApiCredentials")
            .field("api_key", &"<redacted>")
            .field("api_secret", &"<redacted>")
            .field("passphrase", &"<redacted>")
            .finish()
    }
}

/// Bybit private WS credentials (`BYBIT_API_KEY` / `BYBIT_API_SECRET`).
#[derive(Clone)]
pub struct BybitApiCredentials {
    api_key: String,
    api_secret: String,
}

impl BybitApiCredentials {
    /// Load from process environment. Both vars required and non-empty.
    pub fn from_env() -> Result<Self, CredentialsError> {
        Ok(Self {
            api_key: require_env("BYBIT_API_KEY")?,
            api_secret: require_env("BYBIT_API_SECRET")?,
        })
    }

    /// API key. Do not log or record.
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// HMAC secret. Do not log or record.
    pub fn api_secret(&self) -> &str {
        &self.api_secret
    }
}

impl fmt::Debug for BybitApiCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BybitApiCredentials")
            .field("api_key", &"<redacted>")
            .field("api_secret", &"<redacted>")
            .finish()
    }
}

fn require_env(name: &'static str) -> Result<String, CredentialsError> {
    match env::var(name) {
        Ok(v) if !v.is_empty() => Ok(v),
        _ => Err(CredentialsError::Missing(name)),
    }
}

/// Live WS login/auth payload builders (HMAC). Feature `live` only.
#[cfg(feature = "live")]
pub mod sign {
    use base64::Engine;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    use super::{BybitApiCredentials, OkxApiCredentials};

    type HmacSha256 = Hmac<Sha256>;

    /// OKX private WS login body: HMAC-SHA256(timestamp + `GET` + `/users/self/verify`) → Base64.
    ///
    /// `timestamp_secs` is Unix epoch seconds (string form embedded in JSON).
    pub fn okx_login_payload(creds: &OkxApiCredentials, timestamp_secs: i64) -> String {
        let ts = timestamp_secs.to_string();
        let prehash = format!("{ts}GET/users/self/verify");
        let mut mac =
            HmacSha256::new_from_slice(creds.api_secret().as_bytes()).expect("HMAC key length");
        mac.update(prehash.as_bytes());
        let sign = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        // Never log this body — contains apiKey / passphrase / sign.
        format!(
            r#"{{"op":"login","args":[{{"apiKey":"{}","passphrase":"{}","timestamp":"{ts}","sign":"{sign}"}}]}}"#,
            json_escape(creds.api_key()),
            json_escape(creds.passphrase()),
        )
    }

    /// Bybit private WS auth body: hex(HMAC-SHA256(`GET/realtime` + expires)).
    ///
    /// `expires_ms` must be > now (docs: at least 1s ahead).
    pub fn bybit_auth_payload(creds: &BybitApiCredentials, expires_ms: i64) -> String {
        let prehash = format!("GET/realtime{expires_ms}");
        let mut mac =
            HmacSha256::new_from_slice(creds.api_secret().as_bytes()).expect("HMAC key length");
        mac.update(prehash.as_bytes());
        let sign = hex_encode(&mac.finalize().into_bytes());
        format!(
            r#"{{"op":"auth","args":["{}",{expires_ms},"{sign}"]}}"#,
            json_escape(creds.api_key()),
        )
    }

    fn json_escape(s: &str) -> String {
        // ponytail: keys/passphrases are ASCII opaque tokens; escape only JSON string specials.
        s.replace('\\', "\\\\").replace('"', "\\\"")
    }

    fn hex_encode(bytes: &[u8]) -> String {
        const HEX: &[u8] = b"0123456789abcdef";
        let mut out = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0xf) as usize] as char);
        }
        out
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn okx_sign_matches_known_vector() {
            let creds = OkxApiCredentials {
                api_key: "key".into(),
                api_secret: "secret".into(),
                passphrase: "pass".into(),
            };
            let body = okx_login_payload(&creds, 1_538_054_050);
            assert!(body.contains(r#""op":"login""#));
            assert!(body.contains(r#""timestamp":"1538054050""#));
            assert!(!body.contains("secret"));
            let mut mac = HmacSha256::new_from_slice(b"secret").unwrap();
            mac.update(b"1538054050GET/users/self/verify");
            let expect =
                base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
            assert!(body.contains(&expect));
        }

        #[test]
        fn bybit_sign_matches_known_vector() {
            let creds = BybitApiCredentials {
                api_key: "key".into(),
                api_secret: "secret".into(),
            };
            let body = bybit_auth_payload(&creds, 1_662_350_400_000);
            assert!(body.contains(r#""op":"auth""#));
            let mut mac = HmacSha256::new_from_slice(b"secret").unwrap();
            mac.update(b"GET/realtime1662350400000");
            let expect = hex_encode(&mac.finalize().into_bytes());
            assert!(body.contains(&expect));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_binance_secrets() {
        let c = BinanceApiCredentials {
            api_key: "live-key-should-not-appear".into(),
            api_secret: Some("live-secret-should-not-appear".into()),
        };
        let s = format!("{c:?}");
        assert!(s.contains("<redacted>"));
        assert!(!s.contains("live-key"));
        assert!(!s.contains("live-secret"));
    }

    #[test]
    fn debug_redacts_okx_secrets() {
        let c = OkxApiCredentials {
            api_key: "okx-key-leak".into(),
            api_secret: "okx-secret-leak".into(),
            passphrase: "okx-pass-leak".into(),
        };
        let s = format!("{c:?}");
        assert!(s.contains("<redacted>"));
        assert!(!s.contains("okx-key"));
        assert!(!s.contains("okx-secret"));
        assert!(!s.contains("okx-pass"));
    }

    #[test]
    fn debug_redacts_bybit_secrets() {
        let c = BybitApiCredentials {
            api_key: "bybit-key-leak".into(),
            api_secret: "bybit-secret-leak".into(),
        };
        let s = format!("{c:?}");
        assert!(s.contains("<redacted>"));
        assert!(!s.contains("bybit-key"));
        assert!(!s.contains("bybit-secret"));
    }
}
