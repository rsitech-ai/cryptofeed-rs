//! Coinbase International MD credentials — env only.

use std::env;
use std::fmt;

use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialsError {
    pub var: &'static str,
}

impl fmt::Display for CredentialsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "missing or empty env var {}", self.var)
    }
}

impl std::error::Error for CredentialsError {}

#[derive(Clone)]
pub struct CoinbaseIntlCredentials {
    api_key: String,
    api_secret: String,
    passphrase: String,
}

impl CoinbaseIntlCredentials {
    pub fn from_env() -> Result<Self, CredentialsError> {
        Ok(Self {
            api_key: require_env("COINBASE_INTL_API_KEY")?,
            api_secret: require_env("COINBASE_INTL_API_SECRET")?,
            passphrase: require_env("COINBASE_INTL_API_PASSPHRASE")?,
        })
    }

    pub fn fixture() -> Self {
        Self {
            api_key: "fixture-api-key".into(),
            api_secret: base64::engine::general_purpose::STANDARD.encode(b"fixture-secret"),
            passphrase: "fixture-passphrase".into(),
        }
    }

    pub fn sign_subscribe(&self, timestamp_secs: i64) -> SubscribeAuth {
        let ts = timestamp_secs.to_string();
        let prehash = format!("{ts}{}CBINTLMD{}", self.api_key, self.passphrase);
        let key = base64::engine::general_purpose::STANDARD
            .decode(self.api_secret.as_bytes())
            .unwrap_or_else(|_| self.api_secret.as_bytes().to_vec());
        let mut mac = HmacSha256::new_from_slice(&key).expect("HMAC key length");
        mac.update(prehash.as_bytes());
        let signature =
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        SubscribeAuth {
            time: ts,
            key: self.api_key.clone(),
            passphrase: self.passphrase.clone(),
            signature,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SubscribeAuth {
    pub time: String,
    pub key: String,
    pub passphrase: String,
    pub signature: String,
}

impl fmt::Debug for SubscribeAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscribeAuth")
            .field("time", &self.time)
            .field("key", &"<redacted>")
            .field("passphrase", &"<redacted>")
            .field("signature", &"<redacted>")
            .finish()
    }
}

impl fmt::Debug for CoinbaseIntlCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoinbaseIntlCredentials")
            .field("api_key", &"<redacted>")
            .field("api_secret", &"<redacted>")
            .field("passphrase", &"<redacted>")
            .finish()
    }
}

fn require_env(name: &'static str) -> Result<String, CredentialsError> {
    match env::var(name) {
        Ok(v) if !v.is_empty() => Ok(v),
        _ => Err(CredentialsError { var: name }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_matches_known_vector() {
        let secret_b64 = base64::engine::general_purpose::STANDARD.encode(b"secret");
        let c = CoinbaseIntlCredentials {
            api_key: "glK4uG8QRmh3aqnJ".into(), // gitleaks:allow - synthetic signing vector
            api_secret: secret_b64,
            passphrase: "passphrase".into(),
        };
        let auth = c.sign_subscribe(1_683_730_727);
        let mut mac = HmacSha256::new_from_slice(b"secret").unwrap();
        mac.update(b"1683730727glK4uG8QRmh3aqnJCBINTLMDpassphrase");
        let expect = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        assert_eq!(auth.signature, expect);
    }

    #[test]
    fn subscribe_auth_debug_redacts_authentication_material() {
        let auth = SubscribeAuth {
            time: "1683730727".into(),
            key: "sensitive-key".into(),
            passphrase: "sensitive-passphrase".into(),
            signature: "sensitive-signature".into(),
        };

        let debug = format!("{auth:?}");

        assert!(debug.contains("1683730727"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("sensitive-key"));
        assert!(!debug.contains("sensitive-passphrase"));
        assert!(!debug.contains("sensitive-signature"));
    }
}
