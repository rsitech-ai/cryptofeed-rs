//! Coinbase Exchange WebSocket credentials and subscription signing.
//!
//! Contract: <https://docs.cdp.coinbase.com/exchange/websocket-feed/authentication>

use std::{env, fmt};

use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

const VERIFY_PATH: &str = "/users/self/verify";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoinbaseExchangeCredentialsError {
    MissingEnvironmentVariable(&'static str),
    EmptyField(&'static str),
    InvalidBase64Secret,
    InvalidHmacKey,
}

impl fmt::Display for CoinbaseExchangeCredentialsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEnvironmentVariable(name) => {
                write!(f, "missing or empty environment variable {name}")
            }
            Self::EmptyField(field) => write!(f, "{field} must be non-empty"),
            Self::InvalidBase64Secret => write!(f, "API secret must be valid base64"),
            Self::InvalidHmacKey => write!(f, "API secret cannot be used as an HMAC key"),
        }
    }
}

impl std::error::Error for CoinbaseExchangeCredentialsError {}

#[derive(Clone)]
pub struct CoinbaseExchangeCredentials {
    api_key: String,
    api_secret: Vec<u8>,
    passphrase: String,
}

impl CoinbaseExchangeCredentials {
    pub fn new(
        api_key: impl Into<String>,
        api_secret_base64: impl AsRef<str>,
        passphrase: impl Into<String>,
    ) -> Result<Self, CoinbaseExchangeCredentialsError> {
        let api_key = api_key.into();
        let passphrase = passphrase.into();
        let api_secret_base64 = api_secret_base64.as_ref();
        if api_key.trim().is_empty() {
            return Err(CoinbaseExchangeCredentialsError::EmptyField("api key"));
        }
        if api_secret_base64.trim().is_empty() {
            return Err(CoinbaseExchangeCredentialsError::EmptyField("api secret"));
        }
        if passphrase.trim().is_empty() {
            return Err(CoinbaseExchangeCredentialsError::EmptyField("passphrase"));
        }
        let api_secret = base64::engine::general_purpose::STANDARD
            .decode(api_secret_base64.as_bytes())
            .map_err(|_| CoinbaseExchangeCredentialsError::InvalidBase64Secret)?;
        Ok(Self {
            api_key,
            api_secret,
            passphrase,
        })
    }

    pub fn from_env() -> Result<Self, CoinbaseExchangeCredentialsError> {
        let api_key = require_env("COINBASE_EXCHANGE_API_KEY")?;
        let api_secret = require_env("COINBASE_EXCHANGE_API_SECRET")?;
        let passphrase = require_env("COINBASE_EXCHANGE_API_PASSPHRASE")?;
        Self::new(api_key, api_secret, passphrase)
    }

    pub fn sign_subscribe(
        &self,
        timestamp_secs: i64,
    ) -> Result<CoinbaseExchangeSubscribeAuth, CoinbaseExchangeCredentialsError> {
        let timestamp = timestamp_secs.to_string();
        let prehash = format!("{timestamp}GET{VERIFY_PATH}");
        let mut mac = HmacSha256::new_from_slice(&self.api_secret)
            .map_err(|_| CoinbaseExchangeCredentialsError::InvalidHmacKey)?;
        mac.update(prehash.as_bytes());
        let signature =
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        Ok(CoinbaseExchangeSubscribeAuth {
            timestamp,
            key: self.api_key.clone(),
            passphrase: self.passphrase.clone(),
            signature,
        })
    }
}

impl fmt::Debug for CoinbaseExchangeCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoinbaseExchangeCredentials")
            .field("api_key", &"<redacted>")
            .field("api_secret", &"<redacted>")
            .field("passphrase", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct CoinbaseExchangeSubscribeAuth {
    pub timestamp: String,
    pub key: String,
    pub passphrase: String,
    pub signature: String,
}

impl fmt::Debug for CoinbaseExchangeSubscribeAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoinbaseExchangeSubscribeAuth")
            .field("timestamp", &self.timestamp)
            .field("key", &"<redacted>")
            .field("passphrase", &"<redacted>")
            .field("signature", &"<redacted>")
            .finish()
    }
}

fn require_env(name: &'static str) -> Result<String, CoinbaseExchangeCredentialsError> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value),
        _ => Err(CoinbaseExchangeCredentialsError::MissingEnvironmentVariable(name)),
    }
}
