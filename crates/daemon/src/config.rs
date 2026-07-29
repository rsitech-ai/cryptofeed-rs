//! Daemon configuration (TOML) and validation.

use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use marketfeed_model::OverflowPolicy;
use serde::Deserialize;
use thiserror::Error;

const KNOWN_ADAPTERS: &[&str] = &[
    "binance",
    "okx",
    "bybit",
    "kraken",
    "deribit",
    "bitstamp",
    "gemini",
    "coinbase",
    "bitfinex",
    "synthetic",
];
const KNOWN_PROFILES: &[&str] = &["portable", "latency"];
const KNOWN_LOG_FORMATS: &[&str] = &["json", "text"];
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonConfig {
    #[serde(default)]
    pub engine: EngineConfig,
    #[serde(default)]
    pub telemetry: TelemetryConfig,
    #[serde(default)]
    pub readiness: ReadinessConfig,
    #[serde(default)]
    pub recording: RecordingConfig,
    /// Optional private user-data sessions (credentials from env only).
    #[serde(default)]
    pub private: PrivateConfig,
    #[serde(default)]
    pub venues: Vec<VenueConfig>,
    /// Optional external event consumers (`memory` / `logging`). Empty → null-drain.
    #[serde(default)]
    pub sinks: Vec<SinkConfig>,
}

/// Private-account sessions. Secrets must never appear in TOML — only enable flags/ids.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PrivateConfig {
    #[serde(default)]
    pub binance_spot: PrivateBinanceSpotConfig,
    #[serde(default)]
    pub okx_spot: PrivateOkxSpotConfig,
    #[serde(default)]
    pub bybit_spot: PrivateBybitSpotConfig,
}

/// Reserved Binance Spot private-stream configuration.
///
/// `enabled = true` is rejected until authenticated WebSocket API subscription
/// support replaces the retired listen-key protocol.
#[derive(Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PrivateBinanceSpotConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Rejected when set — credentials must come from env, never TOML.
    #[serde(default)]
    pub api_key: Option<String>,
    /// Rejected when set — credentials must come from env, never TOML.
    #[serde(default)]
    pub api_secret: Option<String>,
    /// Rejected when set (alias).
    #[serde(default)]
    pub binance_api_key: Option<String>,
    /// Rejected when set (alias).
    #[serde(default)]
    pub binance_api_secret: Option<String>,
}

impl PrivateBinanceSpotConfig {
    /// True when any secret-bearing field was present in TOML (validation must fail).
    pub fn has_toml_secrets(&self) -> bool {
        self.api_key.is_some()
            || self.api_secret.is_some()
            || self.binance_api_key.is_some()
            || self.binance_api_secret.is_some()
    }
}

impl fmt::Debug for PrivateBinanceSpotConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print credential material if someone bypasses validation.
        f.debug_struct("PrivateBinanceSpotConfig")
            .field("enabled", &self.enabled)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field(
                "api_secret",
                &self.api_secret.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "binance_api_key",
                &self.binance_api_key.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "binance_api_secret",
                &self.binance_api_secret.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// OKX Spot private WS. Credentials: `OKX_API_KEY` / `OKX_API_SECRET` /
/// `OKX_API_PASSPHRASE` from process env only.
#[derive(Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PrivateOkxSpotConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub api_secret: Option<String>,
    #[serde(default)]
    pub passphrase: Option<String>,
    #[serde(default)]
    pub okx_api_key: Option<String>,
    #[serde(default)]
    pub okx_api_secret: Option<String>,
    #[serde(default)]
    pub okx_api_passphrase: Option<String>,
}

impl PrivateOkxSpotConfig {
    pub fn has_toml_secrets(&self) -> bool {
        self.api_key.is_some()
            || self.api_secret.is_some()
            || self.passphrase.is_some()
            || self.okx_api_key.is_some()
            || self.okx_api_secret.is_some()
            || self.okx_api_passphrase.is_some()
    }
}

impl fmt::Debug for PrivateOkxSpotConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrivateOkxSpotConfig")
            .field("enabled", &self.enabled)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field(
                "api_secret",
                &self.api_secret.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "passphrase",
                &self.passphrase.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "okx_api_key",
                &self.okx_api_key.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "okx_api_secret",
                &self.okx_api_secret.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "okx_api_passphrase",
                &self.okx_api_passphrase.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Bybit Spot private WS. Credentials: `BYBIT_API_KEY` / `BYBIT_API_SECRET`
/// from process env only.
#[derive(Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PrivateBybitSpotConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub api_secret: Option<String>,
    #[serde(default)]
    pub bybit_api_key: Option<String>,
    #[serde(default)]
    pub bybit_api_secret: Option<String>,
}

impl PrivateBybitSpotConfig {
    pub fn has_toml_secrets(&self) -> bool {
        self.api_key.is_some()
            || self.api_secret.is_some()
            || self.bybit_api_key.is_some()
            || self.bybit_api_secret.is_some()
    }
}

impl fmt::Debug for PrivateBybitSpotConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrivateBybitSpotConfig")
            .field("enabled", &self.enabled)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field(
                "api_secret",
                &self.api_secret.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "bybit_api_key",
                &self.bybit_api_key.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "bybit_api_secret",
                &self.bybit_api_secret.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineConfig {
    #[serde(default = "default_profile")]
    pub runtime_profile: String,
    #[serde(default = "default_shutdown_secs")]
    pub shutdown_deadline_secs: u64,
}

fn default_profile() -> String {
    "portable".into()
}
fn default_shutdown_secs() -> u64 {
    20
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            runtime_profile: default_profile(),
            shutdown_deadline_secs: default_shutdown_secs(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    #[serde(default = "default_log_format")]
    pub log_format: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_bind")]
    pub bind: String,
}

fn default_log_format() -> String {
    "json".into()
}
fn default_log_level() -> String {
    "info".into()
}
fn default_bind() -> String {
    "127.0.0.1:9108".into()
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            log_format: default_log_format(),
            log_level: default_log_level(),
            bind: default_bind(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadinessConfig {
    #[serde(default = "default_true")]
    pub require_running: bool,
    #[serde(default = "default_true")]
    pub require_required_venues: bool,
    #[serde(default)]
    pub min_live_sessions: u32,
    #[serde(default)]
    pub require_recording_healthy: bool,
}

fn default_true() -> bool {
    true
}

impl Default for ReadinessConfig {
    fn default() -> Self {
        Self {
            require_running: true,
            require_required_venues: true,
            min_live_sessions: 0,
            require_recording_healthy: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RecordingConfig {
    #[serde(default)]
    pub raw: RawRecordingConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawRecordingConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_raw_dir")]
    pub directory: String,
    /// Human size (`64MiB`) or integer bytes.
    #[serde(default = "default_segment_size")]
    pub segment_size: String,
    /// Human duration (`15m`) or integer seconds.
    #[serde(default = "default_segment_duration")]
    pub segment_duration: String,
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,
    #[serde(default = "default_overflow")]
    pub overflow: String,
    /// Human size or integer bytes; `0` disables.
    #[serde(default = "default_min_free")]
    pub min_free_space: String,
}

fn default_raw_dir() -> String {
    "./raw".into()
}
fn default_segment_size() -> String {
    "256MiB".into()
}
fn default_segment_duration() -> String {
    "15m".into()
}
fn default_queue_capacity() -> usize {
    8192
}
const MAX_DAEMON_QUEUE_CAPACITY: usize = 1_048_576;
const MAX_DAEMON_SINKS: usize = 64;
/// Process-wide cap for eagerly reserved queue slots. Every sink owns one
/// worker mailbox plus the concrete sink's batch and system queues.
const MAX_DAEMON_EAGER_QUEUE_SLOTS: usize = 1_048_576;
const EAGER_QUEUES_PER_SINK: usize = 3;
const EAGER_QUEUES_FOR_RECORDING: usize = 2;
fn default_overflow() -> String {
    "fail_engine".into()
}
fn default_min_free() -> String {
    "0".into()
}

impl Default for RawRecordingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            directory: default_raw_dir(),
            segment_size: default_segment_size(),
            segment_duration: default_segment_duration(),
            queue_capacity: default_queue_capacity(),
            overflow: default_overflow(),
            min_free_space: default_min_free(),
        }
    }
}

impl RawRecordingConfig {
    pub fn segment_size_bytes(&self) -> Result<u64, ConfigError> {
        parse_bytes(&self.segment_size)
            .map_err(|e| ConfigError::Validation(format!("recording.raw.segment_size: {e}")))
    }

    pub fn segment_duration(&self) -> Result<Duration, ConfigError> {
        parse_duration(&self.segment_duration)
            .map_err(|e| ConfigError::Validation(format!("recording.raw.segment_duration: {e}")))
    }

    pub fn min_free_bytes(&self) -> Result<u64, ConfigError> {
        parse_bytes(&self.min_free_space)
            .map_err(|e| ConfigError::Validation(format!("recording.raw.min_free_space: {e}")))
    }

    pub fn overflow_policy(&self) -> Result<OverflowPolicy, ConfigError> {
        daemon_overflow_policy("recording.raw.overflow", &self.overflow)
    }
}

/// One `[[sinks]]` entry: bounded `memory` / `logging` / `file` /
/// `protobuf-file` / `protobuf-file-bin` / `udp` / `spill-wal` / `kafka` / `nats`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SinkConfig {
    /// Optional label (diagnostics only).
    #[serde(default)]
    pub id: Option<String>,
    /// When true, readiness fails after this sink worker becomes unhealthy.
    #[serde(default)]
    pub required: bool,
    #[serde(rename = "type")]
    pub sink_type: String,
    /// Required when `type = "file"` / `protobuf-file` / `protobuf-file-bin` / `spill-wal`.
    #[serde(default)]
    pub path: Option<String>,
    /// Required when `type = "udp"` / `kafka` / `nats` — `host:port` (`SocketAddr`).
    #[serde(default)]
    pub address: Option<String>,
    /// Required when `type = "kafka"` — Produce topic name.
    #[serde(default)]
    pub topic: Option<String>,
    /// Required when `type = "nats"` — PUB subject.
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(default = "default_sink_capacity")]
    pub capacity: usize,
    #[serde(default = "default_overflow")]
    pub overflow: String,
    /// Required when `type = "spill-wal"` — hard WAL byte cap (e.g. `"64MiB"`).
    #[serde(default)]
    pub wal_limit: Option<String>,
}

fn default_sink_capacity() -> usize {
    1024
}

impl SinkConfig {
    pub fn overflow_policy(&self) -> Result<OverflowPolicy, ConfigError> {
        let label = self
            .id
            .as_deref()
            .map(|id| format!("sinks[{id}]"))
            .unwrap_or_else(|| format!("sinks[type={}]", self.sink_type));
        daemon_overflow_policy(&format!("{label}.overflow"), &self.overflow)
    }

    pub fn kind(&self) -> Result<SinkKind, ConfigError> {
        match self.sink_type.trim().to_ascii_lowercase().as_str() {
            "memory" => Ok(SinkKind::Memory),
            "logging" => Ok(SinkKind::Logging),
            "file" => Ok(SinkKind::File),
            "protobuf-file" => Ok(SinkKind::ProtobufFile),
            "protobuf-file-bin" => Ok(SinkKind::ProtobufFileBin),
            "udp" => Ok(SinkKind::Udp),
            "kafka" => Ok(SinkKind::Kafka),
            "nats" => Ok(SinkKind::Nats),
            "spill-wal" | "spill_wal" => Ok(SinkKind::SpillWal),
            other => Err(ConfigError::Validation(format!(
                "unknown sink type {other:?} (memory|logging|file|protobuf-file|protobuf-file-bin|udp|spill-wal|kafka|nats)"
            ))),
        }
    }

    /// Append path for `type = "file"` / `protobuf-file` / `protobuf-file-bin` / `spill-wal`.
    pub fn file_path(&self) -> Result<&str, ConfigError> {
        let label = self
            .id
            .as_deref()
            .map(|id| format!("sinks[{id}]"))
            .unwrap_or_else(|| format!("sinks[type={}]", self.sink_type));
        match self
            .path
            .as_deref()
            .map(str::trim)
            .filter(|p| !p.is_empty())
        {
            Some(p) => Ok(p),
            None => Err(ConfigError::Validation(format!(
                "{label}.path required for type=file|protobuf-file|protobuf-file-bin|spill-wal"
            ))),
        }
    }

    /// Destination for `type = "udp"` / `kafka` / `nats`.
    /// WAL byte limit for `type = "spill-wal"`.
    pub fn wal_limit_bytes(&self) -> Result<u64, ConfigError> {
        let label = self
            .id
            .as_deref()
            .map(|id| format!("sinks[{id}]"))
            .unwrap_or_else(|| format!("sinks[type={}]", self.sink_type));
        let raw = self
            .wal_limit
            .as_deref()
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .ok_or_else(|| {
                ConfigError::Validation(format!("{label}.wal_limit required for type=spill-wal"))
            })?;
        let bytes = parse_bytes(raw)
            .map_err(|e| ConfigError::Validation(format!("{label}.wal_limit: {e}")))?;
        if bytes == 0 {
            return Err(ConfigError::Validation(format!(
                "{label}.wal_limit must be > 0"
            )));
        }
        Ok(bytes)
    }

    pub fn socket_address(&self) -> Result<std::net::SocketAddr, ConfigError> {
        let label = self
            .id
            .as_deref()
            .map(|id| format!("sinks[{id}]"))
            .unwrap_or_else(|| format!("sinks[type={}]", self.sink_type));
        let raw = self
            .address
            .as_deref()
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .ok_or_else(|| {
                ConfigError::Validation(format!("{label}.address required for type=udp|kafka|nats"))
            })?;
        raw.parse().map_err(|e| {
            ConfigError::Validation(format!("{label}.address invalid SocketAddr {raw:?}: {e}"))
        })
    }

    /// Destination for `type = "udp"` (alias of [`Self::socket_address`]).
    pub fn udp_address(&self) -> Result<std::net::SocketAddr, ConfigError> {
        self.socket_address()
    }

    /// Kafka Produce topic (`type = "kafka"`).
    pub fn kafka_topic(&self) -> Result<&str, ConfigError> {
        let label = self
            .id
            .as_deref()
            .map(|id| format!("sinks[{id}]"))
            .unwrap_or_else(|| format!("sinks[type={}]", self.sink_type));
        match self
            .topic
            .as_deref()
            .map(str::trim)
            .filter(|p| !p.is_empty())
        {
            Some(t) => Ok(t),
            None => Err(ConfigError::Validation(format!(
                "{label}.topic required for type=kafka"
            ))),
        }
    }

    /// NATS PUB subject (`type = "nats"`).
    pub fn nats_subject(&self) -> Result<&str, ConfigError> {
        let label = self
            .id
            .as_deref()
            .map(|id| format!("sinks[{id}]"))
            .unwrap_or_else(|| format!("sinks[type={}]", self.sink_type));
        match self
            .subject
            .as_deref()
            .map(str::trim)
            .filter(|p| !p.is_empty())
        {
            Some(s) => Ok(s),
            None => Err(ConfigError::Validation(format!(
                "{label}.subject required for type=nats"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkKind {
    Memory,
    Logging,
    File,
    ProtobufFile,
    ProtobufFileBin,
    Udp,
    Kafka,
    Nats,
    SpillWal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransportMode {
    Live,
    Memory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueKind {
    Synthetic,
    BinanceSpot,
    BinanceUsdm,
    BinanceCoinm,
    OkxSpot,
    OkxSwap,
    OkxFutures,
    BybitLinear,
    BybitSpot,
    BybitInverse,
    KrakenSpot,
    KrakenFutures,
    Deribit,
    Bitstamp,
    Gemini,
    CoinbaseSpot,
    CoinbaseAdvanced,
    CoinbaseIntl,
    Bitfinex,
    BitfinexDeriv,
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VenueConfig {
    pub id: String,
    pub adapter: String,
    #[serde(default)]
    pub segment: Option<String>,
    #[serde(default = "default_true")]
    pub required: bool,
    #[serde(default)]
    pub transport: Option<TransportMode>,
    #[serde(default)]
    pub symbols: Vec<String>,
    #[serde(default)]
    pub channels: Vec<String>,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub api_secret: Option<String>,
    #[serde(default)]
    pub passphrase: Option<String>,
}

impl fmt::Debug for VenueConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VenueConfig")
            .field("id", &self.id)
            .field("adapter", &self.adapter)
            .field("segment", &self.segment)
            .field("required", &self.required)
            .field("transport", &self.transport)
            .field("symbols", &self.symbols)
            .field("channels", &self.channels)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field(
                "api_secret",
                &self.api_secret.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "passphrase",
                &self.passphrase.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl VenueConfig {
    pub fn has_toml_secrets(&self) -> bool {
        self.api_key.is_some() || self.api_secret.is_some() || self.passphrase.is_some()
    }
    pub fn resolved_transport(&self) -> TransportMode {
        self.transport.unwrap_or(match self.adapter.as_str() {
            "synthetic" => TransportMode::Memory,
            _ => TransportMode::Live,
        })
    }

    pub fn resolved_kind(&self) -> Result<VenueKind, ConfigError> {
        match self.adapter.as_str() {
            "synthetic" => Ok(VenueKind::Synthetic),
            "binance" => {
                let seg = self
                    .segment
                    .as_deref()
                    .unwrap_or(if self.id.contains("usdm") {
                        "usdm"
                    } else if self.id.contains("coinm") {
                        "coinm"
                    } else {
                        "spot"
                    });
                match seg {
                    "spot" => Ok(VenueKind::BinanceSpot),
                    "usdm" | "linear" => Ok(VenueKind::BinanceUsdm),
                    "coinm" | "inverse" | "dapi" => Ok(VenueKind::BinanceCoinm),
                    other => Err(ConfigError::Validation(format!(
                        "venue {}: unknown binance segment {other:?} (spot|usdm|coinm)",
                        self.id
                    ))),
                }
            }
            "okx" => {
                let seg = self.segment.as_deref().unwrap_or("spot");
                match seg {
                    "spot" => Ok(VenueKind::OkxSpot),
                    "swap" | "linear" | "perp" => Ok(VenueKind::OkxSwap),
                    "futures" | "future" => Ok(VenueKind::OkxFutures),
                    other => Err(ConfigError::Validation(format!(
                        "venue {}: unknown okx segment {other:?} (spot|swap|futures)",
                        self.id
                    ))),
                }
            }
            "bybit" => {
                let seg = self
                    .segment
                    .as_deref()
                    .unwrap_or(if self.id.contains("spot") {
                        "spot"
                    } else if self.id.contains("inverse") {
                        "inverse"
                    } else {
                        "linear"
                    });
                match seg {
                    "linear" | "usdm" | "perp" => Ok(VenueKind::BybitLinear),
                    "spot" => Ok(VenueKind::BybitSpot),
                    "inverse" | "coinm" => Ok(VenueKind::BybitInverse),
                    other => Err(ConfigError::Validation(format!(
                        "venue {}: unknown bybit segment {other:?} (linear|spot|inverse)",
                        self.id
                    ))),
                }
            }
            "kraken" => {
                let seg = self.segment.as_deref().unwrap_or("spot");
                match seg {
                    "spot" => Ok(VenueKind::KrakenSpot),
                    "futures" | "future" | "derivatives" => Ok(VenueKind::KrakenFutures),
                    other => Err(ConfigError::Validation(format!(
                        "venue {}: unknown kraken segment {other:?} (spot|futures)",
                        self.id
                    ))),
                }
            }
            "deribit" => Ok(VenueKind::Deribit),
            "bitstamp" => Ok(VenueKind::Bitstamp),
            "gemini" => Ok(VenueKind::Gemini),
            "coinbase" => {
                let seg = self
                    .segment
                    .as_deref()
                    .unwrap_or(if self.id.contains("intl") {
                        "intl"
                    } else if self.id.contains("adv") {
                        "advanced"
                    } else {
                        "spot"
                    });
                match seg {
                    "spot" | "exchange" => Ok(VenueKind::CoinbaseSpot),
                    "advanced" | "adv" => Ok(VenueKind::CoinbaseAdvanced),
                    "intl" | "international" | "intx" => Ok(VenueKind::CoinbaseIntl),
                    other => Err(ConfigError::Validation(format!(
                        "venue {}: unknown coinbase segment {other:?} (spot|advanced|intl)",
                        self.id
                    ))),
                }
            }
            "bitfinex" => {
                let seg = self
                    .segment
                    .as_deref()
                    .unwrap_or(if self.id.contains("deriv") {
                        "deriv"
                    } else {
                        "spot"
                    });
                match seg {
                    "spot" | "exchange" => Ok(VenueKind::Bitfinex),
                    "deriv" | "derivatives" | "futures" | "perp" => Ok(VenueKind::BitfinexDeriv),
                    other => Err(ConfigError::Validation(format!(
                        "venue {}: unknown bitfinex segment {other:?}",
                        self.id
                    ))),
                }
            }
            other => Err(ConfigError::Validation(format!(
                "unknown adapter {other:?} for venue {}",
                self.id
            ))),
        }
    }

    pub fn wants_l2(&self) -> bool {
        self.channels.iter().any(|c| {
            let c = c.to_ascii_lowercase();
            c == "l2" || c == "l2_book" || c == "book"
        })
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("read config: {0}")]
    Io(String),
    #[error("parse TOML: {0}")]
    Parse(String),
    #[error("validation: {0}")]
    Validation(String),
}

impl DaemonConfig {
    pub fn from_toml_str(s: &str) -> Result<Self, ConfigError> {
        let cfg: Self = toml::from_str(s).map_err(|e| ConfigError::Parse(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn load_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let text =
            std::fs::read_to_string(path.as_ref()).map_err(|e| ConfigError::Io(e.to_string()))?;
        Self::from_toml_str(&text)
    }

    pub fn bind_addr(&self) -> Result<SocketAddr, ConfigError> {
        SocketAddr::from_str(&self.telemetry.bind)
            .map_err(|e| ConfigError::Validation(format!("telemetry.bind: {e}")))
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if !KNOWN_PROFILES.contains(&self.engine.runtime_profile.as_str()) {
            return Err(ConfigError::Validation(format!(
                "unsupported runtime_profile {:?}",
                self.engine.runtime_profile
            )));
        }
        if self.engine.shutdown_deadline_secs == 0 {
            return Err(ConfigError::Validation(
                "engine.shutdown_deadline_secs must be > 0".into(),
            ));
        }
        if !KNOWN_LOG_FORMATS.contains(&self.telemetry.log_format.as_str()) {
            return Err(ConfigError::Validation(format!(
                "telemetry.log_format must be one of {KNOWN_LOG_FORMATS:?}"
            )));
        }
        if self.telemetry.log_level.trim().is_empty() {
            return Err(ConfigError::Validation(
                "telemetry.log_level must be non-empty".into(),
            ));
        }
        let addr = self.bind_addr()?;
        if !addr.ip().is_loopback() {
            return Err(ConfigError::Validation(
                "telemetry.bind must be loopback (insecure remote binding rejected)".into(),
            ));
        }
        if self.recording.raw.enabled {
            if self.recording.raw.directory.trim().is_empty() {
                return Err(ConfigError::Validation(
                    "recording.raw.directory required when enabled".into(),
                ));
            }
            let _ = self.recording.raw.segment_size_bytes()?;
            let _ = self.recording.raw.segment_duration()?;
            let _ = self.recording.raw.min_free_bytes()?;
            let _ = self.recording.raw.overflow_policy()?;
            if self.recording.raw.queue_capacity == 0 {
                return Err(ConfigError::Validation(
                    "recording.raw.queue_capacity must be > 0".into(),
                ));
            }
            if self.recording.raw.queue_capacity > MAX_DAEMON_QUEUE_CAPACITY {
                return Err(ConfigError::Validation(format!(
                    "recording.raw.queue_capacity must be <= {MAX_DAEMON_QUEUE_CAPACITY}"
                )));
            }
        }
        if self.sinks.len() > MAX_DAEMON_SINKS {
            return Err(ConfigError::Validation(format!(
                "sinks must contain at most {MAX_DAEMON_SINKS} entries"
            )));
        }
        let mut eager_queue_slots = if self.recording.raw.enabled {
            self.recording
                .raw
                .queue_capacity
                .checked_mul(EAGER_QUEUES_FOR_RECORDING)
                .ok_or_else(|| {
                    ConfigError::Validation(
                        "recording.raw.queue_capacity overflows eager queue slot accounting".into(),
                    )
                })?
        } else {
            0
        };
        if eager_queue_slots > MAX_DAEMON_EAGER_QUEUE_SLOTS {
            return Err(ConfigError::Validation(format!(
                "recording and sinks reserve {eager_queue_slots} eager queue slots; maximum is {MAX_DAEMON_EAGER_QUEUE_SLOTS}"
            )));
        }
        let mut sink_ids = HashSet::with_capacity(self.sinks.len());
        for (i, sink) in self.sinks.iter().enumerate() {
            if let Some(id) = sink.id.as_deref() {
                if id.trim().is_empty() {
                    return Err(ConfigError::Validation(format!(
                        "sinks[{i}].id must be non-empty when provided"
                    )));
                }
                if id.trim() != id {
                    return Err(ConfigError::Validation(format!(
                        "sinks[{i}].id must not contain leading or trailing whitespace: {id:?}"
                    )));
                }
                if id.contains('\0') {
                    return Err(ConfigError::Validation(format!(
                        "sinks[{i}].id must not contain NUL"
                    )));
                }
                if !sink_ids.insert(id) {
                    return Err(ConfigError::Validation(format!("duplicate sink id {id:?}")));
                }
            }
            if sink.required && sink.id.is_none() {
                return Err(ConfigError::Validation(format!(
                    "sinks[{i}] required sink must have an explicit id"
                )));
            }
            let kind = sink.kind()?;
            let policy = sink.overflow_policy()?;
            if sink.required && policy != OverflowPolicy::FailEngine {
                return Err(ConfigError::Validation(format!(
                    "sinks[{i}] required sink must use overflow=fail_engine (got {policy:?})"
                )));
            }
            if sink.capacity == 0 {
                return Err(ConfigError::Validation(format!(
                    "sinks[{i}].capacity must be > 0"
                )));
            }
            if sink.capacity > MAX_DAEMON_QUEUE_CAPACITY {
                return Err(ConfigError::Validation(format!(
                    "sinks[{i}].capacity must be <= {MAX_DAEMON_QUEUE_CAPACITY}"
                )));
            }
            let sink_slots = sink
                .capacity
                .checked_mul(EAGER_QUEUES_PER_SINK)
                .ok_or_else(|| {
                    ConfigError::Validation(format!(
                        "sinks[{i}].capacity overflows eager queue slot accounting"
                    ))
                })?;
            eager_queue_slots = eager_queue_slots.checked_add(sink_slots).ok_or_else(|| {
                ConfigError::Validation("aggregate eager queue slot accounting overflowed".into())
            })?;
            if eager_queue_slots > MAX_DAEMON_EAGER_QUEUE_SLOTS {
                return Err(ConfigError::Validation(format!(
                    "recording and sinks reserve {eager_queue_slots} eager queue slots; maximum is {MAX_DAEMON_EAGER_QUEUE_SLOTS}"
                )));
            }
            if kind == SinkKind::File
                || kind == SinkKind::ProtobufFile
                || kind == SinkKind::ProtobufFileBin
                || kind == SinkKind::SpillWal
            {
                let _ = sink.file_path()?;
            }
            if kind == SinkKind::SpillWal {
                let _ = sink.wal_limit_bytes()?;
                let policy = sink.overflow_policy()?;
                if policy != OverflowPolicy::SpillToDisk {
                    return Err(ConfigError::Validation(format!(
                        "sinks[{i}].overflow must be spill_to_disk for type=spill-wal (got {policy:?})"
                    )));
                }
                return Err(ConfigError::Validation(format!(
                    "sinks[{i}] type=spill-wal is not a standalone delivery sink: its in-memory prefix has no recovery consumer; attach WAL recovery to a real sink before enabling it in the daemon"
                )));
            }
            if kind == SinkKind::Udp {
                let _ = sink.udp_address()?;
            }
            if kind == SinkKind::Kafka {
                let _ = sink.socket_address()?;
                let _ = sink.kafka_topic()?;
            }
            if kind == SinkKind::Nats {
                let _ = sink.socket_address()?;
                let _ = sink.nats_subject()?;
            }
        }
        let mut venue_ids = HashSet::with_capacity(self.venues.len());
        for (venue_index, v) in self.venues.iter().enumerate() {
            if v.id.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "venues[{venue_index}].id must be non-empty (got {:?})",
                    v.id
                )));
            }
            if v.id.trim() != v.id {
                return Err(ConfigError::Validation(format!(
                    "venues[{venue_index}].id must not contain leading or trailing whitespace: {:?}",
                    v.id
                )));
            }
            if !venue_ids.insert(v.id.as_str()) {
                return Err(ConfigError::Validation(format!(
                    "duplicate venue id {:?} at venues[{venue_index}]",
                    v.id
                )));
            }
            if !KNOWN_ADAPTERS.contains(&v.adapter.as_str()) {
                return Err(ConfigError::Validation(format!(
                    "unknown adapter {:?} for venue {}",
                    v.adapter, v.id
                )));
            }
            let kind = v.resolved_kind()?;
            if kind == VenueKind::CoinbaseSpot && v.has_toml_secrets() {
                return Err(ConfigError::Validation(format!(
                    "venue {}: coinbase-spot secrets must not appear in TOML \
                     (use COINBASE_EXCHANGE_API_KEY / COINBASE_EXCHANGE_API_SECRET / \
                     COINBASE_EXCHANGE_API_PASSPHRASE env vars)",
                    v.id
                )));
            }
            if kind == VenueKind::CoinbaseIntl && v.has_toml_secrets() {
                return Err(ConfigError::Validation(format!(
                    "venue {}: coinbase-intl secrets must not appear in TOML \
                     (use COINBASE_INTL_API_KEY / COINBASE_INTL_API_SECRET / \
                     COINBASE_INTL_API_PASSPHRASE env vars)",
                    v.id
                )));
            }
            let transport = v.resolved_transport();
            match (kind, transport) {
                (VenueKind::Synthetic, TransportMode::Live) => {
                    return Err(ConfigError::Validation(format!(
                        "venue {}: synthetic requires transport=memory",
                        v.id
                    )));
                }
                (
                    VenueKind::BinanceSpot
                    | VenueKind::BinanceUsdm
                    | VenueKind::BinanceCoinm
                    | VenueKind::OkxSpot
                    | VenueKind::OkxSwap
                    | VenueKind::OkxFutures
                    | VenueKind::BybitLinear
                    | VenueKind::BybitSpot
                    | VenueKind::BybitInverse
                    | VenueKind::KrakenSpot
                    | VenueKind::KrakenFutures
                    | VenueKind::Deribit
                    | VenueKind::Bitstamp
                    | VenueKind::Gemini
                    | VenueKind::CoinbaseSpot
                    | VenueKind::CoinbaseAdvanced
                    | VenueKind::CoinbaseIntl
                    | VenueKind::Bitfinex
                    | VenueKind::BitfinexDeriv,
                    TransportMode::Memory,
                ) => {
                    return Err(ConfigError::Validation(format!(
                        "venue {}: adapter {:?} requires transport=live",
                        v.id, v.adapter
                    )));
                }
                _ => {}
            }
            if kind != VenueKind::Synthetic {
                if v.symbols.is_empty() {
                    return Err(ConfigError::Validation(format!(
                        "venue {}: symbols must be non-empty",
                        v.id
                    )));
                }
                if v.channels.is_empty() {
                    return Err(ConfigError::Validation(format!(
                        "venue {}: channels must be non-empty",
                        v.id
                    )));
                }
            }
            let mut symbols = HashSet::with_capacity(v.symbols.len());
            for symbol in &v.symbols {
                if symbol.trim().is_empty() {
                    return Err(ConfigError::Validation(format!(
                        "venue {}: symbols contains blank value {:?}",
                        v.id, symbol
                    )));
                }
                if symbol.trim() != symbol {
                    return Err(ConfigError::Validation(format!(
                        "venue {}: symbol must not contain leading or trailing whitespace: {:?}",
                        v.id, symbol
                    )));
                }
                if !symbols.insert(symbol.as_str()) {
                    return Err(ConfigError::Validation(format!(
                        "venue {}: duplicate symbol {:?}",
                        v.id, symbol
                    )));
                }
            }
            let mut canonical_channels = HashSet::with_capacity(v.channels.len());
            for channel in &v.channels {
                if channel.trim().is_empty() {
                    return Err(ConfigError::Validation(format!(
                        "venue {}: channels contains blank value {:?}",
                        v.id, channel
                    )));
                }
                if channel.trim() != channel {
                    return Err(ConfigError::Validation(format!(
                        "venue {}: channel must not contain leading or trailing whitespace: {:?}",
                        v.id, channel
                    )));
                }
                let canonical = canonical_channel_name(channel).ok_or_else(|| {
                    ConfigError::Validation(format!("venue {}: unknown channel {channel:?}", v.id))
                })?;
                if !canonical_channels.insert(canonical) {
                    return Err(ConfigError::Validation(format!(
                        "venue {}: duplicate channel {:?} (canonical {:?})",
                        v.id, channel, canonical
                    )));
                }
            }
        }
        if self.readiness.min_live_sessions > 0 && self.venues.is_empty() {
            return Err(ConfigError::Validation(
                "impossible readiness: min_live_sessions > 0 but no venues".into(),
            ));
        }
        if self.readiness.require_recording_healthy && !self.recording.raw.enabled {
            return Err(ConfigError::Validation(
                "impossible readiness: require_recording_healthy but recording.raw disabled".into(),
            ));
        }
        // Private sessions: enable flags only — credentials must come from env.
        if self.private.binance_spot.has_toml_secrets() {
            return Err(ConfigError::Validation(
                "private.binance_spot: secrets must not appear in TOML \
                 (use BINANCE_API_KEY / BINANCE_API_SECRET env vars)"
                    .into(),
            ));
        }
        if self.private.binance_spot.enabled {
            return Err(ConfigError::Validation(
                "private.binance_spot: unavailable because Binance retired the listen-key \
                 REST/WebSocket flow on 2026-02-20; migrate this integration to the WebSocket \
                 API userDataStream.subscribe flow before enabling it"
                    .into(),
            ));
        }
        if self.private.okx_spot.has_toml_secrets() {
            return Err(ConfigError::Validation(
                "private.okx_spot: secrets must not appear in TOML \
                 (use OKX_API_KEY / OKX_API_SECRET / OKX_API_PASSPHRASE env vars)"
                    .into(),
            ));
        }
        if self.private.bybit_spot.has_toml_secrets() {
            return Err(ConfigError::Validation(
                "private.bybit_spot: secrets must not appear in TOML \
                 (use BYBIT_API_KEY / BYBIT_API_SECRET env vars)"
                    .into(),
            ));
        }
        if self.private.okx_spot.enabled || self.private.bybit_spot.enabled {
            return Err(ConfigError::Validation(
                "private OKX/Bybit daemon sessions are unavailable until a bounded durable \
                 account-event sink, private readiness/liveness tracking, and reconnect \
                 supervision are implemented; use marketfeed-private with an explicit \
                 AccountEventSink instead"
                    .into(),
            ));
        }
        Ok(())
    }
}

fn canonical_channel_name(name: &str) -> Option<&'static str> {
    match name.to_ascii_lowercase().as_str() {
        "trades" => Some("trades"),
        "quote" | "ticker" => Some("quote"),
        "l2" | "l2_book" | "book" => Some("l2"),
        "funding" => Some("funding"),
        "open_interest" => Some("open_interest"),
        "liquidations" => Some("liquidations"),
        "mark" | "mark_price" => Some("mark"),
        "index" => Some("index"),
        "candles" => Some("candles"),
        _ => None,
    }
}

/// Parse `64MiB`, `1GiB`, `1024`, `1_000_000`.
pub fn parse_bytes(s: &str) -> Result<u64, String> {
    let s = s.trim().replace('_', "");
    if s.is_empty() {
        return Err("empty".into());
    }
    let lower = s.to_ascii_lowercase();
    let (num, mult) = if let Some(n) = lower.strip_suffix("gib") {
        (n, 1024u64 * 1024 * 1024)
    } else if let Some(n) = lower.strip_suffix("gb") {
        (n, 1000u64 * 1000 * 1000)
    } else if let Some(n) = lower.strip_suffix("mib") {
        (n, 1024u64 * 1024)
    } else if let Some(n) = lower.strip_suffix("mb") {
        (n, 1000u64 * 1000)
    } else if let Some(n) = lower.strip_suffix("kib") {
        (n, 1024u64)
    } else if let Some(n) = lower.strip_suffix("kb") {
        (n, 1000u64)
    } else if let Some(n) = lower.strip_suffix('b') {
        (n, 1u64)
    } else {
        (lower.as_str(), 1u64)
    };
    let n: u64 = num
        .trim()
        .parse()
        .map_err(|_| format!("invalid byte size {s:?}"))?;
    Ok(n.saturating_mul(mult))
}

/// Parse `15m`, `1h`, `30s`, or integer seconds.
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty".into());
    }
    let lower = s.to_ascii_lowercase();
    let (num, secs_mult) = if let Some(n) = lower.strip_suffix('h') {
        (n, 3600u64)
    } else if let Some(n) = lower.strip_suffix('m') {
        (n, 60u64)
    } else if let Some(n) = lower.strip_suffix('s') {
        (n, 1u64)
    } else {
        (lower.as_str(), 1u64)
    };
    let n: u64 = num
        .trim()
        .parse()
        .map_err(|_| format!("invalid duration {s:?}"))?;
    Ok(Duration::from_secs(n.saturating_mul(secs_mult)))
}

pub fn parse_overflow(s: &str) -> Result<OverflowPolicy, String> {
    match s.trim().to_ascii_lowercase().as_str() {
        "fail_engine" | "fail" => Ok(OverflowPolicy::FailEngine),
        "drop_newest" => Ok(OverflowPolicy::DropNewest),
        "drop_oldest" => Ok(OverflowPolicy::DropOldest),
        "block_with_deadline" | "block" => Ok(OverflowPolicy::BlockWithDeadline),
        "spill_to_disk" | "spill" => Ok(OverflowPolicy::SpillToDisk),
        other => Err(format!("unknown overflow {other:?}")),
    }
}

fn daemon_overflow_policy(label: &str, value: &str) -> Result<OverflowPolicy, ConfigError> {
    let policy = parse_overflow(value)
        .map_err(|error| ConfigError::Validation(format!("{label}: {error}")))?;
    if policy == OverflowPolicy::BlockWithDeadline {
        return Err(ConfigError::Validation(format!(
            "{label}: block_with_deadline is unsupported by daemon-owned queues"
        )));
    }
    Ok(policy)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_block_with_deadline_for_daemon_owned_queues() {
        let sink = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[sinks]]
            type = "memory"
            capacity = 1
            overflow = "block_with_deadline"
            [[venues]]
            id = "synthetic-demo"
            adapter = "synthetic"
            "#,
        )
        .expect_err("daemon sink has no safe blocking owner");
        assert!(sink.to_string().contains("block_with_deadline"));

        let recording = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [recording.raw]
            enabled = true
            directory = "./raw"
            segment_size = "1MiB"
            segment_duration = "1m"
            queue_capacity = 1
            overflow = "block"
            min_free_space = "1MiB"
            [[venues]]
            id = "synthetic-demo"
            adapter = "synthetic"
            "#,
        )
        .expect_err("daemon recorder has no safe blocking owner");
        assert!(recording.to_string().contains("block_with_deadline"));
    }

    #[test]
    fn accepts_memory_logging_and_file_sinks() {
        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[sinks]]
            id = "probe"
            type = "memory"
            capacity = 32
            overflow = "drop_oldest"
            [[sinks]]
            type = "logging"
            capacity = 8
            overflow = "drop_newest"
            [[sinks]]
            type = "file"
            path = "./events.log"
            capacity = 16
            overflow = "fail_engine"
            [[sinks]]
            type = "protobuf-file"
            path = "./events.mfpe"
            capacity = 16
            overflow = "fail_engine"
            [[sinks]]
            type = "protobuf-file-bin"
            path = "./events.mfpeb"
            capacity = 16
            overflow = "fail_engine"
            [[sinks]]
            type = "udp"
            address = "127.0.0.1:19090"
            capacity = 16
            overflow = "drop_newest"
            [[sinks]]
            type = "kafka"
            address = "127.0.0.1:9092"
            topic = "marketfeed"
            capacity = 16
            overflow = "drop_newest"
            [[sinks]]
            type = "nats"
            address = "127.0.0.1:4222"
            subject = "marketfeed.events"
            capacity = 16
            overflow = "drop_newest"
            [[venues]]
            id = "synthetic-demo"
            adapter = "synthetic"
        "#,
        )
        .unwrap();
        assert_eq!(cfg.sinks.len(), 8);
        assert_eq!(cfg.sinks[0].kind().unwrap(), SinkKind::Memory);
        assert_eq!(cfg.sinks[0].capacity, 32);
        assert_eq!(
            cfg.sinks[0].overflow_policy().unwrap(),
            OverflowPolicy::DropOldest
        );
        assert_eq!(cfg.sinks[1].kind().unwrap(), SinkKind::Logging);
        assert_eq!(cfg.sinks[2].kind().unwrap(), SinkKind::File);
        assert_eq!(cfg.sinks[2].file_path().unwrap(), "./events.log");
        assert_eq!(cfg.sinks[3].kind().unwrap(), SinkKind::ProtobufFile);
        assert_eq!(cfg.sinks[3].file_path().unwrap(), "./events.mfpe");
        assert_eq!(cfg.sinks[4].kind().unwrap(), SinkKind::ProtobufFileBin);
        assert_eq!(cfg.sinks[4].file_path().unwrap(), "./events.mfpeb");
        assert_eq!(cfg.sinks[5].kind().unwrap(), SinkKind::Udp);
        assert_eq!(
            cfg.sinks[5].udp_address().unwrap(),
            "127.0.0.1:19090".parse().unwrap()
        );
        assert_eq!(cfg.sinks[6].kind().unwrap(), SinkKind::Kafka);
        assert_eq!(cfg.sinks[6].kafka_topic().unwrap(), "marketfeed");
        assert_eq!(cfg.sinks[7].kind().unwrap(), SinkKind::Nats);
        assert_eq!(cfg.sinks[7].nats_subject().unwrap(), "marketfeed.events");
    }

    #[test]
    fn rejects_empty_or_duplicate_sink_ids() {
        let empty = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[sinks]]
            id = " "
            type = "memory"
            "#,
        )
        .unwrap_err();
        assert!(empty.to_string().contains("id must be non-empty"));

        let duplicate = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[sinks]]
            id = "primary"
            type = "memory"
            [[sinks]]
            id = "primary"
            type = "logging"
            "#,
        )
        .unwrap_err();
        assert!(duplicate.to_string().contains("duplicate sink id"));
    }

    #[test]
    fn sink_required_defaults_false_and_can_be_enabled() {
        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[sinks]]
            id = "primary"
            type = "memory"
            required = true
            "#,
        )
        .unwrap();
        assert!(cfg.sinks[0].required);

        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[sinks]]
            type = "memory"
            "#,
        )
        .unwrap();
        assert!(!cfg.sinks[0].required);
    }

    #[test]
    fn required_sink_needs_stable_id_and_fail_engine_policy() {
        let missing_id = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[sinks]]
            type = "memory"
            required = true
            overflow = "fail_engine"
            "#,
        )
        .unwrap_err();
        assert!(missing_id.to_string().contains("required sink"));
        assert!(missing_id.to_string().contains("explicit id"));

        let dropping = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[sinks]]
            id = "primary"
            type = "memory"
            required = true
            overflow = "drop_newest"
            "#,
        )
        .unwrap_err();
        assert!(dropping.to_string().contains("required sink"));
        assert!(dropping.to_string().contains("fail_engine"));
    }

    #[test]
    fn rejects_udp_sink_without_address() {
        assert!(
            DaemonConfig::from_toml_str(
                r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[sinks]]
            type = "udp"
            [[venues]]
            id = "synthetic-demo"
            adapter = "synthetic"
        "#,
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_standalone_spill_wal_sink() {
        let error = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[sinks]]
            type = "spill-wal"
            path = "./spill.wal"
            wal_limit = "64MiB"
            capacity = 64
            overflow = "spill_to_disk"
            [[venues]]
            id = "synthetic-demo"
            adapter = "synthetic"
        "#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("not a standalone delivery sink"));
        assert!(error.to_string().contains("recovery consumer"));
    }

    #[test]
    fn rejects_spill_wal_without_limit_or_wrong_policy() {
        assert!(
            DaemonConfig::from_toml_str(
                r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[sinks]]
            type = "spill-wal"
            path = "./spill.wal"
            overflow = "spill_to_disk"
            [[venues]]
            id = "synthetic-demo"
            adapter = "synthetic"
        "#,
            )
            .is_err()
        );
        assert!(
            DaemonConfig::from_toml_str(
                r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[sinks]]
            type = "spill-wal"
            path = "./spill.wal"
            wal_limit = "1MiB"
            overflow = "fail_engine"
            [[venues]]
            id = "synthetic-demo"
            adapter = "synthetic"
        "#,
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_file_sink_without_path() {
        assert!(
            DaemonConfig::from_toml_str(
                r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[sinks]]
            type = "file"
            [[venues]]
            id = "synthetic-demo"
            adapter = "synthetic"
        "#,
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_unknown_sink_type_and_zero_capacity() {
        assert!(
            DaemonConfig::from_toml_str(
                r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[sinks]]
            type = "mystery-bus"
            [[venues]]
            id = "synthetic-demo"
            adapter = "synthetic"
        "#,
            )
            .is_err()
        );
        assert!(
            DaemonConfig::from_toml_str(
                r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[sinks]]
            type = "memory"
            capacity = 0
            [[venues]]
            id = "synthetic-demo"
            adapter = "synthetic"
        "#,
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_unbounded_queue_capacities_and_nul_sink_ids() {
        for (name, body, expected) in [
            (
                "recording queue",
                r#"
                [telemetry]
                bind = "127.0.0.1:9108"
                [recording.raw]
                enabled = true
                queue_capacity = 1048577
                "#,
                "recording.raw.queue_capacity must be <=",
            ),
            (
                "sink queue",
                r#"
                [telemetry]
                bind = "127.0.0.1:9108"
                [[sinks]]
                type = "memory"
                capacity = 1048577
                "#,
                "sinks[0].capacity must be <=",
            ),
            (
                "sink thread name",
                r#"
                [telemetry]
                bind = "127.0.0.1:9108"
                [[sinks]]
                id = "bad\u0000id"
                type = "memory"
                "#,
                "must not contain NUL",
            ),
        ] {
            let error = DaemonConfig::from_toml_str(body)
                .expect_err("unbounded allocations and invalid thread names must be rejected");
            assert!(error.to_string().contains(expected), "{name}: {error}");
        }
    }

    #[test]
    fn rejects_excessive_sink_count_and_aggregate_queue_reservations() {
        let mut too_many_sinks = "[telemetry]\nbind = \"127.0.0.1:9108\"\n".to_owned();
        for _ in 0..=MAX_DAEMON_SINKS {
            too_many_sinks.push_str("[[sinks]]\ntype = \"memory\"\ncapacity = 1\n");
        }
        let error = DaemonConfig::from_toml_str(&too_many_sinks)
            .expect_err("sink worker count must be bounded");
        assert!(error.to_string().contains("at most 64 entries"), "{error}");

        let error = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[sinks]]
            type = "memory"
            capacity = 200000
            [[sinks]]
            type = "memory"
            capacity = 200000
            "#,
        )
        .expect_err("aggregate eager queue reservations must be bounded");
        assert!(error.to_string().contains("eager queue slots"), "{error}");

        let error = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [recording.raw]
            enabled = true
            queue_capacity = 600000
            "#,
        )
        .expect_err("both recording queues must count toward the aggregate reservation");
        assert!(error.to_string().contains("eager queue slots"), "{error}");
    }

    #[test]
    fn rejects_kafka_without_topic_and_nats_without_subject() {
        assert!(
            DaemonConfig::from_toml_str(
                r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[sinks]]
            type = "kafka"
            address = "127.0.0.1:9092"
            [[venues]]
            id = "synthetic-demo"
            adapter = "synthetic"
        "#,
            )
            .is_err()
        );
        assert!(
            DaemonConfig::from_toml_str(
                r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[sinks]]
            type = "nats"
            address = "127.0.0.1:4222"
            [[venues]]
            id = "synthetic-demo"
            adapter = "synthetic"
        "#,
            )
            .is_err()
        );
    }

    #[test]
    fn accepts_minimal_loopback_config() {
        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[venues]]
            id = "binance-spot"
            adapter = "binance"
            symbols = ["BTCUSDT"]
            channels = ["trades"]
        "#,
        )
        .unwrap();
        assert_eq!(
            cfg.venues[0].resolved_kind().unwrap(),
            VenueKind::BinanceSpot
        );
    }

    #[test]
    fn rejects_unknown_root_field() {
        let error = DaemonConfig::from_toml_str(
            r#"
            typo = true
            "#,
        )
        .expect_err("unknown root fields must be rejected");

        assert!(error.to_string().contains("typo"), "{error}");
    }

    #[test]
    fn rejects_unknown_nested_venue_field() {
        let error = DaemonConfig::from_toml_str(
            r#"
            [[venues]]
            id = "binance-spot"
            adapter = "binance"
            symbols = ["BTCUSDT"]
            channels = ["trades"]
            typo = true
            "#,
        )
        .expect_err("unknown venue fields must be rejected");

        assert!(error.to_string().contains("typo"), "{error}");
    }

    #[test]
    fn rejects_ambiguous_live_venue_subscriptions() {
        let cases = [
            (
                "duplicate venue IDs",
                r#"
                [[venues]]
                id = "binance-spot"
                adapter = "binance"
                symbols = ["BTCUSDT"]
                channels = ["trades"]
                [[venues]]
                id = "binance-spot"
                adapter = "binance"
                symbols = ["ETHUSDT"]
                channels = ["quote"]
                "#,
                "duplicate venue id \"binance-spot\"",
            ),
            (
                "whitespace-padded venue ID",
                r#"
                [[venues]]
                id = " binance-spot "
                adapter = "binance"
                symbols = ["BTCUSDT"]
                channels = ["trades"]
                "#,
                "venues[0].id must not contain leading or trailing whitespace: \" binance-spot \"",
            ),
            (
                "empty symbols",
                r#"
                [[venues]]
                id = "binance-spot"
                adapter = "binance"
                channels = ["trades"]
                "#,
                "venue binance-spot: symbols must be non-empty",
            ),
            (
                "empty channels",
                r#"
                [[venues]]
                id = "binance-spot"
                adapter = "binance"
                symbols = ["BTCUSDT"]
                "#,
                "venue binance-spot: channels must be non-empty",
            ),
            (
                "duplicate symbols",
                r#"
                [[venues]]
                id = "binance-spot"
                adapter = "binance"
                symbols = ["BTCUSDT", "BTCUSDT"]
                channels = ["trades"]
                "#,
                "venue binance-spot: duplicate symbol \"BTCUSDT\"",
            ),
            (
                "semantic channel aliases",
                r#"
                [[venues]]
                id = "binance-spot"
                adapter = "binance"
                symbols = ["BTCUSDT"]
                channels = ["quote", "ticker"]
                "#,
                "venue binance-spot: duplicate channel \"ticker\" (canonical \"quote\")",
            ),
        ];

        for (name, body, expected) in cases {
            let error = DaemonConfig::from_toml_str(body)
                .expect_err("ambiguous live venue configuration must be rejected");
            assert!(error.to_string().contains(expected), "{name}: {error}");
        }
    }

    #[test]
    fn rejects_ambiguous_symbols_and_channels() {
        let cases = [
            (
                "blank symbol",
                r#"
                [[venues]]
                id = "binance-spot"
                adapter = "binance"
                symbols = [""]
                channels = ["trades"]
                "#,
                "venue binance-spot: symbols contains blank value \"\"",
            ),
            (
                "whitespace-padded symbol",
                r#"
                [[venues]]
                id = "binance-spot"
                adapter = "binance"
                symbols = [" BTCUSDT "]
                channels = ["trades"]
                "#,
                "venue binance-spot: symbol must not contain leading or trailing whitespace: \" BTCUSDT \"",
            ),
            (
                "blank channel",
                r#"
                [[venues]]
                id = "binance-spot"
                adapter = "binance"
                symbols = ["BTCUSDT"]
                channels = [""]
                "#,
                "venue binance-spot: channels contains blank value \"\"",
            ),
            (
                "whitespace-padded channel",
                r#"
                [[venues]]
                id = "binance-spot"
                adapter = "binance"
                symbols = ["BTCUSDT"]
                channels = [" trades "]
                "#,
                "venue binance-spot: channel must not contain leading or trailing whitespace: \" trades \"",
            ),
        ];

        for (name, body, expected) in cases {
            let error = DaemonConfig::from_toml_str(body)
                .expect_err("ambiguous symbols and channels must be rejected");
            assert!(error.to_string().contains(expected), "{name}: {error}");
        }
    }

    #[test]
    fn accepts_symbols_and_channels() {
        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[venues]]
            id = "binance-spot"
            adapter = "binance"
            symbols = ["BTCUSDT", "ETHUSDT"]
            channels = ["trades", "quote", "l2"]
        "#,
        )
        .unwrap();
        assert_eq!(cfg.venues[0].symbols.len(), 2);
        assert!(cfg.venues[0].wants_l2());
    }

    #[test]
    fn rejects_unknown_channel() {
        let error = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[venues]]
            id = "binance-spot"
            adapter = "binance"
            symbols = ["BTCUSDT"]
            channels = ["nope"]
        "#,
        )
        .unwrap_err();

        assert!(
            error.to_string().contains(r#"unknown channel "nope""#),
            "{error}"
        );
    }

    #[test]
    fn parses_recording_sizes() {
        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [recording.raw]
            enabled = true
            directory = "./raw"
            segment_size = "64MiB"
            segment_duration = "15m"
            queue_capacity = 1024
            overflow = "drop_oldest"
            min_free_space = "1GiB"
        "#,
        )
        .unwrap();
        assert_eq!(
            cfg.recording.raw.segment_size_bytes().unwrap(),
            64 * 1024 * 1024
        );
        assert_eq!(
            cfg.recording.raw.segment_duration().unwrap(),
            Duration::from_secs(15 * 60)
        );
        assert_eq!(
            cfg.recording.raw.overflow_policy().unwrap(),
            OverflowPolicy::DropOldest
        );
    }

    #[test]
    fn synthetic_defaults_and_rejects_live_transport() {
        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[venues]]
            id = "synthetic-demo"
            adapter = "synthetic"
        "#,
        )
        .unwrap();
        assert_eq!(cfg.venues[0].resolved_transport(), TransportMode::Memory);
        assert!(
            DaemonConfig::from_toml_str(
                r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[venues]]
            id = "synthetic-demo"
            adapter = "synthetic"
            transport = "live"
        "#,
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_non_loopback_bind() {
        let err = DaemonConfig::from_toml_str(
            "[telemetry]
bind = \"0.0.0.0:9108\"
",
        )
        .unwrap_err();
        assert!(matches!(err, ConfigError::Validation(_)));
    }

    #[test]
    fn rejects_unknown_adapter_and_impossible_readiness() {
        assert!(
            DaemonConfig::from_toml_str(
                r#"
            [[venues]]
            id = "x"
            adapter = "nope"
        "#,
            )
            .is_err()
        );
        assert!(
            DaemonConfig::from_toml_str(
                r#"
            [readiness]
            min_live_sessions = 1
        "#,
            )
            .is_err()
        );
    }

    #[test]
    fn accepts_all_known_adapters() {
        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[venues]]
            id = "synthetic-demo"
            adapter = "synthetic"
            [[venues]]
            id = "binance-spot"
            adapter = "binance"
            segment = "spot"
            symbols = ["BTCUSDT"]
            channels = ["trades", "quote", "l2"]
            [[venues]]
            id = "binance-usdm"
            adapter = "binance"
            segment = "usdm"
            symbols = ["BTCUSDT"]
            channels = ["trades", "funding"]
            [[venues]]
            id = "binance-coinm"
            adapter = "binance"
            segment = "coinm"
            symbols = ["BTCUSD_PERP"]
            channels = ["trades", "mark", "funding"]
            [[venues]]
            id = "okx-spot"
            adapter = "okx"
            segment = "spot"
            symbols = ["BTC-USDT"]
            channels = ["trades", "ticker"]
            [[venues]]
            id = "okx-swap"
            adapter = "okx"
            segment = "swap"
            symbols = ["BTC-USDT-SWAP"]
            channels = ["trades", "l2", "funding"]
            [[venues]]
            id = "okx-futures"
            adapter = "okx"
            segment = "futures"
            symbols = ["BTC-USDT-250926"]
            channels = ["trades", "l2"]
            [[venues]]
            id = "bybit-linear"
            adapter = "bybit"
            segment = "linear"
            symbols = ["BTCUSDT"]
            channels = ["trades", "l2"]
            [[venues]]
            id = "bybit-spot"
            adapter = "bybit"
            segment = "spot"
            symbols = ["BTCUSDT"]
            channels = ["trades"]
            [[venues]]
            id = "kraken-spot"
            adapter = "kraken"
            segment = "spot"
            symbols = ["BTC/USD"]
            channels = ["trades", "quote"]
            [[venues]]
            id = "deribit"
            adapter = "deribit"
            symbols = ["BTC-PERPETUAL"]
            channels = ["trades", "ticker", "mark"]
            [[venues]]
            id = "bitstamp"
            adapter = "bitstamp"
            symbols = ["btcusd"]
            channels = ["trades", "quote", "l2"]
            [[venues]]
            id = "gemini"
            adapter = "gemini"
            symbols = ["BTCUSD"]
            channels = ["trades", "quote", "l2"]
            [[venues]]
            id = "coinbase-spot"
            adapter = "coinbase"
            segment = "spot"
            symbols = ["BTC-USD"]
            channels = ["trades", "quote", "l2"]
            [[venues]]
            id = "coinbase-adv"
            adapter = "coinbase"
            segment = "advanced"
            symbols = ["BTC-USD"]
            channels = ["candles"]
            [[venues]]
            id = "bitfinex"
            adapter = "bitfinex"
            symbols = ["tBTCUSD"]
            channels = ["trades", "quote", "l2"]
        "#,
        )
        .unwrap();
        assert_eq!(cfg.venues.len(), 16);
        assert_eq!(
            cfg.venues[3].resolved_kind().unwrap(),
            VenueKind::BinanceCoinm
        );
        assert_eq!(cfg.venues[4].resolved_kind().unwrap(), VenueKind::OkxSpot);
        assert_eq!(cfg.venues[5].resolved_kind().unwrap(), VenueKind::OkxSwap);
        assert_eq!(
            cfg.venues[6].resolved_kind().unwrap(),
            VenueKind::OkxFutures
        );
        assert_eq!(
            cfg.venues[7].resolved_kind().unwrap(),
            VenueKind::BybitLinear
        );
        assert_eq!(cfg.venues[8].resolved_kind().unwrap(), VenueKind::BybitSpot);
        assert_eq!(
            cfg.venues[9].resolved_kind().unwrap(),
            VenueKind::KrakenSpot
        );
        assert_eq!(cfg.venues[10].resolved_kind().unwrap(), VenueKind::Deribit);
        assert_eq!(cfg.venues[11].resolved_kind().unwrap(), VenueKind::Bitstamp);
        assert_eq!(cfg.venues[12].resolved_kind().unwrap(), VenueKind::Gemini);
        assert_eq!(
            cfg.venues[13].resolved_kind().unwrap(),
            VenueKind::CoinbaseSpot
        );
        assert_eq!(
            cfg.venues[14].resolved_kind().unwrap(),
            VenueKind::CoinbaseAdvanced
        );
        assert_eq!(cfg.venues[15].resolved_kind().unwrap(), VenueKind::Bitfinex);
    }

    #[test]
    fn coinbase_adv_segment_and_id_inference() {
        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[venues]]
            id = "coinbase-adv"
            adapter = "coinbase"
            symbols = ["BTC-USD"]
            channels = ["candles"]
        "#,
        )
        .unwrap();
        assert_eq!(
            cfg.venues[0].resolved_kind().unwrap(),
            VenueKind::CoinbaseAdvanced
        );
    }

    #[test]
    fn bybit_inverse_segment_and_id_inference() {
        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[venues]]
            id = "bybit-inv"
            adapter = "bybit"
            segment = "inverse"
            symbols = ["BTCUSD"]
            channels = ["trades", "l2"]
            [[venues]]
            id = "bybit-inverse"
            adapter = "bybit"
            symbols = ["ETHUSD"]
            channels = ["trades"]
        "#,
        )
        .unwrap();
        assert_eq!(
            cfg.venues[0].resolved_kind().unwrap(),
            VenueKind::BybitInverse
        );
        assert_eq!(
            cfg.venues[1].resolved_kind().unwrap(),
            VenueKind::BybitInverse
        );
    }

    #[test]
    fn binance_coinm_inferred_from_id_without_segment() {
        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[venues]]
            id = "binance-coinm"
            adapter = "binance"
            symbols = ["BTCUSD_PERP"]
            channels = ["trades", "funding"]
        "#,
        )
        .unwrap();
        assert_eq!(
            cfg.venues[0].resolved_kind().unwrap(),
            VenueKind::BinanceCoinm
        );
    }

    #[test]
    fn rejects_memory_transport_for_live_venues() {
        assert!(
            DaemonConfig::from_toml_str(
                r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[venues]]
            id = "okx-spot"
            adapter = "okx"
            transport = "memory"
        "#,
            )
            .is_err()
        );
    }

    #[test]
    fn parse_bytes_and_duration_helpers() {
        assert_eq!(parse_bytes("1KiB").unwrap(), 1024);
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
    }

    #[test]
    fn rejects_retired_private_binance_listen_key_flow() {
        let err = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [private.binance_spot]
            enabled = true
            [[venues]]
            id = "synthetic-demo"
            adapter = "synthetic"
        "#,
        )
        .expect_err("the retired Binance listen-key flow must fail closed");
        let message = err.to_string();
        assert!(message.contains("Binance retired the listen-key"));
        assert!(message.contains("userDataStream.subscribe"));

        let leaked = PrivateBinanceSpotConfig {
            enabled: true,
            api_key: Some("live-key-must-not-appear".into()),
            api_secret: Some("live-secret-must-not-appear".into()),
            ..PrivateBinanceSpotConfig::default()
        };
        let dbg = format!("{leaked:?}");
        assert!(dbg.contains("<redacted>"));
        assert!(!dbg.contains("live-key"));
        assert!(!dbg.contains("live-secret"));
    }

    #[test]
    fn rejects_private_secrets_in_toml() {
        for body in [
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [private.binance_spot]
            enabled = true
            api_key = "must-not-load"
        "#,
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [private.binance_spot]
            enabled = true
            api_secret = "must-not-load"
        "#,
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [private.binance_spot]
            enabled = true
            binance_api_key = "must-not-load"
        "#,
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [private.okx_spot]
            enabled = true
            api_key = "must-not-load"
        "#,
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [private.okx_spot]
            enabled = true
            passphrase = "must-not-load"
        "#,
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [private.bybit_spot]
            enabled = true
            bybit_api_secret = "must-not-load"
        "#,
        ] {
            let err = DaemonConfig::from_toml_str(body).unwrap_err();
            assert!(
                matches!(err, ConfigError::Validation(ref m) if m.contains("secrets must not")),
                "expected secrets rejection, got {err:?}"
            );
        }
    }

    #[test]
    fn rejects_private_okx_bybit_without_a_daemon_account_sink() {
        let error = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [private.okx_spot]
            enabled = true
            [private.bybit_spot]
            enabled = true
            [[venues]]
            id = "synthetic-demo"
            adapter = "synthetic"
        "#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("account-event sink"));
        assert!(error.to_string().contains("readiness/liveness"));
        assert!(error.to_string().contains("reconnect"));
        let leaked = PrivateOkxSpotConfig {
            enabled: true,
            api_key: Some("okx-key-must-not-appear".into()),
            passphrase: Some("okx-pass-must-not-appear".into()),
            ..PrivateOkxSpotConfig::default()
        };
        let dbg = format!("{leaked:?}");
        assert!(dbg.contains("<redacted>"));
        assert!(!dbg.contains("okx-key"));
        assert!(!dbg.contains("okx-pass"));
    }

    #[test]
    fn coinbase_intl_enable_only_no_secrets() {
        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[venues]]
            id = "coinbase-intl"
            adapter = "coinbase"
            segment = "intl"
            symbols = ["BTC-PERP"]
            channels = ["trades", "quote"]
        "#,
        )
        .unwrap();
        assert_eq!(
            cfg.venues[0].resolved_kind().unwrap(),
            VenueKind::CoinbaseIntl
        );
        assert!(!cfg.venues[0].has_toml_secrets());
    }

    #[test]
    fn rejects_coinbase_intl_secrets_in_toml() {
        for body in [
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[venues]]
            id = "coinbase-intl"
            adapter = "coinbase"
            segment = "intl"
            api_key = "must-not-load"
        "#,
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[venues]]
            id = "coinbase-intl"
            adapter = "coinbase"
            segment = "intl"
            api_secret = "must-not-load"
        "#,
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[venues]]
            id = "coinbase-intl"
            adapter = "coinbase"
            segment = "intl"
            passphrase = "must-not-load"
        "#,
        ] {
            let err = DaemonConfig::from_toml_str(body).unwrap_err();
            assert!(
                matches!(err, ConfigError::Validation(ref m) if m.contains("coinbase-intl secrets must not")),
                "expected coinbase-intl secrets rejection, got {err:?}"
            );
        }
    }

    #[test]
    fn rejects_coinbase_exchange_secrets_in_toml() {
        for secret_field in [
            r#"api_key = "must-not-load""#,
            r#"api_secret = "must-not-load""#,
            r#"passphrase = "must-not-load""#,
        ] {
            let body = format!(
                r#"
                [telemetry]
                bind = "127.0.0.1:9108"
                [[venues]]
                id = "coinbase-spot"
                adapter = "coinbase"
                segment = "exchange"
                symbols = ["BTC-USD"]
                channels = ["l2"]
                {secret_field}
            "#
            );
            let error = DaemonConfig::from_toml_str(&body)
                .expect_err("Coinbase Exchange secrets must be environment-only");
            assert!(
                matches!(
                    error,
                    ConfigError::Validation(ref message)
                        if message.contains("coinbase-spot secrets must not")
                            && message.contains("COINBASE_EXCHANGE_API_KEY")
                            && message.contains("COINBASE_EXCHANGE_API_SECRET")
                            && message.contains("COINBASE_EXCHANGE_API_PASSPHRASE")
                ),
                "unexpected error: {error:?}"
            );
        }
    }

    #[test]
    fn coinbase_intl_segment_and_id_inference() {
        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9108"
            [[venues]]
            id = "coinbase-intl"
            adapter = "coinbase"
            symbols = ["BTC-PERP"]
            channels = ["trades"]
        "#,
        )
        .unwrap();
        assert_eq!(
            cfg.venues[0].resolved_kind().unwrap(),
            VenueKind::CoinbaseIntl
        );
    }
}
