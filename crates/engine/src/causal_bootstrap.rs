//! Exact bootstrap meaning. Hash verification here is not a durable receipt or
//! permission to create a factory; the owning integration supplies those gates.
use super::*;
use sha2::{Digest, Sha256};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompiledBuildDto {
    pub schema_version: u16,
    pub package_version: String,
    pub source_scheme: String,
    pub source_sha256: String,
    pub source_files: u64,
    pub lock_sha256: String,
    pub git_commit: Option<String>,
    pub rustc_verbose: String,
    pub target: String,
    pub profile: String,
    pub opt_level: String,
    pub debug: String,
    pub encoded_rustflags: String,
    /// Enabled flags in each selected runtime package. The actual executable
    /// hash additionally binds dependency feature unification and linked bytes.
    pub package_features: Vec<PackageFeaturesDto>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageFeaturesDto {
    pub package: String,
    pub features: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapBuildDto {
    pub compiled: CompiledBuildDto,
    pub executable_sha256: String,
    pub executable_bytes: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BybitCategoryDto {
    Linear,
    Spot,
    Inverse,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "config",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum FactoryDto {
    BybitPublic {
        category: BybitCategoryDto,
        enable_l2: bool,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapEnvironmentDto {
    Production,
    Sandbox,
    Test,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputProfileDto {
    DiskNullDispatchV1,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BybitEmissionPolicyDto {
    CompleteNativeBundleV1,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurationDto {
    pub seconds: u64,
    pub nanoseconds: u32,
}
impl From<Duration> for DurationDto {
    fn from(value: Duration) -> Self {
        Self {
            seconds: value.as_secs(),
            nanoseconds: value.subsec_nanos(),
        }
    }
}
impl DurationDto {
    pub fn duration(self) -> Result<Duration, CodecError> {
        if self.nanoseconds >= 1_000_000_000 {
            return Err(invalid("noncanonical duration"));
        }
        Ok(Duration::new(self.seconds, self.nanoseconds))
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandleIntervalDto {
    M1,
    M5,
    M15,
    H1,
    D1,
}
impl From<CandleInterval> for CandleIntervalDto {
    fn from(value: CandleInterval) -> Self {
        match value {
            CandleInterval::M1 => Self::M1,
            CandleInterval::M5 => Self::M5,
            CandleInterval::M15 => Self::M15,
            CandleInterval::H1 => Self::H1,
            CandleInterval::D1 => Self::D1,
        }
    }
}
impl From<CandleIntervalDto> for CandleInterval {
    fn from(value: CandleIntervalDto) -> Self {
        match value {
            CandleIntervalDto::M1 => Self::M1,
            CandleIntervalDto::M5 => Self::M5,
            CandleIntervalDto::M15 => Self::M15,
            CandleIntervalDto::H1 => Self::H1,
            CandleIntervalDto::D1 => Self::D1,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "config",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ChannelDto {
    Trades,
    AllTrades,
    Quote,
    L2Book {
        depth: Option<u32>,
        cadence: Option<DurationDto>,
    },
    L3Book,
    Candles {
        interval: CandleIntervalDto,
    },
    MarkPrice,
    IndexPrice,
    Funding,
    OpenInterest,
    Liquidations,
    Statistics24h,
    InstrumentStatus,
}
impl From<&Channel> for ChannelDto {
    fn from(value: &Channel) -> Self {
        match value {
            Channel::Trades => Self::Trades,
            Channel::AllTrades => Self::AllTrades,
            Channel::Quote => Self::Quote,
            Channel::L2Book { depth, cadence } => Self::L2Book {
                depth: *depth,
                cadence: cadence.map(Into::into),
            },
            Channel::L3Book => Self::L3Book,
            Channel::Candles { interval } => Self::Candles {
                interval: (*interval).into(),
            },
            Channel::MarkPrice => Self::MarkPrice,
            Channel::IndexPrice => Self::IndexPrice,
            Channel::Funding => Self::Funding,
            Channel::OpenInterest => Self::OpenInterest,
            Channel::Liquidations => Self::Liquidations,
            Channel::Statistics24h => Self::Statistics24h,
            Channel::InstrumentStatus => Self::InstrumentStatus,
        }
    }
}
impl ChannelDto {
    pub fn channel(&self) -> Result<Channel, CodecError> {
        Ok(match self {
            Self::Trades => Channel::Trades,
            Self::AllTrades => Channel::AllTrades,
            Self::Quote => Channel::Quote,
            Self::L2Book { depth, cadence } => Channel::L2Book {
                depth: *depth,
                cadence: cadence.map(DurationDto::duration).transpose()?,
            },
            Self::L3Book => Channel::L3Book,
            Self::Candles { interval } => Channel::Candles {
                interval: (*interval).into(),
            },
            Self::MarkPrice => Channel::MarkPrice,
            Self::IndexPrice => Channel::IndexPrice,
            Self::Funding => Channel::Funding,
            Self::OpenInterest => Channel::OpenInterest,
            Self::Liquidations => Channel::Liquidations,
            Self::Statistics24h => Channel::Statistics24h,
            Self::InstrumentStatus => Channel::InstrumentStatus,
        })
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryDto {
    pub emit_book_snapshots: bool,
    pub emit_book_deltas: bool,
    pub emit_bbo: bool,
}
impl From<DeliveryOptions> for DeliveryDto {
    fn from(value: DeliveryOptions) -> Self {
        Self {
            emit_book_snapshots: value.emit_book_snapshots,
            emit_book_deltas: value.emit_book_deltas,
            emit_bbo: value.emit_bbo,
        }
    }
}
impl From<DeliveryDto> for DeliveryOptions {
    fn from(value: DeliveryDto) -> Self {
        Self {
            emit_book_snapshots: value.emit_book_snapshots,
            emit_book_deltas: value.emit_book_deltas,
            emit_bbo: value.emit_bbo,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionDto {
    pub instrument: u32,
    pub channel: ChannelDto,
    pub delivery: DeliveryDto,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSpecDto {
    pub endpoint_name: String,
    pub subscriptions: Vec<SubscriptionDto>,
}
impl From<&SessionSpec> for SessionSpecDto {
    fn from(value: &SessionSpec) -> Self {
        Self {
            endpoint_name: value.endpoint_name.clone(),
            subscriptions: value
                .subscriptions
                .items
                .iter()
                .map(|s| SubscriptionDto {
                    instrument: s.instrument.0,
                    channel: (&s.channel).into(),
                    delivery: s.delivery.into(),
                })
                .collect(),
        }
    }
}
impl SessionSpecDto {
    pub fn session_spec(&self) -> Result<SessionSpec, CodecError> {
        Ok(SessionSpec {
            endpoint_name: self.endpoint_name.clone(),
            subscriptions: ConcreteSubscriptionSet {
                items: self
                    .subscriptions
                    .iter()
                    .map(|s| {
                        Ok(ConcreteSubscription {
                            instrument: InstrumentId(s.instrument),
                            channel: s.channel.channel()?,
                            delivery: s.delivery.into(),
                        })
                    })
                    .collect::<Result<_, CodecError>>()?,
            },
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureConfigDto {
    pub venue: u16,
    pub session: u64,
    pub dispatch_capacity: u64,
    pub overflow: OverflowDto,
    pub max_bytes: u64,
    pub max_entries: u64,
}
impl From<CaptureConfig> for CaptureConfigDto {
    fn from(c: CaptureConfig) -> Self {
        Self {
            venue: c.venue.0,
            session: c.session.0,
            dispatch_capacity: c.dispatch_capacity as u64,
            overflow: c.overflow.into(),
            max_bytes: c.max_bytes as u64,
            max_entries: c.max_entries as u64,
        }
    }
}
impl CaptureConfigDto {
    pub fn config(&self) -> Result<CaptureConfig, CodecError> {
        let overflow = match self.overflow {
            OverflowDto::FailEngine => OverflowPolicy::FailEngine,
            OverflowDto::DropNewest => OverflowPolicy::DropNewest,
            OverflowDto::DropOldest => OverflowPolicy::DropOldest,
            _ => return Err(invalid("unsupported bootstrap overflow policy")),
        };
        let c = CaptureConfig {
            venue: VenueId(self.venue),
            session: SessionId(self.session),
            dispatch_capacity: usize::try_from(self.dispatch_capacity)
                .map_err(|_| invalid("dispatch capacity overflow"))?,
            overflow,
            max_bytes: usize::try_from(self.max_bytes)
                .map_err(|_| invalid("capture bytes overflow"))?,
            max_entries: usize::try_from(self.max_entries)
                .map_err(|_| invalid("capture entries overflow"))?,
        };
        c.validate()
            .map_err(|_| invalid("invalid bootstrap capture policy"))?;
        Ok(c)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicReproductionInputDto {
    pub role: String,
    pub sha256: String,
    pub bytes: u64,
    pub payload: BytesDto,
}
/// Bound before factory construction. Limits cover queued plus active ingress;
/// capture, encoded transaction and one committed release are additional bounds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalOwnerPolicyDto {
    pub max_pending_operations: u32,
    pub max_pending_bytes: u64,
    pub max_input_bytes: u64,
    pub max_operation_bytes: u64,
    /// One absolute deadline from accepted ingress, including queue/execution,
    /// admission retries and observed commit. Accepted storage still settles.
    pub max_ingress_age: DurationDto,
    pub receipt_poll_interval: DurationDto,
}
impl Default for JournalOwnerPolicyDto {
    fn default() -> Self {
        Self {
            max_pending_operations: 512,
            max_pending_bytes: 16 * 1024 * 1024,
            max_input_bytes: 2 * 1024 * 1024,
            max_operation_bytes: MAX_OPERATION_BYTES as u64,
            max_ingress_age: Duration::from_secs(10).into(),
            receipt_poll_interval: Duration::from_millis(1).into(),
        }
    }
}
impl JournalOwnerPolicyDto {
    pub fn validate(&self) -> Result<(), CodecError> {
        let age = self.max_ingress_age.duration()?;
        let poll = self.receipt_poll_interval.duration()?;
        if self.max_pending_operations == 0
            || self.max_pending_operations > 65_536
            || self.max_input_bytes == 0
            || self.max_input_bytes > self.max_pending_bytes
            || self.max_pending_bytes > 64 * 1024 * 1024
            || self.max_operation_bytes == 0
            || self.max_operation_bytes > MAX_OPERATION_BYTES as u64
            || usize::try_from(self.max_pending_bytes).is_err()
            || usize::try_from(self.max_input_bytes).is_err()
            || usize::try_from(self.max_operation_bytes).is_err()
            || age.is_zero()
            || age > Duration::from_secs(3600)
            || poll.is_zero()
            || poll > age
            || poll > Duration::from_millis(100)
        {
            return Err(invalid("invalid journal owner bounds/deadline"));
        }
        Ok(())
    }
}
/// MFCJ3's explicit additional retention/finalization policy. Its canonical
/// bytes are a named, hash-bound bootstrap reproduction input. Older profiles
/// cannot silently ignore this reserved role or start the runtime owner.
pub const HISTORY_RUNTIME_POLICY_ROLE: &str = "owner-history-runtime-v1";
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryRuntimePolicyDto {
    pub schema_version: u16,
    pub max_pending_observations: u32,
    pub max_pending_observation_bytes: u64,
    pub max_record_bytes: u64,
    pub max_finalization_age: DurationDto,
}
impl HistoryRuntimePolicyDto {
    pub fn validate(&self, owner: &JournalOwnerPolicyDto) -> Result<(), CodecError> {
        owner.validate()?;
        let age = self.max_finalization_age.duration()?;
        if self.schema_version != 1
            || self.max_pending_observations < 6
            || self.max_pending_observations > 65_536
            || self.max_record_bytes < 4096
            || self.max_record_bytes > owner.max_operation_bytes
            || self.max_record_bytes < 6 * owner.max_input_bytes + 4096
            || self.max_pending_observation_bytes < 3 * self.max_record_bytes + 4096
            || self.max_pending_observation_bytes > 256 * 1024 * 1024
            || age.is_zero()
            || age > Duration::from_secs(3600)
            || owner.receipt_poll_interval.duration()? > age
        {
            return Err(invalid(
                "invalid history runtime retention/finalization policy",
            ));
        }
        Ok(())
    }
}
impl PublicReproductionInputDto {
    pub fn new(role: impl Into<String>, bytes: &[u8]) -> Self {
        Self {
            role: role.into(),
            sha256: bootstrap_sha256(bytes),
            bytes: bytes.len() as u64,
            payload: BytesDto::new(bytes),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapDto {
    pub schema_version: u16,
    pub build: BootstrapBuildDto,
    pub factory: FactoryDto,
    pub environment: BootstrapEnvironmentDto,
    pub output_profile: OutputProfileDto,
    pub adapter_policy: BybitEmissionPolicyDto,
    pub protocol_topics: Vec<String>,
    pub session: SessionSpecDto,
    pub catalog: CatalogDto,
    pub capture: CaptureConfigDto,
    pub runner_policy: CaptureRuntimePolicy,
    /// V1 bootstrap bytes remain decodable and canonical, but cannot start the
    /// new owner without an explicit V2 policy. No silent runtime defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_policy: Option<JournalOwnerPolicyDto>,
    pub public_inputs: Vec<PublicReproductionInputDto>,
}
impl BootstrapDto {
    pub fn bind_history_policy(
        &mut self,
        policy: HistoryRuntimePolicyDto,
    ) -> Result<(), CodecError> {
        if self
            .public_inputs
            .iter()
            .any(|p| p.role == HISTORY_RUNTIME_POLICY_ROLE)
        {
            return Err(invalid("duplicate history runtime policy"));
        }
        policy.validate(
            self.owner_policy
                .as_ref()
                .ok_or_else(|| invalid("missing owner policy"))?,
        )?;
        self.public_inputs.push(PublicReproductionInputDto::new(
            HISTORY_RUNTIME_POLICY_ROLE,
            &serde_json::to_vec(&policy)?,
        ));
        Ok(())
    }
    pub fn history_policy(&self) -> Result<HistoryRuntimePolicyDto, CodecError> {
        let mut rows = self
            .public_inputs
            .iter()
            .filter(|p| p.role == HISTORY_RUNTIME_POLICY_ROLE);
        let row = rows
            .next()
            .ok_or_else(|| invalid("missing history runtime policy"))?;
        if rows.next().is_some() {
            return Err(invalid("duplicate history runtime policy"));
        }
        let bytes = row.payload.bytes()?;
        if row.bytes != bytes.len() as u64 || row.sha256 != bootstrap_sha256(&bytes) {
            return Err(invalid("history policy reproduction digest mismatch"));
        }
        if bytes.len() > 4096 {
            return Err(CodecError::TooLarge);
        }
        let policy: HistoryRuntimePolicyDto = serde_json::from_slice(&bytes)?;
        if serde_json::to_vec(&policy)? != bytes {
            return Err(invalid("noncanonical history runtime policy"));
        }
        policy.validate(
            self.owner_policy
                .as_ref()
                .ok_or_else(|| invalid("missing owner policy"))?,
        )?;
        Ok(policy)
    }
    pub fn validate(&self) -> Result<(), CodecError> {
        let c = self.capture.config()?;
        if !matches!(
            (self.schema_version, self.owner_policy.is_some()),
            (1, false) | (2, true)
        ) || self.runner_policy != c.runtime_policy()
            || self.catalog.venue != self.capture.venue
            || self.catalog.instruments.len() > 65536
            || self.session.subscriptions.len() > 65536
            || self.public_inputs.len() > 4096
            || self.protocol_topics.is_empty()
            || self.protocol_topics.len() > 65536
            || self.protocol_topics.iter().any(String::is_empty)
        {
            return Err(invalid(
                "bootstrap schema, scope or effective runner policy mismatch",
            ));
        }
        if let Some(policy) = &self.owner_policy {
            policy.validate()?;
        }
        self.catalog.catalog()?;
        self.session.session_spec()?;
        let b = &self.build.compiled;
        if b.schema_version != 1
            || b.source_scheme != "sha256-path-length-and-bytes-v1"
            || b.source_files == 0
            || !digest(&b.source_sha256)
            || !digest(&b.lock_sha256)
            || !digest(&self.build.executable_sha256)
            || self.build.executable_bytes == 0
            || b.package_version.is_empty()
            || b.rustc_verbose.is_empty()
            || b.target.is_empty()
            || b.profile.is_empty()
            || b.package_features.is_empty()
            || b.package_features
                .windows(2)
                .any(|w| w[0].package >= w[1].package)
            || b.package_features.iter().any(|p| {
                p.package.is_empty()
                    || p.features.iter().any(String::is_empty)
                    || p.features.windows(2).any(|w| w[0] >= w[1])
            })
            || self
                .public_inputs
                .iter()
                .any(|p| p.role.is_empty() || !digest(&p.sha256))
        {
            return Err(invalid("invalid bootstrap build or reproduction binding"));
        }
        if let Some(commit) = &b.git_commit {
            if !matches!(commit.len(), 40 | 64)
                || !commit
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            {
                return Err(invalid("invalid build source commit"));
            }
        }
        for input in &self.public_inputs {
            let bytes = input.payload.bytes()?;
            if bytes.len() as u64 != input.bytes || bootstrap_sha256(&bytes) != input.sha256 {
                return Err(invalid("reproduction input bytes/digest mismatch"));
            }
        }
        Ok(())
    }
}
fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
pub fn bootstrap_sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Hash covers the exact canonical payload, not a reserialized approximation.
pub fn encode_bootstrap(
    dto: &BootstrapDto,
    max_bytes: usize,
) -> Result<CausalEnvelope, CodecError> {
    encode_bootstrap_for_profile(dto, max_bytes, FeedProfile::EngineCausalV1)
}
/// V2 negotiates owner observations before any records are read. This codec
/// does not enable a daemon factory or migrate a live caller.
pub fn encode_bootstrap_for_profile(
    dto: &BootstrapDto,
    max_bytes: usize,
    profile: FeedProfile,
) -> Result<CausalEnvelope, CodecError> {
    validate_bootstrap_profile(dto, profile)?;
    dto.validate()?;
    if max_bytes == 0 || max_bytes > MAX_OPERATION_BYTES {
        return Err(CodecError::TooLarge);
    }
    let mut buffer = CappedBuffer {
        bytes: Vec::new(),
        max: max_bytes,
        overflow: false,
    };
    if let Err(error) = serde_json::to_writer(&mut buffer, dto) {
        return Err(if buffer.overflow {
            CodecError::TooLarge
        } else {
            CodecError::Json(error)
        });
    }
    Ok(CausalEnvelope::for_profile(
        profile,
        CausalPhase::Bootstrap,
        CausalRoute {
            venue: dto.capture.venue,
            session: dto.capture.session,
            ordinal: 0,
            generation: 0,
            bootstrap_id: bootstrap_sha256(&buffer.bytes),
        },
        &buffer.bytes,
    )?)
}
fn validate_bootstrap_profile(dto: &BootstrapDto, profile: FeedProfile) -> Result<(), CodecError> {
    let has_runtime_policy = dto
        .public_inputs
        .iter()
        .any(|p| p.role == HISTORY_RUNTIME_POLICY_ROLE);
    match profile {
        FeedProfile::EngineCausalV1 if !has_runtime_policy => Ok(()),
        FeedProfile::EngineCausalV2 if dto.schema_version == 2 && !has_runtime_policy => Ok(()),
        FeedProfile::EngineCausalV3 if dto.schema_version == 2 => dto.history_policy().map(|_| ()),
        _ => Err(invalid(
            "bootstrap/profile or explicit runtime policy mismatch",
        )),
    }
}
pub fn decode_bootstrap(envelope: &CausalEnvelope) -> Result<BootstrapDto, CodecError> {
    if envelope.phase != CausalPhase::Bootstrap {
        return Err(invalid("expected bootstrap phase"));
    }
    let payload = envelope.payload()?;
    if bootstrap_sha256(&payload) != envelope.route.bootstrap_id {
        return Err(invalid("bootstrap payload digest mismatch"));
    }
    let dto: BootstrapDto = serde_json::from_slice(&payload)?;
    dto.validate()?;
    validate_bootstrap_profile(
        &dto,
        match envelope.schema_version {
            1 => FeedProfile::EngineCausalV1,
            2 => FeedProfile::EngineCausalV2,
            3 => FeedProfile::EngineCausalV3,
            _ => return Err(invalid("unsupported bootstrap envelope")),
        },
    )?;
    if envelope.route.venue != dto.capture.venue || envelope.route.session != dto.capture.session {
        return Err(invalid("bootstrap inner/outer identity mismatch"));
    }
    let mut check = Canonical {
        expected: &payload,
        offset: 0,
    };
    serde_json::to_writer(&mut check, &dto)?;
    if check.offset != payload.len() {
        return Err(invalid("noncanonical bootstrap"));
    }
    Ok(dto)
}
