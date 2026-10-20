//! First supported causal factory profile: public Bybit, disk/null dispatch.
//! Bootstrap must pass actual-artifact/config verification and a post-barrier
//! actor receipt before any factory/machine is created. No live loop is wired.
use marketfeed_adapter_api::{Capability, VenueFactory};
use marketfeed_adapter_bybit::{
    BYBIT_INVERSE_SPEC, BYBIT_LINEAR_SPEC, BYBIT_SPOT_SPEC, BybitCategory, BybitFactory,
};
use marketfeed_engine::{CaptureSession, causal::*};
use marketfeed_recording::{RecordingError, shared_batch::*, shared_feed::*};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::File,
    io::{self, Read},
};

pub mod owner;

#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error(transparent)]
    Recording(#[from] RecordingError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Commit(#[from] CommitFailure),
    #[error("unsupported or mismatched causal bootstrap: {0}")]
    Unsupported(&'static str),
    #[error("causal factory construction failed: {0}")]
    Factory(String),
}

/// Observe generated source/lock/compiler/build flags and the real current
/// executable file. This is an artifact fingerprint, not signed attestation.
/// Deployment must preserve the running artifact; path replacement cannot be
/// proved equivalent to an OS's loaded image using portable std APIs.
pub fn current_build() -> Result<BootstrapBuildDto, BootstrapError> {
    if !cfg!(feature = "causal-journal") {
        return Err(BootstrapError::Unsupported(
            "causal-journal feature disabled",
        ));
    }
    let mut compiled: CompiledBuildDto =
        serde_json::from_str(include_str!(concat!(env!("OUT_DIR"), "/causal-build.json")))
            .map_err(CodecError::from)?;
    compiled.package_features.extend([
        PackageFeaturesDto {
            package: "marketfeed-adapter-bybit".into(),
            features: marketfeed_adapter_bybit::compiled_features(),
        },
        PackageFeaturesDto {
            package: "marketfeed-engine".into(),
            features: marketfeed_engine::compiled_features(),
        },
        PackageFeaturesDto {
            package: "marketfeed-sinks".into(),
            features: marketfeed_sinks::compiled_features(),
        },
    ]);
    compiled
        .package_features
        .sort_by(|a, b| a.package.cmp(&b.package));
    let path = std::env::current_exe()?;
    let mut file = File::open(&path)?;
    let before = file.metadata()?;
    let mut digest = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 65536];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        bytes = bytes
            .checked_add(count as u64)
            .ok_or(BootstrapError::Unsupported("executable size overflow"))?;
        digest.update(&buffer[..count]);
    }
    let after = file.metadata()?;
    let current = std::fs::metadata(path)?;
    if bytes != before.len()
        || bytes != after.len()
        || bytes != current.len()
        || before.modified()? != after.modified()?
        || after.modified()? != current.modified()?
    {
        return Err(BootstrapError::Unsupported(
            "executable changed during fingerprint",
        ));
    }
    Ok(BootstrapBuildDto {
        compiled,
        executable_sha256: format!("{:x}", digest.finalize()),
        executable_bytes: bytes,
    })
}

fn category(factory: &FactoryDto) -> BybitCategory {
    match factory {
        FactoryDto::BybitPublic { category, .. } => match category {
            BybitCategoryDto::Linear => BybitCategory::Linear,
            BybitCategoryDto::Spot => BybitCategory::Spot,
            BybitCategoryDto::Inverse => BybitCategory::Inverse,
        },
    }
}
fn validate_supported(dto: &BootstrapDto) -> Result<(), BootstrapError> {
    dto.validate()?;
    if dto.environment != BootstrapEnvironmentDto::Production
        || dto.session.subscriptions.is_empty()
    {
        return Err(BootstrapError::Unsupported(
            "only explicit nonempty production public Bybit plans",
        ));
    }
    let category = category(&dto.factory);
    let spec = match category {
        BybitCategory::Linear => &BYBIT_LINEAR_SPEC,
        BybitCategory::Spot => &BYBIT_SPOT_SPEC,
        BybitCategory::Inverse => &BYBIT_INVERSE_SPEC,
    };
    if dto.capture.venue != category.venue_id().0
        || !spec
            .endpoints
            .iter()
            .any(|e| e.url.starts_with("wss://") && e.url == dto.session.endpoint_name)
    {
        return Err(BootstrapError::Unsupported(
            "category/venue/public endpoint mismatch",
        ));
    }
    let mut ids = BTreeSet::new();
    let mut symbols = BTreeSet::new();
    for row in &dto.catalog.instruments {
        if row.id == 0
            || !ids.insert(row.id)
            || !symbols.insert(&row.key.native_symbol)
            || row.key.venue != spec.code
        {
            return Err(BootstrapError::Unsupported(
                "ambiguous or wrong-venue catalog identity",
            ));
        }
        let valid_kind = match category {
            BybitCategory::Spot => row.key.kind == InstrumentKindDto::Spot && !row.inverse,
            BybitCategory::Linear => {
                matches!(
                    row.key.kind,
                    InstrumentKindDto::PerpetualLinear | InstrumentKindDto::FutureLinear
                ) && !row.inverse
            }
            BybitCategory::Inverse => {
                matches!(
                    row.key.kind,
                    InstrumentKindDto::PerpetualInverse | InstrumentKindDto::FutureInverse
                ) && row.inverse
            }
        };
        if !valid_kind {
            return Err(BootstrapError::Unsupported(
                "catalog product/category mismatch",
            ));
        }
    }
    for subscription in &dto.session.subscriptions {
        let capability = match subscription.channel {
            ChannelDto::Trades => Capability::Trades,
            ChannelDto::AllTrades => Capability::AllTrades,
            ChannelDto::Quote => Capability::Quote,
            ChannelDto::L2Book { .. } => Capability::L2Book,
            ChannelDto::L3Book => Capability::L3Book,
            ChannelDto::Candles { .. } => Capability::Candles,
            ChannelDto::MarkPrice => Capability::MarkPrice,
            ChannelDto::IndexPrice => Capability::IndexPrice,
            ChannelDto::Funding => Capability::Funding,
            ChannelDto::OpenInterest => Capability::OpenInterest,
            ChannelDto::Liquidations => Capability::Liquidations,
            ChannelDto::Statistics24h => Capability::Statistics24h,
            ChannelDto::InstrumentStatus => Capability::InstrumentStatus,
        };
        if !ids.contains(&subscription.instrument) || !spec.capabilities.contains(&capability) {
            return Err(BootstrapError::Unsupported(
                "unknown instrument or unsupported channel",
            ));
        }
    }
    let FactoryDto::BybitPublic { enable_l2, .. } = &dto.factory;
    validate_complete_bundle(dto, category)?;
    let topics = BybitFactory::public_plan_topics(
        category,
        *enable_l2,
        &dto.session.session_spec()?,
        &dto.catalog.catalog()?,
    )
    .map_err(|e| BootstrapError::Factory(e.to_string()))?;
    if dto.protocol_topics != topics {
        return Err(BootstrapError::Unsupported(
            "effective ordered protocol topics mismatch",
        ));
    }
    Ok(())
}

/// The existing adapter produces a broad native observation bundle. This first
/// profile admits that exact policy rather than pretending to honor arbitrary
/// channel subsets/negative delivery flags. Legacy arbitrary plans remain
/// outside this supported causal profile.
fn validate_complete_bundle(
    dto: &BootstrapDto,
    category: BybitCategory,
) -> Result<(), BootstrapError> {
    let mut groups = std::collections::BTreeMap::<u32, Vec<&SubscriptionDto>>::new();
    for s in &dto.session.subscriptions {
        if !(s.delivery.emit_book_snapshots && s.delivery.emit_book_deltas && s.delivery.emit_bbo) {
            return Err(BootstrapError::Unsupported(
                "complete native bundle requires all delivery flags",
            ));
        }
        groups.entry(s.instrument).or_default().push(s);
    }
    let FactoryDto::BybitPublic { enable_l2, .. } = dto.factory;
    let mut base = vec![
        ChannelDto::Trades,
        ChannelDto::Quote,
        ChannelDto::Statistics24h,
    ];
    if category != BybitCategory::Spot {
        base.extend([
            ChannelDto::MarkPrice,
            ChannelDto::IndexPrice,
            ChannelDto::Funding,
            ChannelDto::OpenInterest,
            ChannelDto::Liquidations,
        ]);
    }
    let mut expected_shape = None;
    for subscriptions in groups.values() {
        for (index, s) in subscriptions.iter().enumerate() {
            if subscriptions[..index]
                .iter()
                .any(|prior| prior.channel == s.channel)
            {
                return Err(BootstrapError::Unsupported(
                    "duplicate requested native channel",
                ));
            }
        }
        let mut remaining: Vec<_> = subscriptions.iter().map(|s| s.channel.clone()).collect();
        for channel in &base {
            let Some(index) = remaining.iter().position(|c| c == channel) else {
                return Err(BootstrapError::Unsupported(
                    "partial native bundle is unsupported",
                ));
            };
            remaining.remove(index);
        }
        if remaining
            .iter()
            .any(|c| !matches!(c, ChannelDto::L2Book { .. } | ChannelDto::Candles { .. }))
            || remaining
                .iter()
                .filter(|c| matches!(c, ChannelDto::L2Book { .. }))
                .count()
                != usize::from(enable_l2)
        {
            return Err(BootstrapError::Unsupported(
                "unsupported native bundle shape",
            ));
        }
        if let Some(expected) = &expected_shape {
            if expected != &remaining {
                return Err(BootstrapError::Unsupported(
                    "per-symbol L2/candle policy differs from global adapter policy",
                ));
            }
        } else {
            expected_shape = Some(remaining);
        }
    }
    Ok(())
}

/// Owns the same verified immutable payload and prepared transaction across
/// Full admission. There is no factory or mutable runner hidden in this phase.
#[derive(Debug)]
pub struct PreparedBootstrap {
    dto: BootstrapDto,
    identity: OperationIdentity,
    transaction: PreparedFeedTransaction,
}
impl PreparedBootstrap {
    /// Explicit MFCJ3 runtime-owner negotiation; policy bytes are bound into the
    /// canonical bootstrap before the original writer receipt or factory.
    pub fn new_history(dto: BootstrapDto, max_bytes: usize) -> Result<Self, BootstrapError> {
        validate_supported(&dto)?;
        if dto.build != current_build()? {
            return Err(BootstrapError::Unsupported(
                "build or actual executable mismatch",
            ));
        }
        let envelope = encode_bootstrap_for_profile(&dto, max_bytes, FeedProfile::EngineCausalV3)?;
        Self::from_verified(dto, envelope)
    }
    pub fn new(dto: BootstrapDto, max_bytes: usize) -> Result<Self, BootstrapError> {
        validate_supported(&dto)?;
        if dto.build != current_build()? {
            return Err(BootstrapError::Unsupported(
                "build or actual executable mismatch",
            ));
        }
        let envelope = encode_bootstrap(&dto, max_bytes)?;
        Self::from_verified(dto, envelope)
    }
    /// Physical-order replayers can verify bytes before any construction. A
    /// subsequent writer admission is still required by this live-start API.
    pub fn from_envelope(envelope: CausalEnvelope) -> Result<Self, BootstrapError> {
        if !matches!(envelope.schema_version, 1 | 3) {
            return Err(BootstrapError::Unsupported(
                "owner-history journal is not yet wired to the daemon owner",
            ));
        }
        let dto = decode_bootstrap(&envelope)?;
        validate_supported(&dto)?;
        if dto.build != current_build()? {
            return Err(BootstrapError::Unsupported(
                "build or actual executable mismatch",
            ));
        }
        Self::from_verified(dto, envelope)
    }
    fn from_verified(dto: BootstrapDto, envelope: CausalEnvelope) -> Result<Self, BootstrapError> {
        let identity = OperationIdentity {
            venue: envelope.route.venue,
            session: envelope.route.session,
            bootstrap_id: envelope.route.bootstrap_id.clone(),
        };
        let transaction = PreparedFeedTransaction::new(vec![FeedRecord::Causal(envelope)])?;
        Ok(Self {
            dto,
            identity,
            transaction,
        })
    }
    pub fn identity(&self) -> &OperationIdentity {
        &self.identity
    }
    pub fn encoded_bytes_bound(&self) -> usize {
        self.transaction.encoded_bytes_bound()
    }
    pub fn try_admit(
        self,
        handle: &BatchFeedHandle,
    ) -> Result<PendingBootstrap, RejectedBootstrap> {
        let Self {
            dto,
            identity,
            transaction,
        } = self;
        match handle.try_submit(transaction) {
            Ok(ticket) => Ok(PendingBootstrap {
                dto,
                identity,
                ticket,
                handle: handle.clone(),
            }),
            Err(rejected) => Err(RejectedBootstrap {
                reason: rejected.reason,
                prepared: Box::new(Self {
                    dto,
                    identity,
                    transaction: rejected.transaction,
                }),
            }),
        }
    }
}
#[derive(Debug)]
pub struct RejectedBootstrap {
    pub reason: AdmissionFailure,
    prepared: Box<PreparedBootstrap>,
}
impl RejectedBootstrap {
    pub fn into_prepared(self) -> PreparedBootstrap {
        *self.prepared
    }
}
#[derive(Debug)]
pub struct PendingBootstrap {
    dto: BootstrapDto,
    identity: OperationIdentity,
    ticket: CommitTicket,
    handle: BatchFeedHandle,
}
impl PendingBootstrap {
    /// Blocking dedicated-owner wait. Dropping this does not cancel accepted
    /// storage work. No factory can be constructed through this pending token.
    pub fn wait(self) -> Result<CommittedBootstrap, BootstrapError> {
        let admission = self.ticket.admission();
        let receipt = self.ticket.wait()?;
        if receipt.admission != admission
            || receipt.first_sequence != receipt.last_sequence
            || receipt.published.sequence < receipt.last_sequence
        {
            return Err(BootstrapError::Unsupported(
                "mismatched bootstrap commit receipt",
            ));
        }
        Ok(CommittedBootstrap {
            dto: self.dto,
            identity: self.identity,
            receipt,
            handle: self.handle,
        })
    }
}
#[derive(Debug)]
pub struct CommittedBootstrap {
    dto: BootstrapDto,
    identity: OperationIdentity,
    receipt: CommitReceipt,
    handle: BatchFeedHandle,
}
impl CommittedBootstrap {
    pub fn receipt(&self) -> &CommitReceipt {
        &self.receipt
    }
    pub fn instantiate(self) -> Result<BootstrappedSession, BootstrapError> {
        if self.dto.build != current_build()? {
            return Err(BootstrapError::Unsupported(
                "artifact changed while awaiting bootstrap commit",
            ));
        }
        let FactoryDto::BybitPublic { enable_l2, .. } = &self.dto.factory;
        let factory = BybitFactory {
            category: category(&self.dto.factory),
            enable_l2: *enable_l2,
        };
        let machine = factory
            .create_session(
                self.dto.session.session_spec()?,
                self.dto.catalog.catalog()?,
            )
            .map_err(|e| BootstrapError::Factory(e.to_string()))?;
        let capture = CaptureSession::new(machine, self.dto.capture.config()?)
            .map_err(|e| BootstrapError::Factory(e.to_string()))?;
        let history_policy = if self
            .dto
            .public_inputs
            .iter()
            .any(|p| p.role == HISTORY_RUNTIME_POLICY_ROLE)
        {
            Some(self.dto.history_policy()?)
        } else {
            None
        };
        Ok(BootstrappedSession {
            capture,
            identity: self.identity,
            receipt: self.receipt,
            handle: self.handle,
            owner_policy: self.dto.owner_policy,
            history_policy,
        })
    }
}
/// Opaque initial owner. No mutable machine, state, metrics/readiness, stop,
/// books or output handles escape. Operation admission/release is the next layer.
pub struct BootstrappedSession {
    capture: CaptureSession,
    identity: OperationIdentity,
    receipt: CommitReceipt,
    handle: BatchFeedHandle,
    owner_policy: Option<JournalOwnerPolicyDto>,
    history_policy: Option<HistoryRuntimePolicyDto>,
}
impl BootstrappedSession {
    pub fn identity(&self) -> &OperationIdentity {
        &self.identity
    }
    pub fn bootstrap_receipt(&self) -> &CommitReceipt {
        &self.receipt
    }
    pub fn is_poisoned(&self) -> bool {
        self.capture.is_poisoned()
    }
}

#[cfg(all(test, feature = "causal-journal"))]
#[path = "causal_bootstrap_tests.rs"]
mod tests;

#[cfg(all(test, not(feature = "causal-journal")))]
#[test]
fn disabled_profile_returns_explicit_unsupported_without_parsing_manifest() {
    assert!(matches!(
        current_build(),
        Err(BootstrapError::Unsupported(
            "causal-journal feature disabled"
        ))
    ));
}
