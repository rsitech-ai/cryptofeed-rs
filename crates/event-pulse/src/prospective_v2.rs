//! Checked admission for the accepted prospective E2 topology and wire V2.

use std::collections::BTreeMap;

use serde::Deserialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::wire::{
    ClockSourceKeyV1, ConfiguredTargetKeyV1, ConnectionKeyV1, ContributorKeyV1, ContributorRoleV1,
    ContributorSpecV1, CoverageSourceKeyV1, CursorModeV1, FamilyV1, FaultScopeKindV1,
    InstrumentIdentityV1, MechanicsConfigV1, Rfc3339Time, SystemSourceKeyV1, parse_unique_json,
};

const TOPOLOGY_BYTES: &[u8] =
    include_bytes!("../contracts/prospective/event-pulse-e2-producer-evidence-freeze-v2.json");
const WIRE_CONTRACT_BYTES: &[u8] =
    include_bytes!("../contracts/prospective/event-pulse-e2-wire-admission-v2-contract.json");
const TOPOLOGY_SHA256: &str = "7216d9c5bc4b5bcd463b644c53309594413608586288beb0d32623412a42f0d7";
const WIRE_CONTRACT_SHA256: &str =
    "dc79576062caf952be44e4808359c4328c2976282291838973d4884fadafa50b";
const WIRE_MERGED_AT: &str = "2026-08-23T08:10:48Z";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ProspectiveAdmissionErrorV2 {
    #[error("prospective admission/2.0 shape is invalid")]
    Shape,
    #[error("prospective admission/2.0 root binding is invalid")]
    RootBinding,
    #[error("capture start must be canonical UTC and strictly after both root bindings")]
    CaptureTiming,
    #[error("prospective admission authority exceeds the frozen false ceiling")]
    Authority,
    #[error("embedded prospective root contract pin is invalid")]
    EmbeddedContract,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootBindingV2 {
    repository_url: String,
    merge_commit: String,
    merged_at: String,
    path: String,
    byte_length: u64,
    sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorityV2 {
    allocation_allowed: bool,
    canary_allowed: bool,
    capture_allowed: bool,
    credentials_allowed: bool,
    evidence_authoring_allowed: bool,
    execution_allowed: bool,
    live_allowed: bool,
    orders_allowed: bool,
    paper_allowed: bool,
    private_endpoints_allowed: bool,
    promotion_allowed: bool,
    risk_allowed: bool,
}

impl AuthorityV2 {
    fn all_false(&self) -> bool {
        !self.allocation_allowed
            && !self.canary_allowed
            && !self.capture_allowed
            && !self.credentials_allowed
            && !self.evidence_authoring_allowed
            && !self.execution_allowed
            && !self.live_allowed
            && !self.orders_allowed
            && !self.paper_allowed
            && !self.private_endpoints_allowed
            && !self.promotion_allowed
            && !self.risk_allowed
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAdmissionV2 {
    schema: String,
    topology_binding: RootBindingV2,
    wire_contract_binding: RootBindingV2,
    capture_starts_at: String,
    evidence_claim: String,
    source_qualification: String,
    authority: AuthorityV2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProspectiveCaptureAdmissionV2 {
    capture_starts_at: Rfc3339Time,
    mechanics_config: MechanicsConfigV1,
    binding_fingerprint: String,
}

impl ProspectiveCaptureAdmissionV2 {
    pub fn from_json(bytes: &[u8]) -> Result<Self, ProspectiveAdmissionErrorV2> {
        verify_embedded_contracts()?;
        let value = parse_unique_json(bytes).map_err(|_| ProspectiveAdmissionErrorV2::Shape)?;
        if serde_json::to_vec(&value).map_err(|_| ProspectiveAdmissionErrorV2::Shape)? != bytes {
            return Err(ProspectiveAdmissionErrorV2::Shape);
        }
        let raw: RawAdmissionV2 =
            serde_json::from_value(value).map_err(|_| ProspectiveAdmissionErrorV2::Shape)?;
        if raw.schema != "event-pulse-e2-prospective-admission/2.0"
            || raw.evidence_claim != "PROSPECTIVE_CAUSAL_CAPTURE"
            || raw.source_qualification != "UNVERIFIED"
        {
            return Err(ProspectiveAdmissionErrorV2::Shape);
        }
        if raw.topology_binding != expected_topology_binding()
            || raw.wire_contract_binding != expected_wire_binding()
        {
            return Err(ProspectiveAdmissionErrorV2::RootBinding);
        }
        if !raw.authority.all_false() {
            return Err(ProspectiveAdmissionErrorV2::Authority);
        }
        let starts_at = Rfc3339Time::parse(&raw.capture_starts_at)
            .map_err(|_| ProspectiveAdmissionErrorV2::CaptureTiming)?;
        let topology_time = Rfc3339Time::parse(&raw.topology_binding.merged_at)
            .map_err(|_| ProspectiveAdmissionErrorV2::RootBinding)?;
        let wire_time = Rfc3339Time::parse(&raw.wire_contract_binding.merged_at)
            .map_err(|_| ProspectiveAdmissionErrorV2::RootBinding)?;
        if !raw.capture_starts_at.ends_with('Z')
            || starts_at.canonical() != raw.capture_starts_at
            || starts_at <= topology_time
            || starts_at <= wire_time
        {
            return Err(ProspectiveAdmissionErrorV2::CaptureTiming);
        }
        let binding_fingerprint = format!("{:x}", Sha256::digest(bytes));
        Ok(Self {
            capture_starts_at: starts_at,
            mechanics_config: fixed_mechanics_config()?,
            binding_fingerprint,
        })
    }

    pub fn capture_starts_at(&self) -> &Rfc3339Time {
        &self.capture_starts_at
    }

    pub fn mechanics_config(&self) -> &MechanicsConfigV1 {
        &self.mechanics_config
    }

    pub const fn unique_non_system_source_count(&self) -> usize {
        12
    }

    pub const fn evidence_authoring_allowed(&self) -> bool {
        false
    }

    pub const fn blocker(&self) -> &'static str {
        "blocked:fixture-provenance"
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProspectiveSystemArtifactPolicyV2 {
    admission_fingerprint: String,
}

impl ProspectiveSystemArtifactPolicyV2 {
    pub fn from_admission(
        admission: &ProspectiveCaptureAdmissionV2,
    ) -> Result<Self, ProspectiveAdmissionErrorV2> {
        verify_embedded_contracts()?;
        let system = admission.mechanics_config.system_sources();
        if system.len() != 1
            || system[0].source_id() != "capture_system"
            || system[0].scope_kind() != FaultScopeKindV1::Processor
            || system[0].cursor_mode() != CursorModeV1::Derived
            || system[0].configured_target_key().processor_id()
                != Some(admission.mechanics_config.processor_id())
        {
            return Err(ProspectiveAdmissionErrorV2::EmbeddedContract);
        }
        Ok(Self {
            admission_fingerprint: admission.binding_fingerprint.clone(),
        })
    }

    pub const fn mode(&self) -> &'static str {
        "TRUTHFUL_EMPTY"
    }

    pub const fn evidence_authoring_allowed(&self) -> bool {
        false
    }

    pub(crate) fn matches(&self, admission: &ProspectiveCaptureAdmissionV2) -> bool {
        self.admission_fingerprint == admission.binding_fingerprint
    }

    /// Returns whether this non-forgeable policy was minted for `admission`.
    pub fn matches_admission(&self, admission: &ProspectiveCaptureAdmissionV2) -> bool {
        self.matches(admission)
    }
}

fn verify_embedded_contracts() -> Result<(), ProspectiveAdmissionErrorV2> {
    if TOPOLOGY_BYTES.len() != 6_955
        || WIRE_CONTRACT_BYTES.len() != 10_119
        || format!("{:x}", Sha256::digest(TOPOLOGY_BYTES)) != TOPOLOGY_SHA256
        || format!("{:x}", Sha256::digest(WIRE_CONTRACT_BYTES)) != WIRE_CONTRACT_SHA256
    {
        return Err(ProspectiveAdmissionErrorV2::EmbeddedContract);
    }
    Ok(())
}

fn expected_topology_binding() -> RootBindingV2 {
    RootBindingV2 {
        repository_url: "https://github.com/s1korrrr/rsibot.git".to_owned(),
        merge_commit: "05994ccd514ddb69fdd5c21a8c78af8bbe75d506".to_owned(),
        merged_at: "2026-08-23T06:58:18Z".to_owned(),
        path: "docs/superpowers/specs/event-pulse-e2-producer-evidence-freeze-v2.json".to_owned(),
        byte_length: 6_955,
        sha256: TOPOLOGY_SHA256.to_owned(),
    }
}

fn expected_wire_binding() -> RootBindingV2 {
    RootBindingV2 {
        repository_url: "https://github.com/s1korrrr/rsibot.git".to_owned(),
        merge_commit: "44f3e091cb47c1b081f673e8bb09e8723a2090c6".to_owned(),
        merged_at: WIRE_MERGED_AT.to_owned(),
        path: "docs/superpowers/specs/event-pulse-e2-wire-admission-v2-contract.json".to_owned(),
        byte_length: 10_119,
        sha256: WIRE_CONTRACT_SHA256.to_owned(),
    }
}

fn fixed_mechanics_config() -> Result<MechanicsConfigV1, ProspectiveAdmissionErrorV2> {
    let invalid = |_| ProspectiveAdmissionErrorV2::EmbeddedContract;
    let binance = InstrumentIdentityV1::new("BNB", "USDT", "PERPETUAL", "BINANCE", "BNBUSDT")
        .map_err(invalid)?;
    let hyperliquid = InstrumentIdentityV1::new("BNB", "USDT", "PERPETUAL", "HYPERLIQUID", "BNB")
        .map_err(invalid)?;
    let public =
        ContributorKeyV1::new("binance_primary_public", binance.clone()).map_err(invalid)?;
    let market = ContributorKeyV1::new("binance_primary_market", binance).map_err(invalid)?;
    let confirmation =
        ContributorKeyV1::new("hyperliquid_confirmation", hyperliquid).map_err(invalid)?;
    let public_connection =
        ConnectionKeyV1::new("binance_primary_public_connection").map_err(invalid)?;
    let market_connection =
        ConnectionKeyV1::new("binance_primary_market_connection").map_err(invalid)?;
    let confirmation_connection =
        ConnectionKeyV1::new("hyperliquid_confirmation_connection").map_err(invalid)?;
    let contributors = vec![
        ContributorSpecV1::new(
            public.clone(),
            ContributorRoleV1::Primary,
            [FamilyV1::Quote, FamilyV1::Book],
        )
        .map_err(invalid)?,
        ContributorSpecV1::new(
            market.clone(),
            ContributorRoleV1::Primary,
            [
                FamilyV1::Trade,
                FamilyV1::OpenInterest,
                FamilyV1::Liquidation,
            ],
        )
        .map_err(invalid)?,
        ContributorSpecV1::new(
            confirmation.clone(),
            ContributorRoleV1::Confirmation,
            [FamilyV1::ConfirmationPrice],
        )
        .map_err(invalid)?,
    ];
    let clocks = [
        ("clock_binance_public", public.clone()),
        ("clock_binance_market", market.clone()),
        ("clock_hyperliquid_confirmation", confirmation.clone()),
    ]
    .into_iter()
    .map(|(source, subject)| ClockSourceKeyV1::new(source, subject).map_err(invalid))
    .collect::<Result<Vec<_>, _>>()?;
    let coverage = [
        (
            "coverage_binance_public_quote",
            public.clone(),
            FamilyV1::Quote,
        ),
        (
            "coverage_binance_public_book",
            public.clone(),
            FamilyV1::Book,
        ),
        (
            "coverage_binance_market_trade",
            market.clone(),
            FamilyV1::Trade,
        ),
        (
            "coverage_binance_market_open_interest",
            market.clone(),
            FamilyV1::OpenInterest,
        ),
        (
            "coverage_binance_market_liquidation",
            market.clone(),
            FamilyV1::Liquidation,
        ),
        (
            "coverage_hyperliquid_confirmation",
            confirmation.clone(),
            FamilyV1::ConfirmationPrice,
        ),
    ]
    .into_iter()
    .map(|(source, subject, family)| {
        CoverageSourceKeyV1::new(source, subject, family).map_err(invalid)
    })
    .collect::<Result<Vec<_>, _>>()?;
    let system = SystemSourceKeyV1::new(
        "capture_system",
        FaultScopeKindV1::Processor,
        ConfiguredTargetKeyV1::processor("event_pulse_e2_prospective").map_err(invalid)?,
        CursorModeV1::Derived,
    )
    .map_err(invalid)?;
    MechanicsConfigV1::new(
        "event_pulse_e2_prospective",
        vec![
            public_connection.clone(),
            market_connection.clone(),
            confirmation_connection.clone(),
        ],
        contributors,
        BTreeMap::from([
            (public, public_connection),
            (market, market_connection),
            (confirmation, confirmation_connection),
        ]),
        clocks,
        coverage,
        vec![system],
    )
    .map_err(invalid)
}
