use std::collections::BTreeSet;

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::provenance::verify_embedded_contracts;

const Q1: &str = "quant-harness/1.0";
const E1: &str = "event-pulse/1.0";

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ContractError {
    #[error("contract bytes are not valid JSON: {0}")]
    Json(String),
    #[error("contract structure is invalid: {0}")]
    Structure(&'static str),
    #[error("contract semantic validation failed: {0}")]
    Semantic(&'static str),
    #[error("contract content hash does not match canonical v1 payload")]
    HashMismatch,
    #[error("EventPulse semantic category: {0}")]
    EventPulse(EventPulseErrorCode),
    #[error("embedded contract provenance failed: {0}")]
    Provenance(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventPulseErrorCode {
    Identity,
    NumericBounds,
    Ordering,
    Quality,
    ContextRevision,
    HashBinding,
    InputAvailability,
    FutureAvailability,
}

impl std::fmt::Display for EventPulseErrorCode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Identity => "EVENTPULSE_IDENTITY",
            Self::NumericBounds => "EVENTPULSE_NUMERIC_BOUNDS",
            Self::Ordering => "EVENTPULSE_ORDERING",
            Self::Quality => "EVENTPULSE_QUALITY",
            Self::ContextRevision => "EVENTPULSE_CONTEXT_REVISION",
            Self::HashBinding => "EVENTPULSE_HASH_BINDING",
            Self::InputAvailability => "EVENTPULSE_INPUT_AVAILABILITY",
            Self::FutureAvailability => "FUTURE_AVAILABILITY",
        })
    }
}

fn ep(code: EventPulseErrorCode) -> ContractError {
    ContractError::EventPulse(code)
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedContract(Value);

impl ValidatedContract {
    pub fn canonical_json(&self) -> String {
        canonical_json(&self.0)
    }

    pub fn content_hash(&self) -> String {
        try_content_hash(&self.0).expect("validated contract is an object")
    }

    pub fn value(&self) -> &Value {
        &self.0
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ContractBundle(PrivateBundle);
#[derive(Debug, Clone, Copy)]
struct PrivateBundle;

impl ContractBundle {
    pub fn load_embedded() -> Result<Self, ContractError> {
        verify_embedded_contracts()
            .map_err(|error| ContractError::Provenance(error.to_string()))?;
        Ok(Self(PrivateBundle))
    }

    pub fn validate_q1_json(&self, bytes: &[u8]) -> Result<ValidatedContract, ContractError> {
        validate(bytes, Q1)
    }

    pub fn validate_e1_json(&self, bytes: &[u8]) -> Result<ValidatedContract, ContractError> {
        validate(bytes, E1)
    }

    pub fn validate_json(&self, bytes: &[u8]) -> Result<ValidatedContract, ContractError> {
        let value = parse(bytes)?;
        let version = string_field(object(&value)?, "schema_version")?;
        match version {
            Q1 => validate_value(value, Q1),
            E1 => validate_value(value, E1),
            _ => Err(ContractError::Structure("unsupported schema_version")),
        }
    }

    pub fn bind_composite(
        &self,
        mechanics: &ValidatedContract,
        context: Option<&ValidatedContract>,
        composite: &ValidatedContract,
    ) -> Result<(), ContractError> {
        let mechanics = object(mechanics.value())?;
        let composite = object(composite.value())?;
        if string_field(mechanics, "contract_type")? != "mechanics"
            || string_field(composite, "contract_type")? != "composite"
        {
            return Err(ep(EventPulseErrorCode::HashBinding));
        }
        if composite
            .get("mechanics_content_hash")
            .and_then(Value::as_str)
            != Some(string_field(mechanics, "content_hash")?)
            || composite.get("mechanics_lineage_id") != mechanics.get("lineage_id")
        {
            return Err(ep(EventPulseErrorCode::HashBinding));
        }
        if composite.get("event_cluster_id") != mechanics.get("event_cluster_id")
            || composite.get("scope") != mechanics.get("scope")
        {
            return Err(ep(EventPulseErrorCode::Identity));
        }
        match context {
            None if !composite
                .get("context_content_hash")
                .is_some_and(Value::is_null)
                || !composite
                    .get("context_lineage_id")
                    .is_some_and(Value::is_null)
                || !composite
                    .get("catalyst_confidence")
                    .is_some_and(Value::is_null) =>
            {
                return Err(ep(EventPulseErrorCode::HashBinding));
            }
            Some(context) => {
                let context = object(context.value())?;
                if string_field(context, "contract_type")? != "context"
                    || composite
                        .get("context_content_hash")
                        .and_then(Value::as_str)
                        != Some(string_field(context, "content_hash")?)
                    || composite.get("context_lineage_id") != context.get("lineage_id")
                {
                    return Err(ep(EventPulseErrorCode::HashBinding));
                }
                if composite.get("event_cluster_id") != context.get("event_cluster_id")
                    || composite.get("scope") != context.get("scope")
                {
                    return Err(ep(EventPulseErrorCode::Identity));
                }
            }
            _ => {}
        }
        for field_name in [
            "phase",
            "event_type",
            "direction",
            "mechanical_intensity",
            "mechanical_confidence",
            "reversal_risk",
            "expected_half_life_ms",
        ] {
            if composite.get(field_name) != mechanics.get(field_name) {
                return Err(ep(EventPulseErrorCode::HashBinding));
            }
        }
        let composite_time = object(field(composite, "causal_time")?)?;
        let composite_available = parse_time(string_field(composite_time, "available_at")?)?;
        let composite_received = parse_time(string_field(composite_time, "received_at")?)?;
        let composite_normalized = parse_time(string_field(composite_time, "normalized_at")?)?;
        for input in [
            Some(mechanics),
            context.map(|value| object(value.value())).transpose()?,
        ]
        .into_iter()
        .flatten()
        {
            let input_available = parse_time(string_field(
                object(field(input, "causal_time")?)?,
                "available_at",
            )?)?;
            if input_available > composite_available
                || input_available > composite_received
                || input_available > composite_normalized
            {
                return Err(ep(EventPulseErrorCode::InputAvailability));
            }
        }
        let mut expected_flags = BTreeSet::new();
        for input in [
            Some(mechanics),
            context.map(|value| object(value.value())).transpose()?,
        ]
        .into_iter()
        .flatten()
        {
            for flag in array_field(input, "quality_flags")? {
                expected_flags.insert(
                    flag.as_str()
                        .ok_or(ep(EventPulseErrorCode::Quality))?
                        .to_owned(),
                );
            }
        }
        let actual_flags: Vec<_> = array_field(composite, "quality_flags")?
            .iter()
            .map(|value| value.as_str().ok_or(ep(EventPulseErrorCode::Quality)))
            .collect::<Result<_, _>>()?;
        if actual_flags
            != expected_flags
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        {
            return Err(ep(EventPulseErrorCode::Quality));
        }
        if let Some(context) = context {
            if composite.get("catalyst_confidence")
                != object(context.value())?.get("catalyst_confidence")
            {
                return Err(ep(EventPulseErrorCode::HashBinding));
            }
        }
        let expires = parse_time(string_field(composite, "expires_at")?)?;
        let half_life_micros = integer_field(mechanics, "expected_half_life_ms")?
            .checked_mul(1_000)
            .ok_or(ep(EventPulseErrorCode::InputAvailability))?;
        if expires != composite_available + half_life_micros {
            return Err(ep(EventPulseErrorCode::InputAvailability));
        }
        Ok(())
    }
}

pub fn validate_revision_transition(
    previous: &ValidatedContract,
    current: &ValidatedContract,
) -> Result<(), ContractError> {
    let previous = object(&previous.0)?;
    let current = object(&current.0)?;
    if string_field(previous, "content_hash")? == string_field(current, "content_hash")? {
        return Ok(());
    }
    if integer_field(current, "revision")? == integer_field(previous, "revision")? {
        return Err(ContractError::Semantic("post-hoc mutation"));
    }
    for key in [
        "schema_version",
        "contract_type",
        "contract_id",
        "lineage_id",
    ] {
        if field(previous, key)? != field(current, key)? {
            return Err(ContractError::Semantic("revision identity mismatch"));
        }
    }
    if previous.get("scope") != current.get("scope") {
        return if string_field(previous, "schema_version")? == E1 {
            Err(ep(EventPulseErrorCode::Identity))
        } else {
            Err(ContractError::Semantic("revision scope mismatch"))
        };
    }
    let previous_available = parse_time(string_field(
        object(field(previous, "causal_time")?)?,
        "available_at",
    )?)?;
    let current_available = parse_time(string_field(
        object(field(current, "causal_time")?)?,
        "available_at",
    )?)?;
    if current_available < previous_available {
        return Err(ContractError::Semantic("revision availability regression"));
    }
    if integer_field(current, "revision")? != integer_field(previous, "revision")? + 1
        || current
            .get("predecessor_content_hash")
            .and_then(Value::as_str)
            != Some(string_field(previous, "content_hash")?)
    {
        return Err(ContractError::Semantic("revision predecessor"));
    }
    Ok(())
}

pub fn validate_context_revision(
    previous: &ValidatedContract,
    current: &ValidatedContract,
) -> Result<(), ContractError> {
    validate_revision_transition(previous, current)?;
    let previous_evidence = array_field(object(&previous.0)?, "evidence")?;
    let current_evidence = array_field(object(&current.0)?, "evidence")?;
    if current_evidence.len() < previous_evidence.len()
        || current_evidence[..previous_evidence.len()] != previous_evidence[..]
    {
        return Err(ep(EventPulseErrorCode::ContextRevision));
    }
    let previous_available = parse_time(string_field(
        object(field(object(&previous.0)?, "causal_time")?)?,
        "available_at",
    )?)?;
    for item in &current_evidence[previous_evidence.len()..] {
        if parse_time(string_field(object(item)?, "first_seen_at")?)? < previous_available {
            return Err(ep(EventPulseErrorCode::ContextRevision));
        }
    }
    Ok(())
}

/// Enforces the narrower E2-authored mechanics profile without narrowing the
/// accepted E1 wire contract (whose published golden intentionally has one row).
pub fn validate_e2_mechanics_profile(contract: &ValidatedContract) -> Result<(), ContractError> {
    let obj = object(contract.value())?;
    if string_field(obj, "contract_type")? != "mechanics"
        || integer_field(obj, "expected_half_life_ms")? != 15_000
    {
        return Err(ContractError::Semantic("not the E2 mechanics profile"));
    }
    const ROWS: [(&str, i128, &str); 9] = [
        ("book_depth_10bps", 250, "USDC"),
        ("cross_venue_breadth", 1_000, "RATIO"),
        ("cvd_slope", 1_000, "BASE_PER_SECOND"),
        ("liquidation_notional", 5_000, "USDC"),
        ("log_return", 1_000, "LOG_RETURN"),
        ("open_interest_change", 5_000, "CONTRACTS"),
        ("reversal_from_extreme", 5_000, "RATIO"),
        ("spread_bps", 250, "BPS"),
        ("taker_imbalance", 1_000, "RATIO"),
    ];
    let features = array_field(obj, "features")?;
    if features.len() != ROWS.len() {
        return Err(ContractError::Semantic("E2 requires exactly nine features"));
    }
    const REASONS: [&str; 11] = [
        "BOOK_RESYNCING",
        "CLOCK_DEGRADED",
        "DIRECTION_UNKNOWN",
        "INSUFFICIENT_COVERAGE",
        "INSUFFICIENT_SAMPLES",
        "OBSERVATION_VALID",
        "OPTIONAL_SOURCE_UNAVAILABLE",
        "OUT_OF_DOMAIN",
        "RECONNECT_WARMUP",
        "SOURCE_INVALIDATED",
        "SOURCE_STALE",
    ];
    for (feature, expected) in features.iter().zip(ROWS) {
        let feature = object(feature)?;
        if string_field(feature, "name")? != expected.0
            || integer_field(feature, "horizon_ms")? != expected.1
            || string_field(feature, "unit")? != expected.2
            || !REASONS.contains(&string_field(feature, "reason_code")?)
        {
            return Err(ContractError::Semantic("invalid E2 feature profile"));
        }
    }
    Ok(())
}

pub fn canonical_json(value: &Value) -> String {
    let normalized = normalize_temporal_strings(value.clone()).unwrap_or_else(|_| value.clone());
    serde_json::to_string(&normalized).expect("serde JSON values serialize")
}

/// Fallible, non-panicking canonical hash API for untrusted values.
pub fn content_hash(value: &Value) -> Result<String, ContractError> {
    let mut preimage = value.clone();
    preimage
        .as_object_mut()
        .ok_or(ContractError::Structure("hash payload must be an object"))?
        .remove("content_hash");
    let preimage = normalize_temporal_strings(preimage)?;
    Ok(format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_string(&preimage)
                .map_err(|error| ContractError::Json(error.to_string()))?
                .as_bytes()
        )
    ))
}

pub use content_hash as try_content_hash;

fn validate(bytes: &[u8], expected_version: &str) -> Result<ValidatedContract, ContractError> {
    let value = parse(bytes)?;
    validate_value(value, expected_version)
}

fn parse(bytes: &[u8]) -> Result<Value, ContractError> {
    if bytes.len() > 16 * 1024 * 1024 {
        return Err(ContractError::Structure("input exceeds 16 MiB"));
    }
    serde_json::from_slice(bytes).map_err(|error| ContractError::Json(error.to_string()))
}

fn validate_value(
    value: Value,
    expected_version: &str,
) -> Result<ValidatedContract, ContractError> {
    reject_floats(&value)?;
    let obj = object(&value)?;
    if string_field(obj, "schema_version")? != expected_version {
        return Err(ContractError::Structure("wrong schema_version"));
    }
    validate_common(obj, expected_version)?;
    match expected_version {
        Q1 => validate_q1(obj)?,
        E1 => validate_e1(obj)?,
        _ => unreachable!(),
    }
    let provided = string_field(obj, "content_hash")?;
    if !is_hash(provided) || content_hash(&value)? != provided {
        return Err(ContractError::HashMismatch);
    }
    Ok(ValidatedContract(value))
}

fn validate_common(obj: &Map<String, Value>, schema_version: &str) -> Result<(), ContractError> {
    for key in [
        "schema_version",
        "contract_id",
        "contract_type",
        "lineage_id",
        "revision",
        "predecessor_content_hash",
        "content_hash",
        "causal_time",
    ] {
        field(obj, key)?;
    }
    if !is_id(string_field(obj, "lineage_id")?, "lineage_")
        || !is_hash(string_field(obj, "content_hash")?)
    {
        return Err(ContractError::Semantic("invalid Q1 identity"));
    }
    let revision = integer_field(obj, "revision")?;
    if revision < 1 {
        return Err(ContractError::Semantic("revision must be positive"));
    }
    match obj.get("predecessor_content_hash") {
        Some(Value::Null) if revision == 1 => {}
        Some(Value::String(value)) if revision > 1 && is_hash(value) => {}
        _ => return Err(ContractError::Semantic("invalid revision predecessor")),
    }
    validate_causal_time(object(field(obj, "causal_time")?)?, schema_version)
}

fn validate_q1(obj: &Map<String, Value>) -> Result<(), ContractError> {
    match string_field(obj, "contract_type")? {
        "evidence" => {
            require_exact_keys(
                obj,
                ROOT_FIELDS,
                &["evidence_kind", "source_payload_hash", "measurements"],
            )?;
            if !is_id(string_field(obj, "contract_id")?, "evidence_")
                || !is_hash(string_field(obj, "source_payload_hash")?)
                || string_field(obj, "evidence_kind")?.trim().is_empty()
                || array_field(obj, "measurements")?.is_empty()
            {
                return Err(ContractError::Semantic("invalid evidence identity"));
            }
            for value in array_field(obj, "measurements")? {
                let item = object(value)?;
                require_exact_keys(item, &["name", "value", "unit", "price_reference"], &[])?;
                if !is_snake(string_field(item, "name")?)
                    || !is_unit(string_field(item, "unit")?)
                    || !matches!(
                        string_field(item, "price_reference")?,
                        "trade" | "mid" | "bid" | "ask" | "mark" | "index" | "not_applicable"
                    )
                {
                    return Err(ContractError::Semantic("invalid measurement"));
                }
                decimal_unbounded(string_field(item, "value")?)?;
            }
        }
        "proposal_request" => {
            require_exact_keys(
                obj,
                ROOT_FIELDS,
                &[
                    "proposal_kind",
                    "requested_capability",
                    "requester_id",
                    "evidence_content_hashes",
                ],
            )?;
            if !is_id(string_field(obj, "contract_id")?, "proposal_request_")
                || string_field(obj, "requested_capability")? != "evaluate_only"
                || string_field(obj, "proposal_kind")?.trim().is_empty()
                || string_field(obj, "requester_id")?.trim().is_empty()
            {
                return Err(ContractError::Semantic("invalid proposal request"));
            }
            unique_hashes(array_field(obj, "evidence_content_hashes")?)?;
        }
        "risk_decision" => {
            require_exact_keys(
                obj,
                ROOT_FIELDS,
                &[
                    "proposal_request_content_hash",
                    "issuer",
                    "outcome",
                    "reason_codes",
                    "evidence_content_hashes",
                ],
            )?;
            if !is_id(string_field(obj, "contract_id")?, "risk_decision_")
                || string_field(obj, "issuer")? != "research_os_risk_governor"
                || !is_hash(string_field(obj, "proposal_request_content_hash")?)
                || !matches!(string_field(obj, "outcome")?, "allow" | "deny" | "hold")
            {
                return Err(ContractError::Semantic("invalid risk identity"));
            }
            let reasons = array_field(obj, "reason_codes")?;
            if reasons.is_empty() {
                return Err(ContractError::Semantic("invalid risk reason codes"));
            }
            let mut unique = BTreeSet::new();
            for reason in reasons {
                let reason = reason
                    .as_str()
                    .ok_or(ContractError::Structure("risk reason must be a string"))?;
                if reason.trim().is_empty() || !unique.insert(reason) {
                    return Err(ContractError::Semantic("invalid risk reason codes"));
                }
            }
            unique_hashes(array_field(obj, "evidence_content_hashes")?)?;
        }
        _ => return Err(ContractError::Structure("unknown Q1 contract_type")),
    }
    Ok(())
}

fn validate_e1(obj: &Map<String, Value>) -> Result<(), ContractError> {
    for key in ["producer", "event_cluster_id", "scope"] {
        field(obj, key)?;
    }
    if !is_id(string_field(obj, "event_cluster_id")?, "event_cluster_") {
        return Err(ContractError::Semantic("invalid event cluster"));
    }
    validate_scope(object(field(obj, "scope")?)?)?;
    match string_field(obj, "contract_type")? {
        "mechanics" => validate_mechanics(obj),
        "context" => validate_context(obj),
        "composite" => validate_composite(obj),
        _ => Err(ContractError::Structure("unknown E1 contract_type")),
    }
}

fn validate_mechanics(obj: &Map<String, Value>) -> Result<(), ContractError> {
    require_exact_keys(
        obj,
        ROOT_FIELDS,
        &[
            "producer",
            "event_cluster_id",
            "scope",
            "phase",
            "event_type",
            "direction",
            "mechanical_intensity",
            "mechanical_confidence",
            "reversal_risk",
            "quality_state",
            "source_qualification",
            "quality_flags",
            "expected_half_life_ms",
            "features",
            "source_cursors",
        ],
    )?;
    if string_field(obj, "producer")? != "cryptofeed_rs"
        || !is_id(string_field(obj, "contract_id")?, "event_pulse_mechanics_")
        || string_field(obj, "source_qualification")? != "UNVERIFIED"
    {
        return Err(ContractError::Semantic("invalid mechanics literals"));
    }
    validate_quality_flags(array_field(obj, "quality_flags")?)?;
    let half_life = integer_field(obj, "expected_half_life_ms")?;
    if !(1..=86_400_000).contains(&half_life) {
        return Err(ContractError::Semantic("invalid expected half life"));
    }
    if !matches!(
        string_field(obj, "phase")?,
        "NORMAL" | "BUILDUP" | "IGNITION" | "CASCADE" | "EXHAUSTION" | "AFTERMATH" | "INVALID"
    ) || !matches!(
        string_field(obj, "event_type")?,
        "SHORT_SQUEEZE"
            | "LONG_LIQUIDATION"
            | "DERIVATIVES_LED_BREAKOUT"
            | "SPOT_LED_BREAKOUT"
            | "BOOK_DISLOCATION"
            | "MACRO_RISK_ON"
            | "MACRO_RISK_OFF"
            | "NEWS_SHOCK"
            | "FLOW_SHOCK"
            | "CROSS_ASSET_PROPAGATION"
            | "VOLATILITY_SHOCK"
            | "UNKNOWN"
    ) || !matches!(
        string_field(obj, "direction")?,
        "UP" | "DOWN" | "MIXED" | "UNKNOWN"
    ) || !matches!(
        string_field(obj, "quality_state")?,
        "VALIDATED" | "DEGRADED" | "INVALID" | "UNAVAILABLE"
    ) {
        return Err(ContractError::Semantic("invalid mechanics enum"));
    }
    for name in [
        "mechanical_intensity",
        "mechanical_confidence",
        "reversal_risk",
    ] {
        validate_score(string_field(obj, name)?)
            .map_err(|_| ep(EventPulseErrorCode::NumericBounds))?;
    }
    let availability = parse_time(string_field(
        object(field(obj, "causal_time")?)?,
        "available_at",
    )?)?;
    let mut keys = Vec::new();
    let source_cursors = array_field(obj, "source_cursors")?;
    if source_cursors.is_empty() {
        return Err(ContractError::Semantic("empty source cursors"));
    }
    for item in source_cursors {
        let cursor = object(item)?;
        require_exact_keys(
            cursor,
            &[
                "available_at",
                "connection_epoch",
                "sequence_start",
                "sequence_end",
                "source_id",
                "source_payload_hash",
            ],
            &[],
        )?;
        let at = parse_time(string_field(cursor, "available_at")?)?;
        if at > availability {
            return Err(ep(EventPulseErrorCode::InputAvailability));
        }
        if !is_source(string_field(cursor, "source_id")?)
            || !is_epoch(string_field(cursor, "connection_epoch")?)
            || integer_field(cursor, "sequence_start")? > integer_field(cursor, "sequence_end")?
            || integer_field(cursor, "sequence_end")? > i64::MAX as i128
            || !is_hash(string_field(cursor, "source_payload_hash")?)
        {
            return Err(ContractError::Semantic("invalid source cursor"));
        }
        keys.push((
            at,
            string_field(cursor, "source_id")?.to_owned(),
            string_field(cursor, "connection_epoch")?.to_owned(),
            integer_field(cursor, "sequence_start")?,
            integer_field(cursor, "sequence_end")?,
            string_field(cursor, "source_payload_hash")?.to_owned(),
        ));
    }
    if keys.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ep(EventPulseErrorCode::Ordering));
    }
    let validated = string_field(obj, "quality_state")? == "VALIDATED";
    let unusable = matches!(
        string_field(obj, "quality_state")?,
        "INVALID" | "UNAVAILABLE"
    );
    if (string_field(obj, "phase")? == "INVALID") != unusable
        || (unusable && array_field(obj, "quality_flags")?.is_empty())
    {
        return Err(ep(EventPulseErrorCode::Quality));
    }
    let features = array_field(obj, "features")?;
    if features.is_empty() {
        return Err(ContractError::Semantic("empty mechanics features"));
    }
    let mut feature_keys = Vec::new();
    for item in features {
        let feature = object(item)?;
        require_exact_keys(
            feature,
            &[
                "name",
                "horizon_ms",
                "unit",
                "value",
                "quality_state",
                "reason_code",
            ],
            &[],
        )?;
        let name = string_field(feature, "name")?;
        let unit = string_field(feature, "unit")?;
        let quality = string_field(feature, "quality_state")?;
        let horizon = integer_field(feature, "horizon_ms")?;
        if !matches!(
            quality,
            "VALIDATED" | "DEGRADED" | "INVALID" | "UNAVAILABLE"
        ) || !(1..=86_400_000).contains(&horizon)
            || !is_reason_code(string_field(feature, "reason_code")?)
            || expected_feature_unit(name) != Some(unit)
        {
            return Err(ContractError::Semantic("invalid feature vocabulary"));
        }
        if validated && quality != "VALIDATED" {
            return Err(ep(EventPulseErrorCode::Quality));
        }
        if let Some(Value::String(v)) = feature.get("value") {
            decimal(v, 18, 8)?;
            if matches!(quality, "INVALID" | "UNAVAILABLE") || !feature_value_in_domain(name, v)? {
                return Err(ContractError::Semantic("invalid feature value"));
            }
        } else if !feature.get("value").is_some_and(Value::is_null)
            || !matches!(quality, "INVALID" | "UNAVAILABLE")
        {
            return Err(ContractError::Semantic("feature decimal must be string"));
        }
        feature_keys.push((name, horizon));
    }
    if feature_keys.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ep(EventPulseErrorCode::Ordering));
    }
    Ok(())
}

fn validate_context(obj: &Map<String, Value>) -> Result<(), ContractError> {
    require_exact_keys(
        obj,
        ROOT_FIELDS,
        &[
            "producer",
            "event_cluster_id",
            "scope",
            "attribution_state",
            "catalyst_type",
            "catalyst_confidence",
            "quality_state",
            "source_qualification",
            "quality_flags",
            "evidence",
        ],
    )?;
    if string_field(obj, "producer")? != "hummingbot_api_event_pulse_context"
        || !is_id(string_field(obj, "contract_id")?, "event_pulse_context_")
        || string_field(obj, "source_qualification")? != "UNVERIFIED"
        || !is_event_type(string_field(obj, "catalyst_type")?)
        || !matches!(
            string_field(obj, "attribution_state")?,
            "UNKNOWN" | "CANDIDATE" | "CONFIRMED" | "DISPUTED"
        )
        || !matches!(
            string_field(obj, "quality_state")?,
            "VALIDATED" | "DEGRADED" | "INVALID" | "UNAVAILABLE"
        )
    {
        return Err(ContractError::Semantic("invalid context literals"));
    }
    decimal(string_field(obj, "catalyst_confidence")?, 1, 8)?;
    validate_score(string_field(obj, "catalyst_confidence")?)
        .map_err(|_| ep(EventPulseErrorCode::NumericBounds))?;
    validate_quality_flags(array_field(obj, "quality_flags")?)?;
    let unavailable = matches!(
        string_field(obj, "quality_state")?,
        "INVALID" | "UNAVAILABLE"
    );
    if unavailable && array_field(obj, "quality_flags")?.is_empty() {
        return Err(ep(EventPulseErrorCode::Quality));
    }
    if string_field(obj, "attribution_state")? == "UNKNOWN"
        && (string_field(obj, "catalyst_type")? != "UNKNOWN"
            || !decimal_is_zero(string_field(obj, "catalyst_confidence")?))
    {
        return Err(ep(EventPulseErrorCode::Quality));
    }
    validate_context_evidence(obj)?;
    Ok(())
}

fn validate_composite(obj: &Map<String, Value>) -> Result<(), ContractError> {
    require_exact_keys(
        obj,
        ROOT_FIELDS,
        &[
            "producer",
            "event_cluster_id",
            "scope",
            "mechanics_content_hash",
            "context_content_hash",
            "mechanics_lineage_id",
            "context_lineage_id",
            "phase",
            "event_type",
            "direction",
            "mechanical_intensity",
            "mechanical_confidence",
            "catalyst_confidence",
            "reversal_risk",
            "evidence_state",
            "source_qualification",
            "quality_flags",
            "expected_half_life_ms",
            "expires_at",
            "execution_authority",
            "risk_authority",
            "protective_exit_control",
        ],
    )?;
    if string_field(obj, "producer")? != "hummingbot_api_event_pulse_composite"
        || !is_id(string_field(obj, "contract_id")?, "event_pulse_composite_")
        || obj.get("execution_authority") != Some(&Value::Bool(false))
        || obj.get("risk_authority") != Some(&Value::Bool(false))
        || obj.get("protective_exit_control") != Some(&Value::Bool(false))
    {
        return Err(ContractError::Semantic("invalid composite literals"));
    }
    if string_field(obj, "evidence_state")? != "UNAVAILABLE"
        || string_field(obj, "source_qualification")? != "UNVERIFIED"
    {
        return Err(ep(EventPulseErrorCode::Quality));
    }
    for name in ["mechanics_content_hash"] {
        if !is_hash(string_field(obj, name)?) {
            return Err(ContractError::Semantic("invalid composite hash"));
        }
    }
    validate_quality_flags(array_field(obj, "quality_flags")?)?;
    if !is_event_type(string_field(obj, "event_type")?)
        || !matches!(
            string_field(obj, "phase")?,
            "NORMAL" | "BUILDUP" | "IGNITION" | "CASCADE" | "EXHAUSTION" | "AFTERMATH" | "INVALID"
        )
        || !matches!(
            string_field(obj, "direction")?,
            "UP" | "DOWN" | "MIXED" | "UNKNOWN"
        )
        || !(1..=86_400_000).contains(&integer_field(obj, "expected_half_life_ms")?)
    {
        return Err(ContractError::Semantic("invalid composite vocabulary"));
    }
    for field_name in [
        "mechanical_intensity",
        "mechanical_confidence",
        "reversal_risk",
    ] {
        validate_score(string_field(obj, field_name)?)?;
    }
    if let Some(Value::String(value)) = obj.get("catalyst_confidence") {
        validate_score(value)?;
    } else if !obj.get("catalyst_confidence").is_some_and(Value::is_null) {
        return Err(ContractError::Structure("invalid catalyst confidence"));
    }
    let context_absent = obj.get("context_content_hash").is_some_and(Value::is_null);
    if context_absent != obj.get("context_lineage_id").is_some_and(Value::is_null)
        || context_absent != obj.get("catalyst_confidence").is_some_and(Value::is_null)
    {
        return Err(ContractError::Semantic("incomplete context binding"));
    }
    if !context_absent
        && (!obj
            .get("context_content_hash")
            .and_then(Value::as_str)
            .is_some_and(is_hash)
            || !obj
                .get("context_lineage_id")
                .and_then(Value::as_str)
                .is_some_and(|v| is_id(v, "lineage_")))
    {
        return Err(ContractError::Semantic("invalid context binding"));
    }
    let available = parse_time(string_field(
        object(field(obj, "causal_time")?)?,
        "available_at",
    )?)?;
    if parse_time(string_field(obj, "expires_at")?)? <= available {
        return Err(ContractError::Semantic("composite expiry"));
    }
    Ok(())
}

fn validate_scope(obj: &Map<String, Value>) -> Result<(), ContractError> {
    match string_field(obj, "kind")? {
        "GLOBAL_CRYPTO" => {
            require_exact_keys(obj, &["kind", "asset", "venue", "instrument"], &[])?;
            if obj.get("asset").is_some_and(Value::is_null)
                && obj.get("venue").is_some_and(Value::is_null)
                && obj.get("instrument").is_some_and(Value::is_null)
            {
                Ok(())
            } else {
                Err(ContractError::Semantic("invalid scope"))
            }
        }
        "ASSET" => {
            require_exact_keys(obj, &["kind", "asset", "venue", "instrument"], &[])?;
            if obj.get("venue").is_some_and(Value::is_null)
                && obj.get("instrument").is_some_and(Value::is_null)
                && obj
                    .get("asset")
                    .and_then(Value::as_str)
                    .is_some_and(is_asset)
            {
                Ok(())
            } else {
                Err(ContractError::Semantic("invalid scope"))
            }
        }
        "VENUE" => {
            require_exact_keys(obj, &["kind", "asset", "venue", "instrument"], &[])?;
            if obj.get("asset").is_some_and(Value::is_null)
                && obj.get("instrument").is_some_and(Value::is_null)
                && obj
                    .get("venue")
                    .and_then(Value::as_str)
                    .is_some_and(is_venue)
            {
                Ok(())
            } else {
                Err(ContractError::Semantic("invalid scope"))
            }
        }
        "PAIR" => {
            require_exact_keys(obj, &["kind", "asset", "venue", "instrument"], &[])?;
            let asset = string_field(obj, "asset")?;
            let venue = string_field(obj, "venue")?;
            if !is_asset(asset) || !is_venue(venue) {
                return Err(ContractError::Semantic("invalid scope"));
            }
            let instrument = object(field(obj, "instrument")?)?;
            require_exact_keys(
                instrument,
                &[
                    "base_asset",
                    "quote_asset",
                    "market_type",
                    "venue",
                    "venue_symbol",
                ],
                &[],
            )?;
            if string_field(instrument, "base_asset")? != asset
                || string_field(instrument, "venue")? != venue
                || !matches!(
                    string_field(instrument, "quote_asset")?,
                    "USD" | "USDC" | "USDT"
                )
                || !matches!(
                    string_field(instrument, "market_type")?,
                    "SPOT" | "PERPETUAL"
                )
                || !is_symbol(string_field(instrument, "venue_symbol")?)
            {
                return Err(ContractError::Semantic("invalid scope"));
            }
            Ok(())
        }
        _ => Err(ContractError::Semantic("invalid scope")),
    }
}
fn validate_causal_time(
    obj: &Map<String, Value>,
    schema_version: &str,
) -> Result<(), ContractError> {
    require_exact_keys(
        obj,
        &[
            "source_event_time",
            "received_at",
            "normalized_at",
            "available_at",
            "decision_time",
            "clock_quality",
        ],
        &[],
    )?;
    let times = [
        "source_event_time",
        "received_at",
        "normalized_at",
        "available_at",
        "decision_time",
    ]
    .map(|key| parse_time(string_field(obj, key)?));
    let times: Result<Vec<_>, _> = times.into_iter().collect();
    let times = times?;
    if times[3] > times[4] {
        return if schema_version == E1 {
            Err(ep(EventPulseErrorCode::FutureAvailability))
        } else {
            Err(ContractError::Semantic("causal time inversion"))
        };
    }
    if !(times[0] <= times[1]
        && times[1] <= times[2]
        && times[2] <= times[3]
        && times[3] <= times[4])
    {
        return Err(ContractError::Semantic("causal time inversion"));
    }
    let clock = object(field(obj, "clock_quality")?)?;
    require_exact_keys(
        clock,
        &[
            "source_id",
            "clock_state",
            "observed_skew_ms",
            "freshness_limit_ms",
            "quality_state",
            "reason_code",
        ],
        &[],
    )?;
    if !matches!(
        string_field(clock, "clock_state")?,
        "synchronized" | "degraded"
    ) || !matches!(
        string_field(clock, "quality_state")?,
        "validated" | "degraded"
    ) {
        return Err(ContractError::Semantic("clock state"));
    }
    if string_field(clock, "source_id")?.trim().is_empty()
        || string_field(clock, "reason_code")?.trim().is_empty()
    {
        return Err(ContractError::Semantic("clock identity"));
    }
    decimal_unbounded(string_field(clock, "observed_skew_ms")?)?;
    let freshness = integer_field(clock, "freshness_limit_ms")?;
    if freshness <= 0 || times[4] - times[0] > freshness * 1_000 {
        return Err(ContractError::Semantic("clock freshness"));
    }
    Ok(())
}
fn parse_time(value: &str) -> Result<i128, ContractError> {
    crate::wire::Rfc3339Time::parse(value)
        .map(|time| i128::from(time.utc_micros()))
        .map_err(|_| ContractError::Semantic("invalid RFC3339 timestamp"))
}
fn decimal(value: &str, max_integer: usize, max_fraction: usize) -> Result<(), ContractError> {
    crate::wire::CanonicalDecimal::parse(value, max_integer, max_fraction)
        .map(|_| ())
        .map_err(|_| ContractError::Semantic("noncanonical decimal string"))
}
fn decimal_unbounded(value: &str) -> Result<(), ContractError> {
    crate::wire::CanonicalDecimal::parse(value, value.len(), value.len())
        .map(|_| ())
        .map_err(|_| ContractError::Semantic("noncanonical decimal string"))
}

fn normalize_temporal_strings(mut value: Value) -> Result<Value, ContractError> {
    fn walk(value: &mut Value, field_name: Option<&str>) -> Result<(), ContractError> {
        match value {
            Value::String(text)
                if matches!(
                    field_name,
                    Some(
                        "source_event_time"
                            | "received_at"
                            | "normalized_at"
                            | "available_at"
                            | "decision_time"
                            | "first_seen_at"
                            | "expires_at"
                            | "covered_from"
                            | "covered_through"
                            | "observed_at"
                            | "occurred_at"
                    )
                ) =>
            {
                *text = crate::wire::Rfc3339Time::parse(text)
                    .map_err(|_| ContractError::Semantic("invalid RFC3339 timestamp"))?
                    .canonical()
                    .to_owned();
            }
            Value::Array(values) => {
                for item in values {
                    walk(item, None)?;
                }
            }
            Value::Object(map) => {
                for (key, item) in map {
                    walk(item, Some(key))?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    walk(&mut value, None)?;
    Ok(value)
}

fn validate_score(value: &str) -> Result<(), ContractError> {
    decimal(value, 1, 8)?;
    let scaled = decimal_scaled(value)?;
    if !(0..=100_000_000).contains(&scaled) {
        return Err(ContractError::Semantic("score outside unit interval"));
    }
    Ok(())
}

fn decimal_scaled(value: &str) -> Result<i128, ContractError> {
    let negative = value.starts_with('-');
    let body = value.strip_prefix('-').unwrap_or(value);
    let (integer, fraction) = body.split_once('.').unwrap_or((body, ""));
    let integer: i128 = integer
        .parse()
        .map_err(|_| ContractError::Semantic("decimal overflow"))?;
    let mut fraction_text = fraction.to_owned();
    while fraction_text.len() < 8 {
        fraction_text.push('0');
    }
    let fraction: i128 = if fraction_text.is_empty() {
        0
    } else {
        fraction_text
            .parse()
            .map_err(|_| ContractError::Semantic("decimal overflow"))?
    };
    let result = integer
        .checked_mul(100_000_000)
        .and_then(|value| value.checked_add(fraction))
        .ok_or(ContractError::Semantic("decimal overflow"))?;
    Ok(if negative { -result } else { result })
}

fn decimal_is_zero(value: &str) -> bool {
    decimal_scaled(value) == Ok(0)
}

fn is_event_type(value: &str) -> bool {
    matches!(
        value,
        "SHORT_SQUEEZE"
            | "LONG_LIQUIDATION"
            | "DERIVATIVES_LED_BREAKOUT"
            | "SPOT_LED_BREAKOUT"
            | "BOOK_DISLOCATION"
            | "MACRO_RISK_ON"
            | "MACRO_RISK_OFF"
            | "NEWS_SHOCK"
            | "FLOW_SHOCK"
            | "CROSS_ASSET_PROPAGATION"
            | "VOLATILITY_SHOCK"
            | "UNKNOWN"
    )
}

fn is_source(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.as_bytes()[0].is_ascii_lowercase()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn is_reason_code(value: &str) -> bool {
    !value.is_empty()
        && value.as_bytes()[0].is_ascii_uppercase()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn validate_quality_flags(values: &[Value]) -> Result<(), ContractError> {
    let allowed = [
        "BOOK_RESYNCING",
        "CLOCK_UNCERTAIN",
        "CROSS_VENUE_DIVERGENCE",
        "INSUFFICIENT_COVERAGE",
        "LATE_CONTEXT",
        "MARK_MISSING",
        "OI_STALE",
        "QUEUE_DROP",
        "RECONNECT_WARMUP",
        "SEQUENCE_GAP",
        "SOURCE_STALE",
    ];
    let flags = values
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or(ContractError::Structure("quality flag must be string"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if flags.windows(2).any(|pair| pair[0] >= pair[1])
        || flags.iter().any(|flag| !allowed.contains(flag))
    {
        return Err(ContractError::Semantic("invalid quality flags"));
    }
    Ok(())
}

fn expected_feature_unit(name: &str) -> Option<&'static str> {
    Some(match name {
        "log_return" => "LOG_RETURN",
        "taker_imbalance" | "cross_venue_breadth" | "reversal_from_extreme" => "RATIO",
        "cvd_slope" => "BASE_PER_SECOND",
        "spread_bps" => "BPS",
        "book_depth_10bps" | "liquidation_notional" => "USDC",
        "open_interest_change" => "CONTRACTS",
        _ => return None,
    })
}

fn feature_value_in_domain(name: &str, value: &str) -> Result<bool, ContractError> {
    let scaled = decimal_scaled(value)?;
    Ok(match name {
        "log_return" | "taker_imbalance" => (-100_000_000..=100_000_000).contains(&scaled),
        "cross_venue_breadth" | "reversal_from_extreme" => (0..=100_000_000).contains(&scaled),
        "spread_bps" | "book_depth_10bps" | "liquidation_notional" => scaled >= 0,
        _ => true,
    })
}

fn validate_context_evidence(obj: &Map<String, Value>) -> Result<(), ContractError> {
    let envelope_available = parse_time(string_field(
        object(field(obj, "causal_time")?)?,
        "available_at",
    )?)?;
    let evidence = array_field(obj, "evidence")?;
    if evidence.is_empty() {
        return Err(ContractError::Semantic("empty context evidence"));
    }
    let mut ids = BTreeSet::new();
    let mut ordering = Vec::new();
    for item in evidence {
        let item = object(item)?;
        require_exact_keys(
            item,
            &[
                "evidence_id",
                "evidence_type",
                "source_id",
                "source_payload_hash",
                "first_seen_at",
                "available_at",
                "attribution_state",
            ],
            &[],
        )?;
        let id = string_field(item, "evidence_id")?;
        let first = parse_time(string_field(item, "first_seen_at")?)?;
        let available = parse_time(string_field(item, "available_at")?)?;
        if !is_id(id, "event_evidence_")
            || !ids.insert(id)
            || !is_source(string_field(item, "source_id")?)
            || !is_hash(string_field(item, "source_payload_hash")?)
            || !matches!(
                string_field(item, "evidence_type")?,
                "OFFICIAL_RELEASE"
                    | "NEWS"
                    | "WALLET_FLOW"
                    | "EXCHANGE_FLOW"
                    | "ONCHAIN"
                    | "MARKET_CONTEXT"
            )
            || !matches!(
                string_field(item, "attribution_state")?,
                "UNKNOWN" | "CANDIDATE" | "CONFIRMED" | "DISPUTED"
            )
            || first > available
            || available > envelope_available
        {
            return Err(ContractError::Semantic("invalid context evidence"));
        }
        ordering.push((
            available,
            string_field(item, "source_id")?,
            id,
            string_field(item, "source_payload_hash")?,
        ));
    }
    if ordering.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ContractError::Semantic(
            "context evidence not canonically ordered",
        ));
    }
    Ok(())
}
fn reject_floats(value: &Value) -> Result<(), ContractError> {
    match value {
        Value::Number(n) if n.is_f64() => {
            Err(ContractError::Structure("binary float is forbidden"))
        }
        Value::Array(v) => {
            for x in v {
                reject_floats(x)?;
            }
            Ok(())
        }
        Value::Object(m) => {
            for x in m.values() {
                reject_floats(x)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
const ROOT_FIELDS: &[&str] = &[
    "schema_version",
    "contract_id",
    "contract_type",
    "lineage_id",
    "revision",
    "predecessor_content_hash",
    "content_hash",
    "causal_time",
];
fn require_exact_keys(
    obj: &Map<String, Value>,
    common: &[&str],
    extra: &[&str],
) -> Result<(), ContractError> {
    let mut allowed: BTreeSet<&str> = common.iter().copied().collect();
    allowed.extend(extra.iter().copied());
    if obj.keys().any(|key| !allowed.contains(key.as_str())) {
        return Err(ContractError::Structure("unknown field"));
    }
    if allowed.iter().any(|key| !obj.contains_key(*key)) {
        return Err(ContractError::Structure("required field missing"));
    }
    Ok(())
}
fn object(value: &Value) -> Result<&Map<String, Value>, ContractError> {
    value
        .as_object()
        .ok_or(ContractError::Structure("expected object"))
}
fn field<'a>(obj: &'a Map<String, Value>, key: &str) -> Result<&'a Value, ContractError> {
    obj.get(key)
        .ok_or(ContractError::Structure("required field missing"))
}
fn string_field<'a>(obj: &'a Map<String, Value>, key: &str) -> Result<&'a str, ContractError> {
    field(obj, key)?
        .as_str()
        .ok_or(ContractError::Structure("expected string"))
}
fn integer_field(obj: &Map<String, Value>, key: &str) -> Result<i128, ContractError> {
    field(obj, key)?
        .as_i64()
        .map(i128::from)
        .or_else(|| field(obj, key).ok().and_then(Value::as_u64).map(i128::from))
        .ok_or(ContractError::Structure("expected integer"))
}
fn array_field<'a>(
    obj: &'a Map<String, Value>,
    key: &str,
) -> Result<&'a Vec<Value>, ContractError> {
    field(obj, key)?
        .as_array()
        .ok_or(ContractError::Structure("expected array"))
}
fn is_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn is_id(value: &str, prefix: &str) -> bool {
    value.trim() == value
        && value.strip_prefix(prefix).is_some_and(|suffix| {
            !suffix.is_empty()
                && suffix.as_bytes()[0].is_ascii_alphanumeric()
                && suffix
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        })
}
fn is_epoch(value: &str) -> bool {
    value.strip_prefix("epoch_").is_some_and(|suffix| {
        !suffix.is_empty()
            && suffix.len() <= 64
            && suffix.as_bytes()[0].is_ascii_alphanumeric()
            && suffix
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
    })
}
fn is_asset(value: &str) -> bool {
    matches!(value, "BTC" | "ETH" | "SOL" | "BNB" | "HYPE")
}
fn is_venue(value: &str) -> bool {
    matches!(value, "BINANCE" | "HYPERLIQUID")
}
fn is_symbol(value: &str) -> bool {
    !value.is_empty()
        && (value.as_bytes()[0].is_ascii_uppercase() || value.as_bytes()[0].is_ascii_digit())
        && value
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || matches!(b, b'-' | b'_'))
}
fn is_snake(value: &str) -> bool {
    !value.is_empty()
        && value.as_bytes()[0].is_ascii_lowercase()
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}
fn is_unit(value: &str) -> bool {
    !value.is_empty()
        && value.as_bytes()[0].is_ascii_uppercase()
        && value.bytes().all(|b| {
            b.is_ascii_uppercase() || b.is_ascii_digit() || matches!(b, b'_' | b'/' | b'-')
        })
}
fn unique_hashes(values: &Vec<Value>) -> Result<(), ContractError> {
    if values.is_empty() {
        return Err(ContractError::Semantic("hash list must be nonempty"));
    }
    let mut seen = BTreeSet::new();
    for value in values {
        let hash = value
            .as_str()
            .filter(|value| is_hash(value))
            .ok_or(ContractError::Semantic("invalid hash"))?;
        if !seen.insert(hash) {
            return Err(ContractError::Semantic("duplicate hash"));
        }
    }
    Ok(())
}
