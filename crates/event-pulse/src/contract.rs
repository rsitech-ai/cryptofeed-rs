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
    #[error("composite does not bind the supplied exact mechanics/context objects")]
    HashBinding,
    #[error("embedded contract provenance failed: {0}")]
    Provenance(String),
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
            return Err(ContractError::HashBinding);
        }
        if composite
            .get("mechanics_content_hash")
            .and_then(Value::as_str)
            != Some(string_field(mechanics, "content_hash")?)
            || composite.get("mechanics_lineage_id") != mechanics.get("lineage_id")
            || composite.get("event_cluster_id") != mechanics.get("event_cluster_id")
            || composite.get("scope") != mechanics.get("scope")
        {
            return Err(ContractError::HashBinding);
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
                return Err(ContractError::HashBinding);
            }
            Some(context) => {
                let context = object(context.value())?;
                if string_field(context, "contract_type")? != "context"
                    || composite
                        .get("context_content_hash")
                        .and_then(Value::as_str)
                        != Some(string_field(context, "content_hash")?)
                    || composite.get("context_lineage_id") != context.get("lineage_id")
                    || composite.get("event_cluster_id") != context.get("event_cluster_id")
                    || composite.get("scope") != context.get("scope")
                {
                    return Err(ContractError::HashBinding);
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
        ] {
            if composite.get(field_name) != mechanics.get(field_name) {
                return Err(ContractError::HashBinding);
            }
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
        return Err(ContractError::Semantic("revision scope mismatch"));
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
        return Err(ContractError::Semantic(
            "context evidence is not append-only",
        ));
    }
    Ok(())
}

pub fn canonical_json(value: &Value) -> String {
    serde_json::to_string(value).expect("serde JSON values serialize")
}

/// Fallible, non-panicking canonical hash API for untrusted values.
pub fn content_hash(value: &Value) -> Result<String, ContractError> {
    let mut preimage = value.clone();
    preimage
        .as_object_mut()
        .ok_or(ContractError::Structure("hash payload must be an object"))?
        .remove("content_hash");
    Ok(format!(
        "{:x}",
        Sha256::digest(canonical_json(&preimage).as_bytes())
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
    validate_common(obj)?;
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

fn validate_common(obj: &Map<String, Value>) -> Result<(), ContractError> {
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
    validate_causal_time(object(field(obj, "causal_time")?)?)
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
                decimal(string_field(item, "value")?, 18, 8)?;
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
            {
                return Err(ContractError::Semantic("invalid risk identity"));
            }
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
        decimal(string_field(obj, name)?, 1, 8)?;
    }
    let availability = parse_time(string_field(
        object(field(obj, "causal_time")?)?,
        "available_at",
    )?)?;
    let mut keys = Vec::new();
    for item in array_field(obj, "source_cursors")? {
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
        if at > availability
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
        return Err(ContractError::Semantic(
            "source cursors not canonically ordered",
        ));
    }
    let validated = string_field(obj, "quality_state")? == "VALIDATED";
    for item in array_field(obj, "features")? {
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
        if validated && string_field(feature, "quality_state")? != "VALIDATED" {
            return Err(ContractError::Semantic(
                "validated mechanics has unusable feature",
            ));
        }
        if let Some(Value::String(v)) = feature.get("value") {
            decimal(v, 18, 8)?;
        } else if !feature.get("value").is_some_and(Value::is_null) {
            return Err(ContractError::Semantic("feature decimal must be string"));
        }
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
    {
        return Err(ContractError::Semantic("invalid context literals"));
    }
    decimal(string_field(obj, "catalyst_confidence")?, 1, 8)?;
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
        || string_field(obj, "evidence_state")? != "UNAVAILABLE"
        || string_field(obj, "source_qualification")? != "UNVERIFIED"
    {
        return Err(ContractError::Semantic("invalid composite literals"));
    }
    for name in ["mechanics_content_hash"] {
        if !is_hash(string_field(obj, name)?) {
            return Err(ContractError::Semantic("invalid composite hash"));
        }
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
fn validate_causal_time(obj: &Map<String, Value>) -> Result<(), ContractError> {
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
    decimal(string_field(clock, "observed_skew_ms")?, 18, 8)?;
    let freshness = integer_field(clock, "freshness_limit_ms")?;
    if freshness <= 0 || times[4] - times[0] > freshness * 1_000 {
        return Err(ContractError::Semantic("clock freshness"));
    }
    Ok(())
}
fn parse_time(value: &str) -> Result<i128, ContractError> {
    crate::wire::Rfc3339Time::parse(value)
        .map(|time| time.utc_micros())
        .map_err(|_| ContractError::Semantic("invalid RFC3339 timestamp"))
}
fn decimal(value: &str, max_integer: usize, max_fraction: usize) -> Result<(), ContractError> {
    crate::wire::CanonicalDecimal::parse(value, max_integer, max_fraction)
        .map(|_| ())
        .map_err(|_| ContractError::Semantic("noncanonical decimal string"))
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
                && suffix
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        })
}
fn is_epoch(value: &str) -> bool {
    is_id(value, "epoch_") && value.len() <= 70
}
fn is_asset(value: &str) -> bool {
    matches!(value, "BTC" | "ETH" | "SOL" | "BNB" | "HYPE")
}
fn is_venue(value: &str) -> bool {
    matches!(value, "BINANCE" | "HYPERLIQUID")
}
fn is_symbol(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|b| {
            b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'-' || b == b'_' || b == b'.'
        })
}
fn is_snake(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}
fn is_unit(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|b| {
            b.is_ascii_uppercase() || b.is_ascii_digit() || matches!(b, b'_' | b'/' | b'-')
        })
}
fn unique_hashes(values: &Vec<Value>) -> Result<(), ContractError> {
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
