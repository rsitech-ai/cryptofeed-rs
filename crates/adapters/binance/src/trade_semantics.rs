//! Native trade identity/provenance without inventing execution counts.
use marketfeed_model::{Fixed, SourceId, TradeGranularity, TradeProvenance};
use serde_json::Value;
use std::collections::BTreeMap;

pub(crate) fn provenance(
    value: &Value,
    aggregate: bool,
    documented_individual: bool,
    expected_symbol_type: Option<u64>,
) -> Result<Box<TradeProvenance>, String> {
    let id = |key: &str| -> Result<Option<u64>, String> {
        value
            .get(key)
            .map(|v| v.as_u64().ok_or_else(|| format!("invalid native {key}")))
            .transpose()
    };
    for key in ["E", "T"] {
        if let Some(time) = value.get(key) {
            time.as_i64()
                .and_then(|ms| ms.checked_mul(1_000_000))
                .ok_or_else(|| format!("invalid or overflowing native {key} timestamp"))?;
        }
    }
    let first = if aggregate { id("f")? } else { None };
    let last = if aggregate { id("l")? } else { None };
    if first.is_some() != last.is_some() || first.zip(last).is_some_and(|(f, l)| f > l) {
        return Err("incomplete or reversed native aggregate range".into());
    }
    if let Some(st) = id("st")? {
        if !matches!(st, 1 | 2) || expected_symbol_type.is_some_and(|expected| expected != st) {
            return Err("native symbol type does not match configured product".into());
        }
    }
    let mut attributes = BTreeMap::new();
    for key in [
        "e", "E", "T", "a", "t", "f", "l", "m", "M", "X", "st", "p", "q", "nq",
    ] {
        if let Some(v) = value.get(key) {
            let text = match v {
                Value::String(s) => s.clone(),
                Value::Number(n) if n.is_i64() || n.is_u64() => n.to_string(),
                Value::Bool(b) => b.to_string(),
                _ => return Err(format!("invalid native trade attribute {key}")),
            };
            attributes.insert(key.into(), text);
        }
    }
    if let Some(nq) = value.get("nq") {
        let normal = Fixed::parse_str(nq.as_str().ok_or("nq must be a decimal string")?)
            .map_err(|e| e.to_string())?;
        let quantity = Fixed::parse_str(value["q"].as_str().ok_or("q must be a decimal string")?)
            .map_err(|e| e.to_string())?;
        if normal.coefficient < 0 || normal.numeric_cmp(quantity).is_gt() {
            return Err("native normal quantity outside total quantity".into());
        }
    }
    Ok(Box::new(TradeProvenance {
        source_channel: if aggregate { "aggTrade" } else { "trade" }.into(),
        granularity: if aggregate {
            TradeGranularity::NativeAggregate
        } else if documented_individual {
            TradeGranularity::IndividualExecution
        } else {
            TradeGranularity::Unverified
        },
        constituent_first_id: first.map(|id| SourceId(id.to_string())),
        constituent_last_id: last.map(|id| SourceId(id.to_string())),
        native_count: None,
        native_attributes: attributes,
    }))
}

/// Conservative tape-admission policy, not a claim about economic authenticity.
/// Unknown/non-market native X values remain durable classified source records.
pub(crate) fn exclusion(value: &Value) -> Result<Option<String>, String> {
    value
        .get("X")
        .map(|v| {
            v.as_str()
                .ok_or_else(|| "native X must be a string".into())
                .map(|kind| (kind != "MARKET").then(|| format!("native_type_not_admitted:{kind}")))
        })
        .transpose()
        .map(Option::flatten)
}

pub(crate) fn identity(id: u64, provenance: &TradeProvenance) -> SourceId {
    if provenance.source_channel == "aggTrade" {
        SourceId(format!("aggTrade:{id}"))
    } else {
        SourceId(id.to_string())
    }
}
