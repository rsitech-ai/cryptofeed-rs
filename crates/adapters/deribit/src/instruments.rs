//! Deribit `public/get_instruments` parsing.

use marketfeed_adapter_api::{AdapterError, HttpResponse};
use marketfeed_model::{
    AssetCode, Fixed, InstrumentDefinition, InstrumentKey, InstrumentKind, InstrumentStatus,
    VenueCode,
};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
struct RpcList {
    result: Vec<InstrumentInfo>,
}

#[derive(Debug, Deserialize)]
struct InstrumentInfo {
    instrument_name: String,
    kind: String,
    is_active: bool,
    base_currency: String,
    quote_currency: String,
    settlement_period: Option<String>,
    tick_size: Value,
    min_trade_amount: Value,
    contract_size: Option<Value>,
    expiration_timestamp: Option<i64>,
    is_inverse: Option<bool>,
}

/// Parse futures/perpetuals from get_instruments HTTP bodies.
pub fn parse_instruments(
    responses: &[HttpResponse],
) -> Result<Vec<InstrumentDefinition>, AdapterError> {
    let mut out = Vec::new();
    for resp in responses {
        if resp.status != 200 {
            return Err(AdapterError::Catalog(format!(
                "get_instruments HTTP {}",
                resp.status
            )));
        }
        let body: RpcList =
            serde_json::from_slice(&resp.body).map_err(|e| AdapterError::Catalog(e.to_string()))?;
        for info in body.result {
            if info.kind != "future" {
                continue;
            }
            let status = map_active(info.is_active);
            let price_inc = fixed_from_json(&info.tick_size)
                .map_err(|e| AdapterError::Catalog(format!("tick_size: {e}")))?;
            let qty_inc = fixed_from_json(&info.min_trade_amount)
                .map_err(|e| AdapterError::Catalog(format!("min_trade_amount: {e}")))?;
            let inverse = info.is_inverse.unwrap_or(true);
            let kind = match info.settlement_period.as_deref() {
                Some("perpetual") if inverse => InstrumentKind::PerpetualInverse,
                Some("perpetual") => InstrumentKind::PerpetualLinear,
                Some(_) if inverse => InstrumentKind::FutureInverse,
                Some(_) => InstrumentKind::FutureLinear,
                None if inverse => InstrumentKind::FutureInverse,
                None => InstrumentKind::FutureLinear,
            };
            let contract_size = info
                .contract_size
                .as_ref()
                .map(fixed_from_json)
                .transpose()
                .map_err(AdapterError::Catalog)?;
            let expiry_ns = info
                .expiration_timestamp
                .map(|ms| ms.saturating_mul(1_000_000));
            out.push(InstrumentDefinition {
                key: InstrumentKey {
                    venue: VenueCode("deribit".into()),
                    native_symbol: info.instrument_name,
                    kind,
                    settlement: Some(AssetCode(info.quote_currency.clone())),
                    expiry_ns,
                },
                base: AssetCode(info.base_currency),
                quote: AssetCode(info.quote_currency),
                settlement: None,
                price_scale: price_inc.scale,
                quantity_scale: qty_inc.scale,
                price_increment: price_inc,
                quantity_increment: qty_inc,
                min_quantity: Some(qty_inc),
                max_quantity: None,
                min_notional: None,
                contract_size,
                expiry_ns,
                status,
                inverse,
            });
        }
    }
    if out.is_empty() {
        return Err(AdapterError::Catalog(
            "get_instruments produced no futures".into(),
        ));
    }
    Ok(out)
}

/// Map Deribit `is_active` → `InstrumentStatus`.
fn map_active(is_active: bool) -> InstrumentStatus {
    if is_active {
        InstrumentStatus::Active
    } else {
        InstrumentStatus::Suspended
    }
}

fn fixed_from_json(v: &Value) -> Result<Fixed, String> {
    match v {
        Value::String(s) => Fixed::parse_str(s).map_err(|e| e.to_string()),
        Value::Number(n) => Fixed::parse_str(&n.to_string()).map_err(|e| e.to_string()),
        _ => Err("expected number or string".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn parses_perpetual_inverse() {
        let body = br#"{
          "jsonrpc":"2.0",
          "result":[{
            "instrument_name":"BTC-PERPETUAL",
            "kind":"future",
            "is_active":true,
            "base_currency":"BTC",
            "quote_currency":"USD",
            "settlement_period":"perpetual",
            "tick_size":0.5,
            "min_trade_amount":10,
            "contract_size":10,
            "is_inverse":true
          },{
            "instrument_name":"DEAD",
            "kind":"future",
            "is_active":false,
            "base_currency":"BTC",
            "quote_currency":"USD",
            "tick_size":1,
            "min_trade_amount":1
          }]
        }"#;
        let defs = parse_instruments(&[HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(body),
        }])
        .unwrap();
        assert_eq!(defs.len(), 2);
        assert_eq!(defs[0].key.native_symbol, "BTC-PERPETUAL");
        assert_eq!(defs[0].key.kind, InstrumentKind::PerpetualInverse);
        assert_eq!(defs[0].status, InstrumentStatus::Active);
        assert_eq!(defs[0].price_scale, 1);
        assert_eq!(defs[1].key.native_symbol, "DEAD");
        assert_eq!(defs[1].status, InstrumentStatus::Suspended);
    }
}
