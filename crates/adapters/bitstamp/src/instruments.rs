//! Bitstamp Spot `GET /trading-pairs-info/` REST parsing.

use marketfeed_adapter_api::{AdapterError, HttpResponse};
use marketfeed_model::{
    AssetCode, Fixed, InstrumentDefinition, InstrumentKey, InstrumentKind, InstrumentStatus,
    VenueCode,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct TradingPair {
    name: String,
    url_symbol: String,
    #[serde(default)]
    base_decimals: u8,
    #[serde(default)]
    counter_decimals: u8,
    #[serde(default)]
    trading: String,
}

/// Parse spot pairs from `GET /api/v2/trading-pairs-info/`.
pub fn parse_trading_pairs(
    responses: &[HttpResponse],
) -> Result<Vec<InstrumentDefinition>, AdapterError> {
    let mut out = Vec::new();
    for resp in responses {
        if resp.status != 200 {
            return Err(AdapterError::Catalog(format!(
                "trading-pairs-info HTTP {}",
                resp.status
            )));
        }
        let pairs: Vec<TradingPair> =
            serde_json::from_slice(&resp.body).map_err(|e| AdapterError::Catalog(e.to_string()))?;
        for p in pairs {
            let status = map_trading(&p.trading);
            let (base, quote) = split_pair_name(&p.name);
            let price_scale = p.counter_decimals;
            let qty_scale = p.base_decimals;
            out.push(InstrumentDefinition {
                key: InstrumentKey {
                    venue: VenueCode("bitstamp".into()),
                    native_symbol: p.url_symbol,
                    kind: InstrumentKind::Spot,
                    settlement: None,
                    expiry_ns: None,
                },
                base: AssetCode(base),
                quote: AssetCode(quote),
                settlement: None,
                price_scale,
                quantity_scale: qty_scale,
                price_increment: Fixed::new(1, price_scale),
                quantity_increment: Fixed::new(1, qty_scale),
                min_quantity: None,
                max_quantity: None,
                min_notional: None,
                contract_size: None,
                expiry_ns: None,
                status,
                inverse: false,
            });
        }
    }
    if out.is_empty() {
        return Err(AdapterError::Catalog(
            "trading-pairs-info produced no spot instruments".into(),
        ));
    }
    Ok(out)
}

/// Map Bitstamp `trading` → `InstrumentStatus`.
fn map_trading(trading: &str) -> InstrumentStatus {
    match trading {
        "Enabled" | "" => InstrumentStatus::Active,
        "Disabled" => InstrumentStatus::Suspended,
        _ => InstrumentStatus::Unknown,
    }
}

/// Bitstamp `name` is `"BTC/USD"` (base/quote).
fn split_pair_name(name: &str) -> (String, String) {
    match name.split_once('/') {
        Some((b, q)) => (b.to_string(), q.to_string()),
        None => (name.to_string(), String::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn parses_enabled_trading_pair() {
        let body = br#"[
          {"name":"BTC/USD","url_symbol":"btcusd","base_decimals":8,"counter_decimals":2,"trading":"Enabled"},
          {"name":"DEAD/USD","url_symbol":"deadusd","base_decimals":8,"counter_decimals":2,"trading":"Disabled"}
        ]"#;
        let defs = parse_trading_pairs(&[HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(body),
        }])
        .unwrap();
        assert_eq!(defs.len(), 2);
        assert_eq!(defs[0].key.native_symbol, "btcusd");
        assert_eq!(defs[0].base.0, "BTC");
        assert_eq!(defs[0].quote.0, "USD");
        assert_eq!(defs[0].status, InstrumentStatus::Active);
        assert_eq!(defs[0].price_scale, 2);
        assert_eq!(defs[0].quantity_scale, 8);
        assert_eq!(defs[1].key.native_symbol, "deadusd");
        assert_eq!(defs[1].status, InstrumentStatus::Suspended);
    }
}
