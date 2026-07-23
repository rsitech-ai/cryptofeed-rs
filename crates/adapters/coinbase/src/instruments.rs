//! Coinbase Exchange `/products` REST parsing.

use marketfeed_adapter_api::{AdapterError, HttpResponse};
use marketfeed_model::{
    AssetCode, Fixed, InstrumentDefinition, InstrumentKey, InstrumentKind, InstrumentStatus,
    VenueCode,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Product {
    id: String,
    base_currency: String,
    quote_currency: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    quote_increment: Option<String>,
    #[serde(default)]
    base_increment: Option<String>,
    #[serde(default)]
    min_market_funds: Option<String>,
}

/// Parse spot products from `GET /products`.
pub fn parse_products(
    responses: &[HttpResponse],
) -> Result<Vec<InstrumentDefinition>, AdapterError> {
    let mut out = Vec::new();
    for resp in responses {
        if resp.status != 200 {
            return Err(AdapterError::Catalog(format!(
                "products HTTP {}",
                resp.status
            )));
        }
        let products: Vec<Product> =
            serde_json::from_slice(&resp.body).map_err(|e| AdapterError::Catalog(e.to_string()))?;
        for p in products {
            let status = map_status(&p.status);
            let price_inc = p
                .quote_increment
                .as_deref()
                .map(Fixed::parse_str)
                .transpose()
                .map_err(|e| AdapterError::Catalog(e.to_string()))?
                .unwrap_or(Fixed::new(1, 2));
            let qty_inc = p
                .base_increment
                .as_deref()
                .map(Fixed::parse_str)
                .transpose()
                .map_err(|e| AdapterError::Catalog(e.to_string()))?
                .unwrap_or(Fixed::new(1, 8));
            let min_notional = p
                .min_market_funds
                .as_deref()
                .map(Fixed::parse_str)
                .transpose()
                .map_err(|e| AdapterError::Catalog(e.to_string()))?;
            out.push(InstrumentDefinition {
                key: InstrumentKey {
                    venue: VenueCode("coinbase-spot".into()),
                    native_symbol: p.id,
                    kind: InstrumentKind::Spot,
                    settlement: None,
                    expiry_ns: None,
                },
                base: AssetCode(p.base_currency),
                quote: AssetCode(p.quote_currency),
                settlement: None,
                price_scale: price_inc.scale,
                quantity_scale: qty_inc.scale,
                price_increment: price_inc,
                quantity_increment: qty_inc,
                min_quantity: Some(qty_inc),
                max_quantity: None,
                min_notional,
                contract_size: None,
                expiry_ns: None,
                status,
                inverse: false,
            });
        }
    }
    if out.is_empty() {
        return Err(AdapterError::Catalog(
            "products produced no spot instruments".into(),
        ));
    }
    Ok(out)
}

/// Map Coinbase Exchange Classic `status` → `InstrumentStatus`.
fn map_status(status: &str) -> InstrumentStatus {
    match status {
        "online" | "" => InstrumentStatus::Active,
        "offline" | "disabled" | "internal" => InstrumentStatus::Suspended,
        "delisted" => InstrumentStatus::Delisted,
        _ => InstrumentStatus::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn parses_online_spot_product() {
        let body = br#"[
          {"id":"BTC-USD","base_currency":"BTC","quote_currency":"USD","status":"online",
           "quote_increment":"0.01","base_increment":"0.00000001","min_market_funds":"1"},
          {"id":"DEAD-USD","base_currency":"DEAD","quote_currency":"USD","status":"delisted",
           "quote_increment":"0.01","base_increment":"1"}
        ]"#;
        let defs = parse_products(&[HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(body),
        }])
        .unwrap();
        assert_eq!(defs.len(), 2);
        assert_eq!(defs[0].key.native_symbol, "BTC-USD");
        assert_eq!(defs[0].key.kind, InstrumentKind::Spot);
        assert_eq!(defs[0].status, InstrumentStatus::Active);
        assert_eq!(defs[0].price_scale, 2);
        assert_eq!(defs[0].quantity_scale, 8);
        assert_eq!(defs[1].key.native_symbol, "DEAD-USD");
        assert_eq!(defs[1].status, InstrumentStatus::Delisted);
    }
}
