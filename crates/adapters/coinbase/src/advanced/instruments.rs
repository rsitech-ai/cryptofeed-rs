//! Coinbase Advanced Trade public `/market/products` REST parsing.
//!
//! Endpoint: `GET {REST_BASE}/products` (no auth). Envelope differs from
//! Exchange Classic's bare array — Adv returns `{ "products": [...] }`.

use marketfeed_adapter_api::{AdapterError, HttpResponse};
use marketfeed_model::{
    AssetCode, Fixed, InstrumentDefinition, InstrumentKey, InstrumentKind, InstrumentStatus,
    VenueCode,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ProductsEnvelope {
    products: Vec<Product>,
}

#[derive(Debug, Deserialize)]
struct Product {
    product_id: String,
    #[serde(default)]
    base_currency_id: String,
    #[serde(default)]
    quote_currency_id: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    product_type: String,
    #[serde(default)]
    trading_disabled: bool,
    #[serde(default)]
    is_disabled: bool,
    #[serde(default)]
    quote_increment: Option<String>,
    #[serde(default)]
    base_increment: Option<String>,
    #[serde(default)]
    quote_min_size: Option<String>,
    #[serde(default)]
    base_min_size: Option<String>,
}

/// Parse spot products from Adv public `GET .../market/products`.
pub fn parse_products(
    responses: &[HttpResponse],
) -> Result<Vec<InstrumentDefinition>, AdapterError> {
    let mut out = Vec::new();
    for resp in responses {
        if resp.status != 200 {
            return Err(AdapterError::Catalog(format!(
                "adv products HTTP {}",
                resp.status
            )));
        }
        let envelope: ProductsEnvelope =
            serde_json::from_slice(&resp.body).map_err(|e| AdapterError::Catalog(e.to_string()))?;
        for p in envelope.products {
            if !p.product_type.is_empty() && !p.product_type.eq_ignore_ascii_case("SPOT") {
                continue;
            }
            if p.base_currency_id.is_empty() || p.quote_currency_id.is_empty() {
                continue;
            }
            let status = map_status(&p.status, p.trading_disabled || p.is_disabled);
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
            let min_quantity = p
                .base_min_size
                .as_deref()
                .map(Fixed::parse_str)
                .transpose()
                .map_err(|e| AdapterError::Catalog(e.to_string()))?
                .or(Some(qty_inc));
            let min_notional = p
                .quote_min_size
                .as_deref()
                .map(Fixed::parse_str)
                .transpose()
                .map_err(|e| AdapterError::Catalog(e.to_string()))?;
            out.push(InstrumentDefinition {
                key: InstrumentKey {
                    venue: VenueCode("coinbase-adv".into()),
                    native_symbol: p.product_id,
                    kind: InstrumentKind::Spot,
                    settlement: None,
                    expiry_ns: None,
                },
                base: AssetCode(p.base_currency_id),
                quote: AssetCode(p.quote_currency_id),
                settlement: None,
                price_scale: price_inc.scale,
                quantity_scale: qty_inc.scale,
                price_increment: price_inc,
                quantity_increment: qty_inc,
                min_quantity,
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
            "adv products produced no spot instruments".into(),
        ));
    }
    Ok(out)
}

/// Map Adv `status` + disable flags → `InstrumentStatus`.
fn map_status(status: &str, disabled: bool) -> InstrumentStatus {
    if disabled {
        return InstrumentStatus::Suspended;
    }
    match status {
        "online" | "" => InstrumentStatus::Active,
        "offline" | "disabled" => InstrumentStatus::Suspended,
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
        let body = br#"{
          "products":[
            {"product_id":"BTC-USD","base_currency_id":"BTC","quote_currency_id":"USD",
             "status":"online","product_type":"SPOT","trading_disabled":false,"is_disabled":false,
             "quote_increment":"0.01","base_increment":"0.00000001",
             "quote_min_size":"1","base_min_size":"0.00000001"},
            {"product_id":"DEAD-USD","base_currency_id":"DEAD","quote_currency_id":"USD",
             "status":"delisted","product_type":"SPOT","trading_disabled":false,"is_disabled":false,
             "quote_increment":"0.01","base_increment":"1"},
            {"product_id":"OFF-USD","base_currency_id":"OFF","quote_currency_id":"USD",
             "status":"online","product_type":"SPOT","trading_disabled":true,"is_disabled":false,
             "quote_increment":"0.01","base_increment":"1"},
            {"product_id":"ETH-PERP-INTX","base_currency_id":"ETH","quote_currency_id":"USDC",
             "status":"online","product_type":"FUTURE","trading_disabled":false,"is_disabled":false,
             "quote_increment":"0.01","base_increment":"0.001"}
          ]
        }"#;
        let defs = parse_products(&[HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(body),
        }])
        .unwrap();
        assert_eq!(defs.len(), 3);
        assert_eq!(defs[0].key.native_symbol, "BTC-USD");
        assert_eq!(defs[0].key.venue.0, "coinbase-adv");
        assert_eq!(defs[0].key.kind, InstrumentKind::Spot);
        assert_eq!(defs[0].status, InstrumentStatus::Active);
        assert_eq!(defs[0].price_scale, 2);
        assert_eq!(defs[0].quantity_scale, 8);
        assert_eq!(defs[0].min_notional, Some(Fixed::new(1, 0)));
        assert_eq!(defs[1].key.native_symbol, "DEAD-USD");
        assert_eq!(defs[1].status, InstrumentStatus::Delisted);
        assert_eq!(defs[2].key.native_symbol, "OFF-USD");
        assert_eq!(defs[2].status, InstrumentStatus::Suspended);
    }
}
