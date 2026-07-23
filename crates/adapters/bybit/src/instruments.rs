//! Bybit V5 `instruments-info` parsing for price/qty scales.

use marketfeed_adapter_api::{AdapterError, HttpResponse};
use marketfeed_model::{
    AssetCode, Fixed, InstrumentDefinition, InstrumentKey, InstrumentKind, InstrumentStatus,
    VenueCode,
};
use serde::Deserialize;

use crate::specification::BybitCategory;

#[derive(Debug, Deserialize)]
struct InstrumentsResponse {
    #[serde(rename = "retCode")]
    ret_code: i64,
    result: InstrumentsResult,
}

#[derive(Debug, Deserialize)]
struct InstrumentsResult {
    list: Vec<InstrumentRow>,
}

#[derive(Debug, Deserialize)]
struct InstrumentRow {
    symbol: String,
    status: String,
    #[serde(rename = "baseCoin")]
    base_coin: String,
    #[serde(rename = "quoteCoin")]
    quote_coin: String,
    #[serde(rename = "contractType")]
    contract_type: Option<String>,
    #[serde(rename = "priceFilter")]
    price_filter: PriceFilter,
    #[serde(rename = "lotSizeFilter")]
    lot_size_filter: LotSizeFilter,
}

#[derive(Debug, Deserialize)]
struct PriceFilter {
    #[serde(rename = "tickSize")]
    tick_size: String,
}

#[derive(Debug, Deserialize)]
struct LotSizeFilter {
    #[serde(rename = "qtyStep")]
    qty_step: Option<String>,
    #[serde(rename = "basePrecision")]
    base_precision: Option<String>,
}

/// Parse trading instruments from one or more instruments-info HTTP bodies.
pub fn parse_instruments_info(
    responses: &[HttpResponse],
    category: BybitCategory,
) -> Result<Vec<InstrumentDefinition>, AdapterError> {
    let mut out = Vec::new();
    let venue_code = match category {
        BybitCategory::Linear => "bybit-linear",
        BybitCategory::Spot => "bybit-spot",
        BybitCategory::Inverse => "bybit-inverse",
    };
    for resp in responses {
        if resp.status != 200 {
            return Err(AdapterError::Catalog(format!(
                "instruments-info HTTP {}",
                resp.status
            )));
        }
        let body: InstrumentsResponse =
            serde_json::from_slice(&resp.body).map_err(|e| AdapterError::Catalog(e.to_string()))?;
        if body.ret_code != 0 {
            return Err(AdapterError::Catalog(format!(
                "instruments-info retCode {}",
                body.ret_code
            )));
        }
        for row in body.result.list {
            let status = map_status(&row.status);
            let price_inc = Fixed::parse_str(&row.price_filter.tick_size).map_err(|e| {
                AdapterError::Catalog(format!("tickSize {}: {e}", row.price_filter.tick_size))
            })?;
            let qty_raw = row
                .lot_size_filter
                .qty_step
                .as_deref()
                .or(row.lot_size_filter.base_precision.as_deref())
                .ok_or_else(|| AdapterError::Catalog("missing qty step".into()))?;
            let qty_inc = Fixed::parse_str(qty_raw)
                .map_err(|e| AdapterError::Catalog(format!("qtyStep {qty_raw}: {e}")))?;
            let kind = match category {
                BybitCategory::Spot => InstrumentKind::Spot,
                BybitCategory::Linear => match row.contract_type.as_deref() {
                    Some("LinearFutures") => InstrumentKind::FutureLinear,
                    _ => InstrumentKind::PerpetualLinear,
                },
                BybitCategory::Inverse => match row.contract_type.as_deref() {
                    Some("InverseFutures") => InstrumentKind::FutureInverse,
                    _ => InstrumentKind::PerpetualInverse,
                },
            };
            out.push(InstrumentDefinition {
                key: InstrumentKey {
                    venue: VenueCode(venue_code.into()),
                    native_symbol: row.symbol,
                    kind,
                    settlement: None,
                    expiry_ns: None,
                },
                base: AssetCode(row.base_coin),
                quote: AssetCode(row.quote_coin),
                settlement: None,
                price_scale: price_inc.scale,
                quantity_scale: qty_inc.scale,
                price_increment: price_inc,
                quantity_increment: qty_inc,
                min_quantity: None,
                max_quantity: None,
                min_notional: None,
                contract_size: None,
                expiry_ns: None,
                status,
                inverse: matches!(category, BybitCategory::Inverse),
            });
        }
    }
    if out.is_empty() {
        return Err(AdapterError::Catalog(
            "instruments-info produced no symbols".into(),
        ));
    }
    Ok(out)
}

/// Map Bybit V5 `status` → `InstrumentStatus`.
fn map_status(status: &str) -> InstrumentStatus {
    match status {
        "Trading" => InstrumentStatus::Active,
        "PreLaunch" | "Settling" => InstrumentStatus::Suspended,
        "Closed" | "Delivering" => InstrumentStatus::Delisted,
        _ => InstrumentStatus::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn parses_linear_perpetual_scales() {
        let body = br#"{
          "retCode":0,
          "result":{"list":[{
            "symbol":"BTCUSDT",
            "status":"Trading",
            "baseCoin":"BTC",
            "quoteCoin":"USDT",
            "contractType":"LinearPerpetual",
            "priceFilter":{"tickSize":"0.10"},
            "lotSizeFilter":{"qtyStep":"0.001"}
          },{
            "symbol":"DEAD",
            "status":"Closed",
            "baseCoin":"X",
            "quoteCoin":"Y",
            "priceFilter":{"tickSize":"1"},
            "lotSizeFilter":{"qtyStep":"1"}
          }]}
        }"#;
        let defs = parse_instruments_info(
            &[HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(body),
            }],
            BybitCategory::Linear,
        )
        .unwrap();
        assert_eq!(defs.len(), 2);
        assert_eq!(defs[0].key.native_symbol, "BTCUSDT");
        assert_eq!(defs[0].key.kind, InstrumentKind::PerpetualLinear);
        assert_eq!(defs[0].status, InstrumentStatus::Active);
        assert_eq!(defs[0].price_scale, 2);
        assert_eq!(defs[0].quantity_scale, 3);
        assert_eq!(defs[1].key.native_symbol, "DEAD");
        assert_eq!(defs[1].status, InstrumentStatus::Delisted);
    }

    #[test]
    fn parses_inverse_perpetual_scales() {
        let body = br#"{
          "retCode":0,
          "result":{"list":[{
            "symbol":"BTCUSD",
            "status":"Trading",
            "baseCoin":"BTC",
            "quoteCoin":"USD",
            "contractType":"InversePerpetual",
            "priceFilter":{"tickSize":"0.5"},
            "lotSizeFilter":{"qtyStep":"1"}
          }]}
        }"#;
        let defs = parse_instruments_info(
            &[HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(body),
            }],
            BybitCategory::Inverse,
        )
        .unwrap();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].key.venue, VenueCode("bybit-inverse".into()));
        assert_eq!(defs[0].key.kind, InstrumentKind::PerpetualInverse);
        assert!(defs[0].inverse);
    }
}
