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
    category: String,
    #[serde(rename = "nextPageCursor", default, deserialize_with = "cursor_string")]
    next_page_cursor: Option<String>,
    list: Vec<InstrumentRow>,
}

fn cursor_string<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    String::deserialize(d).map(Some)
}

const MAX_PAGES: usize = 16;
const MAX_PAGE_BYTES: usize = 8 * 1024 * 1024;

fn instrument_kind(
    category: BybitCategory,
    contract_type: Option<&str>,
) -> Result<InstrumentKind, AdapterError> {
    match (category, contract_type) {
        (BybitCategory::Spot, None | Some("")) => Ok(InstrumentKind::Spot),
        (BybitCategory::Linear, Some("LinearPerpetual")) => Ok(InstrumentKind::PerpetualLinear),
        (BybitCategory::Linear, Some("LinearFutures")) => Ok(InstrumentKind::FutureLinear),
        (BybitCategory::Inverse, Some("InversePerpetual")) => Ok(InstrumentKind::PerpetualInverse),
        (BybitCategory::Inverse, Some("InverseFutures")) => Ok(InstrumentKind::FutureInverse),
        _ => Err(AdapterError::Catalog(
            "missing, unknown or category-mismatched contractType".into(),
        )),
    }
}

fn pages(
    responses: &[HttpResponse],
    category: BybitCategory,
) -> Result<Vec<InstrumentsResponse>, AdapterError> {
    let error = |s: &str| AdapterError::Catalog(s.into());
    if responses.is_empty() || responses.len() > MAX_PAGES {
        return Err(error("bounded nonempty instruments-info page set required"));
    }
    let mut pages = Vec::new();
    let mut cursors = std::collections::BTreeSet::new();
    let mut symbols = std::collections::BTreeSet::new();
    for response in responses {
        if pages.last().is_some_and(|page: &InstrumentsResponse| {
            page.result
                .next_page_cursor
                .as_deref()
                .unwrap_or("")
                .is_empty()
        }) {
            return Err(error("instruments-info response after terminal page"));
        }
        if response.status != 200 || response.body.len() > MAX_PAGE_BYTES {
            return Err(error("instruments-info HTTP status or page byte limit"));
        }
        // Validate duplicate keys and structural/work bounds before typed
        // decoding the original bytes. Original numeric/string types survive.
        crate::json::value_from_slice(&response.body).map_err(AdapterError::Catalog)?;
        let page: InstrumentsResponse = serde_json::from_slice(&response.body)
            .map_err(|e| AdapterError::Catalog(e.to_string()))?;
        if page.ret_code != 0 || page.result.category != category.as_str() {
            return Err(error("instruments-info retCode/category mismatch"));
        }
        let limit = if matches!(category, BybitCategory::Spot) {
            4096
        } else {
            1000
        };
        if page.result.list.len() > limit {
            return Err(error("instruments-info row limit"));
        }
        let cursor = page.result.next_page_cursor.as_deref();
        if matches!(category, BybitCategory::Spot) {
            if cursor.is_some_and(|cursor| !cursor.is_empty()) {
                return Err(error("spot instruments-info does not support pagination"));
            }
        } else if cursor.is_none() {
            return Err(error("derivative instruments-info missing nextPageCursor"));
        }
        if let Some(cursor) = cursor.filter(|cursor| !cursor.is_empty()) {
            if cursor.len() > 2048 || !cursors.insert(cursor.to_owned()) {
                return Err(error("instruments-info cursor limit or cycle"));
            }
        }
        for row in &page.result.list {
            instrument_kind(category, row.contract_type.as_deref())?;
            if row.symbol.is_empty()
                || row.base_coin.is_empty()
                || row.quote_coin.is_empty()
                || !symbols.insert(row.symbol.clone())
            {
                return Err(error("instruments-info empty identity or duplicate symbol"));
            }
        }
        pages.push(page);
    }
    Ok(pages)
}

/// Return the exact opaque continuation token, or reject a broken page chain.
pub(crate) fn next_cursor(
    responses: &[HttpResponse],
    category: BybitCategory,
) -> Result<Option<String>, AdapterError> {
    let mut pages = pages(responses, category)?;
    let cursor = pages
        .pop()
        .expect("nonempty pages")
        .result
        .next_page_cursor
        .filter(|cursor| !cursor.is_empty());
    if cursor.is_some() && responses.len() == MAX_PAGES {
        return Err(AdapterError::Catalog(
            "instruments-info page limit; incomplete catalog".into(),
        ));
    }
    Ok(cursor)
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
    if next_cursor(responses, category)?.is_some() {
        return Err(AdapterError::Catalog(
            "incomplete instruments-info page set".into(),
        ));
    }
    for body in pages(responses, category)? {
        for row in body.result.list {
            let status = map_status(&row.status);
            let price_inc = Fixed::parse_str(&row.price_filter.tick_size).map_err(|e| {
                AdapterError::Catalog(format!("tickSize {}: {e}", row.price_filter.tick_size))
            })?;
            let qty_raw = match category {
                BybitCategory::Spot => row.lot_size_filter.base_precision.as_deref(),
                BybitCategory::Linear | BybitCategory::Inverse => {
                    row.lot_size_filter.qty_step.as_deref()
                }
            }
            .ok_or_else(|| {
                AdapterError::Catalog("missing category-specific quantity increment".into())
            })?;
            let qty_inc = Fixed::parse_str(qty_raw)
                .map_err(|e| AdapterError::Catalog(format!("qtyStep {qty_raw}: {e}")))?;
            if price_inc.coefficient <= 0 || qty_inc.coefficient <= 0 {
                return Err(AdapterError::Catalog(
                    "nonpositive instrument increment".into(),
                ));
            }
            let kind = instrument_kind(category, row.contract_type.as_deref())?;
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
          "result":{"category":"linear","nextPageCursor":"","list":[{
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
            "contractType":"LinearPerpetual",
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
          "result":{"category":"inverse","nextPageCursor":"","list":[{
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
