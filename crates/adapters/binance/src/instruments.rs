//! Minimal Binance Spot `exchangeInfo` parsing for price/qty scales.

use marketfeed_adapter_api::{AdapterError, HttpResponse};
use marketfeed_model::{
    AssetCode, Fixed, InstrumentDefinition, InstrumentKey, InstrumentKind, InstrumentStatus,
    VenueCode,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ExchangeInfo {
    symbols: Vec<SymbolInfo>,
}

#[derive(Debug, Deserialize)]
struct SymbolInfo {
    symbol: String,
    status: String,
    #[serde(rename = "baseAsset")]
    base_asset: String,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
    filters: Vec<Filter>,
}

#[derive(Debug, Deserialize)]
struct Filter {
    #[serde(rename = "filterType")]
    filter_type: String,
    #[serde(rename = "tickSize")]
    tick_size: Option<String>,
    #[serde(rename = "stepSize")]
    step_size: Option<String>,
}

/// Map Binance `symbol.status` → `InstrumentStatus`.
fn map_status(status: &str) -> InstrumentStatus {
    match status {
        "TRADING" => InstrumentStatus::Active,
        "BREAK" | "HALT" | "END_OF_DAY" | "AUCTION_MATCH" | "PENDING_TRADING" | "PRE_TRADING"
        | "POST_TRADING" | "SETTLING" | "CLOSE" => InstrumentStatus::Suspended,
        "DELIVERING" => InstrumentStatus::Expired,
        _ => InstrumentStatus::Unknown,
    }
}

/// Parse Spot symbols from one or more exchangeInfo HTTP bodies.
pub fn parse_exchange_info(
    responses: &[HttpResponse],
) -> Result<Vec<InstrumentDefinition>, AdapterError> {
    let mut out = Vec::new();
    for resp in responses {
        if resp.status != 200 {
            return Err(AdapterError::Catalog(format!(
                "exchangeInfo HTTP {}",
                resp.status
            )));
        }
        let info: ExchangeInfo =
            serde_json::from_slice(&resp.body).map_err(|e| AdapterError::Catalog(e.to_string()))?;
        for sym in info.symbols {
            let status = map_status(&sym.status);
            let Some(tick) = sym
                .filters
                .iter()
                .find(|f| f.filter_type == "PRICE_FILTER")
                .and_then(|f| f.tick_size.as_deref())
            else {
                continue;
            };
            let Some(step) = sym
                .filters
                .iter()
                .find(|f| f.filter_type == "LOT_SIZE")
                .and_then(|f| f.step_size.as_deref())
            else {
                continue;
            };
            let price_inc = Fixed::parse_str(tick)
                .map_err(|e| AdapterError::Catalog(format!("tickSize {tick}: {e}")))?;
            let qty_inc = Fixed::parse_str(step)
                .map_err(|e| AdapterError::Catalog(format!("stepSize {step}: {e}")))?;
            out.push(InstrumentDefinition {
                key: InstrumentKey {
                    venue: VenueCode("binance-spot".into()),
                    native_symbol: sym.symbol,
                    kind: InstrumentKind::Spot,
                    settlement: None,
                    expiry_ns: None,
                },
                base: AssetCode(sym.base_asset),
                quote: AssetCode(sym.quote_asset),
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
                inverse: false,
            });
        }
    }
    if out.is_empty() {
        return Err(AdapterError::Catalog(
            "exchangeInfo produced no symbols".into(),
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn parses_scales_and_status() {
        let body = br#"{
          "symbols":[{
            "symbol":"BTCUSDT",
            "status":"TRADING",
            "baseAsset":"BTC",
            "quoteAsset":"USDT",
            "filters":[
              {"filterType":"PRICE_FILTER","tickSize":"0.01"},
              {"filterType":"LOT_SIZE","stepSize":"0.00001000"}
            ]
          },{
            "symbol":"DEAD",
            "status":"BREAK",
            "baseAsset":"X",
            "quoteAsset":"Y",
            "filters":[
              {"filterType":"PRICE_FILTER","tickSize":"0.01"},
              {"filterType":"LOT_SIZE","stepSize":"1"}
            ]
          }]
        }"#;
        let defs = parse_exchange_info(&[HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(body),
        }])
        .unwrap();
        assert_eq!(defs.len(), 2);
        assert_eq!(defs[0].key.native_symbol, "BTCUSDT");
        assert_eq!(defs[0].status, InstrumentStatus::Active);
        assert_eq!(defs[0].price_scale, 2);
        assert_eq!(defs[0].quantity_scale, 8);
        assert_eq!(defs[1].key.native_symbol, "DEAD");
        assert_eq!(defs[1].status, InstrumentStatus::Suspended);
    }
}
