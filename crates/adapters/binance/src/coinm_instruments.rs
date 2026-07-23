//! Binance Coin-M `exchangeInfo` → instrument definitions (inverse).

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
    #[serde(rename = "contractType")]
    contract_type: String,
    #[serde(rename = "baseAsset")]
    base_asset: String,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
    #[serde(rename = "marginAsset")]
    margin_asset: String,
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

fn kind_for(contract_type: &str) -> Option<InstrumentKind> {
    match contract_type {
        "PERPETUAL" => Some(InstrumentKind::PerpetualInverse),
        "CURRENT_QUARTER" | "NEXT_QUARTER" | "CURRENT_MONTH" | "NEXT_MONTH" => {
            Some(InstrumentKind::FutureInverse)
        }
        _ => None,
    }
}

/// Map Binance Coin-M `symbol.status` → `InstrumentStatus`.
fn map_status(status: &str) -> InstrumentStatus {
    match status {
        "TRADING" => InstrumentStatus::Active,
        "BREAK" | "HALT" | "END_OF_DAY" | "AUCTION_MATCH" | "PENDING_TRADING" | "PRE_TRADING"
        | "POST_TRADING" | "SETTLING" | "CLOSE" | "PENDING_SETTLE" => InstrumentStatus::Suspended,
        "DELIVERING" => InstrumentStatus::Expired,
        _ => InstrumentStatus::Unknown,
    }
}

/// Parse Coin-M inverse futures/perps from dapi exchangeInfo bodies.
pub fn parse_coinm_exchange_info(
    responses: &[HttpResponse],
) -> Result<Vec<InstrumentDefinition>, AdapterError> {
    let mut out = Vec::new();
    for resp in responses {
        if resp.status != 200 {
            return Err(AdapterError::Catalog(format!(
                "coinm exchangeInfo HTTP {}",
                resp.status
            )));
        }
        let info: ExchangeInfo =
            serde_json::from_slice(&resp.body).map_err(|e| AdapterError::Catalog(e.to_string()))?;
        for sym in info.symbols {
            let status = map_status(&sym.status);
            let Some(kind) = kind_for(&sym.contract_type) else {
                continue;
            };
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
                    venue: VenueCode("binance-coinm".into()),
                    native_symbol: sym.symbol,
                    kind,
                    settlement: Some(AssetCode(sym.margin_asset.clone())),
                    expiry_ns: None,
                },
                base: AssetCode(sym.base_asset),
                quote: AssetCode(sym.quote_asset),
                settlement: Some(AssetCode(sym.margin_asset)),
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
                inverse: true,
            });
        }
    }
    if out.is_empty() {
        return Err(AdapterError::Catalog(
            "coinm exchangeInfo produced no symbols".into(),
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn parses_inverse_perpetual() {
        let body = br#"{
          "symbols":[{
            "symbol":"BTCUSD_PERP",
            "status":"TRADING",
            "contractType":"PERPETUAL",
            "baseAsset":"BTC",
            "quoteAsset":"USD",
            "marginAsset":"BTC",
            "filters":[
              {"filterType":"PRICE_FILTER","tickSize":"0.10"},
              {"filterType":"LOT_SIZE","stepSize":"1"}
            ]
          },{
            "symbol":"DEAD_PERP",
            "status":"BREAK",
            "contractType":"PERPETUAL",
            "baseAsset":"X",
            "quoteAsset":"USD",
            "marginAsset":"X",
            "filters":[
              {"filterType":"PRICE_FILTER","tickSize":"0.10"},
              {"filterType":"LOT_SIZE","stepSize":"1"}
            ]
          }]
        }"#;
        let defs = parse_coinm_exchange_info(&[HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(body),
        }])
        .unwrap();
        assert_eq!(defs.len(), 2);
        assert_eq!(defs[0].key.native_symbol, "BTCUSD_PERP");
        assert_eq!(defs[0].key.kind, InstrumentKind::PerpetualInverse);
        assert_eq!(defs[0].status, InstrumentStatus::Active);
        assert!(defs[0].inverse);
        assert_eq!(defs[0].price_scale, 2);
        assert_eq!(defs[0].quantity_scale, 0);
        assert_eq!(defs[1].key.native_symbol, "DEAD_PERP");
        assert_eq!(defs[1].status, InstrumentStatus::Suspended);
    }
}
