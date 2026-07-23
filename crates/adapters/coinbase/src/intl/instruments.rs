//! Coinbase International `GET /api/v1/instruments` REST parsing (public, no auth).

use marketfeed_adapter_api::{AdapterError, HttpResponse};
use marketfeed_model::{
    AssetCode, Fixed, InstrumentDefinition, InstrumentKey, InstrumentKind, InstrumentStatus,
    VenueCode,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct InstrumentRow {
    symbol: String,
    #[serde(rename = "type")]
    instrument_type: String,
    #[serde(default)]
    base_asset_name: String,
    #[serde(default)]
    quote_asset_name: String,
    #[serde(default)]
    quote_increment: Option<String>,
    #[serde(default)]
    base_increment: Option<String>,
    #[serde(default)]
    min_quantity: Option<String>,
    #[serde(default)]
    min_notional_value: Option<String>,
    #[serde(default)]
    trading_state: String,
}

pub fn parse_instruments(
    responses: &[HttpResponse],
) -> Result<Vec<InstrumentDefinition>, AdapterError> {
    let mut out = Vec::new();
    for resp in responses {
        if resp.status != 200 {
            return Err(AdapterError::Catalog(format!(
                "intl instruments HTTP {}",
                resp.status
            )));
        }
        let rows: Vec<InstrumentRow> =
            serde_json::from_slice(&resp.body).map_err(|e| AdapterError::Catalog(e.to_string()))?;
        for row in rows {
            if !row.instrument_type.eq_ignore_ascii_case("PERP") {
                continue;
            }
            if row.base_asset_name.is_empty() || row.quote_asset_name.is_empty() {
                continue;
            }
            let price_inc = row
                .quote_increment
                .as_deref()
                .map(Fixed::parse_str)
                .transpose()
                .map_err(|e| AdapterError::Catalog(e.to_string()))?
                .unwrap_or(Fixed::new(1, 1));
            let qty_inc = row
                .base_increment
                .as_deref()
                .map(Fixed::parse_str)
                .transpose()
                .map_err(|e| AdapterError::Catalog(e.to_string()))?
                .unwrap_or(Fixed::new(1, 4));
            let min_quantity = row
                .min_quantity
                .as_deref()
                .map(Fixed::parse_str)
                .transpose()
                .map_err(|e| AdapterError::Catalog(e.to_string()))?
                .or(Some(qty_inc));
            let min_notional = row
                .min_notional_value
                .as_deref()
                .map(Fixed::parse_str)
                .transpose()
                .map_err(|e| AdapterError::Catalog(e.to_string()))?;
            out.push(InstrumentDefinition {
                key: InstrumentKey {
                    venue: VenueCode("coinbase-intl".into()),
                    native_symbol: row.symbol,
                    kind: InstrumentKind::PerpetualLinear,
                    settlement: None,
                    expiry_ns: None,
                },
                base: AssetCode(row.base_asset_name),
                quote: AssetCode(row.quote_asset_name),
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
                status: map_status(&row.trading_state),
                inverse: false,
            });
        }
    }
    if out.is_empty() {
        return Err(AdapterError::Catalog(
            "intl instruments produced no PERP instruments".into(),
        ));
    }
    Ok(out)
}

fn map_status(trading_state: &str) -> InstrumentStatus {
    match trading_state {
        "TRADING" | "" => InstrumentStatus::Active,
        "PAUSED" | "HALTED" => InstrumentStatus::Suspended,
        "DELISTED" => InstrumentStatus::Delisted,
        _ => InstrumentStatus::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn parses_perp_instruments() {
        let body = br#"[
          {"symbol":"BTC-PERP","type":"PERP","base_asset_name":"BTC","quote_asset_name":"USDC",
           "quote_increment":"0.1","base_increment":"0.0001","min_quantity":"0.0001",
           "min_notional_value":"10","trading_state":"TRADING"},
          {"symbol":"ETH-SPOT","type":"SPOT","base_asset_name":"ETH","quote_asset_name":"USDC",
           "trading_state":"TRADING"},
          {"symbol":"ETH-PERP","type":"PERP","base_asset_name":"ETH","quote_asset_name":"USDC",
           "quote_increment":"0.01","base_increment":"0.0001","trading_state":"PAUSED"}
        ]"#;
        let defs = parse_instruments(&[HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(body),
        }])
        .unwrap();
        assert_eq!(defs.len(), 2);
        assert_eq!(defs[0].key.native_symbol, "BTC-PERP");
        assert_eq!(defs[0].key.kind, InstrumentKind::PerpetualLinear);
        assert_eq!(defs[1].status, InstrumentStatus::Suspended);
    }
}
