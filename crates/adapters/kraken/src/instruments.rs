//! Kraken Spot `AssetPairs` parsing for price/qty scales.

use marketfeed_adapter_api::{AdapterError, HttpResponse};
use marketfeed_model::{
    AssetCode, Fixed, InstrumentDefinition, InstrumentKey, InstrumentKind, InstrumentStatus,
    VenueCode,
};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
struct AssetPairsResponse {
    error: Vec<String>,
    result: serde_json::Map<String, Value>,
}

#[derive(Debug, Deserialize)]
struct PairInfo {
    #[allow(dead_code)]
    altname: Option<String>,
    wsname: Option<String>,
    base: String,
    quote: String,
    pair_decimals: u8,
    lot_decimals: u8,
    status: Option<String>,
}

/// Parse spot pairs from AssetPairs HTTP bodies.
pub fn parse_asset_pairs(
    responses: &[HttpResponse],
) -> Result<Vec<InstrumentDefinition>, AdapterError> {
    let mut out = Vec::new();
    for resp in responses {
        if resp.status != 200 {
            return Err(AdapterError::Catalog(format!(
                "AssetPairs HTTP {}",
                resp.status
            )));
        }
        let body: AssetPairsResponse =
            serde_json::from_slice(&resp.body).map_err(|e| AdapterError::Catalog(e.to_string()))?;
        if !body.error.is_empty() {
            return Err(AdapterError::Catalog(format!(
                "AssetPairs error: {:?}",
                body.error
            )));
        }
        for (_key, val) in body.result {
            let info: PairInfo =
                serde_json::from_value(val).map_err(|e| AdapterError::Catalog(e.to_string()))?;
            let status = map_status(info.status.as_deref());
            let Some(symbol) = info.wsname.clone() else {
                continue;
            };
            let price_inc = Fixed::new(1, info.pair_decimals);
            let qty_inc = Fixed::new(1, info.lot_decimals);
            out.push(InstrumentDefinition {
                key: InstrumentKey {
                    venue: VenueCode("kraken-spot".into()),
                    native_symbol: symbol,
                    kind: InstrumentKind::Spot,
                    settlement: None,
                    expiry_ns: None,
                },
                base: AssetCode(info.base),
                quote: AssetCode(info.quote),
                settlement: None,
                price_scale: info.pair_decimals,
                quantity_scale: info.lot_decimals,
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
        return Err(AdapterError::Catalog("AssetPairs produced no pairs".into()));
    }
    Ok(out)
}

/// Map Kraken Spot `status` → `InstrumentStatus`.
fn map_status(status: Option<&str>) -> InstrumentStatus {
    match status {
        Some("online") | None => InstrumentStatus::Active,
        Some("cancel_only" | "post_only" | "limit_only" | "reduce_only") => {
            InstrumentStatus::Suspended
        }
        Some("delisted") => InstrumentStatus::Delisted,
        Some(_) => InstrumentStatus::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn parses_wsname_and_scales() {
        let body = br#"{
          "error":[],
          "result":{
            "XXBTZUSD":{
              "altname":"XBTUSD",
              "wsname":"XBT/USD",
              "base":"XXBT",
              "quote":"ZUSD",
              "pair_decimals":1,
              "lot_decimals":8,
              "status":"online"
            },
            "DEAD":{
              "altname":"DEAD",
              "wsname":"DEAD/USD",
              "base":"X",
              "quote":"Y",
              "pair_decimals":1,
              "lot_decimals":1,
              "status":"delisted"
            },
            "HALT":{
              "altname":"HALT",
              "wsname":"HALT/USD",
              "base":"H",
              "quote":"ZUSD",
              "pair_decimals":1,
              "lot_decimals":1,
              "status":"cancel_only"
            }
          }
        }"#;
        let defs = parse_asset_pairs(&[HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(body),
        }])
        .unwrap();
        assert_eq!(defs.len(), 3);
        let active = defs
            .iter()
            .find(|d| d.key.native_symbol == "XBT/USD")
            .expect("online");
        assert_eq!(active.status, InstrumentStatus::Active);
        assert_eq!(active.price_scale, 1);
        assert_eq!(active.quantity_scale, 8);
        let dead = defs
            .iter()
            .find(|d| d.key.native_symbol == "DEAD/USD")
            .expect("delisted");
        assert_eq!(dead.status, InstrumentStatus::Delisted);
        let halt = defs
            .iter()
            .find(|d| d.key.native_symbol == "HALT/USD")
            .expect("cancel_only");
        assert_eq!(halt.status, InstrumentStatus::Suspended);
    }
}
