//! Kraken Futures `GET /derivatives/api/v3/instruments` catalog parse.

use marketfeed_adapter_api::{AdapterError, HttpResponse};
use marketfeed_model::{
    AssetCode, Fixed, InstrumentDefinition, InstrumentKey, InstrumentKind, InstrumentStatus,
    VenueCode,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct InstrumentsBody {
    result: String,
    instruments: Vec<InstrumentRow>,
}

#[derive(Debug, Deserialize)]
struct InstrumentRow {
    symbol: String,
    #[serde(rename = "type", default)]
    instrument_type: Option<String>,
    #[serde(default)]
    underlying: Option<String>,
    #[serde(default)]
    tradeable: Option<bool>,
    #[serde(rename = "tickSize", default)]
    tick_size: Option<serde_json::Value>,
    #[serde(rename = "contractSize", default)]
    contract_size: Option<serde_json::Value>,
}

/// Parse tradeable futures instruments (PF_/PI_/FI_ public symbols).
pub fn parse_futures_instruments(
    responses: &[HttpResponse],
) -> Result<Vec<InstrumentDefinition>, AdapterError> {
    let mut out = Vec::new();
    for resp in responses {
        if resp.status != 200 {
            return Err(AdapterError::Catalog(format!(
                "instruments HTTP {}",
                resp.status
            )));
        }
        let body: InstrumentsBody =
            serde_json::from_slice(&resp.body).map_err(|e| AdapterError::Catalog(e.to_string()))?;
        if body.result != "success" {
            return Err(AdapterError::Catalog(format!(
                "instruments result {}",
                body.result
            )));
        }
        for row in body.instruments {
            let status = map_tradeable(row.tradeable);
            let Some(def) = parse_row(row, status)? else {
                continue;
            };
            out.push(def);
        }
    }
    if out.is_empty() {
        return Err(AdapterError::Catalog(
            "instruments produced no futures symbols".into(),
        ));
    }
    Ok(out)
}

/// Map Kraken Futures `tradeable` → `InstrumentStatus`.
fn map_tradeable(tradeable: Option<bool>) -> InstrumentStatus {
    match tradeable {
        Some(false) => InstrumentStatus::Suspended,
        Some(true) | None => InstrumentStatus::Active,
    }
}

fn parse_row(
    row: InstrumentRow,
    status: InstrumentStatus,
) -> Result<Option<InstrumentDefinition>, AdapterError> {
    let tick = row
        .tick_size
        .as_ref()
        .map(fixed_from_json)
        .transpose()
        .map_err(AdapterError::Catalog)?
        .unwrap_or_else(|| Fixed::new(1, 1));
    let contract_size = row
        .contract_size
        .as_ref()
        .map(fixed_from_json)
        .transpose()
        .map_err(AdapterError::Catalog)?;

    let underlying = row.underlying.unwrap_or_else(|| "XBT".into());
    // PF_ = flexible USD-margined perp (linear), PI_ = inverse perp, FI_ = inverse dated.
    let (kind, inverse) = match row.instrument_type.as_deref() {
        Some("flexible_futures") | Some("futures_inverse") if row.symbol.starts_with("PF_") => {
            (InstrumentKind::PerpetualLinear, false)
        }
        Some("futures_inverse") | Some("inverse_futures") if row.symbol.starts_with("PI_") => {
            (InstrumentKind::PerpetualInverse, true)
        }
        Some("futures_inverse") | Some("inverse_futures") if row.symbol.starts_with("FI_") => {
            (InstrumentKind::FutureInverse, true)
        }
        // Fallback on symbol prefix when type is missing / unexpected.
        _ if row.symbol.starts_with("PF_") => (InstrumentKind::PerpetualLinear, false),
        _ if row.symbol.starts_with("PI_") => (InstrumentKind::PerpetualInverse, true),
        _ if row.symbol.starts_with("FI_") => (InstrumentKind::FutureInverse, true),
        _ => return Ok(None),
    };

    let quote = "USD";
    let settlement = if inverse {
        underlying.clone()
    } else {
        "USD".into()
    };

    Ok(Some(InstrumentDefinition {
        key: InstrumentKey {
            venue: VenueCode("kraken-futures".into()),
            native_symbol: row.symbol,
            kind,
            settlement: Some(AssetCode(settlement.clone())),
            expiry_ns: None,
        },
        base: AssetCode(underlying),
        quote: AssetCode(quote.into()),
        settlement: Some(AssetCode(settlement)),
        price_scale: tick.scale,
        quantity_scale: 0,
        price_increment: tick,
        quantity_increment: Fixed::new(1, 0),
        min_quantity: None,
        max_quantity: None,
        min_notional: None,
        contract_size,
        expiry_ns: None,
        status,
        inverse,
    }))
}

fn fixed_from_json(v: &serde_json::Value) -> Result<Fixed, String> {
    match v {
        serde_json::Value::String(s) => Fixed::parse_str(s).map_err(|e| e.to_string()),
        serde_json::Value::Number(n) => Fixed::parse_str(&n.to_string()).map_err(|e| e.to_string()),
        _ => Err("tick/contract not string/number".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn parses_pf_perp() {
        let body = br#"{
          "result":"success",
          "instruments":[{
            "symbol":"PF_XBTUSD",
            "type":"flexible_futures",
            "underlying":"XBT",
            "tradeable":true,
            "tickSize":0.5,
            "contractSize":1
          },{
            "symbol":"PI_DEADUSD",
            "type":"futures_inverse",
            "underlying":"DEAD",
            "tradeable":false,
            "tickSize":1,
            "contractSize":1
          }]
        }"#;
        let defs = parse_futures_instruments(&[HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(body),
        }])
        .unwrap();
        assert_eq!(defs.len(), 2);
        assert_eq!(defs[0].key.native_symbol, "PF_XBTUSD");
        assert_eq!(defs[0].key.kind, InstrumentKind::PerpetualLinear);
        assert_eq!(defs[0].status, InstrumentStatus::Active);
        assert!(!defs[0].inverse);
        assert_eq!(defs[1].key.native_symbol, "PI_DEADUSD");
        assert_eq!(defs[1].status, InstrumentStatus::Suspended);
    }

    #[test]
    fn parses_scientific_notation_tick_size_from_live_catalog() {
        let body = br#"{
          "result":"success",
          "instruments":[{
            "symbol":"PF_GMTUSD",
            "type":"flexible_futures",
            "underlying":"GMT",
            "tradeable":true,
            "tickSize":1e-06,
            "contractSize":1
          }]
        }"#;

        let defs = parse_futures_instruments(&[HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(body),
        }])
        .unwrap();

        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].price_increment, Fixed::new(1, 6));
        assert_eq!(defs[0].price_scale, 6);
    }
}
