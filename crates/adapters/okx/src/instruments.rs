//! OKX `GET /api/v5/public/instruments` parsing for SPOT, SWAP, and FUTURES.

use marketfeed_adapter_api::{AdapterError, HttpResponse};
use marketfeed_model::{
    AssetCode, Fixed, InstrumentDefinition, InstrumentKey, InstrumentKind, InstrumentStatus,
    VenueCode,
};
use serde::Deserialize;

/// OKX `instType` segment being parsed; drives both the REST query and the
/// resulting `InstrumentKind`/`VenueCode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OkxInstType {
    Spot,
    Swap,
    Futures,
}

impl OkxInstType {
    pub fn api_value(self) -> &'static str {
        match self {
            Self::Spot => "SPOT",
            Self::Swap => "SWAP",
            Self::Futures => "FUTURES",
        }
    }

    fn venue_code(self) -> &'static str {
        match self {
            Self::Spot => "okx-spot",
            Self::Swap => "okx-swap",
            Self::Futures => "okx-futures",
        }
    }
}

#[derive(Debug, Deserialize)]
struct InstrumentsBody {
    code: String,
    data: Vec<InstrumentRow>,
}

#[derive(Debug, Deserialize, Default)]
struct InstrumentRow {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "instType")]
    inst_type: String,
    #[serde(rename = "baseCcy", default)]
    base_ccy: String,
    #[serde(rename = "quoteCcy", default)]
    quote_ccy: String,
    #[serde(rename = "tickSz")]
    tick_sz: String,
    #[serde(rename = "lotSz")]
    lot_sz: String,
    #[serde(rename = "minSz")]
    min_sz: Option<String>,
    state: String,
    /// SWAP/FUTURES only: "linear" or "inverse".
    #[serde(rename = "ctType", default)]
    ct_type: Option<String>,
    /// SWAP/FUTURES only: contract value in `ctValCcy` units.
    #[serde(rename = "ctVal", default)]
    ct_val: Option<String>,
    /// SWAP/FUTURES only: settlement/margin currency.
    #[serde(rename = "settleCcy", default)]
    settle_ccy: Option<String>,
    /// SWAP/FUTURES only: underlying pair, e.g. `BTC-USDT`.
    #[serde(default)]
    uly: Option<String>,
    /// FUTURES only: expiry epoch millis as a string; empty/absent for perpetuals.
    #[serde(rename = "expTime", default)]
    exp_time: Option<String>,
}

/// Parse SPOT/SWAP/FUTURES instruments from one or more `instruments` HTTP bodies.
///
/// SPOT rows use `baseCcy`/`quoteCcy` directly. SWAP/FUTURES rows carry no
/// `baseCcy`/`quoteCcy`; base is derived from `uly` (`"BTC-USDT"` -> `"BTC"`).
/// Linear rows take quote/settlement from `settleCcy`; inverse rows take quote
/// from `uly` (`"BTC-USD"` -> `"USD"`) and settlement from `settleCcy` (coin).
///
/// Linear and inverse instruments share `okx-swap` / `okx-futures` VenueIds
/// (same public WS gateway); kinds are `PerpetualLinear`/`FutureLinear` vs
/// `PerpetualInverse`/`FutureInverse`.
pub fn parse_instruments_response(
    responses: &[HttpResponse],
    inst_type: OkxInstType,
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
        if body.code != "0" {
            return Err(AdapterError::Catalog(format!(
                "instruments code {}",
                body.code
            )));
        }
        for row in body.data {
            if row.inst_type != inst_type.api_value() {
                continue;
            }
            let Some(def) = parse_row(row, inst_type)? else {
                continue;
            };
            out.push(def);
        }
    }
    if out.is_empty() {
        return Err(AdapterError::Catalog(format!(
            "instruments produced no {} symbols",
            inst_type.api_value()
        )));
    }
    Ok(out)
}

/// Map OKX `state` → `InstrumentStatus`.
fn map_state(state: &str) -> InstrumentStatus {
    match state {
        "live" => InstrumentStatus::Active,
        "suspend" | "preopen" | "test" => InstrumentStatus::Suspended,
        "expired" => InstrumentStatus::Expired,
        _ => InstrumentStatus::Unknown,
    }
}

fn parse_row(
    row: InstrumentRow,
    inst_type: OkxInstType,
) -> Result<Option<InstrumentDefinition>, AdapterError> {
    let status = map_state(&row.state);
    let price_inc = Fixed::parse_str(&row.tick_sz)
        .map_err(|e| AdapterError::Catalog(format!("tickSz {}: {e}", row.tick_sz)))?;
    let qty_inc = Fixed::parse_str(&row.lot_sz)
        .map_err(|e| AdapterError::Catalog(format!("lotSz {}: {e}", row.lot_sz)))?;
    let min_quantity = row
        .min_sz
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(Fixed::parse_str)
        .transpose()
        .map_err(|e| AdapterError::Catalog(format!("minSz: {e}")))?;

    let (kind, base, quote, settlement, expiry_ns, inverse) = match inst_type {
        OkxInstType::Spot => (
            InstrumentKind::Spot,
            row.base_ccy.clone(),
            row.quote_ccy.clone(),
            None,
            None,
            false,
        ),
        OkxInstType::Swap | OkxInstType::Futures => {
            let inverse = match row.ct_type.as_deref() {
                Some("linear") => false,
                Some("inverse") => true,
                _ => return Ok(None),
            };
            let Some(settle) = row.settle_ccy.clone() else {
                return Ok(None);
            };
            let uly_parts: Vec<&str> = row
                .uly
                .as_deref()
                .unwrap_or("")
                .split('-')
                .filter(|p| !p.is_empty())
                .collect();
            let base = uly_parts
                .first()
                .map(|s| (*s).to_string())
                .unwrap_or_else(|| settle.clone());
            // Linear: quote = settle (USDT/USDC). Inverse: quote from uly (USD).
            let quote = if inverse {
                uly_parts
                    .get(1)
                    .map(|s| (*s).to_string())
                    .unwrap_or_else(|| "USD".into())
            } else {
                settle.clone()
            };
            let kind = match (inst_type, inverse) {
                (OkxInstType::Swap, false) => InstrumentKind::PerpetualLinear,
                (OkxInstType::Swap, true) => InstrumentKind::PerpetualInverse,
                (OkxInstType::Futures, false) => InstrumentKind::FutureLinear,
                (OkxInstType::Futures, true) => InstrumentKind::FutureInverse,
                (OkxInstType::Spot, _) => unreachable!("spot handled above"),
            };
            let expiry_ns = row
                .exp_time
                .as_deref()
                .filter(|s| !s.is_empty())
                .map(str::parse::<i64>)
                .transpose()
                .map_err(|e| AdapterError::Catalog(format!("expTime: {e}")))?
                .map(|ms| ms.saturating_mul(1_000_000));
            (kind, base, quote, Some(settle), expiry_ns, inverse)
        }
    };

    let contract_size = row
        .ct_val
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(Fixed::parse_str)
        .transpose()
        .map_err(|e| AdapterError::Catalog(format!("ctVal: {e}")))?;

    Ok(Some(InstrumentDefinition {
        key: InstrumentKey {
            venue: VenueCode(inst_type.venue_code().into()),
            native_symbol: row.inst_id,
            kind,
            settlement: settlement.clone().map(AssetCode),
            expiry_ns,
        },
        base: AssetCode(base),
        quote: AssetCode(quote),
        settlement: settlement.map(AssetCode),
        price_scale: price_inc.scale,
        quantity_scale: qty_inc.scale,
        price_increment: price_inc,
        quantity_increment: qty_inc,
        min_quantity,
        max_quantity: None,
        min_notional: None,
        contract_size,
        expiry_ns,
        status,
        inverse,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn resp(body: &'static [u8]) -> HttpResponse {
        HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(body),
        }
    }

    #[test]
    fn parses_live_spot_scales() {
        let body = br#"{
          "code":"0",
          "data":[{
            "instId":"BTC-USDT",
            "instType":"SPOT",
            "baseCcy":"BTC",
            "quoteCcy":"USDT",
            "tickSz":"0.1",
            "lotSz":"0.00000001",
            "minSz":"0.00001",
            "state":"live"
          },{
            "instId":"DEAD-USDT",
            "instType":"SPOT",
            "baseCcy":"DEAD",
            "quoteCcy":"USDT",
            "tickSz":"0.1",
            "lotSz":"1",
            "state":"suspend"
          }]
        }"#;
        let defs = parse_instruments_response(&[resp(body)], OkxInstType::Spot).unwrap();
        assert_eq!(defs.len(), 2);
        assert_eq!(defs[0].key.native_symbol, "BTC-USDT");
        assert_eq!(defs[0].status, InstrumentStatus::Active);
        assert_eq!(defs[0].price_scale, 1);
        assert_eq!(defs[0].quantity_scale, 8);
        assert_eq!(defs[1].key.native_symbol, "DEAD-USDT");
        assert_eq!(defs[1].status, InstrumentStatus::Suspended);
    }

    #[test]
    fn parses_swap_perpetual_linear() {
        let body = br#"{
          "code":"0",
          "data":[{
            "instId":"BTC-USDT-SWAP",
            "instType":"SWAP",
            "tickSz":"0.1",
            "lotSz":"1",
            "minSz":"1",
            "state":"live",
            "ctType":"linear",
            "ctVal":"0.01",
            "settleCcy":"USDT",
            "uly":"BTC-USDT"
          },{
            "instId":"BTC-USD-SWAP",
            "instType":"SWAP",
            "tickSz":"0.1",
            "lotSz":"1",
            "state":"live",
            "ctType":"inverse",
            "ctVal":"100",
            "settleCcy":"BTC",
            "uly":"BTC-USD"
          }]
        }"#;
        let defs = parse_instruments_response(&[resp(body)], OkxInstType::Swap).unwrap();
        assert_eq!(defs.len(), 2, "linear + inverse SWAP rows");
        let linear = defs
            .iter()
            .find(|d| d.key.native_symbol == "BTC-USDT-SWAP")
            .expect("linear");
        assert_eq!(linear.key.kind, InstrumentKind::PerpetualLinear);
        assert_eq!(linear.base, AssetCode("BTC".into()));
        assert_eq!(linear.quote, AssetCode("USDT".into()));
        assert_eq!(linear.settlement, Some(AssetCode("USDT".into())));
        assert!(!linear.inverse);
        assert_eq!(linear.contract_size, Some(Fixed::new(1, 2)));

        let inv = defs
            .iter()
            .find(|d| d.key.native_symbol == "BTC-USD-SWAP")
            .expect("inverse");
        assert_eq!(inv.key.kind, InstrumentKind::PerpetualInverse);
        assert_eq!(inv.base, AssetCode("BTC".into()));
        assert_eq!(inv.quote, AssetCode("USD".into()));
        assert_eq!(inv.settlement, Some(AssetCode("BTC".into())));
        assert!(inv.inverse);
        assert_eq!(inv.contract_size, Some(Fixed::new(100, 0)));
    }

    #[test]
    fn parses_futures_expiry() {
        let body = br#"{
          "code":"0",
          "data":[{
            "instId":"BTC-USDT-250328",
            "instType":"FUTURES",
            "tickSz":"0.1",
            "lotSz":"1",
            "state":"live",
            "ctType":"linear",
            "ctVal":"0.01",
            "settleCcy":"USDT",
            "uly":"BTC-USDT",
            "expTime":"1774684800000"
          }]
        }"#;
        let defs = parse_instruments_response(&[resp(body)], OkxInstType::Futures).unwrap();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].key.kind, InstrumentKind::FutureLinear);
        assert_eq!(defs[0].expiry_ns, Some(1_774_684_800_000_000_000));
    }
}
