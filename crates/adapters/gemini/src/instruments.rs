//! Gemini Spot `GET /v1/symbols` (+ optional capped `/v1/symbols/details/{symbol}`).

use marketfeed_adapter_api::{AdapterError, HttpResponse};
use marketfeed_model::{
    AssetCode, Fixed, InstrumentDefinition, InstrumentKey, InstrumentKind, InstrumentStatus,
    VenueCode,
};
use serde::Deserialize;
use serde_json::Value;

pub const DEFAULT_PRICE_SCALE: u8 = 2;
pub const DEFAULT_QTY_SCALE: u8 = 8;
pub const LIVE_DETAILS_MAX_ENV: &str = "GEMINI_LIVE_DETAILS_MAX";

pub fn live_details_max_from_env() -> usize {
    std::env::var(LIVE_DETAILS_MAX_ENV)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// Parse spot symbols from `GET /v1/symbols`.
///
/// # ponytail
/// List endpoint has no tick/lot sizes — default scales 2/8. Ceiling: coarse
/// catalog. Upgrade: set `GEMINI_LIVE_DETAILS_MAX` for capped N+1 details.
pub fn parse_symbols(
    responses: &[HttpResponse],
) -> Result<Vec<InstrumentDefinition>, AdapterError> {
    let mut out = Vec::new();
    for resp in responses {
        if resp.status != 200 {
            return Err(AdapterError::Catalog(format!(
                "symbols HTTP {}",
                resp.status
            )));
        }
        let symbols: Vec<String> =
            serde_json::from_slice(&resp.body).map_err(|e| AdapterError::Catalog(e.to_string()))?;
        for sym in symbols {
            if !sym.is_empty() {
                out.push(def_from_symbol(&sym));
            }
        }
    }
    if out.is_empty() {
        return Err(AdapterError::Catalog(
            "symbols produced no spot instruments".into(),
        ));
    }
    Ok(out)
}

fn def_from_symbol(sym: &str) -> InstrumentDefinition {
    let native = sym.to_ascii_uppercase();
    let (base, quote) = split_gemini_symbol(&native);
    InstrumentDefinition {
        key: InstrumentKey {
            venue: VenueCode("gemini".into()),
            native_symbol: native,
            kind: InstrumentKind::Spot,
            settlement: None,
            expiry_ns: None,
        },
        base: AssetCode(base),
        quote: AssetCode(quote),
        settlement: None,
        price_scale: DEFAULT_PRICE_SCALE,
        quantity_scale: DEFAULT_QTY_SCALE,
        price_increment: Fixed::new(1, DEFAULT_PRICE_SCALE),
        quantity_increment: Fixed::new(1, DEFAULT_QTY_SCALE),
        min_quantity: None,
        max_quantity: None,
        min_notional: None,
        contract_size: None,
        expiry_ns: None,
        status: InstrumentStatus::Active,
        inverse: false,
    }
}

const QUOTE_SUFFIXES: &[&str] = &[
    "usdt", "usdc", "gusd", "usd", "btc", "eth", "eur", "gbp", "sgd", "dai",
];

fn split_gemini_symbol(pair: &str) -> (String, String) {
    let lower = pair.to_ascii_lowercase();
    for q in QUOTE_SUFFIXES {
        if lower.len() > q.len() && lower.ends_with(q) {
            let base = &lower[..lower.len() - q.len()];
            return (base.to_ascii_uppercase(), q.to_ascii_uppercase());
        }
    }
    if lower.len() > 3 {
        let (b, q) = lower.split_at(lower.len() - 3);
        return (b.to_ascii_uppercase(), q.to_ascii_uppercase());
    }
    (lower.to_ascii_uppercase(), String::new())
}

#[derive(Debug, Deserialize)]
struct SymbolDetails {
    symbol: String,
    #[serde(default)]
    base_currency: String,
    #[serde(default)]
    quote_currency: String,
    #[serde(default)]
    tick_size: Option<Value>,
    #[serde(default)]
    quote_increment: Option<Value>,
    #[serde(default)]
    min_order_size: Option<Value>,
}

pub fn apply_symbol_details(
    responses: &[HttpResponse],
    defs: &mut [InstrumentDefinition],
) -> Result<(), AdapterError> {
    for resp in responses {
        if resp.status != 200 {
            continue;
        }
        let Ok(detail) = serde_json::from_slice::<SymbolDetails>(&resp.body) else {
            continue;
        };
        let sym = detail.symbol.to_ascii_uppercase();
        let Some(def) = defs
            .iter_mut()
            .find(|d| d.key.native_symbol.eq_ignore_ascii_case(&sym))
        else {
            continue;
        };
        if !detail.base_currency.is_empty() {
            def.base = AssetCode(detail.base_currency.to_ascii_uppercase());
        }
        if !detail.quote_currency.is_empty() {
            def.quote = AssetCode(detail.quote_currency.to_ascii_uppercase());
        }
        let price_inc = detail
            .quote_increment
            .as_ref()
            .or(detail.tick_size.as_ref())
            .and_then(fixed_from_json)
            .unwrap_or(Fixed::new(1, DEFAULT_PRICE_SCALE));
        let qty_inc = detail
            .min_order_size
            .as_ref()
            .and_then(fixed_from_json)
            .unwrap_or(Fixed::new(1, DEFAULT_QTY_SCALE));
        def.price_scale = price_inc.scale;
        def.quantity_scale = qty_inc.scale;
        def.price_increment = price_inc;
        def.quantity_increment = qty_inc;
        def.min_quantity = Some(qty_inc);
    }
    Ok(())
}

fn fixed_from_json(v: &Value) -> Option<Fixed> {
    match v {
        Value::String(s) => Fixed::parse_str(s).ok(),
        Value::Number(n) => Fixed::parse_str(&n.to_string()).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn parses_symbols_list_default_scales() {
        let defs = parse_symbols(&[HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(br#"["btcusd","ethbtc","gusdusd"]"#),
        }])
        .unwrap();
        assert_eq!(defs.len(), 3);
        assert_eq!(defs[0].key.native_symbol, "BTCUSD");
        assert_eq!(defs[0].base.0, "BTC");
        assert_eq!(defs[0].quote.0, "USD");
        assert_eq!(defs[2].base.0, "GUSD");
        assert_eq!(defs[2].quote.0, "USD");
    }

    #[test]
    fn applies_capped_details_overlay() {
        let mut defs = parse_symbols(&[HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(br#"["btcusd","ethusd"]"#),
        }])
        .unwrap();
        apply_symbol_details(&[HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(br#"{"symbol":"BTCUSD","base_currency":"BTC","quote_currency":"USD","quote_increment":"0.01","min_order_size":"0.00001"}"#),
        }], &mut defs).unwrap();
        assert_eq!(defs[0].quantity_scale, 5);
        assert_eq!(defs[1].quantity_scale, DEFAULT_QTY_SCALE);
    }

    #[test]
    fn empty_symbols_errors() {
        let err = parse_symbols(&[HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(br#"[]"#),
        }])
        .unwrap_err();
        assert!(err.to_string().contains("no spot instruments"));
    }
}
