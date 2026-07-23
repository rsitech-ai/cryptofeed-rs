//! Bitfinex instrument discovery REST parsing (spot + derivatives).

use marketfeed_adapter_api::{AdapterError, HttpResponse};
use marketfeed_model::{
    AssetCode, Fixed, InstrumentDefinition, InstrumentKey, InstrumentKind, InstrumentStatus,
    VenueCode,
};

pub fn parse_pair_list(
    responses: &[HttpResponse],
) -> Result<Vec<InstrumentDefinition>, AdapterError> {
    parse_pair_list_inner(
        responses,
        "bitfinex",
        InstrumentKind::Spot,
        "pub:list:pair:exchange",
    )
}

pub fn parse_futures_pair_list(
    responses: &[HttpResponse],
) -> Result<Vec<InstrumentDefinition>, AdapterError> {
    parse_pair_list_inner(
        responses,
        "bitfinex-deriv",
        InstrumentKind::PerpetualLinear,
        "pub:list:pair:futures",
    )
}

fn parse_pair_list_inner(
    responses: &[HttpResponse],
    venue_code: &str,
    default_kind: InstrumentKind,
    endpoint: &str,
) -> Result<Vec<InstrumentDefinition>, AdapterError> {
    let mut out = Vec::new();
    for resp in responses {
        if resp.status != 200 {
            return Err(AdapterError::Catalog(format!(
                "{endpoint} HTTP {}",
                resp.status
            )));
        }
        let wrapped: Vec<Vec<String>> =
            serde_json::from_slice(&resp.body).map_err(|e| AdapterError::Catalog(e.to_string()))?;
        let pairs = wrapped.into_iter().next().unwrap_or_default();
        for pair in pairs {
            if pair.is_empty() {
                continue;
            }
            let (base, quote) = split_bfx_pair(&pair);
            let kind = if venue_code == "bitfinex-deriv" {
                deriv_kind(&quote)
            } else {
                default_kind
            };
            out.push(InstrumentDefinition {
                key: InstrumentKey {
                    venue: VenueCode(venue_code.into()),
                    native_symbol: format!("t{pair}"),
                    kind,
                    settlement: None,
                    expiry_ns: None,
                },
                base: AssetCode(base),
                quote: AssetCode(quote),
                settlement: None,
                price_scale: 2,
                quantity_scale: 8,
                price_increment: Fixed::new(1, 2),
                quantity_increment: Fixed::new(1, 8),
                min_quantity: None,
                max_quantity: None,
                min_notional: None,
                contract_size: None,
                expiry_ns: None,
                status: InstrumentStatus::Active,
                inverse: kind == InstrumentKind::PerpetualInverse,
            });
        }
    }
    if out.is_empty() {
        return Err(AdapterError::Catalog(format!(
            "{endpoint} produced no instruments"
        )));
    }
    Ok(out)
}

fn split_bfx_pair(pair: &str) -> (String, String) {
    if let Some((b, q)) = pair.split_once(':') {
        return (b.to_string(), q.to_string());
    }
    if pair.len() > 3 {
        let (b, q) = pair.split_at(pair.len() - 3);
        return (b.to_string(), q.to_string());
    }
    (pair.to_string(), String::new())
}

fn deriv_kind(quote: &str) -> InstrumentKind {
    let q = quote.to_ascii_uppercase();
    if q.ends_with("USTF0") || q.ends_with("USDTF0") || q == "UST" || q == "USD" {
        InstrumentKind::PerpetualLinear
    } else {
        InstrumentKind::PerpetualInverse
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn parses_pair_list() {
        let defs = parse_pair_list(&[HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(br#"[["BTCUSD","AAVE:USD"]]"#),
        }])
        .unwrap();
        assert_eq!(defs[0].key.native_symbol, "tBTCUSD");
        assert_eq!(defs[1].key.native_symbol, "tAAVE:USD");
    }

    #[test]
    fn parses_futures_pair_list_linear_and_inverse() {
        let defs = parse_futures_pair_list(&[HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: Bytes::from_static(br#"[["BTCF0:USTF0","ETHF0:BTCF0"]]"#),
        }])
        .unwrap();
        assert_eq!(defs[0].key.kind, InstrumentKind::PerpetualLinear);
        assert_eq!(defs[1].key.kind, InstrumentKind::PerpetualInverse);
        assert_eq!(defs[0].key.venue.0, "bitfinex-deriv");
    }
}
