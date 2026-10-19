//! Metadata parsing retains native spot/token identity separately from tickers.
use marketfeed_adapter_api::{AdapterError, HttpResponse};
use marketfeed_model::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Token {
    pub name: String,
    pub sz_decimals: u8,
    pub wei_decimals: u8,
    pub index: u32,
    pub token_id: String,
    #[serde(default)]
    pub is_canonical: bool,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SpotMarket {
    pub name: String,
    pub tokens: [u32; 2],
    pub index: u32,
    #[serde(default)]
    pub is_canonical: bool,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SpotMetadata {
    pub tokens: Vec<Token>,
    pub universe: Vec<SpotMarket>,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PerpMarket {
    name: String,
    sz_decimals: u8,
    #[serde(default)]
    is_delisted: bool,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PerpMetadata {
    universe: Vec<PerpMarket>,
    collateral_token: u32,
}
fn err(s: impl ToString) -> AdapterError {
    AdapterError::Catalog(s.to_string())
}
pub fn parse_spot_metadata(response: &HttpResponse) -> Result<SpotMetadata, AdapterError> {
    if response.status != 200 {
        return Err(err(format!("spotMeta HTTP {}", response.status)));
    }
    let m: SpotMetadata = serde_json::from_slice(&response.body).map_err(err)?;
    let mut indices = BTreeSet::new();
    for t in &m.tokens {
        if !indices.insert(t.index)
            || t.name.is_empty()
            || t.token_id.is_empty()
            || t.sz_decimals > 8
        {
            return Err(err("invalid or duplicate spot token"));
        }
    }
    Ok(m)
}
fn definition(
    native: String,
    base: String,
    quote: String,
    qty: u8,
    spot: bool,
    status: InstrumentStatus,
) -> Result<InstrumentDefinition, AdapterError> {
    // Hyperliquid price precision is additionally constrained to five significant
    // figures (integer prices excepted); this is a maximum decimal precision,
    // not a promise of a uniform executable tick at every magnitude.
    let price_scale = (if spot { 8u8 } else { 6u8 })
        .checked_sub(qty)
        .ok_or_else(|| err("invalid szDecimals"))?;
    let settlement = if spot {
        None
    } else {
        Some(AssetCode(quote.clone()))
    };
    Ok(InstrumentDefinition {
        key: InstrumentKey {
            venue: VenueCode(
                if spot {
                    "hyperliquid-spot"
                } else {
                    "hyperliquid-perps"
                }
                .into(),
            ),
            native_symbol: native,
            kind: if spot {
                InstrumentKind::Spot
            } else {
                InstrumentKind::PerpetualLinear
            },
            settlement: settlement.clone(),
            expiry_ns: None,
        },
        base: AssetCode(base),
        quote: AssetCode(quote),
        settlement,
        price_scale,
        quantity_scale: qty,
        price_increment: Fixed::new(1, price_scale),
        quantity_increment: Fixed::new(1, qty),
        min_quantity: None,
        max_quantity: None,
        min_notional: None,
        contract_size: None,
        expiry_ns: None,
        status,
        inverse: false,
    })
}
pub fn parse_instruments(
    responses: &[HttpResponse],
    spot: bool,
) -> Result<Vec<InstrumentDefinition>, AdapterError> {
    let r = responses
        .first()
        .ok_or_else(|| err("missing spotMeta response"))?;
    let m = parse_spot_metadata(r)?;
    let tokens: BTreeMap<_, _> = m.tokens.iter().map(|t| (t.index, t)).collect();
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    if spot {
        for p in m.universe {
            if !seen.insert(p.index.to_string()) {
                return Err(err("duplicate spot market index"));
            }
            let base = tokens
                .get(&p.tokens[0])
                .ok_or_else(|| err("unknown base token index"))?;
            let quote = tokens
                .get(&p.tokens[1])
                .ok_or_else(|| err("unknown quote token index"))?;
            let native = if p.index == 0 && p.name == "PURR/USDC" {
                p.name
            } else {
                format!("@{}", p.index)
            };
            out.push(definition(
                native,
                base.name.clone(),
                quote.name.clone(),
                base.sz_decimals,
                true,
                InstrumentStatus::Active,
            )?);
        }
    } else {
        let r = responses
            .get(1)
            .ok_or_else(|| err("missing perp meta response"))?;
        if r.status != 200 {
            return Err(err(format!("meta HTTP {}", r.status)));
        }
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum PerpDiscovery {
            All(Vec<PerpMetadata>),
            Single(PerpMetadata),
        }
        let metas = match serde_json::from_slice::<PerpDiscovery>(&r.body).map_err(err)? {
            PerpDiscovery::All(m) => m,
            PerpDiscovery::Single(m) => vec![m],
        };
        if metas.is_empty() || metas.len() > 64 {
            return Err(err("invalid perpetual DEX discovery count"));
        }
        for m in metas {
            let collateral = tokens
                .get(&m.collateral_token)
                .ok_or_else(|| err("unknown perp collateral token"))?;
            for p in m.universe {
                if p.name.is_empty() || !seen.insert(p.name.clone()) {
                    return Err(err("invalid or duplicate perp name"));
                }
                out.push(definition(
                    p.name.clone(),
                    p.name,
                    collateral.name.clone(),
                    p.sz_decimals,
                    false,
                    if p.is_delisted {
                        InstrumentStatus::Delisted
                    } else {
                        InstrumentStatus::Active
                    },
                )?);
            }
        }
    }
    if out.is_empty() {
        return Err(err("empty Hyperliquid catalog"));
    }
    Ok(out)
}
