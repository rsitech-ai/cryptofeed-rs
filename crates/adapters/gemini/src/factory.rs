//! Gemini Spot VenueFactory.

use std::collections::HashMap;

use marketfeed_adapter_api::{
    AdapterError, CandleInterval, Channel, ConcreteSubscriptionSet, Environment, HttpRequestSpec,
    HttpResponse, SessionMachine, SessionSpec, VenueFactory, VenueSpecification,
};
use marketfeed_model::CatalogView;

use crate::instruments::{apply_symbol_details, live_details_max_from_env, parse_symbols};
use crate::session::{GeminiSession, GeminiSessionConfig};
use crate::specification::{GEMINI_SPEC, GEMINI_VENUE_ID, REST_BASE, ws_url};

#[derive(Debug, Clone)]
pub struct GeminiFactory {
    pub enable_l2: bool,
    /// Cap on `/v1/symbols/details/{symbol}` N+1 follow-ups (`0` = list only).
    pub live_details_max: usize,
}

impl Default for GeminiFactory {
    fn default() -> Self {
        Self {
            enable_l2: false,
            live_details_max: live_details_max_from_env(),
        }
    }
}

pub fn candle_intervals_from(request: &ConcreteSubscriptionSet) -> Vec<CandleInterval> {
    let mut out = Vec::new();
    for item in &request.items {
        if let Channel::Candles { interval } = item.channel {
            if !out.contains(&interval) {
                out.push(interval);
            }
        }
    }
    out
}

/// # ponytail
/// Empty catalog → default BTCUSD stub (daemon config stubs fill symbols).
pub fn session_config_from_catalog(
    catalog: &CatalogView,
    enable_l2: bool,
    candle_intervals: Vec<CandleInterval>,
) -> GeminiSessionConfig {
    if catalog.instruments.is_empty() {
        return GeminiSessionConfig {
            enable_l2,
            candle_intervals,
            ..GeminiSessionConfig::default()
        };
    }
    let mut instrument_ids = HashMap::new();
    let mut symbols = Vec::with_capacity(catalog.instruments.len());
    let mut price_scale = 2u8;
    let mut qty_scale = 8u8;
    for (i, inst) in catalog.instruments.iter().enumerate() {
        symbols.push(inst.key.native_symbol.clone());
        instrument_ids.insert(inst.key.native_symbol.clone(), inst.id);
        if i == 0 {
            price_scale = inst.price_scale;
            qty_scale = inst.quantity_scale;
        }
    }
    GeminiSessionConfig {
        symbols,
        instrument_ids,
        enable_l2,
        candle_intervals,
        price_scale,
        qty_scale,
        ..GeminiSessionConfig::default()
    }
}

fn session_config_from_request(
    catalog: &CatalogView,
    request: &ConcreteSubscriptionSet,
    fallback_enable_l2: bool,
) -> Result<GeminiSessionConfig, AdapterError> {
    if request.items.is_empty() {
        return Ok(session_config_from_catalog(
            catalog,
            fallback_enable_l2,
            Vec::new(),
        ));
    }

    for item in &request.items {
        match &item.channel {
            Channel::Trades
            | Channel::Quote
            | Channel::L2Book { .. }
            | Channel::Candles { .. }
            | Channel::Statistics24h => {}
            unsupported => {
                return Err(AdapterError::UnsupportedCapability(format!(
                    "Gemini does not support {unsupported:?}"
                )));
            }
        }
    }

    let mut instrument_ids = HashMap::new();
    let mut symbols = Vec::new();
    let mut price_scale = None;
    let mut qty_scale = None;
    for item in &request.items {
        if instrument_ids.values().any(|id| *id == item.instrument) {
            continue;
        }
        let instrument = catalog
            .instruments
            .iter()
            .find(|instrument| instrument.id == item.instrument)
            .ok_or_else(|| {
                AdapterError::Catalog(format!(
                    "requested instrument {} missing from Gemini catalog",
                    item.instrument.0
                ))
            })?;
        if price_scale.is_none() {
            price_scale = Some(instrument.price_scale);
            qty_scale = Some(instrument.quantity_scale);
        }
        symbols.push(instrument.key.native_symbol.clone());
        instrument_ids.insert(instrument.key.native_symbol.clone(), instrument.id);
    }

    Ok(GeminiSessionConfig {
        symbols,
        instrument_ids,
        enable_l2: request
            .items
            .iter()
            .any(|item| matches!(&item.channel, Channel::L2Book { .. })),
        candle_intervals: candle_intervals_from(request),
        poll_stats: request
            .items
            .iter()
            .any(|item| matches!(&item.channel, Channel::Statistics24h)),
        price_scale: price_scale.expect("nonempty request selects an instrument"),
        qty_scale: qty_scale.expect("nonempty request selects an instrument"),
        ..GeminiSessionConfig::default()
    })
}

impl VenueFactory for GeminiFactory {
    fn specification(&self) -> &'static VenueSpecification {
        &GEMINI_SPEC
    }

    fn instrument_requests(
        &self,
        _environment: Environment,
    ) -> Result<Vec<HttpRequestSpec>, AdapterError> {
        Ok(vec![HttpRequestSpec {
            id: 1,
            method: marketfeed_adapter_api::HttpMethod::Get,
            url: format!("{REST_BASE}/symbols"),
            headers: Vec::new(),
            body: None,
        }])
    }

    fn parse_instruments(
        &self,
        responses: &[HttpResponse],
        out: &mut Vec<marketfeed_model::InstrumentDefinition>,
    ) -> Result<(), AdapterError> {
        out.extend(parse_symbols(responses)?);
        Ok(())
    }

    fn instrument_detail_requests(
        &self,
        defs: &[marketfeed_model::InstrumentDefinition],
    ) -> Result<Vec<HttpRequestSpec>, AdapterError> {
        let max = self.live_details_max;
        if max == 0 {
            return Ok(Vec::new());
        }
        let n = max.min(defs.len());
        let mut specs = Vec::with_capacity(n);
        for (i, def) in defs.iter().take(n).enumerate() {
            let sym = def.key.native_symbol.to_ascii_lowercase();
            specs.push(HttpRequestSpec {
                id: (i as u64).saturating_add(2),
                method: marketfeed_adapter_api::HttpMethod::Get,
                url: format!("{REST_BASE}/symbols/details/{sym}"),
                headers: Vec::new(),
                body: None,
            });
        }
        Ok(specs)
    }

    fn apply_instrument_details(
        &self,
        responses: &[HttpResponse],
        defs: &mut [marketfeed_model::InstrumentDefinition],
    ) -> Result<(), AdapterError> {
        apply_symbol_details(responses, defs)
    }

    fn plan(
        &self,
        request: &ConcreteSubscriptionSet,
        catalog: &CatalogView,
    ) -> Result<Vec<SessionSpec>, AdapterError> {
        if catalog.venue != GEMINI_VENUE_ID {
            return Err(AdapterError::Catalog("wrong venue for gemini".into()));
        }
        let _ = request;
        Ok(vec![SessionSpec {
            endpoint_name: ws_url(),
            subscriptions: request.clone(),
        }])
    }

    fn create_session(
        &self,
        spec: SessionSpec,
        catalog: CatalogView,
    ) -> Result<Box<dyn SessionMachine>, AdapterError> {
        let cfg = session_config_from_request(&catalog, &spec.subscriptions, self.enable_l2)?;
        Ok(Box::new(GeminiSession::new(spec, catalog, cfg)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketfeed_adapter_api::{ConcreteSubscription, DeliveryOptions};
    use marketfeed_model::{
        AssetCode, CatalogVersion, Fixed, InstrumentDefinition, InstrumentId, InstrumentKey,
        InstrumentKind, InstrumentStatus, VenueCode,
    };
    use std::sync::Arc;

    #[test]
    fn catalog_ids_flow_into_session_config() {
        let def = InstrumentDefinition {
            key: InstrumentKey {
                venue: VenueCode("gemini".into()),
                native_symbol: "ETHUSD".into(),
                kind: InstrumentKind::Spot,
                settlement: None,
                expiry_ns: None,
            },
            base: AssetCode("ETH".into()),
            quote: AssetCode("USD".into()),
            settlement: None,
            price_scale: 2,
            quantity_scale: 6,
            price_increment: Fixed::new(1, 2),
            quantity_increment: Fixed::new(1, 6),
            min_quantity: None,
            max_quantity: None,
            min_notional: None,
            contract_size: None,
            expiry_ns: None,
            status: InstrumentStatus::Active,
            inverse: false,
        };
        let inst = def.into_instrument(InstrumentId(15), CatalogVersion(2));
        let catalog = CatalogView::with_instruments(
            GEMINI_VENUE_ID,
            CatalogVersion(2),
            Arc::<[_]>::from(vec![inst]),
        );
        let cfg = session_config_from_catalog(&catalog, true, vec![CandleInterval::M1]);
        assert_eq!(cfg.symbols, vec!["ETHUSD".to_string()]);
        assert_eq!(cfg.instrument_ids.get("ETHUSD"), Some(&InstrumentId(15)));
    }

    #[test]
    fn explicit_request_selects_only_requested_instruments_and_channels() {
        let eth = InstrumentDefinition {
            key: InstrumentKey {
                venue: VenueCode("gemini".into()),
                native_symbol: "ETHUSD".into(),
                kind: InstrumentKind::Spot,
                settlement: None,
                expiry_ns: None,
            },
            base: AssetCode("ETH".into()),
            quote: AssetCode("USD".into()),
            settlement: None,
            price_scale: 2,
            quantity_scale: 6,
            price_increment: Fixed::new(1, 2),
            quantity_increment: Fixed::new(1, 6),
            min_quantity: None,
            max_quantity: None,
            min_notional: None,
            contract_size: None,
            expiry_ns: None,
            status: InstrumentStatus::Active,
            inverse: false,
        }
        .into_instrument(InstrumentId(15), CatalogVersion(2));
        let btc = InstrumentDefinition {
            key: InstrumentKey {
                venue: VenueCode("gemini".into()),
                native_symbol: "BTCUSD".into(),
                kind: InstrumentKind::Spot,
                settlement: None,
                expiry_ns: None,
            },
            base: AssetCode("BTC".into()),
            quote: AssetCode("USD".into()),
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
            inverse: false,
        }
        .into_instrument(InstrumentId(16), CatalogVersion(2));
        let catalog = CatalogView::with_instruments(
            GEMINI_VENUE_ID,
            CatalogVersion(2),
            Arc::<[_]>::from(vec![eth, btc]),
        );
        let request = ConcreteSubscriptionSet {
            items: vec![ConcreteSubscription {
                instrument: InstrumentId(15),
                channel: Channel::Trades,
                delivery: DeliveryOptions::default(),
            }],
        };

        let cfg = session_config_from_request(&catalog, &request, true).expect("request config");

        assert_eq!(cfg.symbols, vec!["ETHUSD"]);
        assert_eq!(cfg.instrument_ids.len(), 1);
        assert!(!cfg.enable_l2);
        assert!(cfg.candle_intervals.is_empty());
    }

    #[test]
    fn detail_requests_respect_cap() {
        let defs = vec![
            InstrumentDefinition {
                key: InstrumentKey {
                    venue: VenueCode("gemini".into()),
                    native_symbol: "BTCUSD".into(),
                    kind: InstrumentKind::Spot,
                    settlement: None,
                    expiry_ns: None,
                },
                base: AssetCode("BTC".into()),
                quote: AssetCode("USD".into()),
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
                inverse: false,
            },
            InstrumentDefinition {
                key: InstrumentKey {
                    venue: VenueCode("gemini".into()),
                    native_symbol: "ETHUSD".into(),
                    kind: InstrumentKind::Spot,
                    settlement: None,
                    expiry_ns: None,
                },
                base: AssetCode("ETH".into()),
                quote: AssetCode("USD".into()),
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
                inverse: false,
            },
        ];
        let specs = GeminiFactory {
            enable_l2: false,
            live_details_max: 1,
        }
        .instrument_detail_requests(&defs)
        .unwrap();
        assert_eq!(specs.len(), 1);
        assert!(specs[0].url.ends_with("/symbols/details/btcusd"));
        assert!(
            GeminiFactory {
                enable_l2: false,
                live_details_max: 0
            }
            .instrument_detail_requests(&defs)
            .unwrap()
            .is_empty()
        );
    }
}
