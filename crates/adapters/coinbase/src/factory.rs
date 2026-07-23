//! Coinbase Exchange VenueFactory.

use std::collections::HashMap;

use marketfeed_adapter_api::{
    AdapterError, CandleInterval, Channel, ConcreteSubscriptionSet, Environment, HttpRequestSpec,
    HttpResponse, SessionMachine, SessionSpec, VenueFactory, VenueSpecification,
};
use marketfeed_model::CatalogView;

use crate::exchange_credentials::CoinbaseExchangeCredentials;
use crate::instruments::parse_products;
use crate::session::{CoinbaseSessionConfig, CoinbaseSpotSession};
use crate::specification::{COINBASE_SPOT_SPEC, COINBASE_SPOT_VENUE_ID, ws_url};

#[derive(Debug, Default)]
pub struct CoinbaseSpotFactory {
    pub enable_l2: bool,
    pub credentials: Option<CoinbaseExchangeCredentials>,
}

/// Intervals requested via `Channel::Candles` (deduped, order-preserving).
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

/// Build session config from catalog instruments (product ids + scales).
///
/// # ponytail
/// Empty catalog → default BTC-USD stub (daemon config stubs fill products).
pub fn session_config_from_catalog(
    catalog: &CatalogView,
    enable_l2: bool,
    candle_intervals: Vec<CandleInterval>,
) -> CoinbaseSessionConfig {
    if catalog.instruments.is_empty() {
        return CoinbaseSessionConfig {
            enable_l2,
            candle_intervals,
            ..CoinbaseSessionConfig::default()
        };
    }
    let mut instrument_ids = HashMap::new();
    let mut products = Vec::with_capacity(catalog.instruments.len());
    let mut price_scale = 2u8;
    let mut qty_scale = 8u8;
    for (i, inst) in catalog.instruments.iter().enumerate() {
        products.push(inst.key.native_symbol.clone());
        instrument_ids.insert(inst.key.native_symbol.clone(), inst.id);
        if i == 0 {
            price_scale = inst.price_scale;
            qty_scale = inst.quantity_scale;
        }
    }
    CoinbaseSessionConfig {
        products,
        instrument_ids,
        enable_l2,
        candle_intervals,
        price_scale,
        qty_scale,
        ..CoinbaseSessionConfig::default()
    }
}

impl VenueFactory for CoinbaseSpotFactory {
    fn specification(&self) -> &'static VenueSpecification {
        &COINBASE_SPOT_SPEC
    }

    fn instrument_requests(
        &self,
        _environment: Environment,
    ) -> Result<Vec<HttpRequestSpec>, AdapterError> {
        Ok(vec![HttpRequestSpec {
            id: 1,
            method: marketfeed_adapter_api::HttpMethod::Get,
            url: "https://api.exchange.coinbase.com/products".into(),
            headers: Vec::new(),
            body: None,
        }])
    }

    fn parse_instruments(
        &self,
        responses: &[HttpResponse],
        out: &mut Vec<marketfeed_model::InstrumentDefinition>,
    ) -> Result<(), AdapterError> {
        out.extend(parse_products(responses)?);
        Ok(())
    }

    fn plan(
        &self,
        request: &ConcreteSubscriptionSet,
        catalog: &CatalogView,
    ) -> Result<Vec<SessionSpec>, AdapterError> {
        if catalog.venue != COINBASE_SPOT_VENUE_ID {
            return Err(AdapterError::Catalog("wrong venue for coinbase".into()));
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
        let candle_intervals = candle_intervals_from(&spec.subscriptions);
        let mut cfg = session_config_from_catalog(&catalog, self.enable_l2, candle_intervals);
        if self.enable_l2 {
            cfg.credentials = Some(match &self.credentials {
                Some(credentials) => credentials.clone(),
                None => CoinbaseExchangeCredentials::from_env()
                    .map_err(|error| AdapterError::Subscription(error.to_string()))?,
            });
        }
        Ok(Box::new(CoinbaseSpotSession::new(spec, catalog, cfg)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketfeed_adapter_api::Capability;
    use marketfeed_model::{
        AssetCode, CatalogVersion, Fixed, InstrumentDefinition, InstrumentId, InstrumentKey,
        InstrumentKind, InstrumentStatus, VenueCode,
    };
    use std::sync::Arc;

    #[test]
    fn catalog_ids_flow_into_session_config() {
        let def = InstrumentDefinition {
            key: InstrumentKey {
                venue: VenueCode("coinbase-spot".into()),
                native_symbol: "ETH-USD".into(),
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
            status: InstrumentStatus::Suspended,
            inverse: false,
        };
        let inst = def.into_instrument(InstrumentId(16), CatalogVersion(3));
        let catalog = CatalogView::with_instruments(
            COINBASE_SPOT_VENUE_ID,
            CatalogVersion(3),
            Arc::<[_]>::from(vec![inst]),
        );
        let cfg = session_config_from_catalog(&catalog, true, vec![CandleInterval::M1]);
        assert_eq!(cfg.products, vec!["ETH-USD".to_string()]);
        assert_eq!(cfg.instrument_ids.get("ETH-USD"), Some(&InstrumentId(16)));
        assert_eq!(cfg.price_scale, 2);
        assert_eq!(cfg.qty_scale, 8);
        assert!(cfg.enable_l2);
    }

    #[test]
    fn exchange_level2_is_plannable_without_loading_credentials() {
        let factory = CoinbaseSpotFactory {
            enable_l2: true,
            credentials: None,
        };
        assert!(
            factory
                .specification()
                .capabilities
                .contains(&Capability::L2Book)
        );
        let catalog = CatalogView::new(COINBASE_SPOT_VENUE_ID, CatalogVersion(1));
        let plan = factory
            .plan(&ConcreteSubscriptionSet::default(), &catalog)
            .expect("planning is credential-free");
        assert_eq!(plan.len(), 1);
    }
}
