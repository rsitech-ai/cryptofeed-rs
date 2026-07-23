//! Coinbase Advanced Trade VenueFactory (public T/Q/L2 + candles).

use std::collections::HashMap;

use marketfeed_adapter_api::{
    AdapterError, CandleInterval, ConcreteSubscriptionSet, Environment, HttpRequestSpec,
    SessionMachine, SessionSpec, VenueFactory, VenueSpecification,
};
use marketfeed_model::CatalogView;

use crate::advanced::instruments::parse_products;
use crate::advanced::session::{CoinbaseAdvSession, CoinbaseAdvSessionConfig};
use crate::advanced::specification::{COINBASE_ADV_SPEC, COINBASE_ADV_VENUE_ID, REST_BASE, ws_url};
use crate::factory::candle_intervals_from;

#[derive(Debug, Default)]
pub struct CoinbaseAdvFactory {
    pub enable_l2: bool,
}

/// Build session config from catalog instruments (product ids).
///
/// # ponytail
/// Empty catalog → default BTC-USD stub (daemon config stubs fill products).
pub fn session_config_from_catalog(
    catalog: &CatalogView,
    enable_l2: bool,
    candle_intervals: Vec<CandleInterval>,
) -> CoinbaseAdvSessionConfig {
    if catalog.instruments.is_empty() {
        return CoinbaseAdvSessionConfig {
            enable_l2,
            candle_intervals,
            ..CoinbaseAdvSessionConfig::default()
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
    CoinbaseAdvSessionConfig {
        products,
        instrument_ids,
        enable_l2,
        candle_intervals,
        price_scale,
        qty_scale,
        ..CoinbaseAdvSessionConfig::default()
    }
}

impl VenueFactory for CoinbaseAdvFactory {
    fn specification(&self) -> &'static VenueSpecification {
        &COINBASE_ADV_SPEC
    }

    fn instrument_requests(
        &self,
        _environment: Environment,
    ) -> Result<Vec<HttpRequestSpec>, AdapterError> {
        Ok(vec![HttpRequestSpec {
            id: 1,
            method: marketfeed_adapter_api::HttpMethod::Get,
            url: format!("{REST_BASE}/products"),
            headers: Vec::new(),
            body: None,
        }])
    }

    fn parse_instruments(
        &self,
        responses: &[marketfeed_adapter_api::HttpResponse],
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
        if catalog.venue != COINBASE_ADV_VENUE_ID {
            return Err(AdapterError::Catalog("wrong venue for coinbase-adv".into()));
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
        let cfg = session_config_from_catalog(&catalog, self.enable_l2, candle_intervals);
        Ok(Box::new(CoinbaseAdvSession::new(spec, catalog, cfg)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketfeed_model::{
        AssetCode, CatalogVersion, Fixed, InstrumentDefinition, InstrumentId, InstrumentKey,
        InstrumentKind, InstrumentStatus, VenueCode,
    };
    use std::sync::Arc;

    #[test]
    fn catalog_ids_flow_into_session_config() {
        let def = InstrumentDefinition {
            key: InstrumentKey {
                venue: VenueCode("coinbase-adv".into()),
                native_symbol: "ETH-USD".into(),
                kind: InstrumentKind::Spot,
                settlement: None,
                expiry_ns: None,
            },
            base: AssetCode("ETH".into()),
            quote: AssetCode("USD".into()),
            settlement: None,
            price_scale: 2,
            quantity_scale: 5,
            price_increment: Fixed::new(1, 2),
            quantity_increment: Fixed::new(1, 5),
            min_quantity: None,
            max_quantity: None,
            min_notional: None,
            contract_size: None,
            expiry_ns: None,
            status: InstrumentStatus::Active,
            inverse: false,
        };
        let inst = def.into_instrument(InstrumentId(18), CatalogVersion(2));
        let catalog = CatalogView::with_instruments(
            COINBASE_ADV_VENUE_ID,
            CatalogVersion(2),
            Arc::<[_]>::from(vec![inst]),
        );
        let cfg = session_config_from_catalog(&catalog, true, vec![CandleInterval::M1]);
        assert_eq!(cfg.products, vec!["ETH-USD".to_string()]);
        assert_eq!(cfg.instrument_ids.get("ETH-USD"), Some(&InstrumentId(18)));
        assert_eq!(cfg.candle_intervals, vec![CandleInterval::M1]);
        assert!(cfg.enable_l2);
        assert_eq!(cfg.price_scale, 2);
        assert_eq!(cfg.qty_scale, 5);
    }
}
