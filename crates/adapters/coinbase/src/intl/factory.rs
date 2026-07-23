//! Coinbase International VenueFactory (auth MD T/Q/L2).

use std::collections::HashMap;

use marketfeed_adapter_api::{
    AdapterError, ConcreteSubscriptionSet, Environment, HttpRequestSpec, SessionMachine,
    SessionSpec, VenueFactory, VenueSpecification,
};
use marketfeed_model::CatalogView;

use crate::intl::credentials::CoinbaseIntlCredentials;
use crate::intl::instruments::parse_instruments;
use crate::intl::session::{CoinbaseIntlSession, CoinbaseIntlSessionConfig};
use crate::intl::specification::{COINBASE_INTL_SPEC, COINBASE_INTL_VENUE_ID, REST_BASE, ws_url};

#[derive(Debug)]
pub struct CoinbaseIntlFactory {
    pub enable_l2: bool,
    pub credentials: Option<CoinbaseIntlCredentials>,
}

pub fn session_config_from_catalog(
    catalog: &CatalogView,
    enable_l2: bool,
    credentials: CoinbaseIntlCredentials,
) -> CoinbaseIntlSessionConfig {
    if catalog.instruments.is_empty() {
        return CoinbaseIntlSessionConfig {
            enable_l2,
            credentials,
            ..CoinbaseIntlSessionConfig::default()
        };
    }
    let mut instrument_ids = HashMap::new();
    let mut products = Vec::with_capacity(catalog.instruments.len());
    let mut price_scale = 1u8;
    let mut qty_scale = 4u8;
    for (i, inst) in catalog.instruments.iter().enumerate() {
        products.push(inst.key.native_symbol.clone());
        instrument_ids.insert(inst.key.native_symbol.clone(), inst.id);
        if i == 0 {
            price_scale = inst.price_scale;
            qty_scale = inst.quantity_scale;
        }
    }
    CoinbaseIntlSessionConfig {
        products,
        instrument_ids,
        enable_l2,
        price_scale,
        qty_scale,
        credentials,
        ..CoinbaseIntlSessionConfig::default()
    }
}

impl VenueFactory for CoinbaseIntlFactory {
    fn specification(&self) -> &'static VenueSpecification {
        &COINBASE_INTL_SPEC
    }

    fn instrument_requests(
        &self,
        _environment: Environment,
    ) -> Result<Vec<HttpRequestSpec>, AdapterError> {
        Ok(vec![HttpRequestSpec {
            id: 1,
            method: marketfeed_adapter_api::HttpMethod::Get,
            url: format!("{REST_BASE}/instruments"),
            headers: Vec::new(),
            body: None,
        }])
    }

    fn parse_instruments(
        &self,
        responses: &[marketfeed_adapter_api::HttpResponse],
        out: &mut Vec<marketfeed_model::InstrumentDefinition>,
    ) -> Result<(), AdapterError> {
        out.extend(parse_instruments(responses)?);
        Ok(())
    }

    fn plan(
        &self,
        request: &ConcreteSubscriptionSet,
        catalog: &CatalogView,
    ) -> Result<Vec<SessionSpec>, AdapterError> {
        if catalog.venue != COINBASE_INTL_VENUE_ID {
            return Err(AdapterError::Catalog(
                "wrong venue for coinbase-intl".into(),
            ));
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
        let credentials = match &self.credentials {
            Some(c) => c.clone(),
            None => CoinbaseIntlCredentials::from_env()
                .map_err(|e| AdapterError::Subscription(e.to_string()))?,
        };
        let cfg = session_config_from_catalog(&catalog, self.enable_l2, credentials);
        Ok(Box::new(CoinbaseIntlSession::new(spec, catalog, cfg)))
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
    fn catalog_ids_flow() {
        let def = InstrumentDefinition {
            key: InstrumentKey {
                venue: VenueCode("coinbase-intl".into()),
                native_symbol: "ETH-PERP".into(),
                kind: InstrumentKind::PerpetualLinear,
                settlement: None,
                expiry_ns: None,
            },
            base: AssetCode("ETH".into()),
            quote: AssetCode("USDC".into()),
            settlement: None,
            price_scale: 2,
            quantity_scale: 4,
            price_increment: Fixed::new(1, 2),
            quantity_increment: Fixed::new(1, 4),
            min_quantity: None,
            max_quantity: None,
            min_notional: None,
            contract_size: None,
            expiry_ns: None,
            status: InstrumentStatus::Active,
            inverse: false,
        };
        let inst = def.into_instrument(InstrumentId(19), CatalogVersion(2));
        let catalog = CatalogView::with_instruments(
            COINBASE_INTL_VENUE_ID,
            CatalogVersion(2),
            Arc::<[_]>::from(vec![inst]),
        );
        let cfg = session_config_from_catalog(&catalog, true, CoinbaseIntlCredentials::fixture());
        assert_eq!(cfg.products, vec!["ETH-PERP".to_string()]);
        assert_eq!(cfg.instrument_ids.get("ETH-PERP"), Some(&InstrumentId(19)));
        assert!(cfg.enable_l2);
    }
}
