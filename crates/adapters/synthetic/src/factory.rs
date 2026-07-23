//! Synthetic VenueFactory.

use marketfeed_adapter_api::{
    AdapterError, ConcreteSubscriptionSet, Environment, HttpRequestSpec, HttpResponse,
    SessionMachine, SessionSpec, VenueFactory, VenueSpecification,
};
use marketfeed_model::{
    AssetCode, CatalogView, Fixed, InstrumentDefinition, InstrumentKey, InstrumentKind,
    InstrumentStatus, VenueCode,
};

use crate::session::SyntheticSession;
use crate::specification::{SYNTHETIC_SPEC, SYNTHETIC_VENUE_ID};

#[derive(Debug, Default)]
pub struct SyntheticFactory;

impl VenueFactory for SyntheticFactory {
    fn specification(&self) -> &'static VenueSpecification {
        &SYNTHETIC_SPEC
    }

    fn instrument_requests(
        &self,
        _environment: Environment,
    ) -> Result<Vec<HttpRequestSpec>, AdapterError> {
        // Synthetic catalog is local — no HTTP.
        Ok(Vec::new())
    }

    fn parse_instruments(
        &self,
        _responses: &[HttpResponse],
        out: &mut Vec<InstrumentDefinition>,
    ) -> Result<(), AdapterError> {
        out.push(InstrumentDefinition {
            key: InstrumentKey {
                venue: VenueCode("synthetic".into()),
                native_symbol: "BTC-USD".into(),
                kind: InstrumentKind::Spot,
                settlement: None,
                expiry_ns: None,
            },
            base: AssetCode("BTC".into()),
            quote: AssetCode("USD".into()),
            settlement: None,
            price_scale: 2,
            quantity_scale: 3,
            price_increment: Fixed::new(1, 2),
            quantity_increment: Fixed::new(1, 3),
            min_quantity: Some(Fixed::new(1, 3)),
            max_quantity: None,
            min_notional: None,
            contract_size: None,
            expiry_ns: None,
            status: InstrumentStatus::Active,
            inverse: false,
        });
        Ok(())
    }

    fn plan(
        &self,
        request: &ConcreteSubscriptionSet,
        catalog: &CatalogView,
    ) -> Result<Vec<SessionSpec>, AdapterError> {
        if catalog.venue != SYNTHETIC_VENUE_ID {
            return Err(AdapterError::Catalog(
                "synthetic factory got foreign venue".into(),
            ));
        }
        Ok(vec![SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: request.clone(),
        }])
    }

    fn create_session(
        &self,
        spec: SessionSpec,
        catalog: CatalogView,
    ) -> Result<Box<dyn SessionMachine>, AdapterError> {
        Ok(Box::new(SyntheticSession::new(spec, catalog)))
    }
}

/// Helper for tests: one BTC-USD instrument with assigned id.
#[cfg(test)]
#[allow(dead_code)]
pub fn sample_instrument(id: marketfeed_model::InstrumentId) -> marketfeed_model::Instrument {
    use marketfeed_model::CatalogVersion;
    let mut defs = Vec::new();
    SyntheticFactory
        .parse_instruments(&[], &mut defs)
        .expect("synthetic instruments");
    defs.pop()
        .expect("one instrument")
        .into_instrument(id, CatalogVersion(1))
}
