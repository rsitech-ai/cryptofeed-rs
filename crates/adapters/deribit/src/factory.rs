//! Deribit VenueFactory.

use std::collections::HashMap;

use marketfeed_adapter_api::{
    AdapterError, CandleInterval, Channel, ConcreteSubscriptionSet, Environment, HttpRequestSpec,
    HttpResponse, SessionMachine, SessionSpec, VenueFactory, VenueSpecification,
};
use marketfeed_model::CatalogView;

use crate::instruments::parse_instruments;
use crate::session::{DeribitSession, DeribitSessionConfig};
use crate::specification::{DERIBIT_SPEC, DERIBIT_VENUE_ID, ws_url};

#[derive(Debug, Default)]
pub struct DeribitFactory {
    pub enable_l2: bool,
}

fn candle_intervals_from(request: &ConcreteSubscriptionSet) -> Vec<CandleInterval> {
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

fn session_config_from_catalog(
    catalog: &CatalogView,
    request: &ConcreteSubscriptionSet,
    fallback_enable_l2: bool,
) -> Result<DeribitSessionConfig, AdapterError> {
    let candle_intervals = candle_intervals_from(request);
    if request.items.is_empty() {
        return Ok(DeribitSessionConfig {
            enable_l2: fallback_enable_l2,
            candle_intervals,
            ..DeribitSessionConfig::default()
        });
    }

    let mut instrument_ids = HashMap::new();
    let mut instruments = Vec::new();
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
                    "requested instrument {} missing from deribit catalog",
                    item.instrument.0
                ))
            })?;
        if price_scale.is_none() {
            price_scale = Some(instrument.price_scale);
            qty_scale = Some(instrument.quantity_scale);
        }
        instruments.push(instrument.key.native_symbol.clone());
        instrument_ids.insert(instrument.key.native_symbol.clone(), instrument.id);
    }

    Ok(DeribitSessionConfig {
        instruments,
        instrument_ids,
        enable_l2: request
            .items
            .iter()
            .any(|item| matches!(&item.channel, Channel::L2Book { .. })),
        candle_intervals,
        price_scale: price_scale.expect("nonempty request selects an instrument"),
        qty_scale: qty_scale.expect("nonempty request selects an instrument"),
        ..DeribitSessionConfig::default()
    })
}

impl VenueFactory for DeribitFactory {
    fn specification(&self) -> &'static VenueSpecification {
        &DERIBIT_SPEC
    }

    fn instrument_requests(
        &self,
        _environment: Environment,
    ) -> Result<Vec<HttpRequestSpec>, AdapterError> {
        Ok(vec![
            HttpRequestSpec {
                id: 1,
                method: marketfeed_adapter_api::HttpMethod::Get,
                url:
                    "https://www.deribit.com/api/v2/public/get_instruments?currency=BTC&kind=future"
                        .into(),
                headers: Vec::new(),
                body: None,
            },
            HttpRequestSpec {
                id: 2,
                method: marketfeed_adapter_api::HttpMethod::Get,
                url:
                    "https://www.deribit.com/api/v2/public/get_instruments?currency=ETH&kind=future"
                        .into(),
                headers: Vec::new(),
                body: None,
            },
        ])
    }

    fn parse_instruments(
        &self,
        responses: &[HttpResponse],
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
        if catalog.venue != DERIBIT_VENUE_ID {
            return Err(AdapterError::Catalog("wrong venue for deribit".into()));
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
        let cfg = session_config_from_catalog(&catalog, &spec.subscriptions, self.enable_l2)?;
        Ok(Box::new(DeribitSession::new(spec, catalog, cfg)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketfeed_adapter_api::{ConcreteSubscription, DeliveryOptions};
    use marketfeed_model::{
        AssetCode, CatalogVersion, Fixed, Instrument, InstrumentId, InstrumentKey, InstrumentKind,
        InstrumentStatus, VenueCode,
    };
    use std::sync::Arc;

    #[test]
    fn session_config_uses_requested_catalog_instrument() {
        let instrument = Instrument {
            id: InstrumentId(44),
            key: InstrumentKey {
                venue: VenueCode("deribit".into()),
                native_symbol: "ETH-PERPETUAL".into(),
                kind: InstrumentKind::PerpetualLinear,
                settlement: Some(AssetCode("USD".into())),
                expiry_ns: None,
            },
            base: AssetCode("ETH".into()),
            quote: AssetCode("USD".into()),
            settlement: Some(AssetCode("USD".into())),
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
            catalog_version: CatalogVersion(1),
        };
        let catalog = CatalogView::with_instruments(
            DERIBIT_VENUE_ID,
            CatalogVersion(1),
            Arc::<[_]>::from(vec![instrument]),
        );
        let request = ConcreteSubscriptionSet {
            items: vec![ConcreteSubscription {
                instrument: InstrumentId(44),
                channel: Channel::Trades,
                delivery: DeliveryOptions::default(),
            }],
        };

        let cfg =
            session_config_from_catalog(&catalog, &request, false).expect("catalog-backed request");

        assert_eq!(cfg.instruments, vec!["ETH-PERPETUAL"]);
        assert_eq!(
            cfg.instrument_ids.get("ETH-PERPETUAL"),
            Some(&InstrumentId(44))
        );
        assert_eq!(cfg.price_scale, 2);
        assert_eq!(cfg.qty_scale, 4);
        assert!(!cfg.enable_l2);
    }
}
