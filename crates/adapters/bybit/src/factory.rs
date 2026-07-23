//! Bybit V5 VenueFactory.

use std::collections::HashMap;

use marketfeed_adapter_api::{
    AdapterError, CandleInterval, Channel, ConcreteSubscriptionSet, Environment, HttpMethod,
    HttpRequestSpec, HttpResponse, SessionMachine, SessionSpec, VenueFactory, VenueSpecification,
};
use marketfeed_model::CatalogView;

use crate::instruments::parse_instruments_info;
use crate::session::{BybitSession, BybitSessionConfig};
use crate::specification::{
    BYBIT_INVERSE_SPEC, BYBIT_LINEAR_SPEC, BYBIT_SPOT_SPEC, BybitCategory, public_ws_url,
};

#[derive(Debug)]
pub struct BybitFactory {
    pub category: BybitCategory,
    pub enable_l2: bool,
}

impl Default for BybitFactory {
    fn default() -> Self {
        Self {
            category: BybitCategory::Linear,
            enable_l2: false,
        }
    }
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
    category: BybitCategory,
    fallback_enable_l2: bool,
) -> Result<BybitSessionConfig, AdapterError> {
    let candle_intervals = candle_intervals_from(request);
    if request.items.is_empty() {
        return Ok(BybitSessionConfig {
            category,
            enable_l2: fallback_enable_l2,
            candle_intervals,
            ..BybitSessionConfig::default()
        });
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
                    "requested instrument {} missing from bybit catalog",
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

    Ok(BybitSessionConfig {
        category,
        symbols,
        instrument_ids,
        enable_l2: request
            .items
            .iter()
            .any(|item| matches!(&item.channel, Channel::L2Book { .. })),
        price_scale: price_scale.expect("nonempty request selects an instrument"),
        qty_scale: qty_scale.expect("nonempty request selects an instrument"),
        candle_intervals,
        ..BybitSessionConfig::default()
    })
}

impl VenueFactory for BybitFactory {
    fn specification(&self) -> &'static VenueSpecification {
        match self.category {
            BybitCategory::Linear => &BYBIT_LINEAR_SPEC,
            BybitCategory::Spot => &BYBIT_SPOT_SPEC,
            BybitCategory::Inverse => &BYBIT_INVERSE_SPEC,
        }
    }

    fn instrument_requests(
        &self,
        _environment: Environment,
    ) -> Result<Vec<HttpRequestSpec>, AdapterError> {
        Ok(vec![HttpRequestSpec {
            id: 1,
            method: HttpMethod::Get,
            url: format!(
                "https://api.bybit.com/v5/market/instruments-info?category={}",
                self.category.as_str()
            ),
            headers: Vec::new(),
            body: None,
        }])
    }

    fn parse_instruments(
        &self,
        responses: &[HttpResponse],
        out: &mut Vec<marketfeed_model::InstrumentDefinition>,
    ) -> Result<(), AdapterError> {
        out.extend(parse_instruments_info(responses, self.category)?);
        Ok(())
    }

    fn plan(
        &self,
        request: &ConcreteSubscriptionSet,
        catalog: &CatalogView,
    ) -> Result<Vec<SessionSpec>, AdapterError> {
        let expected = self.category.venue_id();
        if catalog.venue != expected {
            return Err(AdapterError::Catalog(format!(
                "wrong venue for bybit-{}",
                self.category.as_str()
            )));
        }
        Ok(vec![SessionSpec {
            endpoint_name: public_ws_url(self.category.as_str()).into(),
            subscriptions: request.clone(),
        }])
    }

    fn create_session(
        &self,
        spec: SessionSpec,
        catalog: CatalogView,
    ) -> Result<Box<dyn SessionMachine>, AdapterError> {
        let cfg = session_config_from_catalog(
            &catalog,
            &spec.subscriptions,
            self.category,
            self.enable_l2,
        )?;
        Ok(Box::new(BybitSession::new(spec, catalog, cfg)))
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

    fn catalog(instruments: Vec<Instrument>) -> CatalogView {
        CatalogView::with_instruments(
            BybitCategory::Linear.venue_id(),
            CatalogVersion(1),
            Arc::<[_]>::from(instruments),
        )
    }

    fn instrument() -> Instrument {
        Instrument {
            id: InstrumentId(42),
            key: InstrumentKey {
                venue: VenueCode("bybit-linear".into()),
                native_symbol: "ETHUSDT".into(),
                kind: InstrumentKind::PerpetualLinear,
                settlement: Some(AssetCode("USDT".into())),
                expiry_ns: None,
            },
            base: AssetCode("ETH".into()),
            quote: AssetCode("USDT".into()),
            settlement: Some(AssetCode("USDT".into())),
            price_scale: 4,
            quantity_scale: 6,
            price_increment: Fixed::new(1, 4),
            quantity_increment: Fixed::new(1, 6),
            min_quantity: None,
            max_quantity: None,
            min_notional: None,
            contract_size: None,
            expiry_ns: None,
            status: InstrumentStatus::Active,
            inverse: false,
            catalog_version: CatalogVersion(1),
        }
    }

    #[test]
    fn linear_session_config_deduplicates_requested_instrument_channels() {
        let request = ConcreteSubscriptionSet {
            items: vec![
                ConcreteSubscription {
                    instrument: InstrumentId(42),
                    channel: Channel::Trades,
                    delivery: DeliveryOptions::default(),
                },
                ConcreteSubscription {
                    instrument: InstrumentId(42),
                    channel: Channel::Candles {
                        interval: CandleInterval::M1,
                    },
                    delivery: DeliveryOptions::default(),
                },
            ],
        };

        let cfg = session_config_from_catalog(
            &catalog(vec![instrument()]),
            &request,
            BybitCategory::Linear,
            true,
        )
        .expect("catalog-backed request");

        assert_eq!(cfg.symbols, vec!["ETHUSDT"]);
        assert_eq!(cfg.instrument_ids.get("ETHUSDT"), Some(&InstrumentId(42)));
        assert_eq!(cfg.price_scale, 4);
        assert_eq!(cfg.qty_scale, 6);
        assert!(!cfg.enable_l2);
        assert_eq!(cfg.candle_intervals, vec![CandleInterval::M1]);
    }
}
