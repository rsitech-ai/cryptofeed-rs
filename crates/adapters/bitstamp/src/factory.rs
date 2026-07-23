//! Bitstamp Spot VenueFactory.

use std::collections::HashMap;

use marketfeed_adapter_api::{
    AdapterError, CandleInterval, Channel, ConcreteSubscriptionSet, Environment, HttpRequestSpec,
    HttpResponse, SessionMachine, SessionSpec, VenueFactory, VenueSpecification,
};
use marketfeed_model::CatalogView;

use crate::instruments::parse_trading_pairs;
use crate::session::{BitstampSession, BitstampSessionConfig};
use crate::specification::{BITSTAMP_SPEC, BITSTAMP_VENUE_ID, REST_BASE, ws_url};

#[derive(Debug, Default)]
pub struct BitstampFactory {
    pub enable_l2: bool,
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

/// Build session config from catalog instruments (native symbols + scales).
///
/// # ponytail
/// Empty catalog → default BTCUSD-style stub (daemon config stubs fill symbols).
pub fn session_config_from_catalog(
    catalog: &CatalogView,
    enable_l2: bool,
    candle_intervals: Vec<CandleInterval>,
) -> BitstampSessionConfig {
    if catalog.instruments.is_empty() {
        return BitstampSessionConfig {
            enable_l2,
            candle_intervals,
            ..BitstampSessionConfig::default()
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
    BitstampSessionConfig {
        symbols,
        instrument_ids,
        enable_l2,
        candle_intervals,
        price_scale,
        qty_scale,
        ..BitstampSessionConfig::default()
    }
}

impl VenueFactory for BitstampFactory {
    fn specification(&self) -> &'static VenueSpecification {
        &BITSTAMP_SPEC
    }

    fn instrument_requests(
        &self,
        _environment: Environment,
    ) -> Result<Vec<HttpRequestSpec>, AdapterError> {
        Ok(vec![HttpRequestSpec {
            id: 1,
            method: marketfeed_adapter_api::HttpMethod::Get,
            url: format!("{REST_BASE}/trading-pairs-info/"),
            headers: Vec::new(),
            body: None,
        }])
    }

    fn parse_instruments(
        &self,
        responses: &[HttpResponse],
        out: &mut Vec<marketfeed_model::InstrumentDefinition>,
    ) -> Result<(), AdapterError> {
        out.extend(parse_trading_pairs(responses)?);
        Ok(())
    }

    fn plan(
        &self,
        request: &ConcreteSubscriptionSet,
        catalog: &CatalogView,
    ) -> Result<Vec<SessionSpec>, AdapterError> {
        if catalog.venue != BITSTAMP_VENUE_ID {
            return Err(AdapterError::Catalog("wrong venue for bitstamp".into()));
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
        Ok(Box::new(BitstampSession::new(spec, catalog, cfg)))
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
                venue: VenueCode("bitstamp".into()),
                native_symbol: "ethusd".into(),
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
        let inst = def.into_instrument(InstrumentId(14), CatalogVersion(2));
        let catalog = CatalogView::with_instruments(
            BITSTAMP_VENUE_ID,
            CatalogVersion(2),
            Arc::<[_]>::from(vec![inst]),
        );
        let cfg = session_config_from_catalog(&catalog, true, vec![CandleInterval::M1]);
        assert_eq!(cfg.symbols, vec!["ethusd".to_string()]);
        assert_eq!(cfg.instrument_ids.get("ethusd"), Some(&InstrumentId(14)));
        assert_eq!(cfg.price_scale, 2);
        assert_eq!(cfg.qty_scale, 5);
        assert!(cfg.enable_l2);
    }
}
