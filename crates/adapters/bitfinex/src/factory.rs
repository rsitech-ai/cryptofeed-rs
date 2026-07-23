//! Bitfinex Spot (**17**) + Derivatives (**20**) VenueFactory.

use std::collections::HashMap;

use marketfeed_adapter_api::{
    AdapterError, CandleInterval, Channel, ConcreteSubscriptionSet, Environment, HttpRequestSpec,
    HttpResponse, SessionMachine, SessionSpec, VenueFactory, VenueSpecification,
};
use marketfeed_model::{CatalogView, VenueId};

use crate::instruments::{parse_futures_pair_list, parse_pair_list};
use crate::session::{BitfinexSession, BitfinexSessionConfig};
use crate::specification::{
    BITFINEX_DERIV_SPEC, BITFINEX_DERIV_VENUE_ID, BITFINEX_SPEC, BITFINEX_VENUE_ID, REST_BASE,
    ws_url,
};

#[derive(Debug, Default)]
pub struct BitfinexFactory {
    pub enable_l2: bool,
}

#[derive(Debug, Default)]
pub struct BitfinexDerivFactory {
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
/// Empty catalog → default tBTCUSD-style stub (daemon config stubs fill symbols).
pub fn session_config_from_catalog(
    catalog: &CatalogView,
    venue: VenueId,
    enable_l2: bool,
    candle_intervals: Vec<CandleInterval>,
    poll_deriv_status: bool,
) -> BitfinexSessionConfig {
    let default_symbol = if poll_deriv_status {
        "tBTCF0:USTF0"
    } else {
        "tBTCUSD"
    };
    if catalog.instruments.is_empty() {
        let mut instrument_ids = HashMap::new();
        instrument_ids.insert(default_symbol.into(), marketfeed_model::InstrumentId(1));
        return BitfinexSessionConfig {
            venue,
            symbols: vec![default_symbol.into()],
            instrument_ids,
            enable_l2,
            candle_intervals,
            poll_deriv_status,
            ..BitfinexSessionConfig::default()
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
    BitfinexSessionConfig {
        venue,
        symbols,
        instrument_ids,
        enable_l2,
        candle_intervals,
        poll_deriv_status,
        price_scale,
        qty_scale,
        ..BitfinexSessionConfig::default()
    }
}

impl VenueFactory for BitfinexFactory {
    fn specification(&self) -> &'static VenueSpecification {
        &BITFINEX_SPEC
    }

    fn instrument_requests(
        &self,
        _environment: Environment,
    ) -> Result<Vec<HttpRequestSpec>, AdapterError> {
        Ok(vec![HttpRequestSpec {
            id: 1,
            method: marketfeed_adapter_api::HttpMethod::Get,
            url: format!("{REST_BASE}/conf/pub:list:pair:exchange"),
            headers: Vec::new(),
            body: None,
        }])
    }

    fn parse_instruments(
        &self,
        responses: &[HttpResponse],
        out: &mut Vec<marketfeed_model::InstrumentDefinition>,
    ) -> Result<(), AdapterError> {
        out.extend(parse_pair_list(responses)?);
        Ok(())
    }

    fn plan(
        &self,
        request: &ConcreteSubscriptionSet,
        catalog: &CatalogView,
    ) -> Result<Vec<SessionSpec>, AdapterError> {
        if catalog.venue != BITFINEX_VENUE_ID {
            return Err(AdapterError::Catalog("wrong venue for bitfinex".into()));
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
        let cfg = session_config_from_catalog(
            &catalog,
            BITFINEX_VENUE_ID,
            self.enable_l2,
            candle_intervals,
            false,
        );
        Ok(Box::new(BitfinexSession::new(spec, catalog, cfg)))
    }
}

impl VenueFactory for BitfinexDerivFactory {
    fn specification(&self) -> &'static VenueSpecification {
        &BITFINEX_DERIV_SPEC
    }

    fn instrument_requests(
        &self,
        _environment: Environment,
    ) -> Result<Vec<HttpRequestSpec>, AdapterError> {
        Ok(vec![HttpRequestSpec {
            id: 1,
            method: marketfeed_adapter_api::HttpMethod::Get,
            url: format!("{REST_BASE}/conf/pub:list:pair:futures"),
            headers: Vec::new(),
            body: None,
        }])
    }

    fn parse_instruments(
        &self,
        responses: &[HttpResponse],
        out: &mut Vec<marketfeed_model::InstrumentDefinition>,
    ) -> Result<(), AdapterError> {
        out.extend(parse_futures_pair_list(responses)?);
        Ok(())
    }

    fn plan(
        &self,
        request: &ConcreteSubscriptionSet,
        catalog: &CatalogView,
    ) -> Result<Vec<SessionSpec>, AdapterError> {
        if catalog.venue != BITFINEX_DERIV_VENUE_ID {
            return Err(AdapterError::Catalog(
                "wrong venue for bitfinex-deriv".into(),
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
        let candle_intervals = candle_intervals_from(&spec.subscriptions);
        let cfg = session_config_from_catalog(
            &catalog,
            BITFINEX_DERIV_VENUE_ID,
            self.enable_l2,
            candle_intervals,
            true,
        );
        Ok(Box::new(BitfinexSession::new(spec, catalog, cfg)))
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
                venue: VenueCode("bitfinex".into()),
                native_symbol: "tETHUSD".into(),
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
        let inst = def.into_instrument(InstrumentId(17), CatalogVersion(2));
        let catalog = CatalogView::with_instruments(
            BITFINEX_VENUE_ID,
            CatalogVersion(2),
            Arc::<[_]>::from(vec![inst]),
        );
        let cfg = session_config_from_catalog(
            &catalog,
            BITFINEX_VENUE_ID,
            true,
            vec![CandleInterval::M1],
            false,
        );
        assert_eq!(cfg.symbols, vec!["tETHUSD".to_string()]);
        assert_eq!(cfg.instrument_ids.get("tETHUSD"), Some(&InstrumentId(17)));
        assert_eq!(cfg.price_scale, 2);
        assert_eq!(cfg.qty_scale, 5);
        assert!(cfg.enable_l2);
        assert!(!cfg.poll_deriv_status);
        assert_eq!(cfg.candle_intervals, vec![CandleInterval::M1]);
    }

    #[test]
    fn deriv_empty_catalog_defaults_to_perp_symbol() {
        let catalog = CatalogView::new(BITFINEX_DERIV_VENUE_ID, CatalogVersion(1));
        let cfg =
            session_config_from_catalog(&catalog, BITFINEX_DERIV_VENUE_ID, false, vec![], true);
        assert_eq!(cfg.symbols, vec!["tBTCF0:USTF0".to_string()]);
        assert!(cfg.poll_deriv_status);
        assert_eq!(cfg.venue, BITFINEX_DERIV_VENUE_ID);
    }

    #[test]
    fn deriv_catalog_ids_flow_into_session_config() {
        let def = InstrumentDefinition {
            key: InstrumentKey {
                venue: VenueCode("bitfinex-deriv".into()),
                native_symbol: "tETHF0:USTF0".into(),
                kind: InstrumentKind::PerpetualLinear,
                settlement: None,
                expiry_ns: None,
            },
            base: AssetCode("ETHF0".into()),
            quote: AssetCode("USTF0".into()),
            settlement: None,
            price_scale: 1,
            quantity_scale: 6,
            price_increment: Fixed::new(1, 1),
            quantity_increment: Fixed::new(1, 6),
            min_quantity: None,
            max_quantity: None,
            min_notional: None,
            contract_size: None,
            expiry_ns: None,
            status: InstrumentStatus::Active,
            inverse: false,
        };
        let inst = def.into_instrument(InstrumentId(20), CatalogVersion(3));
        let catalog = CatalogView::with_instruments(
            BITFINEX_DERIV_VENUE_ID,
            CatalogVersion(3),
            Arc::<[_]>::from(vec![inst]),
        );
        let cfg = session_config_from_catalog(
            &catalog,
            BITFINEX_DERIV_VENUE_ID,
            true,
            vec![CandleInterval::M5],
            true,
        );
        assert_eq!(cfg.symbols, vec!["tETHF0:USTF0".to_string()]);
        assert_eq!(
            cfg.instrument_ids.get("tETHF0:USTF0"),
            Some(&InstrumentId(20))
        );
        assert_eq!(cfg.price_scale, 1);
        assert_eq!(cfg.qty_scale, 6);
        assert!(cfg.enable_l2);
        assert!(cfg.poll_deriv_status);
        assert_eq!(cfg.candle_intervals, vec![CandleInterval::M5]);
    }
}
