//! Binance Coin-M VenueFactory.

use std::collections::HashMap;

use marketfeed_adapter_api::{
    AdapterError, CandleInterval, ConcreteSubscriptionSet, Environment, HttpRequestSpec,
    HttpResponse, SessionMachine, SessionSpec, VenueFactory, VenueSpecification,
};
use marketfeed_model::CatalogView;

use crate::coinm_instruments::parse_coinm_exchange_info;
use crate::coinm_session::{BinanceCoinmSession, BinanceCoinmSessionConfig};
use crate::coinm_specification::{BINANCE_COINM_SPEC, BINANCE_COINM_VENUE_ID, combined_stream_url};
use crate::factory::candle_intervals_from;

#[derive(Debug, Default)]
pub struct BinanceCoinmFactory {
    pub enable_l2: bool,
}

pub fn coinm_session_config_from_catalog(
    catalog: &CatalogView,
    enable_l2: bool,
    candle_intervals: Vec<CandleInterval>,
) -> BinanceCoinmSessionConfig {
    if catalog.instruments.is_empty() {
        return BinanceCoinmSessionConfig {
            enable_l2,
            candle_intervals,
            ..BinanceCoinmSessionConfig::default()
        };
    }
    let mut instrument_ids = HashMap::new();
    let mut symbols = Vec::with_capacity(catalog.instruments.len());
    let mut price_scale = 1u8;
    let mut qty_scale = 0u8;
    for (i, inst) in catalog.instruments.iter().enumerate() {
        symbols.push(inst.key.native_symbol.clone());
        instrument_ids.insert(inst.key.native_symbol.clone(), inst.id);
        if i == 0 {
            price_scale = inst.price_scale;
            qty_scale = inst.quantity_scale;
        }
    }
    BinanceCoinmSessionConfig {
        symbols,
        instrument_ids,
        enable_l2,
        candle_intervals,
        price_scale,
        qty_scale,
        ..BinanceCoinmSessionConfig::default()
    }
}

impl VenueFactory for BinanceCoinmFactory {
    fn specification(&self) -> &'static VenueSpecification {
        &BINANCE_COINM_SPEC
    }

    fn instrument_requests(
        &self,
        _environment: Environment,
    ) -> Result<Vec<HttpRequestSpec>, AdapterError> {
        Ok(vec![HttpRequestSpec {
            id: 1,
            method: marketfeed_adapter_api::HttpMethod::Get,
            url: "https://dapi.binance.com/dapi/v1/exchangeInfo".into(),
            headers: Vec::new(),
            body: None,
        }])
    }

    fn parse_instruments(
        &self,
        responses: &[HttpResponse],
        out: &mut Vec<marketfeed_model::InstrumentDefinition>,
    ) -> Result<(), AdapterError> {
        out.extend(parse_coinm_exchange_info(responses)?);
        Ok(())
    }

    fn plan(
        &self,
        request: &ConcreteSubscriptionSet,
        catalog: &CatalogView,
    ) -> Result<Vec<SessionSpec>, AdapterError> {
        if catalog.venue != BINANCE_COINM_VENUE_ID {
            return Err(AdapterError::Catalog(
                "wrong venue for binance-coinm".into(),
            ));
        }
        let symbols_lower: Vec<String> = if catalog.instruments.is_empty() {
            vec!["btcusd_perp".into()]
        } else {
            catalog
                .instruments
                .iter()
                .map(|i| i.key.native_symbol.to_ascii_lowercase())
                .collect()
        };
        let candles = candle_intervals_from(request);
        Ok(vec![SessionSpec {
            endpoint_name: combined_stream_url(&symbols_lower, self.enable_l2, &candles),
            subscriptions: request.clone(),
        }])
    }

    fn create_session(
        &self,
        spec: SessionSpec,
        catalog: CatalogView,
    ) -> Result<Box<dyn SessionMachine>, AdapterError> {
        let candles = candle_intervals_from(&spec.subscriptions);
        let cfg = coinm_session_config_from_catalog(&catalog, self.enable_l2, candles);
        Ok(Box::new(BinanceCoinmSession::new(spec, catalog, cfg)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketfeed_model::{
        AssetCode, CatalogVersion, Fixed, InstrumentDefinition, InstrumentId, InstrumentKey,
        InstrumentKind, InstrumentStatus, VenueCode, VenueId,
    };
    use std::sync::Arc;

    #[test]
    fn catalog_symbols_wire_into_coinm_config() {
        let def = InstrumentDefinition {
            key: InstrumentKey {
                venue: VenueCode("binance-coinm".into()),
                native_symbol: "ETHUSD_PERP".into(),
                kind: InstrumentKind::PerpetualInverse,
                settlement: Some(AssetCode("ETH".into())),
                expiry_ns: None,
            },
            base: AssetCode("ETH".into()),
            quote: AssetCode("USD".into()),
            settlement: Some(AssetCode("ETH".into())),
            price_scale: 2,
            quantity_scale: 0,
            price_increment: Fixed::new(1, 2),
            quantity_increment: Fixed::new(1, 0),
            min_quantity: None,
            max_quantity: None,
            min_notional: None,
            contract_size: None,
            expiry_ns: None,
            status: InstrumentStatus::Active,
            inverse: true,
        };
        let inst = def.into_instrument(InstrumentId(9), CatalogVersion(1));
        let catalog = CatalogView::with_instruments(
            VenueId(12),
            CatalogVersion(1),
            Arc::<[_]>::from(vec![inst]),
        );
        let cfg = coinm_session_config_from_catalog(&catalog, true, vec![CandleInterval::M1]);
        assert_eq!(cfg.symbols, vec!["ETHUSD_PERP".to_string()]);
        assert_eq!(
            cfg.instrument_ids.get("ETHUSD_PERP"),
            Some(&InstrumentId(9))
        );
        assert!(cfg.enable_l2);
        assert_eq!(cfg.candle_intervals, vec![CandleInterval::M1]);
        assert_eq!(cfg.price_scale, 2);
        assert_eq!(cfg.qty_scale, 0);
    }
}
