//! Binance USD-M VenueFactory.

use std::collections::HashMap;

use marketfeed_adapter_api::{
    AdapterError, CandleInterval, ConcreteSubscriptionSet, Environment, HttpRequestSpec,
    HttpResponse, SessionMachine, SessionSpec, VenueFactory, VenueSpecification,
};
use marketfeed_model::CatalogView;

use crate::factory::candle_intervals_from;
use crate::usdm_instruments::parse_usdm_exchange_info;
use crate::usdm_session::{BinanceUsdmSession, BinanceUsdmSessionConfig};
use crate::usdm_specification::{BINANCE_USDM_SPEC, BINANCE_USDM_VENUE_ID, combined_stream_url};

#[derive(Debug, Default)]
pub struct BinanceUsdmFactory {
    pub enable_l2: bool,
}

pub fn usdm_session_config_from_catalog(
    catalog: &CatalogView,
    enable_l2: bool,
    candle_intervals: Vec<CandleInterval>,
) -> BinanceUsdmSessionConfig {
    if catalog.instruments.is_empty() {
        return BinanceUsdmSessionConfig {
            enable_l2,
            candle_intervals,
            ..BinanceUsdmSessionConfig::default()
        };
    }
    let mut instrument_ids = HashMap::new();
    let mut symbols = Vec::with_capacity(catalog.instruments.len());
    let mut price_scale = 2u8;
    let mut qty_scale = 3u8;
    for (i, inst) in catalog.instruments.iter().enumerate() {
        symbols.push(inst.key.native_symbol.clone());
        instrument_ids.insert(inst.key.native_symbol.clone(), inst.id);
        if i == 0 {
            price_scale = inst.price_scale;
            qty_scale = inst.quantity_scale;
        }
    }
    BinanceUsdmSessionConfig {
        symbols,
        instrument_ids,
        enable_l2,
        candle_intervals,
        price_scale,
        qty_scale,
        ..BinanceUsdmSessionConfig::default()
    }
}

impl VenueFactory for BinanceUsdmFactory {
    fn specification(&self) -> &'static VenueSpecification {
        &BINANCE_USDM_SPEC
    }

    fn instrument_requests(
        &self,
        _environment: Environment,
    ) -> Result<Vec<HttpRequestSpec>, AdapterError> {
        Ok(vec![HttpRequestSpec {
            id: 1,
            method: marketfeed_adapter_api::HttpMethod::Get,
            url: "https://fapi.binance.com/fapi/v1/exchangeInfo".into(),
            headers: Vec::new(),
            body: None,
        }])
    }

    fn parse_instruments(
        &self,
        responses: &[HttpResponse],
        out: &mut Vec<marketfeed_model::InstrumentDefinition>,
    ) -> Result<(), AdapterError> {
        out.extend(parse_usdm_exchange_info(responses)?);
        Ok(())
    }

    fn plan(
        &self,
        request: &ConcreteSubscriptionSet,
        catalog: &CatalogView,
    ) -> Result<Vec<SessionSpec>, AdapterError> {
        if catalog.venue != BINANCE_USDM_VENUE_ID {
            return Err(AdapterError::Catalog("wrong venue for binance-usdm".into()));
        }
        let symbols_lower: Vec<String> = if catalog.instruments.is_empty() {
            let _ = request;
            vec!["btcusdt".into()]
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
        let cfg = usdm_session_config_from_catalog(&catalog, self.enable_l2, candles);
        Ok(Box::new(BinanceUsdmSession::new(spec, catalog, cfg)))
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
    fn catalog_symbols_wire_into_usdm_config() {
        let def = InstrumentDefinition {
            key: InstrumentKey {
                venue: VenueCode("binance-usdm".into()),
                native_symbol: "ETHUSDT".into(),
                kind: InstrumentKind::PerpetualLinear,
                settlement: Some(AssetCode("USDT".into())),
                expiry_ns: None,
            },
            base: AssetCode("ETH".into()),
            quote: AssetCode("USDT".into()),
            settlement: Some(AssetCode("USDT".into())),
            price_scale: 2,
            quantity_scale: 3,
            price_increment: Fixed::new(1, 2),
            quantity_increment: Fixed::new(1, 3),
            min_quantity: None,
            max_quantity: None,
            min_notional: None,
            contract_size: None,
            expiry_ns: None,
            status: InstrumentStatus::Active,
            inverse: false,
        };
        let inst = def.into_instrument(InstrumentId(9), CatalogVersion(1));
        let catalog = CatalogView::with_instruments(
            VenueId(3),
            CatalogVersion(1),
            Arc::<[_]>::from(vec![inst]),
        );
        let cfg = usdm_session_config_from_catalog(&catalog, true, vec![CandleInterval::M1]);
        assert_eq!(cfg.symbols, vec!["ETHUSDT".to_string()]);
        assert_eq!(cfg.instrument_ids.get("ETHUSDT"), Some(&InstrumentId(9)));
        assert!(cfg.enable_l2);
        assert_eq!(cfg.price_scale, 2);
        assert_eq!(cfg.qty_scale, 3);
    }
}
