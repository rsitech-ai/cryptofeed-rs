//! Binance Spot VenueFactory.

use std::collections::HashMap;

use marketfeed_adapter_api::{
    AdapterError, CandleInterval, Channel, ConcreteSubscriptionSet, Environment, HttpRequestSpec,
    HttpResponse, SessionMachine, SessionSpec, VenueFactory, VenueSpecification,
};
use marketfeed_model::CatalogView;

use crate::instruments::parse_exchange_info;
use crate::session::{BinanceSessionConfig, BinanceSpotSession};
use crate::specification::{BINANCE_SPOT_SPEC, BINANCE_SPOT_VENUE_ID, combined_stream_url};

#[derive(Debug, Default)]
pub struct BinanceSpotFactory {
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
/// One shared fallback scale when catalog empty; per-symbol scales applied at
/// book construction via `CatalogView::find_by_native`.
pub fn session_config_from_catalog(
    catalog: &CatalogView,
    enable_l2: bool,
    candle_intervals: Vec<CandleInterval>,
) -> BinanceSessionConfig {
    if catalog.instruments.is_empty() {
        return BinanceSessionConfig {
            enable_l2,
            candle_intervals,
            ..BinanceSessionConfig::default()
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
    BinanceSessionConfig {
        symbols,
        instrument_ids,
        enable_l2,
        candle_intervals,
        price_scale,
        qty_scale,
        ..BinanceSessionConfig::default()
    }
}

impl VenueFactory for BinanceSpotFactory {
    fn specification(&self) -> &'static VenueSpecification {
        &BINANCE_SPOT_SPEC
    }

    fn instrument_requests(
        &self,
        _environment: Environment,
    ) -> Result<Vec<HttpRequestSpec>, AdapterError> {
        Ok(vec![HttpRequestSpec {
            id: 1,
            method: marketfeed_adapter_api::HttpMethod::Get,
            url: "https://api.binance.com/api/v3/exchangeInfo".into(),
            headers: Vec::new(),
            body: None,
        }])
    }

    fn parse_instruments(
        &self,
        responses: &[HttpResponse],
        out: &mut Vec<marketfeed_model::InstrumentDefinition>,
    ) -> Result<(), AdapterError> {
        out.extend(parse_exchange_info(responses)?);
        Ok(())
    }

    fn plan(
        &self,
        request: &ConcreteSubscriptionSet,
        catalog: &CatalogView,
    ) -> Result<Vec<SessionSpec>, AdapterError> {
        if catalog.venue != BINANCE_SPOT_VENUE_ID {
            return Err(AdapterError::Catalog("wrong venue for binance-spot".into()));
        }
        let symbols_lower: Vec<String> = if catalog.instruments.is_empty() {
            vec!["btcusdt".into()]
        } else {
            catalog
                .instruments
                .iter()
                .map(|i| i.key.native_symbol.to_ascii_lowercase())
                .collect()
        };
        let candles = candle_intervals_from(request);
        let url = combined_stream_url(&symbols_lower, self.enable_l2, &candles);
        Ok(vec![SessionSpec {
            endpoint_name: url,
            subscriptions: request.clone(),
        }])
    }

    fn create_session(
        &self,
        spec: SessionSpec,
        catalog: CatalogView,
    ) -> Result<Box<dyn SessionMachine>, AdapterError> {
        let candles = candle_intervals_from(&spec.subscriptions);
        let cfg = session_config_from_catalog(&catalog, self.enable_l2, candles);
        Ok(Box::new(BinanceSpotSession::new(spec, catalog, cfg)))
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
    fn catalog_scales_flow_into_session_config() {
        let def = InstrumentDefinition {
            key: InstrumentKey {
                venue: VenueCode("binance-spot".into()),
                native_symbol: "ETHUSDT".into(),
                kind: InstrumentKind::Spot,
                settlement: None,
                expiry_ns: None,
            },
            base: AssetCode("ETH".into()),
            quote: AssetCode("USDT".into()),
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
        let inst = def.into_instrument(InstrumentId(7), CatalogVersion(3));
        let catalog = CatalogView::with_instruments(
            VenueId(2),
            CatalogVersion(3),
            Arc::<[_]>::from(vec![inst]),
        );
        let cfg = session_config_from_catalog(&catalog, true, vec![CandleInterval::M1]);
        assert_eq!(cfg.symbols, vec!["ETHUSDT".to_string()]);
        assert_eq!(cfg.instrument_ids.get("ETHUSDT"), Some(&InstrumentId(7)));
        assert_eq!(cfg.price_scale, 2);
        assert_eq!(cfg.qty_scale, 4);
        assert!(cfg.enable_l2);
        assert_eq!(cfg.candle_intervals, vec![CandleInterval::M1]);
    }
}
