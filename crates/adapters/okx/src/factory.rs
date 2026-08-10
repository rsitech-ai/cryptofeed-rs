//! OKX `VenueFactory` implementations: Spot, SWAP (perpetuals), Futures (dated).

use std::collections::HashMap;

use marketfeed_adapter_api::{
    AdapterError, CandleInterval, Channel, ConcreteSubscriptionSet, Environment, HttpMethod,
    HttpRequestSpec, HttpResponse, SessionMachine, SessionSpec, VenueFactory, VenueSpecification,
};
use marketfeed_model::{CatalogView, InstrumentId, VenueId};

use crate::instruments::{OkxInstType, parse_instruments_response};
use crate::session::{OkxSession, OkxSessionConfig};
use crate::specification::{
    BUSINESS_WS_URL, OKX_FUTURES_SPEC, OKX_FUTURES_VENUE_ID, OKX_SPOT_SPEC, OKX_SPOT_VENUE_ID,
    OKX_SWAP_SPEC, OKX_SWAP_VENUE_ID, PUBLIC_WS_URL, REST_BASE,
};

fn plan_sessions(request: &ConcreteSubscriptionSet) -> Vec<SessionSpec> {
    if request.items.is_empty() {
        return vec![SessionSpec {
            endpoint_name: PUBLIC_WS_URL.into(),
            subscriptions: request.clone(),
        }];
    }

    let mut public = ConcreteSubscriptionSet::default();
    let mut business = ConcreteSubscriptionSet::default();
    for item in &request.items {
        if matches!(item.channel, Channel::Candles { .. }) {
            business.items.push(item.clone());
        } else {
            public.items.push(item.clone());
        }
    }

    let mut sessions = Vec::with_capacity(2);
    if !public.items.is_empty() {
        sessions.push(SessionSpec {
            endpoint_name: PUBLIC_WS_URL.into(),
            subscriptions: public,
        });
    }
    if !business.items.is_empty() {
        sessions.push(SessionSpec {
            endpoint_name: BUSINESS_WS_URL.into(),
            subscriptions: business,
        });
    }
    sessions
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
) -> Result<OkxSessionConfig, AdapterError> {
    let candle_intervals = candle_intervals_from(request);
    if request.items.is_empty() {
        return Ok(OkxSessionConfig {
            enable_l2: fallback_enable_l2,
            candle_intervals,
            ..OkxSessionConfig::default()
        });
    }

    let mut instrument_ids = HashMap::new();
    let mut symbols = Vec::new();
    let mut price_scale = None;
    let mut qty_scale = None;
    let mut book_scales = HashMap::new();
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
                    "requested instrument {} missing from okx catalog",
                    item.instrument.0
                ))
            })?;
        if price_scale.is_none() {
            price_scale = Some(instrument.price_scale);
            qty_scale = Some(instrument.quantity_scale);
        }
        symbols.push(instrument.key.native_symbol.clone());
        book_scales.insert(
            instrument.key.native_symbol.clone(),
            (instrument.price_scale, instrument.quantity_scale),
        );
        instrument_ids.insert(instrument.key.native_symbol.clone(), instrument.id);
    }

    Ok(OkxSessionConfig {
        symbols,
        instrument_ids,
        enable_l2: request
            .items
            .iter()
            .any(|item| matches!(&item.channel, Channel::L2Book { .. })),
        price_scale: price_scale.expect("nonempty request selects an instrument"),
        qty_scale: qty_scale.expect("nonempty request selects an instrument"),
        book_scales,
        candle_intervals,
        ..OkxSessionConfig::default()
    })
}

/// Build a catalog-backed session config, preserving the per-segment default
/// only for empty subscription requests.
fn single_symbol_session(
    spec: SessionSpec,
    catalog: CatalogView,
    symbol: &str,
    venue: VenueId,
    enable_l2: bool,
    subscribe_mark_funding: bool,
    price_scale: u8,
    qty_scale: u8,
) -> Result<Box<dyn SessionMachine>, AdapterError> {
    let mut cfg = session_config_from_catalog(&catalog, &spec.subscriptions, enable_l2)?;
    if spec.subscriptions.items.is_empty() {
        cfg.symbols = vec![symbol.to_string()];
        cfg.instrument_ids = HashMap::from([(symbol.to_string(), InstrumentId(1))]);
        cfg.price_scale = price_scale;
        cfg.qty_scale = qty_scale;
    }
    cfg.venue = venue;
    cfg.subscribe_mark_funding = subscribe_mark_funding;
    Ok(Box::new(OkxSession::new(spec, catalog, cfg)))
}

#[derive(Debug, Default)]
pub struct OkxSpotFactory {
    pub enable_l2: bool,
}

impl VenueFactory for OkxSpotFactory {
    fn specification(&self) -> &'static VenueSpecification {
        &OKX_SPOT_SPEC
    }

    fn instrument_requests(
        &self,
        _environment: Environment,
    ) -> Result<Vec<HttpRequestSpec>, AdapterError> {
        Ok(vec![HttpRequestSpec {
            id: 1,
            method: HttpMethod::Get,
            url: format!("{REST_BASE}/api/v5/public/instruments?instType=SPOT"),
            headers: Vec::new(),
            body: None,
        }])
    }

    fn parse_instruments(
        &self,
        responses: &[HttpResponse],
        out: &mut Vec<marketfeed_model::InstrumentDefinition>,
    ) -> Result<(), AdapterError> {
        out.extend(parse_instruments_response(responses, OkxInstType::Spot)?);
        Ok(())
    }

    fn plan(
        &self,
        request: &ConcreteSubscriptionSet,
        catalog: &CatalogView,
    ) -> Result<Vec<SessionSpec>, AdapterError> {
        if catalog.venue != OKX_SPOT_VENUE_ID {
            return Err(AdapterError::Catalog("wrong venue for okx-spot".into()));
        }
        Ok(plan_sessions(request))
    }

    fn create_session(
        &self,
        spec: SessionSpec,
        catalog: CatalogView,
    ) -> Result<Box<dyn SessionMachine>, AdapterError> {
        single_symbol_session(
            spec,
            catalog,
            "BTC-USDT",
            OKX_SPOT_VENUE_ID,
            self.enable_l2,
            false,
            // BTC-USDT tickSz=0.1, lotSz=1e-8
            1,
            8,
        )
    }
}

/// SWAP (perpetual) factory: same public WS gateway, `instType=SWAP` instruments,
/// plus mark-price/index-tickers/funding-rate channels.
#[derive(Debug, Default)]
pub struct OkxSwapFactory {
    pub enable_l2: bool,
}

impl VenueFactory for OkxSwapFactory {
    fn specification(&self) -> &'static VenueSpecification {
        &OKX_SWAP_SPEC
    }

    fn instrument_requests(
        &self,
        _environment: Environment,
    ) -> Result<Vec<HttpRequestSpec>, AdapterError> {
        Ok(vec![HttpRequestSpec {
            id: 1,
            method: HttpMethod::Get,
            url: format!("{REST_BASE}/api/v5/public/instruments?instType=SWAP"),
            headers: Vec::new(),
            body: None,
        }])
    }

    fn parse_instruments(
        &self,
        responses: &[HttpResponse],
        out: &mut Vec<marketfeed_model::InstrumentDefinition>,
    ) -> Result<(), AdapterError> {
        out.extend(parse_instruments_response(responses, OkxInstType::Swap)?);
        Ok(())
    }

    fn plan(
        &self,
        request: &ConcreteSubscriptionSet,
        catalog: &CatalogView,
    ) -> Result<Vec<SessionSpec>, AdapterError> {
        if catalog.venue != OKX_SWAP_VENUE_ID {
            return Err(AdapterError::Catalog("wrong venue for okx-swap".into()));
        }
        Ok(plan_sessions(request))
    }

    fn create_session(
        &self,
        spec: SessionSpec,
        catalog: CatalogView,
    ) -> Result<Box<dyn SessionMachine>, AdapterError> {
        single_symbol_session(
            spec,
            catalog,
            "BTC-USDT-SWAP",
            OKX_SWAP_VENUE_ID,
            self.enable_l2,
            true,
            // BTC-USDT-SWAP tickSz=0.1; lotSz is whole contracts.
            1,
            0,
        )
    }
}

/// Dated FUTURES factory: `instType=FUTURES` instruments, linear contracts only.
#[derive(Debug, Default)]
pub struct OkxFuturesFactory {
    pub enable_l2: bool,
}

impl VenueFactory for OkxFuturesFactory {
    fn specification(&self) -> &'static VenueSpecification {
        &OKX_FUTURES_SPEC
    }

    fn instrument_requests(
        &self,
        _environment: Environment,
    ) -> Result<Vec<HttpRequestSpec>, AdapterError> {
        Ok(vec![HttpRequestSpec {
            id: 1,
            method: HttpMethod::Get,
            url: format!("{REST_BASE}/api/v5/public/instruments?instType=FUTURES"),
            headers: Vec::new(),
            body: None,
        }])
    }

    fn parse_instruments(
        &self,
        responses: &[HttpResponse],
        out: &mut Vec<marketfeed_model::InstrumentDefinition>,
    ) -> Result<(), AdapterError> {
        out.extend(parse_instruments_response(responses, OkxInstType::Futures)?);
        Ok(())
    }

    fn plan(
        &self,
        request: &ConcreteSubscriptionSet,
        catalog: &CatalogView,
    ) -> Result<Vec<SessionSpec>, AdapterError> {
        if catalog.venue != OKX_FUTURES_VENUE_ID {
            return Err(AdapterError::Catalog("wrong venue for okx-futures".into()));
        }
        Ok(plan_sessions(request))
    }

    fn create_session(
        &self,
        spec: SessionSpec,
        catalog: CatalogView,
    ) -> Result<Box<dyn SessionMachine>, AdapterError> {
        single_symbol_session(
            spec,
            catalog,
            "BTC-USDT-250328",
            OKX_FUTURES_VENUE_ID,
            self.enable_l2,
            true,
            1,
            0,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketfeed_adapter_api::{ConcreteSubscription, DeliveryOptions};
    use marketfeed_model::{
        AssetCode, CatalogVersion, Fixed, Instrument, InstrumentKey, InstrumentKind,
        InstrumentStatus, VenueCode,
    };
    use std::sync::Arc;

    fn catalog(instruments: Vec<Instrument>) -> CatalogView {
        CatalogView::with_instruments(
            OKX_FUTURES_VENUE_ID,
            CatalogVersion(1),
            Arc::<[_]>::from(instruments),
        )
    }

    fn instrument(
        id: InstrumentId,
        symbol: &str,
        price_scale: u8,
        quantity_scale: u8,
    ) -> Instrument {
        Instrument {
            id,
            key: InstrumentKey {
                venue: VenueCode("okx-futures".into()),
                native_symbol: symbol.into(),
                kind: InstrumentKind::FutureLinear,
                settlement: Some(AssetCode("USDT".into())),
                expiry_ns: Some(1_789_689_600_000_000_000),
            },
            base: AssetCode("ETH".into()),
            quote: AssetCode("USDT".into()),
            settlement: Some(AssetCode("USDT".into())),
            price_scale,
            quantity_scale,
            price_increment: Fixed::new(1, price_scale),
            quantity_increment: Fixed::new(1, quantity_scale),
            min_quantity: None,
            max_quantity: None,
            min_notional: None,
            contract_size: None,
            expiry_ns: Some(1_789_689_600_000_000_000),
            status: InstrumentStatus::Active,
            inverse: false,
            catalog_version: CatalogVersion(1),
        }
    }

    #[test]
    fn futures_session_config_uses_requested_catalog_instrument_and_channels() {
        let request = ConcreteSubscriptionSet {
            items: vec![
                ConcreteSubscription {
                    instrument: InstrumentId(41),
                    channel: Channel::L2Book {
                        depth: None,
                        cadence: None,
                    },
                    delivery: DeliveryOptions::default(),
                },
                ConcreteSubscription {
                    instrument: InstrumentId(41),
                    channel: Channel::Candles {
                        interval: CandleInterval::M5,
                    },
                    delivery: DeliveryOptions::default(),
                },
                ConcreteSubscription {
                    instrument: InstrumentId(41),
                    channel: Channel::Candles {
                        interval: CandleInterval::H1,
                    },
                    delivery: DeliveryOptions::default(),
                },
            ],
        };

        let cfg = session_config_from_catalog(
            &catalog(vec![instrument(InstrumentId(41), "ETH-USDT-260925", 5, 7)]),
            &request,
            false,
        )
        .expect("catalog-backed request");

        assert_eq!(cfg.symbols, vec!["ETH-USDT-260925"]);
        assert_eq!(
            cfg.instrument_ids.get("ETH-USDT-260925"),
            Some(&InstrumentId(41))
        );
        assert_eq!(cfg.price_scale, 5);
        assert_eq!(cfg.qty_scale, 7);
        assert_eq!(cfg.book_scales.get("ETH-USDT-260925"), Some(&(5, 7)));
        assert!(cfg.enable_l2);
        assert_eq!(
            cfg.candle_intervals,
            vec![CandleInterval::M5, CandleInterval::H1]
        );
    }

    #[test]
    fn session_config_preserves_each_symbols_catalog_scales() {
        let eth = instrument(InstrumentId(41), "ETH-USDT-260925", 5, 7);
        let xrp = instrument(InstrumentId(42), "XRP-USDT-260925", 4, 1);
        let request = ConcreteSubscriptionSet {
            items: [InstrumentId(41), InstrumentId(42)]
                .into_iter()
                .map(|instrument| ConcreteSubscription {
                    instrument,
                    channel: Channel::L2Book {
                        depth: None,
                        cadence: None,
                    },
                    delivery: DeliveryOptions::default(),
                })
                .collect(),
        };

        let cfg = session_config_from_catalog(&catalog(vec![eth, xrp]), &request, false)
            .expect("multi-symbol catalog-backed request");

        assert_eq!(cfg.book_scales.get("ETH-USDT-260925"), Some(&(5, 7)));
        assert_eq!(cfg.book_scales.get("XRP-USDT-260925"), Some(&(4, 1)));
        assert!(cfg.enable_l2);
    }

    #[test]
    fn session_config_rejects_requested_id_absent_from_catalog() {
        let request = ConcreteSubscriptionSet {
            items: vec![ConcreteSubscription {
                instrument: InstrumentId(999),
                channel: Channel::Trades,
                delivery: DeliveryOptions::default(),
            }],
        };

        let error = session_config_from_catalog(&catalog(vec![]), &request, false)
            .expect_err("missing instrument must be rejected");
        assert!(matches!(error, AdapterError::Catalog(message) if message.contains("999")));
    }

    #[test]
    fn futures_plan_routes_candles_to_business_endpoint() {
        let request = ConcreteSubscriptionSet {
            items: vec![ConcreteSubscription {
                instrument: InstrumentId(41),
                channel: Channel::Candles {
                    interval: CandleInterval::M5,
                },
                delivery: DeliveryOptions::default(),
            }],
        };
        let plans = OkxFuturesFactory::default()
            .plan(
                &request,
                &catalog(vec![instrument(InstrumentId(41), "ETH-USDT-260925", 5, 7)]),
            )
            .expect("candle plan");

        assert_eq!(plans.len(), 1);
        assert_eq!(
            plans[0].endpoint_name,
            "wss://ws.okx.com:8443/ws/v5/business"
        );
        assert_eq!(plans[0].subscriptions, request);
    }

    #[test]
    fn futures_plan_splits_public_and_business_channels() {
        let trade = ConcreteSubscription {
            instrument: InstrumentId(41),
            channel: Channel::Trades,
            delivery: DeliveryOptions::default(),
        };
        let candle = ConcreteSubscription {
            instrument: InstrumentId(41),
            channel: Channel::Candles {
                interval: CandleInterval::M5,
            },
            delivery: DeliveryOptions::default(),
        };
        let request = ConcreteSubscriptionSet {
            items: vec![trade.clone(), candle.clone()],
        };

        let plans = OkxFuturesFactory::default()
            .plan(
                &request,
                &catalog(vec![instrument(InstrumentId(41), "ETH-USDT-260925", 5, 7)]),
            )
            .expect("mixed plan");

        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].endpoint_name, PUBLIC_WS_URL);
        assert_eq!(plans[0].subscriptions.items, vec![trade]);
        assert_eq!(
            plans[1].endpoint_name,
            "wss://ws.okx.com:8443/ws/v5/business"
        );
        assert_eq!(plans[1].subscriptions.items, vec![candle]);
    }
}
