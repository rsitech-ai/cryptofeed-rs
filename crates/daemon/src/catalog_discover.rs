//! One-shot REST instrument catalog discovery via engine HTTP transport.
//!
//! # ponytail
//! Daemon (not SessionMachine) issues HTTP — SM stays networking-free (ADR-0004).
//! Live path for factories with non-empty `instrument_requests` (Binance / OKX /
//! Bybit / Kraken / Deribit / Coinbase Exchange / Coinbase-adv / Bitstamp /
//! Bitfinex / bitfinex-deriv / Gemini). Gemini optional capped N+1 details via
//! `instrument_detail_requests` (`GEMINI_LIVE_DETAILS_MAX`, default 0).
//! Synthetic stays stub (test venue). Ceiling: CLI one-shot + scripted fixtures;
//! upgrade = engine-owned refresh loop + catalog versioning.

use marketfeed_adapter_api::{
    Environment, HttpMethod as AdapterMethod, HttpRequestSpec, HttpResponse as AdapterHttpResponse,
    VenueFactory,
};
use marketfeed_model::{CatalogVersion, CatalogView, InstrumentDefinition, InstrumentId, VenueId};
use marketfeed_transport::{HttpMethod, HttpRequest, HttpTransport};

/// Discover instruments through `factory.instrument_requests` + `parse_instruments`.
///
/// HTTP is executed by `http` (live `ReqwestHttpTransport` or scripted test stub).
pub async fn discover_catalog<F, H>(
    factory: &F,
    http: &H,
    venue_id: VenueId,
    environment: Environment,
    catalog_version: CatalogVersion,
) -> Result<CatalogView, String>
where
    F: VenueFactory,
    H: HttpTransport,
{
    let specs = factory
        .instrument_requests(environment)
        .map_err(|e| e.to_string())?;
    if specs.is_empty() {
        return Err("venue has no REST instrument discovery (config-stub catalog only)".into());
    }

    let mut responses = Vec::with_capacity(specs.len());
    for spec in specs {
        responses.push(execute_http(http, spec).await?);
    }

    let mut defs: Vec<InstrumentDefinition> = Vec::new();
    factory
        .parse_instruments(&responses, &mut defs)
        .map_err(|e| e.to_string())?;
    if defs.is_empty() {
        return Err("parse_instruments produced no instruments".into());
    }

    let detail_specs = factory
        .instrument_detail_requests(&defs)
        .map_err(|e| e.to_string())?;
    if !detail_specs.is_empty() {
        let mut detail_responses = Vec::with_capacity(detail_specs.len());
        for spec in detail_specs {
            detail_responses.push(execute_http(http, spec).await?);
        }
        factory
            .apply_instrument_details(&detail_responses, &mut defs)
            .map_err(|e| e.to_string())?;
    }

    let instruments: Vec<_> = defs
        .into_iter()
        .enumerate()
        .map(|(i, def)| {
            def.into_instrument(InstrumentId((i as u32).saturating_add(1)), catalog_version)
        })
        .collect();

    Ok(CatalogView::with_instruments(
        venue_id,
        catalog_version,
        std::sync::Arc::from(instruments),
    ))
}

async fn execute_http<H: HttpTransport>(
    http: &H,
    spec: HttpRequestSpec,
) -> Result<AdapterHttpResponse, String> {
    let req = HttpRequest {
        method: match spec.method {
            AdapterMethod::Get => HttpMethod::Get,
            AdapterMethod::Post => HttpMethod::Post,
            AdapterMethod::Put => HttpMethod::Put,
            AdapterMethod::Delete => HttpMethod::Delete,
        },
        url: spec.url,
        headers: spec.headers,
        body: spec.body,
        timeout_ms: 10_000,
        // Binance spot exchangeInfo is ~17MB and growing; 16MiB truncated discovery.
        max_body_bytes: 32 * 1024 * 1024,
    };
    let resp = http.request(req).await.map_err(|e| e.to_string())?;
    Ok(AdapterHttpResponse {
        status: resp.status,
        headers: resp.headers,
        body: resp.body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use marketfeed_adapter_binance::BinanceSpotFactory;
    use marketfeed_adapter_bitfinex::{BitfinexDerivFactory, BitfinexFactory};
    use marketfeed_adapter_bitstamp::BitstampFactory;
    use marketfeed_adapter_coinbase::CoinbaseAdvFactory;
    use marketfeed_adapter_gemini::GeminiFactory;
    use marketfeed_adapter_okx::OkxSpotFactory;
    use marketfeed_transport::{HttpResponse, ScriptedHttpTransport};

    fn binance_exchange_info_body() -> Bytes {
        Bytes::from_static(
            br#"{
          "symbols":[{
            "symbol":"BTCUSDT",
            "status":"TRADING",
            "baseAsset":"BTC",
            "quoteAsset":"USDT",
            "filters":[
              {"filterType":"PRICE_FILTER","tickSize":"0.01"},
              {"filterType":"LOT_SIZE","stepSize":"0.00001000"}
            ]
          },{
            "symbol":"ETHUSDT",
            "status":"TRADING",
            "baseAsset":"ETH",
            "quoteAsset":"USDT",
            "filters":[
              {"filterType":"PRICE_FILTER","tickSize":"0.01"},
              {"filterType":"LOT_SIZE","stepSize":"0.00010000"}
            ]
          }]
        }"#,
        )
    }

    fn okx_spot_instruments_body() -> Bytes {
        Bytes::from_static(
            br#"{
          "code":"0",
          "data":[{
            "instId":"BTC-USDT",
            "instType":"SPOT",
            "baseCcy":"BTC",
            "quoteCcy":"USDT",
            "tickSz":"0.1",
            "lotSz":"0.00000001",
            "minSz":"0.00001",
            "state":"live"
          }]
        }"#,
        )
    }

    #[tokio::test]
    async fn binance_spot_scripted_discovery() {
        let http = ScriptedHttpTransport::new();
        http.push(
            "exchangeInfo",
            HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: binance_exchange_info_body(),
            },
        );
        let catalog = discover_catalog(
            &BinanceSpotFactory { enable_l2: false },
            &http,
            marketfeed_adapter_binance::BINANCE_SPOT_VENUE_ID,
            Environment::Production,
            CatalogVersion(1),
        )
        .await
        .expect("discover");
        assert_eq!(catalog.instruments.len(), 2);
        assert_eq!(catalog.instruments[0].key.native_symbol, "BTCUSDT");
        assert_eq!(catalog.instruments[0].price_scale, 2);
        assert_eq!(catalog.instruments[1].key.native_symbol, "ETHUSDT");
        assert_eq!(catalog.instruments[1].quantity_scale, 8);
    }

    #[tokio::test]
    async fn okx_spot_scripted_discovery() {
        let http = ScriptedHttpTransport::new();
        http.push(
            "instruments",
            HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: okx_spot_instruments_body(),
            },
        );
        let catalog = discover_catalog(
            &OkxSpotFactory { enable_l2: false },
            &http,
            marketfeed_adapter_okx::OKX_SPOT_VENUE_ID,
            Environment::Production,
            CatalogVersion(1),
        )
        .await
        .expect("discover");
        assert_eq!(catalog.instruments.len(), 1);
        assert_eq!(catalog.instruments[0].key.native_symbol, "BTC-USDT");
        assert_eq!(catalog.instruments[0].price_scale, 1);
        assert_eq!(catalog.instruments[0].quantity_scale, 8);
    }

    #[tokio::test]
    async fn bitstamp_scripted_discovery() {
        let http = ScriptedHttpTransport::new();
        http.push(
            "trading-pairs-info",
            HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(
                    br#"[{"name":"BTC/USD","url_symbol":"btcusd","base_decimals":8,"counter_decimals":2,"trading":"Enabled"}]"#,
                ),
            },
        );
        let catalog = discover_catalog(
            &BitstampFactory { enable_l2: false },
            &http,
            marketfeed_adapter_bitstamp::BITSTAMP_VENUE_ID,
            Environment::Production,
            CatalogVersion(1),
        )
        .await
        .expect("discover");
        assert_eq!(catalog.instruments.len(), 1);
        assert_eq!(catalog.instruments[0].key.native_symbol, "btcusd");
        assert_eq!(catalog.instruments[0].price_scale, 2);
        assert_eq!(catalog.instruments[0].quantity_scale, 8);
    }

    #[tokio::test]
    async fn bitfinex_scripted_discovery() {
        let http = ScriptedHttpTransport::new();
        http.push(
            "pub:list:pair:exchange",
            HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(br#"[["BTCUSD","ETHUSD"]]"#),
            },
        );
        let catalog = discover_catalog(
            &BitfinexFactory { enable_l2: false },
            &http,
            marketfeed_adapter_bitfinex::BITFINEX_VENUE_ID,
            Environment::Production,
            CatalogVersion(1),
        )
        .await
        .expect("discover");
        assert_eq!(catalog.instruments.len(), 2);
        assert_eq!(catalog.instruments[0].key.native_symbol, "tBTCUSD");
        assert_eq!(catalog.instruments[1].key.native_symbol, "tETHUSD");
    }

    #[tokio::test]
    async fn bitfinex_deriv_scripted_discovery() {
        let http = ScriptedHttpTransport::new();
        http.push(
            "pub:list:pair:futures",
            HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(br#"[["BTCF0:USTF0","ETHF0:BTCF0"]]"#),
            },
        );
        let catalog = discover_catalog(
            &BitfinexDerivFactory { enable_l2: false },
            &http,
            marketfeed_adapter_bitfinex::BITFINEX_DERIV_VENUE_ID,
            Environment::Production,
            CatalogVersion(1),
        )
        .await
        .expect("discover");
        assert_eq!(catalog.instruments.len(), 2);
        assert_eq!(catalog.instruments[0].key.native_symbol, "tBTCF0:USTF0");
        assert_eq!(catalog.instruments[0].key.venue.0, "bitfinex-deriv");
        assert_eq!(catalog.instruments[1].key.native_symbol, "tETHF0:BTCF0");
    }

    #[tokio::test]
    async fn coinbase_adv_scripted_discovery() {
        let http = ScriptedHttpTransport::new();
        http.push(
            "market/products",
            HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(
                    br#"{
                      "products":[{
                        "product_id":"BTC-USD",
                        "base_currency_id":"BTC",
                        "quote_currency_id":"USD",
                        "status":"online",
                        "product_type":"SPOT",
                        "trading_disabled":false,
                        "is_disabled":false,
                        "quote_increment":"0.01",
                        "base_increment":"0.00000001",
                        "quote_min_size":"1",
                        "base_min_size":"0.00000001"
                      }]
                    }"#,
                ),
            },
        );
        let catalog = discover_catalog(
            &CoinbaseAdvFactory { enable_l2: false },
            &http,
            marketfeed_adapter_coinbase::COINBASE_ADV_VENUE_ID,
            Environment::Production,
            CatalogVersion(1),
        )
        .await
        .expect("discover");
        assert_eq!(catalog.instruments.len(), 1);
        assert_eq!(catalog.instruments[0].key.native_symbol, "BTC-USD");
        assert_eq!(catalog.instruments[0].key.venue.0, "coinbase-adv");
        assert_eq!(catalog.instruments[0].price_scale, 2);
        assert_eq!(catalog.instruments[0].quantity_scale, 8);
    }

    #[tokio::test]
    async fn gemini_scripted_discovery_default_scales() {
        let http = ScriptedHttpTransport::new();
        http.push(
            "/v1/symbols",
            HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(br#"["btcusd","ethusd"]"#),
            },
        );
        let catalog = discover_catalog(
            &GeminiFactory {
                enable_l2: false,
                live_details_max: 0,
            },
            &http,
            marketfeed_adapter_gemini::GEMINI_VENUE_ID,
            Environment::Production,
            CatalogVersion(1),
        )
        .await
        .expect("discover");
        assert_eq!(catalog.instruments.len(), 2);
        assert_eq!(catalog.instruments[0].key.native_symbol, "BTCUSD");
        assert_eq!(catalog.instruments[0].price_scale, 2);
        assert_eq!(catalog.instruments[0].quantity_scale, 8);
    }

    #[tokio::test]
    async fn gemini_scripted_discovery_capped_details() {
        let http = ScriptedHttpTransport::new();
        http.push(
            "/v1/symbols",
            HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(br#"["btcusd","ethusd"]"#),
            },
        );
        http.push(
            "symbols/details/btcusd",
            HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(
                    br#"{"symbol":"BTCUSD","base_currency":"BTC","quote_currency":"USD","quote_increment":"0.01","min_order_size":"0.00001"}"#,
                ),
            },
        );
        let catalog = discover_catalog(
            &GeminiFactory {
                enable_l2: false,
                live_details_max: 1,
            },
            &http,
            marketfeed_adapter_gemini::GEMINI_VENUE_ID,
            Environment::Production,
            CatalogVersion(1),
        )
        .await
        .expect("discover");
        assert_eq!(catalog.instruments[0].quantity_scale, 5);
        assert_eq!(catalog.instruments[1].quantity_scale, 8);
    }

    #[tokio::test]
    async fn stub_venue_rejects_live_discovery() {
        let http = ScriptedHttpTransport::new();
        let err = discover_catalog(
            &marketfeed_adapter_synthetic::SyntheticFactory,
            &http,
            marketfeed_adapter_synthetic::SYNTHETIC_VENUE_ID,
            Environment::Production,
            CatalogVersion(1),
        )
        .await
        .expect_err("stub");
        assert!(
            err.contains("no REST instrument discovery"),
            "unexpected: {err}"
        );
    }
}
