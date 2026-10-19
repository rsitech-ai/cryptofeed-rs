//! Bounded REST instrument catalog discovery via engine HTTP transport.
//!
//! # ponytail
//! Daemon (not SessionMachine) issues HTTP — SM stays networking-free (ADR-0004).
//! Live path for factories with non-empty `instrument_requests` (Binance / OKX /
//! Bybit / Kraken / Deribit / Coinbase Exchange / Coinbase-adv / Bitstamp /
//! Bitfinex / bitfinex-deriv / Gemini). Gemini optional capped N+1 details via
//! `instrument_detail_requests` (`GEMINI_LIVE_DETAILS_MAX`, default 0).
//! Primary/continuation collection is bounded to 32 responses, 128 MiB and a
//! 60-second request phase. The existing optional detail phase is separate.
//! Synthetic stays stub (test venue). Ceiling: CLI one-shot + scripted fixtures;
//! upgrade = engine-owned refresh loop + catalog versioning.

use marketfeed_adapter_api::{
    Environment, HttpMethod as AdapterMethod, HttpRequestSpec, HttpResponse as AdapterHttpResponse,
    VenueFactory,
};
use marketfeed_model::{CatalogVersion, CatalogView, InstrumentDefinition, InstrumentId, VenueId};
use marketfeed_transport::{HttpMethod, HttpRequest, HttpTransport};

const MAX_CATALOG_RESPONSES: usize = 32;
const MAX_CATALOG_BYTES: usize = 128 * 1024 * 1024;
const CATALOG_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);

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
    discover_catalog_with_feed(factory, http, venue_id, environment, catalog_version, None).await
}

pub async fn discover_catalog_with_feed<F: VenueFactory, H: HttpTransport>(
    factory: &F,
    http: &H,
    venue_id: VenueId,
    environment: Environment,
    catalog_version: CatalogVersion,
    feed: Option<&marketfeed_recording::shared_feed::FeedHandle>,
) -> Result<CatalogView, String> {
    let responses = collect_catalog_pages(
        factory,
        http,
        venue_id,
        environment,
        feed,
        tokio::time::Instant::now() + CATALOG_DEADLINE,
    )
    .await?;

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
            detail_responses.push(execute_http_captured(http, spec, venue_id, feed).await?);
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

async fn collect_catalog_pages<F: VenueFactory, H: HttpTransport>(
    factory: &F,
    http: &H,
    venue_id: VenueId,
    environment: Environment,
    feed: Option<&marketfeed_recording::shared_feed::FeedHandle>,
    deadline: tokio::time::Instant,
) -> Result<Vec<AdapterHttpResponse>, String> {
    let specs = factory
        .instrument_requests(environment)
        .map_err(|e| e.to_string())?;
    if specs.is_empty() {
        return Err("venue has no REST instrument discovery (config-stub catalog only)".into());
    }

    let mut responses = Vec::new();
    let mut pending = specs;
    let mut request_ids = std::collections::BTreeSet::new();
    let mut total_bytes = 0usize;
    while !pending.is_empty() {
        if pending.len() > MAX_CATALOG_RESPONSES.saturating_sub(responses.len()) {
            return Err("catalog response limit; refusing partial catalog".into());
        }
        for spec in pending {
            if tokio::time::Instant::now() >= deadline {
                return Err("catalog discovery deadline; refusing partial catalog".into());
            }
            if !request_ids.insert(spec.id) {
                return Err("duplicate catalog request ID".into());
            }
            let response = tokio::time::timeout_at(
                deadline,
                execute_http_captured(http, spec, venue_id, feed),
            )
            .await
            .map_err(|_| "catalog discovery deadline; refusing partial catalog".to_string())??;
            if tokio::time::Instant::now() >= deadline {
                return Err("catalog discovery deadline; refusing partial catalog".into());
            }
            total_bytes = total_bytes
                .checked_add(response.body.len())
                .filter(|size| *size <= MAX_CATALOG_BYTES)
                .ok_or("catalog byte limit; refusing partial catalog")?;
            responses.push(response);
        }
        pending = factory
            .instrument_continuation_requests(&responses)
            .map_err(|e| e.to_string())?;
        if tokio::time::Instant::now() >= deadline {
            return Err("catalog discovery deadline; refusing partial catalog".into());
        }
    }

    Ok(responses)
}

async fn execute_http_captured<H: HttpTransport>(
    http: &H,
    spec: HttpRequestSpec,
    venue: VenueId,
    feed: Option<&marketfeed_recording::shared_feed::FeedHandle>,
) -> Result<AdapterHttpResponse, String> {
    use base64::Engine as _;
    let id = spec.id;
    let url = spec.url.clone();
    let method = format!("{:?}", spec.method);
    let request_headers = spec.headers.clone();
    let request_body_base64 =
        base64::engine::general_purpose::STANDARD.encode(spec.body.as_deref().unwrap_or_default());
    let body = spec
        .body
        .as_ref()
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default();
    let response = execute_http(http, spec).await?;
    if let Some(feed) = feed {
        feed.append(
            marketfeed_recording::shared_feed::FeedRecord::CatalogResponse {
                venue: venue.0,
                request_id: id,
                request_body: body,
                status: response.status,
                body_base64: base64::engine::general_purpose::STANDARD.encode(&response.body),
                provenance: Some(marketfeed_recording::shared_feed::CatalogHttpProvenance {
                    url,
                    method,
                    request_headers,
                    request_body_base64,
                    response_headers: response.headers.clone(),
                    received_at_ns: i64::try_from(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_err(|e| e.to_string())?
                            .as_nanos(),
                    )
                    .map_err(|e| e.to_string())?,
                }),
            },
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(response)
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

    fn bybit_page(symbol: &str, cursor: &str) -> HttpResponse {
        HttpResponse {
            status: 200,
            headers: vec![],
            body: Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "retCode":0,"result":{"category":"linear","nextPageCursor":cursor,"list":[{
                        "symbol":symbol,"status":"Trading","baseCoin":"BTC","quoteCoin":"USDT",
                        "contractType":"LinearPerpetual","priceFilter":{"tickSize":"0.10"},
                        "lotSizeFilter":{"qtyStep":"0.001"}
                    }]}
                }))
                .unwrap(),
            ),
        }
    }
    struct OrderedHttp(std::sync::Mutex<std::collections::VecDeque<(String, HttpResponse)>>);
    impl HttpTransport for OrderedHttp {
        async fn request(
            &self,
            req: HttpRequest,
        ) -> Result<HttpResponse, marketfeed_transport::TransportError> {
            let (url, response) = self
                .0
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected request");
            assert_eq!(req.url, url);
            Ok(response)
        }
    }
    fn two_page_http(second_status: u16) -> OrderedHttp {
        let mut second = bybit_page("ETHUSDT", "");
        second.status = second_status;
        OrderedHttp(std::sync::Mutex::new(std::collections::VecDeque::from([
            ("https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000".into(),bybit_page("BTCUSDT","A&+")),
            ("https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000&cursor=A%26%2B".into(),second),
        ])))
    }
    #[tokio::test]
    async fn bybit_pagination_publishes_both_pages_and_captures_exact_http_provenance() {
        use marketfeed_recording::shared_feed::{FeedHandle, FeedReader, FeedRecord, FeedWriter};
        let directory = std::env::temp_dir().join(format!(
            "marketfeed-catalog-pages-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let feed = FeedHandle::new(FeedWriter::create(&directory, 1_000_000).unwrap());
        let http = two_page_http(200);
        let catalog = discover_catalog_with_feed(
            &marketfeed_adapter_bybit::BybitFactory::default(),
            &http,
            VenueId(5),
            Environment::Production,
            CatalogVersion(7),
            Some(&feed),
        )
        .await
        .unwrap();
        assert_eq!(
            catalog
                .instruments
                .iter()
                .map(|i| i.key.native_symbol.as_str())
                .collect::<Vec<_>>(),
            ["BTCUSDT", "ETHUSDT"]
        );
        assert_eq!(catalog.instruments[0].id, InstrumentId(1));
        assert_eq!(catalog.instruments[1].id, InstrumentId(2));
        assert!(http.0.lock().unwrap().is_empty());
        let mut reader = FeedReader::open(&directory, None).unwrap();
        let mut captured = Vec::new();
        while let Some(record) = reader.read_next().unwrap() {
            if let FeedRecord::CatalogResponse {
                request_id,
                body_base64,
                provenance: Some(provenance),
                ..
            } = record
            {
                use base64::Engine as _;
                let value: serde_json::Value = serde_json::from_slice(
                    &base64::engine::general_purpose::STANDARD
                        .decode(body_base64)
                        .unwrap(),
                )
                .unwrap();
                captured.push((
                    request_id,
                    provenance.url,
                    value["result"]["list"][0]["symbol"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                ));
            }
        }
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].0, 1);
        assert_eq!(captured[1].0, 2);
        assert!(captured[1].1.ends_with("cursor=A%26%2B"));
        assert_eq!(captured[1].2, "ETHUSDT");
        drop(reader);
        drop(feed);
        std::fs::remove_dir_all(directory).unwrap();
    }
    #[tokio::test]
    async fn bybit_failed_later_page_never_returns_partial_catalog() {
        let http = two_page_http(503);
        let error = discover_catalog(
            &marketfeed_adapter_bybit::BybitFactory::default(),
            &http,
            VenueId(5),
            Environment::Production,
            CatalogVersion(1),
        )
        .await
        .unwrap_err();
        assert!(error.contains("HTTP status"), "{error}");
        assert!(http.0.lock().unwrap().is_empty());
    }
    struct LoopFactory {
        duplicate_id: bool,
    }
    impl VenueFactory for LoopFactory {
        fn specification(&self) -> &'static marketfeed_adapter_api::VenueSpecification {
            marketfeed_adapter_synthetic::SyntheticFactory.specification()
        }
        fn instrument_requests(
            &self,
            _: Environment,
        ) -> Result<Vec<HttpRequestSpec>, marketfeed_adapter_api::AdapterError> {
            self.instrument_continuation_requests(&[])
        }
        fn instrument_continuation_requests(
            &self,
            responses: &[AdapterHttpResponse],
        ) -> Result<Vec<HttpRequestSpec>, marketfeed_adapter_api::AdapterError> {
            Ok(vec![HttpRequestSpec {
                id: if self.duplicate_id {
                    1
                } else {
                    responses.len() as u64 + 1
                },
                method: AdapterMethod::Get,
                url: "https://example.invalid/catalog".into(),
                headers: vec![],
                body: None,
            }])
        }
        fn parse_instruments(
            &self,
            _: &[AdapterHttpResponse],
            _: &mut Vec<InstrumentDefinition>,
        ) -> Result<(), marketfeed_adapter_api::AdapterError> {
            panic!("incomplete catalog must not be parsed")
        }
        fn plan(
            &self,
            _: &marketfeed_adapter_api::ConcreteSubscriptionSet,
            _: &CatalogView,
        ) -> Result<Vec<marketfeed_adapter_api::SessionSpec>, marketfeed_adapter_api::AdapterError>
        {
            unreachable!()
        }
        fn create_session(
            &self,
            _: marketfeed_adapter_api::SessionSpec,
            _: CatalogView,
        ) -> Result<
            Box<dyn marketfeed_adapter_api::SessionMachine>,
            marketfeed_adapter_api::AdapterError,
        > {
            unreachable!()
        }
    }
    struct CountingHttp {
        body: Bytes,
        calls: std::sync::atomic::AtomicUsize,
        delay: std::time::Duration,
    }
    impl HttpTransport for CountingHttp {
        async fn request(
            &self,
            _: HttpRequest,
        ) -> Result<HttpResponse, marketfeed_transport::TransportError> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            Ok(HttpResponse {
                status: 200,
                headers: vec![],
                body: self.body.clone(),
            })
        }
    }
    #[tokio::test]
    async fn pagination_request_byte_and_identity_limits_fail_before_parsing() {
        for (duplicate, body_size, expected_calls, reason) in [
            (false, 0, 32, "response limit"),
            (true, 0, 1, "duplicate catalog request ID"),
            (false, 16 * 1024 * 1024, 9, "byte limit"),
        ] {
            let http = CountingHttp {
                body: Bytes::from(vec![0; body_size]),
                calls: 0.into(),
                delay: std::time::Duration::ZERO,
            };
            let error = discover_catalog(
                &LoopFactory {
                    duplicate_id: duplicate,
                },
                &http,
                VenueId(5),
                Environment::Production,
                CatalogVersion(1),
            )
            .await
            .unwrap_err();
            assert!(error.contains(reason), "{error}");
            assert_eq!(
                http.calls.load(std::sync::atomic::Ordering::SeqCst),
                expected_calls
            );
        }
    }
    #[tokio::test]
    async fn pagination_deadline_cancels_slow_http_and_prevents_post_deadline_request() {
        let http = CountingHttp {
            body: Bytes::new(),
            calls: 0.into(),
            delay: std::time::Duration::from_millis(50),
        };
        let factory = LoopFactory {
            duplicate_id: false,
        };
        let error = collect_catalog_pages(
            &factory,
            &http,
            VenueId(5),
            Environment::Production,
            None,
            tokio::time::Instant::now() - std::time::Duration::from_millis(1),
        )
        .await
        .unwrap_err();
        assert!(error.contains("deadline"));
        assert_eq!(http.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        let error = collect_catalog_pages(
            &factory,
            &http,
            VenueId(5),
            Environment::Production,
            None,
            tokio::time::Instant::now() + std::time::Duration::from_millis(5),
        )
        .await
        .unwrap_err();
        assert!(error.contains("deadline"));
        assert!(http.calls.load(std::sync::atomic::Ordering::SeqCst) <= 1);
    }
}
