//! Offline CLI helpers: `catalog` / `plan` / `benchmark` (§20.1).
//!
//! # Catalog modes
//! - **stub** (default): symbols from TOML `[[venues]].symbols` via `catalog_for_venue`.
//! - **live** (`--live`): one-shot REST via `VenueFactory::{instrument_requests,parse_instruments}`
//!   and engine `HttpTransport` (daemon issues HTTP; SessionMachine stays networking-free).
//!
//! # ponytail
//! Live discovery ships for venues with real parsers (Binance / OKX / Bybit / Kraken /
//! Deribit / Coinbase Exchange / Coinbase-adv / Bitstamp / Bitfinex / Gemini). Gemini
//! uses `/v1/symbols` (default scales) + optional capped N+1 details
//! (`GEMINI_LIVE_DETAILS_MAX`, default 0). Synthetic stays stub (test-only).
//! Ceiling: CLI one-shot; upgrade = engine catalog refresh loop.

use std::fmt::Write as _;
use std::path::Path;
use std::time::Instant;

use marketfeed_adapter_api::{ConcreteSubscriptionSet, Environment, VenueFactory};
use marketfeed_adapter_binance::{
    BINANCE_COINM_VENUE_ID, BINANCE_SPOT_VENUE_ID, BINANCE_USDM_VENUE_ID, BinanceCoinmFactory,
    BinanceSpotFactory, BinanceUsdmFactory,
};
use marketfeed_adapter_bitfinex::{
    BITFINEX_DERIV_VENUE_ID, BITFINEX_VENUE_ID, BitfinexDerivFactory, BitfinexFactory,
};
use marketfeed_adapter_bitstamp::{BITSTAMP_VENUE_ID, BitstampFactory};
use marketfeed_adapter_bybit::{
    BYBIT_INVERSE_VENUE_ID, BYBIT_LINEAR_VENUE_ID, BYBIT_SPOT_VENUE_ID, BybitCategory, BybitFactory,
};
use marketfeed_adapter_coinbase::{
    COINBASE_ADV_VENUE_ID, COINBASE_INTL_VENUE_ID, COINBASE_SPOT_VENUE_ID, CoinbaseAdvFactory,
    CoinbaseIntlCredentials, CoinbaseIntlFactory, CoinbaseSpotFactory,
};
use marketfeed_adapter_deribit::{DERIBIT_VENUE_ID, DeribitFactory};
use marketfeed_adapter_gemini::{GEMINI_VENUE_ID, GeminiFactory};
use marketfeed_adapter_kraken::{
    KRAKEN_FUTURES_VENUE_ID, KRAKEN_SPOT_VENUE_ID, KrakenFuturesFactory, KrakenSpotFactory,
};
use marketfeed_adapter_okx::{
    OKX_FUTURES_VENUE_ID, OKX_SPOT_VENUE_ID, OKX_SWAP_VENUE_ID, OkxFuturesFactory, OkxSpotFactory,
    OkxSwapFactory,
};
use marketfeed_adapter_synthetic::{SYNTHETIC_VENUE_ID, SyntheticFactory};
use marketfeed_model::{CatalogVersion, CatalogView, InstrumentKind, VenueId};
use marketfeed_transport::{HttpTransport, ReqwestHttpTransport};

use crate::catalog_discover::discover_catalog;
use crate::config::{DaemonConfig, VenueConfig, VenueKind};
use crate::run::catalog_for_venue;
use crate::subscriptions::expand_concrete_subscriptions;

struct VenueMeta {
    venue_id: VenueId,
    venue_code: &'static str,
    kind: InstrumentKind,
}

fn venue_meta(kind: VenueKind) -> VenueMeta {
    match kind {
        VenueKind::Synthetic => VenueMeta {
            venue_id: SYNTHETIC_VENUE_ID,
            venue_code: "synthetic",
            kind: InstrumentKind::Spot,
        },
        VenueKind::BinanceSpot => VenueMeta {
            venue_id: BINANCE_SPOT_VENUE_ID,
            venue_code: "binance-spot",
            kind: InstrumentKind::Spot,
        },
        VenueKind::BinanceUsdm => VenueMeta {
            venue_id: BINANCE_USDM_VENUE_ID,
            venue_code: "binance-usdm",
            kind: InstrumentKind::PerpetualLinear,
        },
        VenueKind::BinanceCoinm => VenueMeta {
            venue_id: BINANCE_COINM_VENUE_ID,
            venue_code: "binance-coinm",
            kind: InstrumentKind::PerpetualInverse,
        },
        VenueKind::OkxSpot => VenueMeta {
            venue_id: OKX_SPOT_VENUE_ID,
            venue_code: "okx-spot",
            kind: InstrumentKind::Spot,
        },
        VenueKind::OkxSwap => VenueMeta {
            venue_id: OKX_SWAP_VENUE_ID,
            venue_code: "okx-swap",
            kind: InstrumentKind::PerpetualLinear,
        },
        VenueKind::OkxFutures => VenueMeta {
            venue_id: OKX_FUTURES_VENUE_ID,
            venue_code: "okx-futures",
            kind: InstrumentKind::FutureLinear,
        },
        VenueKind::BybitLinear => VenueMeta {
            venue_id: BYBIT_LINEAR_VENUE_ID,
            venue_code: "bybit-linear",
            kind: InstrumentKind::PerpetualLinear,
        },
        VenueKind::BybitSpot => VenueMeta {
            venue_id: BYBIT_SPOT_VENUE_ID,
            venue_code: "bybit-spot",
            kind: InstrumentKind::Spot,
        },
        VenueKind::BybitInverse => VenueMeta {
            venue_id: BYBIT_INVERSE_VENUE_ID,
            venue_code: "bybit-inverse",
            kind: InstrumentKind::PerpetualInverse,
        },
        VenueKind::KrakenSpot => VenueMeta {
            venue_id: KRAKEN_SPOT_VENUE_ID,
            venue_code: "kraken-spot",
            kind: InstrumentKind::Spot,
        },
        VenueKind::KrakenFutures => VenueMeta {
            venue_id: KRAKEN_FUTURES_VENUE_ID,
            venue_code: "kraken-futures",
            kind: InstrumentKind::PerpetualLinear,
        },
        VenueKind::Deribit => VenueMeta {
            venue_id: DERIBIT_VENUE_ID,
            venue_code: "deribit",
            kind: InstrumentKind::PerpetualInverse,
        },
        VenueKind::Bitstamp => VenueMeta {
            venue_id: BITSTAMP_VENUE_ID,
            venue_code: "bitstamp",
            kind: InstrumentKind::Spot,
        },
        VenueKind::Gemini => VenueMeta {
            venue_id: GEMINI_VENUE_ID,
            venue_code: "gemini",
            kind: InstrumentKind::Spot,
        },
        VenueKind::CoinbaseSpot => VenueMeta {
            venue_id: COINBASE_SPOT_VENUE_ID,
            venue_code: "coinbase-spot",
            kind: InstrumentKind::Spot,
        },
        VenueKind::CoinbaseAdvanced => VenueMeta {
            venue_id: COINBASE_ADV_VENUE_ID,
            venue_code: "coinbase-adv",
            kind: InstrumentKind::Spot,
        },
        VenueKind::CoinbaseIntl => VenueMeta {
            venue_id: COINBASE_INTL_VENUE_ID,
            venue_code: "coinbase-intl",
            kind: InstrumentKind::PerpetualLinear,
        },
        VenueKind::Bitfinex => VenueMeta {
            venue_id: BITFINEX_VENUE_ID,
            venue_code: "bitfinex",
            kind: InstrumentKind::Spot,
        },
        VenueKind::BitfinexDeriv => VenueMeta {
            venue_id: BITFINEX_DERIV_VENUE_ID,
            venue_code: "bitfinex-deriv",
            kind: InstrumentKind::PerpetualLinear,
        },
    }
}

fn catalog_for_config_venue(venue: &VenueConfig) -> Result<(VenueKind, CatalogView), String> {
    let kind = venue.resolved_kind().map_err(|e| e.to_string())?;
    let meta = venue_meta(kind);
    let catalog = catalog_for_venue(meta.venue_id, meta.venue_code, meta.kind, &venue.symbols);
    Ok((kind, catalog))
}

fn plan_with_factory<F: VenueFactory>(
    factory: F,
    request: &ConcreteSubscriptionSet,
    catalog: &CatalogView,
) -> Result<Vec<String>, String> {
    let plan = factory.plan(request, catalog).map_err(|e| e.to_string())?;
    Ok(plan
        .into_iter()
        .map(|s| {
            format!(
                "endpoint={} subs={}",
                s.endpoint_name,
                s.subscriptions.items.len()
            )
        })
        .collect())
}

fn plan_lines(
    kind: VenueKind,
    venue: &VenueConfig,
    catalog: &CatalogView,
) -> Result<Vec<String>, String> {
    let l2 = venue.wants_l2();
    let request = expand_concrete_subscriptions(venue, catalog).map_err(|e| e.to_string())?;
    match kind {
        VenueKind::Synthetic => plan_with_factory(SyntheticFactory, &request, catalog),
        VenueKind::BinanceSpot => {
            plan_with_factory(BinanceSpotFactory { enable_l2: l2 }, &request, catalog)
        }
        VenueKind::BinanceUsdm => {
            plan_with_factory(BinanceUsdmFactory { enable_l2: l2 }, &request, catalog)
        }
        VenueKind::BinanceCoinm => {
            plan_with_factory(BinanceCoinmFactory { enable_l2: l2 }, &request, catalog)
        }
        VenueKind::OkxSpot => {
            plan_with_factory(OkxSpotFactory { enable_l2: l2 }, &request, catalog)
        }
        VenueKind::OkxSwap => {
            plan_with_factory(OkxSwapFactory { enable_l2: l2 }, &request, catalog)
        }
        VenueKind::OkxFutures => {
            plan_with_factory(OkxFuturesFactory { enable_l2: l2 }, &request, catalog)
        }
        VenueKind::BybitLinear => plan_with_factory(
            BybitFactory {
                category: BybitCategory::Linear,
                enable_l2: l2,
            },
            &request,
            catalog,
        ),
        VenueKind::BybitSpot => plan_with_factory(
            BybitFactory {
                category: BybitCategory::Spot,
                enable_l2: l2,
            },
            &request,
            catalog,
        ),
        VenueKind::BybitInverse => plan_with_factory(
            BybitFactory {
                category: BybitCategory::Inverse,
                enable_l2: l2,
            },
            &request,
            catalog,
        ),
        VenueKind::KrakenSpot => {
            plan_with_factory(KrakenSpotFactory { enable_l2: l2 }, &request, catalog)
        }
        VenueKind::KrakenFutures => {
            plan_with_factory(KrakenFuturesFactory { enable_l2: l2 }, &request, catalog)
        }
        VenueKind::Deribit => {
            plan_with_factory(DeribitFactory { enable_l2: l2 }, &request, catalog)
        }
        VenueKind::Bitstamp => {
            plan_with_factory(BitstampFactory { enable_l2: l2 }, &request, catalog)
        }
        VenueKind::Gemini => plan_with_factory(
            GeminiFactory {
                enable_l2: l2,
                ..Default::default()
            },
            &request,
            catalog,
        ),
        VenueKind::CoinbaseSpot => plan_with_factory(
            CoinbaseSpotFactory {
                enable_l2: l2,
                credentials: None,
            },
            &request,
            catalog,
        ),
        VenueKind::CoinbaseAdvanced => {
            plan_with_factory(CoinbaseAdvFactory { enable_l2: l2 }, &request, catalog)
        }
        VenueKind::CoinbaseIntl => plan_with_factory(
            CoinbaseIntlFactory {
                enable_l2: l2,
                credentials: Some(CoinbaseIntlCredentials::fixture()),
            },
            &request,
            catalog,
        ),
        VenueKind::Bitfinex => {
            plan_with_factory(BitfinexFactory { enable_l2: l2 }, &request, catalog)
        }
        VenueKind::BitfinexDeriv => {
            plan_with_factory(BitfinexDerivFactory { enable_l2: l2 }, &request, catalog)
        }
    }
}

fn render_catalog(
    venue: &VenueConfig,
    meta: &VenueMeta,
    catalog: &CatalogView,
    source: &str,
) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "venue_id={} adapter={} segment={:?} venue_code={} catalog_version={} instruments={} source={}",
        venue.id,
        venue.adapter,
        venue.segment,
        meta.venue_code,
        catalog.version.0,
        catalog.instruments.len(),
        source
    );
    if catalog.instruments.is_empty() {
        let _ = writeln!(out, "(empty stub catalog — add symbols= in config)");
    }
    for inst in catalog.instruments.iter() {
        let _ = writeln!(
            out,
            "  id={} symbol={} kind={:?} price_scale={} qty_scale={}",
            inst.id.0, inst.key.native_symbol, inst.key.kind, inst.price_scale, inst.quantity_scale
        );
    }
    out
}

async fn discover_catalog_for_venue<H: HttpTransport>(
    kind: VenueKind,
    venue: &VenueConfig,
    http: &H,
) -> Result<CatalogView, String> {
    let meta = venue_meta(kind);
    let l2 = venue.wants_l2();
    let env = Environment::Production;
    let ver = CatalogVersion(1);
    match kind {
        VenueKind::BinanceSpot => {
            discover_catalog(
                &BinanceSpotFactory { enable_l2: l2 },
                http,
                meta.venue_id,
                env,
                ver,
            )
            .await
        }
        VenueKind::BinanceUsdm => {
            discover_catalog(
                &BinanceUsdmFactory { enable_l2: l2 },
                http,
                meta.venue_id,
                env,
                ver,
            )
            .await
        }
        VenueKind::BinanceCoinm => {
            discover_catalog(
                &BinanceCoinmFactory { enable_l2: l2 },
                http,
                meta.venue_id,
                env,
                ver,
            )
            .await
        }
        VenueKind::OkxSpot => {
            discover_catalog(
                &OkxSpotFactory { enable_l2: l2 },
                http,
                meta.venue_id,
                env,
                ver,
            )
            .await
        }
        VenueKind::OkxSwap => {
            discover_catalog(
                &OkxSwapFactory { enable_l2: l2 },
                http,
                meta.venue_id,
                env,
                ver,
            )
            .await
        }
        VenueKind::OkxFutures => {
            discover_catalog(
                &OkxFuturesFactory { enable_l2: l2 },
                http,
                meta.venue_id,
                env,
                ver,
            )
            .await
        }
        VenueKind::BybitLinear => {
            discover_catalog(
                &BybitFactory {
                    category: BybitCategory::Linear,
                    enable_l2: l2,
                },
                http,
                meta.venue_id,
                env,
                ver,
            )
            .await
        }
        VenueKind::BybitSpot => {
            discover_catalog(
                &BybitFactory {
                    category: BybitCategory::Spot,
                    enable_l2: l2,
                },
                http,
                meta.venue_id,
                env,
                ver,
            )
            .await
        }
        VenueKind::BybitInverse => {
            discover_catalog(
                &BybitFactory {
                    category: BybitCategory::Inverse,
                    enable_l2: l2,
                },
                http,
                meta.venue_id,
                env,
                ver,
            )
            .await
        }
        VenueKind::KrakenSpot => {
            discover_catalog(
                &KrakenSpotFactory { enable_l2: l2 },
                http,
                meta.venue_id,
                env,
                ver,
            )
            .await
        }
        VenueKind::KrakenFutures => {
            discover_catalog(
                &KrakenFuturesFactory { enable_l2: l2 },
                http,
                meta.venue_id,
                env,
                ver,
            )
            .await
        }
        VenueKind::Deribit => {
            discover_catalog(
                &DeribitFactory { enable_l2: l2 },
                http,
                meta.venue_id,
                env,
                ver,
            )
            .await
        }
        VenueKind::CoinbaseSpot => {
            discover_catalog(
                &CoinbaseSpotFactory {
                    enable_l2: l2,
                    credentials: None,
                },
                http,
                meta.venue_id,
                env,
                ver,
            )
            .await
        }
        VenueKind::CoinbaseAdvanced => {
            discover_catalog(
                &CoinbaseAdvFactory { enable_l2: l2 },
                http,
                meta.venue_id,
                env,
                ver,
            )
            .await
        }
        VenueKind::CoinbaseIntl => {
            discover_catalog(
                &CoinbaseIntlFactory {
                    enable_l2: l2,
                    credentials: Some(CoinbaseIntlCredentials::fixture()),
                },
                http,
                meta.venue_id,
                env,
                ver,
            )
            .await
        }
        VenueKind::Bitstamp => {
            discover_catalog(
                &BitstampFactory { enable_l2: l2 },
                http,
                meta.venue_id,
                env,
                ver,
            )
            .await
        }
        VenueKind::Bitfinex => {
            discover_catalog(
                &BitfinexFactory { enable_l2: l2 },
                http,
                meta.venue_id,
                env,
                ver,
            )
            .await
        }
        VenueKind::BitfinexDeriv => {
            discover_catalog(
                &BitfinexDerivFactory { enable_l2: l2 },
                http,
                meta.venue_id,
                env,
                ver,
            )
            .await
        }
        VenueKind::Gemini => {
            discover_catalog(
                &GeminiFactory {
                    enable_l2: l2,
                    ..Default::default()
                },
                http,
                meta.venue_id,
                env,
                ver,
            )
            .await
        }
        VenueKind::Synthetic => Err(format!(
            "venue {:?} has stub instrument discovery only (use config symbols=; no --live)",
            kind
        )),
    }
}

/// Format catalog for one venue id (stub from config, or `--live` REST discovery).
pub fn format_catalog(cfg: &DaemonConfig, venue_id: &str, live: bool) -> Result<String, String> {
    let venue = cfg
        .venues
        .iter()
        .find(|v| v.id == venue_id)
        .ok_or_else(|| format!("venue id {venue_id:?} not in config"))?;
    let kind = venue.resolved_kind().map_err(|e| e.to_string())?;
    let meta = venue_meta(kind);
    if !live {
        let (_, catalog) = catalog_for_config_venue(venue)?;
        return Ok(render_catalog(venue, &meta, &catalog, "stub"));
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    rt.block_on(format_catalog_live(cfg, venue_id))
}

/// Async live catalog path for callers that already own a Tokio runtime.
pub async fn format_catalog_live(cfg: &DaemonConfig, venue_id: &str) -> Result<String, String> {
    let http = ReqwestHttpTransport::new().map_err(|e| e.to_string())?;
    format_catalog_with_http_async(cfg, venue_id, &http).await
}

/// Test/helper: format live catalog using a supplied HTTP transport (scripted fixtures).
pub fn format_catalog_with_http<H: HttpTransport>(
    cfg: &DaemonConfig,
    venue_id: &str,
    http: &H,
) -> Result<String, String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    rt.block_on(format_catalog_with_http_async(cfg, venue_id, http))
}

/// Async test/helper variant that is safe inside an existing Tokio runtime.
pub async fn format_catalog_with_http_async<H: HttpTransport>(
    cfg: &DaemonConfig,
    venue_id: &str,
    http: &H,
) -> Result<String, String> {
    let venue = cfg
        .venues
        .iter()
        .find(|v| v.id == venue_id)
        .ok_or_else(|| format!("venue id {venue_id:?} not in config"))?;
    let kind = venue.resolved_kind().map_err(|e| e.to_string())?;
    let meta = venue_meta(kind);
    let catalog = discover_catalog_for_venue(kind, venue, http).await?;
    Ok(render_catalog(venue, &meta, &catalog, "live"))
}

/// Format session plans for all configured venues.
pub fn format_plan(cfg: &DaemonConfig) -> Result<String, String> {
    let mut out = String::new();
    let _ = writeln!(out, "venues={}", cfg.venues.len());
    for venue in &cfg.venues {
        let (kind, catalog) = catalog_for_config_venue(venue)?;
        let lines = plan_lines(kind, venue, &catalog)?;
        let _ = writeln!(
            out,
            "venue={} adapter={} sessions={}",
            venue.id,
            venue.adapter,
            lines.len()
        );
        for (i, line) in lines.iter().enumerate() {
            let _ = writeln!(out, "  session[{i}] {line}");
        }
    }
    Ok(out)
}

/// Micro-benchmark: JSON parse of a fixture (or raw byte scan for non-JSON).
pub fn format_benchmark(path: &Path, iterations: u32) -> Result<String, String> {
    let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
    let iters = iterations.max(1);
    let start = Instant::now();
    let mut ok = 0u32;
    for _ in 0..iters {
        match serde_json::from_slice::<serde_json::Value>(&bytes) {
            Ok(_) => ok += 1,
            Err(_) => {
                let _ = bytes.iter().fold(0u8, |a, b| a ^ b);
                ok += 1;
            }
        }
    }
    let elapsed = start.elapsed();
    let ns_per = elapsed.as_nanos() / u128::from(iters);
    Ok(format!(
        "fixture={} bytes={} iterations={iters} ok={ok} elapsed_ns={} ns_per_iter={ns_per}",
        path.display(),
        bytes.len(),
        elapsed.as_nanos()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn synthetic_cfg() -> DaemonConfig {
        let toml = r#"
[engine]
[telemetry]
bind = "127.0.0.1:0"
[readiness]
require_recording_healthy = false
[[venues]]
id = "syn"
adapter = "synthetic"
required = true
symbols = ["BTC-USD"]
channels = ["trades", "l2"]
"#;
        DaemonConfig::from_toml_str(toml).expect("cfg")
    }

    #[test]
    fn catalog_lists_stub_instruments() {
        let cfg = synthetic_cfg();
        let out = format_catalog(&cfg, "syn", false).unwrap();
        assert!(out.contains("instruments=1"), "{out}");
        assert!(out.contains("BTC-USD"), "{out}");
        assert!(out.contains("source=stub"), "{out}");
    }

    #[tokio::test]
    async fn catalog_live_binance_scripted_http_inside_runtime() {
        use bytes::Bytes;
        use marketfeed_transport::{HttpResponse, ScriptedHttpTransport};

        let toml = r#"
[engine]
[telemetry]
bind = "127.0.0.1:0"
[readiness]
require_recording_healthy = false
[[venues]]
id = "binance"
adapter = "binance"
segment = "spot"
symbols = ["IGNORED"]
channels = ["trades"]
"#;
        let cfg = DaemonConfig::from_toml_str(toml).expect("cfg");
        let http = ScriptedHttpTransport::new();
        http.push(
            "exchangeInfo",
            HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(
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
          }]
        }"#,
                ),
            },
        );
        let out = format_catalog_with_http_async(&cfg, "binance", &http)
            .await
            .unwrap();
        assert!(out.contains("source=live"), "{out}");
        assert!(out.contains("instruments=1"), "{out}");
        assert!(out.contains("BTCUSDT"), "{out}");
        assert!(!out.contains("IGNORED"), "{out}");
    }

    #[test]
    fn catalog_live_bitstamp_scripted_http() {
        use bytes::Bytes;
        use marketfeed_transport::{HttpResponse, ScriptedHttpTransport};

        let toml = r#"
[engine]
[telemetry]
bind = "127.0.0.1:0"
[readiness]
require_recording_healthy = false
[[venues]]
id = "bitstamp"
adapter = "bitstamp"
symbols = ["IGNORED"]
channels = ["trades"]
"#;
        let cfg = DaemonConfig::from_toml_str(toml).expect("cfg");
        let http = ScriptedHttpTransport::new();
        http.push(
            "trading-pairs-info",
            HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(
                    br#"[{"name":"ETH/USD","url_symbol":"ethusd","base_decimals":5,"counter_decimals":2,"trading":"Enabled"}]"#,
                ),
            },
        );
        let out = format_catalog_with_http(&cfg, "bitstamp", &http).unwrap();
        assert!(out.contains("source=live"), "{out}");
        assert!(out.contains("instruments=1"), "{out}");
        assert!(out.contains("ethusd"), "{out}");
        assert!(!out.contains("IGNORED"), "{out}");
    }

    #[test]
    fn catalog_live_bitfinex_scripted_http() {
        use bytes::Bytes;
        use marketfeed_transport::{HttpResponse, ScriptedHttpTransport};

        let toml = r#"
[engine]
[telemetry]
bind = "127.0.0.1:0"
[readiness]
require_recording_healthy = false
[[venues]]
id = "bitfinex"
adapter = "bitfinex"
symbols = ["IGNORED"]
channels = ["trades"]
"#;
        let cfg = DaemonConfig::from_toml_str(toml).expect("cfg");
        let http = ScriptedHttpTransport::new();
        http.push(
            "pub:list:pair:exchange",
            HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(br#"[["BTCUSD"]]"#),
            },
        );
        let out = format_catalog_with_http(&cfg, "bitfinex", &http).unwrap();
        assert!(out.contains("source=live"), "{out}");
        assert!(out.contains("instruments=1"), "{out}");
        assert!(out.contains("tBTCUSD"), "{out}");
        assert!(!out.contains("IGNORED"), "{out}");
    }

    #[test]
    fn catalog_live_bitfinex_deriv_scripted_http() {
        use bytes::Bytes;
        use marketfeed_transport::{HttpResponse, ScriptedHttpTransport};

        let toml = r#"
[engine]
[telemetry]
bind = "127.0.0.1:0"
[readiness]
require_recording_healthy = false
[[venues]]
id = "bitfinex-deriv"
adapter = "bitfinex"
segment = "deriv"
symbols = ["IGNORED"]
channels = ["trades"]
"#;
        let cfg = DaemonConfig::from_toml_str(toml).expect("cfg");
        let http = ScriptedHttpTransport::new();
        http.push(
            "pub:list:pair:futures",
            HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(br#"[["BTCF0:USTF0"]]"#),
            },
        );
        let out = format_catalog_with_http(&cfg, "bitfinex-deriv", &http).unwrap();
        assert!(out.contains("source=live"), "{out}");
        assert!(out.contains("instruments=1"), "{out}");
        assert!(out.contains("tBTCF0:USTF0"), "{out}");
        assert!(out.contains("venue_code=bitfinex-deriv"), "{out}");
        assert!(!out.contains("IGNORED"), "{out}");
    }

    #[test]
    fn bitfinex_deriv_catalog_stub_symbols() {
        let toml = r#"
[engine]
[telemetry]
bind = "127.0.0.1:0"
[readiness]
require_recording_healthy = false
[[venues]]
id = "bitfinex-deriv"
adapter = "bitfinex"
segment = "deriv"
symbols = ["tETHF0:USTF0"]
channels = ["trades"]
"#;
        let cfg = DaemonConfig::from_toml_str(toml).expect("cfg");
        let out = format_catalog(&cfg, "bitfinex-deriv", false).unwrap();
        assert!(out.contains("instruments=1"), "{out}");
        assert!(out.contains("tETHF0:USTF0"), "{out}");
        assert!(out.contains("venue_code=bitfinex-deriv"), "{out}");
        assert!(out.contains("source=stub"), "{out}");
        // Daemon stub catalog → factory session_config_from_catalog product list.
        let (_, catalog) = catalog_for_config_venue(cfg.venues.first().unwrap()).unwrap();
        let cfg_sess = marketfeed_adapter_bitfinex::session_config_from_catalog(
            &catalog,
            BITFINEX_DERIV_VENUE_ID,
            false,
            vec![],
            true,
        );
        assert_eq!(cfg_sess.symbols, vec!["tETHF0:USTF0".to_string()]);
        assert!(cfg_sess.poll_deriv_status);
        assert_eq!(cfg_sess.venue, BITFINEX_DERIV_VENUE_ID);
    }

    #[test]
    fn catalog_live_coinbase_adv_scripted_http() {
        use bytes::Bytes;
        use marketfeed_transport::{HttpResponse, ScriptedHttpTransport};

        let toml = r#"
[engine]
[telemetry]
bind = "127.0.0.1:0"
[readiness]
require_recording_healthy = false
[[venues]]
id = "coinbase-adv"
adapter = "coinbase"
segment = "advanced"
symbols = ["IGNORED"]
channels = ["trades"]
"#;
        let cfg = DaemonConfig::from_toml_str(toml).expect("cfg");
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
        let out = format_catalog_with_http(&cfg, "coinbase-adv", &http).unwrap();
        assert!(out.contains("source=live"), "{out}");
        assert!(out.contains("instruments=1"), "{out}");
        assert!(out.contains("BTC-USD"), "{out}");
        assert!(!out.contains("IGNORED"), "{out}");
    }

    #[test]
    fn catalog_live_gemini_scripted_http() {
        use bytes::Bytes;
        use marketfeed_transport::{HttpResponse, ScriptedHttpTransport};

        let toml = r#"
[engine]
[telemetry]
bind = "127.0.0.1:0"
[readiness]
require_recording_healthy = false
[[venues]]
id = "gemini"
adapter = "gemini"
symbols = ["IGNORED"]
channels = ["trades"]
"#;
        let cfg = DaemonConfig::from_toml_str(toml).expect("cfg");
        let http = ScriptedHttpTransport::new();
        http.push(
            "/v1/symbols",
            HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: Bytes::from_static(br#"["btcusd","ethusd"]"#),
            },
        );
        let out = format_catalog_with_http(&cfg, "gemini", &http).unwrap();
        assert!(out.contains("source=live"), "{out}");
        assert!(out.contains("instruments=2"), "{out}");
        assert!(out.contains("BTCUSD"), "{out}");
        assert!(!out.contains("IGNORED"), "{out}");
    }

    #[test]
    fn coinbase_adv_catalog_stub_symbols() {
        let toml = r#"
[engine]
[telemetry]
bind = "127.0.0.1:0"
[readiness]
require_recording_healthy = false
[[venues]]
id = "coinbase-adv"
adapter = "coinbase"
segment = "advanced"
symbols = ["ETH-USD"]
channels = ["candles"]
"#;
        let cfg = DaemonConfig::from_toml_str(toml).expect("cfg");
        let out = format_catalog(&cfg, "coinbase-adv", false).unwrap();
        assert!(out.contains("instruments=1"), "{out}");
        assert!(out.contains("ETH-USD"), "{out}");
        assert!(out.contains("venue_code=coinbase-adv"), "{out}");
        assert!(out.contains("source=stub"), "{out}");
        // Daemon stub catalog → factory session_config_from_catalog product list.
        let (_, catalog) = catalog_for_config_venue(cfg.venues.first().unwrap()).unwrap();
        let cfg_sess = marketfeed_adapter_coinbase::adv_session_config_from_catalog(
            &catalog,
            false,
            vec![marketfeed_adapter_api::CandleInterval::M1],
        );
        assert_eq!(cfg_sess.products, vec!["ETH-USD".to_string()]);
    }

    #[test]
    fn plan_lists_sessions() {
        let cfg = synthetic_cfg();
        let out = format_plan(&cfg).unwrap();
        assert!(out.contains("venues=1"), "{out}");
        assert!(out.contains("sessions=1"), "{out}");
        assert!(out.contains("endpoint="), "{out}");
    }

    #[test]
    fn plan_lists_configured_subscription_count() {
        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:0"

            [[venues]]
            id = "binance-spot"
            adapter = "binance"
            symbols = ["ETHUSDT", "BTCUSDT"]
            channels = ["trades", "l2"]
            "#,
        )
        .expect("config");

        let out = format_plan(&cfg).expect("plan");

        assert!(out.contains("subs=4"), "{out}");
    }

    #[test]
    fn benchmark_json_fixture() {
        let dir = std::env::temp_dir().join(format!(
            "mf-bench-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("fx.json");
        let mut f = std::fs::File::create(&path).unwrap();
        write!(f, r#"{{"e":"trade","p":"1.0"}}"#).unwrap();
        let out = format_benchmark(&path, 10).unwrap();
        assert!(out.contains("iterations=10"), "{out}");
        assert!(out.contains("ok=10"), "{out}");
        let _ = std::fs::remove_dir_all(dir);
    }
}
