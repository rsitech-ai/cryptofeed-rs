//! Config → venue session wiring (engine owns I/O; adapters stay pure).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures_util::stream::{FuturesUnordered, StreamExt};
use marketfeed_adapter_api::{
    ConcreteSubscriptionSet, ReconnectPolicy, SessionMachine, SessionSpec, VenueFactory,
};
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
    CoinbaseIntlFactory, CoinbaseSpotFactory,
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
use marketfeed_engine::{EngineSupervisor, SessionRunnerConfig};
use marketfeed_model::{
    AssetCode, CatalogVersion, CatalogView, Fixed, InstrumentDefinition, InstrumentId,
    InstrumentKey, InstrumentKind, InstrumentStatus, OverflowPolicy, SessionId, VenueCode, VenueId,
};
use marketfeed_transport::{
    MemoryWebSocket, ReqwestHttpTransport, TungsteniteWebSocket, WebSocketSpec,
};
use tokio::sync::watch;

use crate::config::{TransportMode, VenueConfig, VenueKind};
use crate::sinks::SharedDaemonSinks;
use crate::state::DaemonState;
use crate::subscriptions::expand_concrete_subscriptions;

static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);

struct ActivePublicVenueTask {
    state: Arc<DaemonState>,
}

impl Drop for ActivePublicVenueTask {
    fn drop(&mut self) {
        self.state
            .active_public_venue_tasks
            .fetch_sub(1, Ordering::AcqRel);
    }
}

fn next_session_id() -> SessionId {
    SessionId(NEXT_SESSION.fetch_add(1, Ordering::Relaxed))
}

/// Spawn one background task per configured venue (+ optional recording drain task).
pub fn spawn_venues(
    state: Arc<DaemonState>,
    shutdown: watch::Receiver<bool>,
) -> Vec<tokio::task::JoinHandle<Result<(), String>>> {
    let mut handles = Vec::new();
    if state.config.recording.raw.enabled {
        let state_rec = Arc::clone(&state);
        let shutdown_rec = shutdown.clone();
        handles.push(tokio::spawn(async move {
            let result = run_recording(Arc::clone(&state_rec), shutdown_rec).await;
            if let Err(e) = &result {
                state_rec.recording_healthy.store(false, Ordering::Relaxed);
                tracing::error!(error = %e, "recording pipeline exited");
            }
            result
        }));
    }
    for venue in state.config.venues.clone() {
        let Some(flag) = state.venue_flag(&venue.id) else {
            tracing::error!(id = %venue.id, "missing venue live flag");
            continue;
        };
        let Some(stop) = state.venue_stop(&venue.id) else {
            tracing::error!(id = %venue.id, "missing venue stop flag");
            continue;
        };
        let state = Arc::clone(&state);
        let shutdown = shutdown.clone();
        state
            .active_public_venue_tasks
            .fetch_add(1, Ordering::AcqRel);
        handles.push(tokio::spawn(async move {
            let _active_task = ActivePublicVenueTask {
                state: Arc::clone(&state),
            };
            let result = run_venue(Arc::clone(&state), venue, flag, stop, shutdown).await;
            if let Err(e) = &result {
                tracing::error!(error = %e, "venue session exited");
            }
            result
        }));
    }
    handles
}

async fn run_recording(
    state: Arc<DaemonState>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), String> {
    let pipeline = state
        .recording_pipeline
        .clone()
        .ok_or_else(|| "recording enabled without an initialized pipeline".to_string())?;
    let initial = pipeline.snapshot().map_err(|e| e.to_string())?;
    state.recording_healthy.store(true, Ordering::Relaxed);
    tracing::info!(dir = %initial.directory.display(), "recording pipeline ready");

    loop {
        if *shutdown.borrow() {
            break;
        }
        if state.recording_rotate.take_request() {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0);
            match pipeline.rotate_now(ts) {
                Ok(()) => {
                    let snapshot = pipeline.snapshot().map_err(|e| e.to_string())?;
                    tracing::info!(rotations = snapshot.rotations, "recording rotated");
                }
                Err(e) => {
                    state.recording_healthy.store(false, Ordering::Relaxed);
                    return Err(format!("recording rotate failed: {e}"));
                }
            }
        }
        pipeline.flush_pending(4096).map_err(|e| e.to_string())?;
        update_recording_metrics(&state, &pipeline)?;
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }

    state.shutdown_draining.store(true, Ordering::Relaxed);
    let deadline =
        Instant::now() + Duration::from_secs(state.config.engine.shutdown_deadline_secs.max(1));
    while state.active_public_venue_tasks.load(Ordering::Acquire) > 0 {
        if Instant::now() >= deadline {
            state.recording_healthy.store(false, Ordering::Relaxed);
            return Err(format!(
                "recording shutdown timed out waiting for {} public venue task(s)",
                state.active_public_venue_tasks.load(Ordering::Acquire)
            ));
        }
        pipeline.flush_pending(4096).map_err(|e| e.to_string())?;
        update_recording_metrics(&state, &pipeline)?;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let drain_error = match pipeline.shutdown_drain(deadline) {
        Ok(()) => {
            state.recording_healthy.store(true, Ordering::Relaxed);
            tracing::info!("recording pipeline drained");
            None
        }
        Err(e) => {
            state.recording_healthy.store(false, Ordering::Relaxed);
            tracing::error!(error = %e, "recording drain failed");
            Some(e.to_string())
        }
    };
    update_recording_metrics(&state, &pipeline)?;
    state.shutdown_draining.store(false, Ordering::Relaxed);
    if let Some(error) = drain_error {
        return Err(format!("recording drain failed: {error}"));
    }
    Ok(())
}

fn update_recording_metrics(
    state: &DaemonState,
    pipeline: &marketfeed_recording::RecordingHandle,
) -> Result<(), String> {
    let snapshot = pipeline.snapshot().map_err(|e| e.to_string())?;
    state
        .recording_queue_len
        .store(snapshot.queue_len as u64, Ordering::Relaxed);
    state
        .recording_rotations
        .store(snapshot.rotations, Ordering::Relaxed);
    state
        .recording_written
        .store(snapshot.records_written, Ordering::Relaxed);
    state
        .recording_dropped
        .store(snapshot.dropped_total, Ordering::Relaxed);
    state
        .disk_pressure
        .store(snapshot.disk_pressure, Ordering::Relaxed);
    for event in pipeline.take_overflow_events().map_err(|e| e.to_string())? {
        tracing::warn!(?event, "recording queue overflow");
    }
    Ok(())
}

async fn run_venue(
    state: Arc<DaemonState>,
    venue: VenueConfig,
    live_flag: Arc<AtomicBool>,
    stop_flag: Arc<AtomicBool>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), String> {
    let kind = venue.resolved_kind().map_err(|e| e.to_string())?;
    match kind {
        VenueKind::Synthetic => {
            run_synthetic_memory(&state, &venue, live_flag, stop_flag, &mut shutdown).await
        }
        VenueKind::BinanceSpot => {
            run_live_ws(
                &state,
                &venue,
                live_flag,
                stop_flag,
                &mut shutdown,
                BinanceSpotFactory {
                    enable_l2: venue.wants_l2(),
                },
                BINANCE_SPOT_VENUE_ID,
                "binance-spot",
                InstrumentKind::Spot,
            )
            .await
        }
        VenueKind::BinanceUsdm => {
            run_live_ws(
                &state,
                &venue,
                live_flag,
                stop_flag,
                &mut shutdown,
                BinanceUsdmFactory {
                    enable_l2: venue.wants_l2(),
                },
                BINANCE_USDM_VENUE_ID,
                "binance-usdm",
                InstrumentKind::PerpetualLinear,
            )
            .await
        }
        VenueKind::BinanceCoinm => {
            run_live_ws(
                &state,
                &venue,
                live_flag,
                stop_flag,
                &mut shutdown,
                BinanceCoinmFactory {
                    enable_l2: venue.wants_l2(),
                },
                BINANCE_COINM_VENUE_ID,
                "binance-coinm",
                InstrumentKind::PerpetualInverse,
            )
            .await
        }
        VenueKind::OkxSpot => {
            run_live_ws(
                &state,
                &venue,
                live_flag,
                stop_flag,
                &mut shutdown,
                OkxSpotFactory {
                    enable_l2: venue.wants_l2(),
                },
                OKX_SPOT_VENUE_ID,
                "okx-spot",
                InstrumentKind::Spot,
            )
            .await
        }
        VenueKind::OkxSwap => {
            run_live_ws(
                &state,
                &venue,
                live_flag,
                stop_flag,
                &mut shutdown,
                OkxSwapFactory {
                    enable_l2: venue.wants_l2(),
                },
                OKX_SWAP_VENUE_ID,
                "okx-swap",
                InstrumentKind::PerpetualLinear,
            )
            .await
        }
        VenueKind::OkxFutures => {
            run_live_ws(
                &state,
                &venue,
                live_flag,
                stop_flag,
                &mut shutdown,
                OkxFuturesFactory {
                    enable_l2: venue.wants_l2(),
                },
                OKX_FUTURES_VENUE_ID,
                "okx-futures",
                InstrumentKind::FutureLinear,
            )
            .await
        }
        VenueKind::BybitLinear => {
            run_live_ws(
                &state,
                &venue,
                live_flag,
                stop_flag,
                &mut shutdown,
                BybitFactory {
                    category: BybitCategory::Linear,
                    enable_l2: venue.wants_l2(),
                },
                BYBIT_LINEAR_VENUE_ID,
                "bybit-linear",
                InstrumentKind::PerpetualLinear,
            )
            .await
        }
        VenueKind::BybitSpot => {
            run_live_ws(
                &state,
                &venue,
                live_flag,
                stop_flag,
                &mut shutdown,
                BybitFactory {
                    category: BybitCategory::Spot,
                    enable_l2: venue.wants_l2(),
                },
                BYBIT_SPOT_VENUE_ID,
                "bybit-spot",
                InstrumentKind::Spot,
            )
            .await
        }
        VenueKind::BybitInverse => {
            run_live_ws(
                &state,
                &venue,
                live_flag,
                stop_flag,
                &mut shutdown,
                BybitFactory {
                    category: BybitCategory::Inverse,
                    enable_l2: venue.wants_l2(),
                },
                BYBIT_INVERSE_VENUE_ID,
                "bybit-inverse",
                InstrumentKind::PerpetualInverse,
            )
            .await
        }
        VenueKind::KrakenSpot => {
            run_live_ws(
                &state,
                &venue,
                live_flag,
                stop_flag,
                &mut shutdown,
                KrakenSpotFactory {
                    enable_l2: venue.wants_l2(),
                },
                KRAKEN_SPOT_VENUE_ID,
                "kraken-spot",
                InstrumentKind::Spot,
            )
            .await
        }
        VenueKind::KrakenFutures => {
            run_live_ws(
                &state,
                &venue,
                live_flag,
                stop_flag,
                &mut shutdown,
                KrakenFuturesFactory {
                    enable_l2: venue.wants_l2(),
                },
                KRAKEN_FUTURES_VENUE_ID,
                "kraken-futures",
                InstrumentKind::PerpetualLinear,
            )
            .await
        }
        VenueKind::Deribit => {
            run_live_ws(
                &state,
                &venue,
                live_flag,
                stop_flag,
                &mut shutdown,
                DeribitFactory {
                    enable_l2: venue.wants_l2(),
                },
                DERIBIT_VENUE_ID,
                "deribit",
                InstrumentKind::PerpetualInverse,
            )
            .await
        }
        VenueKind::Bitstamp => {
            run_live_ws(
                &state,
                &venue,
                live_flag,
                stop_flag,
                &mut shutdown,
                BitstampFactory {
                    enable_l2: venue.wants_l2(),
                },
                BITSTAMP_VENUE_ID,
                "bitstamp",
                InstrumentKind::Spot,
            )
            .await
        }
        VenueKind::Gemini => {
            run_live_ws(
                &state,
                &venue,
                live_flag,
                stop_flag,
                &mut shutdown,
                GeminiFactory {
                    enable_l2: venue.wants_l2(),
                    ..Default::default()
                },
                GEMINI_VENUE_ID,
                "gemini",
                InstrumentKind::Spot,
            )
            .await
        }
        VenueKind::CoinbaseSpot => {
            run_live_ws(
                &state,
                &venue,
                live_flag,
                stop_flag,
                &mut shutdown,
                CoinbaseSpotFactory {
                    enable_l2: venue.wants_l2(),
                    credentials: None,
                },
                COINBASE_SPOT_VENUE_ID,
                "coinbase-spot",
                InstrumentKind::Spot,
            )
            .await
        }
        VenueKind::CoinbaseAdvanced => {
            run_live_ws(
                &state,
                &venue,
                live_flag,
                stop_flag,
                &mut shutdown,
                CoinbaseAdvFactory {
                    enable_l2: venue.wants_l2(),
                },
                COINBASE_ADV_VENUE_ID,
                "coinbase-adv",
                InstrumentKind::Spot,
            )
            .await
        }
        VenueKind::CoinbaseIntl => {
            run_live_ws(
                &state,
                &venue,
                live_flag,
                stop_flag,
                &mut shutdown,
                CoinbaseIntlFactory {
                    enable_l2: venue.wants_l2(),
                    credentials: None,
                },
                COINBASE_INTL_VENUE_ID,
                "coinbase-intl",
                InstrumentKind::PerpetualLinear,
            )
            .await
        }
        VenueKind::Bitfinex => {
            run_live_ws(
                &state,
                &venue,
                live_flag,
                stop_flag,
                &mut shutdown,
                BitfinexFactory {
                    enable_l2: venue.wants_l2(),
                },
                BITFINEX_VENUE_ID,
                "bitfinex",
                InstrumentKind::Spot,
            )
            .await
        }
        VenueKind::BitfinexDeriv => {
            run_live_ws(
                &state,
                &venue,
                live_flag,
                stop_flag,
                &mut shutdown,
                BitfinexDerivFactory {
                    enable_l2: venue.wants_l2(),
                },
                BITFINEX_DERIV_VENUE_ID,
                "bitfinex-deriv",
                InstrumentKind::PerpetualLinear,
            )
            .await
        }
    }
}

/// Build a stub catalog so `VenueFactory::plan` venue_id checks pass.
///
/// # ponytail
/// Live venues without catalog-driven session config still hardcode symbols inside
/// their factories (OKX/Bybit/Kraken/Deribit). Ceiling: config `symbols` matter
/// for Binance + bitstamp/gemini/coinbase-spot/coinbase-adv/bitfinex/bitfinex-deriv
/// (+ peers with `session_config_from_catalog`). Upgrade: remaining factories.
pub(crate) fn catalog_for_venue(
    venue_id: VenueId,
    venue_code: &str,
    kind: InstrumentKind,
    symbols: &[String],
) -> CatalogView {
    if symbols.is_empty() {
        return CatalogView::new(venue_id, CatalogVersion(1));
    }
    let instruments: Vec<_> = symbols
        .iter()
        .enumerate()
        .map(|(i, sym)| {
            let def = InstrumentDefinition {
                key: InstrumentKey {
                    venue: VenueCode(venue_code.into()),
                    native_symbol: sym.clone(),
                    kind,
                    settlement: None,
                    expiry_ns: None,
                },
                base: AssetCode("BASE".into()),
                quote: AssetCode("USDT".into()),
                settlement: None,
                price_scale: 2,
                quantity_scale: 8,
                price_increment: Fixed::new(1, 2),
                quantity_increment: Fixed::new(1, 8),
                min_quantity: None,
                max_quantity: None,
                min_notional: None,
                contract_size: None,
                expiry_ns: None,
                status: InstrumentStatus::Active,
                inverse: matches!(
                    kind,
                    InstrumentKind::PerpetualInverse | InstrumentKind::FutureInverse
                ),
            };
            def.into_instrument(InstrumentId((i as u32) + 1), CatalogVersion(1))
        })
        .collect();
    CatalogView::with_instruments(
        venue_id,
        CatalogVersion(1),
        std::sync::Arc::from(instruments),
    )
}

async fn run_synthetic_memory(
    state: &DaemonState,
    venue: &VenueConfig,
    live_flag: Arc<AtomicBool>,
    stop_flag: Arc<AtomicBool>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), String> {
    if venue.resolved_transport() == TransportMode::Live {
        return Err(format!(
            "venue {}: synthetic only supports transport=memory",
            venue.id
        ));
    }

    let factory = SyntheticFactory;
    let catalog = CatalogView::new(SYNTHETIC_VENUE_ID, CatalogVersion(1));
    let plan = factory
        .plan(&ConcreteSubscriptionSet::default(), &catalog)
        .map_err(|e| e.to_string())?;
    let spec = plan
        .into_iter()
        .next()
        .ok_or_else(|| "synthetic plan empty".to_string())?;
    let session = next_session_id();
    if let Some(pipeline) = &state.recording_pipeline {
        let mut recording_spec = spec.clone();
        recording_spec.endpoint_name = "memory://synthetic".into();
        pipeline
            .register_metadata(marketfeed_recording::MetadataRecord::Session(
                marketfeed_recording::SessionRecordingMetadata::from_plan(
                    session,
                    SYNTHETIC_VENUE_ID,
                    &venue.adapter,
                    "test",
                    &recording_spec,
                    &catalog,
                ),
            ))
            .map_err(|e| e.to_string())?;
    }
    let machine = factory
        .create_session(spec, catalog)
        .map_err(|e| e.to_string())?;

    let mut supervisor = EngineSupervisor::new();
    supervisor.mark_running();
    supervisor
        .insert_session(
            machine,
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session,
                record: state.config.recording.raw.enabled,
                recording_pipeline: state.recording_pipeline.clone(),
                overflow: OverflowPolicy::FailEngine,
                mirror_capacity: 0,
                live_signal: Some(Arc::clone(&live_flag)),
                stop_signal: Some(Arc::clone(&stop_flag)),
                metrics: state.venue_metrics(&venue.id),
                ..SessionRunnerConfig::default()
            },
        )
        .map_err(|e| e.to_string())?;

    let mut ws = MemoryWebSocket::new();
    ws.push_text(b"SUB BTC-USD".to_vec());
    ws.push_text(b"BOOK_SNAP 1 BID 100.00:1.000 ASK 101.00:1.000".to_vec());

    supervisor
        .drain_memory_ws(
            session,
            &mut ws,
            &WebSocketSpec {
                url: "memory://synthetic".into(),
                ..WebSocketSpec::default()
            },
            1,
        )
        .await
        .map_err(|e| e.to_string())?;

    // Forward into configured sinks, or drop when none (FailEngine-safe drain).
    {
        let runner = supervisor.session_mut(session).map_err(|e| e.to_string())?;
        let mut shared = SharedDaemonSinks::new(Arc::clone(&state.sinks));
        runner
            .consume_dispatch(Some(&mut shared))
            .map_err(|e| e.to_string())?;
    }

    if !live_flag.load(Ordering::Relaxed) {
        return Err(format!("venue {}: synthetic did not reach Live", venue.id));
    }
    tracing::info!(id = %venue.id, "synthetic venue live (memory)");

    while !*shutdown.borrow() && !stop_flag.load(Ordering::Relaxed) {
        if shutdown.changed().await.is_err() {
            break;
        }
    }
    stop_flag.store(true, Ordering::Relaxed);
    live_flag.store(false, Ordering::Relaxed);
    supervisor.begin_shutdown().map_err(|e| e.to_string())?;
    {
        let runner = supervisor.session_mut(session).map_err(|e| e.to_string())?;
        let mut shared = SharedDaemonSinks::new(Arc::clone(&state.sinks));
        runner
            .consume_dispatch(Some(&mut shared))
            .map_err(|e| e.to_string())?;
    }
    {
        let mut shared = SharedDaemonSinks::new(Arc::clone(&state.sinks));
        supervisor
            .finish_shutdown_to(Some(&mut shared))
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

async fn run_live_ws<F: VenueFactory>(
    state: &DaemonState,
    venue: &VenueConfig,
    live_flag: Arc<AtomicBool>,
    stop_flag: Arc<AtomicBool>,
    shutdown: &mut watch::Receiver<bool>,
    factory: F,
    venue_id: VenueId,
    venue_code: &str,
    kind: InstrumentKind,
) -> Result<(), String> {
    if venue.resolved_transport() != TransportMode::Live {
        return Err(format!(
            "venue {}: requires transport=live (got {:?})",
            venue.id,
            venue.resolved_transport()
        ));
    }

    let max_frame_bytes = factory.specification().max_frame_bytes;
    let catalog = catalog_for_venue(venue_id, venue_code, kind, &venue.symbols);
    let request = expand_concrete_subscriptions(venue, &catalog).map_err(|e| e.to_string())?;
    let plan = factory
        .plan(&request, &catalog)
        .map_err(|e| e.to_string())?;
    let session_specs = require_session_specs(&venue.id, plan)?;
    let mut prepared = Vec::with_capacity(session_specs.len());
    for session_spec in session_specs {
        let session = next_session_id();
        let url = session_spec.endpoint_name.clone();
        if let Some(pipeline) = &state.recording_pipeline {
            pipeline
                .register_metadata(marketfeed_recording::MetadataRecord::Session(
                    marketfeed_recording::SessionRecordingMetadata::from_plan(
                        session,
                        venue_id,
                        &venue.adapter,
                        "production",
                        &session_spec,
                        &catalog,
                    ),
                ))
                .map_err(|e| e.to_string())?;
        }
        let machine = factory
            .create_session(session_spec, catalog.clone())
            .map_err(|e| e.to_string())?;
        prepared.push((session, machine, url, Arc::new(AtomicBool::new(false))));
    }
    let session_live_flags: Vec<_> = prepared
        .iter()
        .map(|(_, _, _, flag)| Arc::clone(flag))
        .collect();
    let runs = prepared
        .into_iter()
        .map(|(session, machine, url, session_live)| {
            run_live_ws_session(
                state,
                venue,
                venue_id,
                session,
                machine,
                url,
                max_frame_bytes,
                session_live,
                Arc::clone(&stop_flag),
                shutdown.clone(),
            )
        });
    let all_sessions = drain_session_runs(
        runs,
        Arc::clone(&stop_flag),
        Duration::from_secs(state.config.engine.shutdown_deadline_secs.max(1)),
    );
    tokio::pin!(all_sessions);
    let mut live_tick = tokio::time::interval(Duration::from_millis(50));

    loop {
        tokio::select! {
            result = &mut all_sessions => {
                live_flag.store(false, Ordering::Relaxed);
                if result.is_err() {
                    stop_flag.store(true, Ordering::Relaxed);
                }
                return result.map(|_| ());
            }
            _ = live_tick.tick() => {
                live_flag.store(all_sessions_live(&session_live_flags), Ordering::Relaxed);
            }
        }
    }
}

fn all_sessions_live(flags: &[Arc<AtomicBool>]) -> bool {
    !flags.is_empty() && flags.iter().all(|flag| flag.load(Ordering::Relaxed))
}

async fn drain_session_runs<I, F>(
    runs: I,
    stop_flag: Arc<AtomicBool>,
    failure_drain_deadline: Duration,
) -> Result<(), String>
where
    I: IntoIterator<Item = F>,
    F: std::future::Future<Output = Result<(), String>>,
{
    let mut pending: FuturesUnordered<F> = runs.into_iter().collect();
    let mut first_error = None;
    let mut drain_deadline = None;

    while !pending.is_empty() {
        let next = if let Some(deadline) = drain_deadline {
            match tokio::time::timeout_at(deadline, pending.next()).await {
                Ok(result) => result,
                Err(_) => {
                    let first = first_error.expect("drain deadline requires a session error");
                    return Err(format!("{first}; sibling session drain deadline exceeded"));
                }
            }
        } else {
            pending.next().await
        };

        let Some(result) = next else {
            break;
        };
        if let Err(error) = result {
            if first_error.is_none() {
                first_error = Some(error);
                stop_flag.store(true, Ordering::Relaxed);
                drain_deadline = Some(tokio::time::Instant::now() + failure_drain_deadline);
            }
        }
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn require_session_specs(
    venue_id: &str,
    plan: Vec<SessionSpec>,
) -> Result<Vec<SessionSpec>, String> {
    if plan.is_empty() {
        Err(format!("venue {venue_id}: empty plan"))
    } else {
        Ok(plan)
    }
}

fn live_websocket_spec(url: String, max_frame_bytes: usize) -> WebSocketSpec {
    WebSocketSpec {
        url,
        max_frame_bytes,
        ..WebSocketSpec::default()
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_live_ws_session(
    state: &DaemonState,
    venue: &VenueConfig,
    venue_id: VenueId,
    session: SessionId,
    machine: Box<dyn SessionMachine>,
    url: String,
    max_frame_bytes: usize,
    session_live: Arc<AtomicBool>,
    stop_flag: Arc<AtomicBool>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), String> {
    let mut supervisor = EngineSupervisor::new();
    supervisor.mark_running();
    supervisor
        .insert_session(
            machine,
            SessionRunnerConfig {
                venue: venue_id,
                session,
                record: state.config.recording.raw.enabled,
                recording_pipeline: state.recording_pipeline.clone(),
                overflow: OverflowPolicy::FailEngine,
                mirror_capacity: 0,
                live_signal: Some(Arc::clone(&session_live)),
                stop_signal: Some(Arc::clone(&stop_flag)),
                metrics: state.venue_metrics(&venue.id),
                ..SessionRunnerConfig::default()
            },
        )
        .map_err(|e| e.to_string())?;

    let mut ws = TungsteniteWebSocket::new();
    let http = ReqwestHttpTransport::new().map_err(|e| e.to_string())?;
    let spec = live_websocket_spec(url, max_frame_bytes);
    let policy = ReconnectPolicy {
        min_delay_ms: 200,
        max_delay_ms: 30_000,
        reset_after_live_ms: 60_000,
    };

    tracing::info!(
        id = %venue.id,
        session = session.0,
        adapter = %venue.adapter,
        symbols = ?venue.symbols,
        channels = ?venue.channels,
        "starting live venue session"
    );

    // Always forward: empty DaemonSinks is a null-sink (drops; FailEngine-safe).
    // Concrete SharedDaemonSinks (not dyn) so the live loop stays Send.
    let mut shared = SharedDaemonSinks::new(Arc::clone(&state.sinks));
    let run = supervisor.run_session_loop_to(
        session,
        &mut ws,
        &http,
        &spec,
        policy,
        u32::MAX,
        Some(&mut shared),
    );
    tokio::pin!(run);
    tokio::select! {
        res = &mut run => {
            session_live.store(false, Ordering::Relaxed);
            res.map_err(|e| e.to_string())?;
        }
        _ = shutdown.changed() => {
            tracing::info!(id = %venue.id, "shutdown: signaling venue session stop");
            stop_flag.store(true, Ordering::Relaxed);
            state.shutdown_draining.store(true, Ordering::Relaxed);
            let deadline = Duration::from_secs(state.config.engine.shutdown_deadline_secs.max(1));
            let shutdown_result = match tokio::time::timeout(deadline, run).await {
                Ok(Ok(())) => {
                    tracing::info!(id = %venue.id, "venue session stopped cleanly");
                    Ok(())
                }
                Ok(Err(e)) => Err(format!("venue {} session stop error: {e}", venue.id)),
                Err(_) => Err(format!("venue {} session stop deadline exceeded", venue.id)),
            };
            session_live.store(false, Ordering::Relaxed);
            shutdown_result?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_websocket_spec_preserves_venue_frame_limit() {
        let spec = live_websocket_spec("wss://example.test".into(), 8 * 1024 * 1024);

        assert_eq!(spec.url, "wss://example.test");
        assert_eq!(spec.max_frame_bytes, 8 * 1024 * 1024);
    }

    #[test]
    fn planned_session_specs_preserve_every_factory_plan_entry() {
        let plan = vec![
            SessionSpec {
                endpoint_name: "wss://example.test/public".into(),
                subscriptions: ConcreteSubscriptionSet::default(),
            },
            SessionSpec {
                endpoint_name: "wss://example.test/business".into(),
                subscriptions: ConcreteSubscriptionSet::default(),
            },
        ];

        let sessions = require_session_specs("venue", plan).expect("nonempty plan");

        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].endpoint_name, "wss://example.test/public");
        assert_eq!(sessions[1].endpoint_name, "wss://example.test/business");
    }

    #[test]
    fn venue_is_live_only_after_every_planned_session_is_live() {
        let public = Arc::new(AtomicBool::new(true));
        let business = Arc::new(AtomicBool::new(false));
        let flags = vec![Arc::clone(&public), Arc::clone(&business)];

        assert!(!all_sessions_live(&flags));
        business.store(true, Ordering::Relaxed);
        assert!(all_sessions_live(&flags));
        public.store(false, Ordering::Relaxed);
        assert!(!all_sessions_live(&flags));
        assert!(!all_sessions_live(&[]));
    }

    #[tokio::test]
    async fn first_session_error_signals_stop_and_drains_sibling_sessions() {
        let stop = Arc::new(AtomicBool::new(false));
        let sibling_completed = Arc::new(AtomicBool::new(false));

        let failing = async { Err::<(), String>("public session failed".into()) };
        let sibling_stop = Arc::clone(&stop);
        let completed = Arc::clone(&sibling_completed);
        let sibling = async move {
            while !sibling_stop.load(Ordering::Relaxed) {
                tokio::task::yield_now().await;
            }
            completed.store(true, Ordering::Relaxed);
            Ok(())
        };

        let runs: Vec<
            std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>,
        > = vec![Box::pin(failing), Box::pin(sibling)];
        let result = drain_session_runs(runs, Arc::clone(&stop), Duration::from_secs(1)).await;

        assert_eq!(result, Err("public session failed".into()));
        assert!(stop.load(Ordering::Relaxed));
        assert!(sibling_completed.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn session_failure_drain_is_bounded_by_shutdown_deadline() {
        let stop = Arc::new(AtomicBool::new(false));
        let runs: Vec<
            std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>,
        > = vec![
            Box::pin(async { Err("public session failed".into()) }),
            Box::pin(std::future::pending()),
        ];

        let result = drain_session_runs(runs, Arc::clone(&stop), Duration::from_millis(10)).await;

        assert_eq!(
            result,
            Err("public session failed; sibling session drain deadline exceeded".into())
        );
        assert!(stop.load(Ordering::Relaxed));
    }
}
