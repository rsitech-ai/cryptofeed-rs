//! `marketfeed` CLI — validate / run / replay / inspect-recording / version.
//!
//! Installs the tracing subscriber here only (never in library crates).

use std::env;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use marketfeed_daemon::cli::{format_benchmark, format_catalog, format_catalog_live, format_plan};
use marketfeed_daemon::{
    DaemonConfig, DaemonState, classify_reload, serve, spawn_private_sessions, spawn_venues,
};
use marketfeed_recording::RawSegmentReader;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;
use tracing_subscriber::reload;

type DaemonTaskHandle = tokio::task::JoinHandle<Result<(), String>>;

async fn wait_for_shutdown_or_task_exit<F>(
    handles: &mut Vec<DaemonTaskHandle>,
    shutdown: F,
    state: Option<Arc<DaemonState>>,
) -> Option<String>
where
    F: Future<Output = ()>,
{
    tokio::pin!(shutdown);
    loop {
        if let Some(index) = handles.iter().position(DaemonTaskHandle::is_finished) {
            let result = handles.swap_remove(index).await;
            let error = match result {
                Ok(Ok(())) => "daemon task exited unexpectedly".to_string(),
                Ok(Err(error)) => format!("daemon task failed: {error}"),
                Err(error) => format!("daemon task join failure: {error}"),
            };
            return Some(error);
        }
        if let Some(state) = state.as_ref() {
            let failed = state
                .sinks
                .lock()
                .expect("sinks lock")
                .snapshots()
                .into_iter()
                .find(|sink| sink.required && !sink.healthy);
            if let Some(sink) = failed {
                return Some(format!(
                    "required sink {} ({}) failed: {}",
                    sink.id,
                    sink.kind,
                    sink.last_error.as_deref().unwrap_or("unknown error")
                ));
            }
        }
        tokio::select! {
            _ = &mut shutdown => return None,
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
}

fn usage() -> ! {
    eprintln!(
        "usage:
  marketfeed validate --config <path>
  marketfeed catalog --config <path> --venue <id> [--live]
  marketfeed plan --config <path>
  marketfeed run --config <path>
  marketfeed replay --input <segment>
  marketfeed inspect-recording --input <segment>
  marketfeed benchmark --fixture <path> [--iterations <n>]
  marketfeed version
  marketfeed --help"
    );
    std::process::exit(2);
}

/// Install tracing; returns a reload handle for §21.4 log-level hot reload.
fn install_tracing(cfg: &DaemonConfig) -> reload::Handle<EnvFilter, tracing_subscriber::Registry> {
    let filter =
        EnvFilter::try_new(&cfg.telemetry.log_level).unwrap_or_else(|_| EnvFilter::new("info"));
    let (filter_layer, handle) = reload::Layer::new(filter);
    match cfg.telemetry.log_format.as_str() {
        "text" => {
            tracing_subscriber::registry()
                .with(filter_layer)
                .with(tracing_subscriber::fmt::layer())
                .init();
        }
        _ => {
            tracing_subscriber::registry()
                .with(filter_layer)
                .with(tracing_subscriber::fmt::layer().json())
                .init();
        }
    }
    handle
}

fn apply_reload_plan(
    state: &DaemonState,
    plan: &marketfeed_daemon::ReloadPlan,
    filter: &reload::Handle<EnvFilter, tracing_subscriber::Registry>,
) {
    if let Some(level) = &plan.apply_log_level {
        match EnvFilter::try_new(level) {
            Ok(next) => match filter.reload(next) {
                Ok(()) => {
                    state.reloadable.lock().expect("reloadable lock").log_level = level.clone();
                    tracing::info!(%level, "config reload: applied telemetry.log_level");
                }
                Err(e) => tracing::error!(error = %e, "config reload: log filter reload failed"),
            },
            Err(e) => tracing::error!(error = %e, %level, "config reload: invalid log_level"),
        }
    }
    if let Some(readiness) = &plan.apply_readiness {
        state.reloadable.lock().expect("reloadable lock").readiness = readiness.clone();
        tracing::info!("config reload: applied readiness policy");
    }
    for key in &plan.restart_required {
        tracing::warn!(key, "config reload: restart required");
    }
    if plan.is_noop() {
        tracing::info!("config reload: validated, no changes");
    }
}

fn handle_config_reload(
    path: &Path,
    state: &DaemonState,
    filter: &reload::Handle<EnvFilter, tracing_subscriber::Registry>,
) {
    let new = match DaemonConfig::load_path(path) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(
                error = %e,
                path = %path.display(),
                "config reload: validation failed; keeping running config"
            );
            return;
        }
    };
    let applied = state.reloadable.lock().expect("reloadable lock").clone();
    let plan = classify_reload(&state.config, &applied, &new);
    apply_reload_plan(state, &plan, filter);
}

fn parse_flag(args: &[String], name: &str) -> Option<PathBuf> {
    let mut i = 0;
    while i < args.len() {
        if args[i] == name {
            return Some(PathBuf::from(args.get(i + 1)?));
        }
        i += 1;
    }
    None
}

fn parse_flag_str(args: &[String], name: &str) -> Option<String> {
    let mut i = 0;
    while i < args.len() {
        if args[i] == name {
            return args.get(i + 1).cloned();
        }
        i += 1;
    }
    None
}

fn parse_config_arg(args: &[String]) -> PathBuf {
    parse_flag(args, "--config").unwrap_or_else(|| usage())
}

fn parse_fixture_arg(args: &[String]) -> PathBuf {
    parse_flag(args, "--fixture").unwrap_or_else(|| usage())
}

fn parse_iterations(args: &[String]) -> u32 {
    parse_flag_str(args, "--iterations")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000)
}

fn parse_input_arg(args: &[String]) -> PathBuf {
    parse_flag(args, "--input").unwrap_or_else(|| usage())
}

fn inspect_recording(path: &PathBuf) -> Result<(), String> {
    let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
    let len = bytes.len();
    let mut reader = RawSegmentReader::from_bytes(bytes).map_err(|e| e.to_string())?;
    let start = reader.start_ts_ns;
    let records = reader.read_all().map_err(|e| e.to_string())?;
    let inbound = records
        .iter()
        .filter(|r| r.header.direction == marketfeed_recording::Direction::Inbound)
        .count();
    let outbound = records.len() - inbound;
    println!("path={}", path.display());
    println!("bytes={len}");
    println!("start_ts_ns={start}");
    println!("records={}", records.len());
    println!("inbound={inbound}");
    println!("outbound={outbound}");
    if let Some(first) = records.first() {
        println!(
            "first_frame_seq={} opcode={:?}",
            first.header.frame_seq, first.header.opcode
        );
    }
    if let Some(last) = records.last() {
        println!(
            "last_frame_seq={} opcode={:?}",
            last.header.frame_seq, last.header.opcode
        );
    }
    Ok(())
}

fn replay_recording(path: &PathBuf) -> Result<(), String> {
    // Fast-as-possible raw scan (adapter replay needs a venue machine; this CLI
    // validates the segment and reports frame counts for ops/offline checks).
    let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
    let mut reader = RawSegmentReader::from_bytes(bytes).map_err(|e| e.to_string())?;
    let records = reader.read_all().map_err(|e| e.to_string())?;
    let mut frames = 0u64;
    for rec in &records {
        if rec.header.direction == marketfeed_recording::Direction::Inbound {
            frames += 1;
        }
    }
    println!(
        "replay_ok path={} inbound_frames={frames} records={}",
        path.display(),
        records.len()
    );
    Ok(())
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() {
        usage();
    }
    match args[0].as_str() {
        "help" | "--help" | "-h" => {
            eprintln!(
                "usage:
  marketfeed validate --config <path>
  marketfeed catalog --config <path> --venue <id> [--live]
  marketfeed plan --config <path>
  marketfeed run --config <path>
  marketfeed replay --input <segment>
  marketfeed inspect-recording --input <segment>
  marketfeed benchmark --fixture <path> [--iterations <n>]
  marketfeed version
  marketfeed --help"
            );
        }
        "version" => println!("marketfeed {}", env!("CARGO_PKG_VERSION")),
        "catalog" => {
            let rest = &args[1..];
            let path = parse_config_arg(rest);
            let venue = parse_flag_str(rest, "--venue").unwrap_or_else(|| usage());
            let live = rest.iter().any(|a| a == "--live");
            let cfg = match DaemonConfig::load_path(&path) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("invalid config: {e}");
                    std::process::exit(1);
                }
            };
            let result = if live {
                format_catalog_live(&cfg, &venue).await
            } else {
                format_catalog(&cfg, &venue, false)
            };
            match result {
                Ok(out) => print!("{out}"),
                Err(e) => {
                    eprintln!("catalog failed: {e}");
                    std::process::exit(1);
                }
            }
        }
        "plan" => {
            let path = parse_config_arg(&args[1..]);
            let cfg = match DaemonConfig::load_path(&path) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("invalid config: {e}");
                    std::process::exit(1);
                }
            };
            match format_plan(&cfg) {
                Ok(out) => print!("{out}"),
                Err(e) => {
                    eprintln!("plan failed: {e}");
                    std::process::exit(1);
                }
            }
        }
        "benchmark" => {
            let rest = &args[1..];
            let path = parse_fixture_arg(rest);
            let iters = parse_iterations(rest);
            match format_benchmark(&path, iters) {
                Ok(out) => println!("{out}"),
                Err(e) => {
                    eprintln!("benchmark failed: {e}");
                    std::process::exit(1);
                }
            }
        }
        "validate" => {
            let path = parse_config_arg(&args[1..]);
            match DaemonConfig::load_path(&path) {
                Ok(_) => println!("ok: {}", path.display()),
                Err(e) => {
                    eprintln!("invalid config: {e}");
                    std::process::exit(1);
                }
            }
        }
        "inspect-recording" => {
            let path = parse_input_arg(&args[1..]);
            if let Err(e) = inspect_recording(&path) {
                eprintln!("inspect-recording failed: {e}");
                std::process::exit(1);
            }
        }
        "replay" => {
            let path = parse_input_arg(&args[1..]);
            if let Err(e) = replay_recording(&path) {
                eprintln!("replay failed: {e}");
                std::process::exit(1);
            }
        }
        "run" => {
            let path = parse_config_arg(&args[1..]);
            let cfg = match DaemonConfig::load_path(&path) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("invalid config: {e}");
                    std::process::exit(1);
                }
            };
            let filter_reload = install_tracing(&cfg);
            if let Some(profile) =
                marketfeed_engine::RuntimeProfile::parse(&cfg.engine.runtime_profile)
            {
                marketfeed_engine::apply_runtime_profile(profile);
            }
            tracing::info!(
                config = %path.display(),
                bind = %cfg.telemetry.bind,
                ui_bind = ?cfg.telemetry.ui_bind,
                venues = cfg.venues.len(),
                private_binance_spot = cfg.private.binance_spot.enabled,
                private_okx_spot = cfg.private.okx_spot.enabled,
                private_bybit_spot = cfg.private.bybit_spot.enabled,
                recording = cfg.recording.raw.enabled,
                runtime_profile = %cfg.engine.runtime_profile,
                "marketfeed daemon starting"
            );

            let state = DaemonState::try_new(cfg.clone()).unwrap_or_else(|e| {
                tracing::error!(error = %e, "daemon runtime initialization failed");
                std::process::exit(1);
            });
            state.mark_supervisor_running();
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let mut venue_handles = spawn_venues(Arc::clone(&state), shutdown_rx.clone());
            venue_handles.extend(spawn_private_sessions(&cfg, shutdown_rx));

            let addr = cfg.bind_addr().expect("validated bind");
            let listener = TcpListener::bind(addr).await.unwrap_or_else(|e| {
                tracing::error!(error = %e, %addr, "bind failed");
                std::process::exit(1);
            });
            tracing::info!(%addr, "health endpoints listening");
            let serve_state = Arc::clone(&state);
            tokio::spawn(async move {
                if let Err(e) = serve(listener, serve_state).await {
                    tracing::error!(error = %e, "health server exited");
                }
            });

            #[cfg(feature = "ui-api")]
            {
                if let Some(ui_addr) = cfg.ui_bind_addr().expect("validated ui_bind") {
                    if ui_addr != addr {
                        let ui_listener = TcpListener::bind(ui_addr).await.unwrap_or_else(|e| {
                            tracing::error!(error = %e, %ui_addr, "ui_bind failed");
                            std::process::exit(1);
                        });
                        tracing::info!(%ui_addr, "view API listening");
                        let ui_state = Arc::clone(&state);
                        tokio::spawn(async move {
                            if let Err(e) =
                                marketfeed_daemon::serve_view(ui_listener, ui_state).await
                            {
                                tracing::error!(error = %e, "view server exited");
                            }
                        });
                    } else {
                        tracing::info!(%addr, "view API sharing telemetry.bind");
                    }
                } else {
                    tracing::info!(%addr, "view API enabled on telemetry.bind (/v1/*)");
                }
            }
            // §21.4: SIGHUP re-validates TOML; applies log_level/readiness; else restart required.
            let reload_state = Arc::clone(&state);
            let reload_path = path.clone();
            let reload_filter = filter_reload.clone();
            tokio::spawn(async move {
                config_reload_loop(reload_path, reload_state, reload_filter).await;
            });

            let trigger_error = wait_for_shutdown_or_task_exit(
                &mut venue_handles,
                shutdown_signal(),
                Some(Arc::clone(&state)),
            )
            .await;
            if let Some(error) = &trigger_error {
                tracing::error!(%error, "runtime task exited; initiating coordinated shutdown");
            } else {
                tracing::info!("shutdown signal received");
            }
            tracing::info!(
                deadline_secs = cfg.engine.shutdown_deadline_secs,
                "coordinated shutdown initiated"
            );
            state
                .shutdown_draining
                .store(true, std::sync::atomic::Ordering::Relaxed);
            state.request_all_stops();
            let _ = shutdown_tx.send(true);
            let deadline = Duration::from_secs(cfg.engine.shutdown_deadline_secs.max(1));
            let shutdown_started = std::time::Instant::now();
            let join = async move {
                let mut errors = trigger_error.into_iter().collect::<Vec<_>>();
                for h in venue_handles {
                    match h.await {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => errors.push(error),
                        Err(error) => errors.push(format!("task join failure: {error}")),
                    }
                }
                if errors.is_empty() {
                    Ok(())
                } else {
                    Err(errors.join("; "))
                }
            };
            // The recorder owns the configured drain deadline. Give the outer
            // coordinator a small scheduling margin so it can observe and
            // report the recorder's result instead of aborting it at the same instant.
            let outer_deadline = deadline.saturating_add(Duration::from_secs(5));
            let mut shutdown_error = match tokio::time::timeout(outer_deadline, join).await {
                Ok(Ok(())) => {
                    tracing::info!("all daemon tasks joined cleanly");
                    None
                }
                Ok(Err(error)) => {
                    tracing::error!(%error, "one or more daemon tasks failed");
                    Some(error)
                }
                Err(_) => {
                    let error = "shutdown deadline exceeded waiting for daemon tasks".to_string();
                    tracing::error!(%error);
                    Some(error)
                }
            };
            let sink_deadline = shutdown_started + outer_deadline;
            let sink_result = state
                .sinks
                .lock()
                .expect("sinks lock")
                .shutdown(sink_deadline);
            match sink_result {
                Ok(()) => tracing::info!("all sink workers drained cleanly"),
                Err(error) => {
                    tracing::error!(%error, "sink worker drain failed");
                    shutdown_error = Some(match shutdown_error {
                        Some(existing) => format!("{existing}; {error}"),
                        None => error,
                    });
                }
            }
            state
                .process_live
                .store(false, std::sync::atomic::Ordering::Relaxed);
            state
                .shutdown_draining
                .store(false, std::sync::atomic::Ordering::Relaxed);
            if shutdown_error.is_some() {
                tracing::error!("marketfeed daemon stopped with errors");
                std::process::exit(1);
            }
            tracing::info!("marketfeed daemon stopped cleanly");
        }
        _ => usage(),
    }
}

#[cfg(unix)]
async fn config_reload_loop(
    path: PathBuf,
    state: Arc<DaemonState>,
    filter: reload::Handle<EnvFilter, tracing_subscriber::Registry>,
) {
    let mut sighup = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "SIGHUP handler unavailable; config hot reload disabled");
            return;
        }
    };
    loop {
        sighup.recv().await;
        tracing::info!(path = %path.display(), "SIGHUP: reloading config");
        handle_config_reload(&path, &state, &filter);
    }
}

#[cfg(not(unix))]
async fn config_reload_loop(
    _path: PathBuf,
    _state: Arc<DaemonState>,
    _filter: reload::Handle<EnvFilter, tracing_subscriber::Registry>,
) {
    // ponytail: Windows has no SIGHUP; restart for config changes.
    std::future::pending::<()>().await
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("ctrl_c");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("sigterm")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketfeed_adapter_api::EventBatch;
    use marketfeed_model::SessionId;
    use marketfeed_sinks::EventSink;

    #[tokio::test]
    async fn unexpected_task_failure_triggers_supervised_shutdown() {
        let mut handles = vec![tokio::spawn(async {
            Err::<(), String>("recording write failed".into())
        })];
        let error =
            wait_for_shutdown_or_task_exit(&mut handles, std::future::pending(), None).await;
        assert_eq!(
            error.as_deref(),
            Some("daemon task failed: recording write failed")
        );
        assert!(handles.is_empty());
    }

    #[tokio::test]
    async fn required_sink_failure_triggers_supervised_shutdown() {
        let config = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:0"
            [[sinks]]
            id = "required-memory"
            type = "memory"
            required = true
            capacity = 1
            overflow = "fail_engine"
            "#,
        )
        .unwrap();
        let state = DaemonState::new(config);
        let batch = |frame_seq| EventBatch {
            session: SessionId(1),
            frame_seq,
            events: Vec::new(),
        };
        state.sinks.lock().unwrap().push_batch(batch(1)).unwrap();
        for _ in 0..100 {
            if state.sinks.lock().unwrap().memory_batch_len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        state.sinks.lock().unwrap().push_batch(batch(2)).unwrap();

        let mut handles = Vec::new();
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            wait_for_shutdown_or_task_exit(
                &mut handles,
                std::future::pending(),
                Some(Arc::clone(&state)),
            ),
        )
        .await
        .expect("required sink supervision timed out")
        .expect("required sink failure must trigger shutdown");
        assert!(error.contains("required sink required-memory"));
        assert!(error.contains("FailEngine"));
    }
}
