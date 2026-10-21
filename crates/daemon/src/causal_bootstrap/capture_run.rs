//! Fresh-directory orchestration. The projection independently reexecutes only
//! committed source records. No second exchange collector or consumer sink.
use super::*;
use crate::causal_bootstrap::{PreparedBootstrap, live::*, projection::*, replay::*};
use marketfeed_engine::causal::projection::*;
use marketfeed_recording::{shared_batch::*, shared_feed::*};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

type Error = Box<dyn std::error::Error + Send + Sync>;
pub fn projection_limits() -> ProjectionLimits {
    ProjectionLimits {
        max_group_bytes: 8 << 20,
        max_items: 4096,
        max_owners: 2,
        max_bootstrap_bytes: 16 << 20,
        max_catalog_entries: 10_000,
        max_pending_inputs: 4096,
    }
}
pub fn replay_limits() -> MultiReplayLimits {
    MultiReplayLimits {
        per_owner: OwnerHistoryLimits {
            max_retained_input_bytes: 64 << 20,
            max_input_bytes: 128 << 10,
            max_record_bytes: MAX_OPERATION_BYTES,
        },
        maximum: ReplayResources {
            owners: 2,
            bootstrap_payload_bytes: 16 << 20,
            pending_inputs: 4096,
            pending_input_bytes: 128 << 20,
            active_operation_and_state_bytes: 64 << 20,
            source_owned_pending_inputs: 4096,
            source_owned_pending_bytes: 128 << 20,
            capture_trace_reservation_bytes: 8 << 20,
            catalog_entries: 10_000,
            selected_instruments: 128,
            maintained_book_levels: 128 * 256,
            cached_book_levels: 0,
        },
    }
}
fn batch() -> BatchConfig {
    BatchConfig {
        max_pending_operations: 16,
        max_pending_bytes: 32 << 20,
        max_group_operations: 8,
        max_group_bytes: 16 << 20,
        max_age: Duration::from_millis(1),
    }
}
pub(super) fn preflight(bootstraps: &[BootstrapDto]) -> Result<(), Error> {
    let limits = projection_limits();
    let mut total = 0usize;
    let mut catalog_entries = 0usize;
    if bootstraps.is_empty() || bootstraps.len() > limits.max_owners {
        return Err("bootstrap count exceeds capture projection profile".into());
    }
    for dto in bootstraps {
        catalog_entries = catalog_entries
            .checked_add(dto.catalog.instruments.len())
            .ok_or("catalog count overflow")?;
        if catalog_entries > limits.max_catalog_entries {
            return Err("catalog aggregate exceeds shared consumer profile".into());
        }
        let bytes = serde_json::to_vec(dto)?.len();
        // Exact serialized DTO plus a conservative 4KiB for the fixed bootstrap
        // group wrapper, policy, two bounded physical cursors and owner identity.
        if bytes
            .checked_add(4096)
            .is_none_or(|n| n > limits.max_group_bytes)
        {
            return Err("bootstrap exceeds capture projection group profile".into());
        }
        total = total.checked_add(bytes).ok_or("bootstrap byte overflow")?;
        if total > limits.max_bootstrap_bytes
            || total > replay_limits().maximum.bootstrap_payload_bytes
        {
            return Err("bootstrap aggregate exceeds capture replay/projection profile".into());
        }
    }
    Ok(())
}
fn retain_artifact(root: &Path, expected: &BootstrapBuildDto) -> Result<(), Error> {
    let mut source = std::fs::File::open(std::env::current_exe()?)?;
    if !source.metadata()?.is_file() || source.metadata()?.len() != expected.executable_bytes {
        return Err("capture artifact changed before retention".into());
    }
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o555);
    }
    let mut target = options.open(root.join("capture-executable"))?;
    let mut hash = Sha256::new();
    let mut copied = 0u64;
    let mut buffer = [0u8; 65536];
    let mut bounded = (&mut source).take(
        expected
            .executable_bytes
            .checked_add(1)
            .ok_or("artifact size overflow")?,
    );
    loop {
        let count = bounded.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        copied += count as u64;
        hash.update(&buffer[..count]);
        target.write_all(&buffer[..count])?;
    }
    if copied != expected.executable_bytes
        || format!("{:x}", hash.finalize()) != expected.executable_sha256
        || current_build()? != *expected
    {
        return Err("capture artifact changed during retention".into());
    }
    target.sync_all()?;
    #[cfg(unix)]
    std::fs::File::open(root)?.sync_all()?;
    write_new(&root.join("build.json"), &serde_json::to_value(expected)?)?;
    Ok(())
}
fn write_new(path: &Path, value: &Value) -> Result<(), Error> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    #[cfg(unix)]
    std::fs::File::open(path.parent().ok_or("output has no parent")?)?.sync_all()?;
    Ok(())
}
struct Finished(Arc<AtomicBool>);
impl Drop for Finished {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
struct ProjectionFailure {
    flag: Arc<AtomicBool>,
    success: bool,
}
impl Drop for ProjectionFailure {
    fn drop(&mut self) {
        if !self.success {
            self.flag.store(true, Ordering::Release);
        }
    }
}
fn writer_report(report: &BatchReport) -> Value {
    json!({
        "published": report.published,
        "committed_operations": report.committed_operations,
        "groups": report.groups,
        "timings": report.timings,
        "timing_scope": "cumulative completed local writer calls; includes failed I/O, excludes initial feed-directory bootstrap; not source freshness or wall time"
    })
}
fn project(
    pump: ProjectionPump,
    finished: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
) -> Result<Value, Error> {
    project_with_hook(pump, finished, failed, |_| {})
}
fn project_with_hook(
    mut pump: ProjectionPump,
    finished: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
    mut after_poll: impl FnMut(ProjectionPoll),
) -> Result<Value, Error> {
    let mut failure = ProjectionFailure {
        flag: failed.clone(),
        success: false,
    };
    let result = (|| -> Result<Value, Error> {
        loop {
            // Completion is published only AFTER source actor shutdown. Load
            // before polling: CaughtUp observed before that publication cannot
            // authorize exit, even if completion becomes visible afterwards.
            let source_finished = finished.load(Ordering::Acquire);
            let poll = pump.poll()?;
            after_poll(poll);
            if poll == ProjectionPoll::Sealed
                || (poll == ProjectionPoll::CaughtUp && source_finished)
            {
                break;
            }
            if matches!(
                poll,
                ProjectionPoll::Writing | ProjectionPoll::Full | ProjectionPoll::CaughtUp
            ) {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        let checkpoint = pump.last_acknowledged();
        Ok(
            json!({"projection_batch_groups":pump.max_groups_per_publication(),"last_publication":pump.last_publication(),"groups":pump.prepared_groups(),"source":checkpoint.source,"target":checkpoint.target,"sealed":checkpoint.sealed}),
        )
    })();
    if result.is_err() {
        failed.store(true, Ordering::Release);
    }
    // Join accepted writes even after replay/encoding failure. A later complete
    // target group may exceed the last observed ACK; never invent its cursor.
    let shutdown = pump.shutdown();
    match (result, shutdown) {
        (Ok(mut value), Ok(writer)) => {
            value["target_writer"] = writer_report(&writer);
            failure.success = true;
            Ok(value)
        }
        (Err(e), _) => Err(e),
        (_, Err(e)) => {
            failed.store(true, Ordering::Release);
            Err(Box::new(e))
        }
    }
}
/// The output root must not exist. Each market has one persistent owner and
/// owned concrete transport; both share the same collector rate budget.
pub async fn run_public(request: CaptureRequest, root: &Path) -> Result<Value, Error> {
    request.validate()?;
    let build = current_build()?;
    if root.try_exists()? {
        return Err("capture output already exists".into());
    }
    let acquired = acquire_public(&request).await?;
    let bootstraps = acquired.prepare(&request)?;
    preflight(&bootstraps)?;
    let prepared = bootstraps
        .into_iter()
        .map(|dto| PreparedBootstrap::new_wire_history(dto, MAX_OPERATION_BYTES))
        .collect::<Result<Vec<_>, _>>()?;
    let state = crate::DaemonState::new(crate::DaemonConfig::from_toml_str("")?);
    let budget = CollectorBudget::from_collector(state)?;
    std::fs::create_dir(root)?;
    #[cfg(unix)]
    std::fs::File::open(
        root.parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?
    .sync_all()?;
    write_new(&root.join("request.json"), &serde_json::to_value(&request)?)?;
    retain_artifact(root, &build)?;
    let source = root.join("source");
    let target = root.join("feed");
    let actor = FeedBatchActor::spawn(
        FeedWriter::create_profile(
            &source,
            request.source_max_bytes,
            FeedProfile::EngineCausalV5,
        )?,
        batch(),
    )?;
    let reader =
        MultiHistoryReplay::open_profile(&source, replay_limits(), FeedProfile::EngineCausalV5)?;
    let pump = ProjectionPump::create_batched(
        reader,
        ProjectionPolicy {
            schema_version: 1,
            source_profile: 5,
            clock_policy: ProjectionClockPolicy::PerOwnerStartedPhysicalOrderV1,
            limits: projection_limits(),
        },
        &target,
        request.target_max_bytes,
        batch(),
        request.projection_batch_groups,
    )?;
    let finished = Arc::new(AtomicBool::new(false));
    let failed = Arc::new(AtomicBool::new(false));
    let finish_guard = Finished(finished.clone());
    // Drivers are constructed before any transport starts. Setup failure drops
    // the owners and joins all accepted storage work, leaving an unsealed root.
    let mut drivers = Vec::new();
    let mut controls = Vec::new();
    for (bootstrap, selection) in prepared.into_iter().zip(&request.markets) {
        let pending = bootstrap
            .try_admit(&actor.handle())
            .map_err(|e| format!("bootstrap admission: {:?}", e.reason))?;
        let (driver, control) = CausalHyperliquidDriver::new(pending.wait()?, budget.clone())?;
        drivers.push((selection.market, driver));
        controls.push(control);
    }
    let projection = tokio::task::spawn_blocking({
        let finished = finished.clone();
        let failed = failed.clone();
        move || project(pump, finished, failed)
    });
    use futures_util::{StreamExt, stream::FuturesUnordered};
    let mut runs = drivers
        .into_iter()
        .map(|(market, d)| async move { (market, d.run().await) })
        .collect::<FuturesUnordered<_>>();
    let mut reports = Vec::new();
    let mut interrupted = false;
    let mut stopping = false;
    let signal = tokio::signal::ctrl_c();
    tokio::pin!(signal);
    while !runs.is_empty() {
        tokio::select! {
            report = runs.next() => { if let Some((market,report))=report { if !report.clean_stop { stopping=true; } reports.push((market,report)); } }
            signal = &mut signal, if !interrupted => { interrupted=true; stopping=true; if let Err(error)=signal { eprintln!("capture signal handler: {error}"); } }
            _ = tokio::time::sleep(Duration::from_millis(10)) => { if failed.load(Ordering::Acquire) { stopping=true; } }
        }
        if stopping {
            for control in &controls {
                let _ = control.request_stop();
            }
        }
    }
    let clean = reports.len() == request.markets.len()
        && reports.iter().all(|(_, r)| r.clean_stop)
        && !failed.load(Ordering::Acquire);
    let seal_result: Result<(), Error> = (|| {
        if clean {
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos();
            let transaction = PreparedFeedTransaction::new(vec![FeedRecord::Seal {
                schema_version: 1,
                now_ns: i64::try_from(now_ns)?,
                reason: "all owned drivers completed a committed clean Stop".into(),
            }])?;
            match actor.handle().try_submit(transaction) {
                Ok(ticket) => ticket.wait().map(|_| ()).map_err(|e| Box::new(e) as Error),
                Err(e) => Err(format!("Seal admission: {:?}", e.reason).into()),
            }
        } else {
            Ok(())
        }
    })();
    let source_result = actor.shutdown();
    drop(finish_guard);
    let projection_result = match projection.await {
        Ok(result) => result,
        Err(error) => Err(format!("projection worker join: {error}").into()),
    };
    let report = json!({"schema_version":1,"scope":"public capture and native source reexecution; no availability or strategy readiness attestation",
        "interrupted":interrupted,"clean_drivers":clean,"sealed":clean && seal_result.is_ok() && source_result.is_ok() && projection_result.as_ref().is_ok_and(|p|p["sealed"]==true),
        "drivers":reports.iter().map(|(market,r)|driver_report_value(market,r)).collect::<Vec<_>>(),
        "source_writer":source_result.as_ref().ok().map(writer_report),
        "projection":projection_result.as_ref().ok(),"projection_error":projection_result.as_ref().err().map(ToString::to_string),"seal_error":seal_result.as_ref().err().map(ToString::to_string),"source_error":source_result.as_ref().err().map(ToString::to_string)});
    write_new(&root.join("report.json"), &report)?;
    if report["sealed"] != true {
        return Err(format!(
            "capture did not complete cleanly; inspect {}",
            root.join("report.json").display()
        )
        .into());
    }
    Ok(report)
}
/// Requires the exact capture executable. Independently checks native callbacks
/// and target/source pairing, retaining prefix uncertainty on unsealed captures.
pub fn verify(root: &Path) -> Result<Value, Error> {
    current_build()?;
    let comparison = compare_with_source(
        MultiHistoryReplay::open_profile(
            root.join("source"),
            replay_limits(),
            FeedProfile::EngineCausalV5,
        )?,
        ProjectionReader::open(root.join("feed"), projection_limits())?,
    )?;
    Ok(
        json!({"schema_version":1,"groups":comparison.groups,"source_after":comparison.source_after,"sealed":comparison.sealed,"scope":"native comparison through initially observed target committed bound"}),
    )
}

#[cfg(all(test, feature = "causal-journal"))]
mod tests {
    use super::*;
    use crate::causal_bootstrap::{
        capture::tests::{build, request},
        owner::{InputSettlement, OwnerPhase},
    };
    use marketfeed_engine::CaptureInput;
    use marketfeed_model::ConnectionId;
    #[test]
    fn two_actual_native_owners_project_while_growing_and_verify_after_clean_seal() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let source = root.join("source");
        let actor = FeedBatchActor::spawn(
            FeedWriter::create_profile(&source, 64 << 20, FeedProfile::EngineCausalV5).unwrap(),
            batch(),
        )
        .unwrap();
        let pump = ProjectionPump::create(
            MultiHistoryReplay::open_profile(&source, replay_limits(), FeedProfile::EngineCausalV5)
                .unwrap(),
            ProjectionPolicy {
                schema_version: 1,
                source_profile: 5,
                clock_policy: ProjectionClockPolicy::PerOwnerStartedPhysicalOrderV1,
                limits: projection_limits(),
            },
            root.join("feed"),
            64 << 20,
            batch(),
        )
        .unwrap();
        let finished = Arc::new(AtomicBool::new(false));
        let failed = Arc::new(AtomicBool::new(false));
        let (caught_tx, caught_rx) = std::sync::mpsc::sync_channel(1);
        let (continue_tx, continue_rx) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::spawn({
            let finished = finished.clone();
            let failed = failed.clone();
            move || {
                let mut first = true;
                project_with_hook(pump, finished, failed, |poll| {
                    if first && poll == ProjectionPoll::CaughtUp {
                        first = false;
                        caught_tx.send(()).unwrap();
                        continue_rx.recv().unwrap();
                    }
                })
            }
        });
        caught_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        for dto in build(&request()).unwrap() {
            let owner = PreparedBootstrap::new_wire_history(dto, MAX_OPERATION_BYTES)
                .unwrap()
                .try_admit(&actor.handle())
                .unwrap()
                .wait()
                .unwrap()
                .instantiate()
                .unwrap()
                .spawn_history_owner()
                .unwrap();
            assert!(matches!(
                owner
                    .ingress()
                    .try_submit(ConnectionId(0), CaptureInput::Stop)
                    .unwrap()
                    .wait(),
                InputSettlement::Committed { .. }
            ));
            owner.close_ingress();
            let start = std::time::Instant::now();
            loop {
                if owner.status().owner.phase == OwnerPhase::Stopped {
                    break;
                }
                assert!(start.elapsed() < Duration::from_secs(10));
                std::thread::sleep(Duration::from_millis(1));
            }
            assert!(owner.wire_progress().unwrap().2);
            owner.shutdown();
        }
        actor
            .handle()
            .try_submit(
                PreparedFeedTransaction::new(vec![FeedRecord::Seal {
                    schema_version: 1,
                    now_ns: 1,
                    reason: "actual native Stop fixture".into(),
                }])
                .unwrap(),
            )
            .unwrap()
            .wait()
            .unwrap();
        actor.shutdown().unwrap();
        finished.store(true, Ordering::Release);
        continue_tx.send(()).unwrap();
        let projected = thread.join().unwrap().unwrap();
        assert_eq!(projected["sealed"], true);
        assert!(!failed.load(Ordering::Acquire));
        let checked = verify(root).unwrap();
        assert_eq!(checked["sealed"], true);
        assert_eq!(checked["groups"], projected["groups"]);
        // Exercise the actual reader with the existing HLS consumer ceiling;
        // a producer-declared larger policy is refused even for a tiny group.
        let mut consumer = ProjectionReader::open(
            root.join("feed"),
            ProjectionLimits {
                max_group_bytes: 8 << 20,
                max_items: 4096,
                max_owners: 10,
                max_bootstrap_bytes: 16 << 20,
                max_catalog_entries: 10_000,
                max_pending_inputs: 4096,
            },
        )
        .unwrap();
        let mut groups = 0u64;
        while let Some(group) = consumer.read_next().unwrap() {
            assert_eq!(group.policy.source_profile, 5);
            groups += 1;
        }
        assert_eq!(json!(groups), projected["groups"]);
        let progress = consumer.progress().unwrap();
        assert!(progress.sealed);
        assert_eq!(projected["target"], json!(progress.target_cursor));
        assert_eq!(projected["source"], json!(progress.source_cursor));
        // Read the physical target independently of the pump and its receipts.
        let mut physical = FeedReader::open_profile(
            root.join("feed"),
            None,
            FeedProfile::ObservationProjectionV1,
        )
        .unwrap();
        let mut physical_groups = 0u64;
        while let Some(record) = physical.read_next().unwrap() {
            assert!(matches!(record, FeedRecord::Projection(_)));
            physical_groups += 1;
        }
        assert_eq!(physical_groups, groups);
        assert_eq!(
            projected["target_writer"]["published"],
            json!(physical.cursor())
        );
        assert_eq!(
            projected["target_writer"]["committed_operations"],
            json!(physical_groups)
        );
        assert_eq!(physical.cursor(), physical.observed_commit());
    }
    #[test]
    fn report_never_overwrites_existing_file() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("report.json");
        write_new(&path, &json!({"first":true})).unwrap();
        let before = std::fs::read(&path).unwrap();
        assert!(write_new(&path, &json!({"first":false})).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }
}
