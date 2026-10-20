//! Finite offline snapshot-ingress diagnostic. No network and no data admission.
//! Run one arm per externally supervised process; see the registered plan.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use marketfeed_adapter_api::{
    Channel, ConcreteSubscription, ConcreteSubscriptionSet, DeliveryOptions, SessionSpec,
    VenueFactory,
};
use marketfeed_adapter_bybit::{BybitCategory, BybitFactory};
use marketfeed_engine::{SessionLifecycle, SessionRunner, SessionRunnerConfig};
use marketfeed_model::*;
use marketfeed_recording::shared_feed::{FeedHandle, FeedRecord, FeedWriter};
use marketfeed_recording::{
    MetadataRecord, PipelineConfig, RecordingHandle, RecordingPipeline, RotationConfig,
    SessionRecordingMetadata,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::Read,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const RATE: usize = 160;
const SECONDS: usize = 10;
const BUDGET: Duration = Duration::from_secs(15);
const MAX_QUEUE_FRAMES: usize = 512;
const MAX_QUEUE_BYTES: usize = 2 * 1024 * 1024;
type Error = Box<dyn std::error::Error + Send + Sync>;
type Failure = Arc<Mutex<Option<String>>>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema_version: u16,
    sessions: Vec<FixtureSession>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureSession {
    metadata: SessionRecordingMetadata,
    symbol: String,
    payloads: Vec<String>,
}

#[derive(Default, Serialize)]
struct Timing {
    count: u64,
    total_ns: u64,
    max_ns: u64,
    buckets: [u64; 6],
}
impl Timing {
    fn add(&mut self, d: Duration) {
        let ns = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        self.count += 1;
        self.total_ns = self.total_ns.saturating_add(ns);
        self.max_ns = self.max_ns.max(ns);
        let bucket = [10_000, 100_000, 1_000_000, 10_000_000, 100_000_000]
            .iter()
            .position(|v| ns < *v)
            .unwrap_or(5);
        self.buckets[bucket] += 1;
    }
}
#[derive(Default, Serialize)]
struct QueueState {
    frames: usize,
    bytes: usize,
    peak_frames: usize,
    peak_bytes: usize,
}
#[derive(Default)]
struct Queue(Mutex<QueueState>);
struct Packet {
    ordinal: usize,
    planned: Instant,
    payload: Arc<Vec<u8>>,
    lease: Option<Arc<Queue>>,
}
impl Packet {
    fn unqueue(&mut self) {
        if let Some(queue) = self.lease.take() {
            let mut q = queue.0.lock().unwrap();
            q.frames -= 1;
            q.bytes -= self.payload.len();
        }
    }
}
impl Drop for Packet {
    fn drop(&mut self) {
        self.unqueue();
    }
}
impl Queue {
    fn reserve(
        self: &Arc<Self>,
        ordinal: usize,
        planned: Instant,
        payload: Arc<Vec<u8>>,
    ) -> Option<Packet> {
        let mut q = self.0.lock().unwrap();
        if q.frames >= MAX_QUEUE_FRAMES || payload.len() > MAX_QUEUE_BYTES.saturating_sub(q.bytes) {
            return None;
        }
        q.frames += 1;
        q.bytes += payload.len();
        q.peak_frames = q.peak_frames.max(q.frames);
        q.peak_bytes = q.peak_bytes.max(q.bytes);
        Some(Packet {
            ordinal,
            planned,
            payload,
            lease: Some(self.clone()),
        })
    }
}
fn fail(failure: &Failure, message: impl Into<String>) {
    let mut first = failure.lock().unwrap();
    if first.is_none() {
        *first = Some(message.into());
    }
}
fn failed(failure: &Failure) -> bool {
    failure.lock().unwrap().is_some()
}
fn spawn_report<T: Send + 'static>(
    failure: Failure,
    role: &'static str,
    call: impl FnOnce() -> T + Send + 'static,
) -> thread::JoinHandle<Option<T>> {
    thread::spawn(move || match catch_unwind(AssertUnwindSafe(call)) {
        Ok(report) => Some(report),
        Err(_) => {
            // Signal immediately, regardless of which handle the parent joins
            // first. Sibling workers must stop even if that join is waiting.
            fail(&failure, format!("{role} panic"));
            None
        }
    })
}
fn join_reports<T>(
    handles: Vec<thread::JoinHandle<Option<T>>>,
    failure: &Failure,
    role: &str,
) -> Result<Vec<T>, Error> {
    let mut reports = Vec::new();
    let mut panicked = false;
    for handle in handles {
        match handle.join() {
            Ok(Some(report)) => reports.push(report),
            _ => {
                panicked = true;
                fail(failure, format!("{role} panic"));
            }
        }
    }
    if panicked {
        Err(format!("{role} panic; all workers joined").into())
    } else {
        Ok(reports)
    }
}
// Ensure setup/report errors cannot strand the raw worker. The explicit join
// below reports panic; Drop also stops and joins on every early-return path.
struct RawWorker {
    done: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}
impl RawWorker {
    fn finish(&mut self) -> Result<(), Error> {
        self.done.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            handle.join().map_err(|_| "raw worker panic")?;
        }
        Ok(())
    }
}
impl Drop for RawWorker {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}
fn stamp(start: Instant) -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64,
        ),
        mono_ns: start.elapsed().as_nanos() as u64,
    }
}
fn drain(runner: &mut SessionRunner, require_live: bool) -> Result<(usize, usize, usize), Error> {
    let batches = runner.dispatch_mut().drain_batches();
    let systems = runner.dispatch_mut().drain_systems();
    for event in &systems {
        if !matches!(
            event,
            SystemEvent::ConnectionStateChanged { .. }
                | SystemEvent::SubscriptionStateChanged { .. }
                | SystemEvent::BookResynchronized { .. }
        ) {
            return Err(format!("unexpected system event: {event:?}").into());
        }
    }
    if runner.reconnect_requested
        || runner.is_stop_requested()
        || (require_live && runner.lifecycle != SessionLifecycle::Live)
    {
        return Err(format!("unexpected runtime state {:?}", runner.lifecycle).into());
    }
    if !runner.take_pending_http().is_empty() {
        return Err("unexpected HTTP intent".into());
    }
    let writes = runner.take_pending_writes();
    if require_live && !writes.is_empty() {
        return Err("unexpected load-time outbound intent".into());
    }
    Ok((
        batches.iter().map(|b| b.events.len()).sum(),
        systems.len(),
        writes.len(),
    ))
}
fn catalog(session: &FixtureSession) -> Result<CatalogView, Error> {
    let m = session
        .metadata
        .catalog
        .iter()
        .find(|m| m.native_symbol == session.symbol)
        .ok_or("missing native metadata")?;
    let fixed = |m: &marketfeed_recording::FixedMetadata| -> Result<Fixed, Error> {
        Ok(Fixed::new(m.coefficient.parse()?, m.scale))
    };
    let kind = match m.kind.as_str() {
        "Spot" => InstrumentKind::Spot,
        "PerpetualLinear" => InstrumentKind::PerpetualLinear,
        "PerpetualInverse" => InstrumentKind::PerpetualInverse,
        _ => return Err("unsupported diagnostic kind".into()),
    };
    if m.status != "Active" {
        return Err("diagnostic requires active native instrument".into());
    }
    let version = CatalogVersion(session.metadata.catalog_version);
    Ok(CatalogView::with_instruments(
        VenueId(session.metadata.venue_id),
        version,
        vec![Instrument {
            id: InstrumentId(m.instrument_id),
            key: InstrumentKey {
                venue: VenueCode(session.metadata.adapter.clone()),
                native_symbol: m.native_symbol.clone(),
                kind,
                settlement: m.settlement.clone().map(AssetCode),
                expiry_ns: m.expiry_ns,
            },
            base: AssetCode(m.base.clone()),
            quote: AssetCode(m.quote.clone()),
            settlement: m.settlement.clone().map(AssetCode),
            price_scale: m.price_scale,
            quantity_scale: m.quantity_scale,
            price_increment: fixed(&m.price_increment)?,
            quantity_increment: fixed(&m.quantity_increment)?,
            min_quantity: m.min_quantity.as_ref().map(fixed).transpose()?,
            max_quantity: m.max_quantity.as_ref().map(fixed).transpose()?,
            min_notional: m.min_notional.as_ref().map(fixed).transpose()?,
            contract_size: m.contract_size.as_ref().map(fixed).transpose()?,
            expiry_ns: m.expiry_ns,
            status: InstrumentStatus::Active,
            inverse: m.inverse,
            catalog_version: version,
        }],
    ))
}
fn setup(
    session: FixtureSession,
    raw: RecordingHandle,
    feed: Option<FeedHandle>,
    clock: Instant,
) -> Result<(SessionRunner, Vec<Arc<Vec<u8>>>, u64, usize), Error> {
    let category = match session.metadata.venue_id {
        5 => BybitCategory::Linear,
        6 => BybitCategory::Spot,
        11 => BybitCategory::Inverse,
        _ => return Err("unexpected diagnostic venue".into()),
    };
    if session.payloads.len() != 3 {
        return Err("three payloads required: L1/L2/ticker".into());
    }
    let payloads: Vec<_> = session
        .payloads
        .iter()
        .map(|s| STANDARD.decode(s).map(Arc::new))
        .collect::<Result<_, _>>()?;
    if payloads.iter().any(|p| p.len() > 65536 || p.is_empty()) {
        return Err("diagnostic payload bound".into());
    }
    for (index, payload) in payloads.iter().enumerate() {
        let v: Value = serde_json::from_slice(payload)?;
        let topic = if index == 0 {
            format!("orderbook.1.{}", session.symbol)
        } else if index == 1 {
            format!("orderbook.50.{}", session.symbol)
        } else {
            format!("tickers.{}", session.symbol)
        };
        if v["topic"] != topic || v["type"] != "snapshot" {
            return Err("unexpected diagnostic native template".into());
        }
    }
    let catalog = catalog(&session)?;
    let instrument = catalog.instruments[0].id;
    let spec = SessionSpec {
        endpoint_name: "memory://paced-offline".into(),
        subscriptions: ConcreteSubscriptionSet {
            items: [
                Channel::Trades,
                Channel::Quote,
                Channel::L2Book {
                    depth: Some(50),
                    cadence: None,
                },
                Channel::Statistics24h,
            ]
            .into_iter()
            .map(|channel| ConcreteSubscription {
                instrument,
                channel,
                delivery: DeliveryOptions::default(),
            })
            .collect(),
        },
    };
    let metadata = MetadataRecord::Session(SessionRecordingMetadata::from_plan(
        SessionId(session.metadata.session_id),
        catalog.venue,
        "bybit diagnostic snapshot stress",
        "offline synthetic handshake",
        &spec,
        &catalog,
    ));
    raw.register_metadata(metadata.clone())?;
    if let Some(feed) = &feed {
        feed.append(FeedRecord::Metadata(metadata))?;
    }
    let machine = BybitFactory {
        category,
        enable_l2: true,
    }
    .create_session(spec, catalog.clone())?;
    let mut runner = SessionRunner::new(
        machine,
        SessionRunnerConfig {
            venue: catalog.venue,
            session: SessionId(session.metadata.session_id),
            record: true,
            recording_pipeline: Some(raw),
            shared_feed: feed,
            mirror_capacity: 0,
            dispatch_capacity: 64,
            ..Default::default()
        },
    )?;
    runner.on_connected(stamp(clock).receive_ts)?;
    let requests = runner.take_pending_writes();
    let warmup_frames = requests.len() + 2;
    if requests.is_empty() {
        return Err("missing actual subscription requests".into());
    }
    drain(&mut runner, false)?;
    for request in requests {
        let v: Value = serde_json::from_slice(&request.payload)?;
        if v["op"] != "subscribe" || !v["req_id"].is_string() {
            return Err("unexpected fixture startup request".into());
        }
        let mut ack = serde_json::to_vec(
            &json!({"success":true,"op":"subscribe","ret_msg":"","req_id":v["req_id"],"conn_id":format!("synthetic-offline-{}",catalog.venue.0)}),
        )?;
        runner.on_text_frame(&mut ack, stamp(clock))?;
        drain(&mut runner, false)?;
    }
    for p in &payloads[..2] {
        runner.on_text_frame(&mut p.as_ref().clone(), stamp(clock))?;
        drain(&mut runner, false)?;
    }
    drain(&mut runner, true)?;
    Ok((runner, payloads, session.metadata.session_id, warmup_frames))
}
#[derive(Default, Serialize)]
struct ProducerReport {
    planned: usize,
    attempted: usize,
    enqueued: usize,
    enqueue_rejected: usize,
    scheduling_lateness: Timing,
}
#[derive(Default, Serialize)]
struct ConsumerReport {
    session: u64,
    dequeued: usize,
    callbacks_completed: usize,
    callbacks_returned_ok: usize,
    warmup_frames: usize,
    market_rows: usize,
    system_events: usize,
    source_span_ns: u64,
    read_age: Timing,
    callback: Timing,
}

fn run(fixture: Fixture, root: &Path, durable: bool, delay: Duration) -> Result<Value, Error> {
    if fixture.schema_version != 1 || fixture.sessions.len() != 3 {
        return Err("unsupported fixture".into());
    }
    let mut venues: Vec<_> = fixture
        .sessions
        .iter()
        .map(|s| s.metadata.venue_id)
        .collect();
    venues.sort_unstable();
    if venues != [5, 6, 11] {
        return Err("three distinct Bybit venues required".into());
    }
    let mut sessions: Vec<_> = fixture
        .sessions
        .iter()
        .map(|s| s.metadata.session_id)
        .collect();
    sessions.sort_unstable();
    sessions.dedup();
    if sessions.len() != 3 {
        return Err("three distinct sessions required".into());
    }
    // Conservative bound includes base64 expansion, framing, synthetic ACKs and
    // warmup. Rotation alone is not a total raw disk budget.
    let payload_bytes: usize = fixture
        .sessions
        .iter()
        .map(|s| {
            s.payloads
                .iter()
                .map(String::len)
                .max()
                .unwrap_or(0)
                .saturating_mul(RATE * SECONDS + 22 + 4)
        })
        .sum();
    if payload_bytes > 48 * 1024 * 1024 {
        return Err("registered raw payload budget exceeds 48MiB".into());
    }
    fs::create_dir(root)?;
    let failure: Failure = Arc::new(Mutex::new(None));
    let raw = RecordingHandle::new(RecordingPipeline::open(PipelineConfig {
        directory: root.join("raw"),
        queue_capacity: 512,
        queue_byte_capacity: 2 * 1024 * 1024,
        overflow: OverflowPolicy::FailEngine,
        rotation: RotationConfig {
            max_bytes: 64 * 1024 * 1024,
            max_duration: Duration::ZERO,
        },
        min_free_bytes: 1024 * 1024 * 1024,
    })?);
    let feed = if durable {
        Some(FeedHandle::new(FeedWriter::create(
            root.join("feed"),
            128 * 1024 * 1024,
        )?))
    } else {
        None
    };
    if let Some(feed) = &feed {
        feed.append(FeedRecord::artifact(
            "diagnostic.synthetic_paced_snapshot_stress",
            br#"{"qualified":false,"synthetic_handshake":true,"native_payloads_historical":true}"#,
        ))?;
    }
    let done = Arc::new(AtomicBool::new(false));
    let worker = {
        let raw = raw.clone();
        let done = done.clone();
        let failure = failure.clone();
        thread::spawn(move || {
            while !done.load(Ordering::Acquire) && !failed(&failure) {
                if let Err(e) = raw.flush_pending(4096) {
                    fail(&failure, format!("raw worker: {e}"));
                    break;
                }
                thread::sleep(Duration::from_millis(10));
            }
        })
    };
    let mut worker = RawWorker {
        done,
        handle: Some(worker),
    };
    let clock = Instant::now();
    let mut prepared = Vec::new();
    for session in fixture.sessions {
        prepared.push(setup(session, raw.clone(), feed.clone(), clock)?);
    }
    let timings_before = feed.as_ref().map(FeedHandle::timings).transpose()?;
    let start = Instant::now() + Duration::from_millis(100);
    let deadline = start + BUDGET;
    let mut producers = Vec::new();
    let mut consumers = Vec::new();
    let mut queues = Vec::new();
    for (slot, (mut runner, pool, session_id, warmup_frames)) in prepared.into_iter().enumerate() {
        let (tx, rx) = mpsc::sync_channel::<Packet>(MAX_QUEUE_FRAMES);
        let queue = Arc::new(Queue::default());
        queues.push(queue.clone());
        let producer_failure = failure.clone();
        producers.push(spawn_report(failure.clone(), "producer", move || {
            let burst = if slot == 0 { 22 } else { 21 };
            let mut report = ProducerReport {
                planned: RATE * SECONDS + burst,
                ..Default::default()
            };
            let mut ordinal = 0;
            'source: for index in 0..RATE * SECONDS {
                let planned =
                    start + Duration::from_nanos((index as u64 * 1_000_000_000) / RATE as u64);
                if let Some(wait) = planned.checked_duration_since(Instant::now()) {
                    thread::sleep(wait);
                }
                let count = if index == RATE * 5 { 1 + burst } else { 1 };
                for _ in 0..count {
                    if failed(&producer_failure) || Instant::now() >= deadline {
                        break 'source;
                    }
                    report.attempted += 1;
                    report
                        .scheduling_lateness
                        .add(Instant::now().saturating_duration_since(planned));
                    let which = match index % 16 {
                        0..=9 => 0,
                        10..=14 => 1,
                        _ => 2,
                    };
                    let Some(packet) = queue.reserve(ordinal, planned, pool[which].clone()) else {
                        report.enqueue_rejected += 1;
                        fail(&producer_failure, "count/byte ingress budget exhausted");
                        break 'source;
                    };
                    if tx.try_send(packet).is_err() {
                        report.enqueue_rejected += 1;
                        fail(&producer_failure, "bounded transport queue refused input");
                        break 'source;
                    }
                    report.enqueued += 1;
                    ordinal += 1;
                }
            }
            report
        }));
        let consumer_failure = failure.clone();
        consumers.push(spawn_report(failure.clone(), "consumer", move || {
            let mut report = ConsumerReport {
                session: session_id,
                warmup_frames,
                ..Default::default()
            };
            let mut first = None;
            loop {
                if failed(&consumer_failure) || Instant::now() >= deadline {
                    if Instant::now() >= deadline {
                        fail(&consumer_failure, "15s work/drain deadline");
                    }
                    break;
                }
                let mut packet = match rx.recv_timeout(Duration::from_millis(10)) {
                    Ok(packet) => packet,
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                };
                let read_return = Instant::now();
                packet.unqueue();
                if packet.ordinal != report.dequeued {
                    fail(&consumer_failure, "per-session input order changed");
                    break;
                }
                report.dequeued += 1;
                report
                    .read_age
                    .add(read_return.saturating_duration_since(packet.planned));
                let first = *first.get_or_insert(packet.planned);
                report.source_span_ns = packet.planned.duration_since(first).as_nanos() as u64;
                let received = stamp(clock);
                let began = Instant::now();
                if !delay.is_zero() {
                    thread::sleep(delay);
                }
                let result = runner.on_text_frame(&mut packet.payload.as_ref().clone(), received);
                report.callback.add(began.elapsed());
                if let Err(e) = result {
                    fail(&consumer_failure, format!("callback: {e}"));
                    break;
                }
                report.callbacks_returned_ok += 1;
                match drain(&mut runner, true) {
                    Ok((rows, systems, _)) => {
                        report.callbacks_completed += 1;
                        report.market_rows += rows;
                        report.system_events += systems;
                    }
                    Err(e) => {
                        fail(&consumer_failure, format!("dispatch/readiness: {e}"));
                        break;
                    }
                }
            }
            runner.request_stop();
            (report, runner.lifecycle)
        }));
    }
    let producer_reports = join_reports(producers, &failure, "producer");
    let consumer_reports = join_reports(consumers, &failure, "consumer");
    worker.finish()?;
    let drain_started = Instant::now();
    if let Err(e) = raw.shutdown_drain(deadline) {
        fail(&failure, format!("raw shutdown: {e}"));
    }
    let raw_drain_ns = drain_started.elapsed().as_nanos();
    let producer_reports = producer_reports?;
    let consumer_reports = consumer_reports?;
    let raw_snapshot = raw.snapshot()?;
    let exact = producer_reports
        .iter()
        .zip(&consumer_reports)
        .all(|(p, (c, _))| {
            p.planned == p.enqueued
                && p.attempted == p.enqueued
                && p.enqueue_rejected == 0
                && p.enqueued == c.dequeued
                && c.dequeued == c.callbacks_completed
        });
    let freshness = consumer_reports
        .iter()
        .all(|(c, _)| c.source_span_ns >= 9_500_000_000 && c.read_age.max_ns <= 1_000_000_000);
    if !exact {
        fail(&failure, "input accounting incomplete");
    }
    if !freshness {
        fail(&failure, "synthetic schedule freshness bound failed");
    }
    if start.elapsed() > BUDGET {
        fail(&failure, "15s total/drain deadline exceeded");
    }
    let passed =
        !failed(&failure) && raw_snapshot.queue_len == 0 && raw_snapshot.dropped_total == 0;
    let first_failure = failure.lock().unwrap().clone();
    if let Some(feed) = &feed {
        if let Err(e) = feed.append(FeedRecord::Seal {
            schema_version: 1,
            now_ns: stamp(clock).receive_ts.0,
            reason: format!(
                "diagnostic_nonqualified: {}",
                if passed { "complete" } else { "failed" }
            ),
        }) {
            fail(&failure, format!("seal: {e}"));
        }
    }
    let elapsed = start.elapsed();
    if elapsed > BUDGET {
        fail(&failure, "15s total deadline exceeded including seal");
    }
    let accounting: Vec<_> = producer_reports
        .iter()
        .zip(&consumer_reports)
        .map(|(p, (c, _))| {
            json!({
                "session": c.session,
                "not_attempted": p.planned - p.attempted,
                "abandoned_enqueued": p.enqueued - c.dequeued,
                "dequeued_without_validated_callback": c.dequeued - c.callbacks_completed,
            })
        })
        .collect();
    let queue_reports: Vec<_> = queues
        .iter()
        .map(|q| serde_json::to_value(&*q.0.lock().unwrap()).unwrap())
        .collect();
    Ok(
        json!({"schema":"marketfeed.paced_snapshot_diagnostic.v1","qualified_dataset":false,"independent_storage_audit_required":true,"verdict_scope":"local_harness_only","exchange_latency_measured":false,"durable_shared_arm":durable,"delay_us":delay.as_micros(),"rate_per_session":RATE,"scheduled_frames":4800,"burst_frames":64,"queue_frame_capacity":MAX_QUEUE_FRAMES,"queue_byte_capacity":MAX_QUEUE_BYTES,"elapsed_ns":elapsed.as_nanos(),"raw_drain_ns":raw_drain_ns,"shutdown_tail_ns":drain_started.elapsed().as_nanos(),"timing_scope":"cumulative snapshots; differences include load and final seal; maxima are cumulative, not subtractable","passed":passed && !failed(&failure),"first_failure":first_failure.or_else(||failure.lock().unwrap().clone()),"producers":producer_reports,"consumers":consumer_reports.into_iter().map(|(r,_)|r).collect::<Vec<_>>(),"input_disposition":accounting,"queue_reservations":queue_reports,"raw": {"records_written":raw_snapshot.records_written,"queue_len":raw_snapshot.queue_len,"dropped_total":raw_snapshot.dropped_total},"writer_timings_before":timings_before,"writer_timings_after":feed.as_ref().map(FeedHandle::timings).transpose()?}),
    )
}
fn main() {
    let result = (|| -> Result<Value, Error> {
        let args: Vec<_> = std::env::args().skip(1).collect();
        if !(args.len() == 3 || args.len() == 4)
            || !matches!(args[1].as_str(), "baseline" | "durable")
        {
            return Err(
                "usage: paced_shared_feed FIXTURE baseline|durable NEW_DIRECTORY [DELAY_US]".into(),
            );
        }
        let mut bytes = Vec::new();
        File::open(&args[0])?
            .take(1_048_577)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 1_048_576 {
            return Err("fixture byte limit".into());
        }
        let fixture: Fixture = serde_json::from_slice(&bytes)?;
        let delay: u64 = args.get(3).map(|s| s.parse()).transpose()?.unwrap_or(0);
        if delay > 100_000 {
            return Err("delay limit".into());
        }
        run(
            fixture,
            &PathBuf::from(&args[2]),
            args[1] == "durable",
            Duration::from_micros(delay),
        )
    })();
    match result {
        Ok(report) => {
            println!("{report}");
            if report["passed"] != true {
                std::process::exit(1);
            }
        }
        Err(error) => {
            println!(
                "{}",
                json!({"passed":false,"error":error.to_string(),"qualified_dataset":false})
            );
            std::process::exit(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_reservations_are_bounded_and_released_on_every_drop() {
        let queue = Arc::new(Queue::default());
        let payload = Arc::new(vec![0; MAX_QUEUE_BYTES / 2]);
        let first = queue.reserve(0, Instant::now(), payload.clone()).unwrap();
        let second = queue.reserve(1, Instant::now(), payload.clone()).unwrap();
        assert!(queue.reserve(2, Instant::now(), payload.clone()).is_none());
        drop(first);
        let mut third = queue.reserve(2, Instant::now(), payload).unwrap();
        third.unqueue();
        third.unqueue(); // Read-return and Drop must not release twice.
        drop(third);
        drop(second);
        let state = queue.0.lock().unwrap();
        assert_eq!(state.frames, 0);
        assert_eq!(state.bytes, 0);
        assert_eq!(state.peak_bytes, MAX_QUEUE_BYTES);
    }

    #[test]
    fn frame_budget_is_independent_of_byte_budget() {
        let queue = Arc::new(Queue::default());
        let payload = Arc::new(vec![0]);
        let packets: Vec<_> = (0..MAX_QUEUE_FRAMES)
            .map(|n| queue.reserve(n, Instant::now(), payload.clone()).unwrap())
            .collect();
        assert!(
            queue
                .reserve(MAX_QUEUE_FRAMES, Instant::now(), payload)
                .is_none()
        );
        drop(packets);
        let state = queue.0.lock().unwrap();
        assert_eq!(state.peak_frames, MAX_QUEUE_FRAMES);
        assert_eq!(state.frames, 0);
    }

    #[test]
    fn early_return_stops_and_joins_raw_worker() {
        let done = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let thread_done = done.clone();
        let thread_stopped = stopped.clone();
        let handle = thread::spawn(move || {
            while !thread_done.load(Ordering::Acquire) {
                thread::yield_now();
            }
            thread_stopped.store(true, Ordering::Release);
        });
        drop(RawWorker {
            done,
            handle: Some(handle),
        });
        assert!(stopped.load(Ordering::Acquire));
    }

    #[test]
    fn either_worker_panic_signals_and_joins_all_siblings() {
        for role in ["producer", "consumer"] {
            let failure: Failure = Arc::new(Mutex::new(None));
            let stopped = Arc::new(AtomicBool::new(false));
            let child_failure = failure.clone();
            let child_stopped = stopped.clone();
            let sibling = spawn_report(failure.clone(), role, move || {
                while !failed(&child_failure) {
                    thread::yield_now();
                }
                child_stopped.store(true, Ordering::Release);
            });
            let panicking =
                spawn_report(failure.clone(), role, || panic!("injected worker failure"));
            // Join the waiting sibling FIRST: the panic must signal without
            // relying on the parent's join order.
            assert!(join_reports(vec![sibling, panicking], &failure, role).is_err());
            assert!(stopped.load(Ordering::Acquire));
            assert_eq!(
                failure.lock().unwrap().as_deref(),
                Some(format!("{role} panic").as_str())
            );
        }
    }
}
