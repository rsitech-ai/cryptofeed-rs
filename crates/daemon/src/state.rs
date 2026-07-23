//! Process health state and Prometheus text exposition.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use marketfeed_engine::{EngineLifecycle, EngineMetrics, RecordingRotateHandle};
use marketfeed_recording::{
    PipelineConfig, RecordingHandle, RecordingPipeline, RotationConfig, df_free_bytes,
};

use crate::config::{DaemonConfig, ReadinessConfig};
use crate::reload::ReloadableConfig;
use crate::sinks::DaemonSinks;

fn prometheus_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

/// Shared daemon runtime flags (health + thin metrics).
///
/// # ponytail
/// Venue liveness is `Arc<AtomicBool>` shared with SessionRunner live_signal.
/// EngineMetrics arcs are shared with runners for §23.2 scrape.
/// `reloadable` holds §21.4 in-process knobs (log filter / readiness); venues
/// stay frozen on `config` until a shared EngineControl plane exists.
#[derive(Debug)]
pub struct DaemonState {
    pub config: DaemonConfig,
    /// Hot-reloadable subset (§21.4); see `reload` module.
    pub reloadable: Mutex<ReloadableConfig>,
    pub supervisor_lifecycle: Mutex<EngineLifecycle>,
    pub process_live: AtomicBool,
    pub venue_flags: HashMap<String, Arc<AtomicBool>>,
    pub venue_stops: HashMap<String, Arc<AtomicBool>>,
    pub venue_metrics: HashMap<String, Arc<EngineMetrics>>,
    /// Configured sinks; empty ⇒ venue loops null-drain dispatch.
    pub sinks: Arc<Mutex<DaemonSinks>>,
    pub recording_healthy: AtomicBool,
    pub recording_queue_len: AtomicU64,
    pub recording_rotations: AtomicU64,
    pub recording_written: AtomicU64,
    pub recording_dropped: AtomicU64,
    /// Control-plane rotate request (§19.2); recording task polls via `take_request`.
    pub recording_rotate: Arc<RecordingRotateHandle>,
    /// Shared bounded raw-frame pipeline used by every public venue runner.
    pub recording_pipeline: Option<RecordingHandle>,
    /// Public venue tasks that can still enqueue raw frames during shutdown.
    pub active_public_venue_tasks: AtomicU64,
    pub disk_pressure: AtomicBool,
    pub shutdown_draining: AtomicBool,
    pub http_requests: AtomicU64,
    pub started_unix_secs: u64,
}

impl DaemonState {
    pub fn new(config: DaemonConfig) -> Arc<Self> {
        Self::try_new(config).expect("validated daemon runtime resources")
    }

    pub fn try_new(config: DaemonConfig) -> Result<Arc<Self>, String> {
        let mut venue_flags = HashMap::new();
        let mut venue_stops = HashMap::new();
        let mut venue_metrics = HashMap::new();
        for v in &config.venues {
            venue_flags.insert(v.id.clone(), Arc::new(AtomicBool::new(false)));
            venue_stops.insert(v.id.clone(), Arc::new(AtomicBool::new(false)));
            venue_metrics.insert(v.id.clone(), Arc::new(EngineMetrics::new()));
        }
        let sinks = DaemonSinks::from_config(&config).map_err(|e| e.to_string())?;
        let recording_pipeline = if config.recording.raw.enabled {
            let raw = &config.recording.raw;
            let pipeline = RecordingPipeline::open_with_metadata(
                PipelineConfig {
                    directory: raw.directory.clone().into(),
                    queue_capacity: raw.queue_capacity,
                    overflow: raw.overflow_policy().map_err(|e| e.to_string())?,
                    rotation: RotationConfig {
                        max_bytes: raw.segment_size_bytes().map_err(|e| e.to_string())?,
                        max_duration: raw.segment_duration().map_err(|e| e.to_string())?,
                    },
                    min_free_bytes: raw.min_free_bytes().map_err(|e| e.to_string())?,
                },
                vec![marketfeed_recording::MetadataRecord::current_build()],
            )
            .map_err(|e| e.to_string())?;
            let handle = RecordingHandle::new(pipeline);
            handle
                .set_free_space_probe(Box::new(df_free_bytes))
                .map_err(|e| e.to_string())?;
            Some(handle)
        } else {
            None
        };
        let recording_enabled = recording_pipeline.is_some();
        let started_unix_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let reloadable = ReloadableConfig::from_daemon(&config);
        Ok(Arc::new(Self {
            config,
            reloadable: Mutex::new(reloadable),
            supervisor_lifecycle: Mutex::new(EngineLifecycle::Starting),
            process_live: AtomicBool::new(false),
            venue_flags,
            venue_stops,
            venue_metrics,
            sinks: Arc::new(Mutex::new(sinks)),
            recording_healthy: AtomicBool::new(!recording_enabled),
            recording_queue_len: AtomicU64::new(0),
            recording_rotations: AtomicU64::new(0),
            recording_written: AtomicU64::new(0),
            recording_dropped: AtomicU64::new(0),
            recording_rotate: Arc::new(RecordingRotateHandle::new()),
            recording_pipeline,
            active_public_venue_tasks: AtomicU64::new(0),
            disk_pressure: AtomicBool::new(false),
            shutdown_draining: AtomicBool::new(false),
            http_requests: AtomicU64::new(0),
            started_unix_secs,
        }))
    }

    pub fn has_sinks(&self) -> bool {
        !self.sinks.lock().expect("sinks lock").is_empty()
    }

    pub fn venue_flag(&self, id: &str) -> Option<Arc<AtomicBool>> {
        self.venue_flags.get(id).map(Arc::clone)
    }

    pub fn venue_stop(&self, id: &str) -> Option<Arc<AtomicBool>> {
        self.venue_stops.get(id).map(Arc::clone)
    }

    pub fn venue_metrics(&self, id: &str) -> Option<Arc<EngineMetrics>> {
        self.venue_metrics.get(id).map(Arc::clone)
    }

    pub fn request_all_stops(&self) {
        for flag in self.venue_stops.values() {
            flag.store(true, Ordering::Relaxed);
        }
    }

    pub fn mark_supervisor_running(&self) {
        *self.supervisor_lifecycle.lock().expect("lifecycle lock") = EngineLifecycle::Running;
        self.process_live.store(true, Ordering::Relaxed);
    }

    pub fn live_session_count(&self) -> u64 {
        self.venue_flags
            .values()
            .filter(|f| f.load(Ordering::Relaxed))
            .count() as u64
    }

    pub fn is_live(&self) -> bool {
        self.process_live.load(Ordering::Relaxed)
            && *self.supervisor_lifecycle.lock().expect("lifecycle lock")
                == EngineLifecycle::Running
    }

    pub fn is_ready(&self) -> bool {
        let venue_live: HashMap<String, bool> = self
            .venue_flags
            .iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
            .collect();
        let readiness = self
            .reloadable
            .lock()
            .expect("reloadable lock")
            .readiness
            .clone();
        evaluate_readiness(
            &readiness,
            &self.config,
            self.is_live(),
            &venue_live,
            self.live_session_count(),
            self.recording_healthy.load(Ordering::Relaxed),
        )
    }

    pub fn prometheus_text(&self) -> String {
        let live = if self.is_live() { 1 } else { 0 };
        let ready = if self.is_ready() { 1 } else { 0 };
        let sessions = self.live_session_count();
        let reqs = self.http_requests.load(Ordering::Relaxed);
        let rec_en = if self.config.recording.raw.enabled {
            1
        } else {
            0
        };
        let rec_ok = if self.recording_healthy.load(Ordering::Relaxed) {
            1
        } else {
            0
        };
        let disk = if self.disk_pressure.load(Ordering::Relaxed) {
            1
        } else {
            0
        };
        let draining = if self.shutdown_draining.load(Ordering::Relaxed) {
            1
        } else {
            0
        };
        let venues = self.config.venues.len();
        let mut out = String::with_capacity(1024);
        out.push_str("# HELP marketfeed_up 1 if process/supervisor is live\n");
        out.push_str("# TYPE marketfeed_up gauge\n");
        out.push_str(&format!("marketfeed_up {live}\n"));
        out.push_str("# HELP marketfeed_ready 1 if readiness policy is satisfied\n");
        out.push_str("# TYPE marketfeed_ready gauge\n");
        out.push_str(&format!("marketfeed_ready {ready}\n"));
        out.push_str("# HELP marketfeed_live_sessions Current live session count\n");
        out.push_str("# TYPE marketfeed_live_sessions gauge\n");
        out.push_str(&format!("marketfeed_live_sessions {sessions}\n"));
        out.push_str("# HELP marketfeed_venues_configured Configured venue count\n");
        out.push_str("# TYPE marketfeed_venues_configured gauge\n");
        out.push_str(&format!("marketfeed_venues_configured {venues}\n"));
        out.push_str("# HELP marketfeed_http_requests_total Health HTTP requests\n");
        out.push_str("# TYPE marketfeed_http_requests_total counter\n");
        out.push_str(&format!("marketfeed_http_requests_total {reqs}\n"));
        out.push_str("# HELP marketfeed_process_start_time_seconds Unix start time\n");
        out.push_str("# TYPE marketfeed_process_start_time_seconds gauge\n");
        out.push_str(&format!(
            "marketfeed_process_start_time_seconds {}\n",
            self.started_unix_secs
        ));
        out.push_str("# HELP marketfeed_recording_enabled 1 if raw recording is enabled\n");
        out.push_str("# TYPE marketfeed_recording_enabled gauge\n");
        out.push_str(&format!("marketfeed_recording_enabled {rec_en}\n"));
        out.push_str("# HELP marketfeed_recording_healthy 1 if recording path is healthy\n");
        out.push_str("# TYPE marketfeed_recording_healthy gauge\n");
        out.push_str(&format!("marketfeed_recording_healthy {rec_ok}\n"));
        out.push_str("# HELP marketfeed_recording_queue_len Pending recording frames\n");
        out.push_str("# TYPE marketfeed_recording_queue_len gauge\n");
        out.push_str(&format!(
            "marketfeed_recording_queue_len {}\n",
            self.recording_queue_len.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP marketfeed_recording_rotations_total Segment rotations\n");
        out.push_str("# TYPE marketfeed_recording_rotations_total counter\n");
        out.push_str(&format!(
            "marketfeed_recording_rotations_total {}\n",
            self.recording_rotations.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP marketfeed_recording_records_written_total Frames written\n");
        out.push_str("# TYPE marketfeed_recording_records_written_total counter\n");
        out.push_str(&format!(
            "marketfeed_recording_records_written_total {}\n",
            self.recording_written.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP marketfeed_recording_frames_dropped_total Raw frames dropped before persistence\n");
        out.push_str("# TYPE marketfeed_recording_frames_dropped_total counter\n");
        out.push_str(&format!(
            "marketfeed_recording_frames_dropped_total {}\n",
            self.recording_dropped.load(Ordering::Relaxed)
        ));
        out.push_str("# HELP marketfeed_disk_pressure 1 if free space below threshold\n");
        out.push_str("# TYPE marketfeed_disk_pressure gauge\n");
        out.push_str(&format!("marketfeed_disk_pressure {disk}\n"));
        out.push_str("# HELP marketfeed_shutdown_draining 1 while graceful shutdown drains\n");
        out.push_str("# TYPE marketfeed_shutdown_draining gauge\n");
        out.push_str(&format!("marketfeed_shutdown_draining {draining}\n"));
        for (id, flag) in &self.venue_flags {
            let v = if flag.load(Ordering::Relaxed) { 1 } else { 0 };
            let id = prometheus_label_value(id);
            out.push_str(&format!("marketfeed_venue_live{{id=\"{id}\"}} {v}\n"));
        }
        // Aggregate + per-venue labeled series (§23.2 scrape).
        let agg = EngineMetrics::new();
        let mut fr_l = String::new();
        let mut fs_l = String::new();
        let mut br_l = String::new();
        let mut bs_l = String::new();
        let mut en_l = String::new();
        let mut ed_l = String::new();
        let mut drop_l = String::new();
        let mut qo_l = String::new();
        let mut abo_l = String::new();
        let mut rc_l = String::new();
        let mut sg_l = String::new();
        let mut cm_l = String::new();
        let mut bi_l = String::new();
        let mut bsr_l = String::new();
        let mut brs_l = String::new();
        let mut pf_l = String::new();
        let mut um_l = String::new();
        let mut occ_l = String::new();
        let mut cap_l = String::new();
        let mut system_occ_l = String::new();
        let mut system_cap_l = String::new();
        let mut valid_books_l = String::new();
        let mut frame_latency_l = String::new();
        let mut parse_latency_l = String::new();
        let mut rest_latency_l = String::new();
        let mut sink_latency_l = String::new();
        for (id, m) in &self.venue_metrics {
            let id = prometheus_label_value(id);
            let fr = m.frames_received.load(Ordering::Relaxed);
            let fs = m.frames_sent.load(Ordering::Relaxed);
            let br = m.bytes_received.load(Ordering::Relaxed);
            let bs = m.bytes_sent.load(Ordering::Relaxed);
            let en = m.events_normalized.load(Ordering::Relaxed);
            let ed = m.events_dispatched.load(Ordering::Relaxed);
            let drop = m.events_dropped.load(Ordering::Relaxed);
            let qo = m.queue_overflows.load(Ordering::Relaxed);
            let rc = m.reconnects.load(Ordering::Relaxed);
            let sg = m.sequence_gaps.load(Ordering::Relaxed);
            let cm = m.checksum_mismatches.load(Ordering::Relaxed);
            let bi = m.book_invalidations.load(Ordering::Relaxed);
            let bsr = m.book_snapshot_rejections.load(Ordering::Relaxed);
            let brs = m.book_resynchronizations.load(Ordering::Relaxed);
            let pf = m.parse_failures.load(Ordering::Relaxed);
            let um = m.unknown_messages.load(Ordering::Relaxed);
            let abo = m.action_buffer_overflows.load(Ordering::Relaxed);
            let occ = m.batch_queue_occupancy.load(Ordering::Relaxed);
            let cap = m.batch_queue_capacity.load(Ordering::Relaxed);
            let system_occ = m.system_queue_occupancy.load(Ordering::Relaxed);
            let system_cap = m.system_queue_capacity.load(Ordering::Relaxed);
            let vb = m.valid_books.load(Ordering::Relaxed);

            agg.frames_received.fetch_add(fr, Ordering::Relaxed);
            agg.frames_sent.fetch_add(fs, Ordering::Relaxed);
            agg.bytes_received.fetch_add(br, Ordering::Relaxed);
            agg.bytes_sent.fetch_add(bs, Ordering::Relaxed);
            agg.events_normalized.fetch_add(en, Ordering::Relaxed);
            agg.events_dispatched.fetch_add(ed, Ordering::Relaxed);
            agg.events_dropped.fetch_add(drop, Ordering::Relaxed);
            agg.queue_overflows.fetch_add(qo, Ordering::Relaxed);
            agg.reconnects.fetch_add(rc, Ordering::Relaxed);
            agg.sequence_gaps.fetch_add(sg, Ordering::Relaxed);
            agg.checksum_mismatches.fetch_add(cm, Ordering::Relaxed);
            agg.book_invalidations.fetch_add(bi, Ordering::Relaxed);
            agg.book_snapshot_rejections
                .fetch_add(bsr, Ordering::Relaxed);
            agg.book_resynchronizations
                .fetch_add(brs, Ordering::Relaxed);
            agg.parse_failures.fetch_add(pf, Ordering::Relaxed);
            agg.unknown_messages.fetch_add(um, Ordering::Relaxed);
            agg.action_buffer_overflows
                .fetch_add(abo, Ordering::Relaxed);
            agg.valid_books.fetch_add(vb, Ordering::Relaxed);
            agg.frame_to_event_latency
                .add_from(&m.frame_to_event_latency);
            agg.parse_duration.add_from(&m.parse_duration);
            agg.rest_latency.add_from(&m.rest_latency);
            agg.sink_write_latency.add_from(&m.sink_write_latency);
            if occ > agg.batch_queue_occupancy.load(Ordering::Relaxed) {
                agg.batch_queue_occupancy.store(occ, Ordering::Relaxed);
            }
            agg.batch_queue_capacity.fetch_add(cap, Ordering::Relaxed);
            if system_occ > agg.system_queue_occupancy.load(Ordering::Relaxed) {
                agg.system_queue_occupancy
                    .store(system_occ, Ordering::Relaxed);
            }
            agg.system_queue_capacity
                .fetch_add(system_cap, Ordering::Relaxed);

            fr_l.push_str(&format!(
                "marketfeed_venue_frames_received_total{{id=\"{id}\"}} {fr}\n"
            ));
            fs_l.push_str(&format!(
                "marketfeed_venue_frames_sent_total{{id=\"{id}\"}} {fs}\n"
            ));
            br_l.push_str(&format!(
                "marketfeed_venue_bytes_received_total{{id=\"{id}\"}} {br}\n"
            ));
            bs_l.push_str(&format!(
                "marketfeed_venue_bytes_sent_total{{id=\"{id}\"}} {bs}\n"
            ));
            en_l.push_str(&format!(
                "marketfeed_venue_events_normalized_total{{id=\"{id}\"}} {en}\n"
            ));
            ed_l.push_str(&format!(
                "marketfeed_venue_events_dispatched_total{{id=\"{id}\"}} {ed}\n"
            ));
            drop_l.push_str(&format!(
                "marketfeed_venue_events_dropped_total{{id=\"{id}\"}} {drop}\n"
            ));
            qo_l.push_str(&format!(
                "marketfeed_venue_queue_overflows_total{{id=\"{id}\"}} {qo}\n"
            ));
            abo_l.push_str(&format!(
                "marketfeed_venue_action_buffer_overflows_total{{id=\"{id}\"}} {abo}\n"
            ));
            rc_l.push_str(&format!(
                "marketfeed_venue_reconnects_total{{id=\"{id}\"}} {rc}\n"
            ));
            sg_l.push_str(&format!(
                "marketfeed_venue_sequence_gaps_total{{id=\"{id}\"}} {sg}\n"
            ));
            cm_l.push_str(&format!(
                "marketfeed_venue_checksum_mismatches_total{{id=\"{id}\"}} {cm}\n"
            ));
            bi_l.push_str(&format!(
                "marketfeed_venue_book_invalidations_total{{id=\"{id}\"}} {bi}\n"
            ));
            bsr_l.push_str(&format!(
                "marketfeed_venue_book_snapshot_rejections_total{{id=\"{id}\"}} {bsr}\n"
            ));
            brs_l.push_str(&format!(
                "marketfeed_venue_book_resynchronizations_total{{id=\"{id}\"}} {brs}\n"
            ));
            pf_l.push_str(&format!(
                "marketfeed_venue_parse_failures_total{{id=\"{id}\"}} {pf}\n"
            ));
            um_l.push_str(&format!(
                "marketfeed_venue_unknown_messages_total{{id=\"{id}\"}} {um}\n"
            ));
            occ_l.push_str(&format!(
                "marketfeed_venue_queue_occupancy{{id=\"{id}\"}} {occ}\n"
            ));
            cap_l.push_str(&format!(
                "marketfeed_venue_batch_queue_capacity{{id=\"{id}\"}} {cap}\n"
            ));
            system_occ_l.push_str(&format!(
                "marketfeed_venue_system_queue_occupancy{{id=\"{id}\"}} {system_occ}\n"
            ));
            system_cap_l.push_str(&format!(
                "marketfeed_venue_system_queue_capacity{{id=\"{id}\"}} {system_cap}\n"
            ));
            valid_books_l.push_str(&format!(
                "marketfeed_venue_valid_books{{id=\"{id}\"}} {vb}\n"
            ));
            frame_latency_l.push_str(&m.frame_to_event_latency.prometheus_series_with_label(
                "marketfeed_venue_frame_to_event_latency_seconds",
                Some(("id", &id)),
            ));
            parse_latency_l.push_str(&m.parse_duration.prometheus_series_with_label(
                "marketfeed_venue_parse_duration_seconds",
                Some(("id", &id)),
            ));
            rest_latency_l.push_str(&m.rest_latency.prometheus_series_with_label(
                "marketfeed_venue_rest_latency_seconds",
                Some(("id", &id)),
            ));
            sink_latency_l.push_str(&m.sink_write_latency.prometheus_series_with_label(
                "marketfeed_venue_sink_write_latency_seconds",
                Some(("id", &id)),
            ));
        }
        out.push_str(&agg.prometheus_text());
        let emit = |out: &mut String, name: &str, help: &str, ty: &str, body: &str| {
            out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {ty}\n{body}"));
        };
        emit(
            &mut out,
            "marketfeed_venue_frames_received_total",
            "Per-venue inbound frames",
            "counter",
            &fr_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_frames_sent_total",
            "Per-venue outbound frames",
            "counter",
            &fs_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_events_dispatched_total",
            "Per-venue dispatched events",
            "counter",
            &ed_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_bytes_received_total",
            "Per-venue inbound payload bytes",
            "counter",
            &br_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_bytes_sent_total",
            "Per-venue outbound payload bytes",
            "counter",
            &bs_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_events_normalized_total",
            "Per-venue normalized market events",
            "counter",
            &en_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_events_dropped_total",
            "Per-venue dropped events",
            "counter",
            &drop_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_queue_overflows_total",
            "Per-venue queue overflow incidents",
            "counter",
            &qo_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_action_buffer_overflows_total",
            "Per-venue ActionBuffer overflow incidents",
            "counter",
            &abo_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_reconnects_total",
            "Per-venue reconnects",
            "counter",
            &rc_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_sequence_gaps_total",
            "Per-venue sequence gaps",
            "counter",
            &sg_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_checksum_mismatches_total",
            "Per-venue checksum mismatches",
            "counter",
            &cm_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_book_invalidations_total",
            "Per-venue book invalidations",
            "counter",
            &bi_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_book_snapshot_rejections_total",
            "Per-venue rejected replacement book snapshots",
            "counter",
            &bsr_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_book_resynchronizations_total",
            "Per-venue book resynchronizations",
            "counter",
            &brs_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_parse_failures_total",
            "Per-venue parse failures",
            "counter",
            &pf_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_unknown_messages_total",
            "Per-venue unknown message events",
            "counter",
            &um_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_queue_occupancy",
            "Per-venue dispatch queue occupancy",
            "gauge",
            &occ_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_batch_queue_capacity",
            "Per-venue dispatch batch queue capacity",
            "gauge",
            &cap_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_system_queue_occupancy",
            "Per-venue dispatch system queue occupancy",
            "gauge",
            &system_occ_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_system_queue_capacity",
            "Per-venue dispatch system queue capacity",
            "gauge",
            &system_cap_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_valid_books",
            "Per-venue valid books",
            "gauge",
            &valid_books_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_frame_to_event_latency_seconds",
            "Per-venue frame ingress to action-apply latency",
            "histogram",
            &frame_latency_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_parse_duration_seconds",
            "Per-venue SessionMachine parse and normalization latency",
            "histogram",
            &parse_latency_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_rest_latency_seconds",
            "Per-venue HTTP request round-trip latency",
            "histogram",
            &rest_latency_l,
        );
        emit(
            &mut out,
            "marketfeed_venue_sink_write_latency_seconds",
            "Per-venue configured sink forward-drain latency",
            "histogram",
            &sink_latency_l,
        );

        out
    }
}

pub fn evaluate_readiness(
    policy: &ReadinessConfig,
    config: &DaemonConfig,
    process_live: bool,
    venue_live: &HashMap<String, bool>,
    live_sessions: u64,
    recording_healthy: bool,
) -> bool {
    if policy.require_running && !process_live {
        return false;
    }
    if policy.require_required_venues {
        for v in &config.venues {
            if v.required && !venue_live.get(&v.id).copied().unwrap_or(false) {
                return false;
            }
        }
    }
    if live_sessions < u64::from(policy.min_live_sessions) {
        return false;
    }
    if policy.require_recording_healthy && !recording_healthy {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_requires_required_venues() {
        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9"
            [[venues]]
            id = "binance-spot"
            adapter = "binance"
            required = true
            symbols = ["BTCUSDT"]
            channels = ["trades"]
        "#,
        )
        .unwrap();
        let mut venues = HashMap::new();
        venues.insert("binance-spot".into(), false);
        assert!(!evaluate_readiness(
            &cfg.readiness,
            &cfg,
            true,
            &venues,
            0,
            true
        ));
        venues.insert("binance-spot".into(), true);
        assert!(evaluate_readiness(
            &cfg.readiness,
            &cfg,
            true,
            &venues,
            0,
            true
        ));
    }

    #[test]
    fn metrics_include_recording_and_session_series() {
        let cfg = DaemonConfig::from_toml_str(
            r#"
            [telemetry]
            bind = "127.0.0.1:9"
            [[venues]]
            id = "syn"
            adapter = "synthetic"
            required = false
            [recording.raw]
            enabled = true
            directory = "./raw"
        "#,
        )
        .unwrap();
        let state = DaemonState::new(cfg);
        let metrics = state.venue_metrics.get("syn").unwrap();
        metrics.frames_received.store(9, Ordering::Relaxed);
        metrics.frames_sent.store(2, Ordering::Relaxed);
        metrics.bytes_received.store(900, Ordering::Relaxed);
        metrics.bytes_sent.store(200, Ordering::Relaxed);
        metrics.events_normalized.store(8, Ordering::Relaxed);
        metrics.events_dispatched.store(7, Ordering::Relaxed);
        metrics.unknown_messages.store(1, Ordering::Relaxed);
        metrics.checksum_mismatches.store(2, Ordering::Relaxed);
        metrics.book_resynchronizations.store(3, Ordering::Relaxed);
        metrics.queue_overflows.store(4, Ordering::Relaxed);
        metrics.action_buffer_overflows.store(5, Ordering::Relaxed);
        metrics.valid_books.store(6, Ordering::Relaxed);
        metrics.batch_queue_occupancy.store(7, Ordering::Relaxed);
        metrics.batch_queue_capacity.store(8, Ordering::Relaxed);
        metrics.system_queue_occupancy.store(9, Ordering::Relaxed);
        metrics.system_queue_capacity.store(10, Ordering::Relaxed);
        metrics.observe_frame_to_event_ns(50_000);
        metrics.observe_parse_duration_ns(80_000);
        metrics.observe_rest_latency_ns(15_000_000);
        metrics.observe_sink_write_ns(400_000);
        let text = state.prometheus_text();
        assert!(text.contains("marketfeed_recording_enabled 1"));
        assert!(text.contains("marketfeed_recording_frames_dropped_total 0"));
        assert!(text.contains("marketfeed_disk_pressure 0"));
        assert!(text.contains("marketfeed_frames_received_total 9"));
        assert!(text.contains("marketfeed_events_dropped_total"));
        assert!(text.contains("marketfeed_batch_queue_occupancy"));
        assert!(text.contains("marketfeed_venue_frames_received_total{id=\"syn\"} 9"));
        assert!(text.contains("marketfeed_venue_frames_sent_total{id=\"syn\"} 2"));
        assert!(text.contains("marketfeed_venue_bytes_received_total{id=\"syn\"} 900"));
        assert!(text.contains("marketfeed_venue_bytes_sent_total{id=\"syn\"} 200"));
        assert!(text.contains("marketfeed_venue_events_normalized_total{id=\"syn\"} 8"));
        assert!(text.contains("marketfeed_venue_events_dispatched_total{id=\"syn\"} 7"));
        assert!(text.contains("marketfeed_venue_unknown_messages_total{id=\"syn\"} 1"));
        assert!(text.contains("marketfeed_venue_checksum_mismatches_total{id=\"syn\"} 2"));
        assert!(text.contains("marketfeed_venue_book_resynchronizations_total{id=\"syn\"} 3"));
        assert!(text.contains("marketfeed_venue_queue_overflows_total{id=\"syn\"} 4"));
        assert!(text.contains("marketfeed_venue_action_buffer_overflows_total{id=\"syn\"} 5"));
        assert!(text.contains("marketfeed_venue_valid_books{id=\"syn\"} 6"));
        assert!(text.contains("marketfeed_venue_queue_occupancy{id=\"syn\"} 7"));
        assert!(text.contains("marketfeed_venue_batch_queue_capacity{id=\"syn\"} 8"));
        assert!(text.contains("marketfeed_venue_system_queue_occupancy{id=\"syn\"} 9"));
        assert!(text.contains("marketfeed_venue_system_queue_capacity{id=\"syn\"} 10"));
        assert!(text.contains("marketfeed_venue_live{id=\"syn\"} 0"));
        assert!(text.contains("# TYPE marketfeed_frame_to_event_latency_seconds histogram"));
        assert!(text.contains("marketfeed_frame_to_event_latency_seconds_count 1"));
        assert!(text.contains("# TYPE marketfeed_parse_duration_seconds histogram"));
        assert!(text.contains("marketfeed_parse_duration_seconds_count 1"));
        assert!(text.contains("# TYPE marketfeed_rest_latency_seconds histogram"));
        assert!(text.contains("marketfeed_rest_latency_seconds_count 1"));
        assert!(text.contains("# TYPE marketfeed_sink_write_latency_seconds histogram"));
        assert!(text.contains("marketfeed_sink_write_latency_seconds_count 1"));
        assert!(text.contains(
            "marketfeed_venue_frame_to_event_latency_seconds_bucket{id=\"syn\",le=\"0.0001\"} 1"
        ));
        assert!(
            text.contains("marketfeed_venue_frame_to_event_latency_seconds_count{id=\"syn\"} 1")
        );
        assert!(text.contains(
            "marketfeed_venue_parse_duration_seconds_bucket{id=\"syn\",le=\"0.0001\"} 1"
        ));
        assert!(text.contains("marketfeed_venue_parse_duration_seconds_count{id=\"syn\"} 1"));
        assert!(
            text.contains(
                "marketfeed_venue_rest_latency_seconds_bucket{id=\"syn\",le=\"0.025\"} 1"
            )
        );
        assert!(text.contains("marketfeed_venue_rest_latency_seconds_count{id=\"syn\"} 1"));
        assert!(text.contains(
            "marketfeed_venue_sink_write_latency_seconds_bucket{id=\"syn\",le=\"0.0005\"} 1"
        ));
        assert!(text.contains("marketfeed_venue_sink_write_latency_seconds_count{id=\"syn\"} 1"));
    }

    #[test]
    fn prometheus_label_values_escape_quotes_backslashes_and_newlines() {
        assert_eq!(
            prometheus_label_value("venue\"\\\nnext"),
            "venue\\\"\\\\\\nnext"
        );
    }

    #[test]
    fn runtime_initialization_fails_closed_when_sink_cannot_open() {
        let dir = std::env::temp_dir().join(format!(
            "marketfeed-sink-init-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let parent_file = dir.join("not-a-directory");
        std::fs::write(&parent_file, b"x").unwrap();
        let sink_path = parent_file.join("events.log");
        let cfg = DaemonConfig::from_toml_str(&format!(
            r#"
            [telemetry]
            bind = "127.0.0.1:0"
            [[sinks]]
            type = "file"
            path = "{}"
            capacity = 8
            overflow = "fail_engine"
            [[venues]]
            id = "synthetic-demo"
            adapter = "synthetic"
            "#,
            sink_path.display()
        ))
        .unwrap();

        let err = DaemonState::try_new(cfg).unwrap_err();
        assert!(err.contains("open file sink"), "{err}");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
