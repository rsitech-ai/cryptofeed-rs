//! Process/session counters for spec §23.2 Prometheus exposition.
//!
//! High-cardinality labels (native symbol) are intentionally omitted; scrapers
//! get venue-agnostic totals. Daemon `/metrics` appends this text.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use marketfeed_model::{InstrumentId, SessionId, SystemEvent};

#[derive(Default)]
struct ValidBooks {
    by_session: HashMap<SessionId, HashSet<InstrumentId>>,
    session_references: HashMap<InstrumentId, usize>,
}

impl ValidBooks {
    fn insert(&mut self, session: SessionId, instrument: InstrumentId) {
        if self
            .by_session
            .entry(session)
            .or_default()
            .insert(instrument)
        {
            *self.session_references.entry(instrument).or_default() += 1;
        }
    }

    fn remove(&mut self, session: SessionId, instrument: InstrumentId) {
        let removed = self
            .by_session
            .get_mut(&session)
            .is_some_and(|books| books.remove(&instrument));
        if removed {
            self.decrement_reference(instrument);
        }
        if self.by_session.get(&session).is_some_and(HashSet::is_empty) {
            self.by_session.remove(&session);
        }
    }

    fn clear_session(&mut self, session: SessionId) {
        if let Some(instruments) = self.by_session.remove(&session) {
            for instrument in instruments {
                self.decrement_reference(instrument);
            }
        }
    }

    fn decrement_reference(&mut self, instrument: InstrumentId) {
        if let Some(references) = self.session_references.get_mut(&instrument) {
            *references -= 1;
            if *references == 0 {
                self.session_references.remove(&instrument);
            }
        }
    }

    fn unique_count(&self) -> usize {
        self.session_references.len()
    }
}

/// Hot-path counters shared with the daemon `/metrics` scrape.
/// Alias kept for call sites that still say SessionMetrics.
pub type SessionMetrics = EngineMetrics;

/// Fixed upper bounds (ns) for hot-path latencies (parse / frame-to-event / sink).
/// Last series in Prometheus output is +Inf.
pub const FAST_LATENCY_BOUNDS_NS: &[u64] = &[
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
];

/// Fixed upper bounds (ns) for REST / network round-trips (1ms … 10s +Inf).
pub const REST_LATENCY_BOUNDS_NS: &[u64] = &[
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    1_000_000_000,
    2_500_000_000,
    5_000_000_000,
    10_000_000_000,
];

/// Lock-free fixed-bucket histogram (no hdrhistogram / prometheus-client dep).
///
/// ponytail: fixed latency buckets only; ceiling = no HDR / dynamic rebucketing;
/// upgrade = `metrics`/`prometheus` Histogram or HDR.
#[derive(Debug)]
pub struct FixedHistogram {
    bounds_ns: &'static [u64],
    buckets: Box<[AtomicU64]>,
    sum_ns: AtomicU64,
    count: AtomicU64,
}

impl FixedHistogram {
    pub fn with_bounds(bounds_ns: &'static [u64]) -> Self {
        let n = bounds_ns.len() + 1;
        Self {
            bounds_ns,
            buckets: (0..n).map(|_| AtomicU64::new(0)).collect(),
            sum_ns: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    pub fn observe_ns(&self, ns: u64) {
        let mut idx = self.bounds_ns.len();
        for (i, &bound) in self.bounds_ns.iter().enumerate() {
            if ns <= bound {
                idx = i;
                break;
            }
        }
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Fold another histogram into `self` (daemon multi-venue aggregate scrape).
    ///
    /// Callers must fold histograms that share the same `bounds_ns`.
    pub fn add_from(&self, other: &Self) {
        debug_assert_eq!(self.bounds_ns, other.bounds_ns);
        debug_assert_eq!(self.buckets.len(), other.buckets.len());
        for (dst, src) in self.buckets.iter().zip(other.buckets.iter()) {
            dst.fetch_add(src.load(Ordering::Relaxed), Ordering::Relaxed);
        }
        self.sum_ns
            .fetch_add(other.sum_ns.load(Ordering::Relaxed), Ordering::Relaxed);
        self.count
            .fetch_add(other.count.load(Ordering::Relaxed), Ordering::Relaxed);
    }

    fn prometheus_text(&self, name: &str, help: &str) -> String {
        let mut out = String::with_capacity(1024);
        out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} histogram\n"));
        out.push_str(&self.prometheus_series_with_label(name, None));
        out
    }

    /// Render histogram samples with one optional, already-escaped label.
    ///
    /// Daemon aggregation uses this to expose venue-comparable latency without
    /// putting high-cardinality symbols or instruments into metric labels.
    pub fn prometheus_series_with_label(&self, name: &str, label: Option<(&str, &str)>) -> String {
        let mut out = String::with_capacity(1024);
        let prefix = label
            .map(|(key, value)| format!("{key}=\"{value}\","))
            .unwrap_or_default();
        let scalar_label = label
            .map(|(key, value)| format!("{{{key}=\"{value}\"}}"))
            .unwrap_or_default();
        let mut cum = 0u64;
        for (i, &bound) in self.bounds_ns.iter().enumerate() {
            cum = cum.saturating_add(self.buckets[i].load(Ordering::Relaxed));
            let le_secs = bound as f64 / 1_000_000_000.0;
            out.push_str(&format!(
                "{name}_bucket{{{prefix}le=\"{le_secs}\"}} {cum}\n"
            ));
        }
        cum = cum.saturating_add(self.buckets[self.bounds_ns.len()].load(Ordering::Relaxed));
        out.push_str(&format!("{name}_bucket{{{prefix}le=\"+Inf\"}} {cum}\n"));
        let sum_secs = self.sum_ns.load(Ordering::Relaxed) as f64 / 1_000_000_000.0;
        out.push_str(&format!(
            "{name}_sum{scalar_label} {sum_secs}\n{name}_count{scalar_label} {}\n",
            self.count.load(Ordering::Relaxed)
        ));
        out
    }
}

pub struct EngineMetrics {
    pub frames_received: AtomicU64,
    pub frames_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub events_normalized: AtomicU64,
    pub events_dispatched: AtomicU64,
    pub parse_failures: AtomicU64,
    pub unknown_messages: AtomicU64,
    pub reconnects: AtomicU64,
    pub sequence_gaps: AtomicU64,
    pub checksum_mismatches: AtomicU64,
    pub book_invalidations: AtomicU64,
    pub book_snapshot_rejections: AtomicU64,
    pub book_resynchronizations: AtomicU64,
    pub queue_overflows: AtomicU64,
    pub events_dropped: AtomicU64,
    pub action_buffer_overflows: AtomicU64,
    pub valid_books: AtomicU64,
    valid_books_by_session: Mutex<ValidBooks>,
    pub batch_queue_occupancy: AtomicU64,
    pub batch_queue_capacity: AtomicU64,
    pub system_queue_occupancy: AtomicU64,
    pub system_queue_capacity: AtomicU64,
    /// Frame ingress to after action apply (approx. ingress-to-dispatch).
    pub frame_to_event_latency: FixedHistogram,
    /// `SessionMachine::on_input` wall time (parse / normalize path).
    pub parse_duration: FixedHistogram,
    /// HTTP transport round-trip for adapter `RequestHttp` (REST snapshot etc.).
    pub rest_latency: FixedHistogram,
    /// Sink `push_batch` / `push_system` drain wall time via `consume_dispatch`.
    pub sink_write_latency: FixedHistogram,
}

impl Default for EngineMetrics {
    fn default() -> Self {
        Self {
            frames_received: AtomicU64::new(0),
            frames_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            events_normalized: AtomicU64::new(0),
            events_dispatched: AtomicU64::new(0),
            parse_failures: AtomicU64::new(0),
            unknown_messages: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
            sequence_gaps: AtomicU64::new(0),
            checksum_mismatches: AtomicU64::new(0),
            book_invalidations: AtomicU64::new(0),
            book_snapshot_rejections: AtomicU64::new(0),
            book_resynchronizations: AtomicU64::new(0),
            queue_overflows: AtomicU64::new(0),
            events_dropped: AtomicU64::new(0),
            action_buffer_overflows: AtomicU64::new(0),
            valid_books: AtomicU64::new(0),
            valid_books_by_session: Mutex::new(ValidBooks::default()),
            batch_queue_occupancy: AtomicU64::new(0),
            batch_queue_capacity: AtomicU64::new(0),
            system_queue_occupancy: AtomicU64::new(0),
            system_queue_capacity: AtomicU64::new(0),
            frame_to_event_latency: FixedHistogram::with_bounds(FAST_LATENCY_BOUNDS_NS),
            parse_duration: FixedHistogram::with_bounds(FAST_LATENCY_BOUNDS_NS),
            rest_latency: FixedHistogram::with_bounds(REST_LATENCY_BOUNDS_NS),
            sink_write_latency: FixedHistogram::with_bounds(FAST_LATENCY_BOUNDS_NS),
        }
    }
}

impl std::fmt::Debug for EngineMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EngineMetrics { .. }")
    }
}

impl EngineMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    pub fn record_frame_received(&self, nbytes: usize) {
        Self::add(&self.frames_received, 1);
        Self::add(&self.bytes_received, nbytes as u64);
    }

    pub fn record_frame_sent(&self, nbytes: usize) {
        Self::add(&self.frames_sent, 1);
        Self::add(&self.bytes_sent, nbytes as u64);
    }

    pub fn record_batch_dispatched(&self) {
        Self::add(&self.events_dispatched, 1);
    }

    pub fn record_reconnect(&self) {
        Self::add(&self.reconnects, 1);
    }

    pub fn record_queue_overflow(&self) {
        Self::add(&self.queue_overflows, 1);
    }

    pub fn record_action_buffer_overflow(&self, count: u64) {
        Self::add(&self.action_buffer_overflows, count);
    }

    pub fn record_events_dropped(&self, count: u64) {
        Self::add(&self.events_dropped, count);
    }

    /// Observe frame-to-event latency (ns) into §23.2 fixed buckets.
    pub fn observe_frame_to_event_ns(&self, ns: u64) {
        self.frame_to_event_latency.observe_ns(ns);
    }

    /// Observe parse / `on_input` duration (ns).
    pub fn observe_parse_duration_ns(&self, ns: u64) {
        self.parse_duration.observe_ns(ns);
    }

    /// Observe REST / HTTP request latency (ns).
    pub fn observe_rest_latency_ns(&self, ns: u64) {
        self.rest_latency.observe_ns(ns);
    }

    /// Observe sink write / forward drain latency (ns).
    pub fn observe_sink_write_ns(&self, ns: u64) {
        self.sink_write_latency.observe_ns(ns);
    }

    pub fn set_queue_gauges(
        &self,
        batch_occupancy: usize,
        batch_capacity: usize,
        system_occupancy: usize,
        system_capacity: usize,
    ) {
        self.batch_queue_occupancy
            .store(batch_occupancy as u64, Ordering::Relaxed);
        self.batch_queue_capacity
            .store(batch_capacity as u64, Ordering::Relaxed);
        self.system_queue_occupancy
            .store(system_occupancy as u64, Ordering::Relaxed);
        self.system_queue_capacity
            .store(system_capacity as u64, Ordering::Relaxed);
    }

    pub fn observe_system(&self, ev: &SystemEvent) {
        self.observe_system_for_session(SessionId(0), ev);
    }

    pub fn observe_system_for_session(&self, session: SessionId, ev: &SystemEvent) {
        match ev {
            SystemEvent::ParseError { .. } => Self::add(&self.parse_failures, 1),
            SystemEvent::UnknownMessage { .. } => Self::add(&self.unknown_messages, 1),
            SystemEvent::SequenceGap { .. } => Self::add(&self.sequence_gaps, 1),
            SystemEvent::ChecksumMismatch { .. } => Self::add(&self.checksum_mismatches, 1),
            SystemEvent::BookInvalidated { instrument, .. } => {
                Self::add(&self.book_invalidations, 1);
                let mut valid = self.valid_books_by_session.lock().expect("valid book set");
                valid.remove(session, *instrument);
                self.valid_books
                    .store(valid.unique_count() as u64, Ordering::Relaxed);
            }
            SystemEvent::BookSnapshotRejected { .. } => {
                Self::add(&self.book_snapshot_rejections, 1);
            }
            SystemEvent::BookResynchronized { instrument } => {
                Self::add(&self.book_resynchronizations, 1);
                let mut valid = self.valid_books_by_session.lock().expect("valid book set");
                valid.insert(session, *instrument);
                self.valid_books
                    .store(valid.unique_count() as u64, Ordering::Relaxed);
            }
            SystemEvent::EventsDropped { count, .. } => Self::add(&self.events_dropped, *count),
            SystemEvent::EngineStateChanged { .. }
            | SystemEvent::ConnectionStateChanged { .. }
            | SystemEvent::SubscriptionStateChanged { .. }
            | SystemEvent::InstrumentCatalogUpdated { .. }
            | SystemEvent::HeartbeatMissed
            | SystemEvent::RateLimited
            | SystemEvent::QueuePressure { .. }
            | SystemEvent::RecordingRotated
            | SystemEvent::DiskPressure
            | SystemEvent::ClockJump { .. }
            | SystemEvent::SinkStateChanged { .. }
            | SystemEvent::ShutdownStarted
            | SystemEvent::ShutdownCompleted => {}
        }
    }

    /// Remove every book owned by a disconnected session before reconnection.
    ///
    /// Adapters invalidate their local books on transport loss, but they are
    /// not required to emit one `BookInvalidated` event per configured symbol.
    /// Clearing here prevents stale pre-reconnect books from satisfying daemon
    /// readiness while replacement snapshots are still arriving.
    pub fn clear_valid_books_for_session(&self, session: SessionId) {
        let mut valid = self.valid_books_by_session.lock().expect("valid book set");
        valid.clear_session(session);
        self.valid_books
            .store(valid.unique_count() as u64, Ordering::Relaxed);
    }

    pub fn prometheus_text(&self) -> String {
        let g = |name: &str, help: &str, v: u64| {
            format!("# HELP {name} {help}\n# TYPE {name} gauge\n{name} {v}\n")
        };
        let c = |name: &str, help: &str, v: u64| {
            format!("# HELP {name} {help}\n# TYPE {name} counter\n{name} {v}\n")
        };
        let mut out = String::with_capacity(8192);
        out.push_str(&c(
            "marketfeed_frames_received_total",
            "Inbound frames",
            self.frames_received.load(Ordering::Relaxed),
        ));
        out.push_str(&c(
            "marketfeed_frames_sent_total",
            "Outbound frames",
            self.frames_sent.load(Ordering::Relaxed),
        ));
        out.push_str(&c(
            "marketfeed_bytes_received_total",
            "Inbound payload bytes",
            self.bytes_received.load(Ordering::Relaxed),
        ));
        out.push_str(&c(
            "marketfeed_bytes_sent_total",
            "Outbound payload bytes",
            self.bytes_sent.load(Ordering::Relaxed),
        ));
        out.push_str(&c(
            "marketfeed_events_normalized_total",
            "Normalized market events",
            self.events_normalized.load(Ordering::Relaxed),
        ));
        out.push_str(&c(
            "marketfeed_events_dispatched_total",
            "Accepted dispatch batches",
            self.events_dispatched.load(Ordering::Relaxed),
        ));
        out.push_str(&c(
            "marketfeed_parse_failures_total",
            "Parse error system events",
            self.parse_failures.load(Ordering::Relaxed),
        ));
        out.push_str(&c(
            "marketfeed_unknown_messages_total",
            "Unknown message system events",
            self.unknown_messages.load(Ordering::Relaxed),
        ));
        out.push_str(&c(
            "marketfeed_reconnects_total",
            "Session reconnects",
            self.reconnects.load(Ordering::Relaxed),
        ));
        out.push_str(&c(
            "marketfeed_sequence_gaps_total",
            "Sequence gap system events",
            self.sequence_gaps.load(Ordering::Relaxed),
        ));
        out.push_str(&c(
            "marketfeed_checksum_mismatches_total",
            "Checksum mismatch system events",
            self.checksum_mismatches.load(Ordering::Relaxed),
        ));
        out.push_str(&c(
            "marketfeed_book_invalidations_total",
            "Book invalidation events",
            self.book_invalidations.load(Ordering::Relaxed),
        ));
        out.push_str(&c(
            "marketfeed_book_snapshot_rejections_total",
            "Rejected replacement book snapshots",
            self.book_snapshot_rejections.load(Ordering::Relaxed),
        ));
        out.push_str(&c(
            "marketfeed_book_resynchronizations_total",
            "Book resynchronization events",
            self.book_resynchronizations.load(Ordering::Relaxed),
        ));
        out.push_str(&c(
            "marketfeed_queue_overflows_total",
            "Queue overflow incidents",
            self.queue_overflows.load(Ordering::Relaxed),
        ));
        out.push_str(&c(
            "marketfeed_events_dropped_total",
            "Dropped events/batches by overflow policy",
            self.events_dropped.load(Ordering::Relaxed),
        ));
        out.push_str(&c(
            "marketfeed_action_buffer_overflows_total",
            "Actions dropped because ActionBuffer was full",
            self.action_buffer_overflows.load(Ordering::Relaxed),
        ));
        out.push_str(&self.frame_to_event_latency.prometheus_text(
            "marketfeed_frame_to_event_latency_seconds",
            "Frame ingress to action-apply latency (approx. ingress-to-dispatch; §23.2)",
        ));
        out.push_str(&self.parse_duration.prometheus_text(
            "marketfeed_parse_duration_seconds",
            "SessionMachine on_input duration (parse/normalize path; §23.2)",
        ));
        out.push_str(&self.rest_latency.prometheus_text(
            "marketfeed_rest_latency_seconds",
            "HTTP RequestHttp round-trip latency (REST snapshot etc.; §23.2)",
        ));
        out.push_str(&self.sink_write_latency.prometheus_text(
            "marketfeed_sink_write_latency_seconds",
            "EventSink forward drain latency (consume_dispatch; §23.2)",
        ));
        out.push_str(&g(
            "marketfeed_valid_books",
            "Valid books gauge (resync - invalidate)",
            self.valid_books.load(Ordering::Relaxed),
        ));
        out.push_str(&g(
            "marketfeed_batch_queue_occupancy",
            "Dispatch batch queue occupancy",
            self.batch_queue_occupancy.load(Ordering::Relaxed),
        ));
        out.push_str(&g(
            "marketfeed_batch_queue_capacity",
            "Dispatch batch queue capacity",
            self.batch_queue_capacity.load(Ordering::Relaxed),
        ));
        out.push_str(&g(
            "marketfeed_system_queue_occupancy",
            "Dispatch system queue occupancy",
            self.system_queue_occupancy.load(Ordering::Relaxed),
        ));
        out.push_str(&g(
            "marketfeed_system_queue_capacity",
            "Dispatch system queue capacity",
            self.system_queue_capacity.load(Ordering::Relaxed),
        ));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use marketfeed_model::{InstrumentId, SystemEvent};

    #[test]
    fn system_events_increment_counters() {
        let m = EngineMetrics::new();
        m.observe_system(&SystemEvent::ParseError {
            detail: "bad".into(),
        });
        m.observe_system(&SystemEvent::SequenceGap {
            expected: 1,
            actual: 3,
        });
        m.observe_system(&SystemEvent::ChecksumMismatch {
            detail: "crc".into(),
        });
        m.observe_system(&SystemEvent::BookInvalidated {
            instrument: InstrumentId(1),
            reason: "gap".into(),
        });
        m.observe_system(&SystemEvent::BookSnapshotRejected {
            instrument: InstrumentId(1),
            reason: "crossed replacement".into(),
        });
        m.observe_system(&SystemEvent::BookResynchronized {
            instrument: InstrumentId(1),
        });
        assert_eq!(m.parse_failures.load(Ordering::Relaxed), 1);
        assert_eq!(m.sequence_gaps.load(Ordering::Relaxed), 1);
        assert_eq!(m.checksum_mismatches.load(Ordering::Relaxed), 1);
        assert_eq!(m.book_invalidations.load(Ordering::Relaxed), 1);
        assert_eq!(m.book_snapshot_rejections.load(Ordering::Relaxed), 1);
        assert_eq!(m.book_resynchronizations.load(Ordering::Relaxed), 1);
        assert_eq!(m.valid_books.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn valid_book_gauge_tracks_unique_instruments() {
        let m = EngineMetrics::new();
        let resynchronized = SystemEvent::BookResynchronized {
            instrument: InstrumentId(7),
        };
        m.observe_system(&resynchronized);
        m.observe_system(&resynchronized);
        assert_eq!(m.valid_books.load(Ordering::Relaxed), 1);

        m.observe_system(&SystemEvent::BookResynchronized {
            instrument: InstrumentId(8),
        });
        assert_eq!(m.valid_books.load(Ordering::Relaxed), 2);

        let invalidated = SystemEvent::BookInvalidated {
            instrument: InstrumentId(7),
            reason: "gap".into(),
        };
        m.observe_system(&invalidated);
        m.observe_system(&invalidated);
        assert_eq!(m.valid_books.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn disconnect_clears_only_the_sessions_valid_books() {
        let m = EngineMetrics::new();
        let shared = SystemEvent::BookResynchronized {
            instrument: InstrumentId(7),
        };
        m.observe_system_for_session(SessionId(1), &shared);
        m.observe_system_for_session(SessionId(2), &shared);
        m.observe_system_for_session(
            SessionId(1),
            &SystemEvent::BookResynchronized {
                instrument: InstrumentId(8),
            },
        );
        assert_eq!(m.valid_books.load(Ordering::Relaxed), 2);

        m.clear_valid_books_for_session(SessionId(1));
        assert_eq!(
            m.valid_books.load(Ordering::Relaxed),
            1,
            "the shared instrument remains valid through session 2"
        );

        m.clear_valid_books_for_session(SessionId(2));
        assert_eq!(m.valid_books.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn frame_to_event_histogram_buckets_export() {
        let m = EngineMetrics::new();
        m.observe_frame_to_event_ns(50_000);
        m.observe_frame_to_event_ns(2_000_000);
        m.observe_frame_to_event_ns(500_000_000);
        assert_eq!(m.frame_to_event_latency.count(), 3);
        let text = m.prometheus_text();
        assert!(text.contains("# TYPE marketfeed_frame_to_event_latency_seconds histogram"));
        assert!(text.contains("marketfeed_frame_to_event_latency_seconds_bucket{le=\"0.0001\"} 1"));
        assert!(text.contains("marketfeed_frame_to_event_latency_seconds_bucket{le=\"+Inf\"} 3"));
        assert!(text.contains("marketfeed_frame_to_event_latency_seconds_count 3"));
    }

    #[test]
    fn parse_rest_sink_histograms_export() {
        let m = EngineMetrics::new();
        m.observe_parse_duration_ns(80_000);
        m.observe_rest_latency_ns(15_000_000); // 15ms
        m.observe_sink_write_ns(400_000);
        assert_eq!(m.parse_duration.count(), 1);
        assert_eq!(m.rest_latency.count(), 1);
        assert_eq!(m.sink_write_latency.count(), 1);
        let text = m.prometheus_text();
        assert!(text.contains("# TYPE marketfeed_parse_duration_seconds histogram"));
        assert!(text.contains("marketfeed_parse_duration_seconds_bucket{le=\"0.0001\"} 1"));
        assert!(text.contains("marketfeed_parse_duration_seconds_count 1"));
        assert!(text.contains("# TYPE marketfeed_rest_latency_seconds histogram"));
        assert!(text.contains("marketfeed_rest_latency_seconds_bucket{le=\"0.025\"} 1"));
        assert!(text.contains("marketfeed_rest_latency_seconds_count 1"));
        assert!(text.contains("# TYPE marketfeed_sink_write_latency_seconds histogram"));
        assert!(text.contains("marketfeed_sink_write_latency_seconds_bucket{le=\"0.0005\"} 1"));
        assert!(text.contains("marketfeed_sink_write_latency_seconds_count 1"));
        // Stub counter removed — do not reintroduce sample-count-only series.
        assert!(!text.contains("marketfeed_parse_duration_samples_total"));
    }

    #[test]
    fn histogram_add_from_folds_counts() {
        let a = EngineMetrics::new();
        let b = EngineMetrics::new();
        a.observe_parse_duration_ns(50_000);
        b.observe_parse_duration_ns(50_000);
        a.parse_duration.add_from(&b.parse_duration);
        assert_eq!(a.parse_duration.count(), 2);
    }
}
