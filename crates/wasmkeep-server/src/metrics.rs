//! Process-wide metrics in the Prometheus text format, served by
//! `serve --metrics-listen` (off by default). Labels are bounded: class
//! names, event kinds and fixed reasons, never actor names.
//!
//! Counters and histograms are recorded where things happen; gauges (resident
//! actors, open sockets) are read from the service when scraped.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Duration;

/// Upper bounds of histogram buckets, in seconds.
const BUCKETS: [f64; 14] = [
    0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1.0, 5.0, 30.0,
];

/// A cumulative-bucket histogram of durations.
#[derive(Debug, Default)]
pub(crate) struct Histogram {
    buckets: [AtomicU64; BUCKETS.len()],
    count: AtomicU64,
    sum_ns: AtomicU64,
}

impl Histogram {
    pub(crate) fn observe(&self, d: Duration) {
        let secs = d.as_secs_f64();
        for (bound, bucket) in BUCKETS.iter().zip(&self.buckets) {
            if secs <= *bound {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        let ns = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
    }

    fn render(&self, out: &mut String, name: &str, labels: &str) {
        let sep = if labels.is_empty() { "" } else { "," };
        for (bound, bucket) in BUCKETS.iter().zip(&self.buckets) {
            let n = bucket.load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{{labels}{sep}le=\"{bound}\"}} {n}");
        }
        let count = self.count.load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{{labels}{sep}le=\"+Inf\"}} {count}");
        let sum = Duration::from_nanos(self.sum_ns.load(Ordering::Relaxed)).as_secs_f64();
        let braces = |s: &str| {
            if s.is_empty() {
                String::new()
            } else {
                format!("{{{s}}}")
            }
        };
        let _ = writeln!(out, "{name}_sum{} {sum}", braces(labels));
        let _ = writeln!(out, "{name}_count{} {count}", braces(labels));
    }
}

/// Values keyed by their label set (rendered `k="v",...`, sorted).
#[derive(Debug)]
pub(crate) struct Family<T> {
    entries: RwLock<BTreeMap<String, Arc<T>>>,
}

impl<T> Default for Family<T> {
    fn default() -> Self {
        Self {
            entries: RwLock::new(BTreeMap::new()),
        }
    }
}

impl<T: Default> Family<T> {
    /// The value for these labels, created on first use.
    pub(crate) fn get(&self, labels: &[(&str, &str)]) -> Arc<T> {
        let key = render_labels(labels);
        if let Some(v) = self
            .entries
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&key)
        {
            return Arc::clone(v);
        }
        let mut entries = self.entries.write().unwrap_or_else(|p| p.into_inner());
        Arc::clone(entries.entry(key).or_default())
    }

    fn each(&self, mut f: impl FnMut(&str, &T)) {
        for (labels, v) in self
            .entries
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
        {
            f(labels, v);
        }
    }
}

fn render_labels(labels: &[(&str, &str)]) -> String {
    let mut out = String::new();
    for (i, (k, v)) in labels.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(k);
        out.push_str("=\"");
        for c in v.chars() {
            match c {
                '\\' => out.push_str("\\\\"),
                '"' => out.push_str("\\\""),
                '\n' => out.push_str("\\n"),
                c => out.push(c),
            }
        }
        out.push('"');
    }
    out
}

/// Everything WasmKeep counts.
#[derive(Debug, Default)]
pub(crate) struct Metrics {
    /// Events run, by class, kind (`request`, `alarm`, `websocket`, `init`)
    /// and outcome (`ok`, `failed`).
    pub(crate) events: Family<AtomicU64>,
    /// How long events ran, by class.
    pub(crate) event_seconds: Family<Histogram>,
    /// Events stopped by the host, by reason (`wall`, `lease`).
    pub(crate) cuts: Family<AtomicU64>,
    /// Instantiating an actor's component (wake or first request).
    pub(crate) instantiate_seconds: Histogram,
    /// Actors brought into memory (created or woken).
    pub(crate) activations: AtomicU64,
    /// Actors that left memory (hibernated, evicted, deleted or shut down).
    pub(crate) hibernations: AtomicU64,
    /// Requests and calls not run because their sender had gone while they
    /// waited.
    pub(crate) requests_abandoned: AtomicU64,
    /// Requests and calls sent to the node that owns the actor, by kind
    /// (`request`, `websocket`, `call`).
    pub(crate) forwards: Family<AtomicU64>,
    /// `exec-durable`, from the call to durable.
    pub(crate) durable_write_seconds: Histogram,
    /// One listing of the cluster's heartbeats, with the view and pins it
    /// refreshes (every heartbeat period, and early on an unknown pin).
    /// Fences run apart.
    pub(crate) listing_seconds: Histogram,
    /// One object a restore read from the bucket (a segment or a
    /// snapshot), from its request to its last byte, resends included.
    pub(crate) restore_read_seconds: Histogram,
    /// Node lease writes that reached the bucket (ADR 0020): the heartbeat,
    /// one bucket write each.
    pub(crate) node_lease_writes: AtomicU64,
    /// Node lease writes that failed or timed out: with the defaults, two
    /// in a row (a write every 3 s, valid for 8 s) and the lease runs out.
    pub(crate) node_lease_failures: AtomicU64,
    /// Fences this node wrote over another node's lease (ADR 0020): that
    /// node stopped writing it for longer than its ttl plus grace.
    pub(crate) node_fences: AtomicU64,
    /// Alarm deliveries rescheduled after mailbox capacity or queue refusal.
    pub(crate) alarm_admission_retries: AtomicU64,
    /// Requests to the bucket, by operation (`get`, `head`, `put`, `delete`,
    /// `list`, `copy`, `rename`, `multipart`) and status (`ok`,
    /// `not_found`, `precondition`, `error`).
    pub(crate) bucket_requests: Family<AtomicU64>,
    /// Actors this node acquired whose ownership record still named another
    /// node, found free because that node's process was fenced, released or
    /// replaced (see `Hold::previous`, ADR 0020).
    pub(crate) failovers: AtomicU64,
    /// Lines guests printed that reached the node's log.
    pub(crate) guest_log_lines: AtomicU64,
    /// Lines guests printed that were dropped (over the line rate).
    pub(crate) guest_log_dropped: AtomicU64,
    /// Objects of actors' history deleted by garbage collection (ADR 0012).
    pub(crate) history_collected: AtomicU64,
    /// Garbage-collection passes that failed (a bucket error); the next
    /// snapshot tries again.
    pub(crate) history_collect_failures: AtomicU64,
}

/// The process's metrics.
pub(crate) static METRICS: LazyLock<Metrics> = LazyLock::new(Metrics::default);

/// Adds one to a counter.
pub(crate) fn inc(c: &AtomicU64) {
    c.fetch_add(1, Ordering::Relaxed);
}

/// Gauges read from the service at scrape time.
#[derive(Debug, Clone, Copy, Default)]
pub struct Gauges {
    /// Actors resident in memory.
    pub resident: usize,
    /// Open WebSockets.
    pub websockets: usize,
    /// Actor classes served.
    pub classes: usize,
    /// Why the node is not ready, if it is not (the public readiness check
    /// only says "not ready").
    pub not_ready: Option<&'static str>,
}

/// The metrics page: everything recorded so far plus `gauges`, and the
/// runtime's CPU-budget traps.
pub fn render(gauges: Gauges) -> String {
    let m = &*METRICS;
    let mut out = String::new();
    let head = |out: &mut String, name: &str, kind: &str, help: &str| {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} {kind}");
    };
    let counter = |out: &mut String, name: &str, help: &str, v: u64| {
        head(out, name, "counter", help);
        let _ = writeln!(out, "{name} {v}");
    };
    let gauge = |out: &mut String, name: &str, help: &str, v: usize| {
        head(out, name, "gauge", help);
        let _ = writeln!(out, "{name} {v}");
    };
    let family = |out: &mut String, name: &str, help: &str, f: &Family<AtomicU64>| {
        head(out, name, "counter", help);
        f.each(|labels, v| {
            let _ = writeln!(out, "{name}{{{labels}}} {}", v.load(Ordering::Relaxed));
        });
    };

    gauge(
        &mut out,
        "wasmkeep_actors_resident",
        "Actors resident in memory.",
        gauges.resident,
    );
    gauge(
        &mut out,
        "wasmkeep_websockets_open",
        "Open WebSockets.",
        gauges.websockets,
    );
    gauge(
        &mut out,
        "wasmkeep_ready",
        "1 when /_wasmkeep/ready answers 200.",
        usize::from(gauges.not_ready.is_none()),
    );
    head(
        &mut out,
        "wasmkeep_not_ready",
        "gauge",
        "Why the node is not ready (one series, value 1, while it is not).",
    );
    if let Some(reason) = gauges.not_ready {
        let _ = writeln!(
            out,
            "wasmkeep_not_ready{{{}}} 1",
            render_labels(&[("reason", reason)])
        );
    }
    gauge(
        &mut out,
        "wasmkeep_classes",
        "Actor classes served.",
        gauges.classes,
    );
    family(
        &mut out,
        "wasmkeep_events_total",
        "Events run, by class, kind and outcome.",
        &m.events,
    );
    head(
        &mut out,
        "wasmkeep_event_duration_seconds",
        "histogram",
        "How long events ran, by class.",
    );
    m.event_seconds
        .each(|labels, h| h.render(&mut out, "wasmkeep_event_duration_seconds", labels));
    family(
        &mut out,
        "wasmkeep_event_cuts_total",
        "Events stopped by the host, by reason (wall clock, lost lease).",
        &m.cuts,
    );
    counter(
        &mut out,
        "wasmkeep_cpu_budget_exceeded_total",
        "Guest calls stopped for using their CPU budget.",
        wasmkeep_runtime::cpu_budget_exceeded_total(),
    );
    head(
        &mut out,
        "wasmkeep_instantiate_seconds",
        "histogram",
        "Instantiating an actor's component.",
    );
    m.instantiate_seconds
        .render(&mut out, "wasmkeep_instantiate_seconds", "");
    counter(
        &mut out,
        "wasmkeep_wakes_total",
        "Actors brought into memory (created or woken).",
        m.activations.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_requests_abandoned_total",
        "Requests and actor calls not run because their sender had gone while they waited in the actor's queue.",
        m.requests_abandoned.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_hibernations_total",
        "Actors that left memory (idle, evicted, deleted or shut down).",
        m.hibernations.load(Ordering::Relaxed),
    );
    family(
        &mut out,
        "wasmkeep_forwards_total",
        "Requests and calls sent to the node owning the actor, by kind.",
        &m.forwards,
    );
    head(
        &mut out,
        "wasmkeep_durable_write_seconds",
        "histogram",
        "exec-durable, from the call until the write is durable.",
    );
    m.durable_write_seconds
        .render(&mut out, "wasmkeep_durable_write_seconds", "");
    head(
        &mut out,
        "wasmkeep_listing_seconds",
        "histogram",
        "One listing of the cluster's heartbeats, with the view and pins it refreshes.",
    );
    m.listing_seconds
        .render(&mut out, "wasmkeep_listing_seconds", "");
    head(
        &mut out,
        "wasmkeep_restore_read_seconds",
        "histogram",
        "One object a restore read from the bucket, from its request to its last byte, resends included.",
    );
    m.restore_read_seconds
        .render(&mut out, "wasmkeep_restore_read_seconds", "");
    counter(
        &mut out,
        "wasmkeep_guest_log_lines_total",
        "Lines guests printed that reached the node's log.",
        m.guest_log_lines.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_guest_log_dropped_total",
        "Lines guests printed that were dropped over the line rate.",
        m.guest_log_dropped.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_node_lease_writes_total",
        "Node lease (heartbeat) writes that reached the bucket.",
        m.node_lease_writes.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_node_lease_failures_total",
        "Node lease (heartbeat) writes that failed or timed out.",
        m.node_lease_failures.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_node_fences_total",
        "Fences this node wrote over a node that stopped renewing its lease.",
        m.node_fences.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_alarm_admission_retries_total",
        "Alarm deliveries rescheduled after mailbox admission refusal.",
        m.alarm_admission_retries.load(Ordering::Relaxed),
    );
    family(
        &mut out,
        "wasmkeep_bucket_requests_total",
        "Requests to the bucket, by operation and status.",
        &m.bucket_requests,
    );
    counter(
        &mut out,
        "wasmkeep_failovers_total",
        "Actor leases taken from another node whose lease expired without being released.",
        m.failovers.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_history_collected_total",
        "Objects of actors' history deleted from the bucket by garbage collection.",
        m.history_collected.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_history_collect_failures_total",
        "Garbage-collection passes that failed; the next snapshot tries again.",
        m.history_collect_failures.load(Ordering::Relaxed),
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histograms_are_cumulative_and_labels_escaped() {
        let h = Histogram::default();
        h.observe(Duration::from_micros(50));
        h.observe(Duration::from_millis(3));
        h.observe(Duration::from_secs(60));
        let mut out = String::new();
        h.render(&mut out, "t", "class=\"a\"");
        assert!(
            out.contains("t_bucket{class=\"a\",le=\"0.0001\"} 1\n"),
            "{out}"
        );
        assert!(
            out.contains("t_bucket{class=\"a\",le=\"0.005\"} 2\n"),
            "{out}"
        );
        assert!(out.contains("t_bucket{class=\"a\",le=\"30\"} 2\n"), "{out}");
        assert!(
            out.contains("t_bucket{class=\"a\",le=\"+Inf\"} 3\n"),
            "{out}"
        );
        assert!(out.contains("t_count{class=\"a\"} 3\n"), "{out}");
        assert_eq!(
            render_labels(&[("class", "a\"b\\c\nd")]),
            "class=\"a\\\"b\\\\c\\nd\""
        );
    }

    #[test]
    fn readiness_and_its_reason_are_on_the_page() {
        let page = render(Gauges::default());
        assert!(page.contains("\nwasmkeep_ready 1\n"), "{page}");
        assert!(!page.contains("wasmkeep_not_ready{"), "{page}");
        let page = render(Gauges {
            not_ready: Some("bucket unreachable (node lease not renewed)"),
            ..Gauges::default()
        });
        assert!(page.contains("\nwasmkeep_ready 0\n"), "{page}");
        assert!(
            page.contains(
                "wasmkeep_not_ready{reason=\"bucket unreachable (node lease not renewed)\"} 1\n"
            ),
            "{page}"
        );
    }

    #[test]
    fn alarm_admission_retries_are_exposed_without_actor_labels() {
        let page = render(Gauges::default());
        assert!(page.contains("# TYPE wasmkeep_alarm_admission_retries_total counter\n"));
        assert!(page.contains("\nwasmkeep_alarm_admission_retries_total "));
        assert!(!page.contains("wasmkeep_alarm_admission_retries_total{"));
    }

    #[test]
    fn a_family_keeps_one_value_per_label_set() {
        let f: Family<AtomicU64> = Family::default();
        inc(&f.get(&[("class", "a"), ("outcome", "ok")]));
        inc(&f.get(&[("class", "a"), ("outcome", "ok")]));
        inc(&f.get(&[("class", "b"), ("outcome", "ok")]));
        let mut seen = Vec::new();
        f.each(|labels, v| seen.push((labels.to_owned(), v.load(Ordering::Relaxed))));
        assert_eq!(
            seen,
            [
                ("class=\"a\",outcome=\"ok\"".to_owned(), 2),
                ("class=\"b\",outcome=\"ok\"".to_owned(), 1)
            ]
        );
    }

    /// The Grafana dashboard in `deploy/grafana` queries only metrics this
    /// page has.
    #[test]
    fn the_grafana_dashboard_uses_only_metrics_that_exist() {
        fn exprs(v: &serde_json::Value, out: &mut Vec<String>) {
            match v {
                serde_json::Value::Object(m) => {
                    for (k, v) in m {
                        match (k.as_str(), v) {
                            ("expr" | "definition", serde_json::Value::String(s)) => {
                                out.push(s.clone());
                            }
                            _ => exprs(v, out),
                        }
                    }
                }
                serde_json::Value::Array(a) => a.iter().for_each(|v| exprs(v, out)),
                _ => {}
            }
        }
        let dashboard: serde_json::Value =
            serde_json::from_str(include_str!("../../../deploy/grafana/wasmkeep.json"))
                .expect("the dashboard is JSON");
        let mut found = Vec::new();
        exprs(&dashboard, &mut found);
        assert!(found.len() > 10, "{found:?}");
        let page = render(Gauges::default());
        let mut names = std::collections::BTreeSet::new();
        for expr in &found {
            let mut rest = expr.as_str();
            while let Some(at) = rest.find("wasmkeep_") {
                let name: String = rest[at..]
                    .chars()
                    .take_while(|c| c.is_ascii_lowercase() || *c == '_')
                    .collect();
                rest = &rest[at + name.len()..];
                names.insert(name);
            }
        }
        for name in &names {
            let base = ["_bucket", "_sum", "_count"]
                .iter()
                .find_map(|s| name.strip_suffix(s))
                .filter(|b| page.contains(&format!("# TYPE {b} histogram\n")))
                .unwrap_or(name);
            assert!(
                page.contains(&format!("# TYPE {base} ")),
                "the dashboard queries {name}, which the metrics page does not have"
            );
        }
    }
}
