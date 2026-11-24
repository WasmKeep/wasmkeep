//! Process-wide metrics in the Prometheus text format, served by
//! `serve --metrics-listen` (off by default). Labels are bounded: the
//! node's tenant and app, class names, event kinds and fixed reasons, never
//! actor names.
//!
//! Series with a `class` label also carry `tenant` and `app`: those of the
//! node the page is about ([`Identity`]), added when the page is rendered.
//! A process serves one app for now, so these families are kept without
//! it; when one serves several, they would be kept by app instead.
//!
//! Counters and histograms are recorded where things happen; gauges (resident
//! actors, open sockets) are read from the service when scraped.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Duration;

#[derive(Default)]
struct AllocatorMetrics {
    duration: Histogram,
    trims: [AtomicU64; 2],
}

impl AllocatorMetrics {
    fn record(&self, result: wasmkeep_runtime::AllocatorTrim) {
        self.duration.observe(result.duration);
        self.trims[usize::from(result.released)].fetch_add(1, Ordering::Relaxed);
    }

    fn render(&self, out: &mut String) {
        out.push_str("# HELP wasmkeep_allocator_trim_seconds Time inside malloc_trim.\n# TYPE wasmkeep_allocator_trim_seconds histogram\n");
        self.duration
            .render(out, "wasmkeep_allocator_trim_seconds", "");
        out.push_str("# HELP wasmkeep_allocator_trims_total Actual malloc_trim calls by release result.\n# TYPE wasmkeep_allocator_trims_total counter\n");
        for (index, released) in [false, true].into_iter().enumerate() {
            let _ = writeln!(
                out,
                "wasmkeep_allocator_trims_total{{released=\"{released}\"}} {}",
                self.trims[index].load(Ordering::Relaxed)
            );
        }
    }
}

static ALLOCATOR: LazyLock<AllocatorMetrics> = LazyLock::new(AllocatorMetrics::default);

pub(crate) fn allocator_trim(result: wasmkeep_runtime::AllocatorTrim) {
    ALLOCATOR.record(result);
}

static INGRESS: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];
pub(crate) fn ingress_admission(reason: Option<crate::rate_limit::Rejection>) {
    let index = match reason {
        None => 0,
        Some(crate::rate_limit::Rejection::Quota(_)) => 1,
        Some(crate::rate_limit::Rejection::Capacity) => 2,
        Some(crate::rate_limit::Rejection::InvalidKey) => 3,
    };
    INGRESS[index].fetch_add(1, Ordering::Relaxed);
}

/// Upper bounds of histogram buckets, in seconds.
const BUCKETS: [f64; 14] = [
    0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1.0, 5.0, 30.0,
];

/// [`BUCKETS`] with 0.5, 2 and 2.5 s added, for the histograms that must
/// read a shift between 0.25 s and 30 s (segment uploads, bucket requests,
/// scheduling lag). Kept apart from [`BUCKETS`] because the per-class and
/// per-app histograms share that one, and each added bound is a series per
/// label set.
const WIDE_BUCKETS: [f64; 17] = [
    0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0,
    2.5, 5.0, 30.0,
];

/// The upper bounds, in seconds, of a [`Histogram`].
pub(crate) trait Bounds {
    const SECONDS: &'static [f64];
}

/// [`BUCKETS`]: the bounds of most histograms.
#[derive(Debug)]
pub(crate) struct Standard;
impl Bounds for Standard {
    const SECONDS: &'static [f64] = &BUCKETS;
}

/// [`WIDE_BUCKETS`].
#[derive(Debug)]
pub(crate) struct Wide;
impl Bounds for Wide {
    const SECONDS: &'static [f64] = &WIDE_BUCKETS;
}

/// A cumulative-bucket histogram of durations.
#[derive(Debug)]
pub(crate) struct Histogram<B: Bounds = Standard> {
    buckets: Vec<AtomicU64>,
    count: AtomicU64,
    sum_ns: AtomicU64,
    bounds: std::marker::PhantomData<fn() -> B>,
}

impl<B: Bounds> Default for Histogram<B> {
    fn default() -> Self {
        Self {
            buckets: B::SECONDS.iter().map(|_| AtomicU64::new(0)).collect(),
            count: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
            bounds: std::marker::PhantomData,
        }
    }
}

impl<B: Bounds> Histogram<B> {
    pub(crate) fn observe(&self, d: Duration) {
        let secs = d.as_secs_f64();
        for (bound, bucket) in B::SECONDS.iter().zip(&self.buckets) {
            if secs <= *bound {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        let ns = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
    }

    /// Observations so far.
    #[cfg(test)]
    pub(crate) fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    fn render(&self, out: &mut String, name: &str, labels: &str) {
        let sep = if labels.is_empty() { "" } else { "," };
        for (bound, bucket) in B::SECONDS.iter().zip(&self.buckets) {
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

/// A cumulative-bucket histogram of counts (events, bytes), with fixed
/// upper bounds.
#[derive(Debug)]
pub(crate) struct Counts {
    bounds: &'static [u64],
    buckets: Vec<AtomicU64>,
    count: AtomicU64,
    sum: AtomicU64,
}

impl Counts {
    fn new(bounds: &'static [u64]) -> Self {
        Self {
            bounds,
            buckets: bounds.iter().map(|_| AtomicU64::new(0)).collect(),
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
        }
    }

    pub(crate) fn observe(&self, v: u64) {
        for (bound, bucket) in self.bounds.iter().zip(&self.buckets) {
            if v <= *bound {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(v, Ordering::Relaxed);
    }

    fn render(&self, out: &mut String, name: &str) {
        self.render_labels(out, name, "");
    }

    fn render_labels(&self, out: &mut String, name: &str, labels: &str) {
        let sep = if labels.is_empty() { "" } else { "," };
        for (bound, bucket) in self.bounds.iter().zip(&self.buckets) {
            let n = bucket.load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{{labels}{sep}le=\"{bound}\"}} {n}");
        }
        let count = self.count.load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{{labels}{sep}le=\"+Inf\"}} {count}");
        let labels = if labels.is_empty() {
            String::new()
        } else {
            format!("{{{labels}}}")
        };
        let _ = writeln!(
            out,
            "{name}_sum{labels} {}",
            self.sum.load(Ordering::Relaxed)
        );
        let _ = writeln!(out, "{name}_count{labels} {count}");
    }
}

/// Upper bounds, in milliseconds, of the histograms of the heartbeats'
/// timing (ADR 0020, amendment of 2026-11-20): fine around the default
/// period (3 s), the lease attempt (2.67 s) and the horizon (10 s).
const HEARTBEAT_MS: [u64; 16] = [
    10, 25, 50, 100, 250, 500, 1_000, 1_500, 2_000, 2_500, 3_000, 4_000, 6_000, 8_000, 10_000,
    15_000,
];

/// A histogram of the heartbeats' timing, in milliseconds.
#[derive(Debug)]
pub(crate) struct HeartbeatMs(Counts);
impl Default for HeartbeatMs {
    fn default() -> Self {
        Self(Counts::new(&HEARTBEAT_MS))
    }
}

impl HeartbeatMs {
    pub(crate) fn observe(&self, d: Duration) {
        self.0
            .observe(u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
    }

    pub(crate) fn observe_ms(&self, ms: u64) {
        self.0.observe(ms);
    }

    /// Observations so far.
    #[cfg(test)]
    pub(crate) fn count(&self) -> u64 {
        self.0.count.load(Ordering::Relaxed)
    }
}

/// Fuel units per guest event.
#[derive(Debug)]
pub(crate) struct EventFuel(Counts);
impl Default for EventFuel {
    fn default() -> Self {
        Self(Counts::new(&[
            10_000,
            100_000,
            1_000_000,
            5_000_000,
            10_000_000,
            25_000_000,
            50_000_000,
            100_000_000,
            250_000_000,
            500_000_000,
            1_000_000_000,
            10_000_000_000,
        ]))
    }
}

pub(crate) fn record_event_fuel(app: &str, class: &str, kind: &str, fuel: u64) {
    METRICS
        .event_fuel
        .get_in(app, &[("class", class), ("kind", event_kind(kind))])
        .0
        .observe(fuel);
}

fn event_kind(kind: &str) -> &str {
    match kind {
        "request" => "fetch",
        "alarm" => "alarm",
        "websocket" => "websocket",
        _ => "other",
    }
}

/// Events per batched segment: the bounds of `wasmkeep_segment_events`.
const SEGMENT_EVENT_BOUNDS: &[u64] = &[1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000];
/// Encoded bytes per batched segment: the bounds of
/// `wasmkeep_segment_bytes`, 4 KiB to 256 MiB.
const SEGMENT_BYTE_BOUNDS: &[u64] = &[
    4_096,
    16_384,
    65_536,
    262_144,
    1_048_576,
    4_194_304,
    16_777_216,
    67_108_864,
    268_435_456,
];

/// [`Counts`] of events per segment, on [`SEGMENT_EVENT_BOUNDS`].
#[derive(Debug)]
pub(crate) struct SegmentEvents(Counts);
/// [`Counts`] of bytes per segment, on [`SEGMENT_BYTE_BOUNDS`].
#[derive(Debug)]
pub(crate) struct SegmentBytes(Counts);

impl Default for SegmentEvents {
    fn default() -> Self {
        Self(Counts::new(SEGMENT_EVENT_BOUNDS))
    }
}
impl Default for SegmentBytes {
    fn default() -> Self {
        Self(Counts::new(SEGMENT_BYTE_BOUNDS))
    }
}
impl std::ops::Deref for SegmentEvents {
    type Target = Counts;
    fn deref(&self) -> &Counts {
        &self.0
    }
}
impl std::ops::Deref for SegmentBytes {
    type Target = Counts;
    fn deref(&self) -> &Counts {
        &self.0
    }
}

/// Why a batched actor's next event waited (ADR 0034, section 3): the
/// `reason` of `wasmkeep_batched_waits_total`.
pub(crate) const BATCHED_WAIT_REASONS: [&str; 3] = ["age", "bytes", "budget"];

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
        self.get_key(render_labels(labels))
    }

    /// The value for the labels of `app` ([`app_labels`]) then `labels`.
    pub(crate) fn get_in(&self, app: &str, labels: &[(&str, &str)]) -> Arc<T> {
        self.get_key(join_labels(app, &render_labels(labels)))
    }

    fn get_key(&self, key: String) -> Arc<T> {
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

/// The node a metrics page is about: the app it serves (ADR 0022) and, in
/// a cluster, its node id. Without actors, the implicit app
/// `default/default` and no node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// The app's tenant.
    pub tenant: String,
    /// The app, within its tenant.
    pub app: String,
    /// The node id, or empty when not in a cluster.
    pub node: String,
}

impl Default for Identity {
    fn default() -> Self {
        Self {
            tenant: "default".into(),
            app: crate::cluster::IMPLICIT_APP_NAME.into(),
            node: String::new(),
        }
    }
}

impl Identity {
    /// `tenant="...",app="..."`, in front of a class's labels.
    fn app_labels(&self) -> String {
        render_labels(&[("tenant", &self.tenant), ("app", &self.app)])
    }
}

/// `front` then `labels`, either possibly empty.
fn join_labels(front: &str, labels: &str) -> String {
    match (front.is_empty(), labels.is_empty()) {
        (true, _) => labels.to_owned(),
        (_, true) => front.to_owned(),
        _ => format!("{front},{labels}"),
    }
}

/// Everything WasmKeep counts.
#[derive(Debug, Default)]
pub(crate) struct Metrics {
    /// Events run, by class, kind (`request`, `alarm`, `websocket`, `init`)
    /// and outcome (`ok`, `failed`); rendered with the node's tenant and app.
    pub(crate) events: Family<AtomicU64>,
    /// How long guest events ran, by class and duration kind (fetch,
    /// alarm, websocket, other), with tenant and app.
    pub(crate) event_seconds: Family<Histogram>,
    pub(crate) event_fuel: Family<EventFuel>,
    /// Accepted guest-event jobs waiting for their mailbox turn, by class.
    pub(crate) mailbox_wait_seconds: Family<Histogram>,
    pub(crate) mailbox_host_work_seconds: Family<Histogram>,
    pub(crate) alarm_stale_hints: Family<AtomicU64>,
    /// Alarm deliveries an actor's mailbox ran, by class and outcome
    /// (`ran`, `stale`, `duplicate`, `deferred`), with tenant and app.
    pub(crate) alarm_deliveries: Family<AtomicU64>,
    /// Batched event boundaries gating the next guest event, by class.
    pub(crate) event_start_rule_wait_seconds: Family<Histogram>,
    /// Events stopped by the host, by reason (`wall`, `lease`, `replica`).
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
    /// Placement at wake-up (ADR 0041, section 11), each with its app:
    /// decisions that sent an event, by `kind` and `outcome`.
    pub(crate) placements: Family<AtomicU64>,
    /// Placed events received, by `outcome`.
    pub(crate) placements_received: Family<AtomicU64>,
    /// Placeable wake-ups kept here, by `reason`.
    pub(crate) placement_kept: Family<AtomicU64>,
    /// Wake-ups of an actor kept apart from a companion held here (ADR
    /// 0041, amendment of 2026-11-22), by `outcome`.
    pub(crate) placement_apart: Family<AtomicU64>,
    /// Wake-ups of a spread actor, a big room's edge (ADR 0040, amendment
    /// of 2026-11-22), by `outcome`.
    pub(crate) placement_spread: Family<AtomicU64>,
    /// This node id's fill in thousandths, by `kind` (a gauge).
    pub(crate) placement_fill: Family<AtomicU64>,
    /// Requests kept until a dead node's actor could be taken over, by
    /// kind (`request`, `websocket`) and how the wait ended.
    pub(crate) failover_waits: Family<AtomicU64>,
    /// `exec-durable`, from the call to durable.
    pub(crate) durable_write_seconds: Histogram,
    /// One listing of the cluster's heartbeats, with the view and pins it
    /// refreshes (every heartbeat period, and early on an unknown pin).
    /// Fences run apart.
    pub(crate) listing_seconds: Histogram,
    /// One read of a peer's heartbeat object (ms), timed out ones at the
    /// read attempt: this node's side of how late a heartbeat is seen.
    pub(crate) heartbeat_read_ms: HeartbeatMs,
    /// How old each peer's heartbeat was when this node applied it (ms,
    /// this clock less its `written_at`): the writer's period and write
    /// latency, this node's read latency, and the clocks' difference.
    pub(crate) heartbeat_age_ms: HeartbeatMs,
    /// Reads of a peer's heartbeat that failed or timed out: that peer is
    /// kept as last read, trusted until its own horizon.
    pub(crate) heartbeat_read_failures: AtomicU64,
    /// Peers' heartbeats read before the next round because the trust in
    /// them would have ended before that round could land.
    pub(crate) heartbeat_early_reads: AtomicU64,
    /// One object a restore read from the bucket (a segment or a
    /// snapshot), from its request to its last byte, resends included.
    pub(crate) restore_read_seconds: Histogram,
    /// Restores from the bucket that failed or ran out of time: the event
    /// that woke the actor was refused, and the next one restores again.
    pub(crate) restore_failures: AtomicU64,
    /// Node lease writes that reached the bucket (ADR 0020): the heartbeat,
    /// one bucket write each.
    pub(crate) node_lease_writes: AtomicU64,
    /// Node lease writes that failed or timed out: with the defaults, two
    /// in a row (a write every 3 s, valid for 8 s) and the lease runs out.
    pub(crate) node_lease_failures: AtomicU64,
    /// Node lease reads made to judge an ownership record that returned a
    /// body older than one this process read before (ADR 0020, lease
    /// reads): read again.
    pub(crate) lease_reads_behind: AtomicU64,
    /// Judgements where every read was such a body: refused for now.
    pub(crate) lease_reads_stale: AtomicU64,
    /// Newest lease bodies replaced by an older one after no read
    /// confirmed them for ttl plus grace.
    pub(crate) lease_reads_unpinned: AtomicU64,
    /// One node lease write, from its send to its answer (ms): the
    /// writer's side of how late peers see this node's heartbeat.
    pub(crate) node_lease_write_ms: HeartbeatMs,
    /// Fences this node wrote over another node's lease (ADR 0020): that
    /// node stopped writing it for longer than its ttl plus grace.
    pub(crate) node_fences: AtomicU64,
    /// Hellos from peers that just started (ADR 0011, amendment of
    /// 2026-11-14): each asks for a listing of `nodes/` at once.
    pub(crate) hellos_received: AtomicU64,
    /// Ownership records this node released after its own lease ran out,
    /// before its lease task saw the lapse (ADR 0020, section 1): another
    /// node may take those actors at once, without a fence.
    pub(crate) releases_after_lapse: AtomicU64,
    /// Alarm deliveries rescheduled after mailbox capacity or queue refusal.
    pub(crate) alarm_admission_retries: AtomicU64,
    /// Alarm deliveries the mailbox dropped (the actor failed to start, or
    /// the event stopped before answering), rescheduled with backoff.
    pub(crate) alarm_dropped_deliveries: AtomicU64,
    /// Requests to the bucket, by operation (`get`, `head`, `put`, `delete`,
    /// `list`, `copy`, `rename`, `multipart`) and status (`ok`,
    /// `not_found`, `precondition`, `error`).
    pub(crate) bucket_requests: Family<AtomicU64>,
    /// How long each bucket request took, by operation and status
    /// (the labels of `bucket_requests`).
    pub(crate) bucket_request_seconds: Family<Histogram<Wide>>,
    /// How much later than asked the runtime woke the probe task.
    pub(crate) runtime_schedule_lag_seconds: Histogram<Wide>,
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
    /// Snapshot uploads that were not acknowledged (given up; one may
    /// still land later): a periodic one is tried again, see
    /// `Replica::snapshot_due`.
    pub(crate) snapshot_upload_failures: AtomicU64,
    /// Idle snapshots taken at hibernation (ADR 0042, section 2), by
    /// `outcome` ([`IDLE_SNAPSHOT_OUTCOMES`]).
    pub(crate) idle_snapshots: Family<AtomicU64>,
    /// Idle passes a retention window after a release (ADR 0042, section
    /// 3), by `outcome` ([`IDLE_COMPACTION_OUTCOMES`]).
    pub(crate) idle_compactions: Family<AtomicU64>,
    /// Idle passes not queued because the queue was full (ADR 0042).
    pub(crate) idle_passes_dropped: AtomicU64,
    /// Node calls refused because the record of used signatures was full,
    /// by record (`calls`; `control`, takes and alarm hand-offs; `ingress`, charged proofs).
    pub(crate) signatures_refused_full: Family<AtomicU64>,
    /// Signed calls on the public port in refuse mode (refused anyway) not
    /// recorded because their record was full: nothing else is refused.
    pub(crate) signatures_public_dropped: AtomicU64,
    /// Node calls refused because they were signed before this node
    /// started (a replay, or the sender's clock behind).
    pub(crate) signatures_before_start: AtomicU64,
    /// Node calls refused because their signature did not hold: bad,
    /// expired, or made for another node id.
    pub(crate) signatures_bad: AtomicU64,
    /// WebSockets this node closed with 1012 because it no longer owned
    /// their actor, by how it found out (`event`, `sweep`, `lease_lapse`,
    /// `drain`, `move`).
    pub(crate) websocket_owner_closes: Family<AtomicU64>,
    /// Actor deletes recorded, by `how` (ADR 0028).
    pub(crate) actor_deletes: Family<AtomicU64>,
    /// Actor deletes that failed to be recorded, by `how`.
    pub(crate) actor_delete_failures: Family<AtomicU64>,
    /// Sweep records the blob sweep acted on, by app and `outcome` (ADR
    /// 0030, section 7).
    pub(crate) blob_sweeps: Family<AtomicU64>,
    /// File calls, by app, `op` and `outcome`.
    pub(crate) blob_ops: Family<AtomicU64>,
    /// Bytes of files through the host, by app and `direction`.
    pub(crate) blob_bytes: Family<AtomicU64>,
    /// Links made, by app and `kind`.
    pub(crate) blob_links: Family<AtomicU64>,
    /// Node-link requests answered, by app and `outcome`.
    pub(crate) blob_node_links: Family<AtomicU64>,
    /// The last hourly count of an app's files: bytes, and files.
    pub(crate) blob_stored_bytes: Family<AtomicU64>,
    pub(crate) blob_stored_files: Family<AtomicU64>,
    /// 1 when downloads are presigned (the start proof passed), else 0:
    /// the process's.
    pub(crate) blob_links_mode: AtomicU64,
    /// Idle-expiry passes this node ran as the collector (ADR 0028).
    pub(crate) expiry_passes: AtomicU64,
    /// Idle actors those passes found (candidates acted on).
    pub(crate) expiry_candidates: AtomicU64,
    /// Candidates kept, by `reason` (ADR 0028): the record `changed` since
    /// the listing (a wake), an `alarm` or a `socket`, the actor `active`
    /// here, or the expiry `failed`.
    pub(crate) expiry_kept: Family<AtomicU64>,
    /// Ownership records the WebSocket owner sweep read.
    pub(crate) websocket_sweep_reads: AtomicU64,
    /// Ownership records the WebSocket owner sweep could not decode and
    /// passed over, by `read` (`owner`, `parked`).
    pub(crate) websocket_sweep_unreadable: Family<AtomicU64>,
    /// Records kept for actors that hibernated with sockets open (a gauge,
    /// ADR 0024).
    pub(crate) parked_records: AtomicU64,
    /// Wakes of a parked actor on its node (ADR 0024).
    pub(crate) parked_wakes: AtomicU64,
    /// Parked records released, by `reason` (`last_socket`, `drain`,
    /// `move`, `lapse`, `stop`).
    pub(crate) parked_releases: Family<AtomicU64>,
    /// Events whose writes were committed but not acknowledged when a
    /// batched hold ended without a flush, by `reason`
    /// ([`PLAIN_DROP_REASONS`], ADR 0034, section 10). A strict hold never
    /// counts here.
    pub(crate) plain_events_dropped: Family<AtomicU64>,
    /// The oldest uncaptured commit's age at each batched capture (ADR
    /// 0034, section 16).
    pub(crate) unsent_age_seconds: Histogram,
    /// Events per batched segment.
    pub(crate) segment_events: SegmentEvents,
    /// Encoded bytes per batched segment.
    pub(crate) segment_bytes: SegmentBytes,
    /// Segment uploads acknowledged, first PUT to acknowledgement,
    /// resends included.
    pub(crate) segment_upload_seconds: Histogram<Wide>,
    /// PUTs of a segment sent again after a failure, a stall or a race.
    pub(crate) segment_upload_retries: AtomicU64,
    /// Segment uploads given up (not acknowledged).
    pub(crate) segment_upload_failures: AtomicU64,
    /// The event-start rule acting, by `reason` ([`BATCHED_WAIT_REASONS`]).
    pub(crate) batched_waits: Family<AtomicU64>,
    /// How long a batched `flush` (or `exec-durable`'s flush) took.
    pub(crate) flush_seconds: Histogram,
    /// Front requests (ADR 0023), by outcome (`ok`, `busy`,
    /// `unconfigured`, `cpu_budget`, `wall`, `guest_error`); rendered with
    /// the node's tenant and app.
    pub(crate) front_requests: Family<AtomicU64>,
    asset_requests: Family<AtomicU64>,
    asset_fetches: Family<AtomicU64>,
    asset_bytes: Family<AtomicU64>,
    asset_cache: Family<AtomicU64>,
    /// How long front requests took, to the response's last byte; rendered
    /// with the node's tenant and app.
    pub(crate) front_request_seconds: Family<Histogram>,
    /// Guest CPU time each front request was charged against its budget,
    /// instantiation included; rendered with the node's tenant and app.
    pub(crate) front_cpu_seconds: Family<Histogram>,
    /// Wasmtime fuel each front request consumed, when measured.
    pub(crate) front_fuel: Family<EventFuel>,
    /// Front requests running now (a gauge).
    pub(crate) front_slots_in_use: AtomicU64,
    /// `actors.call` from the front, by outcome (`ok`, `busy`, `timeout`,
    /// `failed`); rendered with the node's tenant and app.
    pub(crate) front_calls: Family<AtomicU64>,
    /// Requests, WebSocket upgrades and actor calls refused before their
    /// event ran, by [`Refusal`].
    pub(crate) admission_refused: [AtomicU64; Refusal::ALL.len()],
}

/// Why an event was refused before it ran: the fixed `reason` label of
/// `wasmkeep_admission_refused_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The class's access rule (ADR 0010): an internal class reached from
    /// outside (403), or a new name of an `existing` class (404).
    Access,
    /// No resident capacity on the node (503 with `Retry-After`).
    Capacity,
    /// The actor's queue is full (429).
    Overloaded,
    /// The node cannot run the actor now (503): a required value not set,
    /// the deployed version or the app's secrets not applied, the bucket
    /// not answering for its ownership.
    Unavailable,
    /// A hop from another node not for this node's app (ADR 0022): it
    /// names another app (503 "not this app's node"), or none (a node of
    /// an older version).
    WrongApp,
}

impl Refusal {
    const ALL: [Self; 5] = [
        Self::Access,
        Self::Capacity,
        Self::Overloaded,
        Self::Unavailable,
        Self::WrongApp,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::Access => "access",
            Self::Capacity => "capacity",
            Self::Overloaded => "overloaded",
            Self::Unavailable => "unavailable",
            Self::WrongApp => "wrong_app",
        }
    }
}

/// Counts one admission refusal ([`Metrics::admission_refused`]).
pub(crate) fn refused(why: Refusal) {
    if let Some(i) = Refusal::ALL.iter().position(|r| *r == why) {
        inc(&METRICS.admission_refused[i]);
    }
}

/// Counts `n` WebSockets closed for `reason` ([`Metrics::websocket_owner_closes`]).
pub(crate) fn owner_closes(reason: &str, n: usize) {
    if n > 0 {
        METRICS
            .websocket_owner_closes
            .get(&[("reason", reason)])
            .fetch_add(u64::try_from(n).unwrap_or(u64::MAX), Ordering::Relaxed);
    }
}

/// Counts an actor delete recorded, by `how` ([`DELETE_HOWS`]).
pub(crate) fn deleted(how: &str) {
    inc(&METRICS.actor_deletes.get(&[("how", how)]));
}

/// Counts an idle-expiry candidate kept, by `reason`
/// ([`EXPIRY_KEPT_REASONS`]).
pub(crate) fn expiry_kept(reason: &str) {
    inc(&METRICS.expiry_kept.get(&[("reason", reason)]));
}

/// Counts an actor delete that failed to be recorded, by `how`.
pub(crate) fn delete_failed(how: &str) {
    inc(&METRICS.actor_delete_failures.get(&[("how", how)]));
}

/// Counts a record the WebSocket sweep could not decode, for `read`
/// ([`Metrics::websocket_sweep_unreadable`]).
pub(crate) fn sweep_unreadable(read: &str) {
    METRICS
        .websocket_sweep_unreadable
        .get(&[("read", read)])
        .fetch_add(1, Ordering::Relaxed);
}

/// Counts `n` parked records released for `reason`
/// ([`Metrics::parked_releases`]).
pub(crate) fn parked_released(reason: &str, n: usize) {
    if n > 0 {
        METRICS
            .parked_releases
            .get(&[("reason", reason)])
            .fetch_add(u64::try_from(n).unwrap_or(u64::MAX), Ordering::Relaxed);
    }
}

/// Every event counted by `wasmkeep_plain_events_dropped_total` in this
/// process, whatever its reason (ADR 0034, section 10).
pub fn plain_events_dropped_total() -> u64 {
    METRICS.plain_events_dropped.total()
}

/// The events `wasmkeep_plain_events_dropped_total` counted for `reason`
/// in this process.
pub fn plain_events_dropped_by(reason: &str) -> u64 {
    METRICS
        .plain_events_dropped
        .get(&[("reason", reason)])
        .load(Ordering::Relaxed)
}

/// Counts `n` events dropped for `reason`
/// ([`Metrics::plain_events_dropped`]).
pub(crate) fn plain_events_dropped(reason: &str, n: u64) {
    if n > 0 {
        METRICS
            .plain_events_dropped
            .get(&[("reason", reason)])
            .fetch_add(n, Ordering::Relaxed);
    }
}

impl Family<AtomicU64> {
    /// The sum over every label set.
    pub(crate) fn total(&self) -> u64 {
        let mut sum = 0u64;
        self.each(|_, v| sum = sum.saturating_add(v.load(Ordering::Relaxed)));
        sum
    }

    /// The sum over the label sets of the app `app` names
    /// ([`app_labels`]).
    pub(crate) fn total_in(&self, app: &str) -> u64 {
        let mut sum = 0u64;
        self.each(|labels, v| {
            let theirs = labels
                .strip_prefix(app)
                .is_some_and(|rest| rest.is_empty() || rest.starts_with(','));
            if theirs {
                sum = sum.saturating_add(v.load(Ordering::Relaxed));
            }
        });
        sum
    }
}

/// An app's labels, `tenant="...",app="..."`: what its work is recorded
/// under (ADR 0038, section 9).
pub(crate) fn app_labels(tenant: &str, app: &str) -> Arc<str> {
    Arc::from(render_labels(&[("tenant", tenant), ("app", app)]))
}

/// One front request of the app `app` names ([`app_labels`]), ended
/// with `outcome` ([`FRONT_REQUEST_OUTCOMES`]).
pub(crate) fn asset_request(app: &str, outcome: &str) {
    inc(&METRICS.asset_requests.get_in(app, &[("outcome", outcome)]));
}
pub(crate) fn asset_fetch(app: &str, outcome: &str) {
    inc(&METRICS.asset_fetches.get_in(app, &[("outcome", outcome)]));
}
pub(crate) fn asset_bytes(app: &str, encoding: &str, bytes: u64) {
    METRICS
        .asset_bytes
        .get_in(app, &[("encoding", encoding)])
        .fetch_add(bytes, Ordering::Relaxed);
}
pub(crate) fn asset_cache_add(app: &str, bytes: u64) {
    METRICS
        .asset_cache
        .get_in(app, &[])
        .fetch_add(bytes, Ordering::Relaxed);
}
pub(crate) fn asset_cache_remove(app: &str, bytes: u64) {
    METRICS
        .asset_cache
        .get_in(app, &[])
        .fetch_sub(bytes, Ordering::Relaxed);
}

pub(crate) fn front_request(app: &str, outcome: &str) {
    inc(&METRICS.front_requests.get_in(app, &[("outcome", outcome)]));
}

/// How long a front request of `app` took, to its response's last byte.
pub(crate) fn front_request_took(app: &str, elapsed: std::time::Duration) {
    METRICS
        .front_request_seconds
        .get_in(app, &[])
        .observe(elapsed);
}

/// The guest work of one front request of `app`, from instantiation to the
/// instance's end: its CPU time and, when measured, its fuel.
pub(crate) fn front_request_work(app: &str, cpu: std::time::Duration, fuel: Option<u64>) {
    METRICS.front_cpu_seconds.get_in(app, &[]).observe(cpu);
    if let Some(fuel) = fuel {
        METRICS.front_fuel.get_in(app, &[]).0.observe(fuel);
    }
}

/// One `actors.call` from the front of `app`, ended with `outcome`
/// ([`FRONT_CALL_OUTCOMES`]).
pub(crate) fn front_call(app: &str, outcome: &str) {
    inc(&METRICS.front_calls.get_in(app, &[("outcome", outcome)]));
}

/// One event of `class` in the app `app` names ([`app_labels`]), of
/// `kind`, ended with `outcome`, after `elapsed`.
pub(crate) fn record_event(
    app: &str,
    class: &str,
    kind: &str,
    outcome: &str,
    elapsed: std::time::Duration,
) {
    inc(&METRICS.events.get_in(
        app,
        &[("class", class), ("kind", kind), ("outcome", outcome)],
    ));
    let duration_kind = event_kind(kind);
    METRICS
        .event_seconds
        .get_in(app, &[("class", class), ("kind", duration_kind)])
        .observe(elapsed);
}

/// `kind` of `wasmkeep_failover_waits_total` ([`crate::cluster::failover_wait`]).
pub(crate) const FAILOVER_KINDS: [&str; 2] = ["request", "websocket"];
/// `outcome` of `wasmkeep_failover_waits_total`: how a kept request's wait
/// ended.
pub(crate) const FAILOVER_OUTCOMES: [&str; 9] = [
    "served",
    "forwarded",
    "owner_alive",
    "expired",
    "shutdown",
    "lease_lost",
    "left",
    "answered_other",
    "over_cap",
];
/// `operation` of `wasmkeep_bucket_requests_total` ([`crate::counted_store`]).
pub(crate) const BUCKET_OPERATIONS: [&str; 8] = [
    "get",
    "head",
    "put",
    "delete",
    "list",
    "copy",
    "rename",
    "multipart",
];
/// `status` of `wasmkeep_bucket_requests_total`.
pub(crate) const BUCKET_STATUSES: [&str; 4] = ["ok", "not_found", "precondition", "error"];
/// `reason` of `wasmkeep_event_cuts_total`.
pub(crate) const CUT_REASONS: [&str; 4] = ["wall", "lease", "replica", "stop"];
/// `kind` of `wasmkeep_forwards_total`.
pub(crate) const FORWARD_KINDS: [&str; 3] = ["request", "websocket", "call"];
/// `outcome` of `wasmkeep_idle_snapshots_total` (ADR 0042, section 2):
/// created and acknowledged, already there, not acknowledged (given up,
/// refused by the bucket, landed after the hold ended), refused before
/// any upload (not at the head's database: replication stops), not
/// taken because the node's idle snapshots were all under way.
pub(crate) const IDLE_SNAPSHOT_OUTCOMES: [&str; 5] =
    ["created", "existed", "failed", "refused", "busy"];

/// One idle snapshot ended with `outcome` ([`IDLE_SNAPSHOT_OUTCOMES`]).
pub(crate) fn idle_snapshot(outcome: &str) {
    inc(&METRICS.idle_snapshots.get(&[("outcome", outcome)]));
}

/// `outcome` of `wasmkeep_idle_compactions_total` (ADR 0042, section 3):
/// compacted (at least one object deleted, or the history already
/// compact), the record changed since the release (a wake, a move, an
/// expiry, the app deleted or recreated), no snapshot of the released
/// generation to compact from, refused (the node draining, the app
/// deleted, restoring or recreated, a class with an idle limit, the
/// bucket failing), the actor in use on this node when it was due.
pub(crate) const IDLE_COMPACTION_OUTCOMES: [&str; 5] =
    ["done", "changed", "nothing", "refused", "busy"];

/// One idle pass ended with `outcome` ([`IDLE_COMPACTION_OUTCOMES`]).
pub(crate) fn idle_compaction(outcome: &str) {
    inc(&METRICS.idle_compactions.get(&[("outcome", outcome)]));
}

/// `record` of `wasmkeep_signatures_refused_full_total`.
pub(crate) const SIGNATURE_RECORDS: [&str; 3] = ["calls", "control", "ingress"];
/// `reason` of `wasmkeep_websocket_owner_closes_total`.
pub(crate) const OWNER_CLOSE_REASONS: [&str; 5] =
    ["event", "sweep", "lease_lapse", "drain", "move"];
/// `how` of `wasmkeep_actor_deletes_total` and
/// `wasmkeep_actor_delete_failures_total` (ADR 0028).
pub(crate) const DELETE_HOWS: [&str; 4] = ["self", "self_durable", "operator", "expired"];
/// `outcome` of `wasmkeep_blob_sweeps_total` (ADR 0030, section 7): a
/// namespace removed after a judgement (`swept`), a record dropped because
/// the actor is at its incarnation (`not_deleted`) or after its last
/// listing (`final`), one nobody could judge for 25 hours (`aged_out`),
/// and a step that failed (`failed`).
/// `old_epoch` counts an earlier epoch's files removed by the hourly pass.
pub(crate) const BLOB_SWEEP_OUTCOMES: [&str; 6] = [
    "swept",
    "not_deleted",
    "final",
    "aged_out",
    "failed",
    "old_epoch",
];
/// `outcome` of `wasmkeep_blob_node_link_requests_total`.
pub(crate) const BLOB_NODE_LINK_OUTCOMES: [&str; 6] = [
    "ok",
    "expired",
    "bad_token",
    "wrong_app",
    "exists",
    "wrong_size",
];

/// Counts a sweep record of the app `app` names acted on.
pub(crate) fn blob_sweep(app: &str, outcome: &str) {
    inc(&METRICS.blob_sweeps.get_in(app, &[("outcome", outcome)]));
}
/// One file call of `app` ([`app_labels`]): `op` (`head`, `get`, `put`,
/// `read`, `write`, `delete`, `list`) ended with `outcome`.
pub(crate) fn blob_op(app: &str, op: &str, outcome: &str) {
    inc(&METRICS
        .blob_ops
        .get_in(app, &[("op", op), ("outcome", outcome)]));
}
/// `n` bytes of `app`'s files through the host, `in` or `out`.
pub(crate) fn blob_bytes(app: &str, direction: &str, n: u64) {
    if n > 0 {
        METRICS
            .blob_bytes
            .get_in(app, &[("direction", direction)])
            .fetch_add(n, Ordering::Relaxed);
    }
}
/// A link of `app` made, `download` or `upload`.
pub(crate) fn blob_link(app: &str, kind: &str) {
    inc(&METRICS.blob_links.get_in(app, &[("kind", kind)]));
}
/// A node link of `app` answered with `outcome`
/// ([`BLOB_NODE_LINK_OUTCOMES`]).
pub(crate) fn blob_node_link(app: &str, outcome: &str) {
    inc(&METRICS.blob_node_links.get_in(app, &[("outcome", outcome)]));
}
/// A placed hop of `kind` (`request`, `call`) that ended as `outcome`
/// (ADR 0041, section 11).
pub(crate) fn placement_sent(app: &str, kind: &str, outcome: &str) {
    inc(&METRICS
        .placements
        .get_in(app, &[("kind", kind), ("outcome", outcome)]));
}
/// A placed event received here that ended as `outcome`.
pub(crate) fn placement_received(app: &str, outcome: &str) {
    inc(&METRICS
        .placements_received
        .get_in(app, &[("outcome", outcome)]));
}
/// A placeable wake-up kept here for `reason`.
pub(crate) fn placement_kept(app: &str, reason: &str) {
    inc(&METRICS.placement_kept.get_in(app, &[("reason", reason)]));
}
/// A wake-up of an actor whose companion this node holds: `sent` to a
/// peer, or woken here, `alone`, with no peer to take it.
pub(crate) fn placement_apart(app: &str, outcome: &str) {
    inc(&METRICS.placement_apart.get_in(app, &[("outcome", outcome)]));
}
/// A wake-up of a spread actor: `sent` to its node, or woken `here` (this
/// node is the rendezvous choice, or no peer can take it).
pub(crate) fn placement_spread(app: &str, outcome: &str) {
    inc(&METRICS
        .placement_spread
        .get_in(app, &[("outcome", outcome)]));
}
/// This node id's fills, in thousandths.
pub(crate) fn placement_fill(app: &str, residents: u64, sockets: u64) {
    for (kind, v) in [("residents", residents), ("sockets", sockets)] {
        METRICS
            .placement_fill
            .get_in(app, &[("kind", kind)])
            .store(v, Ordering::Relaxed);
    }
}
/// The last count of `app`'s files.
pub(crate) fn blob_stored(app: &str, bytes: u64, files: u64) {
    METRICS
        .blob_stored_bytes
        .get_in(app, &[])
        .store(bytes, Ordering::Relaxed);
    METRICS
        .blob_stored_files
        .get_in(app, &[])
        .store(files, Ordering::Relaxed);
}
/// Whether this process presigns downloads (`wasmkeep_blob_links_mode`).
pub(crate) fn blob_links_mode(presigned: bool) {
    METRICS
        .blob_links_mode
        .store(u64::from(presigned), Ordering::Relaxed);
}

/// `reason` of `wasmkeep_expiry_kept_total` (ADR 0028).
pub(crate) const EXPIRY_KEPT_REASONS: [&str; 5] =
    ["changed", "alarm", "socket", "active", "failed"];
/// `read` of `wasmkeep_websocket_sweep_unreadable_total`.
pub(crate) const SWEEP_UNREADABLE_READS: [&str; 2] = ["owner", "parked"];
/// `reason` of `wasmkeep_parked_releases_total`.
pub(crate) const PARKED_RELEASE_REASONS: [&str; 5] =
    ["last_socket", "drain", "move", "lapse", "stop"];
/// `reason` of `wasmkeep_plain_events_dropped_total` (ADR 0034, section
/// 10).
pub(crate) const PLAIN_DROP_REASONS: [&str; 7] = [
    "stop",
    "failure",
    "lapse",
    "lost_lease",
    "panic",
    "app_deleted",
    "unbarriered",
];
/// `outcome` of `wasmkeep_front_requests_total`.
pub(crate) const FRONT_REQUEST_OUTCOMES: [&str; 8] = [
    "ok",
    "handoff",
    "busy",
    "unconfigured",
    "cpu_budget",
    "wall",
    "guest_error",
    "client_gone",
];
/// `outcome` of `wasmkeep_front_calls_total`.
pub(crate) const FRONT_CALL_OUTCOMES: [&str; 4] = ["ok", "busy", "timeout", "failed"];

/// Every `reason` of `wasmkeep_not_ready`: those of
/// [`crate::ActorService::not_ready`], of the cluster's readiness and the
/// words of a stopped node. A reason not listed here is still rendered
/// while it holds; a test keeps the list complete.
pub(crate) const NOT_READY_REASONS: [&str; 23] = [
    "shutting down",
    "checking that no other process runs with this node id",
    "another process runs with this node id",
    "this node was fenced",
    "this node's lease was released by another process",
    "this node's lease object was deleted",
    "an older version of WasmKeep runs with this node id",
    "a node of an older WasmKeep version runs in this cluster",
    "this node's lease object holds something this version cannot read",
    "this node holds no lease",
    "the bucket does not enforce conditional writes",
    "the app's upgrade floor is above this node's version, or unreadable",
    "cluster secret not loaded",
    "a live node of an older version (no multi-app) runs in this cluster",
    "bucket unreachable (node lease not renewed)",
    "cluster identity not published for a heartbeat period yet",
    "this node has not yet checked whether its app was deleted",
    "the app was deleted",
    "disk not checked yet",
    "disk check stalled",
    "disk almost full",
    "cannot apply the deployed version",
    "cannot open the app's secrets",
];

impl Metrics {
    /// Every series of a family whose label values are a fixed set, at 0:
    /// on the page from the first scrape, so `increase()` sees the first
    /// event and rules can be checked against the names. Families keyed by
    /// class get a series at a class's first event only.
    fn seeded() -> Self {
        let m = Self::default();
        for kind in FAILOVER_KINDS {
            for outcome in FAILOVER_OUTCOMES {
                m.failover_waits
                    .get(&[("kind", kind), ("outcome", outcome)]);
            }
        }
        for operation in BUCKET_OPERATIONS {
            for status in BUCKET_STATUSES {
                m.bucket_requests
                    .get(&[("operation", operation), ("status", status)]);
            }
        }
        let one = |f: &Family<AtomicU64>, label: &str, values: &[&str]| {
            for v in values {
                f.get(&[(label, v)]);
            }
        };
        one(&m.cuts, "reason", &CUT_REASONS);
        one(&m.forwards, "kind", &FORWARD_KINDS);
        one(&m.signatures_refused_full, "record", &SIGNATURE_RECORDS);
        one(&m.idle_snapshots, "outcome", &IDLE_SNAPSHOT_OUTCOMES);
        one(&m.idle_compactions, "outcome", &IDLE_COMPACTION_OUTCOMES);
        one(&m.websocket_owner_closes, "reason", &OWNER_CLOSE_REASONS);
        one(&m.actor_deletes, "how", &DELETE_HOWS);
        one(&m.actor_delete_failures, "how", &DELETE_HOWS);
        one(&m.expiry_kept, "reason", &EXPIRY_KEPT_REASONS);
        one(
            &m.websocket_sweep_unreadable,
            "read",
            &SWEEP_UNREADABLE_READS,
        );
        one(&m.parked_releases, "reason", &PARKED_RELEASE_REASONS);
        one(&m.plain_events_dropped, "reason", &PLAIN_DROP_REASONS);
        one(&m.batched_waits, "reason", &BATCHED_WAIT_REASONS);
        m
    }
}

/// The process's metrics.
pub(crate) static METRICS: LazyLock<Metrics> = LazyLock::new(Metrics::seeded);

/// Adds one to a counter.
/// One segment upload ended: its time (acknowledged ones only), the
/// PUTs it sent again, and whether it failed.
pub(crate) fn segment_upload(elapsed: Duration, resends: u64, ok: bool) {
    let m = &METRICS;
    m.segment_upload_retries
        .fetch_add(resends, Ordering::Relaxed);
    if ok {
        m.segment_upload_seconds.observe(elapsed);
    } else {
        inc(&m.segment_upload_failures);
    }
}

/// The interval the scheduling probe sleeps.
const LAG_PROBE_INTERVAL: Duration = Duration::from_millis(100);

/// A task that sleeps [`LAG_PROBE_INTERVAL`] over and over and records how
/// much later than that it woke, in `wasmkeep_runtime_schedule_lag_seconds`.
/// One timer and one wake-up every 100 ms; stopped when dropped.
pub(crate) struct LagProbe(tokio::task::JoinHandle<()>);

impl LagProbe {
    pub(crate) fn start() -> Self {
        Self(tokio::spawn(async {
            loop {
                let asked = tokio::time::Instant::now() + LAG_PROBE_INTERVAL;
                tokio::time::sleep_until(asked).await;
                METRICS
                    .runtime_schedule_lag_seconds
                    .observe(tokio::time::Instant::now().saturating_duration_since(asked));
            }
        }))
    }
}

impl Drop for LagProbe {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) fn inc(c: &AtomicU64) {
    c.fetch_add(1, Ordering::Relaxed);
}

/// Gauges read from the service at scrape time.
#[derive(Debug, Clone, Copy, Default)]
pub struct Gauges {
    /// Process sessions, including relays and upgrades still opening.
    pub socket_sessions: usize,
    pub socket_requested: usize,
    pub socket_effective: usize,
    pub socket_actor_requested: usize,
    pub socket_actor_effective: usize,
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

impl Gauges {
    /// The process's gauges from each app runtime's (ADR 0038, section 9):
    /// residents, WebSockets and classes summed; the socket budget, shared,
    /// once; the first reason not to be ready.
    pub(crate) fn of_process(apps: &[Gauges]) -> Gauges {
        let Some(first) = apps.first() else {
            return Gauges::default();
        };
        Gauges {
            resident: apps.iter().map(|g| g.resident).sum(),
            websockets: apps.iter().map(|g| g.websockets).sum(),
            classes: apps.iter().map(|g| g.classes).sum(),
            not_ready: apps.iter().find_map(|g| g.not_ready),
            ..*first
        }
    }
}

/// The metrics page of a process serving no actors ([`render_for`] with
/// the implicit app `default/default` and no node).
pub fn render(gauges: Gauges) -> String {
    render_for(gauges, &Identity::default())
}

/// The metrics page of the node `identity` names: everything recorded so
/// far plus `gauges`, and the runtime's CPU-budget traps. Series with a
/// class carry its tenant and app; `wasmkeep_info` its version and node.
pub fn render_for(gauges: Gauges, identity: &Identity) -> String {
    render_with(&METRICS, gauges, std::slice::from_ref(identity))
}

/// The page of a process serving several apps (ADR 0038, section 9): one
/// `wasmkeep_info` per app's node, and each app's front series from the
/// first scrape.
pub(crate) fn render_for_apps(gauges: Gauges, identities: &[Identity]) -> String {
    render_with(&METRICS, gauges, identities)
}

/// [`render_for`], from `m`, for the nodes `identities` name (at least the
/// implicit app's).
fn render_with(m: &Metrics, gauges: Gauges, identities: &[Identity]) -> String {
    let implicit = [Identity::default()];
    let identities = if identities.is_empty() {
        &implicit[..]
    } else {
        identities
    };
    let apps: Vec<String> = identities.iter().map(Identity::app_labels).collect();
    let mut out = String::new();
    ALLOCATOR.render(&mut out);
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

    head(
        &mut out,
        "wasmkeep_info",
        "gauge",
        "Always 1: the running version, and the tenant, app and node id (in a cluster) of the node, for joining with other series.",
    );
    for identity in identities {
        let mut info = vec![
            ("version", wasmkeep_cluster::VERSION),
            ("tenant", identity.tenant.as_str()),
            ("app", identity.app.as_str()),
        ];
        if !identity.node.is_empty() {
            info.push(("node", identity.node.as_str()));
        }
        let _ = writeln!(out, "wasmkeep_info{{{}}} 1", render_labels(&info));
    }
    gauge(
        &mut out,
        "wasmkeep_actors_resident",
        "Actors resident in memory.",
        gauges.resident,
    );
    for (name, help, value) in [
        (
            "wasmkeep_socket_sessions",
            "Process socket sessions including relays and opening upgrades.",
            gauges.socket_sessions,
        ),
        (
            "wasmkeep_socket_limit_requested",
            "Requested process socket sessions.",
            gauges.socket_requested,
        ),
        (
            "wasmkeep_socket_limit_effective",
            "Effective process socket sessions after descriptor budgeting.",
            gauges.socket_effective,
        ),
        (
            "wasmkeep_socket_actor_limit_requested",
            "Requested sockets per actor.",
            gauges.socket_actor_requested,
        ),
        (
            "wasmkeep_socket_actor_limit_effective",
            "Sockets per actor bounded by app and process ceilings.",
            gauges.socket_actor_effective,
        ),
    ] {
        gauge(&mut out, name, help, value);
    }
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
        "Why the node is not ready: one series per reason, 1 for the reason it is not ready, 0 for the others.",
    );
    let not_ready = |out: &mut String, reason: &str, v: u8| {
        let _ = writeln!(
            out,
            "wasmkeep_not_ready{{{}}} {v}",
            render_labels(&[("reason", reason)])
        );
    };
    for reason in NOT_READY_REASONS {
        not_ready(&mut out, reason, u8::from(gauges.not_ready == Some(reason)));
    }
    if let Some(reason) = gauges.not_ready
        && !NOT_READY_REASONS.contains(&reason)
    {
        not_ready(&mut out, reason, 1);
    }
    gauge(
        &mut out,
        "wasmkeep_classes",
        "Actor classes served.",
        gauges.classes,
    );
    head(
        &mut out,
        "wasmkeep_events_total",
        "counter",
        "Events run, by tenant, app, class, kind and outcome.",
    );
    // Recorded with their app (ADR 0038, section 9).
    m.events.each(|labels, v| {
        let _ = writeln!(
            out,
            "wasmkeep_events_total{{{labels}}} {}",
            v.load(Ordering::Relaxed)
        );
    });
    head(
        &mut out,
        "wasmkeep_event_duration_seconds",
        "histogram",
        "How long guest events ran, by tenant, app, class and kind (fetch, alarm, websocket, other).",
    );
    head(
        &mut out,
        "wasmkeep_event_fuel",
        "histogram",
        "Wasmtime fuel units consumed per guest event; excludes host work.",
    );
    m.event_fuel
        .each(|labels, h| h.0.render_labels(&mut out, "wasmkeep_event_fuel", labels));
    m.event_seconds.each(|labels, h| {
        h.render(&mut out, "wasmkeep_event_duration_seconds", labels);
    });
    family(
        &mut out,
        "wasmkeep_alarm_stale_hints_total",
        "Stale alarm hint clear attempts, by tenant, app and class.",
        &m.alarm_stale_hints,
    );
    family(
        &mut out,
        "wasmkeep_alarm_deliveries_total",
        "Alarm deliveries run in actor mailboxes, by tenant, app, class and outcome (ran, stale, duplicate, deferred).",
        &m.alarm_deliveries,
    );
    for (name, help, durations) in [
        (
            "wasmkeep_mailbox_host_work_seconds",
            "Exclusive mailbox host work time, by tenant, app, class and work.",
            &m.mailbox_host_work_seconds,
        ),
        (
            "wasmkeep_mailbox_wait_seconds",
            "Time from accepted enqueue attempt to mailbox job start, by tenant, app and class.",
            &m.mailbox_wait_seconds,
        ),
        (
            "wasmkeep_event_start_rule_wait_seconds",
            "Batched boundary time gating the next event (capture, budget and upload wait), by tenant, app and class.",
            &m.event_start_rule_wait_seconds,
        ),
    ] {
        head(&mut out, name, "histogram", help);
        durations.each(|labels, h| h.render(&mut out, name, labels));
    }
    family(
        &mut out,
        "wasmkeep_event_cuts_total",
        "Events stopped by the host, by reason (wall clock, lost lease, failed replica).",
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
    family(
        &mut out,
        "wasmkeep_failover_waits_total",
        "Requests kept for an actor whose node was dead or stopping instead of a 503 at once, by kind and how the wait ended.",
        &m.failover_waits,
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
        "wasmkeep_heartbeat_read_milliseconds",
        "histogram",
        "One read of a peer's heartbeat, timed out ones at the read attempt.",
    );
    m.heartbeat_read_ms
        .0
        .render(&mut out, "wasmkeep_heartbeat_read_milliseconds");
    head(
        &mut out,
        "wasmkeep_heartbeat_age_milliseconds",
        "histogram",
        "How old each peer's heartbeat was when this node applied it, by this node's clock.",
    );
    m.heartbeat_age_ms
        .0
        .render(&mut out, "wasmkeep_heartbeat_age_milliseconds");
    head(
        &mut out,
        "wasmkeep_node_lease_write_milliseconds",
        "histogram",
        "One node lease (heartbeat) write of this node, from its send to its answer.",
    );
    m.node_lease_write_ms
        .0
        .render(&mut out, "wasmkeep_node_lease_write_milliseconds");
    counter(
        &mut out,
        "wasmkeep_heartbeat_read_failures_total",
        "Reads of a peer's heartbeat that failed or timed out; the peer is kept as last read until its own horizon.",
        m.heartbeat_read_failures.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_heartbeat_early_reads_total",
        "Peers' heartbeats read before the next round, the trust in them ending before it could land.",
        m.heartbeat_early_reads.load(Ordering::Relaxed),
    );
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
        "wasmkeep_restore_failures_total",
        "Restores from the bucket that failed or ran out of time; the next event restores again.",
        m.restore_failures.load(Ordering::Relaxed),
    );
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
        "wasmkeep_lease_reads_behind_total",
        "Node lease reads, made to judge an ownership record, older than a body this process read before: read again.",
        m.lease_reads_behind.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_lease_reads_stale_total",
        "Ownership judgements refused because every node lease read was older than a body read before.",
        m.lease_reads_stale.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_lease_reads_unpinned_total",
        "Newest node lease bodies replaced by an older one after no read confirmed them for ttl plus grace.",
        m.lease_reads_unpinned.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_node_fences_total",
        "Fences this node wrote over a node that stopped renewing its lease.",
        m.node_fences.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_node_hellos_total",
        "Signed hellos from peers that just started, each asking for a listing of the nodes.",
        m.hellos_received.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_releases_after_lapse_total",
        "Actors this node released after its lease ran out, before it saw the lapse.",
        m.releases_after_lapse.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_alarm_admission_retries_total",
        "Alarm deliveries rescheduled after mailbox admission refusal.",
        m.alarm_admission_retries.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_alarm_dropped_deliveries_total",
        "Alarm deliveries the mailbox dropped before they answered, rescheduled with backoff.",
        m.alarm_dropped_deliveries.load(Ordering::Relaxed),
    );
    family(
        &mut out,
        "wasmkeep_bucket_requests_total",
        "Requests to the bucket, by operation and status.",
        &m.bucket_requests,
    );
    head(
        &mut out,
        "wasmkeep_bucket_request_seconds",
        "histogram",
        "How long a bucket request took, by operation and status; a listing counts to its end or drop.",
    );
    m.bucket_request_seconds.each(|labels, h| {
        h.render(&mut out, "wasmkeep_bucket_request_seconds", labels);
    });
    head(
        &mut out,
        "wasmkeep_runtime_schedule_lag_seconds",
        "histogram",
        "How much later than asked a task sleeping 100 ms woke: the async runtime's scheduling delay.",
    );
    m.runtime_schedule_lag_seconds
        .render(&mut out, "wasmkeep_runtime_schedule_lag_seconds", "");
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
    counter(
        &mut out,
        "wasmkeep_snapshot_upload_failures_total",
        "Snapshot uploads not acknowledged; a periodic one is tried again.",
        m.snapshot_upload_failures.load(Ordering::Relaxed),
    );
    family(
        &mut out,
        "wasmkeep_idle_snapshots_total",
        "Snapshots at the head taken when an actor hibernates, by outcome.",
        &m.idle_snapshots,
    );
    family(
        &mut out,
        "wasmkeep_idle_compactions_total",
        "Idle passes a retention window after an actor's release, by outcome.",
        &m.idle_compactions,
    );
    counter(
        &mut out,
        "wasmkeep_idle_passes_dropped_total",
        "Idle passes not remembered because the node's queue was full.",
        m.idle_passes_dropped.load(Ordering::Relaxed),
    );
    family(
        &mut out,
        "wasmkeep_signatures_refused_full_total",
        "Node calls refused because the record of used signatures was full, by record.",
        &m.signatures_refused_full,
    );
    counter(
        &mut out,
        "wasmkeep_signatures_public_dropped_total",
        "Signed calls on the public port, refused anyway, not recorded because their record was full.",
        m.signatures_public_dropped.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_signatures_before_start_total",
        "Node calls refused because they were signed before this node started.",
        m.signatures_before_start.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_signatures_bad_total",
        "Node calls refused because their signature did not hold (bad, expired or misaddressed).",
        m.signatures_bad.load(Ordering::Relaxed),
    );
    family(
        &mut out,
        "wasmkeep_websocket_owner_closes_total",
        "WebSockets closed with 1012 because this node no longer owned their actor, by how it found out.",
        &m.websocket_owner_closes,
    );
    family(
        &mut out,
        "wasmkeep_actor_deletes_total",
        "Actor deletes recorded, by how they were asked for.",
        &m.actor_deletes,
    );
    family(
        &mut out,
        "wasmkeep_actor_delete_failures_total",
        "Actor deletes that failed to be recorded (the data stays), by how they were asked for.",
        &m.actor_delete_failures,
    );
    counter(
        &mut out,
        "wasmkeep_expiry_passes_total",
        "Idle-expiry passes this node ran as the collecting node.",
        m.expiry_passes.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_expiry_candidates_total",
        "Idle actors the expiry passes found and acted on.",
        m.expiry_candidates.load(Ordering::Relaxed),
    );
    family(
        &mut out,
        "wasmkeep_expiry_kept_total",
        "Idle-expiry candidates kept, by reason (the record changed since the listing, an alarm, a socket, active here, or failed).",
        &m.expiry_kept,
    );
    counter(
        &mut out,
        "wasmkeep_websocket_sweep_reads_total",
        "Ownership records the WebSocket owner sweep read.",
        m.websocket_sweep_reads.load(Ordering::Relaxed),
    );
    family(
        &mut out,
        "wasmkeep_websocket_sweep_unreadable_total",
        "Ownership records the WebSocket owner sweep could not decode and passed over, by read.",
        &m.websocket_sweep_unreadable,
    );
    head(
        &mut out,
        "wasmkeep_parked_records",
        "gauge",
        "Ownership records kept for actors that hibernated with sockets open on this node.",
    );
    let _ = writeln!(
        out,
        "wasmkeep_parked_records {}",
        m.parked_records.load(Ordering::Relaxed)
    );
    counter(
        &mut out,
        "wasmkeep_parked_wakes_total",
        "Wakes of an actor whose record was parked here, on this node.",
        m.parked_wakes.load(Ordering::Relaxed),
    );
    family(
        &mut out,
        "wasmkeep_parked_releases_total",
        "Parked ownership records released, by reason.",
        &m.parked_releases,
    );
    family(
        &mut out,
        "wasmkeep_plain_events_dropped_total",
        "Events whose writes were committed but not acknowledged when a batched hold ended without a flush, by reason.",
        &m.plain_events_dropped,
    );
    head(
        &mut out,
        "wasmkeep_unsent_age_seconds",
        "histogram",
        "The oldest uncaptured commit's age at each batched capture.",
    );
    m.unsent_age_seconds
        .render(&mut out, "wasmkeep_unsent_age_seconds", "");
    head(
        &mut out,
        "wasmkeep_segment_events",
        "histogram",
        "Events per batched segment.",
    );
    m.segment_events.render(&mut out, "wasmkeep_segment_events");
    head(
        &mut out,
        "wasmkeep_segment_bytes",
        "histogram",
        "Encoded bytes per batched segment.",
    );
    m.segment_bytes.render(&mut out, "wasmkeep_segment_bytes");
    head(
        &mut out,
        "wasmkeep_segment_upload_seconds",
        "histogram",
        "A segment's upload, from its first PUT to the bucket's acknowledgement, resends included; failed uploads are not observed.",
    );
    m.segment_upload_seconds
        .render(&mut out, "wasmkeep_segment_upload_seconds", "");
    counter(
        &mut out,
        "wasmkeep_segment_upload_retries_total",
        "PUTs of a segment sent again after an error, a stall or a race with an earlier attempt.",
        m.segment_upload_retries.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "wasmkeep_segment_upload_failures_total",
        "Segment uploads given up without an acknowledgement.",
        m.segment_upload_failures.load(Ordering::Relaxed),
    );
    family(
        &mut out,
        "wasmkeep_batched_waits_total",
        "Times a batched actor's next event waited at the end of the one before, by reason (age, bytes, budget).",
        &m.batched_waits,
    );
    head(
        &mut out,
        "wasmkeep_flush_seconds",
        "histogram",
        "How long a batched flush took.",
    );
    m.flush_seconds
        .render(&mut out, "wasmkeep_flush_seconds", "");
    gauge(
        &mut out,
        "wasmkeep_upload_budget_bytes_in_use",
        "Bytes of the node's budget of batched segments in flight in use now.",
        crate::actor::upload_budget_in_use(),
    );
    // Each app's part of the shared budgets and its use (ADR 0038,
    // section 8): none on a node that joined no cluster.
    let shares: [(&str, &crate::shares::Shares); 3] = [
        ("uploads", &crate::actor::UPLOAD_SHARES),
        ("restore_reads", &crate::replica::RESTORE_SLOT_SHARES),
        ("restore_megabytes", &crate::replica::RESTORE_BYTE_SHARES),
    ];
    for (name, help, part) in [
        (
            "wasmkeep_budget_share_part",
            "Each app's equal part of a shared budget (uploads in bytes, restore reads, restore megabytes); half of it is a floor never lent.",
            true,
        ),
        (
            "wasmkeep_budget_share_in_use",
            "What each app holds now of a shared budget.",
            false,
        ),
    ] {
        head(&mut out, name, "gauge", help);
        for (budget, s) in shares {
            s.each(|app, app_part, used| {
                let (tenant, app) = app.split_once('/').unwrap_or((app, ""));
                let labels = render_labels(&[("budget", budget), ("tenant", tenant), ("app", app)]);
                let v = if part { app_part } else { used };
                let _ = writeln!(out, "{name}{{{labels}}} {v}");
            });
        }
    }
    // Recorded with their app (ADR 0038, section 9); the page's own app
    // has every outcome from the first scrape, at 0 until counted.
    let app_family =
        |out: &mut String, name: &str, help: &str, f: &Family<AtomicU64>, outcomes: &[&str]| {
            head(out, name, "counter", help);
            let mut rows = BTreeMap::new();
            for o in outcomes {
                for app in &apps {
                    rows.insert(join_labels(app, &render_labels(&[("outcome", o)])), 0);
                }
            }
            f.each(|labels, v| {
                rows.insert(labels.to_owned(), v.load(Ordering::Relaxed));
            });
            for (labels, v) in rows {
                let _ = writeln!(out, "{name}{{{labels}}} {v}");
            }
        };
    head(
        &mut out,
        "wasmkeep_rate_limit_requests_total",
        "counter",
        "Ingress admission decisions, by outcome.",
    );
    for (label, count) in ["admitted", "quota", "capacity", "invalid_key"]
        .iter()
        .zip(&INGRESS)
    {
        let _ = writeln!(
            out,
            "wasmkeep_rate_limit_requests_total{{outcome=\"{label}\"}} {}",
            count.load(Ordering::Relaxed)
        );
    }
    head(
        &mut out,
        "wasmkeep_rate_limit_active_keys",
        "gauge",
        "Retained ingress client buckets on this process.",
    );
    let _ = writeln!(
        out,
        "wasmkeep_rate_limit_active_keys {}",
        crate::rate_limit::active_keys()
    );
    for (name, kind, family) in [
        (
            "wasmkeep_asset_requests_total",
            "counter",
            &m.asset_requests,
        ),
        ("wasmkeep_asset_fetches_total", "counter", &m.asset_fetches),
        ("wasmkeep_asset_bytes_total", "counter", &m.asset_bytes),
        ("wasmkeep_asset_cache_bytes", "gauge", &m.asset_cache),
    ] {
        head(&mut out, name, kind, "Static assets, by tenant and app.");
        family.each(|labels, value| {
            let _ = writeln!(out, "{name}{{{labels}}} {}", value.load(Ordering::Relaxed));
        });
    }
    for (name, kind, help, family) in [
        (
            "wasmkeep_placements_total",
            "counter",
            "Wake-ups this node sent to another node (ADR 0041), by tenant, app, kind and outcome.",
            &m.placements,
        ),
        (
            "wasmkeep_placements_received_total",
            "counter",
            "Placed wake-ups received, by tenant, app and outcome.",
            &m.placements_received,
        ),
        (
            "wasmkeep_placement_kept_total",
            "counter",
            "Placeable wake-ups kept on this node, by tenant, app and reason.",
            &m.placement_kept,
        ),
        (
            "wasmkeep_placement_apart_total",
            "counter",
            "Wake-ups of an actor whose companion this node held (ADR 0041), by tenant, app and outcome.",
            &m.placement_apart,
        ),
        (
            "wasmkeep_placement_spread_total",
            "counter",
            "Wake-ups of a big room's edge spread over the live nodes (ADR 0040), by tenant, app and outcome.",
            &m.placement_spread,
        ),
        (
            "wasmkeep_placement_fill",
            "gauge",
            "This node id's fill in thousandths, by tenant, app and kind.",
            &m.placement_fill,
        ),
    ] {
        head(&mut out, name, kind, help);
        family.each(|labels, value| {
            let _ = writeln!(out, "{name}{{{labels}}} {}", value.load(Ordering::Relaxed));
        });
    }
    app_family(
        &mut out,
        "wasmkeep_blob_sweeps_total",
        "Sweep records of deleted actors' files this node acted on, by tenant, app and outcome.",
        &m.blob_sweeps,
        &BLOB_SWEEP_OUTCOMES,
    );
    app_family(
        &mut out,
        "wasmkeep_blob_node_link_requests_total",
        "Node-link requests answered, by tenant, app and outcome.",
        &m.blob_node_links,
        &BLOB_NODE_LINK_OUTCOMES,
    );
    for (name, kind, help, family) in [
        (
            "wasmkeep_blob_ops_total",
            "counter",
            "File calls, by tenant, app, op and outcome.",
            &m.blob_ops,
        ),
        (
            "wasmkeep_blob_bytes_total",
            "counter",
            "Bytes of files through the host, node links included, by tenant, app and direction.",
            &m.blob_bytes,
        ),
        (
            "wasmkeep_blob_links_total",
            "counter",
            "Links to files made, by tenant, app and kind.",
            &m.blob_links,
        ),
        (
            "wasmkeep_blob_stored_bytes",
            "gauge",
            "Bytes of an app's files at its last hourly count.",
            &m.blob_stored_bytes,
        ),
        (
            "wasmkeep_blob_stored_files",
            "gauge",
            "Files of an app at its last hourly count.",
            &m.blob_stored_files,
        ),
    ] {
        head(&mut out, name, kind, help);
        family.each(|labels, value| {
            let _ = writeln!(out, "{name}{{{labels}}} {}", value.load(Ordering::Relaxed));
        });
    }
    head(
        &mut out,
        "wasmkeep_blob_links_mode",
        "gauge",
        "1 when this process presigns downloads (the start proof passed), 0 for node links only.",
    );
    let _ = writeln!(
        out,
        "wasmkeep_blob_links_mode {}",
        m.blob_links_mode.load(Ordering::Relaxed)
    );
    app_family(
        &mut out,
        "wasmkeep_front_requests_total",
        "Front requests, by tenant, app and outcome.",
        &m.front_requests,
        &FRONT_REQUEST_OUTCOMES,
    );
    head(
        &mut out,
        "wasmkeep_front_request_seconds",
        "histogram",
        "How long front requests took, to the response's last byte, by tenant and app.",
    );
    m.front_request_seconds.each(|labels, h| {
        if h.count.load(Ordering::Relaxed) > 0 {
            h.render(&mut out, "wasmkeep_front_request_seconds", labels);
        }
    });
    head(
        &mut out,
        "wasmkeep_front_cpu_seconds",
        "histogram",
        "Guest CPU time each front request was charged against its budget, instantiation included, by tenant and app.",
    );
    m.front_cpu_seconds.each(|labels, h| {
        if h.count.load(Ordering::Relaxed) > 0 {
            h.render(&mut out, "wasmkeep_front_cpu_seconds", labels);
        }
    });
    head(
        &mut out,
        "wasmkeep_front_fuel",
        "histogram",
        "Wasmtime fuel units each front request consumed, instantiation included; excludes host work.",
    );
    m.front_fuel
        .each(|labels, h| h.0.render_labels(&mut out, "wasmkeep_front_fuel", labels));
    head(
        &mut out,
        "wasmkeep_front_slots_in_use",
        "gauge",
        "Front requests running now.",
    );
    let _ = writeln!(
        out,
        "wasmkeep_front_slots_in_use {}",
        m.front_slots_in_use.load(Ordering::Relaxed)
    );
    app_family(
        &mut out,
        "wasmkeep_front_calls_total",
        "actors.call from the front, by tenant, app and outcome.",
        &m.front_calls,
        &FRONT_CALL_OUTCOMES,
    );
    head(
        &mut out,
        "wasmkeep_admission_refused_total",
        "counter",
        "Requests, WebSocket upgrades and actor calls refused before their event ran, by reason.",
    );
    for (reason, n) in Refusal::ALL.iter().zip(&m.admission_refused) {
        let _ = writeln!(
            out,
            "wasmkeep_admission_refused_total{{{}}} {}",
            render_labels(&[("reason", reason.label())]),
            n.load(Ordering::Relaxed)
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each app's part of the process's shared budgets and what it uses,
    /// once per budget and app.
    #[test]
    fn allocator_metrics_count_actual_results_and_duration() {
        let metrics = AllocatorMetrics::default();
        for released in [false, true, true] {
            metrics.record(wasmkeep_runtime::AllocatorTrim {
                released,
                duration: Duration::from_millis(10),
            });
        }
        let mut out = String::new();
        metrics.render(&mut out);
        assert!(out.contains("wasmkeep_allocator_trims_total{released=\"false\"} 1\n"));
        assert!(out.contains("wasmkeep_allocator_trims_total{released=\"true\"} 2\n"));
        assert!(out.contains("wasmkeep_allocator_trim_seconds_count 3\n"));
        assert!(out.contains("wasmkeep_allocator_trim_seconds_sum 0.03\n"));
        assert!(out.contains("wasmkeep_allocator_trim_seconds_bucket{le=\"0.01\"} 3\n"));
    }

    #[test]
    fn budget_shares_are_on_the_page_by_app() {
        let _shares = [
            crate::actor::UPLOAD_SHARES.join("acme/shop"),
            crate::actor::UPLOAD_SHARES.join("acme/blog"),
        ];
        let _held = crate::actor::UPLOAD_SHARES
            .try_acquire("acme/shop", 1000)
            .unwrap();
        let page = render(Gauges::default());
        let half = crate::replica::NODE_UPLOAD_BYTES / 2;
        for line in [
            format!("\nwasmkeep_budget_share_part{{budget=\"uploads\",tenant=\"acme\",app=\"shop\"}} {half}\n"),
            format!("\nwasmkeep_budget_share_part{{budget=\"uploads\",tenant=\"acme\",app=\"blog\"}} {half}\n"),
            "\nwasmkeep_budget_share_in_use{budget=\"uploads\",tenant=\"acme\",app=\"shop\"} 1000\n".to_owned(),
            "\nwasmkeep_budget_share_in_use{budget=\"uploads\",tenant=\"acme\",app=\"blog\"} 0\n".to_owned(),
        ] {
            assert!(page.contains(&line), "no {line} in\n{page}");
        }
        assert_eq!(
            page.matches("# TYPE wasmkeep_budget_share_part ").count(),
            1
        );
    }

    #[test]
    fn a_process_of_several_apps_has_an_info_line_per_app() {
        let id = |app: &str, node: &str| Identity {
            tenant: "acme".into(),
            app: app.into(),
            node: node.into(),
        };
        let page = render_for_apps(
            Gauges::default(),
            &[id("shop", "shop-1"), id("blog", "blog-1")],
        );
        let v = wasmkeep_cluster::VERSION;
        for line in [
            format!(
                "\nwasmkeep_info{{version=\"{v}\",tenant=\"acme\",app=\"shop\",node=\"shop-1\"}} 1\n"
            ),
            format!(
                "\nwasmkeep_info{{version=\"{v}\",tenant=\"acme\",app=\"blog\",node=\"blog-1\"}} 1\n"
            ),
            "\nwasmkeep_front_requests_total{tenant=\"acme\",app=\"blog\",outcome=\"ok\"} "
                .to_owned(),
        ] {
            assert!(page.contains(&line), "no {line} in\n{page}");
        }
        assert_eq!(page.matches("# TYPE wasmkeep_info ").count(), 1);
    }

    #[test]
    fn a_process_of_several_apps_sums_their_gauges() {
        let g = |resident, websockets, not_ready| Gauges {
            socket_sessions: 7,
            socket_requested: 100,
            socket_effective: 90,
            socket_actor_requested: 10,
            socket_actor_effective: 10,
            resident,
            websockets,
            classes: 2,
            not_ready,
        };
        let p = Gauges::of_process(&[g(3, 1, None), g(4, 2, Some("disk almost full"))]);
        assert_eq!((p.resident, p.websockets, p.classes), (7, 3, 4));
        assert_eq!((p.socket_sessions, p.socket_effective), (7, 90));
        assert_eq!(p.not_ready, Some("disk almost full"));
        assert_eq!(Gauges::of_process(&[]).resident, 0);
    }

    /// Two apps' fronts in one process: each request and call is counted
    /// under its own app, and the sum is the work done.
    #[test]
    fn front_work_is_counted_under_its_own_app() {
        let (shop, blog) = (app_labels("acme", "shop"), app_labels("acme", "blog"));
        front_request(&shop, "ok");
        front_request(&shop, "ok");
        front_request(&blog, "busy");
        front_request_took(&shop, Duration::from_millis(2));
        front_request_took(&blog, Duration::from_millis(2));
        front_call(&blog, "timeout");
        assert_eq!(METRICS.front_requests.total_in(&shop), 2);
        assert_eq!(METRICS.front_requests.total_in(&blog), 1);
        let page = render_for(Gauges::default(), &Identity::default());
        for line in [
            "\nwasmkeep_front_requests_total{tenant=\"acme\",app=\"shop\",outcome=\"ok\"} 2\n",
            "\nwasmkeep_front_requests_total{tenant=\"acme\",app=\"blog\",outcome=\"busy\"} 1\n",
            "\nwasmkeep_front_request_seconds_count{tenant=\"acme\",app=\"shop\"} 1\n",
            "\nwasmkeep_front_request_seconds_count{tenant=\"acme\",app=\"blog\"} 1\n",
            "\nwasmkeep_front_calls_total{tenant=\"acme\",app=\"blog\",outcome=\"timeout\"} 1\n",
        ] {
            assert!(page.contains(line), "no {line} in\n{page}");
        }
    }

    /// The front's series are on an app's page at 0 from the first scrape,
    /// under that app only.
    #[test]
    fn an_apps_page_has_its_front_series_at_zero() {
        let page = render_for(
            Gauges::default(),
            &Identity {
                tenant: "acme".into(),
                app: "shop".into(),
                node: String::new(),
            },
        );
        for o in FRONT_REQUEST_OUTCOMES {
            let line = format!(
                "\nwasmkeep_front_requests_total{{tenant=\"acme\",app=\"shop\",outcome=\"{o}\"}} 0\n"
            );
            assert!(page.contains(&line), "no {line} in\n{page}");
        }
        assert!(
            !page.contains("wasmkeep_front_requests_total{tenant=\"default\""),
            "{page}"
        );
    }

    /// Two apps of one process: each event is counted once, under its own
    /// app, whatever identity the page is rendered for; the sum over apps
    /// is the work done, and the process's series appear once.
    #[test]
    fn events_are_counted_under_their_own_app() {
        let (shop, blog) = (app_labels("acme", "shop"), app_labels("acme", "blog"));
        let ms = Duration::from_millis(3);
        for _ in 0..2 {
            record_event(&shop, "c", "request", "ok", ms);
        }
        for _ in 0..3 {
            record_event(&blog, "c", "request", "ok", ms);
        }
        assert_eq!(METRICS.events.total_in(&shop), 2);
        assert_eq!(METRICS.events.total_in(&blog), 3);
        assert_eq!(METRICS.events.total(), 5);
        for identity in [
            Identity {
                tenant: "acme".into(),
                app: "shop".into(),
                node: "n1".into(),
            },
            Identity::default(),
        ] {
            let page = render_for(Gauges::default(), &identity);
            for line in [
                "\nwasmkeep_events_total{tenant=\"acme\",app=\"shop\",class=\"c\",kind=\"request\",outcome=\"ok\"} 2\n",
                "\nwasmkeep_events_total{tenant=\"acme\",app=\"blog\",class=\"c\",kind=\"request\",outcome=\"ok\"} 3\n",
                "\nwasmkeep_event_duration_seconds_count{tenant=\"acme\",app=\"shop\",class=\"c\",kind=\"fetch\"} 2\n",
                "\nwasmkeep_event_duration_seconds_count{tenant=\"acme\",app=\"blog\",class=\"c\",kind=\"fetch\"} 3\n",
            ] {
                assert!(page.contains(line), "no {line} in\n{page}");
            }
            let events: u64 = page
                .lines()
                .filter(|l| l.starts_with("wasmkeep_events_total{"))
                .filter_map(|l| l.rsplit(' ').next()?.parse::<u64>().ok())
                .sum();
            assert_eq!(events, 5, "{page}");
            assert_eq!(page.matches("\nwasmkeep_info{").count(), 1);
            assert_eq!(page.matches("\nwasmkeep_actors_resident ").count(), 1);
        }
    }

    #[test]
    fn a_family_total_sums_every_label_set() {
        let f: Family<AtomicU64> = Family::default();
        assert_eq!(f.total(), 0);
        inc(&f.get(&[("class", "a"), ("outcome", "ok")]));
        inc(&f.get(&[("class", "a"), ("outcome", "ok")]));
        inc(&f.get(&[("class", "b"), ("outcome", "failed")]));
        assert_eq!(f.total(), 3);
    }

    #[test]
    fn histograms_are_cumulative_and_labels_escaped() {
        let h = Histogram::<Standard>::default();
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
        assert!(
            page.lines()
                .filter(|l| l.starts_with("wasmkeep_not_ready{"))
                .all(|l| l.ends_with("} 0")),
            "{page}"
        );
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

    /// The node's tenant and app for [`render_for`]: `acme/shop` on `n1`.
    fn shop() -> Identity {
        Identity {
            tenant: "acme".into(),
            app: "shop".into(),
            node: "n1".into(),
        }
    }

    #[test]
    fn series_with_a_class_label_carry_the_tenant_and_app() {
        record_event(
            &app_labels("acme", "shop"),
            "c",
            "request",
            "ok",
            Duration::from_millis(3),
        );
        record_event(
            &app_labels("default", "default"),
            "c",
            "request",
            "ok",
            Duration::from_millis(3),
        );
        inc(&METRICS
            .bucket_requests
            .get(&[("operation", "get"), ("status", "ok")]));
        let page = render_for(Gauges::default(), &shop());
        for line in [
            "wasmkeep_events_total{tenant=\"acme\",app=\"shop\",class=\"c\",kind=\"request\",outcome=\"ok\"} 1\n",
            "wasmkeep_event_duration_seconds_bucket{tenant=\"acme\",app=\"shop\",class=\"c\",kind=\"fetch\",le=\"0.005\"} 1\n",
            "wasmkeep_event_duration_seconds_bucket{tenant=\"acme\",app=\"shop\",class=\"c\",kind=\"fetch\",le=\"+Inf\"} 1\n",
            "wasmkeep_event_duration_seconds_count{tenant=\"acme\",app=\"shop\",class=\"c\",kind=\"fetch\"} 1\n",
        ] {
            assert!(page.contains(line), "no {line} in\n{page}");
        }
        assert!(
            page.contains(
                "\nwasmkeep_event_duration_seconds_sum{tenant=\"acme\",app=\"shop\",class=\"c\",kind=\"fetch\"} "
            ),
            "{page}"
        );
        // Without a node's identity (no actors served): the implicit app.
        let page = render(Gauges::default());
        assert!(
            page.contains("wasmkeep_events_total{tenant=\"default\",app=\"default\",class=\"c\",kind=\"request\",outcome=\"ok\"} 1\n"),
            "{page}"
        );
        // Every series with a class has them (its own app's, recorded with
        // the event: ADR 0038, section 9), and so do the front's counters
        // and histogram (ADR 0023); the others are as they were, and
        // gauges of the process stay unlabelled.
        for l in page.lines().filter(|l| !l.starts_with('#')) {
            if l.contains("class=\"") {
                assert!(
                    l.contains("{tenant=\"default\",app=\"default\",")
                        || l.contains("{tenant=\"acme\",app=\"shop\","),
                    "{l}"
                );
            } else if (l.starts_with("wasmkeep_front_") && !l.starts_with("wasmkeep_front_slots"))
                || (l.starts_with("wasmkeep_blob_") && !l.starts_with("wasmkeep_blob_links_mode"))
            {
                assert!(l.contains("{tenant=\"default\",app=\"default\","), "{l}");
            } else if !l.starts_with("wasmkeep_info{") {
                assert!(!l.contains("tenant=") && !l.contains("app="), "{l}");
            }
        }
        for line in [
            "\nwasmkeep_bucket_requests_total{operation=\"get\",status=\"ok\"} 1\n",
            "\nwasmkeep_admission_refused_total{reason=\"access\"} 0\n",
            "\nwasmkeep_actors_resident 0\n",
            "\nwasmkeep_websockets_open 0\n",
        ] {
            assert!(page.contains(line), "no {line} in\n{page}");
        }
    }

    #[test]
    fn the_info_series_names_the_version_tenant_app_and_node() {
        let version = wasmkeep_cluster::VERSION;
        let page = render_for(Gauges::default(), &shop());
        assert!(page.contains("# TYPE wasmkeep_info gauge\n"), "{page}");
        assert!(
            page.contains(&format!(
                "\nwasmkeep_info{{version=\"{version}\",tenant=\"acme\",app=\"shop\",node=\"n1\"}} 1\n"
            )),
            "{page}"
        );
        // Not clustered: no node label.
        let page = render(Gauges::default());
        assert!(
            page.contains(&format!(
                "\nwasmkeep_info{{version=\"{version}\",tenant=\"default\",app=\"default\"}} 1\n"
            )),
            "{page}"
        );
        assert_eq!(page.matches("\nwasmkeep_info{").count(), 1, "{page}");
    }

    /// `docs/monitoring.md` lists every metric on the page.
    #[test]
    fn the_monitoring_docs_list_every_metric() {
        let docs = include_str!("../../../docs/monitoring.md");
        let page = render(Gauges::default());
        let names: Vec<&str> = page
            .lines()
            .filter_map(|l| l.strip_prefix("# TYPE ")?.split(' ').next())
            .collect();
        assert!(names.len() > 20, "{names:?}");
        for name in names {
            assert!(
                docs.contains(&format!("`{name}`")) || docs.contains(&format!("`{name}{{")),
                "docs/monitoring.md does not list {name}"
            );
        }
    }

    /// The alert rules in `deploy/prometheus` name only metrics this page
    /// has, with labels it gives them.
    #[test]
    fn the_alert_rules_use_only_metrics_that_exist() {
        let rules = include_str!("../../../deploy/prometheus/alerts.yml");
        // Without the comments: the header is prose about the file.
        let exprs: String = rules
            .lines()
            .filter(|l| !l.trim_start().starts_with('#'))
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(exprs.matches("- alert: ").count(), 11, "{exprs}");
        // A fresh node's page: the rules' series are there from the start,
        // except those keyed by class, which a class's first event adds.
        let m = Metrics::seeded();
        inc(&m
            .events
            .get(&[("class", "c"), ("kind", "request"), ("outcome", "failed")]));
        let page = render_with(&m, Gauges::default(), &[Identity::default()]);
        let mut names = std::collections::BTreeSet::new();
        let mut rest = exprs.as_str();
        while let Some(at) = rest.find("wasmkeep_") {
            let name: String = rest[at..]
                .chars()
                .take_while(|c| c.is_ascii_lowercase() || *c == '_')
                .collect();
            rest = &rest[at + name.len()..];
            names.insert(name);
        }
        assert!(names.len() >= 7, "{names:?}");
        for name in &names {
            let base = name
                .strip_suffix("_bucket")
                .filter(|b| page.contains(&format!("# TYPE {b} histogram\n")))
                .unwrap_or(name);
            assert!(
                page.contains(&format!("# TYPE {base} ")),
                "an alert rule names {name}, which the metrics page does not have"
            );
        }
        for (metric, label) in [
            ("wasmkeep_events_total", "outcome=\"failed\""),
            ("wasmkeep_events_total", "class=\""),
            ("wasmkeep_bucket_requests_total", "status=\"error\""),
            ("wasmkeep_failover_waits_total", "outcome=\"expired\""),
            ("wasmkeep_failover_waits_total", "outcome=\"over_cap\""),
            ("wasmkeep_admission_refused_total", "reason=\"capacity\""),
            ("wasmkeep_admission_refused_total", "reason=\"overloaded\""),
            ("wasmkeep_admission_refused_total", "reason=\"wrong_app\""),
            ("wasmkeep_admission_refused_total", "reason=\"unavailable\""),
        ] {
            assert!(
                page.lines()
                    .any(|l| l.starts_with(&format!("{metric}{{")) && l.contains(label)),
                "the metrics page has no {metric} series with {label}"
            );
        }
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

    /// A page rendered from a fresh, seeded [`Metrics`]: what a node
    /// serves before anything happened.
    fn fresh_page() -> String {
        render_with(
            &Metrics::seeded(),
            Gauges::default(),
            &[Identity::default()],
        )
    }

    /// Families whose label values are a fixed set have every series on
    /// the page from the start, at 0: their names can be checked and
    /// `increase()` sees the first event.
    #[test]
    fn bounded_families_are_on_the_page_at_zero_from_the_start() {
        let page = fresh_page();
        let mut expected = Vec::new();
        for kind in FAILOVER_KINDS {
            for outcome in FAILOVER_OUTCOMES {
                expected.push(format!(
                    "wasmkeep_failover_waits_total{{kind=\"{kind}\",outcome=\"{outcome}\"}} 0"
                ));
            }
        }
        for operation in BUCKET_OPERATIONS {
            for status in BUCKET_STATUSES {
                expected.push(format!(
                    "wasmkeep_bucket_requests_total{{operation=\"{operation}\",status=\"{status}\"}} 0"
                ));
            }
        }
        for (name, label, values) in [
            ("wasmkeep_event_cuts_total", "reason", &CUT_REASONS[..]),
            ("wasmkeep_forwards_total", "kind", &FORWARD_KINDS[..]),
            (
                "wasmkeep_signatures_refused_full_total",
                "record",
                &SIGNATURE_RECORDS[..],
            ),
            (
                "wasmkeep_websocket_owner_closes_total",
                "reason",
                &OWNER_CLOSE_REASONS[..],
            ),
            (
                "wasmkeep_websocket_sweep_unreadable_total",
                "read",
                &SWEEP_UNREADABLE_READS[..],
            ),
            ("wasmkeep_actor_deletes_total", "how", &DELETE_HOWS[..]),
            (
                "wasmkeep_expiry_kept_total",
                "reason",
                &EXPIRY_KEPT_REASONS[..],
            ),
            (
                "wasmkeep_actor_delete_failures_total",
                "how",
                &DELETE_HOWS[..],
            ),
            (
                "wasmkeep_parked_releases_total",
                "reason",
                &PARKED_RELEASE_REASONS[..],
            ),
            (
                "wasmkeep_plain_events_dropped_total",
                "reason",
                &PLAIN_DROP_REASONS[..],
            ),
        ] {
            for v in values {
                expected.push(format!("{name}{{{label}=\"{v}\"}} 0"));
            }
        }
        for o in FRONT_REQUEST_OUTCOMES {
            expected.push(format!(
                "wasmkeep_front_requests_total{{tenant=\"default\",app=\"default\",outcome=\"{o}\"}} 0"
            ));
        }
        for o in BLOB_SWEEP_OUTCOMES {
            expected.push(format!(
                "wasmkeep_blob_sweeps_total{{tenant=\"default\",app=\"default\",outcome=\"{o}\"}} 0"
            ));
        }
        for o in FRONT_CALL_OUTCOMES {
            expected.push(format!(
                "wasmkeep_front_calls_total{{tenant=\"default\",app=\"default\",outcome=\"{o}\"}} 0"
            ));
        }
        for line in &expected {
            assert!(page.lines().any(|l| l == line), "no {line} in\n{page}");
        }
        // Families keyed by class get a series at a class's first event
        // only: no names beyond the classes that ran.
        assert!(!page.contains("wasmkeep_events_total{"), "{page}");
        assert!(
            !page.contains("wasmkeep_event_duration_seconds_bucket{"),
            "{page}"
        );
    }

    /// Every not-ready reason is a series from the start: 0, and 1 for the
    /// reason the node is not ready, if it is not.
    #[test]
    fn every_not_ready_reason_is_a_series() {
        let reasons = |page: &str| -> Vec<(String, String)> {
            page.lines()
                .filter_map(|l| l.strip_prefix("wasmkeep_not_ready{reason=\""))
                .map(|l| {
                    let (reason, v) = l.split_once("\"} ").unwrap();
                    (reason.to_owned(), v.to_owned())
                })
                .collect()
        };
        let ready = reasons(&fresh_page());
        assert_eq!(ready.len(), NOT_READY_REASONS.len(), "{ready:?}");
        assert!(ready.iter().all(|(_, v)| v == "0"), "{ready:?}");
        let why = "bucket unreachable (node lease not renewed)";
        let not_ready = reasons(&render_with(
            &Metrics::seeded(),
            Gauges {
                not_ready: Some(why),
                ..Gauges::default()
            },
            &[Identity::default()],
        ));
        assert_eq!(not_ready.len(), NOT_READY_REASONS.len(), "{not_ready:?}");
        let ones: Vec<_> = not_ready.iter().filter(|(_, v)| v == "1").collect();
        assert_eq!(ones, [&(why.to_owned(), "1".to_owned())], "{not_ready:?}");
        // A reason missing from the list is still on the page.
        let page = render(Gauges {
            not_ready: Some("something new"),
            ..Gauges::default()
        });
        assert!(
            page.contains("\nwasmkeep_not_ready{reason=\"something new\"} 1\n"),
            "{page}"
        );
    }

    /// The quoted literal after each `needle` in `src`, whitespace ignored:
    /// the first `"..."` before the next `)` or `;`.
    fn literals(src: &str, needle: &str) -> Vec<String> {
        let compact: String = src.chars().filter(|c| !c.is_whitespace()).collect();
        let needle: String = needle.chars().filter(|c| !c.is_whitespace()).collect();
        let mut out = Vec::new();
        let mut rest = compact.as_str();
        while let Some(at) = rest.find(&needle) {
            rest = &rest[at + needle.len()..];
            let end = rest.find([')', ';']).unwrap_or(rest.len());
            let call = &rest[..end];
            if let Some(open) = call.find('"') {
                let value = &call[open + 1..];
                if let Some(close) = value.find('"') {
                    out.push(value[..close].to_owned());
                }
            }
        }
        out
    }

    /// ADR 0034's section 16: the batched uploader's series are on the
    /// page from the start, the waits' reasons seeded at 0, and what is
    /// observed shows in them.
    #[test]
    fn the_batched_series_are_on_the_page() {
        let m = Metrics::seeded();
        m.unsent_age_seconds.observe(Duration::from_millis(30));
        m.segment_events.observe(3);
        m.segment_bytes.observe(5_000);
        crate::metrics::inc(&m.batched_waits.get(&[("reason", "age")]));
        m.flush_seconds.observe(Duration::from_millis(60));
        m.segment_upload_seconds.observe(Duration::from_millis(40));
        m.segment_upload_retries.store(2, Ordering::Relaxed);
        m.segment_upload_failures.store(1, Ordering::Relaxed);
        let page = render_with(&m, Gauges::default(), &[Identity::default()]);
        for want in [
            "wasmkeep_unsent_age_seconds_count 1",
            "wasmkeep_segment_events_bucket{le=\"5\"} 1",
            "wasmkeep_segment_events_count 1",
            "wasmkeep_segment_bytes_bucket{le=\"16384\"} 1",
            "wasmkeep_segment_bytes_sum 5000",
            "wasmkeep_batched_waits_total{reason=\"age\"} 1",
            "wasmkeep_batched_waits_total{reason=\"bytes\"} 0",
            "wasmkeep_batched_waits_total{reason=\"budget\"} 0",
            "wasmkeep_flush_seconds_count 1",
            "wasmkeep_segment_upload_seconds_bucket{le=\"0.05\"} 1",
            "wasmkeep_segment_upload_seconds_count 1",
            "wasmkeep_segment_upload_retries_total 2",
            "wasmkeep_segment_upload_failures_total 1",
            "# TYPE wasmkeep_upload_budget_bytes_in_use gauge",
        ] {
            assert!(page.contains(want), "{want}:\n{page}");
        }
    }

    /// The wide histograms have 0.5, 2 and 2.5 s bounds; the standard one
    /// does not.
    #[test]
    fn wide_histograms_have_the_half_two_and_two_and_a_half_second_bounds() {
        let wide = Histogram::<Wide>::default();
        wide.observe(Duration::from_millis(700));
        wide.observe(Duration::from_millis(2_200));
        let mut out = String::new();
        wide.render(&mut out, "w", "");
        for want in [
            "w_bucket{le=\"0.5\"} 0\n",
            "w_bucket{le=\"1\"} 1\n",
            "w_bucket{le=\"2\"} 1\n",
            "w_bucket{le=\"2.5\"} 2\n",
            "w_bucket{le=\"+Inf\"} 2\n",
        ] {
            assert!(out.contains(want), "{want} in\n{out}");
        }
        let standard = Histogram::<Standard>::default();
        let mut out = String::new();
        standard.render(&mut out, "s", "");
        for gone in ["le=\"0.5\"", "le=\"2\"", "le=\"2.5\""] {
            assert!(!out.contains(gone), "{gone} in\n{out}");
        }
    }

    /// A segment upload recorded by `segment_upload` (what the replica
    /// calls when an upload ends) is on the page: its time when
    /// acknowledged, its resends, and a failure.
    #[test]
    fn a_recorded_segment_upload_shows_on_the_page() {
        let m = &METRICS;
        let (seen, resent, failed) = (
            m.segment_upload_seconds.count(),
            m.segment_upload_retries.load(Ordering::Relaxed),
            m.segment_upload_failures.load(Ordering::Relaxed),
        );
        segment_upload(Duration::from_millis(30), 1, true);
        segment_upload(Duration::from_secs(2), 2, false);
        assert_eq!(m.segment_upload_seconds.count(), seen + 1);
        assert_eq!(m.segment_upload_retries.load(Ordering::Relaxed), resent + 3);
        assert_eq!(
            m.segment_upload_failures.load(Ordering::Relaxed),
            failed + 1
        );
        let page = render_with(m, Gauges::default(), &[Identity::default()]);
        for want in [
            "# TYPE wasmkeep_segment_upload_seconds histogram",
            "wasmkeep_segment_upload_seconds_bucket{le=\"+Inf\"}",
            "# TYPE wasmkeep_segment_upload_retries_total counter",
            "# TYPE wasmkeep_segment_upload_failures_total counter",
        ] {
            assert!(page.contains(want), "{want}:\n{page}");
        }
    }

    /// The scheduling probe's wake-ups reach the page.
    #[tokio::test(start_paused = true)]
    async fn the_schedule_lag_probe_reaches_the_page() {
        let probe = LagProbe::start();
        tokio::time::sleep(Duration::from_millis(350)).await;
        drop(probe);
        assert!(METRICS.runtime_schedule_lag_seconds.count() >= 3);
        let page = render(Gauges::default());
        assert!(
            page.contains("# TYPE wasmkeep_runtime_schedule_lag_seconds histogram"),
            "{page}"
        );
        assert!(
            !page.contains("wasmkeep_runtime_schedule_lag_seconds_count 0\n"),
            "{page}"
        );
    }

    /// The label values seeded at 0 are the ones the code counts: a value
    /// counted somewhere is in its list.
    #[test]
    fn the_seeded_label_values_are_the_ones_counted() {
        let failover = include_str!("cluster/failover_wait.rs");
        let check = |what: &str, found: Vec<String>, list: &[&str]| {
            assert!(!found.is_empty(), "{what}: no call site found");
            for v in &found {
                assert!(
                    list.contains(&v.as_str()),
                    "{what}: {v} is counted but not seeded"
                );
            }
        };
        check(
            "failover outcomes",
            [
                literals(failover, "self.outcome = "),
                literals(failover, "outcome: "),
                literals(failover, "FailoverWait::count(kind, "),
                literals(include_str!("actor/requests.rs"), ".end("),
                literals(include_str!("actor/upgrade.rs"), ".end("),
            ]
            .concat(),
            &FAILOVER_OUTCOMES,
        );
        check(
            "bucket operations",
            literals(include_str!("counted_store.rs"), "record("),
            &BUCKET_OPERATIONS,
        );
        check(
            "event cuts",
            literals(include_str!("actor/host.rs"), "cut("),
            &CUT_REASONS,
        );
        check(
            "forwards",
            literals(include_str!("cluster.rs"), ".forwards.get(&[(\"kind\", "),
            &FORWARD_KINDS,
        );
        check(
            "signature records",
            [
                literals(include_str!("cluster.rs"), "SeenSignatures::named("),
                literals(include_str!("cluster.rs"), "SeenSignatures::with_cap("),
            ]
            .concat(),
            &SIGNATURE_RECORDS,
        );
        check(
            "owner closes",
            [
                include_str!("actor/drain.rs"),
                include_str!("actor/moving.rs"),
                include_str!("actor/socket_owners.rs"),
                include_str!("actor/sockets.rs"),
            ]
            .iter()
            .flat_map(|src| literals(src, "owner_closes("))
            .collect(),
            &OWNER_CLOSE_REASONS,
        );
        check(
            "actor deletes",
            [
                include_str!("actor/host.rs"),
                include_str!("actor/state.rs"),
                include_str!("actor/deletes.rs"),
                include_str!("actor/expiry.rs"),
            ]
            .iter()
            .flat_map(|src| {
                [
                    literals(src, "metrics::deleted("),
                    literals(src, "metrics::delete_failed("),
                ]
                .concat()
            })
            .collect(),
            &DELETE_HOWS,
        );
        check(
            "parked releases",
            [
                include_str!("actor.rs"),
                include_str!("actor/drain.rs"),
                include_str!("actor/moving.rs"),
                include_str!("actor/socket_owners.rs"),
                include_str!("actor/state.rs"),
                include_str!("cluster/lease_hold.rs"),
            ]
            .iter()
            .flat_map(|src| {
                [
                    literals(src, "release_parked("),
                    literals(src, "release_all_parked("),
                    literals(src, "parked_released("),
                ]
                .concat()
            })
            .collect(),
            &PARKED_RELEASE_REASONS,
        );
        check(
            "sweep unreadable",
            literals(
                include_str!("actor/socket_owners.rs"),
                "unreadable(&logged, ",
            )
            .into_iter()
            .chain(literals(
                include_str!("actor/socket_owners.rs"),
                "unreadable(logged, ",
            ))
            .collect(),
            &SWEEP_UNREADABLE_READS,
        );
        check(
            "front requests",
            literals(include_str!("front.rs"), "outcome("),
            &FRONT_REQUEST_OUTCOMES,
        );
    }

    /// The not-ready reasons listed are every reason a node gives
    /// ([`crate::ActorService::not_ready`], the cluster's, and a stopped
    /// node's words).
    #[test]
    fn the_not_ready_reasons_are_every_reason_given() {
        /// The body of the method `fn name(` (to its closing brace at four
        /// spaces), or of a function at none.
        fn body<'a>(src: &'a str, name: &str) -> &'a str {
            let at = src.find(&format!("fn {name}(")).expect(name);
            let rest = &src[at..];
            let end = rest.find("\n    }\n").expect(name);
            &rest[..end]
        }
        fn quoted(src: &str, after: &str) -> Vec<String> {
            src.split(after)
                .skip(1)
                .filter_map(|s| Some(s.split_once('"')?.0.to_owned()))
                .collect()
        }
        let mut given = quoted(body(include_str!("actor.rs"), "not_ready"), "Some(\"");
        given.extend(quoted(
            body(include_str!("cluster.rs"), "not_ready"),
            "Some(\"",
        ));
        given.extend(quoted(
            body(include_str!("cluster/identity.rs"), "words"),
            "=> \"",
        ));
        assert!(given.len() > 10, "{given:?}");
        for reason in &given {
            assert!(
                NOT_READY_REASONS.contains(&reason.as_str()),
                "the not-ready reason {reason:?} is not in NOT_READY_REASONS"
            );
        }
        for reason in NOT_READY_REASONS {
            assert!(
                given.iter().any(|g| g == reason),
                "NOT_READY_REASONS lists {reason:?}, which no code gives"
            );
        }
    }
}

#[cfg(test)]
mod fuel_tests {
    use super::*;

    #[test]
    fn fuel_histograms_preserve_app_class_and_kind() {
        let m = Metrics::default();
        for (app, kind, value) in [("one", "request", 100_000), ("two", "alarm", 2_000_000)] {
            let labels = app_labels("tenant", app);
            m.event_fuel
                .get_in(&labels, &[("class", "room"), ("kind", event_kind(kind))])
                .0
                .observe(value);
        }
        let page = render_with(&m, Gauges::default(), &[]);
        assert!(page.contains("# TYPE wasmkeep_event_fuel histogram"));
        assert!(page.contains("wasmkeep_event_fuel_bucket{tenant=\"tenant\",app=\"one\",class=\"room\",kind=\"fetch\",le=\"100000\"} 1"));
        assert!(page.contains("wasmkeep_event_fuel_sum{tenant=\"tenant\",app=\"two\",class=\"room\",kind=\"alarm\"} 2000000"));
        assert!(page.contains("wasmkeep_event_fuel_count{tenant=\"tenant\",app=\"two\",class=\"room\",kind=\"alarm\"} 1"));
    }
}
