//! The golden trace (ADR 0034, "strict unchanged"): one scripted life of
//! one actor, and the ordered bucket operations it caused, compared with
//! a trace recorded at 0.4.2 (`golden_trace.txt` beside this file). Any
//! operation, key or order that changes fails the test with a diff.
//!
//! The scenario, actor `t/x` on two nodes sharing an in-memory bucket:
//! plain writes, an `exec-durable`, an alarm set then changed, enough
//! writes for a periodic snapshot, a segment upload that fails (and the
//! restore after it), a hibernation and the wake after it, a move to the
//! other node, a `delete-self` there and the event after it, then a
//! `delete-self-durable` and the event after that.
//!
//! What is recorded: each request to the actor's history (every method),
//! and each write or delete of its alarm index entries and of its
//! ownership record, with the node that sent it and what the bucket
//! answered. An LTX object is recorded decoded: its header without the
//! timestamp, its pages, its checksums. Anything random or timed is
//! replaced by a label in order of first appearance: database checksums
//! (`C1`...), alarm nonces (`N1`...), due times (`T1`...); the class
//! version a database records (the component's digest, which follows
//! the path the guest was built in) is masked. A page holding
//! the actor's alarm index entry (which carries a nonce), or a delete's
//! `replaced` label (which carries its time), is recorded as `volatile`. Requests that may start in either order are sorted within
//! each run of them: the reads and listings of the history (a restore
//! reads side by side; a collection pass lists in the background) and
//! the deletes of alarm entries (an event retires one while the pass
//! starts). Heartbeats, listings of the alarm index and of ownership
//! records, and reads of ownership records are left out: they follow the
//! clock, not the actor.
//!
//! To record it again on purpose (a change meant to alter the trace):
//!
//! ```text
//! WASMKEEP_RECORD_TRACE=1 cargo nextest run -p wasmkeep-server --test cluster golden_trace
//! ```
//!
//! then review the diff of `golden_trace.txt` like code.
//!
//! Changed on purpose since 0.4.2: the hibernation takes an idle snapshot
//! at the head, then lists the history to collect below it (deleting
//! nothing: all of it is younger than the retention window), before its
//! release (ADR 0042, section 2, 0.8.0). The move's
//! restore on the other node then reads that snapshot instead of the
//! periodic one and the three segments after it, and the erasure after
//! the `delete-self` deletes it too. Nothing else moved.
//!
//! A second, separate trace (`golden_trace_idle_pass.txt`) records the
//! idle pass of ADR 0042, section 3: writes, a hibernation, and the pass a
//! retention window later, whose operations are its acquisition, one
//! listing, its deletes and its release, in that order. The main trace
//! wakes its actor well inside the window, so no pass appears in it.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::ops::Range;
use std::sync::Mutex;

use bytes::Bytes;
use futures_util::StreamExt as _;
use futures_util::stream::BoxStream;
use object_store::ObjectStoreExt as _;
use object_store::path::Path as StorePath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};

use super::*;

/// Where the expected trace lives.
const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/cluster/golden_trace.txt"
);

/// Due times far in the future, so no alarm fires during the test.
const FIRST_ALARM: u64 = 4_102_444_800_000;
const SECOND_ALARM: u64 = FIRST_ALARM + 60_000;

/// One recorded request, as the bucket saw it.
#[derive(Debug, Clone)]
enum Op {
    /// A step of the scenario starts.
    Step(&'static str),
    Call {
        node: &'static str,
        method: &'static str,
        key: String,
        /// A PUT's mode.
        mode: Option<&'static str>,
        /// A PUT's body, for history objects and ownership records.
        body: Option<Bytes>,
        /// What the bucket answered; `None` while unanswered (a listing,
        /// or a request still in flight).
        outcome: Option<&'static str>,
    },
}

impl Op {
    /// Whether this is a `method` request on a key containing `part`.
    fn is(&self, method: &str, part: &str) -> bool {
        matches!(self, Self::Call { method: m, key, .. } if *m == method && key.contains(part))
    }
}

/// The trace, shared by every node's [`Recording`].
#[derive(Debug, Default)]
struct Log {
    ops: Mutex<Vec<Op>>,
}

impl Log {
    fn step(&self, name: &'static str) {
        self.ops.lock().unwrap().push(Op::Step(name));
    }

    fn start(
        &self,
        node: &'static str,
        method: &'static str,
        key: String,
        mode: Option<&'static str>,
        body: Option<Bytes>,
    ) -> usize {
        let mut ops = self.ops.lock().unwrap();
        ops.push(Op::Call {
            node,
            method,
            key,
            mode,
            body,
            outcome: None,
        });
        ops.len() - 1
    }

    fn answered<T>(&self, at: Option<usize>, result: &object_store::Result<T>) {
        let Some(at) = at else { return };
        if let Op::Call { outcome, .. } = &mut self.ops.lock().unwrap()[at] {
            *outcome = Some(answer(result));
        }
    }

    /// The requests recorded from position `from` on.
    fn since(&self, from: usize) -> Vec<Op> {
        self.ops.lock().unwrap()[from..].to_vec()
    }

    fn len(&self) -> usize {
        self.ops.lock().unwrap().len()
    }

    /// Waits until no request was recorded for a while: what an event
    /// leaves running after its answer (index updates, a collection pass)
    /// has ended.
    async fn quiet(&self) {
        let mut seen = self.len();
        loop {
            tokio::time::sleep(Duration::from_millis(400)).await;
            let now = self.len();
            if now == seen {
                return;
            }
            seen = now;
        }
    }
}

fn answer<T>(result: &object_store::Result<T>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(object_store::Error::NotFound { .. }) => "not_found",
        Err(object_store::Error::AlreadyExists { .. }) => "exists",
        Err(object_store::Error::Precondition { .. }) => "precondition",
        Err(_) => "error",
    }
}

/// Which keys belong in the trace: the actor's own.
#[derive(Debug, Clone)]
struct Keys {
    history: String,
    owner: String,
    alarms: String,
    /// Reads of the ownership record are recorded too (the placed life:
    /// placement must add none).
    owner_reads: bool,
}

impl Keys {
    fn new() -> Self {
        Self {
            history: format!("{}/", test_key(Family::History, "t/x")),
            owner: test_key(Family::Owners, "t/x"),
            alarms: test_key(Family::Alarms, ""),
            owner_reads: false,
        }
    }

    fn is_history(&self, key: &str) -> bool {
        key.starts_with(&self.history) || key == self.history.trim_end_matches('/')
    }

    /// `alarms/<tenant>/<at>/t.t/78/<tag>` (or the legacy form) for `t/x`.
    fn is_alarm(&self, key: &str) -> bool {
        let Some(rest) = key.strip_prefix(&self.alarms) else {
            return false;
        };
        let parts: Vec<&str> = rest.split('/').collect();
        matches!(parts.get(1), Some(&"t" | &"t.t")) && parts.get(2) == Some(&"78")
    }
}

/// A bucket that records the requests a node makes about actor `t/x`.
#[derive(Debug)]
struct Recording {
    inner: Arc<dyn ObjectStore>,
    node: &'static str,
    log: Arc<Log>,
    keys: Keys,
}

impl std::fmt::Display for Recording {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Recording({})", self.inner)
    }
}

impl Recording {
    /// Starts a record of `method` on `key` if it belongs in the trace.
    fn start(
        &self,
        method: &'static str,
        key: &str,
        mode: Option<&'static str>,
        body: Option<&PutPayload>,
    ) -> Option<usize> {
        let history = self.keys.is_history(key);
        let owner = key == self.keys.owner;
        let alarm = self.keys.is_alarm(key);
        let wanted = history
            || ((owner || alarm) && matches!(method, "PUT" | "DELETE" | "COPY" | "RENAME"))
            || (owner && self.keys.owner_reads && matches!(method, "GET" | "HEAD"));
        if !wanted {
            return None;
        }
        let body = body
            .filter(|_| history || owner)
            .map(|p| Bytes::from(p.clone()));
        Some(
            self.log
                .start(self.node, method, key.to_owned(), mode, body),
        )
    }
}

fn mode_name(mode: &PutMode) -> &'static str {
    match mode {
        PutMode::Overwrite => "overwrite",
        PutMode::Create => "create",
        PutMode::Update(_) => "update",
    }
}

#[async_trait::async_trait]
impl ObjectStore for Recording {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let at = self.start(
            "PUT",
            location.as_ref(),
            Some(mode_name(&opts.mode)),
            Some(&payload),
        );
        let result = self.inner.put_opts(location, payload, opts).await;
        self.log.answered(at, &result);
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let at = self.start("MULTIPART", location.as_ref(), None, None);
        let result = self.inner.put_multipart_opts(location, opts).await;
        self.log.answered(at, &result);
        result
    }

    async fn get_opts(
        &self,
        location: &StorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let method = if options.head { "HEAD" } else { "GET" };
        let at = self.start(method, location.as_ref(), None, None);
        let result = self.inner.get_opts(location, options).await;
        self.log.answered(at, &result);
        result
    }

    async fn get_ranges(
        &self,
        location: &StorePath,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let at = self.start("GET_RANGES", location.as_ref(), None, None);
        let result = self.inner.get_ranges(location, ranges).await;
        self.log.answered(at, &result);
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<StorePath>>,
    ) -> BoxStream<'static, object_store::Result<StorePath>> {
        let (log, node, keys) = (Arc::clone(&self.log), self.node, self.keys.clone());
        let recorder = Recording {
            inner: Arc::clone(&self.inner),
            node,
            log,
            keys,
        };
        let locations = locations
            .inspect(move |l| {
                if let Ok(l) = l {
                    // Answered as a stream: the outcome is not tracked.
                    let _ = recorder.start("DELETE", l.as_ref(), None, None);
                }
            })
            .boxed();
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let key = prefix.map_or(String::new(), |p| p.to_string());
        let _ = self.start("LIST", &key, None, None);
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&StorePath>,
        offset: &StorePath,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let key = prefix.map_or(String::new(), |p| p.to_string());
        let _ = self.start("LIST", &key, None, None);
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<ListResult> {
        let key = prefix.map_or(String::new(), |p| p.to_string());
        let at = self.start("LIST_DELIMITED", &key, None, None);
        let result = self.inner.list_with_delimiter(prefix).await;
        self.log.answered(at, &result);
        result
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let at = self.start("COPY", to.as_ref(), None, None);
        let result = self.inner.copy_opts(from, to, options).await;
        self.log.answered(at, &result);
        result
    }

    async fn rename_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        let at = self.start("RENAME", to.as_ref(), None, None);
        let result = self.inner.rename_opts(from, to, options).await;
        self.log.answered(at, &result);
        result
    }
}

/// Labels for values that change from run to run, in order of first
/// appearance.
#[derive(Default)]
struct Labels(HashMap<String, String>, HashMap<char, usize>);

impl Labels {
    fn of(&mut self, kind: char, value: &str) -> String {
        if let Some(label) = self.0.get(&format!("{kind}{value}")) {
            return label.clone();
        }
        let n = self.1.entry(kind).or_default();
        *n += 1;
        let label = format!("{kind}{n}");
        self.0.insert(format!("{kind}{value}"), label.clone());
        label
    }
}

/// The trace as text: one line per request, normalised (see the module
/// documentation).
fn render(ops: &[Op], keys: &Keys, version: &str) -> String {
    let mut labels = Labels::default();
    // Nonces seen in alarm keys, to spot pages that hold one.
    let mut nonces: Vec<String> = Vec::new();
    let mut lines: Vec<(bool, String)> = Vec::new();
    for op in ops {
        let (node, method, key, mode, body, outcome) = match op {
            Op::Step(name) => {
                lines.push((false, format!("## {name}")));
                continue;
            }
            Op::Call {
                node,
                method,
                key,
                mode,
                body,
                outcome,
            } => (node, method, key, mode, body, outcome),
        };
        let shown = if keys.is_history(key) {
            format!("history/{}", key.strip_prefix(&keys.history).unwrap_or(""))
        } else if *key == keys.owner {
            "owner".to_owned()
        } else {
            let rest = key.strip_prefix(&keys.alarms).unwrap_or(key);
            let mut parts: Vec<String> = rest.split('/').map(str::to_owned).collect();
            if let Some(at) = parts.get_mut(0) {
                *at = labels.of('T', at);
            }
            if let Some(tag) = parts.get_mut(3)
                && let Some((generation, nonce)) = tag.split_once('-')
            {
                if !nonces.iter().any(|n| n == nonce) {
                    nonces.push(nonce.to_owned());
                }
                *tag = format!(
                    "{}-{}",
                    generation.trim_start_matches('0'),
                    labels.of('N', nonce)
                );
            }
            format!("alarms/{}", parts.join("/"))
        };
        let mut line = format!("{node} {method} {shown}");
        if let Some(mode) = mode {
            let _ = write!(line, " [{mode}]");
        }
        let _ = write!(line, " -> {}", outcome.unwrap_or("-"));
        if let Some(body) = body {
            if *key == keys.owner {
                let text = String::from_utf8_lossy(body);
                let lines: Vec<&str> = text.lines().collect();
                let holder = lines
                    .get(1)
                    .and_then(|l| l.split(' ').next())
                    .unwrap_or("?");
                let generation = lines.get(2).copied().unwrap_or("?");
                let _ = write!(line, " holder={holder} generation={generation}");
            } else if key.ends_with(".ltx") {
                let _ = write!(line, " {}", ltx(body, &nonces, version, &mut labels));
            }
        }
        // Requests that may start in either order: a restore's reads, run
        // side by side, and a collection pass (its listing) started in the
        // background while the event goes on to retire an alarm entry.
        let unordered = (matches!(*method, "GET" | "HEAD" | "LIST") && keys.is_history(key))
            || (*method == "DELETE" && keys.is_alarm(key));
        lines.push((unordered, line));
    }
    // Sorted within each run of them.
    let mut out = String::new();
    let mut run: Vec<String> = Vec::new();
    for (unordered, line) in lines {
        if unordered {
            run.push(line);
            continue;
        }
        run.sort();
        for l in run.drain(..) {
            out.push_str(&l);
            out.push('\n');
        }
        out.push_str(&line);
        out.push('\n');
    }
    run.sort();
    for l in run {
        out.push_str(&l);
        out.push('\n');
    }
    out
}

/// An LTX object, decoded, without its timestamp.
fn ltx(bytes: &[u8], nonces: &[String], version: &str, labels: &mut Labels) -> String {
    let file = match wasmkeep_storage::ltx::decode(bytes) {
        Ok(file) => file,
        Err(e) => return format!("undecodable: {e}"),
    };
    let h = file.header;
    let pre = if h.pre_apply_checksum == 0 {
        "-".to_owned()
    } else {
        labels.of('C', &h.pre_apply_checksum.to_string())
    };
    let post = labels.of('C', &file.trailer.post_apply_checksum.to_string());
    let pages: Vec<String> = file
        .pages
        .iter()
        .map(|(pgno, data)| {
            // A delete's `replaced` label (ADR 0033) carries the time of
            // the delete, like an alarm entry carries its nonce.
            let volatile = contains(data, b"alarm_indexed")
                || contains(data, b"replaced")
                || nonces.iter().any(|n| contains(data, n.as_bytes()));
            if volatile {
                format!("{pgno}:volatile")
            } else {
                let data = masked(data, version.as_bytes());
                format!(
                    "{pgno}:{:016x}",
                    wasmkeep_storage::ltx::page_checksum(*pgno, &data)
                )
            }
        })
        .collect();
    format!(
        "ltx(flags={} page_size={} commit={} txid={}..{} pre={pre} post={post} wal={}/{}/{}/{} node={} pages=[{}])",
        h.flags,
        h.page_size,
        h.commit,
        h.min_txid,
        h.max_txid,
        h.wal_offset,
        h.wal_size,
        h.wal_salt1,
        h.wal_salt2,
        h.node_id,
        pages.join(" ")
    )
}

/// `data` with every `needle` in it replaced by as many `v`s.
fn masked(data: &[u8], needle: &[u8]) -> Vec<u8> {
    let mut out = data.to_vec();
    if needle.is_empty() {
        return out;
    }
    let mut at = 0;
    while at + needle.len() <= out.len() {
        if &out[at..at + needle.len()] == needle {
            out[at..at + needle.len()].fill(b'v');
            at += needle.len();
        } else {
            at += 1;
        }
    }
    out
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty() && haystack.windows(needle.len()).any(|w| w == needle)
}

/// A line diff of `want` against `got` (longest common subsequence), with
/// two lines of context around each change.
fn diff(want: &str, got: &str) -> String {
    let (a, b): (Vec<&str>, Vec<&str>) = (want.lines().collect(), got.lines().collect());
    let (n, m) = (a.len(), b.len());
    let mut lcs = vec![vec![0u32; m + 1]; n + 1];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            lcs[i][j] = if a[i] == b[j] {
                lcs[i + 1][j + 1] + 1
            } else {
                lcs[i + 1][j].max(lcs[i][j + 1])
            };
        }
    }
    let mut edits: Vec<(char, &str)> = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < n || j < m {
        if i < n && j < m && a[i] == b[j] {
            edits.push((' ', a[i]));
            i += 1;
            j += 1;
        } else if j < m && (i == n || lcs[i][j + 1] >= lcs[i + 1][j]) {
            edits.push(('+', b[j]));
            j += 1;
        } else {
            edits.push(('-', a[i]));
            i += 1;
        }
    }
    let near = |k: usize| {
        edits[k.saturating_sub(2)..(k + 3).min(edits.len())]
            .iter()
            .any(|(c, _)| *c != ' ')
    };
    let mut out = String::new();
    let mut skipped = false;
    for (k, (c, line)) in edits.iter().enumerate() {
        if near(k) {
            if skipped {
                out.push_str("...\n");
                skipped = false;
            }
            let _ = writeln!(out, "{c} {line}");
        } else {
            skipped = true;
        }
    }
    out
}

/// Sends `method path` to `node` until the node runs it: a refusal before
/// the event (this node cannot take the actor now, or has no room for it
/// yet) is sent again.
async fn run(node: &Node, method: &str, path: &str) -> Reply {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let r = send(node.addr, method, path, "").await;
        if r.status != 503 || std::time::Instant::now() >= deadline {
            return r;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Waits until the event just answered by `node` has finished: its
/// upload and its index updates run after its answer, and a read event
/// (which sends nothing to the bucket) runs only after them. Then waits
/// for what it left running in the background.
async fn settled(log: &Log, node: &Node) {
    let r = run(node, "GET", "/actor/t/x/plain").await;
    assert_eq!(r.status, 200, "{}: {}", r.head, r.body);
    log.quiet().await;
}

/// Runs the scenario and returns its trace.
/// `placement`: both nodes with placement at wake-up on (ADR 0041). b
/// then has one resident slot, like a: a full node's wake-up of another
/// actor is evaluated, and kept, since 80% of a's fill is less than one
/// resident on b (rule 4's share of 0), so the actor's life is the same.
async fn scenario(placement: bool) -> String {
    let keys = Keys::new();
    let log = Arc::new(Log::default());
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    // a's segment uploads can be made to fail.
    let flaky = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let recording = |node: &'static str, inner: Arc<dyn ObjectStore>| -> Arc<dyn ObjectStore> {
        Arc::new(Recording {
            inner,
            node,
            log: Arc::clone(&log),
            keys: keys.clone(),
        })
    };
    let seen_by_a = recording("a", Arc::clone(&flaky) as Arc<dyn ObjectStore>);
    let seen_by_b = recording("b", Arc::clone(&shared));
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    // Longer leases than the other tests: a lapse on a loaded machine
    // would end a hold and change the trace. Nothing hibernates unless the
    // test makes it: a holds one actor at most, so waking another sends
    // the first to sleep.
    let adjust = |resident: usize| {
        move |config: &mut ActorConfig| {
            config.mailbox.max_resident = resident;
            let cluster = config.cluster.as_mut().unwrap();
            cluster.lease = LeaseConfig::new(6_000, 500, 1_000, 100).unwrap();
            cluster.placement = placement;
        }
    };
    let idle = Duration::from_secs(600);
    let wall = Duration::from_secs(30);
    let a = node_configured(
        "a",
        dirs.0.path(),
        &seen_by_a,
        idle,
        &["t"],
        test_actor_wasm(),
        wall,
        "",
        adjust(1),
    )
    .await;
    let b = node_configured(
        "b",
        dirs.1.path(),
        &seen_by_b,
        idle,
        &["t"],
        test_actor_wasm(),
        wall,
        "",
        adjust(if placement { 1 } else { usize::MAX }),
    )
    .await;
    settle().await;
    log.quiet().await;
    // Whatever the nodes did before the first event is not the actor's.
    log.ops.lock().unwrap().clear();

    let expect = |r: Reply, body: &str| {
        assert_eq!((r.status, r.body.trim()), (200, body), "{}", r.head);
    };

    log.step("plain writes: the first event takes the actor, then a segment each");
    for n in ["1", "2", "3"] {
        expect(run(&a, "POST", "/actor/t/x/plain").await, n);
        settled(&log, &a).await;
    }

    log.step("exec-durable");
    expect(run(&a, "POST", "/actor/t/x/durable").await, "1");
    settled(&log, &a).await;

    log.step("an alarm set, then changed");
    let path = format!("/actor/t/x/alarm?at={FIRST_ALARM}");
    expect(run(&a, "POST", &path).await, "ok");
    settled(&log, &a).await;
    let path = format!("/actor/t/x/alarm?at={SECOND_ALARM}");
    expect(run(&a, "POST", &path).await, "ok");
    settled(&log, &a).await;

    log.step("plain writes up to a periodic snapshot, and the collection pass after it");
    let mut plain = 3;
    let from = log.len();
    loop {
        plain += 1;
        expect(
            run(&a, "POST", "/actor/t/x/plain").await,
            &plain.to_string(),
        );
        expect(run(&a, "GET", "/actor/t/x/plain").await, &plain.to_string());
        if log.since(from).iter().any(|op| op.is("PUT", "/snapshot/")) {
            break;
        }
        assert!(plain < 200, "no periodic snapshot after {plain} writes");
    }
    eventually("the collection pass after the snapshot", async || {
        let ops = log.since(from);
        let at = ops.iter().position(|op| op.is("PUT", "/snapshot/"));
        at.is_some_and(|at| ops[at..].iter().any(|op| op.is("LIST", "")))
    })
    .await;
    log.quiet().await;
    log.step("two more writes after the snapshot");
    for _ in 0..2 {
        plain += 1;
        expect(
            run(&a, "POST", "/actor/t/x/plain").await,
            &plain.to_string(),
        );
        settled(&log, &a).await;
    }

    log.step("a segment upload that fails: the actor is given up");
    flaky.fail_wal.store(3, Ordering::SeqCst);
    // Committed locally and answered, never in the bucket.
    expect(
        run(&a, "POST", "/actor/t/x/plain").await,
        &(plain + 1).to_string(),
    );
    eventually("every attempt failed and x was given up", async || {
        flaky.fail_wal.load(Ordering::SeqCst) == 0
            && owner_record(&shared, "x")
                .await
                .is_some_and(|(n, _)| n.is_none())
    })
    .await;
    log.quiet().await;
    log.step("the next event restores what the bucket has");
    plain += 1;
    expect(
        run(&a, "POST", "/actor/t/x/plain").await,
        &plain.to_string(),
    );
    settled(&log, &a).await;

    log.step("hibernation: another actor needs a's only slot");
    let r = run(&a, "GET", "/actor/t/y/plain").await;
    assert_eq!(r.status, 200, "{}: {}", r.head, r.body);
    eventually("x hibernated", async || {
        owner_record(&shared, "x")
            .await
            .is_some_and(|(n, _)| n.is_none())
    })
    .await;
    log.quiet().await;
    log.step("wake: the local file is the bucket's head");
    plain += 1;
    expect(
        run(&a, "POST", "/actor/t/x/plain").await,
        &plain.to_string(),
    );
    settled(&log, &a).await;

    log.step("hand-over: x moves to b");
    let key = request_move(&shared, "x", "b").await;
    eventually("x moved to b", async || {
        matches!(
            shared.head(&key).await,
            Err(object_store::Error::NotFound { .. })
        ) && lease_holder(&shared, "x").await.as_deref() == Some("b")
    })
    .await;
    log.quiet().await;
    plain += 1;
    let r = run(&b, "POST", "/actor/t/x/plain").await;
    assert_eq!(r.node.as_deref(), Some("b"));
    expect(r, &plain.to_string());
    settled(&log, &b).await;
    expect(
        run(&b, "GET", "/actor/t/x/alarm").await,
        &SECOND_ALARM.to_string(),
    );

    log.step("delete-self on b, and the erase of the history before it");
    expect(run(&b, "POST", "/actor/t/x/delete").await, "ok");
    eventually("the history before the delete is erased", async || {
        actor_keys(&shared, "x").await.len() == 2
    })
    .await;
    log.quiet().await;
    log.step("the event after the delete starts from an empty database");
    expect(run(&b, "POST", "/actor/t/x/plain").await, "1");
    settled(&log, &b).await;

    log.step("delete-self-durable on b: the reset in the call, the actor given up");
    expect(
        run(&b, "POST", "/actor/t/x/delete-durable").await,
        "deleted",
    );
    eventually("x was given up after its durable delete", async || {
        owner_record(&shared, "x")
            .await
            .is_some_and(|(n, _)| n.is_none())
    })
    .await;
    log.quiet().await;
    log.step("the event after it restores the empty database");
    expect(run(&b, "POST", "/actor/t/x/plain").await, "1");
    settled(&log, &b).await;

    let ops = log.ops.lock().unwrap().clone();
    drop((a, b));
    // The class version the database records is the component's digest,
    // which changes with the path the guest was built in.
    render(&ops, &keys, &sha256_hex(test_actor_wasm()))
}

/// The same life with placement at wake-up on (ADR 0041): the same
/// operations, in the same order.
#[tokio::test(flavor = "multi_thread")]
async fn the_bucket_operations_are_the_same_with_placement_on() {
    if over_tls() {
        return;
    }
    let got = scenario(true).await;
    let want = std::fs::read_to_string(FIXTURE).unwrap_or_default();
    assert!(
        want == got,
        "placement changed the actor's bucket operations (- expected, + now):\n{}",
        diff(&want, &got)
    );
}

/// Where the placed life's trace lives.
const PLACED_FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/cluster/golden_trace_placed.txt"
);

/// A placed life (ADR 0041): a is full, b has room, both place. x's first
/// event, sent to a, is placed on b: one read of x's record on a (the
/// read routing makes anyway), then b's ordinary acquisition and first
/// write; the next request follows the owner hint to b with no read on a.
/// Reads of the record are recorded here, unlike the main trace.
async fn placed_life() -> String {
    let mut keys = Keys::new();
    keys.owner_reads = true;
    let log = Arc::new(Log::default());
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let recording = |node: &'static str| -> Arc<dyn ObjectStore> {
        Arc::new(Recording {
            inner: Arc::clone(&shared),
            node,
            log: Arc::clone(&log),
            keys: keys.clone(),
        })
    };
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let adjust = |resident: usize| {
        move |config: &mut ActorConfig| {
            config.mailbox.max_resident = resident;
            let cluster = config.cluster.as_mut().unwrap();
            cluster.lease = LeaseConfig::new(6_000, 500, 1_000, 100).unwrap();
            cluster.placement = true;
        }
    };
    let idle = Duration::from_secs(600);
    let wall = Duration::from_secs(30);
    let (seen_by_a, seen_by_b) = (recording("a"), recording("b"));
    let a = node_configured(
        "a",
        dirs.0.path(),
        &seen_by_a,
        idle,
        &["t"],
        test_actor_wasm(),
        wall,
        "",
        adjust(1),
    )
    .await;
    let b = node_configured(
        "b",
        dirs.1.path(),
        &seen_by_b,
        idle,
        &["t"],
        test_actor_wasm(),
        wall,
        "",
        adjust(usize::MAX),
    )
    .await;
    settle().await;
    // a's only slot: another actor (not recorded) fills it.
    let r = run(&a, "GET", "/actor/t/w/plain").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.head
    );
    settle().await;
    log.quiet().await;
    log.ops.lock().unwrap().clear();

    log.step("placed: a, full, sends x's first event to b, which takes x");
    let r = run(&a, "POST", "/actor/t/x/plain").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "{}",
        r.head
    );
    assert_eq!(r.body.trim(), "1");
    settled(&log, &b).await;
    log.step("the next request to a follows the hint to b");
    let r = run(&a, "POST", "/actor/t/x/plain").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "{}",
        r.head
    );
    assert_eq!(r.body.trim(), "2");
    settled(&log, &b).await;

    let ops = log.ops.lock().unwrap().clone();
    drop((a, b));
    render(&ops, &keys, &sha256_hex(test_actor_wasm()))
}

#[tokio::test(flavor = "multi_thread")]
async fn the_bucket_operations_of_a_placed_life() {
    if over_tls() {
        return;
    }
    let got = placed_life().await;
    let want = std::fs::read_to_string(PLACED_FIXTURE).unwrap_or_default();
    assert!(
        want == got,
        "the placed life's bucket operations changed (- expected, + now):\n{}",
        diff(&want, &got)
    );
}

/// See the module documentation.
#[tokio::test(flavor = "multi_thread")]
async fn the_bucket_operations_of_one_actors_life_are_those_of_0_4_2() {
    // The bucket's requests are the same over TLS: one copy is enough.
    if over_tls() {
        return;
    }
    let got = scenario(false).await;
    if std::env::var_os("WASMKEEP_RECORD_TRACE").is_some_and(|v| v == "1") {
        std::fs::write(FIXTURE, &got).unwrap();
        eprintln!("recorded {FIXTURE}");
        return;
    }
    let want = std::fs::read_to_string(FIXTURE).unwrap_or_default();
    assert!(
        want == got,
        "the actor's bucket operations changed (- expected at 0.4.2, + now); \
         if that is meant, record the trace again (see golden_trace.rs):\n{}",
        diff(&want, &got)
    );
}

/// Where the idle pass's trace lives (ADR 0042, Tests: a second trace,
/// kept apart so the main one stays a comparison with 0.4.2).
const IDLE_PASS_FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/cluster/golden_trace_idle_pass.txt"
);

/// The idle pass's retention window here: long next to the writes and
/// the hibernation before it (whose own collection then deletes nothing),
/// short enough to wait out.
const IDLE_PASS_WINDOW: Duration = Duration::from_secs(5);

/// One actor's writes, its hibernation, and the idle pass a window later
/// (ADR 0042, section 3): the pass's acquisition, its one listing, its
/// deletes and its release, in that order, and nothing else.
async fn idle_pass_life() -> String {
    let keys = Keys::new();
    let log = Arc::new(Log::default());
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let seen_by_a: Arc<dyn ObjectStore> = Arc::new(Recording {
        inner: Arc::clone(&shared),
        node: "a",
        log: Arc::clone(&log),
        keys: keys.clone(),
    });
    let dir = tempfile::tempdir().unwrap();
    let a = node_configured(
        "a",
        dir.path(),
        &seen_by_a,
        // The actor hibernates when idle: the node keeps room, so the pass
        // is admitted (it never evicts an actor for its job).
        Duration::from_secs(1),
        &["t"],
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        |config: &mut ActorConfig| {
            let cluster = config.cluster.as_mut().unwrap();
            cluster.lease = LeaseConfig::new(6_000, 500, 1_000, 100).unwrap();
            cluster.placement = false;
            cluster.history_retention = IDLE_PASS_WINDOW;
        },
    )
    .await;
    a.service.pause_idle_passes(true);
    settle().await;
    log.quiet().await;
    log.ops.lock().unwrap().clear();
    let expect = |r: Reply, body: &str| {
        assert_eq!((r.status, r.body.trim()), (200, body), "{}", r.head);
    };

    log.step("plain writes: the first event takes the actor, then a segment each");
    for n in ["1", "2", "3"] {
        expect(run(&a, "POST", "/actor/t/x/plain").await, n);
    }
    settled(&log, &a).await;
    log.step(
        "hibernation: the idle snapshot, the collection under its hold (nothing old enough), \
         the release",
    );
    let released = std::time::Instant::now();
    eventually("x hibernated", async || {
        owner_record(&shared, "x")
            .await
            .is_some_and(|(n, _)| n.is_none())
    })
    .await;
    log.quiet().await;
    assert!(
        released.elapsed() < IDLE_PASS_WINDOW,
        "the hibernation took longer than the window"
    );
    assert_eq!(a.service.idle_passes_queued().len(), 1);
    // A window after the release, and its skew bound.
    tokio::time::sleep(
        IDLE_PASS_WINDOW.saturating_sub(released.elapsed()) + Duration::from_millis(300),
    )
    .await;
    log.step("the idle pass: acquisition, one listing, the deletes, the release");
    a.service.idle_passes_due_now();
    a.service.pause_idle_passes(false);
    eventually("the pass ran", async || {
        a.service.idle_passes_queued().is_empty()
            && log.since(0).iter().any(|op| op.is("DELETE", ""))
            && owner_record(&shared, "x")
                .await
                .is_some_and(|(n, _)| n.is_none())
    })
    .await;
    log.quiet().await;
    let ops = log.ops.lock().unwrap().clone();
    drop(a);
    render(&ops, &keys, &sha256_hex(test_actor_wasm()))
}

/// See [`idle_pass_life`].
#[tokio::test(flavor = "multi_thread")]
async fn the_bucket_operations_of_an_idle_pass() {
    if over_tls() {
        return;
    }
    let got = idle_pass_life().await;
    if std::env::var_os("WASMKEEP_RECORD_TRACE").is_some_and(|v| v == "1") {
        std::fs::write(IDLE_PASS_FIXTURE, &got).unwrap();
        eprintln!("recorded {IDLE_PASS_FIXTURE}");
        return;
    }
    let want = std::fs::read_to_string(IDLE_PASS_FIXTURE).unwrap_or_default();
    assert!(
        want == got,
        "the idle pass's bucket operations changed (- expected, + now); \
         if that is meant, record the trace again (see golden_trace.rs):\n{}",
        diff(&want, &got)
    );
}
