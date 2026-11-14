//! What an idle node lists and reads in the bucket, and when it lists
//! sooner (ADR 0009, 0011, 0014 and 0020, amendments of 2026-11-14):
//! `nodes/` every ten heartbeat periods, the known nodes read one by one
//! in between; the intents when `control/changed` changes, or every
//! twenty periods; the alarm index at most every six sweep periods when it
//! has nothing due, at once when the view of the nodes changes.
//!
//! Each node here reaches the shared bucket through its own [`Counted`],
//! which logs every request with the node's own period count (its reads
//! of its own heartbeat, one a period): the checks count periods, not
//! wall time, so a loaded machine does not make them fail.

use super::*;

use object_store::path::Path as StorePath;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    List,
    Get,
    Put,
    Delete,
}

/// One request: what, the key or prefix.
#[derive(Debug, Clone)]
struct Logged {
    op: Op,
    key: String,
}

/// A bucket that logs every request a node sends it.
#[derive(Debug)]
struct Counted {
    inner: Arc<dyn ObjectStore>,
    log: Arc<std::sync::Mutex<Vec<Logged>>>,
}

impl Counted {
    fn over(inner: &Arc<dyn ObjectStore>) -> (Arc<dyn ObjectStore>, Log) {
        let log: Arc<std::sync::Mutex<Vec<Logged>>> = Arc::default();
        let store: Arc<dyn ObjectStore> = Arc::new(Self {
            inner: Arc::clone(inner),
            log: Arc::clone(&log),
        });
        (store, Log(log))
    }

    fn note(&self, op: Op, key: &str) {
        self.log.lock().unwrap().push(Logged {
            op,
            key: key.to_owned(),
        });
    }
}

impl std::fmt::Display for Counted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Counted({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Counted {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.note(Op::Put, location.as_ref());
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.note(Op::Put, location.as_ref());
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &StorePath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.note(Op::Get, location.as_ref());
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, object_store::Result<StorePath>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<StorePath>> {
        use futures_util::StreamExt as _;
        let log = Arc::clone(&self.log);
        self.inner.delete_stream(
            locations
                .map(move |l| {
                    if let Ok(l) = &l {
                        log.lock().unwrap().push(Logged {
                            op: Op::Delete,
                            key: l.as_ref().to_owned(),
                        });
                    }
                    l
                })
                .boxed(),
        )
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.note(Op::List, prefix.map_or("", |p| p.as_ref()));
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&StorePath>,
        offset: &StorePath,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.note(Op::List, prefix.map_or("", |p| p.as_ref()));
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.note(Op::List, prefix.map_or("", |p| p.as_ref()));
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.note(Op::Put, to.as_ref());
        self.inner.copy_opts(from, to, options).await
    }
}

/// A node's request log.
#[derive(Clone)]
struct Log(Arc<std::sync::Mutex<Vec<Logged>>>);

impl Log {
    fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }

    /// The requests from `from` on.
    fn since(&self, from: usize) -> Vec<Logged> {
        self.0.lock().unwrap()[from..].to_vec()
    }

    /// Waits until `id`'s node has refreshed its view `periods` more
    /// times (its reads of its own heartbeat, one a refresh); the log
    /// index then.
    async fn periods(&self, id: &str, periods: usize) -> usize {
        let own = format!("nodes/{id}");
        let from = self.len();
        let started = std::time::Instant::now();
        loop {
            let seen = self
                .since(from)
                .iter()
                .filter(|l| l.op == Op::Get && l.key == own)
                .count();
            if seen >= periods {
                return self.len();
            }
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "{id} refreshed its view {seen} times, not {periods}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// Counts of `entries` by kind, for the idle checks.
#[derive(Debug, Default, PartialEq, Eq)]
struct Rates {
    periods: usize,
    list_nodes: usize,
    list_control: usize,
    list_alarms: usize,
    list_other: usize,
    get_nodes: usize,
    get_marker: usize,
    get_other: usize,
}

fn rates(id: &str, entries: &[Logged]) -> Rates {
    let own = format!("nodes/{id}");
    let mut r = Rates::default();
    for l in entries {
        match l.op {
            Op::List if l.key.starts_with("nodes") => r.list_nodes += 1,
            Op::List
                if l.key.starts_with("control")
                    || l.key.contains("/moves")
                    || l.key.contains("/deletes") =>
            {
                r.list_control += 1;
            }
            Op::List if l.key.contains("alarms") => r.list_alarms += 1,
            Op::List => r.list_other += 1,
            Op::Get if l.key == own => {
                r.periods += 1;
                r.get_nodes += 1;
            }
            Op::Get if l.key.starts_with("nodes/") => r.get_nodes += 1,
            Op::Get if l.key == wasmkeep_cluster::control::CHANGED => r.get_marker += 1,
            Op::Get => r.get_other += 1,
            Op::Put | Op::Delete => {}
        }
    }
    r
}

/// Starts node `id` over its own [`Counted`] view of `shared`.
async fn counted_node(
    id: &str,
    data: &Path,
    shared: &Arc<dyn ObjectStore>,
    wasm: &[u8],
) -> (Node, Log) {
    let (store, log) = Counted::over(shared);
    let node = node_with(id, data, &store, Duration::from_secs(30), "t", wasm).await;
    (node, log)
}

/// The idle cost of three nodes, per node and heartbeat period: a
/// regression in any of the cadences shows here. Before the amendment it
/// was 4 listings (`nodes/`, `control/drain/`, the app's moves and
/// deletes) a period,
/// plus the alarm index every sweep period; 3 reads of heartbeats.
#[tokio::test(flavor = "multi_thread")]
async fn an_idle_node_lists_and_reads_what_its_cadences_say() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs = [(); 3].map(|()| tempfile::tempdir().unwrap());
    let mut nodes = Vec::new();
    for (id, dir) in ["a", "b", "c"].iter().zip(&dirs) {
        nodes.push(counted_node(id, dir.path(), &shared, test_actor_wasm()).await);
    }
    // Past the start: every node has listed and said hello.
    for ((_, log), id) in nodes.iter().zip(["a", "b", "c"]) {
        log.periods(id, 25).await;
    }
    const PERIODS: usize = 120;
    let from: Vec<usize> = nodes.iter().map(|(_, log)| log.len()).collect();
    nodes[0].1.periods("a", PERIODS).await;
    let mut total = Rates::default();
    for (((_, log), id), from) in nodes.iter().zip(["a", "b", "c"]).zip(from) {
        let r = rates(id, &log.since(from));
        eprintln!("{id}: {r:?}");
        let periods = r.periods.max(1);
        let per = |n: usize| n as f64 / periods as f64;
        // `nodes/` every 10 periods (0.1 a period), and three heartbeats
        // read a period, listed or not.
        assert!(
            (0.05..=0.2).contains(&per(r.list_nodes)),
            "{id} listed nodes/ {} times in {periods} periods",
            r.list_nodes
        );
        assert!(
            (2.7..=3.3).contains(&per(r.get_nodes)),
            "{id} read {} heartbeats in {periods} periods",
            r.get_nodes
        );
        // `control/changed` once a period; the intents (three prefixes:
        // drains, the app's moves and deletes) every 20 periods.
        assert!(
            (0.9..=1.1).contains(&per(r.get_marker)),
            "{id} read control/changed {} times in {periods} periods",
            r.get_marker
        );
        assert!(
            (0.1..=0.2).contains(&per(r.list_control)),
            "{id} listed the intents {} times in {periods} periods",
            r.list_control
        );
        // An empty alarm index: a sweep every six sweep periods (200 ms
        // here, two heartbeat periods): one listing in 12 periods.
        assert!(
            (0.04..=0.15).contains(&per(r.list_alarms)),
            "{id} listed the alarm index {} times in {periods} periods",
            r.list_alarms
        );
        assert_eq!(r.list_other, 0, "{id}: {r:?}");
        total.periods += r.periods;
        total.list_nodes += r.list_nodes;
        total.list_control += r.list_control;
        total.list_alarms += r.list_alarms;
        total.get_nodes += r.get_nodes;
        total.get_marker += r.get_marker;
        total.get_other += r.get_other;
    }
    let periods = total.periods as f64;
    let lists = (total.list_nodes + total.list_control + total.list_alarms) as f64 / periods;
    let gets = (total.get_nodes + total.get_marker + total.get_other) as f64 / periods;
    // Printed for the record (with a 3 s period: per second, a third).
    eprintln!("per node and period: {lists:.3} listings, {gets:.3} reads");
    assert!(lists < 0.45, "{lists} listings a period");
    drop(nodes);
}

/// ADR 0014, amended: an intent written with `control/changed` (as the
/// CLI does) is listed within a period; one written without it (a CLI of
/// 0.4.1 and before), by the fallback, within twenty.
#[tokio::test(flavor = "multi_thread")]
async fn an_announced_intent_is_listed_within_a_period_and_an_unannounced_one_by_the_fallback() {
    use object_store::ObjectStoreExt as _;
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let data = tempfile::tempdir().unwrap();
    let (_a, log) = counted_node("a", data.path(), &shared, test_actor_wasm()).await;
    wait_drain_line(&shared, "a", "serving").await;
    log.periods("a", 3).await;

    // Announced: the next period's read of the marker lists.
    let marker = object_store::path::Path::from("control/drain/a");
    shared.put(&marker, Vec::new().into()).await.unwrap();
    announce(&shared, "one").await;
    let from = log.len();
    log.periods("a", 2).await;
    let r = rates("a", &log.since(from));
    assert!(r.list_control >= 1, "not listed after the announce: {r:?}");
    wait_drain_line(&shared, "a", "drained").await;

    // Not announced: no listing until the fallback, then one: counted in
    // a's periods from its last listing of the intents.
    let to = log.periods("a", 2).await;
    shared.delete(&marker).await.unwrap();
    let fallback = wasmkeep_cluster::control::LISTING_FALLBACK_EVERY as usize;
    let is_control = |l: &Logged| l.op == Op::List && l.key.starts_with("control");
    let last = log.since(0)[..to]
        .iter()
        .rposition(is_control)
        .expect("a listed the intents before");
    loop {
        let entries = log.since(last + 1);
        if let Some(next) = entries.iter().position(is_control) {
            let r = rates("a", &entries[..next]);
            assert!(
                (fallback - 1..=fallback + 1).contains(&r.periods),
                "the fallback listed after {} periods, not {fallback}",
                r.periods
            );
            break;
        }
        let r = rates("a", &entries);
        assert!(
            r.periods <= fallback + 2,
            "the fallback never listed: {r:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    wait_drain_line(&shared, "a", "serving").await;
}

/// Writes `control/changed` as the CLI does.
async fn announce(store: &Arc<dyn ObjectStore>, token: &str) {
    let keys = wasmkeep_cluster::object_store::ObjectStoreBucket::new(Arc::clone(store), "");
    wasmkeep_cluster::control::announce(&keys, token)
        .await
        .unwrap();
}

/// ADR 0011 and 0020, amended: a node that starts says hello, and a peer
/// lists `nodes/` within a period or two of the new node's first write,
/// although it lists by schedule only every ten.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_joins_is_listed_by_its_peers_within_a_period() {
    use object_store::ObjectStoreExt as _;
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (_a, log) = counted_node("a", da.path(), &shared, test_actor_wasm()).await;
    // Just past a scheduled listing: the next is ten periods away.
    let started = log.len();
    loop {
        let r = rates("a", &log.since(started));
        if r.list_nodes > 0 {
            break;
        }
        assert!(r.periods < 30, "a never listed: {r:?}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let joined = log.len();
    let b = tokio::spawn({
        let shared = Arc::clone(&shared);
        let dir = db.path().to_owned();
        async move { counted_node("b", &dir, &shared, test_actor_wasm()).await }
    });
    // From b's first write on, in a's periods.
    let key = object_store::path::Path::from("nodes/b");
    while shared.head(&key).await.is_err() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let written = log.len().max(joined);
    loop {
        let r = rates("a", &log.since(written));
        if r.list_nodes > 0 || r.get_nodes > r.periods {
            // Listed, or already reading b's heartbeat one by one.
            assert!(
                r.periods <= 3,
                "seen only after {} periods: {r:?}",
                r.periods
            );
            break;
        }
        assert!(
            r.periods <= 4,
            "a did not list nodes/ within 4 periods of b's start: {r:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // And it was b's start that made a list, not a's schedule (which a
    // slow start could put in that window): b's hello. Over TLS the first
    // round is refused at the handshake (a has not listed b's pin), which
    // is what makes a list; the next round, a period later, lands.
    eventually("a got b's hello", async || {
        metric("wasmkeep_node_hellos_total").await >= 1
    })
    .await;
    drop(b.await.unwrap());
}

/// A counter of this process's metrics page (every node of the test runs
/// in this process: here, only `a` receives hellos).
async fn metric(name: &str) -> u64 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(wasmkeep_server::serve_metrics(
        listener,
        None,
        std::future::pending(),
    ));
    let (status, page) = get_raw(addr, "/metrics").await;
    server.abort();
    assert_eq!(status, 200);
    page.lines()
        .find_map(|l| l.strip_prefix(name).and_then(|v| v.strip_prefix(' ')))
        .unwrap_or_else(|| panic!("no {name} in\n{page}"))
        .trim()
        .parse()
        .unwrap()
}

/// ADR 0009, amended: with nothing due, the sweep waits up to six sweep
/// periods; a node leaving (here a clean stop, `released`) makes the
/// survivors sweep within about one.
#[tokio::test(flavor = "multi_thread")]
async fn a_view_change_wakes_the_alarm_sweep() {
    use object_store::ObjectStoreExt as _;
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (_a, log) = counted_node("a", da.path(), &shared, test_actor_wasm()).await;
    let (b, _) = counted_node("b", db.path(), &shared, test_actor_wasm()).await;
    log.periods("a", 20).await;
    // Right after one of a's sweeps: the next idle one is 12 periods away.
    let from = log.len();
    loop {
        if rates("a", &log.since(from)).list_alarms > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    drop(b);
    let key = object_store::path::Path::from("nodes/b");
    loop {
        let body = shared.get(&key).await.unwrap().bytes().await.unwrap();
        if String::from_utf8_lossy(&body)
            .lines()
            .nth(1)
            .is_some_and(|l| l.starts_with("released"))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stopped = log.len();
    let is_sweep = |l: &Logged| l.op == Op::List && l.key.contains("alarms");
    let last = log.since(0)[..stopped]
        .iter()
        .rposition(is_sweep)
        .expect("a swept before");
    loop {
        let entries = log.since(last + 1);
        if let Some(next) = entries.iter().position(is_sweep) {
            // Woken: well before the idle wait (12 periods) since a's last
            // sweep, whenever b's stop came; and soon after the release.
            let since_sweep = rates("a", &entries[..next]).periods;
            assert!(
                since_sweep < 10,
                "swept {since_sweep} periods after the last sweep"
            );
            let at = last + 1 + next;
            let since_stop = rates("a", &log.since(0)[stopped.min(at)..at]).periods;
            assert!(since_stop <= 5, "swept {since_stop} periods after b left");
            break;
        }
        let r = rates("a", &log.since(stopped));
        assert!(r.periods <= 6, "no sweep after b left: {r:?}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A rolling upgrade: a node of 0.4.1 (here its heartbeat, kept fresh by
/// the test, at an address that answers 404 to everything, as 0.4.1
/// answers a hello) says no hello and ignores one. A new node beside it
/// starts, says hello to it without harm, and reads its heartbeat; a new
/// node already running finds it by its scheduled listing, within ten
/// periods.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_without_hellos_is_found_by_the_scheduled_listing() {
    use object_store::ObjectStoreExt as _;
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let da = tempfile::tempdir().unwrap();
    let (_a, log) = counted_node("a", da.path(), &shared, test_actor_wasm()).await;
    log.periods("a", 3).await;

    // The old node: answers 404, counts the hellos it gets.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let old_addr = listener.local_addr().unwrap();
    let hellos = Arc::new(AtomicUsize::new(0));
    tokio::spawn({
        let hellos = Arc::clone(&hellos);
        async move {
            loop {
                let Ok((mut s, _)) = listener.accept().await else {
                    return;
                };
                let hellos = Arc::clone(&hellos);
                tokio::spawn(async move {
                    let mut buf = vec![0; 4096];
                    let n = s.read(&mut buf).await.unwrap_or(0);
                    if String::from_utf8_lossy(&buf[..n]).starts_with("POST /_wasmkeep/hello ") {
                        hellos.fetch_add(1, Ordering::SeqCst);
                    }
                    let _ = s
                        .write_all(
                            b"HTTP/1.1 404 Not Found\r\ncontent-length: 10\r\n\r\nnot found\n",
                        )
                        .await;
                });
            }
        }
    });
    let key = object_store::path::Path::from("nodes/old");
    let beat = {
        let shared = Arc::clone(&shared);
        let key = key.clone();
        tokio::spawn(async move {
            loop {
                let body = format!(
                    "wasmkeep-node 3\nlive feedface\n{}\nold\nhttp://{old_addr}\n0.4.1\ndefault/default\n\n\nserving\n\ntake\n",
                    now_ms()
                );
                shared.put(&key, body.into_bytes().into()).await.unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };
    // a finds it by a scheduled listing, then reads it one by one.
    let from = log.len();
    loop {
        let entries = log.since(from);
        let r = rates("a", &entries);
        if entries
            .iter()
            .any(|l| l.op == Op::Get && l.key == "nodes/old")
        {
            assert!(r.periods <= 12, "found after {} periods", r.periods);
            break;
        }
        assert!(
            r.periods <= 12,
            "a never read the old node's heartbeat: {r:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // A new node starting beside it says hello to both; the old one's 404
    // changes nothing.
    let db = tempfile::tempdir().unwrap();
    let (_b, blog) = counted_node("b", db.path(), &shared, test_actor_wasm()).await;
    blog.periods("b", 3).await;
    eventually("the old node got b's hello", async || {
        hellos.load(Ordering::SeqCst) >= 1
    })
    .await;
    let from = blog.len();
    blog.periods("b", 2).await;
    assert!(
        blog.since(from)
            .iter()
            .any(|l| l.op == Op::Get && l.key == "nodes/old"),
        "b does not follow the old node"
    );
    beat.abort();
}
