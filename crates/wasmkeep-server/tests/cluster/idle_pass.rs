//! The idle pass (ADR 0042, section 3): one retention window after a hold
//! that wrote released an actor, its node takes the record again, only if
//! it is still the one that release wrote, and deletes the history below
//! the idle snapshot. What it leaves restores the same database; a wake,
//! a drain, or a recreated app make it compact nothing.

use super::*;
use wasmkeep_runtime::ActorAddress;
use wasmkeep_server::history::{self, Point};
use wasmkeep_storage::{SqlValue, readonly};

/// The retention window of these tests: long next to a burst of writes
/// and a hibernation, so what the passes delete is what the window kept.
const WINDOW: Duration = Duration::from_secs(3);

/// Idle time before an actor leaves memory.
const IDLE: Duration = Duration::from_millis(300);

/// A node of class `t` with placement off, a window of `retention` and
/// idle compaction on, its passes paused until the test lets them run.
async fn compacting(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    retention: Duration,
) -> Node {
    let node = node_configured(
        id,
        data,
        store,
        IDLE,
        &["t"],
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        move |c: &mut ActorConfig| {
            no_placement(c);
            let cluster = c.cluster.as_mut().unwrap();
            cluster.history_retention = retention;
            cluster.idle_compaction = true;
        },
    )
    .await;
    node.service.pause_idle_passes(true);
    node
}

/// `count` durable writes to `t/<name>` on `node`, from `from` + 1.
async fn writes(node: &Node, name: &str, from: u64, count: u64) {
    for n in from + 1..=from + count {
        let r = send(node.addr, "POST", &format!("/actor/t/{name}/durable"), "").await;
        assert_eq!(
            (r.status, r.body.trim()),
            (200, n.to_string().as_str()),
            "{}",
            r.body
        );
    }
}

/// Waits until `node` holds no actor in memory, and its release landed.
async fn hibernated(node: &Node) {
    eventually("the actor hibernates", async || {
        node.service.resident() == 0
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
}

/// The counter `wasmkeep_idle_compactions_total{outcome}`.
async fn compactions(outcome: &str) -> u64 {
    super::snapshots::counter(&format!(
        r#"wasmkeep_idle_compactions_total{{outcome="{outcome}"}}"#
    ))
    .await
}

/// A verified export of `t/<name>` at `point`: what it read and the
/// durable count in it.
async fn export(store: &Arc<dyn ObjectStore>, name: &str, point: Point) -> (history::Export, i64) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("export.db");
    let result = history::export(
        store,
        &StateRoot::new("", "default", "default"),
        &ActorAddress::new("default", "t", name).unwrap(),
        point,
        &path,
        1 << 20,
    )
    .await
    .unwrap();
    let rows = readonly::query(&path, "SELECT value FROM config WHERE key='durable'").unwrap();
    let SqlValue::Integer(n) = rows.values[0][0] else {
        panic!("not an integer")
    };
    (result, n)
}

const OUTCOMES: [&str; 7] = [
    "done",
    "continued",
    "stopped",
    "changed",
    "nothing",
    "refused",
    "busy",
];

/// Every outcome's count.
async fn all_compactions() -> Vec<u64> {
    let mut counts = Vec::new();
    for outcome in OUTCOMES {
        counts.push(compactions(outcome).await);
    }
    counts
}

/// Lets `node`'s one remembered pass run now, and waits until it was
/// counted: its outcome.
async fn run_passes(node: &Node) -> &'static str {
    let before = all_compactions().await;
    node.service.idle_passes_due_now();
    node.service.pause_idle_passes(false);
    eventually("the idle pass ran", async || {
        node.service.idle_passes_queued().is_empty() && all_compactions().await != before
    })
    .await;
    // Its release and the hibernation of its job.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after = all_compactions().await;
    let moved: Vec<&str> = OUTCOMES
        .iter()
        .zip(before.iter().zip(&after))
        .filter(|(_, (b, a))| a > b)
        .map(|(o, _)| *o)
        .collect();
    assert_eq!(moved.len(), 1, "{moved:?}");
    moved[0]
}

/// Lets `node`'s one remembered pass run now, waits until it was counted,
/// and stops the node taking another: its outcome. Unlike [`run_passes`],
/// the queue may hold the pass's continuation after it.
async fn run_one_pass(node: &Node) -> &'static str {
    let before = all_compactions().await;
    node.service.idle_passes_due_now();
    node.service.pause_idle_passes(false);
    eventually("the idle pass ran", async || {
        all_compactions().await != before
    })
    .await;
    node.service.pause_idle_passes(true);
    // Its job's hibernation.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after = all_compactions().await;
    let moved: Vec<&str> = OUTCOMES
        .iter()
        .zip(before.iter().zip(&after))
        .filter(|(_, (b, a))| a > b)
        .map(|(o, _)| *o)
        .collect();
    assert_eq!(moved.len(), 1, "{moved:?}");
    moved[0]
}

/// Two bursts of `t/<name>` on `a`, hibernated, the window passed: the
/// keys then, and the export at the head.
async fn burst_and_rest(
    a: &Node,
    store: &Arc<dyn ObjectStore>,
    name: &str,
) -> (Vec<String>, history::Export) {
    writes(a, name, 0, 5).await;
    hibernated(a).await;
    writes(a, name, 5, 5).await;
    hibernated(a).await;
    assert_eq!(a.service.idle_passes_queued().len(), 1);
    let before = actor_keys(store, name).await;
    let (head, n) = export(store, name, Point::Head).await;
    assert_eq!(n, 10);
    tokio::time::sleep(WINDOW + Duration::from_millis(200)).await;
    (before, head)
}

/// The fleet's case (an actor with thousands of objects lost one batch
/// and stopped), made deterministic: each pass deletes one batch of 2,
/// then stops as at its deadline. Each stopped pass is counted
/// `continued` and queues the next on the record its own release wrote,
/// one generation up, for the writer's snapshot; the last is `done`. The
/// prefix ends as the snapshot at the head and its segment, the database
/// unchanged, and a restore on the other node reads it.
#[tokio::test(flavor = "multi_thread")]
async fn a_pass_stopped_at_its_deadline_is_continued_until_the_history_is_compact() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = compacting("a", da.path(), &store, WINDOW).await;
    let b = compacting("b", db.path(), &store, WINDOW).await;
    settle().await;
    let (before, head) = burst_and_rest(&a, &store, "long").await;
    let gone = before.len() - 2;
    assert!(gone >= 5, "not several batches: {before:?}");
    a.service.limit_idle_passes(Some((2, 1)));
    let passes = gone.div_ceil(2);
    for pass in 1..=passes {
        let outcome = run_one_pass(&a).await;
        let left = actor_keys(&store, "long").await;
        assert_eq!(
            left.len(),
            before.len() - (2 * pass).min(gone),
            "pass {pass}"
        );
        let queued = a.service.idle_passes_queued();
        if pass < passes {
            assert_eq!(outcome, "continued", "pass {pass}");
            // The continuation: on the record this pass's release wrote,
            // at its hold's generation.
            let (holder, generation) = owner_record(&store, "long").await.unwrap();
            assert_eq!(holder, None);
            assert_eq!(generation, head.position.generation + pass as u64);
            assert_eq!(queued.len(), 1, "pass {pass}: {queued:?}");
            assert_eq!(queued[0].1, generation, "pass {pass}");
        } else {
            assert_eq!(outcome, "done", "pass {pass}");
            assert!(queued.is_empty(), "{queued:?}");
        }
    }
    let after = actor_keys(&store, "long").await;
    assert_eq!(after.len(), 2, "not S and the segment at P: {after:?}");
    let (again, n) = export(&store, "long", Point::Head).await;
    assert_eq!(
        (again.position, again.raw_checksum, n),
        (head.position, head.raw_checksum, 10)
    );
    assert_eq!(
        again.owner_generation,
        Some(head.position.generation + passes as u64),
        "one generation per pass"
    );
    let r = send(b.addr, "GET", "/actor/t/long/durable", "").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "10", Some("b")),
        "{}",
        r.body
    );
}

/// A write between two passes of a chain: the actor woken on the other
/// node acquires the record the first pass released, writes and
/// hibernates. The continuation then finds the record changed, as a first
/// pass after a wake does, and deletes nothing; on the node that queued
/// it, any acquisition (a wake here) forgets it instead.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_between_two_passes_stops_the_chain() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = compacting("a", da.path(), &store, WINDOW).await;
    let b = compacting("b", db.path(), &store, WINDOW).await;
    settle().await;
    let (before, _) = burst_and_rest(&a, &store, "between").await;
    assert!(before.len() - 2 >= 5, "not several batches: {before:?}");
    a.service.limit_idle_passes(Some((2, 1)));
    assert_eq!(run_one_pass(&a).await, "continued");
    assert_eq!(a.service.idle_passes_queued().len(), 1);
    // Elsewhere: b takes the record the pass released, and writes.
    let r = send(b.addr, "POST", "/actor/t/between/durable", "").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "11", Some("b")),
        "{}",
        r.body
    );
    hibernated(&b).await;
    let keys = actor_keys(&store, "between").await;
    let (head, n) = export(&store, "between", Point::Head).await;
    assert_eq!(n, 11);
    assert_eq!(run_one_pass(&a).await, "changed");
    assert!(a.service.idle_passes_queued().is_empty());
    assert_eq!(actor_keys(&store, "between").await, keys, "deleted");
    let (again, n) = export(&store, "between", Point::Head).await;
    assert_eq!(
        (again.position, again.raw_checksum, n),
        (head.position, head.raw_checksum, 11)
    );
    // Here: a wake between two passes forgets the continuation.
    let (before, _) = burst_and_rest(&a, &store, "here").await;
    assert!(before.len() - 2 >= 5, "not several batches: {before:?}");
    assert_eq!(run_one_pass(&a).await, "continued");
    assert_eq!(a.service.idle_passes_queued().len(), 1);
    let r = send(a.addr, "GET", "/actor/t/here/durable", "").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "10", Some("a")),
        "{}",
        r.body
    );
    assert!(
        a.service.idle_passes_queued().is_empty(),
        "the wake here forgot it"
    );
}

/// ADR 0042, Tests, "Restore identical": a burst over two generations,
/// hibernated, then the pass a window later. The prefix is exactly the
/// snapshot at the head and the segment at the head; a restore on the
/// other node, and an export, give the database from before the pass,
/// which passes SQLite's integrity check. Before the pass, a point in
/// time inside the window is still there.
#[tokio::test(flavor = "multi_thread")]
async fn an_idle_pass_leaves_the_snapshot_and_its_segment_and_the_same_database() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = compacting("a", da.path(), &store, WINDOW).await;
    let b = compacting("b", db.path(), &store, WINDOW).await;
    settle().await;
    writes(&a, "x", 0, 5).await;
    hibernated(&a).await;
    let between = now_ms() as i64;
    tokio::time::sleep(Duration::from_millis(20)).await;
    writes(&a, "x", 5, 5).await;
    hibernated(&a).await;
    let queued = a.service.idle_passes_queued();
    assert_eq!(queued.len(), 1, "one entry per actor: {queued:?}");
    assert!(queued[0].0.ends_with("/t/x"), "{queued:?}");
    let before = actor_keys(&store, "x").await;
    assert!(before.len() > 2, "nothing to compact: {before:?}");
    let (head, n) = export(&store, "x", Point::Head).await;
    assert_eq!(n, 10);
    // `actor export --at` inside the window, before the pass.
    let (_, at) = export(&store, "x", Point::At(between)).await;
    assert_eq!(at, 5, "the point between the two bursts");

    // A window (and the skew bound) after the release.
    tokio::time::sleep(WINDOW + Duration::from_millis(200)).await;
    let collected = super::snapshots::counter("wasmkeep_history_collected_total").await;
    assert_eq!(run_passes(&a).await, "done");
    let after = actor_keys(&store, "x").await;
    assert_eq!(after.len(), 2, "not S and the segment at P: {after:?}");
    let at_head = |kind: &str| {
        let pos = format!(
            "{:016x}-{:016x}",
            head.position.txid, head.position.generation
        );
        after
            .iter()
            .any(|k| k.contains(&format!("/{kind}/")) && k.contains(&pos))
    };
    assert!(at_head("snapshot") && at_head("wal"), "{after:?}");
    assert!(
        super::snapshots::counter("wasmkeep_history_collected_total").await
            >= collected + (before.len() - 2) as u64,
        "the pass's deletes are counted"
    );
    let (again, n) = export(&store, "x", Point::Head).await;
    assert_eq!(
        (again.position, again.raw_checksum, n),
        (head.position, head.raw_checksum, 10)
    );
    // The snapshot, and the segment at P for its capture stamp.
    assert!(again.objects <= 2, "read {} objects", again.objects);
    // The record is one generation past the head now (the pass's hold),
    // as after a wake that only read: `--require-owner-match` refuses
    // until the actor next writes.
    assert_eq!(
        again.owner_generation,
        Some(head.position.generation + 1),
        "the pass's release"
    );
    let dir = tempfile::tempdir().unwrap();
    let strict = history::export_with_options(
        &store,
        &StateRoot::new("", "default", "default"),
        &ActorAddress::new("default", "t", "x").unwrap(),
        Point::Head,
        &dir.path().join("strict.db"),
        history::Options {
            require_owner_match: true,
            ..history::Options::default()
        },
    )
    .await;
    assert!(
        matches!(strict, Err(history::Error::OwnerMismatch)),
        "{strict:?}"
    );
    // A restore on the other node.
    let r = send(b.addr, "GET", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "10", Some("b")),
        "{}",
        r.body
    );
    // b's restore read the compacted history; what it left is that
    // database, byte for byte.
    hibernated(&b).await;
    let (after_b, _) = export(&store, "x", Point::Head).await;
    assert_eq!(
        (after_b.position, after_b.raw_checksum),
        (head.position, head.raw_checksum)
    );
}

/// ADR 0042, Tests, "Readers": an export that listed before the pass and
/// reads while it deletes (its segment reads held back) still publishes
/// the same database. What it plans from (the snapshot at the head and the
/// segment there) is never in the pass's set.
#[tokio::test(flavor = "multi_thread")]
async fn an_export_racing_a_pass_publishes_the_same_database() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let da = tempfile::tempdir().unwrap();
    let a = compacting("a", da.path(), &store, Duration::from_millis(500)).await;
    settle().await;
    writes(&a, "e", 0, 6).await;
    hibernated(&a).await;
    let (head, _) = export(&store, "e", Point::Head).await;
    let slow = Arc::new(Flaky::new(
        Arc::clone(&store),
        Arc::new(AtomicBool::new(false)),
    ));
    slow.delay_wal_get_ms.store(1_500, Ordering::SeqCst);
    let reader: Arc<dyn ObjectStore> = Arc::clone(&slow) as Arc<dyn ObjectStore>;
    tokio::time::sleep(Duration::from_millis(700)).await;
    let pass = async {
        // The export has listed; its segment read waits.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let outcome = run_passes(&a).await;
        (outcome, actor_keys(&store, "e").await.len())
    };
    let ((raced, n), (outcome, left)) = tokio::join!(export(&reader, "e", Point::Head), pass);
    assert_eq!((outcome, left), ("done", 2));
    assert_eq!(
        (raced.position, raced.raw_checksum, n),
        (head.position, head.raw_checksum, 6)
    );
}

/// A wake before the pass, even one that only reads, rewrites the record.
/// On the node that remembers the pass, the wake's acquisition forgets it.
/// On another node, the pass finds the record changed (one read, before
/// its job is submitted) and leaves every object.
#[tokio::test(flavor = "multi_thread")]
async fn a_wake_before_the_pass_leaves_every_object() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = compacting("a", da.path(), &store, Duration::from_millis(500)).await;
    let b = compacting("b", db.path(), &store, Duration::from_millis(500)).await;
    settle().await;
    // Here: forgotten at the wake.
    writes(&a, "w", 0, 4).await;
    hibernated(&a).await;
    assert_eq!(a.service.idle_passes_queued().len(), 1);
    let r = send(a.addr, "GET", "/actor/t/w/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "4"), "{}", r.body);
    hibernated(&a).await;
    assert!(
        a.service.idle_passes_queued().is_empty(),
        "the wake here forgot it"
    );
    // Elsewhere: found changed.
    writes(&a, "w", 4, 1).await;
    hibernated(&a).await;
    assert_eq!(a.service.idle_passes_queued().len(), 1);
    let r = send(b.addr, "GET", "/actor/t/w/durable", "").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "5", Some("b")),
        "{}",
        r.body
    );
    hibernated(&b).await;
    let before = actor_keys(&store, "w").await;
    tokio::time::sleep(Duration::from_millis(700)).await;
    let outcome = run_passes(&a).await;
    assert_eq!(actor_keys(&store, "w").await, before);
    assert_eq!(outcome, "changed");
}

/// A draining node runs no pass: its entries are dropped, nothing listed
/// or deleted.
#[tokio::test(flavor = "multi_thread")]
async fn a_draining_node_runs_no_pass() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = compacting("a", da.path(), &store, Duration::from_millis(500)).await;
    let _b = compacting("b", db.path(), &store, Duration::from_millis(500)).await;
    settle().await;
    writes(&a, "d", 0, 4).await;
    hibernated(&a).await;
    assert_eq!(a.service.idle_passes_queued().len(), 1);
    let before = actor_keys(&store, "d").await;
    drain(&store, "a").await;
    wait_drain_line(&store, "a", "drained").await;
    tokio::time::sleep(Duration::from_millis(700)).await;
    let outcome = run_passes(&a).await;
    assert_eq!(actor_keys(&store, "d").await, before);
    assert_eq!(outcome, "refused");
}

/// ADR 0042, constraint 7, the recreate case. The app is deleted and
/// recreated while node a keeps running (it never saw the tombstone: in
/// this harness nodes do not follow deploys, as a node paused through the
/// delete would not). The recreated app writes another history for the
/// same actor, up to the same generation, and its released record is
/// given the version a remembered (on S3 the `ETag` of a version 1
/// released record at the same generation is the same, a hash of the same
/// bytes). The pass on a takes that record, but the app's epoch (its
/// `recreated` marker) is not the one a started with: it deletes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_pass_deletes_nothing_of_a_recreated_app() {
    use object_store::ObjectStoreExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, dc) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = compacting("a", da.path(), &store, Duration::from_millis(500)).await;
    settle().await;
    // The first incarnation: two generations.
    writes(&a, "r", 0, 3).await;
    hibernated(&a).await;
    writes(&a, "r", 3, 3).await;
    hibernated(&a).await;
    let queued = a.service.idle_passes_queued();
    assert_eq!(queued.len(), 1, "{queued:?}");
    let generation = queued[0].1;
    // `app delete`: the actor's history and record are gone. Then
    // `deploy --recreate` leaves its marker.
    for key in actor_keys(&store, "r").await {
        store
            .delete(&object_store::path::Path::from(key))
            .await
            .unwrap();
    }
    let record = object_store::path::Path::from(test_key(Family::Owners, "t/r"));
    store.delete(&record).await.unwrap();
    store
        .put(
            &object_store::path::Path::from("apps/default/default/recreated"),
            b"deleted by the test".to_vec().into(),
        )
        .await
        .unwrap();
    // The recreated app's node writes another history, up to the same
    // generation, keeping it all (its own window is long).
    let c = compacting("c", dc.path(), &store, Duration::from_secs(600)).await;
    settle().await;
    writes(&c, "r", 0, 4).await;
    hibernated(&c).await;
    writes(&c, "r", 4, 4).await;
    hibernated(&c).await;
    assert_eq!(
        c.service.idle_passes_queued().first().map(|e| e.1),
        Some(generation),
        "the recreated app's release is not at the same generation"
    );
    let before = actor_keys(&store, "r").await;
    assert!(before.len() > 2, "{before:?}");
    // The remembered version, as an `ETag` of the same bytes would be.
    a.service.idle_passes_take_current_versions().await;
    tokio::time::sleep(Duration::from_millis(700)).await;
    let outcome = run_passes(&a).await;
    assert_eq!(
        actor_keys(&store, "r").await,
        before,
        "a's pass deleted objects of the recreated app"
    );
    assert_eq!(outcome, "refused");
    let r = send(c.addr, "GET", "/actor/t/r/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "8"), "{}", r.body);
}

/// ADR 0042, constraint 8: a class with an idle limit is never compacted,
/// since the pass's two rewrites of the record would restart the actor's
/// idle clock (as built, in either record format: see the ADR).
#[tokio::test(flavor = "multi_thread")]
async fn a_class_with_an_idle_limit_is_not_compacted() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let da = tempfile::tempdir().unwrap();
    let a = node_configured(
        "a",
        da.path(),
        &store,
        IDLE,
        &["t"],
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        |c: &mut ActorConfig| {
            no_placement(c);
            c.expire_after_idle
                .insert("t".into(), Duration::from_secs(3600));
            c.cluster.as_mut().unwrap().history_retention = Duration::from_millis(500);
        },
    )
    .await;
    a.service.pause_idle_passes(true);
    settle().await;
    writes(&a, "l", 0, 4).await;
    hibernated(&a).await;
    let before = actor_keys(&store, "l").await;
    tokio::time::sleep(Duration::from_millis(700)).await;
    let outcome = run_passes(&a).await;
    assert_eq!(actor_keys(&store, "l").await, before);
    assert_eq!(outcome, "refused");
}

/// With idle compaction off, a release remembers nothing.
#[tokio::test(flavor = "multi_thread")]
async fn no_pass_is_queued_with_idle_compaction_off() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let da = tempfile::tempdir().unwrap();
    let a = node_configured(
        "a",
        da.path(),
        &store,
        IDLE,
        &["t"],
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        |c: &mut ActorConfig| {
            no_placement(c);
            c.cluster.as_mut().unwrap().idle_compaction = false;
        },
    )
    .await;
    settle().await;
    writes(&a, "o", 0, 3).await;
    hibernated(&a).await;
    assert!(a.service.idle_passes_queued().is_empty());
}

/// A bucket with two hooks for the idle pass's races: history listings
/// held back `delay_list_ms` (counted in `lists` once they start), and an
/// app marker written as an ownership record naming a holder is written,
/// once armed (the pass's acquisition: between its two marker reads).
#[derive(Debug)]
struct Hooked {
    inner: Arc<dyn ObjectStore>,
    delay_list_ms: std::sync::atomic::AtomicU64,
    lists: Arc<std::sync::atomic::AtomicU32>,
    marker_at_acquisition: std::sync::Mutex<Option<&'static str>>,
    /// Reads of an ownership record's metadata (a HEAD) held back this
    /// long, counted in `heads` once they start.
    delay_owner_head_ms: std::sync::atomic::AtomicU64,
    heads: std::sync::atomic::AtomicU32,
}

impl Hooked {
    fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            delay_list_ms: Default::default(),
            lists: Default::default(),
            marker_at_acquisition: Default::default(),
            delay_owner_head_ms: Default::default(),
            heads: Default::default(),
        })
    }
}

impl std::fmt::Display for Hooked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hooked")
    }
}

#[async_trait::async_trait]
impl ObjectStore for Hooked {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        use object_store::ObjectStoreExt as _;
        if location.as_ref().contains("/owners/") {
            let body: Vec<u8> = (&payload).into_iter().flat_map(|b| b.to_vec()).collect();
            let held = wasmkeep_cluster::node_lease::OwnerRecord::decode(&body)
                .is_some_and(|r| r.holder.is_some());
            let marker = if held {
                self.marker_at_acquisition.lock().unwrap().take()
            } else {
                None
            };
            if let Some(marker) = marker {
                self.inner
                    .put(
                        &object_store::path::Path::from(format!("apps/default/default/{marker}")),
                        b"written during the pass".to_vec().into(),
                    )
                    .await?;
            }
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let delay = self.delay_owner_head_ms.load(Ordering::SeqCst);
        if options.head && delay > 0 && location.as_ref().contains("/owners/") {
            self.heads.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        use futures_util::StreamExt as _;
        let delay = self.delay_list_ms.load(Ordering::SeqCst);
        let history = prefix.is_some_and(|p| p.as_ref().contains("/actors/"));
        let listing = self.inner.list(prefix);
        if delay == 0 || !history {
            return listing;
        }
        let lists = Arc::clone(&self.lists);
        futures_util::stream::once(async move {
            lists.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(delay)).await;
            listing
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A node `a` on `hooked`, an actor written over two generations and
/// hibernated, its pass remembered, the window passed: what the M2 and
/// mid-pass tests start from. Returns the node and the keys before.
async fn rested(
    dir: &Path,
    hooked: &Arc<Hooked>,
    name: &str,
    retention: Duration,
) -> (Node, Vec<String>) {
    let store: Arc<dyn ObjectStore> = Arc::clone(hooked) as Arc<dyn ObjectStore>;
    let a = compacting("a", dir, &store, retention).await;
    settle().await;
    writes(&a, name, 0, 3).await;
    hibernated(&a).await;
    writes(&a, name, 3, 3).await;
    hibernated(&a).await;
    assert_eq!(a.service.idle_passes_queued().len(), 1);
    let before = actor_keys(&hooked.inner, name).await;
    assert!(before.len() > 2, "{before:?}");
    tokio::time::sleep(retention + Duration::from_millis(200)).await;
    (a, before)
}

/// M2: a marker written after the pass's first read of the markers and
/// before its second (here at its acquisition's write) makes the pass
/// compact nothing. The check made under the hold is what refuses.
async fn marker_during_the_pass(marker: &'static str) {
    let hooked = Hooked::new(Arc::new(InMemory::new()));
    let da = tempfile::tempdir().unwrap();
    let (a, before) = rested(da.path(), &hooked, "m", Duration::from_millis(500)).await;
    *hooked.marker_at_acquisition.lock().unwrap() = Some(marker);
    let outcome = run_passes(&a).await;
    assert!(
        hooked.marker_at_acquisition.lock().unwrap().is_none(),
        "the pass did not acquire"
    );
    assert_eq!(
        actor_keys(&hooked.inner, "m").await,
        before,
        "the pass deleted objects after the app's {marker} marker appeared"
    );
    assert_eq!(outcome, "refused");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_recreated_marker_written_during_the_pass_stops_it() {
    marker_during_the_pass("recreated").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restored_marker_written_during_the_pass_stops_it() {
    marker_during_the_pass("restored").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deleted_or_restoring_marker_written_during_the_pass_stops_it() {
    marker_during_the_pass("deleted").await;
    marker_during_the_pass("restoring").await;
}

/// M1: passes are taken as slots free up, not a few per tick. 50 actors
/// rest, their passes come due together; with a window long enough that
/// the loop's tick is its longest (5 s), all 50 run within a few seconds.
#[tokio::test(flavor = "multi_thread")]
async fn fifty_due_passes_all_run_promptly() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let da = tempfile::tempdir().unwrap();
    let a = compacting("a", da.path(), &store, Duration::from_secs(60)).await;
    settle().await;
    for i in 0..50 {
        writes(&a, &format!("f{i}"), 0, 1).await;
    }
    eventually("all hibernate", async || a.service.resident() == 0).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(a.service.idle_passes_queued().len(), 50);
    let before: u64 = all_compactions().await.iter().sum();
    a.service.idle_passes_due_now();
    a.service.pause_idle_passes(false);
    let started = std::time::Instant::now();
    loop {
        let ran = all_compactions().await.iter().sum::<u64>() - before;
        if ran == 50 && a.service.idle_passes_queued().is_empty() {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(8),
            "{ran} of 50 passes ran in {:?}",
            started.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// S9: a drain that begins after the pass's first checks and before its
/// job starts (here during its read of the record) is seen by the job,
/// which refuses before acquiring anything.
#[tokio::test(flavor = "multi_thread")]
async fn a_drain_begun_before_the_pass_job_starts_refuses_it() {
    let hooked = Hooked::new(Arc::new(InMemory::new()));
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let store: Arc<dyn ObjectStore> = Arc::clone(&hooked) as Arc<dyn ObjectStore>;
    let _b = compacting("b", db.path(), &store, Duration::from_millis(500)).await;
    let (a, before) = rested(da.path(), &hooked, "dj", Duration::from_millis(500)).await;
    let (_, generation) = owner_record(&hooked.inner, "dj").await.unwrap();
    hooked.delay_owner_head_ms.store(4_000, Ordering::SeqCst);
    let refused = compactions("refused").await;
    a.service.idle_passes_due_now();
    a.service.pause_idle_passes(false);
    eventually("the pass reads the record", async || {
        hooked.heads.load(Ordering::SeqCst) > 0
    })
    .await;
    drain(&hooked.inner, "a").await;
    eventually("the pass ended", async || {
        compactions("refused").await > refused
    })
    .await;
    assert_eq!(actor_keys(&hooked.inner, "dj").await, before);
    let (_, now) = owner_record(&hooked.inner, "dj").await.unwrap();
    assert_eq!(now, generation, "the record was acquired");
}

/// S9: a drain that begins while the pass lists stops it before any
/// delete (the pass's stop, checked before each batch, covers a drain).
#[tokio::test(flavor = "multi_thread")]
async fn a_drain_begun_during_a_pass_stops_its_deletes() {
    let hooked = Hooked::new(Arc::new(InMemory::new()));
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let store: Arc<dyn ObjectStore> = Arc::clone(&hooked) as Arc<dyn ObjectStore>;
    let _b = compacting("b", db.path(), &store, Duration::from_millis(500)).await;
    let (a, before) = rested(da.path(), &hooked, "dm", Duration::from_millis(500)).await;
    hooked.delay_list_ms.store(4_000, Ordering::SeqCst);
    let lists = hooked.lists.load(Ordering::SeqCst);
    a.service.idle_passes_due_now();
    a.service.pause_idle_passes(false);
    eventually("the pass lists", async || {
        hooked.lists.load(Ordering::SeqCst) > lists
    })
    .await;
    drain(&hooked.inner, "a").await;
    eventually("the pass ended", async || {
        a.service.idle_passes_queued().is_empty() && a.service.resident() == 0
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(actor_keys(&hooked.inner, "dm").await, before);
}

/// S9: an event that arrives while the pass lists waits behind it in the
/// mailbox; the pass stops before deleting, releases, and the event is
/// served after it, from the same history, under a hold of its own.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_during_a_slow_pass_is_served_after_it() {
    let hooked = Hooked::new(Arc::new(InMemory::new()));
    let da = tempfile::tempdir().unwrap();
    let (a, before) = rested(da.path(), &hooked, "w2", Duration::from_millis(500)).await;
    let (_, generation) = owner_record(&hooked.inner, "w2").await.unwrap();
    hooked.delay_list_ms.store(2_000, Ordering::SeqCst);
    let lists = hooked.lists.load(Ordering::SeqCst);
    let (stopped, continued) = (compactions("stopped").await, compactions("continued").await);
    a.service.idle_passes_due_now();
    a.service.pause_idle_passes(false);
    eventually("the pass lists", async || {
        hooked.lists.load(Ordering::SeqCst) > lists
    })
    .await;
    let started = std::time::Instant::now();
    let r = send(a.addr, "GET", "/actor/t/w2/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "6"), "{}", r.body);
    assert!(
        started.elapsed() >= Duration::from_millis(1_000),
        "served before the pass ended ({:?})",
        started.elapsed()
    );
    hooked.delay_list_ms.store(0, Ordering::SeqCst);
    assert_eq!(actor_keys(&hooked.inner, "w2").await, before, "deleted");
    // Stopped by the waiting event: counted, and not continued.
    eventually("the pass is counted", async || {
        compactions("stopped").await > stopped
    })
    .await;
    assert_eq!(compactions("continued").await, continued);
    // The pass's hold at generation + 1, released; the event's at + 2.
    let (holder, now) = owner_record(&hooked.inner, "w2").await.unwrap();
    assert_eq!((holder.as_deref(), now), (Some("a"), generation + 2));
}

/// S7, and a pass on a version 2 app: the pass releases with the head it
/// inherited, so the record still names the head (one generation past
/// it), and `--require-owner-match` accepts the compacted actor.
#[tokio::test(flavor = "multi_thread")]
async fn on_a_version_2_app_a_pass_keeps_the_recorded_head_and_the_owner_match() {
    use object_store::ObjectStoreExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(
            &object_store::path::Path::from(wasmkeep_cluster::app_format::key(
                "", "default", "default",
            )),
            wasmkeep_cluster::app_format::ENABLED_BODY.to_vec().into(),
        )
        .await
        .unwrap();
    let da = tempfile::tempdir().unwrap();
    let a = compacting("a", da.path(), &store, Duration::from_millis(500)).await;
    settle().await;
    writes(&a, "v2", 0, 3).await;
    hibernated(&a).await;
    writes(&a, "v2", 3, 3).await;
    hibernated(&a).await;
    let (head, _) = export(&store, "v2", Point::Head).await;
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(run_passes(&a).await, "done");
    assert_eq!(actor_keys(&store, "v2").await.len(), 2);
    let record = store
        .get(&object_store::path::Path::from(test_key(
            Family::Owners,
            "t/v2",
        )))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let record = wasmkeep_cluster::node_lease::OwnerRecord::decode(&record).unwrap();
    assert_eq!(record.generation, head.position.generation + 1);
    let recorded = record.recorded.expect("a version 2 record");
    assert!(
        matches!(
            recorded.head,
            wasmkeep_cluster::recorded_head::Position::Known { generation, txid, .. }
                if generation == head.position.generation && txid == head.position.txid
        ),
        "{recorded:?}"
    );
    let dir = tempfile::tempdir().unwrap();
    let strict = history::export_with_options(
        &store,
        &StateRoot::new("", "default", "default"),
        &ActorAddress::new("default", "t", "v2").unwrap(),
        Point::Head,
        &dir.path().join("strict.db"),
        history::Options {
            require_owner_match: true,
            ..history::Options::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(strict.raw_checksum, head.raw_checksum);
}
