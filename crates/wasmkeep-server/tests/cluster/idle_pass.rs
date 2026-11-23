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

/// Lets `node`'s remembered passes run now, and waits until its queue is
/// empty and `outcome` was counted once more.
async fn run_passes(node: &Node, outcome: &str) {
    let before = compactions(outcome).await;
    node.service.idle_passes_due_now();
    node.service.pause_idle_passes(false);
    eventually("the idle pass ran", async || {
        node.service.idle_passes_queued().is_empty() && compactions(outcome).await > before
    })
    .await;
    // Its release and the hibernation of its job.
    tokio::time::sleep(Duration::from_millis(300)).await;
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
    run_passes(&a, "done").await;
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
    // A restore on the other node.
    let r = send(b.addr, "GET", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "10", Some("b")),
        "{}",
        r.body
    );
}

/// A wake before the pass, even one that only reads, rewrites the record:
/// the pass finds it changed and leaves every object.
#[tokio::test(flavor = "multi_thread")]
async fn a_wake_before_the_pass_leaves_every_object() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let da = tempfile::tempdir().unwrap();
    let a = compacting("a", da.path(), &store, Duration::from_millis(500)).await;
    settle().await;
    writes(&a, "w", 0, 4).await;
    hibernated(&a).await;
    assert_eq!(a.service.idle_passes_queued().len(), 1);
    let r = send(a.addr, "GET", "/actor/t/w/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "4"), "{}", r.body);
    hibernated(&a).await;
    let before = actor_keys(&store, "w").await;
    tokio::time::sleep(Duration::from_millis(700)).await;
    run_passes(&a, "changed").await;
    assert_eq!(actor_keys(&store, "w").await, before);
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
    run_passes(&a, "refused").await;
    assert_eq!(actor_keys(&store, "d").await, before);
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
    run_passes(&a, "refused").await;
    assert_eq!(
        actor_keys(&store, "r").await,
        before,
        "a's pass deleted objects of the recreated app"
    );
    let r = send(c.addr, "GET", "/actor/t/r/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "8"), "{}", r.body);
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
