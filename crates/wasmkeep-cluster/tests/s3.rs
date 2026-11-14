//! Node leases and ownership records (ADR 0020) against a real S3 API
//! (moto's S3 server; R2, OVHcloud and any S3-compatible store work): the
//! conditional writes they rest on, one winner among concurrent acquires,
//! a renewal whose reply was lost adopted from a read, and a fence that a
//! renewal landing between its read and its write defeats.
//!
//! Skipped unless `WASMKEEP_TEST_S3_BUCKET` names a bucket; the endpoint and
//! credentials come from the AWS_* environment variables, e.g.
//! `AWS_ENDPOINT=http://127.0.0.1:9000 AWS_ALLOW_HTTP=true`.

#![cfg(feature = "object-store")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use wasmkeep_cluster::lease::{Bucket, BucketError, Clock, LeaseConfig, Version};
use wasmkeep_cluster::node_lease::{
    AcquireError, Fence, Hold, LostCause, NodeLease, NodeLeaseError, NodeLeaseObject, NodeState,
    OwnerRecord, Owners, Released, fence,
};
use wasmkeep_cluster::object_store::ObjectStoreBucket;

#[derive(Debug, Clone, Copy)]
struct Wall;

impl Clock for Wall {
    fn now_ms(&self) -> u64 {
        let since = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        u64::try_from(since.as_millis()).unwrap()
    }
}

/// The store and a key prefix unique to this run and test, or `None` to
/// skip.
fn s3(test: &str) -> Option<(Arc<dyn ObjectStore>, String)> {
    let Ok(bucket) = std::env::var("WASMKEEP_TEST_S3_BUCKET") else {
        eprintln!("WASMKEEP_TEST_S3_BUCKET not set; skipping");
        return None;
    };
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .build()
        .unwrap();
    let prefix = format!("test-{}-{}-{test}", Wall.now_ms(), std::process::id());
    Some((Arc::new(store), prefix))
}

#[tokio::test(flavor = "multi_thread")]
async fn conditional_writes_are_enforced() {
    let Some((store, prefix)) = s3("conditional_writes_are_enforced") else {
        return;
    };
    let bucket = ObjectStoreBucket::new(store, format!("{prefix}/cw"));
    let v1 = bucket.create("k", b"one".to_vec()).await.unwrap();
    assert_eq!(
        bucket.create("k", b"two".to_vec()).await,
        Err(BucketError::Conflict),
        "create over an existing object must fail"
    );
    let v2 = bucket.replace("k", b"two".to_vec(), &v1).await.unwrap();
    assert_eq!(
        bucket.replace("k", b"three".to_vec(), &v1).await,
        Err(BucketError::Conflict),
        "replace with a stale version must fail"
    );
    // The version a read returns is accepted like the one the write
    // returned, though the two tokens may differ (R2 adds a version id to
    // a write's): only the bucket compares them.
    let (body, read) = bucket.get("k").await.unwrap().unwrap();
    assert_eq!(body, b"two");
    let v3 = bucket.replace("k", b"three".to_vec(), &read).await.unwrap();
    assert_eq!(
        bucket.replace("k", b"four".to_vec(), &v2).await,
        Err(BucketError::Conflict),
        "the write's own version is stale after another write"
    );
    bucket.replace("k", b"four".to_vec(), &v3).await.unwrap();
}

/// The check every node runs at start passes against this store.
#[tokio::test(flavor = "multi_thread")]
async fn the_start_check_passes() {
    let Some((store, prefix)) = s3("the_start_check_passes") else {
        return;
    };
    let bucket = ObjectStoreBucket::new(store, format!("{prefix}/"));
    assert_eq!(
        wasmkeep_cluster::bucket_check::check_conditional_writes(&bucket, "probe/x").await,
        Ok(())
    );
}

/// Node leases live here (`nodes/<id>`), under the run's prefix.
const NODES: &str = "nodes/";

/// A started node lease for node `id`, process `instance`.
async fn started(
    bucket: ObjectStoreBucket,
    id: &str,
    instance: &str,
    config: LeaseConfig,
) -> NodeLease<ObjectStoreBucket, Wall> {
    let mut lease = NodeLease::new(bucket, Wall, format!("{NODES}{id}"), instance, config);
    lease.start(None).await.unwrap();
    lease
}

/// The ownership record at `key`.
async fn record(bucket: &ObjectStoreBucket, key: &str) -> OwnerRecord {
    let (body, _) = bucket.get(key).await.unwrap().unwrap();
    OwnerRecord::decode(&body).unwrap()
}

/// The node lease object at `key`.
async fn lease_object(bucket: &ObjectStoreBucket, key: &str) -> NodeLeaseObject {
    let (body, _) = bucket.get(key).await.unwrap().unwrap();
    NodeLeaseObject::decode(&body).unwrap()
}

/// What one race of eight acquires left: the winner (node, its records
/// client, its hold) and the unresolved acquires (client, generation, the
/// record its write was conditioned on).
type Race = (
    (usize, Owners<ObjectStoreBucket, Wall>, Hold),
    Vec<(Owners<ObjectStoreBucket, Wall>, u64, Option<OwnerRecord>)>,
);

/// Eight nodes acquire `key` at once; exactly one must win.
async fn race(bucket: &ObjectStoreBucket, key: &str, config: LeaseConfig, round: &str) -> Race {
    let mut tasks = Vec::new();
    for node in 0..8 {
        let owners = Owners::new(
            bucket.clone(),
            Wall,
            format!("n{node}"),
            format!("i{node}"),
            NODES,
            config,
        );
        let key = key.to_owned();
        // Every lease was just written: valid for seconds.
        tasks.push(tokio::spawn(async move {
            let got = owners.acquire(&key, || true, async || Ok(1)).await;
            (node, owners, got)
        }));
    }
    let (mut winners, mut unresolved, mut named) = (Vec::new(), Vec::new(), Vec::new());
    for task in tasks {
        match task.await.unwrap() {
            (node, owners, Ok(hold)) => winners.push((node, owners, hold)),
            // Read before writing, or read back after a refused write.
            (_, _, Err(AcquireError::Held { node, instance })) => named.push((node, instance)),
            // A conflict on the write: it may have landed, as far as that
            // node knows.
            (_, owners, Err(AcquireError::Unresolved { generation, before })) => {
                unresolved.push((owners, generation, before));
            }
            (node, _, Err(e)) => panic!("{round}: n{node}: unexpected error: {e:?}"),
        }
    }
    let won: Vec<usize> = winners.iter().map(|(n, _, _)| *n).collect();
    assert_eq!(
        won.len(),
        1,
        "{round}: exactly one node holds the record: {won:?}"
    );
    let winner = (format!("n{}", won[0]), format!("i{}", won[0]));
    assert!(
        named.iter().all(|n| *n == winner),
        "{round}: every loser told it is held names the winner {winner:?}: {named:?}"
    );
    (winners.pop().unwrap(), unresolved)
}

/// One winner among eight concurrent acquires, both for a new record (the
/// create race) and for a released one (the conditional replace a
/// takeover uses); resolving the losers' unresolved acquires never frees
/// the winner's record.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_acquires_have_one_winner() {
    let Some((store, prefix)) = s3("concurrent_acquires_have_one_winner") else {
        return;
    };
    let config = LeaseConfig::new(10_000, 5_000, 2_000, 1_000).unwrap();
    let bucket = ObjectStoreBucket::new(Arc::clone(&store), prefix.clone());
    // Eight live nodes, each with its node lease written.
    for node in 0..8 {
        started(
            bucket.clone(),
            &format!("n{node}"),
            &format!("i{node}"),
            config,
        )
        .await;
    }
    let mut unresolved_seen = 0;
    for round in 0..5 {
        let key = format!("owners/t/c/race-{round}");
        let mut previous = None;
        for (pass, what) in ["create", "replace"].iter().enumerate() {
            let label = format!("round {round}, {what}");
            let ((winner, owners, hold), unresolved) = race(&bucket, &key, config, &label).await;
            let held = record(&bucket, &key).await;
            assert_eq!(
                held.holder,
                Some((format!("n{winner}"), format!("i{winner}"))),
                "{label}: the record names the winner"
            );
            assert_eq!(held.generation, hold.generation, "{label}");
            if let Some(previous) = previous {
                assert_eq!(hold.generation, previous + 1, "{label}: generation + 1");
            }
            unresolved_seen += unresolved.len();
            // Even while still waiting for an unchanged record: the
            // record changed, to the winner's.
            for (loser, generation, before) in unresolved {
                assert_eq!(
                    loser.resolve(&key, generation, before.as_ref(), true).await,
                    Released::Superseded,
                    "{label}: a loser's resolution"
                );
            }
            assert_eq!(record(&bucket, &key).await, held, "{label}");
            // Released for the next pass: the replace race.
            if pass == 0 {
                assert_eq!(owners.release(&hold).await, Released::Released, "{label}");
                assert_eq!(record(&bucket, &key).await.holder, None, "{label}");
            }
            previous = Some(hold.generation);
        }
    }
    // Losers read the winner's record back (Held, naming it): a loser left
    // unresolved is rare now; any was resolved above without freeing it.
    eprintln!("{unresolved_seen} acquires left unresolved");
}

/// A bucket whose next successful replace answers as if its reply was
/// lost: the write landed, the writer does not know.
struct LoseReply {
    inner: ObjectStoreBucket,
    lose: Arc<AtomicBool>,
    /// Replaces refused on their condition.
    conflicts: Arc<AtomicU32>,
}

impl Bucket for LoseReply {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
        self.inner.get(key).await
    }

    async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
        self.inner.create(key, body).await
    }

    async fn replace(
        &self,
        key: &str,
        body: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BucketError> {
        let written = self.inner.replace(key, body, expected).await;
        if written == Err(BucketError::Conflict) {
            self.conflicts.fetch_add(1, Ordering::SeqCst);
        }
        if written.is_ok() && self.lose.swap(false, Ordering::SeqCst) {
            return Err(BucketError::Unavailable("reply lost".into()));
        }
        written
    }
}

/// A renewal that landed with its reply lost: the next renewal's condition
/// fails, the node reads its own write back, adopts the read's token (which
/// may differ from the write's: R2 adds a version id), and renews again.
#[tokio::test(flavor = "multi_thread")]
async fn a_renewal_whose_reply_was_lost_is_adopted() {
    let Some((store, prefix)) = s3("a_renewal_whose_reply_was_lost_is_adopted") else {
        return;
    };
    let config = LeaseConfig::new(10_000, 5_000, 2_000, 1_000).unwrap();
    let plain = ObjectStoreBucket::new(Arc::clone(&store), prefix.clone());
    let lose = Arc::new(AtomicBool::new(false));
    let conflicts = Arc::new(AtomicU32::new(0));
    let bucket = LoseReply {
        inner: plain.clone(),
        lose: Arc::clone(&lose),
        conflicts: Arc::clone(&conflicts),
    };
    let mut lease = NodeLease::new(bucket, Wall, format!("{NODES}a"), "i1", config);
    lease.start(None).await.unwrap();
    let key = format!("{NODES}a");
    lose.store(true, Ordering::SeqCst);
    assert!(
        matches!(lease.renew().await, Err(NodeLeaseError::Unavailable(_))),
        "the reply was lost"
    );
    // Its version is stale now: the conflict reads the object back, finds
    // it live under this instance, and adopts it with the written_at it
    // stores (the lost write's), not this attempt's send time.
    lease.renew().await.unwrap();
    assert_eq!(
        conflicts.load(Ordering::SeqCst),
        1,
        "the stale version was refused"
    );
    let landed = lease_object(&plain, &key).await;
    assert_eq!(
        lease.valid_until_ms(),
        Some(landed.written_at_ms + config.ttl_ms() - config.safety_ms()),
        "validity counts from the landed write"
    );
    // With the read's token, the next renewal writes, with no conflict.
    let before = lease.valid_until_ms().unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    lease.renew().await.unwrap();
    assert_eq!(
        conflicts.load(Ordering::SeqCst),
        1,
        "the adopted token was current"
    );
    assert!(lease.valid_until_ms().unwrap() > before, "renewed");
    let object = lease_object(&plain, &key).await;
    assert_eq!(object.state, NodeState::Live);
    assert_eq!(object.instance.as_deref(), Some("i1"));
    assert!(object.written_at_ms > landed.written_at_ms);
}

/// A peer's bucket that lets the watched node renew right before the
/// peer's first replace: the renewal lands between the fence's read and
/// its write.
struct RenewFirst {
    inner: ObjectStoreBucket,
    watched: tokio::sync::Mutex<NodeLease<ObjectStoreBucket, Wall>>,
    armed: AtomicBool,
}

impl Bucket for RenewFirst {
    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, BucketError> {
        self.inner.get(key).await
    }

    async fn create(&self, key: &str, body: Vec<u8>) -> Result<Version, BucketError> {
        self.inner.create(key, body).await
    }

    async fn replace(
        &self,
        key: &str,
        body: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BucketError> {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.watched.lock().await.renew().await.unwrap();
        }
        self.inner.replace(key, body, expected).await
    }
}

/// A fence is conditional on the version it read: a renewal landing
/// between its read and its write defeats it. Without one, the fence lands
/// and the fenced node's next renewal finds its lease lost.
#[tokio::test(flavor = "multi_thread")]
async fn a_fence_loses_to_a_renewal_that_landed_between() {
    let Some((store, prefix)) = s3("a_fence_loses_to_a_renewal_that_landed_between") else {
        return;
    };
    // A short ttl and no grace, so the watched node is stale quickly (one
    // clock, no skew), yet long enough that a remote bucket's round trips
    // (R2: a few hundred ms) leave a fresh node fresh.
    let config = LeaseConfig::new(2_000, 0, 100, 0).unwrap();
    let plain = ObjectStoreBucket::new(Arc::clone(&store), prefix.clone());
    let key = format!("{NODES}a");
    let a = started(plain.clone(), "a", "i1", config).await;
    let peer = RenewFirst {
        inner: plain.clone(),
        watched: tokio::sync::Mutex::new(a),
        armed: AtomicBool::new(true),
    };
    let watched = lease_object(&plain, &key).await;
    // Not stale yet: refused before any write.
    assert_eq!(
        fence(&peer, &Wall, &config, &key, "i1", watched.written_at_ms).await,
        Fence::Alive
    );
    assert!(
        peer.armed.load(Ordering::SeqCst),
        "no write tried on a fresh node"
    );
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    // Stale, but not the instance or write the peer watched: refused
    // before any write too.
    for (instance, written_at) in [
        ("i2", watched.written_at_ms),
        ("i1", watched.written_at_ms - 1),
    ] {
        assert_eq!(
            fence(&peer, &Wall, &config, &key, instance, written_at).await,
            Fence::Alive,
            "watched {instance} at {written_at}"
        );
    }
    assert!(
        peer.armed.load(Ordering::SeqCst),
        "no write tried on other values"
    );
    assert_eq!(
        fence(&peer, &Wall, &config, &key, "i1", watched.written_at_ms).await,
        Fence::Alive,
        "the renewal landed between the fence's read and its write"
    );
    assert!(
        !peer.armed.load(Ordering::SeqCst),
        "the fence got as far as its write (the node was stale)"
    );
    assert_eq!(lease_object(&plain, &key).await.state, NodeState::Live);

    // Stale again, and nothing in between: fenced.
    let watched = lease_object(&plain, &key).await;
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    assert_eq!(
        fence(&plain, &Wall, &config, &key, "i1", watched.written_at_ms).await,
        Fence::Fenced
    );
    assert_eq!(lease_object(&plain, &key).await.state, NodeState::Fenced);
    assert_eq!(
        peer.watched.lock().await.renew().await,
        Err(NodeLeaseError::Lost(LostCause::Fenced)),
        "a fenced node's renewal finds its lease lost"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn alarm_index_keys_never_nest_and_absent_deletes_succeed() {
    // What the alarm index (ADR 0009) needs from the store, which the
    // in-memory store used by the other tests cannot show: a legacy entry
    // and an actor's tagged entries exist side by side, with no key under
    // another (MinIO cannot hold an object and a folder of the same name);
    // a listing of the tagged folder returns exactly that actor's entries
    // (not one whose name merely starts with the same characters); a
    // listing of the time shows both kinds; deleting a key that is already
    // gone succeeds, and removes nothing else.
    use futures_util::TryStreamExt as _;
    use object_store::ObjectStoreExt as _;
    use object_store::path::Path;
    use wasmkeep_cluster::alarm_index::{self, Entry};
    let Some((store, prefix)) = s3("alarm_index_keys_never_nest_and_absent_deletes_succeed") else {
        return;
    };
    let tail = |name: &str, entry: &Entry| {
        format!(
            "{prefix}/alarms/t/{}",
            alarm_index::key_tail("c", name, entry)
        )
    };
    let tagged = Entry::owned(10, 2, "ab").unwrap();
    let legacy = tail("x", &Entry::legacy(10));
    let mine = tail("x", &tagged);
    let other = tail("xy", &Entry::owned(10, 2, "cd").unwrap());
    let keys = [legacy.as_str(), mine.as_str(), other.as_str()];
    for key in keys {
        assert!(
            keys.iter().all(|k| !k.starts_with(&format!("{key}/"))),
            "{key} would be a folder"
        );
        store
            .put(&Path::from(key), object_store::PutPayload::new())
            .await
            .unwrap();
    }
    let list = |base: String| {
        let store = Arc::clone(&store);
        async move {
            let mut listed: Vec<String> = store
                .list(Some(&Path::from(base)))
                .map_ok(|m| m.location.to_string())
                .try_collect()
                .await
                .unwrap();
            listed.sort();
            listed
        }
    };
    let folder = format!(
        "{prefix}/alarms/t/{}",
        alarm_index::tagged_prefix("c", "x", 10)
    );
    assert_eq!(list(folder).await, std::slice::from_ref(&mine));
    let mut all = keys.map(str::to_owned).to_vec();
    all.sort();
    assert_eq!(list(format!("{prefix}/alarms/t")).await, all);

    store.delete(&Path::from(mine.as_str())).await.unwrap();
    store.delete(&Path::from(mine.as_str())).await.unwrap();
    for key in [&legacy, &other] {
        assert!(
            store.head(&Path::from(key.as_str())).await.is_ok(),
            "exact key only"
        );
    }
}
