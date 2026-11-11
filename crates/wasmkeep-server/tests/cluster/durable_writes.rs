//! Durable writes are acknowledged only once uploaded and survive their node.

use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn a_durable_write_survives_its_node_losing_the_bucket() {
    durable_write_survives_failover("x").await;
}

/// A durable write whose segment upload the bucket leaves hanging is
/// acknowledged within seconds: the upload is given up and sent again, as
/// a restore's reads are. It used to wait for the client's 30 s timeout,
/// past the event's own limit (seen in a soak against a real bucket).
#[tokio::test(flavor = "multi_thread")]
async fn a_hung_segment_upload_does_not_stall_a_durable_write() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let seen_by_a: Arc<dyn ObjectStore> = flaky.clone();
    let da = tempfile::tempdir().unwrap();
    let a = node_with(
        "a",
        da.path(),
        &seen_by_a,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;
    let path = "/actor/t/upload/durable";
    let r = send(a.addr, "POST", path, "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"), "{}", r.body);
    flaky.hang_wal_puts.store(1, Ordering::SeqCst);
    let started = std::time::Instant::now();
    let r = send(a.addr, "POST", path, "").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "2", Some("a")),
        "after {:?}: {}",
        started.elapsed(),
        r.body
    );
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "took {:?}",
        started.elapsed()
    );
    assert_eq!(
        flaky.hang_wal_puts.load(Ordering::SeqCst),
        0,
        "the hang was met"
    );
    drop(a);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_durable_write_stops_replication_until_restored() {
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    let r = send(p.a.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!((r.body.trim(), r.node.as_deref()), ("1", Some("a")));

    // The first upload of the event fails, every one of its three attempts
    // (one failure is now sent again). The second must not be acknowledged
    // on top of a history that lacks the first.
    p.flaky.fail_wal.store(3, Ordering::SeqCst);
    let r = send(p.a.addr, "POST", "/actor/t/x/durable-twice", "").await;
    assert_eq!(r.body.trim(), "first=err second=err");
    assert_eq!(
        p.flaky.fail_wal.load(Ordering::SeqCst),
        0,
        "every attempt used"
    );

    let r = b_restores(&p).await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "restorable"
    );
    assert_eq!(r.body.trim(), "1", "exactly the acknowledged writes");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_durable_write_cut_mid_upload_is_not_built_on() {
    // The actor stays resident throughout (no hibernation to hide it).
    let p = pair(Duration::from_millis(700), Duration::from_secs(30)).await;
    let r = send(p.a.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!(r.body.trim(), "1");

    // The upload outlasts the event's wall-clock limit: the event is cut
    // while the upload is in flight.
    p.flaky.delay_wal_ms.store(3_000, Ordering::SeqCst);
    let r = send(p.a.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!(r.status, 500);
    p.flaky.delay_wal_ms.store(0, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(3_000)).await; // let it land

    let r = send(p.a.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!(r.status, 200);
    let acked: u64 = r.body.trim().parse().unwrap();

    let r = b_restores(&p).await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "restorable"
    );
    assert_eq!(
        r.body.trim(),
        acked.to_string(),
        "the last acknowledged value"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_durable_write_inside_a_transaction_is_refused_in_a_cluster() {
    // Nothing inside a transaction is durable before COMMIT; claiming so
    // would let the guest act on a write a rollback or failover then loses.
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    let r = send(p.a.addr, "POST", "/actor/t/x/durable-in-tx", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "err"));
    let r = send(p.a.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!(r.body.trim(), "1", "the actor carries on");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_committed_with_exec_durable_survives_failover() {
    // BEGIN ... exec-durable("COMMIT"): the commit is acknowledged only once
    // the whole transaction is in the bucket.
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    let r = send(p.a.addr, "POST", "/actor/t/x/durable-commit", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.body
    );
    let r = send(p.a.addr, "POST", "/actor/t/x/durable-commit", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "2"));

    let r = b_restores(&p).await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "restorable"
    );
    assert_eq!(r.body.trim(), "2", "both acknowledged commits");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_durable_commit_is_acknowledged_only_once_uploaded() {
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    let r = send(p.a.addr, "POST", "/actor/t/x/durable-commit", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"));

    // The commit's upload fails, every one of its three attempts: not
    // acknowledged, and not built on.
    p.flaky.fail_wal.store(3, Ordering::SeqCst);
    let r = send(p.a.addr, "POST", "/actor/t/x/durable-commit", "").await;
    assert_eq!(r.status, 503, "{}", r.body);
    assert_eq!(
        p.flaky.fail_wal.load(Ordering::SeqCst),
        0,
        "every attempt used"
    );
    assert!(r.body.contains("not durable"), "{}", r.body);

    let r = b_restores(&p).await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "restorable"
    );
    assert_eq!(r.body.trim(), "1", "exactly the acknowledged commit");
}

/// What a power loss may leave of an actor's local files: only what was
/// synced. A cluster node's durable write is acknowledged once the bucket
/// holds it, so whatever the disk kept, a restarted node must answer with
/// the bucket's state, never with a local file that is behind it.
#[derive(Clone, Copy, Debug)]
enum Lost {
    /// The WAL's frames are gone (an empty file) while the sidecar names
    /// the bucket's head: the main file is older than the sidecar claims.
    Wal,
    /// The WAL kept only its first half. (The restarted node's scan for
    /// pending alarms may open the database first and fold that half into
    /// the main file; either way the file is not the bucket's head.)
    HalfTheWal,
    /// The sidecar is gone; the database and its WAL are whole.
    Sidecar,
    /// The sidecar is an empty file (written, its rename kept, its content
    /// not), beside an emptied WAL.
    SidecarContent,
    /// Everything is as it was after the first of three writes.
    TwoWrites,
}

async fn acknowledged_durable_writes_survive_losing(lost: Lost) {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let down = Arc::new(AtomicBool::new(false));
    let seen_by_old: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down)));
    let da = tempfile::tempdir().unwrap();
    let start = |store: &Arc<dyn ObjectStore>| {
        let (store, data) = (Arc::clone(store), da.path().to_owned());
        async move {
            let n = node_with(
                "a",
                &data,
                &store,
                Duration::from_secs(30),
                "t",
                test_actor_wasm(),
            )
            .await;
            n.service.node_id_checked().await.unwrap();
            n
        }
    };
    let a = start(&seen_by_old).await;
    settle().await;

    let db = da.path().join("actors/default/t/x.db");
    let beside = |suffix: &str| {
        let mut p = db.clone().into_os_string();
        p.push(suffix);
        std::path::PathBuf::from(p)
    };
    let files = || {
        (
            std::fs::read(&db).unwrap(),
            std::fs::read(beside("-wal")).unwrap(),
            std::fs::read(beside(".replica")).unwrap(),
        )
    };
    let path = "/actor/t/x/durable";
    let r = send(a.addr, "POST", path, "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"), "{}", r.body);
    let after_one = files();
    for expected in ["2", "3"] {
        let r = send(a.addr, "POST", path, "").await;
        assert_eq!((r.status, r.body.trim()), (200, expected), "{}", r.body);
    }
    let (main, wal, sidecar) = files();
    // Without these the cases below would test nothing: the writes are in
    // the WAL, not yet in the main file, and the sidecar moved with them.
    assert!(wal.len() > 4096, "the writes are in the WAL: {}", wal.len());
    assert!(
        !sidecar.is_empty() && sidecar != after_one.2,
        "the sidecar follows the head"
    );

    // Killed: nothing more reaches the bucket, then it serves nothing.
    down.store(true, Ordering::SeqCst);
    stop_listening(a).await;
    // In this process the stopped node closes its database a moment later,
    // and SQLite then checkpoints and removes the WAL by name: wait for
    // that, or it would remove the WAL this test is about to put there.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while beside("-wal").exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "the stopped node still has its database open"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // What the disk kept, in place of what the clean close left.
    let put = |to: std::path::PathBuf, bytes: &[u8]| {
        let mut tmp = to.clone().into_os_string();
        tmp.push(".power-loss");
        std::fs::write(&tmp, bytes).unwrap();
        std::fs::rename(&tmp, &to).unwrap();
    };
    let _ = std::fs::remove_file(beside("-shm"));
    match lost {
        Lost::Wal => {
            put(db.clone(), &main);
            put(beside("-wal"), &[]);
            put(beside(".replica"), &sidecar);
        }
        Lost::HalfTheWal => {
            put(db.clone(), &main);
            put(beside("-wal"), &wal[..wal.len() / 2]);
            put(beside(".replica"), &sidecar);
        }
        Lost::Sidecar => {
            put(db.clone(), &main);
            put(beside("-wal"), &wal);
            std::fs::remove_file(beside(".replica")).unwrap();
        }
        Lost::SidecarContent => {
            put(db.clone(), &main);
            put(beside("-wal"), &[]);
            put(beside(".replica"), &[]);
        }
        Lost::TwoWrites => {
            put(db.clone(), &after_one.0);
            put(beside("-wal"), &after_one.1);
            put(beside(".replica"), &after_one.2);
        }
    }

    let again = start(&shared).await;
    let r = send(again.addr, "GET", path, "").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "3", Some("a")),
        "{lost:?}: every acknowledged write, from the bucket: {}",
        r.body
    );
    // And replication goes on from there.
    let r = send(again.addr, "POST", path, "").await;
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "4"),
        "{lost:?}: {}",
        r.body
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_durable_write_survives_a_power_loss_that_empties_the_wal() {
    acknowledged_durable_writes_survive_losing(Lost::Wal).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_durable_write_survives_a_power_loss_that_cuts_the_wal() {
    acknowledged_durable_writes_survive_losing(Lost::HalfTheWal).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_durable_write_survives_a_power_loss_that_takes_the_sidecar() {
    acknowledged_durable_writes_survive_losing(Lost::Sidecar).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_durable_write_survives_a_power_loss_that_empties_the_sidecar() {
    acknowledged_durable_writes_survive_losing(Lost::SidecarContent).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_durable_write_survives_a_disk_rolled_back_two_writes() {
    acknowledged_durable_writes_survive_losing(Lost::TwoWrites).await;
}

/// On a cluster node a durable write is acknowledged once the bucket holds
/// it, and a local file is trusted again only after it is checked against
/// the bucket's head (the tests above): the commit is not fsynced locally
/// as well. It used to be, which cost a disk sync on every durable write.
#[tokio::test(flavor = "multi_thread")]
async fn a_durable_write_on_a_cluster_node_is_not_synced_locally() {
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    let r = send(p.a.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"), "{}", r.body);
    let before = wasmkeep_storage::faults::local_syncs();
    let r = send(p.a.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "2"), "{}", r.body);
    // A transaction ended by exec-durable("COMMIT") is the other path.
    let r = send(p.a.addr, "POST", "/actor/t/x/durable-commit", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(
        wasmkeep_storage::faults::local_syncs(),
        before,
        "no local sync for a write the bucket makes durable"
    );
    // Still what it promises: in the bucket when acknowledged.
    let r = b_restores(&p).await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "3", Some("b")),
        "{}",
        r.body
    );
}
