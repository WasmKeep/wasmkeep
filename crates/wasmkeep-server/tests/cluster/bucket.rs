//! Replication to the bucket: restores, deletes, bounds, outages, readiness and history collection.

use super::*;

/// The new owner of `path`'s actor (written `writes` times by a, then a cut
/// off) serves it on b, which reads the bucket through `flaky_b`: how long
/// from the cut.
async fn takeover_time(
    path: &str,
    writes: u32,
    fault: impl FnOnce(&Flaky),
) -> (Duration, Vec<u32>) {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let down = Arc::new(AtomicBool::new(false));
    let seen_by_a: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down)));
    let flaky_b = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let seen_by_b: Arc<dyn ObjectStore> = flaky_b.clone();
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let a = node_with("a", da.path(), &seen_by_a, idle, "t", test_actor_wasm()).await;
    let b = node_with("b", db.path(), &seen_by_b, idle, "t", test_actor_wasm()).await;
    settle().await;
    for expected in 1..=writes {
        let r = send(a.addr, "POST", path, "").await;
        assert_eq!(
            (r.status, r.node.as_deref()),
            (200, Some("a")),
            "{}",
            r.body
        );
        assert_eq!(r.body.trim(), expected.to_string());
    }
    fault(&flaky_b);
    down.store(true, Ordering::SeqCst);
    let started = std::time::Instant::now();
    loop {
        let r = tokio::time::timeout(Duration::from_secs(2), send(b.addr, "GET", path, "")).await;
        if let Ok(r) = r
            && r.node.as_deref() == Some("b")
        {
            assert_eq!(
                (r.status, r.body.trim()),
                (200, writes.to_string().as_str())
            );
            drop(a);
            let took = started.elapsed();
            let mut reads: Vec<u32> = flaky_b.wal_gets.lock().unwrap().values().copied().collect();
            reads.sort_unstable();
            return (took, reads);
        }
        assert!(
            started.elapsed() < Duration::from_secs(40),
            "b never served it"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A restore reads a few objects ahead (12-hour soak: one restore of 55
/// objects took 56 s, each read about a second, one after another). Here
/// every segment read takes 300 ms: 30 in turn would take 9 s on top of
/// the takeover; read ahead, about a second.
#[tokio::test(flavor = "multi_thread")]
async fn a_restore_reads_ahead_so_slow_reads_do_not_add_up() {
    let (took, reads) = takeover_time("/actor/t/ahead/durable", 30, |f| {
        f.delay_wal_get_ms.store(300, Ordering::SeqCst);
    })
    .await;
    // The takeover itself (a's lease) takes about 2 s with these timings;
    // 30 reads in turn would add 9 s, read ahead about 1.2 s.
    assert!(reads.len() >= 30, "{reads:?}");
    assert!(took < Duration::from_secs(7), "{took:?}");
}

/// One node's restores together read at most 32 objects at once, however
/// many start together (one kill starts a takeover of each actor its node
/// held, and each restore reads up to 8 objects ahead). Here six actors of
/// 30 segments each, every read 300 ms, are taken over at once: 48 reads
/// at once without the limit.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_s_restores_share_one_limit_on_reads() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let down = Arc::new(AtomicBool::new(false));
    let seen_by_a: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down)));
    let flaky_b = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let seen_by_b: Arc<dyn ObjectStore> = flaky_b.clone();
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let a = node_with("a", da.path(), &seen_by_a, idle, "t", test_actor_wasm()).await;
    let b = node_with("b", db.path(), &seen_by_b, idle, "t", test_actor_wasm()).await;
    settle().await;
    let names: Vec<String> = (0..6)
        .map(|i| format!("/actor/t/many{i}/durable"))
        .collect();
    for path in &names {
        for _ in 0..30 {
            let r = send(a.addr, "POST", path, "").await;
            assert_eq!(
                (r.status, r.node.as_deref()),
                (200, Some("a")),
                "{}",
                r.body
            );
        }
    }
    flaky_b.delay_wal_get_ms.store(300, Ordering::SeqCst);
    down.store(true, Ordering::SeqCst);
    let started = std::time::Instant::now();
    let tasks: Vec<_> = names
        .iter()
        .map(|path| {
            let (addr, path) = (b.addr, path.clone());
            tokio::spawn(async move {
                loop {
                    let r =
                        tokio::time::timeout(Duration::from_secs(5), send(addr, "GET", &path, ""))
                            .await;
                    if let Ok(r) = r
                        && r.node.as_deref() == Some("b")
                    {
                        assert_eq!((r.status, r.body.trim()), (200, "30"));
                        return;
                    }
                    assert!(
                        started.elapsed() < Duration::from_secs(60),
                        "{path} never served"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    let peak = flaky_b.wal_gets_peak.load(Ordering::SeqCst);
    assert!(peak <= 32, "{peak} segment reads at once");
    assert!(peak > 8, "{peak}: the restores did not read side by side");
    drop(a);
}

/// A read slow past its limit (5 s plus a second per megabyte, from its
/// request), though never silent for 5 s, is sent again: here one read
/// trickles for 20 s.
#[tokio::test(flavor = "multi_thread")]
async fn a_restore_read_slow_past_its_limit_is_sent_again() {
    let (took, reads) = takeover_time("/actor/t/trickle/durable", 5, |f| {
        f.trickle_wal_gets.store(1, Ordering::SeqCst);
    })
    .await;
    assert!(took < Duration::from_secs(14), "{took:?}");
    // Sent again within the one restore: the trickled segment read twice,
    // every other once (a failed restore redone would read them all again).
    let twice = reads.iter().filter(|&&n| n == 2).count();
    assert!(twice == 1 && reads.iter().all(|&n| n <= 2), "{reads:?}");
}

/// A restore whose GET hangs is not held by it: the read is given up and
/// sent again, and the new owner serves the actor within seconds (one
/// hung read held a restore for 19.8 s in the real-bucket soak).
#[tokio::test(flavor = "multi_thread")]
async fn a_hung_read_does_not_stall_a_restore() {
    let path = "/actor/t/stall/durable";
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let down = Arc::new(AtomicBool::new(false));
    let seen_by_a: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down)));
    let flaky_b = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let seen_by_b: Arc<dyn ObjectStore> = flaky_b.clone();
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let a = node_with("a", da.path(), &seen_by_a, idle, "t", test_actor_wasm()).await;
    let b = node_with("b", db.path(), &seen_by_b, idle, "t", test_actor_wasm()).await;
    settle().await;

    // A history of a snapshot and several segments.
    for expected in 1..=5 {
        let r = send(a.addr, "POST", path, "").await;
        assert_eq!(
            (r.status, r.body.trim(), r.node.as_deref()),
            (200, expected.to_string().as_str(), Some("a"))
        );
    }
    // b's first read of a segment will hang; a is cut off, so b takes over.
    flaky_b.hang_wal_gets.store(1, Ordering::SeqCst);
    down.store(true, Ordering::SeqCst);
    let started = std::time::Instant::now();
    let r = loop {
        // A request waiting on a stalled restore would wait for good.
        let r = tokio::time::timeout(Duration::from_secs(2), send(b.addr, "GET", path, "")).await;
        if let Ok(r) = r
            && r.node.as_deref() == Some("b")
        {
            break r;
        }
        // The hung read costs up to 5 s (`READ_IDLE`) on top of the
        // takeover; a stalled restore would never finish.
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "b did not serve the actor within 15 s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(
        flaky_b.hang_wal_gets.load(Ordering::SeqCst),
        0,
        "the hung read was made"
    );
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "5", Some("b")),
        "b serves the actor, every write restored, after {:?}",
        started.elapsed()
    );
}

/// Two nodes and an actor at `path` that a wrote `writes` times before it
/// was cut off from the bucket: b, which reads the bucket through the
/// `Flaky` returned and gives a restore `budget` before sizes are counted
/// (30 s on a real node), takes the actor at its next request.
async fn cut_off_owner(
    path: &str,
    writes: u32,
    budget: Duration,
) -> (
    Node,
    Node,
    Arc<Flaky>,
    (tempfile::TempDir, tempfile::TempDir),
) {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let down = Arc::new(AtomicBool::new(false));
    let seen_by_a: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down)));
    let flaky_b = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let seen_by_b: Arc<dyn ObjectStore> = flaky_b.clone();
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let a = node_with("a", dirs.0.path(), &seen_by_a, idle, "t", test_actor_wasm()).await;
    let b = node_configured(
        "b",
        dirs.1.path(),
        &seen_by_b,
        idle,
        &["t"],
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        move |config: &mut ActorConfig| {
            config.cluster.as_mut().unwrap().restore_budget = budget;
        },
    )
    .await;
    settle().await;
    for expected in 1..=writes {
        let r = send(a.addr, "POST", path, "").await;
        assert_eq!(
            (r.status, r.body.trim(), r.node.as_deref()),
            (200, expected.to_string().as_str(), Some("a"))
        );
    }
    down.store(true, Ordering::SeqCst);
    (a, b, flaky_b, dirs)
}

/// The answer to the request that waited for b's first restore of
/// `path`'s actor, and how long that request took. Requests are sent one
/// at a time, so the one during which b first read the actor's history is
/// the one that waited for the restore: its answer is the restore's, not
/// that of a later request that found the work redone.
async fn answer_behind_the_restore(b: &Node, path: &str, flaky_b: &Flaky) -> (Reply, Duration) {
    let started = std::time::Instant::now();
    loop {
        let sent = std::time::Instant::now();
        let r = tokio::time::timeout(Duration::from_secs(40), send(b.addr, "GET", path, ""))
            .await
            .expect("a request behind a restore is answered");
        if flaky_b.history_gets.load(Ordering::SeqCst) > 0 {
            return (r, sent.elapsed());
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "b never restored the actor"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Files a restore leaves while it builds a database (`<db>.restore`),
/// anywhere under `dir`.
fn restore_leftovers(dir: &Path) -> Vec<std::path::PathBuf> {
    walk(dir)
        .into_iter()
        .filter(|path| path.extension().is_some_and(|e| e == "restore"))
        .collect()
}

/// b serves `path`'s actor with `writes` writes, within 15 s.
async fn restores_cleanly(b: &Node, path: &str, writes: u32) {
    let started = std::time::Instant::now();
    loop {
        let r = send(b.addr, "GET", path, "").await;
        if r.status == 200 {
            assert_eq!(
                (r.body.trim(), r.node.as_deref()),
                (writes.to_string().as_str(), Some("b")),
                "every acknowledged write restored"
            );
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "b did not serve the actor: {} {}",
            r.status,
            r.body
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A read the bucket refuses once with an error a resend gets past (a
/// 5xx, a reset connection) is sent again within the restore: the request
/// that woke the actor is served, and nothing is read a second time but
/// the object refused. It used to fail the whole restore, and the request
/// with it.
#[tokio::test(flavor = "multi_thread")]
async fn a_restore_read_refused_once_is_sent_again() {
    let path = "/actor/t/refused/durable";
    let (_a, b, flaky_b, _dirs) = cut_off_owner(path, 5, Duration::from_secs(30)).await;
    flaky_b.fail_wal_gets.store(1, Ordering::SeqCst);
    let (r, _) = answer_behind_the_restore(&b, path, &flaky_b).await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "5", Some("b")),
        "the request that woke the actor is served, every write restored"
    );
    assert_eq!(
        flaky_b.fail_wal_gets.load(Ordering::SeqCst),
        0,
        "the refused read was made"
    );
    let mut reads: Vec<u32> = flaky_b.wal_gets.lock().unwrap().values().copied().collect();
    reads.sort_unstable();
    let twice = reads.iter().filter(|&&n| n == 2).count();
    assert!(twice == 1 && reads.iter().all(|&n| n <= 2), "{reads:?}");
    assert_eq!(
        super::snapshots::counter("wasmkeep_restore_failures_total").await,
        0
    );
}

/// A bucket that leaves every read of the history hanging fails the
/// restore within its reads' deadline (here 1 s, a second for the megabyte
/// started and a second for the objects: 3 s; each read alone would be
/// tried three times, 5 s each), with the answer of a bucket that does
/// not answer: 503 with `Retry-After`. Nothing half-built stays on disk,
/// and once the bucket answers the next request restores every write.
#[tokio::test(flavor = "multi_thread")]
async fn a_restore_the_bucket_never_answers_ends_within_its_deadline() {
    let path = "/actor/t/silent/durable";
    let (_a, b, flaky_b, dirs) = cut_off_owner(path, 5, Duration::from_secs(1)).await;
    flaky_b.hang_history_gets.store(true, Ordering::SeqCst);
    let (r, took) = answer_behind_the_restore(&b, path, &flaky_b).await;
    assert_eq!(
        (r.status, r.retry_after.as_deref()),
        (503, Some("1")),
        "{}",
        r.body
    );
    assert!(took < Duration::from_secs(8), "{took:?}");
    assert_eq!(
        restore_leftovers(dirs.1.path()),
        Vec::<std::path::PathBuf>::new()
    );
    assert!(super::snapshots::counter("wasmkeep_restore_failures_total").await >= 1);
    flaky_b.hang_history_gets.store(false, Ordering::SeqCst);
    restores_cleanly(&b, path, 5).await;
    assert_eq!(
        restore_leftovers(dirs.1.path()),
        Vec::<std::path::PathBuf>::new()
    );
}

/// Reads that are each answered in time still add up: here every segment
/// read takes 2 s, under any one read's limit, so the 30 segments, read
/// eight at a time, take 8 s. The restore has 6 s (1 s, a second for the
/// megabyte started, a second per eight objects) for its reads: it is given
/// up then,
/// answered as a bucket that does not answer is, and the next request
/// restores every write once the bucket is fast again. Without a deadline
/// for the reads together, the caller waited as long as the chain took.
#[tokio::test(flavor = "multi_thread")]
async fn slow_reads_that_add_up_past_the_deadline_end_a_restore() {
    let path = "/actor/t/slow/durable";
    let (_a, b, flaky_b, dirs) = cut_off_owner(path, 30, Duration::from_secs(1)).await;
    flaky_b.delay_wal_get_ms.store(2_000, Ordering::SeqCst);
    let (r, took) = answer_behind_the_restore(&b, path, &flaky_b).await;
    assert_eq!(
        (r.status, r.retry_after.as_deref()),
        (503, Some("1")),
        "after {took:?}: {}",
        r.body
    );
    assert_eq!(
        restore_leftovers(dirs.1.path()),
        Vec::<std::path::PathBuf>::new()
    );
    assert!(super::snapshots::counter("wasmkeep_restore_failures_total").await >= 1);
    flaky_b.delay_wal_get_ms.store(0, Ordering::SeqCst);
    restores_cleanly(&b, path, 30).await;
}

/// A restore that fails once its database is complete, while putting it
/// in place, is the one failure that changes the node's files: the file
/// that was there has been set aside (`<db>.replaced`) and its sidecar
/// removed. Here the sidecar of the new file cannot be written. Nothing
/// then says the file in place is the bucket's, so the next restore
/// trusts nothing local, reads the history again and serves every write;
/// the file set aside by the first is replaced by the copy the first
/// left in place (one generation is kept).
#[tokio::test(flavor = "multi_thread")]
async fn a_restore_that_fails_putting_its_file_in_place_is_redone_from_the_bucket() {
    let path = "/actor/t/aside/durable";
    let (_a, b, flaky_b, dirs) = cut_off_owner(path, 5, Duration::from_secs(30)).await;
    // b has an older file for the actor, and cannot write a sidecar.
    let dir = dirs.1.path().join("actors/default/t");
    let file = |suffix: &str| dir.join(format!("aside.db{suffix}"));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(file(""), b"what was there").unwrap();
    std::fs::create_dir(file(".replica.tmp")).unwrap();

    let (r, _) = answer_behind_the_restore(&b, path, &flaky_b).await;
    assert_ne!(r.status, 200, "{}", r.body);
    assert!(super::snapshots::counter("wasmkeep_restore_failures_total").await >= 1);
    assert_eq!(
        std::fs::read(file(".replaced")).unwrap(),
        b"what was there",
        "the file that was there is set aside"
    );
    assert!(
        !file(".replica").exists(),
        "nothing vouches for a local file"
    );
    assert_eq!(
        restore_leftovers(dirs.1.path()),
        Vec::<std::path::PathBuf>::new()
    );

    std::fs::remove_dir(file(".replica.tmp")).unwrap();
    restores_cleanly(&b, path, 5).await;
    assert!(file(".replica").exists());
    assert_ne!(std::fs::read(file(".replaced")).unwrap(), b"what was there");
}

/// `actors` actors that a wrote about 1.4 MiB of incompressible data to,
/// each in one segment after a small snapshot, before a was cut off from
/// the bucket. b reads the bucket through the `Flaky` returned, every
/// segment answered after 4 s (under the 5 s of silence that gives a read
/// up), and gives a restore half a second before sizes are counted: with
/// the two megabytes started and the second for its objects, 3.5 s for
/// its reads when nothing else is read on the node. The actors' paths
/// come with it.
async fn large_actors_cut_off(
    name: &str,
    actors: usize,
) -> (
    Node,
    Node,
    Arc<Flaky>,
    Vec<String>,
    (tempfile::TempDir, tempfile::TempDir),
) {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let down = Arc::new(AtomicBool::new(false));
    let seen_by_a: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down)));
    let flaky_b = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let seen_by_b: Arc<dyn ObjectStore> = flaky_b.clone();
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let a = node_with("a", dirs.0.path(), &seen_by_a, idle, "t", test_actor_wasm()).await;
    let b = node_configured(
        "b",
        dirs.1.path(),
        &seen_by_b,
        idle,
        &["t"],
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        |config: &mut ActorConfig| {
            config.cluster.as_mut().unwrap().restore_budget = Duration::from_millis(500);
        },
    )
    .await;
    settle().await;
    let paths: Vec<String> = (0..actors)
        .map(|i| format!("/actor/t/{name}{i}/durable"))
        .collect();
    for path in &paths {
        let r = send(a.addr, "POST", path, "").await;
        assert_eq!(
            (r.status, r.body.trim(), r.node.as_deref()),
            (200, "1", Some("a"))
        );
        let sql_path = path.replace("/durable", "/sql");
        for sql in [
            "CREATE TABLE big (x)",
            "INSERT INTO big VALUES (randomblob(1500000))",
        ] {
            let mut stream = TcpStream::connect(a.addr).await.unwrap();
            let req = format!(
                "POST {sql_path} HTTP/1.1\r\nHost: t\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{sql}",
                sql.len()
            );
            stream.write_all(req.as_bytes()).await.unwrap();
            let mut raw = String::new();
            stream.read_to_string(&mut raw).await.unwrap();
            assert!(raw.contains("ok:"), "{raw}");
        }
    }
    // A plain query answers before its segment is uploaded: wait until
    // every actor's large segment is in the bucket.
    let started = std::time::Instant::now();
    loop {
        use futures_util::TryStreamExt;
        let sizes: Vec<u64> = shared
            .list(None)
            .map_ok(|m| m.size)
            .try_collect()
            .await
            .unwrap();
        if sizes.iter().filter(|&&n| n > 1_000_000).count() == actors {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "the large segments never landed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    flaky_b.delay_wal_get_ms.store(4_000, Ordering::SeqCst);
    down.store(true, Ordering::SeqCst);
    (a, b, flaky_b, paths, dirs)
}

/// Restores that read side by side are not ended for each other's bytes.
/// A read's own limit counts every megabyte read alongside it on the
/// node, other restores' included, so the deadline of a restore's reads
/// together has to count them too: after a kill one node restores many
/// actors at once over one link. Here five actors of about 1.4 MiB each
/// are taken over together and every segment read takes 4 s, inside each
/// read's limit. A restore alone has 3.5 s (the test below: it is ended),
/// but with the others' megabytes in flight each has several seconds
/// more: every one completes at its first try, each segment read once.
/// Sized from its own bytes only, the deadline ended all five at 3.5 s,
/// every read inside its limit, and the callers that tried again met the
/// same end together.
#[tokio::test(flavor = "multi_thread")]
async fn restores_side_by_side_are_not_ended_for_each_others_bytes() {
    let (_a, b, flaky_b, paths, _dirs) = large_actors_cut_off("beside", 5).await;
    let started = std::time::Instant::now();
    let tasks: Vec<_> = paths
        .iter()
        .map(|path| {
            let (addr, path) = (b.addr, path.clone());
            tokio::spawn(async move {
                loop {
                    let r = send(addr, "GET", &path, "").await;
                    if r.status == 200 && r.node.as_deref() == Some("b") {
                        assert_eq!(r.body.trim(), "1");
                        return;
                    }
                    assert!(
                        started.elapsed() < Duration::from_secs(40),
                        "{path} never served: {} {}",
                        r.status,
                        r.body
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(
        super::snapshots::counter("wasmkeep_restore_failures_total").await,
        0,
        "no restore was ended"
    );
    let reads = flaky_b.wal_gets.lock().unwrap().clone();
    assert!(
        reads.len() >= paths.len() && reads.values().all(|&n| n == 1),
        "every segment read once: {reads:?}"
    );
}

/// The same actor restored alone is ended by its deadline: no megabyte
/// of another restore is in flight, so its reads have the 3.5 s its own
/// size gives and the 4 s read does not fit. What the test above shows is
/// the other restores' bytes, not a deadline that lets slow reads through.
#[tokio::test(flavor = "multi_thread")]
async fn a_restore_alone_has_no_more_than_its_own_deadline() {
    let (_a, b, flaky_b, paths, _dirs) = large_actors_cut_off("alone", 1).await;
    let (r, took) = answer_behind_the_restore(&b, &paths[0], &flaky_b).await;
    assert_eq!(
        (r.status, r.retry_after.as_deref()),
        (503, Some("1")),
        "after {took:?}: {}",
        r.body
    );
    assert!(super::snapshots::counter("wasmkeep_restore_failures_total").await >= 1);
    flaky_b.delay_wal_get_ms.store(0, Ordering::SeqCst);
    restores_cleanly(&b, &paths[0], 1).await;
}

/// A node cut off from the bucket cannot take an actor: it answers 503
/// with `Retry-After`, as clients and load balancers expect for a
/// temporary refusal, not a 500 counted as the guest's failure (review
/// finding D4).
#[tokio::test(flavor = "multi_thread")]
async fn a_bucket_outage_answers_503_with_retry_after() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let down = Arc::new(AtomicBool::new(false));
    let seen_by_a: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down)));
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
    down.store(true, Ordering::SeqCst);
    let r = send(a.addr, "POST", "/actor/t/new/durable", "").await;
    assert_eq!(
        (r.status, r.retry_after.as_deref()),
        (503, Some("1")),
        "{}",
        r.body
    );
    // Back: the same request goes through once the bucket answers (the
    // node lease may have to be written again first).
    down.store(false, Ordering::SeqCst);
    let started = std::time::Instant::now();
    let r = loop {
        let r = send(a.addr, "POST", "/actor/t/new/durable", "").await;
        if r.status == 200 || started.elapsed() > Duration::from_secs(10) {
            break r;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!((r.status, r.body.trim()), (200, "1"), "recovered");
    drop(a);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deleted_actor_stays_deleted_on_another_node() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_millis(200);
    let a = node_with("a", da.path(), &store, idle, "t", test_actor_wasm()).await;
    let b = node_with("b", db.path(), &store, idle, "t", test_actor_wasm()).await;
    settle().await;

    let r = send(a.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!((r.body.trim(), r.node.as_deref()), ("1", Some("a")));
    let r = send(a.addr, "POST", "/actor/t/x/delete", "").await;
    assert_eq!(r.status, 200);

    // On a again (same lease), then on b after a hands it over: empty both
    // times, and still replicating afterwards.
    let r = send(a.addr, "GET", "/actor/t/x/durable", "").await;
    assert_eq!((r.body.trim(), r.node.as_deref()), ("0", Some("a")));
    let r = send(a.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!(r.body.trim(), "1");
    tokio::time::sleep(Duration::from_millis(600)).await;
    let r = send(b.addr, "GET", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "1", Some("b"))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_chain_too_large_for_the_node_is_refused_and_leaves_nothing_behind() {
    // a writes about 2 MB of incompressible data; b's cap is 16 KB (its
    // restore bound 64 KB): b refuses before reading the objects, and no
    // half-written .restore file stays on its disk.
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let down = Arc::new(AtomicBool::new(false));
    let seen_by_a: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down)));
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (idle, wall) = (Duration::from_secs(30), Duration::from_secs(30));
    let wasm = test_actor_wasm();
    let a = node_configured(
        "a",
        da.path(),
        &seen_by_a,
        idle,
        &["t"],
        wasm,
        wall,
        "",
        |_| {},
    )
    .await;
    let b = node_configured("b", db.path(), &shared, idle, &["t"], wasm, wall, "", |c| {
        c.storage.max_db_bytes = 16 * 1024;
    })
    .await;
    settle().await;

    for sql in [
        "CREATE TABLE big (x)",
        "INSERT INTO big VALUES (randomblob(2000000))",
    ] {
        let mut stream = TcpStream::connect(a.addr).await.unwrap();
        let req = format!(
            "POST /actor/t/x/sql HTTP/1.1\r\nHost: t\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{sql}",
            sql.len()
        );
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut raw = String::new();
        stream.read_to_string(&mut raw).await.unwrap();
        assert!(raw.contains("ok:"), "{raw}");
    }
    // A plain query answers before its segment is uploaded: wait until
    // the 2 MB is in the bucket, or b restores the short chain that is
    // (it failed so under full-suite load with a fixed 300 ms wait).
    let started = std::time::Instant::now();
    loop {
        use futures_util::TryStreamExt;
        let sizes: Vec<u64> = shared
            .list(None)
            .map_ok(|m| m.size)
            .try_collect()
            .await
            .unwrap();
        if sizes.iter().any(|&n| n > 1_000_000) {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "the 2 MB segment never landed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    down.store(true, Ordering::SeqCst);

    let started = std::time::Instant::now();
    loop {
        let r = send(b.addr, "GET", "/actor/t/x/inits", "").await;
        // Forwarded to a while a's lease lasts; b itself must never serve.
        assert!(
            !(r.status == 200 && r.node.as_deref() == Some("b")),
            "b restored a chain over its bound"
        );
        if started.elapsed() > Duration::from_secs(5) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let leftovers: Vec<_> = walk(db.path())
        .into_iter()
        .filter(|p| p.to_string_lossy().ends_with(".restore"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_committed_before_a_startup_error_still_reach_the_bucket() {
    // on-init commits its writes, then recording that it ran fails (a full
    // disk): the event fails, but what on-init committed must still be
    // replicated, or b, taking over, starts from nothing.
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    wasmkeep_storage::faults::fail_next("set_meta:init_version");
    let r = send(p.a.addr, "GET", "/actor/t/x/inits", "").await;
    assert_eq!(
        r.status, 500,
        "the injected fault fails the event: {}",
        r.body
    );

    // a is cut off; b restores the actor, finds on-init not recorded and
    // runs it again: two runs, the first one's row kept.
    tokio::time::sleep(Duration::from_millis(600)).await;
    p.flaky.down.store(true, Ordering::SeqCst);
    let started = std::time::Instant::now();
    let r = loop {
        let r = send(p.b.addr, "GET", "/actor/t/x/inits", "").await;
        if r.node.as_deref() == Some("b") || started.elapsed() > Duration::from_secs(10) {
            break r;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(r.node.as_deref(), Some("b"), "b took over");
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "2"),
        "on-init's first, committed run was lost"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn readiness_follows_the_bucket_and_metrics_count_events() {
    // Metrics are per process, and nextest runs each test in its own: the
    // counts below are this test's alone.
    let down = Arc::new(AtomicBool::new(false));
    let store: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::new(InMemory::new()), Arc::clone(&down)));
    let data = tempfile::tempdir().unwrap();
    let a = node_with(
        "a",
        data.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    // Ready once the secret is loaded and a heartbeat reached the bucket.
    assert_eq!(wait_for(a.addr, "/_wasmkeep/ready", 200).await, "ready\n");
    for _ in 0..3 {
        let r = send(a.addr, "POST", "/actor/t/x/durable", "").await;
        assert_eq!(r.status, 200, "{}", r.body);
    }
    let r = send(a.addr, "GET", "/actor/t/x/no-such-route", "").await;
    assert_eq!(r.status, 404, "the guest answers it: an ok event");

    // The bucket goes away: not ready, and the public answer says no more.
    down.store(true, Ordering::SeqCst);
    assert_eq!(
        wait_for(a.addr, "/_wasmkeep/ready", 503).await,
        "not ready\n"
    );
    down.store(false, Ordering::SeqCst);
    wait_for(a.addr, "/_wasmkeep/ready", 200).await;

    // The metrics page, on its own listener.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let metrics = listener.local_addr().unwrap();
    tokio::spawn(wasmkeep_server::serve_metrics(
        listener,
        None,
        std::future::pending(),
    ));
    let (status, page) = get_raw(metrics, "/metrics").await;
    assert_eq!(status, 200);
    let value = |series: &str| -> u64 {
        page.lines()
            .find_map(|l| l.strip_prefix(series).and_then(|v| v.strip_prefix(' ')))
            .unwrap_or_else(|| panic!("no {series} in\n{page}"))
            .trim()
            .parse()
            .unwrap()
    };
    let events = |kind: &str, outcome: &str| {
        value(&format!(
            "wasmkeep_events_total{{class=\"t\",kind=\"{kind}\",outcome=\"{outcome}\"}}"
        ))
    };
    // One actor: created once, its on-init run once, four requests.
    assert_eq!(events("init", "ok"), 1, "{page}");
    assert_eq!(events("request", "ok"), 4, "{page}");
    assert!(!page.contains("outcome=\"failed\""), "{page}");
    assert_eq!(value("wasmkeep_wakes_total"), 1, "{page}");
    assert_eq!(value("wasmkeep_hibernations_total"), 0, "{page}");
    assert_eq!(value("wasmkeep_instantiate_seconds_count"), 1, "{page}");
    assert_eq!(value("wasmkeep_durable_write_seconds_count"), 3, "{page}");
    assert_eq!(
        value("wasmkeep_event_duration_seconds_count{class=\"t\"}"),
        5,
        "{page}"
    );
    // Every bucket request is counted, failures apart from the rest.
    let bucket = |operation: &str, status: &str| {
        let series = format!(
            "wasmkeep_bucket_requests_total{{operation=\"{operation}\",status=\"{status}\"}} "
        );
        page.lines()
            .find_map(|l| l.strip_prefix(series.as_str()))
            .map_or(0, |v| v.trim().parse::<u64>().unwrap())
    };
    assert!(bucket("put", "ok") >= 3, "segments and heartbeats\n{page}");
    assert!(
        bucket("get", "ok") + bucket("get", "not_found") >= 1,
        "{page}"
    );
    assert!(bucket("put", "error") >= 1, "the bucket was down\n{page}");
    assert_eq!(value("wasmkeep_failovers_total"), 0, "{page}");
    // The node lease's writes are counted (ADR 0020: what replaced the
    // per-actor renewals).
    assert!(value("wasmkeep_node_lease_writes_total") >= 1, "{page}");
    // Only /metrics is served there.
    assert_eq!(get_raw(metrics, "/").await.0, 404);
}

#[tokio::test(flavor = "multi_thread")]
async fn history_below_the_owners_snapshot_is_collected() {
    // ADR 0012, with no retention window: after the periodic snapshot (64
    // segments), the prefix keeps that snapshot, the segment at its
    // position and the segments after it; another node restores from them.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = node_retaining("a", da.path(), &store, Some(Duration::ZERO)).await;
    let b = node_retaining("b", db.path(), &store, Some(Duration::ZERO)).await;
    settle().await;
    for i in 1..=70 {
        let r = send(a.addr, "POST", "/actor/t/gc/durable", "").await;
        assert_eq!((r.status, r.body.trim()), (200, i.to_string().as_str()));
    }
    let started = std::time::Instant::now();
    let (snapshots, segments) = loop {
        let (snapshots, segments) = history_of(&store, "gc").await;
        if snapshots.len() == 1 && segments.len() < 20 {
            break (snapshots, segments);
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "never collected: snapshots {snapshots:?}, {} segments",
            segments.len()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let at = snapshots[0];
    assert!(
        segments.contains(&at) && segments.iter().all(|&t| t >= at),
        "kept: snapshot at {at}, segments {segments:?}"
    );
    // b restores the actor from what is left.
    drop(a);
    let started = std::time::Instant::now();
    loop {
        let r = send(b.addr, "GET", "/actor/t/gc/durable", "").await;
        // a may still answer one forwarded request while it shuts down,
        // before it hands the actor over: wait for b.
        if r.status == 200 && r.node.as_deref() == Some("b") {
            assert_eq!(r.body.trim(), "70");
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "{} {}",
            r.status,
            r.body
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn history_younger_than_the_retention_window_is_kept() {
    // The default window (1 h): nothing written in this test is old enough.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let da = tempfile::tempdir().unwrap();
    let a = node_retaining("a", da.path(), &store, None).await;
    settle().await;
    for _ in 0..70 {
        assert_eq!(
            send(a.addr, "POST", "/actor/t/kept/durable", "")
                .await
                .status,
            200
        );
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let (snapshots, segments) = history_of(&store, "kept").await;
    assert!(
        snapshots.len() >= 2,
        "the periodic snapshot is there: {snapshots:?}"
    );
    assert!(
        segments.len() >= 70,
        "nothing collected: {} segments",
        segments.len()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deleted_actors_history_is_collected_down_to_two_objects() {
    // A delete erases the actor's history whatever the retention window:
    // the reset segment and a snapshot of the empty database remain, so the
    // actor is still known to exist.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let da = tempfile::tempdir().unwrap();
    let a = node_retaining("a", da.path(), &store, None).await;
    settle().await;
    for _ in 0..10 {
        assert_eq!(
            send(a.addr, "POST", "/actor/t/gone/durable", "")
                .await
                .status,
            200
        );
    }
    assert_eq!(
        send(a.addr, "POST", "/actor/t/gone/delete", "")
            .await
            .status,
        200
    );
    let started = std::time::Instant::now();
    loop {
        let keys = actor_keys(&store, "gone").await;
        if keys.len() == 2 {
            assert!(keys.iter().any(|k| k.contains("/snapshot/")), "{keys:?}");
            assert!(keys.iter().any(|k| k.contains("/wal/")), "{keys:?}");
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "still {} objects: {keys:?}",
            keys.len()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Another node restores what is left: the empty database.
    drop(a);
    let db = tempfile::tempdir().unwrap();
    let b = node_retaining("b", db.path(), &store, None).await;
    let started = std::time::Instant::now();
    loop {
        let r = send(b.addr, "GET", "/actor/t/gone/durable", "").await;
        if r.status == 200 {
            assert_eq!((r.body.trim(), r.node.as_deref()), ("0", Some("b")));
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "{} {}",
            r.status,
            r.body
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delete_whose_local_files_cannot_be_removed_still_records_an_empty_actor() {
    // The actor deletes itself, and removing its database file fails on
    // this node. The bucket must still record the empty database, not the
    // old data reopened from the file that could not be removed: another
    // node restores the actor empty.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let da = tempfile::tempdir().unwrap();
    let a = node_retaining("a", da.path(), &store, None).await;
    settle().await;
    for _ in 0..3 {
        assert_eq!(
            send(a.addr, "POST", "/actor/t/stuck/durable", "")
                .await
                .status,
            200
        );
    }
    wasmkeep_storage::faults::fail_next("delete");
    assert_eq!(
        send(a.addr, "POST", "/actor/t/stuck/delete", "")
            .await
            .status,
        200
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Its history is erased all the same: the reset and its snapshot.
    let started = std::time::Instant::now();
    while actor_keys(&store, "stuck").await.len() != 2 {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "history not erased: {:?}",
            actor_keys(&store, "stuck").await
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // The node that deleted it does not serve the old file either.
    let r = send(a.addr, "GET", "/actor/t/stuck/durable", "").await;
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "0"),
        "stale local data served"
    );
    drop(a);
    let db = tempfile::tempdir().unwrap();
    let b = node_retaining("b", db.path(), &store, None).await;
    let started = std::time::Instant::now();
    loop {
        let r = send(b.addr, "GET", "/actor/t/stuck/durable", "").await;
        if r.status == 200 {
            assert_eq!(
                (r.body.trim(), r.node.as_deref()),
                ("0", Some("b")),
                "the deleted actor came back with its data"
            );
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "{} {}",
            r.status,
            r.body
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // The history goes on from that reset: a write on b, then a third node
    // restores it.
    let r = send(b.addr, "POST", "/actor/t/stuck/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"));
    drop(b);
    let dc = tempfile::tempdir().unwrap();
    let c = node_retaining("c", dc.path(), &store, None).await;
    let started = std::time::Instant::now();
    loop {
        let r = send(c.addr, "GET", "/actor/t/stuck/durable", "").await;
        if r.status == 200 {
            assert_eq!((r.body.trim(), r.node.as_deref()), ("1", Some("c")));
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "{} {}",
            r.status,
            r.body
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
