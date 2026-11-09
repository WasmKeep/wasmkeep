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
