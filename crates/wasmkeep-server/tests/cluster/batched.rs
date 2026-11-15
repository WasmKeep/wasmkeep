//! Batched plain writes on a cluster (ADR 0034, slice S9), in a binary
//! built with `batched-writes`: an event's writes go up after its answer,
//! several events to a segment; `flush` and `exec-durable` wait for the
//! acknowledged head, until the wall less one second; a failed upload
//! gives the actor up; the event-start rule holds a busy actor whose
//! bucket is slow.
//!
//! The class `t` is batched on node a (through the node's configuration,
//! as a deployed version names it); b restores from the bucket.

use super::*;

/// a (batched, behind `flaky`) and b (strict, the bucket as it is).
async fn batched_pair(wall: Duration, adjust: impl FnOnce(&mut ActorConfig)) -> Pair {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("wasmkeep_server=info")
        .with_test_writer()
        .try_init();
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let seen_by_a: Arc<dyn ObjectStore> = Arc::clone(&flaky) as Arc<dyn ObjectStore>;
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let a = node_configured(
        "a",
        dirs.0.path(),
        &seen_by_a,
        idle,
        &["t"],
        test_actor_wasm(),
        wall,
        "",
        |c| {
            c.batched.insert("t".into());
            adjust(c);
        },
    )
    .await;
    let b = node_full(
        "b",
        dirs.1.path(),
        &shared,
        idle,
        "t",
        test_actor_wasm(),
        wall,
    )
    .await;
    settle().await;
    Pair {
        a,
        b,
        flaky,
        _dirs: dirs,
    }
}

/// The actor's history segments in the bucket now.
async fn segments(p: &Pair) -> usize {
    let store: Arc<dyn ObjectStore> = p.flaky.clone();
    bucket_keys(&store)
        .await
        .iter()
        .filter(|k| k.contains("/wal/"))
        .count()
}

/// a cut off from the bucket; b restores the actor and answers `path`.
async fn b_answers(p: &Pair, path: &str) -> Reply {
    p.flaky.down.store(true, Ordering::SeqCst);
    let started = std::time::Instant::now();
    loop {
        let r = poll_get(p.b.addr, path).await;
        if r.node.as_deref() == Some("b") || started.elapsed() > Duration::from_secs(10) {
            return r;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The first event, its first snapshot and segment up, before a test
/// slows the bucket.
async fn warmed(p: &Pair) {
    let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"), "{}", r.body);
    tokio::time::sleep(Duration::from_millis(500)).await;
}

/// Plain writes answer before their upload, and those made while a
/// segment is in flight go up together, in fewer segments than events;
/// once they landed, a takeover finds every one (ADR 0034, section 2).
#[tokio::test(flavor = "multi_thread")]
async fn plain_writes_go_up_after_their_answer_several_to_a_segment() {
    let p = batched_pair(Duration::from_secs(30), |c| {
        c.max_unsent_age = Duration::from_secs(5);
    })
    .await;
    warmed(&p).await;
    let before = segments(&p).await;
    p.flaky.delay_wal_ms.store(500, Ordering::SeqCst);
    let started = std::time::Instant::now();
    for n in 2..=11 {
        let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
        assert_eq!((r.status, r.body.trim()), (200, n.to_string().as_str()));
    }
    // Ten events, each upload 500 ms: strict would take 5 s.
    assert!(
        started.elapsed() < Duration::from_millis(3_500),
        "the events waited for their uploads: {:?}",
        started.elapsed()
    );
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    let uploaded = segments(&p).await - before;
    assert!(
        (1..10).contains(&uploaded),
        "{uploaded} segments for ten events"
    );
    p.flaky.delay_wal_ms.store(0, Ordering::SeqCst);
    let r = b_answers(&p, "/actor/t/x/plain").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "11", Some("b")),
        "{}",
        r.body
    );
}

/// A durable write of a batched class answers once its segment is
/// acknowledged: the node cut off right after, the next owner has it
/// (ADR 0034, section 4).
#[tokio::test(flavor = "multi_thread")]
async fn a_durable_write_answers_once_acknowledged() {
    let p = batched_pair(Duration::from_secs(30), |_| {}).await;
    warmed(&p).await;
    p.flaky.delay_wal_ms.store(200, Ordering::SeqCst);
    for _ in 0..3 {
        let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
        assert_eq!(r.status, 200, "{}", r.body);
    }
    let r = send(p.a.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"), "{}", r.body);
    let r = b_answers(&p, "/actor/t/x/plain").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "4", Some("b")),
        "the plain writes before the durable one are durable with it: {}",
        r.body
    );
    let r = poll_get(p.b.addr, "/actor/t/x/durable").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"), "{}", r.body);
}

/// A flush whose upload outlasts the wall less one second answers
/// `not durable: still uploading`: the replica stays healthy, a later
/// event of the same hold sees the write, and the write lands (ADR 0034,
/// section 4).
#[tokio::test(flavor = "multi_thread")]
async fn a_flush_past_the_wall_answers_still_uploading_and_lands() {
    let p = batched_pair(Duration::from_secs(3), |_| {}).await;
    warmed(&p).await;
    p.flaky.delay_wal_ms.store(3_500, Ordering::SeqCst);
    let started = std::time::Instant::now();
    let r = send(p.a.addr, "POST", "/actor/t/x/flush?n=1", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(
        r.body.trim(),
        "err:SqlErrorKind::Internal:not durable: still uploading",
        "{}",
        r.body
    );
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(1_500) && took < Duration::from_millis(3_500),
        "answered after {took:?}, not at the wall less one second"
    );
    // The same hold, the write still there.
    let r = send(p.a.addr, "POST", "/actor/t/x/flush?n=0", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    p.flaky.delay_wal_ms.store(0, Ordering::SeqCst);
    let r = send(p.a.addr, "POST", "/actor/t/x/flush?n=0", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "ok:2"), "{}", r.body);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let r = b_answers(&p, "/actor/t/x/plain").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "2", Some("b")),
        "{}",
        r.body
    );
}

/// A flush with nothing committed since the last acknowledged segment
/// answers at once; inside a transaction it is refused (ADR 0034,
/// section 4).
#[tokio::test(flavor = "multi_thread")]
async fn a_flush_with_nothing_pending_or_in_a_transaction() {
    let p = batched_pair(Duration::from_secs(30), |_| {}).await;
    warmed(&p).await;
    let r = send(p.a.addr, "POST", "/actor/t/x/flush?n=0", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "ok:1"), "{}", r.body);
    let r = send(p.a.addr, "POST", "/actor/t/x/flush?n=1&tx", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert!(
        r.body.starts_with("err:SqlErrorKind::NotAllowed"),
        "{}",
        r.body
    );
}

/// An upload that fails after its event answered gives the actor up: the
/// completion job applies the failure, and the next event restores what
/// the bucket has, without that event's write (ADR 0034, sections 2 and 8).
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_upload_gives_the_actor_up_between_events() {
    let p = batched_pair(Duration::from_secs(30), |_| {}).await;
    warmed(&p).await;
    // Every attempt of the next segment's upload fails.
    p.flaky.fail_wal.store(3, Ordering::SeqCst);
    let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "2"), "{}", r.body);
    eventually("every attempt used", async || {
        p.flaky.fail_wal.load(Ordering::SeqCst) == 0
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let r = send(p.a.addr, "GET", "/actor/t/x/plain", "").await;
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "1"),
        "restored without the lost write: {}",
        r.body
    );
}

/// With a slow bucket, an event that ends with a commit older than
/// `--max-unsent-age` uncaptured waits at its end, after its answer, for
/// the segment in flight: the next request queues behind it. Without the
/// bound in reach, it does not (ADR 0034, section 3).
#[tokio::test(flavor = "multi_thread")]
async fn the_event_start_rule_holds_a_busy_actor_behind_a_slow_bucket() {
    async fn next_after_held(max_unsent: Duration) -> Duration {
        let p = batched_pair(Duration::from_secs(30), |c| c.max_unsent_age = max_unsent).await;
        warmed(&p).await;
        p.flaky.delay_wal_ms.store(2_000, Ordering::SeqCst);
        // In flight, then uncaptured behind it.
        for _ in 0..2 {
            let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
            assert_eq!(r.status, 200, "{}", r.body);
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
        assert_eq!(r.status, 200, "{}", r.body);
        let started = std::time::Instant::now();
        let r = send(p.a.addr, "GET", "/actor/t/x/plain", "").await;
        assert_eq!((r.status, r.body.trim()), (200, "4"), "{}", r.body);
        started.elapsed()
    }
    let held = next_after_held(Duration::from_millis(100)).await;
    assert!(
        held >= Duration::from_millis(1_000),
        "the next event started at once: {held:?}"
    );
    let free = next_after_held(Duration::from_secs(5)).await;
    assert!(
        free < Duration::from_millis(900),
        "the next event waited without the rule acting: {free:?}"
    );
}

/// `delete-self-durable` in a batched class flushes, then resets at the
/// acknowledged head: the next owner finds the actor empty (ADR 0034,
/// section 7).
#[tokio::test(flavor = "multi_thread")]
async fn a_durable_delete_flushes_then_resets() {
    let p = batched_pair(Duration::from_secs(30), |_| {}).await;
    warmed(&p).await;
    p.flaky.delay_wal_ms.store(200, Ordering::SeqCst);
    for _ in 0..2 {
        let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
        assert_eq!(r.status, 200, "{}", r.body);
    }
    let r = send(p.a.addr, "POST", "/actor/t/x/delete-durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "deleted"), "{}", r.body);
    p.flaky.delay_wal_ms.store(0, Ordering::SeqCst);
    let r = b_answers(&p, "/actor/t/x/plain").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "0", Some("b")),
        "{}",
        r.body
    );
}

/// `delete-self` at the end of its event flushes, then records the reset
/// (ADR 0034, section 6).
#[tokio::test(flavor = "multi_thread")]
async fn a_delete_at_the_events_end_flushes_then_resets() {
    let p = batched_pair(Duration::from_secs(30), |_| {}).await;
    warmed(&p).await;
    p.flaky.delay_wal_ms.store(200, Ordering::SeqCst);
    let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    let r = send(p.a.addr, "POST", "/actor/t/x/delete", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "ok"), "{}", r.body);
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    p.flaky.delay_wal_ms.store(0, Ordering::SeqCst);
    let r = b_answers(&p, "/actor/t/x/plain").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "0", Some("b")),
        "{}",
        r.body
    );
}

/// How long the event after a plain write takes to start: with a slow
/// bucket, a strict hold's write waits for its upload, a batched one's
/// does not.
async fn next_after_a_write(addr: SocketAddr) -> Duration {
    let r = send(addr, "POST", "/actor/t/x/plain", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    let started = std::time::Instant::now();
    let r = send(addr, "GET", "/actor/t/x/plain", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    started.elapsed()
}

/// A redeploy from batched to strict reaches a resident actor at a
/// `Flushed` barrier before its next event, and its writes go up inline
/// from then on; nothing is given up (ADR 0034, section 1). Strict to
/// batched needs no flush.
#[tokio::test(flavor = "multi_thread")]
async fn a_mode_switch_waits_for_a_flushed_barrier() {
    use wasmkeep_server::apps::{Changes, Writes};
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let store: Arc<dyn ObjectStore> = Arc::clone(&flaky) as Arc<dyn ObjectStore>;
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let rt = deploy_runtime();
    let deploy = |writes| Changes {
        classes: Some(vec![("t".to_owned(), test_actor_wasm().to_vec())]),
        writes: vec![("t".into(), writes)],
        ..Changes::default()
    };
    app.deploy_changes(&rt, deploy(Writes::Batched))
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (node, service) = node_app(
        "a",
        dir.path(),
        &store,
        Duration::from_secs(30),
        Duration::from_millis(100),
    )
    .await;
    let r = send_runnable(node.addr, "POST", "/actor/t/x/plain", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    tokio::time::sleep(Duration::from_millis(300)).await;
    flaky.delay_wal_ms.store(1_200, Ordering::SeqCst);
    let batched = next_after_a_write(node.addr).await;
    assert!(batched < Duration::from_millis(800), "batched: {batched:?}");

    // A segment in flight as the version turns strict (its upload takes
    // longer than the node takes to follow the deploy).
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    let r = send(node.addr, "POST", "/actor/t/x/plain", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    let done = app
        .deploy_changes(&rt, deploy(Writes::Strict))
        .await
        .unwrap();
    runs_deploy(&service, done.current.seq).await;
    let strict = next_after_a_write(node.addr).await;
    assert!(strict >= Duration::from_millis(1_000), "strict: {strict:?}");
    let r = send(node.addr, "GET", "/actor/t/x/plain", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "4"), "{}", r.body);
    assert_eq!(
        wasmkeep_server::metrics::plain_events_dropped_total(),
        0,
        "nothing given up"
    );

    // And back: no flush needed.
    let done = app
        .deploy_changes(&rt, deploy(Writes::Batched))
        .await
        .unwrap();
    runs_deploy(&service, done.current.seq).await;
    let again = next_after_a_write(node.addr).await;
    assert!(
        again < Duration::from_millis(800),
        "batched again: {again:?}"
    );
}

/// An idle batched actor's step before the drop flushes it: nothing it
/// wrote is lost when it hibernates with uploads pending, and a takeover
/// finds every write (ADR 0034, section 6).
#[tokio::test(flavor = "multi_thread")]
async fn hibernation_flushes_first() {
    let p = batched_pair(Duration::from_secs(30), |c| {
        c.mailbox.idle_timeout = Duration::from_millis(300);
        c.max_unsent_age = Duration::from_secs(5);
    })
    .await;
    warmed(&p).await;
    p.flaky.delay_wal_ms.store(700, Ordering::SeqCst);
    for _ in 0..3 {
        let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
        assert_eq!(r.status, 200, "{}", r.body);
    }
    eventually("hibernated", async || p.a.service.resident() == 0).await;
    p.flaky.delay_wal_ms.store(0, Ordering::SeqCst);
    let r = b_answers(&p, "/actor/t/x/plain").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "4", Some("b")),
        "{}",
        r.body
    );
}

/// A flush called with less than a second of the wall left records its
/// boundary and answers `not durable: still uploading` at once; its
/// commit lands with the next segment (ADR 0034, section 4).
#[tokio::test(flavor = "multi_thread")]
async fn a_flush_with_under_a_second_left_answers_at_once_and_lands() {
    let p = batched_pair(Duration::from_secs(3), |_| {}).await;
    warmed(&p).await;
    p.flaky.delay_wal_ms.store(1_500, Ordering::SeqCst);
    let started = std::time::Instant::now();
    let r = send(p.a.addr, "POST", "/actor/t/x/flush?n=1&sleep=2300", "").await;
    let took = started.elapsed();
    assert_eq!(
        (r.status, r.body.trim()),
        (
            200,
            "err:SqlErrorKind::Internal:not durable: still uploading"
        ),
        "{}",
        r.body
    );
    assert!(
        took < Duration::from_millis(2_900),
        "the flush waited: {took:?}"
    );
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    p.flaky.delay_wal_ms.store(0, Ordering::SeqCst);
    let r = b_answers(&p, "/actor/t/x/plain").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "2", Some("b")),
        "{}",
        r.body
    );
}

/// An event that alone writes more than the cap is its own segment, and
/// its actor waits for it at the event's end, as a strict actor does: the
/// next request queues behind that upload (ADR 0034, section 2).
#[tokio::test(flavor = "multi_thread")]
async fn an_event_over_the_cap_waits_for_its_segment() {
    let p = batched_pair(Duration::from_secs(30), |c| {
        c.max_unsent_age = Duration::from_secs(5);
    })
    .await;
    warmed(&p).await;
    p.flaky.delay_wal_ms.store(1_200, Ordering::SeqCst);
    let r = send(p.a.addr, "POST", "/actor/t/x/big?kb=1500", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "big"), "{}", r.body);
    let started = std::time::Instant::now();
    let r = send(p.a.addr, "GET", "/actor/t/x/plain", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"), "{}", r.body);
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(900),
        "the next event did not wait for the oversize segment: {waited:?}"
    );
    // A small write does not hold the next one.
    let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    let started = std::time::Instant::now();
    let r = send(p.a.addr, "GET", "/actor/t/x/plain", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "2"), "{}", r.body);
    assert!(
        started.elapsed() < Duration::from_millis(800),
        "a small write held the next event: {:?}",
        started.elapsed()
    );
}

const STILL: &str = "err:SqlErrorKind::Internal:not durable: still uploading";

/// A flush whose capture waits for room in a full node budget still
/// answers by its deadline, the wall less one second, and the write goes
/// up once there is room (ADR 0034, sections 2 and 4).
#[tokio::test(flavor = "multi_thread")]
async fn a_flush_behind_a_full_node_budget_answers_by_its_deadline() {
    let p = batched_pair(Duration::from_secs(3), |_| {}).await;
    warmed(&p).await;
    let full = wasmkeep_server::hold_node_upload_budget();
    let r = send(p.a.addr, "POST", "/actor/t/x/flush?n=1", "").await;
    assert_eq!((r.status, r.body.trim()), (200, STILL), "{}", r.body);
    drop(full);
    let r = send(p.a.addr, "POST", "/actor/t/x/flush?n=0", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "ok:2"), "{}", r.body);
}

/// An alarm entry PUT slower than the flush's deadline does not hold the
/// flush past it (ADR 0034, section 4).
#[tokio::test(flavor = "multi_thread")]
async fn a_flush_behind_a_slow_alarm_entry_answers_by_its_deadline() {
    let p = batched_pair(Duration::from_secs(3), |_| {}).await;
    warmed(&p).await;
    p.flaky.delay_alarm_puts_ms.store(5_000, Ordering::SeqCst);
    let r = send(p.a.addr, "POST", "/actor/t/x/flush?n=1&alarm=600000", "").await;
    assert_eq!((r.status, r.body.trim()), (200, STILL), "{}", r.body);
}

/// `delete-self-durable` lists a pending alarm in the bucket's index before
/// the flush that uploads it, as `flush` does: no restorable state names an
/// alarm without its entry (ADR 0034, section 9).
#[tokio::test(flavor = "multi_thread")]
async fn a_durable_delete_lists_the_alarm_it_flushes_first() {
    let p = batched_pair(Duration::from_secs(30), |_| {}).await;
    warmed(&p).await;
    let r = send(
        p.a.addr,
        "POST",
        "/actor/t/x/delete-durable?alarm=600000",
        "",
    )
    .await;
    assert_eq!((r.status, r.body.trim()), (200, "deleted"), "{}", r.body);
    let store: Arc<dyn ObjectStore> = p.flaky.clone();
    let entries = bucket_keys(&store)
        .await
        .iter()
        .filter(|k| k.starts_with(&test_key(Family::Alarms, "")))
        .count();
    assert!(entries > 0, "the flushed alarm has no entry");
}

/// A completion job refused by a full mailbox is dropped; its outcome waits
/// in the replica and is applied at the next boundary, and nothing is lost
/// (ADR 0034, section 2).
#[tokio::test(flavor = "multi_thread")]
async fn a_completion_dropped_by_a_full_mailbox_is_applied_at_the_next_boundary() {
    let p = batched_pair(Duration::from_secs(30), |c| {
        c.mailbox.queue_capacity = 1;
    })
    .await;
    warmed(&p).await;
    p.flaky.delay_wal_ms.store(400, Ordering::SeqCst);
    let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "2"), "{}", r.body);
    // The actor busy, its one queue slot taken, as the segment lands.
    let addr = p.a.addr;
    let busy =
        tokio::spawn(async move { send(addr, "POST", "/actor/t/x/sleep?ms=1500", "").await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let queued = tokio::spawn(async move { send(addr, "GET", "/actor/t/x/plain", "").await });
    assert_eq!(busy.await.unwrap().status, 200);
    let r = queued.await.unwrap();
    assert_eq!((r.status, r.body.trim()), (200, "2"), "{}", r.body);
    p.flaky.delay_wal_ms.store(0, Ordering::SeqCst);
    let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "3"), "{}", r.body);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let r = b_answers(&p, "/actor/t/x/plain").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "3", Some("b")),
        "{}",
        r.body
    );
}

/// A drain's hand-over of a batched actor flushes first: the peer that
/// takes it finds every write (ADR 0034, section 6).
#[tokio::test(flavor = "multi_thread")]
async fn a_drain_flushes_a_batched_actor_before_handing_it_over() {
    let p = batched_pair(Duration::from_secs(30), |c| {
        c.max_unsent_age = Duration::from_secs(5);
    })
    .await;
    warmed(&p).await;
    p.flaky.delay_wal_ms.store(600, Ordering::SeqCst);
    for _ in 0..3 {
        let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
        assert_eq!(r.status, 200, "{}", r.body);
    }
    let store: Arc<dyn ObjectStore> = p.flaky.clone();
    drain(&store, "a").await;
    eventually("a handed the actor over", async || {
        lease_holder(&store, "x").await.as_deref() != Some("a")
    })
    .await;
    let r = poll_get(p.b.addr, "/actor/t/x/plain").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "4", Some("b")),
        "{}",
        r.body
    );
    assert_eq!(wasmkeep_server::metrics::plain_events_dropped_total(), 0);
}

/// A batched room hibernating with a socket open parks only once flushed:
/// its step before the drop flushes the segment in flight and the writes
/// behind it, then the record keeps naming its node, nothing is given up,
/// and the room wakes there for the socket's next message (ADR 0024; ADR
/// 0034, section 6).
#[tokio::test(flavor = "multi_thread")]
async fn a_batched_room_parks_only_once_flushed() {
    use super::websockets::{chat_holder, join_lobby, lobby_parked};
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let store: Arc<dyn ObjectStore> = Arc::clone(&flaky) as Arc<dyn ObjectStore>;
    let dir = tempfile::tempdir().unwrap();
    let a = node_configured(
        "a",
        dir.path(),
        &store,
        Duration::from_millis(300),
        &["chat"],
        chat_wasm(),
        Duration::from_secs(30),
        "",
        |c| {
            c.batched.insert("chat".into());
            c.max_unsent_age = Duration::from_secs(5);
        },
    )
    .await;
    settle().await;
    let mut client = join_lobby(a.addr).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    // A message whose write is still uploading as the room goes idle.
    flaky.delay_wal_ms.store(800, Ordering::SeqCst);
    client.send(Message::text("before")).await.unwrap();
    lobby_parked(&store, &a).await;
    assert_eq!(chat_holder(&store, "lobby").await.as_deref(), Some("a"));
    assert_eq!(
        wasmkeep_server::metrics::plain_events_dropped_total(),
        0,
        "parked with nothing given up"
    );
    flaky.delay_wal_ms.store(0, Ordering::SeqCst);
    client.send(Message::text("after")).await.unwrap();
    let mut seen = Vec::new();
    while !seen.iter().any(|t: &String| t.contains("after")) {
        match tokio::time::timeout(Duration::from_secs(5), client.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => seen.push(t.to_string()),
            Ok(Some(Ok(_))) => {}
            other => panic!("no message after the wake: {other:?} after {seen:?}"),
        }
    }
}
