//! Batched plain writes on a cluster (ADR 0034, slice S9): an event's writes go up after its answer,
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

/// The reasons `wasmkeep_plain_events_dropped_total` counted anything
/// under in this process, with their counts.
fn dropped_by_reason() -> Vec<(&'static str, u64)> {
    [
        "stop",
        "failure",
        "lapse",
        "lost_lease",
        "panic",
        "app_deleted",
        "unbarriered",
    ]
    .into_iter()
    .map(|why| (why, wasmkeep_server::metrics::plain_events_dropped_by(why)))
    .filter(|(_, n)| *n > 0)
    .collect()
}

/// A redeploy from batched to strict reaches a resident actor at a
/// `Flushed` barrier before its next event, and its writes go up inline
/// from then on; nothing is given up (ADR 0034, section 1). Strict to
/// batched needs no flush.
///
/// The hold runs under a 3 s lease: its segments take 1.2 s, and under
/// the usual 1 s lease a runner stalled for 0.8 s lapses the hold while
/// one is in flight, which ADR 0034 (sections 3 and 6) counts as given up
/// whatever the mode (one `failure` when the segment lands `Late`). The
/// segment in flight at the switch is held, not timed.
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
    let (node, service) = node_app_leased(
        "a",
        dir.path(),
        &store,
        Duration::from_secs(30),
        Duration::from_millis(100),
        LeaseConfig::new(3_000, 200, 200, 100).unwrap(),
    )
    .await;
    let r = send_runnable(node.addr, "POST", "/actor/t/x/plain", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    tokio::time::sleep(Duration::from_millis(300)).await;
    flaky.delay_wal_ms.store(1_200, Ordering::SeqCst);
    let batched = next_after_a_write(node.addr).await;
    assert!(batched < Duration::from_millis(800), "batched: {batched:?}");

    // A segment in flight as the version turns strict: its upload is held
    // until the next write is seen waiting for it.
    flaky.hold_wal_puts.send_replace(true);
    let r = send(node.addr, "POST", "/actor/t/x/plain", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    let started = std::time::Instant::now();
    while flaky.held_wal_puts.load(Ordering::SeqCst) == 0 {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "no segment went up"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let done = app
        .deploy_changes(&rt, deploy(Writes::Strict))
        .await
        .unwrap();
    runs_deploy(&service, done.current.seq).await;
    let addr = node.addr;
    let write = tokio::spawn(async move { send(addr, "POST", "/actor/t/x/plain", "").await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !write.is_finished(),
        "the switch waits for the segment in flight"
    );
    flaky.hold_wal_puts.send_replace(false);
    let r = write.await.unwrap();
    assert_eq!(r.status, 200, "{}", r.body);
    let strict = next_after_a_write(node.addr).await;
    assert!(strict >= Duration::from_millis(1_000), "strict: {strict:?}");
    let r = send(node.addr, "GET", "/actor/t/x/plain", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "5"), "{}", r.body);
    assert_eq!(dropped_by_reason(), [], "nothing given up");
    assert_eq!(wasmkeep_server::metrics::plain_events_dropped_total(), 0);

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
    let r = send(node.addr, "GET", "/actor/t/x/plain", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "6"), "{}", r.body);
    assert_eq!(dropped_by_reason(), [], "nothing given up switching back");
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

/// The actor's alarm index entries in the bucket now.
async fn entries(p: &Pair) -> Vec<String> {
    let store: Arc<dyn ObjectStore> = p.flaky.clone();
    bucket_keys(&store)
        .await
        .into_iter()
        .filter(|k| k.starts_with(&test_key(Family::Alarms, "")))
        .collect()
}

/// An alarm moved by every event (ADR 0034, section 9): entries are named
/// at each event and listed with the segment that carries them, so an
/// entry named only inside a segment is never written; the entry the
/// acknowledged head named stays until the segment replacing it is
/// acknowledged, then goes. One entry is left, for the last time, and the
/// next owner fires it.
#[tokio::test(flavor = "multi_thread")]
async fn an_alarm_moved_by_every_event_keeps_one_entry_retired_after_acknowledgement() {
    let p = batched_pair(Duration::from_secs(30), |c| {
        c.max_unsent_age = Duration::from_secs(5);
    })
    .await;
    warmed(&p).await;
    let base = now_ms() + 600_000;
    let r = send(p.a.addr, "POST", &format!("/actor/t/x/alarm?at={base}"), "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let first = entries(&p).await;
    assert_eq!(first.len(), 1, "{first:?}");
    p.flaky.delay_wal_ms.store(800, Ordering::SeqCst);
    for i in 1..=5u64 {
        let at = base + i * 1_000;
        let r = send(p.a.addr, "POST", &format!("/actor/t/x/alarm?at={at}"), "").await;
        assert_eq!(r.status, 200, "{}", r.body);
    }
    let during = entries(&p).await;
    assert!(
        during.contains(&first[0]),
        "the acknowledged entry went before its replacement was acknowledged: {during:?}"
    );
    assert!(
        during.len() <= 2,
        "entries named inside a segment were listed: {during:?}"
    );
    p.flaky.delay_wal_ms.store(0, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    let after = entries(&p).await;
    assert_eq!(after.len(), 1, "{after:?}");
    assert!(
        after[0].contains(&format!("{:015}", base + 5_000)),
        "{after:?}"
    );
    let r = b_answers(&p, "/actor/t/x/alarm").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, (base + 5_000).to_string().as_str(), Some("b")),
        "{}",
        r.body
    );
}

/// The actor's entries due at `at` (ms), by their key.
fn due_at(keys: &[String], at: u64) -> usize {
    let at = format!("/{at:015}/");
    keys.iter().filter(|k| k.contains(&at)).count()
}

/// What the actor's handler counted firing, on `addr`.
async fn fired(addr: SocketAddr) -> String {
    send(addr, "GET", "/actor/t/x/fired", "")
        .await
        .body
        .trim()
        .to_owned()
}

/// Recording a batched hold's new alarm entry in its database fails: the
/// replica fails before any boundary ships, so no acknowledged state holds
/// an alarm that no entry lists; the next owner finds either both or
/// neither (ADR 0034, section 9; ADR 0009, R4).
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_entry_record_stops_replication_before_the_alarm_ships() {
    let p = batched_pair(Duration::from_secs(30), |_| {}).await;
    warmed(&p).await;
    let due = now_ms() + 600_000;
    wasmkeep_storage::faults::fail_next("set_meta:alarm_indexed");
    let r = send(p.a.addr, "POST", &format!("/actor/t/x/alarm?at={due}"), "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    tokio::time::sleep(Duration::from_millis(800)).await;
    let listed = due_at(&entries(&p).await, due);
    let r = b_answers(&p, "/actor/t/x/alarm").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "{}",
        r.body
    );
    if r.body.trim() == due.to_string() {
        assert!(listed > 0, "the next owner has an alarm no entry lists");
    } else {
        assert_eq!(r.body.trim(), "none", "{}", r.body);
    }
}

/// The owner dies with its segment's entry PUT in flight: the segment
/// never lands, the next owner has no alarm, and the entry that lands late
/// is removed by the sweep without firing anything (ADR 0034, section 9).
#[tokio::test(flavor = "multi_thread")]
async fn an_owner_dying_with_its_entry_put_in_flight_fires_nothing() {
    let p = batched_pair(Duration::from_secs(30), |_| {}).await;
    warmed(&p).await;
    let due = now_ms() + 2_500;
    p.flaky.delay_alarm_puts_ms.store(1_000, Ordering::SeqCst);
    let r = send(p.a.addr, "POST", &format!("/actor/t/x/alarm?at={due}"), "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    tokio::time::sleep(Duration::from_millis(100)).await;
    p.flaky.down.store(true, Ordering::SeqCst);
    let r = b_answers(&p, "/actor/t/x/alarm").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "none", Some("b")),
        "{}",
        r.body
    );
    let shared: Arc<dyn ObjectStore> = Arc::clone(&p.flaky.inner);
    eventually("the late entry is swept away", async || {
        due_at(&bucket_keys(&shared).await, due) == 0
    })
    .await;
    assert_eq!(fired(p.b.addr).await, "0");
}

/// The owner dies with the segment PUT in flight, its entry already
/// listed: the segment lands late, the next owner restores the alarm, and
/// the sweep fires it there (ADR 0034, section 9).
#[tokio::test(flavor = "multi_thread")]
async fn an_owner_dying_with_its_segment_in_flight_leaves_an_alarm_the_sweep_fires() {
    let p = batched_pair(Duration::from_secs(30), |_| {}).await;
    warmed(&p).await;
    let due = now_ms() + 4_000;
    p.flaky.delay_wal_ms.store(1_000, Ordering::SeqCst);
    let r = send(p.a.addr, "POST", &format!("/actor/t/x/alarm?at={due}"), "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    let shared: Arc<dyn ObjectStore> = Arc::clone(&p.flaky.inner);
    eventually("the entry is listed before the segment", async || {
        due_at(&bucket_keys(&shared).await, due) == 1
    })
    .await;
    // Cut off now: the segment PUT already sent lands late.
    p.flaky.down.store(true, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    eventually("the sweep fires the alarm on b", async || {
        fired(p.b.addr).await == "1"
    })
    .await;
}

/// An alarm delivered for a time a trapped event moved: the local database
/// names the new entry, the acknowledged head still the old one, its
/// segment in flight. The stale entry is cleared only at a barrier, once
/// the head names the new one; until then it stays (ADR 0034, section 9).
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_entry_after_a_trapped_event_is_cleared_only_at_a_barrier() {
    let p = batched_pair(Duration::from_secs(30), |_| {}).await;
    warmed(&p).await;
    let old = now_ms() + 1_500;
    let r = send(p.a.addr, "POST", &format!("/actor/t/x/alarm?at={old}"), "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    let shared: Arc<dyn ObjectStore> = Arc::clone(&p.flaky.inner);
    eventually("the first entry is acknowledged", async || {
        due_at(&bucket_keys(&shared).await, old) == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let new = now_ms() + 600_000;
    p.flaky.delay_wal_ms.store(2_500, Ordering::SeqCst);
    let r = send(
        p.a.addr,
        "POST",
        &format!("/actor/t/x/alarm-then-trap?at={new}"),
        "",
    )
    .await;
    assert_eq!(r.status, 500, "{}", r.body);
    // The old time comes due while the segment naming the new one flies.
    tokio::time::sleep(Duration::from_millis(1_800)).await;
    let keys = bucket_keys(&shared).await;
    assert_eq!(
        due_at(&keys, old),
        1,
        "cleared before the head moved: {keys:?}"
    );
    p.flaky.delay_wal_ms.store(0, Ordering::SeqCst);
    eventually("the stale entry is cleared at the barrier", async || {
        let keys = bucket_keys(&shared).await;
        due_at(&keys, old) == 0 && due_at(&keys, new) == 1
    })
    .await;
    assert_eq!(fired(p.a.addr).await, "0", "the moved alarm did not fire");
}

/// Snapshots and collected segments of the actor, in the bucket now.
async fn history(p: &Pair) -> (usize, usize) {
    let store: Arc<dyn ObjectStore> = p.flaky.clone();
    let keys = bucket_keys(&store).await;
    (
        keys.iter().filter(|k| k.contains("/snapshot/")).count(),
        keys.iter().filter(|k| k.contains("/wal/")).count(),
    )
}

/// A batched hold takes periodic snapshots again, in a mailbox gap at its
/// acknowledged head, and what they make unnecessary is collected: a busy
/// actor's history stays bounded, and the next owner restores every
/// write (ADR 0034, section 7).
#[tokio::test(flavor = "multi_thread")]
async fn a_busy_batched_actor_takes_periodic_snapshots() {
    let p = batched_pair(Duration::from_secs(30), |c| {
        // Collect at once what a snapshot makes unnecessary (ADR 0012).
        if let Some(cluster) = c.cluster.as_mut() {
            cluster.history_retention = Duration::ZERO;
        }
    })
    .await;
    warmed(&p).await;
    for n in 2..=80 {
        let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
        assert_eq!((r.status, r.body.trim()), (200, n.to_string().as_str()));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    // Without a periodic snapshot all 80 segments would stay.
    let (snapshots, segments) = history(&p).await;
    assert!(snapshots >= 1);
    assert!(
        segments < 64,
        "{segments} segments kept: no periodic snapshot"
    );
    let r = b_answers(&p, "/actor/t/x/plain").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "80", Some("b")),
        "{}",
        r.body
    );
}

/// The actor's snapshot keys in the bucket now.
async fn snapshot_keys(p: &Pair) -> Vec<String> {
    let store: Arc<dyn ObjectStore> = p.flaky.clone();
    bucket_keys(&store)
        .await
        .into_iter()
        .filter(|k| k.contains("/snapshot/"))
        .collect()
}

/// A batched hold's periodic snapshot leaves the mailbox once copied: while
/// its upload is held in the bucket, events are answered at once, plain
/// and durable (their segments go up and are acknowledged beside it), and
/// nothing is collected. Once it lands, the history below it is collected
/// and the next owner restores every write (ADR 0034, section 7). Before,
/// the upload ran in the mailbox gap and held every event behind it for
/// its whole attempt.
#[tokio::test(flavor = "multi_thread")]
async fn events_are_answered_while_a_periodic_snapshot_uploads() {
    let p = batched_pair(Duration::from_secs(30), |c| {
        if let Some(cluster) = c.cluster.as_mut() {
            cluster.history_retention = Duration::ZERO;
        }
    })
    .await;
    warmed(&p).await;
    p.flaky.hold_snapshot_puts.store(true, Ordering::SeqCst);
    let held = &p.flaky.snapshot_puts_held;
    let mut n = 1;
    // Far under a periodic snapshot's own bound (6 s for this database).
    let bound = Duration::from_millis(1_500);
    let timed = |path: &'static str| async move {
        let started = std::time::Instant::now();
        let r = tokio::time::timeout(Duration::from_secs(15), send(p.a.addr, "POST", path, ""))
            .await
            .expect("answered within 15 s");
        (r, started.elapsed())
    };
    while held.load(Ordering::SeqCst) == 0 {
        assert!(n < 100, "no periodic snapshot after {n} writes");
        n += 1;
        let (r, took) = timed("/actor/t/x/plain").await;
        assert_eq!((r.status, r.body.trim()), (200, n.to_string().as_str()));
        assert!(took < bound, "write {n} took {took:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let before = snapshot_keys(&p).await;
    // The snapshot's upload is held: the next events are not.
    for _ in 0..3 {
        n += 1;
        let (r, took) = timed("/actor/t/x/plain").await;
        assert_eq!((r.status, r.body.trim()), (200, n.to_string().as_str()));
        assert!(took < bound, "write {n} behind the snapshot took {took:?}");
    }
    let (r, took) = timed("/actor/t/x/durable").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"), "{}", r.body);
    assert!(
        took < bound,
        "a durable write behind the snapshot took {took:?}"
    );
    let (_, segments) = history(&p).await;
    assert_eq!(snapshot_keys(&p).await, before, "nothing landed while held");
    assert!(
        segments >= 64,
        "nothing collected before it landed: {segments}"
    );
    p.flaky.hold_snapshot_puts.store(false, Ordering::SeqCst);
    // It lands, acknowledged beside the segments of the writes that go on,
    // and what it makes unnecessary is collected.
    let started = std::time::Instant::now();
    loop {
        n += 1;
        let (r, took) = timed("/actor/t/x/plain").await;
        assert_eq!((r.status, r.body.trim()), (200, n.to_string().as_str()));
        assert!(
            took < bound,
            "write {n} as the snapshot lands took {took:?}"
        );
        let (snapshots, segments) = (snapshot_keys(&p).await, history(&p).await.1);
        if snapshots.iter().any(|k| !before.contains(k)) && segments < 64 {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{snapshots:?}, {segments} segments"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Durable, and with it every plain write before it.
    let (r, _) = timed("/actor/t/x/durable").await;
    assert_eq!((r.status, r.body.trim()), (200, "2"), "{}", r.body);
    let r = b_answers(&p, "/actor/t/x/plain").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, n.to_string().as_str(), Some("b")),
        "{}",
        r.body
    );
    let r = poll_get(p.b.addr, "/actor/t/x/durable").await;
    assert_eq!((r.status, r.body.trim()), (200, "2"), "{}", r.body);
}

/// A delete waits for a periodic snapshot still uploading before its
/// reset, so the snapshot (of the data the delete erases) cannot land after
/// the erasure: once the erasure ran, no snapshot below the reset is left,
/// and the next owner finds the actor empty (ADR 0034, section 7).
#[tokio::test(flavor = "multi_thread")]
async fn a_delete_waits_for_a_periodic_snapshot_uploading() {
    let p = batched_pair(Duration::from_secs(30), |c| {
        if let Some(cluster) = c.cluster.as_mut() {
            cluster.history_retention = Duration::ZERO;
        }
    })
    .await;
    warmed(&p).await;
    p.flaky.hold_snapshot_puts.store(true, Ordering::SeqCst);
    let mut n = 1;
    while p.flaky.snapshot_puts_held.load(Ordering::SeqCst) == 0 {
        assert!(n < 100, "no periodic snapshot after {n} writes");
        n += 1;
        let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
        assert_eq!((r.status, r.body.trim()), (200, n.to_string().as_str()));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let gate = Arc::clone(&p.flaky.hold_snapshot_puts);
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        gate.store(false, Ordering::SeqCst);
    });
    let started = std::time::Instant::now();
    let r = send(p.a.addr, "POST", "/actor/t/x/delete-durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "deleted"), "{}", r.body);
    assert!(
        started.elapsed() >= Duration::from_millis(900),
        "the reset did not wait for the snapshot: {:?}",
        started.elapsed()
    );
    release.await.unwrap();
    // The erasure leaves no snapshot below the newest one.
    let txid = |k: &String| {
        let name = k.rsplit('/').next().unwrap();
        u64::from_str_radix(&name[..16], 16).unwrap()
    };
    let started = std::time::Instant::now();
    loop {
        let keys = snapshot_keys(&p).await;
        let newest = keys.iter().map(txid).max().unwrap();
        if keys.iter().all(|k| txid(k) == newest) {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "left after the erasure: {keys:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let r = b_answers(&p, "/actor/t/x/plain").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "0", Some("b")),
        "{}",
        r.body
    );
}

/// A periodic snapshot's upload that outlives its hold on this node (the
/// replica failed and the actor was given up) still holds back a delete of
/// the actor under its next hold: the delete waits for it, so it cannot
/// land after the erasure and leave the deleted data in the bucket (ADR
/// 0034, section 7).
#[tokio::test(flavor = "multi_thread")]
async fn a_delete_after_a_given_up_hold_waits_for_its_snapshot_upload() {
    let p = batched_pair(Duration::from_secs(30), |c| {
        if let Some(cluster) = c.cluster.as_mut() {
            cluster.history_retention = Duration::ZERO;
        }
    })
    .await;
    warmed(&p).await;
    p.flaky.hold_snapshot_puts.store(true, Ordering::SeqCst);
    let mut n = 1;
    while p.flaky.snapshot_puts_held.load(Ordering::SeqCst) == 0 {
        assert!(n < 100, "no periodic snapshot after {n} writes");
        n += 1;
        let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
        assert_eq!((r.status, r.body.trim()), (200, n.to_string().as_str()));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The next segment fails: the actor is given up, its upload held.
    p.flaky.fail_wal.store(3, Ordering::SeqCst);
    let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    eventually("every attempt used", async || {
        p.flaky.fail_wal.load(Ordering::SeqCst) == 0
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Restored under a new hold, then deleted, and its history erased.
    let r = send(p.a.addr, "POST", "/actor/t/x/delete-durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "deleted"), "{}", r.body);
    let txid = |k: &String| {
        let name = k.rsplit('/').next().unwrap();
        u64::from_str_radix(&name[..16], 16).unwrap()
    };
    let erased = async || {
        let keys = snapshot_keys(&p).await;
        let newest = keys.iter().map(txid).max().unwrap();
        (keys.iter().all(|k| txid(k) == newest), keys)
    };
    let started = std::time::Instant::now();
    while !erased().await.0 {
        assert!(started.elapsed() < Duration::from_secs(15), "never erased");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // The upload the given-up hold left, released now: had the delete not
    // waited for it, it lands below the erasure.
    p.flaky.hold_snapshot_puts.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let (only_newest, keys) = erased().await;
    assert!(only_newest, "a snapshot below the delete is left: {keys:?}");
    let r = b_answers(&p, "/actor/t/x/plain").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "0", Some("b")),
        "{}",
        r.body
    );
}

/// A delete at a batched hold's barrier retires the entry its state named:
/// the reset is acknowledged, nothing is in flight, so no state can name
/// it again (ADR 0034, section 9; ADR 0028).
#[tokio::test(flavor = "multi_thread")]
async fn a_delete_retires_the_alarm_entry_it_ends() {
    let p = batched_pair(Duration::from_secs(30), |_| {}).await;
    warmed(&p).await;
    let due = now_ms() + 600_000;
    let r = send(p.a.addr, "POST", &format!("/actor/t/x/alarm?at={due}"), "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    let shared: Arc<dyn ObjectStore> = Arc::clone(&p.flaky.inner);
    eventually("the entry is listed", async || {
        due_at(&bucket_keys(&shared).await, due) == 1
    })
    .await;
    let r = send(p.a.addr, "POST", "/actor/t/x/delete", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "ok"), "{}", r.body);
    eventually("the deleted actor's entry is retired", async || {
        due_at(&bucket_keys(&shared).await, due) == 0
    })
    .await;
}

/// A clean stop with a healthy bucket flushes every batched actor before
/// it releases: nothing is given up, and the next owner has every write
/// (ADR 0034, section 10).
#[tokio::test(flavor = "multi_thread")]
async fn a_clean_stop_flushes_and_loses_nothing() {
    let p = batched_pair(Duration::from_secs(30), |c| {
        c.max_unsent_age = Duration::from_secs(5);
    })
    .await;
    warmed(&p).await;
    p.flaky.delay_wal_ms.store(400, Ordering::SeqCst);
    for _ in 0..3 {
        let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
        assert_eq!(r.status, 200, "{}", r.body);
    }
    p.a.service.shutdown(Duration::from_secs(10)).await;
    assert_eq!(wasmkeep_server::metrics::plain_events_dropped_total(), 0);
    p.flaky.delay_wal_ms.store(0, Ordering::SeqCst);
    let r = poll_get(p.b.addr, "/actor/t/x/plain").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "4", Some("b")),
        "{}",
        r.body
    );
}

/// A clean stop under a slow bucket gives up at its deadline: exactly the
/// events not acknowledged (the segment in flight and the one behind it)
/// are counted, as `stop`, the hold released, and the stop ends soon
/// after its limit (ADR 0034, section 10).
#[tokio::test(flavor = "multi_thread")]
async fn a_clean_stop_behind_a_slow_bucket_gives_up_at_its_deadline() {
    let p = batched_pair(Duration::from_secs(30), |c| {
        c.max_unsent_age = Duration::from_secs(5);
    })
    .await;
    warmed(&p).await;
    p.flaky.delay_wal_ms.store(6_000, Ordering::SeqCst);
    for _ in 0..2 {
        let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
        assert_eq!(r.status, 200, "{}", r.body);
    }
    let started = std::time::Instant::now();
    p.a.service.shutdown(Duration::from_secs(1)).await;
    let took = started.elapsed();
    assert!(took < Duration::from_secs(3), "the stop took {took:?}");
    assert_eq!(wasmkeep_server::metrics::plain_events_dropped_by("stop"), 2);
    assert_eq!(wasmkeep_server::metrics::plain_events_dropped_total(), 2);
}

/// A clean stop whose deadline comes while a batched actor runs an event
/// cuts that event at the deadline, and counts what the actor had not
/// acknowledged, as `stop`, before the node lets its actors go: an event
/// in progress does not outlast the stop (ADR 0034, section 10).
#[tokio::test(flavor = "multi_thread")]
async fn a_clean_stop_cuts_a_running_batched_event_at_its_deadline() {
    let p = batched_pair(Duration::from_secs(30), |c| {
        c.max_unsent_age = Duration::from_secs(5);
    })
    .await;
    warmed(&p).await;
    p.flaky.delay_wal_ms.store(6_000, Ordering::SeqCst);
    for _ in 0..2 {
        let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
        assert_eq!(r.status, 200, "{}", r.body);
    }
    let addr = p.a.addr;
    let running =
        tokio::spawn(async move { send(addr, "POST", "/actor/t/x/sleep?ms=8000", "").await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let started = std::time::Instant::now();
    p.a.service.shutdown(Duration::from_secs(1)).await;
    let took = started.elapsed();
    assert!(took < Duration::from_secs(3), "the stop took {took:?}");
    assert_eq!(
        wasmkeep_server::metrics::plain_events_dropped_by("stop"),
        2,
        "counted before the stop returned"
    );
    running.abort();
}

/// An actor acquired again after a hibernation keeps counting toward a
/// snapshot by bytes: two events of 36 MiB, one before the hibernation and
/// one after, make it due, though only a few segments were uploaded (ADR
/// 0034, section 7).
#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_by_bytes_is_due_across_a_hibernation() {
    let p = batched_pair(Duration::from_secs(30), |c| {
        c.mailbox.idle_timeout = Duration::from_millis(500);
    })
    .await;
    warmed(&p).await;
    let r = send(p.a.addr, "POST", "/actor/t/x/big?kb=36864&random", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "big"), "{}", r.body);
    eventually("hibernated", async || p.a.service.resident() == 0).await;
    let (before, _) = history(&p).await;
    let r = send(p.a.addr, "POST", "/actor/t/x/big?kb=36864&random", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "big"), "{}", r.body);
    eventually("a snapshot due by bytes", async || {
        history(&p).await.0 > before
    })
    .await;
}

/// A caller of `actors.call` into a batched actor held by the event-start
/// rule gets its answer when the callee sets it, as an HTTP client does:
/// the wait is after the answer, in the callee's job. The callee's next
/// event, a second call, starts only after that wait (ADR 0034, section 3).
#[tokio::test(flavor = "multi_thread")]
async fn a_held_callee_answers_its_caller_and_holds_its_next_event() {
    let p = batched_pair(Duration::from_secs(30), |c| {
        c.max_unsent_age = Duration::from_millis(100);
    })
    .await;
    warmed(&p).await;
    let r = send(p.a.addr, "POST", "/actor/t/y/plain", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    tokio::time::sleep(Duration::from_millis(500)).await;
    p.flaky.delay_wal_ms.store(2_000, Ordering::SeqCst);
    // y: a segment in flight, then a write behind it that ages.
    for _ in 0..2 {
        let r = send(p.a.addr, "POST", "/actor/t/y/plain", "").await;
        assert_eq!(r.status, 200, "{}", r.body);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let call = "/actor/t/x/call?to=t/y&path=%2Fplain";
    let started = std::time::Instant::now();
    let r = send(p.a.addr, "POST", call, "").await;
    assert_eq!((r.status, r.body.trim()), (200, "200:4"), "{}", r.body);
    assert!(
        started.elapsed() < Duration::from_millis(800),
        "the answer waited for the callee's wait: {:?}",
        started.elapsed()
    );
    let started = std::time::Instant::now();
    let r = send(p.a.addr, "POST", call, "").await;
    assert_eq!((r.status, r.body.trim()), (200, "200:5"), "{}", r.body);
    assert!(
        started.elapsed() >= Duration::from_millis(1_000),
        "the callee's next event started before its wait: {:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timing_start_rule_wait_with_held_upload() {
    let p = batched_pair(Duration::from_secs(30), |c| {
        c.max_unsent_age = Duration::from_millis(100);
    })
    .await;
    warmed(&p).await;
    p.flaky.hold_wal_puts.send_replace(true);
    assert_eq!(
        send(p.a.addr, "POST", "/actor/t/x/plain", "").await.status,
        200
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while p.flaky.held_wal_puts.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        send(p.a.addr, "POST", "/actor/t/x/plain", "").await.status,
        200
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    // This event answers, then its boundary finds the old uncaptured write.
    assert_eq!(
        send(p.a.addr, "GET", "/actor/t/x/plain", "").await.status,
        200
    );
    let next = send(p.a.addr, "GET", "/actor/t/x/plain", "");
    tokio::pin!(next);
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut next)
            .await
            .is_err()
    );
    p.flaky.hold_wal_puts.send_replace(false);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), next)
            .await
            .unwrap()
            .status,
        200
    );
    let page = wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default());
    let series = "wasmkeep_event_start_rule_wait_seconds_sum{tenant=\"default\",app=\"default\",class=\"t\"} ";
    let sum: f64 = page
        .lines()
        .find_map(|line| line.strip_prefix(series))
        .unwrap_or_else(|| panic!("missing start-rule timing: {page}"))
        .parse()
        .unwrap();
    assert!(sum >= 0.1, "held upload wait: {sum}");
}

/// A batched hold whose node lease is fenced while a write is still
/// going up counts that write as `lost_lease`, not as a lapse (ADR 0034,
/// sections 6 and 10).
#[tokio::test(flavor = "multi_thread")]
async fn a_fenced_node_lease_counts_its_pending_writes_as_lost_lease() {
    let p = batched_pair(Duration::from_secs(30), |_| {}).await;
    warmed(&p).await;
    p.flaky.hold_wal_puts.send_replace(true);
    let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "2"), "{}", r.body);
    eventually("the segment is held", async || {
        p.flaky.held_wal_puts.load(Ordering::SeqCst) > 0
    })
    .await;

    // As `wasmkeep cluster fence a` does.
    let store: Arc<dyn ObjectStore> = p.flaky.clone();
    let bucket = wasmkeep_cluster::object_store::ObjectStoreBucket::new(store, "");
    wasmkeep_cluster::node_lease::operator_fence(&bucket, &WallClock, "nodes/a")
        .await
        .unwrap();
    let why = tokio::time::timeout(Duration::from_secs(5), p.a.service.node_id_lost())
        .await
        .expect("the fence was never noticed");
    assert!(why.contains("fenced"), "{why}");

    // The next event finds the hold ended and drops it.
    let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
    assert_ne!(r.status, 200, "still served after the fence: {}", r.body);
    p.flaky.hold_wal_puts.send_replace(false);
    eventually("the drop is counted", async || {
        !dropped_by_reason().is_empty()
    })
    .await;
    let dropped = dropped_by_reason();
    assert!(
        dropped.iter().all(|(why, _)| *why == "lost_lease"),
        "{dropped:?}"
    );
}
