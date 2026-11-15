//! `storage.flush` for a strict class (ADR 0034, section 4): this event's
//! writes so far, shipped in one upload, or nothing at all when there are
//! none; refused inside a transaction; after a failed upload, an error at
//! once.

use super::*;

/// The actor's history segments and alarm index entries in the bucket,
/// once the upload at the end of the last event (after its answer) landed.
async fn counted(p: &Pair) -> (usize, usize) {
    tokio::time::sleep(Duration::from_millis(500)).await;
    let store: Arc<dyn ObjectStore> = p.flaky.clone();
    let keys = bucket_keys(&store).await;
    let wal = keys.iter().filter(|k| k.contains("/wal/")).count();
    let alarms = keys.iter().filter(|k| k.contains("alarms/")).count();
    (wal, alarms)
}

/// A plain write, and its upload after the answer landed.
async fn warmed(p: &Pair) {
    let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"), "{}", r.body);
    tokio::time::sleep(Duration::from_millis(500)).await;
}

/// a cut off from the bucket; b restores the actor and answers `path`.
async fn b_answers(p: &Pair, path: &str) -> Reply {
    tokio::time::sleep(Duration::from_millis(600)).await;
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

#[tokio::test(flavor = "multi_thread")]
async fn a_flush_ships_the_events_writes_in_one_upload_or_none() {
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    warmed(&p).await;
    let before = counted(&p).await;

    // Three plain writes, the flush, one more write: the flush's segment,
    // then the event end's for the write after it. Without the flush the
    // end would upload all four in one.
    let r = send(p.a.addr, "POST", "/actor/t/x/flush?n=3&after=1", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "ok:4"), "{}", r.body);
    assert_eq!(counted(&p).await, (before.0 + 2, before.1), "two uploads");
    // Nothing after it: the flush's one upload, nothing at the end.
    let r = send(p.a.addr, "POST", "/actor/t/x/flush?n=1", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "ok:6"), "{}", r.body);
    assert_eq!(counted(&p).await, (before.0 + 3, before.1), "one upload");

    // Nothing written since: no request at all.
    let keys = bucket_keys(&(p.flaky.clone() as Arc<dyn ObjectStore>)).await;
    let r = send(p.a.addr, "POST", "/actor/t/x/flush", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "ok:6"), "{}", r.body);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        bucket_keys(&(p.flaky.clone() as Arc<dyn ObjectStore>)).await,
        keys,
        "an empty flush writes nothing"
    );

    // A second flush in the same event finds nothing pending.
    let r = send(p.a.addr, "POST", "/actor/t/x/flush?n=1&twice", "").await;
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "ok:7 | ok:7"),
        "{}",
        r.body
    );
    assert_eq!(counted(&p).await, (before.0 + 4, before.1));

    let r = b_answers(&p, "/actor/t/x/plain").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "7", Some("b")),
        "{}",
        r.body
    );
}

/// A pending alarm goes into the bucket's index with the flush that
/// carries it, as with `exec-durable`.
#[tokio::test(flavor = "multi_thread")]
async fn a_flush_lists_a_pending_alarm_first() {
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    warmed(&p).await;
    let before = counted(&p).await;
    // The alarm, the flush, a write after it: the flush's segment carries
    // the alarm (its entry written first), the end's the write.
    let r = send(
        p.a.addr,
        "POST",
        "/actor/t/x/flush?alarm=3600000&after=1",
        "",
    )
    .await;
    assert_eq!((r.status, r.body.trim()), (200, "ok:1"), "{}", r.body);
    assert_eq!(counted(&p).await, (before.0 + 2, before.1 + 1));
    let r = b_answers(&p, "/actor/t/x/alarm").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "{}",
        r.body
    );
    assert_ne!(r.body.trim(), "none", "the alarm is in the bucket");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_flush_inside_a_transaction_is_refused() {
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    let r = send(p.a.addr, "POST", "/actor/t/x/flush?n=1&tx", "").await;
    assert!(
        r.body.trim().starts_with("err:SqlErrorKind::NotAllowed:"),
        "{}",
        r.body
    );
    let r = send(p.a.addr, "POST", "/actor/t/x/plain", "").await;
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "2"),
        "the actor carries on"
    );
}

/// A flush whose upload fails answers `internal` starting `replication
/// stopped`, and so does every later one of the event, at once; the
/// bucket keeps what was acknowledged.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_flush_stops_replication_and_says_so() {
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    warmed(&p).await;
    p.flaky.fail_wal.store(3, Ordering::SeqCst);
    let r = send(p.a.addr, "POST", "/actor/t/x/flush?n=1&twice", "").await;
    assert_eq!(
        p.flaky.fail_wal.load(Ordering::SeqCst),
        0,
        "every attempt used"
    );
    let answers: Vec<&str> = r.body.trim().split(" | ").collect();
    assert_eq!(answers.len(), 2, "{}", r.body);
    for answer in answers {
        assert!(
            answer.starts_with("err:SqlErrorKind::Internal:replication stopped"),
            "{}",
            r.body
        );
        assert!(!answer.contains("still uploading"), "{}", r.body);
    }
    let r = b_answers(&p, "/actor/t/x/plain").await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "1", Some("b")),
        "exactly the acknowledged write: {}",
        r.body
    );
}
