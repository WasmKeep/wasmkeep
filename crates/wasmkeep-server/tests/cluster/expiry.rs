//! Idle expiry (ADR 0028, section 4): the collecting node (the lowest live
//! id) deletes an actor idle past its class's limit, and keeps one in use,
//! one with a pending alarm, and one woken while the expiry was under way.
//! A parked room with an open socket: `websockets.rs`.

use super::deletes::{read, write};
use super::*;

/// How often the collecting node runs a pass in these tests.
const PASS: Duration = Duration::from_millis(300);

/// A node serving the test actor as class `t`, its actors expiring after
/// `limit` idle, hibernating after `idle`.
async fn expiring(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    idle: Duration,
    limit: Duration,
) -> Node {
    node_configured(
        id,
        data,
        store,
        idle,
        &["t"],
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        move |config| {
            config.expire_after_idle.insert("t".into(), limit);
            config.cluster.as_mut().unwrap().expiry_every = PASS;
        },
    )
    .await
}

/// Whether actor `t/<name>`'s history is down to a delete: the reset and
/// the snapshot at it (an expiry erases the rest).
async fn deleted(store: &Arc<dyn ObjectStore>, name: &str) -> bool {
    actor_keys(store, name).await.len() == 2
}

#[tokio::test(flavor = "multi_thread")]
async fn an_idle_actor_is_deleted_and_a_busy_one_kept() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let idle = Duration::from_millis(200);
    let limit = Duration::from_secs(2);
    let a = expiring("a", dirs[0].path(), &store, idle, limit).await;
    let b = expiring("b", dirs[1].path(), &store, idle, limit).await;
    settle().await;
    write(b.addr, "idle", 3).await;
    // Woken every 400 ms, hibernating in between: its record is rewritten
    // at each wake and release, never idle for 2 s.
    let started = std::time::Instant::now();
    let mut busy = 0;
    while started.elapsed() < Duration::from_secs(6) {
        busy += 1;
        let r = send(b.addr, "POST", "/actor/t/busy/durable", "").await;
        assert_eq!(
            (r.status, r.body.trim()),
            (200, busy.to_string().as_str()),
            "the busy actor lost its data"
        );
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    eventually("the idle actor expired", async || {
        deleted(&store, "idle").await
    })
    .await;
    assert_eq!(
        read(a.addr, "idle").await,
        "0",
        "the expired data came back"
    );
    assert_eq!(read(b.addr, "busy").await, busy.to_string());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_actor_with_a_pending_alarm_is_kept() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let idle = Duration::from_millis(200);
    let limit = Duration::from_secs(1);
    let a = expiring("a", dirs[0].path(), &store, idle, limit).await;
    let b = expiring("b", dirs[1].path(), &store, idle, limit).await;
    settle().await;
    write(b.addr, "alarmed", 2).await;
    let r = send(b.addr, "POST", "/actor/t/alarmed/alarm?in=60000", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    write(b.addr, "plain", 1).await;
    // Many passes past the limit: the actor without an alarm goes, the
    // one with an alarm stays.
    eventually("the actor without an alarm expired", async || {
        deleted(&store, "plain").await
    })
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        !deleted(&store, "alarmed").await,
        "an actor with an alarm expired"
    );
    assert_eq!(read(a.addr, "alarmed").await, "2");
    let r = send(a.addr, "GET", "/actor/t/alarmed/alarm", "").await;
    assert_ne!(r.body.trim(), "none", "the alarm was lost");
}

/// A wake between the expiry's listing and its acquisition rewrites the
/// record: the acquisition finds it changed and nothing is deleted. Once
/// idle past the limit again, the actor goes.
#[tokio::test(flavor = "multi_thread")]
async fn a_wake_during_the_expiry_wins() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let seen_by_a: Arc<dyn ObjectStore> = Arc::clone(&flaky) as Arc<dyn ObjectStore>;
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let idle = Duration::from_millis(200);
    let limit = Duration::from_secs(4);
    // a collects (the lowest id); its listings of `owners/` are handed back
    // 2 s after they were taken.
    flaky.delay_owner_lists_ms.store(2_000, Ordering::SeqCst);
    let _a = expiring("a", dirs[0].path(), &seen_by_a, idle, limit).await;
    let b = expiring("b", dirs[1].path(), &shared, idle, limit).await;
    settle().await;
    write(b.addr, "w", 2).await;
    eventually("hibernated", async || b.service.resident() == 0).await;
    // The record's last write, by the bucket's clock (this machine's): a
    // listing taken more than the limit after it is the first to make the
    // actor a candidate; none before can.
    let released = {
        use object_store::ObjectStoreExt as _;
        let key = object_store::path::Path::from(test_key(Family::Owners, "t/w"));
        let meta = shared.head(&key).await.unwrap();
        u64::try_from(meta.last_modified.timestamp_millis()).unwrap()
    };
    let limit_ms = u64::try_from(limit.as_millis()).unwrap();
    eventually("a listing that makes it a candidate", async || {
        flaky
            .owner_lists_taken
            .lock()
            .unwrap()
            .iter()
            .any(|t| *t > released + limit_ms)
    })
    .await;
    let done = flaky.owner_lists_done.load(Ordering::SeqCst);
    // The wake, while the listing is on its way to the expiry.
    let r = send(b.addr, "POST", "/actor/t/w/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "3"));
    eventually("the listing handed back", async || {
        flaky.owner_lists_done.load(Ordering::SeqCst) > done
    })
    .await;
    // The expiry's job ran on what it listed: nothing deleted.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!deleted(&shared, "w").await, "a woken actor expired");
    assert_eq!(read(b.addr, "w").await, "3", "the wake was lost");
    // Idle past the limit again, listed afresh: it goes.
    flaky.delay_owner_lists_ms.store(0, Ordering::SeqCst);
    let started = std::time::Instant::now();
    while !deleted(&shared, "w").await {
        assert!(
            started.elapsed() < limit + Duration::from_secs(10),
            "the actor never expired once idle"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
