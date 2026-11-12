//! A periodic snapshot whose upload hangs or fails: it holds the actor
//! for one short attempt, never fails the event, loses nothing, and is
//! tried again later. The snapshot after a delete is counted apart.

use super::*;

/// The value of counter `name` on the node's metrics page.
pub(super) async fn counter(name: &str) -> u64 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let metrics = listener.local_addr().unwrap();
    tokio::spawn(wasmkeep_server::serve_metrics(
        listener,
        None,
        std::future::pending(),
    ));
    let (status, page) = get_raw(metrics, "/metrics").await;
    assert_eq!(status, 200);
    page.lines()
        .find_map(|l| l.strip_prefix(name)?.strip_prefix(' '))
        .unwrap_or_else(|| panic!("no {name} in\n{page}"))
        .trim()
        .parse()
        .unwrap()
}

/// How many snapshot uploads the node counts as not acknowledged.
async fn snapshot_upload_failures() -> u64 {
    counter("wasmkeep_snapshot_upload_failures_total").await
}

/// Far above anything these tests wait for: no actor hibernates in them
/// unless the test says so, so a second snapshot comes from the rule
/// under test and never from a restore.
const NO_HIBERNATION: Duration = Duration::from_secs(600);

/// [`pair`], with an idle timeout of `idle` and a failed periodic
/// snapshot's minimum wait before the next try set to `retry_min` (30 s
/// on a real node), so these tests take seconds.
async fn pair_waiting(idle: Duration, retry_min: Duration) -> Pair {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let seen_by_a: Arc<dyn ObjectStore> = Arc::clone(&flaky) as Arc<dyn ObjectStore>;
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let wall = Duration::from_secs(30);
    let adjust = move |config: &mut ActorConfig| {
        config.cluster.as_mut().unwrap().snapshot_retry_min = retry_min;
    };
    let a = node_configured(
        "a",
        dirs.0.path(),
        &seen_by_a,
        idle,
        &["t"],
        test_actor_wasm(),
        wall,
        "",
        adjust,
    )
    .await;
    let b = node_configured(
        "b",
        dirs.1.path(),
        &shared,
        idle,
        &["t"],
        test_actor_wasm(),
        wall,
        "",
        adjust,
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

/// One durable write to `t/<name>` on `p.a`, answered within 15 s with
/// the count `expected`.
async fn write(p: &Pair, name: &str, expected: u64) {
    let started = std::time::Instant::now();
    let path = format!("/actor/t/{name}/durable");
    let r = tokio::time::timeout(Duration::from_secs(15), send(p.a.addr, "POST", &path, ""))
        .await
        .unwrap_or_else(|_| panic!("write {expected} was not answered within 15 s"));
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, expected.to_string().as_str(), Some("a")),
        "write {expected} after {:?}: {}",
        started.elapsed(),
        r.body
    );
}

/// Writes until the periodic snapshot's upload has started meeting
/// `fault` (armed by the caller, after the first write: the first
/// snapshot is one too). Returns how many writes were acknowledged, and
/// a time no later than the start of that upload: an actor runs one
/// event at a time and takes the snapshot before the next, so it was due
/// after the last write or the one before it, and the time is the start
/// of the write before the last (one write early at most).
async fn write_until_met(
    p: &Pair,
    name: &str,
    fault: &std::sync::atomic::AtomicU32,
) -> (u64, std::time::Instant) {
    let armed = fault.load(Ordering::SeqCst);
    let mut written = 1;
    let mut starts = [std::time::Instant::now(); 2];
    while fault.load(Ordering::SeqCst) == armed {
        assert!(written < 80, "no periodic snapshot after {written} writes");
        written += 1;
        starts = [starts[1], std::time::Instant::now()];
        write(p, name, written).await;
    }
    (written, starts[0])
}

/// The snapshots in `t/<name>`'s prefix, by transaction.
async fn snapshots_of(p: &Pair, name: &str) -> Vec<u64> {
    let mut snapshots = history_of(&p.flaky.inner, name).await.0;
    snapshots.sort_unstable();
    snapshots
}

/// When the bucket leaves every snapshot upload hanging, a periodic
/// snapshot holds the actor for one short attempt (6 s for a small
/// database), and the events after it are not held at all: it used to be
/// sent again for 21 s, and tried again by the very next event.
#[tokio::test(flavor = "multi_thread")]
async fn a_hung_periodic_snapshot_holds_the_actor_for_one_short_attempt() {
    let p = pair(Duration::from_secs(30), NO_HIBERNATION).await;
    write(&p, "hung", 1).await;
    assert_eq!(snapshots_of(&p, "hung").await.len(), 1, "the first one");
    let failures = snapshot_upload_failures().await;
    let hung = &p.flaky.hang_snapshot_puts;
    hung.store(u32::MAX, Ordering::SeqCst);
    // The clock starts at the write whose snapshot meets the fault (one
    // write earlier at most), not at the first of the writes before it.
    let (written, started) = write_until_met(&p, "hung", hung).await;
    // The write the snapshot was due after, and the one behind it.
    write(&p, "hung", written + 1).await;
    let held = started.elapsed();
    // One attempt of 6 s and two writes: well under the 21 s it used to
    // be sent again for (which no write here waits out: 15 s each), with
    // room for a machine that takes seconds over the writes.
    assert!(held < Duration::from_secs(15), "held for {held:?}");
    // No event tries again at once, write or read: one that did would be
    // held for the 6 s of an attempt.
    let started = std::time::Instant::now();
    for i in 2..=4 {
        write(&p, "hung", written + i).await;
    }
    let r = send(p.a.addr, "GET", "/actor/t/hung/durable", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    let free = started.elapsed();
    assert!(free < Duration::from_secs(5), "held again: {free:?}");
    assert_eq!(
        hung.load(Ordering::SeqCst),
        u32::MAX - 1,
        "one attempt, not sent again and not tried again at once"
    );
    assert_eq!(snapshot_upload_failures().await - failures, 1);
    assert_eq!(snapshots_of(&p, "hung").await.len(), 1);
}

/// A periodic snapshot whose upload fails fails no event and costs
/// nothing acknowledged; it is counted, and tried again once the actor
/// has written again and the minimum wait has passed: not by the events
/// right after it, though they write, and not only 64 writes later. The
/// count of segments since the last snapshot is not reset by a failure:
/// it used to be, and the next snapshot came only after as many again.
/// The actor never hibernates here, so nothing but that rule takes the
/// second snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_periodic_snapshot_is_tried_again_later() {
    // Long next to the five writes and 300 ms that must fit inside it
    // (a third of a second on an idle machine).
    const WAIT: Duration = Duration::from_secs(10);
    let p = pair_waiting(NO_HIBERNATION, WAIT).await;
    write(&p, "x", 1).await;
    let failures = snapshot_upload_failures().await;
    // One upload fails; a try after it would succeed, and show.
    p.flaky.fail_snapshot_puts.store(1, Ordering::SeqCst);
    let (mut written, not_before) = write_until_met(&p, "x", &p.flaky.fail_snapshot_puts).await;
    // The wait runs from the start of the try, which has started.
    let not_after = std::time::Instant::now();
    // More writes: whichever write the snapshot failed after, events
    // have ended since and a segment was uploaded, so only the minimum
    // wait holds the next try back.
    for _ in 0..3 {
        written += 1;
        write(&p, "x", written).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let snapshots = snapshots_of(&p, "x").await.len();
    assert!(
        not_before.elapsed() < WAIT,
        "too slow to tell: the wait had passed before the check"
    );
    assert_eq!(snapshots, 1, "tried again before the minimum wait");
    assert_eq!(
        snapshot_upload_failures().await - failures,
        1,
        "one upload failed"
    );
    // Once the wait has passed, the next write is followed by the try:
    // one write, where an untried snapshot would wait for 64.
    tokio::time::sleep((WAIT + Duration::from_millis(200)).saturating_sub(not_after.elapsed()))
        .await;
    assert_eq!(snapshots_of(&p, "x").await.len(), 1, "no event, no try");
    written += 1;
    write(&p, "x", written).await;
    eventually("the snapshot is tried again", async || {
        snapshots_of(&p, "x").await.len() == 2
    })
    .await;
    let snapshots = snapshots_of(&p, "x").await;
    let mut segments = history_of(&p.flaky.inner, "x").await.1;
    segments.sort_unstable();
    assert_eq!(
        snapshots.last(),
        segments.last(),
        "at the last write: {snapshots:?}, {segments:?}"
    );
    // The count starts again from that snapshot: no third one soon.
    for _ in 0..10 {
        written += 1;
        write(&p, "x", written).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(snapshots_of(&p, "x").await.len(), 2);
    assert_eq!(snapshot_upload_failures().await - failures, 1);
    let r = b_restores(&p).await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, written.to_string().as_str(), Some("b")),
        "{}",
        r.body
    );
}

/// Waits until `p.a` holds no actor in memory, then for the release
/// that follows.
async fn hibernated(p: &Pair) {
    eventually("the actor hibernates", async || p.a.service.resident() == 0).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
}

/// A failed periodic snapshot's wait outlives the actor's time in memory.
/// The node drops an idle actor and restores it at its next event, with
/// every segment since the last snapshot counted again: that restore used
/// to forget the failure, so with a bucket failing snapshot uploads each
/// wake of a long chain paid a try. A wake inside the wait tries nothing;
/// one after it does; and the segments the try after that waits for (8)
/// are still waited for after another hibernation.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_snapshots_wait_outlives_hibernation() {
    // Long next to what must fit inside it: a write, the second of idle
    // time before the actor is dropped, and a wake (1.6 s on an idle
    // machine, most of it those fixed waits).
    const WAIT: Duration = Duration::from_secs(16);
    let p = pair_waiting(Duration::from_secs(1), WAIT).await;
    let read = async || {
        let r = send(p.a.addr, "GET", "/actor/t/x/durable", "").await;
        assert_eq!(
            (r.status, r.node.as_deref()),
            (200, Some("a")),
            "{}",
            r.body
        );
        // The snapshot, if one is due, is taken once the event has ended.
        tokio::time::sleep(Duration::from_millis(300)).await;
    };
    write(&p, "x", 1).await;
    // Every snapshot upload fails: each try shows as one fault used.
    let fault = &p.flaky.fail_snapshot_puts;
    fault.store(u32::MAX, Ordering::SeqCst);
    let tries = || u32::MAX - fault.load(Ordering::SeqCst);
    let (mut written, not_before) = write_until_met(&p, "x", fault).await;
    let not_after = std::time::Instant::now();
    // One more write: the segment the next try waits for is there, so
    // only the minimum wait holds it back.
    written += 1;
    write(&p, "x", written).await;
    assert_eq!(tries(), 1);

    hibernated(&p).await;
    read().await;
    assert!(
        not_before.elapsed() < WAIT,
        "too slow to tell: the wait had passed before the wake"
    );
    assert_eq!(tries(), 1, "a wake inside the wait tried again");

    hibernated(&p).await;
    tokio::time::sleep((WAIT + Duration::from_millis(200)).saturating_sub(not_after.elapsed()))
        .await;
    assert_eq!(tries(), 1, "no event, no try");
    read().await;
    eventually("a wake after the wait tries again", async || tries() == 2).await;
    // The second wait runs from the start of that try: no later than now.
    let second = std::time::Instant::now();

    // The third try waits for 8 more segments, however long the actor
    // sleeps and however often it wakes meanwhile: its minimum time has
    // passed before the next wake, so only the segments hold it back.
    hibernated(&p).await;
    tokio::time::sleep((WAIT + Duration::from_millis(500)).saturating_sub(second.elapsed())).await;
    read().await;
    assert_eq!(tries(), 2, "tried again before 8 more segments");
    for _ in 0..7 {
        written += 1;
        write(&p, "x", written).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(tries(), 2, "tried again after 7 segments");
    written += 1;
    write(&p, "x", written).await;
    eventually("the eighth segment is followed by a try", async || {
        tries() == 3
    })
    .await;
    assert_eq!(snapshots_of(&p, "x").await.len(), 1, "every try failed");
}

/// A periodic snapshot's upload that was given up may still land: the
/// counter says "not acknowledged", not "not there". Nothing is lost by
/// it, the actor keeps answering, a later snapshot succeeds, and another
/// node restores every acknowledged write.
#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_upload_given_up_that_lands_later_loses_nothing() {
    // Much longer than the 8 s the upload takes to land, so the late
    // snapshot is there, and three more writes are answered, well before
    // the next try (12 s to spare); the actor never hibernates.
    const WAIT: Duration = Duration::from_secs(20);
    let p = pair_waiting(NO_HIBERNATION, WAIT).await;
    write(&p, "x", 1).await;
    let failures = snapshot_upload_failures().await;
    p.flaky.late_snapshot_puts.store(1, Ordering::SeqCst);
    let (mut written, not_before) = write_until_met(&p, "x", &p.flaky.late_snapshot_puts).await;
    // Answered once the attempt is given up (6 s), before it lands (8 s).
    written += 1;
    write(&p, "x", written).await;
    let given_up = std::time::Instant::now();
    assert_eq!(
        snapshot_upload_failures().await - failures,
        1,
        "not acknowledged"
    );
    eventually("the upload given up lands", async || {
        snapshots_of(&p, "x").await.len() == 2
    })
    .await;
    let late = snapshots_of(&p, "x").await[1];
    // The actor keeps answering, and the next periodic snapshot is taken
    // at a later position, the late one in the way of nothing.
    for _ in 0..3 {
        written += 1;
        write(&p, "x", written).await;
    }
    let snapshots = snapshots_of(&p, "x").await.len();
    assert!(
        not_before.elapsed() < WAIT,
        "too slow to tell: the wait had passed before the check"
    );
    assert_eq!(snapshots, 2, "not tried yet");
    // The try started before it was given up: the wait has passed by then.
    tokio::time::sleep(WAIT.saturating_sub(given_up.elapsed())).await;
    written += 1;
    write(&p, "x", written).await;
    eventually("a later snapshot succeeds", async || {
        snapshots_of(&p, "x").await.len() == 3
    })
    .await;
    assert!(snapshots_of(&p, "x").await[2] > late);
    assert_eq!(snapshot_upload_failures().await - failures, 1);
    let r = b_restores(&p).await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, written.to_string().as_str(), Some("b")),
        "{}",
        r.body
    );
}

/// The snapshot after a delete whose upload fails (every attempt: it is
/// sent again as a write's file is) is counted as a snapshot upload not
/// acknowledged and as a collection that failed; the delete itself is
/// recorded, and another node finds the actor empty.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_snapshot_after_a_delete_is_counted_twice() {
    let p = pair(Duration::from_secs(30), NO_HIBERNATION).await;
    write(&p, "x", 1).await;
    write(&p, "x", 2).await;
    let failures = snapshot_upload_failures().await;
    let collect_failures = counter("wasmkeep_history_collect_failures_total").await;
    p.flaky.fail_snapshot_puts.store(3, Ordering::SeqCst);
    let r = send(p.a.addr, "POST", "/actor/t/x/delete", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    // The delete is answered before its upload ends.
    eventually("every attempt of the upload failed", async || {
        p.flaky.fail_snapshot_puts.load(Ordering::SeqCst) == 0
    })
    .await;
    eventually("the failure is counted", async || {
        snapshot_upload_failures().await - failures == 1
    })
    .await;
    assert_eq!(
        counter("wasmkeep_history_collect_failures_total").await - collect_failures,
        1
    );
    assert_eq!(snapshots_of(&p, "x").await.len(), 1, "the first one only");
    let r = b_restores(&p).await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "0", Some("b")),
        "{}",
        r.body
    );
}
