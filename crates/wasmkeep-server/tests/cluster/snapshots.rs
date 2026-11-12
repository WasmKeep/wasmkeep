//! A periodic snapshot whose upload hangs or fails: it holds the actor
//! for one short attempt, never fails the event, loses nothing, and is
//! tried again later. The snapshot after a delete is counted apart.

use super::*;

/// The value of counter `name` on the node's metrics page.
async fn counter(name: &str) -> u64 {
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

/// A failed periodic snapshot is not tried again before this long after
/// it (the server's `SNAPSHOT_RETRY_MIN`, and a margin).
const RETRY_WAIT: Duration = Duration::from_secs(31);

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
/// snapshot is one too), and returns how many writes were acknowledged.
/// An actor runs one event at a time and takes the snapshot before the
/// next: it was due after the last write or the one before it.
async fn write_until_met(p: &Pair, name: &str, fault: &std::sync::atomic::AtomicU32) -> u64 {
    let armed = fault.load(Ordering::SeqCst);
    let mut written = 1;
    while fault.load(Ordering::SeqCst) == armed {
        assert!(written < 80, "no periodic snapshot after {written} writes");
        written += 1;
        write(p, name, written).await;
    }
    written
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
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    write(&p, "hung", 1).await;
    assert_eq!(snapshots_of(&p, "hung").await.len(), 1, "the first one");
    let hung = &p.flaky.hang_snapshot_puts;
    hung.store(u32::MAX, Ordering::SeqCst);
    let started = std::time::Instant::now();
    let written = write_until_met(&p, "hung", hung).await;
    // The write the snapshot was due after, and the one behind it.
    write(&p, "hung", written + 1).await;
    let held = started.elapsed();
    assert!(held < Duration::from_secs(10), "held for {held:?}");
    // No event tries again at once, write or read.
    let started = std::time::Instant::now();
    for i in 2..=4 {
        write(&p, "hung", written + i).await;
    }
    let r = send(p.a.addr, "GET", "/actor/t/hung/durable", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    let free = started.elapsed();
    assert!(free < Duration::from_secs(3), "held again: {free:?}");
    assert_eq!(
        hung.load(Ordering::SeqCst),
        u32::MAX - 1,
        "one attempt, not sent again and not tried again at once"
    );
    assert_eq!(snapshot_upload_failures().await, 1);
    assert_eq!(snapshots_of(&p, "hung").await.len(), 1);
}

/// A periodic snapshot whose upload fails fails no event and costs
/// nothing acknowledged; it is counted, and tried again once the actor
/// has written again and a while has passed, not by the next event. The
/// count of segments since the last snapshot is not reset by a failure:
/// it used to be, and the next snapshot came only after as many again.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_periodic_snapshot_is_tried_again_later() {
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    write(&p, "x", 1).await;
    assert_eq!(snapshot_upload_failures().await, 0);
    p.flaky.fail_snapshot_puts.store(1, Ordering::SeqCst);
    let mut written = write_until_met(&p, "x", &p.flaky.fail_snapshot_puts).await;
    let failed = std::time::Instant::now();
    // More writes: whichever write the snapshot failed after, events
    // have ended since, and a segment was uploaded.
    for _ in 0..3 {
        written += 1;
        write(&p, "x", written).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        snapshots_of(&p, "x").await.len(),
        1,
        "not sent again, and not tried again at once"
    );
    assert_eq!(snapshot_upload_failures().await, 1, "one upload failed");
    tokio::time::sleep(RETRY_WAIT.saturating_sub(failed.elapsed())).await;
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
    assert_eq!(snapshot_upload_failures().await, 1);
    let r = b_restores(&p).await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, written.to_string().as_str(), Some("b")),
        "{}",
        r.body
    );
}

/// A periodic snapshot's upload that was given up may still land: the
/// counter says "not acknowledged", not "not there". Nothing is lost by
/// it, the actor keeps answering, a later snapshot succeeds, and another
/// node restores every acknowledged write.
#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_upload_given_up_that_lands_later_loses_nothing() {
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    write(&p, "x", 1).await;
    p.flaky.late_snapshot_puts.store(1, Ordering::SeqCst);
    let mut written = write_until_met(&p, "x", &p.flaky.late_snapshot_puts).await;
    // Answered once the attempt is given up (6 s), before it lands (8 s).
    written += 1;
    write(&p, "x", written).await;
    let given_up = std::time::Instant::now();
    assert_eq!(snapshot_upload_failures().await, 1, "not acknowledged");
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
    tokio::time::sleep(RETRY_WAIT.saturating_sub(given_up.elapsed())).await;
    written += 1;
    write(&p, "x", written).await;
    eventually("a later snapshot succeeds", async || {
        snapshots_of(&p, "x").await.len() == 3
    })
    .await;
    assert!(snapshots_of(&p, "x").await[2] > late);
    assert_eq!(snapshot_upload_failures().await, 1);
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
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    write(&p, "x", 1).await;
    write(&p, "x", 2).await;
    assert_eq!(snapshot_upload_failures().await, 0);
    assert_eq!(counter("wasmkeep_history_collect_failures_total").await, 0);
    p.flaky.fail_snapshot_puts.store(3, Ordering::SeqCst);
    let r = send(p.a.addr, "POST", "/actor/t/x/delete", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    // The delete is answered before its upload ends.
    eventually("every attempt of the upload failed", async || {
        p.flaky.fail_snapshot_puts.load(Ordering::SeqCst) == 0
    })
    .await;
    eventually("the failure is counted", async || {
        snapshot_upload_failures().await == 1
    })
    .await;
    assert_eq!(counter("wasmkeep_history_collect_failures_total").await, 1);
    assert_eq!(snapshots_of(&p, "x").await.len(), 1, "the first one only");
    let r = b_restores(&p).await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, "0", Some("b")),
        "{}",
        r.body
    );
}
