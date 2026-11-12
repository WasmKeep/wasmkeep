//! A periodic snapshot whose upload hangs or fails: it never holds the
//! actor for long, never fails the event, and is tried again.

use super::*;

/// How many snapshot upload failures the node's metrics page counts.
async fn snapshot_upload_failures() -> u64 {
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
        .find_map(|l| l.strip_prefix("wasmkeep_snapshot_upload_failures_total "))
        .unwrap_or_else(|| panic!("no wasmkeep_snapshot_upload_failures_total in\n{page}"))
        .trim()
        .parse()
        .unwrap()
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

/// A periodic snapshot whose upload the bucket leaves hanging is given up
/// after seconds and sent again, as a segment's is: the write that made it
/// due is acknowledged, and so is the next one. It used to hold the actor
/// for as long as the bucket's client would wait.
#[tokio::test(flavor = "multi_thread")]
async fn a_hung_snapshot_upload_does_not_hold_the_actor() {
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    write(&p, "hung", 1).await;
    assert_eq!(snapshots_of(&p, "hung").await.len(), 1, "the first one");
    p.flaky.hang_snapshot_puts.store(1, Ordering::SeqCst);
    let written = write_until_met(&p, "hung", &p.flaky.hang_snapshot_puts).await;
    // The actor is free again within the upload's bound (an attempt is
    // given up after 6 s), not the client's: each write is answered
    // within 15 s.
    write(&p, "hung", written + 1).await;
    // The resend landed: the snapshot is there, and nothing was counted
    // as failed.
    eventually("the snapshot's resend lands", async || {
        snapshots_of(&p, "hung").await.len() == 2
    })
    .await;
    assert_eq!(snapshot_upload_failures().await, 0);
}

/// A periodic snapshot whose upload fails (every attempt) fails no event,
/// and the next event tries again: the count of segments since the last
/// snapshot is not reset by a failure. It used to be, and the next
/// snapshot came only after as many segments again.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_snapshot_is_tried_again_by_the_next_event() {
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    write(&p, "retried", 1).await;
    // Every attempt of one upload (a failure is sent again, three times).
    p.flaky.fail_snapshot_puts.store(3, Ordering::SeqCst);
    let written = write_until_met(&p, "retried", &p.flaky.fail_snapshot_puts).await;
    // One more write: whichever of the last two the snapshot failed
    // after, an event has ended since.
    write(&p, "retried", written + 1).await;
    let started = std::time::Instant::now();
    let snapshots = loop {
        let snapshots = snapshots_of(&p, "retried").await;
        if snapshots.len() == 2 {
            break snapshots;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the failed snapshot was not tried again: {snapshots:?} after {} writes",
            written + 1
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(
        p.flaky.fail_snapshot_puts.load(Ordering::SeqCst),
        0,
        "every attempt of the first upload failed"
    );
    let mut segments = history_of(&p.flaky.inner, "retried").await.1;
    segments.sort_unstable();
    assert!(
        segments
            .iter()
            .rev()
            .take(2)
            .any(|s| Some(s) == snapshots.last()),
        "at one of the last two writes: {snapshots:?}, {segments:?}"
    );
    // The count starts again from that snapshot: no third one soon.
    for i in 2..=10 {
        write(&p, "retried", written + i).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(snapshots_of(&p, "retried").await.len(), 2);
}

/// A failed snapshot upload is counted, once per upload whatever its
/// attempts, and costs nothing acknowledged: another node restores every
/// write from the segments.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_snapshot_is_counted_and_loses_nothing() {
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    write(&p, "x", 1).await;
    assert_eq!(snapshot_upload_failures().await, 0);
    p.flaky.fail_snapshot_puts.store(3, Ordering::SeqCst);
    let written = write_until_met(&p, "x", &p.flaky.fail_snapshot_puts).await;
    write(&p, "x", written + 1).await;
    eventually("the snapshot is tried again", async || {
        snapshots_of(&p, "x").await.len() == 2
    })
    .await;
    assert_eq!(snapshot_upload_failures().await, 1, "one upload failed");
    let r = b_restores(&p).await;
    assert_eq!(
        (r.status, r.body.trim(), r.node.as_deref()),
        (200, (written + 1).to_string().as_str(), Some("b")),
        "{}",
        r.body
    );
}
