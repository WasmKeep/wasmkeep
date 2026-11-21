//! A slow bucket, as seen from one node (ADR 0020 and ADR 0011,
//! amendments of 2026-11-20): its reads of the heartbeats answer late, or
//! its read of `control/changed` never answers. Its trust in a peer that
//! keeps writing must not lapse because of its own late reads, while a
//! peer that stopped writing still lapses one horizon after its last
//! write.
//!
//! Timings here: heartbeats every 100 ms, a horizon of 400 ms (three
//! periods plus 100 ms of allowed skew), one heartbeat read at most 266 ms
//! (the lease attempt).

use super::*;

/// How often the samples below look at `a`'s view.
const SAMPLE: Duration = Duration::from_millis(10);

/// Samples whether `node` reaches `peer` every [`SAMPLE`] for `span`:
/// how many samples found it did not, of how many.
async fn unreached(node: &ActorService, peer: &str, span: Duration) -> (usize, usize) {
    let started = std::time::Instant::now();
    let (mut missed, mut taken) = (0, 0);
    while started.elapsed() < span {
        taken += 1;
        if !node.reaches_node(peer) {
            missed += 1;
        }
        tokio::time::sleep(SAMPLE).await;
    }
    (missed, taken)
}

/// Waits (up to 5 s) until `node` reaches every one of `peers`.
async fn reaches_all(node: &ActorService, peers: &[&str]) {
    let started = std::time::Instant::now();
    while !peers.iter().all(|p| node.reaches_node(p)) {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "never reached all of {peers:?}"
        );
        tokio::time::sleep(SAMPLE).await;
    }
}

/// Every heartbeat read from `a` answers 100 ms late. Read one after the
/// other, with `control/changed` after them in the same loop, `a`'s view
/// of three nodes would be renewed only every 300 ms or more, and its trust
/// in each peer (400 ms after that peer's write) would lapse for part of
/// every round: forwarding refused, hops from that peer refused. Read at
/// once, each is renewed every period. A peer that dies meanwhile (cut
/// off from the bucket, then gone) still lapses within the horizon of its
/// last write.
#[tokio::test(flavor = "multi_thread")]
async fn forwarding_stays_up_on_a_slow_bucket_while_a_dead_peer_expires() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky_a = Arc::new(Flaky::new(Arc::clone(&shared), Arc::default()));
    let seen_by_a: Arc<dyn ObjectStore> = Arc::clone(&flaky_a) as Arc<dyn ObjectStore>;
    let down_c = Arc::new(AtomicBool::new(false));
    let seen_by_c: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down_c)));
    let (da, db, dc) = (
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    );
    let idle = Duration::from_secs(30);
    let a = node_unplaced("a", da.path(), &seen_by_a, idle, "t", test_actor_wasm()).await;
    let b = node_unplaced("b", db.path(), &shared, idle, "t", test_actor_wasm()).await;
    let c = node_unplaced("c", dc.path(), &seen_by_c, idle, "t", test_actor_wasm()).await;
    settle().await;
    // `x` lives on b.
    let r = send(b.addr, "GET", "/actor/t/x/fired", "").await;
    assert_eq!((r.status, r.node.as_deref()), (200, Some("b")));
    reaches_all(&a.service, &["b", "c"]).await;

    flaky_a.delay_node_gets_ms.store(100, Ordering::SeqCst);
    // Requests through a for b's actor all along, while the view is sampled.
    let addr = a.addr;
    let traffic = tokio::spawn(async move {
        let started = std::time::Instant::now();
        let mut failed = Vec::new();
        let mut sent = 0;
        while started.elapsed() < Duration::from_secs(2) {
            sent += 1;
            let r = send(addr, "GET", "/actor/t/x/fired", "").await;
            if (r.status, r.node.as_deref()) != (200, Some("b")) {
                failed.push((r.status, r.body));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        (sent, failed)
    });
    let (missed, taken) = unreached(&a.service, "b", Duration::from_secs(2)).await;
    let (sent, failed) = traffic.await.unwrap();
    assert_eq!(
        missed, 0,
        "a's trust in b lapsed in {missed} of {taken} samples on a slow bucket"
    );
    assert!(
        failed.is_empty(),
        "{} of {sent} forwarded requests failed: {failed:?}",
        failed.len()
    );

    // c dies: cut off from the bucket (it can neither renew nor release
    // its heartbeat), then gone. Its last write came before this.
    down_c.store(true, Ordering::SeqCst);
    let died = std::time::Instant::now();
    stop_listening(c).await;
    while a.service.reaches_node("c") {
        assert!(
            died.elapsed() < Duration::from_secs(5),
            "a still trusts c 5 s after it died"
        );
        tokio::time::sleep(SAMPLE).await;
    }
    let lapsed = died.elapsed();
    // The horizon (400 ms) after its last write, whatever a's reads do;
    // the slack is the sampling and a loaded machine.
    assert!(
        lapsed < Duration::from_millis(900),
        "a trusted c for {lapsed:?} after it died"
    );
    // And b stayed reachable through it all.
    let (missed, taken) = unreached(&a.service, "b", Duration::from_millis(500)).await;
    assert_eq!(missed, 0, "b unreached in {missed} of {taken} samples");
    eprintln!("a stopped trusting c {lapsed:?} after it died");
}

/// A read of `control/changed` that never answers does not hold up the
/// view: the operator's intents are read on a task of their own, so the
/// heartbeats are still read every period and the trust in a peer that
/// keeps writing never lapses.
#[tokio::test(flavor = "multi_thread")]
async fn a_hung_control_read_does_not_hold_up_the_view() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky_a = Arc::new(Flaky::new(Arc::clone(&shared), Arc::default()));
    let seen_by_a: Arc<dyn ObjectStore> = Arc::clone(&flaky_a) as Arc<dyn ObjectStore>;
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let a = node_unplaced("a", da.path(), &seen_by_a, idle, "t", test_actor_wasm()).await;
    let b = node_unplaced("b", db.path(), &shared, idle, "t", test_actor_wasm()).await;
    settle().await;
    reaches_all(&a.service, &["b"]).await;
    flaky_a.hang_control_changed.store(true, Ordering::SeqCst);
    let (missed, taken) = unreached(&a.service, "b", Duration::from_millis(1_500)).await;
    assert_eq!(
        missed, 0,
        "a's trust in b lapsed in {missed} of {taken} samples while control/changed hung"
    );
    let r = send(a.addr, "GET", "/actor/t/y/fired", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    drop(b);
}
