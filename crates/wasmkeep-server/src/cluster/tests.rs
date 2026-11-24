#![allow(clippy::unwrap_used)] // test code

use super::*;

/// The default lease's ttl plus grace, for the heartbeat rounds the tests
/// feed by hand ([`super::process::Transport::note_heartbeats`]).
const LEASE_TTL_GRACE_MS: u64 = 15_000;

#[test]
fn the_pool_drops_a_peer_connection_before_the_peer_closes_it() {
    assert!(PEER_POOL_IDLE < crate::http::ServeLimits::default().header_read_timeout);
}

/// A pooled connection idle past the limit is not reused: the second
/// request opens a new one.
#[tokio::test]
async fn an_idle_peer_connection_is_not_reused() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = Arc::clone(&accepted);
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 1024];
                loop {
                    let Ok(n) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    while let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        buf.drain(..end + 4);
                        let ok = b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n";
                        if socket.write_all(ok).await.is_err() {
                            return;
                        }
                    }
                }
            });
        }
    });
    let client = peer_client_builder(Duration::from_millis(100))
        .build::<_, http_body_util::Empty<bytes::Bytes>>(
            hyper_util::client::legacy::connect::HttpConnector::new(),
        );
    let get = || {
        client.request(
            Request::get(format!("http://{addr}/"))
                .body(http_body_util::Empty::new())
                .unwrap(),
        )
    };
    get().await.unwrap().into_body().collect().await.unwrap();
    get().await.unwrap().into_body().collect().await.unwrap();
    assert_eq!(
        accepted.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "kept alive"
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    get().await.unwrap().into_body().collect().await.unwrap();
    assert_eq!(
        accepted.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "idle one dropped"
    );
}

/// Only nodes of the version before the implicit app are refused: two
/// apps of one tenant share a prefix safely (ADR 0022).
#[test]
fn only_nodes_of_the_version_before_are_refused() {
    let n = |id: &str, app: &str| NodeInfo {
        id: id.into(),
        address: "http://x".into(),
        version: "0".into(),
        written_at_ms: 0,
        app: app.into(),
        cluster_address: None,
        tls_pins: Vec::new(),
        drain: None,
        reads_secrets: false,
        can_take: false,
        can_front: false,
        can_handoff: false,
        can_end: false,
        can_flush: false,
        can_rate_limits: false,
        can_assets: false,
        can_blobs: false,
        can_restore: false,
        can_actor_restore: false,
        instance: None,
        lease: None,
        load: None,
        can_recorded_head: false,
        can_multi_app: false,
        can_place: false,
        process: None,
    };
    let nodes = [
        n("me", "acme/shop"),
        n("b", "acme/shop"),  // same app
        n("c", "other/shop"), // another tenant
        n("d", "acme/admin"), // same tenant, another app: its own root
        n("e", "acme/"),      // command-line classes of an older version
        n("f", ""),           // predates the app line: the start guard's
    ];
    assert_eq!(older_nodes("me", &nodes), vec!["e (acme/)".to_owned()]);
    assert!(older_nodes("e", &nodes[4..5]).is_empty());
    let advice = older_nodes_advice(&["e (default/)".to_owned()]);
    assert!(advice.contains("stop every node"), "{advice}");
    assert!(advice.contains("e (default/)"), "{advice}");
}

#[test]
fn hmac_matches_rfc_4231() {
    let hex = |b: [u8; 32]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
    // Test case 1.
    assert_eq!(
        hex(hmac_sha256(&[0x0b; 20], b"Hi There")),
        "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
    );
    // Test case 2.
    assert_eq!(
        hex(hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    );
    // Test case 6: a key longer than the block.
    assert_eq!(
        hex(hmac_sha256(
            &[0xaa; 131],
            b"Test Using Larger Than Block-Size Key - Hash Key First"
        )),
        "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
    );
}

/// Header values are signed as their bytes: two values that differ only
/// in bytes that are not UTF-8 never share a signature (a lossy decoding
/// would read both as U+FFFD).
#[test]
fn signatures_sign_header_bytes_not_their_lossy_text() {
    let secret = "ab".repeat(32);
    let method = hyper::Method::POST;
    let path = "/actor/u/x/durable";
    let ts = 1_000_000;
    let body = body_sha256(b"");
    let nonce = "0f1e";
    let with = |value: &[u8]| {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(
            CALLER,
            hyper::header::HeaderValue::from_bytes(value).unwrap(),
        );
        headers
    };
    let (ff, fe) = (with(b"default/v/\xff"), with(b"default/v/\xfe"));
    let mac = signature(&secret, ts, nonce, "b", &method, path, &body, &ff);
    assert_ne!(
        mac,
        signature(&secret, ts, nonce, "b", &method, path, &body, &fe)
    );
    let given = format!("v4 {ts} {nonce} {body} {mac}");
    assert!(verify(&secret, &given, ts, "b", &method, path, &ff).is_some());
    assert!(verify(&secret, &given, ts, "b", &method, path, &fe).is_none());
    // Valid UTF-8 signs as it always did: the line is its text.
    let plain = with(b"default/v/a");
    let msg = format!(
        "wasmkeep-call v4\n{ts}\n{nonce}\nb\n{method}\n{path}\n{body}\n{CALLER}:default/v/a\n"
    );
    let want: String = hmac_sha256(secret.as_bytes(), msg.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(
        signature(&secret, ts, nonce, "b", &method, path, &body, &plain),
        want
    );
}

#[test]
fn signatures_cover_every_field_and_expire() {
    let secret = "ab".repeat(32);
    let mut headers = hyper::HeaderMap::new();
    headers.insert(CALLER, "default/v/a".parse().unwrap());
    headers.append(CHAIN, "default/v/root".parse().unwrap());
    let method = hyper::Method::POST;
    let path = "/actor/u/x/durable";
    let ts = 1_000_000;
    let body = body_sha256(b"{\"amount\":10}");
    let nonce = "0f1e";
    let mac = signature(&secret, ts, nonce, "b", &method, path, &body, &headers);
    let given = format!("v4 {ts} {nonce} {body} {mac}");
    let ok = |secret: &str, now, me, m: &hyper::Method, p, h: &hyper::HeaderMap| {
        verify(secret, &given, now, me, m, p, h).is_some()
    };
    let signed = verify(&secret, &given, ts, "b", &method, path, &headers).unwrap();
    assert_eq!(signed.body, body, "the signed body hash is handed back");
    assert_eq!(signed.ts, ts);
    // Another body hash, or another nonce, under the same mac: refused.
    let other_body = format!(
        "v4 {ts} {nonce} {} {mac}",
        body_sha256(b"{\"amount\":9999}")
    );
    assert!(verify(&secret, &other_body, ts, "b", &method, path, &headers).is_none());
    let other_nonce = format!("v4 {ts} 0f1f {body} {mac}");
    assert!(verify(&secret, &other_nonce, ts, "b", &method, path, &headers).is_none());
    assert!(ok(&secret, ts + 5_000, "b", &method, path, &headers));

    // Another secret, target node, method, path, caller or chain:
    // refused.
    assert!(!ok(&"cd".repeat(32), ts, "b", &method, path, &headers));
    assert!(!ok(&secret, ts, "c", &method, path, &headers));
    assert!(!ok(&secret, ts, "b", &hyper::Method::GET, path, &headers));
    assert!(!ok(
        &secret,
        ts,
        "b",
        &method,
        "/actor/u/y/durable",
        &headers
    ));
    let mut other = headers.clone();
    other.insert(CALLER, "default/v/b".parse().unwrap());
    assert!(!ok(&secret, ts, "b", &method, path, &other));
    let mut longer = headers.clone();
    longer.append(CHAIN, "default/v/more".parse().unwrap());
    assert!(!ok(&secret, ts, "b", &method, path, &longer));
    // Too old, or from too far in the future.
    assert!(!ok(
        &secret,
        ts + SIGNATURE_WINDOW_MS + 1,
        "b",
        &method,
        path,
        &headers
    ));
    assert!(!ok(
        &secret,
        ts - SIGNATURE_WINDOW_MS - 1,
        "b",
        &method,
        path,
        &headers
    ));
    // The raw secret, garbage, or the old formats are not signatures.
    let v1 = format!("v1 {ts} {mac}");
    let v2 = format!("v2 {ts} {mac}");
    let v3 = format!("v3 {ts} {body} {mac}");
    let short = format!("v4 {ts} {nonce} abc {mac}");
    let no_nonce = format!("v4 {ts}  {body} {mac}");
    let upper_nonce = format!("v4 {ts} 0F1E {body} {mac}");
    for bad in [
        secret.as_str(),
        "",
        "v4",
        "v4 x y z w",
        v1.as_str(),
        v2.as_str(),
        v3.as_str(),
        short.as_str(),
        no_nonce.as_str(),
        upper_nonce.as_str(),
    ] {
        assert!(
            verify(&secret, bad, ts, "b", &method, path, &headers).is_none(),
            "{bad:?}"
        );
    }
}

#[test]
fn a_replay_whose_body_arrives_after_the_window_is_refused() {
    // The window is checked when the headers arrive; the body may take
    // a while. By then the original's record may have expired: the
    // replay must not take its place.
    let seen = SeenSignatures::default();
    let t = 1_000_000;
    assert!(seen.first_use([1; 32], t, t));
    // Later traffic expires the original's record.
    assert!(seen.first_use([2; 32], t + 61_000, t + 61_000));
    // The replay, signed at t, checked in time, body done at t + 61 s.
    assert!(!seen.first_use([1; 32], t, t + 61_000));
    // Even at the very edge (expired 1 ms ago).
    let edge = SeenSignatures::default();
    assert!(edge.first_use([1; 32], t, t));
    assert!(!edge.first_use([1; 32], t, t + SIGNATURE_WINDOW_MS + 1));
}

#[test]
fn a_clock_stepping_back_does_not_revive_a_used_signature() {
    let seen = SeenSignatures::default();
    let t = 1_000_000;
    assert!(seen.first_use([1; 32], t, t));
    // 70 s later the original's record is dropped...
    assert!(seen.first_use([2; 32], t + 70_000, t + 70_000));
    // ...then the clock steps back 30 s: t is inside the window again
    // for verify, but the record is gone, so it must be refused.
    assert!(!seen.first_use([1; 32], t, t + 40_000));
    // A fresh signature in the stepped-back time still passes.
    assert!(seen.first_use([3; 32], t + 40_000, t + 40_000));
}

#[test]
fn a_steady_stream_of_calls_is_never_refused() {
    // 5,000 calls a second for three minutes (the old record refused
    // everything past about 1,667 a second): entries expire as fast as
    // they arrive, so the record never fills up.
    let seen = SeenSignatures::default();
    let mut n: u32 = 0;
    for ms in 0..180_000u64 {
        for _ in 0..5 {
            let mut mac = [0u8; 32];
            mac[..4].copy_from_slice(&n.to_le_bytes());
            n += 1;
            assert!(seen.first_use(mac, ms, ms), "refused at {ms} ms");
        }
    }
    assert!(seen.len() <= 5 * (SIGNATURE_WINDOW_MS as usize + 1));
}

#[test]
fn identical_calls_in_one_millisecond_get_distinct_signatures() {
    // Same secret, time, target, method, path, body, caller and chain:
    // only the nonce differs, and the signatures with it.
    let secret = "ab".repeat(32);
    let mut headers = hyper::HeaderMap::new();
    headers.insert(CALLER, "default/v/a".parse().unwrap());
    let body = body_sha256(b"same");
    let sign = |nonce: &str| {
        signature(
            &secret,
            7,
            nonce,
            "b",
            &hyper::Method::POST,
            "/actor/u/x/",
            &body,
            &headers,
        )
    };
    assert_ne!(sign("01"), sign("02"));
}

/// A full record refuses fresh signatures as full, not as replays, so
/// the log tells exhaustion from a replay.
#[test]
fn a_full_record_tells_full_from_used() {
    let seen = SeenSignatures::with_cap("control", 2);
    assert_eq!(seen.spend([1; 32], 1_000, 1_000), Spent::Fresh);
    assert_eq!(seen.spend([2; 32], 1_000, 1_000), Spent::Fresh);
    assert_eq!(seen.spend([3; 32], 1_000, 1_000), Spent::Full);
    assert_eq!(seen.spend([1; 32], 1_000, 1_000), Spent::Used);
}

/// The start floor follows this node's clock set back: a node that
/// started with its clock ahead does not refuse every signature until real
/// time catches up with its start, only those made before it started.
#[test]
fn a_clock_set_back_after_the_start_lowers_the_start_floor() {
    let start = 10_000_000;
    let seen = SeenSignatures::default().started_now(0);
    assert!(seen.used([1; 32], start - 5, start));
    // The clock is set back an hour (no signature was spent meanwhile:
    // ahead by more than the window, every one was refused unchecked).
    let now = start - 3_600_000;
    assert!(!seen.used([2; 32], now, now));
    assert_eq!(seen.spend([2; 32], now, now), Spent::Fresh);
    assert_eq!(seen.spend([3; 32], now - 5_000, now), Spent::BeforeStart);
}

/// A sender whose clock is ahead (by up to the allowed skew) may have
/// dated a call to this node's previous process later than this start:
/// the floor is the start plus that skew.
#[test]
fn the_start_floor_covers_a_sender_whose_clock_is_ahead() {
    let start = 10_000_000;
    let seen = SeenSignatures::default().started_now(1_000);
    assert_eq!(seen.spend([1; 32], start + 500, start), Spent::BeforeStart);
    assert_eq!(seen.spend([2; 32], start + 1_000, start), Spent::Fresh);
}

/// This node's clock stepping forward after its start moves the floor with
/// it: a call dated before the start as the clock now reads is refused.
#[test]
fn the_start_floor_follows_a_clock_stepping_forward() {
    let start = 10_000_000;
    let seen = SeenSignatures::default().started_now(0);
    let now = start + 10_000;
    assert_eq!(seen.spend([1; 32], start + 5_000, now), Spent::BeforeStart);
    assert_eq!(seen.spend([2; 32], now, now), Spent::Fresh);
}

/// The public record tells a cluster-port check that a signature may have
/// been spent there once its record may be forgotten, not only while it
/// is held.
#[test]
fn the_public_record_holds_what_it_may_have_forgotten() {
    let seen = SeenSignatures::public();
    let t = 1_000_000;
    assert_eq!(seen.spend([1; 32], t, t), Spent::Fresh);
    assert!(seen.holds(&[1; 32], t));
    let later = t + 2 * SIGNATURE_WINDOW_MS;
    assert_eq!(seen.spend([2; 32], later, later), Spent::Fresh);
    assert_eq!(seen.len(), 1, "the first record was forgotten");
    assert!(
        seen.holds(&[1; 32], t),
        "forgotten, so it may have been spent"
    );
    assert!(!seen.holds(&[9; 32], later));
}

/// A full public record drops new burns, counted apart from the cluster
/// port's records, and refuses nothing.
#[test]
fn a_full_public_record_drops_and_counts() {
    let seen = SeenSignatures::public();
    let dropped = || {
        crate::metrics::METRICS
            .signatures_public_dropped
            .load(std::sync::atomic::Ordering::Relaxed)
    };
    for i in 0..signing::MAX_SEEN_PUBLIC {
        let mut mac = [0u8; 32];
        mac[..8].copy_from_slice(&(i as u64).to_le_bytes());
        assert_eq!(seen.spend(mac, 1_000, 1_000), Spent::Fresh);
    }
    let before = dropped();
    assert_eq!(seen.spend([0xff; 32], 1_000, 1_000), Spent::Full);
    assert!(dropped() > before);
    assert!(!seen.holds(&[0xff; 32], 1_000));
}

#[test]
fn a_signature_is_accepted_once() {
    let seen = SeenSignatures::default();
    assert!(seen.first_use([1; 32], 1_000, 1_000));
    assert!(!seen.first_use([1; 32], 1_000, 1_500), "replayed");
    assert!(seen.first_use([2; 32], 1_500, 1_500));
    // Remembered until its own timestamp leaves the window (when verify
    // would refuse it anyway), then forgotten.
    let end = 1_000 + SIGNATURE_WINDOW_MS;
    assert!(!seen.first_use([1; 32], 1_000, end), "still in the window");
    assert!(seen.first_use([3; 32], end + 1, end + 1));
    assert_eq!(seen.len(), 2, "mac 1 expired, 2 and 3 kept");
}

#[test]
fn only_a_full_strength_secret_is_accepted() {
    assert!(valid_secret(&"0f".repeat(32)));
    for bad in ["", "abc", &"zz".repeat(32), &"ab".repeat(33)] {
        assert!(!valid_secret(bad), "{bad:?}");
    }
}

/// Node `id` on `store`, with 100 ms heartbeats.
fn fast_node(id: &str, store: &Arc<dyn ObjectStore>) -> Arc<Cluster> {
    let mut config = ClusterConfig::new(
        id,
        format!("http://{id}-{}", random_hex(4).unwrap()),
        Arc::clone(store),
    );
    config.heartbeat = Duration::from_millis(100);
    config.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap()
}

#[tokio::test]
async fn a_second_process_with_a_live_node_id_takes_no_lease() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let first = fast_node("a", &store);
    first.identity().await.unwrap();
    let address = ActorAddress::new("t", "c", "x").unwrap();
    let hold = LeaseHold::acquire(&first, &address).await.unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await; // heartbeats land
    let second = fast_node("a", &store);
    let why = second.identity().await.unwrap_err();
    assert!(
        why.starts_with("another process runs with this node id: ") && why.contains("node a"),
        "{why}"
    );
    assert_eq!(
        second.not_ready(),
        Some("another process runs with this node id")
    );
    // Its leases would read as the first's: it takes none.
    let other = ActorAddress::new("t", "c", "y").unwrap();
    assert!(matches!(
        LeaseHold::acquire(&second, &other).await,
        Err(OwnershipError::Refused(_))
    ));
    drop(hold);
}

#[tokio::test]
async fn a_node_restarted_soon_after_it_stopped_keeps_its_id() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let first = fast_node("a", &store);
    first.identity().await.unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    drop(first); // its heartbeat task stops with it
    let started = std::time::Instant::now();
    let again = fast_node("a", &store);
    again.identity().await.unwrap();
    // Its predecessor's heartbeat was still live: it watched it for a
    // lease's ttl plus grace (1.2 s here) before taking anything.
    assert!(
        started.elapsed() >= Duration::from_millis(1_200),
        "{:?}",
        started.elapsed()
    );
    let address = ActorAddress::new("t", "c", "x").unwrap();
    LeaseHold::acquire(&again, &address).await.unwrap();
}

/// A heartbeat in `nodes/a` for node a, written `age` ago.
async fn old_heartbeat(store: &Arc<dyn ObjectStore>, age: Duration) {
    let at = now_ms() - u64::try_from(age.as_millis()).unwrap();
    let body = format!("wasmkeep-node 2\na\nhttp://old\n0.0.0\n{at}\nt/\n");
    store
        .put(&StorePath::from("nodes/a"), body.into_bytes().into())
        .await
        .unwrap();
}

#[tokio::test]
async fn a_fresh_fenced_node_lease_is_watched_like_a_live_heartbeat() {
    // ADR 0020: a fenced or released object proves its process lapsed only
    // once its kept written_at plus ttl and grace has passed. Fresh, the
    // start check must wait as for a live heartbeat, not go on at once.
    for state in ["fenced", "released"] {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let at = now_ms();
        let body = format!(
            "wasmkeep-node 3\n{state} beef\n{at}\na\nhttp://old\n0.0.0\nt/\n\n\nserving\n\ntake\n"
        );
        store
            .put(&StorePath::from("nodes/a"), body.into_bytes().into())
            .await
            .unwrap();
        let started = std::time::Instant::now();
        fast_node("a", &store).identity().await.unwrap();
        assert!(
            started.elapsed() >= Duration::from_millis(1_200),
            "{state}: {:?}",
            started.elapsed()
        );
    }
}

#[tokio::test]
async fn an_expired_heartbeat_is_watched_until_its_leases_lapse() {
    // The heartbeat horizon is 0.4 s here, plus the lease's ttl and grace
    // (1.2 s): a heartbeat written 0.1 s ago keeps the check waiting until
    // it is 1.6 s old (its process may still renew per-actor leases).
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    old_heartbeat(&store, Duration::from_millis(100)).await;
    let started = std::time::Instant::now();
    fast_node("a", &store).identity().await.unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(1_450),
        "{:?}",
        started.elapsed()
    );
    // Older than that: the wait every start makes, ttl plus grace (ADR
    // 0020), and no more.
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    old_heartbeat(&store, Duration::from_secs(10)).await;
    let started = std::time::Instant::now();
    fast_node("a", &store).identity().await.unwrap();
    let took = started.elapsed();
    assert!(
        (Duration::from_millis(1_200)..Duration::from_millis(1_450)).contains(&took),
        "{took:?}"
    );
}

#[tokio::test]
async fn a_heartbeat_this_version_cannot_read_is_watched_too() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let key = StorePath::from("nodes/a");
    let put = async |body: &[u8]| store.put(&key, body.to_vec().into()).await.unwrap();
    put(b"wasmkeep-node 9\nfrom a later version\n").await;
    let started = std::time::Instant::now();
    let node = fast_node("a", &store);
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Not taken for "no heartbeat": still checking.
    assert_eq!(
        node.not_ready(),
        Some("checking that no other process runs with this node id")
    );
    put(b"wasmkeep-node 9\nrewritten\n").await;
    let why = node.identity().await.unwrap_err();
    assert!(why.contains("cannot read"), "{why}");
    assert!(started.elapsed() < Duration::from_millis(1_200));
}

/// ADR 0020: no object is no proof either. A process whose object was
/// deleted while it ran must be given time to show itself, so even a new
/// id waits ttl plus grace before its first write.
#[tokio::test]
async fn a_new_node_id_waits_ttl_plus_grace_too() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let started = std::time::Instant::now();
    let a = fast_node("a", &store);
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(
        store.head(&StorePath::from("nodes/a")).await.is_err(),
        "written before the wait ended"
    );
    a.identity().await.unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(1_200),
        "{:?}",
        started.elapsed()
    );
    // The object then appears, a format-3 live node lease of this process.
    let body = store
        .get(&StorePath::from("nodes/a"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(text.starts_with("wasmkeep-node 3\nlive "), "{text}");
}

fn node(id: &str) -> Arc<Cluster> {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    Cluster::new(
        &ClusterConfig::new(id, format!("http://{id}"), store),
        &StateRoot::new("", "t", "default"),
    )
    .unwrap()
}

#[tokio::test]
async fn the_sweep_cursor_resumes_under_a_prefix_that_needs_escaping() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut config = ClusterConfig::new("a", "http://a", store);
    config.prefix = "team~1".into(); // `~` is escaped in stored keys
    let cluster = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    for (i, name) in ["a", "b", "c", "d", "e"].iter().enumerate() {
        let address = ActorAddress::new("t", "c", *name).unwrap();
        cluster
            .put_new_alarm_entry(&address, 1_000 + i as u64, 1)
            .await
            .unwrap();
    }
    let mut cursor = None;
    let mut names = Vec::new();
    for _ in 0..3 {
        let (due, next) = cluster
            .alarms_due_before(u64::MAX, cursor.as_ref(), 2)
            .await
            .unwrap();
        names.extend(due.into_iter().map(|(a, _, _)| a.name().to_owned()));
        cursor = next;
    }
    assert_eq!(
        names,
        ["a", "b", "c", "d", "e"],
        "each page continues the last"
    );
    assert!(cursor.is_none(), "the listing ended");
}

#[tokio::test]
async fn an_actors_entries_at_one_time_are_listed_and_deleted_exactly() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut config = ClusterConfig::new("a", "http://a", Arc::clone(&store));
    config.prefix = "team~1".into();
    let cluster = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    let x = ActorAddress::new("t", "c", "x").unwrap();
    // "xy" is 7879 in hex: its keys start with x's (78), but not at a
    // segment boundary.
    let xy = ActorAddress::new("t", "c", "xy").unwrap();
    let first = cluster.put_new_alarm_entry(&x, 50, 3).await.unwrap();
    let second = cluster.put_new_alarm_entry(&x, 50, 3).await.unwrap();
    assert_ne!(first, second, "every write gets a fresh key");
    let later = cluster.put_new_alarm_entry(&x, 51, 3).await.unwrap();
    cluster.put_new_alarm_entry(&xy, 50, 3).await.unwrap();
    store
        .put(
            &cluster.alarm_key(&x, &Entry::legacy(50)),
            object_store::PutPayload::new(),
        )
        .await
        .unwrap();

    let mut listed = cluster.alarm_entries_at(&x, 50).await.unwrap();
    listed.sort();
    let mut expected = vec![first.clone(), second.clone()];
    expected.sort();
    assert_eq!(listed, expected, "tagged entries of x at 50 only");

    // The sweep sees all five, legacy included.
    let (due, _) = cluster
        .alarms_due_before(u64::MAX, None, 100)
        .await
        .unwrap();
    assert_eq!(due.len(), 5, "{due:?}");

    cluster.delete_alarm_entry(&x, &first).await.unwrap();
    cluster
        .delete_alarm_entry(&x, &Entry::legacy(50))
        .await
        .unwrap();
    assert_eq!(cluster.alarm_entries_at(&x, 50).await.unwrap(), [second]);
    assert_eq!(cluster.alarm_entries_at(&x, 51).await.unwrap(), [later]);
    let (due, _) = cluster
        .alarms_due_before(u64::MAX, None, 100)
        .await
        .unwrap();
    assert_eq!(due.len(), 3, "{due:?}");
}

#[test]
fn an_alarm_hand_off_body_reads_back_and_junk_does_not() {
    let body = alarm_handoff_body("acme", 1_234);
    assert_eq!(
        parse_alarm_handoff(body.as_bytes()),
        Some(("acme".into(), 1_234))
    );
    for junk in [
        "",
        "wasmkeep-alarm 1\nacme\n",
        "wasmkeep-alarm 2\nacme\n1\n",
        "wasmkeep-alarm 1\nacme\nsoon\n",
        "wasmkeep-alarm 1\nacme\n1\nextra\n",
    ] {
        assert_eq!(parse_alarm_handoff(junk.as_bytes()), None, "{junk:?}");
    }
}

#[test]
fn a_take_body_reads_back_and_the_signature_covers_its_marker() {
    assert_eq!(
        parse_take(take_body("acme").as_bytes()),
        Some("acme".into())
    );
    for junk in [
        "",
        "wasmkeep-take 1\n",
        "wasmkeep-take 2\nacme\n",
        "wasmkeep-take 1\nacme\nx\n",
    ] {
        assert_eq!(parse_take(junk.as_bytes()), None, "{junk:?}");
    }
    let sign = |headers: &hyper::HeaderMap| {
        signature(
            "s",
            1,
            "n",
            "b",
            &hyper::Method::POST,
            "/actor/c/x/",
            "h",
            headers,
        )
    };
    let mut marked = hyper::HeaderMap::new();
    marked.insert(TAKE, hyper::header::HeaderValue::from_static("1"));
    assert_ne!(sign(&hyper::HeaderMap::new()), sign(&marked));
}

#[test]
fn the_signature_covers_the_alarm_hand_off_marker() {
    let sign = |headers: &hyper::HeaderMap| {
        signature(
            "s",
            1,
            "n",
            "b",
            &hyper::Method::POST,
            "/actor/c/x/",
            "h",
            headers,
        )
    };
    let plain = hyper::HeaderMap::new();
    let mut marked = hyper::HeaderMap::new();
    marked.insert(ALARM_HANDOFF, hyper::header::HeaderValue::from_static("1"));
    // Adding or stripping the marker breaks the signature.
    assert_ne!(sign(&plain), sign(&marked));
}

#[test]
fn rendezvous_is_stable_and_spreads_actors() {
    let nodes = ["node-a", "node-b", "node-c", "node-d"];
    let mut per_node = std::collections::HashMap::new();
    for i in 0..4_000 {
        let key = format!("t/room/{i}");
        let pick = rendezvous(&key, nodes).unwrap();
        // The same answer whatever the order of the candidates.
        let mut reversed = nodes;
        reversed.reverse();
        assert_eq!(rendezvous(&key, reversed), Some(pick));
        *per_node.entry(pick).or_insert(0) += 1;
        // Taking another node away moves none of this one's actors.
        let other = nodes.iter().find(|n| **n != pick).unwrap();
        let fewer = nodes.iter().copied().filter(|n| n != other);
        assert_eq!(rendezvous(&key, fewer), Some(pick));
    }
    for n in nodes {
        let got = per_node[n];
        assert!((800..1_200).contains(&got), "{n}: {got} of 4000");
    }
    assert_eq!(rendezvous("k", []), None);
}

#[tokio::test]
async fn only_serving_peers_of_the_same_app_that_are_not_draining_are_chosen() {
    let me = node("me"); // serves `t/default`
    let peer = |app: &str, drain: Option<DrainState>| Peer {
        plain: Some("http://x".into()),
        tls: None,
        app: app.into(),
        drain,
        can_take: false,
        can_handoff: false,
        can_end: false,
        can_blobs: false,
        instance: None,
        can_place: false,
        seen: None,
    };
    let serving = Some(DrainState::Serving);
    *me.peers.write().unwrap() = HashMap::from([
        ("me".to_owned(), peer("t/default", serving)),
        ("other-tenant".to_owned(), peer("u/default", serving)),
        ("too-old".to_owned(), peer("t/default", None)),
        (
            "draining".to_owned(),
            peer("t/default", Some(DrainState::Draining)),
        ),
        ("marked".to_owned(), peer("t/default", serving)),
        (
            "unreachable".to_owned(),
            Peer {
                plain: None,
                tls: Some("https://y:7443".into()),
                ..peer("t/default", serving)
            },
        ),
        ("good".to_owned(), peer("t/default", serving)),
    ]);
    // `marked` is asked to drain, though its heartbeat still says serving.
    me.control
        .write()
        .unwrap()
        .draining
        .insert("marked".to_owned());
    for i in 0..50 {
        let a = ActorAddress::new("t", "room", i.to_string()).unwrap();
        assert_eq!(me.chosen_peer(&a).as_deref(), Some("good"));
    }
    // A peer that failed to take an event is skipped for a while.
    let a = ActorAddress::new("t", "room", "x").unwrap();
    me.suspect("good");
    assert_eq!(me.chosen_peer(&a), None, "the only peer is suspect");
    me.suspects.lock().unwrap().insert("good".to_owned(), 0);
    assert_eq!(
        me.chosen_peer(&a).as_deref(),
        Some("good"),
        "suspicion expired"
    );
    me.peers.write().unwrap().remove("good");
    assert_eq!(me.chosen_peer(&a), None, "no eligible peer");
    assert!(!me.draining());
    me.control.write().unwrap().draining.insert("me".to_owned());
    assert!(me.draining());
}

#[tokio::test]
async fn only_nodes_that_deliver_this_apps_alarms_share_the_sweep() {
    let me = node("me"); // serves `t/`
    let peer = |app: &str, drain: Option<DrainState>| Peer {
        plain: Some("http://x".into()),
        tls: None,
        app: app.into(),
        drain,
        can_take: false,
        can_handoff: false,
        can_end: false,
        can_blobs: false,
        instance: None,
        can_place: false,
        seen: None,
    };
    let serving = Some(DrainState::Serving);
    *me.peers.write().unwrap() = HashMap::from([
        ("me".to_owned(), peer("t/default", serving)),
        ("same".to_owned(), peer("t/default", serving)),
        ("other-app".to_owned(), peer("t/admin", serving)),
        ("other-tenant".to_owned(), peer("u/default", serving)),
        // A node of an older version, which names no app (ADR 0022):
        // refused on every hop, so it is given no share.
        ("older".to_owned(), peer("", None)),
        (
            "draining".to_owned(),
            peer("t/default", Some(DrainState::Draining)),
        ),
        ("marked".to_owned(), peer("t/default", serving)),
    ]);
    me.control
        .write()
        .unwrap()
        .draining
        .insert("marked".to_owned());
    let shards = me.alarm_shards();
    assert_eq!(shards.nodes, ["me", "same"]);
    assert_eq!(shards.me, Some(0));

    // Draining itself: its share goes to the others.
    me.control.write().unwrap().draining.insert("me".to_owned());
    let shards = me.alarm_shards();
    assert_eq!(shards.nodes, ["same"]);
    assert_eq!(shards.me, None);
    let a = ActorAddress::new("t", "c", "x").unwrap();
    assert!(!shards.is_mine(&a));

    // No other node left: this one keeps everything.
    me.peers.write().unwrap().retain(|id, _| id == "me");
    let shards = me.alarm_shards();
    assert_eq!(shards.nodes, ["me"]);
    assert!(shards.is_mine(&a));
}

#[tokio::test]
async fn live_nodes_split_the_alarm_sweep_between_them() {
    let (a, b) = (node("a"), node("b"));
    for c in [&a, &b] {
        let peer = |a: &str| Peer {
            plain: Some(a.to_owned()),
            tls: None,
            app: "t/default".to_owned(),
            drain: None,
            can_take: false,
            can_handoff: false,
            can_end: false,
            can_blobs: false,
            instance: None,
            can_place: false,
            seen: None,
        };
        *c.peers.write().unwrap() = HashMap::from([
            ("a".to_owned(), peer("http://a")),
            ("b".to_owned(), peer("http://b")),
        ]);
    }
    let mut mine_a = 0;
    for i in 0..1_000 {
        let address = ActorAddress::new("t", "c", format!("actor-{i}")).unwrap();
        let (ma, mb) = (
            a.alarm_shard_is_mine(&address),
            b.alarm_shard_is_mine(&address),
        );
        assert!(ma != mb, "exactly one node takes {address}");
        assert_eq!(ma, a.alarm_shard_is_mine(&address), "stable");
        mine_a += usize::from(ma);
    }
    assert!((350..650).contains(&mine_a), "uneven split: {mine_a}/1000");

    // Alone (no fresh heartbeats seen), a node takes everything.
    let alone = node("c");
    let address = ActorAddress::new("t", "c", "x").unwrap();
    assert!(alone.alarm_shard_is_mine(&address));
}

#[tokio::test]
async fn a_tls_node_publishes_its_pin_and_is_ready_a_period_later() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut config = ClusterConfig::new("a", "http://a", Arc::clone(&store));
    config.heartbeat = Duration::from_millis(600);
    // Short, so the start check (ttl plus grace) fits the deadline below.
    config.lease = LeaseConfig::new(1_800, 200, 200, 100).unwrap();
    config.cluster_tls = Some(ClusterTlsConfig::new("https://a.test:7443"));
    let a = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    let tls = Arc::clone(a.tls().unwrap());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while a.published_ms.load(std::sync::atomic::Ordering::Relaxed) == 0 {
        assert!(tokio::time::Instant::now() < deadline, "never published");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // Published, but peers may not have listed it yet.
    assert_eq!(
        a.not_ready(),
        Some("cluster identity not published for a heartbeat period yet")
    );
    let listed = a.membership.live_nodes().await.unwrap();
    assert_eq!(
        listed[0].cluster_address.as_deref(),
        Some("https://a.test:7443")
    );
    assert_eq!(listed[0].tls_pins, vec![tls.pin()]);
    // Ready a period and a half after publishing (900 ms).
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert_eq!(a.not_ready(), None);

    // A node with an identity reaches TLS peers over TLS and the others
    // in plaintext; one without reaches everyone in plaintext.
    let plain = node("p");
    for c in [&a, &plain] {
        *c.peers.write().unwrap() = HashMap::from([
            (
                "b".to_owned(),
                Peer {
                    plain: Some("http://b".into()),
                    tls: Some("https://b:7443".into()),
                    app: String::new(),
                    drain: None,
                    can_take: false,
                    can_handoff: false,
                    can_end: false,
                    can_blobs: false,
                    instance: None,
                    can_place: false,
                    seen: None,
                },
            ),
            (
                "c".to_owned(),
                Peer {
                    plain: Some("http://c".into()),
                    tls: None,
                    app: String::new(),
                    drain: None,
                    can_take: false,
                    can_handoff: false,
                    can_end: false,
                    can_blobs: false,
                    instance: None,
                    can_place: false,
                    seen: None,
                },
            ),
            (
                "d".to_owned(),
                Peer {
                    plain: None,
                    tls: Some("https://d:7443".into()),
                    app: String::new(),
                    drain: None,
                    can_take: false,
                    can_handoff: false,
                    can_end: false,
                    can_blobs: false,
                    instance: None,
                    can_place: false,
                    seen: None,
                },
            ),
        ]);
    }
    assert_eq!(a.address_of("b").as_deref(), Some("https://b:7443"));
    assert_eq!(a.address_of("c").as_deref(), Some("http://c"));
    assert_eq!(a.address_of("d").as_deref(), Some("https://d:7443"));
    assert_eq!(plain.address_of("b").as_deref(), Some("http://b"));
    assert_eq!(plain.address_of("d"), None);
}

#[tokio::test]
async fn connection_pools_are_dropped_when_an_address_changes_hands() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut config = ClusterConfig::new("a", "http://a", Arc::clone(&store));
    // No listing but the ones this test makes.
    config.heartbeat = Duration::from_secs(3_600);
    config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 1_000).unwrap();
    config.cluster_tls = Some(ClusterTlsConfig::new("https://a.test:7443"));
    let a = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    let publish = |pin: u8| {
        let store = Arc::clone(&store);
        async move {
            let body = format!(
                "wasmkeep-node 2\nb\nhttp://b\n0\n{}\nt/\nhttps://b.test:7443\n{}\n",
                now_ms(),
                format!("{pin:02x}").repeat(32)
            );
            store
                .put(&StorePath::from("nodes/b"), body.into_bytes().into())
                .await
                .unwrap();
        }
    };
    publish(1).await;
    a.refresh_view(true).await;
    let first = a.clients();
    publish(1).await;
    a.refresh_view(true).await;
    assert!(Arc::ptr_eq(&first, &a.clients()), "same pins: pools kept");
    publish(2).await;
    a.refresh_view(true).await;
    assert!(!Arc::ptr_eq(&first, &a.clients()), "new pin: pools dropped");
    // And a trusted address whose heartbeat expired is not sent to,
    // even on a pooled connection.
    assert_eq!(a.check_tls_target("https://b.test:7443"), Ok(()));
    assert!(a.check_tls_target("https://c.test:7443").is_err());
    assert_eq!(a.check_tls_target("http://c.test:80"), Ok(()));
}

#[tokio::test]
async fn after_a_heartbeat_outage_the_identity_counts_as_published_anew() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut config = ClusterConfig::new("a", "http://a", store);
    config.heartbeat = Duration::from_secs(3_600);
    config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 1_000).unwrap();
    config.cluster_tls = Some(ClusterTlsConfig::new("https://a.test:7443"));
    let a = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    let published = || a.published_ms.load(std::sync::atomic::Ordering::Relaxed);
    a.heartbeat_reached_bucket(1_000_000);
    assert_eq!(published(), 1_000_000);
    // Heartbeats on time keep the first publication...
    let window = a.heartbeat_window_ms();
    a.heartbeat_reached_bucket(1_000_000 + window);
    assert_eq!(published(), 1_000_000);
    // ...one after a lapse longer than the window starts it again.
    let late = 1_000_000 + 3 * window;
    a.heartbeat_reached_bucket(late);
    assert_eq!(published(), late);
}

#[tokio::test]
async fn refuse_mode_never_reaches_a_peer_in_plaintext() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut config = ClusterConfig::new("a", "http://a", Arc::clone(&store));
    config.heartbeat = Duration::from_secs(3_600);
    config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 1_000).unwrap();
    // Refusing plain HTTP needs a cluster listener.
    config.cluster_plaintext = ClusterPlaintext::Refuse;
    assert!(matches!(
        Cluster::new(&config, &StateRoot::new("", "t", "default")),
        Err(crate::Error::ClusterTls(_))
    ));
    config.cluster_tls = Some(ClusterTlsConfig::new("https://a.test:7443"));
    let a = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    *a.peers.write().unwrap() = HashMap::from([
        (
            "b".to_owned(),
            Peer {
                plain: Some("http://b".into()),
                tls: Some("https://b:7443".into()),
                app: String::new(),
                drain: None,
                can_take: false,
                can_handoff: false,
                can_end: false,
                can_blobs: false,
                instance: None,
                can_place: false,
                seen: None,
            },
        ),
        (
            "c".to_owned(),
            Peer {
                plain: Some("http://c".into()),
                tls: None,
                app: String::new(),
                drain: None,
                can_take: false,
                can_handoff: false,
                can_end: false,
                can_blobs: false,
                instance: None,
                can_place: false,
                seen: None,
            },
        ),
    ]);
    assert_eq!(a.address_of("b").as_deref(), Some("https://b:7443"));
    assert_eq!(a.address_of("c"), None, "a plain-only peer is not used");
    assert!(a.check_tls_target("http://c").is_err());
}

#[tokio::test]
async fn refuse_mode_honours_node_signatures_on_the_cluster_port_only() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let secret = "ab".repeat(32);
    store
        .put(
            &StorePath::from(SECRET_KEY),
            secret.clone().into_bytes().into(),
        )
        .await
        .unwrap();
    let mut config = ClusterConfig::new("a", "http://a", store);
    config.heartbeat = Duration::from_secs(3_600);
    config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 1_000).unwrap();
    config.cluster_tls = Some(ClusterTlsConfig::new("https://a.test:7443"));
    let accept = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    config.cluster_plaintext = ClusterPlaintext::Refuse;
    let refuse = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    let path = "/actor/u/x/";
    let method = hyper::Method::POST;
    let ts = now_ms();
    let hash = body_sha256(b"");
    let mut headers = hyper::HeaderMap::new();
    let mac = signature(&secret, ts, "a1", "a", &method, path, &hash, &headers);
    headers.insert(
        INTERNAL,
        format!("v4 {ts} a1 {hash} {mac}").parse().unwrap(),
    );
    for c in [&accept, &refuse] {
        c.load_secret().await;
        assert!(c.is_internal(&method, path, &headers, true).is_some());
    }
    assert!(accept.is_internal(&method, path, &headers, false).is_some());
    assert!(refuse.is_internal(&method, path, &headers, false).is_none());
}

/// A call to node `a` signed with `secret` at `ts`, with nonce `n` and
/// the `marker` header if any.
fn signed_call(secret: &str, ts: u64, n: u64, marker: Option<&str>) -> hyper::HeaderMap {
    let mut headers = hyper::HeaderMap::new();
    if let Some(m) = marker {
        headers.insert(
            hyper::header::HeaderName::from_bytes(m.as_bytes()).unwrap(),
            hyper::header::HeaderValue::from_static("1"),
        );
    }
    let hash = body_sha256(b"");
    let nonce = format!("{n:x}");
    let mac = signature(
        secret,
        ts,
        &nonce,
        "a",
        &hyper::Method::POST,
        "/actor/u/x/",
        &hash,
        &headers,
    );
    headers.insert(
        INTERNAL,
        format!("v4 {ts} {nonce} {hash} {mac}").parse().unwrap(),
    );
    headers
}

/// Node `a` in refuse mode, the cluster secret `secret` loaded.
async fn refuse_node(secret: &str) -> Arc<Cluster> {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    store
        .put(
            &StorePath::from(SECRET_KEY),
            secret.to_owned().into_bytes().into(),
        )
        .await
        .unwrap();
    let mut config = ClusterConfig::new("a", "http://a", store);
    config.heartbeat = Duration::from_secs(3_600);
    config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 1_000).unwrap();
    config.cluster_tls = Some(ClusterTlsConfig::new("https://a.test:7443"));
    config.cluster_plaintext = ClusterPlaintext::Refuse;
    let c = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    c.load_secret().await;
    c
}

/// Signed calls on the public port (refuse mode) are spent in a record of
/// their own: however many arrive, they never fill the records the
/// cluster port's calls, takes and alarm hand-offs are checked against,
/// and once that record is full new ones are dropped, refusing nothing
/// on the cluster port. One spent there while it had room is still
/// refused on the cluster port.
#[tokio::test]
async fn public_port_signatures_never_fill_the_cluster_ports_records() {
    let secret = "ab".repeat(32);
    let c = refuse_node(&secret).await;
    let m = hyper::Method::POST;
    let path = "/actor/u/x/";
    // Past the allowed skew (1 s here) after the start.
    let ts = now_ms() + 1_500;
    let early = signed_call(&secret, ts, 0, Some(TAKE));
    assert!(c.is_internal(&m, path, &early, false).is_none());
    let copy = c.is_internal(&m, path, &early, true).unwrap();
    assert_eq!(
        c.admission_replayed(&copy, true),
        Some(SignatureRefused::Used)
    );
    assert_eq!(c.first_use(&copy, true), Err(SignatureRefused::Used));
    // More than any record but the calls' holds, two in three with a
    // marker.
    let n = u64::try_from(signing::MAX_SEEN_CONTROL.max(signing::MAX_SEEN_PUBLIC)).unwrap() * 2;
    for i in 1..=n {
        let marker = [Some(TAKE), Some(ALARM_HANDOFF), None][usize::try_from(i % 3).unwrap()];
        let h = signed_call(&secret, ts, i, marker);
        assert!(c.is_internal(&m, path, &h, false).is_none());
    }
    // The cluster port's take, alarm hand-off and call still pass.
    for (i, marker, control) in [
        (n + 1, Some(TAKE), true),
        (n + 2, Some(ALARM_HANDOFF), true),
        (n + 3, None, false),
    ] {
        let h = signed_call(&secret, ts, i, marker);
        let signed = c.is_internal(&m, path, &h, true).unwrap();
        assert_eq!(c.admission_replayed(&signed, control), None, "{marker:?}");
        assert_eq!(c.first_use(&signed, control), Ok(()), "{marker:?} refused");
    }
    assert_eq!(c.seen_control.len(), 2);
    assert_eq!(c.seen.len(), 1);
}

/// A signature made before the receiving runtime started is refused, even
/// within its window: the records of what was used died with the previous
/// process (or runtime) under this node id, so it may have been used
/// there. One made since passes.
#[tokio::test]
async fn a_signature_made_before_this_node_started_is_refused() {
    let secret = "ab".repeat(32);
    let before = now_ms() - 5_000;
    for c in [refuse_node(&secret).await, {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        store
            .put(
                &StorePath::from(SECRET_KEY),
                secret.clone().into_bytes().into(),
            )
            .await
            .unwrap();
        let mut config = ClusterConfig::new("a", "http://a", store);
        config.heartbeat = Duration::from_secs(3_600);
        config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 1_000).unwrap();
        let c = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
        c.load_secret().await;
        c
    }] {
        let m = hyper::Method::POST;
        let path = "/actor/u/x/";
        for (marker, control) in [(None, false), (Some(TAKE), true)] {
            let old = signed_call(&secret, before, 1, marker);
            let signed = c.is_internal(&m, path, &old, true).unwrap();
            assert_eq!(
                c.admission_replayed(&signed, control),
                Some(SignatureRefused::BeforeStart)
            );
            assert_eq!(
                c.first_use(&signed, control),
                Err(SignatureRefused::BeforeStart),
                "an older signature passed"
            );
            // Past the allowed skew (1 s here) after the start.
            let new = signed_call(&secret, now_ms() + 1_500, 2, marker);
            let signed = c.is_internal(&m, path, &new, true).unwrap();
            assert_eq!(c.admission_replayed(&signed, control), None);
            assert_eq!(c.first_use(&signed, control), Ok(()));
        }
    }
}

/// A replay is answered 403; a call signed before this node started, or
/// one past a full record, 503 with a retry (marked refused for a placed
/// event, so its sender runs it): never an answer a guest could take for
/// the callee's.
#[test]
fn only_a_replay_is_answered_forbidden() {
    let plain = hyper::HeaderMap::new();
    let mut placed = hyper::HeaderMap::new();
    placed.insert(PLACED, "1".parse().unwrap());
    let used = SignatureRefused::Used.answer(&plain);
    assert_eq!(used.status(), StatusCode::FORBIDDEN);
    for why in [SignatureRefused::BeforeStart, SignatureRefused::Full] {
        let r = why.answer(&plain);
        assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE, "{why:?}");
        assert!(r.headers().contains_key(hyper::header::RETRY_AFTER));
        assert!(!r.headers().contains_key(PLACED));
        let r = why.answer(&placed);
        assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(r.headers()[PLACED], PLACED_REFUSED);
        assert_eq!(why.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}

/// Call nonces are seeded by randomness never published: not the instance,
/// which every heartbeat carries, so nobody can sign a node's next call
/// ahead of it.
#[tokio::test]
async fn call_nonces_are_not_seeded_by_the_published_instance() {
    let secret = "ab".repeat(32);
    let a = refuse_node(&secret).await;
    let b = refuse_node(&secret).await;
    let instance = a.membership.instance().unwrap().to_owned();
    let nonce = a.nonce();
    assert!(
        !nonce.starts_with(&instance),
        "{nonce} starts with {instance}"
    );
    assert!(nonce.len() >= 32 && nonce.len() <= 64, "{nonce}");
    assert_ne!(nonce[..32], b.nonce()[..32]);
}

/// ADR 0011, amendment of 2026-11-14: a hello asks for a listing only as
/// a node's signed call to this node, with an empty body, once; anything
/// else is refused and asks for nothing.
#[tokio::test]
async fn only_a_nodes_signed_hello_asks_for_a_listing() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let secret = "ab".repeat(32);
    store
        .put(
            &StorePath::from(SECRET_KEY),
            secret.clone().into_bytes().into(),
        )
        .await
        .unwrap();
    let mut config = ClusterConfig::new("a", "http://a", store);
    config.heartbeat = Duration::from_secs(3_600);
    config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 1_000).unwrap();
    let a = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    a.load_secret().await;
    let wanted = |c: &Cluster| {
        c.listing_wanted
            .swap(false, std::sync::atomic::Ordering::AcqRel)
    };
    wanted(&a);
    let post = hyper::Method::POST;
    // Each hello gets a fresh nonce, as a sending node makes it.
    let signed = |to: &str, body: &[u8], path: &str| {
        let nonce = a.nonce();
        let nonce = nonce.as_str();
        // Past the allowed skew (1 s here) after the start.
        let ts = now_ms() + 1_500;
        let hash = body_sha256(body);
        let mut headers = hyper::HeaderMap::new();
        let mac = signature(&secret, ts, nonce, to, &post, path, &hash, &headers);
        headers.insert(
            INTERNAL,
            format!("v4 {ts} {nonce} {hash} {mac}").parse().unwrap(),
        );
        headers
    };
    // Unsigned.
    let none = hyper::HeaderMap::new();
    assert_eq!(
        a.hello_received(&post, &none, b"", true),
        StatusCode::FORBIDDEN
    );
    // Signed for another node, another path, or another body.
    for (headers, body) in [
        (signed("b", b"", HELLO_PATH), &b""[..]),
        (signed("a", b"", "/actor/u/x/"), &b""[..]),
        (signed("a", b"", HELLO_PATH), &b"x"[..]),
    ] {
        assert_eq!(
            a.hello_received(&post, &headers, body, true),
            StatusCode::FORBIDDEN
        );
    }
    // Another method than the one signed.
    let headers = signed("a", b"", HELLO_PATH);
    assert_eq!(
        a.hello_received(&hyper::Method::GET, &headers, b"", true),
        StatusCode::FORBIDDEN
    );
    assert!(!wanted(&a), "a refused hello asked for a listing");
    // A node's hello: a listing; the same again (a replay): refused.
    let headers = signed("a", b"", HELLO_PATH);
    assert_eq!(a.hello_received(&post, &headers, b"", true), StatusCode::OK);
    assert!(wanted(&a));
    assert_eq!(
        a.hello_received(&post, &headers, b"", true),
        StatusCode::FORBIDDEN
    );
    assert!(!wanted(&a));
    // In refuse mode, never on the public port.
    config.cluster_tls = Some(ClusterTlsConfig::new("https://a.test:7443"));
    config.cluster_plaintext = ClusterPlaintext::Refuse;
    let refuse = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    refuse.load_secret().await;
    wanted(&refuse);
    let headers = signed("a", b"", HELLO_PATH);
    assert_eq!(
        refuse.hello_received(&post, &headers, b"", false),
        StatusCode::FORBIDDEN
    );
    assert!(!wanted(&refuse));
}

#[tokio::test]
async fn each_refusal_reason_has_its_own_log_slot() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut config = ClusterConfig::new("a", "http://a", store);
    config.heartbeat = Duration::from_secs(3_600);
    config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 1_000).unwrap();
    let a = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    let t = 1_000_000;
    assert!(a.refusal_log_due("junk on the public port", t));
    assert!(!a.refusal_log_due("junk on the public port", t + 1));
    // Junk does not hide a real refusal...
    assert!(a.refusal_log_due("signature already used", t + 2));
    assert!(!a.refusal_log_due("signature already used", t + 3));
    // ...and each comes back after a minute.
    assert!(a.refusal_log_due("junk on the public port", t + 60_000));
}

#[test]
fn a_relayed_upgrade_carries_no_node_or_framing_headers() {
    let mut client = hyper::HeaderMap::new();
    for (name, value) in [
        ("host", "public.example"),
        (INTERNAL, "v4 1 a b c"),
        (CALLER, "default/t/x"),
        (CHAIN, "default/t/y"),
        (FORWARDED, "0"),
        ("x-wasmkeep-actor", "default/t/z"),
        // Markers only a node may set, and any other name in the reserved
        // range: a client must not choose any of them for the owner.
        (TAKE, "1"),
        (ALARM_HANDOFF, "1"),
        (HOLDER, "a"),
        ("X-WasmKeep-Something-New", "1"),
        ("content-length", "5"),
        ("transfer-encoding", "chunked"),
        ("connection", "keep-alive, upgrade, x-custom"),
        ("x-custom", "1"),
        ("upgrade", "websocket"),
        ("te", "trailers"),
        ("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ=="),
        ("sec-websocket-version", "13"),
        ("origin", "https://app.example"),
        ("cookie", "s=1"),
    ] {
        client.append(name, value.parse().unwrap());
    }
    let out = upgrade_headers(&client);
    for gone in [
        "host",
        INTERNAL,
        CALLER,
        CHAIN,
        FORWARDED,
        "x-wasmkeep-actor",
        TAKE,
        ALARM_HANDOFF,
        HOLDER,
        "x-wasmkeep-something-new",
        "content-length",
        "transfer-encoding",
        "te",
        "x-custom",
    ] {
        assert!(!out.contains_key(gone), "{gone} relayed");
    }
    assert!(
        !out.keys().any(|k| k.as_str().starts_with("x-wasmkeep-")),
        "a reserved header was relayed: {out:?}"
    );
    assert_eq!(out["connection"], "upgrade");
    assert_eq!(out["upgrade"], "websocket");
    for kept in [
        "sec-websocket-key",
        "sec-websocket-version",
        "origin",
        "cookie",
    ] {
        assert_eq!(out[kept], client[kept], "{kept}");
    }
}

#[test]
fn a_forwarded_request_keeps_its_framing_but_not_its_connection_headers() {
    let mut headers = hyper::HeaderMap::new();
    for (name, value) in [
        ("host", "public.example"),
        ("connection", "close, X-Trace-Hop"),
        (
            "connection",
            "content-length, transfer-encoding, bad header",
        ),
        ("x-trace-hop", "1"),
        ("keep-alive", "timeout=5"),
        ("proxy-connection", "keep-alive"),
        ("proxy-authorization", "Basic eDp5"),
        ("te", "trailers"),
        ("trailer", "x-sum"),
        ("upgrade", "h2c"),
        ("content-length", "5"),
        ("transfer-encoding", "chunked"),
        ("content-type", "text/plain"),
        ("authorization", "Bearer t"),
    ] {
        headers.append(name, value.parse().unwrap());
    }
    strip_hop_by_hop(&mut headers, Framing::Keep);
    let mut left: Vec<_> = headers.keys().map(|k| k.as_str()).collect();
    left.sort_unstable();
    assert_eq!(
        left,
        [
            "authorization",
            "content-length",
            "content-type",
            "host",
            "trailer",
            "transfer-encoding",
        ]
    );
    strip_hop_by_hop(&mut headers, Framing::Drop);
    for gone in FRAMING {
        assert!(!headers.contains_key(*gone), "{gone} kept");
    }
}

#[tokio::test]
async fn a_bad_cluster_address_is_refused_at_start() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut config = ClusterConfig::new("a", "http://a", store);
    config.cluster_tls = Some(ClusterTlsConfig::new("http://a.test:7443"));
    assert!(matches!(
        Cluster::new(&config, &StateRoot::new("", "t", "default")),
        Err(crate::Error::ClusterTls(_))
    ));
}

/// ADR 0020, section 1: the node lease is written conditionally, so a
/// writer that replaced it (a fence, a twin, a deletion, an older version)
/// makes the next write fail; the re-read does not show this process live,
/// and it stops, saying which one it found, in the same words in its exit
/// reason and its readiness.
#[tokio::test]
async fn a_replaced_node_lease_stops_the_node() {
    for (how, words) in [
        ("fenced", "this node was fenced"),
        ("deleted", "this node's lease object was deleted"),
        ("twin", "another process runs with this node id"),
        ("old", "an older version of WasmKeep runs with this node id"),
        (
            "released",
            "this node's lease was released by another process",
        ),
        (
            "unreadable",
            "this node's lease object holds something this version cannot read",
        ),
    ] {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let a = fast_node("a", &store);
        a.identity().await.unwrap();
        let key = StorePath::from("nodes/a");
        let wait_valid = std::time::Instant::now();
        while !a.node_lease_valid() {
            assert!(
                wait_valid.elapsed() < Duration::from_secs(2),
                "{how}: never valid"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(a.not_ready(), None, "{how}");
        match how {
            "fenced" => {
                let body = store.get(&key).await.unwrap().bytes().await.unwrap();
                let text = String::from_utf8(body.to_vec()).unwrap();
                let fenced = text.replacen("\nlive ", "\nfenced ", 1);
                assert_ne!(fenced, text);
                store.put(&key, fenced.into_bytes().into()).await.unwrap();
            }
            "twin" => {
                let body = live_lease_body("a", "other", now_ms());
                store.put(&key, body.into_bytes().into()).await.unwrap();
            }
            "released" => {
                let body = live_lease_body("a", "other", now_ms()).replacen(
                    "\nlive other\n",
                    "\nreleased other\n",
                    1,
                );
                store.put(&key, body.into_bytes().into()).await.unwrap();
            }
            "unreadable" => {
                store
                    .put(&key, b"not a node lease".to_vec().into())
                    .await
                    .unwrap();
            }
            "old" => {
                let body = format!(
                    "wasmkeep-node 2\na\nhttp://old\n0.0.0\n{}\nt/\n\n\nserving\n",
                    now_ms()
                );
                store.put(&key, body.into_bytes().into()).await.unwrap();
            }
            _ => store.delete(&key).await.unwrap(),
        }
        let why = tokio::time::timeout(Duration::from_secs(2), a.identity_lost())
            .await
            .unwrap_or_else(|_| panic!("{how}: the node never noticed"));
        assert!(why.starts_with(words), "{how}: {why}");
        assert!(!a.node_lease_valid(), "{how}");
        assert_eq!(a.not_ready(), Some(words), "{how}");
        // It writes nothing more: the fence (or the absence) stays.
        tokio::time::sleep(Duration::from_millis(300)).await;
        match how {
            "fenced" => {
                let body = store.get(&key).await.unwrap().bytes().await.unwrap();
                assert!(String::from_utf8_lossy(&body).contains("\nfenced "));
            }
            "deleted" => assert!(store.head(&key).await.is_err(), "rewritten after deletion"),
            _ => {
                let body = store.get(&key).await.unwrap().bytes().await.unwrap();
                assert!(
                    !String::from_utf8_lossy(&body).contains("\nlive a"),
                    "{how}: rewritten"
                );
            }
        }
    }
}

/// The start check classifies a change it finds as a lease write's
/// re-read would: an object removed meanwhile is a deletion, one this
/// version cannot read is unreadable, not a twin.
#[tokio::test]
async fn the_start_check_names_what_changed_the_object() {
    for (how, words) in [
        ("deleted", "this node's lease object was deleted"),
        (
            "unreadable",
            "this node's lease object holds something this version cannot read",
        ),
    ] {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let key = StorePath::from("nodes/a");
        store
            .put(
                &key,
                live_lease_body("a", "other", now_ms()).into_bytes().into(),
            )
            .await
            .unwrap();
        let a = fast_node("a", &store);
        // During the check's wait (ttl plus grace, 1.2 s here).
        tokio::time::sleep(Duration::from_millis(300)).await;
        match how {
            "deleted" => store.delete(&key).await.unwrap(),
            _ => {
                store
                    .put(&key, b"not a node lease".to_vec().into())
                    .await
                    .unwrap();
            }
        }
        let why = a.identity().await.unwrap_err();
        assert!(why.starts_with(words), "{how}: {why}");
        assert_eq!(a.not_ready(), Some(words), "{how}");
    }
}

/// A peer's fence over the process the start check first read is not a
/// twin: that process stopped writing. The check goes on, and the first
/// write conditions on the fence's version.
#[tokio::test]
async fn a_fence_during_the_start_check_is_not_a_twin() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let key = StorePath::from("nodes/a");
    let live = |state: &str, at: u64| {
        format!(
            "wasmkeep-node 3\n{state} beef\n{at}\na\nhttp://old\n0.0.0\nt/\n\n\nserving\n\ntake\n"
        )
    };
    let at = now_ms();
    store
        .put(&key, live("live", at).into_bytes().into())
        .await
        .unwrap();
    let a = fast_node("a", &store);
    tokio::time::sleep(Duration::from_millis(300)).await;
    store
        .put(&key, live("fenced", at).into_bytes().into())
        .await
        .unwrap();
    a.identity().await.unwrap();
    let body = store.get(&key).await.unwrap().bytes().await.unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(text.starts_with("wasmkeep-node 3\nlive "), "{text}");
    assert!(!text.contains("beef"), "{text}");
}

#[test]
fn a_heartbeat_period_above_a_third_of_the_ttl_is_refused() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut config = ClusterConfig::new("a", "http://a", store);
    config.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    config.heartbeat = Duration::from_millis(334);
    let err = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap_err();
    assert!(err.to_string().contains("at most a third"), "{err}");
    config.heartbeat = Duration::from_millis(333);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async { Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap() });
}

/// ADR 0020, section 4: a clean stop ends with the node lease written
/// `released`, naming this process and keeping its last `written_at`.
#[tokio::test]
async fn a_clean_stop_releases_the_node_lease() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let key = StorePath::from("nodes/a");
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    let instance = a.membership.instance().unwrap().to_owned();
    assert!(a.stop_node_lease(Duration::from_secs(2)).await);
    assert!(!a.node_lease_valid());
    assert!(a.not_ready().is_some());
    let body = store.get(&key).await.unwrap().bytes().await.unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.starts_with(&format!("wasmkeep-node 3\nreleased {instance}\n")),
        "{text}"
    );
    // The heartbeat's fields are kept.
    assert!(text.contains("\na\nhttp://a-"), "{text}");
    // Nothing is written after it.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let again = store.get(&key).await.unwrap().bytes().await.unwrap();
    assert_eq!(again, body);
}

/// A store whose next conditional write of `nodes/` hangs (until the
/// writer gives up) once `hang` is set.
#[derive(Debug)]
struct HangOneLeaseWrite {
    inner: Arc<dyn ObjectStore>,
    hang: std::sync::atomic::AtomicBool,
    /// Reads of ownership records (`owners/`) wait this long first (ms).
    slow_owner_reads_ms: std::sync::atomic::AtomicU64,
    /// The next this-many `released` writes of a node lease fail.
    fail_releases: std::sync::atomic::AtomicU32,
    /// The next this-many `released` writes land, and answer an error.
    lose_release_replies: std::sync::atomic::AtomicU32,
    /// Writes of ownership records (`owners/`) wait this long first (ms).
    slow_owner_writes_ms: std::sync::atomic::AtomicU64,
    /// Writes of ownership records started.
    owner_writes: std::sync::atomic::AtomicU64,
}

impl std::fmt::Display for HangOneLeaseWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HangOneLeaseWrite({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for HangOneLeaseWrite {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if location.as_ref().starts_with("nodes/")
            && matches!(opts.mode, object_store::PutMode::Update(_))
            && self.hang.swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            std::future::pending::<()>().await;
        }
        let slow = self
            .slow_owner_writes_ms
            .load(std::sync::atomic::Ordering::SeqCst);
        if location.as_ref().starts_with("state/t/default/owners/") {
            self.owner_writes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if slow > 0 {
                tokio::time::sleep(Duration::from_millis(slow)).await;
            }
        }
        let released = || {
            let body: bytes::Bytes = payload.clone().into();
            String::from_utf8_lossy(&body).contains("\nreleased ")
        };
        if location.as_ref().starts_with("nodes/")
            && self.fail_releases.load(std::sync::atomic::Ordering::SeqCst) > 0
            && released()
        {
            self.fail_releases
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            return Err(object_store::Error::Generic {
                store: "test",
                source: "release refused".into(),
            });
        }
        if location.as_ref().starts_with("nodes/")
            && self
                .lose_release_replies
                .load(std::sync::atomic::Ordering::SeqCst)
                > 0
            && released()
        {
            self.lose_release_replies
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.put_opts(location, payload, opts).await?;
            return Err(object_store::Error::Generic {
                store: "test",
                source: "reply lost".into(),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &StorePath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let slow = self
            .slow_owner_reads_ms
            .load(std::sync::atomic::Ordering::SeqCst);
        if slow > 0 && location.as_ref().starts_with("state/t/default/owners/") {
            tokio::time::sleep(Duration::from_millis(slow)).await;
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, object_store::Result<StorePath>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<StorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// The lease is written every period from the previous *send*: one write
/// that hangs until its timeout must not push the next one past the
/// lease's validity (ADR 0020, section 1: three tries per lease). With a
/// 1 s ttl, 200 ms safety and a 333 ms period, a write sent at 333 ms
/// times out at 599 ms; the next must go at 666 ms, not 932 ms, after the
/// lease (valid until 800 ms) ran out.
#[tokio::test]
async fn one_hung_lease_write_does_not_make_the_node_not_ready() {
    let gate = Arc::new(HangOneLeaseWrite {
        inner: Arc::new(object_store::memory::InMemory::new()),
        hang: std::sync::atomic::AtomicBool::new(false),
        slow_owner_reads_ms: std::sync::atomic::AtomicU64::new(0),
        fail_releases: std::sync::atomic::AtomicU32::new(0),
        lose_release_replies: std::sync::atomic::AtomicU32::new(0),
        slow_owner_writes_ms: std::sync::atomic::AtomicU64::new(0),
        owner_writes: std::sync::atomic::AtomicU64::new(0),
    });
    let store: Arc<dyn ObjectStore> = Arc::clone(&gate) as Arc<dyn ObjectStore>;
    let mut config = ClusterConfig::new("a", "http://a", store);
    config.heartbeat = Duration::from_millis(333);
    config.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    let a = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    a.identity().await.unwrap();
    // Right after a write landed: the next one hangs.
    let mut valid = a.validity.subscribe();
    valid.changed().await.unwrap();
    gate.hang.store(true, std::sync::atomic::Ordering::SeqCst);
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_millis(1_500) {
        assert!(
            a.node_lease_valid(),
            "lapsed {:?} after the write before the hung one",
            started.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !gate.hang.load(std::sync::atomic::Ordering::SeqCst),
        "never hung"
    );
}

/// A clean stop that arrives while a lease write hangs: that write runs to
/// its timeout (it is not aborted), then `released` is written, within
/// the stop's limit.
#[tokio::test]
async fn a_clean_stop_during_a_hung_lease_write_still_releases() {
    let gate = Arc::new(HangOneLeaseWrite {
        inner: Arc::new(object_store::memory::InMemory::new()),
        hang: std::sync::atomic::AtomicBool::new(false),
        slow_owner_reads_ms: std::sync::atomic::AtomicU64::new(0),
        fail_releases: std::sync::atomic::AtomicU32::new(0),
        lose_release_replies: std::sync::atomic::AtomicU32::new(0),
        slow_owner_writes_ms: std::sync::atomic::AtomicU64::new(0),
        owner_writes: std::sync::atomic::AtomicU64::new(0),
    });
    let store: Arc<dyn ObjectStore> = Arc::clone(&gate) as Arc<dyn ObjectStore>;
    let mut config = ClusterConfig::new("a", "http://a", Arc::clone(&store));
    config.heartbeat = Duration::from_millis(100);
    config.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    let a = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    a.identity().await.unwrap();
    gate.hang.store(true, std::sync::atomic::Ordering::SeqCst);
    while gate.hang.load(std::sync::atomic::Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // The hung write is in flight now.
    let started = std::time::Instant::now();
    assert!(a.stop_node_lease(a.node_lease_stop_limit()).await);
    assert!(
        started.elapsed() >= Duration::from_millis(150),
        "{:?}",
        started.elapsed()
    );
    let body = store
        .get(&StorePath::from("nodes/a"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&body).contains("\nreleased "));
}

/// A clean stop whose `released` write fails tries it again, within the
/// stop's limit: a node lease left live would hold its actors' records
/// until a peer fences it, and a restart under this id would wait too.
#[tokio::test]
async fn a_clean_stop_retries_a_failed_released_write() {
    let gate = Arc::new(HangOneLeaseWrite {
        inner: Arc::new(object_store::memory::InMemory::new()),
        hang: std::sync::atomic::AtomicBool::new(false),
        slow_owner_reads_ms: std::sync::atomic::AtomicU64::new(0),
        fail_releases: std::sync::atomic::AtomicU32::new(2),
        lose_release_replies: std::sync::atomic::AtomicU32::new(0),
        slow_owner_writes_ms: std::sync::atomic::AtomicU64::new(0),
        owner_writes: std::sync::atomic::AtomicU64::new(0),
    });
    let store: Arc<dyn ObjectStore> = Arc::clone(&gate) as Arc<dyn ObjectStore>;
    let mut config = ClusterConfig::new("a", "http://a", Arc::clone(&store));
    config.heartbeat = Duration::from_millis(100);
    config.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    let a = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    a.identity().await.unwrap();
    let limit = a.node_lease_stop_limit();
    let started = std::time::Instant::now();
    assert!(a.stop_node_lease(limit).await);
    assert!(started.elapsed() < limit, "{:?}", started.elapsed());
    assert_eq!(
        gate.fail_releases.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "both failures were met"
    );
    let body = store
        .get(&StorePath::from("nodes/a"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(
        String::from_utf8_lossy(&body).contains("\nreleased "),
        "{}",
        String::from_utf8_lossy(&body)
    );
}

/// A `released` write that landed with its reply lost: the retry's
/// condition fails, the re-read finds the object released (not live), and
/// the stop is done, released, without adopting anything.
#[tokio::test]
async fn a_released_write_that_landed_unanswered_is_not_retried_wrongly() {
    let gate = Arc::new(HangOneLeaseWrite {
        inner: Arc::new(object_store::memory::InMemory::new()),
        hang: std::sync::atomic::AtomicBool::new(false),
        slow_owner_reads_ms: std::sync::atomic::AtomicU64::new(0),
        fail_releases: std::sync::atomic::AtomicU32::new(0),
        lose_release_replies: std::sync::atomic::AtomicU32::new(1),
        slow_owner_writes_ms: std::sync::atomic::AtomicU64::new(0),
        owner_writes: std::sync::atomic::AtomicU64::new(0),
    });
    let store: Arc<dyn ObjectStore> = Arc::clone(&gate) as Arc<dyn ObjectStore>;
    let mut config = ClusterConfig::new("a", "http://a", Arc::clone(&store));
    config.heartbeat = Duration::from_millis(100);
    config.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    let a = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    a.identity().await.unwrap();
    assert!(a.stop_node_lease(a.node_lease_stop_limit()).await);
    assert_eq!(
        gate.lose_release_replies
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the reply was lost"
    );
    let body = store
        .get(&StorePath::from("nodes/a"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&body).contains("\nreleased "));
}

/// A format-3 live node lease for `id`, as its process last wrote it.
fn live_lease_body(id: &str, instance: &str, at: u64) -> String {
    format!(
        "wasmkeep-node 3\nlive {instance}\n{at}\n{id}\nhttp://{id}\n0.0.0\nt/\n\n\nserving\n\ntake\n"
    )
}

/// ADR 0020, section 2: a node whose lease object stays unchanged is
/// fenced by a peer, but only once its `written_at + ttl + grace` passed
/// on the peer's clock (1.2 s here), keeping its fields; the fenced
/// object is then not live.
#[tokio::test]
async fn a_node_that_stopped_renewing_is_fenced() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    let key = StorePath::from("nodes/b");
    let at = now_ms();
    store
        .put(&key, live_lease_body("b", "beef", at).into_bytes().into())
        .await
        .unwrap();
    let fences = || {
        crate::metrics::METRICS
            .node_fences
            .load(std::sync::atomic::Ordering::Relaxed)
    };
    let before = fences();
    let started = std::time::Instant::now();
    let fenced = loop {
        let body = store.get(&key).await.unwrap().bytes().await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        if text.contains("\nfenced ") {
            break text;
        }
        assert!(started.elapsed() < Duration::from_secs(4), "never fenced");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let after = now_ms() - at;
    assert!(after > 1_200, "fenced {after} ms after its last write");
    // And soon after: a guard against gross delays. The exact rule (never
    // ready before ttl plus grace, so no refused first attempt) is
    // `the_watch_is_never_ready_before_the_object_is_stale` in the library.
    assert!(after < 2_000, "fenced {after} ms after its last write");
    assert_eq!(
        fenced,
        live_lease_body("b", "beef", at).replacen("\nlive ", "\nfenced ", 1)
    );
    assert!(fences() > before);
    // a itself is untouched and still ready.
    assert_eq!(a.not_ready(), None);
}

/// Nodes that keep renewing are never fenced, however long they run.
#[tokio::test]
async fn nodes_that_renew_are_never_fenced() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = fast_node("a", &store);
    let b = fast_node("b", &store);
    a.identity().await.unwrap();
    b.identity().await.unwrap();
    tokio::time::sleep(Duration::from_millis(3_000)).await;
    for (id, node) in [("a", &a), ("b", &b)] {
        let body = store
            .get(&StorePath::from(format!("nodes/{id}")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(
            String::from_utf8_lossy(&body).contains("\nlive "),
            "{id} fenced"
        );
        assert_eq!(node.not_ready(), None, "{id}");
    }
}

/// `cluster_tls` trusts only live nodes' pins: a fenced node's pins are
/// dropped at the next listing, as a deleted heartbeat's are.
#[tokio::test]
async fn a_fenced_node_s_pins_are_not_trusted() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut config = ClusterConfig::new("a", "http://a", Arc::clone(&store));
    config.heartbeat = Duration::from_millis(100);
    config.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    config.cluster_tls = Some(ClusterTlsConfig::new("https://a.test:7443"));
    let a = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    a.identity().await.unwrap();
    let pin = "ab".repeat(32);
    let body = |state: &str| {
        format!(
            "wasmkeep-node 3\n{state} beef\n{}\nb\nhttp://b\n0.0.0\nt/\nhttps://b.test:7443\n{pin}\nserving\n\ntake\n",
            now_ms()
        )
    };
    let key = StorePath::from("nodes/b");
    store
        .put(&key, body("live").into_bytes().into())
        .await
        .unwrap();
    a.refresh_view(true).await;
    assert_eq!(a.check_tls_target("https://b.test:7443"), Ok(()));
    store
        .put(&key, body("fenced").into_bytes().into())
        .await
        .unwrap();
    a.refresh_view(true).await;
    assert!(a.check_tls_target("https://b.test:7443").is_err());
}

/// A fence goes out only while this node's lease has room for the whole
/// fence (its read and its write, one attempt timeout): one started just
/// before the lease runs out could be sent after it did (ADR 0020,
/// section 2: only while valid). The node here never gets past its start
/// check (a 3 h ttl), so the test sets its validity by hand.
#[tokio::test]
async fn no_fence_is_started_without_room_left_in_the_lease() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut config = ClusterConfig::new("a", "http://a", Arc::clone(&store));
    config.heartbeat = Duration::from_secs(3_600);
    config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 1_000).unwrap();
    let a = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    let attempt = u64::try_from(a.lease_attempt.as_millis()).unwrap();
    // b last wrote 4 h ago: stale by ttl plus grace.
    let at = now_ms() - 4 * 3_600_000;
    let key = StorePath::from("nodes/b");
    store
        .put(&key, live_lease_body("b", "beef", at).into_bytes().into())
        .await
        .unwrap();
    let all = a.membership.heartbeats().await.unwrap();
    // Watched long enough, by a node valid long enough.
    let prime = |valid_for: u64| {
        let now = now_ms();
        let mut w = a.fence_watch.lock().unwrap();
        let long_ago = now - 10 * attempt - 10_800_000;
        w.own_lease(Some(now + 100 * attempt), long_ago);
        w.observe("b", "beef", at, long_ago);
        drop(w);
        a.validity.set_until(Some(now + valid_for));
    };
    let state = async || {
        let body = store.get(&key).await.unwrap().bytes().await.unwrap();
        String::from_utf8(body.to_vec()).unwrap()
    };
    prime(attempt / 2);
    a.fence_stale_nodes(&all).await;
    assert!(
        state().await.contains("\nlive "),
        "fenced with too little room"
    );
    prime(attempt * 4);
    a.fence_stale_nodes(&all).await;
    assert!(state().await.contains("\nfenced "), "not fenced with room");
}

/// A fence whose write hangs (a slow bucket) does not hold up the
/// listing: the view, and with it the pins peers' handshakes are checked
/// against, is refreshed while the fence waits on its own task (review
/// finding B3: one listing iteration could outlast the heartbeat horizon,
/// and peers refused each other's handshakes meanwhile).
#[tokio::test]
async fn a_hung_fence_does_not_hold_up_the_listing() {
    let gate = Arc::new(HangOneLeaseWrite {
        inner: Arc::new(object_store::memory::InMemory::new()),
        hang: std::sync::atomic::AtomicBool::new(false),
        slow_owner_reads_ms: std::sync::atomic::AtomicU64::new(0),
        fail_releases: std::sync::atomic::AtomicU32::new(0),
        lose_release_replies: std::sync::atomic::AtomicU32::new(0),
        slow_owner_writes_ms: std::sync::atomic::AtomicU64::new(0),
        owner_writes: std::sync::atomic::AtomicU64::new(0),
    });
    let store: Arc<dyn ObjectStore> = Arc::clone(&gate) as Arc<dyn ObjectStore>;
    // As in the room test: a 3 h ttl, so the node never passes its start
    // check (nothing runs in the background) and its validity is set here.
    let mut config = ClusterConfig::new("a", "http://a", Arc::clone(&store));
    config.heartbeat = Duration::from_secs(3_600);
    config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 1_000).unwrap();
    let a = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    let attempt = u64::try_from(a.lease_attempt.as_millis()).unwrap();
    let at = now_ms() - 4 * 3_600_000;
    store
        .put(
            &StorePath::from("nodes/b"),
            live_lease_body("b", "beef", at).into_bytes().into(),
        )
        .await
        .unwrap();
    let now = now_ms();
    {
        let mut w = a.fence_watch.lock().unwrap();
        let long_ago = now - 10 * attempt - 10_800_000;
        w.own_lease(Some(now + 100 * attempt), long_ago);
        w.observe("b", "beef", at, long_ago);
    }
    a.validity.set_until(Some(now + 4 * attempt));
    // b's fence write will hang.
    gate.hang.store(true, std::sync::atomic::Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(1), a.refresh_view(true))
        .await
        .expect("the listing waited for the fence");
    let started = std::time::Instant::now();
    while gate.hang.load(std::sync::atomic::Ordering::SeqCst) {
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the fence never ran"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The lease epoch a fence is checked against is the one when the node
/// was found due: a lapse between the listing and the fence (the fence
/// task busy elsewhere) restarts the watch, even if this node's lease is
/// valid again by then (ADR 0020, section 2: counted from its first
/// write after any lapse).
#[tokio::test]
async fn a_lapse_after_a_node_was_found_due_cancels_its_fence() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut config = ClusterConfig::new("a", "http://a", Arc::clone(&store));
    config.heartbeat = Duration::from_secs(3_600);
    config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 1_000).unwrap();
    let a = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    let attempt = u64::try_from(a.lease_attempt.as_millis()).unwrap();
    let at = now_ms() - 4 * 3_600_000;
    let key = StorePath::from("nodes/b");
    store
        .put(&key, live_lease_body("b", "beef", at).into_bytes().into())
        .await
        .unwrap();
    let now = now_ms();
    {
        let mut w = a.fence_watch.lock().unwrap();
        let long_ago = now - 10 * attempt - 10_800_000;
        w.own_lease(Some(now + 100 * attempt), long_ago);
        w.observe("b", "beef", at, long_ago);
    }
    a.validity.set_until(Some(now + 4 * attempt));
    let all = a.membership.heartbeats().await.unwrap();
    let read: Vec<String> = all.iter().map(|n| n.id.clone()).collect();
    let due = a.fences_due(&all, &read);
    assert!(!due.is_empty(), "b found due");
    // A lapse, and the lease valid again, before the fence task gets to it.
    a.validity.bump_epoch();
    a.fence_due(due).await;
    let body = store.get(&key).await.unwrap().bytes().await.unwrap();
    assert!(
        String::from_utf8_lossy(&body).contains("\nlive "),
        "fenced after a lapse since it was found due"
    );
}

/// Waits until `node`'s lease is valid.
async fn wait_lease_valid(node: &Cluster) {
    let started = std::time::Instant::now();
    while !node.node_lease_valid() {
        assert!(started.elapsed() < Duration::from_secs(3), "never valid");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// ADR 0020, section 3: an acquire with no answer may have landed. The
/// resolver releases what it wrote when no hold of the actor lives here,
/// and leaves alone a later acquisition that landed at the very
/// generation the unresolved one tried (two owners otherwise).
#[tokio::test]
async fn the_resolver_releases_a_landed_acquire_but_never_a_live_hold() {
    use wasmkeep_cluster::node_lease::OwnerRecord;
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let instance = a.membership.instance().unwrap().to_owned();
    let record = |holder: Option<&str>, generation| {
        OwnerRecord {
            holder: holder.map(|i| ("a".to_owned(), i.to_owned())),
            generation,
            recorded: None,
        }
        .encode()
    };
    let read = async |key: &str| {
        let body = store
            .get(&StorePath::from(key))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        OwnerRecord::decode(&body).unwrap()
    };

    // Landed, and nothing holds it here: released, generation kept.
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let key = a.owner_key(&x);
    store
        .put(
            &StorePath::from(key.as_str()),
            record(Some(&instance), 5).into(),
        )
        .await
        .unwrap();
    a.pending.push(PendingEntry::Unresolved {
        key: key.clone(),
        generation: 5,
        before: Some(OwnerRecord {
            holder: None,
            generation: 4,
            recorded: None,
        }),
        until_ms: now_ms() + 30_000,
    });
    assert!(!a.nothing_pending());
    a.retry_pending().await;
    assert_eq!(
        read(&key).await,
        OwnerRecord {
            holder: None,
            generation: 5,
            recorded: None,
        }
    );
    assert!(a.nothing_pending());

    // Did not land (the record names nobody at 5), and a second acquisition
    // then landed at 6, the generation the first one tried: the resolver
    // must leave that hold alone.
    a.pending.push(PendingEntry::Unresolved {
        key: key.clone(),
        generation: 6,
        before: Some(OwnerRecord {
            holder: None,
            generation: 5,
            recorded: None,
        }),
        until_ms: now_ms() + 30_000,
    });
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    assert_eq!(hold.generation(), Some(6));
    a.retry_pending().await;
    assert_eq!(
        read(&key).await,
        OwnerRecord {
            holder: Some(("a".to_owned(), instance.clone())),
            generation: 6,
            recorded: None,
        }
    );
    assert!(hold.is_valid());
    assert!(a.nothing_pending());
}

/// ADR 0020, section 3: an unresolved acquire whose record has not changed
/// stays pending while its wait lasts (a drain is not drained), and a
/// write that lands after the resolver's first read is released; past its
/// wait, an unchanged record ends it. A clean stop does not wait for it.
#[tokio::test]
async fn the_resolver_waits_for_a_write_that_lands_late() {
    use wasmkeep_cluster::node_lease::OwnerRecord;
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let instance = a.membership.instance().unwrap().to_owned();
    let put = async |key: &str, record: OwnerRecord| {
        store
            .put(&StorePath::from(key), record.encode().into())
            .await
            .unwrap();
    };
    let read = async |key: &str| {
        let body = store
            .get(&StorePath::from(key))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        OwnerRecord::decode(&body).unwrap()
    };
    let nobody = |generation| OwnerRecord {
        holder: None,
        generation,
        recorded: None,
    };
    let key = a.owner_key(&ActorAddress::new("t", "c", "late").unwrap());
    put(&key, nobody(4)).await;
    a.pending.push(PendingEntry::Unresolved {
        key: key.clone(),
        generation: 5,
        before: Some(nobody(4)),
        until_ms: now_ms() + 30_000,
    });
    a.retry_pending().await;
    assert!(!a.nothing_pending(), "ended on an unchanged record");
    assert!(
        a.releases_done(Duration::from_millis(100)).await,
        "a clean stop does not wait for an unresolved acquire"
    );
    // The write lands now, after that read.
    put(
        &key,
        OwnerRecord {
            holder: Some(("a".to_owned(), instance.clone())),
            generation: 5,
            recorded: None,
        },
    )
    .await;
    a.retry_pending().await;
    assert_eq!(
        read(&key).await,
        nobody(5),
        "the late write was not released"
    );
    assert!(a.nothing_pending());

    // Unchanged past its wait: ended, the record left alone.
    a.pending.push(PendingEntry::Unresolved {
        key: key.clone(),
        generation: 6,
        before: Some(nobody(5)),
        until_ms: now_ms(),
    });
    a.retry_pending().await;
    assert!(a.nothing_pending());
    assert_eq!(read(&key).await, nobody(5));
}

/// ADR 0020, section 1: a lease that ran out ends every hold at once,
/// into the pending releases, and no hold is valid again even once the
/// lease is: the node acquires afresh, at a new generation.
#[tokio::test]
async fn a_lapsed_lease_ends_every_hold_for_good() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let first = hold.generation().unwrap();
    assert!(hold.is_valid());
    // A lapse as the lease task sees it: its validity ran out before the
    // next write landed.
    a.validity.set_until(Some(now_ms().saturating_sub(1)));
    wait_lease_valid(&a).await; // the next write renews it
    assert!(!hold.is_valid(), "a hold from before the lapse acts again");
    assert_eq!(hold.generation(), None, "not ended at the lapse");
    drop(hold);
    let again = LeaseHold::acquire(&a, &x).await.unwrap();
    assert!(again.generation().unwrap() > first);
    assert!(again.is_valid());
}

/// While the resolver works on an entry, the set is not empty: a drain must
/// not report `drained`, nor a clean stop go on, in the middle of it.
#[tokio::test]
async fn the_pending_set_is_not_empty_while_an_entry_is_being_resolved() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let key = a.owner_key(&x);
    // Its wait is over: the next read that finds no record ends it.
    a.pending.push(PendingEntry::Unresolved {
        key: key.clone(),
        generation: 1,
        before: None,
        until_ms: now_ms(),
    });
    // The resolver waits for the actor's lock, held here.
    let lock = a.pending.lock_of(&key);
    let held = lock.lock().await;
    let pass = tokio::spawn({
        let a = Arc::clone(&a);
        async move { a.retry_pending().await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !a.nothing_pending(),
        "empty while an entry is being resolved"
    );
    drop(held);
    pass.await.unwrap();
    // No record: the acquire never landed, the entry ends.
    assert!(a.nothing_pending());
}

/// A lapse on a node with many holds parks every one as a pending
/// release; over a bucket answering in 20 ms, 500 of them land within two
/// seconds (the set is retried with bounded concurrency, not one entry at
/// a time), so a drain or a shutdown is not held up for long.
#[tokio::test(flavor = "multi_thread")]
async fn many_parked_releases_land_quickly() {
    let gate = Arc::new(HangOneLeaseWrite {
        inner: Arc::new(object_store::memory::InMemory::new()),
        hang: std::sync::atomic::AtomicBool::new(false),
        slow_owner_reads_ms: std::sync::atomic::AtomicU64::new(0),
        fail_releases: std::sync::atomic::AtomicU32::new(0),
        lose_release_replies: std::sync::atomic::AtomicU32::new(0),
        slow_owner_writes_ms: std::sync::atomic::AtomicU64::new(0),
        owner_writes: std::sync::atomic::AtomicU64::new(0),
    });
    let store: Arc<dyn ObjectStore> = Arc::clone(&gate) as Arc<dyn ObjectStore>;
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let mut holds = Vec::new();
    for i in 0..500 {
        let x = ActorAddress::new("t", "c", format!("x{i}")).unwrap();
        holds.push(LeaseHold::acquire(&a, &x).await.unwrap());
    }
    gate.slow_owner_writes_ms
        .store(20, std::sync::atomic::Ordering::SeqCst);
    // Every hold ends into a pending release at once, as a lapse ends them.
    a.pending.end_all(true);
    drop(holds);
    let started = std::time::Instant::now();
    assert!(
        // Well under the ten seconds one at a time would take, with room
        // for a loaded machine.
        a.releases_done(Duration::from_secs(2)).await,
        "500 parked releases still pending after {:?}",
        started.elapsed()
    );
    for i in [0, 250, 499] {
        let key = a.owner_key(&ActorAddress::new("t", "c", format!("x{i}")).unwrap());
        let body = store
            .get(&StorePath::from(key))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let record = wasmkeep_cluster::node_lease::OwnerRecord::decode(&body).unwrap();
        assert_eq!(record.holder, None, "x{i} released");
    }
}

/// A release made in the foreground waits a lease attempt, at most a
/// second (2.67 s with the default timing, so the cap applies).
#[test]
fn a_foreground_release_waits_at_most_a_second() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let config = ClusterConfig::new("a", "http://a", store);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _guard = rt.enter();
    let a = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    assert_eq!(a.release_attempt(), Duration::from_secs(1));
    let other: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let fast = fast_node("b", &other);
    assert!(fast.release_attempt() < Duration::from_secs(1));
}

/// A bucket refusing every write: one pass over many pending releases
/// stops starting new ones after a run of refusals, and puts the rest
/// back, instead of sending them all.
#[tokio::test(flavor = "multi_thread")]
async fn a_pass_over_a_refusing_bucket_stops_early() {
    let gate = Arc::new(HangOneLeaseWrite {
        inner: Arc::new(object_store::memory::InMemory::new()),
        hang: std::sync::atomic::AtomicBool::new(false),
        slow_owner_reads_ms: std::sync::atomic::AtomicU64::new(0),
        fail_releases: std::sync::atomic::AtomicU32::new(0),
        lose_release_replies: std::sync::atomic::AtomicU32::new(0),
        slow_owner_writes_ms: std::sync::atomic::AtomicU64::new(0),
        owner_writes: std::sync::atomic::AtomicU64::new(0),
    });
    let store: Arc<dyn ObjectStore> = Arc::clone(&gate) as Arc<dyn ObjectStore>;
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let mut holds = Vec::new();
    for i in 0..300 {
        let x = ActorAddress::new("t", "c", format!("x{i}")).unwrap();
        holds.push(LeaseHold::acquire(&a, &x).await.unwrap());
    }
    // Ownership-record writes now take longer than any attempt: refused.
    gate.slow_owner_writes_ms
        .store(5_000, std::sync::atomic::Ordering::SeqCst);
    a.pending.end_all(true);
    drop(holds);
    let writes = || gate.owner_writes.load(std::sync::atomic::Ordering::SeqCst);
    let tried = writes();
    a.retry_pending().await;
    let sent = writes() - tried;
    assert!(
        sent < 100,
        "{sent} writes sent to a refusing bucket in one pass"
    );
    assert!(!a.nothing_pending(), "the rest went back");
}

/// An actor's lock leaves the map when its last handle goes: after 200
/// acquisitions, and two handles of one actor dropped together, none is
/// left (no scan of the map on each acquisition either).
#[tokio::test(flavor = "multi_thread")]
async fn actor_locks_leave_the_map_with_their_last_handle() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let mut holds = Vec::new();
    for i in 0..200 {
        let x = ActorAddress::new("t", "c", format!("x{i}")).unwrap();
        holds.push(LeaseHold::acquire(&a, &x).await.unwrap());
    }
    assert_eq!(a.pending.locks_in_use(), 0);
    let (one, two) = (a.pending.lock_of("k"), a.pending.lock_of("k"));
    assert_eq!(a.pending.locks_in_use(), 1);
    drop(one);
    assert_eq!(
        a.pending.locks_in_use(),
        1,
        "the other handle still uses it"
    );
    drop(two);
    assert_eq!(a.pending.locks_in_use(), 0);
    drop(holds);
}

/// An acquisition takes the actor's lock from its read until its hold is
/// registered: one waits while the resolver (here, the test) holds it.
#[tokio::test]
async fn an_acquisition_waits_for_the_actor_s_lock() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let lock = a.pending.lock_of(&a.owner_key(&x));
    let held = lock.lock().await;
    let acquiring = tokio::spawn({
        let (a, x) = (Arc::clone(&a), x.clone());
        async move { LeaseHold::acquire(&a, &x).await.is_ok() }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !acquiring.is_finished(),
        "acquired without the actor's lock"
    );
    drop(held);
    assert!(acquiring.await.unwrap());
}

/// A hold of an earlier epoch is never valid, even with its record hold
/// still in place and the lease valid: what keeps a hold acquired while a
/// lapse was being handled (after `end_all` ran) from acting.
#[tokio::test]
async fn a_hold_from_before_a_lapse_is_never_valid_again() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    assert!(hold.is_valid());
    a.validity.bump_epoch();
    assert!(hold.generation().is_some(), "still held here");
    assert!(a.node_lease_valid());
    assert!(!hold.is_valid());
}

/// Node `z`'s heartbeat in the format from before node leases, written at
/// `at`.
async fn old_format_heartbeat(store: &Arc<dyn ObjectStore>, at: u64) {
    let body = format!("wasmkeep-node 2\nz\nhttp://z\n0.0.0\n{at}\nt/\n");
    store
        .put(&StorePath::from("nodes/z"), body.into_bytes().into())
        .await
        .unwrap();
}

/// ADR 0020, section 6: a new node waits while an old-format heartbeat is
/// younger than its horizon plus a lease's ttl and grace (0.4 + 1.2 s
/// here): its node may still renew per-actor leases. A stale one does not
/// hold the start beyond the start check's own wait.
#[tokio::test]
async fn the_start_guard_waits_for_a_recent_old_format_heartbeat() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    old_format_heartbeat(&store, now_ms()).await;
    let started = std::time::Instant::now();
    fast_node("a", &store).identity().await.unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(1_500),
        "{:?}",
        started.elapsed()
    );

    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    old_format_heartbeat(&store, now_ms() - 10_000).await;
    let started = std::time::Instant::now();
    fast_node("a", &store).identity().await.unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(1_450),
        "{:?}",
        started.elapsed()
    );
}

/// An old-format heartbeat rewritten while the new node starts: an old
/// version runs, and the new node does not start.
#[tokio::test]
async fn the_start_guard_refuses_while_an_old_version_runs() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    old_format_heartbeat(&store, now_ms()).await;
    let a = fast_node("a", &store);
    tokio::time::sleep(Duration::from_millis(300)).await;
    old_format_heartbeat(&store, now_ms()).await;
    let why = a.identity().await.unwrap_err();
    assert!(why.contains("from before node leases"), "{why}");
    assert!(
        store.head(&StorePath::from("nodes/a")).await.is_err(),
        "the refused node wrote its lease"
    );
}

/// One dated ahead of this clock lapses on this node's own count: its
/// horizon plus ttl and grace plus the allowed skew (1.7 s here) after it
/// was first read, not whenever its date passes.
#[tokio::test]
async fn an_old_format_heartbeat_from_the_future_lapses_on_this_node_s_count() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    old_format_heartbeat(&store, now_ms() + 3_600_000).await;
    let started = std::time::Instant::now();
    fast_node("a", &store).identity().await.unwrap();
    let took = started.elapsed();
    assert!(
        (Duration::from_millis(1_600)..Duration::from_secs(3)).contains(&took),
        "{took:?}"
    );
}

/// ADR 0020, section 1: a fence over the process the start check first read
/// ends the wait once that object's kept `written_at` is ttl plus grace
/// (1.2 s) old, not a full wait after the first read; and a node lease
/// needs no age rule: watched unchanged for ttl plus grace is enough.
#[tokio::test]
async fn a_fence_over_the_first_read_process_shortens_the_start_check() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let key = StorePath::from("nodes/a");
    let at = now_ms() - 1_000;
    let body = |state: &str| {
        format!(
            "wasmkeep-node 3\n{state} beef\n{at}\na\nhttp://old\n0.0.0\nt/\n\n\nserving\n\ntake\n"
        )
    };
    store
        .put(&key, body("live").into_bytes().into())
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let a = fast_node("a", &store);
    tokio::time::sleep(Duration::from_millis(100)).await;
    store
        .put(&key, body("fenced").into_bytes().into())
        .await
        .unwrap();
    a.identity().await.unwrap();
    let took = started.elapsed();
    // At about 0.2 s (its written_at plus 1.2 s), well before 1.2 s.
    assert!(took < Duration::from_millis(800), "{took:?}");

    // Live, written just now and left unchanged: the start check's own
    // wait, ttl plus grace from its first read (1.2 s), not the age rule's
    // horizon plus ttl and grace (1.6 s) that old formats keep.
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let fresh = format!(
        "wasmkeep-node 3\nlive beef\n{}\na\nhttp://old\n0.0.0\nt/\n\n\nserving\n\ntake\n",
        now_ms()
    );
    store.put(&key, fresh.into_bytes().into()).await.unwrap();
    let started = std::time::Instant::now();
    fast_node("a", &store).identity().await.unwrap();
    let took = started.elapsed();
    assert!(
        (Duration::from_millis(1_200)..Duration::from_millis(1_550)).contains(&took),
        "{took:?}"
    );
}

/// The shortcut keeps its bound: a fence over the first-read process ends
/// the wait at the fence's kept `written_at` plus ttl and grace, not when
/// the fence is seen; and a first read already fenced or released waits
/// until then too (no sooner), with nothing more once that passed.
#[tokio::test]
async fn the_start_check_shortcut_keeps_its_bound() {
    let key = StorePath::from("nodes/a");
    let body = |state: &str, at: u64| {
        format!(
            "wasmkeep-node 3\n{state} beef\n{at}\na\nhttp://old\n0.0.0\nt/\n\n\nserving\n\ntake\n"
        )
    };
    // Seen live, then fenced: last live write 600 ms before the start, so
    // the check goes on at about 600 ms, not at once, not at 1.2 s.
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let at = now_ms() - 600;
    store
        .put(&key, body("live", at).into_bytes().into())
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let a = fast_node("a", &store);
    tokio::time::sleep(Duration::from_millis(100)).await;
    store
        .put(&key, body("fenced", at).into_bytes().into())
        .await
        .unwrap();
    a.identity().await.unwrap();
    let took = started.elapsed();
    assert!(
        (Duration::from_millis(550)..Duration::from_millis(1_100)).contains(&took),
        "{took:?}"
    );
    // Fenced or released at the first read, 900 ms before the start: at
    // about 300 ms.
    for state in ["fenced", "released"] {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        store
            .put(&key, body(state, now_ms() - 900).into_bytes().into())
            .await
            .unwrap();
        let started = std::time::Instant::now();
        fast_node("a", &store).identity().await.unwrap();
        let took = started.elapsed();
        assert!(
            (Duration::from_millis(250)..Duration::from_millis(800)).contains(&took),
            "{state}: {took:?}"
        );
    }
}

/// While running, an old-format heartbeat is reported as an old version
/// running when rewritten between listings (even with an old date), or
/// dated ahead until the guard's wait passed on this node's count; a
/// stale, unchanged one is a retired node's.
#[tokio::test]
async fn an_old_version_running_is_told_from_a_retired_one() {
    let a = node("a");
    let logged = || {
        let mut l = a.refusal_logged.lock().unwrap();
        l.remove("old version running").is_some()
    };
    let old = |at: u64| {
        let body = format!("wasmkeep-node 2\nz\nhttp://z\n0.0.0\n{at}\nt/\n");
        let mut n = NodeInfo::decode(body.as_bytes()).unwrap();
        n.written_at_ms = at;
        n
    };
    let stale = now_ms() - 600_000;
    a.note_old_versions(&[old(stale)]);
    assert!(!logged(), "a retired node's heartbeat reported as running");
    a.note_old_versions(&[old(stale + 1)]);
    assert!(logged(), "a rewritten one not reported");
    a.note_old_versions(&[old(stale + 1)]);
    assert!(!logged());

    // Dated ahead: running for the guard's wait from when first seen (a
    // default node: 10 s horizon, 15 s, 1 s of skew), here just seen.
    let ahead = now_ms() + 3_600_000;
    a.note_old_versions(&[old(ahead)]);
    assert!(logged());
    *a.old_heartbeats.lock().unwrap().get_mut("z").unwrap() =
        (ahead, tokio::time::Instant::now() - Duration::from_secs(30));
    a.note_old_versions(&[old(ahead)]);
    assert!(!logged(), "dated ahead and reported past the guard's wait");
}

/// A live node lease dated ahead of this clock by more than the allowed
/// skew never goes stale here (it cannot be fenced, and its actors never
/// free): logged, at most once a minute. One within the skew is not.
#[tokio::test]
async fn a_node_lease_dated_ahead_is_logged() {
    let a = node("a");
    let logged = || {
        let mut l = a.refusal_logged.lock().unwrap();
        l.remove("node lease dated ahead").is_some()
    };
    let lease = |at: u64| NodeInfo {
        id: "z".into(),
        address: "http://z:8080".into(),
        version: String::new(),
        written_at_ms: at,
        app: String::new(),
        cluster_address: None,
        tls_pins: Vec::new(),
        drain: None,
        reads_secrets: false,
        can_take: false,
        can_front: false,
        can_handoff: false,
        can_end: false,
        can_flush: false,
        can_rate_limits: false,
        can_assets: false,
        can_blobs: false,
        can_restore: false,
        can_actor_restore: false,
        instance: Some("iz".into()),
        lease: Some(wasmkeep_cluster::node_lease::NodeState::Live),
        load: None,
        can_recorded_head: false,
        can_multi_app: false,
        can_place: false,
        process: None,
    };
    a.note_future_leases(&[lease(now_ms())]);
    assert!(!logged(), "a lease within the skew reported");
    a.note_future_leases(&[lease(now_ms() + 3_600_000)]);
    assert!(logged(), "a lease an hour ahead not reported");
    // Its own lease is never another node's.
    let mut own = lease(now_ms() + 3_600_000);
    own.id = "a".into();
    a.note_future_leases(&[own]);
    assert!(!logged(), "this node's own lease reported");
    // Fenced with its date still ahead: its actors stay held, so still
    // reported; one fenced in the past is not.
    let mut fenced = lease(now_ms() + 3_600_000);
    fenced.lease = Some(wasmkeep_cluster::node_lease::NodeState::Fenced);
    a.note_future_leases(&[fenced.clone()]);
    assert!(logged(), "a fenced lease dated ahead not reported");
    fenced.written_at_ms = now_ms() - 60_000;
    a.note_future_leases(&[fenced]);
    assert!(!logged(), "a fenced lease from the past reported");
}

/// An operator's fence created over a missing object while a new process
/// checks its id (`wasmkeep cluster fence` on a node whose heartbeat was
/// deleted) is not another process running: the check goes on.
#[tokio::test]
async fn an_operator_fence_over_no_object_is_not_a_twin() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = fast_node("a", &store);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let fence = format!("wasmkeep-node 3\nfenced -\n{}\n", now_ms());
    store
        .put(&StorePath::from("nodes/a"), fence.into_bytes().into())
        .await
        .unwrap();
    a.identity().await.unwrap();
}

/// ADR 0020, amendment on lease reads: a process starting over an
/// operator's created fence (no instance) writes its first `live` body no
/// earlier than the fence's `written_at` plus ttl and grace (1.2 s) on its
/// clock, whether the fence was there at the first read or appeared over
/// no object during the check, so its bodies are dated after the fence as
/// `older_than` orders them. Here the fence is dated 400 ms ahead (an
/// operator's clock), and, created late in the check, after the check's own
/// wait would have ended.
#[tokio::test]
async fn a_start_over_a_created_fence_dates_its_body_after_it() {
    let key = StorePath::from("nodes/a");
    let first_body = async |store: &Arc<dyn ObjectStore>| {
        let body = store.get(&key).await.unwrap().bytes().await.unwrap();
        wasmkeep_cluster::node_lease::NodeLeaseObject::decode(&body).unwrap()
    };
    // There at the first read.
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let fenced_at = now_ms() + 400;
    let fence = format!("wasmkeep-node 3\nfenced -\n{fenced_at}\n");
    store.put(&key, fence.into_bytes().into()).await.unwrap();
    fast_node("a", &store).identity().await.unwrap();
    let live = first_body(&store).await;
    assert_eq!(live.state, wasmkeep_cluster::node_lease::NodeState::Live);
    assert!(
        live.written_at_ms >= fenced_at + 1_200,
        "{} < {fenced_at} + 1200",
        live.written_at_ms
    );
    // Created over no object 1 s into the check.
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = fast_node("a", &store);
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    let fenced_at = now_ms();
    let fence = format!("wasmkeep-node 3\nfenced -\n{fenced_at}\n");
    store.put(&key, fence.into_bytes().into()).await.unwrap();
    a.identity().await.unwrap();
    let live = first_body(&store).await;
    assert_eq!(live.state, wasmkeep_cluster::node_lease::NodeState::Live);
    assert!(
        live.written_at_ms >= fenced_at + 1_200,
        "{} < {fenced_at} + 1200",
        live.written_at_ms
    );
}

/// Review finding B5: an acquisition asks, right before its write, whether
/// a hold of the epoch it read may act, not whether the lease is valid by
/// the clock. Here the epoch moves while its read of the record is in
/// flight (as a lapse and a renewal would): no record is written.
#[tokio::test]
async fn an_acquisition_whose_epoch_moved_writes_nothing() {
    let gate = Arc::new(HangOneLeaseWrite {
        inner: Arc::new(object_store::memory::InMemory::new()),
        hang: std::sync::atomic::AtomicBool::new(false),
        slow_owner_reads_ms: std::sync::atomic::AtomicU64::new(0),
        fail_releases: std::sync::atomic::AtomicU32::new(0),
        lose_release_replies: std::sync::atomic::AtomicU32::new(0),
        slow_owner_writes_ms: std::sync::atomic::AtomicU64::new(0),
        owner_writes: std::sync::atomic::AtomicU64::new(0),
    });
    let store: Arc<dyn ObjectStore> = Arc::clone(&gate) as Arc<dyn ObjectStore>;
    let mut config = ClusterConfig::new("a", "http://a", Arc::clone(&store));
    config.heartbeat = Duration::from_millis(100);
    config.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    let a = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    gate.slow_owner_reads_ms
        .store(300, std::sync::atomic::Ordering::SeqCst);
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let acquiring = tokio::spawn({
        let (a, x) = (Arc::clone(&a), x.clone());
        async move { LeaseHold::acquire(&a, &x).await.is_ok() }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    a.validity.bump_epoch();
    assert!(!acquiring.await.unwrap(), "acquired across an epoch change");
    gate.slow_owner_reads_ms
        .store(0, std::sync::atomic::Ordering::SeqCst);
    let key = a.owner_key(&x);
    assert!(
        store.head(&StorePath::from(key.as_str())).await.is_err(),
        "a record was written for a hold dead from the start"
    );
}

#[test]
fn the_default_failover_wait_follows_the_lease_timing() {
    // ttl 10 s + grace 5 s, the fence watch (3.3 s + 2.7 s), one listing
    // (3 s) and a second of spread.
    assert_eq!(
        failover_wait_default(&LeaseConfig::default_safe(), DEFAULT_HEARTBEAT),
        Duration::from_millis(24_999)
    );
    let config = ClusterConfig::new(
        "a",
        "http://a",
        Arc::new(object_store::memory::InMemory::new()),
    );
    assert_eq!(config.failover_wait, None, "on by default, derived");
    assert_eq!(config.failover_wait_max, 2_500);
}

/// Node `id`'s lease as a listing shows it, written `age` ago.
fn listed(id: &str, state: wasmkeep_cluster::node_lease::NodeState, age: Duration) -> NodeInfo {
    NodeInfo {
        id: id.to_owned(),
        address: format!("http://{id}"),
        version: String::new(),
        written_at_ms: now_ms() - u64::try_from(age.as_millis()).unwrap(),
        app: "t/".to_owned(),
        cluster_address: None,
        tls_pins: Vec::new(),
        drain: None,
        reads_secrets: false,
        can_take: false,
        can_front: false,
        can_handoff: false,
        can_end: false,
        can_flush: false,
        can_rate_limits: false,
        can_assets: false,
        can_blobs: false,
        can_restore: false,
        can_actor_restore: false,
        instance: Some("0011223344556677".to_owned()),
        lease: Some(state),
        load: None,
        can_recorded_head: false,
        can_multi_app: false,
        can_place: false,
        process: None,
    }
}

/// How old node a's lease is in [`waiting_node`]'s listing: past the
/// horizon (61 s), its actors free 600 ms from now.
const STALE: Duration = Duration::from_millis(64_400);

/// Node b keeping at most `max` requests, having listed node a's lease
/// `live` and [`STALE`]. Its timing is long (ttl 60 s, grace 5 s), so its
/// own start check outlasts the test: it lists and fences nothing, and
/// the test alone says what it knows. Its lease is set valid by hand.
fn waiting_node(max: usize, wait: Duration) -> Arc<Cluster> {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut config = ClusterConfig::new("b", "http://b", store);
    config.heartbeat = Duration::from_secs(20);
    config.lease = LeaseConfig::new(60_000, 5_000, 2_000, 1_000).unwrap();
    config.failover_wait = Some(wait);
    config.failover_wait_max = max;
    let b = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    b.validity.set_until(Some(now_ms() + 60_000));
    let stale = listed("a", wasmkeep_cluster::node_lease::NodeState::Live, STALE);
    b.failover.observe(&[stale], now_ms());
    b
}

#[tokio::test]
async fn kept_requests_are_capped_and_a_dropped_one_frees_its_place() {
    use wasmkeep_cluster::failover::Verdict;
    let b = waiting_node(2, Duration::from_secs(30));
    assert!(matches!(b.failover.verdict("a"), Verdict::Stale { .. }));
    let actor = |name: &str| ActorAddress::new("t", "c", name).unwrap();
    let deadline = b.failover.deadline_ms().unwrap();
    let keep = |name: &str| b.keep("a", &actor(name), deadline, Kept::Request);
    let (x, y) = (keep("x").unwrap(), keep("y").unwrap());
    assert_eq!(b.failover.kept(), 2);
    assert!(keep("z").is_none(), "over the node's cap");
    // A client that leaves drops its request, and the place with it.
    drop(x);
    assert_eq!(b.failover.kept(), 1);
    let z = keep("z").unwrap();
    drop((y, z));
    assert_eq!(b.failover.kept(), 0);

    // Per actor too, whatever the node's cap leaves.
    let b = waiting_node(1_000, Duration::from_secs(30));
    let deadline = b.failover.deadline_ms().unwrap();
    let kept: Vec<_> = (0..64)
        .map(|_| b.keep("a", &actor("x"), deadline, Kept::WebSocket).unwrap())
        .collect();
    assert!(
        b.keep("a", &actor("x"), deadline, Kept::WebSocket)
            .is_none()
    );
    assert!(
        b.keep("a", &actor("y"), deadline, Kept::WebSocket)
            .is_some()
    );
    drop(kept);
    assert_eq!(b.failover.kept(), 0);
}

#[tokio::test]
async fn nothing_is_kept_with_the_wait_off_for_a_live_owner_or_past_the_deadline() {
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let off = waiting_node(10, Duration::ZERO);
    assert_eq!(off.failover.deadline_ms(), None);
    assert!(
        off.keep("a", &x, now_ms() + 30_000, Kept::Request)
            .is_none()
    );

    let b = waiting_node(10, Duration::from_secs(30));
    let deadline = b.failover.deadline_ms().unwrap();
    // A node that renews, one never listed, this node itself.
    let live = listed(
        "c",
        wasmkeep_cluster::node_lease::NodeState::Live,
        Duration::ZERO,
    );
    let stale = listed("a", wasmkeep_cluster::node_lease::NodeState::Live, STALE);
    // Twice: a later `written_at` than the listing before would show a
    // writing.
    b.failover.observe(&[stale.clone(), live.clone()], now_ms());
    b.failover.observe(&[stale, live], now_ms());
    for owner in ["c", "nobody", "b"] {
        assert!(
            b.keep(owner, &x, deadline, Kept::Request).is_none(),
            "{owner}"
        );
    }
    // a's actors are free 600 ms from now: a deadline before that cannot
    // be met by waiting.
    assert!(b.keep("a", &x, now_ms() + 300, Kept::Request).is_none());
    assert!(b.keep("a", &x, now_ms() + 900, Kept::Request).is_some());
    // Not while this node's own lease is not valid.
    b.validity.set_until(None);
    assert!(b.keep("a", &x, deadline, Kept::Request).is_none());
}

#[tokio::test]
async fn a_kept_request_gives_up_at_its_deadline_at_shutdown_and_when_the_lease_lapses() {
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let b = waiting_node(10, Duration::from_secs(30));
    // The deadline: nobody fences a, so nothing changes until then.
    let started = std::time::Instant::now();
    let mut kept = b.keep("a", &x, now_ms() + 900, Kept::Request).unwrap();
    assert_eq!(kept.wait().await, Waited::GiveUp);
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(850) && took < Duration::from_secs(5),
        "{took:?}"
    );
    drop(kept);

    let deadline = b.failover.deadline_ms().unwrap();
    let mut kept = b.keep("a", &x, deadline, Kept::Request).unwrap();
    let wait = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(kept.wait(), async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            b.validity.set_until(None);
        })
        .0
    });
    assert_eq!(wait.await.unwrap(), Waited::GiveUp, "the lease ran out");
    drop(kept);

    let b = waiting_node(10, Duration::from_secs(30));
    let deadline = b.failover.deadline_ms().unwrap();
    let mut kept = b.keep("a", &x, deadline, Kept::Request).unwrap();
    let wait = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(kept.wait(), async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            b.failover.close();
        })
        .0
    });
    assert_eq!(wait.await.unwrap(), Waited::GiveUp, "the node stops");
    drop(kept);
    assert!(
        b.keep("a", &x, deadline, Kept::Request).is_none(),
        "stopping"
    );
}

#[tokio::test]
async fn a_fence_or_a_listing_wakes_a_kept_request_and_a_renewal_releases_it() {
    use wasmkeep_cluster::node_lease::NodeState;
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let b = waiting_node(10, Duration::from_secs(30));
    let deadline = b.failover.deadline_ms().unwrap();

    // This node's own fence: asked again, without waiting for a listing.
    let mut kept = b.keep("a", &x, deadline, Kept::Request).unwrap();
    let wait = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(kept.wait(), async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            b.failover.nudge("a");
        })
        .0
    });
    assert_eq!(wait.await.unwrap(), Waited::Try);
    // Still held there: it waits again, and a listing showing a writing
    // again releases it to the ordinary answer.
    kept.refused();
    let wait = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(kept.wait(), async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            b.failover
                .observe(&[listed("a", NodeState::Live, Duration::ZERO)], now_ms());
        })
        .0
    });
    assert_eq!(wait.await.unwrap(), Waited::Leave);
    drop(kept);
    assert_eq!(b.failover.kept(), 0);
}

/// The mark behind a suspected peer relies on the HTTP client saying
/// `is_connect` only before a request was handed to a connection: for a
/// connection it could not open (checked here), and for a pooled one it
/// could not hand out (rare, not checked). To check again when hyper-util
/// moves.
#[tokio::test]
async fn only_a_connection_that_could_not_be_opened_counts_as_a_failed_connect() {
    let client = peer_client_builder(Duration::from_secs(1))
        .build::<_, http_body_util::Empty<bytes::Bytes>>(
            hyper_util::client::legacy::connect::HttpConnector::new(),
        );
    let get = |addr: std::net::SocketAddr| {
        client.request(
            Request::get(format!("http://{addr}/"))
                .body(http_body_util::Empty::new())
                .unwrap(),
        )
    };
    // Nothing listens: refused.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let refusing = listener.local_addr().unwrap();
    drop(listener);
    let error = get(refusing).await.unwrap_err();
    assert!(error.is_connect(), "{error:?}");
    // Accepted, then closed without an answer: the request may have been
    // read.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let closing = listener.local_addr().unwrap();
    tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
        }
    });
    let error = get(closing).await.unwrap_err();
    assert!(!error.is_connect(), "{error:?}");
}

/// Signatures of take and alarm hand-off calls are remembered apart: a
/// flood of other node calls (or of signed upgrades) that fills the record
/// must not refuse the calls a drain and a move depend on.
#[tokio::test]
async fn a_full_signature_record_still_accepts_control_calls() {
    let cluster = node("a");
    // Past the allowed skew after the start ([`Seen::floor`]).
    let now = now_ms() + 30_000;
    let signed = |n: u32| {
        let mut mac = [0u8; 32];
        mac[..4].copy_from_slice(&n.to_le_bytes());
        Signed {
            mac,
            ts: now,
            body: String::new(),
            sender: None,
        }
    };
    for n in 0..u32::try_from(MAX_SEEN).unwrap() {
        assert_eq!(
            cluster.first_use(&signed(n), false),
            Ok(()),
            "refused call {n}"
        );
    }
    let next = u32::try_from(MAX_SEEN).unwrap();
    assert!(
        cluster.first_use(&signed(next), false) == Err(SignatureRefused::Full),
        "the record of calls is full"
    );
    assert!(
        cluster.first_use(&signed(next + 1), true).is_ok(),
        "a control call was refused"
    );
    // Still used once only, in its own record.
    assert_eq!(
        cluster.first_use(&signed(next + 1), true),
        Err(SignatureRefused::Used),
        "replayed"
    );
}

/// A lease write that finds the lease ran out runs the lapse hook (the
/// service closes its WebSockets there) before ending every hold.
#[tokio::test]
async fn a_lapse_runs_its_hook_before_ending_the_holds() {
    let cluster = node("a");
    let ran = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = Arc::clone(&ran);
    cluster.on_lease_lapse(Box::new(move || {
        counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }));
    cluster.end_holds_after_lapse();
    assert_eq!(ran.load(std::sync::atomic::Ordering::SeqCst), 1);
}

/// The record of `key` in `store`, decoded.
async fn owner_record(
    store: &Arc<dyn ObjectStore>,
    key: &str,
) -> wasmkeep_cluster::node_lease::OwnerRecord {
    let body = store
        .get(&StorePath::from(key))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    wasmkeep_cluster::node_lease::OwnerRecord::decode(&body).unwrap()
}

/// Waits (5 s at most) until `cond` holds.
async fn until(what: &str, mut cond: impl AsyncFnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !cond().await {
        assert!(tokio::time::Instant::now() < deadline, "never: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// ADR 0024, section 1: a hold parked while it may still act keeps the
/// record naming this node, unwritten; a clean stop's
/// `release_all_parked("stop")` releases it, generation kept, and leaves
/// nothing parked or pending.
#[tokio::test]
async fn a_parked_record_is_kept_until_a_stop_releases_it() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let key = a.owner_key(&x);
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let generation = hold.generation().unwrap();
    hold.park(&x);
    assert!(a.is_parked(&x));
    assert!(
        !a.nothing_pending(),
        "a parked record reads as nothing pending"
    );
    assert!(owner_record(&store, &key).await.holder.is_some());
    a.release_all_parked("stop").await;
    assert!(!a.is_parked(&x));
    let record = owner_record(&store, &key).await;
    assert_eq!((record.holder, record.generation), (None, generation));
    assert!(a.nothing_pending());
}

/// ADR 0024, section 1: a hold whose node lease lapsed since it was
/// acquired is not parked: it is released instead.
#[tokio::test]
async fn a_hold_that_may_not_act_is_released_not_parked() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let key = a.owner_key(&x);
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    a.validity.bump_epoch();
    hold.park(&x);
    assert!(!a.is_parked(&x), "parked a hold that may not act");
    until("the hold was released", async || {
        owner_record(&store, &key).await.holder.is_none()
    })
    .await;
    until("nothing pending", async || a.nothing_pending()).await;
}

/// ADR 0024, section 3: a lapse ends every parked record into the pending
/// releases, which land once the lease is valid again.
#[tokio::test]
async fn a_lapse_makes_parked_records_pending_releases() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let key = a.owner_key(&x);
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    hold.park(&x);
    assert!(a.is_parked(&x));
    a.validity.set_until(Some(now_ms().saturating_sub(1)));
    wait_lease_valid(&a).await; // the next write sees the lapse
    until("the lapse took the parked record", async || {
        !a.is_parked(&x)
    })
    .await;
    until("its release landed", async || {
        owner_record(&store, &key).await.holder.is_none()
    })
    .await;
    until("nothing pending", async || a.nothing_pending()).await;
}

/// ADR 0024, section 3: an unresolved wake of a parked room that landed
/// is released by the resolver (a parked hold counts as held only at its
/// own generation), and the parked hold behind it, which the record no
/// longer names, is forgotten (`forget_parked_behind`).
#[tokio::test]
async fn a_landed_unresolved_wake_supersedes_the_parked_hold() {
    use wasmkeep_cluster::node_lease::OwnerRecord;
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let instance = a.membership.instance().unwrap().to_owned();
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let key = a.owner_key(&x);
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let parked_at = hold.generation().unwrap();
    hold.park(&x);
    let parked_record = owner_record(&store, &key).await;
    // The wake's write, unanswered, landed.
    let woken = OwnerRecord {
        holder: Some(("a".to_owned(), instance)),
        generation: parked_at + 1,
        recorded: None,
    };
    store
        .put(&StorePath::from(key.as_str()), woken.encode().into())
        .await
        .unwrap();
    a.pending.push(PendingEntry::Unresolved {
        key: key.clone(),
        generation: parked_at + 1,
        before: Some(parked_record),
        until_ms: now_ms() + 30_000,
    });
    a.retry_pending().await;
    assert_eq!(
        owner_record(&store, &key).await,
        OwnerRecord {
            holder: None,
            generation: parked_at + 1,
            recorded: None,
        }
    );
    assert!(!a.is_parked(&x), "the superseded parked hold was kept");
    assert!(a.nothing_pending());
}

/// A gate over a fresh in-memory bucket, every fault off.
fn quiet_gate() -> Arc<HangOneLeaseWrite> {
    Arc::new(HangOneLeaseWrite {
        inner: Arc::new(object_store::memory::InMemory::new()),
        hang: std::sync::atomic::AtomicBool::new(false),
        slow_owner_reads_ms: std::sync::atomic::AtomicU64::new(0),
        fail_releases: std::sync::atomic::AtomicU32::new(0),
        lose_release_replies: std::sync::atomic::AtomicU32::new(0),
        slow_owner_writes_ms: std::sync::atomic::AtomicU64::new(0),
        owner_writes: std::sync::atomic::AtomicU64::new(0),
    })
}

/// A node with a hold on `x`, its lease task's next write hanging for its
/// attempt: nothing publishes a validity for a while, whatever the test
/// sets.
async fn held_with_lease_writes_hanging(
    gate: &Arc<HangOneLeaseWrite>,
) -> (Arc<Cluster>, ActorAddress, LeaseHold) {
    let store: Arc<dyn ObjectStore> = Arc::clone(gate) as Arc<dyn ObjectStore>;
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    gate.hang.store(true, std::sync::atomic::Ordering::SeqCst);
    (a, x, hold)
}

/// Whether `x`'s record names nobody, within a second.
async fn released_soon(a: &Cluster, store: &Arc<HangOneLeaseWrite>, x: &ActorAddress) -> bool {
    let key = StorePath::from(a.owner_key(x));
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_secs(1) {
        let body = store.inner.get(&key).await.unwrap().bytes().await.unwrap();
        if wasmkeep_cluster::node_lease::OwnerRecord::decode(&body)
            .unwrap()
            .holder
            .is_none()
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    false
}

/// ADR 0020, section 1 (amended 2026-11-14): a hold dropped after the
/// lease task saw the lapse and before it ended the holds (the lapse hook
/// closing sockets runs in between) is released at once, while the lease
/// is not valid: counted, as one dropped before the lapse was seen is.
#[tokio::test]
async fn a_release_between_the_lapse_and_the_end_of_the_holds_is_counted() {
    let gate = quiet_gate();
    let (a, x, hold) = held_with_lease_writes_hanging(&gate).await;
    let before = a.invalid_releases.total();
    // The lease ran out, and the lease task published it: no validity,
    // a new epoch. `end_all` has not run yet.
    a.validity.set_until(Some(now_ms().saturating_sub(1)));
    assert!(a.validity.publish(None), "a lapse");
    assert_eq!(a.validity.until(), None);
    assert!(hold.generation().is_some(), "still in its cell");
    drop(hold);
    assert!(released_soon(&a, &gate, &x).await, "released at once");
    assert_eq!(a.invalid_releases.total(), before + 1);
}

/// ADR 0020, section 1: a hold registered after the lapse was seen and
/// every hold ended (its acquisition was asked before, its write landed
/// after) is invalid from the start; its actor drops it, and its release
/// is sent at once, while the lease is not valid: counted.
#[tokio::test]
async fn a_hold_registered_after_the_lapse_is_counted_when_released() {
    let gate = quiet_gate();
    let (a, _, first) = held_with_lease_writes_hanging(&gate).await;
    drop(first);
    let y = ActorAddress::new("t", "c", "y").unwrap();
    gate.slow_owner_writes_ms
        .store(60, std::sync::atomic::Ordering::SeqCst);
    let acquiring = tokio::spawn({
        let (a, y) = (Arc::clone(&a), y.clone());
        async move { LeaseHold::acquire(&a, &y).await }
    });
    // In its write (asked while the lease was valid): the lapse is seen,
    // and every hold ends.
    tokio::time::sleep(Duration::from_millis(30)).await;
    let before = a.invalid_releases.total();
    a.validity.set_until(Some(now_ms().saturating_sub(1)));
    assert!(a.validity.publish(None), "a lapse");
    a.pending.end_all(true);
    let late = acquiring.await.unwrap().unwrap();
    assert!(!late.is_valid(), "valid after the lapse");
    gate.slow_owner_writes_ms
        .store(0, std::sync::atomic::Ordering::SeqCst);
    drop(late);
    assert!(released_soon(&a, &gate, &y).await, "released at once");
    assert_eq!(a.invalid_releases.total(), before + 1);
}

/// One release while the lease is valid is not counted; one before the
/// lapse was seen is, once.
#[tokio::test]
async fn a_release_is_counted_only_while_the_lease_is_not_valid() {
    let gate = quiet_gate();
    let (a, x, hold) = held_with_lease_writes_hanging(&gate).await;
    let y = ActorAddress::new("t", "c", "y").unwrap();
    let other = LeaseHold::acquire(&a, &y).await.unwrap();
    let before = a.invalid_releases.total();
    assert_eq!(other.release().await, LeaseReturn::Released);
    assert_eq!(a.invalid_releases.total(), before, "counted while valid");
    // Run out, not seen yet.
    a.validity.set_until(Some(now_ms().saturating_sub(1)));
    assert_eq!(hold.release().await, LeaseReturn::Released);
    assert!(released_soon(&a, &gate, &x).await);
    assert_eq!(a.invalid_releases.total(), before + 1);
}

/// The warning for releases while the lease is not valid is logged once
/// per lapse: for the first one since the lease was last valid, and the
/// count of all of them once it is valid again.
#[test]
fn releases_while_the_lease_is_not_valid_are_logged_once_per_lapse() {
    let r = InvalidReleases::default();
    assert!(r.count(), "the first is logged");
    assert!(!r.count());
    assert!(!r.count());
    assert_eq!(r.valid_again(), 3, "how many, once valid again");
    assert_eq!(r.valid_again(), 0, "logged once");
    assert!(r.count(), "the next lapse's first is logged");
    assert_eq!(r.total(), 4);
}

/// A lapse that drops many holds at once (its WebSockets close) counts
/// each release, and logs the first alone until the lease is valid again.
#[tokio::test]
async fn many_releases_in_one_lapse_are_each_counted_and_logged_once() {
    let gate = quiet_gate();
    let (a, _, first) = held_with_lease_writes_hanging(&gate).await;
    let mut holds = vec![first];
    for i in 0..4 {
        let y = ActorAddress::new("t", "c", format!("y{i}")).unwrap();
        holds.push(LeaseHold::acquire(&a, &y).await.unwrap());
    }
    let before = a.invalid_releases.total();
    // Run out, not seen yet.
    a.validity.set_until(Some(now_ms().saturating_sub(1)));
    drop(holds);
    let started = std::time::Instant::now();
    while a.invalid_releases.total() < before + 5 {
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{} of 5 counted",
            a.invalid_releases.total() - before
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        !a.invalid_releases.count(),
        "a sixth would be logged again before the lease is valid"
    );
}

/// ADR 0035, section 2, on a real hold: the hold cell's acknowledged head
/// is verified by `Replica::restore`, advanced by a first snapshot, a
/// strict segment and a reset (with the database checksum after each),
/// never by a periodic snapshot, and frozen into the pending release a
/// lapse leaves and the parked hold a hibernation leaves. A restore from
/// that history into an empty directory verifies the same head and
/// checksum; a lapse before the verification makes the restore late.
#[tokio::test]
async fn a_real_hold_carries_its_acknowledged_head_into_every_ending() {
    use crate::replica::{Replica, ReplicaError, SnapshotKind};
    use wasmkeep_cluster::recorded_head::Position;
    use wasmkeep_storage::{Storage, StorageConfig};
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let key = a.owner_key(&x);
    let storage_in = |dir: &std::path::Path| {
        Storage::new(
            StorageConfig::new(dir),
            crate::state_dir(&StateRoot::new("", "t", "default")),
        )
    };
    let first_dir = tempfile::tempdir().unwrap();
    let storage = storage_in(first_dir.path());
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let mut replica = Replica::restore(
        &a,
        &x,
        hold.view(),
        hold.generation().unwrap(),
        storage.db_path("c", "x"),
        1 << 30,
    )
    .await
    .unwrap();
    let view = hold.view();
    assert_eq!(view.acknowledged_head(), Some(Position::Empty));
    let mut db = storage.open("c", "x").unwrap();
    db.exec("CREATE TABLE t(v)", &[]).unwrap();
    db.start_capture().unwrap();
    let all = db.snapshot().unwrap();
    assert!(
        replica
            .ship_snapshot(&all, SnapshotKind::First)
            .await
            .unwrap()
    );
    assert_eq!(view.acknowledged_head(), Some(replica.verified_head()));
    db.exec("INSERT INTO t VALUES (1)", &[]).unwrap();
    let changes = db.take_changes().unwrap().unwrap();
    replica.ship(&changes).await.unwrap();
    let segment = replica.verified_head();
    assert!(
        matches!(segment, Position::Known { txid: 2, .. }),
        "{segment:?}"
    );
    assert_eq!(view.acknowledged_head(), Some(segment));
    // A periodic snapshot at the head checks; it never advances.
    let at_head = db.snapshot().unwrap();
    replica
        .ship_snapshot(&at_head, SnapshotKind::Periodic)
        .await
        .unwrap();
    assert_eq!(view.acknowledged_head(), Some(segment));
    // A reset (after `delete-self`) is a new position.
    db.exec("INSERT INTO t VALUES (2)", &[]).unwrap();
    let _ = db.take_changes().unwrap();
    replica.ship_reset(db.snapshot().unwrap()).await.unwrap();
    let reset = replica.verified_head();
    assert_ne!(reset, segment);
    assert_eq!(view.acknowledged_head(), Some(reset));
    drop(db);

    // A lapse ends the hold into a pending release that carries it.
    a.pending.end_all(true);
    assert_eq!(a.pending.frozen_heads(&key), vec![Some(reset)]);
    assert_eq!(view.acknowledged_head(), None, "ended");
    drop((hold, replica));
    assert!(a.releases_done(Duration::from_secs(5)).await);

    // Restored from that history into an empty directory: the same head
    // and checksum, verified into the new hold's cell.
    let second_dir = tempfile::tempdir().unwrap();
    let storage = storage_in(second_dir.path());
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let restored = Replica::restore(
        &a,
        &x,
        hold.view(),
        hold.generation().unwrap(),
        storage.db_path("c", "x"),
        1 << 30,
    )
    .await
    .unwrap();
    assert_eq!(restored.verified_head(), reset);
    assert_eq!(hold.view().acknowledged_head(), Some(reset));
    // A hibernation with sockets open parks it, head and all.
    drop(restored);
    hold.park(&x);
    assert_eq!(a.pending.frozen_heads(&key), vec![Some(reset)]);
    a.release_all_parked("stop").await;

    // A lapse between the restore and its verification: late (answered
    // 503 by `ActorState::ensure_owner_with`).
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let view = hold.view();
    let generation = hold.generation().unwrap();
    a.validity.invalidate();
    let third_dir = tempfile::tempdir().unwrap();
    let late = Replica::restore(
        &a,
        &x,
        view,
        generation,
        storage_in(third_dir.path()).db_path("c", "x"),
        1 << 30,
    )
    .await;
    assert!(matches!(late, Err(ReplicaError::Late)), "{:?}", late.err());
}

/// ADR 0035, section 2, simulation case 2 on the real code: a same-process
/// re-acquire while the old hold's release is queued (dropped, its task
/// not run yet), pending (a lapse ended it), or the hold parked, with the
/// record still naming the old hold, inherits the head it froze.
#[tokio::test]
async fn a_re_acquire_inherits_the_head_its_predecessor_froze() {
    use crate::replica::{Replica, SnapshotKind};
    use wasmkeep_cluster::recorded_head::Completeness;
    use wasmkeep_storage::{Storage, StorageConfig};
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let key = a.owner_key(&x);
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::new(
        StorageConfig::new(dir.path()),
        crate::state_dir(&StateRoot::new("", "t", "default")),
    );
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let generation = hold.generation().unwrap();
    let mut replica = Replica::restore(
        &a,
        &x,
        hold.view(),
        generation,
        storage.db_path("c", "x"),
        1 << 30,
    )
    .await
    .unwrap();
    let mut db = storage.open("c", "x").unwrap();
    db.exec("CREATE TABLE t(v)", &[]).unwrap();
    db.start_capture().unwrap();
    let all = db.snapshot().unwrap();
    replica
        .ship_snapshot(&all, SnapshotKind::First)
        .await
        .unwrap();
    db.exec("INSERT INTO t VALUES (1)", &[]).unwrap();
    replica
        .ship(&db.take_changes().unwrap().unwrap())
        .await
        .unwrap();
    let h = replica.verified_head();
    drop((db, replica));

    let inherited = |hold: &LeaseHold| hold.view().inherited();
    // Queued: dropped, its release task not run yet.
    drop(hold);
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let carried = inherited(&hold).expect("the queued release's head is carried");
    assert_eq!(carried.head, h);
    assert_eq!(
        carried.set_at, generation,
        "stamped with the hold that froze it"
    );
    assert_eq!(carried.completeness, Completeness::Complete);
    // Pending: a lapse ended it into a pending release; never verified,
    // the new hold re-freezes what it inherited, stamp unchanged.
    a.pending.end_all(true);
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let again = inherited(&hold).expect("the pending release's head is carried");
    assert_eq!(again, carried);
    // Parked, then woken.
    hold.park(&x);
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    assert_eq!(inherited(&hold), Some(carried));
    assert_eq!(a.pending.local_head(&key), None, "transferred to the hold");
    drop(hold);
    assert!(a.releases_done(Duration::from_secs(5)).await);
    // Its release landed: the entry ends (the record would carry it once
    // releases write the recorded head).
    assert_eq!(a.pending.local_head(&key), None);
}

/// The app's upgrade floor, read before the first lease write: above this
/// binary's minor version, the start stops with both versions named and
/// no lease written; unreadable, it refuses to start; at or below this
/// version, or absent, the node starts as before.
#[tokio::test]
async fn a_floor_above_this_version_stops_the_start() {
    use wasmkeep_cluster::app_format::floor::{self, Minor};
    let this = Minor::of_version(env!("CARGO_PKG_VERSION")).unwrap();
    let key = StorePath::from(floor::key("", "t", "default"));
    let above = Minor {
        minor: this.minor + 1,
        ..this
    };
    let refused = async |body: Vec<u8>| {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        store.put(&key, body.into()).await.unwrap();
        let a = fast_node("a", &store);
        let why = a.identity().await.unwrap_err();
        assert!(
            matches!(
                store.head(&StorePath::from("nodes/a")).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "no lease written"
        );
        why
    };
    let why = refused(floor::encode(above)).await;
    assert!(
        why.contains(&above.to_string()) && why.contains(&this.to_string()),
        "{why}"
    );
    let why = refused(b"wasmkeep-floor 1\nnine\n".to_vec()).await;
    assert!(
        why.contains("unreadable") && why.contains("formats/t/default/floor"),
        "{why}"
    );
    // Written while the node is in its start check (ttl plus grace, 1.2 s,
    // before its first write): the read after the first write stops it,
    // and its lease is left `released`.
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = fast_node("a", &store);
    tokio::time::sleep(Duration::from_millis(400)).await;
    store.put(&key, floor::encode(above).into()).await.unwrap();
    let why = a.identity().await.unwrap_err();
    assert!(why.contains(&above.to_string()), "{why}");
    let lease = store
        .get(&StorePath::from("nodes/a"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(
        wasmkeep_cluster::node_lease::NodeLeaseObject::decode(&lease).map(|o| o.state),
        Some(wasmkeep_cluster::node_lease::NodeState::Released)
    );
    for body in [Some(floor::encode(this)), None] {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        if let Some(body) = body {
            store.put(&key, body.into()).await.unwrap();
        }
        let a = fast_node("a", &store);
        a.identity().await.unwrap();
        wait_lease_valid(&a).await;
    }
}

/// The floor read after the first lease write refuses only on an answer.
/// With no answer (the floor's reads hang for 7 s, seven lease ttls), the
/// node stays checking, keeps its lease renewed, and reads again: it
/// starts once the bucket answers. After a first write whose answer was
/// lost and adopted, the read is still made, and a floor written during
/// the start check stops the node. A stop while the read waits releases
/// the lease.
#[tokio::test]
async fn the_floor_read_after_the_first_write_waits_for_an_answer() {
    use wasmkeep_cluster::app_format::floor::{self, Minor};
    use wasmkeep_cluster::node_lease::{NodeLeaseObject, NodeState};
    let this = Minor::of_version(env!("CARGO_PKG_VERSION")).unwrap();
    let key = StorePath::from(floor::key("", "t", "default"));
    let above = Minor {
        minor: this.minor + 1,
        ..this
    };
    let lease_of = async |store: &SlowNodeReads| {
        let got = store.inner.get(&StorePath::from("nodes/a")).await.ok()?;
        NodeLeaseObject::decode(&got.bytes().await.unwrap())
    };
    // A 7 s blip of the floor's reads, from within the start check.
    let slow = Arc::new(SlowNodeReads::default());
    let a = fast_node("a", &(Arc::clone(&slow) as Arc<dyn ObjectStore>));
    tokio::time::sleep(Duration::from_millis(400)).await;
    *slow.hang.lock().unwrap() = Some(key.as_ref().to_owned());
    tokio::time::sleep(Duration::from_secs(3)).await;
    let first = lease_of(&slow).await.expect("the first write landed");
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(*a.identity.borrow(), Identity::Checking, "not refused");
    let later = lease_of(&slow).await.unwrap();
    assert_eq!(later.state, NodeState::Live);
    assert!(
        later.written_at_ms > first.written_at_ms,
        "renewed meanwhile"
    );
    *slow.hang.lock().unwrap() = None;
    tokio::time::timeout(Duration::from_secs(5), a.identity())
        .await
        .unwrap()
        .unwrap();
    // The first write's answer is lost (it is adopted), and a floor above
    // is written during the start check: still read, still refused.
    let slow = Arc::new(SlowNodeReads::default());
    *slow.lose_put_reply.lock().unwrap() = Some("nodes/a".to_owned());
    let a = fast_node("a", &(Arc::clone(&slow) as Arc<dyn ObjectStore>));
    tokio::time::sleep(Duration::from_millis(400)).await;
    slow.inner
        .put(&key, floor::encode(above).into())
        .await
        .unwrap();
    let why = a.identity().await.unwrap_err();
    assert!(why.contains(&above.to_string()), "{why}");
    assert!(
        slow.lose_put_reply.lock().unwrap().is_none(),
        "the reply was lost"
    );
    assert_eq!(
        lease_of(&slow).await.map(|o| o.state),
        Some(NodeState::Released)
    );
    // A stop while the read waits: the lease is released.
    let slow = Arc::new(SlowNodeReads::default());
    let a = fast_node("a", &(Arc::clone(&slow) as Arc<dyn ObjectStore>));
    tokio::time::sleep(Duration::from_millis(400)).await;
    *slow.hang.lock().unwrap() = Some(key.as_ref().to_owned());
    while lease_of(&slow).await.is_none() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(a.stop_node_lease(a.node_lease_stop_limit()).await);
    assert_eq!(
        lease_of(&slow).await.map(|o| o.state),
        Some(NodeState::Released)
    );
    assert_eq!(*a.identity.borrow(), Identity::Checking);
}

/// ADR 0035, section 5: the app's format setting. Unreadable, it fails
/// closed without writing a record; absent, records are version 1;
/// enabled, version 2 (an acquisition over nothing carries no evidence).
#[tokio::test]
async fn the_app_format_setting_decides_the_record_version() {
    use wasmkeep_cluster::app_format;
    use wasmkeep_cluster::recorded_head::RecordedHead;
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let record = StorePath::from(a.owner_key(&x));
    let setting = StorePath::from(app_format::key("", "t", "default"));
    store
        .put(&setting, b"wasmkeep-recorded-head 9\n".to_vec().into())
        .await
        .unwrap();
    a.validity.bump_epoch();
    let got = LeaseHold::acquire(&a, &x).await;
    assert!(
        matches!(&got, Err(OwnershipError::Broken(why)) if why.contains("setting")),
        "{:?}",
        got.as_ref().map(|_| ())
    );
    assert!(matches!(
        store.head(&record).await,
        Err(object_store::Error::NotFound { .. })
    ));
    // Absent: version 1.
    store.delete(&setting).await.unwrap();
    a.validity.bump_epoch();
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    assert_eq!(owner_record(&store, &a.owner_key(&x)).await.recorded, None);
    assert_eq!(hold.release().await, LeaseReturn::Released);
    // Enabled: the v1 record is converted, with no evidence.
    store
        .put(&setting, app_format::ENABLED_BODY.to_vec().into())
        .await
        .unwrap();
    a.validity.bump_epoch();
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    assert_eq!(
        owner_record(&store, &a.owner_key(&x)).await.recorded,
        Some(RecordedHead::UNKNOWN)
    );
    drop(hold);
}

/// A bucket whose listings leave out history keys written while `lag` is
/// on (a LIST that lags its PUTs), until `reveal`.
#[derive(Debug)]
struct LaggingList {
    inner: Arc<dyn ObjectStore>,
    lag: std::sync::atomic::AtomicBool,
    hidden: Mutex<std::collections::BTreeSet<String>>,
    lists: std::sync::atomic::AtomicU64,
    /// Each history listing waits this long before it answers (ms).
    slow_lists_ms: std::sync::atomic::AtomicU64,
}

impl LaggingList {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(object_store::memory::InMemory::new()),
            lag: std::sync::atomic::AtomicBool::new(false),
            hidden: Mutex::default(),
            lists: std::sync::atomic::AtomicU64::new(0),
            slow_lists_ms: std::sync::atomic::AtomicU64::new(0),
        })
    }

    fn reveal(&self) {
        self.hidden.lock().unwrap().clear();
    }
}

impl std::fmt::Display for LaggingList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LaggingList")
    }
}

#[async_trait::async_trait]
impl ObjectStore for LaggingList {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if self.lag.load(std::sync::atomic::Ordering::SeqCst)
            && location.as_ref().contains("/actors/")
        {
            self.hidden
                .lock()
                .unwrap()
                .insert(location.as_ref().to_owned());
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &StorePath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, object_store::Result<StorePath>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<StorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        use futures_util::StreamExt as _;
        let history = prefix.is_some_and(|p| p.as_ref().contains("/actors/"));
        if history {
            self.lists.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        let hidden = self.hidden.lock().unwrap().clone();
        let slow = if history {
            self.slow_lists_ms.load(std::sync::atomic::Ordering::SeqCst)
        } else {
            0
        };
        let listed = self.inner.list(prefix).filter(move |m| {
            let keep = m
                .as_ref()
                .map_or(true, |m| !hidden.contains(m.location.as_ref()));
            std::future::ready(keep)
        });
        futures_util::stream::once(async move {
            tokio::time::sleep(Duration::from_millis(slow)).await;
            listed
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// ADR 0035, sections 1 and 3, the bug itself: with the recorded-head
/// format enabled, a release records its head in a version 2 record; a
/// node whose listing still hides that history refuses to activate (503,
/// after the bounded re-list) instead of restoring an older head, and
/// activates once the history is visible.
#[tokio::test]
async fn a_lagging_listing_cannot_hide_a_recorded_head() {
    use crate::replica::{Replica, ReplicaError, SnapshotKind};
    use wasmkeep_cluster::app_format;
    use wasmkeep_cluster::recorded_head::{Completeness, Position};
    use wasmkeep_storage::{Storage, StorageConfig};
    let lagging = LaggingList::new();
    let store: Arc<dyn ObjectStore> = Arc::clone(&lagging) as Arc<dyn ObjectStore>;
    store
        .put(
            &StorePath::from(app_format::key("", "t", "default")),
            app_format::ENABLED_BODY.to_vec().into(),
        )
        .await
        .unwrap();
    let a = fast_node("a", &store);
    let b = fast_node("b", &store);
    for n in [&a, &b] {
        n.identity().await.unwrap();
        wait_lease_valid(n).await;
    }
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let key = a.owner_key(&x);
    let storage_in = |dir: &std::path::Path| {
        Storage::new(
            StorageConfig::new(dir),
            crate::state_dir(&StateRoot::new("", "t", "default")),
        )
    };
    let a_dir = tempfile::tempdir().unwrap();
    let storage = storage_in(a_dir.path());
    // a writes a first snapshot (visible), then a segment the listing
    // hides, and releases.
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let generation = hold.generation().unwrap();
    let mut replica = Replica::restore(
        &a,
        &x,
        hold.view(),
        generation,
        storage.db_path("c", "x"),
        1 << 30,
    )
    .await
    .unwrap();
    let mut db = storage.open("c", "x").unwrap();
    db.exec("CREATE TABLE t(v)", &[]).unwrap();
    db.start_capture().unwrap();
    let all = db.snapshot().unwrap();
    replica
        .ship_snapshot(&all, SnapshotKind::First)
        .await
        .unwrap();
    lagging.lag.store(true, std::sync::atomic::Ordering::SeqCst);
    db.exec("INSERT INTO t VALUES (1)", &[]).unwrap();
    replica
        .ship(&db.take_changes().unwrap().unwrap())
        .await
        .unwrap();
    let h = replica.verified_head();
    drop((db, replica));
    assert_eq!(hold.release().await, LeaseReturn::Released);
    let record = owner_record(&store, &key).await;
    let recorded = record.recorded.expect("a version 2 record");
    assert_eq!(recorded.head, h);
    assert_eq!(recorded.set_at, generation);
    assert_eq!(recorded.completeness, Completeness::Complete);

    // b's listing misses the segment: no activation from the older head.
    let b_dir = tempfile::tempdir().unwrap();
    let hold = LeaseHold::acquire(&b, &x).await.unwrap();
    let floor = hold.view().inherited().expect("the record's floor");
    assert_eq!(floor.head, h);
    let lists = lagging.lists.load(std::sync::atomic::Ordering::SeqCst);
    let started = std::time::Instant::now();
    let refused = Replica::restore(
        &b,
        &x,
        hold.view(),
        hold.generation().unwrap(),
        storage_in(b_dir.path()).db_path("c", "x"),
        1 << 30,
    )
    .await;
    assert!(
        matches!(refused, Err(ReplicaError::Visibility(_))),
        "{:?}",
        refused.err()
    );
    let listed = lagging.lists.load(std::sync::atomic::Ordering::SeqCst) - lists;
    assert!((2..=10).contains(&listed), "{listed} listings");
    assert!(started.elapsed() < Duration::from_secs(16));
    // Given up, the floor intact in the record.
    drop(hold);
    assert!(b.releases_done(Duration::from_secs(5)).await);
    let after = owner_record(&store, &key).await;
    assert_eq!(after.recorded.map(|r| r.head), Some(h));
    assert!(matches!(h, Position::Known { .. }));

    // Visible now: b activates at H.
    lagging.reveal();
    b.pending.clear_cooldown(&key);
    let hold = LeaseHold::acquire(&b, &x).await.unwrap();
    let restored = Replica::restore(
        &b,
        &x,
        hold.view(),
        hold.generation().unwrap(),
        storage_in(b_dir.path()).db_path("c", "x"),
        1 << 30,
    )
    .await
    .unwrap();
    assert_eq!(restored.verified_head(), h);
}

/// ADR 0035, section 6 (simulation case 13 on the real code): a
/// replacement (`actor restore` writes past content as a reset at the
/// live head's txid + 1, as does `delete-self`) is what the release
/// records, never the historical source position; a listing that hides
/// the replacement refuses activation rather than serve the older state.
#[tokio::test]
async fn a_replacement_is_the_recorded_head() {
    use crate::replica::{Replica, ReplicaError, SnapshotKind};
    use wasmkeep_cluster::app_format;
    use wasmkeep_cluster::recorded_head::Position;
    use wasmkeep_storage::{Storage, StorageConfig};
    let lagging = LaggingList::new();
    let store: Arc<dyn ObjectStore> = Arc::clone(&lagging) as Arc<dyn ObjectStore>;
    store
        .put(
            &StorePath::from(app_format::key("", "t", "default")),
            app_format::ENABLED_BODY.to_vec().into(),
        )
        .await
        .unwrap();
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let key = a.owner_key(&x);
    let storage_in = |dir: &std::path::Path| {
        Storage::new(
            StorageConfig::new(dir),
            crate::state_dir(&StateRoot::new("", "t", "default")),
        )
    };
    let dir = tempfile::tempdir().unwrap();
    let storage = storage_in(dir.path());
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let mut replica = Replica::restore(
        &a,
        &x,
        hold.view(),
        hold.generation().unwrap(),
        storage.db_path("c", "x"),
        1 << 30,
    )
    .await
    .unwrap();
    let mut db = storage.open("c", "x").unwrap();
    db.exec("CREATE TABLE t(v)", &[]).unwrap();
    db.start_capture().unwrap();
    replica
        .ship_snapshot(&db.snapshot().unwrap(), SnapshotKind::First)
        .await
        .unwrap();
    db.exec("INSERT INTO t VALUES (1)", &[]).unwrap();
    replica
        .ship(&db.take_changes().unwrap().unwrap())
        .await
        .unwrap();
    let source = replica.verified_head();
    db.exec("INSERT INTO t VALUES (2)", &[]).unwrap();
    replica
        .ship(&db.take_changes().unwrap().unwrap())
        .await
        .unwrap();
    // The replacement, hidden from listings: past content as a new write.
    lagging.lag.store(true, std::sync::atomic::Ordering::SeqCst);
    db.exec("DELETE FROM t WHERE v = 2", &[]).unwrap();
    let _ = db.take_changes().unwrap();
    replica.ship_reset(db.snapshot().unwrap()).await.unwrap();
    let replaced = replica.verified_head();
    assert!(
        matches!((source, replaced), (Position::Known { txid: s, .. }, Position::Known { txid: r, .. }) if r == s + 2),
        "{source:?} then {replaced:?}"
    );
    drop((db, replica));
    assert_eq!(hold.release().await, LeaseReturn::Released);
    assert_eq!(
        owner_record(&store, &key).await.recorded.map(|r| r.head),
        Some(replaced)
    );
    // A listing without the replacement shows the pre-replacement head:
    // refused, not activated.
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let other = tempfile::tempdir().unwrap();
    let refused = Replica::restore(
        &a,
        &x,
        hold.view(),
        hold.generation().unwrap(),
        storage_in(other.path()).db_path("c", "x"),
        1 << 30,
    )
    .await;
    assert!(
        matches!(refused, Err(ReplicaError::Visibility(_))),
        "{:?}",
        refused.err()
    );
}

/// ADR 0035, section 5 (part of simulation case 11 on the real code): an
/// app that enabled the recorded-head format is not served while a live
/// node of it does not announce `recorded-head` (an older binary started
/// by mistake); served again once that node is gone. Nothing is written
/// meanwhile.
#[tokio::test]
async fn an_enabled_app_refuses_service_beside_an_incapable_peer() {
    use wasmkeep_cluster::app_format;
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    store
        .put(
            &StorePath::from(app_format::key("", "t", "default")),
            app_format::ENABLED_BODY.to_vec().into(),
        )
        .await
        .unwrap();
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let old = StorePath::from("nodes/old");
    let body = format!(
        "wasmkeep-node 3\nlive beef\n{}\nold\nhttp://old\n0.4.2\nt/default\n\n\nserving\n\ntake front\n",
        now_ms()
    );
    store.put(&old, body.into_bytes().into()).await.unwrap();
    let x = ActorAddress::new("t", "c", "x").unwrap();
    a.validity.bump_epoch();
    let got = LeaseHold::acquire(&a, &x).await;
    assert!(
        matches!(&got, Err(OwnershipError::Refused(why)) if why.contains("old")),
        "{:?}",
        got.as_ref().map(|_| ())
    );
    assert!(matches!(
        store.head(&StorePath::from(a.owner_key(&x))).await,
        Err(object_store::Error::NotFound { .. })
    ));
    store.delete(&old).await.unwrap();
    a.validity.bump_epoch();
    LeaseHold::acquire(&a, &x).await.unwrap();
}

/// ADR 0035, section 3, simulation case 5 on the real code: liveness after
/// collection on a strongly consistent bucket. A releases H (stamped g1);
/// B inherits it, verifies, writes, snapshots at P and collects every
/// object below P, H's included, then crashes (its record held, a lower
/// bound H stamped g1). A fresh node must activate from P: P's generation
/// is above the stamp, so it proves H though H's object is gone.
#[tokio::test]
async fn a_floor_collected_away_is_proven_by_a_later_generation() {
    use crate::replica::{Replica, SnapshotKind};
    use futures_util::StreamExt as _;
    use wasmkeep_cluster::app_format;
    use wasmkeep_cluster::node_lease::OwnerRecord;
    use wasmkeep_cluster::recorded_head::Completeness;
    use wasmkeep_storage::{Storage, StorageConfig};
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    store
        .put(
            &StorePath::from(app_format::key("", "t", "default")),
            app_format::ENABLED_BODY.to_vec().into(),
        )
        .await
        .unwrap();
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let key = a.owner_key(&x);
    let storage_in = |dir: &std::path::Path| {
        Storage::new(
            StorageConfig::new(dir),
            crate::state_dir(&StateRoot::new("", "t", "default")),
        )
    };
    let dir = tempfile::tempdir().unwrap();
    let storage = storage_in(dir.path());
    let mut db = storage.open("c", "x").unwrap();
    // g1: H.
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let g1 = hold.generation().unwrap();
    let mut replica = Replica::restore(&a, &x, hold.view(), g1, storage.db_path("c", "x"), 1 << 30)
        .await
        .unwrap();
    db.exec("CREATE TABLE t(v)", &[]).unwrap();
    db.start_capture().unwrap();
    replica
        .ship_snapshot(&db.snapshot().unwrap(), SnapshotKind::First)
        .await
        .unwrap();
    db.exec("INSERT INTO t VALUES (1)", &[]).unwrap();
    replica
        .ship(&db.take_changes().unwrap().unwrap())
        .await
        .unwrap();
    drop((db, replica));
    assert_eq!(hold.release().await, LeaseReturn::Released);
    // g2: B writes on, snapshots at P, collects below it.
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let g2 = hold.generation().unwrap();
    let floor = hold.view().inherited().unwrap();
    assert_eq!(floor.set_at, g1);
    let mut replica = Replica::restore(&a, &x, hold.view(), g2, storage.db_path("c", "x"), 1 << 30)
        .await
        .unwrap();
    let mut db = storage.open("c", "x").unwrap();
    db.start_capture().unwrap();
    for v in 2..5 {
        db.exec(&format!("INSERT INTO t VALUES ({v})"), &[])
            .unwrap();
        replica
            .ship(&db.take_changes().unwrap().unwrap())
            .await
            .unwrap();
    }
    replica
        .ship_snapshot(&db.snapshot().unwrap(), SnapshotKind::Periodic)
        .await
        .unwrap();
    let p = replica.head();
    let history = StorePath::from(format!("{}/", a.history_key(&x)));
    let listed: Vec<_> = store.list(Some(&history)).collect().await;
    let snapshot_p = format!("{:016x}-{:016x}.ltx", p.txid, p.generation);
    for meta in listed.into_iter().map(Result::unwrap) {
        if !meta.location.as_ref().ends_with(&snapshot_p) {
            store.delete(&meta.location).await.unwrap();
        }
    }
    drop((db, replica));
    // B crashes: its record stays held, a lower bound H stamped g1, and its
    // node is gone (this test forgets the hold and frees the record as a
    // fence would).
    std::mem::forget(hold);
    let crashed = OwnerRecord {
        holder: Some(("gone".into(), "dead".into())),
        generation: g2,
        recorded: Some(wasmkeep_cluster::local_heads::Requirement::stored(
            Some(floor),
            true,
            None,
        )),
    };
    assert_eq!(
        crashed.recorded.unwrap().completeness,
        Completeness::LowerBound
    );
    store
        .put(&StorePath::from(key.clone()), crashed.encode().into())
        .await
        .unwrap();
    let fenced = wasmkeep_cluster::node_lease::NodeLeaseObject {
        state: wasmkeep_cluster::node_lease::NodeState::Fenced,
        instance: Some("dead".into()),
        written_at_ms: 0,
    };
    store
        .put(&StorePath::from("nodes/gone"), fenced.encode().into())
        .await
        .unwrap();
    // A fresh node, no local evidence: it activates from P.
    let c = fast_node("c", &store);
    c.identity().await.unwrap();
    wait_lease_valid(&c).await;
    let hold = LeaseHold::acquire(&c, &x).await.unwrap();
    let carried = hold.view().inherited().unwrap();
    assert_eq!((carried.head, carried.set_at), (floor.head, floor.set_at));
    assert_eq!(carried.completeness, Completeness::LowerBound);
    let other = tempfile::tempdir().unwrap();
    let restored = Replica::restore(
        &c,
        &x,
        hold.view(),
        hold.generation().unwrap(),
        storage_in(other.path()).db_path("c", "x"),
        1 << 30,
    )
    .await
    .unwrap();
    assert_eq!(restored.head(), p);
}

/// Review question on ADR 0035: the app string a real heartbeat carries
/// must match what the capability checks compare (`tenant/app` from the
/// node's state root). A node of the implicit app writes it empty.
#[tokio::test]
async fn heartbeats_name_the_app_the_capability_checks_compare() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let nodes = wasmkeep_cluster::membership::all_heartbeats(&a.control_keys)
        .await
        .unwrap();
    let me = nodes.iter().find(|n| n.id == "a").expect("a's heartbeat");
    let served = format!("{}/{}", a.root.tenant(), a.root.app());
    assert!(
        wasmkeep_cluster::membership::serves(me, &served),
        "heartbeat app {:?} vs {served:?}",
        me.app
    );
    // As an older binary would announce it: found by the check.
    let mut old = me.clone();
    old.can_recorded_head = false;
    assert_eq!(
        wasmkeep_cluster::membership::without_recorded_head(
            &[old],
            &served,
            now_ms(),
            a.heartbeat_horizon_ms
        ),
        vec!["a"]
    );
}

/// ADR 0035 follow-up S3: an idle expiry whose cycle is refused (its
/// restore cannot prove the floor) rewrites the record twice, acquire and
/// release; the record must keep the idle origin the expiry judged, as
/// `idle-since`, or a refused actor never ages into expiry again.
#[tokio::test]
async fn a_refused_expiry_cycle_keeps_the_idle_origin() {
    use wasmkeep_cluster::app_format;
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    store
        .put(
            &StorePath::from(app_format::key("", "t", "default")),
            app_format::ENABLED_BODY.to_vec().into(),
        )
        .await
        .unwrap();
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let key = a.owner_key(&x);
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    assert_eq!(hold.release().await, LeaseReturn::Released);
    assert_eq!(
        owner_record(&store, &key)
            .await
            .recorded
            .and_then(|r| r.idle_since_ms),
        None,
        "an ordinary release starts a new idle period"
    );
    // The expiry takes the record as listed, idle since 1234 by its
    // listed last-modified time, and its cycle ends without activation.
    let etag = store
        .head(&StorePath::from(key.clone()))
        .await
        .unwrap()
        .e_tag
        .unwrap();
    let hold = LeaseHold::acquire_with(&a, &x, Some((etag.as_str(), 1_234)))
        .await
        .unwrap();
    assert_eq!(
        owner_record(&store, &key)
            .await
            .recorded
            .and_then(|r| r.idle_since_ms),
        Some(1_234),
        "carried by the expiry's acquisition"
    );
    assert_eq!(hold.release().await, LeaseReturn::Released);
    assert_eq!(
        owner_record(&store, &key)
            .await
            .recorded
            .and_then(|r| r.idle_since_ms),
        Some(1_234),
        "kept by the release of a hold never activated"
    );
}

/// ADR 0035 follow-up S4: a local file whose sidecar names the recorded
/// head but whose checksum is another (a database changed outside
/// replication) is not proof of a conflict in the bucket: it is set aside
/// and the history downloaded, which proves the floor.
#[tokio::test]
async fn a_conflicting_local_file_is_set_aside_and_downloaded() {
    use crate::replica::{Replica, SnapshotKind};
    use wasmkeep_cluster::app_format;
    use wasmkeep_storage::{Storage, StorageConfig};
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    store
        .put(
            &StorePath::from(app_format::key("", "t", "default")),
            app_format::ENABLED_BODY.to_vec().into(),
        )
        .await
        .unwrap();
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::new(
        StorageConfig::new(dir.path()),
        crate::state_dir(&StateRoot::new("", "t", "default")),
    );
    let path = storage.db_path("c", "x");
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let mut replica = Replica::restore(
        &a,
        &x,
        hold.view(),
        hold.generation().unwrap(),
        path.clone(),
        1 << 30,
    )
    .await
    .unwrap();
    let mut db = storage.open("c", "x").unwrap();
    db.exec("CREATE TABLE t(v)", &[]).unwrap();
    db.start_capture().unwrap();
    replica
        .ship_snapshot(&db.snapshot().unwrap(), SnapshotKind::First)
        .await
        .unwrap();
    db.exec("INSERT INTO t VALUES (1)", &[]).unwrap();
    replica
        .ship(&db.take_changes().unwrap().unwrap())
        .await
        .unwrap();
    let h = replica.verified_head();
    let head = replica.head();
    drop((db, replica));
    assert_eq!(hold.release().await, LeaseReturn::Released);
    // Changed outside replication, its sidecar rewritten to match.
    let mut db = storage.open("c", "x").unwrap();
    db.exec("INSERT INTO t VALUES (99)", &[]).unwrap();
    drop(db);
    let page_size = 4096;
    let sum = wasmkeep_storage::wal::WalCapture::start(&path, page_size)
        .unwrap()
        .checksum();
    let mut sidecar = path.as_os_str().to_owned();
    sidecar.push(".replica");
    std::fs::write(
        std::path::PathBuf::from(sidecar),
        format!("{} {} {}\n", head.generation, head.txid, sum.value()),
    )
    .unwrap();
    assert_eq!(
        wasmkeep_storage::local_is_at(&path, head.generation, head.txid),
        Some(sum),
        "the local file claims the recorded head"
    );
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let restored = Replica::restore(
        &a,
        &x,
        hold.view(),
        hold.generation().unwrap(),
        path.clone(),
        1 << 30,
    )
    .await
    .expect("set aside and downloaded, not refused");
    assert_eq!(restored.verified_head(), h);
}

/// ADR 0035 follow-up S6: with a recorded floor to reach, a missing link
/// is first taken for a listing behind its writes and listed again; once
/// the discovery bound is spent with the link still missing, it is
/// reported as the corruption it then is (500), not a 503 for ever.
#[tokio::test]
async fn a_missing_link_past_the_bound_is_a_corruption() {
    use crate::replica::{Replica, ReplicaError, SnapshotKind};
    use futures_util::StreamExt as _;
    use wasmkeep_cluster::app_format;
    use wasmkeep_storage::{Storage, StorageConfig};
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    store
        .put(
            &StorePath::from(app_format::key("", "t", "default")),
            app_format::ENABLED_BODY.to_vec().into(),
        )
        .await
        .unwrap();
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let storage_in = |dir: &std::path::Path| {
        Storage::new(
            StorageConfig::new(dir),
            crate::state_dir(&StateRoot::new("", "t", "default")),
        )
    };
    let dir = tempfile::tempdir().unwrap();
    let storage = storage_in(dir.path());
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let mut replica = Replica::restore(
        &a,
        &x,
        hold.view(),
        hold.generation().unwrap(),
        storage.db_path("c", "x"),
        1 << 30,
    )
    .await
    .unwrap();
    let mut db = storage.open("c", "x").unwrap();
    db.exec("CREATE TABLE t(v)", &[]).unwrap();
    db.start_capture().unwrap();
    replica
        .ship_snapshot(&db.snapshot().unwrap(), SnapshotKind::First)
        .await
        .unwrap();
    for v in 1..4 {
        db.exec(&format!("INSERT INTO t VALUES ({v})"), &[])
            .unwrap();
        replica
            .ship(&db.take_changes().unwrap().unwrap())
            .await
            .unwrap();
    }
    drop((db, replica));
    assert_eq!(hold.release().await, LeaseReturn::Released);
    // A middle segment is gone for good.
    let history = StorePath::from(format!("{}/", a.history_key(&x)));
    let mut wal: Vec<StorePath> = store
        .list(Some(&history))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|m| m.unwrap().location)
        .filter(|l| l.as_ref().contains("/wal/"))
        .collect();
    wal.sort();
    store.delete(&wal[1]).await.unwrap();
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let other = tempfile::tempdir().unwrap();
    let got = Replica::restore(
        &a,
        &x,
        hold.view(),
        hold.generation().unwrap(),
        storage_in(other.path()).db_path("c", "x"),
        1 << 30,
    )
    .await;
    assert!(
        matches!(&got, Err(ReplicaError::Restore(why)) if why.contains("missing")),
        "{:?}",
        got.err()
    );
}

/// Review of S6: a missing link is a corruption only when every listing
/// the bound allowed showed it missing. Listings slow enough to spend the
/// deadline first prove nothing: refused as not yet visible (503), and
/// the next request, once the link is listed, activates.
#[tokio::test]
async fn a_missing_link_behind_slow_listings_stays_retryable() {
    use crate::replica::{Replica, ReplicaError, SnapshotKind};
    use wasmkeep_cluster::app_format;
    use wasmkeep_storage::{Storage, StorageConfig};
    let lagging = LaggingList::new();
    let store: Arc<dyn ObjectStore> = Arc::clone(&lagging) as Arc<dyn ObjectStore>;
    store
        .put(
            &StorePath::from(app_format::key("", "t", "default")),
            app_format::ENABLED_BODY.to_vec().into(),
        )
        .await
        .unwrap();
    let a = fast_node("a", &store);
    a.identity().await.unwrap();
    wait_lease_valid(&a).await;
    let x = ActorAddress::new("t", "c", "x").unwrap();
    let storage_in = |dir: &std::path::Path| {
        Storage::new(
            StorageConfig::new(dir),
            crate::state_dir(&StateRoot::new("", "t", "default")),
        )
    };
    let dir = tempfile::tempdir().unwrap();
    let storage = storage_in(dir.path());
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let mut replica = Replica::restore(
        &a,
        &x,
        hold.view(),
        hold.generation().unwrap(),
        storage.db_path("c", "x"),
        1 << 30,
    )
    .await
    .unwrap();
    let mut db = storage.open("c", "x").unwrap();
    db.exec("CREATE TABLE t(v)", &[]).unwrap();
    db.start_capture().unwrap();
    replica
        .ship_snapshot(&db.snapshot().unwrap(), SnapshotKind::First)
        .await
        .unwrap();
    db.exec("INSERT INTO t VALUES (1)", &[]).unwrap();
    replica
        .ship(&db.take_changes().unwrap().unwrap())
        .await
        .unwrap();
    // The middle link is not listed yet; the last one is.
    lagging.lag.store(true, std::sync::atomic::Ordering::SeqCst);
    db.exec("INSERT INTO t VALUES (2)", &[]).unwrap();
    replica
        .ship(&db.take_changes().unwrap().unwrap())
        .await
        .unwrap();
    lagging
        .lag
        .store(false, std::sync::atomic::Ordering::SeqCst);
    db.exec("INSERT INTO t VALUES (3)", &[]).unwrap();
    replica
        .ship(&db.take_changes().unwrap().unwrap())
        .await
        .unwrap();
    let h = replica.verified_head();
    drop((db, replica));
    assert_eq!(hold.release().await, LeaseReturn::Released);
    // Listings slow enough that the deadline ends the discovery first.
    lagging
        .slow_lists_ms
        .store(4_000, std::sync::atomic::Ordering::SeqCst);
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let other = tempfile::tempdir().unwrap();
    let path = storage_in(other.path()).db_path("c", "x");
    let got = Replica::restore(
        &a,
        &x,
        hold.view(),
        hold.generation().unwrap(),
        path.clone(),
        1 << 30,
    )
    .await;
    assert!(
        matches!(&got, Err(ReplicaError::Visibility(_))),
        "{:?}",
        got.err()
    );
    drop(hold);
    assert!(a.releases_done(Duration::from_secs(5)).await);
    // The link is listed now: the next request activates.
    lagging.reveal();
    lagging
        .slow_lists_ms
        .store(0, std::sync::atomic::Ordering::SeqCst);
    a.pending.clear_cooldown(&a.owner_key(&x));
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    let restored = Replica::restore(
        &a,
        &x,
        hold.view(),
        hold.generation().unwrap(),
        path,
        1 << 30,
    )
    .await
    .unwrap();
    assert_eq!(restored.verified_head(), h);
}

/// ADR 0041, section 3: a peer's load is fresh for three periods from
/// when this node first saw that heartbeat, on its own monotonic clock; a
/// heartbeat read again keeps its age, a new one starts again.
#[tokio::test(start_paused = true)]
async fn a_peer_load_is_fresh_for_three_periods_from_its_first_sighting() {
    let period = Duration::from_secs(3);
    let info = |written_at_ms: u64, actors: u64| NodeInfo {
        load: Some(wasmkeep_cluster::membership::Load {
            actors,
            actor_room: Some(100 - actors),
            socket_room: Some(10),
            ..Default::default()
        }),
        can_place: true,
        written_at_ms,
        instance: Some("beef".into()),
        ..node_info("p")
    };
    let view = |n: &NodeInfo| HashMap::from([("p".to_owned(), Peer::of(n).unwrap())]);
    let first = view(&info(1_000, 40));
    let peer = &first["p"];
    assert!(peer.can_place);
    let seen = peer.seen.as_ref().unwrap();
    assert_eq!((seen.load.actors, seen.load.actor_room), (40, Some(60)));
    assert!(seen.fresh(period).is_some());
    // The same heartbeat read again 8 s later: still the first sighting.
    tokio::time::advance(Duration::from_secs(8)).await;
    let mut again = view(&info(1_000, 40));
    carry_first_seen(&first, &mut again);
    assert!(again["p"].seen.as_ref().unwrap().fresh(period).is_some());
    // At three periods exactly it is stale: keep it.
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(again["p"].seen.as_ref().unwrap().fresh(period).is_none());
    // A new heartbeat (another `written_at`) is fresh again, even with
    // the same numbers, whatever its own clock says.
    let mut next = view(&info(500, 40));
    carry_first_seen(&again, &mut next);
    assert!(next["p"].seen.as_ref().unwrap().fresh(period).is_some());
    // A node of a version without rooms has a load but no room: the
    // decision treats it as no candidate (section 4).
    let old = NodeInfo {
        load: Some(wasmkeep_cluster::membership::Load::default()),
        can_place: false,
        ..node_info("p")
    };
    let peer = Peer::of(&old).unwrap();
    assert!(!peer.can_place);
    assert_eq!(peer.seen.unwrap().load.actor_room, None);
}

fn node_info(id: &str) -> NodeInfo {
    NodeInfo {
        id: id.into(),
        address: "http://x".into(),
        version: "0".into(),
        written_at_ms: 0,
        app: "t/default".into(),
        cluster_address: None,
        tls_pins: Vec::new(),
        drain: Some(DrainState::Serving),
        reads_secrets: false,
        can_take: false,
        can_front: false,
        can_handoff: false,
        can_end: false,
        can_flush: false,
        can_rate_limits: false,
        can_assets: false,
        can_blobs: false,
        can_restore: false,
        can_actor_restore: false,
        instance: None,
        lease: None,
        load: None,
        can_recorded_head: false,
        can_multi_app: false,
        can_place: false,
        process: None,
    }
}

/// A store whose reads of heartbeats (`nodes/`) are slow, hang or fail,
/// as the test sets it.
#[derive(Debug, Default)]
struct SlowNodeReads {
    inner: Arc<object_store::memory::InMemory>,
    /// Every read of `nodes/` waits this long first (ms).
    delay_ms: std::sync::atomic::AtomicU64,
    /// Reads of this key never answer.
    hang: std::sync::Mutex<Option<String>>,
    /// Reads of these keys never answer either.
    hang_all: std::sync::Mutex<std::collections::HashSet<String>>,
    /// Reads of this key fail at once.
    fail: std::sync::Mutex<Option<String>>,
    /// Every other read and listing (`control/`, the app's intents) waits
    /// this long first (ms): a slow bucket that still answers.
    others_delay_ms: std::sync::atomic::AtomicU64,
    /// Reads of `nodes/` objects, counted.
    node_gets: std::sync::atomic::AtomicU64,
    /// Reads of peers' `nodes/` objects (not `nodes/a`) in flight now, and
    /// the most at once.
    node_gets_in_flight: std::sync::atomic::AtomicU64,
    node_gets_peak: std::sync::atomic::AtomicU64,
    /// Every listing of `nodes/` waits this long first (ms).
    list_nodes_delay_ms: std::sync::atomic::AtomicU64,
    /// The next write of this key lands, and its answer is lost (once).
    lose_put_reply: std::sync::Mutex<Option<String>>,
}

impl std::fmt::Display for SlowNodeReads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlowNodeReads({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for SlowNodeReads {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let put = self.inner.put_opts(location, payload, opts).await;
        let mut lose = self.lose_put_reply.lock().unwrap();
        if put.is_ok() && lose.as_deref() == Some(location.as_ref()) {
            *lose = None;
            return Err(object_store::Error::Generic {
                store: "test",
                source: "reply lost".into(),
            });
        }
        put
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &StorePath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let key = location.as_ref();
        if key.starts_with("nodes/") {
            use std::sync::atomic::Ordering::SeqCst;
            self.node_gets.fetch_add(1, SeqCst);
            // Peers' objects only: the node's own start check reads its own.
            let peer = key != "nodes/a";
            if peer {
                let now = self.node_gets_in_flight.fetch_add(1, SeqCst) + 1;
                self.node_gets_peak.fetch_max(now, SeqCst);
            }
            let delay = self.delay_ms.load(SeqCst);
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            if peer {
                self.node_gets_in_flight.fetch_sub(1, SeqCst);
            }
        } else {
            let delay = self
                .others_delay_ms
                .load(std::sync::atomic::Ordering::SeqCst);
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
        }
        let hang = self.hang.lock().unwrap().as_deref() == Some(key)
            || self.hang_all.lock().unwrap().contains(key);
        if hang {
            std::future::pending::<()>().await;
        }
        let fail = self.fail.lock().unwrap().as_deref() == Some(key);
        if fail {
            return Err(object_store::Error::Generic {
                store: "test",
                source: "read refused".into(),
            });
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, object_store::Result<StorePath>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<StorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        use futures_util::StreamExt as _;
        let delay = self
            .others_delay_ms
            .load(std::sync::atomic::Ordering::SeqCst);
        let listing = self.inner.list(prefix);
        let nodes = prefix.is_some_and(|p| p.as_ref() == "nodes");
        let delay = if nodes {
            self.list_nodes_delay_ms
                .load(std::sync::atomic::Ordering::SeqCst)
        } else {
            delay
        };
        if delay == 0 {
            return listing;
        }
        futures_util::stream::once(async move {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            listing
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Node `a` over TLS on `store`, 100 ms heartbeats and a 3 h ttl: it never
/// gets past its start check, so nothing reads the bucket but the
/// refreshes the test makes. Its horizon is 300 ms plus `skew_ms`; its
/// heartbeat reads time out after 1 s.
fn quiet_tls_node(store: &Arc<SlowNodeReads>, skew_ms: u64) -> Arc<Cluster> {
    let store: Arc<dyn ObjectStore> = Arc::clone(store) as Arc<dyn ObjectStore>;
    let mut config = ClusterConfig::new("a", "http://a", store);
    config.heartbeat = Duration::from_millis(100);
    config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, skew_ms).unwrap();
    config.cluster_tls = Some(ClusterTlsConfig::new("https://a.test:7443"));
    Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap()
}

/// Publishes a format-2 heartbeat for `id` written at `at`, at
/// `https://<id>.test:7443` with the pin `pin` repeated.
async fn publish_tls(store: &SlowNodeReads, id: &str, at: u64, pin: u8) {
    let body = format!(
        "wasmkeep-node 2\n{id}\nhttp://{id}\n0\n{at}\nt/\nhttps://{id}.test:7443\n{}\n",
        format!("{pin:02x}").repeat(32)
    );
    store
        .inner
        .put(
            &StorePath::from(format!("nodes/{id}")),
            body.into_bytes().into(),
        )
        .await
        .unwrap();
}

/// One peer's heartbeat read hangs: the round still ends within one read
/// attempt, the other peer's newer heartbeat is applied, the hung one is
/// kept as last read (trusted until its own horizon, no longer), and no
/// connection pool is dropped (the view did not change).
#[tokio::test]
async fn one_hung_heartbeat_read_neither_holds_up_the_view_nor_drops_the_pools() {
    let store = Arc::new(SlowNodeReads::default());
    let a = quiet_tls_node(&store, 3_000);
    let t0 = now_ms();
    publish_tls(&store, "b", t0, 1).await;
    publish_tls(&store, "c", t0, 2).await;
    assert!(a.refresh_view(true).await);
    let first = a.clients();
    *store.hang.lock().unwrap() = Some("nodes/c".to_owned());
    let later = now_ms() + 1;
    publish_tls(&store, "b", later, 1).await;
    let failures = || {
        crate::metrics::METRICS
            .heartbeat_read_failures
            .load(std::sync::atomic::Ordering::Relaxed)
    };
    let failed_before = failures();
    let started = std::time::Instant::now();
    let answered = tokio::time::timeout(Duration::from_millis(2_500), a.refresh_view(false))
        .await
        .expect("one hung read held up the whole view");
    assert!(!answered, "a round with a read unanswered is not a success");
    assert!(started.elapsed() < Duration::from_millis(2_000));
    assert_eq!(
        a.transport.heard_body("b").map(|n| n.written_at_ms),
        Some(later),
        "b's newer heartbeat applied"
    );
    assert_eq!(a.check_tls_target("https://b.test:7443"), Ok(()));
    assert_eq!(
        a.check_tls_target("https://c.test:7443"),
        Ok(()),
        "c kept as last read, within its horizon"
    );
    assert!(Arc::ptr_eq(&first, &a.clients()), "pools kept");
    assert!(failures() > failed_before);
}

/// The heartbeats are read at once, not one after the other: a listing,
/// and a round of reads, of the nodes when every read takes 300 ms each
/// end in about one read.
#[tokio::test]
async fn heartbeat_reads_go_out_at_once() {
    let store = Arc::new(SlowNodeReads::default());
    let a = quiet_tls_node(&store, 3_000);
    for (i, id) in ["b", "c", "d", "e", "f"].into_iter().enumerate() {
        publish_tls(&store, id, now_ms(), u8::try_from(i + 1).unwrap()).await;
    }
    store
        .delay_ms
        .store(300, std::sync::atomic::Ordering::SeqCst);
    let reads = || crate::metrics::METRICS.heartbeat_read_ms.count();
    let ages = || crate::metrics::METRICS.heartbeat_age_ms.count();
    let (reads_before, ages_before) = (reads(), ages());
    let started = std::time::Instant::now();
    assert!(a.refresh_view(true).await);
    let listing = started.elapsed();
    let started = std::time::Instant::now();
    assert!(a.refresh_view(false).await);
    let round = started.elapsed();
    assert!(
        listing < Duration::from_millis(1_000),
        "listing {listing:?}"
    );
    assert!(round < Duration::from_millis(1_000), "round {round:?}");
    assert_eq!(a.known_nodes.lock().unwrap().len(), 5);
    // Each read of the round timed (a's own object, absent, too), each
    // peer's heartbeat aged at both refreshes.
    assert!(reads() >= reads_before + 6);
    assert!(ages() >= ages_before + 10);
}

/// A heartbeat dated ahead of this clock (a peer whose clock is fast, past
/// the allowed skew) is trusted one horizon from when this node first read
/// it, not one horizon from its date: a peer that then dies is not trusted
/// for as long as its clock was ahead. Reading the same heartbeat again
/// does not move that.
#[tokio::test]
async fn a_heartbeat_dated_ahead_is_trusted_one_horizon_from_its_first_read() {
    let store = Arc::new(SlowNodeReads::default());
    // Horizon 400 ms.
    let a = quiet_tls_node(&store, 100);
    publish_tls(&store, "c", now_ms() + 3_600_000, 1).await;
    let first_read = std::time::Instant::now();
    assert!(a.refresh_view(true).await);
    assert_eq!(a.check_tls_target("https://c.test:7443"), Ok(()));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(a.refresh_view(false).await);
    tokio::time::sleep(Duration::from_millis(600).saturating_sub(first_read.elapsed())).await;
    let refused = a.check_tls_target("https://c.test:7443");
    assert!(
        refused.as_ref().is_err_and(|e| e.contains("expired")),
        "{refused:?}"
    );
    assert!(!a.reaches("c"));
}

/// A read that fails keeps the peer as last read, and so trusted until
/// that heartbeat's own horizon, never past it, without dropping pools.
#[tokio::test]
async fn a_failed_read_carries_a_heartbeat_only_to_its_own_horizon() {
    let store = Arc::new(SlowNodeReads::default());
    // Horizon 400 ms.
    let a = quiet_tls_node(&store, 100);
    let at = now_ms();
    publish_tls(&store, "b", at, 1).await;
    assert!(a.refresh_view(true).await);
    let first = a.clients();
    *store.fail.lock().unwrap() = Some("nodes/b".to_owned());
    assert!(!a.refresh_view(false).await);
    if now_ms() < at + 300 {
        assert_eq!(a.check_tls_target("https://b.test:7443"), Ok(()));
    }
    assert!(Arc::ptr_eq(&first, &a.clients()), "pools kept");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!a.refresh_view(false).await);
    assert!(a.check_tls_target("https://b.test:7443").is_err());
}

/// The heartbeats as last read are the process's (ADR 0038: its apps share
/// the trust table), and a node's entry is replaced only by a read issued
/// later: a body read earlier that lands late, by this app or another,
/// never undoes a fence read since, and a listing issued before a read
/// does not drop the node it read (ADR 0020, amendment of 2026-11-20).
#[tokio::test]
async fn an_older_read_landing_late_never_undoes_a_newer_one() {
    use super::process::{HeartbeatRead, Heartbeats};
    let store = Arc::new(SlowNodeReads::default());
    let a = quiet_tls_node(&store, 100);
    let body = |state: &str| {
        let text = format!(
            "wasmkeep-node 3\n{state} beef\n{}\nb\nhttp://b\n0.0.0\nt/\nhttps://b.test:7443\n{}\nserving\n\ntake\n",
            now_ms(),
            "ab".repeat(32)
        );
        NodeInfo::decode(text.as_bytes()).unwrap()
    };
    let (older, newer) = (
        std::time::Instant::now(),
        std::time::Instant::now() + Duration::from_millis(1),
    );
    let found =
        |n: NodeInfo| Heartbeats::Read(vec![("b".to_owned(), HeartbeatRead::Found(Box::new(n)))]);
    let fresh = a.transport.note_heartbeats(
        found(body("fenced")),
        newer,
        LEASE_TTL_GRACE_MS,
        |_, _, f| f.to_vec(),
    );
    assert_eq!(fresh, ["b"]);
    let (all, fresh) =
        a.transport
            .note_heartbeats(found(body("live")), older, LEASE_TTL_GRACE_MS, |a, _, f| {
                (a.to_vec(), f.to_vec())
            });
    assert!(fresh.is_empty(), "an older read replaced a newer one");
    assert_eq!(
        all[0].lease,
        Some(wasmkeep_cluster::node_lease::NodeState::Fenced)
    );
    let all = a.transport.note_heartbeats(
        Heartbeats::Listed(Vec::new()),
        older,
        LEASE_TTL_GRACE_MS,
        |a, _, _| a.to_vec(),
    );
    assert_eq!(all.len(), 1, "an older listing dropped a node read since");
    let all = a.transport.note_heartbeats(
        Heartbeats::Read(vec![(
            "b".to_owned(),
            HeartbeatRead::Failed("down".to_owned()),
        )]),
        newer + Duration::from_millis(1),
        LEASE_TTL_GRACE_MS,
        |a, _, _| a.to_vec(),
    );
    assert_eq!(all.len(), 1, "a failed read keeps the node as last read");
}

/// The heartbeats a process reads raise the newest lease bodies its
/// ownership records are judged against (ADR 0020, amendment on lease
/// reads), by date and never back, whatever order the reads were issued
/// in: the table is the one the node's `Owners` holds.
#[tokio::test]
async fn heartbeat_reads_raise_the_newest_lease_bodies() {
    use super::process::{HeartbeatRead, Heartbeats};
    use wasmkeep_cluster::node_lease::NodeState;
    let store = Arc::new(SlowNodeReads::default());
    let a = quiet_tls_node(&store, 100);
    assert!(Arc::ptr_eq(&a.transport.lease_reads, a.owners.reads()));
    let body = |state: &str, instance: &str, at: u64| {
        let text = format!(
            "wasmkeep-node 3\n{state} {instance}\n{at}\nb\nhttp://b\n0.0.0\nt/\nhttps://b.test:7443\n{}\nserving\n\ntake\n",
            "ab".repeat(32)
        );
        NodeInfo::decode(text.as_bytes()).unwrap()
    };
    let found =
        |n: NodeInfo| Heartbeats::Read(vec![("b".to_owned(), HeartbeatRead::Found(Box::new(n)))]);
    let t0 = std::time::Instant::now();
    a.transport.note_heartbeats(
        found(body("live", "cafe", 2_000)),
        t0,
        LEASE_TTL_GRACE_MS,
        |_, _, _| (),
    );
    // Issued later, dated earlier (a stale read): the newest stays.
    a.transport.note_heartbeats(
        found(body("released", "beef", 1_000)),
        t0 + Duration::from_millis(1),
        LEASE_TTL_GRACE_MS,
        |_, _, _| (),
    );
    let newest = a.transport.lease_reads.newest("b").unwrap();
    assert_eq!(
        (
            newest.state,
            newest.instance.as_deref(),
            newest.written_at_ms
        ),
        (NodeState::Live, Some("cafe"), 2_000)
    );
    // A listing's bodies count too.
    a.transport.note_heartbeats(
        Heartbeats::Listed(vec![body("fenced", "cafe", 2_000)]),
        t0 + Duration::from_millis(2),
        LEASE_TTL_GRACE_MS,
        |_, _, _| (),
    );
    assert_eq!(
        a.transport.lease_reads.newest("b").map(|o| o.state),
        Some(NodeState::Fenced)
    );
}

/// A runtime restarted in place (same process, same node id, a new
/// instance) whose first heartbeat has landed is not a twin of its
/// predecessor: the process may still hold the predecessor's heartbeat as
/// last read, and a read of a peer alone applies every node as last read.
/// A heartbeat of another process under this id still is a twin.
#[tokio::test]
async fn a_runtime_restarted_in_place_is_not_its_own_predecessors_twin() {
    use super::process::{HeartbeatRead, Heartbeats};
    let store = Arc::new(SlowNodeReads::default());
    let process = super::process::NodeProcess::new();
    let runtime = || {
        let shared: Arc<dyn ObjectStore> = Arc::clone(&store) as Arc<dyn ObjectStore>;
        let mut config = ClusterConfig::new("a", "http://a", shared);
        config.heartbeat = Duration::from_millis(100);
        // A 3 h ttl, so nothing runs in the background.
        config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 100).unwrap();
        config.process = Some(Arc::clone(&process));
        Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap()
    };
    let body = |instance: &str| {
        let text = format!(
            "wasmkeep-node 3\nlive {instance}\n{}\na\nhttp://a\n0.0.0\nt/\n\n\nserving\n\ntake\n",
            now_ms()
        );
        NodeInfo::decode(text.as_bytes()).unwrap()
    };
    let first = runtime();
    let old = first.membership.instance().unwrap().to_owned();
    // Its own heartbeat, as the process last read it.
    first.transport.note_heartbeats(
        Heartbeats::Read(vec![(
            "a".to_owned(),
            HeartbeatRead::Found(Box::new(body(&old))),
        )]),
        std::time::Instant::now(),
        LEASE_TTL_GRACE_MS,
        |_, _, _| (),
    );
    first.leave_process();
    drop(first);
    let second = runtime();
    assert_ne!(second.membership.instance(), Some(old.as_str()));
    second.heartbeat_reached_bucket(now_ms());
    // A peer read before this runtime's first listing.
    publish_tls(&store, "b", now_ms(), 1).await;
    assert!(second.read_heartbeats(&["b".to_owned()]).await);
    assert!(
        !second.identity_taken(),
        "the restarted runtime stopped on its predecessor's heartbeat"
    );
    // Another process's heartbeat under this id: a twin.
    second.transport.note_heartbeats(
        Heartbeats::Read(vec![(
            "a".to_owned(),
            HeartbeatRead::Found(Box::new(body("beef"))),
        )]),
        std::time::Instant::now(),
        LEASE_TTL_GRACE_MS,
        |_, _, _| (),
    );
    assert!(second.read_heartbeats(&["b".to_owned()]).await);
    assert!(second.identity_taken());
}

/// The first read of a heartbeat dated ahead is the process's (ADR 0038:
/// its apps share the trust table): a runtime that joins the process after
/// another one clamped a dead peer with a fast clock, or one restarted in
/// place, does not count that heartbeat as written now and trust the peer
/// for another horizon, nor rebuild the pools for it.
#[tokio::test]
async fn a_runtime_that_joins_late_does_not_trust_a_dead_fast_clock_peer_again() {
    let store = Arc::new(SlowNodeReads::default());
    let process = super::process::NodeProcess::new();
    let runtime = |id: &str, app: &str| {
        let shared: Arc<dyn ObjectStore> = Arc::clone(&store) as Arc<dyn ObjectStore>;
        let mut config = ClusterConfig::new(id, format!("http://{id}"), shared);
        config.heartbeat = Duration::from_millis(100);
        // Horizon 400 ms; a 3 h ttl, so nothing runs in the background.
        config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 100).unwrap();
        config.cluster_tls = Some(ClusterTlsConfig::new("https://a.test:7443"));
        config.process = Some(Arc::clone(&process));
        Cluster::new(&config, &StateRoot::new("", "t", app)).unwrap()
    };
    let shop = runtime("shop-1", "shop");
    // c's clock is an hour fast; it dies right after this write.
    publish_tls(&store, "c", now_ms() + 3_600_000, 1).await;
    let first_read = std::time::Instant::now();
    assert!(shop.refresh_view(true).await);
    assert_eq!(shop.check_tls_target("https://c.test:7443"), Ok(()));
    tokio::time::sleep(Duration::from_millis(600).saturating_sub(first_read.elapsed())).await;
    assert!(shop.refresh_view(true).await);
    assert!(shop.check_tls_target("https://c.test:7443").is_err());
    let pools = shop.clients();
    // Another app joins the process, and reads the same heartbeat.
    let blog = runtime("blog-1", "blog");
    assert!(blog.refresh_view(true).await);
    assert!(
        shop.check_tls_target("https://c.test:7443").is_err(),
        "the process trusts dead c again once another runtime joined"
    );
    // Restarted in place: the same again.
    drop(blog);
    let blog = runtime("blog-1", "blog");
    assert!(blog.refresh_view(true).await);
    assert!(shop.refresh_view(true).await);
    assert!(blog.check_tls_target("https://c.test:7443").is_err());
    assert!(Arc::ptr_eq(&pools, &shop.clients()), "pools churned");
}

/// The intents are read one bucket operation at a time, each within a
/// read attempt, not the whole refresh within one limit: a bucket that
/// answers every operation of a refresh slowly (200 ms each here, under
/// the 266 ms attempt, five of them past two attempts together) still has
/// the intents applied, late.
#[tokio::test]
async fn a_slow_bucket_still_applies_the_intents() {
    let store = Arc::new(SlowNodeReads::default());
    let shared: Arc<dyn ObjectStore> = Arc::clone(&store) as Arc<dyn ObjectStore>;
    let a = fast_node("a", &shared);
    a.identity().await.unwrap();
    store
        .others_delay_ms
        .store(200, std::sync::atomic::Ordering::SeqCst);
    store
        .inner
        .put(&StorePath::from("control/drain/a"), Vec::new().into())
        .await
        .unwrap();
    store
        .inner
        .put(
            &StorePath::from("control/changed"),
            b"drain a".to_vec().into(),
        )
        .await
        .unwrap();
    let started = std::time::Instant::now();
    while !a.draining() {
        assert!(
            started.elapsed() < Duration::from_secs(8),
            "the drain intent was never applied on a slow bucket"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A read issued later can still have been served before a fence or a
/// release landed: its `live` body of the same instance must not undo the
/// `fenced` one read before it. That process can never write again (its
/// object only changes by conditional writes, and the fence took it), so
/// a live body of it is stale whenever it is read.
#[tokio::test]
async fn a_live_body_read_late_never_undoes_a_fence_of_its_instance() {
    use super::process::{HeartbeatRead, Heartbeats};
    let store = Arc::new(SlowNodeReads::default());
    let a = quiet_tls_node(&store, 100);
    let at = now_ms();
    let body = |state: &str, instance: &str| {
        let text = format!(
            "wasmkeep-node 3\n{state} {instance}\n{at}\nb\nhttp://b\n0.0.0\nt/\nhttps://b.test:7443\n{}\nserving\n\ntake\n",
            "ab".repeat(32)
        );
        NodeInfo::decode(text.as_bytes()).unwrap()
    };
    let found =
        |n: NodeInfo| Heartbeats::Read(vec![("b".to_owned(), HeartbeatRead::Found(Box::new(n)))]);
    let t0 = std::time::Instant::now();
    a.transport.note_heartbeats(
        found(body("fenced", "beef")),
        t0,
        LEASE_TTL_GRACE_MS,
        |_, _, _| (),
    );
    let fresh = a.transport.note_heartbeats(
        found(body("live", "beef")),
        t0 + Duration::from_millis(1),
        LEASE_TTL_GRACE_MS,
        |_, _, f| f.to_vec(),
    );
    assert!(fresh.is_empty(), "a stale live body undid the fence");
    assert_eq!(
        a.transport.heard_body("b").and_then(|n| n.lease),
        Some(wasmkeep_cluster::node_lease::NodeState::Fenced)
    );
    // A new process under that id may go live.
    let fresh = a.transport.note_heartbeats(
        found(body("live", "cafe")),
        t0 + Duration::from_millis(2),
        LEASE_TTL_GRACE_MS,
        |_, _, f| f.to_vec(),
    );
    assert_eq!(fresh, ["b"]);
}

/// ADR 0020, amendment of 2026-11-20: a node whose heartbeat is `released`
/// or `fenced` for an instance cannot come back as that instance (a new
/// process under that id has a new instance, found by the next listing or
/// a hello), so a round reads only the nodes that may still write: with 90
/// released node leases left by earlier clusters on the prefix and three
/// live nodes, a round reads four objects (this node's own included), not
/// 94.
#[tokio::test]
async fn a_round_reads_only_the_nodes_that_may_still_write() {
    let store = Arc::new(SlowNodeReads::default());
    let a = quiet_tls_node(&store, 3_000);
    for i in 0..90 {
        let id = format!("old-{i}");
        let body = format!(
            "wasmkeep-node 3\nreleased i{i}\n{}\n{id}\nhttp://{id}\n0.0.0\nt/\n\n\nserving\n\ntake\n",
            now_ms() - 86_400_000
        );
        store
            .inner
            .put(
                &StorePath::from(format!("nodes/{id}")),
                body.into_bytes().into(),
            )
            .await
            .unwrap();
    }
    for (i, id) in ["b", "c", "d"].into_iter().enumerate() {
        publish_tls(&store, id, now_ms(), u8::try_from(i + 1).unwrap()).await;
    }
    assert!(a.refresh_view(true).await);
    let gets = || store.node_gets.load(std::sync::atomic::Ordering::SeqCst);
    let before = gets();
    a.refresh_view(false).await;
    assert_eq!(
        gets() - before,
        4,
        "a round read the released node leases too"
    );
    // The listing still sees them all (discovery, and the free rule's
    // evidence for whatever records name them).
    assert_eq!(a.known_nodes.lock().unwrap().len(), 93);
}

/// A node lease body of node `id` (format 3): `state` and `instance` (`-`:
/// none) on its second line, `written_at` on its third, the heartbeat's
/// fields after them.
fn node_lease_body(id: &str, state: &str, instance: &str, written_at: u64) -> String {
    format!(
        "wasmkeep-node 3\n{state} {instance}\n{written_at}\n{id}\nhttp://{id}\n0.0.0\nt/\n\n\nserving\n\ntake\n"
    )
}

/// Several apps in one process share the heartbeats as last read (ADR
/// 0038). A sibling app's listing, made before this app's first write over
/// its stopped predecessor's `released` lease, holds that body for this
/// node's id; a read of a peer alone then applies it to this app's view
/// before this app read its own object again. It is the body this app's
/// first write replaced, not a twin. A body of another instance applied the
/// same way still is one.
#[tokio::test]
async fn a_siblings_read_of_this_nodes_predecessor_is_not_a_twin() {
    use super::process::Heartbeats;
    let store = Arc::new(SlowNodeReads::default());
    let process = super::process::NodeProcess::new();
    let runtime = |id: &str, app: &str| {
        let shared: Arc<dyn ObjectStore> = Arc::clone(&store) as Arc<dyn ObjectStore>;
        let mut config = ClusterConfig::new(id, format!("http://{id}"), shared);
        config.heartbeat = Duration::from_millis(100);
        config.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
        config.app = format!("t/{app}");
        config.process = Some(Arc::clone(&process));
        Cluster::new(&config, &StateRoot::new("", "t", app)).unwrap()
    };
    // The predecessor stopped cleanly a minute ago: the start check ends
    // at once.
    let then = now_ms() - 60_000;
    let released = node_lease_body("a", "released", "cafe", then);
    store
        .inner
        .put(
            &StorePath::from("nodes/a"),
            released.clone().into_bytes().into(),
        )
        .await
        .unwrap();
    let peer = |at: u64| node_lease_body("b", "live", "beef", at);
    store
        .inner
        .put(
            &StorePath::from("nodes/b"),
            peer(now_ms()).into_bytes().into(),
        )
        .await
        .unwrap();
    let sibling = runtime("s", "other");
    let a = runtime("a", "default");
    a.identity().await.unwrap();
    // From now on this app's reads of its own object never answer: what
    // the process holds for it is what the sibling read.
    *store.hang.lock().unwrap() = Some("nodes/a".to_owned());
    let decoded = |body: &str| NodeInfo::decode(body.as_bytes()).unwrap();
    sibling.transport.note_heartbeats(
        Heartbeats::Listed(vec![decoded(&released), decoded(&peer(now_ms()))]),
        std::time::Instant::now(),
        LEASE_TTL_GRACE_MS,
        |_, _, _| (),
    );
    assert!(a.read_heartbeats(&["b".to_owned()]).await);
    assert!(
        !a.identity_taken(),
        "stopped on its predecessor's body, read by a sibling app"
    );
    // Another instance under this id, read by the sibling: a twin.
    sibling.transport.note_heartbeats(
        Heartbeats::Listed(vec![
            decoded(&node_lease_body("a", "live", "feed", now_ms())),
            decoded(&peer(now_ms())),
        ]),
        std::time::Instant::now(),
        LEASE_TTL_GRACE_MS,
        |_, _, _| (),
    );
    assert!(a.read_heartbeats(&["b".to_owned()]).await);
    assert!(a.identity_taken());
}

/// The running twin check against a body of this node's own id, once this
/// process's first write replaced the predecessor `cafe` written at `at`:
/// copies of that writer's past are not a twin (its `released` or `fenced`
/// body, an earlier one), everything else is (a later body of it, another
/// instance, a body in the old format or naming no instance).
#[tokio::test]
async fn only_the_replaced_writers_past_is_not_a_twin() {
    let store = Arc::new(SlowNodeReads::default());
    let at = now_ms() - 60_000;
    let cases = [
        (
            "its released body",
            node_lease_body("a", "released", "cafe", at),
            false,
        ),
        (
            "its fenced body",
            node_lease_body("a", "fenced", "cafe", at),
            false,
        ),
        (
            "an earlier live body",
            node_lease_body("a", "live", "cafe", at - 500),
            false,
        ),
        (
            "a later body of it",
            node_lease_body("a", "live", "cafe", at + 1),
            true,
        ),
        (
            "another instance",
            node_lease_body("a", "released", "feed", at - 500),
            true,
        ),
        (
            "no instance",
            node_lease_body("a", "fenced", "-", at - 500),
            true,
        ),
        (
            "the old format",
            format!("wasmkeep-node 2\na\nhttp://a\n0\n{}\nt/\n", at - 500),
            true,
        ),
    ];
    for (what, body, twin) in cases {
        let a = quiet_node(&store);
        *a.replaced.lock().unwrap() = Some(("cafe".to_owned(), at));
        a.heartbeat_reached_bucket(now_ms());
        let listed = NodeInfo::decode(body.as_bytes()).unwrap_or_else(|| panic!("{what}"));
        a.check_twin(&[listed]);
        assert_eq!(a.identity_taken(), twin, "{what}");
    }
}

/// A node `a` on `store` whose start check outlasts the test (a 3 h ttl,
/// no object): it writes and reads nothing in the background.
fn quiet_node(store: &Arc<SlowNodeReads>) -> Arc<Cluster> {
    let shared: Arc<dyn ObjectStore> = Arc::clone(store) as Arc<dyn ObjectStore>;
    let mut config = ClusterConfig::new("a", "http://a", shared);
    config.heartbeat = Duration::from_millis(100);
    config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 100).unwrap();
    Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap()
}

/// Writes `body` as node b's object on `store`.
async fn put_b(store: &SlowNodeReads, body: String) {
    store
        .inner
        .put(&StorePath::from("nodes/b"), body.into_bytes().into())
        .await
        .unwrap();
}

/// A peer whose heartbeat reads fail past its horizon is kept as last
/// read, and the failover wait sees nothing new of it: the watch does not
/// take it for a node that stopped writing (a kept request would then wait
/// for a fence its reads cannot bring). A read that answers again judges
/// it as before.
#[tokio::test]
async fn a_peer_kept_as_last_read_is_not_stale_to_the_failover_wait() {
    use wasmkeep_cluster::failover::Verdict;
    let store = Arc::new(SlowNodeReads::default());
    // Horizon 400 ms.
    let a = quiet_tls_node(&store, 100);
    put_b(&store, node_lease_body("b", "live", "beef", now_ms())).await;
    assert!(a.refresh_view(true).await);
    assert_eq!(a.failover.verdict("b"), Verdict::Forward);
    *store.fail.lock().unwrap() = Some("nodes/b".to_owned());
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!a.refresh_view(false).await);
    assert_eq!(
        a.failover.verdict("b"),
        Verdict::Forward,
        "a body kept after a failed read was judged as read again"
    );
    *store.fail.lock().unwrap() = None;
    assert!(a.refresh_view(false).await);
    assert!(matches!(a.failover.verdict("b"), Verdict::Stale { .. }));
}

/// A read that returns a body older than one the process already read
/// (ADR 0020, amendment on lease reads: a replica or a cache serving an
/// earlier version) is not news to the failover wait: an earlier
/// instance's body does not look like a restart to a kept request.
#[tokio::test]
async fn an_older_lease_body_read_later_misleads_no_kept_request() {
    use wasmkeep_cluster::failover::Verdict;
    let store = Arc::new(SlowNodeReads::default());
    let a = quiet_tls_node(&store, 100);
    let t0 = now_ms();
    put_b(&store, node_lease_body("b", "live", "cafe", t0)).await;
    assert!(a.refresh_view(true).await);
    let held_since = now_ms();
    tokio::time::sleep(Duration::from_millis(5)).await;
    // A stale read: the predecessor's last body, dated before.
    put_b(&store, node_lease_body("b", "live", "beef", t0 - 1_000)).await;
    assert!(a.refresh_view(false).await);
    assert_eq!(
        a.failover.verdict_since("b", held_since),
        Verdict::Forward,
        "an older instance's body read late looked like a restart"
    );
}

/// The heartbeat reads of a round go out at once, but at most
/// `HEARTBEAT_READ_CONCURRENCY` at a time: a prefix with many nodes does
/// not open a connection per node in one burst. Every node is still read,
/// in the order given.
#[tokio::test]
async fn a_round_caps_its_reads_in_flight() {
    let store = Arc::new(SlowNodeReads::default());
    let a = quiet_tls_node(&store, 3_000);
    let ids: Vec<String> = (0..80).map(|i| format!("n{i:02}")).collect();
    store
        .delay_ms
        .store(20, std::sync::atomic::Ordering::SeqCst);
    let reads = a.transport.read_each(&ids, Duration::from_secs(5)).await;
    let peak = store
        .node_gets_peak
        .load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        peak <= super::process::HEARTBEAT_READ_CONCURRENCY as u64,
        "{peak} reads in flight at once"
    );
    assert!(peak > 1, "the reads went out one at a time");
    let got: Vec<&String> = reads.iter().map(|(id, _)| id).collect();
    assert_eq!(got, ids.iter().collect::<Vec<_>>());
}

/// A node whose object is gone (`cluster forget`) leaves the process's
/// table of live senders at the first read that shows it, as its pins do,
/// not only once its last heartbeat's horizon has passed.
#[tokio::test]
async fn a_gone_node_leaves_the_live_senders_at_once() {
    let store = Arc::new(SlowNodeReads::default());
    // Horizon 3.3 s: nothing expires during the test.
    let a = quiet_tls_node(&store, 3_000);
    publish_tls(&store, "b", now_ms(), 1).await;
    assert!(a.refresh_view(true).await);
    assert!(a.transport.live_app("b").is_some());
    store
        .inner
        .delete(&StorePath::from("nodes/b"))
        .await
        .unwrap();
    assert!(a.refresh_view(true).await);
    assert_eq!(a.transport.live_app("b"), None, "a gone node kept as live");
}

/// A node found gone is remembered (so that a read issued before, landing
/// late, does not bring it back) only as long as such a read can still
/// land: the heartbeats as last read do not grow with every node id the
/// prefix ever had.
#[tokio::test]
async fn gone_nodes_are_forgotten_once_no_earlier_read_can_land() {
    use super::process::{HeartbeatRead, Heartbeats};
    let store = Arc::new(SlowNodeReads::default());
    let a = quiet_tls_node(&store, 100);
    let t0 = std::time::Instant::now();
    let body =
        NodeInfo::decode(node_lease_body("b", "released", "beef", 1_000).as_bytes()).unwrap();
    let limit = a.listing_limit();
    a.transport.note_heartbeats(
        Heartbeats::Read(vec![("b".to_owned(), HeartbeatRead::Found(Box::new(body)))]),
        t0,
        LEASE_TTL_GRACE_MS,
        |_, _, _| (),
    );
    a.transport.note_heartbeats(
        Heartbeats::Read(vec![("b".to_owned(), HeartbeatRead::Gone)]),
        t0 + Duration::from_millis(1),
        LEASE_TTL_GRACE_MS,
        |_, _, _| (),
    );
    assert_eq!(a.transport.heard_ids(), ["b"], "gone, and remembered");
    // Within the longest a read may take: still remembered.
    a.transport.note_heartbeats(
        Heartbeats::Listed(Vec::new()),
        t0 + limit,
        LEASE_TTL_GRACE_MS,
        |_, _, _| (),
    );
    assert_eq!(a.transport.heard_ids(), ["b"]);
    // Past it: forgotten.
    a.transport.note_heartbeats(
        Heartbeats::Listed(Vec::new()),
        t0 + limit * 2 + Duration::from_millis(2),
        LEASE_TTL_GRACE_MS,
        |_, _, _| (),
    );
    assert!(
        a.transport.heard_ids().is_empty(),
        "{:?}",
        a.transport.heard_ids()
    );
}

/// A node that starts on a prefix holding only stopped nodes (or nodes of
/// other apps) has no peer to say hello to: its first round of hellos is
/// kept for the first peer of its app that shows up live, rather than
/// spent on nobody (`say_hello` sends to those peers only).
#[tokio::test]
async fn only_live_peers_of_this_app_count_as_found_for_the_hellos() {
    let store = Arc::new(SlowNodeReads::default());
    let a = quiet_tls_node(&store, 3_000);
    let b = |state: &str, instance: &str| {
        format!(
            "wasmkeep-node 3\n{state} {instance}\n{}\nb\nhttp://b\n0.0.0\nt/default\n\n\nserving\n\ntake\n",
            now_ms()
        )
    };
    put_b(&store, b("released", "beef")).await;
    let other = format!(
        "wasmkeep-node 3\nlive f00d\n{}\nc\nhttp://c\n0.0.0\nt/other\n\n\nserving\n\ntake\n",
        now_ms()
    );
    store
        .inner
        .put(&StorePath::from("nodes/c"), other.into_bytes().into())
        .await
        .unwrap();
    assert!(a.refresh_view(true).await);
    assert!(!a.found_peers(), "a stopped node or another app's counted");
    put_b(&store, b("live", "cafe")).await;
    assert!(a.refresh_view(true).await);
    assert!(a.found_peers());
}

/// A runtime that starts in a process that already read the nodes (a
/// second app, or a runtime restarted in place) reads them by key in its
/// first round, and leaves the listing to the listing task: a slow LIST
/// holds up neither its view nor, so, its early reads. A process that read
/// nothing yet still lists first, to find its peers.
#[tokio::test]
async fn a_runtime_joining_a_process_does_not_wait_for_a_slow_listing() {
    let store = Arc::new(SlowNodeReads::default());
    let process = super::process::NodeProcess::new();
    let shared = || Arc::clone(&store) as Arc<dyn ObjectStore>;
    // Shop, which never gets past its start check, read b already.
    let mut config = ClusterConfig::new("shop-1", "http://shop-1", shared());
    config.heartbeat = Duration::from_millis(100);
    config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 100).unwrap();
    config.process = Some(Arc::clone(&process));
    let shop = Cluster::new(&config, &StateRoot::new("", "t", "shop")).unwrap();
    put_b(&store, node_lease_body("b", "live", "beef", now_ms())).await;
    assert!(shop.read_heartbeats(&["b".to_owned()]).await);
    // Every LIST of `nodes/` 3 s late: past the two attempts (2 s) an
    // inline listing gets.
    store
        .list_nodes_delay_ms
        .store(3_000, std::sync::atomic::Ordering::SeqCst);
    // Blog joins with a short lease: its start check ends in about 1.2 s.
    let mut config = ClusterConfig::new("blog-1", "http://blog-1", shared());
    config.heartbeat = Duration::from_millis(100);
    config.lease = LeaseConfig::new(1_000, 200, 200, 100).unwrap();
    config.process = Some(Arc::clone(&process));
    let blog = Cluster::new(&config, &StateRoot::new("", "t", "blog")).unwrap();
    let mut identity = blog.identity.subscribe();
    tokio::time::timeout(
        Duration::from_secs(20),
        identity.wait_for(|s| *s == super::identity::Identity::Unique),
    )
    .await
    .expect("blog never started")
    .unwrap();
    let unique = std::time::Instant::now();
    let attempt = blog.heartbeat_read_attempt;
    loop {
        if blog.known_nodes.lock().unwrap().iter().any(|n| n == "b") {
            break;
        }
        assert!(
            unique.elapsed() < attempt,
            "blog's first round waited for the slow listing"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Two apps in one process share the heartbeats as last read. When one
/// app's read finds a peer fenced, the other app's failover wait takes
/// the fence at its next view, though its own read was not the one that
/// found it (its rounds no longer read a stopped node).
#[tokio::test]
async fn a_fence_another_app_read_reaches_this_apps_failover_wait() {
    use wasmkeep_cluster::failover::Verdict;
    let store = Arc::new(SlowNodeReads::default());
    let process = super::process::NodeProcess::new();
    let runtime = |id: &str, app: &str| {
        let shared: Arc<dyn ObjectStore> = Arc::clone(&store) as Arc<dyn ObjectStore>;
        let mut config = ClusterConfig::new(id, format!("http://{id}"), shared);
        config.heartbeat = Duration::from_millis(100);
        // A 3 h ttl, so nothing runs in the background.
        config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 100).unwrap();
        config.process = Some(Arc::clone(&process));
        Cluster::new(&config, &StateRoot::new("", "t", app)).unwrap()
    };
    let shop = runtime("shop-1", "shop");
    let blog = runtime("blog-1", "blog");
    let at = now_ms();
    put_b(&store, node_lease_body("b", "live", "beef", at)).await;
    let c = node_lease_body("c", "live", "f00d", at);
    store
        .inner
        .put(&StorePath::from("nodes/c"), c.into_bytes().into())
        .await
        .unwrap();
    assert!(
        shop.read_heartbeats(&["b".to_owned(), "c".to_owned()])
            .await
    );
    assert!(
        blog.read_heartbeats(&["b".to_owned(), "c".to_owned()])
            .await
    );
    assert_eq!(blog.failover.verdict("b"), Verdict::Forward);
    put_b(&store, node_lease_body("b", "fenced", "beef", at)).await;
    assert!(shop.read_heartbeats(&["b".to_owned()]).await);
    // Blog reads another node only.
    assert!(blog.read_heartbeats(&["c".to_owned()]).await);
    assert!(
        matches!(blog.failover.verdict("b"), Verdict::Stopping { .. }),
        "{:?}",
        blog.failover.verdict("b")
    );
}

/// Past the cap on reads in flight, hung reads do not queue behind one
/// another: with three reads that never answer spread over 70 peers
/// (more than 32 apart), the round ends in about one attempt, and the
/// peers that answered are applied as they land, before the round ends,
/// so their trust is renewed while the others hang.
#[tokio::test]
async fn hung_reads_past_the_cap_hold_up_neither_the_round_nor_the_answers() {
    let store = Arc::new(SlowNodeReads::default());
    // Heartbeat reads time out after 1 s.
    let a = quiet_tls_node(&store, 3_000);
    let ids: Vec<String> = (0..70).map(|i| format!("n{i:02}")).collect();
    for (i, id) in ids.iter().enumerate() {
        publish_tls(&store, id, now_ms(), u8::try_from(i + 1).unwrap()).await;
    }
    assert!(a.refresh_view(true).await);
    let newer = now_ms() + 1;
    for (i, id) in ids.iter().enumerate() {
        publish_tls(&store, id, newer, u8::try_from(i + 1).unwrap()).await;
    }
    *store.hang_all.lock().unwrap() = ["nodes/n00", "nodes/n33", "nodes/n66"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    let started = std::time::Instant::now();
    let round = {
        let a = Arc::clone(&a);
        tokio::spawn(async move { a.refresh_view(false).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        a.transport.heard_body("n40").map(|n| n.written_at_ms),
        Some(newer),
        "an answered read waited for the round's hung ones"
    );
    assert!(!round.await.unwrap(), "hung reads are a failed round");
    let took = started.elapsed();
    assert!(
        took < Duration::from_millis(1_800),
        "the round took {took:?}"
    );
    assert_ne!(
        a.transport.heard_body("n33").map(|n| n.written_at_ms),
        Some(newer),
        "a hung read's node stays as last read"
    );
    assert!(a.transport.heard_body("n33").is_some());
}

/// Join reads (a hello, a hop, or the name on an unknown certificate,
/// which anyone reaching the cluster port can present) do not set the
/// latency the early reads are planned on: only rounds do.
#[tokio::test]
async fn join_reads_do_not_plan_the_early_reads() {
    let store = Arc::new(SlowNodeReads::default());
    let a = quiet_tls_node(&store, 3_000);
    publish_tls(&store, "b", now_ms(), 1).await;
    store
        .delay_ms
        .store(200, std::sync::atomic::Ordering::SeqCst);
    let slowest = || a.slowest_read_ms.load(std::sync::atomic::Ordering::Relaxed);
    let before = slowest();
    a.read_joined(&["b".to_owned()]).await;
    assert_eq!(slowest(), before, "a join read set the planned latency");
    a.read_heartbeats(&["b".to_owned()]).await;
    assert!(slowest() >= 200);
}

/// Two apps in one process: blog joins while every read of x fails, and
/// sees x only as the process last read it, long ago. Nothing blog read
/// shows x stopped writing, so a request for x's actors is not kept.
#[tokio::test]
async fn an_app_joining_while_a_peers_reads_fail_keeps_no_request_for_it() {
    use wasmkeep_cluster::failover::Verdict;
    let store = Arc::new(SlowNodeReads::default());
    let process = super::process::NodeProcess::new();
    let runtime = |id: &str, app: &str| {
        let shared: Arc<dyn ObjectStore> = Arc::clone(&store) as Arc<dyn ObjectStore>;
        let mut config = ClusterConfig::new(id, format!("http://{id}"), shared);
        config.heartbeat = Duration::from_millis(100);
        // Horizon 400 ms; a 3 h ttl, so nothing runs in the background.
        config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 100).unwrap();
        config.process = Some(Arc::clone(&process));
        Cluster::new(&config, &StateRoot::new("", "t", app)).unwrap()
    };
    let shop = runtime("shop-1", "shop");
    let x = node_lease_body("x", "live", "beef", now_ms());
    store
        .inner
        .put(&StorePath::from("nodes/x"), x.into_bytes().into())
        .await
        .unwrap();
    put_b(&store, node_lease_body("b", "live", "cafe", now_ms())).await;
    assert!(
        shop.read_heartbeats(&["x".to_owned(), "b".to_owned()])
            .await
    );
    *store.fail.lock().unwrap() = Some("nodes/x".to_owned());
    tokio::time::sleep(Duration::from_millis(500)).await;
    let blog = runtime("blog-1", "blog");
    put_b(&store, node_lease_body("b", "live", "cafe", now_ms())).await;
    assert!(blog.read_heartbeats(&["b".to_owned()]).await);
    assert_eq!(blog.failover.verdict("x"), Verdict::Forward);
}

/// Another app's read of an older body (a replica behind) is no news to
/// this app's failover wait either: it does not look like a restart.
#[tokio::test]
async fn another_apps_older_read_misleads_no_kept_request() {
    use wasmkeep_cluster::failover::Verdict;
    let store = Arc::new(SlowNodeReads::default());
    let process = super::process::NodeProcess::new();
    let runtime = |id: &str, app: &str| {
        let shared: Arc<dyn ObjectStore> = Arc::clone(&store) as Arc<dyn ObjectStore>;
        let mut config = ClusterConfig::new(id, format!("http://{id}"), shared);
        config.heartbeat = Duration::from_millis(100);
        config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 100).unwrap();
        config.process = Some(Arc::clone(&process));
        Cluster::new(&config, &StateRoot::new("", "t", app)).unwrap()
    };
    let shop = runtime("shop-1", "shop");
    let blog = runtime("blog-1", "blog");
    let t0 = now_ms();
    put_b(&store, node_lease_body("b", "live", "cafe", t0)).await;
    assert!(blog.read_heartbeats(&["b".to_owned()]).await);
    let held_since = now_ms();
    tokio::time::sleep(Duration::from_millis(5)).await;
    // Shop's read is served the predecessor's last body.
    put_b(&store, node_lease_body("b", "live", "beef", t0 - 1_000)).await;
    assert!(shop.read_heartbeats(&["b".to_owned()]).await);
    // Blog applies the process's view again, reading only its own object.
    blog.read_heartbeats(&["blog-1".to_owned()]).await;
    assert_eq!(
        blog.failover.verdict_since("b", held_since),
        Verdict::Forward
    );
}

/// A read issued before a node was found gone, landing while the gone
/// mark is kept, does not bring the node back.
#[tokio::test]
async fn a_late_read_does_not_bring_a_gone_node_back() {
    use super::process::{HeartbeatRead, Heartbeats};
    let store = Arc::new(SlowNodeReads::default());
    let a = quiet_tls_node(&store, 100);
    let t0 = std::time::Instant::now();
    let body = || {
        Box::new(NodeInfo::decode(node_lease_body("b", "live", "beef", 1_000).as_bytes()).unwrap())
    };
    a.transport.note_heartbeats(
        Heartbeats::Read(vec![("b".to_owned(), HeartbeatRead::Found(body()))]),
        t0,
        LEASE_TTL_GRACE_MS,
        |_, _, _| (),
    );
    a.transport.note_heartbeats(
        Heartbeats::Read(vec![("b".to_owned(), HeartbeatRead::Gone)]),
        t0 + Duration::from_millis(10),
        LEASE_TTL_GRACE_MS,
        |_, _, _| (),
    );
    let all = a.transport.note_heartbeats(
        Heartbeats::Read(vec![("b".to_owned(), HeartbeatRead::Found(body()))]),
        t0 + Duration::from_millis(5),
        LEASE_TTL_GRACE_MS,
        |all, _, _| all.to_vec(),
    );
    assert!(all.is_empty(), "a late read brought a gone node back");
    assert_eq!(a.transport.heard_ids(), ["b"], "still remembered as gone");
}
