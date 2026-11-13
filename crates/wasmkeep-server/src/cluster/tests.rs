#![allow(clippy::unwrap_used)] // test code

use super::*;

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

#[test]
fn only_another_app_of_the_same_tenant_conflicts() {
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
        instance: None,
        lease: None,
    };
    let nodes = [
        n("me", "acme/shop"),
        n("b", "acme/shop"),  // same app: fine
        n("c", "other/shop"), // another tenant: separate keys
        n("d", "acme/admin"), // same tenant, another app: conflict
        n("e", "acme/"),      // command-line classes of an older version: conflict
        n("f", ""),           // an older node: not judged
    ];
    assert_eq!(
        conflicts("me", "acme/shop", &nodes),
        vec![
            "d (acme/admin)".to_owned(),
            "e (acme/, an older version)".to_owned()
        ]
    );
    assert!(conflicts("me", "other/x", &nodes[..1]).is_empty());
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
    Cluster::new(&config, "t/", "t").unwrap()
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
        "t/default",
        "t",
    )
    .unwrap()
}

#[tokio::test]
async fn the_sweep_cursor_resumes_under_a_prefix_that_needs_escaping() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut config = ClusterConfig::new("a", "http://a", store);
    config.prefix = "team~1".into(); // `~` is escaped in stored keys
    let cluster = Cluster::new(&config, "t/", "t").unwrap();
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
    let cluster = Cluster::new(&config, "t/", "t").unwrap();
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
        instance: None,
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
        instance: None,
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
            instance: None,
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
    let a = Cluster::new(&config, "t/", "t").unwrap();
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
                    instance: None,
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
                    instance: None,
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
                    instance: None,
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
    let a = Cluster::new(&config, "t/", "t").unwrap();
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
    a.refresh_view().await;
    let first = a.clients();
    publish(1).await;
    a.refresh_view().await;
    assert!(Arc::ptr_eq(&first, &a.clients()), "same pins: pools kept");
    publish(2).await;
    a.refresh_view().await;
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
    let a = Cluster::new(&config, "t/", "t").unwrap();
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
        Cluster::new(&config, "t/", "t"),
        Err(crate::Error::ClusterTls(_))
    ));
    config.cluster_tls = Some(ClusterTlsConfig::new("https://a.test:7443"));
    let a = Cluster::new(&config, "t/", "t").unwrap();
    *a.peers.write().unwrap() = HashMap::from([
        (
            "b".to_owned(),
            Peer {
                plain: Some("http://b".into()),
                tls: Some("https://b:7443".into()),
                app: String::new(),
                drain: None,
                can_take: false,
                instance: None,
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
                instance: None,
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
    let accept = Cluster::new(&config, "t/", "t").unwrap();
    config.cluster_plaintext = ClusterPlaintext::Refuse;
    let refuse = Cluster::new(&config, "t/", "t").unwrap();
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

#[tokio::test]
async fn each_refusal_reason_has_its_own_log_slot() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut config = ClusterConfig::new("a", "http://a", store);
    config.heartbeat = Duration::from_secs(3_600);
    config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 1_000).unwrap();
    let a = Cluster::new(&config, "t/", "t").unwrap();
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
        Cluster::new(&config, "t/", "t"),
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
    let err = Cluster::new(&config, "t/", "t").unwrap_err();
    assert!(err.to_string().contains("at most a third"), "{err}");
    config.heartbeat = Duration::from_millis(333);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async { Cluster::new(&config, "t/", "t").unwrap() });
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
        if location.as_ref().starts_with("owners/") {
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
        if slow > 0 && location.as_ref().starts_with("owners/") {
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
    let a = Cluster::new(&config, "t/", "t").unwrap();
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
    let a = Cluster::new(&config, "t/", "t").unwrap();
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
    let a = Cluster::new(&config, "t/", "t").unwrap();
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
    let a = Cluster::new(&config, "t/", "t").unwrap();
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
    let a = Cluster::new(&config, "t/", "t").unwrap();
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
    a.refresh_view().await;
    assert_eq!(a.check_tls_target("https://b.test:7443"), Ok(()));
    store
        .put(&key, body("fenced").into_bytes().into())
        .await
        .unwrap();
    a.refresh_view().await;
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
    let a = Cluster::new(&config, "t/", "t").unwrap();
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
    let a = Cluster::new(&config, "t/", "t").unwrap();
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
    tokio::time::timeout(Duration::from_secs(1), a.refresh_view())
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
    let a = Cluster::new(&config, "t/", "t").unwrap();
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
    let due = a.fences_due(&all);
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
    });
    assert!(!a.nothing_pending());
    a.retry_pending().await;
    assert_eq!(
        read(&key).await,
        OwnerRecord {
            holder: None,
            generation: 5
        }
    );
    assert!(a.nothing_pending());

    // Did not land (the record names nobody at 5), and a second acquisition
    // then landed at 6, the generation the first one tried: the resolver
    // must leave that hold alone.
    a.pending.push(PendingEntry::Unresolved {
        key: key.clone(),
        generation: 6,
    });
    let hold = LeaseHold::acquire(&a, &x).await.unwrap();
    assert_eq!(hold.generation(), Some(6));
    a.retry_pending().await;
    assert_eq!(
        read(&key).await,
        OwnerRecord {
            holder: Some(("a".to_owned(), instance.clone())),
            generation: 6
        }
    );
    assert!(hold.is_valid());
    assert!(a.nothing_pending());
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
    a.pending.push(PendingEntry::Unresolved {
        key: key.clone(),
        generation: 1,
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
    let a = Cluster::new(&config, "t/", "t").unwrap();
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
        instance: Some("iz".into()),
        lease: Some(wasmkeep_cluster::node_lease::NodeState::Live),
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
    let a = Cluster::new(&config, "t/", "t").unwrap();
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
        instance: Some("0011223344556677".to_owned()),
        lease: Some(state),
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
    let b = Cluster::new(&config, "t/", "t").unwrap();
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
    let now = now_ms();
    let signed = |n: u32| {
        let mut mac = [0u8; 32];
        mac[..4].copy_from_slice(&n.to_le_bytes());
        Signed {
            mac,
            ts: now,
            body: String::new(),
        }
    };
    for n in 0..u32::try_from(MAX_SEEN).unwrap() {
        assert!(cluster.first_use(&signed(n), false), "refused call {n}");
    }
    let next = u32::try_from(MAX_SEEN).unwrap();
    assert!(
        !cluster.first_use(&signed(next), false),
        "the record of calls is full"
    );
    assert!(
        cluster.first_use(&signed(next + 1), true),
        "a control call was refused"
    );
    // Still used once only, in its own record.
    assert!(!cluster.first_use(&signed(next + 1), true), "replayed");
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
