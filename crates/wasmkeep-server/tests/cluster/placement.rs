//! Placement at wake-up between nodes (ADR 0041): a node clearly fuller
//! than another sends the wake-up of an actor nobody holds there, once,
//! and that node acquires it; every row of the hop's table (section 6)
//! against a peer this test plays; upgrades never placed; a node that
//! joins fills from the others' wake-ups without anything moving back.

use std::sync::Mutex;

use super::*;

/// A node of class `t` with placement on and `resident` slots.
async fn placing_node(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    resident: usize,
) -> Node {
    placing_node_with(id, data, store, resident, true).await
}

async fn placing_node_with(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    resident: usize,
    placement: bool,
) -> Node {
    placing_node_full(id, data, store, resident, placement, &["t"], None).await
}

/// `classes` served; `view_every`: this node's heartbeat period (how often
/// it reads the others' heartbeats), the harness's 100 ms if `None`.
async fn placing_node_full(
    id: &str,
    data: &Path,
    store: &Arc<dyn ObjectStore>,
    resident: usize,
    placement: bool,
    classes: &[&str],
    view_every: Option<Duration>,
) -> Node {
    node_configured(
        id,
        data,
        store,
        Duration::from_secs(600),
        classes,
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        move |config: &mut ActorConfig| {
            config.mailbox.max_resident = resident;
            let cluster = config.cluster.as_mut().unwrap();
            cluster.placement = placement;
            if let Some(every) = view_every {
                // A lease that allows a 3 s period. A node whose heartbeat
                // is written that rarely is fenced by a peer with the
                // harness's 1 s lease: both take this one.
                if every > Duration::from_millis(100) {
                    cluster.heartbeat = every;
                }
                cluster.lease = LeaseConfig::new(9_000, 200, 200, 100).unwrap();
            }
        },
    )
    .await
}

/// The value of one series of this process's metrics page (0 if absent).
fn metric(series: &str) -> u64 {
    let page = wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default());
    page.lines()
        .find_map(|l| l.strip_prefix(series)?.trim().parse().ok())
        .unwrap_or(0)
}

const APP: &str = r#"tenant="default",app="default""#;

/// Wakes `t/<name>` on `node` and returns the reply.
async fn wake(node: &Node, name: &str) -> Reply {
    send(node.addr, "GET", &format!("/actor/t/{name}/fired"), "").await
}

/// What the peer this test plays answers a placed hop.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Answer {
    /// `503` with `x-wasmkeep-placed: refused`.
    Refuse = 0,
    /// `200`, served by it.
    Serve = 1,
    /// Reads the hop, then closes the connection without an answer.
    Drop = 2,
}

/// A peer of `default/default` that only this test runs: its heartbeat
/// written every 100 ms, announcing `place` with room for 100 residents,
/// and an HTTP listener that records every actor hop it reads.
struct FakePeer {
    hops: Arc<Mutex<Vec<String>>>,
    answer: Arc<AtomicU8>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for FakePeer {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

impl FakePeer {
    fn set(&self, answer: Answer) {
        self.answer.store(answer as u8, Ordering::SeqCst);
    }

    fn hops(&self) -> Vec<String> {
        self.hops.lock().unwrap().clone()
    }
}

async fn fake_peer(store: &Arc<dyn ObjectStore>, answer: Answer) -> FakePeer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hops = Arc::new(Mutex::new(Vec::new()));
    let answer = Arc::new(AtomicU8::new(answer as u8));
    let (seen, mode) = (Arc::clone(&hops), Arc::clone(&answer));
    let serve = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let (seen, mode) = (Arc::clone(&seen), Arc::clone(&mode));
            tokio::spawn(async move {
                let mut buf = Vec::new();
                loop {
                    // One request: its head, then its declared body.
                    let head_end = loop {
                        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break i + 4;
                        }
                        let mut chunk = [0u8; 4096];
                        match socket.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                    let length: usize = head
                        .lines()
                        .find_map(|l| {
                            let (k, v) = l.split_once(':')?;
                            k.eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse().ok())?
                        })
                        .unwrap_or(0);
                    while buf.len() < head_end + length {
                        let mut chunk = [0u8; 4096];
                        match socket.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    buf.drain(..head_end + length);
                    let hop = head
                        .split(' ')
                        .nth(1)
                        .is_some_and(|p| p.starts_with("/actor/"));
                    if !hop {
                        let _ = socket
                            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                            .await;
                        continue;
                    }
                    seen.lock().unwrap().push(head);
                    let reply: &[u8] = match mode.load(Ordering::SeqCst) {
                        0 => b"HTTP/1.1 503 Service Unavailable\r\nx-wasmkeep-placed: refused\r\ncontent-length: 0\r\n\r\n",
                        1 => b"HTTP/1.1 200 OK\r\nx-wasmkeep-node: fake\r\ncontent-length: 2\r\n\r\nok",
                        _ => return,
                    };
                    if socket.write_all(reply).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    let store = Arc::clone(store);
    let beat = tokio::spawn(async move {
        use object_store::ObjectStoreExt as _;
        let words = "take front handoff ends flush rate_limits restore actor-restore \
                     recorded-head multi-app assets blobs place";
        loop {
            let body = format!(
                "wasmkeep-node 3\nlive f00d\n{}\nfake\nhttp://{addr}\n{}\ndefault/default\n\n\nserving\n\n{words}\n\
                 load actors=0 sockets=0 events_per_min=- front_per_min=- actor_room=100 socket_room=100\n",
                now_ms(),
                wasmkeep_cluster::VERSION,
            );
            let _ = store
                .put(
                    &object_store::path::Path::from("nodes/fake"),
                    object_store::PutPayload::from(body),
                )
                .await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    FakePeer {
        hops,
        answer,
        tasks: vec![serve, beat],
    }
}

/// Wakes fresh actors `<prefix>0`, `<prefix>1`... on `node` until the
/// fake peer reads one more hop than `before`: that wake's name and reply.
async fn wake_until_hop(
    node: &Node,
    fake: &FakePeer,
    prefix: &str,
    before: usize,
) -> (String, Reply) {
    for i in 0..60 {
        let name = format!("{prefix}{i}");
        let reply = wake(node, &name).await;
        if fake.hops().len() > before {
            return (name, reply);
        }
        assert_eq!(reply.status, 200, "{name}: {}", reply.head);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no placed hop reached the peer");
}

/// The value of header `name` in a recorded request head.
fn header_of<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.eq_ignore_ascii_case(name).then(|| v.trim())
    })
}

/// Section 6, row by row, against a peer this test plays: a refusal runs
/// the event here and backs the peer off; an answer is returned and sets
/// the owner hint; an ambiguous failure answers 503 and does not run it
/// here. The hop carries the signed mark and the sender.
#[tokio::test(flavor = "multi_thread")]
async fn each_answer_to_a_placed_hop_is_handled_as_the_table_says() {
    // The peer has no cluster listener: plain HTTP only.
    if over_tls() {
        return;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let data = tempfile::tempdir().unwrap();
    let a = placing_node("a", data.path(), &store, 10).await;
    // Under the floor with no peer: kept, wherever the fills go.
    for name in ["x0", "x1", "x2"] {
        let r = wake(&a, name).await;
        assert_eq!(
            (r.status, r.node.as_deref()),
            (200, Some("a")),
            "{}",
            r.head
        );
    }
    let fake = fake_peer(&store, Answer::Refuse).await;

    // Refused: run here, by the ordinary path, and the peer backed off.
    let (refused, reply) = wake_until_hop(&a, &fake, "r", 0).await;
    assert_eq!(
        (reply.status, reply.node.as_deref()),
        (200, Some("a")),
        "{}",
        reply.head
    );
    assert_eq!(lease_holder(&store, &refused).await.as_deref(), Some("a"));
    let hop = &fake.hops()[0];
    let fill: u64 = header_of(hop, "x-wasmkeep-placed")
        .unwrap()
        .parse()
        .unwrap();
    assert!((300..=1000).contains(&fill), "{hop}");
    assert!(header_of(hop, "x-wasmkeep-admitted").is_some(), "{hop}");
    assert!(
        header_of(hop, "x-wasmkeep-sender")
            .unwrap()
            .starts_with("a "),
        "{hop}"
    );
    assert_eq!(header_of(hop, "x-wasmkeep-forwarded"), Some("1"), "{hop}");
    // Backed off for a heartbeat horizon: the next wake stays, unsent.
    let r = wake(&a, "kept-0").await;
    assert_eq!((r.status, r.node.as_deref()), (200, Some("a")));
    assert_eq!(fake.hops().len(), 1);

    // Answered: returned as it is, and the next request follows the hint.
    fake.set(Answer::Serve);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (served, reply) = wake_until_hop(&a, &fake, "s", 1).await;
    assert_eq!(
        (reply.status, reply.node.as_deref()),
        (200, Some("fake")),
        "{}",
        reply.head
    );
    assert_eq!(reply.body, "ok");
    assert_eq!(lease_holder(&store, &served).await, None);
    let again = wake(&a, &served).await;
    assert_eq!(again.node.as_deref(), Some("fake"));
    let hops = fake.hops();
    let followed = hops.last().unwrap();
    assert!(
        followed.contains(&format!("/actor/t/{served}/fired")),
        "{followed}"
    );
    // An ordinary forward now: no mark.
    assert_eq!(header_of(followed, "x-wasmkeep-placed"), None, "{followed}");

    // Ambiguous (read, then the connection dropped): 503, not run here,
    // and the outcome unknown (it may have run there): no `Retry-After`.
    fake.set(Answer::Drop);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let before = fake.hops().len();
    let (dropped, reply) = wake_until_hop(&a, &fake, "d", before).await;
    assert_eq!(reply.status, 503, "{}", reply.head);
    assert_eq!(reply.retry_after, None, "{}", reply.head);
    assert_eq!(
        reply.header("x-wasmkeep-outcome").as_deref(),
        Some("unknown"),
        "{}",
        reply.head
    );
    assert_eq!(lease_holder(&store, &dropped).await, None);

    // A client's mark, with the forwarded mark: handled as that forward
    // is (here, never forwarded again), and never counted as placed.
    fake.set(Answer::Serve);
    let r = send(
        a.addr,
        "GET",
        "/actor/t/forged/fired",
        "x-wasmkeep-placed: 900\r\nx-wasmkeep-forwarded: 1\r\nx-wasmkeep-app: default/default\r\n",
    )
    .await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("a")),
        "{}",
        r.head
    );
    assert_eq!(r.header("x-wasmkeep-placed"), None);
    assert!(!fake.hops().iter().any(|h| h.contains("/actor/t/forged/")));
}

/// Section 1: an upgrade is never placed, while a request for another
/// actor at the same moment is; an `actors.call` is placed, and runs here
/// after a refusal (section 6).
#[tokio::test(flavor = "multi_thread")]
async fn upgrades_are_never_placed_and_calls_are() {
    if over_tls() {
        return;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let data = tempfile::tempdir().unwrap();
    // The peer first, so a's first listing finds it. The process's
    // resident limit (14 with the tests' 16 instances): x0, the caller
    // below, stays resident.
    let fake = fake_peer(&store, Answer::Serve).await;
    let a = placing_node("a", data.path(), &store, usize::MAX).await;
    settle().await;
    // Under the floor (2 of 14 is 142): kept.
    for name in ["x0", "x1", "x2"] {
        assert_eq!(wake(&a, name).await.node.as_deref(), Some("a"));
    }
    // The peer is in a's view and fresh once a request is placed there.
    let _ = wake_until_hop(&a, &fake, "p", 0).await;
    let before = fake.hops().len();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(upgrade_status(a.addr, "/actor/t/socket").await, 101);
    assert_eq!(lease_holder(&store, "socket").await.as_deref(), Some("a"));
    assert_eq!(fake.hops().len(), before, "an upgrade was sent to the peer");

    // A call from x0 to an actor nobody holds: placed, the mark signed in
    // the call's own signature.
    let before = fake.hops().len();
    let r = send(a.addr, "POST", "/actor/t/x0/call?to=t%2Fcallee", "").await;
    assert_eq!(r.status, 200, "{}", r.head);
    let hops = fake.hops();
    assert_eq!(hops.len(), before + 1, "{}", r.body);
    let call = hops.last().unwrap();
    assert!(call.contains("/actor/t/callee/fired"), "{call}");
    assert!(header_of(call, "x-wasmkeep-internal").is_some(), "{call}");
    assert!(header_of(call, "x-wasmkeep-placed").is_some(), "{call}");
    assert!(header_of(call, "x-wasmkeep-chain").is_some(), "{call}");
    assert_eq!(lease_holder(&store, "callee").await, None);

    // Refused: the call runs here.
    fake.set(Answer::Refuse);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let before = fake.hops().len();
    let r = send(a.addr, "POST", "/actor/t/x0/call?to=t%2Fcallee2", "").await;
    assert_eq!(r.status, 200, "{}", r.head);
    assert_eq!(fake.hops().len(), before + 1);
    assert_eq!(lease_holder(&store, "callee2").await.as_deref(), Some("a"));
}

/// Sections 5 and 9: between two real nodes, the fuller one places a
/// cold actor on the lighter one, which acquires it and is then reached
/// through the owner hint; a node with placement off receives nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_fuller_node_places_a_cold_actor_on_a_lighter_node() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = placing_node("a", dirs.0.path(), &store, 10).await;
    let b = placing_node("b", dirs.1.path(), &store, 10).await;
    settle().await;
    let mut placed = None;
    for i in 0..9 {
        let name = format!("w{i}");
        let r = wake(&a, &name).await;
        assert_eq!(r.status, 200, "{name}: {}", r.head);
        // Under the floor (0 and 1 of 10): kept.
        if i < 2 {
            assert_eq!(r.node.as_deref(), Some("a"), "{name}");
        }
        if r.node.as_deref() == Some("b") {
            placed = Some(name);
            break;
        }
    }
    let placed = placed.expect("nothing was placed on b");
    assert_eq!(lease_holder(&store, &placed).await.as_deref(), Some("b"));
    // b took it as a placed event (its mark checked), not as a plain
    // forward; a counted it as placed (both nodes share this process's
    // metrics: nextest runs each test alone).
    let page = wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default());
    let count = |series: &str| -> u64 {
        page.lines()
            .find_map(|l| l.strip_prefix(series)?.trim().parse().ok())
            .unwrap_or(0)
    };
    assert_eq!(
        count(
            r#"wasmkeep_placements_received_total{tenant="default",app="default",outcome="acquired"}"#
        ),
        1,
        "{page}"
    );
    assert_eq!(
        count(
            r#"wasmkeep_placements_total{tenant="default",app="default",kind="request",outcome="placed"}"#
        ),
        1,
        "{page}"
    );
    // The next request for it goes to b, and b runs it (no second hop).
    let r = wake(&a, &placed).await;
    assert_eq!((r.status, r.node.as_deref()), (200, Some("b")));
    drop(b);

    // b2 with placement off: never a candidate.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = placing_node("a", dirs.0.path(), &store, 10).await;
    let _b = placing_node_with("b", dirs.1.path(), &store, 10, false).await;
    settle().await;
    for i in 0..8 {
        let r = wake(&a, &format!("k{i}")).await;
        assert_eq!((r.status, r.node.as_deref()), (200, Some("a")), "k{i}");
    }
}

/// "How a joining node fills": two loaded nodes, a third joins empty. The
/// others' cold wake-ups fill it, within the arrival bound, and nothing
/// placed moves again: every actor stays with the node that took it.
#[tokio::test(flavor = "multi_thread")]
async fn a_joining_node_fills_from_wake_ups_without_flapping() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs = [
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    ];
    // a alone takes its five; then b takes its five, a being fuller.
    let a = placing_node("a", dirs[0].path(), &store, 10).await;
    for i in 0..5 {
        assert_eq!(wake(&a, &format!("a{i}")).await.node.as_deref(), Some("a"));
    }
    let b = placing_node("b", dirs[1].path(), &store, 10).await;
    settle().await;
    for i in 0..5 {
        assert_eq!(wake(&b, &format!("b{i}")).await.node.as_deref(), Some("b"));
    }
    let c = placing_node("c", dirs[2].path(), &store, 10).await;
    settle().await;
    let mut ran = Vec::new();
    for i in 0..8 {
        let (node, name) = if i % 2 == 0 {
            (&a, format!("n{i}"))
        } else {
            (&b, format!("n{i}"))
        };
        let r = wake(node, &name).await;
        assert_eq!(r.status, 200, "{name}: {}", r.head);
        ran.push((name, r.node.unwrap()));
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    let on_c = ran.iter().filter(|(_, n)| n == "c").count();
    assert!(on_c >= 1, "c took nothing: {ran:?}");
    // The arrival bound for the fullest sender (9 of 10: t = 7).
    assert!(on_c <= 7, "{ran:?}");
    settle().await;
    for (name, node) in &ran {
        assert_eq!(
            lease_holder(&store, name).await.as_deref(),
            Some(node.as_str()),
            "{name}"
        );
        // Asked again on either sender: the same node answers.
        let r = wake(&a, name).await;
        assert_eq!(r.node.as_deref(), Some(node.as_str()), "{name}");
    }
    for i in 0..5 {
        assert_eq!(
            lease_holder(&store, &format!("a{i}")).await.as_deref(),
            Some("a")
        );
        assert_eq!(
            lease_holder(&store, &format!("b{i}")).await.as_deref(),
            Some("b")
        );
    }
    drop(c);
}

/// Review item 1: a placed wake-up for a class the receiver does not serve
/// yet (a deploy adding it on its way) is refused with the mark, and the
/// sender, which serves the class, runs it.
#[tokio::test(flavor = "multi_thread")]
async fn a_class_the_receiver_lacks_runs_on_the_sender() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = placing_node_full("a", dirs.0.path(), &store, 10, true, &["t", "u"], None).await;
    let b = placing_node_full("b", dirs.1.path(), &store, 10, true, &["t"], None).await;
    settle().await;
    let refused = |o| {
        metric(&format!(
            r#"wasmkeep_placements_total{{{APP},kind="request",outcome="{o}"}}"#
        ))
    };
    for i in 0..30 {
        let r = send(a.addr, "GET", &format!("/actor/u/z{i}/fired"), "").await;
        assert_eq!(
            (r.status, r.node.as_deref()),
            (200, Some("a")),
            "z{i}: {}",
            r.head
        );
        if refused("refused") > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(refused("refused") >= 1, "never placed on b");
    assert_eq!(refused("placed"), 0);
    drop(b);
}

/// A real receiver's live count refuses a hop its advertised load did not
/// show coming (`fuller`), and the sender runs the request or `actors.call`.
async fn fuller_then_run_here(call: bool) {
    if over_tls() {
        return;
    }
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let every = Duration::from_secs(3);
    let a_bucket = Arc::new(Flaky::new(Arc::clone(&store), Arc::default()));
    let a_store: Arc<dyn ObjectStore> = a_bucket.clone();
    let a = placing_node_full("a", dirs.0.path(), &a_store, 10, true, &["t"], Some(every)).await;
    for i in 0..9 {
        assert_eq!(wake(&a, &format!("a{i}")).await.node.as_deref(), Some("a"));
    }
    // Exercise delayed discovery without CPU load or a startup-phase guess:
    // no listing, and no read of b's object by key (b's hello asks for one).
    a_bucket.fail_node_list.store(true, Ordering::SeqCst);
    *a_bucket.fail_gets_of.lock().unwrap() = Some("nodes/b".to_owned());
    let b_bucket = Arc::new(Flaky::new(Arc::clone(&store), Arc::default()));
    let b_store: Arc<dyn ObjectStore> = b_bucket.clone();
    let b = placing_node_full(
        "b",
        dirs.1.path(),
        &b_store,
        10,
        true,
        &["t"],
        Some(Duration::from_millis(100)),
    )
    .await;
    assert_eq!(wake(&b, "b0").await.node.as_deref(), Some("b"));
    // Keep a real published load (0 or 1 resident), but renew the heartbeat
    // normally. Even a refresh during the fills below must not reveal them:
    // the test is about the receiver checking live, not advertised, capacity.
    use object_store::ObjectStoreExt as _;
    let body = store
        .get(&"nodes/b".into())
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    let load = body.lines().find(|line| line.starts_with("load ")).unwrap();
    *b_bucket.frozen_node_load.lock().unwrap() = Some(load.to_owned());
    let advertised = wasmkeep_cluster::membership::NodeInfo::decode(body.as_bytes())
        .unwrap()
        .load
        .unwrap();
    assert!(advertised.actors <= 1);
    assert_eq!(advertised.actor_room, Some(10 - advertised.actors));
    // b0 has an owner, so this probe never places or changes a's load. Until
    // discovery succeeds a cannot route to b. Its first successful forward
    // proves it installed b's fresh heartbeat, including the load above.
    let probe = wake(&a, "b0").await;
    assert_eq!(
        probe.status, 503,
        "discovery was meant to be held: {}",
        probe.head
    );
    a_bucket.fail_node_list.store(false, Ordering::SeqCst);
    *a_bucket.fail_gets_of.lock().unwrap() = None;
    eventually(
        "a's fresh view routes to b with its advertised load",
        async || {
            let r = wake(&a, "b0").await;
            r.status == 200 && r.node.as_deref() == Some("b")
        },
    )
    .await;
    for i in 1..7 {
        assert_eq!(wake(&b, &format!("b{i}")).await.node.as_deref(), Some("b"));
    }
    let fuller = || {
        metric(&format!(
            r#"wasmkeep_placements_received_total{{{APP},outcome="fuller"}}"#
        ))
    };
    let kind = if call { "call" } else { "request" };
    let refused = || {
        metric(&format!(
            r#"wasmkeep_placements_total{{{APP},kind="{kind}",outcome="refused"}}"#
        ))
    };
    // a at 9 of 10 (900), b seen at 0 or 1: t = 7, positive share, sent;
    // b holds 7, so the actor would put it at 8, above 7: refused.
    let r = if call {
        send(a.addr, "POST", "/actor/t/a0/call?to=t%2Fz", "").await
    } else {
        wake(&a, "z").await
    };
    assert_eq!(
        r.status,
        200,
        "{}: {} (fuller {}, refused {})",
        r.head,
        r.body,
        fuller(),
        refused()
    );
    assert_eq!((fuller(), refused()), (1, 1));
    if !call {
        assert_eq!(r.node.as_deref(), Some("a"));
    }
    assert_eq!(lease_holder(&store, "z").await.as_deref(), Some("a"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_the_receiver_finds_fuller_runs_here() {
    fuller_then_run_here(false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_the_receiver_finds_fuller_runs_here() {
    fuller_then_run_here(true).await;
}

/// Review item 7: an alarm due for an actor not in memory on a full node
/// is delivered as alarms are (ADR 0009), never placed, while a lighter
/// node announcing `place` is there.
#[tokio::test(flavor = "multi_thread")]
async fn an_alarm_on_a_full_node_is_never_placed() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = placing_node("a", dirs.0.path(), &store, 3).await;
    let due = epoch_ms() + 3_000;
    let r = send(a.addr, "POST", &format!("/actor/t/x/alarm?at={due}"), "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    for name in ["f1", "f2"] {
        assert_eq!(wake(&a, name).await.node.as_deref(), Some("a"));
    }
    let b = placing_node("b", dirs.1.path(), &store, 10).await;
    settle().await;
    // A WebSocket (never placed) takes x's slot on the full node.
    assert_eq!(upgrade_status(a.addr, "/actor/t/f3").await, 101);
    eventually("the alarm was delivered", async || {
        index_entries_for(&store, "x").await.is_empty()
    })
    .await;
    let placed: u64 = ["request", "call"]
        .iter()
        .flat_map(|k| ["placed", "refused", "unsent", "failed"].map(move |o| (k, o)))
        .map(|(k, o)| {
            metric(&format!(
                r#"wasmkeep_placements_total{{{APP},kind="{k}",outcome="{o}"}}"#
            ))
        })
        .sum();
    assert_eq!(placed, 0, "something was placed");
    drop(b);
}
