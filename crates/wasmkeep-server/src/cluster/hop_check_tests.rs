//! What a receiver makes of a signed hop, over every port, mode, secret
//! state and kind of signature: whether it is a node's call, whether it is
//! refused with 503, what it counts and what it spends on the public port.
//! The table was captured from 0.8.0's code, before the hop's signature
//! was checked once instead of in each of its readers. One change since,
//! on purpose: a bad signature on the cluster port, refused there before
//! any handler read it, is now counted in `wasmkeep_signatures_bad_total`
//! too (it was counted only where a handler read it, off the cluster
//! port), as the metric's documentation says.

//!
//! The counts read here (`wasmkeep_signatures_bad_total` and the test-only
//! count of verified signatures) are process-wide: these tests rely on
//! nextest running each test in a process of its own. Under `cargo test`,
//! tests running beside each other would move them.

use super::*;

const SECRET: &str = "abababababababababababababababababababababababababababababababab";
const PATH: &str = "/actor/c/x/";

/// Node `b` of `t/default`; `a` is live in it, `x` is a node of another
/// app.
async fn receiver(refuse: bool, secret: bool) -> Arc<Cluster> {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    if secret {
        store
            .put(
                &StorePath::from(SECRET_KEY),
                SECRET.as_bytes().to_vec().into(),
            )
            .await
            .unwrap();
    }
    let mut config = ClusterConfig::new("b", "http://b", store);
    config.heartbeat = Duration::from_secs(3_600);
    config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 1_000).unwrap();
    if refuse {
        config.cluster_plaintext = ClusterPlaintext::Refuse;
        config.cluster_tls = Some(ClusterTlsConfig::new("https://b.test:7443"));
    }
    let c = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    if secret {
        c.load_secret().await;
    }
    let peer = |app: &str| Peer {
        plain: Some("http://x".into()),
        tls: None,
        app: app.into(),
        drain: None,
        can_take: true,
        can_handoff: true,
        can_end: true,
        can_blobs: true,
        instance: None,
        can_place: false,
        traces: false,
        seen: None,
    };
    *c.peers.write().unwrap() = HashMap::from([
        ("a".to_owned(), peer("t/default")),
        ("x".to_owned(), peer("t/other")),
    ]);
    c
}

/// A call to `b` at [`PATH`], of kind `kind`.
fn hop(kind: &str) -> hyper::HeaderMap {
    let mut headers = hyper::HeaderMap::new();
    if kind == "absent" {
        return headers;
    }
    headers.insert(APP, "t/default".parse().unwrap());
    // Past the skew a record allows after its start ([`Seen::floor`]),
    // so that a burn on the public port is recorded.
    let ts = now_ms() + 30_000;
    let (nonce, hash) = (format!("{ts:x}"), body_sha256(b""));
    let mac = signature(
        SECRET,
        ts,
        &nonce,
        "b",
        &hyper::Method::POST,
        PATH,
        &hash,
        &headers,
    );
    let given = if kind == "bad" {
        format!("v4 {ts} {nonce} {hash} {}", "00".repeat(32))
    } else {
        format!("v4 {ts} {nonce} {hash} {mac}")
    };
    headers.insert(INTERNAL, given.parse().unwrap());
    let sender = |node: &str| {
        sender_value(SECRET, "internal", &mac, node)
            .parse()
            .unwrap()
    };
    match kind {
        "valid" | "bad" => {
            headers.insert(SENDER, sender("a"));
        }
        "no-sender" => {}
        "sender-twice" => {
            headers.append(SENDER, sender("a"));
            headers.append(SENDER, sender("a"));
        }
        "sender-unbound" => {
            headers.insert(
                SENDER,
                sender_value(SECRET, "internal", &"11".repeat(32), "a")
                    .parse()
                    .unwrap(),
            );
        }
        "sender-other-app" => {
            headers.insert(SENDER, sender("x"));
        }
        "sender-not-live" => {
            headers.insert(SENDER, sender("gone"));
        }
        other => panic!("{other}"),
    }
    headers
}

fn bad_signatures() -> u64 {
    crate::metrics::METRICS
        .signatures_bad
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// One line per case, as a request to an actor path goes: the 503
/// refusal first (the router's), then, for a hop not refused, whether it
/// is a node's call (the actor's handler); the bad-signature count and
/// the public port's burn.
async fn table() -> String {
    let mut out = String::new();
    for refuse in [false, true] {
        for secret in [true, false] {
            for cluster_port in [true, false] {
                for kind in KINDS {
                    let c = receiver(refuse, secret).await;
                    let headers = hop(kind);
                    let m = hyper::Method::POST;
                    let before = bad_signatures();
                    let (refused, internal) = match c.check_hop(&m, PATH, &headers, cluster_port) {
                        HopCheck::Refused(why) => (Some(why), false),
                        HopCheck::Signed(_) => (None, true),
                        HopCheck::Outside => (None, false),
                    };
                    let counted = bad_signatures() - before;
                    out.push_str(&line(
                        &c,
                        refuse,
                        secret,
                        cluster_port,
                        kind,
                        &headers,
                        internal,
                        refused,
                        counted,
                    ));
                }
            }
        }
    }
    out
}

/// The kinds of hop of the table.
const KINDS: [&str; 8] = [
    "absent",
    "valid",
    "bad",
    "no-sender",
    "sender-twice",
    "sender-unbound",
    "sender-other-app",
    "sender-not-live",
];

/// One line of the table, the public port's burn read from `c`.
#[allow(clippy::too_many_arguments)]
fn line(
    c: &Cluster,
    refuse: bool,
    secret: bool,
    cluster_port: bool,
    kind: &str,
    headers: &hyper::HeaderMap,
    internal: bool,
    refused: Option<&str>,
    counted: u64,
) -> String {
    let burnt = headers
        .get(INTERNAL)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            let f: Vec<&str> = v.split(' ').collect();
            Some((f[1].parse::<u64>().ok()?, unhex32(f[4])?))
        })
        .is_some_and(|(ts, mac)| c.seen_public.holds(&mac, ts));
    format!(
        "refuse={refuse} secret={secret} cluster_port={cluster_port} {kind}: \
         internal={internal} refused={refused:?} bad={counted} burnt={burnt}\n"
    )
}

#[tokio::test]
async fn a_hop_is_judged_as_before() {
    let table = table().await;
    assert_eq!(table, EXPECTED, "\n{table}");
}

/// [`verify`] over fixed inputs: the window's edges and malformed values.
#[test]
fn verify_takes_exactly_these_values() {
    let ts = 1_700_000_000_000u64;
    let hash = body_sha256(b"");
    let headers = hyper::HeaderMap::new();
    let m = hyper::Method::POST;
    let mac = |nonce: &str| signature(SECRET, ts, nonce, "b", &m, PATH, &hash, &headers);
    let check = |given: &str, now: u64| verify(SECRET, given, now, "b", &m, PATH, &headers);
    let good = format!("v4 {ts} ab {hash} {}", mac("ab"));
    assert!(check(&good, ts).is_some());
    assert!(check(&good, ts + SIGNATURE_WINDOW_MS).is_some());
    assert!(check(&good, ts - SIGNATURE_WINDOW_MS).is_some());
    assert!(check(&good, ts + SIGNATURE_WINDOW_MS + 1).is_none());
    assert!(check(&good, ts - SIGNATURE_WINDOW_MS - 1).is_none());
    let signed = check(&good, ts).unwrap();
    assert_eq!((signed.ts, signed.body.as_str()), (ts, hash.as_str()));
    assert_eq!(signed.mac, unhex32(&mac("ab")).unwrap());
    assert!(signed.sender.is_none());
    // A nonce of 64 hex digits is taken, of 65 not, nor an empty one.
    let n64 = "a".repeat(64);
    assert!(check(&format!("v4 {ts} {n64} {hash} {}", mac(&n64)), ts).is_some());
    let n65 = "a".repeat(65);
    assert!(check(&format!("v4 {ts} {n65} {hash} {}", mac(&n65)), ts).is_none());
    assert!(check(&format!("v4 {ts}  {hash} {}", mac("")), ts).is_none());
    // Uppercase hex, in the nonce, the hash or the MAC.
    assert!(check(&format!("v4 {ts} AB {hash} {}", mac("AB")), ts).is_none());
    let upper = hash.to_uppercase();
    assert!(check(&format!("v4 {ts} ab {upper} {}", mac("ab")), ts).is_none());
    assert!(
        check(
            &format!("v4 {ts} ab {hash} {}", mac("ab").to_uppercase()),
            ts
        )
        .is_none()
    );
    // A sixth field, a field missing, another version, another node.
    assert!(check(&format!("{good} x"), ts).is_none());
    assert!(check(&format!("v4 {ts} ab {hash}"), ts).is_none());
    assert!(check(&good.replacen("v4", "v3", 1), ts).is_none());
    assert!(verify(SECRET, &good, ts, "c", &m, PATH, &headers).is_none());
}

/// A request to [`PATH`] with `headers`, as it arrived on the public
/// port.
fn request(headers: hyper::HeaderMap) -> Request<()> {
    let mut req = Request::post(PATH).body(()).unwrap();
    *req.headers_mut() = headers;
    req
}

fn verified() -> u64 {
    signing::VERIFIED.load(std::sync::atomic::Ordering::Relaxed)
}

/// The router judges a hop once; the handlers after it (the ingress
/// check, twice with rate limits, then the actor's) read its verdict and
/// check nothing again.
#[tokio::test]
async fn a_hop_is_verified_once() {
    let b = receiver(false, true).await;
    let mut req = request(hop("valid"));
    let before = verified();
    assert_eq!(b.judge_hop(&mut req), None);
    for _ in 0..3 {
        assert!(b.signed_hop(&req).unwrap().is_some());
    }
    assert_eq!(verified() - before, 1);
    // The verdict is the router's: what changes after does not undo it
    // (the body and the replay checks still follow).
    req.headers_mut()
        .insert(INTERNAL, "v4 1 a b c".parse().unwrap());
    assert!(b.signed_hop(&req).unwrap().is_some());
    assert_eq!(verified() - before, 1);
    // Outside and refused hops carry their verdict too.
    let mut bad = request(hop("bad"));
    assert_eq!(b.judge_hop(&mut bad), None);
    let mut forged = request(hop("sender-unbound"));
    assert!(b.judge_hop(&mut forged).is_some());
    let at = verified();
    assert!(b.signed_hop(&bad).unwrap().is_none());
    assert!(b.signed_hop(&forged).is_err());
    assert_eq!(verified(), at);
}

/// A request no router judged, or judged by another runtime, is checked
/// where it is read: a verdict never stands in for a check it was not.
#[tokio::test]
async fn a_hop_without_its_own_verdict_is_checked_where_read() {
    let b = receiver(false, true).await;
    let req = request(hop("valid"));
    let before = verified();
    assert!(b.signed_hop(&req).unwrap().is_some());
    assert_eq!(verified() - before, 1);
    // Judged by another runtime: checked again here, whatever that
    // runtime found (outside here, though signed for `b`; signed there,
    // though junk).
    let mut other = request(hop("valid"));
    other.extensions_mut().insert(CheckedHop {
        by: "another runtime".into(),
        on_cluster_port: false,
        hop: HopCheck::Outside,
    });
    assert!(b.signed_hop(&other).unwrap().is_some());
    let mut junk = request(hop("bad"));
    junk.extensions_mut().insert(CheckedHop {
        by: "another runtime".into(),
        on_cluster_port: false,
        hop: match b.check_hop(&hyper::Method::POST, PATH, &hop("valid"), false) {
            HopCheck::Signed(s) => HopCheck::Signed(s),
            other => panic!("{other:?}"),
        },
    });
    assert!(b.signed_hop(&junk).unwrap().is_none());
    assert_eq!(verified() - before, 4);
}

/// Every app of a process runs under the same node id: a verdict is its
/// own runtime's, never honoured by another runtime of that id.
#[tokio::test]
async fn a_verdict_is_honoured_only_by_the_runtime_that_made_it() {
    let (shop, blog) = (receiver(false, true).await, receiver(false, true).await);
    let mut req = request(hop("valid"));
    assert_eq!(shop.judge_hop(&mut req), None);
    // Changed after the verdict: the runtime that judged it keeps it, the
    // other checks it again and finds no node's call.
    req.headers_mut()
        .insert(INTERNAL, "v4 1 a b c".parse().unwrap());
    assert!(shop.signed_hop(&req).unwrap().is_some());
    assert!(blog.signed_hop(&req).unwrap().is_none());
}

/// A hop the router refused stays refused for every handler after it: a
/// cached refusal is never read as a client's request.
#[tokio::test]
async fn a_refused_hop_stays_refused() {
    let b = receiver(false, true).await;
    let mut forged = request(hop("sender-unbound"));
    let why = b.judge_hop(&mut forged).unwrap();
    assert_eq!(b.signed_hop(&forged).unwrap_err(), why);
    // Unjudged, it is refused where it is read too.
    let forged = request(hop("sender-unbound"));
    assert_eq!(b.signed_hop(&forged).unwrap_err(), why);
}

/// The same table as [`a_hop_is_judged_as_before`], driven as a request
/// goes: the router's [`Cluster::judge_hop`], then a handler's
/// [`Cluster::signed_hop`] reading its verdict.
#[tokio::test]
async fn a_routed_hop_is_judged_as_before() {
    let pins = Arc::new(crate::cluster_tls::pinning::PinSet::default());
    let key = crate::cluster_tls::pinning::Identity::generate("a", "a.test").unwrap();
    let mut out = String::new();
    for refuse in [false, true] {
        for secret in [true, false] {
            for cluster_port in [true, false] {
                for kind in KINDS {
                    let c = receiver(refuse, secret).await;
                    let headers = hop(kind);
                    let mut req = request(headers.clone());
                    if cluster_port {
                        req.extensions_mut().insert(ArrivedOnClusterPort(
                            crate::http::ClusterPeer::for_test(key.pin(), Arc::clone(&pins)),
                        ));
                    }
                    let before = bad_signatures();
                    let refused = c.judge_hop(&mut req);
                    let read = c.signed_hop(&req);
                    assert_eq!(read.as_ref().err().copied(), refused, "{kind}");
                    let internal = matches!(read, Ok(Some(_)));
                    let counted = bad_signatures() - before;
                    out.push_str(&line(
                        &c,
                        refuse,
                        secret,
                        cluster_port,
                        kind,
                        &headers,
                        internal,
                        refused,
                        counted,
                    ));
                }
            }
        }
    }
    assert_eq!(out, EXPECTED, "\n{out}");
}

const EXPECTED: &str = "\
refuse=false secret=true cluster_port=true absent: internal=false refused=None bad=0 burnt=false\n\
refuse=false secret=true cluster_port=true valid: internal=true refused=None bad=0 burnt=false\n\
refuse=false secret=true cluster_port=true bad: internal=false refused=Some(\"bad, expired or misaddressed signature\") bad=1 burnt=false\n\
refuse=false secret=true cluster_port=true no-sender: internal=false refused=Some(\"hop names no sender (every node since 0.6.0 names it)\") bad=0 burnt=false\n\
refuse=false secret=true cluster_port=true sender-twice: internal=false refused=Some(\"hop names its sender twice\") bad=0 burnt=false\n\
refuse=false secret=true cluster_port=true sender-unbound: internal=false refused=Some(\"hop's sender is not bound to its signature\") bad=0 burnt=false\n\
refuse=false secret=true cluster_port=true sender-other-app: internal=false refused=Some(\"hop's sender is a node of another app\") bad=0 burnt=false\n\
refuse=false secret=true cluster_port=true sender-not-live: internal=false refused=Some(\"hop's sender is not a live node\") bad=0 burnt=false\n\
refuse=false secret=true cluster_port=false absent: internal=false refused=None bad=0 burnt=false\n\
refuse=false secret=true cluster_port=false valid: internal=true refused=None bad=0 burnt=false\n\
refuse=false secret=true cluster_port=false bad: internal=false refused=None bad=1 burnt=false\n\
refuse=false secret=true cluster_port=false no-sender: internal=false refused=Some(\"hop names no sender (every node since 0.6.0 names it)\") bad=0 burnt=false\n\
refuse=false secret=true cluster_port=false sender-twice: internal=false refused=Some(\"hop names its sender twice\") bad=0 burnt=false\n\
refuse=false secret=true cluster_port=false sender-unbound: internal=false refused=Some(\"hop's sender is not bound to its signature\") bad=0 burnt=false\n\
refuse=false secret=true cluster_port=false sender-other-app: internal=false refused=Some(\"hop's sender is a node of another app\") bad=0 burnt=false\n\
refuse=false secret=true cluster_port=false sender-not-live: internal=true refused=None bad=0 burnt=false\n\
refuse=false secret=false cluster_port=true absent: internal=false refused=None bad=0 burnt=false\n\
refuse=false secret=false cluster_port=true valid: internal=false refused=Some(\"cluster secret not loaded yet\") bad=0 burnt=false\n\
refuse=false secret=false cluster_port=true bad: internal=false refused=Some(\"cluster secret not loaded yet\") bad=0 burnt=false\n\
refuse=false secret=false cluster_port=true no-sender: internal=false refused=Some(\"cluster secret not loaded yet\") bad=0 burnt=false\n\
refuse=false secret=false cluster_port=true sender-twice: internal=false refused=Some(\"cluster secret not loaded yet\") bad=0 burnt=false\n\
refuse=false secret=false cluster_port=true sender-unbound: internal=false refused=Some(\"cluster secret not loaded yet\") bad=0 burnt=false\n\
refuse=false secret=false cluster_port=true sender-other-app: internal=false refused=Some(\"cluster secret not loaded yet\") bad=0 burnt=false\n\
refuse=false secret=false cluster_port=true sender-not-live: internal=false refused=Some(\"cluster secret not loaded yet\") bad=0 burnt=false\n\
refuse=false secret=false cluster_port=false absent: internal=false refused=None bad=0 burnt=false\n\
refuse=false secret=false cluster_port=false valid: internal=false refused=None bad=0 burnt=false\n\
refuse=false secret=false cluster_port=false bad: internal=false refused=None bad=0 burnt=false\n\
refuse=false secret=false cluster_port=false no-sender: internal=false refused=None bad=0 burnt=false\n\
refuse=false secret=false cluster_port=false sender-twice: internal=false refused=None bad=0 burnt=false\n\
refuse=false secret=false cluster_port=false sender-unbound: internal=false refused=None bad=0 burnt=false\n\
refuse=false secret=false cluster_port=false sender-other-app: internal=false refused=None bad=0 burnt=false\n\
refuse=false secret=false cluster_port=false sender-not-live: internal=false refused=None bad=0 burnt=false\n\
refuse=true secret=true cluster_port=true absent: internal=false refused=None bad=0 burnt=false\n\
refuse=true secret=true cluster_port=true valid: internal=true refused=None bad=0 burnt=false\n\
refuse=true secret=true cluster_port=true bad: internal=false refused=Some(\"bad, expired or misaddressed signature\") bad=1 burnt=false\n\
refuse=true secret=true cluster_port=true no-sender: internal=false refused=Some(\"hop names no sender (every node since 0.6.0 names it)\") bad=0 burnt=false\n\
refuse=true secret=true cluster_port=true sender-twice: internal=false refused=Some(\"hop names its sender twice\") bad=0 burnt=false\n\
refuse=true secret=true cluster_port=true sender-unbound: internal=false refused=Some(\"hop's sender is not bound to its signature\") bad=0 burnt=false\n\
refuse=true secret=true cluster_port=true sender-other-app: internal=false refused=Some(\"hop's sender is a node of another app\") bad=0 burnt=false\n\
refuse=true secret=true cluster_port=true sender-not-live: internal=false refused=Some(\"hop's sender is not a live node\") bad=0 burnt=false\n\
refuse=true secret=true cluster_port=false absent: internal=false refused=None bad=0 burnt=false\n\
refuse=true secret=true cluster_port=false valid: internal=false refused=None bad=0 burnt=true\n\
refuse=true secret=true cluster_port=false bad: internal=false refused=None bad=0 burnt=false\n\
refuse=true secret=true cluster_port=false no-sender: internal=false refused=None bad=0 burnt=true\n\
refuse=true secret=true cluster_port=false sender-twice: internal=false refused=None bad=0 burnt=true\n\
refuse=true secret=true cluster_port=false sender-unbound: internal=false refused=None bad=0 burnt=true\n\
refuse=true secret=true cluster_port=false sender-other-app: internal=false refused=None bad=0 burnt=true\n\
refuse=true secret=true cluster_port=false sender-not-live: internal=false refused=None bad=0 burnt=true\n\
refuse=true secret=false cluster_port=true absent: internal=false refused=None bad=0 burnt=false\n\
refuse=true secret=false cluster_port=true valid: internal=false refused=Some(\"cluster secret not loaded yet\") bad=0 burnt=false\n\
refuse=true secret=false cluster_port=true bad: internal=false refused=Some(\"cluster secret not loaded yet\") bad=0 burnt=false\n\
refuse=true secret=false cluster_port=true no-sender: internal=false refused=Some(\"cluster secret not loaded yet\") bad=0 burnt=false\n\
refuse=true secret=false cluster_port=true sender-twice: internal=false refused=Some(\"cluster secret not loaded yet\") bad=0 burnt=false\n\
refuse=true secret=false cluster_port=true sender-unbound: internal=false refused=Some(\"cluster secret not loaded yet\") bad=0 burnt=false\n\
refuse=true secret=false cluster_port=true sender-other-app: internal=false refused=Some(\"cluster secret not loaded yet\") bad=0 burnt=false\n\
refuse=true secret=false cluster_port=true sender-not-live: internal=false refused=Some(\"cluster secret not loaded yet\") bad=0 burnt=false\n\
refuse=true secret=false cluster_port=false absent: internal=false refused=None bad=0 burnt=false\n\
refuse=true secret=false cluster_port=false valid: internal=false refused=None bad=0 burnt=false\n\
refuse=true secret=false cluster_port=false bad: internal=false refused=None bad=0 burnt=false\n\
refuse=true secret=false cluster_port=false no-sender: internal=false refused=None bad=0 burnt=false\n\
refuse=true secret=false cluster_port=false sender-twice: internal=false refused=None bad=0 burnt=false\n\
refuse=true secret=false cluster_port=false sender-unbound: internal=false refused=None bad=0 burnt=false\n\
refuse=true secret=false cluster_port=false sender-other-app: internal=false refused=None bad=0 burnt=false\n\
refuse=true secret=false cluster_port=false sender-not-live: internal=false refused=None bad=0 burnt=false\n\
";

/// Whose sampled flag a node follows (ADR 0044, section 4): a hop's that
/// authenticates as a node's, when that node announces `traces`. A node
/// that does not (tracing off, or a version before 0.9) passes on the
/// `traceparent` a client or a guest sent, under a signature or a proof
/// that checks all the same: its flag is not followed.
#[tokio::test]
async fn a_hop_is_followed_only_from_a_node_that_traces() {
    const CLIENT: &str = "00-feedfacefeedfacefeedfacefeedface-feedfacefeedface-01";
    let traces = |c: &Cluster, on: bool| {
        for peer in c.peers.write().unwrap().values_mut() {
            peer.traces = on;
        }
    };
    let b = receiver(false, true).await;
    // A signed call from `a`, which does not announce the word.
    assert!(!b.traced_sender(&mut request(hop("valid")), None));
    traces(&b, true);
    // It does now. The signature is checked once: the router's judgement
    // after it reads the verdict.
    let before = verified();
    let mut call = request(hop("valid"));
    assert!(b.traced_sender(&mut call, None));
    assert_eq!(b.judge_hop(&mut call), None);
    assert!(b.signed_hop(&call).unwrap().is_some());
    assert_eq!(verified() - before, 1);
    // The verdict is the public port's: on the cluster port, which judges
    // a signature differently, it is not read. Judged again there (the
    // sender is not this connection's, which the listener checks; the
    // signature itself holds), and what is kept is then that port's.
    let mut moved = request(hop("valid"));
    assert!(b.traced_sender(&mut moved, None));
    moved
        .extensions_mut()
        .insert(ArrivedOnClusterPort(crate::http::ClusterPeer::for_test(
            [7; 32],
            Arc::new(crate::cluster_tls::pinning::PinSet::default()),
        )));
    let at = verified();
    let _ = b.judge_hop(&mut moved);
    assert_eq!(
        verified() - at,
        1,
        "a plain-port verdict read on the cluster port"
    );
    assert!(
        moved
            .extensions()
            .get::<CheckedHop>()
            .unwrap()
            .on_cluster_port
    );
    let _ = b.signed_hop(&moved);
    assert_eq!(verified() - at, 1);
    // A refused hop stays refused after it, and is no node's.
    let mut forged = request(hop("sender-unbound"));
    assert!(!b.traced_sender(&mut forged, None));
    assert!(b.judge_hop(&mut forged).is_some());
    // Nothing that does not authenticate as a live node of this app's. A
    // sender not listed is taken over plain HTTP, and announces nothing.
    for kind in [
        "absent",
        "bad",
        "no-sender",
        "sender-twice",
        "sender-unbound",
        "sender-other-app",
        "sender-not-live",
    ] {
        assert!(!b.traced_sender(&mut request(hop(kind)), None), "{kind}");
    }
    // No secret loaded: nothing checks.
    let cold = receiver(false, false).await;
    traces(&cold, true);
    assert!(!cold.traced_sender(&mut request(hop("valid")), None));
    // In refuse mode a signed call on the public port is from outside.
    let refusing = receiver(true, true).await;
    traces(&refusing, true);
    assert!(!refusing.traced_sender(&mut request(hop("valid")), None));

    // A forward: its proof, from `a`, over the `traceparent` it carries.
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    store
        .put(
            &StorePath::from(SECRET_KEY),
            SECRET.as_bytes().to_vec().into(),
        )
        .await
        .unwrap();
    let mut config = ClusterConfig::new("a", "http://a", store);
    config.heartbeat = Duration::from_secs(3_600);
    config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 1_000).unwrap();
    let a = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    a.load_secret().await;
    let forward = || async {
        let mut req = request(hyper::HeaderMap::new());
        req.headers_mut()
            .insert("traceparent", CLIENT.parse().unwrap());
        req.headers_mut().insert(APP, "t/default".parse().unwrap());
        req.extensions_mut()
            .insert(crate::http::PeerAddress("192.0.2.1:1234".parse().unwrap()));
        a.sign_admission("b", PATH, &mut req).await.unwrap();
        req
    };
    assert!(b.traced_sender(&mut forward().await, None));
    assert!(!refusing.traced_sender(&mut forward().await, None));
    // The header changed after the proof: the proof fails, and the forward
    // is a client's request (ADR 0043, section 7).
    let mut changed = forward().await;
    changed.headers_mut().insert(
        "traceparent",
        "00-feedfacefeedfacefeedfacefeedface-feedfacefeedface-00"
            .parse()
            .unwrap(),
    );
    assert!(b.admission_checked(&changed).is_none());
    assert!(!b.traced_sender(&mut changed, None));
    // No proof (no secret loaded at the sender): from outside.
    let mut bare = request(hyper::HeaderMap::new());
    bare.headers_mut()
        .insert("traceparent", CLIENT.parse().unwrap());
    bare.headers_mut().insert(FORWARDED, "1".parse().unwrap());
    assert!(!b.traced_sender(&mut bare, None));
    // `a` stops announcing the word: its proof still checks, and its
    // forward carries whatever the client sent.
    traces(&b, false);
    let mut passed_on = forward().await;
    assert!(b.admission_checked(&passed_on).is_some());
    assert!(!b.traced_sender(&mut passed_on, None));

    // On the cluster port the connection authenticates: the sender a hop
    // names must publish the certificate presented, and announce the word.
    let pins = Arc::new(crate::cluster_tls::pinning::PinSet::default());
    let (ka, kx) = (
        crate::cluster_tls::pinning::Identity::generate("a", "a.test").unwrap(),
        crate::cluster_tls::pinning::Identity::generate("x", "x.test").unwrap(),
    );
    let until = now_ms() + 60_000;
    pins.replace([("a", &[ka.pin()][..], until), ("x", &[kx.pin()][..], until)]);
    let on_a = crate::http::ClusterPeer::for_test(ka.pin(), Arc::clone(&pins));
    let on_x = crate::http::ClusterPeer::for_test(kx.pin(), Arc::clone(&pins));
    traces(&refusing, false);
    assert!(!refusing.traced_sender(&mut request(hop("valid")), Some(&on_a)));
    traces(&refusing, true);
    traces(&b, true);
    assert!(refusing.traced_sender(&mut request(hop("valid")), Some(&on_a)));
    // `a` named over another node's connection, no sender, or two.
    assert!(!refusing.traced_sender(&mut request(hop("valid")), Some(&on_x)));
    assert!(!refusing.traced_sender(&mut request(hop("no-sender")), Some(&on_a)));
    assert!(!refusing.traced_sender(&mut request(hop("sender-twice")), Some(&on_a)));
}
