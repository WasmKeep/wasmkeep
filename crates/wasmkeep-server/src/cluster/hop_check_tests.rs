//! What a receiver makes of a signed hop, over every port, mode, secret
//! state and kind of signature: whether it is a node's call, whether it is
//! refused with 503, what it counts and what it spends on the public port.
//! The table was captured from 0.8.0's code, before the hop's signature
//! was checked once instead of in each of its readers.

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
                for kind in [
                    "absent",
                    "valid",
                    "bad",
                    "no-sender",
                    "sender-twice",
                    "sender-unbound",
                    "sender-other-app",
                    "sender-not-live",
                ] {
                    let c = receiver(refuse, secret).await;
                    let headers = hop(kind);
                    let m = hyper::Method::POST;
                    let before = bad_signatures();
                    let refused = c.hop_refused(&m, PATH, &headers, cluster_port);
                    let internal = refused.is_none()
                        && c.is_internal(&m, PATH, &headers, cluster_port).is_some();
                    let counted = bad_signatures() - before;
                    let burnt = headers
                        .get(INTERNAL)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| {
                            let f: Vec<&str> = v.split(' ').collect();
                            Some((f[1].parse::<u64>().ok()?, unhex32(f[4])?))
                        })
                        .is_some_and(|(ts, mac)| c.seen_public.holds(&mac, ts));
                    out.push_str(&format!(
                        "refuse={refuse} secret={secret} cluster_port={cluster_port} {kind}: \
                         internal={internal} refused={refused:?} bad={counted} burnt={burnt}\n"
                    ));
                }
            }
        }
    }
    out
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

const EXPECTED: &str = "\
refuse=false secret=true cluster_port=true absent: internal=false refused=None bad=0 burnt=false\n\
refuse=false secret=true cluster_port=true valid: internal=true refused=None bad=0 burnt=false\n\
refuse=false secret=true cluster_port=true bad: internal=false refused=Some(\"bad, expired or misaddressed signature\") bad=0 burnt=false\n\
refuse=false secret=true cluster_port=true no-sender: internal=true refused=None bad=0 burnt=false\n\
refuse=false secret=true cluster_port=true sender-twice: internal=false refused=Some(\"hop names its sender twice\") bad=0 burnt=false\n\
refuse=false secret=true cluster_port=true sender-unbound: internal=false refused=Some(\"hop's sender is not bound to its signature\") bad=0 burnt=false\n\
refuse=false secret=true cluster_port=true sender-other-app: internal=false refused=Some(\"hop's sender is a node of another app\") bad=0 burnt=false\n\
refuse=false secret=true cluster_port=true sender-not-live: internal=false refused=Some(\"hop's sender is not a live node\") bad=0 burnt=false\n\
refuse=false secret=true cluster_port=false absent: internal=false refused=None bad=0 burnt=false\n\
refuse=false secret=true cluster_port=false valid: internal=true refused=None bad=0 burnt=false\n\
refuse=false secret=true cluster_port=false bad: internal=false refused=None bad=1 burnt=false\n\
refuse=false secret=true cluster_port=false no-sender: internal=true refused=None bad=0 burnt=false\n\
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
refuse=true secret=true cluster_port=true bad: internal=false refused=Some(\"bad, expired or misaddressed signature\") bad=0 burnt=false\n\
refuse=true secret=true cluster_port=true no-sender: internal=true refused=None bad=0 burnt=false\n\
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
