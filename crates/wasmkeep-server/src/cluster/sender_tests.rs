//! The sender inside the signed envelope (ADR 0038, section 5): every
//! hop a node signs names its own node id, bound to the hop's signature;
//! a receiver refuses a hop whose sender is forged, of another app, or
//! (on the cluster port) not a live node, and one whose connection's pin
//! the named sender does not publish.

use super::*;
use crate::cluster_tls::pinning::{Identity as TlsIdentity, PinSet};

const SECRET: &str = "abababababababababababababababababababababababababababababababab";

/// Node `id` of `t/default`, with the cluster secret loaded.
async fn node(id: &str) -> Arc<Cluster> {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    store
        .put(
            &StorePath::from(SECRET_KEY),
            SECRET.as_bytes().to_vec().into(),
        )
        .await
        .unwrap();
    let mut config = ClusterConfig::new(id, format!("http://{id}"), store);
    config.heartbeat = Duration::from_secs(3_600);
    config.lease = LeaseConfig::new(10_800_000, 5_000, 2_000, 1_000).unwrap();
    let c = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    c.load_secret().await;
    c
}

fn peer(app: &str) -> Peer {
    Peer {
        plain: Some("http://x".into()),
        tls: None,
        app: app.into(),
        drain: None,
        can_take: true,
        can_handoff: true,
        can_end: true,
        instance: None,
    }
}

/// A call to `to`, signed as node `sender` signs it (`None`: as a node of
/// an older version, without a sender).
fn call(to: &str, path: &str, sender: Option<&str>) -> hyper::HeaderMap {
    let mut headers = hyper::HeaderMap::new();
    headers.insert(APP, "t/default".parse().unwrap());
    let (ts, hash) = (now_ms(), body_sha256(b""));
    let nonce = format!("{:x}", ts);
    let mac = signature(
        SECRET,
        ts,
        &nonce,
        to,
        &hyper::Method::POST,
        path,
        &hash,
        &headers,
    );
    headers.insert(
        INTERNAL,
        format!("v4 {ts} {nonce} {hash} {mac}").parse().unwrap(),
    );
    if let Some(s) = sender {
        headers.insert(
            SENDER,
            sender_value(SECRET, "internal", &mac, s).parse().unwrap(),
        );
    }
    headers
}

const PATH: &str = "/actor/c/x/";

#[test]
fn a_sender_is_bound_to_its_hop() {
    let mac = "11".repeat(32);
    let value = sender_value(SECRET, "internal", &mac, "a");
    assert_eq!(
        verify_sender(SECRET, "internal", &mac, &value).as_deref(),
        Some("a")
    );
    // Another hop, another kind, another secret, another node: refused.
    assert_eq!(
        verify_sender(SECRET, "internal", &"22".repeat(32), &value),
        None
    );
    assert_eq!(verify_sender(SECRET, "admitted", &mac, &value), None);
    assert_eq!(
        verify_sender(&"cd".repeat(32), "internal", &mac, &value),
        None
    );
    let forged = value.replacen("a ", "b ", 1);
    assert_eq!(verify_sender(SECRET, "internal", &mac, &forged), None);
    assert_eq!(verify_sender(SECRET, "internal", &mac, "a"), None);
    assert_eq!(verify_sender(SECRET, "internal", &mac, ""), None);
}

#[tokio::test]
async fn a_hop_from_a_live_node_of_this_app_is_taken() {
    let b = node("b").await;
    *b.peers.write().unwrap() = HashMap::from([("a".to_owned(), peer("t/default"))]);
    let m = hyper::Method::POST;
    assert!(
        b.is_internal(&m, PATH, &call("b", PATH, Some("a")), true)
            .is_some()
    );
    // An older node's hop, without a sender: still taken (ADR 0038,
    // rolling upgrade, stage 1).
    assert!(
        b.is_internal(&m, PATH, &call("b", PATH, None), true)
            .is_some()
    );
}

#[tokio::test]
async fn a_hop_whose_sender_is_forged_of_another_app_or_not_live_is_refused() {
    let b = node("b").await;
    *b.peers.write().unwrap() = HashMap::from([
        ("a".to_owned(), peer("t/default")),
        ("x".to_owned(), peer("t/other")),
    ]);
    let m = hyper::Method::POST;
    // A node of another app.
    assert!(
        b.is_internal(&m, PATH, &call("b", PATH, Some("x")), true)
            .is_none()
    );
    // A node that is not live, on the cluster port.
    assert!(
        b.is_internal(&m, PATH, &call("b", PATH, Some("gone")), true)
            .is_none()
    );
    // A sender copied from another hop.
    let mut forged = call("b", PATH, Some("a"));
    let other = call("b", "/actor/c/y/", Some("a"));
    forged.insert(SENDER, other[SENDER].clone());
    assert!(b.is_internal(&m, PATH, &forged, true).is_none());
    // A sender twice.
    let mut twice = call("b", PATH, Some("a"));
    let again = twice[SENDER].clone();
    twice.append(SENDER, again);
    assert!(b.is_internal(&m, PATH, &twice, true).is_none());
    // A sender value that is not one.
    let mut junk = call("b", PATH, Some("a"));
    junk.insert(SENDER, "a".parse().unwrap());
    assert!(b.is_internal(&m, PATH, &junk, true).is_none());
}

#[tokio::test]
async fn a_hello_from_a_node_not_listed_yet_is_taken() {
    let b = node("b").await;
    let m = hyper::Method::POST;
    assert!(
        b.is_internal(&m, HELLO_PATH, &call("b", HELLO_PATH, Some("new")), true)
            .is_some()
    );
}

#[tokio::test]
async fn the_nodes_own_hops_carry_their_sender() {
    let a = node("a").await;
    let mut out = Request::get("/actor/c/x").body(()).unwrap();
    a.sign_hand_off("b", "/actor/c/x", &mut out).await.unwrap();
    let internal = out.headers()[INTERNAL].to_str().unwrap();
    let mac = internal.rsplit(' ').next().unwrap();
    let sender = out.headers()[SENDER].to_str().unwrap();
    assert_eq!(
        verify_sender(SECRET, "internal", mac, sender).as_deref(),
        Some("a")
    );
    let mut fwd = Request::get("/actor/c/x").body(()).unwrap();
    fwd.extensions_mut()
        .insert(crate::http::PeerAddress("192.0.2.1:1234".parse().unwrap()));
    a.sign_admission("b", "/actor/c/x", &mut fwd).await.unwrap();
    let admitted = fwd.headers()[ADMITTED].to_str().unwrap();
    let mac = admitted.rsplit(' ').next().unwrap();
    let sender = fwd.headers()[SENDER].to_str().unwrap();
    assert_eq!(
        verify_sender(SECRET, "admitted", mac, sender).as_deref(),
        Some("a")
    );
}

#[tokio::test]
async fn a_forward_whose_sender_is_of_another_app_is_refused() {
    let a = node("a").await;
    let b = node("b").await;
    let mut fwd = Request::get("/actor/c/x").body(()).unwrap();
    fwd.extensions_mut()
        .insert(crate::http::PeerAddress("192.0.2.1:1234".parse().unwrap()));
    a.sign_admission("b", "/actor/c/x", &mut fwd).await.unwrap();
    *b.peers.write().unwrap() = HashMap::from([("a".to_owned(), peer("t/default"))]);
    assert!(b.admission_received(&fwd).is_some());
    *b.peers.write().unwrap() = HashMap::from([("a".to_owned(), peer("t/other"))]);
    assert!(b.admission_received(&fwd).is_none());
}

#[tokio::test]
async fn a_connection_must_present_its_senders_own_pin() {
    let pins = Arc::new(PinSet::default());
    let (ka, kc) = (
        TlsIdentity::generate("a", "a.test").unwrap(),
        TlsIdentity::generate("c", "c.test").unwrap(),
    );
    let until = now_ms() + 60_000;
    pins.replace([("a", &[ka.pin()][..], until), ("c", &[kc.pin()][..], until)]);
    let on_a = crate::http::ClusterPeer::for_test(ka.pin(), Arc::clone(&pins));
    assert!(sender_presented(&call("b", PATH, Some("a")), Some(&on_a)));
    // c's hop over a's connection: refused.
    assert!(!sender_presented(&call("b", PATH, Some("c")), Some(&on_a)));
    // No sender (an older node), or plain HTTP: as before.
    assert!(sender_presented(&call("b", PATH, None), Some(&on_a)));
    assert!(sender_presented(&call("b", PATH, Some("c")), None));
}

/// One process's certificate, published by two node ids: what a request
/// of one of them answers, or the socket it relays, stops being trusted
/// when that node id lapses, while the other's on the same connection
/// stays up.
#[test]
fn a_lapsed_senders_answers_and_relays_stop_while_the_siblings_go_on() {
    let pins = Arc::new(PinSet::default());
    let k = TlsIdentity::generate("p", "p.test").unwrap();
    let now = now_ms();
    pins.replace([
        ("shop-1", &[k.pin()][..], now + 60_000),
        ("blog-1", &[k.pin()][..], now + 60_000),
    ]);
    let conn = crate::http::ClusterPeer::for_test(k.pin(), Arc::clone(&pins));
    let (shop, blog) = (conn.sent_by(Some("shop-1")), conn.sent_by(Some("blog-1")));
    assert!(shop.is_live() && blog.is_live());
    pins.replace([
        ("shop-1", &[k.pin()][..], now + 60_000),
        ("blog-1", &[k.pin()][..], now.saturating_sub(1)),
    ]);
    assert!(shop.is_live(), "the healthy app's relay stays up");
    assert!(!blog.is_live(), "the lapsed app's relay is closed");
    assert!(
        conn.is_live(),
        "the connection itself is still a live node's"
    );
    // The lapsed node id's new hops over that connection are refused.
    assert!(!sender_presented(
        &call("x", PATH, Some("blog-1")),
        Some(&conn)
    ));
}
