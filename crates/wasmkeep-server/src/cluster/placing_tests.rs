//! ADR 0041's wiring on one node: the sender's choice over its view, the
//! placed mark inside both signatures, and the receiver's arrival check.

#![allow(clippy::unwrap_used)] // test code

use wasmkeep_cluster::membership::Load;
use wasmkeep_cluster::placement::Arrival;

use super::*;

/// A node of `t/default` with its secret loaded and its lease valid.
async fn placing_node(id: &str) -> Arc<Cluster> {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let config = ClusterConfig::new(id, format!("http://{id}"), store);
    let c = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    c.load_secret().await;
    c.validity.set_until(Some(now_ms() + 600_000));
    c
}

fn load(actors: u64, room: u64) -> Load {
    Load {
        actors,
        actor_room: Some(room),
        socket_room: Some(100),
        ..Load::default()
    }
}

/// A live peer of `t/default` in the view, its heartbeat seen now.
fn peer(place: bool, seen: Option<Load>, written_at: u64) -> Peer {
    Peer {
        plain: Some("http://peer".into()),
        tls: None,
        app: "t/default".into(),
        drain: Some(DrainState::Serving),
        can_take: true,
        can_handoff: true,
        can_end: true,
        can_blobs: true,
        instance: Some("beef".into()),
        can_place: place,
        seen: seen.map(|load| Seen {
            load,
            beat: (Some("beef".into()), written_at),
            at: tokio::time::Instant::now(),
        }),
    }
}

fn actor(name: &str) -> ActorAddress {
    ActorAddress::new("t", "c", name).unwrap()
}

fn set_peer(c: &Cluster, id: &str, p: Peer) {
    c.peers.write().unwrap().insert(id.to_owned(), p);
}

/// Section 5: a sender at 500 with a fresh, empty peer that announces
/// `place` places, charged before any answer; the budget then keeps.
#[tokio::test(start_paused = true)]
async fn a_fuller_node_places_on_a_fresh_lighter_peer_within_its_budget() {
    let a = placing_node("a").await;
    set_peer(&a, "b", peer(true, Some(load(0, 10)), 1));
    let x = actor("x");
    // t = 500 * 4 * 10 / 5000 = 4: a share of 4 with N = 2.
    for _ in 0..4 {
        assert_eq!(
            a.choose_placement(&x, load(50, 50), true),
            Choice::Place {
                node: "b".into(),
                fill: 500
            }
        );
    }
    // Its credit counted: b's estimated fill is 400, within the band.
    assert_eq!(
        a.choose_placement(&x, load(50, 50), true),
        Choice::Keep("band")
    );
    // At 501 the band passes but the budget is spent: t = 4, k = 4.
    assert_eq!(
        a.choose_placement(&x, load(501, 499), true),
        Choice::Keep("budget")
    );
    // A new heartbeat of b gives a new budget.
    set_peer(&a, "b", peer(true, Some(load(0, 10)), 2));
    assert!(matches!(
        a.choose_placement(&x, load(50, 50), true),
        Choice::Place { .. }
    ));
    // Under the floor, or within the band: kept.
    assert_eq!(
        a.choose_placement(&x, load(19, 81), true),
        Choice::Keep("floor")
    );
}

/// Section 3 and 9: a stale heartbeat, a peer without `place`, a peer of
/// another app, a peer backed off and a hop that cannot be signed keep it.
#[tokio::test(start_paused = true)]
async fn stale_unannounced_backed_off_or_unsigned_keeps_it() {
    let a = placing_node("a").await;
    let x = actor("x");
    // No `place`: never a candidate.
    set_peer(&a, "b", peer(false, Some(load(0, 10)), 1));
    assert_eq!(
        a.choose_placement(&x, load(50, 50), true),
        Choice::Keep("no_candidate")
    );
    // Stale: three periods (9 s) after it was first seen.
    set_peer(&a, "b", peer(true, Some(load(0, 10)), 1));
    tokio::time::advance(Duration::from_secs(9)).await;
    assert_eq!(
        a.choose_placement(&x, load(50, 50), true),
        Choice::Keep("no_candidate")
    );
    // A peer of a version without rooms: no candidate.
    set_peer(
        &a,
        "b",
        peer(
            true,
            Some(Load {
                actors: 0,
                ..Load::default()
            }),
            2,
        ),
    );
    assert_eq!(
        a.choose_placement(&x, load(50, 50), true),
        Choice::Keep("no_candidate")
    );
    // Another app's node, with room: never.
    let mut other = peer(true, Some(load(0, 10)), 3);
    other.app = "t/other".into();
    set_peer(&a, "b", other);
    assert_eq!(
        a.choose_placement(&x, load(50, 50), true),
        Choice::Keep("no_candidate")
    );
    // Fresh and lighter, but nothing to prove the client with.
    set_peer(&a, "b", peer(true, Some(load(0, 10)), 4));
    assert_eq!(
        a.choose_placement(&x, load(50, 50), false),
        Choice::Keep("unsigned")
    );
    // Backed off for a heartbeat horizon after a refused hop.
    a.placement_backoff("b");
    assert_eq!(
        a.choose_placement(&x, load(50, 50), true),
        Choice::Keep("no_candidate")
    );
    tokio::time::advance(Duration::from_millis(a.heartbeat_horizon_ms)).await;
    set_peer(&a, "b", peer(true, Some(load(0, 10)), 5));
    assert!(matches!(
        a.choose_placement(&x, load(50, 50), true),
        Choice::Place { .. }
    ));
}

/// Section 1: placement off, a draining node, a lapsed lease and a move
/// naming the actor are not placeable at all (not counted as kept).
#[tokio::test(start_paused = true)]
async fn off_draining_or_without_a_lease_nothing_is_placeable() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut config = ClusterConfig::new("a", "http://a", store);
    config.placement = false;
    let off = Cluster::new(&config, &StateRoot::new("", "t", "default")).unwrap();
    off.load_secret().await;
    off.validity.set_until(Some(now_ms() + 600_000));
    set_peer(&off, "b", peer(true, Some(load(0, 10)), 1));
    let x = actor("x");
    assert!(!off.placement_on());
    assert_eq!(
        off.choose_placement(&x, load(50, 50), true),
        Choice::NotPlaceable
    );
    // Its heartbeat does not announce the word.
    assert!(
        !String::from_utf8(off.membership.lease_payload())
            .unwrap()
            .contains("place")
    );
    let a = placing_node("a").await;
    assert!(
        String::from_utf8(a.membership.lease_payload())
            .unwrap()
            .lines()
            .nth(8)
            .unwrap()
            .split(' ')
            .any(|w| w == "place")
    );
    set_peer(&a, "b", peer(true, Some(load(0, 10)), 1));
    a.validity.set_until(Some(now_ms().saturating_sub(1)));
    assert_eq!(
        a.choose_placement(&x, load(50, 50), true),
        Choice::NotPlaceable
    );
    a.validity.set_until(Some(now_ms() + 600_000));
    a.set_secrets_blocked(true);
    assert_eq!(
        a.choose_placement(&x, load(50, 50), true),
        Choice::NotPlaceable
    );
}

/// Section 5, no return: after accepting a placed event from `b`, `a`
/// sends `b` nothing for three periods; after sending `b` one, it accepts
/// none from `b` for three periods. At 9 s exactly both are allowed.
#[tokio::test(start_paused = true)]
async fn no_return_both_ways_for_three_periods() {
    let a = placing_node("a").await;
    set_peer(&a, "b", peer(true, Some(load(0, 10)), 1));
    let x = actor("x");
    // a sends b one: a refuses b's placed events for 9 s.
    assert!(matches!(
        a.choose_placement(&x, load(50, 50), true),
        Choice::Place { .. }
    ));
    let from_b = Mark {
        fill: Some(1000),
        sender: "b".into(),
    };
    assert!(matches!(
        a.arrive(&from_b, || load(0, 100)),
        Err(Arrival::NoReturn)
    ));
    tokio::time::advance(Duration::from_millis(8_999)).await;
    assert!(matches!(
        a.arrive(&from_b, || load(0, 100)),
        Err(Arrival::NoReturn)
    ));
    tokio::time::advance(Duration::from_millis(1)).await;
    let r = a.arrive(&from_b, || load(0, 100)).unwrap();
    // Accepted from b: b is no candidate for a for 9 s.
    r.accepted();
    drop(r);
    set_peer(&a, "b", peer(true, Some(load(0, 10)), 2));
    assert_eq!(
        a.choose_placement(&x, load(50, 50), true),
        Choice::Keep("no_candidate")
    );
    tokio::time::advance(Duration::from_secs(9)).await;
    set_peer(&a, "b", peer(true, Some(load(0, 10)), 3));
    assert!(matches!(
        a.choose_placement(&x, load(50, 50), true),
        Choice::Place { .. }
    ));
}

/// Section 5, on arrival: the receiver's own count, with the placed
/// events it accepted and has not registered yet, against the mark's
/// fill; a mark outside 200 to 1000 is refused.
#[tokio::test]
async fn the_arrival_check_counts_what_is_on_its_way_in() {
    let b = placing_node("b").await;
    let mark = |fill| Mark {
        fill,
        sender: "a".into(),
    };
    // t = 1000 * 4 * 10 / 5000 = 8: at 7 residents, one more fits.
    let first = b.arrive(&mark(Some(1000)), || load(7, 3)).unwrap();
    // A second hop at once: 7 + 1 in flight + 1 = 9 > 8.
    assert!(matches!(
        b.arrive(&mark(Some(1000)), || load(7, 3)),
        Err(Arrival::Capacity)
    ));
    drop(first);
    assert!(b.arrive(&mark(Some(1000)), || load(7, 3)).is_ok());
    // At t + 1 refused (the ADR's 500 and 400 of 100: 41 > 40).
    assert!(matches!(
        b.arrive(&mark(Some(500)), || load(40, 60)),
        Err(Arrival::Capacity)
    ));
    assert!(b.arrive(&mark(Some(500)), || load(39, 61)).is_ok());
    for bad in [None, Some(0), Some(199), Some(1001)] {
        assert!(
            matches!(
                b.arrive(&mark(bad), || load(0, 100)),
                Err(Arrival::InvalidMark)
            ),
            "{bad:?}"
        );
    }
}

/// Section 8: the mark counts only inside a forward's proof that signs it
/// and names a sender; stripped, added, altered or unsigned, it does not.
#[tokio::test]
async fn the_placed_mark_is_believed_only_inside_the_forward_proof() {
    let c = placing_node("owner").await;
    let path = "/actor/c/a";
    let request = |mark: Option<&str>| {
        let mut req = Request::get(path).body(()).unwrap();
        req.extensions_mut()
            .insert(crate::http::PeerAddress("192.0.2.1:1234".parse().unwrap()));
        req.headers_mut()
            .insert(APP, hyper::header::HeaderValue::from_static("t/default"));
        if let Some(m) = mark {
            req.headers_mut()
                .insert(PLACED, hyper::header::HeaderValue::from_str(m).unwrap());
        }
        req
    };
    let mut signed = request(Some("500"));
    c.sign_admission("owner", path, &mut signed).await.unwrap();
    assert_eq!(
        c.placed_mark(&signed, None),
        Some(Mark {
            fill: Some(500),
            sender: "owner".into(),
        })
    );
    // Altered: the proof no longer checks, so the mark means nothing.
    let mut altered = signed.clone_parts();
    altered
        .headers_mut()
        .insert(PLACED, hyper::header::HeaderValue::from_static("900"));
    assert_eq!(c.placed_mark(&altered, None), None);
    // Stripped: the proof no longer checks either (it signed the mark).
    let mut stripped = signed.clone_parts();
    stripped.headers_mut().remove(PLACED);
    assert!(c.admission_checked(&stripped).is_none());
    // Added to a forward signed without one.
    let mut added = request(None);
    c.sign_admission("owner", path, &mut added).await.unwrap();
    assert!(c.admission_checked(&added).is_some());
    added
        .headers_mut()
        .insert(PLACED, hyper::header::HeaderValue::from_static("500"));
    assert_eq!(c.placed_mark(&added, None), None);
    // Unsigned: a client's or a forged mark.
    assert_eq!(c.placed_mark(&request(Some("500")), None), None);
    // No sender named: not believed (no-return needs one).
    let mut anonymous = signed.clone_parts();
    anonymous.headers_mut().remove(SENDER);
    assert_eq!(c.placed_mark(&anonymous, None), None);
    // Whole numbers only, one value.
    let mut h = hyper::HeaderMap::new();
    for (v, want) in [
        ("500", Some(500)),
        ("0200", Some(200)),
        ("5e2", None),
        ("+500", None),
        ("", None),
        ("10000", None),
    ] {
        h.insert(PLACED, hyper::header::HeaderValue::from_str(v).unwrap());
        assert_eq!(mark_value(&h), want, "{v:?}");
    }
    h.append(PLACED, hyper::header::HeaderValue::from_static("500"));
    assert_eq!(mark_value(&h), None);
}

/// Section 8: a call's signature covers the mark too.
#[tokio::test]
async fn a_call_signature_covers_the_placed_mark() {
    let c = placing_node("b").await;
    let secret = c.secret.get().unwrap().clone();
    let mut headers = hyper::HeaderMap::new();
    headers.insert(APP, hyper::header::HeaderValue::from_static("t/default"));
    headers.insert(PLACED, hyper::header::HeaderValue::from_static("700"));
    let method = hyper::Method::POST;
    let body = body_sha256(b"");
    let ts = now_ms();
    let mac = signature(
        &secret,
        ts,
        "ab",
        "b",
        &method,
        "/actor/c/x/",
        &body,
        &headers,
    );
    let given = format!("v4 {ts} ab {body} {mac}");
    assert!(verify(&secret, &given, ts, "b", &method, "/actor/c/x/", &headers).is_some());
    headers.insert(PLACED, hyper::header::HeaderValue::from_static("1000"));
    assert!(verify(&secret, &given, ts, "b", &method, "/actor/c/x/", &headers).is_none());
    headers.remove(PLACED);
    assert!(verify(&secret, &given, ts, "b", &method, "/actor/c/x/", &headers).is_none());
}

/// Section 6: the refusal is recognised only as `503` with the mark, and
/// a response carrying a client's request mark is not turned into one.
#[test]
fn a_refusal_is_a_503_with_the_mark() {
    assert!(is_placed_refusal(&placed_refusal("no\n")));
    let mut ok = text_response(StatusCode::OK, "ok\n");
    ok.headers_mut().insert(
        PLACED,
        hyper::header::HeaderValue::from_static(PLACED_REFUSED),
    );
    assert!(!is_placed_refusal(&ok));
    assert!(!is_placed_refusal(&unavailable("x\n")));
    let plain = hyper::HeaderMap::new();
    assert!(!is_placed_refusal(&refuse_if_placed(
        &plain,
        unavailable("x\n")
    )));
}

trait CloneParts {
    fn clone_parts(&self) -> Self;
}

impl CloneParts for Request<()> {
    fn clone_parts(&self) -> Self {
        let mut r = Request::new(());
        *r.method_mut() = self.method().clone();
        *r.uri_mut() = self.uri().clone();
        *r.headers_mut() = self.headers().clone();
        *r.extensions_mut() = self.extensions().clone();
        r
    }
}

/// Review item 4: asking whether a wake-up would be placed charges no
/// credit and stamps no send, so reading a slow body first costs nothing.
#[tokio::test(start_paused = true)]
async fn asking_first_charges_nothing() {
    let a = placing_node("a").await;
    set_peer(&a, "b", peer(true, Some(load(0, 10)), 1));
    let x = actor("x");
    for _ in 0..10 {
        assert!(a.would_place(&x, load(50, 50), true));
    }
    // Nothing sent to b: a placed event from b is not refused for no return.
    let from_b = Mark {
        fill: Some(1000),
        sender: "b".into(),
    };
    assert!(a.arrive(&from_b, || load(0, 100)).is_ok());
    // The whole budget of 4 is still there.
    for _ in 0..4 {
        assert!(matches!(
            a.choose_placement(&x, load(50, 50), true),
            Choice::Place { .. }
        ));
    }
}
