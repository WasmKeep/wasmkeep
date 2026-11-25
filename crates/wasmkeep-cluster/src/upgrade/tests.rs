use super::*;
use crate::app_format::floor::Unknown;

const NOW: u64 = 1_700_000_000_000;
const HORIZON: u64 = 10_000;

fn v(major: u32, minor: u32) -> Minor {
    Minor { major, minor }
}

/// A heartbeat of `id` for `acme/shop` at `version`, in `state` (`live`,
/// `released`, `fenced`), announcing `words`.
fn node_body(id: &str, version: &str, state: &str, words: &str) -> Vec<u8> {
    node_body_of("acme/shop", id, version, state, words)
}

fn node_body_of(app: &str, id: &str, version: &str, state: &str, words: &str) -> Vec<u8> {
    format!(
        "wasmkeep-node 3\n{state} beef\n{NOW}\n{id}\nhttp://{id}\n{version}\n{app}\n\n\nserving\n\n{words}\n"
    )
    .into_bytes()
}

fn node(id: &str, version: &str, state: &str, words: &str) -> NodeInfo {
    NodeInfo::decode(&node_body(id, version, state, words)).unwrap()
}

fn live(id: &str, version: &str) -> NodeInfo {
    node(id, version, "live", "take front")
}

/// `minor`'s first patch, as a node writes it.
fn patch(minor: Minor) -> String {
    format!("{minor}.0")
}

#[test]
fn this_build_has_a_minor_version() {
    assert_eq!(
        Minor::of_node_version(crate::VERSION),
        Some(this_minor().unwrap())
    );
    assert_eq!(
        Minor::of_version(env!("CARGO_PKG_VERSION")),
        Some(this_minor().unwrap())
    );
}

/// ADR 0043, section 5: a node of N beside N-2, N-1, N, N+1 and N+2,
/// another major version, and a version line that does not parse. Run for
/// this build's version and for others, so the next version bump keeps it.
#[test]
fn a_version_runs_beside_the_one_before_and_the_one_after() {
    for this in [this_minor().unwrap(), v(0, 9), v(0, 10), v(1, 3)] {
        let at = |minor: u32| patch(v(this.major, minor));
        for (other, ok) in [
            (at(this.minor - 2), false),
            (at(this.minor - 1), true),
            (at(this.minor), true),
            (format!("{}+0a1b2c3d4e5f", at(this.minor)), true),
            (at(this.minor + 1), true),
            (at(this.minor + 2), false),
            (patch(v(this.major + 1, this.minor)), false),
            ("1".to_owned(), false),
            ("0.9".to_owned(), false),
            (String::new(), false),
        ] {
            let found = skew(this, [&live("old", &other)]);
            assert_eq!(found.is_empty(), ok, "{this} beside {other:?}: {found:?}");
            if let Some(why) = found.first() {
                assert!(why.contains("`old`"), "names the node: {why}");
                assert!(why.contains(&format!("`{other}`")), "and its line: {why}");
            }
        }
        // Each offender is named.
        let (low, high) = (at(this.minor - 2), at(this.minor + 2));
        let nodes = [
            live("a", &low),
            live("b", &at(this.minor)),
            live("c", &high),
        ];
        let found = skew(this, &nodes);
        assert_eq!(found.len(), 2, "{found:?}");
        assert!(found[0].contains("`a`") && found[1].contains("`c`"));
    }
    let why = &skew(v(0, 9), [&live("n1", "0.7.3")])[0];
    assert_eq!(
        why,
        "node `n1` runs `0.7.3`; this version (0.9) runs only beside 0.8 to 0.10. Upgrade one \
         minor version at a time"
    );
}

/// Section 3: a node of N refuses an app with registered nodes whose
/// floor is below N-1. With no record, that stops 0.10 and passes 0.9.
#[test]
fn a_floor_below_the_previous_version_stops_a_start() {
    assert!(floor_from_below(None, v(0, 8), true).is_ok());
    assert!(floor_from_below(None, v(0, 9), true).is_ok());
    let why = floor_from_below(None, v(0, 10), true).unwrap_err();
    assert!(
        why.contains("0.8") && why.contains("0.9") && why.contains("0.10"),
        "{why}"
    );
    assert!(why.contains("finalize"), "{why}");
    assert!(floor_from_below(Some(v(0, 9)), v(0, 10), true).is_ok());
    assert!(floor_from_below(Some(v(0, 10)), v(0, 10), true).is_ok());
    assert!(floor_from_below(Some(v(0, 9)), v(0, 11), true).is_err());
    // Above the binary: the floor's own check answers (`floor::allows`).
    assert!(floor_from_below(Some(v(0, 12)), v(0, 10), true).is_ok());
    // No node ever registered: nothing ran the app.
    assert!(floor_from_below(None, v(0, 10), false).is_ok());
    // Another major version below.
    assert!(floor_from_below(Some(v(0, 12)), v(1, 0), true).is_err());
}

/// Section 2: a gated format needs both halves, the floor and every live
/// node's words. Every pair, a live node without the word above the
/// floor included.
#[test]
fn a_gate_needs_the_floor_and_every_live_word() {
    let new = Format {
        name: "wasmkeep-next 1",
        since: v(0, 9),
        rule: Rule::Gate(&["place", "front"]),
        unknown: "",
    };
    let with = node("a", "0.9.0", "live", "take front place");
    let without_word = node("b", "0.9.0", "live", "take front");
    let all = [with.clone()];
    let one_lacks = [with.clone(), without_word.clone()];
    let none: [NodeInfo; 0] = [];
    let floors: [Result<Option<Minor>, Unknown>; 4] = [
        Ok(None),
        Ok(Some(v(0, 8))),
        Ok(Some(v(0, 9))),
        Ok(Some(v(0, 10))),
    ];
    for floor in floors {
        let low = floor::in_force(floor.clone().unwrap()) < v(0, 9);
        for nodes in [&all[..], &one_lacks[..], &none[..]] {
            let got = gate(&new, floor.clone(), nodes);
            let lacking = nodes.len() == 2;
            match (low, lacking) {
                // The floor is named first: finalize comes before words.
                (true, _) => assert_eq!(
                    got,
                    Err(Blocked::Floor {
                        format: "wasmkeep-next 1",
                        needs: v(0, 9),
                        floor: v(0, 8),
                    })
                ),
                (false, true) => assert_eq!(
                    got,
                    Err(Blocked::Nodes {
                        format: "wasmkeep-next 1",
                        word: "place",
                        nodes: vec!["b".to_owned()],
                    })
                ),
                (false, false) => assert_eq!(got, Ok(())),
            }
        }
    }
    // A format gated on the floor alone has no word to keep it from a
    // live older node (one that started while the floor was written): its
    // version does. One whose version does not parse blocks too.
    let floor_only = Format {
        name: "wasmkeep-next 2",
        since: v(0, 9),
        rule: Rule::Gate(&[]),
        unknown: "",
    };
    let late = node("late", "0.8.1", "live", "take front place");
    let odd = node("odd", "nine", "live", "take front place");
    assert_eq!(gate(&floor_only, Ok(Some(v(0, 9))), &all[..]), Ok(()));
    let blocked = gate(&floor_only, Ok(Some(v(0, 9))), [&with, &late, &odd]).unwrap_err();
    assert_eq!(
        blocked,
        Blocked::OlderNodes {
            format: "wasmkeep-next 2",
            needs: v(0, 9),
            nodes: vec!["late (0.8.1)".to_owned(), "odd (nine)".to_owned()],
        }
    );
    assert!(blocked.to_string().contains("late (0.8.1)"), "{blocked}");
    // The same for a format with words: the older node is named for its
    // version before its words.
    assert!(matches!(
        gate(&new, Ok(Some(v(0, 9))), [&with, &late]),
        Err(Blocked::OlderNodes { .. })
    ));
    // An unreadable floor: nothing gated is written.
    assert_eq!(
        gate(&new, Err(Unknown { len: 7 }), &all[..]),
        Err(Blocked::FloorUnreadable { len: 7 })
    );
    let why = gate(&new, Ok(None), &all[..]).unwrap_err().to_string();
    assert!(why.contains("finalize") && why.contains("0.9"), "{why}");
    let why = gate(&new, Ok(Some(v(0, 9))), &one_lacks[..])
        .unwrap_err()
        .to_string();
    assert_eq!(
        why,
        "wasmkeep-next 1 needs `place` on every live node; without it: b"
    );

    // A format 0.8.1 writes keeps its live words alone: every floor is at
    // or above it, a missing one included.
    let seven = manifest_format(7).unwrap();
    let old = node("old", "0.8.1", "live", "take front ends flush rate_limits");
    assert_eq!(
        gate(seven, Ok(None), [&old]),
        Err(Blocked::Nodes {
            format: "wasmkeep-manifest 7",
            word: "assets",
            nodes: vec!["old".to_owned()],
        })
    );
    let able = node(
        "n",
        "0.8.1",
        "live",
        "take front ends flush rate_limits assets",
    );
    assert_eq!(gate(seven, Ok(None), [&able]), Ok(()));
    // Manifest 8 is the first format behind the floor (the per-class
    // placement keys): refused under a floor below 0.9, beside a live
    // node of 0.8.1, and beside a node of 0.9 that does not announce
    // `apart`; written once all three hold.
    let eight = manifest_format(8).unwrap();
    assert_eq!(eight.since, v(0, 9));
    let every = "take front ends flush rate_limits assets";
    let new = node("new", "0.9.0", "live", &format!("{every} apart"));
    let wordless = node("wordless", "0.9.0", "live", every);
    for floor in [None, Some(v(0, 8))] {
        assert_eq!(
            gate(eight, Ok(floor), [&new]),
            Err(Blocked::Floor {
                format: "wasmkeep-manifest 8",
                needs: v(0, 9),
                floor: v(0, 8),
            })
        );
    }
    assert_eq!(
        gate(eight, Ok(Some(v(0, 9))), [&new, &able]),
        Err(Blocked::OlderNodes {
            format: "wasmkeep-manifest 8",
            needs: v(0, 9),
            nodes: vec!["n (0.8.1)".to_owned()],
        })
    );
    assert_eq!(
        gate(eight, Ok(Some(v(0, 9))), [&new, &wordless]),
        Err(Blocked::Nodes {
            format: "wasmkeep-manifest 8",
            word: "apart",
            nodes: vec!["wordless".to_owned()],
        })
    );
    assert_eq!(gate(eight, Ok(Some(v(0, 9))), [&new]), Ok(()));
    assert_eq!(gate(eight, Ok(Some(v(0, 10))), [&new]), Ok(()));
    // The words of a manifest number are named highest first.
    let bare = node("bare", "0.8.1", "live", "take");
    let Err(Blocked::Nodes { word, .. }) = gate(manifest_format(5).unwrap(), Ok(None), [&bare])
    else {
        unreachable!()
    };
    assert_eq!(word, "flush");
    // What is frozen or extended has no gate.
    for name in [
        "wasmkeep-node 3",
        "wasmkeep-blob-usage 1",
        "wasmkeep-call v4",
    ] {
        assert_eq!(
            gate(format(name).unwrap(), Err(Unknown { len: 1 }), [&bare]),
            Ok(())
        );
    }
    assert_eq!(
        without(&["actor-restore", "blobs"], [&able, &bare]),
        ["n", "bare"]
    );
    assert_eq!(without(&["take"], [&able, &bare]), Vec::<String>::new());
}

/// Sections 3 and 5, the CLI: a writer refuses under a floor above its
/// version, an unreadable floor, and a live node outside its range.
#[test]
fn a_writer_older_than_the_cluster_is_refused() {
    let nodes = [live("a", "0.9.0")];
    assert_eq!(writer(v(0, 9), Ok(None), &nodes), Ok(()));
    assert_eq!(writer(v(0, 9), Ok(Some(v(0, 9))), &nodes), Ok(()));
    assert_eq!(writer(v(0, 8), Ok(None), &nodes), Ok(()));
    let why = writer(v(0, 8), Ok(Some(v(0, 9))), &nodes).unwrap_err();
    assert!(why.contains("older than the cluster"), "{why}");
    assert!(why.contains("0.9") && why.contains("0.8"), "{why}");
    let why = writer(v(0, 9), Err(Unknown { len: 3 }), &nodes).unwrap_err();
    assert!(why.contains("unreadable (3 bytes)"), "{why}");
    let why = writer(v(0, 11), Ok(Some(v(0, 9))), &nodes).unwrap_err();
    assert!(why.contains("node `a` runs `0.9.0`"), "{why}");
    // Each refusal says which `wasmkeep` may write instead.
    assert!(
        why.ends_with(
            "Use a `wasmkeep` within one minor version of every live node, and not older than \
             the app's upgrade floor"
        ),
        "{why}"
    );
    let why = writer(v(0, 8), Ok(Some(v(0, 9))), &nodes).unwrap_err();
    assert!(
        why.ends_with(
            "Use a `wasmkeep` of 0.9 or later, within one minor version of every live node"
        ),
        "{why}"
    );
}

fn status(floor: Result<Option<Minor>, Unknown>, nodes: &[NodeInfo]) -> AppUpgrade {
    assess("acme/shop", floor, nodes, NOW, HORIZON)
}

/// Section 4: one state per app, from the floor and the live nodes.
#[test]
fn an_upgrade_has_one_state() {
    let a8 = live("a", "0.8.1");
    let b8 = live("b", "0.8.1");
    let a9 = live("a", "0.9.0");
    let b9 = live("b", "0.9.2+0a1b2c3d4e5f");
    let stopped8 = node("gone", "0.8.1", "released", "take");
    let other_app = {
        let mut n = live("x", "0.3.0");
        n.app = "acme/blog".to_owned();
        n
    };

    let s = status(Ok(None), &[a8.clone(), b8.clone(), other_app.clone()]);
    assert_eq!(s.state, State::OneVersion);
    assert_eq!(s.nodes.len(), 2, "another app's node is not this app's");
    assert_eq!(s.floor_text(), "none (0.8)");
    assert!(s.blocks.is_empty());
    assert_eq!(s.state.exit_code(), 0);

    let s = status(Ok(None), &[a9.clone(), b8.clone(), stopped8.clone()]);
    assert_eq!(s.state, State::Mixed);
    assert_eq!(s.blocks, ["b"], "the older live node blocks a finalize");
    assert_eq!(s.detail, "b (0.8.1) on 0.8, a (0.9.0) on 0.9");
    assert_eq!(s.state.exit_code(), 3);

    let s = status(Ok(None), &[a9.clone(), b9.clone(), stopped8.clone()]);
    assert_eq!(s.state, State::FinalizePending);
    assert!(s.blocks.is_empty(), "a stopped older node blocks nothing");
    assert!(s.detail.contains("finalize"), "{}", s.detail);
    assert_eq!(s.state.exit_code(), 3);
    assert_eq!(
        s.line(),
        format!(
            "acme/shop: floor none (0.8), finalize pending: {}",
            s.detail
        )
    );

    let s = status(Ok(Some(v(0, 9))), &[a9.clone(), b9.clone()]);
    assert_eq!(s.state, State::OneVersion);
    assert_eq!(s.floor_text(), "0.9");

    let s = status(Ok(Some(v(0, 9))), std::slice::from_ref(&stopped8));
    assert_eq!(s.state, State::NoLiveNode);
    assert_eq!(s.state.exit_code(), 3);

    // Something the skew check refuses, named.
    let s = status(Ok(None), &[live("a", "0.10.0"), b8.clone()]);
    assert_eq!(s.state, State::Refused);
    assert!(s.detail.contains("b (0.8.1)") && s.detail.contains("a (0.10.0)"));
    assert_eq!(s.state.exit_code(), 1);
    let s = status(Ok(None), &[a9.clone(), live("odd", "nine")]);
    assert_eq!(s.state, State::Refused);
    assert!(s.detail.contains("odd (nine)"), "{}", s.detail);
    assert_eq!(s.blocks, ["odd"]);
    let s = status(Err(Unknown { len: 5 }), std::slice::from_ref(&a9));
    assert_eq!(s.state, State::Refused);
    assert_eq!(s.floor_text(), "unreadable (5 bytes)");
    assert_eq!(s.floor_in_force(), None);
}

/// ADR 0043, "Not settled" 7, as decided: a live node below the floor is
/// a state of its own. It is what a node that got no answer on its second
/// floor read and keeps checking looks like once the floor is above it,
/// and what a node that passed that read before the floor was written
/// looks like; the bucket does not tell them apart, so the state names
/// both and points at the node's readiness.
#[test]
fn a_live_node_below_the_floor_is_named_apart() {
    let s = status(
        Ok(Some(v(0, 9))),
        &[live("a", "0.9.0"), live("late", "0.8.1")],
    );
    assert_eq!(s.state, State::BelowFloor);
    assert_eq!(s.state.as_str(), "node below the floor");
    assert_eq!(s.state.exit_code(), 1);
    assert_eq!(s.blocks, ["late"]);
    assert!(
        s.detail
            .starts_with("late (0.8.1) live below the floor 0.9")
    );
    assert!(
        s.detail.contains("got no answer on its floor read")
            && s.detail.contains("keeps checking without serving")
            && s.detail.contains("readiness"),
        "{}",
        s.detail
    );
    // Before the floor is written the same node is an older live node:
    // the app is mixed, and it blocks the finalize.
    let s = status(Ok(None), &[live("a", "0.9.0"), live("late", "0.8.1")]);
    assert_eq!(s.state, State::Mixed);
    assert_eq!(s.blocks, ["late"]);
    // Alone under the floor too.
    let s = status(Ok(Some(v(0, 9))), &[live("late", "0.8.1")]);
    assert_eq!(s.state, State::BelowFloor);
}

/// Section 4, steps 1 to 3: each precondition of a finalize.
#[test]
fn finalize_is_refused_for_each_precondition() {
    let a9 = live("a", "0.9.0");
    let b9 = live("b", "0.9.1");
    let check = |target, floor, nodes: &[NodeInfo]| finalize_check(target, &status(floor, nodes));
    let both = [a9.clone(), b9.clone()];
    assert_eq!(
        check(v(0, 9), Ok(None), &both),
        Ok(Finalize::Raise(None, v(0, 9)))
    );
    assert_eq!(
        check(v(0, 9), Ok(Some(v(0, 8))), &both),
        Ok(Finalize::Raise(Some(v(0, 8)), v(0, 9)))
    );
    // Already there: idempotent, whatever is live.
    assert_eq!(
        check(v(0, 9), Ok(Some(v(0, 9))), &[]),
        Ok(Finalize::Already(v(0, 9)))
    );
    // An older live node, named with its version.
    let mixed = [a9.clone(), live("old", "0.8.1"), live("older", "0.8.0")];
    let refused = check(v(0, 9), Ok(None), &mixed).unwrap_err();
    assert_eq!(
        refused,
        FinalizeRefusal::OlderLive {
            target: v(0, 9),
            nodes: "old (0.8.1), older (0.8.0)".to_owned(),
        }
    );
    assert_eq!(
        refused.to_string(),
        "live nodes are older than 0.9: old (0.8.1), older (0.8.0). Upgrade or stop each of \
         them, then finalize"
    );
    // One whose version does not parse counts as older.
    assert!(matches!(
        check(v(0, 9), Ok(None), &[a9.clone(), live("odd", "nine")]),
        Err(FinalizeRefusal::OlderLive { nodes, .. }) if nodes == "odd (nine)"
    ));
    // A newer live node: that version's CLI finalizes.
    let refused = check(v(0, 9), Ok(None), &[a9.clone(), live("new", "0.10.0")]).unwrap_err();
    assert!(
        matches!(&refused, FinalizeRefusal::NewerLive { nodes, .. } if nodes == "new (0.10.0)")
    );
    assert!(refused.to_string().contains("that version's `wasmkeep`"));
    // No live node; a stopped older one changes nothing.
    assert_eq!(
        check(v(0, 9), Ok(None), &[node("gone", "0.8.1", "released", "")]),
        Err(FinalizeRefusal::NoLiveNode)
    );
    // An older lease still `live` but stale is not a stopped node: it
    // may be cut off or slow, and still act. Refused, naming the fence;
    // fenced or released, it is stopped for sure.
    let stale = |state: &str| {
        let mut n = node("cut", "0.8.1", state, "take");
        n.written_at_ms = NOW - 3_600_000;
        n
    };
    let refused = check(v(0, 9), Ok(None), &[a9.clone(), stale("live")]).unwrap_err();
    assert_eq!(
        refused,
        FinalizeRefusal::OlderNotStopped {
            target: v(0, 9),
            nodes: "cut (0.8.1)".to_owned(),
            ids: "cut".to_owned(),
        }
    );
    assert!(
        refused
            .to_string()
            .contains("run `wasmkeep cluster fence <id>` for each (cut)"),
        "{refused}"
    );
    let s = status(Ok(None), &[a9.clone(), stale("live")]);
    assert_eq!(s.state, State::FinalizePending);
    assert_eq!(s.blocks, ["cut"], "it blocks the finalize all the same");
    for stopped in ["fenced", "released"] {
        assert_eq!(
            check(v(0, 9), Ok(None), &[a9.clone(), stale(stopped)]),
            Ok(Finalize::Raise(None, v(0, 9))),
            "{stopped}"
        );
    }
    // A fence proves a stop only once it is a lease's ttl plus grace
    // (15 s) past the node's last write: until then the fenced process
    // may still act. A release is a clean stop at once.
    let fenced_at = |ago: u64| {
        let mut n = node("cut", "0.8.1", "fenced", "take");
        n.written_at_ms = NOW - ago;
        n
    };
    assert!(matches!(
        check(v(0, 9), Ok(None), &[a9.clone(), fenced_at(14_999)]),
        Err(FinalizeRefusal::OlderNotStopped { .. })
    ));
    assert_eq!(
        check(v(0, 9), Ok(None), &[a9.clone(), fenced_at(15_000)]),
        Ok(Finalize::Raise(None, v(0, 9)))
    );
    assert_eq!(
        check(
            v(0, 9),
            Ok(None),
            &[a9.clone(), node("gone", "0.8.1", "released", "take")]
        ),
        Ok(Finalize::Raise(None, v(0, 9)))
    );
    // A stale heartbeat from before node leases cannot be fenced: it
    // blocks too, and the message says what to do.
    let mut old_format = NodeInfo::decode(
        format!(
            "wasmkeep-node 2\nold\nhttp://old\n0.3.9\n{}\nacme/shop\n",
            NOW - 3_600_000
        )
        .as_bytes(),
    )
    .unwrap();
    old_format.app = "acme/shop".to_owned();
    let refused = check(v(0, 9), Ok(None), &[a9.clone(), old_format]).unwrap_err();
    assert!(refused.to_string().contains("cluster forget"), "{refused}");
    // The floor above this CLI, and an unreadable one.
    assert_eq!(
        check(v(0, 9), Ok(Some(v(0, 10))), &both),
        Err(FinalizeRefusal::FloorAbove {
            floor: v(0, 10),
            target: v(0, 9)
        })
    );
    assert_eq!(
        check(v(0, 9), Err(Unknown { len: 4 }), &both),
        Err(FinalizeRefusal::Unreadable { len: 4 })
    );
}

/// Section 5: an object under `nodes/` that does not decode cannot be
/// judged by its fields; rewritten between two listings, a node this
/// version cannot read is running.
#[test]
fn an_unread_node_object_rewritten_between_listings_is_running() {
    let unread = |body: &str| Listed {
        nodes: vec![live("a", "0.9.0")],
        unread: vec![("nodes/x".to_owned(), body.as_bytes().to_vec())],
    };
    let first = unread("wasmkeep-node 4\nlive\n1\n");
    assert!(
        unread("wasmkeep-node 4\nlive\n1\n")
            .rewritten(&first)
            .is_empty()
    );
    assert_eq!(
        unread("wasmkeep-node 4\nlive\n2\n").rewritten(&first),
        ["nodes/x"]
    );
    // One that appeared meanwhile was written meanwhile.
    assert_eq!(first.rewritten(&Listed::default()), ["nodes/x"]);
    // It counts as a registered node of any app.
    assert!(first.registered("acme/blog", "self"));
    assert!(first.registered("acme/shop", "self"));
    assert!(!Listed::default().registered("acme/shop", "self"));
    let own = Listed {
        nodes: vec![live("a", "0.9.0")],
        unread: Vec::new(),
    };
    assert!(!own.registered("acme/shop", "a"), "its own lease left out");
    // At a start, its own earlier lease is left out only at its own
    // version or a later one: restarted in place on a newer binary, the
    // node is not the first node of a new app.
    assert!(!own.registered_at_start("acme/shop", "a", v(0, 9)));
    assert!(!own.registered_at_start("acme/shop", "a", v(0, 8)));
    assert!(own.registered_at_start("acme/shop", "a", v(0, 10)));
    assert!(own.registered_at_start("acme/shop", "b", v(0, 9)));
    let odd = Listed {
        nodes: vec![live("a", "nine")],
        unread: Vec::new(),
    };
    assert!(odd.registered_at_start("acme/shop", "a", v(0, 9)));
    assert!(!own.registered("acme/blog", "b"), "another app's node");
}

#[cfg(feature = "object-store")]
mod bucket {
    use super::*;
    use crate::control::Keys as _;
    use crate::object_store::ObjectStoreBucket;

    fn bucket() -> ObjectStoreBucket {
        ObjectStoreBucket::new(
            std::sync::Arc::new(object_store::memory::InMemory::new()),
            "wk",
        )
    }

    async fn floor_of(bucket: &ObjectStoreBucket) -> Option<Minor> {
        let body = bucket.get_body("formats/acme/shop/floor").await.unwrap();
        floor::decode(body.as_deref()).unwrap()
    }

    async fn put(bucket: &ObjectStoreBucket, id: &str, version: &str, state: &str) {
        bucket
            .put_body(
                &format!("nodes/{id}"),
                node_body(id, version, state, "take front"),
            )
            .await
            .unwrap();
    }

    /// Section 4: refused while a live node is older (nothing written),
    /// accepted once it stopped, idempotent after, and the second listing
    /// names a stopped older node without failing.
    #[tokio::test]
    async fn finalize_writes_the_floor_once_every_live_node_runs_it() {
        let b = bucket();
        put(&b, "a", "0.9.0", "live").await;
        put(&b, "old", "0.8.1", "live").await;
        let run = async || finalize_write(&b, "acme", "shop", v(0, 9), NOW, HORIZON).await;
        assert!(matches!(
            run().await,
            Err(FinalizeRefusal::OlderLive { nodes, .. }) if nodes == "old (0.8.1)"
        ));
        assert_eq!(floor_of(&b).await, None, "a refusal writes nothing");
        put(&b, "old", "0.8.1", "released").await;
        assert_eq!(run().await, Ok(Finalize::Raise(None, v(0, 9))));
        assert_eq!(floor_of(&b).await, Some(v(0, 9)));
        let seen = finalize_confirm(&b, "acme", "shop", v(0, 9), NOW, HORIZON)
            .await
            .unwrap();
        assert!(seen.late.is_empty());
        assert_eq!(seen.stopped_older.len(), 1);
        assert_eq!(seen.stopped_older[0].id, "old");
        // Again: nothing to do, and nothing live is needed for that.
        put(&b, "a", "0.9.0", "released").await;
        assert_eq!(run().await, Ok(Finalize::Already(v(0, 9))));
        // An older CLI finds the floor above it.
        assert_eq!(
            finalize_write(&b, "acme", "shop", v(0, 8), NOW, HORIZON).await,
            Err(FinalizeRefusal::FloorAbove {
                floor: v(0, 9),
                target: v(0, 8)
            })
        );
        // Another app on the prefix has its own floor.
        assert_eq!(
            read_status(&b, "acme", "blog", NOW, HORIZON)
                .await
                .unwrap()
                .floor,
            Ok(None)
        );
        assert_eq!(apps_with_nodes(&b).await.unwrap(), ["acme/shop"]);
    }

    /// Section 8, phase 5, second outcome: an old node whose first lease
    /// write lands after the first listing and whose floor reads came
    /// before the write is live at the second listing. It is reported,
    /// and the floor stays.
    #[tokio::test]
    async fn the_second_listing_finds_a_node_that_started_meanwhile() {
        let b = bucket();
        put(&b, "a", "0.9.0", "live").await;
        assert_eq!(
            finalize_write(&b, "acme", "shop", v(0, 9), NOW, HORIZON).await,
            Ok(Finalize::Raise(None, v(0, 9)))
        );
        // The old node's lease, written directly: it never saw the floor.
        put(&b, "late", "0.8.1", "live").await;
        let seen = finalize_confirm(&b, "acme", "shop", v(0, 9), NOW, HORIZON)
            .await
            .unwrap();
        assert_eq!(seen.late.len(), 1);
        assert_eq!(
            (seen.late[0].id.as_str(), seen.late[0].version.as_str()),
            ("late", "0.8.1")
        );
        assert_eq!(floor_of(&b).await, Some(v(0, 9)), "the floor stays");
        let s = read_status(&b, "acme", "shop", NOW, HORIZON).await.unwrap();
        assert_eq!(s.state, State::BelowFloor);
        // Stopped: nothing is late any more.
        put(&b, "late", "0.8.1", "released").await;
        let seen = finalize_confirm(&b, "acme", "shop", v(0, 9), NOW, HORIZON)
            .await
            .unwrap();
        assert!(seen.late.is_empty());
    }

    async fn born(
        b: &ObjectStoreBucket,
        by: FirstWriter<'_>,
        this: Minor,
    ) -> Result<Born, crate::lease::BucketError> {
        born_finalized(b, "acme", "shop", by, this, NOW, HORIZON).await
    }

    /// Section 3: an app that never ran is born finalized at its first
    /// node's version; a second writer finds the floor.
    #[tokio::test]
    async fn a_fresh_app_is_born_finalized() {
        let b = bucket();
        // Another app's node, deploy and state do not make this one old
        // (a live older node of it would hold the floor down: below).
        b.put_body(
            "nodes/x",
            node_body_of("acme/blog", "x", "0.8.1", "released", "take"),
        )
        .await
        .unwrap();
        b.put_body("apps/acme/shop2/current", b"v 1 3\n".to_vec())
            .await
            .unwrap();
        // Secrets set before the first deploy, the uploads of a deploy
        // that never finished, and for a node the deploy itself: with
        // `serve --app` a node always starts after one.
        for key in [
            "apps/acme/shop/secrets/current",
            "apps/acme/shop/0123456789abcdef/manifest",
            "apps/acme/shop/current",
        ] {
            b.put_body(key, b"x".to_vec()).await.unwrap();
        }
        b.put_body("state/acme/blog/owners/c/n", b"x".to_vec())
            .await
            .unwrap();
        assert_eq!(
            born(&b, FirstWriter::Node("a"), v(0, 9)).await,
            Ok(Born::Created(v(0, 9)))
        );
        assert_eq!(floor_of(&b).await, Some(v(0, 9)));
        assert_eq!(
            born(&b, FirstWriter::Node("b"), v(0, 10)).await,
            Ok(Born::HasFloor)
        );
        assert_eq!(floor_of(&b).await, Some(v(0, 9)), "never raised here");
    }

    /// ADR 0043, "Not settled" 8, as decided: "no node registered" is not
    /// proof of a fresh app. One whose node leases were all removed with
    /// `cluster forget` still has its state, and gets no floor: its way
    /// back ends only with a `finalize`. Nor is a node restarted in place
    /// on a newer binary the first node of a new app.
    #[tokio::test]
    async fn a_forgotten_app_is_not_born_finalized() {
        for (key, why) in [
            ("apps/acme/shop/deleted", "it was deleted"),
            ("state/acme/shop/owners/room/lobby", "it has state"),
            (
                "state/acme/shop/actors/room/lobby/wal/0000000000000001",
                "it has state",
            ),
        ] {
            for by in [FirstWriter::Node("a"), FirstWriter::Deploy] {
                let b = bucket();
                b.put_body(key, b"x".to_vec()).await.unwrap();
                assert_eq!(
                    born(&b, by, v(0, 9)).await,
                    Ok(Born::NotFresh(why)),
                    "{key} {by:?}"
                );
                assert_eq!(floor_of(&b).await, None, "{key}");
            }
        }
        // A deploy: the app ran, for a deploy; a node ignores it.
        let b = bucket();
        b.put_body("apps/acme/shop/current", b"x".to_vec())
            .await
            .unwrap();
        assert_eq!(
            born(&b, FirstWriter::Deploy, v(0, 9)).await,
            Ok(Born::NotFresh("it has a deploy"))
        );
        // A registered node, live or not.
        let b = bucket();
        put(&b, "a", "0.8.1", "released").await;
        for by in [FirstWriter::Node("b"), FirstWriter::Deploy] {
            assert_eq!(
                born(&b, by, v(0, 9)).await,
                Ok(Born::NotFresh("a node is registered for it"))
            );
        }
        // Its own earlier lease at an older version: restarted in place
        // from 0.8.1 to 0.9, it creates no floor.
        assert_eq!(
            born(&b, FirstWriter::Node("a"), v(0, 9)).await,
            Ok(Born::NotFresh("a node is registered for it"))
        );
        assert_eq!(floor_of(&b).await, None);
        // At its own version, its earlier lease is only its own.
        let b = bucket();
        put(&b, "a", "0.9.1", "released").await;
        assert_eq!(
            born(&b, FirstWriter::Node("a"), v(0, 9)).await,
            Ok(Born::Created(v(0, 9)))
        );
        // An object under nodes/ that does not decode: it cannot be shown
        // to belong to another app.
        let b = bucket();
        b.put_body("nodes/z", b"wasmkeep-node 4\n".to_vec())
            .await
            .unwrap();
        assert_eq!(
            born(&b, FirstWriter::Node("a"), v(0, 9)).await,
            Ok(Born::NotFresh("a node is registered for it"))
        );
        // An operator's fence over no object names no node.
        let b = bucket();
        let fence = crate::node_lease::NodeLeaseObject {
            state: crate::node_lease::NodeState::Fenced,
            instance: None,
            written_at_ms: NOW,
        };
        b.put_body("nodes/f", fence.encode()).await.unwrap();
        assert_eq!(
            born(&b, FirstWriter::Node("a"), v(0, 9)).await,
            Ok(Born::Created(v(0, 9)))
        );
    }

    /// Section 3: the first deploy of a fresh app must not shut the
    /// prefix's live nodes out of it. It creates the lower of its own
    /// version and the lowest live version under the prefix (of every
    /// app: the new app has no node yet), and nothing when that is not
    /// above what no record means or a live version does not parse. A
    /// node that starts later creates the floor the deploy left out.
    #[tokio::test]
    async fn a_first_deploy_never_shuts_out_the_live_nodes() {
        let lower = async |cli: Minor, versions: &[&str]| {
            let b = bucket();
            for (i, version) in versions.iter().enumerate() {
                b.put_body(
                    &format!("nodes/n{i}"),
                    node_body_of("acme/blog", &format!("n{i}"), version, "live", "take"),
                )
                .await
                .unwrap();
            }
            let got = born(&b, FirstWriter::Deploy, cli).await.unwrap();
            (got, floor_of(&b).await, b)
        };
        // No node live under the prefix: the CLI's own version.
        let (got, floor, _) = lower(v(0, 9), &[]).await;
        assert_eq!((got, floor), (Born::Created(v(0, 9)), Some(v(0, 9))));
        // Nodes one minor version behind the CLI: their version.
        let (got, floor, _) = lower(v(0, 10), &["0.9.0", "0.10.0", "0.9.3"]).await;
        assert_eq!((got, floor), (Born::Created(v(0, 9)), Some(v(0, 9))));
        // Nodes ahead of the CLI: the CLI's.
        let (got, floor, _) = lower(v(0, 9), &["0.10.0"]).await;
        assert_eq!((got, floor), (Born::Created(v(0, 9)), Some(v(0, 9))));
        // Nodes of 0.8.1 under a 0.9 CLI: 0.8 is what no record means,
        // so nothing is written, and those nodes start into the new app.
        let (got, floor, b) = lower(v(0, 9), &["0.9.0", "0.8.1"]).await;
        assert!(matches!(got, Born::NotFresh(_)), "{got:?}");
        assert_eq!(floor, None);
        // The deploy lands. Once the 0.8.1 node is gone, the first node
        // of the app, of 0.9, creates the floor the deploy left out,
        // whatever the deploy.
        b.put_body("apps/acme/shop/current", b"x".to_vec())
            .await
            .unwrap();
        b.put_body(
            "nodes/n1",
            node_body_of("acme/blog", "n1", "0.9.0", "live", "take"),
        )
        .await
        .unwrap();
        assert_eq!(
            born(&b, FirstWriter::Node("s1"), v(0, 9)).await,
            Ok(Born::Created(v(0, 9)))
        );
        // The scenario of the review, whole: nodes of 0.8.1 serve another
        // app; a 0.9 `wasmkeep` deploys a new app (no floor, rightly);
        // the first node restarted for it runs 0.9. It must not create a
        // floor above the lowest live version, or the 0.8.1 nodes still
        // to be moved to the new app would be shut out for good.
        let (got, floor, b) = lower(v(0, 9), &["0.8.1", "0.8.1"]).await;
        assert!(matches!(got, Born::NotFresh(_)), "{got:?}");
        assert_eq!(floor, None);
        b.put_body("apps/acme/shop/current", b"x".to_vec())
            .await
            .unwrap();
        let first = born(&b, FirstWriter::Node("s1"), v(0, 9)).await.unwrap();
        assert!(matches!(first, Born::NotFresh(_)), "{first:?}");
        assert_eq!(floor_of(&b).await, None, "no floor above 0.8");
        // Its own live lease (a restart within the horizon) is not
        // another node; the 0.8.1 nodes still are.
        put(&b, "s1", "0.9.0", "live").await;
        let again = born(&b, FirstWriter::Node("s1"), v(0, 9)).await.unwrap();
        assert!(matches!(again, Born::NotFresh(_)), "{again:?}");
        // One version later, beside nodes of 0.9: the floor is 0.9, not
        // the starting node's 0.10.
        let (_, _, b) = lower(v(0, 10), &["0.8.1"]).await;
        b.put_body(
            "nodes/n0",
            node_body_of("acme/blog", "n0", "0.9.0", "live", "take"),
        )
        .await
        .unwrap();
        assert_eq!(
            born(&b, FirstWriter::Node("s1"), v(0, 10)).await,
            Ok(Born::Created(v(0, 9)))
        );
        assert_eq!(floor_of(&b).await, Some(v(0, 9)));
        // A live version that does not parse: nothing.
        let (got, floor, _) = lower(v(0, 10), &["0.9.0", "nine"]).await;
        assert!(matches!(got, Born::NotFresh(_)), "{got:?}");
        assert_eq!(floor, None);
        // A stopped node of an old version is not live: it does not count.
        let b = bucket();
        b.put_body(
            "nodes/gone",
            node_body_of("acme/blog", "gone", "0.8.1", "released", "take"),
        )
        .await
        .unwrap();
        assert_eq!(
            born(&b, FirstWriter::Deploy, v(0, 9)).await,
            Ok(Born::Created(v(0, 9)))
        );

        // The rule alone.
        let at = |version: &str| live("n", version);
        assert_eq!(deploy_floor(v(0, 8), []), Some(v(0, 8)));
        assert_eq!(deploy_floor(v(0, 8), [&at("0.8.1")]), None);
        assert_eq!(deploy_floor(v(0, 9), [&at("0.8.1")]), None);
        assert_eq!(
            deploy_floor(v(0, 11), [&at("0.10.0"), &at("0.9.0")]),
            Some(v(0, 9))
        );
        assert_eq!(deploy_floor(v(0, 9), [&at("0.10.0")]), Some(v(0, 9)));
        assert_eq!(deploy_floor(v(0, 10), [&at("0.10.0"), &at("")]), None);
    }
}

/// Every `.rs` file under `crates/`, with its path.
fn sources() -> Vec<(std::path::PathBuf, String)> {
    fn walk(dir: &std::path::Path, out: &mut Vec<(std::path::PathBuf, String)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n != "target") {
                    walk(&path, out);
                }
            } else if path.extension().is_some_and(|e| e == "rs") {
                let text = std::fs::read_to_string(&path).unwrap();
                out.push((path, text));
            }
        }
    }
    let crates = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_owned();
    let mut out = Vec::new();
    walk(&crates, &mut out);
    assert!(out.len() > 100, "found the workspace's crates");
    out
}

/// The part of a source file that ships: not a test file, and nothing
/// from its test module on (their fixtures spell headers no version
/// writes, to show they are refused).
fn shipped(path: &std::path::Path, text: &str) -> Option<String> {
    let name = path.file_name()?.to_str()?;
    let test_file = name == "tests.rs"
        || name.ends_with("_tests.rs")
        || path
            .components()
            .any(|c| c.as_os_str() == "tests" || c.as_os_str() == "benches");
    if test_file {
        return None;
    }
    // An inline test module (`mod tests {`) ends the shipped part; the
    // declaration of a test file (`mod tests;`) does not: shipped code
    // may follow it.
    let marker = "#[cfg(test)]\nmod ";
    let mut from = 0;
    while let Some(at) = text[from..].find(marker) {
        let at = from + at;
        let line = text[at + marker.len()..].lines().next().unwrap_or_default();
        if line.trim_end().ends_with('{') {
            return Some(text[..at].to_owned());
        }
        from = at + marker.len();
    }
    Some(text.to_owned())
}

#[test]
fn only_an_inline_test_module_ends_the_shipped_part() {
    let path = std::path::Path::new("crates/x/src/lib.rs");
    let text = "a\n#[cfg(test)]\nmod x_tests;\nb \"wasmkeep-b 1\"\n#[cfg(test)]\nmod tests {\n\"wasmkeep-c 2\"\n}\n";
    let shipped_part = shipped(path, text).unwrap();
    assert_eq!(headers(&shipped_part), ["wasmkeep-b 1"]);
    assert_eq!(
        shipped(std::path::Path::new("crates/x/src/tests.rs"), text),
        None
    );
    assert_eq!(
        shipped(std::path::Path::new("crates/x/tests/cluster/a.rs"), text),
        None
    );
}

/// Every `wasmkeep-<name> <n>` and `wasmkeep-<name> v<n>` in `text`.
fn headers(text: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut rest = text;
    while let Some(at) = rest.find("wasmkeep-") {
        let tail = &rest[at..];
        let name_end = "wasmkeep-".len()
            + tail["wasmkeep-".len()..]
                .find(|c: char| !(c.is_ascii_lowercase() || c == '-'))
                .unwrap_or(0);
        rest = &tail[name_end.max(1)..];
        let Some(number) = tail[name_end..].strip_prefix(' ') else {
            continue;
        };
        let digits = number.strip_prefix('v').unwrap_or(number);
        let len = digits
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(digits.len());
        // `wasmkeep-node 3`, not a version (`wasmkeep-cli 0.9.0`).
        if len == 0 || digits[len..].starts_with('.') {
            continue;
        }
        let header_len = name_end + 1 + (number.len() - digits.len()) + len;
        found.push(tail[..header_len].to_owned());
    }
    found
}

#[test]
fn the_scan_finds_headers_and_nothing_else() {
    assert_eq!(
        headers(
            "a \"wasmkeep-node 3\\n\" b wasmkeep-call v4\n wasmkeep-cli 0.9.0 wasmkeep-manifest {} wasmkeep-owner 12, wasmkeep-node.toml wasmkeep-x"
        ),
        ["wasmkeep-node 3", "wasmkeep-call v4", "wasmkeep-owner 12"]
    );
}

/// ADR 0043, section 2: the gate table is a test. A header constant in
/// the crates without a row in [`FORMATS`] fails here: add the row, with
/// the version that introduces it and its word, and gate its writer.
#[test]
fn every_format_constant_has_a_row_in_the_gate_table() {
    let sources = sources();
    let mut missing = std::collections::BTreeSet::new();
    let mut seen = std::collections::BTreeSet::new();
    let mut all = String::new();
    for (path, text) in &sources {
        all.push_str(text);
        let Some(text) = shipped(path, text) else {
            continue;
        };
        for header in headers(&text) {
            if format(&header).is_none() {
                missing.insert(format!("{header} ({})", path.display()));
            }
            seen.insert(header);
        }
    }
    assert!(
        missing.is_empty(),
        "format constants with no row in wasmkeep_cluster::upgrade::FORMATS (ADR 0043, \
         section 2): {missing:#?}"
    );
    // The manifest's writer formats its number: 1 to 8 are its decoder's.
    for number in 1..=8 {
        assert!(seen.contains(&format!("wasmkeep-manifest {number}")));
    }
    for row in FORMATS {
        // No row outlives its format.
        if row.name.starts_with("wasmkeep-") {
            assert!(all.contains(row.name), "{} is in no source", row.name);
        }
        // Its test of the unknown case exists.
        assert!(
            all.contains(&format!("fn {}(", row.unknown)),
            "{}: no test named {}",
            row.name,
            row.unknown
        );
        // No format is newer than this build, and its words are ones a
        // node can announce.
        assert!(
            row.since <= this_minor().unwrap().max(v(0, 9)),
            "{}",
            row.name
        );
        if let Rule::Gate(words) = row.rule {
            for word in words {
                assert!(
                    node("n", "0.9.0", "live", word).announces(word),
                    "{}: no node announces `{word}`",
                    row.name
                );
            }
        }
    }
    let mut names: Vec<&str> = FORMATS.iter().map(|f| f.name).collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), FORMATS.len(), "one row per format");
}

/// The decoders of this crate, each given its own header with a later
/// number: none reads it as the form it knows.
#[test]
fn a_later_header_is_never_read_as_a_known_one() {
    use crate::actor_restore::Request;
    use crate::node_lease::{NodeLeaseObject, OwnerRecord, decode_marker, encode_marker};
    /// `body` with the number on its first line replaced by `to`.
    fn renumbered(body: &[u8], to: &str) -> Vec<u8> {
        let text = std::str::from_utf8(body).unwrap();
        let (first, rest) = text.split_once('\n').unwrap();
        let (name, _) = first.rsplit_once(' ').unwrap();
        format!("{name} {to}\n{rest}").into_bytes()
    }
    let request = Request {
        source: "backup run-1 4:8".into(),
        size: 1234,
        page_size: 4096,
        checksum: 123,
        if_head: None,
    };
    type Reads = fn(&[u8]) -> bool;
    let cases: [(&str, Vec<u8>, Reads); 9] = [
        ("wasmkeep-node 3", node_body("a", "0.9.0", "live", "take"), |b| {
            NodeInfo::decode(b).is_some()
        }),
        ("wasmkeep-node 3", node_body("a", "0.9.0", "live", "take"), |b| {
            NodeLeaseObject::decode(b).is_some()
        }),
        ("wasmkeep-owner 1", b"wasmkeep-owner 1\na i1\n3\n".to_vec(), |b| {
            OwnerRecord::decode(b).is_some()
        }),
        (
            "wasmkeep-owner 2",
            b"wasmkeep-owner 2\n- -\n42\nhead 41 187 09abcdef01234567\nset-at 42\ncomplete\nidle-since -\n"
                .to_vec(),
            |b| OwnerRecord::decode(b).is_some(),
        ),
        ("wasmkeep-lease 2", encode_marker(4), |b| {
            decode_marker(b).is_some()
        }),
        ("wasmkeep-lease 1", b"wasmkeep-lease 1\na\n9\n0\n".to_vec(), |b| {
            crate::lease::LeaseRecord::decode(b).is_some()
        }),
        (
            "wasmkeep-blob-sweep 1",
            b"wasmkeep-blob-sweep 1\npending\n6e340b9cffb37a98\nn\n".to_vec(),
            |b| crate::blob_sweep::Record::decode(b).is_some(),
        ),
        ("wasmkeep-restore 1", request.encode(), |b| {
            Request::decode(b).is_some()
        }),
        (
            "wasmkeep-recorded-head 1",
            crate::app_format::ENABLED_BODY.to_vec(),
            |b| crate::app_format::decode(Some(b)) == Ok(crate::app_format::AppFormat::RecordedHead),
        ),
    ];
    for (header, body, reads) in cases {
        assert!(body.starts_with(header.as_bytes()), "{header}");
        assert!(format(header).is_some(), "{header} has a row");
        assert!(reads(&body), "{header}: its own form is read");
        for later in ["9", "10", "99"] {
            assert!(
                !reads(&renumbered(&body, later)),
                "{header} read with the number {later}"
            );
        }
    }
}
