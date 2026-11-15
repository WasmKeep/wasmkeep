#![allow(clippy::unwrap_used)] // test code

use super::*;

/// A page as two nodes see it: entry `i` is in node `i % 2`'s shard,
/// and anyone's once `i < anyones`.
fn two_views(len: usize, anyones: usize) -> [Vec<SweepClaim>; 2] {
    [0, 1].map(|node| {
        (0..len)
            .map(|i| match (i % 2 == node, i < anyones) {
                (true, _) => SweepClaim::Mine,
                (false, true) => SweepClaim::Anyones,
                (false, false) => SweepClaim::Skip,
            })
            .collect()
    })
}

#[test]
fn two_nodes_split_a_page_of_anyones_entries() {
    // A backlog overdue three times over is anyone's. Both
    // nodes' cursors sit on the same key; first come, first served,
    // they delivered the same entries and recovered at one node's rate.
    let [a, b] = two_views(2_048, 2_048);
    let (pa, pb) = (plan_sweep(&a, 256, 0), plan_sweep(&b, 256, 0));
    assert_eq!((pa.deliver.len(), pb.deliver.len()), (256, 256));
    // Each delivers mostly its own share; only the reserve for
    // anyone's entries (an eighth) may overlap the other's.
    let shared = pa.deliver.iter().filter(|i| pb.deliver.contains(i)).count();
    assert!(shared <= 64, "{shared} entries delivered by both nodes");
    for (plan, claims) in [(&pa, &a), (&pb, &b)] {
        let own = plan
            .deliver
            .iter()
            .filter(|&&i| claims[i] == SweepClaim::Mine)
            .count();
        assert_eq!(own, 224);
        assert!((440..460).contains(&plan.walked), "{}", plan.walked);
    }
}

#[test]
fn own_entries_that_never_leave_cannot_hold_anyones_back() {
    // Every page has more own entries than the cap (actors that cannot
    // start keep their entries): anyone's still get their share.
    let claims: Vec<_> = (0..2_048)
        .map(|i| match i % 4 {
            0 => SweepClaim::Anyones,
            _ => SweepClaim::Mine,
        })
        .collect();
    let plan = plan_sweep(&claims, 256, 0);
    let anyones: Vec<_> = plan
        .deliver
        .iter()
        .filter(|&&i| claims[i] == SweepClaim::Anyones)
        .collect();
    assert_eq!(anyones.len(), 32);
    assert_eq!(*anyones[0], 0, "from the rotation's start");
    assert_eq!(plan.deliver.len(), 256);

    // Rotated, the share goes to other anyone's entries of the same
    // walk, so the same ones (actors that cannot start either) do not
    // take it on every pass; the rotation wraps.
    let met = (0..plan.walked)
        .filter(|&i| claims[i] == SweepClaim::Anyones)
        .count() as u64;
    let mut covered: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for rotation in 0..met {
        let rotated = plan_sweep(&claims, 256, rotation);
        assert_eq!(rotated.walked, plan.walked);
        let picked: Vec<_> = rotated
            .deliver
            .iter()
            .filter(|&&i| claims[i] == SweepClaim::Anyones)
            .collect();
        assert_eq!(picked.len(), 32);
        covered.extend(picked.into_iter().copied());
    }
    assert_eq!(
        covered.len() as u64,
        met,
        "every anyone's entry walked is reachable"
    );
    assert_eq!(plan_sweep(&claims, 256, met), plan);
}

#[test]
fn the_walk_never_passes_an_own_entry_it_did_not_deliver() {
    for anyones in [0, 100, 600, 2_048] {
        for claims in two_views(2_048, anyones) {
            let plan = plan_sweep(&claims, 256, 0);
            for (i, claim) in claims[..plan.walked].iter().enumerate() {
                if *claim == SweepClaim::Mine {
                    assert!(plan.deliver.contains(&i), "own entry {i} passed over");
                }
            }
            assert!(plan.deliver.iter().all(|&i| i < plan.walked));
        }
    }
}

#[test]
fn anyones_entries_fill_what_the_cap_leaves() {
    // Few own entries: the rest of the cap goes to anyone's, in order,
    // so an entry whose shard owner never delivers it still goes.
    let mut claims = vec![SweepClaim::Anyones; 10];
    claims.push(SweepClaim::Mine);
    claims.extend([SweepClaim::Skip; 5]);
    let plan = plan_sweep(&claims, 4, 0);
    assert_eq!(plan.deliver, [10, 0, 1, 2]);
    assert_eq!(plan.walked, claims.len());
    // Only anyone's (a node whose shard is empty here): first come.
    let plan = plan_sweep(&[SweepClaim::Anyones; 6], 4, 0);
    assert_eq!((plan.deliver, plan.walked), (vec![0, 1, 2, 3], 6));
    // Nothing to walk past with no cap at all.
    assert_eq!(plan_sweep(&claims, 0, 0).walked, 0);
    assert!(plan_sweep(&[], 4, 0).deliver.is_empty());
}

fn owned(at: u64, generation: u64, nonce: &str) -> Entry {
    Entry::owned(at, generation, nonce).unwrap()
}

#[test]
fn keys_round_trip_through_a_listing() {
    let tagged = owned(1_700_000_000_000, 7, "a1b2c3");
    let tail = key_tail("chat", "Room 1", &tagged);
    assert_eq!(
        tail,
        "001700000000000/chat.t/526f6f6d2031/00000000000000000007-a1b2c3"
    );
    let listed = parse_listed(&tail).unwrap();
    assert_eq!(
        (listed.class, listed.name.as_str(), &listed.entry),
        ("chat", "Room 1", &tagged)
    );

    let legacy = Entry::legacy(5);
    let tail = key_tail("chat", "é", &legacy);
    assert_eq!(tail, "000000000000005/chat/c3a9");
    // Never a folder of tagged keys: they are under a sibling.
    assert_eq!(tagged_prefix("chat", "é", 5), "000000000000005/chat.t/c3a9");
    assert!(!key_tail("chat", "é", &owned(5, 1, "a")).starts_with(&format!("{tail}/")));
    let listed = parse_listed(&tail).unwrap();
    assert_eq!((listed.name.as_str(), &listed.entry), ("é", &legacy));

    // The largest values still fit their fields and parse back.
    let big = owned(u64::MAX, u64::MAX, &"f".repeat(64));
    assert_eq!(parse_listed(&key_tail("c", "n", &big)).unwrap().entry, big);
}

#[test]
fn malformed_keys_are_skipped() {
    for good in [
        "000000000000005/c/6e",
        "000000000000005/c.t/6e/00000000000000000007-ab",
    ] {
        assert!(parse_listed(good).is_some(), "{good:?}");
    }
    for bad in [
        "",
        "000000000000005",
        "000000000000005/c",
        "x00000000000005/c/6e",
        "/c/6e",
        "000000000000005//6e",
        "000000000000005/c/6",    // odd hex
        "000000000000005/c/zz",   // not hex
        "000000000000005/c/ff",   // not UTF-8
        "000000000000005/c.t/6e", // a tagged folder without a tag
        "000000000000005/c.x/6e", // not a class
        "000000000000005/.t/6e/00000000000000000007-ab",
        "000000000000005/c.t.t/6e/00000000000000000007-ab",
        // Tagged keys under the legacy key (never written by any node).
        "000000000000005/c/6e/00000000000000000007-ab",
        "000000000000005/c.t/6e/",
        "000000000000005/c.t/6e/7-ab",
        "000000000000005/c.t/6e/0000000000000000000x-ab",
        "000000000000005/c.t/6e/00000000000000000007",
        "000000000000005/c.t/6e/00000000000000000007-",
        "000000000000005/c.t/6e/00000000000000000007-AB",
        "000000000000005/c.t/6e/00000000000000000007-ab-cd",
        "000000000000005/c.t/6e/00000000000000000007-ab/x",
        "99999999999999999999999/c/6e", // overflows u64
    ] {
        assert_eq!(parse_listed(bad), None, "{bad:?}");
    }
    let long = format!(
        "000000000000005/c.t/6e/00000000000000000007-{}",
        "a".repeat(65)
    );
    assert_eq!(parse_listed(&long), None);
}

#[test]
fn a_mixed_listing_parses_in_key_order() {
    // Legacy and tagged entries of one actor at one time, another actor,
    // and junk: readers get every valid entry, in due-time order (at one
    // time, tagged keys sort first: `.` comes before `/`).
    let mut keys = [
        "000000000000020/c/61",
        "000000000000010/c.t/61/00000000000000000003-0f",
        "000000000000010/c/61",
        "000000000000010/c.t/62/00000000000000000001-1",
        "000000000000010/c.t/61/00000000000000000002-ff",
        "000000000000015/c.t/61/garbage",
        "000000000000015/c",
    ];
    keys.sort_unstable();
    let parsed: Vec<(String, Entry)> = keys
        .iter()
        .filter_map(|k| parse_listed(k))
        .map(|l| (l.name, l.entry))
        .collect();
    assert_eq!(
        parsed,
        [
            ("a".to_owned(), owned(10, 2, "ff")),
            ("a".to_owned(), owned(10, 3, "0f")),
            ("b".to_owned(), owned(10, 1, "1")),
            ("a".to_owned(), Entry::legacy(10)),
            ("a".to_owned(), Entry::legacy(20)),
        ]
    );
}

#[test]
fn meta_round_trips_and_reads_legacy_values() {
    assert_eq!(to_meta(None), "");
    assert_eq!(parse_meta(""), None);
    let tagged = owned(1_234, 9, "beef");
    assert_eq!(to_meta(Some(&tagged)), "1234/00000000000000000009-beef");
    assert_eq!(parse_meta(&to_meta(Some(&tagged))), Some(tagged));
    // What nodes without tags wrote.
    assert_eq!(parse_meta("1234"), Some(Entry::legacy(1_234)));
    assert_eq!(to_meta(Some(&Entry::legacy(1_234))), "1234");
    for bad in [
        "x",
        "-1",
        "+1",
        "1234/",
        "/00000000000000000009-beef",
        "1234/9-beef",
        "1234/00000000000000000009-",
        "1234/00000000000000000009-beef/1",
    ] {
        assert_eq!(parse_meta(bad), None, "{bad:?}");
    }
}

#[test]
fn only_a_change_or_a_legacy_entry_needs_publishing() {
    let tagged = owned(10, 1, "a");
    assert!(!needs_publish(None, None));
    assert!(needs_publish(None, Some(10)));
    assert!(needs_publish(Some(&tagged), None));
    assert!(needs_publish(Some(&tagged), Some(11)));
    assert!(
        !needs_publish(Some(&tagged), Some(10)),
        "unchanged: no write, whatever the generation now"
    );
    assert!(
        needs_publish(Some(&Entry::legacy(10)), Some(10)),
        "migrated"
    );
    assert!(needs_publish(Some(&Entry::legacy(10)), None));
}

#[test]
fn the_generation_limits_what_may_be_deleted() {
    assert!(may_delete(&owned(1, 4, "a"), 5));
    assert!(may_delete(&owned(1, 5, "a"), 5));
    assert!(!may_delete(&owned(1, 6, "a"), 5));
    assert!(may_delete(&Entry::legacy(1), 0));
}

#[test]
fn clearing_spares_the_indexed_entry_and_newer_owners() {
    let hint = 100;
    let mine = owned(hint, 5, "aa");
    let older = owned(hint, 3, "bb");
    let newer = owned(hint, 6, "cc");
    let elsewhere = owned(hint + 1, 2, "dd");
    let listed = [
        mine.clone(),
        older.clone(),
        newer.clone(),
        elsewhere,
        older.clone(),
    ];

    // Nothing indexed at the hint: everything at the hint from this
    // generation or older goes, the legacy key too, each once.
    assert_eq!(
        entries_to_clear(hint, &listed, None, 5),
        [mine.clone(), older.clone(), Entry::legacy(hint)]
    );
    // The indexed entry stays, whoever wrote it.
    assert_eq!(
        entries_to_clear(hint, &listed, Some(&older), 5),
        [mine.clone(), Entry::legacy(hint)]
    );
    assert_eq!(
        entries_to_clear(hint, &listed, Some(&mine), 5),
        [older.clone(), Entry::legacy(hint)]
    );
    // A legacy indexed entry at the hint stays too.
    assert_eq!(
        entries_to_clear(hint, &listed, Some(&Entry::legacy(hint)), 5),
        [mine.clone(), older.clone()]
    );
    // An owner that is itself stale spares what newer owners wrote.
    assert_eq!(
        entries_to_clear(hint, &listed, None, 4),
        [older, Entry::legacy(hint)]
    );
    // An empty listing still clears the legacy key.
    assert_eq!(entries_to_clear(hint, &[], None, 1), [Entry::legacy(hint)]);
}

#[test]
fn clearing_never_returns_the_indexed_entry_or_a_newer_one() {
    // Exhaustive over small cases: every subset of candidate entries
    // listed, each candidate (or nothing) indexed, each generation.
    let candidates = [
        Entry::legacy(7),
        owned(7, 1, "a"),
        owned(7, 2, "a"),
        owned(7, 2, "b"),
        owned(7, 3, "a"),
        owned(8, 2, "a"),
    ];
    for mask in 0u32..(1 << candidates.len()) {
        let listed: Vec<Entry> = candidates
            .iter()
            .enumerate()
            .filter(|(i, _)| mask & (1 << i) != 0)
            .map(|(_, e)| e.clone())
            .collect();
        for indexed in std::iter::once(None).chain(candidates.iter().map(Some)) {
            for generation in 0..4 {
                let clear = entries_to_clear(7, &listed, indexed, generation);
                for e in &clear {
                    assert_ne!(Some(e), indexed, "{listed:?} {indexed:?} {generation}");
                    assert!(may_delete(e, generation), "{e:?} at {generation}");
                    assert_eq!(e.at, 7);
                }
            }
        }
    }
}

#[test]
fn nonces_are_lowercase_hex() {
    assert!(Tag::new(1, "0123456789abcdef").is_some());
    for bad in ["", "A", "g", "-", "a/b", &"a".repeat(65)] {
        assert!(Tag::new(1, bad).is_none(), "{bad:?}");
    }
}

#[test]
fn an_idle_sweep_waits_for_the_earliest_entry_within_bounds() {
    let (period, overdue) = (10_000, 15_000);
    let wait = |found| next_sweep_wait_ms(found, 100_000, period, overdue);
    assert_eq!(wait(SweepFound::Backlog), period);
    assert_eq!(wait(SweepFound::Idle { next_at: None }), 60_000);
    // Due in 5 s: the sweep's 15 s later, 20 s from now.
    assert_eq!(
        wait(SweepFound::Idle {
            next_at: Some(105_000)
        }),
        20_000
    );
    // The sweep's already, or soon: a period, never less.
    assert_eq!(
        wait(SweepFound::Idle {
            next_at: Some(80_000)
        }),
        period
    );
    assert_eq!(
        wait(SweepFound::Idle {
            next_at: Some(90_000)
        }),
        period
    );
    // Far ahead: the longest wait.
    assert_eq!(
        wait(SweepFound::Idle {
            next_at: Some(10_000_000)
        }),
        60_000
    );
}
