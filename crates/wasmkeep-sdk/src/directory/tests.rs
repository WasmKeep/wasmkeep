use super::*;

fn initial() -> State {
    State::create("app/owner", "edge/a", b"a").unwrap()
}
fn split(lo: u64, at: u64, expect: u64, op: &str) -> Change {
    Change::Split {
        lo,
        at,
        target: "edge/b".into(),
        meta: b"b".to_vec(),
        expect,
        op: op.into(),
    }
}
#[test]
fn split_merge_versions_and_replay() {
    let a = initial();
    let b = a.change("app/owner", &split(0, 1 << 63, 1, "s")).unwrap();
    assert_eq!(a.map.lookup(u64::MAX).unwrap().target, "edge/a");
    assert_eq!(b.map.version, 2);
    assert_eq!(
        b.map.entries.iter().map(|e| e.version).collect::<Vec<_>>(),
        vec![2, 2]
    );
    assert_eq!(
        b.change("app/owner", &split(0, 1 << 63, 1, "s")).unwrap(),
        b
    );
    assert_eq!(
        b.change("app/owner", &split(0, 10, 1, "other")),
        Err(Error::Conflict(2))
    );
    for keep in [Keep::Left, Keep::Right] {
        let c = b
            .change(
                "app/owner",
                &Change::Merge {
                    lo: 1 << 63,
                    keep,
                    expect: 2,
                    op: "m".into(),
                },
            )
            .unwrap();
        assert_eq!(c.map.entries.len(), 1);
        assert_eq!(c.map.entries[0].hi, None);
        assert_eq!(c.map.entries[0].version, 3);
        assert_eq!(
            c.map.entries[0].target,
            if keep == Keep::Left {
                "edge/a"
            } else {
                "edge/b"
            }
        );
    }
}
#[test]
fn ownership_limits_and_invalid_boundaries() {
    let a = initial();
    assert_eq!(
        a.change("other", &split(0, 10, 1, "x")),
        Err(Error::Forbidden)
    );
    for (lo, at) in [(1, 2), (0, 0)] {
        assert!(a.change("app/owner", &split(lo, at, 1, "x")).is_err());
    }
    assert!(State::create("app/owner", "edge/a", &vec![0; 1025]).is_err());
    let mut a = a;
    for at in 1..4096 {
        a = a
            .change("app/owner", &split(at - 1, at, at, &at.to_string()))
            .unwrap();
    }
    assert!(
        a.change("app/owner", &split(4095, 4096, 4096, "full"))
            .is_err()
    );
}
#[test]
fn arbitrary_sequences_cover_the_space_and_lookup_sees_whole_snapshots() {
    let mut a = initial();
    let mut seed = 17u64;
    for n in 0..300 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let e = a.map.lookup(seed).unwrap().clone();
        let change = if n % 3 == 0 && a.map.entries.len() > 1 {
            Change::Merge {
                lo: a.map.entries[1].lo,
                keep: Keep::Right,
                expect: a.map.version,
                op: n.to_string(),
            }
        } else {
            split(e.lo, seed, a.map.version, &n.to_string())
        };
        let before = a.map.clone();
        if let Ok(after) = a.change("app/owner", &change) {
            a = after;
        }
        assert_eq!(before.lookup(seed).unwrap().hi, e.hi);
        assert_eq!(a.map.entries[0].lo, 0);
        assert_eq!(a.map.entries.last().unwrap().hi, None);
        for pair in a.map.entries.windows(2) {
            assert_eq!(pair[0].hi, Some(pair[1].lo));
            assert!(pair[0].lo < pair[1].lo);
        }
        assert!(a.map.lookup(0).is_some() && a.map.lookup(u64::MAX).is_some());
    }
}
#[test]
fn shared_point_vectors() {
    for line in include_str!("point-vectors.txt").lines() {
        let (key, expected) = line.split_once(' ').unwrap();
        assert_eq!(
            point(&wire::unhex(key).unwrap()),
            u64::from_str_radix(expected, 16).unwrap()
        );
    }
}

#[test]
fn untouched_versions_boundaries_and_old_operations() {
    let a = initial()
        .change("app/owner", &split(0, 100, 1, "a"))
        .unwrap();
    let b = a.change("app/owner", &split(100, 200, 2, "b")).unwrap();
    assert_eq!(
        b.map.entries.iter().map(|e| e.version).collect::<Vec<_>>(),
        vec![2, 3, 3]
    );
    assert_eq!(
        b.change("app/owner", &split(0, 100, 1, "a")),
        Err(Error::Conflict(3))
    );
    assert!(
        b.change("app/owner", &split(0, 100, 3, "boundary"))
            .is_err()
    );
    assert!(
        b.change(
            "app/owner",
            &Change::Merge {
                lo: 0,
                keep: Keep::Left,
                expect: 3,
                op: "zero".into()
            }
        )
        .is_err()
    );
    assert!(State::create("owner", "edge/a", &vec![0; 1024]).is_ok());
    let encoded = encode_map(&b.map);
    assert_eq!(decode_map(&encoded), Some(b.map));
    for end in 0..encoded.len() {
        assert!(decode_map(&encoded[..end]).is_none());
    }
}
