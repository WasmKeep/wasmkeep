//! Bounded interleaving model for ADR 0033's app-start fence. Explore every
//! interleaving of a node starting (including stale disk) and a restore.
//! The cluster test exercises the corresponding production node paths.
#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum Broken {
    No,
    LateFence,
    EarlyOpen,
    NoMarker,
}
#[derive(Clone, Default)]
struct State {
    node_step: usize,
    restore_step: usize,
    lease: bool,
    checked: bool,
    serving: bool,
    restoring: bool,
    restored: u64,
    local: u64,
    cleared: bool,
    ready: bool,
    aborted: bool,
    stale_disk: bool,
}
fn explore(s: State, broken: Broken, ends: &mut usize) -> bool {
    if s.serving && s.cleared && (!s.ready || s.local != s.restored || s.stale_disk) {
        return false;
    }
    if s.node_step == 4 && (s.restore_step == 9 || s.aborted) {
        *ends += 1;
        return true;
    }
    if s.node_step < 4 {
        let mut n = s.clone();
        match n.node_step {
            0 => {
                if n.local != n.restored {
                    n.stale_disk = false;
                }
                n.local = n.restored; // prepare_local, before the lease
            }
            1 => n.lease = true,
            2 => n.checked = !n.restoring && n.local == n.restored,
            3 => n.serving = n.checked && n.lease,
            _ => unreachable!(),
        }
        n.node_step += 1;
        if !explore(n, broken, ends) {
            return false;
        }
    }
    if s.restore_step < 9 && !s.aborted {
        let mut r = s;
        match r.restore_step {
            0 => {
                // starts fenced before the authoritative nodes listing
                if broken != Broken::LateFence {
                    r.restoring = true;
                }
                if broken != Broken::NoMarker {
                    r.restored += 1;
                }
            }
            1 => {
                if r.lease {
                    r.aborted = true;
                }
            }
            2 => {
                if broken == Broken::LateFence {
                    r.restoring = true;
                }
                r.cleared = true;
            }
            3 => {} // history
            4 => {} // generation floors
            5 => {
                if broken == Broken::EarlyOpen {
                    r.restoring = false;
                }
            }
            6 => {}              // alarms
            7 => r.ready = true, // deploy/secrets
            8 => r.restoring = false,
            _ => unreachable!(),
        }
        r.restore_step += 1;
        if !explore(r, broken, ends) {
            return false;
        }
    }
    true
}
#[test]
fn restore_fence_precedes_nodes_listing_and_opens_last() {
    let mut ends = 0;
    assert!(explore(
        State {
            stale_disk: true,
            ..State::default()
        },
        Broken::No,
        &mut ends
    ));
    assert!(ends > 100, "vacuous enumeration: {ends}");
    for broken in [Broken::LateFence, Broken::EarlyOpen, Broken::NoMarker] {
        assert!(!explore(
            State {
                stale_disk: true,
                ..State::default()
            },
            broken,
            &mut 0
        ));
    }
}
// Seeded, timed uploads and competing operations. The oracle uses the same
// history planner and generation marker decoder as a real restart.
fn timed(seed: u64, wait: bool, marker: bool) -> bool {
    use std::{cell::RefCell, collections::BTreeSet, rc::Rc, time::Duration};
    use wasmkeep_cluster::replication::{Listing, SegmentId, plan_restore, snapshot_key};
    use wasmkeep_sim::rng::Rng;
    #[derive(Default)]
    struct Bucket {
        fence: bool,
        lock: bool,
        marker: u64,
        local: u64,
        keys: BTreeSet<String>,
        seed: Vec<u8>,
        refused: usize,
    }
    let rt = madsim::runtime::Runtime::with_seed_and_config(seed, madsim::Config::default());
    rt.block_on(async move {
        let bucket = Rc::new(RefCell::new(Bucket::default()));
        let mut rng = Rng::new(seed);
        for round in 1..=2 {
            // prepare_local read, followed by a gap before lease/app_check.
            let prepared = bucket.borrow().marker;
            bucket.borrow_mut().local = prepared;
            {
                let mut b = bucket.borrow_mut();
                assert!(!b.lock);
                b.lock = true;
                b.fence = true;
                if marker {
                    b.marker += 1;
                }
            }
            let late = rng.below(30) + 1;
            let pending = bucket.clone();
            let upload = madsim::task::spawn_local(async move {
                madsim::time::sleep(Duration::from_millis(late)).await;
                pending.borrow_mut().keys.insert(snapshot_key(SegmentId {
                    generation: 90 + round,
                    txid: 1,
                }));
            });
            let competing = bucket.clone();
            let delay = rng.below(20);
            let operators = madsim::task::spawn_local(async move {
                madsim::time::sleep(Duration::from_millis(delay)).await;
                let mut b = competing.borrow_mut();
                // Another restore, deploy, and app-delete must all refuse.
                if b.lock {
                    b.refused += 1;
                }
                if b.fence {
                    b.refused += 2;
                }
            });
            if wait {
                madsim::time::sleep(Duration::from_millis(40)).await;
            }
            {
                let mut b = bucket.borrow_mut();
                b.keys.clear();
                b.keys.insert(snapshot_key(SegmentId {
                    generation: round,
                    txid: 1,
                }));
                b.seed = wasmkeep_cluster::node_lease::encode_marker(100 + round);
            }
            upload.await.unwrap();
            operators.await.unwrap();
            let mut b = bucket.borrow_mut();
            let plan =
                plan_restore(&Listing::from_keys(b.keys.iter().map(String::as_str))).unwrap();
            if plan.head()
                != (SegmentId {
                    generation: round,
                    txid: 1,
                })
            {
                return false;
            }
            assert_eq!(
                wasmkeep_cluster::node_lease::decode_marker(&b.seed),
                Some(100 + round)
            );
            b.fence = false;
            // A node prepared before restore must fail its post-lease check.
            if b.local == b.marker {
                return false;
            }
            b.lock = false;
            assert_eq!(b.refused, 3 * round as usize);
        }
        true
    })
}

#[test]
fn seeded_late_uploads_two_restores_and_operator_start_gaps() {
    let seeds = std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200);
    for seed in 0..seeds {
        assert!(timed(seed, true, true), "seed {seed}");
        assert!(
            !timed(seed, false, true),
            "missing quiet-window mutation survived seed {seed}"
        );
        assert!(
            !timed(seed, true, false),
            "missing marker mutation survived seed {seed}"
        );
    }
}
