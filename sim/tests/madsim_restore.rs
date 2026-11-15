//! Bounded interleaving model for ADR 0033's app-start fence. Explore every
//! interleaving of a node starting (including stale disk) and a restore.
//! The CLI is a binary using real Tokio timers and synchronous
//! SQLite replay; it cannot run on madsim's executor/virtual clock as wired.
//! A direct tokio::time::sleep probe panics with "there is no reactor running".
//! Instead this model calls the same fence decisions as the server. The CLI
//! fault tests exercise the full executor on Tokio; this is not executor coverage.
#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use wasmkeep_cluster::restore_fence::{marker_matches, start_allowed};

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
fn explore(s: State, broken: Broken, ends: &mut usize) -> Result<(), Violation> {
    if s.serving && s.cleared {
        if !s.ready {
            return Err(Violation::PartialState);
        }
        if s.local != s.restored || s.stale_disk {
            return Err(Violation::StaleDisk);
        }
    }
    if s.node_step == 4 && (s.restore_step == 9 || s.aborted) {
        *ends += 1;
        return Ok(());
    }
    if s.node_step < 4 {
        let mut n = s.clone();
        match n.node_step {
            0 => {
                if !marker_matches(
                    Some(&n.restored.to_le_bytes()),
                    Some(&n.local.to_le_bytes()),
                ) {
                    n.stale_disk = false;
                }
                n.local = n.restored; // prepare_local, before the lease
            }
            1 => n.lease = true,
            2 => {
                n.checked = start_allowed(false, n.restoring)
                    && marker_matches(
                        Some(&n.restored.to_le_bytes()),
                        Some(&n.local.to_le_bytes()),
                    )
            }
            3 => n.serving = n.checked && n.lease,
            _ => unreachable!(),
        }
        n.node_step += 1;
        explore(n, broken, ends)?;
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
        explore(r, broken, ends)?;
    }
    Ok(())
}
#[test]
fn restore_fence_precedes_nodes_listing_and_opens_last() {
    let mut ends = 0;
    assert_eq!(
        explore(
            State {
                stale_disk: true,
                ..State::default()
            },
            Broken::No,
            &mut ends
        ),
        Ok(())
    );
    assert!(ends > 100, "vacuous enumeration: {ends}");
    for (broken, reason) in [
        (Broken::LateFence, Violation::PartialState),
        (Broken::EarlyOpen, Violation::PartialState),
        (Broken::NoMarker, Violation::StaleDisk),
    ] {
        assert_eq!(
            explore(
                State {
                    stale_disk: true,
                    ..State::default()
                },
                broken,
                &mut 0
            ),
            Err(reason)
        );
    }
}

// The timed model uses shared bucket state and two concurrent restore tasks.
// Faults are retries of failed PUTs; late uploads model an already sent request.
use std::{cell::RefCell, collections::BTreeSet, rc::Rc, time::Duration};
use wasmkeep_cluster::replication::{Listing, SegmentId, plan_restore, snapshot_key};
use wasmkeep_sim::rng::Rng;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mutation {
    None,
    NoQuiet,
    BeyondDeadline,
    NoMarker,
    NoLock,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Violation {
    PartialState,
    LateHistory,
    StaleDisk,
    ConcurrentWriter,
}
#[derive(Default)]
struct Bucket {
    fence: bool,
    lock: bool,
    marker: u64,
    keys: BTreeSet<String>,
    active: usize,
    overlap: bool,
    rejected: usize,
    retries: usize,
}
async fn operation(
    bucket: Rc<RefCell<Bucket>>,
    id: u64,
    delay: u64,
    faults: u64,
    mutation: Mutation,
) -> bool {
    madsim::time::sleep(Duration::from_secs(delay)).await;
    {
        let mut b = bucket.borrow_mut();
        if b.lock && mutation != Mutation::NoLock {
            b.rejected += 1;
            return false;
        }
        b.lock = true;
        b.active += 1;
        b.overlap |= b.active > 1;
        b.fence = true;
        if mutation != Mutation::NoMarker {
            b.marker += 1;
        }
    }
    if mutation != Mutation::NoQuiet {
        madsim::time::sleep(Duration::from_secs(40)).await;
    }
    bucket.borrow_mut().keys.clear();
    for _ in 0..faults {
        bucket.borrow_mut().retries += 1;
        madsim::time::sleep(Duration::from_secs(1)).await;
    }
    bucket.borrow_mut().keys.insert(snapshot_key(SegmentId {
        generation: id,
        txid: 1,
    }));
    // Hold through completion, allowing the competing operation and start task
    // to run even in the no-quiet mutant.
    madsim::time::sleep(Duration::from_secs(40)).await;
    let mut b = bucket.borrow_mut();
    b.fence = false;
    b.active -= 1;
    b.lock = false;
    true
}
fn timed(seed: u64, mutation: Mutation) -> Result<(), Violation> {
    let rt = madsim::runtime::Runtime::with_seed_and_config(seed, madsim::Config::default());
    rt.block_on(async move {
        let bucket = Rc::new(RefCell::new(Bucket::default()));
        let mut rng = Rng::new(seed);
        // The healthy request deadline is strictly below the quiet window.
        // Only a deadline mutant can upload after it (with randomized lateness).
        let late = if mutation == Mutation::BeyondDeadline {
            42 + rng.below(30)
        } else {
            2 + rng.below(35)
        };
        let pending = bucket.clone();
        let upload = madsim::task::spawn_local(async move {
            madsim::time::sleep(Duration::from_secs(late)).await;
            pending.borrow_mut().keys.insert(snapshot_key(SegmentId {
                generation: 99,
                txid: 1,
            }));
        });
        let local = bucket.borrow().marker; // node prepared before either restore
        let first =
            madsim::task::spawn_local(operation(bucket.clone(), 1, 0, 1 + rng.below(3), mutation));
        let second = madsim::task::spawn_local(operation(
            bucket.clone(),
            2,
            1 + rng.below(20),
            rng.below(3),
            mutation,
        ));
        // A live-node start obtains its lease, then uses the server's decision.
        // A refused start releases the lease. Check again after restore below.
        madsim::time::sleep(Duration::from_secs(25)).await;
        let during = {
            let b = bucket.borrow();
            start_allowed(false, b.fence)
                && marker_matches(Some(&b.marker.to_le_bytes()), Some(&local.to_le_bytes()))
        };
        assert!(!during, "node served while fenced, seed {seed}");
        let results = (first.await.unwrap(), second.await.unwrap());
        upload.await.unwrap();
        let b = bucket.borrow();
        if b.overlap {
            return Err(Violation::ConcurrentWriter);
        }
        assert_eq!(
            results,
            (true, false),
            "competing operation was not rejected"
        );
        assert_eq!(b.rejected, 1);
        assert!(b.retries > 0, "fault injection was not exercised");
        let plan = plan_restore(&Listing::from_keys(b.keys.iter().map(String::as_str))).unwrap();
        if plan.head()
            != (SegmentId {
                generation: 1,
                txid: 1,
            })
        {
            return Err(Violation::LateHistory);
        }
        if start_allowed(false, b.fence)
            && marker_matches(Some(&b.marker.to_le_bytes()), Some(&local.to_le_bytes()))
        {
            return Err(Violation::StaleDisk);
        }
        Ok(())
    })
}
#[test]
fn seeded_late_uploads_two_restores_and_operator_start_gaps() {
    let seeds = std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200);
    for seed in 0..seeds {
        assert_eq!(timed(seed, Mutation::None), Ok(()), "seed {seed}");
        for (mutation, reason) in [
            (Mutation::NoQuiet, Violation::LateHistory),
            (Mutation::BeyondDeadline, Violation::LateHistory),
            (Mutation::NoMarker, Violation::StaleDisk),
            (Mutation::NoLock, Violation::ConcurrentWriter),
        ] {
            assert_eq!(
                timed(seed, mutation),
                Err(reason),
                "{mutation:?} must fail for {reason:?}, seed {seed}"
            );
        }
    }
}
