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

// The timed model: shared bucket state, an old holder of the app that pauses
// with an upload in flight and resumes after the restore started, and two
// restores in a row. Its waits are the CLI's own decisions
// (`quiet_until_ms`, `release_at_ms`), the holder's validity its node lease's
// (`written_at + ttl - safety` on its own clock).
use std::{cell::RefCell, collections::BTreeSet, rc::Rc, time::Duration};
use wasmkeep_cluster::lease::LeaseConfig;
use wasmkeep_cluster::replication::{Listing, SegmentId, plan_restore, snapshot_key};
use wasmkeep_cluster::restore_fence::{quiet_until_ms, release_at_ms};
use wasmkeep_sim::rng::Rng;

/// `LATE_WRITES`: a bucket client's request timeout (30 s) plus a lease's ttl.
const LATE: u64 = 40_000;
/// How long a request may take before the client gives up (and it may land).
const REQUEST: u64 = 30_000;
/// The second restore's copy: long enough for a late request to land in it.
const COPY: u64 = 45_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mutation {
    None,
    /// The quiet wait ignores the app's node leases (counts from nothing).
    PrematureQuiet,
    /// An upload lands after the bucket client's request timeout.
    BeyondDeadline,
    /// The paused holder writes on resuming without asking its lease.
    ResumedWrites,
    /// The lock is released at once although a request got no answer.
    PrematureRelease,
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
    fence: Option<u64>,
    lock: bool,
    marker: u64,
    keys: BTreeSet<String>,
    /// The old holder's node lease: `written_at` of its last live write.
    heartbeat: Option<u64>,
    cleared: bool,
    active: usize,
    overlap: bool,
    violation: Option<Violation>,
    retries: usize,
}
async fn until(t: u64) {
    let n = clock();
    if t > n {
        madsim::time::sleep(Duration::from_millis(t - n)).await;
    }
}
thread_local!(static START: RefCell<Option<madsim::time::Instant>> = const { RefCell::new(None) });
fn clock() -> u64 {
    START.with(|s| {
        let start = *s
            .borrow_mut()
            .get_or_insert_with(madsim::time::Instant::now);
        u64::try_from(start.elapsed().as_millis()).unwrap()
    })
}
/// One restore: lock, fence and marker, quiet wait, clear, copy, open, settle.
/// `unknown`: the fence deletion's first attempt gets no answer and lands
/// `unknown` ms after the client gave up. Returns its time under the lock.
#[allow(clippy::too_many_arguments)]
async fn restore(
    bucket: Rc<RefCell<Bucket>>,
    id: u64,
    start: u64,
    copy: u64,
    faults: u64,
    unknown: Option<u64>,
    mutation: Mutation,
) -> u64 {
    until(start).await;
    loop {
        {
            let mut b = bucket.borrow_mut();
            if !b.lock || mutation == Mutation::NoLock {
                b.lock = true;
                b.active += 1;
                b.overlap |= b.active > 1;
                b.fence = Some(id);
                if mutation != Mutation::NoMarker {
                    b.marker += 1;
                }
                break;
            }
        }
        madsim::time::sleep(Duration::from_millis(500)).await;
    }
    let locked = clock();
    // Listed after the fence landed; the listing is complete in this model.
    let last = if mutation == Mutation::PrematureQuiet {
        None
    } else {
        bucket.borrow().heartbeat
    };
    until(quiet_until_ms(locked, last, true, LATE)).await;
    {
        let mut b = bucket.borrow_mut();
        b.keys.clear();
        b.cleared = true;
    }
    for _ in 0..faults {
        bucket.borrow_mut().retries += 1;
        madsim::time::sleep(Duration::from_secs(1)).await;
    }
    bucket.borrow_mut().keys.insert(snapshot_key(SegmentId {
        generation: id,
        txid: 1,
    }));
    madsim::time::sleep(Duration::from_millis(copy)).await;
    // Opening: the fence must still be this restore's own.
    let mut unsettled = None;
    {
        let mut b = bucket.borrow_mut();
        if b.fence != Some(id) {
            b.violation.get_or_insert(Violation::PartialState);
        }
    }
    if let Some(lands) = unknown {
        // The first attempt: sent, no answer until the client gives up.
        madsim::time::sleep(Duration::from_millis(REQUEST)).await;
        let ended = clock();
        unsettled = Some(ended);
        let late = bucket.clone();
        madsim::task::spawn_local(async move {
            madsim::time::sleep(Duration::from_millis(lands)).await;
            late.borrow_mut().fence = None;
        });
    }
    // The retry (or the only attempt) is answered.
    bucket.borrow_mut().fence = None;
    let release = if mutation == Mutation::PrematureRelease {
        clock()
    } else {
        release_at_ms(clock(), unsettled, LATE)
    };
    until(release).await;
    let mut b = bucket.borrow_mut();
    b.active -= 1;
    b.lock = false;
    clock() - locked
}
/// The old holder: one live write of its node lease, an upload sent while
/// valid that lands late, then a pause until after the restore started. On
/// resuming it asks its lease (its own clock) before writing.
async fn old_holder(bucket: Rc<RefCell<Bucket>>, rng: &mut Rng, resume: u64, mutation: Mutation) {
    let lease = LeaseConfig::default_safe();
    let skew = rng.below(2 * lease.max_skew_ms() + 1);
    // Its clock reads `real + skew - max_skew`.
    let own = move |real: u64| (real + skew).saturating_sub(lease.max_skew_ms());
    let written_at = own(1_000);
    bucket.borrow_mut().heartbeat = Some(written_at);
    let valid = move |real: u64| own(real) < written_at + lease.ttl_ms() - lease.safety_ms();
    let send = 5_000 + rng.below(4_000);
    let deadline = if mutation == Mutation::BeyondDeadline {
        LATE + 1_000 + rng.below(20_000)
    } else {
        20_000 + rng.below(REQUEST - 20_000)
    };
    until(send).await;
    assert!(valid(clock()), "the model sends only while valid");
    let late = bucket.clone();
    madsim::task::spawn_local(async move {
        madsim::time::sleep(Duration::from_millis(deadline)).await;
        let mut b = late.borrow_mut();
        if b.cleared {
            b.violation.get_or_insert(Violation::LateHistory);
        }
        b.keys.insert(snapshot_key(SegmentId {
            generation: 7,
            txid: 3,
        }));
    });
    // Paused (a stopped process, a long GC, a frozen VM) until `resume`.
    until(resume).await;
    if valid(clock()) || mutation == Mutation::ResumedWrites {
        let mut b = bucket.borrow_mut();
        if b.cleared {
            b.violation.get_or_insert(Violation::LateHistory);
        }
        b.keys.insert(snapshot_key(SegmentId {
            generation: 7,
            txid: 4,
        }));
    }
}
/// `holder`: the app had a node that stopped moments before the restore.
/// `unknown`: the first restore's fence deletion got no answer.
fn timed(seed: u64, holder: bool, unknown: bool, mutation: Mutation) -> Result<u64, Violation> {
    let rt = madsim::runtime::Runtime::with_seed_and_config(seed, madsim::Config::default());
    rt.block_on(async move {
        START.with(|s| *s.borrow_mut() = Some(madsim::time::Instant::now()));
        let bucket = Rc::new(RefCell::new(Bucket::default()));
        let mut rng = Rng::new(seed);
        let first_at = 15_000 + rng.below(5_000);
        let second_at = first_at + 1_000 + rng.below(19_000);
        let resume = first_at + 30_000 + rng.below(30_000);
        let lands = 1_000 + rng.below(LATE - 2_000);
        let (faults1, faults2) = (1 + rng.below(3), rng.below(3));
        let local = bucket.borrow().marker; // a node prepared before either restore
        let holder_task = holder.then(|| {
            let b = bucket.clone();
            let mut r = Rng::new(seed ^ 0x5eed);
            madsim::task::spawn_local(async move { old_holder(b, &mut r, resume, mutation).await })
        });
        let first = madsim::task::spawn_local(restore(
            bucket.clone(),
            1,
            first_at,
            0,
            faults1,
            unknown.then_some(lands),
            mutation,
        ));
        let second = madsim::task::spawn_local(restore(
            bucket.clone(),
            2,
            second_at,
            COPY,
            faults2,
            None,
            mutation,
        ));
        // A node start in the middle of the first restore is refused.
        until(first_at + 1).await;
        let during = {
            let b = bucket.borrow();
            start_allowed(false, b.fence.is_some())
                && marker_matches(Some(&b.marker.to_le_bytes()), Some(&local.to_le_bytes()))
        };
        assert!(!during, "node served while fenced, seed {seed}");
        let held = first.await.unwrap();
        second.await.unwrap();
        if let Some(task) = holder_task {
            task.await.unwrap();
        }
        // Let every late request land.
        madsim::time::sleep(Duration::from_millis(2 * LATE)).await;
        let b = bucket.borrow();
        if b.overlap {
            return Err(Violation::ConcurrentWriter);
        }
        if let Some(v) = b.violation {
            return Err(v);
        }
        assert!(b.retries > 0, "fault injection was not exercised");
        let plan = plan_restore(&Listing::from_keys(b.keys.iter().map(String::as_str))).unwrap();
        if plan.head()
            != (SegmentId {
                generation: 2,
                txid: 1,
            })
        {
            return Err(Violation::LateHistory);
        }
        if start_allowed(false, b.fence.is_some())
            && marker_matches(Some(&b.marker.to_le_bytes()), Some(&local.to_le_bytes()))
        {
            return Err(Violation::StaleDisk);
        }
        Ok(held)
    })
}
#[test]
fn seeded_paused_old_holder_and_unanswered_requests_across_two_restores() {
    let seeds: u64 = std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200);
    for seed in 0..seeds {
        let mut rng = Rng::new(seed.wrapping_mul(0x9e37_79b9));
        let (holder, unknown) = (rng.chance(50), rng.chance(50));
        let held = timed(seed, holder, unknown, Mutation::None)
            .unwrap_or_else(|v| panic!("seed {seed} ({holder}, {unknown}): {v:?}"));
        // With nothing that may still land, the restore waits for nothing:
        // its time under the lock is its own work (the injected faults).
        if !holder && !unknown {
            assert!(held < 5_000, "seed {seed}: held {held} ms");
        }
        if unknown {
            assert!(
                held >= REQUEST + LATE,
                "seed {seed}: released after {held} ms"
            );
        }
        for (mutation, holder, unknown, reason) in [
            (
                Mutation::PrematureQuiet,
                true,
                unknown,
                Violation::LateHistory,
            ),
            (
                Mutation::BeyondDeadline,
                true,
                unknown,
                Violation::LateHistory,
            ),
            (
                Mutation::ResumedWrites,
                true,
                unknown,
                Violation::LateHistory,
            ),
            (
                Mutation::PrematureRelease,
                holder,
                true,
                Violation::PartialState,
            ),
            (Mutation::NoMarker, holder, unknown, Violation::StaleDisk),
            (Mutation::NoLock, true, unknown, Violation::ConcurrentWriter),
        ] {
            assert_eq!(
                timed(seed, holder, unknown, mutation),
                Err(reason),
                "{mutation:?} must fail for {reason:?}, seed {seed}"
            );
        }
    }
}
