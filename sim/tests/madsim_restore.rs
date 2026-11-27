//! Bounded interleaving model for ADR 0033's app-start fence. Explore every
//! interleaving of a node starting (including stale disk) and a restore.
//! The CLI is a binary using real Tokio timers and synchronous
//! SQLite replay; it cannot run on madsim's executor/virtual clock as wired.
//! A direct tokio::time::sleep probe panics with "there is no reactor running".
//! Instead this model calls the same fence decisions as the server. The CLI
//! fault tests exercise the full executor on Tokio; this is not executor coverage.
//!
//! The generation floor a restore leaves (ADR 0045) is modelled in
//! `support/generation_floor.rs`: up to two restores in place of an actor
//! that lived, before and after the finalize, each followed by the actor
//! deleting itself and being recreated, with markers left (before 0.10) or
//! none (at 0.10), and a cleanup. A restore that forgets the generation
//! floor it clears is caught.
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

// The timed model: shared bucket state stamped with the bucket's clock (the
// true one, at whole seconds, as S3's Last-Modified), old holders of the app
// whose clocks are off by up to a minute, and two restores in a row. The
// CLI's clock is the true one. Its waits are the CLI's own decisions
// (`quiet_wait_ms`, `release_at_ms`); a holder's validity is its node
// lease's (`written_at + ttl - safety` on its own clock).
use std::{cell::RefCell, collections::BTreeSet, rc::Rc, time::Duration};
use wasmkeep_cluster::lease::LeaseConfig;
use wasmkeep_cluster::replication::{Listing, SegmentId, plan_restore, snapshot_key};
use wasmkeep_cluster::restore_fence::{QUIET_ROUNDS, quiet_wait_ms, release_at_ms};
use wasmkeep_sim::rng::Rng;

/// `LATE_WRITES`.
const LATE: u64 = 40_000;
/// How long a request may take before the client gives up (and it may land).
const REQUEST: u64 = 30_000;
/// The second restore's copy: long enough for a late request to land in it.
const COPY: u64 = 45_000;
/// Every time in the model is at least this, so a clock behind stays positive.
const BASE: u64 = 100_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mutation {
    None,
    /// The quiet wait ignores the app's node leases (counts from nothing).
    PrematureQuiet,
    /// The quiet wait compares the node's `written_at` with the CLI's clock.
    NodeClock,
    /// No second look at the leases after the wait.
    NoRecheck,
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
enum Holder {
    /// The app has no node here.
    None,
    /// A node that sent an upload, then froze until after the restore began.
    Paused,
    /// A node that never sees the fence (its deploy checks fail) and keeps
    /// renewing its lease and uploading.
    Serving,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Violation {
    PartialState,
    LateHistory,
    StaleDisk,
    ConcurrentWriter,
}
#[derive(Clone, Copy)]
struct Heartbeat {
    /// On the node's clock.
    written_at: u64,
    /// Last-Modified: the bucket's clock, at whole seconds.
    stored: u64,
}
#[derive(Default)]
struct Bucket {
    fence: Option<u64>,
    lock: bool,
    marker: u64,
    keys: BTreeSet<String>,
    heartbeat: Option<Heartbeat>,
    cleared: bool,
    active: usize,
    overlap: bool,
    violation: Option<Violation>,
    retries: usize,
}
thread_local!(static START: RefCell<Option<madsim::time::Instant>> = const { RefCell::new(None) });
/// The true clock (the CLI's and the bucket's).
fn clock() -> u64 {
    START.with(|s| {
        let start = *s
            .borrow_mut()
            .get_or_insert_with(madsim::time::Instant::now);
        BASE + u64::try_from(start.elapsed().as_millis()).unwrap()
    })
}
/// The bucket's Last-Modified for a write now.
fn stamp() -> u64 {
    clock() / 1_000 * 1_000
}
async fn until(t: u64) {
    let n = clock();
    if t > n {
        madsim::time::sleep(Duration::from_millis(t - n)).await;
    }
}
fn land(bucket: &Rc<RefCell<Bucket>>, generation: u64, txid: u64) {
    let mut b = bucket.borrow_mut();
    if b.cleared {
        b.violation.get_or_insert(Violation::LateHistory);
    }
    b.keys.insert(snapshot_key(SegmentId { generation, txid }));
}
/// The quiet wait, as `backup restore` runs it: listed after the fence,
/// measured from the bucket's stamps, looked at again after each wait.
/// `false`: refused (a node still writes its lease).
async fn quiet(bucket: &Rc<RefCell<Bucket>>, locked: u64, mutation: Mutation) -> bool {
    let fence_stored = locked / 1_000 * 1_000;
    // The probe: a write just before the listing, its Last-Modified.
    let probe = stamp();
    let probed = clock();
    let mut seen = bucket.borrow().heartbeat.map(|h| h.stored);
    for _ in 0..QUIET_ROUNDS {
        let now = clock();
        let wait = match mutation {
            Mutation::PrematureQuiet => 0,
            Mutation::NodeClock => bucket
                .borrow()
                .heartbeat
                .map_or(0, |h| (h.written_at + LATE).saturating_sub(now)),
            _ => quiet_wait_ms(true, seen, fence_stored, probe + (now - probed), LATE),
        };
        madsim::time::sleep(Duration::from_millis(wait)).await;
        let again = bucket.borrow().heartbeat.map(|h| h.stored);
        if again == seen || mutation == Mutation::NoRecheck {
            return true;
        }
        seen = again;
    }
    false
}
/// One restore: lock, fence and marker, quiet wait, clear, copy, open, settle.
/// `unknown`: the fence deletion's first attempt gets no answer and lands
/// `unknown` ms after the client gave up. Returns its time under the lock,
/// and whether it restored (rather than refused).
async fn restore(
    bucket: Rc<RefCell<Bucket>>,
    id: u64,
    start: u64,
    copy: u64,
    faults: u64,
    unknown: Option<u64>,
    mutation: Mutation,
) -> (u64, bool) {
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
    let restored = quiet(&bucket, locked, mutation).await;
    let mut unsettled = None;
    if restored {
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
        {
            let mut b = bucket.borrow_mut();
            if b.fence != Some(id) {
                b.violation.get_or_insert(Violation::PartialState);
            }
        }
        if let Some(lands) = unknown {
            // The first attempt: sent, no answer until the client gives up.
            madsim::time::sleep(Duration::from_millis(REQUEST)).await;
            unsettled = Some(clock());
            let late = bucket.clone();
            madsim::task::spawn_local(async move {
                madsim::time::sleep(Duration::from_millis(lands)).await;
                late.borrow_mut().fence = None;
            });
        }
        // The retry (or the only attempt) is answered.
        bucket.borrow_mut().fence = None;
    }
    let release = if mutation == Mutation::PrematureRelease {
        clock()
    } else {
        release_at_ms(clock(), unsettled, LATE)
    };
    until(release).await;
    let mut b = bucket.borrow_mut();
    b.active -= 1;
    b.lock = false;
    (clock() - locked, restored)
}
/// A paused holder: one live write of its node lease, an upload sent while
/// valid that lands late, then a pause until after the restore started. On
/// resuming it asks its lease (its own clock) before writing.
async fn paused(bucket: Rc<RefCell<Bucket>>, rng: &mut Rng, resume: u64, mutation: Mutation) {
    let lease = LeaseConfig::default_safe();
    // Its clock is off by up to a minute either way; 45 s behind for the
    // mutant comparing it with the CLI's.
    let offset = if mutation == Mutation::NodeClock {
        -45_000
    } else {
        i64::try_from(rng.below(120_001)).unwrap() - 60_000
    };
    let own = move |real: u64| real.checked_add_signed(offset).unwrap();
    let written_at = own(BASE + 1_000);
    until(BASE + 1_000).await;
    bucket.borrow_mut().heartbeat = Some(Heartbeat {
        written_at,
        stored: stamp(),
    });
    let valid = move |real: u64| own(real) < written_at + lease.ttl_ms() - lease.safety_ms();
    let send = BASE + 5_000 + rng.below(4_000);
    let deadline = if mutation == Mutation::BeyondDeadline {
        LATE + 2_000 + rng.below(20_000)
    } else {
        20_000 + rng.below(REQUEST - 20_000)
    };
    until(send).await;
    assert!(valid(clock()), "the model sends only while valid");
    let late = bucket.clone();
    madsim::task::spawn_local(async move {
        madsim::time::sleep(Duration::from_millis(deadline)).await;
        land(&late, 7, 3);
    });
    // Paused (a stopped process, a long GC, a frozen VM) until `resume`.
    until(resume).await;
    if valid(clock()) || mutation == Mutation::ResumedWrites {
        land(&bucket, 7, 4);
    }
}
/// A serving holder: renews its node lease every 3 s and uploads every 2 s
/// (each landing within the request timeout) until `stop`, never seeing the
/// fence. Its clock is a minute behind: by the CLI's its lease looks stale.
async fn serving(bucket: Rc<RefCell<Bucket>>, rng: &mut Rng, stop: u64) {
    let mut next_renewal = BASE;
    let mut txid = 10;
    while clock() < stop {
        if clock() >= next_renewal {
            bucket.borrow_mut().heartbeat = Some(Heartbeat {
                written_at: clock() - 60_000,
                stored: stamp(),
            });
            next_renewal += 3_000;
        }
        let late = bucket.clone();
        let lands = rng.below(REQUEST);
        txid += 1;
        madsim::task::spawn_local(async move {
            madsim::time::sleep(Duration::from_millis(lands)).await;
            land(&late, 8, txid);
        });
        madsim::time::sleep(Duration::from_secs(1)).await;
    }
}
struct Outcome {
    /// The first restore's time under the lock.
    held: u64,
    /// Whether the restores restored, rather than refused.
    restored: (bool, bool),
}
/// `holder`: what old node of the app there is. `unknown`: the first
/// restore's fence deletion got no answer.
fn timed(
    seed: u64,
    holder: Holder,
    unknown: bool,
    mutation: Mutation,
) -> Result<Outcome, Violation> {
    let rt = madsim::runtime::Runtime::with_seed_and_config(seed, madsim::Config::default());
    rt.block_on(async move {
        START.with(|s| *s.borrow_mut() = Some(madsim::time::Instant::now()));
        let bucket = Rc::new(RefCell::new(Bucket::default()));
        let mut rng = Rng::new(seed);
        let first_at = BASE + 15_000 + rng.below(5_000);
        let second_at = first_at + 1_000 + rng.below(19_000);
        let resume = first_at + 30_000 + rng.below(30_000);
        let lands = 1_000 + rng.below(LATE - 2_000);
        let (faults1, faults2) = (1 + rng.below(3), rng.below(3));
        let local = bucket.borrow().marker; // a node prepared before either restore
        let b = bucket.clone();
        let mut r = Rng::new(seed ^ 0x5eed);
        let holder_task = madsim::task::spawn_local(async move {
            match holder {
                Holder::None => {}
                Holder::Paused => paused(b, &mut r, resume, mutation).await,
                Holder::Serving => serving(b, &mut r, BASE + 400_000).await,
            }
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
        let (held, restored_first) = first.await.unwrap();
        let (_, restored_second) = second.await.unwrap();
        holder_task.await.unwrap();
        // Let every late request land.
        madsim::time::sleep(Duration::from_millis(2 * LATE)).await;
        let b = bucket.borrow();
        if b.overlap {
            return Err(Violation::ConcurrentWriter);
        }
        if let Some(v) = b.violation {
            return Err(v);
        }
        if restored_second {
            assert!(b.retries > 0, "fault injection was not exercised");
            let plan =
                plan_restore(&Listing::from_keys(b.keys.iter().map(String::as_str))).unwrap();
            if plan.head()
                != (SegmentId {
                    generation: 2,
                    txid: 1,
                })
            {
                return Err(Violation::LateHistory);
            }
        }
        if start_allowed(false, b.fence.is_some())
            && marker_matches(Some(&b.marker.to_le_bytes()), Some(&local.to_le_bytes()))
        {
            return Err(Violation::StaleDisk);
        }
        Ok(Outcome {
            held,
            restored: (restored_first, restored_second),
        })
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
        let holder = if rng.chance(50) {
            Holder::Paused
        } else {
            Holder::None
        };
        let unknown = rng.chance(50);
        let outcome = timed(seed, holder, unknown, Mutation::None)
            .unwrap_or_else(|v| panic!("seed {seed} ({holder:?}, {unknown}): {v:?}"));
        assert_eq!(outcome.restored, (true, true), "seed {seed}");
        // With nothing that may still land, the restore waits for nothing:
        // its time under the lock is its own work (the injected faults).
        if holder == Holder::None && !unknown {
            assert!(
                outcome.held < 5_000,
                "seed {seed}: held {} ms",
                outcome.held
            );
        }
        if unknown {
            assert!(
                outcome.held >= REQUEST + LATE,
                "seed {seed}: released after {} ms",
                outcome.held
            );
        }
        // A node that keeps acting is never restored over.
        let outcome = timed(seed, Holder::Serving, unknown, Mutation::None)
            .unwrap_or_else(|v| panic!("seed {seed} (serving): {v:?}"));
        assert_eq!(outcome.restored, (false, false), "seed {seed}");
        for (mutation, holder, unknown, reason) in [
            (
                Mutation::PrematureQuiet,
                Holder::Paused,
                unknown,
                Violation::LateHistory,
            ),
            (
                Mutation::NodeClock,
                Holder::Paused,
                unknown,
                Violation::LateHistory,
            ),
            (
                Mutation::NoRecheck,
                Holder::Serving,
                unknown,
                Violation::LateHistory,
            ),
            (
                Mutation::BeyondDeadline,
                Holder::Paused,
                unknown,
                Violation::LateHistory,
            ),
            (
                Mutation::ResumedWrites,
                Holder::Paused,
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
            (
                Mutation::NoLock,
                Holder::Paused,
                unknown,
                Violation::ConcurrentWriter,
            ),
        ] {
            assert_eq!(
                timed(seed, holder, unknown, mutation).map(|o| o.restored),
                Err(reason),
                "{mutation:?} must fail for {reason:?}, seed {seed}"
            );
        }
    }
}

#[path = "support/generation_floor.rs"]
mod generation_floor_model;

/// An actor that lived to generation 5 (the backup's plan at 3), on a node
/// of 0.10 that acquires it, deletes it and acquires it again; two
/// restores in place may run at any quiet point, and a cleanup once the
/// floor is 0.10. `finalized`: the app starts at the floor 0.10 (no
/// marker is ever written), or below it (the restore writes markers).
fn lived(finalized: bool) -> generation_floor_model::Start {
    use generation_floor_model::{Start, Task};
    Start {
        finalized,
        floor: (0, false),
        actors: vec![((!finalized).then_some(0), Some(5), 5, 5, 3)],
        nodes: vec![vec![
            Task::Acquire(0),
            Task::Delete(0),
            Task::Acquire(0),
            Task::Acquire(0),
        ]],
        restores: 2,
        clean: true,
    }
}

/// ADR 0045, exit criterion: after a whole-app restore in place, an actor
/// deleted and recreated starts above every generation its deleted
/// history carried, with leftover markers and without.
#[test]
fn delete_and_recreate_after_a_restore_keeps_the_generation_above_its_history() {
    use generation_floor_model::{Mutation, explore};
    for finalized in [false, true] {
        let reached = explore(&lived(finalized), Mutation::None)
            .unwrap_or_else(|reused| panic!("finalized {finalized}: {reused:?}"));
        assert!(reached.restored > 0 && reached.recreated > 0, "{reached:?}");
        assert!(reached.from_floor > 0 || !finalized, "{reached:?}");
        eprintln!("finalized {finalized}: {reached:?}");
    }
}

/// A second restore in place with no acquisition between, that does not
/// read the first one's generation floor, reuses a generation.
#[test]
fn a_restore_that_forgets_the_generation_floor_is_caught() {
    use generation_floor_model::{Mutation, explore};
    assert!(explore(&lived(true), Mutation::RestoreForgetsFloor).is_err());
}
