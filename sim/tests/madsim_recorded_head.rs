//! ADR 0035, section 2, under madsim: the hold cell as the home of a
//! hold's acknowledged head (`wasmkeep_cluster::hold_cell`, the server's
//! own functions). One hold, one uploader that sends segments one after
//! another and acknowledges each when its PUT completes (a seeded delay),
//! and the end of the hold at a seeded moment: a lapse (the epoch moves,
//! then `end_all` freezes the cell) or a freeze with the lease still valid
//! (what a drop, a park, an explicit release and a clean stop's timeout
//! do). Periodic snapshots are checked
//! between segments and must never move the head.
//!
//! Part of simulation case 4 of the ADR (drop, park, release and a clean
//! stop's timeout all freeze through the one `end` call; batched
//! completion and failed flushes are not modelled here): the frozen head includes every
//! acknowledgement that succeeded, no acknowledgement succeeds after the
//! end, and an acknowledgement whose validity check came before a lapse
//! is either in the frozen head or refused (never a successful reply with
//! a head the release does not carry). Every seed must race the end
//! against an upload in flight, so a pass is not vacuous.
//!
//! Broken variant: acknowledging outside the cell (check validity, await,
//! then advance a copy the end does not see) must be caught.
//!
//! Not covered here (see the ADR's remaining cases): the per-key registry
//! of frozen heads an acquisition combines, the demotion transition, and
//! every case that needs the recorded head written to the bucket.
//!
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use madsim::runtime::Runtime;
use madsim::time::sleep;
use wasmkeep_cluster::hold_cell::{self, HoldHead, Late};
use wasmkeep_cluster::lease::Version;
use wasmkeep_cluster::node_lease::Hold;
use wasmkeep_cluster::recorded_head::Position;
use wasmkeep_sim::rng::Rng;

fn seeds() -> u64 {
    std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}

fn at(txid: u64) -> Position {
    Position::Known {
        generation: 7,
        txid,
        checksum: txid.wrapping_mul(0x9e37_79b9_7f4a_7c15),
    }
}

/// How a hold ends in one run.
#[derive(Debug, Clone, Copy)]
enum Ending {
    /// The node lease ran out: the epoch moves first, then `end_all`.
    Lapse,
    /// Dropped (hibernation), parked, released or a clean stop's timeout:
    /// the cell is frozen with the lease still valid.
    Freeze,
}

#[derive(Debug, Default)]
struct Outcome {
    /// The frozen head.
    frozen: Option<Position>,
    /// The last acknowledgement that succeeded, as the uploader saw it.
    last_ok: Option<Position>,
    /// Acknowledgements that succeeded after the end.
    after_end: u64,
    /// Whether the end met an upload in flight.
    raced: bool,
    /// Whether some periodic snapshot check moved the head.
    snapshot_moved: bool,
}

fn simulate(seed: u64, outside_the_cell: bool) -> Outcome {
    let rt = Runtime::with_seed_and_config(seed, madsim::Config::default());
    rt.block_on(async move {
        let mut rng = Rng::new(seed ^ 0x0035);
        let cell = Arc::new(Mutex::new(Some(Hold {
            key: "owners/t/c/a".into(),
            generation: 7,
            version: Version("v1".into()),
            previous: None,
            head: HoldHead::default(),
        })));
        // The node's validity in this hold's epoch.
        let valid = Arc::new(AtomicBool::new(true));
        // The broken variant's copy of the head, outside the cell.
        let outside: Arc<Mutex<Option<Position>>> = Arc::new(Mutex::new(Some(Position::Empty)));
        hold_cell::verify(&cell, || true, Position::Empty).unwrap();

        let in_flight = Arc::new(AtomicBool::new(false));
        let ended = Arc::new(AtomicBool::new(false));
        let after_end = Arc::new(AtomicU64::new(0));
        let snapshot_moved = Arc::new(AtomicBool::new(false));
        let uploader = {
            let (cell, valid, outside) =
                (Arc::clone(&cell), Arc::clone(&valid), Arc::clone(&outside));
            let (in_flight, ended, after_end) = (
                Arc::clone(&in_flight),
                Arc::clone(&ended),
                Arc::clone(&after_end),
            );
            let snapshot_moved = Arc::clone(&snapshot_moved);
            let mut rng = Rng::new(seed ^ 0xacc);
            madsim::task::spawn(async move {
                let mut last_ok = None;
                for txid in 1..=40 {
                    in_flight.store(true, Ordering::SeqCst);
                    // The PUT, then its completion.
                    sleep(Duration::from_millis(1 + rng.below(30))).await;
                    let acked = if outside_the_cell {
                        // Broken: validity checked, then an await, then the
                        // head advanced where the end does not look.
                        let ok = valid.load(Ordering::SeqCst) && cell.lock().unwrap().is_some();
                        sleep(Duration::from_millis(1 + rng.below(10))).await;
                        if ok {
                            *outside.lock().unwrap() = Some(at(txid));
                            Ok(())
                        } else {
                            Err(Late)
                        }
                    } else {
                        let valid = Arc::clone(&valid);
                        hold_cell::acknowledge(
                            &cell,
                            move || valid.load(Ordering::SeqCst),
                            Some(at(txid)),
                        )
                    };
                    in_flight.store(false, Ordering::SeqCst);
                    match acked {
                        Ok(()) => {
                            if ended.load(Ordering::SeqCst) {
                                after_end.fetch_add(1, Ordering::SeqCst);
                            }
                            last_ok = Some(at(txid));
                        }
                        Err(Late) => break,
                    }
                    // Now and then a periodic snapshot: a check only.
                    if rng.below(4) == 0 {
                        let before = cell.lock().unwrap().as_ref().map(|h| h.head);
                        let v = Arc::clone(&valid);
                        let _ =
                            hold_cell::acknowledge(&cell, move || v.load(Ordering::SeqCst), None);
                        let after = cell.lock().unwrap().as_ref().map(|h| h.head);
                        if before.is_some() && after.is_some() && before != after {
                            snapshot_moved.store(true, Ordering::SeqCst);
                        }
                    }
                }
                last_ok
            })
        };

        // The end, at a seeded moment.
        sleep(Duration::from_millis(5 + rng.below(400))).await;
        let ending = if rng.below(2) == 0 {
            Ending::Lapse
        } else {
            Ending::Freeze
        };
        let raced = in_flight.load(Ordering::SeqCst);
        if matches!(ending, Ending::Lapse) {
            valid.store(false, Ordering::SeqCst);
            // The lease task's `end_all` runs after the epoch moved; an
            // acknowledgement may be between its check and its advance.
            sleep(Duration::from_millis(rng.below(3))).await;
        }
        let frozen = hold_cell::end(&cell).unwrap();
        // The broken variant's release captures its outside copy at the
        // same moment: only an acknowledgement that succeeds after that
        // capture (its check before, its advance after) is missed.
        let frozen = if outside_the_cell {
            *outside.lock().unwrap()
        } else {
            frozen.head.acknowledged
        };
        ended.store(true, Ordering::SeqCst);
        let last_ok = uploader.await.unwrap();
        Outcome {
            frozen,
            last_ok,
            after_end: after_end.load(Ordering::SeqCst),
            raced,
            snapshot_moved: snapshot_moved.load(Ordering::SeqCst),
        }
    })
}

#[test]
fn the_frozen_head_covers_every_acknowledgement() {
    let mut raced = 0;
    for seed in 0..seeds() {
        let o = simulate(seed, false);
        assert_eq!(o.after_end, 0, "seed {seed}: acknowledged after the end");
        assert_eq!(
            o.frozen,
            Some(o.last_ok.unwrap_or(Position::Empty)),
            "seed {seed}: the frozen head is not the last acknowledgement"
        );
        assert!(
            !o.snapshot_moved,
            "seed {seed}: a snapshot check moved the head"
        );
        raced += u64::from(o.raced);
    }
    eprintln!("{} seeds: {raced} ends met an upload in flight", seeds());
    let floor = (seeds() / 4).max(1);
    assert!(
        raced >= floor,
        "{raced} ends met an upload in flight, want {floor}"
    );
}

#[test]
fn acknowledging_outside_the_cell_is_caught() {
    let caught = (0..seeds())
        .filter(|&seed| {
            let o = simulate(seed, true);
            o.after_end > 0 || o.frozen != Some(o.last_ok.unwrap_or(Position::Empty))
        })
        .count() as u64;
    eprintln!(
        "{} seeds: acknowledging outside the cell caught on {caught}",
        seeds()
    );
    let floor = (seeds() / 10).max(1);
    assert!(caught >= floor, "caught on {caught} seeds, want {floor}");
}
