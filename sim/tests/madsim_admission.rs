//! Seeded admission and delayed resource release using the production guard.
//! `RUSTFLAGS="--cfg madsim" cargo test -p wasmkeep-sim --test madsim_admission --target-dir target/madsim`

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use madsim::runtime::Runtime;
use madsim::time::sleep;
use wasmkeep_sim::rng::Rng;

// The type is public in its production crate, private to this test binary.
#[allow(unreachable_pub)]
#[path = "../../crates/wasmkeep-runtime/src/residency.rs"]
mod residency;
use residency::Residency;

#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    admitted: usize,
    refused: usize,
    peak: usize,
    released: usize,
}

fn simulate(seed: u64) -> Outcome {
    let runtime = Runtime::with_seed_and_config(seed, madsim::Config::default());
    runtime.block_on(async move {
        let count = Arc::new(AtomicUsize::new(0));
        let resources = Arc::new(AtomicUsize::new(0));
        let admitted = Arc::new(AtomicUsize::new(0));
        let refused = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(AtomicUsize::new(0));
        let notifications = Arc::clone(&released);
        let callback: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            notifications.fetch_add(1, Ordering::SeqCst);
        });
        let cap = 3;
        let mut submitters = Vec::new();
        for actor in 0..12 {
            let count = Arc::clone(&count);
            let resources = Arc::clone(&resources);
            let admitted = Arc::clone(&admitted);
            let refused = Arc::clone(&refused);
            let peak = Arc::clone(&peak);
            let callback = Arc::clone(&callback);
            submitters.push(madsim::task::spawn(async move {
                let mut rng = Rng::new(seed ^ (actor << 32));
                let mut workers = Vec::new();
                for _ in 0..40 {
                    sleep(Duration::from_millis(rng.below(8))).await;
                    let Some(task) =
                        Residency::try_acquire(&count, cap, Some(Arc::clone(&callback)))
                    else {
                        refused.fetch_add(1, Ordering::SeqCst);
                        continue;
                    };
                    admitted.fetch_add(1, Ordering::SeqCst);
                    let live = resources.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(live, Ordering::SeqCst);
                    assert!(
                        live <= cap,
                        "seed {seed}: admitted {live} resources for cap {cap}"
                    );
                    assert!(count.load(Ordering::SeqCst) <= cap);
                    let worker = task.clone();
                    // The actor task goes away, but its blocking operation
                    // retains the DB and the same reservation for longer.
                    drop(task);
                    let resources = Arc::clone(&resources);
                    let count = Arc::clone(&count);
                    let delay = Duration::from_millis(10 + rng.below(30));
                    workers.push(madsim::task::spawn(async move {
                        sleep(delay).await;
                        assert!(count.load(Ordering::SeqCst) >= resources.load(Ordering::SeqCst));
                        resources.fetch_sub(1, Ordering::SeqCst);
                        drop(worker);
                    }));
                }
                for worker in workers {
                    worker.await.unwrap();
                }
            }));
        }
        for submitter in submitters {
            submitter.await.unwrap();
        }
        assert_eq!(resources.load(Ordering::SeqCst), 0);
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "seed {seed}: leaked reservation"
        );
        assert_eq!(
            released.load(Ordering::SeqCst),
            admitted.load(Ordering::SeqCst),
            "seed {seed}: final-release notification was missed or duplicated"
        );
        Outcome {
            admitted: admitted.load(Ordering::SeqCst),
            refused: refused.load(Ordering::SeqCst),
            peak: peak.load(Ordering::SeqCst),
            released: released.load(Ordering::SeqCst),
        }
    })
}

#[test]
fn admission_stays_bounded_until_detached_resources_close() {
    let seeds = std::env::var("WASMKEEP_SIM_SEEDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(200);
    assert!(
        seeds > 0,
        "the admission simulation needs at least one seed"
    );
    for seed in 0..seeds {
        let outcome = simulate(seed);
        assert!(outcome.admitted > 3, "seed {seed}: slots were never reused");
        assert!(
            outcome.refused > 0,
            "seed {seed}: capacity was never exercised"
        );
        assert_eq!(
            outcome.peak, 3,
            "seed {seed}: did not fill the configured capacity"
        );
    }
}

#[test]
fn an_admission_schedule_replays_with_the_same_seed() {
    assert_eq!(simulate(73), simulate(73));
}
