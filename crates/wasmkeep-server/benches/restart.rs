//! How restarting a node scales with the number of actors on its disk.
//! At start-up a node scans every local actor
//! database for a pending alarm and re-arms what it finds; the scan runs on
//! a blocking thread, beside the listener. One standalone node (no
//! cluster) holds N test actors, each created by a request that set an
//! alarm, so each database is as an actor leaves it (its own tables, the
//! host's meta and alarm tables, WAL mode). Then, per sample, the node is
//! stopped as a crash stops it and started again on the same directory.
//!
//! - `restart/scan/N`: the scan alone (`Storage::pending_alarms`), every
//!   database holding an alarm an hour out: the time until every alarm is
//!   armed again after a restart
//! - `restart/serving/N`: from starting the node (runtime, component from
//!   the on-disk cache, service) to the first 200 from one of its actors,
//!   with the scan running beside it
//! - `restart/fired/N`: every actor's alarm overdue at the restart (set
//!   into the past while the node is down, untimed): from starting the node
//!   until every alarm has run, counted on the metrics page
//!
//! Data lives in the system's temporary directory (tmpfs on the recording
//! workstation): every file is in memory, as after a restart that finds the
//! page cache warm; a cold disk is not measured. Run `bench/run.sh` to
//! record results as CSV.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // bench code

use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, SamplingMode, criterion_group, criterion_main};
use wasmkeep_server::ActorConfig;
use wasmkeep_storage::{Storage, StorageConfig};

#[path = "support/scale.rs"]
mod scale;

use scale::{Client, Node, alarms_fired, now_ms, post_all};

/// Local actor counts.
const SIZES: [usize; 3] = [1_000, 10_000, 100_000];
/// Whose alarms are made overdue for `restart/fired`: the larger sizes
/// take minutes per sample.
const FIRED_SIZES: [usize; 2] = [1_000, 10_000];

fn config(data: &std::path::Path) -> ActorConfig {
    ActorConfig::new(data)
}

/// Sets every actor's alarm to `at`, the node being down.
fn set_alarms(data: &std::path::Path, n: usize, at: u64) {
    let storage = Storage::new(
        StorageConfig::new(data),
        wasmkeep_server::state_dir(&wasmkeep_server::StateRoot::new("", "default")),
    );
    std::thread::scope(|s| {
        let threads = 16;
        for t in 0..threads {
            let storage = &storage;
            s.spawn(move || {
                for i in (t..n).step_by(threads) {
                    let mut db = storage.open("t", &format!("a{i}")).unwrap();
                    db.set_alarm(at).unwrap();
                }
            });
        }
    });
}

/// Starts the node, timing until one of its actors answers.
fn serving(data: &std::path::Path) -> (Node, Duration) {
    let start = Instant::now();
    let node = Node::start(config(data));
    let _ = Client::until_ok(
        node.addr,
        "GET",
        "/actor/t/a0/alarm",
        Duration::from_millis(1),
    );
    (node, start.elapsed())
}

#[allow(clippy::too_many_lines)]
fn benches(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let hour = now_ms() + 3_600_000;
    let mut created = 0;

    let mut group = c.benchmark_group("restart");
    group
        .sample_size(10)
        .sampling_mode(SamplingMode::Flat)
        .warm_up_time(Duration::from_millis(1))
        .measurement_time(Duration::from_secs(1));
    // `WASMKEEP_BENCH_MAX_ACTORS` skips the larger sizes (for a quick run).
    let max = std::env::var("WASMKEEP_BENCH_MAX_ACTORS").map_or(usize::MAX, |v| v.parse().unwrap());
    for n in SIZES.into_iter().filter(|&n| n <= max) {
        // Grow the population to n actors through a running node.
        {
            let node = Node::start(config(data));
            let _ = Client::until_ok(
                node.addr,
                "GET",
                "/actor/t/a0/alarm",
                Duration::from_millis(1),
            );
            let from = created;
            post_all(node.addr, n - from, 32, |i| {
                format!("/actor/t/a{}/alarm?at={hour}", from + i)
            });
            created = n;
            node.stop_and_drain();
        }
        let files = std::fs::read_dir(data.join("actors/default/t"))
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|x| x == "db")
            })
            .count();
        assert_eq!(files, n, "databases on disk");
        let bytes: u64 = std::fs::read_dir(data.join("actors/default/t"))
            .unwrap()
            .map(|e| e.unwrap().metadata().unwrap().len())
            .sum();
        eprintln!(
            "restart: {n} actors, {} bytes on disk ({} per actor)",
            bytes,
            bytes / n as u64
        );

        let storage = Storage::new(
            StorageConfig::new(data),
            wasmkeep_server::state_dir(&wasmkeep_server::StateRoot::new("", "default")),
        );
        let pending = storage.pending_alarms().unwrap();
        assert_eq!(pending.found.len(), n, "alarms found");
        assert!(pending.skipped.is_empty());
        group.bench_with_input(BenchmarkId::new("scan", n), &n, |b, &n| {
            b.iter_custom(|iters| {
                let start = Instant::now();
                for _ in 0..iters {
                    assert_eq!(storage.pending_alarms().unwrap().found.len(), n);
                }
                start.elapsed()
            });
        });

        group.bench_with_input(BenchmarkId::new("serving", n), &n, |b, _| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (node, took) = serving(data);
                    total += took;
                    node.stop_and_drain();
                }
                total
            });
        });

        if FIRED_SIZES.contains(&n) {
            group.bench_with_input(BenchmarkId::new("fired", n), &n, |b, &n| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        set_alarms(data, n, now_ms() - 1_000);
                        let before = alarms_fired();
                        let start = Instant::now();
                        let node = Node::start(config(data));
                        let deadline = start + Duration::from_secs(600);
                        while alarms_fired() < before + n as u64 {
                            assert!(Instant::now() < deadline, "alarms never all fired");
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        total += start.elapsed();
                        node.stop_and_drain();
                    }
                    total
                });
            });
            // Leave every alarm an hour out again for the next size.
            set_alarms(data, n, hour);
        }
    }
    group.finish();
}

criterion_group!(restart, benches);
criterion_main!(restart);
