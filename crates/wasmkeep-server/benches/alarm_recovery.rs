//! How alarm recovery after a node dies scales with the number of alarms it
//! leaves behind. Three nodes share an in-memory
//! bucket. Per sample, node `a` creates N test actors, each with an alarm
//! at the same instant `T` a few seconds out, and is killed (its runtime
//! dropped: no lease released) once the bucket holds every actor and every
//! alarm index entry. Nodes `b` and `c` never saw those actors: they find
//! the alarms in the bucket's index (`sweep_alarms`, at most 256 deliveries
//! per sweep per node) and deliver each by taking the actor over, restoring
//! it from the bucket and running `on-alarm`.
//!
//! - `alarm_recovery/N`: from `T` until every alarm has run on `b` or `c`
//!   and the index is empty
//!
//! Timings are a tenth of the defaults (lease ttl 1 s, grace 0.5 s,
//! heartbeat 0.3 s, sweep every 1 s), so an alarm is overdue, and
//! deliverable, 1.5 s after `T`, and anyone's (not only its shard
//! owner's) 4.5 s after. The parts of the result set by those timings
//! scale with them; the work per delivery does not. Run `bench/run.sh` to
//! record results as CSV.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // bench code

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, SamplingMode, criterion_group, criterion_main};
use futures_util::TryStreamExt;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use wasmkeep_server::{ActorConfig, ClusterConfig, LeaseConfig};

#[path = "support/scale.rs"]
mod scale;

use scale::{Client, Node, admission_retries, alarms_fired, now_ms, post_all};

/// Alarms left behind by the dead node.
const SIZES: [usize; 3] = [100, 1_000, 10_000];

fn node(id: &str, data: &std::path::Path, store: &Arc<dyn ObjectStore>) -> Node {
    let mut config = ActorConfig::new(data);
    let mut cluster = ClusterConfig::new(id, String::new(), Arc::clone(store));
    // A tenth of the defaults (ttl 10 s, grace 5 s, safety 2 s, skew 1 s;
    // heartbeat 3 s; sweep 10 s).
    cluster.lease = LeaseConfig::new(1_000, 500, 200, 100).unwrap();
    cluster.heartbeat = Duration::from_millis(300);
    cluster.alarm_sweep = Duration::from_secs(1);
    config.cluster = Some(cluster);
    Node::start(config)
}

/// Objects under `prefix` in the bucket.
fn count(rt: &tokio::runtime::Runtime, store: &Arc<dyn ObjectStore>, prefix: &str) -> usize {
    rt.block_on(async {
        store
            .list(Some(&StorePath::from(prefix)))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len()
    })
}

/// One sample: returns the time from `T` until every alarm ran and the
/// index emptied. `lead` is how far out `T` is set when the setup starts;
/// it grows if the setup did not finish in time.
fn recover(rt: &tokio::runtime::Runtime, n: usize, lead: &mut Duration) -> Duration {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs = [
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    ];
    let a = node("a", dirs[0].path(), &store);
    let b = node("b", dirs[1].path(), &store);
    let c = node("c", dirs[2].path(), &store);
    for addr in [a.addr, b.addr, c.addr] {
        let _ = Client::until_ok(
            addr,
            "GET",
            "/actor/t/warm/alarm",
            Duration::from_millis(50),
        );
    }
    // The warm-up actors are a's, b's or c's, with no alarm: not counted.

    let setup = Instant::now();
    let at = now_ms() + u64::try_from(lead.as_millis()).unwrap();
    post_all(a.addr, n, 32, |i| format!("/actor/t/x{i}/alarm?at={at}"));
    // Every alarm indexed, then every actor's objects uploaded (they
    // follow the answer): the count under `actors/` stops moving.
    let deadline = Instant::now() + Duration::from_secs(120);
    while count(rt, &store, "alarms") < n {
        assert!(Instant::now() < deadline, "alarms never all indexed");
        std::thread::sleep(Duration::from_millis(20));
    }
    let mut last = 0;
    loop {
        let now = count(rt, &store, "actors");
        if now == last {
            break;
        }
        last = now;
        assert!(Instant::now() < deadline, "uploads never settled");
        std::thread::sleep(Duration::from_millis(300));
    }
    a.kill();
    let ready = setup.elapsed();
    let left = Duration::from_millis(at.saturating_sub(now_ms()));
    if left < Duration::from_millis(500) {
        // Too close (or past): a may have fired some itself. Give the next
        // sample more room and redo this one.
        *lead = (*lead + ready) * 2;
        b.stop_and_drain();
        c.stop_and_drain();
        return recover(rt, n, lead);
    }
    *lead = (*lead).max(ready + ready / 2 + Duration::from_secs(1));

    let fired = alarms_fired();
    std::thread::sleep(left);
    let due = Instant::now();
    let deadline = due + Duration::from_secs(900);
    while alarms_fired() < fired + n as u64 || count(rt, &store, "alarms") > 0 {
        assert!(
            Instant::now() < deadline,
            "{} of {n} alarms fired, {} index entries left",
            alarms_fired() - fired,
            count(rt, &store, "alarms")
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let took = due.elapsed();
    eprintln!(
        "alarm_recovery/{n}: setup {:.1} s, recovered {:.2} s after due",
        ready.as_secs_f64(),
        took.as_secs_f64()
    );
    let extra = alarms_fired() - fired - n as u64;
    if extra > 0 {
        eprintln!("alarm_recovery/{n}: {extra} alarms ran twice");
    }
    // Each node reserves address space for its instance pool: free it
    // before the next sample starts three more.
    b.stop_and_drain();
    c.stop_and_drain();
    took
}

fn benches(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let retries = admission_retries();
    let mut group = c.benchmark_group("alarm_recovery");
    group
        .sample_size(10)
        .sampling_mode(SamplingMode::Flat)
        .warm_up_time(Duration::from_millis(1))
        .measurement_time(Duration::from_secs(1));
    for n in SIZES {
        // Generous for the first sample; then from the setups seen.
        let mut lead = Duration::from_secs(3) + Duration::from_millis(n as u64);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |bench, &n| {
            bench.iter_custom(|iters| (0..iters).map(|_| recover(&rt, n, &mut lead)).sum());
        });
    }
    group.finish();
    eprintln!(
        "alarm_recovery: {} alarm admission retries",
        admission_retries() - retries
    );
}

criterion_group!(alarm_recovery, benches);
criterion_main!(alarm_recovery);
