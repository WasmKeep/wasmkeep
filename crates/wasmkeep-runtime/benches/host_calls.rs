//! CPU-meter switch cost: a compute segment followed by 1,000 cheap host calls.
//! Pin the process to a core. Calibrate WASMKEEP_BENCH_SPIN once with the
//! compute-only case (aim just above the default 250 us switch threshold),
//! then keep that count identical across meter variants and alternating runs.
//! Each iteration resets the meter as a reused actor store does per event.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // bench code

use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmtime::component::Linker;

const GUEST: &str = r#"
(component
  (import "cheap-call" (func $host))
  (core func $host-core (canon lower (func $host)))
  (core module $m
    (import "host" "call" (func $host))
    (func (export "run") (param $work i64) (param $calls i32)
      (loop $compute
        (local.set $work (i64.sub (local.get $work) (i64.const 1)))
        (br_if $compute (i64.ne (local.get $work) (i64.const 0))))
      (block $done
        (br_if $done (i32.eqz (local.get $calls)))
        (loop $calls-loop
          call $host
          (local.set $calls (i32.sub (local.get $calls) (i32.const 1)))
          (br_if $calls-loop (local.get $calls))))))
  (core instance $host-instance (export "call" (func $host-core)))
  (core instance $i (instantiate $m (with "host" (instance $host-instance))))
  (func (export "run") (param "work" s64) (param "calls" s32)
    (canon lift (core func $i "run"))))
"#;

fn host_calls(c: &mut Criterion) {
    let work: i64 = std::env::var("WASMKEEP_BENCH_SPIN")
        .map(|n| n.parse().expect("WASMKEEP_BENCH_SPIN must be an integer"))
        .unwrap_or(400_000);
    assert!(work > 0);
    eprintln!("host-call benchmark: {work} compute iterations per event");
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let runtime = Runtime::new(&RuntimeConfig::default()).unwrap();
    let component = runtime.compile(&wat::parse_str(GUEST).unwrap()).unwrap();
    let mut linker = Linker::<()>::new(runtime.engine());
    linker
        .root()
        .func_wrap("cheap-call", |_, (): ()| Ok(()))
        .unwrap();
    let pre = runtime.instantiate_pre(&linker, &component).unwrap();
    let (mut store, meter) = runtime.new_metered_async_store(());
    let instance = executor
        .block_on(pre.instantiate_async(&mut store))
        .unwrap();
    let run = instance
        .get_typed_func::<(i64, i32), ()>(&mut store, "run")
        .unwrap();
    let mut group = c.benchmark_group("cpu_meter");
    group
        .sample_size(30)
        .measurement_time(Duration::from_secs(5));
    for (name, calls) in [("compute", 0), ("compute_then_1000_host_calls", 1_000)] {
        group.bench_function(name, |b| {
            b.iter(|| {
                meter.reset_event(&mut store);
                // Isolate the long-segment switch from an epoch check landing
                // halfway through the compute prefix. With the default 1 ms
                // tick, two ticks leave at least 1 ms before the first check.
                // The meter still uses the default 250 us switch threshold.
                store.set_epoch_deadline(2);
                executor.block_on(async {
                    run.call_async(&mut store, (work, calls)).await.unwrap();
                });
            });
        });
    }
    group.finish();
}

criterion_group!(benches, host_calls);
criterion_main!(benches);
