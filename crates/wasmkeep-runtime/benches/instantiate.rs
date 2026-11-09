//! Instantiation cost of a cached component through the pooling allocator.
//! Target: p50 under 50 µs (measured: docs/benchmarks.md, "WasmKeep on its own").

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use wasmkeep_runtime::{ComponentCache, Runtime, RuntimeConfig};
use wasmtime::component::Linker;

const ANSWER_WAT: &str = include_str!("../tests/fixtures/answer.wat");

fn instantiate(c: &mut Criterion) {
    let wasm = wat::parse_str(ANSWER_WAT).unwrap();
    let runtime = Runtime::new(&RuntimeConfig::default()).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cache = ComponentCache::new(runtime.clone(), dir.path()).unwrap();
    // Warm the cache, then load from disk as a restarted node would.
    cache.get_or_compile(&wasm).unwrap();
    let component = cache.get_or_compile(&wasm).unwrap();
    let linker = Linker::<()>::new(runtime.engine());
    let pre = runtime.instantiate_pre(&linker, &component).unwrap();

    let mut group = c.benchmark_group("instantiate");

    // New store + instance, then drop: one full wake/hibernate cycle of the
    // Wasm side (the slot returns to the pool on drop).
    group.bench_function("store+instance", |b| {
        b.iter(|| {
            let mut store = runtime.new_store(());
            let instance = pre.instantiate(&mut store).unwrap();
            black_box((store, instance));
        });
    });

    // Same, plus one call into the guest.
    group.bench_function("store+instance+call", |b| {
        b.iter(|| {
            let mut store = runtime.new_store(());
            let instance = pre.instantiate(&mut store).unwrap();
            let run = instance
                .get_typed_func::<(), (i32,)>(&mut store, "run")
                .unwrap();
            black_box(run.call(&mut store, ()).unwrap());
        });
    });

    group.finish();
}

criterion_group!(benches, instantiate);
criterion_main!(benches);
