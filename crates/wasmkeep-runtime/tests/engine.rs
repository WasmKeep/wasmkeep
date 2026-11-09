//! Engine and component cache, exercised with a real (tiny) component.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use wasmkeep_runtime::{ComponentCache, Runtime, RuntimeConfig};
use wasmtime::component::{Component, Linker};

const ANSWER_WAT: &str = include_str!("fixtures/answer.wat");

fn answer_wasm() -> Vec<u8> {
    wat::parse_str(ANSWER_WAT).unwrap()
}

fn small_runtime() -> Runtime {
    Runtime::new(&RuntimeConfig {
        max_instances: 8,
        ..RuntimeConfig::default()
    })
    .unwrap()
}

fn call_run(runtime: &Runtime, component: &Component) -> i32 {
    let linker = Linker::<()>::new(runtime.engine());
    let pre = runtime.instantiate_pre(&linker, component).unwrap();
    let mut store = runtime.new_store(());
    let instance = pre.instantiate(&mut store).unwrap();
    let run = instance
        .get_typed_func::<(), (i32,)>(&mut store, "run")
        .unwrap();
    run.call(&mut store, ()).unwrap().0
}

#[test]
fn compiles_and_runs_a_component() {
    let runtime = small_runtime();
    let component = runtime.compile(&answer_wasm()).unwrap();
    assert_eq!(call_run(&runtime, &component), 42);
}

#[test]
fn rejects_bytes_that_are_not_a_component() {
    let runtime = small_runtime();
    let err = runtime.compile(b"not wasm").unwrap_err();
    assert!(
        matches!(err, wasmkeep_runtime::Error::Compile(_)),
        "{err:?}"
    );
}

#[test]
fn cache_miss_writes_artifact_and_hit_loads_it() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = small_runtime();
    let cache = ComponentCache::new(runtime.clone(), dir.path().join("cache")).unwrap();
    let wasm = answer_wasm();

    let path = cache.path_for(&wasm);
    assert!(!path.exists());

    let first = cache.get_or_compile(&wasm).unwrap();
    assert!(path.exists());
    let written = std::fs::metadata(&path).unwrap().modified().unwrap();
    assert_eq!(call_run(&runtime, &first), 42);

    let second = cache.get_or_compile(&wasm).unwrap();
    assert_eq!(
        std::fs::metadata(&path).unwrap().modified().unwrap(),
        written
    );
    assert_eq!(call_run(&runtime, &second), 42);

    // Exactly one artifact, no leftover temp files.
    let entries: Vec<_> = std::fs::read_dir(cache.dir()).unwrap().collect();
    assert_eq!(entries.len(), 1);
}

#[test]
fn cache_survives_a_new_runtime_with_the_same_config() {
    let dir = tempfile::tempdir().unwrap();
    let wasm = answer_wasm();

    let path = {
        let cache = ComponentCache::new(small_runtime(), dir.path()).unwrap();
        cache.get_or_compile(&wasm).unwrap();
        cache.path_for(&wasm)
    };

    // A restarted node computes the same key and loads the existing file.
    let runtime = small_runtime();
    let cache = ComponentCache::new(runtime.clone(), dir.path()).unwrap();
    assert_eq!(cache.path_for(&wasm), path);
    let component = cache.get_or_compile(&wasm).unwrap();
    assert_eq!(call_run(&runtime, &component), 42);
}

#[test]
fn corrupt_cache_entry_is_recompiled() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = small_runtime();
    let cache = ComponentCache::new(runtime.clone(), dir.path()).unwrap();
    let wasm = answer_wasm();

    std::fs::write(cache.path_for(&wasm), b"garbage").unwrap();
    let component = cache.get_or_compile(&wasm).unwrap();
    assert_eq!(call_run(&runtime, &component), 42);
}

#[test]
fn different_components_get_different_keys() {
    let dir = tempfile::tempdir().unwrap();
    let cache = ComponentCache::new(small_runtime(), dir.path()).unwrap();
    let a = answer_wasm();
    let b = wat::parse_str(ANSWER_WAT.replace("i32.const 42", "i32.const 7")).unwrap();
    assert_ne!(cache.key(&a), cache.key(&b));
}

#[test]
fn pool_limit_caps_live_instances() {
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 2,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(&answer_wasm()).unwrap();
    let linker = Linker::<()>::new(runtime.engine());
    let pre = runtime.instantiate_pre(&linker, &component).unwrap();

    let mut stores: Vec<_> = (0..2).map(|_| runtime.new_store(())).collect();
    for store in &mut stores {
        pre.instantiate(&mut *store).unwrap();
    }
    let mut third = runtime.new_store(());
    assert!(pre.instantiate(&mut third).is_err());

    // Dropping a store frees its slot.
    stores.pop();
    pre.instantiate(&mut third).unwrap();
}
