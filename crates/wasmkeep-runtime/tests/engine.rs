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

#[test]
fn fuel_instrumentation_has_a_distinct_component_cache_key() {
    let dir = tempfile::tempdir().unwrap();
    let wasm = answer_wasm();
    let mut keys = Vec::new();
    for measure_fuel in [false, true] {
        let runtime = Runtime::new(&RuntimeConfig {
            max_instances: 2,
            measure_fuel,
            ..RuntimeConfig::default()
        })
        .unwrap();
        let cache = ComponentCache::new(runtime.clone(), dir.path()).unwrap();
        keys.push(cache.key(&wasm));
        let component = cache.get_or_compile(&wasm).unwrap();
        assert_eq!(call_run(&runtime, &component), 42);
    }
    assert_ne!(keys[0], keys[1]);
}

/// A cached artefact is native code: one that others could have written
/// (group- or world-writable) is not loaded; the component is compiled
/// again and the entry replaced.
#[cfg(unix)]
#[test]
fn a_group_writable_cache_file_is_not_loaded() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let runtime = small_runtime();
    let cache = ComponentCache::new(runtime.clone(), dir.path().join("cache")).unwrap();
    let answer = answer_wasm();
    let other = wat::parse_str(ANSWER_WAT.replace("i32.const 42", "i32.const 7")).unwrap();
    // A valid artefact, of another component, where `answer`'s would be.
    let planted = cache.get_or_compile(&other).unwrap();
    assert_eq!(call_run(&runtime, &planted), 7);
    let path = cache.path_for(&answer);
    std::fs::rename(cache.path_for(&other), &path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o664)).unwrap();

    let component = cache.get_or_compile(&answer).unwrap();
    assert_eq!(call_run(&runtime, &component), 42);
    let mode = std::fs::metadata(&path).unwrap().permissions().mode();
    assert_eq!(mode & 0o077, 0, "replaced, the owner's alone: {mode:o}");
    let again = cache.get_or_compile(&answer).unwrap();
    assert_eq!(call_run(&runtime, &again), 42);
}

/// In a directory others can write to, nothing is loaded from the cache
/// or written to it.
#[cfg(unix)]
#[test]
fn a_group_writable_cache_directory_is_not_used() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let runtime = small_runtime();
    let cache = ComponentCache::new(runtime.clone(), dir.path().join("cache")).unwrap();
    let answer = answer_wasm();
    let other = wat::parse_str(ANSWER_WAT.replace("i32.const 42", "i32.const 7")).unwrap();
    cache.get_or_compile(&other).unwrap();
    let path = cache.path_for(&answer);
    std::fs::rename(cache.path_for(&other), &path).unwrap();
    std::fs::set_permissions(cache.dir(), std::fs::Permissions::from_mode(0o770)).unwrap();

    let component = cache.get_or_compile(&answer).unwrap();
    assert_eq!(call_run(&runtime, &component), 42);
    let fresh = wat::parse_str(ANSWER_WAT.replace("i32.const 42", "i32.const 9")).unwrap();
    let compiled = cache.get_or_compile(&fresh).unwrap();
    assert_eq!(call_run(&runtime, &compiled), 9);
    assert!(!cache.path_for(&fresh).exists(), "nothing written there");
}

/// The directory is made the owner's alone, at an absolute path.
#[cfg(unix)]
#[test]
fn the_cache_directory_is_created_private() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let cache = ComponentCache::new(small_runtime(), dir.path().join("a/cache")).unwrap();
    assert!(cache.dir().is_absolute());
    let mode = std::fs::metadata(cache.dir()).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o700, "{mode:o}");
    // Opened through a symlink, the cache names the real directory.
    std::os::unix::fs::symlink(dir.path().join("a/cache"), dir.path().join("link")).unwrap();
    let linked = ComponentCache::new(small_runtime(), dir.path().join("link")).unwrap();
    assert_eq!(linked.dir(), cache.dir().canonicalize().unwrap());
}

/// An entry that is a symlink is not followed, even to a private file:
/// the component is compiled again and the link replaced by a file.
#[cfg(unix)]
#[test]
fn a_symlinked_cache_entry_is_not_followed() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = small_runtime();
    let cache = ComponentCache::new(runtime.clone(), dir.path().join("cache")).unwrap();
    let answer = answer_wasm();
    let other = wat::parse_str(ANSWER_WAT.replace("i32.const 42", "i32.const 7")).unwrap();
    cache.get_or_compile(&other).unwrap();
    let elsewhere = dir.path().join("elsewhere.cwasm");
    std::fs::rename(cache.path_for(&other), &elsewhere).unwrap();
    let path = cache.path_for(&answer);
    std::os::unix::fs::symlink(&elsewhere, &path).unwrap();

    let component = cache.get_or_compile(&answer).unwrap();
    assert_eq!(call_run(&runtime, &component), 42);
    assert!(std::fs::symlink_metadata(&path).unwrap().is_file());
}

/// An artefact an older version wrote readable by others (0644) is still
/// this user's: it is loaded, and made the owner's alone.
#[cfg(unix)]
#[test]
fn an_older_readable_artefact_is_loaded_and_made_private() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let runtime = small_runtime();
    let cache = ComponentCache::new(runtime.clone(), dir.path().join("cache")).unwrap();
    let answer = answer_wasm();
    cache.get_or_compile(&answer).unwrap();
    let path = cache.path_for(&answer);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let written = std::fs::metadata(&path).unwrap().modified().unwrap();

    let component = cache.get_or_compile(&answer).unwrap();
    assert_eq!(call_run(&runtime, &component), 42);
    let meta = std::fs::metadata(&path).unwrap();
    assert_eq!(
        meta.modified().unwrap(),
        written,
        "loaded, not compiled again"
    );
    assert_eq!(meta.permissions().mode() & 0o777, 0o600);
}
