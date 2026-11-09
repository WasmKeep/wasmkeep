//! Builds guest components for tests and benches. Shared via `#[path]`.

use std::path::PathBuf;
use std::process::Command;

use wasmkeep_runtime::{ComponentCache, Runtime};
use wasmtime::component::Component;

/// Builds the guest crate at `dir` (relative to the wasmkeep-server crate)
/// for wasm32-wasip2 and returns the component bytes.
pub(crate) fn build_guest(dir: &str, name: &str) -> Vec<u8> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(dir);
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let status = Command::new(cargo)
        .current_dir(&root)
        .args([
            "build",
            "--release",
            "--locked",
            "--target",
            "wasm32-wasip2",
        ])
        .status()
        .unwrap_or_else(|e| panic!("running cargo for {dir}: {e}"));
    assert!(status.success(), "building {dir} failed");
    std::fs::read(root.join(format!("target/wasm32-wasip2/release/{name}.wasm"))).unwrap()
}

/// Compiles `wasm` through an on-disk cache shared by every test process
/// (under cargo's per-target temporary directory). Components that embed a
/// language runtime (TypeScript, Python) take tens of seconds to compile,
/// and each test runs in its own process: without the cache, every such
/// test paid that again. Keyed by the engine's compatibility hash and the
/// bytes, so a change to either recompiles.
#[allow(dead_code, reason = "not every test binary compiles through it")]
pub(crate) fn compile(runtime: &Runtime, wasm: &[u8]) -> Component {
    // Set for integration tests; unit tests that share this file use the
    // workspace's target directory.
    let tmp = option_env!("CARGO_TARGET_TMPDIR").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/tmp"),
        PathBuf::from,
    );
    let dir = tmp.join("components");
    ComponentCache::new(runtime.clone(), dir)
        .unwrap()
        .get_or_compile(wasm)
        .unwrap()
}
