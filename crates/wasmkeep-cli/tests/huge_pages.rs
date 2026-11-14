//! `wasmkeep serve` turns transparent huge pages off for its own process
//! (Linux), unless told to keep the system's setting: with the kernel's
//! `always`, they more than double the memory per resident actor
//! (docs/benchmarks.md). The kernel reports it per process in
//! `/proc/<pid>/status` (`THP_enabled`).
#![cfg(target_os = "linux")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

mod support;

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

struct Node(Child);

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn counter_wasm() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/counter");
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
        .unwrap();
    assert!(status.success(), "building examples/counter failed");
    root.join("target/wasm32-wasip2/release/counter.wasm")
}

/// `THP_enabled` of a `wasmkeep serve` started with `extra`, once it runs.
fn thp_enabled(extra: &[&str]) -> Option<String> {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("serve.log");
    let mut node = Node(
        Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
            .arg("serve")
            .arg("--actor")
            .arg(format!("counter={}", counter_wasm().display()))
            .arg("--data-dir")
            .arg(dir.path())
            // Bound by the server, read from its log (support/mod.rs).
            .args(["--listen", "127.0.0.1:0"])
            .args(extra)
            .env("RUST_LOG", "info")
            // The log goes to stdout.
            .stdout(std::fs::File::create(&log).unwrap())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let status = format!("/proc/{}/status", node.0.id());
    // Serving: the setting was made before the runtime started.
    support::listening_on(&mut node.0, &log, Duration::from_secs(30));
    let text = std::fs::read_to_string(status).unwrap();
    text.lines()
        .find_map(|l| l.strip_prefix("THP_enabled:"))
        .map(|v| v.trim().to_owned())
}

#[test]
fn serve_turns_transparent_huge_pages_off_unless_told_otherwise() {
    // Kernels before 5.0 do not report it: nothing to check there.
    let Some(off) = thp_enabled(&[]) else {
        return;
    };
    assert_eq!(off, "0");
    assert_eq!(
        thp_enabled(&["--transparent-huge-pages", "system"]).as_deref(),
        Some("1")
    );
}
