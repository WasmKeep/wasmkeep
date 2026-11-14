//! A node whose log goes to a file or a pipe writes it without colour
//! codes: they only make sense on a terminal.
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

#[test]
fn a_log_written_to_a_file_has_no_colour_codes() {
    let wasm = counter_wasm();
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("node.log");
    let child = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .args(["serve", "--actor"])
        .arg(format!("counter={}", wasm.display()))
        .arg("--data-dir")
        .arg(dir.path().join("data"))
        .arg("--cache-dir")
        .arg(dir.path().join("cache"))
        // Bound by the server, read from its log (support/mod.rs).
        .args(["--listen", "127.0.0.1:0"])
        .env("RUST_LOG", "info")
        // NO_COLOR would hide colours whether or not the file is checked.
        .env_remove("NO_COLOR")
        .stdin(Stdio::null())
        .stdout(std::fs::File::create(&log).unwrap())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut node = Node(child);
    support::listening_on(&mut node.0, &log, Duration::from_secs(60));
    let text = std::fs::read_to_string(&log).unwrap();
    assert!(
        !text.contains('\u{1b}'),
        "colour codes in a log file:\n{text}"
    );
}
