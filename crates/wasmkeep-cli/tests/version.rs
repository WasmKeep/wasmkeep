//! `wasmkeep --version` reports the build's version, the one its heartbeat
//! and `wasmkeep_info{version}` carry: the release's alone, or naming the
//! commit for any other build from a checkout.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::process::Command;

#[test]
fn version_names_the_build() {
    let out = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .arg("--version")
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
    let printed = String::from_utf8(out.stdout).unwrap();
    assert_eq!(printed, format!("wasmkeep {}\n", wasmkeep_server::VERSION));
    assert!(
        wasmkeep_server::VERSION.starts_with(env!("CARGO_PKG_VERSION")),
        "{printed}"
    );
}
