//! The mixed-version test, `scripts/mixed-version.sh`, as an ignored test:
//! three nodes of the published 0.7.0 binary moved one at a time to this
//! checkout's, then one moved back. It downloads the release (gh), starts
//! moto and takes a few minutes, so it runs only when asked twice: with
//! `--run-ignored only` and `WASMKEEP_MIXED_VERSION=1`.
//!
//! ```text
//! WASMKEEP_MIXED_VERSION=1 cargo nextest run -p wasmkeep-cli --test mixed_version \
//!   --run-ignored only --no-capture
//! ```
//!
//! A release-candidate step (docs/releasing.md), never part of CI.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::path::PathBuf;
use std::process::Command;

#[test]
#[ignore = "downloads 0.7.0 and runs three nodes for minutes: scripts/mixed-version.sh"]
fn mixed_version_cluster() {
    if std::env::var_os("WASMKEEP_MIXED_VERSION").is_none() {
        eprintln!("skipped: set WASMKEEP_MIXED_VERSION=1 to run scripts/mixed-version.sh");
        return;
    }
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let status = Command::new(root.join("scripts/mixed-version.sh"))
        .current_dir(&root)
        .status()
        .expect("running scripts/mixed-version.sh");
    assert!(
        status.success(),
        "scripts/mixed-version.sh failed: see target/mixed-version/report.txt"
    );
}
