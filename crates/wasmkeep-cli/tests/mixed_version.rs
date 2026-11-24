//! The mixed-version check, `scripts/mixed-version.sh`, as an ignored test:
//! three nodes of the previous published release (`OLD_VERSION`, v0.8.0 by
//! default) moved one at a time to this checkout's binary, then one moved
//! back. It downloads the release (gh), starts moto and takes several
//! minutes, so it runs only when asked twice: with `--run-ignored only` and
//! `WASMKEEP_MIXED_VERSION=1`.
//!
//! ```text
//! WASMKEEP_MIXED_VERSION=1 cargo nextest run -p wasmkeep-cli --test mixed_version \
//!   --run-ignored only --no-capture
//! ```
//!
//! A gate before tagging a release (docs/releasing.md), run on a
//! workstation alone; never part of CI.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::path::PathBuf;
use std::process::Command;

#[test]
#[ignore = "downloads the previous release and runs three nodes for minutes: scripts/mixed-version.sh"]
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
