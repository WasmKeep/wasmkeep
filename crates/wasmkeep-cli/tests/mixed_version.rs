//! The mixed-version check, `scripts/mixed-version.sh`, as an ignored test:
//! three nodes of the previous published release (`OLD_VERSION`, v0.9.2 by
//! default) moved one at a time to this checkout's binary, one moved back
//! and up again; then, when this checkout is the next minor version, the
//! gate of ADR 0043 (the format the new minor version brings, refused
//! until `cluster upgrade finalize`), nodes of older releases started after
//! it, tracing across versions, the old CLI on the finalized cluster, the
//! skew check and the finalize with no live node. It downloads the
//! release (gh), starts moto and takes about a quarter of an hour, so
//! it runs only when asked twice: with `--run-ignored only` and
//! `WASMKEEP_MIXED_VERSION=1`.
//!
//! The script's variables pass through: `MIXED_VERSION_NEW_VERSION=0.10.0`
//! builds this checkout once more as that version (the test-only feature
//! `test-version` of `wasmkeep-cluster`), into `target/mixed-version/build`,
//! for the next-minor phases before the workspace version is bumped.
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
