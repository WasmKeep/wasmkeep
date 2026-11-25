//! The mixed-version check, `scripts/mixed-version.sh`, as an ignored test:
//! three nodes of the previous published release (`OLD_VERSION`, v0.8.1 by
//! default) moved one at a time to this checkout's binary, one moved back
//! and up again, then the gate of ADR 0043 (a deploy that needs manifest 8,
//! refused until `cluster upgrade finalize`), a node of the old release
//! started after it, tracing across versions, and the old CLI on the
//! finalized cluster. It downloads the release (gh), starts moto and takes
//! about half an hour, so it runs only when asked twice: with
//! `--run-ignored only` and `WASMKEEP_MIXED_VERSION=1`.
//!
//! While the workspace version has the old release's minor version, the
//! script builds this checkout once more as the next one (the test-only
//! feature `test-version` of `wasmkeep-cluster`), into
//! `target/mixed-version/build`.
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
