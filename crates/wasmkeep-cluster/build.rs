//! Sets `WASMKEEP_VERSION`, the version every node reports (`--version`,
//! its heartbeat, `wasmkeep_info{version}`): the workspace version for a
//! release, `<version>+<commit>` for any other build from a git checkout.
//!
//! A release is built from its tag: when the tag `v<version>` names the
//! commit checked out, the version alone. Without git, or outside this
//! repository's checkout (a source tarball, a container build without
//! `.git`, a crate unpacked inside another repository), the version alone
//! too: the build never fails for want of git. The commit is the short id
//! git gives (12 hex digits or more), as semver build metadata, so
//! `0.3.0+0a1b2c3d4e5f` is still version 0.3.0 to anything that compares
//! versions. Uncommitted changes are not marked.

use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let version = env!("CARGO_PKG_VERSION");
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    println!("cargo:rerun-if-changed=build.rs");
    let full = match commit(&manifest, version) {
        Some(commit) => format!("{version}+{commit}"),
        None => version.to_owned(),
    };
    println!("cargo:rustc-env=WASMKEEP_VERSION={full}");
}

/// The commit to name, or `None` for a release (the tag `v<version>` on
/// it) or a build outside a git checkout of this repository.
fn commit(manifest: &Path, version: &str) -> Option<String> {
    // This crate is crates/wasmkeep-cluster in the workspace.
    let root = manifest.parent()?.parent()?.canonicalize().ok()?;
    let top = git(&root, &["rev-parse", "--show-toplevel"])?;
    if Path::new(&top).canonicalize().ok()? != root {
        return None;
    }
    watch(&root);
    let tags = git(&root, &["tag", "--points-at", "HEAD"]).unwrap_or_default();
    if tags.lines().any(|t| t == format!("v{version}")) {
        return None;
    }
    let commit = git(&root, &["rev-parse", "--short=12", "HEAD"])?;
    commit
        .bytes()
        .all(|b| b.is_ascii_hexdigit())
        .then_some(commit)
}

/// Builds again when HEAD moves or a tag is added: the files git keeps
/// them in, in a worktree too (where `.git` is a file). Only paths that
/// exist: cargo reruns the script every time for one that does not.
fn watch(root: &Path) {
    let dir = |arg: &str| git(root, &["rev-parse", arg]).map(|d| root.join(d));
    let (Some(git_dir), Some(common)) = (dir("--git-dir"), dir("--git-common-dir")) else {
        return;
    };
    let mut paths = vec![
        git_dir.join("HEAD"),
        common.join("packed-refs"),
        common.join("refs/tags"),
    ];
    if let Some(head) = git(root, &["symbolic-ref", "-q", "HEAD"]) {
        paths.push(common.join(head));
    }
    for p in paths.iter().filter(|p| p.exists()) {
        println!("cargo:rerun-if-changed={}", p.display());
    }
}

/// `git <args>` in `dir`: its trimmed output, or `None` if git is missing
/// or fails.
fn git(dir: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?.trim().to_owned();
    (!s.is_empty()).then_some(s)
}
