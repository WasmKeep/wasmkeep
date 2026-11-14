//! `wasmkeep new`: every variant (Rust or TypeScript, with or without a
//! front) written into an empty directory, linted, then built and served by
//! `wasmkeep dev` as its README says, and answering the README's requests.
//! A non-empty directory, or a bad `--app`, is refused before anything is
//! written.
//!
//! The generated manifests name the SDK at the CLI's own version, which a
//! registry may not have yet (between a version bump and its release), so
//! the builds here take the SDK from this checkout without changing a
//! generated file: Rust through a `[patch.crates-io]` in a
//! `.cargo/config.toml` beside the project (cargo reads the configuration
//! of every parent directory), TypeScript through `node_modules` links to
//! `sdks/typescript` in place of `npm install`. The TypeScript variants run
//! when `sdks/typescript` has its `node_modules` (`npm ci` there), and must
//! with `WASMKEEP_REQUIRE_LANGS=1`.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

mod support;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use support::request;

/// The version the generated manifests must name: the CLI's.
const VERSION: &str = env!("CARGO_PKG_VERSION");

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

/// `wasmkeep new ARGS...` in `cwd`: whether it succeeded, and its output.
fn new(cwd: &Path, args: &[&str]) -> (bool, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .current_dir(cwd)
        .arg("new")
        .args(args)
        .output()
        .unwrap();
    (
        out.status.success(),
        format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ),
    )
}

fn read(dir: &Path, file: &str) -> String {
    std::fs::read_to_string(dir.join(file)).unwrap_or_else(|e| panic!("{file}: {e}"))
}

/// Runs `program args` in `dir`, failing the test if it fails.
fn run(dir: &Path, program: &str, args: &[&str]) {
    let status = Command::new(program)
        .current_dir(dir)
        .args(args)
        .status()
        .unwrap_or_else(|e| panic!("{program}: {e}"));
    assert!(status.success(), "{program} {args:?} in {}", dir.display());
}

/// A new project `hits` in a temporary directory: the directory, and the
/// project's path in it.
fn generate(args: &[&str]) -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let mut all = vec!["hits"];
    all.extend_from_slice(args);
    let (ok, out) = new(temp.path(), &all);
    assert!(ok, "{out}");
    let dir = temp.path().join("hits");
    // What every variant has, and what it tells the reader to run.
    let readme = read(&dir, "README.md");
    for command in ["wasmkeep dev", "wasmkeep deploy"] {
        assert!(readme.contains(command), "README.md: no {command}");
        assert!(out.contains(command), "the next steps: no {command}: {out}");
    }
    let ignore = read(&dir, ".gitignore");
    for line in [".env.local", ".wasmkeep/"] {
        assert!(ignore.lines().any(|l| l == line), ".gitignore: {line}");
    }
    assert!(read(&dir, ".env.local.example").contains("chmod 600"));
    assert!(!dir.join(".env.local").exists());
    let file = read(&dir, "wasmkeep.toml");
    assert!(
        file.contains("tenant = \"my-team\"\nname = \"hits\""),
        "{file}"
    );
    assert!(!file.contains("{{") && !readme.contains("{{"), "{file}");
    (temp, dir)
}

/// The Rust crates build against this checkout's SDK (see the top).
fn redirect_rust_sdk(temp: &Path) {
    let sdk = repo().join("crates/wasmkeep-sdk");
    std::fs::create_dir(temp.join(".cargo")).unwrap();
    std::fs::write(
        temp.join(".cargo/config.toml"),
        format!(
            "[patch.crates-io]\nwasmkeep-sdk = {{ path = {:?} }}\n",
            sdk.display().to_string()
        ),
    )
    .unwrap();
}

/// A generated crate: its pin, then fmt and clippy as CI runs them on the
/// examples (the templates are not part of the workspace's own checks).
fn check_crate(dir: &Path, front: bool) {
    let manifest = read(dir, "Cargo.toml");
    let pin = if front {
        format!("wasmkeep-sdk = {{ version = \"{VERSION}\", default-features = false }}")
    } else {
        format!("wasmkeep-sdk = \"{VERSION}\"")
    };
    assert!(manifest.contains(&pin), "{manifest}");
    assert!(manifest.contains("\n[workspace]\n"), "{manifest}");
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    run(dir, &cargo, &["fmt", "--check"]);
    run(
        dir,
        &cargo,
        &[
            "clippy",
            "--release",
            "--target",
            "wasm32-wasip2",
            "--quiet",
            "--",
            "-D",
            "warnings",
        ],
    );
}

/// `sdks/typescript`, if its dependencies are installed.
fn ts_sdk() -> Option<PathBuf> {
    let sdk = repo().join("sdks/typescript");
    let ready =
        sdk.join("node_modules").is_dir() && Command::new("node").arg("--version").output().is_ok();
    if ready {
        return Some(sdk);
    }
    assert!(
        std::env::var("WASMKEEP_REQUIRE_LANGS").as_deref() != Ok("1"),
        "the TypeScript variants need node and sdks/typescript/node_modules (npm ci there)"
    );
    None
}

/// What `npm install` would do with the pinned SDK, from this checkout;
/// then the type check.
fn link_ts_sdk(dir: &Path, sdk: &Path) {
    let manifest = read(dir, "package.json");
    assert!(
        manifest.contains(&format!("\"@wasmkeep/sdk\": \"{VERSION}\"")),
        "{manifest}"
    );
    std::fs::create_dir_all(dir.join("node_modules/@wasmkeep")).unwrap();
    std::fs::create_dir_all(dir.join("node_modules/.bin")).unwrap();
    std::os::unix::fs::symlink(sdk, dir.join("node_modules/@wasmkeep/sdk")).unwrap();
    std::os::unix::fs::symlink(
        sdk.join("bin/wasmkeep-ts.mjs"),
        dir.join("node_modules/.bin/wasmkeep-ts"),
    )
    .unwrap();
    let tsc = sdk.join("node_modules/typescript/bin/tsc");
    run(dir, "node", &[tsc.to_str().unwrap(), "--noEmit", "-p", "."]);
}

/// `wasmkeep dev` in the project: it runs every build first, cold here.
fn dev(temp: tempfile::TempDir, dir: PathBuf) -> support::Dev {
    support::start_dev(temp, dir, 100, Duration::from_secs(600))
}

/// The README's requests, without a front: the class is open.
fn counts_without_a_front(dev: &support::Dev) {
    let a = dev.addr;
    let post = request(a, "POST", "/actor/counter/alice", "", "").unwrap();
    assert_eq!(post, (200, "1\n".into()), "{}", dev.log());
    let post = request(a, "POST", "/actor/counter/alice", "", "").unwrap();
    assert_eq!(post, (200, "2\n".into()));
    let get = request(a, "GET", "/actor/counter/bob", "", "").unwrap();
    assert_eq!(get, (200, "0\n".into()));
    clean_log(dev);
}

/// The README's requests, with a front: the page, the API, and the class
/// out of a client's reach.
fn counts_through_the_front(dev: &support::Dev) {
    let a = dev.addr;
    let (status, page) = request(a, "GET", "/", "", "").unwrap();
    assert_eq!(status, 200, "{page} {}", dev.log());
    assert!(page.contains("<strong>0</strong>"), "{page}");
    // The page's button: one more, then back to the page.
    let (status, _) = request(a, "POST", "/", "", "").unwrap();
    assert_eq!(status, 303);
    let (_, page) = request(a, "GET", "/", "", "").unwrap();
    assert!(page.contains("<strong>1</strong>"), "{page}");

    let post = request(a, "POST", "/api/counter/alice", "", "").unwrap();
    assert_eq!(post, (200, "1\n".into()), "{}", dev.log());
    let get = request(a, "GET", "/api/counter/alice", "", "").unwrap();
    assert_eq!(get, (200, "1\n".into()));
    // A name the front does not take: no actor is created for it.
    let (status, _) = request(a, "POST", "/api/counter/No%20Such", "", "").unwrap();
    assert_eq!(status, 404);
    // The class is internal: the front reaches it, a client does not.
    let (status, _) = request(a, "GET", "/actor/counter/alice", "", "").unwrap();
    assert_eq!(status, 403, "{}", dev.log());
    clean_log(dev);
}

/// No instance trapped or failed after its answer.
fn clean_log(dev: &support::Dev) {
    std::thread::sleep(Duration::from_millis(200));
    let log = dev.log();
    assert!(
        !log.contains("guest_error") && !log.contains("trapped"),
        "{log}"
    );
}

#[test]
fn a_rust_actor_builds_and_dev_serves_it() {
    let (temp, dir) = generate(&["--lang", "rust"]);
    assert!(!dir.join("front").exists());
    redirect_rust_sdk(temp.path());
    check_crate(&dir.join("counter"), false);
    counts_without_a_front(&dev(temp, dir));
}

#[test]
fn a_rust_actor_with_a_front_builds_and_dev_serves_it() {
    let (temp, dir) = generate(&["--lang", "rust", "--front"]);
    redirect_rust_sdk(temp.path());
    check_crate(&dir.join("counter"), false);
    check_crate(&dir.join("front"), true);
    counts_through_the_front(&dev(temp, dir));
}

#[test]
fn a_typescript_actor_builds_and_dev_serves_it() {
    let Some(sdk) = ts_sdk() else { return };
    let (temp, dir) = generate(&["--lang", "typescript"]);
    assert!(!dir.join("src/front.ts").exists());
    link_ts_sdk(&dir, &sdk);
    counts_without_a_front(&dev(temp, dir));
}

#[test]
fn a_typescript_actor_with_a_front_builds_and_dev_serves_it() {
    let Some(sdk) = ts_sdk() else { return };
    let (temp, dir) = generate(&["--lang", "typescript", "--front"]);
    link_ts_sdk(&dir, &sdk);
    counts_through_the_front(&dev(temp, dir));
}

#[test]
fn the_app_comes_from_the_flag() {
    let temp = tempfile::tempdir().unwrap();
    let (ok, out) = new(
        temp.path(),
        &["shop", "--lang", "rust", "--app", "acme/store"],
    );
    assert!(ok, "{out}");
    let file = read(&temp.path().join("shop"), "wasmkeep.toml");
    assert!(
        file.contains("tenant = \"acme\"\nname = \"store\""),
        "{file}"
    );
}

#[test]
fn a_directory_that_is_not_empty_or_a_bad_name_is_refused_and_nothing_is_written() {
    let temp = tempfile::tempdir().unwrap();
    let busy = temp.path().join("busy");
    std::fs::create_dir(&busy).unwrap();
    std::fs::write(busy.join("notes.txt"), "mine").unwrap();
    let (ok, out) = new(temp.path(), &["busy", "--lang", "rust"]);
    assert!(!ok, "{out}");
    assert!(out.contains("not empty"), "{out}");
    let left: Vec<_> = std::fs::read_dir(&busy).unwrap().collect();
    assert_eq!(left.len(), 1, "{out}");

    // A file is not a directory.
    std::fs::write(temp.path().join("file"), "").unwrap();
    let (ok, out) = new(temp.path(), &["file", "--lang", "rust"]);
    assert!(!ok, "{out}");

    // An app name a project file would refuse, given or taken from the
    // directory's name.
    for (dir, app) in [
        ("a", Some("Acme/x")),
        ("b", Some("acme")),
        ("c", Some("acme/_front")),
        ("9lives", None),
    ] {
        let mut args = vec![dir, "--lang", "rust"];
        if let Some(app) = app {
            args.extend(["--app", app]);
        }
        let (ok, out) = new(temp.path(), &args);
        assert!(!ok, "{args:?}: {out}");
        assert!(!temp.path().join(dir).exists(), "{args:?}: {out}");
    }

    // An empty directory is fine.
    std::fs::create_dir(temp.path().join("empty")).unwrap();
    let (ok, out) = new(temp.path(), &["empty", "--lang", "typescript"]);
    assert!(ok, "{out}");
    assert!(temp.path().join("empty/wasmkeep.toml").exists());
}
