//! `wasmkeep deploy` and `wasmkeep dev` with a project file (ADR 0026),
//! without a bucket: what is refused before the bucket is opened (flags
//! beside the file, typos, a failed build, a rollback running a build),
//! and `dev` running examples/todo-front's own wasmkeep.toml end to end.
//! Deploys from a file against a bucket are in deploy_config.rs.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};

mod support;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

/// A bucket nothing resolves: reaching it fails, so a test passing proves
/// the command stopped before.
const NO_BUCKET: &str = "s3://unreachable.invalid/p";

fn wasmkeep(dir: &Path, args: &[&str]) -> (bool, String) {
    let Output {
        status,
        stdout,
        stderr,
    } = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .current_dir(dir)
        .args(args)
        // Fail fast if a bucket is reached after all.
        .env("AWS_ACCESS_KEY_ID", "test")
        .env("AWS_SECRET_ACCESS_KEY", "test")
        .env("AWS_REGION", "us-east-1")
        .output()
        .unwrap();
    (
        status.success(),
        format!(
            "{}{}",
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        ),
    )
}

/// Whether the command got as far as the bucket (and failed there: no
/// bucket answers for [`NO_BUCKET`]).
fn reached_the_bucket(out: &str) -> bool {
    out.contains(&format!("Error: bucket {NO_BUCKET}: "))
}

fn project(text: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("wasmkeep.toml"), text).unwrap();
    dir
}

const APP: &str = "[app]\ntenant = \"acme\"\nname = \"shop\"\n";

#[test]
fn flags_beside_the_file_are_refused() {
    let dir = project(&format!("{APP}[classes.c]\ncomponent = \"c.wasm\"\n"));
    for flags in [
        &["--actor", "c=c.wasm"][..],
        &["--access", "c=open"],
        &["--config", "c=c.env"],
        &["--requires", "c=A"],
        &["--front", "f.wasm"],
        &["--no-front"],
        &["--front-config", "f.env"],
        &["--front-requires", "A"],
    ] {
        let mut args = vec!["deploy", "--bucket", NO_BUCKET];
        args.extend_from_slice(flags);
        let (ok, out) = wasmkeep(dir.path(), &args);
        assert!(!ok, "{flags:?}: {out}");
        assert!(out.contains(flags[0]), "{flags:?}: {out}");
        assert!(out.contains("--no-project"), "{flags:?}: {out}");
        assert!(
            !out.contains("unreachable.invalid"),
            "the bucket was reached: {out}"
        );
    }
    // Another app than the file's.
    let (ok, out) = wasmkeep(
        dir.path(),
        &["deploy", "--bucket", NO_BUCKET, "--app", "acme/other"],
    );
    assert!(!ok && out.contains("not the app of"), "{out}");
    // dev: crate directories, --access and --env beside the file.
    for args in [
        &["dev", "some-crate"][..],
        &["dev", "--access", "c=open"],
        &["dev", "--env", "c=x.env"],
    ] {
        let (ok, out) = wasmkeep(dir.path(), args);
        assert!(!ok, "{args:?}: {out}");
        assert!(out.contains("--no-project"), "{args:?}: {out}");
    }
}

#[test]
fn a_typo_fails_with_its_line_and_column() {
    let dir = project(&format!(
        "{APP}[classes.c]\ncomponent = \"c.wasm\"\nacess = \"open\"\n"
    ));
    let (ok, out) = wasmkeep(dir.path(), &["deploy", "--bucket", NO_BUCKET]);
    assert!(!ok, "{out}");
    assert!(
        out.contains("wasmkeep.toml:6:1: unknown key \"acess\" in [classes.c]"),
        "{out}"
    );
    // --project names a directory or a file.
    let elsewhere = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    let (_, out) = wasmkeep(
        elsewhere.path(),
        &["deploy", "--bucket", NO_BUCKET, "--project", path],
    );
    assert!(out.contains("unknown key \"acess\""), "{out}");
    // Without a file, --app is required.
    let (ok, out) = wasmkeep(elsewhere.path(), &["deploy", "--bucket", NO_BUCKET]);
    assert!(!ok && out.contains("give --app"), "{out}");
}

#[test]
fn a_failed_build_stops_before_the_bucket_and_a_rollback_builds_nothing() {
    let dir = project(&format!(
        "{APP}[classes.c]\ncomponent = \"c.wasm\"\nbuild = \"touch built && exit 7\"\n"
    ));
    let (ok, out) = wasmkeep(dir.path(), &["deploy", "--bucket", NO_BUCKET]);
    assert!(!ok, "{out}");
    assert!(out.contains("the build of class c failed"), "{out}");
    assert!(
        !out.contains("unreachable.invalid"),
        "the bucket was reached: {out}"
    );
    assert!(
        dir.path().join("built").exists(),
        "the build ran in the file's directory"
    );
    std::fs::remove_file(dir.path().join("built")).unwrap();

    // A rollback reads [app] only: it reaches for the bucket (and fails
    // there), without building.
    let (ok, out) = wasmkeep(dir.path(), &["deploy", "--bucket", NO_BUCKET, "--rollback"]);
    assert!(!ok, "{out}");
    assert!(reached_the_bucket(&out), "{out}");
    assert!(
        !dir.path().join("built").exists(),
        "a rollback ran a build: {out}"
    );
    assert!(!out.contains("the build of"), "{out}");

    // A mistyped bucket fails before the build.
    let (ok, out) = wasmkeep(dir.path(), &["deploy", "--bucket", "nonsense"]);
    assert!(!ok && out.contains("--bucket"), "{out}");
    assert!(
        !dir.path().join("built").exists(),
        "built before the bucket was checked"
    );

    // A rollback reads [app] only: a typo elsewhere in the file (which may
    // describe a version not deployed yet) does not block it.
    std::fs::write(
        dir.path().join("wasmkeep.toml"),
        format!(
            "{APP}[classes.c]\ncomponent = \"c.wasm\"\nacess = \"x\"\nbuild = \"touch built\"\n"
        ),
    )
    .unwrap();
    let (ok, out) = wasmkeep(dir.path(), &["deploy", "--bucket", NO_BUCKET, "--rollback"]);
    assert!(!ok, "{out}");
    assert!(reached_the_bucket(&out), "{out}");
    assert!(
        !out.contains("acess"),
        "the rest of the file was read: {out}"
    );
    assert!(!dir.path().join("built").exists(), "{out}");
    // Not TOML: refused before the bucket, saying how to roll back
    // without the file.
    std::fs::write(
        dir.path().join("wasmkeep.toml"),
        format!("{APP}[classes.c\n"),
    )
    .unwrap();
    let (ok, out) = wasmkeep(dir.path(), &["deploy", "--bucket", NO_BUCKET, "--rollback"]);
    assert!(!ok, "{out}");
    assert!(!reached_the_bucket(&out), "{out}");
    assert!(
        out.contains("or pass --no-project --app TENANT/APP --rollback"),
        "{out}"
    );
    std::fs::write(
        dir.path().join("wasmkeep.toml"),
        format!("{APP}[classes.c]\ncomponent = \"c.wasm\"\nbuild = \"touch built && exit 7\"\n"),
    )
    .unwrap();
    // --no-build without a file has nothing to skip.
    let elsewhere = tempfile::tempdir().unwrap();
    let (ok, out) = wasmkeep(
        elsewhere.path(),
        &[
            "deploy",
            "--bucket",
            NO_BUCKET,
            "--app",
            "acme/shop",
            "--actor",
            "c=c.wasm",
            "--no-build",
        ],
    );
    assert!(!ok && out.contains("--no-build"), "{out}");

    // --no-build skips the build; the missing component is then the error.
    let (ok, out) = wasmkeep(dir.path(), &["deploy", "--bucket", NO_BUCKET, "--no-build"]);
    assert!(!ok, "{out}");
    assert!(!dir.path().join("built").exists(), "{out}");
    assert!(out.contains("c.wasm"), "{out}");
}

/// A build's output goes to stderr (stdout is the plan), without the
/// deploy's AWS credentials; a missing configuration file stops the deploy
/// before any build runs.
#[test]
fn builds_print_to_stderr_without_the_bucket_credentials() {
    let dir = project(&format!(
        "{APP}[classes.c]\ncomponent = \"c.wasm\"\n\
         build = \"echo out-$AWS_ACCESS_KEY_ID-$AWS_SECRET_ACCESS_KEY-$AWS_SESSION_TOKEN-$AWS_REGION; exit 7\"\n"
    ));
    let out = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .current_dir(dir.path())
        .args(["deploy", "--bucket", NO_BUCKET])
        .env("AWS_ACCESS_KEY_ID", "key-id")
        .env("AWS_SECRET_ACCESS_KEY", "hunter2")
        .env("AWS_SESSION_TOKEN", "session")
        .env("AWS_REGION", "us-east-1")
        .output()
        .unwrap();
    assert!(!out.status.success());
    let (stdout, stderr) = (
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        !stdout.contains("out-"),
        "the build wrote to stdout: {stdout}"
    );
    assert!(stderr.contains("out----us-east-1"), "{stderr}");
    assert!(stderr.contains("the build of class c failed"), "{stderr}");

    // A configuration file that is not there: before the build.
    let dir = project(&format!(
        "{APP}[classes.c]\ncomponent = \"c.wasm\"\nconfig = \"missing.env\"\n\
         build = \"touch built\"\n"
    ));
    let (ok, out) = wasmkeep(dir.path(), &["deploy", "--bucket", NO_BUCKET]);
    assert!(!ok, "{out}");
    assert!(out.contains("missing.env"), "{out}");
    assert!(!dir.path().join("built").exists(), "built first: {out}");
    assert!(!out.contains("unreachable.invalid"), "{out}");
}

// `wasmkeep dev` on examples/todo-front's wasmkeep.toml.

const ALICE: &str = "0123456789abcdef0123456789abcdef";

struct Dev {
    child: Child,
    addr: SocketAddr,
    dir: tempfile::TempDir,
}

impl Dev {
    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.path().join("dev.log")).unwrap_or_default()
    }
}

impl Drop for Dev {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &str,
    body: &str,
) -> Option<(u16, String)> {
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(30))).ok()?;
    write!(
        s,
        "{method} {path} HTTP/1.1\r\nHost: todo.example\r\nConnection: close\r\n\
         {headers}Content-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .ok()?;
    let mut raw = String::new();
    s.read_to_string(&mut raw).ok()?;
    let status = raw.split_whitespace().nth(1)?.parse().ok()?;
    let body = raw.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    Some((status, body.to_owned()))
}

/// The example's own file, unchanged, in a directory of its own whose
/// `front` and `todos` are the example's crates: `.env.local` and the
/// databases stay out of the source tree.
fn dev_on_the_example() -> Dev {
    let example = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/todo-front");
    let example = example.canonicalize().unwrap();
    let dir = tempfile::tempdir().unwrap();
    for part in ["front", "todos"] {
        std::os::unix::fs::symlink(example.join(part), dir.path().join(part)).unwrap();
    }
    std::fs::copy(
        example.join("wasmkeep.toml"),
        dir.path().join("wasmkeep.toml"),
    )
    .unwrap();
    std::fs::write(
        dir.path().join(".env.local"),
        format!("TOKENS=alice:{ALICE}\n"),
    )
    .unwrap();
    let log_path = dir.path().join("dev.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .current_dir(dir.path())
        .args(["dev", "--listen", "127.0.0.1:0", "--poll-ms", "100"])
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .stdin(Stdio::null())
        .spawn()
        .unwrap();
    // Builds both crates first: a cold build takes minutes.
    let addr = support::listening_on(&mut child, &log_path, Duration::from_secs(600));
    let dev = Dev { child, addr, dir };
    let started = Instant::now();
    while request(addr, "GET", "/", "", "").is_none() {
        assert!(
            started.elapsed() < Duration::from_secs(600),
            "dev never answered: {}",
            dev.log()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    dev
}

#[test]
fn dev_runs_the_example_file_with_its_front_rules_and_local_values() {
    let dev = dev_on_the_example();
    let auth = format!("Authorization: Bearer {ALICE}\r\n");
    let (status, body) = request(dev.addr, "POST", "/api/todos", &auth, "buy milk").unwrap();
    assert_eq!(status, 201, "{body} {}", dev.log());
    assert!(body.contains("\"id\":1"), "{body}");
    let (status, body) = request(dev.addr, "GET", "/api/todos", &auth, "").unwrap();
    assert_eq!(status, 200);
    assert!(body.contains("buy milk"), "{body}");
    // Without the token: the front refuses.
    assert_eq!(
        request(dev.addr, "GET", "/api/todos", "", "").unwrap().0,
        401
    );
    // The class's rule is the file's default: internal.
    let (status, _) = request(dev.addr, "GET", "/actor/todos/alice", "", "").unwrap();
    assert_eq!(status, 403, "{}", dev.log());
    // The databases are beside the file, under the file's app.
    assert!(dev.dir.path().join(".wasmkeep/dev").exists());

    // A component written again (as a build run elsewhere would) reloads
    // the whole app; the actor's state is kept.
    let front = dev
        .dir
        .path()
        .join("front/target/wasm32-wasip2/release/todo_front.wasm");
    let bytes = std::fs::read(&front).unwrap();
    std::thread::sleep(Duration::from_millis(1100));
    std::fs::write(&front, bytes).unwrap();
    let started = Instant::now();
    while !dev.log().contains("reloaded") {
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "no reload: {}",
            dev.log()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let (status, body) = request(dev.addr, "GET", "/api/todos", &auth, "").unwrap();
    assert_eq!(status, 200);
    assert!(body.contains("buy milk"), "{body}");
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

/// Waits until `done` holds for `dev`'s directory and log.
fn wait_for(dev: &Dev, what: &str, done: impl Fn(&Path, &str) -> bool) {
    let started = Instant::now();
    while !done(dev.dir.path(), &dev.log()) {
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "{what}: {}",
            dev.log()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// `wasmkeep dev` runs an item's build again when its `watch` paths
/// change and reloads; a failed build keeps the previous version serving
/// and holds the reload until the item builds again.
#[test]
fn dev_rebuilds_on_watch_and_a_failed_build_keeps_the_previous_version() {
    let wasm = counter_wasm();
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("src")).unwrap();
    std::fs::write(dir.path().join("src/lib"), "1").unwrap();
    std::fs::write(
        dir.path().join("wasmkeep.toml"),
        format!(
            "{APP}[classes.c]\ncomponent = \"c.wasm\"\naccess = \"open\"\nwatch = [\"src\"]\n\
             build = \"test ! -e fail || exit 7; cp '{}' c.wasm; echo b >> builds.txt\"\n",
            wasm.display()
        ),
    )
    .unwrap();
    let log_path = dir.path().join("dev.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .current_dir(dir.path())
        .args(["dev", "--listen", "127.0.0.1:0", "--poll-ms", "50"])
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .stdin(Stdio::null())
        .spawn()
        .unwrap();
    let addr = support::listening_on(&mut child, &log_path, Duration::from_secs(60));
    let dev = Dev { child, addr, dir };
    let builds = |d: &Path| {
        std::fs::read_to_string(d.join("builds.txt"))
            .unwrap_or_default()
            .lines()
            .count()
    };
    let reloads = |log: &str| log.matches("reloaded").count();
    let edit = |n: u64| {
        let path = dev.dir.path().join("src/lib");
        std::fs::write(&path, n.to_string()).unwrap();
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(std::time::SystemTime::now() + Duration::from_secs(10 * n))
            .unwrap();
    };
    assert_eq!(builds(dev.dir.path()), 1, "{}", dev.log());
    assert_eq!(request(addr, "GET", "/actor/c/x", "", "").unwrap().0, 200);

    // An edit under watch: built again, reloaded.
    edit(1);
    wait_for(&dev, "a rebuild and reload", |d, log| {
        builds(d) == 2 && reloads(log) == 1
    });

    // A failed build: the previous version keeps serving.
    std::fs::write(dev.dir.path().join("fail"), "").unwrap();
    edit(2);
    wait_for(&dev, "the build failing", |_, log| {
        log.contains("build failed")
    });
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(reloads(&dev.log()), 1, "{}", dev.log());
    assert_eq!(request(addr, "GET", "/actor/c/x", "", "").unwrap().0, 200);

    // Fixed: it builds and reloads.
    std::fs::remove_file(dev.dir.path().join("fail")).unwrap();
    edit(3);
    wait_for(&dev, "the fixed build and reload", |d, log| {
        builds(d) == 3 && reloads(log) == 2
    });
    assert_eq!(request(addr, "GET", "/actor/c/x", "", "").unwrap().0, 200);
}
