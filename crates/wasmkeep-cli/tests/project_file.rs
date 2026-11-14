//! `wasmkeep deploy` and `wasmkeep dev` with a project file (ADR 0026),
//! without a bucket: what is refused before the bucket is opened (flags
//! beside the file, typos, a failed build, a rollback running a build),
//! and `dev` running examples/todo-front's own wasmkeep.toml end to end.
//! Deploys from a file against a bucket are in deploy_config.rs.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
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
    assert!(
        !dir.path().join("built").exists(),
        "a rollback ran a build: {out}"
    );
    assert!(!out.contains("the build of"), "{out}");

    // --no-build skips the build; the missing component is then the error.
    let (ok, out) = wasmkeep(dir.path(), &["deploy", "--bucket", NO_BUCKET, "--no-build"]);
    assert!(!ok, "{out}");
    assert!(!dir.path().join("built").exists(), "{out}");
    assert!(out.contains("c.wasm"), "{out}");
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
    let addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let log = std::fs::File::create(dir.path().join("dev.log")).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .current_dir(dir.path())
        .args(["dev", "--listen", &addr.to_string(), "--poll-ms", "100"])
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .stdin(Stdio::null())
        .spawn()
        .unwrap();
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
