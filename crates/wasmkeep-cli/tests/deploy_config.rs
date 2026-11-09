//! `wasmkeep deploy --access/--config` and `serve --app`, run as binaries
//! against a real S3 API (ADR 0013).
//!
//! Skipped unless `WASMKEEP_TEST_S3_BUCKET` names a bucket; endpoint and
//! credentials come from `AWS_*` (see `.github/workflows/ci.yml`). Each run
//! writes under its own prefix.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

fn deploy(url: &str, args: &[&str]) -> (bool, String, String) {
    let Output {
        status,
        stdout,
        stderr,
    } = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .args(["deploy", "--app", "acme/shop", "--bucket", url])
        .args(args)
        .output()
        .unwrap();
    (
        status.success(),
        String::from_utf8(stdout).unwrap(),
        String::from_utf8(stderr).unwrap(),
    )
}

fn free_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn serve(url: &str, dir: &Path, addr: SocketAddr, extra: &[&str]) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_wasmkeep"));
    cmd.args(["serve", "--app", "acme/shop", "--bucket", url])
        .arg("--data-dir")
        .arg(dir)
        .args(["--listen", &addr.to_string()])
        .args(extra);
    cmd
}

fn status(addr: SocketAddr, path: &str) -> Option<u16> {
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(10))).ok()?;
    let req = format!("GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).ok()?;
    let mut raw = String::new();
    s.read_to_string(&mut raw).ok()?;
    raw.split_whitespace().nth(1)?.parse().ok()
}

struct Killed(Child);
impl Drop for Killed {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn rules_and_config_travel_with_deploys() {
    let Ok(bucket) = std::env::var("WASMKEEP_TEST_S3_BUCKET") else {
        eprintln!("WASMKEEP_TEST_S3_BUCKET not set; skipping");
        return;
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let url = format!("s3://{bucket}/deploy-config-{}-{now}", std::process::id());
    let wasm = counter_wasm();
    let dir = tempfile::tempdir().unwrap();
    let actor = format!("counter={}", wasm.display());

    let (ok, out, err) = deploy(&url, &["--actor", &actor]);
    assert!(ok, "{err}");
    assert!(out.contains("class counter: added"), "{out}");

    // Before rules are deployed, a node with --access starts (migration).
    let addr = free_port();
    let mut early = serve(
        &url,
        &dir.path().join("early"),
        addr,
        &["--access", "counter=existing"],
    );
    let early = Killed(early.stdout(Stdio::null()).spawn().unwrap());
    let started = Instant::now();
    while status(addr, "/actor/counter/x").is_none() {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the node never answered"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(
        status(addr, "/actor/counter/nobody"),
        Some(404),
        "--access applied"
    );
    drop(early);

    // Rules and configuration, with no --actor: the classes are kept.
    let config = dir.path().join("counter.env");
    std::fs::write(
        &config,
        "# API_TOKEN=commented\nAPI_TOKEN=tok-12345\nURL=https://x.test\n",
    )
    .unwrap();
    let (ok, out, err) = deploy(
        &url,
        &[
            "--access",
            "counter=existing",
            "--config",
            &format!("counter={}", config.display()),
        ],
    );
    assert!(ok, "{err}");
    assert!(out.contains("access counter: open -> existing"), "{out}");
    assert!(out.contains("config counter: API_TOKEN added"), "{out}");
    assert!(out.contains("Access rules now come with deploys"), "{out}");
    assert!(err.contains("API_TOKEN looks like a secret"), "{err}");
    assert!(
        !out.contains("tok-12345") && !err.contains("tok-12345"),
        "a value was printed"
    );

    // Rules deployed: --access is refused, with the command to run.
    let refused = serve(
        &url,
        &dir.path().join("late"),
        free_port(),
        &["--access", "counter=open"],
    )
    .output()
    .unwrap();
    assert!(!refused.status.success());
    let err = String::from_utf8(refused.stderr).unwrap();
    assert!(err.contains("wasmkeep deploy --app acme/shop"), "{err}");

    // A node with no --access takes the deployed rules.
    let addr = free_port();
    let log = dir.path().join("node.log");
    let node = serve(&url, &dir.path().join("node"), addr, &[])
        .env("RUST_LOG", "wasmkeep_server=info,wasmkeep=info")
        // The log goes to stdout.
        .stdout(std::fs::File::create(&log).unwrap())
        .spawn()
        .unwrap();
    let _node = Killed(node);
    let started = Instant::now();
    while status(addr, "/actor/counter/x").is_none() {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the node never answered"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(status(addr, "/actor/counter/nobody"), Some(404));
    let logged = std::fs::read_to_string(&log).unwrap();
    assert!(logged.contains("serving the deployed version"), "{logged}");
    assert!(logged.contains("access restricted (deployed)"), "{logged}");
    assert!(
        logged.contains("monitor") || logged.contains("counter"),
        "{logged}"
    );
    assert!(!logged.contains("tok-12345"), "a value was logged");

    // A rollback that reopens the class is refused unless allowed.
    let (ok, _, err) = deploy(&url, &["--rollback"]);
    assert!(!ok);
    assert!(err.contains("counter (existing -> open)"), "{err}");
    let (ok, out, err) = deploy(&url, &["--rollback", "--allow-reopen"]);
    assert!(ok, "{err}");
    assert!(out.contains("access counter: existing -> open"), "{out}");
}
