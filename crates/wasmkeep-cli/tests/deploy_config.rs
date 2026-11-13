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
use std::process::{Child, Command, Output};
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
    deploy_app(url, "acme/shop", args)
}

fn deploy_app(url: &str, app: &str, args: &[&str]) -> (bool, String, String) {
    let Output {
        status,
        stdout,
        stderr,
    } = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .args(["deploy", "--app", app, "--bucket", url])
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
    // Every deploy of this version marks the app (token 3, ADR 0022): rules
    // come with deploys from the first one, and --access is refused.
    assert!(out.contains("Access rules now come with deploys"), "{out}");
    let early = serve(
        &url,
        &dir.path().join("early"),
        free_port(),
        &["--access", "counter=existing"],
    )
    .output()
    .unwrap();
    assert!(!early.status.success());
    let err = String::from_utf8(early.stderr).unwrap();
    assert!(err.contains("wasmkeep deploy --app acme/shop"), "{err}");

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
    assert!(!out.contains("Access rules now come with deploys"), "{out}");
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

/// Writes `body` at `key` under the bucket prefix.
fn put(bucket: &str, key: &str, body: &str) {
    use object_store::ObjectStoreExt as _;
    use object_store::path::Path as StorePath;
    let store = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .build()
        .unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(store.put(&StorePath::from(key), body.to_owned().into()))
        .unwrap();
}

/// The current of `app` under the prefix.
fn current(bucket: &str, prefix: &str, app: &str) -> String {
    use object_store::ObjectStoreExt as _;
    use object_store::path::Path as StorePath;
    let store = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .build()
        .unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let r = store
                .get(&StorePath::from(format!("{prefix}/apps/{app}/current")))
                .await
                .unwrap();
            String::from_utf8(r.bytes().await.unwrap().to_vec()).unwrap()
        })
}

/// Apps of one tenant share a prefix (ADR 0022); a `current` from before
/// 0.4.0 is refused by `serve --app`; a migration in progress refuses
/// deploys and starts.
#[test]
fn apps_of_a_tenant_share_a_prefix_and_old_currents_are_refused() {
    let Ok(bucket) = std::env::var("WASMKEEP_TEST_S3_BUCKET") else {
        eprintln!("WASMKEEP_TEST_S3_BUCKET not set; skipping");
        return;
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let prefix = format!("two-apps-{}-{now}", std::process::id());
    let url = format!("s3://{bucket}/{prefix}");
    let actor = format!("counter={}", counter_wasm().display());

    for app in ["acme/shop", "acme/admin", "acme/blog"] {
        let (ok, _, err) = deploy_app(&url, app, &["--actor", &actor]);
        assert!(ok, "{app}: {err}");
        assert!(!err.contains("apps of one tenant"), "{err}");
        let body = current(&bucket, &prefix, app);
        assert!(body.trim_end().ends_with(" 3"), "{app}: {body}");
    }
    let (ok, _, err) = deploy_app(&url, "acme-2/Admin", &["--actor", &actor]);
    assert!(!ok && err.contains("[a-z][a-z0-9-]{0,63}"), "{err}");

    // A current as 0.3 wrote it: refused at start, naming the command.
    let body = current(&bucket, &prefix, "acme/blog");
    let old = body.trim_end().trim_end_matches(" 3").to_owned() + " 2\n";
    put(&bucket, &format!("{prefix}/apps/acme/blog/current"), &old);
    let dir = tempfile::tempdir().unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .args(["serve", "--app", "acme/blog", "--bucket", &url])
        .arg("--data-dir")
        .arg(dir.path().join("data"))
        .args(["--listen", &free_port().to_string()])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.contains("wasmkeep cluster migrate --bucket"), "{err}");

    // A migration in progress: deploys and starts refused.
    put(&bucket, &format!("{prefix}/cluster/migration"), "lock\n");
    let (ok, _, err) = deploy_app(&url, "acme/shop", &["--actor", &actor]);
    assert!(!ok && err.contains("in progress"), "{err}");
    let out = serve(&url, &dir.path().join("shop"), free_port(), &[])
        .output()
        .unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8(out.stderr).unwrap();
    assert!(err.contains("in progress"), "{err}");
}
