//! `wasmkeep serve --cluster-listen`: two real nodes reach each other over
//! the cluster listener (mutual TLS, ADR 0011), and the flags are checked.
//!
//! The two-node test needs an S3 API: skipped unless
//! `WASMKEEP_TEST_S3_BUCKET` names a bucket; endpoint and credentials come
//! from `AWS_*` (see `.github/workflows/ci.yml`). The flag checks need none.

#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

struct Node {
    child: Child,
    addr: SocketAddr,
    cluster: SocketAddr,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
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

/// A node with a cluster listener whose published plain address is a port
/// nobody listens on: anything another node sends it must go over TLS.
/// `extra` arguments follow.
fn start(id: &str, dir: &Path, wasm: &Path, bucket: &str, extra: &[&str]) -> Node {
    let (addr, cluster, nowhere) = (free_port(), free_port(), free_port());
    let log = std::fs::File::create(dir.join(format!("{id}.log"))).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .args(["serve", "--actor"])
        .arg(format!("counter={}", wasm.display()))
        .arg("--data-dir")
        .arg(dir.join(id))
        .args(["--listen", &addr.to_string(), "--node-id", id])
        .args([
            "--advertise",
            &format!("http://{nowhere}"),
            "--bucket",
            bucket,
        ])
        .args(["--cluster-listen", &cluster.to_string()])
        .args(["--cluster-advertise", &format!("https://{cluster}")])
        .args(extra)
        .env("RUST_LOG", "wasmkeep=info,wasmkeep_server=info")
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    Node {
        child,
        addr,
        cluster,
    }
}

/// (status, x-wasmkeep-node, body); `None` if no HTTP response comes back.
fn send(addr: SocketAddr, method: &str, path: &str) -> Option<(u16, Option<String>, String)> {
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(30))).ok()?;
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    s.write_all(req.as_bytes()).ok()?;
    let mut raw = Vec::new();
    let _ = s.read_to_end(&mut raw);
    let raw = String::from_utf8(raw).ok()?;
    let (head, body) = raw.split_once("\r\n\r\n")?;
    if !head.starts_with("HTTP/1.1 ") {
        return None;
    }
    let status = head.split_whitespace().nth(1)?.parse().ok()?;
    let node = head.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.eq_ignore_ascii_case("x-wasmkeep-node")
            .then(|| v.trim().to_owned())
    });
    // Small bodies, either plain or one chunk.
    let body = match body.split_once("\r\n") {
        Some((size, rest)) if usize::from_str_radix(size.trim(), 16).is_ok() => {
            rest.split("\r\n").next().unwrap_or("").to_owned()
        }
        _ => body.to_owned(),
    };
    Some((status, node, body.trim().to_owned()))
}

fn wait_until<T>(limit: Duration, mut f: impl FnMut() -> Option<T>) -> Option<T> {
    let started = Instant::now();
    while started.elapsed() < limit {
        if let Some(v) = f() {
            return Some(v);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    None
}

fn log_of(dir: &Path, id: &str) -> String {
    let path = dir.join(format!("{id}.log"));
    String::from_utf8_lossy(&std::fs::read(&path).unwrap_or_default()).into_owned()
}

#[test]
fn two_nodes_relay_over_the_cluster_listener() {
    let Ok(bucket) = std::env::var("WASMKEEP_TEST_S3_BUCKET") else {
        eprintln!("WASMKEEP_TEST_S3_BUCKET not set; skipping");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let wasm = counter_wasm();
    let prefix = format!(
        "cluster-tls-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let url = format!("s3://{bucket}/{prefix}");
    let a = start(
        "a",
        dir.path(),
        &wasm,
        &url,
        &["--cluster-plaintext", "accept"],
    );
    // a still accepts plain HTTP between nodes (moving a cluster to TLS);
    // b refuses it, the default (it reaches a over TLS anyway).
    let b = start("b", dir.path(), &wasm, &url, &[]);
    // Ready means each node's identity has been published for a while.
    for (id, n) in [("a", &a), ("b", &b)] {
        wait_until(Duration::from_secs(60), || {
            matches!(send(n.addr, "GET", "/_wasmkeep/ready"), Some((200, _, _))).then_some(())
        })
        .unwrap_or_else(|| panic!("{id} never became ready:\n{}", log_of(dir.path(), id)));
    }

    // a owns the actor.
    let (status, node, body) = send(a.addr, "POST", "/actor/counter/x").unwrap();
    assert_eq!(
        (status, node.as_deref(), body.as_str()),
        (200, Some("a"), "1")
    );

    // b relays to a. a's plain address leads nowhere, so this can only
    // have reached a over its cluster listener.
    let relayed = wait_until(Duration::from_secs(30), || {
        match send(b.addr, "POST", "/actor/counter/x") {
            Some((200, node, body)) => Some((node, body)),
            _ => None,
        }
    })
    .unwrap_or_else(|| {
        panic!(
            "b never relayed to a:\n--- a\n{}\n--- b\n{}",
            log_of(dir.path(), "a"),
            log_of(dir.path(), "b")
        )
    });
    assert_eq!(relayed, (Some("a".to_owned()), "2".to_owned()));
    let (status, node, body) = send(a.addr, "GET", "/actor/counter/x").unwrap();
    assert_eq!(
        (status, node.as_deref(), body.as_str()),
        (200, Some("a"), "2")
    );

    // The cluster port speaks TLS only: plain HTTP gets no response.
    assert!(send(a.cluster, "GET", "/_wasmkeep/health").is_none());
    let log = log_of(dir.path(), "a");
    assert!(log.contains("cluster listener on https://"), "{log}");
    assert!(!log.contains("plain HTTP between nodes refused"), "{log}");
    let log = log_of(dir.path(), "b");
    assert!(log.contains("plain HTTP between nodes refused"), "{log}");

    // SIGTERM stops both ports.
    let mut a = a;
    let status = Command::new("kill")
        .args(["-TERM", &a.child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let exited = wait_until(Duration::from_secs(20), || a.child.try_wait().unwrap());
    let exited = exited.expect("a still running 20 s after SIGTERM");
    assert!(exited.success(), "{exited}: {}", log_of(dir.path(), "a"));
    assert!(TcpStream::connect_timeout(&a.cluster, Duration::from_secs(1)).is_err());
}

/// `wasmkeep serve` with `args`: its exit status and stderr.
fn serve_fails(args: &[&str]) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .arg("serve")
        .args(args)
        .output()
        .unwrap();
    assert!(!out.status.success(), "serve {args:?} succeeded");
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn cluster_listener_flags_are_checked() {
    let cluster = [
        "--actor",
        "counter=counter.wasm",
        "--node-id",
        "a",
        "--advertise",
        "http://127.0.0.1:8080",
        "--bucket",
        "s3://b",
    ];
    let with = |extra: &[&'static str]| {
        let mut v = cluster.to_vec();
        v.extend_from_slice(extra);
        v
    };

    // Advertise without listen, and listen without advertise.
    let e = serve_fails(&with(&["--cluster-advertise", "https://127.0.0.1:7443"]));
    assert!(e.contains("--cluster-listen"), "{e}");
    let e = serve_fails(&with(&["--cluster-listen", "127.0.0.1:7443"]));
    assert!(e.contains("--cluster-advertise"), "{e}");

    // A cluster node without a cluster listener: plain HTTP between nodes
    // is refused by default, whether or not the flag says so.
    for extra in [&[][..], &["--cluster-plaintext", "refuse"][..]] {
        let e = serve_fails(&with(extra));
        assert!(
            e.contains("--cluster-listen") && e.contains("--cluster-plaintext accept"),
            "{e}"
        );
    }

    // A plain address.
    let e = serve_fails(&with(&[
        "--cluster-listen",
        "127.0.0.1:7443",
        "--cluster-advertise",
        "http://127.0.0.1:7443",
    ]));
    assert!(e.contains("https://"), "{e}");

    // Without a cluster.
    let e = serve_fails(&[
        "--actor",
        "counter=counter.wasm",
        "--cluster-listen",
        "127.0.0.1:7443",
        "--cluster-advertise",
        "https://127.0.0.1:7443",
    ]);
    assert!(e.contains("--node-id"), "{e}");

    // Without actors: the cluster port serves nothing else.
    let e = serve_fails(&[
        "hello.wasm",
        "--node-id",
        "a",
        "--advertise",
        "http://127.0.0.1:8080",
        "--bucket",
        "s3://b",
        "--cluster-listen",
        "127.0.0.1:7443",
        "--cluster-advertise",
        "https://127.0.0.1:7443",
    ]);
    assert!(e.contains("--cluster-listen needs actors"), "{e}");
}
