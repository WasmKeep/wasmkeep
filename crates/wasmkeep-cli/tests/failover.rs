//! Failover: kill the owner, another node restores and serves within
//! 30 s, no committed write lost. Two real `wasmkeep serve` processes, the
//! production lease timings (ttl 10 s, grace 5 s), `kill -9`.
//!
//! Needs an S3 API: skipped unless `WASMKEEP_TEST_S3_BUCKET` names a bucket;
//! endpoint and credentials come from `AWS_*` (see `.github/workflows/ci.yml`).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Node {
    child: Child,
    addr: SocketAddr,
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

fn start(id: &str, dir: &Path, wasm: &Path, bucket: &str) -> Node {
    let addr = free_port();
    let log = std::fs::File::create(dir.join(format!("{id}.log"))).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .args(["serve", "--actor"])
        .arg(format!("counter={}", wasm.display()))
        .arg("--data-dir")
        .arg(dir.join(id))
        .args(["--listen", &addr.to_string(), "--node-id", id])
        .args(["--advertise", &format!("http://{addr}"), "--bucket", bucket])
        // Plain HTTP between nodes, on loopback (the TLS cluster is
        // tests/cluster_tls.rs).
        .args(["--cluster-plaintext", "accept"])
        .env("RUST_LOG", "wasmkeep_server=info")
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .unwrap();
    Node { child, addr }
}

/// (status, x-wasmkeep-node, body); `None` if the node does not answer.
fn send(addr: SocketAddr, method: &str, path: &str) -> Option<(u16, Option<String>, String)> {
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(30))).ok()?;
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    s.write_all(req.as_bytes()).ok()?;
    let mut raw = String::new();
    s.read_to_string(&mut raw).ok()?;
    let (head, body) = raw.split_once("\r\n\r\n")?;
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

fn wait_until<T>(limit: Duration, mut f: impl FnMut() -> Option<T>) -> Option<(T, Duration)> {
    let started = Instant::now();
    while started.elapsed() < limit {
        if let Some(v) = f() {
            return Some((v, started.elapsed()));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    None
}

#[test]
fn killing_the_owner_loses_no_committed_write() {
    let Ok(bucket) = std::env::var("WASMKEEP_TEST_S3_BUCKET") else {
        eprintln!("WASMKEEP_TEST_S3_BUCKET not set; skipping");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let wasm = counter_wasm();
    let prefix = format!(
        "failover-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let url = format!("s3://{bucket}/{prefix}");
    let a = start("a", dir.path(), &wasm, &url);
    let b = start("b", dir.path(), &wasm, &url);
    let up = |n: &Node| wait_until(Duration::from_secs(30), || send(n.addr, "GET", "/healthz"));
    up(&a).expect("a did not start");
    up(&b).expect("b did not start");

    for i in 1..=5 {
        let (status, node, body) = send(a.addr, "POST", "/actor/counter/x").unwrap();
        assert_eq!(
            (status, node.as_deref(), body.as_str()),
            (200, Some("a"), i.to_string().as_str())
        );
    }
    // Plain writes replicate when their event ends; give the last one a
    // moment (durable writes need no wait, see tests in wasmkeep-server).
    std::thread::sleep(Duration::from_secs(1));

    let mut a = a;
    a.child.kill().unwrap(); // SIGKILL: no release, no clean shutdown
    a.child.wait().unwrap();
    let killed = Instant::now();

    let served = wait_until(Duration::from_secs(60), || {
        match send(b.addr, "GET", "/actor/counter/x") {
            Some((200, Some(node), body)) if node == "b" => Some(body),
            _ => None,
        }
    });
    let log = std::fs::read_to_string(dir.path().join("b.log")).unwrap_or_default();
    let (body, _) = served.unwrap_or_else(|| panic!("b never served the actor; its log:\n{log}"));
    let took = killed.elapsed();
    eprintln!("failover: b served the actor {took:?} after the owner was killed");
    // Read by bench/run.sh.
    eprintln!("failover_ms={}", took.as_millis());
    assert_eq!(body, "5", "every committed write survived");
    assert!(took < Duration::from_secs(30), "failover took {took:?}");

    let (status, node, body) = send(b.addr, "POST", "/actor/counter/x").unwrap();
    assert_eq!(
        (status, node.as_deref(), body.as_str()),
        (200, Some("b"), "6")
    );
}
