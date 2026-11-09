//! `wasmkeep serve` stops cleanly on SIGTERM, the signal systemd, Docker and
//! Kubernetes send, as it does on Ctrl-C: in-flight requests finish first.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

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

fn get(addr: SocketAddr, path: &str) -> Option<String> {
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(30))).ok()?;
    write!(
        s,
        "GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut raw = String::new();
    s.read_to_string(&mut raw).ok()?;
    Some(raw)
}

#[test]
fn sigterm_shuts_down_gracefully() {
    let wasm = counter_wasm();
    let dir = tempfile::tempdir().unwrap();
    let addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let log = dir.path().join("serve.log");
    let mut child = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .args(["serve", "--actor"])
        .arg(format!("counter={}", wasm.display()))
        .arg("--data-dir")
        .arg(dir.path().join("data"))
        .arg("--cache-dir")
        .arg(dir.path().join("cache"))
        .args(["--listen", &addr.to_string()])
        .env("RUST_LOG", "info")
        // The CLI logs to stdout.
        .stdout(std::fs::File::create(&log).unwrap())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let started = Instant::now();
    while get(addr, "/actor/counter/a").is_none() {
        assert!(
            started.elapsed() < Duration::from_secs(120),
            "server never answered"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    let killed = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(killed.success());
    let stopped = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if stopped.elapsed() > Duration::from_secs(15) {
            let _ = child.kill();
            panic!("still running 15 s after SIGTERM");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let output = std::fs::read_to_string(&log).unwrap();
    assert!(status.success(), "exit status {status:?}; log:\n{output}");
    assert!(output.contains("shutting down"), "log:\n{output}");
}
