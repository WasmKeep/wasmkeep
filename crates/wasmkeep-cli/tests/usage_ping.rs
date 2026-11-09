//! The usage ping is off unless asked for, and sends only the documented
//! report.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::mpsc;
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

/// A one-shot collector: the raw request it receives, if any.
fn collector() -> (String, mpsc::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/v1/ping", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut raw = Vec::new();
        let mut buf = [0u8; 4096];
        // Read the head, then the body by content-length.
        loop {
            let n = s.read(&mut buf).unwrap_or(0);
            if n == 0 {
                break;
            }
            raw.extend_from_slice(&buf[..n]);
            let text = String::from_utf8_lossy(&raw).to_string();
            if let Some((head, body)) = text.split_once("\r\n\r\n") {
                let len = head
                    .lines()
                    .find_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        k.eq_ignore_ascii_case("content-length")
                            .then(|| v.trim().parse::<usize>().ok())?
                    })
                    .unwrap_or(0);
                if body.len() >= len {
                    break;
                }
            }
        }
        let _ = s.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n");
        let _ = tx.send(String::from_utf8_lossy(&raw).to_string());
    });
    (url, rx)
}

/// Starts `serve`. It binds its own port (`--listen 127.0.0.1:0`): this test
/// never connects to it, and a port picked here and freed before the child
/// binds it can be taken meanwhile by another test (any `bind(0)` or outgoing
/// connection draws from the same ephemeral range). The child then exits at
/// once, before its first report, and the test used to wait out its whole
/// timeout. Its output goes to files in `dir`, shown when a test fails.
fn serve(wasm: &std::path::Path, dir: &std::path::Path, url: &str, ping: bool) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_wasmkeep"));
    cmd.args(["serve", "--actor"])
        .arg(format!("counter={}", wasm.display()))
        .arg("--data-dir")
        .arg(dir.join("data"))
        .arg("--cache-dir")
        .arg(dir.join("cache"))
        .args(["--listen", "127.0.0.1:0"])
        .env("WASMKEEP_USAGE_PING_URL", url)
        .env("WASMKEEP_USAGE_PING_FIRST_SECS", "1")
        .env("RUST_LOG", "info")
        .stdout(std::fs::File::create(dir.join("serve.out")).unwrap())
        .stderr(std::fs::File::create(dir.join("serve.err")).unwrap());
    if ping {
        cmd.arg("--usage-ping");
    }
    cmd.spawn().unwrap()
}

/// The child's output, for failure messages.
fn output(dir: &std::path::Path) -> String {
    let out = std::fs::read_to_string(dir.join("serve.out")).unwrap_or_default();
    let err = std::fs::read_to_string(dir.join("serve.err")).unwrap_or_default();
    format!("stdout:\n{out}\nstderr:\n{err}")
}

/// Waits up to `limit` for a report while the child runs. Panics as soon as
/// the child exits, with its status and output, instead of waiting out
/// `limit` for a report that cannot come.
fn report_within(
    child: &mut Child,
    received: &mpsc::Receiver<String>,
    dir: &std::path::Path,
    limit: Duration,
) -> Option<String> {
    let deadline = Instant::now() + limit;
    loop {
        match received.recv_timeout(Duration::from_millis(100)) {
            Ok(raw) => return Some(raw),
            Err(mpsc::RecvTimeoutError::Disconnected) => panic!("the collector stopped"),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("serve exited ({status}) before a report\n{}", output(dir));
        }
        if Instant::now() >= deadline {
            return None;
        }
    }
}

#[test]
fn nothing_is_sent_without_the_flag() {
    let wasm = counter_wasm();
    let dir = tempfile::tempdir().unwrap();
    let (url, received) = collector();
    let mut child = serve(&wasm, dir.path(), &url, false);
    // Still running at the end, or the silence proves nothing.
    let got = report_within(&mut child, &received, dir.path(), Duration::from_secs(8));
    let _ = child.kill();
    let _ = child.wait();
    assert!(got.is_none(), "sent without --usage-ping: {got:?}");
}

#[test]
fn the_flag_sends_the_documented_report() {
    let wasm = counter_wasm();
    let dir = tempfile::tempdir().unwrap();
    let (url, received) = collector();
    let mut child = serve(&wasm, dir.path(), &url, true);
    let got = report_within(&mut child, &received, dir.path(), Duration::from_secs(60));
    let _ = child.kill();
    let _ = child.wait();
    let raw = got.unwrap_or_else(|| panic!("no report within 60 s\n{}", output(dir.path())));
    assert!(raw.starts_with("POST /v1/ping HTTP/1.1"), "{raw}");
    let body = raw.split_once("\r\n\r\n").unwrap().1;
    assert!(body.starts_with(r#"{"schema":1,"version":""#), "{body}");
    assert!(
        body.ends_with(r#""cluster":false,"classes":"1-9","resident_actors":"0"}"#),
        "{body}"
    );
}
