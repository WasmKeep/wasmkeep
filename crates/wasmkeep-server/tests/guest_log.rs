//! What actors write to stdout and stderr reaches the node's log (target
//! `wasmkeep::guest`), line by line, tagged with the actor, within limits.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::io::Write;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService};

#[path = "support/guests.rs"]
mod guests;

/// What `Captured::install` installed, for failure messages.
static INSTALLED: std::sync::OnceLock<Captured> = std::sync::OnceLock::new();

/// The log lines this test process wrote, for the `wasmkeep::guest` target.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Captured {
    /// Each test runs in its own process (nextest), so a global
    /// subscriber is this test's alone.
    fn install() -> Self {
        let captured = Self::default();
        let _ = INSTALLED.set(captured.clone());
        let writer = captured.clone();
        tracing_subscriber::fmt()
            // The server's warnings too: a failed event says why.
            .with_env_filter("wasmkeep::guest=info,wasmkeep_server=warn")
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .init();
        captured
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

async fn start(
    data: &std::path::Path,
    adjust: impl FnOnce(&mut ActorConfig),
) -> (SocketAddr, ActorService) {
    let wasm = guests::build_guest("tests/guests/test-actor", "test_actor");
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 8,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(&wasm).unwrap();
    let mut config = ActorConfig::new(data);
    config.mailbox.idle_timeout = Duration::from_millis(200);
    adjust(&mut config);
    let service = ActorService::new(runtime, config, vec![("t".into(), component, wasm)]).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    std::mem::forget(stop); // serve for the rest of the test process
    tokio::spawn(wasmkeep_server::serve(listener, service.clone(), async {
        let _ = stopped.await;
    }));
    (addr, service)
}

async fn print(addr: SocketAddr, name: &str, body: &str) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "POST /actor/t/{name}/print HTTP/1.1\r\nHost: t\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let log = INSTALLED.get().map(Captured::text).unwrap_or_default();
    assert!(raw.starts_with("HTTP/1.1 200"), "{raw}\nlog:\n{log}");
}

/// Waits until `want` is in the log, or fails with what is there.
async fn logged(captured: &Captured, want: &str) {
    for _ in 0..100 {
        if captured.text().contains(want) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{want:?} not logged:\n{}", captured.text());
}

#[tokio::test(flavor = "multi_thread")]
async fn what_an_actor_prints_is_logged_with_its_address() {
    let captured = Captured::install();
    let data = tempfile::tempdir().unwrap();
    let (addr, _service) = start(data.path(), |_| {}).await;
    print(addr, "alice", "first line\nsecond line\n").await;
    logged(&captured, "first line").await;
    logged(&captured, "second line").await;
    let text = captured.text();
    let first = text.lines().find(|l| l.contains("first line")).unwrap();
    // The actor as its app sees it, the app as a field of its own.
    assert!(first.contains("app=default/default"), "{first}");
    assert!(first.contains("actor=t/alice"), "{first}");
    assert!(
        first.contains("stream=\"stdout\"") || first.contains("stream=stdout"),
        "{first}"
    );
    // stderr's partial line is logged when the instance ends (it
    // hibernates after 200 ms idle).
    logged(&captured, "err: first line").await;
    let err = captured.text();
    let err = err
        .lines()
        .find(|l| l.contains("err: first line"))
        .unwrap()
        .to_owned();
    assert!(
        err.contains("stream=\"stderr\"") || err.contains("stream=stderr"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn control_characters_cannot_forge_log_lines() {
    let captured = Captured::install();
    let data = tempfile::tempdir().unwrap();
    let (addr, _service) = start(data.path(), |_| {}).await;
    print(addr, "mallory", "ok\rFAKE ERROR\u{1b}[31m red\n").await;
    logged(&captured, "FAKE ERROR").await;
    let text = captured.text();
    let line = text.lines().find(|l| l.contains("FAKE ERROR")).unwrap();
    assert!(line.contains("actor=t/mallory"), "{line}");
    assert!(!text.contains('\u{1b}') && !text.contains('\r'), "{text:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_chatty_actor_is_limited_and_the_drops_are_reported() {
    let captured = Captured::install();
    let data = tempfile::tempdir().unwrap();
    let (addr, _service) = start(data.path(), |c| {
        c.guest_log = wasmkeep_server::GuestLog::new(5);
    })
    .await;
    let many: String = (0..200).map(|i| format!("line {i}\n")).collect();
    print(addr, "chatty", &many).await;
    logged(&captured, "dropped").await;
    let text = captured.text();
    // A burst of twice the rate, then the rest dropped and counted.
    let kept = text
        .lines()
        .filter(|l| l.contains("stream=\"stdout\"") && l.contains(": line "))
        .count();
    assert!((10..=11).contains(&kept), "{kept} kept:\n{text}");
    assert!(text.contains("dropped=190"), "{text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rate_of_zero_discards_output_as_before() {
    let captured = Captured::install();
    let data = tempfile::tempdir().unwrap();
    let (addr, _service) = start(data.path(), |c| {
        c.guest_log = wasmkeep_server::GuestLog::new(0);
    })
    .await;
    print(addr, "quiet", "nothing to see\n").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !captured.text().contains("nothing to see"),
        "{}",
        captured.text()
    );
}
