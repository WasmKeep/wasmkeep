//! Alarms end to end, plus traps and delete-self, with tests/guests/test-actor.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::path::Path;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService};

#[path = "support/guests.rs"]
mod guests;

fn wasm() -> &'static [u8] {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("tests/guests/test-actor", "test_actor"))
}

fn service(data: &Path, idle: Duration) -> ActorService {
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 32,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let component = runtime.compile(wasm()).unwrap();
    let mut config = ActorConfig::new(data);
    config.mailbox.idle_timeout = idle;
    config.alarm_retry_initial = Duration::from_millis(50);
    ActorService::new(
        runtime,
        config,
        vec![("t".into(), component, wasm().to_vec())],
    )
    .unwrap()
}

async fn start(service: ActorService) -> (SocketAddr, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    tokio::spawn(wasmkeep_server::serve(listener, service, async {
        let _ = stopped.await;
    }));
    (addr, stop)
}

/// (status, de-chunked body without the trailing newline).
async fn call(addr: SocketAddr, method: &str, path: &str) -> (u16, String) {
    call_at(addr, method, &format!("/actor/t/a{path}")).await
}

/// Like `call`, with the full path.
async fn call_at(addr: SocketAddr, method: &str, full: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} {full} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let status = raw.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = raw.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    let mut text = String::new();
    let mut rest = body;
    while let Some((size, tail)) = rest.split_once("\r\n") {
        match usize::from_str_radix(size.trim(), 16) {
            Ok(0) => break,
            Ok(n) => {
                text.push_str(&tail[..n]);
                rest = &tail[n + 2..];
            }
            Err(_) => {
                text = body.to_owned();
                break;
            }
        }
    }
    (status, text.trim_end().to_owned())
}

async fn number(addr: SocketAddr, path: &str) -> i64 {
    let (status, body) = call(addr, "GET", path).await;
    assert_eq!(status, 200, "{body}");
    body.parse().unwrap_or_else(|_| panic!("{path}: {body:?}"))
}

async fn ok(addr: SocketAddr, method: &str, path: &str) {
    let (status, body) = call(addr, method, path).await;
    assert_eq!(status, 200, "{method} {path}: {body}");
}

/// Polls until `/fired` reaches `n` (or panics after 5 s).
async fn wait_fired(addr: SocketAddr, n: i64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if number(addr, "/fired").await >= n {
            return;
        }
        assert!(Instant::now() < deadline, "alarm did not fire {n} time(s)");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_alarm_fires_once_and_is_cleared() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(service(data.path(), Duration::from_secs(30))).await;
    let set_at = Instant::now();
    ok(addr, "POST", "/alarm?in=150").await;
    assert_ne!(call(addr, "GET", "/alarm").await.1, "none");

    wait_fired(addr, 1).await;
    assert!(
        set_at.elapsed() >= Duration::from_millis(150),
        "fired early"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(number(addr, "/fired").await, 1);
    assert_eq!(number(addr, "/attempts").await, 1);
    assert_eq!(call(addr, "GET", "/alarm").await.1, "none");
}

#[tokio::test(flavor = "multi_thread")]
async fn cleared_and_rolled_back_alarms_never_fire() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(service(data.path(), Duration::from_secs(30))).await;
    ok(addr, "POST", "/alarm?in=150").await;
    ok(addr, "DELETE", "/alarm").await;
    ok(addr, "POST", "/alarm?in=50&rollback").await;
    assert_eq!(call(addr, "GET", "/alarm").await.1, "none");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(number(addr, "/attempts").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_hibernated_actor_wakes_for_its_alarm() {
    let data = tempfile::tempdir().unwrap();
    let svc = service(data.path(), Duration::from_millis(50));
    let (addr, _stop) = start(svc.clone()).await;
    ok(addr, "POST", "/alarm?in=400").await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        svc.resident(),
        0,
        "actor should be hibernating before the alarm"
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(number(addr, "/fired").await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_alarm_set_before_a_crash_fires_exactly_once_after_restart() {
    let data = tempfile::tempdir().unwrap();
    {
        let (addr, stop) = start(service(data.path(), Duration::from_millis(50))).await;
        ok(addr, "POST", "/alarm?in=300").await;
        // "Crash": stop serving and drop the service before the alarm is due.
        drop(stop);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (addr, _stop) = start(service(data.path(), Duration::from_secs(30))).await;
    // Do not touch the actor: the startup scan alone must re-arm the alarm.
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(number(addr, "/fired").await, 1);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(number(addr, "/attempts").await, 1, "fired more than once");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failing_alarm_is_retried_until_it_succeeds() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(service(data.path(), Duration::from_secs(30))).await;
    ok(addr, "POST", "/fail-next?n=2").await;
    ok(addr, "POST", "/alarm?in=0").await;
    wait_fired(addr, 1).await;
    assert_eq!(number(addr, "/attempts").await, 3);
    assert_eq!(call(addr, "GET", "/alarm").await.1, "none");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handler_can_schedule_the_next_alarm() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(service(data.path(), Duration::from_secs(30))).await;
    ok(addr, "POST", "/repeat?n=3").await;
    ok(addr, "POST", "/alarm?in=0").await;
    wait_fired(addr, 3).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(number(addr, "/fired").await, 3);
    assert_eq!(call(addr, "GET", "/alarm").await.1, "none");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_trap_discards_the_instance_but_not_the_data() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(service(data.path(), Duration::from_secs(30))).await;
    ok(addr, "POST", "/alarm?in=0").await;
    wait_fired(addr, 1).await;
    let started = Instant::now();
    assert_eq!(call(addr, "POST", "/trap").await.0, 500);
    // The trap is answered at once, not after the 30 s wall-clock limit, and
    // the actor is free for the next event.
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    let started = Instant::now();
    assert_eq!(number(addr, "/fired").await, 1);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_self_removes_the_actor() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(service(data.path(), Duration::from_secs(30))).await;
    ok(addr, "POST", "/alarm?in=0").await;
    wait_fired(addr, 1).await;
    ok(addr, "POST", "/alarm?in=300").await; // pending, must not survive deletion
    ok(addr, "POST", "/delete").await;
    // The next event starts a fresh actor with an empty database.
    assert_eq!(number(addr, "/fired").await, 0);
    assert_eq!(call(addr, "GET", "/alarm").await.1, "none");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(number(addr, "/attempts").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_alarm_starts_its_backoff_afresh() {
    // An alarm fails five times (backoff 50, 100, 200, 400 ms ...); then a
    // new alarm is set and fails once. Its retry comes after 50 ms, not
    // after the old alarm's grown backoff (50 ms x 2^5 = 1.6 s).
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(service(data.path(), Duration::from_secs(30))).await;
    ok(addr, "POST", "/fail-next?n=100").await;
    ok(addr, "POST", "/alarm?in=0").await;
    let deadline = Instant::now() + Duration::from_secs(5);
    while number(addr, "/attempts").await < 5 {
        assert!(Instant::now() < deadline, "the alarm was not retried");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    ok(addr, "POST", "/fail-next?n=1").await;
    let started = Instant::now();
    ok(addr, "POST", "/alarm?in=0").await;
    wait_fired(addr, 1).await;
    assert!(
        started.elapsed() < Duration::from_millis(800),
        "the new alarm's first retry waited {:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handler_that_reschedules_then_fails_still_backs_off() {
    // on-alarm sets the alarm to "now" and fails, five times. Without
    // backoff that is an immediate loop; with it the retries come after 50,
    // 100, 200, 400 ms ..., so 300 ms in there have been about three.
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(service(data.path(), Duration::from_secs(30))).await;
    ok(addr, "POST", "/resched-fail?n=5").await;
    ok(addr, "POST", "/alarm?in=0").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let early = number(addr, "/attempts").await;
    assert!(early <= 4, "{early} attempts in 300 ms: no backoff");
    wait_fired(addr, 1).await;
    assert_eq!(number(addr, "/attempts").await, 6);
}

#[tokio::test(flavor = "multi_thread")]
async fn awkward_names_keep_their_data_and_alarms_across_a_restart() {
    // Escaped in file names (hashed when long); the startup scan reads the
    // address back from the database, not from the file name.
    let mut names: Vec<String> = ["Alice", "room 1", "50%", "a~b", "é漢字", ".dot", "%41"]
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
    names.push("L".repeat(256));
    let enc = |name: &str| -> String {
        name.bytes()
            .map(|b| {
                if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.') {
                    char::from(b).to_string()
                } else {
                    format!("%{b:02X}")
                }
            })
            .collect()
    };
    let data = tempfile::tempdir().unwrap();
    {
        let (addr, stop) = start(service(data.path(), Duration::from_secs(30))).await;
        for name in &names {
            let base = format!("/actor/t/{}", enc(name));
            assert_eq!(
                call_at(addr, "POST", &format!("{base}/durable")).await.1,
                "1",
                "{name:?}"
            );
            assert_eq!(
                call_at(addr, "POST", &format!("{base}/alarm?in=800"))
                    .await
                    .0,
                200
            );
        }
        drop(stop);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (addr, _stop) = start(service(data.path(), Duration::from_secs(30))).await;
    // Only the startup scan can fire these: nothing touches the actors.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    for name in &names {
        let base = format!("/actor/t/{}", enc(name));
        assert_eq!(
            call_at(addr, "GET", &format!("{base}/fired")).await.1,
            "1",
            "{name:?}: alarm"
        );
        assert_eq!(
            call_at(addr, "GET", &format!("{base}/durable")).await.1,
            "1",
            "{name:?}: data"
        );
    }
}
