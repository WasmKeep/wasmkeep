//! examples/workflows end to end: the SDK's workflow runner (ADR 0019) on
//! one node. An order is reserved, sleeps, waits for approval, is charged
//! and notified; retries, a step that runs out of attempts, an approval
//! that never comes, cancel, idempotent starts, unknown ids, retention and
//! a trap in the middle of a step.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::path::Path;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorEnv, ActorService};

#[path = "support/guests.rs"]
mod guests;

/// The three classes: (class, wasm file stem).
const CLASSES: [(&str, &str); 3] = [
    ("orders", "workflows_orders"),
    ("ledger", "workflows_ledger"),
    ("app", "workflows_app"),
];

fn wasm(name: &str) -> Vec<u8> {
    static BUILT: OnceLock<Vec<(String, Vec<u8>)>> = OnceLock::new();
    let built = BUILT.get_or_init(|| {
        CLASSES
            .into_iter()
            .map(|(dir, name)| {
                let path = format!("../../examples/workflows/{dir}");
                (name.to_owned(), guests::build_guest(&path, name))
            })
            .collect()
    });
    built.iter().find(|(n, _)| n == name).unwrap().1.clone()
}

/// A node with the example's classes; `orders` gets `env` (its timings).
async fn start(data: &Path, env: &str) -> (SocketAddr, oneshot::Sender<()>) {
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 32,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let classes = CLASSES
        .into_iter()
        .map(|(class, name)| {
            let bytes = wasm(name);
            let component = guests::compile(&runtime, &bytes);
            (class.to_owned(), component, bytes)
        })
        .collect();
    let mut config = ActorConfig::new(data);
    // Instances hibernate between events, so runs replay from the database.
    config.mailbox.idle_timeout = Duration::from_millis(100);
    let mut vars = ActorEnv::default();
    vars.extend("orders", wasmkeep_server::env::parse(env).unwrap());
    config.env = vars;
    let svc = ActorService::new(runtime, config, classes).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    tokio::spawn(wasmkeep_server::serve(listener, svc, async {
        let _ = stopped.await;
    }));
    (addr, stop)
}

/// Fast timings: retries from 50 ms, three attempts, a day's retention.
const FAST: &str = "BACKOFF_MS=50\nATTEMPTS=3\nRETENTION_MS=86400000";

async fn request(addr: SocketAddr, method: &str, path: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        dechunk(body)
    } else {
        body.to_owned()
    };
    (status, body)
}

fn dechunk(mut s: &str) -> String {
    let mut out = String::new();
    while let Some((size, rest)) = s.split_once("\r\n") {
        let n = usize::from_str_radix(size.trim(), 16).unwrap_or(0);
        if n == 0 {
            break;
        }
        out.push_str(&rest[..n]);
        s = &rest[n + 2..];
    }
    out
}

const APP: &str = "/actor/app/main";

async fn order(addr: SocketAddr, id: &str) -> (u16, String) {
    request(addr, "GET", &format!("{APP}/orders/{id}"), "").await
}

/// Waits until order `id`'s status contains `want`; returns it.
async fn until(addr: SocketAddr, id: &str, want: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let (_, body) = order(addr, id).await;
        if body.contains(want) {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "order {id} never had {want:?}; last:\n{body}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn ledger(addr: SocketAddr) -> Vec<(String, String, u32)> {
    let (status, body) = request(addr, "GET", "/actor/ledger/main/entries", "").await;
    assert_eq!(status, 200, "{body}");
    body.lines()
        .map(|l| {
            let mut parts = l.split(' ');
            (
                parts.next().unwrap().to_owned(),
                parts.next().unwrap().to_owned(),
                parts.next().unwrap().parse().unwrap(),
            )
        })
        .collect()
}

fn whats(entries: &[(String, String, u32)]) -> Vec<&str> {
    entries.iter().map(|(w, _, _)| w.as_str()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_order_runs_to_the_end_with_every_step_once() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path(), FAST).await;
    let (status, body) = request(
        addr,
        "POST",
        &format!("{APP}/orders/o1"),
        "item=book;cooloff_ms=300;approve_within_ms=10000",
    )
    .await;
    assert_eq!(status, 200, "{body}");
    // Reserved, slept (hibernating meanwhile), now waiting for approval.
    let waiting = until(addr, "o1", "waiting_for: approved").await;
    assert!(waiting.contains("state: waiting"), "{waiting}");
    assert_eq!(whats(&ledger(addr).await), ["reserve:book"]);

    let (status, _) = request(addr, "POST", &format!("{APP}/orders/o1/approve"), "").await;
    assert_eq!(status, 200);
    let done = until(addr, "o1", "state: completed").await;
    assert!(done.contains("output: shipped book"), "{done}");
    assert!(done.contains("positions: 6"), "{done}");

    let entries = ledger(addr).await;
    assert_eq!(
        whats(&entries),
        [
            "reserve:book",
            "charge:book",
            "notify:buyer",
            "notify:warehouse"
        ]
    );
    // Each step ran once, each with a key of its own (a loop's repeated
    // step name included).
    assert!(entries.iter().all(|(_, _, n)| *n == 1), "{entries:?}");
    let mut keys: Vec<_> = entries.iter().map(|(_, k, _)| k.clone()).collect();
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), 4, "{entries:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failing_step_is_retried_and_runs_out_of_attempts() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path(), FAST).await;
    let fail = |times: u32| async move {
        let path = format!("/actor/ledger/main/fail?what=charge&times={times}");
        assert_eq!(request(addr, "POST", &path, "").await.0, 200);
    };
    let input = "item=pen;cooloff_ms=0;approve_within_ms=10000";

    // Two failures, then it works: three attempts are enough.
    fail(2).await;
    request(addr, "POST", &format!("{APP}/orders/ok"), input).await;
    until(addr, "ok", "waiting_for: approved").await;
    request(addr, "POST", &format!("{APP}/orders/ok/approve"), "").await;
    until(addr, "ok", "state: completed").await;
    let charges = whats(&ledger(addr).await)
        .into_iter()
        .filter(|w| w.starts_with("charge"))
        .count();
    assert_eq!(charges, 1);

    // Five failures: the step gives up after three and the order fails.
    fail(5).await;
    request(addr, "POST", &format!("{APP}/orders/ko"), input).await;
    until(addr, "ko", "waiting_for: approved").await;
    request(addr, "POST", &format!("{APP}/orders/ko/approve"), "").await;
    let failed = until(addr, "ko", "state: failed").await;
    assert!(
        failed.contains("step \"charge\" failed after 3 attempts")
            && failed.contains("failed on purpose"),
        "{failed}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_approval_that_never_comes_releases_the_reservation() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path(), FAST).await;
    request(
        addr,
        "POST",
        &format!("{APP}/orders/late"),
        "item=lamp;cooloff_ms=0;approve_within_ms=400",
    )
    .await;
    let failed = until(addr, "late", "state: failed").await;
    assert!(failed.contains("not approved in time"), "{failed}");
    assert!(failed.contains("reserve:lamp#1 was released"), "{failed}");
    assert_eq!(whats(&ledger(addr).await), ["reserve:lamp", "release:lamp"]);
    // A finished order takes no more events.
    let (status, _) = request(addr, "POST", &format!("{APP}/orders/late/approve"), "").await;
    assert_eq!(status, 409);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_order_runs_no_further_step() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path(), FAST).await;
    request(
        addr,
        "POST",
        &format!("{APP}/orders/c"),
        "item=desk;cooloff_ms=600;approve_within_ms=10000",
    )
    .await;
    until(addr, "c", "state: waiting").await;
    let (status, body) = request(addr, "POST", &format!("{APP}/orders/c/cancel"), "").await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("state: cancelled"), "{body}");
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    assert!(order(addr, "c").await.1.contains("state: cancelled"));
    assert_eq!(whats(&ledger(addr).await), ["reserve:desk"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_start_is_idempotent_and_unknown_ids_leave_nothing() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path(), FAST).await;
    let input = "item=cup;cooloff_ms=5000;approve_within_ms=10000";
    request(addr, "POST", &format!("{APP}/orders/same"), input).await;
    until(addr, "same", "state: waiting").await;
    let (status, again) = request(addr, "POST", &format!("{APP}/orders/same"), "item=other").await;
    assert_eq!(status, 200);
    assert!(again.contains("state: waiting"), "{again}");
    assert_eq!(whats(&ledger(addr).await), ["reserve:cup"]);

    assert_eq!(order(addr, "nobody").await.0, 404);
    assert_eq!(
        request(addr, "POST", &format!("{APP}/orders/nobody/approve"), "")
            .await
            .0,
        404
    );
    // The calls created actor orders/nobody; it deleted itself.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let kept = data.path().join("actors/default/orders/same.db");
    assert!(
        kept.exists(),
        "{} is missing: wrong layout?",
        kept.display()
    );
    let db = data.path().join("actors/default/orders/nobody.db");
    assert!(!db.exists(), "{} is still there", db.display());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_finished_order_is_kept_for_its_retention_then_its_id_is_new() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path(), "BACKOFF_MS=50\nATTEMPTS=3\nRETENTION_MS=600").await;
    let input = "item=hat;cooloff_ms=0;approve_within_ms=200";
    request(addr, "POST", &format!("{APP}/orders/r"), input).await;
    until(addr, "r", "state: failed").await;
    // Kept a while, then gone.
    assert_eq!(order(addr, "r").await.0, 200);
    let deadline = Instant::now() + Duration::from_secs(10);
    while order(addr, "r").await.0 != 404 {
        assert!(Instant::now() < deadline, "order r was never deleted");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // The id starts a new run, whose keys are not the old run's.
    request(addr, "POST", &format!("{APP}/orders/r"), input).await;
    until(addr, "r", "state: failed").await;
    let entries = ledger(addr).await;
    let reserves: Vec<_> = entries
        .iter()
        .filter(|(w, _, _)| w == "reserve:hat")
        .collect();
    assert_eq!(reserves.len(), 2, "{entries:?}");
    assert_ne!(reserves[0].1, reserves[1].1, "{entries:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_step_that_traps_is_attempted_again() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path(), FAST).await;
    let path = "/actor/ledger/main/fail?what=reserve&times=1&trap=1";
    assert_eq!(request(addr, "POST", path, "").await.0, 200);
    request(
        addr,
        "POST",
        &format!("{APP}/orders/t"),
        "item=clock;cooloff_ms=0;approve_within_ms=10000",
    )
    .await;
    // The first attempt panicked inside the step; the alarm set before it
    // ran brought the second.
    until(addr, "t", "waiting_for: approved").await;
    assert_eq!(whats(&ledger(addr).await), ["reserve:clock"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_out_of_time_continues_in_a_new_event() {
    let data = tempfile::tempdir().unwrap();
    // No time for new steps in any event: each step runs in an event of
    // its own, continued by the alarm.
    let (addr, _stop) = start(data.path(), &format!("{FAST}\nRUN_BUDGET_MS=0")).await;
    request(
        addr,
        "POST",
        &format!("{APP}/orders/slow"),
        "item=rug;cooloff_ms=0;approve_within_ms=10000",
    )
    .await;
    until(addr, "slow", "waiting_for: approved").await;
    request(addr, "POST", &format!("{APP}/orders/slow/approve"), "").await;
    let done = until(addr, "slow", "state: completed").await;
    assert!(done.contains("positions: 6"), "{done}");
    let entries = ledger(addr).await;
    assert_eq!(
        whats(&entries),
        [
            "reserve:rug",
            "charge:rug",
            "notify:buyer",
            "notify:warehouse"
        ]
    );
    assert!(entries.iter().all(|(_, _, n)| *n == 1), "{entries:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_event_sent_before_its_wait_is_kept_for_it() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path(), FAST).await;
    request(
        addr,
        "POST",
        &format!("{APP}/orders/early"),
        "item=vase;cooloff_ms=800;approve_within_ms=10000",
    )
    .await;
    // Approved while still cooling off: the wait finds the event.
    until(addr, "early", "state: waiting").await;
    let (status, _) = request(addr, "POST", &format!("{APP}/orders/early/approve"), "").await;
    assert_eq!(status, 200);
    let done = until(addr, "early", "state: completed").await;
    assert!(done.contains("shipped vase"), "{done}");
}
