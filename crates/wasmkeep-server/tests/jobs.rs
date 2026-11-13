//! examples/jobs end to end: the SDK's KV and queue patterns (ADR 0015)
//! on one node. KV: versions, compare-and-set, expiry, listing across
//! shards, outside clients refused. Queues: push delivery from the alarm,
//! dead-lettering after the attempts, a pull lease that runs out and is
//! delivered again, stale acks ignored.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService};

#[path = "support/guests.rs"]
mod guests;

/// The three classes, built once per test binary: (class, wasm file stem).
const CLASSES: [(&str, &str); 3] = [
    ("kv", "jobs_kv"),
    ("queue", "jobs_queue"),
    ("app", "jobs_app"),
];

fn wasm(name: &str) -> Vec<u8> {
    static BUILT: OnceLock<Vec<(String, Vec<u8>)>> = OnceLock::new();
    let built = BUILT.get_or_init(|| {
        CLASSES
            .into_iter()
            .map(|(dir, name)| {
                let path = format!("../../examples/jobs/{dir}");
                (name.to_owned(), guests::build_guest(&path, name))
            })
            .collect()
    });
    built.iter().find(|(n, _)| n == name).unwrap().1.clone()
}

async fn start(data: &std::path::Path) -> (SocketAddr, oneshot::Sender<()>) {
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
    let svc = ActorService::new(runtime, ActorConfig::new(data), classes).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    tokio::spawn(wasmkeep_server::serve(listener, svc, async {
        let _ = stopped.await;
    }));
    (addr, stop)
}

struct Reply {
    status: u16,
    etag: Option<String>,
    body: String,
}

async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> Reply {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let extra: String = headers
        .iter()
        .map(|(k, v)| format!("{k}: {v}\r\n"))
        .collect();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: t\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    let etag = head.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.eq_ignore_ascii_case("etag").then(|| v.trim().to_owned())
    });
    // Chunked bodies: keep the text lines, drop the chunk sizes.
    let body = if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        dechunk(body)
    } else {
        body.to_owned()
    };
    Reply { status, etag, body }
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

async fn wait_for(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn kv_versions_compare_and_set_and_delete() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path()).await;
    let key = format!("{APP}/kv/user:1");

    assert_eq!(request(addr, "GET", &key, &[], "").await.status, 404);
    let first = request(addr, "PUT", &key, &[("if-none-match", "*")], "Ada").await;
    assert_eq!(first.status, 200, "{}", first.body);
    let v1 = first.etag.unwrap();
    // Create-only loses once the key exists.
    let again = request(addr, "PUT", &key, &[("if-none-match", "*")], "Bob").await;
    assert_eq!(again.status, 409);

    let read = request(addr, "GET", &key, &[], "").await;
    assert_eq!((read.status, read.body.as_str()), (200, "Ada"));
    assert_eq!(read.etag.as_deref(), Some(v1.as_str()));

    // Compare-and-set: the current version wins once, then it is stale.
    let cas = request(addr, "PUT", &key, &[("if-match", &v1)], "Ada L.").await;
    assert_eq!(cas.status, 200);
    let v2 = cas.etag.unwrap();
    assert_ne!(v1, v2);
    let stale = request(addr, "PUT", &key, &[("if-match", &v1)], "lost").await;
    assert_eq!(stale.status, 409);
    assert_eq!(request(addr, "GET", &key, &[], "").await.body, "Ada L.");

    // A version is never reused, even after a delete and a re-create.
    assert_eq!(request(addr, "DELETE", &key, &[], "").await.status, 200);
    assert_eq!(request(addr, "DELETE", &key, &[], "").await.status, 404);
    let recreated = request(addr, "PUT", &key, &[], "new").await.etag.unwrap();
    let resurrect = request(addr, "PUT", &key, &[("if-match", &v2)], "old").await;
    assert_eq!(resurrect.status, 409);
    assert!(recreated.parse::<u64>().unwrap() > v2.parse::<u64>().unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn kv_keys_expire_and_list_across_shards() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path()).await;
    for i in 0..12 {
        let put = request(addr, "PUT", &format!("{APP}/kv/item:{i:02}"), &[], "x").await;
        assert_eq!(put.status, 200, "{}", put.body);
    }
    request(addr, "PUT", &format!("{APP}/kv/other"), &[], "x").await;
    let listed = request(addr, "GET", &format!("{APP}/kv?prefix=item:"), &[], "").await;
    let expected: String = (0..12).map(|i| format!("item:{i:02}\n")).collect();
    assert_eq!(listed.body, expected, "sorted, all shards, prefix only");
    // The 13 keys spread over more than one shard database.
    let shards = std::fs::read_dir(data.path().join("state/default/default/actors/kv"))
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|x| x == "db")
        })
        .count();
    assert!(shards > 1, "{shards} shard databases");

    let temp = format!("{APP}/kv/session?ttl_ms=2000");
    let written = Instant::now();
    assert_eq!(request(addr, "PUT", &temp, &[], "s").await.status, 200);
    let session = format!("{APP}/kv/session");
    let read = request(addr, "GET", &session, &[], "").await.status;
    assert!(read == 200 || written.elapsed() >= Duration::from_secs(2));
    wait_for("the key to expire", async || {
        request(addr, "GET", &session, &[], "").await.status == 404
    })
    .await;
    assert!(written.elapsed() >= Duration::from_secs(2), "expired early");
}

#[tokio::test(flavor = "multi_thread")]
async fn shards_and_queues_refuse_outside_clients() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path()).await;
    for path in [
        "/actor/kv/shard-0/wasmkeep-kv/v1/k/00",
        "/actor/queue/jobs/wasmkeep-queue/v1/stats",
    ] {
        let r = request(addr, "GET", path, &[], "").await;
        assert_eq!(r.status, 403, "{path}: {}", r.body);
    }
    // A forged caller header from outside is stripped by the host.
    let forged = request(
        addr,
        "POST",
        "/actor/queue/jobs/wasmkeep-queue/v1/lease?max=1",
        &[("x-wasmkeep-caller", "default/app/main")],
        "",
    )
    .await;
    assert_eq!(forged.status, 403);
    let delivery = request(
        addr,
        "POST",
        "/actor/app/w/wasmkeep-queue/v1/deliver",
        &[],
        "",
    )
    .await;
    assert_eq!(delivery.status, 403, "deliveries come from actors");
}

#[tokio::test(flavor = "multi_thread")]
async fn pushed_jobs_are_done_and_failing_ones_dead_lettered() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path()).await;
    let ok = request(addr, "POST", &format!("{APP}/jobs"), &[], "resize 42").await;
    assert_eq!(ok.status, 200, "{}", ok.body);
    let id = ok.body.trim().to_owned();
    let bad = request(addr, "POST", &format!("{APP}/jobs"), &[], "fail always").await;
    let bad_id = bad.body.trim().to_owned();

    let done = format!("{APP}/kv/job:{id}");
    wait_for("the job to be done", async || {
        request(addr, "GET", &done, &[], "").await.body == "RESIZE 42"
    })
    .await;

    let stats = format!("{APP}/jobs/stats");
    wait_for("the failing job to be dead-lettered", async || {
        request(addr, "GET", &stats, &[], "").await.body == "ready=0 delayed=0 in_flight=0 dead=1\n"
    })
    .await;
    let dead = request(addr, "GET", &format!("{APP}/jobs/dead"), &[], "").await;
    assert_eq!(
        dead.body,
        format!("{bad_id} 3 attempts exhausted: the consumer asked for a retry\n")
    );
    assert_eq!(
        request(addr, "GET", &format!("{APP}/kv/job:{bad_id}"), &[], "")
            .await
            .status,
        404
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pull_lease_runs_out_and_the_stale_ack_is_ignored() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path()).await;
    let sent = request(addr, "POST", &format!("{APP}/inbox"), &[], "hello").await;
    let id = sent.body.trim().to_owned();

    let lease = format!("{APP}/inbox/lease?visibility_ms=1000");
    let leased_at = Instant::now();
    let first = request(addr, "POST", &lease, &[], "").await.body;
    assert_eq!(first, format!("{id} 1 hello\n"));
    // Invisible while leased (unless this machine took a second to answer).
    let hidden = request(addr, "POST", &lease, &[], "").await.body;
    assert!(hidden.is_empty() || leased_at.elapsed() >= Duration::from_secs(1));

    let mut second = String::new();
    wait_for("the lease to run out", async || {
        second = request(addr, "POST", &lease, &[], "").await.body;
        !second.is_empty()
    })
    .await;
    assert!(
        leased_at.elapsed() >= Duration::from_secs(1),
        "visible early"
    );
    let attempt = if hidden.is_empty() { 2 } else { 3 };
    assert_eq!(
        second,
        format!("{id} {attempt} hello\n"),
        "same id, next attempt"
    );

    let stale = format!("{APP}/inbox/ack?id={id}&attempt=1");
    let current = format!("{APP}/inbox/ack?id={id}&attempt={attempt}");
    assert_eq!(request(addr, "POST", &stale, &[], "").await.status, 409);
    assert_eq!(request(addr, "POST", &current, &[], "").await.status, 200);
    assert_eq!(
        request(addr, "POST", &current, &[], "").await.status,
        409,
        "settled"
    );

    // Past the visibility timeout, the acked message does not come back.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(
        request(addr, "POST", &lease, &[], "").await.body,
        "",
        "acked is gone"
    );
    let stats = request(addr, "GET", &format!("{APP}/inbox/stats"), &[], "").await;
    assert_eq!(stats.body, "ready=0 delayed=0 in_flight=0 dead=0\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_pull_message_leased_past_its_attempts_is_dead_lettered() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path()).await;
    request(addr, "POST", &format!("{APP}/inbox"), &[], "poison").await;
    // A lease that finds it visible is its next attempt; an empty one (still
    // hidden) changes nothing.
    let lease = format!("{APP}/inbox/lease?visibility_ms=200");
    for attempt in 1..=3 {
        let mut got = String::new();
        wait_for("the next attempt", async || {
            got = request(addr, "POST", &lease, &[], "").await.body;
            !got.is_empty()
        })
        .await;
        assert!(got.ends_with(&format!(" {attempt} poison\n")), "{got}");
    }
    // Once visible again, a lease finds it exhausted and moves it aside.
    let stats = format!("{APP}/inbox/stats");
    wait_for("the message to be dead-lettered", async || {
        let leased = request(addr, "POST", &lease, &[], "").await.body;
        assert_eq!(leased, "", "never a fourth attempt");
        request(addr, "GET", &stats, &[], "").await.body == "ready=0 delayed=0 in_flight=0 dead=1\n"
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_is_queued_in_one_write_with_ids_in_order() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path()).await;
    let first = request(addr, "POST", &format!("{APP}/inbox"), &[], "zero").await;
    let first: u64 = first.body.trim().parse().unwrap();
    let batch = request(
        addr,
        "POST",
        &format!("{APP}/inbox/batch"),
        &[],
        "a\nb\nc\n",
    )
    .await;
    assert_eq!(batch.status, 200, "{}", batch.body);
    let ids: Vec<u64> = batch.body.lines().map(|l| l.parse().unwrap()).collect();
    assert_eq!(ids, vec![first + 1, first + 2, first + 3]);

    let leased = request(addr, "POST", &format!("{APP}/inbox/lease?max=10"), &[], "").await;
    let expected: String = [
        (first, "zero"),
        (first + 1, "a"),
        (first + 2, "b"),
        (first + 3, "c"),
    ]
    .iter()
    .map(|(id, body)| format!("{id} 1 {body}\n"))
    .collect();
    assert_eq!(leased.body, expected, "in send order");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cached_read_is_stale_for_others_writes_but_not_for_its_own() {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path()).await;
    let writer = "/actor/app/writer/kv/flag";
    let reader = "/actor/app/reader/kv/flag";
    request(addr, "PUT", writer, &[], "off").await;
    let cached = format!("{reader}?cached=1");
    assert_eq!(request(addr, "GET", &cached, &[], "").await.body, "off");

    // Another actor's write: the reader's cache still has the old value,
    // a fresh read sees the new one.
    request(addr, "PUT", writer, &[], "on").await;
    assert_eq!(request(addr, "GET", &cached, &[], "").await.body, "off");
    assert_eq!(request(addr, "GET", reader, &[], "").await.body, "on");
    // The fresh read refreshed the cache.
    assert_eq!(request(addr, "GET", &cached, &[], "").await.body, "on");

    // The reader's own writes and deletes go through its cache.
    request(addr, "PUT", reader, &[], "mine").await;
    assert_eq!(request(addr, "GET", &cached, &[], "").await.body, "mine");
    request(addr, "DELETE", reader, &[], "").await;
    assert_eq!(request(addr, "GET", &cached, &[], "").await.status, 404);
}
