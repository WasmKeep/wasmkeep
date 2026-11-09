//! Every guest SDK's kv and queue clients against the Rust shard and
//! queue classes (ADR 0015): `examples/jobs/kv` and `examples/jobs/queue`,
//! built here, serve a jobs `app` in each language (`examples/<lang>-jobs`,
//! the same routes as the Rust app in `examples/jobs/app`) that speaks
//! their protocol over `actors.call`. Covers put/get, compare-and-set
//! conflicts, conditional deletes, expiry, listing across shards, the read
//! cache, send/lease/ack, batches, and push deliveries to each guest's
//! consumer (its SDK's delivery handler), dead letters included.
//!
//! The components are prebuilt (each example's README says how); a missing
//! one is skipped, or fails with `WASMKEEP_REQUIRE_LANGS=1` (CI).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService};

#[path = "support/guests.rs"]
mod guests;

/// The Rust classes, built once per test binary: (class, wasm file stem).
const RUST: [(&str, &str); 2] = [("kv", "jobs_kv"), ("queue", "jobs_queue")];

fn rust_wasm() -> &'static [(String, Vec<u8>)] {
    static BUILT: OnceLock<Vec<(String, Vec<u8>)>> = OnceLock::new();
    BUILT.get_or_init(|| {
        RUST.into_iter()
            .map(|(dir, name)| {
                let path = format!("../../examples/jobs/{dir}");
                (dir.to_owned(), guests::build_guest(&path, name))
            })
            .collect()
    })
}

/// A prebuilt jobs app (`examples/<example>/<file>`), or `None` to skip.
fn prebuilt(example: &str, file: &str) -> Option<Vec<u8>> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples")
        .join(example)
        .join(file);
    match std::fs::read(&path) {
        Ok(bytes) => Some(bytes),
        Err(_) if std::env::var_os("WASMKEEP_REQUIRE_LANGS").is_none() => {
            eprintln!(
                "{example}: {} not built; skipping (see its README)",
                path.display()
            );
            None
        }
        Err(e) => panic!("{example}: {}: {e}", path.display()),
    }
}

async fn start(data: &std::path::Path, app: &[u8]) -> (SocketAddr, oneshot::Sender<()>) {
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 32,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let mut classes: Vec<_> = rust_wasm()
        .iter()
        .map(|(class, bytes)| {
            let component = guests::compile(&runtime, bytes);
            (class.clone(), component, bytes.clone())
        })
        .collect();
    // Class `app`: the `jobs` queue pushes to `app/worker`.
    classes.push((
        "app".to_owned(),
        guests::compile(&runtime, app),
        app.to_vec(),
    ));
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
    let deadline = Instant::now() + Duration::from_secs(15);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The Rust SDK's `kv::shard_for` (FNV-1a 64 modulo the count), copied:
/// it is part of the data layout and never changes.
fn shard_for(key: &[u8], shards: u64) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash % shards
}

/// The shard databases created so far, by actor name.
fn shard_dbs(data: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(data.join("actors/default/kv"))
        .map(|dir| {
            dir.map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .filter(|n| n.ends_with(".db"))
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

/// Put/get, compare-and-set and conditional deletes through the client.
async fn kv_versions_and_conflicts(addr: SocketAddr, data: &std::path::Path, engine: &str) {
    let key = format!("{APP}/kv/user:1");
    assert_eq!(request(addr, "GET", &key, &[], "").await.status, 404);
    // The client picked the shard the Rust SDK picks for this key:
    // `kv::shard_for(b"user:1", 4) == 3`.
    assert_eq!(shard_dbs(data), ["shard-3.db"], "{engine}");
    // Compare-and-set on an absent key (`if-none-match: *`).
    let first = request(addr, "PUT", &key, &[("if-none-match", "*")], "Ada").await;
    assert_eq!(first.status, 200, "{engine}: {}", first.body);
    let v1 = first.etag.unwrap();
    let again = request(addr, "PUT", &key, &[("if-none-match", "*")], "Bob").await;
    assert_eq!(
        again.status, 409,
        "{engine}: create-only on an existing key"
    );

    let read = request(addr, "GET", &key, &[], "").await;
    assert_eq!((read.status, read.body.as_str()), (200, "Ada"), "{engine}");
    assert_eq!(read.etag.as_deref(), Some(v1.as_str()));

    // Compare-and-set: the current version wins once, then it is stale.
    let cas = request(addr, "PUT", &key, &[("if-match", &v1)], "Ada L.").await;
    assert_eq!(cas.status, 200, "{engine}: {}", cas.body);
    let v2 = cas.etag.unwrap();
    assert_ne!(v1, v2);
    let stale = request(addr, "PUT", &key, &[("if-match", &v1)], "lost").await;
    assert_eq!(stale.status, 409, "{engine}: a stale compare-and-set");
    assert_eq!(request(addr, "GET", &key, &[], "").await.body, "Ada L.");

    // A conditional delete on a stale version loses; on the current one wins.
    let lost = request(addr, "DELETE", &key, &[("if-match", &v1)], "").await;
    assert_eq!(lost.status, 409, "{engine}");
    let gone = request(addr, "DELETE", &key, &[("if-match", &v2)], "").await;
    assert_eq!(gone.status, 200, "{engine}: {}", gone.body);
    assert_eq!(request(addr, "DELETE", &key, &[], "").await.status, 404);
    // A version is never reused, even after a delete and a re-create.
    let recreated = request(addr, "PUT", &key, &[], "new").await.etag.unwrap();
    assert!(recreated.parse::<u64>().unwrap() > v2.parse::<u64>().unwrap());
}

/// Listing merges every shard; a ttl write expires.
async fn kv_list_and_expiry(addr: SocketAddr, data: &std::path::Path, engine: &str) {
    // Each write creates the shard database Rust's `shard_for` names, and
    // no other: checked key by key, so a wrong hash cannot pass by chance.
    let mut expected_dbs: std::collections::BTreeSet<String> =
        shard_dbs(data).into_iter().collect();
    for i in 0..12 {
        let key = format!("item:{i:02}");
        let put = request(addr, "PUT", &format!("{APP}/kv/{key}"), &[], "x").await;
        assert_eq!(put.status, 200, "{engine}: {}", put.body);
        expected_dbs.insert(format!("shard-{}.db", shard_for(key.as_bytes(), 4)));
        let dbs: Vec<String> = expected_dbs.iter().cloned().collect();
        assert_eq!(shard_dbs(data), dbs, "{engine}: the shard of {key}");
    }
    request(addr, "PUT", &format!("{APP}/kv/other"), &[], "x").await;
    let listed = request(addr, "GET", &format!("{APP}/kv?prefix=item:"), &[], "").await;
    let expected: String = (0..12).map(|i| format!("item:{i:02}\n")).collect();
    assert_eq!(
        listed.body, expected,
        "{engine}: sorted, all shards, prefix only"
    );
    // The client chose the same shards as Rust would: more than one.
    let shards = shard_dbs(data).len();
    assert!(shards > 1, "{engine}: {shards} shard databases");

    let written = Instant::now();
    let temp = format!("{APP}/kv/session?ttl_ms=1500");
    assert_eq!(request(addr, "PUT", &temp, &[], "s").await.status, 200);
    let session = format!("{APP}/kv/session");
    wait_for("the key to expire", async || {
        request(addr, "GET", &session, &[], "").await.status == 404
    })
    .await;
    assert!(
        written.elapsed() >= Duration::from_millis(1500),
        "{engine}: expired early"
    );
}

/// The read cache: stale for another actor's writes, not for its own.
async fn kv_read_cache(addr: SocketAddr, engine: &str) {
    let writer = "/actor/app/writer/kv/flag";
    let reader = "/actor/app/reader/kv/flag";
    request(addr, "PUT", writer, &[], "off").await;
    let cached = format!("{reader}?cached=1");
    assert_eq!(request(addr, "GET", &cached, &[], "").await.body, "off");
    request(addr, "PUT", writer, &[], "on").await;
    assert_eq!(
        request(addr, "GET", &cached, &[], "").await.body,
        "off",
        "{engine}: cached"
    );
    assert_eq!(request(addr, "GET", reader, &[], "").await.body, "on");
    assert_eq!(request(addr, "GET", &cached, &[], "").await.body, "on");
    request(addr, "PUT", reader, &[], "mine").await;
    assert_eq!(request(addr, "GET", &cached, &[], "").await.body, "mine");
    request(addr, "DELETE", reader, &[], "").await;
    assert_eq!(request(addr, "GET", &cached, &[], "").await.status, 404);
}

/// Pull: send, lease, a stale and a current ack, a batch.
async fn pull_queue(addr: SocketAddr, engine: &str) {
    let sent = request(addr, "POST", &format!("{APP}/inbox"), &[], "hello").await;
    assert_eq!(sent.status, 200, "{engine}: {}", sent.body);
    let id: u64 = sent.body.trim().parse().unwrap();
    let leased = request(
        addr,
        "POST",
        &format!("{APP}/inbox/lease?visibility_ms=30000"),
        &[],
        "",
    )
    .await;
    assert_eq!(leased.body, format!("{id} 1 hello\n"), "{engine}");
    let stats = format!("{APP}/inbox/stats");
    assert_eq!(
        request(addr, "GET", &stats, &[], "").await.body,
        "ready=0 delayed=0 in_flight=1 dead=0\n"
    );
    let stale = format!("{APP}/inbox/ack?id={id}&attempt=2");
    let current = format!("{APP}/inbox/ack?id={id}&attempt=1");
    assert_eq!(request(addr, "POST", &stale, &[], "").await.status, 409);
    assert_eq!(request(addr, "POST", &current, &[], "").await.status, 200);
    assert_eq!(
        request(addr, "POST", &current, &[], "").await.status,
        409,
        "{engine}: settled"
    );

    let batch = request(
        addr,
        "POST",
        &format!("{APP}/inbox/batch"),
        &[],
        "a\nb\nc\n",
    )
    .await;
    assert_eq!(batch.status, 200, "{engine}: {}", batch.body);
    let ids: Vec<u64> = batch.body.lines().map(|l| l.parse().unwrap()).collect();
    assert_eq!(ids, vec![id + 1, id + 2, id + 3], "{engine}: in order");
    let all = request(addr, "POST", &format!("{APP}/inbox/lease?max=10"), &[], "").await;
    let expected: String = ids
        .iter()
        .zip(["a", "b", "c"])
        .map(|(id, b)| format!("{id} 1 {b}\n"))
        .collect();
    assert_eq!(all.body, expected, "{engine}");
    assert_eq!(
        request(addr, "GET", &stats, &[], "").await.body,
        "ready=0 delayed=0 in_flight=3 dead=0\n"
    );
}

/// Push: the Rust queue delivers to the guest worker, which writes through
/// its kv client; a job that always asks for a retry is dead-lettered.
async fn push_to_a_guest_consumer(addr: SocketAddr, engine: &str) {
    let ok = request(addr, "POST", &format!("{APP}/jobs"), &[], "resize 42").await;
    assert_eq!(ok.status, 200, "{engine}: {}", ok.body);
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
        format!("{bad_id} 3 attempts exhausted: the consumer asked for a retry\n"),
        "{engine}"
    );
    // Deliveries come from actors only.
    let outside = request(
        addr,
        "POST",
        "/actor/app/worker/wasmkeep-queue/v1/deliver",
        &[],
        "",
    )
    .await;
    assert_eq!(outside.status, 403, "{engine}: {}", outside.body);
}

async fn scenario(engine: &str, app: &[u8]) {
    let data = tempfile::tempdir().unwrap();
    let (addr, _stop) = start(data.path(), app).await;
    kv_versions_and_conflicts(addr, data.path(), engine).await;
    kv_list_and_expiry(addr, data.path(), engine).await;
    kv_read_cache(addr, engine).await;
    pull_queue(addr, engine).await;
    push_to_a_guest_consumer(addr, engine).await;
}

/// TypeScript on QuickJS, the default engine (`npm run build` in
/// examples/ts-jobs).
#[tokio::test(flavor = "multi_thread")]
async fn typescript() {
    if let Some(app) = prebuilt("ts-jobs", "jobs.wasm") {
        scenario("quickjs", &app).await;
    }
}

/// The same app on StarlingMonkey (`npm run build:starlingmonkey`).
#[tokio::test(flavor = "multi_thread")]
async fn typescript_starlingmonkey() {
    if let Some(app) = prebuilt("ts-jobs", "jobs-starlingmonkey.wasm") {
        scenario("starlingmonkey", &app).await;
    }
}

/// Python with componentize-py (`./build.sh` in examples/py-jobs).
#[tokio::test(flavor = "multi_thread")]
async fn python() {
    if let Some(app) = prebuilt("py-jobs", "jobs.wasm") {
        scenario("componentize-py", &app).await;
    }
}

/// Go on TinyGo (`./build.sh` in examples/go-jobs).
#[tokio::test(flavor = "multi_thread")]
async fn go() {
    if let Some(app) = prebuilt("go-jobs", "jobs.wasm") {
        scenario("tinygo", &app).await;
    }
}

/// C, with the C SDK (`./build.sh` in examples/c-jobs).
#[tokio::test(flavor = "multi_thread")]
async fn c() {
    if let Some(app) = prebuilt("c-jobs", "jobs.wasm") {
        scenario("c", &app).await;
    }
}

/// Zig, with the Zig SDK over the C SDK (`./build.sh` in examples/zig-jobs).
#[tokio::test(flavor = "multi_thread")]
async fn zig() {
    if let Some(app) = prebuilt("zig-jobs", "jobs.wasm") {
        scenario("zig", &app).await;
    }
}
