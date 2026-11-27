//! `wasmkeep actor move`, run as a binary against a small S3 stand-in
//! that keeps its connections open, as real buckets do (moto closes each
//! one, so pooled connections are never reused against it).
//!
//! A request sent on a connection pooled by another runtime of the same
//! process, one that runs nothing while the command waits, hangs until
//! object_store's 30 s timeout: a move took 30 to 90 s that way. And a
//! bucket may answer 409 to one of two overwrites of `control/changed` at
//! once, which parallel moves must not report as a failure.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::collections::HashMap;
use std::net::SocketAddr;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};

/// What the stand-in answers and counts.
#[derive(Default)]
struct Bucket {
    objects: Mutex<HashMap<String, Vec<u8>>>,
    /// Whether each writer's first overwrite of `control/changed` is
    /// answered 409 (writers told apart by the pid in the body,
    /// `cli <pid> <nanos>`).
    conflict_first: bool,
    /// The writers that met their conflict.
    conflicted_pids: Mutex<std::collections::HashSet<String>>,
    /// Overwrites of `control/changed` taken.
    announced: AtomicUsize,
    /// Overwrites of `control/changed` answered 409.
    conflicted: AtomicUsize,
}

/// One connection, kept open for as many requests as the client sends.
async fn serve(sock: tokio::net::TcpStream, bucket: Arc<Bucket>) {
    let (read, mut write) = sock.into_split();
    let mut read = BufReader::new(read);
    loop {
        let mut line = String::new();
        if read.read_line(&mut line).await.unwrap_or(0) == 0 {
            return;
        }
        let mut parts = line.split_whitespace();
        let (method, target) = (
            parts.next().unwrap_or("").to_owned(),
            parts.next().unwrap_or("").to_owned(),
        );
        let mut length = 0;
        loop {
            let mut header = String::new();
            if read.read_line(&mut header).await.unwrap_or(0) == 0 {
                return;
            }
            let header = header.trim_end();
            if header.is_empty() {
                break;
            }
            if let Some((name, value)) = header.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                length = value.trim().parse().unwrap();
            }
        }
        let mut body = vec![0; length];
        if read.read_exact(&mut body).await.is_err() {
            return;
        }
        let (status, reply) = answer(&bucket, &method, &target, body);
        let head = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nContent-Type: application/xml\r\n\
             ETag: \"e\"\r\nLast-Modified: Thu, 26 Nov 2026 00:00:00 GMT\r\n\r\n",
            reply.len()
        );
        if write.write_all(head.as_bytes()).await.is_err() || write.write_all(&reply).await.is_err()
        {
            return;
        }
    }
}

fn answer(bucket: &Bucket, method: &str, target: &str, body: Vec<u8>) -> (&'static str, Vec<u8>) {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let key = path.trim_start_matches('/').to_owned();
    match method {
        // Every listing this command makes is of an empty folder here.
        "GET" if query.contains("list-type=2") => (
            "200 OK",
            b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>b</Name>\
              <KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>"
                .to_vec(),
        ),
        "GET" | "HEAD" => match bucket.objects.lock().unwrap().get(&key) {
            Some(body) => (
                "200 OK",
                if method == "GET" {
                    body.clone()
                } else {
                    Vec::new()
                },
            ),
            None => (
                "404 Not Found",
                b"<?xml version=\"1.0\"?><Error><Code>NoSuchKey</Code></Error>".to_vec(),
            ),
        },
        "PUT" => {
            if key.ends_with("/control/changed") {
                let pid = String::from_utf8_lossy(&body)
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("")
                    .to_owned();
                let conflict =
                    bucket.conflict_first && bucket.conflicted_pids.lock().unwrap().insert(pid);
                if conflict {
                    bucket.conflicted.fetch_add(1, Ordering::SeqCst);
                    return (
                        "409 Conflict",
                        b"<?xml version=\"1.0\"?><Error><Code>OperationAborted</Code></Error>"
                            .to_vec(),
                    );
                }
                bucket.announced.fetch_add(1, Ordering::SeqCst);
            }
            bucket.objects.lock().unwrap().insert(key, body);
            ("200 OK", Vec::new())
        }
        "DELETE" => {
            bucket.objects.lock().unwrap().remove(&key);
            ("204 No Content", Vec::new())
        }
        _ => ("405 Method Not Allowed", Vec::new()),
    }
}

/// Starts the stand-in on a runtime of its own; it lives as long as the
/// test process.
fn stand_in(bucket: Arc<Bucket>) -> SocketAddr {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            tx.send(listener.local_addr().unwrap()).unwrap();
            loop {
                let (sock, _) = listener.accept().await.unwrap();
                tokio::spawn(serve(sock, Arc::clone(&bucket)));
            }
        });
    });
    rx.recv().unwrap()
}

fn move_cmd(addr: SocketAddr, name: &str) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_wasmkeep"));
    cmd.args([
        "actor",
        "move",
        "--app",
        "acme/shop",
        &format!("room/{name}"),
        "--to",
        "node-b",
        "--force",
        "--bucket",
        "s3://b/p",
    ])
    .env_remove("AWS_SESSION_TOKEN")
    .env_remove("AWS_PROFILE")
    .env("AWS_ACCESS_KEY_ID", "test")
    .env("AWS_SECRET_ACCESS_KEY", "test")
    .env("AWS_REGION", "us-east-1")
    .env("AWS_ENDPOINT", format!("http://{addr}"))
    .env("AWS_ALLOW_HTTP", "true");
    cmd
}

fn succeeded_quietly(out: &Output) {
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{err}");
    assert!(!err.contains("warning"), "{err}");
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("Asked the owner"),
        "{err}"
    );
}

/// About ten requests on one bucket: well under a second here. The bound
/// leaves room for a loaded machine, and none for a 30 s timeout.
const BOUND: Duration = Duration::from_secs(10);

#[test]
fn a_move_reuses_its_connections_without_waiting_for_a_timeout() {
    let bucket = Arc::new(Bucket::default());
    let addr = stand_in(Arc::clone(&bucket));
    let started = Instant::now();
    let out = move_cmd(addr, "lobby").output().unwrap();
    let took = started.elapsed();
    succeeded_quietly(&out);
    assert!(took < BOUND, "actor move took {took:?}");
    assert_eq!(bucket.announced.load(Ordering::SeqCst), 1);
    assert!(
        bucket
            .objects
            .lock()
            .unwrap()
            .keys()
            .any(|k| k.contains("/state/acme/shop/moves/room/")),
        "no move request written"
    );
}

#[test]
fn parallel_moves_all_succeed_when_the_bucket_answers_a_conflict() {
    const MOVES: usize = 6;
    let bucket = Arc::new(Bucket {
        // Each move's first announce meets one, as if another were in flight.
        conflict_first: true,
        ..Bucket::default()
    });
    let addr = stand_in(Arc::clone(&bucket));
    let started = Instant::now();
    let children: Vec<_> = (0..MOVES)
        .map(|i| {
            move_cmd(addr, &format!("r{i}"))
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();
    for child in children {
        succeeded_quietly(&child.wait_with_output().unwrap());
    }
    let took = started.elapsed();
    assert!(took < BOUND, "{MOVES} parallel moves took {took:?}");
    assert_eq!(bucket.conflicted.load(Ordering::SeqCst), MOVES);
    assert_eq!(bucket.announced.load(Ordering::SeqCst), MOVES);
}
