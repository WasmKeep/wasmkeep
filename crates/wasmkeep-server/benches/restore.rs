//! How restoring an actor from the bucket scales with the length of its
//! history. Two nodes share an in-memory bucket over
//! loopback: a writer builds one counter's history with N replicated writes,
//! and a restorer is asked for fresh actors whose bucket prefix holds a
//! copy of that history and which it has never run, as on failover or when
//! an actor wakes on another node. Each timed request takes the lease,
//! lists the prefix, plans, downloads and applies the chain, then answers a
//! counter GET.
//!
//! - `restore/new`: an actor with nothing in the bucket (lease, listing,
//!   instantiation; the baseline for the rows below)
//! - `restore/snapshots/N`: the history as the writer left it, with its
//!   periodic snapshot every 64 segments: the replay stays under 64
//!   segments, while the listing holds every segment ever written
//! - `restore/chain/N`: the same history with the periodic snapshots
//!   removed (only the first snapshot is left), so all N segments are
//!   replayed: the worst case, a history whose periodic snapshots failed
//! - `restore/collected/N`: the same history after garbage collection
//!   (ADR 0012): what `collectable` leaves after the last periodic
//!   snapshot, the snapshot, the segment at its position and the ones after
//! - `restore/new-after`: `restore/new` again, after all the rows above:
//!   if it moved, they carry drift (restored actors piling up on the node)
//!   rather than the cost of history length
//! - `list/N`: listing the prefix of the `snapshots` history alone, as
//!   restore does (twice, when it has segments to replay);
//!   `list/collected/N` the same after collection
//!
//! The bucket is in memory: a real bucket adds a round trip per GET and per
//! listing page of 1,000 keys. Each counter write changes one page, so a
//! segment is small: `chain` measures the fixed cost per segment. The
//! history copies are made, and removed with what the restored actor
//! uploads after answering, between the timed requests. Run `bench/run.sh` to record results as CSV.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // bench code

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, SamplingMode, criterion_group, criterion_main};
use futures_util::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService, ClusterConfig};

#[path = "../tests/support/guests.rs"]
mod guests;

/// History lengths, in replicated writes.
const SIZES: [u64; 4] = [10, 100, 1_000, 10_000];

/// Where the counter `name`'s objects live in the bucket.
fn prefix(name: &str) -> String {
    format!("actors/default/counter/{name}")
}

/// Keep-alive HTTP/1.1 client over a blocking socket.
struct Client {
    addr: SocketAddr,
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).unwrap();
        stream.set_nodelay(true).unwrap();
        Self {
            addr,
            stream,
            buf: vec![0; 4096],
        }
    }

    /// Sends `method path` with an empty body; returns the status and the
    /// raw body once it has ended, or `None` if the server closed first.
    fn send(&mut self, method: &str, path: &str) -> Option<(u16, String)> {
        write!(
            self.stream,
            "{method} {path} HTTP/1.1\r\nHost: bench\r\nContent-Length: 0\r\n\r\n"
        )
        .ok()?;
        let mut total = 0;
        loop {
            if total == self.buf.len() {
                self.buf.resize(total * 2, 0);
            }
            let n = self.stream.read(&mut self.buf[total..]).ok()?;
            if n == 0 {
                return None;
            }
            total += n;
            let got = &self.buf[..total];
            let Some(end) = got.windows(4).position(|w| w == b"\r\n\r\n") else {
                continue;
            };
            let head = std::str::from_utf8(&got[..end])
                .unwrap()
                .to_ascii_lowercase();
            let length = head
                .lines()
                .find_map(|l| l.strip_prefix("content-length:"))
                .map(|v| v.trim().parse::<usize>().unwrap());
            let done = match length {
                Some(length) => total >= end + 4 + length,
                None => got.ends_with(b"0\r\n\r\n"),
            };
            if done {
                let body = String::from_utf8_lossy(&got[end + 4..]).into_owned();
                return Some((head[9..12].parse().unwrap(), body));
            }
        }
    }

    /// Like `send`, and the answer must be a 200; returns the body.
    fn ok(&mut self, method: &str, path: &str) -> String {
        match self.send(method, path) {
            Some((200, body)) => body,
            other => panic!("{method} {path}: {other:?}"),
        }
    }

    /// Sends until a 200, reconnecting if the server closed: a node needs
    /// a heartbeat before it serves.
    fn until_ok(&mut self, method: &str, path: &str) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match self.send(method, path) {
                Some((200, _)) => return,
                Some(_) => {}
                None => *self = Self::connect(self.addr),
            }
            assert!(
                Instant::now() < deadline,
                "{method} {path} never answered 200"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

/// Starts node `id` in the cluster sharing `store`; returns its address.
fn node(
    rt: &tokio::runtime::Runtime,
    id: &str,
    store: &Arc<dyn ObjectStore>,
    data: &std::path::Path,
) -> SocketAddr {
    let counter = guests::build_guest("../../examples/counter", "counter");
    // A small pool: the restorer is asked for a fresh actor per iteration,
    // and past the pool the least recently active one hibernates.
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let classes = vec![(
        "counter".into(),
        runtime.compile(&counter).unwrap(),
        counter,
    )];
    let mut config = ActorConfig::new(data);
    config.mailbox.idle_timeout = Duration::from_secs(600);
    rt.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        config.cluster = Some(ClusterConfig::new(
            id,
            format!("http://{addr}"),
            Arc::clone(store),
        ));
        let service = ActorService::new(runtime, config, classes).unwrap();
        tokio::spawn(wasmkeep_server::serve(
            listener,
            service,
            std::future::pending(),
        ));
        addr
    })
}

/// One actor's objects, keyed relative to its prefix.
type History = Vec<(String, Bytes)>;

async fn read_history(store: &Arc<dyn ObjectStore>, name: &str) -> History {
    let base = StorePath::from(prefix(name));
    let metas: Vec<_> = store.list(Some(&base)).try_collect().await.unwrap();
    let mut history = Vec::with_capacity(metas.len());
    for meta in metas {
        let key = meta.location.as_ref()[base.as_ref().len() + 1..].to_owned();
        let bytes = store
            .get(&meta.location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        history.push((key, bytes));
    }
    history
}

async fn put_history(store: &Arc<dyn ObjectStore>, name: &str, history: &History) {
    let base = prefix(name);
    for (key, bytes) in history {
        let location = StorePath::from(format!("{base}/{key}"));
        store
            .put(&location, PutPayload::from(bytes.clone()))
            .await
            .unwrap();
    }
}

/// Waits until the actor `name`'s prefix holds `objects` objects, then
/// removes them all.
async fn clear(store: &Arc<dyn ObjectStore>, name: &str, objects: usize) {
    let base = StorePath::from(prefix(name));
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let metas: Vec<_> = store.list(Some(&base)).try_collect().await.unwrap();
        if metas.len() == objects {
            for meta in metas {
                store.delete(&meta.location).await.unwrap();
            }
            return;
        }
        assert!(
            metas.len() < objects && Instant::now() < deadline,
            "{name}: {} objects, expected {objects}",
            metas.len()
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// The history without its periodic snapshots: only the first remains.
fn without_periodic_snapshots(history: &History) -> History {
    let mut snapshots = history.iter().filter(|(k, _)| k.starts_with("snapshot/"));
    let first = snapshots.next().map(|(k, _)| k.clone());
    history
        .iter()
        .filter(|(k, _)| !k.starts_with("snapshot/") || Some(k) == first.as_ref())
        .cloned()
        .collect()
}

/// Segments after the history's last snapshot: what a restore replays.
fn replayed(history: &History) -> usize {
    let last = history
        .iter()
        .filter_map(|(k, _)| k.strip_prefix("snapshot/"))
        .max()
        .map(|k| k[..16].to_owned())
        .unwrap_or_default();
    history
        .iter()
        .filter_map(|(k, _)| k.strip_prefix("wal/"))
        .filter(|k| k[..16] > *last)
        .count()
}

/// The history after garbage collection (ADR 0012), from its last
/// snapshot: what the server's rule leaves once the window has passed.
fn collected(history: &History) -> History {
    use wasmkeep_cluster::replication::{Listing, collectable};
    let listing = Listing::from_keys(history.iter().map(|(k, _)| k.as_str()));
    let Some(last) = listing
        .snapshots
        .iter()
        .max_by_key(|s| (s.txid, s.generation))
        .copied()
    else {
        return history.clone();
    };
    let gone = collectable(&listing, last.generation, last);
    history
        .iter()
        .filter(|(k, _)| !gone.contains(k))
        .cloned()
        .collect()
}

fn count(history: &History, kind: &str) -> usize {
    history.iter().filter(|(k, _)| k.starts_with(kind)).count()
}

#[allow(clippy::too_many_lines)]
fn benches(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entered = rt.enter();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let mut writer = Client::connect(node(&rt, "writer", &store, dirs[0].path()));
    let mut restorer = Client::connect(node(&rt, "restorer", &store, dirs[1].path()));
    writer.until_ok("GET", "/actor/counter/w");
    restorer.until_ok("GET", "/actor/counter/warm");

    // The writer's history after each size, read back from the bucket.
    let mut histories = Vec::new();
    let mut written = 0;
    for n in SIZES {
        while written < n {
            writer.ok("POST", "/actor/counter/w");
            written += 1;
        }
        // A write is answered before its segment is uploaded: wait for the
        // last one. One segment per write plus one for `on_init` (the
        // table), the first snapshot (the empty database, taken before
        // `on_init`), and a snapshot every 64 segments after it.
        let segments = n + 1;
        let snapshots = 1 + segments / 64;
        let deadline = Instant::now() + Duration::from_secs(30);
        let history = loop {
            let history = rt.block_on(read_history(&store, "w"));
            if count(&history, "wal/") as u64 == segments
                && count(&history, "snapshot/") as u64 == snapshots
            {
                break history;
            }
            assert!(
                Instant::now() < deadline,
                "history of {n} writes incomplete"
            );
            std::thread::sleep(Duration::from_millis(10));
        };
        let chain = without_periodic_snapshots(&history);
        assert_eq!(count(&chain, "snapshot/"), 1);
        assert_eq!(replayed(&chain) as u64, segments);
        assert_eq!(replayed(&history) as u64, segments % 64);
        let kept = collected(&history);
        assert_eq!(
            replayed(&kept),
            replayed(&history),
            "collection replays the same"
        );
        histories.push((n, history, chain, kept));
    }

    // Each restored copy must answer the writer's value. After answering,
    // the restored actor uploads in the background: a new actor its first
    // snapshot and its `on_init` segment, an actor restored from 64
    // segments or more a periodic snapshot. Each iteration waits for those
    // (untimed) so they do not overlap the next one, then clears the prefix.
    let mut fresh = 0u64;
    let mut restore_once = |client: &mut Client, history: &History| {
        fresh += 1;
        let name = format!("r{fresh}");
        rt.block_on(put_history(&store, &name, history));
        let start = Instant::now();
        let body = client.ok("GET", &format!("/actor/counter/{name}"));
        let took = start.elapsed();
        let replayed = replayed(history);
        let uploads = if history.is_empty() {
            2
        } else {
            usize::from(replayed >= 64)
        };
        rt.block_on(clear(&store, &name, history.len() + uploads));
        (took, body)
    };
    for (n, history, chain, kept) in &histories {
        for h in [history, chain, kept] {
            let (_, body) = restore_once(&mut restorer, h);
            assert!(
                body.split_whitespace().any(|t| t == n.to_string()),
                "restored {n}: {body}"
            );
        }
    }

    let mut group = c.benchmark_group("restore");
    group
        .sample_size(10)
        .sampling_mode(SamplingMode::Flat)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(10));
    let empty = History::new();
    group.bench_function("new", |b| {
        b.iter_custom(|iters| {
            (0..iters)
                .map(|_| restore_once(&mut restorer, &empty).0)
                .sum()
        });
    });
    for (n, history, chain, kept) in &histories {
        group.bench_with_input(BenchmarkId::new("snapshots", n), history, |b, h| {
            b.iter_custom(|iters| (0..iters).map(|_| restore_once(&mut restorer, h).0).sum());
        });
        group.bench_with_input(BenchmarkId::new("chain", n), chain, |b, h| {
            b.iter_custom(|iters| (0..iters).map(|_| restore_once(&mut restorer, h).0).sum());
        });
        group.bench_with_input(BenchmarkId::new("collected", n), kept, |b, h| {
            b.iter_custom(|iters| (0..iters).map(|_| restore_once(&mut restorer, h).0).sum());
        });
    }
    // The baseline again, after hundreds of restores: if it moved, the
    // rows above carry drift (actors piling up), not history length.
    group.bench_function("new-after", |b| {
        b.iter_custom(|iters| {
            (0..iters)
                .map(|_| restore_once(&mut restorer, &empty).0)
                .sum()
        });
    });
    group.finish();

    let mut group = c.benchmark_group("list");
    group
        .sample_size(20)
        .measurement_time(Duration::from_secs(5));
    for (n, _, _, kept) in &histories {
        let name = format!("listc{n}");
        rt.block_on(put_history(&store, &name, kept));
        let base = StorePath::from(prefix(&name));
        group.bench_with_input(BenchmarkId::new("collected", n), &base, |b, base| {
            b.iter(|| {
                let metas: Vec<_> = rt
                    .block_on(store.list(Some(base)).try_collect::<Vec<_>>())
                    .unwrap();
                assert_eq!(metas.len(), kept.len());
            });
        });
    }
    for (n, history, _, _) in &histories {
        let name = format!("list{n}");
        rt.block_on(put_history(&store, &name, history));
        let base = StorePath::from(prefix(&name));
        group.bench_with_input(BenchmarkId::from_parameter(n), &base, |b, base| {
            b.iter(|| {
                let metas: Vec<_> = rt
                    .block_on(store.list(Some(base)).try_collect::<Vec<_>>())
                    .unwrap();
                assert_eq!(metas.len(), history.len());
            });
        });
    }
    group.finish();
}

criterion_group!(restore, benches);
criterion_main!(restore);
