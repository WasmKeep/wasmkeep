//! The cost of a hop between nodes, plain HTTP against mutual TLS
//! (ADR 0011), over loopback, two nodes sharing an in-memory bucket:
//!
//! - `cluster/direct`: a counter GET sent to the node that owns the actor
//!   (no hop; the baseline for the rows below)
//! - `cluster/forward/{plain,tls}`: the same GET sent to the other node,
//!   which forwards it to the owner and relays the answer
//! - `cluster/call/{plain,tls}`: a request to an actor on one node whose
//!   handler makes an `actors.call` to an actor on the other node
//!
//! In the TLS pair each node publishes a plain address nobody listens on,
//! so every hop between them must go over the cluster listener. Clients
//! keep their connections alive, and nodes pool theirs, so the rows
//! measure a hop on an established connection; a new connection's
//! handshake is not in them. Production timings (lease ttl 10 s, heartbeat
//! 3 s). Run `bench/run.sh` to record results as CSV.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // bench code

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService, ClusterConfig, ClusterTlsConfig, ServeLimits};

#[path = "../tests/support/guests.rs"]
mod guests;

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

    /// Sends `method path` with an empty body; returns the status code once
    /// the body has ended (chunked or with a length), or `None` if the
    /// server closed the connection first.
    fn send(&mut self, method: &str, path: &str) -> Option<u16> {
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
                return Some(head[9..12].parse().unwrap());
            }
        }
    }

    /// Like `send`, and the answer must be a 200.
    fn ok(&mut self, method: &str, path: &str) {
        let status = self.send(method, path);
        assert_eq!(status, Some(200), "{method} {path}");
    }

    /// Sends until a 200, reconnecting if the server closed: the nodes need
    /// a heartbeat or two to see each other (over TLS, readiness waits a
    /// period and a half).
    fn until_ok(&mut self, method: &str, path: &str) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match self.send(method, path) {
                Some(200) => return,
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

/// Starts node `id` in the cluster sharing `store`, plain or behind a
/// cluster listener; returns its public address.
fn node(
    rt: &tokio::runtime::Runtime,
    id: &str,
    store: &Arc<dyn ObjectStore>,
    tls: bool,
    data: &std::path::Path,
) -> SocketAddr {
    let counter = guests::build_guest("../../examples/counter", "counter");
    let test_actor = guests::build_guest("tests/guests/test-actor", "test_actor");
    // Four nodes in one process: a small pool each (the default reserves
    // address space for many more instances than these few actors).
    let runtime = Runtime::new(&RuntimeConfig {
        max_instances: 16,
        ..RuntimeConfig::default()
    })
    .unwrap();
    let classes = vec![
        (
            "counter".into(),
            runtime.compile(&counter).unwrap(),
            counter,
        ),
        (
            "t".into(),
            runtime.compile(&test_actor).unwrap(),
            test_actor,
        ),
    ];
    let mut config = ActorConfig::new(data);
    config.mailbox.idle_timeout = Duration::from_secs(600);
    rt.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Over TLS, the plain address leads nowhere (port 9 is discard), so
        // a hop in plaintext fails the bench instead of being measured.
        let advertise = if tls {
            "http://127.0.0.1:9".to_owned()
        } else {
            format!("http://{addr}")
        };
        let mut cluster = ClusterConfig::new(id, advertise, Arc::clone(store));
        let cluster_listener = if tls {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let advertise = format!("https://{}", l.local_addr().unwrap());
            cluster.cluster_tls = Some(ClusterTlsConfig::new(advertise));
            Some(l)
        } else {
            None
        };
        config.cluster = Some(cluster);
        let service = ActorService::new(runtime, config, classes).unwrap();
        match cluster_listener {
            Some(cluster_listener) => {
                tokio::spawn(wasmkeep_server::serve_with_cluster_listener(
                    listener,
                    cluster_listener,
                    service,
                    std::future::pending(),
                    ServeLimits::default(),
                ));
            }
            None => {
                tokio::spawn(wasmkeep_server::serve(
                    listener,
                    service,
                    std::future::pending(),
                ));
            }
        }
        addr
    })
}

/// A pair of nodes: `owner` holds the counter `c` and the test actor `y`;
/// `other` holds the test actor `x`. Returns a client to each.
struct Pair {
    owner: Client,
    other: Client,
}

fn pair(rt: &tokio::runtime::Runtime, tls: bool, dirs: &[tempfile::TempDir; 2]) -> Pair {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut owner = Client::connect(node(rt, "a", &store, tls, dirs[0].path()));
    let mut other = Client::connect(node(rt, "b", &store, tls, dirs[1].path()));
    // The first node an actor is asked for takes its lease.
    owner.ok("GET", "/actor/counter/c");
    owner.ok("POST", "/actor/t/y/caller");
    other.ok("POST", "/actor/t/x/caller");
    other.until_ok("GET", "/actor/counter/c");
    other.until_ok("POST", CALL);
    Pair { owner, other }
}

/// `x` (on the other node) calls `y` (on the owner), which answers its
/// caller header.
const CALL: &str = "/actor/t/x/call?to=t/y&path=%2Fcaller";

fn benches(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entered = rt.enter();
    let dirs = || [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let (plain_dirs, tls_dirs) = (dirs(), dirs());
    let mut plain = pair(&rt, false, &plain_dirs);
    let mut tls = pair(&rt, true, &tls_dirs);

    let mut group = c.benchmark_group("cluster");
    group
        .sample_size(30)
        .measurement_time(Duration::from_secs(8));
    group.bench_function("direct", |b| {
        b.iter(|| plain.owner.ok("GET", "/actor/counter/c"));
    });
    group.bench_function("forward/plain", |b| {
        b.iter(|| plain.other.ok("GET", "/actor/counter/c"));
    });
    group.bench_function("forward/tls", |b| {
        b.iter(|| tls.other.ok("GET", "/actor/counter/c"));
    });
    group.bench_function("call/plain", |b| {
        b.iter(|| plain.other.ok("POST", CALL));
    });
    group.bench_function("call/tls", |b| {
        b.iter(|| tls.other.ok("POST", CALL));
    });
    group.finish();
    // Keep the TLS owner's client alive to the end (its node stays served).
    drop(tls.owner);
}

criterion_group!(cluster, benches);
criterion_main!(cluster);
