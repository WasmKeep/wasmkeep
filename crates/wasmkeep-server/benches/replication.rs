//! The cost of replication, over loopback HTTP, one actor, one node:
//!
//! - `write/local`: a write with no cluster (SQLite commit only)
//! - `write/replicated`: the same write in a cluster; after the event its
//!   pages are captured, encoded as LTX and uploaded
//! - `write/durable`: an `exec-durable` write (test actor), which uploads
//!   inside the call and answers only once the upload completed
//!
//! The bucket is in memory, so these measure WasmKeep's own overhead
//! (capture, encode, bookkeeping). A real bucket adds its PUT round trip to
//! `durable` (typically 10-100 ms on S3/R2) and to the time before a plain
//! write is safe. Run `bench/run.sh` to record results as CSV.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // bench code

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService, ClusterConfig, LeaseConfig};

#[path = "../tests/support/guests.rs"]
mod guests;

/// Keep-alive HTTP/1.1 client over a blocking socket.
struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).unwrap();
        stream.set_nodelay(true).unwrap();
        Self {
            stream,
            buf: vec![0; 4096],
        }
    }

    /// POST `path`; returns once the chunked body has ended.
    fn post(&mut self, path: &str) {
        write!(
            self.stream,
            "POST {path} HTTP/1.1\r\nHost: bench\r\nContent-Length: 0\r\n\r\n"
        )
        .unwrap();
        let mut total = 0;
        loop {
            let n = self.stream.read(&mut self.buf[total..]).unwrap();
            assert!(n > 0, "connection closed");
            total += n;
            if self.buf[..total].ends_with(b"0\r\n\r\n") {
                assert!(
                    self.buf.starts_with(b"HTTP/1.1 200"),
                    "{}",
                    String::from_utf8_lossy(&self.buf[..total])
                );
                return;
            }
        }
    }
}

fn serve(rt: &tokio::runtime::Runtime, clustered: bool, data: &std::path::Path) -> SocketAddr {
    let counter = guests::build_guest("../../examples/counter", "counter");
    let test_actor = guests::build_guest("tests/guests/test-actor", "test_actor");
    let runtime = Runtime::new(&RuntimeConfig::default()).unwrap();
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
        if clustered {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let mut cluster = ClusterConfig::new("bench", format!("http://{addr}"), store);
            cluster.lease = LeaseConfig::default_safe();
            config.cluster = Some(cluster);
        }
        let service = ActorService::new(runtime, config, classes).unwrap();
        tokio::spawn(wasmkeep_server::serve(
            listener,
            service,
            std::future::pending(),
        ));
        addr
    })
}

fn benches(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entered = rt.enter();
    let (local_dir, cluster_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let mut local = Client::connect(serve(&rt, false, local_dir.path()));
    let mut cluster = Client::connect(serve(&rt, true, cluster_dir.path()));

    let mut group = c.benchmark_group("write");
    group
        .sample_size(30)
        .measurement_time(Duration::from_secs(8));
    group.bench_function("local", |b| {
        local.post("/actor/counter/w");
        b.iter(|| local.post("/actor/counter/w"));
    });
    group.bench_function("replicated", |b| {
        cluster.post("/actor/counter/w");
        b.iter(|| cluster.post("/actor/counter/w"));
    });
    group.bench_function("durable", |b| {
        cluster.post("/actor/t/d/durable");
        b.iter(|| cluster.post("/actor/t/d/durable"));
    });
    group.finish();
}

criterion_group!(replication, benches);
criterion_main!(replication);
