//! Actor wake-up cost with the real counter example, over loopback HTTP.
//! Target: wake from hibernation (instantiate plus open
//! SQLite) under 1 ms.
//!
//! - `actor/warm`: GET to one resident actor (baseline: HTTP + one event)
//! - `actor/wake`: GET to a hibernated actor (reopen its database,
//!   instantiate the component; on-init does not rerun)
//! - `actor/create`: first GET to a new actor (also creates the database and
//!   runs on-init)
//!
//! Wake cost = wake - warm. Run `bench/run.sh` to record results as CSV.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // bench code

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService};

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

    /// GET `path`; returns once the chunked body has ended.
    fn get(&mut self, path: &str) {
        write!(self.stream, "GET {path} HTTP/1.1\r\nHost: bench\r\n\r\n").unwrap();
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

fn benches(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entered = rt.enter();
    let wasm = guests::build_guest("../../examples/counter", "counter");
    let runtime = Runtime::new(&RuntimeConfig::default()).unwrap();
    let component = runtime.compile(&wasm).unwrap();
    let data = tempfile::tempdir().unwrap();
    let mut config = ActorConfig::new(data.path());
    config.mailbox.idle_timeout = Duration::from_millis(20);
    let service =
        ActorService::new(runtime, config, vec![("counter".into(), component, wasm)]).unwrap();
    let addr = rt.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(wasmkeep_server::serve(
            listener,
            service.clone(),
            std::future::pending(),
        ));
        addr
    });
    let mut client = Client::connect(addr);

    let mut group = c.benchmark_group("actor");
    group
        .sample_size(30)
        .measurement_time(Duration::from_secs(8));

    // Warm: the same actor stays resident (requests come faster than the
    // idle timeout).
    group.bench_function("warm", |b| {
        client.get("/actor/counter/warm");
        b.iter(|| client.get("/actor/counter/warm"));
    });

    let batch = AtomicU64::new(0);
    let wait_hibernated = |service: &ActorService| {
        let deadline = Instant::now() + Duration::from_secs(10);
        while service.resident() > 0 {
            assert!(Instant::now() < deadline, "actors did not hibernate");
            std::thread::sleep(Duration::from_millis(10));
        }
    };

    group.bench_function("create", |b| {
        b.iter_custom(|iters| {
            let n = batch.fetch_add(1, Ordering::Relaxed);
            let start = Instant::now();
            for i in 0..iters {
                client.get(&format!("/actor/counter/c{n}-{i}"));
            }
            start.elapsed()
        });
    });

    group.bench_function("wake", |b| {
        b.iter_custom(|iters| {
            let n = batch.fetch_add(1, Ordering::Relaxed);
            // Create the actors, then let every one of them hibernate.
            for i in 0..iters {
                client.get(&format!("/actor/counter/w{n}-{i}"));
            }
            std::thread::sleep(Duration::from_millis(60));
            wait_hibernated(&service);
            let start = Instant::now();
            for i in 0..iters {
                client.get(&format!("/actor/counter/w{n}-{i}"));
            }
            start.elapsed()
        });
    });

    group.finish();
}

criterion_group!(actors, benches);
criterion_main!(actors);
