//! Performance targets with the real hello component:
//! - instantiation of a cached component, p50 under 50 µs
//! - request overhead over raw hyper, p50 under 200 µs
//!
//! Run `bench/run.sh` to record results as CSV.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // bench code

use std::convert::Infallible;
use std::hint::black_box;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use http_body_util::Full;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use wasmkeep_runtime::{ComponentCache, Runtime, RuntimeConfig};
use wasmkeep_server::HttpComponent;

#[path = "../tests/support/guests.rs"]
mod guests;

/// Keep-alive HTTP/1.1 client over a blocking socket: one request, reads
/// until the end of the chunked or fixed-length body.
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

    fn get(&mut self, marker: &[u8]) -> usize {
        self.stream
            .write_all(b"GET / HTTP/1.1\r\nHost: bench\r\n\r\n")
            .unwrap();
        let mut total = 0;
        loop {
            let n = self.stream.read(&mut self.buf[total..]).unwrap();
            assert!(n > 0, "connection closed");
            total += n;
            if self.buf[..total].ends_with(marker) {
                return total;
            }
        }
    }
}

fn hello(runtime: &Runtime) -> HttpComponent {
    let wasm = guests::build_guest("../../examples/hello", "hello");
    let dir = tempfile::tempdir().unwrap();
    let cache = ComponentCache::new(runtime.clone(), dir.path()).unwrap();
    cache.get_or_compile(&wasm).unwrap();
    // Second call loads the pre-compiled artifact, as a restarted node would.
    let component = cache.get_or_compile(&wasm).unwrap();
    HttpComponent::new(runtime.clone(), &component).unwrap()
}

fn benches(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let runtime = Runtime::new(&RuntimeConfig::default()).unwrap();
    let component = hello(&runtime);

    let mut group = c.benchmark_group("hello");

    group.bench_function("instantiate", |b| {
        b.iter(|| rt.block_on(component.instantiate_once()).unwrap());
    });

    // Full round trip through `wasmkeep_server::serve` on loopback.
    let wasmkeep_addr = rt.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(wasmkeep_server::serve(
            listener,
            component.clone(),
            std::future::pending(),
        ));
        addr
    });
    let mut client = Client::connect(wasmkeep_addr);
    group.bench_function("request/wasmkeep", |b| {
        b.iter(|| black_box(client.get(b"0\r\n\r\n")));
    });

    // Baseline: plain hyper with a static body of the same size.
    let raw_addr = rt.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let service = service_fn(|_req| async {
                        Ok::<_, Infallible>(hyper::Response::new(Full::new(Bytes::from_static(
                            b"Hello from WasmKeep! You sent GET /\n",
                        ))))
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        addr
    });
    let mut client = Client::connect(raw_addr);
    group.bench_function("request/raw-hyper", |b| {
        b.iter(|| black_box(client.get(b"You sent GET /\n")));
    });

    group.finish();
}

criterion_group!(http, benches);
criterion_main!(http);
