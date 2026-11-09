//! Workflow replay cost (ADR 0019): every event that moves a workflow
//! forward replays its recorded steps from the top, so its cost grows with
//! them, against the event's 50 ms CPU budget.
//!
//! `workflow/replay/<n>`: one run of an instance with `n` recorded steps,
//! parked on a sleep (the runner loads the steps, replays them, finds the
//! park, sets the alarm), through an `actors.call` from a driver actor.
//! `n` = 0 is the baseline: HTTP, the call, the runner's own reads.
//! Replay cost per step = (replay/n - replay/0) / n. Recording the steps
//! first is not measured.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // bench code

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService};

#[path = "../tests/support/guests.rs"]
mod guests;

const SIZES: [u64; 5] = [0, 10, 100, 1_000, 5_000];

/// Keep-alive HTTP/1.1 client; POSTs return the (chunked) body's text.
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
            buf: vec![0; 1 << 16],
        }
    }

    fn post(&mut self, path: &str) -> String {
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
                let text = String::from_utf8_lossy(&self.buf[..total]).into_owned();
                assert!(text.starts_with("HTTP/1.1 200"), "{text}");
                return text;
            }
        }
    }
}

/// The driver's call to `wf/n-<n>` at `path`.
fn via_driver(n: u64, path: &str) -> String {
    format!("/actor/t/driver/call?to=wf/n-{n}&path={path}")
}

fn benches(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entered = rt.enter();
    let runtime = Runtime::new(&RuntimeConfig::default()).unwrap();
    let mut classes = Vec::new();
    for (class, dir, name) in [
        ("wf", "tests/guests/wf-bench", "wf_bench"),
        ("t", "tests/guests/test-actor", "test_actor"),
    ] {
        let wasm = guests::build_guest(dir, name);
        let component = runtime.compile(&wasm).unwrap();
        classes.push((class.to_owned(), component, wasm));
    }
    let data = tempfile::tempdir().unwrap();
    let service = ActorService::new(runtime, ActorConfig::new(data.path()), classes).unwrap();
    let addr = rt.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(wasmkeep_server::serve(
            listener,
            service,
            std::future::pending(),
        ));
        addr
    });
    let mut client = Client::connect(addr);

    // Record every size's steps first: start, then run until parked (each
    // run records as many steps as its event allows).
    for n in SIZES {
        client.post(&via_driver(n, "/wasmkeep-workflow/v1/start"));
        let deadline = Instant::now() + Duration::from_secs(300);
        let mut runs = 0;
        while !client
            .post(&via_driver(n, "/bench/replay"))
            .contains("200:parked")
        {
            runs += 1;
            assert!(
                Instant::now() < deadline,
                "wf/n-{n} never parked ({runs} runs)"
            );
        }
        eprintln!("wf/n-{n}: recorded in {runs} runs");
    }

    let mut group = c.benchmark_group("workflow/replay");
    group
        .sample_size(20)
        .measurement_time(Duration::from_secs(5));
    for n in SIZES {
        let path = via_driver(n, "/bench/replay");
        group.bench_function(n.to_string(), |b| {
            b.iter(|| {
                let body = client.post(&path);
                assert!(body.contains("200:parked"), "{body}");
            });
        });
    }
    group.finish();
}

criterion_group!(workflow, benches);
criterion_main!(workflow);
