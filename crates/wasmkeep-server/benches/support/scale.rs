//! Shared by the scale benches (`restart.rs`, `alarm_recovery.rs`): a
//! blocking keep-alive HTTP client, a node on its own Tokio runtime (so it
//! can be stopped as a crash stops a process), and the alarm events the
//! process has run, read from the metrics page.

#![allow(dead_code, reason = "not every bench uses every helper")]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService};

#[path = "../../tests/support/guests.rs"]
mod guests;

/// The test actor (`tests/guests/test-actor`): `POST /alarm?at=MS` sets an
/// alarm, `on-alarm` records a run in its database.
pub(crate) fn test_actor() -> &'static [u8] {
    static WASM: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    WASM.get_or_init(|| guests::build_guest("tests/guests/test-actor", "test_actor"))
}

/// Keep-alive HTTP/1.1 client over a blocking socket.
pub(crate) struct Client {
    addr: SocketAddr,
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    pub(crate) fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).unwrap();
        stream.set_nodelay(true).unwrap();
        Self {
            addr,
            stream,
            buf: vec![0; 4096],
        }
    }

    /// Sends `method path` with an empty body; returns the status, or
    /// `None` if the server closed first.
    pub(crate) fn send(&mut self, method: &str, path: &str) -> Option<u16> {
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

    /// Sends until a 200, reconnecting if the server closed or is not
    /// listening yet; `pause` between attempts.
    pub(crate) fn until_ok(addr: SocketAddr, method: &str, path: &str, pause: Duration) -> Self {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if let Ok(stream) = TcpStream::connect(addr) {
                stream.set_nodelay(true).unwrap();
                let mut client = Self {
                    addr,
                    stream,
                    buf: vec![0; 4096],
                };
                loop {
                    match client.send(method, path) {
                        Some(200) => return client,
                        Some(_) => std::thread::sleep(pause),
                        None => break,
                    }
                    assert!(Instant::now() < deadline, "{method} {path}: never 200");
                }
            }
            assert!(Instant::now() < deadline, "{method} {path}: never 200");
            std::thread::sleep(pause);
        }
    }

    /// Like `send`, and the answer must be a 200 (reconnecting once if the
    /// server closed an idle connection).
    pub(crate) fn ok(&mut self, method: &str, path: &str) {
        let status = self.send(method, path).or_else(|| {
            *self = Self::connect(self.addr);
            self.send(method, path)
        });
        assert_eq!(status, Some(200), "{method} {path}");
    }
}

/// Sends `POST <path_of(i)>` for every `i` in `0..n` over `threads`
/// connections; every answer must be a 200.
pub(crate) fn post_all(
    addr: SocketAddr,
    n: usize,
    threads: usize,
    path_of: impl Fn(usize) -> String + Sync,
) {
    std::thread::scope(|s| {
        for t in 0..threads {
            let path_of = &path_of;
            s.spawn(move || {
                let mut client = Client::connect(addr);
                for i in (t..n).step_by(threads) {
                    client.ok("POST", &path_of(i));
                }
            });
        }
    });
}

/// A node running on its own Tokio runtime: `kill` drops every task at once,
/// as a crash would (nothing released, nothing flushed beyond what was).
pub(crate) struct Node {
    pub(crate) addr: SocketAddr,
    rt: Option<tokio::runtime::Runtime>,
}

impl Node {
    /// Starts a node serving the test actor as class `t`, with `config`;
    /// returns once it listens (it may not serve yet).
    pub(crate) fn start(mut config: ActorConfig) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let wasm = test_actor();
        let addr = rt.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            if let Some(cluster) = config.cluster.as_mut() {
                cluster.advertise = format!("http://{addr}");
            }
            let runtime = Runtime::new(&RuntimeConfig::default()).unwrap();
            let component = guests::compile(&runtime, wasm);
            let classes = vec![("t".to_owned(), component, wasm.to_vec())];
            let service = ActorService::new(runtime, config, classes).unwrap();
            tokio::spawn(wasmkeep_server::serve(
                listener,
                service,
                std::future::pending(),
            ));
            addr
        });
        Self { addr, rt: Some(rt) }
    }

    /// Stops every task now, as a crash would. Blocking threads (an
    /// SQLite statement, the start-up alarm scan) are not waited for.
    pub(crate) fn kill(mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }

    /// Stops every task, then waits (up to a minute) for blocking threads
    /// still running, such as the start-up alarm scan, so they do not
    /// overlap what is measured next.
    pub(crate) fn stop_and_drain(mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_timeout(Duration::from_secs(60));
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}

/// Successful `on-alarm` runs in this process so far, every node together
/// (the metrics are per process).
pub(crate) fn alarms_fired() -> u64 {
    wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default())
        .lines()
        .filter(|l| {
            l.starts_with("wasmkeep_events_total{")
                && l.contains("kind=\"alarm\"")
                && l.contains("outcome=\"ok\"")
        })
        .filter_map(|l| l.rsplit(' ').next()?.parse::<u64>().ok())
        .sum()
}

/// Alarm runs refused by admission and retried, in this process so far.
pub(crate) fn admission_retries() -> u64 {
    wasmkeep_server::metrics::render(wasmkeep_server::metrics::Gauges::default())
        .lines()
        .find_map(|l| l.strip_prefix("wasmkeep_alarm_admission_retries_total "))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// Milliseconds since the epoch.
pub(crate) fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}
