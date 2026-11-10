//! Compares actor runtimes on the same workload: a counter, one object per
//! name, in SQLite (see `examples/counter`, `examples/ts-counter`,
//! `bench/workerd/counter.js`). WasmKeep targets also report their
//! component's size.
//!
//! Every target runs as its own server process and is driven over loopback
//! HTTP/1.1 keep-alive by the same client, so all are measured from outside,
//! the same way. Server memory and CPU time come from `/proc/<pid>`.
//!
//! ```text
//! compare [--quick] [--bucket] TARGET...
//! TARGET = wasmkeep-rust | wasmkeep-rust-durable | wasmkeep-zig | wasmkeep-c |
//! wasmkeep-go | wasmkeep-ts | wasmkeep-ts-starlingmonkey | wasmkeep-py | workerd
//! ```
//!
//! `wasmkeep-rust-durable` is the Rust counter built with its `durable`
//! feature: `POST` returns once the increment is durable (`exec-durable`),
//! fsynced on one node, in the bucket with `--bucket`.
//!
//! `--bucket` runs each WasmKeep target as a one-node cluster on the bucket
//! `WASMKEEP_COMPARE_BUCKET` names (`s3://BUCKET[/PREFIX]`, credentials and
//! endpoint from the `AWS_*` variables, as `wasmkeep serve` reads them), so
//! every write is also replicated to it; each target gets a prefix of its
//! own under it. Its rows are named `compare-bucket/...` rather than
//! `compare/...`: the same benchmark, another setup.
//!
//! Prints CSV rows `bench,value,unit` on stdout (`bench/run.sh` adds them to
//! the results file). Paths come from the environment, with defaults
//! relative to the repository root (the working directory):
//! `WASMKEEP_BIN`, `WORKERD_BIN`.
//!
//! Requires Bash for `bench/preflight.sh` and an inherited soft descriptor
//! limit of at least 4096 for the full workload or 1024 in `--quick` mode:
//! the server sizes its resident-actor cap from its limit (`fds::budget`),
//! and the workload's actors must all fit, or the benchmark would measure
//! evictions. The soft limit is checked because `workerd` uses it as is.
//! The preflight never raises the process's limits or changes the workload
//! to fit them.

// A benchmark harness: a broken setup should stop the run loudly.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// The bucket of a `--bucket` run (`s3://BUCKET[/PREFIX]`), with a prefix
/// unique to the run; unset otherwise.
static BUCKET: OnceLock<String> = OnceLock::new();

/// Connections the server may hold: the harness opens at most ten at once
/// (eight throughput clients and the setup), and every one beyond its
/// needs would come out of the descriptors left for resident actors.
const MAX_CONNECTIONS: usize = 64;
/// `wasmkeep_server::fds::RESERVED` and `PER_ACTOR`.
const FDS_RESERVED: usize = 256;
const FDS_PER_ACTOR: usize = 3;

#[derive(Clone, Copy)]
struct Workload {
    iters: usize,
    actors: usize,
    secs: u64,
}

impl Workload {
    fn for_quick(quick: bool) -> Self {
        if quick {
            Self {
                iters: 300,
                actors: 100,
                secs: 2,
            }
        } else {
            Self {
                iters: 3000,
                actors: 1000,
                secs: 8,
            }
        }
    }

    /// The smallest descriptor limit under which the server keeps every
    /// actor of the workload resident (plus the ready and warm-up actors),
    /// rounded up to an easy-to-set power of two. Mirrors the server's
    /// budget (`wasmkeep_server::fds`), checked by the tests below.
    fn minimum_descriptors(self) -> usize {
        (FDS_RESERVED + MAX_CONNECTIONS + FDS_PER_ACTOR * (self.actors + 2)).next_power_of_two()
    }
}

struct Server {
    child: Child,
    addr: SocketAddr,
    /// URL path of actor `name`.
    path: fn(&str) -> String,
    _dir: TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A scratch directory removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("wasmkeep-compare-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The server's stderr, kept in `target/compare-<label>.log` for diagnosis.
fn log_file(label: &str) -> Stdio {
    let _ = std::fs::create_dir_all("target");
    std::fs::File::create(format!("target/compare-{label}.log"))
        .map_or_else(|_| Stdio::null(), Stdio::from)
}

fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("port")
}

fn env_path(var: &str, default: &str) -> PathBuf {
    std::env::var_os(var).map_or_else(|| PathBuf::from(default), PathBuf::from)
}

fn wasmkeep(label: &str, wasm: &Path) -> Server {
    let dir = TempDir::new(label);
    let addr = free_addr();
    let bin = env_path("WASMKEEP_BIN", "target/release/wasmkeep");
    let mut command = Command::new(&bin);
    command
        .args(["serve", "--actor"])
        .arg(format!("counter={}", wasm.display()))
        .arg("--data-dir")
        .arg(dir.0.join("data"))
        .arg("--cache-dir")
        .arg(dir.0.join("cache"))
        .args(["--listen", &addr.to_string(), "--max-instances", "2000"])
        .args(["--max-connections", &MAX_CONNECTIONS.to_string()]);
    if let Some(bucket) = BUCKET.get() {
        // A one-node cluster: a node with a bucket is a cluster node. Plain
        // HTTP between nodes is accepted, as there are no others.
        command
            .arg("--bucket")
            .arg(format!("{bucket}/{label}"))
            .args(["--node-id", "compare", "--cluster-plaintext", "accept"])
            .arg("--advertise")
            .arg(format!("http://{addr}"));
    }
    let child = command
        .env("RUST_LOG", "warn")
        .stdout(log_file(label))
        .stderr(Stdio::null())
        .spawn()
        .unwrap_or_else(|e| panic!("{}: {e}", bin.display()));
    Server {
        child,
        addr,
        path: |name| format!("/actor/counter/{name}"),
        _dir: dir,
    }
}

fn workerd() -> Server {
    let dir = TempDir::new("workerd");
    let addr = free_addr();
    let src = PathBuf::from("bench/workerd");
    let config = std::fs::read_to_string(src.join("config.capnp"))
        .expect("bench/workerd/config.capnp")
        .replace("127.0.0.1:PORT", &addr.to_string());
    std::fs::write(dir.0.join("config.capnp"), config).unwrap();
    std::fs::copy(src.join("counter.js"), dir.0.join("counter.js")).unwrap();
    std::fs::create_dir_all(dir.0.join("do")).unwrap();
    let bin = std::env::var_os("WORKERD_BIN").map_or_else(
        || workerd_binary(Path::new("bench/workerd/node_modules")),
        PathBuf::from,
    );
    let bin = std::fs::canonicalize(&bin).unwrap_or(bin);
    let child = Command::new(&bin)
        .args(["serve", "config.capnp", "--experimental"])
        .current_dir(&dir.0)
        .stdout(Stdio::null())
        .stderr(log_file("workerd"))
        .spawn()
        .unwrap_or_else(|e| panic!("{}: {e}", bin.display()));
    Server {
        child,
        addr,
        path: |name| format!("/counter/{name}"),
        _dir: dir,
    }
}

/// The native `workerd` binary npm installed for this platform. The
/// `.bin/workerd` launcher is a Node script that starts it as a child: run
/// through the launcher, `/proc/<pid>` measures Node rather than workerd,
/// and stopping the launcher leaves workerd running. The launcher is the
/// fallback when no platform package is found (a warning says so).
fn workerd_binary(node_modules: &Path) -> PathBuf {
    let platform = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => Some("linux-64"),
        ("linux", "aarch64") => Some("linux-arm64"),
        ("macos", "x86_64") => Some("darwin-64"),
        ("macos", "aarch64") => Some("darwin-arm64"),
        _ => None,
    };
    if let Some(platform) = platform {
        let native = node_modules
            .join("@cloudflare")
            .join(format!("workerd-{platform}"))
            .join("bin/workerd");
        if native.is_file() {
            return native;
        }
    }
    eprintln!(
        "warning: no native workerd for this platform under {}; running the npm launcher \
         (its CPU and memory rows measure Node, and it may leave workerd running)",
        node_modules.display()
    );
    node_modules.join(".bin/workerd")
}

/// Keep-alive HTTP/1.1 client over a blocking socket.
struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    fn connect(addr: SocketAddr) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(Duration::from_secs(60)))?;
        Ok(Self {
            stream,
            buf: vec![0; 8192],
        })
    }

    /// Sends a request; returns the status once the whole response is in
    /// (chunked or with a content length).
    fn send(&mut self, method: &str, path: &str) -> u16 {
        write!(
            self.stream,
            "{method} {path} HTTP/1.1\r\nHost: bench\r\nContent-Length: 0\r\n\r\n"
        )
        .expect("write");
        let mut total = 0;
        loop {
            let n = self.stream.read(&mut self.buf[total..]).expect("read");
            assert!(n > 0, "connection closed");
            total += n;
            let data = &self.buf[..total];
            let Some(head_end) = find(data, b"\r\n\r\n") else {
                continue;
            };
            let head = std::str::from_utf8(&data[..head_end]).unwrap_or("");
            let status = head
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let body = &data[head_end + 4..];
            let length = head.lines().find_map(|l| {
                let (k, v) = l.split_once(':')?;
                k.eq_ignore_ascii_case("content-length")
                    .then(|| v.trim().parse::<usize>().ok())?
            });
            let done = match length {
                Some(len) => body.len() >= len,
                None => body.ends_with(b"0\r\n\r\n"),
            };
            if done {
                return status;
            }
        }
    }
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Resident memory and CPU time (user + system) of a process.
fn proc_stats(pid: u32) -> (u64, Duration) {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
    let rss_kb: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.split_whitespace().next()?.parse().ok())
        .unwrap_or(0);
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
    // Fields after the command name (which may contain spaces).
    let fields: Vec<&str> = stat
        .rsplit_once(')')
        .map_or("", |(_, r)| r)
        .split_whitespace()
        .collect();
    let ticks: u64 = [11, 12]
        .iter()
        .filter_map(|&i| fields.get(i)?.parse::<u64>().ok())
        .sum();
    // Linux reports CPU time in clock ticks of 1/100 s on every common config.
    (rss_kb * 1024, Duration::from_millis(ticks * 10))
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

fn row(target: &str, bench: &str, value: u128, unit: &str) {
    let group = if BUCKET.get().is_some() {
        "compare-bucket"
    } else {
        "compare"
    };
    println!("{group}/{target}/{bench},{value},{unit}");
}

fn wait_ready(server: &Server) -> Duration {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(300) {
        if let Ok(mut c) = Client::connect(server.addr)
            && c.send("GET", &(server.path)("ready")) == 200
        {
            return started.elapsed();
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("server did not become ready");
}

/// The counter component a WasmKeep target serves, with its server's label.
fn component(target: &str) -> Option<(&'static str, &'static str)> {
    Some(match target {
        "wasmkeep-rust" => (
            "rust",
            "examples/counter/target/wasm32-wasip2/release/counter.wasm",
        ),
        // `cargo build --release --target wasm32-wasip2 --features durable
        // --target-dir target/durable` in examples/counter.
        "wasmkeep-rust-durable" => (
            "rust-durable",
            "examples/counter/target/durable/wasm32-wasip2/release/counter.wasm",
        ),
        // TypeScript on QuickJS, the default engine. Before 2026-11-06 the
        // `wasmkeep-ts` rows were StarlingMonkey, then the default, and
        // QuickJS was `wasmkeep-ts-quickjs`.
        "wasmkeep-ts" => ("ts", "examples/ts-counter/counter.wasm"),
        // The same TypeScript counter, built with `--engine starlingmonkey`.
        "wasmkeep-ts-starlingmonkey" => (
            "ts-starlingmonkey",
            "examples/ts-counter/counter-starlingmonkey.wasm",
        ),
        // The same counter in every guest language, built by each example's
        // build.sh: the guest language is the only difference.
        "wasmkeep-zig" => ("zig", "examples/zig-counter/counter.wasm"),
        "wasmkeep-c" => ("c", "examples/c-counter/counter.wasm"),
        "wasmkeep-go" => ("go", "examples/go-counter/counter.wasm"),
        "wasmkeep-py" => ("py", "examples/py-counter/counter.wasm"),
        _ => return None,
    })
}

fn run(target: &str, workload: Workload) {
    let server = match (target, component(target)) {
        (_, Some((label, wasm))) => {
            let wasm = PathBuf::from(wasm);
            // A missing file is the server's error to report (a test may
            // stand in a fake server for it).
            if let Ok(meta) = std::fs::metadata(&wasm) {
                row(target, "component_size", u128::from(meta.len()), "bytes");
            }
            wasmkeep(label, &wasm)
        }
        ("workerd", None) => workerd(),
        (other, None) => panic!("unknown target {other}"),
    };
    let pid = server.child.id();
    let ready = wait_ready(&server);
    row(target, "startup_to_first_response", ready.as_nanos(), "ns");

    let Workload {
        iters,
        actors,
        secs,
    } = workload;
    let path = server.path;
    let mut client = Client::connect(server.addr).unwrap();

    // Latency to one resident actor, read and write.
    for (name, method) in [("warm_get", "GET"), ("warm_post", "POST")] {
        for _ in 0..100 {
            client.send(method, &path("warm"));
        }
        let mut samples = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t = Instant::now();
            assert_eq!(client.send(method, &path("warm")), 200);
            samples.push(t.elapsed());
        }
        samples.sort();
        row(
            target,
            &format!("{name}_p50"),
            percentile(&samples, 0.5).as_nanos(),
            "ns",
        );
        row(
            target,
            &format!("{name}_p99"),
            percentile(&samples, 0.99).as_nanos(),
            "ns",
        );
    }

    // Plain JavaScript work (examples/ts-counter/src/work.ts), to compare
    // the two engines where the counter does not: it spends almost no time
    // in JavaScript. One client, so the latency is the engine's time.
    if target.starts_with("wasmkeep-ts") {
        let work = format!("{}/work", path("warm"));
        for _ in 0..20 {
            assert_eq!(client.send("GET", &work), 200);
        }
        let mut samples = Vec::with_capacity(iters / 4);
        for _ in 0..(iters / 4).max(20) {
            let t = Instant::now();
            assert_eq!(client.send("GET", &work), 200);
            samples.push(t.elapsed());
        }
        samples.sort();
        for (name, p) in [("js_work_p50", 0.5), ("js_work_p99", 0.99)] {
            row(target, name, percentile(&samples, p).as_nanos(), "ns");
        }
    }

    // First request to new actors; memory they hold while resident.
    let (rss_before, _) = proc_stats(pid);
    let mut samples = Vec::with_capacity(actors);
    for i in 0..actors {
        let t = Instant::now();
        assert_eq!(client.send("POST", &path(&format!("new-{i}"))), 200);
        samples.push(t.elapsed());
    }
    samples.sort();
    row(
        target,
        "create_p50",
        percentile(&samples, 0.5).as_nanos(),
        "ns",
    );
    std::thread::sleep(Duration::from_millis(200));
    let (rss_after, _) = proc_stats(pid);
    row(
        target,
        "memory_per_resident_actor",
        u128::from(rss_after.saturating_sub(rss_before) / actors as u64),
        "bytes",
    );
    row(target, "memory_rss_total", u128::from(rss_after), "bytes");

    // Throughput: 8 clients, writes spread over the resident actors.
    let (_, cpu_before) = proc_stats(pid);
    let started = Instant::now();
    let deadline = started + Duration::from_secs(secs);
    let addr = server.addr;
    let handles: Vec<_> = (0..8)
        .map(|t| {
            std::thread::spawn(move || {
                let mut c = Client::connect(addr).unwrap();
                let mut n = 0u64;
                let mut i = t;
                while Instant::now() < deadline {
                    assert_eq!(c.send("POST", &path(&format!("new-{}", i % actors))), 200);
                    i += 8;
                    n += 1;
                }
                n
            })
        })
        .collect();
    let requests: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    let elapsed = started.elapsed();
    let (_, cpu_after) = proc_stats(pid);
    row(
        target,
        "throughput_8_clients",
        (requests as f64 / elapsed.as_secs_f64()) as u128,
        "req/s",
    );
    row(
        target,
        "server_cpu_per_request",
        (cpu_after - cpu_before).as_nanos() / u128::from(requests.max(1)),
        "ns",
    );
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let quick = args.iter().any(|a| a == "--quick");
    let targets: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();
    if targets.is_empty() {
        eprintln!(
            "usage: compare [--quick] [--bucket] wasmkeep-rust|wasmkeep-rust-durable|wasmkeep-zig|wasmkeep-c|wasmkeep-go|wasmkeep-ts|wasmkeep-ts-starlingmonkey|wasmkeep-py|workerd ..."
        );
        std::process::exit(2);
    }
    if args.iter().any(|a| a == "--bucket") {
        let Some(bucket) = std::env::var("WASMKEEP_COMPARE_BUCKET")
            .ok()
            .filter(|b| b.starts_with("s3://"))
        else {
            eprintln!("--bucket needs WASMKEEP_COMPARE_BUCKET=s3://BUCKET[/PREFIX]");
            std::process::exit(2);
        };
        let run = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let run = format!("compare-{run}-{}", std::process::id());
        let _ = BUCKET.set([bucket.trim_end_matches('/'), &run].join("/"));
    }
    let workload = Workload::for_quick(quick);
    match Command::new("bash")
        .arg("bench/preflight.sh")
        .arg(workload.minimum_descriptors().to_string())
        .status()
    {
        Ok(status) if status.success() => {}
        Ok(_) => std::process::exit(2),
        Err(error) => {
            eprintln!("cannot run benchmark preflight with Bash: {error}");
            std::process::exit(2);
        }
    }
    for target in targets {
        run(target, workload);
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn the_native_workerd_binary_is_preferred_to_the_npm_launcher() {
        let root = std::env::temp_dir().join(format!("compare-wd-{}", std::process::id()));
        let platform = match (std::env::consts::OS, std::env::consts::ARCH) {
            ("linux", "x86_64") => "linux-64",
            ("linux", "aarch64") => "linux-arm64",
            ("macos", "x86_64") => "darwin-64",
            ("macos", "aarch64") => "darwin-arm64",
            _ => return,
        };
        let native = root.join(format!("@cloudflare/workerd-{platform}/bin/workerd"));
        std::fs::create_dir_all(root.join(".bin")).unwrap();
        std::fs::write(root.join(".bin/workerd"), "#!/usr/bin/env node\n").unwrap();
        assert_eq!(super::workerd_binary(&root), root.join(".bin/workerd"));
        std::fs::create_dir_all(native.parent().unwrap()).unwrap();
        std::fs::write(&native, "binary").unwrap();
        assert_eq!(super::workerd_binary(&root), native);
        let _ = std::fs::remove_dir_all(&root);
    }

    use super::*;

    #[test]
    fn the_minimum_limit_keeps_every_actor_resident_on_the_server() {
        // What `wasmkeep serve --max-instances 2000` wants resident.
        let wanted = 2000 / 10 * 9;
        for quick in [false, true] {
            let workload = Workload::for_quick(quick);
            let limit = workload.minimum_descriptors() as u64;
            let (_, resident) = wasmkeep_server::fds::budget(limit, MAX_CONNECTIONS, wanted);
            assert!(
                resident >= workload.actors + 2,
                "{quick}: {resident} at {limit}"
            );
            // And the floor is not padded past the next power of two.
            let below = limit / 2;
            let (_, resident) = wasmkeep_server::fds::budget(below, MAX_CONNECTIONS, wanted);
            assert!(
                resident < workload.actors + 2,
                "{quick}: {limit} is not minimal"
            );
        }
        assert_eq!(Workload::for_quick(false).minimum_descriptors(), 4096);
        assert_eq!(Workload::for_quick(true).minimum_descriptors(), 1024);
        assert_eq!(FDS_RESERVED as u64, wasmkeep_server::fds::RESERVED);
        assert_eq!(FDS_PER_ACTOR as u64, wasmkeep_server::fds::PER_ACTOR);
    }
}
