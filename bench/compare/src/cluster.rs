//! `compare --cluster`: the same counter on three nodes of each runtime,
//! sharing the run's bucket, every write durable as each runtime defines it.
//!
//! - `wasmkeep-rust-durable`: three `wasmkeep serve` nodes (`exec-durable`,
//!   a write answers once the bucket holds it). `wasmkeep-rust`, plain
//!   `exec`, answers before its upload: not equivalent, reported apart.
//! - `celld-fleet`: three celld nodes, `CELLD_DURABILITY=fleet` (celld's
//!   default): a write answers once a follower node has it on disk, or once
//!   the bucket has it, whichever comes first.
//! - `celld-bucket`: the same with `CELLD_DURABILITY=bucket`: a write
//!   answers once the bucket has it.
//!
//! Both runtimes keep their default lease timings and talk plain HTTP
//! between nodes (celld has no TLS between peers). Every node keeps its data
//! on disk under `target/compare-cluster/` (a follower's fsync on tmpfs
//! would cost nothing). Every request goes to a node picked at random, so
//! two in three are forwarded to the actor's owner.
//!
//! After the steady-state rows (as in a one-node run), each node in turn is
//! killed with SIGKILL while one client per victim writes to it every 20 ms,
//! retrying every 100 ms through a random node until a write is
//! acknowledged. A victim whose longest wait between acknowledgements after
//! the kill exceeds [`AFFECTED`] was on the dead node: the end of that wait
//! is its takeover. The killed node is then restarted on its data.
//!
//! Victims are ledgers, not counters (`bench/ledger`, and the `Ledger`
//! class of `bench/celld/cluster.js`): every attempt writes a token of its
//! own, never sent twice. A counter's increments are interchangeable, so a
//! write that applied without an acknowledgement would hide an acknowledged
//! one that was lost. After each restart, and once more after the last one,
//! every victim's tokens are read back: an acknowledged token that is
//! missing is a lost write ([`audit`]), and a token present twice was
//! applied twice. The counter serves the steady-state rows unchanged.
//!
//! SIGKILL leaves the dead node's files, page cache included. With
//! [`Fault::Wipe`] the node's directory is removed before it restarts, so
//! what it alone held is gone (`cluster3-wipe/...`, takeover rows only).
//! [`Fault::Control`] is the run that must lose writes, to show the check
//! finds them (`cluster3-control/...`): plain `exec`, which answers before
//! its upload, on nodes that reach the bucket through a relay of the
//! harness. Half a second before the kill the relay cuts the node off, so
//! each of its actors acknowledges one write the bucket never gets; the
//! node is then killed and wiped. Without the cut the loss is rare: a small
//! upload has usually left the node by the time its answer reaches the
//! client. The relay forwards plain TCP, so the control needs an `http://`
//! endpoint in `AWS_ENDPOINT`.
//!
//! Settings that differ from each runtime's defaults: WasmKeep nodes run
//! with `--max-instances 2000`, `--max-connections 512` and
//! `--idle-timeout-secs 3600` (no actor hibernates during the run); celld
//! nodes with transparent huge pages off, as WasmKeep turns them off for
//! itself.
//!
//! Rows are `<prefix>/<target>/<bench>`.

use super::*;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

const NODES: usize = 3;
/// A takeover takes seconds (lease timeouts of 10 s and more); a victim on a
/// surviving node answers in milliseconds.
const AFFECTED: Duration = Duration::from_secs(1);
const WRITE_EVERY: Duration = Duration::from_millis(20);
const RETRY_EVERY: Duration = Duration::from_millis(100);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// How long every victim gets to be writable again after a kill.
const TAKEOVER_LIMIT: Duration = Duration::from_secs(120);
/// Connections a WasmKeep node may hold: one per victim client and node,
/// plus the throughput clients and the setup.
const CLUSTER_MAX_CONNECTIONS: usize = 512;

/// The descriptor limit a cluster run needs: [`Workload::minimum_descriptors`]
/// with the cluster's connections (a surviving node may hold every actor).
pub(crate) fn minimum_descriptors(workload: Workload) -> usize {
    (FDS_RESERVED + CLUSTER_MAX_CONNECTIONS + FDS_PER_ACTOR * (workload.actors + 2))
        .next_power_of_two()
}

/// What happens to a node in a takeover.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fault {
    /// SIGKILL: the node restarts on its files.
    Kill,
    /// SIGKILL, then its directory is removed: it restarts empty.
    Wipe,
    /// [`Fault::Wipe`] of a node cut off from the bucket just before: with
    /// plain `exec` this must lose acknowledged writes.
    Control,
}

impl Fault {
    fn prefix(self) -> &'static str {
        match self {
            Self::Kill => "cluster3",
            Self::Wipe => "cluster3-wipe",
            Self::Control => "cluster3-control",
        }
    }
}

struct Node {
    addr: SocketAddr,
    /// Everything the node keeps on disk.
    dir: PathBuf,
    start: Box<dyn Fn() -> Child>,
    child: Option<Child>,
}

impl Node {
    fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }

    /// Kills the node; returns when the signal was sent.
    fn kill(&mut self) -> Instant {
        let Some(mut child) = self.child.take() else {
            return Instant::now();
        };
        let _ = child.kill();
        let killed = Instant::now();
        let _ = child.wait();
        killed
    }
}

struct Cluster {
    nodes: Vec<Node>,
    path: fn(&str) -> String,
    /// A ledger's path, by name.
    ledger: fn(&str) -> String,
    /// Each node's way to the bucket, in a control run.
    relays: Vec<Relay>,
    _dir: TempDir,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            node.kill();
        }
    }
}

impl Cluster {
    fn addrs(&self) -> Vec<SocketAddr> {
        self.nodes.iter().map(|n| n.addr).collect()
    }

    /// Resident memory and CPU time summed over the running nodes.
    fn stats(&self) -> (u64, Duration) {
        self.nodes
            .iter()
            .filter_map(Node::pid)
            .map(proc_stats)
            .fold((0, Duration::ZERO), |(m, c), (m2, c2)| (m + m2, c + c2))
    }
}

/// A scratch directory, removed on drop: on the repository's disk (not
/// tmpfs), or under `CLUSTER_DATA_DIR` to choose what the nodes write to.
fn disk_dir(label: &str) -> TempDir {
    let root = env_path("CLUSTER_DATA_DIR", "target/compare-cluster");
    let dir = root.join(format!("{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("cluster dir");
    TempDir(std::fs::canonicalize(&dir).expect("cluster dir"))
}

/// A node's log, appended to across restarts.
fn node_log(label: &str, node: usize) -> Stdio {
    let _ = std::fs::create_dir_all("target");
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(format!("target/compare-cluster-{label}-n{node}.log"))
        .map_or_else(|_| Stdio::null(), Stdio::from)
}

/// A TCP relay to the bucket's endpoint that can cut its node off: open
/// connections are dropped with what they had not yet forwarded, and new
/// ones are closed at once, until [`Relay::restore`].
struct Relay {
    addr: SocketAddr,
    cut: Arc<AtomicBool>,
    /// Raised by each cut: a connection opened before it is dead.
    generation: Arc<AtomicU64>,
}

impl Relay {
    fn to(upstream: SocketAddr) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("relay");
        let addr = listener.local_addr().expect("relay");
        let cut = Arc::new(AtomicBool::new(false));
        let generation = Arc::new(AtomicU64::new(0));
        let (is_cut, current) = (cut.clone(), generation.clone());
        std::thread::spawn(move || {
            for client in listener.incoming().flatten() {
                if is_cut.load(Ordering::SeqCst) {
                    continue;
                }
                let Ok(server) = TcpStream::connect(upstream) else {
                    continue;
                };
                let born = current.load(Ordering::SeqCst);
                for (from, to) in [(&client, &server), (&server, &client)] {
                    let (Ok(mut from), Ok(mut to)) = (from.try_clone(), to.try_clone()) else {
                        continue;
                    };
                    let current = current.clone();
                    let _ = from.set_read_timeout(Some(Duration::from_millis(20)));
                    std::thread::spawn(move || {
                        let mut buf = [0; 16384];
                        loop {
                            let read = from.read(&mut buf);
                            if current.load(Ordering::SeqCst) != born {
                                break;
                            }
                            match read {
                                Ok(0) => break,
                                Ok(n) if to.write_all(&buf[..n]).is_ok() => {}
                                Err(e)
                                    if matches!(
                                        e.kind(),
                                        std::io::ErrorKind::WouldBlock
                                            | std::io::ErrorKind::TimedOut
                                    ) => {}
                                _ => break,
                            }
                        }
                        let _ = from.shutdown(std::net::Shutdown::Both);
                        let _ = to.shutdown(std::net::Shutdown::Both);
                    });
                }
            }
        });
        Self {
            addr,
            cut,
            generation,
        }
    }

    fn cut(&self) {
        self.cut.store(true, Ordering::SeqCst);
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    fn restore(&self) {
        self.cut.store(false, Ordering::SeqCst);
    }
}

/// The address behind an `http://host:port` endpoint.
fn http_endpoint(endpoint: &str) -> Option<SocketAddr> {
    use std::net::ToSocketAddrs;
    let authority = endpoint.strip_prefix("http://")?.split('/').next()?;
    let authority = if authority.contains(':') {
        authority.to_owned()
    } else {
        format!("{authority}:80")
    };
    authority.to_socket_addrs().ok()?.next()
}

fn wasmkeep_cluster(label: &'static str, wasm: &Path, bucket: &str, relayed: bool) -> Cluster {
    let relays: Vec<Relay> = if relayed {
        let endpoint = std::env::var("AWS_ENDPOINT").unwrap_or_default();
        let upstream = http_endpoint(&endpoint)
            .unwrap_or_else(|| panic!("--control needs AWS_ENDPOINT=http://HOST:PORT"));
        (0..NODES).map(|_| Relay::to(upstream)).collect()
    } else {
        Vec::new()
    };
    let dir = disk_dir(label);
    let bin = std::fs::canonicalize(env_path("WASMKEEP_BIN", "target/release/wasmkeep"))
        .expect("WASMKEEP_BIN");
    let wasm = std::fs::canonicalize(wasm).expect("component");
    // The ledger built as the counter was: with `durable` or without.
    let ledger = if label == "rust-durable" {
        "bench/ledger/target/durable/wasm32-wasip2/release/ledger.wasm"
    } else {
        "bench/ledger/target/wasm32-wasip2/release/ledger.wasm"
    };
    let ledger = std::fs::canonicalize(ledger)
        .unwrap_or_else(|e| panic!("{ledger}: {e} (bench/cluster.sh builds it)"));
    let bucket = format!("{bucket}/{label}");
    let nodes = (0..NODES)
        .map(|i| {
            let addr = free_addr();
            let (bin, wasm, bucket) = (bin.clone(), wasm.clone(), bucket.clone());
            let ledger = ledger.clone();
            let data = dir.0.join(format!("n{i}"));
            let node_dir = data.clone();
            let relay = relays.get(i).map(|r| format!("http://{}", r.addr));
            let start = move || {
                let mut command = Command::new(&bin);
                if let Some(relay) = &relay {
                    command
                        .env("AWS_ENDPOINT", relay)
                        .env("AWS_ENDPOINT_URL", relay);
                }
                command
                    .args(["serve", "--actor"])
                    .arg(format!("counter={}", wasm.display()))
                    .arg("--actor")
                    .arg(format!("ledger={}", ledger.display()))
                    .args(["--idle-timeout-secs", "3600"])
                    .arg("--data-dir")
                    .arg(data.join("data"))
                    .arg("--cache-dir")
                    .arg(data.join("cache"))
                    .args(["--listen", &addr.to_string(), "--max-instances", "2000"])
                    .args(["--max-connections", &CLUSTER_MAX_CONNECTIONS.to_string()])
                    .arg("--bucket")
                    .arg(&bucket)
                    .args([
                        "--node-id",
                        &format!("n{i}"),
                        "--cluster-plaintext",
                        "accept",
                    ])
                    .arg("--advertise")
                    .arg(format!("http://{addr}"))
                    .env("RUST_LOG", "warn")
                    .stdout(node_log(label, i))
                    .stderr(Stdio::null())
                    .spawn()
                    .unwrap_or_else(|e| panic!("{}: {e}", bin.display()))
            };
            Node {
                addr,
                dir: node_dir,
                start: Box::new(start),
                child: None,
            }
        })
        .collect();
    Cluster {
        nodes,
        path: |name| format!("/actor/counter/{name}"),
        ledger: |name| format!("/actor/ledger/{name}"),
        relays,
        _dir: dir,
    }
}

fn celld_cluster(label: &'static str, durability: &'static str, bucket: &str) -> Cluster {
    let dir = disk_dir(label);
    let (bin, with_bucket) = celld_deploy(&format!("{bucket}/{label}"), &dir.0, true);
    let nodes = (0..NODES)
        .map(|i| {
            let (addr, internal) = (free_addr(), free_addr());
            let (bin, with_bucket) = (bin.clone(), with_bucket.clone());
            let state = dir.0.join(format!("n{i}"));
            let node_dir = state.clone();
            let start = move || {
                let mut command = Command::new(&bin);
                with_bucket(&mut command);
                no_huge_pages(&mut command);
                command
                    .args(["--listen", &addr.to_string()])
                    .args(["--internal-listen", &internal.to_string()])
                    .args(["--advertise", &internal.to_string()])
                    .env("CELLD_WATCH", &state)
                    .env("CELLD_DURABILITY", durability)
                    .env("RUST_LOG", "warn")
                    .stdout(Stdio::null())
                    .stderr(node_log(label, i))
                    .spawn()
                    .unwrap_or_else(|e| panic!("{}: {e}", bin.display()))
            };
            Node {
                addr,
                dir: node_dir,
                start: Box::new(start),
                child: None,
            }
        })
        .collect();
    Cluster {
        nodes,
        path: |name| format!("/counter/{name}"),
        ledger: |name| format!("/ledger/{name}"),
        relays: Vec::new(),
        _dir: dir,
    }
}

/// Prints the rows of one target under one prefix.
#[derive(Clone, Copy)]
struct Rows<'a> {
    prefix: &'a str,
    target: &'a str,
}

impl Rows<'_> {
    fn row(self, bench: &str, value: u128, unit: &str) {
        let Self { prefix, target } = self;
        println!("{prefix}/{target}/{bench},{value},{unit}");
    }
}

fn wait_ready(addr: SocketAddr, path: fn(&str) -> String, started: Instant) -> Duration {
    while started.elapsed() < Duration::from_secs(300) {
        if let Ok(mut c) = Client::connect(addr)
            && matches!(c.try_send("GET", &path("ready")), Ok((200, _)))
        {
            return started.elapsed();
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("{addr} did not become ready");
}

/// xorshift64: enough to spread requests over the nodes.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % n as u64) as usize
    }
}

/// One keep-alive connection per node, opened on first use and dropped
/// after a failure.
struct Clients {
    addrs: Vec<SocketAddr>,
    open: Vec<Option<Client>>,
    rng: Rng,
}

impl Clients {
    fn new(addrs: Vec<SocketAddr>, seed: u64) -> Self {
        let open = addrs.iter().map(|_| None).collect();
        Self {
            addrs,
            open,
            rng: Rng::new(seed),
        }
    }

    /// Sends a request to a node picked at random.
    fn send(&mut self, method: &str, path: &str) -> std::io::Result<(u16, String)> {
        let n = self.rng.below(self.addrs.len());
        if self.open[n].is_none() {
            let client = Client::connect(self.addrs[n])?;
            client.stream.set_read_timeout(Some(REQUEST_TIMEOUT))?;
            self.open[n] = Some(client);
        }
        let result = self.open[n].as_mut().map(|c| c.try_send(method, path));
        match result {
            Some(Ok(response)) => Ok(response),
            Some(Err(e)) => {
                self.open[n] = None;
                Err(e)
            }
            None => unreachable!("opened above"),
        }
    }

    /// A `GET`'s body, retried through other nodes for up to
    /// [`TAKEOVER_LIMIT`] (a node may still be settling after a restart).
    /// `None` if no node answered 200 in that time.
    fn body_retrying(&mut self, path: &str) -> Option<String> {
        let limit = Instant::now() + TAKEOVER_LIMIT;
        loop {
            match self.send("GET", path) {
                Ok((200, body)) => return Some(body),
                _ if Instant::now() > limit => return None,
                _ => std::thread::sleep(RETRY_EVERY),
            }
        }
    }

    /// [`Clients::send`] that must succeed with a 200 and a value.
    fn value(&mut self, method: &str, path: &str) -> i64 {
        match self.send(method, path) {
            Ok((200, body)) => body.trim().parse().unwrap_or_else(|_| {
                panic!("{method} {path}: not a value: {body:?}");
            }),
            other => panic!("{method} {path}: {other:?}"),
        }
    }
}

/// Values a client was handed for one actor that it alone writes, in order:
/// how many were not above every value before them (each one an
/// acknowledged write handed out again, so lost).
fn reissued(values: &[i64]) -> u64 {
    let mut highest = i64::MIN;
    let mut count = 0;
    for &v in values {
        if v <= highest {
            count += 1;
        }
        highest = highest.max(v);
    }
    count
}

/// What a ledger holds against what its one writer was told.
#[derive(Debug, Default, PartialEq, Eq)]
struct Audit {
    /// Acknowledged tokens the ledger does not hold: lost writes.
    lost: Vec<u64>,
    /// Tokens held more than once, counted once per extra copy.
    duplicated: u64,
    /// Tokens held that were never acknowledged (the request failed or timed
    /// out after the write applied): allowed.
    unacknowledged: u64,
}

fn audit(acked: &[u64], present: &[u64]) -> Audit {
    let held: HashSet<u64> = present.iter().copied().collect();
    let told: HashSet<u64> = acked.iter().copied().collect();
    let mut lost: Vec<u64> = told.difference(&held).copied().collect();
    lost.sort_unstable();
    Audit {
        lost,
        duplicated: (present.len() - held.len()) as u64,
        unacknowledged: held.difference(&told).count() as u64,
    }
}

/// From the kill, the longest wait between two acknowledgements in a row
/// (the kill counting as one), and when it ended. `None` if nothing was
/// acknowledged after the kill.
fn longest_wait(acks: &[Duration], killed_at: Duration) -> Option<(Duration, Duration)> {
    let mut previous = killed_at;
    let mut longest = None;
    for &at in acks.iter().filter(|&&at| at > killed_at) {
        let wait = at - previous;
        if longest.is_none_or(|(w, _)| wait > w) {
            longest = Some((wait, at));
        }
        previous = at;
    }
    longest
}

fn percentile_of(mut samples: Vec<Duration>, p: f64) -> Duration {
    samples.sort();
    percentile(&samples, p)
}

/// Runs one target; returns the acknowledged writes found lost.
pub(crate) fn run(
    target: &str,
    workload: Workload,
    kills: usize,
    victims: usize,
    fault: Fault,
) -> u64 {
    let bucket = BUCKET
        .get()
        .expect("--cluster needs WASMKEEP_COMPARE_BUCKET");
    let mut cluster = match target {
        "wasmkeep-rust-durable" | "wasmkeep-rust" => {
            let (label, wasm) = component(target).expect("component");
            wasmkeep_cluster(label, Path::new(wasm), bucket, fault == Fault::Control)
        }
        "celld-fleet" => celld_cluster("celld-fleet", "fleet", bucket),
        "celld-bucket" => celld_cluster("celld-bucket", "bucket", bucket),
        other => panic!("unknown cluster target {other}"),
    };
    let path = cluster.path;
    let rows = Rows {
        prefix: fault.prefix(),
        target,
    };

    // Start every node at once; the cluster is up when the last answers.
    let started = Instant::now();
    for node in &mut cluster.nodes {
        node.child = Some((node.start)());
    }
    let ready = cluster
        .addrs()
        .into_iter()
        .map(|addr| wait_ready(addr, path, started))
        .max()
        .unwrap_or_default();

    let Workload {
        iters,
        actors,
        secs,
    } = workload;
    let mut clients = Clients::new(cluster.addrs(), 1);
    let mut handed_twice = 0;
    if fault == Fault::Kill {
        rows.row("startup_to_first_response", ready.as_nanos(), "ns");
        handed_twice = steady_state(&cluster, &mut clients, rows, iters, actors, secs);
    } else {
        // The nodes hold as many resident actors as in the main run.
        for i in 0..actors {
            clients.value("POST", &path(&format!("new-{i}")));
        }
    }

    // Takeovers: each node in turn killed under writes, then restarted.
    let mut waits = Vec::new();
    let (mut failures, mut not_writable) = (0, 0);
    let mut restarts = Vec::new();
    let mut written: Vec<Written> = Vec::new();
    let mut tally = Tally::default();
    for k in 0..kills {
        let names: Vec<String> = (0..victims).map(|i| format!("v{k}-{i}")).collect();
        let t = takeover(&mut cluster, k % NODES, &names, fault);
        waits.extend(t.waits);
        failures += t.failures;
        not_writable += t.not_writable;
        restarts.push(t.restart);
        let from = written.len();
        written.extend(t.written);
        tally.sweep(&cluster, &written[from..], from);
        // Let the restarted node settle before the next kill.
        std::thread::sleep(Duration::from_secs(5));
    }
    // Once more, every victim of every kill, after the last restart.
    tally.sweep(&cluster, &written, 0);

    if kills > 0 {
        rows.row("takeover_victims", written.len() as u128, "actors");
        rows.row("takeover_affected", waits.len() as u128, "actors");
        if !waits.is_empty() {
            let max = waits.iter().max().copied().unwrap_or_default();
            rows.row(
                "takeover_writable_p50",
                percentile_of(waits, 0.5).as_nanos(),
                "ns",
            );
            rows.row("takeover_writable_max", max.as_nanos(), "ns");
        }
        rows.row("takeover_not_writable", not_writable as u128, "actors");
        rows.row("takeover_failed_requests", u128::from(failures), "requests");
        let restart = restarts.into_iter().max().unwrap_or_default();
        rows.row("restart_to_ready_max", restart.as_nanos(), "ns");
    }
    let acked: usize = written.iter().map(|w| w.acked.len()).sum();
    rows.row("acked_writes_checked", acked as u128, "writes");
    rows.row("acked_writes_lost", tally.lost.len() as u128, "writes");
    rows.row(
        "writes_applied_twice",
        u128::from(tally.duplicated),
        "writes",
    );
    rows.row(
        "unacked_writes_applied",
        u128::from(tally.unacknowledged),
        "writes",
    );
    rows.row(
        "ledgers_unreadable",
        tally.unreadable.len() as u128,
        "actors",
    );
    if fault == Fault::Kill {
        rows.row(
            "values_handed_out_twice",
            u128::from(handed_twice),
            "values",
        );
    }
    tally.lost.len() as u64
}

/// The one-node run's rows on three nodes, through a random node: latency,
/// first requests, memory, throughput. Returns the counter values handed out
/// twice in the throughput phase.
fn steady_state(
    cluster: &Cluster,
    clients: &mut Clients,
    rows: Rows<'_>,
    iters: usize,
    actors: usize,
    secs: u64,
) -> u64 {
    let path = cluster.path;
    // Latency to one resident actor through a random node.
    for (name, method) in [("warm_post", "POST"), ("warm_get", "GET")] {
        for _ in 0..100 {
            clients.value(method, &path("warm"));
        }
        let samples = (0..iters)
            .map(|_| {
                let t = Instant::now();
                clients.value(method, &path("warm"));
                t.elapsed()
            })
            .collect::<Vec<_>>();
        for (suffix, p) in [("p50", 0.5), ("p99", 0.99)] {
            let value = percentile_of(samples.clone(), p).as_nanos();
            rows.row(&format!("{name}_{suffix}"), value, "ns");
        }
    }

    // First request to new actors; memory across the three nodes.
    let (rss_before, _) = cluster.stats();
    let samples = (0..actors)
        .map(|i| {
            let t = Instant::now();
            clients.value("POST", &path(&format!("new-{i}")));
            t.elapsed()
        })
        .collect::<Vec<_>>();
    rows.row("create_p50", percentile_of(samples, 0.5).as_nanos(), "ns");
    std::thread::sleep(Duration::from_millis(200));
    let (rss_after, _) = cluster.stats();
    let per_actor = rss_after.saturating_sub(rss_before) / actors as u64;
    rows.row("memory_per_resident_actor", u128::from(per_actor), "bytes");
    rows.row("memory_rss_total", u128::from(rss_after), "bytes");

    // Throughput: 8 clients, writes spread over the resident actors, each
    // actor written by one client alone (actor i by client i % 8). Each
    // client sends its next write when the last is answered, so this is what
    // eight such clients complete, not the cluster's capacity.
    let (_, cpu_before) = cluster.stats();
    let started = Instant::now();
    let deadline = started + Duration::from_secs(secs);
    let handles: Vec<_> = (0..8u64)
        .map(|t| {
            let addrs = cluster.addrs();
            std::thread::spawn(move || {
                let mut clients = Clients::new(addrs, 100 + t);
                let mine: Vec<usize> = (t as usize..actors).step_by(8).collect();
                let mut seen: Vec<Vec<i64>> = vec![Vec::new(); mine.len()];
                let mut n = 0u64;
                while Instant::now() < deadline {
                    let j = n as usize % mine.len();
                    seen[j].push(clients.value("POST", &path(&format!("new-{}", mine[j]))));
                    n += 1;
                }
                (n, seen.iter().map(|s| reissued(s)).sum::<u64>())
            })
        })
        .collect();
    let (requests, handed_twice) = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .fold((0, 0), |(a, b), (n, r)| (a + n, b + r));
    let elapsed = started.elapsed();
    let (_, cpu_after) = cluster.stats();
    rows.row(
        "throughput_8_clients",
        (requests as f64 / elapsed.as_secs_f64()) as u128,
        "req/s",
    );
    rows.row(
        "server_cpu_per_request",
        (cpu_after - cpu_before).as_nanos() / u128::from(requests.max(1)),
        "ns",
    );
    handed_twice
}

/// One victim ledger and the tokens its writer was acknowledged.
struct Written {
    name: String,
    acked: Vec<u64>,
}

/// What the sweeps found, over every victim. A token lost in one sweep
/// stays lost, whatever a later sweep reads.
#[derive(Default)]
struct Tally {
    /// (victim, token) of each acknowledged write a sweep did not find.
    lost: HashSet<(usize, u64)>,
    /// The most copies beyond the first, and the most unacknowledged tokens,
    /// any one sweep found (the last sweep covers every victim).
    duplicated: u64,
    unacknowledged: u64,
    /// Victims a sweep could not read from any node.
    unreadable: HashSet<usize>,
}

impl Tally {
    /// Reads back `written` (victims `from..`) and audits each.
    fn sweep(&mut self, cluster: &Cluster, written: &[Written], from: usize) {
        let mut clients = Clients::new(cluster.addrs(), 7);
        let (mut duplicated, mut unacknowledged) = (0, 0);
        for (i, w) in written.iter().enumerate() {
            let Some(body) = clients.body_retrying(&(cluster.ledger)(&w.name)) else {
                self.unreadable.insert(from + i);
                continue;
            };
            let present: Vec<u64> = body.lines().filter_map(|l| l.trim().parse().ok()).collect();
            let found = audit(&w.acked, &present);
            self.lost
                .extend(found.lost.into_iter().map(|t| (from + i, t)));
            duplicated += found.duplicated;
            unacknowledged += found.unacknowledged;
        }
        self.duplicated = self.duplicated.max(duplicated);
        self.unacknowledged = self.unacknowledged.max(unacknowledged);
    }
}

struct Takeover {
    /// For each victim on the dead node: from the kill to the end of its
    /// longest wait for an acknowledgement.
    waits: Vec<Duration>,
    /// Victims with no acknowledged write after the kill.
    not_writable: usize,
    failures: u64,
    restart: Duration,
    written: Vec<Written>,
}

fn takeover(cluster: &mut Cluster, node: usize, names: &[String], fault: Fault) -> Takeover {
    let (path, ledger) = (cluster.path, cluster.ledger);
    let base = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    // Nanoseconds since `base` of each victim's last acknowledgement.
    let last_ack: Arc<Vec<AtomicU64>> = Arc::new(names.iter().map(|_| AtomicU64::new(0)).collect());
    let handles: Vec<_> = names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let (stop, last_ack) = (stop.clone(), last_ack.clone());
            let (addrs, name) = (cluster.addrs(), ledger(name));
            std::thread::spawn(move || {
                let mut clients = Clients::new(addrs, 1000 + i as u64);
                let mut acked = Vec::new();
                let mut failures = 0u64;
                // A token per attempt: one that failed is not sent again.
                let mut token = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let sent = clients.send("POST", &format!("{name}?t={token}"));
                    match sent {
                        Ok((200, _)) => {
                            let at = base.elapsed();
                            acked.push((at, token));
                            last_ack[i].store(at.as_nanos() as u64, Ordering::Relaxed);
                            std::thread::sleep(WRITE_EVERY);
                        }
                        _ => {
                            failures += 1;
                            std::thread::sleep(RETRY_EVERY);
                        }
                    }
                    token += 1;
                }
                (acked, failures)
            })
        })
        .collect();

    // Writes flow to every victim, then the node dies.
    std::thread::sleep(Duration::from_secs(3));
    if let Some(relay) = cluster.relays.get(node) {
        relay.cut();
        std::thread::sleep(Duration::from_millis(500));
    }
    let killed_at = cluster.nodes[node].kill() - base;
    if fault != Fault::Kill {
        let _ = std::fs::remove_dir_all(&cluster.nodes[node].dir);
    }
    if let Some(relay) = cluster.relays.get(node) {
        relay.restore();
    }
    // Until every victim is being acknowledged again, a little after the
    // kill at the earliest: writes after the takeover are checked too.
    let limit = Instant::now() + TAKEOVER_LIMIT;
    let settled = Duration::from_secs(1);
    loop {
        std::thread::sleep(Duration::from_millis(50));
        let now = base.elapsed();
        let writing = last_ack
            .iter()
            .all(|a| now.saturating_sub(Duration::from_nanos(a.load(Ordering::Relaxed))) < settled);
        if (writing && now > killed_at + Duration::from_secs(2)) || Instant::now() > limit {
            break;
        }
    }
    std::thread::sleep(Duration::from_secs(2));
    stop.store(true, Ordering::Relaxed);
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    let restarted = Instant::now();
    cluster.nodes[node].child = Some((cluster.nodes[node].start)());
    let restart = wait_ready(cluster.nodes[node].addr, path, restarted);

    let mut out = Takeover {
        waits: Vec::new(),
        not_writable: 0,
        failures: 0,
        restart,
        written: Vec::new(),
    };
    for (name, (acked, failures)) in names.iter().zip(results) {
        out.failures += failures;
        let times: Vec<Duration> = acked.iter().map(|(at, _)| *at).collect();
        match longest_wait(&times, killed_at) {
            Some((wait, end)) if wait > AFFECTED => out.waits.push(end - killed_at),
            Some(_) => {}
            None => out.not_writable += 1,
        }
        out.written.push(Written {
            name: name.clone(),
            acked: acked.into_iter().map(|(_, token)| token).collect(),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_value_at_or_below_an_acknowledged_one_is_reissued() {
        assert_eq!(reissued(&[]), 0);
        assert_eq!(reissued(&[1, 2, 5, 9]), 0);
        // 3 was handed out after 5: the writes that made 4 and 5 were lost.
        assert_eq!(reissued(&[1, 2, 5, 3, 6]), 1);
        assert_eq!(reissued(&[4, 4]), 1);
    }

    #[test]
    fn an_acknowledged_token_the_ledger_lacks_is_lost() {
        assert_eq!(audit(&[], &[]), Audit::default());
        assert_eq!(audit(&[0, 1, 2], &[0, 1, 2]), Audit::default());
        // 1 was acknowledged and is gone. 3 failed but applied: with a
        // counter it would have hidden the loss (three writes, value 3).
        let found = audit(&[0, 1, 2], &[0, 2, 3]);
        assert_eq!(found.lost, vec![1]);
        assert_eq!(found.unacknowledged, 1);
        assert_eq!(found.duplicated, 0);
    }

    #[test]
    fn a_token_held_twice_is_counted_once_per_extra_copy() {
        let found = audit(&[0, 1], &[0, 1, 1, 1]);
        assert_eq!(found.duplicated, 2);
        assert!(found.lost.is_empty());
    }

    #[test]
    fn the_takeover_is_the_longest_wait_not_the_first_acknowledgement() {
        let ms = Duration::from_millis;
        // An answer already on its way arrives 1 ms after the kill; the
        // actor is writable again 15 s later.
        let acks = [ms(900), ms(1001), ms(16_000), ms(16_020)];
        assert_eq!(
            longest_wait(&acks, ms(1000)),
            Some((ms(14_999), ms(16_000)))
        );
        // On a surviving node: no wait to speak of.
        assert_eq!(
            longest_wait(&[ms(990), ms(1010), ms(1030)], ms(1000)),
            Some((ms(20), ms(1030)))
        );
        assert_eq!(longest_wait(&[ms(900)], ms(1000)), None);
    }

    #[test]
    fn the_rng_spreads_over_every_node() {
        let mut rng = Rng::new(1);
        let mut hits = [0; NODES];
        for _ in 0..3000 {
            hits[rng.below(NODES)] += 1;
        }
        assert!(hits.iter().all(|&h| h > 800), "{hits:?}");
    }
}
