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
//! killed with SIGKILL while one client per victim actor writes to it every
//! 20 ms, retrying every 100 ms through a random node until a write is
//! acknowledged. A victim whose first acknowledgement after the kill takes
//! longer than [`AFFECTED`] was on the dead node: its wait is its
//! takeover. Each client writes to its actors alone, so the values it is
//! handed must only grow; a value at or below one already acknowledged
//! means an acknowledged write was lost, and so does a final value below the
//! last one acknowledged. The killed node is then restarted on its data.
//!
//! Rows are `cluster3/<target>/<bench>`.

use super::*;

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

struct Node {
    addr: SocketAddr,
    start: Box<dyn Fn() -> Child>,
    child: Option<Child>,
}

impl Node {
    fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }

    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

struct Cluster {
    nodes: Vec<Node>,
    path: fn(&str) -> String,
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

/// A scratch directory on the repository's disk (not tmpfs), removed on drop.
fn disk_dir(label: &str) -> TempDir {
    let dir = Path::new("target/compare-cluster").join(format!("{label}-{}", std::process::id()));
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

fn wasmkeep_cluster(label: &'static str, wasm: &Path, bucket: &str) -> Cluster {
    let dir = disk_dir(label);
    let bin = std::fs::canonicalize(env_path("WASMKEEP_BIN", "target/release/wasmkeep"))
        .expect("WASMKEEP_BIN");
    let wasm = std::fs::canonicalize(wasm).expect("component");
    let bucket = format!("{bucket}/{label}");
    let nodes = (0..NODES)
        .map(|i| {
            let addr = free_addr();
            let (bin, wasm, bucket) = (bin.clone(), wasm.clone(), bucket.clone());
            let data = dir.0.join(format!("n{i}"));
            let start = move || {
                Command::new(&bin)
                    .args(["serve", "--actor"])
                    .arg(format!("counter={}", wasm.display()))
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
                start: Box::new(start),
                child: None,
            }
        })
        .collect();
    Cluster {
        nodes,
        path: |name| format!("/actor/counter/{name}"),
        _dir: dir,
    }
}

fn celld_cluster(label: &'static str, durability: &'static str, bucket: &str) -> Cluster {
    let dir = disk_dir(label);
    let (bin, with_bucket) = celld_deploy(&format!("{bucket}/{label}"), &dir.0);
    let nodes = (0..NODES)
        .map(|i| {
            let (addr, internal) = (free_addr(), free_addr());
            let (bin, with_bucket) = (bin.clone(), with_bucket.clone());
            let state = dir.0.join(format!("n{i}"));
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
                start: Box::new(start),
                child: None,
            }
        })
        .collect();
    Cluster {
        nodes,
        path: |name| format!("/counter/{name}"),
        _dir: dir,
    }
}

fn cluster_row(target: &str, bench: &str, value: u128, unit: &str) {
    println!("cluster3/{target}/{bench},{value},{unit}");
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

    /// A `GET` of the value, retried through other nodes for up to
    /// [`TAKEOVER_LIMIT`] (a node may still be settling after a restart).
    fn value_retrying(&mut self, path: &str) -> i64 {
        let limit = Instant::now() + TAKEOVER_LIMIT;
        loop {
            match self.send("GET", path) {
                Ok((200, body)) => {
                    return body.trim().parse().unwrap_or_else(|_| {
                        panic!("GET {path}: not a value: {body:?}");
                    });
                }
                other if Instant::now() > limit => panic!("GET {path}: {other:?}"),
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

fn percentile_of(mut samples: Vec<Duration>, p: f64) -> Duration {
    samples.sort();
    percentile(&samples, p)
}

pub(crate) fn run(target: &str, workload: Workload, kills: usize, victims: usize) {
    let bucket = BUCKET
        .get()
        .expect("--cluster needs WASMKEEP_COMPARE_BUCKET");
    let mut cluster = match target {
        "wasmkeep-rust-durable" | "wasmkeep-rust" => {
            let (label, wasm) = component(target).expect("component");
            wasmkeep_cluster(label, Path::new(wasm), bucket)
        }
        "celld-fleet" => celld_cluster("celld-fleet", "fleet", bucket),
        "celld-bucket" => celld_cluster("celld-bucket", "bucket", bucket),
        other => panic!("unknown cluster target {other}"),
    };
    let path = cluster.path;

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
    cluster_row(target, "startup_to_first_response", ready.as_nanos(), "ns");

    let Workload {
        iters,
        actors,
        secs,
    } = workload;
    let mut clients = Clients::new(cluster.addrs(), 1);

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
            cluster_row(target, &format!("{name}_{suffix}"), value, "ns");
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
    cluster_row(
        target,
        "create_p50",
        percentile_of(samples, 0.5).as_nanos(),
        "ns",
    );
    std::thread::sleep(Duration::from_millis(200));
    let (rss_after, _) = cluster.stats();
    let per_actor = rss_after.saturating_sub(rss_before) / actors as u64;
    cluster_row(
        target,
        "memory_per_resident_actor",
        u128::from(per_actor),
        "bytes",
    );
    cluster_row(target, "memory_rss_total", u128::from(rss_after), "bytes");

    // Throughput: 8 clients, writes spread over the resident actors, each
    // actor written by one client alone (actor i by client i % 8).
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
    let (requests, mut handed_twice) = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .fold((0, 0), |(a, b), (n, r)| (a + n, b + r));
    let elapsed = started.elapsed();
    let (_, cpu_after) = cluster.stats();
    cluster_row(
        target,
        "throughput_8_clients",
        (requests as f64 / elapsed.as_secs_f64()) as u128,
        "req/s",
    );
    cluster_row(
        target,
        "server_cpu_per_request",
        (cpu_after - cpu_before).as_nanos() / u128::from(requests.max(1)),
        "ns",
    );

    // Takeovers: each node in turn killed under writes, then restarted.
    let mut waits = Vec::new();
    let (mut victims_total, mut lost, mut failures) = (0, 0, 0);
    let mut restarts = Vec::new();
    for k in 0..kills {
        let node = k % NODES;
        let names: Vec<String> = (0..victims)
            .map(|i| format!("new-{}", (k * victims + i) % actors))
            .collect();
        let t = takeover(&mut cluster, node, &names);
        victims_total += names.len();
        waits.extend(t.waits);
        lost += t.lost;
        handed_twice += t.reissued;
        failures += t.failures;
        restarts.push(t.restart);
    }
    if kills > 0 {
        cluster_row(target, "takeover_victims", victims_total as u128, "actors");
        cluster_row(target, "takeover_affected", waits.len() as u128, "actors");
        if !waits.is_empty() {
            let max = waits.iter().max().copied().unwrap_or_default();
            cluster_row(
                target,
                "takeover_writable_p50",
                percentile_of(waits, 0.5).as_nanos(),
                "ns",
            );
            cluster_row(target, "takeover_writable_max", max.as_nanos(), "ns");
        }
        cluster_row(
            target,
            "takeover_failed_requests",
            u128::from(failures),
            "requests",
        );
        let restart = restarts.into_iter().max().unwrap_or_default();
        cluster_row(target, "restart_to_ready_max", restart.as_nanos(), "ns");
    }
    cluster_row(target, "acked_writes_lost", u128::from(lost), "writes");
    cluster_row(
        target,
        "values_handed_out_twice",
        u128::from(handed_twice),
        "values",
    );
}

struct Takeover {
    /// For each victim on the dead node: from the kill to its first
    /// acknowledged write.
    waits: Vec<Duration>,
    /// Victims whose final value is below one acknowledged before.
    lost: u64,
    /// Values handed out again (see [`reissued`]).
    reissued: u64,
    failures: u64,
    restart: Duration,
}

fn takeover(cluster: &mut Cluster, node: usize, names: &[String]) -> Takeover {
    let path = cluster.path;
    let base = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    // Nanoseconds since `base` of each victim's last acknowledgement.
    let last_ack: Arc<Vec<AtomicU64>> = Arc::new(names.iter().map(|_| AtomicU64::new(0)).collect());
    let handles: Vec<_> = names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let (stop, last_ack) = (stop.clone(), last_ack.clone());
            let (addrs, name) = (cluster.addrs(), name.clone());
            std::thread::spawn(move || {
                let mut clients = Clients::new(addrs, 1000 + i as u64);
                let mut acked = Vec::new();
                let mut failures = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    match clients.send("POST", &path(&name)) {
                        Ok((200, body)) => {
                            let at = base.elapsed();
                            let value: i64 = body.trim().parse().expect("a value");
                            acked.push((at, value));
                            last_ack[i].store(at.as_nanos() as u64, Ordering::Relaxed);
                            std::thread::sleep(WRITE_EVERY);
                        }
                        _ => {
                            failures += 1;
                            std::thread::sleep(RETRY_EVERY);
                        }
                    }
                }
                (acked, failures)
            })
        })
        .collect();

    // Writes flow to every victim, then the node dies.
    std::thread::sleep(Duration::from_secs(3));
    let killed_at = base.elapsed();
    cluster.nodes[node].kill();
    let limit = Instant::now() + TAKEOVER_LIMIT;
    while Instant::now() < limit
        && last_ack
            .iter()
            .any(|a| Duration::from_nanos(a.load(Ordering::Relaxed)) <= killed_at)
    {
        std::thread::sleep(Duration::from_millis(50));
    }
    // A little longer: writes after the takeover are checked too.
    std::thread::sleep(Duration::from_secs(2));
    stop.store(true, Ordering::Relaxed);
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    let restarted = Instant::now();
    cluster.nodes[node].child = Some((cluster.nodes[node].start)());
    let restart = wait_ready(cluster.nodes[node].addr, path, restarted);

    let mut out = Takeover {
        waits: Vec::new(),
        lost: 0,
        reissued: 0,
        failures: 0,
        restart,
    };
    let mut check = Clients::new(cluster.addrs(), 7);
    for (name, (acked, failures)) in names.iter().zip(results) {
        out.failures += failures;
        match acked.iter().find(|(at, _)| *at > killed_at) {
            Some((at, _)) if *at - killed_at > AFFECTED => out.waits.push(*at - killed_at),
            Some(_) => {}
            None => panic!("{name} was not writable again within {TAKEOVER_LIMIT:?}"),
        }
        let values: Vec<i64> = acked.iter().map(|(_, v)| *v).collect();
        out.reissued += reissued(&values);
        let last = values.iter().copied().max().unwrap_or(0);
        if check.value_retrying(&path(name)) < last {
            out.lost += 1;
        }
    }
    // Let the restarted node settle before the next kill.
    std::thread::sleep(Duration::from_secs(5));
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
    fn the_rng_spreads_over_every_node() {
        let mut rng = Rng::new(1);
        let mut hits = [0; NODES];
        for _ in 0..3000 {
            hits[rng.below(NODES)] += 1;
        }
        assert!(hits.iter().all(|&h| h > 800), "{hits:?}");
    }
}
