//! The three-node soak against a real bucket (run by
//! `bench/soak.sh`): three `wasmkeep serve` processes with the production
//! lease timings, durable writes from several clients through every node,
//! and one node killed with `kill -9` every few minutes, then restarted.
//!
//! It measures what the in-memory benchmarks cannot: durable-write latency
//! against the bucket, how long an actor whose owner died is unavailable
//! under load, and how late an alarm fires after its node dies. It checks
//! that no acknowledged durable write is lost, that no value is handed out
//! twice (two live owners would), and that every alarm fires.
//!
//! What each metric covers:
//! - `durable_write_*`, `writes_acked`, `write_errors`: the eight clients'
//!   writes (random actor, random live node), as they saw them.
//! - `failover_*`: for each killed node's actors without an alarm, a client
//!   of its own retries every 100 ms from the kill until a write is
//!   acknowledged (`probe_attempts` counts its tries). The alarms' actors
//!   get none: their recovery is the alarm's.
//! - `alarm_late_*`: alarms set on the killed node's actors, due when it
//!   is killed, from then until a live node sees them fired; the node
//!   stays down 75 s, so the others deliver them.
//! - `plain_*`: two more clients writing with a plain `exec` (answered
//!   before the upload) to actors of their own. Reported, not judged: such
//!   a write may be lost with its node; `plain_lost` counts acknowledged
//!   increments lost: values handed out again (`plain_duplicates`, later
//!   writes re-covering a lost one) plus what the actors no longer hold at
//!   the end.
//! - `takeover_reads_*`: during the 30 s after each kill, readers read the
//!   killed node's actors (their durable value) through the live nodes
//!   every 100 ms: answers, failures, latency, and stale reads (a value
//!   below one acknowledged before the kill: a lost durable write, which
//!   fails the run).
//!
//! Node data and logs go to `WASMKEEP_SOAK_DIR`, or `target/soak/<start>`;
//! they are kept after the run, with the forensics: `samples.csv` (one row
//! per write, probe, alarm fired, kill and restart, with wall-clock times,
//! read by `bench/soak_samples.py`), and each live node's metrics page at
//! every kill (`metrics-kill<n>-<id>.txt`) and at the end.
//!
//! `WASMKEEP_SOAK_ACTORS` (60 by default) sets how many actors the clients
//! write to. The run fails if the 99th percentile of "writable again after
//! the kill" passes `WASMKEEP_SOAK_FAILOVER_P99_SECS` (30 by default); the
//! worst is reported, not judged (one slow restore should not fail a long
//! run whose invariants held).
//!
//! Runs only with `WASMKEEP_SOAK_SECS` set (and `WASMKEEP_TEST_S3_BUCKET` and
//! `AWS_*` naming the bucket); skipped otherwise. `WASMKEEP_SOAK_KILL_EVERY`
//! (seconds, 300 by default here, 180 in `bench/soak.sh`) spaces the kills. Prints one
//! `soak_metric NAME VALUE UNIT` line per result.
//!
//! By default the three nodes run here, as child processes. With
//! `WASMKEEP_SOAK_NODES=a=ADDR,b=ADDR,c=ADDR` they are already running
//! elsewhere (a service per machine, with the test actor deployed as class
//! `t`), and two command templates kill and start one, `{id}` replaced by
//! its node id: `WASMKEEP_SOAK_KILL` (`kill -9`, for example
//! `ssh {id} sudo systemctl kill -s KILL wasmkeep`) and `WASMKEEP_SOAK_START`.
//! `WASMKEEP_SOAK_METRICS` lists their metrics addresses the same way.
//! Actor names carry the run's start time, so runs never share an actor.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const NODES: [&str; 3] = ["a", "b", "c"];
/// Actors the clients write to, unless `WASMKEEP_SOAK_ACTORS` says.
const ACTORS: usize = 60;

fn actors() -> usize {
    std::env::var("WASMKEEP_SOAK_ACTORS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(ACTORS)
}

/// The pass bound on "writable again after the kill", at the 99th
/// percentile: `WASMKEEP_SOAK_FAILOVER_P99_SECS`, 30 s by default.
fn failover_bound() -> Duration {
    std::env::var("WASMKEEP_SOAK_FAILOVER_P99_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .map_or(Duration::from_secs(30), Duration::from_secs)
}

/// Whether the takeovers passed: their 99th percentile (sorted ascending)
/// under `bound`. The worst is reported, not judged.
fn failover_verdict(sorted: &[Duration], bound: Duration) -> Result<(), String> {
    let p99 = percentile(sorted, 0.99);
    if p99 < bound {
        Ok(())
    } else {
        Err(format!(
            "writable again after the kill: p99 {p99:?}, over the {bound:?} bound (worst {:?})",
            sorted.last().copied().unwrap_or_default()
        ))
    }
}
const WRITERS: usize = 8;
/// Alarms set on the victim's actors before each kill.
const ALARMS_PER_KILL: usize = 8;
/// Clients writing with a plain `exec`, and the actors they write to (apart
/// from the durable writers').
const PLAIN_WRITERS: usize = 2;
const PLAIN_ACTORS: usize = 20;
/// Readers of the killed node's actors, and for how long after a kill.
const READERS: usize = 4;
const READ_FOR: Duration = Duration::from_secs(30);
/// How long a killed node stays down: longer than its alarms take to be
/// delivered by the others, which is what is measured.
const DOWN: Duration = Duration::from_secs(75);

fn free_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

/// tests/guests/test-actor of wasmkeep-server: `POST /durable`, alarms,
/// `GET /fired`.
fn test_actor_wasm() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../wasmkeep-server/tests/guests/test-actor");
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let status = Command::new(cargo)
        .current_dir(&root)
        .args([
            "build",
            "--release",
            "--locked",
            "--target",
            "wasm32-wasip2",
        ])
        .status()
        .unwrap();
    assert!(status.success(), "building the test actor failed");
    root.join("target/wasm32-wasip2/release/test_actor.wasm")
}

/// Where the nodes run: started here, or already running elsewhere (a
/// service on each machine), killed and started by commands.
enum Nodes {
    Local {
        dir: PathBuf,
        wasm: PathBuf,
        bucket: String,
        metrics: Vec<SocketAddr>,
        children: Vec<Option<Child>>,
    },
    Remote {
        metrics: Vec<Option<SocketAddr>>,
        kill: String,
        start: String,
    },
}

impl Nodes {
    fn start(&mut self, i: usize, addr: SocketAddr) {
        match self {
            Nodes::Local {
                dir,
                wasm,
                bucket,
                metrics,
                children,
            } => children[i] = Some(start(NODES[i], addr, metrics[i], dir, wasm, bucket)),
            Nodes::Remote { start, .. } => run(start, NODES[i]),
        }
    }

    /// `kill -9`: no release, no clean shutdown.
    fn kill(&mut self, i: usize) {
        match self {
            Nodes::Local { children, .. } => {
                if let Some(mut child) = children[i].take() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }
            Nodes::Remote { kill, .. } => run(kill, NODES[i]),
        }
    }

    fn metrics(&self, i: usize) -> Option<SocketAddr> {
        match self {
            Nodes::Local { metrics, .. } => Some(metrics[i]),
            Nodes::Remote { metrics, .. } => metrics[i],
        }
    }
}

impl Drop for Nodes {
    fn drop(&mut self) {
        if let Nodes::Local { children, .. } = self {
            for child in children.iter_mut().flatten() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

/// Runs a command template through `sh -c`, `{id}` replaced by the node id.
fn run(template: &str, id: &str) {
    let command = template.replace("{id}", id);
    let status = Command::new("sh").args(["-c", &command]).status().unwrap();
    assert!(status.success(), "{command}: {status}");
}

/// `a=ADDR,b=ADDR,c=ADDR`, in `NODES` order.
fn node_list(var: &str) -> Option<Vec<SocketAddr>> {
    let value = std::env::var(var).ok()?;
    let map: HashMap<&str, SocketAddr> = value
        .split(',')
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or_else(|| panic!("{var}: {kv}"));
            (
                k.trim(),
                v.trim()
                    .parse()
                    .unwrap_or_else(|e| panic!("{var}: {v}: {e}")),
            )
        })
        .collect();
    Some(NODES.iter().map(|id| map[id]).collect())
}

/// This run's actors: `s<run>-<n>`, so runs against long-lived nodes
/// never share an actor.
fn actor_path(actor: usize, rest: &str) -> String {
    static RUN: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let run = RUN.get_or_init(now_ms);
    format!("/actor/t/s{run}-{actor}{rest}")
}

fn start(
    id: &str,
    addr: SocketAddr,
    metrics: SocketAddr,
    dir: &Path,
    wasm: &Path,
    bucket: &str,
) -> Child {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join(format!("{id}.log")))
        .unwrap();
    Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .args(["serve", "--actor"])
        .arg(format!("t={}", wasm.display()))
        .arg("--data-dir")
        .arg(dir.join(id))
        .args(["--listen", &addr.to_string(), "--node-id", id])
        .args(["--advertise", &format!("http://{addr}"), "--bucket", bucket])
        // Plain HTTP between nodes, on loopback: the bucket is what is
        // measured here.
        .args(["--cluster-plaintext", "accept"])
        .args(["--metrics-listen", &metrics.to_string()])
        .env("RUST_LOG", "wasmkeep_server=info")
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap()
}

/// (status, x-wasmkeep-node, body); `None` if the node does not answer.
fn send(addr: SocketAddr, method: &str, path: &str) -> Option<(u16, Option<String>, String)> {
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(30))).ok()?;
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    s.write_all(req.as_bytes()).ok()?;
    let mut raw = String::new();
    s.read_to_string(&mut raw).ok()?;
    let (head, body) = raw.split_once("\r\n\r\n")?;
    let status = head.split_whitespace().nth(1)?.parse().ok()?;
    let node = head.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.eq_ignore_ascii_case("x-wasmkeep-node")
            .then(|| v.trim().to_owned())
    });
    let body = match body.split_once("\r\n") {
        Some((size, rest)) if usize::from_str_radix(size.trim(), 16).is_ok() => {
            rest.split("\r\n").next().unwrap_or("").to_owned()
        }
        _ => body.to_owned(),
    };
    Some((status, node, body.trim().to_owned()))
}

/// A node's metrics page.
fn metrics_page(addr: SocketAddr) -> Option<String> {
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(10))).ok()?;
    s.write_all(b"GET /metrics HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
        .ok()?;
    let mut raw = String::new();
    s.read_to_string(&mut raw).ok()?;
    Some(raw.split_once("\r\n\r\n")?.1.to_owned())
}

/// A small xorshift: which actor and which node, spread evenly.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        usize::try_from(self.0 % n as u64).unwrap()
    }
}

/// One durable write's outcome, as a client saw it.
struct Write1 {
    actor: usize,
    /// When it was sent (wall clock, ms) and through which node.
    at_ms: u64,
    via: usize,
    took: Duration,
    /// The value it was given and the node that ran it, if acknowledged.
    acked: Option<(u64, String)>,
    /// A recovery probe's attempt, retried every 100 ms against an actor
    /// whose node died: counted apart from the clients' writes.
    probe: bool,
}

#[derive(Default)]
struct Shared {
    writes: Vec<Write1>,
    /// Per actor: the node that ran its last acknowledged write.
    last_node: HashMap<usize, String>,
    /// Plain writes: (actor, value acknowledged, if any).
    plain: Vec<(usize, Option<u64>)>,
}

/// What readers saw during the takeovers.
#[derive(Default)]
struct Reads {
    ok: Vec<Duration>,
    failed: u64,
    /// (actor, value read, value acknowledged before the kill).
    stale: Vec<(usize, u64, u64)>,
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[rank]
}

fn metric(name: &str, value: u128, unit: &str) {
    eprintln!("soak_metric {name} {value} {unit}");
}

fn ns(d: Duration) -> u128 {
    d.as_nanos()
}

/// Records a write and answers what it got.
fn durable_write(
    shared: &Mutex<Shared>,
    addrs: &[SocketAddr],
    via: usize,
    actor: usize,
    probe: bool,
) -> Option<(u64, String)> {
    let at_ms = now_ms();
    let started = Instant::now();
    let answer = send(addrs[via], "POST", &actor_path(actor, "/durable"));
    let took = started.elapsed();
    let acked = match answer {
        Some((200, Some(node), body)) => body.parse().ok().map(|v: u64| (v, node)),
        _ => None,
    };
    let mut s = shared.lock().unwrap();
    if let Some((_, node)) = &acked {
        s.last_node.insert(actor, node.clone());
    }
    s.writes.push(Write1 {
        actor,
        at_ms,
        via,
        took,
        acked: acked.clone(),
        probe,
    });
    acked
}

#[test]
fn three_nodes_against_a_real_bucket() {
    let Ok(secs) = std::env::var("WASMKEEP_SOAK_SECS") else {
        eprintln!("WASMKEEP_SOAK_SECS not set; skipping");
        return;
    };
    let total = Duration::from_secs(secs.parse().unwrap());
    let kill_every = Duration::from_secs(
        std::env::var("WASMKEEP_SOAK_KILL_EVERY")
            .ok()
            .map_or(300, |s| s.parse().unwrap()),
    );
    // Kept: each local node's log and, at the end, every metrics page.
    // On disk, not in /tmp (RAM on many workstations; a 30-minute run's
    // nodes write gigabytes): `WASMKEEP_SOAK_DIR`, or target/soak/<start>.
    let dir = std::env::var_os("WASMKEEP_SOAK_DIR").map_or_else(
        || {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/soak")
                .join(now_ms().to_string())
        },
        PathBuf::from,
    );
    std::fs::create_dir_all(&dir).unwrap();
    let (addrs, mut nodes) = if let Some(addrs) = node_list("WASMKEEP_SOAK_NODES") {
        // Nodes already running elsewhere, one per machine.
        let var = |name: &str| {
            std::env::var(name).unwrap_or_else(|_| panic!("WASMKEEP_SOAK_NODES needs {name}"))
        };
        let metrics = node_list("WASMKEEP_SOAK_METRICS").map_or_else(
            || vec![None; NODES.len()],
            |m| m.into_iter().map(Some).collect(),
        );
        let nodes = Nodes::Remote {
            metrics,
            kill: var("WASMKEEP_SOAK_KILL"),
            start: var("WASMKEEP_SOAK_START"),
        };
        (addrs, nodes)
    } else {
        let Ok(bucket) = std::env::var("WASMKEEP_TEST_S3_BUCKET") else {
            panic!("WASMKEEP_SOAK_SECS needs WASMKEEP_TEST_S3_BUCKET (or WASMKEEP_SOAK_NODES)");
        };
        let addrs: Vec<SocketAddr> = NODES.iter().map(|_| free_port()).collect();
        let mut nodes = Nodes::Local {
            dir: dir.clone(),
            wasm: test_actor_wasm(),
            bucket: format!("s3://{bucket}/soak-{}-{}", std::process::id(), now_ms()),
            metrics: NODES.iter().map(|_| free_port()).collect(),
            children: NODES.iter().map(|_| None).collect(),
        };
        for (i, addr) in addrs.iter().enumerate() {
            nodes.start(i, *addr);
        }
        (addrs, nodes)
    };
    eprintln!(
        "soak: {total:?}, a kill every {kill_every:?}, files in {}",
        dir.display()
    );

    for addr in &addrs {
        let started = Instant::now();
        while send(*addr, "GET", "/_wasmkeep/ready").map(|r| r.0) != Some(200) {
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "{addr} never ready"
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    }
    let live: Arc<Mutex<Vec<bool>>> = Arc::new(Mutex::new(vec![true; NODES.len()]));
    let shared = Arc::new(Mutex::new(Shared::default()));
    let stop = Arc::new(AtomicBool::new(false));

    let actors = actors();
    let writers: Vec<_> = (0..WRITERS)
        .map(|w| {
            let (live, shared, stop, addrs) = (
                Arc::clone(&live),
                Arc::clone(&shared),
                Arc::clone(&stop),
                addrs.clone(),
            );
            std::thread::spawn(move || {
                let mut rng = Rng(0x9e37_79b9_7f4a_7c15 ^ (w as u64 + 1));
                while !stop.load(Ordering::Relaxed) {
                    let actor = rng.below(actors);
                    let up: Vec<usize> = {
                        let l = live.lock().unwrap();
                        (0..NODES.len()).filter(|i| l[*i]).collect()
                    };
                    let node = up[rng.below(up.len())];
                    if durable_write(&shared, &addrs, node, actor, false).is_none() {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                }
            })
        })
        .collect();

    // Plain writers: actors `actors..actors + PLAIN_ACTORS`, apart.
    let plain_writers: Vec<_> = (0..PLAIN_WRITERS)
        .map(|w| {
            let (live, shared, stop, addrs) = (
                Arc::clone(&live),
                Arc::clone(&shared),
                Arc::clone(&stop),
                addrs.clone(),
            );
            std::thread::spawn(move || {
                let mut rng = Rng(0x2545_f491_4f6c_dd1d ^ (w as u64 + 1));
                while !stop.load(Ordering::Relaxed) {
                    let actor = actors + rng.below(PLAIN_ACTORS);
                    let up: Vec<usize> = {
                        let l = live.lock().unwrap();
                        (0..NODES.len()).filter(|i| l[*i]).collect()
                    };
                    let node = up[rng.below(up.len())];
                    let acked = match send(addrs[node], "POST", &actor_path(actor, "/plain")) {
                        Some((200, _, body)) => body.parse::<u64>().ok(),
                        _ => None,
                    };
                    shared.lock().unwrap().plain.push((actor, acked));
                    if acked.is_none() {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                }
            })
        })
        .collect();
    let reads = Arc::new(Mutex::new(Reads::default()));

    // Kills: round robin, one every `kill_every`, the first after one
    // interval; each node comes back after `DOWN`.
    let began = Instant::now();
    let mut kills: Vec<usize> = Vec::new();
    let mut alarm_late: Vec<Duration> = Vec::new();
    let mut alarms_missed = 0u32;
    let mut restart_took: Vec<Duration> = Vec::new();
    let mut failover: Vec<Duration> = Vec::new();
    let mut unavailable = 0u32;
    let mut victim = 0;
    // Kills, restarts and alarms, for samples.csv: (at_ms, kind, actor,
    // node, took_ms).
    let mut events: Vec<Event> = Vec::new();
    loop {
        let next = began + kill_every * u32::try_from(kills.len() + 1).unwrap();
        // Room for the alarms (up to 2 min) and the restart before the end.
        if next + Duration::from_secs(150) > began + total {
            break;
        }
        std::thread::sleep(next.saturating_duration_since(Instant::now()));
        let other = (victim + 1) % NODES.len();
        // The victim's actors, by who ran their last write.
        let owned: Vec<usize> = {
            let s = shared.lock().unwrap();
            let mut v: Vec<usize> = s
                .last_node
                .iter()
                .filter(|(_, n)| n.as_str() == NODES[victim])
                .map(|(a, _)| *a)
                .collect();
            v.sort_unstable();
            v
        };
        // Alarms on some of them, due when the node is killed (setting them
        // takes up to about 1.5 s each; the kill waits for `due`), so how
        // late they fire is how long after their node died. A durable write
        // after each makes the alarm durable too (replication keeps order).
        let due = now_ms() + 20_000;
        let mut alarms: Vec<(usize, u64)> = Vec::new();
        for &actor in owned.iter().take(ALARMS_PER_KILL) {
            let base = send(addrs[other], "GET", &actor_path(actor, "/fired"))
                .and_then(|(s, _, b)| (s == 200).then(|| b.parse::<u64>().ok()).flatten());
            let set = send(
                addrs[victim],
                "POST",
                &actor_path(actor, &format!("/alarm?at={due}")),
            );
            if let (Some(base), Some((200, Some(n), _))) = (base, set)
                && n == NODES[victim]
                && durable_write(&shared, &addrs, victim, actor, false)
                    .is_some_and(|(_, n)| n == NODES[victim])
            {
                alarms.push((actor, base));
            }
        }
        std::thread::sleep(Duration::from_millis(due.saturating_sub(now_ms())));
        live.lock().unwrap()[victim] = false;
        let killed = Instant::now();
        events.push((now_ms(), "kill", None, victim, None));
        nodes.kill(victim);
        // Every other node's metrics as the takeovers begin.
        for (i, id) in NODES.iter().enumerate() {
            if i != victim
                && let Some(page) = nodes.metrics(i).and_then(metrics_page)
            {
                let name = format!("metrics-kill{}-{id}.txt", kills.len() + 1);
                std::fs::write(dir.join(name), page).unwrap();
            }
        }
        eprintln!(
            "soak: {:?} killed {} ({} actors, {} alarms)",
            began.elapsed(),
            NODES[victim],
            owned.len(),
            alarms.len()
        );
        // Readers: the victim's actors, through the live nodes, for
        // READ_FOR; each read against what was acknowledged before the kill.
        let before: HashMap<usize, u64> = {
            let s = shared.lock().unwrap();
            let mut m = HashMap::new();
            for w in s.writes.iter().filter(|w| owned.contains(&w.actor)) {
                if let Some((v, _)) = &w.acked {
                    let e = m.entry(w.actor).or_insert(0);
                    *e = (*e).max(*v);
                }
            }
            m
        };
        let readers: Vec<_> = (0..READERS)
            .filter(|_| !owned.is_empty())
            .map(|r| {
                let (live, addrs, reads, owned, before) = (
                    Arc::clone(&live),
                    addrs.clone(),
                    Arc::clone(&reads),
                    owned.clone(),
                    before.clone(),
                );
                std::thread::spawn(move || {
                    let mut turn = r;
                    while killed.elapsed() < READ_FOR {
                        turn += 1;
                        let actor = owned[turn % owned.len()];
                        let up: Vec<usize> = {
                            let l = live.lock().unwrap();
                            (0..NODES.len()).filter(|i| l[*i]).collect()
                        };
                        let started = Instant::now();
                        let got = send(
                            addrs[up[turn % up.len()]],
                            "GET",
                            &actor_path(actor, "/durable"),
                        )
                        .and_then(|(st, _, b)| {
                            (st == 200).then(|| b.parse::<u64>().ok()).flatten()
                        });
                        let mut rs = reads.lock().unwrap();
                        match got {
                            Some(v) => {
                                rs.ok.push(started.elapsed());
                                let floor = before.get(&actor).copied().unwrap_or(0);
                                if v < floor {
                                    rs.stale.push((actor, v, floor));
                                }
                            }
                            None => rs.failed += 1,
                        }
                        drop(rs);
                        std::thread::sleep(Duration::from_millis(100));
                    }
                })
            })
            .collect();
        // Unavailability: each of the victim's other actors gets a client of
        // its own, retrying a durable write through the live nodes until
        // one is acknowledged (the writers only reach a few at a time).
        // The alarms' actors get none: their recovery is the alarm's.
        let with_alarm: HashSet<usize> = alarms.iter().map(|(a, _)| *a).collect();
        let probes: Vec<_> = owned
            .iter()
            .filter(|a| !with_alarm.contains(a))
            .map(|&actor| {
                let (shared, live, addrs) = (Arc::clone(&shared), Arc::clone(&live), addrs.clone());
                std::thread::spawn(move || {
                    let mut turn = actor;
                    while killed.elapsed() < Duration::from_secs(120) {
                        let up: Vec<usize> = {
                            let l = live.lock().unwrap();
                            (0..NODES.len()).filter(|i| l[*i]).collect()
                        };
                        turn += 1;
                        if durable_write(&shared, &addrs, up[turn % up.len()], actor, true)
                            .is_some()
                        {
                            return Some(killed.elapsed());
                        }
                        std::thread::sleep(Duration::from_millis(100));
                    }
                    None
                })
            })
            .collect();
        // Alarms: when each was first seen fired, through a live node.
        let mut pending = alarms;
        let deadline = killed + Duration::from_secs(120);
        while !pending.is_empty() && Instant::now() < deadline {
            pending.retain(|&(actor, base)| {
                let fired = send(addrs[other], "GET", &actor_path(actor, "/fired"))
                    .and_then(|(s, _, b)| (s == 200).then(|| b.parse::<u64>().ok()).flatten());
                if fired.is_some_and(|f| f > base) {
                    let late = Duration::from_millis(now_ms().saturating_sub(due));
                    eprintln!(
                        "soak: alarm on s-{actor} fired {late:?} late ({:?} after the kill of {})",
                        killed.elapsed(),
                        NODES[victim]
                    );
                    alarm_late.push(late);
                    events.push((
                        now_ms(),
                        "alarm_fired",
                        Some(actor),
                        other,
                        Some(late.as_millis()),
                    ));
                    false
                } else {
                    true
                }
            });
            std::thread::sleep(Duration::from_millis(200));
        }
        alarms_missed += u32::try_from(pending.len()).unwrap();
        for reader in readers {
            reader.join().unwrap();
        }
        for probe in probes {
            match probe.join().unwrap() {
                Some(took) => failover.push(took),
                None => unavailable += 1,
            }
        }
        // Back after DOWN, same id and data directory.
        std::thread::sleep(DOWN.saturating_sub(killed.elapsed()));
        let restarted = Instant::now();
        nodes.start(victim, addrs[victim]);
        while send(addrs[victim], "GET", "/_wasmkeep/ready").map(|r| r.0) != Some(200) {
            assert!(
                restarted.elapsed() < Duration::from_secs(120),
                "{} never ready again",
                NODES[victim]
            );
            std::thread::sleep(Duration::from_millis(200));
        }
        restart_took.push(restarted.elapsed());
        events.push((
            now_ms(),
            "ready",
            None,
            victim,
            Some(restarted.elapsed().as_millis()),
        ));
        live.lock().unwrap()[victim] = true;
        kills.push(victim);
        victim = (victim + 1) % NODES.len();
    }
    std::thread::sleep(total.saturating_sub(began.elapsed()));
    stop.store(true, Ordering::Relaxed);
    for w in writers.into_iter().chain(plain_writers) {
        w.join().unwrap();
    }

    for (i, id) in NODES.iter().enumerate() {
        if let Some(page) = nodes.metrics(i).and_then(metrics_page) {
            std::fs::write(dir.join(format!("metrics-{id}.txt")), page).unwrap();
        }
    }

    // Safety: what every actor holds now against what clients were told.
    let s = shared.lock().unwrap();
    write_samples(&dir.join("samples.csv"), &s.writes, &events);
    let mut max_acked = vec![0u64; actors];
    let mut attempts = vec![0u64; actors];
    let mut seen: HashSet<(usize, u64)> = HashSet::new();
    let mut duplicates = 0u64;
    for w in &s.writes {
        attempts[w.actor] += 1;
        if let Some((v, _)) = &w.acked {
            max_acked[w.actor] = max_acked[w.actor].max(*v);
            if !seen.insert((w.actor, *v)) {
                duplicates += 1;
            }
        }
    }
    let mut lost = 0u64;
    for actor in 0..actors {
        let now = send(addrs[0], "GET", &actor_path(actor, "/durable"))
            .and_then(|(st, _, b)| (st == 200).then(|| b.parse::<u64>().ok()).flatten())
            .unwrap_or_else(|| panic!("s-{actor} unreadable at the end"));
        if now < max_acked[actor] {
            eprintln!(
                "soak: s-{actor} holds {now}, a client was told {}",
                max_acked[actor]
            );
            lost += max_acked[actor] - now;
        }
        assert!(
            now <= attempts[actor],
            "s-{actor}: {now} from {} attempts",
            attempts[actor]
        );
    }

    let mut latency: Vec<Duration> = s
        .writes
        .iter()
        .filter(|w| w.acked.is_some() && !w.probe)
        .map(|w| w.took)
        .collect();
    latency.sort_unstable();
    failover.sort_unstable();
    alarm_late.sort_unstable();
    restart_took.sort_unstable();
    let acked = latency.len() as u128;
    let clients = s.writes.iter().filter(|w| !w.probe).count() as u128;
    let errors = clients - acked;
    let probe_attempts = s.writes.len() as u128 - clients;

    metric("soak/duration", ns(began.elapsed()), "ns");
    metric("soak/writes_acked", acked, "count");
    metric("soak/write_errors", errors, "count");
    metric("soak/probe_attempts", probe_attempts, "count");
    metric(
        "soak/durable_write_p50",
        ns(percentile(&latency, 0.5)),
        "ns",
    );
    metric(
        "soak/durable_write_p99",
        ns(percentile(&latency, 0.99)),
        "ns",
    );
    metric(
        "soak/durable_write_max",
        ns(latency.last().copied().unwrap_or_default()),
        "ns",
    );
    metric("soak/kills", kills.len() as u128, "count");
    metric("soak/failover_actors", failover.len() as u128, "count");
    metric("soak/failover_p50", ns(percentile(&failover, 0.5)), "ns");
    metric("soak/failover_p99", ns(percentile(&failover, 0.99)), "ns");
    metric(
        "soak/failover_max",
        ns(failover.last().copied().unwrap_or_default()),
        "ns",
    );
    metric("soak/failover_never", u128::from(unavailable), "count");
    metric("soak/alarms_fired", alarm_late.len() as u128, "count");
    metric("soak/alarms_missed", u128::from(alarms_missed), "count");
    metric(
        "soak/alarm_late_p50",
        ns(percentile(&alarm_late, 0.5)),
        "ns",
    );
    metric(
        "soak/alarm_late_max",
        ns(alarm_late.last().copied().unwrap_or_default()),
        "ns",
    );
    metric(
        "soak/restart_ready_max",
        ns(restart_took.last().copied().unwrap_or_default()),
        "ns",
    );
    // Plain writes: what the actors hold now against what clients were
    // told (reported only: a plain write may be lost with its node).
    let plain_acked = s.plain.iter().filter(|(_, v)| v.is_some()).count() as u128;
    let (plain_duplicates, plain_lost) = plain_losses(&s.plain, |actor| {
        send(addrs[0], "GET", &actor_path(actor, "/plain"))
            .and_then(|(st, _, b)| (st == 200).then(|| b.parse::<u64>().ok()).flatten())
            .unwrap_or_else(|| panic!("plain s-{actor} unreadable at the end"))
    });
    let rs = reads.lock().unwrap();
    let mut read_took = rs.ok.clone();
    read_took.sort_unstable();
    for (actor, v, floor) in &rs.stale {
        eprintln!("soak: s-{actor} read {v} during a takeover, after {floor} was acknowledged");
    }
    metric("soak/plain_writes_acked", plain_acked, "count");
    metric(
        "soak/plain_duplicates",
        u128::from(plain_duplicates),
        "count",
    );
    metric("soak/plain_lost", u128::from(plain_lost), "count");
    metric("soak/takeover_reads_ok", read_took.len() as u128, "count");
    metric("soak/takeover_reads_failed", u128::from(rs.failed), "count");
    metric(
        "soak/takeover_read_p99",
        ns(percentile(&read_took, 0.99)),
        "ns",
    );
    metric("soak/takeover_reads_stale", rs.stale.len() as u128, "count");
    metric("soak/lost_writes", u128::from(lost), "count");
    metric("soak/duplicate_values", u128::from(duplicates), "count");

    assert_eq!(lost, 0, "acknowledged durable writes were lost");
    assert!(rs.stale.is_empty(), "a takeover read lost a durable write");
    assert_eq!(duplicates, 0, "a value was handed out twice");
    assert_eq!(alarms_missed, 0, "an alarm never fired");
    assert_eq!(unavailable, 0, "an actor stayed unavailable for 2 minutes");
    if let Err(why) = failover_verdict(&failover, failover_bound()) {
        panic!("{why}");
    }
}

/// Plain writes' losses: (values handed out again, all increments lost).
/// A lost increment shows as a value acknowledged twice (later writes
/// re-cover it), or, at the end, as an actor holding less than its last
/// acknowledged value (`now` reads what it holds).
fn plain_losses(acked: &[(usize, Option<u64>)], now: impl Fn(usize) -> u64) -> (u64, u64) {
    let mut seen: HashSet<(usize, u64)> = HashSet::new();
    let mut max: HashMap<usize, u64> = HashMap::new();
    let mut duplicates = 0u64;
    for &(actor, value) in acked {
        let Some(v) = value else { continue };
        if !seen.insert((actor, v)) {
            duplicates += 1;
        }
        let e = max.entry(actor).or_insert(0);
        *e = (*e).max(v);
    }
    let tail: u64 = max
        .iter()
        .map(|(&a, &told)| told.saturating_sub(now(a)))
        .sum();
    (duplicates, duplicates + tail)
}

/// A kill, a restart or an alarm fired, for `samples.csv`: when (wall
/// clock, ms), what, the actor, the node, and how long it took (ms).
type Event = (u64, &'static str, Option<usize>, usize, Option<u128>);

/// `samples.csv`: one row per write and probe (as the clients saw it),
/// kill, restart and alarm fired, by wall-clock time.
fn write_samples(path: &Path, writes: &[Write1], events: &[Event]) {
    let mut rows: Vec<(u64, String)> = Vec::new();
    for w in writes {
        let (value, ran_on) = match &w.acked {
            Some((v, n)) => (v.to_string(), n.clone()),
            None => (String::new(), String::new()),
        };
        let kind = if w.probe { "probe" } else { "write" };
        rows.push((
            w.at_ms,
            format!(
                "{},{kind},{},{},{},{value},{ran_on}",
                w.at_ms,
                w.actor,
                NODES[w.via],
                w.took.as_millis()
            ),
        ));
    }
    for (at, kind, actor, node, took) in events {
        let actor = actor.map(|a| a.to_string()).unwrap_or_default();
        let took = took.map(|t| t.to_string()).unwrap_or_default();
        rows.push((
            *at,
            format!("{at},{kind},{actor},{},{took},,", NODES[*node]),
        ));
    }
    rows.sort_by_key(|(at, _)| *at);
    let mut out = String::from("at_ms,kind,actor,node,took_ms,value,ran_on\n");
    for (_, row) in rows {
        out.push_str(&row);
        out.push('\n');
    }
    std::fs::write(path, out).unwrap();
}

/// The pass bound judges the 99th percentile, not the worst: one slow
/// restore in hundreds of takeovers passes, a slow tail does not.
/// A plain increment lost and re-covered by later writes counts (max
/// minus final read would be 0 whatever was lost), as does a tail
/// the actor no longer holds; a clean run counts nothing.
#[test]
fn plain_losses_count_values_handed_out_again() {
    let clean = [(1, Some(1)), (1, Some(2)), (1, None), (1, Some(3))];
    assert_eq!(plain_losses(&clean, |_| 3), (0, 0));
    // 2 lost with its node, handed out again, then 3: the end holds 3.
    let recovered = [(1, Some(1)), (1, Some(2)), (1, Some(2)), (1, Some(3))];
    assert_eq!(plain_losses(&recovered, |_| 3), (1, 1));
    // The last two lost, nothing after: the actor holds 1.
    let tail = [(1, Some(1)), (1, Some(2)), (1, Some(3))];
    assert_eq!(plain_losses(&tail, |_| 1), (0, 2));
}

#[test]
fn the_failover_bound_judges_the_99th_percentile() {
    let secs = |s: u64| Duration::from_secs(s);
    let mut takeovers: Vec<Duration> = (0..267).map(|i| secs(15 + i % 6)).collect();
    takeovers.push(secs(32));
    takeovers.sort_unstable();
    assert_eq!(failover_verdict(&takeovers, secs(30)), Ok(()));
    let slow: Vec<Duration> = (0..100)
        .map(|i| secs(if i < 95 { 16 } else { 31 }))
        .collect();
    let why = failover_verdict(&slow, secs(30)).unwrap_err();
    assert!(why.contains("p99"), "{why}");
    assert_eq!(failover_verdict(&[], secs(30)), Ok(()));
}
