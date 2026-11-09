//! A node whose open-files limit is low (the common default of 1,024) keeps
//! serving new actors: it sizes its resident-actor and connection caps to
//! the descriptors it may open, instead of failing when they run out (each
//! resident actor holds three).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

struct Node(Child);

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn counter_wasm() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/counter");
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
    assert!(status.success(), "building examples/counter failed");
    root.join("target/wasm32-wasip2/release/counter.wasm")
}

#[derive(Debug)]
struct Reply {
    status: u16,
    retry_after: Option<String>,
    body: String,
}

fn request(addr: SocketAddr, method: &str, path: &str) -> Option<Reply> {
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(30))).ok()?;
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    s.write_all(req.as_bytes()).ok()?;
    let mut raw = String::new();
    s.read_to_string(&mut raw).ok()?;
    let (headers, body) = raw.split_once("\r\n\r\n")?;
    let status = headers.split_whitespace().nth(1)?.parse().ok()?;
    let retry_after = headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("retry-after")
            .then(|| value.trim().to_owned())
    });
    let chunked = headers.lines().any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("transfer-encoding") && value.trim() == "chunked"
        })
    });
    let mut decoded = String::new();
    if chunked {
        let mut rest = body;
        loop {
            let (length, tail) = rest.split_once("\r\n")?;
            let length = usize::from_str_radix(length, 16).ok()?;
            if length == 0 {
                break;
            }
            decoded.push_str(tail.get(..length)?);
            rest = tail.get(length + 2..)?;
        }
    } else {
        decoded.push_str(body);
    }
    Some(Reply {
        status,
        retry_after,
        body: decoded,
    })
}

fn value(addr: SocketAddr, method: &str, actor: usize) -> u64 {
    let path = format!("/actor/counter/a{actor}");
    let deadline = Instant::now() + Duration::from_secs(10);
    for _ in 0..1000 {
        // Retry only an explicit refusal: a broken connection might hide a
        // committed POST, so retrying it could mask a duplicated mutation.
        let reply = request(addr, method, &path).expect("request failed after readiness");
        match reply.status {
            200 => return reply.body.trim().parse().expect("counter value"),
            503 => {
                assert_eq!(reply.retry_after.as_deref(), Some("1"), "{reply:?}");
                assert!(Instant::now() < deadline, "actor {actor} never admitted");
                // Deliberately retry sooner than the advisory one-second
                // client delay to stress admission while eviction finishes.
                std::thread::sleep(Duration::from_millis(5));
            }
            _ => panic!("actor {actor}: expected 200 or retryable 503, got {reply:?}"),
        }
    }
    panic!("actor {actor}: bounded admission retries exhausted")
}

#[cfg(target_os = "linux")]
fn open_descriptors(pid: u32) -> Option<usize> {
    std::fs::read_dir(format!("/proc/{pid}/fd"))
        .ok()
        .map(Iterator::count)
}

#[cfg(not(target_os = "linux"))]
fn open_descriptors(_pid: u32) -> Option<usize> {
    None
}

struct DescriptorSampler {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<Option<usize>>>,
}

impl DescriptorSampler {
    fn start(pid: u32) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            let mut peak = open_descriptors(pid)?;
            while !stopping.load(Ordering::Relaxed) {
                peak = peak.max(open_descriptors(pid).unwrap_or(peak));
                std::thread::sleep(Duration::from_millis(2));
            }
            Some(peak)
        });
        Self {
            stop,
            thread: Some(thread),
        }
    }

    fn finish(mut self) -> Option<usize> {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.take().unwrap().join().unwrap()
    }
}

impl Drop for DescriptorSampler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[test]
fn a_low_open_files_limit_caps_residents_instead_of_failing() {
    let wasm = counter_wasm();
    let dir = tempfile::tempdir().unwrap();
    let addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let log = dir.path().join("node.log");
    // Hard limit 1,024: the node cannot raise it, so it must fit inside.
    let node = Node(
        Command::new("bash")
            .args([
                "-c",
                "ulimit -Sn 1024 && ulimit -Hn 1024 && exec \"$@\"",
                "open-files-test",
                env!("CARGO_BIN_EXE_wasmkeep"),
                "serve",
                "--actor",
            ])
            .arg(format!("counter={}", wasm.display()))
            .arg("--data-dir")
            .arg(dir.path().join("data"))
            .arg("--cache-dir")
            .arg(dir.path().join("cache"))
            .args(["--listen", &addr.to_string(), "--idle-timeout-secs", "600"])
            .env("RUST_LOG", "warn")
            .stdout(std::fs::File::create(&log).unwrap())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let started = Instant::now();
    while !request(addr, "GET", "/_wasmkeep/health").is_some_and(|reply| reply.status == 200) {
        assert!(started.elapsed() < Duration::from_secs(60), "never started");
        std::thread::sleep(Duration::from_millis(100));
    }
    let descriptors = DescriptorSampler::start(node.0.id());
    const ACTORS: usize = 2000;
    const CLIENTS: usize = 8;
    const PER_CLIENT: usize = 32;
    // More than six complete residency turnovers at the 1,024-descriptor
    // limit: every new actor commits once, with no ambiguous POST retries.
    for actor in 0..ACTORS {
        assert_eq!(value(addr, "POST", actor), 1, "actor {actor}");
    }
    // Simultaneous requests mix old actors being restored and new actors
    // being admitted. Each client owns its names, so exact values remain
    // deterministic even when admission refuses and the client retries.
    let barrier = std::sync::Barrier::new(CLIENTS);
    std::thread::scope(|scope| {
        let mut clients = Vec::new();
        for client in 0..CLIENTS {
            let barrier = &barrier;
            clients.push(scope.spawn(move || {
                barrier.wait();
                for offset in 0..PER_CLIENT {
                    let actor = client * PER_CLIENT + offset;
                    assert_eq!(value(addr, "POST", actor), 2, "existing actor {actor}");
                    assert_eq!(value(addr, "POST", ACTORS + actor), 1, "new actor {actor}");
                }
            }));
        }
        for client in clients {
            client.join().unwrap();
        }
    });
    for actor in 0..ACTORS + CLIENTS * PER_CLIENT {
        let expected = if actor < CLIENTS * PER_CLIENT { 2 } else { 1 };
        assert_eq!(
            value(addr, "GET", actor),
            expected,
            "restored actor {actor}"
        );
    }
    // The clients use only eight concurrent connections, covered by the
    // reserved headroom; assert the actor budget, not the OS hard ceiling.
    let (_, resident_limit) = wasmkeep_server::fds::budget(1024, 10_000, 900);
    let descriptor_budget =
        wasmkeep_server::fds::PER_ACTOR * resident_limit as u64 + wasmkeep_server::fds::RESERVED;
    if let Some(peak) = descriptors.finish() {
        assert!(
            peak as u64 <= descriptor_budget,
            "sampled {peak} open descriptors; expected actor budget plus headroom <= {descriptor_budget}"
        );
        eprintln!("verified 2,256 counter states; sampled peak {peak} open descriptors");
    } else {
        eprintln!(
            "verified 2,256 counter states; descriptor sampling unavailable on this platform"
        );
    }
    // And the node said why it capped them.
    let said = std::fs::read_to_string(&log).unwrap();
    assert!(
        said.contains("open-files limit is low"),
        "missing descriptor-cap warning"
    );
    let lower = said.to_ascii_lowercase();
    assert!(
        !lower.contains("too many open files")
            && !lower.contains("emfile")
            && !lower.contains("os error 24"),
        "server logged descriptor exhaustion"
    );
}

#[test]
fn an_eight_instance_cli_node_can_run_an_actor_call() {
    let guest = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../wasmkeep-server/tests/guests/test-actor");
    let status = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()))
        .current_dir(&guest)
        .args([
            "build",
            "--release",
            "--locked",
            "--target",
            "wasm32-wasip2",
        ])
        .status()
        .unwrap();
    assert!(status.success());
    let wasm = guest.join("target/wasm32-wasip2/release/test_actor.wasm");
    let dir = tempfile::tempdir().unwrap();
    let addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let _node = Node(
        Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
            .args(["serve", "--actor"])
            .arg(format!("t={}", wasm.display()))
            .arg("--data-dir")
            .arg(dir.path().join("data"))
            .arg("--cache-dir")
            .arg(dir.path().join("cache"))
            .args(["--listen", &addr.to_string(), "--max-instances", "8"])
            .stdout(Stdio::null())
            // Kept: it once never answered within 60 s under full-suite
            // load, with nothing to say why.
            .stderr(std::fs::File::create(dir.path().join("serve.log")).unwrap())
            .spawn()
            .unwrap(),
    );
    let started = Instant::now();
    while !request(addr, "GET", "/_wasmkeep/health").is_some_and(|reply| reply.status == 200) {
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "never started; its output:\n{}",
            std::fs::read_to_string(dir.path().join("serve.log")).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let reply = request(addr, "POST", "/actor/t/a/call?to=t/b&path=/fired").unwrap();
    assert_eq!(reply.status, 200, "{reply:?}");
    assert_eq!(reply.body.trim(), "200:0", "{reply:?}");
    assert!(dir.path().join("data/actors/default/t/b.db").exists());
}
