//! examples/todo-front end to end, as its README runs it: `wasmkeep serve
//! FRONT --actor todos=... --front-env ...`. Requests through the front
//! reach the `todos` actor with the front's caller value; the actor refuses
//! everyone else, a client posing as the front included. The TypeScript
//! front (examples/ts-todo-front, built by `scripts/languages.sh`) runs the
//! same requests when it is built (on QuickJS, and on StarlingMonkey), and
//! must be with `WASMKEEP_REQUIRE_LANGS=1`.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

mod support;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const ALICE: &str = "0123456789abcdef0123456789abcdef";
const BOB: &str = "fedcba9876543210fedcba9876543210";

fn build(dir: &str, name: &str) -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/todo-front")
        .join(dir);
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
    assert!(
        status.success(),
        "building examples/todo-front/{dir} failed"
    );
    root.join(format!("target/wasm32-wasip2/release/{name}.wasm"))
}

struct Server {
    child: Child,
    addr: SocketAddr,
    dir: tempfile::TempDir,
}

impl Server {
    /// What the server logged so far.
    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.path().join("serve.log")).unwrap_or_default()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The TypeScript front on QuickJS, if it is built.
fn ts_front() -> Option<PathBuf> {
    ts_built("front.wasm")
}

/// `examples/ts-todo-front/<file>`, if it is built.
fn ts_built(file: &str) -> Option<PathBuf> {
    let wasm = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/ts-todo-front")
        .join(file);
    if wasm.exists() {
        return Some(wasm);
    }
    assert!(
        std::env::var("WASMKEEP_REQUIRE_LANGS").as_deref() != Ok("1"),
        "examples/ts-todo-front/{file} is not built (scripts/languages.sh builds it)"
    );
    None
}

fn start(tokens: Option<&str>) -> Server {
    start_front(build("front", "todo_front"), tokens)
}

fn start_front(front: PathBuf, tokens: Option<&str>) -> Server {
    let todos = build("todos", "todo_todos");
    let dir = tempfile::tempdir().unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_wasmkeep"));
    cmd.arg("serve")
        .arg(&front)
        .arg("--actor")
        .arg(format!("todos={}", todos.display()))
        .arg("--data-dir")
        .arg(dir.path().join("data"))
        .arg("--cache-dir")
        .arg(dir.path().join("cache"))
        // Bound by the server, read from its log: a port picked here could
        // be another test's by then (support/mod.rs).
        .args(["--listen", "127.0.0.1:0"])
        .env("RUST_LOG", "info")
        // The CLI logs to stdout.
        .stdout(std::fs::File::create(dir.path().join("serve.log")).unwrap())
        .stderr(Stdio::null());
    if let Some(tokens) = tokens {
        let env = dir.path().join("front.env");
        std::fs::write(&env, format!("TOKENS={tokens}\n")).unwrap();
        cmd.arg("--front-env").arg(env);
    }
    let mut child = cmd.spawn().unwrap();
    let addr = support::listening_on(
        &mut child,
        &dir.path().join("serve.log"),
        Duration::from_secs(120),
    );
    let server = Server { child, addr, dir };
    let started = Instant::now();
    while request(addr, "GET", "/", "", "").is_none() {
        assert!(
            started.elapsed() < Duration::from_secs(120),
            "server never answered"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    server
}

/// The status and body, or `None` if nothing answered.
fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &str,
    body: &str,
) -> Option<(u16, String)> {
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(30))).ok()?;
    write!(
        s,
        "{method} {path} HTTP/1.1\r\nHost: shop.example\r\nConnection: close\r\n\
         {headers}Content-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .ok()?;
    let mut raw = String::new();
    s.read_to_string(&mut raw).ok()?;
    let status = raw.split_whitespace().nth(1)?.parse().ok()?;
    let body = raw.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    Some((status, dechunk(body)))
}

/// The status of a POST whose body is sent in `chunks`, without a length.
fn chunked(addr: SocketAddr, path: &str, headers: &str, chunks: &[String]) -> u16 {
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(2)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    let mut raw = format!(
        "POST {path} HTTP/1.1\r\nHost: shop.example\r\nConnection: close\r\n\
         {headers}Transfer-Encoding: chunked\r\n\r\n"
    );
    for c in chunks {
        raw.push_str(&format!("{:x}\r\n{c}\r\n", c.len()));
    }
    raw.push_str("0\r\n\r\n");
    s.write_all(raw.as_bytes()).unwrap();
    let mut answer = String::new();
    let _ = s.read_to_string(&mut answer);
    answer
        .split_whitespace()
        .nth(1)
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("no status: {answer:?}"))
}

fn dechunk(body: &str) -> String {
    let mut out = String::new();
    let mut rest = body;
    while let Some((size, after)) = rest.split_once("\r\n") {
        let Ok(n) = usize::from_str_radix(size.trim(), 16) else {
            return body.to_owned();
        };
        if n == 0 {
            break;
        }
        out.push_str(&after[..n]);
        rest = &after[n + 2..];
    }
    out
}

fn bearer(token: &str) -> String {
    format!("Authorization: Bearer {token}\r\n")
}

#[test]
fn the_front_signs_in_and_reaches_the_internal_actor() {
    signs_in_and_reaches_the_internal_actor(&start(Some(&format!("alice:{ALICE},bob:{BOB}"))));
}

#[test]
fn the_typescript_front_does_the_same() {
    let Some(front) = ts_front() else { return };
    signs_in_and_reaches_the_internal_actor(&start_front(
        front,
        Some(&format!("alice:{ALICE},bob:{BOB}")),
    ));
}

/// On StarlingMonkey too, whose component lowers the batch's results
/// itself.
#[test]
fn the_typescript_front_on_starlingmonkey_does_the_same() {
    let Some(front) = ts_built("front-starlingmonkey.wasm") else {
        return;
    };
    signs_in_and_reaches_the_internal_actor(&start_front(
        front,
        Some(&format!("alice:{ALICE},bob:{BOB}")),
    ));
}

fn signs_in_and_reaches_the_internal_actor(server: &Server) {
    let a = server.addr;

    let (status, _) = request(a, "GET", "/", "", "").unwrap();
    assert_eq!(status, 200);

    // `scheme()` and `host()`: the client's Host, https only when a proxy
    // says so.
    let (_, api) = request(a, "GET", "/api", "", "").unwrap();
    assert!(api.contains("\"http://shop.example/api/todos\""), "{api}");
    let (_, api) = request(a, "GET", "/api", "X-Forwarded-Proto: https\r\n", "").unwrap();
    assert!(api.contains("\"https://shop.example/api/todos\""), "{api}");
    // The client chooses Host: it is escaped, never JSON of its own.
    let mut s = TcpStream::connect(a).unwrap();
    write!(
        s,
        "GET /api HTTP/1.1\r\nHost: evil\",\"admin\":true,\"x\":\"\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut raw = String::new();
    s.read_to_string(&mut raw).unwrap();
    assert!(!raw.contains("\"admin\":true"), "{raw}");

    // The actor answers only its front: a 201 here is the front's caller
    // value, `default/_front/main`, passing the actor's `caller()` check.
    let (status, body) = request(a, "POST", "/api/todos", &bearer(ALICE), "milk").unwrap();
    assert_eq!(status, 201, "{body}");
    // Past 1000 bytes: 413, with a declared length or chunked (no length
    // to check first: the front stops reading past its limit), and
    // nothing added.
    let big = "x".repeat(1001);
    let (status, _) = request(a, "POST", "/api/todos", &bearer(ALICE), &big).unwrap();
    assert_eq!(status, 413);
    assert_eq!(
        chunked(a, "/api/todos", &bearer(ALICE), &vec!["y".repeat(600); 4]),
        413
    );
    assert_eq!(
        chunked(a, "/api/todos", &bearer(ALICE), &["bread".to_owned()]),
        201
    );
    let (status, body) = request(a, "GET", "/api/todos", &bearer(ALICE), "").unwrap();
    assert_eq!(status, 200);
    assert_eq!(
        body.trim(),
        "[{\"id\":1,\"text\":\"milk\"},{\"id\":2,\"text\":\"bread\"}]"
    );
    // Each user has their own actor.
    let (_, body) = request(a, "GET", "/api/todos", &bearer(BOB), "").unwrap();
    assert_eq!(body.trim(), "[]");
    let (status, _) = request(a, "DELETE", "/api/todos/1", &bearer(ALICE), "").unwrap();
    assert_eq!(status, 204);
    // Every user's count, from one batch of calls (ADR 0025), in order.
    let (status, body) = request(a, "GET", "/api/team", &bearer(BOB), "").unwrap();
    assert_eq!((status, body.trim()), (200, "{\"alice\":1,\"bob\":0}"));
    let (status, _) = request(a, "GET", "/api/team", "", "").unwrap();
    assert_eq!(status, 401);

    // No token, a wrong one, or another scheme: 401, and no call.
    for headers in [
        String::new(),
        bearer("0123456789abcdef0123456789abcdee"),
        format!("Authorization: Basic {ALICE}\r\n"),
    ] {
        let (status, _) = request(a, "GET", "/api/todos", &headers, "").unwrap();
        assert_eq!(status, 401, "{headers}");
    }

    // Past the front, at /actor/ (the class is open on `serve`): the actor
    // refuses a client, one claiming to be the front included.
    for headers in ["", "x-wasmkeep-caller: default/_front/main\r\n"] {
        let (status, body) = request(a, "GET", "/actor/todos/alice", headers, "").unwrap();
        // The actor's own refusal (`caller()`), not the host's.
        assert_eq!((status, body.trim()), (403, "forbidden"), "{headers}");
    }

    // Every request ran to its end: no instance trapped or failed after
    // its answer (reading a body must release it).
    std::thread::sleep(Duration::from_millis(200));
    let log = server.log();
    assert!(
        !log.contains("guest_error") && !log.contains("trapped"),
        "{log}"
    );
}

#[test]
fn without_tokens_nobody_signs_in() {
    for tokens in [None, Some(""), Some("alice:short"), Some("alice")] {
        let server = start(tokens);
        let (status, _) = request(server.addr, "GET", "/api/todos", &bearer("short"), "").unwrap();
        assert_eq!(status, 401, "{tokens:?}");
        let (status, _) = request(server.addr, "GET", "/api/todos", &bearer(ALICE), "").unwrap();
        assert_eq!(status, 401, "{tokens:?}");
    }
}
