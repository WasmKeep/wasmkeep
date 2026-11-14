//! examples/todo-front end to end, as its README runs it: `wasmkeep serve
//! FRONT --actor todos=... --front-env ...`. Requests through the front
//! reach the `todos` actor with the front's caller value; the actor refuses
//! everyone else, a client posing as the front included.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
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
    _dir: tempfile::TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start(tokens: Option<&str>) -> Server {
    let front = build("front", "todo_front");
    let todos = build("todos", "todo_todos");
    let dir = tempfile::tempdir().unwrap();
    let addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_wasmkeep"));
    cmd.arg("serve")
        .arg(&front)
        .arg("--actor")
        .arg(format!("todos={}", todos.display()))
        .arg("--data-dir")
        .arg(dir.path().join("data"))
        .arg("--cache-dir")
        .arg(dir.path().join("cache"))
        .args(["--listen", &addr.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(tokens) = tokens {
        let env = dir.path().join("front.env");
        std::fs::write(&env, format!("TOKENS={tokens}\n")).unwrap();
        cmd.arg("--front-env").arg(env);
    }
    let server = Server {
        child: cmd.spawn().unwrap(),
        addr,
        _dir: dir,
    };
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
    let server = start(Some(&format!("alice:{ALICE},bob:{BOB}")));
    let a = server.addr;

    let (status, page) = request(a, "GET", "/", "", "").unwrap();
    assert_eq!(status, 200);
    assert!(page.contains("<title>Todos</title>"), "{page}");

    // `scheme()` and `host()`: the client's Host, https only when a proxy
    // says so.
    let (_, api) = request(a, "GET", "/api", "", "").unwrap();
    assert!(api.contains("\"http://shop.example/api/todos\""), "{api}");
    let (_, api) = request(a, "GET", "/api", "X-Forwarded-Proto: https\r\n", "").unwrap();
    assert!(api.contains("\"https://shop.example/api/todos\""), "{api}");

    // The actor answers only its front: a 201 here is the front's caller
    // value, `default/_front/main`, passing the actor's `caller()` check.
    let (status, body) = request(a, "POST", "/api/todos", &bearer(ALICE), "milk").unwrap();
    assert_eq!(status, 201, "{body}");
    let (status, body) = request(a, "GET", "/api/todos", &bearer(ALICE), "").unwrap();
    assert_eq!(status, 200);
    assert_eq!(body.trim(), "[{\"id\":1,\"text\":\"milk\"}]");
    // Each user has their own actor.
    let (_, body) = request(a, "GET", "/api/todos", &bearer(BOB), "").unwrap();
    assert_eq!(body.trim(), "[]");
    let (status, _) = request(a, "DELETE", "/api/todos/1", &bearer(ALICE), "").unwrap();
    assert_eq!(status, 204);

    // No token, a wrong one, or another scheme: 401, and no call.
    for headers in [
        String::new(),
        bearer("0123456789abcdef0123456789abcdee"),
        format!("Authorization: Basic {ALICE}\r\n"),
        // A client's caller header never reaches the front.
        "x-wasmkeep-caller: default/_front/main\r\n".to_owned(),
    ] {
        let (status, _) = request(a, "GET", "/api/todos", &headers, "").unwrap();
        assert_eq!(status, 401, "{headers}");
    }

    // Past the front, at /actor/ (the class is open on `serve`): the actor
    // refuses a client, one claiming to be the front included.
    for headers in ["", "x-wasmkeep-caller: default/_front/main\r\n"] {
        let (status, _) = request(a, "GET", "/actor/todos/alice", headers, "").unwrap();
        assert_eq!(status, 403, "{headers}");
    }
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
