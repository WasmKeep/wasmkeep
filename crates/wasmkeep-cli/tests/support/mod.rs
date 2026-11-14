//! Shared by the tests that run `wasmkeep serve` as a process.
//!
//! Picking a free port, releasing it and passing it to the server races
//! with every other test doing the same: two can get one port, and a test
//! then talks to another test's server. So a server binds `--listen
//! 127.0.0.1:0` and the test reads the address it logged
//! ([`listening_on`]). A cluster node must name its address before it
//! starts (`--advertise`), so it still gets a picked port; the test then
//! waits for that process's own `listening on` line before it sends
//! anything, and a node that could not bind fails the test instead of
//! leaving it to talk to someone else's server.

#![allow(dead_code, unreachable_pub)] // each test crate uses part of it

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// A port free a moment ago, for a flag that cannot take port 0 (a
/// cluster node's `--advertise`). Wait for [`listening_on`] before using
/// it.
pub fn picked_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

/// The address `child` logged in `log` (`listening on http://ADDR`, at
/// info level on target `wasmkeep`), once it has. Panics, with the log,
/// if the child exits first or nothing is logged within `limit`.
pub fn listening_on(child: &mut Child, log: &Path, limit: Duration) -> SocketAddr {
    let started = Instant::now();
    loop {
        let text = std::fs::read_to_string(log).unwrap_or_default();
        if let Some(addr) = logged_address(&text) {
            return addr;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("the server exited ({status}) before listening:\n{text}");
        }
        assert!(
            started.elapsed() < limit,
            "the server did not listen within {limit:?}:\n{text}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The first `listening on http://ADDR` address in a log (colored or
/// not).
fn logged_address(log: &str) -> Option<SocketAddr> {
    const MARK: &str = "listening on http://";
    let at = log.find(MARK)? + MARK.len();
    let rest = &log[at..];
    let end = rest
        .find(|c: char| c.is_whitespace() || c == '\u{1b}')
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

/// A `wasmkeep dev` process, killed when dropped, with the temporary
/// directory it runs in.
pub struct Dev {
    pub child: Child,
    pub addr: SocketAddr,
    /// The directory `dev` runs in (the project file's).
    pub dir: PathBuf,
    /// Where its stdout and stderr go.
    pub log_path: PathBuf,
    _temp: tempfile::TempDir,
}

impl Dev {
    /// What it logged so far.
    pub fn log(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }
}

impl Drop for Dev {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Starts `wasmkeep dev` in `dir` (inside `temp`, which the returned
/// [`Dev`] keeps), logging to `temp/dev.log`, polling every `poll_ms`, and
/// waits up to `limit` for it to listen (its builds run first) and answer.
pub fn start_dev(temp: tempfile::TempDir, dir: PathBuf, poll_ms: u64, limit: Duration) -> Dev {
    let log_path = temp.path().join("dev.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let poll = poll_ms.to_string();
    let mut child = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .current_dir(&dir)
        .args(["dev", "--listen", "127.0.0.1:0", "--poll-ms", &poll])
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .stdin(Stdio::null())
        .spawn()
        .unwrap();
    let addr = listening_on(&mut child, &log_path, limit);
    let dev = Dev {
        child,
        addr,
        dir,
        log_path,
        _temp: temp,
    };
    let started = Instant::now();
    while request(addr, "GET", "/", "", "").is_none() {
        assert!(
            started.elapsed() < limit,
            "dev never answered: {}",
            dev.log()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    dev
}

/// The status and body of one HTTP/1.1 request (`headers` are whole
/// lines, each ending in CRLF), or `None` if nothing answered.
pub fn request(
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
        "{method} {path} HTTP/1.1\r\nHost: todo.example\r\nConnection: close\r\n\
         {headers}Content-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .ok()?;
    let mut raw = String::new();
    s.read_to_string(&mut raw).ok()?;
    let status = raw.split_whitespace().nth(1)?.parse().ok()?;
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((&raw, ""));
    let chunked = head
        .lines()
        .any(|l| l.eq_ignore_ascii_case("transfer-encoding: chunked"));
    Some((
        status,
        if chunked {
            dechunk(body)
        } else {
            body.to_owned()
        },
    ))
}

/// A chunked body's bytes (as text), or the body as it came if it is not
/// well formed.
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
        let (Some(chunk), Some(next)) = (after.get(..n), after.get(n + 2..)) else {
            return body.to_owned();
        };
        out.push_str(chunk);
        rest = next;
    }
    out
}

#[test]
fn the_logged_address_is_read_colored_or_not() {
    let plain = "2026 INFO wasmkeep: listening on http://127.0.0.1:40123\n";
    assert_eq!(
        logged_address(plain),
        Some("127.0.0.1:40123".parse().unwrap())
    );
    let colored = "\u{1b}[32m INFO\u{1b}[0m wasmkeep: listening on http://127.0.0.1:9\u{1b}[0m\n";
    assert_eq!(
        logged_address(colored),
        Some("127.0.0.1:9".parse().unwrap())
    );
    assert_eq!(
        logged_address("metrics on http://127.0.0.1:1/metrics\n"),
        None
    );
}
