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

use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::process::Child;
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
