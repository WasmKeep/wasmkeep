//! The cross-node suite (`cluster.rs`) again, with every node behind a
//! cluster listener: forwarded requests, relayed WebSockets and calls
//! between nodes all go over mutual TLS with pinned certificates
//! (ADR 0011), and nodes publish a plain address nobody listens on.

#[path = "cluster/main.rs"]
mod suite;

#[test]
fn this_copy_of_the_suite_runs_over_tls() {
    assert!(suite::over_tls());
}
