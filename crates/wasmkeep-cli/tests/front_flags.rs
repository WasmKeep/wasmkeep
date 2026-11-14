//! `serve`'s front flags (ADR 0023) that are refused at start, before any
//! bucket is read or any component compiled: none of these needs one.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::process::Command;

/// Runs `wasmkeep serve ARGS`, which must fail; returns its stderr.
fn refused(args: &[&str]) -> String {
    let dir = tempfile::tempdir().unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .arg("serve")
        .args(["--data-dir", dir.path().to_str().unwrap()])
        .args(["--cache-dir", dir.path().join("cache").to_str().unwrap()])
        .args(["--listen", "127.0.0.1:0"])
        .args(args)
        .output()
        .unwrap();
    assert!(!out.status.success(), "serve {args:?} started");
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn a_component_with_app_is_refused_naming_the_deploy() {
    // The bucket is never reached: a name nothing resolves.
    let err = refused(&[
        "front-that-does-not-exist.wasm",
        "--app",
        "acme/shop",
        "--bucket",
        "s3://unreachable.invalid/p",
    ]);
    assert!(
        err.contains("wasmkeep deploy --app acme/shop --front PATH"),
        "{err}"
    );
}

#[test]
fn env_does_not_take_the_front() {
    let err = refused(&[
        "--actor",
        "c=missing.wasm",
        "--env",
        "_front=missing.env",
        "front.wasm",
    ]);
    // Refused before the component is read.
    assert!(err.contains("--front-env FILE"), "{err}");
}

#[test]
fn front_requests_must_leave_actors_room() {
    let err = refused(&[
        "front.wasm",
        "--actor",
        "c=missing.wasm",
        "--max-instances",
        "10",
        "--max-front-requests",
        "10",
    ]);
    assert!(err.contains("leaves actors no room"), "{err}");
    let err = refused(&["front.wasm", "--max-front-requests", "0"]);
    assert!(err.contains("at least 1"), "{err}");
    // The default too: one instance cannot hold a front and an actor.
    let err = refused(&[
        "front.wasm",
        "--actor",
        "c=missing.wasm",
        "--max-instances",
        "1",
    ]);
    assert!(err.contains("no room for both"), "{err}");
    // Slots for a front that is not served.
    let err = refused(&["--actor", "c=missing.wasm", "--max-front-requests", "5"]);
    assert!(err.contains("no front is served"), "{err}");
    let err = refused(&["front.wasm", "--front-cpu-budget-ms", "0"]);
    assert!(err.contains("front-cpu-budget-ms"), "{err}");
}

#[test]
fn front_env_needs_a_front() {
    let err = refused(&["--actor", "c=missing.wasm", "--front-env", "f.env"]);
    assert!(err.contains("no front is served"), "{err}");
}
