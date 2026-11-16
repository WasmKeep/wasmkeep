//! Exercise startup in a separate process: never lower the test runner's limit.
#![allow(clippy::unwrap_used)]

#[cfg(unix)]
#[test]
fn low_descriptor_limit_warns_and_continues_startup() {
    let output = std::process::Command::new("sh")
        .args(["-c", "ulimit -n 256; exec \"$@\"", "socket-budget"])
        .arg(env!("CARGO_BIN_EXE_wasmkeep"))
        .args([
            "serve",
            "--actor",
            "chat=not-loaded.wasm",
            "--max-sockets-per-node",
            "0",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("reading not-loaded.wasm"), "{stderr}");
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("degraded"),
        "stdout: {} stderr: {stderr}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn socket_options_are_documented_in_help() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .args(["serve", "--help"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8_lossy(&output.stdout);
    assert!(help.contains("--max-sockets-per-actor"));
    assert!(help.contains("--max-sockets-per-node"));
    assert!(help.contains("--max-sockets-per-app"));
    assert!(help.contains("Zero refuses all WebSockets"));
}
