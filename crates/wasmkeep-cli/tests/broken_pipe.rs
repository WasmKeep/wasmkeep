//! A command whose output goes to a pipe nobody reads any more (`wasmkeep
//! cluster status --json | head`) exits quietly: no panic message, and
//! status 0 or 141 (as if killed by SIGPIPE, what `head`'s other writers
//! report).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::process::{Command, Stdio};

#[cfg(unix)]
#[test]
fn a_closed_stdout_ends_a_command_quietly() {
    let dir = tempfile::tempdir().unwrap();
    // The read end closed before the command starts: its first write fails
    // with EPIPE, whatever the timing.
    let (reader, writer) = std::io::pipe().unwrap();
    drop(reader);
    let out = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .args(["secrets", "new-key", "--out"])
        .arg(dir.path().join("secrets.key"))
        .stdout(writer)
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(!err.contains("panicked"), "{err}");
    assert!(!err.contains("Broken pipe"), "{err}");
    assert!(
        out.status.success() || out.status.code() == Some(141),
        "{:?}: {err}",
        out.status
    );
    // The command did its work before its output was lost.
    assert!(dir.path().join("secrets.key").exists());
}
