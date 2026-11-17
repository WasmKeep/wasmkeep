//! `serve --node-file` (ADR 0038, section 4): a file this version cannot
//! serve is refused at start, before any listener binds, any bucket is
//! read or anything is written to the data directory.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::process::Command;

/// Runs `wasmkeep serve --node-file` with `text` as the file, which must
/// fail; returns its stderr, and checks the data directory was not made.
fn refused(text: &str) -> String {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wasmkeep-node.toml");
    // Held: a serve that got as far as binding would fail on it, with
    // another message.
    let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = held.local_addr().unwrap().port();
    std::fs::write(
        &path,
        text.replace("PORT", &port.to_string())
            .replace("DATA", "data"),
    )
    .unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .arg("serve")
        .args(["--cache-dir", dir.path().join("cache").to_str().unwrap()])
        // The bucket is never reached: a name nothing resolves.
        .args(["--bucket", "s3://unreachable.invalid/p"])
        .args(["--node-file", path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!out.status.success(), "serve started with {text}");
    assert!(
        !dir.path().join("data").exists(),
        "the data directory was made"
    );
    let err = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(!err.contains("binding"), "{err}");
    assert!(err.contains(path.to_str().unwrap()), "{err}");
    err
}

const TWO: &str = r#"
[process]
data-dir = "DATA"

[listen]
public = "127.0.0.1:PORT"

[[app]]
name = "acme/shop"
no-secrets = true

[[app]]
name = "acme/blog"
no-secrets = true
"#;

#[test]
fn a_second_app_is_refused_before_anything_starts() {
    let err = refused(TWO);
    assert!(err.contains("wasmkeep-node.toml:12:1"), "{err}");
    assert!(err.contains("second [[app]] (acme/blog)"), "{err}");
}

#[test]
fn an_unknown_field_is_refused_before_anything_starts() {
    let err = refused(&TWO.replace(
        "no-secrets = true\n\n[[app]]",
        "no-secret = true\n\n[[app]]",
    ));
    assert!(err.contains("wasmkeep-node.toml:10:1"), "{err}");
    assert!(err.contains("unknown key \"no-secret\""), "{err}");
}

#[test]
fn a_field_this_version_cannot_honour_is_refused_before_anything_starts() {
    let one = TWO.split("\n[[app]]\nname = \"acme/blog\"").next().unwrap();
    let err = refused(&format!("{one}hostnames = [\"shop.example.com\"]\n"));
    assert!(err.contains("hostnames is not available"), "{err}");
}
