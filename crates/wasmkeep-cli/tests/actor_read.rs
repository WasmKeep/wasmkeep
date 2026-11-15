//! CLI grammar and help; no bucket credentials or node needed.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use std::process::Command;
fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .args(args)
        .output()
        .unwrap()
}
#[test]
fn actor_export_and_sql_help_describe_read_only_boundaries() {
    for command in ["export", "sql"] {
        let out = run(&["actor", command, "--help"]);
        assert!(out.status.success());
        let help = String::from_utf8(out.stdout).unwrap();
        println!("{help}");
        for option in [
            "TENANT/APP",
            "--bucket",
            "--at",
            "--txid",
            "--max-bytes",
            "--tmpdir",
            "--require-owner-match",
        ] {
            assert!(help.contains(option), "{help}");
        }
    }
}
#[test]
fn conflicting_points_and_unknown_formats_are_rejected_before_bucket_access() {
    for args in [
        vec![
            "actor",
            "export",
            "acme/shop",
            "c/n",
            "--bucket",
            "s3://unused",
            "-o",
            "unused.db",
            "--at",
            "-15m",
            "--txid",
            "2",
        ],
        vec![
            "actor",
            "sql",
            "acme/shop",
            "c/n",
            "SELECT 1",
            "--bucket",
            "s3://unused",
            "--format",
            "xml",
        ],
    ] {
        let out = run(&args);
        assert_eq!(out.status.code(), Some(2));
    }
}
