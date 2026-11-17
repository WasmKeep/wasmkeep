//! The F2 command offers a single poll, with no F5 timer or other target.
#![allow(clippy::unwrap_used)]
use std::process::Command;
#[test]
fn feed_help_and_scope() {
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
            .args(args)
            .output()
            .unwrap()
    };
    let help = run(&["feed", "--help"]);
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).unwrap();
    for option in [
        "TENANT/APP",
        "--bucket",
        "--to",
        "--out",
        "--state",
        "--classes",
        "--max-bytes",
    ] {
        assert!(help.contains(option), "{help}");
    }
    assert!(!help.contains("--every"));
    for extra in [["--to", "duckdb"], ["--every", "60s"]] {
        let mut args = vec![
            "feed",
            "t/a",
            "--bucket",
            "s3://unused",
            "--out",
            "unused",
            "--state",
            "unused-state",
        ];
        args.extend(extra);
        assert_eq!(run(&args).status.code(), Some(2));
    }
}
