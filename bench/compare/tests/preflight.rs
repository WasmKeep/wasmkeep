#![cfg(unix)]
#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "wasmkeep-preflight-test-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        for dir in ["bench/results", "target/criterion", "bin"] {
            fs::create_dir_all(path.join(dir)).unwrap();
        }
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        fs::copy(source.join("run.sh"), path.join("bench/run.sh")).unwrap();
        fs::copy(source.join("preflight.sh"), path.join("bench/preflight.sh")).unwrap();
        fs::write(path.join("target/criterion/KEEP"), "existing estimates").unwrap();
        fs::write(path.join("bench/results/KEEP"), "existing results").unwrap();
        let fixture = Self(path);
        fixture.executable(
            "bin/git",
            "case \"$1\" in\n  rev-parse) printf 'fixture-commit\\n' ;;\n  diff) exit 0 ;;\n  *) exit 99 ;;\nesac\n",
        );
        fixture.executable("bin/rustc", "printf 'rustc 1.99.1 (fixture)\\n'\n");
        fixture.executable("bin/lscpu", "printf 'Model name: fixture CPU\\n'\n");
        fixture.executable("bin/nproc", "printf '1\\n'\n");
        fixture.executable(
            "bin/cargo",
            "ulimit -Sn > \"$PREFLIGHT_TEST_CARGO_MARKER\"\nexit 73\n",
        );
        fixture
    }

    fn executable(&self, name: &str, body: &str) {
        let path = self.0.join(name);
        fs::write(&path, format!("#!/usr/bin/env bash\n{body}")).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    fn fake_server(&self) {
        // Stop only the comparison parent if it reaches the fake server;
        // otherwise its readiness loop would wait five minutes.
        self.executable(
            "bin/fake-server",
            "ulimit -Sn > \"$PREFLIGHT_TEST_SERVER_MARKER\"\nkill -TERM \"$PPID\"\n",
        );
    }

    fn command(&self, soft_limit: u64) -> Command {
        let mut paths = vec![self.0.join("bin")];
        paths.extend(std::env::split_paths(&std::env::var_os("PATH").unwrap()));
        let mut command = Command::new("bash");
        command
            .current_dir(&self.0)
            .env("PATH", std::env::join_paths(paths).unwrap())
            .env("TMPDIR", &self.0)
            .env("PREFLIGHT_TEST_CARGO_MARKER", self.0.join("cargo-ran"))
            .env("PREFLIGHT_TEST_SERVER_MARKER", self.0.join("server-ran"))
            .args([
                "-c",
                "ulimit -Sn \"$1\" || { printf 'test requires child soft limit %s; inherited hard limit is %s\\n' \"$1\" \"$(ulimit -Hn)\" >&2; exit 77; }; shift; exec \"$@\"",
                "preflight-test",
                &soft_limit.to_string(),
            ]);
        command
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn inherited_hard_limit() -> u64 {
    let output = Command::new("bash")
        .args(["-c", "ulimit -Hn"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let hard = String::from_utf8(output.stdout).unwrap();
    match hard.trim() {
        "unlimited" => u64::MAX,
        value => value.parse().expect("numeric hard descriptor limit"),
    }
}

/// Low-limit cases must execute even when the inherited hard limit is low.
fn low_limit(minimum: u64) -> u64 {
    (minimum - 1).min(inherited_hard_limit())
}

/// Only the sufficient-limit controls may skip on a constrained local machine.
/// CI=true (case-insensitive) or CI=1 requires the positive path to be checked.
fn sufficient_limit_control(required: u64) -> bool {
    let hard = inherited_hard_limit();
    if hard >= required {
        return true;
    }
    let reason = format!("requires {required} descriptors, but the inherited hard limit is {hard}");
    let ci_enabled =
        std::env::var("CI").is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
    assert!(
        !ci_enabled,
        "{reason}; CI is enabled: raise the runner's hard limit to at least {required} before rerunning"
    );
    eprintln!("skipping sufficient-limit control: {reason}; CI is not enabled");
    false
}

fn assert_low_limit(output: &Output, limit: u64, minimum: u64) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(2), "{stderr}");
    for expected in [format!("soft={limit}"), minimum.to_string(), "hard=".into()] {
        assert!(stderr.contains(&expected), "missing {expected}: {stderr}");
    }
}

#[test]
fn full_run_refuses_low_limit_before_cleanup_or_build() {
    let fixture = Fixture::new();
    let limit = low_limit(4096);
    let output = fixture
        .command(limit)
        .args(["bash", "bench/run.sh"])
        .output()
        .unwrap();
    assert!(!fixture.0.join("cargo-ran").exists(), "Cargo was launched");
    assert_eq!(
        fs::read_to_string(fixture.0.join("target/criterion/KEEP")).unwrap(),
        "existing estimates"
    );
    assert_eq!(
        fs::read_to_string(fixture.0.join("bench/results/KEEP")).unwrap(),
        "existing results"
    );
    assert_low_limit(&output, limit, 4096);
}

#[test]
fn full_run_preserves_sufficient_limit_when_it_starts_building() {
    if !sufficient_limit_control(4096) {
        return;
    }
    let fixture = Fixture::new();
    let output = fixture
        .command(4096)
        .args(["bash", "bench/run.sh"])
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(73),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(fixture.0.join("cargo-ran")).unwrap(),
        "4096\n"
    );
}

#[test]
fn direct_comparison_refuses_its_workload_low_limit_before_launching_server() {
    for (quick, minimum) in [(false, 4096), (true, 1024)] {
        let fixture = Fixture::new();
        fixture.fake_server();
        let limit = low_limit(minimum);
        let mut command = fixture.command(limit);
        command
            .env("WASMKEEP_BIN", fixture.0.join("bin/fake-server"))
            .arg(env!("CARGO_BIN_EXE_compare"));
        if quick {
            command.arg("--quick");
        }
        let output = command.arg("wasmkeep-rust").output().unwrap();
        assert!(
            !fixture.0.join("server-ran").exists(),
            "server was launched (quick={quick})"
        );
        assert!(
            output.stdout.is_empty(),
            "partial measurements were printed"
        );
        assert_low_limit(&output, limit, minimum);
    }
}

#[test]
fn quick_comparison_accepts_1024_and_preserves_it_when_launching_server() {
    if !sufficient_limit_control(1024) {
        return;
    }
    let fixture = Fixture::new();
    fixture.fake_server();
    let output = fixture
        .command(1024)
        .env("WASMKEEP_BIN", fixture.0.join("bin/fake-server"))
        .args([env!("CARGO_BIN_EXE_compare"), "--quick", "wasmkeep-rust"])
        .output()
        .unwrap();
    assert!(
        fixture.0.join("server-ran").exists(),
        "server did not start: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(fixture.0.join("server-ran")).unwrap(),
        "1024\n"
    );
}

fn mocked_preflight(fixture: &Fixture, minimum: &str, soft: &str, hard: &str) -> Output {
    // Mock the builtin to cover unlimited and low hard limits without
    // changing the test runner's limits or requiring extra privileges.
    Command::new("bash")
        .current_dir(&fixture.0)
        .env("PREFLIGHT_TEST_SOFT", soft)
        .env("PREFLIGHT_TEST_HARD", hard)
        .args([
            "-c",
            "ulimit() { case \"$1\" in -Sn) printf '%s\\n' \"$PREFLIGHT_TEST_SOFT\" ;; -Hn) printf '%s\\n' \"$PREFLIGHT_TEST_HARD\" ;; *) return 1 ;; esac; }; source bench/preflight.sh \"$1\"",
            "preflight-test",
            minimum,
        ])
        .output()
        .unwrap()
}

#[test]
fn shared_preflight_checks_selected_boundary_and_unlimited_limits() {
    let fixture = Fixture::new();
    for (minimum, soft, expected_code) in [
        ("4096", "4095", 2),
        ("4096", "4096", 0),
        ("512", "511", 2),
        ("512", "512", 0),
        ("512", "1024", 0),
        ("4096", "unlimited", 0),
        ("4096", "unknown", 2),
    ] {
        let output = mocked_preflight(&fixture, minimum, soft, "8192");
        assert_eq!(
            output.status.code(),
            Some(expected_code),
            "minimum={minimum}, soft={soft}"
        );
        assert!(output.stdout.is_empty());
        if expected_code == 0 {
            assert!(output.stderr.is_empty());
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(stderr.contains(soft));
            assert!(stderr.contains("hard=8192"));
        }
    }
}

#[test]
fn shared_preflight_only_suggests_raising_soft_limit_when_hard_limit_allows_it() {
    let fixture = Fixture::new();
    for hard in ["4095", "4096", "unlimited"] {
        let output = mocked_preflight(&fixture, "4096", "1024", hard);
        assert_eq!(output.status.code(), Some(2));
        let stderr = String::from_utf8_lossy(&output.stderr);
        if hard == "4095" {
            assert!(stderr.contains("parent shell, service manager or container"));
            assert!(
                !stderr.contains("ulimit -Sn"),
                "impossible advice: {stderr}"
            );
        } else {
            assert!(stderr.contains("ulimit -Sn 4096"), "{stderr}");
        }
    }
}

#[test]
fn shared_preflight_rejects_missing_invalid_or_extra_minimum_arguments() {
    let fixture = Fixture::new();
    for args in [
        vec![],
        vec!["0"],
        vec!["-1"],
        vec!["unknown"],
        vec!["512", "4096"],
    ] {
        let output = Command::new("bash")
            .current_dir(&fixture.0)
            .arg("bench/preflight.sh")
            .args(&args)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "args={args:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("usage:"),
            "args={args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

fn test_with_hard_limit(name: &str, hard_limit: u64, ci: Option<&str>) -> Output {
    let mut command = Command::new("bash");
    command.env_remove("CI");
    if let Some(value) = ci {
        command.env("CI", value);
    }
    command
        .args([
            "-c",
            "ulimit -Sn \"$1\" && ulimit -Hn \"$1\" || exit 77; shift; exec \"$@\"",
            "preflight-policy-test",
            &hard_limit.to_string(),
        ])
        .arg(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .output()
        .unwrap()
}

#[test]
fn sufficient_limit_controls_skip_only_locally_when_the_hard_limit_is_too_low() {
    for (name, required) in [
        (
            "full_run_preserves_sufficient_limit_when_it_starts_building",
            4096,
        ),
        (
            "quick_comparison_accepts_1024_and_preserves_it_when_launching_server",
            1024,
        ),
    ] {
        let hard_limit = low_limit(required);
        for (ci, enabled) in [
            (None, false),
            (Some("false"), false),
            (Some("0"), false),
            (Some(""), false),
            (Some("true"), true),
            (Some("TRUE"), true),
            (Some("1"), true),
        ] {
            let output = test_with_hard_limit(name, hard_limit, ci);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert_eq!(
                output.status.success(),
                !enabled,
                "{name}, CI={ci:?}: {stderr}"
            );
            for expected in [
                format!("requires {required}"),
                format!("hard limit is {hard_limit}"),
            ] {
                assert!(stderr.contains(&expected), "missing {expected}: {stderr}");
            }
            if !enabled {
                assert!(
                    stderr.contains("skipping sufficient-limit control"),
                    "{stderr}"
                );
            } else {
                assert!(!stderr.contains("skipping"), "{stderr}");
                assert!(stderr.contains("CI is enabled"), "{stderr}");
                assert!(stderr.contains("raise the runner's hard limit"), "{stderr}");
            }
        }
    }
}

#[test]
fn low_limit_controls_still_run_with_a_low_hard_limit_in_ci() {
    let hard_limit = low_limit(512);
    for name in [
        "full_run_refuses_low_limit_before_cleanup_or_build",
        "direct_comparison_refuses_its_workload_low_limit_before_launching_server",
    ] {
        let output = test_with_hard_limit(name, hard_limit, Some("true"));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{name}: {stderr}");
        assert!(!stderr.contains("skipping"), "{stderr}");
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("1 passed"),
            "{name}: the low-limit control did not execute"
        );
    }
}
