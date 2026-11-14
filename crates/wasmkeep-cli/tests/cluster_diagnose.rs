//! `wasmkeep cluster diagnose`, run as a binary against a real S3 API.
//!
//! Skipped unless `WASMKEEP_TEST_S3_BUCKET` names a bucket; endpoint and
//! credentials come from `AWS_*` (moto in `scripts/s3-suites.sh` and the
//! CI). Each run writes under its own prefix. The argument checks need no
//! bucket.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::process::{Command, Output};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::TryStreamExt as _;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload};

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

fn diagnose(url: &str, extra: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .args(["cluster", "diagnose", "--bucket", url])
        .args(extra)
        .output()
        .unwrap()
}

/// The bucket and a prefix unique to this test, or `None` to skip.
fn s3(test: &str) -> Option<(String, String, Arc<dyn ObjectStore>)> {
    let Ok(bucket) = std::env::var("WASMKEEP_TEST_S3_BUCKET") else {
        eprintln!("WASMKEEP_TEST_S3_BUCKET not set; skipping");
        return None;
    };
    let store: Arc<dyn ObjectStore> = Arc::new(
        object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(&bucket)
            .build()
            .unwrap(),
    );
    let prefix = format!("{test}-{}-{}", std::process::id(), now_ms());
    Some((bucket, prefix, store))
}

fn listed(store: &Arc<dyn ObjectStore>, prefix: &str) -> Vec<String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        store
            .list(Some(&Path::from(prefix)))
            .map_ok(|m| m.location.to_string())
            .try_collect()
            .await
            .unwrap()
    })
}

fn text(out: &Output) -> String {
    format!(
        "status {:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

#[test]
fn the_test_store_is_safe_and_left_as_found() {
    let Some((bucket, prefix, store)) = s3("diagnose-safe") else {
        return;
    };
    // A cluster's key beside the scratch prefix: never touched.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(store.put(
        &Path::from(format!("{prefix}/nodes/a")),
        PutPayload::from_static(b"lease"),
    ))
    .unwrap();
    let url = format!("s3://{bucket}/{prefix}");
    let out = diagnose(&url, &[]);
    let shown = text(&out);
    assert_eq!(out.status.code(), Some(0), "{shown}");
    let stdout = String::from_utf8_lossy(&out.stdout);
    for name in [
        "start-check",
        "create-if-absent",
        "concurrent-creates",
        "replace-if-match",
        "concurrent-replaces",
        "read-after-write",
        "read-after-overwrite",
        "list-new-key",
        "list-deleted-key",
        "delete-absent",
        "large-object",
        "last-modified",
        "clock-skew",
        "list-paging",
    ] {
        assert!(
            stdout
                .lines()
                .any(|l| l.starts_with("pass") && l.contains(name)),
            "{name}: {shown}"
        );
    }
    assert!(stdout.contains("is empty again"), "{shown}");
    assert!(stdout.trim_end().ends_with("safe for WasmKeep"), "{shown}");
    assert_eq!(
        listed(&store, &prefix),
        [format!("{prefix}/nodes/a")],
        "only the scratch prefix was written, and it was emptied"
    );

    let out = diagnose(&url, &["--json", "--skip-paging"]);
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["verdict"], "safe");
    assert_eq!(json["exit_code"], 0);
    assert_eq!(json["cleanup"]["ok"], true);
    let paging = json["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "list-paging")
        .unwrap();
    assert_eq!(paging["status"], "skipped");
    assert_eq!(listed(&store, &prefix), [format!("{prefix}/nodes/a")]);
}

/// Two runs given one `--prefix-for-test` at once: each writes in its own
/// folder below it, both pass, and an object already there is left alone.
#[test]
fn two_runs_at_once_on_one_prefix_do_not_meet() {
    let Some((bucket, prefix, store)) = s3("diagnose-twice") else {
        return;
    };
    let key = format!("{prefix}/scratch/theirs");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(store.put(&Path::from(key.as_str()), PutPayload::from_static(b"keep")))
        .unwrap();
    let url = format!("s3://{bucket}/{prefix}");
    let runs: Vec<_> = (0..2)
        .map(|_| {
            let url = url.clone();
            std::thread::spawn(move || {
                diagnose(&url, &["--prefix-for-test", "scratch", "--skip-paging"])
            })
        })
        .collect();
    let mut scratch = Vec::new();
    for run in runs {
        let out = run.join().unwrap();
        let shown = text(&out);
        assert_eq!(out.status.code(), Some(0), "{shown}");
        let stdout = String::from_utf8_lossy(&out.stdout);
        let first = stdout.lines().next().unwrap_or_default();
        assert!(first.contains(&format!("{prefix}/scratch/run-")), "{shown}");
        scratch.push(first.to_owned());
    }
    assert_ne!(scratch[0], scratch[1], "each run has its own folder");
    assert_eq!(listed(&store, &prefix), std::slice::from_ref(&key));
    rt.block_on(store.delete(&Path::from(key.as_str())))
        .unwrap();
}

#[test]
fn an_empty_scratch_prefix_is_refused_before_any_request() {
    let out = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .args([
            "cluster",
            "diagnose",
            "--bucket",
            "s3://b/p",
            "--prefix-for-test",
            "/",
        ])
        .env("AWS_REGION", "us-east-1")
        .env("AWS_ENDPOINT", "http://127.0.0.1:9")
        .env("AWS_ALLOW_HTTP", "true")
        .env("AWS_ACCESS_KEY_ID", "test")
        .env("AWS_SECRET_ACCESS_KEY", "test")
        .output()
        .unwrap();
    let shown = text(&out);
    assert_eq!(out.status.code(), Some(2), "{shown}");
    assert!(
        shown.contains("--prefix-for-test must name a folder"),
        "{shown}"
    );
}
