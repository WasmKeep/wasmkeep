//! `wasmkeep cluster status`, run as a binary against a real S3 API.
//!
//! Skipped unless `WASMKEEP_TEST_S3_BUCKET` names a bucket; endpoint and
//! credentials come from `AWS_*` (see `.github/workflows/ci.yml`). Each run
//! writes under its own prefix. The argument checks need no bucket.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // test code

use std::process::{Command, Output};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use wasmkeep_cluster::lease::{Clock, LeaseConfig};
use wasmkeep_cluster::membership::Membership;
use wasmkeep_cluster::node_lease::NodeLease;
use wasmkeep_cluster::object_store::ObjectStoreBucket;

#[derive(Clone, Copy)]
struct At(u64);
impl Clock for At {
    fn now_ms(&self) -> u64 {
        self.0
    }
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

fn status(url: &str, extra: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .args(["cluster", "status", "--bucket", url])
        .args(extra)
        .output()
        .unwrap()
}

/// Node `id`'s node lease (format 3, ADR 0020) as its process wrote it
/// `age_ms` ago, its fields after the lease lines as the server writes
/// them, with a cluster listener when `tls`.
async fn beat(store: &Arc<dyn ObjectStore>, prefix: &str, id: &str, age_ms: u64, tls: bool) {
    let at = At(now_ms() - age_ms);
    let bucket = ObjectStoreBucket::new(Arc::clone(store), prefix);
    let m = Membership::new(
        bucket.clone(),
        at,
        id,
        format!("http://{id}.internal:8080"),
        9_000,
        1_000,
    )
    .unwrap()
    .with_app("acme/shop");
    let m = if tls {
        m.with_cluster_tls(format!("https://{id}.internal:7443"), vec![[0x5a; 32]])
    } else {
        m
    };
    let mut lease = NodeLease::new(
        bucket,
        at,
        format!("nodes/{id}"),
        format!("{id}-1"),
        LeaseConfig::default_safe(),
    );
    lease.set_payload(m.lease_payload());
    lease.start(None).await.unwrap();
}

#[test]
fn status_reads_nodes_and_deploys_from_the_bucket() {
    let Ok(bucket) = std::env::var("WASMKEEP_TEST_S3_BUCKET") else {
        eprintln!("WASMKEEP_TEST_S3_BUCKET not set; skipping");
        return;
    };
    let prefix = format!("cluster-status-{}-{}", std::process::id(), now_ms());
    let store: Arc<dyn ObjectStore> = Arc::new(
        object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(&bucket)
            .build()
            .unwrap(),
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        // a: TLS, b: still plain (a mixed cluster), c: stopped a minute ago.
        beat(&store, &prefix, "a", 0, true).await;
        beat(&store, &prefix, "b", 0, false).await;
        beat(&store, &prefix, "c", 60_000, true).await;
        store
            .put(
                &Path::from(format!("{prefix}/apps/acme/shop/current")),
                PutPayload::from_static(b"0123456789abcdef 3\n"),
            )
            .await
            .unwrap();
        // Only stale heartbeats here.
        beat(&store, &format!("{prefix}-gone"), "z", 60_000, false).await;
    });
    let url = format!("s3://{bucket}/{prefix}");

    let out = status(&url, &[]);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "{text}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    for want in [
        "http://a.internal:8080",
        "https://a.internal:7443",
        "acme/shop",
        "0123456789abcdef",
        "2 of 3 nodes live",
        "warning: mixed plain and TLS nodes: b still plain while a use",
    ] {
        assert!(text.contains(want), "missing {want:?} in\n{text}");
    }

    let out = status(&url, &["--json"]);
    assert!(out.status.success());
    let j: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let live: Vec<_> = j["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| (n["id"].as_str().unwrap(), n["live"].as_bool().unwrap()))
        .collect();
    assert_eq!(live, [("a", true), ("b", true), ("c", false)]);
    assert!(j["nodes"][2]["age_ms"].as_u64().unwrap() >= 60_000);
    assert_eq!(j["nodes"][0]["tls_pins"][0], "5a".repeat(32));
    assert_eq!(j["apps"][0]["seq"], 3);
    assert_eq!(j["summary"]["mixed_tls"], true);

    // No live node: the report still prints, and the exit code says so.
    let out = status(&format!("{url}-gone"), &["--json"]);
    assert_eq!(out.status.code(), Some(1));
    let j: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(j["summary"]["live"], 0);
    assert!(String::from_utf8_lossy(&out.stderr).contains("no live node"));
    let out = status(&format!("{url}-empty"), &[]);
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stdout).contains("No nodes."));

    // Leave the bucket as it was (every key this test wrote).
    rt.block_on(async {
        for key in [
            format!("{prefix}/nodes/a"),
            format!("{prefix}/nodes/b"),
            format!("{prefix}/nodes/c"),
            format!("{prefix}/apps/acme/shop/current"),
            format!("{prefix}-gone/nodes/z"),
        ] {
            store.delete(&Path::from(key)).await.unwrap();
        }
    });
}

#[test]
fn status_needs_an_s3_url() {
    let out = status("wasmkeep/demo", &[]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("s3://BUCKET"));
    let out = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .args(["cluster", "status"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("--bucket"));
}

fn wasmkeep(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .args(args)
        .output()
        .unwrap()
}

/// `cluster drain`, `undrain` and `actor move` write the keys nodes read
/// (ADR 0014).
#[test]
fn drain_and_move_requests_reach_the_bucket() {
    use wasmkeep_cluster::control;
    let Ok(bucket) = std::env::var("WASMKEEP_TEST_S3_BUCKET") else {
        eprintln!("WASMKEEP_TEST_S3_BUCKET not set; skipping");
        return;
    };
    let prefix = format!("cluster-control-{}-{}", std::process::id(), now_ms());
    let url = format!("s3://{bucket}/{prefix}");
    let store: Arc<dyn ObjectStore> = Arc::new(
        object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(&bucket)
            .build()
            .unwrap(),
    );
    let keys = ObjectStoreBucket::new(Arc::clone(&store), prefix.as_str());
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let ok = |out: Output| {
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    };

    let refused = |out: Output, want: &str| {
        let err = String::from_utf8_lossy(&out.stderr).into_owned();
        assert!(!out.status.success(), "accepted: {want}");
        assert!(err.contains(want) && err.contains("--force"), "{err}");
    };
    let move_to = |target: &str, force: bool| {
        let mut args = vec![
            "actor",
            "move",
            "acme/room/lobby/1",
            "--to",
            target,
            "--bucket",
            &url,
        ];
        if force {
            args.push("--force");
        }
        wasmkeep(&args)
    };
    // No heartbeat names node-b or node-c yet: a typo would sit in
    // control/ until an undrain with the same typo.
    refused(
        wasmkeep(&["cluster", "drain", "node-b", "--bucket", &url]),
        "no live node has the id node-b",
    );
    refused(move_to("node-c", false), "no live node has the id node-c");
    // Nor a drain request: nothing to withdraw.
    refused(
        wasmkeep(&["cluster", "undrain", "node-typo", "--bucket", &url]),
        "neither a live node nor a drain request",
    );
    // A typo forced through can be withdrawn without --force.
    ok(wasmkeep(&[
        "cluster",
        "drain",
        "node-typo",
        "--bucket",
        &url,
        "--force",
    ]));
    ok(wasmkeep(&[
        "cluster",
        "undrain",
        "node-typo",
        "--bucket",
        &url,
    ]));
    // Live nodes: accepted (node-c serves and takes moved actors: no
    // warning; the warnings have unit tests in the binary).
    rt.block_on(async {
        beat(&store, &prefix, "node-b", 0, false).await;
        beat(&store, &prefix, "node-c", 0, false).await;
    });
    ok(wasmkeep(&["cluster", "drain", "node-b", "--bucket", &url]));
    let out = move_to("node-c", false);
    let err = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(out.status.success(), "{err}");
    assert!(!err.contains("warning"), "{err}");
    let read = rt.block_on(control::read(&keys, now_ms())).unwrap();
    assert_eq!(read.draining.iter().collect::<Vec<_>>(), ["node-b"]);
    assert_eq!(read.moves.len(), 1);
    let mv = &read.moves[0];
    assert_eq!(
        (&*mv.tenant, &*mv.class, &*mv.name, &*mv.target),
        ("acme", "room", "lobby/1", "node-c")
    );
    // Written by the bucket's clock, within the last minute.
    assert!(now_ms().abs_diff(mv.written_at_ms) < 60_000);

    ok(wasmkeep(&[
        "cluster", "undrain", "node-b", "--bucket", &url,
    ]));
    let read = rt.block_on(control::read(&keys, now_ms())).unwrap();
    assert!(read.draining.is_empty());

    // Leave the bucket as it was.
    rt.block_on(async {
        for key in [
            mv.key(),
            "nodes/node-b".to_owned(),
            "nodes/node-c".to_owned(),
        ] {
            store
                .delete(&Path::from(format!("{prefix}/{key}")))
                .await
                .unwrap();
        }
    });
}

#[test]
fn bad_drain_and_move_requests_are_refused_before_the_bucket() {
    let out = wasmkeep(&["cluster", "drain", "Node B", "--bucket", "s3://b/x"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("invalid node id"));
    let out = wasmkeep(&[
        "actor",
        "move",
        "acme/room",
        "--to",
        "c",
        "--bucket",
        "s3://b",
    ]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("TENANT/CLASS/NAME"));
    let out = wasmkeep(&[
        "actor",
        "move",
        "ac me/room/x",
        "--to",
        "c",
        "--bucket",
        "s3://b",
    ]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("invalid tenant"));
}

/// Runs `wasmkeep` with `stdin` as its input.
/// A new key reaches the bucket only once every node of the app has it: a
/// node without it cannot open the re-sealed set and answers 503 for the
/// app. `rotate-key` and `reset` say so and refuse until told the nodes
/// have it, before reading the keys or opening the bucket.
#[test]
fn a_key_change_needs_the_nodes_to_have_the_new_key() {
    for args in [
        &[
            "secrets",
            "rotate-key",
            "--bucket",
            "s3://b/x",
            "--app",
            "acme/shop",
            "--key",
            "/nonexistent/old",
            "--new-key",
            "/nonexistent/new",
        ][..],
        &[
            "secrets",
            "rotate-key",
            "--bucket",
            "s3://b/x",
            "--all",
            "--key",
            "/nonexistent/old",
            "--new-key",
            "/nonexistent/new",
        ][..],
        &[
            "secrets",
            "reset",
            "--bucket",
            "s3://b/x",
            "--app",
            "acme/shop",
            "--new-key",
            "/nonexistent/new",
            "--from",
            "/nonexistent/set",
        ][..],
    ] {
        let out = wasmkeep(args);
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(!out.status.success(), "{args:?}");
        assert!(
            err.contains("503") && err.contains("--nodes-have-new-key"),
            "{args:?}: {err}"
        );
    }
}

fn wasmkeep_with_stdin(args: &[&str], stdin: &[u8]) -> Output {
    use std::io::Write as _;
    let mut child = Command::new(env!("CARGO_BIN_EXE_wasmkeep"))
        .args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(stdin).unwrap();
    child.wait_with_output().unwrap()
}

/// `wasmkeep secrets` end to end (ADR 0016): values in from stdin and a
/// file, names listed, never a value printed or stored in the clear, a key
/// change and a reset after a lost key.
#[test]
fn secrets_round_trip_through_the_bucket() {
    let Ok(bucket) = std::env::var("WASMKEEP_TEST_S3_BUCKET") else {
        eprintln!("WASMKEEP_TEST_S3_BUCKET not set; skipping");
        return;
    };
    let prefix = format!("secrets-{}-{}", std::process::id(), now_ms());
    let url = format!("s3://{bucket}/{prefix}");
    let dir = tempfile::tempdir().unwrap();
    let (k1, k2, k3) = (
        dir.path().join("k1"),
        dir.path().join("k2"),
        dir.path().join("k3"),
    );
    let ok = |out: Output| {
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    };
    for k in [&k1, &k2, &k3] {
        ok(wasmkeep(&[
            "secrets",
            "new-key",
            "--out",
            k.to_str().unwrap(),
        ]));
    }
    let base = |key: &std::path::Path| {
        vec![
            "--bucket".to_owned(),
            url.clone(),
            "--app".to_owned(),
            "acme/shop".to_owned(),
            "--key".to_owned(),
            key.to_str().unwrap().to_owned(),
        ]
    };
    let run = |sub: &[&str], key: &std::path::Path, stdin: &[u8]| {
        let mut args: Vec<String> = vec!["secrets".into()];
        args.extend(sub.iter().map(|s| (*s).to_owned()));
        args.extend(base(key));
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        wasmkeep_with_stdin(&args, stdin)
    };

    // A live node of the app that does not read secrets (no line 10):
    // refused without --force.
    let beat_store: Arc<dyn ObjectStore> = Arc::new(
        object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(&bucket)
            .build()
            .unwrap(),
    );
    let beat_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let old_beat = format!(
        "wasmkeep-node 2\nold\nhttp://old:8080\n0.0.0\n{}\nacme/shop\n\n\nserving\n",
        now_ms()
    );
    let beat_key = Path::from(format!("{prefix}/nodes/old"));
    beat_rt
        .block_on(beat_store.put(&beat_key, PutPayload::from(old_beat)))
        .unwrap();
    let refused = run(&["set", "--class", "api", "STRIPE_KEY"], &k1, b"v\n");
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("do not read secrets"));
    ok(run(
        &["set", "--force", "--class", "api", "STRIPE_KEY"],
        &k1,
        b"sk_live_secret_value\n",
    ));
    beat_rt.block_on(beat_store.delete(&beat_key)).unwrap();
    let file = dir.path().join("more");
    std::fs::write(&file, "api PEM 10\nline\nline2\nweb TOKEN 5\ntok3n\n").unwrap();
    ok(run(&["set", "--file", file.to_str().unwrap()], &k1, b""));
    let listed = ok(run(&["list"], &k1, b""));
    assert!(
        listed.contains("api: PEM, STRIPE_KEY") && listed.contains("web: TOKEN"),
        "{listed}"
    );
    for value in ["sk_live_secret_value", "line2", "tok3n"] {
        assert!(!listed.contains(value), "{listed}");
    }
    ok(run(&["unset", "--class", "web", "TOKEN"], &k1, b""));
    assert!(!ok(run(&["list"], &k1, b"")).contains("web"));

    // Two keys, the first seals: a new key first (k2, which no node may
    // have yet) is refused without --nodes-have-new-key; the current key
    // first is fine.
    let two = |first: &std::path::Path, second: &std::path::Path, extra: &[&str]| {
        let mut args: Vec<String> = vec![
            "secrets".into(),
            "unset".into(),
            "--class".into(),
            "web".into(),
            "GONE".into(),
        ];
        args.extend(base(first));
        args.extend(["--key".to_owned(), second.to_str().unwrap().to_owned()]);
        args.extend(extra.iter().map(|s| (*s).to_owned()));
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        wasmkeep_with_stdin(&args, b"")
    };
    let refused = two(&k2, &k1, &[]);
    let err = String::from_utf8_lossy(&refused.stderr).into_owned();
    assert!(!refused.status.success(), "a new key sealed silently");
    assert!(err.contains("--nodes-have-new-key"), "{err}");
    ok(two(&k1, &k2, &[]));
    assert!(
        ok(run(&["list"], &k1, b"")).contains("STRIPE_KEY"),
        "still sealed with k1"
    );

    // Nothing in the bucket holds a value or a name in the clear.
    let store: Arc<dyn ObjectStore> = Arc::new(
        object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(&bucket)
            .build()
            .unwrap(),
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let objects = rt.block_on(async {
        use futures_util::TryStreamExt as _;
        let metas: Vec<_> = store
            .list(Some(&Path::from(prefix.clone())))
            .try_collect()
            .await
            .unwrap();
        let mut all = Vec::new();
        for m in metas {
            let bytes = store.get(&m.location).await.unwrap().bytes().await.unwrap();
            all.push((m.location.to_string(), bytes.to_vec()));
        }
        all
    });
    assert!(!objects.is_empty());
    for (key, bytes) in &objects {
        for needle in ["sk_live_secret_value", "line2", "STRIPE_KEY", "PEM"] {
            assert!(
                !bytes.windows(needle.len()).any(|w| w == needle.as_bytes()),
                "{needle} in {key}"
            );
        }
    }

    // A key change: the new key alone opens afterwards.
    ok(wasmkeep(&[
        "secrets",
        "rotate-key",
        "--bucket",
        &url,
        "--app",
        "acme/shop",
        "--key",
        k1.to_str().unwrap(),
        "--new-key",
        k2.to_str().unwrap(),
        "--nodes-have-new-key",
    ]));
    assert!(!run(&["list"], &k1, b"").status.success());
    assert!(ok(run(&["list"], &k2, b"")).contains("STRIPE_KEY"));

    // The key is lost: a reset writes a whole set under a new one.
    let fresh = dir.path().join("fresh");
    std::fs::write(&fresh, "api STRIPE_KEY 3\nnew\n").unwrap();
    ok(wasmkeep(&[
        "secrets",
        "reset",
        "--bucket",
        &url,
        "--app",
        "acme/shop",
        "--new-key",
        k3.to_str().unwrap(),
        "--from",
        fresh.to_str().unwrap(),
        "--nodes-have-new-key",
    ]));
    let listed = ok(run(&["list"], &k3, b""));
    assert!(
        listed.contains("api: STRIPE_KEY") && !listed.contains("PEM"),
        "{listed}"
    );

    // Leave the bucket as it was.
    rt.block_on(async {
        for (key, _) in &objects {
            let _ = store.delete(&Path::from(key.as_str())).await;
        }
        use futures_util::TryStreamExt as _;
        let left: Vec<_> = store
            .list(Some(&Path::from(prefix.clone())))
            .try_collect()
            .await
            .unwrap();
        for m in left {
            store.delete(&m.location).await.unwrap();
        }
    });
}

#[test]
fn secrets_keys_are_never_overwritten_and_values_never_go_on_the_command_line() {
    let dir = tempfile::tempdir().unwrap();
    let key = dir.path().join("k");
    assert!(
        wasmkeep(&["secrets", "new-key", "--out", key.to_str().unwrap()])
            .status
            .success()
    );
    let again = wasmkeep(&["secrets", "new-key", "--out", key.to_str().unwrap()]);
    assert!(!again.status.success(), "an existing key file is refused");
    // There is no argument for a value: a second positional is refused.
    let out = wasmkeep(&[
        "secrets",
        "set",
        "--bucket",
        "s3://b",
        "--app",
        "a/b",
        "--key",
        key.to_str().unwrap(),
        "--class",
        "api",
        "NAME",
        "the-value",
    ]);
    assert!(!out.status.success());
}
