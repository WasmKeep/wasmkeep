//! Deploys from the project file (ADR 0026): the version is exactly the
//! file (classes, every rule written, `open` included, values removed when
//! the file has none), the plan is known before the switch, the same file
//! twice is one version, and a flag deploy after it keeps every rule.

use std::collections::BTreeSet;

use wasmkeep_server::apps::{Changes, DeployError, Diff, FrontChange};

use super::front_deploys::front_app_node;
use super::*;

fn vars(text: &str) -> Option<wasmkeep_server::env::Vars> {
    Some(wasmkeep_server::env::parse(text).unwrap())
}

/// What a deploy from a file with classes `c` (`open`) and `d` (default:
/// `internal`), no configuration, no required names and no front asks of
/// the bucket, as `wasmkeep deploy` builds it.
fn file_changes(with_d: bool) -> Changes {
    let mut classes = vec![("c".to_owned(), counter_wasm().to_vec())];
    let mut access = vec![("c".to_owned(), "open".to_owned())];
    let mut config = vec![("c".to_owned(), None)];
    let mut requires = vec![("c".to_owned(), BTreeSet::new())];
    if with_d {
        classes.push(("d".to_owned(), test_actor_wasm().to_vec()));
        access.push(("d".to_owned(), "internal".to_owned()));
        config.push(("d".to_owned(), None));
        requires.push(("d".to_owned(), BTreeSet::new()));
    }
    Changes {
        classes: Some(classes),
        access,
        config,
        requires,
        front: FrontChange::Remove,
        every_rule: true,
        ..Changes::default()
    }
}

async fn status_of(addr: SocketAddr, path: &str, want: u16) {
    let started = std::time::Instant::now();
    loop {
        let r = send_runnable(addr, "GET", path, "").await;
        if r.status == want {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{path} answered {} {:?}, wanted {want}",
            r.status,
            r.body
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_deploy_is_the_whole_version_and_writes_every_rule() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let rt = deploy_runtime();

    // Before the file: flags, with c and d open, configuration and a
    // required name on c.
    let v1 = app
        .deploy_changes(
            &rt,
            Changes {
                classes: Some(vec![
                    ("c".into(), counter_wasm().to_vec()),
                    ("d".into(), test_actor_wasm().to_vec()),
                ]),
                config: vec![("c".into(), vars("MODE=fast"))],
                requires: vec![("c".into(), ["MODE".to_owned()].into())],
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        app.manifest(&v1.current.version)
            .await
            .unwrap()
            .format_number(),
        2
    );

    // The file: what changes is known before anything is written, and the
    // deploy may stop there (nothing uploaded, `current` unchanged).
    let keys_before = bucket_keys(&store).await;
    let mut planned: Option<Diff> = None;
    let e = app
        .deploy_changes_with(&rt, file_changes(true), |diff| {
            planned = Some(diff.clone());
            false
        })
        .await
        .unwrap_err();
    assert!(matches!(e, DeployError::Stopped), "{e}");
    assert_eq!(bucket_keys(&store).await, keys_before);
    assert_eq!(
        app.current().await.unwrap().unwrap().version,
        v1.current.version
    );
    let plan = planned.expect("the plan was given");
    let v2 = app
        .deploy_changes_with(&rt, file_changes(true), |_| true)
        .await
        .unwrap();
    assert_eq!(plan, v2.diff);
    let lines = v2.diff.lines();
    for want in [
        "access d: open -> internal",
        "config c: MODE removed",
        "requires c: MODE no longer required",
    ] {
        assert!(lines.contains(&want.to_owned()), "{want}: {lines:?}");
    }
    // Every class's rule is written, open included: format 3, no front.
    let m2 = app.manifest(&v2.current.version).await.unwrap();
    assert_eq!(m2.format_number(), 3);
    assert!(m2.front.is_none());
    assert_eq!(m2.access.get("c").map(String::as_str), Some("open"));
    assert_eq!(m2.access.get("d").map(String::as_str), Some("internal"));
    assert!(m2.config.is_empty() && m2.requires.is_empty(), "{m2:?}");

    // Nodes serve it: c open, d internal.
    let dir = tempfile::tempdir().unwrap();
    let node = front_app_node("n1", dir.path(), &store, None).await;
    status_of(node.addr, "/actor/d/x", 403).await;
    let r = send_runnable(node.addr, "GET", "/actor/c/x", "").await;
    assert_ne!(r.status, 403, "{:?}", r.body);

    // The same file again: the same version, and no plan to print.
    let mut called = false;
    let v3 = app
        .deploy_changes_with(&rt, file_changes(true), |_| {
            called = true;
            true
        })
        .await
        .unwrap();
    assert_eq!(v3.current.version, v2.current.version);
    assert!(!called, "a no-op has no plan");

    // A flag deploy after the file keeps every rule; the explicit `open`
    // is written as absence (the same rule), as flag deploys write it.
    let v4 = app
        .deploy_changes(
            &rt,
            Changes {
                config: vec![("c".into(), vars("MODE=slow"))],
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    assert!(
        v4.diff.rules.is_empty() && v4.diff.reopened.is_empty(),
        "{:?}",
        v4.diff
    );
    let m4 = app.manifest(&v4.current.version).await.unwrap();
    assert_eq!(m4.access.get("c"), None);
    assert_eq!(m4.access.get("d").map(String::as_str), Some("internal"));

    // The file without d removes it, saying what its rule was.
    let v5 = app.deploy_changes(&rt, file_changes(false)).await.unwrap();
    assert!(
        v5.diff
            .lines()
            .contains(&"class d: removed (it was internal)".to_owned()),
        "{:?}",
        v5.diff.lines()
    );
    assert!(
        v5.diff
            .lines()
            .contains(&"config c: MODE removed".to_owned()),
        "{:?}",
        v5.diff.lines()
    );
    let m5 = app.manifest(&v5.current.version).await.unwrap();
    assert_eq!(m5.classes.keys().collect::<Vec<_>>(), ["c"]);
    assert_eq!(m5.access.get("c").map(String::as_str), Some("open"));
    status_of(node.addr, "/actor/d/x", 404).await;

    // A rollback restores v4 as written; d comes back internal.
    let back = app.rollback(false).await.unwrap();
    assert_eq!(back.current.version, v4.current.version);
    status_of(node.addr, "/actor/d/x", 403).await;
}
