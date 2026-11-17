use super::*;
use crate::assets::{Bundle, Change, Content, File, Index, digest};
use futures_util::TryStreamExt;

fn bundle(body: &[u8]) -> Bundle {
    let hash = digest(body);
    Bundle {
        index: Index {
            files: BTreeMap::from([(
                "a.bin".into(),
                File {
                    content_type: "application/octet-stream".into(),
                    identity: Content {
                        digest: hash.clone(),
                        size: body.len() as u64,
                    },
                    br: None,
                    gzip: None,
                },
            )]),
            ..Index::default()
        },
        contents: BTreeMap::from([(hash, body.to_vec())]),
    }
}
fn changes(body: &[u8]) -> Changes {
    Changes {
        classes: Some(vec![]),
        assets: Change::Set(bundle(body)),
        ..Changes::default()
    }
}
fn runtime() -> Runtime {
    Runtime::new(&wasmkeep_runtime::RuntimeConfig::default()).unwrap()
}
async fn keys(store: &Arc<dyn ObjectStore>) -> Vec<object_store::ObjectMeta> {
    store.list(None).try_collect().await.unwrap()
}
#[tokio::test]
async fn assets_first_unchanged_repair_size_and_verify() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let app = AppRef::new(store.clone(), "", "default", "shop");
    let runtime = runtime();
    let first = app.deploy_changes(&runtime, changes(b"abc")).await.unwrap();
    assert!(!first.unchanged);
    let m = app.manifest(&first.current.version).await.unwrap();
    assert_eq!(m.format_number(), 7);
    let before = keys(&store).await;
    assert!(
        !before
            .iter()
            .any(|m| m.location.as_ref().contains("deploying/"))
    );
    let second = app
        .deploy_changes_with(&runtime, changes(b"abc"), |_| {
            panic!("unchanged asks nothing")
        })
        .await
        .unwrap();
    assert!(second.unchanged);
    assert_eq!(second.repaired, 0);
    assert_eq!(before, keys(&store).await, "no persistent object rewritten");
    let content = app.path(&format!("_assets/content/{}", digest(b"abc")));
    let index = app.path(&format!("_assets/index/{}", m.assets.unwrap()));
    store.delete(&content).await.unwrap();
    store.delete(&index).await.unwrap();
    let repaired = app.deploy_changes(&runtime, changes(b"abc")).await.unwrap();
    assert!(repaired.unchanged);
    assert_eq!(repaired.repaired, 2);
    store
        .put(&content, b"wrong size".to_vec().into())
        .await
        .unwrap();
    assert_eq!(
        app.deploy_changes(&runtime, changes(b"abc"))
            .await
            .unwrap()
            .repaired,
        1
    );
    store.put(&content, b"bad".to_vec().into()).await.unwrap();
    assert_eq!(
        app.deploy_changes(&runtime, changes(b"abc"))
            .await
            .unwrap()
            .repaired,
        0
    );
    let mut verify = changes(b"abc");
    verify.assets_verify = true;
    assert_eq!(
        app.deploy_changes(&runtime, verify).await.unwrap().repaired,
        1
    );
    assert_eq!(
        store
            .get(&content)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"abc"
    );
    let next = app.deploy_changes(&runtime, changes(b"def")).await.unwrap();
    assert_ne!(first.current.version, next.current.version);
    assert_eq!(
        keys(&store)
            .await
            .iter()
            .filter(|m| m.location.as_ref().contains("_assets/content/"))
            .count(),
        2
    );
    assert!(
        app.version(&next.current.version).await.is_ok(),
        "node loads format 7 with its index"
    );
}

#[tokio::test]
async fn assets_kept_missing_refuses_and_marker_is_cleaned() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let app = AppRef::new(store.clone(), "", "default", "shop");
    let runtime = runtime();
    app.deploy_changes(&runtime, changes(b"abc")).await.unwrap();
    store
        .delete(&app.path(&format!("_assets/content/{}", digest(b"abc"))))
        .await
        .unwrap();
    let error = app
        .deploy_changes(&runtime, Changes::default())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("--assets"), "{error}");
    assert!(
        !keys(&store)
            .await
            .iter()
            .any(|m| m.location.as_ref().contains("deploying/"))
    );
}

#[tokio::test]
async fn assets_format_7_needs_assets_and_every_word_below_it() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let app = AppRef::new(store.clone(), "", "default", "shop");
    let runtime = runtime();
    for missing in ["assets", "rate_limits", "flush", "ends", "front"] {
        let words = ["assets", "rate_limits", "flush", "ends", "front"]
            .into_iter()
            .filter(|w| *w != missing)
            .collect::<Vec<_>>()
            .join(" ");
        super::tests::heartbeat(&store, "n", &words).await;
        let error = app
            .deploy_changes(&runtime, changes(b"abc"))
            .await
            .unwrap_err();
        if missing == "assets" {
            assert!(matches!(error, DeployError::NodesWithoutAssets { .. }));
        }
        assert_eq!(keys(&store).await.len(), 1, "refusal writes nothing");
    }
    super::tests::heartbeat(&store, "n", "assets rate_limits flush ends front").await;
    app.deploy_changes(&runtime, changes(b"abc")).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn assets_marker_wait_refresh_kill_and_stale_collection() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let app = AppRef::new(store.clone(), "", "default", "shop");
    let collecting = app.path("_assets/collecting/0123456789abcdef");
    store.put(&collecting, vec![].into()).await.unwrap();
    let task_app = app.clone();
    let task =
        tokio::spawn(async move { task_app.deploy_changes(&runtime(), changes(b"abc")).await });
    let mut attempts = 0;
    let marker = loop {
        attempts += 1;
        assert!(attempts < 1000, "deploy did not write a marker");
        tokio::task::yield_now().await;
        if let Some(meta) = keys(&store)
            .await
            .into_iter()
            .find(|m| m.location.as_ref().contains("deploying/"))
        {
            break meta;
        }
    };
    assert!(!task.is_finished());
    tokio::time::advance(Duration::from_secs(601)).await;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_ne!(
        store.head(&marker.location).await.unwrap().e_tag,
        marker.e_tag,
        "marker refreshed during wait"
    );
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(
        store.head(&marker.location).await.is_ok(),
        "killed deploy leaves marker"
    );
    // A normal deploy waits until a collector releases its marker.
    let task_app = app.clone();
    let task =
        tokio::spawn(async move { task_app.deploy_changes(&runtime(), changes(b"abc")).await });
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    store.delete(&collecting).await.unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    task.await.unwrap().unwrap();
    assert_eq!(
        keys(&store)
            .await
            .iter()
            .filter(|m| m.location.as_ref().contains("deploying/"))
            .count(),
        1,
        "only killed deploy's marker remains"
    );
    // With the bucket timestamp fixed, monotonic elapsed time expires a stale collection.
    store.put(&collecting, vec![].into()).await.unwrap();
    let task = tokio::spawn(async move { app.deploy_changes(&runtime(), changes(b"xyz")).await });
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(901)).await;
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn assets_immutable_refusal_no_assets_and_plan_cancel() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let app = AppRef::new(store.clone(), "", "default", "shop");
    let runtime = runtime();
    let mut b = bundle(b"abc");
    let file = b.index.files.remove("a.bin").unwrap();
    b.index.files.insert("static/a.bin".into(), file);
    b.index.immutable.insert("static/".into());
    let change = |bundle| Changes {
        assets: Change::Set(bundle),
        ..Changes::default()
    };
    let before = keys(&store).await;
    assert!(matches!(
        app.deploy_changes_with(&runtime, change(b.clone()), |_| false)
            .await,
        Err(DeployError::Stopped)
    ));
    assert_eq!(before, keys(&store).await);
    app.deploy_changes(&runtime, change(b.clone()))
        .await
        .unwrap();
    let before = keys(&store).await;
    let mut other = bundle(b"def");
    b.index.files.insert(
        "static/a.bin".into(),
        other.index.files.remove("a.bin").unwrap(),
    );
    b.contents = other.contents;
    let error = app.deploy_changes(&runtime, change(b)).await.unwrap_err();
    assert!(error.to_string().contains("immutable"));
    assert_eq!(before, keys(&store).await);
    let removed = app
        .deploy_changes(
            &runtime,
            Changes {
                assets: Change::Remove,
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    assert!(
        app.manifest(&removed.current.version)
            .await
            .unwrap()
            .assets
            .is_none()
    );
    assert!(
        keys(&store)
            .await
            .iter()
            .any(|m| m.location.as_ref().contains("_assets/content/")),
        "removal does not collect assets in A2"
    );
}

#[tokio::test]
async fn assets_node_loads_format_seven_and_rejects_missing_index() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let app = AppRef::new(store.clone(), "", "default", "shop");
    let first = app
        .deploy_changes(&runtime(), changes(b"abc"))
        .await
        .unwrap();
    assert!(
        app.version(&first.current.version).await.is_ok(),
        "node must load format 7"
    );
    let manifest = app.manifest(&first.current.version).await.unwrap();
    store
        .delete(&app.path(&format!("_assets/index/{}", manifest.assets.unwrap())))
        .await
        .unwrap();
    let error = app
        .version(&first.current.version)
        .await
        .expect_err("missing index");
    assert!(matches!(error, DeployError::Corrupt(_)), "{error}");
    assert!(error.to_string().contains("(missing)"), "{error}");
}

#[tokio::test]
async fn review_unchanged_format_six_skips_reader_checks() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let app = AppRef::new(store.clone(), "", "default", "shop");
    let runtime = runtime();
    let first = app
        .deploy_changes(
            &runtime,
            Changes {
                classes: Some(vec![]),
                limits: Some(BTreeMap::from([(
                    "login".into(),
                    limits::Policy::parse("/login", "ip", None, "1/s", 1).unwrap(),
                )])),
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        app.manifest(&first.current.version)
            .await
            .unwrap()
            .format_number(),
        6
    );
    super::tests::heartbeat(&store, "old", "take front ends flush").await;
    let before = keys(&store).await;
    let next = app
        .deploy_changes_with(&runtime, Changes::default(), |_| {
            panic!("unchanged must not ask")
        })
        .await
        .unwrap();
    assert!(next.unchanged);
    assert_eq!(before, keys(&store).await);
}

#[tokio::test]
async fn review_changed_immutable_deploy_repairs_lost_old_index() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let app = AppRef::new(store.clone(), "", "default", "shop");
    let runtime = runtime();
    let first = app.deploy_changes(&runtime, changes(b"abc")).await.unwrap();
    let hash = app
        .manifest(&first.current.version)
        .await
        .unwrap()
        .assets
        .unwrap();
    store
        .delete(&app.path(&format!("_assets/index/{hash}")))
        .await
        .unwrap();
    let mut b = bundle(b"different");
    b.index.immutable.insert("static/".into());
    let next = app
        .deploy_changes(
            &runtime,
            Changes {
                assets: Change::Set(b),
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    assert!(!next.unchanged);
    assert!(app.version(&next.current.version).await.is_ok());
}
