//! What this version keeps of the layout before 0.4.0 (ADR 0022), against
//! real nodes. `cluster migrate` was removed in 0.9.0 (a cluster older
//! than 0.4.0 upgrades through 0.8.x first), so a bucket migrated by 0.8.x
//! is made here by hand, as that command left it: the state under the
//! app's root, a tombstone over each bare ownership record, the tenant's
//! commit. A tenant still in the old layout, or a migration a 0.8.x
//! binary holds, is refused.

use super::*;
use wasmkeep_cluster::legacy_layout::{
    COMMIT_PREFIX, LOCK_KEY, StartRefusal, TOMBSTONE_HEADER, start_check,
};
use wasmkeep_cluster::object_store::ObjectStoreBucket;
use wasmkeep_cluster::store::Store;

fn keys(store: &Arc<dyn ObjectStore>) -> ObjectStoreBucket {
    ObjectStoreBucket::new(Arc::clone(store), "")
}

/// A node's local databases moved back to `<data>/actors/<tenant>`, as a
/// node before 0.4.0 kept them.
fn local_to_bare(data: &Path) {
    let new = data.join("state/default/default/actors");
    if new.exists() {
        std::fs::create_dir_all(data.join("actors")).unwrap();
        std::fs::rename(new, data.join("actors/default")).unwrap();
        std::fs::remove_dir_all(data.join("state")).unwrap();
    }
}

/// A bucket a 0.8.x `cluster migrate` committed, before `--remove-old`
/// (old history and lease markers beside their copies, a tombstone over
/// each old record, one `unowned`): the tombstones are read as
/// tombstones, never as records, the commit lets the tenant start over
/// all of it, and a node of this version serves every actor from its
/// root (its old local databases go aside).
/// Bare keys without the commit refuse the tenant, naming 0.8.1, and
/// refuse a deploy too.
#[tokio::test(flavor = "multi_thread")]
async fn a_bucket_migrated_by_0_8_is_served_and_an_unmigrated_one_refused() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let data = tempfile::tempdir().unwrap();
    let names = ["x", "Room 1 é"];
    {
        let a = node_with(
            "a",
            data.path(),
            &store,
            Duration::from_secs(60),
            "t",
            test_actor_wasm(),
        )
        .await;
        settle().await;
        for name in names {
            let path = format!("/actor/t/{}/durable", url_encode(name));
            for want in ["1", "2"] {
                let r = send_runnable(a.addr, "POST", &path, "").await;
                assert_eq!((r.status, r.body.trim()), (200, want), "{name}");
            }
        }
        // Every write uploaded before the node goes.
        tokio::time::sleep(Duration::from_millis(500)).await;
        a.service.shutdown(Duration::from_secs(5)).await;
        stop_listening(a).await;
    }
    local_to_bare(data.path());
    let b = keys(&store);
    assert!(!start_check(&b, "default").await.unwrap());

    // As the migration left it: a tombstone over each bare record.
    let rooted = "state/default/default/owners/";
    let records = b.list(&format!("{rooted}t/")).await.unwrap();
    assert_eq!(records.len(), names.len());
    for meta in &records {
        let bare = meta.key.replacen(rooted, "owners/default/", 1);
        let body = format!("{TOMBSTONE_HEADER}\n{}\n", meta.key).into_bytes();
        let (read, _) = wasmkeep_cluster::lease::Bucket::get(&b, &meta.key)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            wasmkeep_cluster::node_lease::OwnerObject::decode(&read),
            Some(wasmkeep_cluster::node_lease::OwnerObject::Record(_))
        ));
        match wasmkeep_cluster::node_lease::OwnerObject::decode(&body) {
            Some(wasmkeep_cluster::node_lease::OwnerObject::Tombstone { to, unowned }) => {
                assert_eq!((to.as_str(), unowned), (meta.key.as_str(), false));
            }
            other => panic!("{other:?}"),
        }
        b.put(&bare, body).await.unwrap();
    }
    // An `unowned` tombstone, where an actor had history and no record.
    let ghost = format!("{rooted}t/ghost");
    let body = format!("{TOMBSTONE_HEADER}\n{ghost}\nnone\n").into_bytes();
    match wasmkeep_cluster::node_lease::OwnerObject::decode(&body) {
        Some(wasmkeep_cluster::node_lease::OwnerObject::Tombstone { to, unowned }) => {
            assert_eq!((to.as_str(), unowned), (ghost.as_str(), true));
        }
        other => panic!("{other:?}"),
    }
    b.put("owners/default/t/ghost", body).await.unwrap();
    // A bucket before `--remove-old`: the old history and lease markers
    // are still there, beside their copies under the root.
    let mut copies = 0;
    for (family, bare) in [("actors", "actors"), ("leases", "leases")] {
        let root = format!("state/default/default/{family}/");
        for meta in b.list(&root).await.unwrap() {
            let to = meta.key.replacen(&root, &format!("{bare}/default/"), 1);
            b.copy(&meta.key, &to).await.unwrap();
            copies += 1;
        }
    }
    assert!(copies > names.len(), "{copies}");
    // Before the commit: refused, naming the version that migrates.
    match start_check(&b, "default").await {
        Err(StartRefusal::Unmigrated(tenant, found)) => {
            assert_eq!(tenant, "default");
            assert!(found.starts_with("actors/default/"), "{found}");
        }
        other => panic!("{other:?}"),
    }
    let e = AppRef::new(Arc::clone(&store), "", "default", "shop")
        .deploy(
            &deploy_runtime(),
            vec![("c".into(), counter_wasm().to_vec())],
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("0.8.1") && e.contains("cluster migrate"), "{e}");

    b.put(
        &format!("{COMMIT_PREFIX}default"),
        b"wasmkeep-migrated 1\n".to_vec(),
    )
    .await
    .unwrap();
    assert!(start_check(&b, "default").await.unwrap());
    // As `serve` sets it from its start check: the tenant was migrated.
    let c = node_configured(
        "c",
        data.path(),
        &store,
        Duration::from_secs(60),
        &["t"],
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        |config| config.cluster.as_mut().unwrap().migrated = true,
    )
    .await;
    settle().await;
    assert!(data.path().join("actors.pre-0.4/default/t").is_dir());
    for name in names {
        let path = format!("/actor/t/{}/durable", url_encode(name));
        let r = send_runnable(c.addr, "GET", &path, "").await;
        assert_eq!((r.status, r.body.trim()), (200, "2"), "{name}");
        let r = send_runnable(c.addr, "POST", &path, "").await;
        assert_eq!((r.status, r.body.trim()), (200, "3"), "{name}");
    }
    // The tombstones and the old copies stay: nothing here removes them.
    assert_eq!(
        b.list("owners/default/t/").await.unwrap().len(),
        names.len() + 1
    );
    assert!(!b.list("actors/default/t/").await.unwrap().is_empty());
    assert!(!b.list("leases/default/t/").await.unwrap().is_empty());
    drop(c);
}

#[tokio::test(flavor = "multi_thread")]
async fn deploys_and_starts_are_refused_while_a_migration_holds_the_prefix() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let b = keys(&store);
    Store::put(&b, LOCK_KEY, b"wasmkeep-migration 1\nmode apply\n".to_vec())
        .await
        .unwrap();
    assert_eq!(
        start_check(&b, "default").await,
        Err(StartRefusal::InProgress)
    );
    let e = AppRef::new(Arc::clone(&store), "", "acme", "shop")
        .deploy(
            &deploy_runtime(),
            vec![("c".into(), counter_wasm().to_vec())],
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("in progress"), "{e}");
    assert!(
        !bucket_keys(&store)
            .await
            .iter()
            .any(|k| k.starts_with("apps/")),
        "the refused deploy wrote"
    );
}

/// A node joining a bucket with no state of its tenant, with databases of
/// the layout before 0.4.0 from a single-node run: they are the only copy,
/// so they move into its app's root (never aside), and it serves them.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_joining_with_single_node_data_keeps_it() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let data = tempfile::tempdir().unwrap();
    {
        // Written alone, then laid out as a node before 0.4.0 kept it.
        let alone = tempfile::tempdir().unwrap();
        let runtime = Runtime::new(&RuntimeConfig::default()).unwrap();
        let component = runtime.compile(test_actor_wasm()).unwrap();
        let service = ActorService::new(
            runtime,
            ActorConfig::new(alone.path()),
            vec![("t".into(), component, test_actor_wasm().to_vec())],
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel::<()>();
        spawn_serve(listener, None, service.clone(), async {
            let _ = stopped.await;
        });
        for want in ["1", "2"] {
            let r = send(addr, "POST", "/actor/t/kept/durable", "").await;
            assert_eq!((r.status, r.body.trim()), (200, want));
        }
        service.shutdown(Duration::from_secs(5)).await;
        drop((stop, service));
        local_to_bare(alone.path());
        std::fs::rename(alone.path().join("actors"), data.path().join("actors")).unwrap();
    }
    let b = keys(&store);
    assert!(!start_check(&b, "default").await.unwrap());
    let a = node_with(
        "a",
        data.path(),
        &store,
        Duration::from_secs(60),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;
    assert!(!data.path().join("actors.pre-0.4").exists());
    assert!(data.path().join(test_dir("t")).is_dir());
    let r = send_runnable(a.addr, "GET", "/actor/t/kept/durable", "").await;
    assert_eq!((r.status, r.body.trim()), (200, "2"), "{}", r.body);
}
