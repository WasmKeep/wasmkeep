//! Restore fences and stale local files (ADR 0033 section 5).
use super::app_delete::{app_node_following, deploy};
use super::deletes::read;
use super::*;
use futures_util::TryStreamExt;
use object_store::{ObjectStoreExt, path::Path as Key};

#[tokio::test(flavor = "multi_thread")]
async fn restore_fence_stops_nodes_and_invalidates_offline_files() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    deploy(&store).await.unwrap();
    let offline = tempfile::tempdir().unwrap();
    let (old, old_follow) = app_node_following("offline", offline.path(), &store)
        .await
        .unwrap();
    assert_eq!(read(old.addr, "absent").await, "0");
    assert_eq!(
        send(old.addr, "POST", "/actor/t/absent/durable", "")
            .await
            .body
            .trim(),
        "1"
    );
    old_follow.abort();
    drop(old);
    let other_dir = tempfile::tempdir().unwrap();
    let other = node_with(
        "other-app",
        other_dir.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    assert_eq!(read(other.addr, "unrelated").await, "0");
    let dir = tempfile::tempdir().unwrap();
    let (node, follows) = app_node_following("restore-node", dir.path(), &store)
        .await
        .unwrap();
    assert_eq!(read(node.addr, "old").await, "0");
    store
        .put(
            &Key::from("apps/default/shop/restoring"),
            b"run".to_vec().into(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), node.service.app_deleted())
        .await
        .expect("restore did not stop node");
    assert_eq!(
        send(node.addr, "GET", "/actor/t/old/durable", "")
            .await
            .status,
        503
    );
    follows.await.unwrap();
    let local = wasmkeep_server::local_app_dir(offline.path(), "default", "shop");
    std::fs::create_dir_all(&local).unwrap();
    std::fs::write(local.join("stale.db"), b"must disappear").unwrap();
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    assert!(app.prepare_local(&local).await.is_err());
    store
        .put(
            &Key::from("apps/default/shop/restored"),
            b"unique-restore".to_vec().into(),
        )
        .await
        .unwrap();
    // The absent actor's bucket history disappears in an in-place restore.
    // The offline node still has its database; it must not resurrect it.
    let keys: Vec<_> = store
        .list(Some(&Key::from("state/default/shop")))
        .try_collect()
        .await
        .unwrap();
    for meta in keys {
        store.delete(&meta.location).await.unwrap();
    }
    store
        .delete(&Key::from("apps/default/shop/restoring"))
        .await
        .unwrap();
    app.prepare_local(&local).await.unwrap();
    assert!(!local.join("stale.db").exists());
    std::fs::write(local.join("keep"), b"same incarnation").unwrap();
    app.prepare_local(&local).await.unwrap();
    assert!(local.join("keep").exists());
    let (restarted, follow) = app_node_following("offline", offline.path(), &store)
        .await
        .unwrap();
    assert_eq!(
        read(restarted.addr, "absent").await,
        "0",
        "stale actor resurrected from disk"
    );
    assert_eq!(
        read(other.addr, "unrelated").await,
        "0",
        "other app was interrupted"
    );
    follow.abort();
}
