//! A class's mode of writes and the deploy's import check (ADR 0034,
//! sections 1 and 15). Batched writes are not built yet: a deploy refuses
//! them. A strict class, said or not, is written exactly as before; a
//! component that imports `storage.flush` (or `storage.delete-self-durable`)
//! is refused while a live node of the app lacks it.

use object_store::ObjectStoreExt as _;
use wasmkeep_server::apps::{Changes, DeployError, Writes};

use super::*;

/// The version's manifest as the bucket holds it.
async fn manifest_text(store: &Arc<dyn ObjectStore>, app: &str, version: &str) -> String {
    let key = format!("apps/default/{app}/{version}/manifest");
    let got = store.get(&key.as_str().into()).await.unwrap();
    String::from_utf8(got.bytes().await.unwrap().to_vec()).unwrap()
}

/// A live heartbeat of node `id` serving `default/shop`, with these words.
async fn node_with_words(store: &Arc<dyn ObjectStore>, id: &str, words: &str) {
    let body = format!(
        "wasmkeep-node 2\n{id}\nhttp://10.0.0.9:8080\n0.4.2\n{}\ndefault/shop\n\n\nserving\nsecrets\n{words}\n",
        now_ms()
    );
    store
        .put(
            &format!("nodes/{id}").as_str().into(),
            body.into_bytes().into(),
        )
        .await
        .unwrap();
}

fn classes(wasm: &[u8]) -> Option<Vec<(String, Vec<u8>)>> {
    Some(vec![("c".to_owned(), wasm.to_vec())])
}

/// C7: `writes = "strict"`, said or not, writes the manifest of 0.4.2:
/// the same format, bytes and version id.
#[tokio::test(flavor = "multi_thread")]
async fn a_strict_class_keeps_the_manifest_bytes() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let rt = deploy_runtime();
    let plain = AppRef::new(Arc::clone(&store), "", "default", "plain");
    let said = AppRef::new(Arc::clone(&store), "", "default", "said");
    let a = plain
        .deploy_changes(
            &rt,
            Changes {
                classes: classes(counter_wasm()),
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    let b = said
        .deploy_changes(
            &rt,
            Changes {
                classes: classes(counter_wasm()),
                writes: vec![("c".into(), Writes::Strict)],
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(a.current.version, b.current.version, "the same version id");
    let text = manifest_text(&store, "said", &b.current.version).await;
    assert_eq!(
        text,
        manifest_text(&store, "plain", &a.current.version).await
    );
    let digest = {
        use sha2::{Digest, Sha256};
        Sha256::digest(counter_wasm())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    };
    assert_eq!(text, format!("wasmkeep-manifest 1\nclass c {digest}\n"));
    // Said again on the next deploy: nothing changes.
    let again = said
        .deploy_changes(
            &rt,
            Changes {
                writes: vec![("c".into(), Writes::Strict)],
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    assert!(again.unchanged);
}

/// Batched writes are not built yet: refused before anything is written,
/// naming the class.
#[tokio::test(flavor = "multi_thread")]
async fn a_batched_class_is_refused_for_now() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let rt = deploy_runtime();
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    let e = app
        .deploy_changes(
            &rt,
            Changes {
                classes: classes(counter_wasm()),
                writes: vec![("c".into(), Writes::Batched)],
                ..Changes::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&e, DeployError::BatchedNotYet(c) if c == "c"),
        "{e}"
    );
    assert!(e.to_string().contains("not yet available"), "{e}");
    assert!(bucket_keys(&store).await.is_empty(), "nothing written");
    // A mode for a class the version does not have is a mistake.
    let e = app
        .deploy_changes(
            &rt,
            Changes {
                classes: classes(counter_wasm()),
                writes: vec![("other".into(), Writes::Strict)],
                ..Changes::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(e, DeployError::Invalid(..)), "{e}");
}

/// A component importing `storage.flush` needs every live node of the app
/// to announce `flush`; one importing `storage.delete-self-durable` needs
/// `ends`, for a strict class too (ADR 0034, section 1, exception 2), in
/// the components a deploy gives, keeps or a rollback brings back. The
/// refusal names the nodes and the classes, and writes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_component_a_live_node_cannot_link_is_refused() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let rt = deploy_runtime();
    let app = AppRef::new(Arc::clone(&store), "", "default", "shop");
    // test-actor calls both `flush` and `delete-self-durable`, so a Rust
    // build imports both; the counter calls neither.
    node_with_words(&store, "new", "take front handoff ends flush").await;
    node_with_words(&store, "old", "take front handoff ends").await;
    let before = bucket_keys(&store).await;
    let e = app
        .deploy_changes(
            &rt,
            Changes {
                classes: classes(test_actor_wasm()),
                ..Changes::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&e, DeployError::NodesWithoutFlush { nodes, .. } if nodes == "old"),
        "{e}"
    );
    let message = e.to_string();
    for want in [
        "storage.flush",
        "class c imports",
        "upgrade every node",
        "no --force",
    ] {
        assert!(message.contains(want), "{want}: {message}");
    }
    assert_eq!(bucket_keys(&store).await, before, "nothing written");

    // A component that imports neither deploys beside the old node.
    let v1 = app
        .deploy_changes(
            &rt,
            Changes {
                classes: classes(counter_wasm()),
                ..Changes::default()
            },
        )
        .await
        .unwrap();

    // `ends` is checked on its own.
    node_with_words(&store, "old", "take front handoff flush").await;
    let e = app
        .deploy_changes(
            &rt,
            Changes {
                classes: classes(test_actor_wasm()),
                ..Changes::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&e, DeployError::NodesWithoutDeleteDurable { nodes, .. } if nodes == "old"),
        "{e}"
    );
    assert!(e.to_string().contains("storage.delete-self-durable"), "{e}");
    assert_eq!(app.current().await.unwrap().unwrap(), v1.current);

    // Upgraded: the deploy goes through.
    node_with_words(&store, "old", "take front handoff ends flush").await;
    app.deploy_changes(
        &rt,
        Changes {
            classes: classes(test_actor_wasm()),
            ..Changes::default()
        },
    )
    .await
    .unwrap();

    // A node without `flush` again (one started from an older binary): a
    // deploy that keeps the class reads its component from the bucket,
    // and a rollback the version it brings back.
    node_with_words(&store, "old", "take front handoff ends").await;
    let e = app
        .deploy_changes(
            &rt,
            Changes {
                access: vec![("c".into(), "internal".into())],
                ..Changes::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(e, DeployError::NodesWithoutFlush { .. }), "{e}");
    let v3 = app
        .deploy_changes(
            &rt,
            Changes {
                classes: classes(counter_wasm()),
                ..Changes::default()
            },
        )
        .await
        .unwrap();
    let e = app.rollback(false).await.unwrap_err();
    assert!(
        matches!(&e, DeployError::NodesWithoutFlush { nodes, .. } if nodes == "old"),
        "{e}"
    );
    assert_eq!(app.current().await.unwrap().unwrap(), v3.current);
}
