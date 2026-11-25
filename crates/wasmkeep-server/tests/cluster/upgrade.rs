//! The upgrade of WasmKeep itself, one node at a time (ADR 0043): a node
//! of an older version holds back the finalize and a deploy that needs a
//! capability word it lacks; once it stopped, both are accepted, and no
//! node of an older version starts again.
//!
//! Versions are taken from this build (N, N-1, N+2), so the test follows
//! the workspace version.

use object_store::ObjectStoreExt as _;
use wasmkeep_cluster::app_format::floor;
use wasmkeep_cluster::object_store::ObjectStoreBucket;
use wasmkeep_cluster::upgrade::{self, Finalize, FinalizeRefusal, State};
use wasmkeep_server::apps::{Changes, DeployError, Writes};

use super::*;

/// Live as the CLI counts it: the default timings.
fn horizon() -> u64 {
    wasmkeep_server::heartbeat_horizon_ms(
        wasmkeep_server::DEFAULT_HEARTBEAT,
        &LeaseConfig::default_safe(),
    )
}

/// The node lease of `id` for `default/default`, written directly: a node
/// of `version` that announces `words`, in `state` (`live` or `released`).
/// A live one is dated ahead, so it stays live for the whole test.
async fn lease_of(store: &Arc<dyn ObjectStore>, id: &str, version: &str, state: &str, words: &str) {
    let at = now_ms() + if state == "live" { 60_000 } else { 0 };
    let body = format!(
        "wasmkeep-node 3\n{state} beef\n{at}\n{id}\nhttp://127.0.0.1:9\n{version}\ndefault/default\n\n\nserving\n\n{words}\n"
    );
    store
        .put(
            &format!("nodes/{id}").as_str().into(),
            body.into_bytes().into(),
        )
        .await
        .unwrap();
}

async fn floor_record(store: &Arc<dyn ObjectStore>) -> Option<Vec<u8>> {
    match store.get(&"formats/default/default/floor".into()).await {
        Ok(got) => Some(got.bytes().await.unwrap().to_vec()),
        Err(object_store::Error::NotFound { .. }) => None,
        Err(e) => panic!("{e}"),
    }
}

/// A version with a batched class: manifest format 5, which needs the
/// word `flush` on every live node of the app.
fn batched() -> Changes {
    Changes {
        classes: Some(vec![("c".to_owned(), counter_wasm().to_vec())]),
        writes: vec![("c".into(), Writes::Batched)],
        ..Changes::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_older_node_holds_back_the_finalize_and_a_gated_deploy() {
    let this = upgrade::this_minor();
    let before = this.previous().unwrap();
    let previous = format!("{before}.0");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    // The app as the upgrade before this one left it: finalized at the
    // previous minor version.
    store
        .put(
            &"formats/default/default/floor".into(),
            floor::encode(before).into(),
        )
        .await
        .unwrap();
    let bucket = ObjectStoreBucket::new(Arc::clone(&store), "");
    let app = AppRef::new(Arc::clone(&store), "", "default", "default");
    let rt = deploy_runtime();
    let status = async || {
        upgrade::read_status(&bucket, "default", "default", now_ms(), horizon())
            .await
            .unwrap()
    };
    let finalize = async || {
        upgrade::finalize_write(&bucket, "default", "default", this, now_ms(), horizon()).await
    };
    let confirm = async || {
        upgrade::finalize_confirm(&bucket, "default", "default", this, now_ms(), horizon())
            .await
            .unwrap()
    };

    // A node of the previous minor version, without `flush`, and a node
    // of this build beside it: the versions are next to each other, so
    // the new node starts.
    lease_of(&store, "old", &previous, "live", "take front ends").await;
    let dir = tempfile::tempdir().unwrap();
    let a = node("a", dir.path(), &store, Duration::from_secs(60)).await;
    a.service.node_id_checked().await.unwrap();
    assert_eq!(floor_record(&store).await, Some(floor::encode(before)));
    let seen = status().await;
    assert_eq!(seen.state, State::Mixed, "{}", seen.detail);
    assert_eq!(seen.blocks, ["old"]);
    let versions: Vec<(&str, bool)> = seen
        .nodes
        .iter()
        .map(|n| (n.id.as_str(), n.minor == Some(this)))
        .collect();
    assert_eq!(versions, [("a", true), ("old", false)]);

    // The finalize is refused, naming the older node; nothing is written.
    let refused = finalize().await.unwrap_err();
    assert_eq!(
        refused,
        FinalizeRefusal::OlderLive {
            target: this,
            nodes: format!("old ({previous})"),
        }
    );
    assert_eq!(floor_record(&store).await, Some(floor::encode(before)));
    // So is a deploy that needs a word the older node does not announce.
    let e = app.deploy_changes(&rt, batched()).await.unwrap_err();
    assert!(
        matches!(&e, DeployError::NodesWithoutFlush { nodes, .. } if nodes == "old"),
        "{e}"
    );
    assert!(app.current().await.unwrap().is_none(), "nothing deployed");

    // The older node stops: the finalize is accepted and writes the
    // floor, exactly as 0.8.1 reads it.
    lease_of(&store, "old", &previous, "released", "take front ends").await;
    assert_eq!(finalize().await, Ok(Finalize::Raise(Some(before), this)));
    assert_eq!(floor_record(&store).await, Some(floor::encode(this)));
    let listed = confirm().await;
    assert!(listed.late.is_empty(), "{:?}", listed.late);
    let stopped: Vec<&str> = listed.stopped_older.iter().map(|n| n.id.as_str()).collect();
    assert_eq!(stopped, ["old"], "named, and not an error");
    assert_eq!(status().await.state, State::OneVersion);
    assert_eq!(finalize().await, Ok(Finalize::Already(this)));

    // An older node that was starting while the floor was written and
    // read it before the write (its lease written directly: section 8,
    // phase 5): the second listing reports it, the floor stays, and the
    // gated deploy stays refused while it is live.
    lease_of(&store, "late", &previous, "live", "take front ends").await;
    let listed = confirm().await;
    let late: Vec<&str> = listed.late.iter().map(|n| n.id.as_str()).collect();
    assert_eq!(late, ["late"]);
    assert_eq!(status().await.state, State::BelowFloor);
    assert_eq!(floor_record(&store).await, Some(floor::encode(this)));
    let e = app.deploy_changes(&rt, batched()).await.unwrap_err();
    assert!(
        matches!(&e, DeployError::NodesWithoutFlush { nodes, .. } if nodes == "late"),
        "{e}"
    );
    // Stopped: the deploy is accepted, in the gated format.
    lease_of(&store, "late", &previous, "released", "take front ends").await;
    assert!(confirm().await.late.is_empty());
    let done = app.deploy_changes(&rt, batched()).await.unwrap();
    assert_eq!(
        app.manifest(&done.current.version)
            .await
            .unwrap()
            .format_number(),
        5
    );

    // A node of the previous version that starts now stops at the floor,
    // before any lease write.
    let old_dir = tempfile::tempdir().unwrap();
    let b = node_configured(
        "b",
        old_dir.path(),
        &store,
        Duration::from_secs(60),
        &["counter"],
        counter_wasm(),
        Duration::from_secs(30),
        "",
        |config| config.cluster.as_mut().unwrap().version = Some(previous.clone()),
    )
    .await;
    let why = b.service.node_id_checked().await.unwrap_err();
    assert!(
        why.contains(&format!("finalized at {this}")) && why.contains("older than the floor"),
        "{why}"
    );
    assert!(
        matches!(
            store.head(&"nodes/b".into()).await,
            Err(object_store::Error::NotFound { .. })
        ),
        "no lease written"
    );

    // A node two minor versions ahead refuses to start beside this one,
    // naming it and its version.
    let far = format!(
        "{}.0",
        wasmkeep_cluster::app_format::floor::Minor {
            minor: this.minor + 2,
            ..this
        }
    );
    let far_dir = tempfile::tempdir().unwrap();
    let c = node_configured(
        "c",
        far_dir.path(),
        &store,
        Duration::from_secs(60),
        &["counter"],
        counter_wasm(),
        Duration::from_secs(30),
        "",
        |config| config.cluster.as_mut().unwrap().version = Some(far.clone()),
    )
    .await;
    let why = c.service.node_id_checked().await.unwrap_err();
    assert!(
        why.contains("node `a` runs") && why.contains("Upgrade one minor version at a time"),
        "{why}"
    );
    assert!(
        matches!(
            store.head(&"nodes/c".into()).await,
            Err(object_store::Error::NotFound { .. })
        ),
        "no lease written"
    );
    // The node that was there keeps serving.
    assert_eq!(
        send(a.addr, "POST", "/actor/counter/x", "").await.status,
        200
    );
}
