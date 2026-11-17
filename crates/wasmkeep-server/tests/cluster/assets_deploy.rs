//! Compatibility proof: deploy still refuses a live node without assets.
use super::*;
use wasmkeep_server::apps::{Changes, DeployError};
use wasmkeep_server::assets::{Bundle, Change, Content, File, Index, digest};

#[tokio::test]
async fn assets_deploy_is_refused_by_a_live_pre_serving_node() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app = AppRef::new(store.clone(), "", "default", "shop");
    let heartbeat = format!(
        "wasmkeep-node 2\nold\nhttp://127.0.0.1:8080\n0.6.0\n{}\ndefault/shop\n\n\nserving\nsecrets\nfront ends flush rate_limits\n",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    use object_store::ObjectStoreExt;
    store
        .put(&"nodes/old".into(), heartbeat.into_bytes().into())
        .await
        .unwrap();
    let hash = digest(b"site");
    let bundle = Bundle {
        index: Index {
            files: std::collections::BTreeMap::from([(
                "index.html".into(),
                File {
                    content_type: "text/html;charset=utf-8".into(),
                    identity: Content {
                        digest: hash.clone(),
                        size: 4,
                    },
                    br: None,
                    gzip: None,
                },
            )]),
            ..Index::default()
        },
        contents: std::collections::BTreeMap::from([(hash, b"site".to_vec())]),
    };
    let runtime = Runtime::new(&RuntimeConfig::default()).unwrap();
    let result = app
        .deploy_changes(
            &runtime,
            Changes {
                assets: Change::Set(bundle),
                ..Changes::default()
            },
        )
        .await;
    assert!(
        matches!(result, Err(DeployError::NodesWithoutAssets { .. })),
        "{result:?}"
    );
    assert_eq!(bucket_keys(&store).await, vec!["nodes/old"]);
}
