//! A4/A5: real HTTP requests against an app following format 7 deploys.
use super::*;
use object_store::ObjectStoreExt;
use std::collections::BTreeMap;
use wasmkeep_server::apps::Changes;
use wasmkeep_server::assets::{Bundle, Change, Content, File, Index, digest};

fn changes(body: &[u8]) -> Changes {
    let hash = digest(body);
    Changes {
        classes: Some(vec![]),
        assets: Change::Set(Bundle {
            index: Index {
                files: BTreeMap::from([(
                    "index.html".into(),
                    File {
                        content_type: "text/html;charset=utf-8".into(),
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
        }),
        ..Changes::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assets_http_switch_missing_index_repair_and_rollback() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let app_ref = AppRef::new(store.clone(), "", "default", "default");
    let runtime = Runtime::new(&RuntimeConfig::default()).unwrap();
    let first = app_ref
        .deploy_changes(&runtime, changes(b"first"))
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    app_ref
        .prepare_local(&wasmkeep_server::local_app_dir(
            dir.path(),
            "default",
            "default",
        ))
        .await
        .unwrap();
    let mut config = ActorConfig::new(dir.path());
    config.assets = app_ref
        .version(&first.current.version)
        .await
        .unwrap()
        .assets;
    config.deploy_seq = first.current.seq;
    let service = ActorService::new(runtime.clone(), config, vec![]).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let app = wasmkeep_server::App {
        front: Some(wasmkeep_server::HttpComponent::deployed(&service)),
        actors: Some(service.clone()),
    };
    let server = tokio::spawn(wasmkeep_server::serve(listener, app, async {
        let _ = stopped.await;
    }));
    assert_eq!(send(addr, "GET", "/", "").await.body, "first");
    let head = send(addr, "HEAD", "/index.html", "").await;
    assert_eq!(head.status, 200);
    assert_eq!(head.header("content-length").as_deref(), Some("5"));
    assert!(head.body.is_empty());
    for (path, status) in [
        ("/%61ctor/c/x", 400),
        ("/%5fwasmkeep/ready", 400),
        ("/actor/c/x", 404),
    ] {
        assert_eq!(send(addr, "GET", path, "").await.status, status);
    }
    let tag = format!("\"{}\"", &digest(b"first")[..32]);
    assert_eq!(
        send(addr, "GET", "/", &format!("If-None-Match: W/{tag}\r\n"))
            .await
            .status,
        304
    );
    let partial = send(addr, "GET", "/", "Range: bytes=1-3\r\n").await;
    assert_eq!((partial.status, partial.body.as_str()), (206, "irs"));
    assert_eq!(
        send(addr, "GET", "/", "Range: bytes=99-\r\n").await.status,
        416
    );

    let second = app_ref
        .deploy_changes(&runtime, changes(b"second"))
        .await
        .unwrap();
    let manifest = app_ref.manifest(&second.current.version).await.unwrap();
    let key = format!(
        "apps/default/default/_assets/index/{}",
        manifest.assets.unwrap()
    );
    store.delete(&key.into()).await.unwrap();
    let follower = tokio::spawn(
        app_ref
            .clone()
            .follow(service.clone(), Duration::from_millis(20)),
    );
    wait_status(addr, 503).await;
    assert_eq!(
        service.deploy_seq(),
        first.current.seq,
        "missing index must not publish partial version"
    );
    let repaired = app_ref
        .deploy_changes(&runtime, changes(b"second"))
        .await
        .unwrap();
    assert!(repaired.unchanged);
    assert_eq!(repaired.repaired, 1);
    wait_status(addr, 200).await;
    assert_eq!(send(addr, "GET", "/", "").await.body, "second");
    app_ref.rollback(false).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while send(addr, "GET", "/", "").await.body != "first" {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    follower.abort();
    let _ = stop.send(());
    server.await.unwrap().unwrap();
}
async fn wait_status(addr: SocketAddr, status: u16) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while send(addr, "GET", "/_wasmkeep/ready", "").await.status != status {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}
