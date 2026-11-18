//! `/_wasmkeep/ready` means the node can serve: a request sent the moment
//! a fresh node first answers 200 runs, never a 503 to retry.
use super::*;

/// A bucket whose reads of the app's tombstone (`apps/<t>/<a>/deleted`)
/// wait while `slow` is set: the round trips of a real bucket, stretched,
/// between a node's first lease write and its first check that its app
/// was not deleted (ADR 0028, section 5).
#[derive(Debug)]
struct SlowTombstone {
    inner: Arc<dyn ObjectStore>,
    slow: tokio::sync::watch::Receiver<bool>,
}

impl std::fmt::Display for SlowTombstone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlowTombstone({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for SlowTombstone {
    async fn put_opts(
        &self,
        p: &object_store::path::Path,
        b: object_store::PutPayload,
        o: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(p, b, o).await
    }
    async fn put_multipart_opts(
        &self,
        p: &object_store::path::Path,
        o: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(p, o).await
    }
    async fn get_opts(
        &self,
        p: &object_store::path::Path,
        o: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if p.as_ref().starts_with("apps/") && p.as_ref().ends_with("/deleted") {
            let _ = self.slow.clone().wait_for(|slow| !*slow).await;
        }
        self.inner.get_opts(p, o).await
    }
    fn delete_stream(
        &self,
        p: futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.inner.delete_stream(p)
    }
    fn list(
        &self,
        p: Option<&object_store::path::Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(p)
    }
    async fn list_with_delimiter(
        &self,
        p: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(
        &self,
        a: &object_store::path::Path,
        b: &object_store::path::Path,
        o: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(a, b, o).await
    }
}

/// A fresh node following its app, whose first tombstone check is slow
/// (the backup drill's real bucket): it answers ready only once it can
/// acquire an actor, so the first write right after ready runs.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_right_after_ready_on_a_fresh_node_runs() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    AppRef::new(Arc::clone(&inner), "", "default", "shop")
        .deploy(
            &deploy_runtime(),
            vec![("t".into(), test_actor_wasm().to_vec())],
        )
        .await
        .unwrap();
    let (slow, watched) = tokio::sync::watch::channel(true);
    let store: Arc<dyn ObjectStore> = Arc::new(SlowTombstone {
        inner,
        slow: watched,
    });
    let dir = tempfile::tempdir().unwrap();
    let (node, _service) = node_app(
        "fresh",
        dir.path(),
        &store,
        Duration::from_secs(30),
        Duration::from_secs(60),
    )
    .await;
    // The check answers 1.5 s after the node started, well after its
    // first lease write.
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        slow.send_replace(false);
    });
    let started = std::time::Instant::now();
    let mut not_ready = 0;
    loop {
        let (status, _) = get_raw(node.addr, "/_wasmkeep/ready").await;
        if status == 200 {
            break;
        }
        not_ready += 1;
        assert!(started.elapsed() < Duration::from_secs(10), "never ready");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let r = send(node.addr, "POST", "/actor/t/first/durable", "").await;
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "1"),
        "the first request after ready (after {not_ready} not-ready answers)"
    );
    release.await.unwrap();
}
