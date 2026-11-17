//! ADR 0035 on real nodes: an app that enabled the recorded-head format,
//! a bucket whose listings hide history written while `lag` is on. A
//! hand-over by hibernation records the head; the next node refuses (503,
//! `Retry-After`) rather than serve an older head, refuses again at once
//! during the cooldown without listing or acquiring, and serves the
//! acknowledged data once the history is visible.

use super::*;

use object_store::path::Path as StorePath;
use object_store::{ObjectStoreExt as _, PutPayload};

/// A bucket whose listings leave out history keys written while `lag` is
/// on, until `reveal`; counts history listings.
#[derive(Debug)]
struct Lagging {
    inner: Arc<dyn ObjectStore>,
    lag: AtomicBool,
    hidden: std::sync::Mutex<std::collections::BTreeSet<String>>,
    lists: std::sync::atomic::AtomicU64,
}

impl std::fmt::Display for Lagging {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Lagging")
    }
}

#[async_trait::async_trait]
impl ObjectStore for Lagging {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if self.lag.load(Ordering::SeqCst) && location.as_ref().contains("/actors/") {
            self.hidden
                .lock()
                .unwrap()
                .insert(location.as_ref().to_owned());
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &StorePath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, object_store::Result<StorePath>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<StorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        use futures_util::StreamExt as _;
        if prefix.is_some_and(|p| p.as_ref().contains("/actors/")) {
            self.lists.fetch_add(1, Ordering::SeqCst);
        }
        let hidden = self.hidden.lock().unwrap().clone();
        self.inner
            .list(prefix)
            .filter(move |m| {
                std::future::ready(
                    m.as_ref()
                        .map_or(true, |m| !hidden.contains(m.location.as_ref())),
                )
            })
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// The record's generation and recorded head line (`None`: version 1).
async fn record(store: &Arc<dyn ObjectStore>, name: &str) -> (u64, Option<String>) {
    let key = StorePath::from(test_key(Family::Owners, &format!("t/{name}")));
    let body = String::from_utf8(
        store
            .get(&key)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    let lines: Vec<&str> = body.lines().collect();
    let generation = lines[2].parse().unwrap();
    let head = (lines[0] == "wasmkeep-owner 2").then(|| lines[3].to_owned());
    (generation, head)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lagging_listing_refuses_then_serves_the_recorded_head() {
    let lagging = Arc::new(Lagging {
        inner: Arc::new(InMemory::new()),
        lag: AtomicBool::new(false),
        hidden: std::sync::Mutex::default(),
        lists: std::sync::atomic::AtomicU64::new(0),
    });
    let store: Arc<dyn ObjectStore> = Arc::clone(&lagging) as Arc<dyn ObjectStore>;
    store
        .put(
            &StorePath::from(wasmkeep_cluster::app_format::key("", "default", "default")),
            wasmkeep_cluster::app_format::ENABLED_BODY.to_vec().into(),
        )
        .await
        .unwrap();
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    // a hibernates its idle actor after a second, releasing the record.
    let a = node_with(
        "a",
        da.path(),
        &store,
        Duration::from_secs(1),
        "t",
        test_actor_wasm(),
    )
    .await;
    let b = node_with(
        "b",
        db.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;
    let path = "/actor/t/x/durable";
    let r = send(a.addr, "POST", path, "").await;
    assert_eq!((r.status, r.body.trim()), (200, "1"), "{}", r.body);
    // The second write's segment stays out of listings.
    lagging.lag.store(true, Ordering::SeqCst);
    let r = send(a.addr, "POST", path, "").await;
    assert_eq!((r.status, r.body.trim()), (200, "2"), "{}", r.body);
    // Hibernation hands it over: the release records the head.
    let started = std::time::Instant::now();
    loop {
        if lease_holder_any(&store, "x").await.is_none() {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "never released"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (released_at, head) = record(&store, "x").await;
    assert!(
        head.as_deref().is_some_and(|h| h.starts_with("head ")),
        "{head:?}"
    );

    // b's listing misses the second write: refused, not served at "1".
    let r = send(b.addr, "GET", path, "").await;
    assert_eq!(r.status, 503, "{}", r.body);
    assert!(r.retry_after.is_some(), "{}", r.head);
    // At once again: the cooldown refuses without listing or acquiring.
    let (lists, generation) = (
        lagging.lists.load(Ordering::SeqCst),
        record(&store, "x").await.0,
    );
    let asked = std::time::Instant::now();
    let r = send(b.addr, "GET", path, "").await;
    assert_eq!(r.status, 503, "{}", r.body);
    assert!(
        asked.elapsed() < Duration::from_secs(2),
        "{:?}",
        asked.elapsed()
    );
    assert_eq!(
        lagging.lists.load(Ordering::SeqCst),
        lists,
        "listed during the cooldown"
    );
    assert_eq!(
        record(&store, "x").await.0,
        generation,
        "acquired during the cooldown"
    );
    assert!(generation > released_at);
    assert_eq!(record(&store, "x").await.1, head, "the floor is intact");

    // Visible now: after the cooldown, b serves both writes.
    lagging.hidden.lock().unwrap().clear();
    let started = std::time::Instant::now();
    let r = loop {
        let r = send(b.addr, "GET", path, "").await;
        if r.status == 200 || started.elapsed() > Duration::from_secs(10) {
            break r;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    assert_eq!((r.status, r.body.trim()), (200, "2"), "{}", r.body);
}

/// The record's holder, version 1 or 2.
async fn lease_holder_any(store: &Arc<dyn ObjectStore>, name: &str) -> Option<String> {
    let key = StorePath::from(test_key(Family::Owners, &format!("t/{name}")));
    let body =
        String::from_utf8(store.get(&key).await.ok()?.bytes().await.unwrap().to_vec()).unwrap();
    let node = body.lines().nth(1)?.split(' ').next()?.to_owned();
    (node != "-").then_some(node)
}
