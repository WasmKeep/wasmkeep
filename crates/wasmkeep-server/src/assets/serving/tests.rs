use super::*;
use http_body_util::BodyExt;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

async fn fixture(mode: &str) -> (tempfile::TempDir, State) {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut index = Index {
        not_found: mode.into(),
        ..Index::default()
    };
    index.immutable.insert("assets/".into());
    index.front_first.insert("api/".into());
    for name in [
        "index.html",
        "404.html",
        "assets/a.js",
        "dir/index.html",
        "api/a",
    ] {
        let mut contents = Vec::new();
        for bytes in [b"plain content".as_slice(), b"brotli", b"gzip"] {
            let hash = digest(bytes);
            store
                .put(
                    &format!("apps/t/a/_assets/content/{hash}").into(),
                    PutPayload::from(bytes.to_vec()),
                )
                .await
                .unwrap();
            contents.push(Content {
                digest: hash,
                size: bytes.len() as u64,
            });
        }
        index.files.insert(
            name.into(),
            File {
                content_type: "text/html;charset=utf-8".into(),
                identity: contents[0].clone(),
                br: Some(contents[1].clone()),
                gzip: Some(contents[2].clone()),
            },
        );
    }
    let state = State::new(
        Some(Deployed {
            index: Arc::new(index),
            previous: None,
            app: crate::apps::AppRef::new(store, "", "t", "a"),
        }),
        dir.path().join("assets/t/a"),
        Cache::new(1024),
        crate::metrics::app_labels("t", "a"),
    );
    (dir, state)
}
async fn response(
    state: &State,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
) -> Response<HyperOutgoingBody> {
    let mut req = Request::builder().method(method).uri(path);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    state
        .answer(&req.body(()).unwrap())
        .await
        .expect("asset response")
}
#[tokio::test]
async fn assets_responses_status_headers_and_encodings() {
    let (_dir, state) = fixture("front").await;
    let plain_tag = format!("\"{}\"", &digest(b"plain content")[..32]);
    let br_tag = format!("\"{}\"", &digest(b"brotli")[..32]);
    for (headers, status, body, encoding) in [
        (vec![], 200, b"plain content".as_slice(), None),
        (
            vec![("accept-encoding", "gzip, br")],
            200,
            b"brotli".as_slice(),
            Some("br"),
        ),
        (
            vec![("accept-encoding", "br;q=0, gzip")],
            200,
            b"gzip".as_slice(),
            Some("gzip"),
        ),
        (vec![("if-none-match", "*")], 304, b"".as_slice(), None),
        (
            vec![("range", "bytes=0-4"), ("accept-encoding", "br")],
            206,
            b"plain".as_slice(),
            None,
        ),
        (vec![("range", "bytes=999-")], 416, b"".as_slice(), None),
        (
            vec![
                ("range", "bytes=0-4"),
                ("if-range", br_tag.as_str()),
                ("accept-encoding", "br"),
            ],
            200,
            b"brotli".as_slice(),
            Some("br"),
        ),
    ] {
        let res = response(&state, "GET", "/assets/a.js", &headers).await;
        assert_eq!(res.status().as_u16(), status);
        assert_eq!(
            res.headers()
                .get("content-encoding")
                .map(|v| v.to_str().unwrap()),
            encoding
        );
        assert_eq!(res.headers()["vary"], "Accept-Encoding");
        assert_eq!(
            res.headers()["cache-control"],
            "public, max-age=31536000, immutable"
        );
        if status == 206 {
            assert_eq!(res.headers()["content-range"], "bytes 0-4/13");
            assert_eq!(res.headers()["etag"], plain_tag);
        }
        if status == 416 {
            assert_eq!(res.headers()["content-range"], "bytes */13");
        }
        assert_eq!(res.into_body().collect().await.unwrap().to_bytes(), body);
    }
    let weak = format!("\"other\", W/{plain_tag}");
    assert_eq!(
        response(&state, "GET", "/index.html", &[("if-none-match", &weak)])
            .await
            .status(),
        304
    );
    let head = response(&state, "HEAD", "/index.html", &[]).await;
    assert_eq!(head.status(), 200);
    assert_eq!(head.headers()["content-length"], "13");
    assert!(
        head.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty()
    );
    let redirect = response(&state, "GET", "/dir?x=1", &[]).await;
    assert_eq!(redirect.status(), 308);
    assert_eq!(redirect.headers()["location"], "/dir/?x=1");
    for path in ["/%61ctor/a", "/%5fwasmkeep/health"] {
        assert_eq!(response(&state, "GET", path, &[]).await.status(), 400);
    }
}
#[tokio::test]
async fn assets_not_found_and_front_first() {
    for mode in ["front", "spa", "404"] {
        let (_dir, state) = fixture(mode).await;
        for path in ["/api/a", "/bad%2fpath", "/../x"] {
            assert!(
                state
                    .answer(&Request::get(path).body(()).unwrap())
                    .await
                    .is_none()
            );
        }
        let req = Request::get("/missing.js").body(()).unwrap();
        if mode == "404" {
            let res = state.answer(&req).await.unwrap();
            assert_eq!(res.status(), 404);
            assert_eq!(res.headers()["cache-control"], "no-store");
        } else {
            assert!(state.answer(&req).await.is_none());
        }
        if mode == "spa" {
            let res = response(&state, "GET", "/route", &[("accept", "text/html")]).await;
            assert_eq!(res.status(), 200);
            assert_eq!(res.headers()["cache-control"], "no-cache");
            assert!(
                res.headers()["vary"]
                    .to_str()
                    .unwrap()
                    .contains("Sec-Fetch-Mode")
            );
        }
        assert!(
            state
                .answer(&Request::post("/index.html").body(()).unwrap())
                .await
                .is_none()
        );
    }
}

#[tokio::test]
async fn assets_cache_global_lru_integrity_and_restart() {
    let (dir, mut state) = fixture("front").await;
    state.cache = Cache::new(18);
    let body = response(&state, "GET", "/index.html", &[])
        .await
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(body, b"plain content".as_slice());
    response(&state, "GET", "/index.html", &[("accept-encoding", "br")])
        .await
        .into_body()
        .collect()
        .await
        .unwrap();
    {
        let entries = state.cache.inner.lock().unwrap();
        assert_eq!(entries.bytes, 6);
        assert_eq!(entries.files.len(), 1);
    }
    assert!(!state.dir.join(digest(b"plain content")).exists());
    // A sibling app cannot use the first app's cached digest, even within the same process budget.
    let mut other = state.clone();
    other.dir = dir.path().join("assets/t/other");
    other.current.as_mut().unwrap().app = crate::apps::AppRef::new(
        Arc::new(object_store::memory::InMemory::new()),
        "",
        "t",
        "other",
    );
    assert_eq!(
        response(&other, "GET", "/index.html", &[("accept-encoding", "br")])
            .await
            .status(),
        503
    );
    // Startup rejects altered bytes and enforces the byte limit on retained files.
    std::fs::write(state.dir.join(digest(b"brotli")), b"badbad").unwrap();
    let restarted = State::new(
        state.current.clone(),
        state.dir.clone(),
        Cache::new(4),
        state.labels.clone(),
    );
    restarted.initialize().unwrap();
    assert!(!state.dir.join(digest(b"brotli")).exists());
    assert_eq!(restarted.cache.inner.lock().unwrap().bytes, 0);
    // Files larger than the retained cache budget still stream after verification.
    let res = response(&restarted, "GET", "/index.html", &[]).await;
    assert_eq!(
        res.into_body().collect().await.unwrap().to_bytes(),
        b"plain content".as_slice()
    );
    assert_eq!(restarted.cache.inner.lock().unwrap().bytes, 0);
    // Correct-sized tampering in the bucket cannot be served.
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    store
        .put(
            &format!("apps/t/a/_assets/content/{}", digest(b"plain content")).into(),
            b"wrong content".to_vec().into(),
        )
        .await
        .unwrap();
    other.current.as_mut().unwrap().app = crate::apps::AppRef::new(store, "", "t", "a");
    let res = response(&other, "GET", "/index.html", &[]).await;
    assert_eq!(res.status(), 503);
    assert_eq!(res.headers()["retry-after"], "1");
}

#[tokio::test]
async fn assets_previous_index_survives_restart_and_rollback() {
    let (_dir, state) = fixture("front").await;
    state.persist().unwrap();
    let old = state.current.clone();
    let mut deployed = old.clone().unwrap();
    let mut index = (*deployed.index).clone();
    index
        .files
        .insert("assets/b.js".into(), index.files["assets/a.js"].clone());
    index.files.remove("assets/a.js");
    index.files.remove("index.html");
    deployed.index = Arc::new(index);
    let switched = state.switched(Some(deployed.clone())).unwrap();
    assert_eq!(
        response(&switched, "GET", "/assets/a.js", &[])
            .await
            .status(),
        200
    );
    assert!(
        switched
            .answer(&Request::get("/index.html").body(()).unwrap())
            .await
            .is_none()
    );
    let restarted = State::new(
        Some(deployed),
        state.dir.clone(),
        state.cache.clone(),
        state.labels.clone(),
    );
    assert_eq!(
        response(&restarted, "GET", "/assets/a.js", &[])
            .await
            .status(),
        200
    );
    let rollback = restarted.switched(old).unwrap();
    assert_eq!(
        response(&rollback, "GET", "/index.html", &[])
            .await
            .status(),
        200
    );
    assert_eq!(
        response(&rollback, "GET", "/assets/b.js", &[])
            .await
            .status(),
        200
    );
    assert!(
        !rollback
            .previous
            .as_ref()
            .unwrap()
            .files
            .contains_key("index.html")
    );
    let restarted = State::new(
        rollback.current.clone(),
        state.dir.clone(),
        state.cache.clone(),
        state.labels.clone(),
    );
    assert_eq!(
        response(&restarted, "GET", "/assets/b.js", &[])
            .await
            .status(),
        200
    );
}

#[tokio::test]
async fn assets_more_ranges_and_not_found_headers() {
    let (_dir, mut state) = fixture("404").await;
    for (range, expected, body) in [
        ("bytes=-7", 206, "content"),
        ("bytes=6-", 206, "content"),
        ("bytes=0-999", 206, "plain content"),
        ("bytes=0-1,4-5", 200, "plain content"),
        ("invalid", 200, "plain content"),
        ("bytes=-0", 416, ""),
    ] {
        let res = response(&state, "GET", "/index.html", &[("range", range)]).await;
        assert_eq!(res.status().as_u16(), expected);
        assert_eq!(
            res.into_body().collect().await.unwrap().to_bytes(),
            body.as_bytes()
        );
    }
    let res = response(&state, "HEAD", "/index.html", &[("range", "bytes=1-2")]).await;
    assert_eq!(res.status(), 206);
    assert_eq!(res.headers()["content-length"], "2");
    assert!(
        res.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty()
    );
    Arc::make_mut(&mut state.current.as_mut().unwrap().index)
        .files
        .remove("404.html");
    let res = response(&state, "HEAD", "/missing", &[]).await;
    assert_eq!(res.status(), 404);
    assert_eq!(res.headers()["cache-control"], "no-store");
    assert_eq!(res.headers()["content-length"], "10");
    assert!(
        res.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty()
    );
}

#[tokio::test]
async fn assets_cache_eviction_spans_app_directories_and_metrics_are_labeled() {
    let (_a, mut first) = fixture("front").await;
    let (_b, mut second) = fixture("front").await;
    let cache = Cache::new(8);
    first.cache = cache.clone();
    second.cache = cache.clone();
    first.labels = crate::metrics::app_labels("cache-test", "first");
    second.labels = crate::metrics::app_labels("cache-test", "second");
    response(&first, "GET", "/", &[("accept-encoding", "br")])
        .await
        .into_body()
        .collect()
        .await
        .unwrap();
    response(&second, "GET", "/", &[("accept-encoding", "gzip")])
        .await
        .into_body()
        .collect()
        .await
        .unwrap();
    assert_eq!(cache.inner.lock().unwrap().bytes, 4);
    assert!(!first.dir.join(digest(b"brotli")).exists());
    assert!(second.dir.join(digest(b"gzip")).exists());
    let page = crate::metrics::render(Default::default());
    for row in [
        "wasmkeep_asset_cache_bytes{tenant=\"cache-test\",app=\"first\"} 0",
        "wasmkeep_asset_cache_bytes{tenant=\"cache-test\",app=\"second\"} 4",
        "wasmkeep_asset_requests_total{tenant=\"cache-test\",app=\"first\",outcome=\"ok\"} 1",
        "wasmkeep_asset_bytes_total{tenant=\"cache-test\",app=\"second\",encoding=\"gzip\"} 4",
    ] {
        assert!(page.contains(row), "missing {row}");
    }
}

#[tokio::test]
async fn assets_encoding_conditionals_and_exclusions() {
    let (_dir, state) = fixture("front").await;
    let br_tag = format!("\"{}\"", &digest(b"brotli")[..32]);
    let weak = format!("W/{br_tag}");
    let res = response(
        &state,
        "GET",
        "/",
        &[("accept-encoding", "br"), ("if-none-match", &weak)],
    )
    .await;
    assert_eq!(res.status(), 304);
    assert_eq!(res.headers()["etag"], br_tag);
    for method in ["GET", "HEAD"] {
        let res = response(
            &state,
            method,
            "/",
            &[("accept-encoding", "br;q=0,gzip;q=0,identity;q=0")],
        )
        .await;
        assert_eq!(res.status(), 406);
        assert_eq!(res.headers()["vary"], "Accept-Encoding");
    }
    let res = response(
        &state,
        "GET",
        "/",
        &[
            ("range", "bytes=0-1"),
            ("if-range", "Wed, 21 Oct 2015 07:28:00 GMT"),
            ("accept-encoding", "gzip"),
        ],
    )
    .await;
    assert_eq!(res.status(), 200);
    assert_eq!(res.headers()["content-encoding"], "gzip");
    let res = response(
        &state,
        "GET",
        "/",
        &[
            ("range", "bytes=0-1"),
            ("accept-encoding", "identity;q=0,br"),
        ],
    )
    .await;
    assert_eq!(res.status(), 206);
    assert!(!res.headers().contains_key("content-encoding"));
}

#[tokio::test]
#[cfg(unix)]
async fn dev_assets_live_reads_headers_modes_and_symlink_replacement() {
    for mode in ["front", "spa", "404"] {
        let (dir, deployed) = fixture(mode).await;
        let mut index = (*deployed.current.as_ref().unwrap().index).clone();
        for (name, file) in &mut index.files {
            let path = dir.path().join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, b"plain content").unwrap();
            file.br = None;
            file.gzip = None;
        }
        let local = Local::new(dir.path(), index.clone()).unwrap();
        let state = State::new(None, dir.path().join("unused"), Cache::new(0), "".into())
            .with_local(Some(local.clone()));
        let r = response(&state, "GET", "/", &[("accept-encoding", "gzip, br")]).await;
        assert_eq!(r.status(), 200);
        assert!(!r.headers().contains_key("content-encoding"));
        let old_tag = r.headers()["etag"].clone();
        assert_eq!(
            r.into_body().collect().await.unwrap().to_bytes(),
            "plain content"
        );
        // Reads come from disk even before the next table rebuild.
        std::fs::write(dir.path().join("index.html"), b"fresh content").unwrap();
        let r = response(&state, "GET", "/", &[]).await;
        assert_ne!(r.headers()["etag"], old_tag);
        assert_eq!(
            r.into_body().collect().await.unwrap().to_bytes(),
            "fresh content"
        );
        index.files.get_mut("index.html").unwrap().identity.digest = digest(b"fresh content");
        local.replace(index);
        let r = response(&state, "HEAD", "/", &[]).await;
        assert_ne!(r.headers()["etag"], old_tag);
        assert_eq!(r.headers()["content-length"], "13");
        assert!(r.into_body().collect().await.unwrap().to_bytes().is_empty());
        let r = response(&state, "GET", "/", &[("range", "bytes=0-4")]).await;
        assert_eq!(r.status(), 206);
        assert_eq!(r.into_body().collect().await.unwrap().to_bytes(), "fresh");
        let req = Request::builder()
            .uri("/missing")
            .header("accept", "text/html")
            .body(())
            .unwrap();
        let r = state.answer(&req).await;
        match mode {
            "front" => assert!(r.is_none()),
            "spa" => {
                let r = r.unwrap();
                assert_eq!(r.status(), 200);
                assert_eq!(r.headers()["cache-control"], "no-cache");
                assert_eq!(
                    r.into_body().collect().await.unwrap().to_bytes(),
                    "fresh content"
                );
            }
            _ => {
                let r = r.unwrap();
                assert_eq!(r.status(), 404);
                assert_eq!(r.headers()["cache-control"], "no-store");
            }
        }
        assert!(
            state
                .answer(&Request::builder().uri("/api/a").body(()).unwrap())
                .await
                .is_none()
        );
        assert_eq!(
            response(&state, "GET", "/dir?q=1", &[]).await.headers()["location"],
            "/dir/?q=1"
        );
        assert_eq!(
            response(&state, "GET", "/assets/a.js", &[]).await.headers()["cache-control"],
            "public, max-age=31536000, immutable"
        );
        std::fs::remove_file(dir.path().join("index.html")).unwrap();
        std::os::unix::fs::symlink("/etc/passwd", dir.path().join("index.html")).unwrap();
        assert_eq!(response(&state, "GET", "/", &[]).await.status(), 503);
        std::fs::remove_dir_all(dir.path().join("dir")).unwrap();
        std::os::unix::fs::symlink("/etc", dir.path().join("dir")).unwrap();
        assert_eq!(response(&state, "GET", "/dir/", &[]).await.status(), 503);
        assert!(!dir.path().join("unused").exists());
    }
}

#[tokio::test]
async fn review_dev_edits_between_polls_have_current_length() {
    let dir = tempfile::tempdir().unwrap();
    let mut index = Index::default();
    index.files.insert(
        "index.html".into(),
        File {
            content_type: "text/plain".into(),
            identity: Content {
                digest: digest(b"old"),
                size: 3,
            },
            br: None,
            gzip: None,
        },
    );
    std::fs::write(dir.path().join("index.html"), b"old").unwrap();
    let local = Local::new(dir.path(), index).unwrap();
    let state = State::new(None, dir.path().join("unused"), Cache::new(0), "".into())
        .with_local(Some(local));
    for body in ["longer than the old file", "x"] {
        std::fs::write(dir.path().join("index.html"), body).unwrap();
        for method in ["GET", "HEAD"] {
            let r = response(&state, method, "/", &[]).await;
            assert_eq!(r.headers()["content-length"], body.len().to_string());
            assert_eq!(
                r.into_body().collect().await.unwrap().to_bytes(),
                if method == "HEAD" { "" } else { body }
            );
        }
        let r = response(&state, "GET", "/", &[("range", "bytes=0-99")]).await;
        assert_eq!(
            r.headers()["content-range"],
            format!("bytes 0-{}/{}", body.len() - 1, body.len())
        );
        assert_eq!(r.into_body().collect().await.unwrap().to_bytes(), body);
    }
}

#[test]
fn review_register_hashes_without_shared_lock() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(digest(b"cached")), b"cached").unwrap();
    let mut cache = Cache::new(1024);
    Arc::get_mut(&mut cache).unwrap().before_hash = Some(|cache| {
        assert!(
            cache.inner.try_lock().is_ok(),
            "cache hashing holds shared serving lock"
        );
    });
    cache.register(dir.path(), &Arc::from("review")).unwrap();
    assert_eq!(cache.inner.lock().unwrap().bytes, 6);
}

#[derive(Debug)]
struct StalledStore {
    inner: object_store::memory::InMemory,
    started: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    gets: std::sync::atomic::AtomicUsize,
}
impl std::fmt::Display for StalledStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stalled store")
    }
}
#[async_trait::async_trait]
impl ObjectStore for StalledStore {
    async fn put_opts(
        &self,
        p: &object_store::path::Path,
        b: PutPayload,
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
        if !o.head
            && p.as_ref().contains("apps/t/a/")
            && p.filename() == Some(digest(b"plain content").as_str())
        {
            self.gets.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.started.notify_one();
            self.resume.notified().await;
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
#[tokio::test]
async fn review_cold_get_does_not_block_other_paths_or_hits() {
    let (_dir, mut first) = fixture("front").await;
    let store = Arc::new(StalledStore {
        inner: Default::default(),
        started: Default::default(),
        resume: Default::default(),
        gets: Default::default(),
    });
    for app in ["a", "b"] {
        for bytes in [b"plain content".as_slice(), b"brotli", b"gzip"] {
            store
                .put(
                    &format!("apps/t/{app}/_assets/content/{}", digest(bytes)).into(),
                    bytes.to_vec().into(),
                )
                .await
                .unwrap();
        }
    }
    first.current.as_mut().unwrap().app = crate::apps::AppRef::new(store.clone(), "", "t", "a");
    let mut second = first.clone();
    second.dir = first.dir.with_file_name("b");
    second.current.as_mut().unwrap().app = crate::apps::AppRef::new(store.clone(), "", "t", "b");
    // Warm hits in each app before stalling a different representation/path.
    for state in [&first, &second] {
        assert_eq!(
            response(state, "GET", "/", &[("accept-encoding", "gzip")])
                .await
                .status(),
            200
        );
    }
    let cold_state = first.clone();
    let cold = tokio::spawn(async move { response(&cold_state, "GET", "/", &[]).await });
    tokio::time::timeout(std::time::Duration::from_secs(2), store.started.notified())
        .await
        .unwrap();
    let duplicate_state = first.clone();
    let duplicate = tokio::spawn(async move { response(&duplicate_state, "GET", "/", &[]).await });
    for state in [&first, &second] {
        for encoding in ["gzip", "br"] {
            let r = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                response(
                    state,
                    "GET",
                    "/assets/a.js",
                    &[("accept-encoding", encoding)],
                ),
            )
            .await
            .expect("unrelated hit/cold path blocked");
            assert_eq!(r.status(), 200);
            r.into_body().collect().await.unwrap();
        }
    }
    assert_eq!(response(&second, "GET", "/", &[]).await.status(), 200);
    store.resume.notify_one();
    assert_eq!(cold.await.unwrap().status(), 200);
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), duplicate)
            .await
            .unwrap()
            .unwrap()
            .status(),
        200
    );
    assert_eq!(store.gets.load(std::sync::atomic::Ordering::SeqCst), 1);
}
