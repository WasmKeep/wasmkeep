//! The host's file calls on a bucket (in memory) and on disk, through the
//! same [`host::Caller`] a guest's calls reach.

#![allow(clippy::unwrap_used, clippy::panic)] // test code

use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, PutPayload};
use wasmkeep_wit::wasmkeep::actor::blobs::{BlobError, Condition, WriteOptions};
use wasmtime::component::ResourceTable;

use super::host::{Caller, Gate};
use super::*;
use crate::cluster::LeaseView;

fn opts(content_type: Option<&str>, condition: Condition) -> WriteOptions {
    WriteOptions {
        content_type: content_type.map(str::to_owned),
        condition,
    }
}

fn any() -> WriteOptions {
    opts(Some("text/plain"), Condition::Any)
}

struct Fixture {
    _tmp: tempfile::TempDir,
    blobs: Arc<Blobs>,
}

fn bucket_with(store: Arc<dyn ObjectStore>, max: u64) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let blobs = Blobs::on_bucket(
        store,
        "p/",
        "state/acme/shop/",
        "00112233aabbccdd",
        tmp.path().join("spool"),
        max,
    );
    Fixture {
        _tmp: tmp,
        blobs: Arc::new(blobs),
    }
}

fn bucket() -> Fixture {
    bucket_with(Arc::new(InMemory::new()), DEFAULT_MAX_FILE)
}

fn disk_with(max: u64) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let blobs = Blobs::on_disk(tmp.path(), "00112233aabbccdd", max);
    Fixture {
        _tmp: tmp,
        blobs: Arc::new(blobs),
    }
}

fn both() -> [Fixture; 2] {
    [bucket(), disk_with(DEFAULT_MAX_FILE)]
}

fn caller(f: &Fixture, ns: Namespace) -> Caller {
    Caller::new(Some(Arc::clone(&f.blobs)), ns, Gate::Free)
}

#[tokio::test]
async fn put_get_head_delete_and_conditions() {
    for f in both() {
        let c = caller(&f, Namespace::actor("photos", "alice", "0"));
        assert!(c.head("a.txt".into()).await.unwrap().is_none());
        assert!(c.get("a.txt".into()).await.unwrap().is_none());
        let first = c
            .put(
                "a.txt".into(),
                b"hello".to_vec(),
                opts(Some("text/plain; charset=utf-8"), Condition::Absent),
            )
            .await
            .unwrap();
        assert_eq!(
            (first.size, first.content_type.as_str()),
            (5, "text/plain; charset=utf-8")
        );
        let head = c.head("a.txt".into()).await.unwrap().unwrap();
        assert_eq!(
            (
                head.key.as_str(),
                head.size,
                head.content_type.as_str(),
                head.etag.as_str()
            ),
            ("a.txt", 5, "text/plain; charset=utf-8", first.etag.as_str())
        );
        assert!(head.modified_ms > 0);
        let (info, body) = c.get("a.txt".into()).await.unwrap().unwrap();
        assert_eq!((info.etag, body), (first.etag.clone(), b"hello".to_vec()));

        // Each condition.
        assert!(matches!(
            c.put("a.txt".into(), b"x".to_vec(), opts(None, Condition::Absent))
                .await,
            Err(BlobError::Conflict)
        ));
        assert!(matches!(
            c.put(
                "a.txt".into(),
                b"x".to_vec(),
                opts(None, Condition::Etag("\"nope\"".into()))
            )
            .await,
            Err(BlobError::Conflict)
        ));
        assert!(matches!(
            c.put(
                "missing".into(),
                b"x".to_vec(),
                opts(None, Condition::Etag(first.etag.clone()))
            )
            .await,
            Err(BlobError::Conflict)
        ));
        let second = c
            .put(
                "a.txt".into(),
                b"bye".to_vec(),
                opts(None, Condition::Etag(first.etag.clone())),
            )
            .await
            .unwrap();
        assert_ne!(second.etag, first.etag);
        assert_eq!(second.content_type, keys::DEFAULT_CONTENT_TYPE);
        c.put("a.txt".into(), b"any".to_vec(), any()).await.unwrap();
        assert_eq!(c.get("a.txt".into()).await.unwrap().unwrap().1, b"any");
        assert!(matches!(
            c.put(
                "bad".into(),
                Vec::new(),
                opts(Some("text/plain\r\nx: y"), Condition::Any)
            )
            .await,
            Err(BlobError::NotAllowed(_))
        ));

        c.delete("a.txt".into()).await.unwrap();
        c.delete("a.txt".into()).await.unwrap();
        assert!(c.head("a.txt".into()).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn whole_file_calls_stop_at_16_mib() {
    for f in both() {
        let c = caller(&f, Namespace::front());
        let big = vec![7u8; MAX_BUFFERED as usize + 1];
        assert!(matches!(
            c.put("big".into(), big.clone(), any()).await,
            Err(BlobError::TooLarge)
        ));
        // A file over 16 MiB written as a stream is `too-large` for `get`,
        // and read whole through `open-read`.
        let mut c = c;
        let mut table = ResourceTable::new();
        write_streamed(&mut c, &mut table, "big", &big, any())
            .await
            .unwrap();
        assert!(matches!(
            c.get("big".into()).await,
            Err(BlobError::TooLarge)
        ));
        assert_eq!(
            read_streamed(&c, &mut table, "big", None).await.unwrap(),
            big
        );
    }
}

async fn write_streamed(
    c: &mut Caller,
    table: &mut ResourceTable,
    key: &str,
    body: &[u8],
    options: WriteOptions,
) -> Result<wasmkeep_wit::wasmkeep::actor::blobs::BlobInfo, BlobError> {
    let upload = c.open_write(table, key.into(), options).await.unwrap()?;
    let stream = c.body(table, &upload).unwrap()?;
    {
        let s = table.get_mut(&stream).unwrap();
        let mut rest = body;
        while !rest.is_empty() {
            let n = s.check_write().unwrap();
            if n == 0 {
                s.ready().await;
                continue;
            }
            let (now, later) = rest.split_at(n.min(rest.len()));
            s.write(bytes::Bytes::copy_from_slice(now)).unwrap();
            rest = later;
        }
        s.ready().await;
    }
    let r = c.finish(table, upload).await.unwrap();
    table.delete(stream).unwrap();
    r
}

async fn read_streamed(
    c: &Caller,
    table: &mut ResourceTable,
    key: &str,
    range: Option<(u64, u64)>,
) -> Result<Vec<u8>, BlobError> {
    let Some((_, stream)) = c.open_read(table, key.into(), range).await.unwrap()? else {
        panic!("{key} not found");
    };
    let mut out = Vec::new();
    let s = table.get_mut(&stream).unwrap();
    loop {
        s.ready().await;
        match s.read(1 << 20) {
            Ok(b) => out.extend_from_slice(&b),
            Err(wasmtime_wasi::p2::StreamError::Closed) => break,
            Err(e) => panic!("{e}"),
        }
    }
    table.delete(stream).unwrap();
    Ok(out)
}

#[tokio::test]
async fn streams_read_ranges_and_write_with_conditions() {
    for f in both() {
        let mut c = caller(&f, Namespace::actor("c", "n", "0"));
        let mut table = ResourceTable::new();
        let body: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        let info = write_streamed(
            &mut c,
            &mut table,
            "s/x.bin",
            &body,
            opts(None, Condition::Absent),
        )
        .await
        .unwrap();
        assert_eq!(info.size, body.len() as u64);
        assert!(matches!(
            write_streamed(
                &mut c,
                &mut table,
                "s/x.bin",
                b"again",
                opts(None, Condition::Absent)
            )
            .await,
            Err(BlobError::Conflict)
        ));
        assert_eq!(
            read_streamed(&c, &mut table, "s/x.bin", None)
                .await
                .unwrap(),
            body
        );
        assert_eq!(
            read_streamed(&c, &mut table, "s/x.bin", Some((1_000, 70_000)))
                .await
                .unwrap(),
            &body[1_000..71_000]
        );
        assert_eq!(
            read_streamed(&c, &mut table, "s/x.bin", Some((199_990, 100)))
                .await
                .unwrap(),
            &body[199_990..]
        );
        assert!(
            read_streamed(&c, &mut table, "s/x.bin", Some((200_000, 5)))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(matches!(
            read_streamed(&c, &mut table, "s/x.bin", Some((200_001, 5))).await,
            Err(BlobError::NotAllowed(_))
        ));
        assert!(
            c.open_read(&mut table, "nope".into(), None)
                .await
                .unwrap()
                .unwrap()
                .is_none()
        );
        // A body taken twice is refused.
        let upload = c
            .open_write(&mut table, "t".into(), any())
            .await
            .unwrap()
            .unwrap();
        c.body(&mut table, &upload).unwrap().unwrap();
        assert!(matches!(
            c.body(&mut table, &upload).unwrap(),
            Err(BlobError::NotAllowed(_))
        ));
        // Dropped without `finish`: nothing is written.
        c.drop_upload(&mut table, upload).unwrap();
        assert!(c.head("t".into()).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn a_streamed_write_stops_at_the_nodes_limit() {
    for f in [
        bucket_with(Arc::new(InMemory::new()), 100_000),
        disk_with(100_000),
    ] {
        let mut c = caller(&f, Namespace::front());
        let mut table = ResourceTable::new();
        let upload = c
            .open_write(&mut table, "big".into(), any())
            .await
            .unwrap()
            .unwrap();
        let stream = c.body(&mut table, &upload).unwrap().unwrap();
        let s = table.get_mut(&stream).unwrap();
        s.write(bytes::Bytes::from(vec![0u8; 100_000])).unwrap();
        assert!(s.write(bytes::Bytes::from_static(b"x")).is_err());
        assert!(matches!(
            c.finish(&mut table, upload).await.unwrap(),
            Err(BlobError::TooLarge)
        ));
        assert!(c.head("big".into()).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn listings_by_prefix_and_page() {
    for f in both() {
        let c = caller(&f, Namespace::actor("c", "n", "0"));
        let keys = [
            "a",
            "uploads/a",
            "uploads/b/c",
            "uploads~",
            "up",
            "upload",
            "z",
        ];
        for k in keys {
            c.put(
                k.into(),
                k.as_bytes().to_vec(),
                opts(Some("image/png"), Condition::Any),
            )
            .await
            .unwrap();
        }
        let list = |prefix: &str, after: Option<&str>, limit| {
            let c = &c;
            let (prefix, after) = (prefix.to_owned(), after.map(str::to_owned));
            async move {
                let l = c.list(prefix, after, limit).await.unwrap();
                (
                    l.entries.iter().map(|e| e.key.clone()).collect::<Vec<_>>(),
                    l.next,
                )
            }
        };
        assert_eq!(
            list("", None, 1_000).await.0,
            [
                "a",
                "up",
                "upload",
                "uploads/a",
                "uploads/b/c",
                "uploads~",
                "z"
            ]
        );
        assert_eq!(
            list("up", None, 1_000).await.0,
            ["up", "upload", "uploads/a", "uploads/b/c", "uploads~"]
        );
        assert_eq!(
            list("uploads/", None, 1_000).await.0,
            ["uploads/a", "uploads/b/c"]
        );
        assert_eq!(list("uploads/b", None, 1_000).await.0, ["uploads/b/c"]);
        assert_eq!(list("uploads/b/", None, 1_000).await.0, ["uploads/b/c"]);
        assert_eq!(list("nothing", None, 1_000).await, (vec![], None));
        // Pages: `next` until the end, then none.
        let mut all = Vec::new();
        let mut after = None;
        loop {
            let (page, next) = list("", after.as_deref(), 2).await;
            assert!(page.len() <= 2);
            all.extend(page);
            match next {
                Some(n) => after = Some(n),
                None => break,
            }
        }
        assert_eq!(
            all,
            [
                "a",
                "up",
                "upload",
                "uploads/a",
                "uploads/b/c",
                "uploads~",
                "z"
            ]
        );
        assert_eq!(
            list("up", Some("upload"), 2).await,
            (
                vec!["uploads/a".to_owned(), "uploads/b/c".to_owned()],
                Some("uploads/b/c".to_owned())
            )
        );
        // Types come with the entries.
        let l = c.list(String::new(), None, 1).await.unwrap();
        assert_eq!(l.entries[0].content_type, "image/png");
        assert_eq!(l.entries[0].size, 1);
        for limit in [0, 1_001] {
            assert!(matches!(
                c.list(String::new(), None, limit).await,
                Err(BlobError::NotAllowed(_))
            ));
        }
    }
}

#[tokio::test]
async fn namespaces_and_apps_never_meet() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let tmp = tempfile::tempdir().unwrap();
    let app = |app: &str, epoch: &str| {
        Arc::new(Blobs::on_bucket(
            Arc::clone(&store),
            "p/",
            &format!("state/acme/{app}/"),
            epoch,
            tmp.path().join(app).join(epoch),
            DEFAULT_MAX_FILE,
        ))
    };
    let (shop, blog, shop_again) = (app("shop", "e1"), app("blog", "e1"), app("shop", "e2"));
    let callers = [
        Caller::new(
            Some(Arc::clone(&shop)),
            Namespace::actor("c", "alice", "0"),
            Gate::Free,
        ),
        Caller::new(
            Some(Arc::clone(&shop)),
            Namespace::actor("c", "bob", "0"),
            Gate::Free,
        ),
        Caller::new(
            Some(Arc::clone(&shop)),
            Namespace::actor("d", "alice", "0"),
            Gate::Free,
        ),
        Caller::new(Some(Arc::clone(&shop)), Namespace::front(), Gate::Free),
        Caller::new(Some(blog), Namespace::actor("c", "alice", "0"), Gate::Free),
        // The same app after a re-creation or an in-place restore.
        Caller::new(
            Some(shop_again),
            Namespace::actor("c", "alice", "0"),
            Gate::Free,
        ),
    ];
    for (i, c) in callers.iter().enumerate() {
        c.put("k".into(), vec![i as u8], any()).await.unwrap();
    }
    for (i, c) in callers.iter().enumerate() {
        assert_eq!(c.get("k".into()).await.unwrap().unwrap().1, vec![i as u8]);
        assert_eq!(
            c.list(String::new(), None, 10).await.unwrap().entries.len(),
            1
        );
    }
    // Where they are: under each app's root and epoch, the name hashed.
    let keys: Vec<String> = futures_util::StreamExt::collect::<Vec<_>>(store.list(None))
        .await
        .into_iter()
        .map(|m| m.unwrap().location.to_string())
        .collect();
    let id = keys::name_id("alice");
    for want in [
        format!("p/state/acme/shop/blobs/e1/c/{id}/0/k"),
        "p/state/acme/shop/blobs/e1/_front/main/0/k".to_owned(),
        format!("p/state/acme/blog/blobs/e1/c/{id}/0/k"),
        format!("p/state/acme/shop/blobs/e2/c/{id}/0/k"),
    ] {
        assert!(keys.contains(&want), "{want} not in {keys:?}");
    }
    assert_eq!(keys.len(), 6);
}

#[tokio::test]
async fn a_bucket_prefix_past_200_bytes_keeps_no_files() {
    let tmp = tempfile::tempdir().unwrap();
    let blobs = Blobs::on_bucket(
        Arc::new(InMemory::new()),
        &format!("{}/", "p".repeat(MAX_BUCKET_PREFIX)),
        "state/acme/shop/",
        "e",
        tmp.path().join("spool"),
        DEFAULT_MAX_FILE,
    );
    let c = Caller::new(Some(Arc::new(blobs)), Namespace::front(), Gate::Free);
    assert!(matches!(
        c.head("a".into()).await,
        Err(BlobError::Unavailable(_))
    ));
}

/// A store whose writes land after this node's hold lapsed.
#[derive(Debug)]
struct LapseOnWrite {
    inner: InMemory,
    validity: Arc<crate::cluster::NodeValidity>,
}

impl std::fmt::Display for LapseOnWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LapseOnWrite")
    }
}

#[async_trait::async_trait]
impl ObjectStore for LapseOnWrite {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let r = self.inner.put_opts(location, payload, opts).await;
        self.validity.invalidate();
        r
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
        self.validity.invalidate();
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
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

#[tokio::test]
async fn a_write_acknowledged_after_the_hold_lapsed_is_unavailable() {
    let (view, validity) = LeaseView::lapsable(1);
    let store: Arc<dyn ObjectStore> = Arc::new(LapseOnWrite {
        inner: InMemory::new(),
        validity: Arc::clone(&validity),
    });
    let f = bucket_with(store, DEFAULT_MAX_FILE);
    let ns = Namespace::actor("c", "n", "0");
    let c = Caller::new(Some(Arc::clone(&f.blobs)), ns.clone(), Gate::Held(view));
    let r = c.put("a".into(), b"x".to_vec(), any()).await;
    assert!(matches!(r, Err(BlobError::Unavailable(_))), "{r:?}");
    // It landed nonetheless: a plain PUT is not fenced (section 6).
    assert!(f.blobs.head(&ns, "a").await.unwrap().is_some());
    // And the next write is refused before it starts.
    assert!(matches!(
        c.put("b".into(), b"x".to_vec(), any()).await,
        Err(BlobError::NotAllowed(_))
    ));
}

#[tokio::test]
async fn files_sit_under_the_root_the_rest_of_the_state_spells() {
    use object_store::ObjectStoreExt as _;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let tmp = tempfile::tempdir().unwrap();
    // A prefix with a byte `Path::from` escapes.
    let blobs = Blobs::on_bucket(
        Arc::clone(&store),
        "p~x/",
        "state/acme/shop/",
        "e",
        tmp.path().join("spool"),
        DEFAULT_MAX_FILE,
    );
    let c = Caller::new(Some(Arc::new(blobs)), Namespace::front(), Gate::Free);
    c.put("a~b".into(), b"x".to_vec(), any()).await.unwrap();
    // Listed under the app's root as the rest of its state builds it.
    let root = StorePath::from("p~x/state/acme/shop");
    let listed: Vec<String> = futures_util::StreamExt::collect::<Vec<_>>(store.list(Some(&root)))
        .await
        .into_iter()
        .map(|m| m.unwrap().location.to_string())
        .collect();
    assert_eq!(listed, [format!("{root}/blobs/e/_front/main/0/a~b")]);
    assert!(
        store
            .head(&StorePath::parse(&listed[0]).unwrap())
            .await
            .is_ok()
    );
    assert_eq!(c.get("a~b".into()).await.unwrap().unwrap().1, b"x");
    assert_eq!(
        c.list(String::new(), None, 10).await.unwrap().entries.len(),
        1
    );
}

/// Signs nothing: a URL naming what a real signer binds, to read back.
#[derive(Debug)]
struct Recording;

#[async_trait::async_trait]
impl object_store::signer::Signer for Recording {
    async fn signed_url(
        &self,
        method: object_store::signer::Method,
        path: &StorePath,
        expires_in: std::time::Duration,
    ) -> object_store::Result<object_store::signer::Url> {
        self.signed_url_opts(method, path, expires_in, &Default::default())
            .await
    }

    async fn signed_url_opts(
        &self,
        method: object_store::signer::Method,
        path: &StorePath,
        expires_in: std::time::Duration,
        options: &object_store::signer::SignedUrlOptions,
    ) -> object_store::Result<object_store::signer::Url> {
        let mut url =
            object_store::signer::Url::parse(&format!("https://bucket.example/{}", path.as_ref()))
                .unwrap();
        url.query_pairs_mut()
            .append_pair("method", method.as_str())
            .append_pair("expires", &expires_in.as_secs().to_string())
            .extend_pairs(options.extra_query.iter());
        Ok(url)
    }
}

fn with_links(f: Fixture, presigned: bool) -> (tempfile::TempDir, Arc<Blobs>) {
    let Fixture { _tmp, blobs } = f;
    let blobs = Arc::into_inner(blobs)
        .unwrap()
        .with_links(links::Links::new(
            "acme",
            "shop",
            "00112233aabbccdd",
            links::LinkSettings {
                presigner: presigned
                    .then(|| Arc::new(Recording) as Arc<dyn object_store::signer::Signer>),
                base: Some("https://shop.example.com".into()),
            },
            links::MacSource::Key(token::mac_key(b"k", "acme", "shop").unwrap()),
        ));
    (_tmp, Arc::new(blobs))
}

fn download(
    inline: bool,
    name: Option<&str>,
    secs: Option<u32>,
) -> wasmkeep_wit::wasmkeep::actor::blobs::DownloadOptions {
    use wasmkeep_wit::wasmkeep::actor::blobs::{Disposition, DownloadOptions};
    DownloadOptions {
        expires_in_secs: secs,
        disposition: if inline {
            Disposition::Inline
        } else {
            Disposition::Attachment(name.map(str::to_owned))
        },
    }
}

#[tokio::test]
async fn presigned_downloads_override_every_stored_header() {
    let (_tmp, blobs) = with_links(bucket(), true);
    let mut c = Caller::new(Some(blobs), Namespace::actor("c", "n", "0"), Gate::Free);
    c.put(
        "a~b/pic.png".into(),
        b"x".to_vec(),
        opts(Some("image/png"), Condition::Any),
    )
    .await
    .unwrap();
    c.put(
        "page.html".into(),
        b"x".to_vec(),
        opts(Some("text/html"), Condition::Any),
    )
    .await
    .unwrap();
    let url = c
        .link_download("a~b/pic.png".into(), download(false, Some("Été.png"), None))
        .await
        .unwrap();
    let url = object_store::signer::Url::parse(&url).unwrap();
    let q: std::collections::HashMap<String, String> = url.query_pairs().into_owned().collect();
    assert_eq!(
        url.path(),
        format!(
            "/p/state/acme/shop/blobs/00112233aabbccdd/c/{}/0/a~b/pic.png",
            keys::name_id("n")
        )
    );
    assert_eq!(q["method"], "GET");
    assert_eq!(q["expires"], "300");
    assert_eq!(q["response-content-type"], "application/octet-stream");
    assert_eq!(
        q["response-content-disposition"],
        "attachment; filename=\"_t_.png\"; filename*=UTF-8''%C3%89t%C3%A9.png"
    );
    assert_eq!(q["response-cache-control"], "private, max-age=300");
    assert_eq!(q["response-content-encoding"], "identity");
    let url = c
        .link_download("a~b/pic.png".into(), download(true, None, Some(60)))
        .await
        .unwrap();
    let url = object_store::signer::Url::parse(&url).unwrap();
    let q: std::collections::HashMap<String, String> = url.query_pairs().into_owned().collect();
    assert_eq!(
        (
            q["response-content-type"].as_str(),
            q["response-content-disposition"].as_str()
        ),
        ("image/png", "inline")
    );
    assert_eq!(q["expires"], "60");
    // Inline only for what a browser cannot run, and for a file there.
    for key in ["page.html", "missing.png"] {
        assert!(matches!(
            c.link_download(key.into(), download(true, None, None))
                .await,
            Err(BlobError::NotAllowed(_))
        ));
    }
    // Uploads stay node links, under the configured base.
    let up = c
        .link_upload(
            "u/1".into(),
            wasmkeep_wit::wasmkeep::actor::blobs::UploadOptions {
                size: 3,
                content_type: "image/png".into(),
                expires_in_secs: None,
            },
        )
        .unwrap();
    assert!(
        up.url
            .starts_with("https://shop.example.com/_wasmkeep/blobs/v1/"),
        "{}",
        up.url
    );
    assert_eq!(
        (up.method.as_str(), up.headers.clone()),
        ("PUT", vec![("content-type".into(), "image/png".into())])
    );
}

#[tokio::test]
async fn links_need_what_writes_need_and_stop_at_1000_an_event() {
    let (_tmp, blobs) = with_links(disk_with(DEFAULT_MAX_FILE), false);
    let upload = |size| wasmkeep_wit::wasmkeep::actor::blobs::UploadOptions {
        size,
        content_type: "image/png".into(),
        expires_in_secs: None,
    };
    let mut c = Caller::new(
        Some(Arc::clone(&blobs)),
        Namespace::actor("c", "n", "0"),
        Gate::Unheld,
    );
    assert!(matches!(
        c.link_upload("u".into(), upload(1)),
        Err(BlobError::NotAllowed(_))
    ));
    // A download is a read: no hold needed.
    let url = c
        .link_download("k".into(), download(false, None, None))
        .await
        .unwrap();
    assert!(url.starts_with("https://shop.example.com/_wasmkeep/blobs/v1/"));
    c.new_event(Gate::Free);
    assert!(matches!(
        c.link_upload("u".into(), upload(DEFAULT_MAX_FILE + 1)),
        Err(BlobError::TooLarge)
    ));
    for _ in 0..999 {
        c.link_download("k".into(), download(false, None, None))
            .await
            .unwrap();
    }
    // The refused upload above counted; the 1,001st link of the event is refused.
    assert!(matches!(
        c.link_download("k".into(), download(false, None, None))
            .await,
        Err(BlobError::NotAllowed(_))
    ));
    c.new_event(Gate::Free);
    c.link_download("k".into(), download(false, None, None))
        .await
        .unwrap();
    // A bucket's node-link upload is one conditional PUT: 16 MiB at most.
    let (_t, blobs) = with_links(bucket(), false);
    let mut c = Caller::new(Some(blobs), Namespace::front(), Gate::Free);
    assert!(matches!(
        c.link_upload("u".into(), upload(MAX_BUFFERED + 1)),
        Err(BlobError::TooLarge)
    ));
    c.link_upload("u".into(), upload(MAX_BUFFERED)).unwrap();
}
