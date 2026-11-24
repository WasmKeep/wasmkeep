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
            read_streamed(&mut c, &mut table, "big", None)
                .await
                .unwrap(),
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
    c: &mut Caller,
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
            read_streamed(&mut c, &mut table, "s/x.bin", None)
                .await
                .unwrap(),
            body
        );
        assert_eq!(
            read_streamed(&mut c, &mut table, "s/x.bin", Some((1_000, 70_000)))
                .await
                .unwrap(),
            &body[1_000..71_000]
        );
        assert_eq!(
            read_streamed(&mut c, &mut table, "s/x.bin", Some((199_990, 100)))
                .await
                .unwrap(),
            &body[199_990..]
        );
        assert!(
            read_streamed(&mut c, &mut table, "s/x.bin", Some((200_000, 5)))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(matches!(
            read_streamed(&mut c, &mut table, "s/x.bin", Some((200_001, 5))).await,
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

fn with_quota(f: Fixture, limit: u64) -> Fixture {
    let Fixture { _tmp, blobs } = f;
    Fixture {
        _tmp,
        blobs: Arc::new(Arc::into_inner(blobs).unwrap().with_quota(Some(limit))),
    }
}

#[tokio::test]
async fn the_hourly_count_sums_the_current_epoch_and_the_quota_follows_it() {
    for f in [
        with_quota(bucket(), 10),
        with_quota(disk_with(DEFAULT_MAX_FILE), 10),
    ] {
        let (_t, blobs) = with_links(f, false);
        let mut c = Caller::new(
            Some(Arc::clone(&blobs)),
            Namespace::actor("photos", "alice", "0"),
            Gate::Free,
        );
        let front = Caller::new(Some(Arc::clone(&blobs)), Namespace::front(), Gate::Free);
        c.put("a".into(), vec![1; 6], any()).await.unwrap();
        front.put("b".into(), vec![2; 6], any()).await.unwrap();
        // Over the limit, but nothing counted yet: not refused.
        assert!(!blobs.over_quota());
        let n = blobs.count().await.unwrap();
        assert_eq!((n.bytes, n.files), (12, 2));
        blobs.note_usage(&n);
        assert!(blobs.over_quota());
        assert!(matches!(
            c.put("c".into(), vec![3], any()).await,
            Err(BlobError::QuotaExceeded)
        ));
        assert!(matches!(
            c.link_upload(
                "u".into(),
                wasmkeep_wit::wasmkeep::actor::blobs::UploadOptions {
                    size: 1,
                    content_type: "image/png".into(),
                    expires_in_secs: None,
                }
            ),
            Err(BlobError::QuotaExceeded)
        ));
        // A delete frees nothing until the next count.
        c.delete("a".into()).await.unwrap();
        assert!(matches!(
            c.put("c".into(), vec![3], any()).await,
            Err(BlobError::QuotaExceeded)
        ));
        let n = blobs.count().await.unwrap();
        assert_eq!((n.bytes, n.files), (6, 1));
        blobs.note_usage(&n);
        c.put("c".into(), vec![3], any()).await.unwrap();
    }
}

#[tokio::test]
async fn a_bucket_count_is_written_for_every_node_and_other_epochs_are_not_counted() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let f = with_quota(bucket_with(Arc::clone(&store), DEFAULT_MAX_FILE), 5);
    let c = caller(&f, Namespace::front());
    c.put("a".into(), vec![1; 6], any()).await.unwrap();
    store
        .put_opts(
            &StorePath::from("p/state/acme/shop/blobs/ffffffffffffffff/_front/main/0/old"),
            PutPayload::from(vec![0u8; 100]),
            Default::default(),
        )
        .await
        .unwrap();
    let n = f.blobs.count().await.unwrap();
    assert_eq!((n.bytes, n.files), (6, 1));
    f.blobs.publish_usage(&n).await.unwrap();
    // Another node of the app reads it.
    let other = with_quota(bucket_with(Arc::clone(&store), DEFAULT_MAX_FILE), 5);
    assert!(!other.blobs.over_quota());
    let read = other.blobs.read_usage().await.unwrap().unwrap();
    assert_eq!((read.bytes, read.files), (6, 1));
    other.blobs.note_usage(&read);
    assert!(other.blobs.over_quota());
    // A count of another epoch (an earlier life of the app) is ignored.
    let stale = with_quota(bucket_with(Arc::clone(&store), DEFAULT_MAX_FILE), 5);
    let mut wrong = read.clone();
    wrong.epoch = "ffffffffffffffff".into();
    stale.blobs.note_usage(&wrong);
    assert!(!stale.blobs.over_quota());
}

type Body = http_body_util::StreamBody<
    futures_util::stream::BoxStream<
        'static,
        Result<hyper::body::Frame<bytes::Bytes>, std::convert::Infallible>,
    >,
>;

/// Files on disk with node links, spooling under budgets of their own.
fn spooling(app: u64, node: &Arc<streams::Budget>) -> (tempfile::TempDir, Arc<Blobs>) {
    let (tmp, blobs) = with_links(disk_with(DEFAULT_MAX_FILE), false);
    let blobs = Arc::into_inner(blobs)
        .unwrap()
        .with_spool_budgets(Arc::new(streams::Budget::new(app)), Arc::clone(node));
    (tmp, Arc::new(blobs))
}

/// An upload link's path for `size` bytes at `key`.
fn upload_path(blobs: &Blobs, key: &str, size: u64) -> String {
    let (url, _) = blobs
        .link_upload(&Namespace::front(), key, size, "image/png", 60)
        .unwrap();
    url.strip_prefix("https://shop.example.com")
        .unwrap()
        .to_owned()
}

fn frame(n: u64) -> Result<hyper::body::Frame<bytes::Bytes>, std::convert::Infallible> {
    Ok(hyper::body::Frame::data(bytes::Bytes::from(vec![
        1u8;
        n as usize
    ])))
}

/// A PUT of all `size` bytes to `path`.
fn put(path: &str, size: u64) -> hyper::Request<Body> {
    use futures_util::StreamExt as _;
    request(
        path,
        size,
        futures_util::stream::iter([frame(size)]).boxed(),
    )
}

/// A PUT of `size` bytes to `path` that sends one and then nothing more,
/// and the signal of its body asked for a second: the first is spooled.
fn put_stalled(
    path: &str,
    size: u64,
) -> (hyper::Request<Body>, tokio::sync::oneshot::Receiver<()>) {
    use futures_util::StreamExt as _;
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut tx = Some(tx);
    let rest = futures_util::stream::poll_fn(move |_| {
        if let Some(tx) = tx.take() {
            let _ = tx.send(());
        }
        std::task::Poll::Pending
    });
    let stream = futures_util::stream::iter([frame(1)]).chain(rest).boxed();
    (request(path, size, stream), rx)
}

fn request(
    path: &str,
    size: u64,
    stream: futures_util::stream::BoxStream<
        'static,
        Result<hyper::body::Frame<bytes::Bytes>, std::convert::Infallible>,
    >,
) -> hyper::Request<Body> {
    hyper::Request::builder()
        .method("PUT")
        .uri(path)
        .header("content-length", size.to_string())
        .header("content-type", "image/png")
        .body(http_body_util::StreamBody::new(stream))
        .unwrap()
}

async fn send(blobs: &Blobs, req: hyper::Request<Body>) -> hyper::StatusCode {
    serve::serve(blobs, false, req).await.status()
}

/// Starts an upload that stalls after its first byte, holding its spool.
async fn stalled(
    blobs: &Arc<Blobs>,
    path: &str,
    size: u64,
    node: &streams::Budget,
) -> tokio::task::JoinHandle<hyper::StatusCode> {
    let before = node.used();
    let (req, spooled) = put_stalled(path, size);
    let b = Arc::clone(blobs);
    let task = tokio::spawn(async move { send(&b, req).await });
    spooled.await.unwrap();
    assert_eq!(
        node.used(),
        before + size,
        "the stalled upload holds its size"
    );
    task
}

#[tokio::test]
async fn an_upload_link_sent_again_while_its_body_arrives_is_refused() {
    let node = Arc::new(streams::Budget::new(1 << 30));
    let (tmp, blobs) = spooling(1 << 30, &node);
    let tmp_dir = tmp.path().join("blobs/00112233aabbccdd/_front/main/0/tmp");
    let spools = || std::fs::read_dir(&tmp_dir).map_or(0, |d| d.count());
    let path = upload_path(&blobs, "u/1", 1_000);
    let first = stalled(&blobs, &path, 1_000, &node).await;
    let replays =
        futures_util::future::join_all((0..8).map(|_| send(&blobs, put(&path, 1_000)))).await;
    assert!(
        replays.iter().all(|s| *s == hyper::StatusCode::CONFLICT),
        "{replays:?}"
    );
    assert_eq!(spools(), 1, "only the first body is spooled");
    assert_eq!(node.used(), 1_000);
    // The first gone (its client went away), the link works again.
    first.abort();
    assert!(first.await.is_err());
    assert_eq!(
        (node.used(), spools()),
        (0, 0),
        "released with the upload: {:?}",
        std::fs::read_dir(&tmp_dir)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        send(&blobs, put(&path, 1_000)).await,
        hyper::StatusCode::CREATED
    );
    assert_eq!(node.used(), 0, "released once committed");
    assert_eq!(
        send(&blobs, put(&path, 1_000)).await,
        hyper::StatusCode::PRECONDITION_FAILED
    );
}

#[tokio::test]
async fn the_spool_budgets_refuse_past_their_limit_and_recover() {
    // The node's budget, shared by two apps.
    let node = Arc::new(streams::Budget::new(1_500));
    let (_a, one) = spooling(1 << 30, &node);
    let (_b, two) = spooling(1 << 30, &node);
    let first = stalled(&one, &upload_path(&one, "a", 1_000), 1_000, &node).await;
    let other = upload_path(&two, "b", 1_000);
    assert_eq!(
        send(&two, put(&other, 1_000)).await,
        hyper::StatusCode::SERVICE_UNAVAILABLE,
        "past the node's budget, before any byte is read"
    );
    assert_eq!(
        send(&two, put(&upload_path(&two, "c", 400), 400)).await,
        hyper::StatusCode::CREATED,
        "what fits is taken"
    );
    first.abort();
    let _ = first.await;
    assert_eq!(node.used(), 0);
    assert_eq!(
        send(&two, put(&other, 1_000)).await,
        hyper::StatusCode::CREATED
    );

    // An app's share, under a node budget with room.
    let node = Arc::new(streams::Budget::new(1 << 30));
    let (_c, app) = spooling(1_500, &node);
    let first = stalled(&app, &upload_path(&app, "a", 1_000), 1_000, &node).await;
    assert_eq!(
        send(&app, put(&upload_path(&app, "b", 1_000), 1_000)).await,
        hyper::StatusCode::SERVICE_UNAVAILABLE
    );
    first.abort();
    let _ = first.await;
    assert_eq!(node.used(), 0);

    // A guest's streamed write is charged as it is written.
    let node = Arc::new(streams::Budget::new(100_000));
    let (_d, blobs) = spooling(1 << 30, &node);
    let f = Fixture {
        _tmp: tempfile::tempdir().unwrap(),
        blobs,
    };
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
        Err(BlobError::Unavailable(_))
    ));
    assert_eq!(node.used(), 0, "given back when the write fails");
    assert!(c.head("big".into()).await.unwrap().is_none());
    let info = write_streamed(&mut c, &mut table, "fits", &[1; 50_000], any())
        .await
        .unwrap();
    assert_eq!((info.size, node.used()), (50_000, 0));
}

#[tokio::test]
async fn a_guest_opens_at_most_16_streamed_writes_at_once() {
    let f = disk_with(DEFAULT_MAX_FILE);
    let mut c = caller(&f, Namespace::front());
    let mut table = ResourceTable::new();
    let mut open = Vec::new();
    for i in 0..MAX_OPEN_WRITES {
        open.push(
            c.open_write(&mut table, format!("k{i}"), any())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert!(matches!(
        c.open_write(&mut table, "one-more".into(), any())
            .await
            .unwrap(),
        Err(BlobError::NotAllowed(_))
    ));
    // One dropped, another may open; one finished, likewise.
    c.drop_upload(&mut table, open.pop().unwrap()).unwrap();
    c.open_write(&mut table, "again".into(), any())
        .await
        .unwrap()
        .unwrap();
    c.finish(&mut table, open.pop().unwrap())
        .await
        .unwrap()
        .unwrap();
    c.open_write(&mut table, "again2".into(), any())
        .await
        .unwrap()
        .unwrap();
    // The next event starts with none open.
    c.new_event(Gate::Free);
    for i in 0..MAX_OPEN_WRITES {
        c.open_write(&mut table, format!("n{i}"), any())
            .await
            .unwrap()
            .unwrap();
    }
}

/// Files on disk with node links and a largest file of `max`, their
/// share taken of `node`.
fn sharing(node: &Arc<streams::Budget>, max: u64) -> (tempfile::TempDir, Arc<Blobs>) {
    let (tmp, blobs) = with_links(disk_with(max), false);
    let blobs = Arc::into_inner(blobs)
        .unwrap()
        .with_node_spool(Arc::clone(node));
    (tmp, Arc::new(blobs))
}

/// A PUT of `size` bytes sent `chunk` bytes every `every`.
fn put_paced(
    path: &str,
    size: u64,
    chunk: u64,
    every: std::time::Duration,
) -> hyper::Request<Body> {
    use futures_util::StreamExt as _;
    let stream = futures_util::stream::unfold(size, move |left| async move {
        if left == 0 {
            return None;
        }
        tokio::time::sleep(every).await;
        let n = left.min(chunk);
        Some((frame(n), left - n))
    })
    .boxed();
    request(path, size, stream)
}

#[tokio::test(start_paused = true)]
async fn a_trickling_upload_is_aborted_and_gives_its_budget_back() {
    let node = Arc::new(streams::Budget::new(1 << 30));
    let (_t, blobs) = spooling(1 << 30, &node);
    let size = 1_000_000;
    // One byte a minute less a second: never idle for 60 s.
    let start = tokio::time::Instant::now();
    let status = send(
        &blobs,
        put_paced(
            &upload_path(&blobs, "slow", size),
            size,
            1,
            std::time::Duration::from_secs(59),
        ),
    )
    .await;
    assert_eq!(status, hyper::StatusCode::REQUEST_TIMEOUT);
    assert!(
        start.elapsed() < std::time::Duration::from_secs(60),
        "aborted by its rate, not after the link's expiry: {:?}",
        start.elapsed()
    );
    assert_eq!(node.used(), 0, "given back");
}

#[tokio::test(start_paused = true)]
async fn a_slow_but_steady_upload_completes() {
    let node = Arc::new(streams::Budget::new(1 << 30));
    let (_t, blobs) = spooling(1 << 30, &node);
    // 20 KiB a second for a minute: a poor mobile link.
    let size = 60 * 20 * 1024;
    let status = send(
        &blobs,
        put_paced(
            &upload_path(&blobs, "steady", size),
            size,
            20 * 1024,
            std::time::Duration::from_secs(1),
        ),
    )
    .await;
    assert_eq!(status, hyper::StatusCode::CREATED);
    assert_eq!(node.used(), 0);
}

#[tokio::test(start_paused = true)]
async fn an_upload_body_that_stops_is_aborted_after_60_s() {
    use futures_util::StreamExt as _;
    let node = Arc::new(streams::Budget::new(1 << 30));
    let (_t, blobs) = spooling(1 << 30, &node);
    // Half the body at once, ahead of any rate, then nothing more.
    let size = 8 << 20;
    let stream = futures_util::stream::iter([frame(size / 2)])
        .chain(futures_util::stream::pending())
        .boxed();
    let start = tokio::time::Instant::now();
    let status = send(
        &blobs,
        request(&upload_path(&blobs, "stops", size), size, stream),
    )
    .await;
    assert_eq!(status, hyper::StatusCode::REQUEST_TIMEOUT);
    let took = start.elapsed();
    assert!(
        took >= std::time::Duration::from_secs(60) && took < std::time::Duration::from_secs(61),
        "{took:?}"
    );
    assert_eq!(node.used(), 0);
}

#[tokio::test]
async fn one_app_cannot_take_the_whole_node_budget() {
    let node = Arc::new(streams::Budget::new(4_000));
    let (_a, one) = sharing(&node, 1_000);
    let (_b, two) = sharing(&node, 1_000);
    // The first app's links take its share, half the node's.
    let first = stalled(&one, &upload_path(&one, "a", 1_000), 1_000, &node).await;
    let second = stalled(&one, &upload_path(&one, "b", 1_000), 1_000, &node).await;
    assert_eq!(
        send(&one, put(&upload_path(&one, "c", 1_000), 1_000)).await,
        hyper::StatusCode::SERVICE_UNAVAILABLE,
        "past the app's share"
    );
    // The other app still has the rest.
    assert_eq!(
        send(&two, put(&upload_path(&two, "a", 1_000), 1_000)).await,
        hyper::StatusCode::CREATED
    );
    for t in [first, second] {
        t.abort();
        let _ = t.await;
    }
    assert_eq!(node.used(), 0);
    // A node budget below twice an app's largest file is raised to it.
    let node = Arc::new(streams::Budget::new(10));
    let (_c, big) = sharing(&node, 1_000);
    let first = stalled(&big, &upload_path(&big, "a", 1_000), 1_000, &node).await;
    let (_d, other) = sharing(&node, 1_000);
    assert_eq!(
        send(&other, put(&upload_path(&other, "a", 1_000), 1_000)).await,
        hyper::StatusCode::CREATED
    );
    first.abort();
    let _ = first.await;
}

/// Descriptors this process holds open on files under `dir`.
#[cfg(target_os = "linux")]
fn fds_under(dir: &std::path::Path) -> usize {
    let dir = dir.canonicalize().unwrap();
    std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(|e| std::fs::read_link(e.ok()?.path()).ok())
        .filter(|target| target.starts_with(&dir))
        .count()
}

async fn open_one(
    c: &mut Caller,
    table: &mut ResourceTable,
) -> Result<wasmtime::component::Resource<wasmtime_wasi::p2::DynInputStream>, BlobError> {
    c.open_read(table, "r".into(), None)
        .await
        .unwrap()
        .map(|found| found.unwrap().1)
}

/// A guest's read streams: at most 16 open at once, a stream read to its
/// end or dropped no longer counts, and the event's end closes the rest
/// (on disk, their descriptors), so none outlives its event in a resident
/// actor's table.
#[tokio::test]
async fn a_guest_opens_at_most_16_read_streams_and_the_event_closes_them() {
    for (i, f) in both().into_iter().enumerate() {
        let on_disk = i == 1;
        let mut c = caller(&f, Namespace::actor("c", "n", "0"));
        let mut table = ResourceTable::new();
        let body = vec![7u8; 300_000];
        write_streamed(&mut c, &mut table, "r", &body, any())
            .await
            .unwrap();
        let mut open = Vec::new();
        for _ in 0..MAX_OPEN_READS {
            open.push(open_one(&mut c, &mut table).await.unwrap());
        }
        #[cfg(target_os = "linux")]
        if on_disk {
            assert_eq!(fds_under(f._tmp.path()), MAX_OPEN_READS);
        }
        assert!(matches!(
            open_one(&mut c, &mut table).await,
            Err(BlobError::NotAllowed(_))
        ));
        // One dropped, another may open.
        table.delete(open.pop().unwrap()).unwrap();
        open.push(open_one(&mut c, &mut table).await.unwrap());
        // One read to its end, but still held: it no longer counts.
        let s = table.get_mut(&open[0]).unwrap();
        let mut read = 0;
        loop {
            s.ready().await;
            match s.read(1 << 20) {
                Ok(b) => read += b.len(),
                Err(wasmtime_wasi::p2::StreamError::Closed) => break,
                Err(e) => panic!("{e}"),
            }
        }
        assert_eq!(read, body.len());
        open.push(open_one(&mut c, &mut table).await.unwrap());
        // The event ends with every stream still in the table: each is
        // closed, and its descriptor with it.
        c.end_event();
        #[cfg(target_os = "linux")]
        assert_eq!(fds_under(f._tmp.path()), 0, "on disk: {on_disk}");
        for s in &open {
            let s = table.get_mut(s).unwrap();
            s.ready().await;
            assert!(matches!(
                s.read(10),
                Err(wasmtime_wasi::p2::StreamError::Closed)
            ));
        }
        // The next event may open 16 again, the old handles still held.
        c.new_event(Gate::Free);
        for _ in 0..MAX_OPEN_READS {
            open_one(&mut c, &mut table).await.unwrap();
        }
    }
}

/// A wait for a read stream that is given up (the guest polled it beside
/// another pollable that was ready first) loses no bytes: the stream
/// carries on where it was.
#[tokio::test]
async fn a_read_whose_wait_was_given_up_loses_nothing() {
    for f in both() {
        let mut c = caller(&f, Namespace::actor("c", "n", "0"));
        let mut table = ResourceTable::new();
        let body: Vec<u8> = (0..500_000u32).map(|i| (i % 253) as u8).collect();
        write_streamed(&mut c, &mut table, "r", &body, any())
            .await
            .unwrap();
        let stream = open_one(&mut c, &mut table).await.unwrap();
        let s = table.get_mut(&stream).unwrap();
        let mut out = Vec::new();
        loop {
            // Polled once, then dropped, before every real wait.
            {
                let mut ready = std::pin::pin!(s.ready());
                let waker = futures_util::task::noop_waker();
                let _ = ready
                    .as_mut()
                    .poll(&mut std::task::Context::from_waker(&waker));
            }
            s.ready().await;
            match s.read(1 << 20) {
                Ok(b) => out.extend_from_slice(&b),
                Err(wasmtime_wasi::p2::StreamError::Closed) => break,
                Err(e) => panic!("{e}"),
            }
        }
        assert_eq!(out.len(), body.len());
        assert!(out == body);
    }
}
