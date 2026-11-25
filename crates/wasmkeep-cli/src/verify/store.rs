//! The bucket as `cluster verify` sees it: reads and listings pass and are
//! counted, every write is refused before it reaches the bucket.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use futures_util::StreamExt as _;
use futures_util::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

/// Keys a listing page holds on S3: a listing is counted as one request,
/// and one more for every this many keys it returned.
const PAGE: u64 = 1_000;

/// What a run asked of the bucket.
#[derive(Debug, Default)]
pub(super) struct Counts {
    /// GET requests sent, answered or not.
    gets: AtomicU64,
    /// GET requests the bucket answered with an object.
    objects: AtomicU64,
    /// The sizes of those objects.
    bytes: AtomicU64,
    /// Listing requests: one per listing and one per [`PAGE`] keys.
    lists: AtomicU64,
    /// Keys the listings returned.
    listed: AtomicU64,
    /// Writes refused. Nothing in `cluster verify` writes: always 0.
    writes: AtomicU64,
}

impl Counts {
    pub(super) fn objects(&self) -> u64 {
        self.objects.load(Ordering::Relaxed)
    }
    pub(super) fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
    pub(super) fn gets(&self) -> u64 {
        self.gets.load(Ordering::Relaxed)
    }
    pub(super) fn lists(&self) -> u64 {
        self.lists.load(Ordering::Relaxed)
    }
    pub(super) fn listed(&self) -> u64 {
        self.listed.load(Ordering::Relaxed)
    }
    pub(super) fn writes(&self) -> u64 {
        self.writes.load(Ordering::Relaxed)
    }
}

/// `inner`, read-only and counted.
#[derive(Debug)]
pub(super) struct ReadOnly {
    inner: Arc<dyn ObjectStore>,
    counts: Arc<Counts>,
}

impl ReadOnly {
    pub(super) fn wrap(inner: Arc<dyn ObjectStore>) -> (Arc<dyn ObjectStore>, Arc<Counts>) {
        let counts = Arc::new(Counts::default());
        let store: Arc<dyn ObjectStore> = Arc::new(Self {
            inner,
            counts: Arc::clone(&counts),
        });
        (store, counts)
    }

    fn refuse<T>(&self, what: &'static str) -> Result<T> {
        self.counts.writes.fetch_add(1, Ordering::Relaxed);
        Err(object_store::Error::NotSupported {
            source: format!("cluster verify is read-only: refused a {what}").into(),
        })
    }

    fn counted(
        &self,
        listing: BoxStream<'static, Result<ObjectMeta>>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.counts.lists.fetch_add(1, Ordering::Relaxed);
        let counts = Arc::clone(&self.counts);
        listing
            .inspect(move |item| {
                if item.is_ok() {
                    let listed = counts.listed.fetch_add(1, Ordering::Relaxed) + 1;
                    if listed.is_multiple_of(PAGE) {
                        counts.lists.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
            .boxed()
    }
}

impl fmt::Display for ReadOnly {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

#[async_trait]
impl ObjectStore for ReadOnly {
    async fn put_opts(&self, _: &Path, _: PutPayload, _: PutOptions) -> Result<PutResult> {
        self.refuse("PUT")
    }

    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.refuse("multipart upload")
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.counts.gets.fetch_add(1, Ordering::Relaxed);
        let got = self.inner.get_opts(location, options).await?;
        self.counts.objects.fetch_add(1, Ordering::Relaxed);
        self.counts
            .bytes
            .fetch_add(got.meta.size, Ordering::Relaxed);
        Ok(got)
    }

    fn delete_stream(
        &self,
        _: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let refused = self.refuse("DELETE");
        futures_util::stream::once(async move { refused }).boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.counted(self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.counted(self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.counts.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
        self.refuse("COPY")
    }
}
