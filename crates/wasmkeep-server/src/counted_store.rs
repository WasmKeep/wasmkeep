//! A bucket wrapper that counts requests by operation and outcome for
//! `wasmkeep_bucket_requests_total`. Every bucket access of a node goes
//! through it: leases, heartbeats, the alarm index, replication, restore and
//! the deployed app it follows.
//!
//! One call is one count. A listing counts once however many pages the
//! store fetches (S3 returns 1,000 keys per page), and a multipart upload
//! counts once when it starts.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt as _;
use futures_util::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
};

use crate::metrics::{METRICS, inc};

/// `store`, with each request counted.
pub(crate) fn counted(store: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    Arc::new(Counted { inner: store })
}

#[derive(Debug)]
struct Counted {
    inner: Arc<dyn ObjectStore>,
}

impl fmt::Display for Counted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

/// The outcome label. Lost races for a conditional write (`precondition`)
/// and missing objects (`not_found`) are how the lease protocol and the
/// readers work, so they are kept apart from failures (`error`).
fn status<T>(result: &Result<T>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(object_store::Error::NotFound { .. }) => "not_found",
        Err(
            object_store::Error::AlreadyExists { .. }
            | object_store::Error::Precondition { .. }
            | object_store::Error::NotModified { .. },
        ) => "precondition",
        Err(_) => "error",
    }
}

fn count(operation: &str, status: &str, took: Duration) {
    let labels = [("operation", operation), ("status", status)];
    inc(&METRICS.bucket_requests.get(&labels));
    METRICS.bucket_request_seconds.get(&labels).observe(took);
}

/// Counts and times the call that began at `started`.
fn record<T>(operation: &str, started: Instant, result: Result<T>) -> Result<T> {
    count(operation, status(&result), started.elapsed());
    result
}

/// Counts a listing once: as an error at its first failed item, otherwise
/// as ok when it ends or is dropped.
fn counted_listing<T: Send + 'static>(
    stream: BoxStream<'static, Result<T>>,
) -> BoxStream<'static, Result<T>> {
    struct Once(Option<&'static str>, Instant);
    impl Drop for Once {
        fn drop(&mut self) {
            count("list", self.0.unwrap_or("ok"), self.1.elapsed());
        }
    }
    let mut once = Once(None, Instant::now());
    stream
        .map(move |item| {
            if once.0.is_none()
                && let Err(_) = &item
            {
                once.0 = Some(status(&item));
            }
            item
        })
        .boxed()
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for Counted {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let started = Instant::now();
        record(
            "put",
            started,
            self.inner.put_opts(location, payload, opts).await,
        )
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let started = Instant::now();
        record(
            "multipart",
            started,
            self.inner.put_multipart_opts(location, opts).await,
        )
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let operation = if options.head { "head" } else { "get" };
        let started = Instant::now();
        record(
            operation,
            started,
            self.inner.get_opts(location, options).await,
        )
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let started = Instant::now();
        record(
            "get",
            started,
            self.inner.get_ranges(location, ranges).await,
        )
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        // Each item is timed from the one before it (the first from the
        // call), so a batched delete's time is shared out, not repeated.
        let mut last = Instant::now();
        self.inner
            .delete_stream(locations)
            .map(move |r| {
                let started = std::mem::replace(&mut last, Instant::now());
                record("delete", started, r)
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        counted_listing(self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        counted_listing(self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let started = Instant::now();
        record(
            "list",
            started,
            self.inner.list_with_delimiter(prefix).await,
        )
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let started = Instant::now();
        record(
            "copy",
            started,
            self.inner.copy_opts(from, to, options).await,
        )
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        let started = Instant::now();
        record(
            "rename",
            started,
            self.inner.rename_opts(from, to, options).await,
        )
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt as _, PutMode};

    use super::*;
    use crate::metrics::{Gauges, render};

    fn line(page: &str, labels: &str) -> u64 {
        let prefix = format!("wasmkeep_bucket_requests_total{{{labels}}} ");
        page.lines()
            .find_map(|l| l.strip_prefix(&prefix))
            .map_or(0, |v| v.parse().unwrap())
    }

    #[tokio::test]
    async fn requests_are_counted_by_operation_and_status() {
        let store = counted(Arc::new(InMemory::new()));
        let key = Path::from("a/b");
        store.put(&key, PutPayload::from("x")).await.unwrap();
        let create = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        assert!(
            store
                .put_opts(&key, PutPayload::from("y"), create)
                .await
                .is_err()
        );
        store.get(&key).await.unwrap();
        store.head(&key).await.unwrap();
        assert!(store.get(&Path::from("missing")).await.is_err());
        let listed: Vec<_> = store.list(Some(&Path::from("a"))).collect().await;
        assert_eq!(listed.len(), 1);
        store.delete(&key).await.unwrap();

        let page = render(Gauges::default());
        assert!(
            page.contains("# TYPE wasmkeep_bucket_requests_total counter\n"),
            "{page}"
        );
        for (labels, n) in [
            ("operation=\"put\",status=\"ok\"", 1),
            ("operation=\"put\",status=\"precondition\"", 1),
            ("operation=\"get\",status=\"ok\"", 1),
            ("operation=\"get\",status=\"not_found\"", 1),
            ("operation=\"head\",status=\"ok\"", 1),
            ("operation=\"list\",status=\"ok\"", 1),
            ("operation=\"delete\",status=\"ok\"", 1),
        ] {
            assert_eq!(line(&page, labels), n, "{labels} in\n{page}");
        }
    }

    /// Each request's time reaches the page, by operation and status.
    #[tokio::test]
    async fn request_latency_reaches_the_page() {
        let store = counted(Arc::new(InMemory::new()));
        let key = Path::from("lat/b");
        store.put(&key, PutPayload::from("x")).await.unwrap();
        assert!(store.get(&Path::from("lat/missing")).await.is_err());
        let page = render(Gauges::default());
        assert!(
            page.contains("# TYPE wasmkeep_bucket_request_seconds histogram\n"),
            "{page}"
        );
        for want in [
            "wasmkeep_bucket_request_seconds_count{operation=\"put\",status=\"ok\"} 1",
            "wasmkeep_bucket_request_seconds_count{operation=\"get\",status=\"not_found\"} 1",
        ] {
            assert!(page.contains(want), "{want} in\n{page}");
        }
    }
}
