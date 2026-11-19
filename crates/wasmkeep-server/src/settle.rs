//! Which of a command's bucket writes may still land.
//!
//! A mutating request that got an answer (a success, or a definite refusal
//! such as a failed precondition) was applied or refused; it cannot land
//! later. One that ended without an answer (a timeout, a transport error, a
//! 5xx, 408 or 429 status, or a future dropped in flight) may still land.
//! [`Settle`] records when the latest such request ended, so a command such
//! as `backup restore` holds its exclusive lock only as long as a request of
//! its own may still land
//! ([`wasmkeep_cluster::restore_fence::release_at_ms`]).
//!
//! Two layers feed one [`Settle`]: [`tracked`], around an [`ObjectStore`],
//! sees every call and a call dropped unfinished; [`observe`], around the
//! HTTP client of an S3 store, sees every attempt, including the ones the
//! store's own retries hide behind a call that succeeds.

use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt as _;
use futures_util::stream::BoxStream;
use object_store::client::{HttpClient, HttpError, HttpRequest, HttpResponse, HttpService};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result, UploadPart,
};

/// The mutating requests of one command, on a monotonic clock that starts
/// with it.
#[derive(Debug)]
pub struct Settle {
    origin: tokio::time::Instant,
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    /// Requests sent and not yet ended.
    pending: usize,
    /// When the latest request without an answer ended.
    unsettled_ms: Option<u64>,
}

impl Settle {
    /// A new record, its clock at zero.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            origin: tokio::time::Instant::now(),
            state: Mutex::new(State::default()),
        })
    }

    /// Milliseconds since [`Self::new`].
    #[must_use]
    pub fn now_ms(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// When the latest request without an answer ended, or now if one is
    /// still in flight; `None` if every request sent was answered.
    #[must_use]
    pub fn unsettled_ms(&self) -> Option<u64> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.pending > 0 {
            return Some(self.now_ms());
        }
        state.unsettled_ms
    }

    fn begin(self: &Arc<Self>) -> Attempt {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).pending += 1;
        Attempt {
            settle: Arc::clone(self),
            ended: false,
        }
    }
}

/// One mutating request in flight. Ending it without [`Attempt::answered`]
/// (dropped) counts it as unanswered.
struct Attempt {
    settle: Arc<Settle>,
    ended: bool,
}

impl Attempt {
    fn answered(mut self, answered: bool) {
        self.end(answered);
    }

    fn end(&mut self, answered: bool) {
        if self.ended {
            return;
        }
        self.ended = true;
        let now = self.settle.now_ms();
        let mut state = self.settle.state.lock().unwrap_or_else(|p| p.into_inner());
        state.pending -= 1;
        if !answered {
            state.unsettled_ms = Some(state.unsettled_ms.map_or(now, |t| t.max(now)));
        }
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        self.end(false);
    }
}

/// Whether a store's result is a definite answer: applied, or refused.
fn answered<T>(result: &Result<T>) -> bool {
    matches!(
        result,
        Ok(_)
            | Err(object_store::Error::NotFound { .. }
                | object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. }
                | object_store::Error::NotModified { .. }
                | object_store::Error::NotImplemented { .. })
    )
}

/// `store`, with every mutating call recorded in `settle`.
pub fn tracked(store: Arc<dyn ObjectStore>, settle: Arc<Settle>) -> Arc<dyn ObjectStore> {
    Arc::new(Tracked {
        inner: store,
        settle,
    })
}

#[derive(Debug)]
struct Tracked {
    inner: Arc<dyn ObjectStore>,
    settle: Arc<Settle>,
}

impl fmt::Display for Tracked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.inner, f)
    }
}

impl Tracked {
    async fn mutation<T>(&self, call: impl Future<Output = Result<T>>) -> Result<T> {
        let attempt = self.settle.begin();
        let result = call.await;
        attempt.answered(answered(&result));
        result
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for Tracked {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.mutation(self.inner.put_opts(location, payload, opts))
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        // Starting an upload creates no object; completing it does.
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(TrackedUpload {
            inner,
            settle: Arc::clone(&self.settle),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        // One attempt for the whole stream: answered only if it ends with
        // every deletion answered; unanswered if dropped before its end.
        let attempt = self.settle.begin();
        futures_util::stream::unfold(
            (self.inner.delete_stream(locations), Some(attempt)),
            |(mut inner, mut attempt)| async move {
                let Some(item) = inner.next().await else {
                    if let Some(a) = attempt.take() {
                        a.answered(true);
                    }
                    return None;
                };
                if !answered(&item)
                    && let Some(a) = attempt.take()
                {
                    a.answered(false);
                }
                Some((item, (inner, attempt)))
            },
        )
        .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.mutation(self.inner.copy_opts(from, to, options)).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.mutation(self.inner.rename_opts(from, to, options))
            .await
    }
}

#[derive(Debug)]
struct TrackedUpload {
    inner: Box<dyn MultipartUpload>,
    settle: Arc<Settle>,
}

#[async_trait]
impl MultipartUpload for TrackedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        // A part is not an object until the upload completes.
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let attempt = self.settle.begin();
        let result = self.inner.complete().await;
        attempt.answered(answered(&result));
        result
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

/// `client`, with every mutating HTTP attempt (any method but GET, HEAD and
/// OPTIONS) recorded in `settle`: answered once a response with a status
/// other than 5xx, 408 or 429 arrived.
#[must_use]
pub fn observe(client: HttpClient, settle: Arc<Settle>) -> HttpClient {
    HttpClient::new(Observed {
        inner: client,
        settle,
    })
}

#[derive(Debug)]
struct Observed {
    inner: HttpClient,
    settle: Arc<Settle>,
}

#[async_trait]
impl HttpService for Observed {
    async fn call(&self, req: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
        use http::{Method, StatusCode};
        if matches!(*req.method(), Method::GET | Method::HEAD | Method::OPTIONS) {
            return self.inner.execute(req).await;
        }
        let attempt = self.settle.begin();
        let result = self.inner.execute(req).await;
        attempt.answered(matches!(&result, Ok(response) if {
            let status = response.status();
            !(status.is_server_error()
                || status == StatusCode::REQUEST_TIMEOUT
                || status == StatusCode::TOO_MANY_REQUESTS)
        }));
        result
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt as _, PutMode};

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn answered_requests_settle_and_unanswered_ones_do_not() {
        let settle = Settle::new();
        let store = tracked(Arc::new(InMemory::new()), Arc::clone(&settle));
        let key = Path::from("a/b");
        store.put(&key, PutPayload::from("x")).await.unwrap();
        let create = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        // A refusal is an answer.
        assert!(
            store
                .put_opts(&key, PutPayload::from("y"), create)
                .await
                .is_err()
        );
        store.delete(&key).await.unwrap();
        store.get(&Path::from("missing")).await.unwrap_err();
        assert_eq!(settle.unsettled_ms(), None);

        // A put dropped in flight may still land.
        tokio::time::advance(std::time::Duration::from_secs(3)).await;
        drop(settle.begin());
        assert_eq!(settle.unsettled_ms(), Some(3_000));
        // One in flight counts as unsettled now.
        let pending = settle.begin();
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        assert_eq!(settle.unsettled_ms(), Some(5_000));
        pending.answered(true);
        assert_eq!(settle.unsettled_ms(), Some(3_000));
    }

    /// Answers each request with the next status.
    #[derive(Debug)]
    struct Statuses(Mutex<Vec<u16>>);

    #[async_trait]
    impl HttpService for Statuses {
        async fn call(&self, _: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
            let status = self.0.lock().unwrap().remove(0);
            Ok(http::Response::builder()
                .status(status)
                .body(object_store::client::HttpResponseBody::from(Vec::new()))
                .unwrap())
        }
    }

    #[tokio::test]
    async fn an_http_attempt_with_a_server_error_is_unsettled_even_if_retried() {
        let request = |method: &str| {
            http::Request::builder()
                .method(method)
                .uri("http://bucket/key")
                .body(object_store::client::HttpRequestBody::from(Vec::new()))
                .unwrap()
        };
        let settle = Settle::new();
        let client = observe(
            HttpClient::new(Statuses(Mutex::new(vec![200, 404, 412, 503, 200]))),
            Arc::clone(&settle),
        );
        // A read never counts; answered writes, refusals included, settle.
        for method in ["GET", "PUT", "DELETE"] {
            client.execute(request(method)).await.unwrap();
        }
        assert_eq!(settle.unsettled_ms(), None);
        // A 503 then a retry that succeeds, as the S3 client does on its own.
        client.execute(request("PUT")).await.unwrap();
        client.execute(request("PUT")).await.unwrap();
        assert!(settle.unsettled_ms().is_some());
        for method in ["POST", "PUT", "DELETE"] {
            let fresh = Settle::new();
            let client = observe(
                HttpClient::new(Statuses(Mutex::new(vec![match method {
                    "POST" => 500,
                    "PUT" => 408,
                    _ => 429,
                }]))),
                Arc::clone(&fresh),
            );
            client.execute(request(method)).await.unwrap();
            assert!(fresh.unsettled_ms().is_some(), "{method}");
        }
    }

    #[tokio::test]
    async fn a_deletion_stream_dropped_before_its_end_is_unsettled() {
        let settle = Settle::new();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for k in ["a", "b"] {
            inner
                .put(&Path::from(k), PutPayload::from("x"))
                .await
                .unwrap();
        }
        let store = tracked(inner, Arc::clone(&settle));
        let keys = futures_util::stream::iter([Ok(Path::from("a")), Ok(Path::from("b"))]).boxed();
        let all: Vec<_> = store.delete_stream(keys).collect().await;
        assert_eq!(all.len(), 2);
        assert_eq!(settle.unsettled_ms(), None);
        let keys = futures_util::stream::iter([Ok(Path::from("a")), Ok(Path::from("b"))]).boxed();
        let mut stream = store.delete_stream(keys);
        stream.next().await.unwrap().unwrap();
        drop(stream);
        assert!(settle.unsettled_ms().is_some());
    }
}
