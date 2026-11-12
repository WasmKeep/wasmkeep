use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use wasmkeep_runtime::{CpuBudgetExceeded, Runtime};
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};
use wasmtime_wasi_http::p2::bindings::ProxyPre;
use wasmtime_wasi_http::p2::bindings::http::types::Scheme;
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpCtxView, WasiHttpView};

use crate::cluster_tls::pinning::PinSet;
use crate::{ActorService, Error};

/// How long in-flight requests get to finish after shutdown is requested.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// Per-request host state stored in the Wasmtime `Store`.
///
/// Guests get no stdio, environment, arguments or preopened directories:
/// capabilities are granted explicitly, never inherited from the host.
struct HostState {
    wasi: WasiCtx,
    http: WasiHttpCtx,
    table: ResourceTable,
    /// Outbound HTTP under the egress policy.
    egress: crate::egress::EgressHooks,
}

impl HostState {
    fn new(egress: &Arc<crate::egress::EgressPolicy>, log: &crate::guest_log::GuestLog) -> Self {
        Self {
            wasi: log.wasi("stateless".to_owned()),
            http: WasiHttpCtx::new(),
            table: ResourceTable::new(),
            egress: crate::egress::EgressHooks::new(Arc::clone(egress)),
        }
    }
}

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl WasiHttpView for HostState {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.http,
            table: &mut self.table,
            hooks: &mut self.egress,
        }
    }
}

/// A `wasi:http/proxy` component, linked and pre-instantiated, ready to
/// handle requests. Each request gets a fresh instance. Cheap to clone.
#[derive(Clone)]
pub struct HttpComponent {
    runtime: Runtime,
    pre: ProxyPre<HostState>,
    egress: Arc<crate::egress::EgressPolicy>,
    /// Where instances' stdout and stderr go.
    guest_log: crate::guest_log::GuestLog,
    /// Wall-clock limit per request, including streaming the response
    /// and waiting on I/O (a slow client must not hold an instance).
    wall: Duration,
}

impl std::fmt::Debug for HttpComponent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpComponent").finish_non_exhaustive()
    }
}

impl HttpComponent {
    /// Links `component` against the WASI 0.2 and `wasi:http` host
    /// interfaces and checks that it exports the incoming handler.
    pub fn new(runtime: Runtime, component: &Component) -> Result<Self, Error> {
        let mut linker = Linker::<HostState>::new(runtime.engine());
        wasmtime_wasi::p2::add_to_linker_async(&mut linker).map_err(Error::linker)?;
        wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker)
            .map_err(Error::linker)?;
        let instance_pre = runtime
            .instantiate_pre(&linker, component)
            .map_err(Error::not_http)?;
        let pre =
            ProxyPre::new(instance_pre).map_err(|e| Error::not_http(e.into_boxed_dyn_error()))?;
        Ok(Self {
            runtime,
            pre,
            egress: Arc::default(),
            guest_log: crate::guest_log::GuestLog::default(),
            wall: Duration::from_secs(30),
        })
    }

    /// Lets the component's outbound HTTP reach what `policy` allows (by
    /// default: public addresses only).
    #[must_use]
    pub fn with_egress(mut self, policy: crate::egress::EgressPolicy) -> Self {
        self.egress = Arc::new(policy);
        self
    }

    /// Logs what the component prints within `log`'s limits (by default
    /// [`crate::guest_log::DEFAULT_RATE`] lines a second per instance).
    #[must_use]
    pub fn with_guest_log(mut self, log: crate::guest_log::GuestLog) -> Self {
        self.guest_log = log;
        self
    }

    /// Creates and drops one instance without calling it: proves the
    /// component instantiates (start functions, pool capacity) and is the
    /// per-request instantiation cost, minus the request itself.
    pub async fn instantiate_once(&self) -> Result<(), Error> {
        let mut store = self
            .runtime
            .new_async_store(HostState::new(&self.egress, &self.guest_log));
        self.pre
            .instantiate_async(&mut store)
            .await
            .map(drop)
            .map_err(|e| Error::not_http(e.into_boxed_dyn_error()))
    }

    /// Handles one request in a fresh instance. Guest failures (traps, no
    /// response set) become a 500 response, never an error for the caller.
    pub async fn handle(&self, req: Request<Incoming>) -> Response<HyperOutgoingBody> {
        match self.try_handle(req).await {
            Ok(response) => response,
            Err(e) => {
                let reason = if e.downcast_ref::<CpuBudgetExceeded>().is_some() {
                    "cpu_budget"
                } else {
                    "guest_error"
                };
                tracing::warn!(reason, error = %format!("{e:#}"), "request failed");
                internal_error()
            }
        }
    }

    async fn try_handle(
        &self,
        req: Request<Incoming>,
    ) -> wasmtime::Result<Response<HyperOutgoingBody>> {
        let mut store = self
            .runtime
            .new_async_store(HostState::new(&self.egress, &self.guest_log));
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let req = store
            .data_mut()
            .http()
            .new_incoming_request(Scheme::Http, CappedBody::of(req))?;
        let out = store.data_mut().http().new_response_outparam(sender)?;
        let pre = self.pre.clone();

        // The guest may keep streaming the body after it has set the
        // response, so it runs in its own task.
        let wall = self.wall;
        let task = tokio::task::spawn(async move {
            let run = async move {
                let proxy = pre.instantiate_async(&mut store).await?;
                proxy
                    .wasi_http_incoming_handler()
                    .call_handle(store, req, out)
                    .await
            };
            // Past the wall the instance is dropped, which also ends a
            // response still streaming.
            tokio::time::timeout(wall, run)
                .await
                .unwrap_or_else(|_| Err(wasmtime::format_err!("request ran past {wall:?}")))
        });

        let Ok(received) = tokio::time::timeout(wall, receiver).await else {
            return Err(wasmtime::format_err!("no response within {wall:?}"));
        };
        match received {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(code)) => Err(code.into()),
            // The sender was dropped with the store: the guest returned or
            // trapped without setting a response.
            Err(_) => {
                let cause = match task.await {
                    Ok(Ok(())) => wasmtime::format_err!("guest returned without a response"),
                    Ok(Err(e)) => e,
                    Err(join) => join.into(),
                };
                Err(cause.context("guest did not set a response"))
            }
        }
    }
}

fn internal_error() -> Response<HyperOutgoingBody> {
    text_response(StatusCode::INTERNAL_SERVER_ERROR, "internal server error\n")
}

/// A plain-text response generated by the host.
pub(crate) fn text_response(status: StatusCode, text: &str) -> Response<HyperOutgoingBody> {
    let body = Full::new(Bytes::copy_from_slice(text.as_bytes()))
        .map_err(|never| match never {})
        .boxed_unsync();
    let mut response = Response::new(body);
    *response.status_mut() = status;
    response.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

/// What a server serves: actors under `/actor/`, and optionally a stateless
/// component for every other path.
#[derive(Debug, Clone, Default)]
pub struct App {
    /// Handles paths outside `/actor/`, one instance per request.
    pub stateless: Option<HttpComponent>,
    /// Handles `/actor/<class>/<name>/...`.
    pub actors: Option<ActorService>,
}

impl From<HttpComponent> for App {
    fn from(component: HttpComponent) -> Self {
        Self {
            stateless: Some(component),
            actors: None,
        }
    }
}

impl From<ActorService> for App {
    fn from(actors: ActorService) -> Self {
        Self {
            stateless: None,
            actors: Some(actors),
        }
    }
}

/// Answered by the server itself, for load balancers and orchestrators:
/// `200 ok` while the process accepts requests. Paths under `/_wasmkeep/` are
/// reserved; they never reach a component.
pub const HEALTH_PATH: &str = "/_wasmkeep/health";

/// `200 ready` when the node should get traffic, `503 not ready` otherwise
/// (shutting down, bucket unreachable, disk nearly full: the reason is on the
/// metrics page); for load balancers that should route around a sick node.
pub const READY_PATH: &str = "/_wasmkeep/ready";

impl App {
    async fn route(&self, mut req: Request<Incoming>) -> Response<HyperOutgoingBody> {
        if req.uri().path().starts_with("/_wasmkeep/") {
            let get = matches!(*req.method(), hyper::Method::GET | hyper::Method::HEAD);
            return match req.uri().path() {
                HEALTH_PATH if get => text_response(StatusCode::OK, "ok\n"),
                READY_PATH if get => match self.actors.as_ref().and_then(ActorService::not_ready) {
                    None => text_response(StatusCode::OK, "ready\n"),
                    // Why stays on the private metrics page: this port faces
                    // clients.
                    Some(why) => {
                        tracing::debug!(reason = why, "not ready");
                        text_response(StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
                    }
                },
                _ => text_response(StatusCode::NOT_FOUND, "not found\n"),
            };
        }
        if declares_too_large(&req) {
            return text_response(StatusCode::PAYLOAD_TOO_LARGE, "request body too large\n");
        }
        if let Some(actors) = &self.actors
            && let Some(target) = ActorTarget::parse(req.uri())
        {
            let Some(target) = target else {
                return text_response(StatusCode::BAD_REQUEST, "bad actor path\n");
            };
            let original = OriginalPath(
                req.uri()
                    .path_and_query()
                    .map_or("/", |p| p.as_str())
                    .to_owned(),
            );
            req.extensions_mut().insert(original);
            match target.rest.parse::<hyper::Uri>() {
                Ok(uri) => *req.uri_mut() = uri,
                Err(_) => return text_response(StatusCode::BAD_REQUEST, "bad actor path\n"),
            }
            if req.headers().contains_key(hyper::header::UPGRADE) {
                return actors.upgrade(&target.class, &target.name, req).await;
            }
            return actors.handle(&target.class, &target.name, req).await;
        }
        match &self.stateless {
            Some(component) => component.handle(req).await,
            None => text_response(StatusCode::NOT_FOUND, "not found\n"),
        }
    }
}

/// The path and query a request arrived with, before routing rewrote it to
/// what the actor sees; forwarding to another node uses it.
#[derive(Debug, Clone)]
pub(crate) struct OriginalPath(pub(crate) String);

/// `/actor/<class>/<name><rest>` split into its parts; `name` percent-decoded,
/// `rest` (path and query) is what the actor sees, `/` at least.
#[derive(Debug, PartialEq, Eq)]
struct ActorTarget {
    class: String,
    name: String,
    rest: String,
}

impl ActorTarget {
    /// `None` if the path is not under `/actor/`; `Some(None)` if it is but
    /// is malformed.
    fn parse(uri: &hyper::Uri) -> Option<Option<Self>> {
        let tail = uri.path().strip_prefix("/actor/")?;
        Some((|| {
            let (class, tail) = tail.split_once('/')?;
            let (name, path) = match tail.find('/') {
                Some(i) => (&tail[..i], &tail[i..]),
                None => (tail, "/"),
            };
            if class.is_empty() || name.is_empty() {
                return None;
            }
            let rest = match uri.query() {
                Some(q) => format!("{path}?{q}"),
                None => path.to_owned(),
            };
            Some(Self {
                class: class.to_owned(),
                name: percent_decode(name)?,
                rest,
            })
        })())
    }
}

/// Decodes `%XX` escapes; `None` if an escape is malformed or the result is
/// not UTF-8.
pub(crate) fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = s.get(i + 1..i + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Largest request body a component or actor receives (front door and
/// forwarded requests alike); a larger one fails the guest's read.
pub const MAX_BODY_BYTES: u64 = 16 * 1024 * 1024;

/// How long the rest of a request body is still read after its reader let
/// go of it (see [`CappedBody`]).
const UNREAD_BODY_TIMEOUT: Duration = Duration::from_secs(30);

/// A request body that stops at `left` bytes, failing like an oversized
/// body in wasi-http.
///
/// A handler may answer without reading the body, or after reading part
/// of it. hyper closes a connection whose request body nobody reads any
/// more, and a sender still writing that body (a client, or the node
/// forwarding the request) gets a reset, which can cost it the answer.
/// So what is left is read and discarded once the body is dropped: up to
/// the same cap, for at most [`UNREAD_BODY_TIMEOUT`]. A body that failed
/// or went over the cap is not read further.
///
/// Nor is one whose sender waits for `100 Continue` and was never asked
/// for it: hyper sends the 100 when the body is first read, so reading it
/// here would make a client send a body nobody wants. Left alone, that
/// client gets the answer without the 100 and sends nothing.
pub(crate) struct CappedBody {
    inner: Option<UnsyncBoxBody<Bytes, wasmtime_wasi_http::Error>>,
    left: u64,
    /// The sender waits for `100 Continue`, and nothing has read the body.
    awaits_continue: bool,
}

impl CappedBody {
    pub(crate) fn new<B>(inner: B) -> Self
    where
        B: hyper::body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<wasmtime_wasi_http::Error>,
    {
        Self {
            inner: Some(inner.map_err(Into::into).boxed_unsync()),
            left: MAX_BODY_BYTES,
            awaits_continue: false,
        }
    }

    /// `req` with its body capped.
    pub(crate) fn of<B>(req: Request<B>) -> Request<Self>
    where
        B: hyper::body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<wasmtime_wasi_http::Error>,
    {
        // When hyper holds the body back until it is read (it does not
        // for HTTP/1.0).
        let awaits_continue = req.version() > hyper::Version::HTTP_10
            && req
                .headers()
                .get(hyper::header::EXPECT)
                .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"100-continue"));
        req.map(|body| {
            let mut body = Self::new(body);
            body.awaits_continue = awaits_continue;
            body
        })
    }
}

impl hyper::body::Body for CappedBody {
    type Data = Bytes;
    type Error = wasmtime_wasi_http::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, Self::Error>>> {
        use std::task::Poll;
        self.awaits_continue = false;
        let Some(inner) = self.inner.as_mut() else {
            return Poll::Ready(None);
        };
        match std::pin::Pin::new(inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                let len = frame.data_ref().map_or(0, |d| d.len() as u64);
                if len > self.left {
                    self.left = 0;
                    self.inner = None;
                    return Poll::Ready(Some(Err(wasmtime_wasi_http::Error::HttpRequestBodySize(
                        Some(MAX_BODY_BYTES),
                    ))));
                }
                self.left -= len;
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(e))) => {
                self.inner = None;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                self.inner = None;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner
            .as_ref()
            .is_none_or(hyper::body::Body::is_end_stream)
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner
            .as_ref()
            .map_or_else(|| hyper::body::SizeHint::with_exact(0), |b| b.size_hint())
    }
}

impl Drop for CappedBody {
    fn drop(&mut self) {
        let Some(mut inner) = self.inner.take() else {
            return;
        };
        if self.awaits_continue || hyper::body::Body::is_end_stream(&inner) {
            return;
        }
        // Outside a runtime nothing can read on: the connection closes as
        // it would have.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut left = self.left;
        runtime.spawn(async move {
            let rest = async {
                while let Some(Ok(frame)) = inner.frame().await {
                    let len = frame.data_ref().map_or(0, |d| d.len() as u64);
                    if len > left {
                        break;
                    }
                    left -= len;
                }
            };
            let _ = tokio::time::timeout(UNREAD_BODY_TIMEOUT, rest).await;
        });
    }
}

/// Whether a request declares a body larger than [`MAX_BODY_BYTES`].
pub(crate) fn declares_too_large<B>(req: &Request<B>) -> bool {
    req.headers()
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
        .is_some_and(|n| n > MAX_BODY_BYTES)
}

/// Connection limits for [`serve_with`].
#[derive(Debug, Clone)]
pub struct ServeLimits {
    /// Connections open at once; more wait in the kernel's backlog.
    pub max_connections: usize,
    /// Time a client has to send a request's headers, also between
    /// requests on a kept-alive connection (slow clients, idle sockets).
    pub header_read_timeout: Duration,
    /// Wall-clock limit for a stateless component's request.
    pub stateless_wall: Duration,
    /// Connections the cluster port holds at once, when it is served
    /// ([`serve_with_cluster_listener`]); more wait in the kernel's backlog.
    pub cluster_connections: usize,
}

impl Default for ServeLimits {
    fn default() -> Self {
        Self {
            max_connections: 10_000,
            header_read_timeout: Duration::from_secs(30),
            stateless_wall: Duration::from_secs(30),
            cluster_connections: crate::fds::CLUSTER_CONNECTIONS,
        }
    }
}

/// Serves HTTP/1.1 on `listener` until `shutdown` resolves, then stops
/// accepting and gives in-flight requests up to 10 s to finish. Uses
/// [`ServeLimits::default`].
pub async fn serve(
    listener: TcpListener,
    app: impl Into<App>,
    shutdown: impl Future<Output = ()>,
) -> Result<(), Error> {
    serve_with(listener, app, shutdown, ServeLimits::default()).await
}

/// [`serve`] with explicit limits.
///
/// # Errors
/// The app's cluster has a cluster listener configured
/// ([`crate::ClusterConfig::cluster_tls`]): serve it with
/// [`serve_with_cluster_listener`] instead, or peers would find nothing
/// at the address it publishes.
pub async fn serve_with(
    listener: TcpListener,
    app: impl Into<App>,
    shutdown: impl Future<Output = ()>,
    limits: ServeLimits,
) -> Result<(), Error> {
    let app = app.into();
    if app
        .actors
        .as_ref()
        .is_some_and(|a| a.cluster_tls().is_some())
    {
        return Err(Error::ClusterTls(
            "a cluster listener is configured but not served".into(),
        ));
    }
    serve_with_cluster(listener, None, app, shutdown, limits).await
}

/// [`serve_with`], and the cluster listener on `cluster_listener`: mutual
/// TLS 1.3 with the certificates other nodes published in their heartbeats
/// (ADR 0011), serving actor paths and the health and readiness checks
/// only. One shutdown stops both.
///
/// # Errors
/// The app has no actors in a cluster with a cluster listener configured
/// ([`crate::ClusterConfig::cluster_tls`]).
pub async fn serve_with_cluster_listener(
    listener: TcpListener,
    cluster_listener: TcpListener,
    app: impl Into<App>,
    shutdown: impl Future<Output = ()>,
    limits: ServeLimits,
) -> Result<(), Error> {
    let app = app.into();
    let Some(tls) = app.actors.as_ref().and_then(ActorService::cluster_tls) else {
        return Err(Error::ClusterTls(
            "no cluster listener configured for this node".into(),
        ));
    };
    let port = ClusterPort {
        listener: cluster_listener,
        tls: tls.server_config().map_err(Error::ClusterTls)?,
        pins: Arc::clone(tls.pins()),
        handshake_timeout: CLUSTER_HANDSHAKE_TIMEOUT,
    };
    serve_with_cluster(listener, Some(port), app, shutdown, limits).await
}

/// The cluster listener (ADR 0011): TLS 1.3 with a pinned client
/// certificate (`cluster_tls::pinning::server_config`), for other nodes.
pub(crate) struct ClusterPort {
    pub(crate) listener: TcpListener,
    pub(crate) tls: Arc<rustls::ServerConfig>,
    /// The live nodes' pins (those `tls` trusts): checked again on every
    /// request, so a kept-alive connection from a node whose heartbeat
    /// expired (or whose pin changed) is refused too.
    pub(crate) pins: Arc<PinSet>,
    /// Time a peer has to complete its handshake.
    pub(crate) handshake_timeout: Duration,
}

/// A connection on the cluster port: the pin of the certificate its peer
/// presented (hashed once, at the handshake), and the pins it must still
/// be among.
#[derive(Clone)]
pub(crate) struct ClusterPeer {
    pin: crate::cluster_tls::pinning::Pin32,
    pins: Arc<PinSet>,
}

impl ClusterPeer {
    /// Whether the peer's pin is still live.
    pub(crate) fn is_live(&self) -> bool {
        self.pins.node_of_pin(&self.pin).is_some()
    }
}

/// How long a peer has to finish its TLS handshake on the cluster port.
pub(crate) const CLUSTER_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Marks a request that arrived on the cluster port: it is never
/// forwarded again (ADR 0011). Carries the peer, so a connection that
/// outlives the request (an upgraded WebSocket) can be cut once the peer
/// stops being trusted.
#[derive(Clone)]
pub(crate) struct ArrivedOnClusterPort(pub(crate) ClusterPeer);

/// What the cluster port serves: actor paths, and the health and readiness
/// checks; nothing else (no stateless component, no metrics).
fn cluster_serves(path: &str) -> bool {
    path.starts_with("/actor/") || path == HEALTH_PATH || path == READY_PATH
}

/// Serves the public `listener` and, if given, the cluster port, until
/// `shutdown`: one shutdown for both, which stops accepting on both, lets
/// open connections finish (up to the grace period), then stops the
/// actors.
pub(crate) async fn serve_with_cluster(
    listener: TcpListener,
    cluster: Option<ClusterPort>,
    app: impl Into<App>,
    shutdown: impl Future<Output = ()>,
    limits: ServeLimits,
) -> Result<(), Error> {
    let mut app = app.into();
    if let Some(stateless) = app.stateless.as_mut() {
        stateless.wall = limits.stateless_wall;
    }
    let slots = Arc::new(tokio::sync::Semaphore::new(limits.max_connections.max(1)));
    let mut accept_errors = 0u32;
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let mut connections = tokio::task::JoinSet::new();
    let mut shutdown = std::pin::pin!(shutdown);
    let cluster_loop = cluster.map(|port| {
        tokio::spawn(serve_cluster_port(
            port,
            app.clone(),
            limits.header_read_timeout,
            limits.cluster_connections,
            stop_rx.clone(),
        ))
    });

    loop {
        // Reap finished connection tasks so the set does not grow.
        while connections.try_join_next().is_some() {}

        // A slot first: at the cap, new clients wait in the backlog
        // instead of each taking a file descriptor.
        let slot = tokio::select! {
            slot = Arc::clone(&slots).acquire_owned() => match slot {
                Ok(slot) => slot,
                Err(_) => break,
            },
            () = &mut shutdown => break,
        };
        let (stream, peer) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(conn) => {
                    accept_errors = 0;
                    conn
                }
                Err(e) => {
                    // Per-connection accept errors (e.g. EMFILE) must not
                    // kill the server, nor spin it: back off, and log the
                    // first of a run and then every hundredth.
                    if accept_errors.is_multiple_of(100) {
                        tracing::warn!(error = %e, "accept failed");
                    }
                    accept_errors = accept_errors.saturating_add(1);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            () = &mut shutdown => break,
        };

        // Responses are streamed in several writes (headers, chunks, the
        // final chunk); Nagle would hold the last one back for an ACK.
        if let Err(e) = stream.set_nodelay(true) {
            tracing::debug!(%peer, error = %e, "set_nodelay failed");
        }
        let (app, stop, timeout) = (app.clone(), stop_rx.clone(), limits.header_read_timeout);
        connections.spawn(async move {
            let _slot = slot;
            serve_connection(stream, app, None, timeout, stop, peer).await;
        });
    }

    // Not ready from now on, for load balancers still checking.
    if let Some(actors) = &app.actors {
        actors.begin_shutdown();
    }
    drop(listener);
    let _ = stop_tx.send(true);
    // The cluster task's handle stays here, outside the timeout: dropped
    // with it, the task would be detached and outlive `serve`.
    let mut cluster_loop = cluster_loop;
    let drained = tokio::time::timeout(SHUTDOWN_GRACE, async {
        while connections.join_next().await.is_some() {}
        if let Some(cluster_loop) = cluster_loop.as_mut() {
            let _ = cluster_loop.await;
        }
    })
    .await;
    if drained.is_err() {
        tracing::warn!("shutdown grace period elapsed with connections still open");
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        // Aborting the cluster task drops its connection set, which aborts
        // (and so closes) every connection still open on the cluster port.
        if let Some(cluster_loop) = cluster_loop {
            cluster_loop.abort();
            let _ = cluster_loop.await;
        }
    }
    // Then stop the actors, releasing their leases.
    if let Some(actors) = &app.actors {
        actors.shutdown(SHUTDOWN_GRACE).await;
    }
    Ok(())
}

/// The cluster port's accept loop, until `stop`: each connection completes
/// its TLS handshake (within the timeout, in its own task, so a slow peer
/// holds back nobody), then is served like a public one, limited to
/// `cluster_serves` paths. Returns once its connections have finished.
async fn serve_cluster_port(
    port: ClusterPort,
    app: App,
    header_read_timeout: Duration,
    max_connections: usize,
    stop: tokio::sync::watch::Receiver<bool>,
) {
    let slots = Arc::new(tokio::sync::Semaphore::new(max_connections.max(1)));
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::clone(&port.tls));
    let mut connections = tokio::task::JoinSet::new();
    let mut stopped = stop.clone();
    let mut accept_errors = 0u32;
    loop {
        while connections.try_join_next().is_some() {}
        let slot = tokio::select! {
            slot = Arc::clone(&slots).acquire_owned() => match slot {
                Ok(slot) => slot,
                Err(_) => break,
            },
            () = async { let _ = stopped.wait_for(|s| *s).await; } => break,
        };
        let (stream, peer) = tokio::select! {
            accepted = port.listener.accept() => match accepted {
                Ok(conn) => {
                    accept_errors = 0;
                    conn
                }
                Err(e) => {
                    if accept_errors.is_multiple_of(100) {
                        tracing::warn!(error = %e, "cluster port: accept failed");
                    }
                    accept_errors = accept_errors.saturating_add(1);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            () = async { let _ = stopped.wait_for(|s| *s).await; } => break,
        };
        if let Err(e) = stream.set_nodelay(true) {
            tracing::debug!(%peer, error = %e, "set_nodelay failed");
        }
        let (acceptor, app, stop, pins) = (
            acceptor.clone(),
            app.clone(),
            stop.clone(),
            Arc::clone(&port.pins),
        );
        let handshake_timeout = port.handshake_timeout;
        connections.spawn(async move {
            let _slot = slot;
            let tls = match tokio::time::timeout(handshake_timeout, acceptor.accept(stream)).await {
                Ok(Ok(tls)) => tls,
                Ok(Err(e)) => {
                    tracing::debug!(%peer, error = %e, "cluster port: handshake refused");
                    return;
                }
                Err(_) => {
                    tracing::debug!(%peer, "cluster port: handshake timed out");
                    return;
                }
            };
            // Mandatory client authentication: a certificate is there.
            let Some(pin) = tls
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|c| c.first())
                .map(|c| crate::cluster_tls::pinning::fingerprint(c))
            else {
                return;
            };
            let cluster = ClusterPeer { pin, pins };
            serve_connection(tls, app, Some(cluster), header_read_timeout, stop, peer).await;
        });
    }
    drop(port.listener);
    // Connections watch `stop` themselves and finish gracefully.
    while connections.join_next().await.is_some() {}
}

/// Serves one connection with HTTP/1.1 (upgrades included) until it closes,
/// or, once `stop` turns true, until its in-flight requests finish.
/// `cluster`: it arrived on the cluster port (paths limited, requests
/// marked, the peer's pin checked again on each request).
async fn serve_connection<I>(
    io: I,
    app: App,
    cluster: Option<ClusterPeer>,
    header_read_timeout: Duration,
    mut stop: tokio::sync::watch::Receiver<bool>,
    peer: std::net::SocketAddr,
) where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let service = service_fn(move |mut req: Request<Incoming>| {
        let (app, cluster) = (app.clone(), cluster.clone());
        async move {
            let Some(cluster) = cluster else {
                return Ok::<_, std::convert::Infallible>(app.route(req).await);
            };
            if !cluster.is_live() {
                // Its heartbeat expired or its pin changed since the
                // handshake: no more requests on this connection.
                return Ok(not_live());
            }
            if !cluster_serves(req.uri().path()) {
                return Ok(text_response(StatusCode::NOT_FOUND, "not found\n"));
            }
            req.extensions_mut()
                .insert(ArrivedOnClusterPort(cluster.clone()));
            let response = app.route(req).await;
            // Nor is an answer delivered to a peer whose pin lapsed while
            // its request ran, or streamed on after that. The connection
            // stays: its next request is refused, and closes it.
            if !cluster.is_live() {
                return Ok(text_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "the node's certificate stopped being live during the request\n",
                ));
            }
            Ok(response.map(|body| {
                crate::cluster_tls::revocable_body(body, Arc::new(move || cluster.is_live()))
            }))
        }
    });
    // `with_upgrades` hands WebSocket upgrades over to their own task.
    let conn = http1::Builder::new()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(header_read_timeout)
        .keep_alive(true)
        .serve_connection(TokioIo::new(io), service)
        .with_upgrades();
    let mut conn = std::pin::pin!(conn);
    let result = tokio::select! {
        r = conn.as_mut() => r,
        () = async { let _ = stop.wait_for(|s| *s).await; } => {
            // Finish in-flight requests, then close.
            conn.as_mut().graceful_shutdown();
            conn.await
        }
    };
    if let Err(e) = result {
        tracing::debug!(%peer, error = %e, "connection error");
    }
}

/// The cluster port's answer to a peer whose pin is no longer live: 503,
/// and the connection closed.
fn not_live() -> Response<HyperOutgoingBody> {
    let mut r = text_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "not a live node's certificate\n",
    );
    r.headers_mut().insert(
        hyper::header::CONNECTION,
        hyper::header::HeaderValue::from_static("close"),
    );
    r
}

/// Serves `GET /metrics` (Prometheus text format) on its own listener,
/// meant for a private address: the page names classes and counts. Up to
/// 16 scrapers at once; until `shutdown`.
pub async fn serve_metrics(
    listener: TcpListener,
    actors: Option<ActorService>,
    shutdown: impl Future<Output = ()>,
) -> Result<(), Error> {
    let slots = Arc::new(tokio::sync::Semaphore::new(16));
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        let slot = tokio::select! {
            slot = Arc::clone(&slots).acquire_owned() => match slot {
                Ok(slot) => slot,
                Err(_) => return Ok(()),
            },
            () = &mut shutdown => return Ok(()),
        };
        let (stream, _) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::debug!(error = %e, "metrics accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            () = &mut shutdown => return Ok(()),
        };
        let actors = actors.clone();
        let service = service_fn(move |req: Request<Incoming>| {
            let actors = actors.clone();
            async move {
                let page = req.uri().path() == "/metrics"
                    && matches!(*req.method(), hyper::Method::GET | hyper::Method::HEAD);
                let response = if page {
                    let gauges = actors
                        .as_ref()
                        .map(ActorService::gauges)
                        .unwrap_or_default();
                    let mut r = text_response(StatusCode::OK, &crate::metrics::render(gauges));
                    r.headers_mut().insert(
                        hyper::header::CONTENT_TYPE,
                        hyper::header::HeaderValue::from_static(
                            "text/plain; version=0.0.4; charset=utf-8",
                        ),
                    );
                    r
                } else {
                    text_response(StatusCode::NOT_FOUND, "not found\n")
                };
                Ok::<_, std::convert::Infallible>(response)
            }
        });
        tokio::spawn(async move {
            let _slot = slot;
            let conn = http1::Builder::new()
                .timer(hyper_util::rt::TokioTimer::new())
                .header_read_timeout(Duration::from_secs(10))
                .serve_connection(TokioIo::new(stream), service);
            let _ = tokio::time::timeout(Duration::from_secs(60), conn).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(uri: &str) -> Option<Option<ActorTarget>> {
        ActorTarget::parse(&uri.parse().unwrap())
    }

    #[test]
    fn actor_paths_are_split_and_decoded() {
        let t = target("/actor/counter/room%201/inc?by=2").unwrap().unwrap();
        assert_eq!(
            t,
            ActorTarget {
                class: "counter".into(),
                name: "room 1".into(),
                rest: "/inc?by=2".into()
            }
        );
        let t = target("/actor/counter/a").unwrap().unwrap();
        assert_eq!(t.rest, "/");
        assert!(target("/hello").is_none());
        assert!(matches!(target("/actor/counter"), Some(None)));
        assert!(matches!(target("/actor/counter/"), Some(None)));
        assert!(matches!(target("/actor/counter/bad%zz"), Some(None)));
        assert!(matches!(target("/actor/counter/%FF"), Some(None)));
    }

    /// A body of `frames` (a frame's size, or a failure), counting the
    /// bytes read from it.
    struct Frames {
        frames: std::collections::VecDeque<Result<usize, ()>>,
        read: Arc<std::sync::atomic::AtomicU64>,
    }

    impl hyper::body::Body for Frames {
        type Data = Bytes;
        type Error = wasmtime_wasi_http::Error;

        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, Self::Error>>> {
            std::task::Poll::Ready(self.frames.pop_front().map(|frame| match frame {
                Ok(len) => {
                    self.read
                        .fetch_add(len as u64, std::sync::atomic::Ordering::SeqCst);
                    Ok(hyper::body::Frame::data(Bytes::from(vec![0; len])))
                }
                Err(()) => Err(wasmtime_wasi_http::Error::ConnectionTerminated),
            }))
        }
    }

    /// A capped body of `frames`, and the bytes read from it so far.
    fn capped(frames: &[Result<usize, ()>]) -> (CappedBody, Arc<std::sync::atomic::AtomicU64>) {
        let read = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let body = CappedBody::new(Frames {
            frames: frames.iter().copied().collect(),
            read: Arc::clone(&read),
        });
        (body, read)
    }

    /// The bytes read once nothing reads the body any more (it is dropped).
    async fn read_in_the_end(read: Arc<std::sync::atomic::AtomicU64>) -> u64 {
        tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&read) > 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        read.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[tokio::test]
    async fn a_dropped_body_is_read_to_its_end() {
        let (body, read) = capped(&[Ok(1024), Ok(1024), Ok(1024)]);
        drop(body);
        assert_eq!(read_in_the_end(read).await, 3 * 1024);

        // Also one its reader left half-way.
        let (mut body, read) = capped(&[Ok(1024), Ok(1024), Ok(1024)]);
        assert!(body.frame().await.unwrap().is_ok());
        drop(body);
        assert_eq!(read_in_the_end(read).await, 3 * 1024);
    }

    /// A request (`expect`: its `Expect` header), its body capped, and the bytes read
    /// from that body so far.
    fn capped_request(
        version: hyper::Version,
        expect: Option<&str>,
    ) -> (CappedBody, Arc<std::sync::atomic::AtomicU64>) {
        let read = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut req = Request::new(Frames {
            frames: [Ok(1024), Ok(1024)].into(),
            read: Arc::clone(&read),
        });
        *req.version_mut() = version;
        if let Some(expect) = expect {
            req.headers_mut()
                .insert(hyper::header::EXPECT, expect.parse().unwrap());
        }
        (CappedBody::of(req).into_body(), read)
    }

    #[tokio::test]
    async fn a_body_waiting_for_100_continue_is_not_asked_for_when_dropped() {
        let v11 = hyper::Version::HTTP_11;
        let (body, read) = capped_request(v11, Some("100-Continue"));
        drop(body);
        assert_eq!(read_in_the_end(read).await, 0);

        // Once the handler read from it, the sender was told to go on.
        let (mut body, read) = capped_request(v11, Some("100-continue"));
        assert!(body.frame().await.unwrap().is_ok());
        drop(body);
        assert_eq!(read_in_the_end(read).await, 2 * 1024);

        // Nobody waits without the header, or in HTTP/1.0.
        let (body, read) = capped_request(v11, None);
        drop(body);
        assert_eq!(read_in_the_end(read).await, 2 * 1024);
        let (body, read) = capped_request(hyper::Version::HTTP_10, Some("100-continue"));
        drop(body);
        assert_eq!(read_in_the_end(read).await, 2 * 1024);
    }

    #[tokio::test]
    async fn a_dropped_body_is_not_read_past_the_cap() {
        let frame = 1 << 20;
        let frames = vec![Ok(frame); 20];
        let (body, read) = capped(&frames);
        drop(body);
        // The frame that crosses the cap is the last one read.
        assert_eq!(read_in_the_end(read).await, MAX_BODY_BYTES + frame as u64);

        // What the reader took counts against the same cap.
        let (mut body, read) = capped(&frames);
        for _ in 0..10 {
            assert!(body.frame().await.unwrap().is_ok());
        }
        drop(body);
        assert_eq!(read_in_the_end(read).await, MAX_BODY_BYTES + frame as u64);
    }

    #[tokio::test]
    async fn a_body_that_failed_or_went_over_the_cap_is_not_read_on() {
        let (mut body, read) = capped(&[Ok(1024), Err(()), Ok(1024)]);
        assert!(body.frame().await.unwrap().is_ok());
        assert!(body.frame().await.unwrap().is_err());
        drop(body);
        assert_eq!(read_in_the_end(read).await, 1024);

        // Failing while what is left is discarded stops that too.
        let (body, read) = capped(&[Ok(1024), Err(()), Ok(1024)]);
        drop(body);
        assert_eq!(read_in_the_end(read).await, 1024);

        let frame = 1 << 20;
        let (mut body, read) = capped(&vec![Ok(frame); 20]);
        while let Some(Ok(_)) = body.frame().await {}
        drop(body);
        assert_eq!(read_in_the_end(read).await, MAX_BODY_BYTES + frame as u64);
    }
}

#[cfg(test)]
mod cluster_port_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)] // test code

    use super::*;
    use crate::cluster_tls::pinning::{Identity, PinSet, client_config, server_config};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct Running {
        public: std::net::SocketAddr,
        cluster: std::net::SocketAddr,
        pins: Arc<PinSet>,
        stop: Option<tokio::sync::oneshot::Sender<()>>,
        done: tokio::task::JoinHandle<Result<(), Error>>,
    }

    /// A server with both ports: the cluster port trusts `trusted` (and
    /// itself), with handshakes cut after `handshake`.
    async fn start(server: &Identity, trusted: &[&Identity], handshake: Duration) -> Running {
        let pins = Arc::new(PinSet::default());
        let listed: Vec<(String, Vec<[u8; 32]>)> = trusted
            .iter()
            .chain(std::iter::once(&server))
            .enumerate()
            .map(|(i, id)| (format!("n{i}"), vec![id.pin()]))
            .collect();
        pins.replace(
            listed
                .iter()
                .map(|(n, p)| (n.as_str(), p.as_slice(), u64::MAX)),
        );
        let public = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cluster = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (public_addr, cluster_addr) =
            (public.local_addr().unwrap(), cluster.local_addr().unwrap());
        let port = ClusterPort {
            listener: cluster,
            tls: server_config(server, Arc::clone(&pins)).unwrap(),
            pins: Arc::clone(&pins),
            handshake_timeout: handshake,
        };
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let done = tokio::spawn(serve_with_cluster(
            public,
            Some(port),
            App::default(),
            async {
                let _ = stopped.await;
            },
            ServeLimits::default(),
        ));
        Running {
            public: public_addr,
            cluster: cluster_addr,
            pins,
            stop: Some(stop),
            done,
        }
    }

    /// GET `path` over TLS as `client`; the status, or the handshake error.
    async fn get_tls(
        addr: std::net::SocketAddr,
        client: &Identity,
        server_pin: [u8; 32],
        path: &str,
    ) -> Result<u16, String> {
        let config = client_config(client, vec![server_pin]).unwrap();
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let name = rustls::pki_types::ServerName::try_from("node.test").unwrap();
        let mut tls = tokio_rustls::TlsConnector::from(config)
            .connect(name, tcp)
            .await
            .map_err(|e| e.to_string())?;
        let req = format!("GET {path} HTTP/1.1\r\nHost: n\r\nConnection: close\r\n\r\n");
        tls.write_all(req.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        let mut raw = Vec::new();
        tls.read_to_end(&mut raw).await.map_err(|e| e.to_string())?;
        let text = String::from_utf8_lossy(&raw);
        text.split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| format!("no status in {text:?}"))
    }

    async fn get_plain(addr: std::net::SocketAddr, path: &str) -> u16 {
        let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: n\r\nConnection: close\r\n\r\n");
        tcp.write_all(req.as_bytes()).await.unwrap();
        let mut raw = Vec::new();
        tcp.read_to_end(&mut raw).await.unwrap();
        String::from_utf8_lossy(&raw)
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap()
    }

    #[tokio::test]
    async fn a_pinned_peer_is_served_over_tls_and_the_public_port_stays_plain() {
        let (server, peer) = (
            Identity::generate("b", "b.test").unwrap(),
            Identity::generate("a", "a.test").unwrap(),
        );
        let run = start(&server, &[&peer], Duration::from_secs(5)).await;
        assert_eq!(
            get_tls(run.cluster, &peer, server.pin(), HEALTH_PATH).await,
            Ok(200)
        );
        assert_eq!(
            get_tls(run.cluster, &peer, server.pin(), READY_PATH).await,
            Ok(200)
        );
        assert_eq!(get_plain(run.public, HEALTH_PATH).await, 200);
        // Plain HTTP is not spoken on the cluster port.
        let mut tcp = tokio::net::TcpStream::connect(run.cluster).await.unwrap();
        tcp.write_all(b"GET /_wasmkeep/health HTTP/1.1\r\nHost: n\r\n\r\n")
            .await
            .unwrap();
        let mut raw = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), tcp.read_to_end(&mut raw)).await;
        assert!(!String::from_utf8_lossy(&raw).contains("200"), "{raw:?}");
    }

    #[tokio::test]
    async fn an_unknown_certificate_is_refused() {
        let (server, peer) = (
            Identity::generate("b", "b.test").unwrap(),
            Identity::generate("a", "a.test").unwrap(),
        );
        let stranger = Identity::generate("a", "a.test").unwrap();
        let run = start(&server, &[&peer], Duration::from_secs(5)).await;
        assert!(
            get_tls(run.cluster, &stranger, server.pin(), HEALTH_PATH)
                .await
                .is_err()
        );
        // And the client refuses a server whose pin it was not given.
        let other = Identity::generate("b", "b.test").unwrap();
        assert!(
            get_tls(run.cluster, &peer, other.pin(), HEALTH_PATH)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn a_kept_alive_connection_is_refused_once_its_pin_is_gone() {
        let (server, peer) = (
            Identity::generate("b", "b.test").unwrap(),
            Identity::generate("a", "a.test").unwrap(),
        );
        let run = start(&server, &[&peer], Duration::from_secs(5)).await;
        let config = client_config(&peer, vec![server.pin()]).unwrap();
        let tcp = tokio::net::TcpStream::connect(run.cluster).await.unwrap();
        let name = rustls::pki_types::ServerName::try_from("node.test").unwrap();
        let mut tls = tokio_rustls::TlsConnector::from(config)
            .connect(name, tcp)
            .await
            .unwrap();
        let get = format!("GET {HEALTH_PATH} HTTP/1.1\r\nHost: n\r\n\r\n");
        tls.write_all(get.as_bytes()).await.unwrap();
        let mut buf = vec![0u8; 4096];
        let n = tls.read(&mut buf).await.unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).starts_with("HTTP/1.1 200"));
        // The peer's heartbeat expires: the same connection's next request
        // is refused, and the connection closed.
        run.pins.replace(std::iter::empty());
        tls.write_all(get.as_bytes()).await.unwrap();
        let mut raw = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), tls.read_to_end(&mut raw)).await;
        let text = String::from_utf8_lossy(&raw);
        assert!(text.starts_with("HTTP/1.1 503"), "{text}");
        assert!(text.contains("not a live node"), "{text}");
    }

    #[tokio::test]
    async fn a_silent_connection_is_cut_after_the_handshake_timeout() {
        let server = Identity::generate("b", "b.test").unwrap();
        let run = start(&server, &[], Duration::from_millis(300)).await;
        let mut tcp = tokio::net::TcpStream::connect(run.cluster).await.unwrap();
        let mut buf = [0u8; 16];
        // Never sends a ClientHello: closed (EOF or reset) soon after 300 ms.
        let closed = tokio::time::timeout(Duration::from_secs(5), tcp.read(&mut buf)).await;
        assert!(matches!(closed, Ok(Ok(0) | Err(_))), "{closed:?}");
    }

    #[tokio::test]
    async fn shutdown_closes_a_stalled_cluster_connection_after_the_grace_period() {
        // A pinned peer starts a request and never finishes its headers.
        // Shutdown waits the grace period for it, then must close it: the
        // connection may not outlive `serve` (holding its socket and the
        // app) once serving has returned.
        let (server, peer) = (
            Identity::generate("b", "b.test").unwrap(),
            Identity::generate("a", "a.test").unwrap(),
        );
        let mut run = start(&server, &[&peer], Duration::from_secs(5)).await;
        let config = client_config(&peer, vec![server.pin()]).unwrap();
        let tcp = tokio::net::TcpStream::connect(run.cluster).await.unwrap();
        let name = rustls::pki_types::ServerName::try_from("node.test").unwrap();
        let mut tls = tokio_rustls::TlsConnector::from(config)
            .connect(name, tcp)
            .await
            .unwrap();
        tls.write_all(b"GET /_wasmkeep/health HTTP/1.1\r\nHost: n\r\nX-Slow: ")
            .await
            .unwrap();
        tls.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let started = std::time::Instant::now();
        run.stop.take().unwrap().send(()).unwrap();
        tokio::time::timeout(SHUTDOWN_GRACE + Duration::from_secs(5), run.done)
            .await
            .expect("serve returned after the grace period")
            .unwrap()
            .unwrap();
        assert!(started.elapsed() >= SHUTDOWN_GRACE - Duration::from_millis(500));
        let mut buf = [0u8; 64];
        let read = tokio::time::timeout(Duration::from_secs(2), tls.read(&mut buf)).await;
        assert!(
            matches!(read, Ok(Ok(0) | Err(_))),
            "the stalled cluster connection is still open after serve returned: {read:?}"
        );
    }

    #[tokio::test]
    async fn one_shutdown_stops_both_ports() {
        let (server, peer) = (
            Identity::generate("b", "b.test").unwrap(),
            Identity::generate("a", "a.test").unwrap(),
        );
        let mut run = start(&server, &[&peer], Duration::from_secs(5)).await;
        assert_eq!(
            get_tls(run.cluster, &peer, server.pin(), HEALTH_PATH).await,
            Ok(200)
        );
        run.stop.take().unwrap().send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(15), run.done)
            .await
            .expect("serve returned")
            .unwrap()
            .unwrap();
        assert!(tokio::net::TcpStream::connect(run.cluster).await.is_err());
        assert!(tokio::net::TcpStream::connect(run.public).await.is_err());
    }

    #[test]
    fn the_cluster_port_serves_actors_and_health_checks_only() {
        for path in ["/actor/c/n", "/actor/c/n/x", HEALTH_PATH, READY_PATH] {
            assert!(cluster_serves(path), "{path}");
        }
        for path in [
            "/",
            "/metrics",
            "/_wasmkeep/other",
            "/actorx",
            "/static/a.js",
        ] {
            assert!(!cluster_serves(path), "{path}");
        }
    }
}
