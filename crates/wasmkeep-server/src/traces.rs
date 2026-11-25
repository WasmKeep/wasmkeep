//! Request tracing (ADR 0044). Context is copied across task boundaries;
//! the task-local scope is only a convenience inside a single polled future.
//!
//! On a hop (ADR 0044, sections 3 and 4): the sending node sets
//! `traceparent` to its `hop` span before the hop is signed, within the
//! head limits, and the receiving node follows the sampled flag of a hop
//! that authenticates as a node's with tracing on. Nothing of it is
//! signed, and no header of the host's range is added.

use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;

use opentelemetry::trace::{
    Span as _, SpanContext, SpanId, TraceContextExt as _, TraceFlags, TraceId, TraceState,
    Tracer as _, TracerProvider as _,
};
use opentelemetry::{Context, KeyValue};
use opentelemetry_sdk::trace::{IdGenerator as _, RandomIdGenerator, SdkTracer, SdkTracerProvider};

mod exporter;

static TRACING: OnceLock<Tracing> = OnceLock::new();
tokio::task_local! { static CURRENT: Trace; }

/// Process-wide tracing, installed only when the operator supplies an endpoint.
#[derive(Debug)]
pub struct Tracing {
    provider: SdkTracerProvider,
    tracer: SdkTracer,
    ratio: f64,
    trust: bool,
}

/// Flushes the bounded exporter on a clean stop, including an early serve error.
#[derive(Debug)]
pub struct Guard;
impl Drop for Guard {
    fn drop(&mut self) {
        if let Some(t) = TRACING.get() {
            let _ = t.provider.shutdown_with_timeout(Duration::from_secs(3));
        }
    }
}

/// Starts OTLP/HTTP protobuf export. No endpoint does no work at all.
pub fn start(
    endpoint: Option<&str>,
    ratio: f64,
    trust: bool,
    node: &str,
) -> Result<Option<Guard>, String> {
    let Some(endpoint) = endpoint else {
        return Ok(None);
    };
    if !ratio.is_finite() || !(0.0..=1.0).contains(&ratio) {
        return Err("trace-sample must be between 0 and 1".into());
    }
    let provider = exporter::provider(endpoint, node)?;
    install(provider, ratio, trust)
}

/// Installs a provider built elsewhere: for this crate's tests only, not an
/// interface. The process may install it once. Dropping the guard shuts it
/// down; configuration must precede serving.
#[doc(hidden)]
pub fn install(
    provider: SdkTracerProvider,
    ratio: f64,
    trust: bool,
) -> Result<Option<Guard>, String> {
    if !ratio.is_finite() || !(0.0..=1.0).contains(&ratio) {
        return Err("trace-sample must be between 0 and 1".into());
    }
    let tracer = provider.tracer("wasmkeep");
    TRACING
        .set(Tracing {
            provider,
            tracer,
            ratio,
            trust,
        })
        .map_err(|_| "tracing already started")?;
    Ok(Some(Guard))
}

/// Only these values travel into mailbox jobs and upload flights.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Trace {
    trace: TraceId,
    parent: SpanId,
    sampled: bool,
}
impl Trace {
    fn parse(value: &str) -> Option<Self> {
        let b = value.as_bytes();
        if b.len() < 55 || b[2] != b'-' || b[35] != b'-' || b[52] != b'-' {
            return None;
        }
        let hex = |s: &[u8]| {
            s.iter()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(c))
        };
        if !hex(&b[..2]) || !hex(&b[3..35]) || !hex(&b[36..52]) || !hex(&b[53..55]) {
            return None;
        }
        if &b[..2] == b"ff"
            || (&b[..2] == b"00" && b.len() != 55)
            || (b.len() > 55 && b[55] != b'-')
        {
            return None;
        }
        let trace = TraceId::from_hex(&value[3..35]).ok()?;
        let parent = SpanId::from_hex(&value[36..52]).ok()?;
        if trace == TraceId::INVALID || parent == SpanId::INVALID {
            return None;
        }
        Some(Self {
            trace,
            parent,
            sampled: u8::from_str_radix(&value[53..55], 16).ok()? & 1 != 0,
        })
    }
    fn header(self) -> String {
        format!(
            "00-{}-{}-{:02x}",
            self.trace,
            self.parent,
            u8::from(self.sampled)
        )
    }
    fn context(self) -> Context {
        Context::new().with_remote_span_context(SpanContext::new(
            self.trace,
            self.parent,
            if self.sampled {
                TraceFlags::SAMPLED
            } else {
                TraceFlags::default()
            },
            true,
            TraceState::default(),
        ))
    }
}

/// The context a request starts with. `follow`: the incoming sampled flag
/// decides, in both directions (the operator trusts what arrives, or a
/// node with tracing on sent it: [`request`]); otherwise this node draws.
#[cfg(test)]
fn edge(value: Option<&str>, ratio: f64, follow: bool) -> Trace {
    edge_from(value.and_then(Trace::parse), ratio, follow)
}
fn edge_from(incoming: Option<Trace>, ratio: f64, trust: bool) -> Trace {
    // OS randomness, independent of the client-controlled trace id.
    let mut bytes = [0; 8];
    let random =
        ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut bytes).is_ok();
    let sampled = if trust && incoming.is_some() {
        incoming.is_some_and(|c| c.sampled)
    } else {
        random && (ratio >= 1.0 || (u64::from_be_bytes(bytes) as f64 / (u64::MAX as f64)) < ratio)
    };
    Trace {
        trace: incoming.map_or_else(|| RandomIdGenerator::default().new_trace_id(), |c| c.trace),
        parent: incoming.map_or(SpanId::INVALID, |c| c.parent),
        sampled,
    }
}

pub(crate) fn current() -> Option<Trace> {
    CURRENT.try_with(|c| *c).ok()
}
// Projection avoids boxing or multiplying the large actor future in nested
// async state machines. Context is active only during each poll, never across
// a yield on a worker thread.
pin_project_lite::pin_project! {
    pub(crate) struct Scoped<'a, F> {
        #[pin]
        future: F,
        context: Option<Trace>,
        span: Option<&'a mut Span>,
        attributes: Vec<KeyValue>,
    }
}
impl<F: Future> Future for Scoped<'_, F> {
    type Output = F::Output;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        let poll = || with_context(*this.context, || this.future.poll(cx));
        let Some(span) = this.span.as_mut() else {
            return poll();
        };
        let result = ATTRIBUTES.sync_scope(
            std::cell::RefCell::new(std::mem::take(this.attributes)),
            || {
                let result = poll();
                *this.attributes = ATTRIBUTES.with(|a| a.take());
                result
            },
        );
        // Record each poll's attributes even if the future is cancelled later.
        for attribute in this.attributes.drain(..) {
            span.set(attribute);
        }
        result
    }
}
pub(crate) fn scope<F: Future>(context: Option<Trace>, future: F) -> Scoped<'static, F> {
    Scoped {
        future,
        context,
        span: None,
        attributes: Vec::new(),
    }
}

/// A sampled span, or just its propagation context. Dropping ends the span.
#[derive(Debug)]
pub(crate) struct Span {
    context: Option<Trace>,
    span: Option<Box<opentelemetry_sdk::trace::Span>>,
    /// Written once, when the span ends: the last value set wins, so a
    /// span never carries two of an attribute in [`LAST_WINS`].
    late: Vec<KeyValue>,
}
impl Span {
    fn new(t: Option<&Tracing>, name: &'static str, parent: Option<Trace>) -> Self {
        if t.is_none() {
            return Self {
                context: None,
                span: None,
                late: Vec::new(),
            };
        }
        Self::at(t, name, parent, std::time::SystemTime::now())
    }
    fn at(
        t: Option<&Tracing>,
        name: &'static str,
        parent: Option<Trace>,
        started: std::time::SystemTime,
    ) -> Self {
        let Some(t) = t else {
            return Self {
                context: None,
                span: None,
                late: Vec::new(),
            };
        };
        let mut context = parent.unwrap_or_else(|| edge_from(None, t.ratio, false));
        let span = if context.sampled {
            let kind = match name {
                "request" => opentelemetry::trace::SpanKind::Server,
                "bucket" | "hop" => opentelemetry::trace::SpanKind::Client,
                _ => opentelemetry::trace::SpanKind::Internal,
            };
            let span = t
                .tracer
                .span_builder(name)
                .with_kind(kind)
                .with_start_time(started)
                .start_with_context(&t.tracer, &context.context());
            context.parent = span.span_context().span_id();
            context.trace = span.span_context().trace_id();
            Some(Box::new(span))
        } else {
            context.parent = RandomIdGenerator::default().new_span_id();
            None
        };
        Self {
            context: Some(context),
            span,
            late: Vec::new(),
        }
    }
    pub(crate) fn child(name: &'static str) -> Self {
        Self::new(TRACING.get(), name, current())
    }
    /// A span inside the trace the running code is in, and nothing when it
    /// is in none: work that belongs to no request or event (a sweep, a
    /// drain, a move) starts no trace of its own.
    fn inside(name: &'static str) -> Self {
        let parent = current();
        Self::new(TRACING.get().filter(|_| parent.is_some()), name, parent)
    }
    pub(crate) fn recording(&self) -> bool {
        self.span.is_some()
    }
    pub(crate) fn run<F: Future>(&mut self, future: F) -> Scoped<'_, F> {
        Scoped {
            future,
            context: self.context,
            attributes: Vec::new(),
            span: self.span.is_some().then_some(self),
        }
    }
    pub(crate) fn attribute(&mut self, key: &'static str, value: impl Into<opentelemetry::Value>) {
        if self.span.is_some() {
            self.set(KeyValue::new(key, value));
        }
    }
    fn set(&mut self, attribute: KeyValue) {
        if LAST_WINS.contains(&attribute.key.as_str()) {
            self.late.retain(|a| a.key != attribute.key);
            self.late.push(attribute);
        } else if let Some(span) = &mut self.span {
            span.set_attribute(attribute);
        }
    }
    /// The context a guest of this span receives, as its request's
    /// `traceparent` ([`set_guest_header`]).
    pub(crate) fn inject<B>(&self, req: &mut http::Request<B>) {
        if let Some(c) = self.context {
            set_guest_header(req, c);
        }
    }
}
impl Drop for Span {
    fn drop(&mut self) {
        if let Some(s) = &mut self.span {
            for attribute in self.late.drain(..) {
                s.set_attribute(attribute);
            }
            s.end();
        }
    }
}

/// The attributes a span sets more than once (a default, then what
/// happened): kept aside and written when the span ends.
const OUTCOME: &str = "outcome";
const LAST_WINS: [&str; 5] = [OUTCOME, "remote", "resent", "status", "carried"];

/// The request methods a span names; any other token a client sends is
/// `other`, so the attribute is never the client's text.
fn method_name(method: &http::Method) -> &'static str {
    match method.as_str() {
        "GET" => "GET",
        "HEAD" => "HEAD",
        "POST" => "POST",
        "PUT" => "PUT",
        "DELETE" => "DELETE",
        "PATCH" => "PATCH",
        "OPTIONS" => "OPTIONS",
        "CONNECT" => "CONNECT",
        "TRACE" => "TRACE",
        _ => "other",
    }
}

/// Sets a guest's `traceparent` to `context`, if the client's part of the
/// head stays within the head limits with it (ADR 0044, section 3,
/// condition 2: the bytes and the field count a node takes from a client).
/// Otherwise the request goes on unchanged.
fn set_guest_header<B>(req: &mut http::Request<B>, context: Trace) -> bool {
    set_header_within(req, context, 0)
}

/// [`set_guest_header`], the head measured `discount` bytes smaller than
/// [`crate::http::client_head`] counts it (see [`hop`]). Whether it was
/// set. A value that takes no more room than the ones it replaces always
/// fits: the head was within the limits when it arrived.
fn set_header_within<B>(req: &mut http::Request<B>, context: Trace, discount: usize) -> bool {
    let (bytes, count) = crate::http::client_head(req);
    let bytes = bytes.saturating_sub(discount);
    let (had_bytes, had) = req
        .headers()
        .get_all(TRACEPARENT)
        .iter()
        .fold((0, 0), |(bytes, count), v| {
            (bytes + TRACEPARENT.len() + v.len() + 4, count + 1)
        });
    let value = context.header();
    let own = TRACEPARENT.len() + value.len() + 4;
    let fits = (had > 0 && own <= had_bytes)
        || (bytes - had_bytes + own <= crate::http::MAX_REQUEST_HEAD_BYTES
            && count - had < crate::http::MAX_REQUEST_HEADERS);
    let set = fits.then(|| value.parse().ok()).flatten();
    if let Some(value) = set {
        req.headers_mut().insert(TRACEPARENT, value);
        return true;
    }
    false
}

const TRACEPARENT: &str = "traceparent";

/// Whether tracing is on for the process.
pub(crate) fn enabled() -> bool {
    TRACING.get().is_some()
}

/// Whether the code running now is inside a sampled span: what an
/// attribute costs to compute is worth paying only then.
pub(crate) fn recording() -> bool {
    ATTRIBUTES.try_with(|_| ()).is_ok()
}

/// `traceparent` on an `actors.call`, set by the host from the calling
/// event's (or front's) context, replacing whatever the guest put there
/// (ADR 0044, section 2): local or to another node, a guest cannot move a
/// call into another trace or turn sampling on. A call's signature does not
/// cover the header, and `build_request` keeps room for it. With tracing
/// off the guest's header passes through as before.
pub(crate) fn set_call_header<B>(req: &mut http::Request<B>) {
    if enabled() {
        call_header(req, current());
    }
}
fn call_header<B>(req: &mut http::Request<B>, context: Option<Trace>) {
    match context.and_then(|c| c.header().parse().ok()) {
        Some(value) => {
            req.headers_mut().insert(TRACEPARENT, value);
            if let Some(context) = context {
                req.extensions_mut().insert(context);
            }
        }
        None => {
            req.headers_mut().remove(TRACEPARENT);
        }
    }
}

/// The `call` span of one `actors.call`, in the calling event's (or
/// front's) trace: local or to another node, one per entry of a batch.
pub(crate) fn call(class: &str, actor: &str) -> Span {
    let mut span = Span::inside("call");
    if span.recording() {
        span.attribute("class", class.to_owned());
        span.attribute("actor", actor.to_owned());
        span.attribute("remote", false);
    }
    span
}

/// The `hop` span of a request `node` sends to `peer` (a forward, a
/// relayed upgrade, a node's call, a hand-off, a take, a placed request),
/// and its context as the request's `traceparent` (ADR 0044, section 3).
///
/// Called before the hop is signed: a forward's proof hashes the header as
/// it hashes every header of the client's, so it must be in place first; a
/// call's signature does not cover it. The header is set only if the
/// client's part of the head stays within the head limits with it
/// ([`set_guest_header`]), measured as the receiver will count it: a
/// forward's URI is already the peer's address here, which the receiver
/// reads as the `Host` header the request carries, so the address is not
/// counted a second time. Otherwise the request goes on without one and
/// the receiver starts a trace of its own. With tracing off nothing is
/// made and the request is not touched.
///
/// A node with tracing on announces it, and its peers then follow the
/// flag of what it sends ([`request`]). So every hop it sends carries its
/// own `traceparent` or none, never a client's or a guest's: whenever the
/// hop's is not set (no room for it, or outside any trace: a sweep, a
/// drain, a move), a `traceparent` already on the request is removed.
pub(crate) fn hop<B>(
    kind: &'static str,
    node: &str,
    peer: &str,
    req: &mut http::Request<B>,
) -> Span {
    hop_in(TRACING.get(), current(), kind, node, peer, req)
}
fn hop_in<B>(
    t: Option<&Tracing>,
    parent: Option<Trace>,
    kind: &'static str,
    node: &str,
    peer: &str,
    req: &mut http::Request<B>,
) -> Span {
    let mut span = Span::new(t.filter(|_| parent.is_some()), "hop", parent);
    if span.recording() {
        span.attribute("kind", kind);
        span.attribute("node", node.to_owned());
        span.attribute("peer", peer.to_owned());
        span.attribute("resent", false);
        span.attribute(OUTCOME, "failed");
    }
    span.carry(t.is_some(), req);
    span
}

impl Span {
    /// Puts this hop's context on `req` as it is sent, by [`hop`]'s
    /// rules: within the head limits as the receiver counts them, and
    /// with tracing on (`on`) this node's own header or none. `carried`
    /// says whether the header went out on the request sent last.
    fn carry<B>(&mut self, on: bool, req: &mut http::Request<B>) {
        // The address, when the receiver will read it from `Host` instead.
        let discount = match req.uri().authority() {
            Some(authority) if req.headers().contains_key(http::header::HOST) => {
                authority.as_str().len() + 8
            }
            _ => 0,
        };
        let carried = self
            .context
            .is_some_and(|context| set_header_within(req, context, discount));
        if on && !carried {
            // Its own or none: never one this node did not set.
            req.headers_mut().remove(TRACEPARENT);
        }
        self.attribute("carried", carried);
    }

    /// The same hop sent a second time, on a request built again: the
    /// header by the same rules as the first send, and `resent`.
    pub(crate) fn resend<B>(&mut self, req: &mut http::Request<B>) {
        self.resend_in(TRACING.get().is_some(), req);
    }
    fn resend_in<B>(&mut self, on: bool, req: &mut http::Request<B>) {
        self.carry(on, req);
        self.attribute("resent", true);
    }
}

impl Span {
    /// What a hop's answer was: its status, or that none came.
    pub(crate) fn answered(&mut self, status: Option<http::StatusCode>) {
        if self.recording() {
            match status {
                Some(status) => {
                    self.attribute("status", i64::from(status.as_u16()));
                    self.attribute(OUTCOME, "answered");
                }
                None => self.attribute(OUTCOME, "failed"),
            }
        }
    }
}

/// The `request` span of a request this node received, and the context
/// the rest of it runs in.
///
/// The sampled decision (ADR 0044, section 4): the incoming flag is
/// followed, in both directions, when the operator trusts what arrives
/// (`--trace-trust-incoming`), or when `from_tracing_node` says the
/// request is a hop that authenticates as a node's and that node has
/// tracing on, so the header is that node's and the first node of the
/// trace decided. It is asked only when there is a flag to follow and
/// nothing else settles it. Otherwise this node draws at its ratio: a
/// request from outside, a forward with no proof, a hop with no header
/// (the sender had no room for it), and a hop from a node that does not
/// set the header itself (tracing off, or a version before 0.9), which
/// passes on whatever a client or a guest sent.
pub(crate) fn request<B>(
    req: &mut http::Request<B>,
    from_tracing_node: impl FnOnce(&mut http::Request<B>) -> bool,
) -> Span {
    request_in(TRACING.get(), req, from_tracing_node)
}
fn request_in<B>(
    t: Option<&Tracing>,
    req: &mut http::Request<B>,
    from_tracing_node: impl FnOnce(&mut http::Request<B>) -> bool,
) -> Span {
    let Some(t) = t else {
        return Span::new(None, "request", None);
    };
    let incoming = req
        .headers()
        .get(TRACEPARENT)
        .and_then(|h| h.to_str().ok())
        .and_then(Trace::parse);
    let follow = incoming.is_some() && (t.trust || from_tracing_node(req));
    let parent = Some(edge_from(incoming, t.ratio, follow));
    let mut span = Span::new(Some(t), "request", parent);
    span.attribute("method", method_name(req.method()));
    if let Some(c) = span.context {
        req.extensions_mut().insert(c);
    }
    // Do not change the header here: ingress must verify the original proof.
    span
}

pub(crate) fn event<B>(req: &http::Request<B>) -> Span {
    Span::new(
        TRACING.get(),
        "event",
        req.extensions().get::<Trace>().copied().or_else(current),
    )
}

/// Bucket requests outside sampled events (renewals, sweeps) make no spans.
pub(crate) fn bucket(operation: &'static str, key: &str) -> Span {
    let parent = current().filter(|c| c.sampled);
    if parent.is_none() {
        return Span::new(None, "bucket", None);
    }
    let mut span = Span::new(TRACING.get(), "bucket", parent);
    span.attribute("operation", operation);
    // An allowlist, never a raw key or a user-controlled label.
    span.attribute("family", key_family(key));
    span
}

pub(crate) fn inject_current<B>(req: &mut http::Request<B>) {
    if let Some(context) = current() {
        set_guest_header(req, context);
    }
}

/// A streamed delete completes when its result arrives; its start is the
/// preceding item's completion, the same boundary used by bucket metrics.
pub(crate) fn bucket_delete(started: std::time::Instant, key: &str) -> Span {
    let Some(parent) = current().filter(|c| c.sampled) else {
        return Span::new(None, "bucket", None);
    };
    let now = std::time::SystemTime::now();
    let mut span = Span::at(
        TRACING.get(),
        "bucket",
        Some(parent),
        now.checked_sub(started.elapsed()).unwrap_or(now),
    );
    span.attribute("operation", "delete");
    span.attribute("family", key_family(key));
    span
}
/// What a key holds, read at the place the layout puts it: the first
/// segment that is one of the bucket's roots (only the operator's prefix
/// comes before it), then the folder at its fixed depth. A tenant, app,
/// class or actor named like a family is never read as one.
fn key_family(key: &str) -> &'static str {
    let mut parts = key.split('/').skip_while(|p| !ROOTS.contains(p));
    match parts.next() {
        Some("nodes") => "nodes",
        Some("formats") => "formats",
        // `apps/<tenant>/<app>/...`: a deploy, or its static assets.
        Some("apps") => match parts.nth(2) {
            Some("_assets") => "assets",
            _ => "apps",
        },
        // `state/<tenant>/<app>/<folder>/...`
        Some("state") => match parts.nth(2) {
            // `actors/<class>/<name>/{wal,snapshot}/...`
            Some("actors") => match parts.nth(2) {
                Some("wal") => "wal",
                Some("snapshot") => "snapshot",
                _ => "history",
            },
            Some("owners") => "owners",
            Some("leases") => "leases",
            Some("alarms") => "alarms",
            Some("moves") => "moves",
            Some("deletes") => "deletes",
            Some("restores") => "restores",
            Some("blobs" | "blob-usage") => "blobs",
            _ => "other",
        },
        _ => "other",
    }
}
const ROOTS: [&str; 4] = ["state", "apps", "nodes", "formats"];

pub(crate) fn with_context<T>(context: Option<Trace>, f: impl FnOnce() -> T) -> T {
    match context {
        Some(c) => CURRENT.sync_scope(c, f),
        None => f(),
    }
}

tokio::task_local! { static ATTRIBUTES: std::cell::RefCell<Vec<KeyValue>>; }
pub(crate) fn attribute(key: &'static str, value: impl Into<opentelemetry::Value>) {
    let _ = ATTRIBUTES.try_with(|a| a.borrow_mut().push(KeyValue::new(key, value)));
}

pub(crate) fn log_id() -> Option<TraceId> {
    current().filter(|c| c.sampled).map(|c| c.trace)
}
macro_rules! trace_warn {
    ($($fields:tt)*) => {
        if let Some(id) = crate::traces::log_id() {
            tracing::warn!(trace_id = %id, $($fields)*);
        } else {
            tracing::warn!($($fields)*);
        }
    };
}
pub(crate) use trace_warn as warn;
macro_rules! trace_error {
    ($($fields:tt)*) => {
        if let Some(id) = crate::traces::log_id() {
            tracing::error!(trace_id = %id, $($fields)*);
        } else {
            tracing::error!($($fields)*);
        }
    };
}
pub(crate) use trace_error as error;

/// A context for this crate's tests of what a hop carries.
#[cfg(test)]
pub(crate) fn test_context(sampled: bool) -> Trace {
    Trace {
        trace: TraceId::from(0x1234_5678_90ab_cdef_1234_5678_90ab_cdef_u128),
        parent: SpanId::from(0x1234_5678_90ab_cdef_u64),
        sampled,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn tracing(ratio: f64, trust: bool) -> (Tracing, Memory) {
        let memory = Memory::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(memory.clone())
            .build();
        let t = Tracing {
            tracer: provider.tracer("test"),
            provider,
            ratio,
            trust,
        };
        (t, memory)
    }

    /// The sampled decision of a request a node receives (ADR 0044,
    /// section 4), by who sent it, the trust flag, the ratio and the flag.
    #[test]
    fn traces_a_hop_from_a_tracing_node_decides_and_nothing_else_does() {
        // (from a tracing node, trust, ratio, header) -> sampled
        for (node, trust, ratio, header, sampled) in [
            // A hop from a node with tracing on: its flag, whatever the
            // ratio here, with or without the trust flag.
            (true, false, 0.0, Some(YES), true),
            (true, false, 1.0, Some(NO), false),
            (true, true, 0.0, Some(YES), true),
            (true, true, 1.0, Some(NO), false),
            // Anything else (a client, a forward with no proof, a hop from
            // a node that passes on what it got): this node draws.
            (false, false, 0.0, Some(YES), false),
            (false, false, 1.0, Some(NO), true),
            // ...unless the operator trusts what arrives.
            (false, true, 0.0, Some(YES), true),
            (false, true, 1.0, Some(NO), false),
            // No header, or one that does not parse: nothing to follow,
            // from a node or not.
            (true, false, 1.0, None, true),
            (true, false, 0.0, None, false),
            (true, true, 0.0, Some("bad"), false),
            (true, false, 1.0, Some("bad"), true),
        ] {
            let (t, _) = tracing(ratio, trust);
            let mut req = http::Request::builder();
            if let Some(header) = header {
                req = req.header("traceparent", header);
            }
            let mut req = req.body(()).unwrap();
            let span = request_in(Some(&t), &mut req, |_| node);
            let context = span.context.unwrap();
            assert_eq!(
                (context.sampled, span.recording()),
                (sampled, sampled),
                "{node} {trust} {ratio} {header:?}"
            );
            // The trace id is continued either way.
            if header == Some(YES) || header == Some(NO) {
                assert_eq!(context.trace, Trace::parse(YES).unwrap().trace);
            }
        }
        // The sender is asked only when a flag is there to follow and the
        // trust flag does not settle it: no signature is checked for a
        // request with no header.
        let asked = std::cell::Cell::new(0);
        let ask = |_: &mut http::Request<()>| {
            asked.set(asked.get() + 1);
            true
        };
        let (t, _) = tracing(1.0, false);
        let mut none = http::Request::new(());
        let _ = request_in(Some(&t), &mut none, ask);
        let mut bad = http::Request::builder()
            .header("traceparent", "bad")
            .body(())
            .unwrap();
        let _ = request_in(Some(&t), &mut bad, ask);
        let (trusting, _) = tracing(1.0, true);
        let mut yes = http::Request::builder()
            .header("traceparent", YES)
            .body(())
            .unwrap();
        let _ = request_in(Some(&trusting), &mut yes, ask);
        assert_eq!(asked.get(), 0);
        let _ = request_in(Some(&t), &mut yes, ask);
        assert_eq!(asked.get(), 1);
        // Off: not asked, nothing made.
        let off = request_in(None, &mut yes, ask);
        assert!(off.context.is_none() && asked.get() == 1);
    }

    /// What a hop carries (ADR 0044, section 3): the sending node's `hop`
    /// span, in the trace it is sent from, with that trace's decision.
    #[test]
    fn traces_a_hop_carries_its_own_span_or_nothing() {
        let (t, memory) = tracing(0.0, false);
        let client = "00-feedfacefeedfacefeedfacefeedface-feedfacefeedface-01";
        let request = |header: Option<&str>| {
            let mut req = http::Request::builder();
            if let Some(header) = header {
                req = req.header("traceparent", header);
            }
            req.body(()).unwrap()
        };
        // In a sampled trace: a span, a child of the sender's context, and
        // its id in the header, replacing a client's.
        let parent = test_context(true);
        for sent in [None, Some(client)] {
            let mut req = request(sent);
            let hop = hop_in(Some(&t), Some(parent), "forward", "a", "b", &mut req);
            let context = hop.context.unwrap();
            assert!(hop.recording());
            assert_eq!(context.trace, parent.trace);
            assert_ne!(context.parent, parent.parent);
            assert_eq!(req.headers()["traceparent"], context.header().as_str());
            assert_eq!(req.headers().get_all("traceparent").iter().count(), 1);
            assert!(context.header().ends_with("-01"));
        }
        let spans = std::mem::take(&mut *memory.0.lock().unwrap());
        assert_eq!(spans.len(), 2);
        for span in &spans {
            assert_eq!(span.name, "hop");
            assert_eq!(span.parent_span_id, parent.parent);
            let attribute = |key: &str| {
                span.attributes
                    .iter()
                    .find(|a| a.key.as_str() == key)
                    .map(|a| a.value.as_str().into_owned())
            };
            assert_eq!(attribute("kind").as_deref(), Some("forward"));
            assert_eq!(attribute("node").as_deref(), Some("a"));
            assert_eq!(attribute("peer").as_deref(), Some("b"));
            assert_eq!(attribute("outcome").as_deref(), Some("failed"));
        }
        // In an unsampled trace: the context goes on, flag off, so the
        // receiver makes no span either; no span is made here.
        let mut req = request(Some(client));
        let hop = hop_in(
            Some(&t),
            Some(test_context(false)),
            "call",
            "a",
            "b",
            &mut req,
        );
        assert!(!hop.recording());
        let value = req.headers()["traceparent"].to_str().unwrap().to_owned();
        assert!(
            value.starts_with("00-1234567890abcdef1234567890abcdef-") && value.ends_with("-00")
        );
        drop(hop);
        // In no trace (a sweep, a drain): no span, and no header: a node
        // with tracing on never passes on a `traceparent` it did not set.
        let mut req = request(Some(client));
        let hop = hop_in(Some(&t), None, "take", "a", "b", &mut req);
        assert!(hop.context.is_none());
        assert!(req.headers().get("traceparent").is_none());
        drop(hop);
        assert!(memory.0.lock().unwrap().is_empty());
        // Tracing off: the request is not touched, whatever it carries.
        let mut req = request(Some(client));
        let hop = hop_in(None, None, "forward", "a", "b", &mut req);
        assert!(hop.context.is_none());
        assert_eq!(req.headers()["traceparent"], client);
        let mut req = request(None);
        let _ = hop_in(None, Some(parent), "forward", "a", "b", &mut req);
        assert!(req.headers().is_empty());
    }

    /// The head limits on a hop (ADR 0044, section 3, condition 2): the
    /// header is added only if the client's part of the head stays within
    /// them, and a client's header that parses is always replaced.
    #[test]
    fn traces_a_hop_adds_its_header_only_within_the_head_limits() {
        let (t, _) = tracing(1.0, false);
        let parent = Some(test_context(true));
        let full = |traceparent: Option<&str>| {
            let mut req = http::Request::builder();
            let own = usize::from(traceparent.is_some());
            if let Some(value) = traceparent {
                req = req.header("traceparent", value);
            }
            for i in own..crate::http::MAX_REQUEST_HEADERS {
                req = req.header(format!("x-h{i}"), "v");
            }
            // The host's own headers do not count in the client's part.
            req.header("x-wasmkeep-forwarded", "1").body(()).unwrap()
        };
        // At the field count with no header of the client's: unchanged,
        // and the hop says it carried nothing.
        let mut req = full(None);
        let before = req.headers().clone();
        let hop = hop_in(Some(&t), parent, "forward", "a", "b", &mut req);
        assert!(hop.recording());
        assert_eq!(req.headers(), &before);
        // At the field count with the client's own: replaced.
        let mut req = full(Some(NO));
        let hop = hop_in(Some(&t), parent, "forward", "a", "b", &mut req);
        assert_eq!(
            req.headers()["traceparent"],
            hop.context.unwrap().header().as_str()
        );
        assert_eq!(
            crate::http::client_head(&req).1,
            crate::http::MAX_REQUEST_HEADERS
        );
        // At the byte limit exactly, the client's header one that parses
        // (version 00, or a later version's longer form): replaced, and
        // the head no larger than it was.
        let later = format!("{}-more", YES.replacen("00-", "01-", 1));
        for client in [YES, later.as_str()] {
            let mut req = http::Request::builder()
                .header("traceparent", client)
                .body(())
                .unwrap();
            let room = crate::http::MAX_REQUEST_HEAD_BYTES - crate::http::client_head(&req).0;
            req.headers_mut()
                .insert("x-big", "a".repeat(room - 9).parse().unwrap());
            let before = crate::http::client_head(&req).0;
            assert_eq!(before, crate::http::MAX_REQUEST_HEAD_BYTES);
            let hop = hop_in(Some(&t), parent, "forward", "a", "b", &mut req);
            assert_eq!(
                req.headers()["traceparent"],
                hop.context.unwrap().header().as_str()
            );
            assert!(crate::http::client_head(&req).0 <= before);
        }
        // As a forward is when its hop is made: the URI already the
        // peer's address, the client's `Host` still there, and a head
        // that was at the limit when it arrived. The address is not
        // counted twice: the client's header is replaced, never passed on.
        for client in [YES, later.as_str()] {
            let mut req = http::Request::builder()
                .uri("/actor/room/a?q=1")
                .header("host", "app.test")
                .header("traceparent", client)
                .body(())
                .unwrap();
            let room = crate::http::MAX_REQUEST_HEAD_BYTES - crate::http::client_head(&req).0;
            req.headers_mut()
                .insert("x-big", "a".repeat(room - 9).parse().unwrap());
            assert_eq!(
                crate::http::client_head(&req).0,
                crate::http::MAX_REQUEST_HEAD_BYTES
            );
            *req.uri_mut() = "https://node-b.cluster.internal:7443/actor/room/a?q=1"
                .parse()
                .unwrap();
            let hop = hop_in(Some(&t), parent, "forward", "a", "b", &mut req);
            assert_eq!(
                req.headers()["traceparent"],
                hop.context.unwrap().header().as_str()
            );
        }
        // With no header of the client's, the same head has no room: none
        // is added. One byte less in it, and there is room, measured as
        // the owner counts it (the `Host` header, not the address too).
        for (spare, added) in [(69, false), (70, true)] {
            let mut req = http::Request::builder()
                .uri("/actor/room/a?q=1")
                .header("host", "app.test")
                .body(())
                .unwrap();
            let room = crate::http::MAX_REQUEST_HEAD_BYTES - crate::http::client_head(&req).0;
            req.headers_mut()
                .insert("x-big", "a".repeat(room - 9 - spare).parse().unwrap());
            *req.uri_mut() = "https://node-b.cluster.internal:7443/actor/room/a?q=1"
                .parse()
                .unwrap();
            let _ = hop_in(Some(&t), parent, "forward", "a", "b", &mut req);
            assert_eq!(req.headers().contains_key("traceparent"), added, "{spare}");
        }
        // A client's header that does not parse, shorter than the host's,
        // in a head with no room for the host's: removed, not passed on.
        let mut req = http::Request::builder()
            .header("traceparent", "x")
            .body(())
            .unwrap();
        let room = crate::http::MAX_REQUEST_HEAD_BYTES - crate::http::client_head(&req).0;
        req.headers_mut()
            .insert("x-big", "a".repeat(room - 9).parse().unwrap());
        let _ = hop_in(Some(&t), parent, "forward", "a", "b", &mut req);
        assert!(req.headers().get("traceparent").is_none());
        // At the byte limit with none of the client's: unchanged.
        let mut req = http::Request::new(());
        let room = crate::http::MAX_REQUEST_HEAD_BYTES - crate::http::client_head(&req).0;
        req.headers_mut()
            .insert("x-big", "a".repeat(room - 9 - 60).parse().unwrap());
        let _ = hop_in(Some(&t), parent, "forward", "a", "b", &mut req);
        assert!(req.headers().get("traceparent").is_none());
        // One byte of room more than it needs: added.
        let mut req = http::Request::new(());
        let room = crate::http::MAX_REQUEST_HEAD_BYTES - crate::http::client_head(&req).0;
        req.headers_mut().insert(
            "x-big",
            "a".repeat(room - 9 - (11 + 55 + 4)).parse().unwrap(),
        );
        let _ = hop_in(Some(&t), parent, "forward", "a", "b", &mut req);
        assert!(req.headers().get("traceparent").is_some());
        assert_eq!(
            crate::http::client_head(&req).0,
            crate::http::MAX_REQUEST_HEAD_BYTES
        );
    }

    /// `remote`, `resent` and `status` are set as a default and then as
    /// what happened, like `outcome`: one of each on the span.
    #[test]
    fn traces_a_hop_writes_each_changing_attribute_once() {
        let (t, memory) = tracing(1.0, false);
        let mut req = http::Request::new(());
        let mut hop = hop_in(
            Some(&t),
            Some(test_context(true)),
            "call",
            "a",
            "b",
            &mut req,
        );
        let sent = req.headers()["traceparent"].clone();
        // Sent again on a request built again (a relayed upgrade): the
        // same header by the same rules, replacing a client's.
        let mut again = http::Request::builder()
            .header("traceparent", NO)
            .body(())
            .unwrap();
        hop.resend_in(true, &mut again);
        assert_eq!(again.headers()["traceparent"], sent);
        hop.answered(None);
        hop.answered(Some(http::StatusCode::SERVICE_UNAVAILABLE));
        hop.answered(Some(http::StatusCode::OK));
        drop(hop);
        // A second send with no room for the header: the client's does
        // not go on, and the hop says the header did not go out.
        let mut first = http::Request::new(());
        let mut hop = hop_in(
            Some(&t),
            Some(test_context(true)),
            "upgrade",
            "a",
            "b",
            &mut first,
        );
        let mut full = http::Request::builder().header("traceparent", "x");
        for i in 1..crate::http::MAX_REQUEST_HEADERS {
            full = full.header(format!("x-h{i}"), "v");
        }
        let mut full = full.body(()).unwrap();
        let room = crate::http::MAX_REQUEST_HEAD_BYTES - crate::http::client_head(&full).0;
        full.headers_mut()
            .insert("x-h1", "a".repeat(room).parse().unwrap());
        hop.resend_in(true, &mut full);
        assert!(full.headers().get("traceparent").is_none());
        drop(hop);
        let spans = memory.0.lock().unwrap();
        let values = |key: &str| -> Vec<String> {
            spans[0]
                .attributes
                .iter()
                .filter(|a| a.key.as_str() == key)
                .map(|a| a.value.to_string())
                .collect()
        };
        assert_eq!(values("resent"), ["true"]);
        assert_eq!(values("status"), ["200"]);
        assert_eq!(values("outcome"), ["answered"]);
        assert_eq!(values("carried"), ["true"]);
        let values = |key: &str| -> Vec<String> {
            spans[1]
                .attributes
                .iter()
                .filter(|a| a.key.as_str() == key)
                .map(|a| a.value.to_string())
                .collect()
        };
        assert_eq!(values("resent"), ["true"]);
        assert_eq!(values("carried"), ["false"]);
    }
    const YES: &str = "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01";
    const NO: &str = "00-1234567890abcdef1234567890abcdef-1234567890abcdef-00";

    #[test]
    fn traceparent_parsing_and_continuation() {
        assert_eq!(Trace::parse(YES).unwrap().header(), YES);
        for malformed in [
            "",
            "00",
            "00-00000000000000000000000000000000-1234567890abcdef-01",
            "00-1234567890abcdef1234567890abcdef-0000000000000000-01",
            "ff-1234567890abcdef1234567890abcdef-1234567890abcdef-01",
            "00-1234567890ABCDEF1234567890abcdef-1234567890abcdef-01",
            "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01-extra",
            "01-short",
            "01-1234567890abcdef1234567890abcdef-1234567890abcdef-01x",
            "00-1234567890abcdef1234567890abcdef-1234567890abcdef-0z",
        ] {
            assert!(Trace::parse(malformed).is_none(), "{malformed}");
        }
        assert!(Trace::parse(&YES.replacen("00-", "01-", 1)).is_some());
        assert!(Trace::parse(&format!("{}-future", YES.replacen("00-", "01-", 1))).is_some());
        assert_eq!(
            edge(Some(YES), 0.0, false).trace,
            Trace::parse(YES).unwrap().trace
        );
        let provider = SdkTracerProvider::builder().build();
        let t = Tracing {
            tracer: provider.tracer("test"),
            provider,
            ratio: 1.0,
            trust: false,
        };
        let parent = edge(Some(YES), 1.0, false);
        let child = Span::new(Some(&t), "request", Some(parent));
        assert_eq!(child.context.unwrap().trace, parent.trace);
        assert_ne!(child.context.unwrap().parent, parent.parent);
    }

    #[test]
    fn traceparent_trust_controls_both_sampled_flags() {
        assert!(!edge(Some(YES), 0.0, false).sampled);
        assert!(edge(Some(NO), 1.0, false).sampled);
        assert!(edge(Some(YES), 0.0, true).sampled);
        assert!(!edge(Some(NO), 1.0, true).sampled);
        assert!(!edge(Some("bad"), 0.0, true).sampled);
        assert!(edge(None, 1.0, true).sampled);
        // Reusing a client-selected ID does not fix the sampling decision.
        let sampled = (0..1000)
            .filter(|_| edge(Some(YES), 0.5, false).sampled)
            .count();
        assert!((300..700).contains(&sampled));
    }

    #[test]
    fn traces_off_preserves_headers_and_unsampled_allocates_no_span() {
        let mut req = http::Request::builder()
            .header("traceparent", "invalid-but-preserved")
            .header("tracestate", "vendor=value")
            .body(())
            .unwrap();
        let off = Span::new(None, "request", None);
        off.inject(&mut req);
        assert!(off.context.is_none() && off.span.is_none());
        assert_eq!(req.headers()["traceparent"], "invalid-but-preserved");
        assert_eq!(req.headers()["tracestate"], "vendor=value");
        let provider = SdkTracerProvider::builder().build();
        let t = Tracing {
            tracer: provider.tracer("test"),
            provider,
            ratio: 0.0,
            trust: false,
        };
        let span = Span::new(Some(&t), "request", Some(edge(Some(YES), 0.0, false)));
        assert!(span.span.is_none());
        span.inject(&mut req);
        assert!(
            req.headers()["traceparent"]
                .to_str()
                .unwrap()
                .ends_with("-00")
        );
        assert_eq!(req.headers()["tracestate"], "vendor=value");
    }

    #[derive(Debug, Clone, Default)]
    struct Memory(std::sync::Arc<std::sync::Mutex<Vec<opentelemetry_sdk::trace::SpanData>>>);
    impl opentelemetry_sdk::trace::SpanExporter for Memory {
        async fn export(
            &self,
            spans: Vec<opentelemetry_sdk::trace::SpanData>,
        ) -> opentelemetry_sdk::error::OTelSdkResult {
            self.0.lock().unwrap().extend(spans);
            Ok(())
        }
    }

    #[test]
    fn traces_outcome_is_written_once_and_the_last_set_wins() {
        let memory = Memory::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(memory.clone())
            .build();
        let t = Tracing {
            tracer: provider.tracer("test"),
            provider,
            ratio: 1.0,
            trust: false,
        };
        let mut ran = Span::new(Some(&t), "event", None);
        ran.attribute("outcome", "not_run");
        ran.set(KeyValue::new("outcome", "failed"));
        ran.set(KeyValue::new("outcome", "ok"));
        drop(ran);
        let mut refused = Span::new(Some(&t), "event", None);
        refused.attribute("outcome", "not_run");
        drop(refused);
        let outcomes: Vec<Vec<String>> = memory
            .0
            .lock()
            .unwrap()
            .iter()
            .map(|s| {
                s.attributes
                    .iter()
                    .filter(|a| a.key.as_str() == "outcome")
                    .map(|a| a.value.as_str().into_owned())
                    .collect()
            })
            .collect();
        assert_eq!(outcomes, [vec!["ok"], vec!["not_run"]]);
    }

    #[test]
    fn traces_method_is_a_known_name_or_other() {
        assert_eq!(method_name(&http::Method::GET), "GET");
        assert_eq!(method_name(&http::Method::PATCH), "PATCH");
        let long = http::Method::from_bytes("X".repeat(4096).as_bytes()).unwrap();
        assert_eq!(method_name(&long), "other");
        assert_eq!(
            method_name(&http::Method::from_bytes(b"get").unwrap()),
            "other"
        );
    }

    #[test]
    fn traces_key_family_is_read_at_its_place() {
        for (key, family) in [
            ("state/acme/shop/actors/cart/42/wal/0001.ltx", "wal"),
            (
                "pre/fix/state/acme/shop/actors/cart/42/snapshot/1.ltx",
                "snapshot",
            ),
            ("state/acme/shop/actors/cart/42/head", "history"),
            ("state/acme/shop/owners/cart/42", "owners"),
            ("state/acme/shop/alarms/0001", "alarms"),
            ("state/acme/shop/blobs/1/abc", "blobs"),
            ("apps/acme/shop/_assets/content/abc", "assets"),
            ("apps/acme/shop/current", "apps"),
            ("nodes/a", "nodes"),
            // Names an app chooses, where a family's name would be.
            ("state/wal/owners/actors/snapshot/wal/head", "history"),
            ("state/acme/shop/owners/wal/snapshot", "owners"),
            ("apps/nodes/wal/current", "apps"),
            ("elsewhere/wal/1", "other"),
            ("", "other"),
        ] {
            assert_eq!(key_family(key), family, "{key}");
        }
    }

    #[test]
    fn traces_call_header_is_the_hosts_or_absent() {
        let guest = "00-feedfacefeedfacefeedfacefeedface-feedfacefeedface-01";
        let call = || {
            http::Request::builder()
                .header("traceparent", guest)
                .body(())
                .unwrap()
        };
        // In an event: the event's context, whatever the guest set.
        let context = Trace::parse(NO).unwrap();
        let mut req = call();
        call_header(&mut req, Some(context));
        assert_eq!(req.headers()["traceparent"], NO);
        assert_eq!(req.headers().get_all("traceparent").iter().count(), 1);
        assert_eq!(req.extensions().get::<Trace>(), Some(&context));
        // With no context to give, the guest's does not travel either.
        let mut req = call();
        call_header(&mut req, None);
        assert!(req.headers().get("traceparent").is_none());
    }

    #[test]
    fn traces_guest_header_is_added_only_within_the_head_limits() {
        let context = Trace::parse(YES).unwrap();
        // Room for it: added; a client's value is replaced.
        let mut req = http::Request::builder()
            .header("traceparent", "x")
            .body(())
            .unwrap();
        set_guest_header(&mut req, context);
        assert_eq!(req.headers()["traceparent"], YES);
        // At the field count: the request goes on unchanged...
        let mut full = http::Request::builder();
        for i in 0..crate::http::MAX_REQUEST_HEADERS {
            full = full.header(format!("x-h{i}"), "v");
        }
        let mut req = full.body(()).unwrap();
        set_guest_header(&mut req, context);
        assert!(req.headers().get("traceparent").is_none());
        // ...unless the client's own header is the one replaced.
        let mut full = http::Request::builder().header("traceparent", NO);
        for i in 1..crate::http::MAX_REQUEST_HEADERS {
            full = full.header(format!("x-h{i}"), "v");
        }
        let mut req = full.body(()).unwrap();
        set_guest_header(&mut req, context);
        assert_eq!(req.headers()["traceparent"], YES);
        // At the byte limit, likewise unchanged.
        let mut req = http::Request::builder()
            .header(
                "x-big",
                "a".repeat(crate::http::MAX_REQUEST_HEAD_BYTES - 64),
            )
            .body(())
            .unwrap();
        set_guest_header(&mut req, context);
        assert!(req.headers().get("traceparent").is_none());
    }
}
