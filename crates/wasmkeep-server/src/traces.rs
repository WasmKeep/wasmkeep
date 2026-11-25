//! Request tracing (ADR 0044). Context is copied across task boundaries;
//! the task-local scope is only a convenience inside a single polled future.
//! TODO(ADR 0044 section 3, On a hop): authenticated sampling, call/hop
//! spans and propagation before admission signing, subject to head limits.

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

fn edge(value: Option<&str>, ratio: f64, trust: bool) -> Trace {
    let incoming = value.and_then(Trace::parse);
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
    /// span never carries two `outcome` attributes.
    outcome: Option<opentelemetry::Value>,
}
impl Span {
    fn new(t: Option<&Tracing>, name: &'static str, parent: Option<Trace>) -> Self {
        if t.is_none() {
            return Self {
                context: None,
                span: None,
                outcome: None,
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
                outcome: None,
            };
        };
        let mut context = parent.unwrap_or_else(|| edge(None, t.ratio, false));
        let span = if context.sampled {
            let kind = match name {
                "request" => opentelemetry::trace::SpanKind::Server,
                "bucket" => opentelemetry::trace::SpanKind::Client,
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
            outcome: None,
        }
    }
    pub(crate) fn child(name: &'static str) -> Self {
        Self::new(TRACING.get(), name, current())
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
        if attribute.key.as_str() == OUTCOME {
            self.outcome = Some(attribute.value);
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
            if let Some(outcome) = self.outcome.take() {
                s.set_attribute(KeyValue::new(OUTCOME, outcome));
            }
            s.end();
        }
    }
}

/// The one attribute a span sets more than once (a default, then what
/// happened): kept aside and written when the span ends.
const OUTCOME: &str = "outcome";

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
fn set_guest_header<B>(req: &mut http::Request<B>, context: Trace) {
    let (bytes, count) = crate::http::client_head(req);
    let (had_bytes, had) = req
        .headers()
        .get_all(TRACEPARENT)
        .iter()
        .fold((0, 0), |(bytes, count), v| {
            (bytes + TRACEPARENT.len() + v.len() + 4, count + 1)
        });
    let value = context.header();
    let fits = bytes - had_bytes + TRACEPARENT.len() + value.len() + 4
        <= crate::http::MAX_REQUEST_HEAD_BYTES
        && count - had < crate::http::MAX_REQUEST_HEADERS;
    if fits && let Ok(value) = value.parse() {
        req.headers_mut().insert(TRACEPARENT, value);
    }
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

pub(crate) fn request<B>(req: &mut http::Request<B>) -> Span {
    let Some(t) = TRACING.get() else {
        return Span::new(None, "request", None);
    };
    let parent = Some(edge(
        req.headers().get(TRACEPARENT).and_then(|h| h.to_str().ok()),
        t.ratio,
        t.trust,
    ));
    let mut span = Span::new(TRACING.get(), "request", parent);
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
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
