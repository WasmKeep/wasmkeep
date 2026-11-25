//! Traces on a cluster (ADR 0044, section 2): a call that leaves the node
//! carries the host's `traceparent`, never the calling guest's. Hop spans
//! and hop sampling are the next slice; this is the part that holds now.

use super::front::{front_call, front_get, front_node};
use super::*;
use opentelemetry_sdk::{
    error::OTelSdkResult,
    trace::{SdkTracerProvider, SpanData, SpanExporter},
};

#[derive(Debug, Clone, Default)]
struct Memory(Arc<std::sync::Mutex<Vec<SpanData>>>);
impl SpanExporter for Memory {
    async fn export(&self, spans: Vec<SpanData>) -> OTelSdkResult {
        self.0.lock().unwrap().extend(spans);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_guest_set_traceparent_does_not_cross_to_another_node() {
    // The guest's own header: another trace, with the sampled flag on.
    const GUEST: &str = "00-feedfacefeedfacefeedfacefeedface-feedfacefeedface-01";
    let memory = Memory::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(memory.clone())
        .build();
    // Both nodes are this process: every request is sampled, and an
    // incoming flag is honoured, so a guest's header that crossed would
    // show as a trace of its own on b.
    let _guard = wasmkeep_server::traces::install(provider, 1.0, true).unwrap();

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = front_node("a", da.path(), &store, true).await;
    let b = front_node("b", db.path(), &store, false).await;
    settle().await;
    let r = send_runnable(b.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "{}",
        r.body
    );

    // The front on a calls t/x on b, setting `traceparent` itself.
    let path = format!(
        "{}&traceparent={GUEST}",
        front_call("x", "GET", "/header?name=traceparent")
    );
    let r = front_get(a.addr, &path).await;
    assert_eq!(r.status, 200, "{}", r.body);
    // The callee's guest got its own event's context, in the front's trace.
    let seen = r
        .body
        .trim()
        .strip_prefix("200:")
        .expect("the callee's answer");
    assert!(!seen.contains("feedface"), "{seen}");

    // The front's span, and b's request span under it: the hop carried the
    // host's context (the front's), not the guest's.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let (front, hop) = loop {
        let found = {
            let spans = memory.0.lock().unwrap();
            let front = spans.iter().find(|s| {
                s.name == "front" && seen.contains(&s.span_context.trace_id().to_string())
            });
            front.and_then(|front| {
                spans
                    .iter()
                    .find(|s| {
                        s.name == "request" && s.parent_span_id == front.span_context.span_id()
                    })
                    .map(|hop| (front.clone(), hop.clone()))
            })
        };
        match found {
            Some(found) => break found,
            None if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            None => panic!("no request span on b under the front's span"),
        }
    };
    assert_eq!(
        hop.span_context.trace_id(),
        front.span_context.trace_id(),
        "the call stays in the front's trace"
    );
    assert_eq!(lease_holder(&store, "x").await.as_deref(), Some("b"));
    // Nothing anywhere joined the trace the guest named.
    assert!(
        memory
            .0
            .lock()
            .unwrap()
            .iter()
            .all(
                |s| s.span_context.trace_id().to_string() != "feedfacefeedfacefeedfacefeedface"
                    && s.parent_span_id.to_string() != "feedfacefeedface"
            )
    );
}
