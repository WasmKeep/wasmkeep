//! Traces on a cluster (ADR 0044, sections 1 to 4): one request through
//! three nodes is one trace, a hop carries the sending node's context and
//! decision, and with tracing off nothing of it is on the wire.
//!
//! Every node of a test is this process, so they share one tracer and one
//! exporter: a span's node is its `node` attribute, not the resource. Each
//! test installs its tracer once, in a process of its own (nextest).

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

impl Memory {
    /// Tracing on for the process, every span kept here.
    fn install(ratio: f64, trust: bool) -> (Self, wasmkeep_server::traces::Guard) {
        let memory = Self::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(memory.clone())
            .build();
        let guard = wasmkeep_server::traces::install(provider, ratio, trust)
            .unwrap()
            .unwrap();
        (memory, guard)
    }

    /// The spans of trace `id` that are not bucket requests.
    fn trace(&self, id: &str) -> Vec<SpanData> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.span_context.trace_id().to_string() == id && s.name != "bucket")
            .cloned()
            .collect()
    }

    /// The bucket spans under span `parent`.
    fn bucket(&self, parent: &SpanData) -> Vec<SpanData> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.name == "bucket" && s.parent_span_id == parent.span_context.span_id())
            .cloned()
            .collect()
    }
}

fn attribute(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|a| a.key.as_str() == key)
        .map(|a| a.value.to_string())
}

/// The one span named `name` under `parent` in `spans`.
fn child<'a>(spans: &'a [SpanData], parent: &SpanData, name: &str) -> &'a SpanData {
    let found: Vec<&SpanData> = spans
        .iter()
        .filter(|s| s.name == name && s.parent_span_id == parent.span_context.span_id())
        .collect();
    assert_eq!(
        found.len(),
        1,
        "one {name} under {} {:?}: {:#?}",
        parent.name,
        parent.attributes,
        spans
            .iter()
            .map(|s| (
                s.name.to_string(),
                s.parent_span_id,
                s.span_context.span_id()
            ))
            .collect::<Vec<_>>()
    );
    found[0]
}

/// The names of `spans`, sorted.
fn names(spans: &[SpanData]) -> Vec<String> {
    let mut names: Vec<String> = spans.iter().map(|s| s.name.to_string()).collect();
    names.sort();
    names
}

/// A client's `traceparent` for trace number `n`, asking to be sampled.
fn client(n: u32) -> (String, String) {
    let id = format!("{:032x}", 0x00c0_ffee_0000_0000_u128 + u128::from(n));
    let header = format!("traceparent: 00-{id}-00000000000000c1-01\r\n");
    (id, header)
}

/// Three nodes of one app: `a` with the front, `b` holding `t/x`, `c`
/// holding `t/y`. With every node's view settled and one call through all
/// three already answered.
async fn three(store: &Arc<dyn ObjectStore>, dirs: &[tempfile::TempDir; 3]) -> [Node; 3] {
    let a = front_node("a", dirs[0].path(), store, true).await;
    let b = front_node("b", dirs[1].path(), store, false).await;
    let c = front_node("c", dirs[2].path(), store, false).await;
    settle().await;
    for (node, id, name) in [(&b, "b", "x"), (&c, "c", "y")] {
        let r = send_runnable(node.addr, "POST", &format!("/actor/t/{name}/durable"), "").await;
        assert_eq!((r.status, r.node.as_deref()), (200, Some(id)), "{}", r.body);
    }
    // Until each node reaches the next (a peer briefly out of a view
    // answers `err:failed`): the measured requests are then sent once.
    eventually("a call through the three nodes", async || {
        let r = front_get(a.addr, &through()).await;
        r.status == 200 && r.body.trim().starts_with("200:200:")
    })
    .await;
    [a, b, c]
}

/// The front's path that calls `t/x` (on b), which calls `t/y` (on c),
/// which writes durably.
fn through() -> String {
    front_call("x", "POST", "/call-many?to=t/y&path=%2Fdurable")
}

/// The request ADR 0044 is tested by (the 0.9.0 exit criterion): a front
/// on node a calls an actor on b, which calls an actor on c, which writes
/// to the bucket. One trace id, spans from the three nodes, the parents of
/// section 1, and the write under the last event.
#[tokio::test(flavor = "multi_thread")]
async fn one_request_through_three_nodes_is_one_trace() {
    let (memory, _guard) = Memory::install(1.0, false);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs = [(); 3].map(|()| tempfile::tempdir().unwrap());
    let [a, _b, _c] = three(&store, &dirs).await;

    let (id, header) = client(1);
    let r = send(a.addr, "GET", &through(), &header).await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert!(r.body.trim().starts_with("200:200:"), "{}", r.body);
    assert_eq!(lease_holder(&store, "x").await.as_deref(), Some("b"));
    assert_eq!(lease_holder(&store, "y").await.as_deref(), Some("c"));

    // Ten spans, the last of them a's request, which ends after its answer.
    eventually("the trace's ten spans", async || {
        memory.trace(&id).len() >= 10
    })
    .await;
    let spans = memory.trace(&id);
    assert_eq!(
        names(&spans),
        [
            "call", "call", "event", "event", "front", "hop", "hop", "request", "request",
            "request"
        ]
    );
    let root: Vec<&SpanData> = spans
        .iter()
        .filter(|s| s.parent_span_id.to_string() == "00000000000000c1")
        .collect();
    assert_eq!(root.len(), 1, "one span under the client's");
    let request_a = root[0];
    assert_eq!(request_a.name, "request");
    let front = child(&spans, request_a, "front");
    let call_a = child(&spans, front, "call");
    let hop_ab = child(&spans, call_a, "hop");
    let request_b = child(&spans, hop_ab, "request");
    let event_b = child(&spans, request_b, "event");
    let call_b = child(&spans, event_b, "call");
    let hop_bc = child(&spans, call_b, "hop");
    let request_c = child(&spans, hop_bc, "request");
    let event_c = child(&spans, request_c, "event");

    // Spans from three nodes.
    for (span, node) in [
        (request_a, "a"),
        (hop_ab, "a"),
        (request_b, "b"),
        (event_b, "b"),
        (hop_bc, "b"),
        (request_c, "c"),
        (event_c, "c"),
    ] {
        assert_eq!(
            attribute(span, "node").as_deref(),
            Some(node),
            "{} {:?}",
            span.name,
            span.attributes
        );
    }
    for (hop, peer) in [(hop_ab, "b"), (hop_bc, "c")] {
        for (key, value) in [
            ("kind", "call"),
            ("peer", peer),
            ("status", "200"),
            ("outcome", "answered"),
            ("resent", "false"),
            ("carried", "true"),
        ] {
            assert_eq!(attribute(hop, key).as_deref(), Some(value), "{key}");
        }
    }
    for (call, actor) in [(call_a, "x"), (call_b, "y")] {
        for (key, value) in [
            ("class", "t"),
            ("actor", actor),
            ("remote", "true"),
            ("status", "200"),
            ("outcome", "answered"),
        ] {
            assert_eq!(attribute(call, key).as_deref(), Some(value), "{key}");
        }
    }
    assert_eq!(attribute(event_b, "actor").as_deref(), Some("x"));
    assert_eq!(attribute(event_c, "actor").as_deref(), Some("y"));
    // The write that acknowledges, under the last event.
    let writes: Vec<SpanData> = memory
        .bucket(event_c)
        .into_iter()
        .filter(|s| attribute(s, "operation").as_deref() == Some("put"))
        .collect();
    assert!(
        writes
            .iter()
            .any(|s| attribute(s, "family").as_deref() == Some("wal")),
        "{writes:#?}"
    );
    // Every bucket span of the trace hangs under one of its events.
    for span in memory.0.lock().unwrap().iter() {
        if span.name == "bucket" && span.span_context.trace_id().to_string() == id {
            assert!(
                [event_b, event_c]
                    .iter()
                    .any(|e| e.span_context.span_id() == span.parent_span_id),
                "{span:#?}"
            );
        }
    }
}

/// A trace is sampled on every node it crosses, or on none (ADR 0044,
/// section 4): the first node draws, and the hops carry its decision. The
/// client asks for sampling on every request and does not decide it (no
/// trust flag). Before hops carried the decision, each node drew for
/// itself: at this ratio most traces were then in pieces.
#[tokio::test(flavor = "multi_thread")]
async fn a_trace_is_sampled_on_every_node_or_on_none() {
    let (memory, _guard) = Memory::install(0.5, false);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dirs = [(); 3].map(|()| tempfile::tempdir().unwrap());
    let [a, _b, _c] = three(&store, &dirs).await;

    // Through three nodes by calls, and through two by a forward.
    const EACH: u32 = 40;
    let whole_call = [
        "call", "call", "event", "event", "front", "hop", "hop", "request", "request", "request",
    ];
    let whole_forward = ["event", "hop", "request", "request"];
    let mut sent = Vec::new();
    for n in 0..EACH {
        let (id, header) = client(n);
        let r = send(a.addr, "GET", &through(), &header).await;
        assert!(r.body.trim().starts_with("200:200:"), "{}", r.body);
        sent.push((id, &whole_call[..]));
        let (id, header) = client(1_000 + n);
        let r = send(a.addr, "GET", "/actor/t/x/durable", &header).await;
        assert_eq!(
            (r.status, r.node.as_deref()),
            (200, Some("b")),
            "{}",
            r.body
        );
        sent.push((id, &whole_forward[..]));
    }
    let partial = || {
        sent.iter()
            .map(|(id, whole)| (names(&memory.trace(id)), *whole))
            .filter(|(got, whole)| !got.is_empty() && got != whole)
            .collect::<Vec<_>>()
    };
    // The last spans (a's requests) end after their answers.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !partial().is_empty() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(partial(), [], "traces sampled on some nodes only");
    // The node drew: some sampled, some not, of each kind, whatever the
    // client's flag said.
    for whole in [&whole_call[..], &whole_forward[..]] {
        let sampled = sent
            .iter()
            .filter(|(id, w)| *w == whole && !memory.trace(id).is_empty())
            .count();
        assert!(
            (1..EACH as usize).contains(&sampled),
            "{sampled} of {EACH} sampled"
        );
    }
}

/// A forward and a relayed WebSocket upgrade are hops in the request's
/// trace (ADR 0044, section 3): the owner's `request` span is under the
/// forwarding node's `hop` span, and the owner's guest sees its own
/// event's context. The forward's proof, computed over the header the
/// node set, checks at the owner (it would be a client's request there
/// otherwise, and its flag not followed: the trust flag is off).
#[tokio::test(flavor = "multi_thread")]
async fn a_forward_and_a_relayed_upgrade_are_hops_of_the_trace() {
    let (memory, _guard) = Memory::install(1.0, false);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = front_node("a", da.path(), &store, false).await;
    let b = front_node("b", db.path(), &store, false).await;
    settle().await;
    let r = send_runnable(b.addr, "POST", "/actor/t/x/durable", "").await;
    assert_eq!(
        (r.status, r.node.as_deref()),
        (200, Some("b")),
        "{}",
        r.body
    );
    eventually("a forwards to b", async || {
        let r = send(a.addr, "GET", "/actor/t/x/durable", "").await;
        (r.status, r.node.as_deref()) == (200, Some("b"))
    })
    .await;

    // A forward, with and without a `traceparent` of the client's.
    for (n, own) in [(1, true), (2, false)] {
        let (id, header) = client(n);
        let extra = if own {
            header
        } else {
            // Continued from no header: found by what the guest saw.
            String::new()
        };
        let r = send(a.addr, "GET", "/actor/t/x/header?name=traceparent", &extra).await;
        assert_eq!(
            (r.status, r.node.as_deref()),
            (200, Some("b")),
            "{}",
            r.body
        );
        let seen = r.body.trim().to_owned();
        let id = if own { id } else { seen[3..35].to_owned() };
        eventually("the forward's four spans", async || {
            memory.trace(&id).len() >= 4
        })
        .await;
        let spans = memory.trace(&id);
        assert_eq!(names(&spans), ["event", "hop", "request", "request"]);
        let hop = spans.iter().find(|s| s.name == "hop").unwrap();
        let request_a = spans
            .iter()
            .find(|s| s.span_context.span_id() == hop.parent_span_id)
            .unwrap();
        assert_eq!(request_a.name, "request");
        assert_eq!(attribute(request_a, "node").as_deref(), Some("a"));
        let request_b = child(&spans, hop, "request");
        let event_b = child(&spans, request_b, "event");
        for (key, value) in [
            ("kind", "forward"),
            ("node", "a"),
            ("peer", "b"),
            ("status", "200"),
            ("carried", "true"),
        ] {
            assert_eq!(attribute(hop, key).as_deref(), Some(value), "{key}");
        }
        assert_eq!(attribute(request_b, "node").as_deref(), Some("b"));
        // The guest on b got its event's context, sampled.
        assert_eq!(
            seen,
            format!("00-{id}-{}-01", event_b.span_context.span_id()),
            "the owner's guest sees its event"
        );
    }

    // A WebSocket upgrade relayed to the owner.
    let (id, header) = client(3);
    let mut req = {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
        format!("ws://{}/actor/t/x/ws", a.addr)
            .into_client_request()
            .unwrap()
    };
    let value = header
        .trim()
        .strip_prefix("traceparent: ")
        .unwrap()
        .to_owned();
    req.headers_mut()
        .insert("traceparent", value.parse().unwrap());
    let (ws, response) = tokio_tungstenite::connect_async(req).await.unwrap();
    assert_eq!(response.status(), 101);
    eventually("the upgrade's spans", async || {
        let spans = memory.trace(&id);
        spans.iter().any(|s| s.name == "hop")
            && spans.iter().filter(|s| s.name == "request").count() == 2
            && spans.iter().any(|s| s.name == "event")
    })
    .await;
    let spans = memory.trace(&id);
    let hop = spans.iter().find(|s| s.name == "hop").unwrap();
    for (key, value) in [
        ("kind", "upgrade"),
        ("node", "a"),
        ("peer", "b"),
        ("status", "101"),
        ("outcome", "answered"),
    ] {
        assert_eq!(attribute(hop, key).as_deref(), Some(value), "{key}");
    }
    let request_b = child(&spans, hop, "request");
    assert_eq!(attribute(request_b, "node").as_deref(), Some("b"));
    // The socket's open runs on b, in the trace.
    assert!(
        spans
            .iter()
            .any(|s| s.name == "event" && attribute(s, "node").as_deref() == Some("b")),
        "{:?}",
        names(&spans)
    );
    drop(ws);
}

/// With tracing off (no endpoint), nothing of it is on a hop (ADR 0044,
/// section 2): a client's `traceparent` reaches the owner's guest byte for
/// byte across a forward, none is added, and a guest's own header crosses
/// a call as it did in 0.8.
#[tokio::test(flavor = "multi_thread")]
async fn tracing_off_adds_nothing_to_a_hop() {
    const GUEST: &str = "00-feedfacefeedfacefeedfacefeedface-feedfacefeedface-01";
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
    let path = "/actor/t/x/header?name=traceparent";
    eventually("a forwards to b", async || {
        let r = send(a.addr, "GET", path, "").await;
        (r.status, r.node.as_deref()) == (200, Some("b"))
    })
    .await;
    // A forward: none added, and the client's as it came, whatever it is.
    let r = send(a.addr, "GET", path, "").await;
    assert_eq!((r.body.trim(), r.node.as_deref()), ("none", Some("b")));
    for value in ["not a trace context", GUEST] {
        let r = send(a.addr, "GET", path, &format!("traceparent: {value}\r\n")).await;
        assert_eq!((r.body.trim(), r.node.as_deref()), (value, Some("b")));
    }
    // A call: the guest's own header, or none.
    let call = front_call("x", "GET", "/header?name=traceparent");
    let r = front_get(a.addr, &call).await;
    assert_eq!(r.body.trim(), "200:none", "{}", r.body);
    let r = front_get(a.addr, &format!("{call}&traceparent={GUEST}")).await;
    assert_eq!(r.body.trim(), format!("200:{GUEST}"), "{}", r.body);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_guest_set_traceparent_does_not_cross_to_another_node() {
    // The guest's own header: another trace, with the sampled flag on.
    const GUEST: &str = "00-feedfacefeedfacefeedfacefeedface-feedfacefeedface-01";
    // Both nodes are this process: every request is sampled, and an
    // incoming flag is honoured, so a guest's header that crossed would
    // show as a trace of its own on b.
    let (memory, _guard) = Memory::install(1.0, true);

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

    // The front's span, and under it the call, its hop and b's request
    // span: the hop carried the host's context, not the guest's.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let (front, request) = loop {
        let found = {
            let spans = memory.0.lock().unwrap();
            let under = |parent: &SpanData, name: &str| {
                spans
                    .iter()
                    .find(|s| s.name == name && s.parent_span_id == parent.span_context.span_id())
                    .cloned()
            };
            spans
                .iter()
                .find(|s| {
                    s.name == "front" && seen.contains(&s.span_context.trace_id().to_string())
                })
                .and_then(|front| {
                    let call = under(front, "call")?;
                    let hop = under(&call, "hop")?;
                    Some((front.clone(), under(&hop, "request")?))
                })
        };
        match found {
            Some(found) => break found,
            None if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            None => panic!("no request span on b under the front's call"),
        }
    };
    assert_eq!(
        request.span_context.trace_id(),
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
