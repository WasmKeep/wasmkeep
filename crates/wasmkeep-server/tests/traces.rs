//! One process, first off and then on: a real HTTP request, guest, mailbox
//! and counted in-memory bucket, with an in-memory span exporter.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use object_store::memory::InMemory;
use opentelemetry_sdk::{
    error::OTelSdkResult,
    trace::{SdkTracerProvider, SpanData, SpanExporter},
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use wasmkeep_runtime::{Runtime, RuntimeConfig};
use wasmkeep_server::{ActorConfig, ActorService, App, ClusterConfig, HttpComponent};
#[path = "support/guests.rs"]
mod guests;

#[derive(Debug, Clone, Default)]
struct Memory(Arc<Mutex<Vec<SpanData>>>);
impl SpanExporter for Memory {
    async fn export(&self, spans: Vec<SpanData>) -> OTelSdkResult {
        self.0.lock().unwrap().extend(spans);
        Ok(())
    }
}
async fn request(addr: std::net::SocketAddr, path: &str, header: &str) -> String {
    let mut io = TcpStream::connect(addr).await.unwrap();
    io.write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\ntraceparent: {header}\r\ntracestate: vendor=value\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    let mut response = String::new();
    io.read_to_string(&mut response).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    response
}
/// One node: the test actor as class `test`, the test front, a counted
/// in-memory bucket.
struct Node {
    addr: std::net::SocketAddr,
    service: ActorService,
    stop: tokio::sync::oneshot::Sender<()>,
    server: tokio::task::JoinHandle<Result<(), wasmkeep_server::Error>>,
    _directory: tempfile::TempDir,
}
impl Node {
    async fn start() -> Self {
        let wasm = guests::build_guest("tests/guests/test-actor", "test_actor");
        let runtime = Runtime::new(&RuntimeConfig::default()).unwrap();
        let component = guests::compile(&runtime, &wasm);
        let front_wasm = guests::build_guest("tests/guests/test-front", "test_front");
        let front =
            HttpComponent::new(runtime.clone(), &guests::compile(&runtime, &front_wasm)).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut config = ActorConfig::new(directory.path());
        let mut cluster = ClusterConfig::new(
            "traces",
            format!("http://{addr}"),
            Arc::new(InMemory::new()),
        );
        cluster.reads_secrets = false;
        cluster.placement = false;
        config.cluster = Some(cluster);
        let service =
            ActorService::new(runtime, config, vec![("test".into(), component, wasm)]).unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(wasmkeep_server::serve(
            listener,
            App {
                actors: Some(service.clone()),
                front: Some(front),
            },
            async {
                let _ = stopped.await;
            },
        ));
        Self {
            addr,
            service,
            stop,
            server,
            _directory: directory,
        }
    }
    async fn stop(self) {
        self.service.shutdown(Duration::from_secs(5)).await;
        self.stop.send(()).unwrap();
        self.server.await.unwrap().unwrap();
    }
}

/// Tracing on and nothing sampled (ratio 0, the client's flag not
/// trusted): requests are served, guests get a context to pass on, and no
/// span is made.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn traces_unsampled_serves_and_exports_nothing() {
    let node = Node::start().await;
    let memory = Memory::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(memory.clone())
        .build();
    let _guard = wasmkeep_server::traces::install(provider.clone(), 0.0, false).unwrap();
    // The client asks for sampling; without the trust flag it does not get it.
    let incoming = "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01";
    let direct = request(
        node.addr,
        "/actor/test/quiet/header?name=traceparent",
        incoming,
    )
    .await;
    let via_front = request(
        node.addr,
        "/call?to=test/quiet-front&path=%2Fheader%3Fname%3Dtraceparent",
        incoming,
    )
    .await;
    for response in [&direct, &via_front] {
        // The client's trace id goes on, under the host's own parent id,
        // unsampled.
        let seen = response
            .split("00-1234567890abcdef1234567890abcdef-")
            .nth(1)
            .unwrap_or_else(|| panic!("the trace id is continued: {response}"));
        assert!(!seen.starts_with("1234567890abcdef"), "{response}");
        assert_eq!(&seen[16..19], "-00", "{response}");
    }
    node.stop().await;
    provider.force_flush().unwrap();
    assert!(memory.0.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn traces_http_event_bucket_and_off() {
    let node = Node::start().await;
    let (addr, service) = (node.addr, node.service.clone());
    let memory = Memory::default();
    assert!(
        wasmkeep_server::traces::start(None, f64::NAN, true, "unused")
            .unwrap()
            .is_none()
    );
    let off = request(
        addr,
        "/actor/test/off/header?name=traceparent",
        "invalid-but-preserved",
    )
    .await;
    assert!(off.contains("invalid-but-preserved"));
    assert!(memory.0.lock().unwrap().is_empty());

    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(memory.clone())
        .build();
    let _guard = wasmkeep_server::traces::install(provider.clone(), 1.0, false).unwrap();
    let incoming = "00-1234567890abcdef1234567890abcdef-1234567890abcdef-00";
    let response = request(addr, "/actor/test/on/header?name=traceparent", incoming).await;
    let via_front = request(
        addr,
        "/call?to=test/via-front&path=%2Fheader%3Fname%3Dtraceparent",
        incoming,
    )
    .await;
    drop(service);
    node.stop().await;
    provider.force_flush().unwrap();
    let spans = memory.0.lock().unwrap();
    let request = spans
        .iter()
        .find(|s| s.name == "request")
        .expect("HTTP span");
    let event = spans
        .iter()
        .find(|s| s.name == "event")
        .expect("event span");
    assert_eq!(
        request.span_context.trace_id().to_string(),
        "1234567890abcdef1234567890abcdef"
    );
    assert_eq!(event.parent_span_id, request.span_context.span_id());
    assert!(
        spans
            .iter()
            .any(|s| s.name == "bucket" && s.parent_span_id == event.span_context.span_id())
    );
    assert!(spans.iter().any(|s| {
        s.name == "bucket"
            && s.attributes
                .iter()
                .any(|a| a.key.as_str() == "operation" && a.value.as_str() == "put")
    }));
    assert!(
        response.contains(&format!(
            "00-{}-{}-01",
            event.span_context.trace_id(),
            event.span_context.span_id()
        )),
        "{response}"
    );
    let front = spans
        .iter()
        .find(|s| s.name == "front")
        .expect("front span");
    let called = spans
        .iter()
        .find(|s| s.name == "event" && s.parent_span_id == front.span_context.span_id())
        .expect("front's local call event");
    assert!(via_front.contains(&format!(
        "00-{}-{}-01",
        called.span_context.trace_id(),
        called.span_context.span_id()
    )));
    assert!(
        spans
            .iter()
            .all(|s| s.span_context.trace_id() == request.span_context.trace_id())
    ); // Each event span says how it ended once: the default it starts with
    // is replaced, not joined, by what happened.
    for event in spans.iter().filter(|s| s.name == "event") {
        let outcomes: Vec<String> = event
            .attributes
            .iter()
            .filter(|a| a.key.as_str() == "outcome")
            .map(|a| a.value.as_str().into_owned())
            .collect();
        assert_eq!(outcomes, ["ok"], "{:?}", event.attributes);
    }
    // The method is one of the names the host knows, never the client's text.
    assert!(
        request
            .attributes
            .iter()
            .any(|a| { a.key.as_str() == "method" && a.value.as_str() == "GET" })
    );
}
