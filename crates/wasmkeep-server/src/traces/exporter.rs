//! A bounded nonblocking queue in front of the OpenTelemetry exporter.
//! A dedicated runtime owns HTTP I/O; requests only try_send completed spans.
//! The export thread sleeps on both of its channels: nothing polls.
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use opentelemetry::{Context, KeyValue};
use opentelemetry_otlp::{WithExportConfig as _, WithHttpConfig as _};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::trace::{SdkTracerProvider, Span, SpanData, SpanExporter, SpanProcessor};

use crate::metrics::{METRICS, inc};

const QUEUE: usize = 2048;
const BATCH: usize = 256;
const EXPORT_TIMEOUT: Duration = Duration::from_secs(2);

/// How long a batch waits for more spans after its first.
const GATHER: Duration = Duration::from_millis(200);

#[derive(Debug)]
struct Control {
    stop: bool,
    deadline: Instant,
    ack: std::sync::mpsc::SyncSender<()>,
}
#[derive(Debug)]
struct Processor {
    spans: mpsc::Sender<SpanData>,
    control: mpsc::UnboundedSender<Control>,
    stopped: std::sync::atomic::AtomicBool,
}
impl Processor {
    fn new<E: SpanExporter + 'static>(exporter: E) -> Result<Self, String> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let (spans, rx) = mpsc::channel(QUEUE);
        let (control, commands) = mpsc::unbounded_channel::<Control>();
        std::thread::Builder::new()
            .name("wasmkeep-traces".into())
            .spawn(move || rt.block_on(export_loop(exporter, rx, commands)))
            .map_err(|e| e.to_string())?;
        Ok(Self {
            spans,
            control,
            stopped: false.into(),
        })
    }
    fn drain(&self, stop: bool, timeout: Duration) -> OTelSdkResult {
        if stop
            && self
                .stopped
                .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            return Ok(());
        }
        let (ack, done) = std::sync::mpsc::sync_channel(1);
        self.control
            .send(Control {
                stop,
                deadline: Instant::now() + timeout,
                ack,
            })
            .map_err(|_| OTelSdkError::AlreadyShutdown)?;
        done.recv_timeout(timeout)
            .map_err(|_| OTelSdkError::Timeout(timeout))
    }
}
/// The export thread's work: asleep until a span or a command arrives (no
/// timer runs while idle), then one batch at a time. A command (a flush or
/// the stop) is seen at once, except during an export, which
/// [`EXPORT_TIMEOUT`] bounds.
async fn export_loop<E: SpanExporter>(
    exporter: E,
    mut spans: mpsc::Receiver<SpanData>,
    mut commands: mpsc::UnboundedReceiver<Control>,
) {
    let mut last_warning = None;
    loop {
        let mut batch = Vec::with_capacity(BATCH);
        // Both senders live in the processor: either channel closing is
        // the processor gone.
        let mut command = tokio::select! {
            biased;
            command = commands.recv() => match command {
                Some(command) => Some(command),
                None => return,
            },
            span = spans.recv() => match span {
                Some(span) => {
                    batch.push(span);
                    None
                }
                None => return,
            },
        };
        if command.is_none() {
            let full = tokio::time::sleep(GATHER);
            tokio::pin!(full);
            while batch.len() < BATCH {
                tokio::select! {
                    biased;
                    next = commands.recv() => {
                        command = next;
                        break;
                    }
                    span = spans.recv() => match span {
                        Some(span) => batch.push(span),
                        None => break,
                    },
                    () = &mut full => break,
                }
            }
        }
        loop {
            // A flush or a stop takes what is queued now, batch by batch.
            if command.is_some() {
                while batch.len() < BATCH {
                    match spans.try_recv() {
                        Ok(span) => batch.push(span),
                        Err(_) => break,
                    }
                }
            }
            if batch.is_empty() {
                break;
            }
            let timeout = command.as_ref().map_or(EXPORT_TIMEOUT, |c| {
                c.deadline
                    .saturating_duration_since(Instant::now())
                    .min(EXPORT_TIMEOUT)
            });
            let result =
                tokio::time::timeout(timeout, exporter.export(std::mem::take(&mut batch))).await;
            let ok = matches!(result, Ok(Ok(())));
            inc(&METRICS
                .trace_exports
                .get(&[("outcome", if ok { "ok" } else { "error" })]));
            if !ok && last_warning.is_none_or(|at: Instant| at.elapsed() >= Duration::from_secs(60))
            {
                // No exporter error text: it may contain a credential or URL.
                tracing::warn!("trace export failed or timed out");
                last_warning = Some(Instant::now());
            }
            if command
                .as_ref()
                .is_none_or(|c| Instant::now() >= c.deadline)
            {
                break;
            }
        }
        if let Some(command) = command {
            let _ = command.ack.send(());
            if command.stop {
                return;
            }
        }
    }
}

impl SpanProcessor for Processor {
    fn on_start(&self, _: &mut Span, _: &Context) {}
    fn on_end(&self, span: SpanData) {
        if self.spans.try_send(span).is_err() {
            inc(&METRICS.trace_spans_dropped);
        }
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.drain(false, Duration::from_secs(3))
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.drain(true, timeout)
    }
}

pub(super) fn provider(endpoint: &str, node: &str) -> Result<SdkTracerProvider, String> {
    let uri: http::Uri = endpoint
        .parse()
        .map_err(|_| "otlp-endpoint must be an HTTP or HTTPS base URL")?;
    if !matches!(uri.scheme_str(), Some("http" | "https"))
        || uri.host().is_none()
        || uri.authority().is_some_and(|a| a.as_str().contains('@'))
        || uri.query().is_some()
    {
        return Err(
            "otlp-endpoint must be an HTTP or HTTPS base URL without credentials or a query".into(),
        );
    }
    let resource = Resource::builder_empty()
        .with_attributes([
            KeyValue::new("service.name", "wasmkeep"),
            KeyValue::new("service.version", crate::VERSION),
            KeyValue::new("service.instance.id", node.to_owned()),
        ])
        .build();
    // The exporter crate skips a compression it was not built with; say so
    // here, where the operator reads it, and start all the same.
    for name in [
        "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION",
        "OTEL_EXPORTER_OTLP_COMPRESSION",
    ] {
        if std::env::var(name).is_ok_and(|v| !v.is_empty() && !v.eq_ignore_ascii_case("none")) {
            tracing::warn!(
                variable = name,
                "trace export compression is not supported: spans are sent uncompressed"
            );
        }
    }
    // The endpoint named is the endpoint used: no redirect is followed (the
    // collector's credential headers would go with it) and no proxy is
    // taken from the environment.
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .timeout(EXPORT_TIMEOUT)
        .build()
        .map_err(|_| "building the trace exporter's HTTP client failed")?;
    let mut exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_http_client(client)
        .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
        .with_endpoint(format!("{}/v1/traces", endpoint.trim_end_matches('/')))
        .with_timeout(EXPORT_TIMEOUT)
        .build()
        .map_err(|e| format!("building trace exporter: {e}"))?;
    exporter.set_resource(&resource);
    Ok(SdkTracerProvider::builder()
        .with_resource(resource)
        .with_sampler(opentelemetry_sdk::trace::Sampler::AlwaysOn)
        .with_span_processor(Processor::new(exporter)?)
        .build())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use opentelemetry::trace::{Span as _, Tracer as _, TracerProvider as _};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn traces_exporter_sends_http_protobuf_and_flushes() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let collector = tokio::spawn(async move {
            let (mut io, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut block = [0; 4096];
            loop {
                let n = io.read(&mut block).await.unwrap();
                assert_ne!(n, 0);
                request.extend_from_slice(&block[..n]);
                if let Some(end) = request.windows(4).position(|s| s == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&request[..end]).to_lowercase();
                    let len: usize = head
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length: "))
                        .unwrap()
                        .parse()
                        .unwrap();
                    if request.len() >= end + 4 + len {
                        assert!(head.starts_with("post /v1/traces http/1.1"));
                        assert!(head.contains("content-type: application/x-protobuf"));
                        assert!(len > 0);
                        break;
                    }
                }
            }
            io.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/x-protobuf\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
        });
        let provider = provider(&format!("http://{addr}"), "test-node").unwrap();
        provider.tracer("test").start("request").end();
        tokio::task::spawn_blocking(move || provider.shutdown_with_timeout(Duration::from_secs(3)))
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), collector)
            .await
            .unwrap()
            .unwrap();
    }

    #[derive(Debug)]
    struct Stalled;
    impl SpanExporter for Stalled {
        async fn export(&self, _: Vec<SpanData>) -> OTelSdkResult {
            std::future::pending().await
        }
    }
    #[test]
    fn traces_exporter_full_queue_drops_and_stop_is_bounded() {
        let provider = SdkTracerProvider::builder()
            .with_span_processor(Processor::new(Stalled).unwrap())
            .build();
        let tracer = provider.tracer("test");
        for _ in 0..10_000 {
            tracer.start("event").end();
        }
        assert!(
            METRICS
                .trace_spans_dropped
                .load(std::sync::atomic::Ordering::Relaxed)
                > 0
        );
        let now = Instant::now();
        let _ = provider.shutdown_with_timeout(Duration::from_millis(20));
        assert!(now.elapsed() < Duration::from_secs(1));
    }
}
