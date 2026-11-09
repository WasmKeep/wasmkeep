//! The app class of the jobs example: an HTTP API over the SDK's KV and
//! queue helpers, and the consumer the `jobs` queue pushes to.
//!
//! Key-value (keys spread over 4 shards of class `kv`):
//!
//! - `GET /kv/<key>`: the value, with its version in `etag`; `404` if absent.
//!   `?cached=1` reads through this actor's read cache (up to 60 s stale for
//!   other actors' writes; this actor's own writes update it).
//! - `PUT /kv/<key>`: stores the body; `if-match: <version>` or
//!   `if-none-match: *` make it conditional (`409` when it does not hold);
//!   `?ttl_ms=N` expires it. Answers the new version.
//! - `DELETE /kv/<key>`: `200` if it was there, `404` otherwise.
//! - `GET /kv?prefix=P`: the keys starting with `P`, one per line.
//!
//! Queues (actors of class `queue`):
//!
//! - `POST /jobs`: queues the body on `queue/jobs`, which pushes it to
//!   `app/worker`. The worker stores the body in upper case at key
//!   `job:<id>`; a body starting with `fail` asks for a retry every time,
//!   so it ends in the dead-letter table after three attempts.
//! - `GET /jobs/stats`, `GET /jobs/dead`: counts and dead letters.
//! - `POST /inbox`: queues the body on `queue/inbox`, a pull queue.
//! - `POST /inbox/batch`: queues each line of the body as one message, in
//!   one durable write; answers their ids, one per line.
//! - `POST /inbox/lease?visibility_ms=N&max=M`: leases up to `M` (default
//!   one) messages, one per line, answered as
//!   `<id> <attempt> <body>` (empty when none is visible).
//! - `POST /inbox/ack?id=I&attempt=A`: `200` if that lease was current,
//!   `409` if it ran out (the message was leased again) or was settled.

use std::io::Write as _;
use wasmkeep_sdk::http::{self, IncomingRequest, Method, ResponseOutparam};
use wasmkeep_sdk::kv::{self, Expect, PutOptions};
use wasmkeep_sdk::queue::{self, Message, Outcome};

use wasmkeep_sdk::Actor;

const SHARDS: u32 = 4;

struct App;

impl Actor for App {}

/// Keys over 4 shards, with a 60 s read cache: `get` may use it, writes
/// through this client update it, `get_fresh` skips it.
fn store() -> kv::Client {
    kv::Client::new("kv", SHARDS).with_read_cache(60_000)
}

fn param<'a>(path: &'a str, name: &str) -> Option<&'a str> {
    path.split_once('?')?
        .1
        .split('&')
        .find_map(|p| p.strip_prefix(name)?.strip_prefix('='))
}

fn header(request: &IncomingRequest, name: &str) -> Option<String> {
    let values = request.headers().get(name);
    values
        .into_iter()
        .next()
        .and_then(|v| String::from_utf8(v).ok())
}

type Reply = (u16, Vec<(&'static str, String)>, Vec<u8>);

fn text(status: u16, body: impl Into<String>) -> Reply {
    (status, Vec::new(), body.into().into_bytes())
}

fn kv_route(request: &IncomingRequest, key: &str, path: &str) -> Reply {
    let kv = store();
    let result = match request.method() {
        Method::Get => {
            if param(path, "cached") == Some("1") {
                kv.get(key)
            } else {
                kv.get_fresh(key)
            }
        }
        .map(|entry| match entry {
            Some(e) => (200, vec![("etag", e.version.to_string())], e.value),
            None => text(404, "no such key\n"),
        }),
        Method::Put => {
            let Some(value) = http::read_body(request, 1 << 20) else {
                return text(413, "value over 1 MiB\n");
            };
            let expect = if let Some(v) = header(request, "if-match") {
                match v.trim_matches('"').parse() {
                    Ok(v) => Expect::Version(v),
                    Err(_) => return text(400, "if-match is not a version\n"),
                }
            } else if header(request, "if-none-match").as_deref() == Some("*") {
                Expect::Absent
            } else {
                Expect::Any
            };
            let options = PutOptions {
                expect,
                ttl_ms: param(path, "ttl_ms").and_then(|t| t.parse().ok()),
                ..PutOptions::default()
            };
            kv.put_with(key, value, options).map(|v| {
                (
                    200,
                    vec![("etag", v.to_string())],
                    format!("{v}\n").into_bytes(),
                )
            })
        }
        Method::Delete => kv.delete(key).map(|removed| {
            if removed {
                text(200, "deleted\n")
            } else {
                text(404, "no such key\n")
            }
        }),
        _ => Ok(text(405, "use GET, PUT or DELETE\n")),
    };
    result.unwrap_or_else(|e| match e {
        kv::Error::Conflict => text(409, "the key changed\n"),
        e => text(502, format!("{e}\n")),
    })
}

fn list(path: &str) -> Reply {
    let prefix = param(path, "prefix").unwrap_or_default();
    match store().list(prefix, None, 1000) {
        Ok(keys) => {
            let lines: String = keys
                .iter()
                .map(|k| format!("{}\n", String::from_utf8_lossy(k)))
                .collect();
            text(200, lines)
        }
        Err(e) => text(502, format!("{e}\n")),
    }
}

fn send(request: &IncomingRequest, queue: &str) -> Reply {
    let Some(body) = http::read_body(request, 1 << 20) else {
        return text(413, "message over 1 MiB\n");
    };
    match queue::Client::new(queue).send(body) {
        Ok(id) => text(200, format!("{id}\n")),
        Err(e) => text(502, format!("{e}\n")),
    }
}

fn send_batch(request: &IncomingRequest) -> Reply {
    let Some(body) = http::read_body(request, 1 << 20) else {
        return text(413, "batch over 1 MiB\n");
    };
    let lines: Vec<&[u8]> = body
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .collect();
    match queue::Client::new("queue/inbox").send_batch(&lines) {
        Ok(ids) => text(
            200,
            ids.iter().map(|id| format!("{id}\n")).collect::<String>(),
        ),
        Err(e) => text(502, format!("{e}\n")),
    }
}

fn stats(queue: &str) -> Reply {
    match queue::Client::new(queue).stats() {
        Ok(s) => text(
            200,
            format!(
                "ready={} delayed={} in_flight={} dead={}\n",
                s.ready, s.delayed, s.in_flight, s.dead
            ),
        ),
        Err(e) => text(502, format!("{e}\n")),
    }
}

fn dead(queue: &str) -> Reply {
    match queue::Client::new(queue).dead_letters(100) {
        Ok(items) => text(
            200,
            items
                .iter()
                .map(|d| format!("{} {} {}\n", d.message.id, d.message.attempt, d.reason))
                .collect::<String>(),
        ),
        Err(e) => text(502, format!("{e}\n")),
    }
}

fn lease(path: &str) -> Reply {
    let max = param(path, "max").and_then(|v| v.parse().ok()).unwrap_or(1);
    let visibility = param(path, "visibility_ms")
        .and_then(|v| v.parse().ok())
        .unwrap_or(30_000);
    match queue::Client::new("queue/inbox").lease(max, visibility) {
        Ok(messages) => text(
            200,
            messages
                .iter()
                .map(|m| {
                    format!(
                        "{} {} {}\n",
                        m.id,
                        m.attempt,
                        String::from_utf8_lossy(&m.body)
                    )
                })
                .collect::<String>(),
        ),
        Err(e) => text(502, format!("{e}\n")),
    }
}

fn ack(path: &str) -> Reply {
    let (Some(id), Some(attempt)) = (
        param(path, "id").and_then(|v| v.parse().ok()),
        param(path, "attempt").and_then(|v| v.parse().ok()),
    ) else {
        return text(400, "give id and attempt\n");
    };
    let message = Message {
        id,
        attempt,
        enqueued_at: 0,
        body: Vec::new(),
    };
    match queue::Client::new("queue/inbox").ack(&message) {
        Ok(true) => text(200, "acked\n"),
        Ok(false) => text(409, "that lease is no longer current\n"),
        Err(e) => text(502, format!("{e}\n")),
    }
}

/// The worker: one outcome per pushed job. Writing `job:<id>` is
/// idempotent, so a duplicate delivery does no harm.
fn work(message: &Message) -> Outcome {
    if message.body.starts_with(b"fail") {
        return Outcome::Retry(None);
    }
    let key = format!("job:{}", message.id);
    let done = String::from_utf8_lossy(&message.body).to_uppercase();
    match store().put_with(key, done, PutOptions::default()) {
        Ok(_) => Outcome::Ack,
        Err(_) => Outcome::Retry(None),
    }
}

impl http::Guest for App {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        if queue::is_delivery(&request) {
            queue::handle_delivery(request, out, |_queue, m| work(m));
            return;
        }
        let path = request.path_with_query().unwrap_or_default();
        let route = path.split('?').next().unwrap_or_default().to_owned();
        let post = matches!(request.method(), Method::Post);
        let get = matches!(request.method(), Method::Get);
        let (status, headers, body) = match route.as_str() {
            "/kv" if get => list(&path),
            r if r.starts_with("/kv/") && r.len() > 4 => kv_route(&request, &r[4..], &path),
            "/jobs" if post => send(&request, "queue/jobs"),
            "/jobs/stats" if get => stats("queue/jobs"),
            "/jobs/dead" if get => dead("queue/jobs"),
            "/inbox" if post => send(&request, "queue/inbox"),
            "/inbox/batch" if post => send_batch(&request),
            "/inbox/stats" if get => stats("queue/inbox"),
            "/inbox/lease" if post => lease(&path),
            "/inbox/ack" if post => ack(&path),
            _ => text(404, "see the README for routes\n"),
        };
        respond(out, status, &headers, &body);
    }
}

fn respond(out: ResponseOutparam, status: u16, extra: &[(&str, String)], body: &[u8]) {
    let headers = http::Fields::new();
    let _ = headers.set("content-type", &[b"text/plain".to_vec()]);
    for (name, value) in extra {
        let _ = headers.set(name, &[value.as_bytes().to_vec()]);
    }
    let response = http::OutgoingResponse::new(headers);
    let _ = response.set_status_code(status);
    let Ok(outgoing) = response.body() else {
        return;
    };
    ResponseOutparam::set(out, Ok(response));
    if let Ok(mut stream) = outgoing.write() {
        let _ = stream.write_all(body);
        let _ = stream.flush();
    }
    let _ = http::OutgoingBody::finish(outgoing, None);
}

wasmkeep_sdk::export!(App);
