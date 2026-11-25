//! Test front: a component of the `wasmkeep:actor/front` world. Routes (any
//! method; the query's values percent-encoded):
//! - `/call?to=CLASS/NAME&path=P[&method=M][&delay=MS][&spoof][&traceparent=V][&copy]`:
//!   waits MS (no CPU), then `actors.call` with M (GET) and P; answers
//!   `STATUS:BODY` (the body's trailing whitespace trimmed),
//!   or `err:KIND[:DETAIL]`. With `spoof`, the request carries
//!   `x-wasmkeep-` headers the host must drop. With `traceparent=V`, it
//!   carries `traceparent: V`, which the host replaces when tracing is on
//!   (ADR 0044). With `copy`, answers the callee's
//!   status, headers (trapping if one is refused) and body as they came,
//!   plus `x-callee-range: NAMES`: the names of the callee's headers in
//!   the `x-wasmkeep-` range, comma-separated.
//! - `/call-many?to=A,B,...&path=P[&method=M][&body=N][&delay=MS]`: one
//!   `calls.call-many` with an entry per address (CLASS/NAME, comma-separated,
//!   or CLASS/NAME;PATH for an entry's own path), each M (GET) P with an
//!   N-byte body; answers each entry as `/call` does,
//!   joined by ` | `, in input order
//! - `/headers`: the method, then every request header as `name: value`
//! - `/respond?status=N[&spoof]`: answers status N (with `spoof`, with
//!   `x-wasmkeep-` headers the host must drop)
//! - `/after?to=CLASS/NAME[&status=N][&len]`: answers status N (200), with
//!   `Content-Length: 4` and `body` when `len`; finishes the body (with
//!   `len`, only after the call), waits 300 ms, then `POST /durable` on
//!   the actor: the call must complete although the client has its
//!   whole answer
//! - `/self`: `self-address` as `tenant/class/name`
//! - `/env?name=N`: `env.get(N)`, or `none`
//! - `/sleep?ms=MS`: waits MS (no CPU), answers `slept`
//! - `/spin`: never returns (CPU budget)
//! - `/print`: prints a line to stdout, answers `printed`
//! - `/trailers-in`: reads the body to its end, answers the request's
//!   trailers as `name: value` lines
//! - `/trailers-out`: answers `body` with trailers in and out of the
//!   `x-wasmkeep-` range
//!
//! Every answer whose headers or trailers the guest could not set ends
//! with a line `refused:NAMES` (comma-separated).
//! - `/stream-spin`: answers 200, sends `started`, then never returns
//! - `/handoff?class=C&name=N&path=P[&delay=MS][&dup][&partial][&raw]`:
//!   waits MS, then answers 426 with the three hand-off headers (ADR 0027):
//!   N percent-encoded (as given with `raw`), the class header twice with
//!   `dup`, without the path header with `partial`
//! - `/proxy?url=U`: an outbound GET of U, answered as it came, as a
//!   front passing an upstream's answer on would: its status, every
//!   header it may set (the others skipped) and the body, plus
//!   `x-upstream-range: NAMES`, the names of the upstream's headers in
//!   the `x-wasmkeep-` range, comma-separated; or 502 and the error
//! - the files routes of `../blob-routes.rs` (`/b/`, `/head/`, `/list`,
//!   `/stream/`, `/link/`, `/upload-link/`), over the front's own namespace
//! - anything else: `front ok`

use std::io::Write as _;

use wasip2::http::types::{
    Fields, IncomingBody, IncomingRequest, Method, OutgoingBody, OutgoingResponse, ResponseOutparam,
};

#[allow(unsafe_code, clippy::all)]
mod bindings {
    wit_bindgen::generate!({
        path: "../../../../wasmkeep-wit/wit",
        world: "front-core",
        with: {
            "wasi:io/error@0.2.12": wasip2::io::error,
            "wasi:io/poll@0.2.12": wasip2::io::poll,
            "wasi:io/streams@0.2.12": wasip2::io::streams,
        },
    });
}

use bindings::wasmkeep::actor::{actors, blobs, calls, env};

include!("../../blob-routes.rs");

/// The whole request body, or none past 32 MiB.
fn whole_body(request: &IncomingRequest) -> Vec<u8> {
    let Ok(incoming) = request.consume() else {
        return Vec::new();
    };
    let mut bytes = Vec::new();
    if let Ok(stream) = incoming.stream() {
        while let Ok(chunk) = stream.blocking_read(65_536) {
            bytes.extend_from_slice(&chunk);
            if bytes.len() > 32 << 20 {
                return Vec::new();
            }
        }
    }
    drop(IncomingBody::finish(incoming));
    bytes
}

wasip2::http::proxy::export!(Front);

struct Front;

fn decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(if b[i] == b'+' { b' ' } else { b[i] });
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Percent-encodes every byte but ASCII letters, digits and `-._~`.
fn encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn param(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
        (k == key).then(|| decode(v))
    })
}

fn sleep_ms(ms: u64) {
    wasip2::clocks::monotonic_clock::subscribe_duration(ms * 1_000_000).block();
}

/// `fields` with `headers` appended; the names refused, as a last line.
fn fields_of(headers: &[(&str, &str)]) -> (Fields, String) {
    let fields = Fields::new();
    let refused: Vec<&str> = headers
        .iter()
        .filter(|(k, v)| fields.append(k, v.as_bytes()).is_err())
        .map(|(k, _)| *k)
        .collect();
    let note = if refused.is_empty() {
        String::new()
    } else {
        format!("\nrefused:{}", refused.join(","))
    };
    (fields, note)
}

fn respond(out: ResponseOutparam, status: u16, headers: &[(&str, &str)], body: &str) {
    let (fields, refused) = fields_of(headers);
    let body = format!("{body}{refused}");
    let response = OutgoingResponse::new(fields);
    let _ = response.set_status_code(status);
    let outgoing = response.body().unwrap();
    ResponseOutparam::set(out, Ok(response));
    let mut stream = outgoing.write().unwrap();
    let _ = stream.write_all(body.as_bytes());
    let _ = stream.flush();
    drop(stream);
    let _ = OutgoingBody::finish(outgoing, None);
}

const SPOOFED: &[(&str, &str)] = &[
    ("x-wasmkeep-caller", "default/admin/root"),
    ("x-wasmkeep-actor", "default/admin/root"),
    ("x-wasmkeep-forwarded", "1"),
    ("x-wasmkeep-node", "elsewhere"),
    ("x-wasmkeep-chain", "default/t/x"),
    ("X-WasmKeep-Internal", "v4 0 0 0 0"),
    ("x-wasmkeep-future", "anything"),
];

/// A call's result as `STATUS:BODY` (trailing whitespace trimmed) or
/// `err:KIND[:DETAIL]`.
fn answer(result: Result<actors::Response, actors::CallError>) -> String {
    match result {
        Ok(r) => format!(
            "{}:{}",
            r.status,
            String::from_utf8_lossy(&r.body).trim_end()
        ),
        Err(actors::CallError::InvalidAddress(m)) => format!("err:invalid-address:{m}"),
        Err(actors::CallError::Reentrant) => "err:reentrant".into(),
        Err(actors::CallError::TooLarge) => "err:too-large".into(),
        Err(actors::CallError::Timeout) => "err:timeout".into(),
        Err(actors::CallError::Failed(m)) => format!("err:failed:{m}"),
    }
}

fn method_name(method: &Method) -> String {
    match method {
        Method::Get => "GET".into(),
        Method::Head => "HEAD".into(),
        Method::Post => "POST".into(),
        Method::Put => "PUT".into(),
        Method::Delete => "DELETE".into(),
        Method::Connect => "CONNECT".into(),
        Method::Options => "OPTIONS".into(),
        Method::Trace => "TRACE".into(),
        Method::Patch => "PATCH".into(),
        Method::Other(other) => other.clone(),
    }
}

/// An upstream's answer to a GET of `url`: status, headers, body.
type Upstream = (u16, Vec<(String, Vec<u8>)>, Vec<u8>);

fn proxy(url: &str) -> Result<Upstream, String> {
    use wasip2::http::{outgoing_handler, types};
    let (scheme, rest) = url.split_once("://").ok_or("no scheme")?;
    let (authority, path) = rest.find('/').map_or((rest, "/"), |i| rest.split_at(i));
    let request = types::OutgoingRequest::new(Fields::new());
    let scheme = if scheme == "https" {
        types::Scheme::Https
    } else {
        types::Scheme::Http
    };
    request.set_scheme(Some(&scheme)).map_err(|()| "scheme")?;
    request
        .set_authority(Some(authority))
        .map_err(|()| "authority")?;
    request
        .set_path_with_query(Some(path))
        .map_err(|()| "path")?;
    let body = request.body().map_err(|()| "body")?;
    OutgoingBody::finish(body, None).map_err(|e| format!("{e:?}"))?;
    let pending = outgoing_handler::handle(request, None).map_err(|e| format!("{e:?}"))?;
    pending.subscribe().block();
    let response = pending
        .get()
        .ok_or("no answer")?
        .map_err(|()| "taken")?
        .map_err(|e| format!("{e:?}"))?;
    let headers = response.headers().entries();
    let status = response.status();
    let incoming = response.consume().map_err(|()| "consume")?;
    let mut bytes = Vec::new();
    {
        let stream = incoming.stream().map_err(|()| "stream")?;
        loop {
            match stream.blocking_read(65536) {
                Ok(chunk) => bytes.extend_from_slice(&chunk),
                Err(wasip2::io::streams::StreamError::Closed) => break,
                Err(e) => return Err(format!("{e:?}")),
            }
        }
    }
    drop(IncomingBody::finish(incoming));
    Ok((status, headers, bytes))
}

impl wasip2::exports::http::incoming_handler::Guest for Front {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let full = request.path_with_query().unwrap_or_default();
        let (path, query) = full.split_once('?').unwrap_or((full.as_str(), ""));
        if ["/b/", "/head/", "/stream/", "/link/", "/upload-link/"]
            .iter()
            .any(|p| path.starts_with(p))
            || path == "/list"
        {
            let headers = request.headers().entries();
            let header = |name: &str| {
                headers
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(name))
                    .map(|(_, v)| String::from_utf8_lossy(v).into_owned())
            };
            let (if_match, if_none_match) = (header("if-match"), header("if-none-match"));
            let method = method_name(&request.method());
            let body = whole_body(&request);
            let (status, body) = blob_route(
                &method,
                &full,
                if_match,
                if_none_match.as_deref() == Some("*"),
                body,
            )
            .unwrap_or((404, b"no route".to_vec()));
            respond(out, status, &[], &String::from_utf8_lossy(&body));
            return;
        }
        match path {
            "/call" => {
                if let Some(ms) = param(query, "delay").and_then(|v| v.parse().ok()) {
                    sleep_ms(ms);
                }
                let to = param(query, "to").unwrap_or_default();
                let mut headers = Vec::new();
                if param(query, "spoof").is_some() {
                    for (k, v) in SPOOFED {
                        headers.push(((*k).to_owned(), (*v).to_owned()));
                    }
                }
                if let Some(value) = param(query, "traceparent") {
                    headers.push(("traceparent".to_owned(), value));
                }
                let request = actors::Request {
                    method: param(query, "method").unwrap_or_else(|| "GET".into()),
                    path: param(query, "path").unwrap_or_else(|| "/".into()),
                    headers,
                    body: Vec::new(),
                };
                if param(query, "copy").is_some() {
                    // As a front passing the callee's answer on would: its
                    // status, every header and the body as they came.
                    let r = actors::call(&to, &request).expect("the call");
                    let range: Vec<&str> = r
                        .headers
                        .iter()
                        .map(|(k, _)| k.as_str())
                        .filter(|k| k.to_ascii_lowercase().starts_with("x-wasmkeep-"))
                        .collect();
                    let fields = Fields::new();
                    for (k, v) in &r.headers {
                        fields
                            .append(k, v.as_bytes())
                            .expect("a header of the callee's answer");
                    }
                    fields
                        .append("x-callee-range", range.join(",").as_bytes())
                        .unwrap();
                    let response = OutgoingResponse::new(fields);
                    let _ = response.set_status_code(r.status);
                    let outgoing = response.body().unwrap();
                    ResponseOutparam::set(out, Ok(response));
                    let mut stream = outgoing.write().unwrap();
                    let _ = stream.write_all(&r.body);
                    let _ = stream.flush();
                    drop(stream);
                    let _ = OutgoingBody::finish(outgoing, None);
                    return;
                }
                let answer = answer(actors::call(&to, &request));
                respond(out, 200, &[], &answer);
            }
            "/call-many" => {
                if let Some(ms) = param(query, "delay").and_then(|v| v.parse().ok()) {
                    sleep_ms(ms);
                }
                let to = param(query, "to").unwrap_or_default();
                let len: usize = param(query, "body")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                let request = actors::Request {
                    method: param(query, "method").unwrap_or_else(|| "GET".into()),
                    path: param(query, "path").unwrap_or_else(|| "/".into()),
                    headers: Vec::new(),
                    body: vec![b'x'; len],
                };
                let entries: Vec<(String, actors::Request)> = to
                    .split(',')
                    .filter(|a| !a.is_empty())
                    .map(|a| match a.split_once(';') {
                        Some((a, path)) => (
                            a.to_owned(),
                            actors::Request {
                                path: path.to_owned(),
                                ..request.clone()
                            },
                        ),
                        None => (a.to_owned(), request.clone()),
                    })
                    .collect();
                let answers: Vec<String> =
                    calls::call_many(&entries).into_iter().map(answer).collect();
                respond(out, 200, &[], &answers.join(" | "));
            }
            "/headers" => {
                let mut text = method_name(&request.method());
                text.push('\n');
                for (k, v) in request.headers().entries() {
                    text.push_str(&format!("{k}: {}\n", String::from_utf8_lossy(&v)));
                }
                respond(out, 200, &[], &text);
            }
            "/respond" => {
                let status = param(query, "status")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(200);
                let headers: &[(&str, &str)] = if param(query, "spoof").is_some() {
                    SPOOFED
                } else {
                    &[]
                };
                respond(out, status, headers, "responded");
            }
            "/after" => {
                let to = param(query, "to").unwrap_or_default();
                let status = param(query, "status")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(200);
                let len = param(query, "len").is_some();
                let fields = Fields::new();
                if len {
                    fields.append("content-length", b"4").unwrap();
                }
                let response = OutgoingResponse::new(fields);
                let _ = response.set_status_code(status);
                let outgoing = response.body().unwrap();
                ResponseOutparam::set(out, Ok(response));
                let mut stream = outgoing.write().unwrap();
                if len {
                    let _ = stream.write_all(b"body");
                    let _ = stream.flush();
                }
                drop(stream);
                let call = || {
                    sleep_ms(300);
                    let request = actors::Request {
                        method: "POST".into(),
                        path: "/durable".into(),
                        headers: Vec::new(),
                        body: Vec::new(),
                    };
                    let _ = actors::call(&to, &request);
                };
                if len {
                    call();
                    let _ = OutgoingBody::finish(outgoing, None);
                } else {
                    let _ = OutgoingBody::finish(outgoing, None);
                    call();
                }
            }
            "/self" => {
                let me = actors::self_address();
                respond(
                    out,
                    200,
                    &[],
                    &format!("{}/{}/{}", me.tenant, me.class, me.name),
                );
            }
            "/env" => {
                let name = param(query, "name").unwrap_or_default();
                let value = env::get(&name).unwrap_or_else(|| "none".into());
                respond(out, 200, &[], &value);
            }
            "/sleep" => {
                let ms = param(query, "ms").and_then(|v| v.parse().ok()).unwrap_or(0);
                sleep_ms(ms);
                respond(out, 200, &[], "slept");
            }
            "/spin" => {
                let mut n: u64 = 0;
                loop {
                    n = std::hint::black_box(n.wrapping_add(1));
                }
            }
            "/print" => {
                println!("front says hi");
                respond(out, 200, &[], "printed");
            }
            "/trailers-in" => {
                let body = request.consume().unwrap();
                {
                    let stream = body.stream().unwrap();
                    while stream.blocking_read(65536).is_ok() {}
                }
                let trailers = IncomingBody::finish(body);
                trailers.subscribe().block();
                let mut text = String::from("trailers:\n");
                if let Some(Ok(Ok(Some(t)))) = trailers.get() {
                    for (k, v) in t.entries() {
                        text.push_str(&format!("{k}: {}\n", String::from_utf8_lossy(&v)));
                    }
                }
                respond(out, 200, &[], &text);
            }
            "/trailers-out" => {
                let (fields, refused) = fields_of(&[("trailer", "x-wasmkeep-node, x-other")]);
                let (trailers, refused_trailers) =
                    fields_of(&[("x-wasmkeep-node", "forged"), ("x-other", "kept")]);
                let response = OutgoingResponse::new(fields);
                let outgoing = response.body().unwrap();
                ResponseOutparam::set(out, Ok(response));
                let mut stream = outgoing.write().unwrap();
                let _ = stream.write_all(format!("body{refused}{refused_trailers}").as_bytes());
                let _ = stream.flush();
                drop(stream);
                let _ = OutgoingBody::finish(outgoing, Some(trailers));
            }
            "/stream-spin" => {
                let response = OutgoingResponse::new(Fields::new());
                let outgoing = response.body().unwrap();
                ResponseOutparam::set(out, Ok(response));
                let mut stream = outgoing.write().unwrap();
                let _ = stream.write_all(b"started\n");
                let _ = stream.flush();
                let mut n: u64 = 0;
                loop {
                    n = std::hint::black_box(n.wrapping_add(1));
                }
            }
            "/handoff" => {
                if let Some(ms) = param(query, "delay").and_then(|v| v.parse().ok()) {
                    sleep_ms(ms);
                }
                let class = param(query, "class").unwrap_or_default();
                let name = param(query, "name").unwrap_or_default();
                let name = if param(query, "raw").is_some() {
                    name
                } else {
                    encode(&name)
                };
                let target = param(query, "path").unwrap_or_else(|| "/".into());
                let mut headers = vec![
                    ("x-wasmkeep-handoff-class", class.as_str()),
                    ("x-wasmkeep-handoff-name", name.as_str()),
                ];
                if param(query, "partial").is_none() {
                    headers.push(("x-wasmkeep-handoff-path", target.as_str()));
                }
                if param(query, "dup").is_some() {
                    headers.push(("x-wasmkeep-handoff-class", "other"));
                }
                respond(out, 426, &headers, "websocket upgrade required\n");
            }
            "/proxy" => {
                let url = param(query, "url").unwrap_or_default();
                match proxy(&url) {
                    Ok((status, headers, body)) => {
                        let range: Vec<&str> = headers
                            .iter()
                            .map(|(k, _)| k.as_str())
                            .filter(|k| k.to_ascii_lowercase().starts_with("x-wasmkeep-"))
                            .collect();
                        let fields = Fields::new();
                        for (k, v) in &headers {
                            let _ = fields.append(k, v);
                        }
                        fields
                            .append("x-upstream-range", range.join(",").as_bytes())
                            .unwrap();
                        let response = OutgoingResponse::new(fields);
                        let _ = response.set_status_code(status);
                        let outgoing = response.body().unwrap();
                        ResponseOutparam::set(out, Ok(response));
                        let mut stream = outgoing.write().unwrap();
                        let _ = stream.write_all(&body);
                        let _ = stream.flush();
                        drop(stream);
                        let _ = OutgoingBody::finish(outgoing, None);
                    }
                    Err(e) => respond(out, 502, &[], &format!("proxy: {e}")),
                }
            }
            _ => respond(out, 200, &[], "front ok"),
        }
    }
}
