//! Test front: a component of the `wasmkeep:actor/front` world. Routes (any
//! method; the query's values percent-encoded):
//! - `/call?to=CLASS/NAME&path=P[&method=M][&delay=MS][&spoof]`: waits MS
//!   (no CPU), then `actors.call` with M (GET) and P; answers `STATUS:BODY`
//!   (the body's trailing whitespace trimmed),
//!   or `err:KIND[:DETAIL]`. With `spoof`, the request carries
//!   `x-wasmkeep-` headers the host must drop.
//! - `/headers`: the method, then every request header as `name: value`
//! - `/respond?status=N[&spoof]`: answers status N (with `spoof`, with
//!   `x-wasmkeep-` headers the host must drop)
//! - `/self`: `self-address` as `tenant/class/name`
//! - `/env?name=N`: `env.get(N)`, or `none`
//! - `/sleep?ms=MS`: waits MS (no CPU), answers `slept`
//! - `/spin`: never returns (CPU budget)
//! - `/print`: prints a line to stdout, answers `printed`
//! - `/trailers-in`: reads the body to its end, answers the request's
//!   trailers as `name: value` lines
//! - `/trailers-out`: answers `body` with trailers in and out of the
//!   `x-wasmkeep-` range (the guest's attempts to set them are ignored
//!   when refused)
//! - `/stream-spin`: answers 200, sends `started`, then never returns
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
    });
}

use bindings::wasmkeep::actor::{actors, env};

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

fn param(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
        (k == key).then(|| decode(v))
    })
}

fn sleep_ms(ms: u64) {
    wasip2::clocks::monotonic_clock::subscribe_duration(ms * 1_000_000).block();
}

fn respond(out: ResponseOutparam, status: u16, headers: &[(&str, &str)], body: &str) {
    let fields = Fields::new();
    for (k, v) in headers {
        let _ = fields.append(k, v.as_bytes());
    }
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

impl wasip2::exports::http::incoming_handler::Guest for Front {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let full = request.path_with_query().unwrap_or_default();
        let (path, query) = full.split_once('?').unwrap_or((full.as_str(), ""));
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
                let request = actors::Request {
                    method: param(query, "method").unwrap_or_else(|| "GET".into()),
                    path: param(query, "path").unwrap_or_else(|| "/".into()),
                    headers,
                    body: Vec::new(),
                };
                let answer = match actors::call(&to, &request) {
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
                };
                respond(out, 200, &[], &answer);
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
                let fields = Fields::new();
                let _ = fields.append("trailer", b"x-wasmkeep-node, x-other");
                let response = OutgoingResponse::new(fields);
                let outgoing = response.body().unwrap();
                ResponseOutparam::set(out, Ok(response));
                let mut stream = outgoing.write().unwrap();
                let _ = stream.write_all(b"body");
                let _ = stream.flush();
                drop(stream);
                let trailers = Fields::new();
                let _ = trailers.append("x-wasmkeep-node", b"forged");
                let _ = trailers.append("x-other", b"kept");
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
            _ => respond(out, 200, &[], "front ok"),
        }
    }
}
