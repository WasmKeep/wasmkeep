//! The front of the todo-front example (ADR 0023): every request outside
//! `/actor/` comes here. It serves a page, and a JSON API over class
//! `todos` (one actor per user), which it reaches as an inside caller, so
//! the class can be deployed `internal`.
//!
//! - `GET /`, `GET /app.js`: the page
//! - `GET /api`: where the API is, built from `scheme()` and `host()`
//! - `GET /api/todos`, `POST /api/todos`, `DELETE /api/todos/<id>`: the
//!   signed-in user's list, with `Authorization: Bearer <token>`
//!
//! Sign-in is a token per user, from the front's secret `TOKENS`:
//! `alice:<token>,bob:<token>`, each token at least 32 characters (make one
//! with `openssl rand -hex 32`). With `TOKENS` unset, empty or malformed,
//! nobody is signed in: the API answers 401, never a default user. Tokens
//! are compared in constant time, and the user name, which becomes the
//! actor's name, is checked before any call: a name taken from a request
//! would create one database per guess.

use wasmkeep_sdk::front::{self, actors, env};
use wasmkeep_sdk::http::{self, IncomingRequest, Method, ResponseOutparam};

/// The class the front calls; deploy it `--access todos=internal`.
const CLASS: &str = "todos";
/// The shortest token accepted.
const MIN_TOKEN: usize = 32;
/// The largest body passed on to the actor.
const MAX_BODY: usize = 1_000;

const PAGE: &str = include_str!("page.html");
const SCRIPT: &str = include_str!("app.js");

struct Front;

impl http::Guest for Front {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let method = request.method();
        let path = request.path_with_query().unwrap_or_else(|| "/".into());
        let path = path.split('?').next().unwrap_or("/").to_owned();
        match (&method, path.as_str()) {
            (Method::Get, "/") => page(out, "text/html; charset=utf-8", PAGE),
            (Method::Get, "/app.js") => page(out, "text/javascript; charset=utf-8", SCRIPT),
            (Method::Get, "/api") => {
                // The client chooses Host: escape it like any input.
                let base = format!(
                    "{}://{}/api/todos",
                    front::scheme(&request),
                    front::host(&request).unwrap_or_else(|| "localhost".into())
                );
                let body = format!("{{\"todos\":{}}}\n", json_string(&base));
                http::respond(out, 200, "application/json", body.as_bytes());
            }
            (_, p) if p == "/api/todos" || p.starts_with("/api/todos/") => {
                let Some(user) = signed_in(&request) else {
                    let response = actors::Response {
                        status: 401,
                        headers: vec![
                            ("content-type".into(), "text/plain".into()),
                            ("www-authenticate".into(), "Bearer".into()),
                        ],
                        body: b"sign in with Authorization: Bearer <token>\n".to_vec(),
                    };
                    http::respond_with(out, &response);
                    return;
                };
                api(&request, out, &method, &p["/api/todos".len()..], &user);
            }
            _ => http::respond(out, 404, "text/plain", b"not found\n"),
        }
    }
}

/// The user's calls to their own `todos` actor.
fn api(request: &IncomingRequest, out: ResponseOutparam, method: &Method, rest: &str, user: &str) {
    let call = match (method, rest) {
        (Method::Get, "") => call_request("GET", "/", Vec::new()),
        (Method::Post, "") => match http::read_body(request, MAX_BODY) {
            Some(body) => call_request("POST", "/", body),
            None => return http::respond(out, 413, "text/plain", b"at most 1000 bytes\n"),
        },
        (Method::Delete, id)
            if id.len() > 1 && id.len() <= 20 && id[1..].bytes().all(|b| b.is_ascii_digit()) =>
        {
            call_request("DELETE", id, Vec::new())
        }
        _ => return http::respond(out, 404, "text/plain", b"not found\n"),
    };
    match actors::call(&format!("{CLASS}/{user}"), &call) {
        // The actor's status and body; of its headers, the content type.
        Ok(mut response) => {
            response
                .headers
                .retain(|(name, _)| name.eq_ignore_ascii_case("content-type"));
            http::respond_with(out, &response);
        }
        Err(e) => {
            eprintln!("call to {CLASS}/{user}: {e:?}");
            http::respond_with(out, &front::unavailable(&e));
        }
    }
}

fn call_request(method: &str, path: &str, body: Vec<u8>) -> actors::Request {
    actors::Request {
        method: method.into(),
        path: path.into(),
        headers: vec![("content-type".into(), "text/plain; charset=utf-8".into())],
        body,
    }
}

fn page(out: ResponseOutparam, content_type: &str, body: &str) {
    let response = actors::Response {
        status: 200,
        headers: vec![
            ("content-type".into(), content_type.into()),
            ("x-content-type-options".into(), "nosniff".into()),
            (
                "content-security-policy".into(),
                "default-src 'self'; frame-ancestors 'none'".into(),
            ),
        ],
        body: body.as_bytes().to_vec(),
    };
    http::respond_with(out, &response);
}

/// The user whose token the request carries, if any.
fn signed_in(request: &IncomingRequest) -> Option<String> {
    let values = request.headers().get("authorization");
    let value = String::from_utf8(values.into_iter().next()?).ok()?;
    let token = value.strip_prefix("Bearer ")?.trim();
    user_for(&env::get("TOKENS")?, token)
}

/// Looks `token` up in `tokens` (`user:token,user:token`). Every entry is
/// compared, in constant time, whatever matches; entries with an invalid
/// user name or a short token never match.
fn user_for(tokens: &str, token: &str) -> Option<String> {
    if token.len() < MIN_TOKEN {
        return None;
    }
    let mut found = None;
    for entry in tokens.split([',', '\n']) {
        let Some((user, expected)) = entry.trim().split_once(':') else {
            continue;
        };
        let expected = expected.trim();
        if valid_user(user) && expected.len() >= MIN_TOKEN && same(expected, token) {
            found = Some(user.to_owned());
        }
    }
    found
}

/// A user name, used as the actor's name: 1 to 32 of `a-z`, `0-9`, `-`.
fn valid_user(user: &str) -> bool {
    !user.is_empty()
        && user.len() <= 32
        && user
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// `s` as a JSON string literal.
fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if u32::from(c) < 0x20 || c == '<' || c == '>' || c == '&' => {
                out.push_str(&format!("\\u{:04x}", u32::from(c)));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Equal, in time that depends on the lengths only.
fn same(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes()
        .zip(b.bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

wasmkeep_sdk::export_front!(Front);
