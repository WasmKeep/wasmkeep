//! The app's front: every request outside `/actor/` comes here. It holds
//! no state; it calls class `counter`, which it reaches as an inside
//! caller, so the class stays `internal` (no client reaches it directly).
//!
//! - `GET /`: a page with the count of actor `counter/main`, and a button
//! - `POST /`: adds one to `counter/main`, then back to the page
//! - `GET /api/counter/<name>`, `POST /api/counter/<name>`: any counter,
//!   by name (1 to 32 of `a-z`, `0-9`, `-`)
//!
//! Anyone may count: there is no sign-in. examples/todo-front in the
//! WasmKeep repository has one, with a token per user from a secret.

use wasmkeep_sdk::front::{self, actors};
use wasmkeep_sdk::http::{self, IncomingRequest, Method, ResponseOutparam};

struct Front;

impl http::Guest for Front {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let method = request.method();
        let path = request.path_with_query().unwrap_or_else(|| "/".into());
        let path = path.split('?').next().unwrap_or("/").to_owned();
        match (&method, path.as_str()) {
            (Method::Get, "/") => match count("main", "GET") {
                Ok(value) => page(out, &value),
                Err(response) => http::respond_with(out, &response),
            },
            (Method::Post, "/") => match count("main", "POST") {
                Ok(_) => http::respond_with(
                    out,
                    &actors::Response {
                        status: 303,
                        headers: vec![("location".into(), "/".into())],
                        body: Vec::new(),
                    },
                ),
                Err(response) => http::respond_with(out, &response),
            },
            (Method::Get | Method::Post, p) if p.starts_with("/api/counter/") => {
                let name = &p["/api/counter/".len()..];
                if !valid_name(name) {
                    // Checked before any call: a name taken from a request
                    // would create one actor, and one database, per guess.
                    return http::respond(out, 404, "text/plain", b"not found\n");
                }
                let verb = match method {
                    Method::Post => "POST",
                    _ => "GET",
                };
                match count(name, verb) {
                    Ok(value) => {
                        http::respond(out, 200, "text/plain", format!("{value}\n").as_bytes())
                    }
                    Err(response) => http::respond_with(out, &response),
                }
            }
            _ => http::respond(out, 404, "text/plain", b"not found\n"),
        }
    }
}

/// Calls actor `counter/<name>`: its value, or the answer to give instead.
fn count(name: &str, method: &str) -> Result<String, actors::Response> {
    let request = actors::Request {
        method: method.into(),
        path: "/".into(),
        headers: Vec::new(),
        body: Vec::new(),
    };
    match actors::call(&format!("counter/{name}"), &request) {
        Ok(r) if r.status == 200 => Ok(String::from_utf8_lossy(&r.body).trim().to_owned()),
        Ok(r) => {
            eprintln!("counter/{name}: status {}", r.status);
            Err(actors::Response {
                status: 502,
                headers: vec![("content-type".into(), "text/plain".into())],
                body: b"the counter did not answer\n".to_vec(),
            })
        }
        Err(e) => {
            eprintln!("counter/{name}: {e:?}");
            Err(front::unavailable(&e))
        }
    }
}

fn page(out: ResponseOutparam, value: &str) {
    // `value` is the actor's answer, digits only, but escape it anyway.
    let value: String = value.chars().filter(char::is_ascii_digit).collect();
    let body = format!(
        "<!doctype html>\n<meta charset=\"utf-8\">\n<title>{{app}}</title>\n\
         <h1>{{app}}</h1>\n<p>Count: <strong>{value}</strong></p>\n\
         <form method=\"post\" action=\"/\"><button>Add one</button></form>\n"
    );
    http::respond_with(
        out,
        &actors::Response {
            status: 200,
            headers: vec![
                ("content-type".into(), "text/html; charset=utf-8".into()),
                ("x-content-type-options".into(), "nosniff".into()),
                (
                    "content-security-policy".into(),
                    "default-src 'none'; form-action 'self'; frame-ancestors 'none'".into(),
                ),
            ],
            body: body.into_bytes(),
        },
    );
}

/// An actor name the front accepts: 1 to 32 of `a-z`, `0-9`, `-`.
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 32
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

wasmkeep_sdk::export_front!(Front);
