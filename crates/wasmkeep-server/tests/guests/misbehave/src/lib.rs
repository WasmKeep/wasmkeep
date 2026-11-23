//! Test guest. `/spin` never returns, `/panic` traps, `/fields?n=N&bytes=B`
//! holds N `fields` of one B-byte header each, then drops them and answers
//! `held N` (or `err:E` for the first one refused; past the store's table,
//! the call traps), anything else answers `ok`. Used to prove that a bad
//! guest cannot take the server down.

use std::io::Write as _;

use wasip2::http::types::{
    Fields, IncomingRequest, OutgoingBody, OutgoingResponse, ResponseOutparam,
};

wasip2::http::proxy::export!(Misbehave);

struct Misbehave;

impl wasip2::exports::http::incoming_handler::Guest for Misbehave {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let path = request.path_with_query().unwrap_or_default();
        match path.as_str() {
            "/spin" => {
                let mut n: u64 = 0;
                loop {
                    n = std::hint::black_box(n.wrapping_add(1));
                }
            }
            "/panic" => panic!("misbehave: requested panic"),
            _ => {}
        }
        let text = match path.strip_prefix("/fields?") {
            Some(query) => hold_fields(param(query, "n"), param(query, "bytes")),
            None => "ok".to_owned(),
        };
        let response = OutgoingResponse::new(Fields::new());
        let body = response.body().unwrap();
        ResponseOutparam::set(response_out, Ok(response));
        let mut out = body.write().unwrap();
        out.write_all(format!("{text}\n").as_bytes()).unwrap();
        out.flush().unwrap();
        drop(out);
        OutgoingBody::finish(body, None).unwrap();
    }
}

/// The number `key=` holds in `query`, or 0.
fn param(query: &str, key: &str) -> usize {
    query
        .split('&')
        .find_map(|pair| pair.strip_prefix(key)?.strip_prefix('='))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// `/fields`: N `fields` held at once, then dropped.
fn hold_fields(n: usize, bytes: usize) -> String {
    let value = vec![b'v'; bytes];
    let mut held = Vec::with_capacity(n);
    for _ in 0..n {
        match Fields::from_list(&[("x-fill".to_owned(), value.clone())]) {
            Ok(fields) => held.push(fields),
            Err(e) => return format!("err:{e:?}"),
        }
    }
    format!("held {}", held.len())
}
