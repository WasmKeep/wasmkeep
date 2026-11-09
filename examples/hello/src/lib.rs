//! A `wasi:http/incoming-handler` component: answers every request with a
//! greeting and the method and path it received.
//!
//! Build: `cargo build --release --target wasm32-wasip2`
//! Serve: `wasmkeep serve target/wasm32-wasip2/release/hello.wasm`

use std::io::Write as _;

use wasip2::http::types::{
    Fields, IncomingRequest, Method, OutgoingBody, OutgoingResponse, ResponseOutparam,
};

wasip2::http::proxy::export!(Hello);

struct Hello;

impl wasip2::exports::http::incoming_handler::Guest for Hello {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let method = method_name(&request.method());
        let path = request.path_with_query().unwrap_or_else(|| "/".to_owned());
        let body_text = format!("Hello from WasmKeep! You sent {method} {path}\n");

        let headers = Fields::new();
        // Header values are bytes; these literals are always valid.
        let _ = headers.set("content-type", &[b"text/plain; charset=utf-8".to_vec()]);
        let response = OutgoingResponse::new(headers);
        let Ok(body) = response.body() else { return };
        ResponseOutparam::set(response_out, Ok(response));

        if let Ok(mut out) = body.write() {
            let _ = out.write_all(body_text.as_bytes());
            let _ = out.flush();
        }
        let _ = OutgoingBody::finish(body, None);
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
