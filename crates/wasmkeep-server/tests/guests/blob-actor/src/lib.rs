//! Test actor for files (ADR 0030): the routes of `../blob-routes.rs` over
//! the actor's own namespace, through the SDK's raw `blobs` bindings (the
//! SDK's own API comes later), plus `GET /self`: `class/name`, and
//! `POST /alarm`: an alarm now, whose `on-alarm` writes `from-alarm`. An
//! actor whose name starts with `init-` writes `from-init` from `on-init`.

use wasmkeep_sdk::front::bindings::wasmkeep::actor::blobs;
use wasmkeep_sdk::http::{self, IncomingRequest, ResponseOutparam};
use wasmkeep_sdk::{Actor, actors, alarms, now_ms};

include!("../../blob-routes.rs");

struct BlobActor;

impl Actor for BlobActor {
    fn on_init() -> Result<(), String> {
        if actors::self_address().name.starts_with("init-") {
            write_from("from-init")?;
        }
        Ok(())
    }

    fn on_alarm() -> Result<(), String> {
        write_from("from-alarm")
    }
}

/// Writes `key` with its own name as the body: a write outside a request.
fn write_from(key: &str) -> Result<(), String> {
    let options = blobs::WriteOptions {
        content_type: None,
        condition: blobs::Condition::Any,
    };
    blobs::put(key, key.as_bytes(), &options)
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

impl http::Guest for BlobActor {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let full = request.path_with_query().unwrap_or_default();
        if full == "/alarm" {
            alarms::set_alarm(now_ms());
            http::respond(out, 200, "text/plain", b"set");
            return;
        }
        if full == "/self" {
            let me = actors::self_address();
            let body = format!("{}/{}", me.class, me.name);
            http::respond(out, 200, "text/plain", body.as_bytes());
            return;
        }
        let method = http::method_name(&request.method());
        let headers = http::request_headers(&request);
        let if_match = headers.get("if-match").map(str::to_owned);
        let if_none_match = headers.get("if-none-match") == Some("*");
        let body = http::read_body(&request, 32 << 20).unwrap_or_default();
        match blob_route(&method, &full, if_match, if_none_match, body) {
            Some((status, body)) => http::respond(out, status, "text/plain", &body),
            None => http::respond(out, 404, "text/plain", b"no route\n"),
        }
    }
}

wasmkeep_sdk::export!(BlobActor);
