//! The app class of the workflows example (`app/main`): an HTTP API over
//! the `orders` workflow class, through `wasmkeep_sdk::workflow::Client`.
//!
//! - `POST /orders/<id>`: starts order `<id>` with the body as its input
//!   (see the `orders` class); answers its status. Starting an id again
//!   answers the existing order's status.
//! - `GET /orders/<id>`: its status, one `key: value` per line; `404` if
//!   there is none.
//! - `POST /orders/<id>/approve`: delivers the `approved` event.
//! - `POST /orders/<id>/cancel`: cancels it.

use wasmkeep_sdk::Actor;
use wasmkeep_sdk::http::{self, IncomingRequest, Method, ResponseOutparam};
use wasmkeep_sdk::workflow::{Client, Error, State, Status};

struct App;

impl Actor for App {}

fn show(status: &Status) -> String {
    let state = match status.state {
        State::Running => "running",
        State::Waiting => "waiting",
        State::Completed => "completed",
        State::Failed => "failed",
        State::Cancelled => "cancelled",
    };
    let mut out = format!("state: {state}\npositions: {}\n", status.positions);
    if let Some(at) = status.wake_at {
        out.push_str(&format!("wake_at: {at}\n"));
    }
    if !status.waiting_for.is_empty() {
        out.push_str(&format!("waiting_for: {}\n", status.waiting_for.join(",")));
    }
    if let Some(output) = &status.output {
        out.push_str(&format!("output: {}\n", String::from_utf8_lossy(output)));
    }
    if let Some(error) = &status.error {
        out.push_str(&format!("error: {error}\n"));
    }
    out
}

fn reply(result: Result<Status, Error>) -> (u16, String) {
    match result {
        Ok(status) => (200, show(&status)),
        Err(Error::Unknown) => (404, "no such order\n".into()),
        Err(Error::Finished) => (409, "the order has finished\n".into()),
        Err(e) => (502, format!("{e}\n")),
    }
}

impl http::Guest for App {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let orders = Client::new("orders");
        let path = request.path_with_query().unwrap_or_default();
        let parts: Vec<&str> = path
            .split('?')
            .next()
            .unwrap_or_default()
            .trim_matches('/')
            .split('/')
            .collect();
        let (status, body) = match (request.method(), parts.as_slice()) {
            (Method::Post, ["orders", id]) => {
                let input = http::read_body(&request, 1 << 20).unwrap_or_default();
                reply(orders.start(id, input))
            }
            (Method::Get, ["orders", id]) => reply(orders.status(id)),
            (Method::Post, ["orders", id, "approve"]) => reply(
                orders
                    .send_event(id, "approved", b"")
                    .and_then(|()| orders.status(id)),
            ),
            (Method::Post, ["orders", id, "cancel"]) => reply(orders.cancel(id)),
            _ => (404, "not found\n".into()),
        };
        http::respond(out, status, "text/plain", body.as_bytes());
    }
}

wasmkeep_sdk::export!(App);
