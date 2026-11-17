//! One class, two roles: `client/callee` answers a contract, `client/caller`
//! (GET /run) calls it through the typed client and checks every outcome.
use wasmkeep_sdk::client::{Failure, Nullable, actor_client};
use wasmkeep_sdk::problem::{self, Problem};
use wasmkeep_sdk::{Actor, http, record, url_path};

record! {
    #[derive(Debug, Clone, PartialEq)]
    struct AddItem { sku: String, qty: i64 }
}
record! {
    #[derive(Debug, Clone, PartialEq)]
    struct Count { count: i64, note: Option<Nullable<String>> }
}
record! {
    #[derive(Debug, Clone, PartialEq)]
    struct OutOfStock { left: i64 }
}

wasmkeep_sdk::contract! {
    mod cart {
        fn add_item(AddItem) -> Count
            = POST |i: &AddItem| url_path!("/items/{}", i.sku),
            errors AddItemError {
                OutOfStock("cart.out_of_stock"): OutOfStock,
                Closed("cart.closed"),
            };
        fn get() -> Count = GET "/";
        fn bad() -> Count = GET "/bad";
        fn legacy() -> () = GET "/legacy";
    }
}

struct Guest;
impl Actor for Guest {}

fn check() -> Result<(), String> {
    let c = cart::Client::new("client/callee");
    let r = c
        .add_item(&AddItem { sku: "a/b c".into(), qty: 3 })
        .map_err(|e| format!("add: {e}"))?;
    assert_eq!(r.value, Count { count: 3, note: None });
    match c.add_item(&AddItem { sku: "x".into(), qty: 0 }) {
        Err(Failure::Declared { status: 409, error: cart::AddItemError::OutOfStock(o), .. }) if o.left == 0 => {}
        other => return Err(format!("out of stock: {other:?}")),
    }
    match c.add_item(&AddItem { sku: "closed".into(), qty: 1 }) {
        Err(Failure::Declared { error: cart::AddItemError::Closed(()), .. }) => {}
        other => return Err(format!("closed: {other:?}")),
    }
    match c.get() {
        Ok(r) if r.value.note == Some(Nullable(None)) && r.value.count == 7 => {}
        other => return Err(format!("get: {other:?}")),
    }
    if !matches!(c.bad(), Err(Failure::InvalidResponse { .. })) {
        return Err("bad".into());
    }
    if !matches!(c.legacy(), Err(Failure::Http { status: 500, .. })) {
        return Err("legacy".into());
    }
    // A call to an address that names no class is a transport failure.
    let none = cart::Client::new("nope/x");
    if !matches!(none.get(), Err(Failure::Transport(_))) {
        return Err("transport".into());
    }
    // Calling itself is reentrant.
    let me = cart::Client::new("client/caller");
    match me.get() {
        Err(Failure::Transport(e)) if e.kind == "reentrant" => {}
        other => return Err(format!("reentrant: {other:?}")),
    }
    // A batch: each entry on its own, in order.
    let client = actor_client();
    let mut batch = client.batch();
    let a = batch.add("client/callee", &cart::get(), &());
    let b = batch.add("client/callee", &cart::bad(), &());
    let z = batch.add("client/callee", &cart::get(), &());
    let mut r = batch.send();
    if r.take(a).is_err() || r.take(z).is_err() || !matches!(r.take(b), Err(Failure::InvalidResponse { .. })) {
        return Err("batch".into());
    }
    Ok(())
}

impl http::Guest for Guest {
    fn handle(req: http::IncomingRequest, out: http::ResponseOutparam) {
        let path = req.path_with_query().unwrap_or_default();
        let method = http::method_name(&req.method());
        match (method.as_str(), path.as_str()) {
            ("GET", "/run") => match check() {
                Ok(()) => http::respond(out, 200, "text/plain", b"ok"),
                Err(e) => http::respond(out, 500, "text/plain", e.as_bytes()),
            },
            ("GET", "/") => http::respond(out, 200, "application/json", br#"{"count":7,"note":null,"extra":1}"#),
            ("GET", "/bad") => http::respond(out, 200, "application/json", br#"{"count":"x"}"#),
            ("GET", "/legacy") => http::respond(out, 500, "text/plain", b"internal error\n"),
            ("POST", p) if p.starts_with("/items/") => {
                let body = http::read_body(&req, 1024).unwrap_or_default();
                let text = String::from_utf8(body).unwrap_or_default();
                let (status, content_type, body) = if p == "/items/closed" {
                    problem_body(409, "cart.closed", None)
                } else if text.contains(r#""qty":0"#) {
                    problem_body(409, "cart.out_of_stock", Some(r#"{"left":0}"#))
                } else if p != "/items/a%2Fb%20c" || text != r#"{"sku":"a/b c","qty":3}"# {
                    (400, "text/plain".to_owned(), format!("unexpected {p} {text}").into_bytes())
                } else {
                    (200, "application/json".to_owned(), br#"{"count":3}"#.to_vec())
                };
                http::respond(out, status, &content_type, &body);
            }
            _ => http::respond(out, 404, "text/plain", b"not found"),
        }
    }
}

fn problem_body(status: u16, code: &str, details: Option<&str>) -> (u16, String, Vec<u8>) {
    let mut p = Problem::new(code, "m").unwrap_or_else(|_| unreachable!());
    if let Some(d) = details {
        p = p.with_details(d).unwrap_or_else(|_| unreachable!());
    }
    match problem::response(status, &p) {
        Ok(r) => (r.status, problem::CONTENT_TYPE.to_owned(), r.body),
        Err(_) => (500, "text/plain".into(), Vec::new()),
    }
}

wasmkeep_sdk::export!(Guest);
