//! Class `todos`: one todo list per user, in the actor's own SQLite
//! database. The actor's name is the user, chosen and checked by the front.
//!
//! It answers its app's front only (`caller()`): deployed `internal`, a
//! client cannot reach it at `/actor/todos/...` anyway, but the check keeps
//! that true if the class is ever deployed `open`, or the front is replaced.
//!
//! - `GET /`: the list, as JSON `[{"id":1,"text":"..."}]`
//! - `POST /`: adds the body (text, 1 to 1,000 bytes); answers the new id
//! - `DELETE /<id>`: removes an item

use wasmkeep_sdk::http::{self, IncomingRequest, Method, ResponseOutparam};
use wasmkeep_sdk::{Actor, Caller, Value, caller, params, storage};

/// The longest item accepted, in bytes.
const MAX_TEXT: usize = 1_000;

struct Todos;

impl Actor for Todos {
    fn on_init() -> Result<(), String> {
        storage::exec(
            "CREATE TABLE IF NOT EXISTS todos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                text TEXT NOT NULL
            )",
            &[],
        )
        .map(drop)
        .map_err(|e| e.to_string())
    }
}

impl http::Guest for Todos {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        if caller(&request) != Caller::Front {
            http::respond(out, 403, "text/plain", b"forbidden\n");
            return;
        }
        let path = request.path_with_query().unwrap_or_else(|| "/".into());
        match (request.method(), path.as_str()) {
            (Method::Get, "/") => list(out),
            (Method::Post, "/") => match http::read_body(&request, MAX_TEXT) {
                Some(body) if !body.is_empty() => match String::from_utf8(body) {
                    Ok(text) => add(out, &text),
                    Err(_) => http::respond(out, 400, "text/plain", b"text must be UTF-8\n"),
                },
                _ => http::respond(out, 400, "text/plain", b"1 to 1000 bytes of text\n"),
            },
            (Method::Delete, p) => {
                match p.strip_prefix('/').and_then(|id| id.parse::<i64>().ok()) {
                    Some(id) => remove(out, id),
                    None => http::respond(out, 404, "text/plain", b"not found\n"),
                }
            }
            _ => http::respond(out, 404, "text/plain", b"not found\n"),
        }
    }
}

fn list(out: ResponseOutparam) {
    match storage::query("SELECT id, text FROM todos ORDER BY id", &[]) {
        Ok(rows) => {
            let items: Vec<String> = rows
                .values
                .iter()
                .map(|row| match (&row[0], &row[1]) {
                    (Value::Integer(id), Value::Text(text)) => {
                        format!("{{\"id\":{id},\"text\":{}}}", json_string(text))
                    }
                    _ => String::new(),
                })
                .filter(|s| !s.is_empty())
                .collect();
            let body = format!("[{}]\n", items.join(","));
            http::respond(out, 200, "application/json", body.as_bytes());
        }
        Err(e) => failed(out, &e),
    }
}

fn add(out: ResponseOutparam, text: &str) {
    let added = storage::exec("INSERT INTO todos (text) VALUES (?1)", &params![text])
        .and_then(|_| storage::query("SELECT last_insert_rowid() AS id", &[]));
    match added {
        Ok(rows) => {
            let id = rows.get(0, "id").and_then(|v| v.as_i64()).unwrap_or(0);
            http::respond(
                out,
                201,
                "application/json",
                format!("{{\"id\":{id}}}\n").as_bytes(),
            );
        }
        Err(e) => failed(out, &e),
    }
}

fn remove(out: ResponseOutparam, id: i64) {
    match storage::exec("DELETE FROM todos WHERE id = ?1", &params![id]) {
        Ok(0) => http::respond(out, 404, "text/plain", b"not found\n"),
        Ok(_) => http::respond(out, 204, "text/plain", b""),
        Err(e) => failed(out, &e),
    }
}

/// A storage error: the detail to the log, a generic answer to the caller
/// (which the front passes on to the client).
fn failed(out: ResponseOutparam, e: &wasmkeep_sdk::SqlError) {
    eprintln!("storage: {e}");
    http::respond(out, 500, "text/plain", b"internal error\n");
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

wasmkeep_sdk::export!(Todos);
