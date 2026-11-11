//! A ledger actor, for `bench/compare --cluster` only. Each actor name is
//! its own ledger:
//!
//! - `POST ?t=<integer>` records the token and answers `ok`
//! - `GET` returns every recorded token, one per line, in insertion order
//!
//! A counter cannot show every lost write: its increments are
//! interchangeable, so a write that applied without being acknowledged hides
//! one that was acknowledged and lost. Tokens are told apart. A token
//! recorded twice stays twice, so a write applied twice shows too.

use wasmkeep_sdk::http::{self, IncomingRequest, Method, ResponseOutparam};
use wasmkeep_sdk::{Actor, params, storage};

struct Ledger;

impl Actor for Ledger {
    fn on_init() -> Result<(), String> {
        storage::exec(
            "CREATE TABLE IF NOT EXISTS ledger (token INTEGER NOT NULL)",
            &[],
        )
        .map_err(|e| e.to_string())?;
        Ok(())
    }
}

impl http::Guest for Ledger {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        match request.method() {
            Method::Get => match storage::query("SELECT token FROM ledger ORDER BY rowid", &[]) {
                Ok(rows) => {
                    let mut body = String::new();
                    for row in 0..rows.len() {
                        if let Some(token) = rows.get(row, "token").and_then(|v| v.as_i64()) {
                            body.push_str(&token.to_string());
                            body.push('\n');
                        }
                    }
                    http::respond(out, 200, "text/plain", body.as_bytes());
                }
                Err(e) => http::respond(out, 500, "text/plain", format!("{e}\n").as_bytes()),
            },
            Method::Post => {
                let path = request.path_with_query().unwrap_or_default();
                let Some(token) = token(&path) else {
                    http::respond(out, 400, "text/plain", b"POST ?t=<integer>\n");
                    return;
                };
                match record(token) {
                    Ok(()) => http::respond(out, 200, "text/plain", b"ok\n"),
                    Err(e) => http::respond(out, 500, "text/plain", format!("{e}\n").as_bytes()),
                }
            }
            _ => http::respond(out, 405, "text/plain", b"use GET or POST\n"),
        }
    }
}

/// The `t` query parameter, if it is an integer.
fn token(path_with_query: &str) -> Option<i64> {
    let (_, query) = path_with_query.split_once('?')?;
    query
        .split('&')
        .find_map(|pair| pair.strip_prefix("t="))?
        .parse()
        .ok()
}

/// Records the token: committed locally, or with the `durable` feature,
/// durable before it returns.
fn record(token: i64) -> Result<(), storage::SqlError> {
    const SQL: &str = "INSERT INTO ledger (token) VALUES (?1)";
    if cfg!(feature = "durable") {
        storage::exec_durable(SQL, &params![token])?;
    } else {
        storage::exec(SQL, &params![token])?;
    }
    Ok(())
}

wasmkeep_sdk::export!(Ledger);
