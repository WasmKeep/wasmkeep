//! A counter actor. Each actor name is its own counter:
//!
//! - `GET`  returns the current value
//! - `POST` increments it and returns the new value
//!
//! The value lives in the actor's SQLite database, so it survives restarts.

use wasmkeep_sdk::http::{self, IncomingRequest, Method, ResponseOutparam};
use wasmkeep_sdk::{Actor, params, storage};

struct Counter;

impl Actor for Counter {
    fn on_init() -> Result<(), String> {
        storage::exec(
            "CREATE TABLE IF NOT EXISTS counter (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                value INTEGER NOT NULL
            )",
            &[],
        )
        .map_err(|e| e.to_string())?;
        storage::exec(
            "INSERT OR IGNORE INTO counter (id, value) VALUES (?1, ?2)",
            &params![1, 0],
        )
        .map_err(|e| e.to_string())?;
        Ok(())
    }
}

impl http::Guest for Counter {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let result = match request.method() {
            Method::Get => storage::query("SELECT value FROM counter WHERE id = 1", &[]),
            Method::Post => storage::query(
                "UPDATE counter SET value = value + 1 WHERE id = 1 RETURNING value",
                &[],
            ),
            _ => {
                http::respond(out, 405, "text/plain", b"use GET or POST\n");
                return;
            }
        };
        match result {
            Ok(rows) => {
                let value = rows.get(0, "value").and_then(|v| v.as_i64()).unwrap_or(0);
                http::respond(out, 200, "text/plain", format!("{value}\n").as_bytes());
            }
            Err(e) => http::respond(out, 500, "text/plain", format!("{e}\n").as_bytes()),
        }
    }
}

wasmkeep_sdk::export!(Counter);
