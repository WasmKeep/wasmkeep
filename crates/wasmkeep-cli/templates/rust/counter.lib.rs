//! Class `counter`: each actor (`counter/alice`, `counter/bob`, ...) is a
//! counter of its own, in its own SQLite database, so it survives restarts.
//!
//! - `GET /` answers the value
//! - `POST /` adds one and answers the new value

use wasmkeep_sdk::http::{self, IncomingRequest, Method, ResponseOutparam};
use wasmkeep_sdk::{Actor, storage};

struct Counter;

impl Actor for Counter {
    /// Runs before the actor's first request, every time it starts.
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
            "INSERT OR IGNORE INTO counter (id, value) VALUES (1, 0)",
            &[],
        )
        .map_err(|e| e.to_string())?;
        Ok(())
    }
}

impl http::Guest for Counter {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let rows = match request.method() {
            Method::Get => storage::query("SELECT value FROM counter WHERE id = 1", &[]),
            Method::Post => storage::query(
                "UPDATE counter SET value = value + 1 WHERE id = 1 RETURNING value",
                &[],
            ),
            _ => return http::respond(out, 405, "text/plain", b"use GET or POST\n"),
        };
        match rows {
            Ok(rows) => {
                let value = rows.get(0, "value").and_then(|v| v.as_i64()).unwrap_or(0);
                http::respond(out, 200, "text/plain", format!("{value}\n").as_bytes());
            }
            Err(e) => {
                // The detail to the log, a generic answer to the caller.
                eprintln!("storage: {e}");
                http::respond(out, 500, "text/plain", b"internal error\n");
            }
        }
    }
}

wasmkeep_sdk::export!(Counter);
