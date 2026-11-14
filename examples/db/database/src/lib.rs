//! The database class of the db example: one actor per database
//! (`db/main`), its schema applied by `db::migrate` on start, serving the
//! SDK's protocol to the app's other actors only (an outside client gets
//! `403`). The limits are lowered so the example's tests reach them.

use wasmkeep_sdk::http::{IncomingRequest, ResponseOutparam};
use wasmkeep_sdk::{Actor, db, http};

/// The schema, in order. Append to it; never edit an applied entry.
const MIGRATIONS: &[&str] = &[
    "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL UNIQUE)",
    "CREATE TABLE vals (k TEXT PRIMARY KEY, v)",
    "CREATE TABLE tags (note INTEGER NOT NULL REFERENCES notes (id) DEFERRABLE INITIALLY DEFERRED, tag TEXT NOT NULL)",
];

const LIMITS: db::Limits = db::Limits {
    max_request_bytes: 64 * 1024,
    max_statements: 10,
    max_sql_bytes: 4096,
    max_params: 8,
    max_response_bytes: 64 * 1024,
};

struct Database;

impl Actor for Database {
    fn on_init() -> Result<(), String> {
        db::migrate(MIGRATIONS).map(drop).map_err(|e| e.to_string())
    }
}

impl http::Guest for Database {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        db::handle_with(&LIMITS, request, out, |_| true);
    }
}

wasmkeep_sdk::export!(Database);
