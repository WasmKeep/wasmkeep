//! The ledger class of the workflows example (`ledger/main`): it stands in
//! for the services a workflow's steps call (stock, payments, mail), and
//! records every call, so the example (and its tests) can see what ran and
//! how often.
//!
//! - `POST /record?what=W&key=K`: records `W` with idempotency key `K`;
//!   answers `W#n`, `n` the number of calls with that key so far. A second
//!   call with a key is a step that ran again.
//! - `POST /fail?what=PREFIX&times=N`: the next `N` calls whose `what`
//!   starts with `PREFIX` fail (`503`), to see retries; with `&trap=1`
//!   they answer `599`, on which the `orders` steps panic (a trap in the
//!   middle of a step).
//! - `GET /entries`: every call, one per line: `what key n`.
//!
//! Only other actors may call it (`403` from outside), except `/fail` and
//! `/entries`, which the example's tests and readers use.

use wasmkeep_sdk::http::{self, IncomingRequest, Method, ResponseOutparam};
use wasmkeep_sdk::{Actor, Value, params, storage};

struct Ledger;

fn param<'a>(path: &'a str, name: &str) -> Option<&'a str> {
    path.split_once('?')?
        .1
        .split('&')
        .find_map(|p| p.strip_prefix(name)?.strip_prefix('='))
}

fn caller(request: &IncomingRequest) -> bool {
    !request.headers().get("x-wasmkeep-caller").is_empty()
}

impl Actor for Ledger {
    fn on_init() -> Result<(), String> {
        for sql in [
            "CREATE TABLE IF NOT EXISTS entries (seq INTEGER PRIMARY KEY, what TEXT NOT NULL, \
             key TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS failing (prefix TEXT PRIMARY KEY, times INTEGER NOT NULL, \
             trap INTEGER NOT NULL DEFAULT 0)",
        ] {
            storage::exec(sql, &[]).map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

fn record(what: &str, key: &str) -> Result<(u16, String), storage::SqlError> {
    let failing = storage::query(
        "SELECT prefix, trap FROM failing WHERE times > 0 AND substr(?, 1, length(prefix)) = prefix",
        &params![what],
    )?;
    if let Some(row) = failing.values.first()
        && let Value::Text(prefix) = &row[0]
    {
        storage::exec(
            "UPDATE failing SET times = times - 1 WHERE prefix = ?",
            &params![prefix.clone()],
        )?;
        let status = if row[1].as_i64() == Some(1) { 599 } else { 503 };
        return Ok((status, format!("{what} failed on purpose\n")));
    }
    storage::exec_durable(
        "INSERT INTO entries (what, key) VALUES (?, ?)",
        &params![what, key],
    )?;
    let n = storage::query("SELECT count(*) FROM entries WHERE key = ?", &params![key])?
        .values
        .first()
        .and_then(|r| r.first().and_then(Value::as_i64))
        .unwrap_or(0);
    Ok((200, format!("{what}#{n}")))
}

impl http::Guest for Ledger {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let path = request.path_with_query().unwrap_or_default();
        let route = path.split('?').next().unwrap_or_default().to_owned();
        let result = match (request.method(), route.as_str()) {
            (Method::Post, "/record") if caller(&request) => record(
                param(&path, "what").unwrap_or_default(),
                param(&path, "key").unwrap_or_default(),
            ),
            (Method::Post, "/record") => Ok((403, "only actors record\n".into())),
            (Method::Post, "/fail") => storage::exec(
                "INSERT INTO failing (prefix, times, trap) VALUES (?, ?, ?) \
                 ON CONFLICT (prefix) DO UPDATE SET times = excluded.times, trap = excluded.trap",
                &params![
                    param(&path, "what").unwrap_or_default(),
                    param(&path, "times")
                        .and_then(|t| t.parse::<i64>().ok())
                        .unwrap_or(1),
                    i64::from(param(&path, "trap") == Some("1"))
                ],
            )
            .map(|_| (200, "ok\n".into())),
            (Method::Get, "/entries") => storage::query(
                "SELECT what, key, (SELECT count(*) FROM entries e2 WHERE e2.key = e.key \
                 AND e2.seq <= e.seq) FROM entries e ORDER BY seq",
                &[],
            )
            .map(|rows| {
                let lines: String = rows
                    .values
                    .iter()
                    .map(|r| {
                        let t = |v: &Value| match v {
                            Value::Text(s) => s.clone(),
                            Value::Integer(i) => i.to_string(),
                            _ => String::new(),
                        };
                        format!("{} {} {}\n", t(&r[0]), t(&r[1]), t(&r[2]))
                    })
                    .collect();
                (200, lines)
            }),
            _ => Ok((404, "not found\n".into())),
        };
        let (status, body) = result.unwrap_or_else(|e| (500, format!("{e}\n")));
        http::respond(out, status, "text/plain", body.as_bytes());
    }
}

wasmkeep_sdk::export!(Ledger);
