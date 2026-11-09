//! A fixed-window rate limiter. Each actor name is one key (an API key, an
//! IP, a user id), so every key has its own counter and limits never contend.
//!
//! - `POST /` takes one request from the current window: `200` with the
//!   number left, or `429` with `retry-after` once the limit is reached.
//!   Optional `?limit=N&window_ms=MS` set the window size when it opens
//!   (defaults: 10 requests per 10 s).
//! - `GET /` shows the current window without taking from it.
//!
//! The first request of a window sets an alarm for its end. The alarm closes
//! the window; if the window saw no requests beyond the first, the key is
//! idle and the actor deletes itself, so unused keys cost nothing.

use wasmkeep_sdk::http::{self, IncomingRequest, Method, ResponseOutparam};
use wasmkeep_sdk::{Actor, SqlError, Value, alarms, now_ms, params, storage};

const DEFAULT_LIMIT: i64 = 10;
const DEFAULT_WINDOW_MS: i64 = 10_000;

struct RateLimiter;

/// The open window, if any: (limit, ends_at_ms, used).
fn window() -> Result<Option<(i64, i64, i64)>, SqlError> {
    let rows = storage::query("SELECT lim, ends_at, used FROM win WHERE id = 1", &[])?;
    let int = |c: &str| rows.get(0, c).and_then(Value::as_i64);
    Ok(match (int("lim"), int("ends_at"), int("used")) {
        (Some(l), Some(e), Some(u)) => Some((l, e, u)),
        _ => None,
    })
}

fn param(path: &str, key: &str) -> Option<i64> {
    let query = path.split_once('?')?.1;
    query
        .split('&')
        .find_map(|kv| kv.strip_prefix(key)?.strip_prefix('='))?
        .parse()
        .ok()
}

fn take(path: &str) -> Result<(u16, String), SqlError> {
    let now = i64::try_from(now_ms()).unwrap_or(i64::MAX);
    let (limit, ends_at, used) = match window()? {
        Some(w) => w,
        None => {
            let limit = param(path, "limit").unwrap_or(DEFAULT_LIMIT).max(1);
            let ends_at = now + param(path, "window_ms").unwrap_or(DEFAULT_WINDOW_MS).max(1);
            storage::exec(
                "INSERT INTO win (id, lim, ends_at, used) VALUES (1, ?1, ?2, 0)",
                &params![limit, ends_at],
            )?;
            alarms::set_alarm(u64::try_from(ends_at).unwrap_or(u64::MAX));
            (limit, ends_at, 0)
        }
    };
    if used >= limit {
        let retry_after_s = ((ends_at - now).max(0) + 999) / 1000;
        return Ok((429, format!("limited; retry in {retry_after_s}s\n")));
    }
    storage::exec("UPDATE win SET used = used + 1 WHERE id = 1", &[])?;
    Ok((200, format!("ok; {} left\n", limit - used - 1)))
}

impl Actor for RateLimiter {
    fn on_init() -> Result<(), String> {
        storage::exec(
            "CREATE TABLE IF NOT EXISTS win (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                lim INTEGER NOT NULL,
                ends_at INTEGER NOT NULL,
                used INTEGER NOT NULL
            )",
            &[],
        )
        .map(drop)
        .map_err(|e| e.to_string())
    }

    fn on_alarm() -> Result<(), String> {
        let idle = matches!(window(), Ok(Some((_, _, used))) if used <= 1);
        storage::exec("DELETE FROM win", &[]).map_err(|e| e.to_string())?;
        if idle {
            // Nobody is using this key: drop the whole actor.
            storage::delete_self();
        }
        Ok(())
    }
}

impl http::Guest for RateLimiter {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let path = request.path_with_query().unwrap_or_default();
        let result = match request.method() {
            Method::Post => take(&path),
            Method::Get => window().map(|w| match w {
                Some((limit, _, used)) => (200, format!("{} of {limit} used\n", used)),
                None => (200, "no open window\n".to_owned()),
            }),
            _ => Ok((405, "use GET or POST\n".to_owned())),
        };
        match result {
            Ok((status, body)) => http::respond(out, status, "text/plain", body.as_bytes()),
            Err(e) => http::respond(out, 500, "text/plain", format!("{e}\n").as_bytes()),
        }
    }
}

wasmkeep_sdk::export!(RateLimiter);
