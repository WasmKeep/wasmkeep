//! Watch: one actor per monitored URL. The `hub` creates and configures
//! monitors; each runs its own checks on its alarm.
//!
//! - `PUT /config` (from the hub): `{"url", "every_s", "timeout_ms"}`.
//!   Starts checking at once.
//! - `GET /` : `{"id", "url", "state", "since", "last": {...}, "recent": [...],
//!   "uptime": 0.0..1.0}` (uptime over the kept checks).
//! - `DELETE /` (from the hub): stops and deletes the monitor.
//!   Changes from anyone but the hub are refused (403).
//! - WebSocket on the actor: every check as a JSON line, live.
//!
//! On each alarm: one HTTP GET, recorded; when the state flips (two failures
//! in a row to go down, one success to come back), the hub is told, and it
//! sends the alert. Then the next alarm. The schedule is the alarm, so it
//! survives hibernation, restarts and moves between nodes.

use serde::{Deserialize, Serialize};
use wasmkeep_sdk::http::{self, IncomingRequest, Method, ResponseOutparam};
use wasmkeep_sdk::{Actor, Message, Value, actors, alarms, now_ms, params, storage, websockets};

/// Checks kept per monitor.
const KEEP: i64 = 1_000;
/// Consecutive failures before a monitor counts as down.
const DOWN_AFTER: i64 = 2;
const HUB: &str = "hub/main";

struct Monitor;

#[derive(Deserialize, Serialize, Clone)]
struct Config {
    url: String,
    every_s: u64,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
}

fn default_timeout() -> u64 {
    5_000
}

#[derive(Serialize)]
struct Check {
    at: i64,
    ok: bool,
    status: Option<i64>,
    ms: i64,
    error: Option<String>,
}

#[derive(Serialize)]
struct Alert<'a> {
    id: &'a str,
    url: &'a str,
    state: &'a str,
    at: i64,
    detail: String,
}

fn now() -> i64 {
    i64::try_from(now_ms()).unwrap_or(i64::MAX)
}

fn config() -> Option<Config> {
    let rows = storage::query("SELECT json FROM config WHERE id = 1", &[]).ok()?;
    serde_json::from_str(rows.get(0, "json")?.as_str()?).ok()
}

fn meta(key: &str) -> Option<String> {
    let rows = storage::query("SELECT v FROM state WHERE k = ?1", &params![key]).ok()?;
    rows.get(0, "v").and_then(Value::as_str).map(str::to_owned)
}

fn set_meta(key: &str, value: &str) {
    let _ = storage::exec(
        "INSERT INTO state (k, v) VALUES (?1, ?2) ON CONFLICT (k) DO UPDATE SET v = ?2",
        &params![key, value],
    );
}

fn recent(limit: i64) -> Vec<Check> {
    let Ok(rows) = storage::query(
        "SELECT at, ok, status, ms, error FROM checks ORDER BY at DESC LIMIT ?1",
        &params![limit],
    ) else {
        return Vec::new();
    };
    (0..rows.len())
        .map(|i| Check {
            at: rows.get(i, "at").and_then(Value::as_i64).unwrap_or(0),
            ok: rows.get(i, "ok").and_then(Value::as_i64) == Some(1),
            status: rows.get(i, "status").and_then(Value::as_i64),
            ms: rows.get(i, "ms").and_then(Value::as_i64).unwrap_or(0),
            error: rows
                .get(i, "error")
                .and_then(Value::as_str)
                .map(str::to_owned),
        })
        .collect()
}

/// What a check calls itself: the site's operator sees who is asking, and
/// which build. Some sites answer a client they cannot name with an error.
const USER_AGENT: &str = concat!("wasmkeep-watch/", env!("CARGO_PKG_VERSION"));

/// One check: a GET, ok for any status below 400.
fn check(cfg: &Config) -> Check {
    let started = now();
    let result = http::fetch(
        "GET",
        &cfg.url,
        &[("user-agent", USER_AGENT)],
        &[],
        std::time::Duration::from_millis(cfg.timeout_ms),
        0,
    );
    let ms = now() - started;
    match result {
        Ok(r) => Check {
            at: started,
            ok: r.status < 400,
            status: Some(i64::from(r.status)),
            ms,
            error: None,
        },
        Err(e) => Check {
            at: started,
            ok: false,
            status: None,
            ms,
            error: Some(e),
        },
    }
}

fn run_check() -> Result<(), String> {
    let Some(cfg) = config() else {
        return Ok(()); // not configured (or deleted): nothing to do
    };
    let c = check(&cfg);
    storage::exec(
        "INSERT INTO checks (at, ok, status, ms, error) VALUES (?1, ?2, ?3, ?4, ?5)",
        &params![c.at, i64::from(c.ok), c.status, c.ms, c.error.clone()],
    )
    .map_err(|e| e.message)?;
    storage::exec(
        "DELETE FROM checks WHERE at < (SELECT min(at) FROM (SELECT at FROM checks ORDER BY at DESC LIMIT ?1))",
        &params![KEEP],
    )
    .map_err(|e| e.message)?;

    // State: down after DOWN_AFTER failures in a row, up after a success.
    let failures = if c.ok {
        0
    } else {
        meta("failures").and_then(|v| v.parse().ok()).unwrap_or(0) + 1
    };
    set_meta("failures", &failures.to_string());
    let previous = meta("state").unwrap_or_else(|| "unknown".into());
    let state = if c.ok {
        "up"
    } else if failures >= DOWN_AFTER {
        "down"
    } else {
        previous.as_str()
    };
    if state != previous {
        set_meta("state", state);
        set_meta("since", &c.at.to_string());
        // The first "up" of a new monitor is not news.
        if previous != "unknown" || state == "down" {
            let detail = match (&c.error, c.status) {
                (Some(e), _) => e.clone(),
                (None, Some(s)) => format!("HTTP {s}"),
                (None, None) => String::new(),
            };
            let me = actors::self_address().name;
            let alert = Alert {
                id: &me,
                url: &cfg.url,
                state,
                at: c.at,
                detail,
            };
            let body = serde_json::to_vec(&alert).unwrap_or_default();
            // The hub records it durably and delivers it; if the call
            // fails, the alarm is retried (and the flip with it).
            let response = actors::call(
                HUB,
                &actors::Request {
                    method: "POST".into(),
                    path: "/alert".into(),
                    headers: vec![("content-type".into(), "application/json".into())],
                    body,
                },
            )
            .map_err(|e| format!("telling the hub: {e:?}"))?;
            if response.status >= 300 {
                return Err(format!("the hub answered {}", response.status));
            }
        }
    }

    let line = serde_json::to_string(&c).unwrap_or_default();
    for conn in websockets::connections() {
        let _ = websockets::send(conn, &Message::Text(line.clone()));
    }
    let next = u64::try_from(c.at).unwrap_or(0) + cfg.every_s.max(1) * 1_000;
    alarms::set_alarm(next);
    Ok(())
}

fn status_json() -> serde_json::Value {
    let cfg = config();
    let recent = recent(50);
    let kept = storage::query(
        "SELECT count(*) AS n, coalesce(sum(ok), 0) AS up FROM checks",
        &[],
    )
    .ok();
    let n = kept
        .as_ref()
        .and_then(|r| r.get(0, "n").and_then(Value::as_i64))
        .unwrap_or(0);
    let up = kept
        .as_ref()
        .and_then(|r| r.get(0, "up").and_then(Value::as_i64))
        .unwrap_or(0);
    serde_json::json!({
        "id": actors::self_address().name,
        "url": cfg.as_ref().map(|c| c.url.clone()),
        "every_s": cfg.as_ref().map(|c| c.every_s),
        "state": meta("state").unwrap_or_else(|| "unknown".into()),
        "since": meta("since").and_then(|v| v.parse::<i64>().ok()),
        "checks": n,
        "uptime": if n > 0 { up as f64 / n as f64 } else { 1.0 },
        "last": recent.first(),
        "recent": recent,
    })
}

fn json(out: ResponseOutparam, status: u16, value: &serde_json::Value) {
    http::respond(
        out,
        status,
        "application/json",
        serde_json::to_vec(value).unwrap_or_default().as_slice(),
    );
}

impl Actor for Monitor {
    fn on_init() -> Result<(), String> {
        for sql in [
            "CREATE TABLE IF NOT EXISTS config (id INTEGER PRIMARY KEY CHECK (id = 1), json TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS state (k TEXT PRIMARY KEY, v TEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS checks (at INTEGER NOT NULL, ok INTEGER NOT NULL, status INTEGER, ms INTEGER NOT NULL, error TEXT)",
            "CREATE INDEX IF NOT EXISTS checks_at ON checks (at)",
        ] {
            storage::exec(sql, &[]).map_err(|e| e.message)?;
        }
        Ok(())
    }

    fn on_alarm() -> Result<(), String> {
        run_check()
    }

    fn on_websocket_open(conn: u64, _path: String) -> Result<(), String> {
        for c in recent(20).iter().rev() {
            let _ = websockets::send(
                conn,
                &Message::Text(serde_json::to_string(c).unwrap_or_default()),
            );
        }
        Ok(())
    }
}

impl http::Guest for Monitor {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let path = request.path_with_query().unwrap_or_default();
        let path = path.split('?').next().unwrap_or("/").to_owned();
        // Changes come from the hub only: the host sets this header on calls
        // between actors ("<tenant>/<class>/<name>") and strips it from
        // outside requests. Without the check, anyone could point a monitor
        // at any URL or delete it, around the hub and its token.
        let from_hub = request
            .headers()
            .get("x-wasmkeep-caller")
            .first()
            .is_some_and(|c| String::from_utf8_lossy(c).split('/').nth(1) == Some("hub"));
        if !matches!(request.method(), Method::Get | Method::Head) && !from_hub {
            return http::respond(out, 403, "text/plain", b"changes go through the hub\n");
        }
        match (request.method(), path.as_str()) {
            (Method::Put, "/config") => {
                let body = http::read_body(&request, 16 * 1024).unwrap_or_default();
                let Ok(cfg) = serde_json::from_slice::<Config>(&body) else {
                    return http::respond(
                        out,
                        400,
                        "text/plain",
                        b"expected {\"url\", \"every_s\"}\n",
                    );
                };
                let text = serde_json::to_string(&cfg).unwrap_or_default();
                if let Err(e) = storage::exec(
                    "INSERT INTO config (id, json) VALUES (1, ?1) ON CONFLICT (id) DO UPDATE SET json = ?1",
                    &params![text],
                ) {
                    return http::respond(out, 500, "text/plain", e.message.as_bytes());
                }
                alarms::set_alarm(now_ms()); // first check now
                json(out, 200, &status_json());
            }
            (Method::Get, "/") => json(out, 200, &status_json()),
            (Method::Delete, "/") => {
                storage::delete_self();
                http::respond(out, 200, "text/plain", b"deleted\n");
            }
            _ => http::respond(out, 404, "text/plain", b"no route\n"),
        }
    }
}

wasmkeep_sdk::export!(Monitor);
