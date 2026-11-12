//! Test actor. Routes:
//! - `POST /alarm?in=MS[&rollback]` sets an alarm MS from now (or
//!   `?at=MS` at an absolute time, ms since the epoch) (inside a
//!   transaction that is rolled back with `rollback`)
//! - `DELETE /alarm`, `GET /alarm` (`none` or the time)
//! - `GET /fired`: successful `on-alarm` runs; `GET /attempts`: all runs
//! - `POST /fail-next?n=K`: the next K `on-alarm` runs return an error
//! - `POST /repeat?n=K`: `on-alarm` re-arms itself (+20 ms) until K fired
//! - `POST /trap`: panics; `POST /delete`: `delete-self`
//! - `POST /delete-on-alarm`: the next `on-alarm` run calls `delete-self`
//! - `POST /call?to=CLASS/NAME&path=P` (P percent-encoded): `actors.call`
//!   with GET P; answers `STATUS:BODY` or the call error (`&times=N`: the
//!   same call N times in a row, answers joined by ` | `)
//! - `POST /sleep?ms=MS`: waits MS on the host clock (no CPU used), counts
//!   in `slept`; `GET /slept`
//! - `POST /durable`: adds 1 to `durable` with `exec-durable`; answers the
//!   new value; `GET /durable`
//! - `POST /plain`: adds 1 to `plain` with a plain `exec` (uploaded after
//!   the answer: lost with its node if not uploaded yet); answers the new
//!   value; `GET /plain`
//! - `POST /durable-twice`: two `exec-durable` increments in one event;
//!   answers `first=ok|err second=ok|err`
//! - `POST /durable-in-tx`: `exec-durable` inside BEGIN/ROLLBACK; answers
//!   `ok` or `err`
//! - `POST /durable-commit`: BEGIN, adds 1 to `durable` with `exec`, then
//!   `exec-durable("COMMIT")`; answers the new value (503 with the error)
//! - `POST /caller`: the `x-wasmkeep-caller` header, or `none`
//! - WebSockets: accepted (refused with "go away" when the path contains
//!   `refuse`); text is echoed; `POST /ws-closed` counts close callbacks
//! - `POST /broadcast?text=T`: sends T to every connection it sees
//! - `POST /memory`: adds 1 to a counter in the instance's memory (not
//!   the database) and answers it: 1 again means a new instance
//! - `POST /sql` with SQL as the body (and `?blob=N` to bind an N-byte
//!   blob as ?1): runs it with `query`, answers `ok:ROWS` or `err:KIND`
//! - `POST /body-len`: reads the whole body (up to 64 MB) and answers its
//!   length, or `unreadable`
//! - `GET /inits`: how many times `on-init` ran (an actor whose name starts
//!   with `alarm-on-init` also sets an alarm from `on-init`, 1.5 s out)
//! - `GET /fetch?url=U` (U percent-encoded): outbound GET, answers
//!   `STATUS:BODY` or `error: ...`; with `&send=N`, a POST with an N-byte
//!   body instead
//! - `GET /env?name=N`: `env.get(N)`, and whether WASI saw N

use wasmkeep_sdk::http::{self, IncomingRequest, Method, ResponseOutparam};
use wasmkeep_sdk::actors::{self, CallError};
use wasmkeep_sdk::{Actor, Value, alarms, env, now_ms, params, storage};

struct TestActor;

fn get(key: &str) -> i64 {
    storage::query("SELECT value FROM config WHERE key = ?1", &params![key])
        .ok()
        .and_then(|r| r.get(0, "value").and_then(Value::as_i64))
        .unwrap_or(0)
}

fn set(key: &str, value: i64) {
    let _ = storage::exec(
        "INSERT INTO config (key, value) VALUES (?1, ?2)
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        &params![key, value],
    );
}

fn count(table: &str) -> i64 {
    storage::query(&format!("SELECT count(*) AS n FROM {table}"), &[])
        .ok()
        .and_then(|r| r.get(0, "n").and_then(Value::as_i64))
        .unwrap_or(-1)
}

impl Actor for TestActor {
    fn on_init() -> Result<(), String> {
        for sql in [
            "CREATE TABLE IF NOT EXISTS fired (at INTEGER NOT NULL)",
            "CREATE TABLE IF NOT EXISTS attempts (at INTEGER NOT NULL)",
            "CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value INTEGER NOT NULL)",
            "CREATE TABLE IF NOT EXISTS inits (n INTEGER NOT NULL)",
            "INSERT INTO inits VALUES (1)",
        ] {
            storage::exec(sql, &[]).map_err(|e| e.to_string())?;
        }
        // Actors named alarm-on-init... set an alarm from on-init, 1.5 s out.
        if actors::self_address().name.starts_with("alarm-on-init") && alarms::get_alarm().is_none() {
            alarms::set_alarm(now_ms() + 1_500);
        }
        Ok(())
    }

    /// Accepts every WebSocket and echoes text.
    fn on_websocket_open(_conn: u64, path: String) -> Result<(), String> {
        if path.contains("refuse") {
            return Err("go away".into());
        }
        Ok(())
    }

    fn on_websocket_message(conn: u64, msg: wasmkeep_sdk::Message) {
        let _ = wasmkeep_sdk::websockets::send(conn, &msg);
    }

    fn on_websocket_close(_conn: u64, _code: u16, _reason: String) {
        set("ws_closed", get("ws_closed") + 1);
    }

    fn on_alarm() -> Result<(), String> {
        let now = i64::try_from(now_ms()).unwrap_or(i64::MAX);
        storage::exec("INSERT INTO attempts VALUES (?1)", &params![now])
            .map_err(|e| e.to_string())?;
        if get("delete_on_alarm") > 0 {
            storage::delete_self();
            return Ok(());
        }
        // Reschedule for now, then fail (a handler draining a backlog).
        let resched = get("resched_fail");
        if resched > 0 {
            set("resched_fail", resched - 1);
            alarms::set_alarm(now_ms());
            return Err("rescheduled, then failed".into());
        }
        let failures = get("fail_next");
        if failures > 0 {
            set("fail_next", failures - 1);
            return Err("failing on purpose".into());
        }
        storage::exec("INSERT INTO fired VALUES (?1)", &params![now])
            .map_err(|e| e.to_string())?;
        if count("fired") < get("repeat") {
            alarms::set_alarm(now_ms() + 20);
        }
        Ok(())
    }
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() + 1 {
            if let Some(v) = s.get(i + 1..i + 3).and_then(|h| u8::from_str_radix(h, 16).ok()) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn query_param(path: &str, key: &str) -> Option<String> {
    let query = path.split_once('?')?.1;
    query.split('&').find_map(|pair| match pair.split_once('=') {
        Some((k, v)) if k == key => Some(v.to_owned()),
        None if pair == key => Some(String::new()),
        _ => None,
    })
}

impl http::Guest for TestActor {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let full = request.path_with_query().unwrap_or_default();
        let path = full.split('?').next().unwrap_or("/").to_owned();
        let num = |k: &str| query_param(&full, k).and_then(|v| v.parse::<i64>().ok()).unwrap_or(0);
        let body = match (request.method(), path.as_str()) {
            (Method::Post, "/alarm") => {
                let rollback = query_param(&full, "rollback").is_some();
                if rollback {
                    let _ = storage::exec("BEGIN", &[]);
                }
                // `at` is an absolute time (ms since the epoch); `in` relative.
                let at = match query_param(&full, "at").and_then(|v| v.parse::<u64>().ok()) {
                    Some(at) => at,
                    None => now_ms() + u64::try_from(num("in")).unwrap_or(0),
                };
                alarms::set_alarm(at);
                if rollback {
                    let _ = storage::exec("ROLLBACK", &[]);
                }
                "ok".to_owned()
            }
            (Method::Delete, "/alarm") => {
                alarms::clear_alarm();
                "ok".to_owned()
            }
            (Method::Get, "/alarm") => match alarms::get_alarm() {
                Some(at) => at.to_string(),
                None => "none".to_owned(),
            },
            (Method::Get | Method::Post, "/fired") => count("fired").to_string(),
            (Method::Get, "/attempts") => count("attempts").to_string(),
            (Method::Post, "/delete-on-alarm") => {
                set("delete_on_alarm", 1);
                "ok".to_owned()
            }
            (Method::Post, "/fail-next") => {
                set("fail_next", num("n"));
                "ok".to_owned()
            }
            (Method::Get, "/escape") => {
                // Every way out besides outbound HTTP must be closed: raw
                // TCP, name lookups, the filesystem.
                let port = num("port");
                let tcp = std::net::TcpStream::connect(("127.0.0.1", u16::try_from(port).unwrap_or(1)));
                let dns = std::net::ToSocketAddrs::to_socket_addrs(&("localhost", 80));
                let dir = std::fs::read_dir("/");
                let file = std::fs::read("/etc/passwd");
                format!(
                    "tcp={} dns={} dir={} file={}",
                    if tcp.is_ok() { "open" } else { "closed" },
                    if dns.is_ok() { "open" } else { "closed" },
                    if dir.is_ok() { "open" } else { "closed" },
                    if file.is_ok() { "open" } else { "closed" },
                )
            }
            (Method::Post, "/resched-fail") => {
                set("resched_fail", num("n"));
                "ok".to_owned()
            }
            (Method::Post, "/repeat") => {
                set("repeat", num("n"));
                "ok".to_owned()
            }
            (Method::Post, "/trap") => panic!("trap requested"),
            (Method::Post, "/call") => {
                let to = query_param(&full, "to").map(|t| percent_decode(&t)).unwrap_or_default();
                let path = query_param(&full, "path").map_or("/fired".to_owned(), |p| percent_decode(&p));
                let request = actors::Request {
                    method: "POST".into(),
                    path,
                    headers: vec![],
                    body: vec![],
                };
                let call = || match actors::call(&to, &request) {
                    Ok(r) => format!("{}:{}", r.status, String::from_utf8_lossy(&r.body).trim()),
                    Err(CallError::Reentrant) => "reentrant".into(),
                    Err(CallError::InvalidAddress(m)) => format!("invalid-address:{m}"),
                    Err(CallError::Failed(m)) => format!("failed:{m}"),
                    Err(CallError::Timeout) => "timeout".into(),
                    Err(CallError::TooLarge) => "too-large".into(),
                };
                (0..num("times").max(1)).map(|_| call()).collect::<Vec<_>>().join(" | ")
            }
            (Method::Post, "/durable") => {
                let r = storage::exec_durable(
                    "INSERT INTO config (key, value) VALUES ('durable', 1)
                     ON CONFLICT (key) DO UPDATE SET value = value + 1",
                    &[],
                );
                match r {
                    Ok(_) => get("durable").to_string(),
                    Err(e) => {
                        http::respond(out, 503, "text/plain", format!("{e:?}\n").as_bytes());
                        return;
                    }
                }
            }
            (Method::Get, "/durable") => get("durable").to_string(),
            // A GET that leaves a trace: how many of them ran.
            (Method::Get, "/visit") => {
                let _ = storage::exec_durable(
                    "INSERT INTO config (key, value) VALUES ('visits', 1)
                     ON CONFLICT (key) DO UPDATE SET value = value + 1",
                    &[],
                );
                get("visits").to_string()
            }
            (Method::Post, "/plain") => {
                let r = storage::exec(
                    "INSERT INTO config (key, value) VALUES ('plain', 1)
                     ON CONFLICT (key) DO UPDATE SET value = value + 1",
                    &[],
                );
                match r {
                    Ok(_) => get("plain").to_string(),
                    Err(e) => {
                        http::respond(out, 503, "text/plain", format!("{e:?}\n").as_bytes());
                        return;
                    }
                }
            }
            (Method::Get, "/plain") => get("plain").to_string(),
            (Method::Post, "/durable-in-tx") => {
                let _ = storage::exec("BEGIN", &[]);
                let r = storage::exec_durable(
                    "INSERT INTO config (key, value) VALUES ('durable', 1)
                     ON CONFLICT (key) DO UPDATE SET value = value + 1",
                    &[],
                );
                let _ = storage::exec("ROLLBACK", &[]);
                if r.is_ok() { "ok" } else { "err" }.to_owned()
            }
            (Method::Post, "/durable-commit") => {
                let r = storage::exec("BEGIN", &[])
                    .and_then(|_| {
                        storage::exec(
                            "INSERT INTO config (key, value) VALUES ('durable', 1)
                             ON CONFLICT (key) DO UPDATE SET value = value + 1",
                            &[],
                        )
                    })
                    .and_then(|_| storage::exec_durable("COMMIT", &[]));
                match r {
                    Ok(_) => get("durable").to_string(),
                    Err(e) => {
                        http::respond(out, 503, "text/plain", format!("{e:?}\n").as_bytes());
                        return;
                    }
                }
            }
            (Method::Post, "/durable-twice") => {
                let inc = || {
                    storage::exec_durable(
                        "INSERT INTO config (key, value) VALUES ('durable', 1)
                         ON CONFLICT (key) DO UPDATE SET value = value + 1",
                        &[],
                    )
                    .is_ok()
                };
                let word = |ok: bool| if ok { "ok" } else { "err" };
                let (a, b) = (inc(), inc());
                format!("first={} second={}", word(a), word(b))
            }
            (Method::Post, "/broadcast") => {
                let text = query_param(&full, "text").unwrap_or_default();
                let conns = wasmkeep_sdk::websockets::connections();
                for c in &conns {
                    let _ = wasmkeep_sdk::websockets::send(*c, &wasmkeep_sdk::Message::Text(text.clone()));
                }
                conns.len().to_string()
            }
            (Method::Post, "/ws-closed") => get("ws_closed").to_string(),
            (Method::Post, "/sleep") => {
                let ns = u64::try_from(num("ms")).unwrap_or(0) * 1_000_000;
                wasmkeep_sdk::wasi::clocks::monotonic_clock::subscribe_duration(ns).block();
                let _ = storage::exec(
                    "INSERT INTO config (key, value) VALUES ('slept', 1)
                     ON CONFLICT (key) DO UPDATE SET value = value + 1",
                    &[],
                );
                "slept".to_owned()
            }
            (Method::Get, "/slept") => get("slept").to_string(),
            (Method::Post, "/caller") => request
                .headers()
                .get("x-wasmkeep-caller")
                .first()
                .map_or("none".to_owned(), |v| String::from_utf8_lossy(v).into_owned()),
            (Method::Get, "/fetch") => {
                let url = query_param(&full, "url").map(|u| percent_decode(&u)).unwrap_or_default();
                let send: usize = query_param(&full, "send").and_then(|n| n.parse().ok()).unwrap_or(0);
                let body = vec![b'x'; send];
                let method = if send > 0 { "POST" } else { "GET" };
                match wasmkeep_sdk::http::fetch(method, &url, &[], &body, std::time::Duration::from_secs(5), 4096) {
                    Ok(r) => format!("{}:{}", r.status, String::from_utf8_lossy(&r.body)),
                    Err(e) => format!("error: {e}"),
                }
            }
            (Method::Post, "/memory") => {
                static MEMORY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                (MEMORY.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1).to_string()
            }
            (Method::Post, "/sql") => {
                let sql = String::from_utf8(http::read_body(&request, 64 * 1024).unwrap_or_default())
                    .unwrap_or_default();
                let blob = query_param(&full, "blob").and_then(|n| n.parse::<usize>().ok());
                let params: Vec<Value> = blob.map(|n| vec![Value::Blob(vec![0x41; n])]).unwrap_or_default();
                match storage::query(&sql, &params) {
                    Ok(rows) => format!("ok:{}", rows.values.len()),
                    Err(e) => format!("err:{:?}", e.kind),
                }
            }
            (Method::Post, "/body-len") => match http::read_body(&request, 64 << 20) {
                Some(body) => body.len().to_string(),
                None => "unreadable".into(),
            },
            // The body to stdout, and its first line again to stderr
            // without a newline (a partial line, flushed when the instance
            // ends).
            (Method::Post, "/print") => match http::read_body(&request, 1 << 20) {
                Some(body) => {
                    let text = String::from_utf8_lossy(&body);
                    print!("{text}");
                    eprint!("err: {}", text.lines().next().unwrap_or(""));
                    "printed".into()
                }
                None => "unreadable".into(),
            },
            (Method::Get, "/inits") => count("inits").to_string(),
            (Method::Get, "/env") => {
                // `env.get`, and whether the same name leaked into the WASI
                // environment (it must not).
                let name = query_param(&full, "name").unwrap_or_default();
                let wasi = if std::env::var_os(&name).is_some() { "set" } else { "unset" };
                match env::get(&name) {
                    Some(v) => format!("{v} wasi={wasi}"),
                    None => format!("(none) wasi={wasi}"),
                }
            }
            (Method::Post, "/delete") => {
                storage::delete_self();
                "ok".to_owned()
            }
            _ => {
                http::respond(out, 404, "text/plain", b"no route\n");
                return;
            }
        };
        http::respond(out, 200, "text/plain", format!("{body}\n").as_bytes());
    }
}

wasmkeep_sdk::export!(TestActor);
