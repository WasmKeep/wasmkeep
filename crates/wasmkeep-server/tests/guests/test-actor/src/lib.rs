//! Test actor. Routes:
//! - `POST /alarm?in=MS[&rollback]` sets an alarm MS from now (or
//!   `?at=MS` at an absolute time, ms since the epoch) (inside a
//!   transaction that is rolled back with `rollback`)
//! - `DELETE /alarm`, `GET /alarm` (`none` or the time)
//! - `GET /fired`: successful `on-alarm` runs; `GET /attempts`: all runs
//! - `POST /fail-next?n=K`: the next K `on-alarm` runs return an error
//! - `POST /repeat?n=K`: `on-alarm` re-arms itself (+20 ms) until K fired
//! - `POST /trap`: panics; `POST /delete`: `delete-self`
//! - `POST /delete-durable`: `delete-self-durable`; answers `deleted`, or
//!   503 with the error (then `?then=sql` runs a `query` after it and
//!   answers `ok` or `err:KIND`; `?retry=1` calls it twice and answers
//!   `first=ok|err second=ok|err`)
//! - `POST /delete-on-alarm`: the next `on-alarm` run calls `delete-self`
//! - `POST /on-alarm?do=D[&at=T][&n=K]`: what each successful `on-alarm`
//!   run does with the alarm after counting itself (K times, default 1):
//!   `again` sets it to T (the delivered time, when the test set the alarm
//!   with `?at=T`), `set` to an hour from now, `clear` sets T then clears,
//!   `rollback` sets T inside BEGIN ... ROLLBACK, `open` sets T inside a
//!   transaction it leaves open, `restore` saves `get-alarm`, sets two
//!   seconds out and puts the saved value back (clearing for none), and
//!   `look` only reads it. Each records `get-alarm` before and after;
//!   `GET /observed` answers `BEFORE AFTER` (a time or `none`)
//! - `POST /call?to=CLASS/NAME&path=P` (P percent-encoded): `actors.call`
//!   with GET P; answers `STATUS:BODY` or the call error (`&times=N`: the
//!   same call N times in a row, answers joined by ` | `)
//! - `POST /call-many?to=A,B,...&path=P`: one `calls::call_many` with a
//!   POST P to each address (comma-separated, P percent-encoded); answers
//!   each entry as `/call` does, joined by ` | `, in input order
//! - `POST /sleep?ms=MS`: waits MS on the host clock (no CPU used), counts
//!   in `slept`; `GET /slept`
//! - `POST /durable`: adds 1 to `durable` with `exec-durable`; answers the
//!   new value; `GET /durable`
//! - `POST /plain`: adds 1 to `plain` with a plain `exec` (uploaded after
//!   the answer: lost with its node if not uploaded yet); answers the new
//!   value; `GET /plain`
//! - `POST /flush?n=N[&alarm=MS][&tx][&twice][&after=K]`: N plain `exec`
//!   increments of `plain` (default none), an alarm MS from now with
//!   `alarm`, then `flush` (inside BEGIN ... ROLLBACK with `tx`; twice
//!   with `twice`), then K more plain increments; answers each flush as
//!   `ok:PLAIN` (`plain` at the flush) or `err:KIND:MESSAGE`, joined by
//!   ` | `
//! - `POST /durable-twice`: two `exec-durable` increments in one event;
//!   answers `first=ok|err second=ok|err`
//! - `POST /durable-in-tx`: `exec-durable` inside BEGIN/ROLLBACK; answers
//!   `ok` or `err`
//! - `POST /durable-commit`: BEGIN, adds 1 to `durable` with `exec`, then
//!   `exec-durable("COMMIT")`; answers the new value (503 with the error)
//! - `GET /cookies`: answers `cookies` with two `Set-Cookie` headers (the
//!   first with a comma in its `Expires`) and an `x-one: 1`, through
//!   `http::respond_headers`
//! - `POST /caller`: the `x-wasmkeep-caller` header, or `none`
//! - `GET /header?name=N`: the request's header N, or `none`
//! - WebSockets: accepted (refused with "go away" when the path contains
//!   `refuse`); text is echoed; `POST /ws-closed` counts close callbacks;
//!   `GET /opened`: the path of the last socket opened, or `none`
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
//! - `GET /fetch-raw?url=U` (U percent-encoded): outbound GET through
//!   `wasi:http` itself, answers `STATUS`, then `headers: NAMES` and
//!   `trailers: NAMES` (each the answer's names, lowercase, sorted,
//!   comma-separated), then `body: BODY`; or `error: ...`
//! - `GET /fetch-all?u=I&u=...` (each I percent-encoded `N URL`): those
//!   requests (a GET, or a POST of N bytes when N > 0) with
//!   `http::fetch_all`, one answer per line as `/fetch` words it; with
//!   `seq=1`, one `http::fetch` after the other instead
//! - `GET /env?name=N`: `env.get(N)`, and whether WASI saw N
//! - `POST /fields?n=N&bytes=B`: holds N `wasi:http` `fields` of one
//!   B-byte header each, then drops them; answers `held N`, or `err:E` for
//!   the first one refused (past the store's table, the call traps)

use wasmkeep_sdk::actors::{self, CallError};
use wasmkeep_sdk::http::{self, IncomingRequest, Method, ResponseOutparam};
use wasmkeep_sdk::{Actor, Value, alarms, calls, env, now_ms, params, storage};

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

/// `get-alarm` as a stored value: -1 for none.
fn alarm_value() -> i64 {
    alarms::get_alarm().map_or(-1, |at| i64::try_from(at).unwrap_or(i64::MAX))
}

/// The `POST /on-alarm` modes, inside `on-alarm` (`at`: the time given).
fn on_alarm_does(mode: i64, at: u64) {
    set("observed_before", alarm_value());
    match mode {
        1 => alarms::set_alarm(at),
        2 => alarms::set_alarm(now_ms() + 3_600_000),
        3 => {
            alarms::set_alarm(at);
            alarms::clear_alarm();
        }
        4 => {
            let _ = storage::exec("BEGIN", &[]);
            alarms::set_alarm(at);
            let _ = storage::exec("ROLLBACK", &[]);
        }
        5 => {
            set("observed_after", alarm_value());
            let _ = storage::exec("BEGIN", &[]);
            alarms::set_alarm(at);
            return; // the host rolls the open transaction back
        }
        6 => {
            let before = alarms::get_alarm();
            alarms::set_alarm(now_ms() + 2_000);
            match before {
                Some(before) => alarms::set_alarm(before),
                None => alarms::clear_alarm(),
            }
        }
        _ => {}
    }
    set("observed_after", alarm_value());
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
            "CREATE TABLE IF NOT EXISTS opened (n INTEGER PRIMARY KEY, path TEXT NOT NULL)",
            "INSERT INTO inits VALUES (1)",
        ] {
            storage::exec(sql, &[]).map_err(|e| e.to_string())?;
        }
        // Actors named alarm-on-init... set an alarm from on-init, 1.5 s out.
        if actors::self_address().name.starts_with("alarm-on-init") && alarms::get_alarm().is_none()
        {
            alarms::set_alarm(now_ms() + 1_500);
        }
        Ok(())
    }

    /// Accepts every WebSocket and echoes text.
    fn on_websocket_open(conn: u64, path: String) -> Result<(), String> {
        if path.contains("refuse") {
            return Err("go away".into());
        }
        if path == "/slow-close" {
            // The writer starts after this hook: fill all 256 frame permits
            // deterministically, without relying on OS TCP buffer sizes.
            let msg = wasmkeep_sdk::Message::Text("data".into());
            for _ in 0..256 {
                wasmkeep_sdk::websockets::send(conn, &msg).unwrap();
            }
            assert!(wasmkeep_sdk::websockets::send(conn, &msg).is_err());
            wasmkeep_sdk::websockets::close(conn, 4802, "slow");
        }
        let _ = storage::exec("INSERT INTO opened (path) VALUES (?1)", &params![path]);
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
        storage::exec("INSERT INTO fired VALUES (?1)", &params![now]).map_err(|e| e.to_string())?;
        let left = get("on_alarm_left");
        if left > 0 {
            set("on_alarm_left", left - 1);
            on_alarm_does(
                get("on_alarm_do"),
                u64::try_from(get("on_alarm_at")).unwrap_or(0),
            );
        }
        if count("fired") < get("repeat") {
            alarms::set_alarm(now_ms() + 20);
        }
        Ok(())
    }
}

/// A call's result: `STATUS:BODY` (trimmed), or the error.
fn answer(result: Result<actors::Response, CallError>) -> String {
    match result {
        Ok(r) => format!("{}:{}", r.status, String::from_utf8_lossy(&r.body).trim()),
        Err(CallError::Reentrant) => "reentrant".into(),
        Err(CallError::InvalidAddress(m)) => format!("invalid-address:{m}"),
        Err(CallError::Failed(m)) => format!("failed:{m}"),
        Err(CallError::Timeout) => "timeout".into(),
        Err(CallError::TooLarge) => "too-large".into(),
    }
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() + 1 {
            if let Some(v) = s
                .get(i + 1..i + 3)
                .and_then(|h| u8::from_str_radix(h, 16).ok())
            {
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

/// A GET of `url` with `wasi:http` alone, as `/fetch-raw` words it.
fn fetch_raw(url: &str) -> Result<String, String> {
    use wasmkeep_sdk::wasi::http::{outgoing_handler, types};
    let (scheme, rest) = url.split_once("://").ok_or("no scheme")?;
    let (authority, path) = rest.find('/').map_or((rest, "/"), |i| rest.split_at(i));
    let request = types::OutgoingRequest::new(types::Fields::new());
    let scheme = if scheme == "https" {
        types::Scheme::Https
    } else {
        types::Scheme::Http
    };
    request.set_scheme(Some(&scheme)).map_err(|()| "scheme")?;
    request
        .set_authority(Some(authority))
        .map_err(|()| "authority")?;
    request
        .set_path_with_query(Some(path))
        .map_err(|()| "path")?;
    let body = request.body().map_err(|()| "body")?;
    types::OutgoingBody::finish(body, None).map_err(|e| format!("{e:?}"))?;
    let pending = outgoing_handler::handle(request, None).map_err(|e| format!("{e:?}"))?;
    pending.subscribe().block();
    let response = pending
        .get()
        .ok_or("no answer")?
        .map_err(|()| "taken")?
        .map_err(|e| format!("{e:?}"))?;
    let names = |fields: &types::Fields| {
        let mut names: Vec<String> = fields
            .entries()
            .into_iter()
            .map(|(k, _)| k.to_ascii_lowercase())
            .collect();
        names.sort();
        names.dedup();
        names.join(",")
    };
    let headers = names(&response.headers());
    let status = response.status();
    let incoming = response.consume().map_err(|()| "consume")?;
    let mut bytes = Vec::new();
    {
        let stream = incoming.stream().map_err(|()| "stream")?;
        loop {
            match stream.blocking_read(65536) {
                Ok(chunk) => bytes.extend_from_slice(&chunk),
                Err(wasmkeep_sdk::wasi::io::streams::StreamError::Closed) => break,
                Err(e) => return Err(format!("{e:?}")),
            }
        }
    }
    let trailers = types::IncomingBody::finish(incoming);
    trailers.subscribe().block();
    let trailers = match trailers.get() {
        Some(Ok(Ok(Some(t)))) => names(&t),
        Some(Ok(Ok(None))) => String::new(),
        other => return Err(format!("trailers: {other:?}")),
    };
    Ok(format!(
        "{status}\nheaders: {headers}\ntrailers: {trailers}\nbody: {}",
        String::from_utf8_lossy(&bytes)
    ))
}

fn query_param(path: &str, key: &str) -> Option<String> {
    let query = path.split_once('?')?.1;
    query
        .split('&')
        .find_map(|pair| match pair.split_once('=') {
            Some((k, v)) if k == key => Some(v.to_owned()),
            None if pair == key => Some(String::new()),
            _ => None,
        })
}

impl http::Guest for TestActor {
    fn handle(request: IncomingRequest, out: ResponseOutparam) {
        let full = request.path_with_query().unwrap_or_default();
        let path = full.split('?').next().unwrap_or("/").to_owned();
        let num = |k: &str| {
            query_param(&full, k)
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0)
        };
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
                let tcp =
                    std::net::TcpStream::connect(("127.0.0.1", u16::try_from(port).unwrap_or(1)));
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
            (Method::Post, "/on-alarm") => {
                let mode = match query_param(&full, "do").as_deref() {
                    Some("again") => 1,
                    Some("set") => 2,
                    Some("clear") => 3,
                    Some("rollback") => 4,
                    Some("open") => 5,
                    Some("restore") => 6,
                    _ => 0,
                };
                set("on_alarm_do", mode);
                set("on_alarm_at", num("at"));
                set("on_alarm_left", num("n").max(1));
                "ok".to_owned()
            }
            (Method::Get, "/observed") => {
                let show = |v: i64| {
                    if v < 0 {
                        "none".to_owned()
                    } else {
                        v.to_string()
                    }
                };
                format!(
                    "{} {}",
                    show(get("observed_before")),
                    show(get("observed_after"))
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
            // A committed alarm, then a trap: the event ends trapped with
            // its alarm write committed (ADR 0034, section 9).
            (Method::Post, "/alarm-then-trap") => {
                let at = query_param(&full, "at")
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);
                alarms::set_alarm(at);
                panic!("trap requested after setting the alarm")
            }
            (Method::Post, "/call") => {
                let to = query_param(&full, "to")
                    .map(|t| percent_decode(&t))
                    .unwrap_or_default();
                let path =
                    query_param(&full, "path").map_or("/fired".to_owned(), |p| percent_decode(&p));
                let request = actors::Request {
                    method: "POST".into(),
                    path,
                    headers: vec![],
                    body: vec![],
                };
                let call = || answer(actors::call(&to, &request));
                (0..num("times").max(1))
                    .map(|_| call())
                    .collect::<Vec<_>>()
                    .join(" | ")
            }
            (Method::Post, "/call-many") => {
                let to = query_param(&full, "to")
                    .map(|t| percent_decode(&t))
                    .unwrap_or_default();
                let path =
                    query_param(&full, "path").map_or("/fired".to_owned(), |p| percent_decode(&p));
                let entries: Vec<(String, actors::Request)> = to
                    .split(',')
                    .filter(|a| !a.is_empty())
                    .map(|a| {
                        let request = actors::Request {
                            method: "POST".into(),
                            path: path.clone(),
                            headers: vec![],
                            body: vec![],
                        };
                        (a.to_owned(), request)
                    })
                    .collect();
                calls::call_many(&entries)
                    .into_iter()
                    .map(answer)
                    .collect::<Vec<_>>()
                    .join(" | ")
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
            // One event writing `kb` KiB at once: over a batched segment's
            // cap when large (ADR 0034, section 2).
            // `random`: bytes that do not compress, so a segment's encoded
            // size is about the write's.
            (Method::Post, "/big") if query_param(&full, "random").is_some() => {
                // In rows of 256 KiB, under the value limit, in one event.
                let mut answer = "big".to_owned();
                for i in 0..(num("kb") / 256).max(1) {
                    if let Err(e) = storage::exec(
                        "INSERT INTO config (key, value) VALUES (?, randomblob(262144))
                         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                        &[Value::Text(format!("big{i}"))],
                    ) {
                        answer = format!("err:{:?}", e.kind);
                        break;
                    }
                }
                answer
            }
            (Method::Post, "/big") => {
                let blob = vec![0x41; usize::try_from(num("kb")).unwrap_or(0) * 1024];
                match storage::exec(
                    "INSERT INTO config (key, value) VALUES ('big', ?)
                     ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                    &[Value::Blob(blob)],
                ) {
                    Ok(_) => "big".to_owned(),
                    Err(e) => format!("err:{:?}", e.kind),
                }
            }
            (Method::Post, "/flush") => {
                for _ in 0..num("n") {
                    let _ = storage::exec(
                        "INSERT INTO config (key, value) VALUES ('plain', 1)
                         ON CONFLICT (key) DO UPDATE SET value = value + 1",
                        &[],
                    );
                }
                if query_param(&full, "alarm").is_some() {
                    alarms::set_alarm(now_ms() + u64::try_from(num("alarm")).unwrap_or(0));
                }
                // Most of the wall gone before the flush (ADR 0034, section 4).
                if query_param(&full, "sleep").is_some() {
                    let ns = u64::try_from(num("sleep")).unwrap_or(0) * 1_000_000;
                    wasmkeep_sdk::wasi::clocks::monotonic_clock::subscribe_duration(ns).block();
                }
                let tx = query_param(&full, "tx").is_some();
                if tx {
                    let _ = storage::exec("BEGIN", &[]);
                }
                let mut answers = vec![storage::flush()];
                if query_param(&full, "twice").is_some() {
                    answers.push(storage::flush());
                }
                if tx {
                    let _ = storage::exec("ROLLBACK", &[]);
                }
                let answered = answers
                    .into_iter()
                    .map(|r| match r {
                        Ok(()) => format!("ok:{}", get("plain")),
                        Err(e) => format!("err:{:?}:{}", e.kind, e.message),
                    })
                    .collect::<Vec<_>>()
                    .join(" | ");
                for _ in 0..num("after") {
                    let _ = storage::exec(
                        "INSERT INTO config (key, value) VALUES ('plain', 1)
                         ON CONFLICT (key) DO UPDATE SET value = value + 1",
                        &[],
                    );
                }
                answered
            }
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
                    let _ = wasmkeep_sdk::websockets::send(
                        *c,
                        &wasmkeep_sdk::Message::Text(text.clone()),
                    );
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
            (Method::Get, "/cookies") => {
                let mut headers = wasmkeep_sdk::headers::Headers::new();
                headers.append("Set-Cookie", "a=1; Expires=Wed, 21 Oct 2026 07:28:00 GMT");
                headers.append("x-one", "1");
                headers.append("set-cookie", "b=2");
                http::respond_headers(out, 200, &headers, b"cookies\n");
                return;
            }
            (Method::Get, "/slept") => get("slept").to_string(),
            (Method::Post, "/caller") => request
                .headers()
                .get("x-wasmkeep-caller")
                .first()
                .map_or("none".to_owned(), |v| {
                    String::from_utf8_lossy(v).into_owned()
                }),
            (Method::Get, "/header") => {
                let name = query_param(&full, "name").unwrap_or_default();
                request
                    .headers()
                    .get(&name)
                    .first()
                    .map_or("none".to_owned(), |v| {
                        String::from_utf8_lossy(v).into_owned()
                    })
            }
            (Method::Get, "/fetch") => {
                let url = query_param(&full, "url")
                    .map(|u| percent_decode(&u))
                    .unwrap_or_default();
                let send: usize = query_param(&full, "send")
                    .and_then(|n| n.parse().ok())
                    .unwrap_or(0);
                let body = vec![b'x'; send];
                let method = if send > 0 { "POST" } else { "GET" };
                match wasmkeep_sdk::http::fetch(
                    method,
                    &url,
                    &[],
                    &body,
                    std::time::Duration::from_secs(5),
                    4096,
                ) {
                    Ok(r) => format!("{}:{}", r.status, String::from_utf8_lossy(&r.body)),
                    Err(e) => format!("error: {e}"),
                }
            }
            (Method::Get, "/fetch-raw") => {
                let url = query_param(&full, "url")
                    .map(|u| percent_decode(&u))
                    .unwrap_or_default();
                fetch_raw(&url).unwrap_or_else(|e| format!("error: {e}"))
            }
            (Method::Get, "/fetch-all") => {
                let query = full.split_once('?').map_or("", |(_, q)| q);
                let items: Vec<(usize, String)> = query
                    .split('&')
                    .filter_map(|pair| pair.strip_prefix("u="))
                    .filter_map(|v| {
                        let item = percent_decode(v);
                        let (send, url) = item.split_once(' ')?;
                        Some((send.parse().ok()?, url.to_owned()))
                    })
                    .collect();
                let bodies: Vec<Vec<u8>> = items.iter().map(|(n, _)| vec![b'x'; *n]).collect();
                let requests: Vec<http::FetchRequest<'_>> = items
                    .iter()
                    .zip(&bodies)
                    .map(|((send, url), body)| {
                        let method = if *send > 0 { "POST" } else { "GET" };
                        http::FetchRequest::new(method, url).body(body)
                    })
                    .collect();
                let timeout = std::time::Duration::from_secs(5);
                let results = if query_param(&full, "seq").is_some() {
                    requests
                        .iter()
                        .map(|r| http::fetch(r.method, r.url, r.headers, r.body, timeout, 4096))
                        .collect()
                } else {
                    http::fetch_all(&requests, timeout, 4096)
                };
                results
                    .into_iter()
                    .map(|r| match r {
                        Ok(r) => format!("{}:{}\n", r.status, String::from_utf8_lossy(&r.body)),
                        Err(e) => format!("error: {e}\n"),
                    })
                    .collect::<String>()
            }
            (Method::Post, "/memory") => {
                static MEMORY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                (MEMORY.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1).to_string()
            }
            (Method::Post, "/sql") => {
                let sql =
                    String::from_utf8(http::read_body(&request, 64 * 1024).unwrap_or_default())
                        .unwrap_or_default();
                let blob = query_param(&full, "blob").and_then(|n| n.parse::<usize>().ok());
                let params: Vec<Value> = blob
                    .map(|n| vec![Value::Blob(vec![0x41; n])])
                    .unwrap_or_default();
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
            (Method::Get, "/opened") => {
                storage::query("SELECT path FROM opened ORDER BY n DESC LIMIT 1", &[])
                    .ok()
                    .and_then(|r| r.get(0, "path").and_then(|v| v.as_str().map(str::to_owned)))
                    .unwrap_or_else(|| "none".into())
            }
            (Method::Get, "/env") => {
                // `env.get`, and whether the same name leaked into the WASI
                // environment (it must not).
                let name = query_param(&full, "name").unwrap_or_default();
                let wasi = if std::env::var_os(&name).is_some() {
                    "set"
                } else {
                    "unset"
                };
                match env::get(&name) {
                    Some(v) => format!("{v} wasi={wasi}"),
                    None => format!("(none) wasi={wasi}"),
                }
            }
            (Method::Post, "/delete-durable") if query_param(&full, "retry").is_some() => {
                let tag = |r: Result<(), storage::SqlError>| if r.is_ok() { "ok" } else { "err" };
                let first = tag(storage::delete_self_durable());
                let second = tag(storage::delete_self_durable());
                format!("first={first} second={second}")
            }
            // A durable write that fails, then a plain one in the same
            // event: a strict event runs on to its end (ADR 0034, section 8).
            (Method::Post, "/durable-then-plain") => {
                let durable = storage::exec_durable(
                    "INSERT INTO config (key, value) VALUES ('durable', 1)
                     ON CONFLICT (key) DO UPDATE SET value = value + 1",
                    &[],
                );
                let plain = storage::exec(
                    "INSERT INTO config (key, value) VALUES ('plain', 1)
                     ON CONFLICT (key) DO UPDATE SET value = value + 1",
                    &[],
                );
                let word = |ok: bool| if ok { "ok" } else { "err" };
                format!(
                    "durable={} plain={}",
                    word(durable.is_ok()),
                    word(plain.is_ok())
                )
            }
            (Method::Post, "/delete-durable") if query_param(&full, "alarm").is_some() => {
                alarms::set_alarm(now_ms() + u64::try_from(num("alarm")).unwrap_or(0));
                match storage::delete_self_durable() {
                    Ok(()) => "deleted".to_owned(),
                    Err(e) => format!("err:{:?}:{}", e.kind, e.message),
                }
            }
            (Method::Post, "/delete-durable") => match storage::delete_self_durable() {
                Ok(()) if query_param(&full, "then").as_deref() == Some("sql") => {
                    match storage::query("SELECT 1", &[]) {
                        Ok(_) => "ok".to_owned(),
                        Err(e) => format!("err:{:?}", e.kind),
                    }
                }
                Ok(()) => "deleted".to_owned(),
                Err(e) => {
                    http::respond(out, 503, "text/plain", format!("{e:?}\n").as_bytes());
                    return;
                }
            },
            (Method::Post, "/delete") => {
                storage::delete_self();
                "ok".to_owned()
            }
            (Method::Post, "/fields") => hold_fields(
                usize::try_from(num("n")).unwrap_or(0),
                usize::try_from(num("bytes")).unwrap_or(0),
            ),
            _ => {
                http::respond(out, 404, "text/plain", b"no route\n");
                return;
            }
        };
        http::respond(out, 200, "text/plain", format!("{body}\n").as_bytes());
    }
}

/// `POST /fields`: N `fields` held at once, then dropped.
fn hold_fields(n: usize, bytes: usize) -> String {
    let value = vec![b'v'; bytes];
    let mut held = Vec::with_capacity(n);
    for _ in 0..n {
        match http::Fields::from_list(&[("x-fill".to_owned(), value.clone())]) {
            Ok(fields) => held.push(fields),
            Err(e) => return format!("err:{e:?}"),
        }
    }
    format!("held {}", held.len())
}

wasmkeep_sdk::export!(TestActor);
