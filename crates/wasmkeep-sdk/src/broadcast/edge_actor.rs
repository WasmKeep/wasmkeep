use super::{Edge, HostSockets, Position, Push};
use crate::{Actor, Message, http, storage, wire};
thread_local! { static EDGE: std::cell::RefCell<Edge> = std::cell::RefCell::new(Edge::default()); }
/// Internal edge class with durable ownership ranges. No outbound calls.
#[derive(Debug)]
pub struct EdgeActor;
fn decimal(s: &str) -> Result<u64, String> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err("invalid position".into());
    }
    let n: u64 = s.parse().map_err(|_| "invalid position")?;
    if n > 9007199254740991 {
        return Err("invalid position".into());
    }
    Ok(n)
}
#[derive(Debug)]
struct Config {
    room: String,
    relay_prefix: Option<String>,
    lo: u64,
    hi: Option<u64>,
    drain: bool,
    cap: u64,
    head: Option<Position>,
}
fn config(body: &str) -> Result<Config, String> {
    let j = crate::json::parse_unique(body).ok_or("invalid config")?;
    let hex = |key: &str| -> Result<u64, String> {
        let v = super::edge::string(&j, key)?;
        if v.len() != 16
            || !v
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("invalid range".into());
        }
        u64::from_str_radix(v, 16).map_err(|_| "invalid range".into())
    };
    let lo = hex("lo")?;
    let hi = if j.get("hi").is_some() {
        Some(hex("hi")?)
    } else {
        None
    };
    let cap = if j.get("cap").is_some() {
        super::edge::number(&j, "cap")?
    } else {
        500
    };
    if hi.is_some_and(|hi| hi <= lo) || cap == 0 || cap > 500 {
        return Err("invalid config".into());
    }
    let drain = match super::edge::string(&j, "state")? {
        "active" => false,
        "draining" => true,
        _ => return Err("invalid state".into()),
    };
    let head = if j.get("e").is_some() || j.get("n").is_some() {
        Some(Position {
            e: super::edge::number(&j, "e")?,
            n: super::edge::number(&j, "n")?,
        })
    } else {
        None
    };
    Ok(Config {
        relay_prefix: j
            .get("relayPrefix")
            .map(|_| super::edge::string(&j, "relayPrefix").map(String::from))
            .transpose()?,
        room: super::edge::string(&j, "room")?.into(),
        lo,
        hi,
        drain,
        cap,
        head,
    })
}
fn load() -> Result<Option<Config>, String> {
    let rows = storage::query("SELECT config FROM wasmkeep_edge WHERE id=1", &[])
        .map_err(|e| e.to_string())?;
    rows.values
        .first()
        .map(|r| config(r[0].as_str().ok_or("invalid config")?))
        .transpose()
}
impl Actor for EdgeActor {
    fn on_init() -> Result<(), String> {
        storage::exec("CREATE TABLE IF NOT EXISTS wasmkeep_edge(id INTEGER PRIMARY KEY CHECK(id=1),config TEXT NOT NULL)",&[]).map(|_|()).map_err(|e|e.to_string())
    }
    fn on_websocket_open(id: u64, path: String) -> Result<(), String> {
        let Some(config) = load()? else {
            EDGE.with_borrow_mut(|e| e.refuse(0));
            storage::delete_self();
            return Err("unknown".into());
        };
        let point = wire::query_param(&path, "p").ok_or("moved")?;
        if point.len() != 16
            || u64::from_str_radix(point, 16).is_err()
            || wire::query_param(&path, "c").is_none()
        {
            EDGE.with_borrow_mut(|e| e.refuse(1));
            return Err("moved".into());
        }
        let point = u64::from_str_radix(point, 16).map_err(|_| "moved")?;
        if config.drain || point < config.lo || config.hi.is_some_and(|hi| point >= hi) {
            EDGE.with_borrow_mut(|e| e.refuse(1));
            return Err("moved".into());
        }
        let p = match (wire::query_param(&path, "e"), wire::query_param(&path, "n")) {
            (None, None) => None,
            (Some(e), Some(n)) => Some(Position {
                e: decimal(e)?,
                n: decimal(n)?,
            }),
            _ => return Err("invalid position".into()),
        };
        EDGE.with_borrow_mut(|edge| {
            let ids = crate::websockets::connections();
            edge.adopt(
                &ids.iter()
                    .copied()
                    .filter(|other| *other != id)
                    .collect::<Vec<_>>(),
            );
            if ids.len() as u64 > config.cap {
                edge.refuse(2);
                return Err("full".into());
            }
            if !edge.open(id, p, &mut HostSockets) {
                Err("full".into())
            } else {
                edge.set_point(id, point);
                Ok(())
            }
        })
    }
    fn on_websocket_message(id: u64, msg: Message) {
        if let Message::Text(s) = msg
            && let Some(j) = crate::json::parse(&s)
            && super::edge::string(&j, "t") == Ok("ping")
        {
            EDGE.with_borrow_mut(|e| e.ping(id, &mut HostSockets));
        }
    }
    fn on_websocket_close(id: u64, _: u16, _: String) {
        EDGE.with_borrow_mut(|e| e.forget(id));
    }
}
impl http::Guest for EdgeActor {
    fn handle(req: http::IncomingRequest, out: http::ResponseOutparam) {
        let result = (|| -> Result<String, String> {
            let caller = wire::caller(&req).ok_or("forbidden")?;
            if !matches!(req.method(), http::Method::Post) {
                return Err("invalid method".into());
            }
            let path = req.path_with_query().unwrap_or_default();
            let bytes = http::read_body(&req, 1024 * 1024).ok_or("invalid body")?;
            let body = std::str::from_utf8(&bytes).map_err(|_| "invalid UTF-8")?;
            let old = load()?;
            if path == "/wasmkeep-edge/v1/end" {
                if old.as_ref().is_some_and(|c| c.room != caller) {
                    return Err("forbidden".into());
                }
                if old.as_ref().is_some_and(|c| {
                    !c.drain || (!crate::websockets::connections().is_empty() && body != "force")
                }) {
                    return Err("edge still draining".into());
                }
                storage::delete_self();
                return Ok("{}".into());
            }
            if ["init", "range", "drain"]
                .iter()
                .any(|op| path == format!("/wasmkeep-edge/v1/{op}"))
            {
                if old.as_ref().is_some_and(|c| c.room != caller) {
                    return Err("forbidden".into());
                }
                let next = config(body)?;
                if next.room != caller {
                    return Err("forbidden".into());
                }
                let init = path.ends_with("/init");
                if !init && old.is_none() {
                    return Err("invalid unknown edge".into());
                }
                let active = if !init || old.is_none() {
                    storage::exec_durable("INSERT INTO wasmkeep_edge(id,config) VALUES(1,?1) ON CONFLICT(id) DO UPDATE SET config=excluded.config",&[body.into()]).map_err(|e|e.to_string())?;
                    next
                } else {
                    old.ok_or("invalid config")?
                };
                EDGE.with_borrow_mut(|e| {
                    if e.head.is_none() {
                        e.head = active.head;
                        if let Some(h) = active.head {
                            e.floor = h.n;
                        }
                    }
                    if !init {
                        e.range(
                            active.lo,
                            active.hi,
                            active.drain,
                            &crate::websockets::connections(),
                            &mut HostSockets,
                        );
                    }
                });
                return Ok(format!(
                    "{{\"sockets\":{}}}",
                    crate::websockets::connections().len()
                ));
            }
            if !old.as_ref().is_some_and(|c| {
                c.room == caller
                    || c.relay_prefix.as_ref().is_some_and(|p| {
                        caller
                            .strip_prefix(p)
                            .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
                    })
            }) {
                return Err("forbidden".into());
            }
            if path != "/wasmkeep-edge/v1/push" {
                return Err("invalid path".into());
            }
            let push = Push::decode(body)?;
            Ok(EDGE.with_borrow_mut(|e| {
                if e.head.is_none() {
                    e.head = Some(Position {
                        e: push.e,
                        n: push.head,
                    });
                    e.floor = push.head;
                }
                e.adopt(&crate::websockets::connections());
                let ack = e.push(&push, &mut HostSockets);
                ack.encode()
            }))
        })();
        match result {
            Ok(body) => http::respond(out, 200, "application/json", body.as_bytes()),
            Err(e) => http::respond(
                out,
                if e == "forbidden" { 403 } else { 400 },
                "text/plain",
                e.as_bytes(),
            ),
        }
    }
}
