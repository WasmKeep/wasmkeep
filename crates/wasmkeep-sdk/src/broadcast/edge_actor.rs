use super::{Edge, HostSockets, Position, Push};
use crate::{Actor, Message, http, storage, wire};
thread_local! { static EDGE: std::cell::RefCell<Edge> = std::cell::RefCell::new(Edge::default()); }
/// Internal edge class for one fixed full-range edge. No outbound calls.
#[derive(Debug)]
pub struct EdgeActor;
fn owner() -> Result<Option<String>, String> {
    let rows = storage::query("SELECT config FROM wasmkeep_edge WHERE id=1", &[])
        .map_err(|e| e.to_string())?;
    let Some(row) = rows.values.first() else {
        return Ok(None);
    };
    let j = crate::json::parse_unique(row[0].as_str().ok_or("invalid edge config")?)
        .ok_or("invalid edge config")?;
    Ok(Some(super::edge::string(&j, "room")?.into()))
}
impl Actor for EdgeActor {
    fn on_init() -> Result<(), String> {
        storage::exec("CREATE TABLE IF NOT EXISTS wasmkeep_edge(id INTEGER PRIMARY KEY CHECK(id=1),config TEXT NOT NULL)",&[]).map(|_|()).map_err(|e|e.to_string())
    }
    fn on_websocket_open(id: u64, path: String) -> Result<(), String> {
        if owner()?.is_none() {
            storage::delete_self();
            return Err("unknown".into());
        }
        let point = wire::query_param(&path, "p").ok_or("moved")?;
        if point.len() != 16
            || u64::from_str_radix(point, 16).is_err()
            || wire::query_param(&path, "c").is_none()
        {
            return Err("moved".into());
        }
        let p = match (wire::query_param(&path, "e"), wire::query_param(&path, "n")) {
            (None, None) => None,
            (Some(e), Some(n)) => Some(Position {
                e: e.parse().map_err(|_| "invalid epoch")?,
                n: n.parse().map_err(|_| "invalid number")?,
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
            if ids.len() > 500 || !edge.open(id, p, &mut HostSockets) {
                Err("full".into())
            } else {
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
            let old = owner()?;
            if path == "/wasmkeep-edge/v1/init" {
                if old.as_ref().is_some_and(|o| o != &caller) {
                    return Err("forbidden".into());
                }
                let j = crate::json::parse_unique(body).ok_or("invalid config")?;
                if super::edge::string(&j, "room")? != caller
                    || super::edge::string(&j, "lo")? != "0000000000000000"
                    || j.get("hi").is_some()
                    || super::edge::string(&j, "state")? != "active"
                {
                    return Err("invalid config".into());
                }
                if old.is_none() {
                    storage::exec_durable(
                        "INSERT INTO wasmkeep_edge(id,config) VALUES(1,?1)",
                        &[body.into()],
                    )
                    .map_err(|e| e.to_string())?;
                }
                return Ok("{}".into());
            }
            if old.as_ref() != Some(&caller) {
                return Err("forbidden".into());
            }
            if path != "/wasmkeep-edge/v1/push" {
                return Err("invalid path".into());
            }
            let push = Push::decode(body)?;
            Ok(EDGE.with_borrow_mut(|e| {
                let ack = e.push(&push, &mut HostSockets);
                e.adopt(&crate::websockets::connections());
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
