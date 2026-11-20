use super::*;
use crate::wire::{self, Reader, Writer};
const PREFIX: &str = "/wasmkeep-room/v1/";
const POKE: &str = "/wasmkeep-pusher/v1/poke";
fn bad() -> String {
    "invalid room protocol".into()
}
fn number(r: &mut Reader<'_>) -> Result<u64, String> {
    r.u64().ok_or_else(bad)
}
fn text(r: &mut Reader<'_>) -> Result<String, String> {
    String::from_utf8(r.bytes().ok_or_else(bad)?.to_vec()).map_err(|_| bad())
}
fn position(r: &mut Reader<'_>) -> Result<Position, String> {
    Ok(Position {
        e: number(r)?,
        n: number(r)?,
    })
}
fn put(w: &mut Writer, p: Position) {
    w.u64(p.e).u64(p.n);
}
fn end(r: &Reader<'_>) -> Result<(), String> {
    if r.is_empty() { Ok(()) } else { Err(bad()) }
}
fn count(r: &mut Reader<'_>, max: u64) -> Result<usize, String> {
    let n = number(r)?;
    if n > max {
        return Err(bad());
    }
    usize::try_from(n).map_err(|_| bad())
}
fn optional(r: &mut Reader<'_>) -> Result<bool, String> {
    match number(r)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(bad()),
    }
}
/// `adopt` (section 13): a 0.6.0 room's scale rows, read, handed over and
/// retired at its pusher's request. The only time a room calls anyone.
#[cfg(feature = "actor")]
fn adopt(r: &mut Reader<'_>, w: &mut Writer, pusher: &str) -> Result<(), String> {
    let legacy = |sql: &str| -> Option<Vec<u8>> {
        let rows = crate::storage::query(sql, &[]).ok()?;
        match rows.values.first()?.first()? {
            crate::Value::Blob(b) => Some(b.clone()),
            crate::Value::Text(s) => Some(s.clone().into_bytes()),
            _ => None,
        }
    };
    match count(r, 2)? {
        0 => {
            end(r)?;
            let data = legacy("SELECT data FROM wasmkeep_room_scale LIMIT 1");
            let op = data
                .as_ref()
                .and_then(|_| legacy("SELECT args FROM wasmkeep_room_ops LIMIT 1"));
            for v in [data, op] {
                w.u64(u64::from(v.is_some()));
                if let Some(v) = v {
                    w.bytes(&v);
                }
            }
            Ok(())
        }
        2 => {
            end(r)?;
            let sql = |s: &str, durable: bool| {
                if durable {
                    crate::storage::exec_durable(s, &[])
                } else {
                    crate::storage::exec(s, &[])
                }
                .map(|_| ())
                .map_err(|e| e.to_string())
            };
            sql("BEGIN", false)?;
            let result = sql("DROP TABLE IF EXISTS wasmkeep_room_ops", false)
                .and_then(|_| sql("DROP TABLE IF EXISTS wasmkeep_room_scale", false))
                .and_then(|_| sql("COMMIT", true));
            if result.is_err() {
                let _ = sql("ROLLBACK", false);
            }
            result
        }
        _ => {
            let directory = text(r)?;
            let n = count(r, 16)?;
            let edges = (0..n).map(|_| text(r)).collect::<Result<Vec<_>, _>>()?;
            end(r)?;
            // This room is the controller the map and the edges still accept.
            if !directory.is_empty() {
                let mut t = Writer::default();
                t.bytes(pusher.as_bytes());
                let reply = wire::call(
                    &directory,
                    "POST",
                    "/wasmkeep-directory/v1/transfer".into(),
                    vec![],
                    t.finish(),
                )?;
                if reply.status != 200 && reply.status != 404 {
                    return Err(format!("directory transfer answered {}", reply.status));
                }
            }
            let mut body = "{\"room\":".to_owned();
            crate::json::write_str(&mut body, pusher);
            body.push('}');
            let requests: Vec<_> = edges
                .into_iter()
                .map(|e| {
                    (
                        e,
                        crate::actors::Request {
                            method: "POST".into(),
                            path: "/wasmkeep-edge/v1/handover".into(),
                            headers: vec![],
                            body: body.clone().into_bytes(),
                        },
                    )
                })
                .collect();
            if crate::calls::call_many(&requests)
                .into_iter()
                .any(|r| r.map_or(true, |r| r.status != 200))
            {
                return Err("edge handover failed".into());
            }
            Ok(())
        }
    }
}
/// The room's operations, as its handler serves them. `pull` and `adopt`
/// answer the room's pusher only.
#[cfg(feature = "actor")]
pub(super) fn serve<S: Store>(
    core: &mut Core<S>,
    op: &str,
    body: &[u8],
    now: u64,
    caller: &str,
    pusher: &str,
) -> Result<Vec<u8>, String> {
    let mut r = Reader::new(body);
    let mut w = Writer::default();
    if matches!(op, "pull" | "adopt") && caller != pusher {
        return Err("forbidden: the room's pusher only".into());
    }
    match op {
        "pull" => {
            let after = if optional(&mut r)? {
                Some(position(&mut r)?)
            } else {
                None
            };
            let budget = count(&mut r, 1024 * 1024)?;
            let idle = optional(&mut r)?;
            end(&r)?;
            let p = core.pull(after, budget, idle, now)?;
            w.u64(p.e)
                .u64(p.base)
                .u64(p.first)
                .u64(p.head)
                .u64(u64::from(p.recover))
                .u64(p.messages.len() as u64);
            for (m, base) in p.messages {
                put(&mut w, m.position);
                w.u64(base).u64(m.at).u64(u64::from(m.id.is_some()));
                if let Some(id) = m.id {
                    w.bytes(id.as_bytes());
                }
                w.bytes(m.body.as_bytes());
            }
        }
        "adopt" => adopt(&mut r, &mut w, pusher)?,
        "publish" => {
            let len = count(&mut r, 64)?;
            let mut messages = Vec::new();
            for _ in 0..len {
                let id = if optional(&mut r)? {
                    Some(text(&mut r)?)
                } else {
                    None
                };
                messages.push(Publish {
                    id,
                    body: text(&mut r)?,
                });
            }
            end(&r)?;
            let result = core.publish(&messages, now)?;
            w.u64(result.len() as u64);
            for p in result {
                put(&mut w, p);
            }
            // Section 13: ask the front to poke the pusher only when it went idle.
            w.u64(u64::from(core.poke(now)));
        }
        "history" => {
            let after = if optional(&mut r)? {
                Some(position(&mut r)?)
            } else {
                None
            };
            let limit = count(&mut r, 2000)?;
            let last = if optional(&mut r)? {
                Some(count(&mut r, 2000)?)
            } else {
                None
            };
            end(&r)?;
            let h = core.history(after, limit, last)?;
            put(&mut w, h.head);
            w.u64(u64::from(h.lost.is_some()));
            if let Some(e) = h.lost {
                w.u64(e.e).u64(e.base);
            }
            w.u64(u64::from(h.reset.is_some()));
            if let Some(p) = h.reset {
                put(&mut w, p);
            }
            w.u64(u64::from(h.more)).u64(h.messages.len() as u64);
            for m in h.messages {
                put(&mut w, m.position);
                w.u64(m.at).u64(u64::from(m.id.is_some()));
                if let Some(id) = m.id {
                    w.bytes(id.as_bytes());
                }
                w.bytes(m.body.as_bytes());
            }
        }
        _ => return Err(bad()),
    }
    Ok(w.finish())
}
fn encode_client(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}
/// The pusher of a room address: `room/lobby` has `pusher/lobby` (the class
/// swapped, the name kept). Use [`Client::with_pusher`] for another layout.
pub fn pusher_of(room: &str) -> String {
    format!(
        "pusher/{}",
        room.split_once('/').map_or(room, |(_, name)| name)
    )
}
/// Front-side helper: publish and gap requests go over HTTP to the front,
/// which authorizes them and calls this client. Never call it from an edge.
#[derive(Debug)]
pub struct Client {
    address: String,
    pusher: String,
}
impl Client {
    pub fn new(address: impl Into<String>) -> Self {
        let address = address.into();
        Self {
            pusher: pusher_of(&address),
            address,
        }
    }
    /// The pusher to poke, when it is not [`pusher_of`] the room.
    pub fn with_pusher(mut self, pusher: impl Into<String>) -> Self {
        self.pusher = pusher.into();
        self
    }
    fn call(&self, op: &str, body: Vec<u8>) -> Result<Vec<u8>, String> {
        let r = wire::call(&self.address, "POST", format!("{PREFIX}{op}"), vec![], body)?;
        if r.status != 200 {
            return Err(format!("room answered {}", r.status));
        }
        Ok(r.body)
    }
    /// Authorize the client first. Directory lookup and the pusher's
    /// alarm-only poke share one batch; a cold directory is retried once
    /// after that batch.
    pub fn join(
        &self,
        directory: &str,
        client: &str,
        position: Option<Position>,
    ) -> Result<crate::actors::Response, String> {
        let point = crate::directory::point(client.as_bytes());
        let mut w = Writer::default();
        w.u64(point);
        let lookup = crate::actors::Request {
            method: "POST".into(),
            path: "/wasmkeep-directory/v1/lookup".into(),
            headers: vec![],
            body: w.finish(),
        };
        let poke = crate::actors::Request {
            method: "POST".into(),
            path: POKE.into(),
            headers: vec![],
            body: vec![],
        };
        // The poke goes to the pusher (section 13), which creates the group.
        let results = crate::calls::call_many(&[
            (directory.into(), lookup.clone()),
            (self.pusher.clone(), poke),
        ]);
        let mut reply = results
            .into_iter()
            .next()
            .ok_or("missing lookup")?
            .map_err(|e| format!("join lookup: {e:?}"))?;
        if reply.status == 404 {
            reply = crate::actors::call(directory, &lookup)
                .map_err(|e| format!("join lookup: {e:?}"))?;
        }
        if reply.status != 200 {
            return Err(format!("join lookup answered {}", reply.status));
        }
        let mut r = Reader::new(&reply.body);
        number(&mut r)?;
        let entry = crate::directory::decode_entry(&mut r).ok_or_else(bad)?;
        end(&r)?;
        let (class, name) = entry.target.split_once('/').ok_or_else(bad)?;
        let mut path = format!("/?c={}&p={point:016x}", encode_client(client));
        if let Some(p) = position {
            if p.e > 9007199254740991 || p.n > 9007199254740991 {
                return Err("invalid browser position".into());
            }
            path.push_str(&format!("&e={}&n={}", p.e, p.n));
        }
        let r = crate::front::hand_off(class, name, &path);
        Ok(crate::actors::Response {
            status: r.status,
            headers: r.headers,
            body: r.body,
        })
    }
    pub fn publish(&self, messages: &[Publish]) -> Result<Vec<Position>, String> {
        let mut w = Writer::default();
        w.u64(messages.len() as u64);
        for m in messages {
            w.u64(u64::from(m.id.is_some()));
            if let Some(id) = &m.id {
                w.bytes(id.as_bytes());
            }
            w.bytes(m.body.as_bytes());
        }
        let body = self.call("publish", w.finish())?;
        let mut r = Reader::new(&body);
        let len = count(&mut r, 64)?;
        let result = (0..len)
            .map(|_| position(&mut r))
            .collect::<Result<_, _>>()?;
        // A 0.6.0 room answers no poke field.
        let poke = !r.is_empty() && optional(&mut r)?;
        end(&r)?;
        // ADR 0040, section 13: the pusher went idle and asks for a poke. A
        // failed poke is ignored: the room keeps asking until the pusher pulls.
        if poke {
            let mut w = Writer::default();
            w.u64(1);
            let _ = wire::call(&self.pusher, "POST", POKE.into(), vec![], w.finish());
        }
        Ok(result)
    }
    pub fn history(
        &self,
        after: Option<Position>,
        limit: usize,
        last: Option<usize>,
    ) -> Result<History, String> {
        let mut w = Writer::default();
        w.u64(u64::from(after.is_some()));
        if let Some(p) = after {
            put(&mut w, p);
        }
        w.u64(limit as u64).u64(u64::from(last.is_some()));
        if let Some(n) = last {
            w.u64(n as u64);
        }
        let body = self.call("history", w.finish())?;
        let mut r = Reader::new(&body);
        let head = position(&mut r)?;
        let lost = if optional(&mut r)? {
            Some(Epoch {
                e: number(&mut r)?,
                base: number(&mut r)?,
            })
        } else {
            None
        };
        let reset = if optional(&mut r)? {
            Some(position(&mut r)?)
        } else {
            None
        };
        let more = optional(&mut r)?;
        let len = count(&mut r, 2000)?;
        let mut messages = vec![];
        for _ in 0..len {
            messages.push(Message {
                position: position(&mut r)?,
                at: number(&mut r)?,
                id: if optional(&mut r)? {
                    Some(text(&mut r)?)
                } else {
                    None
                },
                body: text(&mut r)?,
            });
        }
        end(&r)?;
        Ok(History {
            head,
            lost,
            reset,
            more,
            messages,
        })
    }
}
#[cfg(feature = "actor")]
mod actor {
    use super::*;
    thread_local! {
        static CORE: std::cell::RefCell<Core> = std::cell::RefCell::new(Core::new(Host, false));
        static PUSHER_CLASS: std::cell::RefCell<String> = std::cell::RefCell::new("pusher".into());
    }
    /// Default room class (`confirm = false`). For confirmation, delegate your
    /// lifecycle to `handler::init(true)`. Its pusher is `pusher/<name>`
    /// unless `handler::pusher_class` names another class.
    #[derive(Debug)]
    pub struct Room;
    impl crate::Actor for Room {
        fn on_init() -> Result<(), String> {
            handler::init(false)
        }
        fn on_alarm() -> Result<(), String> {
            // A 0.6.0 room's alarm fires once more: rounds are the pusher's now.
            Ok(())
        }
    }
    impl crate::http::Guest for Room {
        fn handle(req: crate::http::IncomingRequest, out: crate::http::ResponseOutparam) {
            handler::handle(req, out);
        }
    }
    pub mod handler {
        use super::*;
        pub fn init(confirm: bool) -> Result<(), String> {
            CORE.with_borrow_mut(|c| {
                c.confirm = confirm;
                c.init()
            })
        }
        /// The pusher's class, `pusher` by default (ADR 0040, section 13).
        pub fn pusher_class(class: &str) {
            PUSHER_CLASS.set(class.into());
        }
        pub fn handle(req: crate::http::IncomingRequest, out: crate::http::ResponseOutparam) {
            let Some(caller) = wire::caller(&req) else {
                crate::http::respond(out, 403, "text/plain", b"inside callers only");
                return;
            };
            if !matches!(req.method(), crate::http::Method::Post) {
                crate::http::respond(out, 405, "text/plain", b"POST required");
                return;
            }
            let path = req.path_with_query().unwrap_or_default();
            let Some(op) = path.strip_prefix(PREFIX) else {
                crate::http::respond(out, 404, "text/plain", b"unknown room path");
                return;
            };
            let result = crate::http::read_body(&req, 270 * 1024)
                .ok_or_else(bad)
                .and_then(|b| {
                    // A 0.6.0 front still pokes the room: the front pokes the pusher now.
                    if op == "poke" {
                        return Ok(vec![]);
                    }
                    let a = crate::actors::self_address();
                    let pusher = format!(
                        "{}/{}/{}",
                        a.tenant,
                        PUSHER_CLASS.with_borrow(Clone::clone),
                        a.name
                    );
                    CORE.with_borrow_mut(|c| serve(c, op, &b, crate::now_ms(), &caller, &pusher))
                });
            match result {
                Ok(b) => crate::http::respond(out, 200, "application/octet-stream", &b),
                Err(e) => {
                    let status = if e.starts_with("invalid") {
                        400
                    } else if e.starts_with("forbidden") {
                        403
                    } else {
                        503
                    };
                    crate::http::respond(out, status, "text/plain", e.as_bytes());
                }
            }
        }
    }
}
#[cfg(feature = "actor")]
pub use actor::{Room, handler};
