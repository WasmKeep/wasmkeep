use super::*;
use crate::wire::{self, Reader, Writer};
const PREFIX: &str = "/wasmkeep-room/v1/";
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
#[cfg(feature = "actor")]
fn serve<S: Store>(core: &mut Core<S>, op: &str, body: &[u8], now: u64) -> Result<Vec<u8>, String> {
    let mut r = Reader::new(body);
    let mut w = Writer::default();
    match op {
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
/// Front-side helper: publish and gap requests go over HTTP to the front,
/// which authorizes them and calls this client. Never call it from an edge.
#[derive(Debug)]
pub struct Client {
    address: String,
}
impl Client {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
        }
    }
    fn call(&self, op: &str, body: Vec<u8>) -> Result<Vec<u8>, String> {
        let r = wire::call(&self.address, "POST", format!("{PREFIX}{op}"), vec![], body)?;
        if r.status != 200 {
            return Err(format!("room answered {}", r.status));
        }
        Ok(r.body)
    }
    /// Authorize the client first. Directory lookup and the alarm-only poke
    /// share one batch; a cold directory is retried once after that batch.
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
            path: format!("{PREFIX}poke"),
            headers: vec![],
            body: vec![],
        };
        let results = crate::calls::call_many(&[
            (directory.into(), lookup.clone()),
            (self.address.clone(), poke),
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
        end(&r)?;
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
    thread_local! {static LAST_ROUND:std::cell::Cell<Option<u64>>=const {std::cell::Cell::new(None)};static CORE:std::cell::RefCell<Core> = std::cell::RefCell::new(Core::new(Host,false));}
    /// Default room class (`confirm = false`). For confirmation, delegate your
    /// lifecycle to `handler::init(true)` and `handler::before_push`.
    #[derive(Debug)]
    pub struct Room;
    impl crate::Actor for Room {
        fn on_init() -> Result<(), String> {
            handler::init(false)
        }
        fn on_alarm() -> Result<(), String> {
            handler::before_push().map(|_| ())
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
        /// R3 must call this immediately before sending its round, and send only
        /// when true. In R2 there are no children to send to.
        pub fn before_push() -> Result<bool, String> {
            CORE.with_borrow_mut(|c| c.before_push(crate::now_ms()))
        }
        /// Run a fixed-edge round from the room's alarm.
        pub fn round(round: &mut Round) -> Result<(), String> {
            LAST_ROUND.set(Some(crate::now_ms()));
            CORE.with_borrow_mut(|c| round.run(c, crate::now_ms()))
        }
        /// Run the R4 controller and direct-child round from on-alarm.
        pub fn scaling(scale: &mut Scaling, round: &mut Round) -> Result<(), String> {
            let now = crate::now_ms();
            LAST_ROUND.set(Some(now));
            CORE.with_borrow_mut(|c| scale.run(round, c, now))
        }
        pub fn poke() {
            let now = crate::now_ms();
            if LAST_ROUND
                .get()
                .is_none_or(|last| now.saturating_sub(last) > 1000)
                && crate::alarms::get_alarm().is_none()
            {
                crate::alarms::set_alarm(now);
            }
        }
        pub fn handle(req: crate::http::IncomingRequest, out: crate::http::ResponseOutparam) {
            if wire::caller(&req).is_none() {
                crate::http::respond(out, 403, "text/plain", b"inside callers only");
                return;
            }
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
                    if op == "poke" {
                        if !b.is_empty() {
                            return Err(bad());
                        }
                        poke();
                        Ok(vec![])
                    } else {
                        CORE.with_borrow_mut(|c| serve(c, op, &b, crate::now_ms()))
                    }
                });
            match result {
                Ok(b) => crate::http::respond(out, 200, "application/octet-stream", &b),
                Err(e) => {
                    let status = if e.starts_with("invalid") { 400 } else { 503 };
                    crate::http::respond(out, status, "text/plain", e.as_bytes());
                }
            }
        }
    }
}
#[cfg(feature = "actor")]
pub use actor::{Room, handler};
