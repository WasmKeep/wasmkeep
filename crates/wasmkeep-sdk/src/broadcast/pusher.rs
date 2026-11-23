//! The pusher (ADR 0040, section 13): the room's rounds, children, relays
//! and scaling, in a class of their own. It pulls from the room and pushes
//! to relays and edges; the room never calls it.
use super::*;
use crate::wire::{self, Reader, Writer};
const PREFIX: &str = "/wasmkeep-room/v1/";
/// The pusher's only path: a join's poke (empty body) or a publish's (`u64` 1).
pub(super) const POKE: &str = "/wasmkeep-pusher/v1/poke";
fn bad() -> String {
    "invalid pull answer".into()
}
fn number(r: &mut Reader<'_>) -> Result<u64, String> {
    r.u64().ok_or_else(bad)
}
fn flag(r: &mut Reader<'_>) -> Result<bool, String> {
    match number(r)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(bad()),
    }
}
fn text(r: &mut Reader<'_>) -> Result<String, String> {
    String::from_utf8(r.bytes().ok_or_else(bad)?.to_vec()).map_err(|_| bad())
}
/// The room's `pull` and `adopt`, over calls: the pusher's [`Source`].
#[derive(Debug, Clone)]
pub struct Pulled {
    room: String,
}
impl Pulled {
    pub fn new(room: impl Into<String>) -> Self {
        Self { room: room.into() }
    }
    fn call(&self, op: &str, body: Vec<u8>) -> Result<Vec<u8>, String> {
        let r = wire::call(&self.room, "POST", format!("{PREFIX}{op}"), vec![], body)?;
        if r.status != 200 {
            let detail = String::from_utf8_lossy(&r.body[..r.body.len().min(200)]).into_owned();
            return Err(format!("room answered {}: {detail}", r.status));
        }
        Ok(r.body)
    }
    /// Section 13's migration steps: 0 reads, 1 hands over, 2 retires.
    pub fn adopt(&self, kind: u64, directory: &str, edges: &[String]) -> Result<Vec<u8>, String> {
        let mut w = Writer::default();
        w.u64(kind);
        if kind == 1 {
            w.bytes(directory.as_bytes()).u64(edges.len() as u64);
            for e in edges {
                w.bytes(e.as_bytes());
            }
        }
        self.call("adopt", w.finish())
    }
}
impl Source for Pulled {
    fn pull(
        &mut self,
        after: Option<Position>,
        budget: usize,
        idle: bool,
        _now: u64,
    ) -> Result<Pull, String> {
        let mut w = Writer::default();
        w.u64(u64::from(after.is_some()));
        if let Some(p) = after {
            w.u64(p.e).u64(p.n);
        }
        w.u64(budget as u64).u64(u64::from(idle));
        let body = self.call("pull", w.finish())?;
        let mut r = Reader::new(&body);
        let mut pull = Pull {
            e: number(&mut r)?,
            base: number(&mut r)?,
            first: number(&mut r)?,
            head: number(&mut r)?,
            recover: flag(&mut r)?,
            messages: vec![],
        };
        let n = number(&mut r)?;
        if n > 513 {
            return Err(bad());
        }
        for i in 0..n {
            let position = Position {
                e: number(&mut r)?,
                n: number(&mut r)?,
            };
            let base = number(&mut r)?;
            let at = number(&mut r)?;
            let id = if flag(&mut r)? {
                Some(text(&mut r)?)
            } else {
                None
            };
            let body = text(&mut r)?;
            if Some(position.n) != pull.first.checked_add(i) {
                return Err(bad());
            }
            pull.messages.push((
                Message {
                    position,
                    at,
                    id,
                    body,
                },
                base,
            ));
        }
        if !r.is_empty() {
            return Err(bad());
        }
        Ok(pull)
    }
}
/// After a round whose next alarm is not 20 ms away, the pusher tells the
/// room it stops clocking itself: the room then asks publishers' fronts to
/// poke it. A head that moved meanwhile keeps the 20 ms clock instead.
/// A pusher that went idle after delivering something new keeps one alarm
/// this much later: a poke lost while it could not be reached (its node
/// died between a publish and its round) is caught up then.
pub const SAFETY_ROUND: u64 = 5000;
/// While its group holds sockets, an idle pusher always keeps one durable
/// alarm at most this far out: an acknowledged publish whose poke was lost
/// (a node death, a failed call) is delivered by then even if nothing
/// else ever comes. An empty group keeps none (ADR 0040, section 13).
pub const IDLE_ROUND: u64 = 60_000;
thread_local! {
    static SAFE: std::cell::Cell<Option<Position>> = const { std::cell::Cell::new(None) };
    static IDLE_MS: std::cell::Cell<u64> = const { std::cell::Cell::new(IDLE_ROUND) };
}
pub(super) fn idle(source: &mut impl Source, result: RoundResult, now: u64) {
    if result.next.is_some_and(|at| at <= now.saturating_add(20)) {
        return;
    }
    let keep = |delay: u64| {
        let at = now.saturating_add(delay);
        if result.next.is_none_or(|n| n > at) {
            crate::alarms::set_alarm(at);
        }
    };
    let idle_ms = IDLE_MS.get();
    match source.pull(result.delivered, 0, true, now) {
        Ok(h) => {
            if result
                .delivered
                .is_some_and(|d| d.e != h.e || d.n != h.head)
            {
                keep(20);
            } else if result.delivered.is_some() && SAFE.get() != result.delivered {
                SAFE.set(result.delivered);
                keep(SAFETY_ROUND.min(idle_ms));
            } else if result.occupied {
                keep(idle_ms);
            }
        }
        // The room is unreachable: it cannot ask for a poke, so keep a clock.
        Err(_) => keep(1000),
    }
}
#[cfg(feature = "actor")]
pub mod handler {
    //! Delegate a pusher class's `on_alarm` to [`round`] (fixed edges) or
    //! [`scaling`], and its `handle` to [`handle`] (the poke).
    use super::*;
    thread_local! {
        static LAST_ROUND: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
        static ROOM_CLASS: std::cell::RefCell<String> = std::cell::RefCell::new("room".into());
    }
    /// The longest an idle pusher with sockets waits for its next round
    /// ([`IDLE_ROUND`], 60 s); tests shorten it.
    pub fn idle_period(ms: u64) {
        IDLE_MS.set(ms);
    }
    /// The room's class, `room` by default: the pusher `pusher/lobby` pulls
    /// from `<class>/lobby`. Nodes keep the classes `room` and `pusher`
    /// apart at wake-up (ADR 0041); classes named otherwise are not kept
    /// apart.
    pub fn room_class(class: &str) {
        ROOM_CLASS.set(class.into());
    }
    /// The room this pusher pulls from.
    pub fn room() -> Pulled {
        Pulled::new(format!(
            "{}/{}",
            ROOM_CLASS.with_borrow(Clone::clone),
            crate::actors::self_address().name
        ))
    }
    /// A fixed-edge round (R3) from the pusher's alarm.
    pub fn round(round: &mut Round) -> Result<(), String> {
        let now = crate::now_ms();
        LAST_ROUND.set(Some(now));
        let mut source = room();
        let result = round.run(&mut source, now)?;
        idle(&mut source, result, now);
        Ok(())
    }
    /// The scaled group (R4/R5) from the pusher's alarm: a 0.6.0 room's
    /// state moves over first (section 13), then a round and a scale step.
    pub fn scaling(scale: &mut Scaling, round: &mut Round) -> Result<(), String> {
        let now = crate::now_ms();
        LAST_ROUND.set(Some(now));
        let mut source = room();
        if !scale.migrate(&source, now)? {
            return Ok(());
        }
        let result = scale.run(round, &mut source, now)?;
        idle(&mut source, result, now);
        Ok(())
    }
    /// A join's poke: a round now, at most one a second. A publish's poke:
    /// the round 20 ms from now, bringing a later alarm in.
    pub fn poke(publish: bool) {
        let now = crate::now_ms();
        let at = crate::alarms::get_alarm();
        if publish {
            if at.is_none_or(|at| at > now.saturating_add(20)) {
                crate::alarms::set_alarm(now.saturating_add(20));
            }
        } else if LAST_ROUND
            .get()
            .is_none_or(|last| now.saturating_sub(last) > 1000)
            && at.is_none()
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
        if req.path_with_query().as_deref() != Some(POKE) {
            crate::http::respond(out, 404, "text/plain", b"unknown pusher path");
            return;
        }
        let body = crate::http::read_body(&req, 64);
        let publish = match body.as_deref() {
            Some([]) => Some(false),
            Some(b) => {
                let mut r = Reader::new(b);
                (r.u64() == Some(1) && r.is_empty()).then_some(true)
            }
            None => None,
        };
        match publish {
            Some(publish) => {
                poke(publish);
                crate::http::respond(out, 200, "text/plain", b"");
            }
            None => crate::http::respond(out, 400, "text/plain", b"invalid poke"),
        }
    }
}
