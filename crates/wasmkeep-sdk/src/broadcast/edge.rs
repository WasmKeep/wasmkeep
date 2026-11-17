//! Bounded memory delivery; never calls another actor or writes stream state.
use super::Position;
use crate::json::{self, Json};
use std::collections::{BTreeMap, VecDeque};
#[derive(Debug, Clone)]
pub struct Item {
    /// Original R2 identity for retained messages in a newer round.
    pub epoch: Option<super::Epoch>,
    pub at: u64,
    pub id: Option<String>,
    pub body: String,
}
impl Item {
    fn json(&self) -> String {
        let mut s = format!("{{\"a\":{},\"d\":", self.at);
        json::write_str(&mut s, &self.body);
        if let Some(id) = &self.id {
            s.push_str(",\"i\":");
            json::write_str(&mut s, id);
        }
        if let Some(e) = self.epoch {
            s.push_str(&format!(",\"e\":{},\"b\":{}", e.e, e.base));
        }
        s.push('}');
        s
    }
}
#[derive(Debug, Clone)]
pub struct Push {
    /// The history still has rows whose epoch boundaries have expired.
    pub recover: bool,
    pub e: u64,
    pub base: u64,
    pub first: u64,
    pub head: u64,
    pub messages: Vec<Item>,
}
impl Push {
    pub fn encode(&self) -> String {
        format!(
            "{{\"e\":{},\"b\":{},\"first\":{},\"head\":{},\"messages\":[{}],\"recover\":{}}}",
            self.e,
            self.base,
            self.first,
            self.head,
            self.messages
                .iter()
                .map(Item::json)
                .collect::<Vec<_>>()
                .join(","),
            self.recover
        )
    }
    pub fn decode(s: &str) -> Result<Self, String> {
        let j = json::parse_unique(s).ok_or("invalid push")?;
        let Some(Json::Array(items)) = j.get("messages") else {
            return Err("invalid push messages".into());
        };
        if items.len() > 512 || s.len() > 1024 * 1024 {
            return Err("invalid push size".into());
        }
        let p = Self {
            recover: match j.get("recover") {
                None => false,
                Some(Json::Bool(b)) => *b,
                _ => return Err("invalid recovery flag".into()),
            },
            e: number(&j, "e")?,
            base: number(&j, "b")?,
            first: number(&j, "first")?,
            head: number(&j, "head")?,
            messages: items
                .iter()
                .map(|m| {
                    Ok(Item {
                        epoch: match (m.get("e"), m.get("b")) {
                            (None, None) => None,
                            (Some(_), Some(_)) => Some(super::Epoch {
                                e: number(m, "e")?,
                                base: number(m, "b")?,
                            }),
                            _ => return Err("invalid item epoch".into()),
                        },
                        at: number(m, "a")?,
                        body: string(m, "d")?.into(),
                        id: match m.get("i") {
                            Some(Json::String(s)) => Some(s.clone()),
                            None => None,
                            _ => return Err("invalid id".into()),
                        },
                    })
                })
                .collect::<Result<_, String>>()?,
        };
        if p.first == 0
            || p.base > p.head
            || (!p.messages.is_empty()
                && p.first.saturating_add(p.messages.len() as u64 - 1) > p.head)
        {
            return Err("invalid push position".into());
        }
        for (i, m) in p.messages.iter().enumerate() {
            if m.body.len() > 65536
                || m.id.as_ref().is_some_and(|s| s.len() > 64)
                || m.epoch
                    .is_some_and(|e| e.e > p.e || e.base >= p.first + i as u64)
            {
                return Err("invalid push item".into());
            }
        }
        Ok(p)
    }
}
pub(super) fn number(j: &Json, key: &str) -> Result<u64, String> {
    match j.get(key) {
        Some(Json::Number(n))
            if n.is_finite() && *n >= 0.0 && *n <= 9007199254740991.0 && n.fract() == 0.0 =>
        {
            Ok(*n as u64)
        }
        _ => Err(format!("invalid {key}")),
    }
}
pub(super) fn string<'a>(j: &'a Json, key: &str) -> Result<&'a str, String> {
    match j.get(key) {
        Some(Json::String(s)) => Ok(s),
        _ => Err(format!("invalid {key}")),
    }
}
#[derive(Debug, PartialEq, Eq)]
pub enum SendResult {
    Ok,
    Gone,
    Backpressure,
}
pub trait SocketIo {
    fn send(&mut self, id: u64, text: &str) -> SendResult;
    fn close(&mut self, id: u64, code: u16);
}
#[derive(Debug, Default)]
pub struct HostSockets;
impl SocketIo for HostSockets {
    fn send(&mut self, id: u64, text: &str) -> SendResult {
        match crate::websockets::send(id, &crate::Message::Text(text.into())) {
            Ok(()) => SendResult::Ok,
            Err(crate::websockets::SendError::Backpressure) => SendResult::Backpressure,
            Err(_) => SendResult::Gone,
        }
    }
    fn close(&mut self, id: u64, code: u16) {
        crate::websockets::close(id, code, if code == 4802 { "slow" } else { "moved" });
    }
}
#[derive(Debug)]
pub(super) struct Buffered {
    n: u64,
    e: u64,
    base: u64,
    json: String,
}
#[derive(Debug)]
struct Cursor {
    n: u64,
    lagging: bool,
}
#[derive(Debug, Default)]
pub struct Edge {
    pub head: Option<Position>,
    pub floor: u64,
    pub(super) buffer: VecDeque<Buffered>,
    size: usize,
    cursors: BTreeMap<u64, Cursor>,
    slow: u64,
    opens: u64,
    refusals: [u64; 3],
}
#[derive(Debug)]
pub struct Ack {
    pub position: Position,
    pub sockets: usize,
    pub lagging: usize,
    pub slow: u64,
    pub opens: u64,
    pub refusals: [u64; 3],
}
impl Ack {
    pub fn encode(&self) -> String {
        format!(
            "{{\"e\":{},\"n\":{},\"sockets\":{},\"lagging\":{},\"slow\":{},\"opens\":{},\"refusals\":{{\"unknown\":{},\"moved\":{},\"full\":{}}}}}",
            self.position.e,
            self.position.n,
            self.sockets,
            self.lagging,
            self.slow,
            self.opens,
            self.refusals[0],
            self.refusals[1],
            self.refusals[2]
        )
    }
}
impl Edge {
    pub fn open(&mut self, id: u64, position: Option<Position>, io: &mut impl SocketIo) -> bool {
        if self.cursors.len() >= 500 {
            self.refuse(2);
            return false;
        }
        let resume = self
            .head
            .zip(position)
            .is_some_and(|(h, p)| p.e == h.e && p.n >= self.floor && p.n <= h.n);
        let n = if resume {
            position.map_or(0, |p| p.n)
        } else {
            self.head.map_or(0, |p| p.n)
        };
        self.opens += 1;
        self.cursors.insert(id, Cursor { n, lagging: false });
        let ready = if self.head.is_none() || (position.is_some() && !resume) {
            "null".into()
        } else {
            n.to_string()
        };
        let _ = io.send(
            id,
            &format!(
                "{{\"t\":\"ready\",\"e\":{},\"n\":{ready}}}",
                self.head.map_or(0, |p| p.e)
            ),
        );
        self.deliver(id, io);
        true
    }
    pub(super) fn refuse(&mut self, reason: usize) {
        self.refusals[reason] += 1;
    }
    pub fn forget(&mut self, id: u64) {
        self.cursors.remove(&id);
    }
    pub fn adopt(&mut self, ids: &[u64]) {
        if let Some(h) = self.head {
            for id in ids {
                self.cursors.entry(*id).or_insert(Cursor {
                    n: h.n,
                    lagging: false,
                });
            }
        }
    }
    pub fn ping(&mut self, id: u64, io: &mut impl SocketIo) {
        if let Some(h) = self.head {
            let _ = io.send(
                id,
                &format!("{{\"t\":\"pong\",\"e\":{},\"n\":{}}}", h.e, h.n),
            );
        }
        self.deliver(id, io);
    }
    pub fn push(&mut self, p: &Push, io: &mut impl SocketIo) -> Ack {
        if self.head.is_some_and(|h| p.e < h.e) {
            return self.answer();
        }
        let mut h = self.head.unwrap_or(Position {
            e: p.e,
            n: if p.messages.is_empty() {
                p.head
            } else {
                p.first.saturating_sub(1)
            },
        });
        if self.head.is_none() {
            self.floor = h.n;
        }
        if p.e > h.e {
            self.buffer.retain(|m| m.n <= p.base);
            self.size = self.buffer.iter().map(|m| m.json.len()).sum();
            h = Position {
                e: p.e,
                n: h.n.min(p.base),
            };
            self.floor = self.floor.min(p.base);
            for c in self.cursors.values_mut() {
                c.n = c.n.min(p.base);
            }
        }
        if p.recover {
            self.buffer.clear();
            self.size = 0;
            self.floor = p.head;
            h.n = p.head;
        }
        if !p.messages.is_empty() && p.first > h.n.saturating_add(1) {
            self.buffer.clear();
            self.size = 0;
            self.floor = p.first - 1;
            h.n = self.floor;
        }
        for (i, item) in p.messages.iter().enumerate() {
            let n = p.first + i as u64;
            if n <= h.n {
                continue;
            }
            let epoch = item.epoch.unwrap_or(super::Epoch {
                e: p.e,
                base: p.base,
            });
            let json = Item {
                epoch: None,
                ..item.clone()
            }
            .json();
            self.size += json.len();
            self.buffer.push_back(Buffered {
                n,
                e: epoch.e,
                base: epoch.base,
                json,
            });
            h.n = n;
            while self.buffer.len() > 512 || self.size > 1024 * 1024 {
                if let Some(old) = self.buffer.pop_front() {
                    self.floor = old.n;
                    self.size -= old.json.len();
                }
            }
        }
        self.head = Some(h);
        for id in self.cursors.keys().copied().collect::<Vec<_>>() {
            self.deliver(id, io);
        }
        self.answer()
    }
    fn behind(&mut self, id: u64, io: &mut impl SocketIo) {
        let Some(h) = self.head else { return };
        match io.send(
            id,
            &format!("{{\"t\":\"behind\",\"e\":{},\"n\":{}}}", h.e, h.n),
        ) {
            SendResult::Backpressure => {
                io.close(id, 4802);
                self.slow += 1;
                self.forget(id);
            }
            SendResult::Gone => self.forget(id),
            SendResult::Ok => {
                if let Some(c) = self.cursors.get_mut(&id) {
                    c.n = h.n;
                    c.lagging = false;
                }
            }
        }
    }
    fn deliver(&mut self, id: u64, io: &mut impl SocketIo) {
        let Some(c) = self.cursors.get(&id) else {
            return;
        };
        if c.n < self.floor {
            self.behind(id, io);
            return;
        }
        let mut pending = self.buffer.iter().filter(|m| m.n > c.n);
        let Some(first) = pending.next() else { return };
        let mut frame = format!(
            "{{\"t\":\"m\",\"e\":{},\"b\":{},\"n\":{},\"m\":[",
            first.e, first.base, first.n
        );
        let mut last = None;
        for m in std::iter::once(first).chain(pending) {
            if m.e != first.e
                || frame.len() + m.json.len() + usize::from(last.is_some()) + 2 > 65536
            {
                break;
            }
            if last.is_some() {
                frame.push(',');
            }
            frame.push_str(&m.json);
            last = Some(m.n);
        }
        let Some(last) = last else {
            self.behind(id, io);
            return;
        };
        frame.push_str("]}");
        match io.send(id, &frame) {
            SendResult::Gone => self.forget(id),
            result => {
                if let Some(c) = self.cursors.get_mut(&id) {
                    if result == SendResult::Ok {
                        c.n = last;
                    }
                    c.lagging = self.head.is_some_and(|h| c.n < h.n);
                }
            }
        }
    }
    fn answer(&mut self) -> Ack {
        Ack {
            position: self.head.unwrap_or(Position { e: 0, n: 0 }),
            sockets: self.cursors.len(),
            lagging: self.cursors.values().filter(|c| c.lagging).count(),
            slow: std::mem::take(&mut self.slow),
            opens: std::mem::take(&mut self.opens),
            refusals: std::mem::take(&mut self.refusals),
        }
    }
}
