use super::*;
/// Direct-child delivery positions and suspect backoff, recreated after a wake.
/// Use `new` for fixed edges or let `Scaling` synchronize its durable children.
#[derive(Debug, Default)]
pub struct Round {
    children: Vec<Child>,
    relays: Vec<Relay>,
    relay_suspects: std::collections::BTreeMap<String, Suspect>,
    /// The child the last round deferred first: the next round starts there.
    resume: Option<String>,
}
#[derive(Debug)]
struct Child {
    address: String,
    position: Option<Position>,
    initialized: bool,
    suspect: Suspect,
    sockets: Option<u64>,
    lag_retry: LagRetry,
    /// The delay its last answer asked for (lagging sockets, or behind):
    /// a child left out keeps it.
    retry: Option<u64>,
}
impl Child {
    /// What a child the work bound leaves out needs (ADR 0040, section 13):
    /// `None` defers it to the retry 20 ms later; at the head this round
    /// read (`current`), it needs no messages and keeps only the retry its
    /// last answer asked for, if any, so a group at its head goes idle.
    fn left_out(&self, current: Option<(u64, u64, u64)>) -> Option<Option<u64>> {
        let at_head = self.initialized
            && self
                .position
                .zip(current)
                .is_some_and(|(p, (e, _, head))| p.e == e && p.n == head);
        at_head.then_some(self.retry)
    }
}
/// A relayed push: its header, its slice's key and how many of its rows.
type Part = (Push, Option<(u64, u64)>, usize);
/// The rows one pull asks for: the work left after it and the requests of
/// the `sharers` children at its position buys rows, each counted once
/// read and once pushed (once per child unless `relayed`), at least one.
fn pull_rows(left: usize, sharers: usize, relayed: bool) -> usize {
    let per = if relayed { 2 } else { 1 + sharers };
    (left.saturating_sub(PULL_WORK + PUSH_WORK * sharers) / per).max(1)
}
#[derive(Debug, Default)]
struct Suspect {
    failures: u32,
    due: u64,
}
impl Suspect {
    fn fail(&mut self, now: u64) {
        self.due = now.saturating_add(2000u64 << self.failures.min(3));
        self.failures = self.failures.saturating_add(1);
    }
}

/// Only unchanged, entirely backpressured lag can slow retries. A new head,
/// catch-up work, or a changing stalled count keeps the 20 ms cadence.
#[derive(Debug, Default)]
struct LagRetry {
    position: Option<Position>,
    stalled: u64,
    delay: u64,
}
impl LagRetry {
    fn delay(&mut self, p: Position, head: u64, lagging: u64, stalled: u64) -> u64 {
        let only_stalled = lagging > 0 && stalled == lagging && p.n == head;
        self.delay = if only_stalled && self.position == Some(p) && self.stalled == stalled {
            (self.delay * 2).clamp(20, 45000)
        } else {
            20
        };
        self.position = Some(p);
        self.stalled = if only_stalled { stalled } else { 0 };
        self.delay
    }
}

impl Round {
    pub fn sync(&mut self, children: &[ScaleChild]) {
        self.children
            .retain(|c| children.iter().any(|n| n.name == c.address));
        for c in children {
            if !self.children.iter().any(|n| n.address == c.name) {
                self.children.push(Child {
                    address: c.name.clone(),
                    position: None,
                    initialized: true,
                    suspect: Suspect::default(),
                    sockets: None,
                    lag_retry: LagRetry::default(),
                    retry: None,
                });
            }
        }
    }
    pub fn sync_relays(&mut self, children: &[ScaleChild], relays: &[Relay]) {
        self.sync(children);
        self.relays = relays.to_vec();
        self.relay_suspects
            .retain(|name, _| relays.iter().any(|r| &r.name == name));
    }
    pub fn count(&self, name: &str) -> Option<u64> {
        self.children
            .iter()
            .find(|c| c.address == name)
            .and_then(|c| c.sockets)
    }

    pub fn new(addresses: &[&str]) -> Result<Self, String> {
        if addresses.len() > 16 {
            return Err("at most 16 fixed edges".into());
        }
        Ok(Self {
            relays: vec![],
            relay_suspects: Default::default(),
            resume: None,
            children: addresses
                .iter()
                .map(|address| Child {
                    address: (*address).into(),
                    position: None,
                    initialized: false,
                    suspect: Suspect::default(),
                    sockets: None,
                    lag_retry: LagRetry::default(),
                    retry: None,
                })
                .collect(),
        })
    }
    /// One round over `source` (the room's pull through [`Pulled`], or a
    /// local [`Core`]). Sets the next alarm when one is due and answers it,
    /// with the lowest head a pushed child was given, for the pusher's idle
    /// pull (ADR 0040, section 13).
    pub fn run(&mut self, source: &mut impl Source, now: u64) -> Result<RoundResult, String> {
        let mut indices = vec![];
        let mut heads: Vec<Option<(u64, u64)>> = vec![];
        let mut requests = vec![];
        // Pulls by position: children at one position share one pull and one
        // encoding. One round builds at most ROUND_BYTES of messages (one
        // slice at least); the rest keep their positions for 20 ms later.
        let mut slices: std::collections::BTreeMap<Option<(u64, u64)>, Option<Slice>> =
            Default::default();
        let mut read = 0usize;
        let mut pulls = 0usize;
        let mut budget = ROUND_BYTES;
        // The round's work in counts (ROUND_WORK): pulls, requests and rows.
        let mut work = 0usize;
        let mut deferred = false;
        // The earliest retry a lagging child left out at the head asked for.
        let mut held: Option<u64> = None;
        let mut retry = 20;
        // Relayed, a slice's rows are encoded once, in each relay's pack.
        let relayed = !self.relays.is_empty();
        // Relayed pushes: the header, the slice and how many of its rows.
        let mut parts: Vec<Option<Part>> = vec![];
        let mut delivered: Option<Position> = None;
        // The header of a current-epoch position (or none): the epoch's own base.
        let mut current: Option<(u64, u64, u64)> = None;
        // A continuation starts at the child the last round deferred first,
        // so under sustained publishing every child takes its turn.
        let start = self
            .resume
            .take()
            .and_then(|a| self.children.iter().position(|c| c.address == a))
            .unwrap_or(0);
        let order: Vec<usize> = (start..self.children.len()).chain(0..start).collect();
        let due: Vec<bool> = self
            .children
            .iter()
            .map(|child| {
                child.suspect.due <= now
                    && !self.relays.iter().any(|r| {
                        r.edges.contains(&child.address)
                            && self
                                .relay_suspects
                                .get(&r.name)
                                .is_some_and(|s| s.due > now)
                    })
            })
            .collect();
        // The children at each position: a pull is sized to serve them all.
        let mut sharers: std::collections::BTreeMap<Option<(u64, u64)>, usize> = Default::default();
        for (c, _) in self
            .children
            .iter()
            .zip(&due)
            .filter(|(c, d)| **d && c.initialized)
        {
            *sharers.entry(c.position.map(|p| (p.e, p.n))).or_default() += 1;
        }
        for index in order {
            let child = &self.children[index];
            if !due[index] {
                self.children[index].sockets = None;
                continue;
            }
            if work + PUSH_WORK > ROUND_WORK {
                // A child at the head this round read needs no messages: it
                // goes first next round, and sets no alarm unless its last
                // answer asked for one (its sockets lagging), so a group at
                // its head goes idle. It keeps its last socket count.
                match child.left_out(current) {
                    Some(Some(delay)) => {
                        let at = now.saturating_add(delay);
                        held = Some(held.map_or(at, |h| h.min(at)));
                    }
                    Some(None) => {}
                    None => deferred = true,
                }
                self.resume.get_or_insert_with(|| child.address.clone());
                continue;
            }
            let (path, body, header) = if !child.initialized {
                work += PUSH_WORK;
                let mut body = "{\"room\":".to_owned();
                crate::json::write_str(&mut body, &{
                    let a = crate::actors::self_address();
                    format!("{}/{}/{}", a.tenant, a.class, a.name)
                });
                body.push_str(",\"lo\":\"0000000000000000\",\"state\":\"active\"}");
                ("init", body, None)
            } else {
                let key = child.position.map(|p| (p.e, p.n));
                // Not the entry API: an exhausted budget defers the child before any insert.
                #[allow(clippy::map_entry)]
                if !slices.contains_key(&key) {
                    let shortcut = child
                        .position
                        .zip(current)
                        .filter(|(p, (e, _, head))| p.e == *e && p.n == *head);
                    let slice = if let Some((_, (e, base, head))) = shortcut {
                        // A child at a head the round already pulled needs no read.
                        Some(Slice::header(Pull {
                            e,
                            base,
                            first: head.saturating_add(1),
                            head,
                            recover: false,
                            messages: vec![],
                        }))
                    } else if (key.is_some() && (read >= ROUND_BYTES || budget == 0))
                        || pulls >= MAX_PULLS
                        || work + PULL_WORK + PUSH_WORK + usize::from(key.is_some()) * 2
                            > ROUND_WORK
                    {
                        // Reads stop with the budget too: a deferred child costs nothing.
                        deferred = true;
                        self.resume.get_or_insert_with(|| child.address.clone());
                        continue;
                    } else {
                        pulls += 1;
                        // What the pull reads is pushed to the children at
                        // its position: it is sized to serve them all.
                        let rows = pull_rows(
                            ROUND_WORK - work,
                            sharers.get(&key).copied().unwrap_or(1),
                            relayed,
                        );
                        work += PULL_WORK;
                        let budget = if key.is_some() {
                            (ROUND_BYTES - read).min(rows * super::core::ROW_COST)
                        } else {
                            0
                        };
                        match source.pull(child.position, budget, false, now) {
                            Ok(pull) => {
                                if child.position.is_none_or(|p| p.e == pull.e) {
                                    current = Some((pull.e, pull.base, pull.head));
                                }
                                if delivered.is_none_or(|d| (pull.e, pull.head) < (d.e, d.n)) {
                                    delivered = Some(Position {
                                        e: pull.e,
                                        n: pull.head,
                                    });
                                }
                                let slice = Slice::read(pull);
                                read += slice.read;
                                work += slice.items.len();
                                Some(slice)
                            }
                            Err(e) => {
                                // Not durable yet (confirm): the upload's retry is 50 ms.
                                if e.contains("not durable") {
                                    retry = 50;
                                }
                                None
                            }
                        }
                    };
                    slices.insert(key, slice);
                }
                let Some(slice) = slices.get_mut(&key).and_then(Option::as_mut) else {
                    deferred = true;
                    self.resume.get_or_insert_with(|| child.address.clone());
                    continue;
                };
                let h = &slice.header;
                let mut push = Push {
                    recover: h.recover,
                    e: h.e,
                    base: h.base,
                    first: h.head.saturating_add(1).min(9007199254740991),
                    head: h.head,
                    messages: vec![],
                };
                // Every row encoded counts, once per child it is pushed to,
                // or once per slice when relayed. Its bytes too.
                let (charged, charged_bytes) = if relayed {
                    (slice.charged, slice.charged_bytes)
                } else {
                    (0, 0)
                };
                let rows = ROUND_WORK.saturating_sub(work + PUSH_WORK);
                let (mut k, mut fresh) = (0, 0);
                if let Some(&first) = slice.first.as_ref().filter(|_| !h.recover) {
                    if rows == 0 && charged == 0 {
                        deferred = true;
                        self.resume.get_or_insert_with(|| child.address.clone());
                        continue;
                    }
                    // The cut: at most 512 messages and 512 KiB of JSON per
                    // request, and the round's rows left. The header's JSON
                    // is the same for every child of a slice.
                    push.first = first;
                    let fixed = *slice.fixed.get_or_insert_with(|| push.encode().len());
                    let mut bytes = 0;
                    while k < slice.items.len()
                        && k < 512
                        && k < charged + rows
                        && fixed + bytes + slice.sizes[k] + usize::from(k > 0) <= 512 * 1024
                    {
                        bytes += slice.sizes[k] + usize::from(k > 0);
                        k += 1;
                    }
                    let more = bytes.saturating_sub(charged_bytes);
                    if k > charged && more > budget && budget < ROUND_BYTES {
                        deferred = true;
                        self.resume.get_or_insert_with(|| child.address.clone());
                        continue;
                    }
                    budget = budget.saturating_sub(more);
                    fresh = k.saturating_sub(charged);
                    if relayed && k > charged {
                        slice.charged = k;
                        slice.charged_bytes = bytes;
                    }
                }
                work += PUSH_WORK + fresh;
                let header = Some((h.e, h.head));
                if relayed {
                    // The relay's pack is the only encoding (and its check).
                    parts.push(Some((push, key, k)));
                    ("push", String::new(), header)
                } else {
                    push.messages = slice.items[..k].to_vec();
                    let body = push.encode();
                    Push::decode(&body)?; // Reject numbers JSON cannot represent exactly.
                    parts.push(None);
                    ("push", body, header)
                }
            };
            if path == "init" {
                parts.push(None);
            }
            indices.push(index);
            heads.push(header);
            requests.push((
                child.address.clone(),
                crate::actors::Request {
                    method: "POST".into(),
                    path: format!("/wasmkeep-edge/v1/{path}"),
                    headers: vec![],
                    body: body.into_bytes(),
                },
            ));
        }
        let mut next = deferred.then(|| now.saturating_add(retry));
        if let Some(h) = held {
            next = Some(next.map_or(h, |n| n.min(h)));
        }
        let mut skip = std::collections::BTreeSet::new();
        let results = if self.relays.is_empty() {
            crate::calls::call_many(&requests)
                .into_iter()
                .map(Result::ok)
                .collect::<Vec<_>>()
        } else {
            let groups: Vec<_> = self
                .relays
                .iter()
                .map(|r| {
                    (
                        r,
                        requests
                            .iter()
                            .enumerate()
                            .filter(|(_, (n, _))| r.edges.contains(n))
                            .map(|(i, _)| i)
                            .collect::<Vec<_>>(),
                    )
                })
                .filter(|(_, g)| !g.is_empty())
                .collect();
            let mut relay_requests = vec![];
            for (r, g) in &groups {
                let entries = g
                    .iter()
                    .map(|i| {
                        let (push, key, k) = parts[*i].as_ref().ok_or("invalid relayed push")?;
                        let items = slices
                            .get(key)
                            .and_then(Option::as_ref)
                            .map_or(&[][..], |s| &s.items[..*k]);
                        Ok((requests[*i].0.as_str(), push, items))
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                relay_requests.push((
                    r.name.clone(),
                    crate::actors::Request {
                        method: "POST".into(),
                        path: "/wasmkeep-relay/v1/push".into(),
                        headers: vec![],
                        body: super::relay::pack_parts(&entries)?.into_bytes(),
                    },
                ));
            }
            let mut results = vec![None; requests.len()];
            for ((r, g), reply) in groups
                .into_iter()
                .zip(crate::calls::call_many(&relay_requests))
            {
                let suspect = self.relay_suspects.entry(r.name.clone()).or_default();
                if let Some(answers) = reply
                    .ok()
                    .filter(|r| r.status == 200)
                    .and_then(|r| super::relay::answers(&r.body, g.len()))
                {
                    *suspect = Suspect::default();
                    for (i, a) in g.into_iter().zip(answers) {
                        results[i] = a;
                    }
                } else {
                    suspect.fail(now);
                    skip.extend(g);
                }
            }
            results
        };
        for (slot, (index, result)) in indices.into_iter().zip(results).enumerate() {
            // A child sent a request this round says its count and retry again.
            self.children[index].sockets = None;
            self.children[index].retry = None;
            if skip.contains(&slot) {
                continue;
            }
            let child = &mut self.children[index];
            let ok = result.filter(|r| r.status == 200).and_then(|r| {
                if !child.initialized {
                    child.initialized = true;
                    return Some(20);
                }
                // Each child's answer is checked against its own pull's header.
                let (epoch, head) = heads[slot]?;
                let (p, more) = acknowledgment(&r.body, epoch, head)?;
                child.position = Some(p);
                child.sockets = crate::json::parse_unique(std::str::from_utf8(&r.body).ok()?)
                    .and_then(|j| super::edge::number(&j, "sockets").ok());
                let j = crate::json::parse_unique(std::str::from_utf8(&r.body).ok()?)?;
                let lagging = super::edge::number(&j, "lagging").ok()?;
                let stalled = super::edge::number(&j, "stalled").unwrap_or(0);
                let delay =
                    child
                        .lag_retry
                        .delay(p, head, lagging, if p.e == epoch { stalled } else { 0 });
                let delay = if more { delay } else { 0 };
                child.retry = (delay > 0).then_some(delay);
                Some(delay)
            });
            if let Some(delay) = ok {
                child.suspect = Suspect::default();
                if delay > 0 {
                    let at = now.saturating_add(delay);
                    next = Some(next.map_or(at, |n: u64| n.min(at)));
                }
            } else {
                child.suspect.fail(now);
            }
        }
        for c in &self.children {
            if c.suspect.due > now {
                next = Some(next.map_or(c.suspect.due, |n: u64| n.min(c.suspect.due)));
            }
        }
        for s in self.relay_suspects.values() {
            if s.due > now {
                next = Some(next.map_or(s.due, |n| n.min(s.due)));
            }
        }
        if let Some(at) = next {
            crate::alarms::set_alarm(at);
        }
        let occupied = self
            .children
            .iter()
            .any(|c| c.sockets.is_none_or(|n| n > 0));
        Ok(RoundResult {
            next,
            delivered,
            occupied,
        })
    }
}

/// What a round reads from (ADR 0040, section 13): the room's pull, over
/// calls ([`Pulled`]) in the pusher, or a [`Core`] in this instance.
pub trait Source {
    fn pull(
        &mut self,
        after: Option<Position>,
        budget: usize,
        idle: bool,
        now: u64,
    ) -> Result<Pull, String>;
}
impl<S: Store> Source for Core<S> {
    fn pull(
        &mut self,
        after: Option<Position>,
        budget: usize,
        idle: bool,
        now: u64,
    ) -> Result<Pull, String> {
        Core::pull(self, after, budget, idle, now)
    }
}
/// What a round leaves for the pusher's idle decision.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RoundResult {
    /// The alarm the round set, if any.
    pub next: Option<u64>,
    /// The lowest head a pushed child was given: where an idle pull claims to be.
    pub delivered: Option<Position>,
    /// Some child holds sockets, or did not say: an idle pusher keeps an alarm.
    pub occupied: bool,
}
/// Distinct positions one round pulls at most: the rest are deferred to
/// the next round, so laggards at many positions never make one round a
/// long chain of synchronous calls to the room.
pub const MAX_PULLS: usize = 16;

/// Message bytes one round builds for its children (ADR 0040, section 4).
const ROUND_BYTES: usize = 1024 * 1024;
/// The work one round does at most, in counts a guest can keep (ADR 0040,
/// section 13): each pull costs `PULL_WORK`, each request to a child
/// `PUSH_WORK`, each row read 1 and each row encoded into a child's push
/// 1 (once per child). The children the rest would need are deferred to
/// the next round, which starts at the first of them. Chosen so a round
/// stays near half the 50 ms budget on QuickJS whatever the children lag.
pub const ROUND_WORK: usize = 640;
/// A pull's work: one call to the room and its header.
const PULL_WORK: usize = 8;
/// A request's work: one child's push or init, its call and its answer.
const PUSH_WORK: usize = 3;
/// The messages after one position, each with its JSON size.
struct Slice {
    header: Pull,
    first: Option<u64>,
    items: Vec<Item>,
    sizes: Vec<usize>,
    read: usize,
    /// Relayed: the rows and JSON bytes already counted, since a relay's
    /// children share one encoding of them.
    charged: usize,
    charged_bytes: usize,
    /// The header's JSON size, the same for every child of the slice.
    fixed: Option<usize>,
}
impl Slice {
    fn header(header: Pull) -> Self {
        Slice {
            header,
            first: None,
            items: vec![],
            sizes: vec![],
            read: 0,
            charged: 0,
            charged_bytes: 0,
            fixed: None,
        }
    }
    fn read(mut pull: Pull) -> Self {
        let messages = std::mem::take(&mut pull.messages);
        let mut s = Slice::header(pull);
        for (m, base) in messages {
            s.first.get_or_insert(m.position.n);
            let item = Item {
                epoch: Some(Epoch {
                    e: m.position.e,
                    base,
                }),
                at: m.at,
                id: m.id,
                body: m.body,
            };
            let size = item.json().len();
            // Every row costs, an empty one too: its read, its item and its JSON.
            s.read += super::core::ROW_COST + size;
            s.sizes.push(size);
            s.items.push(item);
        }
        s
    }
}
fn acknowledgment(body: &[u8], epoch: u64, head: u64) -> Option<(Position, bool)> {
    let j = crate::json::parse_unique(std::str::from_utf8(body).ok()?)?;
    let p = Position {
        e: super::edge::number(&j, "e").ok()?,
        n: super::edge::number(&j, "n").ok()?,
    };
    if p.e > epoch || (p.e == epoch && p.n > head) {
        return None;
    }
    let lagging = super::edge::number(&j, "lagging").ok()?;
    Some((p, p.e != epoch || p.n < head || lagging > 0))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn r4_suspect_backoff_and_success_reset() {
        let mut s = Suspect::default();
        let mut now = 0;
        for delay in [2000, 4000, 8000, 16000, 16000] {
            s.fail(now);
            assert_eq!(s.due, now + delay);
            now = s.due;
        }
        s = Suspect::default();
        s.fail(now);
        assert_eq!(s.due, now + 2000);
    }
    #[test]
    fn old_epoch_ack_above_recovered_head_is_not_a_failed_child() {
        assert_eq!(
            acknowledgment(br#"{"e":1,"n":5,"lagging":0}"#, 2, 3),
            Some((Position { e: 1, n: 5 }, true))
        );
        assert!(acknowledgment(br#"{"e":2,"n":5,"lagging":0}"#, 2, 3).is_none());
        assert!(acknowledgment(br#"{"e":3,"n":0,"lagging":0}"#, 2, 3).is_none());
        assert!(acknowledgment(br#"{"e":2,"n":3,"lagging":-1}"#, 2, 3).is_none());
    }
}
#[cfg(test)]
mod stall_tests {
    use super::*;
    #[test]
    fn stalled_only_alarm_backoff_resets_for_healthy_work() {
        let mut retry = LagRetry::default();
        let p = Position { e: 1, n: 1 };
        let delays: Vec<_> = (0..15).map(|_| retry.delay(p, 1, 1, 1)).collect();
        assert_eq!(&delays[..5], &[20, 40, 80, 160, 320]);
        assert_eq!(delays[14], 45000);
        assert_eq!(retry.delay(Position { e: 1, n: 2 }, 2, 1, 1), 20);
        assert_eq!(retry.delay(Position { e: 1, n: 2 }, 2, 2, 1), 20);
    }
}
#[cfg(test)]
mod work_tests {
    use super::*;
    fn child(position: Option<Position>, retry: Option<u64>) -> Child {
        Child {
            address: "edge/e".into(),
            position,
            initialized: true,
            suspect: Suspect::default(),
            sockets: Some(3),
            lag_retry: LagRetry::default(),
            retry,
        }
    }
    #[test]
    fn a_child_left_out_at_the_head_keeps_its_retry() {
        let head = Some((2, 0, 9));
        let at = Some(Position { e: 2, n: 9 });
        // At the head, nothing asked: left out silently.
        assert_eq!(child(at, None).left_out(head), Some(None));
        // At the head with lagging sockets: its own retry.
        assert_eq!(child(at, Some(160)).left_out(head), Some(Some(160)));
        // Behind, unknown, or no head read yet: deferred to the 20 ms retry.
        let behind = Some(Position { e: 2, n: 8 });
        assert_eq!(child(behind, None).left_out(head), None);
        assert_eq!(child(None, None).left_out(head), None);
        assert_eq!(child(at, None).left_out(None), None);
    }
    #[test]
    fn a_pull_is_sized_for_the_children_at_its_position() {
        // One child: half the work left after a pull and a push.
        assert_eq!(pull_rows(ROUND_WORK, 1, false), 314);
        assert_eq!(pull_rows(ROUND_WORK, 1, true), 314);
        // 127 relayed children share one encoding: their requests first.
        assert_eq!(pull_rows(ROUND_WORK, 127, true), 125);
        // Direct, every child is encoded its own rows.
        assert_eq!(pull_rows(ROUND_WORK, 16, false), 34);
        // More children than the work: still one row.
        assert_eq!(pull_rows(ROUND_WORK, 256, true), 1);
    }
}
