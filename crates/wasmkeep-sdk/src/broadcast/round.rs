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
                })
                .collect(),
        })
    }
    pub fn run<S: Store>(&mut self, core: &mut Core<S>, now: u64) -> Result<(), String> {
        for child in &mut self.children {
            child.sockets = None;
        }
        if !core.before_push(now)? {
            return Ok(());
        }
        let epoch = core.wake()?;
        let epochs = core.epochs()?;
        let head = core.last()?.unwrap_or(epoch.base);
        let mut indices = vec![];
        let mut requests = vec![];
        // Slices by start: children at one position share one read and one
        // encoding. One round builds at most ROUND_BYTES of messages (one
        // slice at least); the rest keep their positions for 20 ms later.
        let mut slices: std::collections::BTreeMap<(u64, u64), Slice> = Default::default();
        let mut read = 0usize;
        let mut budget = ROUND_BYTES;
        let mut deferred = false;
        // A continuation starts at the child the last round deferred first,
        // so under sustained publishing every child takes its turn.
        let start = self
            .resume
            .take()
            .and_then(|a| self.children.iter().position(|c| c.address == a))
            .unwrap_or(0);
        let order: Vec<usize> = (start..self.children.len()).chain(0..start).collect();
        for index in order {
            let child = &self.children[index];
            if child.suspect.due > now
                || self.relays.iter().any(|r| {
                    r.edges.contains(&child.address)
                        && self
                            .relay_suspects
                            .get(&r.name)
                            .is_some_and(|s| s.due > now)
                })
            {
                continue;
            }
            let (path, body) = if !child.initialized {
                let mut body = "{\"room\":".to_owned();
                crate::json::write_str(&mut body, &{
                    let a = crate::actors::self_address();
                    format!("{}/{}/{}", a.tenant, a.class, a.name)
                });
                body.push_str(",\"lo\":\"0000000000000000\",\"state\":\"active\"}");
                ("init", body)
            } else {
                let n = child.position.map(|p| {
                    epochs
                        .iter()
                        .filter(|e| e.e > p.e)
                        .fold(p.n, |n, e| n.min(e.base))
                });
                let mut push = Push {
                    recover: false,
                    e: epoch.e,
                    base: child.position.map_or(epoch.base, |p| {
                        epochs
                            .iter()
                            .filter(|e| e.e > p.e)
                            .fold(epoch.base, |b, e| b.min(e.base))
                    }),
                    first: head.saturating_add(1).min(9007199254740991),
                    head,
                    messages: vec![],
                };
                if let Some(n) = n.filter(|n| *n < head) {
                    let key = (n, push.base);
                    if let std::collections::btree_map::Entry::Vacant(e) = slices.entry(key) {
                        if read >= ROUND_BYTES || budget == 0 {
                            deferred = true;
                            self.resume.get_or_insert_with(|| child.address.clone());
                            continue;
                        }
                        // Never more rows than the budget has left (one at least).
                        let rows = ((ROUND_BYTES - read) / ROW_COST).clamp(1, 513);
                        let slice = Slice::read(core, &epochs, n + 1, rows)?;
                        read += slice.read;
                        e.insert(slice);
                    }
                    let slice = &slices[&key];
                    if slice.recover {
                        push.recover = true;
                    } else if let Some(&first) = slice.first.as_ref() {
                        // The cut: at most 512 messages and 512 KiB of JSON per request.
                        push.first = first;
                        let fixed = push.encode().len();
                        let (mut k, mut total) = (0, fixed);
                        while k < slice.items.len()
                            && k < 512
                            && total + slice.sizes[k] + usize::from(k > 0) <= 512 * 1024
                        {
                            total += slice.sizes[k] + usize::from(k > 0);
                            k += 1;
                        }
                        if k > 0 && total - fixed > budget && budget < ROUND_BYTES {
                            deferred = true;
                            self.resume.get_or_insert_with(|| child.address.clone());
                            continue;
                        }
                        budget = budget.saturating_sub(total - fixed);
                        push.messages = slice.items[..k].to_vec();
                    }
                }
                let body = push.encode();
                Push::decode(&body)?; // Reject numbers JSON cannot represent exactly.
                ("push", body)
            };
            indices.push(index);
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
        let mut next = deferred.then(|| now.saturating_add(20));
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
                let entries = g.iter().map(|i| requests[*i].clone()).collect::<Vec<_>>();
                relay_requests.push((
                    r.name.clone(),
                    crate::actors::Request {
                        method: "POST".into(),
                        path: "/wasmkeep-relay/v1/push".into(),
                        headers: vec![],
                        body: super::relay::pack(&entries)?.into_bytes(),
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
            if skip.contains(&slot) {
                continue;
            }
            let child = &mut self.children[index];
            let ok = result.filter(|r| r.status == 200).and_then(|r| {
                if !child.initialized {
                    child.initialized = true;
                    return Some(20);
                }
                let (p, more) = acknowledgment(&r.body, epoch.e, head)?;
                child.position = Some(p);
                child.sockets = crate::json::parse_unique(std::str::from_utf8(&r.body).ok()?)
                    .and_then(|j| super::edge::number(&j, "sockets").ok());
                let j = crate::json::parse_unique(std::str::from_utf8(&r.body).ok()?)?;
                let lagging = super::edge::number(&j, "lagging").ok()?;
                let stalled = super::edge::number(&j, "stalled").unwrap_or(0);
                let delay = child.lag_retry.delay(
                    p,
                    head,
                    lagging,
                    if p.e == epoch.e { stalled } else { 0 },
                );
                Some(if more { delay } else { 0 })
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
        Ok(())
    }
}

/// Message bytes one round builds for its children (ADR 0040, section 4).
const ROUND_BYTES: usize = 1024 * 1024;
/// What reading one row costs the round's budget besides its JSON: at
/// most about 1,000 rows a round.
const ROW_COST: usize = 1024;
/// The messages after one position, each with its JSON size.
struct Slice {
    first: Option<u64>,
    items: Vec<Item>,
    sizes: Vec<usize>,
    recover: bool,
    read: usize,
}
impl Slice {
    fn read<S: Store>(
        core: &Core<S>,
        epochs: &[Epoch],
        from: u64,
        rows: usize,
    ) -> Result<Self, String> {
        let mut s = Slice {
            first: None,
            items: vec![],
            sizes: vec![],
            recover: false,
            read: 0,
        };
        for m in core.rows(from, rows, 512 * 1024)? {
            let Some(boundary) = epochs.iter().find(|e| e.e == m.position.e) else {
                return Ok(Slice { recover: true, ..s });
            };
            s.first.get_or_insert(m.position.n);
            let item = Item {
                epoch: Some(Epoch {
                    e: m.position.e,
                    base: boundary.base,
                }),
                at: m.at,
                id: m.id,
                body: m.body,
            };
            let size = item.json().len();
            // Every row costs, an empty one too: its read, its item and its JSON.
            s.read += ROW_COST + size;
            s.sizes.push(size);
            s.items.push(item);
        }
        Ok(s)
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
