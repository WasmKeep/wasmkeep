use super::*;
/// Direct-child delivery positions and suspect backoff, recreated after a wake.
/// Use `new` for fixed edges or let `Scaling` synchronize its durable children.
#[derive(Debug, Default)]
pub struct Round {
    children: Vec<Child>,
    relays: Vec<Relay>,
    relay_suspects: std::collections::BTreeMap<String, Suspect>,
}
#[derive(Debug)]
struct Child {
    address: String,
    position: Option<Position>,
    initialized: bool,
    suspect: Suspect,
    sockets: Option<u64>,
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
            children: addresses
                .iter()
                .map(|address| Child {
                    address: (*address).into(),
                    position: None,
                    initialized: false,
                    suspect: Suspect::default(),
                    sockets: None,
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
        let rows = core.messages()?;
        let epochs = core.epochs()?;
        let head = rows.last().map_or(epoch.base, |m| m.position.n);
        let mut indices = vec![];
        let mut requests = vec![];
        for (index, child) in self.children.iter().enumerate() {
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
                if let Some(n) = n {
                    for m in rows.iter().filter(|m| m.position.n > n) {
                        let Some(boundary) = epochs.iter().find(|e| e.e == m.position.e) else {
                            push.recover = true;
                            push.messages.clear();
                            break;
                        };
                        if push.messages.is_empty() {
                            push.first = m.position.n;
                        }
                        push.messages.push(Item {
                            epoch: Some(Epoch {
                                e: m.position.e,
                                base: boundary.base,
                            }),
                            at: m.at,
                            id: m.id.clone(),
                            body: m.body.clone(),
                        });
                        if push.messages.len() > 512 || push.encode().len() > 512 * 1024 {
                            push.messages.pop();
                            break;
                        }
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
        let mut next = None;
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
                    return Some(true);
                }
                let (p, more) = acknowledgment(&r.body, epoch.e, head)?;
                child.position = Some(p);
                child.sockets = crate::json::parse_unique(std::str::from_utf8(&r.body).ok()?)
                    .and_then(|j| super::edge::number(&j, "sockets").ok());
                Some(more)
            });
            if let Some(more) = ok {
                child.suspect = Suspect::default();
                if more {
                    next = Some(now.saturating_add(20));
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
