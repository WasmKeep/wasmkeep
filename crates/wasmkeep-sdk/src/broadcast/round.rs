use super::*;
/// Fixed direct children. Recreate this in memory at component startup; the
/// application's fixed addresses must be identical across wakes. No scaling.
#[derive(Debug, Default)]
pub struct Round {
    children: Vec<Child>,
}
#[derive(Debug)]
struct Child {
    address: String,
    position: Option<Position>,
    initialized: bool,
    failures: u32,
    due: u64,
}
impl Round {
    pub fn new(addresses: &[&str]) -> Result<Self, String> {
        if addresses.len() > 16 {
            return Err("at most 16 fixed edges".into());
        }
        Ok(Self {
            children: addresses
                .iter()
                .map(|address| Child {
                    address: (*address).into(),
                    position: None,
                    initialized: false,
                    failures: 0,
                    due: 0,
                })
                .collect(),
        })
    }
    pub fn run<S: Store>(&mut self, core: &mut Core<S>, now: u64) -> Result<(), String> {
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
            if child.due > now {
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
        for (index, result) in indices.into_iter().zip(crate::calls::call_many(&requests)) {
            let child = &mut self.children[index];
            let ok = result.ok().filter(|r| r.status == 200).and_then(|r| {
                if !child.initialized {
                    child.initialized = true;
                    return Some(true);
                }
                let (p, more) = acknowledgment(&r.body, epoch.e, head)?;
                child.position = Some(p);
                Some(more)
            });
            if let Some(more) = ok {
                child.failures = 0;
                child.due = 0;
                if more {
                    next = Some(now.saturating_add(20));
                }
            } else {
                child.failures = child.failures.saturating_add(1);
                child.due = now.saturating_add(2000u64 << child.failures.saturating_sub(1).min(3));
            }
        }
        for c in &self.children {
            if c.due > now {
                next = Some(next.map_or(c.due, |n: u64| n.min(c.due)));
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
