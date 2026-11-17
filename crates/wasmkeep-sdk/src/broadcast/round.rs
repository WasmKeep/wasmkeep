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
                    e: epoch.e,
                    base: epoch.base,
                    first: head.saturating_add(1),
                    head,
                    messages: vec![],
                };
                if let Some(n) = n {
                    for m in rows.iter().filter(|m| m.position.n > n) {
                        if push.messages.is_empty() {
                            push.first = m.position.n;
                        }
                        push.messages.push(Item {
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
                ("push", push.encode())
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
                let j = crate::json::parse_unique(std::str::from_utf8(&r.body).ok()?)?;
                let p = Position {
                    e: super::edge::number(&j, "e").ok()?,
                    n: super::edge::number(&j, "n").ok()?,
                };
                if p.e > epoch.e || p.n > head {
                    return None;
                }
                child.position = Some(p);
                Some(p.e != epoch.e || p.n < head || super::edge::number(&j, "lagging").ok()? > 0)
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
