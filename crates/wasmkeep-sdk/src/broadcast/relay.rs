//! Stateless one-layer relay. Its only outbound calls are pushes to edges.
use super::Push;
use crate::{
    Actor, actors, http,
    json::{self, Json},
    wire,
};

fn stringify(j: &Json) -> String {
    let mut s = String::new();
    j.write(&mut s);
    s
}
fn array<'a>(j: &'a Json, key: &str) -> Result<&'a [Json], String> {
    match j.get(key) {
        Some(Json::Array(a)) => Ok(a),
        _ => Err("invalid relay array".into()),
    }
}
#[cfg(test)]
pub(super) fn pack(entries: &[(String, actors::Request)]) -> Result<String, String> {
    let pushes: Vec<_> = entries
        .iter()
        .map(|(_, r)| Push::decode(std::str::from_utf8(&r.body).map_err(|_| "invalid UTF-8")?))
        .collect::<Result<_, _>>()?;
    let parts: Vec<_> = entries
        .iter()
        .zip(&pushes)
        .map(|((edge, _), p)| (edge.as_str(), p, &p.messages[..]))
        .collect();
    pack_parts(&parts)
}
/// [`pack`] from each edge's push header and messages, as the round has
/// them: the shared messages are encoded once, and each part checked as
/// its own push would be (numbers JSON represents exactly).
pub(super) fn pack_parts(entries: &[(&str, &Push, &[super::Item])]) -> Result<String, String> {
    if entries.is_empty() || entries.len() > 16 {
        return Err("relay fan-out: 1..16".into());
    }
    let (_, head, items) = entries
        .iter()
        .filter(|(_, _, m)| !m.is_empty())
        .min_by_key(|(_, p, _)| p.first)
        .unwrap_or(&entries[0]);
    let shared = Push {
        messages: items.to_vec(),
        ..(*head).clone()
    };
    let encoded = shared.encode();
    Push::decode(&encoded)?;
    let end = shared.first + shared.messages.len() as u64;
    let mut s = format!("{{\"shared\":{encoded},\"children\":[");
    for (i, (edge, p, m)) in entries.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str("{\"edge\":");
        json::write_str(&mut s, edge);
        let count = if p.recover {
            0
        } else {
            m.len().min(end.saturating_sub(p.first) as usize)
        };
        let mut header = (*p).clone();
        header.messages.clear();
        let header = header.encode();
        Push::decode(&header)?;
        s.push_str(&format!(",\"push\":{header},\"count\":{count}}}"));
    }
    s.push_str("]}");
    Ok(s)
}
pub(super) fn unpack(body: &str) -> Result<Vec<(String, actors::Request)>, String> {
    let j = json::parse_unique(body).ok_or("invalid relay")?;
    let shared = Push::decode(&stringify(j.get("shared").ok_or("invalid shared push")?))?;
    let children = array(&j, "children")?;
    if children.is_empty() || children.len() > 16 {
        return Err("invalid relay fan-out".into());
    }
    let mut requests = vec![];
    for c in children {
        let edge = super::edge::string(c, "edge")?.to_owned();
        if requests.iter().any(|(n, _)| n == &edge) {
            return Err("duplicate relay child".into());
        }
        let mut push = Push::decode(&stringify(c.get("push").ok_or("invalid child push")?))?;
        let count = super::edge::number(c, "count")? as usize;
        if !push.messages.is_empty() || count > 512 {
            return Err("invalid relay slice".into());
        }
        if count > 0 {
            let offset = push
                .first
                .checked_sub(shared.first)
                .ok_or("invalid relay offset")? as usize;
            push.messages = shared
                .messages
                .get(offset..offset.saturating_add(count))
                .ok_or("invalid relay slice")?
                .to_vec();
        }
        Push::decode(&push.encode())?;
        requests.push((
            edge,
            actors::Request {
                method: "POST".into(),
                path: "/wasmkeep-edge/v1/push".into(),
                headers: vec![],
                body: push.encode().into_bytes(),
            },
        ));
    }
    Ok(requests)
}
pub(super) fn answers(body: &[u8], count: usize) -> Option<Vec<Option<actors::Response>>> {
    let Json::Array(a) = json::parse_unique(std::str::from_utf8(body).ok()?)? else {
        return None;
    };
    if a.len() != count {
        return None;
    }
    Some(
        a.iter()
            .map(|j| {
                if *j == Json::Null {
                    None
                } else {
                    Some(actors::Response {
                        status: 200,
                        headers: vec![],
                        body: stringify(j).into_bytes(),
                    })
                }
            })
            .collect(),
    )
}
#[derive(Debug)]
pub struct RelayActor;
impl Actor for RelayActor {}
impl http::Guest for RelayActor {
    fn handle(req: http::IncomingRequest, out: http::ResponseOutparam) {
        let result = (|| -> Result<String, String> {
            wire::caller(&req).ok_or("forbidden")?;
            if !matches!(req.method(), http::Method::Post) {
                return Err("invalid method".into());
            }
            match req.path_with_query().as_deref() {
                Some("/wasmkeep-relay/v1/init") => Ok("{}".into()),
                Some("/wasmkeep-relay/v1/end") => {
                    crate::storage::delete_self();
                    Ok("{}".into())
                }
                Some("/wasmkeep-relay/v1/push") => {
                    let bytes = http::read_body(&req, 1024 * 1024).ok_or("invalid body")?;
                    let requests =
                        unpack(std::str::from_utf8(&bytes).map_err(|_| "invalid UTF-8")?)?;
                    let replies = crate::calls::call_many(&requests);
                    let answers: Vec<_> = replies
                        .into_iter()
                        .map(|r| {
                            r.ok()
                                .filter(|r| r.status == 200)
                                .and_then(|r| {
                                    std::str::from_utf8(&r.body)
                                        .ok()
                                        .and_then(json::parse_unique)
                                })
                                .unwrap_or(Json::Null)
                        })
                        .collect();
                    Ok(stringify(&Json::Array(answers)))
                }
                _ => Err("invalid path".into()),
            }
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::broadcast::{Edge, Item, Position, SendResult, SocketIo};
    #[derive(Default)]
    struct Sockets(Vec<String>);
    impl SocketIo for Sockets {
        fn send(&mut self, _id: u64, s: &str) -> SendResult {
            self.0.push(s.into());
            SendResult::Ok
        }
        fn close(&mut self, _id: u64, _code: u16) {}
    }
    #[test]
    fn relay_slice_preserves_order_and_deduplicates() {
        let push = Push {
            recover: false,
            e: 1,
            base: 0,
            first: 1,
            head: 3,
            messages: (1..=3)
                .map(|n| Item {
                    epoch: None,
                    at: n,
                    id: None,
                    body: format!("message{n}"),
                })
                .collect(),
        };
        let mut later = push.clone();
        later.first = 2;
        later.messages.remove(0);
        let entries: Vec<_> = [push.clone(), later]
            .into_iter()
            .enumerate()
            .map(|(i, p)| {
                (
                    format!("edge/{i}"),
                    actors::Request {
                        method: "POST".into(),
                        path: "/wasmkeep-edge/v1/push".into(),
                        headers: vec![],
                        body: p.encode().into_bytes(),
                    },
                )
            })
            .collect();
        let packed = pack(&entries).unwrap();
        assert_eq!(packed.matches("message1").count(), 1);
        for (i, (_, request)) in unpack(&packed).unwrap().into_iter().enumerate() {
            let mut edge = Edge::default();
            let mut io = Sockets::default();
            let mut initial = push.clone();
            initial.head = i as u64;
            initial.first = i as u64 + 1;
            initial.messages.clear();
            edge.push(&initial, &mut io);
            edge.open(1, Some(Position { e: 1, n: i as u64 }), &mut io);
            let p = Push::decode(std::str::from_utf8(&request.body).unwrap()).unwrap();
            edge.push(&p, &mut io);
            edge.push(&p, &mut io);
            let frames: Vec<_> =
                io.0.iter()
                    .filter_map(|s| json::parse_unique(s))
                    .filter(|j| super::super::edge::string(j, "t") == Ok("m"))
                    .collect();
            assert_eq!(frames.len(), 1);
            assert_eq!(
                super::super::edge::number(&frames[0], "n").unwrap(),
                i as u64 + 1
            );
            assert_eq!(array(&frames[0], "m").unwrap().len(), 3 - i);
        }
        assert!(pack(&vec![entries[0].clone(); 17]).is_err());
    }
}
