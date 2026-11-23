use wasmkeep_sdk::broadcast::{Client, Position, Publish, handler, pusher};
use wasmkeep_sdk::broadcast::{EdgeActor, Round};
use wasmkeep_sdk::{Actor, http, storage};
thread_local! { static ROUND: std::cell::RefCell<Round> = std::cell::RefCell::new(Round::new(&["edge/e"]).unwrap()); }
thread_local! { static FANOUT: std::cell::RefCell<wasmkeep_sdk::broadcast::Edge> = Default::default(); }
fn fanout() -> bool {
    wasmkeep_sdk::actors::self_address().class == "fanout"
}
/// An edge with no room: each request is the first push after a wake
/// (`.../header` header-only, otherwise one message) to the sockets kept.
fn wake() -> bool {
    wasmkeep_sdk::actors::self_address().class == "wake"
}
fn edge() -> bool {
    wasmkeep_sdk::actors::self_address().class == "edge"
}
/// The room `t/room` has the pusher `pusher/room` (ADR 0040, section 13).
fn is_pusher() -> bool {
    wasmkeep_sdk::actors::self_address().class == "pusher"
}
struct TestRoom;
impl Actor for TestRoom {
    fn on_init() -> Result<(), String> {
        if wake() {
            Ok(())
        } else if fanout() {
            FANOUT.with_borrow_mut(|edge| {
                edge.push(
                    &wasmkeep_sdk::broadcast::Push {
                        recover: false,
                        e: 1,
                        base: 0,
                        first: 1,
                        head: 512,
                        messages: (0..512)
                            .map(|_| wasmkeep_sdk::broadcast::Item {
                                epoch: None,
                                at: 0,
                                id: None,
                                body: "x".repeat(100),
                            })
                            .collect(),
                    },
                    &mut wasmkeep_sdk::broadcast::HostSockets,
                );
            });
            Ok(())
        } else if edge() {
            EdgeActor::on_init()
        } else if is_pusher() {
            Ok(())
        } else {
            handler::init(false)
        }
    }
    fn on_websocket_open(id: u64, path: String) -> Result<(), String> {
        if fanout() || wake() {
            return FANOUT.with_borrow_mut(|edge| {
                if edge.open(id, None, &mut wasmkeep_sdk::broadcast::HostSockets) {
                    Ok(())
                } else {
                    Err("full".into())
                }
            });
        }
        EdgeActor::on_websocket_open(id, path)
    }
    fn on_websocket_message(id: u64, msg: wasmkeep_sdk::Message) {
        EdgeActor::on_websocket_message(id, msg);
    }
    fn on_websocket_close(id: u64, code: u16, reason: String) {
        if fanout() || wake() {
            FANOUT.with_borrow_mut(|edge| edge.forget(id));
            return;
        }
        EdgeActor::on_websocket_close(id, code, reason);
    }
    fn on_alarm() -> Result<(), String> {
        if is_pusher() {
            TestRoom::round().map(|_| ())
        } else {
            Ok(())
        }
    }
}
impl TestRoom {
    fn round() -> Result<String, String> {
        pusher::handler::room_class("t");
        ROUND
            .with_borrow_mut(pusher::handler::round)
            .map(|_| "ok".into())
    }
}
impl http::Guest for TestRoom {
    fn handle(req: http::IncomingRequest, out: http::ResponseOutparam) {
        if wake() {
            let header = req
                .path_with_query()
                .unwrap_or_default()
                .ends_with("header");
            let ack = FANOUT.with_borrow_mut(|edge| {
                edge.receive(
                    &wasmkeep_sdk::broadcast::Push {
                        recover: false,
                        e: 1,
                        base: 0,
                        first: 513,
                        head: if header { 512 } else { 513 },
                        messages: if header {
                            vec![]
                        } else {
                            vec![wasmkeep_sdk::broadcast::Item {
                                epoch: None,
                                at: 0,
                                id: None,
                                body: "new".into(),
                            }]
                        },
                    },
                    &wasmkeep_sdk::websockets::connections(),
                    &mut wasmkeep_sdk::broadcast::HostSockets,
                )
            });
            http::respond(out, 200, "application/json", ack.encode().as_bytes());
            return;
        }
        if fanout() {
            let ack = FANOUT.with_borrow_mut(|edge| {
                edge.push(
                    &wasmkeep_sdk::broadcast::Push {
                        recover: false,
                        e: 1,
                        base: 0,
                        first: edge.head.unwrap().n + 1,
                        head: edge.head.unwrap().n + 1,
                        messages: vec![wasmkeep_sdk::broadcast::Item {
                            epoch: None,
                            at: 0,
                            id: None,
                            body: "new".into(),
                        }],
                    },
                    &mut wasmkeep_sdk::broadcast::HostSockets,
                )
            });
            http::respond(out, 200, "application/json", ack.encode().as_bytes());
            return;
        }
        if edge() {
            EdgeActor::handle(req, out);
            return;
        }
        let path = req.path_with_query().unwrap_or_default();
        if is_pusher() {
            if path == "/r3-round" {
                match Self::round() {
                    Ok(s) => http::respond(out, 200, "text/plain", s.as_bytes()),
                    Err(s) => http::respond(out, 500, "text/plain", s.as_bytes()),
                }
            } else {
                pusher::handler::handle(req, out);
            }
            return;
        }
        let client = Client::new("t/room");
        let result: Result<String, String> = match path.as_str() {
            "/r3-start" => (|| {
                client.history(None, 100, None)?;
                for _ in 0..2 {
                    let r = wasmkeep_sdk::actors::call(
                        "pusher/room",
                        &wasmkeep_sdk::actors::Request {
                            method: "POST".into(),
                            path: "/r3-round".into(),
                            headers: vec![],
                            body: vec![],
                        },
                    )
                    .map_err(|e| format!("{e:?}"))?;
                    if r.status != 200 {
                        return Err(String::from_utf8_lossy(&r.body).into_owned());
                    }
                }
                Ok("ok".into())
            })(),
            "/r3-publish" => client
                .publish(&[Publish {
                    body: "x".repeat(32700),
                    id: None,
                }])
                .map(|p| format!("{}.{}", p[0].e, p[0].n)),
            "/publish" => client
                .publish(&[Publish {
                    body: "hello".into(),
                    id: None,
                }])
                .map(|p| format!("{}.{}", p[0].e, p[0].n)),
            "/warm" => client
                .history(None, 100, None)
                .map(|h| format!("{}.{}", h.head.e, h.head.n)),
            "/flush" => {
                let r = storage::flush().map_err(|e| e.to_string());
                r.map(|_| "ok".into())
            }
            _ if path.starts_with("/r3-history?") => {
                let raw = path.strip_prefix("/r3-history?after=").unwrap();
                let (e, n) = raw.split_once('.').unwrap();
                client
                    .history(
                        Some(Position {
                            e: e.parse().unwrap(),
                            n: n.parse().unwrap(),
                        }),
                        100,
                        None,
                    )
                    .map(|h| {
                        format!(
                            "{{\"e\":{},\"head\":{},\"reset\":{},\"more\":{},\"numbers\":[{}]}}",
                            h.head.e,
                            h.head.n,
                            h.reset.is_some(),
                            h.more,
                            h.messages
                                .iter()
                                .map(|m| {
                                    assert_eq!(m.body, "x".repeat(32700));
                                    m.position.n.to_string()
                                })
                                .collect::<Vec<_>>()
                                .join(",")
                        )
                    })
            }
            _ if path.starts_with("/reconnect?") => {
                let raw = path.strip_prefix("/reconnect?after=").unwrap();
                let (e, n) = raw.split_once('.').unwrap();
                client
                    .history(
                        Some(Position {
                            e: e.parse().unwrap(),
                            n: n.parse().unwrap(),
                        }),
                        100,
                        None,
                    )
                    .map(|h| {
                        format!(
                            "head={}.{} lost={} reset={} messages={}",
                            h.head.e,
                            h.head.n,
                            h.lost
                                .map(|e| format!("{}.{}", e.e, e.base))
                                .unwrap_or_default(),
                            h.reset.is_some(),
                            h.messages.len()
                        )
                    })
            }
            _ => {
                handler::handle(req, out);
                return;
            }
        };
        match result {
            Ok(s) => http::respond(out, 200, "text/plain", s.as_bytes()),
            Err(s) => http::respond(out, 500, "text/plain", s.as_bytes()),
        }
    }
}
wasmkeep_sdk::export!(TestRoom);
